use std::collections::VecDeque;
use std::env;
use std::io::Write;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use bytes::Bytes;
use flate2::write::GzEncoder;
use prost::Message;
use thiserror::Error;
use url::Url;

use crate::buffers::BufferData;
use crate::config::{Compression, Config, SigV4Config};
use crate::http_client::{ClientError, HttpClient};
use crate::merge;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] ClientError),

    #[error("collector rejected payload: {status}")]
    Rejected { status: hyper::StatusCode },

    #[error("gzip compression failed: {0}")]
    Compression(#[from] std::io::Error),

    #[error("SigV4 signing failed: {0}")]
    Signing(String),
}

#[derive(Debug, Error)]
pub enum ExporterError {
    #[error("failed to build HTTP client: {0}")]
    ClientBuild(#[from] ClientError),
}

/// Abstraction over exporting telemetry data to a collector.
///
/// The returned future must be `Send` because `OutboundBuffer::spawn_flush`
/// calls `exporter.export()` inside `tokio::spawn`.
pub trait Exporter: Send + Sync + 'static {
    fn export(&self, data: &mut BufferData)
    -> impl Future<Output = Result<(), ExportError>> + Send;
}

pub struct OtlpExporter {
    client: HttpClient,
    endpoint: Url,
    compression: Compression,
    headers: Vec<(String, String)>,
    sigv4: Option<SigV4Config>,
}

impl OtlpExporter {
    pub fn new(config: &Config) -> Result<Self, ExporterError> {
        let client = HttpClient::new(
            config.export_timeout,
            config.tls_ca.as_deref(),
            config.tls_client_cert.as_deref(),
            config.tls_client_key.as_deref(),
        )?;

        Ok(Self {
            client,
            endpoint: config.endpoint.clone(),
            compression: config.compression,
            headers: config.export_headers.clone(),
            sigv4: config.sigv4.clone(),
        })
    }

    async fn export_traces(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        match queue.len() {
            0 => Ok(()),
            1 => self.post_bytes("v1/traces", &queue[0]).await,
            _ => self.post("v1/traces", &merge::merge_traces(queue)).await,
        }
    }

    async fn export_metrics(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        match queue.len() {
            0 => Ok(()),
            1 => self.post_bytes("v1/metrics", &queue[0]).await,
            _ => self.post("v1/metrics", &merge::merge_metrics(queue)).await,
        }
    }

    async fn export_logs(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        match queue.len() {
            0 => Ok(()),
            1 => self.post_bytes("v1/logs", &queue[0]).await,
            _ => self.post("v1/logs", &merge::merge_logs(queue)).await,
        }
    }

    /// Send pre-encoded protobuf bytes directly, skipping decode/merge/re-encode.
    async fn post_bytes(&self, path: &str, protobuf: &[u8]) -> Result<(), ExportError> {
        self.post_body(path, protobuf).await
    }

    async fn post(&self, path: &str, msg: &impl Message) -> Result<(), ExportError> {
        self.post_body(path, &msg.encode_to_vec()).await
    }

    async fn post_body(&self, path: &str, protobuf: &[u8]) -> Result<(), ExportError> {
        let url = self.endpoint.join(path).expect("invalid export path");

        let mut headers: Vec<(String, String)> = vec![(
            "content-type".to_owned(),
            "application/x-protobuf".to_owned(),
        )];

        let body = match self.compression {
            Compression::Gzip => {
                headers.push(("content-encoding".to_owned(), "gzip".to_owned()));
                compress_gzip(protobuf)?
            }
            Compression::None => protobuf.to_vec(),
        };

        for (k, v) in &self.headers {
            headers.push((k.clone(), v.clone()));
        }

        if let Some(sigv4) = &self.sigv4 {
            let signing_headers = sign_request(sigv4, url.as_str(), &headers, &body)?;
            headers.extend(signing_headers);
        }

        let resp = self.client.post(url.as_str(), &headers, body).await?;

        if resp.status.is_success() {
            Ok(())
        } else {
            Err(ExportError::Rejected {
                status: resp.status,
            })
        }
    }
}

impl Exporter for OtlpExporter {
    async fn export(&self, data: &mut BufferData) -> Result<(), ExportError> {
        if data.is_empty() {
            return Ok(());
        }

        let (t, m, l) = tokio::join!(
            self.export_traces(&data.traces.queue),
            self.export_metrics(&data.metrics.queue),
            self.export_logs(&data.logs.queue),
        );

        if t.is_ok() {
            data.traces.clear();
        }
        if m.is_ok() {
            data.metrics.clear();
        }
        if l.is_ok() {
            data.logs.clear();
        }

        t.and(m).and(l)
    }
}

/// Read credentials from the environment and sign the request.
///
/// Returns the signing headers to add to the request. Credentials are read
/// fresh on each call because Lambda rotates them during the extension's
/// lifetime.
fn sign_request(
    sigv4: &SigV4Config,
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<Vec<(String, String)>, ExportError> {
    let access_key = env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ExportError::Signing("AWS_ACCESS_KEY_ID not set".into()))?;
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ExportError::Signing("AWS_SECRET_ACCESS_KEY not set".into()))?;
    let session_token = env::var("AWS_SESSION_TOKEN").ok().filter(|s| !s.is_empty());

    let credentials = Credentials::new(
        access_key,
        secret_key,
        session_token,
        None,
        "lambda-otel-relay",
    );
    let identity = credentials.into();

    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(sigv4.region.as_str())
        .name(sigv4.service.as_str())
        .time(SystemTime::now())
        .settings(SigningSettings::default())
        .build()
        .map_err(|e| ExportError::Signing(e.to_string()))?;
    let signing_params = signing_params.into();

    let signable_request = SignableRequest::new(
        "POST",
        url,
        headers.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        SignableBody::Bytes(body),
    )
    .map_err(|e| ExportError::Signing(e.to_string()))?;

    let (instructions, _signature) = sign(signable_request, &signing_params)
        .map_err(|e| ExportError::Signing(e.to_string()))?
        .into_parts();

    Ok(instructions
        .headers()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect())
}

/// Gzip-compress pre-encoded protobuf bytes.
fn compress_gzip(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(data.len()), flate2::Compression::fast());
    encoder.write_all(data)?;
    encoder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
    use crate::proto::opentelemetry::proto::trace::v1::ResourceSpans;
    use flate2::read::GzDecoder;
    use std::io::Read;

    #[test]
    fn compress_gzip_produces_valid_protobuf() {
        let msg = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![],
                schema_url: String::new(),
            }],
        };

        let encoded = msg.encode_to_vec();
        let compressed = compress_gzip(&encoded).unwrap();

        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, encoded);
    }
}
