mod grpc;
mod http_protobuf;

use std::env;
use std::io::Write;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use flate2::write::GzEncoder;
use thiserror::Error;
use url::Url;

use crate::buffers::BufferData;
use crate::config::{Compression, Config, ExportProtocol, SigV4Config};
use crate::grpc as grpc_codec;
use crate::http_client::{ClientError, HttpClient};

pub use self::grpc::GrpcExporter;
pub use self::http_protobuf::HttpProtobufExporter;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] ClientError),

    #[error("collector rejected payload: {status}")]
    Rejected { status: hyper::StatusCode },

    #[error("gRPC error: {0}")]
    Grpc(#[from] grpc_codec::GrpcError),

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

pub enum OtlpExporter {
    HttpProtobuf(HttpProtobufExporter),
    Grpc(GrpcExporter),
}

impl OtlpExporter {
    pub fn new(config: &Config) -> Result<Self, ExporterError> {
        let client = HttpClient::new(
            config.export_timeout,
            config.tls_ca.as_deref(),
            config.tls_client_cert.as_deref(),
            config.tls_client_key.as_deref(),
        )?;

        let common = CommonExporter {
            client,
            endpoint: config.endpoint.clone(),
            compression: config.compression,
            headers: config.export_headers.clone(),
            sigv4: config.sigv4.clone(),
        };

        Ok(match config.protocol {
            ExportProtocol::HttpProtobuf => Self::HttpProtobuf(HttpProtobufExporter(common)),
            ExportProtocol::Grpc => Self::Grpc(GrpcExporter(common)),
        })
    }
}

impl Exporter for OtlpExporter {
    async fn export(&self, data: &mut BufferData) -> Result<(), ExportError> {
        match self {
            Self::HttpProtobuf(e) => e.export(data).await,
            Self::Grpc(e) => e.export(data).await,
        }
    }
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

struct CommonExporter {
    client: HttpClient,
    endpoint: Url,
    compression: Compression,
    headers: Vec<(String, String)>,
    sigv4: Option<SigV4Config>,
}

impl CommonExporter {
    /// Send a request with the given headers and body, applying custom headers
    /// and SigV4 signing.
    async fn send(
        &self,
        url: &Url,
        mut headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<crate::http_client::HttpResponse, ExportError> {
        for (k, v) in &self.headers {
            headers.push((k.clone(), v.clone()));
        }

        if let Some(sigv4) = &self.sigv4 {
            let signing_headers = sign_request(sigv4, url.as_str(), &headers, &body)?;
            headers.extend(signing_headers);
        }

        Ok(self.client.post(url.as_str(), &headers, body).await?)
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

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
    use prost::Message;
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
