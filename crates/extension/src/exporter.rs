use std::collections::VecDeque;
use std::io::Write;

use bytes::Bytes;
use flate2::write::GzEncoder;
use prost::Message;
use reqwest::Client;
use thiserror::Error;
use url::Url;

use crate::buffers::BufferData;
use crate::config::{Compression, Config};
use crate::merge;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("collector rejected payload: {status}")]
    Rejected { status: reqwest::StatusCode },

    #[error("gzip compression failed: {0}")]
    Compression(#[from] std::io::Error),
}

/// Abstraction over exporting telemetry data to a collector.
///
/// The returned future must be `Send` because `OutboundBuffer::spawn_flush`
/// calls `exporter.export()` inside `tokio::spawn`.
pub trait Exporter: Send + Sync + 'static {
    fn export(
        &self,
        data: &mut BufferData,
    ) -> impl Future<Output = Result<(), ExportError>> + Send;
}

pub struct OtlpExporter {
    client: Client,
    endpoint: Url,
    compression: Compression,
    headers: Vec<(String, String)>,
}

impl OtlpExporter {
    pub fn new(config: &Config) -> Self {
        let client = Client::builder()
            .timeout(config.export_timeout)
            .build()
            .expect("failed to build HTTP client");

        Self {
            client,
            endpoint: config.endpoint.clone(),
            compression: config.compression,
            headers: config.export_headers.clone(),
        }
    }

    async fn export_traces(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        if queue.is_empty() {
            return Ok(());
        }
        self.post("v1/traces", &merge::merge_traces(queue)).await
    }

    async fn export_metrics(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        if queue.is_empty() {
            return Ok(());
        }
        self.post("v1/metrics", &merge::merge_metrics(queue)).await
    }

    async fn export_logs(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        if queue.is_empty() {
            return Ok(());
        }
        self.post("v1/logs", &merge::merge_logs(queue)).await
    }

    async fn post(&self, path: &str, msg: &impl Message) -> Result<(), ExportError> {
        let url = self.endpoint.join(path).expect("invalid export path");

        let mut req = self
            .client
            .post(url)
            .header("content-type", "application/x-protobuf");

        let body = match self.compression {
            Compression::Gzip => {
                req = req.header("content-encoding", "gzip");
                encode_gzip(msg)?
            }
            Compression::None => msg.encode_to_vec(),
        };

        for (k, v) in &self.headers {
            req = req.header(k, v);
        }

        let resp = req.body(body).send().await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(ExportError::Rejected {
                status: resp.status(),
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

/// Encode a protobuf message and gzip-compress the result.
/// Pre-sizes both buffers using `encoded_len()` to avoid reallocation.
fn encode_gzip(msg: &impl Message) -> Result<Vec<u8>, std::io::Error> {
    let raw = msg.encode_to_vec();
    let mut encoder = GzEncoder::new(Vec::with_capacity(raw.len()), flate2::Compression::fast());
    encoder.write_all(&raw)?;
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
    fn encode_gzip_produces_valid_protobuf() {
        let msg = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![],
                schema_url: String::new(),
            }],
        };

        let compressed = encode_gzip(&msg).unwrap();

        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, msg.encode_to_vec());
    }
}
