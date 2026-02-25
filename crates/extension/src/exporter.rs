use std::io::Write;

use bytes::Bytes;
use flate2::write::GzEncoder;
use prost::Message;
use reqwest::Client;
use thiserror::Error;
use tracing::warn;
use url::Url;

use crate::buffers::OutboundBuffer;
use crate::config::{Compression, Config};
use crate::proto::opentelemetry::proto::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("collector rejected payload: {status}")]
    Rejected { status: reqwest::StatusCode },

    #[error("gzip compression failed: {0}")]
    Compression(#[from] std::io::Error),
}

pub struct Exporter {
    client: Client,
    endpoint: Url,
    compression: Compression,
    headers: Vec<(String, String)>,
}

impl Exporter {
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

    pub async fn export(&self, buffer: &mut OutboundBuffer) -> Result<(), ExportError> {
        if buffer.is_empty() {
            return Ok(());
        }

        // Traces
        if !buffer.traces.is_empty() {
            let merged = concat_traces(&buffer.traces.queue);
            self.post("v1/traces", &merged.encode_to_vec()).await?;
        }

        // Metrics
        if !buffer.metrics.is_empty() {
            let merged = concat_metrics(&buffer.metrics.queue);
            self.post("v1/metrics", &merged.encode_to_vec()).await?;
        }

        // Logs
        if !buffer.logs.is_empty() {
            let merged = concat_logs(&buffer.logs.queue);
            self.post("v1/logs", &merged.encode_to_vec()).await?;
        }

        buffer.clear();
        Ok(())
    }

    async fn post(&self, path: &str, body: &[u8]) -> Result<(), ExportError> {
        let url = self.endpoint.join(path).expect("invalid export path");

        let body = if self.compression == Compression::Gzip {
            compress_gzip(body)?
        } else {
            body.to_vec()
        };

        let mut req = self
            .client
            .post(url)
            .header("content-type", "application/x-protobuf");

        if self.compression == Compression::Gzip {
            req = req.header("content-encoding", "gzip");
        }

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

fn compress_gzip(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data)?;
    encoder.finish()
}

fn concat_traces(
    payloads: &std::collections::VecDeque<Bytes>,
) -> ExportTraceServiceRequest {
    let mut resource_spans = Vec::new();
    for payload in payloads {
        match ExportTraceServiceRequest::decode(payload.clone()) {
            Ok(req) => resource_spans.extend(req.resource_spans),
            Err(e) => warn!(error = %e, "skipping malformed trace payload"),
        }
    }
    ExportTraceServiceRequest { resource_spans }
}

fn concat_metrics(
    payloads: &std::collections::VecDeque<Bytes>,
) -> ExportMetricsServiceRequest {
    let mut resource_metrics = Vec::new();
    for payload in payloads {
        match ExportMetricsServiceRequest::decode(payload.clone()) {
            Ok(req) => resource_metrics.extend(req.resource_metrics),
            Err(e) => warn!(error = %e, "skipping malformed metrics payload"),
        }
    }
    ExportMetricsServiceRequest { resource_metrics }
}

fn concat_logs(
    payloads: &std::collections::VecDeque<Bytes>,
) -> ExportLogsServiceRequest {
    let mut resource_logs = Vec::new();
    for payload in payloads {
        match ExportLogsServiceRequest::decode(payload.clone()) {
            Ok(req) => resource_logs.extend(req.resource_logs),
            Err(e) => warn!(error = %e, "skipping malformed logs payload"),
        }
    }
    ExportLogsServiceRequest { resource_logs }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::{
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans},
    };
    use flate2::read::GzDecoder;
    use std::io::Read;

    #[test]
    fn concat_combines_resource_entries() {
        let req1 = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let req2 = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let mut queue = std::collections::VecDeque::new();
        queue.push_back(Bytes::from(req1.encode_to_vec()));
        queue.push_back(Bytes::from(req2.encode_to_vec()));

        let merged = concat_traces(&queue);
        assert_eq!(merged.resource_spans.len(), 2);
    }

    #[test]
    fn concat_skips_malformed_payload() {
        let valid = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![],
                schema_url: String::new(),
            }],
        };

        let mut queue = std::collections::VecDeque::new();
        queue.push_back(Bytes::from(vec![0xFF, 0xFF, 0xFF])); // malformed
        queue.push_back(Bytes::from(valid.encode_to_vec()));

        let merged = concat_traces(&queue);
        assert_eq!(merged.resource_spans.len(), 1);
    }

    #[test]
    fn gzip_produces_valid_output() {
        let data = b"hello world, this is a test of gzip compression";
        let compressed = compress_gzip(data).unwrap();

        // Verify it's smaller or at least valid gzip
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, data);
    }
}
