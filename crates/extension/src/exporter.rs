use std::collections::VecDeque;
use std::io::Write;

use bytes::Bytes;
use flate2::write::GzEncoder;
use prost::Message;
use reqwest::Client;
use thiserror::Error;
use url::Url;

use crate::buffers::OutboundBuffer;
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

        let (t, m, l) = tokio::join!(
            self.export_traces(&buffer.traces.queue),
            self.export_metrics(&buffer.metrics.queue),
            self.export_logs(&buffer.logs.queue),
        );

        if t.is_ok() {
            buffer.traces.clear();
        }
        if m.is_ok() {
            buffer.metrics.clear();
        }
        if l.is_ok() {
            buffer.logs.clear();
        }

        t.and(m).and(l)
    }

    async fn export_traces(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        if queue.is_empty() {
            return Ok(());
        }
        let merged = merge::merge_traces(queue);
        self.post("v1/traces", merged.encode_to_vec()).await
    }

    async fn export_metrics(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        if queue.is_empty() {
            return Ok(());
        }
        let merged = merge::merge_metrics(queue);
        self.post("v1/metrics", merged.encode_to_vec()).await
    }

    async fn export_logs(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        if queue.is_empty() {
            return Ok(());
        }
        let merged = merge::merge_logs(queue);
        self.post("v1/logs", merged.encode_to_vec()).await
    }

    async fn post(&self, path: &str, body: Vec<u8>) -> Result<(), ExportError> {
        let url = self.endpoint.join(path).expect("invalid export path");

        let mut req = self
            .client
            .post(url)
            .header("content-type", "application/x-protobuf");

        let body = match self.compression {
            Compression::Gzip => {
                req = req.header("content-encoding", "gzip");
                compress_gzip(&body)?
            }
            Compression::None => body,
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

fn compress_gzip(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(data)?;
    encoder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    #[test]
    fn gzip_produces_valid_output() {
        let data = b"hello world, this is a test of gzip compression";
        let compressed = compress_gzip(data).unwrap();

        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, data);
    }
}
