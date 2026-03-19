use std::collections::VecDeque;

use bytes::Bytes;
use prost::Message;

use super::{CommonExporter, ExportError, Exporter, compress_gzip};
use crate::buffers::BufferData;
use crate::config::Compression;
use crate::merge;

pub struct HttpProtobufExporter(pub(super) CommonExporter);

impl HttpProtobufExporter {
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

    async fn post_bytes(&self, path: &str, protobuf: &[u8]) -> Result<(), ExportError> {
        self.post_body(path, protobuf).await
    }

    async fn post(&self, path: &str, msg: &impl Message) -> Result<(), ExportError> {
        self.post_body(path, &msg.encode_to_vec()).await
    }

    async fn post_body(&self, path: &str, protobuf: &[u8]) -> Result<(), ExportError> {
        let url = self.0.endpoint.join(path).expect("invalid export path");

        let mut headers = vec![(
            "content-type".to_owned(),
            "application/x-protobuf".to_owned(),
        )];

        let body = match self.0.compression {
            Compression::Gzip => {
                headers.push(("content-encoding".to_owned(), "gzip".to_owned()));
                compress_gzip(protobuf)?
            }
            Compression::None => protobuf.to_vec(),
        };

        let resp = self.0.send(&url, headers, body).await?;

        if resp.status.is_success() {
            Ok(())
        } else {
            Err(ExportError::Rejected {
                status: resp.status,
            })
        }
    }
}

impl Exporter for HttpProtobufExporter {
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
