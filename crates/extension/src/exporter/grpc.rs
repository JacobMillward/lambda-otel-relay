use std::collections::VecDeque;

use bytes::Bytes;
use prost::Message;

use super::{CommonExporter, ExportError, Exporter, compress_gzip};
use crate::buffers::BufferData;
use crate::config::Compression;
use crate::grpc as grpc_codec;
use crate::merge;

pub struct GrpcExporter(pub(super) CommonExporter);

impl GrpcExporter {
    async fn export_traces(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        match queue.len() {
            0 => Ok(()),
            1 => self.post_bytes(grpc_codec::TRACES_PATH, &queue[0]).await,
            _ => {
                self.post(grpc_codec::TRACES_PATH, &merge::merge_traces(queue))
                    .await
            }
        }
    }

    async fn export_metrics(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        match queue.len() {
            0 => Ok(()),
            1 => self.post_bytes(grpc_codec::METRICS_PATH, &queue[0]).await,
            _ => {
                self.post(grpc_codec::METRICS_PATH, &merge::merge_metrics(queue))
                    .await
            }
        }
    }

    async fn export_logs(&self, queue: &VecDeque<Bytes>) -> Result<(), ExportError> {
        match queue.len() {
            0 => Ok(()),
            1 => self.post_bytes(grpc_codec::LOGS_PATH, &queue[0]).await,
            _ => {
                self.post(grpc_codec::LOGS_PATH, &merge::merge_logs(queue))
                    .await
            }
        }
    }

    async fn post_bytes(&self, path: &str, protobuf: &[u8]) -> Result<(), ExportError> {
        self.post_body(path, protobuf).await
    }

    async fn post(&self, path: &str, msg: &impl Message) -> Result<(), ExportError> {
        self.post_body(path, &msg.encode_to_vec()).await
    }

    async fn post_body(&self, path: &str, protobuf: &[u8]) -> Result<(), ExportError> {
        // gRPC paths are absolute — join with the authority only.
        let url = {
            let mut u = self.0.endpoint.clone();
            u.set_path(path);
            u
        };

        let mut headers = vec![
            ("content-type".to_owned(), "application/grpc".to_owned()),
            ("te".to_owned(), "trailers".to_owned()),
        ];

        let body = match self.0.compression {
            Compression::Gzip => {
                headers.push(("grpc-encoding".to_owned(), "gzip".to_owned()));
                let compressed = compress_gzip(protobuf)?;
                grpc_codec::encode_frame(true, &compressed)?
            }
            Compression::None => grpc_codec::encode_frame(false, protobuf)?,
        };

        let resp = self.0.send(&url, headers, body.to_vec()).await?;

        if !resp.status.is_success() {
            return Err(ExportError::Rejected {
                status: resp.status,
            });
        }

        grpc_codec::check_grpc_status(resp.trailers.as_ref())?;
        Ok(())
    }
}

impl Exporter for GrpcExporter {
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
