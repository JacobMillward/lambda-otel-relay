use std::sync::Arc;

use bytes::Bytes;

use crate::buffers::{OutboundBuffer, Signal};
use crate::testing::{FailingExporter, MockExporter, PartialFailExporter};

#[tokio::test]
async fn failed_flush_prepends_data_back() {
    let buffer = OutboundBuffer::new(Some(1_000_000));
    buffer.push(Signal::Traces, Bytes::from("trace_data"));
    buffer.push(Signal::Metrics, Bytes::from("metric_data"));

    let exporter = Arc::new(FailingExporter);
    buffer.spawn_flush(&exporter);
    buffer.join_flush_task().await;

    // Data should be prepended back since export failed
    let data = buffer.take();
    assert!(!data.is_empty());
    assert_eq!(data.traces.queue.len(), 1);
    assert_eq!(data.traces.queue[0], Bytes::from("trace_data"));
    assert_eq!(data.metrics.queue.len(), 1);
    assert_eq!(data.metrics.queue[0], Bytes::from("metric_data"));
}

#[tokio::test]
async fn sync_flush_joins_background_task() {
    let buffer = OutboundBuffer::new(None);
    buffer.push(Signal::Traces, Bytes::from("data"));

    let exporter = Arc::new(MockExporter);

    // Spawn a background flush
    buffer.spawn_flush(&exporter);

    // Push more data while background flush is (potentially) in-flight
    buffer.push(Signal::Logs, Bytes::from("new_data"));

    // Sync flush should join the background task, then flush remaining
    buffer.flush(&*exporter).await;

    assert!(buffer.take().is_empty());
}

#[tokio::test]
async fn partial_failure_preserves_only_failed_signals() {
    let buffer = OutboundBuffer::new(None);
    buffer.push(Signal::Traces, Bytes::from("trace_data"));
    buffer.push(Signal::Metrics, Bytes::from("metric_data"));
    buffer.push(Signal::Logs, Bytes::from("log_data"));

    let exporter = Arc::new(PartialFailExporter);
    buffer.spawn_flush(&exporter);
    buffer.join_flush_task().await;

    // Only metrics should remain — traces and logs were cleared by the exporter
    let data = buffer.take();
    assert!(data.traces.is_empty());
    assert_eq!(data.metrics.queue.len(), 1);
    assert_eq!(data.metrics.queue[0], Bytes::from("metric_data"));
    assert!(data.logs.is_empty());
}
