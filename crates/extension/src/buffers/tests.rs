use std::sync::Arc;

use bytes::Bytes;

use super::*;
use crate::testing::SlowExporter;

#[test]
fn push_to_traces() {
    let mut buf = BufferData::new();
    buf.push(Signal::Traces, Bytes::from("trace1"));
    buf.push(Signal::Traces, Bytes::from("trace2"));
    assert_eq!(buf.traces.queue.len(), 2);
    assert_eq!(buf.traces.size_bytes, "trace1".len() + "trace2".len());
}

#[test]
fn push_to_metrics() {
    let mut buf = BufferData::new();
    buf.push(Signal::Metrics, Bytes::from("metric1"));
    assert_eq!(buf.metrics.queue.len(), 1);
    assert_eq!(buf.metrics.size_bytes, "metric1".len());
}

#[test]
fn push_to_logs() {
    let mut buf = BufferData::new();
    buf.push(Signal::Logs, Bytes::from("log1"));
    buf.push(Signal::Logs, Bytes::from("log2"));
    buf.push(Signal::Logs, Bytes::from("log3"));
    assert_eq!(buf.logs.queue.len(), 3);
    assert_eq!(
        buf.logs.size_bytes,
        "log1".len() + "log2".len() + "log3".len()
    );
}

#[test]
fn signal_buffer_clear() {
    let mut buf = BufferData::new();
    buf.push(Signal::Traces, Bytes::from("t1"));
    buf.push(Signal::Traces, Bytes::from("t2"));
    buf.traces.clear();
    assert!(buf.traces.queue.is_empty());
    assert_eq!(buf.traces.size_bytes, 0);
}

#[test]
fn signal_buffer_is_empty() {
    let mut buf = BufferData::new();
    assert!(buf.traces.is_empty());
    buf.push(Signal::Traces, Bytes::from("t1"));
    assert!(!buf.traces.is_empty());
}

#[test]
fn buffer_data_is_empty() {
    let mut buf = BufferData::new();
    assert!(buf.is_empty());
    buf.push(Signal::Metrics, Bytes::from("m"));
    assert!(!buf.is_empty());
}

#[test]
fn total_size_bytes() {
    let mut buf = BufferData::new();
    assert_eq!(buf.total_size_bytes(), 0);
    buf.push(Signal::Traces, Bytes::from("aaaa")); // 4
    buf.push(Signal::Metrics, Bytes::from("bb")); // 2
    buf.push(Signal::Logs, Bytes::from("ccc")); // 3
    assert_eq!(buf.total_size_bytes(), 9);
}

#[test]
fn evict_oldest_removes_front() {
    let mut buf = BufferData::new();
    buf.push(Signal::Traces, Bytes::from("first"));
    buf.push(Signal::Traces, Bytes::from("second"));
    let freed = buf.traces.evict_oldest();
    assert_eq!(freed, 5); // "first".len()
    assert_eq!(buf.traces.queue.len(), 1);
    assert_eq!(buf.traces.queue[0], Bytes::from("second"));
    assert_eq!(buf.traces.size_bytes, 6); // "second".len()
}

#[test]
fn evict_oldest_empty_returns_zero() {
    let mut buf = BufferData::new();
    assert_eq!(buf.traces.evict_oldest(), 0);
}

#[test]
fn evict_to_stops_at_threshold() {
    let mut buf = BufferData::new();
    buf.push(Signal::Traces, Bytes::from("aaaa")); // 4
    buf.push(Signal::Traces, Bytes::from("bbbb")); // 4
    buf.push(Signal::Traces, Bytes::from("cccc")); // 4
    // total = 12, evict to 8 => should drop "aaaa"
    buf.evict_to(8);
    assert_eq!(buf.total_size_bytes(), 8);
    assert_eq!(buf.traces.queue.len(), 2);
}

#[test]
fn evict_to_round_robins_across_signals() {
    let mut buf = BufferData::new();
    // 10 bytes each signal = 30 total
    buf.push(Signal::Traces, Bytes::from("tttttttttt"));
    buf.push(Signal::Metrics, Bytes::from("mmmmmmmmmm"));
    buf.push(Signal::Logs, Bytes::from("llllllllll"));
    // evict to 0 => should evict one from each per round
    buf.evict_to(0);
    assert!(buf.is_empty());
    assert_eq!(buf.total_size_bytes(), 0);
}

#[test]
fn evict_to_noop_when_under_threshold() {
    let mut buf = BufferData::new();
    buf.push(Signal::Traces, Bytes::from("aa")); // 2
    buf.evict_to(100);
    assert_eq!(buf.total_size_bytes(), 2);
    assert_eq!(buf.traces.queue.len(), 1);
}

#[test]
fn evict_to_single_signal_only() {
    let mut buf = BufferData::new();
    buf.push(Signal::Logs, Bytes::from("aaaa")); // 4
    buf.push(Signal::Logs, Bytes::from("bbbb")); // 4
    buf.push(Signal::Logs, Bytes::from("cccc")); // 4
    // total = 12, evict to 4
    buf.evict_to(4);
    assert_eq!(buf.total_size_bytes(), 4);
    assert_eq!(buf.logs.queue.len(), 1);
    assert_eq!(buf.logs.queue[0], Bytes::from("cccc"));
}

#[test]
fn prepend_preserves_order() {
    let mut current = BufferData::new();
    current.push(Signal::Traces, Bytes::from("new1"));
    current.push(Signal::Traces, Bytes::from("new2"));

    let mut older = BufferData::new();
    older.push(Signal::Traces, Bytes::from("old1"));
    older.push(Signal::Traces, Bytes::from("old2"));

    current.prepend(older);

    let items: Vec<&Bytes> = current.traces.queue.iter().collect();
    assert_eq!(
        items,
        vec![
            &Bytes::from("old1"),
            &Bytes::from("old2"),
            &Bytes::from("new1"),
            &Bytes::from("new2"),
        ]
    );
    assert_eq!(current.traces.size_bytes, 16); // 4 * 4
}

#[test]
fn prepend_empty_is_noop() {
    let mut current = BufferData::new();
    current.push(Signal::Traces, Bytes::from("data"));

    let older = BufferData::new();
    current.prepend(older);

    assert_eq!(current.traces.queue.len(), 1);
    assert_eq!(current.traces.size_bytes, 4);
}

use tokio::sync::mpsc;

#[test]
fn prepend_and_evict_drops_oldest_first() {
    let (tx, _) = mpsc::channel(1);
    let buf = OutboundBuffer::new(Some(10), tx);

    // Push 6 bytes of current data
    buf.push(Signal::Traces, Bytes::from("cur123")); // 6 bytes

    // Prepend 8 bytes of older data → total 14, max 10
    let mut older = BufferData::new();
    older.push(Signal::Traces, Bytes::from("old1")); // 4 bytes
    older.push(Signal::Traces, Bytes::from("old2")); // 4 bytes
    buf.prepend_failed(older);

    // Should have evicted oldest ("old1") to get to 10 bytes
    let data = buf.take();
    assert_eq!(data.total_size_bytes(), 10);
    let items: Vec<&Bytes> = data.traces.queue.iter().collect();
    assert_eq!(items, vec![&Bytes::from("old2"), &Bytes::from("cur123")]);
}

#[test]
fn shared_take_and_prepend_round_trip() {
    let (tx, _) = mpsc::channel(1);
    let buf = OutboundBuffer::new(None, tx);
    buf.push(Signal::Traces, Bytes::from("t1"));
    buf.push(Signal::Metrics, Bytes::from("m1"));

    let taken = buf.take();
    assert!(buf.take().is_empty());
    assert!(!taken.is_empty());

    buf.prepend_failed(taken);
    let restored = buf.take();
    assert!(!restored.is_empty());
    assert_eq!(restored.total_size_bytes(), 4); // "t1" + "m1"
}

#[tokio::test]
async fn spawn_flush_skips_if_in_flight() {
    let (tx, _) = mpsc::channel(1);
    let buffer = OutboundBuffer::new(None, tx);
    buffer.push(Signal::Traces, Bytes::from("batch1"));

    let exporter = Arc::new(SlowExporter);
    assert!(buffer.spawn_flush(&exporter));

    // Push more data and try to spawn again while first is in-flight
    buffer.push(Signal::Traces, Bytes::from("batch2"));
    assert!(!buffer.spawn_flush(&exporter)); // Should be no-op

    buffer.join_flush_task().await;

    // "batch2" should still be in the buffer since the second spawn was skipped
    let data = buffer.take();
    assert_eq!(data.traces.queue.len(), 1);
    assert_eq!(data.traces.queue[0], Bytes::from("batch2"));
}

#[tokio::test]
async fn spawn_flush_returns_true_when_spawned() {
    let (tx, _) = mpsc::channel(1);
    let buffer = OutboundBuffer::new(None, tx);
    buffer.push(Signal::Traces, Bytes::from("data"));
    let exporter = Arc::new(crate::testing::MockExporter);
    assert!(buffer.spawn_flush(&exporter));
}

#[tokio::test]
async fn spawn_flush_returns_false_when_empty() {
    let (tx, _) = mpsc::channel(1);
    let buffer = OutboundBuffer::new(None, tx);
    let exporter = Arc::new(crate::testing::MockExporter);
    assert!(!buffer.spawn_flush(&exporter));
}

#[tokio::test]
async fn flush_notifies_when_complete() {
    let (tx, mut rx) = mpsc::channel(1);

    let buffer = OutboundBuffer::new(None, tx);
    buffer.push(Signal::Traces, Bytes::from("cur123"));

    let exporter = crate::testing::MockExporter {};
    buffer.flush(&exporter).await;

    assert_eq!(
        rx.try_recv().unwrap(),
        (),
        "Expected notification for flush completion"
    );
}

#[tokio::test]
async fn spawn_flush_notifies_when_complete() {
    let (tx, mut rx) = mpsc::channel(1);

    let buffer = OutboundBuffer::new(None, tx);
    buffer.push(Signal::Traces, Bytes::from("cur123"));

    let exporter = Arc::new(crate::testing::MockExporter);
    buffer.spawn_flush(&exporter);
    buffer.join_flush_task().await;

    assert_eq!(
        rx.try_recv().unwrap(),
        (),
        "Expected notification for flush completion"
    );
}
