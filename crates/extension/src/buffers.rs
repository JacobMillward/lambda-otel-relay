use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::{error, warn};

use crate::exporter::Exporter;

#[derive(Debug, Clone, Copy)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
}

#[derive(Default)]
pub struct SignalBuffer {
    pub queue: VecDeque<Bytes>,
    pub size_bytes: usize,
}

impl SignalBuffer {
    pub fn clear(&mut self) {
        self.queue.clear();
        self.size_bytes = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Remove the oldest entry from the queue. Returns the number of bytes freed.
    pub fn evict_oldest(&mut self) -> usize {
        if let Some(entry) = self.queue.pop_front() {
            let freed = entry.len();
            self.size_bytes -= freed;
            freed
        } else {
            0
        }
    }

    /// Prepend older data in front of current data.
    /// After this call, `self` contains `[older..., self...]`.
    fn prepend(&mut self, mut older: SignalBuffer) {
        // append drains self.queue into older.queue, leaving self.queue empty.
        // self.size_bytes is stale after this line, but `*self = older` below
        // replaces self entirely so the stale value is never observed.
        older.queue.append(&mut self.queue);
        older.size_bytes += self.size_bytes;
        *self = older;
    }
}

#[derive(Default)]
pub struct BufferData {
    pub traces: SignalBuffer,
    pub metrics: SignalBuffer,
    pub logs: SignalBuffer,
}

impl BufferData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.traces.is_empty() && self.metrics.is_empty() && self.logs.is_empty()
    }

    pub fn push(&mut self, signal: Signal, payload: Bytes) {
        let buf = match signal {
            Signal::Traces => &mut self.traces,
            Signal::Metrics => &mut self.metrics,
            Signal::Logs => &mut self.logs,
        };
        buf.size_bytes += payload.len();
        buf.queue.push_back(payload);
    }

    pub fn total_size_bytes(&self) -> usize {
        self.traces.size_bytes + self.metrics.size_bytes + self.logs.size_bytes
    }

    /// Prepend older data in front of current data for all signals.
    pub fn prepend(&mut self, older: Self) {
        self.traces.prepend(older.traces);
        self.metrics.prepend(older.metrics);
        self.logs.prepend(older.logs);
    }

    /// Round-robin evict oldest entries (traces -> metrics -> logs -> repeat)
    /// until total size is at or below `max_bytes`, or all queues are empty.
    pub fn evict_to(&mut self, max_bytes: usize) {
        let mut dropped_bytes = [0usize; 3]; // traces, metrics, logs
        let mut dropped_count = [0usize; 3];

        while self.total_size_bytes() > max_bytes {
            let mut any_evicted = false;

            if self.total_size_bytes() > max_bytes {
                let freed = self.traces.evict_oldest();
                if freed > 0 {
                    dropped_bytes[0] += freed;
                    dropped_count[0] += 1;
                    any_evicted = true;
                }
            }
            if self.total_size_bytes() > max_bytes {
                let freed = self.metrics.evict_oldest();
                if freed > 0 {
                    dropped_bytes[1] += freed;
                    dropped_count[1] += 1;
                    any_evicted = true;
                }
            }
            if self.total_size_bytes() > max_bytes {
                let freed = self.logs.evict_oldest();
                if freed > 0 {
                    dropped_bytes[2] += freed;
                    dropped_count[2] += 1;
                    any_evicted = true;
                }
            }

            if !any_evicted {
                break;
            }
        }

        if dropped_count[0] > 0 {
            warn!(bytes = dropped_bytes[0], count = dropped_count[0], "evicted traces data from buffer");
        }
        if dropped_count[1] > 0 {
            warn!(bytes = dropped_bytes[1], count = dropped_count[1], "evicted metrics data from buffer");
        }
        if dropped_count[2] > 0 {
            warn!(bytes = dropped_bytes[2], count = dropped_count[2], "evicted logs data from buffer");
        }
    }
}

/// Internal state behind the single mutex — data and flush task together,
/// so there is no lock ordering to get wrong.
struct BufferState {
    data: BufferData,
    flush_task: Option<JoinHandle<()>>,
}

/// Shared wrapper around `BufferData` that manages flush lifecycle.
///
/// Uses `std::sync::Mutex` (not tokio) because the lock is never held across
/// `.await` — all operations are sub-microsecond field swaps.
#[derive(Clone)]
pub struct OutboundBuffer {
    state: Arc<Mutex<BufferState>>,
    max_bytes: Option<usize>,
}

impl OutboundBuffer {
    pub fn new(max_bytes: Option<usize>) -> Self {
        Self {
            state: Arc::new(Mutex::new(BufferState {
                data: BufferData::new(),
                flush_task: None,
            })),
            max_bytes,
        }
    }

    pub fn push(&self, signal: Signal, payload: Bytes) {
        self.state.lock().unwrap().data.push(signal, payload);
    }

    /// Returns true if `max_bytes` is set and the buffer exceeds it.
    pub fn over_threshold(&self) -> bool {
        match self.max_bytes {
            Some(max) => self.state.lock().unwrap().data.total_size_bytes() > max,
            None => false,
        }
    }

    /// Take all data out of the buffer, leaving it empty.
    pub fn take(&self) -> BufferData {
        std::mem::take(&mut self.state.lock().unwrap().data)
    }

    /// Prepend failed export data back into the buffer and evict if over capacity.
    /// No-op if `data` is empty.
    fn prepend_failed(&self, data: BufferData) {
        if data.is_empty() {
            return;
        }
        let mut guard = self.state.lock().unwrap();
        guard.data.prepend(data);
        if let Some(max) = self.max_bytes {
            guard.data.evict_to(max);
        }
    }

    /// Spawn a background flush. No-op if a flush is already in-flight or buffer is empty.
    pub fn spawn_flush<E: Exporter>(&self, exporter: &Arc<E>) {
        let mut guard = self.state.lock().unwrap();

        // Skip if a flush is already in-flight
        if let Some(handle) = guard.flush_task.as_ref()
            && !handle.is_finished()
        {
            return;
        }

        // Join the finished task to surface panics before overwriting.
        if let Some(mut handle) = guard.flush_task.take() {
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            if let Poll::Ready(Err(e)) = Pin::new(&mut handle).poll(&mut cx) {
                error!(error = %e, "background flush task panicked");
            }
        }

        let mut snapshot = std::mem::take(&mut guard.data);
        if snapshot.is_empty() {
            return;
        }

        let exporter = Arc::clone(exporter);
        let buffer = self.clone();

        guard.flush_task = Some(tokio::spawn(async move {
            if let Err(e) = exporter.export(&mut snapshot).await {
                error!(error = %e, "background flush failed");
            }
            // Prepend any remaining data (failed signals). No-op if export cleared everything.
            buffer.prepend_failed(snapshot);
        }));
    }

    /// Join any in-flight background flush to completion.
    pub async fn join_flush_task(&self) {
        let handle = self.state.lock().unwrap().flush_task.take();
        if let Some(h) = handle
            && let Err(e) = h.await
        {
            error!(error = %e, "background flush task panicked");
        }
    }

    /// Synchronous flush: join in-flight background flush, then take + export + handle failures.
    pub async fn flush<E: Exporter>(&self, exporter: &E) {
        self.join_flush_task().await;
        let mut snapshot = self.take();
        if snapshot.is_empty() {
            return;
        }
        if let Err(e) = exporter.export(&mut snapshot).await {
            error!(error = %e, "flush failed");
        }
        self.prepend_failed(snapshot);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::exporter::{ExportError, Exporter};

    use super::*;

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

    #[test]
    fn prepend_and_evict_drops_oldest_first() {
        let buf = OutboundBuffer::new(Some(10));

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
        assert_eq!(
            items,
            vec![&Bytes::from("old2"), &Bytes::from("cur123")]
        );
    }

    #[test]
    fn shared_take_and_prepend_round_trip() {
        let buf = OutboundBuffer::new(None);
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

    #[test]
    fn shared_over_threshold() {
        // No max_bytes → never over threshold
        let buf = OutboundBuffer::new(None);
        buf.push(Signal::Traces, Bytes::from("data"));
        assert!(!buf.over_threshold());

        // With max_bytes, under threshold
        let buf = OutboundBuffer::new(Some(100));
        buf.push(Signal::Traces, Bytes::from("data"));
        assert!(!buf.over_threshold());

        // With max_bytes, over threshold
        let buf = OutboundBuffer::new(Some(2));
        buf.push(Signal::Traces, Bytes::from("data"));
        assert!(buf.over_threshold());
    }

    struct SlowExporter;

    impl Exporter for SlowExporter {
        async fn export(&self, data: &mut BufferData) -> Result<(), ExportError> {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            data.traces.clear();
            data.metrics.clear();
            data.logs.clear();
            Ok(())
        }
    }

    #[tokio::test]
    async fn spawn_flush_skips_if_in_flight() {
        let buffer = OutboundBuffer::new(None);
        buffer.push(Signal::Traces, Bytes::from("batch1"));

        let exporter = Arc::new(SlowExporter);
        buffer.spawn_flush(&exporter);

        // Push more data and try to spawn again while first is in-flight
        buffer.push(Signal::Traces, Bytes::from("batch2"));
        buffer.spawn_flush(&exporter); // Should be no-op

        buffer.join_flush_task().await;

        // "batch2" should still be in the buffer since the second spawn was skipped
        let data = buffer.take();
        assert_eq!(data.traces.queue.len(), 1);
        assert_eq!(data.traces.queue[0], Bytes::from("batch2"));
    }
}
