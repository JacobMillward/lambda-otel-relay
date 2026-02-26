use std::collections::VecDeque;

use bytes::Bytes;
use tracing::warn;

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
}

#[derive(Default)]
pub struct OutboundBuffer {
    pub traces: SignalBuffer,
    pub metrics: SignalBuffer,
    pub logs: SignalBuffer,
}

impl OutboundBuffer {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_to_traces() {
        let mut buf = OutboundBuffer::new();
        buf.push(Signal::Traces, Bytes::from("trace1"));
        buf.push(Signal::Traces, Bytes::from("trace2"));
        assert_eq!(buf.traces.queue.len(), 2);
        assert_eq!(buf.traces.size_bytes, "trace1".len() + "trace2".len());
    }

    #[test]
    fn push_to_metrics() {
        let mut buf = OutboundBuffer::new();
        buf.push(Signal::Metrics, Bytes::from("metric1"));
        assert_eq!(buf.metrics.queue.len(), 1);
        assert_eq!(buf.metrics.size_bytes, "metric1".len());
    }

    #[test]
    fn push_to_logs() {
        let mut buf = OutboundBuffer::new();
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
        let mut buf = OutboundBuffer::new();
        buf.push(Signal::Traces, Bytes::from("t1"));
        buf.push(Signal::Traces, Bytes::from("t2"));
        buf.traces.clear();
        assert!(buf.traces.queue.is_empty());
        assert_eq!(buf.traces.size_bytes, 0);
    }

    #[test]
    fn signal_buffer_is_empty() {
        let mut buf = OutboundBuffer::new();
        assert!(buf.traces.is_empty());
        buf.push(Signal::Traces, Bytes::from("t1"));
        assert!(!buf.traces.is_empty());
    }

    #[test]
    fn outbound_buffer_is_empty() {
        let mut buf = OutboundBuffer::new();
        assert!(buf.is_empty());
        buf.push(Signal::Metrics, Bytes::from("m"));
        assert!(!buf.is_empty());
    }

    #[test]
    fn total_size_bytes() {
        let mut buf = OutboundBuffer::new();
        assert_eq!(buf.total_size_bytes(), 0);
        buf.push(Signal::Traces, Bytes::from("aaaa")); // 4
        buf.push(Signal::Metrics, Bytes::from("bb")); // 2
        buf.push(Signal::Logs, Bytes::from("ccc")); // 3
        assert_eq!(buf.total_size_bytes(), 9);
    }

    #[test]
    fn evict_oldest_removes_front() {
        let mut buf = OutboundBuffer::new();
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
        let mut buf = OutboundBuffer::new();
        assert_eq!(buf.traces.evict_oldest(), 0);
    }

    #[test]
    fn evict_to_stops_at_threshold() {
        let mut buf = OutboundBuffer::new();
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
        let mut buf = OutboundBuffer::new();
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
        let mut buf = OutboundBuffer::new();
        buf.push(Signal::Traces, Bytes::from("aa")); // 2
        buf.evict_to(100);
        assert_eq!(buf.total_size_bytes(), 2);
        assert_eq!(buf.traces.queue.len(), 1);
    }

    #[test]
    fn evict_to_single_signal_only() {
        let mut buf = OutboundBuffer::new();
        buf.push(Signal::Logs, Bytes::from("aaaa")); // 4
        buf.push(Signal::Logs, Bytes::from("bbbb")); // 4
        buf.push(Signal::Logs, Bytes::from("cccc")); // 4
        // total = 12, evict to 4
        buf.evict_to(4);
        assert_eq!(buf.total_size_bytes(), 4);
        assert_eq!(buf.logs.queue.len(), 1);
        assert_eq!(buf.logs.queue[0], Bytes::from("cccc"));
    }
}
