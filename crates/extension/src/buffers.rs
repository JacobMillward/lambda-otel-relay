use std::collections::VecDeque;

use bytes::Bytes;

#[derive(Debug, Clone, Copy)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
}

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
}

pub struct OutboundBuffer {
    pub traces: SignalBuffer,
    pub metrics: SignalBuffer,
    pub logs: SignalBuffer,
}

impl OutboundBuffer {
    pub fn new() -> Self {
        Self {
            traces: SignalBuffer {
                queue: VecDeque::new(),
                size_bytes: 0,
            },
            metrics: SignalBuffer {
                queue: VecDeque::new(),
                size_bytes: 0,
            },
            logs: SignalBuffer {
                queue: VecDeque::new(),
                size_bytes: 0,
            },
        }
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
}
