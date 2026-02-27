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
        let mut total = self.total_size_bytes();

        while total > max_bytes {
            let mut any_evicted = false;

            let freed = self.traces.evict_oldest();
            if freed > 0 {
                total -= freed;
                dropped_bytes[0] += freed;
                dropped_count[0] += 1;
                any_evicted = true;
            }
            if total > max_bytes {
                let freed = self.metrics.evict_oldest();
                if freed > 0 {
                    total -= freed;
                    dropped_bytes[1] += freed;
                    dropped_count[1] += 1;
                    any_evicted = true;
                }
            }
            if total > max_bytes {
                let freed = self.logs.evict_oldest();
                if freed > 0 {
                    total -= freed;
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
            warn!(
                bytes = dropped_bytes[0],
                count = dropped_count[0],
                "evicted traces data from buffer"
            );
        }
        if dropped_count[1] > 0 {
            warn!(
                bytes = dropped_bytes[1],
                count = dropped_count[1],
                "evicted metrics data from buffer"
            );
        }
        if dropped_count[2] > 0 {
            warn!(
                bytes = dropped_bytes[2],
                count = dropped_count[2],
                "evicted logs data from buffer"
            );
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

    /// Push a payload and, if over the byte threshold, try to spawn a background
    /// flush — all under a single lock acquisition.
    ///
    /// Returns `true` if a flush was spawned.
    pub fn push_and_maybe_flush<E: Exporter>(
        &self,
        signal: Signal,
        payload: Bytes,
        exporter: &Arc<E>,
    ) -> bool {
        let mut guard = self.state.lock().unwrap();
        guard.data.push(signal, payload);
        match self.max_bytes {
            Some(max) if guard.data.total_size_bytes() > max => {
                self.try_spawn_flush(&mut guard, exporter)
            }
            _ => false,
        }
    }

    /// Spawn a background flush. Returns `true` if a flush was spawned, `false` if
    /// skipped (already in-flight or buffer empty).
    pub fn spawn_flush<E: Exporter>(&self, exporter: &Arc<E>) -> bool {
        let mut guard = self.state.lock().unwrap();
        self.try_spawn_flush(&mut guard, exporter)
    }

    /// Inner spawn logic, called with the lock already held.
    fn try_spawn_flush<E: Exporter>(&self, state: &mut BufferState, exporter: &Arc<E>) -> bool {
        // Skip if a flush is already in-flight
        if let Some(handle) = state.flush_task.as_ref()
            && !handle.is_finished()
        {
            return false;
        }

        // Join the finished task to surface panics before overwriting.
        if let Some(mut handle) = state.flush_task.take() {
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            if let Poll::Ready(Err(e)) = Pin::new(&mut handle).poll(&mut cx) {
                error!(error = %e, "background flush task panicked");
            }
        }

        let mut snapshot = std::mem::take(&mut state.data);
        if snapshot.is_empty() {
            return false;
        }

        let exporter = Arc::clone(exporter);
        let buffer = self.clone();

        state.flush_task = Some(tokio::spawn(async move {
            if let Err(e) = exporter.export(&mut snapshot).await {
                error!(error = %e, "background flush failed");
            }
            // Prepend any remaining data (failed signals). No-op if export cleared everything.
            buffer.prepend_failed(snapshot);
        }));

        true
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
    /// Returns `true` if data was exported, `false` if the buffer was empty.
    pub async fn flush<E: Exporter>(&self, exporter: &E) -> bool {
        self.join_flush_task().await;
        let mut snapshot = self.take();
        if snapshot.is_empty() {
            return false;
        }
        if let Err(e) = exporter.export(&mut snapshot).await {
            error!(error = %e, "flush failed");
        }
        self.prepend_failed(snapshot);
        true
    }
}

#[cfg(test)]
mod tests;
