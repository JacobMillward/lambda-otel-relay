use std::ops::ControlFlow;
use std::sync::Arc;

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, ReusableBoxFuture};
use tracing::{debug, error};

use crate::buffers::{OutboundBuffer, Signal};
use crate::config::Config;
use crate::exporter::Exporter;
use crate::extensions_api::{self, ApiError, ExtensionsApi, ExtensionsApiEvent};
use crate::telemetry_listener::TelemetryEvent;
use crate::{otlp_listener, telemetry_listener};

/// Owns all state for the extension's main select! loop.
///
/// Constructed in `main()` after registration, then driven by `run()`.
/// Listener tasks are joined during shutdown to allow in-flight handlers to complete.
pub struct EventLoop<'a, A: ExtensionsApi, E: Exporter> {
    api: &'a A,
    exporter: Arc<E>,
    buffer: OutboundBuffer,
    otlp_rx: mpsc::Receiver<(Signal, Bytes)>,
    telemetry_rx: mpsc::Receiver<TelemetryEvent>,
    cancel: CancellationToken,
    otlp_task: JoinHandle<()>,
    telemetry_task: JoinHandle<()>,
    next_event_fut: ReusableBoxFuture<'a, Result<ExtensionsApiEvent, ApiError>>,
}

impl<'a, A: ExtensionsApi, E: Exporter> EventLoop<'a, A, E> {
    /// Bind both listeners, register with the Telemetry API, and spawn
    /// the OTLP and telemetry server tasks.
    pub async fn new(
        api: &'a A,
        exporter: E,
        config: &Config,
    ) -> Result<Self, extensions_api::ApiError> {
        let cancel = CancellationToken::new();
        let (otlp_tx, otlp_rx) = mpsc::channel::<(Signal, Bytes)>(128);
        let (telemetry_tx, telemetry_rx) = mpsc::channel::<TelemetryEvent>(64);

        let otlp_listener = TcpListener::bind(("127.0.0.1", config.listener_port))
            .await
            .expect("failed to bind OTLP listener");

        let telemetry_listener = TcpListener::bind(("0.0.0.0", config.telemetry_port))
            .await
            .expect("failed to bind telemetry listener");
        api.register_telemetry(config.telemetry_port).await?;

        let otlp_task = tokio::spawn(otlp_listener::serve(otlp_listener, otlp_tx, cancel.clone()));
        let telemetry_task = tokio::spawn(telemetry_listener::serve(
            telemetry_listener,
            telemetry_tx,
            cancel.clone(),
        ));

        Ok(Self {
            api,
            exporter: Arc::new(exporter),
            buffer: OutboundBuffer::new(config.buffer_max_bytes),
            otlp_rx,
            telemetry_rx,
            cancel,
            otlp_task,
            telemetry_task,
            next_event_fut: ReusableBoxFuture::new(api.next_event()),
        })
    }

    /// Run the event loop until it receives a Shutdown event from the extensions API.
    pub async fn run(&mut self) {
        loop {
            if let ControlFlow::Break(()) = self.tick().await {
                break;
            }
        }
    }

    /// Run one tick of the event loop
    ///
    /// Multiplexes extensions API, OTLP payloads, and telemetry events.
    ///
    /// The `next_event` future is boxed and pinned on the event loop so that it
    /// survives across `select!` iterations. Without this, receiving an OTLP
    /// payload or telemetry event would cancel the in-flight long-poll to the
    /// Extensions API, leaving an orphaned HTTP request that corrupts the RIE
    /// state machine.
    ///
    /// Returns `ControlFlow::Break(())` when it receives a Shutdown event from the lambda extension API. Otherwise
    /// returns `ControlFlow::Continue(())`.
    async fn tick(&mut self) -> ControlFlow<()> {
        tokio::select! {
            event = &mut self.next_event_fut => {
                match event {
                    Ok(ExtensionsApiEvent::Invoke { request_id }) => {
                        debug!(request_id, "Received invoke event");
                        self.buffer.flush(&*self.exporter).await;
                    }
                    Ok(ExtensionsApiEvent::Shutdown { reason }) => {
                        debug!(reason, "Received shutdown event");
                        self.cancel.cancel();
                        self.buffer.join_flush_task().await;

                        // Wait for listener tasks to finish in-flight handlers.
                        // Once they exit, their channel senders are dropped.
                        let _ = (&mut self.otlp_task).await;
                        let _ = (&mut self.telemetry_task).await;

                        // Drain any payloads still in the channel
                        while let Ok((signal, payload)) = self.otlp_rx.try_recv() {
                            self.buffer.push(signal, payload);
                        }

                        // Best-effort final flush. prepend_failed inside flush is
                        // harmless — the buffer is about to be dropped.
                        self.buffer.flush(&*self.exporter).await;

                        return ControlFlow::Break(());
                    }
                    Err(e) => {
                        error!(error = %e, "extensions API error");
                    }
                }
                self.next_event_fut.set(self.api.next_event());
            }
            Some((signal, payload)) = self.otlp_rx.recv() => {
                self.buffer.push(signal, payload);
                if self.buffer.over_threshold() {
                    self.buffer.spawn_flush(&self.exporter);
                }
            }
            Some(event) = self.telemetry_rx.recv() => {
                match event {
                    TelemetryEvent::RuntimeDone { request_id, status } => {
                        debug!(request_id, status, "Received runtimeDone event");
                        // TODO: update invocation state map, emit timeout log record
                    }
                    TelemetryEvent::Start { request_id, tracing_value } => {
                        let _tracing_value = tracing_value;
                        debug!(request_id, "Received start event");
                        // TODO: extract X-Ray trace ID, store in state map
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::sync::{Mutex, Notify};

    use super::*;
    use crate::buffers::BufferData;
    use crate::exporter::{ExportError, Exporter};
    use crate::extensions_api::ApiError;

    struct MockExporter;

    impl Exporter for MockExporter {
        async fn export(&self, data: &mut BufferData) -> Result<(), ExportError> {
            data.traces.clear();
            data.metrics.clear();
            data.logs.clear();
            Ok(())
        }
    }

    struct FailingExporter;

    impl Exporter for FailingExporter {
        async fn export(&self, _data: &mut BufferData) -> Result<(), ExportError> {
            Err(ExportError::Rejected {
                status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            })
        }
    }

    /// Simulates partial failure: traces export succeeds, metrics fails.
    struct PartialFailExporter;

    impl Exporter for PartialFailExporter {
        async fn export(&self, data: &mut BufferData) -> Result<(), ExportError> {
            // Traces succeed — clear them
            data.traces.clear();
            // Metrics fail — leave them untouched
            // Logs succeed — clear them
            data.logs.clear();
            Err(ExportError::Rejected {
                status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            })
        }
    }

    struct MockApiState {
        next_event_calls: AtomicU32,
        release: Notify,
        events: Mutex<VecDeque<Result<ExtensionsApiEvent, ApiError>>>,
    }

    struct MockApi {
        state: Arc<MockApiState>,
    }

    impl MockApi {
        fn new(events: Vec<Result<ExtensionsApiEvent, ApiError>>) -> (Self, Arc<MockApiState>) {
            let state = Arc::new(MockApiState {
                next_event_calls: AtomicU32::new(0),
                release: Notify::new(),
                events: Mutex::new(events.into()),
            });
            (
                Self {
                    state: Arc::clone(&state),
                },
                state,
            )
        }
    }

    impl ExtensionsApi for MockApi {
        async fn next_event(&self) -> Result<ExtensionsApiEvent, ApiError> {
            self.state.next_event_calls.fetch_add(1, Ordering::SeqCst);
            self.state.release.notified().await;
            self.state
                .events
                .lock()
                .await
                .pop_front()
                .expect("MockApi: no events left")
        }

        async fn register_telemetry(&self, _port: u16) -> Result<(), ApiError> {
            Ok(())
        }
    }

    fn dummy_config() -> Config {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Config {
            endpoint: url::Url::parse("http://localhost:4318").unwrap(),
            listener_port: 14318,
            telemetry_port: 14319,
            export_timeout: std::time::Duration::from_millis(100),
            compression: crate::config::Compression::None,
            export_headers: vec![],
            buffer_max_bytes: Some(4_194_304),
        }
    }

    /// The event loop must `Box::pin` the `next_event` future so that it
    /// survives across `select!` iterations when another branch fires.
    ///
    /// This test drives the real `EventLoop::run()` with a mock API that
    /// blocks on `next_event` until explicitly released. We send OTLP
    /// payloads via HTTP so they're processed while the next_event future
    /// is still pending. The assertion task then verifies `next_event`
    /// was called exactly once (the future wasn't dropped and recreated)
    /// before releasing it to deliver a SHUTDOWN event.
    #[tokio::test]
    async fn next_event_future_not_dropped_by_channel_activity() {
        let (mock, state) = MockApi::new(vec![Ok(ExtensionsApiEvent::Shutdown {
            reason: "test".into(),
        })]);

        let config = dummy_config();
        let mut event_loop = EventLoop::new(&mock, MockExporter, &config).await.unwrap();

        // Send 2 OTLP payloads via HTTP to trigger the channel branch of select!.
        // The listener is already bound and accepting, so these are queued
        // in the channel buffer before run() starts.
        let client = reqwest::Client::new();
        for _ in 0..2 {
            let resp = client
                .post(format!(
                    "http://127.0.0.1:{}/v1/traces",
                    config.listener_port
                ))
                .body(b"\x0a\x00".to_vec())
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);

            // Process OTLP payload
            let _ = event_loop.tick().await;
        }

        // Release the mock to deliver SHUTDOWN event on next tick
        state.release.notify_one();
        let _ = event_loop.tick().await;

        assert_eq!(
            state.next_event_calls.load(Ordering::SeqCst),
            1,
            "next_event must be called exactly once — the future must not be dropped and recreated"
        );
    }

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

    /// Verify that the OTLP receive → threshold → spawn_flush path works
    /// end-to-end through `tick()`.
    #[tokio::test]
    async fn threshold_triggers_background_flush_via_tick() {
        let (mock, state) = MockApi::new(vec![Ok(ExtensionsApiEvent::Shutdown {
            reason: "test".into(),
        })]);

        // Use different ports to avoid collisions with other event loop tests
        let mut config = dummy_config();
        config.listener_port = 14320;
        config.telemetry_port = 14321;
        config.buffer_max_bytes = Some(1);

        let mut event_loop = EventLoop::new(&mock, MockExporter, &config).await.unwrap();

        // Send a payload that exceeds the 1-byte threshold
        let client = reqwest::Client::new();
        let resp = client
            .post(format!(
                "http://127.0.0.1:{}/v1/traces",
                config.listener_port
            ))
            .body(b"\x0a\x00".to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // This tick receives the payload and should trigger spawn_flush
        let _ = event_loop.tick().await;

        // Send shutdown to cleanly exit
        state.release.notify_one();
        let _ = event_loop.tick().await;

        // Buffer should be empty — the background flush exported everything
        assert!(event_loop.buffer.take().is_empty());
    }
}
