use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::buffers::{OutboundBuffer, Signal};
use crate::config::Config;
use crate::exporter;
use crate::extensions_api::{self, ExtensionsApi, ExtensionsApiEvent};
use crate::telemetry_listener::TelemetryEvent;
use crate::{otlp_listener, telemetry_listener};

/// Owns all state for the extension's main select! loop.
///
/// Constructed in `main()` after registration, then driven by `run()`.
/// Spawned listener tasks are aborted on drop.
pub struct EventLoop<'a, A: ExtensionsApi> {
    api: &'a A,
    exporter: exporter::Exporter,
    buffer: OutboundBuffer,
    otlp_rx: mpsc::Receiver<(Signal, Bytes)>,
    telemetry_rx: mpsc::Receiver<TelemetryEvent>,
    cancel: CancellationToken,
    otlp_task: JoinHandle<()>,
    telemetry_task: JoinHandle<()>,
}

impl<'a, A: ExtensionsApi> EventLoop<'a, A> {
    /// Bind both listeners, register with the Telemetry API, and spawn
    /// the OTLP and telemetry server tasks.
    pub async fn new(api: &'a A, config: &Config) -> Result<Self, extensions_api::ApiError> {
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
            exporter: exporter::Exporter::new(config),
            buffer: OutboundBuffer::new(),
            otlp_rx,
            telemetry_rx,
            cancel,
            otlp_task,
            telemetry_task,
        })
    }

    /// Multiplexes extensions API, OTLP payloads, and telemetry events.
    ///
    /// The `next_event` future is `Box::pin`-ned outside the loop so that it
    /// survives across `select!` iterations. Without this, receiving an OTLP
    /// payload or telemetry event would cancel the in-flight long-poll to the
    /// Extensions API, leaving an orphaned HTTP request that corrupts the RIE
    /// state machine.
    pub async fn run(&mut self) {
        let mut next_event_fut = Box::pin(self.api.next_event());

        loop {
            tokio::select! {
                event = &mut next_event_fut => {
                    match event {
                        Ok(ExtensionsApiEvent::Invoke { request_id }) => {
                            debug!(request_id, "Received invoke event");
                            // TODO: record invocation metadata in state map

                            // Post-invocation flush: export buffered data from previous invocation
                            if let Err(e) = self.exporter.export(&mut self.buffer).await {
                                error!(error = %e, "flush failed");
                            }
                        }
                        Ok(ExtensionsApiEvent::Shutdown { reason }) => {
                            debug!(reason, "Received shutdown event");
                            self.cancel.cancel();

                            // Drain any payloads still in the channel
                            self.otlp_rx.close();
                            while let Some((signal, payload)) = self.otlp_rx.recv().await {
                                self.buffer.push(signal, payload);
                            }

                            if let Err(e) = self.exporter.export(&mut self.buffer).await {
                                error!(error = %e, "shutdown flush failed");
                            }
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "extensions API error");
                        }
                    }
                    next_event_fut = Box::pin(self.api.next_event());
                }
                Some((signal, payload)) = self.otlp_rx.recv() => {
                    self.buffer.push(signal, payload);
                    // TODO: race flush trigger (mid-invocation background flush)
                    // TODO: buffer size threshold flush
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
        }
    }
}

impl<A: ExtensionsApi> Drop for EventLoop<'_, A> {
    fn drop(&mut self) {
        self.otlp_task.abort();
        self.telemetry_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use tokio::sync::{Mutex, Notify};

    use super::*;
    use crate::extensions_api::ApiError;

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
        let mut event_loop = EventLoop::new(&mock, &config).await.unwrap();

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
        }

        // Spawn an assertion task that checks the call count then releases
        // the mock to deliver the SHUTDOWN event.
        let assertion = tokio::spawn(async move {
            // Give the event loop time to process the OTLP messages
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            assert_eq!(
                state.next_event_calls.load(Ordering::SeqCst),
                1,
                "next_event must be called exactly once — the future must not be dropped and recreated"
            );

            state.release.notify_one();
        });

        event_loop.run().await;
        assertion.await.unwrap();
    }
}
