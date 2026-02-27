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
use crate::extensions_api::{self, ApiError, ExitError, ExtensionsApi, ExtensionsApiEvent};
use crate::flush_strategy::{FlushCoordinator, TimerMode};
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
    flush_coordinator: FlushCoordinator,
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
            .map_err(|e| ApiError::InitFailed(format!("failed to bind OTLP listener: {e}")))?;

        let telemetry_listener = TcpListener::bind(("0.0.0.0", config.telemetry_port))
            .await
            .map_err(|e| ApiError::InitFailed(format!("failed to bind telemetry listener: {e}")))?;
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
            flush_coordinator: FlushCoordinator::new(config.flush_strategy.clone()),
            otlp_rx,
            telemetry_rx,
            cancel,
            otlp_task,
            telemetry_task,
            next_event_fut: ReusableBoxFuture::new(api.next_event()),
        })
    }

    /// Run the event loop until it receives a Shutdown event from the extensions API.
    ///
    /// Returns `Ok(())` on clean shutdown, or `Err(ExitError)` if a listener
    /// task died unexpectedly (channel closed while cancel token is not set).
    pub async fn run(&mut self) -> Result<(), ExitError> {
        loop {
            match self.tick().await {
                ControlFlow::Break(result) => return result,
                ControlFlow::Continue(()) => {}
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
    /// Returns `ControlFlow::Break(Ok(()))` on clean shutdown, or
    /// `ControlFlow::Break(Err(ExitError))` if a listener task died unexpectedly.
    async fn tick(&mut self) -> ControlFlow<Result<(), ExitError>> {
        tokio::select! {
            event = &mut self.next_event_fut => {
                match event {
                    Ok(ExtensionsApiEvent::Invoke { request_id }) => {
                        debug!(request_id, "Received invoke event");
                        if self.flush_coordinator.should_flush_at_boundary() {
                            self.buffer.flush(&*self.exporter).await;
                            self.flush_coordinator.record_flush();
                        }
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

                        return ControlFlow::Break(Ok(()));
                    }
                    Err(e) => {
                        error!(error = %e, "extensions API error");
                    }
                }
                self.next_event_fut.set(self.api.next_event());
            }
            result = self.otlp_rx.recv() => {
                match result {
                    Some((signal, payload)) => {
                        if self.buffer.push_and_maybe_flush(
                            signal, payload, &self.exporter,
                        ) {
                            self.flush_coordinator.record_flush();
                        }
                    }
                    None if !self.cancel.is_cancelled() => {
                        return ControlFlow::Break(Err(ExitError::RuntimeFailure(
                            "OTLP listener died unexpectedly".into(),
                        )));
                    }
                    None => {}
                }
            }
            result = self.telemetry_rx.recv() => {
                match result {
                    Some(event) => match event {
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
                    None if !self.cancel.is_cancelled() => {
                        return ControlFlow::Break(Err(ExitError::RuntimeFailure(
                            "telemetry listener died unexpectedly".into(),
                        )));
                    }
                    None => {}
                }
            }
            _ = self.flush_coordinator.next_tick() => {
                if self.flush_coordinator.should_flush_on_timer() {
                    match self.flush_coordinator.timer_mode() {
                        TimerMode::Sync => {
                            self.buffer.flush(&*self.exporter).await;
                            self.flush_coordinator.record_flush();
                        }
                        TimerMode::Background => {
                            if self.buffer.spawn_flush(&self.exporter) {
                                self.flush_coordinator.record_flush();
                            }
                        }
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod http_tests;
