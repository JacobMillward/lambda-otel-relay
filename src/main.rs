mod buffers;
mod config;
mod exporter;
mod extensions_api;
mod otlp_listener;
mod telemetry_listener;

use buffers::{OutboundBuffer, Signal};
use bytes::Bytes;
use extensions_api::{ExtensionApiClient, ExtensionsApiEvent};
use telemetry_listener::TelemetryEvent;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

/// Operational init failure — log and exit.
/// Use `expect` instead for programming invariants (bugs).
fn fatal(msg: &str, error: &dyn std::fmt::Display) -> ! {
    error!(%error, "{msg}");
    std::process::exit(1);
}

fn setup_logging() {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::prelude::*;

    let level = std::env::var("LAMBDA_OTEL_RELAY_LOG_LEVEL")
        .ok()
        .and_then(|val| {
            val.parse::<LevelFilter>().ok().or_else(|| {
                eprintln!("invalid LAMBDA_OTEL_RELAY_LOG_LEVEL: {val:?}, defaulting to WARN");
                None
            })
        })
        .unwrap_or(LevelFilter::WARN);

    tracing_subscriber::registry()
        .with(level)
        .with(tracing_microjson::JsonLayer::new(std::io::stderr).with_target(false))
        .init();
}

fn setup_rustls() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring provider");
}

#[tokio::main]
async fn main() {
    setup_logging();
    setup_rustls();

    let config = config::Config::from_env().unwrap_or_else(|e| fatal("config error", &e));

    let runtime_api = std::env::var("AWS_LAMBDA_RUNTIME_API")
        .unwrap_or_else(|e| fatal("AWS_LAMBDA_RUNTIME_API not set in the environment. This extension must be run within a Lambda environment.", &e));

    // Register with Extensions API
    let ext = ExtensionApiClient::register(&runtime_api)
        .await
        .unwrap_or_else(|e| fatal("failed to register extension", &e));

    let cancel = CancellationToken::new();
    let mut buffer = OutboundBuffer::new();

    // OTLP listener → buffer
    let (otlp_tx, mut otlp_rx) = mpsc::channel::<(Signal, Bytes)>(128);

    // Telemetry API listener → event processor
    let (telemetry_tx, mut telemetry_rx) = mpsc::channel::<TelemetryEvent>(64);

    // Task 1: OTLP listener on localhost:4318
    let otlp_cancel = cancel.clone();
    let otlp_task = tokio::spawn(otlp_listener::serve(
        config.listener_port,
        otlp_tx,
        otlp_cancel,
    ));

    // Task 2: Telemetry API listener on 0.0.0.0:4319
    // Receives platform events (platform.runtimeDone, platform.start) from Lambda
    let telemetry_cancel = cancel.clone();
    let telemetry_task = tokio::spawn(telemetry_listener::serve(
        config.telemetry_port,
        telemetry_tx,
        telemetry_cancel,
    ));

    // Event loop — multiplexes extensions API, OTLP payloads, and telemetry events
    loop {
        tokio::select! {
            event = ext.next_event() => {
                match event {
                    Ok(ExtensionsApiEvent::Invoke { request_id }) => {
                        debug!(request_id, "Received invoke event");
                        // TODO: record invocation metadata in state map

                        // Post-invocation flush: export buffered data from previous invocation
                        if let Err(e) = exporter::export(&config.endpoint, &mut buffer).await {
                            error!(error = %e, "flush failed");
                        }
                    }
                    Ok(ExtensionsApiEvent::Shutdown { reason }) => {
                        debug!(reason, "Received shutdown event");
                        cancel.cancel();

                        // Drain any payloads still in the channel
                        otlp_rx.close();
                        while let Some((signal, payload)) = otlp_rx.recv().await {
                            buffer.push(signal, payload);
                        }

                        if let Err(e) = exporter::export(&config.endpoint, &mut buffer).await {
                            error!(error = %e, "shutdown flush failed");
                        }
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, "extensions API error");
                    }
                }
            }
            Some((signal, payload)) = otlp_rx.recv() => {
                buffer.push(signal, payload);
                // TODO: race flush trigger (mid-invocation background flush)
                // TODO: buffer size threshold flush
            }
            Some(event) = telemetry_rx.recv() => {
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

    let _ = tokio::join!(otlp_task, telemetry_task);
}
