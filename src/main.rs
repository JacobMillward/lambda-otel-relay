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

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring provider");

    let config = config::Config::from_env().unwrap_or_else(|e| config::fatal(&e));

    let runtime_api =
        std::env::var("AWS_LAMBDA_RUNTIME_API").expect("AWS_LAMBDA_RUNTIME_API was not set");

    // Register with Extensions API
    let ext = ExtensionApiClient::register(&runtime_api)
        .await
        .expect("failed to register extension");

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

    // TODO: Subscribe to Lambda Telemetry API

    // Event loop — multiplexes extensions API, OTLP payloads, and telemetry events
    loop {
        tokio::select! {
            event = ext.next_event() => {
                match event {
                    Ok(ExtensionsApiEvent::Invoke { request_id }) => {
                        eprintln!("invoke: requestId={request_id}");
                        // TODO: record invocation metadata in state map

                        // Post-invocation flush: export buffered data from previous invocation
                        if let Err(e) = exporter::export(&config.endpoint, &mut buffer).await {
                            eprintln!("flush error: {e}");
                        }
                    }
                    Ok(ExtensionsApiEvent::Shutdown { reason }) => {
                        eprintln!("shutdown: reason={reason}");
                        cancel.cancel();

                        // Drain any payloads still in the channel
                        otlp_rx.close();
                        while let Some((signal, payload)) = otlp_rx.recv().await {
                            buffer.push(signal, payload);
                        }

                        if let Err(e) = exporter::export(&config.endpoint, &mut buffer).await {
                            eprintln!("shutdown flush error: {e}");
                        }
                        break;
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
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
                        eprintln!("runtimeDone: requestId={request_id} status={status}");
                        // TODO: update invocation state map, emit timeout log record
                    }
                    TelemetryEvent::Start { request_id, tracing_value } => {
                        let _tracing_value = tracing_value;
                        eprintln!("start: requestId={request_id}");
                        // TODO: extract X-Ray trace ID, store in state map
                    }
                }
            }
        }
    }

    let _ = tokio::join!(otlp_task, telemetry_task);
}
