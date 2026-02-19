mod buffers;
mod config;
mod extensions_api;
mod otlp_listener;

use buffers::{OutboundBuffer, Signal};
use bytes::Bytes;
use extensions_api::{ExtensionApiClient, ExtensionsApiEvent};
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
    let (telemetry_tx, mut telemetry_rx) = mpsc::channel::<Bytes>(64);

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
    let telemetry_task = tokio::spawn(async move {
        // TODO: hyper HTTP server bound to 0.0.0.0:4319
        let _tx = telemetry_tx;
        telemetry_cancel.cancelled().await;
    });

    // TODO: Subscribe to Lambda Telemetry API

    // Event loop — multiplexes extensions API, OTLP payloads, and telemetry events
    loop {
        tokio::select! {
            event = ext.next_event() => {
                match event {
                    Ok(ExtensionsApiEvent::Invoke { request_id }) => {
                        eprintln!("invoke: requestId={request_id}");
                        // TODO: Post-invocation flush for previous invocation
                        // TODO: Record invocation metadata in state map
                    }
                    Ok(ExtensionsApiEvent::Shutdown { reason }) => {
                        eprintln!("shutdown: reason={reason}");
                        cancel.cancel();

                        // Drain any payloads still in the channel
                        otlp_rx.close();
                        while let Some((signal, payload)) = otlp_rx.recv().await {
                            buffer.push(signal, payload);
                        }

                        // TODO: Flush entire buffer to collector with shutdown timeout
                        break;
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                    }
                }
            }
            Some((signal, payload)) = otlp_rx.recv() => {
                buffer.push(signal, payload);
                // TODO: Check race flush trigger
                // TODO: Check buffer size threshold
            }
            Some(event) = telemetry_rx.recv() => {
                let _event = event;
                // TODO: Process platform.runtimeDone, platform.start
            }
        }
    }

    let _ = tokio::join!(otlp_task, telemetry_task);
}
