mod buffers;
mod config;
mod event_loop;
mod exporter;
mod extensions_api;
mod merge;
mod otlp_listener;
mod proto;
mod telemetry_listener;

use event_loop::EventLoop;
use extensions_api::ExtensionApiClient;
use tracing::error;

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
        .with(tracing_microjson::JsonLayer::new(std::io::stderr).with_target(true))
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

    let exporter = exporter::OtlpExporter::new(&config);
    let mut event_loop = EventLoop::new(&ext, exporter, &config)
        .await
        .unwrap_or_else(|e| fatal("failed to start event loop", &e));

    event_loop.run().await;
}
