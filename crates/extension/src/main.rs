mod buffers;
mod config;
mod event_loop;
mod exporter;
mod extensions_api;
mod flush_strategy;
mod merge;
mod otlp_listener;
mod proto;
mod telemetry_listener;

#[cfg(test)]
mod testing;

use event_loop::EventLoop;
use extensions_api::{ExtensionApiClient, InitError};
use tracing::error;

/// Exceptional init failure — log and exit.
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

    let ext = ExtensionApiClient::register()
        .await
        .unwrap_or_else(|e| fatal("failed to register extension", &e));

    // Config parsing moved after registration so errors can be reported
    let config = match config::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            let err = InitError::from(e);
            error!(%err, "config error");
            ext.report_init_error(&err).await;
            std::process::exit(1);
        }
    };

    let exporter = match exporter::OtlpExporter::new(&config) {
        Ok(e) => e,
        Err(e) => {
            let err = InitError::from(e);
            error!(%err, "TLS configuration error");
            ext.report_init_error(&err).await;
            std::process::exit(1);
        }
    };
    let mut event_loop = match EventLoop::new(&ext, exporter, &config).await {
        Ok(el) => el,
        Err(e) => {
            let err = InitError::ListenerBind(e);
            error!(%err, "failed to start event loop");
            ext.report_init_error(&err).await;
            std::process::exit(1);
        }
    };

    match event_loop.run().await {
        Ok(()) => {}
        Err(e) => {
            error!(%e, "runtime error");
            ext.report_exit_error(&e).await;
            std::process::exit(1);
        }
    }
}
