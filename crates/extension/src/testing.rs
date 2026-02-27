use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::sync::{Mutex, Notify};

use crate::buffers::BufferData;
use crate::exporter::{ExportError, Exporter};
use crate::extensions_api::{ApiError, ExtensionsApi, ExtensionsApiEvent};

pub struct MockExporter;

impl Exporter for MockExporter {
    async fn export(&self, data: &mut BufferData) -> Result<(), ExportError> {
        data.traces.clear();
        data.metrics.clear();
        data.logs.clear();
        Ok(())
    }
}

pub struct FailingExporter;

impl Exporter for FailingExporter {
    async fn export(&self, _data: &mut BufferData) -> Result<(), ExportError> {
        Err(ExportError::Rejected {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        })
    }
}

/// Simulates partial failure: traces export succeeds, metrics fails.
pub struct PartialFailExporter;

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

pub struct SlowExporter;

impl Exporter for SlowExporter {
    async fn export(&self, data: &mut BufferData) -> Result<(), ExportError> {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        data.traces.clear();
        data.metrics.clear();
        data.logs.clear();
        Ok(())
    }
}

pub struct MockApiState {
    pub next_event_calls: AtomicU32,
    pub release: Notify,
    pub events: Mutex<VecDeque<Result<ExtensionsApiEvent, ApiError>>>,
}

pub struct MockApi {
    pub state: Arc<MockApiState>,
}

impl MockApi {
    pub fn new(events: Vec<Result<ExtensionsApiEvent, ApiError>>) -> (Self, Arc<MockApiState>) {
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

/// Bind to port 0 and return the OS-assigned port.
/// The listener is dropped, freeing the port for the caller to rebind.
pub async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

pub async fn dummy_config() -> crate::config::Config {
    let _ = rustls::crypto::ring::default_provider().install_default();
    crate::config::Config {
        endpoint: url::Url::parse("http://localhost:4318").unwrap(),
        listener_port: free_port().await,
        telemetry_port: free_port().await,
        export_timeout: std::time::Duration::from_millis(100),
        compression: crate::config::Compression::None,
        export_headers: vec![],
        buffer_max_bytes: Some(4_194_304),
        flush_strategy: crate::flush_strategy::FlushStrategy::End,
    }
}
