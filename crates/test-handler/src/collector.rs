use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tokio::net::TcpListener;

use test_handler::CollectedExport;

pub type CollectorStore = Arc<Mutex<Vec<CollectedExport>>>;

const EXCLUDED_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "user-agent",
    "accept",
    "connection",
    "transfer-encoding",
    "content-type",
    "content-encoding",
];

pub fn start(port: u16) -> CollectorStore {
    let store: CollectorStore = Arc::new(Mutex::new(Vec::new()));
    let store_clone = store.clone();

    tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("failed to bind collector listener");

        loop {
            let (stream, _) = listener
                .accept()
                .await
                .expect("failed to accept connection");
            let store = store_clone.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let store = store.clone();
                    handle(req, store)
                });
                let _ = Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });

    store
}

async fn handle<B>(
    req: Request<B>,
    store: CollectorStore,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
{
    if req.method() != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::default())
            .unwrap());
    }

    let path = req.uri().path().to_owned();

    let content_type = req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let content_encoding = req
        .headers()
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    let mut headers = HashMap::new();
    for (name, value) in req.headers() {
        let name_str = name.as_str();
        if !EXCLUDED_HEADERS.contains(&name_str)
            && let Ok(v) = value.to_str()
        {
            headers.insert(name_str.to_owned(), v.to_owned());
        }
    }

    let body_bytes = req
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default();

    let export = CollectedExport {
        path,
        content_type,
        content_encoding,
        headers,
        body: BASE64.encode(&body_bytes),
    };

    store.lock().unwrap().push(export);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(Full::default())
        .unwrap())
}

pub async fn drain(
    store: &CollectorStore,
    timeout_ms: u64,
    min_expected: usize,
) -> Vec<CollectedExport> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout_ms);

    loop {
        {
            let current = store.lock().unwrap();
            if current.len() >= min_expected {
                break;
            }
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    store.lock().unwrap().drain(..).collect()
}
