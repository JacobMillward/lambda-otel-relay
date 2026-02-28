use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request, StatusCode};
use tokio::sync::mpsc;

use super::*;

fn post(path: &str, body: &[u8]) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .body(Full::new(Bytes::copy_from_slice(body)))
        .unwrap()
}

fn get(path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Full::default())
        .unwrap()
}

#[tokio::test]
async fn routes_traces_payload_to_channel() {
    let (tx, mut rx) = mpsc::channel(8);
    let resp = handle(post("/v1/traces", b"trace-payload"), tx)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let (signal, body) = rx.recv().await.unwrap();
    assert!(matches!(signal, Signal::Traces));
    assert_eq!(body.as_ref(), b"trace-payload");
}

#[tokio::test]
async fn routes_metrics_payload_to_channel() {
    let (tx, mut rx) = mpsc::channel(8);
    let resp = handle(post("/v1/metrics", b"metric-payload"), tx)
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let (signal, body) = rx.recv().await.unwrap();
    assert!(matches!(signal, Signal::Metrics));
    assert_eq!(body.as_ref(), b"metric-payload");
}

#[tokio::test]
async fn routes_logs_payload_to_channel() {
    let (tx, mut rx) = mpsc::channel(8);
    let resp = handle(post("/v1/logs", b"log-payload"), tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let (signal, body) = rx.recv().await.unwrap();
    assert!(matches!(signal, Signal::Logs));
    assert_eq!(body.as_ref(), b"log-payload");
}

#[tokio::test]
async fn rejects_unknown_path_with_404() {
    let (tx, _rx) = mpsc::channel(8);
    let resp = handle(post("/v1/unknown", b""), tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rejects_non_post_with_405() {
    let (tx, _rx) = mpsc::channel(8);
    let resp = handle(get("/v1/traces"), tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn returns_503_with_retry_when_channel_full() {
    let (tx, _rx) = mpsc::channel(1);
    tx.try_send((Signal::Traces, Bytes::new())).unwrap();
    let resp = handle(post("/v1/traces", b"overflow"), tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers()["Retry-After"], "1");
}

#[tokio::test]
async fn returns_502_when_channel_closed() {
    let (tx, rx) = mpsc::channel(8);
    drop(rx);
    let resp = handle(post("/v1/traces", b"orphan"), tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert!(resp.headers().get("Retry-After").is_none());
}

/// A body that always errors on poll, for testing the collect failure path.
struct FailBody;

impl hyper::body::Body for FailBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Ready(Some(Err(std::io::Error::other("bad body"))))
    }
}

#[tokio::test]
async fn returns_400_when_body_read_fails() {
    let (tx, _rx) = mpsc::channel(8);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/traces")
        .body(FailBody)
        .unwrap();
    let resp = handle(req, tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
