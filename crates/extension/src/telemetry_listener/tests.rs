use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request, StatusCode};
use tokio::sync::mpsc;

use super::*;

fn post(body: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri("/")
        .body(Full::new(Bytes::from(body.to_owned())))
        .unwrap()
}

fn get() -> Request<Full<Bytes>> {
    Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Full::default())
        .unwrap()
}

#[tokio::test]
async fn accepts_post_and_forwards_events() {
    let (tx, mut rx) = mpsc::channel(8);
    let body = r#"[{"type":"platform.runtimeDone","time":"2024-01-01T00:00:00Z","record":{"requestId":"req-6","status":"failure"}}]"#;
    let resp = handle(post(body), tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let event = rx.recv().await.unwrap();
    assert_eq!(
        event,
        TelemetryEvent::RuntimeDone {
            request_id: "req-6".into(),
            status: "failure".into(),
        }
    );
}

#[tokio::test]
async fn rejects_non_post_with_405() {
    let (tx, _rx) = mpsc::channel(8);
    let resp = handle(get(), tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn returns_400_for_invalid_utf8() {
    let (tx, _rx) = mpsc::channel(8);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/")
        .body(Full::new(Bytes::from(vec![0xFF, 0xFE])))
        .unwrap();
    let resp = handle(req, tx).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn logs_when_channel_full() {
    let (tx, _rx) = mpsc::channel(1);
    // Fill the channel
    tx.try_send(TelemetryEvent::RuntimeDone {
        request_id: "fill".into(),
        status: "success".into(),
    })
    .unwrap();

    let body = r#"[{"type":"platform.runtimeDone","time":"2024-01-01T00:00:00Z","record":{"requestId":"overflow","status":"success"}}]"#;
    let resp = handle(post(body), tx).await.unwrap();
    // Still returns 200 — we don't fail the Lambda platform request
    assert_eq!(resp.status(), StatusCode::OK);
}
