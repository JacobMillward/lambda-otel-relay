mod events;

pub use events::TelemetryEvent;

use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn response(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::default())
        .unwrap()
}

/// Validate the incoming request and return the body as a string.
async fn validate<B>(req: Request<B>) -> Result<String, (StatusCode, String)>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
{
    let method = req.method().clone();

    if method != Method::POST {
        return Err((
            StatusCode::METHOD_NOT_ALLOWED,
            format!("{method} not allowed"),
        ));
    }

    let body = req
        .collect()
        .await
        .map_err(|_| (StatusCode::BAD_REQUEST, "failed to read body".to_owned()))?;

    String::from_utf8(body.to_bytes().to_vec())
        .map_err(|_| (StatusCode::BAD_REQUEST, "body is not valid UTF-8".to_owned()))
}

/// Handle a batch of telemetry events from the Lambda platform.
/// Lambda POSTs a JSON array of Event objects to this endpoint.
/// We always respond 200 — Lambda does not retry on failure.
/// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-api.html
async fn handle<B>(
    req: Request<B>,
    tx: mpsc::Sender<TelemetryEvent>,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
{
    let body_str = match validate(req).await {
        Ok(s) => s,
        Err((status, reason)) => {
            tracing::warn!(reason, "telemetry request rejected");
            return Ok(response(status));
        }
    };

    let events = TelemetryEvent::parse_batch(&body_str);
    for event in events {
        if let Err(e) = tx.try_send(event) {
            tracing::warn!(error = %e, "telemetry event dropped");
        }
    }

    Ok(response(StatusCode::OK))
}

/// Telemetry API listener on 0.0.0.0:<port>.
/// Receives platform events (platform.runtimeDone, platform.start) from the
/// Lambda platform. Must be bound to 0.0.0.0, not localhost, to be reachable
/// by the Lambda sandbox.
/// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-api-reference.html
pub async fn serve(port: u16, tx: mpsc::Sender<TelemetryEvent>, cancel: CancellationToken) {
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind telemetry listener");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result.expect("failed to accept connection");
                let tx = tx.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let tx = tx.clone();
                        handle(req, tx)
                    });
                    let _ = Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
            _ = cancel.cancelled() => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
}
