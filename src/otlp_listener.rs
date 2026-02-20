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

use crate::Signal;

fn response(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::default())
        .unwrap()
}

/// Validate the incoming request: route, method, and body.
async fn validate<B>(req: Request<B>) -> Result<(Signal, Bytes), (StatusCode, String)>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
{
    let path = req.uri().path().to_owned();
    let method = req.method().clone();

    let signal = match path.as_str() {
        "/v1/traces" => Ok(Signal::Traces),
        "/v1/metrics" => Ok(Signal::Metrics),
        "/v1/logs" => Ok(Signal::Logs),
        _ => Err((StatusCode::NOT_FOUND, format!("unknown path: {path}"))),
    }
    .and_then(|signal| {
        if method == Method::POST {
            Ok(signal)
        } else {
            Err((StatusCode::METHOD_NOT_ALLOWED, format!("{method} {path}")))
        }
    })?;

    let body = req
        .collect()
        .await
        .map(|c| c.to_bytes())
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("POST {path} — failed to read body")))?;

    Ok((signal, body))
}

async fn handle<B>(
    req: Request<B>,
    tx: mpsc::Sender<(Signal, Bytes)>,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
{
    let (signal, body) = match validate(req).await {
        Ok(pair) => pair,
        Err((status, reason)) => {
            eprintln!("otlp request rejected: {reason}");
            return Ok(response(status));
        }
    };

    use tokio::sync::mpsc::error::TrySendError;
    match tx.try_send((signal, body)) {
        Ok(()) => Ok(response(StatusCode::OK)),
        Err(TrySendError::Full(_)) => Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Retry-After", "1")
            .body(Full::default())
            .unwrap()),
        // Channel closed means the receiver is gone (shutdown). Retrying won't
        // help — return 502 since this proxy's backend is no longer available.
        Err(TrySendError::Closed(_)) => Ok(response(StatusCode::BAD_GATEWAY)),
    }
}

pub async fn serve(port: u16, tx: mpsc::Sender<(Signal, Bytes)>, cancel: CancellationToken) {
    let listener = TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("failed to bind OTLP listener");

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
}
