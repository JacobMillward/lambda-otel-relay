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

use crate::buffers::Signal;

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

    let body = req.collect().await.map(|c| c.to_bytes()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!("POST {path} — failed to read body"),
        )
    })?;

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
            tracing::warn!(reason, "otlp request rejected");
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

pub async fn serve(
    listener: TcpListener,
    tx: mpsc::Sender<(Signal, Bytes)>,
    cancel: CancellationToken,
) {
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
mod tests;
