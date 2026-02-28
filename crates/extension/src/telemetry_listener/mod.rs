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

    String::from_utf8(body.to_bytes().into()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "body is not valid UTF-8".to_owned(),
        )
    })
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

/// Telemetry API listener.
/// Receives platform events (platform.runtimeDone, platform.start) from the
/// Lambda platform. The listener must already be bound to 0.0.0.0, not
/// localhost, to be reachable by the Lambda sandbox.
/// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-api-reference.html
pub async fn serve(
    listener: TcpListener,
    tx: mpsc::Sender<TelemetryEvent>,
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
#[path = "tests.rs"]
mod tests;
