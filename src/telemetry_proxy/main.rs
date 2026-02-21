//! Reverse proxy for integration tests.
//!
//! The Lambda RIE doesn't implement the Telemetry API (`PUT /2022-08-01/telemetry`),
//! so the extension crashes during init. This proxy sits between the extension and
//! the RIE: it intercepts the Telemetry API call (returns 200 OK) and forwards
//! everything else (Extensions API, Runtime API) to the real RIE unchanged.
//!
//! ```text
//! Extension ──► Proxy (:9002) ──► RIE (:9001)
//!                 │
//!                 ├─ PUT /telemetry → 200 (intercepted)
//!                 └─ everything else → forwarded
//! ```

use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

const LISTEN_ADDR: &str = "127.0.0.1:9002";
const UPSTREAM_ADDR: &str = "127.0.0.1:9001";
const READY_PATH: &str = "/tmp/telemetry-proxy-ready";

async fn handle(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
    // Intercept PUT requests to the Telemetry API
    if req.method() == Method::PUT && req.uri().path().contains("/telemetry") {
        eprintln!(
            "[telemetry-proxy] intercepted {} {} -> 200 OK",
            req.method(),
            req.uri()
        );
        return Ok(Response::new(Full::new(Bytes::from("OK"))));
    }

    // Forward everything else to the real RIE
    match forward(req).await {
        Ok(resp) => Ok(resp),
        Err(e) => {
            eprintln!("[telemetry-proxy] forward error: {e}");
            Ok(Response::builder()
                .status(502)
                .body(Full::new(Bytes::from(format!("proxy error: {e}"))))
                .unwrap())
        }
    }
}

async fn forward(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Box<dyn std::error::Error + Send + Sync>> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();

    let body_bytes = http_body_util::BodyExt::collect(req.into_body())
        .await?
        .to_bytes();

    let url = format!(
        "http://{UPSTREAM_ADDR}{}{}",
        uri.path(),
        uri.query().map(|q| format!("?{q}")).unwrap_or_default()
    );

    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("failed to build reqwest client");

    let mut upstream_req = client.request(method.clone(), &url);
    for (name, value) in &headers {
        if name == hyper::header::HOST {
            continue;
        }
        upstream_req = upstream_req.header(name.as_str(), value.as_bytes());
    }
    upstream_req = upstream_req.body(body_bytes.to_vec());

    let upstream_resp = upstream_req.send().await?;

    let status = upstream_resp.status();
    let resp_headers = upstream_resp.headers().clone();
    let resp_body = upstream_resp.bytes().await?;

    let mut response = Response::builder().status(status);
    for (name, value) in &resp_headers {
        response = response.header(name, value);
    }
    Ok(response.body(Full::new(resp_body)).unwrap())
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring provider");

    let addr: SocketAddr = LISTEN_ADDR.parse().unwrap();
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind telemetry proxy");

    // Signal readiness so the entrypoint can start the RIE.
    std::fs::write(READY_PATH, b"").expect("failed to write ready file");
    eprintln!("[telemetry-proxy] listening on {LISTEN_ADDR}");

    loop {
        let (stream, _) = listener.accept().await.expect("accept failed");
        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service_fn(handle))
                .await
            {
                eprintln!("[telemetry-proxy] connection error: {e}");
            }
        });
    }
}
