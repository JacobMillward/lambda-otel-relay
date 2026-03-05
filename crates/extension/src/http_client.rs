use std::io::Cursor;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use rustls::ClientConfig;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid CA certificate PEM")]
    InvalidCaCertificate,

    #[error("invalid client certificate/key PEM: {0}")]
    InvalidClientIdentity(String),

    #[error("TLS configuration error: {0}")]
    Tls(#[from] rustls::Error),

    #[error("HTTP request failed: {0}")]
    Http(String),

    #[error("request timed out")]
    Timeout,
}

pub struct HttpClient {
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Full<Bytes>,
    >,
    timeout: Duration,
}

pub struct HttpResponse {
    pub status: hyper::StatusCode,
}

impl HttpClient {
    pub fn new(
        timeout: Duration,
        ca_pem: Option<&[u8]>,
        client_cert_pem: Option<&[u8]>,
        client_key_pem: Option<&[u8]>,
    ) -> Result<Self, ClientError> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        if let Some(ca) = ca_pem {
            let certs = rustls_pemfile::certs(&mut Cursor::new(ca))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ClientError::InvalidCaCertificate)?;
            for cert in certs {
                root_store
                    .add(cert)
                    .map_err(|_| ClientError::InvalidCaCertificate)?;
            }
        }

        let tls_config = if let (Some(cert_pem), Some(key_pem)) = (client_cert_pem, client_key_pem)
        {
            let certs = rustls_pemfile::certs(&mut Cursor::new(cert_pem))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| ClientError::InvalidClientIdentity(e.to_string()))?;
            let key = rustls_pemfile::private_key(&mut Cursor::new(key_pem))
                .map_err(|e| ClientError::InvalidClientIdentity(e.to_string()))?
                .ok_or_else(|| {
                    ClientError::InvalidClientIdentity("no private key found in PEM".into())
                })?;
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)?
        } else {
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };

        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_all_versions()
            .build();

        let client = Client::builder(TokioExecutor::new()).build(connector);

        Ok(Self { client, timeout })
    }

    pub async fn post(
        &self,
        url: &str,
        headers: &[(String, String)],
        body: Vec<u8>,
    ) -> Result<HttpResponse, ClientError> {
        let mut builder = hyper::Request::builder()
            .method(hyper::Method::POST)
            .uri(url);

        for (k, v) in headers {
            builder = builder.header(k.as_str(), v.as_str());
        }

        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let resp = tokio::time::timeout(self.timeout, self.client.request(request))
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let status = resp.status();

        Ok(HttpResponse { status })
    }
}
