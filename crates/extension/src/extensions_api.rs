#![allow(clippy::question_mark)] // nanoserde DeJson derive

use std::future::Future;

use nanoserde::DeJson;
use thiserror::Error;

const EXTENSION_NAME: &str = "lambda-otel-relay";

pub trait ExtensionsApi {
    fn next_event(&self) -> impl Future<Output = Result<ExtensionsApiEvent, ApiError>> + Send + '_;
    fn register_telemetry(
        &self,
        port: u16,
    ) -> impl Future<Output = Result<(), ApiError>> + Send + '_;
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("extensions API HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to parse response: {0}")]
    Parse(#[from] nanoserde::DeJsonErr),
    #[error("missing Lambda-Extension-Identifier header")]
    MissingExtensionId,
    #[error("unknown event type: {0}")]
    UnknownExtensionsApiEventType(String),
    #[error("telemetry API registration failed: HTTP {status} — {body}")]
    TelemetryRegistrationFailed { status: u16, body: String },
}

#[derive(DeJson)]
struct RegisterResponse {
    #[nserde(rename = "functionName")]
    function_name: String,
    #[nserde(rename = "functionVersion")]
    function_version: String,
    handler: String,
}

#[derive(DeJson)]
struct ExtensionsApiEventResponse {
    #[nserde(rename = "eventType")]
    event_type: String,
    #[nserde(rename = "requestId")]
    request_id: Option<String>,
    #[nserde(rename = "shutdownReason")]
    shutdown_reason: Option<String>,
}

#[derive(Debug)]
pub enum ExtensionsApiEvent {
    Invoke { request_id: String },
    Shutdown { reason: String },
}

#[derive(Debug)]
pub struct ExtensionApiClient {
    client: reqwest::Client,
    runtime_api: String,
    ext_id: String,
}

impl ExtensionApiClient {
    pub async fn register(runtime_api: &str) -> Result<Self, ApiError> {
        let client = reqwest::Client::new();
        let extensions_url = format!("http://{runtime_api}/2020-01-01/extension");

        let resp = client
            .post(format!("{extensions_url}/register"))
            .header("Lambda-Extension-Name", EXTENSION_NAME)
            .body(r#"{"events":["INVOKE","SHUTDOWN"]}"#)
            .send()
            .await?;

        let ext_id = resp
            .headers()
            .get("Lambda-Extension-Identifier")
            .ok_or(ApiError::MissingExtensionId)?
            .to_str()
            .map_err(|_| ApiError::MissingExtensionId)?
            .to_owned();

        let body = resp.text().await?;
        let reg: RegisterResponse = DeJson::deserialize_json(&body)?;
        tracing::debug!(
            function = reg.function_name,
            version = reg.function_version,
            handler = reg.handler,
            "Extension registered with Lambda Runtime API"
        );

        Ok(Self {
            client,
            runtime_api: runtime_api.to_owned(),
            ext_id,
        })
    }
}

impl ExtensionsApi for ExtensionApiClient {
    /// Subscribe to the Lambda Telemetry API to receive platform lifecycle events.
    /// Must be called after the telemetry listener is bound and accepting connections.
    async fn register_telemetry(&self, port: u16) -> Result<(), ApiError> {
        let url = format!("http://{}/2022-08-01/telemetry", self.runtime_api);
        let body = format!(
            r#"{{"schemaVersion":"2022-07-01","types":["platform"],"buffering":{{"timeoutMs":25,"maxBytes":262144,"maxItems":1000}},"destination":{{"protocol":"HTTP","URI":"http://sandbox:{port}"}}}}"#
        );

        let resp = self
            .client
            .put(&url)
            .header("Lambda-Extension-Identifier", &self.ext_id)
            .body(body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ApiError::TelemetryRegistrationFailed {
                status: status.as_u16(),
                body,
            });
        }

        tracing::debug!("Subscribed to Lambda Telemetry API on port {port}");
        Ok(())
    }

    async fn next_event(&self) -> Result<ExtensionsApiEvent, ApiError> {
        let resp = self
            .client
            .get(format!(
                "http://{}/2020-01-01/extension/event/next",
                self.runtime_api
            ))
            .header("Lambda-Extension-Identifier", &self.ext_id)
            .send()
            .await?;

        let body = resp.text().await?;
        parse_event(&body)
    }
}

fn parse_event(body: &str) -> Result<ExtensionsApiEvent, ApiError> {
    let raw: ExtensionsApiEventResponse = DeJson::deserialize_json(body)?;

    match raw.event_type.as_str() {
        "INVOKE" => Ok(ExtensionsApiEvent::Invoke {
            request_id: raw.request_id.unwrap_or_default(),
        }),
        "SHUTDOWN" => Ok(ExtensionsApiEvent::Shutdown {
            reason: raw.shutdown_reason.unwrap_or_default(),
        }),
        other => Err(ApiError::UnknownExtensionsApiEventType(other.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_invoke() {
        let event = parse_event(r#"{"eventType":"INVOKE","requestId":"req-abc-123"}"#).unwrap();
        assert!(
            matches!(event, ExtensionsApiEvent::Invoke { request_id } if request_id == "req-abc-123")
        );
    }

    #[test]
    fn parse_invoke_missing_request_id() {
        let event = parse_event(r#"{"eventType":"INVOKE"}"#).unwrap();
        assert!(
            matches!(event, ExtensionsApiEvent::Invoke { request_id } if request_id.is_empty())
        );
    }

    #[test]
    fn parse_shutdown() {
        let event = parse_event(r#"{"eventType":"SHUTDOWN","shutdownReason":"timeout"}"#).unwrap();
        assert!(matches!(event, ExtensionsApiEvent::Shutdown { reason } if reason == "timeout"));
    }

    #[test]
    fn parse_shutdown_missing_reason() {
        let event = parse_event(r#"{"eventType":"SHUTDOWN"}"#).unwrap();
        assert!(matches!(event, ExtensionsApiEvent::Shutdown { reason } if reason.is_empty()));
    }

    #[test]
    fn parse_unknown_event_type() {
        let err = parse_event(r#"{"eventType":"BANANA"}"#).unwrap_err();
        assert!(matches!(err, ApiError::UnknownExtensionsApiEventType(t) if t == "BANANA"));
    }

    #[test]
    fn parse_malformed_json() {
        let err = parse_event("{not valid").unwrap_err();
        assert!(matches!(err, ApiError::Parse(_)));
    }

    #[test]
    fn parse_empty_body() {
        let err = parse_event("").unwrap_err();
        assert!(matches!(err, ApiError::Parse(_)));
    }
}
