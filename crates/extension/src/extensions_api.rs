#![allow(clippy::question_mark)] // nanoserde DeJson derive

use std::fmt;
use std::future::Future;

use nanoserde::DeJson;
use thiserror::Error;

use crate::config::ConfigError;

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
    #[error("init failed: {0}")]
    InitFailed(String),
    #[error("AWS_LAMBDA_RUNTIME_API not set")]
    MissingRuntimeApi,
}

/// Errors reported to the Extensions API via `/extension/init/error`.
#[derive(Debug, Error)]
pub enum InitError {
    #[error("{0}")]
    Config(#[from] ConfigError),
    #[error("{0}")]
    ListenerBind(ApiError),
}

impl InitError {
    fn error_type(&self) -> &'static str {
        match self {
            InitError::Config(_) => "Extension.ConfigInvalid",
            InitError::ListenerBind(_) => "Extension.InitFailed",
        }
    }
}

/// Errors reported to the Extensions API via `/extension/exit/error`.
#[derive(Debug)]
#[allow(dead_code)]
pub enum ExitError {
    RuntimeFailure(String),
}

impl fmt::Display for ExitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExitError::RuntimeFailure(msg) => f.write_str(msg),
        }
    }
}

impl ExitError {
    fn error_type(&self) -> &'static str {
        match self {
            ExitError::RuntimeFailure(_) => "Extension.RuntimeFailure",
        }
    }
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
    /// Read `AWS_LAMBDA_RUNTIME_API` from the environment and register the
    /// extension with the Lambda Extensions API.
    pub async fn register() -> Result<Self, ApiError> {
        let runtime_api =
            std::env::var("AWS_LAMBDA_RUNTIME_API").map_err(|_| ApiError::MissingRuntimeApi)?;
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
            runtime_api,
            ext_id,
        })
    }

    /// Report an init error to the Lambda Extensions API.
    /// Called when the extension fails to initialize after registration.
    pub async fn report_init_error(&self, error: &InitError) {
        let error_type = error.error_type();
        let message = error.to_string();
        let url = format!(
            "http://{}/2020-01-01/extension/init/error",
            self.runtime_api
        );
        let body = format!(r#"{{"errorMessage":"{message}","errorType":"{error_type}"}}"#);
        let _ = self
            .client
            .post(&url)
            .header("Lambda-Extension-Identifier", &self.ext_id)
            .header("Lambda-Extension-Function-Error-Type", error_type)
            .body(body)
            .send()
            .await;
    }

    /// Report an exit error to the Lambda Extensions API before exiting.
    #[allow(dead_code)]
    pub async fn report_exit_error(&self, error: &ExitError) {
        let error_type = error.error_type();
        let message = error.to_string();
        let url = format!(
            "http://{}/2020-01-01/extension/exit/error",
            self.runtime_api
        );
        let body = format!(r#"{{"errorMessage":"{message}","errorType":"{error_type}"}}"#);
        let _ = self
            .client
            .post(&url)
            .header("Lambda-Extension-Identifier", &self.ext_id)
            .header("Lambda-Extension-Function-Error-Type", error_type)
            .body(body)
            .send()
            .await;
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
