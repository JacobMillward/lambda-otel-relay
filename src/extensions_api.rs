use nanoserde::DeJson;
use thiserror::Error;

const EXTENSION_NAME: &str = "lambda-otel-flush";

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

pub enum ExtensionsApiEvent {
    Invoke { request_id: String },
    Shutdown { reason: String },
}

pub struct ExtensionApiClient {
    client: reqwest::Client,
    base_url: String,
    ext_id: String,
}

impl ExtensionApiClient {
    pub async fn register(runtime_api: &str) -> Result<Self, ApiError> {
        let client = reqwest::Client::new();
        let base_url = format!("http://{runtime_api}/2020-01-01/extension");

        let resp = client
            .post(format!("{base_url}/register"))
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
        eprintln!(
            "registered: function={} version={} handler={}",
            reg.function_name, reg.function_version, reg.handler
        );

        Ok(Self {
            client,
            base_url,
            ext_id,
        })
    }

    pub async fn next_event(&self) -> Result<ExtensionsApiEvent, ApiError> {
        let resp = self
            .client
            .get(format!("{}/event/next", self.base_url))
            .header("Lambda-Extension-Identifier", &self.ext_id)
            .send()
            .await?;

        let body = resp.text().await?;
        let raw: ExtensionsApiEventResponse = DeJson::deserialize_json(&body)?;

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
}
