use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
/// Describes what the test handler does during a single invocation
pub struct Scenario {
    pub actions: Vec<Action>,
}

impl Scenario {
    pub fn new() -> Self {
        Self { actions: vec![] }
    }

    /// The handler will POST to the extension's OTLP listener at the given path.
    pub fn post_otlp(mut self, path: &str, body: &[u8]) -> Self {
        self.actions.push(Action::PostOtlp {
            path: path.into(),
            body: BASE64.encode(body),
        });
        self
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Action {
    #[serde(rename = "post_otlp")]
    PostOtlp { path: String, body: String },
}

#[derive(Serialize, Deserialize)]
pub struct ActionResult {
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
}
