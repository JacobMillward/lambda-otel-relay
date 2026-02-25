use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
/// Describes what the test handler does during a single invocation
pub struct Scenario {
    pub actions: Vec<Action>,
}

impl Scenario {
    pub fn new() -> Self {
        Self::default()
    }

    /// The handler will POST to the extension's OTLP listener at the given path.
    pub fn post_otlp(mut self, path: &str, body: &[u8]) -> Self {
        self.actions.push(Action::PostOtlp {
            path: path.into(),
            body: BASE64.encode(body),
        });
        self
    }

    /// The handler will drain and return collected exports from the mock collector.
    pub fn get_collected(mut self, timeout_ms: Option<u64>, min_expected: Option<usize>) -> Self {
        self.actions.push(Action::GetCollected {
            timeout_ms,
            min_expected,
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

    #[serde(rename = "get_collected")]
    GetCollected {
        timeout_ms: Option<u64>,
        min_expected: Option<usize>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectedExport {
    pub path: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: HashMap<String, String>,
    pub body: String,
}

#[derive(Serialize, Deserialize)]
pub struct ActionResult {
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collected: Option<Vec<CollectedExport>>,
}
