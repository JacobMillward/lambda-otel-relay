#![allow(clippy::question_mark)] // nanoserde DeJson derive

use nanoserde::DeJson;

/// Raw JSON shape for a single element in the Telemetry API batch.
/// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-schema-reference.html
#[derive(DeJson)]
struct RawTelemetryEvent {
    #[nserde(rename = "type")]
    event_type: String,
    record: RawRecord,
}

#[derive(DeJson)]
struct RawRecord {
    #[nserde(rename = "requestId")]
    request_id: Option<String>,
    status: Option<String>,
    tracing: Option<RawTracing>,
}

#[derive(DeJson)]
struct RawTracing {
    value: Option<String>,
}

/// A platform event delivered by the Lambda Telemetry API.
/// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-api.html
#[derive(Debug, PartialEq)]
pub enum TelemetryEvent {
    /// `platform.runtimeDone` — reports the outcome of an invocation.
    /// status is one of: success, failure, error, timeout.
    /// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-schema-reference.html#platform-runtimeDone
    RuntimeDone { request_id: String, status: String },
    /// `platform.start` — carries X-Ray trace context when active tracing is enabled.
    /// https://docs.aws.amazon.com/lambda/latest/dg/telemetry-schema-reference.html#platform-start
    Start {
        request_id: String,
        tracing_value: Option<String>,
    },
}

impl TelemetryEvent {
    pub fn parse_batch(body: &str) -> Vec<Self> {
        let raw: Vec<RawTelemetryEvent> = match DeJson::deserialize_json(body) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "telemetry batch parse failed");
                return Vec::new();
            }
        };

        let mut events = Vec::new();
        for item in raw {
            match item.event_type.as_str() {
                "platform.runtimeDone" => {
                    events.push(TelemetryEvent::RuntimeDone {
                        request_id: item.record.request_id.unwrap_or_default(),
                        status: item.record.status.unwrap_or_default(),
                    });
                }
                "platform.start" => {
                    events.push(TelemetryEvent::Start {
                        request_id: item.record.request_id.unwrap_or_default(),
                        tracing_value: item.record.tracing.and_then(|t| t.value),
                    });
                }
                _ => {
                    // Ignore event types we don't care about (e.g. platform.initStart)
                }
            }
        }
        events
    }
}

#[cfg(test)]
#[path = "events_tests.rs"]
mod tests;
