use super::*;

#[test]
fn parses_runtime_done_event() {
    let events = TelemetryEvent::parse_batch(
        r#"[{"type":"platform.runtimeDone","time":"2024-01-01T00:00:00Z","record":{"requestId":"req-1","status":"success"}}]"#,
    );
    assert_eq!(
        events,
        vec![TelemetryEvent::RuntimeDone {
            request_id: "req-1".into(),
            status: "success".into(),
        }]
    );
}

#[test]
fn parses_runtime_done_timeout() {
    let events = TelemetryEvent::parse_batch(
        r#"[{"type":"platform.runtimeDone","time":"2024-01-01T00:00:00Z","record":{"requestId":"req-2","status":"timeout"}}]"#,
    );
    assert_eq!(
        events,
        vec![TelemetryEvent::RuntimeDone {
            request_id: "req-2".into(),
            status: "timeout".into(),
        }]
    );
}

#[test]
fn parses_start_event_with_tracing() {
    let events = TelemetryEvent::parse_batch(
        r#"[{"type":"platform.start","time":"2024-01-01T00:00:00Z","record":{"requestId":"req-3","tracing":{"value":"Root=1-abc-def;Parent=123;Sampled=1"}}}]"#,
    );
    assert_eq!(
        events,
        vec![TelemetryEvent::Start {
            request_id: "req-3".into(),
            tracing_value: Some("Root=1-abc-def;Parent=123;Sampled=1".into()),
        }]
    );
}

#[test]
fn parses_start_event_without_tracing() {
    let events = TelemetryEvent::parse_batch(
        r#"[{"type":"platform.start","time":"2024-01-01T00:00:00Z","record":{"requestId":"req-4"}}]"#,
    );
    assert_eq!(
        events,
        vec![TelemetryEvent::Start {
            request_id: "req-4".into(),
            tracing_value: None,
        }]
    );
}

#[test]
fn ignores_unknown_event_types() {
    let events = TelemetryEvent::parse_batch(
        r#"[{"type":"someUnknownType","time":"2024-01-01T00:00:00Z","record":{}}]"#,
    );
    assert!(events.is_empty());
}

#[test]
fn parses_mixed_batch() {
    let events = TelemetryEvent::parse_batch(
        r#"[
                {"type":"platform.start","time":"2024-01-01T00:00:00Z","record":{"requestId":"req-5"}},
                {"type":"platform.initStart","time":"2024-01-01T00:00:00Z","record":{}},
                {"type":"platform.runtimeDone","time":"2024-01-01T00:00:00Z","record":{"requestId":"req-5","status":"success"}}
            ]"#,
    );
    assert_eq!(
        events,
        vec![
            TelemetryEvent::Start {
                request_id: "req-5".into(),
                tracing_value: None
            },
            TelemetryEvent::RuntimeDone {
                request_id: "req-5".into(),
                status: "success".into()
            },
        ]
    );
}

#[test]
fn returns_empty_for_malformed_json() {
    let events = TelemetryEvent::parse_batch("{not json");
    assert!(events.is_empty());
}

#[test]
fn returns_empty_for_empty_array() {
    let events = TelemetryEvent::parse_batch("[]");
    assert!(events.is_empty());
}

#[test]
fn parses_real_aws_runtime_done_with_extra_fields() {
    let events = TelemetryEvent::parse_batch(
        r#"[{
            "time": "2022-10-12T00:01:15.000Z",
            "type": "platform.runtimeDone",
            "record": {
                "requestId": "6d68ca91-49c9-448d-89b8-7ca3e6dc66aa",
                "status": "success",
                "tracing": {
                    "spanId": "54565fb41ac79632",
                    "type": "X-Amzn-Trace-Id",
                    "value": "Root=1-62e900b2-710d76f009d6e7785905449a;Parent=0efbd19962d95b05;Sampled=1"
                },
                "spans": [{"name": "someTimeSpan", "start": "2022-08-02T12:01:23:521Z", "durationMs": 80.0}],
                "metrics": {"durationMs": 140.0, "producedBytes": 16}
            }
        }]"#,
    );
    assert_eq!(
        events,
        vec![TelemetryEvent::RuntimeDone {
            request_id: "6d68ca91-49c9-448d-89b8-7ca3e6dc66aa".into(),
            status: "success".into(),
        }]
    );
}
