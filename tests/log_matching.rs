mod support;

use support::{LogLevel, buf_contains_source, line_matches_source};

#[test]
fn line_matches_source_matching_target_and_message() {
    let line = r#"{"level":"DEBUG","fields":{"message":"Received invoke event","request_id":"abc"},"target":"lambda_otel_relay"}"#;
    assert!(line_matches_source(
        line,
        "Received invoke event",
        "lambda_otel_relay",
        Some(LogLevel::Debug),
    ));
}

#[test]
fn line_matches_source_wrong_target() {
    let line = r#"{"level":"DEBUG","fields":{"message":"Received invoke event","request_id":"abc"},"target":"test_handler"}"#;
    assert!(!line_matches_source(
        line,
        "Received invoke event",
        "lambda_otel_relay",
        Some(LogLevel::Debug),
    ));
}

#[test]
fn line_matches_source_wrong_message() {
    let line =
        r#"{"level":"DEBUG","fields":{"message":"Something else"},"target":"lambda_otel_relay"}"#;
    assert!(!line_matches_source(
        line,
        "Received invoke event",
        "lambda_otel_relay",
        Some(LogLevel::Debug),
    ));
}

#[test]
fn line_matches_source_non_json() {
    assert!(!line_matches_source(
        "plain text log line",
        "plain text",
        "lambda_otel_relay",
        None,
    ));
}

#[test]
fn line_matches_source_no_level_filter() {
    let line = r#"{"level":"WARN","fields":{"message":"something happened"},"target":"lambda_otel_relay"}"#;
    assert!(line_matches_source(
        line,
        "something happened",
        "lambda_otel_relay",
        None,
    ));
}

#[test]
fn line_matches_source_wrong_level() {
    let line = r#"{"level":"WARN","fields":{"message":"something happened"},"target":"lambda_otel_relay"}"#;
    assert!(!line_matches_source(
        line,
        "something happened",
        "lambda_otel_relay",
        Some(LogLevel::Debug),
    ));
}

#[test]
fn line_matches_source_target_prefix() {
    let line =
        r#"{"level":"DEBUG","fields":{"message":"detail"},"target":"lambda_otel_relay::exporter"}"#;
    assert!(line_matches_source(
        line,
        "detail",
        "lambda_otel_relay",
        Some(LogLevel::Debug),
    ));
}

#[test]
fn buf_contains_source_finds_matching_line() {
    let buf = r#"garbage
{"level":"DEBUG","fields":{"message":"hello"},"target":"test_handler"}
{"level":"DEBUG","fields":{"message":"hello"},"target":"lambda_otel_relay"}
more
"#;
    assert!(buf_contains_source(
        buf,
        "hello",
        "lambda_otel_relay",
        Some(LogLevel::Debug),
    ));
}

#[test]
fn buf_contains_source_no_match_wrong_source() {
    let buf = r#"{"level":"DEBUG","fields":{"message":"hello"},"target":"test_handler"}
"#;
    assert!(!buf_contains_source(
        buf,
        "hello",
        "lambda_otel_relay",
        Some(LogLevel::Debug),
    ));
}
