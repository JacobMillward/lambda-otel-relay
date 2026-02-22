mod support;

use support::{buf_contains, line_matches};

#[test]
fn line_matches_json_message_exact() {
    let line =
        r#"{"level":"DEBUG","fields":{"message":"Received invoke event","request_id":"abc"}}"#;
    assert!(line_matches(line, "Received invoke event"));
}

#[test]
fn line_matches_json_message_no_match() {
    let line =
        r#"{"level":"DEBUG","fields":{"message":"Received invoke event","request_id":"abc"}}"#;
    assert!(!line_matches(line, "invoke"));
}

#[test]
fn line_matches_plain_text_fallback() {
    assert!(line_matches("some plain log invoke event", "invoke"));
}

#[test]
fn line_matches_plain_text_no_match() {
    assert!(!line_matches("some plain log", "invoke"));
}

#[test]
fn buf_contains_finds_json_line() {
    let buf = "garbage\n{\"level\":\"DEBUG\",\"fields\":{\"message\":\"hello world\"}}\nmore\n";
    assert!(buf_contains(buf, "hello world"));
}

#[test]
fn buf_contains_no_match() {
    let buf = "garbage\n{\"level\":\"DEBUG\",\"fields\":{\"message\":\"hello\"}}\n";
    assert!(!buf_contains(buf, "hello world"));
}
