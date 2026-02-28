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
    assert!(matches!(event, ExtensionsApiEvent::Invoke { request_id } if request_id.is_empty()));
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

// -- InitError / ExitError enum tests --

#[test]
fn init_error_config_error_type() {
    let err = InitError::Config(ConfigError::EndpointMissing);
    assert_eq!(err.error_type(), "Extension.ConfigInvalid");
}

#[test]
fn init_error_listener_bind_error_type() {
    let err = InitError::ListenerBind(ApiError::InitFailed("port in use".into()));
    assert_eq!(err.error_type(), "Extension.InitFailed");
}

#[test]
fn init_error_displays_inner() {
    let err = InitError::Config(ConfigError::InvalidCompression("snappy".into()));
    assert_eq!(
        err.to_string(),
        ConfigError::InvalidCompression("snappy".into()).to_string()
    );
}

#[test]
fn exit_error_runtime_failure_error_type() {
    let err = ExitError::RuntimeFailure("boom".into());
    assert_eq!(err.error_type(), "Extension.RuntimeFailure");
}

#[test]
fn exit_error_runtime_failure_display() {
    let err = ExitError::RuntimeFailure("something broke".into());
    assert_eq!(err.to_string(), "something broke");
}
