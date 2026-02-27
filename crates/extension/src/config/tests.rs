use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use super::*;

fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn parses_endpoint_and_applies_default_ports() {
    let config = Config::parse(&vars(&[(
        "LAMBDA_OTEL_RELAY_ENDPOINT",
        "https://collector.example.com:4318",
    )]))
    .unwrap();
    assert_eq!(config.endpoint.scheme(), "https");
    assert_eq!(config.endpoint.host_str(), Some("collector.example.com"));
    assert_eq!(
        config.listener_port, 4318,
        "default otlp listener port should be 4318"
    );
    assert_eq!(
        config.telemetry_port, 4319,
        "default telemetry port should be 4319"
    );
}

#[test]
fn overrides_default_ports_when_set() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_LISTENER_PORT", "9090"),
        ("LAMBDA_OTEL_RELAY_TELEMETRY_PORT", "9091"),
    ]))
    .unwrap();
    assert_eq!(
        config.listener_port, 9090,
        "should parse custom listener port"
    );
    assert_eq!(
        config.telemetry_port, 9091,
        "should parse custom telemetry port"
    );
}

#[test]
fn rejects_missing_endpoint() {
    let err = Config::parse(&vars(&[])).unwrap_err();
    assert!(
        matches!(err, ConfigError::EndpointMissing),
        "should require endpoint"
    );
}

#[test]
fn rejects_empty_endpoint() {
    let err = Config::parse(&vars(&[("LAMBDA_OTEL_RELAY_ENDPOINT", "")])).unwrap_err();
    assert!(
        matches!(err, ConfigError::EndpointMissing),
        "should reject empty endpoint"
    );
}

#[test]
fn rejects_invalid_endpoint_url() {
    let err = Config::parse(&vars(&[("LAMBDA_OTEL_RELAY_ENDPOINT", "not a url")])).unwrap_err();
    assert!(
        matches!(err, ConfigError::EndpointInvalidUrl(_)),
        "should reject invalid endpoint URL"
    );
}

#[test]
fn rejects_non_numeric_port() {
    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_LISTENER_PORT", "abc"),
    ]))
    .unwrap_err();
    assert!(
        matches!(err, ConfigError::InvalidNumeric(_, _)),
        "should reject non-numeric port"
    );
}

#[test]
fn default_export_timeout() {
    let config = Config::parse(&vars(&[(
        "LAMBDA_OTEL_RELAY_ENDPOINT",
        "http://localhost:4318",
    )]))
    .unwrap();
    assert_eq!(config.export_timeout, Duration::from_millis(5000));
}

#[test]
fn custom_export_timeout() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_EXPORT_TIMEOUT_MS", "10000"),
    ]))
    .unwrap();
    assert_eq!(config.export_timeout, Duration::from_millis(10000));
}

#[test]
fn invalid_export_timeout() {
    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_EXPORT_TIMEOUT_MS", "not_a_number"),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::InvalidNumeric(_, _)));
}

#[test]
fn default_compression_is_gzip() {
    let config = Config::parse(&vars(&[(
        "LAMBDA_OTEL_RELAY_ENDPOINT",
        "http://localhost:4318",
    )]))
    .unwrap();
    assert_eq!(config.compression, Compression::Gzip);
}

#[test]
fn compression_none() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_COMPRESSION", "none"),
    ]))
    .unwrap();
    assert_eq!(config.compression, Compression::None);
}

#[test]
fn invalid_compression() {
    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_COMPRESSION", "snappy"),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::InvalidCompression(_)));
}

#[test]
fn parses_export_headers() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        (
            "LAMBDA_OTEL_RELAY_EXPORT_HEADERS",
            "x-api-key=abc123,x-tenant=foo",
        ),
    ]))
    .unwrap();
    assert_eq!(
        config.export_headers,
        vec![
            ("x-api-key".to_owned(), "abc123".to_owned()),
            ("x-tenant".to_owned(), "foo".to_owned()),
        ]
    );
}

#[test]
fn default_buffer_max_bytes() {
    let config = Config::parse(&vars(&[(
        "LAMBDA_OTEL_RELAY_ENDPOINT",
        "http://localhost:4318",
    )]))
    .unwrap();
    assert_eq!(config.buffer_max_bytes, Some(4_194_304));
}

#[test]
fn custom_buffer_max_bytes() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES", "1048576"),
    ]))
    .unwrap();
    assert_eq!(config.buffer_max_bytes, Some(1_048_576));
}

#[test]
fn zero_buffer_max_bytes_disables() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES", "0"),
    ]))
    .unwrap();
    assert_eq!(config.buffer_max_bytes, None);
}

#[test]
fn invalid_buffer_max_bytes() {
    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES", "not_a_number"),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::InvalidNumeric(_, _)));
}

#[test]
fn empty_headers_returns_empty_vec() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_EXPORT_HEADERS", ""),
    ]))
    .unwrap();
    assert!(config.export_headers.is_empty());
}

#[test]
fn default_flush_strategy_is_default() {
    let config = Config::parse(&vars(&[(
        "LAMBDA_OTEL_RELAY_ENDPOINT",
        "http://localhost:4318",
    )]))
    .unwrap();
    assert!(matches!(config.flush_strategy, FlushStrategy::Default));
}

#[test]
fn explicit_end_flush_strategy() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end"),
    ]))
    .unwrap();
    assert!(matches!(config.flush_strategy, FlushStrategy::End));
}

#[test]
fn invalid_flush_strategy() {
    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "bogus"),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::FlushStrategy(_)));
}

#[test]
fn periodically_flush_strategy() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "periodically,60000"),
    ]))
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::Periodically { interval } if interval == Duration::from_millis(60000)
    ));
}

#[test]
fn periodically_missing_param() {
    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "periodically"),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::FlushStrategy(_)));
}

#[test]
fn end_periodically_flush_strategy() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end,30000"),
    ]))
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::EndPeriodically { interval } if interval == Duration::from_millis(30000)
    ));
}

#[test]
fn continuously_flush_strategy() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "continuously,60000"),
    ]))
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::Continuously { interval } if interval == Duration::from_millis(60000)
    ));
}

#[test]
fn certificate_absent_returns_none() {
    let config = Config::parse(&vars(&[(
        "LAMBDA_OTEL_RELAY_ENDPOINT",
        "http://localhost:4318",
    )]))
    .unwrap();
    assert!(config.tls_ca.is_none());
    assert!(config.tls_client_cert.is_none());
    assert!(config.tls_client_key.is_none());
}

#[test]
fn certificate_reads_file_contents() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"--- CA PEM ---").unwrap();

    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        (
            "LAMBDA_OTEL_RELAY_CERTIFICATE",
            f.path().to_str().unwrap(),
        ),
    ]))
    .unwrap();
    assert_eq!(config.tls_ca.as_deref(), Some(b"--- CA PEM ---".as_slice()));
}

#[test]
fn certificate_missing_file_errors() {
    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_CERTIFICATE", "/no/such/file.pem"),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::CertificateFile { .. }));
}

#[test]
fn certificate_empty_value_treated_as_absent() {
    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_CERTIFICATE", ""),
    ]))
    .unwrap();
    assert!(config.tls_ca.is_none());
}

#[test]
fn client_cert_without_key_errors() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"cert").unwrap();

    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_CLIENT_CERT", f.path().to_str().unwrap()),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::ClientIdentityIncomplete));
}

#[test]
fn client_key_without_cert_errors() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"key").unwrap();

    let err = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        ("LAMBDA_OTEL_RELAY_CLIENT_KEY", f.path().to_str().unwrap()),
    ]))
    .unwrap_err();
    assert!(matches!(err, ConfigError::ClientIdentityIncomplete));
}

#[test]
fn client_cert_and_key_both_set() {
    let mut cert = tempfile::NamedTempFile::new().unwrap();
    cert.write_all(b"cert-pem").unwrap();
    let mut key = tempfile::NamedTempFile::new().unwrap();
    key.write_all(b"key-pem").unwrap();

    let config = Config::parse(&vars(&[
        ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
        (
            "LAMBDA_OTEL_RELAY_CLIENT_CERT",
            cert.path().to_str().unwrap(),
        ),
        (
            "LAMBDA_OTEL_RELAY_CLIENT_KEY",
            key.path().to_str().unwrap(),
        ),
    ]))
    .unwrap();
    assert_eq!(config.tls_client_cert.as_deref(), Some(b"cert-pem".as_slice()));
    assert_eq!(config.tls_client_key.as_deref(), Some(b"key-pem".as_slice()));
}
