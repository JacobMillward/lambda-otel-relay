use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use super::*;
use crate::runtime_mode::RuntimeMode;

fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn parses_endpoint_and_applies_default_ports() {
    let config = Config::parse(
        &vars(&[(
            "LAMBDA_OTEL_RELAY_ENDPOINT",
            "https://collector.example.com:4318",
        )]),
        RuntimeMode::Standard,
    )
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
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_LISTENER_PORT", "9090"),
            ("LAMBDA_OTEL_RELAY_TELEMETRY_PORT", "9091"),
        ]),
        RuntimeMode::Standard,
    )
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
    let err = Config::parse(&vars(&[]), RuntimeMode::Standard).unwrap_err();
    assert!(
        matches!(err, ConfigError::EndpointMissing),
        "should require endpoint"
    );
}

#[test]
fn rejects_empty_endpoint() {
    let err =
        Config::parse(&vars(&[("LAMBDA_OTEL_RELAY_ENDPOINT", "")]), RuntimeMode::Standard)
            .unwrap_err();
    assert!(
        matches!(err, ConfigError::EndpointMissing),
        "should reject empty endpoint"
    );
}

#[test]
fn rejects_invalid_endpoint_url() {
    let err = Config::parse(
        &vars(&[("LAMBDA_OTEL_RELAY_ENDPOINT", "not a url")]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(
        matches!(err, ConfigError::EndpointInvalidUrl(_)),
        "should reject invalid endpoint URL"
    );
}

#[test]
fn rejects_non_numeric_port() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_LISTENER_PORT", "abc"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(
        matches!(err, ConfigError::InvalidNumeric(_, _)),
        "should reject non-numeric port"
    );
}

#[test]
fn default_export_timeout() {
    let config = Config::parse(
        &vars(&[(
            "LAMBDA_OTEL_RELAY_ENDPOINT",
            "http://localhost:4318",
        )]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.export_timeout, Duration::from_millis(5000));
}

#[test]
fn custom_export_timeout() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_EXPORT_TIMEOUT_MS", "10000"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.export_timeout, Duration::from_millis(10000));
}

#[test]
fn invalid_export_timeout() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_EXPORT_TIMEOUT_MS", "not_a_number"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::InvalidNumeric(_, _)));
}

#[test]
fn default_compression_is_gzip() {
    let config = Config::parse(
        &vars(&[(
            "LAMBDA_OTEL_RELAY_ENDPOINT",
            "http://localhost:4318",
        )]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.compression, Compression::Gzip);
}

#[test]
fn compression_none() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_COMPRESSION", "none"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.compression, Compression::None);
}

#[test]
fn invalid_compression() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_COMPRESSION", "snappy"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::InvalidCompression(_)));
}

#[test]
fn parses_export_headers() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            (
                "LAMBDA_OTEL_RELAY_EXPORT_HEADERS",
                "x-api-key=abc123,x-tenant=foo",
            ),
        ]),
        RuntimeMode::Standard,
    )
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
    let config = Config::parse(
        &vars(&[(
            "LAMBDA_OTEL_RELAY_ENDPOINT",
            "http://localhost:4318",
        )]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.buffer_max_bytes, Some(4_194_304));
}

#[test]
fn custom_buffer_max_bytes() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES", "1048576"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.buffer_max_bytes, Some(1_048_576));
}

#[test]
fn zero_buffer_max_bytes_disables() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES", "0"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.buffer_max_bytes, None);
}

#[test]
fn invalid_buffer_max_bytes() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES", "not_a_number"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::InvalidNumeric(_, _)));
}

#[test]
fn empty_headers_returns_empty_vec() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_EXPORT_HEADERS", ""),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(config.export_headers.is_empty());
}

#[test]
fn default_flush_strategy_is_default() {
    let config = Config::parse(
        &vars(&[(
            "LAMBDA_OTEL_RELAY_ENDPOINT",
            "http://localhost:4318",
        )]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(matches!(config.flush_strategy, FlushStrategy::Default));
}

#[test]
fn explicit_end_flush_strategy() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(matches!(config.flush_strategy, FlushStrategy::End));
}

#[test]
fn invalid_flush_strategy() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "bogus"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::FlushStrategy(_)));
}

#[test]
fn periodically_flush_strategy() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "periodically,60000"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::Periodically { interval } if interval == Duration::from_millis(60000)
    ));
}

#[test]
fn periodically_missing_param() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "periodically"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::FlushStrategy(_)));
}

#[test]
fn end_periodically_flush_strategy() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end,30000"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::EndPeriodically { interval } if interval == Duration::from_millis(30000)
    ));
}

#[test]
fn continuously_flush_strategy() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "continuously,60000"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::Continuously { interval } if interval == Duration::from_millis(60000)
    ));
}

#[test]
fn certificate_absent_returns_none() {
    let config = Config::parse(
        &vars(&[(
            "LAMBDA_OTEL_RELAY_ENDPOINT",
            "http://localhost:4318",
        )]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(config.tls_ca.is_none());
    assert!(config.tls_client_cert.is_none());
    assert!(config.tls_client_key.is_none());
}

#[test]
fn certificate_reads_file_contents() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"--- CA PEM ---").unwrap();

    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_CERTIFICATE", f.path().to_str().unwrap()),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(config.tls_ca.as_deref(), Some(b"--- CA PEM ---".as_slice()));
}

#[test]
fn certificate_missing_file_errors() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_CERTIFICATE", "/no/such/file.pem"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::CertificateFile { .. }));
}

#[test]
fn certificate_empty_value_treated_as_absent() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_CERTIFICATE", ""),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(config.tls_ca.is_none());
}

#[test]
fn client_cert_without_key_errors() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"cert").unwrap();

    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_CLIENT_CERT", f.path().to_str().unwrap()),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::ClientIdentityIncomplete));
}

#[test]
fn client_key_without_cert_errors() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"key").unwrap();

    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_CLIENT_KEY", f.path().to_str().unwrap()),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::ClientIdentityIncomplete));
}

#[test]
fn client_cert_and_key_both_set() {
    let mut cert = tempfile::NamedTempFile::new().unwrap();
    cert.write_all(b"cert-pem").unwrap();
    let mut key = tempfile::NamedTempFile::new().unwrap();
    key.write_all(b"key-pem").unwrap();

    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            (
                "LAMBDA_OTEL_RELAY_CLIENT_CERT",
                cert.path().to_str().unwrap(),
            ),
            ("LAMBDA_OTEL_RELAY_CLIENT_KEY", key.path().to_str().unwrap()),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert_eq!(
        config.tls_client_cert.as_deref(),
        Some(b"cert-pem".as_slice())
    );
    assert_eq!(
        config.tls_client_key.as_deref(),
        Some(b"key-pem".as_slice())
    );
}

#[test]
fn sigv4_disabled_when_service_not_set() {
    let config = Config::parse(
        &vars(&[(
            "LAMBDA_OTEL_RELAY_ENDPOINT",
            "http://localhost:4318",
        )]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(config.sigv4.is_none());
}

#[test]
fn sigv4_disabled_when_service_empty() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", ""),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(config.sigv4.is_none());
}

#[test]
fn sigv4_enabled_with_all_required_vars() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", "aps"),
            ("AWS_REGION", "us-east-1"),
            ("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE"),
            (
                "AWS_SECRET_ACCESS_KEY",
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            ),
            ("AWS_SESSION_TOKEN", "FwoGZXIvY..."),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    let sigv4 = config.sigv4.as_ref().unwrap();
    assert_eq!(sigv4.service, "aps");
    assert_eq!(sigv4.region, "us-east-1");
}

#[test]
fn sigv4_region_override() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", "aps"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_REGION", "eu-west-1"),
            ("AWS_REGION", "us-east-1"),
            ("AWS_ACCESS_KEY_ID", "AKID"),
            ("AWS_SECRET_ACCESS_KEY", "SECRET"),
            ("AWS_SESSION_TOKEN", "TOKEN"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    let sigv4 = config.sigv4.as_ref().unwrap();
    assert_eq!(sigv4.region, "eu-west-1");
}

#[test]
fn sigv4_falls_back_to_aws_default_region() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", "aps"),
            ("AWS_DEFAULT_REGION", "ap-southeast-2"),
            ("AWS_ACCESS_KEY_ID", "AKID"),
            ("AWS_SECRET_ACCESS_KEY", "SECRET"),
            ("AWS_SESSION_TOKEN", "TOKEN"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    let sigv4 = config.sigv4.as_ref().unwrap();
    assert_eq!(sigv4.region, "ap-southeast-2");
}

#[test]
fn sigv4_missing_region_errors() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", "aps"),
            ("AWS_ACCESS_KEY_ID", "AKID"),
            ("AWS_SECRET_ACCESS_KEY", "SECRET"),
            ("AWS_SESSION_TOKEN", "TOKEN"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::SigV4MissingRegion));
}

#[test]
fn sigv4_missing_access_key_errors() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", "aps"),
            ("AWS_REGION", "us-east-1"),
            ("AWS_SECRET_ACCESS_KEY", "SECRET"),
            ("AWS_SESSION_TOKEN", "TOKEN"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::SigV4MissingCredentials));
}

#[test]
fn sigv4_missing_secret_key_errors() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", "aps"),
            ("AWS_REGION", "us-east-1"),
            ("AWS_ACCESS_KEY_ID", "AKID"),
            ("AWS_SESSION_TOKEN", "TOKEN"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::SigV4MissingCredentials));
}

#[test]
fn sigv4_missing_session_token_errors() {
    let err = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE", "aps"),
            ("AWS_REGION", "us-east-1"),
            ("AWS_ACCESS_KEY_ID", "AKID"),
            ("AWS_SECRET_ACCESS_KEY", "SECRET"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap_err();
    assert!(matches!(err, ConfigError::SigV4MissingCredentials));
}

#[test]
fn managed_instances_overrides_default_to_continuously() {
    let config = Config::parse(
        &vars(&[("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318")]),
        RuntimeMode::ManagedInstances,
    )
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::Continuously { interval } if interval == Duration::from_secs(60)
    ));
}

#[test]
fn managed_instances_preserves_explicit_continuously() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "continuously,5000"),
        ]),
        RuntimeMode::ManagedInstances,
    )
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::Continuously { interval } if interval == Duration::from_millis(5000)
    ));
}

#[test]
fn managed_instances_overrides_boundary_strategy() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end"),
        ]),
        RuntimeMode::ManagedInstances,
    )
    .unwrap();
    assert!(matches!(
        config.flush_strategy,
        FlushStrategy::Continuously { interval } if interval == Duration::from_secs(60)
    ));
}

#[test]
fn standard_mode_does_not_override_strategy() {
    let config = Config::parse(
        &vars(&[
            ("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318"),
            ("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end"),
        ]),
        RuntimeMode::Standard,
    )
    .unwrap();
    assert!(matches!(config.flush_strategy, FlushStrategy::End));
}
