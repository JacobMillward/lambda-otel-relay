use std::collections::HashMap;
use std::env;
use std::time::Duration;

use thiserror::Error;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Compression {
    Gzip,
    None,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("LAMBDA_OTEL_RELAY_ENDPOINT is required but not set")]
    EndpointMissing,

    #[error("LAMBDA_OTEL_RELAY_ENDPOINT is not a valid URL: {0}")]
    EndpointInvalidUrl(String),

    #[error("{0} has invalid value: {1}")]
    InvalidNumeric(String, String),

    #[error("LAMBDA_OTEL_RELAY_COMPRESSION has invalid value: {0} (expected \"gzip\" or \"none\")")]
    InvalidCompression(String),
}

#[derive(Debug)]
pub struct Config {
    pub endpoint: Url,
    pub listener_port: u16,
    #[allow(dead_code)]
    pub telemetry_port: u16,
    pub export_timeout: Duration,
    pub compression: Compression,
    pub export_headers: Vec<(String, String)>,
    pub buffer_max_bytes: Option<usize>,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let vars: HashMap<String, String> = env::vars()
            .filter(|(k, _)| k.starts_with("LAMBDA_OTEL_RELAY_"))
            .collect();
        Self::parse(&vars)
    }

    fn parse(vars: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let endpoint = parse_endpoint(vars)?;
        let listener_port = parse_port(vars, "LAMBDA_OTEL_RELAY_LISTENER_PORT", 4318)?;
        let telemetry_port = parse_port(vars, "LAMBDA_OTEL_RELAY_TELEMETRY_PORT", 4319)?;
        let export_timeout = parse_duration_ms(vars, "LAMBDA_OTEL_RELAY_EXPORT_TIMEOUT_MS", 5000)?;
        let compression = parse_compression(vars)?;
        let export_headers = parse_headers(vars);
        let buffer_max_bytes = parse_buffer_max_bytes(vars, "LAMBDA_OTEL_RELAY_BUFFER_MAX_BYTES")?;

        Ok(Self {
            endpoint,
            listener_port,
            telemetry_port,
            export_timeout,
            compression,
            export_headers,
            buffer_max_bytes,
        })
    }
}

fn parse_endpoint(vars: &HashMap<String, String>) -> Result<Url, ConfigError> {
    let raw = vars
        .get("LAMBDA_OTEL_RELAY_ENDPOINT")
        .filter(|s| !s.is_empty())
        .ok_or(ConfigError::EndpointMissing)?;

    Url::parse(raw).map_err(|_| ConfigError::EndpointInvalidUrl(raw.clone()))
}

fn parse_port(
    vars: &HashMap<String, String>,
    name: &str,
    default: u16,
) -> Result<u16, ConfigError> {
    match vars.get(name) {
        Some(val) => val
            .parse()
            .map_err(|_| ConfigError::InvalidNumeric(name.to_owned(), val.clone())),
        None => Ok(default),
    }
}

fn parse_duration_ms(
    vars: &HashMap<String, String>,
    name: &str,
    default_ms: u64,
) -> Result<Duration, ConfigError> {
    match vars.get(name) {
        Some(val) => {
            let ms: u64 = val
                .parse()
                .map_err(|_| ConfigError::InvalidNumeric(name.to_owned(), val.clone()))?;
            Ok(Duration::from_millis(ms))
        }
        None => Ok(Duration::from_millis(default_ms)),
    }
}

fn parse_compression(vars: &HashMap<String, String>) -> Result<Compression, ConfigError> {
    match vars
        .get("LAMBDA_OTEL_RELAY_COMPRESSION")
        .map(|s| s.as_str())
    {
        Some("gzip") | None => Ok(Compression::Gzip),
        Some("none") => Ok(Compression::None),
        Some(other) => Err(ConfigError::InvalidCompression(other.to_owned())),
    }
}

fn parse_buffer_max_bytes(
    vars: &HashMap<String, String>,
    name: &str,
) -> Result<Option<usize>, ConfigError> {
    match vars.get(name) {
        Some(val) => {
            let bytes: usize = val
                .parse()
                .map_err(|_| ConfigError::InvalidNumeric(name.to_owned(), val.clone()))?;
            if bytes == 0 {
                Ok(None)
            } else {
                Ok(Some(bytes))
            }
        }
        None => Ok(Some(4_194_304)), // 4 MiB default
    }
}

fn parse_headers(vars: &HashMap<String, String>) -> Vec<(String, String)> {
    vars.get("LAMBDA_OTEL_RELAY_EXPORT_HEADERS")
        .filter(|s| !s.is_empty())
        .map(|raw| {
            raw.split(',')
                .filter_map(|pair| {
                    let (k, v) = pair.split_once('=')?;
                    let k = k.trim();
                    let v = v.trim();
                    if k.is_empty() {
                        return None;
                    }
                    Some((k.to_owned(), v.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
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
}
