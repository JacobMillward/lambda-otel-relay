use std::collections::HashMap;
use std::env;

use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("LAMBDA_OTEL_RELAY_ENDPOINT is required but not set")]
    EndpointMissing,

    #[error("LAMBDA_OTEL_RELAY_ENDPOINT is not a valid URL: {0}")]
    EndpointInvalidUrl(String),

    #[error("{0} has invalid value: {1}")]
    InvalidNumeric(String, String),
}

#[derive(Debug)]
pub struct Config {
    #[allow(dead_code)]
    pub endpoint: Url,
    pub listener_port: u16,
    #[allow(dead_code)]
    pub telemetry_port: u16,
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

        Ok(Self {
            endpoint,
            listener_port,
            telemetry_port,
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

pub fn fatal(err: &ConfigError) -> ! {
    eprintln!("{{\"level\":\"FATAL\",\"msg\":\"{err}\"}}");
    std::process::exit(1);
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
}
