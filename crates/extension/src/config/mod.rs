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
mod tests;
