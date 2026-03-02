use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::time::Duration;

use thiserror::Error;
use url::Url;

use crate::flush_strategy::{FlushStrategy, FlushStrategyError};

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

    #[error("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY: {0}")]
    FlushStrategy(#[from] FlushStrategyError),

    #[error("{var}: cannot read file '{path}': {reason}")]
    CertificateFile {
        var: &'static str,
        path: String,
        reason: String,
    },

    #[error("LAMBDA_OTEL_RELAY_CLIENT_CERT and LAMBDA_OTEL_RELAY_CLIENT_KEY must both be set")]
    ClientIdentityIncomplete,

    #[error(
        "LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE is set but AWS credentials are missing \
             (need AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN)"
    )]
    SigV4MissingCredentials,

    #[error(
        "LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE is set but no AWS region found \
             (set LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_REGION, AWS_REGION, or AWS_DEFAULT_REGION)"
    )]
    SigV4MissingRegion,
}

/// Configuration for AWS SigV4 request signing.
///
/// Enabled by setting `LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE` to the target AWS
/// service code (e.g. `aps` for Amazon Managed Grafana, `xray` for X-Ray).
/// You can use [`aws service-quotas list-services`] to find service codes.
///
/// [`aws service-quotas list-services`]: https://docs.aws.amazon.com/cli/latest/reference/service-quotas/list-services.html
///
/// The signing region is read from `LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_REGION`, falling
/// back to `AWS_REGION` then `AWS_DEFAULT_REGION`.
///
/// AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
/// `AWS_SESSION_TOKEN`) must be present at startup and are re-read on each
/// export to handle Lambda credential rotation.
#[derive(Debug, Clone)]
pub struct SigV4Config {
    pub service: String,
    pub region: String,
}

#[derive(Debug)]
pub struct Config {
    pub endpoint: Url,
    pub listener_port: u16,
    pub telemetry_port: u16,
    pub export_timeout: Duration,
    pub compression: Compression,
    pub export_headers: Vec<(String, String)>,
    pub buffer_max_bytes: Option<usize>,
    pub flush_strategy: FlushStrategy,
    pub tls_ca: Option<Vec<u8>>,
    pub tls_client_cert: Option<Vec<u8>>,
    pub tls_client_key: Option<Vec<u8>>,
    pub sigv4: Option<SigV4Config>,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let vars: HashMap<String, String> = env::vars()
            .filter(|(k, _)| k.starts_with("LAMBDA_OTEL_RELAY_") || k.starts_with("AWS_"))
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
        let flush_strategy = vars
            .get("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY")
            .map(|s| s.as_str())
            .unwrap_or("")
            .parse()?;

        let tls_ca = parse_certificate_file(vars, "LAMBDA_OTEL_RELAY_CERTIFICATE")?;
        let tls_client_cert = parse_certificate_file(vars, "LAMBDA_OTEL_RELAY_CLIENT_CERT")?;
        let tls_client_key = parse_certificate_file(vars, "LAMBDA_OTEL_RELAY_CLIENT_KEY")?;

        if tls_client_cert.is_some() != tls_client_key.is_some() {
            return Err(ConfigError::ClientIdentityIncomplete);
        }

        let sigv4 = parse_sigv4(vars)?;

        Ok(Self {
            endpoint,
            listener_port,
            telemetry_port,
            export_timeout,
            compression,
            export_headers,
            buffer_max_bytes,
            flush_strategy,
            tls_ca,
            tls_client_cert,
            tls_client_key,
            sigv4,
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

fn parse_certificate_file(
    vars: &HashMap<String, String>,
    name: &'static str,
) -> Result<Option<Vec<u8>>, ConfigError> {
    match vars.get(name).filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(path_str) => {
            let path = Path::new(path_str);
            std::fs::read(path)
                .map(Some)
                .map_err(|e| ConfigError::CertificateFile {
                    var: name,
                    path: path_str.clone(),
                    reason: e.to_string(),
                })
        }
    }
}

fn parse_sigv4(vars: &HashMap<String, String>) -> Result<Option<SigV4Config>, ConfigError> {
    let service = match vars
        .get("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_SERVICE")
        .filter(|s| !s.is_empty())
    {
        Some(s) => s.clone(),
        None => return Ok(None),
    };

    let region = vars
        .get("LAMBDA_OTEL_RELAY_ENDPOINT_SIGV4_REGION")
        .or_else(|| vars.get("AWS_REGION"))
        .or_else(|| vars.get("AWS_DEFAULT_REGION"))
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or(ConfigError::SigV4MissingRegion)?;

    let has_key = vars.get("AWS_ACCESS_KEY_ID").is_some_and(|s| !s.is_empty());
    let has_secret = vars
        .get("AWS_SECRET_ACCESS_KEY")
        .is_some_and(|s| !s.is_empty());
    let has_token = vars.get("AWS_SESSION_TOKEN").is_some_and(|s| !s.is_empty());
    if !has_key || !has_secret || !has_token {
        return Err(ConfigError::SigV4MissingCredentials);
    }

    Ok(Some(SigV4Config { service, region }))
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
