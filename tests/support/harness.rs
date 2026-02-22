use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::AsyncBufReadExt;

use super::container_ext::{buf_contains, line_matches};

// ---------------------------------------------------------------------------
// Scenario — describes what the test handler does during a single invocation
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct Scenario {
    actions: Vec<Action>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum Action {
    #[serde(rename = "post_otlp")]
    PostOtlp { path: String, body: String },
}

impl Scenario {
    pub fn new() -> Self {
        Self { actions: vec![] }
    }

    /// The handler will POST to the extension's OTLP listener at the given path.
    pub fn post_otlp(mut self, path: &str, body: &[u8]) -> Self {
        self.actions.push(Action::PostOtlp {
            path: path.into(),
            body: BASE64.encode(body),
        });
        self
    }

    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

// ---------------------------------------------------------------------------
// LambdaTest — builder that configures the environment and starts a container
// ---------------------------------------------------------------------------

pub struct LambdaTest {
    env: Vec<(String, String)>,
    scenario: Option<Scenario>,
}

impl LambdaTest {
    pub fn new() -> Self {
        Self {
            env: vec![("LAMBDA_OTEL_RELAY_LOG_LEVEL".into(), "debug".into())],
            scenario: None,
        }
    }

    /// Override the extension's log level (default: debug).
    #[allow(dead_code)]
    pub fn log_level(mut self, level: &str) -> Self {
        self.env.retain(|(k, _)| k != "LAMBDA_OTEL_RELAY_LOG_LEVEL");
        self.env
            .push(("LAMBDA_OTEL_RELAY_LOG_LEVEL".into(), level.into()));
        self
    }

    /// Set an arbitrary environment variable on the container.
    #[allow(dead_code)]
    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Configure what the handler does on the first invocation.
    /// The closure receives a fresh `Scenario` builder.
    pub fn on_invoke(mut self, f: impl FnOnce(Scenario) -> Scenario) -> Self {
        self.scenario = Some(f(Scenario::new()));
        self
    }

    /// Start the container and return a `Harness`.
    pub async fn start(self) -> Harness {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            rustls::crypto::ring::default_provider()
                .install_default()
                .expect("failed to install rustls ring provider");
        });

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let extension_path = std::fs::canonicalize(
            manifest_dir.join("target/lambda/extensions/lambda-otel-relay"),
        )
        .expect(
            "Extension binary not found. Run `cargo lambda build --release --extension` first.",
        );

        let handler_path =
            std::fs::canonicalize(manifest_dir.join("target/lambda/test-handler/bootstrap"))
                .expect(
                "Test handler binary not found. Run `cargo lambda build --release --bin test-handler` first.",
            );

        // Create a unique temp directory for the scenario file.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let scenario_dir =
            std::env::temp_dir().join(format!("lambda-test-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&scenario_dir).expect("Failed to create scenario dir");

        if let Some(scenario) = &self.scenario {
            std::fs::write(scenario_dir.join("scenario.json"), scenario.to_json()).unwrap();
        }

        let mut image = GenericImage::new("mock-rie", "latest")
            .with_exposed_port(8080.tcp())
            .with_wait_for(WaitFor::message_on_stderr("exec '/var/runtime/bootstrap'"))
            .with_mount(testcontainers::core::Mount::bind_mount(
                extension_path.to_str().unwrap(),
                "/opt/lambda-otel-relay-bin",
            ))
            .with_mount(testcontainers::core::Mount::bind_mount(
                handler_path.to_str().unwrap(),
                "/var/runtime/bootstrap",
            ))
            .with_mount(testcontainers::core::Mount::bind_mount(
                scenario_dir.to_str().unwrap(),
                "/tmp/scenario",
            ))
            .with_cmd(["bootstrap"])
            .with_startup_timeout(Duration::from_secs(30))
            .with_env_var("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318");

        for (key, val) in &self.env {
            image = image.with_env_var(key, val);
        }

        let container = image
            .start()
            .await
            .expect("Failed to start Lambda RIE container");

        Harness {
            container,
            scenario_dir,
            invoke_count: std::cell::Cell::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Harness — owns a running container; provides invoke, reconfigure, shutdown
// ---------------------------------------------------------------------------

pub struct Harness {
    container: ContainerAsync<GenericImage>,
    scenario_dir: PathBuf,
    /// Tracks how many invocations have been made. Uses `Cell` so `invoke()` can
    /// take `&self` (matching `on_invoke(&self)`). `AtomicU32` is a drop-in
    /// replacement if `Sync` is ever needed.
    invoke_count: std::cell::Cell<u32>,
}

impl Harness {
    /// Reconfigure what the handler does on the next invocation.
    #[allow(dead_code)]
    pub fn on_invoke(&self, f: impl FnOnce(Scenario) -> Scenario) -> &Self {
        let scenario = f(Scenario::new());
        std::fs::write(self.scenario_dir.join("scenario.json"), scenario.to_json()).unwrap();
        self
    }

    /// Invoke the Lambda function and return structured results.
    pub async fn invoke(&self) -> InvokeResult {
        self.invoke_count.set(self.invoke_count.get() + 1);
        let expected = self.invoke_count.get();

        // POST to the RIE invocation endpoint.
        let host_port = self
            .container
            .get_host_port_ipv4(8080.tcp())
            .await
            .expect("Failed to get mapped port");

        let resp = reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{host_port}/2015-03-31/functions/function/invocations"
            ))
            .header("Content-Type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("Failed to invoke Lambda function");

        let body = resp.text().await.expect("Failed to read response body");

        // Wait for the extension to log "Received invoke event" the Nth time,
        // ensuring it has processed this invocation before we snapshot logs.
        let logs = self
            .wait_for_nth_occurrence("Received invoke event", expected)
            .await;

        // Parse handler action results from the response body.
        let results: Vec<ActionResult> = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("results").cloned())
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();

        InvokeResult {
            body,
            results,
            logs,
        }
    }

    /// Stop the container and return all captured logs.
    ///
    /// NOTE: This snapshots logs before the container is actually stopped (which
    /// happens when `Harness` is dropped). Shutdown-specific messages (e.g.
    /// "Received shutdown event") will not appear in the returned logs. To capture
    /// those, this method will need to explicitly stop the container before the snapshot.
    #[allow(dead_code)]
    pub async fn shutdown(self) -> Logs {
        let stdout_bytes = self.container.stdout_to_vec().await.unwrap_or_default();
        let stderr_bytes = self.container.stderr_to_vec().await.unwrap_or_default();
        Logs {
            stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
            stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
        }
    }

    /// Stream both stdout and stderr until the target message has appeared `n` times.
    async fn wait_for_nth_occurrence(&self, target: &str, n: u32) -> Logs {
        let timeout = Duration::from_secs(10);
        let result = tokio::time::timeout(timeout, async {
            let mut stdout_reader = self.container.stdout(true);
            let mut stderr_reader = self.container.stderr(true);
            let mut stdout_buf = String::new();
            let mut stderr_buf = String::new();
            let mut stdout_line = String::new();
            let mut stderr_line = String::new();
            let mut stdout_eof = false;
            let mut stderr_eof = false;
            let mut count = 0u32;
            loop {
                if stdout_eof && stderr_eof {
                    break;
                }
                tokio::select! {
                    result = stdout_reader.read_line(&mut stdout_line), if !stdout_eof => {
                        match result {
                            Ok(0) => stdout_eof = true,
                            Ok(_) => {
                                stdout_buf.push_str(&stdout_line);
                                if line_matches(stdout_line.trim(), target) {
                                    count += 1;
                                    if count >= n {
                                        return Logs { stdout: stdout_buf, stderr: stderr_buf };
                                    }
                                }
                                stdout_line.clear();
                            }
                            Err(e) => panic!("failed to read stdout: {e}"),
                        }
                    }
                    result = stderr_reader.read_line(&mut stderr_line), if !stderr_eof => {
                        match result {
                            Ok(0) => stderr_eof = true,
                            Ok(_) => {
                                stderr_buf.push_str(&stderr_line);
                                if line_matches(stderr_line.trim(), target) {
                                    count += 1;
                                    if count >= n {
                                        return Logs { stdout: stdout_buf, stderr: stderr_buf };
                                    }
                                }
                                stderr_line.clear();
                            }
                            Err(e) => panic!("failed to read stderr: {e}"),
                        }
                    }
                }
            }
            Logs {
                stdout: stdout_buf,
                stderr: stderr_buf,
            }
        })
        .await;

        match result {
            Ok(logs) => logs,
            Err(_) => {
                let stdout = self.container.stdout_to_vec().await.unwrap_or_default();
                let stderr = self.container.stderr_to_vec().await.unwrap_or_default();
                let logs = format!(
                    "{}{}",
                    String::from_utf8_lossy(&stdout),
                    String::from_utf8_lossy(&stderr),
                );
                panic!(
                    "Timed out waiting for {target:?} (occurrence {n}) after {timeout:?}.\nLogs:\n{logs}"
                );
            }
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.scenario_dir);
    }
}

// ---------------------------------------------------------------------------
// InvokeResult
// ---------------------------------------------------------------------------

pub struct InvokeResult {
    pub body: String,
    pub logs: Logs,
    results: Vec<ActionResult>,
}

#[derive(Deserialize, Default)]
struct ActionResult {
    action: String,
    path: Option<String>,
    status: Option<u16>,
}

impl InvokeResult {
    /// Get the HTTP status returned by a `post_otlp` action for the given path.
    pub fn otlp_status(&self, path: &str) -> u16 {
        self.results
            .iter()
            .find(|r| r.action == "post_otlp" && r.path.as_deref() == Some(path))
            .and_then(|r| r.status)
            .unwrap_or_else(|| panic!("No post_otlp result for path {path}"))
    }
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

pub struct Logs {
    pub stdout: String,
    pub stderr: String,
}

impl Logs {
    /// JSON-aware message check across both stdout and stderr.
    pub fn contains_message(&self, target: &str) -> bool {
        buf_contains(&self.stdout, target) || buf_contains(&self.stderr, target)
    }
}
