mod support;

use std::path::PathBuf;
use std::time::Duration;
use support::{LogStream, WaitForLog};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

struct TestContext {
    extension_path: PathBuf,
    bootstrap_path: PathBuf,
}

fn setup() -> TestContext {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("failed to install rustls ring provider");
    });

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let extension_path =
        std::fs::canonicalize(manifest_dir.join("target/lambda/extensions/lambda-otel-relay"))
            .expect(
                "Extension binary not found. Run `cargo lambda build --release --extension` first.",
            );

    let bootstrap_path = std::fs::canonicalize(manifest_dir.join("tests/fixtures/bootstrap"))
        .expect("Bootstrap fixture not found at tests/fixtures/bootstrap");

    TestContext {
        extension_path,
        bootstrap_path,
    }
}

/// Start a Lambda RIE container with the extension and bootstrap mounted.
/// The RIE uses lazy init — extensions only start on the first invocation.
async fn start_lambda_container(ctx: &TestContext) -> ContainerAsync<GenericImage> {
    start_lambda_container_with_env(ctx, &[]).await
}

async fn start_lambda_container_with_env(
    ctx: &TestContext,
    extra_env: &[(&str, &str)],
) -> ContainerAsync<GenericImage> {
    let mut image = GenericImage::new("public.ecr.aws/lambda/provided", "al2023")
        .with_exposed_port(8080.tcp())
        .with_wait_for(WaitFor::message_on_stderr("exec '/var/runtime/bootstrap'"))
        .with_mount(testcontainers::core::Mount::bind_mount(
            ctx.extension_path
                .to_str()
                .expect("extension path is not valid UTF-8"),
            "/opt/extensions/lambda-otel-relay",
        ))
        .with_mount(testcontainers::core::Mount::bind_mount(
            ctx.bootstrap_path
                .to_str()
                .expect("bootstrap path is not valid UTF-8"),
            "/var/runtime/bootstrap",
        ))
        .with_cmd(["bootstrap"])
        .with_startup_timeout(Duration::from_secs(30))
        .with_env_var("LAMBDA_OTEL_RELAY_ENDPOINT", "http://localhost:4318");

    for (key, val) in extra_env {
        image = image.with_env_var(*key, *val);
    }

    image
        .start()
        .await
        .expect("Failed to start Lambda RIE container")
}

async fn invoke_function(container: &ContainerAsync<GenericImage>) -> String {
    let host_port = container
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

    resp.text().await.expect("Failed to read response body")
}

#[test]
fn extension_binary_is_valid_linux_elf() {
    let ctx = setup();

    let bytes = std::fs::read(&ctx.extension_path).expect("failed to read extension binary");
    assert!(
        bytes.len() > 1000,
        "Binary suspiciously small: {} bytes",
        bytes.len()
    );
    assert_eq!(
        &bytes[..4],
        b"\x7fELF",
        "Binary is not a Linux ELF executable. Got magic bytes: {:?}",
        &bytes[..4]
    );
}

#[tokio::test]
async fn extension_registers_and_handles_invoke() {
    let ctx = setup();
    let container = start_lambda_container(&ctx).await;

    let body = invoke_function(&container).await;
    assert!(
        body.contains("statusCode"),
        "Lambda invoke should return handler response. Body: {body}"
    );

    let logs = container
        .wait_for_log(LogStream::Stdout("\"message\":\"invoke\""))
        .await;

    assert!(
        logs.contains("\"message\":\"registered\""),
        "Extension should have logged successful registration. Logs:\n{logs}"
    );
}

#[tokio::test]
async fn extension_returns_200_v1_traces() {
    let ctx = setup();
    let container =
        start_lambda_container_with_env(&ctx, &[("TEST_OTLP_PATH", "/v1/traces")]).await;

    let body = invoke_function(&container).await;
    assert!(
        body.contains("\"otlpStatus\":200") || body.contains("\"otlpStatus\": 200"),
        "OTLP listener should return 200 for POST /v1/traces. Body: {body}"
    );
}

#[tokio::test]
async fn extension_returns_200_v1_metrics() {
    let ctx = setup();
    let container =
        start_lambda_container_with_env(&ctx, &[("TEST_OTLP_PATH", "/v1/metrics")]).await;

    let body = invoke_function(&container).await;
    assert!(
        body.contains("\"otlpStatus\":200") || body.contains("\"otlpStatus\": 200"),
        "OTLP listener should return 200 for POST /v1/metrics. Body: {body}"
    );
}

#[tokio::test]
async fn extension_returns_200_v1_logs() {
    let ctx = setup();
    let container = start_lambda_container_with_env(&ctx, &[("TEST_OTLP_PATH", "/v1/logs")]).await;

    let body = invoke_function(&container).await;
    assert!(
        body.contains("\"otlpStatus\":200") || body.contains("\"otlpStatus\": 200"),
        "OTLP listener should return 200 for POST /v1/logs. Body: {body}"
    );
}
