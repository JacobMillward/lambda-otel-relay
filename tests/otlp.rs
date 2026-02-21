mod support;

use support::lambda::{invoke_function, setup, start_lambda_container_with_env};

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
