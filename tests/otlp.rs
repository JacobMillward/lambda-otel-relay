mod support;

use support::harness::LambdaTest;

#[tokio::test]
async fn extension_returns_200_v1_traces() {
    let harness = LambdaTest::new()
        .on_invoke(|s| s.post_otlp("/v1/traces", b"test-payload"))
        .start()
        .await;

    let result = harness.invoke().await;
    assert_eq!(
        result.otlp_status("/v1/traces"),
        200,
        "OTLP listener should return 200 for POST /v1/traces. Body: {}",
        result.body
    );
}

#[tokio::test]
async fn extension_returns_200_v1_metrics() {
    let harness = LambdaTest::new()
        .on_invoke(|s| s.post_otlp("/v1/metrics", b"test-payload"))
        .start()
        .await;

    let result = harness.invoke().await;
    assert_eq!(
        result.otlp_status("/v1/metrics"),
        200,
        "OTLP listener should return 200 for POST /v1/metrics. Body: {}",
        result.body
    );
}

#[tokio::test]
async fn extension_returns_200_v1_logs() {
    let harness = LambdaTest::new()
        .on_invoke(|s| s.post_otlp("/v1/logs", b"test-payload"))
        .start()
        .await;

    let result = harness.invoke().await;
    assert_eq!(
        result.otlp_status("/v1/logs"),
        200,
        "OTLP listener should return 200 for POST /v1/logs. Body: {}",
        result.body
    );
}
