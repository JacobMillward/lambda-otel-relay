mod support;

use support::harness::{LambdaTest, Scenario};

#[tokio::test]
async fn extension_registers_and_handles_invoke() {
    let harness = LambdaTest::new().start().await;

    let result = harness.invoke(Scenario::new()).await;
    assert!(
        result.body.contains("statusCode"),
        "Lambda invoke should return handler response. Body: {}",
        result.body
    );
    assert!(
        result
            .logs
            .contains_extension_message("Extension registered with Lambda Runtime API", None),
        "Extension should have logged successful registration. Logs:\n{}",
        result.logs.stderr
    );
    assert!(
        result
            .logs
            .contains_extension_message("Received invoke event", None),
        "Extension should have logged invoke event. Logs:\n{}",
        result.logs.stderr
    );
}

#[tokio::test]
async fn extension_registers_with_telemetry_api() {
    let harness = LambdaTest::new().start().await;

    let result = harness.invoke(Scenario::new()).await;
    assert!(
        result
            .logs
            .contains_extension_message("Subscribed to Lambda Telemetry API on port 4319", None),
        "Extension should have logged telemetry subscription. Logs:\n{}",
        result.logs.stderr
    );
}
