mod support;

use support::harness::LambdaTest;

#[tokio::test]
async fn extension_registers_and_handles_invoke() {
    let harness = LambdaTest::new().start().await;

    let result = harness.invoke().await;
    assert!(
        result.body.contains("statusCode"),
        "Lambda invoke should return handler response. Body: {}",
        result.body
    );
    assert!(
        result
            .logs
            .contains_message("Extension registered with Lambda Runtime API"),
        "Extension should have logged successful registration. Logs:\n{}",
        result.logs.stdout
    );
    assert!(
        result.logs.contains_message("Received invoke event"),
        "Extension should have logged invoke event. Logs:\n{}",
        result.logs.stdout
    );
}

#[tokio::test]
async fn extension_registers_with_telemetry_api() {
    let harness = LambdaTest::new().start().await;

    let result = harness.invoke().await;
    assert!(
        result
            .logs
            .contains_message("Subscribed to Lambda Telemetry API on port 4319"),
        "Extension should have logged telemetry subscription. Logs:\n{}",
        result.logs.stdout
    );
}
