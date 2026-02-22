mod support;

use support::lambda::{invoke_function, setup, start_lambda_container_with_env};
use support::{LogStream, WaitForLog};

#[tokio::test]
async fn extension_registers_and_handles_invoke() {
    let ctx = setup();
    let container =
        start_lambda_container_with_env(&ctx, &[("LAMBDA_OTEL_RELAY_LOG_LEVEL", "debug")]).await;

    let body = invoke_function(&container).await;
    assert!(
        body.contains("statusCode"),
        "Lambda invoke should return handler response. Body: {body}"
    );

    let logs = container
        .wait_for_log(LogStream::Stdout("Received invoke event"))
        .await;

    assert!(
        logs.contains("Extension registered with Lambda Runtime API"),
        "Extension should have logged successful registration. Logs:\n{logs}"
    );
}

#[tokio::test]
async fn extension_registers_with_telemetry_api() {
    let ctx = setup();
    let container =
        start_lambda_container_with_env(&ctx, &[("LAMBDA_OTEL_RELAY_LOG_LEVEL", "debug")]).await;

    invoke_function(&container).await;

    let logs = container
        .wait_for_log(LogStream::Stdout(
            "Subscribed to Lambda Telemetry API on port 4319",
        ))
        .await;

    assert!(
        logs.contains("Subscribed to Lambda Telemetry API"),
        "Extension should have logged telemetry subscription. Logs:\n{logs}"
    );
}
