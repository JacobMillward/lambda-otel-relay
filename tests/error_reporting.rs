// Note: exit errors (listener task crashes) are tested at the unit level in
// event_loop::tests::otlp_listener_crash_returns_exit_error. An integration
// test would require injecting a runtime panic into a spawned task, which
// isn't feasible without test-only code in the production binary.

mod support;

use support::harness::LambdaTest;
use support::LogLevel;

/// When config is invalid, the extension should register with the Extensions API,
/// report the init error via `/extension/init/error`, and exit.
///
/// Note: The Lambda RIE buffers all subprocess output until the first invocation,
/// so we fire a dummy invoke to flush logs before asserting.
#[tokio::test]
async fn config_error_reports_init_error() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_COMPRESSION", "banana")
        .start()
        .await;

    let logs = harness
        .invoke_and_wait_for_extension_log("config error", Some(LogLevel::Error))
        .await;

    // Verify the error detail mentions the invalid value
    let combined = format!("{}{}", logs.stdout, logs.stderr);
    assert!(
        combined.contains("banana"),
        "Error should mention the invalid compression value. Logs:\n{combined}",
    );
}
