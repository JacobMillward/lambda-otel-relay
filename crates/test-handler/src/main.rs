mod collector;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use tracing::debug;

use test_handler::{Action, ActionResult, Scenario};

fn setup_logging() {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::prelude::*;

    tracing_subscriber::registry()
        .with(LevelFilter::DEBUG)
        .with(tracing_microjson::JsonLayer::new(std::io::stdout).with_target(true))
        .init();
}

#[tokio::main]
async fn main() {
    setup_logging();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring provider");

    let store = collector::start(4200);
    let client = reqwest::Client::new();
    let runtime_api = std::env::var("AWS_LAMBDA_RUNTIME_API").unwrap();

    loop {
        // 1. Get next invocation
        let resp = client
            .get(format!(
                "http://{runtime_api}/2018-06-01/runtime/invocation/next"
            ))
            .send()
            .await
            .unwrap();
        let request_id = resp
            .headers()
            .get("Lambda-Runtime-Aws-Request-Id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        debug!(request_id = %request_id, "Received invocation");

        // 2. Read scenario from event payload (invalid/empty = no-op)
        let scenario: Scenario = resp.json().await.unwrap_or(Scenario { actions: vec![] });

        // 3. Execute actions sequentially
        let mut results = vec![];
        for action in &scenario.actions {
            match action {
                Action::PostOtlp { path, body } => {
                    debug!(action = "post_otlp", path = %path, "Executing action");
                    let decoded = BASE64.decode(body).unwrap_or_default();
                    let status = client
                        .post(format!("http://localhost:4318{path}"))
                        .header("Content-Type", "application/x-protobuf")
                        .body(decoded)
                        .send()
                        .await
                        .map(|r| r.status().as_u16())
                        .unwrap_or(0);
                    results.push(ActionResult {
                        action: "post_otlp".into(),
                        path: Some(path.clone()),
                        status: Some(status),
                        collected: None,
                    });
                }
                Action::GetCollected {
                    timeout_ms,
                    min_expected,
                } => {
                    debug!(action = "get_collected", "Executing action");
                    let timeout = timeout_ms.unwrap_or(2000);
                    let min = min_expected.unwrap_or(1);
                    let exports = collector::drain(&store, timeout, min).await;
                    results.push(ActionResult {
                        action: "get_collected".into(),
                        path: None,
                        status: None,
                        collected: Some(exports),
                    });
                }
            }
        }

        // 4. Respond
        let response = serde_json::json!({
            "statusCode": 200,
            "results": results,
        });

        debug!(request_id = %request_id, "Sending response");

        client
            .post(format!(
                "http://{runtime_api}/2018-06-01/runtime/invocation/{request_id}/response"
            ))
            .body(response.to_string())
            .send()
            .await
            .unwrap();
    }
}
