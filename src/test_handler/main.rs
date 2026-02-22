use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Scenario {
    actions: Vec<Action>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum Action {
    #[serde(rename = "post_otlp")]
    PostOtlp { path: String, body: String },
}

#[derive(Serialize)]
struct ActionResult {
    action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring provider");

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

        // 2. Read scenario (missing or invalid file = no-op)
        let scenario: Scenario = std::fs::read_to_string("/tmp/scenario/scenario.json")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or(Scenario { actions: vec![] });

        // 3. Execute actions sequentially
        let mut results = vec![];
        for action in &scenario.actions {
            match action {
                Action::PostOtlp { path, body } => {
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
                    });
                }
            }
        }

        // 4. Respond
        let response = serde_json::json!({
            "statusCode": 200,
            "results": results,
        });
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
