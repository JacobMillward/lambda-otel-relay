use nanoserde::DeJson;

#[derive(DeJson)]
struct RegisterResponse {
    #[nserde(rename = "functionName")]
    function_name: String,
    #[nserde(rename = "functionVersion")]
    function_version: String,
    handler: String,
}

#[derive(DeJson)]
struct EventResponse {
    #[nserde(rename = "eventType")]
    event_type: String,
    #[nserde(rename = "requestId")]
    request_id: Option<String>,
    #[nserde(rename = "shutdownReason")]
    shutdown_reason: Option<String>,
}

#[tokio::main]
async fn main() {
    let runtime_api =
        std::env::var("AWS_LAMBDA_RUNTIME_API").expect("AWS_LAMBDA_RUNTIME_API not set");
    let base_url = format!("http://{runtime_api}/2020-01-01/extension");

    rustls::crypto::ring::default_provider()
      .install_default()
      .expect("failed to install crypto provider");

    let client = reqwest::Client::new();

    // Register as an external extension
    let register_resp = client
        .post(format!("{base_url}/register"))
        .header("Lambda-Extension-Name", "lambda-otel-flush")
        .body(r#"{"events":["INVOKE","SHUTDOWN"]}"#)
        .send()
        .await
        .expect("failed to register extension");

    let ext_id = register_resp
        .headers()
        .get("Lambda-Extension-Identifier")
        .expect("missing Lambda-Extension-Identifier header")
        .to_str()
        .expect("non-ascii extension identifier")
        .to_owned();

    let body = register_resp
        .text()
        .await
        .expect("failed to read register response body");
    let reg: RegisterResponse =
        DeJson::deserialize_json(&body).expect("failed to parse register response");
    eprintln!(
        "registered: function={} version={} handler={}",
        reg.function_name, reg.function_version, reg.handler
    );

    // Event loop
    loop {
        let resp = client
            .get(format!("{base_url}/event/next"))
            .header("Lambda-Extension-Identifier", &ext_id)
            .send()
            .await
            .expect("failed to get next event");

        let body = resp
            .text()
            .await
            .expect("failed to read event response body");
        let event: EventResponse =
            DeJson::deserialize_json(&body).expect("failed to parse event response");

        match event.event_type.as_str() {
            "INVOKE" => {
                eprintln!(
                    "invoke: requestId={}",
                    event.request_id.as_deref().unwrap_or("unknown")
                );
            }
            "SHUTDOWN" => {
                eprintln!(
                    "shutdown: reason={}",
                    event.shutdown_reason.as_deref().unwrap_or("unknown")
                );
                break;
            }
            other => {
                eprintln!("unknown event type: {other}");
            }
        }
    }
}
