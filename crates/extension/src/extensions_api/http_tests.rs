use super::*;

async fn mock_api_client() -> (ExtensionApiClient, tokio::net::TcpListener) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let client = ExtensionApiClient {
        client: reqwest::Client::new(),
        runtime_api: format!("127.0.0.1:{port}"),
        ext_id: "test-ext-id".into(),
    };
    (client, listener)
}

/// Accept a single HTTP request, send a 202 response, and return the raw request bytes.
async fn accept_raw_request(listener: tokio::net::TcpListener) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    stream
        .write_all(b"HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\n\r\n")
        .await
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[tokio::test]
async fn report_init_error_sends_correct_request() {
    let (client, listener) = mock_api_client().await;
    let error = InitError::Config(ConfigError::EndpointMissing);

    let (_, raw) = tokio::join!(
        client.report_init_error(&error),
        accept_raw_request(listener)
    );

    assert!(raw.contains("POST /2020-01-01/extension/init/error"));
    assert!(raw.contains("lambda-extension-identifier: test-ext-id"));
    assert!(raw.contains("lambda-extension-function-error-type: Extension.ConfigInvalid"));
    assert!(raw.contains(r#""errorType":"Extension.ConfigInvalid""#));
    assert!(raw.contains(r#""errorMessage":"LAMBDA_OTEL_RELAY_ENDPOINT is required but not set""#));
}

#[tokio::test]
async fn report_exit_error_sends_correct_request() {
    let (client, listener) = mock_api_client().await;
    let error = ExitError::RuntimeFailure("segfault in handler".into());

    let (_, raw) = tokio::join!(
        client.report_exit_error(&error),
        accept_raw_request(listener)
    );

    assert!(raw.contains("POST /2020-01-01/extension/exit/error"));
    assert!(raw.contains("lambda-extension-identifier: test-ext-id"));
    assert!(raw.contains("lambda-extension-function-error-type: Extension.RuntimeFailure"));
    assert!(raw.contains(r#""errorType":"Extension.RuntimeFailure""#));
    assert!(raw.contains(r#""errorMessage":"segfault in handler""#));
}
