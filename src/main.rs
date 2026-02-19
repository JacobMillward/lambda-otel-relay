mod extensions_api;

use extensions_api::{ExtensionsApiEvent, ExtensionApiClient};

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring provider");

    let runtime_api =
        std::env::var("AWS_LAMBDA_RUNTIME_API").expect("AWS_LAMBDA_RUNTIME_API was not set in the environment. This variable is set by the Lambda service when the extension is invoked");

    let ext = ExtensionApiClient::register(&runtime_api)
        .await
        .expect("failed to register extension with Lambda runtime API");

    loop {
        match ext.next_event().await {
            Ok(ExtensionsApiEvent::Invoke { request_id }) => {
                eprintln!("invoke: requestId={request_id}");
            }
            Ok(ExtensionsApiEvent::Shutdown { reason }) => {
                eprintln!("shutdown: reason={reason}");
                break;
            }
            Err(e) => {
                eprintln!("error: {e}");
                continue;
            }
        }
    }
}
