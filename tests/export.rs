mod support;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use flate2::read::GzDecoder;
use prost::Message;
use std::io::Read;

use support::harness::{LambdaTest, Scenario};
use support::proto::{
    ExportTraceServiceRequest, KeyValue, Resource, ResourceLogs, ResourceMetrics, ResourceSpans,
};

fn simple_trace_payload() -> Vec<u8> {
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(support::proto::AnyValue {
                        value: Some(
                            support::proto::opentelemetry::proto::common::v1::any_value::Value::StringValue(
                                "test-service".into(),
                            ),
                        ),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

fn simple_metrics_payload() -> Vec<u8> {
    use support::proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

fn simple_logs_payload() -> Vec<u8> {
    use support::proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

#[tokio::test]
async fn export_delivers_traces() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .start()
        .await;

    // Invoke 1: send trace data
    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &simple_trace_payload()))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    // Invoke 2: triggers export and retrieves collected
    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1, "expected 1 collected export");
    assert_eq!(collected[0].path, "/v1/traces");
}

#[tokio::test]
async fn export_delivers_all_signals() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .start()
        .await;

    // Invoke 1: send all three signal types
    let result = harness
        .invoke(
            Scenario::new()
                .post_otlp("/v1/traces", &simple_trace_payload())
                .post_otlp("/v1/metrics", &simple_metrics_payload())
                .post_otlp("/v1/logs", &simple_logs_payload()),
        )
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);
    assert_eq!(result.otlp_status("/v1/metrics"), 200);
    assert_eq!(result.otlp_status("/v1/logs"), 200);

    // Invoke 2: triggers export and retrieves collected
    let result = harness
        .invoke(Scenario::new().get_collected(None, Some(3)))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 3, "expected 3 collected exports");

    let mut paths: Vec<&str> = collected.iter().map(|e| e.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, vec!["/v1/logs", "/v1/metrics", "/v1/traces"]);
}

#[tokio::test]
async fn export_gzip_compression() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .start()
        .await;

    let payload = simple_trace_payload();
    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &payload))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1);
    assert_eq!(
        collected[0].content_encoding.as_deref(),
        Some("gzip"),
        "default compression should be gzip"
    );

    // Decompress and verify valid protobuf
    let compressed = BASE64.decode(&collected[0].body).unwrap();
    let mut decoder = GzDecoder::new(&compressed[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();
    let _parsed = ExportTraceServiceRequest::decode(&decompressed[..])
        .expect("decompressed body should be valid protobuf");
}

#[tokio::test]
async fn export_uncompressed() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .env("LAMBDA_OTEL_RELAY_COMPRESSION", "none")
        .start()
        .await;

    let payload = simple_trace_payload();
    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &payload))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1);
    assert!(
        collected[0].content_encoding.is_none(),
        "uncompressed export should have no content-encoding"
    );

    // Body should be raw protobuf (decodable directly)
    let raw = BASE64.decode(&collected[0].body).unwrap();
    let _parsed =
        ExportTraceServiceRequest::decode(&raw[..]).expect("body should be valid raw protobuf");
}

#[tokio::test]
async fn export_custom_headers() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .env(
            "LAMBDA_OTEL_RELAY_EXPORT_HEADERS",
            "x-api-key=abc123,x-tenant=foo",
        )
        .start()
        .await;

    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &simple_trace_payload()))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1);
    assert_eq!(
        collected[0].headers.get("x-api-key").map(|s| s.as_str()),
        Some("abc123")
    );
    assert_eq!(
        collected[0].headers.get("x-tenant").map(|s| s.as_str()),
        Some("foo")
    );
}

#[tokio::test]
async fn export_content_type() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .start()
        .await;

    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &simple_trace_payload()))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1);
    assert_eq!(
        collected[0].content_type.as_deref(),
        Some("application/x-protobuf")
    );
}

#[tokio::test]
async fn export_empty_buffer_no_export() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .start()
        .await;

    // Invoke 1: no OTLP data posted
    harness.invoke(Scenario::new()).await;

    // Invoke 2: check nothing was exported
    let result = harness
        .invoke(Scenario::new().get_collected(Some(500), Some(0)))
        .await;
    let collected = result.collected();
    assert!(
        collected.is_empty(),
        "empty buffer should produce no exports"
    );
}

// ---------------------------------------------------------------------------
// gRPC export tests
// ---------------------------------------------------------------------------

/// Decode a gRPC length-prefixed frame: [compressed:u8][length:u32 BE][payload]
fn decode_grpc_frame(body: &[u8]) -> (bool, Vec<u8>) {
    assert!(body.len() >= 5, "gRPC frame must be at least 5 bytes");
    let compressed = body[0] != 0;
    let len = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
    assert_eq!(
        body.len(),
        5 + len,
        "gRPC frame length mismatch: expected {} payload bytes, got {}",
        len,
        body.len() - 5
    );
    let payload = body[5..5 + len].to_vec();
    (compressed, payload)
}

#[tokio::test]
async fn export_grpc_traces() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .env("LAMBDA_OTEL_RELAY_PROTOCOL", "grpc")
        .env("LAMBDA_OTEL_RELAY_COMPRESSION", "none")
        .start()
        .await;

    let payload = simple_trace_payload();
    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &payload))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1);

    // gRPC service path
    assert_eq!(
        collected[0].path,
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
    );

    // content-type
    assert_eq!(
        collected[0].content_type.as_deref(),
        Some("application/grpc")
    );

    // te: trailers header
    assert_eq!(
        collected[0].headers.get("te").map(|s| s.as_str()),
        Some("trailers")
    );

    // Body is a gRPC frame containing valid protobuf
    let raw = BASE64.decode(&collected[0].body).unwrap();
    let (compressed, frame_payload) = decode_grpc_frame(&raw);
    assert!(!compressed, "uncompressed frame should have flag = 0");
    let _parsed = ExportTraceServiceRequest::decode(&frame_payload[..])
        .expect("gRPC frame payload should be valid protobuf");
}

#[tokio::test]
async fn export_grpc_gzip() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .env("LAMBDA_OTEL_RELAY_PROTOCOL", "grpc")
        .start()
        .await;

    let payload = simple_trace_payload();
    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &payload))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1);

    // gRPC compression uses grpc-encoding header, not content-encoding
    assert!(
        collected[0].content_encoding.is_none(),
        "gRPC should not use HTTP content-encoding"
    );
    assert_eq!(
        collected[0]
            .headers
            .get("grpc-encoding")
            .map(|s| s.as_str()),
        Some("gzip")
    );

    // Decode gRPC frame — compressed flag should be set
    let raw = BASE64.decode(&collected[0].body).unwrap();
    let (compressed, frame_payload) = decode_grpc_frame(&raw);
    assert!(compressed, "compressed frame should have flag = 1");

    // Decompress and verify valid protobuf
    let mut decoder = GzDecoder::new(&frame_payload[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();
    let _parsed = ExportTraceServiceRequest::decode(&decompressed[..])
        .expect("decompressed gRPC payload should be valid protobuf");
}

#[tokio::test]
async fn export_grpc_all_signals() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .env("LAMBDA_OTEL_RELAY_PROTOCOL", "grpc")
        .env("LAMBDA_OTEL_RELAY_COMPRESSION", "none")
        .start()
        .await;

    let result = harness
        .invoke(
            Scenario::new()
                .post_otlp("/v1/traces", &simple_trace_payload())
                .post_otlp("/v1/metrics", &simple_metrics_payload())
                .post_otlp("/v1/logs", &simple_logs_payload()),
        )
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);
    assert_eq!(result.otlp_status("/v1/metrics"), 200);
    assert_eq!(result.otlp_status("/v1/logs"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, Some(3)))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 3, "expected 3 collected exports");

    let mut paths: Vec<&str> = collected.iter().map(|e| e.path.as_str()).collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
            "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
        ]
    );

    // All should be gRPC
    for export in collected {
        assert_eq!(export.content_type.as_deref(), Some("application/grpc"));
    }
}

#[tokio::test]
async fn export_grpc_custom_headers() {
    let harness = LambdaTest::new()
        .env("LAMBDA_OTEL_RELAY_FLUSH_STRATEGY", "end")
        .env("LAMBDA_OTEL_RELAY_PROTOCOL", "grpc")
        .env("LAMBDA_OTEL_RELAY_COMPRESSION", "none")
        .env("LAMBDA_OTEL_RELAY_EXPORT_HEADERS", "x-api-key=secret")
        .start()
        .await;

    let result = harness
        .invoke(Scenario::new().post_otlp("/v1/traces", &simple_trace_payload()))
        .await;
    assert_eq!(result.otlp_status("/v1/traces"), 200);

    let result = harness
        .invoke(Scenario::new().get_collected(None, None))
        .await;
    let collected = result.collected();
    assert_eq!(collected.len(), 1);
    assert_eq!(
        collected[0].headers.get("x-api-key").map(|s| s.as_str()),
        Some("secret")
    );
}
