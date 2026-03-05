use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

/// gRPC service paths for OTLP signals.
pub const TRACES_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
pub const METRICS_PATH: &str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
pub const LOGS_PATH: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

/// Encode a protobuf payload into a gRPC length-prefixed message.
///
/// Wire format: `[compressed:u8][length:u32 big-endian][payload]`
pub fn encode_frame(compressed: bool, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(u8::from(compressed));
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    buf.freeze()
}

#[derive(Debug, Error)]
#[error("gRPC error: status {status}{}", message.as_ref().map(|m| format!(", {m}")).unwrap_or_default())]
pub struct GrpcError {
    pub status: u32,
    pub message: Option<String>,
}

/// Check the `grpc-status` trailer from an HTTP/2 response.
///
/// Returns `Ok(())` for status 0 (OK) or missing trailers (some servers
/// only send trailers on error). Returns `Err` with the status code and
/// optional message otherwise.
pub fn check_grpc_status(trailers: Option<&hyper::HeaderMap>) -> Result<(), GrpcError> {
    let Some(trailers) = trailers else {
        return Ok(());
    };

    let Some(raw_status) = trailers.get("grpc-status") else {
        return Ok(());
    };

    let status: u32 = raw_status.to_str().unwrap_or("0").parse().unwrap_or(0);

    if status == 0 {
        return Ok(());
    }

    let message = trailers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    Err(GrpcError { status, message })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_frame_uncompressed() {
        let payload = b"hello";
        let frame = encode_frame(false, payload);
        assert_eq!(frame[0], 0); // not compressed
        assert_eq!(u32::from_be_bytes(frame[1..5].try_into().unwrap()), 5);
        assert_eq!(&frame[5..], b"hello");
    }

    #[test]
    fn encode_frame_compressed() {
        let payload = b"data";
        let frame = encode_frame(true, payload);
        assert_eq!(frame[0], 1); // compressed
        assert_eq!(u32::from_be_bytes(frame[1..5].try_into().unwrap()), 4);
        assert_eq!(&frame[5..], b"data");
    }

    #[test]
    fn encode_frame_empty_payload() {
        let frame = encode_frame(false, &[]);
        assert_eq!(frame.len(), 5);
        assert_eq!(u32::from_be_bytes(frame[1..5].try_into().unwrap()), 0);
    }

    #[test]
    fn check_status_ok() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert("grpc-status", "0".parse().unwrap());
        assert!(check_grpc_status(Some(&headers)).is_ok());
    }

    #[test]
    fn check_status_missing_trailers() {
        assert!(check_grpc_status(None).is_ok());
    }

    #[test]
    fn check_status_missing_header() {
        let headers = hyper::HeaderMap::new();
        assert!(check_grpc_status(Some(&headers)).is_ok());
    }

    #[test]
    fn check_status_error_with_message() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert("grpc-status", "13".parse().unwrap());
        headers.insert("grpc-message", "internal error".parse().unwrap());
        let err = check_grpc_status(Some(&headers)).unwrap_err();
        assert_eq!(err.status, 13);
        assert_eq!(err.message.as_deref(), Some("internal error"));
    }

    #[test]
    fn check_status_error_without_message() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert("grpc-status", "2".parse().unwrap());
        let err = check_grpc_status(Some(&headers)).unwrap_err();
        assert_eq!(err.status, 2);
        assert!(err.message.is_none());
    }
}
