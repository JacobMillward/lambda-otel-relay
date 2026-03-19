use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

/// gRPC service paths for OTLP signals.
pub const TRACES_PATH: &str = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
pub const METRICS_PATH: &str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";
pub const LOGS_PATH: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

/// Encode a protobuf payload into a gRPC length-prefixed message.
///
/// Wire format: `[compressed:u8][length:u32 big-endian][payload]`
///
/// Returns an error if the payload exceeds the gRPC maximum message size
/// (2^32 - 1 bytes ≈ 4 GiB).
pub fn encode_frame(compressed: bool, payload: &[u8]) -> Result<Bytes, GrpcError> {
    let len = u32::try_from(payload.len()).map_err(|_| GrpcError::PayloadTooLarge)?;
    let mut buf = BytesMut::with_capacity(5 + payload.len());
    buf.put_u8(u8::from(compressed));
    buf.put_u32(len);
    buf.put_slice(payload);
    Ok(buf.freeze())
}

#[derive(Debug, Error)]
pub enum GrpcError {
    #[error("gRPC error: status {status}{}", message.as_ref().map(|m| format!(", {m}")).unwrap_or_default())]
    Status {
        status: u32,
        message: Option<String>,
    },

    #[error("gRPC payload exceeds maximum frame size (4 GiB)")]
    PayloadTooLarge,
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

    Err(GrpcError::Status { status, message })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_frame_uncompressed() {
        let payload = b"hello";
        let frame = encode_frame(false, payload).unwrap();
        assert_eq!(frame[0], 0); // not compressed
        assert_eq!(u32::from_be_bytes(frame[1..5].try_into().unwrap()), 5);
        assert_eq!(&frame[5..], b"hello");
    }

    #[test]
    fn encode_frame_compressed() {
        let payload = b"data";
        let frame = encode_frame(true, payload).unwrap();
        assert_eq!(frame[0], 1); // compressed
        assert_eq!(u32::from_be_bytes(frame[1..5].try_into().unwrap()), 4);
        assert_eq!(&frame[5..], b"data");
    }

    #[test]
    fn encode_frame_empty_payload() {
        let frame = encode_frame(false, &[]).unwrap();
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
        assert!(
            matches!(err, GrpcError::Status { status: 13, ref message } if message.as_deref() == Some("internal error"))
        );
    }

    #[test]
    fn check_status_error_without_message() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert("grpc-status", "2".parse().unwrap());
        let err = check_grpc_status(Some(&headers)).unwrap_err();
        assert!(matches!(
            err,
            GrpcError::Status {
                status: 2,
                message: None
            }
        ));
    }
}
