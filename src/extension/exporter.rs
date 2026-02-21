use crate::buffers::OutboundBuffer;
use url::Url;

/// Drain the buffer, batch per signal, and export to the external collector.
/// Produces at most three HTTP requests (one per signal type).
///
/// On failure the buffer is **not** cleared — data remains for retry
/// on the next flush trigger.
pub async fn export(_endpoint: &Url, _buffer: &mut OutboundBuffer) -> Result<(), ExportError> {
    // TODO: drain buffer
    // TODO: batch/merge payloads per signal
    // TODO: POST to collector
    Ok(())
}

#[derive(Debug)]
pub struct ExportError;

impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("export failed")
    }
}

impl std::error::Error for ExportError {}
