// Each integration test (`lifecycle.rs`, `otlp.rs`, `log_matching.rs`) compiles this
// module independently via `mod support;`, so items used by one test appear unused in others.
#![allow(unused)]

mod container_ext;
pub mod harness;

pub use container_ext::{LogLevel, buf_contains, buf_contains_source, line_matches, line_matches_source};
