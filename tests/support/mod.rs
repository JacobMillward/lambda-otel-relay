// Each integration test (`lifecycle.rs`, `otlp.rs`, `log_matching.rs`) compiles this
// module independently via `mod support;`, so items used by one test appear unused in others.
#![allow(unused)]

mod container_ext;
pub mod harness;
pub mod proto;

pub use container_ext::{LogLevel, buf_contains_source, line_matches_source};
