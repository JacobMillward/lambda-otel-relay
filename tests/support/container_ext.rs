#[derive(Clone, Copy, Debug)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

pub fn line_matches_source(
    line: &str,
    target_msg: &str,
    source: &str,
    level: Option<LogLevel>,
) -> bool {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line) else {
        return false;
    };

    let msg_matches = parsed
        .get("fields")
        .and_then(|f| f.get("message"))
        .and_then(|m| m.as_str())
        == Some(target_msg);

    let source_matches = parsed
        .get("target")
        .and_then(|t| t.as_str())
        .is_some_and(|t| t == source || t.starts_with(&format!("{source}::")));

    let level_matches =
        level.is_none_or(|lvl| parsed.get("level").and_then(|l| l.as_str()) == Some(lvl.as_str()));

    msg_matches && source_matches && level_matches
}

pub fn buf_contains_source(
    buf: &str,
    target_msg: &str,
    source: &str,
    level: Option<LogLevel>,
) -> bool {
    buf.lines()
        .any(|line| line_matches_source(line.trim(), target_msg, source, level))
}
