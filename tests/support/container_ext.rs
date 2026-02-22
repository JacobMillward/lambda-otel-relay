pub fn line_matches(line: &str, target: &str) -> bool {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(line)
        && let Some(msg) = parsed
            .get("fields")
            .and_then(|f| f.get("message"))
            .and_then(|m| m.as_str())
    {
        return msg == target;
    }
    line.contains(target)
}

pub fn buf_contains(buf: &str, target: &str) -> bool {
    buf.lines().any(|line| line_matches(line.trim(), target))
}
