use std::time::Duration;
use testcontainers::{ContainerAsync, Image};
use tokio::io::AsyncBufReadExt;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[allow(dead_code)]
pub enum LogStream<'a> {
    Stdout(&'a str),
    Stderr(&'a str),
    Either(&'a str),
}

impl LogStream<'_> {
    fn message(&self) -> &str {
        match self {
            LogStream::Stdout(m) | LogStream::Stderr(m) | LogStream::Either(m) => m,
        }
    }
}

fn line_matches(line: &str, target: &str) -> bool {
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

fn buf_contains(buf: &str, target: &str) -> bool {
    buf.lines().any(|line| line_matches(line.trim(), target))
}

pub trait WaitForLog {
    /// Stream container logs until the message appears, with a default 10s timeout.
    /// Returns all captured log output on success, panics on timeout.
    async fn wait_for_log(&self, target: LogStream<'_>) -> String;

    /// Like `wait_for_log` but with a custom timeout.
    async fn wait_for_log_with_timeout(&self, target: LogStream<'_>, timeout: Duration) -> String;
}

impl<I: Image> WaitForLog for ContainerAsync<I> {
    async fn wait_for_log(&self, target: LogStream<'_>) -> String {
        self.wait_for_log_with_timeout(target, DEFAULT_TIMEOUT)
            .await
    }

    async fn wait_for_log_with_timeout(&self, target: LogStream<'_>, timeout: Duration) -> String {
        let message = target.message();

        let result = tokio::time::timeout(timeout, async {
            let mut buf = String::new();
            match target {
                LogStream::Stdout(_) | LogStream::Stderr(_) => {
                    let mut reader = match target {
                        LogStream::Stdout(_) => self.stdout(true),
                        _ => self.stderr(true),
                    };
                    let mut line = String::new();
                    loop {
                        match reader.read_line(&mut line).await {
                            Ok(0) => break,
                            Ok(_) => {
                                buf.push_str(&line);
                                if line_matches(line.trim(), message) {
                                    line.clear();
                                    return buf;
                                }
                                line.clear();
                            }
                            Err(e) => panic!("failed to read container logs: {e}"),
                        }
                    }
                }
                LogStream::Either(_) => {
                    let mut stdout = self.stdout(true);
                    let mut stderr = self.stderr(true);
                    let mut stdout_line = String::new();
                    let mut stderr_line = String::new();
                    let mut stdout_eof = false;
                    let mut stderr_eof = false;
                    loop {
                        if stdout_eof && stderr_eof {
                            break;
                        }
                        tokio::select! {
                            result = stdout.read_line(&mut stdout_line), if !stdout_eof => {
                                match result {
                                    Ok(0) => stdout_eof = true,
                                    Ok(_) => {
                                        buf.push_str(&stdout_line);
                                        if line_matches(stdout_line.trim(), message) {
                                            stdout_line.clear();
                                            return buf;
                                        }
                                        stdout_line.clear();
                                    }
                                    Err(e) => panic!("failed to read stdout: {e}"),
                                }
                            }
                            result = stderr.read_line(&mut stderr_line), if !stderr_eof => {
                                match result {
                                    Ok(0) => stderr_eof = true,
                                    Ok(_) => {
                                        buf.push_str(&stderr_line);
                                        if line_matches(stderr_line.trim(), message) {
                                            stderr_line.clear();
                                            return buf;
                                        }
                                        stderr_line.clear();
                                    }
                                    Err(e) => panic!("failed to read stderr: {e}"),
                                }
                            }
                        }
                    }
                }
            }
            buf
        })
        .await;

        match result {
            Ok(logs) => logs,
            Err(_) => {
                // On timeout, snapshot whatever logs exist for the error message.
                let stdout = self.stdout_to_vec().await.unwrap_or_default();
                let stderr = self.stderr_to_vec().await.unwrap_or_default();
                let logs = format!(
                    "{}{}",
                    String::from_utf8_lossy(&stdout),
                    String::from_utf8_lossy(&stderr),
                );
                panic!(
                    "Timed out waiting for {message:?} in container logs after {timeout:?}.\nLogs:\n{logs}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_matches_json_message_exact() {
        let line =
            r#"{"level":"DEBUG","fields":{"message":"Received invoke event","request_id":"abc"}}"#;
        assert!(line_matches(line, "Received invoke event"));
    }

    #[test]
    fn line_matches_json_message_no_match() {
        let line =
            r#"{"level":"DEBUG","fields":{"message":"Received invoke event","request_id":"abc"}}"#;
        assert!(!line_matches(line, "invoke"));
    }

    #[test]
    fn line_matches_plain_text_fallback() {
        assert!(line_matches("some plain log invoke event", "invoke"));
    }

    #[test]
    fn line_matches_plain_text_no_match() {
        assert!(!line_matches("some plain log", "invoke"));
    }

    #[test]
    fn buf_contains_finds_json_line() {
        let buf = "garbage\n{\"level\":\"DEBUG\",\"fields\":{\"message\":\"hello world\"}}\nmore\n";
        assert!(buf_contains(buf, "hello world"));
    }

    #[test]
    fn buf_contains_no_match() {
        let buf = "garbage\n{\"level\":\"DEBUG\",\"fields\":{\"message\":\"hello\"}}\n";
        assert!(!buf_contains(buf, "hello world"));
    }
}
