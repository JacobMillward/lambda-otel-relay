use std::fmt;
use std::time::Duration;

use thiserror::Error;
use tokio::time::{Instant, Interval};

/// Debounce window: flushes within this duration of the last flush are skipped.
const DEDUP_WINDOW: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum FlushStrategyError {
    #[error("unknown flush strategy: {0}")]
    UnknownStrategy(String),

    #[allow(dead_code)] // used by parameterized strategies
    #[error("flush strategy {strategy} requires a positive integer parameter: {detail}")]
    InvalidParameter { strategy: String, detail: String },
}

#[derive(Debug, Clone)]
pub enum FlushStrategy {
    End,
}

impl FlushStrategy {
    pub fn parse(raw: &str) -> Result<Self, FlushStrategyError> {
        match raw {
            "" | "end" => Ok(FlushStrategy::End),
            other => Err(FlushStrategyError::UnknownStrategy(other.to_owned())),
        }
    }
}

impl fmt::Display for FlushStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlushStrategy::End => write!(f, "end"),
        }
    }
}

/// Whether a timer-triggered flush should block the event loop or run in the background.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerMode {
    #[allow(dead_code)] // used by periodically/end-periodically strategies
    Sync,
    Background,
}

enum FlushTimer {
    #[allow(dead_code)] // used by timer-based strategies
    Active { interval: Interval },
    Inactive,
}

pub struct FlushCoordinator {
    strategy: FlushStrategy,
    last_flush: Option<Instant>,
    timer: FlushTimer,
}

impl FlushCoordinator {
    pub fn new(strategy: FlushStrategy) -> Self {
        let timer = match &strategy {
            FlushStrategy::End => FlushTimer::Inactive,
        };
        Self {
            strategy,
            last_flush: None,
            timer,
        }
    }

    /// Await the next timer tick. Pends forever if the timer is inactive.
    pub async fn next_tick(&mut self) {
        match &mut self.timer {
            FlushTimer::Active { interval } => {
                interval.tick().await;
            }
            FlushTimer::Inactive => std::future::pending().await,
        }
    }

    /// Whether to flush at an INVOKE boundary.
    pub fn should_flush_at_boundary(&self) -> bool {
        if self.within_dedup_window() {
            return false;
        }
        match &self.strategy {
            FlushStrategy::End => true,
        }
    }

    /// Whether to flush on a timer tick.
    pub fn should_flush_on_timer(&self) -> bool {
        if self.within_dedup_window() {
            return false;
        }
        match &self.strategy {
            FlushStrategy::End => false,
        }
    }

    /// The timer mode for this strategy (sync or background).
    pub fn timer_mode(&self) -> TimerMode {
        match &self.strategy {
            FlushStrategy::End => TimerMode::Background,
        }
    }

    /// Record that a flush just completed.
    pub fn record_flush(&mut self) {
        self.last_flush = Some(Instant::now());
        self.reset_timer();
    }

    fn within_dedup_window(&self) -> bool {
        self.last_flush
            .map(|t| t.elapsed() < DEDUP_WINDOW)
            .unwrap_or(false)
    }

    fn reset_timer(&mut self) {
        if let FlushTimer::Active { interval } = &mut self.timer {
            interval.reset();
        }
    }
}

#[cfg(test)]
mod tests;
