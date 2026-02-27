use std::fmt;
use std::time::Duration;

use thiserror::Error;
use tokio::time::{Instant, Interval, MissedTickBehavior};

/// Debounce window: flushes within this duration of the last flush are skipped.
const DEDUP_WINDOW: Duration = Duration::from_millis(100);

#[derive(Debug, Error)]
pub enum FlushStrategyError {
    #[error("unknown flush strategy: {0}")]
    UnknownStrategy(String),

    #[error("flush strategy {strategy} requires a positive integer parameter: {detail}")]
    InvalidParameter { strategy: String, detail: String },
}

#[derive(Debug, Clone)]
pub enum FlushStrategy {
    End,
    Periodically { interval: Duration },
}

impl FlushStrategy {
    pub fn parse(raw: &str) -> Result<Self, FlushStrategyError> {
        match raw {
            "" | "end" => Ok(FlushStrategy::End),
            _ if raw.starts_with("periodically") => {
                let ms = parse_ms_param("periodically", raw)?;
                Ok(FlushStrategy::Periodically {
                    interval: Duration::from_millis(ms),
                })
            }
            other => Err(FlushStrategyError::UnknownStrategy(other.to_owned())),
        }
    }
}

impl fmt::Display for FlushStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlushStrategy::End => write!(f, "end"),
            FlushStrategy::Periodically { interval } => {
                write!(f, "periodically,{}", interval.as_millis())
            }
        }
    }
}

/// Whether a timer-triggered flush should block the event loop or run in the background.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerMode {
    Sync,
    #[allow(dead_code)] // used by default/continuously strategies
    Background,
}

enum FlushTimer {
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
            FlushStrategy::Periodically { interval } => FlushTimer::Active {
                interval: build_interval(*interval),
            },
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
            FlushStrategy::Periodically { interval } => self.elapsed_since_flush() >= *interval,
        }
    }

    /// Whether to flush on a timer tick.
    pub fn should_flush_on_timer(&self) -> bool {
        if self.within_dedup_window() {
            return false;
        }
        match &self.strategy {
            FlushStrategy::End => false,
            FlushStrategy::Periodically { .. } => true,
        }
    }

    /// The timer mode for this strategy (sync or background).
    pub fn timer_mode(&self) -> TimerMode {
        match &self.strategy {
            FlushStrategy::End | FlushStrategy::Periodically { .. } => TimerMode::Sync,
        }
    }

    /// Record that a flush just completed.
    pub fn record_flush(&mut self) {
        self.last_flush = Some(Instant::now());
        self.reset_timer();
    }

    fn elapsed_since_flush(&self) -> Duration {
        self.last_flush
            .map(|t| t.elapsed())
            .unwrap_or(Duration::MAX)
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

fn build_interval(period: Duration) -> Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Reset so next_tick() waits for a full period instead of firing immediately.
    interval.reset();
    interval
}

/// Parse `"strategy,<ms>"` → ms as u64, validating the comma-separated parameter.
fn parse_ms_param(strategy: &str, raw: &str) -> Result<u64, FlushStrategyError> {
    let param = raw.strip_prefix(strategy).unwrap_or("");
    let param = param.strip_prefix(',').ok_or_else(|| {
        FlushStrategyError::InvalidParameter {
            strategy: strategy.to_owned(),
            detail: "missing comma-separated millisecond parameter".to_owned(),
        }
    })?;
    let ms: u64 = param.parse().map_err(|_| FlushStrategyError::InvalidParameter {
        strategy: strategy.to_owned(),
        detail: format!("{param:?} is not a valid positive integer"),
    })?;
    if ms == 0 {
        return Err(FlushStrategyError::InvalidParameter {
            strategy: strategy.to_owned(),
            detail: "interval must be greater than 0".to_owned(),
        });
    }
    Ok(ms)
}

#[cfg(test)]
mod tests;
