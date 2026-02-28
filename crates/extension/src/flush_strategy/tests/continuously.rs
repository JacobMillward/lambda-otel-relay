use std::time::Duration;

use tokio::time;

use super::super::*;

#[tokio::test(start_paused = true)]
async fn never_flushes_at_boundary() {
    let coord = FlushCoordinator::new(FlushStrategy::Continuously {
        interval: Duration::from_secs(60),
    });
    // Even on first invocation (last_flush = None), continuously never flushes at boundary
    assert!(!coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn never_flushes_at_boundary_after_long_gap() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Continuously {
        interval: Duration::from_secs(60),
    });
    coord.record_flush();
    time::advance(Duration::from_secs(300)).await;
    assert!(!coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn timer_fires_background_flush() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Continuously {
        interval: Duration::from_secs(60),
    });
    coord.next_tick().await;
    assert!(coord.should_flush_on_timer());
    assert_eq!(coord.timer_mode(), TimerMode::Background);
}

#[tokio::test(start_paused = true)]
async fn dedup_suppresses_timer() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Continuously {
        interval: Duration::from_secs(60),
    });
    coord.record_flush();
    // Within 100ms dedup window
    assert!(!coord.should_flush_on_timer());
}
