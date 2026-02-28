use std::time::Duration;

use tokio::time;

use super::super::*;

#[tokio::test(start_paused = true)]
async fn first_invocation_always_flushes() {
    let coord = FlushCoordinator::new(FlushStrategy::Periodically {
        interval: Duration::from_secs(60),
    });
    // last_flush is None → elapsed = Duration::MAX → always >= interval
    assert!(coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn boundary_skip_when_under_interval() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Periodically {
        interval: Duration::from_secs(60),
    });
    coord.record_flush();
    time::advance(Duration::from_secs(10)).await;
    assert!(!coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn boundary_flush_when_interval_elapsed() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Periodically {
        interval: Duration::from_secs(60),
    });
    coord.record_flush();
    time::advance(Duration::from_secs(61)).await;
    assert!(coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn timer_fires_at_interval() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Periodically {
        interval: Duration::from_secs(5),
    });
    // Timer should fire after 5s
    coord.next_tick().await;
    assert!(coord.should_flush_on_timer());
    assert_eq!(coord.timer_mode(), TimerMode::Sync);
}

#[tokio::test(start_paused = true)]
async fn timer_reset_after_boundary_flush() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Periodically {
        interval: Duration::from_secs(60),
    });
    // Simulate a boundary flush at t=0
    coord.record_flush();

    // Advance 30s — timer should not have fired yet (reset to 60s from record_flush)
    time::advance(Duration::from_secs(30)).await;

    // Timer should fire at t=60s (30s more from now)
    coord.next_tick().await;
    assert!(coord.should_flush_on_timer());
}

#[tokio::test(start_paused = true)]
async fn dedup_between_boundary_and_timer() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Periodically {
        interval: Duration::from_secs(60),
    });
    // Simulate boundary flush
    coord.record_flush();
    // Timer fires within 100ms dedup window
    assert!(!coord.should_flush_on_timer());
}
