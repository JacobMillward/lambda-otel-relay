use std::time::Duration;

use tokio::time;

use super::super::*;

#[tokio::test(start_paused = true)]
async fn always_flushes_at_boundary() {
    let mut coord = FlushCoordinator::new(FlushStrategy::EndPeriodically {
        interval: Duration::from_secs(60),
    });
    // First invocation
    assert!(coord.should_flush_at_boundary());
    coord.record_flush();

    // Even shortly after a flush (outside dedup), still flushes at boundary
    time::advance(Duration::from_secs(1)).await;
    assert!(coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn timer_fires_sync_flush() {
    let mut coord = FlushCoordinator::new(FlushStrategy::EndPeriodically {
        interval: Duration::from_secs(30),
    });
    coord.next_tick().await;
    assert!(coord.should_flush_on_timer());
    assert_eq!(coord.timer_mode(), TimerMode::Sync);
}

#[tokio::test(start_paused = true)]
async fn dedup_between_boundary_and_timer() {
    let mut coord = FlushCoordinator::new(FlushStrategy::EndPeriodically {
        interval: Duration::from_secs(60),
    });
    coord.record_flush();
    // Within 100ms dedup window
    assert!(!coord.should_flush_at_boundary());
    assert!(!coord.should_flush_on_timer());
}
