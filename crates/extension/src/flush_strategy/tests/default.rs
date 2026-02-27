use std::time::Duration;

use tokio::time;

use super::super::*;

#[tokio::test(start_paused = true)]
async fn first_invocation_always_flushes_at_boundary() {
    // Cold start: last_flush = None → elapsed = Duration::MAX → always >= 60s
    let coord = FlushCoordinator::new(FlushStrategy::Default);
    assert!(coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn low_traffic_flushes_at_boundary() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Default);
    coord.record_flush();
    // 90s gap — exceeds 60s threshold
    time::advance(Duration::from_secs(90)).await;
    assert!(coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn high_traffic_skips_boundary() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Default);
    coord.record_flush();
    // 5s gap — well under 60s threshold
    time::advance(Duration::from_secs(5)).await;
    assert!(!coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn timer_fires_background_flush() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Default);
    coord.next_tick().await;
    assert!(coord.should_flush_on_timer());
    assert_eq!(coord.timer_mode(), TimerMode::Background);
}

#[tokio::test(start_paused = true)]
async fn transition_low_to_high_traffic() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Default);

    // Low traffic: 90s gap → flushes at boundary
    coord.record_flush();
    time::advance(Duration::from_secs(90)).await;
    assert!(coord.should_flush_at_boundary());
    coord.record_flush();

    // Now high traffic: 5s gap → skips boundary
    time::advance(Duration::from_secs(5)).await;
    assert!(!coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn transition_high_to_low_traffic() {
    let mut coord = FlushCoordinator::new(FlushStrategy::Default);

    // High traffic: 5s gap → skips boundary
    coord.record_flush();
    time::advance(Duration::from_secs(5)).await;
    assert!(!coord.should_flush_at_boundary());

    // Now low traffic: 90s gap → resumes boundary flushing
    time::advance(Duration::from_secs(90)).await;
    assert!(coord.should_flush_at_boundary());
}
