use std::time::Duration;

use tokio::time;

use super::super::*;

#[tokio::test(start_paused = true)]
async fn always_flushes_at_boundary() {
    let coord = FlushCoordinator::new(FlushStrategy::End);
    assert!(coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn does_not_flush_on_timer() {
    let coord = FlushCoordinator::new(FlushStrategy::End);
    assert!(!coord.should_flush_on_timer());
}

#[tokio::test(start_paused = true)]
async fn dedup_suppresses_boundary_flush() {
    let mut coord = FlushCoordinator::new(FlushStrategy::End);
    coord.record_flush();
    // Within 100ms dedup window
    assert!(!coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn dedup_expires_after_window() {
    let mut coord = FlushCoordinator::new(FlushStrategy::End);
    coord.record_flush();
    time::advance(Duration::from_millis(101)).await;
    assert!(coord.should_flush_at_boundary());
}

#[tokio::test(start_paused = true)]
async fn first_invocation_always_flushes() {
    // last_flush is None, so dedup doesn't apply
    let coord = FlushCoordinator::new(FlushStrategy::End);
    assert!(coord.should_flush_at_boundary());
}
