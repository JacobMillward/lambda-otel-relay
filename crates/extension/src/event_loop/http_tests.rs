use std::ops::ControlFlow;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::extensions_api::ExtensionsApiEvent;
use crate::flush_strategy::FlushStrategy;
use crate::testing::{MockApi, MockExporter, dummy_config};

use super::EventLoop;

/// The event loop must `Box::pin` the `next_event` future so that it
/// survives across `select!` iterations when another branch fires.
///
/// This test drives the real `EventLoop::run()` with a mock API that
/// blocks on `next_event` until explicitly released. We send OTLP
/// payloads via HTTP so they're processed while the next_event future
/// is still pending. The assertion task then verifies `next_event`
/// was called exactly once (the future wasn't dropped and recreated)
/// before releasing it to deliver a SHUTDOWN event.
#[tokio::test]
async fn next_event_future_not_dropped_by_channel_activity() {
    let (mock, state) = MockApi::new(vec![Ok(ExtensionsApiEvent::Shutdown {
        reason: "test".into(),
    })]);

    let config = dummy_config().await;
    let mut event_loop = EventLoop::new(&mock, MockExporter, &config).await.unwrap();

    // Send 2 OTLP payloads via HTTP to trigger the channel branch of select!.
    // The listener is already bound and accepting, so these are queued
    // in the channel buffer before run() starts.
    let client = reqwest::Client::new();
    for _ in 0..2 {
        let resp = client
            .post(format!(
                "http://127.0.0.1:{}/v1/traces",
                config.listener_port
            ))
            .body(b"\x0a\x00".to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // Process OTLP payload
        let _ = event_loop.tick().await;
    }

    // Release the mock to deliver SHUTDOWN event on next tick
    state.release.notify_one();
    let _ = event_loop.tick().await;

    assert_eq!(
        state.next_event_calls.load(Ordering::SeqCst),
        1,
        "next_event must be called exactly once — the future must not be dropped and recreated"
    );
}

/// Verify that the OTLP receive → threshold → spawn_flush path works
/// end-to-end through `tick()`.
#[tokio::test]
async fn threshold_triggers_background_flush_via_tick() {
    let (mock, state) = MockApi::new(vec![Ok(ExtensionsApiEvent::Shutdown {
        reason: "test".into(),
    })]);

    let mut config = dummy_config().await;
    config.buffer_max_bytes = Some(1);

    let mut event_loop = EventLoop::new(&mock, MockExporter, &config).await.unwrap();

    // Send a payload that exceeds the 1-byte threshold
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}/v1/traces",
            config.listener_port
        ))
        .body(b"\x0a\x00".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // This tick receives the payload and should trigger spawn_flush
    let _ = event_loop.tick().await;

    // Send shutdown to cleanly exit
    state.release.notify_one();
    let _ = event_loop.tick().await;

    // Buffer should be empty — the background flush exported everything
    assert!(event_loop.buffer.take().is_empty());
}

#[tokio::test]
async fn otlp_listener_crash_returns_exit_error() {
    let (mock, _state) = MockApi::new(vec![]);

    let config = dummy_config().await;

    let mut event_loop = EventLoop::new(&mock, MockExporter, &config).await.unwrap();

    // Kill the OTLP listener task — simulates a panic
    event_loop.otlp_task.abort();

    // tick() should detect the closed channel and return an error
    match event_loop.tick().await {
        ControlFlow::Break(Err(e)) => {
            assert!(e.to_string().contains("OTLP listener"), "error: {e}");
        }
        other => panic!("expected Break(Err(...)), got {other:?}"),
    }
}

/// Verify that the timer branch in select! fires and exports data for the
/// `continuously` strategy, where the timer is the sole flush mechanism
/// (no boundary flushing).
#[tokio::test]
async fn continuously_timer_flushes_buffer_via_tick() {
    let (mock, state) = MockApi::new(vec![Ok(ExtensionsApiEvent::Shutdown {
        reason: "test".into(),
    })]);

    let mut config = dummy_config().await;
    config.flush_strategy = FlushStrategy::Continuously {
        interval: Duration::from_millis(10),
    };

    let mut event_loop = EventLoop::new(&mock, MockExporter, &config).await.unwrap();

    // Send a payload via HTTP
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "http://127.0.0.1:{}/v1/traces",
            config.listener_port
        ))
        .body(b"\x0a\x00".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // tick() receives the OTLP payload into the buffer.
    // Continuously never flushes at boundary, so data stays buffered.
    let _ = event_loop.tick().await;

    // Wait for the timer interval to elapse
    tokio::time::sleep(Duration::from_millis(50)).await;

    // This tick should fire the timer branch (only resolvable branch),
    // triggering a background flush via spawn_flush.
    let _ = event_loop.tick().await;

    // Wait for the background flush task to complete
    event_loop.buffer.join_flush_task().await;

    // Buffer should be empty — timer-driven background flush exported everything
    assert!(event_loop.buffer.take().is_empty());

    // Clean shutdown
    state.release.notify_one();
    let _ = event_loop.tick().await;
}
