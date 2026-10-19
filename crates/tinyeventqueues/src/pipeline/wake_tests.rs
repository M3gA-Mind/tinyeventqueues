//! Tests for [`Pipeline::next_wake`]: ready work, debounce flushes and staged
//! cadence releases, on a fake clock.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{Budget, EventKind, SourcePolicy};

use crate::config::shipped_pipeline_config;
use crate::test_support::{Fixture, ScriptedHost, event};

#[tokio::test]
async fn an_idle_pipeline_waits_on_nothing() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    assert_eq!(pipeline.next_wake().await.unwrap(), None);
}

#[tokio::test]
async fn queued_ingress_wants_a_pass_now() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    pipeline
        .ingest(vec![event("channel:telegram", "1", "c")])
        .await
        .unwrap();
    assert_eq!(
        pipeline.next_wake().await.unwrap(),
        Some(fixture.clock.now())
    );
}

#[tokio::test]
async fn a_debounced_message_wakes_the_pass_when_its_buffer_flushes() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let addressed = event("channel:telegram", "1", "c").addressed(true);
    pipeline.ingest(vec![addressed]).await.unwrap();
    pipeline.drain().await.unwrap();
    let now = fixture.clock.now();
    let debounce = pipeline.config().coalesce.debounce_ms;
    let wake = pipeline.next_wake().await.unwrap().expect("a buffer waits");
    assert!(wake > now && wake <= now + debounce, "{wake} vs {now}");
}

#[tokio::test]
async fn a_staged_turn_wakes_the_pass_at_its_cadence_tick() {
    let fixture = Fixture::new();
    let mut config = shipped_pipeline_config().unwrap();
    let mut scheduled = SourcePolicy::new("composio:*");
    scheduled.cadence = Some("every(10m)".into());
    scheduled.budget = Some(Budget::new(5));
    config.sources.insert(0, scheduled);
    let pipeline = fixture
        .builder()
        .config(config)
        .host(ScriptedHost::accepting())
        .build()
        .await
        .unwrap();
    let mut trigger = event("composio:github", "1", "repo");
    trigger.kind = EventKind::Trigger;
    pipeline.ingest(vec![trigger]).await.unwrap();
    // Flush the debounce buffer so the turn is staged for the tick.
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();

    let now = fixture.clock.now();
    let wake = pipeline
        .next_wake()
        .await
        .unwrap()
        .expect("a turn is staged");
    assert!(wake > now, "{wake} vs {now}");
    assert_eq!(wake % 600_000, 0, "every(10m) ticks on the ten minutes");

    // At the tick the turn is released and delivered; nothing waits after.
    fixture.clock.set(wake);
    pipeline.drain().await.unwrap();
    pipeline.drain().await.unwrap();
    assert_eq!(pipeline.next_wake().await.unwrap(), None);
}
