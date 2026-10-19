//! End-to-end tests of cadences and ingestion budgets, on a fake clock.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::sync::Arc;

use tinyeventqueues_bus::{
    Budget, Disposition, EventKind, InboundEvent, Lane, PipelineConfig, Reason, SourcePolicy,
};

use crate::config::shipped_pipeline_config;
use crate::test_support::{Fixture, event, take_turns};
use crate::{CollectingSink, Pipeline};

const TEN_MINUTES: u64 = 600_000;

fn trigger(source: &str, id: &str, conversation: &str, at: u64) -> InboundEvent {
    let mut event = event(source, id, conversation);
    event.kind = EventKind::Trigger;
    event.received_at_ms = at;
    event
}

fn scheduled(budget: Budget) -> PipelineConfig {
    let mut config = shipped_pipeline_config().unwrap();
    let mut x = SourcePolicy::new("composio:*");
    x.cadence = Some("every(10m)".into());
    x.budget = Some(budget);
    config.sources.insert(0, x);
    config.gate.rate_limits = tinyeventqueues_bus::RateLimits::default();
    config
}

/// Drain until the debounce buffers are flushed.
async fn settle(fixture: &Fixture, pipeline: &Pipeline) {
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
}

/// Move to the next ten-minute tick and drain.
async fn tick(fixture: &Fixture, pipeline: &Pipeline) {
    let now = fixture.clock.now();
    fixture.clock.set((now / TEN_MINUTES + 1) * TEN_MINUTES);
    pipeline.drain().await.unwrap();
}

#[tokio::test]
async fn a_scheduled_source_flooded_with_a_thousand_events_stays_within_budget_per_tick() {
    let fixture = Fixture::new();
    let mut budget = Budget::new(5);
    budget.backlog_ticks = 10;
    let pipeline = fixture
        .builder()
        .config(scheduled(budget))
        .build()
        .await
        .unwrap();
    let now = fixture.clock.now();
    let events: Vec<_> = (0..1_000)
        .map(|i| {
            trigger(
                "composio:twitter",
                &i.to_string(),
                &format!("thread-{}", i % 20),
                now,
            )
        })
        .collect();
    pipeline.ingest(events).await.unwrap();
    settle(&fixture, &pipeline).await;
    // Nothing goes out before the tick.
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        0
    );

    let mut delivered = BTreeMap::new();
    for _ in 0..4 {
        tick(&fixture, &pipeline).await;
        let turns = take_turns(&fixture, &pipeline, Lane::Background).await;
        assert_eq!(turns.len(), 5, "one tick releases exactly the budget");
        for turn in turns {
            // Each conversation's events folded into one pending turn.
            assert_eq!(turn.events.len(), 50);
            assert!(
                delivered
                    .insert(turn.conversation_key.clone(), turn.events.len())
                    .is_none()
            );
        }
    }
    assert_eq!(delivered.values().sum::<usize>(), 1_000);
    tick(&fixture, &pipeline).await;
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        0
    );
}

#[tokio::test]
async fn events_arriving_between_ticks_fold_into_the_waiting_entry() {
    let fixture = Fixture::new();
    let pipeline = fixture
        .builder()
        .config(scheduled(Budget::new(5)))
        .build()
        .await
        .unwrap();
    for (i, wait) in [(0, 0), (1, 60_000), (2, 60_000)] {
        fixture.clock.advance(wait);
        let at = fixture.clock.now();
        pipeline
            .ingest(vec![trigger("composio:x", &i.to_string(), "c", at)])
            .await
            .unwrap();
        settle(&fixture, &pipeline).await;
    }
    tick(&fixture, &pipeline).await;
    let turns = take_turns(&fixture, &pipeline, Lane::Background).await;
    assert_eq!(turns.len(), 1);
    let ids: Vec<_> = turns[0]
        .events
        .iter()
        .map(|e| e.event_id.as_str())
        .collect();
    assert_eq!(ids, ["0", "1", "2"]);
}

#[tokio::test]
async fn backlog_beyond_the_budget_folds_into_the_digest() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let mut budget = Budget::new(2);
    budget.backlog_ticks = 1;
    let pipeline = fixture
        .builder()
        .config(scheduled(budget))
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    let now = fixture.clock.now();
    let events: Vec<_> = (0..6)
        .map(|i| trigger("composio:x", &i.to_string(), &format!("c{i}"), now))
        .collect();
    pipeline.ingest(events).await.unwrap();
    settle(&fixture, &pipeline).await;
    tick(&fixture, &pipeline).await;
    // Two released, two wait for the next tick, the two oldest beyond that
    // fold into the digest.
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        2
    );
    let over: Vec<_> = sink
        .outcomes()
        .into_iter()
        .filter(|o| o.disposition == Disposition::Deferred && o.reason == Reason::OverBudget)
        .collect();
    assert_eq!(over.len(), 4);
    fixture.clock.advance(pipeline.config().digest.max_wait_ms);
    tick(&fixture, &pipeline).await;
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        2
    );
    let digest = take_turns(&fixture, &pipeline, Lane::Digest).await;
    assert_eq!(digest[0].events.len(), 2);
}

#[tokio::test]
async fn a_restart_between_ticks_neither_skips_nor_double_fires() {
    for native in [true, false] {
        restart_between_ticks(Fixture::with_native(native)).await;
    }
}

async fn restart_between_ticks(fixture: Fixture) {
    let config = scheduled(Budget::new(3));
    let first = fixture
        .builder()
        .config(config.clone())
        .build()
        .await
        .unwrap();
    let now = fixture.clock.now();
    let events: Vec<_> = (0..7)
        .map(|i| trigger("composio:x", &i.to_string(), &format!("c{i}"), now))
        .collect();
    first.ingest(events).await.unwrap();
    settle(&fixture, &first).await;
    drop(first);

    // Restart just before the tick: nothing fires early.
    let tick_at = (fixture.clock.now() / TEN_MINUTES + 1) * TEN_MINUTES;
    fixture.clock.set(tick_at - 1);
    let restarted = fixture
        .builder()
        .config(config.clone())
        .build()
        .await
        .unwrap();
    restarted.drain().await.unwrap();
    assert_eq!(
        take_turns(&fixture, &restarted, Lane::Background)
            .await
            .len(),
        0
    );

    // The tick fires once, however many passes and restarts follow it.
    fixture.clock.set(tick_at);
    restarted.drain().await.unwrap();
    restarted.drain().await.unwrap();
    let again = fixture.builder().config(config).build().await.unwrap();
    again.drain().await.unwrap();
    fixture.clock.set(tick_at + TEN_MINUTES - 1);
    again.drain().await.unwrap();
    assert_eq!(
        take_turns(&fixture, &again, Lane::Background).await.len(),
        3
    );

    // The next tick is not skipped.
    fixture.clock.set(tick_at + TEN_MINUTES);
    again.drain().await.unwrap();
    assert_eq!(
        take_turns(&fixture, &again, Lane::Background).await.len(),
        3
    );
}

#[tokio::test]
async fn interactive_work_is_promoted_past_a_scheduled_cadence() {
    let fixture = Fixture::new();
    let pipeline = fixture
        .builder()
        .config(scheduled(Budget::new(1)))
        .build()
        .await
        .unwrap();
    let now = fixture.clock.now();
    pipeline
        .ingest(vec![trigger("composio:x", "1", "c", now)])
        .await
        .unwrap();
    settle(&fixture, &pipeline).await;
    let mut urgent = trigger("composio:x", "2", "c", fixture.clock.now()).addressed(true);
    urgent.received_at_ms = fixture.clock.now();
    pipeline.ingest(vec![urgent]).await.unwrap();
    settle(&fixture, &pipeline).await;
    // The waiting entry of the same thread goes with it, in order, on the
    // interactive lane.
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(turns.len(), 1);
    let ids: Vec<_> = turns[0]
        .events
        .iter()
        .map(|e| e.event_id.as_str())
        .collect();
    assert_eq!(ids, ["1", "2"]);
    // Another thread with nothing waiting goes straight out.
    let mut fresh = trigger("composio:x", "3", "d", fixture.clock.now()).addressed(true);
    fresh.received_at_ms = fixture.clock.now();
    pipeline.ingest(vec![fresh]).await.unwrap();
    settle(&fixture, &pipeline).await;
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Interactive)
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn an_instant_source_over_budget_degrades_to_a_debounce_and_recovers() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.rate_limits = tinyeventqueues_bus::RateLimits::default();
    let mut hooks = SourcePolicy::new("webhook:*");
    let mut budget = Budget::new(2);
    budget.degrade_debounce_ms = 60_000;
    budget.backlog_ticks = 100;
    hooks.cadence = Some("instant".into());
    hooks.budget = Some(budget);
    config.sources.insert(0, hooks);
    let pipeline = fixture
        .builder()
        .config(config)
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    let now = fixture.clock.now();
    let burst: Vec<_> = (0..8)
        .map(|i| trigger("webhook:ci", &i.to_string(), &format!("c{i}"), now))
        .collect();
    pipeline.ingest(burst).await.unwrap();
    settle(&fixture, &pipeline).await;
    // Two go out at once; the rest wait out the debounce.
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        2
    );
    let degraded = sink
        .outcomes()
        .into_iter()
        .filter(|o| o.reason == Reason::OverBudget)
        .count();
    assert_eq!(degraded, 6);
    // A late event of a waiting thread folds into it instead of overtaking.
    let waiting = sink
        .outcomes()
        .into_iter()
        .find(|o| o.reason == Reason::OverBudget)
        .unwrap()
        .event
        .event_id;
    let late = trigger(
        "webhook:ci",
        "late",
        &format!("c{waiting}"),
        fixture.clock.now(),
    );
    pipeline.ingest(vec![late]).await.unwrap();
    settle(&fixture, &pipeline).await;
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        0
    );

    // Each debounce releases what the per-second budget allows.
    let mut released = 0;
    for _ in 0..4 {
        fixture.clock.advance(60_000);
        pipeline.drain().await.unwrap();
        let turns = take_turns(&fixture, &pipeline, Lane::Background).await;
        assert!(turns.len() <= 2);
        released += turns.len();
    }
    assert_eq!(released, 6);

    // Quiet again: the source is instant.
    fixture.clock.advance(60_000);
    let after = trigger("webhook:ci", "after", "fresh", fixture.clock.now());
    pipeline.ingest(vec![after]).await.unwrap();
    settle(&fixture, &pipeline).await;
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn a_cadence_removed_from_the_config_releases_its_waiting_turns() {
    let fixture = Fixture::new();
    let pipeline = fixture
        .builder()
        .config(scheduled(Budget::new(1)))
        .build()
        .await
        .unwrap();
    let now = fixture.clock.now();
    pipeline
        .ingest(vec![trigger("composio:x", "1", "c", now)])
        .await
        .unwrap();
    settle(&fixture, &pipeline).await;
    pipeline
        .configure(shipped_pipeline_config().unwrap())
        .unwrap();
    tick(&fixture, &pipeline).await;
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn a_changed_schedule_holds_early_entries_until_its_own_tick() {
    let fixture = Fixture::new();
    let mut config = shipped_pipeline_config().unwrap();
    config.lanes.background.cadence = Some("every(10m)".into());
    let pipeline = fixture
        .builder()
        .config(config.clone())
        .build()
        .await
        .unwrap();
    let now = fixture.clock.now();
    pipeline
        .ingest(vec![trigger("channel:x", "1", "c", now).addressed(false)])
        .await
        .unwrap();
    let mut t = trigger("monitor:y", "2", "d", now);
    t.kind = EventKind::Notification;
    pipeline.ingest(vec![t]).await.unwrap();
    settle(&fixture, &pipeline).await;
    // Move the lane to an hourly schedule: entries staged for the ten-minute
    // tick wait for the hour.
    config.lanes.background.cadence = Some("every(1h)".into());
    pipeline.configure(config).unwrap();
    tick(&fixture, &pipeline).await;
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        0
    );
}
