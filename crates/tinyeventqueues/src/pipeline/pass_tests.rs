//! End-to-end tests of one pipeline pass: ingress through the gate and the
//! debounce buffer into the lane queues.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use serde_json::json;
use tinyeventqueues_bus::{
    Disposition, EventKind, EventRef, InboundEvent, Lane, MediaRef, RateLimit, Reason, Sender,
};
use tinystoragedrivers_core::queue::NewMessage;

use crate::CollectingSink;
use crate::config::shipped_pipeline_config;
use crate::test_support::{Fixture, event, take_turns};

fn ids(events: &[InboundEvent]) -> Vec<&str> {
    events.iter().map(|e| e.event_id.as_str()).collect()
}

fn mention(id: &str, conversation: &str) -> InboundEvent {
    event("channel:slack", id, conversation).addressed(true)
}

#[tokio::test]
async fn unaddressed_chatter_becomes_context_for_the_next_turn() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let pipeline = fixture.builder().sink(sink.clone()).build().await.unwrap();
    pipeline
        .ingest(vec![
            event("channel:slack", "1", "general"),
            event("channel:slack", "2", "general"),
            mention("3", "general"),
        ])
        .await
        .unwrap();
    let report = pipeline.drain().await.unwrap();
    assert_eq!(report.ingested, 3);
    assert_eq!(report.flushed, 0);

    fixture.clock.advance(1_500);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 1);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(turns.len(), 1);
    assert_eq!(ids(&turns[0].events), ["3"]);
    assert_eq!(ids(&turns[0].context), ["1", "2"]);

    let context: Vec<_> = sink
        .outcomes()
        .into_iter()
        .filter(|o| o.disposition == Disposition::Context)
        .map(|o| (o.event.event_id, o.reason))
        .collect();
    assert_eq!(
        context,
        [
            ("1".into(), Reason::Unaddressed),
            ("2".into(), Reason::Unaddressed)
        ]
    );
    assert_eq!(pipeline.stats().await.unwrap().outcomes.context, 2);

    // The context was handed over: the next turn does not repeat it.
    pipeline
        .ingest(vec![mention("4", "general")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(1_500);
    pipeline.drain().await.unwrap();
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(turns[0].context.len(), 0);
}

#[tokio::test]
async fn bursts_coalesce_per_thread_without_merging_threads_and_keep_order() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let events = vec![
        mention("1", "c").thread("a"),
        mention("2", "c").thread("b"),
        mention("3", "c").thread("a"),
        mention("4", "c").thread("b"),
        mention("5", "c").thread("a"),
    ];
    pipeline.ingest(events).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 2);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    let mut by_thread: Vec<_> = turns
        .iter()
        .map(|t| (t.thread_key.clone().unwrap(), ids(&t.events).join(",")))
        .collect();
    by_thread.sort();
    assert_eq!(
        by_thread,
        [("a".into(), "1,3,5".into()), ("b".into(), "2,4".into())]
    );
}

#[tokio::test]
async fn media_and_commands_flush_in_the_same_pass() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    pipeline
        .ingest(vec![
            mention("1", "photos").with_media(MediaRef::new("image", "blob://1")),
            mention("2", "ops").text("/status"),
            mention("3", "slow"),
        ])
        .await
        .unwrap();
    assert_eq!(pipeline.drain().await.unwrap().flushed, 2);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    let mut conversations: Vec<_> = turns.iter().map(|t| t.conversation_key.as_str()).collect();
    conversations.sort_unstable();
    assert_eq!(conversations, ["ops", "photos"]);
}

#[tokio::test]
async fn a_looping_bot_is_dropped_with_a_visible_reason() {
    let fixture = Fixture::new();
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.bot_loop.max_events = 2;
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    let bot = |id: &str| {
        let mut event = mention(id, "c");
        event.sender = Sender::new("other-bot").as_bot();
        event
    };
    pipeline
        .ingest(vec![bot("1"), bot("2"), bot("3"), bot("4")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    let outcomes = pipeline.stats().await.unwrap().outcomes;
    assert_eq!(outcomes.dropped, 2);
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(ids(&turns[0].events), ["1", "2"]);
}

#[tokio::test]
async fn self_echoes_and_denied_senders_never_reach_a_lane() {
    let fixture = Fixture::new();
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.deny_senders = vec!["spammer".into()];
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    let mut echo = mention("1", "c");
    echo.sender = Sender::new("agent").as_self();
    let mut spam = mention("2", "c");
    spam.sender = Sender::new("spammer");
    pipeline.ingest(vec![echo, spam]).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 0);
    assert_eq!(pipeline.stats().await.unwrap().outcomes.dropped, 2);
}

#[tokio::test]
async fn rate_limited_events_spill_to_the_digest_lane() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.rate_limits.per_sender = Some(RateLimit::new(1, 1));
    let pipeline = fixture
        .builder()
        .config(config)
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    pipeline
        .ingest(vec![mention("1", "c"), mention("2", "c")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    let digest = take_turns(&fixture, &pipeline, Lane::Digest).await;
    assert_eq!(digest.len(), 1);
    assert_eq!(ids(&digest[0].events), ["2"]);
    assert_eq!(digest[0].reason, Reason::RateLimited);
    assert!(
        sink.outcomes()
            .iter()
            .any(|o| o.disposition == Disposition::Deferred && o.reason == Reason::RateLimited)
    );
}

#[tokio::test]
async fn a_delete_before_delivery_retracts_the_message() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let mut delete = mention("2", "c").target("1");
    delete.kind = EventKind::Delete;
    let mut edit = event("channel:slack", "3", "c").target("4").text("fixed");
    edit.kind = EventKind::Edit;
    pipeline
        .ingest(vec![
            mention("1", "c"),
            delete,
            mention("4", "c").text("tpyo"),
            edit,
        ])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    assert_eq!(pipeline.stats().await.unwrap().outcomes.dropped, 2);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(ids(&turns[0].events), ["4"]);
    assert_eq!(turns[0].events[0].text, "fixed");
    assert_eq!(turns[0].absorbed, [EventRef::new("channel:slack", "3")]);
}

#[tokio::test]
async fn unaddressed_work_goes_to_the_actionable_lane() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let mut trigger = event("composio:github", "1", "repo");
    trigger.kind = EventKind::Trigger;
    pipeline.ingest(vec![trigger]).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Actionable)
            .await
            .len(),
        1
    );
}

#[tokio::test]
async fn an_unreadable_ingress_message_is_dead_lettered() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let ingress = pipeline.queues().ingress.name;
    fixture
        .queues
        .enqueue(&ingress, &[NewMessage::new(json!({"not": "an envelope"}))])
        .await
        .unwrap();
    assert_eq!(pipeline.drain().await.unwrap().ingested, 1);
    let dead = pipeline.list_dead(10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason, "unreadable ingress envelope");
}

#[tokio::test]
async fn a_restart_mid_debounce_loses_nothing() {
    let fixture = Fixture::new();
    let first = fixture.pipeline().await;
    first.ingest(vec![mention("1", "c")]).await.unwrap();
    first.drain().await.unwrap();
    drop(first);
    let restarted = fixture.pipeline().await;
    restarted.ingest(vec![mention("2", "c")]).await.unwrap();
    restarted.drain().await.unwrap();
    fixture.clock.advance(10_000);
    restarted.drain().await.unwrap();
    let turns = take_turns(&fixture, &restarted, Lane::Interactive).await;
    assert_eq!(ids(&turns[0].events), ["1", "2"]);
}
