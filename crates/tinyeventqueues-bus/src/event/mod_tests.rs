//! Wire-form tests for the inbound event envelope.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::json;

use super::{EventKind, EventRef, InboundEvent, MediaRef, PayloadRef, Sender};

fn event() -> InboundEvent {
    InboundEvent::new(
        "channel:telegram",
        "42",
        "chat-1",
        Sender::new("u1"),
        EventKind::Message,
        1_000,
    )
}

#[test]
fn a_minimal_event_serializes_without_empty_optionals() {
    assert_eq!(
        serde_json::to_value(event()).unwrap(),
        json!({
            "event_id": "42",
            "source": "channel:telegram",
            "conversation_key": "chat-1",
            "sender": {"id": "u1", "is_self": false, "is_bot": false},
            "kind": "message",
            "addressed_to_agent": false,
            "is_direct": false,
            "received_at_ms": 1000
        })
    );
}

#[test]
fn a_full_event_round_trips() {
    let full = event()
        .thread("t1")
        .text("hello")
        .addressed(true)
        .direct(true)
        .with_media(MediaRef::new("image", "blob://a"))
        .target("41")
        .raw(json!({"update_id": 7}));
    let mut full = full;
    full.raw_ref = Some(PayloadRef::new("blob://raw/42"));
    full.sender = Sender::new("u1").display("Ann").as_bot().as_self();
    let encoded = serde_json::to_string(&full).unwrap();
    assert_eq!(
        serde_json::from_str::<InboundEvent>(&encoded).unwrap(),
        full
    );
}

#[test]
fn kinds_use_snake_case_names() {
    for (kind, name) in [
        (EventKind::Message, "message"),
        (EventKind::Reply, "reply"),
        (EventKind::Mention, "mention"),
        (EventKind::Reaction, "reaction"),
        (EventKind::Edit, "edit"),
        (EventKind::Delete, "delete"),
        (EventKind::Trigger, "trigger"),
        (EventKind::Notification, "notification"),
    ] {
        assert_eq!(serde_json::to_value(kind).unwrap(), json!(name));
    }
}

#[test]
fn the_dedupe_key_joins_source_and_id() {
    assert_eq!(event().dedupe_key(), "channel:telegram:42");
    assert_eq!(event().reference(), EventRef::new("channel:telegram", "42"));
}

#[test]
fn an_event_missing_its_id_is_rejected() {
    let decoded = serde_json::from_value::<InboundEvent>(json!({
        "source": "s", "conversation_key": "c",
        "sender": {"id": "u"}, "kind": "message", "received_at_ms": 1
    }));
    assert!(decoded.is_err());
}
