//! Wire-form tests for the host callback payloads.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::json;

use super::{
    ClassifySalienceRequest, ClassifySalienceResponse, DeliverRequest, DeliverResponse,
    DeliverStatus, SalienceClass, Turn,
};
use crate::{EventKind, InboundEvent, Lane, Sender};

fn event() -> InboundEvent {
    InboundEvent::new("s", "1", "c", Sender::new("u"), EventKind::Message, 1)
}

#[test]
fn a_turn_round_trips_and_knows_whether_it_is_a_digest() {
    let turn = Turn::new("t1", Lane::Interactive, "c", vec![event()]);
    assert!(!turn.is_digest());
    assert_eq!(turn.attempt, 1);
    let request = DeliverRequest::new(turn);
    let encoded = serde_json::to_string(&request).unwrap();
    assert_eq!(
        serde_json::from_str::<DeliverRequest>(&encoded).unwrap(),
        request
    );
    assert!(Turn::new("t2", Lane::Digest, "", vec![]).is_digest());
}

#[test]
fn deliver_statuses_are_tagged() {
    assert_eq!(
        serde_json::to_value(DeliverResponse::new(DeliverStatus::Accepted)).unwrap(),
        json!({"status": "accepted"})
    );
    assert_eq!(
        serde_json::to_value(DeliverResponse::new(DeliverStatus::Retry {
            after_ms: Some(5)
        }))
        .unwrap(),
        json!({"status": "retry", "after_ms": 5})
    );
    let rejected: DeliverResponse =
        serde_json::from_value(json!({"status": "rejected", "detail": "no"})).unwrap();
    assert_eq!(
        rejected.status,
        DeliverStatus::Rejected {
            detail: "no".into()
        }
    );
    let retry: DeliverResponse = serde_json::from_value(json!({"status": "retry"})).unwrap();
    assert_eq!(retry.status, DeliverStatus::Retry { after_ms: None });
}

#[test]
fn salience_classification_round_trips() {
    let request = ClassifySalienceRequest::new(event(), 55);
    let encoded = serde_json::to_string(&request).unwrap();
    assert_eq!(
        serde_json::from_str::<ClassifySalienceRequest>(&encoded).unwrap(),
        request
    );
    assert_eq!(
        serde_json::to_value(ClassifySalienceResponse::new(SalienceClass::Urgent)).unwrap(),
        json!({"class": "urgent"})
    );
}
