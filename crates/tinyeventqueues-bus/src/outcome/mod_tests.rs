//! Wire-form tests for outcomes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::json;

use super::{Disposition, Outcome, OutcomeCounts, Reason};
use crate::EventRef;

#[test]
fn only_the_five_end_states_are_terminal() {
    for disposition in Disposition::TERMINAL {
        assert!(disposition.is_terminal());
    }
    for disposition in [
        Disposition::Accepted,
        Disposition::Duplicate,
        Disposition::Deferred,
    ] {
        assert!(!disposition.is_terminal());
    }
}

#[test]
fn an_outcome_serializes_to_its_wire_form() {
    let outcome = Outcome::new(
        EventRef::new("s", "1"),
        Disposition::Dropped,
        Reason::BotLoop,
        5,
    );
    assert_eq!(
        serde_json::to_value(&outcome).unwrap(),
        json!({
            "event": {"source": "s", "event_id": "1"},
            "disposition": "dropped",
            "reason": "bot_loop",
            "at_ms": 5
        })
    );
    let delivered = outcome.in_turn("t1");
    let encoded = serde_json::to_string(&delivered).unwrap();
    assert_eq!(
        serde_json::from_str::<Outcome>(&encoded).unwrap(),
        delivered
    );
}

#[test]
fn counts_track_each_terminal_disposition() {
    let mut counts = OutcomeCounts::default();
    for (n, disposition) in (1..).zip(Disposition::TERMINAL) {
        counts.set(disposition, n);
        assert_eq!(counts.get(disposition), n);
    }
    counts.set(Disposition::Accepted, 99);
    assert_eq!(counts.get(Disposition::Accepted), 0);
    assert_eq!(counts.total(), 15);
}
