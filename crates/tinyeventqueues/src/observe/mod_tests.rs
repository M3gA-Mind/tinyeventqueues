//! Unit tests for the outcome sinks.

use tinyeventqueues_bus::{Disposition, EventRef, Outcome, Reason};

use super::*;

#[test]
fn the_collecting_sink_keeps_outcomes_in_order() {
    let sink = CollectingSink::new();
    let first = Outcome::new(
        EventRef::new("s", "1"),
        Disposition::Accepted,
        Reason::Ingested,
        1,
    );
    let second = Outcome::new(
        EventRef::new("s", "2"),
        Disposition::Dropped,
        Reason::BotLoop,
        2,
    );
    sink.observe(&first);
    NoopSink.observe(&first);
    sink.observe(&second);
    assert_eq!(sink.outcomes(), vec![first, second]);
}
