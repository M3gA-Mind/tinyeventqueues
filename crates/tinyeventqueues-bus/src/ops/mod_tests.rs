//! Wire-form tests for the member payloads.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use super::*;
use crate::{Disposition, EventKind, EventRef, InboundEvent, OutcomeCounts, Reason, Sender};

fn round_trip<T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug>(value: &T) {
    let encoded = serde_json::to_string(value).unwrap();
    assert_eq!(&serde_json::from_str::<T>(&encoded).unwrap(), value);
}

#[test]
fn every_payload_round_trips() {
    round_trip(&IngestRequest::new(vec![InboundEvent::new(
        "s",
        "1",
        "c",
        Sender::new("u"),
        EventKind::Message,
        1,
    )]));
    round_trip(&IngestResponse::new(vec![IngestResult::new(
        EventRef::new("s", "1"),
        Disposition::Duplicate,
        Some(Reason::DuplicateRedelivery),
        true,
    )]));
    round_trip(&ConfigureResponse::new(true));
    round_trip(&StatsRequest::new());
    round_trip(&StatsResponse::new(
        vec![QueueDepth::new("q", 1, 2, 3, 4)],
        OutcomeCounts::default(),
    ));
    round_trip(&ListDeadRequest::new(10));
    round_trip(&ListDeadResponse::new(vec![DeadItem::new(
        "id",
        "teq.lane.interactive",
        "max attempts exceeded",
        3,
        vec![EventRef::new("s", "1")],
    )]));
    round_trip(&RedriveRequest::new("id"));
    round_trip(&RedriveResponse::new(true));
    round_trip(&DrainRequest::new());
    round_trip(&DrainResponse::new(DrainReport::default()));
}

#[test]
fn an_ingest_result_serializes_to_its_wire_form() {
    let result = IngestResult::new(EventRef::new("s", "1"), Disposition::Accepted, None, true);
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        json!({
            "event": {"source": "s", "event_id": "1"},
            "disposition": "accepted",
            "ack_platform": true
        })
    );
}

#[test]
fn an_empty_drain_report_decodes_from_an_empty_object() {
    let report: DrainReport = serde_json::from_value(json!({})).unwrap();
    assert_eq!(report, DrainReport::default());
}
