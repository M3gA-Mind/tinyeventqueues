//! Wire-form tests for the pipeline configuration.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::json;

use super::{AckPolicy, Lane, PipelineConfig, SourcePolicy};

fn minimal() -> serde_json::Value {
    json!({
        "version": 1,
        "ingress": {"queue": "teq.ingress"},
        "dead_letter": {"queue": "teq.dead"},
        "lanes": {
            "interactive": {"queue": "teq.lane.interactive"},
            "actionable": {"queue": "teq.lane.actionable"},
            "background": {"queue": "teq.lane.background"},
            "digest": {"queue": "teq.lane.digest", "concurrency": 1}
        }
    })
}

#[test]
fn a_minimal_file_fills_in_defaults() {
    let config: PipelineConfig = serde_json::from_value(minimal()).unwrap();
    assert_eq!(config.ingress.tombstone_ttl_ms, 2_592_000_000);
    assert_eq!(config.ingress.batch_size, 64);
    assert_eq!(config.lanes.get(Lane::Interactive).concurrency, 4);
    assert_eq!(config.lanes.get(Lane::Digest).concurrency, 1);
    assert_eq!(
        config.lanes.get(Lane::Actionable).queue,
        "teq.lane.actionable"
    );
    assert_eq!(
        config.lanes.get(Lane::Background).queue,
        "teq.lane.background"
    );
    assert_eq!(config.sources.len(), 0);
    assert!(config.schema.is_none());
}

#[test]
fn unknown_fields_are_rejected_at_every_level() {
    let mut top = minimal();
    top["surprise"] = json!(1);
    assert!(serde_json::from_value::<PipelineConfig>(top).is_err());

    let mut nested = minimal();
    nested["ingress"]["batchsize"] = json!(1);
    let error = serde_json::from_value::<PipelineConfig>(nested).unwrap_err();
    assert!(error.to_string().contains("batchsize"));
}

#[test]
fn the_schema_key_is_accepted() {
    let mut with_schema = minimal();
    with_schema["$schema"] = json!("./schemas/pipeline.schema.json");
    let config: PipelineConfig = serde_json::from_value(with_schema).unwrap();
    assert_eq!(
        config.schema.as_deref(),
        Some("./schemas/pipeline.schema.json")
    );
}

#[test]
fn source_patterns_match_exactly_by_prefix_or_everything() {
    assert!(SourcePolicy::new("channel:telegram").matches("channel:telegram"));
    assert!(!SourcePolicy::new("channel:telegram").matches("channel:discord"));
    assert!(SourcePolicy::new("channel:*").matches("channel:discord"));
    assert!(!SourcePolicy::new("channel:*").matches("webhook:x"));
    assert!(SourcePolicy::new("*").matches("anything"));
}

#[test]
fn the_first_matching_source_policy_wins() {
    let mut value = minimal();
    value["sources"] = json!([
        {"match": "channel:telegram", "ack": "after_turn"},
        {"match": "channel:*", "ack": "after_dispatch"}
    ]);
    let config: PipelineConfig = serde_json::from_value(value).unwrap();
    assert_eq!(
        config.source_policy("channel:telegram").ack,
        AckPolicy::AfterTurn
    );
    assert_eq!(
        config.source_policy("channel:slack").ack,
        AckPolicy::AfterDispatch
    );
    assert_eq!(
        config.source_policy("webhook:x").ack,
        AckPolicy::AfterIngest
    );
}

#[test]
fn lanes_have_stable_wire_names() {
    for lane in Lane::ALL {
        assert_eq!(serde_json::to_value(lane).unwrap(), json!(lane.as_str()));
    }
}
