//! Tests for loading and validating the editable files, including the copies
//! shipped at the repository root.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::{Value, json};
use tinyeventqueues_bus::Lane;

use super::*;

fn shipped_value() -> Value {
    serde_json::from_str(SHIPPED_PIPELINE_JSON).unwrap()
}

fn parse(value: &Value) -> Result<PipelineConfig> {
    parse_pipeline_config(&value.to_string())
}

fn field_of(error: Error) -> String {
    match error {
        Error::Config { field, .. } => field,
        other => panic!("expected a config error, got {other}"),
    }
}

#[test]
fn the_shipped_files_load_and_resolve() {
    let manifest = shipped_manifest().unwrap();
    let config = shipped_pipeline_config().unwrap();
    let queues = resolve_queues(&config, &manifest).unwrap();
    assert_eq!(queues.ingress.name, config.ingress.queue);
    assert_eq!(queues.dead.name, config.dead_letter.queue);
    for lane in Lane::ALL {
        assert_eq!(queues.lane(lane).name, config.lanes.get(lane).queue);
    }
    assert_eq!(queues.all().len(), 6);
    assert!(queues.by_name(&config.ingress.queue).is_some());
    assert!(queues.by_name("teq.nope").is_none());
}

#[test]
fn every_shipped_queue_is_used_and_declares_a_lease() {
    let manifest = shipped_manifest().unwrap();
    let queues = resolve_queues(&shipped_pipeline_config().unwrap(), &manifest).unwrap();
    for declared in &manifest.queues {
        assert!(
            queues.by_name(&declared.name).is_some(),
            "{} unused",
            declared.name
        );
        assert!(declared.visibility_ms.is_some());
        assert_ne!(declared.description.len(), 0);
    }
}

#[test]
fn the_shipped_files_load_from_disk() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest = load_manifest(root.join("queues.json")).unwrap();
    assert_eq!(manifest, shipped_manifest().unwrap());
    let config = load_pipeline_config(root.join("pipeline.json")).unwrap();
    assert_eq!(config, shipped_pipeline_config().unwrap());
}

fn walk(schema: &Value, defs: &Value, value: &Value, path: &str) {
    let schema = match schema.get("$ref").and_then(Value::as_str) {
        Some(reference) => &defs[reference.trim_start_matches("#/$defs/")],
        None => schema,
    };
    match value {
        Value::Object(fields) => {
            assert_eq!(schema["additionalProperties"], json!(false), "{path}");
            for (key, field) in fields {
                let described = &schema["properties"][key];
                assert!(!described.is_null(), "{path}.{key} is not in the schema");
                walk(described, defs, field, &format!("{path}.{key}"));
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(&schema["items"], defs, item, &format!("{path}[]"));
            }
        }
        _ => {}
    }
}

#[test]
fn the_schema_describes_every_field_the_shipped_config_sets() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let schema: Value = serde_json::from_str(
        &std::fs::read_to_string(root.join("schemas/pipeline.schema.json")).unwrap(),
    )
    .unwrap();
    let full = serde_json::to_value(shipped_pipeline_config().unwrap()).unwrap();
    walk(&schema, &schema["$defs"], &full, "");
}

#[test]
fn unknown_fields_are_named_by_their_path() {
    let mut value = shipped_value();
    value["lanes"]["digest"]["concurency"] = json!(2);
    let error = parse(&value).unwrap_err();
    assert!(error.to_string().contains("concurency"));
    assert_eq!(field_of(error), "lanes.digest.concurency");
}

#[test]
fn wrong_types_and_missing_fields_are_named() {
    let mut value = shipped_value();
    value["ingress"]["batch_size"] = json!("many");
    assert_eq!(field_of(parse(&value).unwrap_err()), "ingress.batch_size");

    let mut value = shipped_value();
    value["sources"][1]["ack"] = json!("eventually");
    assert_eq!(field_of(parse(&value).unwrap_err()), "sources[1].ack");

    let mut value = shipped_value();
    value.as_object_mut().unwrap().remove("lanes");
    assert_eq!(field_of(parse(&value).unwrap_err()), "(root)");

    assert_eq!(field_of(parse_pipeline_config("{").unwrap_err()), "(root)");
}

#[test]
fn out_of_range_values_are_named() {
    type Break = fn(&mut Value);
    let cases: [(&str, Break); 6] = [
        ("version", |v| {
            v["version"] = json!(2);
        }),
        ("ingress.tombstone_ttl_ms", |v| {
            v["ingress"]["tombstone_ttl_ms"] = json!(0);
        }),
        ("ingress.batch_size", |v| {
            v["ingress"]["batch_size"] = json!(0);
        }),
        ("lanes.background.concurrency", |v| {
            v["lanes"]["background"]["concurrency"] = json!(5000);
        }),
        ("sources[0].match", |v| {
            v["sources"][0]["match"] = json!("");
        }),
        ("sources[2].match", |v| {
            v["sources"][2]["match"] = json!("a*b");
        }),
    ];
    for (field, break_it) in cases {
        let mut value = shipped_value();
        break_it(&mut value);
        assert_eq!(field_of(parse(&value).unwrap_err()), field);
    }
}

#[test]
fn queue_references_are_checked_against_the_manifest() {
    let manifest = shipped_manifest().unwrap();

    let mut config = shipped_pipeline_config().unwrap();
    config.lanes.actionable.queue = "teq.missing".to_owned();
    let error = resolve_queues(&config, &manifest).unwrap_err();
    assert!(error.to_string().contains("not declared"));
    assert_eq!(field_of(error), "lanes.actionable.queue");

    let mut config = shipped_pipeline_config().unwrap();
    config.lanes.digest.queue = config.ingress.queue.clone();
    let error = resolve_queues(&config, &manifest).unwrap_err();
    assert!(error.to_string().contains("already used by ingress.queue"));

    let mut without_lease = manifest.clone();
    without_lease.queues[0].visibility_ms = None;
    let error = resolve_queues(&shipped_pipeline_config().unwrap(), &without_lease).unwrap_err();
    assert_eq!(field_of(error), "ingress.queue");
}

#[test]
fn a_missing_file_is_reported_with_its_path() {
    let error = load_pipeline_config("/nonexistent/pipeline.json").unwrap_err();
    assert!(matches!(error, Error::ConfigFile { .. }));
    assert!(error.to_string().contains("/nonexistent/pipeline.json"));
    assert!(matches!(
        load_manifest("/nonexistent/queues.json").unwrap_err(),
        Error::Manifest(_)
    ));
    assert!(matches!(
        parse_manifest("{}").unwrap_err(),
        Error::Manifest(_)
    ));
}
