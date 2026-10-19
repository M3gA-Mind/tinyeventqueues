//! Tests for the module's storage declaration.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use tinystoragedrivers_core::schema::{Item, Registry};

#[test]
fn every_collection_is_module_scoped_under_the_owner() {
    let schema = storage_schema();
    assert_eq!(schema.owner.as_str(), OWNER);
    assert_eq!(schema.items.len(), collection_specs().len());
    for item in &schema.items {
        assert_eq!(item.scope(), ScopeClass::Module);
        assert!(item.name().starts_with("teq."), "{}", item.name());
    }
}

#[test]
fn declared_specs_are_the_specs_the_module_ensures() {
    let declared: Vec<CollectionSpec> = storage_schema()
        .items
        .into_iter()
        .filter_map(|item| match item {
            Item::Collection { spec, version, .. } => {
                assert_eq!(version, COLLECTION_VERSION);
                Some(spec)
            }
            _ => None,
        })
        .collect();
    assert_eq!(declared, collection_specs());
}

#[test]
fn the_schema_composes_alone() {
    let registry = Registry::compose(vec![storage_schema()]);
    assert!(registry.is_ok(), "{:?}", registry.err());
}

#[test]
fn shipped_files_are_json_documents() {
    for text in [SHIPPED_QUEUES_JSON, SHIPPED_PIPELINE_JSON] {
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(value["version"], 1);
    }
}
