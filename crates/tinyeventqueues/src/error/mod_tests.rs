//! Unit tests for the crate-wide error type.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;

#[test]
fn config_errors_name_the_field() {
    let error = Error::config("lanes.digest.queue", "not declared");
    assert_eq!(
        error.to_string(),
        "pipeline config: lanes.digest.queue: not declared"
    );
}

#[test]
fn every_variant_renders_a_message() {
    let rendered = [
        Error::Storage(StorageError::conflict("x")).to_string(),
        Error::ConfigFile {
            path: "p.json".into(),
            source: std::io::Error::other("gone"),
        }
        .to_string(),
        Error::Manifest(StorageError::invalid_input("bad")).to_string(),
        Error::from(serde_json::from_str::<u8>("x").unwrap_err()).to_string(),
        Error::Host {
            method: "Deliver",
            message: "down".into(),
        }
        .to_string(),
        Error::UnknownDeadItem("x".into()).to_string(),
    ];
    assert!(rendered[0].starts_with("storage: "));
    assert_eq!(rendered[1], "pipeline config p.json: gone");
    assert!(rendered[2].starts_with("queue manifest: "));
    assert!(rendered[3].starts_with("record encoding: "));
    assert_eq!(rendered[4], "host callback Deliver: down");
    assert_eq!(rendered[5], "unknown dead item id \"x\"");
}

#[test]
fn is_a_standard_error() {
    fn assert_error<E: std::error::Error>(_: &E) {}
    assert_error(&Error::UnknownDeadItem(String::new()));
}
