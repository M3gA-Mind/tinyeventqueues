//! Unit tests for the bus name table.

use super::{INTERFACE, METHODS, OBJECT_PATH, host, methods};

fn path_form(interface: &str) -> String {
    format!("/{}", interface.replace('.', "/"))
}

#[test]
fn the_object_paths_are_the_interfaces_in_path_form() {
    assert_eq!(OBJECT_PATH, path_form(INTERFACE));
    assert_eq!(host::OBJECT_PATH, path_form(host::INTERFACE));
}

#[test]
fn every_member_is_listed_exactly_once() {
    for table in [METHODS, host::METHODS] {
        let mut sorted = table.to_vec();
        sorted.sort_unstable();
        let mut deduplicated = sorted.clone();
        deduplicated.dedup();
        assert_eq!(sorted, deduplicated);
    }
}

#[test]
fn the_method_tables_hold_the_declared_members() {
    assert_eq!(
        METHODS,
        [
            methods::INGEST,
            methods::CONFIGURE,
            methods::STATS,
            methods::LIST_DEAD,
            methods::REDRIVE,
            methods::DRAIN,
        ]
    );
    assert_eq!(
        host::METHODS,
        [host::methods::CLASSIFY_SALIENCE, host::methods::DELIVER]
    );
}

#[test]
fn no_member_name_is_empty() {
    assert!(METHODS.iter().chain(host::METHODS).all(|m| !m.is_empty()));
}
