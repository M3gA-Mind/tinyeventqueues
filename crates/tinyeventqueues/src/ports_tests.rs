//! Tests for the owner-local storage key encoding.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::key_of;

#[test]
fn composite_keys_escape_delimiters_and_reserved_prefixes() {
    let key = key_of(&["a/b%", "~lead", "tail~", "\0x"]).unwrap();
    assert_eq!(key.as_str(), "a%2Fb%25/~lead/tail~/%00x");
    assert_eq!(key_of(&["~lead", "tail"]).unwrap().as_str(), "%7Elead/tail");
}

#[test]
fn oversized_composite_keys_are_bounded_and_deterministic() {
    let long = "x".repeat(tinystoragedrivers_types::MAX_KEY_LEN + 1);
    let first = key_of(&[&long]).unwrap();
    let second = key_of(&[&long]).unwrap();
    assert_eq!(first, second);
    assert!(first.as_str().starts_with('~'));
    assert!(first.as_str().len() <= tinystoragedrivers_types::MAX_KEY_LEN);
}

#[test]
fn oversized_keys_hash_exactly_like_the_legacy_key_of() {
    // Golden values from the legacy `Key::of` (tinystoragedrivers-core
    // 0c6dcfce): `~` + lowercase unpadded base32 of the SHA-256 of the escaped
    // key. A different encoding would orphan dedupe and buffer rows written
    // before the port.
    let long = "x".repeat(600);
    assert_eq!(
        key_of(&[&long]).unwrap().as_str(),
        "~keylgptlq757kmlo3ecj5gesj2yrbaalzovk3acq6zbpxjw7g7eq"
    );
    let tail = "y".repeat(600);
    assert_eq!(
        key_of(&["a/b", &tail]).unwrap().as_str(),
        "~2w4dnc7zzw6x7ml5j4pvc3pb6t34fqpcabnkn6yi6a2gpeyx5gba"
    );
}
