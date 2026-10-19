//! Unit tests for cadence parsing and tick computation.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;

const HOUR: u64 = 60 * MINUTE_MS;
/// 2026-10-19 00:00:00 UTC, a Monday.
const MONDAY: u64 = 1_792_368_000_000;

#[test]
fn parses_every_form() {
    assert_eq!(Cadence::parse(" instant ").unwrap(), Cadence::Instant);
    assert_eq!(
        Cadence::parse("every(10m)").unwrap(),
        Cadence::Every(600_000)
    );
    assert_eq!(Cadence::parse("every(250ms)").unwrap(), Cadence::Every(250));
    assert_eq!(
        Cadence::parse("every(30s)").unwrap(),
        Cadence::Every(30_000)
    );
    assert_eq!(
        Cadence::parse("every(2h)").unwrap(),
        Cadence::Every(2 * HOUR)
    );
    assert_eq!(
        Cadence::parse("every(1d)").unwrap(),
        Cadence::Every(24 * HOUR)
    );
    assert!(matches!(
        Cadence::parse("cron(\"*/10 * * * *\")").unwrap(),
        Cadence::Cron(_)
    ));
    assert!(matches!(
        Cadence::parse("cron(0 9 * * 1-5)").unwrap(),
        Cadence::Cron(_)
    ));
}

#[test]
fn rejects_malformed_cadences_with_a_reason() {
    for (text, says) in [
        ("sometimes", "expected"),
        ("every(10)", "unit"),
        ("every(m)", "not a number"),
        ("every(10y)", "unknown unit"),
        ("every(0s)", "positive"),
        ("every(99999999999999999d)", "too long"),
        ("cron(* * *)", "five fields"),
        ("cron(61 * * * *)", "outside"),
        ("cron(x * * * *)", "not a number"),
        ("cron(*/0 * * * *)", "step"),
        ("cron(*/x * * * *)", "bad step"),
        ("cron(5-1 * * * *)", "empty"),
    ] {
        let error = Cadence::parse(text).unwrap_err();
        assert!(error.to_string().contains(says), "{text}: {error}");
    }
}

#[test]
fn every_ticks_are_aligned_to_the_epoch() {
    let ten = Cadence::Every(600_000);
    assert_eq!(ten.next_tick(0), Some(600_000));
    assert_eq!(ten.next_tick(599_999), Some(600_000));
    assert_eq!(ten.next_tick(600_000), Some(1_200_000));
    assert_eq!(Cadence::Instant.next_tick(5), None);
    assert_eq!(Cadence::Every(u64::MAX).next_tick(u64::MAX), None);
}

#[test]
fn civil_dates_are_right() {
    assert_eq!(civil(0), (1970, 1, 1));
    assert_eq!(civil(MONDAY / (24 * HOUR)), (2026, 10, 19));
    assert_eq!(civil(11_016), (2000, 2, 29));
}

#[test]
fn cron_finds_the_next_matching_minute() {
    let tick = |expr: &str, after: u64| Cron::parse(expr).unwrap().next_after(after).unwrap();
    assert_eq!(tick("*/10 * * * *", MONDAY), MONDAY + 10 * MINUTE_MS);
    assert_eq!(tick("*/10 * * * *", MONDAY + 1), MONDAY + 10 * MINUTE_MS);
    assert_eq!(
        tick("30 9 * * *", MONDAY),
        MONDAY + 9 * HOUR + 30 * MINUTE_MS
    );
    // Weekdays only: from Friday evening to Monday 09:00.
    let friday_evening = MONDAY + 4 * 24 * HOUR + 20 * HOUR;
    assert_eq!(
        tick("0 9 * * 1-5", friday_evening),
        MONDAY + 7 * 24 * HOUR + 9 * HOUR
    );
    // Sunday as 7.
    assert_eq!(tick("0 0 * * 7", MONDAY), MONDAY + 6 * 24 * HOUR);
    // Day of month or weekday: the 20th (a Tuesday) comes before Sunday.
    assert_eq!(tick("0 0 20 * 0", MONDAY), MONDAY + 24 * HOUR);
    // Lists, ranges with steps, a month.
    assert_eq!(
        tick("15,45 * * * *", MONDAY + 20 * MINUTE_MS),
        MONDAY + 45 * MINUTE_MS
    );
    assert_eq!(tick("0 1-23/2 * * *", MONDAY), MONDAY + HOUR);
    assert_eq!(
        tick("5/20 * * * *", MONDAY + 6 * MINUTE_MS),
        MONDAY + 25 * MINUTE_MS
    );
    assert_eq!(tick("0 0 1 12 *", MONDAY), 1_796_083_200_000);
}

#[test]
fn a_cron_that_never_fires_has_no_tick() {
    let never = Cron::parse("0 0 31 2 *").unwrap();
    assert_eq!(never.next_after(MONDAY), None);
    assert_eq!(Cadence::Cron(never).next_tick(MONDAY), None);
}

#[test]
fn fingerprints_tell_schedules_apart() {
    assert_eq!(Cadence::Instant.fingerprint(), "instant");
    assert_eq!(
        Cadence::parse("every(10m)").unwrap().fingerprint(),
        "every:600000"
    );
    assert_eq!(
        Cadence::parse("cron(\" */5  * * * * \")")
            .unwrap()
            .fingerprint(),
        "cron:*/5 * * * *"
    );
}
