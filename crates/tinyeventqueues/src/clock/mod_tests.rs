//! Unit tests for the clocks.

use super::*;

#[test]
fn a_manual_clock_moves_only_when_told() {
    let clock = ManualClock::new(10);
    let read = clock.clock();
    assert_eq!(read(), 10);
    clock.advance(5);
    assert_eq!(clock.now(), 15);
    clock.set(3);
    assert_eq!(read(), 3);
}

#[test]
fn the_system_clock_reads_a_plausible_time() {
    // 2020-01-01 in epoch milliseconds.
    assert!(system_clock()() > 1_577_836_800_000);
}
