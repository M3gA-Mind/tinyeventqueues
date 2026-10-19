//! Tests for the pass timer: wake-up delays, progress, and the slot.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use tinyeventqueues_bus::DrainReport;

use super::*;
use crate::test_support::{Fixture, event};

fn settings(min: u64, max: u64) -> TimerSettings {
    TimerSettings {
        enabled: true,
        min_interval_ms: min,
        max_interval_ms: max,
    }
}

#[test]
fn only_moved_work_counts_as_progress() {
    assert!(!progressed(&DrainReport::default()));
    let mut report = DrainReport::default();
    report.pruned = 3;
    assert!(!progressed(&report));
    report.dispatched = 1;
    assert!(progressed(&report));
}

#[tokio::test]
async fn the_delay_follows_the_next_wake_within_the_bounds() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    // Nothing waits: the longest interval.
    assert_eq!(
        delay(&pipeline, settings(10, 1_000), false).await,
        Duration::from_millis(1_000)
    );
    // Progress: the shortest.
    assert_eq!(
        delay(&pipeline, settings(10, 1_000), true).await,
        Duration::from_millis(10)
    );
    // Ready ingress: due now, floored at the shortest.
    pipeline.ingest(vec![event("s", "1", "c")]).await.unwrap();
    assert_eq!(
        delay(&pipeline, settings(10, 1_000), false).await,
        Duration::from_millis(10)
    );
    // A debounce buffer: until it flushes.
    pipeline.drain().await.unwrap();
    pipeline
        .ingest(vec![event("s", "2", "c").addressed(true)])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    let debounce = pipeline.config().coalesce.debounce_ms;
    let wait = delay(&pipeline, settings(10, 60_000), false).await;
    assert!(wait > Duration::from_millis(10) && wait <= Duration::from_millis(debounce));
}

#[tokio::test]
async fn a_pass_reports_progress_and_failures_do_not_stop_it() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    assert!(!pass(&pipeline).await);
    pipeline.ingest(vec![event("s", "1", "c")]).await.unwrap();
    assert!(pass(&pipeline).await);
}

#[tokio::test]
async fn a_new_setup_replaces_the_timer_and_disabling_stops_it() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let slot = Slot::default();
    assert!(!slot.running());
    let wake = start(&slot, pipeline.clone(), settings(5, 50));
    wake.now();
    assert!(slot.running());
    let _again = start(&slot, pipeline.clone(), settings(5, 50));
    assert!(slot.running());
    let disabled = TimerSettings {
        enabled: false,
        ..TimerSettings::default()
    };
    let _off = start(&slot, pipeline, disabled);
    assert!(!slot.running());
    assert!(format!("{slot:?}").contains("Slot"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_timer_runs_passes_on_its_own() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let slot = Slot::default();
    let wake = start(&slot, pipeline.clone(), settings(5, 20));
    pipeline.ingest(vec![event("s", "1", "c")]).await.unwrap();
    wake.now();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let ingress = pipeline.queues().ingress.name;
            if fixture.queues.stats(&ingress).await.unwrap().ready == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the timer took the ingress");
}
