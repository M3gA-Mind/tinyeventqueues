//! Unit tests for the pipeline: construction, restart behavior, and the
//! operational members.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{Disposition, Lane};

use crate::Error;
use crate::config::{shipped_manifest, shipped_pipeline_config};
use crate::test_support::{Fixture, event};

#[tokio::test]
async fn a_redelivery_after_a_restart_is_rejected_on_both_backends() {
    for dedupe in [true, false] {
        let fixture = Fixture::with_dedupe(dedupe);
        let first = fixture.pipeline().await;
        let accepted = first
            .ingest(vec![event("channel:telegram", "7", "c")])
            .await
            .unwrap();
        assert_eq!(accepted.results[0].disposition, Disposition::Accepted);
        drop(first);

        // A new process over the same storage.
        let restarted = fixture.pipeline().await;
        let again = restarted
            .ingest(vec![event("channel:telegram", "7", "c")])
            .await
            .unwrap();
        assert_eq!(
            again.results[0].disposition,
            Disposition::Duplicate,
            "dedupe={dedupe}"
        );
        assert!(again.results[0].ack_platform);
        let ingress = restarted.queues().ingress.name;
        assert_eq!(fixture.queues.stats(&ingress).await.unwrap().ready, 1);
    }
}

#[tokio::test]
async fn a_processed_event_keeps_rejecting_redeliveries_until_its_tombstone_expires() {
    for dedupe in [true, false] {
        let fixture = Fixture::with_dedupe(dedupe);
        let pipeline = fixture.pipeline().await;
        let ingress = pipeline.queues().ingress;
        pipeline.ingest(vec![event("s", "1", "c")]).await.unwrap();
        // The next stage takes the event and acknowledges it.
        let taken = fixture
            .queues
            .receive(&ingress.name, 1, 1_000, "next")
            .await
            .unwrap();
        assert!(
            fixture
                .queues
                .ack(&ingress.name, &taken[0].receipt)
                .await
                .unwrap()
        );

        let restarted = fixture.pipeline().await;
        let again = restarted.ingest(vec![event("s", "1", "c")]).await.unwrap();
        assert_eq!(
            again.results[0].disposition,
            Disposition::Duplicate,
            "dedupe={dedupe}"
        );

        // Thirty days later the tombstone is gone.
        fixture
            .clock
            .advance(restarted.config().ingress.tombstone_ttl_ms + 1);
        restarted.drain().await.unwrap();
        let late = restarted.ingest(vec![event("s", "1", "c")]).await.unwrap();
        assert_eq!(
            late.results[0].disposition,
            Disposition::Accepted,
            "dedupe={dedupe}"
        );
    }
}

#[tokio::test]
async fn stats_report_every_queue_and_the_outcome_counts() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    pipeline
        .ingest(vec![event("s", "1", "c"), event("s", "2", "c")])
        .await
        .unwrap();
    let stats = pipeline.stats().await.unwrap();
    let names: Vec<_> = stats.queues.iter().map(|q| q.queue.clone()).collect();
    let expected: Vec<_> = pipeline
        .queues()
        .all()
        .iter()
        .map(|q| q.name.clone())
        .collect();
    assert_eq!(names, expected);
    assert_eq!(stats.queues[0].ready, 2);
    assert_eq!(stats.outcomes.total(), 0);
}

#[tokio::test]
async fn configure_swaps_the_config_or_keeps_the_old_one() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let mut config = shipped_pipeline_config().unwrap();
    config.lanes.interactive.concurrency = 1;
    pipeline.configure(config.clone()).unwrap();
    assert_eq!(pipeline.config(), config);

    let mut broken = config.clone();
    broken.lanes.background.queue = "teq.nowhere".to_owned();
    let error = pipeline.configure(broken).unwrap_err();
    assert!(error.to_string().contains("lanes.background.queue"));
    let mut out_of_range = config.clone();
    out_of_range.ingress.batch_size = 0;
    assert!(pipeline.configure(out_of_range).is_err());
    assert_eq!(pipeline.config(), config);
    assert_eq!(
        pipeline.queues().lane(Lane::Interactive).name,
        "teq.lane.interactive"
    );
}

#[tokio::test]
async fn the_builder_loads_files_from_disk_and_rejects_bad_values() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture = Fixture::new();
    let pipeline = fixture
        .builder()
        .manifest_path(root.join("queues.json"))
        .config_path(root.join("pipeline.json"))
        .consumer("worker-a")
        .build()
        .await
        .unwrap();
    assert_eq!(pipeline.config(), shipped_pipeline_config().unwrap());
    assert!(format!("{pipeline:?}").contains("Pipeline"));
    assert!(format!("{:?}", fixture.builder()).contains("PipelineBuilder"));

    let mut manifest = shipped_manifest().unwrap();
    manifest.version = 9;
    let error = fixture
        .builder()
        .manifest(manifest)
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Manifest(_)));

    let mut config = shipped_pipeline_config().unwrap();
    config.version = 2;
    let error = fixture.builder().config(config).build().await.unwrap_err();
    assert!(error.to_string().contains("version"));

    let mut config = shipped_pipeline_config().unwrap();
    config.ingress.queue = "teq.undeclared".to_owned();
    let error = fixture.builder().config(config).build().await.unwrap_err();
    assert!(error.to_string().contains("ingress.queue"));
}

#[tokio::test]
async fn the_manifest_declares_its_queues_on_build() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let manifest = shipped_manifest().unwrap();
    for declared in &manifest.queues {
        // Declared queues accept work and report stats.
        assert_eq!(
            fixture.queues.stats(&declared.name).await.unwrap(),
            tinystoragedrivers_core::queue::QueueStats::default()
        );
    }
    assert_eq!(pipeline.list_dead(10).await.unwrap(), Vec::new());
    assert!(!pipeline.redrive("teq.dead/none").await.unwrap());
}
