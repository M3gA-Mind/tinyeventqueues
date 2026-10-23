//! Tests for opening host-provided storage and refusing missing storage.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_busproxy::{PerCallerEndpoint, StorageServer, serve};
use tinystoragedrivers_types::Scope;

use super::*;
use crate::ports::ErrorKind;
use crate::tinybus_module::tests::rig;

#[tokio::test]
async fn a_missing_callback_is_refused_even_when_the_global_host_is_serving_storage() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let refused = open(module, None).await;
    assert!(matches!(
        &refused,
        Err(crate::Error::Storage(error)) if error.kind() == ErrorKind::Unavailable
    ));
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("storage_callback")
    );
}

#[tokio::test]
async fn a_host_without_storage_refuses_setup_instead_of_using_memory() {
    let rig = rig(false).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(open(module, None).await.is_err());
}

#[tokio::test]
async fn a_storage_host_that_cannot_be_reached_fails_setup_instead_of_using_memory() {
    let rig = rig(false).await;
    // Something owns the storage name but serves no storage objects: a host
    // whose storage is present but failing.
    rig.host
        .request_name(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .unwrap();
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let callback = rig.host.unique_name().unwrap();
    let refused = open(module, Some(callback.as_str())).await;
    assert!(refused.is_err(), "fell back to memory: {refused:?}");
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("storage_callback")
    );
}

#[tokio::test]
async fn an_unadmitted_storage_destination_does_not_fall_back_to_global_storage() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = Arc::new(tinystoragedrivers_testkit::MemoryBackend::new());
    let providers = Arc::new(AtomicUsize::new(0));
    let served_backend = Arc::clone(&backend);
    let provider_calls = Arc::clone(&providers);
    let endpoint = PerCallerEndpoint::new(move |_sender| {
        provider_calls.fetch_add(1, Ordering::Relaxed);
        let storage = served_backend.for_scope(&Scope::new("local").ok()?).ok()?;
        Some(StorageServer::new(storage))
    });
    let _served = serve(&host, Arc::new(endpoint)).await.unwrap();
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();

    // The ordinary global host is live, but a caller-supplied callback name is
    // an authority boundary and must be used as-is.
    let refused = open(module, Some(":1.999999")).await;
    assert!(matches!(
        &refused,
        Err(crate::Error::Storage(error)) if error.kind() == ErrorKind::Unavailable
    ));
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("storage_callback")
    );
    assert_eq!(
        providers.load(Ordering::Relaxed),
        0,
        "the rejected callback fell through to the global storage provider"
    );
}

#[tokio::test]
async fn an_invalid_storage_destination_is_rejected_before_connecting() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let refused = open(module, Some("")).await;
    assert!(matches!(
        &refused,
        Err(crate::Error::Storage(error)) if error.kind() == ErrorKind::Unavailable
    ));
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("storage_callback")
    );
}

#[tokio::test]
async fn the_shared_storage_service_name_reaches_the_host_serving_it() {
    // A host that serves one storage service for every module names that
    // service instead of a per-instance callback binding.
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let opened = open(module, Some(tinystoragedrivers_busproxy::HOST_NAME)).await;
    assert!(
        opened.is_ok(),
        "the shared storage service was refused: {opened:?}"
    );
}

#[tokio::test]
async fn the_shared_storage_service_name_fails_setup_when_nobody_serves_it() {
    let rig = rig(false).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let refused = open(module, Some(tinystoragedrivers_busproxy::HOST_NAME)).await;
    assert!(refused.is_err(), "fell back to memory: {refused:?}");
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("storage_callback")
    );
}
