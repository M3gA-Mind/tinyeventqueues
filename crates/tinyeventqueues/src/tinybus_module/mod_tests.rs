//! Tests for the `TinyBus` module adapter and its declared surface.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinybus::{Connection, Interface, Proxy};
use tinyeventqueues_bus::{
    ConfigureRequest, ConfigureResponse, Disposition, DrainRequest, DrainResponse, EventKind,
    InboundEvent, IngestRequest, IngestResponse, ListDeadRequest, ListDeadResponse, RedriveRequest,
    RedriveResponse, Sender, StatsRequest, StatsResponse, names,
};

use super::{EventQueuesService, memory_pipeline, setup};

#[tokio::test]
async fn declared_methods_match_the_dispatch_table() {
    let service = EventQueuesService {
        pipeline: memory_pipeline().await.unwrap(),
    };
    let methods = service
        .members()
        .into_iter()
        .map(|member| member.to_string())
        .collect::<Vec<_>>();
    assert_eq!(methods, names::METHODS.to_vec());
    assert_eq!(service.name().to_string(), names::INTERFACE);
}

async fn proxy() -> (Proxy, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    setup(service.clone()).await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = client
        .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
        .unwrap();
    // The service connection must outlive the calls.
    (proxy, service)
}

#[tokio::test]
async fn the_module_serves_every_member_over_a_real_bus() {
    let (proxy, _service) = proxy().await;
    let event = InboundEvent::new(
        "web:chat",
        "1",
        "c",
        Sender::new("u"),
        EventKind::Message,
        0,
    );

    let ingested: IngestResponse = proxy
        .call(
            names::methods::INGEST,
            (IngestRequest::new(vec![event.clone()]),),
        )
        .await
        .unwrap();
    assert_eq!(ingested.results[0].disposition, Disposition::Accepted);
    let again: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await
        .unwrap();
    assert_eq!(again.results[0].disposition, Disposition::Duplicate);

    let stats: StatsResponse = proxy
        .call(names::methods::STATS, (StatsRequest::new(),))
        .await
        .unwrap();
    assert_eq!(stats.queues[0].ready, 1);

    let config = crate::config::shipped_pipeline_config().unwrap();
    let configured: ConfigureResponse = proxy
        .call(names::methods::CONFIGURE, (ConfigureRequest::new(config),))
        .await
        .unwrap();
    assert!(configured.applied);

    let dead: ListDeadResponse = proxy
        .call(names::methods::LIST_DEAD, (ListDeadRequest::new(10),))
        .await
        .unwrap();
    assert_eq!(dead.dead.len(), 0);

    let drained: DrainResponse = proxy
        .call(names::methods::DRAIN, (DrainRequest::new(),))
        .await
        .unwrap();
    assert_eq!(drained.report.dead, 0);

    let missing = proxy
        .call::<RedriveResponse>(names::methods::REDRIVE, (RedriveRequest::new("nope"),))
        .await;
    assert!(
        missing
            .unwrap_err()
            .to_string()
            .contains("unknown dead item")
    );
    let absent: RedriveResponse = proxy
        .call(
            names::methods::REDRIVE,
            (RedriveRequest::new("teq.dead/none"),),
        )
        .await
        .unwrap();
    assert!(!absent.redriven);
}

#[tokio::test]
async fn an_invalid_configuration_is_refused_over_the_bus() {
    let (proxy, _service) = proxy().await;
    let mut config = crate::config::shipped_pipeline_config().unwrap();
    config.lanes.digest.queue = "teq.missing".to_owned();
    let refused = proxy
        .call::<ConfigureResponse>(names::methods::CONFIGURE, (ConfigureRequest::new(config),))
        .await;
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("lanes.digest.queue")
    );
}
