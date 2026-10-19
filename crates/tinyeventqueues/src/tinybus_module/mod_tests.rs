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

use super::{BusHost, EventQueuesService, memory_pipeline, setup};
use crate::Host;

#[tokio::test]
async fn declared_methods_match_the_dispatch_table() {
    let service = EventQueuesService {
        pipeline: memory_pipeline(None).await.unwrap(),
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

/// A host serving the callback interface on the bus, accepting every turn.
#[derive(Clone, Default)]
struct FakeHost {
    turns: std::sync::Arc<std::sync::Mutex<Vec<tinyeventqueues_bus::Turn>>>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyeventqueues.Host")]
impl FakeHost {
    async fn classify_salience(
        &self,
        _request: tinyeventqueues_bus::ClassifySalienceRequest,
    ) -> tinybus::Result<tinyeventqueues_bus::ClassifySalienceResponse> {
        std::future::ready(Ok(tinyeventqueues_bus::ClassifySalienceResponse::new(
            tinyeventqueues_bus::SalienceClass::Urgent,
        )))
        .await
    }

    async fn deliver(
        &self,
        request: tinyeventqueues_bus::DeliverRequest,
    ) -> tinybus::Result<tinyeventqueues_bus::DeliverResponse> {
        self.turns.lock().unwrap().push(request.turn);
        std::future::ready(Ok(tinyeventqueues_bus::DeliverResponse::new(
            tinyeventqueues_bus::DeliverStatus::Accepted,
        )))
        .await
    }
}

#[test]
fn the_host_interface_matches_the_contract() {
    let host = FakeHost::default();
    let methods: Vec<_> = host.members().into_iter().map(|m| m.to_string()).collect();
    assert_eq!(methods, names::host::METHODS.to_vec());
    assert_eq!(host.name().to_string(), names::host::INTERFACE);
}

#[tokio::test]
async fn turns_are_delivered_to_the_host_over_the_bus() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host_side = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = FakeHost::default();
    host_side
        .serve_at(names::host::OBJECT_PATH.try_into().unwrap(), host.clone())
        .await
        .unwrap();
    host_side
        .request_name(names::host::INTERFACE)
        .await
        .unwrap();
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    setup(service.clone()).await.unwrap();
    let proxy = host_side
        .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
        .unwrap();

    let event = InboundEvent::new(
        "web:chat",
        "1",
        "c",
        Sender::new("u"),
        EventKind::Message,
        0,
    )
    .direct(true)
    .text("/help");
    let _: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await
        .unwrap();
    let drained: DrainResponse = proxy
        .call(names::methods::DRAIN, (DrainRequest::new(),))
        .await
        .unwrap();
    assert_eq!(drained.report.dispatched, 1);
    let turns = host.turns.lock().unwrap().clone();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].events[0].event_id, "1");
}

#[tokio::test]
async fn an_absent_host_makes_delivery_back_off() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let bus_host = BusHost {
        connection: service.clone(),
    };
    assert!(format!("{bus_host:?}").contains("BusHost"));
    let request = tinyeventqueues_bus::ClassifySalienceRequest::new(
        InboundEvent::new("s", "1", "c", Sender::new("u"), EventKind::Message, 0),
        30,
    );
    let error = bus_host.classify_salience(request).await.unwrap_err();
    assert!(error.to_string().contains("ClassifySalience"));
}
