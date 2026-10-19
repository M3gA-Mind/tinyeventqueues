//! `TinyBus` module entrypoint and bus-facing interface.
//!
//! The adapter serves [`Pipeline`] at [`names::OBJECT_PATH`]. The names and
//! payload types come from [`tinyeventqueues_bus`], so a host spells them from
//! the contract crate instead of repeating string literals.
//!
//! Storage: until the module is wired to the host's storage through the
//! `tinystoragedrivers` storage bus (a later change), the loaded module runs
//! its pipeline over an in-process `MemoryStorage` with the shipped
//! `queues.json` and `pipeline.json`. That is enough to exercise the contract
//! end to end, but it is not durable across a host restart.

use std::sync::Arc;

use tinybus::{Connection, Result as TinyBusResult};
use tinyeventqueues_bus::{
    ConfigureRequest, ConfigureResponse, DrainRequest, DrainResponse, IngestRequest,
    IngestResponse, ListDeadRequest, ListDeadResponse, RedriveRequest, RedriveResponse,
    StatsRequest, StatsResponse, names,
};
use tinystoragedrivers_core::queue::DocumentQueue;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

use crate::Pipeline;

struct EventQueuesService {
    pipeline: Pipeline,
}

fn failed(error: &crate::Error) -> tinybus::Error {
    tinybus::Error::failed(error.to_string())
}

#[tinybus::interface(name = "ai.tinyhumans.tinyeventqueues.EventQueues")]
impl EventQueuesService {
    async fn ingest(&self, request: IngestRequest) -> TinyBusResult<IngestResponse> {
        self.pipeline
            .ingest(request.events)
            .await
            .map_err(|error| failed(&error))
    }

    async fn configure(&self, request: ConfigureRequest) -> TinyBusResult<ConfigureResponse> {
        std::future::ready(self.pipeline.configure(request.config))
            .await
            .map(|()| ConfigureResponse::new(true))
            .map_err(|error| failed(&error))
    }

    async fn stats(&self, _request: StatsRequest) -> TinyBusResult<StatsResponse> {
        self.pipeline.stats().await.map_err(|error| failed(&error))
    }

    async fn list_dead(&self, request: ListDeadRequest) -> TinyBusResult<ListDeadResponse> {
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        self.pipeline
            .list_dead(limit)
            .await
            .map(ListDeadResponse::new)
            .map_err(|error| failed(&error))
    }

    async fn redrive(&self, request: RedriveRequest) -> TinyBusResult<RedriveResponse> {
        self.pipeline
            .redrive(&request.id)
            .await
            .map(RedriveResponse::new)
            .map_err(|error| failed(&error))
    }

    async fn drain(&self, _request: DrainRequest) -> TinyBusResult<DrainResponse> {
        self.pipeline
            .drain()
            .await
            .map(DrainResponse::new)
            .map_err(|error| failed(&error))
    }
}

/// A pipeline over a fresh in-process memory store with the shipped files.
async fn memory_pipeline() -> crate::Result<Pipeline> {
    let storage = MemoryStorage::new().for_scope(&Scope::local())?;
    let queues = Arc::new(DocumentQueue::new(storage.documents()));
    Pipeline::builder(queues, Arc::clone(storage.documents()))
        .build()
        .await
}

async fn setup(connection: Connection) -> TinyBusResult<()> {
    let pipeline = memory_pipeline().await.map_err(|error| failed(&error))?;
    connection
        .serve_at(
            names::OBJECT_PATH.try_into()?,
            EventQueuesService { pipeline },
        )
        .await?;
    connection.request_name(names::INTERFACE).await?;
    Ok(())
}

tinybus_module::module_export! {
    setup = setup,
    worker_threads = 1,
    provides = ["ai.tinyhumans.tinyeventqueues.EventQueues"],
    methods = ["Ingest", "Configure", "Stats", "ListDead", "Redrive", "Drain"],
    signals = [],
    requires = [],
    optional = [],
    lazy = false,
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
