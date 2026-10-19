//! External event management and queuing: durably queue noisy inbound events
//! and filter them the way a person filters noise, before they reach an agent.
//!
//! Events arrive from chat channels (web chat, Telegram, Discord, Slack),
//! connector triggers (X replies through Composio), webhooks and
//! notifications. The [`Pipeline`] moves each one through
//!
//! ```text
//! ingress → normalize → gate → coalesce → salience → attention lanes
//!         → cadence / ingestion budget → dispatch (host callback)
//! ```
//!
//! and every ingested event ends in exactly one terminal [`Disposition`]:
//! delivered, context, digest, dead, or dropped with a [`Reason`]. The full
//! contract is `docs/specs/event-pipeline.md`; this release implements the
//! ingress stage, tombstones, dead letters and configuration.
//!
//! # Storage
//!
//! The pipeline runs over the storage ports of `tinystoragedrivers`: an
//! `Arc<dyn QueueStore>` for work and an `Arc<dyn DocumentStore>` for state,
//! so it runs on memory, `SQLite`, file, `MongoDB` or a broker. Time comes from
//! an injectable [`Clock`], so tests drive it with a [`ManualClock`].
//!
//! # Configuration
//!
//! Two editable JSON files, both strict: `queues.json` declares every queue
//! (a `tinystoragedrivers` `QueueManifest`) and `pipeline.json` holds the
//! pipeline's tunables ([`PipelineConfig`]), referring to queues by name. See
//! [`config`].
//!
//! # Example
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! use std::sync::Arc;
//! use tinyeventqueues::{Disposition, EventKind, InboundEvent, Pipeline, Sender};
//! use tinystoragedrivers_core::queue::DocumentQueue;
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//!
//! let storage = MemoryStorage::new().for_scope(&Scope::local())?;
//! let queues = Arc::new(DocumentQueue::new(storage.documents()));
//! let pipeline = Pipeline::builder(queues, Arc::clone(storage.documents())).build().await?;
//!
//! let event = InboundEvent::new(
//!     "channel:telegram", "1001", "chat-42", Sender::new("user-7"), EventKind::Message, 0,
//! );
//! let first = pipeline.ingest(vec![event.clone()]).await?;
//! assert_eq!(first.results[0].disposition, Disposition::Accepted);
//! let again = pipeline.ingest(vec![event]).await?;
//! assert_eq!(again.results[0].disposition, Disposition::Duplicate);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # }).unwrap();
//! ```

pub mod clock;
pub mod config;
mod dead;
mod error;
pub mod ingress;
pub mod observe;
mod pipeline;
mod store;
mod tinybus_module;

#[cfg(test)]
mod test_support;

pub use clock::{Clock, ManualClock, system_clock};
pub use error::{Error, Result};
pub use observe::{CollectingSink, NoopSink, OutcomeSink};
pub use pipeline::{Pipeline, PipelineBuilder};

// The wire contract, re-exported by module and by item so every path through
// this crate resolves to the same definitions the contract crate publishes.
pub use tinyeventqueues_bus;
pub use tinyeventqueues_bus::{
    AckPolicy, CONTRACT_VERSION, ClassifySalienceRequest, ClassifySalienceResponse,
    ConfigureRequest, ConfigureResponse, DeadItem, DeadLetterConfig, DeliverRequest,
    DeliverResponse, DeliverStatus, Disposition, DrainReport, DrainRequest, DrainResponse,
    EventKind, EventRef, INTERFACE, InboundEvent, IngestRequest, IngestResponse, IngestResult,
    IngressConfig, Lane, LaneConfig, LanesConfig, ListDeadRequest, ListDeadResponse, METHODS,
    MediaRef, OBJECT_PATH, Outcome, OutcomeCounts, PayloadRef, PipelineConfig, QueueDepth, Reason,
    RedriveRequest, RedriveResponse, SalienceClass, Sender, SourcePolicy, StatsRequest,
    StatsResponse, Turn, is_compatible, names, version,
};
