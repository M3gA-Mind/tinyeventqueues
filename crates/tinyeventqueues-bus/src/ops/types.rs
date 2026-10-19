//! Member payload types.

use serde::{Deserialize, Serialize};

use crate::{Disposition, EventRef, InboundEvent, OutcomeCounts, PipelineConfig, Reason};

/// The argument to `Ingest`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct IngestRequest {
    /// The events, in arrival order.
    pub events: Vec<InboundEvent>,
}

impl IngestRequest {
    /// A request ingesting `events`.
    #[must_use]
    pub fn new(events: Vec<InboundEvent>) -> Self {
        Self { events }
    }
}

/// What `Ingest` did with one event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct IngestResult {
    /// The event.
    pub event: EventRef,
    /// [`Accepted`](Disposition::Accepted) (durably queued),
    /// [`Duplicate`](Disposition::Duplicate) (seen before, nothing stored) or
    /// [`Dropped`](Disposition::Dropped) (rejected, with a reason).
    pub disposition: Disposition,
    /// Why, for a duplicate or a drop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason>,
    /// The caller may acknowledge the platform now. True for every
    /// duplicate and drop, and for accepted events of `after_ingest` sources;
    /// the others are acknowledged when their turn is dispatched or done.
    pub ack_platform: bool,
}

/// The reply from `Ingest`: one result per event, in request order.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct IngestResponse {
    /// One result per event.
    pub results: Vec<IngestResult>,
}

impl IngestResponse {
    /// A reply carrying `results`.
    #[must_use]
    pub fn new(results: Vec<IngestResult>) -> Self {
        Self { results }
    }
}

impl IngestResult {
    /// A result for `event`.
    #[must_use]
    pub fn new(
        event: EventRef,
        disposition: Disposition,
        reason: Option<Reason>,
        ack_platform: bool,
    ) -> Self {
        Self {
            event,
            disposition,
            reason,
            ack_platform,
        }
    }
}

/// The argument to `Configure`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ConfigureRequest {
    /// The configuration to run with from now on.
    pub config: PipelineConfig,
}

impl ConfigureRequest {
    /// A request applying `config`.
    #[must_use]
    pub fn new(config: PipelineConfig) -> Self {
        Self { config }
    }
}

/// The reply from `Configure`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ConfigureResponse {
    /// The configuration validated and is now in effect.
    pub applied: bool,
}

impl ConfigureResponse {
    /// A reply saying whether the configuration was applied.
    #[must_use]
    pub fn new(applied: bool) -> Self {
        Self { applied }
    }
}

/// The argument to `Stats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StatsRequest {}

impl StatsRequest {
    /// A stats request.
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

/// How many messages one queue holds, by state.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueueDepth {
    /// The manifest queue name.
    pub queue: String,
    /// Deliverable now.
    pub ready: u64,
    /// Waiting for a delay, a tick or a backoff.
    pub delayed: u64,
    /// In flight.
    pub leased: u64,
    /// Dead-lettered.
    pub dead: u64,
}

impl QueueDepth {
    /// Depths for `queue`.
    #[must_use]
    pub fn new(queue: impl Into<String>, ready: u64, delayed: u64, leased: u64, dead: u64) -> Self {
        Self {
            queue: queue.into(),
            ready,
            delayed,
            leased,
            dead,
        }
    }
}

/// The reply from `Stats`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StatsResponse {
    /// Every queue the pipeline uses.
    pub queues: Vec<QueueDepth>,
    /// Terminal outcomes so far.
    pub outcomes: OutcomeCounts,
}

impl StatsResponse {
    /// A reply with `queues` and `outcomes`.
    #[must_use]
    pub fn new(queues: Vec<QueueDepth>, outcomes: OutcomeCounts) -> Self {
        Self { queues, outcomes }
    }
}

/// The argument to `ListDead`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ListDeadRequest {
    /// The most items to return.
    pub limit: u32,
}

impl ListDeadRequest {
    /// A request for up to `limit` items.
    #[must_use]
    pub fn new(limit: u32) -> Self {
        Self { limit }
    }
}

/// One piece of dead-lettered work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DeadItem {
    /// The id to pass to `Redrive`.
    pub id: String,
    /// The manifest queue the work died in.
    pub origin: String,
    /// Why it died.
    pub reason: String,
    /// Deliveries it got.
    pub attempt: u32,
    /// The events it carries.
    pub events: Vec<EventRef>,
}

impl DeadItem {
    /// A dead item.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        origin: impl Into<String>,
        reason: impl Into<String>,
        attempt: u32,
        events: Vec<EventRef>,
    ) -> Self {
        Self {
            id: id.into(),
            origin: origin.into(),
            reason: reason.into(),
            attempt,
            events,
        }
    }
}

/// The reply from `ListDead`, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ListDeadResponse {
    /// The dead items.
    pub dead: Vec<DeadItem>,
}

impl ListDeadResponse {
    /// A reply listing `dead`.
    #[must_use]
    pub fn new(dead: Vec<DeadItem>) -> Self {
        Self { dead }
    }
}

/// The argument to `Redrive`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RedriveRequest {
    /// A [`DeadItem::id`].
    pub id: String,
}

impl RedriveRequest {
    /// A request redriving `id`.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

/// The reply from `Redrive`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RedriveResponse {
    /// A dead item had that id and is back in the pipeline.
    pub redriven: bool,
}

impl RedriveResponse {
    /// A reply saying whether the item was redriven.
    #[must_use]
    pub fn new(redriven: bool) -> Self {
        Self { redriven }
    }
}

/// The argument to `Drain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DrainRequest {}

impl DrainRequest {
    /// A drain request.
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

/// What one pipeline pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DrainReport {
    /// Ingress messages taken off the ingress queue.
    #[serde(default)]
    pub ingested: u64,
    /// Coalesced groups flushed towards the lanes.
    #[serde(default)]
    pub flushed: u64,
    /// Turns released by a cadence tick.
    #[serde(default)]
    pub scheduled: u64,
    /// Turns handed to the host.
    #[serde(default)]
    pub dispatched: u64,
    /// Work moved to the dead-letter queue.
    #[serde(default)]
    pub dead: u64,
    /// Expired tombstones removed.
    #[serde(default)]
    pub pruned: u64,
}

/// The reply from `Drain`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DrainResponse {
    /// What the pass did.
    pub report: DrainReport,
}

impl DrainResponse {
    /// A reply carrying `report`.
    #[must_use]
    pub fn new(report: DrainReport) -> Self {
        Self { report }
    }
}
