//! The pipeline's document collections and the outcome ledger.
//!
//! Queues hold work; documents hold everything else the pipeline must
//! remember across a restart. This module owns the collection names and the
//! two records every stage shares:
//!
//! - the **outcome ledger** (`teq.outcomes`): one record per event that reached
//!   a terminal disposition, written with a create-only precondition so an
//!   event can end only once, even when a stage replays after a crash. Records
//!   expire after the tombstone TTL, and while they live they also reject late
//!   redeliveries of completed events.
//! - **ingress tombstones** (`teq.tombstones`): for queue backends that cannot
//!   dedupe (`QueueFeatures::dedupe == false`), one record per accepted
//!   `source:event_id`, so a redelivery is recognized without the queue's help.

mod ledger;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, Filter, IndexSpec, Key, Precondition,
};

use crate::Result;

pub(crate) use ledger::Ledger;

/// Terminal outcomes, one per event.
pub(crate) const OUTCOMES: &str = "teq.outcomes";
/// Ingress tombstones, for queue backends without dedupe.
pub(crate) const TOMBSTONES: &str = "teq.tombstones";
/// Dead letters already copied back to their origin by a redrive.
pub(crate) const REDRIVEN: &str = "teq.redriven";
/// Token buckets and bot-loop windows.
pub(crate) const BUCKETS: &str = "teq.buckets";
/// Ambient context per conversation.
pub(crate) const AMBIENT: &str = "teq.ambient";
/// The debounce buffer.
pub(crate) const COALESCE: &str = "teq.coalesce";
/// Habituation fingerprints per source.
pub(crate) const HABITUATION: &str = "teq.habituation";
/// The digest accumulator.
pub(crate) const DIGEST: &str = "teq.digest";
/// Turns staged for a cadence tick.
pub(crate) const STAGED: &str = "teq.staged";
/// Each cadence's schedule.
pub(crate) const CADENCE: &str = "teq.cadence";
/// Per-conversation turn order, on queue backends without groups.
pub(crate) const CONVERSATIONS: &str = "teq.conversations";
/// Queued, unsettled turns per thread.
pub(crate) const THREADS: &str = "teq.threads";

/// The collections the pipeline uses, with their indexes.
pub(crate) fn collection_specs() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(OUTCOMES)
            .index(IndexSpec::new("by_disposition", ["disposition"]))
            .index(IndexSpec::new("by_expiry", ["expires_ms"])),
        CollectionSpec::new(TOMBSTONES).index(IndexSpec::new("by_expiry", ["expires_ms"])),
        CollectionSpec::new(REDRIVEN),
        CollectionSpec::new(BUCKETS),
        CollectionSpec::new(AMBIENT),
        CollectionSpec::new(HABITUATION),
        CollectionSpec::new(DIGEST),
        CollectionSpec::new(STAGED)
            .index(IndexSpec::new("by_due", ["due_ms"]))
            .index(IndexSpec::new("by_cadence", ["cadence_key"])),
        CollectionSpec::new(CADENCE),
        CollectionSpec::new(CONVERSATIONS),
        CollectionSpec::new(THREADS),
        CollectionSpec::new(COALESCE)
            .index(IndexSpec::new("by_due", ["due_ms"]))
            .index(IndexSpec::new("by_thread", ["thread_group"])),
    ]
}

/// Declare every collection on `docs`. Idempotent.
pub(crate) async fn ensure(docs: &dyn DocumentStore) -> Result<()> {
    for spec in collection_specs() {
        docs.ensure_collection(&spec).await?;
    }
    Ok(())
}

/// The document id of an event: its source and id, escaped and joined.
pub(crate) fn event_id(source: &str, event_id: &str) -> Result<String> {
    Ok(Key::of([source, event_id])?.as_str().to_owned())
}

/// An ingress tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Tombstone {
    expires_ms: u64,
}

/// Ingress tombstones for queue backends that cannot dedupe.
#[derive(Debug, Clone)]
pub(crate) struct Tombstones {
    docs: Arc<dyn DocumentStore>,
}

impl Tombstones {
    pub(crate) fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self { docs }
    }

    /// Whether `source:event_id` has a live tombstone at `now`.
    pub(crate) async fn live(&self, source: &str, event: &str, now: u64) -> Result<bool> {
        let id = event_id(source, event)?;
        let Some(found) = self.docs.get(TOMBSTONES, &id).await? else {
            return Ok(false);
        };
        let tombstone: Tombstone = serde_json::from_value(found.doc)?;
        Ok(tombstone.expires_ms > now)
    }

    /// Record `source:event_id` as accepted until `expires_ms`.
    pub(crate) async fn put(&self, source: &str, event: &str, expires_ms: u64) -> Result<()> {
        let id = event_id(source, event)?;
        let doc = serde_json::to_value(Tombstone { expires_ms })?;
        self.docs
            .put(TOMBSTONES, &id, doc, Precondition::None)
            .await?;
        Ok(())
    }

    /// Remove tombstones expired at `now`.
    pub(crate) async fn prune(&self, now: u64) -> Result<u64> {
        Ok(self
            .docs
            .delete_where(TOMBSTONES, &Filter::lte("expires_ms", now))
            .await?)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
