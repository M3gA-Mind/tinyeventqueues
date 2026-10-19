//! What the module stores, as a schema a host composes.
//!
//! The pipeline keeps its state in `teq.*` document collections, one scope per
//! tenant: the module runs for one tenant at a time, so every collection is at
//! [`ScopeClass::Module`]. A host composes [`storage_schema`] into its schema
//! registry and derives the module's storage grant from it, so the grant never
//! spells the collections (or their indexes) out a second time.
//!
//! The queues themselves are not collections of this owner. They are declared
//! in `queues.json` (a `tinystoragedrivers` `QueueManifest`, shipped as
//! [`SHIPPED_QUEUES_JSON`]) and served by the host's queue port, whose own
//! bookkeeping lives under the reserved `tsd.` owner.
//!
//! [`collection_specs`] is what the module ensures at startup; the host only
//! admits an `EnsureCollection` whose spec equals the declared one, so both
//! sides read the specs from here.

use tinystoragedrivers_core::schema::{Owner, Schema, ScopeClass};
use tinystoragedrivers_core::{CollectionSpec, IndexSpec};

/// The schema owner, and the prefix of every collection name.
pub const OWNER: &str = "teq";

/// The version every collection is declared at. Bump a collection's version
/// (and add a step) when its stored shape changes.
pub const COLLECTION_VERSION: u32 = 1;

/// Terminal outcomes, one per event.
pub const OUTCOMES: &str = "teq.outcomes";
/// Ingress tombstones, for queue backends without dedupe.
pub const TOMBSTONES: &str = "teq.tombstones";
/// Dead letters already copied back to their origin by a redrive.
pub const REDRIVEN: &str = "teq.redriven";
/// Token buckets and bot-loop windows.
pub const BUCKETS: &str = "teq.buckets";
/// Ambient context per conversation.
pub const AMBIENT: &str = "teq.ambient";
/// The debounce buffer.
pub const COALESCE: &str = "teq.coalesce";
/// Habituation fingerprints per source.
pub const HABITUATION: &str = "teq.habituation";
/// The digest accumulator.
pub const DIGEST: &str = "teq.digest";
/// Turns staged for a cadence tick.
pub const STAGED: &str = "teq.staged";
/// Each cadence's schedule.
pub const CADENCE: &str = "teq.cadence";
/// Per-conversation turn order, on queue backends without groups.
pub const CONVERSATIONS: &str = "teq.conversations";
/// Queued, unsettled turns per thread.
pub const THREADS: &str = "teq.threads";

/// The shipped queue manifest (`queues.json`). A host that serves the module's
/// queues declares exactly these unless its operator overrides the file.
pub const SHIPPED_QUEUES_JSON: &str = include_str!("../../../../queues.json");

/// The shipped pipeline configuration (`pipeline.json`).
pub const SHIPPED_PIPELINE_JSON: &str = include_str!("../../../../pipeline.json");

/// The collections the pipeline uses, with their indexes.
#[must_use]
pub fn collection_specs() -> Vec<CollectionSpec> {
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

/// Everything the module stores, for the host's `Registry::compose`.
///
/// # Panics
///
/// Never in practice: the owner name is a constant that satisfies the owner
/// grammar, which the tests pin.
#[must_use]
pub fn storage_schema() -> Schema {
    #[allow(clippy::expect_used)]
    let owner = Owner::new(OWNER).expect("`teq` is a valid schema owner");
    collection_specs()
        .into_iter()
        .fold(Schema::new(owner), |schema, spec| {
            schema.collection_with(COLLECTION_VERSION, spec, ScopeClass::Module)
        })
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
