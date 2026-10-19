//! The module's storage: the host's, over the storage bus, or a loud fallback.
//!
//! The module owns no storage backend, file or path. It connects to the host's
//! `ai.tinyhumans.tinystorage.Storage` objects and takes the one scope the
//! host bound it to: documents for the pipeline's `teq.*` state and the queue
//! port for its queues, so queued events survive a restart and live wherever
//! the host keeps its data (`SQLite`, `MongoDB`, a broker).
//!
//! A host that serves no storage (an older host, a bare test broker) leaves
//! the module on in-process memory. That still runs the contract end to end,
//! but nothing queued survives the process, so it is logged as an error.

use std::sync::Arc;

use tinybus::Connection;
use tinystoragedrivers_core::queue::QueueStore;
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, ScopedStorage, StorageBackend};

use crate::Result;

/// Where the pipeline keeps its work and state.
#[derive(Debug, Clone)]
pub(crate) struct Backing {
    pub(crate) queues: Arc<dyn QueueStore>,
    pub(crate) docs: Arc<dyn DocumentStore>,
    /// Whether the backing is the host's storage (durable) or memory.
    pub(crate) durable: bool,
}

impl Backing {
    fn of(storage: &ScopedStorage, durable: bool) -> Self {
        Self {
            queues: Arc::clone(storage.queues()),
            docs: Arc::clone(storage.documents()),
            durable,
        }
    }

    /// Fresh in-process memory storage: not durable.
    ///
    /// # Errors
    ///
    /// Never in practice: the local scope always opens on memory.
    pub(crate) fn memory() -> Result<Self> {
        let storage = MemoryStorage::new().for_scope(&Scope::local())?;
        Ok(Self::of(&storage, false))
    }
}

/// The host's storage for this module, or memory when the host serves none.
///
/// # Errors
///
/// Only when the memory fallback cannot open, which does not happen.
pub(crate) async fn open(connection: Connection) -> Result<Backing> {
    match tinystoragedrivers_busproxy::connect_storage(connection).await {
        Ok(bus) => match bus.for_scope(&tinystoragedrivers_busproxy::module_scope_label()) {
            Ok(scoped) => {
                log::info!("[tinyeventqueues] pipeline on host storage (durable)");
                return Ok(Backing::of(&scoped, true));
            }
            Err(error) => log::error!(
                "[tinyeventqueues] host storage refused the module scope: {error}; \
                 running on IN-MEMORY storage, queued events are NOT durable"
            ),
        },
        Err(error) => log::error!(
            "[tinyeventqueues] host serves no storage: {error}; \
             running on IN-MEMORY storage, queued events are NOT durable"
        ),
    }
    Backing::memory()
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
