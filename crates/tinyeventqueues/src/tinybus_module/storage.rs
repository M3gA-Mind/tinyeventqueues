//! The module's host-owned storage ports.

use std::sync::Arc;

use tinybus::Connection;
use tinystoragedrivers_api::{DocumentStore, QueueStore, Storage};
use tinystoragedrivers_types::StorageError;

use crate::{Error, Result};

/// The host-provided storage handles used by the pipeline.
#[derive(Debug, Clone)]
pub(crate) struct Backing {
    pub(crate) queues: Arc<dyn QueueStore>,
    pub(crate) docs: Arc<dyn DocumentStore>,
}

impl Backing {
    pub(crate) fn of(storage: &Storage) -> Self {
        Self {
            queues: Arc::clone(storage.queues()),
            docs: Arc::clone(storage.documents()),
        }
    }
}

/// Connect to the host's storage ports. Missing or unusable host storage is a
/// setup error; this module never acknowledges work against an ephemeral
/// fallback engine.
pub(crate) async fn open(connection: Connection, callback: Option<&str>) -> Result<Backing> {
    let storage = match callback {
        Some(destination) => {
            let destination = tinybus::BusName::new(destination).map_err(|error| {
                Error::from(tinystoragedrivers_types::StorageError::unavailable(
                    format!("invalid storage_callback {destination:?}: {error}"),
                ))
            })?;
            tinystoragedrivers_busproxy::connect_storage_at(connection, destination)
                .await
                .map_err(|error| {
                    Error::from(
                        StorageError::new(
                            error.kind(),
                            format!(
                                "storage_callback {destination} is unavailable: {}",
                                error.message()
                            ),
                        )
                        .with_source(error),
                    )
                })?
        }
        None => {
            return Err(Error::from(
                tinystoragedrivers_types::StorageError::unavailable(
                    "host callback destination is required: set storage_callback in the module configuration",
                ),
            ));
        }
    };
    Ok(Backing::of(&storage))
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
