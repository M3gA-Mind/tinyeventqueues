//! What the ingress queue stores.

use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::InboundEvent;

/// One ingress message: the event, its raw payload inside it, and when it was
/// accepted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IngressEnvelope {
    pub(crate) event: InboundEvent,
    pub(crate) ingested_at_ms: u64,
}

impl IngressEnvelope {
    pub(crate) fn new(event: InboundEvent, ingested_at_ms: u64) -> Self {
        Self {
            event,
            ingested_at_ms,
        }
    }
}
