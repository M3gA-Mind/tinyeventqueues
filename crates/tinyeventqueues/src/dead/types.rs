//! What the dead-letter queue stores.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyeventqueues_bus::EventRef;

/// A message parked in the dead-letter queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Parked {
    /// The queue it came from, and goes back to on redrive.
    pub(crate) origin: String,
    /// Why it died.
    pub(crate) reason: String,
    /// The deliveries it got at its origin.
    pub(crate) attempt: u32,
    /// Its group at the origin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) group: Option<String>,
    /// The turn it was, if it was one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) turn_id: Option<String>,
    /// The events it carries.
    pub(crate) events: Vec<EventRef>,
    /// The original message body.
    pub(crate) payload: Value,
}
