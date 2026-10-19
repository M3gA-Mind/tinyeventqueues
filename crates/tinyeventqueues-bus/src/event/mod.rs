//! The inbound event envelope: what every source (chat channels, connector
//! triggers, webhooks, notifications) is normalized into before it is queued.
//!
//! The envelope carries routing keys (`source`, `conversation_key`,
//! `thread_key`), the sender, what kind of event it is, whether it is aimed at
//! the agent, the text and media, and the raw platform payload (inline or by
//! reference) so nothing the platform sent is lost by normalization.

mod types;

pub use types::{EventKind, EventRef, InboundEvent, MediaRef, PayloadRef, Sender};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
