//! The payloads of the host callbacks: what the module sends the host and
//! what it expects back.
//!
//! The module never talks to an agent itself. When a turn is due it calls the
//! host's `Deliver`; when a score is borderline it may ask the host's
//! `ClassifySalience`. Both are served by the host at
//! [`crate::names::host::OBJECT_PATH`].

mod types;

pub use types::{
    ClassifySalienceRequest, ClassifySalienceResponse, DeliverRequest, DeliverResponse,
    DeliverStatus, SalienceClass, Turn,
};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
