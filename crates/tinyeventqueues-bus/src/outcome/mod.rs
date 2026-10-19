//! What happened to an event, and why.
//!
//! Every ingested event ends in exactly one *terminal* [`Disposition`]:
//! [`Delivered`](Disposition::Delivered), [`Context`](Disposition::Context),
//! [`Digest`](Disposition::Digest), [`Dead`](Disposition::Dead) or
//! [`Dropped`](Disposition::Dropped). Along the way the pipeline may also
//! report non-terminal dispositions ([`Accepted`](Disposition::Accepted),
//! [`Duplicate`](Disposition::Duplicate), [`Deferred`](Disposition::Deferred)).
//! Every drop, deferral and dead letter carries a [`Reason`]: nothing is lost
//! silently.

mod types;

pub use types::{Disposition, Outcome, OutcomeCounts, Reason};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
