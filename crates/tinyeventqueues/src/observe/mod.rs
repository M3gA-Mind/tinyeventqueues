//! Observing outcomes as they happen.
//!
//! The pipeline reports every [`Outcome`] — accepted, duplicate, deferred and
//! each terminal disposition — to an [`OutcomeSink`] the host installs. The
//! durable record of terminal outcomes is the outcome ledger in the document
//! store; the sink is for live metrics, logs and tests.

use std::sync::Mutex;

use tinyeventqueues_bus::Outcome;

/// Receives every outcome the pipeline reports.
pub trait OutcomeSink: Send + Sync + std::fmt::Debug {
    /// Called once per reported outcome. Must not block.
    fn observe(&self, outcome: &Outcome);
}

/// A sink that ignores everything.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopSink;

impl OutcomeSink for NoopSink {
    fn observe(&self, _outcome: &Outcome) {}
}

/// A sink that keeps every outcome in memory, for tests and diagnostics.
#[derive(Debug, Default)]
pub struct CollectingSink {
    seen: Mutex<Vec<Outcome>>,
}

impl CollectingSink {
    /// An empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every outcome observed so far, in order.
    #[must_use]
    pub fn outcomes(&self) -> Vec<Outcome> {
        self.seen.lock().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |seen| seen.clone(),
        )
    }
}

impl OutcomeSink for CollectingSink {
    fn observe(&self, outcome: &Outcome) {
        let mut seen = self
            .seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        seen.push(outcome.clone());
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
