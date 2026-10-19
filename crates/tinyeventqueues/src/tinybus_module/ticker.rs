//! The module-owned pass timer.
//!
//! A pass ([`Pipeline::drain`]) moves ingested events through the pipeline and
//! delivers due turns. The loaded module runs passes on its own so a host only
//! has to `Ingest`: after each pass the timer sleeps until
//! [`Pipeline::next_wake`] (a debounce flush, a cadence tick, ready work),
//! bounded by the configured interval, and an `Ingest` wakes it at once.
//! `Drain` still runs a pass on demand, for tests and manual operation.
//!
//! A failed pass is logged and retried after the longest interval; it never
//! stops the timer.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tinyeventqueues_bus::DrainReport;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::settings::TimerSettings;
use crate::Pipeline;

/// Wakes the timer early, after new work arrived.
#[derive(Debug, Clone, Default)]
pub(crate) struct Wake(Arc<Notify>);

impl Wake {
    /// Ask for a pass now.
    pub(crate) fn now(&self) {
        self.0.notify_one();
    }
}

/// The running timer of one module instance. Dropping it stops the timer.
#[derive(Debug)]
pub(crate) struct Ticker {
    task: JoinHandle<()>,
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The timer of one module instance, shared by its setups.
///
/// A module reinitialization runs `setup` again over a new pipeline; the
/// earlier setup's timer must not keep running passes over a pipeline nobody
/// serves any more, so each setup replaces the timer in the instance's slot.
/// The slot is captured by the instance's setup factory, not a process-wide
/// static: separate instances (and tests) never stop each other's timers.
#[derive(Debug, Clone, Default)]
pub(crate) struct Slot(Arc<Mutex<Option<Ticker>>>);

impl Slot {
    fn replace(&self, next: Option<Ticker>) {
        let mut current = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Dropping the previous ticker aborts its task.
        *current = next;
    }

    /// Whether a timer is running in this slot.
    #[cfg(test)]
    pub(crate) fn running(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|ticker| !ticker.task.is_finished())
    }
}

/// Whether a pass moved anything; when it did, more may be ready at once.
fn progressed(report: &DrainReport) -> bool {
    report.ingested + report.flushed + report.scheduled + report.dispatched + report.dead > 0
}

/// How long to sleep before the next pass.
async fn delay(pipeline: &Pipeline, settings: TimerSettings, progressed: bool) -> Duration {
    let min = settings.min_interval_ms;
    let max = settings.max_interval_ms;
    let wait = if progressed {
        min
    } else {
        match pipeline.next_wake().await {
            Ok(Some(at)) => at.saturating_sub(pipeline.now()).clamp(min, max),
            Ok(None) => max,
            Err(error) => {
                log::warn!("[tinyeventqueues] could not read the next wake-up: {error}");
                max
            }
        }
    };
    Duration::from_millis(wait)
}

/// One pass, logged. Returns whether it made progress.
async fn pass(pipeline: &Pipeline) -> bool {
    match pipeline.drain().await {
        Ok(report) => {
            if progressed(&report) {
                log::debug!(
                    "[tinyeventqueues] pass ingested={} flushed={} scheduled={} dispatched={} dead={}",
                    report.ingested,
                    report.flushed,
                    report.scheduled,
                    report.dispatched,
                    report.dead
                );
                true
            } else {
                false
            }
        }
        Err(error) => {
            log::warn!("[tinyeventqueues] pass failed, retrying later: {error}");
            false
        }
    }
}

/// Start the timer over `pipeline` in `slot`, replacing the timer of the
/// instance's previous setup. Returns the handle that wakes it early.
pub(crate) fn start(slot: &Slot, pipeline: Pipeline, settings: TimerSettings) -> Wake {
    let wake = Wake::default();
    if !settings.enabled {
        log::info!("[tinyeventqueues] pass timer disabled; passes run on Drain only");
        slot.replace(None);
        return wake;
    }
    let signal = Arc::clone(&wake.0);
    let task = tokio::spawn(async move {
        loop {
            let moved = pass(&pipeline).await;
            let sleep = delay(&pipeline, settings, moved).await;
            tokio::select! {
                () = tokio::time::sleep(sleep) => {}
                () = signal.notified() => {}
            }
        }
    });
    slot.replace(Some(Ticker { task }));
    wake
}

#[cfg(test)]
#[path = "ticker_tests.rs"]
mod tests;
