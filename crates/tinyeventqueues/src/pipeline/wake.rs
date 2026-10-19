//! When the next pass has work: what a host- or module-owned timer sleeps on.
//!
//! [`Pipeline::drain`] runs one pass at the time it is called. Something has
//! to call it again: when a queue holds ready work, when a coalesce buffer's
//! debounce runs out, and when a cadence tick releases staged turns.
//! [`Pipeline::next_wake`] answers that from the stored state, so a timer can
//! sleep exactly until then instead of polling on a fixed beat.

use super::Pipeline;
use crate::Result;

impl Pipeline {
    /// The earliest time, in clock milliseconds, at which a pass has work.
    ///
    /// * `Some(now)` when the ingress or a lane queue holds ready work (the
    ///   dead-letter queue does not count: nothing consumes it on its own).
    /// * Otherwise the earliest coalesce flush or staged cadence release,
    ///   which may lie in the past when a pass is overdue.
    /// * `None` when nothing waits on time. Work delayed inside a queue (a
    ///   retry's back-off) is not visible here; a timer bounds its sleep to
    ///   pick it up.
    ///
    /// # Errors
    ///
    /// A storage failure reading queue stats or the stored schedules.
    pub async fn next_wake(&self) -> Result<Option<u64>> {
        let running = self.running();
        let now = self.now();
        let consumed = std::iter::once(&running.queues.ingress).chain(running.queues.lanes.iter());
        for queue in consumed {
            if self.inner.queues.stats(&queue.name).await?.ready > 0 {
                return Ok(Some(now));
            }
        }
        let flush = self.inner.coalescer.earliest_due().await?;
        let release = self.inner.scheduler.earliest_due().await?;
        Ok(match (flush, release) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (one, other) => one.or(other),
        })
    }
}

#[cfg(test)]
#[path = "wake_tests.rs"]
mod tests;
