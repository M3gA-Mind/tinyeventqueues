//! One pass of the pipeline's stages, in order.

use tinyeventqueues_bus::{Disposition, EventKind, InboundEvent, Lane, Outcome, Reason};
use tinystoragedrivers_core::queue::Retry;

use super::{Pipeline, Running};
use crate::Result;
use crate::coalesce::{Added, amends};
use crate::gate::Verdict;
use crate::ingress::IngressEnvelope;
use crate::turn::PendingTurn;

/// The most ingress batches one pass takes, so a pass is bounded.
const MAX_INGRESS_BATCHES: u32 = 64;

/// The most threads one pass flushes.
const MAX_FLUSH_THREADS: usize = 1_024;

/// The lane a flushed batch goes to before salience scoring exists: work
/// aimed at the agent is interactive, the rest actionable.
fn provisional_lane(events: &[InboundEvent]) -> Lane {
    if events
        .iter()
        .any(|e| e.is_direct || e.addressed_to_agent || e.kind == EventKind::Mention)
    {
        Lane::Interactive
    } else {
        Lane::Actionable
    }
}

impl Pipeline {
    /// Take events off the ingress queue, gate them, and buffer the ones that
    /// pass. Returns how many ingress messages were taken.
    pub(super) async fn take_ingress(&self, running: &Running, now: u64) -> Result<u64> {
        let queue = &running.queues.ingress;
        let mut taken = 0;
        for _ in 0..MAX_INGRESS_BATCHES {
            let batch = self
                .inner
                .queues
                .receive(
                    &queue.name,
                    running.config.ingress.batch_size,
                    queue.visibility_ms,
                    &self.inner.consumer,
                )
                .await?;
            if batch.is_empty() {
                break;
            }
            for delivery in batch {
                taken += 1;
                if let Ok(envelope) = serde_json::from_value::<IngressEnvelope>(delivery.payload) {
                    self.admit(envelope, running, now).await?;
                } else {
                    let reason = Retry::DeadLetter("unreadable ingress envelope".to_owned());
                    self.inner
                        .queues
                        .nack(&queue.name, &delivery.receipt, reason)
                        .await?;
                    continue;
                }
                self.inner
                    .queues
                    .ack(&queue.name, &delivery.receipt)
                    .await?;
            }
        }
        Ok(taken)
    }

    /// Gate one event and act on the verdict.
    async fn admit(&self, envelope: IngressEnvelope, running: &Running, now: u64) -> Result<()> {
        let config = &running.config;
        let ttl = config.ingress.tombstone_ttl_ms;
        let event = envelope.event;
        let verdict = self.inner.gate.check(&event, &config.gate, now).await?;
        match verdict {
            Verdict::Drop(reason) => {
                self.end(&event, Disposition::Dropped, reason, now, ttl)
                    .await
            }
            Verdict::Context => {
                if amends(&event)
                    && let Some(added) = self.inner.coalescer.try_amend(&event).await?
                {
                    return self.settle_added(added, now, ttl).await;
                }
                self.inner
                    .ambient
                    .keep(&event, &config.gate.ambient)
                    .await?;
                self.end(&event, Disposition::Context, Reason::Unaddressed, now, ttl)
                    .await
            }
            Verdict::Spill(reason) => {
                let turn =
                    PendingTurn::new(Lane::Digest, reason, vec![event.clone()], vec![], now)?;
                self.inner
                    .lanes
                    .enqueue(running.queues.lane(Lane::Digest), &turn, now, ttl)
                    .await?;
                self.inner.ledger.notify(&Outcome::new(
                    event.reference(),
                    Disposition::Deferred,
                    reason,
                    now,
                ));
                Ok(())
            }
            Verdict::Pass => {
                let added = self
                    .inner
                    .coalescer
                    .add(&event, envelope.ingested_at_ms, &config.coalesce, now)
                    .await?;
                self.settle_added(added, now, ttl).await
            }
        }
    }

    async fn settle_added(&self, added: Added, now: u64, ttl: u64) -> Result<()> {
        if let Added::Retracted(events) = added {
            for event in events {
                let outcome =
                    Outcome::new(event, Disposition::Dropped, Reason::RetractedBySender, now);
                self.inner.ledger.record(outcome, ttl).await?;
            }
        }
        Ok(())
    }

    async fn end(
        &self,
        event: &InboundEvent,
        disposition: Disposition,
        reason: Reason,
        now: u64,
        ttl: u64,
    ) -> Result<()> {
        let outcome = Outcome::new(event.reference(), disposition, reason, now);
        self.inner.ledger.record(outcome, ttl).await?;
        Ok(())
    }

    /// Flush every due buffer into a pending turn on its lane, with the
    /// conversation's ambient context. Returns how many buffers flushed.
    pub(super) async fn flush_due(&self, running: &Running, now: u64) -> Result<u64> {
        let mut flushed = 0;
        for due in self.inner.coalescer.due(now, MAX_FLUSH_THREADS).await? {
            let buffer = &due.found.doc;
            if let Some(first) = buffer.events.first() {
                let (source, conversation) = (first.source.clone(), first.conversation_key.clone());
                let lane = provisional_lane(&buffer.events);
                let absorbed = buffer.absorbed.iter().map(|a| a.event.clone()).collect();
                let mut turn =
                    PendingTurn::new(lane, Reason::Turn, buffer.events.clone(), absorbed, now)?;
                turn.context = self.inner.ambient.peek(&source, &conversation).await?;
                self.inner
                    .lanes
                    .enqueue(
                        running.queues.lane(lane),
                        &turn,
                        now,
                        running.config.ingress.tombstone_ttl_ms,
                    )
                    .await?;
                let taken: Vec<_> = turn.context.iter().map(InboundEvent::reference).collect();
                self.inner
                    .ambient
                    .release(&source, &conversation, &taken)
                    .await?;
                flushed += 1;
            }
            self.inner.coalescer.remove(&due).await?;
        }
        Ok(flushed)
    }
}

#[cfg(test)]
#[path = "pass_tests.rs"]
mod tests;
