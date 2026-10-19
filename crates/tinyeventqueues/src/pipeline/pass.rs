//! One pass of the pipeline's stages, in order.

use tinyeventqueues_bus::{
    ClassifySalienceRequest, Disposition, EventRef, InboundEvent, Lane, Outcome, Reason,
    SalienceClass,
};
use tinystoragedrivers_core::queue::Retry;

use super::{Pipeline, Running};
use crate::Result;
use crate::coalesce::{Added, amends};
use crate::gate::Verdict;
use crate::ingress::IngressEnvelope;
use crate::salience::{lane_for, lane_for_class, rule_score};
use crate::turn::PendingTurn;

/// The most ingress batches one pass takes, so a pass is bounded.
const MAX_INGRESS_BATCHES: u32 = 64;

/// The most threads one pass flushes.
const MAX_FLUSH_THREADS: usize = 1_024;

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
                self.spill(std::slice::from_ref(&event), &[], reason, running, now)
                    .await
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

    /// Flush every due buffer: score it, pick its lane, and enqueue it as a
    /// pending turn with the conversation's ambient context (or spill it to
    /// the digest). Returns how many buffers flushed.
    pub(super) async fn flush_due(&self, running: &Running, now: u64) -> Result<u64> {
        let mut flushed = 0;
        for due in self.inner.coalescer.due(now, MAX_FLUSH_THREADS).await? {
            let buffer = &due.found.doc;
            if !buffer.events.is_empty() {
                let absorbed: Vec<EventRef> =
                    buffer.absorbed.iter().map(|a| a.event.clone()).collect();
                self.route(&buffer.events, &absorbed, running, now).await?;
                flushed += 1;
            }
            self.inner.coalescer.remove(&due).await?;
        }
        Ok(flushed)
    }

    /// Score a flushed batch and send it to its lane, or spill it.
    async fn route(
        &self,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        running: &Running,
        now: u64,
    ) -> Result<()> {
        let config = &running.config;
        let mut best: Option<(u32, &InboundEvent)> = None;
        let mut habituated = false;
        for event in events {
            let weight = config.source_policy(&event.source).weight;
            let rule = rule_score(event, &config.salience, weight, now);
            let scored = self
                .inner
                .habituation
                .apply(event, rule, &config.salience.habituation, now)
                .await?;
            habituated |= scored.habituated;
            if best.is_none_or(|(score, _)| scored.score > score) {
                best = Some((scored.score, event));
            }
        }
        let Some((score, top)) = best else {
            return Ok(());
        };
        let lane = match self.classify(top, score, running, now).await? {
            Some(class) => lane_for_class(class),
            None => lane_for(score, &config.salience.thresholds),
        };
        let Some(lane) = lane else {
            let reason = if habituated {
                Reason::Habituated
            } else {
                Reason::Noise
            };
            return self.spill(events, absorbed, reason, running, now).await;
        };
        let queue = running.queues.lane(lane);
        let stats = self.inner.queues.stats(&queue.name).await?;
        if lane != Lane::Digest && stats.ready + stats.delayed >= config.lanes.get(lane).max_backlog
        {
            return self
                .spill(events, absorbed, Reason::LaneOverflow, running, now)
                .await;
        }
        let (source, conversation) = (top.source.clone(), top.conversation_key.clone());
        let mut turn =
            PendingTurn::new(lane, Reason::Turn, events.to_vec(), absorbed.to_vec(), now)?;
        turn.context = self.inner.ambient.peek(&source, &conversation).await?;
        self.inner.lanes.enqueue(queue, &turn).await?;
        let taken: Vec<_> = turn.context.iter().map(InboundEvent::reference).collect();
        self.inner
            .ambient
            .release(&source, &conversation, &taken)
            .await
    }

    /// Ask the host to classify a borderline score, within budget.
    async fn classify(
        &self,
        event: &InboundEvent,
        score: u32,
        running: &Running,
        now: u64,
    ) -> Result<Option<SalienceClass>> {
        let (Some(host), Some(band)) = (&self.inner.host, running.config.salience.borderline)
        else {
            return Ok(None);
        };
        if !(band.low..band.high).contains(&score)
            || !self
                .inner
                .gate
                .classify_budget(band.budget_per_hour, now)
                .await?
        {
            return Ok(None);
        }
        let request = ClassifySalienceRequest::new(event.clone(), score);
        Ok(host
            .classify_salience(request)
            .await
            .ok()
            .map(|reply| reply.class))
    }

    /// Fold events into the digest, reporting each as deferred with
    /// `reason`, and release a digest turn if one is due.
    async fn spill(
        &self,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        reason: Reason,
        running: &Running,
        now: u64,
    ) -> Result<()> {
        self.inner.digest.add(events, absorbed, now).await?;
        for event in events
            .iter()
            .map(InboundEvent::reference)
            .chain(absorbed.iter().cloned())
        {
            self.inner
                .ledger
                .notify(&Outcome::new(event, Disposition::Deferred, reason, now));
        }
        self.release_digest(running, now).await?;
        Ok(())
    }

    /// Enqueue every digest turn due at `now`. Returns how many.
    pub(super) async fn release_digest(&self, running: &Running, now: u64) -> Result<u64> {
        let mut released = 0;
        while let Some(turn) = self.inner.digest.due(&running.config.digest, now).await? {
            self.inner
                .lanes
                .enqueue(running.queues.lane(Lane::Digest), &turn)
                .await?;
            self.inner.digest.release(&turn, now).await?;
            released += 1;
        }
        Ok(released)
    }
}

#[cfg(test)]
#[path = "pass_tests.rs"]
mod tests;
