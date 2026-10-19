//! Outcome types.

use serde::{Deserialize, Serialize};

use crate::EventRef;

/// Where an event is in its life, as reported to observers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Disposition {
    /// Durably queued at ingress.
    Accepted,
    /// A redelivery of an event already accepted; nothing new was stored.
    Duplicate,
    /// Held back for later (a scheduled cadence, a budget, a retry).
    Deferred,
    /// Terminal: delivered to the agent in a turn.
    Delivered,
    /// Terminal: kept as ambient context, surfaced with the conversation's
    /// next real turn.
    Context,
    /// Terminal: delivered folded into a digest turn.
    Digest,
    /// Terminal: dead-lettered after delivery kept failing.
    Dead,
    /// Terminal: dropped on purpose, with a reason.
    Dropped,
}

impl Disposition {
    /// Whether this disposition ends the event's life.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Delivered | Self::Context | Self::Digest | Self::Dead | Self::Dropped
        )
    }

    /// Every terminal disposition.
    pub const TERMINAL: [Self; 5] = [
        Self::Delivered,
        Self::Context,
        Self::Digest,
        Self::Dead,
        Self::Dropped,
    ];
}

/// Why an event got its disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Reason {
    /// Durably queued at ingress.
    Ingested,
    /// The envelope failed validation (an empty id, source or conversation).
    InvalidEnvelope,
    /// The `source:event_id` pair was seen before.
    DuplicateRedelivery,
    /// The sender is on the deny list.
    SenderDenied,
    /// An allow list is set and the sender is not on it.
    SenderNotAllowed,
    /// Direct messages are disabled by policy.
    DirectMessagesDisabled,
    /// Group messages are disabled by policy.
    GroupsDisabled,
    /// The event is the agent's own message echoed back.
    SelfEcho,
    /// A bot pair exceeded the loop guard.
    BotLoop,
    /// A sender, conversation or source exceeded its rate limit.
    RateLimited,
    /// Group chatter not addressed to the agent.
    Unaddressed,
    /// The sender deleted the message before it was delivered.
    RetractedBySender,
    /// Near-duplicate content the source keeps repeating.
    Habituated,
    /// The salience score or host classification marked it as noise.
    Noise,
    /// The lane was full, so it spilled to the digest.
    LaneOverflow,
    /// The source's cadence holds it until the next tick.
    ScheduledCadence,
    /// The cadence's ingestion budget for this tick was spent.
    OverBudget,
    /// The host asked for a retry.
    RetryRequested,
    /// The host rejected the turn.
    HostRejected,
    /// Delivery failed on every allowed attempt.
    MaxAttempts,
    /// Delivered as part of an ordinary turn.
    Turn,
}

/// One observable outcome for one event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Outcome {
    /// The event.
    pub event: EventRef,
    /// What happened.
    pub disposition: Disposition,
    /// Why.
    pub reason: Reason,
    /// The turn the event went out in, for delivered, digest and dead events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    /// When, Unix epoch milliseconds.
    pub at_ms: u64,
}

impl Outcome {
    /// An outcome for `event`.
    #[must_use]
    pub fn new(event: EventRef, disposition: Disposition, reason: Reason, at_ms: u64) -> Self {
        Self {
            event,
            disposition,
            reason,
            turn_id: None,
            at_ms,
        }
    }

    /// Set [`turn_id`](Self::turn_id).
    #[must_use]
    pub fn in_turn(mut self, turn_id: impl Into<String>) -> Self {
        self.turn_id = Some(turn_id.into());
        self
    }
}

/// How many events reached each terminal disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OutcomeCounts {
    /// Delivered in a turn.
    pub delivered: u64,
    /// Kept as ambient context.
    pub context: u64,
    /// Delivered in a digest.
    pub digest: u64,
    /// Dead-lettered.
    pub dead: u64,
    /// Dropped with a reason.
    pub dropped: u64,
}

impl OutcomeCounts {
    /// The count for one terminal disposition; `0` for a non-terminal one.
    #[must_use]
    pub fn get(&self, disposition: Disposition) -> u64 {
        match disposition {
            Disposition::Delivered => self.delivered,
            Disposition::Context => self.context,
            Disposition::Digest => self.digest,
            Disposition::Dead => self.dead,
            Disposition::Dropped => self.dropped,
            _ => 0,
        }
    }

    /// Set the count for one terminal disposition; a non-terminal one is
    /// ignored.
    pub fn set(&mut self, disposition: Disposition, count: u64) {
        match disposition {
            Disposition::Delivered => self.delivered = count,
            Disposition::Context => self.context = count,
            Disposition::Digest => self.digest = count,
            Disposition::Dead => self.dead = count,
            Disposition::Dropped => self.dropped = count,
            _ => {}
        }
    }

    /// Every event that reached a terminal disposition.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.delivered + self.context + self.digest + self.dead + self.dropped
    }
}
