//! Host callback types.

use serde::{Deserialize, Serialize};

use crate::{EventRef, InboundEvent, Lane};

/// One unit of work for the agent: the events of one conversation thread
/// (or a digest of many), delivered together.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Turn {
    /// A stable id: redelivering the same turn (after a crash or a retry)
    /// carries the same id, so the host can make delivery idempotent.
    pub turn_id: String,
    /// The lane the turn was scheduled on.
    pub lane: Lane,
    /// The conversation, or empty for a digest spanning several.
    pub conversation_key: String,
    /// The thread, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_key: Option<String>,
    /// The events, oldest first.
    pub events: Vec<InboundEvent>,
    /// Ambient context: unaddressed chatter from the conversation since its
    /// last turn, oldest first. Informational; not the agent's to answer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<InboundEvent>,
    /// Events whose platform acknowledgement waits on this turn
    /// (`after_dispatch` and `after_turn` sources).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub platform_acks: Vec<EventRef>,
    /// Which delivery of this turn this is, starting at 1.
    pub attempt: u32,
}

impl Turn {
    /// A turn for `conversation_key` on `lane`.
    #[must_use]
    pub fn new(
        turn_id: impl Into<String>,
        lane: Lane,
        conversation_key: impl Into<String>,
        events: Vec<InboundEvent>,
    ) -> Self {
        Self {
            turn_id: turn_id.into(),
            lane,
            conversation_key: conversation_key.into(),
            thread_key: None,
            events,
            context: Vec::new(),
            platform_acks: Vec::new(),
            attempt: 1,
        }
    }

    /// Whether this is a digest turn.
    #[must_use]
    pub fn is_digest(&self) -> bool {
        self.lane == Lane::Digest
    }
}

/// The argument to the host's `Deliver`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DeliverRequest {
    /// The turn.
    pub turn: Turn,
}

impl DeliverRequest {
    /// A request delivering `turn`.
    #[must_use]
    pub fn new(turn: Turn) -> Self {
        Self { turn }
    }
}

/// What the host did with a turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliverStatus {
    /// The host took the turn. Its events are delivered.
    Accepted,
    /// Try again later, after `after_ms` if given, else the module's backoff.
    Retry {
        /// How long to wait, in milliseconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_ms: Option<u64>,
    },
    /// The host refuses the turn for good; it is dead-lettered now.
    Rejected {
        /// Why, for the dead letter.
        detail: String,
    },
}

/// The reply from the host's `Deliver`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DeliverResponse {
    /// What the host did.
    #[serde(flatten)]
    pub status: DeliverStatus,
}

impl DeliverResponse {
    /// A reply carrying `status`.
    #[must_use]
    pub fn new(status: DeliverStatus) -> Self {
        Self { status }
    }
}

/// A host's verdict on a borderline event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SalienceClass {
    /// Not worth the agent's attention.
    Noise,
    /// Worth knowing, not worth acting on now.
    Fyi,
    /// The agent should act on it.
    Actionable,
    /// The agent should act on it now.
    Urgent,
}

/// The argument to the host's `ClassifySalience`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ClassifySalienceRequest {
    /// The event to classify.
    pub event: InboundEvent,
    /// The rule score the module computed, `0..=100`.
    pub score: u32,
}

impl ClassifySalienceRequest {
    /// A request classifying `event`, which scored `score`.
    #[must_use]
    pub fn new(event: InboundEvent, score: u32) -> Self {
        Self { event, score }
    }
}

/// The reply from the host's `ClassifySalience`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ClassifySalienceResponse {
    /// The verdict.
    pub class: SalienceClass,
}

impl ClassifySalienceResponse {
    /// A reply carrying `class`.
    #[must_use]
    pub fn new(class: SalienceClass) -> Self {
        Self { class }
    }
}
