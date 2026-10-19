//! Pending turns: what waits in a lane queue for dispatch.
//!
//! A [`PendingTurn`] is the unit the lanes hold and the host eventually
//! receives as a [`Turn`](tinyeventqueues_bus::Turn): the events of one
//! conversation thread (or a digest), the events absorbed into them (edits
//! that amended a pending message), and the ambient context collected since
//! the conversation's last turn. Its id is derived from its first event, so
//! re-enqueueing the same turn after a crash is a duplicate, not a second
//! turn.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{EventRef, InboundEvent, Lane, Reason};
use tinystoragedrivers_core::Key;
use tinystoragedrivers_core::queue::{NewMessage, QueueStore};

use crate::Result;
use crate::config::QueueRef;
use crate::store::Tombstones;

/// A turn waiting in a lane queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingTurn {
    pub(crate) turn_id: String,
    pub(crate) lane: Lane,
    /// Why the turn is on its lane: an ordinary [`Reason::Turn`], or a
    /// spill such as [`Reason::RateLimited`].
    pub(crate) reason: Reason,
    pub(crate) conversation_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) thread_key: Option<String>,
    pub(crate) events: Vec<InboundEvent>,
    /// Events folded into `events` (edits) that share the turn's fate.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) absorbed: Vec<EventRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) context: Vec<InboundEvent>,
    pub(crate) created_ms: u64,
}

impl PendingTurn {
    /// A turn of `events` on `lane`, which must not be empty.
    pub(crate) fn new(
        lane: Lane,
        reason: Reason,
        events: Vec<InboundEvent>,
        absorbed: Vec<EventRef>,
        created_ms: u64,
    ) -> Result<Self> {
        let first = events.first();
        let conversation_key = first
            .map(|e| e.conversation_key.clone())
            .unwrap_or_default();
        let thread_key = first.and_then(|e| e.thread_key.clone());
        let (source, id) = first.map_or((String::new(), String::new()), |e| {
            (e.source.clone(), e.event_id.clone())
        });
        let turn_id = Key::of([
            "turn",
            lane.as_str(),
            conversation_key.as_str(),
            thread_key.as_deref().unwrap_or(""),
            source.as_str(),
            id.as_str(),
        ])?
        .as_str()
        .to_owned();
        Ok(Self {
            turn_id,
            lane,
            reason,
            conversation_key,
            thread_key,
            events,
            absorbed,
            context: Vec::new(),
            created_ms,
        })
    }
}

/// Writes pending turns to lane queues.
///
/// On a backend with dedupe the turn id is the message's dedupe key; without
/// it, a tombstone per turn id in the document store (written after the
/// enqueue) recognizes a re-enqueue. On a backend with groups the
/// conversation is the message's group; without them the dispatcher
/// serializes conversations itself.
#[derive(Debug, Clone)]
pub(crate) struct Lanes {
    queues: Arc<dyn QueueStore>,
    tombstones: Tombstones,
}

/// The tombstone namespace of turn ids.
const TURN_TOMBSTONES: &str = "turn";

impl Lanes {
    pub(crate) fn new(queues: Arc<dyn QueueStore>, tombstones: Tombstones) -> Self {
        Self { queues, tombstones }
    }

    /// Enqueue `turn` on `queue`. Returns `false` when the turn was already
    /// queued. Without native dedupe, the turn's tombstone lives `ttl_ms`.
    pub(crate) async fn enqueue(
        &self,
        queue: &QueueRef,
        turn: &PendingTurn,
        now: u64,
        ttl_ms: u64,
    ) -> Result<bool> {
        let features = self.queues.features();
        if !features.dedupe
            && self
                .tombstones
                .live(TURN_TOMBSTONES, &turn.turn_id, now)
                .await?
        {
            return Ok(false);
        }
        let mut message = NewMessage::new(serde_json::to_value(turn)?);
        if features.dedupe {
            message = message.dedupe_key(&turn.turn_id);
        }
        if features.groups && !turn.conversation_key.is_empty() {
            message = message.group(&turn.conversation_key);
        }
        let stored = self.queues.enqueue(&queue.name, &[message]).await?;
        if !features.dedupe {
            self.tombstones
                .put(TURN_TOMBSTONES, &turn.turn_id, now.saturating_add(ttl_ms))
                .await?;
        }
        Ok(stored.first().is_some_and(|stored| !stored.duplicate))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
