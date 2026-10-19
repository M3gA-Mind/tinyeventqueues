//! The stored debounce buffer.

use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{EventKind, EventRef, InboundEvent};

use super::{Added, amends};

/// An edit folded into the event it amended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Absorbed {
    /// The edit.
    pub(crate) event: EventRef,
    /// The id of the event it amended.
    pub(crate) target: String,
}

/// One debounce buffer: one sender's pending events in one thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Buffer {
    pub(crate) thread_group: String,
    pub(crate) events: Vec<InboundEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) absorbed: Vec<Absorbed>,
    pub(crate) first_ms: u64,
    pub(crate) last_ms: u64,
    pub(crate) due_ms: u64,
    /// Media or a control command arrived: flush at once.
    #[serde(default)]
    pub(crate) urgent: bool,
    /// When the buffer's first event arrived at ingress; orders the buffers
    /// of one thread.
    pub(crate) arrival_ms: u64,
}

impl Buffer {
    pub(super) fn empty(group: &str, arrival_ms: u64, now: u64) -> Self {
        Self {
            thread_group: group.to_owned(),
            events: Vec::new(),
            absorbed: Vec::new(),
            first_ms: now,
            last_ms: now,
            due_ms: u64::MAX,
            urgent: false,
            arrival_ms,
        }
    }

    /// Whether the buffer already holds `event`.
    pub(super) fn holds(&self, event: &InboundEvent) -> bool {
        let reference = event.reference();
        self.events.iter().any(|e| e.reference() == reference)
            || self.absorbed.iter().any(|a| a.event == reference)
    }

    /// Whether `event` targets an event this buffer holds.
    pub(super) fn targets(&self, event: &InboundEvent) -> bool {
        event.target_event_id.as_deref().is_some_and(|target| {
            self.events
                .iter()
                .any(|e| e.event_id == target && e.source == event.source)
        })
    }

    /// Add `event`: append it, or apply it to the buffered event it amends.
    pub(super) fn add(&mut self, event: &InboundEvent) -> Added {
        let target = event.target_event_id.as_deref();
        let position = target.and_then(|target| {
            self.events
                .iter()
                .position(|e| e.event_id == target && e.source == event.source)
        });
        match (amends(event), position) {
            (true, Some(at)) if event.kind == EventKind::Delete => {
                let removed = self.events.remove(at);
                let mut retracted = vec![removed.reference(), event.reference()];
                let target = removed.event_id;
                self.absorbed.retain(|a| {
                    let gone = a.target == target;
                    if gone {
                        retracted.push(a.event.clone());
                    }
                    !gone
                });
                Added::Retracted(retracted)
            }
            (true, Some(at)) => {
                let amended = &mut self.events[at];
                amended.text.clone_from(&event.text);
                if !event.media.is_empty() {
                    amended.media.clone_from(&event.media);
                }
                self.absorbed.push(Absorbed {
                    event: event.reference(),
                    target: amended.event_id.clone(),
                });
                Added::Buffered
            }
            _ => {
                self.events.push(event.clone());
                Added::Buffered
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty() && self.absorbed.is_empty()
    }

    /// Every event the buffer decides the fate of.
    pub(crate) fn refs(&self) -> Vec<EventRef> {
        self.events
            .iter()
            .map(InboundEvent::reference)
            .chain(self.absorbed.iter().map(|a| a.event.clone()))
            .collect()
    }
}
