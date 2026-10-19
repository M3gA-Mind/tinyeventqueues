//! Envelope types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One inbound event, normalized from whatever the platform sent.
///
/// `source` and `event_id` together identify the event: the module dedupes
/// redeliveries on `source:event_id`, so a platform that redelivers after a
/// restart sends the same pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct InboundEvent {
    /// The platform's id for the event, unique within `source`.
    pub event_id: String,
    /// Where the event came from, as `<family>:<name>`: `channel:telegram`,
    /// `composio:twitter`, `webhook:<tunnel>`, `web:chat`.
    pub source: String,
    /// The conversation the event belongs to (a chat, a DM, a repository).
    /// Events of one conversation are delivered one turn at a time.
    pub conversation_key: String,
    /// The thread within the conversation, if the platform has threads.
    /// Events of different threads are never merged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_key: Option<String>,
    /// Who sent it.
    pub sender: Sender,
    /// What kind of event it is.
    pub kind: EventKind,
    /// The event is aimed at the agent: a mention, a reply to the agent, a
    /// command, a web-chat message.
    #[serde(default)]
    pub addressed_to_agent: bool,
    /// The conversation is a direct (one-to-one) conversation with the agent.
    #[serde(default)]
    pub is_direct: bool,
    /// The message text, if any.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    /// Media attached to the event.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub media: Vec<MediaRef>,
    /// The event this one refers to: the message an edit, delete, reaction or
    /// reply targets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_event_id: Option<String>,
    /// When the platform received the event, Unix epoch milliseconds.
    pub received_at_ms: u64,
    /// The raw platform payload, inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<Value>,
    /// The raw platform payload, stored elsewhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_ref: Option<PayloadRef>,
}

impl InboundEvent {
    /// An event of `kind` from `sender` with every optional field empty.
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        event_id: impl Into<String>,
        conversation_key: impl Into<String>,
        sender: Sender,
        kind: EventKind,
        received_at_ms: u64,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            source: source.into(),
            conversation_key: conversation_key.into(),
            thread_key: None,
            sender,
            kind,
            addressed_to_agent: false,
            is_direct: false,
            text: String::new(),
            media: Vec::new(),
            target_event_id: None,
            received_at_ms,
            raw: None,
            raw_ref: None,
        }
    }

    /// Set [`thread_key`](Self::thread_key).
    #[must_use]
    pub fn thread(mut self, thread_key: impl Into<String>) -> Self {
        self.thread_key = Some(thread_key.into());
        self
    }

    /// Set [`text`](Self::text).
    #[must_use]
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = text.into();
        self
    }

    /// Set [`addressed_to_agent`](Self::addressed_to_agent).
    #[must_use]
    pub fn addressed(mut self, addressed: bool) -> Self {
        self.addressed_to_agent = addressed;
        self
    }

    /// Set [`is_direct`](Self::is_direct).
    #[must_use]
    pub fn direct(mut self, direct: bool) -> Self {
        self.is_direct = direct;
        self
    }

    /// Add one [`media`](Self::media) reference.
    #[must_use]
    pub fn with_media(mut self, media: MediaRef) -> Self {
        self.media.push(media);
        self
    }

    /// Set [`target_event_id`](Self::target_event_id).
    #[must_use]
    pub fn target(mut self, target_event_id: impl Into<String>) -> Self {
        self.target_event_id = Some(target_event_id.into());
        self
    }

    /// Set [`raw`](Self::raw).
    #[must_use]
    pub fn raw(mut self, raw: Value) -> Self {
        self.raw = Some(raw);
        self
    }

    /// The key redeliveries are deduplicated on: `source:event_id`.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!("{}:{}", self.source, self.event_id)
    }

    /// A short reference to this event.
    #[must_use]
    pub fn reference(&self) -> EventRef {
        EventRef {
            source: self.source.clone(),
            event_id: self.event_id.clone(),
        }
    }
}

/// Who sent an event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Sender {
    /// The platform's id for the sender.
    pub id: String,
    /// A display name, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
    /// The sender is the agent itself (an echo of its own message).
    #[serde(default)]
    pub is_self: bool,
    /// The sender is a bot.
    #[serde(default)]
    pub is_bot: bool,
}

impl Sender {
    /// A human sender with `id`.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            display: None,
            is_self: false,
            is_bot: false,
        }
    }

    /// Set [`display`](Self::display).
    #[must_use]
    pub fn display(mut self, display: impl Into<String>) -> Self {
        self.display = Some(display.into());
        self
    }

    /// Mark the sender as the agent itself.
    #[must_use]
    pub fn as_self(mut self) -> Self {
        self.is_self = true;
        self
    }

    /// Mark the sender as a bot.
    #[must_use]
    pub fn as_bot(mut self) -> Self {
        self.is_bot = true;
        self
    }
}

/// What kind of event an [`InboundEvent`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EventKind {
    /// A new message.
    Message,
    /// A reply to an earlier message.
    Reply,
    /// A message that mentions the agent.
    Mention,
    /// A reaction to an earlier message.
    Reaction,
    /// An edit of an earlier message; `target_event_id` names it.
    Edit,
    /// A deletion of an earlier message; `target_event_id` names it.
    Delete,
    /// A connector trigger (a new tweet reply, a repository event).
    Trigger,
    /// A notification from a service.
    Notification,
}

/// A media attachment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MediaRef {
    /// What it is: `image`, `audio`, `video`, `file`.
    pub kind: String,
    /// Where it can be fetched.
    pub uri: String,
    /// Its MIME type, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime: Option<String>,
}

impl MediaRef {
    /// Media of `kind` at `uri`.
    #[must_use]
    pub fn new(kind: impl Into<String>, uri: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            uri: uri.into(),
            mime: None,
        }
    }
}

/// A raw payload stored outside the envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PayloadRef {
    /// Where the payload is stored (a blob key or URI).
    pub uri: String,
    /// Its content type, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Its size in bytes, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

impl PayloadRef {
    /// A payload at `uri`.
    #[must_use]
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            content_type: None,
            size: None,
        }
    }
}

/// A short reference to one event: its source and id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
pub struct EventRef {
    /// The event's source.
    pub source: String,
    /// The event's id within its source.
    pub event_id: String,
}

impl EventRef {
    /// A reference to `event_id` from `source`.
    #[must_use]
    pub fn new(source: impl Into<String>, event_id: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            event_id: event_id.into(),
        }
    }
}
