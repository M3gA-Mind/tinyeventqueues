//! Configuration types.

use serde::{Deserialize, Serialize};

/// The whole pipeline configuration (`pipeline.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PipelineConfig {
    /// The JSON Schema the file declares, for editors. Ignored by the module.
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// The file format version. Only `1` exists.
    pub version: u32,
    /// The ingress stage.
    pub ingress: IngressConfig,
    /// Where dead-lettered work is parked.
    pub dead_letter: DeadLetterConfig,
    /// The attention lanes.
    pub lanes: LanesConfig,
    /// Per-source policies, first match wins. A source no entry matches gets
    /// [`SourcePolicy::default`].
    #[serde(default)]
    pub sources: Vec<SourcePolicy>,
    /// The deterministic gate.
    #[serde(default)]
    pub gate: GateConfig,
    /// The debounce buffer.
    #[serde(default)]
    pub coalesce: CoalesceConfig,
}

/// The deterministic gate: who may reach the agent, and how often.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct GateConfig {
    /// If not empty, only these senders pass. An entry is a sender id, or
    /// `<source>/<sender id>` to name one source's sender.
    #[serde(default)]
    pub allow_senders: Vec<String>,
    /// These senders never pass; same entry forms as `allow_senders`. The
    /// deny list wins over the allow list.
    #[serde(default)]
    pub deny_senders: Vec<String>,
    /// Direct messages may reach the agent.
    #[serde(default = "enabled")]
    pub direct_messages: bool,
    /// Group conversations may reach the agent.
    #[serde(default = "enabled")]
    pub groups: bool,
    /// The bot-loop guard.
    #[serde(default)]
    pub bot_loop: BotLoopConfig,
    /// Token buckets; an event over any of them spills to the digest.
    #[serde(default)]
    pub rate_limits: RateLimits,
    /// The per-conversation buffer of unaddressed chatter.
    #[serde(default)]
    pub ambient: AmbientConfig,
}

fn enabled() -> bool {
    true
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            allow_senders: Vec::new(),
            deny_senders: Vec::new(),
            direct_messages: true,
            groups: true,
            bot_loop: BotLoopConfig::default(),
            rate_limits: RateLimits::default(),
            ambient: AmbientConfig::default(),
        }
    }
}

/// The bot-loop guard: a bot that sends more than `max_events` events into
/// one conversation within `window_ms` is looping with the agent, and the
/// excess is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct BotLoopConfig {
    /// Events allowed per window.
    #[serde(default = "default_bot_events")]
    pub max_events: u32,
    /// The window, in milliseconds.
    #[serde(default = "default_bot_window_ms")]
    pub window_ms: u64,
}

fn default_bot_events() -> u32 {
    20
}

fn default_bot_window_ms() -> u64 {
    60_000
}

impl Default for BotLoopConfig {
    fn default() -> Self {
        Self {
            max_events: default_bot_events(),
            window_ms: default_bot_window_ms(),
        }
    }
}

/// Token buckets by scope. A missing bucket is unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RateLimits {
    /// One bucket per sender (per source).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_sender: Option<RateLimit>,
    /// One bucket per conversation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_conversation: Option<RateLimit>,
    /// One bucket per source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_source: Option<RateLimit>,
}

/// A token bucket: `capacity` events at once, refilled at
/// `refill_per_minute`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RateLimit {
    /// The burst size.
    pub capacity: u32,
    /// Tokens added per minute.
    pub refill_per_minute: u32,
}

impl RateLimit {
    /// A bucket of `capacity` refilled at `refill_per_minute`.
    #[must_use]
    pub fn new(capacity: u32, refill_per_minute: u32) -> Self {
        Self {
            capacity,
            refill_per_minute,
        }
    }
}

/// The ambient buffer of unaddressed group chatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AmbientConfig {
    /// The most events kept per conversation; the oldest are evicted first.
    #[serde(default = "default_ambient_items")]
    pub max_items: u32,
}

fn default_ambient_items() -> u32 {
    50
}

impl Default for AmbientConfig {
    fn default() -> Self {
        Self {
            max_items: default_ambient_items(),
        }
    }
}

/// The debounce buffer keyed by conversation, thread and sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CoalesceConfig {
    /// Flush this long after a key's last event, in milliseconds.
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    /// Flush this long after a key's first event at the latest.
    #[serde(default = "default_max_wait_ms")]
    pub max_wait_ms: u64,
    /// Text prefixes that mark a control command, which flushes at once.
    #[serde(default = "default_control_prefixes")]
    pub control_prefixes: Vec<String>,
}

fn default_debounce_ms() -> u64 {
    1_500
}

fn default_max_wait_ms() -> u64 {
    10_000
}

fn default_control_prefixes() -> Vec<String> {
    vec!["/".to_owned()]
}

impl Default for CoalesceConfig {
    fn default() -> Self {
        Self {
            debounce_ms: default_debounce_ms(),
            max_wait_ms: default_max_wait_ms(),
            control_prefixes: default_control_prefixes(),
        }
    }
}

/// The ingress stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct IngressConfig {
    /// The manifest queue raw events are persisted to.
    pub queue: String,
    /// How long a completed event keeps rejecting redeliveries, in
    /// milliseconds. Used when the queue backend cannot dedupe and the module
    /// keeps its own tombstones.
    #[serde(default = "default_tombstone_ttl_ms")]
    pub tombstone_ttl_ms: u64,
    /// How many ingress messages one pass takes.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

/// Thirty days.
fn default_tombstone_ttl_ms() -> u64 {
    30 * 24 * 60 * 60 * 1000
}

fn default_batch_size() -> u32 {
    64
}

/// Where dead-lettered work is parked for `ListDead` and `Redrive`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DeadLetterConfig {
    /// The manifest queue dead work is parked in.
    pub queue: String,
}

/// One attention lane per field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct LanesConfig {
    /// Direct messages, mentions and web chat.
    pub interactive: LaneConfig,
    /// Work the agent should act on, not urgently.
    pub actionable: LaneConfig,
    /// Low-salience work.
    pub background: LaneConfig,
    /// Overflow and backlog, summarized into one turn.
    pub digest: LaneConfig,
}

impl LanesConfig {
    /// The configuration of `lane`.
    #[must_use]
    pub fn get(&self, lane: Lane) -> &LaneConfig {
        match lane {
            Lane::Interactive => &self.interactive,
            Lane::Actionable => &self.actionable,
            Lane::Background => &self.background,
            Lane::Digest => &self.digest,
        }
    }
}

/// One attention lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct LaneConfig {
    /// The manifest queue the lane's turns wait in.
    pub queue: String,
    /// How many turns of this lane may be in flight at once, across
    /// conversations. A conversation never has more than one.
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
}

fn default_concurrency() -> u32 {
    4
}

/// An attention lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lane {
    /// Direct messages, mentions and web chat.
    Interactive,
    /// Work the agent should act on, not urgently.
    Actionable,
    /// Low-salience work.
    Background,
    /// Overflow and backlog, summarized into one turn.
    Digest,
}

impl Lane {
    /// Every lane, highest priority first.
    pub const ALL: [Self; 4] = [
        Self::Interactive,
        Self::Actionable,
        Self::Background,
        Self::Digest,
    ];

    /// The lane's wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Actionable => "actionable",
            Self::Background => "background",
            Self::Digest => "digest",
        }
    }
}

/// When the platform may be acknowledged for an event of a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AckPolicy {
    /// As soon as `Ingest` returns: the event is durable.
    #[default]
    AfterIngest,
    /// Once the turn carrying it was handed to the host.
    AfterDispatch,
    /// Once the host reports the agent's turn finished (the host acks the
    /// platform when its `Deliver` handler completes the turn).
    AfterTurn,
}

/// The policy for the sources one pattern matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SourcePolicy {
    /// The sources this entry covers: an exact source (`channel:telegram`), a
    /// prefix ending in `*` (`channel:*`), or `*` for every source.
    #[serde(rename = "match")]
    pub pattern: String,
    /// When the platform may be acknowledged.
    #[serde(default)]
    pub ack: AckPolicy,
}

impl Default for SourcePolicy {
    fn default() -> Self {
        Self {
            pattern: "*".to_owned(),
            ack: AckPolicy::default(),
        }
    }
}

impl SourcePolicy {
    /// A policy for `pattern` with every setting at its default.
    #[must_use]
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            ..Self::default()
        }
    }

    /// Whether this entry covers `source`.
    #[must_use]
    pub fn matches(&self, source: &str) -> bool {
        match self.pattern.strip_suffix('*') {
            Some(prefix) => source.starts_with(prefix),
            None => self.pattern == source,
        }
    }
}

impl PipelineConfig {
    /// The policy for `source`: the first entry that matches it, or the
    /// default policy.
    #[must_use]
    pub fn source_policy(&self, source: &str) -> SourcePolicy {
        self.sources
            .iter()
            .find(|policy| policy.matches(source))
            .cloned()
            .unwrap_or_default()
    }
}
