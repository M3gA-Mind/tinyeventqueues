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
