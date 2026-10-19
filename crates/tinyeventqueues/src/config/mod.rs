//! Loading and validating the two editable files the pipeline runs from.
//!
//! - `queues.json`, a [`QueueManifest`]: every queue the pipeline uses, with
//!   its attempts, dedupe window and lease length. Nothing in Rust names a
//!   queue or its spec.
//! - `pipeline.json`, a [`PipelineConfig`]: the pipeline's own tunables, which
//!   refer to manifest queues by name.
//!
//! Both are strict. Unknown fields are rejected, and every error names the
//! offending field by its JSON path (`lanes.digest.concurrency`,
//! `sources[2].match`), so whoever edits the file — a person or an agent — is
//! told exactly what to fix. The repository root ships both files, with JSON
//! Schemas in `schemas/`; [`shipped_manifest`] and
//! [`shipped_pipeline_config`] return the copies compiled into this crate.

mod types;

use std::path::Path;

use tinyeventqueues_bus::{Lane, PipelineConfig};
use tinystoragedrivers_core::queue::QueueManifest;

use crate::{Error, Result};

pub use types::{QueueRef, ResolvedQueues};

/// The `queues.json` shipped at the repository root.
pub const SHIPPED_QUEUES_JSON: &str = include_str!("../../../../queues.json");

/// The `pipeline.json` shipped at the repository root.
pub const SHIPPED_PIPELINE_JSON: &str = include_str!("../../../../pipeline.json");

/// The pipeline configuration format version this crate reads.
pub const PIPELINE_CONFIG_VERSION: u32 = 1;

/// The most ingress messages one pass may take.
const MAX_BATCH_SIZE: u32 = 10_000;

/// The most turns of one lane that may be in flight at once.
const MAX_CONCURRENCY: u32 = 1_024;

/// Parse the shipped `queues.json`.
///
/// # Errors
///
/// [`Error::Manifest`] if the shipped file does not validate.
pub fn shipped_manifest() -> Result<QueueManifest> {
    parse_manifest(SHIPPED_QUEUES_JSON)
}

/// Parse and check the shipped `pipeline.json`.
///
/// # Errors
///
/// [`Error::Config`] if the shipped file does not validate.
pub fn shipped_pipeline_config() -> Result<PipelineConfig> {
    parse_pipeline_config(SHIPPED_PIPELINE_JSON)
}

/// Parse a queue manifest.
///
/// # Errors
///
/// [`Error::Manifest`] naming the offending field or queue.
pub fn parse_manifest(text: &str) -> Result<QueueManifest> {
    QueueManifest::from_json(text).map_err(Error::Manifest)
}

/// Read and parse the queue manifest at `path`.
///
/// # Errors
///
/// [`Error::Manifest`] when the file is missing, unreadable or invalid.
pub fn load_manifest(path: impl AsRef<Path>) -> Result<QueueManifest> {
    QueueManifest::load(path).map_err(Error::Manifest)
}

/// Parse a pipeline configuration strictly and check its values.
///
/// This checks everything that does not need the queue manifest; the queue
/// references are checked by [`resolve_queues`].
///
/// # Errors
///
/// [`Error::Config`] with the JSON path of the first offending field: an
/// unknown field, a missing one, a wrong type, or a value out of range.
pub fn parse_pipeline_config(text: &str) -> Result<PipelineConfig> {
    let deserializer = &mut serde_json::Deserializer::from_str(text);
    let config: PipelineConfig =
        serde_path_to_error::deserialize(deserializer).map_err(|error| {
            let path = error.path().to_string();
            let field = if matches!(path.as_str(), "." | "?") {
                "(root)".to_owned()
            } else {
                path
            };
            Error::config(field, error.into_inner().to_string())
        })?;
    check_values(&config)?;
    Ok(config)
}

/// Read and parse the pipeline configuration at `path`.
///
/// # Errors
///
/// [`Error::ConfigFile`] when the file cannot be read, otherwise as
/// [`parse_pipeline_config`].
pub fn load_pipeline_config(path: impl AsRef<Path>) -> Result<PipelineConfig> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|source| Error::ConfigFile {
        path: path.display().to_string(),
        source,
    })?;
    parse_pipeline_config(&text)
}

/// Check the values of a configuration that already decoded.
///
/// # Errors
///
/// [`Error::Config`] naming the first offending field.
pub fn check_values(config: &PipelineConfig) -> Result<()> {
    if config.version != PIPELINE_CONFIG_VERSION {
        return Err(Error::config(
            "version",
            format!(
                "version {} is not supported, expected {PIPELINE_CONFIG_VERSION}",
                config.version
            ),
        ));
    }
    if config.ingress.tombstone_ttl_ms == 0 {
        return Err(Error::config(
            "ingress.tombstone_ttl_ms",
            "must be at least 1",
        ));
    }
    if !(1..=MAX_BATCH_SIZE).contains(&config.ingress.batch_size) {
        return Err(Error::config(
            "ingress.batch_size",
            format!("must be 1 to {MAX_BATCH_SIZE}"),
        ));
    }
    for lane in Lane::ALL {
        if !(1..=MAX_CONCURRENCY).contains(&config.lanes.get(lane).concurrency) {
            return Err(Error::config(
                format!("lanes.{}.concurrency", lane.as_str()),
                format!("must be 1 to {MAX_CONCURRENCY}"),
            ));
        }
    }
    for (index, source) in config.sources.iter().enumerate() {
        let pattern = source.pattern.as_str();
        let body = pattern.strip_suffix('*').unwrap_or(pattern);
        if pattern.is_empty() || body.contains('*') {
            return Err(Error::config(
                format!("sources[{index}].match"),
                "must be a source, a prefix ending in `*`, or `*`",
            ));
        }
    }
    Ok(())
}

/// Resolve every queue the configuration names against the manifest.
///
/// # Errors
///
/// [`Error::Config`] naming the field whose queue is not declared, has no
/// `visibility_ms`, or is already used by another role.
pub fn resolve_queues(config: &PipelineConfig, manifest: &QueueManifest) -> Result<ResolvedQueues> {
    let mut used: Vec<(String, &str)> = Vec::new();
    let mut resolve = |field: String, name: &str| -> Result<QueueRef> {
        let Some(declared) = manifest.get(name) else {
            return Err(Error::config(
                field,
                format!("queue {name:?} is not declared in the queue manifest"),
            ));
        };
        let Some(visibility_ms) = declared.visibility_ms else {
            return Err(Error::config(
                field,
                format!("queue {name:?} needs a visibility_ms in the queue manifest"),
            ));
        };
        if let Some((other, _)) = used.iter().find(|(_, queue)| *queue == name) {
            return Err(Error::config(
                field,
                format!("queue {name:?} is already used by {other}"),
            ));
        }
        used.push((field, &declared.name));
        Ok(QueueRef {
            name: declared.name.clone(),
            visibility_ms,
            max_attempts: declared.max_attempts,
        })
    };
    let ingress = resolve("ingress.queue".to_owned(), &config.ingress.queue)?;
    let dead = resolve("dead_letter.queue".to_owned(), &config.dead_letter.queue)?;
    let mut lane = |lane: Lane| {
        let field = format!("lanes.{}.queue", lane.as_str());
        resolve(field, &config.lanes.get(lane).queue)
    };
    let lanes = [
        lane(Lane::Interactive)?,
        lane(Lane::Actionable)?,
        lane(Lane::Background)?,
        lane(Lane::Digest)?,
    ];
    Ok(ResolvedQueues {
        ingress,
        dead,
        lanes,
    })
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
