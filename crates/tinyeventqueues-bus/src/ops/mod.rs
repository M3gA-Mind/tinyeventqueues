//! The request and response payloads of the module's own members: `Ingest`,
//! `Configure`, `Stats`, `ListDead`, `Redrive` and `Drain`.

mod types;

pub use types::{
    ConfigureRequest, ConfigureResponse, DeadItem, DrainReport, DrainRequest, DrainResponse,
    IngestRequest, IngestResponse, IngestResult, ListDeadRequest, ListDeadResponse, QueueDepth,
    RedriveRequest, RedriveResponse, StatsRequest, StatsResponse,
};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
