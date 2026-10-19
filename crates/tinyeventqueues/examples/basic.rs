//! Minimal end-to-end usage of the crate: build a pipeline over in-memory
//! storage, ingest events, and see a redelivery rejected.
//!
//! Examples are compiled and linted in CI, so they cannot drift from the API.
//! Run it with:
//!
//! ```sh
//! cargo run -p tinyeventqueues --example basic
//! ```

use std::sync::Arc;

use tinyeventqueues::{EventKind, InboundEvent, Pipeline, Sender};
use tinystoragedrivers_core::queue::DocumentQueue;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = MemoryStorage::new().for_scope(&Scope::local())?;
    let queues = Arc::new(DocumentQueue::new(storage.documents()));
    let pipeline = Pipeline::builder(queues, Arc::clone(storage.documents()))
        .build()
        .await?;

    let event = InboundEvent::new(
        "channel:telegram",
        "1001",
        "chat-42",
        Sender::new("user-7"),
        EventKind::Message,
        0,
    )
    .text("are you there?")
    .direct(true);

    for attempt in ["first delivery", "platform redelivery"] {
        let response = pipeline.ingest(vec![event.clone()]).await?;
        let result = &response.results[0];
        println!(
            "{attempt}: {:?} (ack platform now: {})",
            result.disposition, result.ack_platform
        );
    }

    // A bad envelope is dropped visibly, never silently.
    let mut broken = event.clone();
    broken.event_id.clear();
    let response = pipeline.ingest(vec![broken]).await?;
    println!(
        "empty id: {:?} because {:?}",
        response.results[0].disposition, response.results[0].reason
    );

    let stats = pipeline.stats().await?;
    println!("ingress depth: {}", stats.queues[0].ready);
    Ok(())
}
