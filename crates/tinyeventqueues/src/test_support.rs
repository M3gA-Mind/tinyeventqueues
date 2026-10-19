//! Shared fixtures for the unit tests: storage, a queue backend without
//! dedupe, and events.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use async_trait::async_trait;
use tinyeventqueues_bus::{EventKind, InboundEvent, Sender};
use tinystoragedrivers_core::queue::{
    DeadLetter, Delivery, DocumentQueue, Enqueued, NewMessage, QueueFeatures, QueueSpec,
    QueueStats, QueueStore, Receipt, Retry,
};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Result, Scope, StorageBackend};

use crate::{ManualClock, Pipeline, PipelineBuilder};

/// One storage backend, shared by every pipeline built over it, so a new
/// pipeline is a restart.
pub(crate) struct Fixture {
    pub(crate) clock: ManualClock,
    pub(crate) docs: Arc<dyn DocumentStore>,
    pub(crate) queues: Arc<dyn QueueStore>,
}

impl Fixture {
    /// A queue backend with native dedupe.
    pub(crate) fn new() -> Self {
        Self::with_dedupe(true)
    }

    /// A queue backend that reports `dedupe` as given and, without it,
    /// ignores dedupe keys the way a broker would.
    pub(crate) fn with_dedupe(dedupe: bool) -> Self {
        let clock = ManualClock::new(START_MS);
        let storage = MemoryStorage::with_clock(clock.clock())
            .for_scope(&Scope::local())
            .unwrap();
        let docs = Arc::clone(storage.documents());
        let queue = DocumentQueue::new(&docs).with_clock(clock.clock());
        let queues: Arc<dyn QueueStore> = if dedupe {
            Arc::new(queue)
        } else {
            Arc::new(NoDedupe(queue))
        };
        Self {
            clock,
            docs,
            queues,
        }
    }

    pub(crate) fn builder(&self) -> PipelineBuilder {
        Pipeline::builder(Arc::clone(&self.queues), Arc::clone(&self.docs))
            .clock(self.clock.clock())
    }

    /// A pipeline with the shipped files: building one again is a restart.
    pub(crate) async fn pipeline(&self) -> Pipeline {
        self.builder().build().await.unwrap()
    }
}

/// Where every [`Fixture`] clock starts.
pub(crate) const START_MS: u64 = 1_000_000;

/// An event from a human in a group conversation, received at [`START_MS`].
pub(crate) fn event(source: &str, id: &str, conversation: &str) -> InboundEvent {
    InboundEvent::new(
        source,
        id,
        conversation,
        Sender::new("user-1"),
        EventKind::Message,
        START_MS,
    )
    .text(format!("message {id} {}", words(id)))
}

/// A queue backend without dedupe: a broker such as `RabbitMQ`.
#[derive(Debug)]
pub(crate) struct NoDedupe(pub(crate) DocumentQueue);

#[async_trait]
impl QueueStore for NoDedupe {
    fn features(&self) -> QueueFeatures {
        QueueFeatures {
            dedupe: false,
            ..self.0.features()
        }
    }
    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()> {
        self.0.ensure_queue(queue, spec).await
    }
    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>> {
        let stripped: Vec<NewMessage> = messages
            .iter()
            .cloned()
            .map(|mut message| {
                message.dedupe_key = None;
                message
            })
            .collect();
        self.0.enqueue(queue, &stripped).await
    }
    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> Result<Vec<Delivery>> {
        self.0.receive(queue, max, visibility_ms, consumer).await
    }
    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool> {
        self.0.ack(queue, receipt).await
    }
    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool> {
        self.0.nack(queue, receipt, retry).await
    }
    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool> {
        self.0.extend(queue, receipt, visibility_ms).await
    }
    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        self.0.stats(queue).await
    }
    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>> {
        self.0.dead_letters(queue, limit).await
    }
    async fn redrive(&self, queue: &str, id: &str) -> Result<bool> {
        self.0.redrive(queue, id).await
    }
    async fn purge(&self, queue: &str) -> Result<u64> {
        self.0.purge(queue).await
    }
}

/// Take every turn waiting on `lane`, in delivery order, acknowledging each
/// so the next of its conversation becomes deliverable.
pub(crate) async fn take_turns(
    fixture: &Fixture,
    pipeline: &Pipeline,
    lane: tinyeventqueues_bus::Lane,
) -> Vec<crate::turn::PendingTurn> {
    let queue = pipeline.queues().lane(lane).name.clone();
    let mut turns = Vec::new();
    loop {
        let batch = fixture
            .queues
            .receive(&queue, 100, 60_000, "inspect")
            .await
            .unwrap();
        if batch.is_empty() {
            return turns;
        }
        for delivery in batch {
            turns.push(serde_json::from_value(delivery.payload).unwrap());
            fixture.queues.ack(&queue, &delivery.receipt).await.unwrap();
        }
    }
}

/// A few words derived from `id`, so test events' texts are not
/// near-duplicates of each other (which would habituate their scores).
fn words(id: &str) -> String {
    const WORDS: [&str; 16] = [
        "amber", "birch", "cobalt", "delta", "ember", "fjord", "garnet", "harbor", "iris",
        "juniper", "kelp", "lumen", "maple", "nectar", "onyx", "prism",
    ];
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in id.bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
    }
    (0..6)
        .map(|_| {
            hash = hash
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            WORDS[usize::try_from(hash >> 60).unwrap()]
        })
        .collect::<Vec<_>>()
        .join(" ")
}
