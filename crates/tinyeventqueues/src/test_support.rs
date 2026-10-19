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
use tinystoragedrivers_core::{
    DocumentStore, MemoryStorage, Result, Scope, StorageBackend, StorageError,
};

use crate::{ManualClock, Pipeline, PipelineBuilder};

/// One storage backend, shared by every pipeline built over it, so a new
/// pipeline is a restart.
pub(crate) struct Fixture {
    pub(crate) clock: ManualClock,
    pub(crate) docs: Arc<dyn DocumentStore>,
    pub(crate) queues: Arc<dyn QueueStore>,
}

impl Fixture {
    /// A queue backend with every feature (`DocumentQueue`).
    pub(crate) fn new() -> Self {
        Self::with_native(true)
    }

    /// With `native`, a `DocumentQueue`; without, a [`Broker`] that has
    /// neither dedupe nor groups and refuses messages that use them, the way
    /// the `RabbitMQ` driver does.
    pub(crate) fn with_native(native: bool) -> Self {
        let clock = ManualClock::new(1_000_000);
        let storage = MemoryStorage::with_clock(clock.clock())
            .for_scope(&Scope::local())
            .unwrap();
        let docs = Arc::clone(storage.documents());
        let queue = DocumentQueue::new(&docs).with_clock(clock.clock());
        let queues: Arc<dyn QueueStore> = if native {
            Arc::new(queue)
        } else {
            Arc::new(Broker(queue))
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

/// An event from a human in a group conversation.
pub(crate) fn event(source: &str, id: &str, conversation: &str) -> InboundEvent {
    InboundEvent::new(
        source,
        id,
        conversation,
        Sender::new("user-1"),
        EventKind::Message,
        0,
    )
    .text(format!("message {id}"))
}

/// A queue backend like the `RabbitMQ` driver: no dedupe and no groups, and
/// a message that sets `group` or `dedupe_key` is refused.
#[derive(Debug)]
pub(crate) struct Broker(pub(crate) DocumentQueue);

#[async_trait]
impl QueueStore for Broker {
    fn features(&self) -> QueueFeatures {
        QueueFeatures {
            dedupe: false,
            groups: false,
            ..self.0.features()
        }
    }
    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()> {
        self.0.ensure_queue(queue, spec).await
    }
    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>> {
        if messages
            .iter()
            .any(|message| message.group.is_some() || message.dedupe_key.is_some())
        {
            return Err(StorageError::invalid_input(
                "this queue backend supports neither groups nor dedupe keys",
            ));
        }
        self.0.enqueue(queue, messages).await
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
