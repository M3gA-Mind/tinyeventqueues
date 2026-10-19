//! The host callbacks the pipeline calls.
//!
//! A library host implements [`Host`] directly; the `TinyBus` adapter
//! implements it by calling the host's `ai.tinyhumans.tinyeventqueues.Host`
//! interface. A pipeline without a host never classifies (rule scores stand)
//! and never dispatches (turns wait in their lanes until one is installed).

use async_trait::async_trait;
use tinyeventqueues_bus::{
    ClassifySalienceRequest, ClassifySalienceResponse, DeliverRequest, DeliverResponse,
};

use crate::Result;

/// What the pipeline asks of its host.
#[async_trait]
pub trait Host: Send + Sync + std::fmt::Debug {
    /// Classify a borderline event. Called only for scores inside the
    /// configured borderline band, within its hourly budget.
    ///
    /// # Errors
    ///
    /// Any failure; the pipeline then keeps the rule score.
    async fn classify_salience(
        &self,
        request: ClassifySalienceRequest,
    ) -> Result<ClassifySalienceResponse>;

    /// Deliver one turn to the agent. Delivery is at least once: the same
    /// `turn_id` can arrive again after a crash or a retry, so handling must
    /// be idempotent. For `after_turn` sources, return only once the agent's
    /// turn is done, then acknowledge the turn's `platform_acks`.
    ///
    /// # Errors
    ///
    /// Any failure; the pipeline retries with backoff, like
    /// [`DeliverStatus::Retry`](tinyeventqueues_bus::DeliverStatus::Retry).
    async fn deliver(&self, request: DeliverRequest) -> Result<DeliverResponse>;
}
