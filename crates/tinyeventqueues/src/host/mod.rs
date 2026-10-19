//! The host callbacks the pipeline calls.
//!
//! A library host implements [`Host`] directly; the `TinyBus` adapter
//! implements it by calling the host's `ai.tinyhumans.tinyeventqueues.Host`
//! interface. A pipeline without a host never classifies: rule scores stand.

use async_trait::async_trait;
use tinyeventqueues_bus::{ClassifySalienceRequest, ClassifySalienceResponse};

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
}
