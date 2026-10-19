//! The host's callback interface, reached over the bus.
//!
//! The host serves `ai.tinyhumans.tinyeventqueues.Host` at
//! [`names::host::OBJECT_PATH`] for `ClassifySalience` and `Deliver`. It is an
//! optional dependency of the module: a host that does not serve it yet makes
//! classification fall back to the scored class and deliveries fail, and the
//! turns back off and retry until it does.

use tinybus::Connection;
use tinyeventqueues_bus::{
    ClassifySalienceRequest, ClassifySalienceResponse, DeliverRequest, DeliverResponse, names,
};

use crate::Host;

/// [`Host`] over the bus.
#[derive(Clone)]
pub(crate) struct BusHost {
    pub(crate) connection: Connection,
}

impl std::fmt::Debug for BusHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusHost").finish_non_exhaustive()
    }
}

impl BusHost {
    async fn call<R: serde::de::DeserializeOwned>(
        &self,
        method: &'static str,
        request: impl serde::Serialize + Send,
    ) -> crate::Result<R> {
        let failed = |error: tinybus::Error| crate::Error::Host {
            method,
            message: error.to_string(),
        };
        let proxy = self
            .connection
            .proxy(
                names::host::INTERFACE,
                names::host::OBJECT_PATH,
                names::host::INTERFACE,
            )
            .map_err(failed)?;
        proxy.call(method, (request,)).await.map_err(failed)
    }
}

#[async_trait::async_trait]
impl Host for BusHost {
    async fn classify_salience(
        &self,
        request: ClassifySalienceRequest,
    ) -> crate::Result<ClassifySalienceResponse> {
        self.call(names::host::methods::CLASSIFY_SALIENCE, request)
            .await
    }

    async fn deliver(&self, request: DeliverRequest) -> crate::Result<DeliverResponse> {
        self.call(names::host::methods::DELIVER, request).await
    }
}
