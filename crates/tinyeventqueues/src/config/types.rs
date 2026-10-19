//! Resolved queue references.

use tinyeventqueues_bus::Lane;

/// One manifest queue the pipeline uses, resolved from its declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRef {
    /// The queue name.
    pub name: String,
    /// The lease the pipeline asks for when it receives.
    pub visibility_ms: u64,
    /// Deliveries a message gets before the queue dead-letters it.
    pub max_attempts: u32,
}

/// Every queue the pipeline uses, by role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedQueues {
    /// Raw inbound events.
    pub ingress: QueueRef,
    /// Parked dead work.
    pub dead: QueueRef,
    /// One queue per lane, in [`Lane::ALL`] order.
    pub lanes: [QueueRef; 4],
}

impl ResolvedQueues {
    /// The queue of `lane`.
    #[must_use]
    pub fn lane(&self, lane: Lane) -> &QueueRef {
        let [interactive, actionable, background, digest] = &self.lanes;
        match lane {
            Lane::Interactive => interactive,
            Lane::Actionable => actionable,
            Lane::Background => background,
            Lane::Digest => digest,
        }
    }

    /// Every queue: ingress, the lanes, then the dead queue.
    #[must_use]
    pub fn all(&self) -> Vec<&QueueRef> {
        let mut all = vec![&self.ingress];
        all.extend(self.lanes.iter());
        all.push(&self.dead);
        all
    }

    /// The queue named `name`, if the pipeline uses one.
    #[must_use]
    pub fn by_name(&self, name: &str) -> Option<&QueueRef> {
        self.all().into_iter().find(|queue| queue.name == name)
    }
}
