//! The bus identity of the tinyeventqueues module: interface names, object
//! paths, and one constant per member.
//!
//! Two interfaces cross the bus. The module serves [`INTERFACE`] at
//! [`OBJECT_PATH`]; hosts call it to ingest events and operate the pipeline.
//! The host serves [`host::INTERFACE`] at [`host::OBJECT_PATH`]; the module
//! calls it back to classify borderline salience and to deliver turns.
//!
//! Nothing here is a string literal at a call site. A host names a member
//! through [`methods`] and the object through [`OBJECT_PATH`], so a rename is a
//! compile error in every consumer rather than a runtime "unknown method".

/// The well-known interface name the module claims on the bus.
pub const INTERFACE: &str = "ai.tinyhumans.tinyeventqueues.EventQueues";

/// The object path the module serves its interface at.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyeventqueues/EventQueues";

/// One constant per member of [`INTERFACE`].
pub mod methods {
    /// Durably queues inbound events. Takes a [`crate::IngestRequest`] and
    /// returns a [`crate::IngestResponse`]; it returns only after the durable
    /// write, so the platform acknowledgement may follow it.
    pub const INGEST: &str = "Ingest";
    /// Replaces the running pipeline configuration. Takes a
    /// [`crate::ConfigureRequest`] and returns a [`crate::ConfigureResponse`].
    pub const CONFIGURE: &str = "Configure";
    /// Reports queue depths and outcome counts. Takes a
    /// [`crate::StatsRequest`] and returns a [`crate::StatsResponse`].
    pub const STATS: &str = "Stats";
    /// Lists dead-lettered work. Takes a [`crate::ListDeadRequest`] and returns
    /// a [`crate::ListDeadResponse`].
    pub const LIST_DEAD: &str = "ListDead";
    /// Puts one dead letter back into the pipeline. Takes a
    /// [`crate::RedriveRequest`] and returns a [`crate::RedriveResponse`].
    pub const REDRIVE: &str = "Redrive";
    /// Runs one pass of the pipeline now. Takes a [`crate::DrainRequest`] and
    /// returns a [`crate::DrainResponse`].
    pub const DRAIN: &str = "Drain";
}

/// Every member of [`INTERFACE`], in the order the interface dispatches them.
///
/// `crates/tinyeventqueues` asserts its served members against this list, so
/// the two cannot drift.
pub const METHODS: &[&str] = &[
    methods::INGEST,
    methods::CONFIGURE,
    methods::STATS,
    methods::LIST_DEAD,
    methods::REDRIVE,
    methods::DRAIN,
];

/// The callback interface a host serves for the module.
pub mod host {
    /// The interface name of the host callbacks.
    pub const INTERFACE: &str = "ai.tinyhumans.tinyeventqueues.Host";

    /// The object path the host serves its callbacks at.
    pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyeventqueues/Host";

    /// One constant per host callback.
    pub mod methods {
        /// Classifies a borderline event. Takes a
        /// [`crate::ClassifySalienceRequest`] and returns a
        /// [`crate::ClassifySalienceResponse`].
        pub const CLASSIFY_SALIENCE: &str = "ClassifySalience";
        /// Delivers one turn to the agent. Takes a [`crate::DeliverRequest`]
        /// and returns a [`crate::DeliverResponse`].
        pub const DELIVER: &str = "Deliver";
    }

    /// Every host callback, in dispatch order.
    pub const METHODS: &[&str] = &[methods::CLASSIFY_SALIENCE, methods::DELIVER];
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
