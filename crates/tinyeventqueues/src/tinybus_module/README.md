# TinyBus Adapter

This module is the boundary between the pipeline and TinyBus module ABI v1.
`EventQueuesService` serves `Pipeline` as the typed members `Ingest`,
`Configure`, `Stats`, `ListDead`, `Redrive` and `Drain`, and `setup` registers
the object and claims the well-known interface name. Neither the name, the
object path, nor the payload types are spelled here: they come from
`tinyeventqueues-bus`, so a rename is a compile error in every consumer instead
of an `UnknownMethod` at runtime.

`tinybus_module::module_export!` emits the descriptor, embedded manifest, and
initialization symbols consumed by the dynamic loader. The manifest method list
must stay aligned with the interface macro's dispatch table and with
`tinyeventqueues_bus::names::METHODS`; the unit tests check both.

Host callbacks: a `BusHost` installed as the pipeline's `Host` calls the
host's `ai.tinyhumans.tinyeventqueues.Host` interface (`ClassifySalience`,
`Deliver`) at `names::host::OBJECT_PATH`. While no host serves it, deliveries
fail and turns back off and retry. Passes run when the host calls `Drain`.

Storage: until the module is wired to the host's storage through the
`tinystoragedrivers` storage bus, `setup` builds the pipeline over an
in-process `MemoryStorage` with the shipped `queues.json` and
`pipeline.json`. That exercises the contract end to end but is not durable
across a host restart.
