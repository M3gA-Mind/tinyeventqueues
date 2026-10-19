# tinyeventqueues-bus

Every type that crosses the tinyeventqueues module's `TinyBus` boundary, and
the names of the members that carry them.

The module ships as a loadable `cdylib`; a host can load it but cannot `use`
anything out of it, so the vocabulary is published here as an ordinary
library.

| module    | what it holds                                                           |
| --------- | ----------------------------------------------------------------------- |
| `event`   | the `InboundEvent` envelope every source is normalized into              |
| `ops`     | payloads of `Ingest`, `Configure`, `Stats`, `ListDead`, `Redrive`, `Drain` |
| `host`    | payloads of the host callbacks `ClassifySalience` and `Deliver` (`Turn`) |
| `outcome` | `Disposition`, `Reason`, `Outcome`: what happened to an event and why    |
| `config`  | `PipelineConfig`, the strict wire form of `pipeline.json`                |
| `names`   | interface names, object paths, one constant per member                   |
| `version` | `CONTRACT_VERSION` and the bind rule a host applies to it                |

Two dependencies, both pure Rust: `serde` and `serde_json`. CI asserts the
crate never pulls in a transport, an async runtime, an HTTP client or a native
library.

## This crate sits underneath `tinyeventqueues`

`tinyeventqueues` depends on this crate and re-exports all of it, so
`tinyeventqueues::InboundEvent` and `tinyeventqueues_bus::InboundEvent` are the
same type. A module author depends on `tinyeventqueues` and gets behavior and
vocabulary; a host depends on `tinyeventqueues-bus` and gets vocabulary alone.

## Making a call

Arguments travel as a positional JSON array, and member names come from
`names`:

```rust,ignore
use tinyeventqueues_bus::{names, IngestRequest, IngestResponse};

let proxy = connection.proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)?;
let reply: IngestResponse = proxy
    .call(names::methods::INGEST, (IngestRequest::new(events),))
    .await?;
```

A host serves `names::host::INTERFACE` at `names::host::OBJECT_PATH` with the
`names::host::methods` members to receive turns and classification requests.

## Versioning

`CONTRACT_VERSION` describes this vocabulary, not the package. Bump the major
component when a payload's wire form changes incompatibly or a member is
removed or renamed, the minor component when a member or an optional field is
added. The payload tests pin the serde representation, because it is the wire
form.
