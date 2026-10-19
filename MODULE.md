# tinyeventqueues TinyBus Module

This package contains the native `tinyeventqueues` module for TinyBus module
ABI v1. Install only the archive matching the host operating system and
architecture.

The module claims `ai.tinyhumans.tinyeventqueues.EventQueues`, serves it at
`/ai/tinyhumans/tinyeventqueues/EventQueues`, and provides `Ingest`,
`Configure`, `Stats`, `ListDead`, `Redrive` and `Drain`. It durably queues
inbound events and filters them before they reach an agent; hosts serve
`ai.tinyhumans.tinyeventqueues.Host` (`ClassifySalience`, `Deliver`) for the
module's callbacks. The interface names, object paths, member names and
payload types are published as the `tinyeventqueues-bus` crate, so a host
names them from a library rather than by string literal.

Until the module is wired to the host's storage through the
`tinystoragedrivers` storage bus, it runs over in-process memory storage with
the queue manifest and pipeline configuration compiled into it; its queues do
not survive a host restart in that mode.

The archive contains one `.so`, `.dylib`, or `.dll` plus `modules.toml`. Keep
those files together when copying them into a TinyBus module directory. The
allowlist binds the native library filename to its SHA-256 digest so TinyBus can
reject a missing, renamed, or modified artifact before initialization.

The GitHub release also publishes `checksum.toml` as a separate asset. TinyBus
checks that manifest before downloading and extracting the selected platform
archive. Install directly from a tagged release with:

```sh
tinybus modules load-github \
  https://github.com/tinyhumansai/tinyeventqueues/releases/tag/v0.1.0 \
  tinyeventqueues-0.1.0-ubuntu-24.04-x86_64.tar.gz \
  <archive-sha256>
```

TinyBus modules are trusted in-process code. Install release artifacts only
from a trusted source and restart the host after replacing a loaded module.
