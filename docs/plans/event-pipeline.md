# Implement the Inbound Event Pipeline

Linked specification: [`../specs/event-pipeline.md`](../specs/event-pipeline.md)

The pipeline lands as five stacked pull requests, each green on its own
(format, clippy with `-D warnings`, build, tests, 90% per-file coverage).
Each adds its configuration section to `pipeline.json`, its schema, and the
strict loader together, with a test that the shipped files still load.

Assumptions: `tinystoragedrivers-core` with the `QueueStore` port and
`QueueManifest` (tinyhumansai/tinystoragedrivers#71) is pinned to the fork
branch until that PR merges, then re-pinned to a tinyhumansai revision. Hosts
wire the module to their storage in a later change.

## 1. De-template, contract, ingress

- Rename the template crates to `tinyeventqueues` / `tinyeventqueues-bus`, the
  bus names to `ai.tinyhumans.tinyeventqueues.*`, reset `CONTRACT_VERSION` to
  0.1, and rewrite the project docs.
- Contract: `InboundEvent` envelope; payloads of `Ingest`, `Configure`,
  `Stats`, `ListDead`, `Redrive`, `Drain`; host callbacks `ClassifySalience`
  and `Deliver`; `Disposition` and `Reason`; `PipelineConfig`.
- `queues.json` (queue manifest) and `pipeline.json` with
  `schemas/pipeline.schema.json`; strict loading with field-path errors.
- Module: ingress stage, tombstones for backends without dedupe, the outcome
  ledger, dead-letter parking/listing/redrive, `Pipeline` + builder, the
  TinyBus adapter.
- Tests first: a duplicate redelivery after a restart is rejected on both
  backends; the shipped files load and resolve.

## 2. Gate, rate limits, coalesce

- `gate` and `coalesce` sections in `pipeline.json`. Until salience lands, a
  flushed batch goes to `interactive` when any event is direct, addressed or
  a mention, otherwise to `actionable`; rate-limited events spill to `digest`
  as single-event turns (folding comes with the digest work).
- Tests first: self-echo, deny/allow lists, DM/group policy, bot-loop guard,
  token buckets surviving a restart, `context` outcome for unaddressed chatter;
  no cross-thread merge, per-key order, media and control flush, edit/delete
  amendments.

## 3. Salience, habituation, lanes, digest spill

- `salience` section; `ClassifySalience` through a `Host` trait.
- Tests first: rule scores, SimHash habituation decay, borderline-only
  classification within budget, lane assignment, overflow spilling to digest.

## 4. Cadence scheduler, budgets, overflow folding

- `cadence` per source/lane, budgets, persisted next-due ticks.
- Fake-clock tests first: an `every(10m)` source with 1,000 events stays within
  budget per tick and folds the rest; a restart between ticks neither skips
  nor double-fires; an instant source over budget degrades and recovers.

## 5. Dispatch, retry, dead letters, property and flood tests

- `Deliver` with ack policies, backoff through nack, parking after
  `max_attempts`; the TinyBus adapter keeps exporting the full contract.
- The no-loss property test and a 10,000-event mixed flood test.

## Completion checklist

- [x] 1. De-template, contract, ingress
- [x] 2. Gate, rate limits, coalesce
- [ ] 3. Salience, habituation, lanes, digest spill
- [ ] 4. Cadence scheduler, budgets, overflow folding
- [ ] 5. Dispatch, retry, dead letters, property and flood tests
