# Implement the Inbound Event Pipeline

Linked specification: [`../specs/event-pipeline.md`](../specs/event-pipeline.md)

The pipeline lands as five stacked pull requests, each green on its own
(format, clippy with `-D warnings`, build, tests, 90% per-file coverage).
Each adds its configuration section to `pipeline.json`, its schema, and the
strict loader together, with a test that the shipped files still load.

Assumptions: `tinystoragedrivers-core` with the `QueueStore` port and
`QueueManifest` (tinyhumansai/tinystoragedrivers#71) is pinned to the
tinyhumansai revision that merged it. Hosts wire the module to their storage
in a later change.

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

- `salience` and `digest` sections and per-lane `max_backlog`;
  `ClassifySalience` through a `Host` trait. A batch's score is its best
  event's score; noise (below the background threshold) folds into the
  digest with `noise` or `habituated`, a lane at `max_backlog` spills with
  `lane_overflow`, and the digest accumulator releases one turn per
  `max_items` events or `max_wait_ms`.
- Tests first: rule scores, SimHash habituation decay, borderline-only
  classification within budget, lane assignment, overflow spilling to digest.

## 4. Cadence scheduler, budgets, overflow folding

- `cadence` and `budget` per source or lane (a source's wins), persisted
  next-due ticks with the firing tick and its release count, so a crash
  mid-tick resumes with what is left of the budget. Ticks are aligned to the
  epoch (`every`) or UTC minutes (`cron`); ticks missed while down collapse
  into one. Staging is one entry per cadence and thread; a thread's later
  turn (any lane) folds into its waiting entry, which then goes out on the
  more urgent lane, so nothing overtakes. Interactive-scored work bypasses a
  scheduled cadence (taking its thread's waiting entry with it). An instant
  cadence over its per-second budget stages for `degrade_debounce_ms` and
  releases those entries within the budget.
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
- [x] 3. Salience, habituation, lanes, digest spill
- [x] 4. Cadence scheduler, budgets, overflow folding
- [ ] 5. Dispatch, retry, dead letters, property and flood tests
