# Roadmap

The contract is [`docs/specs/event-pipeline.md`](docs/specs/event-pipeline.md);
the delivery order is [`docs/plans/event-pipeline.md`](docs/plans/event-pipeline.md).

## Shipped

- the wire contract (`tinyeventqueues-bus`): envelope, members, host
  callbacks, outcomes, configuration
- `queues.json` and `pipeline.json`, strict, with field-path errors
- ingress with completion tombstones that survive a restart, on backends with
  and without native dedupe
- the outcome ledger, dead-letter parking, `ListDead` and `Redrive`
- the TinyBus adapter over in-process storage
- the gate (allow/deny, DM/group policy, self-echo, bot-loop guard, durable
  token buckets), ambient context, and the durable debounce buffer
- salience with habituation and borderline host classification; lanes with
  overflow spilling to a folded digest

## Next

- cadence scheduler with ingestion budgets and overflow folding
- dispatch through `Deliver`, retries and dead letters; the no-loss property
  test and a 10,000-event flood test
- wiring the module to the host's storage through the storage bus
- re-pinning `tinystoragedrivers-core` once tinyhumansai/tinystoragedrivers#71
  merges

## Out Of Scope

- talking to platforms: hosts normalize events and acknowledge platforms
- running the agent: the host's `Deliver` does
- exactly-once delivery: delivery is at least once, turns carry stable ids
