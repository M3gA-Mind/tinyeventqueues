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
- the TinyBus adapter over the host's storage (storage bus documents and
  queues, memory only when the host serves none), calling the host back over
  the bus, with a module-owned pass timer, a private configuration blob and a
  `static-link` build for hosts that link the module
- the gate (allow/deny, DM/group policy, self-echo, bot-loop guard, durable
  token buckets), ambient context, and the durable debounce buffer
- salience with habituation and borderline host classification; lanes with
  overflow spilling to a folded digest
- the cadence scheduler with ingestion budgets and overflow folding, restart
  safe

- dispatch through the host's `Deliver` with ack policies, exponential
  backoff and dead letters; per-thread order across lanes and on queue
  backends without groups
- a no-loss property test and a 10,000-event flood test

## Next

- adopting the shared module CI and release workflows

## Out Of Scope

- talking to platforms: hosts normalize events and acknowledge platforms
- running the agent: the host's `Deliver` does
- exactly-once delivery: delivery is at least once, turns carry stable ids
