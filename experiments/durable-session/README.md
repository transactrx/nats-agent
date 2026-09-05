# Durable session lifecycle experiment

This nested Go module is an **isolated proof**, excluded from the parent module's
normal build. It does not change nats-agent's chat protocol or live applications.
See [the platform design](../../docs/durable-execution-platform.md).

The experiment implements regional DynamoDB conditional ownership, fencing epochs,
explicit cancellation requests, grant checks, step intents, committed outputs,
and environment manifest/workspace references retained for the next turn. Wakeup
notifications are optional hints. Durable state is the authority.

## Run locally

```
cd experiments/durable-session
go test -race -count=1 ./...
go vet ./...
```

The default tests use an in-memory CAS implementation to exercise the protocol.
The process-recovery test is skipped without an explicit scratch table.

## Run against a scratch DynamoDB table

Create a disposable on-demand table with string partition key `pk` in a test
account. Set AWS_PROFILE, AWS_REGION, and DURABLE_PROBE_TABLE, then run:

```
DURABLE_PROBE_TABLE=<scratch-table> go test -race -count=1 -v ./...
```

Never point this experiment at an application table. Each test uses synthetic
scope identifiers and synthetic artifact references. Delete the table afterward.
The test does not create/delete infrastructure itself.

## Evidence, September 5, 2026

All eight tests passed against a newly created on-demand table in AWS profile
Development / us-east-1; the temporary table was deleted afterward. Race-enabled
suite elapsed about seven seconds. This is correctness evidence, not a benchmark.

- Durable acceptance succeeds even when the wakeup callback reports NATS down;
  canceling the browser context afterward does not cancel the run.
- Exactly one of 32 concurrent claimants wins.
- A replacement worker advances the ownership epoch; the stale worker cannot
  heartbeat or publish a checkpoint.
- A completed step is read back instead of dispatched again. A started step with
  no committed result is rejected as uncertain, requiring reconciliation.
- Duplicate current submissions are idempotent; changed input and a competing
  turn are rejected. Tenant scopes have distinct storage keys.
- Revoked/expired grants block progress; explicit cancellation fences further
  checkpoints and keeps the session busy until termination is confirmed.
- A separate test process checkpoints to DynamoDB and exits via os.Exit without
  cleanup; the parent uses a new SDK client, acquires a new epoch after simulated
  lease expiry, reads the committed result, and completes the run.
- A subsequent turn retains the environment/workspace references.

## Boundaries and unfinished production work

- No real model, tool, sandbox, package installation, S3 object transfer, browser
  replay, durable scheduling loop, or Identity service is exercised. Artifact
  references are synthetic; Authorizer is a test double.
- Clock advancement simulates lease expiry. The production lease protocol needs
  bounded clock-skew assumptions, supervisor termination, and target-side fencing.
- A single bounded session aggregate is intentional for the experiment. It holds
  only the current run, has a 128 KiB limit, and is NOT the production schema.
  Production needs separate run/step/event records, retained request deduplication,
  atomic acceptance/session ordering, sharded ready indexes, and archival policy.
- No workload, chaos, multi-region, or tenant-fairness capacity benchmark was run.
- A CAS fence prevents obsolete checkpoint publication; it does not kill old code
  or fence a shared EFS directory. Each execution attempt needs private writable
  scratch plus immutable committed snapshots and broker-enforced epochs.
- Cancellation is requested, not reported completed. Sandbox-stop confirmation,
  deadline reconciliation, pause/resume, and terminal cleanup are future work.
- Submit/Cancel/Load are internal primitives, not public authorized endpoints.
  The production API must derive Scope from verified identity and enforce access
  before every read/control operation. There is no public listener in this proof.
- The regional store must not be used as a global lock through eventual replicas.
- External actions are not guaranteed exactly once. Unknown outcomes require a
  target idempotency key, a safe replay policy, or operator reconciliation.
