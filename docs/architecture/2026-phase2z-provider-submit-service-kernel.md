# Phase 2Z: Provider Submit Service Kernel

Date: 2026-07-17

Status: implemented and verified with local fake providers plus PostgreSQL 18.
This phase activates no provider, credential, public route, billing behavior,
or external call.

Follow-up: Phase 2AA closes the recoverable per-launch CLI submit workspace and
frozen process-path binding:
[`2026-phase2aa-recoverable-submit-attempt-workspace.md`](2026-phase2aa-recoverable-submit-attempt-workspace.md).

## Scope

Phase 2Z composes the existing submit primitives into one provider-neutral
service iteration:

```text
database submit deadline
  -> provider/account recovery claim
  -> fresh executor resume or claim
  -> provider-specific typed projector
  -> unique submit orchestrator
  -> fenced attach, terminal result, or deferred evidence recovery
```

The composition owns scheduling order, executor and recovery heartbeats,
projection failure semantics, retry command identity, and bounded daemon
lifecycle. Provider code owns official request parsing and typed command
projection. PostgreSQL remains the durable queue and authority source.

The owner resume path is backed by a partial index on active `executor_owner`
values. The index excludes lease expiry, so periodic heartbeats do not rewrite
its key. Fresh recovery therefore adds one indexed read, not a second queue or
a per-heartbeat index update.

## Directory Boundary

Submit code now has one explicit module directory:

```text
crates/image-gateway/src/provider_tasks/submit/
  daemon.rs
  driver.rs
  mod.rs
  orchestrator.rs
  service.rs
```

- `driver.rs` defines the provider dispatch contract;
- `orchestrator.rs` owns the durable journal and one remote side effect;
- `service.rs` owns database scheduling, projection, and lease heartbeats; and
- `daemon.rs` owns lanes, pacing, error backoff, and shutdown drain.

The lower-level gated process remains under `provider_tasks/remote_submit/`.
Provider-specific request fields remain under `providers/`.

## Scheduling Order

Each service iteration performs exactly one durable unit in this order:

1. resolve one due submit deadline;
2. claim one due recovery for the configured provider/account;
3. in one owner-scoped read, resume fresh work already running or holding a
   still-valid pre-launch lease;
4. claim and start one prepared executor submission; or
5. return idle.

Deadline and recovery work therefore cannot be bypassed by a continuously
non-empty fresh queue. There is no in-memory job queue, broker, global registry,
or provider-specific branch in the scheduler.

## Retry Identity

Each daemon lane owns a tuple:

```text
(process owner prefix, lane, sequence)
```

It derives three bounded identities:

- executor owner;
- recovery claim command ID; and
- recovery defer command ID.

An iteration error does not advance the sequence. The next attempt reuses all
three identities. Recovery claim and defer therefore replay the same durable
commands after a lost PostgreSQL response. A fresh executor claim does not add
another command ledger: before any provider side effect, the same owner resumes
its still-valid `leased` row directly. A durable non-idle result advances the
sequence. Idle polling reuses the unused identity and is paced with bounded
jitter.

The process owner prefix must be unique for each process boot. It is not a
durable global sequence. After a process crash, old leases expire under
database time and a new process identity claims recovery normally.

## Heartbeats

Fresh submit owns a live `ExecutorSubmissionLease`. Recovery owns a live
`ProviderSubmitRecoveryLease`. The service uses separate heartbeat methods for
those authorities while the same orchestrator future is running.

Both loops:

- prioritize a completed provider operation over a simultaneous heartbeat;
- use delayed missed-tick behavior rather than burst renewals;
- poll the provider future once more before propagating a renewal error; and
- replace the local lease only after PostgreSQL returns the renewed fence.

Provider deadlines remain inside the unique orchestrator and are derived from
database time. Heartbeats extend ownership; they do not extend the provider
deadline.

## Projection Boundary

`ProviderSubmitProjector<D>` has two explicit entry points:

- fresh projection from `ExecutorSubmissionLease` plus
  `ExecutorLaunchContext`; and
- recovery projection from `ProviderSubmitRecoveryLease`.

A fresh projection failure is confirmed to have no remote effect. The service
records a deterministic failed executor outcome before any provider driver is
called. A recovery projection failure cannot prove what happened remotely, so
the service defers the same fenced recovery instead of terminalizing it.

`DreaminaCliSubmitCodecV1` now implements this projector contract. It parses
the frozen `dreamina-cli.submit.v1` JSON into `DreaminaSubmitRequestV1`, creates
`DreaminaSubmitPayloadV1`, and returns a typed `SingleOutputCommand`. This is
compiled but inactive.

## Daemon Lifecycle

`ProviderSubmitDaemon` provides:

- 1 to 1,024 bounded lanes;
- a distinct owner and command identity per lane/sequence;
- half-to-full idle jitter;
- full-jitter exponential error backoff;
- fail-closed lane panic detection;
- graceful in-flight drain; and
- bounded abort after the configured drain timeout.

Task capacity remains authoritative in PostgreSQL. Daemon lane count is only a
local upper bound. CLI rate limits, provider quotas, spend limits, and billing
reservations remain separate policies and are not inferred from concurrency.

## Verification

Unit tests prove:

1. invalid daemon bounds fail closed;
2. a transient iteration error reuses the exact same owner and command IDs;
3. the first durable success advances the lane sequence;
4. lanes enforce the configured in-flight ceiling;
5. shutdown drains completed work;
6. drain timeout aborts and drops a pending iteration; and
7. a lane panic fails closed and stops the daemon.

Real PostgreSQL tests prove:

1. fresh prepared work is claimed, started, projected, submitted, and attached;
2. an expired recovery is processed before simultaneously available fresh
   work;
3. projection failure records executor/submission failure with zero provider
   calls;
4. a provider operation outliving its initial executor lease is heartbeated,
   invoked exactly once, and durably becomes `outcome_unknown`;
5. a fresh claim whose response is lost resumes the exact lease epoch and
   submits without waiting for lease expiry or claiming a second epoch; and
6. expired pre-launch leases are not resumable, while the active owner lookup
   remains a valid partial index that does not index heartbeat expiry.

Dreamina unit tests prove strict frozen JSON projection and rejection of
invalid source or output identity.

## Evidence Basis

PostgreSQL documents `SKIP LOCKED` for queue-like access by multiple consumers:
<https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE>.

PostgreSQL documents statement time separately from host application clocks:
<https://www.postgresql.org/docs/18/functions-datetime.html>.

Tokio documents interval missed-tick behavior and cancellation-safe ticks:
<https://docs.rs/tokio/latest/tokio/time/struct.Interval.html>.

These sources justify the selected primitives. They do not prove SOTA,
throughput, fairness under every workload, or production readiness.

## Remaining Gates

The next independent phase must close these items before activating a submit
process:

1. compose an activation-gated `provider-submitd` around the Phase 2AA
   workspace boundary;
2. add real process restart and SIGTERM tests for the composed binary; and
3. benchmark mixed recovery/fresh workloads, database lock wait, allocations,
   journal fsync cost, and p50/p95/p99 scheduling latency.
