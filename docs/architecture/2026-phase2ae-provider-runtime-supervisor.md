# Phase 2AE: Provider Runtime Supervisor

Date: 2026-07-16

Status: implemented and verified for the inactive `provider-submitd` and
`provider-pollerd` compositions. No paid provider was called, no provider was
activated, and no public API contract changed.

## Decision

Compose the Phase 2AD database lease outside the submit and poll daemon lane
implementations.

```text
validated process composition
  -> register process runtime lease
  -> run existing bounded daemon
  -> renew one process lease
  -> draining before daemon shutdown notification
  -> continue renewal while lanes drain
  -> withdraw after lane completion
```

`ProviderRuntimeSupervisor` is provider-neutral. It knows only:

- one `ProviderRuntimeReadinessStore`;
- one profile, role, runtime ID, and owner registration;
- one process lease duration;
- one heartbeat interval;
- one external shutdown future; and
- one runtime future that accepts a shutdown signal.

It does not know provider commands, credentials, CLIs, task claims, artifacts,
lane counts, billing, or API facades.

## Why The Supervisor Is Outside Daemon Lanes

Putting process heartbeats in every lane would multiply writes by configured
concurrency and make process readiness depend on task throughput. Putting
database logic directly in both daemon implementations would duplicate lifecycle
code and couple provider scheduling to PostgreSQL.

The supervisor adds one timer and one latest-value `watch` signal per process.
It does not spawn a heartbeat task, allocate an in-memory queue, or add a mutex
to the provider execution path. The existing daemon remains the sole owner of
lane creation, pacing, task heartbeats, and bounded drain.

Tokio documents `watch` as suitable for signalling program state changes such
as shutdown:

- <https://docs.rs/tokio/latest/tokio/sync/watch/>

Heartbeat intervals use `MissedTickBehavior::Delay`, so executor stalls do not
create a burst of catch-up database writes:

- <https://docs.rs/tokio/latest/tokio/time/enum.MissedTickBehavior.html>

These primitives support the implementation choice; they do not by themselves
prove availability or industry leadership.

Every runtime-lease database operation is bounded by one heartbeat interval.
Heartbeat renewal still listens for external shutdown and daemon completion
while the SQL future is in flight. A pool stall therefore cannot create an
unbounded hidden heartbeat wait; timeout is treated as `Unavailable`.

## Startup Contract

Both binaries complete all existing checks before the supervisor is run:

1. exact activation token;
2. migrated database schema;
3. active frozen execution profile;
4. account capability identity;
5. private canonical roots;
6. executable and runner digests;
7. adapter and daemon configuration; and
8. bounded runtime lease configuration.

Registration is the final gate before the daemon future is constructed.
Consequently, the `started` log is emitted only after the database granted the
runtime lease.

The runtime owner reuses the existing process owner identity:

- submit role: `PROVIDER_SUBMITTER_OWNER_PREFIX`;
- poll role: `PROVIDER_POLLER_OWNER`.

Each process still generates a new random `runtime_id`. A live duplicate owner
is fenced by PostgreSQL, while an expired owner can be reclaimed.

## Shutdown Contract

For SIGINT or SIGTERM:

1. transition the runtime lease from `active` to `draining`;
2. notify the existing daemon shutdown future;
3. stop new iterations;
4. continue renewing the draining process lease;
5. wait for the daemon's existing bounded lane drain; and
6. withdraw the process lease.

Kubernetes documents the same separation between readiness and graceful
termination: terminating endpoints become not ready while in-flight work may
still complete.

- <https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/>

The database transition is awaited before the daemon sees shutdown. This makes
the externally observed state order deterministic under normal database
operation.

## Heartbeat-Loss Contract

Any process heartbeat error is fail closed:

1. attempt a monotonic transition to `draining`;
2. notify the daemon immediately;
3. let the daemon perform its bounded lane drain;
4. attempt withdrawal; and
5. return a service-unavailable error even if a later cleanup call succeeds.

A stale caller cannot recover authority by continuing work. If PostgreSQL is
unavailable or the lease already expired, the row remains non-authoritative and
expires or is reaped during the next registration.

Runtime lease parameters are independent from task lease parameters:

| Process | Lease variable | Heartbeat variable | Defaults |
|---|---|---|---:|
| submit | `PROVIDER_SUBMITTER_RUNTIME_LEASE_MS` | `PROVIDER_SUBMITTER_RUNTIME_HEARTBEAT_INTERVAL_MS` | `60000 / 10000` |
| poll | `PROVIDER_POLLER_RUNTIME_LEASE_MS` | `PROVIDER_POLLER_RUNTIME_HEARTBEAT_INTERVAL_MS` | `60000 / 10000` |

Every value is bounded to one millisecond through 24 hours. Three heartbeat
intervals must fit inside the lease.

## Error Precedence

The supervisor preserves the most actionable failure:

1. a daemon error is returned when lane execution or bounded drain fails;
2. otherwise heartbeat or drain authority loss is returned;
3. otherwise withdrawal failure is returned; and
4. only complete daemon drain plus successful withdrawal returns success.

Secondary cleanup remains best effort. An active lease is never deleted through
an unfenced shortcut.

## Verification

Unit tests prove:

- invalid lease configuration fails before registration;
- external shutdown publishes drain before runtime notification;
- heartbeats continue while the runtime is draining;
- heartbeat loss notifies and stops the runtime; and
- withdrawal follows completed drain.

Real PostgreSQL 18 tests prove:

- a forced lease expiry returns `StaleLease`;
- heartbeat authority loss stops the supervised runtime;
- the expired owner can be cleaned and registered by a replacement;
- the real poller process registers and periodically renews its lease;
- the real submitter process registers and periodically renews its lease;
- SIGTERM exposes `draining` before an in-flight fake CLI finishes;
- both binaries withdraw after graceful shutdown; and
- same-owner submitter restart does not resubmit an attached operation.

The process tests use local fake Dreamina executables and isolated test schemas.
They perform no network or paid-provider operation.

## Explicit Limits

This phase does not add:

- `/readyz` or an administrative readiness endpoint;
- a desired-state rollout controller;
- automatic process replacement;
- provider circuit breakers or cooldown policy;
- credential rotation;
- metrics export or alert thresholds; or
- Dreamina, Grok, Seedance, or other provider activation.

## Next Gate

Expose readiness without weakening liveness:

1. keep `/healthz` dependency-free;
2. add a cheap database-backed `/readyz`;
3. report aggregate configured/active/draining/blocked profile state;
4. omit account, credential, executable, and filesystem identity;
5. bound query latency and response cardinality; and
6. prove status and timeout behavior through HTTP plus real PostgreSQL tests.
