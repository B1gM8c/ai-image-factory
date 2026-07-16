# Phase 2AD: Provider Runtime Readiness Kernel

Date: 2026-07-16

Status: implemented and verified as an inactive PostgreSQL runtime-readiness
kernel. It launches no CLI, reads no provider credential, activates no provider,
and changes no public API.

## Decision

Keep `/healthz` as a dependency-free process liveness signal. Add a separate
durable provider runtime lease model before exposing readiness or composing
provider daemons into it.

This phase adds:

```text
provider execution profile
  -> zero or more submit runtime leases
  -> zero or more poll runtime leases
  -> configured | active | draining | blocked projection
```

The runtime table is operational state, not a billing ledger or provider task
queue. A runtime lease contains no credential reference, executable path,
command, remote operation identifier, or customer data.

## State Contract

Only `remote_task` execution profiles participate. The projection uses one
PostgreSQL statement and the database clock.

State precedence is:

1. `blocked`: the profile, credential pool, account, or resource policy is
   disabled, or the configured concurrency is outside the supported runtime
   lane bound;
2. `active`: at least one non-expired submit lease and one non-expired poll
   lease are active;
3. `draining`: the profile is runnable, is not fully active, and at least one
   non-expired runtime lease is draining;
4. `configured`: the profile graph is runnable but the required submit and poll
   roles are not both active.

An old draining instance does not make a profile draining when replacement
submit and poll instances are already fully active. This avoids reporting a
healthy rolling replacement as unavailable.

An expired process lease is not interpreted as blocked. It returns the profile
to `configured`; a crash is different from an operator-disabled dependency.

## Lease Fencing

The registration identity is:

```text
runtime_id + execution_profile_id + runtime_role + runtime_owner
```

Registration:

1. deletes expired leases only for the exact profile and role;
2. takes shared locks on the enabled profile dependency graph;
3. validates `remote_task` completion and the supported lane bound;
4. inserts an active lease using the PostgreSQL clock; and
5. permits an exact retry of the same `runtime_id` while rejecting a different
   live runtime with the same profile, role, and owner.

Heartbeat updates only time columns and always preserves the database's current
state. A caller holding an old `active` value cannot reactivate a lease after a
concurrent drain transition.

Drain is monotonic:

```text
active -> draining -> withdrawn
```

The database trigger rejects `draining -> active`, identity changes, backward
or future heartbeat movement, and deletion of a live active lease. Clean
withdrawal performs the drain transition and deletion in one transaction when a
separate observable drain period is unnecessary.

Every lease mutation is fenced by its full identity and fails after database
expiry. No host wall clock grants authority.

## Storage And Index Cost

The table has one row per live or not-yet-reaped runtime process, not one row
per provider capacity unit.

The lookup index contains only:

```text
(execution_profile_id, runtime_role)
```

`heartbeat_at_ms`, `lease_expires_at_ms`, and `state` are deliberately absent.
Normal heartbeats therefore update heap tuples without rewriting this B-tree.
Expiry cleanup is scoped by the same low-cardinality profile and role lookup;
there is no global expiration scan on the heartbeat path.

## Why Capacity Slots Were Not Added

Phase 2AC measured the remaining resource-policy counter contention. This phase
reviewed two replacements and rejected an immediate migration:

- one row per capacity slot would require up to one million rows for one policy
  under the existing schema bound and would couple every allocation, release,
  reconciliation, and migration to slot ownership;
- fixed counter stripes would require an unproven stripe count and add a second
  durable allocation identity before production-scale contention evidence
  exists.

The measured atomic counter remains the lower total-cost design for the current
activation boundary. This is a deferral based on storage and correctness cost,
not a claim that the counter scales without limit.

## Evidence Basis

Kubernetes distinguishes liveness, which determines restart, from readiness,
which determines whether a process receives traffic:

- <https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/>

Kubernetes marks terminating endpoints not ready while allowing graceful
serving and drain semantics to remain observable:

- <https://kubernetes.io/docs/tutorials/services/pods-and-endpoint-termination-flow/>

PostgreSQL documents compatible shared row locks, conflicting administrative
updates, and consistent lock ordering as the primary deadlock defense:

- <https://www.postgresql.org/docs/18/explicit-locking.html>

These sources justify the lifecycle split and locking primitives. They do not
prove production availability or industry leadership.

## Verification

This phase proves with PostgreSQL 18:

- configured, active, draining, and blocked projections;
- exact registration retry;
- one winner for concurrent duplicate-owner registration;
- stale heartbeat rejection after database expiry;
- future heartbeat rejection even when a writer bypasses the Rust store;
- registration after expired-row cleanup;
- disabled dependency rejection;
- monotonic drain under an old active heartbeat;
- migration `0 -> 29`;
- concurrent fresh migration repeatability; and
- schema/index presence.

## Explicit Limits

This phase does not provide:

- provider daemon registration or periodic heartbeats;
- `/readyz` or an admin runtime status endpoint;
- a desired activation state or rollout controller;
- provider query rate limiting, cooldown, circuit breaking, or account
  rotation;
- credential resolution or rotation;
- production metrics and alert thresholds; or
- Dreamina, Grok, Seedance, or any paid-provider activation.

## Next Gate

The next phase should compose submit and poll daemons with the lease store:

1. register only after all local and database configuration checks pass;
2. renew with a bounded interval shorter than the lease;
3. make heartbeat loss initiate daemon shutdown;
4. publish `draining` before graceful shutdown;
5. withdraw only after all lanes finish; and
6. expose a cheap `/readyz` database check plus aggregate profile state without
   leaking account or credential identity.
