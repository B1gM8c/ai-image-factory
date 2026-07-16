# Phase 2AI: Runtime Profile PostgreSQL Ownership

Date: 2026-07-16

Status: implemented without changing SQL, migrations, public APIs, runtime
configuration, or provider activation.

## Finding

`provider_tasks/postgres.rs` owned both the durable remote-task state machine
and the independent runtime-profile query adapter. The adapter depended only
on:

- `PostgresProviderTaskStore`;
- `ProviderRuntimeProfileStore`;
- one row projection;
- the profile-key validator; and
- one SQL error mapping.

It did not share a transaction, row type, or state transition with submit,
poll, cancellation, callback, or artifact resolution. Keeping it in the
5,197-line task-state file obscured that boundary without reducing coupling.

## Decision

The PostgreSQL adapter now lives beside its consumer-owned port:

```text
provider_tasks/
  runtime_profile.rs
  runtime_profile/
    postgres.rs
  postgres.rs
```

`runtime_profile.rs` continues to own the public port and validated domain
object. `runtime_profile/postgres.rs` owns the PostgreSQL row projection and
the sole production implementation. The shared `PostgresProviderTaskStore`
remains the connection-pool holder.

The move reduces `provider_tasks/postgres.rs` from 5,197 to 5,096 lines. That
line count is evidence of physical extraction only; it is not a maintainability
or performance metric.

## Preserved Contract

The migration preserves:

- every selected column and join predicate;
- all enabled-state and `remote_task` filters;
- one bound `profile_key` parameter;
- `NotFound` for an absent active profile;
- `Conflict` for an invalid projected domain object;
- `Unavailable` for SQL errors; and
- the existing static `ProviderRuntimeProfileStore` dispatch.

The moved adapter reuses the domain module's profile-key validator. Its accepted
alphabet and 128-byte bound are identical to the removed local call.

No trait object, boxed future, allocation, query, transaction, lock, network
call, or serialization step was added. Rust module placement is a compile-time
ownership change, so no throughput claim is attached to it.

## Adversarial Boundary

This phase deliberately does not split the remaining task-state file by line
range. Submit, poll, deadline, artifact, and terminal-resolution helpers share
transaction-local invariants and row projections. Moving them before their
compiler-visible ownership is separated would either expose internals or add
delegating layers solely to obtain smaller files.

The runtime-profile adapter was selected first because its dependency graph was
already independent and its port already existed. Later extractions must meet
the same test: one coherent capability owner, one authoritative SQL
implementation, and no behavior change hidden inside a directory move.

## Verification

Verification completed on 2026-07-16:

- the three runtime-profile domain tests passed;
- active profile projection and disabled-dependency rejection passed against
  real PostgreSQL;
- all 81 real PostgreSQL provider-task tests passed serially;
- the gateway library passed 239 tests;
- API integration passed 58 tests;
- executor integration passed 46 tests;
- migration integration passed 10 tests;
- process integration passed 7 tests;
- the full workspace passed with serial database tests;
- Clippy passed for the full workspace and all targets with warnings denied;
  and
- the moved SQL block's SHA-256 matched the pre-move block exactly.

The real Codex image-generation smoke remains intentionally uninvoked because
it consumes external quota.

## Explicit Limits

This phase does not:

- split the submit or poll state machine;
- remove the compatibility `ProviderTaskStore`;
- change profile provisioning or desired state;
- add credential discovery or rotation;
- activate Dreamina, Grok, Seedance, or another provider; or
- claim SOTA, latency, throughput, or production-readiness improvement.
