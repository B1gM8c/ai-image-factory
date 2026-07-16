# Phase 2U: Active Poll Runtime Profile

Date: 2026-07-16

Status: implemented and verified with unit tests plus real PostgreSQL 18
integration tests. This phase activates no provider, credential resolution,
CLI process, poll service binary, route, billing behavior, or external call.

Follow-up: Phase 2X loads this snapshot into a runnable but inactive
single-profile process:
[`2026-phase2x-inactive-provider-poll-service.md`](2026-phase2x-inactive-provider-poll-service.md).
Phase 2AB moves the snapshot to the shared provider-task boundary and composes
the inactive submit process:
[`2026-phase2ab-inactive-provider-submit-service.md`](2026-phase2ab-inactive-provider-submit-service.md).

## Scope

Phase 2U adds the read-only startup boundary that a provider remote-task
service must cross before constructing a bounded daemon:

```text
profile_key
  -> one PostgreSQL SELECT
  -> enabled profile + pool + account + resource policy
  -> exact immutable identity validation
  -> redacted ProviderRuntimeProfile
  -> claim scope + conservative lane bound
```

The result is an owned value. It holds no database connection, transaction,
provider client, secret value, scheduler, or live-reload handle.

## Runtime Contract

The public store port is provider-neutral:

```rust
pub trait ProviderRuntimeProfileStore {
    fn load_active_runtime_profile(
        &self,
        profile_key: &str,
    ) -> impl Future<
        Output = Result<ProviderRuntimeProfile, ProviderTaskStoreError>
    > + Send;
}
```

`PostgresProviderTaskStore` implements this port with one statement. The query
returns a profile only when all of these conditions hold in the same statement
snapshot:

- `provider_execution_profiles.state = 'enabled'`;
- `provider_credential_pools.state = 'enabled'`;
- `provider_accounts.state = 'enabled'`;
- `executor_resource_policies.state = 'enabled'`; and
- `completion_mode = 'remote_task'`.

Every join also rechecks the durable foreign-key identity:

```text
provider
credential pool
provider account
credential ref + revision
resource policy + revision
```

An absent, disabled, inline, or no-longer-matching binding is not runnable.
Malformed durable identity fails closed as a conflict rather than producing a
partially initialized daemon.

## Frozen Identity

`ProviderRuntimeProfile` owns the values needed by submit and poll
composition:

- execution profile ID and key;
- provider and command schema;
- operation ID, descriptor revision, and descriptor digest;
- completion and idempotency mode;
- adapter revision;
- credential pool and provider account IDs;
- credential reference, revision, and authentication digest;
- resource policy ID and revision;
- provider/account claim scope; and
- derived maximum in-flight remote-task count.

The wrapped `ExecutorExecutionProfile` is private and is never returned by
reference. Construction validates non-nil UUIDs, bounded identifiers,
lower-case SHA-256 values, positive revisions, remote-task completion mode, and
the daemon lane safety bound.

The profile has a custom `Debug` implementation. It redacts both
`credential_ref` and `credential_auth_sha256`; deriving `Debug` from the inner
execution profile would leak those values into generic startup errors or
diagnostics.

The credential reference and authentication digest remain available through
explicit getters because a later provider-specific composition layer must bind
the selected account to its credential broker result. They are not secret
material themselves, but they are treated as sensitive identity data.

## Snapshot Semantics

The active-state and identity checks are one `SELECT`, not a chain of reads.
Under PostgreSQL Read Committed, the statement sees one database snapshot from
the instant the query begins. This prevents a profile from being assembled
from independently observed revisions.

This is a startup eligibility snapshot, not a permanent authorization lease.
Disabling a dependency after the value has been loaded does not mutate the
owned snapshot or rewrite the identity of already attached remote tasks.

A future service process must define its own restart or shutdown policy for
configuration revocation. Phase 2U deliberately avoids a polling refresh loop:

- hot-path provider polls perform no profile query;
- no process-local mutable configuration is introduced;
- no distributed invalidation protocol is implied; and
- in-flight durable task identity remains the authority for replay.

## Capacity Derivation

The profile derives:

```text
max_in_flight = executor_resource_policies.max_concurrency
```

This is a conservative upper bound, not a provider rate limit. Every attached
remote task already owns one durable held capacity allocation, and each task
can hold only one fenced poll lease. Therefore the total number of concurrently
pollable tasks for the account cannot legitimately exceed the account's active
execution capacity.

The shared runtime boundary enforces `MAX_PROVIDER_RUNTIME_LANES = 1024`.
Profiles above that limit fail before daemon construction. This makes an
unexpectedly large durable policy an explicit deployment-design decision
rather than an unbounded task allocation.

The following policies remain independent and are not inferred from execution
capacity:

- provider query requests per second;
- provider cooldown and retry-after behavior;
- circuit-breaker state;
- account spend or quota;
- artifact streaming concurrency; and
- cross-account scheduling weights.

Conflating these controls would hide different failure domains behind one
number. They require separate durable or measured contracts when an activated
provider proves the need.

## Cost Model

One successful load performs:

- one indexed lookup by unique `profile_key`;
- three identity joins over primary or unique keys;
- no row lock;
- no explicit transaction;
- one owned runtime value containing the returned strings; and
- no database profile work on the submit or poll hot path.

Runtime profile access is ordinary immutable field access. Claim scope creation
clones only the provider identifier because the existing claim API owns its
scope.

These are structural cost bounds, not benchmark results. Phase 2U does not
claim SOTA or performance leadership. Production proof still requires query
plans and p50/p95/p99 startup, claim, provider, lock-wait, CPU, memory, and WAL
measurements under representative cardinality and failure.

## Verification

Unit tests prove:

- a valid remote-task profile freezes the exact claim scope and lane count;
- `Debug` does not contain the credential reference or authentication digest;
- inline profiles fail before daemon construction;
- profiles above the daemon safety limit fail closed; and
- malformed frozen digest identity is rejected.

Real PostgreSQL 18 tests prove:

- the seeded durable profile loads every expected immutable identity value;
- `max_concurrency = 100` derives `max_in_flight = 100`;
- the provider/account claim scope is exact;
- the debug representation remains redacted after database loading;
- disabling the execution profile rejects the load;
- disabling the credential pool rejects the load;
- disabling the provider account rejects the load;
- disabling the resource policy rejects the load; and
- re-enabling each dependency restores a valid load.

No paid provider, CLI, external network, production credential, or production
artifact root is used.

## Evidence Basis

PostgreSQL 18 documents that a Read Committed `SELECT` sees one snapshot of
committed data as of the instant the query begins:
<https://www.postgresql.org/docs/18/transaction-iso.html#XACT-READ-COMMITTED>.

PostgreSQL 18 documents the `SELECT` processing model and explicit row-locking
clauses. The Phase 2U load does not request a row lock:
<https://www.postgresql.org/docs/18/sql-select.html>.

These official semantics support the one-statement eligibility snapshot. They
do not prove that this repository is industry-leading.

## Explicit Limits

Phase 2U does not provide:

- a runnable provider poll service or environment contract;
- credential secret resolution or rotation;
- live profile revocation propagation;
- provider query QPS or distributed rate limiting;
- circuit breaking, quota probing, or account rotation;
- provider-specific poll request or response codecs;
- process-group cancellation proof for a query CLI;
- image or video media conformance for Dreamina, Seedance, or Grok;
- production metrics, alert thresholds, or benchmark evidence; or
- paid-provider activation.

## Follow-up

Phase 2V implements and locally verifies the first provider-specific,
image-only poll driver without activating Dreamina:
[`2026-phase2v-inactive-dreamina-image-poll-driver.md`](2026-phase2v-inactive-dreamina-image-poll-driver.md).

Phase 2W binds that driver to an exclusive descriptor-relative attempt
workspace with startup crash recovery:
[`2026-phase2w-exclusive-cli-attempt-workspace.md`](2026-phase2w-exclusive-cli-attempt-workspace.md).

Phase 2X closes inactive service composition with a deployment-injected
account-home capability, digest-pinned driver, Phase 2T daemon, and real
PostgreSQL fake-CLI proof. A production credential broker, revocation policy,
health contract, and provider activation remain separate gates.
