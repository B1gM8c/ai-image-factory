# Phase 2D: Frozen Provider Context and Submit Recovery Leases

Status: implemented and PostgreSQL-tested. Phase 2E adds fenced provider-capacity
heartbeats and deadline-bounded recovery renewal. Remote provider activation
remains blocked by the deadline resolver, recoverable artifact materialization,
operation descriptor identity, and the single submit orchestrator.

## Scope

This phase closes two boundaries left open by Phase 2C:

1. submit, recovery, poll, and cancel workers receive the same immutable provider
   execution context;
2. an abandoned or deadline-due submit has one provider/account-scoped recovery
   owner with a monotonic lease epoch.

It does not start a daemon, call a provider, retry a CLI side effect, or decide
the economic policy for an unknown remote effect.

## Evidence Behind the Design

- PostgreSQL documents `FOR UPDATE SKIP LOCKED` as suitable for queue-like
  tables with multiple consumers. Claims use it with a deterministic order and
  `LIMIT 1`: <https://www.postgresql.org/docs/18/sql-select.html>
- The partial expression index repeats the claim predicate and effective-due
  expression exactly. PostgreSQL requires the query predicate to imply a partial
  index predicate: <https://www.postgresql.org/docs/current/indexes-partial.html>
- Deadline creation uses `clock_timestamp()`. Claim statements use the database's
  stable `statement_timestamp()` so the effective-due upper bound is an index
  condition while every comparison and written lease timestamp remains identical
  within the claim statement:
  <https://www.postgresql.org/docs/18/functions-datetime.html>
- The owner, epoch, expiry, and reclaim model follows the same fencing properties
  as Kubernetes Lease records: <https://kubernetes.io/docs/concepts/architecture/leases/>
- A timeout after a side effect does not prove that the side effect did not
  happen. Recovery therefore never retries submit blindly and never maps a local
  deadline to provider rejection:
  <https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/>
- Provider idempotency is treated as an explicit capability, not assumed from a
  customer request key:
  <https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/>

## Frozen Context

`provider_submissions` remains the immutable root for model, command identity,
execution profile, adapter revision, credential revision, and resource policy
revision. `provider_remote_submit_intents` owns the provider idempotency key.
`provider_submit_recoveries` adds only facts that begin at the side-effect
linearization point:

- `invocation_attempt`;
- bounded `provider_timeout_ms`;
- database-time `provider_deadline_at_ms`;
- recovery due time and lease fence.

The store projects `ProviderExecutionContext` by joining those immutable rows and
the immutable provider account identity. It does not duplicate the context into
submit intents or remote tasks. The projection includes:

- model, command schema, and command hash;
- execution profile and adapter revision;
- credential pool, reference, revision, and authentication-file digest;
- resource policy identity and revision;
- provider idempotency key and invocation attempt;
- provider timeout and absolute deadline.

Context fields and the context carried by leases are private and exposed through
read-only accessors. Their `Debug` representation redacts the credential locator
and authentication digest, so a caller cannot mutate a snapshot and renew it as
authoritative or leak credential identity through routine lease logging.

`start_submit` returns this context only after the `reserved -> sending` compare
and-set and recovery-row insert commit together. `claim_due` and
`claim_submit_recovery` return the same context from their claim statement. A
profile, credential, account, or policy later becoming disabled does not rewrite
an in-flight submission.

`admission_sessions.deadline_at_ms` is deliberately not reused. It is the bounded
request-receive deadline and has ended before provider execution begins.
`start_submit` instead accepts an explicit provider timeout, bounds it to 30 days,
and freezes `clock_timestamp() + timeout` in the same transaction as send
authority.

## Recovery State

```text
reserved                       no recovery row, no side effect
  -> sending                   active recovery, submit may have happened
  -> outcome_unknown           active recovery, never blind-retry
  -> operation_known           active recovery, attach the known operation
  -> attached | rejected       closed recovery history
```

The narrow `provider_submit_recoveries` table is separate from the wider evidence
row so heartbeats update fewer bytes and do not turn submit evidence into a hot
tuple. Closed rows remain durable because poll/cancel workers still need the
frozen deadline and invocation identity.

The effective claim time is:

```text
max(next_recovery_at_ms, recovery_lease_expires_at_ms or next_recovery_at_ms)
```

Both the index and claim query use this expression. A crashed oldest claimant is
therefore ordered by its lease expiry on the next pass; it cannot continuously
jump ahead of work that became due while it held the lease.

Claims are scoped by exact `(provider_id, provider_account_id)`. Account fairness
is owned by the caller that rotates scopes; the database query never performs a
global provider scan. Profile or adapter revisions are returned in the frozen
context rather than added to claim scope, so a rollout cannot make old work
unclaimable.

## Authority and Races

Recovery is an authority, not a discovery hint.

- Before executor lease expiry and provider deadline, an unclaimed recovery row
  permits attach with the original executor owner and epoch.
- Once recovery is claimed, attach requires the live recovery owner and epoch.
- An expired executor cannot attach directly.
- A stale recovery epoch cannot attach after reclaim.
- Idempotent attach replay must carry the same recovery fence that was persisted
  on the task; an older epoch cannot receive a successful acknowledgment after a
  newer epoch attached.
- Attach records the recovery fence on the remote task for durable audit, then
  closes the recovery row in the same transaction.
- Late submit receipts may still append the known remote operation under the
  frozen submit identity. They do not release capacity or grant attach authority.
- Confirmed rejection may close an unclaimed or expired recovery lease, or the
  exact live recovery fence may close its own lease. An unrelated claimant cannot
  preempt the live owner.

The lock order for state-changing paths is executor execution and submission,
capacity allocation, submit intent, recovery row, task/evidence, then canonical
resolution. Claim transactions lock only the recovery candidate and commit before
any external provider inspection. No SQL transaction is held across CLI or
network I/O.

## What Recovery May Do

Allowed actions are deliberately narrow:

- inspect durable/local/provider evidence;
- attach an already known remote operation under the recovery fence;
- defer evidence inspection before the absolute deadline;
- later, finalize a deadline as unknown remote effect.

There is no `RetrySubmit` operation. `sending` means the side-effect boundary may
have been crossed even when no receipt is available.

## Migration Policy

Migration 0019 refuses to run when migration 0018 contains any non-reserved
submit intent or remote task. Those rows have no truthful provider deadline to
backfill. Operators must drain them or supply an explicit data-repair migration;
the schema does not infer a deadline from admission, executor lease, or poll time.
The drain check and all DDL run atomically.

## Verification

Real PostgreSQL tests cover:

- fresh and legacy migration convergence through version 19;
- fail-closed migration with legacy attached remote activity;
- one sender and one recovery winner under concurrent calls;
- provider/account scope isolation;
- monotonic recovery heartbeat, defer, and epoch reclaim;
- immutable timeout and absolute deadline under replay and raw SQL;
- exact frozen context equality across submit, recovery, and poll claims;
- old-executor and stale-recovery attach rejection;
- recovered attach and atomic recovery close;
- rejection, ambiguity, late receipt, capacity, and terminal projection behavior.

Structural PostgreSQL 18 `EXPLAIN` checks confirm both recovery and poll use their
named expression indexes, include scope plus effective due time in `Index Cond`,
and require no candidate sort. A million-row, 64-claimant `EXPLAIN (ANALYZE,
BUFFERS, WAL, FORMAT JSON)` benchmark is still required before a production
throughput claim. Target acceptance is no sequential scan or candidate sort, no
deadlocks, one winner per row, and warm cache p95 below 10 ms for recovery and
poll claims.

The expression indexes intentionally make lease heartbeat updates non-HOT. The
recovery row is kept narrow to bound heap and WAL cost; committed mixed-load
benchmarks must measure this tradeoff before tuning heartbeat cadence or splitting
lease state again.

## Remaining Activation Gates

1. Add a fenced deadline resolver. Deadline means `unknown_remote_effect`, never
   `rejected`; capacity release versus quarantined counting needs an explicit
   policy that cannot oversubscribe a provider account.
2. Make `artifact_ready -> canonical success` atomic or restart-recoverable.
3. Add the single submit orchestrator. Only `ProviderSubmitStart::Acquired` may
   spawn a CLI process.
4. Persist operation descriptor identity and add a checked SDK constructor that
   binds submission, provider, idempotency, operation, descriptor, and adapter
   identities before adapters can evolve those contracts independently.

Until these gates close, Dreamina and other remote CLI providers stay inactive.
