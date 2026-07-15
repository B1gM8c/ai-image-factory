# Phase 2G: Provider Capacity Reconciliation

Status: implemented and PostgreSQL-tested. Deadline-quarantined remote submits
now have an independent, provider/account-scoped reconciliation queue. Capacity
can be released exactly once from strong evidence without changing the canonical
customer result. Remote providers remain inactive until the remaining activation
gates close.

## Scope

This phase adds one narrow durable boundary:

1. migration 0022 creates `provider_capacity_reconciliations` and backfills
   existing `deadline_quarantined` submits;
2. an exact `(provider_id, provider_account_id)` worker claims one due row with
   a database lease and frozen provider execution context;
3. a worker may heartbeat, defer, or record one of two strong evidence forms;
4. evidence recording and capacity release happen in one transaction;
5. late submit receipts remain append-only before or after capacity release.

It does not call a provider, retry a submit, alter billing, reopen a customer
task, materialize artifacts, or schedule work across accounts.

## Evidence Policy

Only these outcomes can release capacity:

- `confirmed_no_effect`: a monotonic provider or idempotency authority proves
  that the operation did not occur;
- `remote_terminal`: an operation already owned by the write-once submit receipt
  is in `succeeded`, `failed`, or `canceled` state.

A timeout, expired lease, transient `not found`, transport error, ambiguous
response, or local process exit is not release evidence. The customer decision
remains:

```text
intent:               deadline_quarantined
execution/submission: uncertain
decision source:      remote_submit_deadline
decision error:       provider_submit_deadline
```

The reconciliation row is separate evidence authority. It never rewrites that
decision.

## Durable Model

One row serves two related states instead of introducing a queue plus a second
evidence ledger:

```text
active
  available_at_ms = next due time or current lease expiry
  owner + epoch + claimed_evidence_revision fence external inspection

released
  owner + epoch identify the committing lease
  evidence kind, event identity, optional operation and terminal state are frozen
  payload_hash provides exact replay identity
```

`executor_capacity_allocations.release_reconciliation_id` independently binds
the release to this evidence. The old `release_decision_id` still points to the
canonical deadline decision, but that decision alone cannot release capacity.

The invariant remains:

```text
resource_policy.allocated_count == count(capacity allocation where state = held)
```

The reconciliation update, allocation release, and policy counter decrement are
one transaction. Exact release replay returns the frozen row and does not
decrement twice; different evidence conflicts.

## Claims, Wakeups, And Replay

Claim and defer accept caller-generated command identities. An exact claim retry
returns the same epoch, including after that lease has expired; it cannot silently
turn an acknowledgement retry into a new authority grant. An exact defer retry
returns success while its command remains the latest state transition.

`evidence_revision` is the receipt generation. Claim freezes it as
`claimed_evidence_revision`. A late receipt increments the generation:

- if the row is unowned, it is made immediately due;
- if the row is owned, its lease expiry is retained, but heartbeat and evidence
  commit fail the old generation;
- defer observes the mismatch and preserves immediate due time instead of
  pushing new evidence into the future.

This closes the race where a worker inspects an old view while a receipt arrives.
`remote_terminal` cannot establish a new operation identity and therefore cannot
bypass the submit-intent operation uniqueness index. After a
`confirmed_no_effect` release, a receipt remains appendable as contradictory
audit evidence and never re-holds capacity.

## Lock Order

No transaction holds database locks across provider I/O. Runtime paths use:

```text
claim / heartbeat / defer:
  capacity allocation -> capacity reconciliation

receipt:
  execution + submission + capacity allocation
  -> submit intent -> capacity reconciliation wake

evidence release:
  execution + submission + capacity allocation
  -> submit intent -> capacity reconciliation
  -> resource policy counter
```

PostgreSQL recommends consistent lock order for deadlock avoidance. Queue claims
use `SKIP LOCKED` only as a contention primitive, not as a fairness claim:
<https://www.postgresql.org/docs/18/explicit-locking.html>
<https://www.postgresql.org/docs/18/sql-select.html>

The caller owns cross-account fairness by rotating exact scopes and limiting each
scope to one claim per round.

## Index And Cost Model

The partial claim index is:

```text
(provider_account_id, available_at_ms, provider_deadline_at_ms, submission_id)
WHERE state = 'active'
```

The leading account equality followed by due-time range follows PostgreSQL's
multicolumn B-tree guidance. Provider account UUIDs are globally unique, so
duplicating provider text in the index adds width without narrowing the scan:
<https://www.postgresql.org/docs/18/indexes-multicolumn.html>
<https://www.postgresql.org/docs/current/indexes-partial.html>

Claim, heartbeat, and defer change indexed `available_at_ms`, so those writes are
non-HOT. A receipt wake can remain HOT when its due value does not change. Each
successful action is bounded to one reconciliation and one capacity row; release
also updates one policy row. Empty exact-scope claims are index lookups and do
not mutate state.

The claim first materializes at most 64 ordered queue candidates from the partial
index, then applies allocation `SKIP LOCKED` inside that fixed window. This avoids
a backlog-sized join and external sort. If all 64 allocations are temporarily
locked, the call returns no row and the scope scheduler retries in a later round;
one call never scans an unbounded locked prefix.

The current implementation favors inspectable application transactions over a
stored procedure. Moving the transaction into PostgreSQL would reduce round
trips but increase database coupling; that change requires measured p95/p99 and
WAL evidence first.

Active claim command identities have a narrow unique partial index scoped by
provider, account, owner, and command ID, so unrelated accounts cannot collide.
The original claim timestamp, expiry, and evidence generation are persisted as a
minimal response snapshot; an exact acknowledgement replay waits through a short
allocation lock and returns that snapshot rather than minting a new epoch. Defer
replay already addresses one known submission and uses its unique key.

## Migration Policy

Migration 0022 is a transactional maintenance-window migration. It acquires the
affected table locks up front, creates the queue and evidence constraints,
backfills existing quarantines, and switches the mutually dependent allocation
and projection guards atomically.

This is a strict schema-before-binary rollout: gateway and worker processes must
be drained before migration, and a Phase 2G binary must not start until migration
verification succeeds. Running the migration concurrently with capacity claims
is unsupported because table-level DDL locks and row-level runtime locks have
different graphs. An operator must set a bounded `lock_timeout` and abort the
rollout if the drain is incomplete; retrying after the blocker is removed is
safe because the migration is transactional.

The claim index is built inside that transaction because remote providers are
not active. Before a large production table exists, rollout must either remain a
bounded maintenance operation or split concurrent index creation into a staged,
verified deployment. PostgreSQL documents the different locking and failure
semantics of concurrent index builds:
<https://www.postgresql.org/docs/18/sql-createindex.html>

## Verification

Real PostgreSQL 18.3 tests cover:

- fresh, repeated, and concurrent migrations through version 22;
- a `21 -> 22` backfill containing a real deadline-quarantined held allocation;
- exact provider/account scope and 64 concurrent claimers with one winner;
- structural use of the partial claim index with no candidate `Sort`;
- frozen execution context, heartbeat fencing, expiry reclaim, and stale epoch
  rejection;
- exact claim, defer, and release acknowledgement replay;
- receipt generation fencing so new evidence cannot be deferred away;
- raw intent-only receipt rejection and exact claim replay through a concurrent
  allocation lock;
- `confirmed_no_effect` and `remote_terminal` release, operation conflict, and
  raw SQL release rejection;
- receipt-before-release, release-before-receipt, and concurrent receipt/release
  convergence without deadlock;
- unchanged customer uncertainty and exactly one shared policy counter decrement.

An adversarial PostgreSQL 18.3 plan probe used 200,000 all-due rows for one
account. The final bounded query completed in 0.307 ms with 34 shared buffers and
no sequential scan, hash join, sort, or spill. With the ordered first 90
allocations locked, it inspected only the 64-row window, returned empty in
0.477 ms with 389 hit buffers, and did not continue into row 65. Exact command
replay used the scoped command index in 0.024 ms with 7 hit buffers. These are
isolated local plan probes, not production latency claims.

These tests establish correctness and structural boundedness at test cardinality.
They do not establish production throughput or a universal SOTA claim. The
activation benchmark still needs representative cardinality and concurrency with
`EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)`, lock waits, deadlock count, WAL,
and p50/p95/p99 latency.

## Remaining Activation Gates

1. Make `artifact_ready -> canonical success` atomic or restart-recoverable.
2. Add the single submit/recovery/deadline/reconciliation/poll/materialization
   orchestrator. It must be the only external side-effect caller.
3. Persist operation descriptor identity and bind provider, operation,
   descriptor, adapter, submission, and idempotency identities.
4. Make the earlier submit-recovery claim/defer commands exactly replayable, then
   run the mixed-load million-row latency, lock, buffer, and WAL benchmark.

Dreamina and other remote CLI providers remain inactive until these gates close.
