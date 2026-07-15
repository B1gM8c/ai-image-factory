# Phase 2F: Provider Submit Deadline Quarantine

Status: implemented and PostgreSQL-tested. A submit whose remote effect remains
unknown at its frozen provider deadline now reaches a customer-terminal uncertain
projection without releasing provider capacity. Remote provider activation still
requires capacity reconciliation evidence, recoverable artifact materialization,
operation descriptor identity, and the single provider orchestrator.

## Scope

This phase closes the local deadline transition for a remote submit:

1. a provider/account-scoped resolver selects one due submit using database time;
2. one short transaction records a canonical deadline decision, closes submit
   recovery, and terminalizes the customer projection as `uncertain`;
3. the executor capacity allocation remains `held` and continues to count against
   the account policy;
4. a late trusted receipt can append its operation identity exactly once without
   reopening attach or changing the customer result.

It does not call a provider, retry submit, create a normal polling task after the
deadline, or infer that timeout proves the remote side effect did not happen.

## Evidence Behind the Design

- PostgreSQL documents `SKIP LOCKED` for queue-like tables with multiple
  consumers. The resolver uses it only for bounded provider/account work
  selection, with deterministic deadline and submission ordering:
  <https://www.postgresql.org/docs/18/sql-select.html>
- PostgreSQL recommends acquiring multiple objects in a consistent order to
  avoid deadlocks. Deadline resolution follows the existing parent-before-submit
  order and never holds a transaction over external I/O:
  <https://www.postgresql.org/docs/18/explicit-locking.html>
- A multicolumn B-tree is most effective from leading equality columns through
  the first range column. Deadline selection therefore has its own
  `(account, deadline, submission)` partial index instead of assuming the
  earlier effective-recovery-due column can be skipped. Provider account IDs
  are globally unique, so the index omits the redundant provider text key:
  <https://www.postgresql.org/docs/18/indexes-multicolumn.html>
- AWS's idempotency guidance treats a timeout after a resource-creating request
  as an ambiguous outcome unless an explicit idempotency contract proves safe
  retry semantics. CLI adapters do not inherit that guarantee automatically:
  <https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/>

These sources validate the selected primitives. They do not establish a universal
throughput or SOTA claim; production latency, WAL, and fairness still require a
committed mixed-load benchmark at the intended cardinality and concurrency.

## State And Evidence

```text
sending | outcome_unknown | operation_known
    -- database_now >= provider_deadline_at_ms -->
deadline_quarantined

executor/submission: uncertain
decision source:      remote_submit_deadline
decision error:       provider_submit_deadline
submit recovery:      closed
capacity allocation:  held
remote task:          absent
```

`deadline_quarantined` is terminal for customer and attach semantics, but its
receipt fields remain a write-once evidence surface. An exact late receipt replay
returns the persisted row; a different operation, request, event, submit identity,
or launch fence conflicts. Receipt arrival never transitions back to
`operation_known` and never creates a normal provider task.

No new capacity state or duplicate quarantine column is added. The canonical
decision, submit state, and deferred projection already prove the relationship,
while `state = 'held'` preserves the existing invariant:

```text
resource_policy.allocated_count == count(held capacity allocations)
```

A dedicated database trigger prevents this deadline decision from being reused
as capacity-release evidence. Future reconciliation must append separate strong
evidence that the remote operation is terminal or did not occur; it must not
rewrite the customer's canonical `uncertain` decision. That future migration
must change the release evidence/FK, the hold guard, and the deferred submit
projection together, including acceptance of receipts that arrive after a proven
capacity release. Adding only a release reason would remain fail-closed.

## Resolver And Locking

`resolve_due_submit_deadline(scope)` handles one row per transaction. The caller
rotates exact `(provider_id, provider_account_id)` scopes, preserving the same
fairness ownership boundary as submit recovery and remote polling.

The candidate query uses database `statement_timestamp()`, the deadline partial
index, deterministic ordering, and `FOR UPDATE ... SKIP LOCKED`. It locks the
parent execution, submission, and capacity rows before loading the submit intent
and recovery row:

```text
executor execution + provider submission + capacity allocation
-> submit intent
-> submit recovery
-> resolution decision
-> terminal projections
```

Every predicate is rechecked after the locks. An attach, receipt, heartbeat,
defer, or rejection transaction can win before the resolver, but no stale
snapshot is terminalized. A live recovery lease cannot cross the absolute
deadline because migration 0020 already constrains its expiry to that deadline.

The resolver is intentionally not another leased worker action. It performs no
provider inspection or other external I/O, so a second owner/epoch/heartbeat
would add state and failure modes without fencing an external side effect.

## Cost Model

The deadline queue adds one narrow partial B-tree entry per active recovery. Its
account, deadline, and submission fields are immutable during recovery heartbeats;
the existing effective-due index already makes those heartbeat updates non-HOT.
Resolution removes the row from both partial indexes by changing recovery to
`closed`.

The hot path remains one submit/recovery write transaction. No capacity row or
counter is changed at deadline, so quarantine creates no account oversubscription
window and no capacity-counter write. Empty scoped resolver calls are index
lookups and do not mutate rows.

## Verification

Real PostgreSQL 18.3 tests cover:

- fresh and concurrent migrations through version 21;
- a `20 -> 21` migration containing an already due active recovery;
- a concurrent `20 -> 21` lock probe proving the migration waits directly for
  `ACCESS EXCLUSIVE` without first holding `SHARE ROW EXCLUSIVE`;
- 64 concurrent deadline resolvers with exactly one winner;
- resolver-before-receipt and receipt-before-resolver persistence, plus an
  uncontrolled concurrent deadline/receipt race that converges;
- deadline versus attach and expired recovery heartbeat without deadlock;
- exact late receipt and ambiguity replay, with conflicting payload rejection;
- terminal execution/submission uncertainty, closed recovery, one canonical
  decision/reduction, no remote task, held capacity, and unchanged allocation
  count;
- raw SQL inability to release capacity using the deadline decision;
- existing submit, attach, poll, callback, cancellation, rejection, recovery,
  heartbeat, and migration regressions.

A PostgreSQL 18 structural `EXPLAIN` test disables sequential scans and requires
`provider_submit_recoveries_deadline_idx` with no candidate `Sort`. This proves
the intended access path remains available; it is not a substitute for the
million-row `EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON)` benchmark.

## Migration Policy

Migration 0021 is transactional and intentionally takes table locks before
replacing mutually dependent constraints/functions and building the deadline
index. It requests the required `ACCESS EXCLUSIVE` locks up front rather than
upgrading a weaker held lock, and uses the same parent-before-submit order as the
runtime transaction graph. It is not an online migration: writes to the affected
provider execution tables can wait for the migration. Remote CLI providers are
still inactive, so the current rollout must run before activation or in a bounded
maintenance window with an operator-defined `lock_timeout`. A future large-table
rollout must split concurrent index creation from the transactional constraint
switch and verify both stages before enabling deadline workers.

## Remaining Activation Gates

1. Add independent capacity reconciliation evidence and a scoped queue for late
   receipts, confirmed no-effect outcomes, and remote terminal evidence. It may
   release capacity exactly once but cannot change the customer decision.
2. Make `artifact_ready -> canonical success` atomic or restart-recoverable.
3. Add the single submit/recovery/deadline/poll/materialization orchestrator. It
   must be the only external side-effect caller.
4. Persist operation descriptor identity and bind provider, operation,
   descriptor, adapter, submission, and idempotency identities.
5. Make recovery claim/defer commands exactly replayable after commit-response
   loss, then run the mixed-load latency, lock, buffer, and WAL benchmark.

Dreamina and other remote CLI providers remain inactive until these gates close.
