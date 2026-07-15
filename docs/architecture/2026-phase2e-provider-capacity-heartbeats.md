# Phase 2E: Fenced Provider Capacity Heartbeats

Status: implemented and PostgreSQL-tested. Remote provider activation remains
blocked by deadline resolution, recoverable artifact materialization, operation
descriptor identity, and the single submit orchestrator that drives these store
primitives.

## Scope

This phase closes the durable capacity-heartbeat boundary for remote provider
work:

1. a successful submit-recovery claim or renewal refreshes its held executor
   capacity allocation;
2. a successful remote-task poll claim or renewal does the same;
3. a stale or expired provider fence cannot refresh capacity;
4. a submit-recovery lease cannot extend beyond its frozen absolute provider
   deadline.

It does not choose a heartbeat cadence, start a daemon, call a provider, release
capacity at a deadline, or infer that a stale heartbeat proves a remote side
effect stopped.

## Evidence Behind the Design

- Kubernetes uses a holder identity, duration, and `renewTime` for leases and
  node heartbeats. The provider owner/epoch/expiry remains the authority while
  the capacity timestamp records observed liveness:
  <https://kubernetes.io/docs/concepts/architecture/leases/>
- PostgreSQL recommends acquiring locks on the same objects in a consistent
  order to avoid deadlocks. Recovery paths therefore preserve the existing
  `capacity allocation -> recovery row` order used by submit terminalization:
  <https://www.postgresql.org/docs/18/explicit-locking.html>
- PostgreSQL executes sibling data-modifying CTEs with an unpredictable update
  order. Heartbeat operations therefore use explicit short transactions and do
  not treat a multi-update CTE as lock-order proof:
  <https://www.postgresql.org/docs/18/queries-with.html>
- `statement_timestamp()` is stable for the whole statement. Every lease check,
  expiry, and capacity timestamp in one heartbeat therefore uses one database
  time observation:
  <https://www.postgresql.org/docs/18/functions-datetime.html>

These sources validate the primitives, not a universal throughput claim. The
repository still requires committed mixed-load benchmarks before production
cadence or capacity sizing is declared optimal.

## Authority Model

`executor_capacity_allocations.last_heartbeat_at_ms` is liveness evidence. It is
not a lease and cannot independently authorize work or capacity release.

The authoritative fences remain:

- submit recovery: recovery owner, epoch, and expiry;
- remote poll/materialization: poll owner, epoch, and expiry;
- capacity: one durable allocation whose state is `held`.

A heartbeat succeeds only when both the provider fence and its exact allocation
are live. The store returns the renewed lease only after both updates commit. A
failed fence check rolls the transaction back, so no candidate or stale fence
performs a capacity update.

Capacity release still requires durable terminal provider evidence and a
canonical resolution decision. A stale capacity timestamp is never interpreted
as rejection, cancellation, or proof that the remote operation is absent.

## Lock Order

Submit terminalization already locks the capacity allocation before the recovery
row. Recovery claim and renewal preserve that order:

```text
capacity allocation -> recovery row -> capacity heartbeat update
```

The capacity candidate is locked with `SKIP LOCKED`; a busy allocation does not
stall unrelated provider/account work. The recovery row is then fenced and its
capacity timestamp is updated before the short transaction commits.

Remote-task terminalization already locks the task before releasing capacity.
Poll claim and renewal preserve its order:

```text
remote task -> capacity heartbeat update
```

No SQL transaction or row lock crosses provider CLI, network, download, hashing,
or object-storage I/O.

## Deadline Boundary

Recovery can inspect evidence only before `provider_deadline_at_ms`:

- claim excludes rows whose deadline is due;
- claim expiry is `min(database_now + lease, provider_deadline_at_ms)`;
- renewal requires a future deadline and is capped by that deadline;
- recovered attach rechecks the future deadline in Rust and a database insert
  trigger, so a once-live recovery epoch cannot attach after time advances;
- migration 0020 adds a database check preventing old binaries or raw SQL from
  persisting a recovery expiry beyond the deadline.

At the deadline, renewal returns `StaleLease`. This does not release capacity or
convert the submit to `rejected`; the next fenced deadline resolver must record
`unknown_remote_effect` and apply an explicit quarantine/capacity policy.

## Cost Model

Each successful provider claim or renewal adds one point update to the exact held
capacity allocation in the same short transaction. Empty claims and stale
renewals do not update capacity. Lookup keys are unique execution/submission
identities; no global capacity scan is introduced.

This changes claim and renewal from one autocommit statement to a bounded
transaction with one additional indexed update. The extra database round trips
are accepted for demonstrated lock-order and rollback semantics; collapsing them
into a stored procedure is deferred until a measured latency budget justifies
moving operation logic into PostgreSQL.

The capacity heartbeat column participates in the held-allocation orphan index,
so successful heartbeats are non-HOT and generate heap, index, and WAL work. This
is intentional until stale-capacity reconciliation policy is defined. The future
orchestrator should renew at a bounded fraction of the lease duration, skip
missed ticks instead of bursting them, and stop external work immediately when
renewal loses the fence.

Splitting heartbeat state into another table would add a join, another durable
identity, and migration complexity without measured evidence that the current
point update is the bottleneck. That optimization remains benchmark-driven.

## Verification

Real PostgreSQL 18 tests cover:

- fresh and concurrent migrations through version 20;
- fail-closed `19 -> 20` migration with a future legacy heartbeat, followed by a
  successful retry after transaction rollback;
- the recovery-expiry database constraint;
- provider/account-scoped concurrent recovery claim;
- capacity advancement on recovery claim and renewal;
- capacity advancement on poll claim and renewal;
- no capacity mutation from an expired poll fence;
- no capacity mutation from callback wakeups and rejection of future capacity
  timestamps;
- recovery claim and renewal capped by the absolute deadline;
- existing attach, callback, observation, cancellation, terminal release, and
  migration invariants.

Structural PostgreSQL 18 `EXPLAIN` checks show recovery selection retaining
`provider_submit_recoveries_claim_idx` with scope, effective-due, and deadline
conditions in `Index Cond` and no candidate `Sort`. Capacity locks and both lease
renewals use index scans; no sequential scan is introduced by this phase.

Before provider activation, add a mixed workload benchmark covering poll claim,
poll renewal, recovery claim, recovery renewal, and terminalization against the
same allocations. Acceptance requires no deadlocks, no sequential capacity scan,
one fence winner, bounded WAL, and measured p95/p99 at the intended account and
worker concurrency.

## Remaining Activation Gates

1. Add the fenced deadline resolver and explicit unknown-effect capacity policy.
2. Make `artifact_ready -> canonical success` atomic or restart-recoverable.
3. Add the single submit/recovery/poll orchestrator. It must be the only external
   side-effect caller and must drive these heartbeat primitives.
4. Persist operation descriptor identity and require the SDK to bind provider,
   operation, descriptor, adapter, submission, and idempotency identities.

Dreamina and other remote CLI providers remain inactive until these gates close.
