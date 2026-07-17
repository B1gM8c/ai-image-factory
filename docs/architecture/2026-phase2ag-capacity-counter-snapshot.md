# Phase 2AG: Capacity Counter Snapshot

Date: 2026-07-16

Status: implemented and verified against PostgreSQL 18 without launching a CLI,
calling a provider, reading a production credential, or changing an API
contract.

## Finding

The Phase 2AC benchmark passed at 512 rows but failed repeatedly when expanded
to:

- 4096 submissions;
- 20 percent due submit recovery;
- 64 concurrent claimants; and
- 32 seed workers.

Three consecutive pre-fix runs failed during a recovery claim. The production
store correctly reduced the database error to the public `Conflict` category.
A temporary local diagnostic build exposed the internal PostgreSQL trigger
message:

```text
executor capacity allocation counter is unbalanced
```

The diagnostic print was removed immediately after the root cause was captured.
It is not part of the committed logging or API surface.

## Root Cause

`enforce_executor_capacity_counter_balance()` was a deferred constraint trigger
on every insert or update of `executor_capacity_allocations`.

It also ran for a heartbeat-only update:

```text
held allocation
  -> update last_heartbeat_at_ms
  -> count every held allocation for the policy
  -> compare with executor_resource_policies.allocated_count
```

The trigger read the policy counter and held allocation count in two successive
`SELECT` commands. PostgreSQL's default Read Committed isolation assigns a new
snapshot to each command. PostgreSQL explicitly documents that two successive
`SELECT` commands in one transaction can observe different concurrent commits:

- <https://www.postgresql.org/docs/18/transaction-iso.html>

A fresh capacity acquisition could commit between those two reads. The trigger
then compared the old side of the invariant with the new side and rejected a
valid recovery heartbeat.

This was not a theoretical anomaly: the 4096-row workload reproduced it three
times. The smaller 1024-row workload could still pass, demonstrating why green
small tests were insufficient evidence.

## Fix

Migration `0031_capacity_counter_snapshot.sql` makes two changes.

First, a pure `held -> held` allocation update returns immediately. The earlier
transition trigger already proves that identity, state, release evidence, and
all non-heartbeat fields remain valid. A heartbeat cannot change the number of
held allocations, so recounting the policy is redundant.

Second, state-changing checks read `allocated_count` and the held row count in
one joined `SELECT`. Both values therefore come from one PostgreSQL command
snapshot.

The authoritative model remains unchanged:

```text
executor_resource_policies.allocated_count
  == count(executor_capacity_allocations where state = held)
```

Fresh acquisition still increments the policy counter and inserts one held
allocation in the same transaction. Release still transitions one allocation
and decrements the policy counter in the same transaction. Direct counter drift
still fails at commit.

## Cost Boundary

Before this phase, every provider submit recovery claim, poll claim, poll
heartbeat, and related capacity heartbeat could count all held allocations for
its policy at commit.

After this phase:

- heartbeat-only writes perform no policy read and no held-allocation count;
- acquisition and release retain the invariant check;
- no retry, mutex, queue, table, index, task, or network hop was added; and
- no weaker isolation level or deferred repair path was introduced.

This removes work from the steady-state heartbeat path. It does not remove the
single policy counter row from fresh acquisition and release.

## Deterministic Regression Test

The real PostgreSQL test:

1. creates a held allocation;
2. holds `ACCESS EXCLUSIVE` on `executor_resource_policies` in another
   transaction;
3. requires a heartbeat-only allocation update to commit within 500
   milliseconds; and
4. separately proves a forged `allocated_count + 1` update still fails.

The table lock makes the old policy read deterministic: the old trigger would
wait, while the new heartbeat path never accesses the policy table.

Migration tests additionally verify:

- migration `0 -> 31`;
- concurrent fresh migration repeatability; and
- the installed function contains the heartbeat fast path and one joined
  capacity projection.

## Repeated 4096-Row Evidence

Two consecutive post-fix runs completed:

| Metric | Run 1 | Run 2 |
| --- | ---: | ---: |
| Acquired | 4096 / 4096 | 4096 / 4096 |
| Recovery / fresh | 819 / 3277 | 819 / 3277 |
| Throughput | 405.3 ops/s | 285.6 ops/s |
| Acquire p99 | 368.3 ms | 381.7 ms |
| Recovery p99 | 83.0 ms | 81.1 ms |
| Fresh p99 | 374.2 ms | 389.6 ms |
| Deadlocks | 0 | 0 |
| Deadline quarantines | 0 | 0 |
| Exact-once projection | true | true |
| Held rows / counter | 4096 / 4096 | 4096 / 4096 |

Raw reports:

- [`../benchmarks/2026-07-16-provider-submit-4096-counter-fix-run1.json`](../benchmarks/2026-07-16-provider-submit-4096-counter-fix-run1.json)
- [`../benchmarks/2026-07-16-provider-submit-4096-counter-fix-run2.json`](../benchmarks/2026-07-16-provider-submit-4096-counter-fix-run2.json)

The runs varied materially in throughput, WAL, and sampled lock waits on the
shared local cluster. They prove the exercised concurrency contract, not a
stable production performance number or an industry-leading result.

## Benchmark Diagnostics

The benchmark now wraps failures with:

- prepared queue seed stage;
- recovery queue seed stage;
- scheduler `ANALYZE` stage;
- measured workload stage;
- fixture kind and row index; and
- claimant, attempt, and claim operation.

This context contains synthetic indices and owner identities only. Database
URLs, credential references, customer data, and raw internal SQL errors remain
absent from the report and public domain errors.

## Residual Bottleneck

Fresh claim p99 remains about 374 to 390 milliseconds at this local 4096-row
load, and lock sampling still shows the shared policy counter row.

The durable policy-slot semaphore proposed in Phase 2AC remains a candidate,
not a conclusion. It would multiply rows by configured capacity and complicate
migration, disable fencing, release repair, and policy resizing. Adopting it
still requires a controlled implementation and measured comparison against the
current counter.

### Rejected Held-Policy Index Experiment

On 2026-07-17, a temporary uncommitted migration tested a narrower alternative:

1. add a partial B-tree index on
   `(resource_policy_id, resource_policy_revision) WHERE state = 'held'`;
2. express the balance count as a correlated subquery in the existing
   single-snapshot statement; and
3. rerun the same 4096-row, 64-claimant workload.

The experiment preserved 4096 / 4096 exact-once projection and zero deadlocks,
but did not produce a material tail-latency improvement:

| Metric | Current counter | Temporary partial index |
| --- | ---: | ---: |
| Throughput | 274.7 ops/s | 293.7 ops/s |
| Fresh p99 | 391.3 ms | 387.6 ms |
| Sampled lock waits | 17,934 | 20,193 |

The throughput delta was smaller than the variance already observed between
the two retained Phase 2AG runs. The index did not distribute ownership of the
single policy counter row, and it added another index write to every allocation
insert and release. The migration and test edits were therefore removed rather
than committed.

PostgreSQL documents that index-only scans are most beneficial when heap pages
are sufficiently old to be marked all-visible. This benchmark continuously
inserts and updates allocation rows, so that prerequisite is weak:

- <https://www.postgresql.org/docs/18/indexes-index-only-scans.html>

PostgreSQL also recommends checking actual plans and workload behavior instead
of assuming an index will be selected or beneficial:

- <https://www.postgresql.org/docs/18/indexes-examine.html>
- <https://www.postgresql.org/docs/18/sql-explain.html>

The experiment narrows the next candidate: a meaningful improvement must
distribute capacity authority across independently lockable rows. Merely
accelerating the invariant recount does not remove the serialized counter.

### Rejected Bounded Shard Experiment

On 2026-07-17, a second uncommitted experiment replaced the policy counter
with at most 64 capacity shards per policy. Allocation identity froze its shard,
release decremented that exact shard, and migration testing backfilled 70 held
allocations from schema 31 while bounding a one-million permit policy to 64
rows. The upgrade test also exposed a pending deferred-trigger event that had
to be settled before altering the allocation table.

Two implementations ran against the same 4096-row, 64-claimant workload:

| Metric | Shared policy fence | Control-plane shard fence |
| --- | ---: | ---: |
| Throughput | 242.3 ops/s | 484.5 ops/s |
| Fresh p99 | 667.9 ms | 495.6 ms |
| Recovery p99 | 124.4 ms | 157.9 ms |
| Deadlocks | 0 | 0 |
| Exact-once projection | true | true |

The first version retained a shared policy-row lock on every claim and was
slower than the counter. The second moved disable fencing to the low-frequency
policy transition and materially improved throughput, but fresh p99 remained
about 27 percent worse than the slower retained counter run (495.6 ms versus
389.6 ms). It therefore failed the predeclared requirement to improve both
throughput and tail latency.

All shard migration, runtime, fixture, and benchmark edits were removed. The
result is evidence against adopting a more complex semaphore solely from
throughput measurements; any future replacement must also improve tail latency
under a repeated, isolated workload.

## Explicit Limits

This phase does not add:

- provider desired state or rollout policy;
- credential resolution or rotation;
- quota, pricing, or billing administration APIs;
- production-sized multi-host evidence;
- policy-slot capacity allocation; or
- Dreamina, Grok, Seedance, or paid-provider activation.
