# Phase 2AC: Provider Submit Scheduler Benchmark

Date: 2026-07-16

Status: implemented and verified as an isolated PostgreSQL development
benchmark. It activates no provider, reads no provider credential, launches no
CLI, and changes no public API.

## Decision

Add one explicit workspace tool:

```text
tools/provider-submit-bench
```

The tool calls the production PostgreSQL executor and provider stores. It does
not copy the claim SQL or implement a second scheduler. Each measured cycle
uses the same precedence as `ProviderSubmitService`:

1. resolve one due submit deadline;
2. claim one due recovery;
3. otherwise claim one fresh prepared executor submission.

The benchmark exists to falsify performance assumptions. It is not a
production daemon, a provider simulator, or a criterion for claiming industry
leadership.

## Isolation And Safety

Execution requires both:

- `PROVIDER_SUBMIT_BENCH_ACK=isolated-test-database-v1`; and
- `TEST_DATABASE_URL` naming a database whose name contains `test`.

The tool:

- creates a random schema;
- runs the real migrations in that schema;
- provisions one synthetic provider/account/profile;
- prepares only synthetic jobs and recoveries;
- runs no external process or network provider call;
- emits one JSON report;
- drops the schema on success and ordinary error paths; and
- never includes the database URL or synthetic credential reference in the
  report.

Queue preparation is outside the measured interval. Seed work is kept in a
fixed `JoinSet` window, so memory is bounded by seed concurrency rather than
queue cardinality.

## Workload Contract

The configurable workload records:

- total, fresh, and recovery row counts;
- claimant and seed concurrency;
- successful acquire p50, p95, p99, min, and max;
- fresh and recovery latency separately;
- total throughput and empty claim cycles;
- PostgreSQL wait-event samples by type and event;
- lock-wait samples;
- database deadlock delta;
- cluster-wide WAL LSN delta; and
- the first fresh and recovery completion ordinals.

After the measured interval, the benchmark fails unless the database proves:

- every seeded row was claimed exactly once;
- recovery claim commands equal recovery rows;
- fresh and recovery owners are unique;
- no prepared row remains;
- no deadline was quarantined;
- every execution still owns one held capacity allocation; and
- policy capacity equals the number of held allocations.

The WAL value is an upper bound if unrelated writers use the same PostgreSQL
cluster. Wait-event sampling is observational and can miss short waits. Both
limitations are encoded in the report contract rather than hidden.

## Finding

The first release run used:

- 512 total rows;
- 102 due recoveries and 410 fresh submissions;
- 16 concurrent claimants;
- 16 seed workers;
- PostgreSQL 18.3; and
- the same local machine and database for both runs.

The baseline showed that `claim_prepared` took exclusive `FOR UPDATE` locks on
the execution profile, credential pool, provider account, and resource policy
before selecting work. Every fresh claim for one profile therefore serialized
on static configuration rows, even though claim ownership was already
partitioned by `SKIP LOCKED`.

The fix separates configuration fencing from capacity mutation:

- handoff uses concurrent `FOR SHARE` locks on profile dependencies;
- fresh claim uses `FOR SHARE` on profile, pool, and account;
- policy enablement and capacity remain enforced by the later atomic
  `allocated_count < max_concurrency` update; and
- bound handoff replay uses shared locks because binding identity is immutable.

Profile, pool, and account disable updates still conflict with `FOR SHARE`, so
the optimization does not weaken the active-profile fence. Policy disable
still conflicts with the atomic policy update. A future administrative
mutation transaction must preserve the same profile/pool/account-before-policy
lock order; no such production mutation path exists in this phase.

## Measured Result

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Throughput | 286.0 ops/s | 1437.7 ops/s | 5.03x |
| All acquire p50 | 28.9 ms | 8.3 ms | -71% |
| All acquire p99 | 265.8 ms | 33.8 ms | -87% |
| Fresh acquire p99 | 273.9 ms | 34.2 ms | -88% |
| Sampled lock waits | 7136 | 456 | -94% |
| Deadlocks | 0 | 0 | unchanged |
| Exact-once projection | true | true | preserved |

The machine-readable inputs and outputs are preserved in:

- `docs/benchmarks/2026-07-16-provider-submit-before.json`; and
- `docs/benchmarks/2026-07-16-provider-submit-after.json`.

This is a controlled before/after comparison, not a production throughput
claim. It does not include journal fsync, CLI startup, provider latency,
artifact materialization, multi-host network latency, or unrelated database
traffic.

## Residual Bottleneck

The after run still sampled 456 lock waits. The remaining exact capacity fence
updates one `executor_resource_policies.allocated_count` row for every fresh
claim and terminal release.

The next optimization must be evaluated as a separate migration, not inferred
from this local result. The leading candidate is a durable policy-slot
semaphore:

```text
policy revision
  -> N immutable capacity-slot rows
  -> claim one free slot with FOR UPDATE SKIP LOCKED
  -> bind the slot to one held allocation
  -> release the allocation to make the slot claimable again
```

Before adopting it, tests must prove exact global capacity under concurrent
claim/release, migration correctness for existing held allocations, bounded
storage cost for large policies, disable/update fencing, and better measured
tail latency than the single counter. Until that evidence exists, the current
counter remains authoritative.

Phase 2AG later found and fixed a separate Read Committed snapshot race in the
counter's heartbeat-only constraint path. The shared counter remains
authoritative and remains the measured fresh-claim contention point:

- [`2026-phase2ag-capacity-counter-snapshot.md`](2026-phase2ag-capacity-counter-snapshot.md)

## Operation

Example:

```bash
PROVIDER_SUBMIT_BENCH_ACK=isolated-test-database-v1 \
TEST_DATABASE_URL=postgresql://127.0.0.1:5432/ai_image_factory_test \
PROVIDER_SUBMIT_BENCH_QUEUE_ROWS=4096 \
PROVIDER_SUBMIT_BENCH_RECOVERY_PERCENT=20 \
PROVIDER_SUBMIT_BENCH_CLAIMANTS=64 \
PROVIDER_SUBMIT_BENCH_SEED_CONCURRENCY=32 \
PROVIDER_SUBMIT_BENCH_OUTPUT=target/provider-submit-bench.json \
cargo run --release -p provider-submit-bench
```

Defaults are bounded. Claimants are capped at 64, queue rows at one million,
and all lease and provider timeout values have explicit ranges.

## Evidence Basis

PostgreSQL documents `SKIP LOCKED` as appropriate for avoiding row-lock
contention among multiple consumers of a queue-like table:

- <https://www.postgresql.org/docs/18/sql-select.html>

PostgreSQL documents `pg_stat_activity.wait_event_type` and `wait_event`, the
lag and transaction snapshot semantics of cumulative statistics, and the
`pg_stat_wal` and `pg_stat_database` views:

- <https://www.postgresql.org/docs/18/monitoring-stats.html>

PostgreSQL documents machine-readable `EXPLAIN`, `ANALYZE`, `BUFFERS`, `WAL`,
and the timing overhead tradeoff:

- <https://www.postgresql.org/docs/18/sql-explain.html>

These sources justify the measurement primitives. The benchmark result itself
is local empirical evidence and is intentionally not described as SOTA.

## Verification

This phase passed:

- benchmark configuration and percentile unit tests;
- 46 real PostgreSQL executor tests;
- an isolated 128-row smoke run;
- the controlled 512-row release before/after runs;
- exact-once post-run projections;
- zero-deadlock checks;
- full workspace tests with real PostgreSQL;
- workspace Clippy with warnings denied; and
- formatting and diff review.

The real Codex image smoke remains ignored because it would consume an external
provider quota. No Dreamina, Grok, or other external provider was invoked.
