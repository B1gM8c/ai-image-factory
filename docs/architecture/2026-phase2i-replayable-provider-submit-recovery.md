# Phase 2I: Replayable Provider Submit Recovery

Date: 2026-07-16

Status: the recovery command protocol and bounded claim are implemented and
PostgreSQL-verified. Remote CLI providers remain inactive. This phase is a
prerequisite for an orchestrator, not permission to start one.

## Scope

This phase closes two submit-recovery defects without adding a provider daemon,
a second queue, a generic scheduler, or an external provider call:

1. `claim_submit_recovery` and `defer_submit_recovery` now accept a
   caller-generated command identity and can replay any committed command
   acknowledgement;
2. one claim call examines at most 64 ordered recovery candidates before
   applying `SKIP LOCKED` to held capacity.

The public image API, provider activation set, billing behavior, and artifact
contract do not change.

## Decision

Keep PostgreSQL as the only durable queue and add one append-only command receipt
table. Saving only the latest command on the recovery row was rejected during
adversarial review: a delayed retry could otherwise mint authority for another
submission after a later reclaim overwrote its evidence.

Each successful claim or defer appends one immutable receipt keyed by exact
`(provider, account, owner, command_id)`. A claim receipt freezes the full
returned lease, including the mutable submit-intent snapshot. A defer receipt
freezes the requested delay and target lease. A claim identity always resolves
to its originally selected submission and conflicts on kind or duration. A
defer identity also conflicts on a different submission or epoch. Historical
replay remains valid after heartbeat, receipt arrival, defer, reclaim, close,
and capacity release.

A claim that returns no work has created no durable authority and does not append
a receipt. Retrying such a no-effect poll may observe later work. Persisting every
empty scheduler probe would create unbounded idle write amplification without
protecting a side effect.

This is acknowledgement replay, not a claim of exactly-once external execution.
No provider I/O occurs in these methods.

## Command Protocol

For a claim:

1. validate the exact provider/account scope, owner, command identity, and lease
   duration;
2. take a transaction advisory lock derived from the command key, so concurrent
   retries cannot both miss an uncommitted receipt;
3. if found, verify the request duration and reconstruct the original response
   from the immutable command and intent snapshots;
4. otherwise materialize the first 64 due recoveries in deterministic order;
5. lock one held capacity allocation with `SKIP LOCKED`, then lock its recovery
   row and advance the recovery epoch;
6. append the command and its complete response snapshot, heartbeat capacity,
   and commit in the same transaction.

Heartbeat changes only the live recovery row. The command receipt is immutable,
so a lost claim acknowledgement never replays a response manufactured by later
work or enriched by a late receipt.

For a defer, the transaction takes the same command lock, checks historical
replay and payload equality, then locks held capacity before recovery. It appends
the receipt and clears the live lease in one transaction. The retry time is
capped by the frozen provider deadline.

## Database Invariants

Migration `0024_replayable_provider_submit_recovery_commands.sql` adds one
command table whose primary key is the replay identity. Its checks and triggers
enforce:

- command kind and identity are non-null and use the same syntax as Rust;
- request duration is frozen for both claim and defer;
- claim time, initial expiry, and the complete mutable intent response are
  present only for claim receipts;
- a new claim receipt exactly matches a live recovery lease and current intent;
- a new defer receipt exactly matches the live lease being relinquished;
- one submission epoch has at most one claim and one defer command identity;
- command rows cannot be updated, deleted, or truncated; and
- every recovery acquisition/reclaim and defer has matching command evidence at
  transaction commit, so an old writer fails closed.

The schema-before-binary migration requests `ACCESS EXCLUSIVE` immediately,
avoiding a weaker-lock upgrade cycle, and fails closed when a legacy worker still
owns a recovery lease. Deployment must drain all recovery writers and use a
bounded session `lock_timeout`. PostgreSQL transactional DDL removes the table,
primary key, functions, and triggers if any later statement fails.

This repository uses forward-only schema versions; an old binary rejects schema
24 after migration. Release rollback is therefore a tested forward repair or
database restore, not starting the version-23 binary against the new schema.
The compatible binary and rollback artifact must be validated before migration.

## Bounded Claim And Locking

Recovery ordering remains:

```text
max(next_recovery_at, live lease expiry)
-> provider deadline
-> submission id
```

The first CTE materializes no more than 64 rows from the existing partial scope
index. Only that fixed window is joined to held capacity and considered by
`FOR UPDATE ... SKIP LOCKED`. If all 64 allocations are locked, the call returns
empty instead of scanning row 65. A later scheduler round may retry the scope.

Deadline filtering happens after this window is fixed. Therefore an expired
64-row prefix also returns empty instead of scanning past the bound. The future
orchestrator must run deadline quarantine before recovery claims so that expired
rows are removed from the active window; this is an intentional coupling of lane
order, not hidden work inside recovery claim.

This bounds work per database call; it does not provide cross-account fairness.
The future orchestrator must rotate exact provider/account scopes and avoid
draining one hot scope continuously.

All runtime mutations use the existing lock order:

```text
command advisory key -> held capacity allocation -> submit recovery
```

Paths that do not create a command keep the prior
`held capacity allocation -> submit recovery` order. No path takes the command
lock after a row lock, and hash collisions only serialize unrelated commands;
the primary key remains the correctness authority.

No transaction holds locks while external provider work is running.

## Cost Model

The steady-state claim path adds one transaction advisory lock and one
primary-key replay lookup before the bounded queue claim. A successful claim or
defer appends one narrow receipt; an empty claim appends nothing. There is no
second work queue, dispatcher hop, or background cleanup. The receipt table uses
its replay primary key plus a narrow unique transition index on
`(submission, epoch, kind)`. The second index bounds projection checks and
prevents aliases for the same claim or defer. Command evidence is retained
alongside the already durable recovery history.

The 64-row window is a deterministic operational bound, not a measured optimal
batch size. Production activation still requires mixed-load measurements of
latency, lock waits, buffers, WAL, bloat, and fairness. This phase makes no SOTA
or throughput claim.

## Verification

The PostgreSQL 18.3 integration suite covers:

- one winner under concurrent claims;
- concurrent retries of one command returning the same result and one receipt;
- exact claim acknowledgement replay after a later reclaim;
- replay after lease expiry without minting a new epoch;
- replay after heartbeat with the original response snapshot;
- replay after a late receipt without changing mutable intent fields;
- exact defer acknowledgement replay;
- claim/defer payload mismatch rejection;
- empty claim probes producing no command rows;
- command update and deletion rejection;
- provider/account scope isolation;
- stale heartbeat and stale attachment fences;
- claim and heartbeat capacity heartbeats;
- a fully locked 64-row window returning empty without claiming row 65;
- a fully expired 64-row window returning empty without scanning to row 65;
- successful claim after that window unlocks;
- rejection of migration with a live legacy claimant;
- successful migration after drain and rejection of an old writer; and
- complete transactional rollback after a forced late migration failure.

The recorded verification used an explicit `TEST_DATABASE_URL`; the server
reported `server_version=18.3` and `server_version_num=180003`. Reproduction:

```bash
psql "$TEST_DATABASE_URL" -Atc 'SHOW server_version; SHOW server_version_num;'
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p gpt-image-2-gateway \
  --test postgres_provider_tasks -- --test-threads=1
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p gpt-image-2-gateway \
  --test postgres_migrations -- --test-threads=1
```

The local test harness skips PostgreSQL cases when `TEST_DATABASE_URL` is
absent. CI and release gates must therefore require and export it; a green run
without that environment variable is not database evidence.

The phase gate also requires formatting, Clippy with warnings denied, all
workspace tests against real PostgreSQL, and independent adversarial review.

## Evidence Basis

PostgreSQL documents `SKIP LOCKED` as suitable for queue-like consumers while
warning that it presents an inconsistent view; this design uses it only for
work selection, never for business truth:
<https://www.postgresql.org/docs/18/sql-select.html>.

PostgreSQL rollback discards all updates made by the transaction, which is the
basis for the forced-late-failure migration test:
<https://www.postgresql.org/docs/18/sql-rollback.html>.

Caller-provided request identity and semantic equivalence are the basis of safe
retry contracts; the command ID here identifies a state transition, not the
remote provider operation:
<https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/>.

## Remaining Activation Gates

1. Add a database absolute deadline to attached remote tasks. Poll claim and
   heartbeat must stop at that deadline, and expiry must quarantine unresolved
   remote effects without releasing capacity.
2. Freeze operation descriptor identity and idempotency mode with the existing
   provider, adapter, command, account, credential, and policy identities.
3. Add the unique orchestrator code path only after the deadline state machine
   is safe. It must be the sole adapter side-effect boundary; this does not
   require a global singleton process.
4. Add durable submit helper evidence or prove provider-native idempotent submit
   before any non-idempotent CLI can be activated.
5. Close artifact publication races with proven byte-stable replay or immutable
   epoch/content-addressed staging, a one-shot lease-scoped sink, and bounded
   orphan cleanup.
6. Harden CLI process and download boundaries with absolute cancellation,
   private directories, regular-file/link checks, media decoding, resource
   limits, and credential isolation.
7. Run the mixed-load million-row benchmark before activating a remote CLI
   provider.

Until these gates close, `providerd` remains a design target rather than a
production process, and Dreamina/Grok remain inactive.
