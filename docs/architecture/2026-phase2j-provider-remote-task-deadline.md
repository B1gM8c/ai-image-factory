# Phase 2J: Provider Remote Task Deadline Authority

Date: 2026-07-16

Status: the attached-task absolute deadline, deadline quarantine, and committed
artifact recovery paths are implemented and PostgreSQL-verified. Remote CLI
providers remain inactive. This phase closes a storage-state prerequisite; it
does not authorize external provider execution.

## Scope

Phase 2J closes the period after a remote submit receipt has been attached but
before a provider poll produces canonical terminal evidence. It adds no new
daemon, queue service, public image endpoint, provider activation, billing
behavior, or external provider call.

The public `ProviderTaskState` and `ProviderRemoteTask` shapes remain unchanged.
An additive `ProviderTaskDeadlineStore` owns maintenance resolution so existing
`ProviderTaskStore` implementations are not source-broken. Internally, a task
deadline quarantine projects through the existing public `Uncertain` state.

## Authority Model

The active submit recovery already freezes one database absolute
`provider_deadline_at_ms`. Attach copies that exact value onto the remote task;
it never recomputes a relative timeout. Initial poll time, poll claim expiry,
heartbeat extension, callback wakeup, cancel wakeup, and waiting observations
are all bounded by this task deadline.

Database time is read only after the task row lock is held. A process-local
clock or a timestamp captured before lock wait cannot authorize a write after
the deadline.

At or after the deadline, one provider/account-scoped maintenance call locks a
due task and selects exactly one of two branches:

```text
provider_waiting + committed result manifest
  -> artifact_recovery observation
  -> artifact_ready
  -> remote_provider_observation decision
  -> canonical succeeded + capacity released

provider_waiting + no committed result manifest
  -> immutable remote task quarantine
  -> remote_task_deadline decision
  -> canonical uncertain + capacity held
```

The artifact branch closes the crash gap in which immutable artifact authority
and its result manifest committed, but the poll observation acknowledgement was
lost. The recovery observation is database-generated, uses a deterministic
event identity reserved from public attach, poll, cancel, and callback inputs,
uses a manifest-derived artifact reference, and must match the immutable
manifest fingerprint.

The quarantine branch is deliberately not a provider observation. A timeout
proves only that local observation authority expired; it does not prove whether
the remote side effect completed. The append-only quarantine is therefore a
separate evidence type linked exactly to the task, decision, execution, and
submission. Capacity remains held until a later phase defines strong late
evidence and an explicit reconciliation protocol.

## Database Invariants

Migration `0025_provider_remote_task_deadline_quarantine.sql` enforces:

- task deadline equals the frozen closed submit-recovery deadline;
- task identity and deadline are immutable after attach;
- poll claim, reclaim, heartbeat, callback, cancel, and provider observations
  cannot cross the database absolute deadline;
- poll leases and next-poll times cannot extend beyond that deadline;
- exact observation acknowledgement replay remains read-only after lease or
  task terminalization, while a new stale event is rejected;
- waiting acknowledgement replay compares the original relative poll delay,
  while retaining compatibility with pre-25 absolute-time hashes;
- a deadline quarantine exactly matches provider, account, operation, deadline,
  error, and quarantine time;
- quarantine rows cannot be updated, deleted, or truncated;
- task, quarantine, decision, parent states, and held capacity form one deferred
  all-or-nothing projection;
- an `artifact_recovery` observation requires its exact immutable authority and
  result manifest;
- arbitrary `terminal_evidence` cannot release capacity without either its
  exact active runner observation or the unique late observation for the same
  execution after an `executor_lease_expired` decision; and
- a `remote_task_deadline` decision cannot release capacity in this phase.

`provider_remote_task_deadline` is descriptive error text, not authority. A
provider observation may use that error code under the existing public
contract. Only a non-null quarantine foreign key and its exact
`remote_task_deadline` decision activate quarantine invariants.

The resolver transaction locks the task before checking artifact authority.
Artifact publication also locks the task before inserting authority and the
manifest. These paths therefore serialize at the authority boundary: the
resolver observes either the complete committed manifest or no manifest, never
a split publication.

## Migration Protocol

The schema-before-binary migration requests `ACCESS EXCLUSIVE` on every affected
table in one first `LOCK TABLE` statement. This avoids lock-upgrade deadlocks and
waits for old writers before inspecting legacy state.

The migration fails closed when any legacy poll owner remains, a waiting task is
already past its deadline, a next-poll time exceeds the frozen deadline, submit
recovery lineage is incomplete, parent state is split, or capacity is not held.
It does not guess at an operational outcome during DDL.

Backfill copies the exact closed recovery deadline. Existing deferred projection
events are settled under the table locks before later `ALTER TABLE` statements,
then deferred mode is restored. A forced exception after the final migration
statement proves that PostgreSQL transactional DDL restores the old columns,
triggers, and schema with no partial residue.

Deployment must drain provider poll writers, measure affected row count and lock
duration on a production-sized clone, set bounded session and lock timeouts, and
ship the compatible binary before applying the migration. Schema versions are
forward-only; rollback is a tested forward repair or database restore, not
starting a version-24 binary against schema 25.

## Bounded Selection And Scheduling

Poll claim fixes its ordered 64-row candidate window before excluding expired
tasks. The deadline resolver independently fixes at most 64 due tasks in exact
provider/account scope, then uses `FOR UPDATE ... SKIP LOCKED` to select one. If
all 64 rows are locked, the call returns empty instead of scanning row 65.

This bounds database work per call but does not by itself provide cross-account
fairness. The future single orchestrator code path must rotate configured
provider/account scopes and run lanes in this order:

```text
remote task deadline
-> submit deadline
-> capacity reconciliation
-> submit recovery
-> poll and cancel
```

Deadline lanes run first so expired prefixes cannot starve active recovery or
poll windows. “Single orchestrator” means one code path owns adapter side
effects; it does not require one global process. Multiple replicas may compete
through the existing database fences.

No transaction holds a row lock while a provider CLI or network operation is
running.

## Cost Model

The steady poll path adds one immutable deadline column comparison and caps the
existing lease update. No extra queue hop or steady-state append is introduced.
Deadline maintenance uses the existing PostgreSQL pool, one partial scope index,
a fixed 64-row window, and one transaction per resolved task. Only terminal
deadline resolution appends quarantine or recovery evidence.

These are structural bounds, not throughput results. The value 64 is an
operational bound, not a measured optimum. Production activation still requires
mixed-load measurements of p50/p95/p99 latency, lock waits, scanned rows, buffer
hits, WAL, bloat, starvation, and recovery throughput at production cardinality.
This phase makes no SOTA, million-row, or zero-overhead claim.

## Verification

The PostgreSQL 18.3 suite covers:

- exact deadline copy and initial next-poll cap;
- claim and saturated heartbeat expiry capped at the task deadline;
- task-lock wait crossing the deadline for heartbeat, poll observation, and
  artifact publication, with no late durable evidence;
- provider/account scope isolation;
- one winner under concurrent deadline resolution;
- immutable quarantine, canonical uncertain parents, and capacity retention;
- committed artifact authority recovery to canonical success and exact capacity
  release;
- exact observation acknowledgement replay after terminalization;
- rejection of a reused waiting event with a different relative delay;
- rejection of the reserved artifact-recovery event identity at both Rust and
  database boundaries;
- preservation of the public uncertainty error-code space without creating a
  quarantine marker;
- a locked 64-row deadline window returning empty without scanning row 65;
- 24-to-25 backfill with a real legacy attached row;
- `ACCESS EXCLUSIVE` requested before a weaker migration lock;
- rejection of a live legacy poll owner and an already-due waiting task;
- rejection of a legacy observation occupying the new internal event identity;
- complete rollback after a forced late migration failure; and
- all prior submit recovery, capacity, artifact, and migration regressions.

The full executor regression also proves that a lease-expired canonical
`uncertain` execution may release local capacity only after its own unique late
runner observation arrives. That observation does not rewrite the canonical
outcome.

Reproduction:

```bash
psql "$TEST_DATABASE_URL" -Atc 'SHOW server_version; SHOW server_version_num;'
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p gpt-image-2-gateway \
  --test postgres_provider_tasks -- --test-threads=1
TEST_DATABASE_URL="$TEST_DATABASE_URL" cargo test -p gpt-image-2-gateway \
  --test postgres_migrations -- --test-threads=1
```

The local harness skips PostgreSQL tests when `TEST_DATABASE_URL` is absent. CI
and release gates must require it; a skipped run is not database evidence.

## Evidence Basis

PostgreSQL documents `clock_timestamp()` as the wall-clock source that advances
within a statement, unlike transaction and statement timestamps:
<https://www.postgresql.org/docs/18/functions-datetime.html>.

PostgreSQL documents `SKIP LOCKED` for queue-like consumers while warning that it
provides an inconsistent view. This design uses it only for bounded work
selection, never as business truth:
<https://www.postgresql.org/docs/18/sql-select.html>.

PostgreSQL recommends taking the strongest required lock first when concurrent
DDL could otherwise require a lock upgrade:
<https://www.postgresql.org/docs/18/sql-lock.html>.

Constraint triggers and their effects participate in the same transaction and
roll back with it:
<https://www.postgresql.org/docs/current/trigger-definition.html>.

The absolute-deadline model follows the same elapsed-time-preserving principle
documented for gRPC deadline propagation:
<https://grpc.io/docs/guides/deadlines/>.

Timeouts do not prove remote non-execution, and retry amplification must be
controlled at one layer:
<https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/>.

## Remaining Activation Gates

1. Add strong-evidence reconciliation for quarantined remote tasks, including
   late terminal evidence, exact capacity release, and billing consequences.
2. Freeze operation descriptor identity and idempotency mode with provider,
   adapter, command, account, credential, and policy identities.
3. Implement the sole orchestrator code path with scope rotation and the lane
   order above; do not activate a provider from ad hoc loops.
4. Bind an absolute deadline to the external CLI process group and prove kill,
   reap, and orphan cleanup. The database fence rejects late commits but cannot
   stop an already-running process.
5. Prove provider-native idempotent submit or durable helper evidence before any
   non-idempotent CLI submit can be retried.
6. Complete byte-stable artifact staging, media decoding, private-directory and
   link checks, resource limits, credential isolation, and bounded orphan
   cleanup.
7. Run migration rehearsals and mixed-load production-scale benchmarks before
   enabling Dreamina, Grok, or another remote CLI provider.

Until these gates close, the public Codex image behavior remains unchanged and
all remote CLI providers remain inactive.
