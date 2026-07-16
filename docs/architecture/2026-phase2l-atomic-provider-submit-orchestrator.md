# Phase 2L: Atomic Provider Submit Orchestrator

Date: 2026-07-16

Status: the remote-task submit edge has one candidate authoritative
orchestration boundary, an atomic PostgreSQL dispatch election, exact
command-object binding, and replay behavior that never repeats a
`submission_bound` side effect through that boundary. Remote CLI providers
remain inactive. Poll, cancel, materialization, durable process journaling, and
capacity reconciliation are still activation gates.

## Scope

Phase 2L implements only the first external side-effect edge:

```text
executor lease + canonical command
  -> atomic acquire
  -> exactly one Dispatch authority
  -> provider submit
  -> durable receipt or failure
  -> attach known remote operation
```

It adds no API route, scheduler daemon, billing behavior, network call, CLI
launch, provider credential, or provider activation. All integration tests use
`ScriptedFakeProvider`; no paid or external provider is contacted.

## Decisions

### One atomic dispatch election

`PostgresProviderTaskStore::acquire_submit` locks the execution, submission,
and capacity allocation, validates the frozen binding, and creates both the
`sending` intent and active recovery row in one transaction. It returns one of
five restricted actions:

- `Dispatch`: the only action carrying submit context;
- `AttachOnly`: a receipt exists, so only attach may run;
- `Busy`: another process owns or has already started the send;
- `ObserveOnly`: remote effect is uncertain and must not be repeated; or
- `Terminal`: submit processing has already converged.

The dispatch and attach authorities have private fields and are not `Clone`.
Thirty-two competing replicas therefore receive exactly one `Dispatch`. This
is a static code-path boundary backed by row locks and durable state; it is not
a distributed mutex service or an in-memory leader election.

The older `reserve_submit` and `start_submit` primitives remain temporarily for
historical state-machine tests and migration compatibility. Production code has
no call site for them. Their removal and the narrowing of gateway exports remain
API-hardening work; this phase does not claim that Rust can prevent a future
crate from deliberately bypassing the orchestrator.

### Exact command ownership

`ProviderSubmitWork<P>` owns one `SingleOutputCommand<P::Payload>`. The same
object supplies the reservation identity and is borrowed by the provider
adapter after dispatch authority is acquired. The work constructor checks its
output index, source admission-command SHA-256, command schema, and adapter
revision against the executor lease. Atomic acquire also compares both output
index and output total with immutable `job_outputs`/`jobs` identity before it
can create dispatch authority. The orchestrator then checks provider identity
and the complete frozen context before any provider call.

`SingleOutputCommand::new` no longer accepts independent schema, adapter, and
digest arguments. The adapter-owned `CanonicalCommandPayload` consumes its
typed payload into owned canonical bytes. The SDK stores those immutable bytes
and computes the per-output digest itself, including schema, adapter revision,
source command hash, and output slot. Providers can read only that frozen byte
buffer from the command. This removes the previous caller-controlled digest and
mutable-payload split. It does not prove that an external process obeyed the
bytes; process-level attribution remains an activation test.

### Remote submit is asynchronous only

`RemoteTaskProvider::submit` now returns only `PendingOperation`. Synchronous
artifact completion remains an `InlineProvider` responsibility. This matches
the durable database state machine: a remote submit always produces an
operation receipt before polling or materialization. It avoids a successful
`Submission::Completed` result for which no atomic persistence edge existed.

`SubmitCall` packages the frozen invocation context, exact command reference,
and idempotency mode. The context includes the persisted timeout and absolute
Unix deadline. The current orchestrator accepts only `submission_bound` and
never retries `sending` or `outcome_unknown`. On replay, a changed process
configuration cannot replace the timeout frozen on the existing intent.

Dispatch authority also carries the remaining budget computed from PostgreSQL
time immediately before commit. The orchestrator converts that duration to a
Tokio monotonic timeout around the provider future. A stuck submit becomes
`outcome_unknown`; it cannot wait without bound. Dropping a future is still not
a daemon-crash recovery mechanism, so process-group enforcement and the durable
helper remain activation gates.

### Receipt before attach

A valid pending result is attributed to both the frozen provider and submission.
The orchestrator durably records its receipt before attach. A crash or database
error after the external side effect leaves `sending`, `outcome_unknown`, or
`operation_known`; replay returns `Busy`, `ObserveOnly`, or `AttachOnly` and
does not call submit again.

A provider or submission mismatch is recorded as `outcome_unknown`, never
attached. Its observed provider, submission, operation, request ID, expected
provider, and execution binding are preserved in the append-only
`provider_submit_quarantined_receipts` table for reconciliation and audit. A
failure with `NoRemoteEffect` becomes `rejected`; a failure with
`UnknownRemoteEffect` remains recoverable evidence and cannot be automatically
retried. Receipt, failure, and attach event identities are deterministic hashes
of the frozen execution binding and evidence.

## Migration 0027

Migration `0027_atomic_provider_submit_acquisition.sql` permits a submit intent
to be inserted directly as `sending` only when:

- its lifecycle fields describe a fresh send;
- its frozen output index and total match the durable job projection;
- its execution/submission/allocation binding is live and exact; and
- an active recovery row exists by transaction commit.

Existing schema 26 intents are backfilled from `job_outputs.output_index` and
`jobs.requested_units` while the submit table is write-locked. The fields then
become non-null, constrained, and immutable. Their addition does not change the
version-1 execution-binding digest, so old reserved and sending rows remain
replayable by the new binary.

The deferred intent/recovery projection now runs after both INSERT and UPDATE.
Committing `sending` without recovery fails and rolls back. A process crash at
any point before commit therefore exposes neither row; after commit both rows
are durable.

The migration takes an explicit write lock only on
`provider_remote_submit_intents`, while the authoritative parent rows are read
under normal PostgreSQL read locks, and uses a five-second local lock timeout.
A conflicting writer causes a bounded transactional failure; retry after
release succeeds.

The gateway build script marks the migration directory as a Cargo input. This
prevents `sqlx::migrate!` test binaries from silently retaining an older embedded
migration set after a new SQL file is added.

Schema 27 is forward-only for binaries: a version-26 binary rejects a database
with a newer migration version. Deployment must stop old submit writers, apply
the migration with the compatible binary, and restart that binary. Rollback is
forward repair or a database restore paired with the older binary; merely
restarting version 26 against schema 27 is not a supported rollback. Remote
providers are still inactive, so no remote history needs draining in this phase.

## Cost Model

The submit path performs one existing binding query under row lock, one intent
insert, one recovery insert, and two small context reads in a single transaction.
It computes fixed-size SHA-256 identities over already-loaded metadata. Provider
dispatch uses generic static dispatch; there is no trait object, runtime registry,
message broker, global account lock, or additional network hop.

Replay performs the same indexed binding lock/read and returns a restricted
state. It does not allocate artifact-sized buffers or invoke the provider. These
are structural cost bounds, not throughput claims. Production activation still
requires p50/p95/p99, lock-wait, buffer, WAL, allocation, CPU, and fairness
measurements on a production-sized PostgreSQL clone.

## Verification

The PostgreSQL 18.3 tests cover:

- 32 concurrent atomic acquires electing one dispatch with one recovery row and
  no `reserved` row;
- schema 26 to 27 preservation of existing `reserved` and `sending` intents,
  output-projection backfill, and atomic adoption of only the reserved intent;
- rejection of a command whose output total disagrees with durable job identity
  before the fake provider is called;
- rejection and rollback of `sending` without recovery;
- 32 concurrent orchestrator calls invoking fake submit exactly once;
- successful receipt, attach, and replay without resubmit;
- replay after changing configured timeout while preserving the frozen timeout;
- a stuck provider future becoming uncertain within its database-derived
  remaining budget;
- preservation of `outcome_unknown` across replay;
- append-only quarantine of a receipt attributed to another provider;
- bounded migration lock failure and clean retry; and
- all prior submit recovery, deadline, capacity, artifact, migration, and
  execution-binding regressions.

Provider SDK and conformance tests cover payload-owned command identity,
deadline propagation, asynchronous submit, provider-token representation, poll
completion, and restart without submit.

## Evidence Basis

PostgreSQL documents that row locks block concurrent writers and lockers on the
same row until transaction end:
<https://www.postgresql.org/docs/18/explicit-locking.html#LOCKING-ROWS>.

PostgreSQL documents deferred constraint triggers as firing at transaction end
and `SET CONSTRAINTS` behavior:
<https://www.postgresql.org/docs/18/sql-set-constraints.html>.

Tokio documents that cancelling a future drops it, but that property alone does
not provide process-crash durability:
<https://docs.rs/tokio/latest/tokio/time/fn.timeout.html>.

## Remaining Activation Gates

1. Route submit through the existing durable helper/journal so daemon death
   cannot lose a receipt after spawning a side-effecting CLI process.
2. Bind the PostgreSQL absolute deadline to the external process group and prove
   kill, reap, and zero-orphan behavior.
3. Extend the orchestrator with recovery claims, poll, cancel, callback wakeup,
   artifact materialization, deadline resolution, and provider/account scope
   rotation.
4. Replace full-media artifact buffers with a lease-scoped streaming sink before
   video providers are enabled.
5. Reconcile remote-task deadline capacity using strong late evidence and bind
   the resulting terminal state to usage and billing decisions.
6. Internalize the raw task-store lifecycle API after historical integration
   tests are moved behind a crate-private test boundary.
7. Run production-scale mixed-account benchmarks and migration rehearsal before
   enabling Dreamina, Grok, or any other remote CLI provider.

Until these gates close, all remote CLI providers remain planned/inactive and
the public Codex image behavior remains unchanged.
