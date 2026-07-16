# Phase 2Y: Fenced Provider Submit Recovery

Date: 2026-07-16

Status: implemented and verified against PostgreSQL 18 plus the real gated
submit helper using a local fake CLI. This phase activates no provider,
credential, route, model, billing behavior, daemon, or external call.

## Scope

Phase 2Y closes the recovery gap inside the existing unique submit
orchestrator:

```text
provider/account recovery claim
  -> frozen source command + database remaining budget
  -> typed canonical provider command
  -> exact Phase 2M/2N journal attempt
  -> fenced rejection or fenced attach
```

It does not create another submit queue or reconstruct an expired executor
lease. Fresh submit still requires a live `ExecutorSubmissionLease`.
Abandoned submit recovery uses only the provider/account-scoped recovery lease
created at the original `sending` linearization point.

## Authority Split

There are now two explicit work types:

- `ProviderSubmitWork<D>` owns fresh work authorized by a live executor lease;
- `ProviderSubmitRecoveryWork<D>` owns recovery metadata authorized by a live
  provider submit recovery fence.

The recovery work constructor validates:

- output index and output total;
- source command SHA-256;
- provider command schema and adapter revision;
- canonical provider command SHA-256;
- frozen execution binding;
- remote-task completion and submission-bound idempotency; and
- a positive PostgreSQL-derived remaining deadline budget.

The work copies only bounded intent/context metadata and the recovery fence. It
does not clone the source `command_json`, which may be up to the admission
request limit. The typed provider command remains an `Arc` shared by journal,
driver, and receipt replay.

## Frozen Command Projection

`claim_submit_recovery` now returns the immutable source `command_json` joined
from `job_payloads`. The claim projection verifies that serializing the JSON
still produces the SHA-256 frozen on `provider_submissions`.

The command is not duplicated into the recovery table or command ledger. The
existing immutable job payload remains authoritative, while the recovery lease
provides a read-only accessor for a provider-specific projector. This keeps
provider DTO parsing outside PostgreSQL code and keeps SQL outside provider
adapters.

The recovery lease uses a custom `Debug` implementation. Source command JSON,
credential identity, authentication digest, and authority seal are redacted.

## Database Time

The recovery claim projection computes:

```text
remaining_budget_ms =
  max(provider_deadline_at_ms - claim_claimed_at_ms, 0)
```

`claim_claimed_at_ms` is written from the database statement clock and stored
in the replayable recovery command. The first response and every exact retry of
the same command therefore return the same authority snapshot.

Recovery heartbeat uses a new database observation time and refreshes the
returned remaining budget while preserving owner and epoch. The orchestrator
subtracts local monotonic elapsed time spent preparing and fsyncing the journal
before dispatch. It does not derive provider authority from a host wall clock.

## Recovery State Machine

| Frozen submit state | Recovery action |
| --- | --- |
| `sending` | prepare or reopen the exact journal attempt, commit its unique launch if absent, then dispatch/recover that attempt |
| `outcome_unknown` | observe durable journal/process evidence only; never commit a new launch |
| `operation_known` | attach the known operation under the recovery fence |
| `attached` | replay the attached task |
| `rejected` or `deadline_quarantined` | replay terminal state |
| `reserved` | reject as invalid recovery work |

Completing an elected `sending` attempt after a crash is not a resubmit. The
PostgreSQL intent, provider command digest, execution binding, journal
submission directory, launch nonce, and gated process identity are unchanged.
`RemoteSubmitJournal::commit_launch` still publishes one create-once launch
authority.

An `outcome_unknown` recovery never calls `commit_launch`. It may import a
durable accepted receipt, confirmed rejection, or existing released-process
evidence, but absent such evidence it remains awaiting recovery.

## Fence Semantics

The recovery fence is deliberately narrower than a generic write token:

- confirmed `NoRemoteEffect` rejection carries the fence and atomically closes
  recovery;
- a valid remote operation receipt is recorded first, then attach carries the
  fence and atomically closes recovery;
- `UnknownRemoteEffect` evidence never carries the fence and cannot close
  recovery; and
- a mismatched receipt remains quarantined and recoverable.

This preserves the existing store invariant that only a confirmed rejection or
an attached operation terminalizes submit recovery ownership.

## Cost Model

The claim transaction adds one indexed `job_payloads` join and returns one
bounded JSONB value already needed to reconstruct provider work. It adds no
table, migration, durable duplicate, global scan, message broker, registry,
trait object, or network hop.

Recovery work construction hashes no source JSON and performs no deep JSON
clone. The store verifies the source hash once while loading the claim.
Heartbeat hot paths remain on the narrow recovery row and do not reload the
command.

These are structural cost bounds, not throughput or SOTA claims. Production
defaults still require mixed-account p50/p95/p99, lock-wait, JSON payload-size,
CPU, allocation, and WAL measurements.

## Verification

The existing six real PostgreSQL gated-submit tests all pass and now include:

1. a crash after PostgreSQL elected `sending` but before any journal file or
   CLI process existed;
2. expiry of the original executor lease;
3. a provider/account recovery claim with the exact source command and
   database-derived remaining budget;
4. creation and execution of exactly one journal attempt;
5. attachment under the exact recovery owner/epoch; and
6. no request JSON in recovery debug output.

A second fault test produces one invalid CLI receipt and durable
`outcome_unknown`, lets the executor lease expire, claims recovery, and calls
recovery twice. Both calls remain observation-only and the fake CLI side effect
count stays exactly one.

The prior tests continue to cover 32 concurrent fresh callers, released-gate
restart, durable receipt replay after PostgreSQL transaction failure, and
pre-release deadline exhaustion.

No paid provider, production credential, provider login, callback, or external
network is used.

## Evidence Basis

PostgreSQL 18 documents `statement_timestamp()` as the start time of the
current statement:
<https://www.postgresql.org/docs/18/functions-datetime.html>.

PostgreSQL documents `SKIP LOCKED` as appropriate for queue-like multiple
consumer access:
<https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE>.

Tokio documents `spawn_blocking` as the boundary for blocking filesystem and
digest work:
<https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html>.

The durable journal and gated-process evidence basis remains in Phase 2M
through Phase 2O.

These sources justify the primitives. They do not prove that the repository is
industry-leading or production-ready.

## Next Gate

The next independent phase should compose a provider-neutral submit iteration
and bounded daemon:

1. prioritize deadline resolution and claimed recovery before fresh work;
2. heartbeat the exact recovery or executor lease during orchestration;
3. project source commands through a statically dispatched provider factory;
4. pace idle/error loops without an in-memory work queue;
5. drain active attempts on shutdown; and
6. bind one inactive Dreamina profile/account/home/helper composition without
   adding public routing or making an external call.

Phase 2Z implements the provider-neutral service and bounded daemon kernel,
including stable retry identities and the inactive Dreamina projector:
[`2026-phase2z-provider-submit-service-kernel.md`](2026-phase2z-provider-submit-service-kernel.md).
