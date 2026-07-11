# Phase 1D Provider Submission Kernel

Status: implementation checkpoint for the Codex vertical slice.

This document narrows the Phase 1 target architecture into the next additive
database and process-boundary changes. It does not replace
`2026-ai-image-factory-target-architecture.md`.

## 1. Decision

The platform must persist a provider side-effect identity before it moves Codex
execution into `executord`. A worker attempt is not that identity: the current
`WorkLease.execution_id` is created for a lease epoch and can change after an
unstarted lease is requeued.

The identity chain is:

```text
job_id
  -> output_id
    -> work_item_id
      -> attempt execution_id
        -> submission_id
          -> executor_execution_id
```

- `output_id` identifies one requested deliverable.
- attempt `execution_id` fences one worker lease.
- `submission_id` identifies one logical provider side effect.
- `executor_execution_id` is the durable `start_or_attach` identity used by the
  executor supervisor.

Phase 1 initially uses one submission and executor execution per output. The
identities remain distinct even while their cardinality is one-to-one.

## 2. Trust Boundaries

Three credentials are separate capabilities and must never be interchangeable:

1. A client Bearer credential authorizes an API request.
2. A provider credential authorizes an upstream account or CLI session.
3. An internal executor lease authorizes one fenced execution transition.

Client credentials, API-key digests, HMAC peppers, database credentials, and
gateway administrator tokens must not enter provider commands, executor spool,
request directories, artifacts, or provider logs.

`workerd` will eventually own queue consumption and orchestration only.
`executord` will own durable start-or-attach supervision. Only an isolated
executor runner may spawn a provider CLI.

## 3. Database State Machines

### Job output

```text
pending -> running -> succeeded
                   -> failed
                   -> uncertain
```

`(job_id, output_index)` is unique. A terminal output never returns to a
non-terminal state. The submission kernel creates outputs but does not advance
them; only the fenced economic reducer owns output state transitions.

### Provider submission

```text
prepared -> running -> succeeded
                    -> failed
                    -> uncertain
prepared            -> canceled
```

- `prepared` means no executor has committed launch authority.
- `running` is the irreversible side-effect boundary. Loss of evidence after
  this point resolves to `uncertain`, never to an automatic retry.
- terminal states are immutable.
- `canceled` is a proven pre-launch abandonment and carries no provider side
  effect.

### Executor execution

```text
prepared -> leased -> running -> succeeded
                              -> failed
                              -> uncertain
                 \-> canceled
```

The executor execution is a separate row from the provider submission.
`leased` is a short consumer lease and may be reclaimed only before `running`.
Its epoch is not the worker lease epoch. Every `start`, `heartbeat`, and outcome
transition compares submission ID, executor execution ID, owner, and epoch.

## 4. Required Invariants

1. A job has at most one output for each output index.
2. An output has at most one submission and one executor execution.
3. Preparing the same durable command returns the same identities, including
   after an unstarted worker lease is requeued.
4. Command hash and output count are derived from the locked durable payload;
   callers cannot self-report either value.
5. Submission preparation validates the current worker lease and attempt in the
   same PostgreSQL transaction.
6. No provider process may start before the prepared submission transaction
   commits.
7. Only a `prepared` or expired `leased` executor execution may be claimed.
8. A `running` or terminal executor execution is never automatically claimed
   again.
9. Start requires a locked, unexpired running work lease; after launch, outcome
   evidence is fenced by the executor lease and can survive worker loss.
10. A completed submission is not a completed customer job until artifact,
    metering, rating, ledger, quota, job state, and outbox settlement commit.
11. Timeout is an observer result, not proof that a provider side effect did not
    occur.
12. Ambiguous execution retains quota and future monetary holds until evidence
    or an audited manual decision resolves it.
13. Tenant, provider, and model are frozen on the submission and protected by a
    composite foreign key to the accepted job.
14. Every orchestration attempt attachment is append-only and has composite
    foreign keys to the same submission, job, work item, execution, and epoch.

## 5. Transaction Boundaries

### Prepare submissions

Lock the current work item, attempt, and durable payload; derive the canonical
command hash and output count; create stable outputs, submissions, executor
executions, and attempt attachments; then commit. IDs are generated by the
application but semantic uniqueness is enforced by PostgreSQL constraints.

### Start executor work

Claim a prepared executor execution with `FOR UPDATE SKIP LOCKED`, issue an
independent executor lease epoch, and commit. `executord` then locks and verifies
the current unexpired running work before a fenced
`leased -> running` transition immediately before handing launch authority to
the durable runner. Every claim includes a provider and command-schema scope;
the eventual Unix-socket protocol additionally authenticates the executor
principal and account-pool capability.

### Record outcome

Provider evidence and a submission-bound durable result manifest are committed
with a `running -> succeeded` transition. This does not settle the customer
output. A definite rejection may become `failed`. An expired running executor
lease is reconciled to `uncertain`; it is never reclaimed or retried.
Repeating the identical terminal outcome returns the committed result, covering
lost PostgreSQL COMMIT acknowledgements; a different replay is a conflict.

An expired `leased` executor whose work is already terminal is reconciled to
`canceled`. A leased execution cannot heartbeat; only a running executor can
extend its independent lease.

### Settle output and reduce job

The economic reducer will lock rows in one documented order, deduplicate every
fact by semantic key, and atomically publish output/job state with quota,
billing, artifact metadata, events, and outbox records.

## 6. Economic Kernel Dependency

The submission kernel deliberately lands before the full ledger, but the
executor cutover must not bypass the economic kernel. Before production traffic
uses `executord`, Phase 1 adds:

- immutable output-scoped price quotes;
- quota and monetary reservation slices;
- provider receipts and immutable metering facts;
- rated usage using integer micros and frozen price versions;
- balanced double-entry ledger postings;
- a fenced output settlement and job reducer.

Provider cost and customer charge are separate facts. A failed or free customer
outcome must not erase known provider cost.

## 7. Process Migration

1. Add output and submission schema plus PostgreSQL conformance tests.
2. Add fake-executor application ports for prepare, claim, start, outcome, and
   reduction tests.
3. Add `executord` and a durable runner/spool with `start_or_attach`.
4. Move Codex process creation out of `workerd` and into the runner.
5. Add production Linux cgroup/namespace isolation and account-pool separation.
6. Add crash injection at every commit and process boundary.
7. Only then enable the new path for production Codex traffic.

The rollout is expand/contract. Older binaries must remain able to read existing
jobs while new jobs are feature-gated onto the submission kernel. Restricted
API keys, new provider adapters, and generalized billing remain disabled during
mixed-version operation.

## 8. Verification Gate

The Codex Phase 1 gate is not satisfied until all of the following are proven:

- concurrent prepare calls return one stable identity set;
- concurrent executor claims have one winner;
- stale worker and executor leases are fenced;
- killing gateway or workerd does not duplicate a provider invocation;
- killing the executor supervisor attaches to the same durable runner;
- losing runner evidence fails closed as uncertain;
- idempotent API replay returns the committed result without invoking Codex;
- partial output facts survive a later output failure;
- quota, rating, ledger, and artifact settlement is atomic and balanced;
- hostile prompts cannot read another account or host credential;
- the real Images API smoke traverses gateway, PostgreSQL, workerd, executord,
  isolated runner, Codex, artifact storage, and fenced settlement.

## 9. Claims We Do Not Make Yet

The submission schema alone does not provide persistent child supervision,
credential isolation, cross-host recovery, exactly-once provider execution,
partial billing, or hostile multi-tenant isolation. Codex has no upstream
idempotency receipt that proves a lost invocation did or did not run. When the
system cannot prove no side effect occurred, its safe result is `uncertain`.
