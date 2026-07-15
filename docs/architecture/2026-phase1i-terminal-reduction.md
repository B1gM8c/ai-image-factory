# Phase 1I: Canonical Terminal Reduction

Status: the canonical terminal queue and leased read model are implemented.
Customer artifact publication, economic settlement, parent aggregation, and
public Images V2 routing remain disabled.

## 1. Authority Boundary

The reducer never accepts a caller-created provider receipt or a process return
value. Its only input is the immutable executor resolution graph:

```text
runner observation
  -> executor resolution decision
  -> executor and provider terminal projections
  -> executor terminal reduction queue
  -> leased canonical terminal read model
```

`executor_resolution_decisions` remains the fact authority. The reduction queue
contains only the decision identity, resolved state, and reducer lease state. It
does not duplicate prompts, prices, artifact bytes, customer amounts, or error
interpretation.

## 2. Atomic Queue Publication

Migration `0015_executor_terminal_reductions.sql` installs an `AFTER UPDATE`
trigger on `provider_submissions`. When a V2 submission becomes terminal in the
same transaction that projects its resolution decision, the trigger inserts one
`ready` reduction row. A deferred constraint trigger rejects commit unless the
terminal submission has exactly one matching queue identity.

Existing V2 terminal submissions are backfilled during migration. Composite
foreign keys bind every queue row to both the provider submission and the exact
decision state. Queue identities are immutable and rows cannot be deleted or
truncated.

## 3. Reducer Lease

`ExecutorTerminalStore::claim_terminal(owner, lease_ms)` performs one
`FOR UPDATE SKIP LOCKED` claim. A candidate must still satisfy all of these
conditions:

- job contract is V2;
- executor execution, provider submission, and resolution decision agree;
- parent work is `awaiting_executor` and its attempt is `handed_off`;
- the exact attempt attachment exists;
- customer output is not terminal yet;
- the reduction is ready or its previous lease has expired.

The claim increments a monotonic reducer epoch. A live lease cannot be claimed
again. Expired work can be reclaimed exactly once, and heartbeat requires the
same owner, epoch, decision, and live deadline.

## 4. Trusted Read Model

The returned `ExecutorTerminalLease` carries only identities required for the
next reducer transaction: submission, executor, decision, output, job, tenant,
work, worker attempt, and reducer lease fences.

Success additionally requires the deterministic result manifest and immutable
artifact authority:

```text
manifest_id  = submission_id
authority_id = executor_execution_id
```

The authority includes storage backend, namespace, object key, SHA-256, byte
size, and media type. Failure, uncertainty, and cancellation carry only the
canonical decision error code and cannot carry a manifest or artifact.

## 5. Next Transaction Boundary

Artifact I/O must not hold economic database locks. The next reducer slice will:

1. claim one canonical terminal lease;
2. for success, read and verify the private executor object;
3. publish a deterministic customer object idempotently;
4. begin the final PostgreSQL transaction;
5. revalidate the live reducer lease and canonical decision;
6. persist the customer artifact and trusted provider receipt;
7. rate the output, settle its hold, and append ledger facts;
8. aggregate all outputs and atomically project work, attempt, job,
   idempotency, quota, response, and outbox state;
9. mark the reduction completed in the same commit.

Crash before step 4 leaves only an idempotent customer object. Crash during the
transaction leaves no economic or parent projection. Lost acknowledgement after
commit replays by immutable submission and artifact identities.

## 6. Verification

- A real executor success queues and reconstructs the exact decision, attempt,
  manifest, and artifact authority identities.
- A live reduction lease cannot be claimed twice.
- Twenty concurrent claims after expiry produce one epoch-two winner.
- The stale epoch cannot heartbeat after reclaim.
- Decision identity mutation and queue deletion are rejected.
- Fresh and concurrent migrations apply through version 15.

## 7. Activation Gate

The presence of a reduction queue does not make V2 externally available. The
gateway must continue admitting public traffic as LegacyV1 until deterministic
customer publication, trusted economic reduction, parent aggregation, replay,
and fake plus credentialed Codex API tests all pass.
