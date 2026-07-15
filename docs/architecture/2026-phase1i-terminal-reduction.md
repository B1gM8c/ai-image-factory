# Phase 1I: Canonical Terminal Reduction

Status: the canonical terminal queue, customer artifact publication, output
economics, quota slices, and parent aggregation are implemented. The standalone
reducer daemon and public Images V2 routing remain disabled.

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

## 5. Final Transaction Boundary

Artifact I/O does not hold economic database locks. The implemented completion
path performs:

1. claim one canonical terminal lease;
2. for success, read and verify the private executor object;
3. publish a deterministic customer object idempotently;
4. begin the final PostgreSQL transaction;
5. revalidate the live reducer lease and canonical decision;
6. derive and persist an `executor.resolution.v1` receipt from the canonical
   decision rather than accepting caller receipt data;
7. persist the customer artifact, rate the output, settle its hold, append
   ledger facts, and apply one quota slice;
8. aggregate all outputs and atomically project work, attempt, job,
   idempotency, quota, response, and outbox state;
9. mark the reduction completed in the same commit.

Crash before step 4 leaves only an idempotent customer object. Crash during the
transaction leaves no economic or parent projection. Lost acknowledgement after
commit replays only for the same reducer owner, epoch, decision, receipt, and
artifact identities.

Migration `0016_terminal_reduction_completion.sql` binds every completed row to
its completion owner, trusted receipt, optional customer artifact, and quota
reservation. Deferred constraints verify receipt evidence, immutable artifact
authority, economic meter/rating/hold state, quota totals, and the parent
projection at outer commit. Standalone V2 receipt settlement is rejected unless
the same commit links it to a completed canonical reduction.

Success commits one charged quota unit. Failed or proven no-effect output
commits one released unit. Uncertain output commits neither, keeps its monetary
hold, and makes the parent uncertain only after every output has been reduced.
The parent cannot become terminal before all reductions complete.

Pre-launch `canceled` is not a public cancellation protocol. It previously
required an already-terminal parent, which violates the V2 reducer ownership
boundary. Migration 0016 rejects that early parent transition. Future request
cancellation must collect auditable evidence and enter this reducer only after
proving provider no-effect.

## 6. Verification

- Success atomically commits receipt, meter, rating, artifact, quota, response,
  parent state, job event, and outbox; exact replay changes no identity.
- A deferred outer-commit failure rolls back every database effect while the
  deterministic customer blob remains reusable.
- Twelve duplicate same-lease completions persist one result.
- Concurrent completion of three outputs finalizes the parent exactly once.
- Multi-output success does not finalize early; partial failure and uncertainty
  preserve exact charge, release, artifact, and hold totals.
- Expired reducer leases and forged replay owners are fenced.
- Standalone V2 economics cannot create a receipt before reducer completion.
- Fresh and concurrent migrations apply through version 16.
- `cargo test --workspace --all-targets` passes 356 tests with one credentialed
  Codex smoke intentionally ignored; workspace clippy passes with warnings
  denied.

## 7. Activation Gate

The internal reducer kernel does not make V2 externally available. The gateway
must continue admitting public traffic as LegacyV1 until a standalone reducerd
drives claim, publication, heartbeat, and completion; the public API selects V2;
and fake plus credentialed Codex API tests prove the complete process topology.
