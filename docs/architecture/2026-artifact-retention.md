# Bounded Artifact Retention

## Scope

Migration `0037_artifact_retention` governs customer response objects and the
executor artifact copies that back them. It does not delete jobs, idempotency
bindings, response projections, artifact metadata, authentication attribution,
usage, receipts, rating, or ledger facts. Those rows are the durable economic
and audit record.

Runner journals are execution-local recovery evidence and require a separate
janitor that proves terminal database convergence and exclusive runner-lock
ownership. They are deliberately not deleted by the platform artifact worker.

## State Machine

```text
available --database expiry--> expired --lease claim--> deleting --delete ack--> deleted
                                  ^                         |
                                  +-------- retry ----------+
```

The policy deadline is the response linearization point. Reads compare
`expires_at_ms` with the PostgreSQL clock, so a stalled reconciler cannot extend
the retention contract. `available -> expired` durably records that deadline
for cleanup. New image and edit replays return
`410 idempotency_result_expired`; known video content returns
`410 artifact_expired`. A missing or corrupt object before the deadline remains
an integrity/storage failure. The loader rechecks retention after a read
failure so a concurrent valid deletion cannot leak as a `500` or `503`.

The snapshotted read-drain duration separates logical expiry from physical
deletion and is anchored at `expires_at_ms`, not at the time a backlog happens
to be discovered. A read that observed `available` may finish during this
interval without adding a write lease to the HTTP hot path.

## Ownership And Crash Recovery

Due rows are selected with the PostgreSQL clock and `FOR UPDATE SKIP LOCKED`.
The claim transaction increments `lease_epoch`, stores an owner and expiry, and
commits before file I/O. The lease starts only after the immutable artifact
manifest has been loaded and validated. Invalid manifests are durably deferred
with an error code so one poison row cannot block later work. Completion and retry require exact
`(job_id, owner, lease_epoch)` equality. Missing files are successful idempotent
deletes. A crash after any unlink is recovered by reclaiming the expired lease
and repeating deletion; a stale owner cannot finalize a newer lease.

Customer object keys are derived from immutable artifact IDs. Executor object
keys and storage namespaces are independently reconstructed and validated from
append-only authority rows before deletion. Both customer and executor roots
remain bound to opened private directory descriptors; shard traversal uses
`openat` with `O_NOFOLLOW`, and deletion uses `unlinkat` plus directory `fsync`.
A missing or replaced shard is deferred as unavailable instead of following a
new path or acknowledging deletion. No database identity or accounting row is
removed.

The filesystem store persists a private UUID marker at the artifact root and
uses it as the executor namespace across process restarts and filesystem device
changes. Legacy device/inode namespaces remain cleanup-compatible only when the
authority object key, byte size, and SHA-256 all match the private object before
unlink. Directory descriptor checks still fail closed on in-process root or
shard replacement.

## Policy And Operations

`artifact_retention_policies` contains the active policy. Every response copies
the policy version, retention duration, read-drain duration, and retry delay
into `job_artifact_retention`, so later policy changes cannot rewrite old data.
The initial policy was 24 hours retention, 15 minutes read drain, and 60 seconds
retry delay. Migration `0112_artifact_retention_30_minutes` changes newly
created response snapshots to 30 minutes retention, 1 minute read drain, and
60 seconds retry delay. Existing response rows keep their original policy
snapshot, so deploying a shorter policy cannot immediately delete historical
results.

`reconcilerd` performs bounded work using `RECONCILER_BATCH_SIZE`; artifact
leases use `RECONCILER_ARTIFACT_CLEANUP_LEASE_MS`. Artifact retention runs as
an independent reconciliation branch, so a persistent work, orphan, or input
cleanup failure cannot starve expiry. Logs report expired, claimed, deleted,
and failed counts. Multiple replicas may compete safely.

## Acceptance Invariants

1. Same key and same request returns the retained response or permanent 410;
   it never creates another job, provider call, or charge.
2. Same key and a different request remains 409 after artifact expiry.
3. Logical expiry always commits before physical deletion.
4. Customer and executor bytes disappear only for succeeded, projected jobs.
5. Partial deletion and crash-before-finalize are replayable.
6. Unknown, cross-tenant, missing, and corrupt live objects are not reported as
   normal expiry.
7. Economic and identity rows are byte-for-byte unaffected by retention.
