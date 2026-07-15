# Phase 2H: Atomic Provider Artifact Resolution

Date: 2026-07-16

Status: the database transition is implemented and PostgreSQL-verified.
End-to-end provider materialization is not implemented, and remote CLI providers
remain inactive until the remaining activation gates close.

## Scope

This phase closes the committed intermediate state between durable artifact
authority metadata and canonical executor success. It does not add a provider daemon,
activate Dreamina, call an external provider, change the public image API, or
introduce a second artifact store.

Before this phase, the database path was split into three independently
committed calls:

```text
publish deterministic artifact authority and result manifest
-> record artifact_ready provider observation
-> resolve canonical executor success
```

The second call cleared the poll lease and made the task unclaimable. A process
crash or lost dispatch between the second and third calls could therefore leave
the provider task at `artifact_ready` while the executor, submission, capacity,
and reducer remained at `provider_waiting`.

## Decision

Keep artifact I/O under the existing provider/account-scoped poll lease, and
make `record_observation(ArtifactReady)` perform the canonical resolution in the
same PostgreSQL transaction.

No materialization queue or second lease is added. For a provider whose terminal
query and download are repeatable, the current provider SDK contract makes that
the smallest database design for this transition:

- `RemoteTaskProvider::poll` receives an `ArtifactSink`;
- `PollObservation::Completed` contains a `DurableArtifactManifest` that the
  orchestrator must compare with the manifest finalized by its supplied sink;
- the executor object key, authority ID, and manifest ID are deterministic from
  the frozen execution and submission identities; and
- the filesystem store uses create-new/no-replace publication and verifies an
  existing object by digest and size on replay.

An independent materialization state and lease becomes necessary if an operation
descriptor returns a temporary or non-repeatable locator, if a terminal query
cannot be replayed after a crash, or if artifact transfer must outlive the
maximum poll lease. Dreamina's exact replay behavior has not yet been proven, so
it remains inactive and must satisfy this descriptor-level gate before using the
single-lease path. Adding a queue before that evidence would duplicate the poll
fence without closing a demonstrated contract.

The SDK alone does not yet make it impossible for an adapter to construct a
`Completed` value without using the supplied sink. The future unique
orchestrator must therefore wrap that sink as a one-shot, lease-scoped
capability, retain its finalized publication, and require exact identity and
metadata equality with the provider result before committing success.

The current filesystem executor path publishes to one deterministic key per
execution with no-replace semantics. That is sufficient only when replaying the
same remote operation is byte-stable. A stale epoch can otherwise publish bytes
`A` without authority while the current epoch obtains bytes `B`, leaving `B`
unable to occupy the key. Remote provider byte stability has not been proven.
Before activation, provider materialization must therefore either prove
byte-identical replay or use epoch staging/content-addressed immutable objects
and let the live database fence select one authority. Stale objects then require
bounded asynchronous garbage collection, not deletion by a worker that no
longer owns the fence.

## Commit Protocol

1. Claim one due task within a frozen `(provider_id, provider_account_id)`
   scope using the existing poll owner, epoch, expiry, and database clock.
2. Invoke the frozen provider context outside any database transaction. The
   future orchestrator must stream output into its controlled artifact sink and
   validate bytes, media type, digest, size, and object identity. `artifact_ref`
   remains untrusted opaque audit evidence; it is never parsed as a filesystem
   path or object key.
3. Publish the immutable authority and result manifest. The store returns a
   private-field `ProviderArtifactPublication` capability containing the exact
   manifest, digest, size, and media type. New publication locks the exact task
   row and checks the live poll fence. Exact acknowledgement replay first reads
   the already committed authority and returns it only when every immutable
   field matches, even when the caller's local lease snapshot is stale.
4. Record the append-only `artifact_ready` observation. In the same transaction,
   validate the deterministic manifest, insert the one resolution decision,
   terminalize executor and provider submission projections, release capacity,
   and enqueue the existing reducer.
5. If the commit acknowledgement is lost, replay the same event identity and
   publication capability. The store loads the exact observation, compares its
   manifest fingerprint, and verifies the already committed canonical decision
   instead of creating new evidence.

External artifact I/O never runs inside a PostgreSQL transaction.

## Crash Matrix

| Crash or ambiguity | Durable state | Recovery |
| --- | --- | --- |
| Before object publication | `provider_waiting`, live or expiring poll lease | heartbeat or reclaim and poll again |
| Object committed, no authority row | unreferenced immutable object | byte-stable replay may reuse it; otherwise the activated design must use epoch staging/content addressing and later GC |
| Authority commit acknowledgement lost | authority and manifest may exist | exact metadata replay returns the committed publication; a different payload conflicts |
| Authority committed, no observation | `provider_waiting` remains claimable | reclaim and replay the verified provider artifact flow |
| During artifact observation transaction | no partial database state commits | transaction rollback; task remains claimable |
| Artifact observation commit acknowledgement lost | task evidence and canonical success are both committed | exact event replay returns the same task and decision |
| Expired worker publishes late | database authority/observation is rejected; an object write may remain orphaned | current poll owner remains the database authority; activated object storage still needs epoch staging/content addressing and GC |
| Same object key with different bytes | no-replace store detects digest/byte mismatch | reject as integrity/conflict; never overwrite authority |

Once immutable artifact authority exists, contradictory failure or cancellation
evidence is rejected: verified success evidence has priority and must be
projected canonically. Before authority exists, the existing terminal
compare-and-set still chooses one canonical outcome.

## Database Invariants

Migration `0023_atomic_provider_artifact_resolution.sql` is a strict
schema-before-binary migration:

- it drains writers with explicit `SHARE ROW EXCLUSIVE` locks across the
  affected projections, while each `ALTER TABLE` still takes the stronger lock
  PostgreSQL requires for that table;
- it fails closed if any legacy `artifact_ready` task lacks canonical success;
- an `artifact_ready` observation records the exact manifest ID, digest, byte
  size, and media type and requires matching immutable authority and manifest;
- every terminal provider observation must project the task and canonical
  executor/submission result in the same transaction; and
- an `artifact_ready` task can commit only with a succeeded executor and
  submission projection.

The migration temporarily removes the old observation mutation-rejection
trigger only while holding the writer-drain locks, backfills already canonical
version-22 artifact observations from immutable authority, and recreates the
trigger before exposing any new constraint or index. PostgreSQL transactional
DDL restores the old trigger and removes all new schema objects if any later
statement fails.

Digest, size, and media type are intentionally repeated on the terminal
observation even though authority also stores them. They bind the append-only
event identity and payload hash to the exact publication fingerprint, so a
future replay or serialized worker command cannot substitute metadata merely
because the deterministic manifest ID is unchanged. This write amplification is
limited to one successful artifact observation per manifest; the partial unique
index enforces that cardinality.

The attachment replay path locks the parent projection first and reads immutable
task identity through an MVCC snapshot. This removes the previous
`allocation -> task` versus `task -> allocation` lock-order cycle without
weakening mutable-state checks on new attachment.

The explicit table-lock order starts with remote task/observation authorities
and follows the runtime terminalization order. Migration still requires drained
writers, a bounded `lock_timeout`, and a maintenance window. It performs full
preflight scans, an artifact-observation backfill, constraint validation, and a
transactional partial-index build. If these tables become large before rollout,
the migration must be staged with measured backfill and index-build budgets;
the current one-shot form is accepted only because remote providers are
inactive.

Online execution with provider writers is unsupported: runtime observation and
migration table-lock acquisition do not share one universal lock order and can
deadlock. Deployment must prove all provider writers are drained before running
0023, abort on `lock_timeout`, and retry only after the blocker is removed.

The migration does not guess how to repair legacy half-terminal evidence. Such
rows require an operator-reviewed drain before rollout because inventing success
would release capacity and create billable evidence.

## Cost Model

This phase adds no table, scheduler scan, materialization queue, or
materialization index. It adds four artifact fingerprint columns plus one narrow
partial uniqueness index to the append-only observation ledger. New authority
publication uses an exact row-locking fence lookup; acknowledgement replay adds
an exact authority lookup.
Artifact observation performs the resolution work that was already required,
but does it before the same commit instead of through a second API call and
transaction.

The successful first-commit path therefore removes the separate resolution API
and transaction while retaining bounded primary/unique-key lookups. This is a
code-path argument, not a production latency or SOTA claim.

Poll claim now materializes at most 64 ordered rows from the existing partial
scope index before applying `SKIP LOCKED`. A fully locked window returns empty
and defers the scope to a later scheduler round instead of scanning an unbounded
locked prefix. This bounds one call; cross-scope fairness remains the future
orchestrator's responsibility.

Poll and submit-recovery heartbeat now acquire their exact authority row before
reading `clock_timestamp()`. This adds one bounded primary-key lock/read to each
heartbeat, but prevents an old epoch from using a statement-start timestamp to
revive itself after waiting past absolute expiry. Removing that round trip would
require an equally verifiable server-side primitive and measured benefit.

## Verification

The PostgreSQL 18.3 integration suite covers:

- rejection and rollback of `artifact_ready` before authority publication;
- rejection of failure evidence after immutable artifact authority exists;
- rejection of a raw terminal observation without canonical projection;
- atomic task, executor, submission, capacity, and reducer resolution;
- exact authority and observation commit-ack replay with one observation and one
  decision;
- migration rollback for a real version-22 half-terminal task;
- successful fingerprint backfill for a canonical version-22 artifact task,
  restoration of append-only protection, and no schema residue after a forced
  late migration failure;
- concurrent attachment replay and poll claim without the former lock-order
  cycle;
- rejection of authority, observation, poll heartbeat, and submit-recovery
  heartbeat writes when their lease expires while waiting for a fence lock;
- a locked 64-row poll window returning empty without claiming row 65; and
- the existing submit, callback, deadline, and reconciliation race matrix.

The phase gate also requires formatting, Clippy with warnings denied, all
workspace tests against real PostgreSQL, and independent adversarial review.

## Evidence Basis

PostgreSQL documents that deferred constraint triggers run at transaction end
and roll back with the transaction on error:
<https://www.postgresql.org/docs/18/trigger-definition.html>.

PostgreSQL recommends acquiring locks in a consistent order to avoid deadlocks:
<https://www.postgresql.org/docs/18/sql-lock.html>.

AWS describes caller-provided identity and semantically equivalent replay as
the basis of safe idempotent retries:
<https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/>.

For a future S3 backend, the equivalent immutable publication primitive is a
conditional write such as `If-None-Match: *`, not an unconditional overwrite:
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html>.

## Remaining Activation Gates

1. Add the single submit/recovery/deadline/reconciliation/poll orchestrator. It
   must be the only external side-effect caller, maintain poll heartbeats while
   artifact I/O is active, own a one-shot lease-scoped sink, compare the
   provider's `Completed` manifest with the sink's finalized publication, and be
   the only caller allowed to publish artifact authority. The database method
   validates descriptors; it does not independently read object bytes.
   Long artifact transfers need a separate process-local semaphore so they do
   not consume every lightweight poll slot.
2. Persist operation descriptor identity and bind provider, operation,
   descriptor, adapter, submission, and idempotency identities. Each descriptor
   must declare and prove whether terminal query/download is repeatable; a
   temporary or non-repeatable locator requires a distinct materialization
   state and lease.
3. Make the earlier submit-recovery claim/defer commands exactly replayable.
4. Close the object-publication race with proven byte-stable replay or
   epoch-staged/content-addressed immutable keys plus bounded orphan GC. The
   current deterministic execution key is not an activation-ready contract for
   unproven remote output.
5. Harden each CLI download boundary with a fresh private directory, bounded
   bytes/pixels, regular-file and link checks, MIME decoding, cancellation, and
   absolute deadlines.
6. Run the mixed-load million-row latency, lock, buffer, WAL, bloat, and fairness
   benchmark before activating a remote CLI provider.

The current PostgreSQL integration fixtures prove metadata and state-machine
invariants, not that a provider object physically exists. End-to-end object
verification belongs to the orchestrator activation gate above.
