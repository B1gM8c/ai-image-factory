# Phase 2K: Immutable Provider Operation Binding

Date: 2026-07-16

Status: operation descriptors, submit idempotency semantics, provider command
identity, execution context, and remote evidence are durably bound and
PostgreSQL-verified. Remote CLI providers remain inactive. This phase closes a
storage and type-boundary prerequisite; it does not authorize external provider
execution.

## Scope

Phase 2K prevents a provider submission from being reinterpreted after handoff.
It freezes the operation contract selected by the execution profile, binds one
canonical per-output provider command identity to the submit intent, and returns
the same execution context to submit recovery and remote-task polling.

It adds no public image API behavior, provider activation, scheduler daemon,
billing rule, network call, or CLI process launch. The existing Codex inline
path remains the only active provider path.

## Decision

The durable binding is a direct immutable snapshot on the profile and
submission, not a runtime registry lookup:

```text
operation id + descriptor revision + descriptor sha256_v1
+ completion mode + idempotency mode
+ provider/account/model + execution profile/adapter
+ credential identity/auth digest + resource policy
+ executor fence + submission idempotency identity
+ canonical provider command digest + provider timeout
= execution_binding_sha256
```

This deliberately duplicates a small fixed descriptor snapshot. Poll and
recovery must not re-resolve mutable configuration after submit. A registry
would add a hot-path join, another lifecycle, and another consistency boundary
without removing the need to freeze the selected version.

The profile remains the provisioning authority. A version-2 submission must
copy the exact enabled profile snapshot in the same transaction. Database
triggers reject missing, partial, mismatched, or later-mutated snapshots.

## Canonical Descriptor

`OperationDescriptor::canonical_sha256_v1()` hashes every current semantic
field with:

- a versioned domain separator;
- stable string codes for enum values;
- explicit field names; and
- 64-bit length prefixes for both field names and values.

The persisted column is explicitly named
`operation_descriptor_sha256_v1`; the algorithm version is not hidden in an
unversioned API. Golden digests pin the active Codex generation and edit
descriptors. Field-mutation tests cover identifiers, schemas, media and
operation kinds, remote controls, artifact mode and byte limit, streaming,
idempotency, billing, and official-parameter kind/schema/passthrough.

Adding a descriptor field requires adding it to the v1 mutation test or
introducing a new hash version. It must never silently change the meaning of an
existing persisted digest.

## Command And Execution Identity

The task store no longer accepts an arbitrary SHA-256 string from its caller.
`RemoteTaskSubmitReservation` accepts the private-byte
`ProviderCommandIdentity` emitted by `SingleOutputCommand`. The store encodes
that identity once, combines it with the frozen database rows, and persists the
resulting execution binding before submit authority can be acquired.

Exact reservation replay must match command identity, timeout, executor fence,
and the complete execution binding. Receipt, failure, attach, recovery, and
poll paths carry or reload the same binding. Evidence from another binding is a
conflict even when provider, account, or remote operation text happens to
match.

This type boundary removes a free-form digest parameter. It cannot prove that
an external CLI obeyed a command. The future sole orchestrator must build the
canonical command once, reserve with that exact identity, and pass the same
command object to the adapter. Remote activation remains blocked until that
single side-effect path exists and is verified.

Poll and submit-recovery leases also contain a private SHA-256 authority seal
over the durable submission/execution identity, provider scope, binding, owner,
and lease epoch. Cloning a lease and replacing its public task or intent with a
second submission invalidates the seal before any database write. This closes
cross-submission lease splicing without adding a database query or heap
allocation.

## Submit Idempotency

`SubmitIdempotency` is a private representation with two constructors:

- `submission_bound()` means the platform permits one immutable submit attempt
  and recovers only from durable evidence;
- `provider_token(token)` represents a provider-enforced token and validates
  length and control characters before construction.

Direct construction cannot bypass token validation. The provider SDK exposes
idempotency only to `submit`; `poll` and `cancel` cannot receive or accidentally
reuse it by type. Poll context has no public accessor for the submission key.
The conformance fake records the complete invocation, command identity, observed
mode, and exact token.

No execution profile currently uses `provider_token`. That mode may be enabled
only after the provider's official contract proves token namespace, retention,
parameter-mismatch behavior, and replay semantics. A locally generated token
does not create provider-side idempotency. The SDK does not infer an operation's
expected mode; the future sole orchestrator must compare the frozen descriptor
mode and construct the matching submit value. The current database gate admits
only `submission_bound` remote intents.

## Migration And Compatibility

Migration `0026_immutable_provider_operation_binding.sql` is a pre-activation
migration. It must run before any remote provider has produced durable submit,
recovery, or task evidence and before a non-Codex execution profile is
provisioned. Those append-only histories cannot be described as drainable; the
migration rejects them because this repository has not activated remote
providers yet.

Deployment order is:

```text
stop old workerd/executord writers
-> verify no active executor submission and no remote history
-> apply 0026
-> start the compatible binaries
```

Mixed old/new binaries and zero-downtime deployment are not supported by this
migration. A local five-second `lock_timeout` bounds lock acquisition failure.
All required tables are requested in one `ACCESS EXCLUSIVE` statement before
the gates are evaluated, so committed old writers are observed after the lock.
The active-execution gate uses the existing partial indexes for
`prepared`/`leased` and `running`; it does not join or scan terminal submission
history. `provider_waiting` is impossible after the earlier no-remote-history
gate succeeds.

The migration does not rewrite historical `provider_submissions`. Existing
terminal rows receive a metadata-only constant default
`operation_binding_version = 1`; descriptor columns remain null. A `NOT VALID`
row check avoids a historical table scan while enforcing complete, valid
version-2 bindings on every new insert or update. The insert trigger separately
rejects any new version-1 submission. Version-1 terminal handoff acknowledgement
replay accepts only matching durable command identity and terminal parent
states; it can never re-enter execution or remote submit.

Profiles are a bounded configuration set, so known Codex profiles are updated
in place and made non-null. Unknown profiles fail closed because their operation
semantics cannot be inferred safely.

The submit-intent transition trigger uses explicit comparisons of the only
fields each transition may change. It does not convert full rows to JSONB on
the submit hot path.

Before migration, release operations must verify the new binaries and schema on
a production-sized clone and create a tested database restore point. A failure
before commit rolls back transactional DDL. After schema 26 commits, rollback
means forward repair or restoring the database together with the previous
binary; starting a version-25 binary against schema 26 is not supported. This
phase tests transactional rollback, not disaster-recovery restore execution.

## Cost Model

The reserve path adds one SHA-256 over a small fixed set of already-loaded
fields and one hexadecimal encoding. It adds no registry table, queue index,
global account lock, or network hop. Lease validation adds one fixed SHA-256 and
no allocation before a database mutation.

Poll and recovery currently return the full frozen context. This costs several
small strings and roughly hundreds of bytes per claim, but changing the shape
without a mixed-load benchmark would trade evidence integrity for an unproven
optimization. A future measured optimization may use fixed 32-byte digest
types or a narrower internal projection without reintroducing mutable lookup.

These are structural bounds, not throughput results. Production activation
still requires measurements of p50/p95/p99 latency, allocations, CPU, lock wait,
rows scanned, buffer hits, WAL, replication lag, bloat, and fairness under
provider/account mixed load. This phase makes no SOTA, zero-overhead, or
zero-downtime claim.

## Verification

The PostgreSQL 18.3 integration suites cover:

- known Codex profile backfill and restored profile immutability;
- atomic rejection of unknown profiles, active executions, and any remote
  history;
- terminal version-1 preservation with successful idempotent handoff replay;
- rejection of inline profiles entering the remote lifecycle;
- exact version-2 profile/submission snapshot matching;
- raw SQL mutation rejection for profile and submission semantics;
- reservation conflict on changed provider command identity or timeout;
- submit, recovery, and poll returning the same frozen context;
- receipt, failure, and attach rejection across execution bindings;
- poll and recovery lease-splicing attacks across two live submissions;
- all submit deadline, task deadline, artifact authority, capacity,
  reconciliation, and bounded-claim regressions; and
- fresh and historical migration paths through schema version 26.

Provider contract, SDK, and conformance tests additionally cover versioned
goldens, field mutation, command and invocation validation, non-bypassable
submit tokens, both idempotency modes, and submit-only type isolation.

The local harness skips PostgreSQL tests when `TEST_DATABASE_URL` is absent. CI
and release gates must require a real PostgreSQL URL; a skipped run is not
database evidence.

## Evidence Basis

PostgreSQL documents that adding a column with a constant non-volatile default
stores the value in metadata and does not rewrite existing rows:
<https://www.postgresql.org/docs/18/ddl-alter.html>.

PostgreSQL documents that `NOT VALID` skips the existing-table scan while still
checking subsequent inserts and updates:
<https://www.postgresql.org/docs/18/sql-altertable.html>.

PostgreSQL documents table lock lifetime and explicit lock acquisition:
<https://www.postgresql.org/docs/18/sql-lock.html>.

PostgreSQL documents row-local `CHECK` constraints and recommends triggers or
relational constraints for cross-row rules:
<https://www.postgresql.org/docs/18/ddl-constraints.html>.

AWS's retry guidance distinguishes caller-provided request identity from the
service-side semantic guarantee required to make retries safe:
<https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/>.

## Remaining Activation Gates

1. Implement one orchestrator code path that owns canonical command creation,
   reserve/start, adapter submit, durable receipt/failure, attach, recovery,
   poll, cancel, and deadline lanes. Multiple replicas may compete only through
   existing database fences.
2. Bind the database absolute deadline to the external CLI process group and
   prove kill, reap, and orphan cleanup.
3. Prove provider-native idempotency semantics or durable helper evidence before
   permitting any ambiguous submit retry.
4. Complete byte-stable artifact staging, media decoding, private-directory and
   link checks, resource limits, credential isolation, and bounded orphan
   cleanup.
5. Validate strong late evidence, capacity release, and billing consequences for
   quarantined remote work.
6. Rehearse migration 0026 and benchmark the final orchestrator on a
   production-sized clone before enabling Dreamina, Grok, or another remote CLI
   provider.

Until these gates close, the public Codex image behavior remains unchanged and
all remote CLI providers remain inactive.
