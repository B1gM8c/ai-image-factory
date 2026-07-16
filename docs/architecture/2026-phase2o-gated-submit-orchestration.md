# Phase 2O: Gated Provider Submit Orchestration

Date: 2026-07-16

Status: implemented and workspace-verified as inactive provider infrastructure.
No Codex quota, Dreamina, Grok, or other external provider is activated by this
phase.

## Decision

The PostgreSQL-elected submit orchestrator now composes the Phase 2M durable
provider journal with the Phase 2N gated CLI process protocol. The composition
uses one statically dispatched port:

```text
ProviderSubmitDriver
  prepare before durable dispatch release
  dispatch after durable dispatch release
  recover an already released attempt
```

Every existing `RemoteTaskProvider` receives a blanket direct-driver
implementation, preserving its prior behavior. A remote CLI uses
`GatedCliSubmitDriver<C>`, where `C: GatedCliSubmitCodec` owns only:

- provider ID;
- canonical provider command to pinned CLI projection; and
- bounded stdout receipt to canonical `PendingOperation` decoding.

The codec does not own PostgreSQL state, scheduling, quota, billing, process
identity, release authority, timeout enforcement, or recovery.
Its methods receive only the frozen intent/context and canonical provider
command needed for command projection, or the intent/command and bounded stdout
needed for receipt decoding. Journal paths, launch nonces, and scheduling
budgets are not exposed to the codec.

There is no runtime provider registry, boxed future, message broker, second
queue, distributed lock, or workflow engine. Driver selection is ordinary Rust
generic static dispatch.

## Required Ordering

The side-effecting path is:

```text
PostgreSQL acquire_submit elects one sending attempt
  -> Phase 2M prepare and commit_launch
  -> driver.prepare builds one immutable process request
  -> runner publishes durable helper and blocked-child identity
  -> caller observes GatedCliReady
  -> Phase 2M dispatch-released is fsynced
  -> Phase 2N process-dispatch-released is fsynced
  -> the gate execs the provider CLI
  -> runner publishes bounded terminal evidence
  -> codec decodes a canonical pending receipt
  -> Phase 2M publishes receipt or failure evidence
  -> PostgreSQL records receipt/failure and attaches the task
```

The Phase 2M release must precede the process release. Reversing that order
would allow a provider call without durable submit authority. Starting the
runner only inside `RemoteTaskProvider::submit` would create the opposite gap:
the journal could be released, the daemon could die before spawn, and recovery
would have neither a unique process nor permission to launch another one.

## Ownership Boundaries

`provider_tasks/submit_driver.rs` owns:

- the generic `ProviderSubmitDriver` lifecycle;
- the immutable owned driver call; and
- the blanket direct-provider compatibility implementation.

`provider_tasks/orchestrator.rs` remains the only component that:

- acquires PostgreSQL submit authority;
- creates or reopens Phase 2M launch/release authority;
- decides whether a driver may prepare or dispatch;
- imports accepted, rejected, unknown, or quarantined evidence; and
- attaches a known remote operation.

`provider_tasks/remote_submit/driver.rs` owns:

- digest-pinned `remote-submit-runner` startup;
- process request preparation;
- ready, running, terminal, and lost-state observation;
- release of an already authorized gate;
- orphan cleanup requests; and
- effect-certainty normalization around process evidence.

`provider_tasks/remote_submit/process/` remains provider-neutral. It never
parses provider receipts or accesses PostgreSQL.

## Recovery State Machine

| Phase 2M state | Process state | Allowed action |
| --- | --- | --- |
| launch committed | no helper | verify and start one runner |
| launch committed | starting or ready | wait or reuse the same gate |
| launch committed | lost, never released | record no remote effect |
| dispatch released | ready | release that exact gate |
| dispatch released | running | wait; never launch another CLI |
| dispatch released | terminal | decode and import evidence |
| dispatch released | lost after process release | clean the identity-fenced target and record unknown effect |
| terminal | any | replay Phase 2M evidence; do not consult the provider again |

The driver has a process-local startup mutex only to coalesce concurrent
verification and spawn attempts in one orchestrator instance. It carries no
durable authority and no submission registry. After a crash, the filesystem
journal and PostgreSQL remain the only recovery sources.

The filesystem root path is revalidated against the journal's open directory
device/inode before it is handed to the process driver. This prevents an
ordinary path replacement from silently splitting Phase 2M and Phase 2N into
different directories.

## Failure Semantics

Before Phase 2M release, process or codec preparation failures are normalized
to `NoRemoteEffect`. Before recording such a failure, the orchestrator
re-observes the journal: if another concurrent caller already released or
terminalized the attempt, recovery wins over the local preparation failure.

After Phase 2M release:

- a successful, untruncated exit may be decoded as `PendingOperation`;
- an invalid receipt is `UnknownRemoteEffect`;
- timeout, absolute deadline after exec, signal exit, residual descendants, or
  lost released evidence is `UnknownRemoteEffect`;
- a durable terminal proving that exec never started is `NoRemoteEffect`; and
- no automatic resubmit is permitted for an unknown effect.

The direct-provider blanket driver retains the existing bounded timeout and
unknown-effect behavior.

## Performance Model

The orchestrator adds no dynamic dispatch or broker hop. A direct provider adds
only an owned `Arc` to the canonical command and the existing timeout wrapper.

The gated path adds:

- one command projection on a blocking worker;
- one runner executable SHA-256 verification per actual startup;
- one harmless blocked process before provider exec;
- bounded 10 ms recovery observation;
- fixed-size evidence files and captured streams; and
- one process-local mutex acquisition under concurrent startup.

The mutex coalesces normal same-process callers to one verification and one
spawn. Cross-process races remain fenced by the durable helper lock. These are
design properties, not production throughput claims; mixed-account benchmark
evidence is still required.

Pre-release preparation is bounded by the remaining PostgreSQL deadline budget.
When that budget expires, the driver requests identity-fenced termination of
the still-blocked child and records a no-effect failure.

## Verification

Real PostgreSQL and process tests prove:

- 32 concurrent orchestrator calls produce one fake-CLI side effect and replay
  the attached task after restart;
- a runner-lock loser cannot race a false PostgreSQL rejection against the
  winning helper;
- after Phase 2M release but before process release, restart releases the same
  ready gate, imports its receipt, and does not launch another CLI;
- after a durable CLI receipt but an injected PostgreSQL receipt transaction
  failure, restart attaches from durable evidence without a second CLI call;
- an intentionally slow local command projection that exhausts the pre-release
  database budget cannot release or invoke the fake provider CLI;
- direct-provider dispatch, timeout, unknown-effect, receipt recovery,
  quarantine, and released-future abort behavior remain unchanged; and
- the lower process suite still proves pre-release zero effect, bounded output,
  deadline hard kill, wall timeout, residual group cleanup, and released orphan
  cleanup.

The real Codex image smoke remains ignored because it can consume quota.

## Evidence Basis

PostgreSQL row locking and transaction-end constraint behavior remain the
authoritative dispatch boundary:
<https://www.postgresql.org/docs/18/explicit-locking.html#LOCKING-ROWS> and
<https://www.postgresql.org/docs/18/sql-set-constraints.html>.

Rust return-position `impl Trait` in traits provides static dispatch without a
boxed future or object-safe provider registry:
<https://doc.rust-lang.org/reference/types/impl-trait.html#return-position-impl-trait-in-traits-and-trait-implementations>.

Tokio documents that `spawn_blocking` isolates blocking filesystem and digest
work from asynchronous executor workers:
<https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html>.

The process durability and containment evidence basis remains in Phase 2N.

## Remaining Activation Gates

1. Implement one provider-specific composition adapter, initially Dreamina,
   without enabling credentials or external calls.
2. Add Linux per-submit cgroup v2 containment and prove `populated 0`; define
   the macOS production supervisor boundary.
3. Freeze the deployed helper identity and close or explicitly accept the
   remaining same-UID path-to-exec race.
4. Add journal retention only after PostgreSQL terminal convergence and an
   audited safety interval.
5. Run production-equivalent process, filesystem, mixed-account, and receipt
   decoding benchmarks before any activation or SOTA claim.
