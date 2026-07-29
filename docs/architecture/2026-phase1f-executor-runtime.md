# Phase 1F: Persistent Executor Runtime

Status: the persistent executor runtime, independent `executord`, Codex helper,
private process spool, immutable artifact authority, and append-only
observation/resolution path are implemented. Phase 1G also binds every new
submission to an immutable database execution profile and acquires durable
provider capacity before launch. Phase 1H atomically transfers V2 work from
the worker lease into executor ownership. Process-level PostgreSQL tests cover
one launch, restart attach, `SIGTERM` drain, owner-session loss, capacity
races, handoff rollback/replay, late evidence after executor lease expiry, and
the explicit default-off public V2 route. The production Images API defaults to
`LegacyV1`; operators may select V2 generation only after satisfying the
deployment-owned isolation gate in this document.

The checkpoint includes owner-and-scope singleton supervision, exact launch
context projection, output-scoped Codex command projection, idempotent database
start replay, active lease heartbeats, retryable evidence publication, and a
dirfd-bound filesystem journal with atomic no-replace markers. The helper uses
an execution-private Codex home and spool, persists PID plus OS start identity,
and launches one provider invocation for one output. Verified artifacts use an
isolated write-once namespace and an append-only PostgreSQL authority reference.

## 1. Scope

Phase 1F moves provider launch authority out of `workerd` and establishes the
runtime boundary for CLI-backed providers. It builds on the durable provider
submission and economic kernels; it does not replace either one.

The runtime has four responsibilities:

1. `workerd` validates and prepares immutable provider submissions.
2. `executord` claims provider-scoped executor work under an independent lease.
3. a durable runner implements `start_or_attach(executor_execution_id)` and
   owns the provider process and local spool;
4. `reconcilerd` fails closed when durable evidence cannot prove an outcome.

This phase does not claim exactly-once provider execution. Codex CLI does not
provide an upstream idempotency receipt. Ambiguous launch or result evidence is
therefore `uncertain`, never an automatic retry.

## 2. Process Boundary

```text
gateway -> PostgreSQL <- workerd
                    <- executord -> durable runner -> provider CLI
                    <- reconcilerd

durable runner -> submission-scoped spool -> artifact store
```

`workerd` never spawns a provider process on the V2 path. It may only prepare a
submission and atomically move the parent work to `awaiting_executor`. The
worker owner and deadline are cleared in the same commit. `executord`
does not settle customer balances. It records provider evidence and a durable
result manifest; the economic reducer performs customer settlement later.

## 3. Identity And Authority

The following identities are distinct:

- `execution_id`: one worker attempt;
- `submission_id`: one immutable provider submission for one output;
- `executor_execution_id`: stable runner start-or-attach identity;
- `executor_owner`: stable identity of one executord deployment or sandbox
  pool, not a random process incarnation;
- `executor_lease_epoch`: PostgreSQL fencing token for executor mutations.
- `execution_profile_id`: immutable provider, adapter, credential account, and
  resource-policy binding selected before submission preparation;
- `allocation_id`: durable provider-capacity ownership, equal to the executor
  execution identity for the current one-output runtime.

Only PostgreSQL may grant launch authority. A valid runner request contains the
database-bound executor lease and no client-controlled command, environment, or
credential material. Provider adapters are selected from the database profile,
not from public model names or provider/schema environment variables. The
executord process names a profile key and an opaque mounted-credential
reference; startup rejects any provider, command schema, adapter revision,
credential revision, reference, or immutable `auth.json` digest mismatch. The
same digest is checked again when credentials are copied into or reused from
the private spool.

Immediately before launch, executord resolves the immutable command through
`ExecutorLaunchContextStore`. The lookup requires an exact unexpired `running`
executor identity, owner, epoch, submission, output, provider, model, work item,
schema, and command hash. It uses PostgreSQL time and recomputes the canonical
command hash. The resulting context intentionally does not implement `Debug`:
it may contain a prompt and is transient authority data, never journal data.

A restarted executord may resume an unexpired `running` execution only when the
stored owner and scope match exactly. Resume does not change the owner, epoch,
or expiry. A `leased`, expired, differently owned, or differently scoped row is
not resumable.

## 4. Start-Or-Attach Contract

For one `executor_execution_id`, the durable runner must enforce:

1. journal the immutable execution identity and sandbox specification before
   provider launch;
2. serialize concurrent `start_or_attach` calls;
3. return the existing execution when the immutable specification matches;
4. reject conflicting specifications without launching anything;
5. publish process identity before reporting `running`;
6. write output into an execution-private spool;
7. fsync files and directories before atomically publishing terminal state;
8. report success only after every manifest object is durable and validated;
9. keep temporarily unavailable local or database evidence retryable without
   publishing an immutable terminal marker;
10. return `uncertain` only when durable evidence proves the provider effect is
    unknowable rather than merely unavailable;
11. never convert an internal runner/storage failure into a definite provider
    rejection.

The journal and spool must reject symlinks, traversal, non-regular files,
unbounded files, and ownership or permission mismatches. Existing directories
with wider permissions are rejected rather than repaired. Journal and database
terminal validation share the same byte limit, MIME allowlist, object metadata,
SHA-256, and error-code boundary. Process attachment must validate more than a
reusable PID; Linux deployments should bind the PID to a process start-time or
pidfd identity.

## 5. Lease And Crash Semantics

`executord` first imports pending late evidence, then resumes its own unexpired
running work, then claims prepared work whose parent is durably
`awaiting_executor/handed_off`. A fresh claim transitions
`prepared -> leased`; immediately before runner authority is handed over, it
transitions `leased -> running`.

While `start_or_attach` is active, executord heartbeats the independent executor
lease. A stale lease cannot publish a canonical outcome. Durable runner evidence
may be retained for reconciliation, but cannot bypass PostgreSQL fencing.
PostgreSQL enforces one-way executor and submission transitions, a write-once
launch owner and epoch, monotonic live heartbeats, and rejection of any attempt
to revive an expired lease under the same fence.

Terminal recording has one production entry point and one PostgreSQL
transaction. For a still-live fence it applies a bounded finalization grace,
then atomically validates artifact authority, appends the runner observation,
creates the canonical decision, updates both executor projections, and releases
capacity. A transaction that reaches the database after expiry records only
late evidence; reconciliation retains ownership of the conservative canonical
decision. There is no post-run heartbeat commit gap.

A resumed execution is not renewed before local attach evidence is found. If a
second process has the same configured owner but a different spool, the missing
launch marker is retryable and produces no heartbeat, observation, or canonical
resolution. A dedicated PostgreSQL advisory-lock session owns each
owner/profile/provider/schema/adapter tuple; executord verifies that exact backend session both
between and during executions and exits fail-closed when the session is lost.

The database `start` transition is itself idempotent for the exact same
execution, submission, owner, and epoch. This covers a committed `running`
transition whose acknowledgement was lost. It must not rewrite the original
start timestamp or accept a different owner or epoch.

Crash outcomes are deliberately asymmetric:

| Crash boundary | Recovery |
| --- | --- |
| before handoff commit | worker lease reconciliation may safely requeue; no executor identity is visible |
| after handoff commit, before executor claim | `awaiting_executor` is independent of the old worker deadline and remains claimable |
| after executor claim, before executor start | the expired executor lease may be reclaimed with the same identity and capacity allocation |
| after executor start, before runner journal | do not renew or relaunch; expiry becomes canonical uncertain |
| after journal, before provider launch evidence | attach only; missing local proof remains retryable until expiry |
| after provider launch | attach the same execution; never launch a replacement |
| after spool commit, before artifact/DB acknowledgement | retain success spool and replay publication before writing terminal |
| after executor lease expiry | append late evidence for audit and keep canonical state uncertain; never retry provider |

Provider capacity is separate from both worker and executor leases. A fresh
claim increments the immutable resource-policy revision counter and inserts a
held allocation in the same transaction as `prepared -> leased`. Reclaiming an
expired, unstarted executor reuses that allocation. Lease expiry, heartbeat
failure, daemon exit, and owner-session loss never release capacity by
themselves. A release requires a durable resolution decision; running expiry
also requires terminal runner evidence, while an abandoned unstarted lease may
release through its fenced canceled decision.

## 6. Internal Protocol

The first implementation keeps the daemon-to-runner API as an in-process Rust
port so state-machine tests do not depend on transport. A later Unix-domain
socket adapter must preserve the same types and add all of these checks:

- socket path and parent directory are not symlinks and are owned by the
  expected service account;
- filesystem mode denies access to unrelated users;
- peer credentials are checked (`SO_PEERCRED` on Linux);
- the authenticated principal is authorized for the provider/account pool;
- requests are length-bounded, schema-versioned, and reject unknown fields;
- no API bearer key, admin token, database URL, HMAC pepper, raw provider
  credential, prompt, upload, or artifact bytes enter protocol logs.

The socket is not a public API and does not reuse the official Images facade.

## 7. Implementation Slices

1. Add the `DurableRunner` port and `ExecutorDaemon` application state machine.
2. Add owner-and-scope-fenced resume of unexpired running executor leases.
3. Add the filesystem execution journal and atomic spool publication.
4. Add an `executord` binary with a stable configured owner and provider scope.
5. Move Codex process launch behind the runner and remove it from V2 workerd.
6. Connect provider evidence, output reduction, and economic settlement.
7. Add crash injection, hostile-input, concurrent attach, and real Codex tests.

Each slice is additive. All seven slices are implemented, while `LegacyV1`
remains the safe default. The public V2 process proof composes the real gateway,
PostgreSQL, handoff workerd, executord, `codex-runner`, reducerd, artifact
hydration, economics, and idempotent replay. Its release gate uses `n=2` and a
non-zero exact price so an output/job cardinality error or duplicate settlement
cannot pass invisibly.

The current `ImageGenerator` interface is job-level and may loop over `n`.
Provider submissions are output-level, so executord must not call that interface
with the original command. Each adapter must expose a trusted single-output
operation bound to `output_index`; otherwise an `n`-output request could launch
the provider `n * n` times.

The schema activation blockers are complete. Runner outcomes are first retained
as append-only observations under the immutable launch fence. An active or
expiry decision owns every terminal canonical transition, so late evidence
survives without allowing a stale executor to overwrite the chosen state. The
active observation and decision now commit together rather than exposing a
partially recorded terminal state.

The artifact-authority blocker is complete: a successful executor manifest can
contain only deterministic authority IDs, and PostgreSQL accepts it only after
the publisher has durably written and reread the isolated object, independently
derived its type, hash, and size, and committed its append-only authority row.

The current child protocol is a versioned, length-bounded private filesystem
request consumed by `codex-runner`; it rejects unknown fields, uses explicit
canonical executable paths and hashes, clears ambient environment, and copies
only a private `auth.json` into an execution-specific Codex home. The child uses
the restricted `PATH=/usr/bin:/bin`. Executord accepts a native
Codex binary or a script with an existing executable absolute shebang
interpreter. An `/usr/bin/env` wrapper must resolve its command in the
restricted path, so executord rejects `#!/usr/bin/env node` before claiming
work when `node` is absent. Malformed or unknown executable formats also fail
startup. This remains a durability boundary, not a hostile multi-tenant
security boundary. A same-UID
process can still race executable replacement or inspect another same-UID
process on common operating systems. Production activation therefore requires
a dedicated service identity plus an external container/cgroup/sandbox policy
that restricts executable mounts, process creation, network egress, credentials,
and access to gateway/database secrets.

Codex image sessions may delete their generated image before the outer CLI
process exits. The runner therefore keeps the normal `CliRuntime` output
contract as the primary path and concurrently observes one adapter-derived
`provider-output.*` filename through the already bound workspace directory FD.
The target must be absent before provider spawn. Every read uses `openat` with
`O_NOFOLLOW`, requires a current-user, single-link, non-special, non-writable
regular file, bounds bytes and decoded pixels, and rechecks file identity after
the read. Two identical snapshots establish a candidate, but observation
continues until process exit so a later stable version supersedes it. Blocking
file reads and decoding run outside Tokio workers. The candidate is accepted
only when the provider exits successfully and the primary contract reports
`Missing` or `NotFound`; failure, timeout, observer, wait, integrity, or process
group errors never publish captured bytes.

## 8. Activation Gates

Production V2 traffic remains disabled according to this gate matrix:

| Gate | Status | Evidence or remaining requirement |
| --- | --- | --- |
| concurrent prepare/claim/resume/attach preserve one identity | passed | PostgreSQL concurrency and process restart tests |
| workerd handoff is atomic and independent of worker lease | passed | migration 0014, commit-failure rollback, replay, reconciliation, and V2 workerd tests |
| stale worker/executor epochs cannot mutate canonical state | passed | database fencing and expiry tests |
| restart attaches without a second provider launch | passed | real executord/helper test, invocation count equals one |
| journal/spool/commit ambiguity fails closed and can replay | passed | hostile filesystem tests, late-evidence test, artifact retry test |
| owner singleton and session loss fail closed | passed | advisory-lock and backend-termination tests |
| terminal artifact, economics, quota, and parent reduction are atomic | passed | migration 0016, rollback/replay, partial/uncertain, stale lease, and concurrent completion tests |
| provider process identity and orphan cleanup | passed | nonce/inode binding and real helper-death cleanup test |
| credential pool, adapter revision, and resource policy are database-bound | passed | migration 0013, exact profile claim scope, journal binding, global capacity race tests |
| repeatable profile provisioning preserves operator kill switches | passed | `factoryctl provision-codex-profile`, private auth digest, concurrent exact/conflict and rollback tests |
| standalone terminal reducer lifecycle | passed | `reducerd` claim/heartbeat/publication/completion, transient retry, and bounded drain tests |
| hostile multi-tenant CLI isolation | open | dedicated UID plus externally enforced sandbox/cgroup/mount/network policy |
| public Images API traverses the complete V2 path | passed | default-off generation-only route gate; `n=2`, non-zero pricing, real gateway/workerd/executord/codex-runner/reducerd process smoke; restart replay keeps one job and two provider invocations |
| credentialed real Codex CLI image generation | passed | 2026-07-19 official-shape `1024x1024` request returned a verified native 1:1 PNG through the full V2 topology after runtime-safe capture; replay was byte-identical with one durable job, one provider submission, and one charge |

The credentialed smoke used the self-contained native binary shipped in the
official Codex package. The npm launcher is a Node script and is intentionally
incompatible with the executor's restricted path unless its interpreter is
explicitly present there. The verified `1024x1024` request produced Codex-native
`1254x1254` output. The gateway accepts it as the same 1:1 aspect ratio without
cropping, stretching, or resampling provider output.
