# Phase 1F: Persistent Executor Runtime

Status: runtime kernel checkpoint implemented. The production Images API
remains on `LegacyV1` until every activation gate in this document passes.

The checkpoint includes the executor daemon application port, owner-and-scope
resume of unexpired running executions, idempotent database start replay,
pre-launch lease renewal, and a dirfd-bound filesystem journal with atomic
no-replace launch and terminal markers. It does not yet include an executord
binary or provider process supervisor.

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
submission and make it eligible for a provider-scoped executor. `executord`
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

Only PostgreSQL may grant launch authority. A valid runner request contains the
database-bound executor lease and no client-controlled command, environment, or
credential material. Provider adapters are selected from the trusted
`provider_id` and `command_schema` scope configured for executord.

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
9. return `uncertain` when launch or terminal evidence is incomplete;
10. never convert an internal runner/storage failure into a definite provider
    rejection.

The journal and spool must reject symlinks, traversal, non-regular files,
unbounded files, and ownership or permission mismatches. Process attachment
must validate more than a reusable PID; Linux deployments should bind the PID
to a process start-time or pidfd identity.

## 5. Lease And Crash Semantics

`executord` first resumes its own unexpired running work, then claims prepared
work. A fresh claim transitions `prepared -> leased`; immediately before runner
authority is handed over, it transitions `leased -> running`.

While `start_or_attach` is active, executord heartbeats the independent executor
lease. A stale lease cannot publish a canonical outcome. Durable runner evidence
may be retained for reconciliation, but cannot bypass PostgreSQL fencing.

The database `start` transition is itself idempotent for the exact same
execution, submission, owner, and epoch. This covers a committed `running`
transition whose acknowledgement was lost. It must not rewrite the original
start timestamp or accept a different owner or epoch.

Crash outcomes are deliberately asymmetric:

| Crash boundary | Recovery |
| --- | --- |
| before executor start | prepared/leased work may be safely reclaimed according to its state |
| after executor start, before runner journal | running lease is resumed by the same stable owner; expiry becomes uncertain |
| after journal, before provider launch evidence | attach journal; missing proof becomes uncertain |
| after provider launch | attach the same execution; never launch a replacement |
| after spool commit, before DB commit acknowledgement | replay the identical terminal outcome |
| after executor lease expiry | canonical state becomes uncertain; no automatic provider retry |

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

Each slice is additive. `LegacyV1` remains the default until slice 7 passes.

At this checkpoint, slices 1 and 2 are complete and the durable journal portion
of slice 3 is complete. Process attachment, process identity, private output
spooling, and artifact publication remain part of slices 3 through 5.

Two schema capabilities remain explicit activation blockers after the first
daemon slice:

- an append-only runner observation and resolution decision path, so an expired
  running lease can retain a late durable manifest without letting a stale
  executor overwrite canonical state;
- an artifact-authority reference proving every successful executor manifest
  names an already durable immutable object, rather than trusting caller-supplied
  object metadata.

## 8. Activation Gates

Production V2 traffic remains disabled until tests prove:

- concurrent prepare, claim, resume, and attach preserve one identity;
- stale worker and executor epochs cannot mutate canonical state;
- restarting executord attaches rather than launches a second CLI invocation;
- incomplete journal, process, spool, or PostgreSQL commit evidence fails closed;
- terminal replay is idempotent and conflicting replay is rejected;
- artifact integrity and economic settlement remain atomic and balanced;
- provider credentials are isolated from prompts and client-controlled input;
- the real Images API traverses gateway, PostgreSQL, workerd, executord, runner,
  Codex CLI, artifact publication, reduction, and replay.
