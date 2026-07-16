# Phase 2AA: Recoverable Submit Attempt Workspace

Date: 2026-07-16

Status: implemented and verified as inactive provider infrastructure. This
phase activates no provider, credential, public route, billing behavior, model
advertisement, or external call.

## Decision

Every gated CLI submit attempt receives one private directory derived from the
durable submit identity:

```text
provider/account private workspace root
  + submission UUID
  + frozen launch nonce
  -> deterministic attempt name
  -> fd-bound 0700 directory
  -> provider command projection
  -> frozen gated-process request digest
  -> terminal process evidence
  -> explicit bounded cleanup
```

The provider-neutral capability is `RecoverableAttemptWorkspace` in
`crates/cli-runtime`. The gated submit driver owns composition. Provider codecs
receive a `WorkingDirectory` capability selected by the platform; they do not
select a path.

Dreamina is the first inactive consumer. Its policy accepts only one direct
child of the configured submit workspace root, uses that directory as both the
current directory and `TMPDIR`, and keeps the account home separate.

## Why Submit Differs From Poll

The Phase 2W poll workspace has one live process owner. It can hold a root-wide
lock and remove all crash-left attempts during startup because no other
cooperating poll process may still own the root.

A submit helper can outlive the gateway process that launched it. A restarted
gateway may therefore observe a still-running gated helper. Startup deletion
would race that helper and could remove its current directory before terminal
evidence is published.

Submit consequently uses different semantics:

- no root-wide ownership lock;
- no startup sweep;
- one deterministic directory per durable launch;
- replay opens the same directory;
- cleanup occurs only after terminal evidence or safe orphan termination; and
- concurrent cleanup is serialized only for that one attempt.

The two workspace types share descriptor-relative validation and bounded
cleanup primitives, but not ownership policy.

## Attempt Identity

The attempt key is:

```text
<submission UUID without separators>-<launch nonce without separators>
```

The configured prefix is `.provider-submit-`. Keys are restricted to ASCII
letters, digits, underscore, and hyphen. The full filename is capped at 255
bytes.

The submission UUID binds the directory to one provider submission. The launch
nonce binds it to the one dispatch authority committed by the remote-submit
journal. Replaying the same launch opens the same directory; a new launch
cannot alias an older attempt.

The directory path is included in `DiskRequest::canonical_payload_sha256`.
Changing the path after process preparation therefore conflicts with the
frozen process request rather than silently redirecting execution.

## Root And Directory Capability

Construction accepts a previously verified `WorkingDirectory`, clones its
descriptor, and records the root device. Every operation revalidates that:

- the public root path still names the held device and inode;
- the root is a current-user-owned `0700` directory;
- the attempt is opened with `O_DIRECTORY | O_NOFOLLOW`;
- the attempt is a current-user-owned same-device `0700` directory; and
- the public attempt path resolves to the same inode as the held descriptor.

Creation uses `mkdirat` relative to the held root descriptor. The attempt and
root directories are synchronized before process preparation can publish the
path. Every opener performs this synchronization because a concurrent opener
can observe a newly created directory before the creator reaches `fsync`.

`GatedCliCommand` and `DiskRequest::rebuild_command` require a private
`WorkingDirectory`. The generic submit driver additionally rejects a codec if
the returned command uses any path other than the platform-allocated attempt.

## Lifecycle

### Prepare

1. derive the process binding and deterministic attempt key;
2. open or create the attempt on a blocking worker;
3. project the provider command inside that exact capability;
4. reject codec errors, invalid effect classification, or workspace mismatch;
5. reject an exhausted local preparation budget before starting a helper;
6. persist the gated process request; and
7. start or attach to the one gated runner.

Codec failure, codec-worker failure, workspace mismatch, and projection-time
deadline exhaustion clean the directory before returning. They occur before a
helper or provider process exists.

### Dispatch

After a successful prepare, the orchestrator always publishes the durable
dispatch decision before calling the driver, even if the locally calculated
remaining budget reached zero between prepare and dispatch. This closes the
prepared-resource abandonment gap.

A zero-budget direct provider driver performs no provider call and returns
`provider_submit_deadline_elapsed` with `NoRemoteEffect`. The gated driver
continues under the frozen absolute deadline. If the deadline has elapsed, the
runner publishes non-execution terminal evidence; the provider CLI is not
executed.

### Recovery

Released recovery derives the same attempt key from the frozen submission and
launch nonce. It does not create a new provider attempt. It observes the
existing process journal, recovers the terminal receipt or failure, and then
removes the attempt.

### Cleanup

Cleanup is explicit rather than `Drop`-driven because dropping the restarted
gateway's local object does not prove that an independent helper has stopped.

Removal:

- is relative to the held root and attempt descriptors;
- does not follow symlinks;
- rejects ownership, mode, device, or inode drift;
- is capped at 1024 entries and 32 directory levels;
- synchronizes removed contents and the root;
- is idempotent when the directory is already absent; and
- uses a nonblocking exclusive lock on only the attempt directory.

If another recovery caller is already cleaning the same attempt, later callers
return immediately. Different attempts never contend on a shared workspace
lock.

## Performance Bound

The prepare path adds:

- one deterministic name construction;
- one root identity validation;
- one `mkdirat` or existing-entry branch;
- one `openat`, permission check, and inode comparison;
- one attempt and root synchronization;
- one path-to-descriptor identity proof; and
- no database round trip, random-number generation, global mutex, background
  janitor, content copy, or provider-specific branch.

Cleanup is outside provider execution and bounded by fixed entry and depth
limits. Its lock is per-attempt and nonblocking.

The synchronization cost is intentional: unlike poll scratch space, the submit
path is frozen into a crash-recoverable process request. Representative
filesystem and scheduling benchmarks are still required before making latency
or SOTA claims.

## Failure Classification

Workspace construction and runtime errors map to non-secret static codes:

- `provider_submit_workspace_unavailable`;
- `provider_submit_workspace_invalid`;
- `provider_submit_workspace_binding_mismatch`; and
- `provider_submit_workspace_worker_stopped`.

All occur before provider execution and are classified as `NoRemoteEffect`.
Cleanup failures are logged without paths and do not replace already durable
provider terminal evidence.

## Adversarial Verification

Provider-neutral runtime tests prove:

- deterministic reopen returns the same private directory;
- nested cleanup does not follow an external symlink;
- repeated cleanup is idempotent;
- 32 concurrent cleaners remove exactly one attempt without a root-wide lock;
- root replacement cannot redirect cleanup; and
- existing poll attempt directories are now revalidated as private before use.

Dreamina policy tests prove:

- the command current directory and `TMPDIR` are the allocated attempt;
- the account home remains separate; and
- an outside or nested non-direct workspace is rejected.

Real process tests prove the private-directory requirement does not change the
gated runner's release, timeout, orphan, output-bound, or absolute-deadline
behavior.

Real PostgreSQL tests prove:

- 32 concurrent submit callers still execute one CLI side effect;
- terminal recovery removes the original deterministic attempt;
- database receipt failure replays terminal evidence without a second CLI;
- a replaced workspace root fails before process or provider execution;
- a codec cannot replace the platform-selected workspace;
- slow projection expires with zero provider calls and zero attempt residue;
- a delayed runner crossing the dispatch budget publishes non-execution
  terminal evidence and still cleans the attempt;
- unlaunched recovery uses the frozen launch identity; and
- outcome-unknown recovery never relaunches the CLI.

## Evidence Basis

The pinned `rustix` 1.1.4 API exposes the descriptor-relative and nonblocking
locking primitives used here:

- `openat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.openat.html>
- `mkdirat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.mkdirat.html>
- `statat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.statat.html>
- `unlinkat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.unlinkat.html>
- `FlockOperation`: <https://docs.rs/rustix/1.1.4/rustix/fs/enum.FlockOperation.html>

POSIX specifies that relative `mkdirat` and `unlinkat` operations are resolved
against the supplied directory descriptor:

- <https://pubs.opengroup.org/onlinepubs/9799919799/functions/mkdir.html>
- <https://pubs.opengroup.org/onlinepubs/9799919799/functions/unlink.html>

These sources justify the primitives. The implementation and local tests do
not prove production throughput, hostile same-UID isolation, distributed
ownership, or industry leadership.

## Explicit Limits

This phase does not provide:

- protection from a hostile non-cooperating process with the same OS identity;
- mount namespace, seccomp, container, or network-egress isolation;
- cross-host shared-filesystem cleanup authority;
- automatic cleanup after storage integrity failure;
- provider credential discovery or login;
- CLI rate-limit, spend, or billing policy;
- production-scale latency and filesystem benchmarks;
- a credentialed Dreamina call;
- Seedance, Grok, or video artifact activation; or
- a runnable submit daemon.

If terminal evidence is unavailable and a live helper cannot be safely
terminated, the directory is intentionally retained. Removing uncertain live
state is less safe than bounded residue requiring operator inspection.

## Next Gate

Phase 2AB now composes the activation-gated `provider-submitd`, exact runtime
profile/account binding, digest-pinned runner and codec, and real PostgreSQL
SIGTERM/restart proof:

- [`2026-phase2ab-inactive-provider-submit-service.md`](2026-phase2ab-inactive-provider-submit-service.md)

Provider activation, external credentials, model advertisement, pricing, paid
smoke tests, and production-scale benchmarks remain separate decisions.
