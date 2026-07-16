# Phase 2W: Exclusive CLI Attempt Workspace

Date: 2026-07-16

Status: implemented and locally verified as a provider-neutral runtime
capability. Dreamina remains inactive. This phase performs no external
provider call, credential login, quota consumption, route activation, billing
change, or production artifact write.

Follow-up: Phase 2X composes this capability into a runnable but inactive
single-profile poll process:
[`2026-phase2x-inactive-provider-poll-service.md`](2026-phase2x-inactive-provider-poll-service.md).

## Decision

Each CLI poll process must receive an attempt directory created by one
exclusive workspace authority. The authority is bound to the exact
`WorkingDirectory` descriptor already frozen into the provider policy; it does
not reopen the root from a path.

```text
provider/account WorkingDirectory capability
  -> validate 0700 + effective-user ownership + inode identity
  -> validate/create 0600 one-link empty lock file
  -> nonblocking exclusive flock for driver lifetime
  -> clean crash-left directories with the exact attempt prefix
  -> create each attempt with mkdirat(root_fd, ...)
  -> bind attempt fd and path to the same inode
  -> provider process
  -> fd-relative recursive cleanup on drop
```

The implementation lives in `cli-runtime`. It does not depend on Dreamina,
the gateway, PostgreSQL, scheduling, billing, or an artifact backend.

Dreamina is the first consumer. `DreaminaCliPollDriverV1` owns the workspace
authority for its full lifetime and creates every query attempt through it.

## Why This Boundary Exists

The Phase 2V driver used a path-based temporary-directory helper. It provided
ordinary scope cleanup but left three production gaps:

1. two poll processes could point at the same root;
2. `SIGKILL` or host loss could leave attempt directories; and
3. path-based recursive cleanup could be redirected if a root path was
   replaced after construction.

A periodic janitor would add a second owner and require age heuristics. This
phase instead makes startup recovery part of the single-owner acquisition:
the process obtains the lock first, then removes only entries in its dedicated
namespace. There is no janitor racing a live attempt.

## Root Capability

`ExclusiveAttemptWorkspace::acquire` accepts a `WorkingDirectory`, not a path.
This avoids a second independent root lookup after the provider policy has
already pinned the directory.

The root must remain:

- an absolute canonical directory;
- owned by the effective user;
- mode `0700`;
- the same device and inode as the held descriptor; and
- free of entries outside the lock file and exact attempt prefix.

The lock file must remain:

- a regular file;
- owned by the effective user;
- mode `0600`;
- empty; and
- linked exactly once.

An existing malformed, symlinked, hard-linked, non-empty, or incorrectly
permissioned lock file fails closed. Lock contention is distinct from
integrity failure and is exposed as `AlreadyLocked`.

The lock is advisory. A non-cooperating process running as the same OS user can
ignore it. Production activation therefore still requires one dedicated root
per provider account and process identity, plus host-level process and mount
isolation.

## Attempt Creation

Attempt names combine the provider-specific prefix, process ID, and a
process-local atomic sequence. Unpredictability is not an authority boundary:
the root is private and exclusively owned. The sequence prevents collisions
without adding a random-number dependency or a global mutex.

Creation uses `mkdirat` against the held root descriptor. The directory is
opened with `O_DIRECTORY | O_NOFOLLOW`, forced to mode `0700`, and compared
with the current root-relative entry. The root path is revalidated after
creation.

`AttemptDirectory::working_directory` then opens the public path needed by the
external CLI and proves that it resolves to the same attempt inode already
held by the authority. The existing process runtime separately revalidates
the policy workspace immediately before spawn.

If the root path was replaced before a poll, attempt creation fails before the
provider process starts. If replacement occurs after attempt creation,
`WorkingDirectory` identity validation or command revalidation fails closed.

This is not a claim of protection against a hostile same-UID process racing
the final validation and `exec`. That threat requires an isolated OS identity,
mount namespace or equivalent sandbox, and constrained filesystem mounts.

## Crash Recovery

After obtaining the exclusive lock, startup scans the root by descriptor:

- the lock file is ignored;
- an unknown entry fails acquisition;
- only names with the exact configured prefix are candidates;
- each top-level attempt must be a same-device, effective-user-owned `0700`
  directory;
- nested directories are opened relative to their parent with `O_NOFOLLOW`;
- cross-device nested directories are rejected;
- symlinks and non-directory entries are unlinked, never followed;
- directory identity is checked before and after recursion; and
- cleanup is capped at 1024 entries and 32 nested directory levels; and
- cleanup is synchronized before acquisition succeeds.

The root is intentionally dedicated. Mixing submit attempts, poll attempts,
operator files, or multiple provider prefixes in one root is rejected rather
than guessed.

## Ordinary Cleanup

`AttemptDirectory` owns the attempt descriptor and a duplicate of the original
root descriptor. Its `Drop` implementation recursively removes contents
relative to those descriptors.

Before removing the top-level name, it verifies that the current root-relative
entry is still the same inode. If another entry replaced that name, the
replacement is not removed.

Ordinary cleanup is best effort. Cancellation may race a child that is still
exiting, and host loss can interrupt cleanup. Any residue is handled by the
next exclusive startup. Provider bytes in an attempt directory never become
artifact authority merely because cleanup succeeded or failed.

Cleanup exceeding 1024 entries or 32 nested directory levels fails closed
instead of performing unbounded synchronous work. An operator must inspect and
remove such hostile residue before that dedicated root can start again.

## Performance Bound

The hot path adds bounded metadata operations:

- one root identity validation;
- one atomic counter increment;
- one `mkdirat`;
- one attempt `openat` plus permission and inode checks;
- one path-to-descriptor identity check; and
- descriptor-relative removal on scope exit, capped at 1024 entries and 32
  nested directory levels.

It adds no per-attempt database call, background task, global mutex, file
content copy, random-number dependency, or storage synchronization barrier.

`fsync` is retained for one-time lock creation and startup recovery, where
durable cleanup is useful and outside the poll hot path. Per-poll creation and
drop deliberately do not call `fsync`: an attempt directory disappearing or
remaining after a crash are both safe states because it contains no durable
authority and startup recovery is repeatable.

These are code-path bounds, not production latency measurements. No SOTA,
lowest-overhead, or industry-leading claim is made without representative
p50/p95/p99 process, filesystem, CPU, memory, and concurrency benchmarks.

## Failure Classification

Provider-neutral workspace errors distinguish:

- invalid static configuration;
- unavailable filesystem operations;
- integrity violations; and
- an already-held workspace lock.

Dreamina maps transient filesystem unavailability to retryable transport
failure. Integrity drift and lock/configuration violations fail as permanent
workspace contract errors.

The Dreamina driver configuration error now preserves the non-secret workspace
error as its source. It does not include the root path, credential reference,
account home, command, or provider output.

## Adversarial Verification

Provider-neutral tests prove:

- nested crash-left attempts are removed without following an external
  symlink;
- malformed lock mode, lock content, and lock hard links fail closed;
- excessive cleanup entry counts and directory depth fail closed;
- a second owner cannot acquire the same root and can acquire after release;
- 32 concurrent attempt creations produce unique bound directories and leave
  no attempt residue;
- replacing the root path after attempt creation does not redirect cleanup;
- a replacement directory with the same attempt name remains untouched;
- a replaced root prevents subsequent attempt creation; and
- unknown entries, symlinked attempts, and non-private roots fail closed.

Dreamina tests prove:

- crash-left `.dreamina-poll-*` attempts are cleaned during driver
  construction;
- a second Dreamina driver cannot own the same root;
- replacing the root prevents the query process from starting;
- real child-process cancellation still removes the attempt; and
- pending, failed, successful, malformed, oversized, and invalid-media paths
  leave no attempt directory.

The Dreamina test group passed 20 consecutive parallel rounds after the final
capability binding. The real cancellation path also passed repeated isolated
rounds. These are local stress checks, not production benchmarks.

## Evidence Basis

Rust 1.97 documents that `std::fs::remove_dir_all` currently uses
`openat`, `fdopendir`, `unlinkat`, and `lstat` on most Unix-family platforms,
and explicitly discusses symlink TOCTOU protection:
<https://doc.rust-lang.org/std/fs/fn.remove_dir_all.html>.

This implementation uses the same descriptor-relative family directly because
it must also validate the dedicated root namespace, ownership, same-device
recursion, lock file, and top-level inode before removal.

The pinned `rustix` 1.1.4 API documents the used primitives:

- `openat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.openat.html>
- `mkdirat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.mkdirat.html>
- `statat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.statat.html>
- `unlinkat`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.unlinkat.html>
- `flock`: <https://docs.rs/rustix/1.1.4/rustix/fs/fn.flock.html>

POSIX specifies the directory-descriptor-relative `mkdirat` and `unlinkat`
operations:

- <https://pubs.opengroup.org/onlinepubs/9799919799/functions/mkdirat.html>
- <https://pubs.opengroup.org/onlinepubs/9799919799/functions/unlinkat.html>

These sources support the selected primitives. They do not prove that this
repository is SOTA or production-ready.

## Explicit Limits

Phase 2W does not provide:

- a credential broker or any inferred Dreamina credential-home schema;
- a runnable provider poll service binary or environment contract;
- submit-side workspace composition;
- container, mount-namespace, seccomp, sandbox, or network-egress isolation;
- protection from a hostile non-cooperating process with the same OS identity;
- distributed ownership across hosts;
- provider query rate limiting, cooldown, circuit breaking, or account
  rotation;
- metrics export, alert thresholds, or production-scale benchmarks;
- a credentialed Dreamina smoke test;
- Dreamina activation or model advertisement; or
- Seedance, Grok, video artifacts, or video billing.

No public source inspected for Dreamina defines a stable credential directory
or authentication-file schema. Inventing a hash target or credential broker
contract would freeze an unverified provider assumption, so that work remains
blocked on authoritative CLI inspection or an explicitly approved isolated
login experiment.

## Next Gate

Phase 2X closes the runnable but inactive poll-service composition around one
Phase 2U profile, one deployment-injected account-home capability, the
digest-pinned Dreamina image driver, the Phase 2T daemon, redacted lifecycle
diagnostics, and real PostgreSQL shutdown proof.

Provider activation, external calls, credential provisioning, video support,
pricing, and account routing remain separate gates.
