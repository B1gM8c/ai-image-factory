# Phase 2M: Durable Provider Submit Journal

Date: 2026-07-16

Status: implemented and tested as the local durability boundary around the
unique provider-submit orchestrator. Remote CLI providers remain inactive. The
journal closes receipt loss after canonical receipt evidence has been synced
but before PostgreSQL can commit it. It deliberately does not claim to close
daemon death after remote acceptance but before that evidence sync; a gated
helper that writes the same journal is still an activation gate.

## Scope

The submit path now has two narrowly separated authorities:

- PostgreSQL is authoritative for scheduling, frozen execution context,
  dispatch election, recovery ownership, task state, and attach; and
- the private local journal is authoritative only for evidence at the external
  side-effect boundary.

The journal is not a second queue, an event bus, or a replacement state store.
It cannot elect work without PostgreSQL and it cannot independently create a
provider task. A terminal journal entry can only be imported through the
existing PostgreSQL receipt/failure and attach transitions.

No route, provider credential, scheduler daemon, billing behavior, CLI process,
or remote provider is enabled by this phase. Tests use only
`ScriptedFakeProvider` and local PostgreSQL fault injection.

## Durable Protocol

```text
PostgreSQL atomic Dispatch authority
  -> fsync command.bin
  -> fsync spec.json
  -> create-once launch.json
  -> create-once dispatch-released.json
  -> call provider exactly once
  -> fsync canonical receipt.evidence, if accepted
  -> create-once terminal.json
  -> record PostgreSQL receipt or failure
  -> attach known operation
```

Every file publication uses a private temporary file, mode `0600`, file sync,
`renameat(..., RENAME_NOREPLACE)`, and directory sync. Creating the private
per-submission directory also syncs its parent directory. macOS uses
`F_FULLFSYNC` for file data; other Unix targets use `fsync`. Existing markers
are compared exactly and never overwritten.

The immutable spec binds the journal entry to submission and executor IDs,
provider/account, executor owner and lease epoch, output slot, command schema,
adapter revision, canonical command bytes, provider-command digest, execution
binding, execution profile, credential identity and revision, resource policy
identity and revision, and the absolute provider deadline. It stores the
credential authentication digest, never the credential reference or secret.
The bounded canonical command contract is validated before PostgreSQL acquire,
so an empty or larger-than-1-MiB command cannot consume the unique dispatch
authority and strand a `sending` intent.

`launch.json` records a recoverable pre-release prefix. Concurrent callers may
observe that prefix, but `dispatch-released.json` is published create-once and
elects exactly one local dispatch authority. A crash before release can
therefore resume without stranding `sending`; once release exists, an absent
terminal means the remote effect is unknown and replay can only observe. The
future gated helper must add durable process identity before this release
marker; the current in-process provider future does not satisfy that activation
requirement.

For an accepted result, bounded canonical receipt evidence is durable before
the terminal marker. The evidence binds the launch nonce and execution digest
to structured provider, submission, operation, request, and polling fields. The
evidence also carries a domain-separated SHA-256 over those canonical fields,
and the terminal stores the complete evidence SHA-256 and byte size. Receipt evidence can reconstruct
`PendingOperation` even if a crash prevents terminal publication; when both
files exist, replay validates both before reconstruction.
Provider opaque IDs are validated by the provider SDK's `OpaqueProviderId`
contract rather than a duplicated journal-specific parser.

## Recovery Semantics

`Busy` and `ObserveOnly` now carry the same read-only frozen invocation context
as the original dispatch. This permits exact spec reconstruction without
granting submit authority.

Replay behavior is exhaustive:

| PostgreSQL state | Journal observation | Allowed action |
| --- | --- | --- |
| `sending` | missing, prepared, or launch committed | elect one create-once release and dispatch |
| `outcome_unknown` | missing, prepared, or launch committed | wait; never submit |
| `sending` / `outcome_unknown` | dispatch released, no receipt or terminal | wait; never submit |
| `sending` / `outcome_unknown` | accepted receipt evidence, terminal absent | import receipt, then attach |
| `sending` / `outcome_unknown` | accepted terminal | import receipt, then attach |
| `sending` / `outcome_unknown` | rejected terminal | import rejected failure |
| `sending` / `outcome_unknown` | unknown terminal | import unknown-effect failure |
| any observable state | identity or integrity mismatch | fail closed |

An accepted receipt whose provider or submission differs from the frozen
intent still goes through the existing append-only quarantine path. Journal
recovery therefore cannot bypass attribution checks.

All per-submit and replay journal reads, writes, hashes, and syncs run on
Tokio's blocking pool. Normal replay first validates the existing command and
spec and performs no temporary-file publication. The provider timeout subtracts
monotonic elapsed time after PostgreSQL returns its remaining budget, so journal
latency cannot extend a provider call past that database-derived budget. If the
budget reaches zero after dispatch release, a rejected terminal is recorded and
the provider is not called.

After a provider result is already known, journal `Unavailable` is not allowed
to discard that in-memory evidence: the orchestrator emits an operational
warning and attempts the existing PostgreSQL receipt/failure transition. An
identity conflict or integrity failure still fails closed. Simultaneous journal
and PostgreSQL failure remains an unresolved in-process crash window until the
gated helper writes evidence independently.

## Filesystem Trust Boundary

The root must be an absolute, owner-controlled `0700` directory. The journal
opens roots and entries with `O_NOFOLLOW`, retains directory file descriptors,
and performs entry operations relative to those descriptors. Read files must be
owner-controlled regular files with mode `0600`, one hard link, and bounded
size. Symlink roots, non-private roots, marker symlinks, hard-linked markers,
unexpected JSON fields, invalid predecessor order, changed bytes, and changed
identity all fail closed.

This is a host-local protocol. Shared filesystems, cross-host failover, hostile
same-UID processes, and journal-root migration are not supported activation
modes. The service manager must provision the root on local durable storage
under a dedicated service identity.

## Cost Model

A new remote submit adds six bounded create-once marker publications in the
accepted case: command, spec, launch, release, receipt evidence, and terminal.
Rejected and unknown results omit receipt evidence. Replay performs bounded
file reads and SHA-256 validation, normally performs no writes, and makes no
provider call. Blocking filesystem work is isolated from Tokio workers. There
is no broker, polling sidecar, distributed lock, transaction coordinator,
background journal scanner, or artifact-sized allocation.

These sync operations are intentional durability costs on a remote CLI path;
their absolute and tail latency still require production filesystem benchmarks.
The design makes no unsupported throughput or SOTA claim.

## Verification

Local unit tests cover:

- exact prepare replay and conflicting immutable identity;
- 32 concurrent release attempts producing exactly one dispatch authority;
- rejection of a terminal marker without release predecessors;
- accepted receipt recovery before terminal publication and exact reopen after
  terminal publication, including SDK-valid opaque IDs;
- repair of a command-only durable prepare prefix;
- self-digest rejection after changing an operation ID to another syntactically
  valid value, plus hard-linked marker rejection; and
- symlink and non-private journal-root rejection.

PostgreSQL 18 tests cover:

- 32 concurrent orchestrator calls invoking submit exactly once;
- normal receipt, attach, restart, timeout, unknown effect, and attribution
  quarantine behavior;
- forced receipt-transaction rollback after one provider result, followed by a
  fresh orchestrator importing the durable receipt without another provider
  call; and
- recovery from a durable command/spec/launch prefix after PostgreSQL has
  committed `sending`, with exactly one provider call and final attach;
- task cancellation after dispatch release while the provider future is still
  pending, followed by restart returning only `AwaitingEvidence` with no second
  provider call; and
- rejection of empty and over-1-MiB commands before any submit-intent row is
  created.

Unit tests also prove monotonic deduction of journal time from the
database-derived budget and that only storage unavailability, not conflict or
integrity failure, permits the PostgreSQL result fallback.

## Evidence Basis

Linux documents that `fsync` on a file does not necessarily persist its
directory entry, so the containing directory must also be synced:
<https://man7.org/linux/man-pages/man2/fsync.2.html>.

Rust documents the strict safety constraints around work between `fork` and
`exec`, which is why the next phase will use an explicit gated helper rather
than putting journal I/O in `pre_exec`:
<https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html>.

Tokio documents process-group configuration and that kill-on-drop reaping is
best effort rather than a durable crash-recovery protocol:
<https://docs.rs/tokio/latest/tokio/process/struct.Command.html>.

Linux cgroup v2 documents `cgroup.kill` as killing all processes in a cgroup,
including concurrent forks, which is the target Linux process-tree containment
primitive for the gated helper:
<https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html>.

## Remaining Activation Gates

1. Launch a harmless gated helper, persist host/boot/process identity, then
   release it to `exec` the provider CLI without a pre-release side effect.
2. Make the helper write bounded raw receipt evidence and canonical terminal
   state directly, then prove daemon `SIGKILL` recovery.
3. Bind the PostgreSQL absolute deadline to process-tree kill and reap. Linux
   requires cgroup-v2 zero-populated proof; macOS requires an external service
   supervisor because a process group alone cannot contain `setsid` escape.
4. Authenticate helper launch authority and reject shared or incorrectly
   provisioned journal filesystems.
5. Add bounded retention and deletion only after PostgreSQL terminal convergence
   and an audited safety interval.
6. Benchmark sync latency, allocations, recovery scan cost, and mixed-account
   load on production-equivalent local storage.

Until these gates close, Dreamina, Grok, and other remote CLI providers remain
inactive. The public Codex image API and its existing native runtime behavior
are unchanged.
