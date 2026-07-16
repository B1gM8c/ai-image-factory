# Phase 2N: Gated CLI Submit Runner

Date: 2026-07-16

Status: implemented and workspace-verified as inactive infrastructure.
Dreamina, Grok, and other remote CLI providers remain disabled. Phase 2O now
composes the unique PostgreSQL provider-submit orchestrator with this runner,
but no provider-specific production adapter is activated.

## Decision

A remote-effecting CLI must not use the ordinary in-process `CliRuntime`
submit path. That runtime necessarily spawns the CLI before its observer can
persist process identity. For artifact-only Codex execution this is recoverable
through the existing executor spool, but for a remote submit the same ordering
can lose whether the provider was called.

The new runner introduces a provider-neutral host-local process protocol:

```text
prepare immutable process request and optional stdin
  -> start independent supervisor
  -> supervisor starts a harmless gate child in a new process group
  -> fsync helper identity and blocked child identity
  -> caller observes GatedCliReady
  -> caller durably authorizes release
  -> supervisor writes one byte to the private release pipe
  -> gate revalidates request, release, executable digest, and working directory
  -> gate fsyncs a create-once pre-exec marker after all validation
  -> gate execs the CLI in the same PID and process group
  -> supervisor hard-kills at the absolute provider deadline or gracefully
     terminates on the shorter wall timeout
  -> supervisor drains bounded stdout/stderr
  -> supervisor fsyncs one self-digested terminal record
```

The gate child cannot execute the CLI before both its durable identity and the
create-once release marker exist. If the supervisor dies before release, pipe
EOF makes the harmless gate exit. If it dies after release, recovery observes
one boot-fenced PID/start-token/PGID cleanup target and never launches another
process for the immutable attempt.

## Ownership Boundaries

`crates/image-gateway/src/provider_tasks/remote_submit/process/` is split by
responsibility:

- `mod.rs` owns the public gated-command API and private journal state;
- `protocol.rs` owns immutable disk schemas, validation, self-digests, and
  conversion to public observations;
- `runner.rs` owns the supervisor/gate state machine, capture, timeout, and
  terminal publication; and
- `unix.rs` owns file locks, file descriptors, boot identity, PID start tokens,
  process groups, and signals.

The `remote-submit-runner` binary is only a process boundary. It does not know
provider payloads, scheduling, quota, billing, PostgreSQL state, or receipt
semantics. Provider adapters will remain responsible for translating canonical
commands and parsing official CLI receipts. PostgreSQL remains the sole queue,
dispatch, task-state, and recovery authority.

No broker, workflow engine, SQLite outbox, second queue, distributed lock, or
provider registry was added.

## Durable Files

The process protocol shares the existing private per-submission directory and
uses separate `process-*` names:

| File | Purpose |
| --- | --- |
| `process-request.json` | immutable binding, launch nonce, absolute provider deadline, executable digest, argv, non-secret environment, working directory, stdin digest, and timeouts |
| `process-stdin.bin` | optional bounded stdin bytes |
| `process-runner.lock` | exclusive supervisor ownership |
| `process-helper.json` | boot token, helper PID/start token, nonce, and lock inode binding |
| `process-ready.json` | blocked child PID/start token, PGID, boot token, and helper/child nonces |
| `process-dispatch-released.json` | create-once authorization bound to the exact ready identities |
| `process-exec-started.json` | create-once proof that every gate validation completed before the provider exec attempt |
| `process-terminal.json` | release/exec facts, exit outcome, bounded streams, and a canonical self-digest |

The root and submission directory must be owner-controlled mode `0700`. Marker
files use mode `0600`, one hard link, `O_NOFOLLOW`, bounded reads, file sync,
create-without-replacement rename, and directory sync. The implementation
reuses the Phase 2M filesystem primitives rather than creating a second
durability implementation.

The request is capped at 256 KiB, stdin at 8 MiB, and each captured stream at
64 KiB while the reader continues draining excess output. Environment values
are persisted and therefore must be non-secret. Provider credentials must be
supplied through separately provisioned account homes or another explicit
secret boundary, never through this durable command record.

## Recovery Semantics

| Durable observation | Allowed action |
| --- | --- |
| request only | start one supervisor |
| helper active, child absent | wait for startup |
| blocked child active, release absent | return `GatedCliReady`; caller may release before the absolute provider deadline |
| release present, helper active | wait; never launch another CLI |
| pre-exec marker present | provider exec may have been attempted; never infer no effect from process exit alone |
| terminal present | validate and import bounded raw result |
| helper lost before release, child gone | no remote effect; do not infer a provider call |
| helper lost after release, child current | kill the identity-fenced boot/PID/start-token/PGID target; never relaunch |
| helper lost, child identity stale | treat as no live cleanup target |
| identity, self-digest, predecessor, permission, symlink, or hard-link mismatch | fail closed |

Release checks the absolute provider deadline both before and immediately
before marker publication. The supervisor independently re-reads final release
evidence after terminating a non-released gate, closing the narrow
deadline/release race without pretending that a late marker means the CLI was
executed. The gate checks the same deadline immediately before and after
syncing its pre-exec marker, so executable validation, stdin verification, and
marker durability cannot silently consume the remaining launch window.
`released` and `exec_started` are separate terminal facts. `exec_started` is
derived only from the durable pre-exec marker. It means every gate validation
completed and the provider exec attempt was authorized; it does not claim that
the target executable successfully replaced the gate image.

The exec-status pipe remains a bounded diagnostic channel. It is marked
`CLOEXEC` immediately before provider exec, while controlled validation or exec
failures write an error code. EOF is not treated as sole proof of successful
exec because an abnormal pre-exec process death also closes the descriptor.

If the CLI leader exits while descendants still hold the process group or
capture pipes, the supervisor terminates the residual group before joining
capture readers and records `ResidualProcessGroup`. This prevents a background
descendant from blocking terminal publication indefinitely.

The absolute provider deadline is a hard external-effect boundary: the
supervisor sends `SIGKILL` to the process group immediately and boundedly reaps
the child. The CLI wall timeout remains an operational timeout and uses
`SIGTERM`, the configured bounded grace, then `SIGKILL`. A termination grace
therefore cannot extend provider execution beyond the database-bound absolute
deadline.

## Containment Limits

The current portable implementation uses a dedicated process group and
double-checked boot/PID/start-token identity. This is sufficient for the tested
fake CLI tree, but it cannot contain a descendant that deliberately calls
`setsid` or migrates away from the group.

Linux production activation still requires a per-submit cgroup v2 boundary,
`cgroup.kill`, and proof that `cgroup.events` reports `populated 0`. A pidfd can
strengthen leader observation but does not replace cgroup tree containment.
macOS has no equivalent process-tree primitive in this implementation and
requires an external service supervisor or a dedicated execution identity.

The gate revalidates the configured executable SHA-256 immediately before
`exec`, but path-based exec still has a same-UID replacement race. Production
activation must execute from an immutable deployment path or add an
OS-specific file-descriptor execution strategy where supported.

Shared filesystems and hostile same-UID processes remain unsupported trust
models.

## Performance Model

The normal path adds one harmless process plus the eventual CLI process image,
two short-lived capture threads, bounded polling at 10 ms, and five process
evidence publications after the immutable request. There is no scheduler hot
path allocation proportional to provider output, no unbounded `wait_with_output`,
and no Tokio worker blocking because the runner is a separate synchronous
process.

These choices minimize coupling and make crash state explicit; they are not a
throughput claim. Production-equivalent measurements are still required for
sync latency, process startup, tail timeout cleanup, and high-concurrency
account mixes.

## Verification

Real binary/process tests prove:

- no fake-provider side effect exists before durable release;
- release leads to exactly one fake-CLI invocation and a durable bounded terminal;
- helper `SIGKILL` before release causes gate EOF and zero provider effect;
- gate `SIGKILL` before release is classified as a gate failure rather than a
  provider deadline;
- helper `SIGKILL` after release exposes one identity-fenced orphan process
  group, which recovery kills without a relaunch;
- an expired absolute provider deadline rejects release and terminates the gate
  without provider execution;
- a released, running CLI is hard-killed at the absolute provider deadline
  without consuming its deliberately configured five-second termination grace;
- a released CLI exceeding its wall timeout is killed and recorded as
  `TimedOut` with `exec_started = true`;
- a CLI leader that exits while leaving a background process in its group is
  boundedly terminated and recorded as `ResidualProcessGroup`;
- output beyond 64 KiB is still drained while the retained evidence remains
  bounded and marked truncated.

Unit tests cover immutable request replay, conflicting binding rejection,
bounded prepare ordering, command-value compatibility, and domain-separated
request and terminal self-digests. Formatting, workspace Clippy with warnings
denied, and the complete workspace suite against real PostgreSQL all passed
before commit.

No test invokes Codex quota, Dreamina, Grok, or another external provider.

## Evidence Basis

Rust documents that `pre_exec` runs after `fork` under strict
async-signal-safety constraints, while `exec` replaces the current process
without another fork. The explicit gate process avoids journal I/O in
`pre_exec`:
<https://doc.rust-lang.org/std/os/unix/process/trait.CommandExt.html>.

Linux documents that syncing a file does not necessarily persist its directory
entry, so the directory must also be synced:
<https://man7.org/linux/man-pages/man2/fsync.2.html>.

Linux cgroup v2 documents that `cgroup.kill` kills the cgroup and descendants,
including concurrent forks, and that `cgroup.events` exposes recursive
`populated` state:
<https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html>.

Linux pidfds provide a race-resistant handle for one process identity, which is
useful for leader observation but not complete descendant containment:
<https://man7.org/linux/man-pages/man2/pidfd_open.2.html>.

## Remaining Activation Gates

1. Add one provider-specific Phase 2O codec without activating external
   credentials or provider calls.
2. Add Linux cgroup v2 containment and zero-populated verification; define the
   macOS production supervisor contract.
3. Freeze and authenticate the helper executable/deployment identity and
   benchmark executable/working-directory replacement defenses.
4. Add retention only after PostgreSQL terminal convergence and an audited
   safety interval.
5. Run production-equivalent filesystem/process/concurrency benchmarks before
   making any SOTA or activation claim.
