# Phase 2Q: Fresh CLI Output Directory

Date: 2026-07-16

Status: implemented and unit-verified as a provider-neutral runtime primitive.
No provider route, credential, daemon, poll loop, installer, or external call is
activated by this phase.

## Scope

Remote CLI query commands may select the artifact filename inside a caller
supplied download directory. The existing `OutputContract` is intentionally
stricter: it seals one known filename. Dreamina `query_result --download_dir`
therefore cannot safely use that contract without guessing a provider-selected
name or scanning a path after the process exits.

Phase 2Q adds `FreshOutputDirectory` to `image-cli-runtime`. It binds and
validates one fresh staging directory before process execution, then provides
two explicit post-execution outcomes:

```text
provider still pending
  -> ensure_empty()

provider reports success
  -> seal_single_file_to_sink()
```

The runtime primitive does not parse a provider receipt or decide which branch
is correct. That belongs to the provider codec and the future poll
orchestrator.

## Contract

Construction requires:

- an already verified `WorkingDirectory`;
- exact directory mode `0700`, including rejection of setuid, setgid, and sticky
  special bits;
- ownership by the current effective user;
- an empty directory; and
- a non-zero maximum artifact size.

The directory file descriptor is retained. Final enumeration uses
`rustix::fs::Dir::read_from` on that descriptor rather than reopening the path.
The capability is intentionally not cloneable, and the private-mode and owner
checks are repeated after CLI execution and again after artifact streaming.
Only two non-dot entries are retained because the accepted cardinality is
exactly one:

```text
0 entries -> missing
1 entry   -> candidate
2 entries -> reject immediately
```

The candidate is opened relative to the retained directory descriptor with
`O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC`. It must be:

- a regular file;
- non-empty;
- below the configured byte limit; and
- single-linked.

Directories, symlinks, hard links, FIFOs, sockets, and devices therefore do not
enter the sink path.

## Streaming And Identity

The artifact is copied through the existing 64 KiB stack buffer while SHA-256
and byte count are computed. The runtime retains no artifact-sized `Vec<u8>`.
The caller chooses the `Write` sink and is responsible for giving that sink
bounded or streaming behavior. That sink must remain provisional: no object or
manifest may become authoritative until `seal_single_file_to_sink` returns
success. A validation or sink failure consumes and drops the sink so an RAII
implementation can clean up its private temporary object.

Before and after the copy, the runtime compares:

- device and inode;
- mode and link count;
- size;
- modification time; and
- change time.

After the copy it enumerates the bound directory again, requires the same sole
filename, reopens that current directory entry, and compares the complete file
identity again. A same-name replacement during streaming therefore fails with
`ChangedDuringRead`.

The fixed-filename `OutputContract` path now receives the same final directory
entry identity check.

## Cost Model

Directory memory is constant: at most two names are retained. Artifact memory
inside the runtime is one 64 KiB buffer plus SHA-256 state. CPU and I/O are
linear in artifact bytes because complete hashing and copying are required
before immutable artifact authority can exist.

This is a structural bound, not a throughput or zero-copy claim. A future
filesystem object sink may use a private temporary file and atomic no-replace
publication, but it must still read and hash every byte. Avoiding that integrity
pass would weaken the authority contract.

## Adversarial Verification

The runtime unit suite covers:

- one provider-selected filename larger than two streaming buffers;
- exact SHA-256, byte count, filename, and maximum write chunk;
- non-empty initial directories;
- missing, empty, multiple, and oversized outputs;
- nested directories, symlinks, hard links, and FIFOs;
- non-private and special-bit directory permissions;
- directory permissions changed after preflight;
- retention of the original directory after its path is replaced; and
- rejection when a sink moves the opened file away and installs a same-name
  replacement during the first write, for both provider-selected and fixed
  filenames.

All prior process-group, timeout, bounded receipt, known-file output, stdin, and
executable-digest tests remain unchanged.

## Evidence Basis

Rustix 1.1.4 exposes descriptor-relative directory iteration through
`Dir::read_from`:
<https://docs.rs/rustix/1.1.4/rustix/fs/struct.Dir.html>.

POSIX specifies that `openat` resolves a relative path against the supplied
directory file descriptor:
<https://pubs.opengroup.org/onlinepubs/9799919799/functions/open.html>.

The runtime still uses `O_NOFOLLOW`, regular-file and link-count checks, and
pre/post identity validation because descriptor-relative lookup alone does not
prove artifact type, immutability, or cardinality.

## Explicit Limits

This phase does not provide:

- filesystem quota or protection against temporary disk amplification;
- Linux cgroup or mount-namespace containment;
- image/video MIME decoding, dimensions, duration, or codec validation;
- immutable object-store publication or orphan garbage collection;
- poll lease heartbeats, cancellation, backoff, or database observations; or
- proof that Dreamina terminal query/download is byte-stable across retries.

The same-UID process and path-to-exec limitations recorded in Phase 2N still
apply. Production activation requires OS containment and measured storage
limits.

## Next Gate

The next implementation must compose, without activating Dreamina:

1. a digest-pinned Dreamina `query_result` policy using a fresh per-attempt
   directory;
2. strict pending/success receipt handling;
3. a one-shot lease-scoped artifact sink that validates media and publishes an
   immutable object;
4. poll heartbeat during query and artifact transfer;
5. exact manifest equality before `artifact_ready`; and
6. PostgreSQL replay tests for crash windows before and after object and
   authority publication.
