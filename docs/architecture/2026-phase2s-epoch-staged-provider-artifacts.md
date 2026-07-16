# Phase 2S: Epoch-Staged Provider Artifacts

Date: 2026-07-16

Status: implemented and verified with filesystem adversarial tests plus real
PostgreSQL 18 integration tests. This phase activates no provider, credential,
CLI query command, daemon, route, billing behavior, or external call.

## Scope

Phase 2S supplies the production filesystem implementation behind the Phase 2R
`ProviderArtifactStagerFactory` port:

```text
first provider byte
  -> acquire materialization permit
  -> create one private poll-epoch stage
  -> stream chunks directly to the stage
  -> incrementally count bytes and hash SHA-256
  -> durably flush the staged file
  -> decode and validate the actual image
  -> publish one deterministic immutable object without replacement
  -> return matching SDK manifest and database authority
```

The poll orchestrator, provider SDK, PostgreSQL authority transaction, and
filesystem mechanics remain separate ownership boundaries. No provider-specific
command, response parser, credential, or account policy enters the artifact
store.

## Ownership Boundaries

The implementation is split across two modules:

- `artifacts/filesystem.rs` owns private directories, fd-relative filesystem
  operations, provisional files, immutable publication, durable synchronization,
  byte-stable replay, and namespace identity;
- `artifacts/provider.rs` owns provider sink limits, incremental SHA-256,
  media validation, durable SDK manifest construction, and
  `ProviderArtifactAuthority` construction.

The filesystem layer receives only:

```text
artifact authority UUID
poll lease epoch
chunks
expected SHA-256 and byte size at commit
```

It does not import a provider driver, task store, poll lease, database pool, or
SDK poll result. The provider adapter receives the narrow
`ProviderArtifactStageContext` established in Phase 2R and cannot access a local
path or filesystem descriptor.

`FilesystemProviderArtifactStagerFactory` is the only new public composition
type. Its concrete stage and storage error types remain internal.

## Directory Layout

The existing artifact root gains a separate provisional namespace:

```text
GATEWAY_ARTIFACT_ROOT/
  executor-staging/
    <first-two-uuid-hex>/
      <executor-execution-uuid>/
        .epoch-<poll-lease-epoch>-<random-nonce>
  executor-objects/
    <first-two-uuid-hex>/
      <executor-execution-uuid>
```

The final object key remains unchanged:

```text
executor-objects/<shard>/<executor_execution_id>
```

Therefore this phase does not change the database authority contract, reducer
contract, result manifest identity, or customer artifact projection.

The stage path is not authority. It is a private, disposable capability scoped
to one executor execution and one poll lease epoch. The random nonce prevents
same-epoch name collision; the epoch makes failure evidence inspectable and
lets a new lease clean the exact execution-local staging directory without
scanning an unbounded global prefix.

## Filesystem Trust Boundary

At startup, the artifact root, immutable object root, and staging root must be:

- existing real directories rather than symlinks;
- owned by the service user;
- non-group-writable and non-world-writable at the artifact root;
- mode `0700` for internal storage directories.

The service opens and retains directory descriptors for both
`executor-objects` and `executor-staging`. Before each stage begins, it reopens
the visible path and compares device and inode identity with the retained
descriptor. Replacing either visible directory fails closed.

All stage and final-object operations below the retained roots are fd-relative:

- `mkdirat` for private shards and execution directories;
- `openat` with `O_EXCL`, `O_NOFOLLOW`, and `O_CLOEXEC` for stages;
- `openat` with `O_NOFOLLOW` for validation and replay;
- `unlinkat` for abandoned stages; and
- `renameat_with(..., RENAME_NOREPLACE)` for immutable publication.

No provider-controlled filename, URL, absolute path, or path separator is
accepted.

## Epoch Fencing

Beginning a stage opens the exact execution-local staging directory and removes
only well-formed regular stage entries whose encoded epoch is less than or
equal to the current lease epoch. A future epoch, unexpected name, or
non-regular entry fails closed.

This gives the following stale-worker behavior:

```text
epoch N opens .epoch-N-a and starts writing
epoch N lease expires
epoch N+1 opens the same execution stage directory
epoch N+1 unlinks .epoch-N-a
epoch N still holds an inode but no publishable source name
epoch N finalize cannot reopen or rename its stage
```

The old process may finish an already-issued write to its unlinked inode, but it
cannot publish that inode into the immutable namespace. The database lease still
fences authority publication independently, so filesystem and PostgreSQL
fences are additive rather than substitutes.

Dropping a live stage attempts to unlink its provisional name and synchronize
the staging directory. A process crash can leave a stage name, but the next
lease for that execution removes it before creating a new stage.

## Streaming And Limits

The stager never accumulates the compressed artifact in a `Vec<u8>`.

For each non-empty provider chunk it:

1. checks the next cumulative byte count with overflow protection;
2. rejects a count above the factory's configured bound;
3. writes the chunk directly to the Tokio file; and
4. updates SHA-256 only after the write succeeds.

The configured bound must be in `1..=256 MiB`. The Phase 2R materialization
semaphore still limits the number of concurrent stages, and is acquired before
the factory creates any file.

At finalization, the file is flushed and converted back to a standard file
descriptor. Linux and other Unix targets use `sync_all`; macOS uses
`F_FULLFSYNC`, matching the durable submit-journal policy.

## Media Validation

The staged file is reopened read-only with `O_NOFOLLOW`, and its type, owner,
mode, link count, and exact compressed byte size are checked before decoding.

Image validation uses one content-sniffed `ImageReader` decode with explicit
limits:

- accepted formats: PNG, JPEG, WebP;
- maximum width: 8192;
- maximum height: 8192;
- maximum pixels: 16,777,216;
- maximum decoder allocation budget: 134,217,728 bytes.

The provider-declared media type must equal the type derived from decoded
content. A filename or SDK metadata string cannot turn PNG bytes into JPEG
authority.

The same reader-based validator now serves the existing inline executor
publisher. This removes its former dimensions pass plus second full decode; the
accepted formats and dimension policy remain unchanged while decoder allocation
is now bounded explicitly.

Video is deliberately rejected in this phase. Accepting `video/*` without a
real container parser, duration limit, codec policy, frame/dimension limits, and
canonical result schema would be an unsupported security claim.

## Immutable Publication

After validation, the stager constructs the deterministic manifest and
authority in memory before exposing an object:

```text
manifest ID       = submission_id
authority ID      = executor_execution_id
object key        = executor-objects/<shard>/<executor_execution_id>
durable SDK ref   = submission_id:executor_execution_id
SHA-256 / size    = incrementally derived from accepted chunks
media type        = derived from decoded content
```

The provisional file is then renamed from its execution staging directory into
the final object shard with `RENAME_NOREPLACE`. Destination and source
directories are synchronized after a successful rename.

No code path overwrites a final executor object.

If the final object already exists, the provisional file is not accepted
blindly. The existing object is opened without following symlinks, validated as
a private single-link regular file, and hashed with a fixed 64 KiB buffer:

- exact size and SHA-256 match: discard the provisional file and reuse the
  immutable object;
- mismatch: discard the provisional file and report an integrity conflict.

This replay path performs no artifact-sized allocation.

## Crash Matrix

| Crash or failure point | Filesystem state | Recovery |
| --- | --- | --- |
| Before first byte | no stage | reclaim and poll |
| During chunk streaming | provisional epoch file | drop cleanup or next-epoch cleanup |
| After file sync, before validation | durable provisional file | next-epoch cleanup |
| Validation failure | no final object | drop cleanup; terminal contract becomes uncertain |
| Before immutable rename | provisional file | next-epoch cleanup |
| After rename, before directory sync acknowledgment | final object may exist | replay validates exact size and SHA-256 |
| After final object, before authority transaction | immutable unreferenced object | new epoch re-polls; byte-stable output reuses object |
| After authority, before observation | immutable object plus database authority | Phase 2R claim recovery skips provider and stager |
| Contradictory replay bytes | original immutable object | reject conflict; never replace |

The pre-authority window cannot be made atomic across a local filesystem and
PostgreSQL without introducing a distributed transaction protocol. The selected
design instead makes the object immutable and replay-verifiable, then makes the
database authority and observation independently idempotent.

## Cost Model

A pending poll still creates no directory or file and performs no storage sync.

A first successful completed poll performs:

- one direct sequential write of provider chunks;
- one incremental SHA-256 pass during that write;
- one bounded image decode from the staged file;
- one file durability synchronization;
- one no-replace rename; and
- source and destination directory synchronization.

It does not:

- copy the compressed artifact into an artifact-sized owned buffer;
- hash the newly written final object a second time;
- perform provider or filesystem I/O inside a PostgreSQL transaction; or
- dynamically dispatch the hot stager path through boxed futures or trait
  objects.

A pre-authority crash replay necessarily re-downloads bytes because no database
authority exists. It then validates the existing immutable object with one
fixed-buffer hash pass. A post-authority crash performs no provider or storage
call because Phase 2R returns committed authority in the claim.

These are structural bounds, not benchmark results. They do not prove throughput
leadership or a SOTA claim. Production activation still requires p50/p95/p99
latency, CPU, allocation, filesystem sync latency, decoder memory, mixed-size
fairness, and crash-injection measurements on deployment filesystems.

## Verification

Filesystem unit and adversarial tests prove:

- invalid size-limit configuration fails before runtime use;
- valid PNG chunks stream into the exact deterministic immutable object;
- same-byte replay reuses the object;
- different-byte replay cannot replace the object;
- a newer epoch unlinks an older stage and prevents stale finalization;
- an older epoch cannot delete a future stage name;
- media-type spoofing and size overflow publish no final object;
- normal drop and simulated crash leftovers are cleaned;
- visible staging-root replacement fails closed; and
- all prior executor-object replacement and authority tests remain green.

Real PostgreSQL 18 integration tests prove:

- a completed provider poll streams a real valid PNG through the filesystem
  stager and atomically produces one authority, manifest, artifact observation,
  canonical success decision, ready reduction, and capacity release;
- a simulated failure after immutable object publication but before authority
  publication leaves zero database authorities; and
- after lease expiry, a new epoch re-polls the same bytes, reuses the immutable
  object, and converges to exactly one authority and one manifest.

The existing Phase 2R test still proves that a committed authority is recovered
without provider polling or stager initialization.

No paid provider, CLI, external network, production credential, or production
artifact root is used.

## Evidence Basis

Linux documents `RENAME_NOREPLACE` as refusing to overwrite an existing
destination, with the rename operation providing the atomic namespace change:
<https://www.man7.org/linux/man-pages/man2/renameat.2.html>.

Linux documents that syncing a file does not necessarily persist its directory
entry, which is why the implementation also synchronizes affected directories:
<https://www.man7.org/linux/man-pages/man2/fsync.2.html>.

`rustix` 1.1.4 exposes `RenameFlags::NOREPLACE` for `renameat_with` and fd-relative
filesystem operations:
<https://docs.rs/rustix/1.1.4/rustix/fs/struct.RenameFlags.html>.

Tokio documents that `flush` alone does not guarantee durable storage and that
`sync_all` requests data and metadata synchronization:
<https://docs.rs/tokio/1.52.3/tokio/fs/struct.File.html>.

The `image` crate documents content-based `with_guessed_format`, buffered
readers, decode limits, and `decode`:
<https://docs.rs/image/0.25.10/image/struct.ImageReader.html>.

Apple documents that `F_FULLFSYNC` requests the drive to flush buffered data to
permanent storage and is stronger than ordinary `fsync` for applications that
require tighter ordering:
<https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html>.

These sources justify the selected primitives. They do not establish that this
repository is SOTA; that remains an empirical production claim.

## Explicit Limits

Phase 2S does not provide:

- video container, duration, codec, frame, or pixel validation;
- object-store multipart staging, conditional put, or checksum verification;
- bounded cleanup of empty long-lived staging directories;
- garbage collection for immutable objects that never gain database authority;
- crash-injection tests under power loss or network filesystems;
- a provider poll daemon, adaptive pacing, metrics, or account rotation;
- Dreamina query, Seedance download, Grok, or any provider activation; or
- production-scale mixed image/video benchmark evidence.

## Next Gate

Phase 2T should wire one provider-neutral poll daemon around the verified
orchestrator without activating a provider:

1. scope one daemon instance to one immutable provider/account execution
   profile;
2. derive concurrency from durable account capacity rather than a second
   scheduler;
3. add idle backoff, shutdown, bounded error pacing, and observability;
4. preserve the existing database deadline and lease authority;
5. prove two-daemon fairness and crash recovery against PostgreSQL; and
6. benchmark idle, pending, and completed paths before selecting production
   defaults.

Dreamina query integration remains inactive until this daemon boundary and a
video-validation contract are independently verified.
