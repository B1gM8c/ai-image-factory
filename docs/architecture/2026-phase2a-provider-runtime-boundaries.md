# Phase 2A: Provider and CLI Runtime Boundaries

Status: accepted for incremental implementation on 2026-07-15.

## Decision

Phase 2A extends the proven Codex V2 vertical slice without replacing its
PostgreSQL scheduling, economic, artifact-authority, or terminal-reduction
protocols.

The implementation has four boundaries:

1. `image-provider-contracts` owns immutable provider, model, and operation
   descriptors. Capabilities are operation-level facts, not a provider-level
   bag of overlapping booleans.
2. `image-provider-sdk` owns provider execution ports, single-output command
   identity, remote-operation receipts, failure semantics, and the streaming
   artifact sink contract. It has no runtime, SQL, HTTP, API DTO, or billing
   dependency.
3. `image-cli-runtime` owns generic Unix process launch, explicit environment,
   process-group termination, bounded output collection, and sealed output
   evidence. Vendor prompt, argv, output policy, and credentials remain in the
   concrete provider adapter.
4. `image-gateway` remains the composition and persistence boundary. It owns
   admission, PostgreSQL leases, provider-task observations, canonical
   reduction, quota, and economics. A provider adapter cannot mutate any of
   them directly.

`image-provider-test-support` is a dev-only conformance harness. It is never
registered as a production provider.

## Dependency Direction

```text
image-provider-contracts
        ^
        |
image-provider-sdk      image-cli-runtime
        ^                       ^
        |                       |
provider adapters --------------+
        ^
        |
image-gateway (composition, persistence, scheduling, economics)

image-provider-test-support -> image-provider-sdk
```

No lower crate may depend on Axum, SQLx, gateway errors, tenant balances,
quota, ledgers, or public HTTP DTOs. Dynamic shared-library providers and a
second in-memory job queue are out of scope.

## Dispatch and Backpressure

Provider ports use return-position `impl Future + Send`, associated request
types, and generic artifact sinks. Concrete adapters are selected in a
compile-time composition enum. This keeps provider execution statically
dispatched, makes multi-thread worker compatibility a compile-time condition,
and avoids a boxed future for each operation. Runtime plugin-style trait
objects would require allocation or a second erased API and are deliberately
excluded.

PostgreSQL remains the durable queue and fencing authority. `FOR UPDATE SKIP
LOCKED` is used only for queue-like claims; PostgreSQL explicitly documents
that it produces an inconsistent view and is therefore unsuitable as a
general consistency mechanism. In-process wakeups, when added, must be bounded
and cannot carry correctness authority.

## Operation Model

Each provider descriptor contains one or more operation descriptors:

```text
provider + model + media + operation
  command schema and output schema
  inline or remote-task completion
  none, final, or partial streaming
  inline-bounded or streamed artifact delivery
  idempotency, poll, cancel, and callback controls
  billing metric and one-output submission limit
```

The selected descriptor revision, command schema, adapter revision, output
index, and command hash are bound before execution. A provider upgrade cannot
reinterpret an in-flight submission.

Provider execution is always one output per submission. Batch API parameters
such as OpenAI `n` are projected into stable output slots before provider
execution.

## Inline and Remote Tasks

Inline providers return terminal evidence only after a bounded artifact sink
has finalized one durable manifest.

Remote providers have distinct `submit`, `poll`, `cancel`, callback
verification, and artifact materialization operations. Before submit, the
platform reserves one durable intent and freezes the provider idempotency key.
A submit acknowledgement attaches the stable remote operation reference to
that intent and releases the short executor lease. Providers without a real
idempotency token must execute submit through the existing durable helper
journal. The platform retains its durable capacity allocation while the task
is waiting.

The durable remote-task state machine is:

```text
provider_waiting -> provider_waiting
provider_waiting -> artifact_ready -> succeeded
provider_waiting -> failed | canceled | uncertain
```

`cancel_requested` is intent, not terminal evidence. An ambiguous cancel does
not become `canceled`. Poll and verified callback observations are append-only
and deduplicated; callbacks may wake polling but cannot directly grant artifact
or terminal authority.

Temporary download URLs, authorization headers, credentials, and arbitrary
provider response JSON are forbidden from durable task records. Persisted
receipts contain only bounded identifiers and redacted evidence.

Remote failure, uncertainty, and confirmed cancellation produce one
`remote_provider_observation` resolution decision in the same transaction
that terminalizes the parent execution, releases capacity, and enqueues the
existing reducer. Artifact readiness alone is not success. The composition
layer must first publish a verified deterministic artifact authority and result
manifest under the live poll fence, then resolve it through the same canonical
decision path.

## CLI Runtime

Command execution uses an argv vector and never a shell string. Executables
and working directories are absolute, the environment is cleared before an
explicit allowlist is applied, stdin is bounded, and stdout/stderr are bounded
or discarded by policy. Tokio recommends absolute executable paths and
explicit environment control for `Command`.

Timeout and cancellation cleanup is explicit:

```text
TERM process group -> bounded grace -> KILL process group -> wait -> verify exit
```

`kill_on_drop` is only defense in depth because Tokio documents it as
best-effort. Provider output is read from a no-follow regular file with a hard
limit and copied through a fixed-size buffer while hashing. Runtime results are
sealed metadata or durable manifest references, never a video-sized `Vec<u8>`.

Dropping or aborting the runtime future activates a process-group guard that
kills the leader and original process group and starts a bounded reap. Unix
process groups cannot contain a descendant that deliberately calls
`setsid(2)`, and post-exit size checks cannot prevent arbitrary workspace disk
amplification. Production therefore still requires an OS sandbox plus
filesystem quota. These are deployment controls, not properties claimed by
the Rust wrapper.

The first runtime is Unix-only. Linux executable-fd execution and production
sandbox isolation remain deployment concerns; macOS development execution must
use an immutable, administrator-controlled executable directory. A local
process group is not claimed as hostile multi-tenant isolation.

## Rejected Alternatives

- Do not create all target crates as empty shells. A crate is introduced only
  when code and an enforced dependency boundary move into it.
- Do not extend the legacy job-level `ImageGenerator` with optional polling or
  cancellation. Its OpenAI image DTO and `Vec<GeneratedImage>` result are not a
  general media provider port.
- Do not add dynamic Rust plugins, `Arc<dyn Provider>`, `BoxFuture`, or an RPC
  provider daemon before an external deployment requirement exists.
- Do not keep remote tasks alive by extending the local executor lease.
- Do not let provider adapters write SQL, economic receipts, customer
  artifacts, or canonical terminal decisions.
- Do not store provider-native catch-all JSON or temporary artifact URLs.
- Do not reuse queue `SKIP LOCKED` queries to prove billing consistency.

## Acceptance Gates

1. Existing OpenAI Images request/response snapshots and Codex V2 durable
   protocol remain unchanged.
2. The active Codex operation descriptor is validated in the production
   request projection; existing public API compatibility snapshots remain
   unchanged.
3. A scripted SDK harness proves inline and remote port semantics. A real
   PostgreSQL suite proves submit reservation, attach-without-resubmit, pending
   polling, callback/observation idempotency, cancellation uncertainty,
   canonical terminal reduction, and single artifact authority.
4. Poll leases are fenced by owner and epoch. Waiting tasks do not hold an
   executor lease.
5. A large CLI output is copied with bounded memory; timeout and observer
   failure reap the entire process group.
6. `cargo tree` proves provider SDK and contracts contain no Axum, SQLx, Tokio,
   billing, or concrete provider dependency. CLI runtime contains no provider,
   SQL, HTTP, or economics dependency.
7. Workspace tests, clippy, process crash/attach tests, idempotency replay,
   multi-output reduction, and PostgreSQL invariants remain green.

## Compatibility Boundary

The current HTTP response contract is named
`factory.openai-compatible.images.response.v1`, not strict upstream Images
v1. Existing snapshots intentionally preserve known gaps such as completed SSE
usage and partial-image streaming. A provider descriptor may reference a
strict official schema only after those fixtures close the gaps; CLI execution
contracts and public HTTP compatibility are versioned independently.

## Primary References

- Rust Reference, dyn compatibility:
  https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility
- Rust Reference, return-position `impl Trait` in traits:
  https://doc.rust-lang.org/stable/reference/types/impl-trait.html
- Tokio process `Command`:
  https://docs.rs/tokio/latest/tokio/process/struct.Command.html
- Tokio bounded MPSC channel:
  https://docs.rs/tokio/latest/tokio/sync/mpsc/fn.channel.html
- Tower `Service` readiness and backpressure:
  https://docs.rs/tower/latest/tower/trait.Service.html
- PostgreSQL `SELECT`, including `SKIP LOCKED`:
  https://www.postgresql.org/docs/current/sql-select.html
