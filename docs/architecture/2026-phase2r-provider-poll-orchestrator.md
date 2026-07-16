# Phase 2R: Fenced Provider Poll Orchestrator

Date: 2026-07-16

Status: implemented and verified with unit tests plus real PostgreSQL 18
integration tests. This phase activates no provider, credential, CLI query
command, daemon, route, billing behavior, or external call.

## Scope

Phase 2R adds the single provider-neutral orchestration boundary for one due
remote-task poll:

```text
provider/account-scoped claim
  -> recover committed authority, or invoke one provider poll
  -> heartbeat while external work is active
  -> stream through a controlled one-shot sink
  -> publish immutable artifact authority
  -> append one provider observation
  -> atomically project canonical terminal state
```

It composes existing database state machines. It does not introduce another
queue, workflow engine, distributed lock, dynamic provider registry, artifact
table, or transaction coordinator.

## Ownership Boundaries

`ProviderPollOrchestrator<S, D, F>` depends on three narrow ports:

- `ProviderPollStore`: claim, heartbeat, authority publication, and observation;
- `ProviderPollDriver`: one frozen provider poll invocation; and
- `ProviderArtifactStagerFactory`: provisional artifact materialization.

All three use generic static dispatch. Async trait methods use return-position
`impl Future`; there is no boxed future or trait-object dispatch in the poll
kernel. `ProviderPollOrchestratorConfig` carries the provider/account scope,
owner, lease duration, heartbeat interval, and process-local materialization
limit without an eight-argument positional constructor. Construction validates
the complete configuration before creating the Tokio semaphore, so invalid
limits return an error instead of reaching a runtime panic.

The stager does not receive `ProviderTaskLease`. It receives only
`ProviderArtifactStageContext`:

```text
submission_id
executor_execution_id
poll_lease_epoch
```

Provider credentials, command bytes, execution-profile metadata, and resource
policy fields do not cross into the artifact port.

## Claim And Recovery

`claim_due` still elects one provider/account-scoped task with the bounded
64-row candidate window and `FOR UPDATE SKIP LOCKED`. The claim query now also
left-joins the deterministic result manifest and artifact authority.

If immutable authority was committed by an earlier process but its
`artifact_ready` observation was not, the new lease carries the exact manifest
ID, authority ID, SHA-256, byte size, and media type. The orchestrator records
the artifact observation immediately:

```text
authority committed
  -> new claim returns committed fingerprint
  -> no provider poll
  -> no stager initialization
  -> artifact_ready and canonical success
```

This closes the authority-publication crash window without a second database
read, provider re-download, or waiting until the provider deadline.

The poll lease capability seal is versioned and covers the remote operation,
provider request ID, task state, cancel-request snapshot, task poll epoch,
frozen execution binding, owner, and lease epoch. Mutating a cloned lease's
decision fields is rejected before a database write.

## Deadline And Heartbeat

The claim computes remaining provider budget from the PostgreSQL clock and
stores it privately in `ProviderTaskLease`. The orchestrator wraps the entire
provider poll plus heartbeat future in a Tokio monotonic timeout using that
database-derived duration. The local monotonic deadline is conservatively
anchored before the claim begins, so claim and setup latency can shorten but
never extend the database budget.

The heartbeat interval must be positive and no greater than one third of the
lease duration. `MissedTickBehavior::Delay` prevents catch-up bursts: after a
delayed renewal, the next renewal remains one full interval away. Losing the
poll fence or exhausting the database-derived budget drops the in-flight
provider future and records no observation. The existing deadline resolver
remains authoritative for post-deadline quarantine.

This is an in-process cancellation boundary. An activated CLI driver must still
prove that dropping its future terminates the complete process group under the
Phase 2N containment contract.

## Controlled Artifact Sink

`ControlledProviderArtifactSink` is one-shot and has four internal states:

```text
Pristine -> Streaming -> Finalized
                     \-> Failed
```

The stager and materialization permit are both lazy. A pending poll that writes
no bytes:

- acquires no semaphore permit;
- creates no staging object or directory; and
- records only a waiting observation.

The first non-empty chunk acquires one fair Tokio semaphore permit and then
asynchronously initializes the stager. The permit remains held through artifact
streaming and finalization and is released before the short database
publication transaction.

The provider can complete only after exactly one finalization. The staged
authority must match the sink manifest by SHA-256, byte size, and media type.
The provider-returned `Completed` manifest must then equal the sink-finalized
manifest exactly. Any pending, failed, canceled, or error result after artifact
bytes were written becomes `uncertain`.

This phase's PostgreSQL integration stager is deliberately manifest-only. It
proves orchestration and database authority semantics, not physical object
durability. The production filesystem stager remains the next gate.

## Result Semantics

| Provider result | Required evidence | Durable outcome |
| --- | --- | --- |
| `Pending` | sink remains pristine | waiting with bounded next poll |
| `Completed` | exact finalized manifest and matching optional request ID | publish authority, then artifact ready |
| `Failed` | pristine sink, no-effect poll, non-ambiguous, non-retryable | failed |
| `Canceled` | pristine sink, durable cancel request, matching optional request ID | canceled |
| retryable error | pristine sink | waiting |
| ambiguous or non-retryable error | pristine sink | uncertain |
| any artifact/result contract drift | none | uncertain |

An omitted provider request ID is accepted because some provider query
responses do not repeat the submit request ID. If an ID is present, it must
equal the immutable attached request ID.

`PollObservation::Failed` is not trusted solely because it is a terminal enum
variant. Retryable, ambiguous, or unknown-effect failures are rejected as a
provider contract violation and become `uncertain`.

## Transaction And Crash Matrix

External provider and artifact I/O never runs inside a PostgreSQL transaction.

| Crash point | Durable state | Recovery |
| --- | --- | --- |
| Before provider result | waiting with an expiring lease | reclaim and poll |
| During provisional staging | no authority | reclaim; future epoch staging must clean orphan |
| After finalization, before authority | no authority | reclaim; replay policy remains provider-specific |
| After authority, before observation | waiting plus immutable authority | same-query recovery, no re-poll |
| During observation transaction | transaction rolls back | reclaim or exact replay |
| After terminal commit | canonical success and released capacity | normal terminal replay |

Authority publication still precedes the atomic observation/canonical
transaction. Exact publication acknowledgement is idempotent. Once authority
exists, contradictory failure or cancellation cannot win.

## Cost Model

A lightweight pending poll performs:

- one bounded scoped claim transaction;
- one provider poll;
- heartbeat point updates only if the call crosses an interval; and
- one observation transaction.

It allocates no artifact-sized buffer, staging object, or materialization
permit.

A completed poll adds streaming/hash/storage work in the stager and one
authority publication transaction. The poll kernel itself retains only fixed
metadata, the provider's current chunk, and one semaphore permit. The current
manifest-only integration stager buffers bytes for test convenience and is not
a production memory claim.

Committed-authority recovery adds no provider or storage call and no extra
database round trip beyond claim plus observation.

These are structural cost bounds. They do not prove throughput leadership.
Provider activation still requires p50/p95/p99 latency, allocation, CPU, WAL,
lock-wait, storage amplification, fairness, and mixed image/video load
measurements on production-sized data.

## Verification

Ten poll-kernel tests cover:

- fail-closed construction for zero or over-limit materialization capacity;
- lazy semaphore and stager acquisition on the first byte;
- pending without any materialization;
- publication before terminal observation;
- authority recovery without provider or stager calls;
- repeated heartbeat during a long pending call;
- provider-future cancellation after heartbeat fence loss;
- provider-future cancellation at the PostgreSQL-derived deadline;
- forged completed-manifest rejection; and
- retryable or ambiguous terminal-failure rejection.

Real PostgreSQL 18 tests cover:

- one completed fake-provider poll producing exactly one authority, manifest,
  artifact observation, canonical resolution decision, ready reduction, and
  capacity release; and
- a simulated crash after authority publication, followed by lease expiry and
  recovery to canonical success with zero provider polls and zero stager starts.

The existing PostgreSQL lease-splicing test now also proves that request ID,
cancel snapshot, task state, and task epoch mutations invalidate the lease
capability.

No paid provider, CLI, external network, or production credential is used.

## Evidence Basis

PostgreSQL documents `SKIP LOCKED` as appropriate for avoiding lock contention
among multiple consumers of a queue-like table:
<https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE>.

PostgreSQL row locks block competing writers and lockers until transaction end,
and its documentation recommends short transactions and consistent lock order:
<https://www.postgresql.org/docs/18/explicit-locking.html#LOCKING-ROWS>.

Tokio documents its semaphore as a fair asynchronous counting semaphore,
including owned permits for work that crosses async boundaries:
<https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html>.

Tokio documents `MissedTickBehavior::Delay` as scheduling the next tick one
full period after a delayed tick instead of producing a catch-up burst:
<https://docs.rs/tokio/latest/tokio/time/enum.MissedTickBehavior.html>.

The Rust Reference documents return-position `impl Trait` in traits as an
anonymous associated type. This permits static async port implementations
without boxed trait objects:
<https://doc.rust-lang.org/reference/types/impl-trait.html#return-position-impl-trait-in-traits-and-trait-implementations>.

These sources justify the selected primitives. They do not by themselves prove
that this repository is SOTA; that claim remains benchmark- and
production-evidence dependent.

## Explicit Limits

Phase 2R does not provide:

- a real epoch-staged filesystem or object-store artifact implementation;
- decoded image/video validation, dimensions, duration, or codec limits;
- video-capable executor authority and canonical-result schema;
- orphan staging cleanup or content-addressed immutable publication;
- a poll daemon, account rotation loop, adaptive pacing, or observability;
- a Dreamina query codec, Grok adapter, or any provider activation;
- a cancel orchestrator or callback-driven terminal fast path;
- Linux cgroup v2 containment for provider query processes; or
- production-scale mixed-load benchmark evidence.

## Next Gate

Phase 2S should implement a provider-neutral epoch-staged filesystem object
stager:

1. derive private provisional identity from execution ID and poll epoch;
2. stream without artifact-sized memory;
3. decode and validate supported media under explicit limits;
4. fsync provisional bytes and parent directories where required;
5. publish immutable deterministic authority without overwrite;
6. clean abandoned epochs with bounded garbage collection; and
7. prove crash behavior before and after object publication using real files
   and PostgreSQL.

Dreamina query integration remains inactive until this storage boundary is
verified.
