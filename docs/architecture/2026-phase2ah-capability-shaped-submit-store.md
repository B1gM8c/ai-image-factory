# Phase 2AH: Capability-Shaped Submit Store

Date: 2026-07-16

Status: implemented without changing SQL, migrations, HTTP contracts, provider
activation, or runtime configuration.

## Finding

`ProviderTaskStore` contains the complete durable remote-task surface:

- submit reservation and evidence;
- submit deadline and recovery scheduling;
- task attach and load;
- poll claim and heartbeat;
- artifact authority publication;
- cancellation;
- observation recording; and
- callback wakeups.

The submit orchestrator used only six of those methods. The submit service used
four additional scheduling methods, but its generic bound still required the
entire store, including poll, cancellation, artifact publication, and callback
capabilities.

That wide bound had no immediate runtime cost, but it was an ownership problem:
a focused submit implementation, test double, or future persistence adapter
could not satisfy the compiler without also owning unrelated poll behavior.
The poll orchestrator already avoided this problem through its narrow
`ProviderPollStore` port.

## Decision

Phase 2AH adds two consumer-owned static ports in
`provider_tasks/submit/store.rs`.

`ProviderSubmitOrchestrationStore` owns exactly the durable effects used by the
sole submit orchestrator:

| Capability | Purpose |
| --- | --- |
| `acquire_submit` | elect dispatch, attach, observe, busy, or terminal authority |
| `record_submit_failure` | persist rejected or outcome-unknown evidence |
| `record_submit_receipt` | persist a matched remote operation |
| `quarantine_submit_receipt` | retain a mismatched receipt without attachment |
| `attach` | atomically create the pollable remote task |
| `load` | replay an already attached task |

`ProviderSubmitSchedulingStore` owns only the service loop's database scheduler:

| Capability | Purpose |
| --- | --- |
| `resolve_due_submit_deadline` | terminalize one due submit deadline |
| `claim_submit_recovery` | claim one scoped replayable recovery |
| `heartbeat_submit_recovery` | renew exact recovery authority |
| `defer_submit_recovery` | replayably defer unresolved evidence |

The dependency direction is now:

```text
ProviderSubmitService
  -> ProviderSubmitSchedulingStore
  -> ProviderSubmitOrchestrator
       -> ProviderSubmitOrchestrationStore

PostgresProviderTaskStore
  -> existing ProviderTaskStore implementation
  -> blanket adapters for both narrow submit ports
```

The PostgreSQL store remains the production implementation. The blanket
adapters preserve its existing method bodies and error mapping.

## Why The Port Belongs To The Consumer

The submit orchestrator determines which persistence capabilities form one
application boundary. Keeping the port beside that consumer prevents a
database module or future provider adapter from expanding it for their own
convenience.

This is an incremental dependency inversion, not a speculative crate split.
The existing broad store remains available to integration tests and lower-level
database verification while consumers move to capability-shaped bounds.

## Runtime Cost

Both ports use generic trait bounds and return-position `impl Future` in the
same style as the existing poll port.

The implementation adds:

- no trait object;
- no virtual dispatch;
- no boxed future;
- no heap allocation;
- no runtime registry;
- no clone or serialization step; and
- no database or network call.

The Rust Reference specifies that return-position `impl Trait` represents an
unnamed concrete type and can avoid the allocation and dynamic dispatch of a
boxed trait object:

- <https://doc.rust-lang.org/reference/types/impl-trait.html>

The Rust book describes generic trait bounds as monomorphized static dispatch,
in contrast to trait-object dynamic dispatch:

- <https://doc.rust-lang.org/book/ch18-02-trait-objects.html>

These language properties establish the dispatch boundary. They do not imply a
measurable throughput improvement; the SQL path is intentionally unchanged.

## Adversarial Alternatives

### Move the 5,000-line PostgreSQL file first

Rejected for this phase. Moving methods among files without narrowing consumer
requirements changes physical layout but not coupling. The capability boundary
must be compiler-visible before a later storage-module split has architectural
meaning.

### Split `ProviderTaskStore` immediately

Deferred. Replacing the broad production trait in one change would touch every
PostgreSQL test and recovery helper while adding no behavior. Blanket adapters
let consumers migrate independently and keep this change reviewable.

### Use a dynamic provider registry

Rejected. Submit and poll binaries already select one compiled provider/runtime
profile at their composition roots. Dynamic dispatch would add a hot-path
indirection without solving persistence ownership.

### Duplicate a submit-only PostgreSQL implementation

Rejected. A second SQL implementation would create drift between orchestration,
recovery, and direct invariant tests. One durable implementation remains
authoritative.

## Verification

A compile-time test defines:

- a store implementing only `ProviderSubmitOrchestrationStore`; and
- a different store implementing only `ProviderSubmitSchedulingStore`.

Neither implements `ProviderTaskStore`. This proves the narrow ports are
independent capabilities rather than aliases that secretly retain the broad
bound.

The submit daemon, driver, and orchestrator unit suite remains unchanged and
passes through the new bounds. Verification completed on 2026-07-16:

- the focused submit suite passed 9 tests;
- the real PostgreSQL provider-task suite passed 81 tests serially;
- the gateway library passed 239 tests;
- executor integration passed 46 tests;
- migration integration passed 10 tests;
- API integration passed 58 tests;
- process integration passed 7 tests;
- the full workspace passed with serial database tests; and
- Clippy passed for the full workspace and all targets with warnings denied.

The real Codex image-generation smoke remains intentionally uninvoked because
it consumes external quota. This structural change does not require a provider
side effect to verify its behavior.

## Explicit Limits

This phase does not:

- split the production PostgreSQL implementation into physical files;
- remove the compatibility `ProviderTaskStore`;
- change a query, transaction, lock, lease, deadline, or state transition;
- add a provider registry or provider-specific persistence port;
- add desired state, credential rotation, pricing, quota, or billing APIs;
- activate Dreamina, Grok, Seedance, or any paid provider; or
- claim latency, throughput, or industry leadership.

The next structural extraction should use these compiler-enforced capability
ports when moving PostgreSQL ownership. It should not introduce a second SQL
implementation or change the durable protocol merely to obtain smaller files.
