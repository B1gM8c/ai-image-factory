# Phase 2T: Provider-Neutral Poll Daemon

Date: 2026-07-16

Status: implemented and verified with adversarial unit tests plus real
PostgreSQL 18 concurrent-claim integration tests. This phase activates no
provider, credential, CLI query command, service binary, route, billing
behavior, or external call.

Follow-up: Phase 2U now provides the active, frozen, redacted runtime profile
and derives the daemon lane bound from durable provider-account capacity:
[`2026-phase2u-active-poll-runtime-profile.md`](2026-phase2u-active-poll-runtime-profile.md).

Phase 2X composes this daemon into a runnable but inactive single-profile
process and verifies it against real PostgreSQL plus a local fake CLI:
[`2026-phase2x-inactive-provider-poll-service.md`](2026-phase2x-inactive-provider-poll-service.md).

## Scope

Phase 2T adds the reusable lifecycle boundary around the Phase 2R
`ProviderPollOrchestrator`:

```text
one immutable provider/account orchestrator
  -> N fixed async poll lanes
  -> each lane calls the bounded PostgreSQL claim directly
  -> observed work immediately tries the next claim
  -> idle work sleeps with equal jitter
  -> iteration errors sleep with capped exponential full jitter
  -> shutdown stops new iterations and drains active iterations
  -> drain timeout cancels remaining provider futures
```

The daemon is a library component. It deliberately does not add a runnable
provider process before an activated provider has a verified query driver,
credential binding, process-containment contract, and media contract.

## Ownership Boundaries

`ProviderPollDaemon<I>` depends on one narrow static-dispatch port:

```rust
pub trait ProviderPollIteration {
    type Error;
    fn run_once(&self) -> impl Future<Output = Result<ProviderPollRun, Self::Error>> + Send;
}
```

`ProviderPollOrchestrator<S, D, F>` implements this port directly. The daemon
does not import a database pool, provider SDK request, artifact stager, lease,
credential, command codec, or provider-specific error.

The hot path has no boxed futures, trait objects, dynamic provider registry, or
in-process work queue. Each lane invokes the existing bounded
provider/account-scoped PostgreSQL claim. Therefore:

- PostgreSQL remains the only durable scheduling and lease authority;
- `FOR UPDATE SKIP LOCKED` still elects one owner per due task;
- the orchestrator still owns heartbeat, deadline, artifact, and observation
  semantics; and
- the daemon cannot reorder or manufacture provider work.

`ProviderPollDaemonConfig` contains only lifecycle policy:

```text
max_in_flight
idle_delay
error_base_delay
error_max_delay
shutdown_drain_timeout
```

Construction rejects zero lanes, more than 1024 lanes, zero delays, delays over
24 hours, and an error base greater than its cap.

## Capacity Model

`max_in_flight` creates exactly that many long-lived Tokio tasks. It is an
upper bound on concurrent `run_once` futures, not a second semaphore layered
over the database claim.

Future service composition must derive this value from the already durable
provider-account capacity/profile binding. The daemon does not read or mutate
capacity by itself. Capacity allocation, provider poll materialization, and
daemon lane count remain separate limits:

```text
durable account capacity  -> service chooses lane count
poll lanes                -> concurrent task claims/provider polls
materialization semaphore -> concurrent artifact streams only
```

This prevents lightweight pending polls from consuming artifact
materialization capacity while still bounding provider query concurrency.

## Pacing

An observed task resets the error streak and immediately attempts another
claim. This avoids adding an artificial sleep while a due backlog exists.

An idle claim resets the error streak and sleeps in the upper half of the
configured idle interval:

```text
[idle_delay / 2, idle_delay]
```

An iteration error increments a lane-local streak and sleeps with capped
exponential full jitter:

```text
cap = min(error_base_delay * 2^(streak - 1), error_max_delay)
sleep = random(1 ns, cap)
```

The jitter sample is derived from one process-local UUID seed, lane index, and
delay sequence using SHA-256. It is deterministic for a supplied seed in tests,
different across daemon instances in normal construction, and evaluated only
on idle or error paths. Successful observed work performs no jitter hash.

Error streaks are lane-local so one slow or failing lane cannot impose a shared
cooldown on independent in-flight work. Provider-returned retry scheduling
remains durable in `next_poll_at_ms`; daemon error backoff applies only when the
iteration itself could not durably resolve its claim.

## Shutdown And Cancellation

A Tokio `watch<bool>` channel broadcasts one shutdown state to every lane.
Each lane checks it:

- before starting a new iteration; and
- while sleeping after idle or error.

Shutdown does not race-cancel an active `run_once`. Active orchestrators may
finish heartbeat, artifact authority, and observation work during the configured
drain interval.

The daemon owns all lane tasks in one `JoinSet`. If the drain timeout expires,
it aborts every remaining task and waits for cancellation to finish before
returning `ShutdownDrainTimedOut`. Dropping the orchestrator future then invokes
the Phase 2R cancellation boundary. An activated CLI query driver must still
prove that future cancellation terminates its complete process group.

Any lane panic or unexpected early exit stops the daemon, aborts its peers,
waits for their cancellation, and returns `LaneTerminated`. A silently reduced
capacity mode is not accepted.

## Logging Boundary

Iteration errors are counted and logged by associated Rust error type, lane,
and consecutive-error count. The daemon does not format the provider/store
error value itself. This avoids making a generic lifecycle layer an accidental
sink for provider response bodies, commands, credential paths, or other
provider-specific diagnostics.

Provider-aware error codes and redacted details remain the responsibility of
the orchestrator, driver, and telemetry composition layers that own those
contracts.

## Cost Model

For one daemon instance:

- task memory is O(`max_in_flight`);
- each active lane holds one `run_once` future;
- no channel carries jobs or artifacts;
- no polling coordinator lock exists;
- observed work performs no daemon sleep or jitter hash;
- idle/error work performs one fixed-size SHA-256 hash before sleeping; and
- shutdown uses one capacity-one watch state plus the existing `JoinSet`.

The database cost of each iteration is unchanged from Phase 2R. Increasing lane
count increases concurrent bounded claims; it does not change claim query
shape, candidate-window size, lease authority, or transaction duration.

These are structural bounds, not throughput measurements. They do not prove a
SOTA or industry-leading claim. Production defaults require measured p50/p95/p99
claim latency, provider latency, CPU, allocation, lock wait, WAL, error
recovery, and fairness under production-sized mixed workloads.

## Verification

Eight unit and adversarial tests prove:

- invalid lane and duration configuration fails before spawning;
- idle and error jitter are bounded and reproducible with a fixed seed;
- idle claims are paced instead of spinning;
- active iterations never exceed the configured lane count;
- a transient iteration error backs off and then recovers;
- shutdown waits for an active iteration to finish;
- drain timeout aborts and drops a pending iteration future; and
- a lane panic fails closed and stops the daemon.

The first real PostgreSQL 18 integration test creates six due remote tasks
under one provider/account scope and runs three daemon lanes. It proves:

- exactly six provider polls occur;
- exactly six waiting observations commit;
- every submission receives exactly one poll observation;
- all poll leases are released; and
- the daemon reports six observed tasks and zero iteration errors.

The second real PostgreSQL test starts two independent one-lane daemons under
the same provider/account scope with distinct owners and blocked provider
calls. It proves:

- both daemons can hold leases concurrently;
- the two leases cover distinct tasks and owners;
- each daemon invokes its provider driver exactly once;
- each submission receives exactly one observation; and
- both leases are released after durable observation.

This is a non-overlap and coexistence proof, not a statistical throughput
fairness benchmark.

No paid provider, CLI, external network, production credential, or production
artifact root is used.

## Evidence Basis

Tokio 1.52.3 documents `JoinSet` as an owned collection of runtime tasks,
documents that dropping it aborts contained tasks, and provides explicit
abort-and-drain operations:
<https://docs.rs/tokio/1.52.3/tokio/task/join_set/struct.JoinSet.html>.

Tokio documents `watch` as a multi-consumer channel retaining only the latest
state, including the `borrow_and_update` pattern used before waiting for
changes:
<https://docs.rs/tokio/1.52.3/tokio/sync/watch/index.html>.

Tokio documents that an elapsed `timeout` cancels the wrapped future by
dropping it:
<https://docs.rs/tokio/1.52.3/tokio/time/fn.timeout.html>.

The Amazon Builders' Library recommends jitter to reduce correlated retries and
timer-driven traffic spikes, and notes that stable seeds can improve
reproducibility:
<https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/>
and
<https://aws.amazon.com/builders-library/minimizing-correlated-failures-in-distributed-systems/>.

PostgreSQL documents `SKIP LOCKED` as suitable for avoiding lock contention
among multiple consumers of a queue-like table:
<https://www.postgresql.org/docs/18/sql-select.html#SQL-FOR-UPDATE-SHARE>.

These sources justify the selected runtime primitives. They do not establish a
performance-leadership claim for this repository.

## Explicit Limits

Phase 2T does not provide:

- a runnable provider poll service or environment-variable contract;
- durable loading or live resizing of lane count;
- cross-account lane sharing or an in-memory account scheduler;
- callback-driven wakeup, `LISTEN/NOTIFY`, or adaptive idle delay;
- metrics export, tracing spans, or production alert thresholds;
- sustained multi-daemon fairness and crash-injection benchmarks;
- video media validation or Seedance result materialization;
- Dreamina query, Grok query, or any provider activation; or
- production-scale benchmark evidence.

## Follow-up

Phase 2U closes selected provider/account profile loading. Phase 2V implements
and locally verifies the inactive Dreamina image poll driver:
[`2026-phase2v-inactive-dreamina-image-poll-driver.md`](2026-phase2v-inactive-dreamina-image-poll-driver.md).

Phase 2W adds exclusive descriptor-relative attempt ownership and startup
crash recovery:
[`2026-phase2w-exclusive-cli-attempt-workspace.md`](2026-phase2w-exclusive-cli-attempt-workspace.md).

Phase 2X closes the inactive service composition, exact account capability
binding, redacted lifecycle diagnostics, and real PostgreSQL process
verification. Credential provisioning, production health semantics, query
rate/cooldown policy, and provider activation remain open. Dreamina and Grok
remain disabled.
