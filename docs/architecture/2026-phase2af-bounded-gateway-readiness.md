# Phase 2AF: Bounded Gateway Readiness

Date: 2026-07-16

Status: implemented and verified without activating or calling a paid provider.
The OpenAI-compatible Images API contract and the existing `/healthz` response
are unchanged.

## Decision

Expose two different process signals:

| Route | Meaning | Dependency access |
|---|---|---|
| `GET /healthz` | the HTTP process is alive | none |
| `GET /readyz` | the gateway can read its durable control plane | one bounded PostgreSQL query |

`/readyz` returns HTTP 200 only when the PostgreSQL readiness projection can be
read before `GATEWAY_READINESS_TIMEOUT_MS`. A store error or timeout returns
HTTP 503 with a fixed body and no internal error detail.

Kubernetes distinguishes these signals: readiness failure removes a Pod from
Service traffic, while liveness is used to decide whether to restart it.

- <https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/>

The Kubernetes probe timeout defaults to one second. The gateway default is
500 milliseconds to leave time for the outer HTTP probe and local scheduling.
That 500 millisecond choice is a local budget inference, not a Kubernetes
recommendation. Operators can set a value from 1 through 60000 milliseconds
and must configure the outer probe timeout above it.

## HTTP Contract

Ready response:

```json
{
  "status": "ready",
  "provider_profiles": {
    "configured": 0,
    "active": 1,
    "draining": 0,
    "blocked": 0
  }
}
```

Unavailable response:

```json
{
  "status": "not_ready",
  "provider_profiles": null
}
```

Both responses set `Cache-Control: no-store`. The route requires no bearer
token because an orchestrator must be able to probe it independently from
tenant and administrator credentials. The gateway still refuses non-loopback
binds; a TLS reverse proxy or workload network policy owns external exposure.

The response never includes:

- profile keys or execution profile IDs;
- provider or account identifiers;
- credential references or revisions;
- executable, digest, filesystem, or artifact identity; or
- SQL and store error details.

## Provider State Is Diagnostic

Profile counts do not decide the HTTP status. This is intentional.

The platform currently has durable observed state but no durable desired
activation policy. A `configured` profile may be deliberately inactive, and a
`blocked` profile may be held back during rollout. Treating either count as a
global 503 would allow an inactive Dreamina profile to remove the Codex-backed
OpenAI API from service.

A future rollout controller may add an explicit desired-state policy. Until
then, database reachability is the only readiness gate and profile counts are
bounded diagnostics.

Zero remote profiles is a valid ready state.

## Storage Boundary

The HTTP state receives only `ProviderProfileReadinessStore`, whose sole method
returns four counts. It cannot register, heartbeat, drain, withdraw, claim,
submit, poll, settle, or bill work.

The PostgreSQL implementation executes one aggregate statement and returns one
row. It does not load all profiles into the gateway and fold them in Rust.
Migration `0030_provider_profile_readiness_projection.sql` defines the
authoritative per-profile projection once; both the detailed internal list and
the HTTP aggregate read that view.

The view applies this precedence:

1. `blocked`: the immutable profile graph is disabled, mismatched, or outside
   the supported 1 through 1024 runtime-lane contract;
2. `active`: at least one unexpired active submitter and poller exist;
3. `draining`: no active pair exists and at least one unexpired runtime drains;
4. `configured`: none of the preceding conditions applies.

Lease expiry uses one `statement_timestamp()` value for the entire statement,
so every row is classified against the same database clock. PostgreSQL
documents that this function returns the start time of the current statement:

- <https://www.postgresql.org/docs/current/functions-datetime.html>

The one-row clock CTE is materialized to make its single evaluation explicit.
The profile CTE is left foldable so the optimizer can avoid an unnecessary
intermediate copy. PostgreSQL documents both the optimization restriction from
`MATERIALIZED` and its explicit single-calculation semantics:

- <https://www.postgresql.org/docs/18/queries-with.html>

The runtime-lease join is supported by the existing
`provider_runtime_leases_profile_role_idx`, while PostgreSQL remains free to
choose a different plan for small tables. Heartbeat timestamp columns remain
outside that index, so probe support does not add heartbeat index churn.

## Timeout And Cancellation

The timeout covers both pool acquisition and query execution. It prevents an
HTTP probe from waiting indefinitely when the pool or database is stalled.

SQLx documents that cancellation during pool acquisition may discard a
connection rather than return uncertain state to the pool:

- <https://docs.rs/sqlx/latest/sqlx/pool/struct.Pool.html>

For that reason this endpoint is an orchestrator probe, not a high-rate metrics
API. It adds no retry loop: retries under database distress would increase
connection churn and hide the real readiness failure. Provider profile metrics
belong in the later observability phase.

## Verification

Unit and HTTP tests prove:

- `/healthz` remains dependency-free with its original body;
- empty in-memory provider configuration is ready;
- success returns only four aggregate counts;
- store errors and stalled futures return 503 without internal details;
- stalled probes obey the configured timeout;
- both results disable caching; and
- OpenAPI documents 200 and 503 responses.

Real PostgreSQL 18 tests prove:

- migration `0 -> 30`;
- repeatable concurrent fresh migration;
- the readiness view's exact eight-column projection;
- configured, active, draining, blocked, and withdrawn transitions;
- blocked precedence over runtime state; and
- detailed and aggregate readers share the same status projection.

A production-process smoke test starts the real gateway binary against an
isolated migrated schema and requires both `/healthz` and `/readyz` before
continuing. No external provider process or network API is called.

These checks establish the stated contracts. They do not establish a universal
latency or throughput claim; that requires representative production profile
cardinality and database load.

## Explicit Limits

This phase does not add:

- desired provider activation state;
- provider-aware global traffic policy;
- automatic daemon replacement;
- circuit breaking, cooldown, or account rotation;
- a public provider inventory endpoint;
- high-cardinality readiness metrics or alerts; or
- Dreamina, Grok, Seedance, or paid-provider activation.

## Next Gate

The next platform gate should make durable provider desired state explicit
before any readiness count can affect routing. That work must define rollout,
maintenance, and partial-provider failure semantics without coupling the
OpenAI-compatible API to a specific CLI.
