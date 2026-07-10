# Scheduler And Quota Runtime Design

> Historical P0 design and implementation record. The authoritative scheduler,
> work-item, fencing, quota, metering, and billing target is
> `2026-ai-image-factory-target-architecture.md`.

## Goal

Introduce a durable scheduling and quota lifecycle for `crates/image-gateway`
without changing the current OpenAI Images-compatible surface.

The first implementation keeps `POST /v1/images/generations` and
`POST /v1/images/edits` synchronous. Internally, those requests should move from
"in-process semaphore + charge before provider execution" to a recoverable
workflow:

```text
validate -> reserve quota -> create job -> run provider -> persist result facts
-> commit quota on success
                         -> release quota on failure
```

## First-Phase Scope

The first phase intentionally stays small:

- Keep the only active provider as `openai-codex`.
- Keep existing response bodies, errors, SSE behavior, and quota headers.
- Keep the existing in-process scheduler as the local capacity gate.
- Add durable job, reservation, and metering records around the synchronous
  image execution path.
- Add state-machine and store tests before changing request behavior.

The first phase does not expose public `/v1/jobs`, does not add webhooks, does
not add provider account pools, and does not introduce Apalis or Temporal as a
runtime dependency.

## Module Boundaries

```text
crates/image-gateway/src/
  api/
    images.rs          # OpenAI-compatible synchronous facade
  jobs/
    mod.rs             # public module exports
    state.rs           # job, attempt, reservation state machines
    store.rs           # repository trait and in-memory store
    service.rs         # reserve/create/run/commit/release orchestration
  quota/
    mod.rs             # quota reservation contracts
  metering/
    mod.rs             # immutable platform facts
  usage/
    mod.rs             # legacy quota headers and store compatibility
```

Later phases can split `scheduler/postgres.rs`, `workers/`, `storage/`,
`billing/`, and `provider_accounts/` out of the same boundaries. The important
rule is that provider adapters never mutate quota, billing, or job state
directly.

## State Model

The target job lifecycle is:

```text
accepted -> reserved -> queued -> leased -> running -> provider_waiting
                                      -> artifact_ready -> succeeded
                                      -> failed | canceled | timed_out
```

The first synchronous phase uses this shorter subset:

```text
accepted -> reserved -> running -> succeeded
                              -> failed
```

Quota reservation states:

```text
reserved -> committed
reserved -> released
reserved -> expired
```

`commit` is legal only after a result is deliverable. `release` is used for
provider failure, timeout, queue rejection, and platform failure before a
deliverable artifact exists. `expire` is reserved for background recovery.

## Durable Data

Existing tables are evolved additively. The first phase records enough data to
support recovery and future workers:

- `jobs`: tenant, request, operation, provider, model, state, requested units,
  charged units, reservation id, timestamps, and last error.
- `quota_reservations`: tenant, request, job, requested units, committed units,
  released units, state, and expiry.
- `usage_events`: legacy quota event stream used for existing rate-limit
  headers and rolling-window checks.
- `metering_events`: immutable facts for reservation and job outcomes.

Future tables are planned but not part of the first patch: `job_attempts`,
`job_leases`, `billing_ledger_entries`, `provider_accounts`,
`provider_account_leases`, `artifacts`, `audit_events`, and
`idempotency_requests`.

## Transaction Rules

Reserve:

1. Lock the tenant quota scope.
2. Count committed `usage_events` plus active, unexpired reserved units.
3. Reject with `429` if the requested units exceed either quota window.
4. Insert a `jobs` row in `reserved`.
5. Insert a `quota_reservations` row in `reserved`.
6. Emit a `metering_events` fact for `quota_reserved`.

Commit:

1. Lock the reservation.
2. Require state `reserved`.
3. Insert a legacy `usage_events` charge for committed units.
4. Update reservation to `committed`.
5. Update job to `succeeded`.
6. Emit `quota_committed` and `job_succeeded` facts.

Release:

1. Lock the reservation.
2. Require state `reserved`.
3. Update reservation to `released`.
4. Update job to `failed`.
5. Emit `quota_released` and `job_failed` facts.

All commit/release operations must be idempotent by reservation id. Retrying the
same terminal transition must not double-charge.

## Worker Lease Direction

The durable worker phase will add `job_attempts` and `job_leases`. Lease
acquisition should use `FOR UPDATE SKIP LOCKED`, with a fencing token required
for heartbeat and completion:

```sql
UPDATE job_leases
SET heartbeat_at_ms = $now,
    expires_at_ms = $now + $ttl
WHERE lease_id = $lease_id
  AND lease_token = $lease_token
  AND released_at_ms IS NULL
  AND expires_at_ms > $now;
```

The first implementation keeps synchronous inline execution, so it only prepares
the job and reservation model.

## Security And Compatibility

- API keys remain hashed and redacted. A later phase should upgrade key hashes
  to a peppered/HMAC format.
- Provider credentials stay out of job, metering, and trace payloads.
- Prompt text, image bytes, API keys, admin tokens, and provider secrets must
  not be written to logs or metering events.
- Existing OpenAI-compatible responses and quota headers remain stable.

## Verification Matrix

First phase:

- State-machine tests for legal and illegal transitions.
- In-memory reservation tests for reserve, commit, release, and idempotent
  terminal operations.
- API tests proving provider success commits quota and provider failure releases
  quota.
- Existing API tests for auth, key isolation, quota headers, queue overflow,
  SSE, edits, OpenAPI, and Codex smoke remain valid.

Future Postgres phase:

- Concurrent reservations cannot oversell quota.
- Multiple workers cannot lease the same job.
- Expired leases cannot complete jobs.
- Stale workers with old lease tokens cannot commit results.
