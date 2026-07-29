# Admin Read Models

Status: implemented for platform-owner and project-member console reads. Cross-user inspection
remains platform-owner only.

## Decision

Operational reporting is a separate read boundary, not a method added to the admission,
settlement, or provider write stores.

```text
Next.js console
  -> same-origin BFF allowlist
  -> Axum platform-owner JWT guard
  -> AdminReadStore
  -> isolated PostgreSQL read pool
  -> durable facts and bounded projections
```

The `/admin/v1/*` handlers accept only an identity JWT containing the exact `admin:*` scope.
The legacy static admin token is intentionally rejected even when the transition switch is on.
The `/v1/console/*` handlers require `workspace:read` and derive organization and project scope
from the authenticated principal. Only a platform owner may select another user.

## Code Ownership

```text
crates/image-gateway/src/admin_read/
  mod.rs       Read contracts and repository port
  postgres.rs  PostgreSQL projection adapter

crates/image-gateway/src/api/admin_read.rs
  HTTP parsing, platform-owner authorization, and request timeout

apps/admin-console/src/lib/admin/
  Browser-safe TypeScript contracts and integer formatting

apps/admin-console/src/components/admin-*.tsx
  Operational views consuming only BFF responses
```

Provider-native public API contracts remain in `crates/api-contracts`; admin projections do
not become provider request DTOs.

## API Surface

| Endpoint | Maximum window | Authority |
| --- | ---: | --- |
| `GET /admin/v1/overview` | 7 days | Global operational cohort |
| `GET /admin/v1/billing/summary` | 31 days | Global financial facts by tenant and currency |
| `GET /admin/v1/provider-accounts` | Current snapshot | Redacted configuration, runtime, and capacity |
| `GET /admin/v1/scheduler/queues` | 7-day uncertain lookback | Durable queue stages and due work |
| `GET /admin/v1/jobs` | 31 days | Global keyset-paginated job list |
| `GET /admin/v1/jobs/{job_id}/economics` | One repeatable-read snapshot | Complete customer and provider economics |
| `GET /v1/console/jobs/{job_id}/economics` | One repeatable-read snapshot | Project-authorized customer economics only |

All windows use PostgreSQL `transaction_timestamp()` and the half-open interval
`[from_ms, to_ms)`. Job pagination uses `(created_at_ms, job_id)` and carries the first
page's `to_ms` into subsequent pages, so a changing wall clock cannot move the result set.

## Fact Semantics

The API does not add similarly named tables together.

- Job cohorts and terminal elapsed time come from `jobs`.
- Charged quota usage comes from `usage_events`, grouped by metric, unit, and outcome.
- Rated quantity and customer amount come from `rated_usage` joined to its immutable meter fact.
- Period financial totals come only from sealed ledger transactions and their positive posting.
- Current credit, held, captured, and available balances come from `billing_accounts`; these are
  current/cumulative snapshots, not period flows.
- Provider costs are reported separately with receipt coverage. Missing cost evidence is unknown,
  not zero, and no margin is calculated from incomplete coverage.
- Work item, provider submission, and remote-task states remain separate stages.

All money, quantities, and unbounded counts are decimal JSON strings. The console formats them
with `BigInt`; it never converts monetary micros to JavaScript `number`.

## Request Economics

The request drawer is backed by one `REPEATABLE READ READ ONLY` snapshot. It projects the frozen
customer quote and quote lines, hold state, native usage facts, customer rating, sealed ledger
transactions, and administrator-only provider cost evidence. Its state machine is derived from
durable facts:

```text
contract != v4 -> legacy_contract
v4 + no quote   -> awaiting_quote
v4 + quote      -> quoted
v4 + usage      -> metered
v4 + rating     -> rated
```

Provider observations are attributed to a job only when their immutable fact set references one
distinct job. Observations spanning multiple jobs are returned as `shared` without an attributed
amount. Allocated provider costs remain a separate basis. Receipt-only historical costs are
explicitly `legacy_unverified`.

The member DTO structurally omits `provider_costs` and filters the ledger to customer charges.
It does not merely hide those fields in the React view. Missing and unauthorized job IDs both
return the same `404 resource_not_found`, preventing existence disclosure.

For member reads, omitting `project_id` shows only jobs directly attributed to that user. Supplying
an authorized project allows the member to see that project's user-session, API-key, and service
account jobs. A project outside the principal's membership set fails before any projection query.

## Provider Safety

Provider account responses expose account/profile identifiers, configuration state, runtime
readiness, and capacity. They never serialize `credential_ref`, authentication hashes, runtime
owners, provider request IDs, or raw evidence. Upstream member quota stays `unknown` until a
provider-specific authoritative collector records limit, remaining value, reset time, and
observation time.

## PostgreSQL Isolation

The admin adapter uses a dedicated pool with three connections, one-second acquisition timeout,
500 ms statement timeout, 100 ms lock timeout, two-second idle transaction timeout, and
`default_transaction_read_only=on`. Each aggregate uses a short `REPEATABLE READ READ ONLY`
transaction. The API layer adds a 750 ms wall-clock timeout.

Production should set `GATEWAY_ADMIN_READ_DATABASE_URL` to a PostgreSQL role that has only
`SELECT` on the required tables/views. Falling back to the main Gateway URL exists for local
development and does not provide database-role isolation.

Migration `0035_admin_read_indexes.sql` adds bounded-window, keyset, due-work, and uncertain-state
indexes. An asynchronous rollup system is deliberately deferred until production-sized
`EXPLAIN (ANALYZE, BUFFERS)` data proves that indexed 24-hour scans cannot meet the latency target.

## Non-Claims And Gates

API-key and service-account attribution is durable for newly admitted jobs. Legacy jobs can remain
unattributed and must not be guessed into a project or user cohort. Tenant-wide delegation is not
yet exposed as a generic role; cross-user inspection remains a platform-owner capability.

The read surface also does not claim HTTP latency where request boundary timestamps are absent,
nor upstream membership balance where a provider does not expose an authoritative observation.
Unknown values remain unknown rather than becoming zero.

Before adding delegated organization-wide reads:

1. Define an explicit organization-admin delegation contract.
2. Use a database-enforced read-only role in production.
3. Run production-cardinality query plans and concurrency tests.
4. Add permission-revocation and cache-isolation tests for delegated roles.
