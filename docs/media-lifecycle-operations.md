# Sidecar lifecycle and quota observer rollout

This is an opt-in Factory change. Blog owns durable successful JSON and its
task/result authorization binding. Factory is a short-lived computation/cache
service; it does not retain all original images permanently or schedule Blog
backfills. No changes are made to the standard image generation/edit contract,
image billing, or the admin image-generation UI.

## Migrations and activation

- `0130_media_segment_source_lifecycle.sql` adds source ownership state and
  bounded cleanup indexes to the existing sidecar tables, plus a small analyzer
  heartbeat table. Existing sources start `retained` and keep their original TTL.
- `0131_provider_quota_refresh.sql` adds a separate per-account observation
  lease/backoff record. It does not change credential refresh or billing tables.
- Apply using the release's normal migration workflow. Then run the **same
  release binary** `factoryctl verify-migrations` to check every migration's
  version, success and checksum without mutation or provider initialization.
- Keep `GATEWAY_BBOX_RELEASE_TERMINAL_SOURCES=false` in `segments.env` and
  `GATEWAY_CODEX_QUOTA_AUTO_REFRESH_ENABLED=false` in `app.env` until consumer
  persistence, runtime identity and the isolated gates below pass.

After separate production authorization, terminal release is enabled on
segmentd; auto quota observation is enabled on gateway. No timer, launchd unit,
queue middleware, or new service is installed. Existing process/release identity
verification remains required in addition to these readiness checks.

## Bounded quota observation

| Setting | Default | Bound |
| --- | --- | --- |
| `GATEWAY_CODEX_QUOTA_AUTO_REFRESH_ENABLED` | `false` | explicit opt-in |
| `GATEWAY_CODEX_QUOTA_AUTO_REFRESH_INTERVAL_SECONDS` | `15` | 5–300 seconds |
| `GATEWAY_CODEX_QUOTA_AUTO_REFRESH_CONCURRENCY` | `2` | 1–4 per gateway |
| Candidate batch | 16 | fixed |
| Account observation timeout | 90 seconds | fixed |
| PG statement / outer call timeout | 4 / 5 seconds | fixed |
| Observation lease | 105 seconds | fixed |
| Failure delay | 30 seconds | 30, 60, 120, 240, then 300 seconds |

Candidate selection joins only current, enabled routes and members with active
Codex account/profile/pool/environment/credential/control/operation/resource
policy. Multiple routes share one account observation and use their shortest
quota TTL. Refresh is due before expiry by `min(60 seconds, TTL/2)`, or when
windows reset or cease to be usable. Retired/draining and historical route
revisions are deliberately not automatic candidates; use the existing manual
refresh path for previously bound work if needed.

Manual and automatic refresh share the database lease. The automatic path
rechecks eligibility/freshness under the account's scheduler row lock, preventing
a stale candidate scan from issuing a duplicate observation. Neither CLI work
nor credential refresh holds that database transaction open. Snapshot publication
and lease completion are fenced in the same transaction; an expired owner
cannot overwrite a newer observation. Empty/invalid observations are failures,
not fresh quota. Backoff survives process restart.

`unknown_quota_policy=block` and route TTL are unchanged. An exhausted or unknown
account can still block image dispatch; automatic observation does not grant
credits, purchase a reset, or create images. Bbox inference likewise does not
consume Factory IMAGE credits but may use upstream inference entitlement.

## Readiness and observability

Sidecar SQL uses a dedicated three-connection pool with a one-second acquire
timeout, four-second statement timeout, two-second lock timeout and ten-second
idle-transaction timeout. Session limits are set once on connection, not for each
request. Each store operation also has a five-second application deadline for a
stalled connection. The original image pool and 95-second analysis deadline are
unchanged. A timed-out registration COMMIT has an unknown outcome: a cache miss
on another connection is not proof of rollback, so its pixels are preserved.

`/v1/media/readiness` is authenticated, read-only, bounded to two seconds, and
independent of image-generation readiness. It exposes only the opaque analyzer
key, latest matching heartbeat, its age/150-second TTL, and source-release mode.
No account paths or identities are exposed. A heartbeat records worker-loop
progress; it is not a guarantee of model accuracy, credentials, or future
upstream availability. Workers must use matching analyzer config and consistent
cleanup mode during a rollout. Fresh heartbeats from both cleanup modes produce
`worker_configuration_mismatch` and fail readiness, even when their analyzer
keys differ (source cleanup is shared). After stopping all old-mode
workers, allow their 150-second heartbeat TTL to expire before declaring the
rollout/rollback complete.

When quota auto-refresh is enabled, `/readyz` additionally reports
`codex_quota_refresh`: enabled/running/healthy, last successful bounded scan,
in-flight count, attempts, successes, failures and timeouts. An absent/stalled
enabled loop makes readiness fail; upstream observation failures are tracked
separately and leave admission fail-closed. Counters are per gateway process;
durable per-account `last_attempt_at_ms`, `last_completed_at_ms`,
`last_error_code`, backoff and lease epoch live in
`provider_account_quota_refreshes`.

Source cleanup emits bounded batch counts and failures. Inspect storage pressure
with the following read-only aggregation; do not treat expired but still-owned
sources as free space:

```sql
SELECT source_state, count(*) AS assets, sum(byte_size) AS original_bytes
FROM media_segment_assets
GROUP BY source_state;

SELECT status, count(*) AS results
FROM media_segment_results
GROUP BY status;

SELECT consecutive_failures, count(*) AS accounts,
       min(next_attempt_at_ms) AS next_attempt_at_ms
FROM provider_account_quota_refreshes
GROUP BY consecutive_failures;
```

The API wire examples are in
[`contracts/media-lifecycle-v1.json`](contracts/media-lifecycle-v1.json), with
idempotency and retry rules in [`media-segments.md`](media-segments.md).

## Executable rollout and rollback gates

The release packager already includes `deploy/hooks/*`. The opt-in hook below
only executes read-only migration verification and GET requests. It never
registers an asset, enqueues analysis, refreshes quota, or submits an image.
Supply the usual private database environment and a private `images:read` key
file; no credentials are passed as command-line values or printed.

```sh
python3 deploy/hooks/verify-media-segments \
  --base-url http://127.0.0.1:8787 \
  --factoryctl /opt/ai-image-factory/current/bin/factoryctl \
  --api-key-file /private/path/media-probe.key
```

For a feature rollback, keep the new compatible binaries and migrations, set
both new flags to `false`, restart the corresponding processes, then run the
same hook with `--source-release disabled --quota-refresh disabled`. Cached
results remain readable. Already reclaimed pixels are not resurrected; a later
genuine cache miss can restore the original bytes through registration. Stop
all enabled old instances before asserting the cleanup mode has changed.

**Do not blindly switch to the old binary or delete migration records.** The
existing migration verifier rejects newer database versions, and an old cleaner
does not understand restored sources' new blob-session identities. Rollback here
means a tested configuration rollback on the compatible release. A binary/database
downgrade needs a separately designed, backed-up maintenance operation.

Run the reproducible local gates against a disposable database:

```sh
python3 scripts/test-media-runtime-gate.py
cargo build -p gpt-image-2-gateway --bin gpt-image-2-gateway --bin segmentd --bin factoryctl
TEST_DATABASE_URL=postgresql://TEST_USER@127.0.0.1:TEST_PORT/TEST_DATABASE \
  python3 scripts/test-media-runtime-smoke.py --bin-dir target/debug --psql /path/to/psql
```

The smoke creates/drops only a new UUID schema. It proves missing migrations
fail, absent/stale workers fail, enabled real processes pass, and both features
disabled pass the rollback gate. Its provider executable is `/usr/bin/false`,
with no provider accounts or submitted work; this proves runtime wiring without
using a real model. It retains private evidence/logs and reports their path.
This is not production deployment or real-upstream acceptance evidence.
