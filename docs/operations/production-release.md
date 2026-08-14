# Production Release Runbook

This runbook is the release boundary for the current AI Image Factory feature
set. Production rollout is blocked unless every required process, migration,
economic invariant, artifact check, and browser smoke test below passes.

## Supported Production Shape

Use one PostgreSQL service and one application database. Do not split identity,
billing, scheduling, or Batch state into separate PostgreSQL instances. They
share transactions and foreign-key invariants.

Required processes:

| Process | Responsibility |
| --- | --- |
| `gpt-image-2-gateway` | HTTP, authentication, admission, Batch orchestration, and read APIs |
| `workerd` | Durable work claim and executor handoff |
| `executord` | Provider/account-bound CLI execution |
| `reducerd` | Terminal result, quota, pricing, ledger, and artifact settlement |
| `reconcilerd` | Expired lease recovery, uncertainty fencing, input cleanup, and artifact retention |
| Admin console | Next.js BFF and operator UI |

Conditional processes:

- Run `provider-submitd` and `provider-pollerd` for enabled remote-task
  providers, including asynchronous Dreamina/Seedance profiles.
- Run `webhookd` when project webhooks are enabled.

`reconcilerd` is not optional. Without it, a worker crash after claim can leave
expired work, quota reservations, and customer billing holds unresolved.

All stateful processes use the same PostgreSQL database. Gateway, workers, and
reconcilers must also use the same durable `GATEWAY_ARTIFACT_ROOT`. The current
filesystem backend is horizontally safe only on one host or a shared persistent
POSIX volume with correct `fsync`, ownership, and atomic-rename semantics.

## Immutable Release Unit

Build every Rust binary from one Git commit and deploy them together:

```bash
cargo build --locked --release -p gpt-image-2-gateway \
  --bins
cargo build --locked --release -p ai-image-factory-updater \
  --bin updated
npm ci
npm run build:admin
```

Record the Git commit, migration version, binary checksums, frontend build
identifier, and configuration revision in the release manifest. Never mix old
workers with a newer schema unless the release notes explicitly define that
compatibility window.

## Storage And Secret Preconditions

- PostgreSQL backups and point-in-time recovery are enabled and tested.
- `GATEWAY_ARTIFACT_ROOT` is absolute, persistent, not a symlink, and writable
  only by the application service account.
- Executor runner roots are separate from artifact and credential roots and
  have mode `0700`.
- Each CLI account has an isolated credential home and execution profile.
- API-key peppers, refresh-token peppers, JWT keys, database credentials, and
  provider credentials come from a secret manager or private mounted files.
- The browser receives only HttpOnly session cookies and the CSRF cookie. It
  never receives Gateway admin tokens or provider credentials.
- TLS terminates at the same-host reverse proxy. Gateway and Next.js bind to
  loopback.
- If Grok video is enabled without account-managed S3/Qiniu output,
  `GATEWAY_PROVIDER_UPLOAD_PUBLIC_BASE_URL` is an Internet-reachable HTTPS
  origin. Its reverse proxy preserves `Host` and forwards
  `/v1/internal/provider-uploads/s3/` directly to the gateway; SigV4 is the
  route's only authentication.

For the official npm Codex package, `EXECUTOR_CODEX_EXECUTABLE` must reference
the native platform binary, not the Node launcher.

## Migration And Startup Order

1. Stop new admission at the reverse proxy.
2. Allow active HTTP requests and leased work to drain, then prove every
   business process unit is `inactive` or `failed` with `MainPID=0`.
3. Verify there are no unsafe `running` migrations or unresolved manual repair
   operations.
4. Back up PostgreSQL and the artifact root as one logical recovery point.
5. Run `factoryctl migrate` with the migration-owner role.
6. Verify the schema version before starting any application process.
7. Start `reconcilerd`.
8. Start the provider/account `executord` instances.
9. Start `reducerd`.
10. Start `workerd`.
11. Start conditional `provider-submitd`, `provider-pollerd`, and `webhookd`
    processes.
12. Start `gpt-image-2-gateway` and the admin console.
13. Pass the release gates below, then reopen admission.

Every daemon verifies the migration version at startup and must fail closed on
schema drift. Do not run a migration while an old worker or executor can still
restart. After migration, admission stays closed until the new process set has
held stable MainPID, restart count, release path, and health/readiness results
for the configured verification window. A failed post-migration activation is
a recovery event; it must not leave the gateway accepting new work.

## Release Gates

Code and contract checks:

```bash
cargo fmt --all -- --check
cargo check --locked -p gpt-image-2-gateway --all-targets
DATABASE_URL="$TEST_DATABASE_URL" \
  cargo test --locked --workspace -- --test-threads=1
cargo clippy --locked --workspace --all-targets
npm run typecheck:admin
npm run build:admin
```

The repository currently compiles under Clippy but has an existing warning
baseline, so `clippy -D warnings` is not yet a valid release claim. Keep the
warning count from increasing and remove that baseline in a separate,
behavior-preserving cleanup after this production release.

Basic HTTP checks:

```bash
curl --fail http://127.0.0.1:8787/healthz
curl --fail http://127.0.0.1:8787/readyz
curl --fail http://127.0.0.1:8787/openapi.json >/dev/null
```

`/readyz` fails closed when an aged work or executor backlog has no valid
consumer lease for its own execution profile, or when the global reducer queue
has no valid reducer lease. The response exposes only bounded aggregate counts,
not profile identities. `GATEWAY_READINESS_STALL_THRESHOLD_SECS` controls the
age threshold and defaults to 60 seconds.

`/readyz` alone is insufficient. Before opening traffic, verify:

- the `provider_profiles` aggregate covers remote-task runtime profiles only;
  local durable consumer state is reported under `execution_queue`;
- `stalled_work_profiles` and `stalled_executor_profiles` are both zero;
- ready-work depth and oldest age are bounded;
- expired `leased` work is being reclaimed by `reconcilerd`;
- no `running` work is past its lease without becoming `uncertain`;
- each enabled execution profile has a live executor owner and positive
  available capacity;
- terminal-reduction backlog and oldest age are bounded;
- provider-submit and provider-poll backlogs are bounded for enabled remote
  providers;
- unresolved customer billing holds have a corresponding active job;
- every sealed ledger transaction is balanced;
- artifact metadata resolves to an existing object with matching size and hash;
- webhook backlog is bounded when webhooks are enabled.

Configured account concurrency is an upper bound, not evidence of live
capacity. For example, `desired_max_concurrency=20` with zero workerd or
executord leases has effective capacity zero and must not be reported as ready.

Run one low-cost canary through the public API and one through Batch. The Batch
canary must prove:

- one input line creates one job, one work item, and one provider submission;
- the quote records `processing_mode=batch`;
- the Batch rate adjustment is exactly `1/2`;
- one successful low-quality image captures exactly half of the synchronous
  price;
- one sealed ledger transaction has two postings whose sum is zero;
- repeated status reads and result downloads create no additional job, usage,
  artifact, rating, or ledger rows;
- the output JSONL has one line with the original `custom_id`, HTTP `200`, and a
  non-empty `data[0].b64_json`;
- the downloaded JSONL hash matches the authoritative project-file record.

Browser gates:

- login, refresh rotation, logout, and CSRF rejection;
- overview, API logs, usage, keys, images, videos, Batch, and operator pages;
- desktop and `390x844` mobile layouts without horizontal overflow;
- no browser console errors during the canary workflow.

## Client Timeout And Late Completion

An HTTP client timeout is not a durable job cancellation. Clients must submit a
stable `Idempotency-Key` and reconcile the same operation after reconnecting;
retrying with a different key may create duplicate provider work. Refund or
downstream failure should become final only after the Factory terminal outcome
is known. A late successful terminal outcome remains authoritative for artifact
and customer settlement even when the original HTTP connection has closed.

## Batch Limits

The current production boundary is intentionally bounded:

- endpoint: `/v1/images/generations`;
- completion window: `24h`;
- maximum input upload: 8 MiB;
- maximum requests per Batch: 1,000;
- maximum persisted result body: 128 MiB;
- maximum materialized result file: 144 MiB;
- output retention: 30 minutes, followed by the snapshotted reader-drain period.

Batch result files are private project files. Download authorization must be
rechecked on every request; generated files must not be exposed as public paths.

## Backup

Quiesce admission or take a storage-consistent snapshot. Back up both state
planes:

```bash
pg_dump --format=custom \
  --file=/backup/ai-image-factory-DB_TIMESTAMP.dump \
  "$DATABASE_URL"
tar -C "$GATEWAY_ARTIFACT_ROOT" -czf \
  /backup/ai-image-factory-artifacts-TIMESTAMP.tar.gz .
sha256sum /backup/ai-image-factory-DB_TIMESTAMP.dump \
  /backup/ai-image-factory-artifacts-TIMESTAMP.tar.gz
pg_restore --list /backup/ai-image-factory-DB_TIMESTAMP.dump >/dev/null
tar -tzf /backup/ai-image-factory-artifacts-TIMESTAMP.tar.gz >/dev/null
```

Store database and artifact checksums in the release record. A database-only
backup is not a valid recovery point because PostgreSQL stores object metadata,
not generated media bytes.

## Rollback

1. Close admission.
2. Drain active requests and stop workers before Gateway.
3. Preserve the failed release database, artifacts, logs, and release manifest.
4. If the schema is backward compatible, deploy the previous complete binary
   set and frontend from its recorded commit.
5. If the schema is not backward compatible, restore both the database dump and
   artifact archive from the same pre-release recovery point.
6. Start processes in the documented order and rerun every release gate.
7. Reopen admission only after queue, billing, ledger, and artifact checks pass.

Migrations are immutable. Do not edit an applied migration or attempt ad hoc
down-migration SQL during an incident.

The updater intentionally rejects a downgrade after an Apply has committed.
Do not perform an ad hoc in-place rollback by separately restoring PostgreSQL,
artifacts, and the `current` symlink. Those operations are not one atomic
recovery point and can expose mixed release state.

For an unfinished Apply with a protected recovery descriptor, use only the
fenced recovery command:

```bash
sudo systemctl stop ai-image-factory-updater.service
sudo systemctl start ai-image-factory-updater-recover@COMMAND_UUID.service
sudo systemctl start ai-image-factory.target
```

The recovery unit reacquires the database and lease fences, consumes the
recorded descriptor, restores one matched database-and-artifact recovery point,
validates Gateway and admin, persists the leased restoration phase, starts and
verifies the full process scope, reopens admission, and only then commits
`restored`.

A rollback after a successful release is a disaster-recovery cutover, not an
updater command. Keep admission closed and all business processes stopped.
Require verified checksums, a database and artifact backup from the same
recovery point, the previous signed release manifest, the exact recorded
migration version, and an atomic traffic or host pointer cutover. Until that
procedure has passed the documented failure-injection drill, restore onto a
replacement database and host, run the complete release gate there, and then
cut traffic over. Preserve the failed database, artifacts, release tree, and
logs until the incident is closed.

## Alerts

Page immediately on:

- any ledger imbalance or unsealed customer charge;
- a settled job with an unresolved billing hold;
- repeated lease expiry for the same work item;
- any `running` work past lease without an uncertainty transition;
- terminal-reduction oldest age above 60 seconds;
- ready-work oldest age above the service objective;
- executor capacity at zero for an enabled production route;
- artifact hash, size, or authority mismatch;
- database migration mismatch at process startup;
- refresh-token replay detection or sustained authentication throttling;
- Batch finalization failure or stored-result capacity exhaustion.

Warn on sustained quota-observation failures, provider cooldown, webhook
dead-letter growth, artifact cleanup retries, and Batch completion time
approaching the 24-hour window.

## Current Boundaries

- Batch currently supports image generations, not edits or videos.
- The filesystem artifact backend requires one host or a shared POSIX volume;
  object storage is the next horizontal-scaling boundary.
- Agentic CLI credentials are isolated by provider account, but the host remains
  a trusted execution boundary. Do not mix unrelated trust domains in one
  credential or runner root.
- A provider-reported quota window is observational, not a customer billing
  balance.
- Do not enable a model or route without a published customer price, an active
  execution profile, and positive provider capacity.
