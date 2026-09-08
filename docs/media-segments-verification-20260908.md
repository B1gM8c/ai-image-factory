# Bbox sidecar verification — 2026-09-08

## Scope and compatibility

Base: `origin/main` at `40c74329080aeaea7b4eddeaf56a20fead554c4f`.
Implementation uses an isolated worktree; the existing dirty local main remains untouched.

- No Factory UI, generation/edit provider, image response, billing, or generation queue changes.
- New optional `/v1/media/assets`, `/v1/media/segments`, polling and capabilities routes.
- New additive migration `0129_media_segments.sql`: two independent tables, scoped uniqueness,
  fenced leases, expiry and indexed reads. No existing table is altered.
- One independent `segmentd` process, bounded queue, separate three-connection gateway pool,
  two concurrent upload/decode slots acquired before buffering. No new dependencies.
- Default off. Deployment must separately provision the isolated Codex account, apply migration,
  enable the gateway capability and explicitly enable the optional worker. No production deployment
  or server CLI upgrade was performed for this change.

The returned groups describe the final flat image; they are not PSD layers or pixel masks.
`supports_bbox_sidecar` describes configured gateway capability, not worker liveness.

## Automated gates

Local environment: macOS arm64, Rust 1.96.0, dedicated PostgreSQL 18 UTF-8 test database.
Run database-heavy suites serially to avoid exhausting the temporary database's schema-lock budget.

| Gate | Evidence |
| --- | --- |
| Gateway library | 585 passed, 7 existing ignored, 1 explicitly excluded baseline Keychain case |
| Original image/API integration | `gateway_api`: 63 passed |
| Generation/edit idempotency | `idempotency_api`: 9 passed |
| OpenAI and xAI contract tests | One passed in each suite |
| Sidecar HTTP and shared Blog fixture | 4 passed; live model test separately opted in |
| Real PostgreSQL sidecar store | 1 matrix passed: concurrent dedup, owner/project isolation, queue/storage caps, fencing, timeout, expiry, cleanup |
| Full PostgreSQL migrations | 19 passed, strict sequence through version 129 |
| Release process hooks | Passed; optional worker participates only when enabled |

The normal sidecar HTTP tests use real PostgreSQL, the actual router and a controlled analyzer.
They prove failure sanitization and terminal replay without invoking another model; the live test
below proves the real Codex path. The shared JSON fixture is deserialized and round-tripped by
Factory and consumed unchanged by both Blog repositories.

Reproduction (use a disposable database whose name contains `test`):

```bash
export TEST_DATABASE_URL=postgres://USER@127.0.0.1:PORT/DATABASE_test
cargo test --locked -p gpt-image-2-gateway --lib --test gateway_api \
  --test openai_contract_snapshot --test xai_image_api --test idempotency_api \
  --test media_segments_http --test media_segments_postgres --test postgres_migrations \
  -- --test-threads=1 \
  --skip providers::dreamina_cli::credential_environment::tests::separate_homes_use_separate_login_keychains
cargo check --locked -p gpt-image-2-gateway --bins --tests
bash scripts/test-release-process-hooks.sh
```

The excluded `separate_homes_use_separate_login_keychains` case fails at
`credential_environment.rs:1032` with `KeychainUnavailable`. It was separately reproduced in a
clean, unmodified checkout of the base SHA, so it is not claimed as passed or modified here.
An initial parallel library run also hit process timeout failures; the serial run passed all
those tests, including sidecar subprocess termination/cancellation. Full strict Clippy has
pre-existing warnings outside this feature; no unrelated warning cleanup is included.

## Live and latency evidence

Codex executable: native **0.151.0**. Model `gpt-5.6-luna`, reasoning `none`, zero retries.
Production analyzer uses `--image`, `--output-schema`, `--output-last-message`, read-only sandbox,
private temporary directories, an auth-only copy, disabled tools/plugins/hooks and a 90-second
subprocess deadline. The worker has a 95-second outer deadline and a 120-second fenced lease.
Codex's documented non-interactive command surface is described in the
[official CLI reference](https://developers.openai.com/codex/cli/reference/); the exact installed
flags and behavior here are verified by local help, fake-process tests and real model execution.

| Measurement | Observed result | Boundary |
| --- | --- | --- |
| Screenshot, 1280×720, initial live Rust worker | 20.444 s, 7 Chinese groups | Model + validation + terminal write |
| Same screenshot, final auth-isolated analyzer | 19.695 s, 5 Chinese groups | Independent live run; cached lookup 1.268 ms |
| Blog Go adapter, existing star-field image, 1920×480 | 32.16 s, 5 Chinese groups | Register → enqueue → 2-second polling → completed |
| Cached POST, n=25 | P50 0.418 ms / P95 1.226 ms | In-process HTTP + auth + two real PostgreSQL reads |
| Cached GET, n=25 | P50 0.707 ms / P95 3.675 ms | Local TCP/HTTP client, auth and database; first request max 22.415 ms |

The two screenshot runs confirm operational behavior, not annotated bbox accuracy or deterministic
semantic grouping. These small live samples are not production latency percentiles or an SLA.
No main-image generation was timed in this implementation: existing final images were used so
the analysis path could be measured independently. No Grok bbox production provider was added;
Grok-generated image bytes use the same vendor-neutral registration path.

The live smoke is reproducible with explicit credentials and an absolute image path:

```bash
export GATEWAY_BBOX_CODEX_BIN=/absolute/versioned/native/codex
export GATEWAY_BBOX_CODEX_HOME=/absolute/private/auth-home
export BBOX_SMOKE_IMAGE=/absolute/final-image.png
cargo test --locked -p gpt-image-2-gateway --test media_segments_http \
  live_codex -- --ignored --nocapture
```

Runtime logs separate `cli_start_ms`, `bbox_ms`, `validation_ms`, terminal `storage_ms`,
and retries. `analysis_ms` includes failed/timeout attempts; successful per-stage samples are
persisted for aggregation. `storage_ms` is only accurate in the post-write log, not persisted in
the same row. Cache reads never decode pixels or start a CLI.

## Blog coordination

Coordinated with task `01a0344c-030e-7643-947a-44950127218b`. Blog reported a successful real
Go-adapter replay using the same asset and segmentation IDs, not a fixture-only HTTP success.
Its HMAC poll handle binds user, task, result index, asset and segmentation, because the Factory
project service key itself is shared among Blog users. Cross-user/task/result, expiry, tampering
and cross-asset response checks are covered by Blog-side tests. Blog owns its UI and commits;
Factory does not edit that repository or claim production/browser rollout from this local smoke.
