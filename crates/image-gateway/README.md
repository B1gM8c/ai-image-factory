# gpt-image-2 Codex Gateway

Rust API gateway for AI Image Factory. This app is the first active provider
runtime: a local OpenAI Images API-compatible gateway backed by Codex CLI image
generation.

It exposes:

- `POST /v1/images/generations`
- `POST /v1/images/edits`
- `POST /api/v3/images/generations`
- `POST /api/v3/contents/generations/tasks`
- `GET /api/v3/contents/generations/tasks/{task_id}`
- `GET /api/v3/files/{file_id}/content`
- `POST /v1/dreamina/images/generations`
- `POST /v1/dreamina/videos/generations`
- `GET /v1/dreamina/videos/{task_id}`
- `GET /v1/dreamina/files/{file_id}/content`
- `POST /v1/organization/projects/{project_id}/service_accounts`
- `GET /v1/organization/projects/{project_id}/api_keys`
- `DELETE /v1/organization/projects/{project_id}/api_keys/{api_key_id}`
- `GET /v1/models`
- `GET /healthz`
- `GET /openapi.json`
- `GET /docs`

The gateway returns OpenAI-style `data[].b64_json` responses. It uses only the native `codex exec` CLI path with `--ignore-user-config`, `--ignore-rules`, and disabled user plugin/app features, asks Codex to create the requested image, copy the final file into a request-scoped temporary directory, reads only that directory, verifies explicit `WIDTHxHEIGHT` requests, encodes requested output formats when possible, and returns base64 without exposing local paths. It does not call private ChatGPT/Codex backend endpoints or reuse OAuth sessions directly. Codex CLI's built-in image generation skill is treated as native Codex capability; the gateway isolates user config/plugins/apps and gateway process secrets, not the system image tool itself.

## Run

```bash
cd crates/image-gateway
export DATABASE_URL='postgres://user:pass@localhost:5432/gpt_image_gateway'
export GATEWAY_API_TOKEN='local-token'
export GATEWAY_ADMIN_TOKEN='admin-token'
export GATEWAY_API_KEY_PEPPERS="1:$(openssl rand -hex 32)"
export GATEWAY_API_KEY_CURRENT_PEPPER_VERSION='1'
export GATEWAY_CODEX_HOME='/srv/gpt-image-codex-home'
export GATEWAY_ARTIFACT_ROOT='/srv/ai-image-factory/artifacts'
export RECONCILER_ORPHAN_GRACE_MS='60000'
export RECONCILER_INPUT_CLEANUP_GRACE_MS='60000'
export RECONCILER_INPUT_CLEANUP_LEASE_MS='60000'
cargo run --bin factoryctl -- migrate
cargo run --bin workerd
cargo run --bin reconcilerd
cargo run --bin gpt-image-2-gateway
```

Migration `0006_execution_context` is intentionally fail-closed when an older deployment still has active `reserved`, `queued`, or `running` jobs without persisted quota snapshots. Stop admission, drain or explicitly resolve that old queue, then run `factoryctl migrate`; the missing historical snapshot cannot be reconstructed safely from aggregate usage.

From the repository root, use:

```bash
cargo run -p gpt-image-2-gateway --bin factoryctl -- migrate
cargo run -p gpt-image-2-gateway --bin workerd
cargo run -p gpt-image-2-gateway --bin reconcilerd
cargo run -p gpt-image-2-gateway --bin gpt-image-2-gateway
```

The output-level V2 executor topology is available for generation requests but
remains disabled by default. Before enabling it, provision the immutable Codex
execution binding from the same private `auth.json` that will be mounted into
executord:

```bash
export EXECUTOR_PROFILE_KEY='openai-codex-generation-v1-account-1'
export EXECUTOR_CREDENTIAL_POOL_KEY='openai-codex-production'
export EXECUTOR_PROVIDER_ACCOUNT_KEY='account-1-revision-1'
export EXECUTOR_CREDENTIAL_REF='mounted.openai-codex.account-1.1'
export EXECUTOR_CREDENTIAL_REVISION='1'
export EXECUTOR_CODEX_CREDENTIAL_HOME='/srv/codex-credentials/account-1'
export EXECUTOR_MAX_CONCURRENCY='1'
cargo run -p gpt-image-2-gateway --bin factoryctl -- provision-codex-profile
```

Provisioning hashes the exact private `auth.json` bytes and atomically creates
or verifies the credential pool, account, resource policy, and execution
profile. Exact enabled replay returns the same identities. A disabled row or
any credential, revision, adapter, or capacity drift fails closed; this command
never overrides an operational kill switch.

`workerd` uses `WORKER_EXECUTION_MODE=executor-handoff` and the same
`EXECUTOR_PROFILE_KEY`; `executord` additionally requires its owner, mounted
credential identity, runner root, helper executable, and Codex executable.
Set `EXECUTOR_CODEX_EXECUTABLE` to the self-contained native Codex binary when
using the official npm package, not its Node launcher. Executor children use
`PATH=/usr/bin:/bin`; startup accepts native binaries and shebang scripts whose
absolute interpreter exists and is executable. For `#!/usr/bin/env ...`
wrappers, the requested interpreter must resolve inside that restricted path;
incompatible or malformed wrappers are rejected before claiming work. Keep the
restricted path instead of exposing an ambient package-manager environment to
provider children.
`reducerd` uses `REDUCER_OWNER`, `REDUCER_LEASE_MS`,
`REDUCER_HEARTBEAT_INTERVAL_MS`, and `REDUCER_POLL_INTERVAL_MS`, with safe
defaults. It verifies and publishes customer artifacts before atomically
completing receipt, rating, quota, parent state, response projection, and
outbox. `GATEWAY_IMAGES_GENERATION_CONTRACT` is required and accepts
`legacy-v1`, `output-economics-v2`, or `customer-pricing-v4`; an unset or
invalid value fails startup. Use `customer-pricing-v4` only after model routes,
published customer price versions, billing accounts, and the handoff workerd,
executord, and reducerd are healthy. The selected contract applies to both
generation and edit admission, so edits cannot silently bypass the frozen
customer quote and billing-hold path. Keep a compatible worker pool only for
jobs admitted under an older contract during a controlled migration.

`size=auto` preserves the provider's native dimensions and reports the actual
`WIDTHxHEIGHT`. Explicit dimensions constrain the output aspect ratio: Codex
native dimensions with the same ratio are accepted. The executor never locally
crops, stretches, or resamples generated pixels.

Default bind address is `127.0.0.1:8787`. Gateway startup requires at least one of `GATEWAY_API_TOKEN` or `GATEWAY_ADMIN_TOKEN`, a versioned API-key pepper keyring, and an explicit absolute, existing `GATEWAY_ARTIFACT_ROOT`; it does not require Codex CLI or `GATEWAY_CODEX_HOME`. Worker startup separately requires an explicit absolute, existing, writable `GATEWAY_CODEX_HOME` plus the shared artifact root. If both tokens are configured, they must be different. `GATEWAY_ADMIN_TOKEN` protects Admin endpoints for creating and revoking project API keys; it never authorizes image calls. An admin-only startup can bootstrap project keys, and image calls must then use one of those keys. `GATEWAY_API_TOKEN` remains a legacy image token and is not accepted on Admin endpoints.

`GATEWAY_API_KEY_PEPPERS` is a comma-separated `version:64-hex` keyring. `GATEWAY_API_KEY_CURRENT_PEPPER_VERSION` selects the version used for new keys. During rotation, keep both versions configured, change the current version, then retire the old version after its keys have been rotated or revoked. Pepper values belong in KMS/secret-manager injection, never in source control.

Legacy SHA-256 credential reads are disabled by default. Set `GATEWAY_API_KEY_ALLOW_LEGACY_SHA256=true` only during a controlled migration window, then remove it after those keys are rotated or revoked.

The gateway does not yet provide native TLS and rejects every non-loopback bind. Bind it to loopback and place a TLS reverse proxy on the same host in front of it. Provision `GATEWAY_CODEX_HOME` before each worker startup and grant the worker service account permission to create and remove files there; the path must not be a symlink or the filesystem root. `--ignore-user-config` skips user `config.toml`; Codex may still load its native system image generation skill.

`GATEWAY_ARTIFACT_ROOT` stores immutable generated-image blobs and durable edit inputs in separate object namespaces. The root must be owned by the gateway service user, must not be a symlink, and must not be group or world writable. The current filesystem backend is production-supported only when every gateway/worker process runs on one host or mounts the same persistent POSIX volume. PostgreSQL stores artifact metadata, ordered edit-input manifests, immutable response projections, and retention tombstones, not image bytes. A successful generation or edit writes and syncs every output blob before the fenced settlement transaction commits job success, quota, metering, artifact metadata, and the replay projection. Reusing the same completed `Idempotency-Key` reconstructs the original JSON or final SSE response without another provider invocation or charge while the result is retained. After logical expiry, the same key and request return `410 idempotency_result_expired`; unknown or corrupted live artifacts are never disguised as expiry.

Grok video generation does not require Qiniu or another customer-managed object store. When an account has no explicit `[tools.zdr_video_output_s3]` configuration, set `GATEWAY_PROVIDER_UPLOAD_PUBLIC_BASE_URL` to the public HTTPS origin of this gateway. For each fenced execution, `executord` injects a private, short-lived S3-compatible upload credential and enables Grok's ZDR-compatible video mode in that execution's isolated Grok home. The provider uploads to `/v1/internal/provider-uploads/s3/...`; Grok CLI downloads the same object into its normal output slot; then the existing executor validates, publishes, settles, and exposes it through the project-scoped video URL. The temporary provider upload is never returned to the user and is removed after its ticket expires. An explicit account S3/Qiniu configuration takes precedence.

`GATEWAY_PROVIDER_UPLOAD_PUBLIC_BASE_URL` must contain only an origin, for example `https://media-gateway.example.com`. The reverse proxy must preserve the public `Host` header and route the internal upload path to `gpt-image-2-gateway` without adding JWT or API-key authentication; the route authenticates only short-lived AWS SigV4 presigned PUT/GET/HEAD requests. Gateway, `executord`, and `reconcilerd` must share the same `GATEWAY_ARTIFACT_ROOT`. Loopback HTTP is accepted only for protocol tests; a real Grok/xAI generation needs an Internet-reachable HTTPS origin.

Production generation and edit execution are external to the HTTP process. `gpt-image-2-gateway` authenticates, reserves quota, writes edit inputs when present, atomically attaches ready work, and waits on PostgreSQL. Quota reservation retries are keyed by the admission session, and attach validates the exact tenant, operation, request, job, and quota/session binding. `workerd` claims weighted ready work, validates and reconstructs the versioned command and quota snapshot, verifies edit-input blob hashes before marking the attempt running, executes Codex under a heartbeated fenced lease, persists output artifacts, and requests atomic settlement. A transient input-store failure leaves the unstarted lease recoverable; an input-integrity failure is terminalized without invoking Codex. On SIGTERM the worker stops claiming and drains the in-flight attempt within a bounded window.

The Codex adapter treats prompts as untrusted descriptions, strips untrusted image metadata by decoding and re-encoding every output, checks output byte and pixel budgets before allocation, and clears gateway secrets from the child environment. These controls do not make an agentic CLI credential domain suitable for mutually hostile tenants: the Codex parent runtime must still access its own provider credential. Until the planned externally sandboxed executor/account-pool isolation is implemented, deploy each Codex worker pool with a dedicated least-privilege account and do not mix unrelated trust domains in one `GATEWAY_CODEX_HOME`.

`reconcilerd` safely requeues expired leases that never reached `running`; an expired `running` attempt becomes `uncertain`, keeps its economic reservation and edit inputs, and is never automatically submitted or garbage-collected. It atomically releases a reserved job/admission pair that still has no work item after `RECONCILER_ORPHAN_GRACE_MS` (default 60000). It also leases session-scoped input cleanup and artifact retention work. Artifact expiry is committed before any file I/O, then physical customer and executor objects are deleted after the snapshotted read-drain interval. A fenced tombstone is committed only after idempotent deletion succeeds; failures use the job's snapshotted retry delay. New response snapshots retain results for 30 minutes and drain readers for 1 minute; existing rows keep the policy captured when they were created. `RECONCILER_ARTIFACT_CLEANUP_LEASE_MS` (default 60000) controls deletion ownership. `RECONCILER_INTERVAL_MS` (default 1000) and `RECONCILER_BATCH_SIZE` (default 100) bound each scan.

Migration `0007_edit_inputs` adds the quota/admission binding, ordered edit-input manifests, immutable input object identities, operation-aware response projections, and leased input-cleanup state required by durable edits. The gateway never invokes the provider CLI; only `workerd` does.

## Example

Create a project service account and gateway API key:

```bash
curl http://127.0.0.1:8787/v1/organization/projects/proj_alpha/service_accounts \
  -H 'Authorization: Bearer admin-token' \
  -H 'Content-Type: application/json' \
  -d '{ "name": "Production App" }'
```

The `api_key.value` is shown only in the create response. Store that value in your client secret manager.

```bash
curl http://127.0.0.1:8787/v1/images/generations \
  -H 'Authorization: Bearer sk-gw-...' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-image-2",
    "prompt": "a minimal terminal icon",
    "size": "1024x1024",
    "quality": "low",
    "output_format": "png"
  }'
```

Edit with multiple base64 input images and request multiple outputs:

```bash
curl http://127.0.0.1:8787/v1/images/edits \
  -H 'Authorization: Bearer sk-gw-...' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-image-2",
    "prompt": "combine these references into clean product poster candidates",
    "images": [
      { "b64_json": "<base64-image-1>", "mime_type": "image/png" },
      { "b64_json": "<base64-image-2>", "mime_type": "image/jpeg" }
    ],
    "n": 3,
    "size": "4:3",
    "output_format": "png"
  }'
```

## Volcengine Ark Compatibility

The canonical Dreamina public surface follows Volcengine Ark's `/api/v3`
routes and request envelopes. It is an explicit compatibility adapter over the
isolated Dreamina CLI execution profile, so CLI command names never leak into
new client integrations.

Generate Seedream images as base64:

```bash
curl http://127.0.0.1:8787/api/v3/images/generations \
  -H 'Authorization: Bearer sk-gw-...' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ark-image-example-1' \
  -d '{
    "model": "doubao-seedream-5-0-260128",
    "prompt": "a clean product photograph on a white background",
    "size": "2K",
    "sequential_image_generation": "disabled",
    "response_format": "b64_json",
    "stream": false
  }'
```

Submit and poll a Seedance 2.0 video task:

```bash
curl http://127.0.0.1:8787/api/v3/contents/generations/tasks \
  -H 'Authorization: Bearer sk-gw-...' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ark-video-example-1' \
  -d '{
    "model": "doubao-seedance-2-0-fast-260128",
    "content": [{
      "type": "text",
      "text": "slow camera push toward a futuristic city at sunrise"
    }],
    "ratio": "16:9",
    "resolution": "720p",
    "duration": 5
  }'

curl http://127.0.0.1:8787/api/v3/contents/generations/tasks/<cgt-task-id> \
  -H 'Authorization: Bearer sk-gw-...'
```

The request DTOs retain the current official Ark fields, but the adapter fails
closed with `unsupported_parameter` when the installed CLI cannot honor a
field. Today that includes image references, URL image responses, streaming,
seed/guidance/watermark/prompt optimization/tools, video reference media,
callbacks, audio, draft mode, frame counts, and service-tier controls. The
supported CLI subset is text-to-image with base64 output and text-only
Seedance 2.0 at 720p. Collection listing and task cancellation are not yet
implemented.

## Dreamina CLI Compatibility Routes

The older `/v1/dreamina/*` facade preserves the current CLI field names for
existing clients. New integrations should use the Ark-compatible `/api/v3`
routes above.

Generate one or more images and receive OpenAI-style base64 output:

```bash
curl http://127.0.0.1:8787/v1/dreamina/images/generations \
  -H 'Authorization: Bearer sk-gw-...' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: dreamina-image-example-1' \
  -d '{
    "prompt": "a clean product photograph on a white background",
    "model_version": "5.0Pro",
    "ratio": "1:1",
    "resolution_type": "2k",
    "generate_num": 2
  }'
```

Submit a Seedance text-to-video task, then poll it:

```bash
curl http://127.0.0.1:8787/v1/dreamina/videos/generations \
  -H 'Authorization: Bearer sk-gw-...' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: dreamina-video-example-1' \
  -d '{
    "prompt": "slow camera push toward a futuristic city at sunrise",
    "model_version": "seedance2.0fast",
    "ratio": "16:9",
    "duration": 5,
    "video_resolution": "720p"
  }'

curl http://127.0.0.1:8787/v1/dreamina/videos/<task-id> \
  -H 'Authorization: Bearer sk-gw-...'
```

Completed video tasks return a tenant-scoped `content.video_url`. Video
admission requires an explicit positive `video_second` price; no free wildcard
video price is published. Migration `0050` permits one account to carry the
separate image and video operation descriptors, and gateway startup
idempotently adds the missing video profile and account route to older managed
Dreamina accounts.

## API Docs

Scalar API Reference is served at:

```text
http://127.0.0.1:8787/docs
```

The OpenAPI 3.1 document is available at:

```text
http://127.0.0.1:8787/openapi.json
```

The OpenAPI document is generated from Rust path/schema annotations with `utoipa`; Scalar loads `https://cdn.jsdelivr.net/npm/@scalar/api-reference` and points it at the same-origin OpenAPI document.

## PostgreSQL Usage And Keys

PostgreSQL is the authority for local API keys and 5-hour / 7-day image usage accounting. Run `factoryctl migrate` before starting or upgrading the gateway. Gateway startup does not create or alter database objects.

Current MVP behavior:

- New API keys contain a public key id plus a 256-bit secret and are stored as versioned HMAC-SHA-256 digests. The unredacted key value is only returned when a project service account is created.
- Legacy SHA-256 rows are readable only when the explicit migration switch is enabled; no new legacy rows are created.
- `last_used_at` writes are coalesced to at most once per minute per key.
- Usage is isolated by project/tenant. Legacy `GATEWAY_API_TOKEN` calls use the `proj_default` tenant.
- `GATEWAY_IMAGE_LIMIT_5H` and `GATEWAY_IMAGE_LIMIT_7D` default to `2147483647`,
  the database-safe unbounded sentinel. Set either variable explicitly to enforce
  a smaller customer usage quota. Billing admission, project budgets, queue and
  concurrency controls, request validation, timeouts, and upstream provider
  limits remain independent safeguards.
- After validation and queue admission, a request reserves `n` image units before Codex generation starts.
- Successful generation commits the reservation. Provider failures release it. A failure after the provider returned, such as artifact persistence failure, marks the work uncertain and leaves the reservation for reconciliation instead of falsely declaring that no provider cost occurred.
- Generation artifacts are persisted before success settlement. Completed idempotent generation requests replay the original response projection and verified bytes without reserving quota again.
- Responses include `x-request-id`, `openai-version`, `openai-project`, and image-unit quota headers when available.
- PostgreSQL reservation transitions are serialized per tenant, and integration tests cover concurrent reservations without quota oversell.
- Local Codex capacity permits remain in-process; durable work ordering and weighted finish tags are stored in PostgreSQL.

## Concurrency

Defaults:

- `GATEWAY_MAX_CONCURRENT_JOBS=1`
- `GATEWAY_MAX_QUEUE_SIZE=8`
- `GATEWAY_MAX_CONCURRENT_JOBS_PER_TENANT=1`
- `GATEWAY_MAX_QUEUE_SIZE_PER_TENANT=8`
- `GATEWAY_QUEUE_TIMEOUT_SECS=120`
- `GATEWAY_REQUEST_TIMEOUT_SECS=900`

The execution scheduler uses two pools: a global pool for local Codex capacity and a per-tenant pool keyed by project id. A separate scheduler applies the same global/per-tenant limits before edit request bodies are parsed, bounding concurrent multipart/base64 buffering and staging without holding upload permits during queued provider execution. One project can fill its own queue without blocking another project that still fits in the global pool.

`n > 1` is executed as repeated Codex image attempts, not parallel attempts.

## Proxy

Proxy values are passed to Codex child processes:

- `GATEWAY_HTTP_PROXY` or `HTTP_PROXY`
- `GATEWAY_HTTPS_PROXY` or `HTTPS_PROXY`
- `GATEWAY_ALL_PROXY` or `ALL_PROXY`
- `GATEWAY_NO_PROXY` or `NO_PROXY`

Proxy URLs are not written to traces by this service.

The Codex child process runs with a cleared environment. The gateway only passes a small allowlist such as `PATH`, `HOME` pointed at the active Codex home, `CODEX_HOME`, temporary-directory variables, certificate variables, locale, shell, and the proxy variables above. It does not pass `DATABASE_URL`, gateway/admin tokens, API-key peppers, OTEL exporter settings, or other gateway process secrets.

## OpenTelemetry

Normal logs use `tracing`. If `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the service exports traces via OTLP/HTTP.

The gateway records route, request id, unit counts, generator name, and status. It does not record full prompts, bearer tokens, uploaded image bytes, base64 image data, proxy credentials, or Codex stdout/stderr.

## Compatibility Notes

This is a compatibility subset, not the official OpenAI Images API.

- Only `gpt-image-2` is supported.
- The `gpt-image-2-2026-04-21` snapshot model id is accepted as an alias.
- The backend is strictly native Codex CLI execution via `codex exec`; private ChatGPT/Codex backend APIs are intentionally out of scope.
- Codex's built-in image generation skill is considered native Codex capability. The gateway no longer tries to avoid it; it avoids user config/plugins/apps and gateway secret leakage.
- Responses always use `data[].b64_json`.
- `response_format=url` is rejected.
- Explicit `WIDTHxHEIGHT` requests are enforced through the Codex prompt and then verified as an aspect-ratio constraint by the gateway. Codex-native dimensions with the same ratio are accepted. The gateway does not crop, stretch, or resample images, and responses report the actual output dimensions as `WIDTHxHEIGHT`.
- As a Codex gateway extension, request `size` may also be an aspect ratio such as `1:1`, `4:3`, or `16:9`. These requests do not require an exact pixel size; the gateway prompts Codex for the native ratio and verifies the returned image with a 1% aspect-ratio tolerance. Responses still report the actual output dimensions as `WIDTHxHEIGHT`.
- Requested `output_format` values are encoded by the gateway after Codex generation when possible. JPEG `output_compression` maps to encoder quality and defaults to 100. WebP `output_compression` is accepted for request compatibility, but the current encoder path does not guarantee official quality parity.
- `stream=true` with `partial_images=0` returns a final-only `text/event-stream` response after Codex finishes. The event is `image_generation.completed` for generations and `image_edit.completed` for edits, with `data` containing `{ "type": "...completed", "b64_json": "..." }`. It does not stream thinking or live partial image progress. `partial_images=1..3` is rejected because native Codex CLI does not expose official partial image events.
- `/v1/images/variations` is not implemented.
- `background=transparent` is rejected because `gpt-image-2` does not support native transparent backgrounds. `background=auto` is accepted but responses report `opaque`. The gateway prompts Codex for an opaque output and flattens any returned alpha channel onto a white background.
- `moderation=auto` is accepted. `moderation=low` is rejected as unsupported because the current Codex adapter cannot enforce that official behavior.
- `input_fidelity` is rejected for `gpt-image-2`; this model always uses high fidelity for input images.
- Image edits support multipart uploads and JSON `image` / `images` / `mask` references. JSON clients can send raw base64 with `{ "b64_json": "...", "mime_type": "image/png" }`; `mime_type` may be omitted when the gateway can infer PNG/JPEG/WebP from magic bytes. Base64 data URLs in `image_url` are also supported. Remote `http(s)` image URLs and `file_id` references are rejected because they require additional SSRF-safe fetching or OpenAI Files access outside native Codex CLI.
- `usage` and `revised_prompt` are not returned.
- Local Admin endpoints mirror the shape of OpenAI's project service-account/API-key management, but they manage gateway-local keys only. They do not create or revoke OpenAI platform keys.
- Latency and failure modes follow Codex CLI, not the OpenAI API backend.
- Mask support is best-effort through Codex image context. PNG mask bytes, alpha channel, and mask dimensions against the first input image are validated when the inputs can be decoded, but pixel-level inpainting parity with the official API is not guaranteed.
