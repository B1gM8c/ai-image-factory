# AI Image Factory Platform Upgrade

> Historical implementation snapshot. The authoritative target architecture is
> `2026-ai-image-factory-target-architecture.md`. Where this document describes
> future state or gaps differently, follow the target architecture.

## Goal

Turn the current `gpt-image-2` Codex CLI gateway into a multi-provider media API
platform while preserving the working OpenAI Images-compatible contract.

The repository is already a monorepo, so multiple `src` directories are expected:

- `apps/admin-console/src` is the Next.js operator console.
- `crates/image-gateway/src` is the Rust Axum API gateway.
- `crates/provider-contracts/src` is shared media/provider/job metadata.

The structural problem is not the number of `src` roots. The problem is that
the gateway must keep API handlers, provider traits, concrete CLI execution,
quota, scheduling, database setup, and platform policy behind stable module
boundaries. That shape is fine for a synchronous MVP, but it will not scale
cleanly to Codex CLI, JiMeng CLI, Grok CLI, managed APIs, video tasks, billing,
and multi-instance scheduling if every provider reaches into the HTTP layer.

## Current Boundaries

The current active product is:

- Rust service: `crates/image-gateway`
- Binary/package name: `gpt-image-2-gateway`
- First active provider: `openai-codex`
- Public compatible endpoints:
  - `POST /v1/images/generations`
  - `POST /v1/images/edits`
  - `GET /v1/models`
  - `GET /openapi.json`
  - `GET /docs`

The first provider remains native `codex exec` execution. It does not call
private ChatGPT or Codex backend endpoints directly. It keeps the existing
protections: isolated request directory, environment allowlist, disabled user
plugins/apps, output file validation, process timeout, and no local path exposure
in responses.

Existing platform pieces:

- Gateway-local service accounts and API keys are implemented.
- 5-hour and 7-day image unit accounting is implemented.
- Global and per-tenant in-process concurrency limits are implemented.
- PostgreSQL tables for `usage_events`, `quota_reservations`, and `jobs` exist.
- OpenAPI, Scalar docs, request IDs, tracing, and OTLP export exist.

Important gaps:

- `quota_reservations` and `jobs` are not wired into a real runtime lifecycle.
- Quota is charged synchronously before provider execution and cannot be
  precisely released on failure, timeout, cancel, or retry.
- Scheduling is in-memory, so it is not safe for multi-instance deployments.
- Generic CLI process execution is still inside the Codex provider and should
  move to an `adapters/cli` module before adding the next CLI.
- The admin console uses static provider/job data and does not read gateway
  state.
- The admin console BFF injects `GATEWAY_ADMIN_TOKEN` only after callers present
  `ADMIN_CONSOLE_ACCESS_TOKEN`, and it proxies only an allowlist of
  admin/system routes. A full user/session auth layer is still required before
  public deployment.

## API Strategy

There are two API layers.

### OpenAI-Compatible Images Facade

Keep this layer narrow and stable:

- `GET /v1/models`
- `POST /v1/images/generations`
- `POST /v1/images/edits`

This layer should accept only fields that can be represented safely in the
OpenAI Images contract. Provider-specific fields must not leak into
`ImagesResponse`.

The current Codex provider remains a compatibility subset:

- Responses always use `data[].b64_json`.
- `response_format=url` is rejected.
- `stream=true` is final-only SSE until true partial image events are supported.
- `partial_images > 0` is rejected for Codex CLI.
- `background=transparent` is rejected for `gpt-image-2`.
- Aspect-ratio sizes such as `16:9` are a gateway extension.

Official OpenAI docs currently show `/v1/images/generations` and
`/v1/images/edits` as the Images API shape, including streaming image events.
The current public docs are not fully consistent about `gpt-image-2` visibility,
so this project should describe the Codex path as a local compatibility subset,
not as a complete official OpenAI model implementation.

### Platform-Native Jobs API

Add a provider-neutral async API for work that cannot fit cleanly into
OpenAI-compatible images:

- `POST /v1/jobs`
- `GET /v1/jobs/{job_id}`
- `POST /v1/jobs/{job_id}/cancel`
- `GET /v1/jobs/{job_id}/artifacts`
- `GET /v1/providers`
- `GET /v1/providers/{provider_id}/models`

The create body should use a stable envelope:

```json
{
  "kind": "image_generation",
  "model": "gpt-image-2",
  "provider": "openai-codex",
  "input": {
    "prompt": "a product icon"
  },
  "parameters": {
    "size": "1024x1024",
    "quality": "auto"
  },
  "provider_options": {}
}
```

Supported `kind` values should start with:

- `image_generation`
- `image_edit`
- `video_generation`

Provider-specific parameters belong in `provider_options` or in typed
provider-native job schemas. Examples:

- xAI/Grok image: `aspect_ratio`, `resolution`
- xAI/Grok video: `duration`, `aspect_ratio`, `resolution`
- Seedance/JiMeng video: task content, duration, resolution, camera options,
  callback settings, watermark controls

Do not put provider task IDs, provider request IDs, temporary provider URLs, cost
ticks, or native billing fields inside `ImagesResponse`. Store them as job,
metering, and artifact metadata.

### Video Compatibility

Do not label video endpoints as OpenAI-compatible.

If xAI client compatibility is useful, add an alias:

- `POST /v1/videos/generations`
- `GET /v1/videos/{request_id}`

This should map to the internal `jobs` table and be documented as
xAI-compatible/provider-native. Seedance and JiMeng should use `/v1/jobs` first,
because their official shape is task creation plus polling/cancel.

## Current And Target Directory Structure

The backend crate now lives under `crates/image-gateway`, matching the
repository convention that Rust backend modules and shared contracts live under
`crates`. `apps` is reserved for browser-facing applications such as the admin
console.

```text
apps/
  admin-console/
    src/
      app/
      components/
      lib/
crates/
  image-gateway/
    src/
      api/
        admin.rs
        edit_input.rs
        images.rs
        middleware.rs
        responses.rs
      core/
        image_bytes/
        normalization/
        provider/
      providers/
        openai_codex/
      api_keys/
      auth/
      config/
      docs/
      error/
      models/
      scheduler/
      size/
      telemetry/
      usage/
    migrations/
    tests/
  provider-contracts/
    src/
      jobs/
      media/
      official_params/
      provider/
```

The next code migration should be mechanical:

1. Move generic command execution, environment allowlisting, process-group kill,
   timeout, and output scanning into `adapters/cli`.
2. Keep `providers/openai_codex` responsible only for Codex prompts, Codex model
   capability mapping, and conversion between core jobs and Codex CLI execution.
3. Add `api/jobs`, `storage`, `metering`, `quota/reservations`, and `workers`
   only when `/v1/jobs` is implemented.

## Provider Model

Use a runtime provider registry. `crates/provider-contracts` is the shared
metadata crate, but it describes capabilities rather than owning execution
policy.

Minimum provider metadata:

- provider id
- display name
- status: active, planned, disabled
- execution mode: native CLI, CLI bridge, managed API
- supported model ids and aliases
- media kinds: image, video
- operations: generation, edit, variation
- sync/async support
- streaming support: none, final-only, partial
- input limits: prompt length, image count, video/audio inputs, max upload bytes
- output formats
- billing unit type: image, second, token, provider-native unit
- provider-native parameter schema

Provider adapters should expose a small surface:

```rust
trait ProviderAdapter {
    fn definition(&self) -> ProviderDefinition;
    async fn submit(&self, job: MediaJob, account: ProviderAccountLease) -> ProviderSubmitResult;
    async fn poll(&self, provider_job_id: &str) -> ProviderPollResult;
    async fn cancel(&self, provider_job_id: &str) -> ProviderCancelResult;
}
```

Synchronous Codex image generation can be implemented as a submit call that runs
to completion inside the worker. Async providers should return a provider job id
and move the local job into `provider_waiting`.

## Job Lifecycle

Use this state machine for long-running work:

```text
accepted -> reserved -> queued -> leased -> running -> provider_waiting
-> artifact_ready -> succeeded
                     -> failed
                     -> canceled
                     -> timed_out
```

Responsibilities:

- `api/jobs` validates and creates a job.
- `quota` reserves units before queue admission.
- `scheduler` admits and leases jobs.
- `workers` execute provider attempts and poll async providers.
- `storage` persists artifacts before success is committed.
- `metering` records immutable facts for every attempt and outcome.
- `quota` commits or releases reservations.
- `billing` turns metering facts into user-facing balances, invoices, or plan
  usage.
- `webhooks` publishes completion events after the final state is durable.
- `audit_logs` records admin/security-sensitive actions.

## Billing And Metering

Separate quota, metering, and billing.

- Quota answers: may this tenant start this job now?
- Metering answers: what happened, with which provider, model, units, duration,
  cost basis, and outcome?
- Billing answers: how does the metering fact affect balance, invoice, plan, or
  refund state?

Do not make provider adapters mutate billing directly. They should report
provider usage and artifacts; platform services decide how that maps to user
charges.

Minimum tables to add or evolve:

- `jobs`
- `job_attempts`
- `job_leases`
- `quota_reservations`
- `metering_events`
- `billing_accounts`
- `provider_accounts`
- `provider_account_leases`
- `artifacts`
- `webhook_endpoints`
- `webhook_deliveries`
- `audit_events`

## Admin Console

The Next.js console is an operator workspace, not a landing page.

Before any public deployment:

- Replace the temporary `ADMIN_CONSOLE_ACCESS_TOKEN` with real console-level
  authentication.
- Keep the strict BFF route allowlist.
- Inject `GATEWAY_ADMIN_TOKEN` only for allowed server-side admin routes.
- Never expose gateway admin tokens to browser code.

Then replace static data with real API calls for:

- provider registry
- API keys and service accounts
- quota and usage
- jobs and leases
- metering and billing
- webhooks
- audit events
- runtime health and trace links

## Migration Plan

1. Secure the admin-console BFF with auth and route allowlisting.
2. Move provider-neutral traits and job structs out of `openai_codex`.
3. Extract generic CLI process execution into `adapters/cli`.
4. Add provider registry APIs and make `/v1/models` read from compatible active
   models.
5. Introduce DB-backed job creation, leases, and state transitions.
6. Split current `usage::charge` into quota reserve, metering record, and quota
   commit/release.
7. Keep `/v1/images/*` as a synchronous facade over the new job service.
8. Add `/v1/jobs` and artifact storage for async providers.
9. Add provider account leases, cooldowns, and encrypted credential storage.
10. Add webhook outbox, audit logs, metrics, and OpenAPI diffing in CI; retain
    the bounded `/readyz` delivered in Phase 2AF.
11. Connect admin-console modules to real gateway APIs.
12. Add JiMeng/Seedance/Grok adapters behind the registry one provider at a time.

## Verification Plan

Unit tests:

- provider capability routing and unsupported operation errors
- quota reserve, commit, release, and expiry
- job state machine legal and illegal transitions
- CLI env allowlist and output collection
- provider account lease and cooldown logic
- webhook signing and retry backoff
- audit event redaction

Integration tests:

- PostgreSQL concurrent reservations cannot oversell quota
- two workers cannot lease the same job
- expired leases and reservations are recovered after worker crash
- API key create, authenticate, list, revoke, and last-used updates
- artifact rows are durable before job success

E2E tests:

- create service account -> submit image job -> fetch artifact -> verify metering
- submit video job -> poll -> provider waiting -> success -> webhook delivery
- cancel queued/running/provider-waiting jobs
- admin console cannot proxy admin routes without console auth

Load and failure tests:

- global and per-tenant queues at capacity
- provider timeout and failure storms
- DB lock contention under high reservation concurrency
- webhook delivery backlog
- no token, prompt, base64 image, proxy secret, or provider credential leakage in
  logs/traces/errors

## Source Notes

Official interface checks used for this design:

- OpenAI Images guide and API reference:
  `https://developers.openai.com/api/docs/guides/image-generation`
  and `https://developers.openai.com/api/reference/resources/images`
- xAI Images API:
  `https://docs.x.ai/developers/rest-api-reference/inference/images`
- xAI video generation:
  `https://docs.x.ai/developers/model-capabilities/video/generation`
- BytePlus ModelArk / Seedance API navigation:
  `https://docs.byteplus.com/en/docs/ModelArk/1520757`
- Volcengine JiMeng docs navigation:
  `https://www.volcengine.com/docs/85621/1995636`
