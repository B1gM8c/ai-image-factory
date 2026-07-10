# gpt-image-2 Codex Gateway

Rust API gateway for AI Image Factory. This app is the first active provider
runtime: a local OpenAI Images API-compatible gateway backed by Codex CLI image
generation.

It exposes:

- `POST /v1/images/generations`
- `POST /v1/images/edits`
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
export GATEWAY_CODEX_HOME='/srv/gpt-image-codex-home'
cargo run --bin factoryctl -- migrate
cargo run
```

From the repository root, use:

```bash
cargo run -p gpt-image-2-gateway --bin factoryctl -- migrate
cargo run -p gpt-image-2-gateway
```

Default bind address is `127.0.0.1:8787`. Every startup requires at least one of `GATEWAY_API_TOKEN` or `GATEWAY_ADMIN_TOKEN`, plus an explicit absolute, existing, writable `GATEWAY_CODEX_HOME`. If both tokens are configured, they must be different. `GATEWAY_ADMIN_TOKEN` protects Admin endpoints for creating and revoking project API keys; it never authorizes image calls. An admin-only startup can bootstrap project keys, and image calls must then use one of those keys. `GATEWAY_API_TOKEN` remains a legacy image token and is not accepted on Admin endpoints.

The gateway does not yet provide native TLS and rejects every non-loopback bind. Bind it to loopback and place a TLS reverse proxy on the same host in front of it. Provision `GATEWAY_CODEX_HOME` before every startup and grant the gateway service account permission to create and remove files there; the path must not be a symlink or the filesystem root. `--ignore-user-config` skips user `config.toml`; Codex may still load its native system image generation skill.

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

- API keys are gateway-local and stored as SHA-256 hashes. The unredacted key value is only returned when a project service account is created.
- Usage is isolated by project/tenant. Legacy `GATEWAY_API_TOKEN` calls use the `proj_default` tenant.
- `GATEWAY_IMAGE_LIMIT_5H` defaults to `40`.
- `GATEWAY_IMAGE_LIMIT_7D` defaults to `200`.
- After validation and queue admission, a request reserves `n` image units before Codex generation starts.
- Successful generation commits the reservation. Generation or edit failure releases it, so failed provider attempts are not charged.
- Responses include `x-request-id`, `openai-version`, `openai-project`, and image-unit quota headers when available.
- PostgreSQL reservation transitions are serialized per tenant, and integration tests cover concurrent reservations without quota oversell.
- The request scheduler is still in-process, so queue state is not shared across gateway instances.

## Concurrency

Defaults:

- `GATEWAY_MAX_CONCURRENT_JOBS=1`
- `GATEWAY_MAX_QUEUE_SIZE=8`
- `GATEWAY_MAX_CONCURRENT_JOBS_PER_TENANT=1`
- `GATEWAY_MAX_QUEUE_SIZE_PER_TENANT=8`
- `GATEWAY_QUEUE_TIMEOUT_SECS=120`
- `GATEWAY_REQUEST_TIMEOUT_SECS=900`

The scheduler uses two pools: a global pool for local Codex capacity and a per-tenant pool keyed by project id. One project can fill its own queue without blocking another project that still fits in the global pool.

`n > 1` is executed as repeated Codex image attempts, not parallel attempts.

## Proxy

Proxy values are passed to Codex child processes:

- `GATEWAY_HTTP_PROXY` or `HTTP_PROXY`
- `GATEWAY_HTTPS_PROXY` or `HTTPS_PROXY`
- `GATEWAY_ALL_PROXY` or `ALL_PROXY`
- `GATEWAY_NO_PROXY` or `NO_PROXY`

Proxy URLs are not written to traces by this service.

The Codex child process runs with a cleared environment. The gateway only passes a small allowlist such as `PATH`, `HOME` pointed at the active Codex home, `CODEX_HOME`, temporary-directory variables, certificate variables, locale, shell, and the proxy variables above. It does not pass `DATABASE_URL`, `GATEWAY_API_TOKEN`, OTEL exporter settings, or other gateway process secrets.

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
- Explicit `WIDTHxHEIGHT` requests are enforced through the Codex prompt and then verified by the gateway. The gateway does not crop, stretch, or resample images to force a requested size; if Codex returns the wrong dimensions, the request fails. Responses report the actual output dimensions as `WIDTHxHEIGHT`.
- As a Codex gateway extension, request `size` may also be an aspect ratio such as `1:1`, `4:3`, or `16:9`. These requests do not require an exact pixel size; the gateway prompts Codex for the native ratio and verifies the returned image with a 1% aspect-ratio tolerance. Responses still report the actual output dimensions as `WIDTHxHEIGHT`.
- Requested `output_format` values are encoded by the gateway after Codex generation when possible. JPEG `output_compression` maps to encoder quality and defaults to 100. WebP `output_compression` is accepted for request compatibility, but the current encoder path does not guarantee official quality parity.
- `stream=true` with `partial_images=0` returns a final-only `text/event-stream` response after Codex finishes. The event is `image_generation.completed` for generations and `image_edit.completed` for edits, with `data` containing `{ "type": "...completed", "b64_json": "..." }`. It does not stream thinking or live partial image progress. `partial_images=1..3` is rejected because native Codex CLI does not expose official partial image events.
- `/v1/images/variations` is not implemented.
- `background=transparent` is rejected because `gpt-image-2` does not support native transparent backgrounds. `background=auto` is accepted but responses report `opaque`. The gateway prompts Codex for an opaque output and flattens any returned alpha channel onto a white background.
- `moderation=auto` and `moderation=low` are accepted for OpenAI request compatibility, but moderation behavior is governed by the underlying Codex image tool.
- `input_fidelity` is rejected for `gpt-image-2`; this model always uses high fidelity for input images.
- Image edits support multipart uploads and JSON `image` / `images` / `mask` references. JSON clients can send raw base64 with `{ "b64_json": "...", "mime_type": "image/png" }`; `mime_type` may be omitted when the gateway can infer PNG/JPEG/WebP from magic bytes. Base64 data URLs in `image_url` are also supported. Remote `http(s)` image URLs and `file_id` references are rejected because they require additional SSRF-safe fetching or OpenAI Files access outside native Codex CLI.
- `usage` and `revised_prompt` are not returned.
- Local Admin endpoints mirror the shape of OpenAI's project service-account/API-key management, but they manage gateway-local keys only. They do not create or revoke OpenAI platform keys.
- Latency and failure modes follow Codex CLI, not the OpenAI API backend.
- Mask support is best-effort through Codex image context. PNG mask bytes, alpha channel, and mask dimensions against the first input image are validated when the inputs can be decoded, but pixel-level inpainting parity with the official API is not guaranteed.
