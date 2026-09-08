# Media segmentation sidecar API

The media segmentation API is an ai-image-factory extension. It is not part of
the OpenAI Images API or the xAI Imagine API. It describes semantic bounding
boxes over the final, flat image; it does not create editable layers or pixel
masks.

Image generation remains unchanged. In particular,
`POST /v1/images/generations` and `POST /v1/images/edits` keep their existing
request and response contracts and do not wait for segmentation. A client first
displays the final image. An explicitly enabled consumer may then register
the final bytes and enqueue analysis asynchronously, without delaying image
delivery. The consumer owns automatic first-use admission and durable reuse;
Factory does not schedule analysis from image-generation or image-read routes.

## Client lifecycle and persistent reuse

The same interface accepts newly generated and older images; registration uses
the final bytes, not the generation date or generator identity. Integrations
must still authorize access to the source image before submitting it.

- When enabled, a newly completed generation/edit result is automatically
  admitted once per authorized task/result binding after displaying the image.
  An eligible historical image first reads its consumer-owned state; only
  `not_started` may automatically ensure a task on first open. This is not a
  bulk historical backfill.
- The consumer uses a database-unique task/result binding and single-flight
  admission across repeated opens, tabs, concurrent requests and restarts.
  Only the owning durable job submits analysis to Factory. A GET remains
  read-only; UI state loading never directly invokes an analyzer.
- Reuse a completed persisted result without contacting Factory. A
  `processing` result resumes bounded status reads, not another submission.
  `failed` or expired work is never implicitly resubmitted. An uncertain prior
  submission may recover the same saved analyzer key through cache-only lookup;
  a cache miss is not permission to repeat possibly billable analysis.
- A consumer's durable background job may poll its own `processing` result;
  UI polling stops on terminal state, close or image change. Reopening an image
  reads the consumer's saved state and never silently invokes the model again.
- Save successful JSON, actual dimensions, image digest, schema/analyzer
  revision, and authorized task/result binding in the consumer's storage.
  Factory's temporary `asset_id`/`segmentation.id` are not permanent storage
  handles. A successful saved result remains usable after Factory's TTL.
- Cache expiry or failure is not permission to automatically recompute.
  Analysis failure never blocks the original image.

For Blog, the current authorization contract addresses an authenticated user's
task ID and result index. It covers available historical task results without a
date restriction, but not arbitrary external URLs, public gallery images lacking
a task mapping, or reference uploads. Source deletion/unavailability must remain
an explicit error rather than bypassing ownership checks.

## Capabilities

```http
GET /v1/media/capabilities
Authorization: Bearer $API_KEY
```

```json
{
  "supports_bbox_sidecar": true,
  "supports_mask_sidecar": false,
  "supports_terminal_source_release": true,
  "supports_analyzer_key_pin": true
}
```

This endpoint requires `images:read`. Both values are `false` when the optional
sidecar service is not configured. Capability discovery therefore does not
return `503` merely because the worker is disabled.

Capabilities describe protocol support, not worker liveness. All four flags
are false when the optional service is not configured.

### Read-only worker readiness

`GET /v1/media/readiness` requires `images:read` and never starts work. Example:

```json
{
  "object": "media.readiness",
  "status": "ready",
  "analyzer_key": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "last_heartbeat_at_ms": 1788848639234,
  "heartbeat_age_ms": 1200,
  "heartbeat_ttl_ms": 150000,
  "source_release_enabled": true
}
```

The opaque key is exactly 64 lowercase hexadecimal ASCII characters and
contains no account identity or credentials. The heartbeat must match the
gateway's analyzer key and be at most 150 seconds old. A missing, future-dated,
stale, or unreadable heartbeat returns `503` with `status="not_ready"` and a
bounded `reason`; the response uses `Cache-Control: no-store`. A fresh heartbeat
proves the worker loop progressed, **not** that a future model call will succeed.
The isolated bbox worker does not make the image-generation `/readyz` fail.
Fresh workers with inconsistent cleanup modes return `503` with reason
`worker_configuration_mismatch` and `source_release_enabled=null`; a last-writer
heartbeat cannot hide an older worker still deleting source bytes.

## Register final image bytes

```bash
curl "$BASE_URL/v1/media/assets" \
  -H "Authorization: Bearer $API_KEY" \
  -F 'image=@final-image.jpg'
```

The multipart body must contain exactly one field named `image`. The image may
be PNG, JPEG, or WebP, must not exceed 20 MiB, and must be within 8192 pixels per
edge and 16 megapixels. Registration fully decodes the image and derives its
content digest and actual dimensions; model-reported dimensions are never
trusted.

The endpoint requires `images:write` and returns:

```json
{
  "object": "media.asset",
  "asset_id": "img_0123456789abcdef0123456789abcdef",
  "image": {
    "width": 1024,
    "height": 1024,
    "coordinate_system": "pixel_xyxy"
  },
  "expires_at": 1788796800
}
```

Assets are isolated by tenant, project, and credential owner. Re-registering
identical bytes in the same scope reuses the asset. Asset metadata and its
sidecars have a 24-hour retention window. With terminal-source cleanup enabled,
source bytes may be deleted earlier, after every associated analysis is
terminal; cached JSON and IDs remain available until the original expiry.

The standard Images API currently returns image bytes (for example,
`b64_json`) rather than a persistent media asset identifier. Registering final
bytes is therefore explicit and provider-neutral: an image generated by Grok,
Codex, or another provider follows the same path.

## Request a segmentation

```http
POST /v1/media/segments
Authorization: Bearer $API_KEY
Content-Type: application/json

{
  "asset_id": "img_0123456789abcdef0123456789abcdef",
  "cached_only": false,
  "detail": "bbox",
  "language": "zh-CN",
  "mask_format": "none",
  "expected_analyzer_key": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

Defaults are `cached_only=true`, `detail="bbox"`, `language="zh-CN"`, and
`mask_format="none"`. Version 1 only accepts these detail, language, and mask
values. User-visible names and categories are Chinese; `mask_key` and generated
identifiers remain stable English-compatible keys. The JSON body is limited to
4 KiB independently of the general upload limit.

- `cached_only=true` requires `images:read`. A cache miss returns `404` with
  error code `segments_not_cached` and never starts model work.
- `cached_only=false` requires `images:write`. A cache miss atomically enqueues
  one analysis job and returns `202 Accepted` with `Retry-After: 2`.
- A cached `completed` or `failed` result returns `200 OK`.

Repeated `cached_only=false` submissions of the same scoped content and analyzer
configuration reuse the same queued, processing, completed, or failed result.
They do not reset the TTL or create a second model attempt. Transport retries
therefore reuse the original result even after an uncertain POST response.
Queue saturation is a rejected enqueue, not an accepted model attempt; callers
may retry admission with bounded backoff. Stop retrying once a result is known.

The cache key covers the asset content digest, analyzer provider/model and
prompt revision, schema version, language, detail, and mask format. It is not
coupled to the provider that generated the image.

`expected_analyzer_key` is optional for compatibility; new durable consumers
must obtain it from readiness and save it **before** their first POST. It must
match `^[0-9a-f]{64}$`. Keep that same value for every ensure/cache recovery of
the business task, including recovery after a lost HTTP response. If the gateway
configuration has changed, Factory can still return an existing result for the
same scope, asset, and opaque key without reconstructing the old model config.
If no such result exists, `409 segmentation_analyzer_changed` stops submission;
it never silently creates a result under the new configuration. Do not replace
the saved key merely to turn this conflict into success.

### Source ownership and errors

The existing 64-source/512 MiB per-project limits count sources still owned by
Factory, including expired bytes awaiting deletion. Metadata/results are
separately bounded to 4096 assets per project and retain the original 24-hour
TTL. Active analysis limits remain 64 globally and 16 per project.

Terminal cleanup moves sources through `retained → releasing → released`.
Queued/processing analyses prevent release. Deletion failures keep the source
charged to storage capacity; retries and crash recovery use the exact immutable
blob session. Capacity is freed only after deletion is confirmed. Result reads
do not require the pixels and remain available throughout cleanup.

- `409 asset_source_releasing`: source cleanup is in progress; retry the same
  registration with bounded backoff. Do not start another analysis.
- `409 asset_source_released`: the requested result is absent and source bytes
  have been reclaimed. First try `cached_only=true` with the saved analyzer key.
  If genuinely absent and the key has not changed, register the same final bytes
  to restore source ownership, then ensure with the same key. Existing completed
  and failed results are reused, never reset by re-registration.
- `404 asset_not_found` / `segmentation_not_found`: unknown, expired, or outside
  the caller's scope. A persisted consumer result remains usable independently;
  expiry is not automatic permission to run the model again.

Neither registration nor source restoration extends the existing asset TTL.

## Poll a segmentation

```http
GET /v1/media/segments/seg_0123456789abcdef0123456789abcdef
Authorization: Bearer $API_KEY
```

Polling requires `images:read`. Known results always return `200 OK`; a
`processing` result also includes `Retry-After: 2`. Unknown, expired, or
cross-scope identifiers return `404`, without revealing whether another scope
owns the identifier.

The completed response contract is pinned by
[`contracts/media-segmentation-v1.json`](contracts/media-segmentation-v1.json).
Coordinates are integer pixel `[x1, y1, x2, y2]` boxes. The server verifies
coordinate order and image bounds, rejects cyclic/missing parents, ensures each
group contains its child boxes, and generates all IDs and stable keys itself.

`failed` is a terminal state and includes an `error` object. Failed jobs are not
automatically retried because another CLI call may incur cost; a future retry
requires an explicit new contract. A sidecar failure never changes the outcome
of the original image generation or edit.

## Runtime boundary

The gateway only validates uploads, serves cached state, and durably enqueues
work. The optional `segmentd` worker performs the model call, validation, and
terminal write. Enable both with the documented bbox runtime configuration;
when the service is absent, upload/request/poll endpoints return the standard
`503 service_unavailable` envelope while capability discovery returns `false`.

This separation keeps CLI startup, image decoding, and bbox inference entirely
off the existing image-generation hot path.

### Runtime configuration

The gateway process reads the following entries from `app.env`:

```dotenv
GATEWAY_BBOX_ENABLED=true
GATEWAY_BBOX_MODEL=gpt-5.6-luna
GATEWAY_BBOX_REASONING_EFFORT=none
```

The independent worker additionally reads `segments.env`:

```dotenv
GATEWAY_BBOX_CODEX_BIN=/opt/ai-image-factory/provider-tools/codex/0.153.4/bin/codex
GATEWAY_BBOX_CODEX_HOME=/var/lib/ai-image-factory/bbox-codex
GATEWAY_BBOX_MODEL=gpt-5.6-luna
GATEWAY_BBOX_REASONING_EFFORT=none
```

`GATEWAY_BBOX_MODEL` and `GATEWAY_BBOX_REASONING_EFFORT` must match between the
gateway and worker because they are part of the queue and cache identity. The
Codex executable must be an absolute, versioned native binary. The bbox Codex
home is intentionally separate from generation accounts. Provision both for
the `ai-image-factory` service user and protect `segments.env` and the home with
private ownership/permissions (for example, owner-only access and mode `0600`
for credential-bearing files). The supplied systemd unit also applies
`UMask=0077`.

Queued work has a five-minute admission deadline. A worker claim has a
120-second fenced lease and contains a 95-second analysis timeout. Expiry
publishes a terminal failure rather than replaying a possibly billable call.

### Performance evidence

A cache-only POST performs two bounded, read-only PostgreSQL lookups: one for
the scope-bound unexpired asset and one indexed cache-result lookup. It takes no
advisory lock, writes no rows, decodes no image, and starts no CLI. A GET by
segmentation ID performs one indexed, scope-bound read.

The worker stores `cli_start_ms`, `bbox_ms`, `validation_ms`, and `retries` in
`timings_json`. `cli_start_ms` is pre-spawn preparation and `bbox_ms` is the
interval from observed process spawn to validated output capture. The
`bbox_sidecar` structured log additionally reports the measured terminal-store
duration as `storage_ms`; that value is intentionally not represented as a
persisted per-row timing. A PostgreSQL deployment can calculate persisted stage
percentiles, for example:

```sql
SELECT
  COUNT(*) AS n,
  percentile_cont(0.50) WITHIN GROUP
    (ORDER BY (timings_json->>'bbox_ms')::double precision) AS bbox_p50_ms,
  percentile_cont(0.95) WITHIN GROUP
    (ORDER BY (timings_json->>'bbox_ms')::double precision) AS bbox_p95_ms,
  percentile_cont(0.50) WITHIN GROUP
    (ORDER BY (timings_json->>'validation_ms')::double precision) AS validation_p50_ms,
  percentile_cont(0.95) WITHIN GROUP
    (ORDER BY (timings_json->>'validation_ms')::double precision) AS validation_p95_ms
FROM media_segment_results
WHERE timings_json IS NOT NULL
  AND status = 'completed'
  AND updated_at_ms >= :window_start_ms
HAVING COUNT(*) >= 20;
```

The `HAVING` guard deliberately returns no percentile row for fewer than 20
observations. For smaller samples, report `n` and min/median/max only; do not
label an order statistic as production P95. The PostgreSQL HTTP integration
test also measures 25 repeated cache hits and prints its observed P50/P95, but
that in-process test is evidence for regression comparison, not a production
latency guarantee.

On 2026-09-08, the real PostgreSQL HTTP test measured 25 cached requests at
P50 0.418 ms and P95 1.226 ms. A separate live Codex run over a 1280x720 image
completed the worker path in 20.444 seconds and returned seven Chinese groups;
its subsequent cached HTTP lookup took 1.538 ms. The single live model run is a
smoke result, not a model latency percentile or SLA.

The final auth-isolated run completed in 19.695 seconds. Full gate results,
baseline exceptions and real Blog-adapter evidence are recorded in
[`media-segments-verification-20260908.md`](media-segments-verification-20260908.md).
