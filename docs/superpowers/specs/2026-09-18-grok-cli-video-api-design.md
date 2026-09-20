# Grok CLI 1.0.34 Video API Design

Date: 2026-09-18

Status: approved for implementation on 2026-09-19

Scope owner: AI Image Factory

## Decision

Expose the existing asynchronous xAI-shaped endpoints:

- `POST /v1/videos/generations`
- `GET /v1/videos/{request_id}`

The public request and response vocabulary follows xAI's published video API. The executable subset is the intersection of that public contract and the media tools registered by the pinned Grok CLI 1.0.34 binary. Unsupported official fields fail before admission, billing reservation, queueing, or provider execution.

This phase does not add `/v1/videos/edits`, `/v1/videos/extensions`, or a Factory-private video endpoint. The later bounded extension exposes Grok CLI-only `keyframes` on the same generation endpoint, explicitly labels them as a Factory extension, and retains the public reference-image limit rather than exposing the CLI's higher raw count.

## Evidence and authority

The contract is based on the xAI capability guides and public OpenAPI document observed on 2026-09-18:

- <https://docs.x.ai/developers/model-capabilities/video/generation>
- <https://docs.x.ai/developers/model-capabilities/video/image-to-video>
- <https://docs.x.ai/developers/model-capabilities/video/reference-to-video>
- <https://api.x.ai/api-docs/openapi.json>

The executable binding is based on a live Grok CLI 1.0.34 registered-tool schema probe:

- `image_to_video`: one image, optional prompt, duration 6 or 10 seconds, 480p or 720p.
- `reference_to_video`: reference images, first frame, last frame, preset voices, duration 1 through 15 seconds, seven official aspect ratios, 480p or 720p.
- `image_gen` plus `image_to_video`: the existing CLI-only composition used for text-to-video.
- No registered CLI tool for video edit or video extension.
- No registered CLI control for 1080p or `generate_audio=false`.

The capability guide is newer than the public OpenAPI snapshot for `last_frame` and `generate_audio`. We represent the published fields in the wire DTO, then apply the pinned CLI capability projection. We do not infer support from documentation alone.

## Supported public modes

| xAI generation mode | Official request fields | Factory execution in this phase | Bound limits |
| --- | --- | --- | --- |
| Text to video | `prompt` | `image_gen` followed by `image_to_video` | 6 or 10 seconds; 480p or 720p; seven official ratios accepted by the CLI composition |
| Image to video | `image`, optional `prompt` | one `image_to_video` CLI tool call | 6 or 10 seconds; 480p or 720p; input aspect ratio retained |
| Reference to video | one or more of `reference_images`, `reference_audios`, `last_frame`, or Factory-extension `keyframes`; optional pinned first frame in `image` | one `reference_to_video` CLI tool call | 1 to 15 seconds; 480p or 720p; max 7 reference images; max 4 mid-clip keyframes; max 3 preset voices; seven official ratios |
| First and last frame | `image` plus `last_frame`, optional references and prompt | `reference_to_video` with `first_frame` and `last_frame` | same as reference to video |

The API accepts xAI's `duration` and `seconds` alias. Default values remain official: duration 8 seconds, resolution 480p, and aspect ratio 16:9 when the selected mode uses an explicit ratio. A default must still be executable by the selected CLI tool. For example, an omitted duration on image-to-video defaults to 8 officially but is rejected by the 1.0.34 CLI binding because `image_to_video` supports only 6 or 10. The service never silently changes 8 to 6 or 10.

## Public request boundary

Extend the current `XaiVideoGenerationRequest` with only published generation fields needed by the supported intersection:

```json
{
  "model": "grok-imagine-video-1.5",
  "prompt": "optional when a frame or reference is present",
  "duration": 10,
  "aspect_ratio": "16:9",
  "resolution": "720p",
  "image": {"url": "https://example.invalid/first.png"},
  "last_frame": {"url": "https://example.invalid/last.png"},
  "reference_images": [{"url": "https://example.invalid/subject.png"}],
  "keyframes": [{"image": {"url": "https://example.invalid/middle.png"}, "timestamp_s": 3.0}],
  "reference_audios": [{"voice_id": "eve"}]
}
```

The DTO retains official fields such as `output`, `storage_options`, `user`,
URL/data-URI inputs, and the `input_reference` alias for wire-shape
compatibility. The Grok CLI V2 binding rejects `output` and `storage_options`
with HTTP 400 before admission because it has no remote delivery adapter.
Image inputs may be base64 data URLs or bounded public HTTPS URLs (no redirects
or private-address resolution). `file_id` remains represented but is rejected
before admission until the Factory has a verified xAI Files API fetch binding.

The DTO also represents `generate_audio`, because it is a published field, but the CLI projection accepts only omitted or `true`. `false` returns the existing xAI-shaped invalid-request response with `param=generate_audio`. Reference-audio entries accept `voice_id`; caller-supplied audio `url` entries fail with `param=reference_audios` because the live CLI tool exposes preset voices rather than arbitrary audio clips.

All unknown fields continue to be rejected. The existing `request_id` start response and polled terminal response remain unchanged.

## Workflow classification and validation

Classification is deterministic and independent of the executor:

1. No frames or references: text-to-video; `prompt` is required.
2. `image` only: image-to-video; `prompt` is optional.
3. Any `last_frame`, `reference_images`, `reference_audios`, or `keyframes`: reference-to-video; `image`, when present, is the pinned first frame; `prompt` is optional.

Validation runs before remote image fetching and before durable side effects:

- only `grok-imagine-video-1.5` and explicitly retained legacy aliases are accepted for newly admitted work;
- at most 7 `reference_images`, at most 4 `keyframes`, and at most 3 `reference_audios`;
- at most 9 combined first-frame, last-frame, reference-image, and keyframe image inputs under the existing immutable V2 pricing surface;
- keyframe timestamps are strictly increasing, strictly inside the clip, and on the 1/3-second grid; off-grid values fail closed instead of being silently snapped;
- each image specifies exactly one of `url` or `file_id`;
- each audio specifies exactly one of `voice_id` or `url`, followed by the CLI-binding rejection of `url`;
- voice IDs are trimmed, bounded, and compared case-insensitively while preserving the original value in the canonical command;
- 1080p, `generate_audio=false`, edit, and extension fail closed;
- V2 `output` and `storage_options` fail closed with HTTP 400 before admission;
- image-to-video rejects explicit `aspect_ratio` if the CLI would ignore it;
- all source images are fetched once, size/type bounded, digest sealed, and staged before admission completes;
- a CLI capability mismatch produces a stable public parameter error, never a silent downgrade.

## Versioned command and replay compatibility

Add a new canonical command schema and adapter revision for newly admitted requests. Do not rewrite or reinterpret existing V1 command JSON.

- V1 commands keep the 1.0.5 parser, adapter revision, and existing execution path so queued and replayed jobs remain valid.
- V2 commands encode the workflow, first/last/reference image digests, preset voices, duration, ratio, resolution, model, and source-command digest.
- Executor selection is driven by the command schema and frozen adapter revision, not the currently installed CLI version.
- New admission emits V2 only after the 1.0.34 executable, profile, pricing, and readiness rows agree on the same immutable identity.

No SQL schema migration is required: durable commands are already versioned JSON and the existing tables carry schema/profile/adapter identity. Deployment does require an additive V2 profile/configuration rollout. Old V1 profile rows must remain available until no V1 job can be replayed.

## CLI execution binding

New V2 work executes through the pinned Grok CLI binary and its registered media tools. It does not add a new native xAI REST client.

The current V1 direct image-to-video executor remains only for V1 replay compatibility. Newly admitted V2 image-to-video jobs use the CLI tool binding, which satisfies the requested provider boundary and keeps every new capability on one receipt-validation path.

Each V2 invocation:

- uses a private, isolated `HOME`, `GROK_HOME`, workspace, and session ID;
- enables only the exact required tool set;
- supplies a deterministic dispatch prompt and bounded turn count;
- validates the emitted tool name and semantic JSON arguments against the frozen command;
- accepts exactly one bounded MP4 artifact from the expected session directory;
- records safe stage timings and hashed diagnostics without prompts, credentials, URLs, voice IDs, or tool arguments in logs.

Text-to-video is the sole two-tool flow. The generated first-frame artifact is passed directly to `image_to_video` inside the same isolated session and is not published as a customer output.

## Performance and coupling

Reuse the existing API, admission, scheduler, runner journal, account isolation, artifact store, billing, and polling response. Do not add a service, queue, scheduler, database abstraction, or per-mode executor.

To minimize overhead:

- capability schemas are probed during release verification, not per request;
- the immutable CLI digest and adapter revision are resolved at process startup;
- one CLI process performs one media request; text-to-video is the only intentional two-tool exception;
- reference images are downloaded and hashed once, then reused from the sealed workspace;
- polling clients use the existing `request_id` endpoint; API workers never block for generation;
- no speculative video, mask, frame, or audio preprocessing is performed.

Metrics split queue wait, CLI startup, first tool dispatch, provider generation, artifact availability, validation/storage, total terminal latency, failures, and retries. P50 and P95 are reported per workflow, duration, and resolution.

## Failure semantics

- Contract or CLI-capability errors return an xAI-shaped 4xx response before a job exists.
- A failure before verified remote submission is definite and may release the reservation.
- A lost or ambiguous receipt after tool dispatch is uncertain; the runner journal prevents a second provider submission.
- Artifact validation or storage failure does not claim provider failure and follows the existing uncertain/recovery path.
- A V2 failure never falls back to V1, direct REST, another model, a different duration, or a different resolution.

## CLI release and production rollout

The lock manifest is updated from 1.0.5 to 1.0.34 with official Linux x86_64 and aarch64 URLs, exact byte sizes, SHA-256 hashes, and the observed version output. Release scripts and readiness checks must require the new digest and V2 adapter revision.

Production does not run an in-place `grok update`. The verified binary is installed into a new immutable release, health-checked, then activated atomically. Rollback switches back to the previous application release and 1.0.5 binary/profile while preserving V1 replay.

The public video feature remains default-off until all gates pass:

1. source and contract tests;
2. both architecture artifacts verified against the lock manifest;
3. fake-process receipt and failure-path tests;
4. local real-CLI smoke for each newly bound workflow;
5. production migration/profile readiness and health checks;
6. authenticated production API smoke using a bounded low-cost case;
7. verified remote Git SHA, deployed release SHA, executable version/digest, and API response evidence.

No Blog UI or existing image-generation UI is changed by this work.

## Test matrix

| Layer | Required proof |
| --- | --- |
| Wire contract | Official JSON examples deserialize; aliases work; unknown fields fail; response shapes remain stable |
| Workflow selection | T2V, I2V, R2V, first+last frame, audio-only R2V, and combined references classify deterministically |
| Capability rejection | 1080p, silent audio, custom audio URL, >7 images, >3 voices, edit, extension, invalid duration/ratio, and ignored I2V ratio fail before side effects |
| Canonical command | V2 digest binds every media digest and voice; tampering is rejected; V1 fixtures still parse byte-for-byte |
| CLI policy | Exact tool allowlist, prompt, arguments, turn bound, isolated directories, and expected artifact path |
| Receipt | Wrong tool, missing/extra call, argument drift, duplicate artifact, non-MP4, oversize, timeout, cancellation, and ambiguous completion fail safely |
| Durable pipeline | Idempotent start, queue/replay, account fencing, billing reserve/settle/refund, status polling, and artifact publication |
| Performance | No per-request schema probe; one process per job; stage timings emitted; P50/P95 harness records workflow/duration/resolution |
| Regression | Existing OpenAI image calls, xAI image calls, media segmentation, V1 Grok jobs, admin UI typecheck/build, and workspace Rust tests |
| Production | Feature-off deploy, version/digest/readiness proof, feature-on canary, real API start/poll/result, rollback rehearsal |

## Delivery sequence

Each independently verifiable feature is committed and pushed before the next begins:

1. Contract V2 and fail-closed projection.
2. Grok CLI 1.0.34 request/policy/receipt binding with V1 replay.
3. Gateway admission/execution/pricing/readiness wiring.
4. OpenAPI, operator documentation, lock manifest, and release gates.
5. Local real-CLI and API smoke evidence.
6. Main merge, immutable production deployment, production CLI/API verification.

Every merge is a normal non-force merge from a current-main isolated worktree. Only task files are staged, existing features are retained, and production activation occurs only after the release identity and rollback target are proven.

## Explicit non-goals

- More than four Grok CLI `keyframes`, silently snapped keyframe timestamps, or presenting `keyframes` as an official xAI REST field.
- More than 7 reference images even though the CLI schema permits more.
- Video edit or extension.
- 1080p.
- Silent-video control.
- Arbitrary reference-audio URLs.
- A native xAI REST execution adapter for new V2 work.
- Blog changes, admin image-generation UI changes, or automatic per-view generation.
