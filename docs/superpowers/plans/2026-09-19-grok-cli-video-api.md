# Grok CLI 1.0.34 Video API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an xAI-shaped asynchronous video-generation API whose new jobs execute the public/CLI capability intersection through pinned Grok CLI 1.0.34 while preserving byte-stable V1 replay.

**Architecture:** Keep the public start/poll facade and reuse the durable admission, scheduler, runner, artifact, billing, and polling pipeline. Add independent V2 source/provider commands and strict 1.0.34 CLI policy/receipt validation; retain the V1 parser, direct I2V replay path, profile, and 1.0.5 binary. Package both immutable CLI versions and select them by execution profile without adding a service or SQL schema.

**Tech Stack:** Rust 2024, Axum, Tokio, Serde, Reqwest, SQLx/PostgreSQL, shell/Node release tooling, Next.js admin build verification.

**Spec:** `docs/superpowers/specs/2026-09-18-grok-cli-video-api-design.md`

## Global Constraints

- Public endpoints remain exactly `POST /v1/videos/generations` and `GET /v1/videos/{request_id}`.
- New admission emits `grok-cli.videos.generate.v2` and uses Grok CLI 1.0.34; no newly admitted V2 job may use the native xAI REST client.
- V1 command bytes, parser, adapter revision, direct I2V path, profile, and Grok CLI 1.0.5 artifact remain available for replay.
- Supported intersection: T2V by `image_gen` then `image_to_video`; I2V by `image_to_video`; R2V/first-last/preset voices by `reference_to_video`.
- Public maximums are 7 reference images and 3 preset voices; Grok CLI-only keyframes and references 8 through 14 are not exposed.
- 1080p, `generate_audio=false`, custom reference-audio URLs, edit, and extension fail before remote fetch, claim, reserve, queue, or provider execution.
- No SQL schema migration, new service, new queue, per-workflow executor, Blog change, or image-generation UI change.
- No per-request CLI capability probe or binary hashing; identities are checked at executor startup and release verification.
- Every production-code change follows red-green-refactor and preserves unrelated dirty work.
- Each task ends with a conventional commit and push to `origin/codex/grok-cli-video-api-1.0.34` after review.

---

### Task 1: Versioned xAI Video Wire Contract

**Files:**

- Modify: `crates/api-contracts/src/xai/videos.rs`
- Modify: `crates/api-contracts/src/xai/mod.rs`
- Modify mechanically for new optional request fields: every Rust file returned by `rg -l 'XaiVideoGenerationRequest \{' crates`

**Interfaces:**

- Produces: `XaiVideoAudioReference`, `XaiVideoGenerationCommandV2`, `XAI_VIDEO_GENERATION_COMMAND_SCHEMA_V2`, and V2 workflow classification.
- Preserves: `XaiVideoGenerationCommandV1`, `XAI_VIDEO_GENERATION_COMMAND_SCHEMA`, and every existing V1 serialized command shape.

- [ ] **Step 1: Add failing contract tests**

Add literal-driven tests to `crates/api-contracts/src/xai/videos.rs`:

```rust
#[test]
fn v2_classifies_last_frame_as_reference_video() {
    let request: XaiVideoGenerationRequest = serde_json::from_value(serde_json::json!({
        "model": "grok-imagine-video-1.5",
        "last_frame": {"url": "data:image/png;base64,AA=="}
    })).unwrap();
    let command = XaiVideoGenerationCommandV2::from_request(request).unwrap();
    assert_eq!(command.workflow(), XaiVideoWorkflow::ReferenceToVideo);
}

#[test]
fn v2_preserves_official_defaults_and_seconds_alias() {
    let request: XaiVideoGenerationRequest = serde_json::from_value(serde_json::json!({
        "model": "grok-imagine-video-1.5",
        "prompt": "moonlit lake",
        "seconds": "8"
    })).unwrap();
    let command = XaiVideoGenerationCommandV2::from_request(request).unwrap();
    assert_eq!(command.duration, 8);
    assert_eq!(command.resolution, XaiVideoResolution::P480);
}

#[test]
fn v2_rejects_more_than_seven_images() {
    let mut request = request_with_reference_images(8);
    assert_eq!(
        XaiVideoGenerationCommandV2::from_request(request.clone()),
        Err(XaiVideoRequestError::TooManyReferenceImages)
    );
    request.reference_images.truncate(7);
    assert!(XaiVideoGenerationCommandV2::from_request(request).is_ok());
}

#[test]
fn v2_rejects_ambiguous_audio_sources() {
    let request: XaiVideoGenerationRequest = serde_json::from_value(serde_json::json!({
        "model": "grok-imagine-video-1.5",
        "reference_audios": [{"url": "https://example.invalid/a.wav", "voice_id": "eve"}]
    })).unwrap();
    assert_eq!(
        XaiVideoGenerationCommandV2::from_request(request),
        Err(XaiVideoRequestError::InvalidReferenceAudio)
    );
}

#[test]
fn v1_golden_command_bytes_do_not_change() {
    let command = v1_text_command();
    assert_eq!(
        serde_json::to_vec(&command).unwrap(),
        br#"{"schema_version":1,"operation":"videos.generations","aspect_ratio":"16:9","duration":6,"image":null,"model":"grok-imagine-video-1.5-preview","output":null,"prompt":"moonlit lake","reference_images":[],"resolution":"480p","storage_options":null,"user":null}"#
    );
}
```

These tests catch accidental V1 reinterpretation and wrong official workflow/default/limit handling.

- [ ] **Step 2: Run the tests and verify RED**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-api-contracts xai::videos::tests -- --nocapture
```

Expected: compilation or assertion failure because V2 fields/types do not exist; the V1 golden test already passes.

- [ ] **Step 3: Add the V2 DTO and canonical source command**

Extend `XaiVideoGenerationRequest` with:

```rust
#[serde(default)]
pub generate_audio: Option<bool>,
#[serde(default)]
pub last_frame: Option<XaiVideoImageUrl>,
#[serde(default)]
pub reference_audios: Vec<XaiVideoAudioReference>,
```

Add:

```rust
pub const XAI_VIDEO_GENERATION_COMMAND_SCHEMA_V2: &str = "xai.videos.generations.v2";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoAudioReference {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub voice_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiVideoGenerationCommandV2 {
    pub schema_version: u16,
    pub operation: String,
    pub aspect_ratio: Option<XaiVideoAspectRatio>,
    pub duration: u8,
    pub generate_audio: bool,
    pub image: Option<XaiVideoImageUrl>,
    pub last_frame: Option<XaiVideoImageUrl>,
    pub model: Option<String>,
    pub output: Option<XaiVideoOutput>,
    pub prompt: Option<String>,
    pub reference_audios: Vec<XaiVideoAudioReference>,
    pub reference_images: Vec<XaiVideoImageUrl>,
    pub resolution: XaiVideoResolution,
    pub storage_options: Option<XaiVideoStorageOptions>,
    pub user: Option<String>,
}
```

`from_request` validates structural official rules only: each image/audio union has exactly one source, counts and scalar bounds hold, duration is 1..=15, official defaults are preserved, and prompt is required only when no image/frame/reference exists. Frames and references may be combined. It does not decide CLI support for 1080p, silent video, audio URL, or file IDs.

- [ ] **Step 4: Keep V1 construction explicit and byte-stable**

Make `XaiVideoGenerationCommandV1::from_request` reject nonempty V2-only fields rather than ignore them. Add `None`/empty values to all request literals; do not add a request `Default` implementation that could hide missing fields in tests.

- [ ] **Step 5: Run contract and compile checks**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-api-contracts xai::videos::tests
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli --no-run
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway --no-run
```

Expected: all commands exit 0 and V1 golden bytes remain unchanged.

- [ ] **Step 6: Commit and push**

```bash
git add crates/api-contracts/src/xai crates/provider-grok-cli/src crates/image-gateway/src crates/image-gateway/tests
git commit -m "feat: add xai video generation v2 contract"
git push
```

### Task 2: Canonical Grok Video V2 Projection

**Files:**

- Create: `crates/provider-grok-cli/src/video_v2.rs`
- Create: `crates/provider-grok-cli/tests/fixtures/grok-video-v1-reference.json`
- Modify: `crates/provider-grok-cli/src/lib.rs`
- Modify: `crates/provider-grok-cli/src/capabilities.rs`
- Test: `crates/provider-grok-cli/src/video_v2.rs`

**Interfaces:**

- Consumes: `XaiVideoGenerationCommandV2` and `StagedImageV1`.
- Produces: `GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2`, `VIDEO_ADAPTER_REVISION_V2`, `GrokVideoGenerationInputsV2`, `GrokVideoGenerationRequestV2`, `GrokVideoGenerationPayloadV2`, `parse_video_generation_payload_v2`, and `parse_video_generation_command_v2`.

- [ ] **Step 1: Add failing projection and tamper tests**

```rust
#[test]
fn v2_projects_first_last_references_and_voices_in_semantic_order() {
    let payload = project_reference_fixture();
    let request = payload.request().as_reference().unwrap();
    assert_eq!(request.first_frame().unwrap().filename(), "first.png");
    assert_eq!(request.last_frame().unwrap().filename(), "last.png");
    assert_eq!(request.reference_images()[0].filename(), "reference-0.png");
    assert_eq!(request.voices(), &["eve", "leo"]);
}

#[test]
fn v2_accepts_reference_duration_one_through_fifteen() {
    for seconds in [1, 8, 15] {
        assert_eq!(ReferenceVideoDurationV2::new(seconds).unwrap().seconds(), seconds);
    }
    assert!(ReferenceVideoDurationV2::new(0).is_err());
    assert!(ReferenceVideoDurationV2::new(16).is_err());
}

#[test]
fn v2_rejects_silent_video_before_inputs() {
    let command = source_command_with_generate_audio(false);
    assert_eq!(
        GrokVideoGenerationPayloadV2::preflight(&command),
        Err(XaiGrokVideoProjectionErrorV2::UnsupportedGenerateAudio)
    );
}

#[test]
fn v2_i2v_accepts_only_six_or_ten_seconds() {
    assert!(project_i2v_duration(6).is_ok());
    assert_eq!(
        project_i2v_duration(8),
        Err(XaiGrokVideoProjectionErrorV2::UnsupportedDuration)
    );
    assert!(project_i2v_duration(10).is_ok());
}

#[test]
fn v2_canonical_command_rejects_role_tampering() {
    let bytes = project_reference_fixture().into_canonical_bytes(output_slot());
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["inputs"][0]["role"] = serde_json::json!("last_frame");
    assert_eq!(
        parse_video_generation_payload_v2(&serde_json::to_vec(&value).unwrap()),
        Err(GrokCommandError::IntegrityMismatch)
    );
}

#[test]
fn legacy_v1_payload_fixture_still_parses_identically() {
    let bytes = include_bytes!("../tests/fixtures/grok-video-v1-reference.json");
    let parsed = parse_video_generation_payload(bytes).unwrap();
    assert_eq!(parsed.request().duration().seconds(), 6);
}
```

These tests catch provider commands that silently drop, reorder, or change a public semantic field.

Before changing the V1 serializer, create the fixture by copying the current canonical reference-video payload bytes into the new file and record its SHA-256 in the test. The test must compare both the literal digest and the parsed V1 request; it must not regenerate the expected fixture through the code under test.

- [ ] **Step 2: Run provider tests and verify RED**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli video_v2 -- --nocapture
```

Expected: failure because the V2 module and exports are absent.

- [ ] **Step 3: Implement bounded V2 provider values**

Use these identities and shapes:

```rust
pub const GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2: &str = "grok-cli.videos.generate.v2";
pub const VIDEO_ADAPTER_REVISION_V2: &str = "grok-cli-1.0.34.agentic-video.v1";

pub struct GrokVideoGenerationInputsV2 {
    first_frame: Option<StagedImageV1>,
    last_frame: Option<StagedImageV1>,
    reference_images: Vec<StagedImageV1>,
}

pub enum GrokVideoGenerationRequestV2 {
    TextToVideo(TextToVideoRequestV2),
    ImageToVideo(ImageToVideoRequestV2),
    ReferenceToVideo(ReferenceToVideoRequestV2),
}
```

Canonical input order is first frame, last frame, then reference images. Serialize each semantic role beside filename/digest. Normalize provider voice IDs to lowercase after preserving trimmed original spelling in the V2 source command.

- [ ] **Step 4: Implement pure fail-closed projection**

Reject before staged bytes are required:

```rust
match (command.generate_audio, command.resolution) {
    (false, _) => Err(UnsupportedGenerateAudio),
    (_, XaiVideoResolution::P1080) => Err(UnsupportedResolution),
    _ => Ok(()),
}
```

Reject audio `url`, all `file_id`, non-1.5 models, explicit I2V ratio, T2V/I2V durations other than 6/10, and any staged manifest mismatch. Accept R2V 1..=15, seven official ratios, up to seven reference images, up to three preset voice IDs, and omitted R2V prompt as `None` for the policy layer.

- [ ] **Step 5: Serialize and parse V2 independently**

Canonical JSON includes schema version 2, operation, adapter revision, source-command hash, workflow tag, controls, original and normalized voices, and each staged role/filename/digest. Parsing recomputes every derivable field and never calls V1 normalization.

- [ ] **Step 6: Run provider regression tests**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli
```

Expected: V2 tests and all existing V1 tests pass.

- [ ] **Step 7: Commit and push**

```bash
git add crates/provider-grok-cli/src
git commit -m "feat: add grok cli video v2 projection"
git push
```

### Task 3: Strict Grok CLI 1.0.34 Policy and Receipt

**Files:**

- Create: `providers/grok-cli-1.0.34-video-capabilities.json`
- Modify: `crates/provider-grok-cli/src/policy.rs`
- Modify: `crates/provider-grok-cli/src/receipt.rs`
- Modify: `crates/provider-grok-cli/src/lib.rs`
- Test: `crates/provider-grok-cli/src/tests.rs`

**Interfaces:**

- Consumes: `GrokVideoGenerationRequestV2`.
- Produces: a V2 CLI request variant using the existing isolated process policy and a strict receipt argument policy that leaves V1 behavior unchanged.

- [ ] **Step 1: Add failing exact-call tests**

```rust
#[test]
fn v2_i2v_dispatches_one_exact_image_to_video_call() {
    let invocation = v2_i2v_invocation("first.png", "camera pan", 6, "480p");
    assert_eq!(invocation.expected_tool_calls().len(), 1);
    assert_eq!(invocation.expected_tool_calls()[0].tool(), GrokTool::ImageToVideo);
    assert_eq!(invocation.expected_tool_calls()[0].arguments(), &json!({
        "prompt": "camera pan",
        "image": invocation.workspace().join("first.png"),
        "duration": 6,
        "resolution_name": "480p"
    }));
}

#[test]
fn v2_reference_dispatch_maps_frames_references_and_voice_strings() {
    let invocation = v2_reference_invocation();
    assert_eq!(invocation.expected_tool_calls()[0].arguments(), &json!({
        "prompt": "",
        "images": [invocation.workspace().join("reference-0.png")],
        "first_frame": invocation.workspace().join("first.png"),
        "last_frame": invocation.workspace().join("last.png"),
        "voices": ["eve", "leo"],
        "aspect_ratio": "16:9",
        "duration": 8,
        "resolution_name": "720p"
    }));
}

#[test]
fn v2_text_video_dispatches_image_gen_then_image_to_video() {
    let invocation = v2_text_invocation();
    assert_eq!(
        invocation.expected_tool_calls().iter().map(|call| call.tool()).collect::<Vec<_>>(),
        vec![GrokTool::ImageGeneration, GrokTool::ImageToVideo]
    );
    assert_eq!(
        invocation.expected_tool_calls()[1].arguments()["image"],
        json!(invocation.expected_tool_calls()[0].artifact_path())
    );
}

#[test]
fn v2_receipt_rejects_omitted_default_and_extra_arguments() {
    let invocation = v2_i2v_invocation("first.png", "camera pan", 6, "480p");
    let actual = json!({
        "prompt": "camera pan",
        "image": invocation.workspace().join("first.png"),
        "duration": 6,
        "unexpected": true
    });
    assert_eq!(
        validate_recorded_arguments(&invocation, actual),
        Err(GrokReceiptError::ToolArgumentsMismatch)
    );
}

#[test]
fn v2_receipt_rejects_wrong_tool_order_and_duplicate_results() {
    let invocation = v2_text_invocation();
    assert_eq!(
        parse_invocation_receipt(&reversed_two_call_stdout(&invocation), &valid_history(&invocation), &invocation),
        Err(GrokReceiptError::ToolCallSequenceMismatch)
    );
    assert_eq!(
        parse_invocation_receipt(&duplicate_terminal_stdout(&invocation), &valid_history(&invocation), &invocation),
        Err(GrokReceiptError::DuplicateTerminalResult)
    );
}

#[test]
fn v1_receipt_keeps_legacy_numeric_and_default_equivalence() {
    let invocation = v1_i2v_invocation_480p_6s();
    assert!(parse_invocation_receipt(
        &v1_stdout_with_omitted_defaults(&invocation),
        &valid_history(&invocation),
        &invocation
    ).is_ok());
}
```

These tests catch agent normalization or receipt permissiveness changing an admitted request.

Create the capability fixture from the locally registered 1.0.34 definitions for only `image_gen`, `image_to_video`, and `reference_to_video`; canonicalize key order, secret-scan it, and record its SHA-256 in the provider test. Production argument names and required/optional status must match this checked-in fixture, not prose or a newer installed binary.

- [ ] **Step 2: Run tests and verify RED**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli v2_ -- --nocapture
```

Expected: V2 dispatch/receipt assertions fail before implementation.

- [ ] **Step 3: Add an explicit argument policy**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GrokToolArgumentPolicy {
    Exact,
    LegacyVideoDefaults,
    ImagePromptMayNormalize,
}
```

Set `Exact` for every V2 call, retain current policies for V1/image calls, and make receipt validation select only from this frozen field.

- [ ] **Step 4: Emit exact registered 1.0.34 arguments**

```rust
// image_to_video
json!({
    "prompt": request.prompt(),
    "image": workspace.join(request.image().filename()),
    "duration": request.duration(),
    "resolution_name": request.resolution().as_str(),
})

// reference_to_video
json!({
    "prompt": request.prompt().unwrap_or(""),
    "images": absolute_image_paths(workspace, request.reference_images()),
    "first_frame": optional_absolute_path(workspace, request.first_frame()),
    "last_frame": optional_absolute_path(workspace, request.last_frame()),
    "voices": request.voices(),
    "aspect_ratio": request.aspect_ratio().as_str(),
    "duration": request.duration(),
    "resolution_name": request.resolution().as_str(),
})
```

The CLI schema requires a prompt string but has no minimum length. Map an omitted official R2V prompt to `""`; never synthesize text. Task 7 live smoke is the acceptance gate. Do not send `keyframes`.

- [ ] **Step 5: Reuse the isolated process envelope**

Keep private `HOME`, `GROK_HOME`, workspace/session, `--no-memory`, `--no-plan`, `--no-subagents`, `--disable-web-search`, exact tool allowlist, bounded turns, streaming JSON, and expected artifact path. One V2 job starts one CLI process; only T2V issues two calls.

- [ ] **Step 6: Run provider tests**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli
```

Expected: all V1 and V2 policy/receipt tests pass.

- [ ] **Step 7: Commit and push**

```bash
git add crates/provider-grok-cli/src
git commit -m "feat: bind grok cli 1.0.34 video tools"
git push
```

### Task 4: V2 API Admission and Bounded Image Inputs

**Files:**

- Create: `crates/image-gateway/src/api/video_inputs.rs`
- Modify: `crates/image-gateway/src/api/mod.rs`
- Modify: `crates/image-gateway/src/api/videos.rs`
- Modify: `crates/image-gateway/src/admission/xai_videos.rs`
- Modify: `crates/image-gateway/src/admission/mod.rs`
- Test: module tests in those files

**Interfaces:**

- Consumes: V2 source/provider payload from Tasks 1-2.
- Produces: pure preflight before I/O, deterministic roles/order, bounded data-URI/HTTPS loading, sealed V2 admission, and zero-image voice-only R2V.

- [ ] **Step 1: Add failing no-side-effect and manifest tests**

```rust
#[tokio::test]
async fn unsupported_v2_controls_fail_before_input_fetch_and_claim() {
    let harness = VideoAdmissionHarness::new();
    let response = harness.post(json!({
        "model": "grok-imagine-video-1.5",
        "prompt": "wind in grass",
        "image": {"url": "https://public.example/input.png"},
        "generate_audio": false
    })).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(harness.fetch_calls(), 0);
    assert_eq!(harness.claim_calls(), 0);
    assert_eq!(harness.reserve_calls(), 0);
}

#[test]
fn v2_manifest_binds_first_last_and_reference_roles() {
    let plan = v2_first_last_reference_plan();
    assert_eq!(
        plan.inputs().iter().map(|input| (input.role(), input.role_index())).collect::<Vec<_>>(),
        vec![(FirstFrame, 0), (LastFrame, 0), (ReferenceImage, 0)]
    );
    assert_eq!(plan.input_manifest_json(), expected_first_last_reference_manifest_json());
    assert_eq!(plan.input_manifest_sha256(), sha256_hex(expected_first_last_reference_manifest_json()));
}

#[test]
fn voice_only_v2_reference_video_has_no_input_manifest() {
    let plan = v2_voice_only_plan("eve");
    assert!(plan.inputs().is_empty());
    assert!(validate_video_attach_request(&plan.attach_request()).is_ok());
}

#[test]
fn first_last_plus_seven_references_is_not_counted_as_nine_references() {
    let plan = v2_plan_with_frames_and_references(7);
    assert_eq!(plan.reference_image_count(), 7);
    assert_eq!(plan.inputs().len(), 9);
    assert!(validate_video_attach_request(&plan.attach_request()).is_ok());
}

#[test]
fn changing_a_v2_input_role_breaks_manifest_validation() {
    let mut request = v2_first_last_reference_plan().attach_request();
    request.inputs[0].semantic_role = "last_frame".to_owned();
    assert_eq!(
        validate_video_attach_request(&request),
        Err(AdmissionAttachError::InputManifestMismatch)
    );
}
```

These tests catch remote I/O before capability rejection and media-role swaps surviving durable validation.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway admission::xai_videos::tests -- --nocapture
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway api::videos::tests -- --nocapture
```

Expected: failures because admission still emits V1 and cannot bind new modes.

- [ ] **Step 3: Add pure V2 preflight before decoding**

`XaiVideoAdmissionIntent::new` normalizes with `XaiVideoGenerationCommandV2`; `preflight_grok_binding` calls pure V2 capability projection using source fields only. `create_video` performs this before input decoding, claim, reserve, or stores.

- [ ] **Step 4: Add deterministic semantic input descriptors**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XaiVideoInputRoleV2 {
    FirstFrame,
    LastFrame,
    ReferenceImage,
}

pub struct XaiVideoAdmissionInputV2 {
    role: XaiVideoInputRoleV2,
    role_index: u8,
    filename: String,
    blob: InputBlobRef,
    media_type: String,
}
```

Flatten first frame, last frame, then references. Serialize role/index into the V2 manifest hash while retaining generic `EditInputRoleV1::Image` in existing SQL rows.

- [ ] **Step 5: Add bounded data-URI and HTTPS loading**

`video_inputs.rs` accepts only `data:image/...;base64,...` or public HTTPS URLs. HTTPS policy is exact:

```rust
const MAX_VIDEO_INPUT_BYTES: usize = 32 * 1024 * 1024;

// Reject credentials, fragments, non-HTTPS schemes, non-default ports,
// redirects, and every non-public resolved IP. Pin the approved resolution,
// bound Content-Length and streamed bytes, validate PNG/JPEG/WebP signature,
// and enforce the request-wide upload limit.
```

Cache decoded results by exact source string during one request so the same source is fetched and decoded once. Do not add a background cache or service.

- [ ] **Step 6: Bind V2 admission**

`XaiVideoAdmissionPlan` emits `GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2` and `VIDEO_ADAPTER_REVISION_V2`, uses model `grok-imagine-video-1.5`, supports zero-image voice-only R2V, and preserves output/billing/schedule behavior. V1 validation remains a separate match arm.

- [ ] **Step 7: Run focused and compile tests**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway admission::xai_videos::tests
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway api::videos::tests
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway --no-run
```

Expected: all commands exit 0.

- [ ] **Step 8: Commit and push**

```bash
git add crates/image-gateway/src/api crates/image-gateway/src/admission
git commit -m "feat: admit xai video v2 requests"
git push
```

### Task 5: Durable V2 Execution, Pricing, and Replay Isolation

**Files:**

- Modify: `crates/image-gateway/src/executor/grok_request.rs`
- Modify: `crates/image-gateway/src/executor/grok_supervisor.rs`
- Modify: `crates/image-gateway/src/executor/profile_binding.rs`
- Modify: `crates/image-gateway/src/executor/provisioning.rs`
- Modify: `crates/image-gateway/src/bin/executord.rs`
- Modify: `crates/image-gateway/src/bin/factoryctl.rs`
- Modify: `crates/image-gateway/src/provider_management/route_reconciliation.rs`
- Modify: `crates/image-gateway/src/workers/daemon.rs`
- Modify: `crates/image-gateway/src/pricing/admission.rs`
- Modify: `crates/image-gateway/src/reduction/postgres/completion.rs`
- Modify: `crates/image-gateway/tests/grok_process_smoke.rs`
- Modify: `crates/image-gateway/tests/postgres_video_api.rs`
- Modify: `crates/image-gateway/tests/postgres_provisioning.rs`
- Create: `crates/image-gateway/migrations/0132_grok_video_v2_bindings.sql`

**Interfaces:**

- Consumes: V2 schema/parser/adapter/CLI request.
- Produces: additive V2 profile/route, schema-aware dispatch, V1-only direct replay, V2 pricing/reduction, and readiness.

- [ ] **Step 1: Add failing schema-isolation tests**

```rust
#[test]
fn v2_profile_requires_v2_schema_adapter_and_operation_descriptor() {
    let mut profile = grok_video_v2_profile();
    assert_eq!(identify_executor_profile_binding(&profile), Ok(ExecutorProfileBinding::GrokVideoGenerationV2));
    profile.adapter_revision = VIDEO_ADAPTER_REVISION.to_owned();
    assert_eq!(identify_executor_profile_binding(&profile), Err(ExecutorProfileBindingError::BindingMismatch));
}

#[tokio::test]
async fn v2_i2v_spawns_cli_and_never_calls_direct_video_http() {
    let harness = GrokProcessHarness::for_v2_i2v();
    let result = harness.run().await.unwrap();
    assert_eq!(harness.cli_launches(), 1);
    assert_eq!(harness.direct_http_requests(), 0);
    assert_eq!(result.media_type(), "video/mp4");
}

#[test]
fn v1_i2v_replay_retains_the_direct_executor_path() {
    let request = project_grok_execution_request(&v1_i2v_lease(), &v1_i2v_context()).unwrap();
    assert!(matches!(request, GrokExecutionRequest::VideoGenerationV1(GrokVideoGenerationRequestV1::ImageToVideo(_))));
}

#[test]
fn v2_pricing_uses_frozen_duration_resolution_and_input_count() {
    let facts = pricing_facts(&v2_first_last_reference_command()).unwrap();
    assert_eq!(facts.provider_model_id, "grok-imagine-video-1.5");
    assert_eq!(facts.requested_seconds, Decimal::from(8));
    assert_eq!(facts.resolution, "720p");
    assert_eq!(facts.input_image_count, 3);
}

#[test]
fn v2_completion_accepts_only_v2_model_and_duration_evidence() {
    let command = v2_reference_command_8s();
    assert!(validate_video_completion(&command, &completion("grok-imagine-video-1.5", 8)).is_ok());
    assert!(validate_video_completion(&command, &completion("grok-imagine-video", 8)).is_err());
    assert!(validate_video_completion(&command, &completion("grok-imagine-video-1.5", 6)).is_err());
}
```

These tests catch V2 execution through V1/direct REST and consumer rejection or misrating.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway executor::profile_binding::tests -- --nocapture
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway pricing::admission::tests -- --nocapture
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway reduction::postgres::completion::tests -- --nocapture
```

Expected: V2 profile/schema tests fail before implementation.

- [ ] **Step 3: Add V2 request and profile dispatch**

Add `GrokExecutionRequest::VideoGenerationV2`. Extend each schema match in the Files list. Use a distinct operation descriptor revision/hash and V2 adapter while retaining operation ID `videos.generations`. Crossed identities fail before spawn.

- [ ] **Step 4: Gate the direct path by V1 variant**

Retain `generate_image_to_video` only for:

```rust
GrokExecutionRequest::VideoGenerationV1(
    GrokVideoGenerationRequestV1::ImageToVideo(request)
)
```

`GrokExecutionRequest::VideoGenerationV2(_)` always uses CLI policy, receipt validation, and MP4 publication.

- [ ] **Step 5: Bind executable identity at executor startup**

Add a V2 binding and a provider-crate function returning approved current-target SHA/version for V1 or V2. `executord` passes expected SHA into `GrokProcessSupervisor::new`, which compares the actual binary hash once. Separate profile instances select immutable `bin/grok-v1` or `bin/grok-v2` via `EXECUTOR_PROVIDER_EXECUTABLE`.

- [ ] **Step 6: Extend durable consumers**

Add explicit V2 arms for admission validation, worker support, pricing facts, route reconciliation, provisioning, factory control, terminal reduction, and readiness. Keep metric `VideoSecond`, operation `video_generation`, output count 1, and current polling response. Reference count excludes first/last; total staged input count includes them. Add forward-only data migration `0132_grok_video_v2_bindings.sql` for V2 pricing/profile/route rows; it changes data only and performs no DDL. Do not edit the V1 migration or rows.

- [ ] **Step 7: Run unit and fake-process tests**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway --test grok_process_smoke -- --nocapture
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway executor::
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway pricing::
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway reduction::postgres::completion::tests
```

Expected: exit 0; fake V2 I2V proves a CLI child and zero direct HTTP submissions.

- [ ] **Step 8: Run configured PostgreSQL tests**

```bash
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway --test postgres_video_api -- --nocapture --test-threads=1
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway --test postgres_provisioning -- --nocapture --test-threads=1
```

Expected: tests execute rather than skip and pass. Database unavailability is recorded as unverified, never a pass.

- [ ] **Step 9: Commit and push**

```bash
git add crates/image-gateway
git commit -m "feat: execute grok video v2 durably"
git push
```

### Task 6: Dual CLI Release Lock, OpenAPI, and Operations

**Files:**

- Create: `providers/grok-cli-v1.lock.json` as an exact copy of current 1.0.5 lock
- Modify: `providers/grok-cli.lock.json` to verified 1.0.34
- Modify: `scripts/fetch-grok-cli.sh`
- Modify: `scripts/package-release.sh`
- Modify: `scripts/test-gateway-runtime-gate.sh`
- Modify: `deploy/hooks/verify-gateway-runtime`
- Modify: `deploy/systemd/app.env.example`
- Modify: `crates/image-gateway/src/docs/mod.rs`
- Modify: `docs/architecture/2026-grok-cli-xai-media-binding.md`
- Modify: `docs/operations/production-release.md`
- Modify: `docs/README.zh-CN.md`

**Interfaces:**

- Produces: immutable `bin/grok-v1`, `bin/grok-v2`, lock-verified manifest identities, V2 OpenAPI, and rollback instructions.

- [ ] **Step 1: Add failing release-gate tests**

Extend `scripts/test-gateway-runtime-gate.sh` to prove matching V1/V2 pass and crossed revision, missing V1, or wrong digest fail.

```bash
bash scripts/test-gateway-runtime-gate.sh
```

Expected: new assertions fail before dual-lock support.

- [ ] **Step 2: Fetch and verify official 1.0.34 artifacts**

Download both official Linux artifacts into a temporary directory. Record exact bytes/SHA-256, validate ELF machine 62/183, and verify `grok 1.0.34 (3736acbc8658)` on a runnable matching architecture. Do not commit binaries.

- [ ] **Step 3: Preserve V1 and update the primary lock**

Copy current lock verbatim to `providers/grok-cli-v1.lock.json`. Update primary lock to 1.0.34 with official URLs, exact sizes/hashes, compatibility `grok-cli-1.0.34`, and the V2 video adapter. Keep V1 image adapter only in the V1 lock; do not claim image binding upgrade.

- [ ] **Step 4: Package and verify both binaries**

`scripts/package-release.sh` accepts two explicit sources, validates each lock, installs `bin/grok-v1`/`bin/grok-v2`, and writes both identities in the release manifest. Hooks verify files, sizes, hashes, architecture, version output, and adapter pairing before activation.

- [ ] **Step 5: Update OpenAPI and operator documentation**

Document public V2 fields and accepted/rejected intersection. Keep edit/extension absent. Document separate profile keys/executables, default-off activation, V1 pending/replay checks, disable-and-drain rollback, and the requirement that rollback still understands V2 until V2 jobs drain.

- [ ] **Step 6: Run release and docs checks**

```bash
bash scripts/test-gateway-runtime-gate.sh
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p image-provider-grok-cli pinned_lock
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test -p gpt-image-2-gateway docs::
npm run typecheck:admin
npm run build:admin
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit and push**

```bash
git add providers scripts deploy crates/image-gateway/src/docs docs
git commit -m "build: pin grok cli 1.0.34 video runtime"
git push
```

### Task 7: Verification, Real Smoke, Merge, and Production

**Files:**

- Modify: `crates/image-gateway/src/executor/grok_supervisor/live_smoke.rs`
- Modify: `crates/image-gateway/tests/grok_process_smoke.rs`
- Create: `scripts/benchmark-grok-video.sh`
- Create: `docs/operations/grok-video-v2-verification-20260919.md`

**Interfaces:**

- Produces: repeatable performance/evidence harness, verified identities/API receipts, main merge, and production acceptance.

- [ ] **Step 1: Add opt-in real-CLI smoke coverage**

Add ignored tests for T2V, I2V, R2V references, first+last, and preset voice. Record workflow, duration, resolution, CLI startup, first dispatch, provider completion, artifact availability, validation/store, total time, retry count, MIME, and MP4 size. Logs contain hashes/IDs only.

- [ ] **Step 2: Add benchmark harness**

`scripts/benchmark-grok-video.sh` accepts a bounded case manifest, runs serially by default, emits JSONL receipts, computes P50/P95 per workflow/duration/resolution, and has a no-credit dry-run. It never probes capabilities per request.

- [ ] **Step 3: Run repository gates**

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo clippy --locked --workspace --all-targets -- -D warnings
CARGO_TARGET_DIR=/private/tmp/aif-grok-video-target cargo test --locked --workspace --all-targets -- --test-threads=1
npm run typecheck:admin
npm run build:admin
bash scripts/test-gateway-runtime-gate.sh
```

Record baseline-only failures separately with exact test names; do not relabel them as passes.

- [ ] **Step 4: Run local CLI and API smoke**

Use pinned 1.0.34 and a managed test account. Begin with one 6-second 480p I2V, then one case for each new R2V control. Verify one CLI process/job, exact receipt, one MP4, polling, download, signature, and timings. If empty-prompt R2V fails live, add a failing regression test and reject omitted R2V prompt; never synthesize prompt text.

- [ ] **Step 5: Final review and evidence commit**

Write the verification document with source SHA, test results, artifact identities, redacted API receipt, timing table, and unverified boundaries.

```bash
git add crates/image-gateway/src/executor/grok_supervisor/live_smoke.rs crates/image-gateway/tests/grok_process_smoke.rs scripts/benchmark-grok-video.sh docs/operations/grok-video-v2-verification-20260919.md
git commit -m "test: verify grok cli video v2 pipeline"
git push
```

- [ ] **Step 6: Merge current remote main safely**

Fetch, prove the isolated branch clean, review complete diff and secret scan, normal-merge against current remote main, rerun required gates, push main, and prove remote ancestry/SHA. Never reset, force-push, or use the dirty original checkout.

- [ ] **Step 7: Deploy immutable production release**

Build from proven remote main with both binaries. Install a new release with the feature off, pass hooks/readiness/health, verify V1/V2 profiles and executable digests, then enable only V2 admission. Never upgrade a shared binary in place.

- [ ] **Step 8: Production canary and rollback proof**

Run one authenticated bounded 6-second 480p request through start/poll/download; verify release SHA, CLI 1.0.34 digest, V2 schema/adapter, one job/artifact/charge, image endpoint regression, and timings. Roll back by disabling new V2 admission while retaining compatible V2 executor/reducer until all V2 jobs drain.

---

## Plan Self-Review

- Spec coverage: Tasks 1-7 cover contract, projection, CLI/receipt, inputs/admission, durable consumers, replay, dual binaries, OpenAPI/docs, performance, merge, and production.
- Type consistency: `XaiVideoGenerationCommandV2` projects to `GrokVideoGenerationPayloadV2`, stored under `GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2`, executed as `GrokExecutionRequest::VideoGenerationV2`, and bound by `VIDEO_ADAPTER_REVISION_V2`.
- Compatibility: V1 constants/bytes remain frozen; binary/profile selection makes replay independent of the latest installed CLI.
- Scope: no keyframes, edit, extension, 1080p, silent video, custom audio, new SQL, new service, or UI work.
