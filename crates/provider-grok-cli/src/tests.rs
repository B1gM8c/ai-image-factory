use std::{ffi::OsStr, fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};

use image_api_contracts::xai::{
    XAI_IMAGE_GENERATION_COMMAND_SCHEMA, XaiImageAspectRatio, XaiImageGenerationCommandV1,
    XaiImageGenerationRequest, XaiImageResolution, XaiImageResponseFormat,
    XaiVideoAspectRatio as OfficialVideoAspectRatio, XaiVideoGenerationCommandV1,
    XaiVideoGenerationRequest, XaiVideoImageUrl, XaiVideoResolution as OfficialVideoResolution,
};
use image_cli_runtime::WorkingDirectory;
use image_provider_contracts::{
    ArtifactDelivery, BillingMetric, CompletionMode, OfficialParamsKind,
};
use image_provider_sdk::{OutputSlot, SingleOutputCommand};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::*;

const SESSION_ID: &str = "019f6ded-4ffe-73f3-80e1-d1f11287bd96";
const SOURCE_SHA256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const IMAGE_SHA256: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

#[test]
fn image_edit_descriptor_hash_is_stable() {
    assert_eq!(
        GROK_IMAGE_EDIT_OPERATION_V1.canonical_sha256_v1_hex(),
        "8b4e7bf805b5cc45bdd2316c72b4915f43e885fc908131a7bc8d920302c18db7"
    );
}

#[test]
fn capability_descriptors_expose_only_the_exact_cli_completion_model() {
    assert_eq!(GROK_IMAGE_GENERATION_OPERATION_V1.id, "images.generations");
    assert_eq!(GROK_IMAGE_EDIT_OPERATION_V1.id, "images.edits");
    assert_eq!(GROK_VIDEO_GENERATION_OPERATION_V1.id, "videos.generations");
    assert_eq!(
        GROK_IMAGE_GENERATION_OPERATION_V1.completion,
        CompletionMode::Inline
    );
    assert_eq!(
        GROK_VIDEO_GENERATION_OPERATION_V1.completion,
        CompletionMode::Inline
    );
    assert_eq!(
        GROK_IMAGE_GENERATION_OPERATION_V1.official_params.kind,
        OfficialParamsKind::XaiImage
    );
    assert_eq!(
        GROK_IMAGE_GENERATION_OPERATION_V1.official_params.schema_id,
        XAI_IMAGE_GENERATION_COMMAND_SCHEMA
    );
    assert_eq!(
        GROK_VIDEO_GENERATION_OPERATION_V1.official_params.kind,
        OfficialParamsKind::XaiVideo
    );
    assert_eq!(
        GROK_VIDEO_GENERATION_OPERATION_V1.billing_metric,
        BillingMetric::VideoSecond
    );
    assert_eq!(
        GROK_VIDEO_GENERATION_OPERATION_V1.artifact_delivery,
        ArtifactDelivery::InlineBounded {
            max_bytes: 256 * 1024 * 1024
        }
    );
    assert_eq!(
        GROK_VIDEO_GENERATION_OPERATION_V1
            .canonical_sha256_v1_hex()
            .len(),
        64
    );
}

#[test]
fn requests_reject_prompt_reference_and_staging_boundary_violations() {
    assert_eq!(
        GrokImageGenerationRequestV1::new(" ", ImageModel::Base, ImageAspectRatio::Auto),
        Err(RequestValidationError::EmptyPrompt)
    );
    assert_eq!(
        GrokImageGenerationRequestV1::new(
            "x".repeat(1_025),
            ImageModel::Base,
            ImageAspectRatio::Auto,
        ),
        Err(RequestValidationError::PromptTooLong)
    );
    assert_eq!(
        StagedImageV1::new("../secret.png", IMAGE_SHA256),
        Err(RequestValidationError::InvalidStagedFilename)
    );
    assert_eq!(
        StagedImageV1::new("input.png", "ABC"),
        Err(RequestValidationError::InvalidStagedSha256)
    );

    let duplicate = staged("same.png");
    assert_eq!(
        ReferenceToVideoRequestV1::new(
            "move",
            vec![duplicate.clone(), duplicate],
            VideoAspectRatio::R16x9,
            VideoDuration::Seconds6,
            VideoResolution::P480,
        ),
        Err(RequestValidationError::DuplicateReferenceFilename)
    );

    assert_eq!(
        GrokImageEditRequestV1::new("edit", vec![staged("single.png")], ImageAspectRatio::R16x9,),
        Err(RequestValidationError::SingleImageAspectRatioUnsupported)
    );

    assert!(
        GrokImageEditRequestV1::new(
            "combine",
            vec![staged("one.png"), staged("two.png"), staged("three.png")],
            ImageAspectRatio::R16x9,
        )
        .is_ok()
    );
    assert_eq!(
        GrokImageEditRequestV1::new(
            "combine",
            vec![
                staged("one.png"),
                staged("two.png"),
                staged("three.png"),
                staged("four.png"),
            ],
            ImageAspectRatio::R16x9,
        ),
        Err(RequestValidationError::InvalidReferenceCount)
    );
}

#[test]
fn canonical_commands_are_strict_and_round_trip_the_cli_subset() {
    let source = XaiImageGenerationCommandV1::from_request(XaiImageGenerationRequest {
        aspect_ratio: Some(XaiImageAspectRatio::R16x9),
        model: Some("grok-imagine-image-quality".to_owned()),
        n: Some(1),
        prompt: "a red fox".to_owned(),
        resolution: Some(XaiImageResolution::R1k),
        response_format: Some(XaiImageResponseFormat::B64Json),
        storage_options: None,
        user: None,
    })
    .unwrap();
    let source_sha256 = source.canonical_sha256_hex();
    let image = GrokImageGenerationRequestV1::new(
        "a red fox",
        ImageModel::Quality,
        ImageAspectRatio::R16x9,
    )
    .unwrap();
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).unwrap(),
        GrokImageGenerationPayloadV1::from_xai_command(source.clone()).unwrap(),
    )
    .unwrap();
    assert_eq!(command.schema_id(), GROK_IMAGE_GENERATION_COMMAND_SCHEMA);
    assert_eq!(
        parse_image_generation_payload(command.canonical_payload())
            .unwrap()
            .source_command_sha256(),
        source_sha256
    );
    assert_eq!(
        parse_image_generation_command(command.canonical_payload()).unwrap(),
        image
    );

    let mut unsupported_source = source.clone();
    unsupported_source.resolution = XaiImageResolution::R2k;
    assert_eq!(
        GrokImageGenerationPayloadV1::from_xai_command(unsupported_source),
        Err(GrokCommandError::SourceProjection(
            XaiGrokProjectionError::UnsupportedResolution
        ))
    );

    let mut unsupported: serde_json::Value =
        serde_json::from_slice(command.canonical_payload()).unwrap();
    unsupported["resolution"] = "2k".into();
    assert_eq!(
        parse_image_generation_command(&serde_json::to_vec(&unsupported).unwrap()),
        Err(GrokCommandError::UnsupportedCliOption)
    );
    unsupported["resolution"] = "1k".into();
    unsupported["source_command_sha256"] = "not-a-sha256".into();
    assert_eq!(
        parse_image_generation_payload(&serde_json::to_vec(&unsupported).unwrap()),
        Err(GrokCommandError::InvalidSourceCommand)
    );
    unsupported["source_command_sha256"] = source_sha256.clone().into();
    unsupported["unknown"] = true.into();
    assert_eq!(
        parse_image_generation_command(&serde_json::to_vec(&unsupported).unwrap()),
        Err(GrokCommandError::InvalidCanonicalCommand)
    );

    let mut forged: serde_json::Value =
        serde_json::from_slice(command.canonical_payload()).unwrap();
    forged["prompt"] = "a different image".into();
    assert_eq!(
        parse_image_generation_payload(&serde_json::to_vec(&forged).unwrap()),
        Err(GrokCommandError::InvalidCanonicalCommand)
    );

    let edit = GrokImageEditRequestV1::new(
        "add a hat",
        vec![staged("source.png")],
        ImageAspectRatio::Auto,
    )
    .unwrap();
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).unwrap(),
        GrokImageEditPayloadV1::new(SOURCE_SHA256, edit.clone()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        parse_image_edit_payload(command.canonical_payload())
            .unwrap()
            .source_command_sha256(),
        SOURCE_SHA256
    );
    assert_eq!(
        parse_image_edit_command(command.canonical_payload()).unwrap(),
        edit
    );
    let mut drifted: serde_json::Value =
        serde_json::from_slice(command.canonical_payload()).unwrap();
    drifted["aspect_ratio"] = "16:9".into();
    assert_eq!(
        parse_image_edit_command(&serde_json::to_vec(&drifted).unwrap()),
        Err(GrokCommandError::InvalidRequest(
            RequestValidationError::SingleImageAspectRatioUnsupported
        ))
    );
}

#[test]
fn video_command_preserves_the_two_distinct_fixed_model_workflows() {
    let source = XaiVideoGenerationCommandV1::from_request(XaiVideoGenerationRequest {
        aspect_ratio: None,
        duration: Some(10),
        image: Some(XaiVideoImageUrl {
            file_id: None,
            url: Some("data:image/png;base64,AA==".to_owned()),
        }),
        model: Some("grok-imagine-video-1.5".to_owned()),
        output: None,
        prompt: Some("slow push in".to_owned()),
        reference_images: Vec::new(),
        resolution: Some(OfficialVideoResolution::P720),
        storage_options: None,
        user: None,
    })
    .unwrap();
    let source_sha256 = source.canonical_sha256_hex();
    let image_to_video = ImageToVideoRequestV1::new(
        Some("slow push in".to_owned()),
        staged("first.png"),
        VideoDuration::Seconds10,
        VideoResolution::P720,
    )
    .unwrap();
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).unwrap(),
        GrokVideoGenerationPayloadV1::from_xai_command(source, vec![staged("first.png")]).unwrap(),
    )
    .unwrap();
    assert_eq!(
        parse_video_generation_payload(command.canonical_payload())
            .unwrap()
            .source_command_sha256(),
        source_sha256
    );
    assert_eq!(
        parse_video_generation_command(command.canonical_payload()).unwrap(),
        GrokVideoGenerationRequestV1::ImageToVideo(image_to_video)
    );
    assert!(
        std::str::from_utf8(command.canonical_payload())
            .unwrap()
            .contains("grok-imagine-video-1.5-preview")
    );
    assert!(
        !std::str::from_utf8(command.canonical_payload())
            .unwrap()
            .contains("data:image")
    );
    assert!(
        std::str::from_utf8(command.canonical_payload())
            .unwrap()
            .contains("factory-staged-sha256:")
    );
    let mut tampered: serde_json::Value =
        serde_json::from_slice(command.canonical_payload()).unwrap();
    tampered["source_command"]["image"]["url"] = json!(
        "factory-staged-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        parse_video_generation_payload(&serde_json::to_vec(&tampered).unwrap()),
        Err(GrokCommandError::InvalidCanonicalCommand)
    );

    let reference_images = vec![staged("one.png"), staged("two.png")];
    let reference = ReferenceToVideoRequestV1::new(
        "cinematic motion",
        reference_images.clone(),
        VideoAspectRatio::R2x3,
        VideoDuration::Seconds6,
        VideoResolution::P480,
    )
    .unwrap();
    let source = XaiVideoGenerationCommandV1::from_request(XaiVideoGenerationRequest {
        aspect_ratio: Some(OfficialVideoAspectRatio::R2x3),
        duration: Some(6),
        image: None,
        model: Some("grok-imagine-video".to_owned()),
        output: None,
        prompt: Some("cinematic motion".to_owned()),
        reference_images: vec![
            XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".to_owned()),
            },
            XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AQ==".to_owned()),
            },
        ],
        resolution: Some(OfficialVideoResolution::P480),
        storage_options: None,
        user: None,
    })
    .unwrap();
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).unwrap(),
        GrokVideoGenerationPayloadV1::from_xai_command(source, reference_images).unwrap(),
    )
    .unwrap();
    assert_eq!(
        parse_video_generation_command(command.canonical_payload()).unwrap(),
        GrokVideoGenerationRequestV1::ReferenceToVideo(reference)
    );
    assert!(
        std::str::from_utf8(command.canonical_payload())
            .unwrap()
            .contains("grok-imagine-video\"")
    );
    assert!(
        !std::str::from_utf8(command.canonical_payload())
            .unwrap()
            .contains("data:image")
    );
}

#[test]
fn policy_separates_runtime_credentials_workspace_and_user_prompt() {
    let fixture = PolicyFixture::new();
    let injection = "image; $(touch /tmp/should-not-exist) && end";
    let request =
        GrokImageGenerationRequestV1::new(injection, ImageModel::Base, ImageAspectRatio::R1x1)
            .unwrap();
    let (command, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();

    assert!(
        !command
            .arguments()
            .iter()
            .any(|argument| argument == injection)
    );
    assert!(
        std::str::from_utf8(command.stdin_bytes())
            .unwrap()
            .contains(injection)
    );
    assert_eq!(
        command.environment().get(OsStr::new("HOME")).unwrap(),
        fixture.runtime_home.path().as_os_str()
    );
    assert_eq!(
        command.environment().get(OsStr::new("GROK_HOME")).unwrap(),
        fixture.grok_home.path().as_os_str()
    );
    assert_ne!(
        command.environment().get(OsStr::new("HOME")),
        command.environment().get(OsStr::new("GROK_HOME"))
    );
    assert_eq!(invocation.tool(), GrokTool::ImageGeneration);
    assert_eq!(
        invocation.expected_arguments(),
        &json!({"prompt": injection, "aspect_ratio": "1:1"})
    );
    assert!(
        invocation
            .artifact_path()
            .ends_with(PathBuf::from("images").join("1.jpg"))
    );
}

#[test]
fn video_policy_enables_the_cli_zdr_output_binding() {
    let fixture = PolicyFixture::new();
    let request = GrokVideoGenerationRequestV1::ImageToVideo(
        ImageToVideoRequestV1::new(
            Some("slow push in".to_owned()),
            staged("first.png"),
            VideoDuration::Seconds6,
            VideoResolution::P480,
        )
        .unwrap(),
    );
    let (video_command, _) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    assert_eq!(
        video_command
            .environment()
            .get(OsStr::new("GROK_DISABLE_ZDR_INCOMPATIBLE_TOOLS"))
            .unwrap(),
        OsStr::new("true")
    );

    let fixture = PolicyFixture::new();
    let image_request =
        GrokImageGenerationRequestV1::new("image", ImageModel::Base, ImageAspectRatio::R1x1)
            .unwrap();
    let (image_command, _) = fixture
        .policy
        .command_spec_in(&image_request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    assert!(
        image_command
            .environment()
            .get(OsStr::new("GROK_DISABLE_ZDR_INCOMPATIBLE_TOOLS"))
            .is_none()
    );
}

#[test]
fn receipt_requires_matching_terminal_tool_arguments_and_local_artifact() {
    let fixture = PolicyFixture::new();
    let request = GrokImageGenerationRequestV1::new(
        "a black circle",
        ImageModel::Quality,
        ImageAspectRatio::R1x1,
    )
    .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    write_artifact(&invocation);
    let stdout = valid_stdout();
    let history = valid_history(&invocation);

    let receipt = parse_invocation_receipt(&stdout, &history, &invocation).unwrap();
    assert_eq!(receipt.session_id(), SESSION_ID);
    assert_eq!(receipt.headless_request_id(), "request-1");
    assert_eq!(receipt.effective_tool_prompt(), Some("a black circle"));
    assert_eq!(receipt.artifact_path(), invocation.artifact_path());
    assert_eq!(receipt.headless_usage().unwrap()["inputTokens"], 12);
    assert!(receipt.provider_reported_cost().is_none());
}

#[test]
fn receipt_preserves_exact_cli_invocation_ticks_without_promoting_float_cost() {
    let fixture = PolicyFixture::new();
    let request = GrokImageGenerationRequestV1::new(
        "a black circle",
        ImageModel::Quality,
        ImageAspectRatio::R1x1,
    )
    .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    write_artifact(&invocation);
    let stdout = format!(
        "{{\"type\":\"end\",\"stopReason\":\"end_turn\",\"sessionId\":\"{SESSION_ID}\",\"requestId\":\"request-1\",\"costUSD\":99.99,\"total_cost_usd_ticks\":200000000}}\n"
    );

    let receipt =
        parse_invocation_receipt(stdout.as_bytes(), &valid_history(&invocation), &invocation)
            .unwrap();
    let evidence = receipt.provider_reported_cost().unwrap();
    assert_eq!(evidence.scope().as_str(), "cli_invocation");
    assert_eq!(evidence.observation().native_quantity, 200_000_000);
    assert_eq!(
        evidence.observation().evidence_path,
        "end.total_cost_usd_ticks"
    );
    assert_eq!(
        receipt.headless_usage().unwrap()["costUSD"],
        serde_json::json!(99.99)
    );
}

#[test]
fn receipt_preserves_explicit_zero_ticks_as_exact_cost() {
    let fixture = PolicyFixture::new();
    let request =
        GrokImageGenerationRequestV1::new("prompt", ImageModel::Quality, ImageAspectRatio::Auto)
            .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    write_artifact(&invocation);
    let stdout = format!(
        "{{\"type\":\"end\",\"stopReason\":\"end_turn\",\"sessionId\":\"{SESSION_ID}\",\"requestId\":\"request-1\",\"total_cost_usd_ticks\":0}}\n"
    );

    let receipt =
        parse_invocation_receipt(stdout.as_bytes(), &valid_history(&invocation), &invocation)
            .unwrap();
    assert_eq!(
        receipt
            .provider_reported_cost()
            .unwrap()
            .observation()
            .native_quantity,
        0
    );
}

#[test]
fn receipt_rejects_fractional_or_incomplete_exact_ticks() {
    let fixture = PolicyFixture::new();
    let request =
        GrokImageGenerationRequestV1::new("prompt", ImageModel::Quality, ImageAspectRatio::Auto)
            .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    write_artifact(&invocation);
    let history = valid_history(&invocation);

    for extra in [
        "\"total_cost_usd_ticks\":1.5",
        "\"total_cost_usd_ticks\":-1",
        "\"total_cost_usd_ticks\":\"200000000\"",
        "\"total_cost_usd_ticks\":null",
        "\"total_cost_usd_ticks\":18446744073709551616",
        "\"total_cost_usd_ticks\":10,\"usage_is_incomplete\":true",
    ] {
        let stdout = format!(
            "{{\"type\":\"end\",\"stopReason\":\"end_turn\",\"sessionId\":\"{SESSION_ID}\",\"requestId\":\"request-1\",{extra}}}\n"
        );
        assert!(matches!(
            parse_invocation_receipt(stdout.as_bytes(), &history, &invocation),
            Err(GrokReceiptError::InvalidTerminalEvent)
        ));
    }
}

#[test]
fn receipt_rejects_agent_argument_drift_and_artifact_escape() {
    let fixture = PolicyFixture::new();
    let request =
        GrokImageGenerationRequestV1::new("original", ImageModel::Quality, ImageAspectRatio::R1x1)
            .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    write_artifact(&invocation);

    let drifted_arguments = json!({"prompt":"rewritten", "aspect_ratio":"1:1"});
    let history = history_with(&invocation, drifted_arguments, invocation.artifact_path());
    assert!(matches!(
        parse_invocation_receipt(&valid_stdout(), &history, &invocation),
        Err(GrokReceiptError::ToolArgumentsMismatch)
    ));

    let history = history_with(
        &invocation,
        invocation.expected_arguments().clone(),
        std::path::Path::new("/tmp/escaped.jpg"),
    );
    assert!(matches!(
        parse_invocation_receipt(&valid_stdout(), &history, &invocation),
        Err(GrokReceiptError::ArtifactPathMismatch)
    ));
}

#[test]
fn receipt_classifies_grok_tool_failures_before_artifact_validation() {
    let fixture = PolicyFixture::new();
    let request =
        GrokImageGenerationRequestV1::new("prompt", ImageModel::Quality, ImageAspectRatio::Auto)
            .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    let tool_call_id = "tool-call-1";
    let assistant = json!({
        "type": "assistant",
        "tool_calls": [{
            "id": tool_call_id,
            "name": invocation.tool().name(),
            "arguments": serde_json::to_string(invocation.expected_arguments()).unwrap()
        }]
    });

    for (content, expected) in [
        (
            "Tool `image_to_video` failed: Video generation failed with HTTP 400 Bad Request: {\"code\":\"invalid-argument\",\"error\":\"Zero Data Retention teams must provide output.upload_url for video generation.\"}",
            GrokReceiptError::VideoOutputUploadUrlRequired,
        ),
        (
            "Tool `image_to_video` failed: upstream unavailable",
            GrokReceiptError::ToolExecutionFailed,
        ),
    ] {
        let result = json!({
            "type": "tool_result",
            "tool_call_id": tool_call_id,
            "content": content
        });
        let history = format!("{assistant}\n{result}\n");
        assert_eq!(
            parse_invocation_receipt(&valid_stdout(), history.as_bytes(), &invocation).unwrap_err(),
            expected
        );
    }
}

#[test]
fn receipt_rejects_duplicate_or_non_terminal_end_events() {
    let fixture = PolicyFixture::new();
    let request =
        GrokImageGenerationRequestV1::new("prompt", ImageModel::Quality, ImageAspectRatio::Auto)
            .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    write_artifact(&invocation);
    let history = valid_history(&invocation);
    let duplicate = [valid_stdout(), valid_stdout()].concat();
    assert!(matches!(
        parse_invocation_receipt(&duplicate, &history, &invocation),
        Err(GrokReceiptError::MissingTerminalEvent)
    ));
}

#[test]
fn receipt_is_evidence_only_and_does_not_claim_artifact_authority() {
    let fixture = PolicyFixture::new();
    let request =
        GrokImageGenerationRequestV1::new("prompt", ImageModel::Quality, ImageAspectRatio::Auto)
            .unwrap();
    let (_, invocation) = fixture
        .policy
        .command_spec_in(&request.into(), SESSION_ID, fixture.workspace.clone())
        .unwrap();
    write_artifact(&invocation);
    fs::hard_link(
        invocation.artifact_path(),
        fixture.workspace.path().join("artifact-alias.jpg"),
    )
    .unwrap();

    assert!(
        parse_invocation_receipt(&valid_stdout(), &valid_history(&invocation), &invocation).is_ok()
    );
}

fn staged(filename: &str) -> StagedImageV1 {
    StagedImageV1::new(filename, IMAGE_SHA256).unwrap()
}

fn valid_stdout() -> Vec<u8> {
    format!(
        "{{\"type\":\"text\",\"data\":\"done\"}}\n{{\"type\":\"end\",\"stopReason\":\"end_turn\",\"sessionId\":\"{SESSION_ID}\",\"requestId\":\"request-1\",\"inputTokens\":12}}\n"
    )
    .into_bytes()
}

fn valid_history(invocation: &GrokInvocationV1) -> Vec<u8> {
    history_with(
        invocation,
        invocation.expected_arguments().clone(),
        invocation.artifact_path(),
    )
}

fn history_with(
    invocation: &GrokInvocationV1,
    arguments: serde_json::Value,
    artifact_path: &std::path::Path,
) -> Vec<u8> {
    let call = json!({
        "type":"assistant",
        "tool_calls":[{
            "id":"call-1",
            "name":invocation.tool().name(),
            "arguments":serde_json::to_string(&arguments).unwrap()
        }]
    });
    let content = json!({
        "path":artifact_path,
        "filename":artifact_path.file_name().unwrap().to_str().unwrap(),
        "session_folder":artifact_path
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap(),
        "message":"saved"
    });
    let result = json!({
        "type":"tool_result",
        "tool_call_id":"call-1",
        "content":serde_json::to_string(&content).unwrap()
    });
    format!("{}\n{}\n", call, result).into_bytes()
}

fn write_artifact(invocation: &GrokInvocationV1) {
    fs::create_dir_all(invocation.artifact_path().parent().unwrap()).unwrap();
    fs::write(invocation.artifact_path(), b"not-empty").unwrap();
}

struct PolicyFixture {
    _temp: TempDir,
    policy: GrokCliPolicyV1,
    workspace: WorkingDirectory,
    runtime_home: WorkingDirectory,
    grok_home: WorkingDirectory,
}

impl PolicyFixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("grok");
        fs::write(&executable, b"#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
        let executable_sha256: [u8; 32] = Sha256::digest(fs::read(&executable).unwrap()).into();

        let workspace_root = private_directory(temp.path().join("workspaces"));
        let workspace = private_directory(workspace_root.path().join("attempt-1"));
        let runtime_home = private_directory(temp.path().join("runtime-home"));
        let grok_home = private_directory(temp.path().join("grok-home"));
        let policy = GrokCliPolicyV1::new(
            &executable,
            executable_sha256,
            workspace_root,
            runtime_home.clone(),
            grok_home.clone(),
            Duration::from_secs(300),
            Duration::from_secs(2),
        )
        .unwrap();
        Self {
            _temp: temp,
            policy,
            workspace,
            runtime_home,
            grok_home,
        }
    }
}

fn private_directory(path: PathBuf) -> WorkingDirectory {
    fs::create_dir_all(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    WorkingDirectory::new_private(path).unwrap()
}
