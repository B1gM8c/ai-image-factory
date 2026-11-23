use std::path::Path;

use image_provider_dreamina_cli::{
    DreaminaSubmitPayloadV1, DreaminaSubmitRequestV1, StagedInputKindV1, parse_submit_command,
};
use image_provider_sdk::SingleOutputCommand;
use serde::{Deserialize, Deserializer};
use uuid::Uuid;

use crate::{
    admission::valid_input_descriptor,
    executor::ExecutorInputObject,
    provider_tasks::{
        GatedCliProcessOutcome, GatedCliProcessTerminal, GatedCliReceiptEvidence,
        ProviderSubmitDriverCall,
    },
};

use super::submit_recovery::{absolute_media_arguments, has_no_acceptance_evidence};

// The pinned Linux Dreamina 1.4.21 handlers return these exact upload
// failures before reaching the generation client: image2image at 0x117070d
// (error branches to 0x117088f), image2video at 0x116c46d,
// frames2video at 0x116d6e3, multiframe2video at 0x1171ec8, and
// multimodal2video at 0x116eb29 (images), 0x116ebaf (videos), 0x116ec32 (audio).
const REVIEWED_DREAMINA_SHA256: &str =
    "0a5fdc262ab44669e7db4ce67e6543ece6daf405804aca3d1d17d1345c146f69";
const UPLOAD_FAILURE_PREFIX: &str = "upload resource \"";
const IMAGE_UPLOAD_FAILURE_SUFFIX: &str =
    "\": upload image: upload phase, no file upload, please check log for more details";
const TIMED_UPLOAD_FAILURE_SUFFIX: &str = "\": upload video/audio: no result returned";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FailedUploadReceipt {
    submit_id: String,
    gen_status: String,
    #[serde(default, deserialize_with = "present_prompt")]
    prompt: Option<String>,
    fail_reason: String,
}

fn present_prompt<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    // Go's omitempty omits an empty prompt entirely. A present null is not a receipt.
    String::deserialize(deserializer).map(Some)
}

pub(super) struct MaterialUploadFailure {
    workflow: &'static str,
    model: &'static str,
    kind: StagedInputKindV1,
    position: usize,
    ordinal: usize,
    sha256: String,
    dimensions: Option<(u32, u32)>,
    duration_ms: Option<u64>,
}

impl MaterialUploadFailure {
    fn role(&self) -> &'static str {
        match self.kind {
            StagedInputKindV1::Image => "image",
            StagedInputKindV1::Video => "video",
            StagedInputKindV1::Audio => "audio",
        }
    }

    fn frozen_input<'a>(
        &self,
        inputs: &'a [ExecutorInputObject],
    ) -> Option<&'a ExecutorInputObject> {
        inputs.get(self.position).filter(|input| {
            usize::from(input.index()) == self.position
                && input.role() == self.role()
                && valid_input_descriptor(self.role(), input.index(), input.media_type())
                && input.blob().sha256_hex == self.sha256
        })
    }

    pub(super) fn log(
        &self,
        call: &ProviderSubmitDriverCall<DreaminaSubmitPayloadV1>,
        evidence: &GatedCliReceiptEvidence,
    ) {
        let input = self.frozen_input(call.inputs());
        let is_image = self.kind == StagedInputKindV1::Image;
        let media_type = input.map(ExecutorInputObject::media_type);
        let byte_size = input.map(|input| input.blob().byte_size);
        tracing::warn!(
            submission_id = %call.intent().submission_id,
            error_code = "dreamina_material_upload_failed",
            workflow = self.workflow,
            model = self.model,
            material_kind = self.role(),
            material_ordinal = self.ordinal,
            material_duration_ms = self.duration_ms,
            material_media_type = media_type,
            material_byte_size = byte_size,
            image_ordinal = is_image.then_some(self.ordinal),
            image_width = self.dimensions.map(|(width, _)| width),
            image_height = self.dimensions.map(|(_, height)| height),
            image_media_type = media_type.filter(|_| is_image),
            image_byte_size = byte_size.filter(|_| is_image),
            cli_elapsed_ms = evidence.finished_at_ms() - evidence.started_at_ms(),
            "Dreamina stopped before generation submission; CLI did not report the underlying upload error"
        );
    }
}

pub(crate) fn is_dreamina_material_upload_failure(
    terminal: &GatedCliProcessTerminal,
    evidence: &GatedCliReceiptEvidence,
    command: &SingleOutputCommand<DreaminaSubmitPayloadV1>,
) -> bool {
    dreamina_material_upload_failure(terminal, evidence, command).is_some()
}

pub(super) fn dreamina_material_upload_failure(
    terminal: &GatedCliProcessTerminal,
    evidence: &GatedCliReceiptEvidence,
    command: &SingleOutputCommand<DreaminaSubmitPayloadV1>,
) -> Option<MaterialUploadFailure> {
    if !terminal.released()
        || !terminal.exec_started()
        || terminal.stdout_truncated()
        || terminal.stderr_truncated()
        || !terminal.stderr().is_empty()
        || !matches!(
            terminal.outcome(),
            GatedCliProcessOutcome::Exited {
                exit_code: Some(0),
                signal: None,
            }
        )
    {
        return None;
    }
    let request = parse_submit_command(command.canonical_payload()).ok()?;
    let process = evidence.command();
    matches_material_upload_failure(
        terminal.stdout(),
        process.executable_sha256(),
        process.working_directory(),
        process.arguments(),
        &request,
    )
    .filter(|_| has_no_acceptance_evidence(evidence))
}

fn matches_material_upload_failure(
    stdout: &[u8],
    executable_sha256: &str,
    workspace: &Path,
    observed_argv: &[String],
    request: &DreaminaSubmitRequestV1,
) -> Option<MaterialUploadFailure> {
    if executable_sha256 != REVIEWED_DREAMINA_SHA256 {
        return None;
    }
    let receipt = serde_json::from_slice::<FailedUploadReceipt>(stdout).ok()?;
    let candidate_id = Uuid::parse_str(&receipt.submit_id).ok()?;
    let (argv, prompt, model, workflow) = match request {
        DreaminaSubmitRequestV1::ImageToImage(request) if request.generate_num() == 1 => (
            request.to_argv(),
            request.parameters().prompt(),
            request.parameters().model().as_str(),
            "image2image",
        ),
        DreaminaSubmitRequestV1::MultimodalToVideo(request) => (
            request.to_argv(),
            request.prompt(),
            request.model().as_str(),
            "multimodal2video",
        ),
        DreaminaSubmitRequestV1::ImageToVideo(request) => (
            request.to_argv(),
            request.options().prompt(),
            request.options().model().as_str(),
            "image2video",
        ),
        DreaminaSubmitRequestV1::FramesToVideo(request) => (
            request.to_argv(),
            request.options().prompt(),
            request.options().model().as_str(),
            "frames2video",
        ),
        // The pinned CLI plan records transition prompts separately and the
        // receipt printer omits the prompt field for both multiframe forms.
        DreaminaSubmitRequestV1::MultiframeToVideo(request) => (
            request.to_argv(),
            "",
            "multiframe2video",
            "multiframe2video",
        ),
        _ => return None,
    };
    let prompt = prompt.trim();
    if candidate_id.is_nil()
        || candidate_id.hyphenated().to_string() != receipt.submit_id
        || receipt.gen_status != "fail"
        || receipt.prompt.as_deref() != (!prompt.is_empty()).then_some(prompt)
    {
        return None;
    }
    let reason = receipt.fail_reason.strip_prefix(UPLOAD_FAILURE_PREFIX)?;
    let (path, is_image) = if let Some(path) = reason.strip_suffix(IMAGE_UPLOAD_FAILURE_SUFFIX) {
        (path, true)
    } else if matches!(request, DreaminaSubmitRequestV1::MultimodalToVideo(_)) {
        (reason.strip_suffix(TIMED_UPLOAD_FAILURE_SUFFIX)?, false)
    } else {
        return None;
    };
    let relative_argv = argv
        .into_iter()
        .map(|arg| arg.into_string())
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    let absolute = if observed_argv == relative_argv {
        false
    } else if matches!(request, DreaminaSubmitRequestV1::MultimodalToVideo(_))
        && workspace.is_absolute()
        && absolute_media_arguments(&relative_argv, workspace, false)
            .is_ok_and(|argv| argv == observed_argv)
    {
        // Only the normal multimodal projection replaces basenames with
        // absolute paths under the same isolated workspace.
        true
    } else {
        return None;
    };
    let (position, input) =
        request
            .staged_inputs()
            .into_iter()
            .enumerate()
            .find(|(_, input)| {
                (input.kind == StagedInputKindV1::Image) == is_image
                    && if absolute {
                        workspace.join(input.filename).as_os_str() == path
                    } else {
                        input.filename == path
                    }
            })?;
    Some(MaterialUploadFailure {
        workflow,
        model,
        kind: input.kind,
        position,
        ordinal: input.index + 1,
        sha256: input.sha256.to_owned(),
        dimensions: input.dimensions,
        duration_ms: input.duration_ms,
    })
}

#[cfg(test)]
mod tests {
    use image_provider_dreamina_cli::{
        FramesToVideoRequestV1, ImageModelVersion, ImageRatio, ImageResolution,
        ImageToImageRequestV1, ImageToVideoRequestV1, MultiframeToVideoRequestV1,
        MultimodalToVideoRequestV1, ReferenceVideoOptionsV1, StagedImageV1, StagedTimedMediaV1,
        TextToImageRequestV1, VideoModelVersion, VideoRatio, VideoResolution, VideoTransitionV1,
    };
    use serde_json::{Value, json};

    use crate::input_blobs::{InputBlobKey, InputBlobRef};

    use super::*;

    const TEST_ID: &str = "a12bdcaa-9068-4c93-8653-2c0f4813643f";
    const WORKSPACE: &str = "/frozen/private/.provider-submit-fixture";
    const PROMPT: &str = "画面\n--image";

    fn request() -> MultimodalToVideoRequestV1 {
        request_with_prompt(PROMPT)
    }

    fn request_with_prompt(prompt: &str) -> MultimodalToVideoRequestV1 {
        MultimodalToVideoRequestV1::new(
            vec![
                StagedImageV1::new("input-0.png", "a".repeat(64), 1600, 900).unwrap(),
                StagedImageV1::new("input-1.jpg", "c".repeat(64), 1674, 940).unwrap(),
            ],
            vec![StagedTimedMediaV1::new("video-0.mp4", "d".repeat(64), 2_400).unwrap()],
            vec![StagedTimedMediaV1::new("audio-0.wav", "b".repeat(64), 2_000).unwrap()],
            prompt,
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            6,
            VideoResolution::P720,
        )
        .unwrap()
    }

    fn argv(request: &MultimodalToVideoRequestV1) -> Vec<String> {
        request
            .to_argv()
            .into_iter()
            .map(|arg| arg.into_string().unwrap())
            .collect()
    }

    fn test_input(role: &str, index: u16, media_type: &str, digest: &str) -> ExecutorInputObject {
        ExecutorInputObject::new(
            InputBlobRef {
                key: InputBlobKey {
                    admission_session_id: Uuid::new_v4(),
                    input_id: Uuid::new_v4(),
                },
                storage_backend: "test".into(),
                object_key: "fixture".into(),
                sha256_hex: digest.repeat(64),
                byte_size: 1_767_471,
            },
            role,
            index,
            media_type,
        )
        .unwrap()
    }

    fn receipt(path: &str) -> Value {
        json!({
            "submit_id": TEST_ID,
            "gen_status": "fail",
            "prompt": PROMPT,
            "fail_reason": format!("{UPLOAD_FAILURE_PREFIX}{path}{IMAGE_UPLOAD_FAILURE_SUFFIX}"),
        })
    }

    fn matches(stdout: &[u8], argv: &[String], request: &DreaminaSubmitRequestV1) -> bool {
        matches_material_upload_failure(
            stdout,
            REVIEWED_DREAMINA_SHA256,
            Path::new(WORKSPACE),
            argv,
            request,
        )
        .is_some()
    }

    #[test]
    fn material_upload_failure_requires_the_sealed_image_argument() {
        let request = request();
        let relative = argv(&request);
        let request = request.into();
        let workspace = Path::new(WORKSPACE);
        let absolute = absolute_media_arguments(&relative, workspace, false).unwrap();
        let absolute_image = workspace.join("input-0.png");
        let absolute_image = absolute_image.to_str().unwrap();
        let historical = receipt("input-0.png").to_string();
        let current = receipt(absolute_image).to_string();

        assert!(matches(historical.as_bytes(), &relative, &request));
        assert!(matches(current.as_bytes(), &absolute, &request));
        assert!(!matches(current.as_bytes(), &relative, &request));
        assert!(!matches(historical.as_bytes(), &absolute, &request));
        assert!(!matches(
            receipt("audio-0.wav").to_string().as_bytes(),
            &relative,
            &request,
        ));
        assert!(!matches(
            receipt("/different/attempt/input-0.png")
                .to_string()
                .as_bytes(),
            &absolute,
            &request,
        ));
        let mut other_command = absolute.clone();
        *other_command.last_mut().unwrap() = "--poll=1".into();
        assert!(!matches(current.as_bytes(), &other_command, &request));
        assert!(
            matches_material_upload_failure(
                current.as_bytes(),
                &"f".repeat(64),
                workspace,
                &absolute,
                &request,
            )
            .is_none()
        );
    }

    #[test]
    fn material_upload_diagnostics_select_the_bound_failed_image() {
        let request = request();
        let workspace = Path::new(WORKSPACE);
        let absolute = absolute_media_arguments(&argv(&request), workspace, false).unwrap();
        let path = workspace.join("input-1.jpg");
        let failure = matches_material_upload_failure(
            receipt(path.to_str().unwrap()).to_string().as_bytes(),
            REVIEWED_DREAMINA_SHA256,
            workspace,
            &absolute,
            &request.into(),
        )
        .unwrap();

        assert_eq!(failure.workflow, "multimodal2video");
        assert_eq!(failure.model, VideoModelVersion::Seedance2_5.as_str());
        assert_eq!(failure.ordinal, 2);
        assert_eq!(failure.dimensions, Some((1674, 940)));
        let mut inputs = [
            test_input("image", 0, "image/png", "a"),
            test_input("image", 1, "image/jpeg", "c"),
        ];
        let selected = failure.frozen_input(&inputs).unwrap();
        assert_eq!(selected.media_type(), "image/jpeg");
        assert_eq!(selected.blob().byte_size, 1_767_471);
        assert!(failure.frozen_input(&[]).is_none());
        inputs[1] = test_input("image", 1, "image/jpeg", "d");
        assert!(failure.frozen_input(&inputs).is_none());
    }

    #[test]
    fn material_upload_failure_rejects_inexact_or_competing_receipt_evidence() {
        let request = request();
        let relative = argv(&request);
        let request = request.into();
        let valid = receipt("input-0.png");
        for (field, value) in [
            ("gen_status", json!("querying")),
            ("submit_id", json!("00000000-0000-0000-0000-000000000000")),
            ("submit_id", json!("not-a-uuid")),
            ("prompt", json!("different prompt")),
            ("logid", json!("competing-evidence")),
            ("result_json", Value::Null),
            ("fail_reason", json!("upload image: no file upload")),
        ] {
            let mut changed = valid.clone();
            changed[field] = value;
            assert!(!matches(
                changed.to_string().as_bytes(),
                &relative,
                &request
            ));
        }
        let valid = valid.to_string();
        let duplicate = format!("{},\"gen_status\":\"fail\"}}", &valid[..valid.len() - 1]);
        assert!(!matches(duplicate.as_bytes(), &relative, &request));
        let trailing = format!("{valid}\n{{}}");
        assert!(!matches(trailing.as_bytes(), &relative, &request));
    }

    #[test]
    fn material_upload_failure_binds_single_output_image_edit() {
        for count in [1, 2] {
            let request = ImageToImageRequestV1::new(
                vec![
                    StagedImageV1::new("input-0.png", "a".repeat(64), 800, 600).unwrap(),
                    StagedImageV1::new("input-1.png", "b".repeat(64), 900, 900).unwrap(),
                    StagedImageV1::new("input-2.png", "c".repeat(64), 1024, 768).unwrap(),
                ],
                TextToImageRequestV1::new(
                    PROMPT,
                    ImageModelVersion::V5_0,
                    ImageRatio::R1x1,
                    ImageResolution::K2,
                    count,
                )
                .unwrap(),
            )
            .unwrap();
            let argv = request
                .to_argv()
                .into_iter()
                .map(|arg| arg.into_string().unwrap())
                .collect::<Vec<_>>();
            let request = request.into();
            let workspace = Path::new(WORKSPACE);
            let raw = receipt("input-2.png").to_string();
            let failure = matches_material_upload_failure(
                raw.as_bytes(),
                REVIEWED_DREAMINA_SHA256,
                workspace,
                &argv,
                &request,
            );
            if count == 1 {
                let failure = failure.unwrap();
                assert_eq!(failure.workflow, "image2image");
                assert_eq!(failure.ordinal, 3);
                assert_eq!(failure.dimensions, Some((1024, 768)));
            } else {
                assert!(failure.is_none());
            }
            assert!(!matches(
                receipt("other.png").to_string().as_bytes(),
                &argv,
                &request,
            ));
            let mut changed = receipt("input-2.png");
            changed["fail_reason"] = json!("upload image: no file upload");
            assert!(!matches(changed.to_string().as_bytes(), &argv, &request));
            assert!(
                matches_material_upload_failure(
                    raw.as_bytes(),
                    &"f".repeat(64),
                    workspace,
                    &argv,
                    &request
                )
                .is_none()
            );
        }
    }

    #[test]
    fn material_upload_failure_binds_the_single_image_to_video_workflow() {
        let request = ImageToVideoRequestV1::new(
            StagedImageV1::new("input-0.png", "a".repeat(64), 1600, 900).unwrap(),
            ReferenceVideoOptionsV1::new(
                PROMPT,
                VideoModelVersion::Seedance2_0FastVip,
                Some(VideoRatio::R16x9),
                10,
                VideoResolution::P720,
            )
            .unwrap(),
        )
        .unwrap();
        let relative = request
            .to_argv()
            .into_iter()
            .map(|arg| arg.into_string().unwrap())
            .collect::<Vec<_>>();
        let request = request.into();
        let raw = receipt("input-0.png").to_string();
        assert!(matches(raw.as_bytes(), &relative, &request));

        let mut different_workflow = relative.clone();
        different_workflow[0] = "multimodal2video".into();
        assert!(!matches(raw.as_bytes(), &different_workflow, &request));

        let workspace = Path::new(WORKSPACE);
        let absolute = absolute_media_arguments(&relative, workspace, false).unwrap();
        let raw_absolute = receipt(workspace.join("input-0.png").to_str().unwrap()).to_string();
        assert!(!matches(raw_absolute.as_bytes(), &absolute, &request));
    }
    #[test]
    fn material_upload_failure_binds_both_frame_paths_and_native_trimmed_prompt() {
        let request = FramesToVideoRequestV1::new(
            StagedImageV1::new("first.png", "a".repeat(64), 1600, 900).unwrap(),
            StagedImageV1::new("last.jpg", "b".repeat(64), 1600, 900).unwrap(),
            ReferenceVideoOptionsV1::new(
                format!(" \t{PROMPT}\n\u{85}"),
                VideoModelVersion::Seedance2_0FastVip,
                Some(VideoRatio::R16x9),
                10,
                VideoResolution::P720,
            )
            .unwrap(),
        )
        .unwrap();
        let argv = request
            .to_argv()
            .into_iter()
            .map(|arg| arg.into_string().unwrap())
            .collect::<Vec<_>>();
        let request = request.into();
        for path in ["first.png", "last.jpg"] {
            assert!(matches(
                receipt(path).to_string().as_bytes(),
                &argv,
                &request
            ));
        }
        assert!(!matches(
            receipt("other.png").to_string().as_bytes(),
            &argv,
            &request,
        ));
        let mut changed = receipt("last.jpg");
        changed["prompt"] = json!(format!(" {PROMPT} "));
        assert!(!matches(changed.to_string().as_bytes(), &argv, &request));
        let mut other_argv = argv.clone();
        other_argv.swap(2, 4);
        assert!(!matches(
            receipt("last.jpg").to_string().as_bytes(),
            &other_argv,
            &request,
        ));
    }

    #[test]
    fn material_upload_failure_requires_omitted_prompt_for_both_multiframe_forms() {
        for count in [2, 4] {
            let frames = (0..count)
                .map(|index| {
                    StagedImageV1::new(format!("frame-{index}.png"), "a".repeat(64), 1600, 900)
                        .unwrap()
                })
                .collect();
            let transitions = (1..count)
                .map(|index| VideoTransitionV1::new(format!("transition {index}"), 2_000).unwrap())
                .collect();
            let request =
                MultiframeToVideoRequestV1::new(frames, transitions, VideoResolution::P720)
                    .unwrap();
            let argv = request
                .to_argv()
                .into_iter()
                .map(|arg| arg.into_string().unwrap())
                .collect::<Vec<_>>();
            let request = request.into();
            let mut omitted = receipt("frame-1.png");
            omitted.as_object_mut().unwrap().remove("prompt");
            assert!(matches(omitted.to_string().as_bytes(), &argv, &request));
            for extra_prompt in [json!(null), json!(""), json!("transition 1")] {
                let mut bad = omitted.clone();
                bad["prompt"] = extra_prompt;
                assert!(!matches(bad.to_string().as_bytes(), &argv, &request));
            }
            let mut bad = omitted.clone();
            bad["fail_reason"] = json!(format!(
                "{UPLOAD_FAILURE_PREFIX}frame-{count}.png{IMAGE_UPLOAD_FAILURE_SUFFIX}"
            ));
            assert!(!matches(bad.to_string().as_bytes(), &argv, &request));
        }
    }

    #[test]
    fn material_upload_failure_binds_video_and_audio_to_frozen_role_and_position() {
        let request = request();
        let relative = argv(&request);
        let workspace = Path::new(WORKSPACE);
        let absolute = absolute_media_arguments(&relative, workspace, false).unwrap();
        let request = request.into();
        let mut inputs = [
            test_input("image", 0, "image/png", "a"),
            test_input("image", 1, "image/jpeg", "c"),
            test_input("video", 2, "video/mp4", "d"),
            test_input("audio", 3, "audio/wav", "b"),
        ];
        for (path, kind, position, media_type, duration_ms) in [
            (
                "video-0.mp4",
                StagedInputKindV1::Video,
                2,
                "video/mp4",
                2_400,
            ),
            (
                "audio-0.wav",
                StagedInputKindV1::Audio,
                3,
                "audio/wav",
                2_000,
            ),
        ] {
            let mut raw = receipt(path);
            raw["fail_reason"] = json!(format!(
                "{UPLOAD_FAILURE_PREFIX}{path}{TIMED_UPLOAD_FAILURE_SUFFIX}"
            ));
            assert!(matches(raw.to_string().as_bytes(), &relative, &request));
            assert!(!matches(raw.to_string().as_bytes(), &absolute, &request));
            let full_path = workspace.join(path);
            raw["fail_reason"] = json!(format!(
                "{UPLOAD_FAILURE_PREFIX}{}{TIMED_UPLOAD_FAILURE_SUFFIX}",
                full_path.display()
            ));
            let failure = matches_material_upload_failure(
                raw.to_string().as_bytes(),
                REVIEWED_DREAMINA_SHA256,
                workspace,
                &absolute,
                &request,
            )
            .unwrap();
            assert_eq!(failure.kind, kind);
            assert_eq!(failure.position, position);
            assert_eq!(failure.ordinal, 1);
            assert_eq!(failure.duration_ms, Some(duration_ms));
            assert_eq!(
                failure.frozen_input(&inputs).unwrap().media_type(),
                media_type
            );
            assert!(!matches(raw.to_string().as_bytes(), &relative, &request));

            inputs.swap(2, 3);
            assert!(failure.frozen_input(&inputs).is_none());
            inputs.swap(2, 3);
        }
        for wrong_reason in [
            format!("{UPLOAD_FAILURE_PREFIX}input-0.png{TIMED_UPLOAD_FAILURE_SUFFIX}"),
            format!("{UPLOAD_FAILURE_PREFIX}unknown.wav{TIMED_UPLOAD_FAILURE_SUFFIX}"),
            format!("{UPLOAD_FAILURE_PREFIX}audio-0.wav\": upload video/audio: network error"),
        ] {
            let mut bad = receipt("audio-0.wav");
            bad["fail_reason"] = json!(wrong_reason);
            assert!(!matches(bad.to_string().as_bytes(), &relative, &request));
        }
    }

    #[test]
    fn material_upload_failure_binds_native_omission_for_trimmed_empty_multimodal_prompt() {
        let request = request_with_prompt(" \n\t");
        let argv = argv(&request);
        let request = request.into();
        let mut raw = receipt("input-0.png");
        raw.as_object_mut().unwrap().remove("prompt");
        assert!(matches(raw.to_string().as_bytes(), &argv, &request));
        for invalid_prompt in [json!(""), json!(null), json!(" \n\t"), json!(PROMPT)] {
            raw["prompt"] = invalid_prompt;
            assert!(!matches(raw.to_string().as_bytes(), &argv, &request));
        }
    }
}
