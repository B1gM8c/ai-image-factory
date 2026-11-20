use std::path::Path;

use image_provider_dreamina_cli::{
    DreaminaSubmitPayloadV1, DreaminaSubmitRequestV1, StagedImageV1, VideoModelVersion,
    parse_submit_command,
};
use image_provider_sdk::SingleOutputCommand;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    executor::ExecutorInputObject,
    provider_tasks::{
        GatedCliProcessOutcome, GatedCliProcessTerminal, GatedCliReceiptEvidence,
        ProviderSubmitDriverCall,
    },
};

use super::submit_recovery::{absolute_media_arguments, has_no_acceptance_evidence};

// In this exact Linux Dreamina 1.4.21 build, both reviewed video handlers
// return directly on an image upload error, before the generation client:
// multimodal2video at 0x116eb29 and image2video at 0x116c46d.
const REVIEWED_DREAMINA_SHA256: &str =
    "0a5fdc262ab44669e7db4ce67e6543ece6daf405804aca3d1d17d1345c146f69";
const UPLOAD_FAILURE_PREFIX: &str = "upload resource \"";
const UPLOAD_FAILURE_SUFFIX: &str =
    "\": upload image: upload phase, no file upload, please check log for more details";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FailedUploadReceipt {
    submit_id: String,
    gen_status: String,
    prompt: String,
    fail_reason: String,
}

pub(super) struct MaterialUploadFailure {
    workflow: &'static str,
    model: VideoModelVersion,
    image_index: usize,
    image: StagedImageV1,
}

impl MaterialUploadFailure {
    fn frozen_input<'a>(
        &self,
        inputs: &'a [ExecutorInputObject],
    ) -> Option<&'a ExecutorInputObject> {
        inputs.get(self.image_index).filter(|input| {
            usize::from(input.index()) == self.image_index
                && input.role() == "image"
                && matches!(
                    input.media_type(),
                    "image/png" | "image/jpeg" | "image/webp"
                )
                && input.blob().sha256_hex == self.image.sha256()
        })
    }

    pub(super) fn log(
        &self,
        call: &ProviderSubmitDriverCall<DreaminaSubmitPayloadV1>,
        evidence: &GatedCliReceiptEvidence,
    ) {
        let input = self.frozen_input(call.inputs());
        tracing::warn!(
            submission_id = %call.intent().submission_id,
            error_code = "dreamina_material_upload_failed",
            workflow = self.workflow,
            model = self.model.as_str(),
            image_ordinal = self.image_index + 1,
            image_width = self.image.width(),
            image_height = self.image.height(),
            image_media_type = input.map(ExecutorInputObject::media_type),
            image_byte_size = input.map(|input| input.blob().byte_size),
            cli_elapsed_ms = evidence.finished_at_ms() - evidence.started_at_ms(),
            "Dreamina stopped before video submission; CLI did not report the underlying upload error"
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
    let (argv, prompt, images, model, workflow) = match request {
        DreaminaSubmitRequestV1::MultimodalToVideo(request) => (
            request.to_argv(),
            request.prompt(),
            request.images(),
            request.model(),
            "multimodal2video",
        ),
        DreaminaSubmitRequestV1::ImageToVideo(request) => (
            request.to_argv(),
            request.options().prompt(),
            std::slice::from_ref(request.image()),
            request.options().model(),
            "image2video",
        ),
        _ => return None,
    };
    if candidate_id.is_nil()
        || candidate_id.hyphenated().to_string() != receipt.submit_id
        || receipt.gen_status != "fail"
        || receipt.prompt != prompt
    {
        return None;
    }
    let path = receipt
        .fail_reason
        .strip_prefix(UPLOAD_FAILURE_PREFIX)
        .and_then(|reason| reason.strip_suffix(UPLOAD_FAILURE_SUFFIX))?;
    let relative_argv = argv
        .into_iter()
        .map(|arg| arg.into_string())
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    let image_index = if observed_argv == relative_argv {
        images.iter().position(|image| image.filename() == path)
    } else if matches!(request, DreaminaSubmitRequestV1::MultimodalToVideo(_))
        && workspace.is_absolute()
        && absolute_media_arguments(&relative_argv, workspace, false)
            .is_ok_and(|argv| argv == observed_argv)
    {
        // Only the newer normal multimodal projection replaces basenames with
        // absolute paths under the same isolated workspace.
        images
            .iter()
            .position(|image| workspace.join(image.filename()).as_os_str() == path)
    } else {
        None
    }?;
    Some(MaterialUploadFailure {
        workflow,
        model,
        image_index,
        image: images[image_index].clone(),
    })
}

#[cfg(test)]
mod tests {
    use image_provider_dreamina_cli::{
        ImageToVideoRequestV1, MultimodalToVideoRequestV1, ReferenceVideoOptionsV1,
        StagedTimedMediaV1, VideoRatio, VideoResolution,
    };
    use serde_json::{Value, json};

    use crate::input_blobs::{InputBlobKey, InputBlobRef};

    use super::*;

    const TEST_ID: &str = "a12bdcaa-9068-4c93-8653-2c0f4813643f";
    const WORKSPACE: &str = "/frozen/private/.provider-submit-fixture";
    const PROMPT: &str = "画面\n--image";

    fn request() -> MultimodalToVideoRequestV1 {
        MultimodalToVideoRequestV1::new(
            vec![
                StagedImageV1::new("input-0.png", "a".repeat(64), 1600, 900).unwrap(),
                StagedImageV1::new("input-1.jpg", "c".repeat(64), 1674, 940).unwrap(),
            ],
            vec![],
            vec![StagedTimedMediaV1::new("audio-0.wav", "b".repeat(64), 2_000).unwrap()],
            PROMPT,
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

    fn receipt(path: &str) -> Value {
        json!({
            "submit_id": TEST_ID,
            "gen_status": "fail",
            "prompt": PROMPT,
            "fail_reason": format!("{UPLOAD_FAILURE_PREFIX}{path}{UPLOAD_FAILURE_SUFFIX}"),
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
        assert_eq!(failure.model, VideoModelVersion::Seedance2_5);
        assert_eq!(failure.image_index + 1, 2);
        assert_eq!((failure.image.width(), failure.image.height()), (1674, 940));
        let input = |index, media_type, digest| {
            ExecutorInputObject::new(
                InputBlobRef {
                    key: InputBlobKey {
                        admission_session_id: Uuid::new_v4(),
                        input_id: Uuid::new_v4(),
                    },
                    storage_backend: "test".into(),
                    object_key: "fixture".into(),
                    sha256_hex: digest,
                    byte_size: 1_767_471,
                },
                "image",
                index,
                media_type,
            )
            .unwrap()
        };
        let mut inputs = [
            input(0, "image/png", "a".repeat(64)),
            input(1, "image/jpeg", "c".repeat(64)),
        ];
        let selected = failure.frozen_input(&inputs).unwrap();
        assert_eq!(selected.media_type(), "image/jpeg");
        assert_eq!(selected.blob().byte_size, 1_767_471);
        assert!(failure.frozen_input(&[]).is_none());
        inputs[1] = input(1, "image/jpeg", "d".repeat(64));
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
}
