use std::{ffi::OsString, fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};

use image_cli_runtime::{
    CliRuntime, NoopSpawnObserver, ReceiptCliPolicy, VerifiedExecutable, WorkingDirectory,
};
use image_provider_contracts::{CallbackMode, CancellationMode, all_provider_roadmap};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use super::*;

const IMAGE_MODELS: [ImageModelVersion; 8] = [
    ImageModelVersion::V3_0,
    ImageModelVersion::V3_1,
    ImageModelVersion::V4_0,
    ImageModelVersion::V4_1,
    ImageModelVersion::V4_5,
    ImageModelVersion::V4_6,
    ImageModelVersion::V4_7,
    ImageModelVersion::V5_0,
];

const VIDEO_MODELS: [VideoModelVersion; 5] = [
    VideoModelVersion::Seedance2_0,
    VideoModelVersion::Seedance2_0Fast,
    VideoModelVersion::Seedance2_0Vip,
    VideoModelVersion::Seedance2_0FastVip,
    VideoModelVersion::Seedance2_0Mini,
];

#[test]
fn image_request_enforces_count_and_model_resolution_boundaries() {
    for model in IMAGE_MODELS {
        for count in [1, 10] {
            let expected = match model {
                ImageModelVersion::V3_0 | ImageModelVersion::V3_1 => {
                    [ImageResolution::K1, ImageResolution::K2].as_slice()
                }
                _ => [ImageResolution::K2, ImageResolution::K4].as_slice(),
            };
            for resolution in expected {
                assert!(
                    TextToImageRequestV1::new(
                        "prompt",
                        model,
                        ImageRatio::R1x1,
                        *resolution,
                        count,
                    )
                    .is_ok()
                );
            }
        }
    }

    assert!(matches!(
        TextToImageRequestV1::new(
            "prompt",
            ImageModelVersion::V3_0,
            ImageRatio::R1x1,
            ImageResolution::K2,
            0,
        ),
        Err(RequestValidationError::InvalidGenerateNum(0))
    ));
    assert!(matches!(
        TextToImageRequestV1::new(
            "prompt",
            ImageModelVersion::V5_0,
            ImageRatio::R1x1,
            ImageResolution::K2,
            11,
        ),
        Err(RequestValidationError::InvalidGenerateNum(11))
    ));
    assert!(matches!(
        TextToImageRequestV1::new(
            "prompt",
            ImageModelVersion::V3_1,
            ImageRatio::R1x1,
            ImageResolution::K4,
            1,
        ),
        Err(RequestValidationError::UnsupportedImageResolution { .. })
    ));
    assert!(matches!(
        TextToImageRequestV1::new(
            "prompt",
            ImageModelVersion::V4_7,
            ImageRatio::R1x1,
            ImageResolution::K1,
            1,
        ),
        Err(RequestValidationError::UnsupportedImageResolution { .. })
    ));
}

#[test]
fn every_image_ratio_projects_to_the_official_value() {
    let models = [
        (ImageModelVersion::V3_0, "3.0"),
        (ImageModelVersion::V3_1, "3.1"),
        (ImageModelVersion::V4_0, "4.0"),
        (ImageModelVersion::V4_1, "4.1"),
        (ImageModelVersion::V4_5, "4.5"),
        (ImageModelVersion::V4_6, "4.6"),
        (ImageModelVersion::V4_7, "4.7"),
        (ImageModelVersion::V5_0, "5.0"),
    ];
    for (model, expected) in models {
        assert_eq!(model.as_str(), expected);
    }

    let ratios = [
        (ImageRatio::R21x9, "21:9"),
        (ImageRatio::R16x9, "16:9"),
        (ImageRatio::R3x2, "3:2"),
        (ImageRatio::R4x3, "4:3"),
        (ImageRatio::R1x1, "1:1"),
        (ImageRatio::R3x4, "3:4"),
        (ImageRatio::R2x3, "2:3"),
        (ImageRatio::R9x16, "9:16"),
    ];
    for (ratio, expected) in ratios {
        assert_eq!(ratio.as_str(), expected);
    }

    assert_eq!(ImageResolution::K1.as_str(), "1k");
    assert_eq!(ImageResolution::K2.as_str(), "2k");
    assert_eq!(ImageResolution::K4.as_str(), "4k");
}

#[test]
fn video_request_enforces_duration_and_vip_resolution_boundaries() {
    for model in VIDEO_MODELS {
        for duration in [4, 15] {
            assert!(
                TextToVideoRequestV1::new(
                    "prompt",
                    model,
                    VideoRatio::R16x9,
                    duration,
                    VideoResolution::P720,
                )
                .is_ok()
            );
        }
    }

    for resolution in [VideoResolution::P1080, VideoResolution::K4] {
        assert!(
            TextToVideoRequestV1::new(
                "prompt",
                VideoModelVersion::Seedance2_0Vip,
                VideoRatio::R16x9,
                4,
                resolution,
            )
            .is_ok()
        );
    }

    for model in [
        VideoModelVersion::Seedance2_0,
        VideoModelVersion::Seedance2_0Fast,
        VideoModelVersion::Seedance2_0FastVip,
        VideoModelVersion::Seedance2_0Mini,
    ] {
        for resolution in [VideoResolution::P1080, VideoResolution::K4] {
            assert!(matches!(
                TextToVideoRequestV1::new("prompt", model, VideoRatio::R16x9, 4, resolution,),
                Err(RequestValidationError::UnsupportedVideoResolution { .. })
            ));
        }
    }

    for duration in [3, 16] {
        assert!(matches!(
            TextToVideoRequestV1::new(
                "prompt",
                VideoModelVersion::Seedance2_0,
                VideoRatio::R16x9,
                duration,
                VideoResolution::P720,
            ),
            Err(RequestValidationError::InvalidVideoDuration(value)) if value == duration
        ));
    }
}

#[test]
fn every_video_ratio_and_model_projects_to_the_official_value() {
    let ratios = [
        (VideoRatio::R1x1, "1:1"),
        (VideoRatio::R3x4, "3:4"),
        (VideoRatio::R16x9, "16:9"),
        (VideoRatio::R4x3, "4:3"),
        (VideoRatio::R9x16, "9:16"),
        (VideoRatio::R21x9, "21:9"),
    ];
    for (ratio, expected) in ratios {
        assert_eq!(ratio.as_str(), expected);
    }

    assert_eq!(VideoModelVersion::Seedance2_0.as_str(), "seedance2.0");
    assert_eq!(
        VideoModelVersion::Seedance2_0Fast.as_str(),
        "seedance2.0fast"
    );
    assert_eq!(
        VideoModelVersion::Seedance2_0Vip.as_str(),
        "seedance2.0_vip"
    );
    assert_eq!(
        VideoModelVersion::Seedance2_0FastVip.as_str(),
        "seedance2.0fast_vip"
    );
    assert_eq!(
        VideoModelVersion::Seedance2_0Mini.as_str(),
        "seedance2.0mini"
    );
    assert_eq!(VideoResolution::P720.as_str(), "720p");
    assert_eq!(VideoResolution::P1080.as_str(), "1080p");
    assert_eq!(VideoResolution::K4.as_str(), "4k");
}

#[test]
fn generation_argv_is_shell_free_and_keeps_injection_text_in_one_argument() {
    let injection = r#"image; touch /tmp/not-created && $(id) "quoted""#;
    let image = TextToImageRequestV1::new(
        injection,
        ImageModelVersion::V4_7,
        ImageRatio::R21x9,
        ImageResolution::K4,
        10,
    )
    .unwrap();
    assert_eq!(
        image.to_argv(),
        vec![
            OsString::from("text2image"),
            OsString::from("--prompt"),
            OsString::from(injection),
            OsString::from("--model_version"),
            OsString::from("4.7"),
            OsString::from("--ratio"),
            OsString::from("21:9"),
            OsString::from("--resolution_type"),
            OsString::from("4k"),
            OsString::from("--generate_num"),
            OsString::from("10"),
            OsString::from("--poll=0"),
        ]
    );

    let video = TextToVideoRequestV1::new(
        injection,
        VideoModelVersion::Seedance2_0Vip,
        VideoRatio::R9x16,
        15,
        VideoResolution::K4,
    )
    .unwrap();
    assert_eq!(
        video.to_argv(),
        vec![
            OsString::from("text2video"),
            OsString::from("--prompt"),
            OsString::from(injection),
            OsString::from("--model_version"),
            OsString::from("seedance2.0_vip"),
            OsString::from("--ratio"),
            OsString::from("9:16"),
            OsString::from("--duration"),
            OsString::from("15"),
            OsString::from("--video_resolution"),
            OsString::from("4k"),
            OsString::from("--poll=0"),
        ]
    );
}

#[test]
fn query_result_projects_submit_id_and_download_directory_exactly() {
    let request = QueryResultRequestV1::new("task-1", PathBuf::from("/tmp/output dir")).unwrap();
    assert_eq!(
        request.to_argv(),
        vec![
            OsString::from("query_result"),
            OsString::from("--submit_id"),
            OsString::from("task-1"),
            OsString::from("--download_dir"),
            OsString::from("/tmp/output dir"),
        ]
    );
}

#[test]
fn request_text_and_query_boundaries_fail_closed() {
    assert!(matches!(
        TextToImageRequestV1::new(
            "   ",
            ImageModelVersion::V4_0,
            ImageRatio::R1x1,
            ImageResolution::K2,
            1,
        ),
        Err(RequestValidationError::EmptyPrompt)
    ));
    assert!(matches!(
        TextToVideoRequestV1::new(
            "bad\0prompt",
            VideoModelVersion::Seedance2_0,
            VideoRatio::R16x9,
            4,
            VideoResolution::P720,
        ),
        Err(RequestValidationError::InvalidPrompt)
    ));
    assert!(matches!(
        QueryResultRequestV1::new(" ", "output"),
        Err(RequestValidationError::EmptySubmitId)
    ));
    assert!(matches!(
        QueryResultRequestV1::new("submit", PathBuf::new()),
        Err(RequestValidationError::EmptyDownloadDirectory)
    ));
    assert!(matches!(
        QueryResultRequestV1::new("submit;$(id)", "/tmp/output"),
        Err(RequestValidationError::InvalidSubmitId)
    ));
    assert!(matches!(
        QueryResultRequestV1::new("task-1", "output"),
        Err(RequestValidationError::DownloadDirectoryNotAbsolute)
    ));
    assert!(matches!(
        QueryResultRequestV1::new("task-1", "/tmp/staging/../escape"),
        Err(RequestValidationError::InvalidDownloadDirectory)
    ));
}

#[test]
fn receipt_accepts_only_querying_or_success_with_a_nonempty_submit_id() {
    let querying = parse_receipt(br#"{"submit_id":"task-1","gen_status":"querying"}"#).unwrap();
    assert_eq!(querying.submit_id(), "task-1");
    assert_eq!(querying.status(), AcceptedStatus::Querying);

    let success = parse_receipt(
        br#"{"submit_id":"task-2","gen_status":"success","result_json":{"images":[]}}"#,
    )
    .unwrap();
    assert_eq!(success.submit_id(), "task-2");
    assert_eq!(success.status(), AcceptedStatus::Success);

    for submit_id in ["", "   "] {
        let payload = format!(r#"{{"submit_id":"{submit_id}","gen_status":"success"}}"#);
        assert!(matches!(
            parse_receipt(payload.as_bytes()),
            Err(ReceiptError::EmptySubmitId)
        ));
    }
    assert!(matches!(
        parse_receipt(br#"{"submit_id":"task/1","gen_status":"success"}"#),
        Err(ReceiptError::InvalidSubmitId)
    ));
}

#[test]
fn failed_receipt_returns_a_single_line_sanitized_reason() {
    let error = parse_receipt(
        br#"{"submit_id":"task-1","gen_status":"fail","fail_reason":"  denied\n\u001b[31m retry\t later  "}"#,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ReceiptError::GenerationFailed { reason } if reason == "denied [31m retry later"
    ));

    let error = parse_receipt(br#"{"gen_status":"fail","fail_reason":"\n\t"}"#).unwrap_err();
    assert!(matches!(
        error,
        ReceiptError::GenerationFailed { reason }
            if reason == "Dreamina returned no failure reason"
    ));
}

#[test]
fn receipt_rejects_unknown_status_trailing_json_non_json_and_oversize_input() {
    assert!(matches!(
        parse_receipt(br#"{"submit_id":"task-1","gen_status":"queued"}"#),
        Err(ReceiptError::UnknownStatus(status)) if status == "queued"
    ));
    assert!(matches!(
        parse_receipt(br#"{"submit_id":"task-1","gen_status":"success"}{"gen_status":"fail"}"#),
        Err(ReceiptError::InvalidJson(_))
    ));
    assert!(matches!(
        parse_receipt(b"not json"),
        Err(ReceiptError::InvalidJson(_))
    ));

    let oversized = vec![b' '; MAX_RECEIPT_BYTES + 1];
    assert!(matches!(
        parse_receipt(&oversized),
        Err(ReceiptError::InputTooLarge { actual }) if actual == MAX_RECEIPT_BYTES + 1
    ));
}

#[test]
fn capability_metadata_is_polling_only() {
    assert!(
        all_provider_roadmap()
            .iter()
            .any(|provider| provider.id() == PROVIDER_ID)
    );
    assert_eq!(
        DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1.callback,
        CallbackMode::Unsupported
    );
    assert_eq!(
        DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1.cancellation,
        CancellationMode::Unsupported
    );
}

#[tokio::test]
async fn policy_runs_a_digest_pinned_cli_with_an_isolated_home_and_bounded_receipt() {
    let root = TempDir::new().expect("temp directory");
    let workspace = root.path().join("workspace");
    let account_home = root.path().join("account-home");
    fs::create_dir(&workspace).expect("workspace");
    fs::create_dir(&account_home).expect("account home");
    let executable_path = root.path().join("dreamina");
    let executable_bytes = br#"#!/bin/sh
printf '%s\n' "$@" > "$TMPDIR/argv"
printf '%s' "$HOME" > "$TMPDIR/home"
printf '{"submit_id":"task-1","gen_status":"querying"}'
"#;
    fs::write(&executable_path, executable_bytes).expect("fake CLI");
    fs::set_permissions(&executable_path, fs::Permissions::from_mode(0o500))
        .expect("executable permissions");
    let digest: [u8; 32] = Sha256::digest(executable_bytes).into();
    let executable = VerifiedExecutable::new_with_sha256(&executable_path, digest)
        .expect("digest-pinned executable");
    let working_directory = WorkingDirectory::new(&workspace).expect("verified workspace");
    let account_home = WorkingDirectory::new(&account_home).expect("verified account home");
    assert!(matches!(
        DreaminaCliPolicyV1::new(
            executable.clone(),
            working_directory.clone(),
            working_directory.clone(),
            Duration::from_secs(2),
            Duration::from_millis(50),
        ),
        Err(DreaminaCliPolicyError::OverlappingDirectories)
    ));
    let policy = DreaminaCliPolicyV1::new(
        executable,
        working_directory,
        account_home.clone(),
        Duration::from_secs(2),
        Duration::from_millis(50),
    )
    .expect("policy");
    let request = TextToImageRequestV1::new(
        "image; $(touch should-not-run)",
        ImageModelVersion::V5_0,
        ImageRatio::R1x1,
        ImageResolution::K2,
        1,
    )
    .expect("request");
    let batch = TextToImageRequestV1::new(
        "two outputs",
        ImageModelVersion::V5_0,
        ImageRatio::R1x1,
        ImageResolution::K2,
        2,
    )
    .expect("official batch request");
    assert!(matches!(
        ReceiptCliPolicy::command(&policy, &batch.into()),
        Err(DreaminaCliPolicyError::BatchSubmissionUnsupported)
    ));
    let result = CliRuntime::new(policy)
        .run_receipt(&request.into(), &mut NoopSpawnObserver)
        .await
        .expect("accepted receipt");

    assert_eq!(result.receipt.submit_id(), "task-1");
    assert_eq!(
        fs::read_to_string(workspace.join("home")).expect("captured home"),
        account_home.path().to_string_lossy()
    );
    let argv = fs::read_to_string(workspace.join("argv")).expect("captured argv");
    assert!(argv.contains("image; $(touch should-not-run)\n"));
    assert!(argv.ends_with("--poll=0\n"));
    assert!(!workspace.join("should-not-run").exists());
}
