use super::*;
use image_cli_runtime::WorkingDirectory;
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    time::Duration,
};

const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
fn image(name: &str) -> StagedImageV1 {
    StagedImageV1::new(name, SHA, 1600, 900).unwrap()
}
fn images(count: usize) -> Vec<StagedImageV1> {
    (0..count)
        .map(|i| image(&format!("image-{i}.png")))
        .collect()
}
fn timed(prefix: &str, count: usize, ms: u64) -> Vec<StagedTimedMediaV1> {
    (0..count)
        .map(|i| StagedTimedMediaV1::new(format!("{prefix}-{i}.bin"), SHA, ms).unwrap())
        .collect()
}
fn image_parameters(model: ImageModelVersion, count: u8) -> TextToImageRequestV1 {
    TextToImageRequestV1::new(
        "paint it",
        model,
        ImageRatio::R16x9,
        ImageResolution::K2,
        count,
    )
    .unwrap()
}
fn options(
    model: VideoModelVersion,
    ratio: Option<VideoRatio>,
    duration: u8,
    resolution: VideoResolution,
) -> Result<ReferenceVideoOptionsV1, RequestValidationError> {
    ReferenceVideoOptionsV1::new("camera pan", model, ratio, duration, resolution)
}
fn multimodal(
    model: VideoModelVersion,
    images: Vec<StagedImageV1>,
    videos: Vec<StagedTimedMediaV1>,
    audios: Vec<StagedTimedMediaV1>,
) -> Result<MultimodalToVideoRequestV1, RequestValidationError> {
    MultimodalToVideoRequestV1::new(
        images,
        videos,
        audios,
        "",
        model,
        VideoRatio::R16x9,
        5,
        VideoResolution::P720,
    )
}
fn arg_value<'a>(args: &'a [OsString], flag: &str) -> &'a std::ffi::OsStr {
    args[args.iter().position(|arg| arg == flag).unwrap() + 1].as_os_str()
}

#[test]
fn staged_inputs_accept_only_safe_basenames_and_valid_frozen_metadata() {
    for name in [
        "",
        ".",
        "..",
        "../secret",
        "/tmp/secret",
        "a/b",
        "a\\b",
        "a,b",
        "--flag",
        "a\0b",
        "a b",
        "$(id)",
    ] {
        assert_eq!(
            LocalInputName::new(name),
            Err(RequestValidationError::InvalidStagedFilename),
            "{name:?}"
        );
    }
    assert!(LocalInputName::new("input-00_abcdef.mp4").is_ok());
    assert!(LocalInputName::new("a".repeat(256)).is_err());
    assert_eq!(
        StagedImageV1::new("a.png", "A".repeat(64), 1, 1),
        Err(RequestValidationError::InvalidStagedSha256)
    );
    assert_eq!(
        StagedImageV1::new("a.png", SHA, 0, 1),
        Err(RequestValidationError::InvalidStagedImageDimensions)
    );
    assert_eq!(
        StagedTimedMediaV1::new("a.mp4", SHA, 0),
        Err(RequestValidationError::InvalidStagedMediaDuration)
    );
}

#[test]
fn official_image_resolution_matrix_and_custom_geometry_are_enforced() {
    for resolution in [
        ImageResolution::K1_5,
        ImageResolution::K2,
        ImageResolution::K4,
    ] {
        let request = TextToImageRequestV1::new(
            "p",
            ImageModelVersion::V5_0Pro,
            ImageRatio::R1x1,
            resolution,
            1,
        )
        .unwrap();
        let request = DreaminaSubmitRequestV1::from(request);
        assert_eq!(
            parse_submit_command(&encode_submit_command(request.clone())).unwrap(),
            request
        );
    }
    assert!(
        TextToImageRequestV1::new(
            "p",
            ImageModelVersion::V5_0Pro,
            ImageRatio::R1x1,
            ImageResolution::K1,
            1
        )
        .is_err()
    );
    for model in [ImageModelVersion::V3_0, ImageModelVersion::V3_1] {
        assert_eq!(
            TextToImageRequestV1::new_custom("p", model, 1024, 1024, ImageResolution::K1, 1),
            Err(RequestValidationError::UnsupportedCustomImageResolution)
        );
        assert!(
            TextToImageRequestV1::new_custom("p", model, 1024, 1024, ImageResolution::K2, 1)
                .is_ok()
        );
    }
    for (width, height, accepted) in [
        (972, 972, true),
        (2268, 972, true),
        (1536, 1536, true),
        (971, 1000, false),
        (2269, 972, false),
        (1537, 1537, false),
    ] {
        assert_eq!(
            TextToImageRequestV1::new_custom(
                "p",
                ImageModelVersion::V5_0Pro,
                width,
                height,
                ImageResolution::K1_5,
                1
            )
            .is_ok(),
            accepted
        );
    }
}

#[test]
fn image_edit_model_counts_and_single_output_projection_are_preserved() {
    for model in [
        ImageModelVersion::V4_0,
        ImageModelVersion::V4_1,
        ImageModelVersion::V4_5,
        ImageModelVersion::V4_6,
        ImageModelVersion::V4_7,
        ImageModelVersion::V5_0,
        ImageModelVersion::V5_0Pro,
    ] {
        for count in [1, 10] {
            assert!(ImageToImageRequestV1::new(images(count), image_parameters(model, 1)).is_ok());
        }
    }
    for model in [ImageModelVersion::V3_0, ImageModelVersion::V3_1] {
        assert_eq!(
            ImageToImageRequestV1::new(images(1), image_parameters(model, 1)),
            Err(RequestValidationError::UnsupportedWorkflowModel)
        );
    }
    for count in [0, 11] {
        assert_eq!(
            ImageToImageRequestV1::new(images(count), image_parameters(ImageModelVersion::V5_0, 1)),
            Err(RequestValidationError::InvalidInputCount)
        );
    }
    assert_eq!(
        ImageToImageRequestV1::new(
            vec![image("same.png"), image("same.png")],
            image_parameters(ImageModelVersion::V5_0, 1)
        ),
        Err(RequestValidationError::DuplicateStagedFilename)
    );
    let request: DreaminaSubmitRequestV1 =
        ImageToImageRequestV1::new(images(1), image_parameters(ImageModelVersion::V5_0, 10))
            .unwrap()
            .into();
    assert_eq!(request.output_count(), 10);
    assert_eq!(
        DreaminaSubmitPayloadV1::new(SHA, request.clone()),
        Err(DreaminaSubmitCommandError::BatchSubmissionUnsupported)
    );
    assert!(matches!(
        request.argv(),
        Err(DreaminaCliPolicyError::BatchSubmissionUnsupported)
    ));
    assert_eq!(request.into_single_output().output_count(), 1);
}

#[test]
fn upscale_exposes_only_official_parameters() {
    for resolution in [
        UpscaleResolution::K2,
        UpscaleResolution::K4,
        UpscaleResolution::K8,
    ] {
        let args = ImageUpscaleRequestV1::new(image("source.png"), resolution).to_argv();
        assert_eq!(
            args,
            [
                "image_upscale",
                "--image",
                "source.png",
                "--resolution_type",
                resolution.as_str(),
                "--poll=0"
            ]
            .map(OsString::from)
        );
    }
}

#[test]
fn reference_video_models_enforce_workflow_and_duration_boundaries() {
    let models = [
        (VideoModelVersion::Seedance1_0Fast, 5, 10),
        (VideoModelVersion::Seedance1_5Pro, 5, 12),
        (VideoModelVersion::Seedance2_0, 4, 15),
        (VideoModelVersion::Seedance2_0Fast, 4, 15),
        (VideoModelVersion::Seedance2_0Vip, 4, 15),
        (VideoModelVersion::Seedance2_0FastVip, 4, 15),
        (VideoModelVersion::Seedance2_0Mini, 4, 15),
        (VideoModelVersion::Seedance2_5, 4, 30),
    ];
    for (model, min, max) in models {
        for duration in [min, max] {
            let options = options(model, None, duration, VideoResolution::P720).unwrap();
            assert!(ImageToVideoRequestV1::new(image("first.png"), options.clone()).is_ok());
            assert_eq!(
                FramesToVideoRequestV1::new(image("first.png"), image("last.png"), options).is_ok(),
                model != VideoModelVersion::Seedance1_0Fast
            );
        }
        for duration in [min - 1, max + 1] {
            assert_eq!(
                options(model, None, duration, VideoResolution::P720),
                Err(RequestValidationError::InvalidVideoDuration(duration))
            );
        }
        for resolution in [
            VideoResolution::P480,
            VideoResolution::P720,
            VideoResolution::P1080,
            VideoResolution::K4,
        ] {
            let accepted = match model {
                VideoModelVersion::Seedance2_5 => resolution != VideoResolution::K4,
                VideoModelVersion::Seedance2_0Vip => resolution != VideoResolution::P480,
                _ => resolution == VideoResolution::P720,
            };
            assert_eq!(options(model, None, min, resolution).is_ok(), accepted);
        }
    }
    for model in [
        VideoModelVersion::Seedance1_0Fast,
        VideoModelVersion::Seedance1_5Pro,
    ] {
        assert_eq!(
            TextToVideoRequestV1::new("p", model, VideoRatio::R16x9, 5, VideoResolution::P720),
            Err(RequestValidationError::UnsupportedWorkflowModel)
        );
        assert_eq!(
            multimodal(model, images(1), vec![], vec![]),
            Err(RequestValidationError::UnsupportedWorkflowModel)
        );
    }
}

#[test]
fn reference_ratio_is_omitted_or_checked_against_the_model_rules() {
    assert_eq!(
        options(
            VideoModelVersion::Seedance2_5,
            Some(VideoRatio::R16x9),
            5,
            VideoResolution::P480
        ),
        Err(RequestValidationError::ReferenceRatioUnsupported)
    );
    for model in [
        VideoModelVersion::Seedance1_0Fast,
        VideoModelVersion::Seedance1_5Pro,
    ] {
        assert!(
            ImageToVideoRequestV1::new(
                image("first.png"),
                options(model, Some(VideoRatio::R16x9), 5, VideoResolution::P720).unwrap()
            )
            .is_ok()
        );
        assert_eq!(
            ImageToVideoRequestV1::new(
                image("first.png"),
                options(model, Some(VideoRatio::R1x1), 5, VideoResolution::P720).unwrap()
            ),
            Err(RequestValidationError::ReferenceRatioMismatch)
        );
    }
    assert!(
        ImageToVideoRequestV1::new(
            image("first.png"),
            options(
                VideoModelVersion::Seedance2_0,
                Some(VideoRatio::R1x1),
                5,
                VideoResolution::P720
            )
            .unwrap()
        )
        .is_ok()
    );
    let args = ImageToVideoRequestV1::new(
        image("first.png"),
        options(
            VideoModelVersion::Seedance2_5,
            None,
            5,
            VideoResolution::P480,
        )
        .unwrap(),
    )
    .unwrap()
    .to_argv();
    assert!(!args.contains(&OsString::from("--ratio")));
}

#[test]
fn multiframe_validates_segments_and_emits_exact_decimal_seconds() {
    let transitions = |count, ms| {
        (0..count)
            .map(|_| VideoTransitionV1::new("move, then turn", ms).unwrap())
            .collect()
    };
    for (count, ms) in [(2, 2000), (3, 1000), (20, 8000)] {
        assert!(
            MultiframeToVideoRequestV1::new(
                images(count),
                transitions(count - 1, ms),
                VideoResolution::P1080
            )
            .is_ok()
        );
    }
    assert_eq!(
        MultiframeToVideoRequestV1::new(images(2), transitions(1, 1000), VideoResolution::P720),
        Err(RequestValidationError::InvalidTransitionDuration)
    );
    assert_eq!(
        MultiframeToVideoRequestV1::new(images(3), transitions(1, 3000), VideoResolution::P720),
        Err(RequestValidationError::InvalidTransitionCount)
    );
    assert_eq!(
        MultiframeToVideoRequestV1::new(images(21), transitions(20, 3000), VideoResolution::P720),
        Err(RequestValidationError::InvalidInputCount)
    );
    for ms in [0, 999, 8001, u32::MAX] {
        assert_eq!(
            VideoTransitionV1::new("p", ms),
            Err(RequestValidationError::InvalidTransitionDuration)
        );
    }
    for resolution in [VideoResolution::P480, VideoResolution::K4] {
        assert_eq!(
            MultiframeToVideoRequestV1::new(images(2), transitions(1, 3000), resolution),
            Err(RequestValidationError::InvalidMultiframeResolution)
        );
    }
    let args =
        MultiframeToVideoRequestV1::new(images(2), transitions(1, 3125), VideoResolution::P720)
            .unwrap()
            .to_argv();
    assert_eq!(arg_value(&args, "--duration"), "3.125");
    assert_eq!(arg_value(&args, "--prompt"), "move, then turn");
    let args =
        MultiframeToVideoRequestV1::new(images(3), transitions(2, 1250), VideoResolution::P720)
            .unwrap()
            .to_argv();
    assert_eq!(
        args.iter().filter(|a| *a == "--transition-prompt").count(),
        2
    );
    assert_eq!(
        args.iter()
            .filter(|a| *a == "--transition-duration")
            .count(),
        2
    );
    assert_eq!(arg_value(&args, "--transition-duration"), "1.25");
    assert!(!args.contains(&OsString::from("--ratio")));
    assert!(!args.contains(&OsString::from("--model_version")));
}

#[test]
fn multimodal_enforces_model_specific_counts_audio_only_and_timing() {
    let old = VideoModelVersion::Seedance2_0Vip;
    let new = VideoModelVersion::Seedance2_5;
    assert!(
        multimodal(
            old,
            images(9),
            timed("video", 2, 2000),
            timed("audio", 1, 2000)
        )
        .is_ok()
    );
    assert!(
        multimodal(
            new,
            images(30),
            timed("video", 10, 2000),
            timed("audio", 10, 2000)
        )
        .is_ok()
    );
    assert!(multimodal(new, vec![], vec![], timed("audio", 1, 2000)).is_ok());
    assert_eq!(
        multimodal(old, vec![], vec![], timed("audio", 1, 2000)),
        Err(RequestValidationError::InvalidInputCount)
    );
    for model in [old, new] {
        assert_eq!(
            multimodal(model, vec![], vec![], vec![]),
            Err(RequestValidationError::InvalidInputCount)
        );
    }
    for (model, count) in [(old, 10), (new, 31)] {
        assert_eq!(
            multimodal(model, images(count), vec![], vec![]),
            Err(RequestValidationError::InvalidInputCount)
        );
    }
    assert_eq!(
        multimodal(
            old,
            images(9),
            timed("video", 3, 2000),
            timed("audio", 1, 2000)
        ),
        Err(RequestValidationError::InvalidInputCount)
    );
    for (model, count, max) in [(old, 3, 15000), (new, 10, 30000)] {
        assert!(
            multimodal(
                model,
                images(1),
                timed("video", count, max / count as u64),
                vec![]
            )
            .is_ok()
        );
        assert_eq!(
            multimodal(
                model,
                images(1),
                timed("video", count, max / count as u64 + 1),
                vec![]
            ),
            Err(RequestValidationError::UnsupportedReferenceDuration)
        );
        for ms in [1, 1999, max + 1, u64::MAX] {
            assert_eq!(
                multimodal(model, images(1), timed("video", 1, ms), vec![]),
                Err(RequestValidationError::UnsupportedReferenceDuration)
            );
            assert_eq!(
                multimodal(model, images(1), vec![], timed("audio", 1, ms)),
                Err(RequestValidationError::UnsupportedReferenceDuration)
            );
        }
        assert!(
            multimodal(
                model,
                images(1),
                timed("video", 1, max),
                timed("audio", 1, max)
            )
            .is_ok()
        );
    }
    let duplicate = StagedTimedMediaV1::new("image-0.png", SHA, 2000).unwrap();
    assert_eq!(
        multimodal(new, images(1), vec![duplicate], vec![]),
        Err(RequestValidationError::DuplicateStagedFilename)
    );
}

fn examples() -> Vec<DreaminaSubmitRequestV1> {
    vec![
        image_parameters(ImageModelVersion::V5_0, 1).into(),
        TextToVideoRequestV1::new(
            "p",
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            5,
            VideoResolution::P480,
        )
        .unwrap()
        .into(),
        ImageToImageRequestV1::new(images(2), image_parameters(ImageModelVersion::V5_0, 1))
            .unwrap()
            .into(),
        ImageUpscaleRequestV1::new(image("source.png"), UpscaleResolution::K8).into(),
        ImageToVideoRequestV1::new(
            image("first.png"),
            options(
                VideoModelVersion::Seedance1_0Fast,
                None,
                5,
                VideoResolution::P720,
            )
            .unwrap(),
        )
        .unwrap()
        .into(),
        FramesToVideoRequestV1::new(
            image("first.png"),
            image("last.png"),
            options(
                VideoModelVersion::Seedance2_5,
                None,
                5,
                VideoResolution::P480,
            )
            .unwrap(),
        )
        .unwrap()
        .into(),
        MultiframeToVideoRequestV1::new(
            images(3),
            vec![
                VideoTransitionV1::new("one", 1250).unwrap(),
                VideoTransitionV1::new("two", 3500).unwrap(),
            ],
            VideoResolution::P1080,
        )
        .unwrap()
        .into(),
        multimodal(
            VideoModelVersion::Seedance2_5,
            images(1),
            timed("video", 1, 2000),
            timed("audio", 1, 3000),
        )
        .unwrap()
        .into(),
    ]
}

#[test]
fn all_eight_workflows_round_trip_canonical_argv_and_staged_input_order() {
    for request in examples() {
        let encoded = encode_submit_command(request.clone());
        let decoded = parse_submit_command(&encoded).unwrap();
        assert_eq!(decoded, request);
        assert_eq!(encode_submit_command(decoded.clone()), encoded);
        assert_eq!(decoded.argv().unwrap(), request.argv().unwrap());
        assert_eq!(
            decoded.input_names(),
            request
                .staged_inputs()
                .iter()
                .map(|i| i.filename)
                .collect::<Vec<_>>()
        );
    }
    let request = examples().pop().unwrap();
    let inputs = request.staged_inputs();
    assert_eq!(
        inputs.iter().map(|i| (i.kind, i.index)).collect::<Vec<_>>(),
        vec![
            (StagedInputKindV1::Image, 0),
            (StagedInputKindV1::Video, 0),
            (StagedInputKindV1::Audio, 0)
        ]
    );
    assert_eq!(inputs[0].dimensions, Some((1600, 900)));
    assert_eq!(inputs[1].duration_ms, Some(2000));
}

#[test]
fn old_v1_wire_bytes_remain_stable() {
    let image = image_parameters(ImageModelVersion::V5_0, 1).into();
    assert_eq!(encode_submit_command(image),br#"{"operation":"text2image","schema_version":1,"prompt":"paint it","model_version":"5.0","ratio":"16:9","resolution_type":"2k","generate_num":1,"poll":0}"#);
    let video = TextToVideoRequestV1::new(
        "p",
        VideoModelVersion::Seedance2_5,
        VideoRatio::R16x9,
        5,
        VideoResolution::P480,
    )
    .unwrap()
    .into();
    assert_eq!(encode_submit_command(video),br#"{"operation":"text2video","schema_version":1,"prompt":"p","model_version":"seedance2.5","ratio":"16:9","duration":5,"video_resolution":"480p","poll":0}"#);
}

#[test]
fn canonical_rejects_unsupported_fields_and_tampered_input_constraints() {
    for request in examples() {
        let value: serde_json::Value =
            serde_json::from_slice(&encode_submit_command(request)).unwrap();
        for (field, extra) in [
            ("poll", serde_json::json!(1)),
            ("session", serde_json::json!(5)),
            ("schema_version", serde_json::json!(2)),
            ("seed", serde_json::json!(42)),
        ] {
            let mut modified = value.clone();
            modified[field] = extra;
            assert!(
                parse_submit_command(&serde_json::to_vec(&modified).unwrap()).is_err(),
                "{modified}"
            );
        }
    }
    let mut value: serde_json::Value =
        serde_json::from_slice(&encode_submit_command(examples().pop().unwrap())).unwrap();
    value["videos"][0]["duration_ms"] = serde_json::json!(1999);
    assert!(parse_submit_command(&serde_json::to_vec(&value).unwrap()).is_err());
    value["videos"][0]["duration_ms"] = serde_json::json!(2000);
    value["images"][0]["filename"] = serde_json::json!("/etc/passwd");
    assert!(parse_submit_command(&serde_json::to_vec(&value).unwrap()).is_err());
    value["images"][0]["filename"] = serde_json::json!("image-0.png");
    value["images"][0]["extra"] = serde_json::json!(true);
    assert!(parse_submit_command(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn execution_checks_staged_digest_regular_file_and_workspace_binding() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let account = root.path().join("account");
    fs::create_dir(&workspace).unwrap();
    fs::create_dir(&account).unwrap();
    let binary = root.path().join("dreamina");
    fs::write(&binary, b"#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();
    let binary_sha: [u8; 32] = Sha256::digest(fs::read(&binary).unwrap()).into();
    let policy = DreaminaCliPolicyV1::new(
        &binary,
        binary_sha,
        WorkingDirectory::new(&workspace).unwrap(),
        WorkingDirectory::new(&account).unwrap(),
        Duration::from_secs(5),
        Duration::from_secs(1),
    )
    .unwrap();
    let bytes = b"frozen image bytes";
    let digest = Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let input = StagedImageV1::new("input.png", digest, 1600, 900).unwrap();
    let request: DreaminaSubmitRequestV1 =
        ImageUpscaleRequestV1::new(input, UpscaleResolution::K2).into();
    assert!(matches!(
        policy.command_spec(&request),
        Err(DreaminaCliPolicyError::InvalidStagedInput)
    ));
    fs::write(workspace.join("input.png"), bytes).unwrap();
    assert!(policy.command_spec(&request).is_ok());
    fs::write(workspace.join("input.png"), b"modified").unwrap();
    assert!(matches!(
        policy.command_spec(&request),
        Err(DreaminaCliPolicyError::InvalidStagedInput)
    ));
    fs::remove_file(workspace.join("input.png")).unwrap();
    let outside = root.path().join("outside.png");
    fs::write(&outside, bytes).unwrap();
    symlink(&outside, workspace.join("input.png")).unwrap();
    assert!(matches!(
        policy.command_spec(&request),
        Err(DreaminaCliPolicyError::InvalidStagedInput)
    ));
}
