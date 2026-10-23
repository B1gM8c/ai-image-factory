use super::{project_submit_command, request_matches_platform};
use crate::{
    executor::{ExecutorLaunchContext, ExecutorSubmissionLease},
    provider_tasks::{
        GatedCliSubmitDriver, ProviderSubmitProjectionError, ProviderSubmitProjector,
    },
};
use image_provider_dreamina_cli::*;
use image_provider_sdk::OutputSlot;

#[test]
fn workflows_bind_the_exact_operation_and_execution_model() {
    let image = |name| StagedImageV1::new(name, "a".repeat(64), 1600, 900).unwrap();
    let parameters = TextToImageRequestV1::new(
        "p",
        ImageModelVersion::V5_0,
        ImageRatio::R16x9,
        ImageResolution::K2,
        1,
    )
    .unwrap();
    let options =
        |model| ReferenceVideoOptionsV1::new("p", model, None, 5, VideoResolution::P720).unwrap();
    let cases = [
        (
            ImageToImageRequestV1::new(vec![image("one.png")], parameters)
                .unwrap()
                .into(),
            "images.generations",
            "dreamina-image-5.0",
        ),
        (
            ImageUpscaleRequestV1::new(image("one.png"), UpscaleResolution::K4).into(),
            "images.generations",
            "dreamina-image-image_upscale",
        ),
        (
            ImageToVideoRequestV1::new(
                image("one.png"),
                options(VideoModelVersion::Seedance1_0Fast),
            )
            .unwrap()
            .into(),
            "videos.generations",
            "seedance1.0fast",
        ),
        (
            FramesToVideoRequestV1::new(
                image("one.png"),
                image("two.png"),
                options(VideoModelVersion::Seedance1_5Pro),
            )
            .unwrap()
            .into(),
            "videos.generations",
            "seedance1.5pro",
        ),
        (
            MultimodalToVideoRequestV1::new(
                vec![image("one.png")],
                vec![],
                vec![],
                "p",
                VideoModelVersion::Seedance2_5,
                VideoRatio::R16x9,
                5,
                VideoResolution::P720,
            )
            .unwrap()
            .into(),
            "videos.generations",
            "seedance2.5",
        ),
    ];
    for (command, operation, model) in cases {
        assert!(request_matches_platform(&command, operation, model));
        assert!(!request_matches_platform(
            &command,
            operation,
            "wrong-model"
        ));
        assert!(!request_matches_platform(&command, "images.edits", model));
    }
    let multiframe: DreaminaSubmitRequestV1 = MultiframeToVideoRequestV1::new(
        vec![image("one.png"), image("two.png")],
        vec![VideoTransitionV1::new("p", 2500).unwrap()],
        VideoResolution::P720,
    )
    .unwrap()
    .into();
    assert!(request_matches_platform(
        &multiframe,
        "videos.generations",
        "multiframe2video"
    ));
    assert!(!request_matches_platform(
        &multiframe,
        "videos.generations",
        "seedance2.5"
    ));
    assert!(!request_matches_platform(
        &multiframe,
        "images.generations",
        "multiframe2video"
    ));
}

fn image_source(count: u8, edit: bool) -> serde_json::Value {
    let parameters = TextToImageRequestV1::new(
        "paint a red fox",
        ImageModelVersion::V5_0,
        ImageRatio::R16x9,
        ImageResolution::K2,
        count,
    )
    .unwrap();
    let request = if edit {
        ImageToImageRequestV1::new(
            vec![StagedImageV1::new("input-0.png", "a".repeat(64), 1600, 900).unwrap()],
            parameters,
        )
        .unwrap()
        .into()
    } else {
        DreaminaSubmitRequestV1::from(parameters)
    };
    serde_json::from_slice(&encode_submit_command(request)).unwrap()
}

fn historical_image_source(count: u8, edit: bool) -> serde_json::Value {
    let mut source = image_source(count, edit);
    // This is the persisted pre-native-batch command format, not a new request.
    source["schema_version"] = serde_json::json!(1);
    source
}

#[test]
fn historical_image_fanout_preserves_requested_count_and_unique_single_output_commands() {
    use image_cli_runtime::WorkingDirectory;
    use sha2::{Digest, Sha256};
    use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let account = root.path().join("account");
    fs::create_dir(&workspace).unwrap();
    fs::create_dir(&account).unwrap();
    let executable = root.path().join("cli");
    let bytes = b"#!/bin/sh\nexit 0\n";
    fs::write(&executable, bytes).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
    let policy = DreaminaCliPolicyV1::new(
        &executable,
        Sha256::digest(bytes).into(),
        WorkingDirectory::new(&workspace).unwrap(),
        WorkingDirectory::new(&account).unwrap(),
        Duration::from_secs(5),
        Duration::from_secs(1),
    )
    .unwrap();
    let profile_id = uuid::Uuid::new_v4();
    let codec = super::DreaminaCliSubmitCodecV1::new(
        policy,
        super::DreaminaCliRuntimeBindingV1::new(profile_id, uuid::Uuid::new_v4(), "a".repeat(64))
            .unwrap(),
    );
    for count in [2, 10] {
        for edit in [false, true] {
            let source = historical_image_source(count, edit);
            let source_hash = "b".repeat(64);
            let mut identities = std::collections::HashSet::new();
            for index in 0..i32::from(count) {
                let lease = ExecutorSubmissionLease {
                    submission_id: uuid::Uuid::new_v4(),
                    executor_execution_id: uuid::Uuid::new_v4(),
                    output_id: uuid::Uuid::new_v4(),
                    job_id: uuid::Uuid::new_v4(),
                    tenant_id: "fanout-test".into(),
                    provider_id: PROVIDER_ID.into(),
                    model: "dreamina-image-5.0".into(),
                    work_item_id: uuid::Uuid::new_v4(),
                    output_index: index,
                    command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
                    command_hash: source_hash.clone(),
                    execution_profile_id: profile_id,
                    adapter_revision: ADAPTER_REVISION.into(),
                    executor_owner: "fanout-test".into(),
                    executor_lease_epoch: 1,
                    executor_lease_expires_at_ms: i64::MAX,
                };
                let context = ExecutorLaunchContext::new(
                    "fanout-request",
                    "dreamina-cli.images.v1",
                    index,
                    DREAMINA_SUBMIT_COMMAND_SCHEMA,
                    source_hash.clone(),
                    source.clone(),
                )
                .unwrap();
                // Exercise the production fresh path that previously hardcoded total=1.
                let command = <super::DreaminaCliSubmitCodecV1 as ProviderSubmitProjector<
                    GatedCliSubmitDriver<super::DreaminaCliSubmitCodecV1>,
                >>::project_fresh(&codec, &lease, &context)
                .unwrap();
                assert_eq!(
                    command.output(),
                    OutputSlot::new(index as u32, u32::from(count)).unwrap()
                );
                assert_eq!(command.source_command_sha256(), source_hash);
                assert!(identities.insert(*command.canonical_sha256()));
                assert!(!is_native_image_batch_command(command.canonical_payload()).unwrap());
                let request = parse_submit_command(command.canonical_payload()).unwrap();
                assert_eq!(request.output_count(), 1);
                let args = match &request {
                    DreaminaSubmitRequestV1::TextToImage(request) => request.to_argv(),
                    DreaminaSubmitRequestV1::ImageToImage(request) => {
                        assert_eq!(request.images()[0].filename(), "input-0.png");
                        request.to_argv()
                    }
                    _ => panic!("image workflow changed during projection"),
                };
                assert_eq!(args[0], if edit { "image2image" } else { "text2image" });
                let count_arg = args.iter().position(|arg| arg == "--generate_num").unwrap();
                assert_eq!(args[count_arg + 1], "1");
                assert!(args.iter().any(|arg| arg == "--poll=0"));
                // Recovery must reproduce the exact command identity, including its slot.
                let recovered =
                    project_submit_command(&source, &source_hash, index, Some(i32::from(count)))
                        .unwrap();
                assert_eq!(recovered, command);
            }
            assert_eq!(identities.len(), usize::from(count));
            assert_eq!(source["schema_version"], 1);
            assert_eq!(source["generate_num"], count);
        }
    }
}

#[test]
fn image_fanout_rejects_out_of_range_slots_and_recovery_total_drift() {
    for count in [2, 10] {
        for edit in [false, true] {
            let source = historical_image_source(count, edit);
            let source_hash = "b".repeat(64);
            for index in [-1, i32::from(count)] {
                assert_eq!(
                    project_submit_command(&source, &source_hash, index, None),
                    Err(ProviderSubmitProjectionError::OutputOutOfRange)
                );
            }
            for total in [-1, 0, 1, i32::from(count) + 1] {
                assert_eq!(
                    project_submit_command(&source, &source_hash, 0, Some(total)),
                    Err(ProviderSubmitProjectionError::OutputOutOfRange)
                );
            }
        }
    }
}

#[test]
fn native_image_edit_batch_preserves_all_parameters_and_only_projects_the_leader() {
    for count in [2, 10] {
        let source = image_source(count, true);
        assert_eq!(source["schema_version"], 2);
        assert_eq!(source["generate_num"], count);
        let source_hash = "b".repeat(64);
        let command =
            project_submit_command(&source, &source_hash, 0, Some(i32::from(count))).unwrap();
        assert_eq!(
            command.output(),
            OutputSlot::new(0, u32::from(count)).unwrap()
        );
        assert_eq!(command.source_command_sha256(), source_hash);
        assert!(is_native_image_batch_command(command.canonical_payload()).unwrap());
        let request = parse_submit_command(command.canonical_payload()).unwrap();
        assert!(request_matches_platform(
            &request,
            "images.generations",
            "dreamina-image-5.0"
        ));
        let DreaminaSubmitRequestV1::ImageToImage(request) = request else {
            panic!("native image edit workflow changed during projection");
        };
        assert_eq!(request.generate_num(), count);
        assert_eq!(request.parameters().prompt(), "paint a red fox");
        assert_eq!(request.parameters().model(), ImageModelVersion::V5_0);
        assert_eq!(request.parameters().ratio(), Some(ImageRatio::R16x9));
        assert_eq!(request.parameters().resolution(), ImageResolution::K2);
        assert_eq!(request.images().len(), 1);
        let input = &request.images()[0];
        assert_eq!(input.filename(), "input-0.png");
        assert_eq!(input.sha256(), "a".repeat(64));
        assert_eq!((input.width(), input.height()), (1600, 900));
        let args = request.to_argv();
        assert_eq!(args[0], "image2image");
        assert_eq!(
            args.iter().filter(|arg| *arg == "--generate_num").count(),
            1
        );
        let count_arg = args.iter().position(|arg| arg == "--generate_num").unwrap();
        assert_eq!(
            args[count_arg + 1],
            std::ffi::OsString::from(count.to_string())
        );
        assert_eq!(args.last(), Some(&std::ffi::OsString::from("--poll=0")));
        let recovered =
            project_submit_command(&source, &source_hash, 0, Some(i32::from(count))).unwrap();
        assert_eq!(command, recovered);
        for follower in 1..i32::from(count) {
            assert_eq!(
                project_submit_command(&source, &source_hash, follower, Some(i32::from(count))),
                Err(ProviderSubmitProjectionError::OutputOutOfRange)
            );
        }
        for total in [1, i32::from(count) + 1] {
            assert_eq!(
                project_submit_command(&source, &source_hash, 0, Some(total)),
                Err(ProviderSubmitProjectionError::OutputOutOfRange)
            );
        }
    }
}
