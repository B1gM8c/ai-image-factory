use std::{ffi::OsString, fs, os::unix::fs::PermissionsExt, time::Duration};

use image_cli_runtime::WorkingDirectory;
use image_provider_sdk::{OutputSlot, SingleOutputCommand};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::*;

const SOURCE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn image_request(count: u8, edit: bool) -> DreaminaSubmitRequestV1 {
    let parameters = TextToImageRequestV1::new_custom(
        "paint it; $(not-a-shell)",
        ImageModelVersion::V5_0,
        1024,
        1024,
        ImageResolution::K2,
        count,
    )
    .unwrap();
    if edit {
        ImageToImageRequestV1::new(
            vec![StagedImageV1::new("input.png", SOURCE_SHA, 1024, 1024).unwrap()],
            parameters,
        )
        .unwrap()
        .into()
    } else {
        parameters.into()
    }
}

#[test]
fn image_batch_encoding_is_explicit_and_single_image_encoding_stays_version_one() {
    for edit in [false, true] {
        for count in [1, 2, 10] {
            let request = image_request(count, edit);
            let bytes = encode_submit_command(request.clone());
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["schema_version"], if count == 1 { 1 } else { 2 });
            assert_eq!(value["generate_num"], count);
            assert_eq!(value["poll"], 0);
            assert_eq!(value["width"], 1024);
            assert_eq!(value["height"], 1024);
            assert_eq!(parse_submit_command(&bytes).unwrap(), request);
            assert_eq!(is_native_image_batch_command(&bytes).unwrap(), count > 1);
            if count > 1 {
                let batch = parse_native_image_batch_command(&bytes).unwrap();
                assert_eq!(batch.request(), &request);
                let argv = batch.argv();
                let index = argv.iter().position(|a| a == "--generate_num").unwrap();
                assert_eq!(argv[index + 1], OsString::from(count.to_string()));
                assert_eq!(argv.last(), Some(&OsString::from("--poll=0")));
                assert_eq!(
                    DreaminaSubmitPayloadV1::new(SOURCE_SHA, request),
                    Err(DreaminaSubmitCommandError::BatchSubmissionUnsupported)
                );
            } else {
                assert_eq!(
                    parse_native_image_batch_command(&bytes),
                    Err(DreaminaSubmitCommandError::NativeImageBatchRequired)
                );
            }
        }
    }
}

#[test]
fn historical_version_one_batches_still_parse_without_enabling_native_execution() {
    for edit in [false, true] {
        let request = image_request(10, edit);
        let mut canonical: Value =
            serde_json::from_slice(&encode_submit_command(request.clone())).unwrap();
        canonical["schema_version"] = json!(1);
        let bytes = serde_json::to_vec(&canonical).unwrap();
        assert_eq!(parse_submit_command(&bytes).unwrap(), request);
        assert!(!is_native_image_batch_command(&bytes).unwrap());
        assert_eq!(
            parse_native_image_batch_command(&bytes),
            Err(DreaminaSubmitCommandError::NativeImageBatchRequired)
        );
        assert_eq!(
            DreaminaSubmitPayloadV1::new(SOURCE_SHA, request.clone()),
            Err(DreaminaSubmitCommandError::BatchSubmissionUnsupported)
        );
        assert_eq!(request.into_single_output().output_count(), 1);
    }
}

#[test]
fn native_batch_detection_rejects_invalid_counts_polling_and_unknown_fields() {
    for edit in [false, true] {
        let canonical: Value =
            serde_json::from_slice(&encode_submit_command(image_request(2, edit))).unwrap();
        for count in [0, 1, 11, 255] {
            let mut invalid = canonical.clone();
            invalid["generate_num"] = json!(count);
            let bytes = serde_json::to_vec(&invalid).unwrap();
            assert!(parse_submit_command(&bytes).is_err());
            assert!(is_native_image_batch_command(&bytes).is_err());
            assert!(parse_native_image_batch_command(&bytes).is_err());
        }
        for (field, value) in [
            ("schema_version", json!(3)),
            ("poll", json!(1)),
            ("unknown", json!(true)),
            ("model_version", json!("future")),
        ] {
            let mut invalid = canonical.clone();
            invalid[field] = value;
            let bytes = serde_json::to_vec(&invalid).unwrap();
            assert!(parse_submit_command(&bytes).is_err());
            assert!(is_native_image_batch_command(&bytes).is_err());
            assert!(parse_native_image_batch_command(&bytes).is_err());
        }
    }
    for bytes in [b"".as_slice(), b"{}", b"[]", b"null"] {
        assert!(is_native_image_batch_command(bytes).is_err());
    }
    assert!(is_native_image_batch_command(&vec![b' '; MAX_SUBMIT_COMMAND_BYTES + 1]).is_err());
}

#[test]
fn native_batch_version_cannot_be_used_for_upscale_or_video_workflows() {
    let image = || StagedImageV1::new("input.png", SOURCE_SHA, 1600, 900).unwrap();
    let options = || {
        ReferenceVideoOptionsV1::new(
            "pan",
            VideoModelVersion::Seedance2_5,
            None,
            5,
            VideoResolution::P480,
        )
        .unwrap()
    };
    let requests = vec![
        ImageUpscaleRequestV1::new(image(), UpscaleResolution::K4).into(),
        TextToVideoRequestV1::new(
            "pan",
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            5,
            VideoResolution::P480,
        )
        .unwrap()
        .into(),
        ImageToVideoRequestV1::new(image(), options())
            .unwrap()
            .into(),
        FramesToVideoRequestV1::new(
            image(),
            StagedImageV1::new("last.png", SOURCE_SHA, 1600, 900).unwrap(),
            options(),
        )
        .unwrap()
        .into(),
        MultiframeToVideoRequestV1::new(
            vec![
                image(),
                StagedImageV1::new("last.png", SOURCE_SHA, 1600, 900).unwrap(),
            ],
            vec![VideoTransitionV1::new("pan", 2000).unwrap()],
            VideoResolution::P720,
        )
        .unwrap()
        .into(),
        MultimodalToVideoRequestV1::new(
            vec![image()],
            vec![],
            vec![],
            "pan",
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            5,
            VideoResolution::P480,
        )
        .unwrap()
        .into(),
    ];
    for request in requests {
        let mut canonical: Value = serde_json::from_slice(&encode_submit_command(request)).unwrap();
        assert_eq!(canonical["schema_version"], 1);
        assert!(!is_native_image_batch_command(&serde_json::to_vec(&canonical).unwrap()).unwrap());
        canonical["schema_version"] = json!(2);
        let bytes = serde_json::to_vec(&canonical).unwrap();
        assert_eq!(
            parse_submit_command(&bytes),
            Err(DreaminaSubmitCommandError::UnsupportedSchemaVersion)
        );
        assert!(is_native_image_batch_command(&bytes).is_err());
        assert!(parse_native_image_batch_command(&bytes).is_err());
    }
}

#[test]
fn native_batch_payload_retains_source_and_output_identity_binding() {
    let bytes = encode_submit_command(image_request(2, false));
    let command = |source: &str, index| {
        SingleOutputCommand::new(
            OutputSlot::new(index, 2).unwrap(),
            DreaminaSubmitPayloadV1::new_native_image_batch(
                source,
                parse_native_image_batch_command(&bytes).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let first = command(SOURCE_SHA, 0);
    assert_eq!(first.canonical_payload(), bytes);
    assert_eq!(first.source_command_sha256(), SOURCE_SHA);
    assert_eq!(first.schema_id(), DREAMINA_SUBMIT_COMMAND_SCHEMA);
    assert_eq!(first.adapter_revision(), ADAPTER_REVISION);
    assert_eq!(first.output(), OutputSlot::new(0, 2).unwrap());
    assert_eq!(first, command(SOURCE_SHA, 0));
    assert_ne!(
        first.canonical_sha256(),
        command(SOURCE_SHA, 1).canonical_sha256()
    );
    assert_ne!(
        first.canonical_sha256(),
        command(&"b".repeat(64), 0).canonical_sha256()
    );
    assert_eq!(
        DreaminaSubmitPayloadV1::new_native_image_batch(
            "not-a-digest",
            parse_native_image_batch_command(&bytes).unwrap(),
        ),
        Err(DreaminaSubmitCommandError::InvalidSourceCommand)
    );
}

#[test]
fn native_batch_policy_preserves_count_input_integrity_and_workspace_confinement() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let account = root.path().join("account");
    let attempt = workspace.join("attempt");
    let outside = root.path().join("outside");
    for dir in [&workspace, &account, &attempt, &outside] {
        fs::create_dir(dir).unwrap();
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let executable = root.path().join("fake-cli");
    let executable_bytes = b"#!/bin/sh\nexit 99\n";
    fs::write(&executable, executable_bytes).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
    let policy = DreaminaCliPolicyV1::new(
        &executable,
        Sha256::digest(executable_bytes).into(),
        WorkingDirectory::new_private(&workspace).unwrap(),
        WorkingDirectory::new_private(&account).unwrap(),
        Duration::from_secs(2),
        Duration::from_millis(50),
    )
    .unwrap();
    let input_bytes = b"frozen offline input";
    fs::write(attempt.join("input.png"), input_bytes).unwrap();
    let input_sha = Sha256::digest(input_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    for edit in [false, true] {
        for count in [2, 10] {
            let mut canonical: Value =
                serde_json::from_slice(&encode_submit_command(image_request(count, edit))).unwrap();
            if edit {
                canonical["images"][0]["sha256"] = json!(input_sha);
            }
            let batch =
                parse_native_image_batch_command(&serde_json::to_vec(&canonical).unwrap()).unwrap();
            assert!(matches!(
                policy.command_spec(batch.request()),
                Err(DreaminaCliPolicyError::BatchSubmissionUnsupported)
            ));
            let command = policy
                .command_spec_native_image_batch_in(
                    &batch,
                    WorkingDirectory::new_private(&attempt).unwrap(),
                )
                .unwrap();
            let args = command.arguments();
            let index = args.iter().position(|a| a == "--generate_num").unwrap();
            assert_eq!(args[index + 1], OsString::from(count.to_string()));
            assert_eq!(args.last(), Some(&OsString::from("--poll=0")));
            assert_eq!(
                command.working_directory().path(),
                fs::canonicalize(&attempt).unwrap()
            );
            assert_eq!(
                command.environment().get(std::ffi::OsStr::new("HOME")),
                Some(&fs::canonicalize(&account).unwrap().as_os_str().to_owned())
            );
            assert!(matches!(
                policy.command_spec_native_image_batch_in(
                    &batch,
                    WorkingDirectory::new_private(&outside).unwrap(),
                ),
                Err(DreaminaCliPolicyError::ExecutionWorkspaceOutsideRoot)
            ));
            if edit {
                fs::write(attempt.join("input.png"), b"changed").unwrap();
                assert!(matches!(
                    policy.command_spec_native_image_batch_in(
                        &batch,
                        WorkingDirectory::new_private(&attempt).unwrap(),
                    ),
                    Err(DreaminaCliPolicyError::InvalidStagedInput)
                ));
                fs::write(attempt.join("input.png"), input_bytes).unwrap();
            }
        }
    }
}
