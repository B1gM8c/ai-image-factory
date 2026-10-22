use super::*;
use crate::{
    executor::{ExecutorInputObject, ExecutorLaunchContext},
    input_blobs::InputBlobKey,
};

fn blob(index: u16) -> InputBlobRef {
    InputBlobRef {
        key: InputBlobKey {
            admission_session_id: Uuid::new_v4(),
            input_id: Uuid::new_v4(),
        },
        storage_backend: "memory".to_owned(),
        object_key: format!("input-{index}"),
        sha256_hex: "a".repeat(64),
        byte_size: 128,
    }
}

fn context() -> ExecutorLaunchContext {
    ExecutorLaunchContext::new(
        "request-1",
        "ark-v3",
        0,
        "dreamina.test.v1",
        "a".repeat(64),
        serde_json::json!({}),
    )
    .unwrap()
}

#[test]
fn multimodal_storage_descriptors_have_bounded_role_specific_media_types() {
    for (role, media_type) in [
        ("image", "image/png"),
        ("image", "image/jpeg"),
        ("image", "image/webp"),
        ("video", "video/mp4"),
        ("audio", "audio/mpeg"),
        ("audio", "audio/wav"),
    ] {
        assert!(ExecutorInputObject::new(blob(49), role, 49, media_type).is_some());
        assert!(ExecutorInputObject::new(blob(50), role, 50, media_type).is_none());
    }
    assert!(ExecutorInputObject::new(blob(0), "mask", 0, "image/png").is_some());
    for (role, index, media_type) in [
        ("image", 0, "video/mp4"),
        ("video", 0, "image/png"),
        ("audio", 0, "video/mp4"),
        ("audio", 0, "audio/ogg"),
        ("mask", 1, "image/png"),
        ("mask", 0, "image/jpeg"),
        ("reference", 0, "image/png"),
    ] {
        assert!(ExecutorInputObject::new(blob(index), role, index, media_type).is_none());
    }
}

#[test]
fn multimodal_launch_context_preserves_fifty_continuous_global_positions() {
    let inputs: Vec<_> = (0..50)
        .map(|index| {
            let (role, media_type) = match index {
                0..=19 => ("image", "image/png"),
                20..=39 => ("video", "video/mp4"),
                _ => ("audio", "audio/wav"),
            };
            ExecutorInputObject::new(blob(index), role, index, media_type).unwrap()
        })
        .collect();
    assert_eq!(
        context().with_inputs(inputs.clone()).unwrap().inputs(),
        inputs
    );
    let mut duplicate_position = inputs.clone();
    duplicate_position[49] = duplicate_position[48].clone();
    assert!(context().with_inputs(duplicate_position).is_none());
    let mut too_many = inputs;
    too_many.push(too_many[49].clone());
    assert!(context().with_inputs(too_many).is_none());
}

fn edit_attach(roles: &[(EditInputRoleV1, &str)]) -> AttachJob {
    let session_id = Uuid::new_v4();
    let inputs: Vec<_> = roles
        .iter()
        .enumerate()
        .map(|(index, (role, media_type))| {
            let index = if *role == EditInputRoleV1::Mask {
                0
            } else {
                u16::try_from(index).unwrap()
            };
            let mut blob = blob(index);
            blob.key.admission_session_id = session_id;
            blob.object_key = format!("{}-{index}", role.as_str());
            AttachInputObject {
                blob,
                role: *role,
                index,
                media_type: (*media_type).to_owned(),
            }
        })
        .collect();
    let command = EditCommandV1 {
        background: "auto".to_owned(),
        inputs: inputs
            .iter()
            .map(|input| EditInputDescriptorV1 {
                byte_size: input.blob.byte_size,
                index: input.index,
                media_type: input.media_type.clone(),
                role: input.role,
                sha256_hex: input.blob.sha256_hex.clone(),
            })
            .collect(),
        model: "gpt-image-2".to_owned(),
        moderation: None,
        n: 1,
        operation: EDIT_OPERATION.to_owned(),
        output_compression: None,
        output_format: "png".to_owned(),
        partial_images: 0,
        prompt: "replace the sky".to_owned(),
        provider_id: "openai-codex".to_owned(),
        quality: "auto".to_owned(),
        schema_version: EDIT_COMMAND_SCHEMA_VERSION,
        size: "auto".to_owned(),
        source_api_profile: "openai-images-v1".to_owned(),
        stream: false,
    };
    AttachJob {
        ticket: AdmissionTicket {
            session_id,
            owner_token: Uuid::new_v4(),
            request_hash: command.request_hash_hex(),
        },
        job_id: Uuid::new_v4(),
        command_schema: EDIT_COMMAND_SCHEMA.to_owned(),
        command_json: serde_json::to_value(&command).unwrap(),
        input_manifest: Some(AttachInputManifest {
            manifest_schema: EDIT_INPUT_MANIFEST_SCHEMA.to_owned(),
            manifest_hash: command.input_manifest_hash_hex(),
            inputs,
        }),
        work_kind: "edit".to_owned(),
        schedule_scope: "test".to_owned(),
        schedule_weight: 1,
        schedule_priority: 0,
        schedule_cost: 1,
        contract: AdmissionContract::LegacyV1,
        customer_pricing: None,
    }
}

#[test]
fn codex_edit_admission_retains_legacy_image_limits_and_rejects_timed_media() {
    let mut roles = vec![(EditInputRoleV1::Image, "image/png"); 16];
    roles.push((EditInputRoleV1::Mask, "image/png"));
    assert!(validate_attach_request(&edit_attach(&roles)).is_ok());
    assert!(
        validate_attach_request(&edit_attach(&[(EditInputRoleV1::Image, "image/png"); 17]))
            .is_err()
    );
    for input in [
        (EditInputRoleV1::Video, "video/mp4"),
        (EditInputRoleV1::Audio, "audio/wav"),
    ] {
        assert!(matches!(
            validate_attach_request(&edit_attach(&[
                (EditInputRoleV1::Image, "image/png"),
                input
            ])),
            Err(AdmissionError::InvalidCommand)
        ));
    }
}

#[test]
fn new_dreamina_workflow_stays_closed_until_input_admission_is_bound() {
    use image_provider_dreamina_cli::{
        ImageUpscaleRequestV1, StagedImageV1, UpscaleResolution, encode_submit_command,
    };

    let command = ImageUpscaleRequestV1::new(
        StagedImageV1::new("source.png", "a".repeat(64), 1600, 900).unwrap(),
        UpscaleResolution::K2,
    );
    let bytes = encode_submit_command(command.into());
    let mut request = edit_attach(&[(EditInputRoleV1::Image, "image/png")]);
    request.command_schema = DREAMINA_SUBMIT_COMMAND_SCHEMA.to_owned();
    request.command_json = serde_json::from_slice(&bytes).unwrap();
    request.ticket.request_hash = hex::encode(Sha256::digest(&bytes));
    request.input_manifest = None;
    assert!(matches!(
        validate_attach_request(&request),
        Err(AdmissionError::InvalidCommand)
    ));
    assert!(matches!(
        attach_operation(&request),
        Err(AdmissionError::InvalidCommand)
    ));
}
