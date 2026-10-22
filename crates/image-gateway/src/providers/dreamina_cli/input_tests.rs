use super::*;
use crate::input_blobs::{InputBlobKey, InputBlobRef};
use image_provider_dreamina_cli::{
    FramesToVideoRequestV1, ImageUpscaleRequestV1, MultimodalToVideoRequestV1,
    ReferenceVideoOptionsV1, StagedImageV1, StagedTimedMediaV1, UpscaleResolution,
    VideoModelVersion, VideoRatio, VideoResolution,
};

fn object(index: u16, role: &str, digest: &str, media_type: &str) -> ExecutorInputObject {
    ExecutorInputObject::new(
        InputBlobRef {
            key: InputBlobKey {
                admission_session_id: Uuid::new_v4(),
                input_id: Uuid::new_v4(),
            },
            storage_backend: "test".into(),
            object_key: format!("input-{index}"),
            sha256_hex: digest.into(),
            byte_size: 42,
        },
        role,
        index,
        media_type,
    )
    .unwrap()
}

#[test]
fn canonical_inputs_reject_missing_extra_digest_and_order_mismatches() {
    let digest = "a".repeat(64);
    let request = DreaminaSubmitRequestV1::ImageUpscale(ImageUpscaleRequestV1::new(
        StagedImageV1::new("image-0.png", &digest, 2, 3).unwrap(),
        UpscaleResolution::K2,
    ));
    let input = object(0, "image", &digest, "image/png");
    assert_eq!(
        dreamina_input_files(&request, std::slice::from_ref(&input))
            .unwrap()
            .len(),
        1
    );
    assert!(dreamina_input_files(&request, &[]).is_err());
    assert!(dreamina_input_files(&request, &[input.clone(), input.clone()]).is_err());
    let changed = object(0, "image", &"b".repeat(64), "image/png");
    assert!(dreamina_input_files(&request, &[changed]).is_err());
    let wrong_index = object(1, "image", &digest, "image/png");
    assert!(dreamina_input_files(&request, &[wrong_index]).is_err());

    let request = DreaminaSubmitRequestV1::FramesToVideo(
        FramesToVideoRequestV1::new(
            StagedImageV1::new("first.png", &digest, 2, 3).unwrap(),
            StagedImageV1::new("last.png", "b".repeat(64), 2, 3).unwrap(),
            ReferenceVideoOptionsV1::new(
                "animate",
                VideoModelVersion::Seedance2_5,
                None,
                5,
                VideoResolution::P480,
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let last = object(1, "image", &"b".repeat(64), "image/png");
    assert!(dreamina_input_files(&request, &[input.clone(), last.clone()]).is_ok());
    assert!(dreamina_input_files(&request, &[last, input]).is_err());
}

#[test]
fn multimodal_global_indices_and_audio_probe_boundary_are_explicit() {
    let image_hash = "a".repeat(64);
    let video_hash = "b".repeat(64);
    let request = DreaminaSubmitRequestV1::MultimodalToVideo(
        MultimodalToVideoRequestV1::new(
            vec![StagedImageV1::new("image.png", &image_hash, 2, 3).unwrap()],
            vec![StagedTimedMediaV1::new("video.mp4", &video_hash, 2000).unwrap()],
            vec![],
            "animate",
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            5,
            VideoResolution::P480,
        )
        .unwrap(),
    );
    assert!(
        dreamina_input_files(
            &request,
            &[
                object(0, "image", &image_hash, "image/png"),
                object(1, "video", &video_hash, "video/mp4"),
            ]
        )
        .is_ok()
    );
    assert!(
        dreamina_input_files(
            &request,
            &[
                object(0, "image", &image_hash, "image/png"),
                object(0, "video", &video_hash, "video/mp4"),
            ]
        )
        .is_err()
    );

    let request = DreaminaSubmitRequestV1::MultimodalToVideo(
        MultimodalToVideoRequestV1::new(
            vec![],
            vec![],
            vec![StagedTimedMediaV1::new("audio.wav", &image_hash, 2000).unwrap()],
            "animate",
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            5,
            VideoResolution::P480,
        )
        .unwrap(),
    );
    assert!(
        dreamina_input_files(&request, &[object(0, "audio", &image_hash, "audio/wav")]).is_ok()
    );
    assert!(
        dreamina_input_files(&request, &[object(0, "audio", &image_hash, "audio/mpeg")]).is_ok()
    );
    let failure = dreamina_input_files(
        &request,
        &[object(0, "audio", &"b".repeat(64), "audio/wav")],
    )
    .unwrap_err();
    assert_eq!(failure.code(), "dreamina_submit_input_manifest_invalid");
    assert_eq!(failure.effect(), EffectCertainty::NoRemoteEffect);
}
