use super::*;
use base64::Engine;
use image_api_contracts::dreamina::{DreaminaImageOutputCounts, DreaminaImageRequestMetadata};
use serde_json::{Value, json};

fn projection(status: ImageResultStatus) -> ImageTaskProjection {
    ImageTaskProjection {
        model: "dreamina-image-3.0".to_owned(),
        request_id: "original-request".to_owned(),
        request: DreaminaImageRequestMetadata {
            generate_num: Some(1),
            model_version: Some("3.0".to_owned()),
            workflow: Some("text2image".to_owned()),
            resolution_type: Some("2k".to_owned()),
            ratio: Some("16:9".to_owned()),
            ..Default::default()
        },
        status,
    }
}

fn progress() -> VideoProviderProgress {
    VideoProviderProgress {
        progress: image_provider_sdk::ProviderTaskProgress {
            provider_status: "Queueing".to_owned(),
            queue_position: Some(3341),
            queue_total: Some(476961),
            estimated_remaining_seconds: Some(27131),
        },
        observed_at_ms: 1_792_828_821_077,
    }
}

#[test]
fn image_async_preference_is_explicit_and_case_insensitive() {
    let mut headers = HeaderMap::new();
    assert!(!prefers_async(&headers));
    headers.insert(
        "prefer",
        HeaderValue::from_static("wait=10, RESPOND-ASYNC; wait=0"),
    );
    assert!(prefers_async(&headers));
    headers.insert("prefer", HeaderValue::from_static("respond-async=false"));
    assert!(!prefers_async(&headers));
    headers.insert("prefer", HeaderValue::from_static("respond-async-similar"));
    assert!(!prefers_async(&headers));
}

#[test]
fn image_lookup_operation_is_an_explicit_closed_enum() {
    let empty: ImageLookupQuery = serde_json::from_value(json!({})).unwrap();
    assert!(matches!(empty.operation, ImageLookupOperation::Generation));
    let edit: ImageLookupQuery = serde_json::from_value(json!({"operation": "edit"})).unwrap();
    assert!(matches!(edit.operation, ImageLookupOperation::Edit));
    assert!(
        serde_json::from_value::<ImageLookupQuery>(json!({"operation": "image2image"})).is_err()
    );
    assert!(serde_json::from_value::<ImageLookupQuery>(json!({"key": "secret"})).is_err());
}

#[test]
fn image_running_projection_preserves_durable_progress_and_safe_frozen_metadata() {
    let (task, usage) = image_task(
        Uuid::nil(),
        projection(ImageResultStatus::Pending {
            stage: VideoPendingStage::Processing,
            provider_progress: Some(progress()),
        }),
    )
    .unwrap();
    let value = serde_json::to_value(task).unwrap();
    assert_eq!(value["status"], "running");
    assert_eq!(value["stage"], "provider_queued");
    assert_eq!(
        value["provider_progress"]["observed_at_ms"],
        1_792_828_821_077_i64
    );
    assert_eq!(value["provider_progress"]["queue_position"], 3341);
    assert_eq!(value["request_id"], "original-request");
    assert_eq!(value["request"]["generate_num"], 1);
    assert_eq!(value["request"]["model_version"], "3.0");
    assert_eq!(value["request"]["ratio"], "16:9");
    assert_eq!(value["request"]["resolution_type"], "2k");
    assert_eq!(value["cancellation"]["supported"], false);
    assert!(value["request"].get("prompt").is_none());
    assert!(value.get("data").is_none());
    assert!(usage.is_none());
}

#[test]
fn image_unknown_progress_does_not_invent_queue_or_eta() {
    let (task, _) = image_task(
        Uuid::nil(),
        projection(ImageResultStatus::Pending {
            stage: VideoPendingStage::Processing,
            provider_progress: None,
        }),
    )
    .unwrap();
    let value = serde_json::to_value(task).unwrap();
    assert_eq!(value["stage"], "processing");
    assert!(value.get("provider_progress").is_none());
}

#[test]
fn image_terminal_states_omit_old_progress_and_uncertain_remains_distinct() {
    for (status, expected, code) in [
        (
            ImageResultStatus::Uncertain,
            "uncertain",
            "generation_uncertain",
        ),
        (ImageResultStatus::Expired, "expired", "artifact_expired"),
        (
            ImageResultStatus::Failed { error_code: None },
            "failed",
            "generation_failed",
        ),
    ] {
        let (task, usage) = image_task(Uuid::nil(), projection(status)).unwrap();
        let value = serde_json::to_value(task).unwrap();
        assert_eq!(value["status"], expected);
        assert_eq!(value["error"]["code"], code);
        assert!(value.get("stage").is_none());
        assert!(value.get("provider_progress").is_none());
        assert!(value.get("data").is_none());
        assert!(value.get("output_counts").is_none());
        assert!(usage.is_none());
    }
}

#[test]
fn image_terminal_known_reason_is_displayed_without_changing_unknown_fallback() {
    for (code, message) in [
        (
            "dreamina_copyright_rejected",
            "生成的视频里包含受版权保护元素",
        ),
        (
            "dreamina_generation_failed",
            "Dreamina image generation failed",
        ),
        ("future_reason", "Dreamina image generation failed"),
    ] {
        let (task, _) = image_task(
            Uuid::nil(),
            projection(ImageResultStatus::Failed {
                error_code: Some(code.to_owned()),
            }),
        )
        .unwrap();
        assert_eq!(task.status, "failed");
        assert!(task.data.is_none());
        let error = task.error.unwrap();
        assert_eq!(error.code, code);
        assert_eq!(error.message, message);
    }
}

fn success(count: usize) -> StoredGenerationResult {
    let mut encoded = std::io::Cursor::new(Vec::new());
    image::RgbaImage::from_pixel(1, 1, image::Rgba([30, 100, 200, 255]))
        .write_to(&mut encoded, image::ImageFormat::Png)
        .unwrap();
    let bytes = encoded.into_inner();
    StoredGenerationResult {
        projection: crate::artifacts::GenerationResponseProjection {
            api_profile: DREAMINA_IMAGES_API_PROFILE.to_owned(),
            operation: GENERATION_OPERATION.to_owned(),
            response_schema: crate::artifacts::GENERATION_RESPONSE_SCHEMA.to_owned(),
            created_at_seconds: 1234,
            output_format: "auto".to_owned(),
            quality: "standard".to_owned(),
            size: "1x1".to_owned(),
            background: "opaque".to_owned(),
            stream: false,
            usage: crate::usage::UsageSnapshot {
                limit_5h: 20,
                remaining_5h: 19,
                limit_7d: 100,
                remaining_7d: 99,
            },
        },
        images: (0..count)
            .map(|_| crate::generator::GeneratedImage {
                bytes: bytes.clone(),
            })
            .collect(),
    }
}

#[test]
fn image_success_reuses_frozen_images_payload_and_usage() {
    let result = success(1);
    let expected_base64 = base64::engine::general_purpose::STANDARD.encode(&result.images[0].bytes);
    let (task, usage) = image_task(
        Uuid::nil(),
        projection(ImageResultStatus::Succeeded {
            result,
            output_counts: DreaminaImageOutputCounts {
                requested: 1,
                delivered: 1,
                failed: 0,
            },
        }),
    )
    .unwrap();
    let value = serde_json::to_value(task).unwrap();
    assert_eq!(value["status"], "succeeded");
    assert_eq!(value["created"], 1234);
    assert_eq!(value["data"], json!([{"b64_json": expected_base64}]));
    assert_eq!(value["output_format"], "png");
    assert_eq!(value["size"], "1x1");
    assert_eq!(
        value["output_counts"],
        json!({"requested": 1, "delivered": 1, "failed": 0})
    );
    assert!(value.get("stage").is_none());
    assert!(value.get("provider_progress").is_none());
    assert_eq!(usage.unwrap().remaining_5h, 19);
    assert!(serde_json::from_value::<DreaminaImageTask>(value).is_ok());
}

#[test]
fn image_success_rejects_more_images_than_the_frozen_request() {
    assert!(
        image_task(
            Uuid::nil(),
            projection(ImageResultStatus::Succeeded {
                result: success(4),
                output_counts: DreaminaImageOutputCounts {
                    requested: 1,
                    delivered: 4,
                    failed: 0
                },
            })
        )
        .is_err()
    );
}

#[test]
fn image_partial_success_keeps_original_request_and_delivered_count() {
    let mut projected = projection(ImageResultStatus::Succeeded {
        result: success(2),
        output_counts: DreaminaImageOutputCounts {
            requested: 3,
            delivered: 2,
            failed: 1,
        },
    });
    projected.request.generate_num = Some(3);
    let (task, _) = image_task(Uuid::nil(), projected).unwrap();
    let value = serde_json::to_value(task).unwrap();
    assert_eq!(value["status"], "succeeded");
    assert_eq!(value["request"]["generate_num"], 3);
    assert_eq!(value["data"].as_array().unwrap().len(), 2);
    assert_eq!(
        value["output_counts"],
        json!({"requested": 3, "delivered": 2, "failed": 1})
    );
}

#[test]
fn image_success_rejects_inconsistent_output_proof() {
    for counts in [
        DreaminaImageOutputCounts {
            requested: 3,
            delivered: 1,
            failed: 1,
        },
        DreaminaImageOutputCounts {
            requested: 3,
            delivered: 0,
            failed: 3,
        },
        DreaminaImageOutputCounts {
            requested: 2,
            delivered: 2,
            failed: 0,
        },
        DreaminaImageOutputCounts {
            requested: 3,
            delivered: 3,
            failed: 0,
        },
    ] {
        let mut projected = projection(ImageResultStatus::Succeeded {
            result: success(2),
            output_counts: counts,
        });
        projected.request.generate_num = Some(3);
        assert!(image_task(Uuid::nil(), projected).is_err());
    }
}

#[test]
fn image_response_metadata_rejects_extra_fields() {
    let mut value: Value = serde_json::to_value(DreaminaImageRequestMetadata::default()).unwrap();
    value["prompt"] = json!("must not escape");
    assert!(serde_json::from_value::<DreaminaImageRequestMetadata>(value).is_err());
}
