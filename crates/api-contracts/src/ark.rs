use serde::{Deserialize, Serialize};

pub const ARK_IMAGES_API_PROFILE: &str = "volcengine-ark-images-v3";
pub const ARK_CONTENT_GENERATION_API_PROFILE: &str = "volcengine-ark-content-generation-v3";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArkImageGenerationRequest {
    pub model: String,
    pub prompt: String,
    #[serde(default)]
    pub image: Option<ArkStringOrStrings>,
    #[serde(default)]
    pub response_format: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub seed: Option<i64>,
    #[serde(default)]
    pub guidance_scale: Option<f64>,
    #[serde(default)]
    pub watermark: Option<bool>,
    #[serde(default)]
    pub optimize_prompt: Option<bool>,
    #[serde(default)]
    pub optimize_prompt_options: Option<ArkOptimizePromptOptions>,
    #[serde(default)]
    pub sequential_image_generation: Option<String>,
    #[serde(default)]
    pub sequential_image_generation_options: Option<ArkSequentialImageGenerationOptions>,
    #[serde(default)]
    pub tools: Option<Vec<ArkContentGenerationTool>>,
    #[serde(default)]
    pub output_format: Option<String>,
    #[serde(default)]
    pub stream: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ArkStringOrStrings {
    One(String),
    Many(Vec<String>),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArkOptimizePromptOptions {
    #[serde(default)]
    pub thinking: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArkSequentialImageGenerationOptions {
    #[serde(default)]
    pub max_images: Option<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArkContentGenerationTool {
    #[serde(rename = "type")]
    pub tool_type: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArkContentGenerationTaskRequest {
    pub model: String,
    pub content: Vec<ArkContentItem>,
    #[serde(default)]
    pub safety_identifier: Option<String>,
    #[serde(default)]
    pub callback_url: Option<String>,
    #[serde(default)]
    pub return_last_frame: Option<bool>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub execution_expires_after: Option<u32>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub generate_audio: Option<bool>,
    #[serde(default)]
    pub draft: Option<bool>,
    #[serde(default)]
    pub camera_fixed: Option<bool>,
    #[serde(default)]
    pub watermark: Option<bool>,
    #[serde(default)]
    pub seed: Option<i64>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub ratio: Option<String>,
    #[serde(default)]
    pub duration: Option<i16>,
    #[serde(default)]
    pub frames: Option<u32>,
    #[serde(default)]
    pub tools: Option<Vec<ArkContentGenerationTool>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArkContentItem {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image_url")]
    ImageUrl {
        image_url: ArkMediaUrl,
        #[serde(default)]
        role: Option<String>,
    },
    #[serde(rename = "audio_url")]
    AudioUrl {
        audio_url: ArkMediaUrl,
        role: String,
    },
    #[serde(rename = "video_url")]
    VideoUrl {
        video_url: ArkMediaUrl,
        role: String,
    },
    #[serde(rename = "draft_task")]
    DraftTask { draft_task: ArkDraftTaskRef },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArkMediaUrl {
    pub url: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArkDraftTaskRef {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkImageGenerationResponse {
    pub model: String,
    /// Field name used by the public ImageGenerations API documentation.
    pub created: i64,
    /// Field name used by the current official Ark Python SDK.
    pub created_at: i64,
    pub data: Vec<ArkImageData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ArkContentGenerationError>,
    pub usage: ArkImageUsage,
    pub tool: Vec<ArkContentGenerationTool>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkImageData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub b64_json: Option<String>,
    pub size: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkImageUsage {
    pub generated_images: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkContentGenerationTaskId {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safety_identifier: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkContentGenerationTask {
    pub id: String,
    pub model: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ArkContentGenerationError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<ArkGeneratedContent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ArkContentGenerationUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ArkContentGenerationTaskMetadata>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkContentGenerationTaskMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_progress: Option<crate::dreamina::DreaminaProviderProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_cancellation: Option<crate::dreamina::DreaminaManualCancellation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkContentGenerationError {
    pub message: String,
    pub code: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkGeneratedContent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_frame_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArkContentGenerationUsage {
    pub completion_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_metadata_preserves_legacy_shape_and_exact_upstream_observation() {
        let legacy =
            serde_json::json!({"id":"cgt-original","model":"seedance2.5","status":"failed"});
        let task: ArkContentGenerationTask = serde_json::from_value(legacy.clone()).unwrap();
        assert!(task.metadata.is_none());
        assert_eq!(serde_json::to_value(task).unwrap(), legacy);
        let queued = serde_json::json!({"id":"cgt-original","model":"seedance2.5","status":"running", "metadata":{
            "stage":"provider_queued", "provider_progress":{"provider_status":"Queueing","queue_position":610,"queue_total":233061,"estimated_remaining_seconds":5880,"observed_at_ms":1792995932000_i64}
        }});
        let task: ArkContentGenerationTask = serde_json::from_value(queued.clone()).unwrap();
        assert_eq!(serde_json::to_value(task).unwrap(), queued);
    }

    #[test]
    fn content_roles_and_nested_fields_are_not_silently_discarded() {
        let item: ArkContentItem = serde_json::from_str(
            r#"{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}"#,
        )
        .unwrap();
        assert!(matches!(item, ArkContentItem::ImageUrl { role: None, .. }));
        for json in [
            r#"{"type":"text","text":"move","role":"reference_image"}"#,
            r#"{"type":"image_url","image_url":{"url":"https://example.com/a.png","width":1024}}"#,
            r#"{"type":"video_url","video_url":{"url":"https://example.com/a.mp4"},"role":"reference_video","duration":3}"#,
        ] {
            assert!(serde_json::from_str::<ArkContentItem>(json).is_err());
        }
    }

    #[test]
    fn image_request_matches_the_official_ark_wire_shape() {
        let request: ArkImageGenerationRequest = serde_json::from_str(
            r#"{
                "model":"doubao-seedream-5-0-260128",
                "prompt":"a city",
                "size":"2K",
                "sequential_image_generation":"auto",
                "sequential_image_generation_options":{"max_images":3},
                "response_format":"b64_json",
                "stream":false
            }"#,
        )
        .unwrap();
        assert_eq!(request.model, "doubao-seedream-5-0-260128");
        assert_eq!(
            request
                .sequential_image_generation_options
                .and_then(|options| options.max_images),
            Some(3)
        );
    }

    #[test]
    fn video_content_matches_the_official_tagged_union() {
        let request: ArkContentGenerationTaskRequest = serde_json::from_str(
            r#"{
                "model":"doubao-seedance-2-0-fast-260128",
                "content":[{"type":"text","text":"camera push in"}],
                "resolution":"720p",
                "ratio":"16:9",
                "duration":5
            }"#,
        )
        .unwrap();
        assert!(matches!(
            request.content.as_slice(),
            [ArkContentItem::Text { text }] if text == "camera push in"
        ));
    }

    #[test]
    fn top_level_unknown_fields_fail_closed() {
        assert!(
            serde_json::from_str::<ArkImageGenerationRequest>(
                r#"{"model":"doubao-seedream-5-0-260128","prompt":"city","n":2}"#
            )
            .is_err()
        );
    }
}
