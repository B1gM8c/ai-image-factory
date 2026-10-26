use serde::{Deserialize, Serialize};

pub const DREAMINA_IMAGES_API_PROFILE: &str = "dreamina-cli-images-v1";
pub const DREAMINA_VIDEOS_API_PROFILE: &str = "dreamina-cli-videos-v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageGenerationRequest {
    pub prompt: String,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub ratio: Option<String>,
    pub resolution_type: String,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub generate_num: Option<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaVideoGenerationRequest {
    pub prompt: String,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub ratio: Option<String>,
    #[serde(default)]
    pub duration: Option<u8>,
    pub video_resolution: String,
}

/// Missing `workflow` retains the original text-generation contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum DreaminaImageRequest {
    Legacy(DreaminaImageGenerationRequest),
    Workflow(DreaminaImageWorkflowRequest),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "workflow", deny_unknown_fields)]
pub enum DreaminaImageWorkflowRequest {
    #[serde(rename = "text2image")]
    TextToImage(DreaminaImageGenerationRequest),
    #[serde(rename = "image2image")]
    ImageToImage(DreaminaImageToImageRequest),
    #[serde(rename = "image_upscale")]
    ImageUpscale(DreaminaImageUpscaleRequest),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageToImageRequest {
    pub images: Vec<String>,
    pub prompt: String,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub ratio: Option<String>,
    pub resolution_type: String,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub generate_num: Option<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageUpscaleRequest {
    pub image: String,
    pub resolution_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum DreaminaVideoRequest {
    Legacy(DreaminaVideoGenerationRequest),
    Workflow(DreaminaVideoWorkflowRequest),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "workflow", deny_unknown_fields)]
pub enum DreaminaVideoWorkflowRequest {
    #[serde(rename = "text2video")]
    TextToVideo(DreaminaVideoGenerationRequest),
    #[serde(rename = "image2video")]
    ImageToVideo(DreaminaImageToVideoRequest),
    #[serde(rename = "frames2video")]
    FramesToVideo(DreaminaFramesToVideoRequest),
    #[serde(rename = "multiframe2video")]
    MultiframeToVideo(DreaminaMultiframeToVideoRequest),
    #[serde(rename = "multimodal2video")]
    MultimodalToVideo(DreaminaMultimodalToVideoRequest),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageToVideoRequest {
    pub image: String,
    pub prompt: String,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub ratio: Option<String>,
    #[serde(default)]
    pub duration: Option<u8>,
    pub video_resolution: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaFramesToVideoRequest {
    pub first: String,
    pub last: String,
    pub prompt: String,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub ratio: Option<String>,
    #[serde(default)]
    pub duration: Option<u8>,
    pub video_resolution: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaMultiframeToVideoRequest {
    pub images: Vec<String>,
    pub video_resolution: String,
    #[serde(default)]
    pub transitions: Option<Vec<DreaminaVideoTransition>>,
    #[serde(default)]
    pub transition_prompt: Option<Vec<String>>,
    #[serde(default)]
    pub transition_duration: Option<Vec<serde_json::Number>>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub duration: Option<serde_json::Number>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaVideoTransition {
    pub prompt: String,
    #[serde(default)]
    pub duration: Option<serde_json::Number>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaMultimodalToVideoRequest {
    #[serde(default)]
    pub image: Vec<String>,
    #[serde(default)]
    pub video: Vec<String>,
    #[serde(default)]
    pub audio: Vec<String>,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub model_version: Option<String>,
    #[serde(default)]
    pub ratio: Option<String>,
    #[serde(default)]
    pub duration: Option<u8>,
    pub video_resolution: String,
}

fn native_request_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<serde_json::Value, D::Error> {
    let mut value = serde_json::Value::deserialize(deserializer)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| serde::de::Error::custom("Dreamina request must be an object"))?;
    // Submission is asynchronous and CLI sessions belong to the service account.
    // Explicit defaults are equivalent to omission; other values need a gateway contract.
    for field in ["session", "poll"] {
        if let Some(option) = object.remove(field)
            && option.as_u64() != Some(0)
        {
            return Err(serde::de::Error::custom(format!(
                "unsupported_parameter: `{field}` supports only 0"
            )));
        }
    }
    Ok(value)
}

impl<'de> Deserialize<'de> for DreaminaImageRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = native_request_value(deserializer)?;
        if value.get("workflow").is_some() {
            serde_json::from_value(value).map(Self::Workflow)
        } else {
            serde_json::from_value(value).map(Self::Legacy)
        }
        .map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for DreaminaVideoRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = native_request_value(deserializer)?;
        if value.get("workflow").is_some() {
            serde_json::from_value(value).map(Self::Workflow)
        } else {
            serde_json::from_value(value).map(Self::Legacy)
        }
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaTaskCreated {
    pub id: String,
}

/// Read-only native image task. Image payload fields are present only after success.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageTask {
    pub id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_progress: Option<DreaminaProviderProgress>,
    pub cancellation: DreaminaCancellation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<DreaminaImageRequestMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DreaminaTaskError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<DreaminaImageData>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_counts: Option<DreaminaImageOutputCounts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageData {
    pub b64_json: String,
}

/// Final image delivery quantities; the original requested quantity is retained.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageOutputCounts {
    pub requested: u32,
    pub delivered: u32,
    pub failed: u32,
}

/// Specifications frozen at admission; excludes prompts and input locations.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaImageRequestMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_num: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ratio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaVideoTask {
    pub id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_progress: Option<DreaminaProviderProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<DreaminaCancellation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DreaminaTaskError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<DreaminaVideoContent>,
    /// Present only for an audited operator resolution backed by a fully released hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement: Option<DreaminaReleasedSettlement>,
    /// Operator-confirmed cancellation classification; ordinary failure is not proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_cancellation: Option<DreaminaManualCancellation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaManualCancellation {
    pub schema: String,
    pub status: String,
    pub authority_id: String,
    pub job_id: String,
    pub request_id: String,
    pub upstream_task_id: String,
    pub submission_id: String,
    pub hold_id: String,
    pub confirmed_at_ms: i64,
    pub currency: String,
    /// Monetary micros are decimal strings to preserve exact integer precision.
    pub held_micros: String,
    pub captured_micros: String,
    pub released_micros: String,
    /// Original durable terminal event; this classification creates no refund ledger entry.
    pub release_event_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaProviderProgress {
    pub provider_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_total: Option<u64>,
    /// The provider's estimate at observed_at_ms, not a completion guarantee.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_remaining_seconds: Option<u64>,
    /// Time of the durable provider observation; reading a task does not refresh it.
    pub observed_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaCancellation {
    pub supported: bool,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaReleasedSettlement {
    pub state: String,
    pub resolution_audit_id: String,
    pub job_id: String,
    pub request_id: String,
    pub hold_id: String,
    pub currency: String,
    /// Monetary micros are decimal strings to preserve exact integer precision.
    pub held_micros: String,
    pub released_micros: String,
    pub captured_micros: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaVideoContent {
    pub video_url: String,
    pub duration: serde_json::Number,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaTaskError {
    pub code: String,
    pub message: String,
    /// A safe, bounded cause for an unconfirmed submission; not a retry or billing decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_code: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_error_reason_is_optional_and_does_not_change_legacy_json() {
        let legacy = serde_json::json!({
            "code": "generation_uncertain", "message": "Keep the original task"
        });
        let error: DreaminaTaskError = serde_json::from_value(legacy.clone()).unwrap();
        assert!(error.reason_code.is_none());
        assert_eq!(serde_json::to_value(error).unwrap(), legacy);

        let diagnosed = serde_json::json!({
            "code": "generation_uncertain", "message": "Keep the original task",
            "reason_code": "provider_submit_timeout"
        });
        let error: DreaminaTaskError = serde_json::from_value(diagnosed.clone()).unwrap();
        assert_eq!(serde_json::to_value(error).unwrap(), diagnosed);
    }

    #[test]
    fn video_release_receipt_is_optional_and_keeps_exact_decimal_money() {
        let legacy = r#"{"id":"task-a","status":"running"}"#;
        let task: DreaminaVideoTask = serde_json::from_str(legacy).unwrap();
        assert!(task.settlement.is_none());
        assert!(task.manual_cancellation.is_none());
        assert!(
            serde_json::to_value(task)
                .unwrap()
                .get("settlement")
                .is_none()
        );

        let receipt = serde_json::json!({
            "id": "task-a", "status": "failed",
            "settlement": {
                "state": "released",
                "resolution_audit_id": "00000000-0000-4000-8000-000000000001",
                "job_id": "00000000-0000-4000-8000-000000000002",
                "request_id": "req_a", "hold_id": "00000000-0000-4000-8000-000000000003",
                "currency": "USD", "held_micros": "9007199254740993",
                "released_micros": "9007199254740993", "captured_micros": "0"
            }
        });
        let parsed: DreaminaVideoTask = serde_json::from_value(receipt.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), receipt);
    }

    #[test]
    fn provider_progress_is_optional_and_preserves_unknown_queue_values() {
        let legacy = serde_json::json!({"id":"task-a","status":"running"});
        let task: DreaminaVideoTask = serde_json::from_value(legacy.clone()).unwrap();
        assert!(task.stage.is_none());
        assert!(task.provider_progress.is_none());
        assert!(task.cancellation.is_none());
        assert_eq!(serde_json::to_value(task).unwrap(), legacy);

        let queued = serde_json::json!({
            "id":"task-a", "status":"running", "stage":"provider_queued",
            "provider_progress":{"provider_status":"Queueing", "observed_at_ms":1792828821077_i64},
            "cancellation":{"supported":false,"reason":"provider_cancellation_unsupported"}
        });
        let task: DreaminaVideoTask = serde_json::from_value(queued.clone()).unwrap();
        assert!(
            task.provider_progress
                .as_ref()
                .unwrap()
                .queue_position
                .is_none()
        );
        assert!(
            task.provider_progress
                .as_ref()
                .unwrap()
                .queue_total
                .is_none()
        );
        assert!(
            task.provider_progress
                .as_ref()
                .unwrap()
                .estimated_remaining_seconds
                .is_none()
        );
        assert_eq!(serde_json::to_value(task).unwrap(), queued);
    }

    #[test]
    fn all_native_workflows_have_strict_distinct_wire_shapes() {
        for json in [
            r#"{"workflow":"text2image","prompt":"city","resolution_type":"2k"}"#,
            r#"{"workflow":"image2image","images":["https://example.com/a.png"],"prompt":"city","resolution_type":"2k"}"#,
            r#"{"workflow":"image_upscale","image":"https://example.com/a.png","resolution_type":"8k"}"#,
        ] {
            let request: DreaminaImageRequest = serde_json::from_str(json).unwrap();
            assert!(matches!(request, DreaminaImageRequest::Workflow(_)));
        }
        for json in [
            r#"{"workflow":"text2video","prompt":"move","video_resolution":"720p"}"#,
            r#"{"workflow":"image2video","image":"https://example.com/a.png","prompt":"move","video_resolution":"720p"}"#,
            r#"{"workflow":"frames2video","first":"https://example.com/a.png","last":"https://example.com/b.png","prompt":"move","video_resolution":"720p"}"#,
            r#"{"workflow":"multiframe2video","images":["a","b"],"prompt":"move","duration":2.501,"video_resolution":"720p"}"#,
            r#"{"workflow":"multimodal2video","audio":["https://example.com/a.wav"],"model_version":"seedance2.5","video_resolution":"480p"}"#,
        ] {
            let request: DreaminaVideoRequest = serde_json::from_str(json).unwrap();
            assert!(matches!(request, DreaminaVideoRequest::Workflow(_)));
        }
        for json in [
            r#"{"workflow":"image_upscale","image":"a","resolution_type":"8k","prompt":"ignored"}"#,
            r#"{"workflow":"image_upscale","image":"a","resolution_type":"8k","model_version":"5.0"}"#,
        ] {
            assert!(serde_json::from_str::<DreaminaImageRequest>(json).is_err());
        }
        for json in [
            r#"{"workflow":"multiframe2video","images":["a","b"],"prompt":"move","video_resolution":"720p","ratio":"16:9"}"#,
            r#"{"workflow":"image2video","image":"a","prompt":"move","video_resolution":"720p","duration_ms":5000}"#,
            r#"{"workflow":"unknown","prompt":"move","video_resolution":"720p"}"#,
        ] {
            assert!(serde_json::from_str::<DreaminaVideoRequest>(json).is_err());
        }
    }

    #[test]
    fn explicit_server_control_defaults_preserve_legacy_request() {
        let base = r#"{"prompt":"city","resolution_type":"2k"}"#;
        let explicit = r#"{"prompt":"city","resolution_type":"2k","session":0,"poll":0}"#;
        assert_eq!(
            serde_json::from_str::<DreaminaImageRequest>(base).unwrap(),
            serde_json::from_str::<DreaminaImageRequest>(explicit).unwrap()
        );
        for option in [r#""poll":1"#, r#""session":5"#, r#""session":null"#] {
            let json = format!(r#"{{"prompt":"city","resolution_type":"2k",{option}}}"#);
            let error = serde_json::from_str::<DreaminaImageRequest>(&json).unwrap_err();
            assert!(error.to_string().contains("unsupported_parameter"));
        }
    }

    #[test]
    fn decimal_request_keeps_submillisecond_digits_until_validation() {
        let request: DreaminaVideoRequest = serde_json::from_str(r#"{"workflow":"multiframe2video","images":["a","b"],"prompt":"move","duration":2.0000000000000001,"video_resolution":"720p"}"#).unwrap();
        let DreaminaVideoRequest::Workflow(DreaminaVideoWorkflowRequest::MultiframeToVideo(
            request,
        )) = request
        else {
            panic!("wrong workflow")
        };
        assert_eq!(request.duration.unwrap().to_string(), "2.0000000000000001");
        // Integer commands keep the original JSON representation with the feature enabled.
        assert_eq!(
            serde_json::to_string(&serde_json::json!({"schema_version":1,"duration":5})).unwrap(),
            r#"{"duration":5,"schema_version":1}"#
        );
    }

    #[test]
    fn cli_wire_fields_are_strict_and_keep_custom_dimensions() {
        let request: DreaminaImageGenerationRequest = serde_json::from_str(
            r#"{"prompt":"city","model_version":"5.0Pro","resolution_type":"2k","width":1536,"height":1024,"generate_num":2}"#,
        )
        .unwrap();
        assert_eq!(request.model_version.as_deref(), Some("5.0Pro"));
        assert_eq!((request.width, request.height), (Some(1536), Some(1024)));
        assert!(
            serde_json::from_str::<DreaminaImageGenerationRequest>(
                r#"{"prompt":"city","resolution_type":"2k","unknown":true}"#
            )
            .is_err()
        );
    }

    #[test]
    fn video_wire_fields_match_the_cli_guide() {
        let request: DreaminaVideoGenerationRequest = serde_json::from_str(
            r#"{"prompt":"camera push in","model_version":"seedance2.0fast","ratio":"16:9","duration":5,"video_resolution":"720p"}"#,
        )
        .unwrap();
        assert_eq!(request.duration, Some(5));
        assert_eq!(request.video_resolution, "720p");
    }
}
