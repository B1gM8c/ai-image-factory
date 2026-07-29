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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaTaskCreated {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaVideoTask {
    pub id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DreaminaTaskError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<DreaminaVideoContent>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaVideoContent {
    pub video_url: String,
    pub duration: u8,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreaminaTaskError {
    pub code: String,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

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
