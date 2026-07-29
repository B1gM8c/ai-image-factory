use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const XAI_IMAGES_API_PROFILE: &str = "xai-images-v1";
pub const XAI_IMAGE_GENERATION_COMMAND_SCHEMA: &str = "xai.images.generations.v1";
pub const XAI_MAX_IMAGES_PER_REQUEST: u32 = 10;
const MIN_STORAGE_TTL_SECONDS: u32 = 3_600;
const MAX_STORAGE_TTL_SECONDS: u32 = 2_592_000;

/// Versioned xAI wire DTO. Unknown fields fail closed so paid requests never lose semantics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiImageGenerationRequest {
    /// Official xAI field. The OpenAPI default is `auto`.
    #[serde(default)]
    pub aspect_ratio: Option<XaiImageAspectRatio>,
    /// Official xAI field. Provider bindings decide which concrete model names they execute.
    #[serde(default)]
    pub model: Option<String>,
    /// Official xAI supports 1 through 10 outputs. A CLI may expose a smaller subset.
    #[serde(default)]
    pub n: Option<u32>,
    pub prompt: String,
    /// Official xAI supports `1k` and `2k`; the OpenAPI default is `1k`.
    #[serde(default)]
    pub resolution: Option<XaiImageResolution>,
    /// Official default is `url`; privacy-first bindings may require explicit `b64_json`.
    #[serde(default)]
    pub response_format: Option<XaiImageResponseFormat>,
    /// Retained for wire compatibility even when a selected binding has no Files API support.
    #[serde(default)]
    pub storage_options: Option<XaiImageStorageOptions>,
    /// Official end-user attribution field. It is not a provider credential or routing key.
    #[serde(default)]
    pub user: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum XaiImageAspectRatio {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "1:1")]
    R1x1,
    #[serde(rename = "3:4")]
    R3x4,
    #[serde(rename = "4:3")]
    R4x3,
    #[serde(rename = "9:16")]
    R9x16,
    #[serde(rename = "16:9")]
    R16x9,
    #[serde(rename = "2:3")]
    R2x3,
    #[serde(rename = "3:2")]
    R3x2,
    #[serde(rename = "9:19.5")]
    R9x19_5,
    #[serde(rename = "19.5:9")]
    R19_5x9,
    #[serde(rename = "9:20")]
    R9x20,
    #[serde(rename = "20:9")]
    R20x9,
    #[serde(rename = "1:2")]
    R1x2,
    #[serde(rename = "2:1")]
    R2x1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum XaiImageResolution {
    #[serde(rename = "1k")]
    R1k,
    #[serde(rename = "2k")]
    R2k,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum XaiImageResponseFormat {
    #[serde(rename = "url")]
    Url,
    #[serde(rename = "b64_json")]
    B64Json,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiImageStorageOptions {
    /// Official file lifetime in seconds. Omission requests permanent storage.
    #[serde(default)]
    pub expires_after: Option<u32>,
    /// Official stored filename; the factory also applies local path-safety validation.
    pub filename: String,
    /// Official boolean-or-object public URL control.
    #[serde(default)]
    pub public_url: Option<XaiPublicUrlOptions>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum XaiPublicUrlOptions {
    Enabled(bool),
    Options(XaiPublicUrlConfig),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiPublicUrlConfig {
    #[serde(default)]
    pub expires_after: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiImagesResponse {
    pub data: Vec<XaiImageData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<XaiImageUsage>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiImageData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub b64_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_output: Option<XaiImageFileOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Kept for compatibility with older official examples; the current OpenAPI omits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revised_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiImageFileOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    pub file_id: String,
    pub filename: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url_expires_at: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XaiImageUsage {
    pub cost_in_usd_ticks: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct XaiImageGenerationCommandV1 {
    pub schema_version: u16,
    pub operation: String,
    pub model: Option<String>,
    pub prompt: String,
    pub n: u32,
    pub aspect_ratio: XaiImageAspectRatio,
    pub resolution: XaiImageResolution,
    pub response_format: XaiImageResponseFormat,
    pub storage_options: Option<XaiImageStorageOptions>,
    pub user: Option<String>,
}

impl XaiImageGenerationCommandV1 {
    pub fn from_request(request: XaiImageGenerationRequest) -> Result<Self, XaiRequestError> {
        if request.prompt.trim().is_empty() || request.prompt.contains('\0') {
            return Err(XaiRequestError::InvalidPrompt);
        }
        if request
            .n
            .is_some_and(|count| !(1..=XAI_MAX_IMAGES_PER_REQUEST).contains(&count))
        {
            return Err(XaiRequestError::InvalidOutputCount);
        }
        if request
            .model
            .as_deref()
            .is_some_and(|model| model.trim().is_empty() || model.chars().any(char::is_control))
        {
            return Err(XaiRequestError::InvalidModel);
        }
        if request
            .user
            .as_deref()
            .is_some_and(|user| user.is_empty() || user.chars().any(char::is_control))
        {
            return Err(XaiRequestError::InvalidUser);
        }
        if let Some(storage) = &request.storage_options {
            validate_storage(storage)?;
        }
        Ok(Self {
            schema_version: 1,
            operation: "images.generations".to_owned(),
            model: request.model,
            prompt: request.prompt,
            n: request.n.unwrap_or(1),
            aspect_ratio: request.aspect_ratio.unwrap_or(XaiImageAspectRatio::Auto),
            resolution: request.resolution.unwrap_or(XaiImageResolution::R1k),
            response_format: request
                .response_format
                .unwrap_or(XaiImageResponseFormat::Url),
            storage_options: request.storage_options,
            user: request.user,
        })
    }

    pub fn canonical_sha256_hex(&self) -> String {
        let bytes = serde_json::to_vec(self)
            .expect("xAI image generation command serialization cannot fail");
        hex::encode(Sha256::digest(bytes))
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum XaiRequestError {
    #[error("xAI image prompt is invalid")]
    InvalidPrompt,
    #[error("xAI image output count must be between 1 and 10")]
    InvalidOutputCount,
    #[error("xAI image model is invalid")]
    InvalidModel,
    #[error("xAI image user is invalid")]
    InvalidUser,
    #[error("xAI image storage options are invalid")]
    InvalidStorageOptions,
}

impl XaiRequestError {
    pub const fn parameter(self) -> &'static str {
        match self {
            Self::InvalidPrompt => "prompt",
            Self::InvalidOutputCount => "n",
            Self::InvalidModel => "model",
            Self::InvalidUser => "user",
            Self::InvalidStorageOptions => "storage_options",
        }
    }
}

fn validate_storage(storage: &XaiImageStorageOptions) -> Result<(), XaiRequestError> {
    // The length and control-character checks are factory path-safety policy; xAI does not
    // currently publish a filename length limit.
    if storage.filename.is_empty()
        || storage.filename.len() > 255
        || storage.filename.chars().any(char::is_control)
        || storage.expires_after.is_some_and(|seconds| {
            !(MIN_STORAGE_TTL_SECONDS..=MAX_STORAGE_TTL_SECONDS).contains(&seconds)
        })
        || public_url_expiry(storage.public_url.as_ref()).is_some_and(|seconds| {
            !(MIN_STORAGE_TTL_SECONDS..=MAX_STORAGE_TTL_SECONDS).contains(&seconds)
                || storage
                    .expires_after
                    .is_some_and(|file_expiry| seconds > file_expiry)
        })
    {
        Err(XaiRequestError::InvalidStorageOptions)
    } else {
        Ok(())
    }
}

fn public_url_expiry(public_url: Option<&XaiPublicUrlOptions>) -> Option<u32> {
    match public_url {
        Some(XaiPublicUrlOptions::Options(options)) => options.expires_after,
        Some(XaiPublicUrlOptions::Enabled(_)) | None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_generation_example_normalizes_to_a_stable_command() {
        let request: XaiImageGenerationRequest = serde_json::from_str(
            r#"{
                "model":"grok-imagine-image-quality",
                "prompt":"A collage of London landmarks"
            }"#,
        )
        .unwrap();
        let command = XaiImageGenerationCommandV1::from_request(request).unwrap();

        assert_eq!(command.schema_version, 1);
        assert_eq!(command.operation, "images.generations");
        assert_eq!(command.n, 1);
        assert_eq!(command.aspect_ratio, XaiImageAspectRatio::Auto);
        assert_eq!(command.resolution, XaiImageResolution::R1k);
        assert_eq!(command.response_format, XaiImageResponseFormat::Url);
        assert_eq!(
            command.canonical_sha256_hex(),
            "f1d5fad48efe97fbdad0dae694de8d616111d4bafd073deefe9fb07c67a3ce7e"
        );

        let explicit_defaults: XaiImageGenerationRequest = serde_json::from_str(
            r#"{
                "aspect_ratio":"auto",
                "model":"grok-imagine-image-quality",
                "n":1,
                "prompt":"A collage of London landmarks",
                "resolution":"1k",
                "response_format":"url"
            }"#,
        )
        .unwrap();
        assert_eq!(
            command,
            XaiImageGenerationCommandV1::from_request(explicit_defaults).unwrap()
        );
    }

    #[test]
    fn official_options_round_trip_without_provider_projection() {
        let request: XaiImageGenerationRequest = serde_json::from_str(
            r#"{
                "aspect_ratio":"19.5:9",
                "model":"grok-imagine-image",
                "n":2,
                "prompt":"city",
                "resolution":"2k",
                "response_format":"b64_json",
                "storage_options":{
                    "expires_after":3600,
                    "filename":"city.jpg",
                    "public_url":true
                },
                "user":"customer-1"
            }"#,
        )
        .unwrap();
        let command = XaiImageGenerationCommandV1::from_request(request).unwrap();
        let encoded = serde_json::to_value(command).unwrap();

        assert_eq!(encoded["aspect_ratio"], "19.5:9");
        assert_eq!(encoded["resolution"], "2k");
        assert_eq!(encoded["response_format"], "b64_json");
        assert_eq!(encoded["storage_options"]["public_url"], true);
    }

    #[test]
    fn unknown_or_invalid_fields_fail_closed() {
        assert!(
            serde_json::from_str::<XaiImageGenerationRequest>(
                r#"{"prompt":"image","quality":"high"}"#
            )
            .is_err()
        );
        let request: XaiImageGenerationRequest = serde_json::from_str(
            r#"{
                "prompt":"image",
                "storage_options":{"expires_after":2592001,"filename":"image.jpg"}
            }"#,
        )
        .unwrap();
        assert_eq!(
            XaiImageGenerationCommandV1::from_request(request),
            Err(XaiRequestError::InvalidStorageOptions)
        );

        let too_many: XaiImageGenerationRequest =
            serde_json::from_str(r#"{"prompt":"image","n":11}"#).unwrap();
        assert_eq!(
            XaiImageGenerationCommandV1::from_request(too_many),
            Err(XaiRequestError::InvalidOutputCount)
        );
    }

    #[test]
    fn official_storage_expiry_constraints_are_enforced() {
        let request = |file_expiry, public_expiry| XaiImageGenerationRequest {
            aspect_ratio: None,
            model: Some("grok-imagine-image-quality".to_owned()),
            n: None,
            prompt: "image".to_owned(),
            resolution: None,
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: Some(XaiImageStorageOptions {
                expires_after: file_expiry,
                filename: "image.jpg".to_owned(),
                public_url: Some(XaiPublicUrlOptions::Options(XaiPublicUrlConfig {
                    expires_after: public_expiry,
                })),
            }),
            user: None,
        };

        assert!(
            XaiImageGenerationCommandV1::from_request(request(Some(7_200), Some(3_600))).is_ok()
        );
        assert_eq!(
            XaiImageGenerationCommandV1::from_request(request(Some(3_600), Some(7_200))),
            Err(XaiRequestError::InvalidStorageOptions)
        );
        assert_eq!(
            XaiImageGenerationCommandV1::from_request(request(Some(3_599), None)),
            Err(XaiRequestError::InvalidStorageOptions)
        );
    }

    #[test]
    fn official_response_shape_round_trips_without_local_paths() {
        let response: XaiImagesResponse = serde_json::from_str(
            r#"{
                "data":[{
                    "b64_json":"aW1hZ2U=",
                    "mime_type":"image/jpeg",
                    "revised_prompt":"",
                    "file_output":{
                        "file_id":"file_123",
                        "filename":"image.jpg",
                        "expires_at":1700000000,
                        "public_url":"https://files.example/image.jpg",
                        "public_url_expires_at":1699990000
                    }
                }],
                "usage":{"cost_in_usd_ticks":200000000}
            }"#,
        )
        .unwrap();
        let encoded = serde_json::to_value(response).unwrap();

        assert_eq!(encoded["data"][0]["b64_json"], "aW1hZ2U=");
        assert_eq!(encoded["data"][0]["file_output"]["file_id"], "file_123");
        assert_eq!(encoded["usage"]["cost_in_usd_ticks"], 200_000_000_u64);
        assert!(encoded["data"][0].get("url").is_none());
    }
}
