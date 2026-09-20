use image_api_contracts::xai::{
    XAI_VIDEOS_API_PROFILE, XaiVideoGenerationCommandV1, XaiVideoGenerationCommandV2,
    XaiVideoGenerationRequest, XaiVideoRequestError, XaiVideoWorkflow,
};
use image_provider_grok_cli::{
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA, GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2,
    GrokCommandError, GrokVideoGenerationPayloadV1, GrokVideoGenerationPayloadV2,
    GrokVideoGenerationRequestV1, PROVIDER_ID, StagedImageV1, VIDEO_ADAPTER_REVISION,
    VIDEO_ADAPTER_REVISION_V2, XaiGrokVideoProjectionError,
};
use image_provider_sdk::{CanonicalCommandPayload, OutputSlot};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::input_blobs::InputBlobRef;

use super::{
    AdmissionContract, AdmissionTicket, AttachInputManifest, AttachInputObject, AttachJob,
    ClaimAdmission, EditInputRoleV1,
};

pub const VIDEO_GENERATION_OPERATION: &str = "video_generation";
pub const XAI_VIDEO_INPUT_MANIFEST_SCHEMA: &str = "xai.videos.inputs.v1";
pub const XAI_VIDEO_INPUT_MANIFEST_SCHEMA_V2: &str = "xai.videos.inputs.v2";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XaiVideoInputRoleV2 {
    FirstFrame,
    LastFrame,
    ReferenceImage,
    Keyframe,
}

impl XaiVideoInputRoleV2 {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FirstFrame => "first_frame",
            Self::LastFrame => "last_frame",
            Self::ReferenceImage => "reference_image",
            Self::Keyframe => "keyframe",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XaiVideoAdmissionInput {
    filename: String,
    blob: InputBlobRef,
    media_type: String,
    role_v2: Option<XaiVideoInputRoleV2>,
    role_index_v2: u8,
}

impl XaiVideoAdmissionInput {
    pub fn new(
        filename: impl Into<String>,
        blob: InputBlobRef,
        media_type: impl Into<String>,
    ) -> Result<Self, XaiVideoAdmissionError> {
        let filename = filename.into();
        let media_type = media_type.into();
        StagedImageV1::new(&filename, &blob.sha256_hex)
            .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?;
        if blob.byte_size == 0
            || !matches!(
                media_type.as_str(),
                "image/png" | "image/jpeg" | "image/webp"
            )
        {
            return Err(XaiVideoAdmissionError::InvalidInputManifest);
        }
        Ok(Self {
            filename,
            blob,
            media_type,
            role_v2: None,
            role_index_v2: 0,
        })
    }

    fn new_v2(
        filename: impl Into<String>,
        blob: InputBlobRef,
        media_type: impl Into<String>,
        role: XaiVideoInputRoleV2,
        role_index: u8,
    ) -> Result<Self, XaiVideoAdmissionError> {
        let mut input = Self::new(filename, blob, media_type)?;
        input.role_v2 = Some(role);
        input.role_index_v2 = role_index;
        Ok(input)
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn blob(&self) -> &InputBlobRef {
        &self.blob
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub fn role(&self) -> XaiVideoInputRoleV2 {
        self.role_v2.unwrap_or(XaiVideoInputRoleV2::ReferenceImage)
    }

    pub fn role_index(&self) -> u8 {
        self.role_index_v2
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XaiVideoAdmissionIntent {
    source_command: XaiVideoGenerationCommandV1,
    source_command_v2: Option<XaiVideoGenerationCommandV2>,
    source_request_hash: String,
}

impl XaiVideoAdmissionIntent {
    pub fn new(request: XaiVideoGenerationRequest) -> Result<Self, XaiVideoAdmissionError> {
        let source_command = XaiVideoGenerationCommandV1::from_request(request)?;
        let source_request_hash = source_command.canonical_sha256_hex();
        Ok(Self {
            source_command,
            source_command_v2: None,
            source_request_hash,
        })
    }

    /// Constructs the explicit 1.0.34/Grok CLI V2 admission path.  V1 remains
    /// the default constructor so replayed requests cannot be silently
    /// reinterpreted merely because they use the same public model name.
    pub fn new_v2(request: XaiVideoGenerationRequest) -> Result<Self, XaiVideoAdmissionError> {
        let source_command_v2 = XaiVideoGenerationCommandV2::from_request(request.clone())?;
        let source_command = XaiVideoGenerationCommandV1 {
            schema_version: 1,
            operation: source_command_v2.operation.clone(),
            aspect_ratio: source_command_v2.aspect_ratio,
            duration: source_command_v2.duration,
            image: None,
            model: source_command_v2.model.clone(),
            output: source_command_v2.output.clone(),
            prompt: source_command_v2.prompt.clone(),
            reference_images: Vec::new(),
            resolution: source_command_v2.resolution,
            storage_options: source_command_v2.storage_options.clone(),
            user: source_command_v2.user.clone(),
        };
        Ok(Self {
            source_command,
            source_request_hash: source_command_v2.canonical_sha256_hex(),
            source_command_v2: Some(source_command_v2),
        })
    }

    pub fn source_command(&self) -> &XaiVideoGenerationCommandV1 {
        &self.source_command
    }

    pub fn source_request_hash(&self) -> &str {
        &self.source_request_hash
    }

    pub fn source_command_v2(&self) -> Option<&XaiVideoGenerationCommandV2> {
        self.source_command_v2.as_ref()
    }

    pub fn claim(
        &self,
        owner_token: Uuid,
        tenant_id: impl Into<String>,
        project_id: impl Into<String>,
        request_id: impl Into<String>,
        idempotency_key_digest: Option<String>,
        deadline_at_ms: i64,
    ) -> ClaimAdmission {
        ClaimAdmission {
            owner_token,
            tenant_id: tenant_id.into(),
            project_id: project_id.into(),
            api_profile: XAI_VIDEOS_API_PROFILE.to_owned(),
            operation: VIDEO_GENERATION_OPERATION.to_owned(),
            request_id: request_id.into(),
            idempotency_key_digest,
            request_hash: self.source_request_hash.clone(),
            deadline_at_ms,
        }
    }

    pub fn bind_grok_cli(
        self,
        inputs: Vec<XaiVideoAdmissionInput>,
    ) -> Result<XaiVideoAdmissionPlan, XaiVideoAdmissionError> {
        XaiVideoAdmissionPlan::from_intent(self, inputs)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XaiVideoAdmissionPlan {
    source_command: XaiVideoGenerationCommandV1,
    source_command_v2: Option<XaiVideoGenerationCommandV2>,
    source_request_hash: String,
    provider_model: String,
    provider_command: Value,
    inputs: Vec<XaiVideoAdmissionInput>,
    input_manifest_hash: String,
}

impl XaiVideoAdmissionPlan {
    pub fn for_grok_cli(
        request: XaiVideoGenerationRequest,
        inputs: Vec<XaiVideoAdmissionInput>,
    ) -> Result<Self, XaiVideoAdmissionError> {
        XaiVideoAdmissionIntent::new(request)?.bind_grok_cli(inputs)
    }

    pub fn for_grok_cli_v2(
        request: XaiVideoGenerationRequest,
        inputs: Vec<XaiVideoAdmissionInput>,
    ) -> Result<Self, XaiVideoAdmissionError> {
        XaiVideoAdmissionIntent::new_v2(request)?.bind_grok_cli(inputs)
    }

    fn from_intent(
        intent: XaiVideoAdmissionIntent,
        inputs: Vec<XaiVideoAdmissionInput>,
    ) -> Result<Self, XaiVideoAdmissionError> {
        if let Some(source_command_v2) = intent.source_command_v2.clone() {
            return Self::from_intent_v2(
                intent.source_command,
                source_command_v2,
                intent.source_request_hash,
                inputs,
            );
        }
        let source_command = intent.source_command;
        let expected_inputs = match source_command.workflow() {
            XaiVideoWorkflow::TextToVideo => 0,
            XaiVideoWorkflow::ImageToVideo => 1,
            XaiVideoWorkflow::ReferenceToVideo => source_command.reference_images.len(),
        };
        if inputs.len() != expected_inputs {
            return Err(XaiVideoAdmissionError::InvalidInputManifest);
        }
        let staged_images = inputs
            .iter()
            .map(|input| StagedImageV1::new(&input.filename, &input.blob.sha256_hex))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?;
        let source_request_hash = intent.source_request_hash;
        let payload =
            GrokVideoGenerationPayloadV1::from_xai_command(source_command.clone(), staged_images)
                .map_err(|error| match error {
                GrokCommandError::VideoSourceProjection(error) => {
                    XaiVideoAdmissionError::UnsupportedBinding(error)
                }
                _ => XaiVideoAdmissionError::InvalidProviderCommand,
            })?;
        let provider_model = match payload.request() {
            GrokVideoGenerationRequestV1::TextToVideo(_) => "grok-imagine-video-1.5-preview",
            GrokVideoGenerationRequestV1::ImageToVideo(_) => "grok-imagine-video-1.5-preview",
            GrokVideoGenerationRequestV1::ReferenceToVideo(_) => "grok-imagine-video",
        }
        .to_owned();
        let provider_command =
            serde_json::from_slice(&payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap()))
                .map_err(|_| XaiVideoAdmissionError::InvalidProviderCommand)?;
        let input_manifest_hash = input_manifest_hash(&inputs)?;
        Ok(Self {
            source_command,
            source_command_v2: None,
            source_request_hash,
            provider_model,
            provider_command,
            inputs,
            input_manifest_hash,
        })
    }

    fn from_intent_v2(
        source_command: XaiVideoGenerationCommandV1,
        source_command_v2: XaiVideoGenerationCommandV2,
        source_request_hash: String,
        inputs: Vec<XaiVideoAdmissionInput>,
    ) -> Result<Self, XaiVideoAdmissionError> {
        let expected_inputs = usize::from(source_command_v2.image.is_some())
            + usize::from(source_command_v2.last_frame.is_some())
            + source_command_v2.reference_images.len()
            + source_command_v2.keyframes.len();
        if inputs.len() != expected_inputs {
            return Err(XaiVideoAdmissionError::InvalidInputManifest);
        }
        let mut semantic_inputs = Vec::with_capacity(inputs.len());
        let first_count = usize::from(source_command_v2.image.is_some());
        let last_count = usize::from(source_command_v2.last_frame.is_some());
        let reference_count = source_command_v2.reference_images.len();
        for (position, input) in inputs.into_iter().enumerate() {
            let (role, role_index) = if position < first_count {
                (XaiVideoInputRoleV2::FirstFrame, 0)
            } else if position < first_count + last_count {
                (XaiVideoInputRoleV2::LastFrame, 0)
            } else if position < first_count + last_count + reference_count {
                (
                    XaiVideoInputRoleV2::ReferenceImage,
                    u8::try_from(position - first_count - last_count)
                        .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?,
                )
            } else {
                (
                    XaiVideoInputRoleV2::Keyframe,
                    u8::try_from(position - first_count - last_count - reference_count)
                        .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?,
                )
            };
            semantic_inputs.push(XaiVideoAdmissionInput::new_v2(
                input.filename,
                input.blob,
                input.media_type,
                role,
                role_index,
            )?);
        }
        let staged_images = semantic_inputs
            .iter()
            .map(|input| StagedImageV1::new(&input.filename, &input.blob.sha256_hex))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?;
        let payload = GrokVideoGenerationPayloadV2::from_xai_command(
            source_command_v2.clone(),
            staged_images,
        )
        .map_err(XaiVideoAdmissionError::UnsupportedBindingV2)?;
        let provider_command = serde_json::from_slice(
            &payload
                .clone()
                .into_canonical_bytes(OutputSlot::new(0, 1).unwrap()),
        )
        .map_err(|_| XaiVideoAdmissionError::InvalidProviderCommand)?;
        let input_manifest_hash = input_manifest_hash_v2(&semantic_inputs)?;
        Ok(Self {
            source_command,
            source_command_v2: Some(source_command_v2),
            source_request_hash,
            provider_model: "grok-imagine-video-1.5".to_owned(),
            provider_command,
            inputs: semantic_inputs,
            input_manifest_hash,
        })
    }

    pub fn source_command(&self) -> &XaiVideoGenerationCommandV1 {
        &self.source_command
    }

    pub fn source_request_hash(&self) -> &str {
        &self.source_request_hash
    }

    pub fn source_command_v2(&self) -> Option<&XaiVideoGenerationCommandV2> {
        self.source_command_v2.as_ref()
    }

    pub fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }

    pub fn provider_model(&self) -> &str {
        &self.provider_model
    }

    pub fn command_schema(&self) -> &'static str {
        if self.source_command_v2.is_some() {
            GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2
        } else {
            GROK_VIDEO_GENERATION_COMMAND_SCHEMA
        }
    }

    pub fn command_json(&self) -> &Value {
        &self.provider_command
    }

    pub fn adapter_revision(&self) -> &'static str {
        if self.source_command_v2.is_some() {
            VIDEO_ADAPTER_REVISION_V2
        } else {
            VIDEO_ADAPTER_REVISION
        }
    }

    pub fn inputs(&self) -> &[XaiVideoAdmissionInput] {
        &self.inputs
    }

    pub fn reference_image_count(&self) -> usize {
        self.source_command_v2
            .as_ref()
            .map_or(self.source_command.reference_images.len(), |command| {
                command.reference_images.len()
            })
    }

    pub fn input_manifest_hash(&self) -> &str {
        &self.input_manifest_hash
    }

    pub fn output_count(&self) -> u32 {
        1
    }

    pub fn billing_units(&self) -> u32 {
        u32::from(self.source_command.duration)
    }

    pub fn schedule_cost(&self) -> u64 {
        u64::from(self.source_command.duration)
    }

    pub fn claim(
        &self,
        owner_token: Uuid,
        tenant_id: impl Into<String>,
        project_id: impl Into<String>,
        request_id: impl Into<String>,
        idempotency_key_digest: Option<String>,
        deadline_at_ms: i64,
    ) -> ClaimAdmission {
        XaiVideoAdmissionIntent {
            source_command: self.source_command.clone(),
            source_command_v2: self.source_command_v2.clone(),
            source_request_hash: self.source_request_hash.clone(),
        }
        .claim(
            owner_token,
            tenant_id,
            project_id,
            request_id,
            idempotency_key_digest,
            deadline_at_ms,
        )
    }

    pub fn attach(
        &self,
        ticket: AdmissionTicket,
        job_id: Uuid,
        schedule_scope: impl Into<String>,
        contract: AdmissionContract,
    ) -> AttachJob {
        let inputs: Vec<AttachInputObject> = self
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input)| AttachInputObject {
                blob: input.blob.clone(),
                role: EditInputRoleV1::Image,
                index: u16::try_from(index).expect("xAI video input count is bounded"),
                media_type: input.media_type.clone(),
            })
            .collect();
        let input_manifest = (!inputs.is_empty()).then(|| AttachInputManifest {
            manifest_schema: if self.source_command_v2.is_some() {
                XAI_VIDEO_INPUT_MANIFEST_SCHEMA_V2.to_owned()
            } else {
                XAI_VIDEO_INPUT_MANIFEST_SCHEMA.to_owned()
            },
            manifest_hash: self.input_manifest_hash.clone(),
            inputs,
        });
        AttachJob {
            ticket,
            job_id,
            command_schema: self.command_schema().to_owned(),
            command_json: self.provider_command.clone(),
            input_manifest,
            work_kind: "video_single".to_owned(),
            schedule_scope: schedule_scope.into(),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: self.schedule_cost(),
            contract,
            customer_pricing: None,
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum XaiVideoAdmissionError {
    #[error(transparent)]
    InvalidRequest(#[from] XaiVideoRequestError),
    #[error(transparent)]
    UnsupportedBinding(#[from] XaiGrokVideoProjectionError),
    #[error(transparent)]
    UnsupportedBindingV2(#[from] image_provider_grok_cli::XaiGrokVideoProjectionErrorV2),
    #[error("xAI video sealed input manifest is invalid")]
    InvalidInputManifest,
    #[error("xAI video provider command is invalid")]
    InvalidProviderCommand,
}

#[derive(Serialize)]
struct ManifestDescriptor<'a> {
    byte_size: u64,
    filename: &'a str,
    index: u16,
    media_type: &'a str,
    role: &'static str,
    sha256_hex: &'a str,
}

#[derive(Serialize)]
struct ManifestDescriptorV2<'a> {
    byte_size: u64,
    filename: &'a str,
    index: u16,
    media_type: &'a str,
    role: &'static str,
    role_index: u8,
    sha256_hex: &'a str,
}

fn input_manifest_hash(
    inputs: &[XaiVideoAdmissionInput],
) -> Result<String, XaiVideoAdmissionError> {
    let descriptors = inputs
        .iter()
        .enumerate()
        .map(
            |(index, input)| -> Result<ManifestDescriptor<'_>, XaiVideoAdmissionError> {
                Ok(ManifestDescriptor {
                    byte_size: input.blob.byte_size,
                    filename: &input.filename,
                    index: u16::try_from(index)
                        .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?,
                    media_type: &input.media_type,
                    role: EditInputRoleV1::Image.as_str(),
                    sha256_hex: &input.blob.sha256_hex,
                })
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    let bytes = serde_json::to_vec(&descriptors)
        .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn input_manifest_hash_v2(
    inputs: &[XaiVideoAdmissionInput],
) -> Result<String, XaiVideoAdmissionError> {
    let descriptors = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            Ok(ManifestDescriptorV2 {
                byte_size: input.blob.byte_size,
                filename: &input.filename,
                index: u16::try_from(index)
                    .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?,
                media_type: &input.media_type,
                role: input.role().as_str(),
                role_index: input.role_index(),
                sha256_hex: &input.blob.sha256_hex,
            })
        })
        .collect::<Result<Vec<_>, XaiVideoAdmissionError>>()?;
    let bytes = serde_json::to_vec(&descriptors)
        .map_err(|_| XaiVideoAdmissionError::InvalidInputManifest)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(super) fn video_input_manifest_hash_matches(
    expected_images: &[StagedImageV1],
    manifest: &AttachInputManifest,
) -> bool {
    let descriptors = expected_images
        .iter()
        .zip(&manifest.inputs)
        .map(|(expected, input)| ManifestDescriptor {
            byte_size: input.blob.byte_size,
            filename: expected.filename(),
            index: input.index,
            media_type: &input.media_type,
            role: input.role.as_str(),
            sha256_hex: &input.blob.sha256_hex,
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&descriptors)
        .map(|bytes| hex::encode(Sha256::digest(bytes)) == manifest.manifest_hash)
        .unwrap_or(false)
}

pub(super) fn video_input_manifest_hash_matches_v2(
    expected_images: &[(XaiVideoInputRoleV2, u8, StagedImageV1)],
    manifest: &AttachInputManifest,
) -> bool {
    if expected_images.len() != manifest.inputs.len() {
        return false;
    }
    let descriptors = expected_images
        .iter()
        .zip(&manifest.inputs)
        .enumerate()
        .map(|(index, ((role, role_index, expected), input))| {
            if input.index != index as u16 {
                return None;
            }
            Some(ManifestDescriptorV2 {
                byte_size: input.blob.byte_size,
                filename: expected.filename(),
                index: index as u16,
                media_type: &input.media_type,
                role: role.as_str(),
                role_index: *role_index,
                sha256_hex: &input.blob.sha256_hex,
            })
        })
        .collect::<Option<Vec<_>>>();
    let Some(descriptors) = descriptors else {
        return false;
    };
    serde_json::to_vec(&descriptors)
        .map(|bytes| hex::encode(Sha256::digest(bytes)) == manifest.manifest_hash)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::{
        XaiVideoAudioReference, XaiVideoImageUrl, XaiVideoKeyframe, XaiVideoResolution,
    };

    use super::*;
    use crate::input_blobs::InputBlobKey;

    fn input() -> XaiVideoAdmissionInput {
        input_for_session(Uuid::new_v4())
    }

    fn input_for_session(admission_session_id: Uuid) -> XaiVideoAdmissionInput {
        XaiVideoAdmissionInput::new(
            "source.png",
            InputBlobRef {
                key: InputBlobKey {
                    admission_session_id,
                    input_id: Uuid::new_v4(),
                },
                storage_backend: "filesystem-v1".to_owned(),
                object_key: "inputs/source".to_owned(),
                sha256_hex: "a".repeat(64),
                byte_size: 128,
            },
            "image/png",
        )
        .unwrap()
    }

    fn request(duration: Option<u8>) -> XaiVideoGenerationRequest {
        XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration,
            generate_audio: None,
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".to_owned()),
            }),
            keyframes: Vec::new(),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: Some("slow camera movement".to_owned()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        }
    }

    fn text_request() -> XaiVideoGenerationRequest {
        XaiVideoGenerationRequest {
            aspect_ratio: Some(image_api_contracts::xai::XaiVideoAspectRatio::R9x16),
            duration: Some(10),
            generate_audio: None,
            image: None,
            keyframes: Vec::new(),
            last_frame: None,
            model: Some("grok-imagine-video-1.5-preview".to_owned()),
            output: None,
            prompt: Some("a paper boat crossing a moonlit lake".to_owned()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P720),
            storage_options: None,
            user: None,
        }
    }

    fn v2_reference_request() -> XaiVideoGenerationRequest {
        XaiVideoGenerationRequest {
            aspect_ratio: Some(image_api_contracts::xai::XaiVideoAspectRatio::R16x9),
            duration: Some(8),
            generate_audio: Some(true),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".to_owned()),
            }),
            keyframes: Vec::new(),
            last_frame: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".to_owned()),
            }),
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: None,
            reference_audios: vec![XaiVideoAudioReference {
                url: None,
                voice_id: Some("eve".to_owned()),
            }],
            reference_images: vec![XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".to_owned()),
            }],
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        }
    }

    #[test]
    fn v2_manifest_binds_first_last_reference_and_keyframe_roles() {
        let session_id = Uuid::new_v4();
        let mut request = v2_reference_request();
        request.keyframes.push(XaiVideoKeyframe {
            image: XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".to_owned()),
            },
            timestamp_s: 2.0,
        });
        let inputs = (0..4)
            .map(|index| {
                XaiVideoAdmissionInput::new(
                    format!("input-{index}.png"),
                    InputBlobRef {
                        key: InputBlobKey {
                            admission_session_id: session_id,
                            input_id: Uuid::from_u128(index as u128 + 1),
                        },
                        storage_backend: "test".to_owned(),
                        object_key: format!("input-{index}.png"),
                        sha256_hex: format!("{index:064x}"),
                        byte_size: 128,
                    },
                    "image/png",
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let plan = XaiVideoAdmissionPlan::for_grok_cli_v2(request, inputs).unwrap();
        assert_eq!(
            plan.command_schema(),
            image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2
        );
        assert_eq!(
            plan.adapter_revision(),
            image_provider_grok_cli::VIDEO_ADAPTER_REVISION_V2
        );
        assert_eq!(plan.inputs().len(), 4);
        assert_eq!(plan.inputs()[0].role(), XaiVideoInputRoleV2::FirstFrame);
        assert_eq!(plan.inputs()[1].role(), XaiVideoInputRoleV2::LastFrame);
        assert_eq!(plan.inputs()[2].role(), XaiVideoInputRoleV2::ReferenceImage);
        assert_eq!(plan.inputs()[2].role_index(), 0);
        assert_eq!(plan.inputs()[3].role(), XaiVideoInputRoleV2::Keyframe);
        assert_eq!(plan.inputs()[3].role_index(), 0);
        assert!(plan.input_manifest_hash().len() == 64);

        let attach = plan.attach(
            AdmissionTicket {
                session_id,
                owner_token: Uuid::new_v4(),
                request_hash: plan.source_request_hash().to_owned(),
            },
            Uuid::new_v4(),
            "tenant:test",
            AdmissionContract::MediaEconomicsV3,
        );
        crate::admission::validate_attach_request(&attach).unwrap();

        let mut role_tamper = attach.clone();
        role_tamper.input_manifest.as_mut().unwrap().inputs[0].role = EditInputRoleV1::Mask;
        assert!(crate::admission::validate_attach_request(&role_tamper).is_err());

        let mut order_tamper = attach.clone();
        order_tamper
            .input_manifest
            .as_mut()
            .unwrap()
            .inputs
            .swap(0, 1);
        assert!(crate::admission::validate_attach_request(&order_tamper).is_err());

        let mut digest_tamper = attach.clone();
        digest_tamper.input_manifest.as_mut().unwrap().inputs[2]
            .blob
            .sha256_hex = "f".repeat(64);
        assert!(crate::admission::validate_attach_request(&digest_tamper).is_err());

        let mut semantic_role_tamper = attach.clone();
        let manifest = semantic_role_tamper.input_manifest.as_mut().unwrap();
        let descriptors = manifest
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input)| ManifestDescriptorV2 {
                byte_size: input.blob.byte_size,
                filename: plan.inputs()[index].filename(),
                index: index as u16,
                media_type: &input.media_type,
                role: match index {
                    0 => "last_frame",
                    1 => "last_frame",
                    _ => "reference_image",
                },
                role_index: 0,
                sha256_hex: &input.blob.sha256_hex,
            })
            .collect::<Vec<_>>();
        manifest.manifest_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&descriptors).expect("tampered V2 descriptor encoding"),
        ));
        assert!(crate::admission::validate_attach_request(&semantic_role_tamper).is_err());
    }

    #[test]
    fn voice_only_v2_reference_has_no_input_manifest() {
        let mut request = v2_reference_request();
        request.image = None;
        request.last_frame = None;
        request.reference_images.clear();
        request.keyframes.clear();
        let plan = XaiVideoAdmissionPlan::for_grok_cli_v2(request, Vec::new()).unwrap();
        assert!(plan.inputs().is_empty());
        assert!(
            plan.attach(
                AdmissionTicket {
                    session_id: Uuid::new_v4(),
                    owner_token: Uuid::new_v4(),
                    request_hash: plan.source_request_hash().to_owned(),
                },
                Uuid::new_v4(),
                "tenant:test",
                AdmissionContract::MediaEconomicsV3,
            )
            .input_manifest
            .is_none()
        );
        let attach = plan.attach(
            AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: Uuid::new_v4(),
                request_hash: plan.source_request_hash().to_owned(),
            },
            Uuid::new_v4(),
            "tenant:test",
            AdmissionContract::MediaEconomicsV3,
        );
        crate::admission::validate_attach_request(&attach).unwrap();

        let mut empty_manifest = attach;
        empty_manifest.input_manifest = Some(AttachInputManifest {
            manifest_schema: XAI_VIDEO_INPUT_MANIFEST_SCHEMA_V2.to_owned(),
            manifest_hash: "a".repeat(64),
            inputs: Vec::new(),
        });
        assert!(crate::admission::validate_attach_request(&empty_manifest).is_err());
    }

    #[test]
    fn first_last_plus_seven_references_has_seven_reference_count() {
        let mut request = v2_reference_request();
        request.reference_images = (0..7)
            .map(|_| XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,AA==".to_owned()),
            })
            .collect();
        let session_id = Uuid::new_v4();
        let inputs = (0..9)
            .map(|index| {
                XaiVideoAdmissionInput::new(
                    format!("input-{index}.png"),
                    InputBlobRef {
                        key: InputBlobKey {
                            admission_session_id: session_id,
                            input_id: Uuid::from_u128(index as u128 + 1),
                        },
                        storage_backend: "test".to_owned(),
                        object_key: format!("input-{index}.png"),
                        sha256_hex: format!("{index:064x}"),
                        byte_size: 128,
                    },
                    "image/png",
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let plan = XaiVideoAdmissionPlan::for_grok_cli_v2(request, inputs).unwrap();
        assert_eq!(plan.reference_image_count(), 7);
        assert_eq!(plan.inputs().len(), 9);
        let attach = plan.attach(
            AdmissionTicket {
                session_id,
                owner_token: Uuid::new_v4(),
                request_hash: plan.source_request_hash().to_owned(),
            },
            Uuid::new_v4(),
            "tenant:test",
            AdmissionContract::MediaEconomicsV3,
        );
        crate::admission::validate_attach_request(&attach).unwrap();
    }

    #[test]
    fn plan_separates_output_cardinality_billing_and_schedule_cost() {
        let plan = XaiVideoAdmissionPlan::for_grok_cli(request(Some(6)), vec![input()]).unwrap();
        assert_eq!(plan.output_count(), 1);
        assert_eq!(plan.billing_units(), 6);
        assert_eq!(plan.schedule_cost(), 6);
        assert_eq!(plan.provider_model(), "grok-imagine-video-1.5-preview");
        assert_eq!(plan.input_manifest_hash().len(), 64);
        assert_eq!(
            plan.claim(Uuid::new_v4(), "tenant", "project", "request", None, 1)
                .api_profile,
            XAI_VIDEOS_API_PROFILE
        );
    }

    #[test]
    fn official_eight_second_default_is_rejected_instead_of_downgraded() {
        assert_eq!(
            XaiVideoAdmissionPlan::for_grok_cli(request(None), vec![input()]),
            Err(XaiVideoAdmissionError::UnsupportedBinding(
                XaiGrokVideoProjectionError::UnsupportedDuration
            ))
        );
    }

    #[test]
    fn source_request_and_input_content_have_independent_hashes() {
        let first = XaiVideoAdmissionPlan::for_grok_cli(request(Some(6)), vec![input()]).unwrap();
        let mut changed = input();
        changed.blob.sha256_hex = "b".repeat(64);
        let second = XaiVideoAdmissionPlan::for_grok_cli(request(Some(6)), vec![changed]).unwrap();
        assert_eq!(first.source_request_hash(), second.source_request_hash());
        assert_ne!(first.input_manifest_hash(), second.input_manifest_hash());
        assert_ne!(first.command_json(), second.command_json());
    }

    #[test]
    fn intent_claim_precedes_session_bound_provider_plan() {
        let intent = XaiVideoAdmissionIntent::new(request(Some(6))).unwrap();
        let session_id = Uuid::new_v4();
        let ticket = AdmissionTicket {
            session_id,
            owner_token: Uuid::new_v4(),
            request_hash: intent.source_request_hash().to_owned(),
        };
        let plan = intent
            .bind_grok_cli(vec![input_for_session(session_id)])
            .unwrap();
        let attach = plan.attach(
            ticket,
            Uuid::new_v4(),
            "tenant:tenant-1",
            AdmissionContract::MediaEconomicsV3,
        );

        crate::admission::validate_attach_request(&attach).unwrap();
        assert_eq!(attach.contract, AdmissionContract::MediaEconomicsV3);
        assert_eq!(attach.schedule_cost, 6);

        let mut forged = attach.clone();
        forged.input_manifest.as_mut().unwrap().inputs[0]
            .blob
            .sha256_hex = "b".repeat(64);
        assert!(crate::admission::validate_attach_request(&forged).is_err());
    }

    #[test]
    fn text_video_attach_is_a_single_input_free_durable_job() {
        let intent = XaiVideoAdmissionIntent::new(text_request()).unwrap();
        let session_id = Uuid::new_v4();
        let ticket = AdmissionTicket {
            session_id,
            owner_token: Uuid::new_v4(),
            request_hash: intent.source_request_hash().to_owned(),
        };
        let plan = intent.bind_grok_cli(Vec::new()).unwrap();
        let attach = plan.attach(
            ticket,
            Uuid::new_v4(),
            "tenant:tenant-1",
            AdmissionContract::MediaEconomicsV3,
        );

        assert!(attach.input_manifest.is_none());
        assert_eq!(attach.work_kind, "video_single");
        assert_eq!(attach.schedule_cost, 10);
        crate::admission::validate_attach_request(&attach).unwrap();
    }

    #[test]
    fn video_workflows_reject_the_wrong_input_cardinality() {
        assert_eq!(
            XaiVideoAdmissionPlan::for_grok_cli(text_request(), vec![input()]),
            Err(XaiVideoAdmissionError::InvalidInputManifest)
        );
        assert_eq!(
            XaiVideoAdmissionPlan::for_grok_cli(request(Some(6)), Vec::new()),
            Err(XaiVideoAdmissionError::InvalidInputManifest)
        );
        assert_eq!(
            XaiVideoAdmissionPlan::for_grok_cli(request(Some(6)), vec![input(), input()]),
            Err(XaiVideoAdmissionError::InvalidInputManifest)
        );
    }
}
