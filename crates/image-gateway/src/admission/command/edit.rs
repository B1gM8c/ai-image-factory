use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::generator::EditJob;

pub const EDIT_COMMAND_SCHEMA_VERSION: u16 = 1;
pub const EDIT_COMMAND_SCHEMA: &str = "openai.images.edit.v1";
pub const EDIT_INPUT_MANIFEST_SCHEMA: &str = "openai.images.edit.inputs.v1";
pub const EDIT_OPERATION: &str = "edit";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EditInputRoleV1 {
    Image,
    Mask,
}

impl EditInputRoleV1 {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Mask => "mask",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EditInputDescriptorV1 {
    pub byte_size: u64,
    pub index: u16,
    pub media_type: String,
    pub role: EditInputRoleV1,
    pub sha256_hex: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EditCommandV1 {
    pub background: String,
    pub inputs: Vec<EditInputDescriptorV1>,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moderation: Option<String>,
    pub n: u32,
    pub operation: String,
    pub output_compression: Option<u8>,
    pub output_format: String,
    pub partial_images: u32,
    pub prompt: String,
    pub provider_id: String,
    pub quality: String,
    pub schema_version: u16,
    pub size: String,
    pub source_api_profile: String,
    pub stream: bool,
}

impl EditCommandV1 {
    pub fn from_edit_job(
        job: &EditJob,
        inputs: Vec<EditInputDescriptorV1>,
        source_api_profile: impl Into<String>,
        provider_id: impl Into<String>,
    ) -> Self {
        Self {
            background: job.background.clone(),
            inputs,
            model: job.model.clone(),
            moderation: (job.moderation != "auto").then(|| job.moderation.clone()),
            n: job.n,
            operation: EDIT_OPERATION.to_string(),
            output_compression: job.output_compression,
            output_format: job.output_format.clone(),
            partial_images: job.partial_images,
            prompt: job.prompt.clone(),
            provider_id: provider_id.into(),
            quality: job.quality.clone(),
            schema_version: EDIT_COMMAND_SCHEMA_VERSION,
            size: job.size.clone(),
            source_api_profile: source_api_profile.into(),
            stream: job.stream,
        }
    }

    pub fn canonical_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("EditCommandV1 serialization cannot fail")
    }

    pub fn request_hash_hex(&self) -> String {
        hex::encode(Sha256::digest(self.canonical_json_bytes()))
    }

    pub fn input_manifest_hash_hex(&self) -> String {
        let inputs = serde_json::to_vec(&self.inputs)
            .expect("EditInputDescriptorV1 serialization cannot fail");
        hex::encode(Sha256::digest(inputs))
    }
}
