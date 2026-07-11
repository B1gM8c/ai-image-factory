use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::generator::GenerationJob;

mod edit;

pub use edit::{
    EDIT_COMMAND_SCHEMA, EDIT_COMMAND_SCHEMA_VERSION, EDIT_INPUT_MANIFEST_SCHEMA, EDIT_OPERATION,
    EditCommandV1, EditInputDescriptorV1, EditInputRoleV1,
};

pub const GENERATION_COMMAND_SCHEMA_VERSION: u16 = 1;
pub const GENERATION_COMMAND_SCHEMA: &str = "openai.images.generation.v1";
pub const GENERATION_OPERATION: &str = "generation";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GenerationCommandV1 {
    pub background: String,
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

impl GenerationCommandV1 {
    pub fn from_generation_job(
        job: &GenerationJob,
        source_api_profile: impl Into<String>,
        provider_id: impl Into<String>,
    ) -> Self {
        Self {
            background: job.background.clone(),
            model: job.model.clone(),
            moderation: (job.moderation != "auto").then(|| job.moderation.clone()),
            n: job.n,
            operation: GENERATION_OPERATION.to_string(),
            output_compression: job.output_compression,
            output_format: job.output_format.clone(),
            partial_images: job.partial_images,
            prompt: job.prompt.clone(),
            provider_id: provider_id.into(),
            quality: job.quality.clone(),
            schema_version: GENERATION_COMMAND_SCHEMA_VERSION,
            size: job.size.clone(),
            source_api_profile: source_api_profile.into(),
            stream: job.stream,
        }
    }

    pub fn canonical_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("GenerationCommandV1 serialization cannot fail")
    }

    pub fn request_hash_hex(&self) -> String {
        hex::encode(Sha256::digest(self.canonical_json_bytes()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum IdempotencyKeyError {
    #[error("idempotency key must contain between 1 and 255 bytes")]
    InvalidLength,
    #[error("idempotency key must contain only visible ASCII characters")]
    NonVisibleAscii,
}

pub fn validate_idempotency_key(key: &str) -> Result<(), IdempotencyKeyError> {
    if !(1..=255).contains(&key.len()) {
        return Err(IdempotencyKeyError::InvalidLength);
    }
    if !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(IdempotencyKeyError::NonVisibleAscii);
    }
    Ok(())
}

pub fn idempotency_key_digest(
    project_id: &str,
    api_profile: &str,
    operation: &str,
    key: &str,
) -> Result<String, IdempotencyKeyError> {
    validate_idempotency_key(key)?;
    let mut hasher = Sha256::new();
    hasher.update(b"ai-image-factory:idempotency-key:v1");
    for part in [project_id, api_profile, operation, key] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation_job() -> GenerationJob {
        GenerationJob {
            request_id: "request-1".to_string(),
            model: "gpt-image-2".to_string(),
            prompt: "a red fox".to_string(),
            moderation: "auto".to_string(),
            n: 2,
            size: "1024x1024".to_string(),
            quality: "high".to_string(),
            output_format: "png".to_string(),
            output_compression: Some(80),
            background: "auto".to_string(),
            stream: false,
            partial_images: 0,
        }
    }

    fn command() -> GenerationCommandV1 {
        GenerationCommandV1::from_generation_job(
            &generation_job(),
            "openai-images-v1",
            "openai-codex",
        )
    }

    #[test]
    fn same_command_has_stable_canonical_json_and_hash() {
        let command = command();
        let expected_json = r#"{"background":"auto","model":"gpt-image-2","n":2,"operation":"generation","output_compression":80,"output_format":"png","partial_images":0,"prompt":"a red fox","provider_id":"openai-codex","quality":"high","schema_version":1,"size":"1024x1024","source_api_profile":"openai-images-v1","stream":false}"#;

        assert_eq!(command.canonical_json_bytes(), expected_json.as_bytes());
        assert_eq!(
            command.canonical_json_bytes(),
            command.canonical_json_bytes()
        );
        assert_eq!(
            command.request_hash_hex(),
            "d682742ff44cc1610ef182fe229d57102de146a45c20c2f6570a61163e5fd638"
        );
    }

    #[test]
    fn canonical_json_is_independent_of_input_field_order() {
        let canonical = command().canonical_json_bytes();
        let reordered = br#"{
            "stream": false,
            "source_api_profile": "openai-images-v1",
            "size": "1024x1024",
            "schema_version": 1,
            "quality": "high",
            "provider_id": "openai-codex",
            "prompt": "a red fox",
            "partial_images": 0,
            "output_format": "png",
            "output_compression": 80,
            "operation": "generation",
            "n": 2,
            "model": "gpt-image-2",
            "background": "auto"
        }"#;
        let parsed: GenerationCommandV1 = serde_json::from_slice(reordered).unwrap();

        assert_eq!(parsed.canonical_json_bytes(), canonical);
    }

    #[test]
    fn request_hash_changes_when_any_command_field_changes() {
        let base = command();
        let base_hash = base.request_hash_hex();
        let variants = [
            GenerationCommandV1 {
                schema_version: 2,
                ..base.clone()
            },
            GenerationCommandV1 {
                source_api_profile: "native-v1".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                operation: "edit".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                provider_id: "other-provider".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                model: "other-model".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                moderation: Some("low".to_string()),
                ..base.clone()
            },
            GenerationCommandV1 {
                prompt: "a blue fox".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                n: 1,
                ..base.clone()
            },
            GenerationCommandV1 {
                size: "1536x1024".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                quality: "medium".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                output_format: "webp".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                output_compression: None,
                ..base.clone()
            },
            GenerationCommandV1 {
                background: "transparent".to_string(),
                ..base.clone()
            },
            GenerationCommandV1 {
                stream: true,
                ..base.clone()
            },
            GenerationCommandV1 {
                partial_images: 1,
                ..base.clone()
            },
        ];

        for variant in variants {
            assert_ne!(variant.request_hash_hex(), base_hash);
        }
    }

    #[test]
    fn idempotency_digest_is_stable_and_scoped() {
        let digest =
            idempotency_key_digest("project-a", "openai-images-v1", "generation", "retry-123")
                .unwrap();

        assert_eq!(
            digest,
            idempotency_key_digest("project-a", "openai-images-v1", "generation", "retry-123")
                .unwrap()
        );
        assert_ne!(
            digest,
            idempotency_key_digest("project-b", "openai-images-v1", "generation", "retry-123")
                .unwrap()
        );
        assert_ne!(
            digest,
            idempotency_key_digest("project-a", "native-v1", "generation", "retry-123").unwrap()
        );
        assert_ne!(
            digest,
            idempotency_key_digest("project-a", "openai-images-v1", "edit", "retry-123").unwrap()
        );
    }

    #[test]
    fn idempotency_key_accepts_visible_ascii_at_length_boundaries() {
        assert_eq!(validate_idempotency_key("!"), Ok(()));
        assert_eq!(validate_idempotency_key(&"~".repeat(255)), Ok(()));
    }

    #[test]
    fn idempotency_key_rejects_invalid_lengths_and_characters() {
        assert_eq!(
            validate_idempotency_key(""),
            Err(IdempotencyKeyError::InvalidLength)
        );
        assert_eq!(
            validate_idempotency_key(&"a".repeat(256)),
            Err(IdempotencyKeyError::InvalidLength)
        );

        for key in [
            "has space",
            "line\nbreak",
            "tab\tkey",
            "delete\u{7f}",
            "caf\u{e9}",
        ] {
            assert_eq!(
                validate_idempotency_key(key),
                Err(IdempotencyKeyError::NonVisibleAscii)
            );
        }
    }

    fn edit_job() -> crate::generator::EditJob {
        crate::generator::EditJob {
            request_id: "edit-request-1".to_string(),
            model: "gpt-image-2".to_string(),
            prompt: "replace the sky".to_string(),
            moderation: "auto".to_string(),
            images: Vec::new(),
            mask: None,
            n: 1,
            size: "1024x1024".to_string(),
            quality: "high".to_string(),
            output_format: "png".to_string(),
            output_compression: None,
            background: "auto".to_string(),
            stream: false,
            partial_images: 0,
        }
    }

    fn edit_inputs() -> Vec<EditInputDescriptorV1> {
        vec![
            EditInputDescriptorV1 {
                byte_size: 123,
                index: 0,
                media_type: "image/png".to_string(),
                role: EditInputRoleV1::Image,
                sha256_hex: "1".repeat(64),
            },
            EditInputDescriptorV1 {
                byte_size: 45,
                index: 0,
                media_type: "image/png".to_string(),
                role: EditInputRoleV1::Mask,
                sha256_hex: "2".repeat(64),
            },
        ]
    }

    #[test]
    fn edit_command_has_stable_canonical_request_hash() {
        let command = EditCommandV1::from_edit_job(
            &edit_job(),
            edit_inputs(),
            "openai-images-v1",
            "openai-codex",
        );
        let reparsed: EditCommandV1 =
            serde_json::from_slice(&command.canonical_json_bytes()).unwrap();

        assert_eq!(reparsed, command);
        assert_eq!(command.operation, EDIT_OPERATION);
        assert_eq!(command.schema_version, EDIT_COMMAND_SCHEMA_VERSION);
        assert_eq!(command.request_hash_hex().len(), 64);
        assert_eq!(command.request_hash_hex(), reparsed.request_hash_hex());
        assert_eq!(
            command.input_manifest_hash_hex(),
            reparsed.input_manifest_hash_hex()
        );
    }

    #[test]
    fn edit_request_hash_covers_input_order_role_hash_and_size() {
        let base = EditCommandV1::from_edit_job(
            &edit_job(),
            edit_inputs(),
            "openai-images-v1",
            "openai-codex",
        );
        let base_hash = base.request_hash_hex();
        let mut variants = Vec::new();
        let mut reordered = base.clone();
        reordered.inputs.swap(0, 1);
        variants.push(reordered);
        let mut changed_role = base.clone();
        changed_role.inputs[0].role = EditInputRoleV1::Mask;
        variants.push(changed_role);
        let mut changed_hash = base.clone();
        changed_hash.inputs[0].sha256_hex = "3".repeat(64);
        variants.push(changed_hash);
        let mut changed_size = base.clone();
        changed_size.inputs[0].byte_size += 1;
        variants.push(changed_size);

        for variant in variants {
            assert_ne!(variant.request_hash_hex(), base_hash);
        }
    }
}
