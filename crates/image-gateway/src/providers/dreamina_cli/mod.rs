use std::{collections::BTreeMap, fmt};

use image_cli_runtime::CommandSpec;
use image_provider_dreamina_cli::{
    ADAPTER_REVISION, DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaCliPolicyV1, DreaminaSubmitPayloadV1,
    DreaminaSubmitRequestV1, PROVIDER_ID, ReceiptError, parse_receipt, parse_submit_command,
};
use image_provider_sdk::{
    EffectCertainty, PendingOperation, ProviderFailure, ProviderFailureClass, RemoteOperationRef,
    RetryDirective, SingleOutputCommand,
};
use uuid::Uuid;

use crate::provider_tasks::{
    GatedCliCommand, GatedCliSubmitCodec, ProviderExecutionContext, ProviderSubmitIntent,
};

mod poll;

pub use poll::{
    DreaminaCliPollDriverConfigError, DreaminaCliPollDriverV1, DreaminaCliPollProcessConfig,
};

const DEFAULT_POLL_AFTER_MS: u64 = 1_000;

#[derive(Clone, Eq, PartialEq)]
pub struct DreaminaCliRuntimeBindingV1 {
    execution_profile_id: Uuid,
    provider_account_id: Uuid,
    credential_auth_sha256: String,
}

impl fmt::Debug for DreaminaCliRuntimeBindingV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DreaminaCliRuntimeBindingV1")
            .field("execution_profile_id", &self.execution_profile_id)
            .field("provider_account_id", &self.provider_account_id)
            .field("credential_auth_sha256", &"[redacted]")
            .finish()
    }
}

impl DreaminaCliRuntimeBindingV1 {
    pub fn new(
        execution_profile_id: Uuid,
        provider_account_id: Uuid,
        credential_auth_sha256: impl Into<String>,
    ) -> Result<Self, DreaminaCliCodecConfigError> {
        let credential_auth_sha256 = credential_auth_sha256.into();
        if execution_profile_id.is_nil()
            || provider_account_id.is_nil()
            || !valid_sha256(&credential_auth_sha256)
        {
            return Err(DreaminaCliCodecConfigError::InvalidRuntimeBinding);
        }
        Ok(Self {
            execution_profile_id,
            provider_account_id,
            credential_auth_sha256,
        })
    }

    fn matches_submit(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
    ) -> bool {
        intent.provider_account_id == self.provider_account_id
            && context.execution_profile_id() == self.execution_profile_id
            && context.credential_auth_sha256() == self.credential_auth_sha256
    }

    fn matches_poll(&self, provider_account_id: Uuid, context: &ProviderExecutionContext) -> bool {
        provider_account_id == self.provider_account_id
            && context.execution_profile_id() == self.execution_profile_id
            && context.credential_auth_sha256() == self.credential_auth_sha256
    }
}

#[derive(Clone)]
pub struct DreaminaCliSubmitCodecV1 {
    policy: DreaminaCliPolicyV1,
    binding: DreaminaCliRuntimeBindingV1,
}

impl DreaminaCliSubmitCodecV1 {
    pub fn new(policy: DreaminaCliPolicyV1, binding: DreaminaCliRuntimeBindingV1) -> Self {
        Self { policy, binding }
    }

    fn project_request(
        &self,
        request: &DreaminaSubmitRequestV1,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        let command = self
            .policy
            .command_spec(request)
            .map_err(|_| no_effect_failure("dreamina_submit_command_invalid"))?;
        command_to_gated(&command, self.policy.executable_sha256())
    }

    fn decode_for_submission(
        submission_id: Uuid,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure> {
        let receipt = parse_receipt(stdout).map_err(receipt_failure)?;
        let operation = RemoteOperationRef::new(
            PROVIDER_ID,
            submission_id.to_string(),
            receipt.submit_id().to_owned(),
        )
        .map_err(|_| unknown_failure("dreamina_submit_receipt_invalid"))?;
        Ok(PendingOperation::new(
            operation,
            None,
            Some(DEFAULT_POLL_AFTER_MS),
        ))
    }
}

impl GatedCliSubmitCodec for DreaminaCliSubmitCodecV1 {
    type Payload = DreaminaSubmitPayloadV1;

    fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }

    fn command(
        &self,
        intent: &ProviderSubmitIntent,
        context: &ProviderExecutionContext,
        command: &SingleOutputCommand<Self::Payload>,
    ) -> Result<GatedCliCommand, ProviderFailure> {
        if intent.provider_id != PROVIDER_ID
            || !self.binding.matches_submit(intent, context)
            || context.command_schema() != DREAMINA_SUBMIT_COMMAND_SCHEMA
            || context.adapter_revision() != ADAPTER_REVISION
            || intent.output_index != command.output().index()
            || intent.output_total != command.output().total()
        {
            return Err(no_effect_failure("dreamina_submit_binding_mismatch"));
        }
        let request = parse_submit_command(command.canonical_payload())
            .map_err(|_| no_effect_failure("dreamina_submit_command_invalid"))?;
        if !request_matches_context(&request, context) {
            return Err(no_effect_failure("dreamina_submit_context_mismatch"));
        }
        self.project_request(&request)
    }

    fn decode_receipt(
        &self,
        intent: &ProviderSubmitIntent,
        _command: &SingleOutputCommand<Self::Payload>,
        stdout: &[u8],
    ) -> Result<PendingOperation, ProviderFailure> {
        Self::decode_for_submission(intent.submission_id, stdout)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DreaminaCliCodecConfigError {
    #[error("Dreamina CLI runtime binding is invalid")]
    InvalidRuntimeBinding,
}

fn command_to_gated(
    command: &CommandSpec,
    executable_sha256: [u8; 32],
) -> Result<GatedCliCommand, ProviderFailure> {
    if command.output().is_some() {
        return Err(no_effect_failure("dreamina_submit_output_contract_invalid"));
    }
    let arguments = command
        .arguments()
        .iter()
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| no_effect_failure("dreamina_submit_command_non_utf8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let environment = command
        .environment()
        .iter()
        .map(|(name, value)| {
            let name = name
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| no_effect_failure("dreamina_submit_command_non_utf8"))?;
            let value = value
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| no_effect_failure("dreamina_submit_command_non_utf8"))?;
            Ok((name, value))
        })
        .collect::<Result<BTreeMap<_, _>, ProviderFailure>>()?;
    GatedCliCommand::new(
        command.executable().path(),
        hex::encode(executable_sha256),
        command.working_directory().path(),
        arguments,
        environment,
        command.stdin_bytes().to_vec(),
        command.wall_timeout(),
        command.termination_grace(),
    )
    .map_err(|_| no_effect_failure("dreamina_submit_command_invalid"))
}

fn receipt_failure(error: ReceiptError) -> ProviderFailure {
    let code = match error {
        ReceiptError::GenerationFailed { .. } => "dreamina_submit_rejected",
        ReceiptError::InputTooLarge { .. } => "dreamina_submit_receipt_too_large",
        ReceiptError::InvalidJson(_)
        | ReceiptError::MissingStatus
        | ReceiptError::UnknownStatus(_)
        | ReceiptError::EmptySubmitId
        | ReceiptError::InvalidSubmitId => "dreamina_submit_receipt_invalid",
    };
    unknown_failure(code)
}

fn no_effect_failure(code: &str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static Dreamina provider failure must be valid")
}

fn unknown_failure(code: &str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Ambiguous,
        code,
        EffectCertainty::UnknownRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static Dreamina provider failure must be valid")
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn request_matches_context(
    request: &DreaminaSubmitRequestV1,
    context: &ProviderExecutionContext,
) -> bool {
    request_matches_platform(request, context.operation_id(), context.model())
}

fn request_matches_platform(
    request: &DreaminaSubmitRequestV1,
    operation_id: &str,
    model: &str,
) -> bool {
    match request {
        DreaminaSubmitRequestV1::TextToImage(request) => {
            operation_id == "images.generations"
                && model
                    .strip_prefix("dreamina-image-")
                    .is_some_and(|version| version == request.model().as_str())
        }
        DreaminaSubmitRequestV1::TextToVideo(request) => {
            operation_id == "videos.generations" && model == request.model().as_str()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

    use image_cli_runtime::WorkingDirectory;
    use image_provider_dreamina_cli::{
        ImageModelVersion, ImageRatio, ImageResolution, TextToImageRequestV1, TextToVideoRequestV1,
        VideoModelVersion, VideoRatio, VideoResolution,
    };
    use image_provider_sdk::EffectCertainty;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn projects_the_pinned_policy_into_a_gated_command() {
        let fixture = Fixture::new();
        let request = fixture.request();
        let projected = fixture.codec.project_request(&request).unwrap();
        let rendered = format!("{projected:?}");

        assert!(rendered.contains("text2image"));
        assert!(rendered.contains("--poll=0"));
        assert!(rendered.contains("HOME"));
        assert!(rendered.contains("TMPDIR"));
        assert!(rendered.contains(&fixture.executable_sha256));
    }

    #[test]
    fn accepted_receipt_maps_to_the_frozen_submission() {
        let submission_id = Uuid::new_v4();
        let pending = DreaminaCliSubmitCodecV1::decode_for_submission(
            submission_id,
            br#"{"submit_id":"dreamina-task-1","gen_status":"querying"}"#,
        )
        .unwrap();

        assert_eq!(pending.operation().provider_id(), PROVIDER_ID);
        assert_eq!(
            pending.operation().submission_id(),
            submission_id.to_string()
        );
        assert_eq!(pending.operation().operation_id(), "dreamina-task-1");
        assert_eq!(pending.next_poll_after_ms(), Some(DEFAULT_POLL_AFTER_MS));
    }

    #[test]
    fn malformed_or_failed_receipts_remain_unknown_effect() {
        for receipt in [
            br#"{"gen_status":"success"}"#.as_slice(),
            br#"{"gen_status":"fail","fail_reason":"denied"}"#.as_slice(),
        ] {
            let failure = DreaminaCliSubmitCodecV1::decode_for_submission(Uuid::new_v4(), receipt)
                .unwrap_err();
            assert_eq!(failure.effect(), EffectCertainty::UnknownRemoteEffect);
            assert_eq!(failure.retry(), RetryDirective::Never);
        }
    }

    #[test]
    fn runtime_binding_rejects_nil_or_malformed_identity() {
        assert_eq!(
            DreaminaCliRuntimeBindingV1::new(Uuid::nil(), Uuid::new_v4(), "a".repeat(64)),
            Err(DreaminaCliCodecConfigError::InvalidRuntimeBinding)
        );
        assert_eq!(
            DreaminaCliRuntimeBindingV1::new(Uuid::new_v4(), Uuid::new_v4(), "not-a-digest"),
            Err(DreaminaCliCodecConfigError::InvalidRuntimeBinding)
        );
    }

    #[test]
    fn runtime_binding_debug_redacts_credential_authentication_digest() {
        let digest = "a".repeat(64);
        let binding =
            DreaminaCliRuntimeBindingV1::new(Uuid::new_v4(), Uuid::new_v4(), digest.clone())
                .unwrap();
        let rendered = format!("{binding:?}");

        assert!(rendered.contains("[redacted]"));
        assert!(!rendered.contains(&digest));
    }

    #[test]
    fn platform_operation_and_model_mapping_is_strict() {
        let image: DreaminaSubmitRequestV1 = TextToImageRequestV1::new(
            "image",
            ImageModelVersion::V5_0,
            ImageRatio::R1x1,
            ImageResolution::K2,
            1,
        )
        .unwrap()
        .into();
        assert!(request_matches_platform(
            &image,
            "images.generations",
            "dreamina-image-5.0"
        ));
        assert!(!request_matches_platform(
            &image,
            "videos.generations",
            "dreamina-image-5.0"
        ));
        assert!(!request_matches_platform(
            &image,
            "images.generations",
            "dreamina-image-4.7"
        ));

        let video: DreaminaSubmitRequestV1 = TextToVideoRequestV1::new(
            "video",
            VideoModelVersion::Seedance2_0,
            VideoRatio::R16x9,
            4,
            VideoResolution::P720,
        )
        .unwrap()
        .into();
        assert!(request_matches_platform(
            &video,
            "videos.generations",
            "seedance2.0"
        ));
        assert!(!request_matches_platform(
            &video,
            "images.generations",
            "seedance2.0"
        ));
    }

    struct Fixture {
        _root: TempDir,
        codec: DreaminaCliSubmitCodecV1,
        executable_sha256: String,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().unwrap();
            let workspace = root.path().join("workspace");
            let account_home = root.path().join("account-home");
            fs::create_dir(&workspace).unwrap();
            fs::create_dir(&account_home).unwrap();
            let executable = root.path().join("dreamina");
            let bytes = b"#!/bin/sh\nprintf '{}'\n";
            fs::write(&executable, bytes).unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
            let digest: [u8; 32] = Sha256::digest(bytes).into();
            let policy = DreaminaCliPolicyV1::new(
                &executable,
                digest,
                WorkingDirectory::new(&workspace).unwrap(),
                WorkingDirectory::new(&account_home).unwrap(),
                Duration::from_secs(30),
                Duration::from_millis(100),
            )
            .unwrap();
            let binding =
                DreaminaCliRuntimeBindingV1::new(Uuid::new_v4(), Uuid::new_v4(), "a".repeat(64))
                    .unwrap();
            Self {
                _root: root,
                codec: DreaminaCliSubmitCodecV1::new(policy, binding),
                executable_sha256: hex::encode(digest),
            }
        }

        fn request(&self) -> DreaminaSubmitRequestV1 {
            TextToImageRequestV1::new(
                "a red fox",
                ImageModelVersion::V5_0,
                ImageRatio::R1x1,
                ImageResolution::K2,
                1,
            )
            .unwrap()
            .into()
        }
    }
}
