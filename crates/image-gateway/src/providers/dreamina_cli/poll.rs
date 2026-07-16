use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use super::DreaminaCliRuntimeBindingV1;
use crate::{
    artifacts::MAX_ARTIFACT_BYTES,
    provider_tasks::{
        ProviderAccountHomeCapability, ProviderAccountHomeCapabilityError, ProviderPollDriver,
        ProviderPollDriverCall, ProviderPollRuntimeProfile,
    },
};
use image_cli_runtime::{
    AsyncOutputSealError, AsyncOutputSink, AttemptWorkspaceError, CliRuntime,
    ExclusiveAttemptWorkspace, FreshOutputDirectory, NoopSpawnObserver, OutputError, ProcessError,
    RuntimeError, WorkingDirectory,
};
use image_provider_dreamina_cli::{
    ADAPTER_REVISION, DREAMINA_IMAGE_GENERATION_OPERATION_V1, DREAMINA_SUBMIT_COMMAND_SCHEMA,
    DreaminaCliQueryPolicyError, DreaminaCliQueryPolicyV1, DreaminaQueryStatusV1, PROVIDER_ID,
    QueryResultRequestV1,
};
use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind, Completed,
    EffectCertainty, PollObservation, ProviderFailure, ProviderFailureClass, RemoteOperationRef,
    RetryDirective,
};

const DEFAULT_POLL_AFTER_MS: u64 = 1_000;
const MEDIA_PREFIX_BYTES: usize = 12;
const DREAMINA_POLL_ATTEMPT_PREFIX: &str = ".dreamina-poll-";

pub struct DreaminaCliPollDriverV1 {
    runtime: CliRuntime<DreaminaCliQueryPolicyV1>,
    binding: DreaminaCliRuntimeBindingV1,
    max_artifact_bytes: u64,
    workspace: ExclusiveAttemptWorkspace,
}

pub struct DreaminaCliPollProcessConfig {
    executable_path: PathBuf,
    executable_sha256: [u8; 32],
    workspace_root: WorkingDirectory,
    wall_timeout: Duration,
    termination_grace: Duration,
    max_artifact_bytes: u64,
}

impl DreaminaCliPollProcessConfig {
    pub fn new(
        executable_path: impl AsRef<Path>,
        executable_sha256: [u8; 32],
        workspace_root: WorkingDirectory,
        wall_timeout: Duration,
        termination_grace: Duration,
        max_artifact_bytes: u64,
    ) -> Self {
        Self {
            executable_path: executable_path.as_ref().to_path_buf(),
            executable_sha256,
            workspace_root,
            wall_timeout,
            termination_grace,
            max_artifact_bytes,
        }
    }
}

impl DreaminaCliPollDriverV1 {
    pub fn from_runtime_profile(
        profile: &ProviderPollRuntimeProfile,
        account_home: &ProviderAccountHomeCapability,
        process: DreaminaCliPollProcessConfig,
    ) -> Result<Self, DreaminaCliPollDriverConfigError> {
        let operation = DREAMINA_IMAGE_GENERATION_OPERATION_V1;
        if profile.provider_id() != PROVIDER_ID
            || profile.command_schema() != operation.command_schema
            || profile.operation_id() != operation.id
            || profile.operation_descriptor_revision() != operation.descriptor_revision
            || profile.operation_descriptor_sha256_v1() != operation.canonical_sha256_v1_hex()
            || profile.idempotency_mode() != operation.idempotency.as_str()
            || profile.adapter_revision() != ADAPTER_REVISION
        {
            return Err(DreaminaCliPollDriverConfigError::ProfileMismatch);
        }
        let account_home = account_home
            .bind(profile)
            .map_err(DreaminaCliPollDriverConfigError::AccountHome)?;
        let binding = DreaminaCliRuntimeBindingV1::new(
            profile.execution_profile_id(),
            profile.provider_account_id(),
            profile.credential_auth_sha256(),
        )
        .map_err(|_| DreaminaCliPollDriverConfigError::ProfileMismatch)?;
        let policy = DreaminaCliQueryPolicyV1::new(
            process.executable_path,
            process.executable_sha256,
            process.workspace_root,
            account_home,
            process.wall_timeout,
            process.termination_grace,
        )
        .map_err(DreaminaCliPollDriverConfigError::Policy)?;
        Self::new(policy, binding, process.max_artifact_bytes)
    }

    pub fn new(
        policy: DreaminaCliQueryPolicyV1,
        binding: DreaminaCliRuntimeBindingV1,
        max_artifact_bytes: u64,
    ) -> Result<Self, DreaminaCliPollDriverConfigError> {
        if !(1..=MAX_ARTIFACT_BYTES).contains(&max_artifact_bytes) {
            return Err(DreaminaCliPollDriverConfigError::InvalidArtifactLimit);
        }
        let workspace = ExclusiveAttemptWorkspace::acquire(
            policy.workspace_root_directory(),
            DREAMINA_POLL_ATTEMPT_PREFIX,
        )
        .map_err(DreaminaCliPollDriverConfigError::Workspace)?;
        Ok(Self {
            runtime: CliRuntime::new(policy),
            binding,
            max_artifact_bytes,
            workspace,
        })
    }

    async fn poll_operation<S: ArtifactSink>(
        &self,
        operation: &RemoteOperationRef,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        let attempt = self
            .workspace
            .create_attempt()
            .map_err(map_attempt_workspace_error)?;
        let download_dir = attempt.path().to_path_buf();
        let working_directory = attempt
            .working_directory()
            .map_err(map_attempt_workspace_error)?;
        let output = FreshOutputDirectory::new(&working_directory, self.max_artifact_bytes)
            .map_err(map_fresh_output_error)?;
        let request = QueryResultRequestV1::new(operation.operation_id(), &download_dir)
            .map_err(|_| contract_failure("dreamina_poll_request_invalid"))?;
        let receipt = self
            .runtime
            .run_receipt(&request, &mut NoopSpawnObserver)
            .await
            .map_err(map_runtime_error)?
            .receipt;
        if receipt.submit_id() != operation.operation_id() {
            return Err(contract_failure("dreamina_poll_receipt_mismatch"));
        }

        match receipt.status() {
            DreaminaQueryStatusV1::Querying => {
                output
                    .ensure_empty()
                    .map_err(|_| contract_failure("dreamina_poll_pending_artifact"))?;
                Ok(PollObservation::Pending {
                    next_poll_after_ms: Some(DEFAULT_POLL_AFTER_MS),
                })
            }
            DreaminaQueryStatusV1::Failed => {
                output
                    .ensure_empty()
                    .map_err(|_| contract_failure("dreamina_poll_failed_artifact"))?;
                Ok(PollObservation::Failed(terminal_failure(
                    "dreamina_generation_failed",
                )))
            }
            DreaminaQueryStatusV1::Success => {
                let bridge = ArtifactSinkBridge::new(sink);
                let (sealed, bridge) = output
                    .seal_single_file_to_async_sink(bridge)
                    .await
                    .map_err(map_output_seal_error)?;
                let (prefix, prefix_len) = bridge.into_prefix();
                let media_type = image_media_type(&prefix[..prefix_len])
                    .ok_or_else(|| contract_failure("dreamina_poll_media_invalid"))?;
                let manifest = sink
                    .finalize(ArtifactMetadata { media_type })
                    .await
                    .map_err(map_sink_error)?;
                let mut sealed_sha256 = [0_u8; 32];
                if hex::decode_to_slice(&sealed.sha256_hex, &mut sealed_sha256).is_err()
                    || manifest.byte_size() != sealed.byte_size
                    || manifest.media_type() != media_type
                    || manifest.sha256() != &sealed_sha256
                {
                    return Err(contract_failure("dreamina_poll_manifest_mismatch"));
                }
                Ok(PollObservation::Completed(Completed::new(manifest, None)))
            }
        }
    }
}

impl ProviderPollDriver for DreaminaCliPollDriverV1 {
    fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }

    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        let context = call.execution_context();
        if call.operation().provider_id() != PROVIDER_ID
            || !self
                .binding
                .matches_poll(call.provider_account_id(), context)
            || context.command_schema() != DREAMINA_SUBMIT_COMMAND_SCHEMA
            || context.adapter_revision() != ADAPTER_REVISION
            || context.operation_id() != "images.generations"
            || context.completion_mode() != "remote_task"
            || !supported_image_model(context.model())
        {
            return Err(contract_failure("dreamina_poll_binding_mismatch"));
        }
        self.poll_operation(call.operation(), sink).await
    }
}

struct ArtifactSinkBridge<'a, S> {
    sink: &'a mut S,
    prefix: [u8; MEDIA_PREFIX_BYTES],
    prefix_len: usize,
}

impl<'a, S> ArtifactSinkBridge<'a, S> {
    fn new(sink: &'a mut S) -> Self {
        Self {
            sink,
            prefix: [0_u8; MEDIA_PREFIX_BYTES],
            prefix_len: 0,
        }
    }

    fn into_prefix(self) -> ([u8; MEDIA_PREFIX_BYTES], usize) {
        (self.prefix, self.prefix_len)
    }
}

impl<S> AsyncOutputSink for ArtifactSinkBridge<'_, S>
where
    S: ArtifactSink,
{
    type Error = ArtifactSinkError;

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Self::Error> {
        let remaining = MEDIA_PREFIX_BYTES - self.prefix_len;
        let copied = remaining.min(chunk.len());
        self.prefix[self.prefix_len..self.prefix_len + copied].copy_from_slice(&chunk[..copied]);
        self.prefix_len += copied;
        self.sink.write_chunk(chunk).await
    }
}

fn map_attempt_workspace_error(error: AttemptWorkspaceError) -> ProviderFailure {
    match error {
        AttemptWorkspaceError::Unavailable => {
            retryable_failure("dreamina_poll_workspace_unavailable")
        }
        AttemptWorkspaceError::InvalidConfiguration
        | AttemptWorkspaceError::Integrity
        | AttemptWorkspaceError::AlreadyLocked => {
            contract_failure("dreamina_poll_workspace_invalid")
        }
    }
}

fn image_media_type(prefix: &[u8]) -> Option<&'static str> {
    if prefix.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if prefix.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if prefix.len() >= 12 && &prefix[..4] == b"RIFF" && &prefix[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

fn supported_image_model(model: &str) -> bool {
    matches!(
        model,
        "dreamina-image-3.0"
            | "dreamina-image-3.1"
            | "dreamina-image-4.0"
            | "dreamina-image-4.1"
            | "dreamina-image-4.5"
            | "dreamina-image-4.6"
            | "dreamina-image-4.7"
            | "dreamina-image-5.0"
    )
}

fn map_fresh_output_error(error: OutputError) -> ProviderFailure {
    match error {
        OutputError::Unavailable(_) => retryable_failure("dreamina_poll_workspace_unavailable"),
        OutputError::UnsafeDirectory
        | OutputError::InvalidLimit
        | OutputError::DirectoryNotEmpty
        | OutputError::Missing
        | OutputError::MultipleEntries
        | OutputError::NotRegular
        | OutputError::Empty
        | OutputError::TooLarge
        | OutputError::ChangedDuringRead
        | OutputError::Sink(_) => contract_failure("dreamina_poll_artifact_invalid"),
    }
}

fn map_runtime_error(error: RuntimeError) -> ProviderFailure {
    match error {
        RuntimeError::Process(ProcessError::InvalidCommand(_)) => {
            contract_failure("dreamina_poll_process_invalid")
        }
        RuntimeError::Process(
            ProcessError::Spawn(_)
            | ProcessError::IdentityUnavailable
            | ProcessError::Observer { .. }
            | ProcessError::Stdin { .. }
            | ProcessError::TimedOut { .. }
            | ProcessError::Wait { .. }
            | ProcessError::Capture { .. }
            | ProcessError::ResidualProcessGroup { .. },
        )
        | RuntimeError::ProcessFailed { .. } => {
            retryable_failure("dreamina_poll_process_unavailable")
        }
        RuntimeError::Policy(_)
        | RuntimeError::MissingOutputContract
        | RuntimeError::UnexpectedOutputContract
        | RuntimeError::CapturedOutputTooLarge { .. }
        | RuntimeError::Receipt(_)
        | RuntimeError::Output(_)
        | RuntimeError::OutputTask(_) => contract_failure("dreamina_poll_protocol_invalid"),
    }
}

fn map_output_seal_error(error: AsyncOutputSealError<ArtifactSinkError>) -> ProviderFailure {
    match error {
        AsyncOutputSealError::Output(error) => map_fresh_output_error(error),
        AsyncOutputSealError::Sink(error) => map_sink_error(error),
    }
}

fn map_sink_error(error: ArtifactSinkError) -> ProviderFailure {
    match error.kind() {
        ArtifactSinkErrorKind::Storage => {
            retryable_failure("dreamina_poll_artifact_storage_unavailable")
        }
        ArtifactSinkErrorKind::AlreadyFinalized | ArtifactSinkErrorKind::InvalidArtifact => {
            contract_failure("dreamina_poll_artifact_invalid")
        }
    }
}

fn retryable_failure(code: &'static str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Transient,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Backoff,
    )
    .expect("static Dreamina poll failure must be valid")
}

fn contract_failure(code: &'static str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static Dreamina poll failure must be valid")
}

fn terminal_failure(code: &'static str) -> ProviderFailure {
    ProviderFailure::new(
        ProviderFailureClass::Permanent,
        code,
        EffectCertainty::NoRemoteEffect,
        RetryDirective::Never,
    )
    .expect("static Dreamina terminal failure must be valid")
}

#[derive(Debug, thiserror::Error)]
pub enum DreaminaCliPollDriverConfigError {
    #[error("Dreamina CLI poll runtime profile is incompatible")]
    ProfileMismatch,
    #[error("Dreamina CLI poll account-home capability is incompatible")]
    AccountHome(#[source] ProviderAccountHomeCapabilityError),
    #[error("Dreamina CLI query policy configuration is invalid")]
    Policy(#[source] DreaminaCliQueryPolicyError),
    #[error("Dreamina CLI poll artifact limit is invalid")]
    InvalidArtifactLimit,
    #[error("Dreamina CLI poll workspace configuration is invalid")]
    Workspace(#[source] AttemptWorkspaceError),
}

#[cfg(test)]
mod tests;
