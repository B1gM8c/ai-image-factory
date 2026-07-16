use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};

use image_cli_runtime::{
    AsyncOutputSealError, AsyncOutputSink, CliRuntime, FreshOutputDirectory, NoopSpawnObserver,
    OutputError, ProcessError, RuntimeError, WorkingDirectory,
};
use image_provider_dreamina_cli::{
    ADAPTER_REVISION, DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaCliQueryPolicyV1,
    DreaminaQueryStatusV1, PROVIDER_ID, QueryResultRequestV1,
};
use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind, Completed,
    EffectCertainty, PollObservation, ProviderFailure, ProviderFailureClass, RemoteOperationRef,
    RetryDirective,
};
use tempfile::Builder;

use super::DreaminaCliRuntimeBindingV1;
use crate::{
    artifacts::MAX_ARTIFACT_BYTES,
    provider_tasks::{ProviderPollDriver, ProviderPollDriverCall},
};

const DEFAULT_POLL_AFTER_MS: u64 = 1_000;
const MEDIA_PREFIX_BYTES: usize = 12;

#[derive(Clone)]
pub struct DreaminaCliPollDriverV1 {
    runtime: CliRuntime<DreaminaCliQueryPolicyV1>,
    binding: DreaminaCliRuntimeBindingV1,
    max_artifact_bytes: u64,
}

impl DreaminaCliPollDriverV1 {
    pub fn new(
        policy: DreaminaCliQueryPolicyV1,
        binding: DreaminaCliRuntimeBindingV1,
        max_artifact_bytes: u64,
    ) -> Result<Self, DreaminaCliPollDriverConfigError> {
        if !(1..=MAX_ARTIFACT_BYTES).contains(&max_artifact_bytes)
            || !private_workspace_root(policy.workspace_root())
        {
            return Err(DreaminaCliPollDriverConfigError);
        }
        Ok(Self {
            runtime: CliRuntime::new(policy),
            binding,
            max_artifact_bytes,
        })
    }

    async fn poll_operation<S: ArtifactSink>(
        &self,
        operation: &RemoteOperationRef,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        let workspace = Builder::new()
            .prefix(".dreamina-poll-")
            .tempdir_in(self.runtime.policy().workspace_root())
            .map_err(|_| retryable_failure("dreamina_poll_workspace_unavailable"))?;
        fs::set_permissions(workspace.path(), fs::Permissions::from_mode(0o700))
            .map_err(|_| retryable_failure("dreamina_poll_workspace_unavailable"))?;
        let download_dir = fs::canonicalize(workspace.path())
            .map_err(|_| retryable_failure("dreamina_poll_workspace_unavailable"))?;
        let working_directory = WorkingDirectory::new(&download_dir)
            .map_err(|_| retryable_failure("dreamina_poll_workspace_unavailable"))?;
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

fn private_workspace_root(path: &Path) -> bool {
    fs::metadata(path).is_ok_and(|metadata| {
        metadata.is_dir()
            && metadata.mode() & 0o7777 == 0o700
            && metadata.uid() == unsafe { libc::geteuid() }
    })
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("Dreamina CLI poll driver configuration is invalid")]
pub struct DreaminaCliPollDriverConfigError;

#[cfg(test)]
mod tests;
