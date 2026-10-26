use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use super::{DreaminaCliRuntimeBindingV1, dreamina_operation};
use crate::{
    artifacts::MAX_ARTIFACT_BYTES,
    provider_tasks::{
        ProviderAccountHomeCapability, ProviderAccountHomeCapabilityError,
        ProviderAdditionalImageArtifactSink, ProviderNativeBatchArtifactSink,
        ProviderNativeBatchPollObservation, ProviderPollDriver, ProviderPollDriverCall,
        ProviderRuntimeProfile,
    },
};
use image_cli_runtime::{
    AsyncOutputSealError, AsyncOutputSink, AttemptWorkspaceError, CliRuntime,
    ExclusiveAttemptWorkspace, FreshOutputDirectory, NoopSpawnObserver, OutputError, ProcessError,
    RuntimeError, VerifiedFreshOutputFile, WorkingDirectory,
};
use image_provider_dreamina_cli::{
    ADAPTER_REVISION, DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaCliQueryPolicyError,
    DreaminaCliQueryPolicyV1, DreaminaQueryReceiptV1, DreaminaQueryStatusV1, PROVIDER_ID,
    QueryResultRequestV1,
};
use image_provider_sdk::{
    ArtifactMetadata, ArtifactSink, ArtifactSinkError, ArtifactSinkErrorKind, Completed,
    CompletedRemoteVideo, EffectCertainty, PollObservation, ProviderFailure, ProviderFailureClass,
    ProviderTaskProgress, RemoteOperationRef, RetryDirective,
};

const DEFAULT_POLL_AFTER_MS: u64 = 5_000;
const QUEUED_POLL_AFTER_MS: u64 = 30_000;
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
        profile: &ProviderRuntimeProfile,
        account_home: &ProviderAccountHomeCapability,
        process: DreaminaCliPollProcessConfig,
    ) -> Result<Self, DreaminaCliPollDriverConfigError> {
        let operation = dreamina_operation(profile.operation_id())
            .ok_or(DreaminaCliPollDriverConfigError::ProfileMismatch)?;
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
        remote_video: bool,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        self.poll_image_results(operation, remote_video, &mut SingleImageSink(sink))
            .await
    }

    async fn poll_image_results<S: ProviderAdditionalImageArtifactSink>(
        &self,
        operation: &RemoteOperationRef,
        remote_video: bool,
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
        let output = if remote_video {
            None
        } else {
            Some(
                FreshOutputDirectory::new(&working_directory, self.max_artifact_bytes)
                    .map_err(map_fresh_output_error)?,
            )
        };
        let request = if remote_video {
            QueryResultRequestV1::query_only(operation.operation_id(), &download_dir)
        } else {
            QueryResultRequestV1::new(operation.operation_id(), &download_dir)
        }
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

        if remote_video {
            return match receipt.status() {
                DreaminaQueryStatusV1::Querying => Ok(pending_observation(&receipt)),
                DreaminaQueryStatusV1::Failed => Ok(PollObservation::Failed(terminal_failure(
                    "dreamina_generation_failed",
                ))),
                DreaminaQueryStatusV1::Success => {
                    let result = receipt
                        .remote_video_result()
                        .cloned()
                        .ok_or_else(|| contract_failure("dreamina_poll_video_url_invalid"))?;
                    Ok(PollObservation::CompletedRemoteVideo(
                        CompletedRemoteVideo::new(result, None),
                    ))
                }
            };
        }
        let output = output.expect("image polling creates a fresh output directory");

        match receipt.status() {
            DreaminaQueryStatusV1::Querying => {
                output
                    .ensure_empty()
                    .map_err(|_| contract_failure("dreamina_poll_pending_artifact"))?;
                Ok(pending_observation(&receipt))
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
                let files = output.verify_files(10).map_err(map_fresh_output_error)?;
                let files = ordered_image_outputs(files, &receipt)?;
                if files.len() > sink.image_capacity() {
                    return Err(contract_failure("dreamina_additional_image_sink_required"));
                }
                let multiple_outputs = files.len() > 1;
                let mut primary = None;
                for (source_index, file) in files.into_iter().enumerate() {
                    sink.select_image(source_index).map_err(map_sink_error)?;
                    let bridge = ArtifactSinkBridge::new(sink);
                    let (sealed, bridge) = file
                        .seal_to_async_sink(bridge)
                        .await
                        .map_err(map_output_seal_error)?;
                    let (prefix, prefix_len) = bridge.into_prefix();
                    let media_type = artifact_media_type(&prefix[..prefix_len])
                        .filter(|media_type| !multiple_outputs || media_type.starts_with("image/"))
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
                    if source_index == 0 {
                        primary = Some(manifest);
                    }
                }
                let primary =
                    primary.ok_or_else(|| contract_failure("dreamina_poll_artifact_invalid"))?;
                Ok(PollObservation::Completed(Completed::new(primary, None)))
            }
        }
    }

    async fn poll_native_batch_operation<S: ProviderNativeBatchArtifactSink>(
        &self,
        operation: &RemoteOperationRef,
        output_count: usize,
        sink: &mut S,
    ) -> Result<ProviderNativeBatchPollObservation, ProviderFailure> {
        if !(2..=10).contains(&output_count) {
            return Err(contract_failure("dreamina_native_batch_count_invalid"));
        }
        let attempt = self
            .workspace
            .create_attempt()
            .map_err(map_attempt_workspace_error)?;
        let working_directory = attempt
            .working_directory()
            .map_err(map_attempt_workspace_error)?;
        let output = FreshOutputDirectory::new(&working_directory, self.max_artifact_bytes)
            .map_err(map_fresh_output_error)?;
        let request = QueryResultRequestV1::new(operation.operation_id(), attempt.path())
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
                let (next_poll_after_ms, progress) = pending_progress(&receipt);
                Ok(ProviderNativeBatchPollObservation::PendingWithProgress {
                    next_poll_after_ms: Some(next_poll_after_ms),
                    progress,
                })
            }
            DreaminaQueryStatusV1::Failed => {
                output
                    .ensure_empty()
                    .map_err(|_| contract_failure("dreamina_poll_failed_artifact"))?;
                Ok(ProviderNativeBatchPollObservation::Failed(
                    terminal_failure("dreamina_generation_failed"),
                ))
            }
            DreaminaQueryStatusV1::Success => {
                // Verify and open the entire set before any output is staged.
                let files = match output.verify_files(output_count) {
                    Ok(files) => files,
                    Err(OutputError::Missing) => {
                        return Ok(ProviderNativeBatchPollObservation::Failed(
                            terminal_failure("dreamina_native_batch_result_empty"),
                        ));
                    }
                    Err(error) => return Err(map_fresh_output_error(error)),
                };
                let actual_count = files.len();
                for (index, file) in files.into_iter().enumerate() {
                    sink.select_output(index).map_err(map_sink_error)?;
                    let bridge = ArtifactSinkBridge::new(sink);
                    let (sealed, bridge) = file
                        .seal_to_async_sink(bridge)
                        .await
                        .map_err(map_output_seal_error)?;
                    let (prefix, prefix_len) = bridge.into_prefix();
                    let media_type = artifact_media_type(&prefix[..prefix_len])
                        .filter(|value| value.starts_with("image/"))
                        .ok_or_else(|| contract_failure("dreamina_poll_media_invalid"))?;
                    let manifest = sink
                        .finalize(ArtifactMetadata { media_type })
                        .await
                        .map_err(map_sink_error)?;
                    let mut digest = [0_u8; 32];
                    if hex::decode_to_slice(&sealed.sha256_hex, &mut digest).is_err()
                        || manifest.byte_size() != sealed.byte_size
                        || manifest.media_type() != media_type
                        || manifest.sha256() != &digest
                    {
                        return Err(contract_failure("dreamina_poll_manifest_mismatch"));
                    }
                }
                Ok(ProviderNativeBatchPollObservation::Completed {
                    output_count: actual_count,
                })
            }
        }
    }

    fn validate_poll_call(&self, call: &ProviderPollDriverCall) -> Result<(), ProviderFailure> {
        let context = call.execution_context();
        if call.operation().provider_id() != PROVIDER_ID
            || !self
                .binding
                .matches_poll(call.provider_account_id(), context)
            || context.command_schema() != DREAMINA_SUBMIT_COMMAND_SCHEMA
            || context.adapter_revision() != ADAPTER_REVISION
            || !matches!(
                context.operation_id(),
                "images.generations" | "videos.generations"
            )
            || context.completion_mode() != "remote_task"
            || !supported_model(context.operation_id(), context.model())
        {
            return Err(contract_failure("dreamina_poll_binding_mismatch"));
        }
        Ok(())
    }
}

impl ProviderPollDriver for DreaminaCliPollDriverV1 {
    fn supports_additional_images(&self) -> bool {
        true
    }

    fn provider_id(&self) -> &'static str {
        PROVIDER_ID
    }

    async fn poll<S: ArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        self.validate_poll_call(call)?;
        self.poll_operation(
            call.operation(),
            call.execution_context().operation_id() == "videos.generations",
            sink,
        )
        .await
    }

    async fn poll_with_additional_images<S: ProviderAdditionalImageArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        sink: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        self.validate_poll_call(call)?;
        if call.execution_context().operation_id() != "images.generations" {
            return Err(contract_failure(
                "dreamina_additional_image_operation_invalid",
            ));
        }
        self.poll_image_results(call.operation(), false, sink).await
    }

    async fn poll_native_batch<S: ProviderNativeBatchArtifactSink>(
        &self,
        call: &ProviderPollDriverCall,
        output_count: usize,
        sink: &mut S,
    ) -> Result<ProviderNativeBatchPollObservation, ProviderFailure> {
        self.validate_poll_call(call)?;
        if call.execution_context().operation_id() != "images.generations"
            || call.execution_context().model() == "dreamina-image-image_upscale"
        {
            return Err(contract_failure("dreamina_native_batch_operation_invalid"));
        }
        self.poll_native_batch_operation(call.operation(), output_count, sink)
            .await
    }
}

fn ordered_image_outputs(
    files: Vec<VerifiedFreshOutputFile>,
    receipt: &DreaminaQueryReceiptV1,
) -> Result<Vec<VerifiedFreshOutputFile>, ProviderFailure> {
    if files.len() == 1 {
        if receipt.has_result_metadata() && receipt.image_result_count() != Some(1) {
            return Err(contract_failure("dreamina_poll_artifact_invalid"));
        }
        return Ok(files);
    }
    let count = files.len();
    if receipt.image_result_count() != Some(count) {
        return Err(contract_failure("dreamina_poll_artifact_invalid"));
    }
    let prefix = format!("{}_image_", receipt.submit_id());
    let mut seen = [false; 10];
    let mut selected = (0..count).map(|_| None).collect::<Vec<_>>();
    for file in files {
        let (index, extension) = file
            .relative_filename()
            .to_str()
            .and_then(|name| name.strip_prefix(&prefix))
            .and_then(|name| name.rsplit_once('.'))
            .ok_or_else(|| contract_failure("dreamina_poll_artifact_invalid"))?;
        let source_index = index
            .parse::<usize>()
            .ok()
            .filter(|value| (1..=count).contains(value) && value.to_string() == index)
            .ok_or_else(|| contract_failure("dreamina_poll_artifact_invalid"))?;
        if seen[source_index - 1] || !matches!(extension, "png" | "jpg" | "jpeg" | "webp") {
            return Err(contract_failure("dreamina_poll_artifact_invalid"));
        }
        seen[source_index - 1] = true;
        selected[source_index - 1] = Some(file);
    }
    selected
        .into_iter()
        .map(|file| file.ok_or_else(|| contract_failure("dreamina_poll_artifact_invalid")))
        .collect()
}

/// A legacy caller cannot silently throw away additional physical results.
struct SingleImageSink<'a, S>(&'a mut S);

impl<S: ArtifactSink> ProviderAdditionalImageArtifactSink for SingleImageSink<'_, S> {
    fn select_image(&mut self, source_index: usize) -> Result<(), ArtifactSinkError> {
        if source_index == 0 {
            Ok(())
        } else {
            Err(ArtifactSinkError::new(
                ArtifactSinkErrorKind::InvalidArtifact,
                "dreamina_additional_image_sink_required",
            ))
        }
    }
    fn image_capacity(&self) -> usize {
        1
    }
}

impl<S: ArtifactSink> ArtifactSink for SingleImageSink<'_, S> {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactSinkError> {
        self.0.write_chunk(chunk).await
    }
    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<image_provider_sdk::DurableArtifactManifest, ArtifactSinkError> {
        self.0.finalize(metadata).await
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

fn artifact_media_type(prefix: &[u8]) -> Option<&'static str> {
    if prefix.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if prefix.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if prefix.len() >= 12 && &prefix[..4] == b"RIFF" && &prefix[8..12] == b"WEBP" {
        Some("image/webp")
    } else if prefix.len() >= 12 && &prefix[4..8] == b"ftyp" {
        Some("video/mp4")
    } else {
        None
    }
}

fn supported_model(operation_id: &str, model: &str) -> bool {
    match operation_id {
        "images.generations" => matches!(
            model,
            "dreamina-image-3.0"
                | "dreamina-image-3.1"
                | "dreamina-image-4.0"
                | "dreamina-image-4.1"
                | "dreamina-image-4.5"
                | "dreamina-image-4.6"
                | "dreamina-image-4.7"
                | "dreamina-image-5.0"
                | "dreamina-image-5.0Pro"
                | "dreamina-image-image_upscale"
        ),
        "videos.generations" => matches!(
            model,
            "seedance1.0fast"
                | "seedance1.5pro"
                | "seedance2.0"
                | "seedance2.0fast"
                | "seedance2.0_vip"
                | "seedance2.0fast_vip"
                | "seedance2.0mini"
                | "seedance2.5"
                | "multiframe2video"
        ),
        _ => false,
    }
}

fn map_fresh_output_error(error: OutputError) -> ProviderFailure {
    match error {
        OutputError::Unavailable(_) => retryable_failure("dreamina_poll_workspace_unavailable"),
        OutputError::UnsafeDirectory
        | OutputError::InvalidLimit
        | OutputError::InvalidFileLimit
        | OutputError::DirectoryNotEmpty
        | OutputError::Missing
        | OutputError::MultipleEntries
        | OutputError::TooManyEntries
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
        RuntimeError::Receipt(_) | RuntimeError::CapturedOutputTooLarge { .. } => {
            retryable_failure("dreamina_poll_receipt_unavailable")
                .with_retry_after_ms(QUEUED_POLL_AFTER_MS)
        }
        RuntimeError::Policy(_)
        | RuntimeError::MissingOutputContract
        | RuntimeError::UnexpectedOutputContract
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
    .with_retry_after_ms(DEFAULT_POLL_AFTER_MS)
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

fn pending_observation(receipt: &DreaminaQueryReceiptV1) -> PollObservation {
    let (next_poll_after_ms, progress) = pending_progress(receipt);
    PollObservation::PendingWithProgress {
        next_poll_after_ms: Some(next_poll_after_ms),
        progress,
    }
}

pub(crate) fn pending_progress(receipt: &DreaminaQueryReceiptV1) -> (u64, ProviderTaskProgress) {
    let queue = receipt.queue_info();
    let estimate = receipt
        .estimated_remaining_seconds()
        .filter(|seconds| *seconds <= 30 * 24 * 60 * 60);
    let progress = ProviderTaskProgress {
        provider_status: queue
            .map(|queue| queue.queue_status.clone())
            .unwrap_or_else(|| "querying".to_owned()),
        queue_position: queue.map(|queue| queue.queue_idx),
        queue_total: queue.map(|queue| queue.queue_length),
        estimated_remaining_seconds: estimate,
    };
    // ETA only changes the query cadence; completion still requires success.
    let next_poll_after_ms = if progress.provider_status == "Queueing"
        && !estimate.is_some_and(|seconds| seconds <= 60)
    {
        QUEUED_POLL_AFTER_MS
    } else {
        DEFAULT_POLL_AFTER_MS
    };
    (next_poll_after_ms, progress)
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
