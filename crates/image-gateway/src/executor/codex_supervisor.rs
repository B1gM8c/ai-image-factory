use std::{
    env, fs,
    io::{self, BufRead, BufReader, Cursor, Read},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use image_cli_runtime::{
    CliPolicy, CliRuntime, CommandSpec, CommandSpecError, ExitClassification, OutputContract,
    OutputError, ProcessCompletion, ProcessError, RuntimeError, SpawnEvidence, SpawnObserver,
    VerifiedExecutable, WorkingDirectory,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{process::Command, sync::oneshot, time::Instant};
use uuid::Uuid;

use super::{
    CodexOutputRequest, ExecutorLaunchContext, ExecutorSubmissionLease, RunnerError,
    SingleOutputSupervisor, SupervisedOutput,
    private_auth::{auth_file_sha256, prepare_isolated_auth, validate_auth_source},
    project_codex_output_request,
    runner::RunnerLaunchBinding,
};
use crate::{
    ImageGatewayError, ProxyConfig,
    generator::GenerationJob,
    providers::openai_codex::{
        build_codex_prompt_for_output, provider_output_filename, read_codex_output,
        select_image_output,
    },
    runner::{
        FilesystemRunnerJournal, LaunchDecision,
        process::{
            ExecutionSpool, ProcessObservation, ProcessSpoolError, ProcessTerminal,
            ProviderProcessIdentity, RunnerLock, WorkspaceOutputSnapshot, sha256,
        },
    },
};

pub const CODEX_GENERATION_ADAPTER_REVISION: &str = "openai-codex-generation-v1";
const MAX_EXECUTABLE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_SHEBANG_BYTES: usize = 4096;
const MAX_RUNNER_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const CHILD_REAP_TIMEOUT: Duration = Duration::from_secs(5);
const CODEX_CHILD_PATH: &str = "/usr/bin:/bin";
const MAX_CODEX_RUNTIME_OUTPUT_BYTES: u64 = 32 * 1024 * 1024;
const CODEX_RUNTIME_OUTPUT_FILE: &str = "sealed-output.bin";
const EPHEMERAL_OUTPUT_POLL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_DECODED_IMAGE_PIXELS: u64 = 16 * 1024 * 1024;
const MAX_DECODED_IMAGE_DIMENSION: u32 = 8 * 1024;
const MAX_CODEX_SESSION_FILES: usize = 32;
const MAX_CODEX_SESSION_DEPTH: usize = 8;
const MAX_CODEX_SESSION_LINE_BYTES: usize =
    ((MAX_CODEX_RUNTIME_OUTPUT_BYTES as usize + 2) / 3) * 4 + 64 * 1024;
const MAX_CODEX_SESSION_FILE_BYTES: u64 = (MAX_CODEX_SESSION_LINE_BYTES as u64) * 4;

pub struct CodexProcessSupervisor {
    journal: Arc<FilesystemRunnerJournal>,
    helper_executable: PathBuf,
    codex_executable: PathBuf,
    codex_executable_sha256: String,
    credential_auth_file: PathBuf,
    credential_auth_sha256: String,
    credential_resolver: Option<(Uuid, Arc<dyn crate::OperationalCredentialResolver>)>,
    request_timeout: Duration,
    poll_interval: Duration,
    startup_grace: Duration,
    child_env: Vec<(String, String)>,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CodexChildRequest {
    schema_version: u16,
    adapter_revision: String,
    executor_execution_id: String,
    launch: RunnerLaunchBinding,
    codex_executable: String,
    codex_executable_sha256: String,
    timeout_ms: u64,
    output: CodexOutputRequest,
}

enum ChildOutcome {
    Succeeded(Vec<u8>),
    Failed(&'static str),
    Uncertain(&'static str),
}

struct CodexCliPolicy;

struct CodexCliInvocation {
    executable: PathBuf,
    workspace: PathBuf,
    output_dir: PathBuf,
    codex_home: PathBuf,
    timeout: Duration,
    prompt: Vec<u8>,
    output_filename: &'static str,
    environment: Vec<(String, String)>,
}

struct CodexSpawnObserver {
    spool: Arc<ExecutionSpool>,
    runner_lock: Arc<RunnerLock>,
    helper: crate::runner::process::ProcessIdentity,
    events: CodexEventSummary,
}

#[derive(Default)]
struct CodexEventSummary {
    thread_id: Option<Uuid>,
    saw_image_generation: bool,
    completed_image_generation: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    stderr_present: bool,
    malformed_events: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodexOutputSource {
    Workspace,
    Runtime,
    Native,
    Session,
}

struct CapturedCodexOutput {
    bytes: Vec<u8>,
    source: CodexOutputSource,
}

#[derive(Default)]
struct CodexOutputCapture {
    output: Option<CapturedCodexOutput>,
    workspace_observed: bool,
    runtime_observed: bool,
    native_observed: bool,
}

impl CodexProcessSupervisor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        journal: Arc<FilesystemRunnerJournal>,
        helper_executable: impl AsRef<Path>,
        codex_executable: impl AsRef<Path>,
        credential_home: impl AsRef<Path>,
        credential_auth_sha256: &str,
        request_timeout: Duration,
        poll_interval: Duration,
        startup_grace: Duration,
        proxy: &ProxyConfig,
    ) -> Result<Self, ImageGatewayError> {
        if request_timeout.is_zero()
            || request_timeout > MAX_RUNNER_TIMEOUT
            || poll_interval.is_zero()
            || startup_grace.is_zero()
        {
            return Err(ImageGatewayError::config(
                "Codex executor timeout configuration is invalid",
            ));
        }
        let helper_executable = canonical_executable(helper_executable.as_ref())?;
        let codex_executable = canonical_executable(codex_executable.as_ref())?;
        validate_codex_executable_compatibility(&codex_executable, CODEX_CHILD_PATH)?;
        let codex_executable_sha256 = hash_bounded_file(&codex_executable)?;
        let credential_auth_file = validate_auth_source(
            credential_home.as_ref(),
            credential_auth_sha256,
        )
        .map_err(|_| {
            ImageGatewayError::config(
                "EXECUTOR_CODEX_CREDENTIAL_HOME/auth.json is invalid or does not match the database credential",
            )
        })?;
        Ok(Self {
            journal,
            helper_executable,
            codex_executable,
            codex_executable_sha256,
            credential_auth_file,
            credential_auth_sha256: credential_auth_sha256.to_string(),
            credential_resolver: None,
            request_timeout,
            poll_interval,
            startup_grace,
            child_env: child_environment(proxy),
        })
    }

    pub fn with_credential_resolver(
        mut self,
        provider_account_id: Uuid,
        resolver: Arc<dyn crate::OperationalCredentialResolver>,
    ) -> Result<Self, ImageGatewayError> {
        if provider_account_id.is_nil() {
            return Err(ImageGatewayError::config(
                "Codex credential resolver account is invalid",
            ));
        }
        self.credential_resolver = Some((provider_account_id, resolver));
        Ok(self)
    }

    async fn credential_source(&self) -> Result<(PathBuf, String, i64), RunnerError> {
        let Some((provider_account_id, resolver)) = &self.credential_resolver else {
            return Ok((
                self.credential_auth_file.clone(),
                self.credential_auth_sha256.clone(),
                1,
            ));
        };
        let credential = resolver
            .resolve(*provider_account_id)
            .await
            .map_err(|_| RunnerError::Unavailable)?;
        if credential.provider_id != image_provider_contracts::openai_codex::PROVIDER_ID
            || credential.provider_account_id != *provider_account_id
            || self.credential_auth_file.parent() != Some(credential.home())
        {
            return Err(RunnerError::Unavailable);
        }
        let source =
            validate_auth_source(credential.home(), &credential.material_fingerprint_sha256)
                .map_err(|_| RunnerError::Unavailable)?;
        Ok((
            source,
            credential.material_fingerprint_sha256,
            credential.revision,
        ))
    }

    fn child_request(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<CodexChildRequest, RunnerError> {
        if lease.adapter_revision != CODEX_GENERATION_ADAPTER_REVISION {
            return Err(RunnerError::Definite {
                error_code: "executor_adapter_revision_mismatch".to_string(),
            });
        }
        let output =
            project_codex_output_request(lease, context).map_err(|_| RunnerError::Definite {
                error_code: "executor_command_rejected".to_string(),
            })?;
        Ok(CodexChildRequest {
            schema_version: 1,
            adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
            executor_execution_id: lease.executor_execution_id.to_string(),
            launch: RunnerLaunchBinding::from_lease(lease),
            codex_executable: self.codex_executable.to_string_lossy().into_owned(),
            codex_executable_sha256: self.codex_executable_sha256.clone(),
            timeout_ms: self.request_timeout.as_millis() as u64,
            output,
        })
    }

    fn spawn_helper(&self, lease: &ExecutorSubmissionLease) -> Result<(), RunnerError> {
        let mut command = Command::new(&self.helper_executable);
        command
            .arg(self.journal.root_path())
            .arg(lease.executor_execution_id.to_string())
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        for (name, value) in &self.child_env {
            command.env(name, value);
        }
        command.spawn().map_err(|_| RunnerError::Unknown {
            error_code: "runner_spawn_failed".to_string(),
        })?;
        Ok(())
    }

    async fn terminate_orphaned_provider(
        &self,
        provider: &ProviderProcessIdentity,
    ) -> Result<(), RunnerError> {
        if !provider
            .kill_process_group_if_current()
            .map_err(map_spool_error)?
        {
            return Ok(());
        }
        let started = Instant::now();
        loop {
            if !provider
                .is_current_process_group()
                .map_err(map_spool_error)?
            {
                return Ok(());
            }
            if started.elapsed() >= CHILD_REAP_TIMEOUT {
                return Err(RunnerError::Unknown {
                    error_code: "runner_orphan_cleanup_timeout".to_string(),
                });
            }
            tokio::time::sleep(self.poll_interval.min(Duration::from_millis(50))).await;
        }
    }
}

pub fn codex_auth_file_sha256(
    credential_home: impl AsRef<Path>,
) -> Result<String, ImageGatewayError> {
    auth_file_sha256(credential_home.as_ref()).map_err(|_| {
        ImageGatewayError::config("EXECUTOR_CODEX_CREDENTIAL_HOME/auth.json is invalid")
    })
}

pub fn prepare_codex_auth_copy(
    destination_home: impl AsRef<Path>,
    source_home: impl AsRef<Path>,
    expected_sha256: &str,
) -> Result<(), ImageGatewayError> {
    let source = validate_auth_source(source_home.as_ref(), expected_sha256)
        .map_err(|_| ImageGatewayError::config("managed Codex auth source is invalid"))?;
    prepare_isolated_auth(destination_home.as_ref(), &source, expected_sha256)
        .map_err(|_| ImageGatewayError::config("managed Codex auth copy is invalid"))
}

#[async_trait]
impl SingleOutputSupervisor for CodexProcessSupervisor {
    async fn prepare(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<(), RunnerError> {
        let (credential_auth_file, credential_auth_sha256, credential_revision) =
            self.credential_source().await?;
        let request = self.child_request(lease, context)?;
        let bytes = serde_json::to_vec(&request).map_err(|_| RunnerError::Internal)?;
        let spool = ExecutionSpool::for_lease(&self.journal, lease).map_err(map_spool_error)?;
        prepare_isolated_auth(
            spool.codex_home_path().map_err(map_spool_error)?,
            &credential_auth_file,
            &credential_auth_sha256,
        )
        .map_err(|_| RunnerError::Unavailable)?;
        tracing::debug!(
            execution.profile.id = %lease.execution_profile_id,
            credential.revision = credential_revision,
            "resolved Codex operational credential"
        );
        spool.prepare_request(&bytes).map_err(map_spool_error)
    }

    async fn start_or_attach(
        &self,
        lease: &ExecutorSubmissionLease,
        decision: LaunchDecision,
    ) -> Result<SupervisedOutput, RunnerError> {
        let spool = ExecutionSpool::for_lease(&self.journal, lease).map_err(map_spool_error)?;
        if decision == LaunchDecision::LaunchOnce {
            self.spawn_helper(lease)?;
        }
        let started = Instant::now();
        let supervision_timeout = self.request_timeout.saturating_add(self.startup_grace);
        loop {
            match spool.observe().map_err(map_spool_error)? {
                ProcessObservation::Succeeded(bytes) => return Ok(bytes),
                ProcessObservation::Failed { error_code } => {
                    return Err(RunnerError::Definite { error_code });
                }
                ProcessObservation::Uncertain { error_code } => {
                    return Err(RunnerError::Unknown { error_code });
                }
                ProcessObservation::Lost { provider } => {
                    if let Some(provider) = provider {
                        self.terminate_orphaned_provider(&provider).await?;
                    }
                    return Err(RunnerError::Unknown {
                        error_code: "runner_process_lost".to_string(),
                    });
                }
                ProcessObservation::AwaitingProcess if started.elapsed() >= self.startup_grace => {
                    return Err(RunnerError::Unknown {
                        error_code: "runner_process_missing".to_string(),
                    });
                }
                ProcessObservation::AwaitingProcess | ProcessObservation::Running(_) => {}
            }
            if started.elapsed() >= supervision_timeout {
                return Err(RunnerError::Unknown {
                    error_code: "runner_supervision_timeout".to_string(),
                });
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

pub async fn run_codex_runner_child(
    runner_root: impl AsRef<Path>,
    executor_execution_id: Uuid,
) -> Result<(), ImageGatewayError> {
    let runner_root = runner_root.as_ref();
    let spool = Arc::new(
        ExecutionSpool::open(runner_root, executor_execution_id).map_err(child_spool_error)?,
    );
    let request = spool
        .read_request()
        .map_err(child_spool_error)
        .and_then(|bytes| {
            serde_json::from_slice::<CodexChildRequest>(&bytes).map_err(|_| {
                ImageGatewayError::service_unavailable("Codex runner request is invalid")
            })
        })?;
    let lease = validate_child_request(&request, executor_execution_id)?;
    FilesystemRunnerJournal::new(runner_root)
        .and_then(|journal| journal.verify_launch_committed(&lease))
        .map_err(|_| {
            ImageGatewayError::service_unavailable("Codex launch authority is unavailable")
        })?;
    let runner_lock = Arc::new(spool.acquire_runner_lock().map_err(child_spool_error)?);
    let identity = runner_lock.identity().map_err(child_spool_error)?;
    spool
        .publish_process(&runner_lock, &identity)
        .map_err(child_spool_error)?;
    let outcome = run_codex_child(
        Arc::clone(&spool),
        Arc::clone(&runner_lock),
        identity.clone(),
        executor_execution_id,
    )
    .await;
    match outcome {
        ChildOutcome::Succeeded(bytes) => {
            spool.publish_output(&bytes).map_err(child_spool_error)?;
            if spool.cleanup_codex_runtime().is_err() {
                spool
                    .publish_terminal(
                        &runner_lock,
                        &ProcessTerminal::Uncertain {
                            helper_nonce: identity.nonce.clone(),
                            error_code: "codex_local_cleanup_failed".to_owned(),
                        },
                    )
                    .map_err(child_spool_error)?;
                drop(runner_lock);
                return Ok(());
            }
            spool
                .publish_terminal(
                    &runner_lock,
                    &ProcessTerminal::Succeeded {
                        helper_nonce: identity.nonce.clone(),
                        sha256_hex: sha256(&bytes),
                        byte_size: bytes.len() as u64,
                        provider_reported_cost: None,
                    },
                )
                .map_err(child_spool_error)?;
        }
        ChildOutcome::Failed(error_code) => {
            let cleanup_failed = spool.cleanup_codex_runtime().is_err();
            let terminal = if cleanup_failed {
                ProcessTerminal::Uncertain {
                    helper_nonce: identity.nonce.clone(),
                    error_code: "codex_local_cleanup_failed".to_owned(),
                }
            } else {
                ProcessTerminal::Failed {
                    helper_nonce: identity.nonce.clone(),
                    error_code: error_code.to_owned(),
                }
            };
            spool
                .publish_terminal(&runner_lock, &terminal)
                .map_err(child_spool_error)?
        }
        ChildOutcome::Uncertain(error_code) => {
            let cleanup_failed = spool.cleanup_codex_runtime().is_err();
            spool
                .publish_terminal(
                    &runner_lock,
                    &ProcessTerminal::Uncertain {
                        helper_nonce: identity.nonce.clone(),
                        error_code: if cleanup_failed {
                            "codex_local_cleanup_failed".to_owned()
                        } else {
                            error_code.to_owned()
                        },
                    },
                )
                .map_err(child_spool_error)?
        }
    }
    drop(runner_lock);
    Ok(())
}

async fn run_codex_child(
    spool: Arc<ExecutionSpool>,
    runner_lock: Arc<RunnerLock>,
    helper: crate::runner::process::ProcessIdentity,
    executor_execution_id: Uuid,
) -> ChildOutcome {
    let request = match spool
        .read_request()
        .map_err(child_spool_error)
        .and_then(|bytes| {
            serde_json::from_slice::<CodexChildRequest>(&bytes).map_err(|_| {
                ImageGatewayError::service_unavailable("Codex runner request is invalid")
            })
        }) {
        Ok(request) => request,
        Err(_) => return ChildOutcome::Uncertain("runner_request_invalid"),
    };
    if validate_child_request(&request, executor_execution_id).is_err() {
        return ChildOutcome::Uncertain("runner_request_invalid");
    }
    let workspace = match spool.workspace_path() {
        Ok(path) => path,
        Err(_) => return ChildOutcome::Uncertain("runner_workspace_invalid"),
    };
    let codex_home = match spool.codex_home_path() {
        Ok(path) => path,
        Err(_) => return ChildOutcome::Uncertain("runner_codex_home_invalid"),
    };
    let output_dir = match spool.runtime_home_path() {
        Ok(path) => path,
        Err(_) => return ChildOutcome::Uncertain("runner_output_directory_invalid"),
    };
    let job = generation_job(&request.output);
    let prompt = build_codex_prompt_for_output(
        &job,
        workspace,
        request.output.candidate_index,
        output_dir,
        CODEX_RUNTIME_OUTPUT_FILE,
    );
    let invocation = CodexCliInvocation {
        executable: PathBuf::from(&request.codex_executable),
        workspace: workspace.to_path_buf(),
        output_dir: output_dir.to_path_buf(),
        codex_home: codex_home.to_path_buf(),
        timeout: Duration::from_millis(request.timeout_ms),
        prompt: prompt.into_bytes(),
        output_filename: CODEX_RUNTIME_OUTPUT_FILE,
        environment: allowed_child_environment(),
    };
    let mut observer = CodexSpawnObserver {
        spool: Arc::clone(&spool),
        runner_lock: Arc::clone(&runner_lock),
        helper,
        events: CodexEventSummary::default(),
    };
    let capture_filename = provider_output_filename(&request.output.output_format);
    match spool.read_workspace_output(capture_filename, MAX_CODEX_RUNTIME_OUTPUT_BYTES) {
        Ok(WorkspaceOutputSnapshot::Missing) => {}
        Ok(WorkspaceOutputSnapshot::Incomplete | WorkspaceOutputSnapshot::Bytes(_)) => {
            return ChildOutcome::Uncertain("codex_workspace_output_preexisting");
        }
        Err(_) => return ChildOutcome::Uncertain("codex_ephemeral_output_unavailable"),
    }
    let (stop_capture, capture_stop) = oneshot::channel();
    let capture_spool = Arc::clone(&spool);
    let capture_format = request.output.output_format.clone();
    let capture_codex_home = codex_home.to_path_buf();
    let capture = tokio::spawn(async move {
        capture_ephemeral_output(
            capture_spool,
            capture_filename,
            CODEX_RUNTIME_OUTPUT_FILE,
            capture_codex_home,
            capture_format,
            capture_stop,
        )
        .await
    });
    let runtime_result = CliRuntime::new(CodexCliPolicy)
        .run_to_sink(&invocation, &mut observer, Vec::new())
        .await;
    let _ = stop_capture.send(());
    let captured = match capture.await {
        Ok(result) => result,
        Err(_) => Err(ProcessSpoolError::Unavailable),
    };
    let outcome = match runtime_result {
        Ok(result) => ChildOutcome::Succeeded(result.sink),
        Err(error) if ephemeral_capture_fallback_allowed(&error) => match captured {
            Ok(capture) if capture.output.is_some() => {
                let output = capture.output.expect("capture output checked");
                tracing::warn!(
                    request.id = %request.output.request_id,
                    output.index = request.output.candidate_index,
                    codex.output.recovery_source = ?output.source,
                    codex.output.workspace_observed = capture.workspace_observed,
                    codex.output.runtime_observed = capture.runtime_observed,
                    codex.output.native_observed = capture.native_observed,
                    "recovered Codex output before an ephemeral provider path disappeared"
                );
                ChildOutcome::Succeeded(output.bytes)
            }
            Ok(capture) => match final_fallback_output(
                &spool,
                capture_filename,
                codex_home,
                observer.events.thread_id,
                &request.output.output_format,
            )
            .await
            {
                Ok(Some(output)) => {
                    tracing::warn!(
                        request.id = %request.output.request_id,
                        output.index = request.output.candidate_index,
                        codex.output.recovery_source = ?output.source,
                        "recovered Codex output after process exit"
                    );
                    ChildOutcome::Succeeded(output.bytes)
                }
                Ok(None) => {
                    tracing::warn!(
                        request.id = %request.output.request_id,
                        output.index = request.output.candidate_index,
                        codex.thread.id = ?observer.events.thread_id,
                        codex.image_generation.seen = observer.events.saw_image_generation,
                        codex.image_generation.completed = observer.events.completed_image_generation,
                        codex.events.malformed = observer.events.malformed_events,
                        codex.stdout.truncated = observer.events.stdout_truncated,
                        codex.stderr.truncated = observer.events.stderr_truncated,
                        codex.stderr.present = observer.events.stderr_present,
                        codex.output.stage = "post_exit_recovery",
                        codex.output.workspace_observed = capture.workspace_observed,
                        codex.output.runtime_observed = capture.runtime_observed,
                        codex.output.native_observed = capture.native_observed,
                        error.code = codex_output_error_code(&error),
                        "Codex completed without a recoverable image artifact"
                    );
                    map_cli_runtime_error_with_events(error, &observer.events)
                }
                Err(ProcessSpoolError::Integrity) => {
                    ChildOutcome::Failed("codex_ephemeral_output_invalid")
                }
                Err(
                    ProcessSpoolError::InvalidInput
                    | ProcessSpoolError::Conflict
                    | ProcessSpoolError::Unavailable,
                ) => ChildOutcome::Uncertain("codex_ephemeral_output_unavailable"),
            },
            Err(ProcessSpoolError::Integrity) => {
                ChildOutcome::Failed("codex_ephemeral_output_invalid")
            }
            Err(
                ProcessSpoolError::InvalidInput
                | ProcessSpoolError::Conflict
                | ProcessSpoolError::Unavailable,
            ) => ChildOutcome::Uncertain("codex_ephemeral_output_unavailable"),
        },
        Err(error) => map_cli_runtime_error(error),
    };
    match outcome {
        ChildOutcome::Succeeded(bytes) => match normalize_captured_image(
            bytes,
            &job.size,
            &job.output_format,
            job.output_compression,
        ) {
            Ok(bytes) => ChildOutcome::Succeeded(bytes),
            Err(()) => ChildOutcome::Failed("codex_ephemeral_output_invalid"),
        },
        outcome => outcome,
    }
}

fn normalize_captured_image(
    bytes: Vec<u8>,
    requested_size: &str,
    output_format: &str,
    output_compression: Option<u8>,
) -> Result<Vec<u8>, ()> {
    let actual_format = image::ImageReader::new(Cursor::new(&bytes))
        .with_guessed_format()
        .ok()
        .and_then(|reader| reader.format())
        .ok_or(())?;
    let requested_format = match output_format {
        "png" => image::ImageFormat::Png,
        "jpeg" => image::ImageFormat::Jpeg,
        "webp" => image::ImageFormat::WebP,
        _ => return Err(()),
    };
    if actual_format == requested_format {
        return Ok(bytes);
    }

    // Codex's native image tool can emit PNG even when the Images API caller
    // requested JPEG or WebP. Keep the CLI sandbox strict and perform the
    // required format conversion in the trusted gateway normalization layer.
    let mut images = crate::core::normalize_generated_images(
        vec![crate::core::GeneratedImage { bytes }],
        requested_size,
        output_format,
        output_compression,
    )
    .map_err(|_| ())?;
    if images.len() != 1 {
        return Err(());
    }
    Ok(images.remove(0).bytes)
}

async fn final_fallback_output(
    spool: &ExecutionSpool,
    workspace_filename: &str,
    codex_home: &Path,
    thread_id: Option<Uuid>,
    output_format: &str,
) -> Result<Option<CapturedCodexOutput>, ProcessSpoolError> {
    if let Some(bytes) = final_workspace_output(spool, workspace_filename)? {
        return Ok(Some(CapturedCodexOutput {
            bytes,
            source: CodexOutputSource::Workspace,
        }));
    }

    let generated_images = codex_home.join("generated_images");
    let thread_root = thread_id.map(|id| generated_images.join(id.to_string()));
    let root = thread_root.as_deref().unwrap_or(&generated_images);
    if let Some(path) = select_image_output(root, output_format) {
        let bytes = read_codex_output(&path)
            .await
            .map_err(|_| ProcessSpoolError::Unavailable)?;
        if !valid_captured_image(&bytes) {
            return Err(ProcessSpoolError::Integrity);
        }
        return Ok(Some(CapturedCodexOutput {
            bytes,
            source: CodexOutputSource::Native,
        }));
    }

    recover_codex_session_output(codex_home, thread_id).await
}

async fn recover_codex_session_output(
    codex_home: &Path,
    thread_id: Option<Uuid>,
) -> Result<Option<CapturedCodexOutput>, ProcessSpoolError> {
    let Some(thread_id) = thread_id else {
        return Ok(None);
    };
    let codex_home = codex_home.to_path_buf();
    tokio::task::spawn_blocking(move || {
        recover_codex_session_output_blocking(&codex_home, thread_id)
    })
    .await
    .map_err(|_| ProcessSpoolError::Unavailable)?
}

fn recover_codex_session_output_blocking(
    codex_home: &Path,
    thread_id: Uuid,
) -> Result<Option<CapturedCodexOutput>, ProcessSpoolError> {
    let mut session_files = Vec::new();
    collect_codex_session_files(
        &codex_home.join("sessions"),
        0,
        thread_id,
        &mut session_files,
    )?;
    session_files.sort();
    session_files.reverse();

    for path in session_files {
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = options
            .open(path)
            .map_err(|_| ProcessSpoolError::Unavailable)?;
        let metadata = file
            .metadata()
            .map_err(|_| ProcessSpoolError::Unavailable)?;
        if !metadata.is_file() || metadata.len() > MAX_CODEX_SESSION_FILE_BYTES {
            return Err(ProcessSpoolError::Integrity);
        }
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        let mut recovered = None;
        loop {
            let bytes = read_bounded_line(&mut reader, &mut line, MAX_CODEX_SESSION_LINE_BYTES)
                .map_err(|error| match error.kind() {
                    io::ErrorKind::InvalidData => ProcessSpoolError::Integrity,
                    _ => ProcessSpoolError::Unavailable,
                })?;
            if bytes == 0 {
                break;
            }
            let Ok(event) = serde_json::from_slice::<serde_json::Value>(&line) else {
                continue;
            };
            let Some(result) = codex_inline_image_result(&event) else {
                continue;
            };
            let encoded = result
                .split_once(",")
                .filter(|(prefix, _)| {
                    prefix.starts_with("data:image/") && prefix.ends_with(";base64")
                })
                .map_or(result, |(_, encoded)| encoded);
            if encoded.len() > MAX_CODEX_SESSION_LINE_BYTES {
                return Err(ProcessSpoolError::Integrity);
            }
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| ProcessSpoolError::Integrity)?;
            if bytes.is_empty() || bytes.len() as u64 > MAX_CODEX_RUNTIME_OUTPUT_BYTES {
                return Err(ProcessSpoolError::Integrity);
            }
            if !valid_captured_image(&bytes) {
                return Err(ProcessSpoolError::Integrity);
            }
            recovered = Some(CapturedCodexOutput {
                bytes,
                source: CodexOutputSource::Session,
            });
        }
        if recovered.is_some() {
            return Ok(recovered);
        }
    }
    Ok(None)
}

fn collect_codex_session_files(
    directory: &Path,
    depth: usize,
    thread_id: Uuid,
    files: &mut Vec<PathBuf>,
) -> Result<(), ProcessSpoolError> {
    if depth > MAX_CODEX_SESSION_DEPTH {
        return Err(ProcessSpoolError::Integrity);
    }
    let metadata = match fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(ProcessSpoolError::Unavailable),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ProcessSpoolError::Integrity);
    }
    let entries = fs::read_dir(directory).map_err(|_| ProcessSpoolError::Unavailable)?;
    for entry in entries {
        let entry = entry.map_err(|_| ProcessSpoolError::Unavailable)?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|_| ProcessSpoolError::Unavailable)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_codex_session_files(&path, depth + 1, thread_id, files)?;
            continue;
        }
        if metadata.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some("jsonl")
            && path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.contains(&thread_id.to_string()))
        {
            if files.len() == MAX_CODEX_SESSION_FILES {
                return Err(ProcessSpoolError::Integrity);
            }
            files.push(path);
        }
    }
    Ok(())
}

fn read_bounded_line(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
    max_bytes: usize,
) -> io::Result<usize> {
    line.clear();
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(line.len());
        }
        let count = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(buffer.len(), |index| index + 1);
        if line.len().saturating_add(count) > max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Codex session event exceeds the bounded recovery limit",
            ));
        }
        line.extend_from_slice(&buffer[..count]);
        reader.consume(count);
        if line.last() == Some(&b'\n') {
            return Ok(line.len());
        }
    }
}

fn codex_inline_image_result(value: &serde_json::Value) -> Option<&str> {
    match value {
        serde_json::Value::Object(fields) => {
            let event_type = fields
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if matches!(event_type, "image_generation_call" | "image_generation_end") {
                if let Some(result) = fields
                    .get("result")
                    .and_then(serde_json::Value::as_str)
                    .filter(|result| !result.is_empty())
                {
                    return Some(result);
                }
            }
            fields.values().find_map(codex_inline_image_result)
        }
        serde_json::Value::Array(values) => values.iter().find_map(codex_inline_image_result),
        _ => None,
    }
}

fn final_workspace_output(
    spool: &ExecutionSpool,
    filename: &str,
) -> Result<Option<Vec<u8>>, ProcessSpoolError> {
    match spool.read_workspace_output(filename, MAX_CODEX_RUNTIME_OUTPUT_BYTES)? {
        WorkspaceOutputSnapshot::Missing | WorkspaceOutputSnapshot::Incomplete => Ok(None),
        WorkspaceOutputSnapshot::Bytes(bytes) if valid_captured_image(&bytes) => Ok(Some(bytes)),
        WorkspaceOutputSnapshot::Bytes(_) => Err(ProcessSpoolError::Integrity),
    }
}

async fn capture_ephemeral_output(
    spool: Arc<ExecutionSpool>,
    workspace_filename: &'static str,
    runtime_filename: &'static str,
    codex_home: PathBuf,
    output_format: String,
    mut stop: oneshot::Receiver<()>,
) -> Result<CodexOutputCapture, ProcessSpoolError> {
    let mut capture = CodexOutputCapture::default();
    let mut first_poll = true;
    loop {
        let mut stopping = false;
        if first_poll {
            first_poll = false;
        } else {
            tokio::select! {
                biased;
                _ = &mut stop => stopping = true,
                _ = tokio::time::sleep(EPHEMERAL_OUTPUT_POLL_INTERVAL) => {}
            }
        }
        let read_spool = Arc::clone(&spool);
        let snapshots = tokio::task::spawn_blocking(move || {
            Ok::<_, ProcessSpoolError>((
                read_spool
                    .read_workspace_output(workspace_filename, MAX_CODEX_RUNTIME_OUTPUT_BYTES)?,
                read_spool.read_runtime_output(runtime_filename, MAX_CODEX_RUNTIME_OUTPUT_BYTES)?,
            ))
        })
        .await
        .map_err(|_| ProcessSpoolError::Unavailable)??;
        let workspace = validated_snapshot(snapshots.0, CodexOutputSource::Workspace).await?;
        let runtime = validated_snapshot(snapshots.1, CodexOutputSource::Runtime).await?;
        let native = snapshot_native_codex_output(&codex_home, &output_format).await?;

        if workspace.is_some() {
            capture.workspace_observed = true;
        }
        if runtime.is_some() {
            capture.runtime_observed = true;
        }
        if native.is_some() {
            capture.native_observed = true;
        }
        capture.output = runtime.or(workspace).or(native).or(capture.output);
        if stopping {
            return Ok(capture);
        }
    }
}

async fn validated_snapshot(
    snapshot: WorkspaceOutputSnapshot,
    source: CodexOutputSource,
) -> Result<Option<CapturedCodexOutput>, ProcessSpoolError> {
    let WorkspaceOutputSnapshot::Bytes(bytes) = snapshot else {
        return Ok(None);
    };
    tokio::task::spawn_blocking(move || {
        Ok(valid_captured_image(&bytes).then_some(CapturedCodexOutput { bytes, source }))
    })
    .await
    .map_err(|_| ProcessSpoolError::Unavailable)?
}

async fn snapshot_native_codex_output(
    codex_home: &Path,
    output_format: &str,
) -> Result<Option<CapturedCodexOutput>, ProcessSpoolError> {
    let Some(path) = select_image_output(&codex_home.join("generated_images"), output_format)
    else {
        return Ok(None);
    };
    let bytes = read_codex_output(&path)
        .await
        .map_err(|_| ProcessSpoolError::Unavailable)?;
    Ok(valid_captured_image(&bytes).then_some(CapturedCodexOutput {
        bytes,
        source: CodexOutputSource::Native,
    }))
}

fn valid_captured_image(bytes: &[u8]) -> bool {
    let mut reader = match image::ImageReader::new(Cursor::new(bytes)).with_guessed_format() {
        Ok(reader)
            if matches!(
                reader.format(),
                Some(image::ImageFormat::Png | image::ImageFormat::Jpeg | image::ImageFormat::WebP)
            ) =>
        {
            reader
        }
        _ => return false,
    };
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_DECODED_IMAGE_DIMENSION);
    limits.max_image_height = Some(MAX_DECODED_IMAGE_DIMENSION);
    limits.max_alloc = Some(MAX_DECODED_IMAGE_PIXELS * 8);
    reader.limits(limits);
    reader.decode().is_ok_and(|image| {
        image.width() > 0
            && image.height() > 0
            && u64::from(image.width()).saturating_mul(u64::from(image.height()))
                <= MAX_DECODED_IMAGE_PIXELS
    })
}

fn ephemeral_capture_fallback_allowed(error: &RuntimeError) -> bool {
    match error {
        RuntimeError::Output(OutputError::Missing) => true,
        RuntimeError::Output(OutputError::Unavailable(error)) => {
            error.kind() == std::io::ErrorKind::NotFound
        }
        _ => false,
    }
}

impl CliPolicy for CodexCliPolicy {
    type Request = CodexCliInvocation;
    type Error = CommandSpecError;

    fn command(&self, request: &Self::Request) -> Result<CommandSpec, Self::Error> {
        let mut command = CommandSpec::new(
            VerifiedExecutable::new(&request.executable)?,
            WorkingDirectory::new(&request.output_dir)?,
            OutputContract::new(request.output_filename, MAX_CODEX_RUNTIME_OUTPUT_BYTES)?,
            request.timeout,
            CHILD_REAP_TIMEOUT,
        )?
        .require_directory(WorkingDirectory::new(&request.workspace)?);
        for argument in [
            "exec",
            "--ignore-user-config",
            "--ignore-rules",
            "--disable",
            "plugins",
            "--disable",
            "apps",
            "--sandbox",
            "workspace-write",
            "--skip-git-repo-check",
            "--cd",
        ] {
            command = command.arg(argument)?;
        }
        command = command
            .arg(request.workspace.as_os_str())?
            .arg("--add-dir")?
            .arg(request.output_dir.as_os_str())?
            .arg("--json")?
            .arg("-")?;
        for (name, value) in [
            ("HOME", request.codex_home.to_string_lossy().into_owned()),
            (
                "CODEX_HOME",
                request.codex_home.to_string_lossy().into_owned(),
            ),
            ("TMPDIR", request.workspace.to_string_lossy().into_owned()),
            ("PATH", CODEX_CHILD_PATH.to_string()),
        ] {
            command = command.env(name, value)?;
        }
        for (name, value) in &request.environment {
            command = command.env(name, value)?;
        }
        command
            .stdin(request.prompt.clone())
            .map(CommandSpec::capture_process_output)
    }

    fn classify_exit(&self, status: &std::process::ExitStatus) -> ExitClassification {
        if status.success() {
            ExitClassification::Success
        } else {
            ExitClassification::Failed {
                code: "codex_cli_failed".to_string(),
            }
        }
    }
}

impl SpawnObserver for CodexSpawnObserver {
    type Error = ProcessSpoolError;

    fn observe_spawn(&mut self, evidence: &SpawnEvidence) -> Result<(), Self::Error> {
        ProviderProcessIdentity::capture(evidence.pid, &self.helper.nonce).and_then(|provider| {
            self.spool
                .publish_provider_process(&self.runner_lock, &self.helper, &provider)
        })
    }

    fn observe_completion(&mut self, completion: &ProcessCompletion) -> Result<(), Self::Error> {
        self.events = summarize_codex_events(completion);
        Ok(())
    }
}

fn summarize_codex_events(completion: &ProcessCompletion) -> CodexEventSummary {
    summarize_codex_event_stream(
        completion.stdout.bytes(),
        completion.stdout.is_truncated(),
        completion.stderr.bytes(),
        completion.stderr.is_truncated(),
    )
}

fn summarize_codex_event_stream(
    stdout: &[u8],
    stdout_truncated: bool,
    stderr: &[u8],
    stderr_truncated: bool,
) -> CodexEventSummary {
    let mut summary = CodexEventSummary {
        stdout_truncated,
        stderr_truncated,
        stderr_present: !stderr.is_empty(),
        ..CodexEventSummary::default()
    };
    for line in stdout.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let Ok(event) = serde_json::from_slice::<serde_json::Value>(line) else {
            summary.malformed_events = summary.malformed_events.saturating_add(1);
            continue;
        };
        if event.get("type").and_then(serde_json::Value::as_str) == Some("thread.started") {
            summary.thread_id = event
                .get("thread_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok());
        }
        let image_event = codex_image_event_state(&event);
        if image_event.seen {
            summary.saw_image_generation = true;
        }
        if image_event.completed {
            summary.completed_image_generation = true;
        }
    }
    summary
}

#[derive(Clone, Copy, Default)]
struct CodexImageEventState {
    seen: bool,
    completed: bool,
}

fn codex_image_event_state(value: &serde_json::Value) -> CodexImageEventState {
    match value {
        serde_json::Value::Object(fields) => {
            let event_type = fields
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let mut state = CodexImageEventState {
                seen: matches!(
                    event_type,
                    "image_generation_call" | "image_generation_begin" | "image_generation_end"
                ),
                completed: event_type == "image_generation_end",
            };
            for value in fields.values() {
                let child = codex_image_event_state(value);
                state.seen |= child.seen;
                state.completed |= child.completed;
            }
            if event_type == "item.completed" && state.seen {
                state.completed = true;
            }
            state
        }
        serde_json::Value::Array(values) => {
            values
                .iter()
                .fold(CodexImageEventState::default(), |mut state, value| {
                    let child = codex_image_event_state(value);
                    state.seen |= child.seen;
                    state.completed |= child.completed;
                    state
                })
        }
        _ => CodexImageEventState::default(),
    }
}

fn map_cli_runtime_error_with_events(
    error: RuntimeError,
    events: &CodexEventSummary,
) -> ChildOutcome {
    if events.completed_image_generation
        && matches!(
            error,
            RuntimeError::Output(OutputError::Missing)
                | RuntimeError::Output(OutputError::Unavailable(_))
        )
    {
        return ChildOutcome::Failed("codex_image_output_disappeared");
    }
    map_cli_runtime_error(error)
}

fn codex_output_error_code(error: &RuntimeError) -> &'static str {
    match error {
        RuntimeError::Output(OutputError::Missing) => "codex_no_image_output",
        RuntimeError::Output(OutputError::Unavailable(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            "codex_image_output_disappeared"
        }
        _ => "codex_runtime_failed",
    }
}

fn map_cli_runtime_error(error: RuntimeError) -> ChildOutcome {
    match error {
        RuntimeError::ProcessFailed { .. } => ChildOutcome::Failed("codex_cli_failed"),
        RuntimeError::Process(ProcessError::Spawn(_)) => {
            ChildOutcome::Failed("codex_cli_unavailable")
        }
        RuntimeError::Process(ProcessError::TimedOut { .. }) => {
            ChildOutcome::Uncertain("codex_timeout")
        }
        RuntimeError::Process(ProcessError::Observer { .. })
        | RuntimeError::Process(ProcessError::IdentityUnavailable) => {
            ChildOutcome::Uncertain("codex_process_identity_unavailable")
        }
        RuntimeError::Process(ProcessError::Stdin { .. }) => {
            ChildOutcome::Uncertain("codex_stdin_failed")
        }
        RuntimeError::Output(error) => match error {
            OutputError::Missing => ChildOutcome::Failed("codex_no_image_output"),
            OutputError::MultipleEntries => ChildOutcome::Failed("codex_multiple_image_outputs"),
            OutputError::NotRegular => ChildOutcome::Failed("codex_image_output_not_regular"),
            OutputError::Empty => ChildOutcome::Failed("codex_image_output_empty"),
            OutputError::TooLarge => ChildOutcome::Failed("codex_image_output_too_large"),
            OutputError::ChangedDuringRead => ChildOutcome::Uncertain("codex_image_output_changed"),
            OutputError::Unavailable(error) => match error.kind() {
                std::io::ErrorKind::NotFound => {
                    ChildOutcome::Failed("codex_image_output_disappeared")
                }
                std::io::ErrorKind::PermissionDenied => {
                    ChildOutcome::Failed("codex_image_output_unreadable")
                }
                _ => ChildOutcome::Uncertain("codex_output_unavailable"),
            },
            OutputError::UnsafeDirectory => {
                ChildOutcome::Uncertain("codex_output_directory_unsafe")
            }
            OutputError::InvalidLimit => ChildOutcome::Uncertain("codex_output_contract_invalid"),
            OutputError::DirectoryNotEmpty => {
                ChildOutcome::Failed("codex_output_directory_not_empty")
            }
            OutputError::Sink(_) => ChildOutcome::Uncertain("codex_output_sink_failed"),
        },
        RuntimeError::Policy(_)
        | RuntimeError::MissingOutputContract
        | RuntimeError::UnexpectedOutputContract
        | RuntimeError::CapturedOutputTooLarge { .. }
        | RuntimeError::Receipt(_)
        | RuntimeError::OutputTask(_)
        | RuntimeError::Process(ProcessError::InvalidCommand(_))
        | RuntimeError::Process(ProcessError::Capture { .. })
        | RuntimeError::Process(ProcessError::ResidualProcessGroup { .. })
        | RuntimeError::Process(ProcessError::Wait { .. }) => {
            ChildOutcome::Uncertain("codex_runtime_failed")
        }
    }
}

fn validate_child_request(
    request: &CodexChildRequest,
    executor_execution_id: Uuid,
) -> Result<ExecutorSubmissionLease, ImageGatewayError> {
    let lease = request.launch.to_lease().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Codex runner lease binding is invalid")
    })?;
    if request.schema_version != 1
        || request.adapter_revision != CODEX_GENERATION_ADAPTER_REVISION
        || request.executor_execution_id != executor_execution_id.to_string()
        || lease.executor_execution_id != executor_execution_id
        || lease.provider_id != image_provider_contracts::openai_codex::PROVIDER_ID
        || lease.adapter_revision != CODEX_GENERATION_ADAPTER_REVISION
        || lease.command_schema != crate::admission::GENERATION_COMMAND_SCHEMA
        || lease.model != request.output.model
        || u32::try_from(lease.output_index)
            .ok()
            .map(|index| index + 1)
            != Some(request.output.candidate_index)
        || request.timeout_ms == 0
        || request.timeout_ms > MAX_RUNNER_TIMEOUT.as_millis() as u64
        || request.output.validate().is_err()
    {
        return Err(ImageGatewayError::service_unavailable(
            "Codex runner request is invalid",
        ));
    }
    let executable = canonical_executable(Path::new(&request.codex_executable))?;
    if executable.to_string_lossy() != request.codex_executable
        || hash_bounded_file(&executable)? != request.codex_executable_sha256
    {
        return Err(ImageGatewayError::service_unavailable(
            "Codex executable identity changed",
        ));
    }
    Ok(lease)
}

fn generation_job(request: &CodexOutputRequest) -> GenerationJob {
    GenerationJob {
        request_id: request.request_id.clone(),
        model: request.model.clone(),
        prompt: request.prompt.clone(),
        moderation: request.moderation.clone(),
        n: request.original_n,
        size: request.size.clone(),
        quality: request.quality.clone(),
        output_format: request.output_format.clone(),
        output_compression: request.output_compression,
        background: request.background.clone(),
        stream: request.stream,
        partial_images: request.partial_images,
    }
}

fn canonical_executable(path: &Path) -> Result<PathBuf, ImageGatewayError> {
    if !path.is_absolute() {
        return Err(ImageGatewayError::config(
            "executor executable paths must be absolute",
        ));
    }
    let canonical = fs::canonicalize(path)
        .map_err(|_| ImageGatewayError::config("executor executable does not exist"))?;
    let metadata = fs::symlink_metadata(&canonical)
        .map_err(|_| ImageGatewayError::config("executor executable is invalid"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o111 == 0
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(ImageGatewayError::config(
            "executor executable must be a non-writable regular executable",
        ));
    }
    Ok(canonical)
}

fn validate_codex_executable_compatibility(
    path: &Path,
    restricted_path: &str,
) -> Result<(), ImageGatewayError> {
    let mut file = fs::File::open(path)
        .map_err(|_| ImageGatewayError::config("Codex executable is unreadable"))?;
    let mut header = [0_u8; MAX_SHEBANG_BYTES + 1];
    let count = file
        .read(&mut header)
        .map_err(|_| ImageGatewayError::config("Codex executable is unreadable"))?;
    let header = &header[..count];
    if !header.starts_with(b"#!") {
        return if is_native_executable_header(header) {
            Ok(())
        } else {
            Err(ImageGatewayError::config(
                "Codex executable must be a native executable or a valid shebang script",
            ))
        };
    }

    let line_end = header
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(count);
    if line_end > MAX_SHEBANG_BYTES {
        return Err(ImageGatewayError::config(
            "Codex executable shebang is invalid",
        ));
    }
    let shebang = std::str::from_utf8(&header[2..line_end])
        .map_err(|_| ImageGatewayError::config("Codex executable shebang is invalid"))?
        .trim_end_matches('\r');
    let mut arguments = shebang.split_ascii_whitespace();
    let interpreter = arguments
        .next()
        .ok_or_else(|| ImageGatewayError::config("Codex executable shebang is invalid"))?;
    let interpreter_path = Path::new(interpreter);
    if !interpreter_path.is_absolute() || !is_executable_file(interpreter_path) {
        return Err(ImageGatewayError::config(format!(
            "Codex executable shebang interpreter '{interpreter}' is unavailable"
        )));
    }

    if interpreter_path.file_name().and_then(|name| name.to_str()) == Some("env") {
        let arguments = arguments.collect::<Vec<_>>();
        let command = env_shebang_command(&arguments)
            .ok_or_else(|| ImageGatewayError::config("Codex executable shebang is invalid"))?;
        if !command_available(command, restricted_path) {
            return Err(ImageGatewayError::config(format!(
                "Codex executable requires '{command}', but it is unavailable in restricted PATH {restricted_path}"
            )));
        }
    }

    Ok(())
}

fn is_native_executable_header(header: &[u8]) -> bool {
    const NATIVE_MAGICS: [[u8; 4]; 9] = [
        *b"\x7fELF",
        [0xfe, 0xed, 0xfa, 0xce],
        [0xce, 0xfa, 0xed, 0xfe],
        [0xfe, 0xed, 0xfa, 0xcf],
        [0xcf, 0xfa, 0xed, 0xfe],
        [0xca, 0xfe, 0xba, 0xbe],
        [0xbe, 0xba, 0xfe, 0xca],
        [0xca, 0xfe, 0xba, 0xbf],
        [0xbf, 0xba, 0xfe, 0xca],
    ];
    header
        .get(..4)
        .is_some_and(|magic| NATIVE_MAGICS.iter().any(|expected| magic == expected))
}

fn env_shebang_command<'a>(arguments: &'a [&'a str]) -> Option<&'a str> {
    let mut index = 0;
    while let Some(argument) = arguments.get(index).copied() {
        match argument {
            "--" | "-S" | "--split-string" => return arguments.get(index + 1).copied(),
            "-u" | "--unset" | "-C" | "--chdir" => index += 2,
            _ if argument.starts_with('-') || argument.contains('=') => index += 1,
            _ => return Some(argument),
        }
    }
    None
}

fn command_available(command: &str, restricted_path: &str) -> bool {
    if command.contains('/') {
        return Path::new(command).is_absolute() && is_executable_file(Path::new(command));
    }
    restricted_path
        .split(':')
        .filter(|directory| !directory.is_empty())
        .any(|directory| is_executable_file(&Path::new(directory).join(command)))
}

fn is_executable_file(path: &Path) -> bool {
    fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

fn hash_bounded_file(path: &Path) -> Result<String, ImageGatewayError> {
    let mut file = fs::File::open(path)
        .map_err(|_| ImageGatewayError::config("executor executable is unreadable"))?;
    let size = file
        .metadata()
        .map_err(|_| ImageGatewayError::config("executor executable is unreadable"))?
        .len();
    if size == 0 || size > MAX_EXECUTABLE_BYTES {
        return Err(ImageGatewayError::config(
            "executor executable size is invalid",
        ));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut read = 0_u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| ImageGatewayError::config("executor executable is unreadable"))?;
        if count == 0 {
            break;
        }
        read += count as u64;
        if read > MAX_EXECUTABLE_BYTES {
            return Err(ImageGatewayError::config(
                "executor executable size is invalid",
            ));
        }
        hasher.update(&buffer[..count]);
    }
    if read != size {
        return Err(ImageGatewayError::config(
            "executor executable changed during validation",
        ));
    }
    Ok(hex::encode(hasher.finalize()))
}

fn child_environment(proxy: &ProxyConfig) -> Vec<(String, String)> {
    let mut values = Vec::new();
    for name in ["LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR"] {
        if let Ok(value) = env::var(name) {
            values.push((name.to_string(), value));
        }
    }
    for (name, value) in [
        ("HTTP_PROXY", proxy.http_proxy.as_ref()),
        ("HTTPS_PROXY", proxy.https_proxy.as_ref()),
        ("ALL_PROXY", proxy.all_proxy.as_ref()),
        ("NO_PROXY", proxy.no_proxy.as_ref()),
    ] {
        if let Some(value) = value {
            values.push((name.to_string(), value.clone()));
            values.push((name.to_ascii_lowercase(), value.clone()));
        }
    }
    values
}

fn allowed_child_environment() -> Vec<(String, String)> {
    [
        "LANG",
        "LC_ALL",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ]
    .into_iter()
    .filter_map(|name| env::var(name).ok().map(|value| (name.to_string(), value)))
    .collect()
}

fn map_spool_error(error: ProcessSpoolError) -> RunnerError {
    let error_code = match error {
        ProcessSpoolError::InvalidInput => "runner_spool_invalid",
        ProcessSpoolError::Conflict => "runner_spool_conflict",
        ProcessSpoolError::Integrity => "runner_spool_integrity",
        ProcessSpoolError::Unavailable => "runner_spool_unavailable",
    };
    RunnerError::Unknown {
        error_code: error_code.to_string(),
    }
}

fn child_spool_error(_error: ProcessSpoolError) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Codex runner spool is unavailable")
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use tempfile::TempDir;

    use super::*;
    use crate::admission::{
        GENERATION_COMMAND_SCHEMA, GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION,
        GenerationCommandV1,
    };
    use crate::executor::private_auth::{AUTH_FILE, MAX_AUTH_BYTES};

    #[test]
    fn child_environment_excludes_gateway_and_database_secrets() {
        let proxy = ProxyConfig::default();
        let names = child_environment(&proxy)
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>();

        for forbidden in [
            "DATABASE_URL",
            "GATEWAY_API_TOKEN",
            "GATEWAY_ADMIN_TOKEN",
            "OTEL_EXPORTER_OTLP_HEADERS",
        ] {
            assert!(!names.iter().any(|name| name == forbidden));
        }
    }

    #[test]
    fn output_seal_failures_preserve_actionable_terminal_codes() {
        assert!(matches!(
            map_cli_runtime_error(RuntimeError::Output(OutputError::Missing)),
            ChildOutcome::Failed("codex_no_image_output")
        ));
        assert!(matches!(
            map_cli_runtime_error(RuntimeError::Output(OutputError::MultipleEntries)),
            ChildOutcome::Failed("codex_multiple_image_outputs")
        ));
        assert!(matches!(
            map_cli_runtime_error(RuntimeError::Output(OutputError::NotRegular)),
            ChildOutcome::Failed("codex_image_output_not_regular")
        ));
        assert!(matches!(
            map_cli_runtime_error(RuntimeError::Output(OutputError::TooLarge)),
            ChildOutcome::Failed("codex_image_output_too_large")
        ));
        assert!(matches!(
            map_cli_runtime_error(RuntimeError::Output(OutputError::ChangedDuringRead)),
            ChildOutcome::Uncertain("codex_image_output_changed")
        ));
        assert!(matches!(
            map_cli_runtime_error(RuntimeError::Output(OutputError::Unavailable(
                std::io::Error::from(std::io::ErrorKind::NotFound),
            ))),
            ChildOutcome::Failed("codex_image_output_disappeared")
        ));
    }

    #[test]
    fn codex_json_events_bind_native_output_and_terminal_diagnostics() {
        let thread_id = Uuid::new_v4();
        let stdout = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{thread_id}\"}}\n\
             {{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\"}}}}\n\
             not-json\n"
        );

        let summary = summarize_codex_event_stream(stdout.as_bytes(), false, b"warning", true);

        assert_eq!(summary.thread_id, Some(thread_id));
        assert!(summary.saw_image_generation);
        assert!(summary.completed_image_generation);
        assert_eq!(summary.malformed_events, 1);
        assert!(summary.stderr_present);
        assert!(summary.stderr_truncated);
    }

    #[test]
    fn completed_image_event_distinguishes_lost_artifact_from_no_generation() {
        let events = CodexEventSummary {
            saw_image_generation: true,
            completed_image_generation: true,
            ..CodexEventSummary::default()
        };

        assert!(matches!(
            map_cli_runtime_error_with_events(RuntimeError::Output(OutputError::Missing), &events,),
            ChildOutcome::Failed("codex_image_output_disappeared")
        ));
        assert!(matches!(
            map_cli_runtime_error_with_events(
                RuntimeError::Output(OutputError::Missing),
                &CodexEventSummary::default(),
            ),
            ChildOutcome::Failed("codex_no_image_output")
        ));
    }

    #[test]
    fn auth_digest_uses_the_same_private_file_contract_as_runtime() {
        let temp = TempDir::new().unwrap();
        let auth = temp.path().join(AUTH_FILE);
        fs::write(&auth, b"{\"tokens\":{}}").unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            codex_auth_file_sha256(temp.path()).unwrap(),
            sha256(b"{\"tokens\":{}}")
        );

        fs::set_permissions(&auth, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(codex_auth_file_sha256(temp.path()).is_err());
    }

    #[test]
    fn auth_digest_is_bound_to_exact_bytes_and_rejects_file_aliases() {
        let first = TempDir::new().unwrap();
        let first_auth = first.path().join(AUTH_FILE);
        fs::write(&first_auth, b"{}\n").unwrap();
        fs::set_permissions(&first_auth, fs::Permissions::from_mode(0o600)).unwrap();
        let first_digest = codex_auth_file_sha256(first.path()).unwrap();

        let second = TempDir::new().unwrap();
        let second_auth = second.path().join(AUTH_FILE);
        fs::write(&second_auth, b"{}").unwrap();
        fs::set_permissions(&second_auth, fs::Permissions::from_mode(0o600)).unwrap();
        assert_ne!(first_digest, codex_auth_file_sha256(second.path()).unwrap());

        let hardlink = second.path().join("auth-hardlink.json");
        fs::hard_link(&second_auth, hardlink).unwrap();
        assert!(codex_auth_file_sha256(second.path()).is_err());

        let symlinked = TempDir::new().unwrap();
        let target = symlinked.path().join("target.json");
        fs::write(&target, b"{}").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        symlink(&target, symlinked.path().join(AUTH_FILE)).unwrap();
        assert!(codex_auth_file_sha256(symlinked.path()).is_err());
    }

    #[test]
    fn auth_digest_rejects_relative_empty_and_oversized_sources() {
        assert!(codex_auth_file_sha256(Path::new("relative-home")).is_err());

        let empty = TempDir::new().unwrap();
        let empty_auth = empty.path().join(AUTH_FILE);
        fs::write(&empty_auth, []).unwrap();
        fs::set_permissions(&empty_auth, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(codex_auth_file_sha256(empty.path()).is_err());

        let oversized = TempDir::new().unwrap();
        let oversized_auth = oversized.path().join(AUTH_FILE);
        fs::write(&oversized_auth, vec![b'x'; MAX_AUTH_BYTES as usize + 1]).unwrap();
        fs::set_permissions(&oversized_auth, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(codex_auth_file_sha256(oversized.path()).is_err());
    }

    #[test]
    fn native_codex_executable_is_compatible_with_restricted_path() {
        validate_codex_executable_compatibility(
            &std::env::current_exe().unwrap(),
            CODEX_CHILD_PATH,
        )
        .unwrap();
    }

    #[test]
    fn absolute_shebang_interpreter_is_compatible_when_executable() {
        let temp = TempDir::new().unwrap();
        let script = temp.path().join("codex-shell-wrapper");
        fs::write(&script, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        validate_codex_executable_compatibility(&script, CODEX_CHILD_PATH).unwrap();
    }

    #[test]
    fn env_shebang_is_rejected_when_node_is_missing_from_restricted_path() {
        let temp = TempDir::new().unwrap();
        let script = temp.path().join("codex-node-wrapper");
        fs::write(&script, "#!/usr/bin/env node\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let empty_path = temp.path().join("restricted-bin");
        fs::create_dir(&empty_path).unwrap();

        let error = validate_codex_executable_compatibility(&script, empty_path.to_str().unwrap())
            .unwrap_err();

        assert_eq!(error.error_code(), Some("configuration_error"));
        let error = format!("{error:?}");
        assert!(error.contains("requires 'node'"));
        assert!(error.contains("unavailable in restricted PATH"));
    }

    #[test]
    fn malformed_or_bom_prefixed_script_is_not_treated_as_native() {
        let temp = TempDir::new().unwrap();
        let script = temp.path().join("codex-invalid-wrapper");
        fs::write(&script, b"\xef\xbb\xbf#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        let error = validate_codex_executable_compatibility(&script, CODEX_CHILD_PATH).unwrap_err();

        assert_eq!(error.error_code(), Some("configuration_error"));
        assert!(format!("{error:?}").contains("native executable or a valid shebang"));
    }

    #[tokio::test]
    async fn helper_process_spool_attaches_without_a_second_codex_launch() {
        let fixture = CodexFixture::new();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let root = fixture.journal.root_path().to_path_buf();
        let execution_id = lease.executor_execution_id;
        let child = tokio::spawn(async move {
            run_codex_runner_child(root, execution_id).await.unwrap();
        });

        let first = fixture
            .supervisor
            .start_or_attach(&lease, LaunchDecision::Attach)
            .await
            .unwrap();
        child.await.unwrap();
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("codex-home").exists());
        assert!(!execution_root.join("workspace").exists());
        assert!(!execution_root.join("runtime-home").exists());
        assert!(execution_root.join("output.bin").is_file());
        let replay = fixture
            .supervisor
            .start_or_attach(&lease, LaunchDecision::Attach)
            .await
            .unwrap();

        assert_eq!(first, replay);
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n");
    }

    #[tokio::test]
    async fn captures_provider_output_before_codex_deletes_it_on_clean_exit() {
        let fixture = CodexFixture::ephemeral_workspace_output(0);
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        let expected = fs::read(fixture._temp.path().join("source.png")).unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert!(
            fixture
                ._temp
                .path()
                .join("provider-output-created")
                .is_file()
        );
        assert!(
            fixture
                ._temp
                .path()
                .join("provider-output-deleted")
                .is_file()
        );
        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(
                expected.clone()
            ))
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert_eq!(
            fs::read(execution_root.join("output.bin")).unwrap(),
            expected
        );
        assert!(!execution_root.join("codex-home").exists());
        assert!(!execution_root.join("workspace").exists());
        assert!(!execution_root.join("runtime-home").exists());
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n");
    }

    #[tokio::test]
    async fn recovers_retained_workspace_output_after_runtime_output_disappears() {
        let fixture = CodexFixture::disappearing_runtime_output();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        let expected = fs::read(fixture._temp.path().join("source.png")).unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
        );
    }

    #[tokio::test]
    async fn recovers_short_lived_workspace_output_from_one_complete_snapshot() {
        let fixture = CodexFixture::short_lived_workspace_output();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        let expected = fs::read(fixture._temp.path().join("source.png")).unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
        );
    }

    #[tokio::test]
    async fn stop_signal_takes_one_final_runtime_output_snapshot() {
        let fixture = CodexFixture::new();
        let lease = fixture.lease();
        fixture.journal.start_or_attach(&lease).unwrap();
        let spool = Arc::new(ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap());
        let expected = fs::read(fixture._temp.path().join("source.png")).unwrap();
        let runtime_output = spool
            .runtime_home_path()
            .unwrap()
            .join(CODEX_RUNTIME_OUTPUT_FILE);
        let codex_home = spool.codex_home_path().unwrap().to_path_buf();
        let (stop, stop_rx) = oneshot::channel();
        let capture_spool = Arc::clone(&spool);
        let capture = tokio::spawn(async move {
            capture_ephemeral_output(
                capture_spool,
                "provider-output.png",
                CODEX_RUNTIME_OUTPUT_FILE,
                codex_home,
                "png".to_string(),
                stop_rx,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        fs::write(runtime_output, &expected).unwrap();
        stop.send(()).unwrap();
        let capture = capture.await.unwrap().unwrap();
        let output = capture.output.expect("final snapshot should retain output");

        assert_eq!(output.source, CodexOutputSource::Runtime);
        assert_eq!(output.bytes, expected);
        assert!(capture.runtime_observed);
        assert!(!capture.workspace_observed);
    }

    #[tokio::test]
    async fn native_output_capture_is_scoped_to_the_execution_codex_home() {
        let fixture = CodexFixture::new();
        let first_lease = fixture.lease();
        let second_lease = fixture.lease();
        fixture.journal.start_or_attach(&first_lease).unwrap();
        fixture.journal.start_or_attach(&second_lease).unwrap();
        let first_spool = ExecutionSpool::for_lease(&fixture.journal, &first_lease).unwrap();
        let second_spool = ExecutionSpool::for_lease(&fixture.journal, &second_lease).unwrap();
        let expected = fs::read(fixture._temp.path().join("source.png")).unwrap();
        let second_home = second_spool.codex_home_path().unwrap().to_path_buf();
        let second_output = second_home
            .join("generated_images")
            .join("019fd9f5-badb-7dd3-8903-28ffded0ef54")
            .join("generated.png");
        fs::create_dir_all(second_output.parent().unwrap()).unwrap();
        fs::write(&second_output, &expected).unwrap();

        let first = snapshot_native_codex_output(first_spool.codex_home_path().unwrap(), "png")
            .await
            .unwrap();
        let second = snapshot_native_codex_output(&second_home, "png")
            .await
            .unwrap()
            .expect("the owning execution should recover its native output");

        assert!(first.is_none());
        assert_eq!(second.source, CodexOutputSource::Native);
        assert_eq!(second.bytes, expected);
        assert_ne!(
            first_spool.codex_home_path().unwrap(),
            second_spool.codex_home_path().unwrap()
        );
    }

    #[tokio::test]
    async fn native_png_is_published_as_the_requested_jpeg_format() {
        let fixture = CodexFixture::generated_images_output_only();
        let mut command = fixture.command();
        command.output_format = "jpeg".to_string();
        command.output_compression = Some(80);
        let lease = fixture.lease_for_command(&command);
        let context = fixture.context_for_command(&lease, command);
        let expected = normalize_captured_image(
            fs::read(fixture._temp.path().join("source.png")).unwrap(),
            "1024x1024",
            "jpeg",
            Some(80),
        )
        .unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert!(expected.starts_with(&[0xff, 0xd8, 0xff]));
        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
        );
    }

    #[tokio::test]
    async fn recovers_isolated_codex_generated_image_when_prompt_sealing_is_missing() {
        let fixture = CodexFixture::generated_images_output_only();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        let expected = fs::read(fixture._temp.path().join("source.png")).unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("codex-home").exists());
        assert!(!execution_root.join("workspace").exists());
        assert!(!execution_root.join("runtime-home").exists());
    }

    #[tokio::test]
    async fn recovers_inline_image_from_the_execution_session_before_cleanup() {
        let fixture = CodexFixture::session_output_only();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        let expected = fs::read(fixture._temp.path().join("source.png")).unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("codex-home").exists());
        assert!(!execution_root.join("workspace").exists());
        assert!(!execution_root.join("runtime-home").exists());
    }

    #[tokio::test]
    async fn concurrent_session_recovery_is_scoped_to_each_execution_home() {
        let root = TempDir::new().unwrap();
        let first_home = root.path().join("first");
        let second_home = root.path().join("second");
        let first_thread = Uuid::new_v4();
        let second_thread = Uuid::new_v4();
        let first = png_bytes(1, 1);
        let second = png_bytes(2, 1);
        write_session_rollout(&first_home, first_thread, &first);
        write_session_rollout(&second_home, second_thread, &second);

        let (first_recovery, second_recovery) = tokio::join!(
            recover_codex_session_output(&first_home, Some(first_thread)),
            recover_codex_session_output(&second_home, Some(second_thread)),
        );

        let first_recovery = first_recovery.unwrap().unwrap();
        let second_recovery = second_recovery.unwrap().unwrap();
        assert_eq!(first_recovery.source, CodexOutputSource::Session);
        assert_eq!(second_recovery.source, CodexOutputSource::Session);
        assert_eq!(first_recovery.bytes, first);
        assert_eq!(second_recovery.bytes, second);
        assert!(
            recover_codex_session_output(&first_home, Some(second_thread))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn session_recovery_uses_the_latest_image_after_a_bounded_tool_retry() {
        let root = TempDir::new().unwrap();
        let thread_id = Uuid::new_v4();
        let first = png_bytes(1, 1);
        let retried = png_bytes(2, 1);
        write_session_rollout_with_images(root.path(), thread_id, &[&first, &retried]);

        let recovered = recover_codex_session_output(root.path(), Some(thread_id))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(recovered.source, CodexOutputSource::Session);
        assert_eq!(recovered.bytes, retried);
    }

    #[tokio::test]
    async fn captured_output_is_never_published_after_failed_codex_exit() {
        let fixture = CodexFixture::ephemeral_workspace_output(1);
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Failed {
                error_code: "codex_cli_failed".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("output.bin").exists());
    }

    #[tokio::test]
    async fn preexisting_workspace_output_is_never_attributed_to_a_new_process() {
        let fixture = CodexFixture::new();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        fs::copy(
            fixture._temp.path().join("source.png"),
            spool.workspace_path().unwrap().join("provider-output.png"),
        )
        .unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Uncertain {
                error_code: "codex_workspace_output_preexisting".to_string(),
            }
        );
        assert!(!fixture.invocations.exists());
    }

    #[tokio::test]
    async fn capture_tracks_the_latest_stable_workspace_output_until_exit() {
        let fixture = CodexFixture::changing_workspace_output();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        let expected = fs::read(fixture._temp.path().join("source-2.png")).unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
        );
    }

    #[tokio::test]
    async fn helper_rejects_direct_execution_before_launch_commit() {
        let fixture = CodexFixture::new();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();

        assert!(
            run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id,)
                .await
                .is_err()
        );
        assert!(!fixture.invocations.exists());
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        assert!(matches!(
            spool.observe().unwrap(),
            ProcessObservation::AwaitingProcess
        ));
    }

    #[test]
    fn orphan_helper_subprocess_entry() {
        let Some(root) = env::var_os("CODEX_TEST_RUNNER_ROOT") else {
            return;
        };
        let execution_id = env::var("CODEX_TEST_EXECUTION_ID")
            .ok()
            .and_then(|value| Uuid::parse_str(&value).ok())
            .unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_codex_runner_child(PathBuf::from(root), execution_id))
            .unwrap();
    }

    #[tokio::test]
    async fn killed_helper_causes_bounded_provider_process_group_cleanup() {
        let fixture = CodexFixture::slow();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        let mut helper = Command::new(std::env::current_exe().unwrap());
        helper
            .arg("orphan_helper_subprocess_entry")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env("CODEX_TEST_RUNNER_ROOT", fixture.journal.root_path())
            .env(
                "CODEX_TEST_EXECUTION_ID",
                lease.executor_execution_id.to_string(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut helper = helper.spawn().unwrap();
        wait_for_path(&fixture._temp.path().join("provider-started")).await;
        helper.start_kill().unwrap();
        helper.wait().await.unwrap();

        let provider = loop {
            match spool.observe().unwrap() {
                ProcessObservation::Lost {
                    provider: Some(provider),
                } => break provider,
                ProcessObservation::Running(_) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                observation => panic!("unexpected process observation: {observation:?}"),
            }
        };
        assert!(provider.is_current_process_group().unwrap());
        assert_eq!(
            fixture
                .supervisor
                .start_or_attach(&lease, LaunchDecision::Attach)
                .await,
            Err(RunnerError::Unknown {
                error_code: "runner_process_lost".to_string(),
            })
        );
        assert!(!provider.is_current_process_group().unwrap());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!fixture._temp.path().join("provider-completed").exists());
    }

    async fn wait_for_path(path: &Path) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    struct CodexFixture {
        _temp: TempDir,
        journal: Arc<FilesystemRunnerJournal>,
        supervisor: CodexProcessSupervisor,
        invocations: PathBuf,
    }

    impl CodexFixture {
        fn new() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\n/bin/cp '{}' sealed-output.bin\n",
                    invocations.display(),
                    image.display()
                )
            })
        }

        fn slow() -> Self {
            Self::with_script(|invocations, image, root| {
                format!(
                    "#!/bin/sh\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nprintf 'started\\n' > '{}'\n/bin/sleep 30\nprintf 'completed\\n' > '{}'\n/bin/cp '{}' sealed-output.bin\n",
                    invocations.display(),
                    root.join("provider-started").display(),
                    root.join("provider-completed").display(),
                    image.display()
                )
            })
        }

        fn ephemeral_workspace_output(exit_code: u8) -> Self {
            Self::with_script(|invocations, image, root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nworkspace=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--cd' ]; then\n    shift\n    workspace=\"$1\"\n    break\n  fi\n  shift\ndone\ntest -n \"$workspace\"\n/bin/cp '{}' \"$workspace/provider-output.png\"\nprintf 'created\\n' > '{}'\n/bin/sleep 1\n/bin/rm \"$workspace/provider-output.png\"\nprintf 'deleted\\n' > '{}'\nexit {}\n",
                    invocations.display(),
                    image.display(),
                    root.join("provider-output-created").display(),
                    root.join("provider-output-deleted").display(),
                    exit_code,
                )
            })
        }

        fn disappearing_runtime_output() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nworkspace=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--cd' ]; then\n    shift\n    workspace=\"$1\"\n    break\n  fi\n  shift\ndone\ntest -n \"$workspace\"\n/bin/cp '{}' \"$workspace/provider-output.png\"\n/bin/cp '{}' sealed-output.bin\n/bin/rm sealed-output.bin\n",
                    invocations.display(),
                    image.display(),
                    image.display(),
                )
            })
        }

        fn short_lived_workspace_output() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nworkspace=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--cd' ]; then\n    shift\n    workspace=\"$1\"\n    break\n  fi\n  shift\ndone\ntest -n \"$workspace\"\n/bin/sleep 0.05\n/bin/cp '{}' \"$workspace/provider-output.png\"\n/bin/sleep 0.25\n/bin/rm \"$workspace/provider-output.png\"\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn generated_images_output_only() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\n/bin/mkdir -p \"$CODEX_HOME/generated_images/$thread_id\"\n/bin/cp '{}' \"$CODEX_HOME/generated_images/$thread_id/generated.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\"}}}}\\n'\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn session_output_only() -> Self {
            Self::with_script(|invocations, image, _root| {
                let encoded = STANDARD.encode(fs::read(image).unwrap());
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nfor argument in \"$@\"; do\n  test \"$argument\" != '--ephemeral'\ndone\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\nsession_dir=\"$CODEX_HOME/sessions/2026/08/13\"\n/bin/mkdir -p \"$session_dir\"\nprintf '%s\\n' '{{\"timestamp\":\"2026-08-13T00:00:00Z\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"image_generation_end\",\"call_id\":\"ig-test\",\"status\":\"generating\",\"result\":\"{}\"}}}}' > \"$session_dir/rollout-2026-08-13T00-00-00-$thread_id.jsonl\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\"}}}}\\n'\n",
                    invocations.display(),
                    encoded,
                )
            })
        }

        fn changing_workspace_output() -> Self {
            Self::with_script(|invocations, image, root| {
                let second = root.join("source-2.png");
                let mut bytes = std::io::Cursor::new(Vec::new());
                image::DynamicImage::new_rgb8(2, 1)
                    .write_to(&mut bytes, image::ImageFormat::Png)
                    .unwrap();
                fs::write(&second, bytes.into_inner()).unwrap();
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nworkspace=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--cd' ]; then\n    shift\n    workspace=\"$1\"\n    break\n  fi\n  shift\ndone\ntest -n \"$workspace\"\n/bin/cp '{}' \"$workspace/provider-output.png\"\n/bin/sleep 1\n/bin/cp '{}' \"$workspace/provider-output.png\"\n/bin/sleep 1\n/bin/rm \"$workspace/provider-output.png\"\nexit 0\n",
                    invocations.display(),
                    image.display(),
                    second.display(),
                )
            })
        }

        fn with_script(build_script: impl FnOnce(&Path, &Path, &Path) -> String) -> Self {
            let temp = TempDir::new().unwrap();
            let journal =
                Arc::new(FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap());
            let credentials = temp.path().join("credentials");
            fs::create_dir(&credentials).unwrap();
            let auth = credentials.join(AUTH_FILE);
            fs::write(&auth, b"{}").unwrap();
            fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();
            let image = temp.path().join("source.png");
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgba8(1, 1)
                .write_to(&mut bytes, image::ImageFormat::Png)
                .unwrap();
            fs::write(&image, bytes.into_inner()).unwrap();
            let invocations = temp.path().join("invocations");
            let executable = temp.path().join("fake-codex");
            fs::write(&executable, build_script(&invocations, &image, temp.path())).unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
            let supervisor = CodexProcessSupervisor::new(
                journal.clone(),
                &executable,
                &executable,
                &credentials,
                &sha256(b"{}"),
                Duration::from_secs(5),
                Duration::from_millis(10),
                Duration::from_secs(1),
                &ProxyConfig::default(),
            )
            .unwrap();
            Self {
                _temp: temp,
                journal,
                supervisor,
                invocations,
            }
        }

        fn command(&self) -> GenerationCommandV1 {
            GenerationCommandV1 {
                background: "opaque".to_string(),
                model: "gpt-image-2".to_string(),
                moderation: None,
                n: 2,
                operation: GENERATION_OPERATION.to_string(),
                output_compression: None,
                output_format: "png".to_string(),
                partial_images: 0,
                prompt: "draw a lighthouse".to_string(),
                provider_id: "openai-codex".to_string(),
                quality: "high".to_string(),
                schema_version: GENERATION_COMMAND_SCHEMA_VERSION,
                size: "1024x1024".to_string(),
                source_api_profile: "openai-images-v1".to_string(),
                stream: false,
            }
        }

        fn lease(&self) -> ExecutorSubmissionLease {
            let command = self.command();
            self.lease_for_command(&command)
        }

        fn lease_for_command(&self, command: &GenerationCommandV1) -> ExecutorSubmissionLease {
            ExecutorSubmissionLease {
                submission_id: Uuid::new_v4(),
                executor_execution_id: Uuid::new_v4(),
                output_id: Uuid::new_v4(),
                job_id: Uuid::new_v4(),
                tenant_id: "tenant-1".to_string(),
                provider_id: command.provider_id.clone(),
                model: command.model.clone(),
                work_item_id: Uuid::new_v4(),
                output_index: 1,
                command_schema: GENERATION_COMMAND_SCHEMA.to_string(),
                command_hash: command.request_hash_hex(),
                execution_profile_id: Uuid::new_v4(),
                adapter_revision: CODEX_GENERATION_ADAPTER_REVISION.to_string(),
                executor_owner: "executor-owner-1".to_string(),
                executor_lease_epoch: 1,
                executor_lease_expires_at_ms: i64::MAX,
            }
        }

        fn context(&self, lease: &ExecutorSubmissionLease) -> ExecutorLaunchContext {
            let command = self.command();
            self.context_for_command(lease, command)
        }

        fn context_for_command(
            &self,
            lease: &ExecutorSubmissionLease,
            command: GenerationCommandV1,
        ) -> ExecutorLaunchContext {
            ExecutorLaunchContext {
                request_id: "request-1".to_string(),
                api_profile: command.source_api_profile.clone(),
                output_index: lease.output_index,
                command_schema: lease.command_schema.clone(),
                command_hash: lease.command_hash.clone(),
                command_json: serde_json::to_value(command).unwrap(),
                inputs: Vec::new(),
            }
        }
    }

    fn png_bytes(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(width, height)
            .write_to(&mut bytes, image::ImageFormat::Png)
            .unwrap();
        bytes.into_inner()
    }

    fn write_session_rollout(codex_home: &Path, thread_id: Uuid, image: &[u8]) {
        write_session_rollout_with_images(codex_home, thread_id, &[image]);
    }

    fn write_session_rollout_with_images(codex_home: &Path, thread_id: Uuid, images: &[&[u8]]) {
        let directory = codex_home.join("sessions/2026/08/13");
        fs::create_dir_all(&directory).unwrap();
        let events = images
            .iter()
            .enumerate()
            .map(|(index, image)| {
                serde_json::json!({
                    "timestamp": "2026-08-13T00:00:00Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "image_generation_end",
                        "call_id": format!("ig-test-{index}"),
                        "status": "generating",
                        "result": STANDARD.encode(image),
                    }
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(
            directory.join(format!("rollout-2026-08-13T00-00-00-{thread_id}.jsonl")),
            format!("{events}\n"),
        )
        .unwrap();
    }
}
