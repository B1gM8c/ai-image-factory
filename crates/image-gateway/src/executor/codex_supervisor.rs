use std::{
    env, fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

#[cfg(test)]
use std::io::Cursor;

use async_trait::async_trait;
use image_cli_runtime::{
    CliPolicy, CliRuntime, CommandSpec, CommandSpecError, ExitClassification, OutputContract,
    OutputError, ProcessCompletion, ProcessError, RuntimeError, SpawnEvidence, SpawnObserver,
    VerifiedExecutable, WorkingDirectory,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{process::Command, time::Instant};
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
    providers::openai_codex::build_codex_prompt_for_output,
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

#[derive(Clone)]
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
    thread_id_ambiguous: bool,
    image_call_id: Option<String>,
    image_call_ambiguous: bool,
    native_handoff: CodexNativeHandoff,
    saw_image_generation: bool,
    completed_image_generation: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    stderr_present: bool,
    malformed_events: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum CodexNativeHandoff {
    #[default]
    NotAttempted,
    Sealed,
    Missing,
    Invalid,
    Unavailable,
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
            let terminal = ProcessTerminal::Failed {
                helper_nonce: identity.nonce.clone(),
                error_code: error_code.to_owned(),
            };
            spool
                .publish_terminal(&runner_lock, &terminal)
                .map_err(child_spool_error)?
        }
        ChildOutcome::Uncertain(error_code) => spool
            .publish_terminal(
                &runner_lock,
                &ProcessTerminal::Uncertain {
                    helper_nonce: identity.nonce.clone(),
                    error_code: error_code.to_owned(),
                },
            )
            .map_err(child_spool_error)?,
    }
    if let Err(error) = spool.cleanup_codex_runtime() {
        tracing::warn!(
            %executor_execution_id,
            ?error,
            "Codex runtime cleanup failed after durable terminal; runtime retained"
        );
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
    let runtime_result = CliRuntime::new(CodexCliPolicy)
        .run_to_sink(&invocation, &mut observer, Vec::new())
        .await;
    let outcome = match runtime_result {
        Ok(result) => ChildOutcome::Succeeded(result.sink),
        Err(error) => {
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
                codex.output.stage = "sealed_handoff",
                error.code = codex_output_error_code(&error),
                "Codex completed without a valid sealed image handoff"
            );
            map_cli_runtime_error_with_events(error, &observer.events)
        }
    };
    match outcome {
        ChildOutcome::Succeeded(bytes) => {
            match normalize_captured_image(bytes, &job.output_format, job.output_compression) {
                Ok(bytes) => ChildOutcome::Succeeded(bytes),
                Err(()) => ChildOutcome::Failed("codex_durable_output_invalid"),
            }
        }
        outcome => outcome,
    }
}

fn normalize_captured_image(
    bytes: Vec<u8>,
    output_format: &str,
    output_compression: Option<u8>,
) -> Result<Vec<u8>, ()> {
    // Codex's native image tool can emit PNG even when the Images API caller
    // requested JPEG or WebP. Always use the trusted gateway normalization
    // path so validation, metadata stripping, alpha flattening, compression,
    // and geometry preservation cannot diverge from inline execution.
    let mut images = crate::core::normalize_generated_images(
        vec![crate::core::GeneratedImage { bytes }],
        output_format,
        output_compression,
    )
    .map_err(|_| ())?;
    if images.len() != 1 {
        return Err(());
    }
    Ok(images.remove(0).bytes)
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
        if self.events.completed_image_generation {
            if self.events.stdout_truncated || self.events.malformed_events > 0 {
                self.events.native_handoff =
                    match self.spool.discard_runtime_output(CODEX_RUNTIME_OUTPUT_FILE) {
                        Ok(()) => CodexNativeHandoff::Invalid,
                        Err(ProcessSpoolError::Unavailable) => CodexNativeHandoff::Unavailable,
                        Err(error) => return Err(error),
                    };
                return Ok(());
            }
            self.events.native_handoff = match self
                .spool
                .read_runtime_output(CODEX_RUNTIME_OUTPUT_FILE, MAX_CODEX_RUNTIME_OUTPUT_BYTES)
            {
                Ok(WorkspaceOutputSnapshot::Missing)
                    if !self.events.thread_id_ambiguous && !self.events.image_call_ambiguous =>
                {
                    match (self.events.thread_id, self.events.image_call_id.as_deref()) {
                        (Some(thread_id), Some(call_id)) => {
                            match self.spool.seal_codex_extension_output(
                                &thread_id.to_string(),
                                call_id,
                                CODEX_RUNTIME_OUTPUT_FILE,
                                MAX_CODEX_RUNTIME_OUTPUT_BYTES,
                            ) {
                                Ok(true) => CodexNativeHandoff::Sealed,
                                Ok(false) => CodexNativeHandoff::Missing,
                                Err(ProcessSpoolError::Unavailable) => {
                                    CodexNativeHandoff::Unavailable
                                }
                                Err(
                                    ProcessSpoolError::InvalidInput
                                    | ProcessSpoolError::Conflict
                                    | ProcessSpoolError::Integrity,
                                ) => CodexNativeHandoff::Invalid,
                            }
                        }
                        _ => CodexNativeHandoff::NotAttempted,
                    }
                }
                Ok(WorkspaceOutputSnapshot::Missing) => CodexNativeHandoff::Invalid,
                Ok(WorkspaceOutputSnapshot::Incomplete)
                | Ok(WorkspaceOutputSnapshot::Bytes(_))
                | Err(
                    ProcessSpoolError::InvalidInput
                    | ProcessSpoolError::Conflict
                    | ProcessSpoolError::Integrity,
                ) => match self.spool.discard_runtime_output(CODEX_RUNTIME_OUTPUT_FILE) {
                    Ok(()) => CodexNativeHandoff::Invalid,
                    Err(ProcessSpoolError::Unavailable) => CodexNativeHandoff::Unavailable,
                    Err(error) => return Err(error),
                },
                Err(ProcessSpoolError::Unavailable) => CodexNativeHandoff::Unavailable,
            };
        }
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
        record_codex_thread_id(&event, &mut summary);
        record_codex_image_call_ids(&event, &mut summary);
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

fn record_codex_image_call_ids(value: &serde_json::Value, summary: &mut CodexEventSummary) {
    match value {
        serde_json::Value::Object(fields) => {
            let event_type = fields
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            let candidate = match event_type {
                "image_generation_call" => fields.get("id"),
                "image_generation_begin" | "image_generation_end" => fields.get("call_id"),
                _ => None,
            }
            .and_then(serde_json::Value::as_str);
            if let Some(candidate) = candidate {
                if !valid_codex_image_call_id(candidate) {
                    summary.image_call_id = None;
                    summary.image_call_ambiguous = true;
                } else if let Some(existing) = summary.image_call_id.as_deref() {
                    if existing != candidate {
                        summary.image_call_id = None;
                        summary.image_call_ambiguous = true;
                    }
                } else if !summary.image_call_ambiguous {
                    summary.image_call_id = Some(candidate.to_string());
                }
            }
            for value in fields.values() {
                record_codex_image_call_ids(value, summary);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                record_codex_image_call_ids(value, summary);
            }
        }
        _ => {}
    }
}

fn record_codex_thread_id(value: &serde_json::Value, summary: &mut CodexEventSummary) {
    if value.get("type").and_then(serde_json::Value::as_str) != Some("thread.started") {
        return;
    }
    let Some(candidate) = value
        .get("thread_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
    else {
        return;
    };
    if let Some(existing) = summary.thread_id {
        if existing != candidate {
            summary.thread_id = None;
            summary.thread_id_ambiguous = true;
        }
    } else if !summary.thread_id_ambiguous {
        summary.thread_id = Some(candidate);
    }
}

fn valid_codex_image_call_id(call_id: &str) -> bool {
    !call_id.is_empty()
        && call_id.len() <= 255 - ".png".len()
        && call_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
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
    if matches!(
        error,
        RuntimeError::Output(OutputError::Missing)
            | RuntimeError::Output(OutputError::Unavailable(_))
    ) {
        match events.native_handoff {
            CodexNativeHandoff::Invalid => {
                return ChildOutcome::Failed("codex_image_output_disappeared");
            }
            CodexNativeHandoff::Unavailable => {
                return ChildOutcome::Uncertain("service_unavailable");
            }
            CodexNativeHandoff::NotAttempted
            | CodexNativeHandoff::Sealed
            | CodexNativeHandoff::Missing => {}
        }
    }
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
    use std::collections::HashSet;
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
             {{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"call_exact_image\"}}}}\n\
             not-json\n"
        );

        let summary = summarize_codex_event_stream(stdout.as_bytes(), false, b"warning", true);

        assert_eq!(summary.thread_id, Some(thread_id));
        assert_eq!(summary.image_call_id.as_deref(), Some("call_exact_image"));
        assert!(!summary.image_call_ambiguous);
        assert!(summary.saw_image_generation);
        assert!(summary.completed_image_generation);
        assert_eq!(summary.malformed_events, 1);
        assert!(summary.stderr_present);
        assert!(summary.stderr_truncated);
    }

    #[test]
    fn multiple_native_image_call_ids_fail_closed() {
        let thread_id = Uuid::new_v4();
        let stdout = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{thread_id}\"}}\n\
             {{\"type\":\"item.started\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"call_first\"}}}}\n\
             {{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"call_second\"}}}}\n"
        );

        let summary = summarize_codex_event_stream(stdout.as_bytes(), false, &[], false);

        assert!(summary.image_call_id.is_none());
        assert!(summary.image_call_ambiguous);
    }

    #[test]
    fn multiple_thread_ids_fail_closed() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let stdout = format!(
            "{{\"type\":\"thread.started\",\"thread_id\":\"{first}\"}}\n\
             {{\"type\":\"thread.started\",\"thread_id\":\"{second}\"}}\n\
             {{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"call_exact\"}}}}\n"
        );

        let summary = summarize_codex_event_stream(stdout.as_bytes(), false, &[], false);

        assert!(summary.thread_id.is_none());
        assert!(summary.thread_id_ambiguous);
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
    async fn exact_native_artifact_is_sealed_by_factory_without_polling() {
        let fixture = CodexFixture::exact_native_output();
        let mut command = fixture.command();
        command.output_format = "jpeg".to_string();
        command.output_compression = Some(80);
        let lease = fixture.lease_for_command(&command);
        let context = fixture.context_for_command(&lease, command);
        let expected = normalize_captured_image(
            fs::read(fixture._temp.path().join("source.png")).unwrap(),
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
    async fn truncated_event_stream_never_seals_prefix_native_identity() {
        let fixture = CodexFixture::truncated_event_stream_with_hidden_identity();
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
                error_code: "codex_image_output_disappeared".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("output.bin").exists());
    }

    #[tokio::test]
    async fn malformed_event_suffix_never_seals_prefix_native_identity() {
        let fixture = CodexFixture::malformed_event_suffix_after_native_identity();
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
                error_code: "codex_image_output_disappeared".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("output.bin").exists());
    }

    #[tokio::test]
    async fn agent_written_runtime_output_is_not_a_success_authority() {
        let fixture = CodexFixture::agent_written_output_with_native();
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
                error_code: "codex_image_output_disappeared".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("output.bin").exists());
    }

    #[tokio::test]
    async fn transient_native_file_without_durable_handoff_fails_closed() {
        let fixture = CodexFixture::transient_native_output_only();
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
                error_code: "codex_image_output_disappeared".to_string(),
            }
        );
    }

    #[tokio::test]
    #[ignore = "40-process stress gate; run explicitly to avoid starving unrelated process tests"]
    async fn forty_concurrent_durable_handoffs_are_execution_scoped() {
        const CONCURRENCY: usize = 40;
        let shared_root = TempDir::new().unwrap();
        let shared_journal =
            Arc::new(FilesystemRunnerJournal::new(shared_root.path().join("journal")).unwrap());
        let fixtures = (0..CONCURRENCY)
            .map(|index| {
                let mut fixture = CodexFixture::sealed_output_with_marker_on(
                    Arc::clone(&shared_journal),
                    index as u8,
                );
                fixture.supervisor.request_timeout = Duration::from_secs(30);
                fixture
            })
            .collect::<Vec<_>>();
        let leases = fixtures.iter().map(CodexFixture::lease).collect::<Vec<_>>();
        for (fixture, lease) in fixtures.iter().zip(&leases) {
            fixture.journal.start_or_attach(lease).unwrap();
            fixture
                .supervisor
                .prepare(lease, &fixture.context(lease))
                .await
                .unwrap();
            assert_eq!(
                fixture.journal.commit_launch(lease).unwrap(),
                LaunchDecision::LaunchOnce
            );
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (fixture, lease) in fixtures.iter().zip(&leases) {
            let root = fixture.journal.root_path().to_path_buf();
            let execution_id = lease.executor_execution_id;
            tasks.spawn(async move { run_codex_runner_child(root, execution_id).await });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }

        let expected = fixtures
            .iter()
            .map(|fixture| normalized_fixture_png(&fixture._temp.path().join("source.png")))
            .collect::<Vec<_>>();
        assert_eq!(
            expected
                .iter()
                .map(|bytes| sha256(bytes))
                .collect::<HashSet<_>>()
                .len(),
            CONCURRENCY
        );

        for ((fixture, lease), expected) in fixtures.iter().zip(&leases).zip(expected) {
            let spool = ExecutionSpool::for_lease(&fixture.journal, lease).unwrap();
            assert_eq!(
                spool.observe().unwrap(),
                ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
            );
        }
    }

    #[tokio::test]
    async fn unlinked_partial_handoff_is_never_published() {
        let fixture = CodexFixture::unlinked_partial_output();
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
                error_code: "codex_image_output_disappeared".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("output.bin").exists());
    }

    #[test]
    fn native_png_provider_geometry_is_preserved() {
        let normalized = normalize_captured_image(png_bytes(1659, 948), "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (1659, 948));
    }

    #[test]
    fn native_png_with_matching_format_preserves_provider_dimensions() {
        let normalized = normalize_captured_image(png_bytes(1254, 1254), "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (1254, 1254));
    }

    #[test]
    fn native_png_non_square_dimensions_match_core_normalization() {
        let normalized = normalize_captured_image(png_bytes(1600, 909), "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized).unwrap();

        assert_eq!((decoded.width(), decoded.height()), (1600, 909));
    }

    #[test]
    fn native_same_format_png_still_flattens_alpha() {
        let normalized = normalize_captured_image(png_bytes(8, 8), "png", None).unwrap();
        let decoded = image::load_from_memory(&normalized).unwrap();

        assert!(!decoded.color().has_alpha());
    }

    #[test]
    fn native_same_format_jpeg_strips_metadata_and_applies_compression() {
        let secret = b"provider-metadata-must-not-survive";
        let normalized =
            normalize_captured_image(jpeg_with_comment(secret), "jpeg", Some(65)).unwrap();

        assert!(normalized.starts_with(&[0xff, 0xd8, 0xff]));
        assert!(
            !normalized
                .windows(secret.len())
                .any(|window| window == secret)
        );
    }

    #[tokio::test]
    async fn native_output_is_never_published_after_failed_codex_exit() {
        let fixture = CodexFixture::native_output_on_failed_exit();
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
    async fn preexisting_workspace_output_is_ignored_as_non_authoritative() {
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
        fs::write(
            spool.workspace_path().unwrap().join("provider-output.png"),
            png_bytes(9, 1),
        )
        .unwrap();
        let expected = normalized_fixture_png(&fixture._temp.path().join("source.png"));

        run_codex_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(SupervisedOutput::without_provider_cost(expected))
        );
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n");
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
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_fixture_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\n",
                    invocations.display(),
                    image.display()
                )
            })
        }

        fn slow() -> Self {
            Self::with_script(|invocations, image, root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nprintf 'started\\n' > '{}'\n/bin/sleep 30\nprintf 'completed\\n' > '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_slow_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\n",
                    invocations.display(),
                    root.join("provider-started").display(),
                    root.join("provider-completed").display(),
                    image.display()
                )
            })
        }

        fn native_output_on_failed_exit() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_failed_process_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\nexit 1\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn transient_native_output_only() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_transient_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/rm \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn exact_native_output() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_durable_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn truncated_event_stream_with_hidden_identity() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_visible_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\n/usr/bin/head -c 70000 /dev/zero | /usr/bin/tr '\\000' x\nprintf '\\n{{\"type\":\"thread.started\",\"thread_id\":\"019fd9f5-badb-7dd3-8903-28ffded0ef55\"}}\\n'\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"call_hidden_image\"}}}}\\n'\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn malformed_event_suffix_after_native_identity() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_visible_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":'\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn agent_written_output_with_native() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_untrusted_handoff'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\n/bin/cp '{}' sealed-output.bin\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\n",
                    invocations.display(),
                    image.display(),
                    image.display(),
                )
            })
        }

        fn unlinked_partial_output() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\n/bin/dd if='{}' of=sealed-output.bin.partial bs=1 count=16 2>/dev/null\n/bin/rm sealed-output.bin.partial\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn sealed_output_with_marker_on(journal: Arc<FilesystemRunnerJournal>, marker: u8) -> Self {
            Self::with_script_and_journal(journal, |invocations, image, _root| {
                fs::write(image, png_bytes(u32::from(marker) + 1, 1)).unwrap();
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nthread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\ncall_id='call_concurrent_image'\noutput_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n/bin/mkdir -p \"$output_dir\"\n/bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n/bin/cp '{}' \"$output_dir/$call_id.png\"\n/bin/chmod 600 \"$output_dir/$call_id.png\"\nprintf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\nprintf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\n",
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn with_script(build_script: impl FnOnce(&Path, &Path, &Path) -> String) -> Self {
            let temp = TempDir::new().unwrap();
            let journal =
                Arc::new(FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap());
            Self::with_temp_script_and_journal(temp, journal, build_script)
        }

        fn with_script_and_journal(
            journal: Arc<FilesystemRunnerJournal>,
            build_script: impl FnOnce(&Path, &Path, &Path) -> String,
        ) -> Self {
            Self::with_temp_script_and_journal(TempDir::new().unwrap(), journal, build_script)
        }

        fn with_temp_script_and_journal(
            temp: TempDir,
            journal: Arc<FilesystemRunnerJournal>,
            build_script: impl FnOnce(&Path, &Path, &Path) -> String,
        ) -> Self {
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

    fn jpeg_with_comment(comment: &[u8]) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(8, 8)
            .write_to(&mut bytes, image::ImageFormat::Jpeg)
            .unwrap();
        let encoded = bytes.into_inner();
        let segment_len = u16::try_from(comment.len() + 2).unwrap();
        let mut with_comment = vec![0xff, 0xd8, 0xff, 0xfe];
        with_comment.extend_from_slice(&segment_len.to_be_bytes());
        with_comment.extend_from_slice(comment);
        with_comment.extend_from_slice(&encoded[2..]);
        with_comment
    }

    fn normalized_fixture_png(path: &Path) -> Vec<u8> {
        normalize_captured_image(fs::read(path).unwrap(), "png", None).unwrap()
    }
}
