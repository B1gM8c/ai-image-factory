use std::{
    env, fs,
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use image_cli_runtime::{
    CliRuntime, CommandSpec, ExitClassification, ProcessError, ReceiptCliPolicy, RuntimeError,
    SpawnEvidence, SpawnObserver, VerifiedExecutable, WorkingDirectory,
};
use image_provider_grok_cli::{
    GrokCliPolicyV1, GrokCliReceiptV1, GrokCliRequestV1, GrokInvocationV1, MAX_HISTORY_BYTES,
    parse_invocation_receipt,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{process::Command, time::Instant};
use uuid::Uuid;

use super::grok_request::expected_grok_adapter_revision;
use super::{
    ExecutorLaunchContext, ExecutorSubmissionLease, GrokExecutionRequest, RunnerError,
    SingleOutputSupervisor, SupervisedOutput, private_auth, project_grok_execution_request,
    runner::RunnerLaunchBinding,
};
use crate::{
    ImageGatewayError, ProxyConfig,
    artifacts::media_type_from_bytes,
    input_blobs::InputBlobStore,
    provider_uploads::ProviderUploadService,
    runner::{
        FilesystemRunnerJournal, LaunchDecision,
        process::{
            ExecutionSpool, ProcessObservation, ProcessSpoolError, ProcessTerminal,
            ProviderProcessIdentity, RunnerLock, sha256,
        },
    },
};

const MAX_EXECUTABLE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_IMAGE_ARTIFACT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_VIDEO_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_INPUT_IMAGE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_RUNNER_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const CHILD_REAP_TIMEOUT: Duration = Duration::from_secs(5);

pub struct GrokProcessSupervisor {
    journal: Arc<FilesystemRunnerJournal>,
    helper_executable: PathBuf,
    grok_executable: PathBuf,
    grok_executable_sha256: String,
    credential_auth_file: PathBuf,
    credential_auth_sha256: String,
    credential_resolver: Option<(Uuid, Arc<dyn crate::OperationalCredentialResolver>)>,
    request_timeout: Duration,
    poll_interval: Duration,
    startup_grace: Duration,
    child_env: Vec<(String, String)>,
    input_blobs: Option<Arc<dyn InputBlobStore>>,
    local_video_uploads: Option<Arc<ProviderUploadService>>,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct GrokChildRequest {
    schema_version: u16,
    launch: RunnerLaunchBinding,
    grok_executable: String,
    grok_executable_sha256: String,
    timeout_ms: u64,
    command_json: Value,
    api_profile: String,
}

enum ChildOutcome {
    Succeeded {
        bytes: Vec<u8>,
        provider_reported_cost: Option<image_provider_contracts::ProviderReportedCostEvidenceV1>,
    },
    Failed(&'static str),
    Uncertain(&'static str),
}

struct GrokReceiptPolicy {
    command: CommandSpec,
    invocation: GrokInvocationV1,
    spool: Arc<ExecutionSpool>,
    history_relative_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
enum GrokReceiptPolicyError {
    #[error("Grok history is unavailable")]
    History,
    #[error(transparent)]
    Receipt(#[from] image_provider_grok_cli::GrokReceiptError),
}

struct GrokSpawnObserver {
    spool: Arc<ExecutionSpool>,
    runner_lock: Arc<RunnerLock>,
    helper: crate::runner::process::ProcessIdentity,
}

impl GrokProcessSupervisor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        journal: Arc<FilesystemRunnerJournal>,
        helper_executable: impl AsRef<Path>,
        grok_executable: impl AsRef<Path>,
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
                "Grok executor timeout configuration is invalid",
            ));
        }
        let helper_executable = canonical_executable(helper_executable.as_ref())?;
        let grok_executable = canonical_executable(grok_executable.as_ref())?;
        let grok_executable_sha256 = hash_bounded_file(&grok_executable)?;
        let credential_auth_file = private_auth::validate_auth_source(
            credential_home.as_ref(),
            credential_auth_sha256,
        )
        .map_err(|_| {
            ImageGatewayError::config(
                "EXECUTOR_GROK_CREDENTIAL_HOME/auth.json is invalid or does not match the database credential",
            )
        })?;
        Ok(Self {
            journal,
            helper_executable,
            grok_executable,
            grok_executable_sha256,
            credential_auth_file,
            credential_auth_sha256: credential_auth_sha256.to_owned(),
            credential_resolver: None,
            request_timeout,
            poll_interval,
            startup_grace,
            child_env: child_environment(proxy),
            input_blobs: None,
            local_video_uploads: None,
        })
    }

    pub fn with_input_blobs(mut self, input_blobs: Arc<dyn InputBlobStore>) -> Self {
        self.input_blobs = Some(input_blobs);
        self
    }

    pub fn with_local_video_uploads(
        mut self,
        local_video_uploads: Arc<ProviderUploadService>,
    ) -> Self {
        self.local_video_uploads = Some(local_video_uploads);
        self
    }

    pub fn with_credential_resolver(
        mut self,
        provider_account_id: Uuid,
        resolver: Arc<dyn crate::OperationalCredentialResolver>,
    ) -> Result<Self, ImageGatewayError> {
        if provider_account_id.is_nil() {
            return Err(ImageGatewayError::config(
                "Grok credential resolver account is invalid",
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
        if credential.provider_id != image_provider_grok_cli::PROVIDER_ID
            || credential.provider_account_id != *provider_account_id
            || self.credential_auth_file.parent() != Some(credential.home())
        {
            return Err(RunnerError::Unavailable);
        }
        let source = private_auth::validate_auth_source(
            credential.home(),
            &credential.material_fingerprint_sha256,
        )
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
    ) -> Result<GrokChildRequest, RunnerError> {
        project_grok_execution_request(lease, context).map_err(|_| RunnerError::Definite {
            error_code: "executor_command_rejected".to_owned(),
        })?;
        Ok(GrokChildRequest {
            schema_version: 1,
            launch: RunnerLaunchBinding::from_lease(lease),
            grok_executable: self.grok_executable.to_string_lossy().into_owned(),
            grok_executable_sha256: self.grok_executable_sha256.clone(),
            timeout_ms: self.request_timeout.as_millis() as u64,
            command_json: context.command_json().clone(),
            api_profile: context.api_profile().to_owned(),
        })
    }

    async fn stage_inputs(
        &self,
        request: &GrokExecutionRequest,
        context: &ExecutorLaunchContext,
        spool: &ExecutionSpool,
    ) -> Result<(), RunnerError> {
        let expected_inputs = match request {
            GrokExecutionRequest::ImageGeneration(_) => {
                if context.inputs().is_empty() {
                    return Ok(());
                }
                return Err(RunnerError::Definite {
                    error_code: "grok_input_manifest_invalid".to_owned(),
                });
            }
            GrokExecutionRequest::ImageEdit(request) => request.images().iter().collect(),
            GrokExecutionRequest::VideoGeneration(
                image_provider_grok_cli::GrokVideoGenerationRequestV1::TextToVideo(_),
            ) => {
                if context.inputs().is_empty() {
                    return Ok(());
                }
                return Err(RunnerError::Definite {
                    error_code: "grok_input_manifest_invalid".to_owned(),
                });
            }
            GrokExecutionRequest::VideoGeneration(
                image_provider_grok_cli::GrokVideoGenerationRequestV1::ImageToVideo(request),
            ) => vec![request.image()],
            GrokExecutionRequest::VideoGeneration(
                image_provider_grok_cli::GrokVideoGenerationRequestV1::ReferenceToVideo(request),
            ) => request.images().iter().collect(),
        };
        if expected_inputs.len() != context.inputs().len() {
            return Err(RunnerError::Definite {
                error_code: "grok_input_manifest_invalid".to_owned(),
            });
        }
        let blobs = self.input_blobs.as_ref().ok_or(RunnerError::Definite {
            error_code: "grok_input_store_unavailable".to_owned(),
        })?;
        for (position, (expected, input)) in
            expected_inputs.iter().zip(context.inputs()).enumerate()
        {
            let valid_binding = match request {
                GrokExecutionRequest::ImageEdit(request) => {
                    crate::admission::expected_grok_image_edit_input_binding(
                        request.prompt(),
                        expected_inputs.len(),
                        position,
                        input.media_type(),
                    )
                    .is_some_and(|binding| {
                        input.role() == binding.role.as_str()
                            && input.index() == binding.index
                            && expected.filename() == binding.filename
                    })
                }
                _ => input.role() == "image" && usize::from(input.index()) == position,
            };
            if !valid_binding
                || input.blob().sha256_hex != expected.sha256()
                || input.blob().byte_size == 0
                || input.blob().byte_size > MAX_INPUT_IMAGE_BYTES
            {
                return Err(RunnerError::Definite {
                    error_code: "grok_input_manifest_invalid".to_owned(),
                });
            }
            let bytes = blobs.get(input.blob()).await.map_err(|error| match error {
                crate::input_blobs::InputBlobReadError::Unavailable => RunnerError::Unavailable,
                crate::input_blobs::InputBlobReadError::Integrity => RunnerError::Definite {
                    error_code: "grok_input_integrity_failed".to_owned(),
                },
            })?;
            if bytes.len() as u64 != input.blob().byte_size
                || sha256(&bytes) != expected.sha256()
                || media_type_from_bytes(&bytes).ok() != Some(input.media_type())
            {
                return Err(RunnerError::Definite {
                    error_code: "grok_input_integrity_failed".to_owned(),
                });
            }
            spool
                .stage_provider_input(expected.filename(), &bytes, MAX_INPUT_IMAGE_BYTES)
                .map_err(map_spool_error)?;
        }
        Ok(())
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
            error_code: "runner_spawn_failed".to_owned(),
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
                    error_code: "runner_orphan_cleanup_timeout".to_owned(),
                });
            }
            tokio::time::sleep(self.poll_interval.min(Duration::from_millis(50))).await;
        }
    }
}

pub fn grok_auth_file_sha256(
    credential_home: impl AsRef<Path>,
) -> Result<String, ImageGatewayError> {
    private_auth::auth_file_sha256(credential_home.as_ref()).map_err(|_| {
        ImageGatewayError::config("EXECUTOR_GROK_CREDENTIAL_HOME/auth.json is invalid")
    })
}

#[async_trait]
impl SingleOutputSupervisor for GrokProcessSupervisor {
    async fn prepare(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<(), RunnerError> {
        let (credential_auth_file, credential_auth_sha256, credential_revision) =
            self.credential_source().await?;
        let projected =
            project_grok_execution_request(lease, context).map_err(|_| RunnerError::Definite {
                error_code: "executor_command_rejected".to_owned(),
            })?;
        let request = self.child_request(lease, context)?;
        let bytes = serde_json::to_vec(&request).map_err(|_| RunnerError::Internal)?;
        let spool = ExecutionSpool::for_lease(&self.journal, lease).map_err(map_spool_error)?;
        private_auth::prepare_isolated_auth(
            spool.provider_home_path().map_err(map_spool_error)?,
            &credential_auth_file,
            &credential_auth_sha256,
        )
        .map_err(|_| RunnerError::Unavailable)?;
        let provider_home = spool.provider_home_path().map_err(map_spool_error)?;
        let has_managed_video_output = private_auth::prepare_isolated_grok_config(
            provider_home,
            credential_auth_file
                .parent()
                .ok_or(RunnerError::Unavailable)?,
        )
        .map_err(|_| RunnerError::Unavailable)?;
        if matches!(projected, GrokExecutionRequest::VideoGeneration(_))
            && !has_managed_video_output
        {
            let configuration = self
                .local_video_uploads
                .as_ref()
                .ok_or(RunnerError::Definite {
                    error_code: "grok_video_output_upload_url_required".to_owned(),
                })?
                .issue_grok_video_output(lease, self.request_timeout)
                .map_err(|_| RunnerError::Unavailable)?
                .ok_or(RunnerError::Definite {
                    error_code: "grok_video_output_upload_url_required".to_owned(),
                })?;
            private_auth::prepare_isolated_grok_fallback_config(provider_home, &configuration)
                .map_err(|_| RunnerError::Unavailable)?;
        }
        tracing::debug!(
            execution.profile.id = %lease.execution_profile_id,
            credential.revision = credential_revision,
            "resolved Grok operational credential"
        );
        self.stage_inputs(&projected, context, &spool).await?;
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
                        error_code: "runner_process_lost".to_owned(),
                    });
                }
                ProcessObservation::AwaitingProcess if started.elapsed() >= self.startup_grace => {
                    return Err(RunnerError::Unknown {
                        error_code: "runner_process_missing".to_owned(),
                    });
                }
                ProcessObservation::AwaitingProcess | ProcessObservation::Running(_) => {}
            }
            if started.elapsed() >= supervision_timeout {
                return Err(RunnerError::Unknown {
                    error_code: "runner_supervision_timeout".to_owned(),
                });
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }
}

pub async fn run_grok_runner_child(
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
            serde_json::from_slice::<GrokChildRequest>(&bytes).map_err(|_| {
                ImageGatewayError::service_unavailable("Grok runner request is invalid")
            })
        })?;
    let (lease, _) = validate_child_request(&request, executor_execution_id)?;
    FilesystemRunnerJournal::new(runner_root)
        .and_then(|journal| journal.verify_launch_committed(&lease))
        .map_err(|_| {
            ImageGatewayError::service_unavailable("Grok launch authority is unavailable")
        })?;
    let runner_lock = Arc::new(spool.acquire_runner_lock().map_err(child_spool_error)?);
    let identity = runner_lock.identity().map_err(child_spool_error)?;
    spool
        .publish_process(&runner_lock, &identity)
        .map_err(child_spool_error)?;
    let outcome = run_grok_child(
        Arc::clone(&spool),
        Arc::clone(&runner_lock),
        identity.clone(),
        executor_execution_id,
    )
    .await;
    match outcome {
        ChildOutcome::Succeeded {
            bytes,
            provider_reported_cost,
        } => {
            spool.publish_output(&bytes).map_err(child_spool_error)?;
            if spool.cleanup_provider_runtime().is_err() {
                spool
                    .publish_terminal(
                        &runner_lock,
                        &ProcessTerminal::Uncertain {
                            helper_nonce: identity.nonce.clone(),
                            error_code: "grok_local_cleanup_failed".to_owned(),
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
                        provider_reported_cost,
                    },
                )
                .map_err(child_spool_error)?;
        }
        ChildOutcome::Failed(error_code) => {
            let cleanup_failed = spool.cleanup_provider_runtime().is_err();
            let terminal = if cleanup_failed {
                ProcessTerminal::Uncertain {
                    helper_nonce: identity.nonce.clone(),
                    error_code: "grok_local_cleanup_failed".to_owned(),
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
            let cleanup_failed = spool.cleanup_provider_runtime().is_err();
            spool
                .publish_terminal(
                    &runner_lock,
                    &ProcessTerminal::Uncertain {
                        helper_nonce: identity.nonce.clone(),
                        error_code: if cleanup_failed {
                            "grok_local_cleanup_failed".to_owned()
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

async fn run_grok_child(
    spool: Arc<ExecutionSpool>,
    runner_lock: Arc<RunnerLock>,
    helper: crate::runner::process::ProcessIdentity,
    executor_execution_id: Uuid,
) -> ChildOutcome {
    let request = match spool.read_request().and_then(|bytes| {
        serde_json::from_slice::<GrokChildRequest>(&bytes).map_err(|_| ProcessSpoolError::Integrity)
    }) {
        Ok(request) => request,
        Err(_) => return ChildOutcome::Uncertain("runner_request_invalid"),
    };
    let cli_request = match validate_child_request(&request, executor_execution_id) {
        Ok((_, request)) => request,
        Err(_) => return ChildOutcome::Uncertain("runner_request_invalid"),
    };
    let workspace = match spool.provider_attempt_path() {
        Ok(path) => path,
        Err(_) => return ChildOutcome::Uncertain("runner_workspace_invalid"),
    };
    let workspace_root = match spool.provider_workspaces_path() {
        Ok(path) => path,
        Err(_) => return ChildOutcome::Uncertain("runner_workspace_invalid"),
    };
    let policy = match build_policy(&request, workspace_root, workspace, &spool) {
        Ok(policy) => policy,
        Err(_) => return ChildOutcome::Uncertain("grok_runtime_policy_invalid"),
    };
    let (command, invocation) = match policy.command_spec_in(
        &cli_request,
        &executor_execution_id.to_string(),
        match WorkingDirectory::new_private(workspace) {
            Ok(workspace) => workspace,
            Err(_) => return ChildOutcome::Uncertain("runner_workspace_invalid"),
        },
    ) {
        Ok(prepared) => prepared,
        Err(_) => return ChildOutcome::Uncertain("grok_runtime_policy_invalid"),
    };
    let artifact_limit = match cli_request {
        GrokCliRequestV1::VideoGeneration(_) => MAX_VIDEO_ARTIFACT_BYTES,
        GrokCliRequestV1::ImageGeneration(_) | GrokCliRequestV1::ImageEdit(_) => {
            MAX_IMAGE_ARTIFACT_BYTES
        }
    };
    let artifact_path = invocation.artifact_path().to_path_buf();
    let provider_home = match spool.provider_home_path() {
        Ok(path) => path,
        Err(_) => return ChildOutcome::Uncertain("grok_provider_home_invalid"),
    };
    let history_relative_path = match invocation.history_path().strip_prefix(provider_home) {
        Ok(path) => path.to_path_buf(),
        Err(_) => return ChildOutcome::Uncertain("grok_history_path_invalid"),
    };
    let artifact_relative_path = match artifact_path.strip_prefix(provider_home) {
        Ok(path) => path.to_path_buf(),
        Err(_) => return ChildOutcome::Uncertain("grok_artifact_path_invalid"),
    };
    let mut observer = GrokSpawnObserver {
        spool: Arc::clone(&spool),
        runner_lock: Arc::clone(&runner_lock),
        helper,
    };
    let runtime = CliRuntime::new(GrokReceiptPolicy {
        command,
        invocation,
        spool: Arc::clone(&spool),
        history_relative_path,
    });
    match runtime.run_receipt(&(), &mut observer).await {
        Ok(success) => {
            if success.receipt.artifact_path() != artifact_path {
                return ChildOutcome::Uncertain("grok_artifact_invalid");
            }
            let provider_reported_cost = success.receipt.provider_reported_cost().cloned();
            match spool
                .open_provider_file(&artifact_relative_path)
                .map_err(|_| ())
                .and_then(|file| read_bounded_regular_file(file, artifact_limit).map_err(|_| ()))
            {
                Ok(bytes) => ChildOutcome::Succeeded {
                    bytes,
                    provider_reported_cost,
                },
                Err(()) => ChildOutcome::Uncertain("grok_artifact_invalid"),
            }
        }
        Err(error) => map_cli_runtime_error(error),
    }
}

fn build_policy(
    request: &GrokChildRequest,
    workspace_root: &Path,
    workspace: &Path,
    spool: &ExecutionSpool,
) -> Result<GrokCliPolicyV1, ImageGatewayError> {
    let executable_sha256 = parse_sha256(&request.grok_executable_sha256)?;
    GrokCliPolicyV1::new(
        &request.grok_executable,
        executable_sha256,
        WorkingDirectory::new_private(workspace_root).map_err(|_| {
            ImageGatewayError::service_unavailable("Grok workspace root is invalid")
        })?,
        WorkingDirectory::new_private(spool.runtime_home_path().map_err(child_spool_error)?)
            .map_err(|_| ImageGatewayError::service_unavailable("Grok runtime home is invalid"))?,
        WorkingDirectory::new_private(spool.provider_home_path().map_err(child_spool_error)?)
            .map_err(|_| ImageGatewayError::service_unavailable("Grok provider home is invalid"))?,
        Duration::from_millis(request.timeout_ms),
        CHILD_REAP_TIMEOUT,
    )
    .map_err(|_| ImageGatewayError::service_unavailable("Grok CLI policy is invalid"))
    .and_then(|policy| {
        if workspace.parent() == Some(workspace_root) {
            Ok(policy)
        } else {
            Err(ImageGatewayError::service_unavailable(
                "Grok workspace binding is invalid",
            ))
        }
    })
}

impl ReceiptCliPolicy for GrokReceiptPolicy {
    type Request = ();
    type Receipt = GrokCliReceiptV1;
    type Error = GrokReceiptPolicyError;

    fn command(&self, _request: &Self::Request) -> Result<CommandSpec, Self::Error> {
        Ok(self.command.clone())
    }

    fn classify_exit(&self, status: &std::process::ExitStatus) -> ExitClassification {
        if status.success() {
            ExitClassification::Success
        } else {
            ExitClassification::Failed {
                code: "grok_cli_failed".to_owned(),
            }
        }
    }

    fn parse_receipt(&self, stdout: &[u8]) -> Result<Self::Receipt, Self::Error> {
        let history_file = self
            .spool
            .open_provider_file(&self.history_relative_path)
            .map_err(|_| GrokReceiptPolicyError::History)?;
        let history = read_bounded_regular_file(history_file, MAX_HISTORY_BYTES as u64)
            .map_err(|_| GrokReceiptPolicyError::History)?;
        parse_invocation_receipt(stdout, &history, &self.invocation).map_err(Into::into)
    }
}

impl SpawnObserver for GrokSpawnObserver {
    type Error = ProcessSpoolError;

    fn observe_spawn(&mut self, evidence: &SpawnEvidence) -> Result<(), Self::Error> {
        ProviderProcessIdentity::capture(evidence.pid, &self.helper.nonce).and_then(|provider| {
            self.spool
                .publish_provider_process(&self.runner_lock, &self.helper, &provider)
        })
    }
}

fn validate_child_request(
    request: &GrokChildRequest,
    executor_execution_id: Uuid,
) -> Result<(ExecutorSubmissionLease, GrokCliRequestV1), ImageGatewayError> {
    let lease = request.launch.to_lease().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Grok runner lease binding is invalid")
    })?;
    if request.schema_version != 1
        || Some(lease.adapter_revision.as_str())
            != expected_grok_adapter_revision(lease.command_schema.as_str())
        || lease.executor_execution_id != executor_execution_id
        || request.timeout_ms == 0
        || request.timeout_ms > MAX_RUNNER_TIMEOUT.as_millis() as u64
        || lease.provider_id != image_provider_grok_cli::PROVIDER_ID
        || lease.output_index != 0
    {
        return Err(ImageGatewayError::service_unavailable(
            "Grok runner request is invalid",
        ));
    }
    let command_bytes = serde_json::to_vec(&request.command_json)
        .map_err(|_| ImageGatewayError::service_unavailable("Grok runner request is invalid"))?;
    if hex::encode(Sha256::digest(&command_bytes)) != lease.command_hash {
        return Err(ImageGatewayError::service_unavailable(
            "Grok runner request digest is invalid",
        ));
    }
    let context = ExecutorLaunchContext::new(
        "grok-runner-child",
        request.api_profile.clone(),
        lease.output_index,
        lease.command_schema.clone(),
        lease.command_hash.clone(),
        request.command_json.clone(),
    )
    .ok_or_else(|| ImageGatewayError::service_unavailable("Grok runner request is invalid"))?;
    let media_request = project_grok_execution_request(&lease, &context)
        .map_err(|_| ImageGatewayError::service_unavailable("Grok runner request is invalid"))?;
    let executable_sha256 = parse_sha256(&request.grok_executable_sha256)?;
    let executable =
        VerifiedExecutable::new_with_sha256(Path::new(&request.grok_executable), executable_sha256)
            .map_err(|_| {
                ImageGatewayError::service_unavailable("Grok executable identity changed")
            })?;
    if executable.path().to_string_lossy() != request.grok_executable {
        return Err(ImageGatewayError::service_unavailable(
            "Grok executable identity changed",
        ));
    }
    Ok((lease, media_request.into_cli_request()))
}

fn map_cli_runtime_error(error: RuntimeError) -> ChildOutcome {
    match error {
        RuntimeError::Process(ProcessError::Spawn(_)) => {
            ChildOutcome::Failed("grok_cli_unavailable")
        }
        RuntimeError::Policy(_) | RuntimeError::UnexpectedOutputContract => {
            ChildOutcome::Failed("grok_cli_policy_rejected")
        }
        RuntimeError::ProcessFailed { .. } => ChildOutcome::Uncertain("grok_cli_failed"),
        RuntimeError::Process(ProcessError::TimedOut { .. }) => {
            ChildOutcome::Uncertain("grok_cli_timeout")
        }
        RuntimeError::Receipt(message) => {
            let error_code = grok_receipt_error_code(&message);
            if matches!(
                error_code,
                "grok_video_output_upload_url_required" | "grok_tool_execution_failed"
            ) {
                ChildOutcome::Failed(error_code)
            } else {
                ChildOutcome::Uncertain(error_code)
            }
        }
        RuntimeError::CapturedOutputTooLarge { .. } => {
            ChildOutcome::Uncertain("grok_receipt_too_large")
        }
        RuntimeError::Process(ProcessError::Observer { .. })
        | RuntimeError::Process(ProcessError::IdentityUnavailable) => {
            ChildOutcome::Uncertain("grok_process_identity_unavailable")
        }
        RuntimeError::Process(ProcessError::Stdin { .. }) => {
            ChildOutcome::Uncertain("grok_stdin_failed")
        }
        RuntimeError::Process(ProcessError::InvalidCommand(_)) => {
            ChildOutcome::Uncertain("grok_command_changed")
        }
        RuntimeError::Process(ProcessError::Capture { .. }) => {
            ChildOutcome::Uncertain("grok_capture_failed")
        }
        RuntimeError::Process(ProcessError::ResidualProcessGroup { .. }) => {
            ChildOutcome::Uncertain("grok_process_group_residual")
        }
        RuntimeError::Process(ProcessError::Wait { .. }) => {
            ChildOutcome::Uncertain("grok_process_wait_failed")
        }
        RuntimeError::MissingOutputContract
        | RuntimeError::Output(_)
        | RuntimeError::OutputTask(_) => ChildOutcome::Uncertain("grok_runtime_failed"),
    }
}

fn grok_receipt_error_code(message: &str) -> &'static str {
    match message {
        "Grok history is unavailable" => "grok_history_unavailable",
        "Grok stdout is empty or exceeds the bounded receipt limit" => {
            "grok_receipt_stdout_invalid"
        }
        "Grok history is empty or exceeds the bounded history limit" => {
            "grok_receipt_history_invalid"
        }
        "Grok streaming output contains invalid JSON" => "grok_receipt_stream_invalid",
        "Grok streaming output must end with exactly one end event" => {
            "grok_receipt_terminal_missing"
        }
        "Grok terminal event does not match the expected session" => {
            "grok_receipt_session_mismatch"
        }
        "Grok terminal event contains invalid identifiers" => "grok_receipt_terminal_invalid",
        "Grok history contains invalid JSON" => "grok_receipt_history_json_invalid",
        "Grok history contains an unexpected tool call" => "grok_receipt_tool_unexpected",
        "Grok history must contain exactly one expected tool call and result" => {
            "grok_receipt_tool_result_missing"
        }
        "Grok tool arguments differ from the admitted request" => {
            "grok_receipt_tool_arguments_mismatch"
        }
        "Grok video generation requires output.upload_url for a Zero Data Retention team" => {
            "grok_video_output_upload_url_required"
        }
        "Grok tool execution failed" => "grok_tool_execution_failed",
        "Grok tool result is invalid" => "grok_receipt_tool_result_invalid",
        "Grok tool result points outside the expected session artifact path" => {
            "grok_receipt_artifact_path_mismatch"
        }
        message if message.starts_with("Grok streaming output reported an error:") => {
            "grok_receipt_cli_error"
        }
        _ => "grok_receipt_invalid",
    }
}

fn read_bounded_regular_file(mut file: fs::File, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    let before = file.metadata()?;
    if !before.is_file()
        || before.nlink() != 1
        || before.uid() != unsafe { libc::geteuid() }
        || before.permissions().mode() & 0o022 != 0
        || before.len() == 0
        || before.len() > max_bytes
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bounded file validation failed",
        ));
    }
    let mut bytes = Vec::with_capacity(before.len() as usize);
    Read::by_ref(&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if bytes.len() as u64 != before.len()
        || before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bounded file changed while reading",
        ));
    }
    Ok(bytes)
}

fn canonical_executable(path: &Path) -> Result<PathBuf, ImageGatewayError> {
    VerifiedExecutable::new(path)
        .map(|executable| executable.path().to_path_buf())
        .map_err(|_| ImageGatewayError::config("executor executable is invalid"))
}

fn hash_bounded_file(path: &Path) -> Result<String, ImageGatewayError> {
    let mut file = fs::File::open(path)
        .map_err(|_| ImageGatewayError::config("Grok executable is unreadable"))?;
    let size = file
        .metadata()
        .map_err(|_| ImageGatewayError::config("Grok executable is unreadable"))?
        .len();
    if size == 0 || size > MAX_EXECUTABLE_BYTES {
        return Err(ImageGatewayError::config("Grok executable size is invalid"));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| ImageGatewayError::config("Grok executable is unreadable"))?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(read as u64);
        if total > MAX_EXECUTABLE_BYTES {
            return Err(ImageGatewayError::config("Grok executable size is invalid"));
        }
        digest.update(&buffer[..read]);
    }
    if total != size {
        return Err(ImageGatewayError::config(
            "Grok executable changed while hashing",
        ));
    }
    Ok(hex::encode(digest.finalize()))
}

fn parse_sha256(value: &str) -> Result<[u8; 32], ImageGatewayError> {
    let bytes = hex::decode(value)
        .map_err(|_| ImageGatewayError::service_unavailable("Grok executable digest is invalid"))?;
    bytes
        .try_into()
        .map_err(|_| ImageGatewayError::service_unavailable("Grok executable digest is invalid"))
}

fn child_environment(proxy: &ProxyConfig) -> Vec<(String, String)> {
    let mut values = Vec::new();
    for name in ["LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR"] {
        if let Ok(value) = env::var(name) {
            values.push((name.to_owned(), value));
        }
    }
    for (name, value) in [
        ("HTTP_PROXY", proxy.http_proxy.as_ref()),
        ("HTTPS_PROXY", proxy.https_proxy.as_ref()),
        ("ALL_PROXY", proxy.all_proxy.as_ref()),
        ("NO_PROXY", proxy.no_proxy.as_ref()),
    ] {
        if let Some(value) = value {
            values.push((name.to_owned(), value.clone()));
            values.push((name.to_ascii_lowercase(), value.clone()));
        }
    }
    values
}

fn map_spool_error(error: ProcessSpoolError) -> RunnerError {
    let error_code = match error {
        ProcessSpoolError::InvalidInput => "runner_spool_invalid",
        ProcessSpoolError::Conflict => "runner_spool_conflict",
        ProcessSpoolError::Integrity => "runner_spool_integrity",
        ProcessSpoolError::Unavailable => "runner_spool_unavailable",
    };
    RunnerError::Unknown {
        error_code: error_code.to_owned(),
    }
}

fn child_spool_error(_error: ProcessSpoolError) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Grok runner spool is unavailable")
}

#[cfg(test)]
mod live_smoke;

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use image_api_contracts::xai::{
        XaiImageAspectRatio, XaiImageGenerationCommandV1, XaiImageGenerationRequest,
        XaiImageResolution, XaiImageResponseFormat, XaiVideoGenerationCommandV1,
        XaiVideoGenerationRequest, XaiVideoImageUrl, XaiVideoResolution as OfficialVideoResolution,
    };
    use image_provider_grok_cli::{
        GROK_IMAGE_EDIT_COMMAND_SCHEMA, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
        GrokImageEditPayloadV1, GrokImageEditRequestV1, GrokImageGenerationPayloadV1,
        GrokVideoGenerationPayloadV1, ImageAspectRatio, StagedImageV1,
    };
    use image_provider_sdk::{CanonicalCommandPayload, OutputSlot};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        artifacts::InMemoryArtifactBlobStore,
        executor::{ExecutorInputObject, XAI_IMAGES_API_PROFILE, XAI_VIDEOS_API_PROFILE},
        input_blobs::{InputBlobKey, InputBlobStore},
    };

    #[test]
    fn receipt_errors_keep_a_stable_diagnostic_category() {
        assert_eq!(
            grok_receipt_error_code("Grok tool result is invalid"),
            "grok_receipt_tool_result_invalid"
        );
        assert_eq!(
            grok_receipt_error_code(
                "Grok video generation requires output.upload_url for a Zero Data Retention team"
            ),
            "grok_video_output_upload_url_required"
        );
        assert!(matches!(
            map_cli_runtime_error(RuntimeError::Receipt(
                "Grok video generation requires output.upload_url for a Zero Data Retention team"
                    .to_owned()
            )),
            ChildOutcome::Failed("grok_video_output_upload_url_required")
        ));
        assert_eq!(
            grok_receipt_error_code(
                "Grok streaming output reported an error: upstream unavailable"
            ),
            "grok_receipt_cli_error"
        );
        assert_eq!(
            grok_receipt_error_code("unrecognized receipt failure"),
            "grok_receipt_invalid"
        );
    }

    #[tokio::test]
    async fn helper_attaches_and_replays_without_a_second_grok_launch() {
        let fixture = GrokFixture::new();
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
            run_grok_runner_child(root, execution_id).await.unwrap();
        });

        let first = fixture
            .supervisor
            .start_or_attach(&lease, LaunchDecision::Attach)
            .await
            .unwrap();
        child.await.unwrap();
        let replay = fixture
            .supervisor
            .start_or_attach(&lease, LaunchDecision::Attach)
            .await
            .unwrap();

        assert_eq!(first, replay);
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n");
    }

    #[tokio::test]
    async fn helper_rejects_direct_execution_before_launch_commit() {
        let fixture = GrokFixture::new();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();

        assert!(
            run_grok_runner_child(fixture.journal.root_path(), lease.executor_execution_id,)
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

    #[tokio::test]
    async fn helper_rejects_hardlinked_artifact_before_spooling_bytes() {
        let fixture = GrokFixture::hardlinked_artifact();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        fixture.journal.commit_launch(&lease).unwrap();

        run_grok_runner_child(fixture.journal.root_path(), lease.executor_execution_id)
            .await
            .unwrap();
        assert_eq!(
            fixture
                .supervisor
                .start_or_attach(&lease, LaunchDecision::Attach)
                .await,
            Err(RunnerError::Unknown {
                error_code: "grok_artifact_invalid".to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn supervisor_stages_only_digest_bound_input_media() {
        let fixture = GrokFixture::new();
        let mut image_bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(2, 2)
            .write_to(&mut image_bytes, image::ImageFormat::Jpeg)
            .unwrap();
        let image_bytes = image_bytes.into_inner();
        let blob = fixture
            .input_blobs
            .put(
                InputBlobKey {
                    admission_session_id: Uuid::new_v4(),
                    input_id: Uuid::new_v4(),
                },
                &image_bytes,
            )
            .await
            .unwrap();
        let image = StagedImageV1::new("input.jpg", &blob.sha256_hex).unwrap();
        let source = XaiVideoGenerationCommandV1::from_request(XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/jpeg;base64,AA==".to_owned()),
            }),
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: None,
            reference_images: Vec::new(),
            resolution: Some(OfficialVideoResolution::P480),
            storage_options: None,
            user: None,
        })
        .unwrap();
        let payload = GrokVideoGenerationPayloadV1::from_xai_command(source, vec![image]).unwrap();
        let command = serde_json::from_slice::<Value>(
            &payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap()),
        )
        .unwrap();
        let hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
        let mut lease = fixture.lease();
        lease.model = "grok-imagine-video-1.5-preview".to_owned();
        lease.command_schema =
            image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA.to_owned();
        lease.adapter_revision = image_provider_grok_cli::VIDEO_ADAPTER_REVISION.to_owned();
        lease.command_hash.clone_from(&hash);
        let context = ExecutorLaunchContext {
            request_id: "request-video".to_owned(),
            api_profile: XAI_VIDEOS_API_PROFILE.to_owned(),
            output_index: 0,
            command_schema: lease.command_schema.clone(),
            command_hash: hash,
            command_json: command,
            inputs: vec![ExecutorInputObject::new(blob, "image", 0, "image/jpeg").unwrap()],
        };

        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        assert_eq!(
            fs::read(spool.provider_attempt_path().unwrap().join("input.jpg")).unwrap(),
            image_bytes
        );
        let projected =
            fs::read_to_string(spool.provider_home_path().unwrap().join("config.toml")).unwrap();
        assert!(projected.contains("[tools.zdr_video_output_s3]"));
        assert!(
            projected
                .contains("endpoint = \"http://127.0.0.1:8787/v1/internal/provider-uploads/s3/\"")
        );
        assert!(projected.contains("[tools.zdr_video_output_s3.read_write]"));
    }

    #[tokio::test]
    async fn supervisor_accepts_text_video_without_staging_a_first_frame() {
        let fixture = GrokFixture::new();
        let source = XaiVideoGenerationCommandV1::from_request(XaiVideoGenerationRequest {
            aspect_ratio: Some(image_api_contracts::xai::XaiVideoAspectRatio::R9x16),
            duration: Some(6),
            image: None,
            model: Some("grok-imagine-video-1.5-preview".to_owned()),
            output: None,
            prompt: Some("a paper boat crossing a moonlit lake".to_owned()),
            reference_images: Vec::new(),
            resolution: Some(OfficialVideoResolution::P480),
            storage_options: None,
            user: None,
        })
        .unwrap();
        let payload = GrokVideoGenerationPayloadV1::from_xai_command(source, Vec::new()).unwrap();
        let command = serde_json::from_slice::<Value>(
            &payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap()),
        )
        .unwrap();
        let hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
        let mut lease = fixture.lease();
        lease.model = "grok-imagine-video-1.5-preview".to_owned();
        lease.command_schema =
            image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA.to_owned();
        lease.adapter_revision = image_provider_grok_cli::VIDEO_ADAPTER_REVISION.to_owned();
        lease.command_hash.clone_from(&hash);
        let context = ExecutorLaunchContext {
            request_id: "request-text-video".to_owned(),
            api_profile: XAI_VIDEOS_API_PROFILE.to_owned(),
            output_index: 0,
            command_schema: lease.command_schema.clone(),
            command_hash: hash,
            command_json: command,
            inputs: Vec::new(),
        };

        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();

        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        assert!(spool.provider_attempt_path().unwrap().is_dir());
        assert!(
            !spool
                .provider_attempt_path()
                .unwrap()
                .join("input.jpg")
                .exists()
        );
    }

    #[tokio::test]
    async fn supervisor_stages_every_digest_bound_image_edit_reference() {
        let fixture = GrokFixture::new();
        let mut staged = Vec::new();
        let mut inputs = Vec::new();
        let mut expected_bytes = Vec::new();

        for index in 0..2_u16 {
            let mut image_bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgb8(u32::from(index) + 2, 2)
                .write_to(&mut image_bytes, image::ImageFormat::Png)
                .unwrap();
            let image_bytes = image_bytes.into_inner();
            let blob = fixture
                .input_blobs
                .put(
                    InputBlobKey {
                        admission_session_id: Uuid::new_v4(),
                        input_id: Uuid::new_v4(),
                    },
                    &image_bytes,
                )
                .await
                .unwrap();
            staged
                .push(StagedImageV1::new(format!("image-{index}.png"), &blob.sha256_hex).unwrap());
            inputs.push(ExecutorInputObject::new(blob, "image", index, "image/png").unwrap());
            expected_bytes.push(image_bytes);
        }

        let request = GrokImageEditRequestV1::new(
            "keep the subject and change the background",
            staged,
            ImageAspectRatio::R16x9,
        )
        .unwrap();
        let payload = GrokImageEditPayloadV1::new("a".repeat(64), request).unwrap();
        let command = serde_json::from_slice::<Value>(
            &payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap()),
        )
        .unwrap();
        let hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
        let mut lease = fixture.lease();
        lease.command_schema = GROK_IMAGE_EDIT_COMMAND_SCHEMA.to_owned();
        lease.command_hash.clone_from(&hash);
        let context = ExecutorLaunchContext {
            request_id: "request-edit".to_owned(),
            api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
            output_index: 0,
            command_schema: lease.command_schema.clone(),
            command_hash: hash,
            command_json: command,
            inputs,
        };

        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        for (index, bytes) in expected_bytes.iter().enumerate() {
            assert_eq!(
                fs::read(
                    spool
                        .provider_attempt_path()
                        .unwrap()
                        .join(format!("image-{index}.png"))
                )
                .unwrap(),
                *bytes
            );
        }
    }

    #[tokio::test]
    async fn supervisor_stages_opted_in_semantic_mask_as_mask_png() {
        let fixture = GrokFixture::new();
        let mut staged = Vec::new();
        let mut inputs = Vec::new();
        let mut expected_files = Vec::new();

        for (position, (role, index, filename)) in [
            ("image", 0_u16, "image-0.png"),
            ("image", 1_u16, "image-1.png"),
            ("mask", 0_u16, "mask.png"),
        ]
        .into_iter()
        .enumerate()
        {
            let mut image_bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgba8(position as u32 + 2, 2)
                .write_to(&mut image_bytes, image::ImageFormat::Png)
                .unwrap();
            let image_bytes = image_bytes.into_inner();
            let blob = fixture
                .input_blobs
                .put(
                    InputBlobKey {
                        admission_session_id: Uuid::new_v4(),
                        input_id: Uuid::new_v4(),
                    },
                    &image_bytes,
                )
                .await
                .unwrap();
            staged.push(StagedImageV1::new(filename, &blob.sha256_hex).unwrap());
            inputs.push(ExecutorInputObject::new(blob, role, index, "image/png").unwrap());
            expected_files.push((filename, image_bytes));
        }

        let request = GrokImageEditRequestV1::new(
            "replace the selection\n[factory-spatial-edit:semantic-mask-v1]",
            staged,
            ImageAspectRatio::R16x9,
        )
        .unwrap();
        let payload = GrokImageEditPayloadV1::new("a".repeat(64), request).unwrap();
        let command = serde_json::from_slice::<Value>(
            &payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap()),
        )
        .unwrap();
        let hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
        let mut lease = fixture.lease();
        lease.command_schema = GROK_IMAGE_EDIT_COMMAND_SCHEMA.to_owned();
        lease.command_hash.clone_from(&hash);
        let context = ExecutorLaunchContext {
            request_id: "request-semantic-mask-edit".to_owned(),
            api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
            output_index: 0,
            command_schema: lease.command_schema.clone(),
            command_hash: hash,
            command_json: command,
            inputs,
        };

        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        for (filename, bytes) in expected_files {
            assert_eq!(
                fs::read(spool.provider_attempt_path().unwrap().join(filename)).unwrap(),
                bytes
            );
        }
    }

    struct GrokFixture {
        _temp: TempDir,
        credentials: PathBuf,
        journal: Arc<FilesystemRunnerJournal>,
        supervisor: GrokProcessSupervisor,
        input_blobs: Arc<InMemoryArtifactBlobStore>,
        invocations: PathBuf,
        command: Value,
        command_hash: String,
    }

    impl GrokFixture {
        fn new() -> Self {
            Self::with_artifact_hardlink(false)
        }

        fn hardlinked_artifact() -> Self {
            Self::with_artifact_hardlink(true)
        }

        fn with_artifact_hardlink(hardlink_artifact: bool) -> Self {
            let temp = TempDir::new().unwrap();
            let journal =
                Arc::new(FilesystemRunnerJournal::new(temp.path().join("journal")).unwrap());
            let credentials = temp.path().join("credentials");
            fs::create_dir(&credentials).unwrap();
            fs::set_permissions(&credentials, fs::Permissions::from_mode(0o700)).unwrap();
            let auth = credentials.join(private_auth::AUTH_FILE);
            fs::write(&auth, b"{}").unwrap();
            fs::set_permissions(&auth, fs::Permissions::from_mode(0o600)).unwrap();

            let image = temp.path().join("source.jpg");
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgb8(1, 1)
                .write_to(&mut bytes, image::ImageFormat::Jpeg)
                .unwrap();
            fs::write(&image, bytes.into_inner()).unwrap();
            let invocations = temp.path().join("invocations");
            let executable = temp.path().join("fake-grok");
            fs::write(
                &executable,
                fake_grok_script(&invocations, &image, hardlink_artifact),
            )
            .unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();

            let source = XaiImageGenerationCommandV1::from_request(XaiImageGenerationRequest {
                aspect_ratio: Some(XaiImageAspectRatio::R1x1),
                model: Some("grok-imagine-image-quality".to_owned()),
                n: Some(1),
                prompt: "draw a lighthouse".to_owned(),
                resolution: Some(XaiImageResolution::R1k),
                response_format: Some(XaiImageResponseFormat::B64Json),
                storage_options: None,
                user: None,
            })
            .unwrap();
            let payload = GrokImageGenerationPayloadV1::from_xai_command(source).unwrap();
            let command = serde_json::from_slice::<Value>(
                &payload.into_canonical_bytes(OutputSlot::new(0, 1).unwrap()),
            )
            .unwrap();
            let command_hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
            let input_blobs = Arc::new(InMemoryArtifactBlobStore::default());
            let provider_artifacts = temp.path().join("provider-artifacts");
            fs::create_dir(&provider_artifacts).unwrap();
            let local_video_uploads = Arc::new(
                ProviderUploadService::new(&provider_artifacts, Some("http://127.0.0.1:8787"))
                    .unwrap(),
            );
            let supervisor = GrokProcessSupervisor::new(
                Arc::clone(&journal),
                &executable,
                &executable,
                &credentials,
                &sha256(b"{}"),
                Duration::from_secs(5),
                Duration::from_millis(10),
                Duration::from_secs(1),
                &ProxyConfig::default(),
            )
            .unwrap()
            .with_input_blobs(input_blobs.clone())
            .with_local_video_uploads(local_video_uploads);
            Self {
                _temp: temp,
                credentials,
                journal,
                supervisor,
                input_blobs,
                invocations,
                command,
                command_hash,
            }
        }

        fn lease(&self) -> ExecutorSubmissionLease {
            ExecutorSubmissionLease {
                submission_id: Uuid::new_v4(),
                executor_execution_id: Uuid::new_v4(),
                output_id: Uuid::new_v4(),
                job_id: Uuid::new_v4(),
                tenant_id: "tenant-1".to_owned(),
                provider_id: image_provider_grok_cli::PROVIDER_ID.to_owned(),
                model: "grok-imagine-image-quality".to_owned(),
                work_item_id: Uuid::new_v4(),
                output_index: 0,
                command_schema: GROK_IMAGE_GENERATION_COMMAND_SCHEMA.to_owned(),
                command_hash: self.command_hash.clone(),
                execution_profile_id: Uuid::new_v4(),
                adapter_revision: image_provider_grok_cli::ADAPTER_REVISION.to_owned(),
                executor_owner: "executor-1".to_owned(),
                executor_lease_epoch: 1,
                executor_lease_expires_at_ms: i64::MAX,
            }
        }

        fn context(&self, lease: &ExecutorSubmissionLease) -> ExecutorLaunchContext {
            ExecutorLaunchContext {
                request_id: "request-1".to_owned(),
                api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
                output_index: lease.output_index,
                command_schema: lease.command_schema.clone(),
                command_hash: lease.command_hash.clone(),
                command_json: self.command.clone(),
                inputs: Vec::new(),
            }
        }
    }

    #[tokio::test]
    async fn prepare_projects_managed_video_output_into_isolated_grok_home() {
        let fixture = GrokFixture::new();
        fs::write(
            fixture.credentials.join("config.toml"),
            r#"
[marketplace]
enabled = true

[tools.zdr_video_output_s3]
bucket = "video-output"
region = "z2"
endpoint = "https://s3-z2.qiniucs.com"

[tools.zdr_video_output_s3.read_write]
access_key_id = "ak"
secret_access_key = "sk"
"#,
        )
        .unwrap();
        fs::set_permissions(
            fixture.credentials.join("config.toml"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();

        fixture.supervisor.prepare(&lease, &context).await.unwrap();

        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();
        let projected =
            fs::read_to_string(spool.provider_home_path().unwrap().join("config.toml")).unwrap();
        assert!(projected.contains("[tools.zdr_video_output_s3]"));
        assert!(projected.contains("[tools.zdr_video_output_s3.read_write]"));
        assert!(!projected.contains("marketplace"));
    }

    fn fake_grok_script(invocations: &Path, image: &Path, hardlink_artifact: bool) -> String {
        let hardlink = if hardlink_artifact {
            "/bin/ln \"$session_dir/images/1.jpg\" \"$session_dir/images/alias.jpg\""
        } else {
            ""
        };
        format!(
            r#"#!/bin/sh
cwd=""
session=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --cwd) cwd="$2"; shift 2 ;;
    --session-id) session="$2"; shift 2 ;;
    *) shift ;;
  esac
done
/bin/cat >/dev/null
printf '1\n' >> '{}'
encoded=$(printf '%s' "$cwd" | /usr/bin/sed 's/%/%25/g; s|/|%2F|g')
session_dir="$GROK_HOME/sessions/$encoded/$session"
/bin/mkdir -p "$session_dir/images"
/bin/cp '{}' "$session_dir/images/1.jpg"
{}
artifact="$session_dir/images/1.jpg"
printf '%s\n' '{{"type":"assistant","tool_calls":[{{"name":"image_gen","id":"call-1","arguments":"{{\"aspect_ratio\":\"1:1\",\"prompt\":\"draw a lighthouse\"}}"}}]}}' > "$session_dir/chat_history.jsonl"
printf '{{"type":"tool_result","tool_call_id":"call-1","content":"{{\\"path\\":\\"%s\\",\\"filename\\":\\"1.jpg\\",\\"session_folder\\":\\"images\\"}}"}}\n' "$artifact" >> "$session_dir/chat_history.jsonl"
printf '{{"type":"end","sessionId":"%s","requestId":"headless-1","stopReason":"end_turn"}}\n' "$session"
"#,
            invocations.display(),
            image.display(),
            hardlink,
        )
    }
}
