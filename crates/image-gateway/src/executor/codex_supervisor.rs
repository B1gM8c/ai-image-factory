use std::{
    env, fs,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(test)]
use std::io::Cursor;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{process::Command, time::Instant};
use uuid::Uuid;

use super::{
    CodexExecutionRequest, CodexOutputRequest, ExecutorLaunchContext, ExecutorSubmissionLease,
    RunnerError, SingleOutputSupervisor, SupervisedOutput,
    private_auth::{
        auth_file_sha256, prepare_isolated_auth, replace_isolated_auth, validate_auth_source,
    },
    project_codex_execution_request,
    runner::RunnerLaunchBinding,
};
use crate::{
    ImageGatewayError, ProxyConfig,
    artifacts::media_type_from_bytes,
    generator::GenerationJob,
    input_blobs::InputBlobStore,
    providers::openai_codex::{build_codex_prompt, build_edit_prompt},
    runner::{
        FilesystemRunnerJournal, LaunchDecision,
        process::{
            CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE, CODEX_AUTH_REFRESH_REQUEST_FILE,
            CODEX_AUTH_REFRESH_RESULT_FILE, ExecutionSpool, ProcessObservation, ProcessSpoolError,
            ProcessTerminal, ProviderProcessIdentity, RunnerLock, sha256,
        },
    },
};

pub const CODEX_GENERATION_ADAPTER_REVISION: &str = "openai-codex-generation-v1";
const MAX_EXECUTABLE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_SHEBANG_BYTES: usize = 4096;
const MAX_RUNNER_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const CHILD_REAP_TIMEOUT: Duration = Duration::from_secs(5);
const CODEX_CHILD_PATH: &str = "/usr/bin:/bin";
const MAX_INPUT_IMAGE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Clone)]
pub struct CodexProcessSupervisor {
    journal: Arc<FilesystemRunnerJournal>,
    helper_executable: PathBuf,
    codex_executable: PathBuf,
    codex_executable_sha256: String,
    credential_auth_file: PathBuf,
    credential_auth_sha256: String,
    credential_resolver: Option<(Uuid, Arc<dyn crate::OperationalCredentialResolver>)>,
    credential_refresher: Option<Arc<dyn crate::OperationalCredentialRefresher>>,
    request_timeout: Duration,
    poll_interval: Duration,
    startup_grace: Duration,
    child_env: Vec<(String, String)>,
    input_blobs: Option<Arc<dyn InputBlobStore>>,
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
    credential_revision: i64,
    credential_auth_sha256: String,
    timeout_ms: u64,
    output: CodexExecutionRequest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CodexAuthRefreshRequestV1 {
    schema_version: u16,
    executor_execution_id: String,
    observed_revision: i64,
    observed_fingerprint_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum CodexAuthRefreshResultV1 {
    Succeeded {
        schema_version: u16,
        executor_execution_id: String,
        observed_revision: i64,
        observed_fingerprint_sha256: String,
        promoted_revision: i64,
        promoted_fingerprint_sha256: String,
    },
    Failed {
        schema_version: u16,
        executor_execution_id: String,
        observed_revision: i64,
        observed_fingerprint_sha256: String,
    },
}

enum ChildOutcome {
    Succeeded(Vec<u8>),
    Failed(&'static str),
    Uncertain(&'static str),
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
            credential_refresher: None,
            request_timeout,
            poll_interval,
            startup_grace,
            child_env: child_environment(proxy),
            input_blobs: None,
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

    pub fn with_credential_refresher(
        mut self,
        refresher: Arc<dyn crate::OperationalCredentialRefresher>,
    ) -> Self {
        self.credential_refresher = Some(refresher);
        self
    }

    pub fn with_input_blobs(mut self, input_blobs: Arc<dyn InputBlobStore>) -> Self {
        self.input_blobs = Some(input_blobs);
        self
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
        credential_revision: i64,
        credential_auth_sha256: &str,
    ) -> Result<CodexChildRequest, RunnerError> {
        if !matches!(
            lease.adapter_revision.as_str(),
            CODEX_GENERATION_ADAPTER_REVISION | super::CODEX_EDIT_INLINE_ADAPTER_REVISION
        ) {
            return Err(RunnerError::Definite {
                error_code: "executor_adapter_revision_mismatch".to_string(),
            });
        }
        let output =
            project_codex_execution_request(lease, context).map_err(|_| RunnerError::Definite {
                error_code: "executor_command_rejected".to_string(),
            })?;
        Ok(CodexChildRequest {
            schema_version: 2,
            adapter_revision: lease.adapter_revision.clone(),
            executor_execution_id: lease.executor_execution_id.to_string(),
            launch: RunnerLaunchBinding::from_lease(lease),
            codex_executable: self.codex_executable.to_string_lossy().into_owned(),
            codex_executable_sha256: self.codex_executable_sha256.clone(),
            credential_revision,
            credential_auth_sha256: credential_auth_sha256.to_string(),
            timeout_ms: self.request_timeout.as_millis() as u64,
            output,
        })
    }

    async fn stage_inputs(
        &self,
        request: &CodexExecutionRequest,
        context: &ExecutorLaunchContext,
        spool: &ExecutionSpool,
    ) -> Result<(), RunnerError> {
        let CodexExecutionRequest::Edit(request) = request else {
            return if context.inputs().is_empty() {
                Ok(())
            } else {
                Err(RunnerError::Definite {
                    error_code: "codex_input_manifest_invalid".to_string(),
                })
            };
        };
        if request.inputs.len() != context.inputs().len() {
            return Err(RunnerError::Definite {
                error_code: "codex_input_manifest_invalid".to_string(),
            });
        }
        let blobs = self.input_blobs.as_ref().ok_or(RunnerError::Definite {
            error_code: "codex_input_store_unavailable".to_string(),
        })?;
        for (expected, input) in request.inputs.iter().zip(context.inputs()) {
            if expected.role != input.role()
                || expected.index != input.index()
                || expected.media_type != input.media_type()
                || expected.sha256_hex != input.blob().sha256_hex
                || expected.byte_size != input.blob().byte_size
                || expected.byte_size > MAX_INPUT_IMAGE_BYTES
            {
                return Err(RunnerError::Definite {
                    error_code: "codex_input_manifest_invalid".to_string(),
                });
            }
            let bytes = blobs.get(input.blob()).await.map_err(|error| match error {
                crate::input_blobs::InputBlobReadError::Unavailable => RunnerError::Unavailable,
                crate::input_blobs::InputBlobReadError::Integrity => RunnerError::Definite {
                    error_code: "codex_input_integrity_failed".to_string(),
                },
            })?;
            if bytes.len() as u64 != expected.byte_size
                || sha256(&bytes) != expected.sha256_hex
                || media_type_from_bytes(&bytes).ok() != Some(expected.media_type.as_str())
            {
                return Err(RunnerError::Definite {
                    error_code: "codex_input_integrity_failed".to_string(),
                });
            }
            spool
                .stage_provider_input(&expected.filename, &bytes, MAX_INPUT_IMAGE_BYTES)
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
            .stderr(Stdio::inherit())
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

    async fn handle_auth_refresh_request(
        &self,
        lease: &ExecutorSubmissionLease,
        spool: &ExecutionSpool,
    ) -> Result<(), RunnerError> {
        if spool
            .read_diagnostic::<CodexAuthRefreshResultV1>(CODEX_AUTH_REFRESH_RESULT_FILE)
            .map_err(map_spool_error)?
            .is_some()
        {
            return Ok(());
        }
        let Some(request) = spool
            .read_diagnostic::<CodexAuthRefreshRequestV1>(CODEX_AUTH_REFRESH_REQUEST_FILE)
            .map_err(map_spool_error)?
        else {
            return Ok(());
        };
        if request.schema_version != 1
            || request.executor_execution_id != lease.executor_execution_id.to_string()
            || request.observed_revision <= 0
            || !is_sha256(&request.observed_fingerprint_sha256)
        {
            return Err(RunnerError::Unknown {
                error_code: "codex_auth_refresh_request_invalid".to_string(),
            });
        }
        let Some((provider_account_id, _)) = &self.credential_resolver else {
            return Err(RunnerError::Unavailable);
        };
        let Some(refresher) = &self.credential_refresher else {
            return Err(RunnerError::Unavailable);
        };
        let refreshed = refresher
            .refresh_after_authentication_rejection(
                *provider_account_id,
                request.observed_revision,
                &request.observed_fingerprint_sha256,
            )
            .await;
        let result = match refreshed {
            Ok(credential)
                if credential.provider_account_id == *provider_account_id
                    && credential.provider_id
                        == image_provider_contracts::openai_codex::PROVIDER_ID
                    && credential.revision > request.observed_revision
                    && credential.material_fingerprint_sha256
                        != request.observed_fingerprint_sha256 =>
            {
                let rebound = match (
                    validate_auth_source(
                        credential.home(),
                        &credential.material_fingerprint_sha256,
                    ),
                    spool.codex_home_path(),
                ) {
                    (Ok(source), Ok(destination)) => rebind_isolated_auth(
                        destination,
                        &source,
                        &request.observed_fingerprint_sha256,
                        &credential.material_fingerprint_sha256,
                    ),
                    _ => Err(()),
                };
                if rebound.is_ok() {
                    CodexAuthRefreshResultV1::Succeeded {
                        schema_version: 1,
                        executor_execution_id: request.executor_execution_id.clone(),
                        observed_revision: request.observed_revision,
                        observed_fingerprint_sha256: request.observed_fingerprint_sha256.clone(),
                        promoted_revision: credential.revision,
                        promoted_fingerprint_sha256: credential.material_fingerprint_sha256,
                    }
                } else {
                    CodexAuthRefreshResultV1::Failed {
                        schema_version: 1,
                        executor_execution_id: request.executor_execution_id.clone(),
                        observed_revision: request.observed_revision,
                        observed_fingerprint_sha256: request.observed_fingerprint_sha256.clone(),
                    }
                }
            }
            _ => CodexAuthRefreshResultV1::Failed {
                schema_version: 1,
                executor_execution_id: request.executor_execution_id.clone(),
                observed_revision: request.observed_revision,
                observed_fingerprint_sha256: request.observed_fingerprint_sha256.clone(),
            },
        };
        spool
            .publish_diagnostic(CODEX_AUTH_REFRESH_RESULT_FILE, &result)
            .map_err(map_spool_error)
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

fn rebind_isolated_auth(
    destination_home: &Path,
    source: &Path,
    observed_sha256: &str,
    promoted_sha256: &str,
) -> Result<(), ()> {
    match auth_file_sha256(destination_home) {
        Ok(current) if current == promoted_sha256 => Ok(()),
        Ok(current) if current == observed_sha256 => {
            replace_isolated_auth(destination_home, source, observed_sha256, promoted_sha256)
                .map_err(|_| ())
        }
        _ => Err(()),
    }
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
        let request =
            self.child_request(lease, context, credential_revision, &credential_auth_sha256)?;
        let bytes = serde_json::to_vec(&request).map_err(|_| RunnerError::Internal)?;
        let spool = ExecutionSpool::for_lease(&self.journal, lease).map_err(map_spool_error)?;
        self.stage_inputs(&request.output, context, &spool).await?;
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
            self.handle_auth_refresh_request(lease, &spool).await?;
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
    let (prompt, input_paths) = match child_invocation(&request.output, &spool, workspace) {
        Ok(invocation) => invocation,
        Err(outcome) => return outcome,
    };
    let mut environment = allowed_child_environment();
    environment.push(("PATH".to_string(), CODEX_CHILD_PATH.to_string()));
    let request_timeout = Duration::from_millis(request.timeout_ms);
    let deadline = Instant::now() + request_timeout;
    let mut attempt = 1_u8;
    let outcome = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break ChildOutcome::Uncertain("codex_timeout");
        }
        let diagnostic = Mutex::new(None);
        let provider_process = Mutex::new(None);
        let diagnostic_sink =
            |value: &crate::codex_app_server::CodexAppServerFailureDiagnosticV1| {
                *diagnostic.lock().map_err(|_| ())? = Some(value.clone());
                Ok(())
            };
        let runtime_result = crate::codex_app_server::run_codex_app_server(
            crate::codex_app_server::CodexAppServerRequest {
                request_id: request.output.request_id(),
                image_index: request.output.candidate_index(),
                attempt,
                executable: Path::new(&request.codex_executable),
                workspace,
                codex_home,
                prompt: &prompt,
                input_paths: &input_paths,
                timeout: remaining,
                environment: &environment,
                failure_diagnostic_sink: Some(&diagnostic_sink),
            },
            |pid| {
                ProviderProcessIdentity::capture(pid, &helper.nonce)
                    .and_then(|provider| {
                        spool.publish_provider_process(&runner_lock, &helper, &provider)?;
                        *provider_process
                            .lock()
                            .map_err(|_| ProcessSpoolError::Unavailable)? = Some(provider);
                        Ok(())
                    })
                    .map_err(|_| ())
            },
        )
        .await;
        match runtime_result {
            Ok(bytes) => break ChildOutcome::Succeeded(bytes),
            Err(error) => {
                let diagnostic = diagnostic.into_inner().ok().flatten();
                let provider_process = provider_process.into_inner().ok().flatten();
                let retryable = attempt == 1
                    && diagnostic
                        .as_ref()
                        .is_some_and(|value| value.is_retryable_authentication_rejection())
                    && deadline.saturating_duration_since(Instant::now())
                        > Duration::from_millis(250)
                    && provider_process.as_ref().is_some_and(|provider| {
                        spool
                            .retire_provider_process(&runner_lock, &helper, provider)
                            .is_ok()
                    });
                if retryable {
                    tracing::warn!(
                        request.id = %request.output.request_id(),
                        output.index = request.output.candidate_index(),
                        codex.attempt = attempt,
                        error.code = error.code(),
                        "retrying one definitive Codex authentication rejection"
                    );
                    let refresh_request = CodexAuthRefreshRequestV1 {
                        schema_version: 1,
                        executor_execution_id: request.executor_execution_id.clone(),
                        observed_revision: request.credential_revision,
                        observed_fingerprint_sha256: request.credential_auth_sha256.clone(),
                    };
                    if spool
                        .publish_diagnostic(CODEX_AUTH_REFRESH_REQUEST_FILE, &refresh_request)
                        .is_err()
                    {
                        break ChildOutcome::Uncertain("codex_auth_refresh_handoff_failed");
                    }
                    let refresh_result = loop {
                        match spool.read_diagnostic::<CodexAuthRefreshResultV1>(
                            CODEX_AUTH_REFRESH_RESULT_FILE,
                        ) {
                            Ok(Some(result)) => break result,
                            Ok(None) => {}
                            Err(_) => {
                                break CodexAuthRefreshResultV1::Failed {
                                    schema_version: 1,
                                    executor_execution_id: request.executor_execution_id.clone(),
                                    observed_revision: request.credential_revision,
                                    observed_fingerprint_sha256: request
                                        .credential_auth_sha256
                                        .clone(),
                                };
                            }
                        }
                        if deadline.saturating_duration_since(Instant::now()).is_zero() {
                            break CodexAuthRefreshResultV1::Failed {
                                schema_version: 1,
                                executor_execution_id: request.executor_execution_id.clone(),
                                observed_revision: request.credential_revision,
                                observed_fingerprint_sha256: request.credential_auth_sha256.clone(),
                            };
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    };
                    match refresh_result {
                        CodexAuthRefreshResultV1::Succeeded {
                            schema_version: 1,
                            executor_execution_id,
                            observed_revision,
                            observed_fingerprint_sha256,
                            promoted_revision,
                            promoted_fingerprint_sha256,
                        } if executor_execution_id == request.executor_execution_id
                            && observed_revision == request.credential_revision
                            && observed_fingerprint_sha256 == request.credential_auth_sha256
                            && promoted_revision > observed_revision
                            && promoted_fingerprint_sha256 != observed_fingerprint_sha256
                            && auth_file_sha256(codex_home).ok().as_deref()
                                == Some(promoted_fingerprint_sha256.as_str()) =>
                        {
                            attempt = 2;
                            continue;
                        }
                        _ => break ChildOutcome::Failed("codex_authentication_rejected"),
                    }
                }
                if let Some(diagnostic) = diagnostic.as_ref()
                    && spool
                        .publish_diagnostic(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE, diagnostic)
                        .is_err()
                {
                    tracing::warn!(
                        request.id = %request.output.request_id(),
                        output.index = request.output.candidate_index(),
                        "Codex failure diagnostic could not be persisted"
                    );
                }
                tracing::warn!(
                    request.id = %request.output.request_id(),
                    output.index = request.output.candidate_index(),
                    codex.output.stage = "sealed_handoff",
                    error.code = error.code(),
                    "Codex completed without a valid sealed image handoff"
                );
                break map_codex_app_server_child_error(error);
            }
        }
    };
    match outcome {
        ChildOutcome::Succeeded(bytes) => {
            match normalize_captured_image(
                bytes,
                request.output.output_format(),
                request.output.output_compression(),
            ) {
                Ok(bytes) => ChildOutcome::Succeeded(bytes),
                Err(()) => ChildOutcome::Failed("codex_durable_output_invalid"),
            }
        }
        outcome => outcome,
    }
}

fn map_codex_app_server_child_error(
    error: crate::codex_app_server::CodexAppServerError,
) -> ChildOutcome {
    use crate::codex_app_server::CodexAppServerError;

    match error {
        CodexAppServerError::Unavailable => ChildOutcome::Failed("codex_cli_unavailable"),
        CodexAppServerError::RequestRejected => {
            ChildOutcome::Failed("codex_app_server_request_rejected")
        }
        CodexAppServerError::TurnFailed => ChildOutcome::Failed("codex_turn_failed"),
        CodexAppServerError::ImageToolFailed => ChildOutcome::Failed("codex_image_tool_failed"),
        CodexAppServerError::ContentPolicyRejected => {
            ChildOutcome::Failed("content_policy_rejected")
        }
        CodexAppServerError::NoImage => ChildOutcome::Failed("codex_no_image_output"),
        CodexAppServerError::ImageIncomplete
        | CodexAppServerError::OutputMissing
        | CodexAppServerError::OutputInvalid => {
            ChildOutcome::Failed("codex_image_output_disappeared")
        }
        CodexAppServerError::MultipleImages => ChildOutcome::Failed("codex_multiple_image_outputs"),
        CodexAppServerError::SpawnIdentity => {
            ChildOutcome::Uncertain("codex_process_identity_unavailable")
        }
        CodexAppServerError::Stdin => ChildOutcome::Uncertain("codex_stdin_failed"),
        CodexAppServerError::Timeout => ChildOutcome::Uncertain("codex_timeout"),
        CodexAppServerError::Protocol => ChildOutcome::Uncertain("codex_event_capture_invalid"),
        CodexAppServerError::ProcessExited => {
            ChildOutcome::Uncertain("codex_process_exited_without_terminal")
        }
        CodexAppServerError::OutputUnavailable => ChildOutcome::Uncertain("service_unavailable"),
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

fn validate_child_request(
    request: &CodexChildRequest,
    executor_execution_id: Uuid,
) -> Result<ExecutorSubmissionLease, ImageGatewayError> {
    let lease = request.launch.to_lease().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Codex runner lease binding is invalid")
    })?;
    let valid_binding = matches!(
        (
            &request.output,
            lease.command_schema.as_str(),
            lease.adapter_revision.as_str()
        ),
        (
            CodexExecutionRequest::Generation(_),
            crate::admission::GENERATION_COMMAND_SCHEMA,
            CODEX_GENERATION_ADAPTER_REVISION
        ) | (
            CodexExecutionRequest::Edit(_),
            crate::admission::EDIT_COMMAND_SCHEMA,
            super::CODEX_EDIT_INLINE_ADAPTER_REVISION
        )
    );
    if request.schema_version != 2
        || request.adapter_revision != lease.adapter_revision
        || request.executor_execution_id != executor_execution_id.to_string()
        || lease.executor_execution_id != executor_execution_id
        || lease.provider_id != image_provider_contracts::openai_codex::PROVIDER_ID
        || !valid_binding
        || lease.model != request.output.model()
        || u32::try_from(lease.output_index)
            .ok()
            .map(|index| index + 1)
            != Some(request.output.candidate_index())
        || request.credential_revision <= 0
        || !is_sha256(&request.credential_auth_sha256)
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

fn child_invocation(
    request: &CodexExecutionRequest,
    spool: &ExecutionSpool,
    workspace: &Path,
) -> Result<(String, Vec<PathBuf>), ChildOutcome> {
    match request {
        CodexExecutionRequest::Generation(request) => {
            let job = generation_job(request);
            Ok((
                build_codex_prompt(&job, workspace, request.candidate_index),
                Vec::new(),
            ))
        }
        CodexExecutionRequest::Edit(request) => {
            let image_count = request
                .inputs
                .iter()
                .filter(|input| input.role == "image")
                .count();
            let has_mask = request.inputs.iter().any(|input| input.role == "mask");
            let mut prompt = build_edit_prompt(&request.prompt, image_count, has_mask);
            if request.original_n > 1 {
                prompt.push_str(&format!(
                    "\n整个请求需要 {} 张候选结果；当前只生成第 {}/{} 张。请只输出这一张，并让它保持用户需求一致但与其他候选有独立细节。",
                    request.original_n, request.candidate_index, request.original_n
                ));
            }
            let root = spool
                .provider_attempt_path()
                .map_err(|_| ChildOutcome::Uncertain("codex_input_integrity_failed"))?;
            let mut paths = Vec::with_capacity(request.inputs.len());
            for input in &request.inputs {
                let mut file = spool
                    .open_provider_input(&input.filename)
                    .map_err(|_| ChildOutcome::Failed("codex_input_integrity_failed"))?;
                let mut bytes = Vec::with_capacity(input.byte_size as usize);
                Read::by_ref(&mut file)
                    .take(MAX_INPUT_IMAGE_BYTES + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|_| ChildOutcome::Failed("codex_input_integrity_failed"))?;
                if bytes.len() as u64 != input.byte_size
                    || sha256(&bytes) != input.sha256_hex
                    || media_type_from_bytes(&bytes).ok() != Some(input.media_type.as_str())
                {
                    return Err(ChildOutcome::Failed("codex_input_integrity_failed"));
                }
                paths.push(root.join(&input.filename));
            }
            Ok((prompt, paths))
        }
    }
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

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
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
        EDIT_COMMAND_SCHEMA, EditCommandV1, EditInputDescriptorV1, EditInputRoleV1,
        GENERATION_COMMAND_SCHEMA, GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION,
        GenerationCommandV1,
    };
    use crate::artifacts::InMemoryArtifactBlobStore;
    use crate::executor::private_auth::{AUTH_FILE, MAX_AUTH_BYTES};
    use crate::input_blobs::{InputBlobKey, InputBlobStore};

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
    fn auth_rebind_replay_accepts_the_same_promoted_revision_after_parent_crash() {
        let root = TempDir::new().unwrap();
        let destination = root.path().join("destination");
        let source = root.path().join("source");
        fs::create_dir(&destination).unwrap();
        fs::create_dir(&source).unwrap();
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&source, fs::Permissions::from_mode(0o700)).unwrap();
        let old = br#"{"revision":1}"#;
        let new = br#"{"revision":2}"#;
        fs::write(destination.join(AUTH_FILE), old).unwrap();
        fs::write(source.join(AUTH_FILE), new).unwrap();
        fs::set_permissions(
            destination.join(AUTH_FILE),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        fs::set_permissions(source.join(AUTH_FILE), fs::Permissions::from_mode(0o600)).unwrap();

        let observed = sha256(old);
        let promoted = sha256(new);
        rebind_isolated_auth(&destination, &source.join(AUTH_FILE), &observed, &promoted).unwrap();
        rebind_isolated_auth(&destination, &source.join(AUTH_FILE), &observed, &promoted).unwrap();
        assert_eq!(codex_auth_file_sha256(&destination).unwrap(), promoted);

        let foreign = sha256(b"foreign");
        assert!(
            rebind_isolated_auth(&destination, &source.join(AUTH_FILE), &foreign, &observed,)
                .is_err()
        );
    }

    #[test]
    fn app_server_failures_keep_stable_terminal_categories() {
        use crate::codex_app_server::CodexAppServerError;

        for (error, expected) in [
            (
                CodexAppServerError::RequestRejected,
                "codex_app_server_request_rejected",
            ),
            (CodexAppServerError::TurnFailed, "codex_turn_failed"),
            (
                CodexAppServerError::ImageToolFailed,
                "codex_image_tool_failed",
            ),
            (
                CodexAppServerError::ContentPolicyRejected,
                "content_policy_rejected",
            ),
        ] {
            match map_codex_app_server_child_error(error) {
                ChildOutcome::Failed(actual) => assert_eq!(actual, expected),
                ChildOutcome::Succeeded(_) | ChildOutcome::Uncertain(_) => {
                    panic!("stable app-server failure was not terminalized as failed")
                }
            }
        }
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
        assert!(
            !execution_root
                .join(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE)
                .exists()
        );
        let replay = fixture
            .supervisor
            .start_or_attach(&lease, LaunchDecision::Attach)
            .await
            .unwrap();

        assert_eq!(first, replay);
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n");
    }

    #[tokio::test]
    async fn image_tool_failure_persists_redacted_diagnostic_before_terminal_cleanup() {
        let fixture = CodexFixture::image_tool_failure();
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
                error_code: "codex_image_tool_failed".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        let diagnostic_path = execution_root.join(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE);
        let diagnostic_bytes = fs::read(&diagnostic_path).unwrap();
        assert!(diagnostic_bytes.len() <= 64 * 1024);
        assert!(
            !diagnostic_bytes
                .windows(b"provider-sensitive-prompt-fragment".len())
                .any(|value| value == b"provider-sensitive-prompt-fragment")
        );
        let diagnostic: serde_json::Value = serde_json::from_slice(&diagnostic_bytes).unwrap();
        assert_eq!(diagnostic["schema_version"], 1);
        assert_eq!(diagnostic["failure_category"], "codex_image_tool_failed");
        assert_eq!(diagnostic["source"], "image_generation_item");
        assert_eq!(diagnostic["class"], "rate_limit");
        assert_eq!(
            diagnostic["message"]["bytes"],
            b"provider-sensitive-prompt-fragment".len()
        );
        assert_eq!(diagnostic["message"]["sha256"].as_str().unwrap().len(), 64);
        assert!(execution_root.join("result.json").is_file());
        assert!(!execution_root.join("output.bin").exists());
        assert!(!execution_root.join("codex-home").exists());
        assert!(!execution_root.join("workspace").exists());
        assert!(!execution_root.join("runtime-home").exists());
        assert!(diagnostic_path.is_file());
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n");

        assert_eq!(
            fixture
                .supervisor
                .start_or_attach(&lease, LaunchDecision::Attach)
                .await,
            Err(RunnerError::Definite {
                error_code: "codex_image_tool_failed".to_string(),
            })
        );
        assert_eq!(fs::read(diagnostic_path).unwrap(), diagnostic_bytes);
    }

    #[tokio::test]
    async fn definitive_http_401_is_retried_once_and_returns_the_second_output() {
        let fixture = CodexFixture::transient_http_401_then_success();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        let runner_root = fixture.journal.root_path().to_path_buf();
        let execution_id = lease.executor_execution_id;
        let child =
            tokio::spawn(async move { run_codex_runner_child(runner_root, execution_id).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if spool
                    .read_diagnostic::<CodexAuthRefreshRequestV1>(CODEX_AUTH_REFRESH_REQUEST_FILE)
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let replacement_home = fixture._temp.path().join("replacement-credentials");
        fs::create_dir(&replacement_home).unwrap();
        let replacement = replacement_home.join(AUTH_FILE);
        fs::write(&replacement, br#"{"fresh":true}"#).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
        let replacement_sha = sha256(br#"{"fresh":true}"#);
        replace_isolated_auth(
            spool.codex_home_path().unwrap(),
            &replacement,
            &sha256(b"{}"),
            &replacement_sha,
        )
        .unwrap();
        spool
            .publish_diagnostic(
                CODEX_AUTH_REFRESH_RESULT_FILE,
                &CodexAuthRefreshResultV1::Succeeded {
                    schema_version: 1,
                    executor_execution_id: lease.executor_execution_id.to_string(),
                    observed_revision: 1,
                    observed_fingerprint_sha256: sha256(b"{}"),
                    promoted_revision: 2,
                    promoted_fingerprint_sha256: replacement_sha,
                },
            )
            .unwrap();
        child.await.unwrap().unwrap();

        assert!(matches!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(_)
        ));
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n1\n");
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(execution_root.join("output.bin").is_file());
        assert!(execution_root.join("result.json").is_file());
        assert!(
            !execution_root
                .join(CODEX_APP_SERVER_FAILURE_DIAGNOSTIC_FILE)
                .exists()
        );
    }

    #[tokio::test]
    async fn repeated_http_401_stops_after_one_rebound_retry() {
        let fixture = CodexFixture::permanent_http_401();
        let lease = fixture.lease();
        let context = fixture.context(&lease);
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture.supervisor.prepare(&lease, &context).await.unwrap();
        assert_eq!(
            fixture.journal.commit_launch(&lease).unwrap(),
            LaunchDecision::LaunchOnce
        );
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        let runner_root = fixture.journal.root_path().to_path_buf();
        let execution_id = lease.executor_execution_id;
        let child =
            tokio::spawn(async move { run_codex_runner_child(runner_root, execution_id).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if spool
                    .read_diagnostic::<CodexAuthRefreshRequestV1>(CODEX_AUTH_REFRESH_REQUEST_FILE)
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let replacement_home = fixture._temp.path().join("replacement-credentials");
        fs::create_dir(&replacement_home).unwrap();
        let replacement = replacement_home.join(AUTH_FILE);
        fs::write(&replacement, br#"{"fresh":true}"#).unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
        let replacement_sha = sha256(br#"{"fresh":true}"#);
        replace_isolated_auth(
            spool.codex_home_path().unwrap(),
            &replacement,
            &sha256(b"{}"),
            &replacement_sha,
        )
        .unwrap();
        spool
            .publish_diagnostic(
                CODEX_AUTH_REFRESH_RESULT_FILE,
                &CodexAuthRefreshResultV1::Succeeded {
                    schema_version: 1,
                    executor_execution_id: lease.executor_execution_id.to_string(),
                    observed_revision: 1,
                    observed_fingerprint_sha256: sha256(b"{}"),
                    promoted_revision: 2,
                    promoted_fingerprint_sha256: replacement_sha,
                },
            )
            .unwrap();
        child.await.unwrap().unwrap();

        assert_eq!(
            spool.observe().unwrap(),
            ProcessObservation::Failed {
                error_code: "codex_image_tool_failed".to_string(),
            }
        );
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n1\n");
    }

    #[tokio::test]
    async fn edit_prepare_stages_the_exact_digest_bound_input_for_one_output() {
        let fixture = CodexFixture::new();
        let blobs = Arc::new(InMemoryArtifactBlobStore::default());
        let input_bytes = png_bytes(2, 3);
        let input = blobs
            .put(
                InputBlobKey {
                    admission_session_id: Uuid::new_v4(),
                    input_id: Uuid::new_v4(),
                },
                &input_bytes,
            )
            .await
            .unwrap();
        let command = EditCommandV1::from_edit_job(
            &crate::EditJob {
                request_id: "edit-request-1".to_string(),
                model: "gpt-image-2".to_string(),
                prompt: "replace the sky".to_string(),
                moderation: "auto".to_string(),
                images: Vec::new(),
                mask: None,
                n: 2,
                size: "auto".to_string(),
                quality: "high".to_string(),
                output_format: "png".to_string(),
                output_compression: None,
                background: "opaque".to_string(),
                stream: false,
                partial_images: 0,
            },
            vec![EditInputDescriptorV1 {
                byte_size: input.byte_size,
                index: 0,
                media_type: "image/png".to_string(),
                role: EditInputRoleV1::Image,
                sha256_hex: input.sha256_hex.clone(),
            }],
            "openai-images-v1",
            "openai-codex",
        );
        let lease = ExecutorSubmissionLease {
            submission_id: Uuid::new_v4(),
            executor_execution_id: Uuid::new_v4(),
            output_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            tenant_id: "tenant-1".to_string(),
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            work_item_id: Uuid::new_v4(),
            output_index: 1,
            command_schema: EDIT_COMMAND_SCHEMA.to_string(),
            command_hash: command.request_hash_hex(),
            execution_profile_id: Uuid::new_v4(),
            adapter_revision: crate::executor::CODEX_EDIT_INLINE_ADAPTER_REVISION.to_string(),
            executor_owner: "executor-owner-1".to_string(),
            executor_lease_epoch: 1,
            executor_lease_expires_at_ms: i64::MAX,
        };
        let context = ExecutorLaunchContext::new(
            "edit-request-1",
            "openai-images-v1",
            1,
            EDIT_COMMAND_SCHEMA,
            command.request_hash_hex(),
            serde_json::to_value(&command).unwrap(),
        )
        .unwrap()
        .with_inputs(vec![
            crate::executor::ExecutorInputObject::new(input, "image", 0, "image/png").unwrap(),
        ])
        .unwrap();
        fixture.journal.start_or_attach(&lease).unwrap();
        fixture
            .supervisor
            .clone()
            .with_input_blobs(blobs)
            .prepare(&lease, &context)
            .await
            .unwrap();
        let spool = ExecutionSpool::for_lease(&fixture.journal, &lease).unwrap();

        assert_eq!(
            fs::read(spool.provider_attempt_path().unwrap().join("input-0.png")).unwrap(),
            input_bytes
        );
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
            ProcessObservation::Uncertain {
                error_code: "codex_event_capture_invalid".to_string(),
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
            ProcessObservation::Uncertain {
                error_code: "codex_event_capture_invalid".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("output.bin").exists());
    }

    #[tokio::test]
    async fn orphan_runtime_output_without_image_event_never_succeeds() {
        let fixture = CodexFixture::orphan_runtime_output_without_image_event();
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
                error_code: "codex_no_image_output".to_string(),
            }
        );
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(!execution_root.join("output.bin").exists());
    }

    #[tokio::test]
    async fn agent_written_runtime_output_cannot_replace_exact_native_authority() {
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

        assert!(matches!(
            spool.observe().unwrap(),
            ProcessObservation::Succeeded(_)
        ));
        let execution_root = fixture
            .journal
            .root_path()
            .join(lease.executor_execution_id.simple().to_string());
        assert!(execution_root.join("output.bin").exists());
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

    async fn assert_concurrent_durable_handoffs_are_execution_scoped(concurrency: usize) {
        let shared_root = TempDir::new().unwrap();
        let shared_journal =
            Arc::new(FilesystemRunnerJournal::new(shared_root.path().join("journal")).unwrap());
        let fixtures = (0..concurrency)
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
            concurrency
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
    #[ignore = "61-process stress gate; run explicitly to avoid starving unrelated process tests"]
    async fn concurrent_durable_handoffs_are_scoped_at_1_20_40() {
        for concurrency in [1, 20, 40] {
            let started = Instant::now();
            assert_concurrent_durable_handoffs_are_execution_scoped(concurrency).await;
            let elapsed = started.elapsed();
            eprintln!("managed Codex handoff concurrency={concurrency} elapsed={elapsed:?}");
            assert!(
                elapsed < Duration::from_secs(60),
                "managed Codex handoff concurrency={concurrency} exceeded 60 seconds"
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
                error_code: "codex_no_image_output".to_string(),
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
                error_code: "codex_turn_failed".to_string(),
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

    fn app_server_fixture_script(exec_body: String) -> String {
        let image_tool_failure = usize::from(exec_body.contains("codex-test-image-tool-failure"));
        let transient_http_401 = usize::from(exec_body.contains("codex-test-transient-http-401"));
        let force_malformed = usize::from(
            exec_body.contains("/usr/bin/head -c 70000")
                || exec_body.contains("printf '{\"type\":\"thread.started\",\"thread_id\":'")
                || exec_body.contains("printf '{{\"type\":\"thread.started\",\"thread_id\":'"),
        );
        let exec_body = exec_body
            .lines()
            .filter(|line| !line.starts_with("#!") && *line != "set -eu")
            .collect::<Vec<_>>()
            .join("\n")
            .replace("/bin/cat >/dev/null", ":");
        format!(
            r#"#!/bin/sh
set -eu
thread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'
turn_id='019fd9f5-badb-7dd3-8903-28ffded0ef55'
IFS= read -r initialize
printf '{{"id":1,"result":{{"codexHome":"%s"}}}}\n' "$CODEX_HOME"
IFS= read -r initialized
IFS= read -r thread_start
printf '{{"method":"thread/started","params":{{"thread":{{"id":"%s"}}}}}}\n' "$thread_id"
printf '{{"id":2,"result":{{"thread":{{"id":"%s"}}}}}}\n' "$thread_id"
IFS= read -r turn_start
printf '{{"method":"turn/started","params":{{"threadId":"%s","turn":{{"id":"%s"}}}}}}\n' "$thread_id" "$turn_id"
printf '{{"id":3,"result":{{"turn":{{"id":"%s"}}}}}}\n' "$turn_id"
legacy_events="$CODEX_HOME/legacy-events.jsonl"
set +e
(
{exec_body}
) > "$legacy_events"
legacy_status=$?
set -e
image_tool_failure={image_tool_failure}
transient_http_401={transient_http_401}
if [ "$transient_http_401" -eq 1 ] && [ -f "$CODEX_HOME/transient-http-401" ]; then
  image_tool_failure=1
fi
malformed={force_malformed}
while IFS= read -r event; do
  case "$event" in
    '{{"type":"thread.started","thread_id":"'*'"}}') ;;
    '{{"type":"item.completed","item":{{"type":"image_generation_call","id":"'*'"}}}}') ;;
    '') ;;
    *) malformed=1 ;;
  esac
done < "$legacy_events"
if [ "$malformed" -eq 0 ]; then
/usr/bin/sed -n 's/.*"type":"image_generation_call","id":"\([^"]*\)".*/\1/p' "$legacy_events" | while IFS= read -r call_id; do
  output_path="$CODEX_HOME/generated_images/$thread_id/$call_id.png"
  printf '{{"method":"item/started","params":{{"threadId":"%s","turnId":"%s","item":{{"type":"imageGeneration","id":"%s","status":"inProgress"}}}}}}\n' "$thread_id" "$turn_id" "$call_id"
  printf '{{"method":"item/completed","params":{{"threadId":"%s","turnId":"%s","item":{{"type":"imageGeneration","id":"%s","status":"completed","result":"cG5n","savedPath":"%s"}}}}}}\n' "$thread_id" "$turn_id" "$call_id" "$output_path"
done
fi
if [ "$image_tool_failure" -eq 1 ]; then
  call_id='call_failed_image'
  printf '{{"method":"item/started","params":{{"threadId":"%s","turnId":"%s","item":{{"type":"imageGeneration","id":"%s","status":"inProgress"}}}}}}\n' "$thread_id" "$turn_id" "$call_id"
  printf '{{"method":"item/completed","params":{{"threadId":"%s","turnId":"%s","item":{{"type":"imageGeneration","id":"%s","status":"failed","result":{{"code":"rate_limit_exceeded","message":"provider-sensitive-prompt-fragment"}}}}}}}}\n' "$thread_id" "$turn_id" "$call_id"
fi
if [ "$legacy_status" -eq 0 ]; then
  printf '{{"method":"turn/completed","params":{{"threadId":"%s","turn":{{"id":"%s","status":"completed"}}}}}}\n' "$thread_id" "$turn_id"
else
  printf '{{"method":"turn/completed","params":{{"threadId":"%s","turn":{{"id":"%s","status":"failed"}}}}}}\n' "$thread_id" "$turn_id"
fi
if [ "$malformed" -ne 0 ]; then printf 'not-json\n'; fi
while IFS= read -r ignored; do :; done
"#
        )
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

        fn image_tool_failure() -> Self {
            Self::with_script(|invocations, _image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\n# codex-test-image-tool-failure\n",
                    invocations.display()
                )
            })
        }

        fn transient_http_401_then_success() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\n# codex-test-transient-http-401\nif [ ! -f '{}' ]; then\n  printf '1\\n' >> '{}'\n  printf 'HTTP 401 Unauthorized\\n' >&2\n  : > \"$CODEX_HOME/transient-http-401\"\nelse\n  printf '1\\n' >> '{}'\n  /bin/rm -f \"$CODEX_HOME/transient-http-401\"\n  thread_id='019fd9f5-badb-7dd3-8903-28ffded0ef54'\n  call_id='call_retry_image'\n  output_dir=\"$CODEX_HOME/generated_images/$thread_id\"\n  /bin/mkdir -p \"$output_dir\"\n  /bin/chmod 700 \"$CODEX_HOME/generated_images\" \"$output_dir\"\n  /bin/cp '{}' \"$output_dir/$call_id.png\"\n  /bin/chmod 600 \"$output_dir/$call_id.png\"\n  printf '{{\"type\":\"thread.started\",\"thread_id\":\"%s\"}}\\n' \"$thread_id\"\n  printf '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"image_generation_call\",\"id\":\"%s\"}}}}\\n' \"$call_id\"\nfi\n",
                    invocations.display(),
                    invocations.display(),
                    invocations.display(),
                    image.display(),
                )
            })
        }

        fn permanent_http_401() -> Self {
            Self::with_script(|invocations, _image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\n# codex-test-transient-http-401\nprintf '1\\n' >> '{}'\nprintf 'HTTP 401 Unauthorized\\n' >&2\n: > \"$CODEX_HOME/transient-http-401\"\n",
                    invocations.display(),
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

        fn orphan_runtime_output_without_image_event() -> Self {
            Self::with_script(|invocations, image, _root| {
                format!(
                    "#!/bin/sh\nset -eu\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\n/bin/cp '{}' sealed-output.bin\n",
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
            let exec_body = build_script(&invocations, &image, temp.path());
            fs::write(&executable, app_server_fixture_script(exec_body)).unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
            let supervisor = CodexProcessSupervisor::new(
                journal.clone(),
                &executable,
                &executable,
                &credentials,
                &sha256(b"{}"),
                Duration::from_secs(30),
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
