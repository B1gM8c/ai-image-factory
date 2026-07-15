use std::{
    env, fs,
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, process::Command, time::Instant};
use uuid::Uuid;

use super::{
    CodexOutputRequest, ExecutorLaunchContext, ExecutorSubmissionLease, RunnerError,
    SingleOutputSupervisor, project_codex_output_request,
};
use crate::{
    ImageGatewayError, ProxyConfig,
    generator::GenerationJob,
    providers::openai_codex::{build_codex_prompt, final_output_filename, read_codex_output},
    runner::{
        FilesystemRunnerJournal, LaunchDecision,
        process::{
            ExecutionSpool, ProcessObservation, ProcessSpoolError, ProcessTerminal,
            ProviderProcessIdentity, RunnerLock, sha256,
        },
    },
};

pub const CODEX_GENERATION_ADAPTER_REVISION: &str = "openai-codex-generation-v1";
const AUTH_FILE: &str = "auth.json";
const MAX_AUTH_BYTES: u64 = 1024 * 1024;
const MAX_EXECUTABLE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_RUNNER_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const CHILD_REAP_TIMEOUT: Duration = Duration::from_secs(5);

pub struct CodexProcessSupervisor {
    journal: Arc<FilesystemRunnerJournal>,
    helper_executable: PathBuf,
    codex_executable: PathBuf,
    codex_executable_sha256: String,
    credential_auth_file: PathBuf,
    credential_auth_sha256: String,
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

struct ProcessGroupGuard {
    identity: Option<ProviderProcessIdentity>,
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
        let codex_executable_sha256 = hash_bounded_file(&codex_executable)?;
        let credential_auth_file =
            validate_auth_source(credential_home.as_ref(), credential_auth_sha256)?;
        Ok(Self {
            journal,
            helper_executable,
            codex_executable,
            codex_executable_sha256,
            credential_auth_file,
            credential_auth_sha256: credential_auth_sha256.to_string(),
            request_timeout,
            poll_interval,
            startup_grace,
            child_env: child_environment(proxy),
        })
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

#[async_trait]
impl SingleOutputSupervisor for CodexProcessSupervisor {
    async fn prepare(
        &self,
        lease: &ExecutorSubmissionLease,
        context: &ExecutorLaunchContext,
    ) -> Result<(), RunnerError> {
        let request = self.child_request(lease, context)?;
        let bytes = serde_json::to_vec(&request).map_err(|_| RunnerError::Internal)?;
        let spool = ExecutionSpool::for_lease(&self.journal, lease).map_err(map_spool_error)?;
        prepare_isolated_auth(
            spool.codex_home_path().map_err(map_spool_error)?,
            &self.credential_auth_file,
            &self.credential_auth_sha256,
        )
        .map_err(|_| RunnerError::Unavailable)?;
        spool.prepare_request(&bytes).map_err(map_spool_error)
    }

    async fn start_or_attach(
        &self,
        lease: &ExecutorSubmissionLease,
        decision: LaunchDecision,
    ) -> Result<Vec<u8>, RunnerError> {
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
    let spool = ExecutionSpool::open(runner_root.as_ref(), executor_execution_id)
        .map_err(child_spool_error)?;
    let runner_lock = spool.acquire_runner_lock().map_err(child_spool_error)?;
    let identity = runner_lock.identity().map_err(child_spool_error)?;
    spool
        .publish_process(&runner_lock, &identity)
        .map_err(child_spool_error)?;
    let outcome = run_codex_child(&spool, &runner_lock, &identity, executor_execution_id).await;
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
                    },
                )
                .map_err(child_spool_error)?;
        }
        ChildOutcome::Failed(error_code) => spool
            .publish_terminal(
                &runner_lock,
                &ProcessTerminal::Failed {
                    helper_nonce: identity.nonce.clone(),
                    error_code: error_code.to_string(),
                },
            )
            .map_err(child_spool_error)?,
        ChildOutcome::Uncertain(error_code) => spool
            .publish_terminal(
                &runner_lock,
                &ProcessTerminal::Uncertain {
                    helper_nonce: identity.nonce.clone(),
                    error_code: error_code.to_string(),
                },
            )
            .map_err(child_spool_error)?,
    }
    drop(runner_lock);
    Ok(())
}

async fn run_codex_child(
    spool: &ExecutionSpool,
    runner_lock: &RunnerLock,
    helper: &crate::runner::process::ProcessIdentity,
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
    let job = generation_job(&request.output);
    let prompt = build_codex_prompt(&job, workspace, request.output.candidate_index);
    let mut command = Command::new(&request.codex_executable);
    command
        .arg("exec")
        .arg("--ephemeral")
        .arg("--ignore-user-config")
        .arg("--ignore-rules")
        .arg("--disable")
        .arg("plugins")
        .arg("--disable")
        .arg("apps")
        .arg("--sandbox")
        .arg("workspace-write")
        .arg("--skip-git-repo-check")
        .arg("--cd")
        .arg(workspace)
        .arg("-")
        .current_dir(workspace)
        .env_clear()
        .env("HOME", codex_home)
        .env("CODEX_HOME", codex_home)
        .env("TMPDIR", workspace)
        .env("PATH", "/usr/bin:/bin")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    copy_allowed_child_env(&mut command);
    configure_process_group(&mut command);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => return ChildOutcome::Failed("codex_cli_unavailable"),
    };
    let Some(provider_pid) = child.id() else {
        let _ = child.start_kill();
        let _ = tokio::time::timeout(CHILD_REAP_TIMEOUT, child.wait()).await;
        return ChildOutcome::Uncertain("codex_process_identity_unavailable");
    };
    let provider = match ProviderProcessIdentity::capture(provider_pid, &helper.nonce) {
        Ok(provider) => provider,
        Err(_) => {
            let _ = child.start_kill();
            let _ = tokio::time::timeout(CHILD_REAP_TIMEOUT, child.wait()).await;
            return ChildOutcome::Uncertain("codex_process_identity_unavailable");
        }
    };
    let mut process_group = ProcessGroupGuard {
        identity: Some(provider.clone()),
    };
    if spool
        .publish_provider_process(runner_lock, helper, &provider)
        .is_err()
    {
        process_group.kill();
        let _ = child.start_kill();
        let _ = tokio::time::timeout(CHILD_REAP_TIMEOUT, child.wait()).await;
        return ChildOutcome::Uncertain("codex_process_identity_unavailable");
    }
    let Some(mut stdin) = child.stdin.take() else {
        return ChildOutcome::Uncertain("codex_stdin_unavailable");
    };
    if stdin.write_all(prompt.as_bytes()).await.is_err() {
        process_group.kill();
        return ChildOutcome::Uncertain("codex_stdin_failed");
    }
    drop(stdin);
    let timeout = Duration::from_millis(request.timeout_ms);
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => return ChildOutcome::Uncertain("codex_wait_failed"),
        Err(_) => {
            process_group.kill();
            let _ = child.start_kill();
            let _ = tokio::time::timeout(CHILD_REAP_TIMEOUT, child.wait()).await;
            return ChildOutcome::Uncertain("codex_timeout");
        }
    };
    process_group.kill();
    if !status.success() {
        return ChildOutcome::Failed("codex_cli_failed");
    }
    let output = workspace.join(final_output_filename(&request.output.output_format));
    match read_codex_output(&output).await {
        Ok(bytes) => ChildOutcome::Succeeded(bytes),
        Err(_) => ChildOutcome::Failed("codex_no_image_output"),
    }
}

fn validate_child_request(
    request: &CodexChildRequest,
    executor_execution_id: Uuid,
) -> Result<(), ImageGatewayError> {
    if request.schema_version != 1
        || request.adapter_revision != CODEX_GENERATION_ADAPTER_REVISION
        || request.executor_execution_id != executor_execution_id.to_string()
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
    Ok(())
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

fn prepare_isolated_auth(
    destination_home: &Path,
    source: &Path,
    expected_sha256: &str,
) -> std::io::Result<()> {
    let destination = destination_home.join(AUTH_FILE);
    match fs::symlink_metadata(&destination) {
        Ok(metadata) => {
            validate_private_file_metadata(&metadata)?;
            let bytes = read_private_auth(&destination)?;
            return ensure_auth_digest(&bytes, expected_sha256);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let bytes = read_private_auth(source)?;
    ensure_auth_digest(&bytes, expected_sha256)?;
    let mut destination_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&destination)?;
    destination_file.write_all(&bytes)?;
    destination_file.sync_all()?;
    fs::File::open(destination_home)?.sync_all()
}

fn validate_auth_source(home: &Path, expected_sha256: &str) -> Result<PathBuf, ImageGatewayError> {
    if !home.is_absolute() {
        return Err(ImageGatewayError::config(
            "EXECUTOR_CODEX_CREDENTIAL_HOME must be absolute",
        ));
    }
    if !is_sha256(expected_sha256) {
        return Err(ImageGatewayError::config(
            "database credential auth digest is invalid",
        ));
    }
    let source = home.join(AUTH_FILE);
    let bytes = read_private_auth(&source).map_err(|_| {
        ImageGatewayError::config("EXECUTOR_CODEX_CREDENTIAL_HOME/auth.json is invalid")
    })?;
    if sha256(&bytes) != expected_sha256 {
        return Err(ImageGatewayError::config(
            "EXECUTOR_CODEX_CREDENTIAL_HOME/auth.json does not match the database credential",
        ));
    }
    Ok(source)
}

fn read_private_auth(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut file = open_private_file(path)?;
    let size = file.metadata()?.len();
    if size == 0 || size > MAX_AUTH_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid auth file",
        ));
    }
    let mut bytes = Vec::with_capacity(size as usize);
    Read::by_ref(&mut file)
        .take(MAX_AUTH_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "auth file changed",
        ));
    }
    Ok(bytes)
}

fn ensure_auth_digest(bytes: &[u8], expected_sha256: &str) -> std::io::Result<()> {
    if sha256(bytes) != expected_sha256 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "auth file digest mismatch",
        ));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn open_private_file(path: &Path) -> std::io::Result<fs::File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    validate_private_file_metadata(&file.metadata()?)?;
    Ok(file)
}

fn validate_private_file_metadata(metadata: &fs::Metadata) -> std::io::Result<()> {
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private file validation failed",
        ));
    }
    Ok(())
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

fn copy_allowed_child_env(command: &mut Command) {
    for name in [
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
    ] {
        if let Ok(value) = env::var(name) {
            command.env(name, value);
        }
    }
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

impl ProcessGroupGuard {
    fn kill(&mut self) {
        if let Some(identity) = self.identity.take() {
            let _ = identity.kill_process_group_if_current();
        }
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

fn configure_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use tempfile::TempDir;

    use super::*;
    use crate::admission::{
        GENERATION_COMMAND_SCHEMA, GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION,
        GenerationCommandV1,
    };

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
        let replay = fixture
            .supervisor
            .start_or_attach(&lease, LaunchDecision::Attach)
            .await
            .unwrap();

        assert_eq!(first, replay);
        assert_eq!(fs::read_to_string(&fixture.invocations).unwrap(), "1\n");
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
                    "#!/bin/sh\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\n/bin/cp '{}' final.png\n",
                    invocations.display(),
                    image.display()
                )
            })
        }

        fn slow() -> Self {
            Self::with_script(|invocations, image, root| {
                format!(
                    "#!/bin/sh\n/bin/cat >/dev/null\nprintf '1\\n' >> '{}'\nprintf 'started\\n' > '{}'\n/bin/sleep 30\nprintf 'completed\\n' > '{}'\n/bin/cp '{}' final.png\n",
                    invocations.display(),
                    root.join("provider-started").display(),
                    root.join("provider-completed").display(),
                    image.display()
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
            ExecutorLaunchContext {
                request_id: "request-1".to_string(),
                api_profile: command.source_api_profile.clone(),
                output_index: lease.output_index,
                command_schema: lease.command_schema.clone(),
                command_hash: lease.command_hash.clone(),
                command_json: serde_json::to_value(command).unwrap(),
            }
        }
    }
}
