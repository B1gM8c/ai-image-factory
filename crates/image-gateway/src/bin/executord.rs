use std::{env, path::PathBuf, sync::Arc, time::Duration};

use gpt_image_2_gateway::executor::{ExecutorDaemon, ExecutorDaemonRun};
use gpt_image_2_gateway::{
    CODEX_GENERATION_ADAPTER_REVISION, CodexProcessSupervisor, ExecutorExecutionProfileStore,
    ExecutorOwnerGuardError, ImageGatewayError, JournaledDurableRunner, PostgresExecutorOwnerGuard,
    PostgresExecutorSubmissionStore, ProxyConfig,
    admission::GENERATION_COMMAND_SCHEMA,
    artifacts::{
        ExecutorArtifactPublisher, FilesystemArtifactBlobStore, artifact_root_from_env,
        validate_artifact_root_isolated,
    },
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
    runner::FilesystemRunnerJournal,
};
use image_provider_contracts::openai_codex;

const DEFAULT_LEASE_MS: u64 = 60_000;
const DEFAULT_HEARTBEAT_MS: u64 = 10_000;
const DEFAULT_POLL_MS: u64 = 250;
const DEFAULT_PROCESS_POLL_MS: u64 = 100;
const DEFAULT_PROCESS_STARTUP_GRACE_MS: u64 = 5_000;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 15 * 60 * 1_000;
const DEFAULT_OWNER_GUARD_TIMEOUT_MS: u64 = 5_000;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

struct ExecutorConfig {
    owner: String,
    profile_key: String,
    credential_ref: String,
    credential_revision: i64,
    runner_root: PathBuf,
    helper_executable: PathBuf,
    codex_executable: PathBuf,
    credential_home: PathBuf,
    lease_ms: i64,
    heartbeat_interval: Duration,
    poll_interval: Duration,
    process_poll_interval: Duration,
    process_startup_grace: Duration,
    request_timeout: Duration,
    owner_guard_timeout: Duration,
    proxy: ProxyConfig,
}

impl ExecutorConfig {
    fn from_env() -> Result<Self, ImageGatewayError> {
        let owner = required_env("EXECUTOR_OWNER")?;
        if owner.len() > 128 || !owner.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(ImageGatewayError::config(
                "EXECUTOR_OWNER must contain at most 128 visible ASCII bytes",
            ));
        }
        let profile_key = required_env("EXECUTOR_PROFILE_KEY")?;
        let credential_ref = required_env("EXECUTOR_CREDENTIAL_REF")?;
        let credential_revision = required_env("EXECUTOR_CREDENTIAL_REVISION")?
            .parse::<i64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                ImageGatewayError::config("EXECUTOR_CREDENTIAL_REVISION must be positive")
            })?;
        let runner_root = absolute_env_path("EXECUTOR_RUNNER_ROOT")?;
        let helper_executable = absolute_env_path("EXECUTOR_HELPER_EXECUTABLE")?;
        let codex_executable = absolute_env_path("EXECUTOR_CODEX_EXECUTABLE")?;
        let credential_home = absolute_env_path("EXECUTOR_CODEX_CREDENTIAL_HOME")?;
        let lease_ms = env_u64("EXECUTOR_LEASE_MS", DEFAULT_LEASE_MS)?;
        let heartbeat_ms = env_u64("EXECUTOR_HEARTBEAT_INTERVAL_MS", DEFAULT_HEARTBEAT_MS)?;
        let poll_ms = env_u64("EXECUTOR_POLL_INTERVAL_MS", DEFAULT_POLL_MS)?;
        let process_poll_ms =
            env_u64("EXECUTOR_PROCESS_POLL_INTERVAL_MS", DEFAULT_PROCESS_POLL_MS)?;
        let process_startup_grace_ms = env_u64(
            "EXECUTOR_PROCESS_STARTUP_GRACE_MS",
            DEFAULT_PROCESS_STARTUP_GRACE_MS,
        )?;
        let request_timeout_ms =
            env_u64("EXECUTOR_REQUEST_TIMEOUT_MS", DEFAULT_REQUEST_TIMEOUT_MS)?;
        let owner_guard_timeout_ms = env_u64(
            "EXECUTOR_OWNER_GUARD_TIMEOUT_MS",
            DEFAULT_OWNER_GUARD_TIMEOUT_MS,
        )?;
        if lease_ms == 0
            || lease_ms > 24 * 60 * 60 * 1_000
            || heartbeat_ms == 0
            || heartbeat_ms.saturating_mul(3) > lease_ms
            || poll_ms == 0
            || process_poll_ms == 0
            || process_startup_grace_ms == 0
            || request_timeout_ms == 0
            || request_timeout_ms > 60 * 60 * 1_000
            || owner_guard_timeout_ms == 0
        {
            return Err(ImageGatewayError::config(
                "executord duration configuration is invalid",
            ));
        }
        Ok(Self {
            owner,
            profile_key,
            credential_ref,
            credential_revision,
            runner_root,
            helper_executable,
            codex_executable,
            credential_home,
            lease_ms: i64::try_from(lease_ms)
                .map_err(|_| ImageGatewayError::config("EXECUTOR_LEASE_MS is too large"))?,
            heartbeat_interval: Duration::from_millis(heartbeat_ms),
            poll_interval: Duration::from_millis(poll_ms),
            process_poll_interval: Duration::from_millis(process_poll_ms),
            process_startup_grace: Duration::from_millis(process_startup_grace_ms),
            request_timeout: Duration::from_millis(request_timeout_ms),
            owner_guard_timeout: Duration::from_millis(owner_guard_timeout_ms),
            proxy: proxy_from_env(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = ExecutorConfig::from_env()?;
    let telemetry = init_telemetry()?;
    let artifact_root = artifact_root_from_env()?;
    let artifacts = Arc::new(FilesystemArtifactBlobStore::new(&artifact_root)?);
    let journal = Arc::new(
        FilesystemRunnerJournal::new(&config.runner_root)
            .map_err(|_| ImageGatewayError::config("EXECUTOR_RUNNER_ROOT is invalid"))?,
    );
    validate_isolated_trees(&artifact_root, &config.runner_root, &config.credential_home)?;
    validate_artifact_root_isolated(&artifact_root, &config.credential_home)?;
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let store = PostgresExecutorSubmissionStore::new(pool.clone());
    let profile = store
        .load_execution_profile(&config.profile_key)
        .await
        .map_err(|_| ImageGatewayError::config("EXECUTOR_PROFILE_KEY is unavailable"))?;
    let operation = openai_codex::operation("images.generations").ok_or_else(|| {
        ImageGatewayError::config("Codex generation operation descriptor is unavailable")
    })?;
    if profile.provider_id != openai_codex::PROVIDER_ID
        || profile.command_schema != GENERATION_COMMAND_SCHEMA
        || profile.operation_id != operation.id
        || profile.operation_descriptor_revision != operation.descriptor_revision
        || profile.operation_descriptor_sha256_v1 != operation.canonical_sha256_v1_hex()
        || profile.completion_mode != operation.completion.as_str()
        || profile.idempotency_mode != operation.idempotency.as_str()
        || profile.adapter_revision != CODEX_GENERATION_ADAPTER_REVISION
        || profile.credential_ref != config.credential_ref
        || profile.credential_revision != config.credential_revision
    {
        return Err(ImageGatewayError::config(
            "executor runtime does not match the selected database profile",
        ));
    }
    let scope = profile.claim_scope();
    let mut owner_guard = PostgresExecutorOwnerGuard::acquire(
        &pool,
        &config.owner,
        &scope,
        config.owner_guard_timeout,
    )
    .await
    .map_err(|error| ImageGatewayError::service_unavailable(error.to_string()))?;
    let supervisor = CodexProcessSupervisor::new(
        journal.clone(),
        &config.helper_executable,
        &config.codex_executable,
        &config.credential_home,
        &profile.credential_auth_sha256,
        config.request_timeout,
        config.process_poll_interval,
        config.process_startup_grace,
        &config.proxy,
    )?;
    let publisher = ExecutorArtifactPublisher::with_filesystem_store(artifacts, store.clone());
    let runner = JournaledDurableRunner::new(store.clone(), journal, supervisor, publisher);
    let daemon = ExecutorDaemon::new(
        store,
        runner,
        scope.clone(),
        config.owner.clone(),
        config.lease_ms,
        config.heartbeat_interval,
    );
    tracing::info!(
        owner = %config.owner,
        execution.profile.id = %scope.execution_profile_id,
        execution.profile.key = %profile.profile_key,
        provider.id = %scope.provider_id,
        command.schema = %scope.command_schema,
        adapter.revision = %scope.adapter_revision,
        "executord started"
    );
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let drain_timeout = config
        .request_timeout
        .saturating_add(config.process_startup_grace)
        .saturating_add(SHUTDOWN_GRACE);

    'executor: loop {
        owner_guard
            .verify()
            .await
            .map_err(|error| ImageGatewayError::service_unavailable(error.to_string()))?;
        let run = async {
            match daemon.recover_evidence_once().await? {
                ExecutorDaemonRun::Recorded => Ok(ExecutorDaemonRun::Recorded),
                ExecutorDaemonRun::Idle => daemon.run_once().await,
            }
        };
        tokio::pin!(run);
        let guard_watch = monitor_owner_guard(&mut owner_guard, config.heartbeat_interval);
        tokio::pin!(guard_watch);
        let (result, shutting_down) = tokio::select! {
            guard = &mut guard_watch => {
                telemetry.shutdown();
                return Err(ImageGatewayError::service_unavailable(guard.to_string()));
            }
            _ = &mut shutdown => {
                tracing::info!("executord draining in-flight execution");
                match tokio::time::timeout(drain_timeout, &mut run).await {
                    Ok(result) => (result, true),
                    Err(_) => {
                        telemetry.shutdown();
                        return Err(ImageGatewayError::service_unavailable(
                            "executord shutdown drain timed out; durable helper evidence was retained",
                        ));
                    }
                }
            }
            result = &mut run => (result, false),
        };
        match result {
            Ok(ExecutorDaemonRun::Recorded) => {
                tracing::info!("executor outcome recorded");
            }
            Ok(ExecutorDaemonRun::Idle) => {}
            Err(error) => tracing::error!(error = ?error, "executor iteration failed"),
        }
        if shutting_down {
            break 'executor;
        }
        tokio::select! {
            _ = &mut shutdown => break 'executor,
            _ = tokio::time::sleep(config.poll_interval) => {}
        }
    }
    telemetry.shutdown();
    Ok(())
}

async fn monitor_owner_guard(
    guard: &mut PostgresExecutorOwnerGuard,
    interval: Duration,
) -> ExecutorOwnerGuardError {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(error) = guard.verify().await {
            return error;
        }
    }
}

fn validate_isolated_trees(
    artifact_root: &std::path::Path,
    runner_root: &std::path::Path,
    credential_home: &std::path::Path,
) -> Result<(), ImageGatewayError> {
    let paths = [artifact_root, runner_root, credential_home]
        .map(|path| {
            std::fs::canonicalize(path)
                .map_err(|_| ImageGatewayError::config("executor filesystem root is invalid"))
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    for left in 0..paths.len() {
        for right in left + 1..paths.len() {
            if paths[left].starts_with(&paths[right]) || paths[right].starts_with(&paths[left]) {
                return Err(ImageGatewayError::config(
                    "artifact, runner, and credential roots must be separate directory trees",
                ));
            }
        }
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, ImageGatewayError> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ImageGatewayError::config(format!("{name} is required")))
}

fn absolute_env_path(name: &str) -> Result<PathBuf, ImageGatewayError> {
    let path = PathBuf::from(required_env(name)?);
    if !path.is_absolute() {
        return Err(ImageGatewayError::config(format!(
            "{name} must be an absolute path"
        )));
    }
    Ok(path)
}

fn env_u64(name: &str, default: u64) -> Result<u64, ImageGatewayError> {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        })
        .unwrap_or(Ok(default))
}

fn proxy_from_env() -> ProxyConfig {
    ProxyConfig {
        http_proxy: env_value("EXECUTOR_HTTP_PROXY").or_else(|| env_value("HTTP_PROXY")),
        https_proxy: env_value("EXECUTOR_HTTPS_PROXY").or_else(|| env_value("HTTPS_PROXY")),
        all_proxy: env_value("EXECUTOR_ALL_PROXY").or_else(|| env_value("ALL_PROXY")),
        no_proxy: env_value("EXECUTOR_NO_PROXY").or_else(|| env_value("NO_PROXY")),
    }
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree_isolation_rejects_nested_roots() {
        let temp = tempfile::tempdir().unwrap();
        let artifact = temp.path().join("artifact");
        let runner = artifact.join("runner");
        let credentials = temp.path().join("credentials");
        std::fs::create_dir(&artifact).unwrap();
        std::fs::create_dir(&runner).unwrap();
        std::fs::create_dir(&credentials).unwrap();

        assert!(validate_isolated_trees(&artifact, &runner, &credentials).is_err());
    }
}
