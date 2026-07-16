use std::{env, path::PathBuf, sync::Arc, time::Duration};

use gpt_image_2_gateway::{
    DreaminaCliSubmitCodecV1, DreaminaCliSubmitProcessConfig, GatedCliSubmitDriver,
    ImageGatewayError, PostgresExecutorSubmissionStore, PostgresProviderTaskStore,
    ProviderAccountHomeCapability, ProviderRuntimeProfileStore, ProviderRuntimeRegistration,
    ProviderRuntimeRole, ProviderRuntimeSupervisor, ProviderRuntimeSupervisorConfig,
    ProviderRuntimeSupervisorError, ProviderSubmitDaemon, ProviderSubmitDaemonConfig,
    ProviderSubmitDaemonError, ProviderSubmitService, ProviderSubmitServiceConfig,
    ProviderSubmitServiceError, ProviderTaskStoreError,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
};
use image_cli_runtime::WorkingDirectory;
use image_provider_dreamina_cli::PROVIDER_ID as DREAMINA_PROVIDER_ID;
use uuid::Uuid;

const ACTIVATION_TOKEN: &str = "dreamina-image-submit-v1";
const DEFAULT_PROVIDER_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_EXECUTOR_LEASE_MS: u64 = 60_000;
const DEFAULT_RECOVERY_LEASE_MS: u64 = 60_000;
const DEFAULT_HEARTBEAT_MS: u64 = 10_000;
const DEFAULT_RECOVERY_RETRY_MS: u64 = 1_000;
const DEFAULT_IDLE_DELAY_MS: u64 = 250;
const DEFAULT_ERROR_BASE_DELAY_MS: u64 = 250;
const DEFAULT_ERROR_MAX_DELAY_MS: u64 = 30_000;
const DEFAULT_SHUTDOWN_DRAIN_MS: u64 = 30_000;
const DEFAULT_CLI_WALL_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_CLI_TERMINATION_GRACE_MS: u64 = 2_000;
const DEFAULT_RUNTIME_LEASE_MS: u64 = 60_000;
const DEFAULT_RUNTIME_HEARTBEAT_MS: u64 = 10_000;
const MAX_DURATION_MS: u64 = 24 * 60 * 60 * 1_000;

struct ProviderSubmitterConfig {
    profile_key: String,
    owner_prefix: String,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    account_home: PathBuf,
    workspace_root: PathBuf,
    journal_root: PathBuf,
    executable: PathBuf,
    executable_sha256: [u8; 32],
    runner: PathBuf,
    runner_sha256: String,
    provider_timeout_ms: i64,
    executor_lease_ms: i64,
    recovery_lease_ms: i64,
    heartbeat_interval: Duration,
    recovery_retry_after_ms: i64,
    idle_delay: Duration,
    error_base_delay: Duration,
    error_max_delay: Duration,
    shutdown_drain_timeout: Duration,
    cli_wall_timeout: Duration,
    cli_termination_grace: Duration,
    runtime_lease_ms: i64,
    runtime_heartbeat_interval: Duration,
}

impl ProviderSubmitterConfig {
    fn from_env() -> Result<Self, ImageGatewayError> {
        if required_env("PROVIDER_SUBMITTER_ACTIVATION")? != ACTIVATION_TOKEN {
            return Err(ImageGatewayError::config(
                "PROVIDER_SUBMITTER_ACTIVATION does not enable a supported provider runtime",
            ));
        }
        let profile_key = required_env("PROVIDER_SUBMITTER_PROFILE_KEY")?;
        let owner_prefix = optional_env("PROVIDER_SUBMITTER_OWNER_PREFIX")
            .unwrap_or_else(|| format!("provider-submitd-{}", Uuid::new_v4().simple()));
        if owner_prefix.len() > 80 || !owner_prefix.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(ImageGatewayError::config(
                "PROVIDER_SUBMITTER_OWNER_PREFIX must contain at most 80 visible ASCII bytes",
            ));
        }
        let credential_pool_id = env_uuid("PROVIDER_SUBMITTER_CREDENTIAL_POOL_ID")?;
        let provider_account_id = env_uuid("PROVIDER_SUBMITTER_ACCOUNT_ID")?;
        let credential_ref = required_env("PROVIDER_SUBMITTER_CREDENTIAL_REF")?;
        let credential_revision = positive_i64("PROVIDER_SUBMITTER_CREDENTIAL_REVISION")?;
        let credential_auth_sha256 = lower_sha256_env("PROVIDER_SUBMITTER_CREDENTIAL_AUTH_SHA256")?;
        let account_home = absolute_env_path("PROVIDER_SUBMITTER_ACCOUNT_HOME")?;
        let workspace_root = absolute_env_path("PROVIDER_SUBMITTER_WORKSPACE_ROOT")?;
        let journal_root = absolute_env_path("PROVIDER_SUBMITTER_JOURNAL_ROOT")?;
        let executable = absolute_env_path("PROVIDER_SUBMITTER_EXECUTABLE")?;
        let executable_sha256 = sha256_bytes("PROVIDER_SUBMITTER_EXECUTABLE_SHA256")?;
        let runner = absolute_env_path("PROVIDER_SUBMITTER_RUNNER")?;
        let runner_sha256 = lower_sha256_env("PROVIDER_SUBMITTER_RUNNER_SHA256")?;
        let provider_timeout_ms = duration_env(
            "PROVIDER_SUBMITTER_PROVIDER_TIMEOUT_MS",
            DEFAULT_PROVIDER_TIMEOUT_MS,
        )?;
        let executor_lease_ms = duration_env(
            "PROVIDER_SUBMITTER_EXECUTOR_LEASE_MS",
            DEFAULT_EXECUTOR_LEASE_MS,
        )?;
        let recovery_lease_ms = duration_env(
            "PROVIDER_SUBMITTER_RECOVERY_LEASE_MS",
            DEFAULT_RECOVERY_LEASE_MS,
        )?;
        let heartbeat_ms = duration_env(
            "PROVIDER_SUBMITTER_HEARTBEAT_INTERVAL_MS",
            DEFAULT_HEARTBEAT_MS,
        )?;
        let recovery_retry_ms = duration_env(
            "PROVIDER_SUBMITTER_RECOVERY_RETRY_MS",
            DEFAULT_RECOVERY_RETRY_MS,
        )?;
        let idle_delay_ms =
            duration_env("PROVIDER_SUBMITTER_IDLE_DELAY_MS", DEFAULT_IDLE_DELAY_MS)?;
        let error_base_delay_ms = duration_env(
            "PROVIDER_SUBMITTER_ERROR_BASE_DELAY_MS",
            DEFAULT_ERROR_BASE_DELAY_MS,
        )?;
        let error_max_delay_ms = duration_env(
            "PROVIDER_SUBMITTER_ERROR_MAX_DELAY_MS",
            DEFAULT_ERROR_MAX_DELAY_MS,
        )?;
        let shutdown_drain_ms = duration_env(
            "PROVIDER_SUBMITTER_SHUTDOWN_DRAIN_MS",
            DEFAULT_SHUTDOWN_DRAIN_MS,
        )?;
        let cli_wall_timeout_ms = duration_env(
            "PROVIDER_SUBMITTER_CLI_WALL_TIMEOUT_MS",
            DEFAULT_CLI_WALL_TIMEOUT_MS,
        )?;
        let cli_termination_grace_ms = duration_env(
            "PROVIDER_SUBMITTER_CLI_TERMINATION_GRACE_MS",
            DEFAULT_CLI_TERMINATION_GRACE_MS,
        )?;
        let runtime_lease_ms = duration_env(
            "PROVIDER_SUBMITTER_RUNTIME_LEASE_MS",
            DEFAULT_RUNTIME_LEASE_MS,
        )?;
        let runtime_heartbeat_ms = duration_env(
            "PROVIDER_SUBMITTER_RUNTIME_HEARTBEAT_INTERVAL_MS",
            DEFAULT_RUNTIME_HEARTBEAT_MS,
        )?;
        if heartbeat_ms.saturating_mul(3) > executor_lease_ms
            || heartbeat_ms.saturating_mul(3) > recovery_lease_ms
            || runtime_heartbeat_ms.saturating_mul(3) > runtime_lease_ms
            || error_base_delay_ms > error_max_delay_ms
        {
            return Err(ImageGatewayError::config(
                "provider-submitd bounded configuration is invalid",
            ));
        }
        Ok(Self {
            profile_key,
            owner_prefix,
            credential_pool_id,
            provider_account_id,
            credential_ref,
            credential_revision,
            credential_auth_sha256,
            account_home,
            workspace_root,
            journal_root,
            executable,
            executable_sha256,
            runner,
            runner_sha256,
            provider_timeout_ms: to_i64(provider_timeout_ms, "provider timeout")?,
            executor_lease_ms: to_i64(executor_lease_ms, "executor lease")?,
            recovery_lease_ms: to_i64(recovery_lease_ms, "recovery lease")?,
            heartbeat_interval: Duration::from_millis(heartbeat_ms),
            recovery_retry_after_ms: to_i64(recovery_retry_ms, "recovery retry")?,
            idle_delay: Duration::from_millis(idle_delay_ms),
            error_base_delay: Duration::from_millis(error_base_delay_ms),
            error_max_delay: Duration::from_millis(error_max_delay_ms),
            shutdown_drain_timeout: Duration::from_millis(shutdown_drain_ms),
            cli_wall_timeout: Duration::from_millis(cli_wall_timeout_ms),
            cli_termination_grace: Duration::from_millis(cli_termination_grace_ms),
            runtime_lease_ms: to_i64(runtime_lease_ms, "runtime lease")?,
            runtime_heartbeat_interval: Duration::from_millis(runtime_heartbeat_ms),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = ProviderSubmitterConfig::from_env()?;
    let telemetry = init_telemetry()?;
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let provider_store = PostgresProviderTaskStore::new(pool.clone());
    let profile = provider_store
        .load_active_runtime_profile(&config.profile_key)
        .await
        .map_err(map_profile_error)?;
    let account_home = ProviderAccountHomeCapability::new(
        DREAMINA_PROVIDER_ID,
        config.credential_pool_id,
        config.provider_account_id,
        config.credential_ref,
        config.credential_revision,
        config.credential_auth_sha256,
        &config.account_home,
    )
    .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    let workspace_root = WorkingDirectory::new_private(&config.workspace_root)
        .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    let _journal_root = WorkingDirectory::new_private(&config.journal_root)
        .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    validate_isolated_roots(&[
        &config.account_home,
        &config.workspace_root,
        &config.journal_root,
    ])?;
    let codec = DreaminaCliSubmitCodecV1::from_runtime_profile(
        &profile,
        &account_home,
        DreaminaCliSubmitProcessConfig::new(
            &config.executable,
            config.executable_sha256,
            workspace_root.clone(),
            config.cli_wall_timeout,
            config.cli_termination_grace,
        ),
    )
    .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    let driver = GatedCliSubmitDriver::new(
        codec.clone(),
        &config.runner,
        &config.runner_sha256,
        workspace_root,
    )
    .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    let executor_store = PostgresExecutorSubmissionStore::new(pool);
    let provider_scope = profile.claim_scope();
    let service = Arc::new(
        ProviderSubmitService::new(
            executor_store,
            provider_store.clone(),
            driver,
            codec,
            ProviderSubmitServiceConfig {
                executor_scope: profile.executor_claim_scope(),
                provider_scope: provider_scope.clone(),
                provider_timeout_ms: config.provider_timeout_ms,
                executor_lease_ms: config.executor_lease_ms,
                recovery_lease_ms: config.recovery_lease_ms,
                heartbeat_interval: config.heartbeat_interval,
                recovery_retry_after_ms: config.recovery_retry_after_ms,
            },
            &config.journal_root,
        )
        .map_err(map_service_error)?,
    );
    let daemon = ProviderSubmitDaemon::new(
        service,
        ProviderSubmitDaemonConfig {
            max_in_flight: profile.max_in_flight(),
            owner_prefix: config.owner_prefix.clone(),
            idle_delay: config.idle_delay,
            error_base_delay: config.error_base_delay,
            error_max_delay: config.error_max_delay,
            shutdown_drain_timeout: config.shutdown_drain_timeout,
        },
    )
    .map_err(map_daemon_error)?;
    let runtime = ProviderRuntimeSupervisor::new(
        provider_store,
        ProviderRuntimeRegistration {
            runtime_id: Uuid::new_v4(),
            execution_profile_id: profile.execution_profile_id(),
            role: ProviderRuntimeRole::Submit,
            runtime_owner: config.owner_prefix.clone(),
        },
        ProviderRuntimeSupervisorConfig {
            lease_ms: config.runtime_lease_ms,
            heartbeat_interval: config.runtime_heartbeat_interval,
        },
    );

    let report = runtime
        .run_until_shutdown(shutdown_signal(), |shutdown| {
            tracing::info!(
                owner.prefix = %config.owner_prefix,
                execution.profile.id = %profile.execution_profile_id(),
                execution.profile.key = %profile.profile_key(),
                provider.id = %provider_scope.provider_id,
                provider.account.id = %provider_scope.provider_account_id,
                max.in_flight = profile.max_in_flight(),
                "provider-submitd started"
            );
            daemon.run_until_shutdown(shutdown.wait())
        })
        .await
        .map_err(map_runtime_error)?;
    tracing::info!(
        deadline.resolved = report.deadline_resolved,
        fresh.submitted = report.fresh_submitted,
        projection.rejected = report.fresh_projection_rejected,
        recovery.completed = report.recovery_completed,
        recovery.deferred = report.recovery_deferred,
        idle = report.idle,
        errors = report.errors,
        "provider-submitd stopped"
    );
    telemetry.shutdown();
    Ok(())
}

fn validate_isolated_roots(roots: &[&std::path::Path]) -> Result<(), ImageGatewayError> {
    let roots = roots
        .iter()
        .map(|path| {
            std::fs::canonicalize(path)
                .map_err(|_| ImageGatewayError::config("provider-submitd root is unavailable"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for left in 0..roots.len() {
        for right in left + 1..roots.len() {
            if roots[left].starts_with(&roots[right]) || roots[right].starts_with(&roots[left]) {
                return Err(ImageGatewayError::config(
                    "provider-submitd roots must be separate directory trees",
                ));
            }
        }
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, ImageGatewayError> {
    optional_env(name).ok_or_else(|| ImageGatewayError::config(format!("{name} is required")))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
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

fn env_uuid(name: &str) -> Result<Uuid, ImageGatewayError> {
    required_env(name)?
        .parse::<Uuid>()
        .ok()
        .filter(|value| !value.is_nil())
        .ok_or_else(|| ImageGatewayError::config(format!("{name} must be a non-nil UUID")))
}

fn positive_i64(name: &str) -> Result<i64, ImageGatewayError> {
    required_env(name)?
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ImageGatewayError::config(format!("{name} must be positive")))
}

fn lower_sha256_env(name: &str) -> Result<String, ImageGatewayError> {
    let value = required_env(name)?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ImageGatewayError::config(format!(
            "{name} must be a lower-case SHA-256 value"
        )));
    }
    Ok(value)
}

fn sha256_bytes(name: &str) -> Result<[u8; 32], ImageGatewayError> {
    let value = lower_sha256_env(name)?;
    let mut digest = [0_u8; 32];
    hex::decode_to_slice(value, &mut digest)
        .map_err(|_| ImageGatewayError::config(format!("{name} is invalid")))?;
    Ok(digest)
}

fn duration_env(name: &str, default: u64) -> Result<u64, ImageGatewayError> {
    let value = optional_env(name)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        })
        .unwrap_or(Ok(default))?;
    if value == 0 || value > MAX_DURATION_MS {
        return Err(ImageGatewayError::config(format!(
            "{name} must be between 1 and {MAX_DURATION_MS}"
        )));
    }
    Ok(value)
}

fn to_i64(value: u64, name: &str) -> Result<i64, ImageGatewayError> {
    i64::try_from(value).map_err(|_| ImageGatewayError::config(format!("{name} is too large")))
}

fn map_daemon_error(error: ProviderSubmitDaemonError) -> ImageGatewayError {
    match error {
        ProviderSubmitDaemonError::InvalidConfiguration => {
            ImageGatewayError::config(error.to_string())
        }
        ProviderSubmitDaemonError::LaneTerminated
        | ProviderSubmitDaemonError::ShutdownDrainTimedOut => {
            ImageGatewayError::service_unavailable(error.to_string())
        }
    }
}

fn map_runtime_error(
    error: ProviderRuntimeSupervisorError<ProviderSubmitDaemonError>,
) -> ImageGatewayError {
    match error {
        ProviderRuntimeSupervisorError::InvalidConfiguration => {
            ImageGatewayError::config(error.to_string())
        }
        ProviderRuntimeSupervisorError::Runtime(error) => map_daemon_error(error),
        ProviderRuntimeSupervisorError::Registration(ProviderTaskStoreError::InvalidInput)
        | ProviderRuntimeSupervisorError::Registration(ProviderTaskStoreError::Conflict)
        | ProviderRuntimeSupervisorError::Registration(ProviderTaskStoreError::NotFound)
        | ProviderRuntimeSupervisorError::Registration(ProviderTaskStoreError::StaleLease) => {
            ImageGatewayError::config(
                "provider-submitd runtime identity is unavailable or incompatible",
            )
        }
        ProviderRuntimeSupervisorError::Registration(ProviderTaskStoreError::Unavailable)
        | ProviderRuntimeSupervisorError::Heartbeat(_)
        | ProviderRuntimeSupervisorError::Drain(_)
        | ProviderRuntimeSupervisorError::Withdraw(_) => {
            ImageGatewayError::service_unavailable(error.to_string())
        }
    }
}

fn map_service_error(error: ProviderSubmitServiceError) -> ImageGatewayError {
    match error {
        ProviderSubmitServiceError::InvalidConfiguration => {
            ImageGatewayError::config(error.to_string())
        }
        ProviderSubmitServiceError::Executor(_)
        | ProviderSubmitServiceError::Provider(_)
        | ProviderSubmitServiceError::Orchestrator(_) => {
            ImageGatewayError::service_unavailable(error.to_string())
        }
    }
}

fn map_profile_error(error: ProviderTaskStoreError) -> ImageGatewayError {
    match error {
        ProviderTaskStoreError::Unavailable => {
            ImageGatewayError::service_unavailable(error.to_string())
        }
        ProviderTaskStoreError::InvalidInput
        | ProviderTaskStoreError::Conflict
        | ProviderTaskStoreError::NotFound
        | ProviderTaskStoreError::StaleLease => ImageGatewayError::config(
            "PROVIDER_SUBMITTER_PROFILE_KEY is unavailable or incompatible",
        ),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::{fs::PermissionsExt, fs::symlink},
    };

    use super::*;

    #[test]
    fn account_workspace_and_journal_roots_must_be_separate_trees() {
        let root = tempfile::tempdir().unwrap();
        let account = root.path().join("account");
        let workspace = root.path().join("workspace");
        let journal = root.path().join("journal");
        for path in [&account, &workspace, &journal] {
            fs::create_dir(path).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        assert!(validate_isolated_roots(&[&account, &workspace, &journal]).is_ok());

        let nested = account.join("nested");
        fs::create_dir(&nested).unwrap();
        assert!(validate_isolated_roots(&[&account, &workspace, &nested]).is_err());

        let alias = root.path().join("account-alias");
        symlink(&account, &alias).unwrap();
        assert!(validate_isolated_roots(&[&account, &workspace, &alias]).is_err());
    }
}
