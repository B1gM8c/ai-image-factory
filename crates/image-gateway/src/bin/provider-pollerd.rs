use std::{env, path::PathBuf, sync::Arc, time::Duration};

use gpt_image_2_gateway::{
    DreaminaCliPollDriverV1, DreaminaCliPollProcessConfig, FilesystemArtifactBlobStore,
    FilesystemProviderArtifactStagerFactory, ImageGatewayError, PostgresProviderTaskStore,
    ProviderAccountHomeCapability, ProviderPollDaemon, ProviderPollDaemonConfig,
    ProviderPollDaemonError, ProviderPollOrchestrator, ProviderPollOrchestratorConfig,
    ProviderRuntimeProfileStore, ProviderRuntimeRegistration, ProviderRuntimeRole,
    ProviderRuntimeSupervisor, ProviderRuntimeSupervisorConfig, ProviderRuntimeSupervisorError,
    ProviderTaskStoreError,
    artifacts::artifact_root_from_env,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry, prepare_dreamina_account_home,
};
use image_cli_runtime::WorkingDirectory;
use image_provider_dreamina_cli::PROVIDER_ID as DREAMINA_PROVIDER_ID;
use uuid::Uuid;

const ACTIVATION_TOKENS: &[&str] = &["dreamina-remote-poll-v1", "dreamina-image-v1"];
const DEFAULT_LEASE_MS: u64 = 60_000;
const DEFAULT_HEARTBEAT_MS: u64 = 10_000;
const DEFAULT_IDLE_DELAY_MS: u64 = 250;
const DEFAULT_ERROR_BASE_DELAY_MS: u64 = 250;
const DEFAULT_ERROR_MAX_DELAY_MS: u64 = 30_000;
const DEFAULT_SHUTDOWN_DRAIN_MS: u64 = 30_000;
const DEFAULT_CLI_WALL_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_CLI_TERMINATION_GRACE_MS: u64 = 2_000;
const DEFAULT_RUNTIME_LEASE_MS: u64 = 60_000;
const DEFAULT_RUNTIME_HEARTBEAT_MS: u64 = 10_000;
const MAX_DURATION_MS: u64 = 24 * 60 * 60 * 1_000;

struct ProviderPollerConfig {
    profile_key: String,
    owner: String,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    account_home: PathBuf,
    workspace_root: PathBuf,
    executable: PathBuf,
    executable_sha256: [u8; 32],
    max_artifact_bytes: u64,
    max_materializations: usize,
    lease_ms: i64,
    heartbeat_interval: Duration,
    idle_delay: Duration,
    error_base_delay: Duration,
    error_max_delay: Duration,
    shutdown_drain_timeout: Duration,
    cli_wall_timeout: Duration,
    cli_termination_grace: Duration,
    runtime_lease_ms: i64,
    runtime_heartbeat_interval: Duration,
}

impl ProviderPollerConfig {
    fn from_env() -> Result<Self, ImageGatewayError> {
        if !ACTIVATION_TOKENS.contains(&required_env("PROVIDER_POLLER_ACTIVATION")?.as_str()) {
            return Err(ImageGatewayError::config(
                "PROVIDER_POLLER_ACTIVATION does not enable a supported provider runtime",
            ));
        }
        let profile_key = required_env("PROVIDER_POLLER_PROFILE_KEY")?;
        let owner = optional_env("PROVIDER_POLLER_OWNER")
            .unwrap_or_else(|| format!("provider-pollerd-{}", Uuid::new_v4().simple()));
        if owner.len() > 255 || !owner.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(ImageGatewayError::config(
                "PROVIDER_POLLER_OWNER must contain at most 255 visible ASCII bytes",
            ));
        }
        let credential_pool_id = env_uuid("PROVIDER_POLLER_CREDENTIAL_POOL_ID")?;
        let provider_account_id = env_uuid("PROVIDER_POLLER_ACCOUNT_ID")?;
        let credential_ref = required_env("PROVIDER_POLLER_CREDENTIAL_REF")?;
        let credential_revision = required_env("PROVIDER_POLLER_CREDENTIAL_REVISION")?
            .parse::<i64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                ImageGatewayError::config("PROVIDER_POLLER_CREDENTIAL_REVISION must be positive")
            })?;
        let credential_auth_sha256 = lower_sha256_env("PROVIDER_POLLER_CREDENTIAL_AUTH_SHA256")?;
        let account_home = absolute_env_path("PROVIDER_POLLER_ACCOUNT_HOME")?;
        let workspace_root = absolute_env_path("PROVIDER_POLLER_WORKSPACE_ROOT")?;
        let executable = absolute_env_path("PROVIDER_POLLER_EXECUTABLE")?;
        let executable_sha256 = sha256_bytes("PROVIDER_POLLER_EXECUTABLE_SHA256")?;
        let max_artifact_bytes = required_u64("PROVIDER_POLLER_MAX_ARTIFACT_BYTES")?;
        let max_materializations =
            required_u64("PROVIDER_POLLER_MAX_MATERIALIZATIONS").and_then(|value| {
                usize::try_from(value).map_err(|_| {
                    ImageGatewayError::config("PROVIDER_POLLER_MAX_MATERIALIZATIONS is too large")
                })
            })?;
        let lease_ms = env_u64("PROVIDER_POLLER_LEASE_MS", DEFAULT_LEASE_MS)?;
        let heartbeat_ms = env_u64(
            "PROVIDER_POLLER_HEARTBEAT_INTERVAL_MS",
            DEFAULT_HEARTBEAT_MS,
        )?;
        let idle_delay_ms = env_u64("PROVIDER_POLLER_IDLE_DELAY_MS", DEFAULT_IDLE_DELAY_MS)?;
        let error_base_delay_ms = env_u64(
            "PROVIDER_POLLER_ERROR_BASE_DELAY_MS",
            DEFAULT_ERROR_BASE_DELAY_MS,
        )?;
        let error_max_delay_ms = env_u64(
            "PROVIDER_POLLER_ERROR_MAX_DELAY_MS",
            DEFAULT_ERROR_MAX_DELAY_MS,
        )?;
        let shutdown_drain_ms = env_u64(
            "PROVIDER_POLLER_SHUTDOWN_DRAIN_MS",
            DEFAULT_SHUTDOWN_DRAIN_MS,
        )?;
        let cli_wall_timeout_ms = env_u64(
            "PROVIDER_POLLER_CLI_WALL_TIMEOUT_MS",
            DEFAULT_CLI_WALL_TIMEOUT_MS,
        )?;
        let cli_termination_grace_ms = env_u64(
            "PROVIDER_POLLER_CLI_TERMINATION_GRACE_MS",
            DEFAULT_CLI_TERMINATION_GRACE_MS,
        )?;
        let runtime_lease_ms =
            env_u64("PROVIDER_POLLER_RUNTIME_LEASE_MS", DEFAULT_RUNTIME_LEASE_MS)?;
        let runtime_heartbeat_ms = env_u64(
            "PROVIDER_POLLER_RUNTIME_HEARTBEAT_INTERVAL_MS",
            DEFAULT_RUNTIME_HEARTBEAT_MS,
        )?;
        let durations = [
            lease_ms,
            heartbeat_ms,
            idle_delay_ms,
            error_base_delay_ms,
            error_max_delay_ms,
            shutdown_drain_ms,
            cli_wall_timeout_ms,
            cli_termination_grace_ms,
            runtime_lease_ms,
            runtime_heartbeat_ms,
        ];
        if durations
            .iter()
            .any(|value| *value == 0 || *value > MAX_DURATION_MS)
            || heartbeat_ms.saturating_mul(3) > lease_ms
            || runtime_heartbeat_ms.saturating_mul(3) > runtime_lease_ms
            || error_base_delay_ms > error_max_delay_ms
            || max_artifact_bytes == 0
            || max_materializations == 0
        {
            return Err(ImageGatewayError::config(
                "provider-pollerd bounded configuration is invalid",
            ));
        }
        Ok(Self {
            profile_key,
            owner,
            credential_pool_id,
            provider_account_id,
            credential_ref,
            credential_revision,
            credential_auth_sha256,
            account_home,
            workspace_root,
            executable,
            executable_sha256,
            max_artifact_bytes,
            max_materializations,
            lease_ms: i64::try_from(lease_ms)
                .map_err(|_| ImageGatewayError::config("PROVIDER_POLLER_LEASE_MS is too large"))?,
            heartbeat_interval: Duration::from_millis(heartbeat_ms),
            idle_delay: Duration::from_millis(idle_delay_ms),
            error_base_delay: Duration::from_millis(error_base_delay_ms),
            error_max_delay: Duration::from_millis(error_max_delay_ms),
            shutdown_drain_timeout: Duration::from_millis(shutdown_drain_ms),
            cli_wall_timeout: Duration::from_millis(cli_wall_timeout_ms),
            cli_termination_grace: Duration::from_millis(cli_termination_grace_ms),
            runtime_lease_ms: i64::try_from(runtime_lease_ms).map_err(|_| {
                ImageGatewayError::config("PROVIDER_POLLER_RUNTIME_LEASE_MS is too large")
            })?,
            runtime_heartbeat_interval: Duration::from_millis(runtime_heartbeat_ms),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = ProviderPollerConfig::from_env()?;
    prepare_dreamina_account_home(&config.account_home)
        .await
        .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    let telemetry = init_telemetry()?;
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let store = PostgresProviderTaskStore::new(pool);
    let profile = store
        .load_active_runtime_profile(&config.profile_key)
        .await
        .map_err(map_profile_error)?;
    if config.max_materializations > profile.max_in_flight() {
        return Err(ImageGatewayError::config(
            "PROVIDER_POLLER_MAX_MATERIALIZATIONS exceeds profile concurrency",
        ));
    }

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
    let artifact_root = artifact_root_from_env()?;
    validate_isolated_roots(&artifact_root, &config.account_home, &config.workspace_root)?;
    let artifacts = Arc::new(FilesystemArtifactBlobStore::new(artifact_root)?);
    let stagers = FilesystemProviderArtifactStagerFactory::new(
        Arc::clone(&artifacts),
        config.max_artifact_bytes,
    )
    .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    let driver = DreaminaCliPollDriverV1::from_runtime_profile(
        &profile,
        &account_home,
        DreaminaCliPollProcessConfig::new(
            &config.executable,
            config.executable_sha256,
            workspace_root,
            config.cli_wall_timeout,
            config.cli_termination_grace,
            config.max_artifact_bytes,
        ),
    )
    .map_err(|error| ImageGatewayError::config(error.to_string()))?;
    let scope = profile.claim_scope();
    let orchestrator = Arc::new(
        ProviderPollOrchestrator::new(
            store.clone(),
            driver,
            stagers,
            ProviderPollOrchestratorConfig {
                scope: scope.clone(),
                owner: config.owner.clone(),
                lease_ms: config.lease_ms,
                heartbeat_interval: config.heartbeat_interval,
                max_materializations: config.max_materializations,
            },
        )
        .map_err(|error| ImageGatewayError::config(error.to_string()))?,
    );
    let daemon = ProviderPollDaemon::new(
        orchestrator,
        ProviderPollDaemonConfig {
            max_in_flight: profile.max_in_flight(),
            idle_delay: config.idle_delay,
            error_base_delay: config.error_base_delay,
            error_max_delay: config.error_max_delay,
            shutdown_drain_timeout: config.shutdown_drain_timeout,
        },
    )
    .map_err(map_daemon_error)?;
    let runtime = ProviderRuntimeSupervisor::new(
        store,
        ProviderRuntimeRegistration {
            runtime_id: Uuid::new_v4(),
            execution_profile_id: profile.execution_profile_id(),
            role: ProviderRuntimeRole::Poll,
            runtime_owner: config.owner.clone(),
        },
        ProviderRuntimeSupervisorConfig {
            lease_ms: config.runtime_lease_ms,
            heartbeat_interval: config.runtime_heartbeat_interval,
        },
    );

    let report = runtime
        .run_until_shutdown(shutdown_signal(), |shutdown| {
            tracing::info!(
                owner = %config.owner,
                execution.profile.id = %profile.execution_profile_id(),
                execution.profile.key = %profile.profile_key(),
                provider.id = %scope.provider_id,
                provider.account.id = %scope.provider_account_id,
                max.in_flight = profile.max_in_flight(),
                max.materializations = config.max_materializations,
                "provider-pollerd started"
            );
            daemon.run_until_shutdown(shutdown.wait())
        })
        .await
        .map_err(map_runtime_error)?;
    tracing::info!(
        observed = report.observed,
        idle = report.idle,
        errors = report.errors,
        "provider-pollerd stopped"
    );
    telemetry.shutdown();
    Ok(())
}

fn validate_isolated_roots(
    artifact_root: &std::path::Path,
    account_home: &std::path::Path,
    workspace_root: &std::path::Path,
) -> Result<(), ImageGatewayError> {
    let roots = [artifact_root, account_home, workspace_root]
        .map(|path| {
            std::fs::canonicalize(path)
                .map_err(|_| ImageGatewayError::config("provider-pollerd root is unavailable"))
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    for left in 0..roots.len() {
        for right in left + 1..roots.len() {
            if roots[left].starts_with(&roots[right]) || roots[right].starts_with(&roots[left]) {
                return Err(ImageGatewayError::config(
                    "provider-pollerd roots must be separate directory trees",
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

fn required_u64(name: &str) -> Result<u64, ImageGatewayError> {
    required_env(name)?
        .parse()
        .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
}

fn env_u64(name: &str, default: u64) -> Result<u64, ImageGatewayError> {
    optional_env(name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        })
        .unwrap_or(Ok(default))
}

fn map_daemon_error(error: ProviderPollDaemonError) -> ImageGatewayError {
    match error {
        ProviderPollDaemonError::InvalidConfiguration => {
            ImageGatewayError::config(error.to_string())
        }
        ProviderPollDaemonError::LaneTerminated
        | ProviderPollDaemonError::ShutdownDrainTimedOut => {
            ImageGatewayError::service_unavailable(error.to_string())
        }
    }
}

fn map_runtime_error(
    error: ProviderRuntimeSupervisorError<ProviderPollDaemonError>,
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
                "provider-pollerd runtime identity is unavailable or incompatible",
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

fn map_profile_error(error: ProviderTaskStoreError) -> ImageGatewayError {
    match error {
        ProviderTaskStoreError::Unavailable => {
            ImageGatewayError::service_unavailable(error.to_string())
        }
        ProviderTaskStoreError::InvalidInput
        | ProviderTaskStoreError::Conflict
        | ProviderTaskStoreError::NotFound
        | ProviderTaskStoreError::StaleLease => {
            ImageGatewayError::config("PROVIDER_POLLER_PROFILE_KEY is unavailable or incompatible")
        }
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
    use std::{fs, os::unix::fs::symlink};

    use super::*;

    #[test]
    fn artifact_account_and_workspace_roots_must_be_separate_trees() {
        let root = tempfile::tempdir().unwrap();
        let artifact = root.path().join("artifact");
        let account = root.path().join("account");
        let workspace = root.path().join("workspace");
        for path in [&artifact, &account, &workspace] {
            fs::create_dir(path).unwrap();
        }
        assert!(validate_isolated_roots(&artifact, &account, &workspace).is_ok());

        let nested = artifact.join("nested-account");
        fs::create_dir(&nested).unwrap();
        assert!(validate_isolated_roots(&artifact, &nested, &workspace).is_err());

        let account_alias = root.path().join("account-alias");
        symlink(&account, &account_alias).unwrap();
        assert!(validate_isolated_roots(&artifact, &account, &account_alias).is_err());
    }
}
