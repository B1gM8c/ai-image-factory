use std::{env, sync::Arc, time::Duration};

use gpt_image_2_gateway::{
    ImageGatewayError,
    artifacts::{FilesystemArtifactBlobStore, artifact_root_from_env},
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
    reduction::{
        CustomerArtifactPublisher, PostgresExecutorTerminalStore, ReducerDaemon, ReducerDaemonError,
    },
};

const DEFAULT_LEASE_MS: u64 = 60_000;
const DEFAULT_HEARTBEAT_MS: u64 = 10_000;
const DEFAULT_POLL_MS: u64 = 250;
const MAX_LEASE_MS: u64 = 10 * 60 * 1_000;
const MAX_POLL_MS: u64 = 60_000;
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

struct ReducerConfig {
    owner: String,
    lease_ms: i64,
    heartbeat_interval: Duration,
    poll_interval: Duration,
}

impl ReducerConfig {
    fn from_env() -> Result<Self, ImageGatewayError> {
        let owner = optional_env("REDUCER_OWNER")
            .unwrap_or_else(|| format!("reducerd-{}", uuid::Uuid::new_v4().simple()));
        if owner.len() > 255 || !owner.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(ImageGatewayError::config(
                "REDUCER_OWNER must contain at most 255 visible ASCII bytes",
            ));
        }
        let lease_ms = env_u64("REDUCER_LEASE_MS", DEFAULT_LEASE_MS)?;
        let heartbeat_ms = env_u64("REDUCER_HEARTBEAT_INTERVAL_MS", DEFAULT_HEARTBEAT_MS)?;
        let poll_ms = env_u64("REDUCER_POLL_INTERVAL_MS", DEFAULT_POLL_MS)?;
        if !(1..=MAX_LEASE_MS).contains(&lease_ms)
            || heartbeat_ms == 0
            || heartbeat_ms.saturating_mul(3) > lease_ms
            || !(1..=MAX_POLL_MS).contains(&poll_ms)
        {
            return Err(ImageGatewayError::config(
                "reducerd duration configuration is invalid",
            ));
        }
        Ok(Self {
            owner,
            lease_ms: i64::try_from(lease_ms)
                .map_err(|_| ImageGatewayError::config("REDUCER_LEASE_MS is too large"))?,
            heartbeat_interval: Duration::from_millis(heartbeat_ms),
            poll_interval: Duration::from_millis(poll_ms),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = ReducerConfig::from_env()?;
    let telemetry = init_telemetry()?;
    let artifact_root = artifact_root_from_env()?;
    let artifacts = Arc::new(FilesystemArtifactBlobStore::new(artifact_root)?);
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let store = PostgresExecutorTerminalStore::new(pool);
    let publisher = CustomerArtifactPublisher::new(artifacts);
    let daemon = ReducerDaemon::new(
        store,
        publisher,
        config.owner.clone(),
        config.lease_ms,
        config.heartbeat_interval,
    );
    tracing::info!(owner = %config.owner, "reducerd started");
    let result = daemon
        .run_until_shutdown(
            shutdown_signal(),
            config.poll_interval,
            SHUTDOWN_DRAIN_TIMEOUT,
        )
        .await;
    telemetry.shutdown();
    result.map_err(map_daemon_error)
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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

fn map_daemon_error(error: ReducerDaemonError) -> ImageGatewayError {
    match error {
        ReducerDaemonError::InvalidConfiguration => ImageGatewayError::config(error.to_string()),
        ReducerDaemonError::Store(_)
        | ReducerDaemonError::Publish(_)
        | ReducerDaemonError::ShutdownDrainTimedOut => {
            ImageGatewayError::service_unavailable(error.to_string())
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
