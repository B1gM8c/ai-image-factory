use std::{env, sync::Arc, time::Duration};

use gpt_image_2_gateway::{
    AppConfig, CodexImageGenerator, ImageGatewayError, PostgresExecutionContextStore,
    PostgresExecutionSettlementStore, Workerd,
    admission::PostgresAdmissionStore,
    artifacts::{
        FilesystemArtifactBlobStore, artifact_root_from_env, validate_artifact_root_isolated,
    },
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
};

const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
const SHUTDOWN_DRAIN_GRACE: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = AppConfig::from_env()?;
    config.validate_worker_startup()?;
    let artifact_root = artifact_root_from_env()?;
    let codex_home = config
        .codex_home
        .as_deref()
        .ok_or_else(|| ImageGatewayError::config("GATEWAY_CODEX_HOME is required"))?;
    validate_artifact_root_isolated(&artifact_root, std::path::Path::new(codex_home))?;
    let artifact_store = Arc::new(FilesystemArtifactBlobStore::new(artifact_root)?);
    let telemetry = init_telemetry()?;

    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let admission = Arc::new(PostgresAdmissionStore::new(pool.clone()));
    let contexts = Arc::new(PostgresExecutionContextStore::new(pool.clone()));
    let settlement = Arc::new(PostgresExecutionSettlementStore::new(
        pool,
        artifact_store.clone(),
    ));
    let generator = Arc::new(CodexImageGenerator::new(config.clone()));
    let worker_id = env::var("WORKER_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("workerd-{}", uuid::Uuid::new_v4().simple()));
    let poll_interval = Duration::from_millis(poll_interval_ms()?);
    let shutdown_drain_timeout = config.request_timeout.saturating_add(SHUTDOWN_DRAIN_GRACE);
    let workerd = Workerd::new(
        worker_id.clone(),
        generator,
        admission,
        contexts,
        settlement,
        artifact_store,
        config.request_timeout,
    );
    tracing::info!(%worker_id, "workerd started");
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    'worker: loop {
        let run = workerd.run_once();
        tokio::pin!(run);
        let (result, shutting_down) = tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("workerd draining in-flight work");
                match tokio::time::timeout(shutdown_drain_timeout, &mut run).await {
                    Ok(result) => (result, true),
                    Err(_) => {
                        telemetry.shutdown();
                        return Err(ImageGatewayError::service_unavailable(
                            "workerd shutdown drain timed out",
                        ));
                    }
                }
            }
            result = &mut run => (result, false),
        };
        match result {
            Ok(Some(job_id)) => tracing::info!(%job_id, "durable work completed"),
            Ok(None) => {}
            Err(error) => tracing::error!(error = ?error, "durable work execution failed"),
        }
        if shutting_down {
            break 'worker;
        }
        tokio::select! {
            _ = &mut shutdown => break 'worker,
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
    telemetry.shutdown();
    Ok(())
}

fn poll_interval_ms() -> Result<u64, ImageGatewayError> {
    env::var("WORKER_POLL_INTERVAL_MS")
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                ImageGatewayError::config("WORKER_POLL_INTERVAL_MS must be an integer")
            })
        })
        .unwrap_or(Ok(DEFAULT_POLL_INTERVAL_MS))
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
