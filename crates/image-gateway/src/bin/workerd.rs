use std::{env, sync::Arc, time::Duration};

use gpt_image_2_gateway::{
    AppConfig, CODEX_GENERATION_ADAPTER_REVISION, CodexImageGenerator,
    ExecutorExecutionProfileStore, ImageGatewayError, PostgresExecutionContextStore,
    PostgresExecutionSettlementStore, PostgresExecutorSubmissionStore, Workerd,
    admission::{GENERATION_COMMAND_SCHEMA, PostgresAdmissionStore},
    artifacts::{
        FilesystemArtifactBlobStore, artifact_root_from_env, validate_artifact_root_isolated,
    },
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
};
use image_provider_contracts::openai_codex;

const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
const DEFAULT_HANDOFF_LEASE_MS: u64 = 60_000;
const SHUTDOWN_DRAIN_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum WorkerExecutionMode {
    LegacyInline,
    ExecutorHandoff,
}

impl WorkerExecutionMode {
    fn from_env() -> Result<Self, ImageGatewayError> {
        match optional_env("WORKER_EXECUTION_MODE").as_deref() {
            None | Some("legacy-inline") => Ok(Self::LegacyInline),
            Some("executor-handoff") => Ok(Self::ExecutorHandoff),
            Some(_) => Err(ImageGatewayError::config(
                "WORKER_EXECUTION_MODE must be legacy-inline or executor-handoff",
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::LegacyInline => "legacy-inline",
            Self::ExecutorHandoff => "executor-handoff",
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = AppConfig::from_env()?;
    let execution_mode = WorkerExecutionMode::from_env()?;
    let telemetry = init_telemetry()?;
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let admission = Arc::new(PostgresAdmissionStore::new(pool.clone()));
    let contexts = Arc::new(PostgresExecutionContextStore::new(pool.clone()));
    let worker_id = optional_env("WORKER_ID")
        .unwrap_or_else(|| format!("workerd-{}", uuid::Uuid::new_v4().simple()));
    let (workerd, shutdown_drain_timeout) = match execution_mode {
        WorkerExecutionMode::LegacyInline => {
            config.validate_worker_startup()?;
            let artifact_root = artifact_root_from_env()?;
            let codex_home = config
                .codex_home
                .as_deref()
                .ok_or_else(|| ImageGatewayError::config("GATEWAY_CODEX_HOME is required"))?;
            validate_artifact_root_isolated(&artifact_root, std::path::Path::new(codex_home))?;
            let artifact_store = Arc::new(FilesystemArtifactBlobStore::new(artifact_root)?);
            let settlement = Arc::new(PostgresExecutionSettlementStore::new(
                pool.clone(),
                artifact_store.clone(),
            ));
            let generator = Arc::new(CodexImageGenerator::new(config.clone()));
            let workerd = Workerd::new(
                worker_id.clone(),
                generator,
                admission,
                contexts,
                settlement,
                artifact_store.clone(),
                artifact_store,
                config.request_timeout,
            )?;
            (
                workerd,
                config.request_timeout.saturating_add(SHUTDOWN_DRAIN_GRACE),
            )
        }
        WorkerExecutionMode::ExecutorHandoff => {
            let profile_key = optional_env("EXECUTOR_PROFILE_KEY").ok_or_else(|| {
                ImageGatewayError::config(
                    "EXECUTOR_PROFILE_KEY is required in executor-handoff mode",
                )
            })?;
            let executor_store = Arc::new(PostgresExecutorSubmissionStore::new(pool));
            let profile = executor_store
                .load_execution_profile(&profile_key)
                .await
                .map_err(|_| {
                    ImageGatewayError::config("EXECUTOR_PROFILE_KEY is unavailable to workerd")
                })?;
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
            {
                return Err(ImageGatewayError::config(
                    "workerd executor handoff profile is incompatible",
                ));
            }
            let handoff_lease = Duration::from_millis(handoff_lease_ms()?);
            tracing::info!(
                execution.profile.id = %profile.execution_profile_id,
                execution.profile.key = %profile.profile_key,
                "workerd V2 executor handoff enabled"
            );
            let workerd = Workerd::new_handoff_only(
                worker_id.clone(),
                admission,
                contexts,
                executor_store,
                profile.execution_profile_id,
                handoff_lease,
            )?;
            (workerd, handoff_lease.saturating_add(SHUTDOWN_DRAIN_GRACE))
        }
    };
    let poll_interval = Duration::from_millis(poll_interval_ms()?);
    tracing::info!(%worker_id, execution.mode = execution_mode.as_str(), "workerd started");
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
            Ok(Some(job_id)) => tracing::info!(%job_id, "durable work processed"),
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

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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

fn handoff_lease_ms() -> Result<u64, ImageGatewayError> {
    let lease_ms = env::var("WORKER_HANDOFF_LEASE_MS")
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                ImageGatewayError::config("WORKER_HANDOFF_LEASE_MS must be an integer")
            })
        })
        .unwrap_or(Ok(DEFAULT_HANDOFF_LEASE_MS))?;
    if lease_ms == 0 || lease_ms > 10 * 60 * 1_000 {
        return Err(ImageGatewayError::config(
            "WORKER_HANDOFF_LEASE_MS must be between 1 and 600000",
        ));
    }
    Ok(lease_ms)
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
