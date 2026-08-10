use std::{env, sync::Arc, time::Duration};

use tokio::{sync::watch, task::JoinSet};

use gpt_image_2_gateway::admission::AdmissionContract;
use gpt_image_2_gateway::{
    AppConfig, CODEX_EDIT_INLINE_ADAPTER_REVISION, CodexImageGenerator, ExecutorExecutionProfile,
    ExecutorExecutionProfileStore, GenerationAdmissionContract, ImageGatewayError,
    OperationalCredentialResolver, PostgresCredentialStore, PostgresExecutionContextStore,
    PostgresExecutionSettlementStore, PostgresExecutorSubmissionStore, Workerd,
    admission::{EDIT_COMMAND_SCHEMA, PostgresAdmissionStore},
    artifacts::{
        FilesystemArtifactBlobStore, artifact_root_from_env, validate_artifact_root_isolated,
    },
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    identify_executor_profile_binding, init_telemetry,
};
use image_provider_contracts::openai_codex;
use image_provider_dreamina_cli::{
    ADAPTER_REVISION as DREAMINA_ADAPTER_REVISION, DREAMINA_IMAGE_GENERATION_OPERATION_V1,
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DREAMINA_VIDEO_GENERATION_OPERATION_V1,
    PROVIDER_ID as DREAMINA_PROVIDER_ID,
};

const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
const DEFAULT_HANDOFF_LEASE_MS: u64 = 60_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 1;
const MAX_WORKER_LANES: usize = 64;
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
    let configured_max_in_flight = worker_max_in_flight()?;
    let (workerd, shutdown_drain_timeout, max_in_flight) = match execution_mode {
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
            let (workerd, max_in_flight) = match config.generation_admission_contract {
                GenerationAdmissionContract::CustomerPricingV4 => {
                    let profile_key = optional_env("EXECUTOR_PROFILE_KEY").ok_or_else(|| {
                        ImageGatewayError::config(
                            "EXECUTOR_PROFILE_KEY is required for inline V4 edits",
                        )
                    })?;
                    let executor_store = PostgresExecutorSubmissionStore::new(pool.clone());
                    let profile = executor_store
                        .load_execution_profile(&profile_key)
                        .await
                        .map_err(|_| {
                            ImageGatewayError::config(
                                "EXECUTOR_PROFILE_KEY is unavailable to workerd",
                            )
                        })?;
                    validate_inline_edit_profile(&profile).map_err(|_| {
                        ImageGatewayError::config("workerd inline edit profile is incompatible")
                    })?;
                    let operational_credential = PostgresCredentialStore::new(pool.clone())
                        .resolve(profile.provider_account_id)
                        .await
                        .map_err(|_| {
                            ImageGatewayError::config(
                                "workerd inline edit credential is unavailable",
                            )
                        })?;
                    if operational_credential.provider_id != openai_codex::PROVIDER_ID
                        || operational_credential.home() != std::path::Path::new(codex_home)
                    {
                        return Err(ImageGatewayError::config(
                            "workerd inline edit credential environment does not match its profile",
                        ));
                    }
                    tracing::info!(
                        execution.profile.id = %profile.execution_profile_id,
                        execution.profile.key = %profile.profile_key,
                        "workerd inline edit profile enabled"
                    );
                    let profile_max_in_flight =
                        usize::try_from(profile.max_concurrency).map_err(|_| {
                            ImageGatewayError::config(
                                "workerd inline edit profile concurrency is invalid",
                            )
                        })?;
                    let max_in_flight = configured_max_in_flight.min(profile_max_in_flight);
                    if max_in_flight == 0 {
                        return Err(ImageGatewayError::config(
                            "workerd inline edit concurrency is unavailable",
                        ));
                    }
                    (
                        workerd.with_claim_profile(
                            AdmissionContract::CustomerPricingV4,
                            EDIT_COMMAND_SCHEMA,
                            profile.execution_profile_id,
                        )?,
                        max_in_flight,
                    )
                }
                _ if configured_max_in_flight == 1 => (workerd, 1),
                _ => {
                    return Err(ImageGatewayError::config(
                        "WORKER_MAX_IN_FLIGHT above 1 requires an inline V4 execution profile",
                    ));
                }
            };
            (
                workerd,
                config.request_timeout.saturating_add(SHUTDOWN_DRAIN_GRACE),
                max_in_flight,
            )
        }
        WorkerExecutionMode::ExecutorHandoff => {
            if configured_max_in_flight != 1 {
                return Err(ImageGatewayError::config(
                    "WORKER_MAX_IN_FLIGHT is only supported in legacy-inline mode",
                ));
            }
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
            validate_handoff_profile(&profile).map_err(|_| {
                ImageGatewayError::config("workerd executor handoff profile is incompatible")
            })?;
            let handoff_lease = Duration::from_millis(handoff_lease_ms()?);
            tracing::info!(
                execution.profile.id = %profile.execution_profile_id,
                execution.profile.key = %profile.profile_key,
                "workerd V2 executor handoff enabled"
            );
            let contract = handoff_admission_contract(
                config.generation_admission_contract,
                &profile.operation_id,
            );
            let workerd = Workerd::new_handoff_only_with_contract(
                worker_id.clone(),
                admission,
                contexts,
                executor_store,
                profile.execution_profile_id,
                profile.command_schema,
                handoff_lease,
                contract,
            )?;
            (
                workerd,
                handoff_lease.saturating_add(SHUTDOWN_DRAIN_GRACE),
                1,
            )
        }
    };
    let poll_interval = Duration::from_millis(poll_interval_ms()?);
    tracing::info!(
        %worker_id,
        execution.mode = execution_mode.as_str(),
        worker.max_in_flight = max_in_flight,
        "workerd started"
    );
    run_worker_lanes(
        Arc::new(workerd),
        &worker_id,
        max_in_flight,
        poll_interval,
        shutdown_drain_timeout,
        shutdown_signal(),
    )
    .await?;
    telemetry.shutdown();
    Ok(())
}

async fn run_worker_lanes<S>(
    workerd: Arc<Workerd>,
    worker_id: &str,
    max_in_flight: usize,
    poll_interval: Duration,
    shutdown_drain_timeout: Duration,
    shutdown: S,
) -> Result<(), ImageGatewayError>
where
    S: std::future::Future<Output = ()>,
{
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut lanes = JoinSet::new();
    for lane in 0..max_in_flight {
        lanes.spawn(run_worker_lane(
            Arc::clone(&workerd),
            lane_worker_id(worker_id, lane),
            poll_interval,
            shutdown_rx.clone(),
        ));
    }
    drop(shutdown_rx);
    tokio::pin!(shutdown);

    tokio::select! {
        _ = &mut shutdown => {
            tracing::info!(worker.max_in_flight = max_in_flight, "workerd draining in-flight work");
        }
        result = lanes.join_next() => {
            trace_lane_termination(result);
            let _ = shutdown_tx.send(true);
            lanes.abort_all();
            while lanes.join_next().await.is_some() {}
            return Err(ImageGatewayError::service_unavailable(
                "workerd execution lane terminated",
            ));
        }
    }

    let _ = shutdown_tx.send(true);
    match tokio::time::timeout(shutdown_drain_timeout, async {
        while let Some(result) = lanes.join_next().await {
            if let Err(error) = result {
                tracing::error!(
                    task.id = ?error.id(),
                    task.cancelled = error.is_cancelled(),
                    task.panicked = error.is_panic(),
                    "workerd execution lane failed while draining"
                );
                return Err(ImageGatewayError::service_unavailable(
                    "workerd execution lane terminated",
                ));
            }
        }
        Ok(())
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            lanes.abort_all();
            while lanes.join_next().await.is_some() {}
            Err(ImageGatewayError::service_unavailable(
                "workerd shutdown drain timed out",
            ))
        }
    }
}

async fn run_worker_lane(
    workerd: Arc<Workerd>,
    worker_id: String,
    poll_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        match workerd.run_once_with_worker_id(&worker_id).await {
            Ok(Some(job_id)) => tracing::info!(%worker_id, %job_id, "durable work processed"),
            Ok(None) => {}
            Err(error) => {
                tracing::error!(%worker_id, error = ?error, "durable work execution failed")
            }
        }
        if *shutdown.borrow() {
            return;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
}

fn trace_lane_termination(result: Option<Result<(), tokio::task::JoinError>>) {
    match result {
        Some(Ok(())) => tracing::error!("workerd execution lane exited before shutdown"),
        Some(Err(error)) => tracing::error!(
            task.id = ?error.id(),
            task.cancelled = error.is_cancelled(),
            task.panicked = error.is_panic(),
            "workerd execution lane failed"
        ),
        None => tracing::error!("workerd lost all execution lanes"),
    }
}

fn lane_worker_id(worker_id: &str, lane: usize) -> String {
    format!("{worker_id}/lane-{lane}")
}

fn validate_inline_edit_profile(profile: &ExecutorExecutionProfile) -> Result<(), ()> {
    let operation = openai_codex::operation("images.edits").ok_or(())?;
    if profile.provider_id == openai_codex::PROVIDER_ID
        && profile.command_schema == EDIT_COMMAND_SCHEMA
        && profile.operation_id == operation.id
        && profile.operation_descriptor_revision == operation.descriptor_revision
        && profile.operation_descriptor_sha256_v1 == operation.canonical_sha256_v1_hex()
        && profile.completion_mode == operation.completion.as_str()
        && profile.idempotency_mode == operation.idempotency.as_str()
        && profile.adapter_revision == CODEX_EDIT_INLINE_ADAPTER_REVISION
    {
        Ok(())
    } else {
        Err(())
    }
}

fn validate_handoff_profile(profile: &ExecutorExecutionProfile) -> Result<(), ()> {
    if identify_executor_profile_binding(profile).is_ok() {
        return Ok(());
    }
    if profile.provider_id != DREAMINA_PROVIDER_ID
        || profile.command_schema != DREAMINA_SUBMIT_COMMAND_SCHEMA
        || profile.adapter_revision != DREAMINA_ADAPTER_REVISION
    {
        return Err(());
    }
    let operation = match profile.operation_id.as_str() {
        "images.generations" => DREAMINA_IMAGE_GENERATION_OPERATION_V1,
        "videos.generations" => DREAMINA_VIDEO_GENERATION_OPERATION_V1,
        _ => return Err(()),
    };
    if profile.operation_descriptor_revision == operation.descriptor_revision
        && profile.operation_descriptor_sha256_v1 == operation.canonical_sha256_v1_hex()
        && profile.completion_mode == operation.completion.as_str()
        && profile.idempotency_mode == operation.idempotency.as_str()
    {
        Ok(())
    } else {
        Err(())
    }
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

fn worker_max_in_flight() -> Result<usize, ImageGatewayError> {
    let max_in_flight = env::var("WORKER_MAX_IN_FLIGHT")
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| ImageGatewayError::config("WORKER_MAX_IN_FLIGHT must be an integer"))
        })
        .unwrap_or(Ok(DEFAULT_MAX_IN_FLIGHT))?;
    if !(1..=MAX_WORKER_LANES).contains(&max_in_flight) {
        return Err(ImageGatewayError::config(
            "WORKER_MAX_IN_FLIGHT must be between 1 and 64",
        ));
    }
    Ok(max_in_flight)
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

fn handoff_admission_contract(
    configured: GenerationAdmissionContract,
    operation_id: &str,
) -> AdmissionContract {
    match configured {
        GenerationAdmissionContract::CustomerPricingV4 => AdmissionContract::CustomerPricingV4,
        _ if operation_id == "videos.generations" => AdmissionContract::MediaEconomicsV3,
        GenerationAdmissionContract::LegacyV1 => AdmissionContract::LegacyV1,
        GenerationAdmissionContract::OutputEconomicsV2 => AdmissionContract::OutputEconomicsV2,
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
    use super::*;

    #[test]
    fn customer_priced_video_profiles_claim_v4_work() {
        assert_eq!(
            handoff_admission_contract(
                GenerationAdmissionContract::CustomerPricingV4,
                "videos.generations",
            ),
            AdmissionContract::CustomerPricingV4,
        );
    }

    #[test]
    fn non_customer_priced_video_profiles_claim_media_v3_work() {
        assert_eq!(
            handoff_admission_contract(
                GenerationAdmissionContract::OutputEconomicsV2,
                "videos.generations",
            ),
            AdmissionContract::MediaEconomicsV3,
        );
    }

    #[test]
    fn lane_ids_are_stable_and_distinct() {
        assert_eq!(lane_worker_id("codex-edits", 0), "codex-edits/lane-0");
        assert_eq!(lane_worker_id("codex-edits", 1), "codex-edits/lane-1");
    }
}
