use std::{env, sync::Arc, time::Duration};

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
            let workerd = match config.generation_admission_contract {
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
                    workerd.with_claim_profile(
                        AdmissionContract::CustomerPricingV4,
                        EDIT_COMMAND_SCHEMA,
                        profile.execution_profile_id,
                    )?
                }
                _ => workerd,
            };
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
}
