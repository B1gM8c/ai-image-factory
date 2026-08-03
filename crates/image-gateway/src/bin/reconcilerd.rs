use std::{env, sync::Arc, time::Duration};

use gpt_image_2_gateway::{
    ExecutorSubmissionStore, ImageGatewayError, PostgresArtifactRetentionStore,
    PostgresExecutorSubmissionStore, PostgresReconciliationStore, ProviderUploadService,
    ReconciliationStore,
    artifacts::{FilesystemArtifactBlobStore, artifact_root_from_env},
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    identity::PostgresIdentityMaintenanceStore,
    init_telemetry, reconcile_artifact_retention, reconcile_execution_profile_routes,
    reconcile_input_cleanup,
};

const DEFAULT_INTERVAL_MS: u64 = 1_000;
const MAX_INTERVAL_MS: u64 = 60_000;
const DEFAULT_BATCH_SIZE: u32 = 100;
const MAX_BATCH_SIZE: u32 = 1_000;
const DEFAULT_ORPHAN_GRACE_MS: u64 = 60_000;
const DEFAULT_INPUT_CLEANUP_GRACE_MS: u64 = 60_000;
const DEFAULT_INPUT_CLEANUP_LEASE_MS: u64 = 60_000;
const DEFAULT_ARTIFACT_CLEANUP_LEASE_MS: u64 = 60_000;
const DEFAULT_IDENTITY_GC_INTERVAL_MS: u64 = 5 * 60_000;
const DEFAULT_IDENTITY_SESSION_RETENTION_MS: u64 = 7 * 24 * 60 * 60_000;
const DEFAULT_IDENTITY_THROTTLE_RETENTION_MS: u64 = 24 * 60 * 60_000;
const DEFAULT_IDENTITY_AUDIT_RETENTION_MS: u64 = 180 * 24 * 60 * 60_000;
const PROVIDER_UPLOAD_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const PROVIDER_ROUTE_RECONCILE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
struct ReconcileConfig {
    orphan_grace_ms: u64,
    input_cleanup_grace_ms: u64,
    input_cleanup_lease_ms: u64,
    artifact_cleanup_lease_ms: u64,
    batch_size: u32,
}

#[derive(Clone, Copy)]
struct IdentityMaintenanceConfig {
    interval: Duration,
    session_retention_ms: u64,
    throttle_retention_ms: u64,
    audit_retention_ms: u64,
    batch_size: u32,
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let telemetry = init_telemetry()?;
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let reconciler = PostgresReconciliationStore::new(pool.clone());
    let artifact_retention = PostgresArtifactRetentionStore::new(pool.clone());
    let identity_maintenance = PostgresIdentityMaintenanceStore::new(pool.clone());
    let executor_reconciler = PostgresExecutorSubmissionStore::new(pool.clone());
    let artifact_root = artifact_root_from_env()?;
    let input_blobs = Arc::new(FilesystemArtifactBlobStore::new(&artifact_root)?);
    let provider_uploads = ProviderUploadService::new(&artifact_root, None)?;
    let owner = format!("reconcilerd-{}", uuid::Uuid::new_v4().simple());
    let interval_ms = env_u64("RECONCILER_INTERVAL_MS", DEFAULT_INTERVAL_MS)?;
    if !(1..=MAX_INTERVAL_MS).contains(&interval_ms) {
        return Err(ImageGatewayError::config(format!(
            "RECONCILER_INTERVAL_MS must be between 1 and {MAX_INTERVAL_MS}"
        )));
    }
    let interval = Duration::from_millis(interval_ms);
    let batch_size = env_u32("RECONCILER_BATCH_SIZE", DEFAULT_BATCH_SIZE)?;
    if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
        return Err(ImageGatewayError::config(format!(
            "RECONCILER_BATCH_SIZE must be between 1 and {MAX_BATCH_SIZE}"
        )));
    }
    let orphan_grace_ms = env_u64("RECONCILER_ORPHAN_GRACE_MS", DEFAULT_ORPHAN_GRACE_MS)?;
    let input_cleanup_grace_ms = env_u64(
        "RECONCILER_INPUT_CLEANUP_GRACE_MS",
        DEFAULT_INPUT_CLEANUP_GRACE_MS,
    )?;
    let input_cleanup_lease_ms = env_u64(
        "RECONCILER_INPUT_CLEANUP_LEASE_MS",
        DEFAULT_INPUT_CLEANUP_LEASE_MS,
    )?;
    let artifact_cleanup_lease_ms = env_u64(
        "RECONCILER_ARTIFACT_CLEANUP_LEASE_MS",
        DEFAULT_ARTIFACT_CLEANUP_LEASE_MS,
    )?;
    if artifact_cleanup_lease_ms == 0 {
        return Err(ImageGatewayError::config(
            "RECONCILER_ARTIFACT_CLEANUP_LEASE_MS must be greater than zero",
        ));
    }
    let reconcile_config = ReconcileConfig {
        orphan_grace_ms,
        input_cleanup_grace_ms,
        input_cleanup_lease_ms,
        artifact_cleanup_lease_ms,
        batch_size,
    };
    let identity_maintenance_config = identity_maintenance_config(batch_size)?;
    let mut next_identity_maintenance = tokio::time::Instant::now();
    let mut next_provider_upload_cleanup = tokio::time::Instant::now();
    let mut next_provider_route_reconcile = tokio::time::Instant::now();
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    tracing::info!(batch_size, "reconcilerd started");

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            result = reconcile_all(
                &reconciler,
                &executor_reconciler,
                &artifact_retention,
                input_blobs.as_ref(),
                &owner,
                reconcile_config,
            ) => {
                let (core_result, executor_result, artifact_result) = result;
                match core_result {
                Ok((work, orphan, input)) => {
                    if work.requeued > 0 || work.uncertain > 0 || orphan.orphaned > 0
                        || input.claimed > 0
                    {
                        tracing::info!(
                            requeued = work.requeued,
                            uncertain = work.uncertain,
                            orphaned = orphan.orphaned,
                            input_cleanup_claimed = input.claimed,
                            input_cleanup_completed = input.completed,
                            input_cleanup_failed = input.failed,
                            "durable state reconciled"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(error = ?error, "work reconciliation failed");
                }
                }
                match executor_result {
                    Ok(executor_uncertain) if executor_uncertain > 0 => {
                        tracing::info!(executor_uncertain, "executor state reconciled");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(error = ?error, "executor reconciliation failed");
                    }
                }
                match artifact_result {
                    Ok(artifacts) if artifacts.expired > 0 || artifacts.claimed > 0 => {
                        tracing::info!(
                            artifact_expired = artifacts.expired,
                            artifact_cleanup_claimed = artifacts.claimed,
                            artifact_cleanup_deleted = artifacts.deleted,
                            artifact_cleanup_failed = artifacts.failed,
                            "artifact retention reconciled"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(error = ?error, "artifact retention reconciliation failed");
                    }
                }
            },
        }
        if tokio::time::Instant::now() >= next_identity_maintenance {
            match identity_maintenance
                .purge_expired(
                    identity_maintenance_config.session_retention_ms,
                    identity_maintenance_config.throttle_retention_ms,
                    identity_maintenance_config.audit_retention_ms,
                    identity_maintenance_config.batch_size,
                )
                .await
            {
                Ok(outcome)
                    if outcome.session_families > 0
                        || outcome.login_throttles > 0
                        || outcome.audit_events > 0 =>
                {
                    tracing::info!(
                        session_families = outcome.session_families,
                        login_throttles = outcome.login_throttles,
                        audit_events = outcome.audit_events,
                        "expired identity state purged"
                    );
                }
                Ok(_) => {}
                Err(error) => tracing::error!(error = ?error, "identity maintenance failed"),
            }
            next_identity_maintenance =
                tokio::time::Instant::now() + identity_maintenance_config.interval;
        }
        if tokio::time::Instant::now() >= next_provider_upload_cleanup {
            match provider_uploads.cleanup_expired().await {
                Ok(deleted) if deleted > 0 => {
                    tracing::info!(deleted, "expired provider uploads deleted")
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(error = ?error, "provider upload cleanup failed")
                }
            }
            next_provider_upload_cleanup =
                tokio::time::Instant::now() + PROVIDER_UPLOAD_CLEANUP_INTERVAL;
        }
        if tokio::time::Instant::now() >= next_provider_route_reconcile {
            if let Err(error) = reconcile_execution_profile_routes(&pool).await {
                tracing::error!(error = ?error, "provider route reconciliation failed");
            }
            next_provider_route_reconcile =
                tokio::time::Instant::now() + PROVIDER_ROUTE_RECONCILE_INTERVAL;
        }
        tokio::select! {
            _ = &mut shutdown => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
    telemetry.shutdown();
    Ok(())
}

fn identity_maintenance_config(
    batch_size: u32,
) -> Result<IdentityMaintenanceConfig, ImageGatewayError> {
    let interval_ms = env_u64(
        "RECONCILER_IDENTITY_GC_INTERVAL_MS",
        DEFAULT_IDENTITY_GC_INTERVAL_MS,
    )?;
    if !(1_000..=24 * 60 * 60_000).contains(&interval_ms) {
        return Err(ImageGatewayError::config(
            "RECONCILER_IDENTITY_GC_INTERVAL_MS must be between 1000 and 86400000",
        ));
    }
    let session_retention_ms = env_u64(
        "RECONCILER_IDENTITY_SESSION_RETENTION_MS",
        DEFAULT_IDENTITY_SESSION_RETENTION_MS,
    )?;
    let throttle_retention_ms = env_u64(
        "RECONCILER_IDENTITY_THROTTLE_RETENTION_MS",
        DEFAULT_IDENTITY_THROTTLE_RETENTION_MS,
    )?;
    let audit_retention_ms = env_u64(
        "RECONCILER_IDENTITY_AUDIT_RETENTION_MS",
        DEFAULT_IDENTITY_AUDIT_RETENTION_MS,
    )?;
    if session_retention_ms == 0 || throttle_retention_ms == 0 || audit_retention_ms == 0 {
        return Err(ImageGatewayError::config(
            "identity maintenance retention values must be greater than zero",
        ));
    }
    Ok(IdentityMaintenanceConfig {
        interval: Duration::from_millis(interval_ms),
        session_retention_ms,
        throttle_retention_ms,
        audit_retention_ms,
        batch_size,
    })
}

async fn reconcile_all(
    reconciler: &PostgresReconciliationStore,
    executor_reconciler: &PostgresExecutorSubmissionStore,
    artifact_retention: &PostgresArtifactRetentionStore,
    blobs: &FilesystemArtifactBlobStore,
    owner: &str,
    config: ReconcileConfig,
) -> (
    Result<
        (
            gpt_image_2_gateway::ReconciliationOutcome,
            gpt_image_2_gateway::ReconciliationOutcome,
            gpt_image_2_gateway::InputCleanupOutcome,
        ),
        ImageGatewayError,
    >,
    Result<u64, gpt_image_2_gateway::ExecutorSubmissionError>,
    Result<gpt_image_2_gateway::ArtifactRetentionOutcome, ImageGatewayError>,
) {
    let executor = executor_reconciler.reconcile_expired(config.batch_size);
    let core = reconcile_core(reconciler, blobs, owner, config);
    let artifacts = reconcile_artifact_retention(
        artifact_retention,
        blobs,
        owner,
        config.artifact_cleanup_lease_ms,
        config.batch_size,
    );
    let (core, executor, artifacts) = tokio::join!(core, executor, artifacts);
    (core, executor, artifacts)
}

async fn reconcile_core(
    reconciler: &PostgresReconciliationStore,
    blobs: &FilesystemArtifactBlobStore,
    owner: &str,
    config: ReconcileConfig,
) -> Result<
    (
        gpt_image_2_gateway::ReconciliationOutcome,
        gpt_image_2_gateway::ReconciliationOutcome,
        gpt_image_2_gateway::InputCleanupOutcome,
    ),
    ImageGatewayError,
> {
    let work = reconciler.reconcile_expired_work(config.batch_size).await?;
    let orphan = reconciler
        .reconcile_orphan_reservations(config.orphan_grace_ms, config.batch_size)
        .await?;
    let input = reconcile_input_cleanup(
        reconciler,
        blobs,
        owner,
        config.input_cleanup_grace_ms,
        config.input_cleanup_lease_ms,
        config.batch_size,
    )
    .await?;
    Ok((work, orphan, input))
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

fn env_u32(name: &str, default: u32) -> Result<u32, ImageGatewayError> {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))
        })
        .unwrap_or(Ok(default))
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
