use std::{env, time::Duration};

use gpt_image_2_gateway::{
    ImageGatewayError, PostgresReconciliationStore, ReconciliationStore,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
};

const DEFAULT_INTERVAL_MS: u64 = 1_000;
const MAX_INTERVAL_MS: u64 = 60_000;
const DEFAULT_BATCH_SIZE: u32 = 100;
const DEFAULT_ORPHAN_GRACE_MS: u64 = 60_000;

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let telemetry = init_telemetry()?;
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let reconciler = PostgresReconciliationStore::new(pool);
    let interval_ms = env_u64("RECONCILER_INTERVAL_MS", DEFAULT_INTERVAL_MS)?;
    if !(1..=MAX_INTERVAL_MS).contains(&interval_ms) {
        return Err(ImageGatewayError::config(format!(
            "RECONCILER_INTERVAL_MS must be between 1 and {MAX_INTERVAL_MS}"
        )));
    }
    let interval = Duration::from_millis(interval_ms);
    let batch_size = env_u32("RECONCILER_BATCH_SIZE", DEFAULT_BATCH_SIZE)?;
    let orphan_grace_ms = env_u64("RECONCILER_ORPHAN_GRACE_MS", DEFAULT_ORPHAN_GRACE_MS)?;
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    tracing::info!(batch_size, "reconcilerd started");

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            result = reconcile_once(&reconciler, orphan_grace_ms, batch_size) => match result {
                Ok((work, orphan)) => {
                    if work.requeued > 0 || work.uncertain > 0 || orphan.orphaned > 0 {
                        tracing::info!(
                            requeued = work.requeued,
                            uncertain = work.uncertain,
                            orphaned = orphan.orphaned,
                            "durable state reconciled"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(error = ?error, "work reconciliation failed");
                }
            },
        }
        tokio::select! {
            _ = &mut shutdown => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
    telemetry.shutdown();
    Ok(())
}

async fn reconcile_once(
    reconciler: &PostgresReconciliationStore,
    orphan_grace_ms: u64,
    batch_size: u32,
) -> Result<
    (
        gpt_image_2_gateway::ReconciliationOutcome,
        gpt_image_2_gateway::ReconciliationOutcome,
    ),
    ImageGatewayError,
> {
    let work = reconciler.reconcile_expired_work(batch_size).await?;
    let orphan = reconciler
        .reconcile_orphan_reservations(orphan_grace_ms, batch_size)
        .await?;
    Ok((work, orphan))
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
