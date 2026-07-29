use std::env;

use gpt_image_2_gateway::{
    ImageGatewayError,
    database::{
        connect_pool_with_schema, database_schema_from_env, database_url_from_env,
        verify_migrations,
    },
    webhooks::{
        PostgresWebhookRelay, WebhookDeliveryWorker, WebhookDestinationPolicy,
        WebhookSigningKeyring,
    },
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let max_connections = parse_usize_env("GATEWAY_WEBHOOK_DB_MAX_CONNECTIONS", 8, 1, 64)?;
    let concurrency = parse_usize_env("GATEWAY_WEBHOOK_CONCURRENCY", 16, 1, 100)?;
    let pool =
        connect_pool_with_schema(&database_url, max_connections as u32, &database_schema).await?;
    verify_migrations(&pool).await?;
    let keyring = WebhookSigningKeyring::from_env()?;
    let destination_policy = WebhookDestinationPolicy::from_env()?;
    let relay = PostgresWebhookRelay::new(pool);
    let worker = WebhookDeliveryWorker::new(relay, keyring, destination_policy);
    let worker_id = format!("webhookd-{}", Uuid::new_v4().simple());
    tracing::info!(%worker_id, concurrency, "webhook delivery worker started");
    tokio::select! {
        result = worker.run(worker_id, concurrency) => result,
        _ = shutdown_signal() => Ok(()),
    }
}

fn parse_usize_env(
    name: &str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, ImageGatewayError> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map_err(|_| ImageGatewayError::config(format!("{name} must be an integer")))?,
        Err(env::VarError::NotPresent) => default,
        Err(env::VarError::NotUnicode(_)) => {
            return Err(ImageGatewayError::config(format!(
                "{name} must be valid UTF-8"
            )));
        }
    };
    if !(min..=max).contains(&value) {
        return Err(ImageGatewayError::config(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
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
