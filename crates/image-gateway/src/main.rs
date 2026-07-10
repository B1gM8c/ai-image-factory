use std::sync::Arc;

use gpt_image_2_gateway::{
    AppConfig, CodexImageGenerator, ImageGatewayError, PostgresApiKeyStore, PostgresUsageStore,
    build_router_with_api_key_store,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
};

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = AppConfig::from_env()?;
    config.validate_startup()?;
    let telemetry = init_telemetry()?;

    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let usage_store = Arc::new(PostgresUsageStore::new(pool.clone()));
    let api_key_store = Arc::new(PostgresApiKeyStore::new(pool));
    let generator = Arc::new(CodexImageGenerator::new(config.clone()));
    let bind = config.bind;
    let app = build_router_with_api_key_store(config, generator, usage_store, api_key_store);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| ImageGatewayError::config("failed to bind HTTP listener"))?;
    tracing::info!(%bind, "gpt-image-2 gateway listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|_| ImageGatewayError::internal("HTTP server failed"))?;

    telemetry.shutdown();
    Ok(())
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
