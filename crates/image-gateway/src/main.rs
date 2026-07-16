use std::sync::Arc;

use gpt_image_2_gateway::{
    ApiKeyKeyring, AppConfig, ExternalImageGatewayComponents, ImageGatewayError,
    PostgresApiKeyStore, PostgresProviderTaskStore, PostgresUsageStore,
    admission::PostgresAdmissionStore,
    artifacts::{FilesystemArtifactBlobStore, artifact_root_from_env},
    build_router_with_external_execution,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, verify_migrations,
    },
    init_telemetry,
    settlement::PostgresExecutionSettlementStore,
};

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let config = AppConfig::from_env()?;
    config.validate_startup()?;
    let api_key_keyring = ApiKeyKeyring::from_env()?;
    let artifact_root = artifact_root_from_env()?;
    let artifact_store = Arc::new(FilesystemArtifactBlobStore::new(artifact_root)?);
    let telemetry = init_telemetry()?;

    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    verify_migrations(&pool).await?;
    let usage_store = Arc::new(PostgresUsageStore::new(pool.clone()));
    let api_key_store = Arc::new(PostgresApiKeyStore::new(pool.clone(), api_key_keyring));
    let admission_store = Arc::new(PostgresAdmissionStore::new(pool.clone()));
    let provider_readiness_store = Arc::new(PostgresProviderTaskStore::new(pool.clone()));
    let settlement_store = Arc::new(PostgresExecutionSettlementStore::new(
        pool,
        artifact_store.clone(),
    ));
    let bind = config.bind;
    let generation_contract = config.generation_admission_contract.as_str();
    let app = build_router_with_external_execution(
        config,
        ExternalImageGatewayComponents {
            usage_store,
            api_key_store,
            admission_store,
            settlement_store,
            input_blob_store: artifact_store,
            provider_readiness_store,
        },
    )?;

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|_| ImageGatewayError::config("failed to bind HTTP listener"))?;
    tracing::info!(%bind, generation.contract = generation_contract, "gpt-image-2 gateway listening");

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
