use gpt_image_2_gateway::{
    ImageGatewayError,
    artifacts::{FilesystemArtifactBlobStore, artifact_root_from_env},
    database::{
        connect_media_segments_pool_with_schema, database_schema_from_env, database_url_from_env,
        verify_migrations,
    },
    init_telemetry,
    media_segments::{
        CodexBboxAnalyzer, PostgresSegmentStore, SegmentWorker, analyzer_config_from_env,
        analyzer_key, store_operation,
    },
};
use std::{path::PathBuf, sync::Arc, time::Duration};

fn required_path(name: &str) -> Result<PathBuf, ImageGatewayError> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| ImageGatewayError::config(format!("{name} must be an absolute path")))
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let telemetry = init_telemetry()?;
    let config = analyzer_config_from_env()?;
    let analyzer = Arc::new(CodexBboxAnalyzer::new(
        required_path("GATEWAY_BBOX_CODEX_BIN")?,
        required_path("GATEWAY_BBOX_CODEX_HOME")?,
        config.clone(),
    )?);
    let database_url = database_url_from_env()?;
    let schema = database_schema_from_env()?;
    let pool = connect_media_segments_pool_with_schema(&database_url, &schema).await?;
    store_operation(verify_migrations(&pool)).await?;
    let store = Arc::new(PostgresSegmentStore::new(pool.clone()));
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(artifact_root_from_env()?)?);
    let release_terminal_sources = match std::env::var("GATEWAY_BBOX_RELEASE_TERMINAL_SOURCES") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "false" => false,
        Ok(value) if value == "true" => true,
        _ => {
            return Err(ImageGatewayError::config(
                "GATEWAY_BBOX_RELEASE_TERMINAL_SOURCES must be true or false",
            ));
        }
    };
    let worker = SegmentWorker::new(store.clone(), blobs, analyzer, &config)
        .with_terminal_source_release(release_terminal_sources);
    tracing::info!(
        release_terminal_sources,
        "Segmentation source lifecycle configured"
    );
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .map_err(|_| {
            ImageGatewayError::service_unavailable("Could not connect segmentation wakeups")
        })?;
    listener.listen("media_segments_ready").await.map_err(|_| {
        ImageGatewayError::service_unavailable("Could not listen for segmentation wakeups")
    })?;
    let shutdown = async {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::pin!(shutdown);
    // One in-flight CLI per process. PostgreSQL leases arbitrate multiple workers.
    loop {
        if let Err(error) = store_operation(
            store.record_worker_heartbeat(&analyzer_key(&config), release_terminal_sources),
        )
        .await
        {
            tracing::warn!(?error, "Segmentation heartbeat failed");
        }
        if let Err(error) = worker.maintain().await {
            tracing::warn!(?error, "Segmentation maintenance failed");
        }
        let work = tokio::select! {
            _ = &mut shutdown => break,
            work = worker.run_once() => work,
        };
        if matches!(work, Ok(true)) {
            continue;
        }
        if let Err(error) = work {
            tracing::warn!(?error, "Segmentation worker pass failed");
        }
        tokio::select! {
            _ = &mut shutdown => break,
            _ = tokio::time::sleep(Duration::from_secs(30)) => {},
            notification = listener.recv() => {
                if notification.is_err() { tokio::time::sleep(Duration::from_secs(1)).await; }
            },
        }
    }
    telemetry.shutdown();
    Ok(())
}
