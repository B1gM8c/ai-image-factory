use gpt_image_2_gateway::{
    ImageGatewayError,
    artifacts::{FilesystemArtifactBlobStore, artifact_root_from_env},
    database::{
        connect_pool_with_schema, database_schema_from_env, database_url_from_env,
        verify_migrations,
    },
    init_telemetry,
    media_segments::{
        CodexBboxAnalyzer, PostgresSegmentStore, SegmentWorker, analyzer_config_from_env,
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
    let pool = connect_pool_with_schema(&database_url, 3, &schema).await?;
    verify_migrations(&pool).await?;
    let store = Arc::new(PostgresSegmentStore::new(pool.clone()));
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(artifact_root_from_env()?)?);
    let worker = SegmentWorker::new(store, blobs, analyzer, &config);
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
