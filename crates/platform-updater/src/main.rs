use ai_image_factory_updater::{UPDATER_PROTOCOL_VERSION, Updater, UpdaterConfig, UpdaterError};

#[tokio::main]
async fn main() -> Result<(), UpdaterError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ai_image_factory_updater=info".into()),
        )
        .init();
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if matches!(arguments.as_slice(), [operation] if operation == "version") {
        println!("ai-image-factory-updater protocol={UPDATER_PROTOCOL_VERSION}");
        return Ok(());
    }
    let updater = Updater::from_config(UpdaterConfig::from_env()?).await?;
    match arguments.as_slice() {
        [] => updater.run().await,
        [operation] if operation == "recover-pending" => updater.recover_pending().await,
        [operation, command_id] if operation == "recover" => {
            updater.recover_command(command_id).await
        }
        _ => Err(UpdaterError::Config(
            "usage: updated [version|recover-pending|recover COMMAND_ID]".to_string(),
        )),
    }
}
