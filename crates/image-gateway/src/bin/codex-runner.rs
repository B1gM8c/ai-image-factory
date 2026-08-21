use std::{env, path::PathBuf};

use gpt_image_2_gateway::{ImageGatewayError, run_codex_runner_child};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|_| ImageGatewayError::service_unavailable("Codex runner tracing unavailable"))?;
    let mut args = env::args_os().skip(1);
    let root = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| ImageGatewayError::config("runner root argument is required"))?;
    let execution_id = args
        .next()
        .and_then(|value| value.into_string().ok())
        .and_then(|value| Uuid::parse_str(&value).ok())
        .ok_or_else(|| ImageGatewayError::config("executor execution ID is invalid"))?;
    if args.next().is_some() {
        return Err(ImageGatewayError::config(
            "unexpected Codex runner argument",
        ));
    }
    run_codex_runner_child(root, execution_id).await
}
