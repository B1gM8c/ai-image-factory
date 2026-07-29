use std::{env, path::PathBuf};

use gpt_image_2_gateway::{ImageGatewayError, run_grok_runner_child};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let mut arguments = env::args_os();
    let _program = arguments.next();
    let root = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| ImageGatewayError::service_unavailable("Grok runner root is missing"))?;
    let execution_id = arguments
        .next()
        .and_then(|value| value.to_str().and_then(|value| Uuid::parse_str(value).ok()))
        .ok_or_else(|| ImageGatewayError::service_unavailable("Grok execution ID is invalid"))?;
    if arguments.next().is_some() {
        return Err(ImageGatewayError::service_unavailable(
            "Grok runner arguments are invalid",
        ));
    }
    run_grok_runner_child(root, execution_id).await
}
