use image_provider_contracts::{CallbackMode, CancellationMode, RemoteTaskControls};

pub const DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1: RemoteTaskControls = RemoteTaskControls {
    callback: CallbackMode::Unsupported,
    cancellation: CancellationMode::Unsupported,
};
