#![forbid(unsafe_code)]

mod capabilities;
mod command;
mod policy;
mod receipt;
mod request;

pub use capabilities::{
    DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1, DREAMINA_IMAGE_GENERATION_OPERATION_V1,
};
pub use command::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaSubmitCommandError, DreaminaSubmitPayloadV1,
    MAX_SUBMIT_COMMAND_BYTES, parse_submit_command,
};
pub use policy::{DreaminaCliPolicyError, DreaminaCliPolicyV1, DreaminaSubmitRequestV1};
pub use policy::{DreaminaCliQueryPolicyError, DreaminaCliQueryPolicyV1};
pub use receipt::{
    AcceptedReceipt, AcceptedStatus, DreaminaQueryReceiptV1, DreaminaQueryStatusV1,
    MAX_FAIL_REASON_CHARS, MAX_RECEIPT_BYTES, ReceiptError, parse_query_receipt, parse_receipt,
};
pub use request::{
    ImageModelVersion, ImageRatio, ImageResolution, QueryResultRequestV1, RequestValidationError,
    TextToImageRequestV1, TextToVideoRequestV1, VideoModelVersion, VideoRatio, VideoResolution,
};

pub const PROVIDER_ID: &str = "dreamina-cli";
pub const ADAPTER_REVISION: &str = "dreamina-cli.remote-task.v1";
pub const REQUEST_SCHEMA_VERSION: u16 = 1;

#[cfg(test)]
mod tests;
