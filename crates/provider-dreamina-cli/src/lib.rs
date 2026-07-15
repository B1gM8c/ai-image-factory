#![forbid(unsafe_code)]

mod capabilities;
mod policy;
mod receipt;
mod request;

pub use capabilities::DREAMINA_CLI_REMOTE_TASK_CONTROLS_V1;
pub use policy::{DreaminaCliPolicyError, DreaminaCliPolicyV1, DreaminaSubmitRequestV1};
pub use receipt::{
    AcceptedReceipt, AcceptedStatus, MAX_FAIL_REASON_CHARS, MAX_RECEIPT_BYTES, ReceiptError,
    parse_receipt,
};
pub use request::{
    ImageModelVersion, ImageRatio, ImageResolution, QueryResultRequestV1, RequestValidationError,
    TextToImageRequestV1, TextToVideoRequestV1, VideoModelVersion, VideoRatio, VideoResolution,
};

pub const PROVIDER_ID: &str = "dreamina-cli";
pub const ADAPTER_REVISION: &str = "dreamina-cli/submit/v1";
pub const REQUEST_SCHEMA_VERSION: u16 = 1;

#[cfg(test)]
mod tests;
