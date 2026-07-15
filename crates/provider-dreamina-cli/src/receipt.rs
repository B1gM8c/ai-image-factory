use serde::Deserialize;
use thiserror::Error;

use image_provider_sdk::OpaqueProviderId;

pub const MAX_RECEIPT_BYTES: usize = 64 * 1024;
pub const MAX_FAIL_REASON_CHARS: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptedStatus {
    Querying,
    Success,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedReceipt {
    submit_id: OpaqueProviderId,
    status: AcceptedStatus,
}

impl AcceptedReceipt {
    pub fn submit_id(&self) -> &str {
        self.submit_id.as_str()
    }

    pub fn status(&self) -> AcceptedStatus {
        self.status
    }
}

#[derive(Debug, Error)]
pub enum ReceiptError {
    #[error("Dreamina receipt exceeds the 64 KiB limit: {actual} bytes")]
    InputTooLarge { actual: usize },
    #[error("Dreamina receipt is not one complete JSON object: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("Dreamina receipt is missing gen_status")]
    MissingStatus,
    #[error("Dreamina receipt has unknown gen_status: {0}")]
    UnknownStatus(String),
    #[error("Dreamina receipt has an empty submit_id")]
    EmptySubmitId,
    #[error("Dreamina receipt has an invalid durable submit_id")]
    InvalidSubmitId,
    #[error("Dreamina generation failed: {reason}")]
    GenerationFailed { reason: String },
}

#[derive(Deserialize)]
struct RawReceipt {
    submit_id: Option<String>,
    gen_status: Option<String>,
    fail_reason: Option<String>,
}

pub fn parse_receipt(input: &[u8]) -> Result<AcceptedReceipt, ReceiptError> {
    if input.len() > MAX_RECEIPT_BYTES {
        return Err(ReceiptError::InputTooLarge {
            actual: input.len(),
        });
    }

    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let raw = RawReceipt::deserialize(&mut deserializer).map_err(ReceiptError::InvalidJson)?;
    deserializer.end().map_err(ReceiptError::InvalidJson)?;

    match raw.gen_status.as_deref() {
        Some("querying") => accept(raw.submit_id, AcceptedStatus::Querying),
        Some("success") => accept(raw.submit_id, AcceptedStatus::Success),
        Some("fail") => Err(ReceiptError::GenerationFailed {
            reason: sanitize_fail_reason(raw.fail_reason.as_deref()),
        }),
        Some(status) => Err(ReceiptError::UnknownStatus(sanitize_text(status))),
        None => Err(ReceiptError::MissingStatus),
    }
}

fn accept(
    submit_id: Option<String>,
    status: AcceptedStatus,
) -> Result<AcceptedReceipt, ReceiptError> {
    let submit_id = submit_id.filter(|value| !value.trim().is_empty());
    match submit_id {
        Some(submit_id) => {
            let submit_id =
                OpaqueProviderId::new(submit_id).map_err(|_| ReceiptError::InvalidSubmitId)?;
            Ok(AcceptedReceipt { submit_id, status })
        }
        None => Err(ReceiptError::EmptySubmitId),
    }
}

fn sanitize_fail_reason(reason: Option<&str>) -> String {
    let reason = sanitize_text(reason.unwrap_or_default());
    if reason.is_empty() {
        "Dreamina returned no failure reason".to_owned()
    } else {
        reason
    }
}

fn sanitize_text(value: &str) -> String {
    let mut sanitized = String::new();
    let mut pending_space = false;
    let mut chars = 0;

    for character in value.chars() {
        if character.is_control() || character.is_whitespace() {
            pending_space = !sanitized.is_empty();
            continue;
        }
        if pending_space {
            if chars == MAX_FAIL_REASON_CHARS {
                break;
            }
            sanitized.push(' ');
            chars += 1;
            pending_space = false;
        }
        if chars == MAX_FAIL_REASON_CHARS {
            break;
        }
        sanitized.push(character);
        chars += 1;
    }

    sanitized
}
