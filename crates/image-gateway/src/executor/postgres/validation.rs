use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::admission::WorkLease;

use super::super::{
    ExecutorClaimScope, ExecutorSubmissionError, ExecutorSubmissionLease, ExecutorSubmissionOutcome,
};

const MAX_IMAGE_OUTPUTS: i32 = 10;
const MAX_EXECUTOR_LEASE_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_RESULT_BYTES: u64 = 256 * 1024 * 1024;

pub(super) fn command_output_count(
    requested_units: i32,
    command_json: &Value,
) -> Result<i32, ExecutorSubmissionError> {
    let output_count = command_json
        .get("n")
        .and_then(Value::as_u64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| (1..=MAX_IMAGE_OUTPUTS).contains(value))
        .ok_or(ExecutorSubmissionError::InvalidInput)?;
    if requested_units != output_count {
        return Err(ExecutorSubmissionError::Conflict);
    }
    Ok(output_count)
}

pub(super) fn command_hash(command: &Value) -> Result<String, ExecutorSubmissionError> {
    let bytes = serde_json::to_vec(command).map_err(|_| ExecutorSubmissionError::InvalidInput)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(super) fn validate_work_lease(lease: &WorkLease) -> Result<(), ExecutorSubmissionError> {
    if lease.lease_epoch <= 0 || lease.worker_id.is_empty() || lease.command_schema.is_empty() {
        Err(ExecutorSubmissionError::InvalidInput)
    } else {
        Ok(())
    }
}

pub(super) fn validate_owner_and_duration(
    owner: &str,
    lease_ms: i64,
) -> Result<(), ExecutorSubmissionError> {
    if !is_executor_owner(owner) {
        return Err(ExecutorSubmissionError::InvalidInput);
    }
    validate_lease_duration(lease_ms)
}

pub(super) fn validate_claim_scope(
    scope: &ExecutorClaimScope,
) -> Result<(), ExecutorSubmissionError> {
    if is_bounded_identifier(&scope.provider_id) && is_bounded_identifier(&scope.command_schema) {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::InvalidInput)
    }
}

pub(super) fn validate_lease_duration(lease_ms: i64) -> Result<(), ExecutorSubmissionError> {
    if (1..=MAX_EXECUTOR_LEASE_MS).contains(&lease_ms) {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::InvalidInput)
    }
}

pub(super) fn validate_executor_lease(
    lease: &ExecutorSubmissionLease,
) -> Result<(), ExecutorSubmissionError> {
    if is_executor_owner(&lease.executor_owner) && lease.executor_lease_epoch > 0 {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::InvalidInput)
    }
}

pub(super) fn validate_outcome(
    outcome: &ExecutorSubmissionOutcome,
) -> Result<(), ExecutorSubmissionError> {
    if let Some(error_code) = outcome.error_code() {
        return if is_error_code(error_code) {
            Ok(())
        } else {
            Err(ExecutorSubmissionError::InvalidInput)
        };
    }
    let manifest = outcome
        .manifest()
        .ok_or(ExecutorSubmissionError::InvalidInput)?;
    if manifest.storage_backend.is_empty()
        || manifest.storage_backend.len() > 128
        || manifest.object_key.is_empty()
        || manifest.object_key.len() > 1_024
        || manifest
            .object_key
            .bytes()
            .any(|byte| byte.is_ascii_control())
        || !is_sha256(&manifest.sha256_hex)
        || manifest.byte_size == 0
        || manifest.byte_size > MAX_RESULT_BYTES
        || !matches!(
            manifest.media_type.as_str(),
            "image/png" | "image/jpeg" | "image/webp"
        )
    {
        return Err(ExecutorSubmissionError::InvalidInput);
    }
    Ok(())
}

pub(super) fn distinct_execution_id(work_execution_id: Uuid) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if candidate != work_execution_id {
            return candidate;
        }
    }
}

fn is_bounded_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn is_executor_owner(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn is_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
