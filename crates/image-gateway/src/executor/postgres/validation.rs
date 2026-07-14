use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::admission::WorkLease;

use super::super::{
    ExecutorClaimScope, ExecutorSubmissionError, ExecutorSubmissionLease,
    ExecutorSubmissionOutcome, error_code_is_valid, result_manifest_is_valid,
};

const MAX_IMAGE_OUTPUTS: i32 = 10;
const MAX_EXECUTOR_LEASE_MS: i64 = 24 * 60 * 60 * 1_000;

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
    validate_owner(owner)?;
    validate_lease_duration(lease_ms)
}

pub(super) fn validate_owner(owner: &str) -> Result<(), ExecutorSubmissionError> {
    if is_executor_owner(owner) {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::InvalidInput)
    }
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
        return if error_code_is_valid(error_code) {
            Ok(())
        } else {
            Err(ExecutorSubmissionError::InvalidInput)
        };
    }
    let manifest = outcome
        .manifest()
        .ok_or(ExecutorSubmissionError::InvalidInput)?;
    if !result_manifest_is_valid(manifest) {
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
