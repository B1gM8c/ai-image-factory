use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::admission::WorkLease;

use super::super::{
    ExecutorArtifactAuthority, ExecutorClaimScope, ExecutorSubmissionError,
    ExecutorSubmissionLease, ExecutorSubmissionOutcome, artifact_authority_is_valid,
    error_code_is_valid, result_manifest_is_valid,
};

const MAX_IMAGE_OUTPUTS: i32 = 10;
const MAX_EXECUTOR_LEASE_MS: i64 = 24 * 60 * 60 * 1_000;

pub(super) fn command_output_count(
    requested_units: i32,
    command_json: &Value,
) -> Result<i32, ExecutorSubmissionError> {
    if !(1..=MAX_IMAGE_OUTPUTS).contains(&requested_units) {
        return Err(ExecutorSubmissionError::InvalidInput);
    }
    if let Some(value) = command_json.get("n") {
        let command_units = value
            .as_u64()
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| (1..=MAX_IMAGE_OUTPUTS).contains(value))
            .ok_or(ExecutorSubmissionError::InvalidInput)?;
        if requested_units != command_units {
            return Err(ExecutorSubmissionError::Conflict);
        }
    }
    Ok(requested_units)
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
    if !scope.execution_profile_id.is_nil()
        && is_bounded_identifier(&scope.provider_id)
        && is_bounded_identifier(&scope.command_schema)
        && is_bounded_identifier(&scope.adapter_revision)
    {
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
    if is_executor_owner(&lease.executor_owner)
        && lease.executor_lease_epoch > 0
        && !lease.submission_id.is_nil()
        && !lease.executor_execution_id.is_nil()
        && lease.submission_id != lease.executor_execution_id
        && !lease.output_id.is_nil()
        && !lease.job_id.is_nil()
        && !lease.work_item_id.is_nil()
        && !lease.execution_profile_id.is_nil()
        && is_bounded_identifier(&lease.adapter_revision)
    {
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

pub(super) fn validate_artifact_authority(
    authority: &ExecutorArtifactAuthority,
) -> Result<(), ExecutorSubmissionError> {
    if artifact_authority_is_valid(authority) {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::InvalidInput)
    }
}

pub(super) fn distinct_execution_id(work_execution_id: Uuid, submission_id: Uuid) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if candidate != work_execution_id && candidate != submission_id {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn requested_units_are_authoritative_for_provider_specific_commands() {
        assert_eq!(
            command_output_count(
                1,
                &json!({
                    "operation": "text2image",
                    "generate_num": 1
                })
            ),
            Ok(1)
        );
    }

    #[test]
    fn optional_openai_count_must_match_the_durable_job() {
        assert_eq!(command_output_count(2, &json!({"n": 2})), Ok(2));
        assert_eq!(
            command_output_count(2, &json!({"n": 1})),
            Err(ExecutorSubmissionError::Conflict)
        );
        assert_eq!(
            command_output_count(1, &json!({"n": "one"})),
            Err(ExecutorSubmissionError::InvalidInput)
        );
    }

    #[test]
    fn durable_job_count_remains_bounded_without_a_command_count_field() {
        assert_eq!(
            command_output_count(0, &json!({"operation": "text2image"})),
            Err(ExecutorSubmissionError::InvalidInput)
        );
        assert_eq!(
            command_output_count(11, &json!({"operation": "text2image"})),
            Err(ExecutorSubmissionError::InvalidInput)
        );
    }
}
