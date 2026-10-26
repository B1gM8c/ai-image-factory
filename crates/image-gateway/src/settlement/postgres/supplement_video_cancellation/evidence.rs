use std::{fs::File, path::Path};

use rustix::fs::{Mode, OFlags, open};
use serde::Deserialize;
use uuid::Uuid;

use super::super::manual_video_cancellation::evidence as base;
use crate::{ImageGatewayError, artifacts::sha256_hex};

pub(super) struct ExpectedIdentity {
    pub base: base::ExpectedIdentity,
    pub authorization_id: Uuid,
    pub dispatch_nonce: Uuid,
    pub quote_hash: String,
    pub command_hash: String,
    pub submit_stdout_sha256: String,
}

pub(super) struct VerifiedEvidence {
    pub base: base::VerifiedEvidence,
    pub head_review_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadReview {
    schema: String,
    authorization_id: Uuid,
    dispatch_nonce: Uuid,
    job_id: Uuid,
    request_id: String,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_account_id: Uuid,
    upstream_task_id: String,
    quote_hash: String,
    command_hash: String,
    submit_stdout_sha256: String,
    operator_reference: String,
    query_stdout_sha256: String,
    base_review_sha256: String,
    original_effect_certainty: String,
    customer_charge_waived: bool,
}

/// In addition to the real head-task cancellation evidence, require an explicit
/// operator waiver of customer charges for the still-unknown original attempt.
/// This review never claims that the original attempt had no provider effect.
pub(super) fn load(
    root: &Path,
    expected: &ExpectedIdentity,
) -> Result<VerifiedEvidence, ImageGatewayError> {
    let verified = base::load(root, &expected.base)?;
    let directory: File = open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    base::private_metadata(&directory, true)?;
    let bytes = base::read(&directory, "head-review.json", 64 * 1024)?;
    let head_review_sha256 = validate_head_review(&bytes, expected, &verified)?;
    Ok(VerifiedEvidence {
        base: verified,
        head_review_sha256,
    })
}

fn validate_head_review(
    bytes: &[u8],
    expected: &ExpectedIdentity,
    verified: &base::VerifiedEvidence,
) -> Result<String, ImageGatewayError> {
    let review: HeadReview = serde_json::from_slice(bytes).map_err(|_| conflict())?;
    if expected.authorization_id.is_nil()
        || expected.dispatch_nonce.is_nil()
        || !valid_sha256(&expected.quote_hash)
        || !valid_sha256(&expected.command_hash)
        || !valid_sha256(&expected.submit_stdout_sha256)
        || review.schema != "factory.supplement_cancellation.review.v1"
        || review.authorization_id != expected.authorization_id
        || review.dispatch_nonce != expected.dispatch_nonce
        || review.job_id != expected.base.job_id
        || review.request_id != expected.base.request_id
        || review.submission_id != expected.base.submission_id
        || review.executor_execution_id != expected.base.executor_execution_id
        || review.provider_account_id != expected.base.provider_account_id
        || review.upstream_task_id != expected.base.upstream_task_id
        || review.quote_hash != expected.quote_hash
        || review.command_hash != expected.command_hash
        || review.submit_stdout_sha256 != expected.submit_stdout_sha256
        || review.operator_reference != expected.base.operator_reference
        || review.query_stdout_sha256 != verified.query_stdout_sha256
        || review.base_review_sha256 != verified.review_sha256
        || review.original_effect_certainty != "unknown_remote_effect"
        || !review.customer_charge_waived
    {
        return Err(conflict());
    }
    Ok(sha256_hex(bytes))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Dreamina supplement cancellation conflicts with private head review",
        None,
        "dreamina_supplement_video_cancellation_conflict",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn fixture() -> (ExpectedIdentity, base::VerifiedEvidence, Value) {
        let expected = ExpectedIdentity {
            base: base::ExpectedIdentity {
                job_id: Uuid::new_v4(),
                request_id: "original-request".into(),
                submission_id: Uuid::new_v4(),
                executor_execution_id: Uuid::new_v4(),
                provider_account_id: Uuid::new_v4(),
                upstream_task_id: "supplement-head-1".into(),
                operator_reference: "offline:platform-cost-waiver".into(),
                query_stdout_sha256: "a".repeat(64),
            },
            authorization_id: Uuid::new_v4(),
            dispatch_nonce: Uuid::new_v4(),
            quote_hash: "b".repeat(64),
            command_hash: "c".repeat(64),
            submit_stdout_sha256: "d".repeat(64),
        };
        let verified = base::VerifiedEvidence {
            query_stdout_sha256: expected.base.query_stdout_sha256.clone(),
            execution_audit_sha256: "e".repeat(64),
            review_sha256: "f".repeat(64),
            website_confirmation_sha256: "1".repeat(64),
            upstream_identity_sha256: "2".repeat(64),
            account_home_sha256: "3".repeat(64),
            queried_at_ms: 2000,
            website_confirmed_at_ms: 1000,
        };
        let review = json!({
            "schema":"factory.supplement_cancellation.review.v1",
            "authorization_id":expected.authorization_id,
            "dispatch_nonce":expected.dispatch_nonce,
            "job_id":expected.base.job_id,
            "request_id":expected.base.request_id,
            "submission_id":expected.base.submission_id,
            "executor_execution_id":expected.base.executor_execution_id,
            "provider_account_id":expected.base.provider_account_id,
            "upstream_task_id":expected.base.upstream_task_id,
            "quote_hash":expected.quote_hash,
            "command_hash":expected.command_hash,
            "submit_stdout_sha256":expected.submit_stdout_sha256,
            "operator_reference":expected.base.operator_reference,
            "query_stdout_sha256":verified.query_stdout_sha256,
            "base_review_sha256":verified.review_sha256,
            "original_effect_certainty":"unknown_remote_effect",
            "customer_charge_waived":true,
        });
        (expected, verified, review)
    }

    #[test]
    fn accepts_same_head_review_with_explicit_original_unknown_cost_waiver() {
        let (expected, verified, review) = fixture();
        let bytes = serde_json::to_vec(&review).unwrap();
        assert_eq!(
            validate_head_review(&bytes, &expected, &verified).unwrap(),
            sha256_hex(&bytes)
        );
    }

    #[test]
    fn rejects_cross_head_identity_nonce_hash_and_waiver_changes() {
        for (field, value) in [
            ("schema", json!("factory.manual_cancellation.review.v1")),
            ("authorization_id", json!(Uuid::new_v4())),
            ("dispatch_nonce", json!(Uuid::new_v4())),
            ("job_id", json!(Uuid::new_v4())),
            ("request_id", json!("another-request")),
            ("submission_id", json!(Uuid::new_v4())),
            ("executor_execution_id", json!(Uuid::new_v4())),
            ("provider_account_id", json!(Uuid::new_v4())),
            ("upstream_task_id", json!("another-head")),
            ("quote_hash", json!("4".repeat(64))),
            ("command_hash", json!("5".repeat(64))),
            ("submit_stdout_sha256", json!("6".repeat(64))),
            ("operator_reference", json!("another-reviewer")),
            ("query_stdout_sha256", json!("7".repeat(64))),
            ("base_review_sha256", json!("8".repeat(64))),
            ("original_effect_certainty", json!("confirmed_no_effect")),
            ("customer_charge_waived", json!(false)),
        ] {
            let (expected, verified, mut review) = fixture();
            review[field] = value;
            assert!(
                validate_head_review(&serde_json::to_vec(&review).unwrap(), &expected, &verified)
                    .is_err(),
                "accepted changed {field}"
            );
        }
    }

    #[test]
    fn requires_every_review_field_and_rejects_unreviewed_extensions() {
        let (expected, verified, review) = fixture();
        for field in review.as_object().unwrap().keys() {
            let mut missing = review.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                validate_head_review(&serde_json::to_vec(&missing).unwrap(), &expected, &verified)
                    .is_err(),
                "accepted missing {field}"
            );
        }
        let mut extended = review;
        extended["provider_no_effect"] = json!(true);
        assert!(
            validate_head_review(
                &serde_json::to_vec(&extended).unwrap(),
                &expected,
                &verified
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_expected_head_identity_even_with_matching_review() {
        for (field, value) in [
            ("authorization_id", json!(Uuid::nil())),
            ("dispatch_nonce", json!(Uuid::nil())),
            ("quote_hash", json!("invalid")),
            ("command_hash", json!("invalid")),
            ("submit_stdout_sha256", json!("invalid")),
        ] {
            let (mut expected, verified, mut review) = fixture();
            match field {
                "authorization_id" => expected.authorization_id = Uuid::nil(),
                "dispatch_nonce" => expected.dispatch_nonce = Uuid::nil(),
                "quote_hash" => expected.quote_hash = "invalid".into(),
                "command_hash" => expected.command_hash = "invalid".into(),
                "submit_stdout_sha256" => expected.submit_stdout_sha256 = "invalid".into(),
                _ => unreachable!(),
            }
            review[field] = value;
            assert!(
                validate_head_review(&serde_json::to_vec(&review).unwrap(), &expected, &verified)
                    .is_err()
            );
        }
    }
}
