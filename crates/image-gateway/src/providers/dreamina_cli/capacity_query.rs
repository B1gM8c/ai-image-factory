//! Same-ID, metadata-only queries for quarantined capacity. These observations
//! cannot create a task, download an artifact, settle billing, or assert NoEffect.

use std::{convert::Infallible, time::SystemTime};

use image_cli_runtime::{
    CliRuntime, ExclusiveAttemptWorkspace, ProcessCompletion, SpawnEvidence, SpawnObserver,
};
use image_provider_dreamina_cli::{
    ADAPTER_REVISION, DreaminaCliQueryPolicyV1, DreaminaQueryReceiptV1, DreaminaQueryStatusV1,
    PROVIDER_ID, QueryResultRequestV1,
};
use image_provider_sdk::ProviderTaskProgress;
use sha2::{Digest, Sha256};

use super::{
    DreaminaCliPollDriverConfigError, DreaminaCliPollProcessConfig, DreaminaCliRuntimeBindingV1,
    dreamina_operation,
    poll::{pending_progress, supported_model},
};
use crate::provider_tasks::{
    ProviderAccountHomeCapability, ProviderCapacityReconciliationLease,
    ProviderCapacityReconciliationState, ProviderCapacityTerminalState, ProviderRuntimeProfile,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderCapacityQueryObservation {
    Pending {
        next_poll_after_ms: u64,
        progress: ProviderTaskProgress,
    },
    Terminal {
        terminal_state: ProviderCapacityTerminalState,
        event_identity: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ProviderCapacityQueryError {
    #[error("capacity query binding mismatch")]
    BindingMismatch,
    #[error("capacity query requires an original remote ID")]
    MissingRemoteId,
    #[error("capacity query lease expired")]
    LeaseExpired,
    #[error("capacity query temporarily unavailable")]
    QueryUnavailable,
    #[error("capacity query receipt ID mismatch")]
    ReceiptMismatch,
    #[error("capacity query terminal authority is unclassified")]
    TerminalUnclassified,
}

impl ProviderCapacityQueryError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::BindingMismatch => "capacity_query_binding_mismatch",
            Self::MissingRemoteId => "capacity_query_remote_id_missing",
            Self::LeaseExpired => "capacity_query_lease_expired",
            Self::QueryUnavailable => "capacity_query_unavailable",
            Self::ReceiptMismatch => "capacity_query_receipt_mismatch",
            Self::TerminalUnclassified => "capacity_query_terminal_unclassified",
        }
    }

    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::QueryUnavailable | Self::TerminalUnclassified)
    }
}

pub trait ProviderCapacityQueryDriver: Send + Sync + 'static {
    fn query(
        &self,
        lease: &ProviderCapacityReconciliationLease,
    ) -> impl Future<Output = Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError>> + Send;
}

pub struct DreaminaCliCapacityQueryDriverV1 {
    runtime: CliRuntime<DreaminaCliQueryPolicyV1>,
    binding: DreaminaCliRuntimeBindingV1,
    workspace: ExclusiveAttemptWorkspace,
}

impl DreaminaCliCapacityQueryDriverV1 {
    /// This driver needs a private workspace root distinct from normal polling:
    /// ExclusiveAttemptWorkspace locks the whole root, not only its prefix.
    pub fn from_runtime_profile(
        profile: &ProviderRuntimeProfile,
        account_home: &ProviderAccountHomeCapability,
        process: DreaminaCliPollProcessConfig,
    ) -> Result<Self, DreaminaCliPollDriverConfigError> {
        let operation = dreamina_operation(profile.operation_id())
            .ok_or(DreaminaCliPollDriverConfigError::ProfileMismatch)?;
        if profile.provider_id() != PROVIDER_ID
            || profile.command_schema() != operation.command_schema
            || profile.operation_descriptor_revision() != operation.descriptor_revision
            || profile.operation_descriptor_sha256_v1() != operation.canonical_sha256_v1_hex()
            || profile.idempotency_mode() != operation.idempotency.as_str()
            || profile.adapter_revision() != ADAPTER_REVISION
        {
            return Err(DreaminaCliPollDriverConfigError::ProfileMismatch);
        }
        let home = account_home
            .bind(profile)
            .map_err(DreaminaCliPollDriverConfigError::AccountHome)?;
        let binding = DreaminaCliRuntimeBindingV1::new(
            profile.execution_profile_id(),
            profile.provider_account_id(),
            profile.credential_auth_sha256(),
        )
        .map_err(|_| DreaminaCliPollDriverConfigError::ProfileMismatch)?;
        let policy = DreaminaCliQueryPolicyV1::new(
            process.executable_path,
            process.executable_sha256,
            process.workspace_root,
            home,
            process.wall_timeout,
            process.termination_grace,
        )
        .map_err(DreaminaCliPollDriverConfigError::Policy)?;
        let workspace = ExclusiveAttemptWorkspace::acquire(
            policy.workspace_root_directory(),
            ".dreamina-capacity-query-",
        )
        .map_err(DreaminaCliPollDriverConfigError::Workspace)?;
        Ok(Self {
            runtime: CliRuntime::new(policy),
            binding,
            workspace,
        })
    }

    fn validate_lease<'a>(
        &self,
        lease: &'a ProviderCapacityReconciliationLease,
    ) -> Result<&'a str, ProviderCapacityQueryError> {
        let reconciliation = &lease.reconciliation;
        let context = lease.context();
        let operation = dreamina_operation(context.operation_id())
            .ok_or(ProviderCapacityQueryError::BindingMismatch)?;
        if reconciliation.provider_id != PROVIDER_ID
            || reconciliation.state != ProviderCapacityReconciliationState::Active
            || reconciliation.reconciliation_owner.as_deref()
                != Some(lease.reconciliation_owner.as_str())
            || reconciliation.reconciliation_lease_epoch != lease.reconciliation_lease_epoch
            || reconciliation.evidence_revision != lease.claimed_evidence_revision
            || !self
                .binding
                .matches_poll(reconciliation.provider_account_id, context)
            || context.command_schema() != operation.command_schema
            || context.operation_descriptor_revision() != operation.descriptor_revision
            || context.operation_descriptor_sha256_v1() != operation.canonical_sha256_v1_hex()
            || context.completion_mode() != operation.completion.as_str()
            || context.idempotency_mode() != operation.idempotency.as_str()
            || context.adapter_revision() != ADAPTER_REVISION
            || !supported_model(context.operation_id(), context.model())
        {
            return Err(ProviderCapacityQueryError::BindingMismatch);
        }
        // The old generation deadline has already elapsed. Only the newly
        // claimed reconciliation lease fences this bounded read-only query.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|value| i64::try_from(value.as_millis()).ok())
            .ok_or(ProviderCapacityQueryError::LeaseExpired)?;
        if lease.reconciliation_lease_expires_at_ms <= now {
            return Err(ProviderCapacityQueryError::LeaseExpired);
        }
        lease
            .remote_operation_id()
            .filter(|value| !value.is_empty())
            .ok_or(ProviderCapacityQueryError::MissingRemoteId)
    }
}

impl ProviderCapacityQueryDriver for DreaminaCliCapacityQueryDriverV1 {
    async fn query(
        &self,
        lease: &ProviderCapacityReconciliationLease,
    ) -> Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError> {
        let remote_id = self.validate_lease(lease)?;
        let attempt = self
            .workspace
            .create_attempt()
            .map_err(|_| ProviderCapacityQueryError::QueryUnavailable)?;
        let request = QueryResultRequestV1::query_only(remote_id, attempt.path())
            .map_err(|_| ProviderCapacityQueryError::BindingMismatch)?;
        let mut observer = ReceiptDigestObserver::default();
        let result = self
            .runtime
            .run_receipt(&request, &mut observer)
            .await
            .map_err(|_| ProviderCapacityQueryError::QueryUnavailable)?;
        let stdout_sha256 = observer
            .stdout_sha256
            .ok_or(ProviderCapacityQueryError::QueryUnavailable)?;
        let mut digest = Sha256::new();
        digest.update(b"factory.capacity-query-receipt.v1\0");
        digest.update(lease.reconciliation.provider_account_id.as_bytes());
        digest.update(lease.reconciliation.submission_id.as_bytes());
        digest.update(lease.reconciliation.executor_execution_id.as_bytes());
        digest.update(remote_id.as_bytes());
        digest.update([0]);
        digest.update(stdout_sha256);
        receipt_observation(
            remote_id,
            &result.receipt,
            format!("capacity-query:{}", hex::encode(digest.finalize())),
        )
    }
}

#[derive(Default)]
struct ReceiptDigestObserver {
    stdout_sha256: Option<[u8; 32]>,
}

impl SpawnObserver for ReceiptDigestObserver {
    type Error = Infallible;

    fn observe_spawn(&mut self, _: &SpawnEvidence) -> Result<(), Self::Error> {
        Ok(())
    }

    fn observe_completion(&mut self, completion: &ProcessCompletion) -> Result<(), Self::Error> {
        if completion.status.success() && !completion.stdout.is_truncated() {
            self.stdout_sha256 = Some(Sha256::digest(completion.stdout.bytes()).into());
        }
        Ok(())
    }
}

fn receipt_observation(
    remote_id: &str,
    receipt: &DreaminaQueryReceiptV1,
    event_identity: String,
) -> Result<ProviderCapacityQueryObservation, ProviderCapacityQueryError> {
    if receipt.submit_id() != remote_id {
        return Err(ProviderCapacityQueryError::ReceiptMismatch);
    }
    match receipt.status() {
        DreaminaQueryStatusV1::Querying => {
            let (next_poll_after_ms, progress) = pending_progress(receipt);
            Ok(ProviderCapacityQueryObservation::Pending {
                next_poll_after_ms,
                progress,
            })
        }
        DreaminaQueryStatusV1::Success => Ok(ProviderCapacityQueryObservation::Terminal {
            terminal_state: ProviderCapacityTerminalState::Succeeded,
            event_identity,
        }),
        DreaminaQueryStatusV1::Failed
            if receipt.failure_code() == Some("dreamina_copyright_rejected") =>
        {
            Ok(ProviderCapacityQueryObservation::Terminal {
                terminal_state: ProviderCapacityTerminalState::Failed,
                event_identity,
            })
        }
        // The generic CLI fail shape also covers unclassified query errors.
        // In particular, a not-found response cannot prove a remote terminal.
        DreaminaQueryStatusV1::Failed => Err(ProviderCapacityQueryError::TerminalUnclassified),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use image_provider_dreamina_cli::parse_query_receipt;

    use super::*;

    #[test]
    fn capacity_request_is_only_original_id_query_without_upload_or_download() {
        let request = QueryResultRequestV1::query_only("original-1", "/tmp/private-query").unwrap();
        assert!(!request.downloads_media());
        assert_eq!(
            request.to_argv(),
            ["query_result", "--submit_id", "original-1"].map(OsString::from)
        );
        for remote_id in ["", " ", "https://example.test/task", "task;$(submit)"] {
            assert!(QueryResultRequestV1::query_only(remote_id, "/tmp/private-query").is_err());
        }
    }

    #[test]
    fn only_exact_original_id_can_establish_terminal_capacity() {
        for (status, expected) in [
            ("success", ProviderCapacityTerminalState::Succeeded),
            ("fail", ProviderCapacityTerminalState::Failed),
        ] {
            let receipt = parse_query_receipt(
                serde_json::to_vec(&serde_json::json!({
                    "submit_id":"original-1", "gen_status":status,
                    "fail_reason":"生成的视频里包含受版权保护元素"
                }))
                .unwrap()
                .as_slice(),
            )
            .unwrap();
            assert_eq!(
                receipt_observation("other-2", &receipt, "event".into()),
                Err(ProviderCapacityQueryError::ReceiptMismatch)
            );
            assert_eq!(
                receipt_observation("original-1", &receipt, "event".into()),
                Ok(ProviderCapacityQueryObservation::Terminal {
                    terminal_state: expected,
                    event_identity: "event".into(),
                })
            );
        }
    }

    #[test]
    fn query_errors_and_unknown_failure_do_not_prove_no_effect_or_terminal() {
        for reason in ["not found", "404", "timeout", "", "generation failed"] {
            let receipt = parse_query_receipt(
                serde_json::to_vec(&serde_json::json!({
                    "submit_id":"original-1", "gen_status":"fail", "fail_reason":reason
                }))
                .unwrap()
                .as_slice(),
            )
            .unwrap();
            assert_eq!(
                receipt_observation("original-1", &receipt, "event".into()),
                Err(ProviderCapacityQueryError::TerminalUnclassified)
            );
        }
        assert!(parse_query_receipt(b"").is_err());
        assert!(parse_query_receipt(br#"{"error":"not found"}"#).is_err());
    }

    #[test]
    fn pending_preserves_only_sanitized_metadata_and_never_becomes_terminal() {
        let receipt = parse_query_receipt(br#"{"submit_id":"original-1","gen_status":"querying","queue_info":{"queue_idx":8,"queue_length":80,"queue_status":"Queueing","debug_info":"private"},"estimated_remaining_seconds":120}"#).unwrap();
        assert_eq!(
            receipt_observation("original-1", &receipt, "event".into()),
            Ok(ProviderCapacityQueryObservation::Pending {
                next_poll_after_ms: 30_000,
                progress: ProviderTaskProgress {
                    provider_status: "Queueing".into(),
                    queue_position: Some(8),
                    queue_total: Some(80),
                    estimated_remaining_seconds: Some(120),
                },
            })
        );
    }

    #[test]
    fn only_transient_and_unclassified_errors_are_retryable() {
        assert!(ProviderCapacityQueryError::QueryUnavailable.is_retryable());
        assert!(ProviderCapacityQueryError::TerminalUnclassified.is_retryable());
        for error in [
            ProviderCapacityQueryError::BindingMismatch,
            ProviderCapacityQueryError::MissingRemoteId,
            ProviderCapacityQueryError::LeaseExpired,
            ProviderCapacityQueryError::ReceiptMismatch,
        ] {
            assert!(!error.is_retryable());
            assert!(error.code().starts_with("capacity_query_"));
        }
    }
}
