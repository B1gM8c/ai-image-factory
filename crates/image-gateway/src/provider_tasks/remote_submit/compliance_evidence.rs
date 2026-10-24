use std::path::Path;

use image_provider_dreamina_cli::{
    ADAPTER_REVISION, DREAMINA_SUBMIT_COMMAND_SCHEMA, DREAMINA_VIDEO_GENERATION_OPERATION_V1,
    DreaminaSubmitPayloadV1, DreaminaSubmitRequestV1, PROVIDER_ID, parse_submit_command,
};
use image_provider_sdk::{EffectCertainty, OutputSlot, RetryDirective, SingleOutputCommand};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    GatedCliBinding, GatedCliObservation, GatedCliProcessOutcome, GatedCliSubmission,
    RemoteSubmitJournal, RemoteSubmitJournalObservation, RemoteSubmitJournalSpec,
    RemoteSubmitJournalTerminal,
    journal::{sha256, valid_sha256},
};
use crate::{
    ImageGatewayError,
    provider_tasks::{ProviderExecutionContext, ProviderSubmitIntent, ProviderSubmitIntentState},
    providers::dreamina_cli::DreaminaCliSubmitCodecV1,
};

pub(crate) struct VerifiedComplianceEvidence {
    pub(crate) execution_binding_sha256: String,
    pub(crate) provider_deadline_at_ms: i64,
    pub(crate) launch_nonce: Uuid,
    pub(crate) stdout_sha256: String,
}

#[derive(sqlx::FromRow)]
struct EvidenceRow {
    provider_account_id: Uuid,
    submit_owner: String,
    submit_lease_epoch: i64,
    idempotency_key: String,
    provider_command_sha256: String,
    execution_binding_sha256: String,
    provider_timeout_ms: i64,
    send_started_at_ms: Option<i64>,
    failure_event_identity: Option<String>,
    failure_error_code: Option<String>,
    updated_at_ms: i64,
    model: String,
    command_schema: String,
    command_hash: String,
    operation_id: String,
    operation_descriptor_revision: String,
    operation_descriptor_sha256_v1: String,
    completion_mode: String,
    idempotency_mode: String,
    operation_binding_version: i16,
    execution_profile_id: Uuid,
    adapter_revision: String,
    credential_pool_id: Uuid,
    credential_ref: String,
    credential_revision: i64,
    credential_auth_sha256: String,
    resource_policy_id: Uuid,
    resource_policy_revision: i64,
    invocation_attempt: i32,
    provider_deadline_at_ms: i64,
    command_json: Value,
}

/// Inspect one existing, bound CLI result. This path cannot prepare, launch,
/// release, poll, or alter any provider command or journal marker.
pub(crate) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    root: &Path,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    reviewed_stdout_sha256: &str,
) -> Result<VerifiedComplianceEvidence, ImageGatewayError> {
    if submission_id.is_nil()
        || executor_execution_id.is_nil()
        || !valid_sha256(reviewed_stdout_sha256)
    {
        return Err(conflict());
    }
    let row: EvidenceRow = sqlx::query_as(
        r#"
        SELECT intent.provider_account_id, intent.submit_owner,
               intent.submit_lease_epoch, intent.idempotency_key,
               intent.provider_command_sha256, intent.execution_binding_sha256,
               intent.provider_timeout_ms, intent.send_started_at_ms,
               intent.failure_event_identity, intent.failure_error_code,
               intent.updated_at_ms,
               submission.model, submission.command_schema, submission.command_hash,
               submission.operation_id, submission.operation_descriptor_revision,
               submission.operation_descriptor_sha256_v1, submission.completion_mode,
               submission.idempotency_mode, submission.operation_binding_version,
               submission.execution_profile_id, submission.adapter_revision,
               submission.credential_pool_id, submission.credential_ref,
               submission.credential_revision, account.credential_auth_sha256,
               submission.resource_policy_id, submission.resource_policy_revision,
               recovery.invocation_attempt, recovery.provider_deadline_at_ms,
               payload.command_json
        FROM provider_remote_submit_intents intent
        JOIN provider_submissions submission
          ON submission.submission_id = intent.submission_id
         AND submission.executor_execution_id = intent.executor_execution_id
         AND submission.provider_id = intent.provider_id
         AND submission.provider_account_id = intent.provider_account_id
        JOIN executor_executions execution
          ON execution.executor_execution_id = intent.executor_execution_id
         AND execution.submission_id = intent.submission_id
        JOIN provider_submit_recoveries recovery
          ON recovery.submission_id = intent.submission_id
         AND recovery.executor_execution_id = intent.executor_execution_id
         AND recovery.provider_timeout_ms = intent.provider_timeout_ms
        JOIN provider_accounts account
          ON account.provider_account_id = submission.provider_account_id
         AND account.provider_id = submission.provider_id
         AND account.credential_pool_id = submission.credential_pool_id
         AND account.credential_ref = submission.credential_ref
         AND account.credential_revision = submission.credential_revision
        JOIN job_payloads payload ON payload.job_id = submission.job_id
         AND payload.command_schema = submission.command_schema
        JOIN jobs job ON job.job_id = submission.job_id
        WHERE intent.submission_id = $1 AND intent.executor_execution_id = $2
          AND intent.provider_id = 'dreamina-cli'
          AND intent.state = 'deadline_quarantined'
          AND intent.remote_operation_id IS NULL AND intent.provider_request_id IS NULL
          AND intent.receipt_event_identity IS NULL
          AND intent.output_index = 0 AND intent.output_total = 1
          AND intent.failure_error_code = 'dreamina_submit_rejected'
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND recovery.state = 'closed' AND recovery.invocation_attempt = 1
          AND recovery.recovery_owner IS NULL AND recovery.recovery_lease_expires_at_ms IS NULL
          AND job.operation = 'video_generation' AND job.output_count = 1
          AND NOT EXISTS (SELECT 1 FROM provider_remote_tasks task
                          WHERE task.submission_id = intent.submission_id)
          AND NOT EXISTS (SELECT 1 FROM provider_submit_quarantined_receipts receipt
                          WHERE receipt.submission_id = intent.submission_id)
        FOR UPDATE OF intent, submission, execution, recovery
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| ImageGatewayError::service_unavailable("Dreamina rejection evidence unavailable"))?
    .ok_or_else(conflict)?;

    let intent = ProviderSubmitIntent {
        submission_id,
        executor_execution_id,
        provider_id: PROVIDER_ID.to_owned(),
        provider_account_id: row.provider_account_id,
        submit_owner: row.submit_owner,
        submit_lease_epoch: row.submit_lease_epoch,
        idempotency_key: row.idempotency_key.clone(),
        output_index: 0,
        output_total: 1,
        provider_command_sha256: row.provider_command_sha256.clone(),
        execution_binding_sha256: row.execution_binding_sha256.clone(),
        provider_timeout_ms: row.provider_timeout_ms,
        state: ProviderSubmitIntentState::DeadlineQuarantined,
        remote_operation_id: None,
        provider_request_id: None,
        send_started_at_ms: row.send_started_at_ms,
        receipt_event_identity: None,
        failure_event_identity: row.failure_event_identity,
        failure_error_code: row.failure_error_code,
        updated_at_ms: row.updated_at_ms,
    };
    let context = ProviderExecutionContext {
        model: row.model,
        command_schema: row.command_schema,
        command_hash: row.command_hash,
        operation_id: row.operation_id,
        operation_descriptor_revision: row.operation_descriptor_revision,
        operation_descriptor_sha256_v1: row.operation_descriptor_sha256_v1,
        completion_mode: row.completion_mode,
        idempotency_mode: row.idempotency_mode,
        operation_binding_version: row.operation_binding_version,
        execution_profile_id: row.execution_profile_id,
        adapter_revision: row.adapter_revision,
        credential_pool_id: row.credential_pool_id,
        credential_ref: row.credential_ref,
        credential_revision: row.credential_revision,
        credential_auth_sha256: row.credential_auth_sha256,
        resource_policy_id: row.resource_policy_id,
        resource_policy_revision: row.resource_policy_revision,
        submission_idempotency_key: row.idempotency_key,
        provider_command_sha256: row.provider_command_sha256,
        execution_binding_sha256: row.execution_binding_sha256,
        invocation_attempt: row.invocation_attempt,
        provider_timeout_ms: row.provider_timeout_ms,
        provider_deadline_at_ms: row.provider_deadline_at_ms,
    };
    let source_bytes = serde_json::to_vec(&row.command_json).map_err(|_| conflict())?;
    let request = parse_submit_command(&source_bytes).map_err(|_| conflict())?;
    if context.command_schema() != DREAMINA_SUBMIT_COMMAND_SCHEMA
        || context.adapter_revision() != ADAPTER_REVISION
        || context.operation_id() != DREAMINA_VIDEO_GENERATION_OPERATION_V1.id
        || !matches!(
            request,
            DreaminaSubmitRequestV1::TextToVideo(_)
                | DreaminaSubmitRequestV1::ImageToVideo(_)
                | DreaminaSubmitRequestV1::FramesToVideo(_)
                | DreaminaSubmitRequestV1::MultiframeToVideo(_)
                | DreaminaSubmitRequestV1::MultimodalToVideo(_)
        )
    {
        return Err(conflict());
    }
    let payload =
        DreaminaSubmitPayloadV1::new(context.command_hash(), request).map_err(|_| conflict())?;
    let command = SingleOutputCommand::new(OutputSlot::new(0, 1).map_err(|_| conflict())?, payload)
        .map_err(|_| conflict())?;
    if hex::encode(command.canonical_sha256()) != intent.provider_command_sha256 {
        return Err(conflict());
    }
    let spec = RemoteSubmitJournalSpec::new(&intent, &context, command.canonical_payload())
        .map_err(|_| conflict())?;
    let journal = RemoteSubmitJournal::open_existing(root).map_err(|_| conflict())?;
    if !matches!(
        journal.observe(&spec).map_err(|_| conflict())?,
        RemoteSubmitJournalObservation::Terminal(RemoteSubmitJournalTerminal::Unknown {
            error_code
        }) if error_code == "dreamina_submit_rejected"
    ) {
        return Err(conflict());
    }
    let launch_nonce = journal
        .released_authority(&spec)
        .map_err(|_| conflict())?
        .launch_nonce();
    let binding = GatedCliBinding::new(
        context.execution_binding_sha256(),
        launch_nonce,
        u64::try_from(context.provider_deadline_at_ms()).map_err(|_| conflict())?,
    )
    .map_err(|_| conflict())?;
    let process = GatedCliSubmission::open_existing(root, submission_id).map_err(|_| conflict())?;
    let GatedCliObservation::Terminal(terminal) =
        process.observe(&binding).map_err(|_| conflict())?
    else {
        return Err(conflict());
    };
    verify_terminal(submission_id, &terminal, reviewed_stdout_sha256)?;
    Ok(VerifiedComplianceEvidence {
        execution_binding_sha256: context.execution_binding_sha256().to_owned(),
        provider_deadline_at_ms: context.provider_deadline_at_ms(),
        launch_nonce,
        stdout_sha256: reviewed_stdout_sha256.to_owned(),
    })
}

pub(super) fn verify_terminal(
    submission_id: Uuid,
    terminal: &super::GatedCliProcessTerminal,
    reviewed_stdout_sha256: &str,
) -> Result<(), ImageGatewayError> {
    if !terminal.released()
        || !terminal.exec_started()
        || terminal.stdout_truncated()
        || terminal.stderr_truncated()
        || !matches!(
            terminal.outcome(),
            GatedCliProcessOutcome::Exited {
                exit_code: Some(0),
                signal: None
            }
        )
        || sha256(terminal.stdout()) != reviewed_stdout_sha256
    {
        return Err(conflict());
    }
    let failure = DreaminaCliSubmitCodecV1::decode_for_submission(submission_id, terminal.stdout())
        .err()
        .ok_or_else(conflict)?;
    if failure.code() != "dreamina_compliance_confirmation_required"
        || failure.effect() != EffectCertainty::NoRemoteEffect
        || failure.retry() != RetryDirective::Never
    {
        return Err(conflict());
    }
    Ok(())
}

fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Existing Dreamina command does not prove the reviewed compliance rejection",
        None,
        "dreamina_rejection_evidence_conflict",
    )
}
