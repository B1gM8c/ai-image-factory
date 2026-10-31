//! Operator reconciliation of a historical, proven accepted upstream failure.
//! Never dispatches the provider or changes immutable executor terminal evidence.
use std::path::Path;

use serde::Serialize;
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

use crate::ImageGatewayError;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DreaminaAcceptedTerminalResolutionRequest {
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub reviewed_stdout_sha256: String,
    pub reviewed_query_audit_sha256: String,
    pub expected_remote_id: String,
    pub expected_account_id: Uuid,
    pub expected_hold_id: Uuid,
    pub expected_quote_hash: String,
    pub expected_request_hash: String,
    pub expected_command_hash: String,
    pub operator_reference: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, sqlx::FromRow)]
pub struct DreaminaAcceptedTerminalResolution {
    pub resolution_id: Uuid,
    pub job_id: Uuid,
    pub request_id: String,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub hold_id: Uuid,
    pub currency: String,
    pub released_units: i32,
    pub released_micros: i64,
    pub captured_micros: i64,
}

use super::operator_failure_release::LockedJob;

/// Requires the exact job/execution/submission and an independently reviewed
/// stdout digest. All projections and local capacity/money releases commit once.
pub async fn resolve_dreamina_accepted_terminal(
    pool: &PgPool,
    evidence_root: &Path,
    original_journal_entry: &Path,
    request: &DreaminaAcceptedTerminalResolutionRequest,
) -> Result<DreaminaAcceptedTerminalResolution, ImageGatewayError> {
    if request.job_id.is_nil()
        || request.submission_id.is_nil()
        || request.executor_execution_id.is_nil()
        || request.submission_id == request.executor_execution_id
        || !super::journal::valid_sha256(&request.reviewed_query_audit_sha256)
        || request.expected_account_id.is_nil()
        || request.expected_hold_id.is_nil()
        || request.expected_remote_id.is_empty()
        || request.expected_remote_id.len() > 255
        || request.expected_remote_id.chars().any(char::is_control)
        || [
            &request.expected_quote_hash,
            &request.expected_request_hash,
            &request.expected_command_hash,
        ]
        .iter()
        .any(|h| !super::journal::valid_sha256(h))
        || request.reviewed_stdout_sha256.len() != 64
        || !request
            .reviewed_stdout_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || request.operator_reference.trim().is_empty()
        || request.operator_reference.len() > 255
        || request.operator_reference.chars().any(char::is_control)
    {
        return Err(ImageGatewayError::config(
            "invalid Dreamina accepted terminal resolution identity",
        ));
    }
    let review_sha256 =
        crate::artifacts::sha256_hex(&serde_json::to_vec(request).map_err(|_| conflict())?);
    let mut tx = pool.begin().await.map_err(unavailable)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    // Match the quota and customer settlement lock order before locking the job.
    let (tenant_id, currency): (String, String) = sqlx::query_as(
        "SELECT job.tenant_id, hold.currency FROM jobs job JOIN customer_billing_holds hold USING(job_id) WHERE job.job_id = $1",
    ).bind(request.job_id).fetch_optional(&mut *tx).await.map_err(unavailable)?
        .ok_or_else(conflict)?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::usage::quota_lock_id(&tenant_id))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("budget:{tenant_id}:{currency}"))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SELECT 1 FROM billing_accounts WHERE tenant_id = $1 AND currency = $2 FOR UPDATE")
        .bind(&tenant_id)
        .bind(&currency)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SELECT 1 FROM jobs WHERE job_id = $1 FOR UPDATE")
        .bind(request.job_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;

    let existing: Option<(Uuid, Uuid, String, String)> = sqlx::query_as(
        "SELECT submission_id, executor_execution_id, reviewed_stdout_sha256, review_sha256 FROM dreamina_accepted_terminal_resolutions WHERE job_id = $1",
    ).bind(request.job_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if let Some((submission_id, execution_id, sha, operator_reference)) = existing {
        if submission_id != request.submission_id
            || execution_id != request.executor_execution_id
            || sha != request.reviewed_stdout_sha256
            || operator_reference != review_sha256
        {
            return Err(conflict());
        }
        sqlx::query("SELECT assert_dreamina_accepted_terminal($1)")
            .bind(request.job_id)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?;
        let result = load_result(&mut tx, request.job_id).await?;
        tx.commit().await.map_err(storage_conflict)?;
        return Ok(result);
    }

    let locked: LockedJob = sqlx::query_as(
        r#"
        SELECT job.tenant_id, job.request_id, job.operation, work.work_item_id,
               quota.reservation_id, quota.requested_units, quota.billing_metric,
               quota.billing_unit, hold.hold_id, hold.currency, hold.held_micros,
               hold.grant_held_micros, hold.account_held_micros
        FROM jobs job
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt ON attempt.work_item_id = work.work_item_id
          AND attempt.execution_id = work.execution_id AND attempt.lease_epoch = work.lease_epoch
        JOIN job_outputs output ON output.job_id = job.job_id
        JOIN provider_submissions submission ON submission.job_id = job.job_id AND submission.output_id = output.output_id
        JOIN executor_executions execution ON execution.executor_execution_id = submission.executor_execution_id
          AND execution.submission_id = submission.submission_id
        JOIN provider_remote_submit_intents intent ON intent.submission_id = submission.submission_id
        JOIN provider_submit_recoveries recovery ON recovery.submission_id = submission.submission_id
        JOIN executor_terminal_reductions reduction ON reduction.submission_id = submission.submission_id
        JOIN provider_remote_tasks task ON task.submission_id = submission.submission_id
          AND task.executor_execution_id = execution.executor_execution_id
        JOIN provider_remote_task_quarantines quarantine ON quarantine.quarantine_id = task.deadline_quarantine_id
          AND quarantine.submission_id = submission.submission_id
        JOIN executor_capacity_allocations allocation ON allocation.submission_id = submission.submission_id
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id
          AND quota.job_id = job.job_id AND quota.tenant_id = job.tenant_id
        JOIN customer_billing_holds hold ON hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        WHERE job.job_id = $1 AND submission.submission_id = $2
          AND execution.executor_execution_id = $3
          AND job.provider_id = 'dreamina-cli' AND job.operation = 'video_generation'
          AND job.output_count = 1 AND job.economics_contract_version = 4
          AND job.state = 'uncertain' AND job.charged_units = 0
          AND work.state = 'uncertain' AND work.kind = 'video_single'
          AND work.lease_owner IS NULL AND work.lease_expires_at_ms IS NULL
          AND attempt.state = 'uncertain' AND output.state = 'uncertain'
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND intent.state = 'attached' AND intent.remote_operation_id = task.remote_operation_id
          AND intent.receipt_event_identity IS NOT NULL
          AND task.state = 'uncertain' AND task.error_code = 'provider_remote_task_deadline'
          AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
          AND task.next_poll_at_ms IS NULL AND NOT task.cancel_requested
          AND submission.error_code = 'provider_remote_task_deadline'
          AND execution.error_code = 'provider_remote_task_deadline'
          AND recovery.state = 'closed' AND recovery.recovery_owner IS NULL
          AND recovery.recovery_lease_expires_at_ms IS NULL
          AND reduction.state = 'completed' AND reduction.resolved_state = 'uncertain'
          AND allocation.state = 'held'
          AND quota.state IN ('reserved', 'expired') AND quota.committed_units = 0 AND quota.released_units = 0
          AND quota.requested_units = job.billable_units
          AND hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0
          AND hold.grant_captured_micros = 0 AND hold.account_captured_micros = 0
          AND hold.grant_released_micros = 0 AND hold.account_released_micros = 0
        FOR UPDATE OF job, work, attempt, output, submission, execution, intent, recovery, reduction, allocation, quota, hold, task, quarantine
        "#,
    ).bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id)
        .fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
    let identity: Original = sqlx::query_as(r#"
      SELECT sub.provider_account_id, sub.command_hash, sub.execution_profile_id, sub.credential_pool_id,
             sub.credential_revision, account.credential_auth_sha256, intent.execution_binding_sha256,
             intent.provider_command_sha256, task.remote_operation_id, quarantine.quarantine_id,
             quarantine.quarantined_at_ms, budget.completion_deadline_at_ms, payload.command_json,
             payload.request_hash, quote.quote_hash
      FROM provider_submissions sub JOIN provider_remote_submit_intents intent USING(submission_id)
      JOIN provider_accounts account ON account.provider_account_id=sub.provider_account_id
        AND account.credential_ref=sub.credential_ref AND account.credential_revision=sub.credential_revision
        AND account.provider_id='dreamina-cli' AND account.credential_pool_id=sub.credential_pool_id
      JOIN provider_remote_tasks task ON task.submission_id=sub.submission_id
      JOIN provider_remote_task_quarantines quarantine ON quarantine.quarantine_id=task.deadline_quarantine_id
      JOIN accepted_completion_budgets budget ON budget.submission_id=sub.submission_id
      JOIN job_payloads payload ON payload.job_id=sub.job_id
      JOIN customer_billing_holds hold ON hold.job_id=sub.job_id
      JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id AND quote.job_id=sub.job_id
      WHERE sub.submission_id=$1 AND sub.executor_execution_id=$2 AND sub.job_id=$3
        AND budget.remote_operation_id=task.remote_operation_id AND budget.policy_version=1
      "#).bind(request.submission_id).bind(request.executor_execution_id).bind(request.job_id)
      .fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
    if identity.provider_account_id != request.expected_account_id
        || identity.remote_operation_id != request.expected_remote_id
        || locked.hold_id != request.expected_hold_id
        || identity.quote_hash != request.expected_quote_hash
        || identity.request_hash != request.expected_request_hash
        || identity.command_hash != request.expected_command_hash
    {
        return Err(conflict());
    }
    let parsed = image_provider_dreamina_cli::parse_submit_command(
        &serde_json::to_vec(&identity.command_json).map_err(|_| conflict())?,
    )
    .map_err(|_| conflict())?;
    let prompt = match &parsed {
        image_provider_dreamina_cli::DreaminaSubmitRequestV1::TextToVideo(r) => r.prompt(),
        image_provider_dreamina_cli::DreaminaSubmitRequestV1::ImageToVideo(r) => {
            r.options().prompt()
        }
        image_provider_dreamina_cli::DreaminaSubmitRequestV1::FramesToVideo(r) => {
            r.options().prompt()
        }
        image_provider_dreamina_cli::DreaminaSubmitRequestV1::MultimodalToVideo(r) => r.prompt(),
        _ => return Err(conflict()),
    }
    .to_owned();
    let payload =
        image_provider_dreamina_cli::DreaminaSubmitPayloadV1::new(&identity.command_hash, parsed)
            .map_err(|_| conflict())?;
    let command = image_provider_sdk::SingleOutputCommand::new(
        image_provider_sdk::OutputSlot::new(0, 1).map_err(|_| conflict())?,
        payload,
    )
    .map_err(|_| conflict())?;
    if hex::encode(command.canonical_sha256()) != identity.provider_command_sha256 {
        return Err(conflict());
    }
    let evidence = super::accepted_terminal_evidence::load(
        evidence_root,
        original_journal_entry,
        &super::accepted_terminal_evidence::Identity {
            job_id: request.job_id,
            submission_id: request.submission_id,
            execution_id: request.executor_execution_id,
            account_id: identity.provider_account_id,
            remote_id: &identity.remote_operation_id,
            binding_sha256: &identity.execution_binding_sha256,
            provider_command_sha256: &identity.provider_command_sha256,
            reviewed_stdout_sha256: &request.reviewed_stdout_sha256,
            reviewed_query_audit_sha256: &request.reviewed_query_audit_sha256,
            credential_auth_sha256: &identity.credential_auth_sha256,
            execution_profile_id: identity.execution_profile_id,
            credential_pool_id: identity.credential_pool_id,
            credential_revision: identity.credential_revision,
            prompt: &prompt,
            quarantine_at_ms: identity.quarantined_at_ms,
        },
    )?;
    let reason = "dreamina_copyright_rejected";
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
    let resolution_id = Uuid::new_v4();
    sqlx::query(r#"INSERT INTO dreamina_accepted_terminal_resolutions
        (resolution_id,job_id,submission_id,executor_execution_id,hold_id,reason,reviewed_stdout_sha256,
         execution_binding_sha256,provider_deadline_at_ms,released_units,released_micros,operator_reference,created_at_ms,
         provider_account_id,remote_operation_id,quarantine_id,command_hash,provider_command_sha256,request_hash,quote_hash,
         query_audit_sha256,process_request_sha256,process_terminal_sha256,prompt_sha256,queried_at_ms,review_sha256)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)"#)
        .bind(resolution_id).bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id)
        .bind(locked.hold_id).bind(reason).bind(&evidence.stdout_sha256).bind(&identity.execution_binding_sha256)
        .bind(identity.completion_deadline_at_ms).bind(locked.requested_units).bind(locked.held_micros)
        .bind(&request.operator_reference).bind(now).bind(identity.provider_account_id).bind(&identity.remote_operation_id)
        .bind(identity.quarantine_id).bind(&identity.command_hash).bind(&identity.provider_command_sha256)
        .bind(&identity.request_hash).bind(&identity.quote_hash).bind(&evidence.audit_sha256)
        .bind(&evidence.process_request_sha256).bind(&evidence.process_terminal_sha256).bind(&evidence.prompt_sha256)
        .bind(evidence.queried_at_ms).bind(&review_sha256).execute(&mut *tx).await.map_err(storage_conflict)?;
    crate::executor::release_capacity_allocation(
        &mut tx,
        request.executor_execution_id,
        request.submission_id,
        "uncertain",
        "operator_accepted_terminal",
        None,
        now,
    )
    .await
    .map_err(|_| conflict())?;
    super::operator_failure_release::release(
        &mut tx,
        request.job_id,
        &locked,
        reason,
        now,
        super::operator_failure_release::Authority::AcceptedTerminal,
    )
    .await?;
    let payload = json!({"reason": reason, "error_code": reason,
        "resolution_id": resolution_id, "submission_id": request.submission_id,
        "executor_execution_id": request.executor_execution_id, "hold_id": locked.hold_id,
        "currency": locked.currency, "captured_micros": 0,
        "released_micros": locked.held_micros, "released_units": locked.requested_units,
        "reviewed_stdout_sha256": evidence.stdout_sha256});
    for table in ["job_events", "outbox_events"] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {table} (event_id,job_id,event_type,semantic_key,payload_json,created_at_ms) VALUES ($1,$2,'job.failed','job.operator_dreamina_accepted_terminal',$3,$4)"
        ))).bind(Uuid::new_v4()).bind(request.job_id).bind(&payload).bind(now)
            .execute(&mut *tx).await.map_err(storage_conflict)?;
    }
    sqlx::query("SELECT assert_dreamina_accepted_terminal($1)")
        .bind(request.job_id)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
    let result = load_result(&mut tx, request.job_id).await?;
    tx.commit().await.map_err(storage_conflict)?;
    Ok(result)
}

async fn load_result(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job_id: Uuid,
) -> Result<DreaminaAcceptedTerminalResolution, ImageGatewayError> {
    sqlx::query_as(
        r#"SELECT resolution.resolution_id, resolution.job_id, job.request_id,
                  resolution.submission_id, resolution.executor_execution_id,
                  resolution.hold_id, hold.currency, resolution.released_units,
                  resolution.released_micros, hold.captured_micros
           FROM dreamina_accepted_terminal_resolutions resolution
           JOIN jobs job USING(job_id) JOIN customer_billing_holds hold USING(hold_id)
           WHERE resolution.job_id = $1"#,
    )
    .bind(job_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)
}

pub(super) fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Dreamina accepted terminal resolution conflicts with immutable evidence or settlement",
        None,
        "dreamina_accepted_terminal_resolution_conflict",
    )
}
fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(sqlstate = ?error.as_database_error().and_then(|e| e.code()), "Dreamina accepted terminal recovery storage unavailable");
    ImageGatewayError::service_unavailable(
        "Dreamina accepted terminal recovery storage unavailable",
    )
}
fn storage_conflict(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(sqlstate = ?error.as_database_error().and_then(|e| e.code()), "Dreamina accepted terminal recovery rejected");
    conflict()
}

#[derive(sqlx::FromRow)]
struct Original {
    provider_account_id: Uuid,
    command_hash: String,
    execution_profile_id: Uuid,
    credential_pool_id: Uuid,
    credential_revision: i64,
    credential_auth_sha256: String,
    execution_binding_sha256: String,
    provider_command_sha256: String,
    remote_operation_id: String,
    quarantine_id: Uuid,
    quarantined_at_ms: i64,
    completion_deadline_at_ms: i64,
    command_json: serde_json::Value,
    request_hash: String,
    quote_hash: String,
}
