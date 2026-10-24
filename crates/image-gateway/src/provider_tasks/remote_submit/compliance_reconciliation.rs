//! Operator reconciliation of a historical, proven pre-acceptance refusal.
//! Never dispatches the provider or changes immutable executor terminal evidence.
use std::path::Path;

use serde::Serialize;
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

use crate::{ImageGatewayError, credit_grants::settle_credit_grant_reservations};

const REASON: &str = "dreamina_compliance_confirmation_required";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaComplianceRejectionResolutionRequest {
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub reviewed_stdout_sha256: String,
    pub operator_reference: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, sqlx::FromRow)]
pub struct DreaminaComplianceRejectionResolution {
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

#[derive(sqlx::FromRow)]
struct LockedJob {
    tenant_id: String,
    request_id: String,
    operation: String,
    work_item_id: Uuid,
    reservation_id: Uuid,
    requested_units: i32,
    billing_metric: String,
    billing_unit: String,
    hold_id: Uuid,
    currency: String,
    held_micros: i64,
    grant_held_micros: i64,
    account_held_micros: i64,
}

/// Requires the exact job/execution/submission and an independently reviewed
/// stdout digest. All projections and local capacity/money releases commit once.
pub async fn resolve_dreamina_compliance_rejection(
    pool: &PgPool,
    journal_root: &Path,
    request: &DreaminaComplianceRejectionResolutionRequest,
) -> Result<DreaminaComplianceRejectionResolution, ImageGatewayError> {
    if request.job_id.is_nil()
        || request.submission_id.is_nil()
        || request.executor_execution_id.is_nil()
        || request.submission_id == request.executor_execution_id
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
            "invalid Dreamina compliance resolution identity",
        ));
    }
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
        "SELECT submission_id, executor_execution_id, reviewed_stdout_sha256, operator_reference FROM dreamina_operator_rejection_resolutions WHERE job_id = $1",
    ).bind(request.job_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if let Some((submission_id, execution_id, sha, operator_reference)) = existing {
        if submission_id != request.submission_id
            || execution_id != request.executor_execution_id
            || sha != request.reviewed_stdout_sha256
            || operator_reference != request.operator_reference
        {
            return Err(conflict());
        }
        sqlx::query("SELECT assert_operator_dreamina_rejection($1)")
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
          AND intent.state = 'deadline_quarantined' AND intent.remote_operation_id IS NULL
          AND intent.provider_request_id IS NULL AND intent.receipt_event_identity IS NULL
          AND recovery.state = 'closed' AND recovery.recovery_owner IS NULL
          AND recovery.recovery_lease_expires_at_ms IS NULL
          AND reduction.state = 'completed' AND reduction.resolved_state = 'uncertain'
          AND allocation.state = 'held'
          AND quota.state IN ('reserved', 'expired') AND quota.committed_units = 0 AND quota.released_units = 0
          AND quota.requested_units = job.billable_units
          AND hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0
          AND hold.grant_captured_micros = 0 AND hold.account_captured_micros = 0
          AND hold.grant_released_micros = 0 AND hold.account_released_micros = 0
        FOR UPDATE OF job, work, attempt, output, submission, execution, intent, recovery, reduction, allocation, quota, hold
        "#,
    ).bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id)
        .fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
    let evidence = super::compliance_evidence::load(
        &mut tx,
        journal_root,
        request.submission_id,
        request.executor_execution_id,
        &request.reviewed_stdout_sha256,
    )
    .await?;
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
    let resolution_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO dreamina_operator_rejection_resolutions
          (resolution_id, job_id, submission_id, executor_execution_id, hold_id, reason,
           reviewed_stdout_sha256, execution_binding_sha256, launch_nonce, provider_deadline_at_ms,
           released_units, released_micros, operator_reference, created_at_ms)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        "#,
    )
    .bind(resolution_id)
    .bind(request.job_id)
    .bind(request.submission_id)
    .bind(request.executor_execution_id)
    .bind(locked.hold_id)
    .bind(REASON)
    .bind(&evidence.stdout_sha256)
    .bind(&evidence.execution_binding_sha256)
    .bind(evidence.launch_nonce)
    .bind(evidence.provider_deadline_at_ms)
    .bind(locked.requested_units)
    .bind(locked.held_micros)
    .bind(&request.operator_reference)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(storage_conflict)?;
    super::super::capacity::release_operator_no_effect_capacity(
        &mut tx,
        request.submission_id,
        request.executor_execution_id,
        resolution_id,
    )
    .await
    .map_err(|_| conflict())?;
    settle_credit_grant_reservations(
        &mut tx,
        locked.hold_id,
        &locked.tenant_id,
        &locked.currency,
        locked.grant_held_micros,
        0,
        now,
    )
    .await?;
    require_one(sqlx::query(
        "UPDATE billing_accounts SET held_micros = held_micros - $3, updated_at_ms = $4 WHERE tenant_id = $1 AND currency = $2 AND held_micros >= $3",
    ).bind(&locked.tenant_id).bind(&locked.currency).bind(locked.account_held_micros).bind(now)
        .execute(&mut *tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        r#"UPDATE customer_billing_holds
        SET state = 'released', released_micros = held_micros,
            grant_released_micros = grant_held_micros, account_released_micros = account_held_micros,
            updated_at_ms = $2
        WHERE hold_id = $1 AND state = 'held' AND captured_micros = 0 AND released_micros = 0"#,
    ).bind(locked.hold_id).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE quota_reservations SET state = 'released', released_units = requested_units, updated_at_ms = $2 WHERE reservation_id = $1 AND state IN ('reserved','expired') AND committed_units = 0 AND released_units = 0",
    ).bind(locked.reservation_id).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE jobs SET state = 'failed', last_error_code = $2, updated_at_ms = $3 WHERE job_id = $1 AND state = 'uncertain' AND charged_units = 0",
    ).bind(request.job_id).bind(REASON).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE work_items SET state = 'failed', updated_at_ms = $2 WHERE work_item_id = $1 AND state = 'uncertain'",
    ).bind(locked.work_item_id).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE job_attempts SET state = 'failed', error_code = $2, updated_at_ms = $3 WHERE work_item_id = $1 AND state = 'uncertain'",
    ).bind(locked.work_item_id).bind(REASON).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE job_outputs SET state = 'failed', error_code = $2, updated_at_ms = $3 WHERE job_id = $1 AND state = 'uncertain'",
    ).bind(request.job_id).bind(REASON).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?)?;
    sqlx::query(
        "UPDATE idempotency_requests SET state = 'failed', terminal_outcome = 'failed', updated_at_ms = $2 WHERE job_id = $1 AND state = 'uncertain' AND terminal_outcome = 'uncertain'",
    ).bind(request.job_id).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
    for event_type in ["quota_released", "job_failed"] {
        sqlx::query(
            r#"INSERT INTO metering_events
              (event_id, tenant_id, job_id, reservation_id, request_id, operation,
               event_type, units, outcome, billing_metric, billing_unit, created_at_ms)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"#,
        )
        .bind(Uuid::new_v4())
        .bind(&locked.tenant_id)
        .bind(request.job_id)
        .bind(locked.reservation_id)
        .bind(&locked.request_id)
        .bind(&locked.operation)
        .bind(event_type)
        .bind(locked.requested_units)
        .bind(REASON)
        .bind(&locked.billing_metric)
        .bind(&locked.billing_unit)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
    }
    let payload = json!({"reason": REASON, "error_code": REASON,
        "resolution_id": resolution_id, "submission_id": request.submission_id,
        "executor_execution_id": request.executor_execution_id, "hold_id": locked.hold_id,
        "currency": locked.currency, "captured_micros": 0,
        "released_micros": locked.held_micros, "released_units": locked.requested_units,
        "reviewed_stdout_sha256": evidence.stdout_sha256});
    for table in ["job_events", "outbox_events"] {
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {table} (event_id,job_id,event_type,semantic_key,payload_json,created_at_ms) VALUES ($1,$2,'job.failed','job.operator_dreamina_compliance_rejection',$3,$4)"
        ))).bind(Uuid::new_v4()).bind(request.job_id).bind(&payload).bind(now)
            .execute(&mut *tx).await.map_err(storage_conflict)?;
    }
    sqlx::query("SELECT assert_operator_dreamina_rejection($1)")
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
) -> Result<DreaminaComplianceRejectionResolution, ImageGatewayError> {
    sqlx::query_as(
        r#"SELECT resolution.resolution_id, resolution.job_id, job.request_id,
                  resolution.submission_id, resolution.executor_execution_id,
                  resolution.hold_id, hold.currency, resolution.released_units,
                  resolution.released_micros, hold.captured_micros
           FROM dreamina_operator_rejection_resolutions resolution
           JOIN jobs job USING(job_id) JOIN customer_billing_holds hold USING(hold_id)
           WHERE resolution.job_id = $1"#,
    )
    .bind(job_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)
}

fn require_one(result: sqlx::postgres::PgQueryResult) -> Result<(), ImageGatewayError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(conflict())
    }
}
fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Dreamina compliance resolution conflicts with immutable evidence or settlement",
        None,
        "dreamina_compliance_resolution_conflict",
    )
}
fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error = %error, "Dreamina compliance reconciliation storage unavailable");
    ImageGatewayError::service_unavailable("Dreamina compliance reconciliation storage unavailable")
}
fn storage_conflict(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error = %error, "Dreamina compliance reconciliation rejected");
    conflict()
}
