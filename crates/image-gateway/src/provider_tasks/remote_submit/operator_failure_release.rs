//! Shared zero-capture release for two narrowly audited operator resolutions.
use crate::{ImageGatewayError, credit_grants::settle_credit_grant_reservations};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;
#[derive(sqlx::FromRow)]
pub(super) struct LockedJob {
    pub(super) tenant_id: String,
    pub(super) request_id: String,
    pub(super) operation: String,
    pub(super) work_item_id: Uuid,
    pub(super) reservation_id: Uuid,
    pub(super) requested_units: i32,
    pub(super) billing_metric: String,
    pub(super) billing_unit: String,
    pub(super) hold_id: Uuid,
    pub(super) currency: String,
    pub(super) held_micros: i64,
    pub(super) grant_held_micros: i64,
    pub(super) account_held_micros: i64,
}

pub(super) enum Authority {
    PreAcceptance,
    AcceptedTerminal,
}
pub(super) async fn release(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    locked: &LockedJob,
    reason: &str,
    now: i64,
    authority: Authority,
) -> Result<(), ImageGatewayError> {
    let sql = match authority {
        Authority::PreAcceptance => {
            "SELECT 1 FROM dreamina_operator_rejection_resolutions WHERE job_id=$1 AND hold_id=$2 AND released_micros=$3 AND released_units=$4 AND reason=$5"
        }
        Authority::AcceptedTerminal => {
            "SELECT 1 FROM dreamina_accepted_terminal_resolutions WHERE job_id=$1 AND hold_id=$2 AND released_micros=$3 AND released_units=$4 AND reason=$5"
        }
    };
    sqlx::query(sql)
        .bind(job_id)
        .bind(locked.hold_id)
        .bind(locked.held_micros)
        .bind(locked.requested_units)
        .bind(reason)
        .fetch_optional(&mut **tx)
        .await
        .map_err(storage_conflict)?
        .ok_or_else(conflict)?;
    settle_credit_grant_reservations(
        tx,
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
        .execute(&mut **tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        r#"UPDATE customer_billing_holds
        SET state = 'released', released_micros = held_micros,
            grant_released_micros = grant_held_micros, account_released_micros = account_held_micros,
            updated_at_ms = $2
        WHERE hold_id = $1 AND state = 'held' AND captured_micros = 0 AND released_micros = 0"#,
    ).bind(locked.hold_id).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE quota_reservations SET state = 'released', released_units = requested_units, updated_at_ms = $2 WHERE reservation_id = $1 AND state IN ('reserved','expired') AND committed_units = 0 AND released_units = 0",
    ).bind(locked.reservation_id).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE jobs SET state = 'failed', last_error_code = $2, updated_at_ms = $3 WHERE job_id = $1 AND state = 'uncertain' AND charged_units = 0",
    ).bind(job_id).bind(reason).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE work_items SET state = 'failed', updated_at_ms = $2 WHERE work_item_id = $1 AND state = 'uncertain'",
    ).bind(locked.work_item_id).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE job_attempts SET state = 'failed', error_code = $2, updated_at_ms = $3 WHERE work_item_id = $1 AND state = 'uncertain'",
    ).bind(locked.work_item_id).bind(reason).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?)?;
    require_one(sqlx::query(
        "UPDATE job_outputs SET state = 'failed', error_code = $2, updated_at_ms = $3 WHERE job_id = $1 AND state = 'uncertain'",
    ).bind(job_id).bind(reason).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?)?;
    sqlx::query(
        "UPDATE idempotency_requests SET state = 'failed', terminal_outcome = 'failed', updated_at_ms = $2 WHERE job_id = $1 AND state = 'uncertain' AND terminal_outcome = 'uncertain'",
    ).bind(job_id).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?;
    for event_type in ["quota_released", "job_failed"] {
        sqlx::query(
            r#"INSERT INTO metering_events
              (event_id, tenant_id, job_id, reservation_id, request_id, operation,
               event_type, units, outcome, billing_metric, billing_unit, created_at_ms)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"#,
        )
        .bind(Uuid::new_v4())
        .bind(&locked.tenant_id)
        .bind(job_id)
        .bind(locked.reservation_id)
        .bind(&locked.request_id)
        .bind(&locked.operation)
        .bind(event_type)
        .bind(locked.requested_units)
        .bind(reason)
        .bind(&locked.billing_metric)
        .bind(&locked.billing_unit)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?;
    }
    Ok(())
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
        "Audited release conflicts with original hold",
        None,
        "operator_release_conflict",
    )
}
fn storage_conflict(_: sqlx::Error) -> ImageGatewayError {
    conflict()
}
