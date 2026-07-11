use async_trait::async_trait;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    ExecutorClaimScope, ExecutorResultManifest, ExecutorSubmissionError, ExecutorSubmissionLease,
    ExecutorSubmissionOutcome, ExecutorSubmissionStore, PreparedExecutorSubmission,
};
use crate::admission::WorkLease;

const MAX_RECONCILE_BATCH: u32 = 1_000;
const EXECUTOR_LEASE_EXPIRED: &str = "executor_lease_expired";
const EXECUTOR_START_ABANDONED: &str = "executor_start_abandoned";

mod validation;

use validation::{
    command_hash, command_output_count, distinct_execution_id, validate_claim_scope,
    validate_executor_lease, validate_lease_duration, validate_outcome,
    validate_owner_and_duration, validate_work_lease,
};

#[derive(Clone)]
pub struct PostgresExecutorSubmissionStore {
    pool: PgPool,
}

impl PostgresExecutorSubmissionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct DurableCommandRow {
    requested_units: i32,
    tenant_id: String,
    provider_id: String,
    model: String,
    command_schema: String,
    command_json: Value,
}

#[derive(sqlx::FromRow)]
struct ExistingIdentityRow {
    output_id: Uuid,
    output_index: i32,
    submission_id: Option<Uuid>,
    executor_execution_id: Option<Uuid>,
    tenant_id: Option<String>,
    provider_id: Option<String>,
    model: Option<String>,
    work_item_id: Option<Uuid>,
    command_schema: Option<String>,
    command_hash: Option<String>,
    executor_row_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct ClaimableRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    output_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    provider_id: String,
    model: String,
    work_item_id: Uuid,
    output_index: i32,
    command_schema: String,
    command_hash: String,
    lease_epoch: i64,
}

#[derive(sqlx::FromRow)]
struct TerminalOutcomeRow {
    execution_state: String,
    submission_state: String,
    execution_error_code: Option<String>,
    submission_error_code: Option<String>,
    manifest_id: Option<Uuid>,
    storage_backend: Option<String>,
    object_key: Option<String>,
    sha256_hex: Option<String>,
    byte_size: Option<i64>,
    media_type: Option<String>,
}

#[derive(sqlx::FromRow)]
struct ExpiredExecutionRow {
    executor_execution_id: Uuid,
    submission_id: Uuid,
    executor_state: String,
}

#[derive(sqlx::FromRow)]
struct LockedExecutorRow {
    state: String,
    executor_owner: Option<String>,
    lease_epoch: i64,
    lease_expires_at_ms: Option<i64>,
}

#[async_trait]
impl ExecutorSubmissionStore for PostgresExecutorSubmissionStore {
    async fn prepare_for_lease(
        &self,
        lease: &WorkLease,
    ) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError> {
        validate_work_lease(lease)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let command = lock_durable_command(&mut tx, lease).await?;
        if command.command_schema != lease.command_schema
            || command.command_json != lease.command_json
        {
            return Err(ExecutorSubmissionError::Conflict);
        }
        let output_count = command_output_count(command.requested_units, &command.command_json)?;
        let command_hash = command_hash(&command.command_json)?;

        let existing = load_existing(&mut tx, lease.job_id).await?;
        if !existing.is_empty() {
            let prepared =
                rebuild_existing(existing, lease, output_count, &command_hash, &command)?;
            attach_attempts(&mut tx, lease, &prepared).await?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(prepared);
        }

        let now = database_now(&mut tx).await?;
        let mut prepared = Vec::with_capacity(output_count as usize);
        for output_index in 0..output_count {
            let output_id = Uuid::new_v4();
            let submission_id = Uuid::new_v4();
            let executor_execution_id = distinct_execution_id(lease.execution_id);
            sqlx::query(
                r#"
                INSERT INTO job_outputs
                  (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, 'pending', $4, $4)
                "#,
            )
            .bind(output_id)
            .bind(lease.job_id)
            .bind(output_index)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            sqlx::query(
                r#"
                INSERT INTO provider_submissions
                  (submission_id, executor_execution_id, output_id, job_id,
                   tenant_id, provider_id, model, work_item_id,
                   created_by_execution_id, created_by_lease_epoch, command_schema, command_hash,
                   state, prepared_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        'prepared', $13, $13)
                "#,
            )
            .bind(submission_id)
            .bind(executor_execution_id)
            .bind(output_id)
            .bind(lease.job_id)
            .bind(&command.tenant_id)
            .bind(&command.provider_id)
            .bind(&command.model)
            .bind(lease.work_item_id)
            .bind(lease.execution_id)
            .bind(lease.lease_epoch)
            .bind(&lease.command_schema)
            .bind(&command_hash)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            sqlx::query(
                r#"
                INSERT INTO executor_executions
                  (executor_execution_id, submission_id, state, created_at_ms, updated_at_ms)
                VALUES ($1, $2, 'prepared', $3, $3)
                "#,
            )
            .bind(executor_execution_id)
            .bind(submission_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            let item = PreparedExecutorSubmission {
                submission_id,
                executor_execution_id,
                output_id,
                job_id: lease.job_id,
                tenant_id: command.tenant_id.clone(),
                provider_id: command.provider_id.clone(),
                model: command.model.clone(),
                work_item_id: lease.work_item_id,
                output_index,
                command_schema: lease.command_schema.clone(),
                command_hash: command_hash.clone(),
            };
            insert_attachment(&mut tx, lease, submission_id, now).await?;
            prepared.push(item);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(prepared)
    }

    async fn claim_prepared(
        &self,
        scope: &ExecutorClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ExecutorSubmissionLease>, ExecutorSubmissionError> {
        validate_owner_and_duration(owner, lease_ms)?;
        validate_claim_scope(scope)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let row: Option<ClaimableRow> = sqlx::query_as(
            r#"
            SELECT s.submission_id, e.executor_execution_id, s.output_id, s.job_id,
                   s.tenant_id, s.provider_id, s.model,
                   s.work_item_id, o.output_index, s.command_schema, s.command_hash,
                   e.lease_epoch
            FROM executor_executions e
            JOIN provider_submissions s
              ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = s.job_id
            JOIN work_items w ON w.work_item_id = s.work_item_id AND w.job_id = s.job_id
            JOIN job_attempts a
              ON a.work_item_id = w.work_item_id
             AND a.execution_id = w.execution_id
             AND a.lease_epoch = w.lease_epoch
            JOIN provider_submission_attachments pa
              ON pa.submission_id = s.submission_id
             AND pa.job_id = s.job_id
             AND pa.work_item_id = w.work_item_id
             AND pa.attempt_execution_id = a.execution_id
             AND pa.lease_epoch = a.lease_epoch
            JOIN jobs j ON j.job_id = s.job_id
            WHERE (e.state = 'prepared'
               OR (e.state = 'leased' AND e.lease_expires_at_ms <= $1))
              AND s.state = 'prepared'
              AND w.state = 'running' AND w.lease_expires_at_ms > $1
              AND a.state = 'running'
              AND j.state IN ('reserved', 'queued', 'running')
              AND s.provider_id = $2 AND s.command_schema = $3
            ORDER BY e.created_at_ms, s.job_id, o.output_index
            FOR UPDATE OF e, s SKIP LOCKED
            LIMIT 1
            "#,
        )
        .bind(now)
        .bind(&scope.provider_id)
        .bind(&scope.command_schema)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(row) = row else {
            tx.commit().await.map_err(unavailable)?;
            return Ok(None);
        };
        let executor_lease_epoch = row
            .lease_epoch
            .checked_add(1)
            .ok_or(ExecutorSubmissionError::Unavailable)?;
        let executor_lease_expires_at_ms = now + lease_ms;
        let changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'leased', executor_owner = $2, lease_epoch = $3,
                lease_expires_at_ms = $4, leased_at_ms = $5, updated_at_ms = $5
            WHERE executor_execution_id = $1 AND submission_id = $6
              AND (state = 'prepared'
                OR (state = 'leased' AND lease_expires_at_ms <= $5))
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(owner)
        .bind(executor_lease_epoch)
        .bind(executor_lease_expires_at_ms)
        .bind(now)
        .bind(row.submission_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(ExecutorSubmissionError::Unavailable);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Some(ExecutorSubmissionLease {
            submission_id: row.submission_id,
            executor_execution_id: row.executor_execution_id,
            output_id: row.output_id,
            job_id: row.job_id,
            tenant_id: row.tenant_id,
            provider_id: row.provider_id,
            model: row.model,
            work_item_id: row.work_item_id,
            output_index: row.output_index,
            command_schema: row.command_schema,
            command_hash: row.command_hash,
            executor_owner: owner.to_string(),
            executor_lease_epoch,
            executor_lease_expires_at_ms,
        }))
    }

    async fn start(&self, lease: &ExecutorSubmissionLease) -> Result<(), ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        lock_current_running_work(&mut tx, lease).await?;
        let locked = lock_executor_execution(&mut tx, lease).await?;
        let now = database_now(&mut tx).await?;
        if locked.state != "leased"
            || locked.executor_owner.as_deref() != Some(lease.executor_owner.as_str())
            || locked.lease_epoch != lease.executor_lease_epoch
            || locked
                .lease_expires_at_ms
                .is_none_or(|expires| expires <= now)
        {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = 'running', started_at_ms = $5, updated_at_ms = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND executor_owner = $3 AND lease_epoch = $4 AND state = 'leased'
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let submission_changed = sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = 'running', started_at_ms = $6, updated_at_ms = $6
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND output_id = $3 AND job_id = $4 AND work_item_id = $5
              AND state = 'prepared'
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(lease.output_id)
        .bind(lease.job_id)
        .bind(lease.work_item_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if submission_changed != 1 {
            return Err(ExecutorSubmissionError::Unavailable);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        lease: &ExecutorSubmissionLease,
        lease_ms: i64,
    ) -> Result<ExecutorSubmissionLease, ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        validate_lease_duration(lease_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let locked = lock_executor_execution(&mut tx, lease).await?;
        let now = database_now(&mut tx).await?;
        if locked.state != "running"
            || locked.executor_owner.as_deref() != Some(lease.executor_owner.as_str())
            || locked.lease_epoch != lease.executor_lease_epoch
            || locked
                .lease_expires_at_ms
                .is_none_or(|expires| expires <= now)
        {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let executor_lease_expires_at_ms = locked
            .lease_expires_at_ms
            .unwrap_or_default()
            .max(now + lease_ms);
        let changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET lease_expires_at_ms = $5, updated_at_ms = $6
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND executor_owner = $3 AND lease_epoch = $4
              AND state = 'running'
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(executor_lease_expires_at_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(ExecutorSubmissionLease {
            executor_lease_expires_at_ms,
            ..lease.clone()
        })
    }

    async fn record_outcome(
        &self,
        lease: &ExecutorSubmissionLease,
        outcome: &ExecutorSubmissionOutcome,
    ) -> Result<(), ExecutorSubmissionError> {
        validate_executor_lease(lease)?;
        validate_outcome(outcome)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let state = outcome.state();
        let error_code = outcome.error_code();
        let locked = lock_executor_execution(&mut tx, lease).await?;
        if matches!(locked.state.as_str(), "succeeded" | "failed" | "uncertain") {
            return match terminal_outcome_matches(&mut tx, lease, outcome).await? {
                Some(true) => {
                    tx.commit().await.map_err(unavailable)?;
                    Ok(())
                }
                Some(false) => Err(ExecutorSubmissionError::Conflict),
                None => Err(ExecutorSubmissionError::StaleLease),
            };
        }
        let now = database_now(&mut tx).await?;
        if locked.state != "running"
            || locked.executor_owner.as_deref() != Some(lease.executor_owner.as_str())
            || locked.lease_epoch != lease.executor_lease_epoch
            || locked
                .lease_expires_at_ms
                .is_none_or(|expires| expires <= now)
        {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        let execution_changed = sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $5, executor_owner = NULL, lease_expires_at_ms = NULL,
                finished_at_ms = $6, updated_at_ms = $6, error_code = $7
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND executor_owner = $3 AND lease_epoch = $4
              AND state = 'running'
            "#,
        )
        .bind(lease.executor_execution_id)
        .bind(lease.submission_id)
        .bind(&lease.executor_owner)
        .bind(lease.executor_lease_epoch)
        .bind(state)
        .bind(now)
        .bind(error_code)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if execution_changed != 1 {
            return Err(ExecutorSubmissionError::StaleLease);
        }
        if let Some(manifest) = outcome.manifest() {
            insert_result_manifest(&mut tx, lease, manifest, now).await?;
        }
        let submission_changed = sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $6, result_manifest_id = $7, finished_at_ms = $8,
                updated_at_ms = $8, error_code = $9
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND output_id = $3 AND job_id = $4 AND work_item_id = $5
              AND state = 'running'
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.executor_execution_id)
        .bind(lease.output_id)
        .bind(lease.job_id)
        .bind(lease.work_item_id)
        .bind(state)
        .bind(outcome.manifest().map(|manifest| manifest.manifest_id))
        .bind(now)
        .bind(error_code)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if submission_changed != 1 {
            return Err(ExecutorSubmissionError::Unavailable);
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    async fn reconcile_expired(&self, limit: u32) -> Result<u64, ExecutorSubmissionError> {
        if limit == 0 || limit > MAX_RECONCILE_BATCH {
            return Err(ExecutorSubmissionError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let rows: Vec<ExpiredExecutionRow> = sqlx::query_as(
            r#"
            SELECT e.executor_execution_id, e.submission_id, e.state AS executor_state
            FROM executor_executions e
            JOIN provider_submissions s
             ON s.executor_execution_id = e.executor_execution_id
             AND s.submission_id = e.submission_id
            JOIN work_items w ON w.work_item_id = s.work_item_id AND w.job_id = s.job_id
            WHERE e.lease_expires_at_ms <= $1
              AND (
                (e.state = 'running' AND s.state = 'running')
                OR
                (e.state = 'leased' AND s.state = 'prepared'
                  AND w.state IN ('succeeded', 'failed', 'uncertain'))
              )
            ORDER BY e.lease_expires_at_ms, e.executor_execution_id
            FOR UPDATE OF e, s SKIP LOCKED
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        for row in &rows {
            update_expired_execution(&mut tx, row, now).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(rows.len() as u64)
    }
}

async fn lock_durable_command(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
) -> Result<DurableCommandRow, ExecutorSubmissionError> {
    let job: Option<(i32, String, String, String)> = sqlx::query_as(
        r#"
        SELECT requested_units, tenant_id, provider_id, model
        FROM jobs
        WHERE job_id = $1 AND state IN ('reserved', 'queued', 'running')
        FOR UPDATE
        "#,
    )
    .bind(lease.job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some((requested_units, tenant_id, provider_id, model)) = job else {
        return Err(ExecutorSubmissionError::StaleLease);
    };
    let payload: Option<(String, Value)> = sqlx::query_as(
        r#"
        SELECT p.command_schema, p.command_json
        FROM work_items w
        JOIN job_attempts a
          ON a.work_item_id = w.work_item_id
         AND a.execution_id = w.execution_id
         AND a.lease_epoch = w.lease_epoch
        JOIN job_payloads p ON p.job_id = w.job_id
        WHERE w.work_item_id = $1 AND w.job_id = $2
          AND w.execution_id = $3 AND w.lease_epoch = $4 AND w.lease_owner = $5
          AND w.state = 'leased' AND w.lease_expires_at_ms >
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND a.worker_id = $5 AND a.state = 'claimed'
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.execution_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some((command_schema, command_json)) = payload else {
        return Err(ExecutorSubmissionError::StaleLease);
    };
    Ok(DurableCommandRow {
        requested_units,
        tenant_id,
        provider_id,
        model,
        command_schema,
        command_json,
    })
}

async fn load_existing(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Vec<ExistingIdentityRow>, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT o.output_id, o.output_index, s.submission_id, s.executor_execution_id,
               s.tenant_id, s.provider_id, s.model,
               s.work_item_id, s.command_schema, s.command_hash,
               e.executor_execution_id AS executor_row_id
        FROM job_outputs o
        LEFT JOIN provider_submissions s
          ON s.output_id = o.output_id AND s.job_id = o.job_id
        LEFT JOIN executor_executions e
          ON e.executor_execution_id = s.executor_execution_id
         AND e.submission_id = s.submission_id
        WHERE o.job_id = $1
        ORDER BY o.output_index
        "#,
    )
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)
}

fn rebuild_existing(
    rows: Vec<ExistingIdentityRow>,
    lease: &WorkLease,
    output_count: i32,
    command_hash: &str,
    command: &DurableCommandRow,
) -> Result<Vec<PreparedExecutorSubmission>, ExecutorSubmissionError> {
    if rows.len() != output_count as usize {
        return Err(ExecutorSubmissionError::Conflict);
    }
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            let submission_id = row.submission_id.ok_or(ExecutorSubmissionError::Conflict)?;
            let executor_execution_id = row
                .executor_execution_id
                .ok_or(ExecutorSubmissionError::Conflict)?;
            if row.output_index != index as i32
                || row.work_item_id != Some(lease.work_item_id)
                || row.tenant_id.as_deref() != Some(command.tenant_id.as_str())
                || row.provider_id.as_deref() != Some(command.provider_id.as_str())
                || row.model.as_deref() != Some(command.model.as_str())
                || row.command_schema.as_deref() != Some(lease.command_schema.as_str())
                || row.command_hash.as_deref() != Some(command_hash)
                || row.executor_row_id != Some(executor_execution_id)
            {
                return Err(ExecutorSubmissionError::Conflict);
            }
            Ok(PreparedExecutorSubmission {
                submission_id,
                executor_execution_id,
                output_id: row.output_id,
                job_id: lease.job_id,
                tenant_id: row.tenant_id.ok_or(ExecutorSubmissionError::Conflict)?,
                provider_id: row.provider_id.ok_or(ExecutorSubmissionError::Conflict)?,
                model: row.model.ok_or(ExecutorSubmissionError::Conflict)?,
                work_item_id: lease.work_item_id,
                output_index: row.output_index,
                command_schema: lease.command_schema.clone(),
                command_hash: command_hash.to_string(),
            })
        })
        .collect()
}

async fn attach_attempts(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    submissions: &[PreparedExecutorSubmission],
) -> Result<(), ExecutorSubmissionError> {
    let now = database_now(tx).await?;
    for submission in submissions {
        insert_attachment(tx, lease, submission.submission_id, now).await?;
    }
    Ok(())
}

async fn insert_attachment(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    submission_id: Uuid,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    sqlx::query(
        r#"
        INSERT INTO provider_submission_attachments
          (submission_id, job_id, attempt_execution_id, work_item_id, lease_epoch, attached_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (submission_id, attempt_execution_id) DO NOTHING
        "#,
    )
    .bind(submission_id)
    .bind(lease.job_id)
    .bind(lease.execution_id)
    .bind(lease.work_item_id)
    .bind(lease.lease_epoch)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn lock_current_running_work(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<(), ExecutorSubmissionError> {
    let active: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE
        FROM work_items w
        JOIN job_attempts a
          ON a.work_item_id = w.work_item_id
         AND a.execution_id = w.execution_id
         AND a.lease_epoch = w.lease_epoch
        JOIN jobs j ON j.job_id = w.job_id
        JOIN provider_submission_attachments pa
          ON pa.submission_id = $3
         AND pa.job_id = w.job_id
         AND pa.work_item_id = w.work_item_id
         AND pa.attempt_execution_id = a.execution_id
         AND pa.lease_epoch = a.lease_epoch
        WHERE w.work_item_id = $1 AND w.job_id = $2
          AND w.state = 'running' AND w.lease_expires_at_ms >
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND a.state = 'running'
          AND j.state IN ('reserved', 'queued', 'running')
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    active.ok_or(ExecutorSubmissionError::StaleLease).map(drop)
}

async fn lock_executor_execution(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
) -> Result<LockedExecutorRow, ExecutorSubmissionError> {
    sqlx::query_as(
        r#"
        SELECT state, executor_owner, lease_epoch, lease_expires_at_ms
        FROM executor_executions
        WHERE executor_execution_id = $1 AND submission_id = $2
        FOR UPDATE
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorSubmissionError::StaleLease)
}

async fn insert_result_manifest(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
    manifest: &ExecutorResultManifest,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    sqlx::query(
        r#"
        INSERT INTO executor_result_manifests
          (manifest_id, executor_execution_id, submission_id, storage_backend,
           object_key, sha256_hex, byte_size, media_type, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(manifest.manifest_id)
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(&manifest.storage_backend)
    .bind(&manifest.object_key)
    .bind(&manifest.sha256_hex)
    .bind(i64::try_from(manifest.byte_size).map_err(|_| ExecutorSubmissionError::InvalidInput)?)
    .bind(&manifest.media_type)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn terminal_outcome_matches(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorSubmissionLease,
    outcome: &ExecutorSubmissionOutcome,
) -> Result<Option<bool>, ExecutorSubmissionError> {
    let row: Option<TerminalOutcomeRow> = sqlx::query_as(
        r#"
        SELECT e.state AS execution_state, s.state AS submission_state,
               e.error_code AS execution_error_code,
               s.error_code AS submission_error_code,
               m.manifest_id, m.storage_backend, m.object_key, m.sha256_hex,
               m.byte_size, m.media_type
        FROM executor_executions e
        JOIN provider_submissions s
          ON s.executor_execution_id = e.executor_execution_id
         AND s.submission_id = e.submission_id
        LEFT JOIN executor_result_manifests m
          ON m.manifest_id = s.result_manifest_id
         AND m.executor_execution_id = e.executor_execution_id
         AND m.submission_id = s.submission_id
        WHERE e.executor_execution_id = $1 AND e.submission_id = $2
          AND s.output_id = $3 AND s.job_id = $4 AND s.work_item_id = $5
        FOR UPDATE OF e, s
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(lease.submission_id)
    .bind(lease.output_id)
    .bind(lease.job_id)
    .bind(lease.work_item_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if !matches!(
        row.execution_state.as_str(),
        "succeeded" | "failed" | "uncertain"
    ) || !matches!(
        row.submission_state.as_str(),
        "succeeded" | "failed" | "uncertain"
    ) {
        return Ok(None);
    }
    let base_matches = row.execution_state == outcome.state()
        && row.submission_state == outcome.state()
        && row.execution_error_code.as_deref() == outcome.error_code()
        && row.submission_error_code.as_deref() == outcome.error_code();
    let manifest_matches = match outcome.manifest() {
        Some(manifest) => {
            row.manifest_id == Some(manifest.manifest_id)
                && row.storage_backend.as_deref() == Some(manifest.storage_backend.as_str())
                && row.object_key.as_deref() == Some(manifest.object_key.as_str())
                && row.sha256_hex.as_deref() == Some(manifest.sha256_hex.as_str())
                && row.byte_size.and_then(|value| u64::try_from(value).ok())
                    == Some(manifest.byte_size)
                && row.media_type.as_deref() == Some(manifest.media_type.as_str())
        }
        None => row.manifest_id.is_none(),
    };
    Ok(Some(base_matches && manifest_matches))
}

async fn update_expired_execution(
    tx: &mut Transaction<'_, Postgres>,
    row: &ExpiredExecutionRow,
    now: i64,
) -> Result<(), ExecutorSubmissionError> {
    let (executor_from, submission_from, target, error_code) = if row.executor_state == "running" {
        ("running", "running", "uncertain", EXECUTOR_LEASE_EXPIRED)
    } else {
        ("leased", "prepared", "canceled", EXECUTOR_START_ABANDONED)
    };
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $3, executor_owner = NULL, lease_expires_at_ms = NULL,
                finished_at_ms = $4, updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = $6
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(row.submission_id)
        .bind(target)
        .bind(now)
        .bind(error_code)
        .bind(executor_from)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $3, finished_at_ms = $4,
                updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = $6
            "#,
        )
        .bind(row.executor_execution_id)
        .bind(row.submission_id)
        .bind(target)
        .bind(now)
        .bind(error_code)
        .bind(submission_from)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ExecutorSubmissionError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn require_one(result: sqlx::postgres::PgQueryResult) -> Result<(), ExecutorSubmissionError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ExecutorSubmissionError::Unavailable)
    }
}

fn unavailable(_: sqlx::Error) -> ExecutorSubmissionError {
    ExecutorSubmissionError::Unavailable
}
