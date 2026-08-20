use async_trait::async_trait;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    BlockedTerminalRequeue, BlockedTerminalRequeueError, CanonicalExecutorOutcome,
    ExecutorTerminalArtifact, ExecutorTerminalBlockReason, ExecutorTerminalCompletion,
    ExecutorTerminalError, ExecutorTerminalLease, ExecutorTerminalStore,
};
use crate::artifacts::ArtifactMetadata;

mod completion;

const MAX_LEASE_MS: i64 = 10 * 60 * 1_000;

#[derive(Clone)]
pub struct PostgresExecutorTerminalStore {
    pool: PgPool,
}

impl PostgresExecutorTerminalStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn requeue_blocked_canonical_conflict(
        &self,
        submission_id: Uuid,
        repair_revision: &str,
        requeued_by: &str,
    ) -> Result<BlockedTerminalRequeue, BlockedTerminalRequeueError> {
        validate_requeue_input(submission_id, repair_revision, requeued_by)?;
        let mut tx = self.pool.begin().await.map_err(requeue_unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("terminal-requeue:{submission_id}"))
            .execute(&mut *tx)
            .await
            .map_err(requeue_unavailable)?;
        let existing: Option<(Uuid, String)> = sqlx::query_as(
            r#"
            SELECT executor_execution_id, repair_revision
            FROM operator_terminal_reduction_requeues
            WHERE submission_id = $1
            "#,
        )
        .bind(submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(requeue_unavailable)?;
        if let Some((executor_execution_id, stored_revision)) = existing {
            if stored_revision != repair_revision {
                return Err(BlockedTerminalRequeueError::Conflict);
            }
            let state: Option<String> = sqlx::query_scalar(
                "SELECT state FROM executor_terminal_reductions WHERE submission_id = $1",
            )
            .bind(submission_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(requeue_unavailable)?;
            if state.as_deref() == Some("blocked") || state.is_none() {
                return Err(BlockedTerminalRequeueError::Conflict);
            }
            tx.commit().await.map_err(requeue_unavailable)?;
            return Ok(BlockedTerminalRequeue {
                submission_id,
                executor_execution_id,
                repair_revision: stored_revision,
                already_requeued: true,
            });
        }

        let inserted: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO operator_terminal_reduction_requeues (
                submission_id, executor_execution_id, prior_lease_epoch,
                prior_claimed_at_ms, prior_blocked_error_code, prior_blocked_by,
                prior_blocked_at_ms, repair_revision, requeued_by, requeued_at_ms
            )
            SELECT reduction.submission_id, reduction.executor_execution_id,
                   reduction.lease_epoch, reduction.claimed_at_ms,
                   reduction.blocked_error_code, reduction.blocked_by,
                   reduction.blocked_at_ms, $2, $3,
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            FROM executor_terminal_reductions reduction
            JOIN provider_submissions submission
              ON submission.submission_id = reduction.submission_id
             AND submission.executor_execution_id = reduction.executor_execution_id
             AND submission.resolution_decision_id = reduction.resolution_decision_id
             AND submission.state = reduction.resolved_state
            JOIN executor_executions execution
              ON execution.executor_execution_id = submission.executor_execution_id
             AND execution.submission_id = submission.submission_id
             AND execution.resolution_decision_id = reduction.resolution_decision_id
             AND execution.state = reduction.resolved_state
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = reduction.resolution_decision_id
             AND decision.executor_execution_id = reduction.executor_execution_id
             AND decision.submission_id = reduction.submission_id
             AND decision.resolved_state = reduction.resolved_state
            JOIN jobs job
              ON job.job_id = submission.job_id
             AND job.state IN ('reserved', 'running')
             AND job.economics_contract_version = 4
            JOIN job_outputs output
              ON output.output_id = submission.output_id
             AND output.job_id = submission.job_id
             AND output.state = 'pending'
            JOIN work_items work
              ON work.work_item_id = submission.work_item_id
             AND work.job_id = submission.job_id
             AND work.execution_id = submission.created_by_execution_id
             AND work.lease_epoch = submission.created_by_lease_epoch
             AND work.state = 'awaiting_executor'
            JOIN job_attempts attempt
              ON attempt.execution_id = submission.created_by_execution_id
             AND attempt.work_item_id = submission.work_item_id
             AND attempt.lease_epoch = submission.created_by_lease_epoch
             AND attempt.state = 'handed_off'
            LEFT JOIN executor_result_manifests manifest
              ON manifest.manifest_id = decision.result_manifest_id
             AND manifest.executor_execution_id = reduction.executor_execution_id
             AND manifest.submission_id = reduction.submission_id
            LEFT JOIN executor_artifact_authorities authority
              ON authority.authority_id = manifest.artifact_authority_id
             AND authority.executor_execution_id = manifest.executor_execution_id
             AND authority.submission_id = manifest.submission_id
            WHERE reduction.submission_id = $1
              AND reduction.state = 'blocked'
              AND reduction.blocked_error_code = 'canonical_conflict'
              AND reduction.resolved_state IN ('succeeded', 'failed')
              AND submission.provider_id = 'openai-codex'
              AND submission.adapter_revision = 'openai-codex-generation-v1'
              AND reduction.completion_owner IS NULL
              AND reduction.provider_receipt_id IS NULL
              AND reduction.customer_artifact_id IS NULL
              AND reduction.quota_reservation_id IS NULL
              AND (
                (
                  reduction.resolved_state = 'succeeded'
                  AND decision.error_code IS NULL
                  AND submission.result_manifest_id = manifest.manifest_id
                  AND manifest.manifest_id IS NOT NULL
                  AND authority.authority_id IS NOT NULL
                )
                OR
                (
                  reduction.resolved_state = 'failed'
                  AND decision.error_code IS NOT NULL
                  AND submission.result_manifest_id IS NULL
                  AND manifest.manifest_id IS NULL
                  AND authority.authority_id IS NULL
                )
              )
              AND NOT EXISTS (
                SELECT 1 FROM provider_receipts receipt
                WHERE receipt.submission_id = reduction.submission_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM artifacts artifact
                WHERE artifact.job_id = submission.job_id
                  AND artifact.output_index = output.output_index
              )
              AND NOT EXISTS (
                SELECT 1 FROM provider_usage_facts fact
                WHERE fact.submission_id = reduction.submission_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM provider_remote_tasks remote
                WHERE remote.submission_id = reduction.submission_id
              )
            RETURNING executor_execution_id
            "#,
        )
        .bind(submission_id)
        .bind(repair_revision)
        .bind(requeued_by)
        .fetch_optional(&mut *tx)
        .await
        .map_err(requeue_unavailable)?;
        let Some(executor_execution_id) = inserted else {
            return Err(BlockedTerminalRequeueError::Conflict);
        };
        let changed = sqlx::query(
            r#"
            UPDATE executor_terminal_reductions reduction
            SET state = 'ready',
                lease_owner = NULL,
                lease_expires_at_ms = NULL,
                claimed_at_ms = NULL,
                blocked_error_code = NULL,
                blocked_by = NULL,
                blocked_at_ms = NULL,
                updated_at_ms = requeue.requeued_at_ms
            FROM operator_terminal_reduction_requeues requeue
            WHERE reduction.submission_id = requeue.submission_id
              AND reduction.executor_execution_id = requeue.executor_execution_id
              AND reduction.submission_id = $1
              AND reduction.state = 'blocked'
              AND reduction.lease_epoch = requeue.prior_lease_epoch
              AND reduction.claimed_at_ms = requeue.prior_claimed_at_ms
              AND reduction.blocked_error_code = requeue.prior_blocked_error_code
              AND reduction.blocked_by = requeue.prior_blocked_by
              AND reduction.blocked_at_ms = requeue.prior_blocked_at_ms
            "#,
        )
        .bind(submission_id)
        .execute(&mut *tx)
        .await
        .map_err(requeue_unavailable)?
        .rows_affected();
        if changed != 1 {
            return Err(BlockedTerminalRequeueError::Conflict);
        }
        tx.commit().await.map_err(requeue_unavailable)?;
        Ok(BlockedTerminalRequeue {
            submission_id,
            executor_execution_id,
            repair_revision: repair_revision.to_string(),
            already_requeued: false,
        })
    }
}

#[derive(sqlx::FromRow)]
struct ClaimedTerminalRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    resolution_decision_id: Uuid,
    output_id: Uuid,
    output_index: i32,
    job_id: Uuid,
    tenant_id: String,
    work_item_id: Uuid,
    attempt_execution_id: Uuid,
    attempt_lease_epoch: i64,
    reducer_owner: String,
    reducer_lease_epoch: i64,
    reducer_lease_expires_at_ms: i64,
    resolved_state: String,
    error_code: Option<String>,
    manifest_id: Option<Uuid>,
    authority_id: Option<Uuid>,
    storage_backend: Option<String>,
    storage_namespace: Option<String>,
    object_key: Option<String>,
    sha256_hex: Option<String>,
    byte_size: Option<i64>,
    media_type: Option<String>,
}

#[async_trait]
impl ExecutorTerminalStore for PostgresExecutorTerminalStore {
    async fn claim_terminal(
        &self,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ExecutorTerminalLease>, ExecutorTerminalError> {
        validate_claim(owner, lease_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let row: Option<ClaimedTerminalRow> = sqlx::query_as(
            r#"
            WITH candidate AS (
              SELECT reduction.submission_id
              FROM executor_terminal_reductions reduction
              JOIN provider_submissions submission
                ON submission.submission_id = reduction.submission_id
               AND submission.executor_execution_id = reduction.executor_execution_id
               AND submission.resolution_decision_id = reduction.resolution_decision_id
               AND submission.state = reduction.resolved_state
              JOIN executor_executions execution
                ON execution.executor_execution_id = submission.executor_execution_id
               AND execution.submission_id = submission.submission_id
               AND execution.resolution_decision_id = reduction.resolution_decision_id
               AND execution.state = reduction.resolved_state
              JOIN jobs job
                ON job.job_id = submission.job_id
               AND job.economics_contract_version IN (2, 3, 4)
              JOIN job_outputs output
                ON output.output_id = submission.output_id
               AND output.job_id = submission.job_id
               AND output.state IN ('pending', 'running')
              JOIN work_items work
                ON work.work_item_id = submission.work_item_id
               AND work.job_id = submission.job_id
               AND work.execution_id = submission.created_by_execution_id
               AND work.lease_epoch = submission.created_by_lease_epoch
               AND work.state = 'awaiting_executor'
              JOIN job_attempts attempt
                ON attempt.execution_id = submission.created_by_execution_id
               AND attempt.work_item_id = submission.work_item_id
               AND attempt.lease_epoch = submission.created_by_lease_epoch
               AND attempt.state = 'handed_off'
              JOIN provider_submission_attachments attachment
                ON attachment.submission_id = submission.submission_id
               AND attachment.job_id = submission.job_id
               AND attachment.work_item_id = submission.work_item_id
               AND attachment.attempt_execution_id = submission.created_by_execution_id
               AND attachment.lease_epoch = submission.created_by_lease_epoch
              WHERE reduction.state = 'ready'
                 OR (
                    reduction.state = 'leased'
                    AND reduction.lease_expires_at_ms <=
                        floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
                 )
              ORDER BY reduction.created_at_ms, reduction.submission_id
              FOR UPDATE OF reduction SKIP LOCKED
              LIMIT 1
            ), claimed AS (
              UPDATE executor_terminal_reductions reduction
              SET state = 'leased', lease_owner = $1,
                  lease_epoch = reduction.lease_epoch + 1,
                  lease_expires_at_ms =
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + $2,
                  claimed_at_ms =
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                  updated_at_ms =
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
              FROM candidate
              WHERE reduction.submission_id = candidate.submission_id
              RETURNING reduction.*
            )
            SELECT claimed.submission_id, claimed.executor_execution_id,
                   claimed.resolution_decision_id,
                   submission.output_id, output.output_index, submission.job_id,
                   submission.tenant_id, submission.work_item_id,
                   submission.created_by_execution_id AS attempt_execution_id,
                   submission.created_by_lease_epoch AS attempt_lease_epoch,
                   claimed.lease_owner AS reducer_owner,
                   claimed.lease_epoch AS reducer_lease_epoch,
                   claimed.lease_expires_at_ms AS reducer_lease_expires_at_ms,
                   decision.resolved_state, decision.error_code,
                   manifest.manifest_id, authority.authority_id,
                   authority.storage_backend, authority.storage_namespace,
                   authority.object_key, authority.sha256_hex,
                   authority.byte_size, authority.media_type
            FROM claimed
            JOIN provider_submissions submission
              ON submission.submission_id = claimed.submission_id
             AND submission.executor_execution_id = claimed.executor_execution_id
             AND submission.resolution_decision_id = claimed.resolution_decision_id
             AND submission.state = claimed.resolved_state
            JOIN executor_resolution_decisions decision
              ON decision.decision_id = claimed.resolution_decision_id
             AND decision.executor_execution_id = claimed.executor_execution_id
             AND decision.submission_id = claimed.submission_id
             AND decision.resolved_state = claimed.resolved_state
            JOIN job_outputs output
              ON output.output_id = submission.output_id
             AND output.job_id = submission.job_id
            LEFT JOIN executor_result_manifests manifest
              ON manifest.manifest_id = decision.result_manifest_id
             AND manifest.executor_execution_id = claimed.executor_execution_id
             AND manifest.submission_id = claimed.submission_id
            LEFT JOIN executor_artifact_authorities authority
              ON authority.authority_id = manifest.artifact_authority_id
             AND authority.executor_execution_id = claimed.executor_execution_id
             AND authority.submission_id = claimed.submission_id
            "#,
        )
        .bind(owner)
        .bind(lease_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let lease = row.map(terminal_lease).transpose()?;
        tx.commit().await.map_err(unavailable)?;
        Ok(lease)
    }

    async fn heartbeat_terminal(
        &self,
        lease: &ExecutorTerminalLease,
        lease_ms: i64,
    ) -> Result<ExecutorTerminalLease, ExecutorTerminalError> {
        validate_lease(lease, lease_ms)?;
        let expires_at_ms: Option<i64> = sqlx::query_scalar(
            r#"
            UPDATE executor_terminal_reductions
            SET lease_expires_at_ms = GREATEST(
                  lease_expires_at_ms,
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + $5
                ),
                updated_at_ms =
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            WHERE submission_id = $1 AND resolution_decision_id = $2
              AND lease_owner = $3 AND lease_epoch = $4 AND state = 'leased'
              AND lease_expires_at_ms >
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            RETURNING lease_expires_at_ms
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.resolution_decision_id)
        .bind(&lease.reducer_owner)
        .bind(lease.reducer_lease_epoch)
        .bind(lease_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        let expires_at_ms = expires_at_ms.ok_or(ExecutorTerminalError::StaleLease)?;
        Ok(ExecutorTerminalLease {
            reducer_lease_expires_at_ms: expires_at_ms,
            ..lease.clone()
        })
    }

    async fn complete_terminal(
        &self,
        lease: &ExecutorTerminalLease,
        customer_artifact: Option<&ArtifactMetadata>,
    ) -> Result<ExecutorTerminalCompletion, ExecutorTerminalError> {
        completion::complete(&self.pool, lease, customer_artifact).await
    }

    async fn block_terminal(
        &self,
        lease: &ExecutorTerminalLease,
        reason: ExecutorTerminalBlockReason,
    ) -> Result<(), ExecutorTerminalError> {
        validate_lease(lease, 1)?;
        let blocked: Option<i32> = sqlx::query_scalar(
            r#"
            UPDATE executor_terminal_reductions
            SET state = 'blocked',
                blocked_error_code = $5,
                blocked_by = lease_owner,
                blocked_at_ms =
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                lease_owner = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms =
                    floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            WHERE submission_id = $1
              AND resolution_decision_id = $2
              AND lease_owner = $3
              AND lease_epoch = $4
              AND state = 'leased'
              AND lease_expires_at_ms >
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            RETURNING 1
            "#,
        )
        .bind(lease.submission_id)
        .bind(lease.resolution_decision_id)
        .bind(&lease.reducer_owner)
        .bind(lease.reducer_lease_epoch)
        .bind(reason.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        blocked.ok_or(ExecutorTerminalError::StaleLease)?;
        Ok(())
    }
}

fn terminal_lease(row: ClaimedTerminalRow) -> Result<ExecutorTerminalLease, ExecutorTerminalError> {
    let outcome = match row.resolved_state.as_str() {
        "succeeded" => CanonicalExecutorOutcome::Succeeded(ExecutorTerminalArtifact {
            authority_id: row.authority_id.ok_or(ExecutorTerminalError::Conflict)?,
            storage_backend: row.storage_backend.ok_or(ExecutorTerminalError::Conflict)?,
            storage_namespace: row
                .storage_namespace
                .ok_or(ExecutorTerminalError::Conflict)?,
            object_key: row.object_key.ok_or(ExecutorTerminalError::Conflict)?,
            sha256_hex: row.sha256_hex.ok_or(ExecutorTerminalError::Conflict)?,
            byte_size: u64::try_from(row.byte_size.ok_or(ExecutorTerminalError::Conflict)?)
                .map_err(|_| ExecutorTerminalError::Conflict)?,
            media_type: row.media_type.ok_or(ExecutorTerminalError::Conflict)?,
        }),
        "failed" => CanonicalExecutorOutcome::Failed {
            error_code: terminal_error_code(&row)?,
        },
        "uncertain" => CanonicalExecutorOutcome::Uncertain {
            error_code: terminal_error_code(&row)?,
        },
        "canceled" => CanonicalExecutorOutcome::Canceled {
            error_code: terminal_error_code(&row)?,
        },
        _ => return Err(ExecutorTerminalError::Conflict),
    };
    if matches!(outcome, CanonicalExecutorOutcome::Succeeded(_)) {
        if row.error_code.is_some()
            || row.manifest_id != Some(row.submission_id)
            || row.authority_id != Some(row.executor_execution_id)
        {
            return Err(ExecutorTerminalError::Conflict);
        }
    } else if row.manifest_id.is_some() || row.authority_id.is_some() {
        return Err(ExecutorTerminalError::Conflict);
    }
    Ok(ExecutorTerminalLease {
        submission_id: row.submission_id,
        executor_execution_id: row.executor_execution_id,
        resolution_decision_id: row.resolution_decision_id,
        output_id: row.output_id,
        output_index: row.output_index,
        job_id: row.job_id,
        tenant_id: row.tenant_id,
        work_item_id: row.work_item_id,
        attempt_execution_id: row.attempt_execution_id,
        attempt_lease_epoch: row.attempt_lease_epoch,
        reducer_owner: row.reducer_owner,
        reducer_lease_epoch: row.reducer_lease_epoch,
        reducer_lease_expires_at_ms: row.reducer_lease_expires_at_ms,
        outcome,
    })
}

fn terminal_error_code(row: &ClaimedTerminalRow) -> Result<String, ExecutorTerminalError> {
    row.error_code
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or(ExecutorTerminalError::Conflict)
}

fn validate_claim(owner: &str, lease_ms: i64) -> Result<(), ExecutorTerminalError> {
    if owner.is_empty()
        || owner.len() > 255
        || owner.bytes().any(|byte| byte.is_ascii_control())
        || !(1..=MAX_LEASE_MS).contains(&lease_ms)
    {
        Err(ExecutorTerminalError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_requeue_input(
    submission_id: Uuid,
    repair_revision: &str,
    requeued_by: &str,
) -> Result<(), BlockedTerminalRequeueError> {
    let mut revision = repair_revision.bytes();
    let valid_first = revision
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
    let valid_rest = revision.all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    });
    if submission_id.is_nil()
        || repair_revision.len() > 128
        || !valid_first
        || !valid_rest
        || requeued_by.is_empty()
        || requeued_by.len() > 255
        || requeued_by.bytes().any(|byte| byte.is_ascii_control())
    {
        Err(BlockedTerminalRequeueError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_lease(
    lease: &ExecutorTerminalLease,
    lease_ms: i64,
) -> Result<(), ExecutorTerminalError> {
    validate_claim(&lease.reducer_owner, lease_ms)?;
    if lease.submission_id.is_nil()
        || lease.executor_execution_id.is_nil()
        || lease.resolution_decision_id.is_nil()
        || lease.output_id.is_nil()
        || lease.job_id.is_nil()
        || lease.work_item_id.is_nil()
        || lease.attempt_execution_id.is_nil()
        || lease.output_index < 0
        || lease.attempt_lease_epoch <= 0
        || lease.reducer_lease_epoch <= 0
    {
        Err(ExecutorTerminalError::InvalidInput)
    } else {
        Ok(())
    }
}

fn unavailable(_: sqlx::Error) -> ExecutorTerminalError {
    ExecutorTerminalError::Unavailable
}

fn requeue_unavailable(_: sqlx::Error) -> BlockedTerminalRequeueError {
    BlockedTerminalRequeueError::Unavailable
}
