use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    admission::{GENERATION_COMMAND_SCHEMA, GenerationCommandV1},
    artifacts::{ArtifactMetadata, GENERATION_RESPONSE_SCHEMA, customer_object_key},
    economics::{
        EconomicReceipt, EconomicReceiptOutcome, EconomicSettlementError,
        settle_receipt_in_transaction,
    },
    usage::quota_lock_id,
};

use super::super::{
    CanonicalExecutorOutcome, ExecutorParentTerminalState, ExecutorTerminalArtifact,
    ExecutorTerminalCompletion, ExecutorTerminalError, ExecutorTerminalLease,
};

#[derive(sqlx::FromRow)]
struct LockedCompletionRow {
    reduction_state: String,
    lease_owner: Option<String>,
    lease_epoch: i64,
    lease_expires_at_ms: Option<i64>,
    completion_owner: Option<String>,
    provider_receipt_id: Option<Uuid>,
    customer_artifact_id: Option<Uuid>,
    quota_reservation_id: Option<Uuid>,
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
    reservation_id: Uuid,
}

#[derive(sqlx::FromRow)]
struct QuotaSliceRow {
    reservation_id: Uuid,
    requested_units: i32,
    committed_units: i32,
    released_units: i32,
    state: String,
    request_id: String,
    operation: String,
    limit_5h: i32,
    remaining_5h: i32,
    limit_7d: i32,
    remaining_7d: i32,
}

#[derive(sqlx::FromRow)]
struct OutputAggregate {
    output_count: i64,
    succeeded_count: i64,
    failed_count: i64,
    uncertain_count: i64,
    active_count: i64,
    first_error_code: Option<String>,
}

#[derive(sqlx::FromRow)]
struct StoredArtifactRow {
    artifact_id: Uuid,
    tenant_id: String,
    job_id: Uuid,
    work_item_id: Uuid,
    execution_id: Uuid,
    lease_epoch: i64,
    output_index: i32,
    storage_backend: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
    media_type: String,
}

pub(super) async fn complete(
    pool: &PgPool,
    lease: &ExecutorTerminalLease,
    customer_artifact: Option<&ArtifactMetadata>,
) -> Result<ExecutorTerminalCompletion, ExecutorTerminalError> {
    validate_completion_input(lease, customer_artifact)?;
    let mut tx = pool.begin().await.map_err(unavailable)?;
    lock_quota(&mut tx, &lease.tenant_id).await?;
    let row = lock_completion(&mut tx, lease.submission_id).await?;
    let canonical_outcome = canonical_outcome(&row)?;
    validate_canonical_lease(lease, &row, &canonical_outcome)?;

    if row.reduction_state == "completed" {
        let completion = replay_completion(&mut tx, lease, customer_artifact, &row).await?;
        tx.commit().await.map_err(unavailable)?;
        return Ok(completion);
    }
    let now = database_now(&mut tx).await?;
    validate_live_completion_lease(lease, &row, now)?;

    if let Some(artifact) = customer_artifact {
        persist_customer_artifact(&mut tx, lease, &canonical_outcome, artifact, now).await?;
    }
    let receipt = canonical_receipt(lease, &canonical_outcome)?;
    let settlement = settle_receipt_in_transaction(&mut tx, &receipt)
        .await
        .map_err(map_economic_error)?;
    let quota =
        apply_quota_slice(&mut tx, lease, &canonical_outcome, row.reservation_id, now).await?;
    mark_completed(
        &mut tx,
        lease,
        settlement.receipt_id,
        customer_artifact.map(|artifact| artifact.identity.artifact_id),
        quota.reservation_id,
        now,
    )
    .await?;
    let parent_state = aggregate_parent(&mut tx, lease, &quota, now).await?;
    tx.commit().await.map_err(unavailable)?;
    Ok(ExecutorTerminalCompletion {
        receipt_id: settlement.receipt_id,
        customer_artifact_id: customer_artifact.map(|artifact| artifact.identity.artifact_id),
        parent_state,
    })
}

async fn lock_quota(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
) -> Result<(), ExecutorTerminalError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(tenant_id))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

async fn lock_completion(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<LockedCompletionRow, ExecutorTerminalError> {
    sqlx::query_as(
        r#"
        SELECT reduction.state AS reduction_state, reduction.lease_owner,
               reduction.lease_epoch, reduction.lease_expires_at_ms,
               reduction.completion_owner,
               reduction.provider_receipt_id, reduction.customer_artifact_id,
               reduction.quota_reservation_id,
               reduction.submission_id, reduction.executor_execution_id,
               reduction.resolution_decision_id,
               submission.output_id, output.output_index, submission.job_id,
               submission.tenant_id, submission.work_item_id,
               submission.created_by_execution_id AS attempt_execution_id,
               submission.created_by_lease_epoch AS attempt_lease_epoch,
               decision.resolved_state, decision.error_code,
               manifest.manifest_id, authority.authority_id,
               authority.storage_backend, authority.storage_namespace,
               authority.object_key, authority.sha256_hex,
               authority.byte_size, authority.media_type,
               job.reservation_id
        FROM executor_terminal_reductions reduction
        JOIN provider_submissions submission
          ON submission.submission_id = reduction.submission_id
         AND submission.executor_execution_id = reduction.executor_execution_id
         AND submission.resolution_decision_id = reduction.resolution_decision_id
         AND submission.state = reduction.resolved_state
        JOIN executor_executions execution
          ON execution.executor_execution_id = reduction.executor_execution_id
         AND execution.submission_id = reduction.submission_id
         AND execution.resolution_decision_id = reduction.resolution_decision_id
         AND execution.state = reduction.resolved_state
        JOIN executor_resolution_decisions decision
          ON decision.decision_id = reduction.resolution_decision_id
         AND decision.executor_execution_id = reduction.executor_execution_id
         AND decision.submission_id = reduction.submission_id
         AND decision.resolved_state = reduction.resolved_state
        JOIN job_outputs output
          ON output.output_id = submission.output_id AND output.job_id = submission.job_id
        JOIN jobs job
          ON job.job_id = submission.job_id AND job.tenant_id = submission.tenant_id
         AND job.economics_contract_version = 2
        LEFT JOIN executor_result_manifests manifest
          ON manifest.manifest_id = decision.result_manifest_id
         AND manifest.executor_execution_id = reduction.executor_execution_id
         AND manifest.submission_id = reduction.submission_id
        LEFT JOIN executor_artifact_authorities authority
          ON authority.authority_id = manifest.artifact_authority_id
         AND authority.executor_execution_id = reduction.executor_execution_id
         AND authority.submission_id = reduction.submission_id
        WHERE reduction.submission_id = $1
        FOR UPDATE OF reduction
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorTerminalError::Conflict)
}

fn canonical_outcome(
    row: &LockedCompletionRow,
) -> Result<CanonicalExecutorOutcome, ExecutorTerminalError> {
    match row.resolved_state.as_str() {
        "succeeded" => {
            if row.error_code.is_some()
                || row.manifest_id != Some(row.submission_id)
                || row.authority_id != Some(row.executor_execution_id)
            {
                return Err(ExecutorTerminalError::Conflict);
            }
            Ok(CanonicalExecutorOutcome::Succeeded(
                ExecutorTerminalArtifact {
                    authority_id: row.authority_id.ok_or(ExecutorTerminalError::Conflict)?,
                    storage_backend: row
                        .storage_backend
                        .clone()
                        .ok_or(ExecutorTerminalError::Conflict)?,
                    storage_namespace: row
                        .storage_namespace
                        .clone()
                        .ok_or(ExecutorTerminalError::Conflict)?,
                    object_key: row
                        .object_key
                        .clone()
                        .ok_or(ExecutorTerminalError::Conflict)?,
                    sha256_hex: row
                        .sha256_hex
                        .clone()
                        .ok_or(ExecutorTerminalError::Conflict)?,
                    byte_size: u64::try_from(row.byte_size.ok_or(ExecutorTerminalError::Conflict)?)
                        .map_err(|_| ExecutorTerminalError::Conflict)?,
                    media_type: row
                        .media_type
                        .clone()
                        .ok_or(ExecutorTerminalError::Conflict)?,
                },
            ))
        }
        state @ ("failed" | "uncertain" | "canceled") => {
            if row.manifest_id.is_some() || row.authority_id.is_some() {
                return Err(ExecutorTerminalError::Conflict);
            }
            let error_code = row
                .error_code
                .clone()
                .filter(|value| !value.is_empty())
                .ok_or(ExecutorTerminalError::Conflict)?;
            Ok(match state {
                "failed" => CanonicalExecutorOutcome::Failed { error_code },
                "uncertain" => CanonicalExecutorOutcome::Uncertain { error_code },
                "canceled" => CanonicalExecutorOutcome::Canceled { error_code },
                _ => unreachable!(),
            })
        }
        _ => Err(ExecutorTerminalError::Conflict),
    }
}

fn validate_canonical_lease(
    lease: &ExecutorTerminalLease,
    row: &LockedCompletionRow,
    outcome: &CanonicalExecutorOutcome,
) -> Result<(), ExecutorTerminalError> {
    if lease.submission_id != row.submission_id
        || lease.executor_execution_id != row.executor_execution_id
        || lease.resolution_decision_id != row.resolution_decision_id
        || lease.output_id != row.output_id
        || lease.output_index != row.output_index
        || lease.job_id != row.job_id
        || lease.tenant_id != row.tenant_id
        || lease.work_item_id != row.work_item_id
        || lease.attempt_execution_id != row.attempt_execution_id
        || lease.attempt_lease_epoch != row.attempt_lease_epoch
        || &lease.outcome != outcome
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    if lease.reducer_lease_epoch != row.lease_epoch {
        Err(ExecutorTerminalError::StaleLease)
    } else {
        Ok(())
    }
}

fn validate_live_completion_lease(
    lease: &ExecutorTerminalLease,
    row: &LockedCompletionRow,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    if row.reduction_state != "leased"
        || row.lease_owner.as_deref() != Some(&lease.reducer_owner)
        || row.lease_epoch != lease.reducer_lease_epoch
        || row.lease_expires_at_ms.is_none_or(|expires| expires <= now)
        || row.provider_receipt_id.is_some()
        || row.customer_artifact_id.is_some()
        || row.quota_reservation_id.is_some()
    {
        Err(ExecutorTerminalError::StaleLease)
    } else {
        Ok(())
    }
}

fn validate_completion_input(
    lease: &ExecutorTerminalLease,
    artifact: Option<&ArtifactMetadata>,
) -> Result<(), ExecutorTerminalError> {
    if lease.submission_id.is_nil()
        || lease.executor_execution_id.is_nil()
        || lease.resolution_decision_id.is_nil()
        || lease.output_id.is_nil()
        || lease.job_id.is_nil()
        || lease.work_item_id.is_nil()
        || lease.attempt_execution_id.is_nil()
        || lease.tenant_id.is_empty()
        || lease.reducer_owner.is_empty()
        || lease.output_index < 0
        || lease.attempt_lease_epoch <= 0
        || lease.reducer_lease_epoch <= 0
        || matches!(lease.outcome, CanonicalExecutorOutcome::Succeeded(_)) != artifact.is_some()
    {
        Err(ExecutorTerminalError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_artifact(
    lease: &ExecutorTerminalLease,
    outcome: &CanonicalExecutorOutcome,
    artifact: &ArtifactMetadata,
) -> Result<(), ExecutorTerminalError> {
    let CanonicalExecutorOutcome::Succeeded(authority) = outcome else {
        return Err(ExecutorTerminalError::InvalidInput);
    };
    if artifact.identity.artifact_id != lease.output_id
        || artifact.identity.tenant_id != lease.tenant_id
        || artifact.identity.job_id != lease.job_id
        || artifact.identity.work_item_id != lease.work_item_id
        || artifact.identity.execution_id != lease.attempt_execution_id
        || artifact.identity.lease_epoch != lease.attempt_lease_epoch
        || i32::try_from(artifact.identity.output_index).ok() != Some(lease.output_index)
        || artifact.identity.media_type != authority.media_type
        || artifact.storage_backend != authority.storage_backend
        || artifact.object_key != customer_object_key(lease.output_id)
        || artifact.sha256_hex != authority.sha256_hex
        || artifact.byte_size != authority.byte_size
    {
        Err(ExecutorTerminalError::Conflict)
    } else {
        Ok(())
    }
}

async fn persist_customer_artifact(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    outcome: &CanonicalExecutorOutcome,
    artifact: &ArtifactMetadata,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    validate_artifact(lease, outcome, artifact)?;
    sqlx::query(
        r#"
        INSERT INTO artifacts
          (artifact_id, tenant_id, job_id, work_item_id, execution_id, lease_epoch,
           output_index, state, storage_backend, object_key, sha256_hex, byte_size,
           media_type, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, 'ready', $8, $9, $10, $11, $12, $13)
        ON CONFLICT (artifact_id) DO NOTHING
        "#,
    )
    .bind(artifact.identity.artifact_id)
    .bind(&artifact.identity.tenant_id)
    .bind(artifact.identity.job_id)
    .bind(artifact.identity.work_item_id)
    .bind(artifact.identity.execution_id)
    .bind(artifact.identity.lease_epoch)
    .bind(lease.output_index)
    .bind(&artifact.storage_backend)
    .bind(&artifact.object_key)
    .bind(&artifact.sha256_hex)
    .bind(i64::try_from(artifact.byte_size).map_err(|_| ExecutorTerminalError::InvalidInput)?)
    .bind(&artifact.identity.media_type)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    validate_stored_artifact(tx, artifact).await
}

async fn validate_stored_artifact(
    tx: &mut Transaction<'_, Postgres>,
    artifact: &ArtifactMetadata,
) -> Result<(), ExecutorTerminalError> {
    let stored: StoredArtifactRow = sqlx::query_as(
        r#"
        SELECT artifact_id, tenant_id, job_id, work_item_id, execution_id,
               lease_epoch, output_index, storage_backend, object_key,
               sha256_hex, byte_size, media_type
        FROM artifacts WHERE artifact_id = $1 FOR UPDATE
        "#,
    )
    .bind(artifact.identity.artifact_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorTerminalError::Conflict)?;
    if stored.artifact_id != artifact.identity.artifact_id
        || stored.tenant_id != artifact.identity.tenant_id
        || stored.job_id != artifact.identity.job_id
        || stored.work_item_id != artifact.identity.work_item_id
        || stored.execution_id != artifact.identity.execution_id
        || stored.lease_epoch != artifact.identity.lease_epoch
        || u32::try_from(stored.output_index).ok() != Some(artifact.identity.output_index)
        || stored.storage_backend != artifact.storage_backend
        || stored.object_key != artifact.object_key
        || stored.sha256_hex != artifact.sha256_hex
        || u64::try_from(stored.byte_size).ok() != Some(artifact.byte_size)
        || stored.media_type != artifact.identity.media_type
    {
        Err(ExecutorTerminalError::Conflict)
    } else {
        Ok(())
    }
}

fn canonical_receipt(
    lease: &ExecutorTerminalLease,
    outcome: &CanonicalExecutorOutcome,
) -> Result<EconomicReceipt, ExecutorTerminalError> {
    let (economic_outcome, error_code, artifact) = match outcome {
        CanonicalExecutorOutcome::Succeeded(authority) => (
            EconomicReceiptOutcome::Succeeded,
            None,
            Some(json!({
                "authority_id": authority.authority_id.to_string(),
                "byte_size": authority.byte_size,
                "media_type": authority.media_type,
                "sha256_hex": authority.sha256_hex,
            })),
        ),
        CanonicalExecutorOutcome::Failed { error_code } if error_code == "provider_no_effect" => {
            (EconomicReceiptOutcome::NoEffect, Some(error_code), None)
        }
        CanonicalExecutorOutcome::Failed { error_code } => {
            (EconomicReceiptOutcome::Failed, Some(error_code), None)
        }
        CanonicalExecutorOutcome::Uncertain { error_code } => {
            (EconomicReceiptOutcome::Uncertain, Some(error_code), None)
        }
        CanonicalExecutorOutcome::Canceled { error_code } => {
            (EconomicReceiptOutcome::NoEffect, Some(error_code), None)
        }
    };
    let mut evidence = json!({
        "executor_execution_id": lease.executor_execution_id.to_string(),
        "resolution_decision_id": lease.resolution_decision_id.to_string(),
        "resolved_state": resolved_state(outcome),
        "submission_id": lease.submission_id.to_string(),
    });
    let object = evidence
        .as_object_mut()
        .ok_or(ExecutorTerminalError::Conflict)?;
    if let Some(error_code) = error_code {
        object.insert("error_code".to_string(), Value::String(error_code.clone()));
    }
    if let Some(artifact) = artifact {
        object.insert("artifact".to_string(), artifact);
    }
    EconomicReceipt::new(
        lease.submission_id,
        economic_outcome,
        "executor.resolution.v1",
        evidence,
    )
    .map_err(map_economic_error)
}

fn resolved_state(outcome: &CanonicalExecutorOutcome) -> &'static str {
    match outcome {
        CanonicalExecutorOutcome::Succeeded(_) => "succeeded",
        CanonicalExecutorOutcome::Failed { .. } => "failed",
        CanonicalExecutorOutcome::Uncertain { .. } => "uncertain",
        CanonicalExecutorOutcome::Canceled { .. } => "canceled",
    }
}

async fn apply_quota_slice(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    outcome: &CanonicalExecutorOutcome,
    reservation_id: Uuid,
    now: i64,
) -> Result<QuotaSliceRow, ExecutorTerminalError> {
    let (committed_delta, released_delta, metering_outcome) = match outcome {
        CanonicalExecutorOutcome::Succeeded(_) => (1, 0, "succeeded"),
        CanonicalExecutorOutcome::Failed { error_code } => (0, 1, error_code.as_str()),
        CanonicalExecutorOutcome::Canceled { error_code } => (0, 1, error_code.as_str()),
        CanonicalExecutorOutcome::Uncertain { error_code } => (0, 0, error_code.as_str()),
    };
    let quota: QuotaSliceRow = sqlx::query_as(
        r#"
        UPDATE quota_reservations quota
        SET committed_units = quota.committed_units + $5,
            released_units = quota.released_units + $6,
            state = CASE
              WHEN quota.committed_units + quota.released_units + $5 + $6
                   = quota.requested_units
                   AND quota.committed_units + $5 > 0 THEN 'committed'
              WHEN quota.committed_units + quota.released_units + $5 + $6
                   = quota.requested_units
                   THEN 'released'
              ELSE 'reserved'
            END,
            updated_at_ms = $7
        FROM jobs job
        WHERE quota.reservation_id = $1 AND quota.job_id = $2
          AND quota.tenant_id = $3 AND job.job_id = quota.job_id
          AND job.reservation_id = quota.reservation_id
          AND job.tenant_id = quota.tenant_id
          AND job.economics_contract_version = 2
          AND quota.state IN ('reserved', 'expired')
          AND quota.committed_units + quota.released_units + $5 + $6 <= quota.requested_units
          AND job.request_id = quota.request_id
          AND job.operation = $4
        RETURNING quota.reservation_id, quota.requested_units,
                  quota.committed_units, quota.released_units, quota.state,
                  quota.request_id, job.operation,
                  quota.limit_5h, quota.remaining_5h,
                  quota.limit_7d, quota.remaining_7d
        "#,
    )
    .bind(reservation_id)
    .bind(lease.job_id)
    .bind(&lease.tenant_id)
    .bind("generation")
    .bind(committed_delta)
    .bind(released_delta)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorTerminalError::Conflict)?;

    if committed_delta == 1 {
        sqlx::query(
            r#"
            INSERT INTO usage_events
              (event_id, tenant_id, request_id, operation, units, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, 1, 'charged', $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&lease.tenant_id)
        .bind(&quota.request_id)
        .bind(&quota.operation)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }
    sqlx::query(
        r#"
        INSERT INTO metering_events
          (event_id, tenant_id, job_id, reservation_id, request_id, operation,
           event_type, units, outcome, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, 'executor_output_terminal', $7, $8, $9)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(&lease.tenant_id)
    .bind(lease.job_id)
    .bind(quota.reservation_id)
    .bind(&quota.request_id)
    .bind(&quota.operation)
    .bind(committed_delta + released_delta)
    .bind(metering_outcome)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(quota)
}

async fn mark_completed(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    receipt_id: Uuid,
    artifact_id: Option<Uuid>,
    reservation_id: Uuid,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let changed = sqlx::query(
        r#"
        UPDATE executor_terminal_reductions
        SET state = 'completed', lease_owner = NULL, lease_expires_at_ms = NULL,
            completed_at_ms = $5, updated_at_ms = $5,
            completion_owner = $6, provider_receipt_id = $7,
            customer_artifact_id = $8, quota_reservation_id = $9
        WHERE submission_id = $1 AND resolution_decision_id = $2
          AND lease_owner = $3 AND lease_epoch = $4 AND state = 'leased'
          AND lease_expires_at_ms > $5
        "#,
    )
    .bind(lease.submission_id)
    .bind(lease.resolution_decision_id)
    .bind(&lease.reducer_owner)
    .bind(lease.reducer_lease_epoch)
    .bind(now)
    .bind(&lease.reducer_owner)
    .bind(receipt_id)
    .bind(artifact_id)
    .bind(reservation_id)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if changed == 1 {
        Ok(())
    } else {
        Err(ExecutorTerminalError::StaleLease)
    }
}

async fn aggregate_parent(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    now: i64,
) -> Result<ExecutorParentTerminalState, ExecutorTerminalError> {
    let aggregate: OutputAggregate = sqlx::query_as(
        r#"
        SELECT COUNT(*)::BIGINT AS output_count,
               COUNT(*) FILTER (WHERE state = 'succeeded')::BIGINT AS succeeded_count,
               COUNT(*) FILTER (WHERE state = 'failed')::BIGINT AS failed_count,
               COUNT(*) FILTER (WHERE state = 'uncertain')::BIGINT AS uncertain_count,
               COUNT(*) FILTER (WHERE state IN ('pending', 'running'))::BIGINT AS active_count,
               (array_agg(error_code ORDER BY output_index)
                    FILTER (WHERE error_code IS NOT NULL))[1] AS first_error_code
        FROM job_outputs WHERE job_id = $1
        "#,
    )
    .bind(lease.job_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if aggregate.output_count != i64::from(quota.requested_units)
        || i64::from(quota.committed_units) != aggregate.succeeded_count
        || i64::from(quota.released_units) != aggregate.failed_count
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    if aggregate.active_count > 0 {
        if quota.state != "reserved" {
            return Err(ExecutorTerminalError::Conflict);
        }
        return Ok(ExecutorParentTerminalState::Pending);
    }
    let parent_state = if aggregate.uncertain_count > 0 {
        ExecutorParentTerminalState::Uncertain
    } else if aggregate.failed_count > 0 {
        ExecutorParentTerminalState::Failed
    } else if aggregate.succeeded_count == aggregate.output_count {
        ExecutorParentTerminalState::Succeeded
    } else {
        return Err(ExecutorTerminalError::Conflict);
    };
    let expected_quota_state = match parent_state {
        ExecutorParentTerminalState::Uncertain => "reserved",
        ExecutorParentTerminalState::Succeeded | ExecutorParentTerminalState::Failed
            if quota.committed_units > 0 =>
        {
            "committed"
        }
        ExecutorParentTerminalState::Failed => "released",
        ExecutorParentTerminalState::Pending => return Err(ExecutorTerminalError::Conflict),
        ExecutorParentTerminalState::Succeeded => return Err(ExecutorTerminalError::Conflict),
    };
    if quota.state != expected_quota_state {
        return Err(ExecutorTerminalError::Conflict);
    }
    terminalize_parent(tx, lease, quota, &aggregate, parent_state, now).await?;
    Ok(parent_state)
}

async fn terminalize_parent(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    aggregate: &OutputAggregate,
    parent_state: ExecutorParentTerminalState,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let (state, event_type, error_code) = match parent_state {
        ExecutorParentTerminalState::Succeeded => ("succeeded", "job.succeeded", None),
        ExecutorParentTerminalState::Failed => {
            let error = if aggregate.succeeded_count > 0 {
                "partial_output_failure".to_string()
            } else {
                aggregate
                    .first_error_code
                    .clone()
                    .unwrap_or_else(|| "provider_failed".to_string())
            };
            ("failed", "job.failed", Some(error))
        }
        ExecutorParentTerminalState::Uncertain => (
            "uncertain",
            "job.uncertain",
            Some(
                aggregate
                    .first_error_code
                    .clone()
                    .unwrap_or_else(|| "provider_outcome_uncertain".to_string()),
            ),
        ),
        ExecutorParentTerminalState::Pending => return Err(ExecutorTerminalError::Conflict),
    };
    if parent_state == ExecutorParentTerminalState::Succeeded {
        persist_projection(tx, lease, quota, now).await?;
    }
    require_one(
        sqlx::query(
            r#"
            UPDATE work_items SET state = $5, updated_at_ms = $6
            WHERE work_item_id = $1 AND job_id = $2 AND execution_id = $3
              AND lease_epoch = $4 AND state = 'awaiting_executor'
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.job_id)
        .bind(lease.attempt_execution_id)
        .bind(lease.attempt_lease_epoch)
        .bind(state)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE job_attempts
            SET state = $4, finished_at_ms = $5, error_code = $6, updated_at_ms = $5
            WHERE execution_id = $1 AND work_item_id = $2 AND lease_epoch = $3
              AND state = 'handed_off'
            "#,
        )
        .bind(lease.attempt_execution_id)
        .bind(lease.work_item_id)
        .bind(lease.attempt_lease_epoch)
        .bind(state)
        .bind(now)
        .bind(error_code.as_deref())
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE jobs
            SET state = $4, charged_units = $5, finished_at_ms = $6,
                updated_at_ms = $6, last_error_code = $7
            WHERE job_id = $1 AND tenant_id = $2 AND reservation_id = $3
              AND state IN ('reserved', 'queued', 'running', 'artifact_ready')
            "#,
        )
        .bind(lease.job_id)
        .bind(&lease.tenant_id)
        .bind(quota.reservation_id)
        .bind(state)
        .bind(quota.committed_units)
        .bind(now)
        .bind(error_code.as_deref())
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )?;
    sqlx::query(
        r#"
        UPDATE idempotency_requests
        SET state = $2, terminal_outcome = $2, updated_at_ms = $3
        WHERE job_id = $1 AND state = 'accepted'
        "#,
    )
    .bind(lease.job_id)
    .bind(state)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    append_parent_events(
        tx,
        lease,
        aggregate,
        event_type,
        state,
        error_code.as_deref(),
        now,
    )
    .await
}

async fn persist_projection(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let row: (String, Value) =
        sqlx::query_as("SELECT command_schema, command_json FROM job_payloads WHERE job_id = $1")
            .bind(lease.job_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unavailable)?
            .ok_or(ExecutorTerminalError::Conflict)?;
    if row.0 != GENERATION_COMMAND_SCHEMA {
        return Err(ExecutorTerminalError::Conflict);
    }
    let command: GenerationCommandV1 =
        serde_json::from_value(row.1).map_err(|_| ExecutorTerminalError::Conflict)?;
    if command.n != u32::try_from(quota.requested_units).unwrap_or_default()
        || command.provider_id.is_empty()
        || command.model.is_empty()
        || command.source_api_profile.is_empty()
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    sqlx::query(
        r#"
        INSERT INTO job_response_projections
          (job_id, api_profile, operation, response_schema, created_at_seconds,
           output_format, quality, size, background, stream,
           limit_5h, remaining_5h, limit_7d, remaining_7d,
           artifact_count, created_at_ms)
        VALUES ($1, $2, 'generation', $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15)
        "#,
    )
    .bind(lease.job_id)
    .bind(&command.source_api_profile)
    .bind(GENERATION_RESPONSE_SCHEMA)
    .bind(now / 1_000)
    .bind(&command.output_format)
    .bind(&command.quality)
    .bind(&command.size)
    .bind(&command.background)
    .bind(command.stream)
    .bind(quota.limit_5h)
    .bind(quota.remaining_5h)
    .bind(quota.limit_7d)
    .bind(quota.remaining_7d)
    .bind(quota.requested_units)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn append_parent_events(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    aggregate: &OutputAggregate,
    event_type: &str,
    state: &str,
    error_code: Option<&str>,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let semantic_key = format!("work.{}.executor-terminal", lease.work_item_id);
    let payload = json!({
        "error_code": error_code,
        "failed_outputs": aggregate.failed_count,
        "succeeded_outputs": aggregate.succeeded_count,
        "terminal_state": state,
        "uncertain_outputs": aggregate.uncertain_count,
    });
    for sql in [
        r#"
        INSERT INTO job_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
        r#"
        INSERT INTO outbox_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    ] {
        sqlx::query(sql)
            .bind(Uuid::new_v4())
            .bind(lease.job_id)
            .bind(event_type)
            .bind(&semantic_key)
            .bind(&payload)
            .bind(now)
            .execute(&mut **tx)
            .await
            .map_err(unavailable)?;
    }
    Ok(())
}

async fn replay_completion(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    artifact: Option<&ArtifactMetadata>,
    row: &LockedCompletionRow,
) -> Result<ExecutorTerminalCompletion, ExecutorTerminalError> {
    let receipt_id = row
        .provider_receipt_id
        .ok_or(ExecutorTerminalError::Conflict)?;
    if row.completion_owner.as_deref() != Some(&lease.reducer_owner) {
        return Err(ExecutorTerminalError::StaleLease);
    }
    if row.quota_reservation_id != Some(row.reservation_id)
        || row.customer_artifact_id != artifact.map(|value| value.identity.artifact_id)
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    if let Some(artifact) = artifact {
        validate_artifact(lease, &lease.outcome, artifact)?;
        validate_stored_artifact(tx, artifact).await?;
    }
    let work_state: String =
        sqlx::query_scalar("SELECT state FROM work_items WHERE work_item_id = $1 AND job_id = $2")
            .bind(lease.work_item_id)
            .bind(lease.job_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unavailable)?
            .ok_or(ExecutorTerminalError::Conflict)?;
    let parent_state = match work_state.as_str() {
        "awaiting_executor" => ExecutorParentTerminalState::Pending,
        "succeeded" => ExecutorParentTerminalState::Succeeded,
        "failed" => ExecutorParentTerminalState::Failed,
        "uncertain" => ExecutorParentTerminalState::Uncertain,
        _ => return Err(ExecutorTerminalError::Conflict),
    };
    Ok(ExecutorTerminalCompletion {
        receipt_id,
        customer_artifact_id: row.customer_artifact_id,
        parent_state,
    })
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ExecutorTerminalError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn require_one(result: sqlx::postgres::PgQueryResult) -> Result<(), ExecutorTerminalError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ExecutorTerminalError::Conflict)
    }
}

fn map_economic_error(error: EconomicSettlementError) -> ExecutorTerminalError {
    match error {
        EconomicSettlementError::Unavailable => ExecutorTerminalError::Unavailable,
        EconomicSettlementError::InvalidInput => ExecutorTerminalError::InvalidInput,
        EconomicSettlementError::Conflict | EconomicSettlementError::NotReady => {
            ExecutorTerminalError::Conflict
        }
    }
}

fn unavailable(_: sqlx::Error) -> ExecutorTerminalError {
    ExecutorTerminalError::Unavailable
}
