use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use image_api_contracts::xai::{XAI_IMAGES_API_PROFILE, XAI_VIDEOS_API_PROFILE};
use image_provider_contracts::{
    ProviderCostEvidenceScope, ProviderCostObservationV1, ProviderReportedCostEvidenceV1,
};
use image_provider_dreamina_cli::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaSubmitRequestV1, parse_submit_command,
};
use image_provider_grok_cli::{
    GROK_IMAGE_EDIT_COMMAND_SCHEMA, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA, parse_image_edit_payload, parse_image_generation_payload,
    parse_video_generation_payload,
};

use crate::{
    admission::{
        EDIT_COMMAND_SCHEMA, EDIT_COMMAND_SCHEMA_VERSION, EDIT_OPERATION, EditCommandV1,
        GENERATION_COMMAND_SCHEMA, GENERATION_OPERATION, GenerationCommandV1,
        VIDEO_GENERATION_OPERATION,
    },
    artifacts::{ArtifactMetadata, GENERATION_RESPONSE_SCHEMA, customer_object_key},
    economics::{
        EconomicReceipt, EconomicReceiptOutcome, EconomicSettlementError,
        record_v4_provider_receipt_in_transaction, settle_receipt_in_transaction,
    },
    pricing::{
        PriceResolutionError, PriceResolutionRequest,
        customer_usage::{
            CustomerUsageAuthority, CustomerUsageFactError, CustomerUsageOutput,
            persist_customer_usage_facts,
        },
        postgres_rating::{CustomerRatingStoreError, settle_customer_quote},
        provider_cost::{ProviderCostStoreError, apply_executor_provider_reported_cost},
        resolve_provider_actual_price_version_in_transaction,
    },
    usage::quota_lock_id,
};

const XAI_VIDEO_RESPONSE_SCHEMA: &str = "xai.videos.response.v1";
const NOT_APPLICABLE: &str = "not_applicable";

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
    provider_id: String,
    provider_account_id: Option<Uuid>,
    operation: String,
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
    media_duration_ms: Option<i64>,
    reservation_id: Uuid,
    economics_contract_version: i16,
    provider_cost_scope: Option<String>,
    provider_cost_provider_id: Option<String>,
    provider_cost_execution_surface: Option<String>,
    provider_cost_operation_id: Option<String>,
    provider_cost_currency: Option<String>,
    provider_cost_native_unit: Option<String>,
    provider_cost_native_quantity: Option<String>,
    provider_cost_authority: Option<String>,
    provider_cost_confidence: Option<String>,
    provider_cost_evidence_hash: Option<String>,
    provider_cost_evidence_path: Option<String>,
    provider_cost_created_at_ms: Option<i64>,
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
    economics_contract_version: i16,
    output_count: i32,
    billable_units: i32,
    billing_metric: String,
    billing_unit: String,
    output_billable_units: i32,
}

#[derive(sqlx::FromRow)]
struct OutputAggregate {
    expected_output_count: i32,
    expected_billable_units: i32,
    output_count: i64,
    billable_units: i64,
    succeeded_count: i64,
    succeeded_billable_units: i64,
    failed_count: i64,
    failed_billable_units: i64,
    uncertain_count: i64,
    active_count: i64,
    partial_success_allowed: bool,
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

#[derive(sqlx::FromRow)]
struct FrozenProviderCostContext {
    project_id: String,
    api_profile: String,
    operation: String,
    provider_id: Option<String>,
    provider_model_id: Option<String>,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
}

#[derive(sqlx::FromRow)]
struct StoredProviderCostReplay {
    provider_id: String,
    execution_surface: String,
    provider_operation_id: String,
    currency: String,
    native_unit: String,
    native_quantity: String,
    authority: String,
    confidence: String,
    evidence_hash: String,
    evidence_path: String,
    amount_micros: i64,
    fact_count: i64,
    ledger_count: i64,
}

fn at_completion_stage<T>(
    lease: &ExecutorTerminalLease,
    stage: &'static str,
    result: Result<T, ExecutorTerminalError>,
) -> Result<T, ExecutorTerminalError> {
    result.inspect_err(|error| {
        tracing::error!(
            stage,
            error = ?error,
            job_id = %lease.job_id,
            submission_id = %lease.submission_id,
            "terminal completion stage failed"
        );
    })
}

pub(super) async fn complete(
    pool: &PgPool,
    lease: &ExecutorTerminalLease,
    customer_artifact: Option<&ArtifactMetadata>,
) -> Result<ExecutorTerminalCompletion, ExecutorTerminalError> {
    at_completion_stage(
        lease,
        "validate_completion_input",
        validate_completion_input(lease, customer_artifact),
    )?;
    let mut tx = pool.begin().await.map_err(unavailable)?;
    at_completion_stage(
        lease,
        "lock_quota",
        lock_quota(&mut tx, &lease.tenant_id).await,
    )?;
    let row = at_completion_stage(
        lease,
        "lock_completion",
        lock_completion(&mut tx, lease.submission_id).await,
    )?;
    let canonical_outcome =
        at_completion_stage(lease, "canonical_outcome", canonical_outcome(&row))?;
    let provider_cost_evidence = at_completion_stage(
        lease,
        "provider_cost_evidence",
        provider_cost_evidence(&row),
    )?;
    at_completion_stage(
        lease,
        "validate_operation_artifact",
        validate_operation_artifact(&row.operation, &canonical_outcome),
    )?;
    at_completion_stage(
        lease,
        "validate_canonical_lease",
        validate_canonical_lease(lease, &row, &canonical_outcome),
    )?;

    if row.reduction_state == "completed" {
        let completion = replay_completion(&mut tx, lease, customer_artifact, &row).await?;
        validate_provider_cost_replay(
            &mut tx,
            completion.receipt_id,
            provider_cost_evidence.as_ref(),
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        return Ok(completion);
    }
    let now = at_completion_stage(lease, "database_now", database_now(&mut tx).await)?;
    at_completion_stage(
        lease,
        "validate_live_completion_lease",
        validate_live_completion_lease(lease, &row, now),
    )?;

    if let Some(artifact) = customer_artifact {
        at_completion_stage(
            lease,
            "persist_customer_artifact",
            persist_customer_artifact(&mut tx, lease, &canonical_outcome, artifact, now).await,
        )?;
    }
    let receipt = at_completion_stage(
        lease,
        "canonical_receipt",
        canonical_receipt(lease, &canonical_outcome, provider_cost_evidence.as_ref()),
    )?;
    let receipt_id = if row.economics_contract_version == 4 {
        let record = at_completion_stage(
            lease,
            "record_v4_provider_receipt",
            record_v4_provider_receipt_in_transaction(&mut tx, &receipt)
                .await
                .map_err(map_economic_error),
        )?;
        at_completion_stage(
            lease,
            "terminalize_v4_output",
            terminalize_v4_output(&mut tx, &row, &record.outcome, &canonical_outcome, now).await,
        )?;
        record.receipt_id
    } else {
        at_completion_stage(
            lease,
            "settle_legacy_provider_receipt",
            settle_receipt_in_transaction(&mut tx, &receipt)
                .await
                .map_err(map_economic_error),
        )?
        .receipt_id
    };
    let quota = at_completion_stage(
        lease,
        "apply_quota_slice",
        apply_quota_slice(&mut tx, lease, &canonical_outcome, row.reservation_id, now).await,
    )?;
    at_completion_stage(
        lease,
        "persist_provider_usage_fact",
        persist_provider_usage_fact(&mut tx, &row, &quota, receipt_id, &canonical_outcome, now)
            .await,
    )?;
    at_completion_stage(
        lease,
        "persist_provider_actual_cost",
        persist_provider_actual_cost(
            &mut tx,
            &row,
            receipt_id,
            &canonical_outcome,
            provider_cost_evidence.as_ref(),
        )
        .await,
    )?;
    at_completion_stage(
        lease,
        "mark_completed",
        mark_completed(
            &mut tx,
            lease,
            receipt_id,
            customer_artifact.map(|artifact| artifact.identity.artifact_id),
            quota.reservation_id,
            now,
        )
        .await,
    )?;
    let parent_state = at_completion_stage(
        lease,
        "aggregate_parent",
        aggregate_parent(&mut tx, lease, &quota, now).await,
    )?;
    if row.economics_contract_version == 4
        && matches!(
            parent_state,
            ExecutorParentTerminalState::Succeeded | ExecutorParentTerminalState::Failed
        )
    {
        at_completion_stage(
            lease,
            "settle_customer_quote",
            settle_customer_quote(&mut tx, lease.job_id, &lease.tenant_id)
                .await
                .map_err(map_customer_rating_error),
        )?;
    }
    at_completion_stage(lease, "commit", tx.commit().await.map_err(unavailable))?;
    Ok(ExecutorTerminalCompletion {
        receipt_id,
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
               submission.tenant_id, submission.provider_id,
               submission.provider_account_id, job.operation,
               submission.work_item_id,
               submission.created_by_execution_id AS attempt_execution_id,
               submission.created_by_lease_epoch AS attempt_lease_epoch,
               decision.resolved_state, decision.error_code,
               manifest.manifest_id, authority.authority_id,
               authority.storage_backend, authority.storage_namespace,
               authority.object_key, authority.sha256_hex,
               authority.byte_size, authority.media_type,
               authority.media_duration_ms,
               job.reservation_id, job.economics_contract_version,
               cost.scope AS provider_cost_scope,
               cost.provider_id AS provider_cost_provider_id,
               cost.execution_surface AS provider_cost_execution_surface,
               cost.provider_operation_id AS provider_cost_operation_id,
               cost.currency AS provider_cost_currency,
               cost.native_unit AS provider_cost_native_unit,
               cost.native_quantity::TEXT AS provider_cost_native_quantity,
               cost.authority AS provider_cost_authority,
               cost.confidence AS provider_cost_confidence,
               cost.evidence_hash AS provider_cost_evidence_hash,
               cost.evidence_path AS provider_cost_evidence_path,
               cost.created_at_ms AS provider_cost_created_at_ms
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
         AND job.economics_contract_version IN (2, 3, 4)
        LEFT JOIN executor_result_manifests manifest
          ON manifest.manifest_id = decision.result_manifest_id
         AND manifest.executor_execution_id = reduction.executor_execution_id
         AND manifest.submission_id = reduction.submission_id
        LEFT JOIN executor_artifact_authorities authority
          ON authority.authority_id = manifest.artifact_authority_id
         AND authority.executor_execution_id = reduction.executor_execution_id
         AND authority.submission_id = reduction.submission_id
        LEFT JOIN executor_provider_cost_evidence cost
          ON cost.manifest_id = manifest.manifest_id
         AND cost.executor_execution_id = reduction.executor_execution_id
         AND cost.submission_id = reduction.submission_id
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

fn validate_operation_artifact(
    operation: &str,
    outcome: &CanonicalExecutorOutcome,
) -> Result<(), ExecutorTerminalError> {
    let CanonicalExecutorOutcome::Succeeded(artifact) = outcome else {
        return Ok(());
    };
    let valid = match operation {
        VIDEO_GENERATION_OPERATION => artifact.media_type == "video/mp4",
        GENERATION_OPERATION | EDIT_OPERATION => {
            matches!(
                artifact.media_type.as_str(),
                "image/png" | "image/jpeg" | "image/webp"
            )
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ExecutorTerminalError::Conflict)
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
    provider_cost_evidence: Option<&ProviderReportedCostEvidenceV1>,
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
    if let Some(provider_cost) = provider_cost_evidence {
        object.insert(
            "provider_reported_cost".to_string(),
            json!({
                "canonical_sha256": hex::encode(provider_cost.canonical_sha256_v1()),
                "evidence_scope": provider_cost.scope().as_str(),
                "provider_operation_id": provider_cost.observation().provider_operation_id.as_str(),
            }),
        );
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

async fn terminalize_v4_output(
    tx: &mut Transaction<'_, Postgres>,
    row: &LockedCompletionRow,
    receipt_outcome: &EconomicReceiptOutcome,
    outcome: &CanonicalExecutorOutcome,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let (output_state, error_code) = match (receipt_outcome, outcome) {
        (EconomicReceiptOutcome::Succeeded, CanonicalExecutorOutcome::Succeeded(_)) => {
            ("succeeded", None)
        }
        (EconomicReceiptOutcome::Failed, CanonicalExecutorOutcome::Failed { error_code })
        | (EconomicReceiptOutcome::NoEffect, CanonicalExecutorOutcome::Failed { error_code })
        | (EconomicReceiptOutcome::NoEffect, CanonicalExecutorOutcome::Canceled { error_code }) => {
            ("failed", Some(error_code.as_str()))
        }
        (EconomicReceiptOutcome::Uncertain, CanonicalExecutorOutcome::Uncertain { error_code }) => {
            ("uncertain", Some(error_code.as_str()))
        }
        _ => return Err(ExecutorTerminalError::Conflict),
    };
    require_one(
        sqlx::query(
            r#"
            UPDATE job_outputs
            SET state = $2, started_at_ms = COALESCE(started_at_ms, $3),
                finished_at_ms = $3, updated_at_ms = $3, error_code = $4
            WHERE output_id = $1 AND job_id = $5
              AND state IN ('pending', 'running')
            "#,
        )
        .bind(row.output_id)
        .bind(output_state)
        .bind(now)
        .bind(error_code)
        .bind(row.job_id)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
    )
}

async fn apply_quota_slice(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    outcome: &CanonicalExecutorOutcome,
    reservation_id: Uuid,
    now: i64,
) -> Result<QuotaSliceRow, ExecutorTerminalError> {
    let (commit_slice, release_slice, metering_outcome) = match outcome {
        CanonicalExecutorOutcome::Succeeded(_) => (1, 0, "succeeded"),
        CanonicalExecutorOutcome::Failed { error_code } => (0, 1, error_code.as_str()),
        CanonicalExecutorOutcome::Canceled { error_code } => (0, 1, error_code.as_str()),
        CanonicalExecutorOutcome::Uncertain { error_code } => (0, 0, error_code.as_str()),
    };
    let quota: QuotaSliceRow = sqlx::query_as(
        r#"
        UPDATE quota_reservations quota
        SET committed_units = quota.committed_units + output.billable_units * $5,
            released_units = quota.released_units + output.billable_units * $6,
            state = CASE
              WHEN quota.committed_units + quota.released_units
                   + output.billable_units * ($5 + $6)
                   = quota.requested_units
                   AND quota.committed_units + output.billable_units * $5 > 0
                   THEN 'committed'
              WHEN quota.committed_units + quota.released_units
                   + output.billable_units * ($5 + $6)
                   = quota.requested_units
                   THEN 'released'
              ELSE 'reserved'
            END,
            updated_at_ms = $7
        FROM jobs job
        JOIN job_outputs output ON output.job_id = job.job_id
        WHERE quota.reservation_id = $1 AND quota.job_id = $2
          AND quota.tenant_id = $3 AND job.job_id = quota.job_id
          AND job.reservation_id = quota.reservation_id
          AND job.tenant_id = quota.tenant_id
          AND job.economics_contract_version IN (2, 3, 4)
          AND output.output_id = $4
          AND output.billable_units > 0
          AND quota.state IN ('reserved', 'expired')
          AND quota.committed_units + quota.released_units
              + output.billable_units * ($5 + $6) <= quota.requested_units
          AND job.request_id = quota.request_id
          AND job.requested_units = job.billable_units
          AND quota.requested_units = job.billable_units
          AND quota.billing_metric = job.billing_metric
          AND quota.billing_unit = job.billing_unit
        RETURNING quota.reservation_id, quota.requested_units,
                  quota.committed_units, quota.released_units, quota.state,
                  quota.request_id, job.operation,
                  quota.limit_5h, quota.remaining_5h,
                  quota.limit_7d, quota.remaining_7d,
                  job.economics_contract_version, job.output_count,
                  job.billable_units, job.billing_metric, job.billing_unit,
                  output.billable_units AS output_billable_units
        "#,
    )
    .bind(reservation_id)
    .bind(lease.job_id)
    .bind(&lease.tenant_id)
    .bind(lease.output_id)
    .bind(commit_slice)
    .bind(release_slice)
    .bind(now)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorTerminalError::Conflict)?;

    let committed_delta = quota.output_billable_units * commit_slice;
    let released_delta = quota.output_billable_units * release_slice;
    if committed_delta > 0 {
        sqlx::query(
            r#"
            INSERT INTO usage_events
              (event_id, tenant_id, job_id, request_id, operation, units, outcome,
               billing_metric, billing_unit, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, 'charged', $7, $8, $9)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&lease.tenant_id)
        .bind(lease.job_id)
        .bind(&quota.request_id)
        .bind(&quota.operation)
        .bind(committed_delta)
        .bind(&quota.billing_metric)
        .bind(&quota.billing_unit)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }
    sqlx::query(
        r#"
        INSERT INTO metering_events
          (event_id, tenant_id, job_id, reservation_id, request_id, operation,
           event_type, units, outcome, billing_metric, billing_unit, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, 'executor_output_terminal',
                $7, $8, $9, $10, $11)
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
    .bind(&quota.billing_metric)
    .bind(&quota.billing_unit)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(quota)
}

async fn persist_provider_usage_fact(
    tx: &mut Transaction<'_, Postgres>,
    row: &LockedCompletionRow,
    quota: &QuotaSliceRow,
    receipt_id: Uuid,
    outcome: &CanonicalExecutorOutcome,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let terminal_outcome = match outcome {
        CanonicalExecutorOutcome::Succeeded(_) => "succeeded",
        CanonicalExecutorOutcome::Failed { error_code } if error_code == "provider_no_effect" => {
            "no_effect"
        }
        CanonicalExecutorOutcome::Failed { .. } => "failed",
        CanonicalExecutorOutcome::Canceled { .. } => "no_effect",
        CanonicalExecutorOutcome::Uncertain { .. } => return Ok(()),
    };
    if row.economics_contract_version != 4 && terminal_outcome != "succeeded" {
        return Ok(());
    }
    if row.economics_contract_version == 4 {
        return persist_v4_customer_usage_facts(tx, row, quota, receipt_id, terminal_outcome, now)
            .await;
    }
    let (metric, unit, confidence) =
        match (quota.billing_metric.as_str(), quota.billing_unit.as_str()) {
            ("output", "output") => ("image_output", "image", "exact"),
            ("video_second", "second") => ("video_requested_second", "second", "bounded"),
            ("request", "request") => ("request", "request", "exact"),
            _ => return Err(ExecutorTerminalError::Conflict),
        };
    let semantic_key = format!("{receipt_id}:{metric}:request-derived:v1");
    sqlx::query(
        r#"
        INSERT INTO provider_usage_facts (
            usage_fact_id, semantic_key, job_id, output_id, submission_id,
            receipt_id, provider_id, provider_account_id, execution_surface,
            fact_domain, metric, quantity, unit, quantity_source, confidence, evidence_path,
            metadata_json, billing_partition_key, terminal_outcome, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'provider_cli',
                'customer_billable', $9, $10, $11, 'request_derived', $12,
                'job_outputs.billable_units',
                jsonb_build_object(
                    'operation', $13::TEXT,
                    'billing_metric', $14::TEXT,
                    'billing_unit', $15::TEXT,
                    'basis', 'admitted_output_quantity'
                ) || $16::JSONB,
                $17, $18, $19)
        ON CONFLICT (semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(semantic_key)
    .bind(row.job_id)
    .bind(row.output_id)
    .bind(row.submission_id)
    .bind(receipt_id)
    .bind(&row.provider_id)
    .bind(row.provider_account_id)
    .bind(metric)
    .bind(i64::from(quota.output_billable_units))
    .bind(unit)
    .bind(confidence)
    .bind(&row.operation)
    .bind(&quota.billing_metric)
    .bind(&quota.billing_unit)
    .bind(serde_json::json!({}))
    .bind(format!("output:{}", row.output_id))
    .bind(terminal_outcome)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn persist_provider_actual_cost(
    tx: &mut Transaction<'_, Postgres>,
    row: &LockedCompletionRow,
    receipt_id: Uuid,
    outcome: &CanonicalExecutorOutcome,
    provider_cost_evidence: Option<&ProviderReportedCostEvidenceV1>,
) -> Result<(), ExecutorTerminalError> {
    let Some(evidence) = provider_cost_evidence else {
        return Ok(());
    };
    if !matches!(outcome, CanonicalExecutorOutcome::Succeeded(_))
        || row.economics_contract_version != 4
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    let context: FrozenProviderCostContext = sqlx::query_as(
        r#"
        SELECT project_id, api_profile, operation, provider_id,
               provider_model_id, public_model_id, media_kind, service_tier
        FROM customer_price_quotes
        WHERE job_id = $1 AND tenant_id = $2
        "#,
    )
    .bind(row.job_id)
    .bind(&row.tenant_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorTerminalError::Conflict)?;
    let observation = evidence.observation();
    if context.provider_id.as_deref() != Some(observation.provider_id.as_str()) {
        return Err(ExecutorTerminalError::Conflict);
    }
    let resolution = PriceResolutionRequest {
        purpose: "provider_actual".to_string(),
        organization_id: Some(row.tenant_id.clone()),
        project_id: Some(context.project_id),
        provider_id: Some(observation.provider_id.clone()),
        currency: observation.currency.clone(),
        api_profile: context.api_profile,
        operation: context.operation,
        provider_model_id: context.provider_model_id,
        public_model_id: context.public_model_id,
        media_kind: context.media_kind,
        service_tier: context.service_tier,
        execution_surface: observation.execution_surface.clone(),
        billing_mode: "provider_reported".to_string(),
        at_ms: row
            .provider_cost_created_at_ms
            .ok_or(ExecutorTerminalError::Conflict)?,
    };
    let resolved = resolve_provider_actual_price_version_in_transaction(tx, &resolution)
        .await
        .map_err(map_provider_actual_price_resolution_error)?;
    let source_manifest_id = row.manifest_id.ok_or(ExecutorTerminalError::Conflict)?;
    apply_executor_provider_reported_cost(tx, receipt_id, &resolved, source_manifest_id)
        .await
        .map_err(map_provider_cost_error)?;
    Ok(())
}

fn provider_cost_evidence(
    row: &LockedCompletionRow,
) -> Result<Option<ProviderReportedCostEvidenceV1>, ExecutorTerminalError> {
    let fields = (
        row.provider_cost_scope.as_deref(),
        row.provider_cost_provider_id.as_deref(),
        row.provider_cost_execution_surface.as_deref(),
        row.provider_cost_operation_id.as_deref(),
        row.provider_cost_currency.as_deref(),
        row.provider_cost_native_unit.as_deref(),
        row.provider_cost_native_quantity.as_deref(),
        row.provider_cost_authority.as_deref(),
        row.provider_cost_confidence.as_deref(),
        row.provider_cost_evidence_hash.as_deref(),
        row.provider_cost_evidence_path.as_deref(),
        row.provider_cost_created_at_ms,
    );
    if matches!(
        fields,
        (
            None, None, None, None, None, None, None, None, None, None, None, None
        )
    ) {
        return Ok(None);
    }
    let (
        Some(scope),
        Some(provider_id),
        Some(execution_surface),
        Some(provider_operation_id),
        Some("USD"),
        Some("usd_tick"),
        Some(native_quantity),
        Some("provider_reported"),
        Some("exact"),
        Some(evidence_hash),
        Some(evidence_path),
        Some(created_at_ms),
    ) = fields
    else {
        return Err(ExecutorTerminalError::Conflict);
    };
    if provider_id != row.provider_id || created_at_ms <= 0 {
        return Err(ExecutorTerminalError::Conflict);
    }
    let scope = match scope {
        "api_response" => ProviderCostEvidenceScope::ApiResponse,
        "cli_invocation" => ProviderCostEvidenceScope::CliInvocation,
        _ => return Err(ExecutorTerminalError::Conflict),
    };
    let native_quantity = native_quantity
        .parse::<u128>()
        .map_err(|_| ExecutorTerminalError::Conflict)?;
    let evidence_hash: [u8; 32] = hex::decode(evidence_hash)
        .map_err(|_| ExecutorTerminalError::Conflict)?
        .try_into()
        .map_err(|_| ExecutorTerminalError::Conflict)?;
    let observation = ProviderCostObservationV1::provider_reported_usd_ticks_from_evidence_hash(
        provider_id,
        execution_surface,
        provider_operation_id,
        native_quantity,
        evidence_hash,
        evidence_path,
    )
    .map_err(|_| ExecutorTerminalError::Conflict)?;
    ProviderReportedCostEvidenceV1::from_observation(scope, observation)
        .map(Some)
        .map_err(|_| ExecutorTerminalError::Conflict)
}

async fn validate_provider_cost_replay(
    tx: &mut Transaction<'_, Postgres>,
    receipt_id: Uuid,
    provider_cost_evidence: Option<&ProviderReportedCostEvidenceV1>,
) -> Result<(), ExecutorTerminalError> {
    let receipt_cost: Option<Value> = sqlx::query_scalar(
        r#"
        SELECT evidence -> 'provider_reported_cost'
        FROM provider_receipts
        WHERE receipt_id = $1
        "#,
    )
    .bind(receipt_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .flatten();
    let stored: Option<StoredProviderCostReplay> = sqlx::query_as(
        r#"
        SELECT observation.provider_id, observation.execution_surface,
               observation.provider_operation_id, observation.currency,
               observation.native_unit, observation.native_quantity::TEXT,
               observation.authority, observation.confidence,
               observation.evidence_hash, observation.evidence_path,
               observation.amount_micros,
               (SELECT COUNT(*)::BIGINT
                FROM provider_cost_observation_fact_links fact_link
                WHERE fact_link.provider_cost_observation_id =
                    observation.provider_cost_observation_id) AS fact_count,
               (SELECT COUNT(*)::BIGINT
                FROM ledger_transactions ledger
                WHERE ledger.source_provider_cost_observation_id =
                    observation.provider_cost_observation_id
                  AND ledger.transaction_type = 'provider_cost') AS ledger_count
        FROM provider_cost_observation_receipts receipt_link
        JOIN provider_cost_observations observation
          ON observation.provider_cost_observation_id =
             receipt_link.provider_cost_observation_id
        WHERE receipt_link.receipt_id = $1
        "#,
    )
    .bind(receipt_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(evidence) = provider_cost_evidence else {
        return if receipt_cost.is_none() && stored.is_none() {
            Ok(())
        } else {
            Err(ExecutorTerminalError::Conflict)
        };
    };
    let observation = evidence.observation();
    let expected_receipt_cost = json!({
        "canonical_sha256": hex::encode(evidence.canonical_sha256_v1()),
        "evidence_scope": evidence.scope().as_str(),
        "provider_operation_id": observation.provider_operation_id.as_str(),
    });
    let stored = stored.ok_or(ExecutorTerminalError::Conflict)?;
    if receipt_cost.as_ref() != Some(&expected_receipt_cost)
        || stored.provider_id != observation.provider_id
        || stored.execution_surface != observation.execution_surface
        || stored.provider_operation_id != observation.provider_operation_id
        || stored.currency != observation.currency
        || stored.native_unit != observation.native_unit.as_str()
        || stored.native_quantity != observation.native_quantity.to_string()
        || stored.authority != observation.authority.as_str()
        || stored.confidence != observation.confidence.as_str()
        || stored.evidence_hash != hex::encode(observation.evidence_hash)
        || stored.evidence_path != observation.evidence_path
        || stored.fact_count != 1
        || stored.ledger_count != i64::from(stored.amount_micros > 0)
    {
        Err(ExecutorTerminalError::Conflict)
    } else {
        Ok(())
    }
}

async fn persist_v4_customer_usage_facts(
    tx: &mut Transaction<'_, Postgres>,
    row: &LockedCompletionRow,
    quota: &QuotaSliceRow,
    receipt_id: Uuid,
    terminal_outcome: &str,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    persist_customer_usage_facts(
        tx,
        &CustomerUsageOutput {
            job_id: row.job_id,
            output_id: row.output_id,
            provider_id: &row.provider_id,
            provider_account_id: row.provider_account_id,
            operation: &row.operation,
            billing_metric: &quota.billing_metric,
            billing_unit: &quota.billing_unit,
            output_billable_units: quota.output_billable_units,
            terminal_outcome,
        },
        CustomerUsageAuthority::Durable {
            submission_id: row.submission_id,
            receipt_id,
        },
        now,
    )
    .await
    .map_err(map_customer_usage_fact_error)?;
    let context: Value = sqlx::query_scalar(
        "SELECT request_dimensions_json FROM customer_price_quotes WHERE job_id = $1",
    )
    .bind(row.job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorTerminalError::Conflict)?;
    persist_media_inspected_video_usage(tx, row, receipt_id, terminal_outcome, &context, now).await
}

async fn persist_media_inspected_video_usage(
    tx: &mut Transaction<'_, Postgres>,
    row: &LockedCompletionRow,
    receipt_id: Uuid,
    terminal_outcome: &str,
    request_dimensions: &Value,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    if row.operation != VIDEO_GENERATION_OPERATION || terminal_outcome != "succeeded" {
        return Ok(());
    }
    let Some(duration_ms) = row.media_duration_ms else {
        // Rows published before migration 0068 have no immutable duration evidence.
        return Ok(());
    };
    let duration_ms = u64::try_from(duration_ms).map_err(|_| ExecutorTerminalError::Conflict)?;
    if !(1..=86_400_000).contains(&duration_ms) {
        return Err(ExecutorTerminalError::Conflict);
    }
    let duration_seconds = duration_ms
        .checked_add(999)
        .map(|value| value / 1_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ExecutorTerminalError::Conflict)?;
    let mut metadata = merge_usage_metadata(
        request_dimensions,
        &row.operation,
        "video_second",
        "second",
        "inspected_output_duration",
    )?;
    let metadata_object = metadata
        .as_object_mut()
        .ok_or(ExecutorTerminalError::Conflict)?;
    metadata_object.insert("media_duration_ms".to_string(), json!(duration_ms));
    metadata_object.insert(
        "duration_rounding".to_string(),
        Value::String("ceil_to_second".to_string()),
    );
    let semantic_key = format!("{receipt_id}:video_output_second:media-inspected:v1");
    let partition_key = format!("provider-output:{}", row.output_id);
    let exact_replay: bool = sqlx::query_scalar(
        r#"
        WITH inserted AS (
            INSERT INTO provider_usage_facts (
                usage_fact_id, semantic_key, job_id, output_id, submission_id,
                receipt_id, provider_id, provider_account_id, execution_surface,
                fact_domain, metric, quantity, unit, quantity_source, confidence, evidence_path,
                metadata_json, billing_partition_key, terminal_outcome, created_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'provider_cli',
                    'provider_benchmark', 'video_output_second', $9,
                    'second', 'media_inspected', 'exact',
                    'executor_artifact_authorities.media_duration_ms',
                    $10, $11, 'succeeded', $12)
            ON CONFLICT (semantic_key) DO NOTHING
            RETURNING 1
        )
        SELECT EXISTS (SELECT 1 FROM inserted)
            OR EXISTS (
                SELECT 1
                FROM provider_usage_facts existing
                WHERE existing.semantic_key = $2
                  AND existing.job_id = $3
                  AND existing.output_id = $4
                  AND existing.submission_id = $5
                  AND existing.receipt_id = $6
                  AND existing.provider_id = $7
                  AND existing.provider_account_id IS NOT DISTINCT FROM $8
                  AND existing.execution_surface = 'provider_cli'
                  AND existing.fact_domain = 'provider_benchmark'
                  AND existing.metric = 'video_output_second'
                  AND existing.quantity = $9
                  AND existing.unit = 'second'
                  AND existing.quantity_source = 'media_inspected'
                  AND existing.confidence = 'exact'
                  AND existing.evidence_path =
                      'executor_artifact_authorities.media_duration_ms'
                  AND existing.metadata_json = $10
                  AND existing.billing_partition_key = $11
                  AND existing.terminal_outcome = 'succeeded'
            )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(semantic_key)
    .bind(row.job_id)
    .bind(row.output_id)
    .bind(row.submission_id)
    .bind(receipt_id)
    .bind(&row.provider_id)
    .bind(row.provider_account_id)
    .bind(duration_seconds)
    .bind(metadata)
    .bind(partition_key)
    .bind(now)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if exact_replay {
        Ok(())
    } else {
        Err(ExecutorTerminalError::Conflict)
    }
}

fn merge_usage_metadata(
    dimensions: &serde_json::Value,
    operation: &str,
    billing_metric: &str,
    billing_unit: &str,
    basis: &str,
) -> Result<serde_json::Value, ExecutorTerminalError> {
    let mut metadata = dimensions
        .as_object()
        .cloned()
        .ok_or(ExecutorTerminalError::Conflict)?;
    metadata.insert("operation".to_string(), json!(operation));
    metadata.insert("billing_metric".to_string(), json!(billing_metric));
    metadata.insert("billing_unit".to_string(), json!(billing_unit));
    metadata.insert("basis".to_string(), json!(basis));
    Ok(serde_json::Value::Object(metadata))
}

fn map_customer_usage_fact_error(error: CustomerUsageFactError) -> ExecutorTerminalError {
    match error {
        CustomerUsageFactError::Conflict => ExecutorTerminalError::Conflict,
        CustomerUsageFactError::Unavailable => ExecutorTerminalError::Unavailable,
    }
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
        SELECT job.output_count AS expected_output_count,
               job.billable_units AS expected_billable_units,
               COUNT(output.output_id)::BIGINT AS output_count,
               COALESCE(SUM(output.billable_units), 0)::BIGINT AS billable_units,
               COUNT(*) FILTER (WHERE output.state = 'succeeded')::BIGINT AS succeeded_count,
               COALESCE(SUM(output.billable_units)
                   FILTER (WHERE output.state = 'succeeded'), 0)::BIGINT
                   AS succeeded_billable_units,
               COUNT(*) FILTER (WHERE output.state = 'failed')::BIGINT AS failed_count,
               COALESCE(SUM(output.billable_units)
                   FILTER (WHERE output.state = 'failed'), 0)::BIGINT
                   AS failed_billable_units,
               COUNT(*) FILTER (WHERE output.state = 'uncertain')::BIGINT AS uncertain_count,
               COUNT(*) FILTER (WHERE output.state IN ('pending', 'running'))::BIGINT
                   AS active_count,
               COALESCE((
                   SELECT payload.command_schema IN (
                       'openai.images.generation.v1', 'openai.images.edit.v1'
                   )
                   FROM job_payloads payload
                   WHERE payload.job_id = job.job_id
               ), FALSE) AS partial_success_allowed,
               (array_agg(output.error_code ORDER BY output.output_index)
                    FILTER (WHERE output.error_code IS NOT NULL))[1] AS first_error_code
        FROM jobs job
        LEFT JOIN job_outputs output ON output.job_id = job.job_id
        WHERE job.job_id = $1 AND job.economics_contract_version IN (2, 3, 4)
        GROUP BY job.job_id, job.output_count, job.billable_units
        "#,
    )
    .bind(lease.job_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if aggregate.expected_output_count != quota.output_count
        || aggregate.expected_billable_units != quota.billable_units
        || aggregate.output_count != i64::from(aggregate.expected_output_count)
        || aggregate.billable_units != i64::from(aggregate.expected_billable_units)
        || quota.requested_units != aggregate.expected_billable_units
        || i64::from(quota.committed_units) != aggregate.succeeded_billable_units
        || i64::from(quota.released_units) != aggregate.failed_billable_units
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    if aggregate.active_count > 0 {
        if quota.state != "reserved" {
            return Err(ExecutorTerminalError::Conflict);
        }
        return Ok(ExecutorParentTerminalState::Pending);
    }
    let parent_state = terminal_parent_state(
        aggregate.succeeded_count,
        aggregate.failed_count,
        aggregate.uncertain_count,
        aggregate.output_count,
        aggregate.partial_success_allowed,
    )?;
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

fn terminal_parent_state(
    succeeded_count: i64,
    failed_count: i64,
    uncertain_count: i64,
    output_count: i64,
    partial_success_allowed: bool,
) -> Result<ExecutorParentTerminalState, ExecutorTerminalError> {
    let state = if uncertain_count > 0 {
        ExecutorParentTerminalState::Uncertain
    } else if succeeded_count == output_count || (partial_success_allowed && succeeded_count > 0) {
        ExecutorParentTerminalState::Succeeded
    } else if failed_count > 0 {
        ExecutorParentTerminalState::Failed
    } else {
        return Err(ExecutorTerminalError::Conflict);
    };
    Ok(state)
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
        let artifact_count = i32::try_from(aggregate.succeeded_count)
            .map_err(|_| ExecutorTerminalError::Conflict)?;
        persist_projection(tx, lease, quota, artifact_count, now).await?;
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
    artifact_count: i32,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let (command_schema, command_json, api_profile): (String, Value, String) = sqlx::query_as(
        r#"
        SELECT p.command_schema, p.command_json, a.api_profile
        FROM job_payloads p
        JOIN admission_sessions a ON a.session_id = p.admission_session_id
        WHERE p.job_id = $1
        "#,
    )
    .bind(lease.job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ExecutorTerminalError::Conflict)?;
    match command_schema.as_str() {
        GENERATION_COMMAND_SCHEMA => {
            persist_image_projection(tx, lease, quota, artifact_count, command_json, now).await
        }
        EDIT_COMMAND_SCHEMA => {
            persist_edit_projection(tx, lease, quota, artifact_count, command_json, now).await
        }
        GROK_IMAGE_GENERATION_COMMAND_SCHEMA => {
            persist_grok_image_generation_projection(
                tx,
                lease,
                quota,
                artifact_count,
                command_json,
                now,
            )
            .await
        }
        GROK_IMAGE_EDIT_COMMAND_SCHEMA => {
            persist_grok_image_edit_projection(tx, lease, quota, artifact_count, command_json, now)
                .await
        }
        GROK_VIDEO_GENERATION_COMMAND_SCHEMA => {
            persist_video_projection(tx, lease, quota, artifact_count, command_json, now).await
        }
        DREAMINA_SUBMIT_COMMAND_SCHEMA => {
            persist_dreamina_projection(
                tx,
                lease,
                quota,
                artifact_count,
                command_json,
                &api_profile,
                now,
            )
            .await
        }
        _ => Err(ExecutorTerminalError::Conflict),
    }
}

async fn persist_dreamina_projection(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    artifact_count: i32,
    command_json: Value,
    api_profile: &str,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let bytes = serde_json::to_vec(&command_json).map_err(|_| ExecutorTerminalError::Conflict)?;
    match parse_submit_command(&bytes).map_err(|_| ExecutorTerminalError::Conflict)? {
        DreaminaSubmitRequestV1::TextToImage(request) => {
            if !matches!(quota.economics_contract_version, 2 | 4)
                || quota.operation != GENERATION_OPERATION
                || quota.output_count != i32::from(request.generate_num())
                || quota.billing_metric != "output"
            {
                return Err(ExecutorTerminalError::Conflict);
            }
            let size = match (request.ratio(), request.width(), request.height()) {
                (Some(ratio), None, None) => {
                    format!("{}:{}", request.resolution().as_str(), ratio.as_str())
                }
                (None, Some(width), Some(height)) => format!("{width}x{height}"),
                _ => return Err(ExecutorTerminalError::Conflict),
            };
            insert_response_projection(
                tx,
                lease.job_id,
                api_profile,
                GENERATION_OPERATION,
                GENERATION_RESPONSE_SCHEMA,
                "auto",
                "auto",
                &size,
                "opaque",
                quota,
                artifact_count,
                now,
            )
            .await
        }
        DreaminaSubmitRequestV1::TextToVideo(request) => {
            if !matches!(quota.economics_contract_version, 3 | 4)
                || quota.operation != VIDEO_GENERATION_OPERATION
                || quota.output_count != 1
                || quota.output_billable_units != quota.billable_units
                || quota.billing_metric != "video_second"
                || quota.billing_unit != "second"
                || i32::from(request.duration_seconds()) != quota.billable_units
            {
                return Err(ExecutorTerminalError::Conflict);
            }
            insert_response_projection(
                tx,
                lease.job_id,
                api_profile,
                VIDEO_GENERATION_OPERATION,
                "dreamina-cli.videos.response.v1",
                "mp4",
                NOT_APPLICABLE,
                request.resolution().as_str(),
                NOT_APPLICABLE,
                quota,
                artifact_count,
                now,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_response_projection(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    api_profile: &str,
    operation: &str,
    response_schema: &str,
    output_format: &str,
    quality: &str,
    size: &str,
    background: &str,
    quota: &QuotaSliceRow,
    artifact_count: i32,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    sqlx::query(
        r#"
        INSERT INTO job_response_projections
          (job_id, api_profile, operation, response_schema, created_at_seconds,
           output_format, quality, size, background, stream,
           limit_5h, remaining_5h, limit_7d, remaining_7d,
           artifact_count, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, FALSE,
                $10, $11, $12, $13, $14, $15)
        "#,
    )
    .bind(job_id)
    .bind(api_profile)
    .bind(operation)
    .bind(response_schema)
    .bind(now / 1_000)
    .bind(output_format)
    .bind(quality)
    .bind(size)
    .bind(background)
    .bind(quota.limit_5h)
    .bind(quota.remaining_5h)
    .bind(quota.limit_7d)
    .bind(quota.remaining_7d)
    .bind(artifact_count)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn persist_image_projection(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    artifact_count: i32,
    command_json: Value,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let command: GenerationCommandV1 =
        serde_json::from_value(command_json).map_err(|_| ExecutorTerminalError::Conflict)?;
    if !matches!(quota.economics_contract_version, 2 | 4)
        || command.n != u32::try_from(quota.output_count).unwrap_or_default()
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
    .bind(artifact_count)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn persist_edit_projection(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    artifact_count: i32,
    command_json: Value,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let command: EditCommandV1 =
        serde_json::from_value(command_json).map_err(|_| ExecutorTerminalError::Conflict)?;
    if !matches!(quota.economics_contract_version, 2 | 4)
        || command.schema_version != EDIT_COMMAND_SCHEMA_VERSION
        || command.operation != EDIT_OPERATION
        || quota.operation != EDIT_OPERATION
        || command.n != u32::try_from(quota.output_count).unwrap_or_default()
        || command.provider_id.is_empty()
        || command.model.is_empty()
        || command.source_api_profile.is_empty()
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    insert_response_projection(
        tx,
        lease.job_id,
        &command.source_api_profile,
        EDIT_OPERATION,
        GENERATION_RESPONSE_SCHEMA,
        &command.output_format,
        &command.quality,
        &command.size,
        &command.background,
        quota,
        artifact_count,
        now,
    )
    .await
}

async fn persist_grok_image_generation_projection(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    artifact_count: i32,
    command_json: Value,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let command_bytes =
        serde_json::to_vec(&command_json).map_err(|_| ExecutorTerminalError::Conflict)?;
    let payload = parse_image_generation_payload(&command_bytes)
        .map_err(|_| ExecutorTerminalError::Conflict)?;
    let command = payload.source_command();
    if !matches!(quota.economics_contract_version, 2 | 4)
        || quota.operation != GENERATION_OPERATION
        || command.n != u32::try_from(quota.output_count).unwrap_or_default()
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    let size = format!(
        "{}:{}",
        enum_wire_value(command.resolution)?,
        enum_wire_value(command.aspect_ratio)?
    );
    insert_response_projection(
        tx,
        lease.job_id,
        XAI_IMAGES_API_PROFILE,
        GENERATION_OPERATION,
        GENERATION_RESPONSE_SCHEMA,
        "auto",
        "auto",
        &size,
        "opaque",
        quota,
        artifact_count,
        now,
    )
    .await
}

async fn persist_grok_image_edit_projection(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    artifact_count: i32,
    command_json: Value,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let command_bytes =
        serde_json::to_vec(&command_json).map_err(|_| ExecutorTerminalError::Conflict)?;
    let payload =
        parse_image_edit_payload(&command_bytes).map_err(|_| ExecutorTerminalError::Conflict)?;
    if !matches!(quota.economics_contract_version, 2 | 4)
        || quota.operation != EDIT_OPERATION
        || quota.output_count != 1
    {
        return Err(ExecutorTerminalError::Conflict);
    }
    insert_response_projection(
        tx,
        lease.job_id,
        XAI_IMAGES_API_PROFILE,
        EDIT_OPERATION,
        GENERATION_RESPONSE_SCHEMA,
        "auto",
        "auto",
        payload.request().aspect_ratio().as_str(),
        "opaque",
        quota,
        artifact_count,
        now,
    )
    .await
}

fn enum_wire_value<T: serde::Serialize>(value: T) -> Result<String, ExecutorTerminalError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or(ExecutorTerminalError::Conflict)
}

async fn persist_video_projection(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ExecutorTerminalLease,
    quota: &QuotaSliceRow,
    artifact_count: i32,
    command_json: Value,
    now: i64,
) -> Result<(), ExecutorTerminalError> {
    let command_bytes =
        serde_json::to_vec(&command_json).map_err(|_| ExecutorTerminalError::Conflict)?;
    let payload = parse_video_generation_payload(&command_bytes)
        .map_err(|_| ExecutorTerminalError::Conflict)?;
    let command = payload.source_command();
    let resolution = serde_json::to_value(command.resolution)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or(ExecutorTerminalError::Conflict)?;
    if !matches!(quota.economics_contract_version, 3 | 4)
        || quota.operation != VIDEO_GENERATION_OPERATION
        || quota.output_count != 1
        || quota.output_billable_units != quota.billable_units
        || quota.billing_metric != "video_second"
        || quota.billing_unit != "second"
        || i32::from(command.duration) != quota.billable_units
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
        VALUES ($1, $2, $3, $4, $5, 'mp4', $6, $7, $6, FALSE,
                $8, $9, $10, $11, $12, $13)
        "#,
    )
    .bind(lease.job_id)
    .bind(XAI_VIDEOS_API_PROFILE)
    .bind(VIDEO_GENERATION_OPERATION)
    .bind(XAI_VIDEO_RESPONSE_SCHEMA)
    .bind(now / 1_000)
    .bind(NOT_APPLICABLE)
    .bind(resolution)
    .bind(quota.limit_5h)
    .bind(quota.remaining_5h)
    .bind(quota.limit_7d)
    .bind(quota.remaining_7d)
    .bind(artifact_count)
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

fn map_customer_rating_error(error: CustomerRatingStoreError) -> ExecutorTerminalError {
    match error {
        CustomerRatingStoreError::Unavailable => ExecutorTerminalError::Unavailable,
        CustomerRatingStoreError::InvalidInput => ExecutorTerminalError::InvalidInput,
        CustomerRatingStoreError::Conflict => ExecutorTerminalError::Conflict,
    }
}

fn map_provider_actual_price_resolution_error(
    error: PriceResolutionError,
) -> ExecutorTerminalError {
    match error {
        PriceResolutionError::StoreUnavailable | PriceResolutionError::NotFound => {
            ExecutorTerminalError::Unavailable
        }
        PriceResolutionError::InvalidRequest => ExecutorTerminalError::InvalidInput,
        PriceResolutionError::Ambiguous => ExecutorTerminalError::Conflict,
    }
}

fn map_provider_cost_error(error: ProviderCostStoreError) -> ExecutorTerminalError {
    match error {
        ProviderCostStoreError::Unavailable => ExecutorTerminalError::Unavailable,
        ProviderCostStoreError::InvalidInput => ExecutorTerminalError::InvalidInput,
        ProviderCostStoreError::Conflict => ExecutorTerminalError::Conflict,
    }
}

fn unavailable(_: sqlx::Error) -> ExecutorTerminalError {
    ExecutorTerminalError::Unavailable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_artifacts_are_bound_to_their_media_operation() {
        let image = succeeded("image/png");
        let video = succeeded("video/mp4");
        assert!(validate_operation_artifact(GENERATION_OPERATION, &image).is_ok());
        assert!(validate_operation_artifact(EDIT_OPERATION, &image).is_ok());
        assert!(validate_operation_artifact(VIDEO_GENERATION_OPERATION, &video).is_ok());
        assert_eq!(
            validate_operation_artifact(VIDEO_GENERATION_OPERATION, &image),
            Err(ExecutorTerminalError::Conflict)
        );
        assert_eq!(
            validate_operation_artifact(GENERATION_OPERATION, &video),
            Err(ExecutorTerminalError::Conflict)
        );
        assert!(
            validate_operation_artifact(
                VIDEO_GENERATION_OPERATION,
                &CanonicalExecutorOutcome::Failed {
                    error_code: "provider_failed".to_owned(),
                },
            )
            .is_ok()
        );
    }

    #[test]
    fn partial_parent_success_is_scoped_and_fail_closed() {
        assert_eq!(
            terminal_parent_state(3, 1, 0, 4, true),
            Ok(ExecutorParentTerminalState::Succeeded)
        );
        assert_eq!(
            terminal_parent_state(3, 1, 0, 4, false),
            Ok(ExecutorParentTerminalState::Failed)
        );
        assert_eq!(
            terminal_parent_state(0, 4, 0, 4, true),
            Ok(ExecutorParentTerminalState::Failed)
        );
        assert_eq!(
            terminal_parent_state(3, 0, 1, 4, true),
            Ok(ExecutorParentTerminalState::Uncertain)
        );
    }

    fn succeeded(media_type: &str) -> CanonicalExecutorOutcome {
        CanonicalExecutorOutcome::Succeeded(ExecutorTerminalArtifact {
            authority_id: Uuid::new_v4(),
            storage_backend: "filesystem-v1".to_owned(),
            storage_namespace: "/tmp/test".to_owned(),
            object_key: "executor-objects/test".to_owned(),
            sha256_hex: "a".repeat(64),
            byte_size: 1,
            media_type: media_type.to_owned(),
        })
    }
}
