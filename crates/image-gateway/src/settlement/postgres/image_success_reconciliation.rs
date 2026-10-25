//! A narrowly audited delivery overlay. It never submits or polls a provider and
//! never changes the original uncertain executor, reduction, or receipt.
use std::path::Path;

use image_provider_dreamina_cli::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaSubmitRequestV1, parse_submit_command,
};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, GENERATION_RESPONSE_SCHEMA,
        GenerationResponseProjection, GenerationResultManifest, sha256_hex,
    },
    pricing::{
        admission::dreamina_pricing_facts,
        customer_usage::{
            CustomerUsageAuthority, CustomerUsageOutput,
            persist_customer_usage_facts_with_operator_resolution,
        },
        postgres_rating::settle_customer_quote,
    },
    usage::UsageSnapshot,
};

mod evidence;
mod query_only_evidence;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaImageSuccessResolutionRequest {
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub reviewed_stdout_sha256: String,
    pub operator_reference: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, sqlx::FromRow)]
pub struct DreaminaImageSuccessResolution {
    pub resolution_id: Uuid,
    pub job_id: Uuid,
    pub request_id: String,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub original_receipt_id: Uuid,
    pub hold_id: Uuid,
    pub artifact_id: Uuid,
    pub artifact_sha256: String,
    pub artifact_byte_size: i64,
    pub currency: String,
    pub captured_micros: i64,
    pub released_micros: i64,
    pub committed_units: i32,
    pub released_units: i32,
}

#[derive(sqlx::FromRow)]
struct LockedJob {
    tenant_id: String,
    request_id: String,
    operation: String,
    work_item_id: Uuid,
    attempt_execution_id: Uuid,
    lease_epoch: i64,
    output_id: Uuid,
    provider_account_id: Uuid,
    reservation_id: Uuid,
    original_receipt_id: Uuid,
    hold_id: Uuid,
    command_hash: String,
    command_schema: String,
    command_json: Value,
    execution_binding_sha256: String,
    quote_hash: String,
    request_dimensions_json: Value,
    remote_operation_id: String,
    api_profile: String,
    limit_5h: i32,
    remaining_5h: i32,
    limit_7d: i32,
    remaining_7d: i32,
}

/// Deliver source index zero from an explicitly supported reviewed receipt against
/// the original one-image quote. All database effects commit in one transaction.
pub async fn resolve_dreamina_image_success(
    pool: &PgPool,
    blobs: &dyn ArtifactBlobStore,
    evidence_root: &Path,
    request: &DreaminaImageSuccessResolutionRequest,
) -> Result<DreaminaImageSuccessResolution, ImageGatewayError> {
    if request.job_id.is_nil()
        || request.submission_id.is_nil()
        || request.executor_execution_id.is_nil()
        || request.submission_id == request.executor_execution_id
        || !valid_sha256(&request.reviewed_stdout_sha256)
        || request.operator_reference.trim().is_empty()
        || request.operator_reference.len() > 255
        || request.operator_reference.chars().any(char::is_control)
    {
        return Err(conflict());
    }
    let evidence = evidence::load(evidence_root, request)?;
    let mut input = json!({
        "schema_version": 1, "job_id": request.job_id, "submission_id": request.submission_id,
        "executor_execution_id": request.executor_execution_id, "operator_reference": request.operator_reference,
        "reviewed_stdout_sha256": evidence.stdout_sha256, "execution_audit_sha256": evidence.execution_audit_sha256,
        "review_sha256": evidence.review_sha256, "artifact_sha256": evidence.selected_sha256,
        "selected_source_index": 0, "selected_item_id": evidence.selected_item_id,
        "quote_hash": evidence.quote_hash, "command_hash": evidence.provider_command_sha256,
        "remote_operation_id": evidence.remote_operation_id, "storage_identity": blobs.storage_identity()
    });
    if evidence.mode == query_only_evidence::MODE {
        input["schema_version"] = json!(2);
        input["evidence_mode"] = json!(evidence.mode);
        input["retrieval_audit_sha256"] = json!(evidence.retrieval_audit_sha256);
        input["image_url_sha256"] = json!(evidence.image_url_sha256);
        input["tenant_id"] = json!(evidence.tenant_id);
        input["auth_attribution"] = json!(evidence.auth_attribution);
    }
    let input_sha256 = sha256_hex(&serde_json::to_vec(&input).map_err(|_| conflict())?);
    let original_error = if evidence.mode == query_only_evidence::MODE {
        "dreamina_poll_protocol_invalid"
    } else {
        "dreamina_poll_artifact_invalid"
    };
    let mut tx = pool.begin().await.map_err(unavailable)?;
    // READ COMMITTED lets a waiter observe the committed winner after acquiring
    // these locks. The input hash makes its replay exact and conflict explicit.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let (tenant_id, currency): (String, String) = sqlx::query_as(
        "SELECT job.tenant_id, hold.currency FROM jobs job JOIN customer_billing_holds hold USING (job_id) WHERE job.job_id = $1",
    ).bind(request.job_id).fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
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
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT input_sha256 FROM dreamina_operator_image_success_resolutions WHERE job_id = $1",
    )
    .bind(request.job_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    if let Some(existing) = existing {
        if existing != input_sha256 {
            return Err(conflict());
        }
        assert_resolution(&mut tx, request.job_id).await?;
        let result = load_result(&mut tx, request.job_id).await?;
        tx.commit().await.map_err(storage_conflict)?;
        return Ok(result);
    }
    let row: LockedJob = sqlx::query_as(
        r#"SELECT job.tenant_id, job.request_id, job.operation, work.work_item_id,
          attempt.execution_id AS attempt_execution_id, attempt.lease_epoch, output.output_id,
          submission.provider_account_id, quota.reservation_id, receipt.receipt_id AS original_receipt_id,
          hold.hold_id, submission.command_hash, payload.command_schema, payload.command_json,
          intent.execution_binding_sha256, quote.quote_hash, quote.request_dimensions_json,
          task.remote_operation_id, admission.api_profile,
          quota.limit_5h, quota.remaining_5h, quota.limit_7d, quota.remaining_7d
        FROM jobs job
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt ON attempt.work_item_id = work.work_item_id AND attempt.execution_id = work.execution_id AND attempt.lease_epoch = work.lease_epoch
        JOIN job_outputs output ON output.job_id = job.job_id
        JOIN provider_submissions submission ON submission.job_id = job.job_id AND submission.output_id = output.output_id AND submission.work_item_id = work.work_item_id
        JOIN executor_executions execution ON execution.executor_execution_id = submission.executor_execution_id AND execution.submission_id = submission.submission_id
        JOIN provider_remote_submit_intents intent ON intent.submission_id = submission.submission_id AND intent.executor_execution_id = execution.executor_execution_id
        JOIN provider_remote_tasks task ON task.submission_id = submission.submission_id AND task.executor_execution_id = execution.executor_execution_id
        JOIN executor_terminal_reductions reduction ON reduction.submission_id = submission.submission_id AND reduction.executor_execution_id = execution.executor_execution_id
        JOIN provider_receipts receipt ON receipt.receipt_id = reduction.provider_receipt_id AND receipt.submission_id = submission.submission_id AND receipt.output_id = output.output_id AND receipt.job_id = job.job_id
        JOIN executor_capacity_allocations allocation ON allocation.submission_id = submission.submission_id AND allocation.executor_execution_id = execution.executor_execution_id
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id AND quota.job_id = job.job_id AND quota.tenant_id = job.tenant_id
        JOIN customer_billing_holds hold ON hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id AND quote.job_id = job.job_id
        JOIN job_payloads payload ON payload.job_id = job.job_id
        JOIN admission_sessions admission ON admission.session_id = payload.admission_session_id
        WHERE job.job_id = $1 AND submission.submission_id = $2 AND execution.executor_execution_id = $3
          AND job.provider_id = 'dreamina-cli' AND job.operation = 'generation'
          AND job.output_count = 1 AND job.billable_units = 1 AND job.requested_units = 1
          AND job.economics_contract_version = 4 AND job.state = 'uncertain' AND job.charged_units = 0
          AND job.last_error_code = $4
          AND work.state = 'uncertain' AND work.lease_owner IS NULL AND work.lease_expires_at_ms IS NULL
          AND attempt.state = 'uncertain' AND output.state = 'uncertain' AND output.output_index = 0 AND output.billable_units = 1
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND task.state = 'uncertain' AND task.error_code = $4
          AND task.poll_owner IS NULL AND task.poll_lease_expires_at_ms IS NULL
          AND reduction.state = 'completed' AND reduction.resolved_state = 'uncertain'
          AND reduction.customer_artifact_id IS NULL AND receipt.outcome = 'uncertain'
          AND allocation.state = 'released'
          AND quota.state IN ('reserved','expired') AND quota.committed_units = 0 AND quota.released_units = 0
          AND hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0
        FOR UPDATE OF work, attempt, output, submission, execution, intent, task, reduction, allocation, quota, hold
        "#,
    ).bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id).bind(original_error)
        .fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
    let idempotency_rows: Vec<i32> =
        sqlx::query_scalar("SELECT 1 FROM idempotency_requests WHERE job_id = $1 FOR UPDATE")
            .bind(request.job_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
    if idempotency_rows.is_empty() {
        return Err(conflict());
    }
    if row.command_hash != evidence.provider_command_sha256
        || row.quote_hash != evidence.quote_hash
        || row.remote_operation_id != evidence.remote_operation_id
        || row.command_schema != DREAMINA_SUBMIT_COMMAND_SCHEMA
    {
        return Err(conflict());
    }
    let command =
        parse_submit_command(&serde_json::to_vec(&row.command_json).map_err(|_| conflict())?)
            .map_err(|_| conflict())?;
    let DreaminaSubmitRequestV1::TextToImage(ref image) = command else {
        return Err(conflict());
    };
    if image.generate_num() != 1
        || (evidence.mode == query_only_evidence::MODE
            && (row
                .command_json
                .get("model_version")
                .and_then(Value::as_str)
                != Some("5.0Pro")
                || evidence.tenant_id.as_deref() != Some(row.tenant_id.as_str())))
    {
        return Err(conflict());
    }
    let facts = dreamina_pricing_facts(&command, &row.api_profile).map_err(|_| conflict())?;
    let mut request_dimensions = serde_json::to_value(&facts.dimensions).map_err(|_| conflict())?;
    request_dimensions
        .as_object_mut()
        .ok_or_else(conflict)?
        .insert(
            "processing_mode".into(),
            json!(crate::admission::PricingProcessingMode::Synchronous.as_str()),
        );
    if facts.output_count != 1
        || facts.output_billable_units != 1
        || request_dimensions != row.request_dimensions_json
    {
        return Err(conflict());
    }
    let now = super::database_now(&mut tx).await?;
    let resolution_id = Uuid::new_v4();
    let artifact_id = Uuid::new_v4();
    sqlx::query(r#"INSERT INTO dreamina_operator_image_success_resolutions
        (resolution_id, job_id, submission_id, executor_execution_id, output_id, original_receipt_id, hold_id, artifact_id,
         remote_operation_id, command_hash, execution_binding_sha256, quote_hash, reviewed_stdout_sha256,
         execution_audit_sha256, review_sha256, input_sha256, selected_source_index, selected_item_id,
         upstream_result_count, artifact_sha256, artifact_byte_size, artifact_media_type, artifact_width, artifact_height,
         operator_reference, created_at_ms, evidence_mode, retrieval_audit_sha256, image_url_sha256, tenant_id, auth_attribution_json)
        VALUES ($1,$2,$3,$4,$23,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,0,$16,$24,$17,$18,'image/png',$19,$20,$21,$22,$25,$26,$27,$28,$29)"#)
        .bind(resolution_id).bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id)
        .bind(row.original_receipt_id).bind(row.hold_id).bind(artifact_id).bind(&row.remote_operation_id)
        .bind(&row.command_hash).bind(&row.execution_binding_sha256).bind(&row.quote_hash).bind(&evidence.stdout_sha256)
        .bind(&evidence.execution_audit_sha256).bind(&evidence.review_sha256).bind(&input_sha256).bind(&evidence.selected_item_id)
        .bind(&evidence.selected_sha256).bind(evidence.bytes.len() as i64).bind(i32::try_from(evidence.width).map_err(|_| conflict())?)
        .bind(i32::try_from(evidence.height).map_err(|_| conflict())?).bind(&request.operator_reference).bind(now).bind(row.output_id).bind(evidence.upstream_result_count).bind(evidence.mode).bind(&evidence.retrieval_audit_sha256).bind(&evidence.image_url_sha256).bind(&evidence.tenant_id).bind(&evidence.auth_attribution)
        .execute(&mut *tx).await.map_err(storage_conflict)?;
    let artifact = blobs
        .put(
            ArtifactIdentity {
                artifact_id,
                tenant_id: row.tenant_id.clone(),
                job_id: request.job_id,
                work_item_id: row.work_item_id,
                execution_id: row.attempt_execution_id,
                lease_epoch: row.lease_epoch,
                output_index: 0,
                media_type: "image/png".into(),
            },
            &evidence.bytes,
        )
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("artifact storage unavailable"))?;
    if artifact.sha256_hex != evidence.selected_sha256
        || artifact.byte_size != evidence.bytes.len() as u64
        || blobs
            .get(&artifact)
            .await
            .map_err(|_| ImageGatewayError::artifact_integrity())?
            != evidence.bytes
    {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let result = GenerationResultManifest {
        job_id: request.job_id,
        tenant_id: row.tenant_id.clone(),
        projection: GenerationResponseProjection {
            api_profile: row.api_profile.clone(),
            operation: row.operation.clone(),
            response_schema: GENERATION_RESPONSE_SCHEMA.into(),
            created_at_seconds: now / 1000,
            output_format: "auto".into(),
            quality: "auto".into(),
            size: format!("{}x{}", evidence.width, evidence.height),
            background: "opaque".into(),
            stream: false,
            usage: UsageSnapshot {
                limit_5h: positive(row.limit_5h)?,
                remaining_5h: positive(row.remaining_5h)?,
                limit_7d: positive(row.limit_7d)?,
                remaining_7d: positive(row.remaining_7d)?,
            },
        },
        artifacts: vec![artifact],
    };
    super::results::persist_generation_result(&mut tx, &result, now).await?;
    transition(&mut tx, request, &row, now).await?;
    persist_customer_usage_facts_with_operator_resolution(
        &mut tx,
        &CustomerUsageOutput {
            job_id: request.job_id,
            output_id: row.output_id,
            provider_id: "dreamina-cli",
            provider_account_id: Some(row.provider_account_id),
            operation: &row.operation,
            billing_metric: "output",
            billing_unit: "output",
            output_billable_units: 1,
            terminal_outcome: "succeeded",
        },
        CustomerUsageAuthority::Durable {
            submission_id: request.submission_id,
            receipt_id: row.original_receipt_id,
        },
        resolution_id,
        now,
    )
    .await
    .map_err(|_| conflict())?;
    let rating = settle_customer_quote(&mut tx, request.job_id, &row.tenant_id)
        .await
        .map_err(|_| conflict())?;
    let payload = json!({"resolution_id":resolution_id,"submission_id":request.submission_id,"executor_execution_id":request.executor_execution_id,
        "original_receipt_id":row.original_receipt_id,"artifact_id":artifact_id,"artifact_sha256":evidence.selected_sha256,
        "selected_source_index":0,"upstream_result_count":evidence.upstream_result_count,"evidence_mode":evidence.mode,"delivered_count":1,"committed_units":1,
        "currency":rating.currency,"captured_micros":rating.total_amount_micros,"reviewed_stdout_sha256":evidence.stdout_sha256});
    for table in ["job_events", "outbox_events"] {
        sqlx::query(AssertSqlSafe(format!("INSERT INTO {table} (event_id,job_id,event_type,semantic_key,payload_json,created_at_ms) VALUES ($1,$2,'job.succeeded','job.operator_dreamina_image_success',$3,$4)")))
            .bind(Uuid::new_v4()).bind(request.job_id).bind(&payload).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
    }
    assert_resolution(&mut tx, request.job_id).await?;
    let result = load_result(&mut tx, request.job_id).await?;
    tx.commit().await.map_err(storage_conflict)?;
    // A failed transaction may leave an unreferenced blob. Never delete it here:
    // a concurrent committed resolution may own storage independently.
    Ok(result)
}

async fn transition(
    tx: &mut Transaction<'_, Postgres>,
    request: &DreaminaImageSuccessResolutionRequest,
    row: &LockedJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    for (query, id) in [
        (
            "UPDATE jobs SET state='succeeded',charged_units=1,last_error_code=NULL,updated_at_ms=$2 WHERE job_id=$1 AND state='uncertain' AND charged_units=0",
            request.job_id,
        ),
        (
            "UPDATE work_items SET state='succeeded',updated_at_ms=$2 WHERE work_item_id=$1 AND state='uncertain' AND lease_owner IS NULL AND lease_expires_at_ms IS NULL",
            row.work_item_id,
        ),
        (
            "UPDATE job_attempts SET state='succeeded',error_code=NULL,updated_at_ms=$2 WHERE execution_id=$1 AND state='uncertain'",
            row.attempt_execution_id,
        ),
        (
            "UPDATE job_outputs SET state='succeeded',error_code=NULL,finished_at_ms=$2,updated_at_ms=$2 WHERE output_id=$1 AND state='uncertain'",
            row.output_id,
        ),
        (
            "UPDATE quota_reservations SET state='committed',committed_units=1,updated_at_ms=$2 WHERE reservation_id=$1 AND state IN ('reserved','expired') AND requested_units=1 AND committed_units=0 AND released_units=0",
            row.reservation_id,
        ),
    ] {
        if sqlx::query(query)
            .bind(id)
            .bind(now)
            .execute(&mut **tx)
            .await
            .map_err(storage_conflict)?
            .rows_affected()
            != 1
        {
            return Err(conflict());
        }
    }
    let expected: i64 =
        sqlx::query_scalar("SELECT count(*) FROM idempotency_requests WHERE job_id=$1")
            .bind(request.job_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(unavailable)?;
    let updated = sqlx::query("UPDATE idempotency_requests SET state='succeeded',terminal_outcome='succeeded',updated_at_ms=$2 WHERE job_id=$1 AND state='uncertain' AND terminal_outcome='uncertain'")
        .bind(request.job_id).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?;
    if expected == 0 || updated.rows_affected() != expected as u64 {
        return Err(conflict());
    }
    sqlx::query("INSERT INTO usage_events (event_id,tenant_id,job_id,request_id,operation,units,outcome,billing_metric,billing_unit,created_at_ms) VALUES ($1,$2,$3,$4,$5,1,'charged','output','output',$6)")
        .bind(Uuid::new_v4()).bind(&row.tenant_id).bind(request.job_id).bind(&row.request_id).bind(&row.operation).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?;
    sqlx::query("INSERT INTO metering_events (event_id,tenant_id,job_id,reservation_id,request_id,operation,event_type,units,outcome,billing_metric,billing_unit,created_at_ms) VALUES ($1,$2,$3,$4,$5,$6,'operator_image_success',1,'succeeded','output','output',$7)")
        .bind(Uuid::new_v4()).bind(&row.tenant_id).bind(request.job_id).bind(row.reservation_id).bind(&row.request_id).bind(&row.operation).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?;
    Ok(())
}

async fn assert_resolution(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SELECT assert_operator_dreamina_image_success($1)")
        .bind(job_id)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?;
    Ok(())
}

async fn load_result(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<DreaminaImageSuccessResolution, ImageGatewayError> {
    sqlx::query_as(r#"SELECT resolution.resolution_id,resolution.job_id,job.request_id,resolution.submission_id,resolution.executor_execution_id,
        resolution.original_receipt_id,resolution.hold_id,resolution.artifact_id,resolution.artifact_sha256,resolution.artifact_byte_size,
        hold.currency,hold.captured_micros,hold.released_micros,quota.committed_units,quota.released_units
        FROM dreamina_operator_image_success_resolutions resolution JOIN jobs job USING(job_id)
        JOIN customer_billing_holds hold USING(hold_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id WHERE resolution.job_id=$1"#)
        .bind(job_id).fetch_one(&mut **tx).await.map_err(unavailable)
}

fn positive(value: i32) -> Result<u32, ImageGatewayError> {
    u32::try_from(value).map_err(|_| conflict())
}
fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
}
fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Dreamina image success resolution conflicts with reviewed evidence or frozen settlement",
        None,
        "dreamina_image_success_resolution_conflict",
    )
}
fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error=%error,"Dreamina image reconciliation unavailable");
    ImageGatewayError::service_unavailable("Dreamina image reconciliation unavailable")
}
fn storage_conflict(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error=%error,"Dreamina image reconciliation rejected");
    conflict()
}
