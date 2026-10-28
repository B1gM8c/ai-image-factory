//! Restore the reviewed original MP4 after an empty normal-submit receipt.
//! It never invokes a provider or rewrites historical execution evidence.
use super::*;
use crate::{
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, GenerationResponseProjection, GenerationResultManifest,
    },
    usage::UsageSnapshot,
};
mod evidence;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaVideoArchiveResolutionRequest {
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub reviewed_recovery_sha256: String,
    pub operator_reference: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DreaminaVideoArchiveResolution {
    #[serde(flatten)]
    pub settlement: DreaminaVideoSuccessResolution,
    pub artifact_id: Uuid,
    pub archive_sha256: String,
    pub archive_byte_size: i64,
}

pub async fn resolve_dreamina_video_archive(
    pool: &PgPool,
    blobs: &dyn ArtifactBlobStore,
    evidence_root: &Path,
    request: &DreaminaVideoArchiveResolutionRequest,
) -> Result<DreaminaVideoArchiveResolution, ImageGatewayError> {
    if request.job_id.is_nil()
        || request.submission_id.is_nil()
        || request.executor_execution_id.is_nil()
        || request.submission_id == request.executor_execution_id
        || !valid_sha256(&request.reviewed_recovery_sha256)
        || request.operator_reference.trim().is_empty()
        || request.operator_reference.len() > 255
        || request.operator_reference.chars().any(char::is_control)
    {
        return Err(conflict());
    }
    let evidence = evidence::load(evidence_root, request)?;
    let input_sha256=sha256_hex(&serde_json::to_vec(&json!({
        "evidence_mode":evidence::MODE,"job_id":request.job_id,"submission_id":request.submission_id,
        "executor_execution_id":request.executor_execution_id,"operator_reference":request.operator_reference,
        "review_sha256":request.reviewed_recovery_sha256,"storage_identity":blobs.storage_identity(),
        "query_sha256":evidence.query_sha256,"video_sha256":evidence.video_sha256,
        "provider_command_sha256":evidence.review.provider_command_sha256,
        "quote_hash":evidence.review.quote_hash,"remote_operation_id":evidence.review.remote_operation_id
    })).map_err(|_|conflict())?);
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
        "SELECT input_sha256 FROM dreamina_operator_video_success_resolutions WHERE job_id = $1",
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
        return load_archive_result(pool, result).await;
    }
    let row: LockedJob = sqlx::query_as(
        r#"SELECT job.tenant_id, job.request_id, job.operation, work.work_item_id,
          attempt.execution_id AS attempt_execution_id, attempt.lease_epoch, output.output_id,
          submission.provider_account_id, quota.reservation_id, receipt.receipt_id AS original_receipt_id,
          hold.hold_id, submission.command_hash, payload.command_schema, payload.command_json,
          intent.execution_binding_sha256, quote.quote_hash, quote.request_dimensions_json,
          $4::text AS remote_operation_id, admission.api_profile,
          quota.limit_5h, quota.remaining_5h, quota.limit_7d, quota.remaining_7d
        FROM jobs job
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt ON attempt.work_item_id = work.work_item_id AND attempt.execution_id = work.execution_id AND attempt.lease_epoch = work.lease_epoch
        JOIN job_outputs output ON output.job_id = job.job_id
        JOIN provider_submissions submission ON submission.job_id = job.job_id AND submission.output_id = output.output_id AND submission.work_item_id = work.work_item_id
        JOIN executor_executions execution ON execution.executor_execution_id = submission.executor_execution_id AND execution.submission_id = submission.submission_id
        JOIN provider_remote_submit_intents intent ON intent.submission_id = submission.submission_id AND intent.executor_execution_id = execution.executor_execution_id
        JOIN executor_terminal_reductions reduction ON reduction.submission_id = submission.submission_id AND reduction.executor_execution_id = execution.executor_execution_id
        JOIN provider_receipts receipt ON receipt.receipt_id = reduction.provider_receipt_id AND receipt.submission_id = submission.submission_id AND receipt.output_id = output.output_id AND receipt.job_id = job.job_id
        JOIN executor_capacity_allocations allocation ON allocation.submission_id = submission.submission_id AND allocation.executor_execution_id = execution.executor_execution_id
        JOIN quota_reservations quota ON quota.reservation_id = job.reservation_id AND quota.job_id = job.job_id AND quota.tenant_id = job.tenant_id
        JOIN customer_billing_holds hold ON hold.job_id = job.job_id AND hold.tenant_id = job.tenant_id
        JOIN customer_price_quotes quote ON quote.quote_id = hold.quote_id AND quote.job_id = job.job_id
        JOIN job_payloads payload ON payload.job_id = job.job_id
        JOIN admission_sessions admission ON admission.session_id = payload.admission_session_id
        WHERE job.job_id = $1 AND submission.submission_id = $2 AND execution.executor_execution_id = $3
          AND job.provider_id = 'dreamina-cli' AND job.operation = 'video_generation'
          AND job.output_count = 1 AND job.billable_units = 10 AND job.requested_units = 10
          AND job.economics_contract_version = 4 AND job.state = 'uncertain' AND job.charged_units = 0
          AND job.last_error_code = 'provider_submit_deadline'
          AND intent.state = 'deadline_quarantined' AND intent.remote_operation_id IS NULL
          AND intent.provider_command_sha256 = $5 AND intent.execution_binding_sha256 = $6
          AND work.state = 'uncertain' AND work.lease_owner IS NULL AND work.lease_expires_at_ms IS NULL
          AND attempt.state = 'uncertain' AND output.state = 'uncertain' AND output.output_index = 0 AND output.billable_units = 10
          AND submission.state = 'uncertain' AND execution.state = 'uncertain'
          AND execution.executor_owner IS NULL AND execution.lease_expires_at_ms IS NULL
          AND reduction.state = 'completed' AND reduction.resolved_state = 'uncertain'
          AND reduction.customer_artifact_id IS NULL AND receipt.outcome = 'uncertain'
          AND allocation.state = 'held'
          AND quota.state IN ('reserved','expired') AND quota.committed_units = 0 AND quota.released_units = 0
          AND hold.state = 'held' AND hold.captured_micros = 0 AND hold.released_micros = 0
        FOR UPDATE OF work, attempt, output, submission, execution, intent, reduction, allocation, quota, hold
        "#,
    ).bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id)
        .bind(&evidence.review.remote_operation_id).bind(&evidence.review.provider_command_sha256).bind(&evidence.review.execution_binding_sha256)
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

    let account_home:Option<String>=sqlx::query_scalar("SELECT environment_ref FROM provider_account_environments WHERE provider_account_id=$1 AND provider_id='dreamina-cli' AND environment_kind='dreamina_home_v1' AND state='active' FOR SHARE")
        .bind(row.provider_account_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if account_home
        .is_none_or(|home| sha256_hex(home.as_bytes()) != evidence.review.account_home_sha256)
    {
        return Err(conflict());
    }
    if row.provider_account_id != evidence.review.provider_account_id
        || row.tenant_id != evidence.review.tenant_id
        || row.quote_hash != evidence.review.quote_hash
        || row.command_hash != evidence.review.source_command_sha256
        || row.execution_binding_sha256 != evidence.review.execution_binding_sha256
        || row.command_schema != DREAMINA_SUBMIT_COMMAND_SCHEMA
    {
        return Err(conflict());
    }
    let command =
        parse_submit_command(&serde_json::to_vec(&row.command_json).map_err(|_| conflict())?)
            .map_err(|_| conflict())?;
    if command != evidence.command {
        return Err(conflict());
    }
    let facts = dreamina_pricing_facts(&command, &row.api_profile).map_err(|_| conflict())?;
    let mut dimensions = serde_json::to_value(&facts.dimensions).map_err(|_| conflict())?;
    dimensions.as_object_mut().ok_or_else(conflict)?.insert(
        "processing_mode".into(),
        json!(crate::admission::PricingProcessingMode::Synchronous.as_str()),
    );
    if facts.output_count != 1
        || facts.output_billable_units != 10
        || dimensions != row.request_dimensions_json
    {
        return Err(conflict());
    }
    let now = super::super::database_now(&mut tx).await?;
    let resolution_id = Uuid::new_v4();
    let artifact_id = Uuid::new_v4();
    sqlx::query(r#"INSERT INTO dreamina_operator_video_success_resolutions
        (resolution_id,job_id,submission_id,executor_execution_id,output_id,original_receipt_id,hold_id,
         remote_operation_id,command_hash,execution_binding_sha256,quote_hash,reviewed_stdout_sha256,
         execution_audit_sha256,review_sha256,input_sha256,video_url,video_url_sha256,expires_at_ms,
         operator_reference,created_at_ms,evidence_mode,archive_artifact_id,archive_sha256,archive_byte_size,provider_command_sha256)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NULL,$18,$19,$20,$21,$22,$23,$24)"#)
        .bind(resolution_id).bind(request.job_id).bind(request.submission_id).bind(request.executor_execution_id)
        .bind(row.output_id).bind(row.original_receipt_id).bind(row.hold_id).bind(&evidence.review.remote_operation_id)
        .bind(&row.command_hash).bind(&row.execution_binding_sha256).bind(&row.quote_hash).bind(&evidence.query_sha256)
        .bind(&evidence.audit_sha256).bind(&request.reviewed_recovery_sha256).bind(&input_sha256)
        .bind(&evidence.video_url).bind(&evidence.video_url_sha256).bind(&request.operator_reference).bind(now)
        .bind(evidence::MODE).bind(artifact_id).bind(&evidence.video_sha256).bind(evidence.video.len() as i64)
        .bind(&evidence.review.provider_command_sha256).execute(&mut *tx).await.map_err(storage_conflict)?;
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
                media_type: "video/mp4".into(),
            },
            &evidence.video,
        )
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("artifact storage unavailable"))?;
    if artifact.sha256_hex != evidence.video_sha256
        || artifact.byte_size != evidence.video.len() as u64
        || blobs
            .get(&artifact)
            .await
            .map_err(|_| ImageGatewayError::artifact_integrity())?
            != evidence.video
    {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let positive = |n: i32| u32::try_from(n).map_err(|_| conflict());
    let manifest = GenerationResultManifest {
        job_id: request.job_id,
        tenant_id: row.tenant_id.clone(),
        projection: GenerationResponseProjection {
            api_profile: row.api_profile.clone(),
            operation: row.operation.clone(),
            response_schema: "dreamina-cli.videos.response.v1".into(),
            created_at_seconds: now / 1000,
            output_format: "mp4".into(),
            quality: "auto".into(),
            size: "720p".into(),
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
    super::super::results::persist_generation_result(&mut tx, &manifest, now).await?;
    crate::provider_tasks::release_operator_normal_archive_capacity(
        &mut tx,
        request.submission_id,
        request.executor_execution_id,
        resolution_id,
        &evidence.review.remote_operation_id,
    )
    .await
    .map_err(|_| conflict())?;
    let transition_request = DreaminaVideoSuccessResolutionRequest {
        job_id: request.job_id,
        submission_id: request.submission_id,
        executor_execution_id: request.executor_execution_id,
        reviewed_stdout_sha256: evidence.query_sha256.clone(),
        operator_reference: request.operator_reference.clone(),
    };
    transition(&mut tx, &transition_request, &row, now).await?;
    persist_customer_usage_facts_with_operator_video_resolution(
        &mut tx,
        &CustomerUsageOutput {
            job_id: request.job_id,
            output_id: row.output_id,
            provider_id: "dreamina-cli",
            provider_account_id: Some(row.provider_account_id),
            operation: &row.operation,
            billing_metric: "video_second",
            billing_unit: "second",
            output_billable_units: 10,
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
        "original_receipt_id":row.original_receipt_id,"artifact_id":artifact_id,"archive_sha256":evidence.video_sha256,
        "evidence_mode":evidence::MODE,"delivered_count":1,"committed_units":10,"currency":rating.currency,
        "captured_micros":rating.total_amount_micros,"reviewed_stdout_sha256":evidence.query_sha256});
    for table in ["job_events", "outbox_events"] {
        sqlx::query(AssertSqlSafe(format!("INSERT INTO {table} (event_id,job_id,event_type,semantic_key,payload_json,created_at_ms) VALUES ($1,$2,'job.succeeded','job.operator_dreamina_video_success',$3,$4)")))
            .bind(Uuid::new_v4()).bind(request.job_id).bind(&payload).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
    }
    assert_resolution(&mut tx, request.job_id).await?;
    let result = load_result(&mut tx, request.job_id).await?;
    tx.commit().await.map_err(storage_conflict)?;
    load_archive_result(pool, result).await
}
async fn load_archive_result(
    pool: &PgPool,
    settlement: DreaminaVideoSuccessResolution,
) -> Result<DreaminaVideoArchiveResolution, ImageGatewayError> {
    let (artifact_id,archive_sha256,archive_byte_size):(Uuid,String,i64)=sqlx::query_as("SELECT archive_artifact_id,archive_sha256,archive_byte_size FROM dreamina_operator_video_success_resolutions WHERE job_id=$1 AND evidence_mode='normal_submit_archive_v1'")
        .bind(settlement.job_id).fetch_one(pool).await.map_err(unavailable)?;
    Ok(DreaminaVideoArchiveResolution {
        settlement,
        artifact_id,
        archive_sha256,
        archive_byte_size,
    })
}
