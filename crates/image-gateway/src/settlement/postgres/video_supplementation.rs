//! One-shot operator video attempts, separate from the unknown original execution.
use super::PostgresExecutionSettlementStore;
use crate::{
    ImageGatewayError,
    artifacts::sha256_hex,
    pricing::{
        admission::dreamina_pricing_facts,
        customer_usage::{
            CustomerUsageAuthority, CustomerUsageOutput,
            persist_customer_usage_facts_with_operator_video_supplement,
        },
        postgres_rating::settle_customer_quote,
    },
};
use image_provider_dreamina_cli::{DREAMINA_SUBMIT_COMMAND_SCHEMA, parse_submit_command};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, Postgres, Transaction};
use std::path::Path;
use uuid::Uuid;
mod evidence;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaVideoSupplementAuthorizationRequest {
    pub runtime_binding_json: Value,
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub expected_command_sha256: String,
    pub expected_quote_hash: String,
    pub operator_reference: String,
}
#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct DreaminaVideoSupplementPlan {
    pub runtime_binding_json: Value,
    pub authorization_id: Uuid,
    pub dispatch_nonce: Uuid,
    pub job_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub output_id: Uuid,
    pub provider_account_id: Uuid,
    pub execution_profile_id: Uuid,
    pub command_json: Value,
    pub command_hash: String,
    pub quote_hash: String,
    pub execution_binding_sha256: String,
    pub billable_units: i32,
    pub api_profile: String,
    pub state: String,
    pub remote_operation_id: Option<String>,
    pub submit_stdout_sha256: Option<String>,
}
#[derive(Clone, Debug)]
pub struct DreaminaVideoSupplementAcceptedRequest {
    pub authorization_id: Uuid,
    pub dispatch_nonce: Uuid,
    pub remote_operation_id: String,
    pub submit_stdout_sha256: String,
}
#[derive(Clone, Debug)]
pub struct DreaminaVideoSupplementUncertainRequest {
    pub authorization_id: Uuid,
    pub dispatch_nonce: Uuid,
    pub error_code: String,
}
#[derive(Clone, Debug)]
pub struct DreaminaVideoSupplementPublishRequest {
    pub authorization_id: Uuid,
    pub dispatch_nonce: Uuid,
    pub reviewed_stdout_sha256: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, sqlx::FromRow)]
pub struct DreaminaVideoSupplementResolution {
    pub artifact_id: Uuid,
    pub resolution_id: Uuid,
    pub authorization_id: Uuid,
    pub job_id: Uuid,
    pub request_id: String,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub original_receipt_id: Uuid,
    pub hold_id: Uuid,
    pub remote_operation_id: String,
    pub reviewed_stdout_sha256: String,
    pub quote_hash: String,
    pub video_url_sha256: String,
    pub retrieved_video_sha256: String,
    pub retrieved_video_byte_size: i64,
    pub video_width: i32,
    pub video_height: i32,
    pub video_duration_ms: i64,
    pub currency: String,
    pub captured_micros: i64,
    pub released_micros: i64,
    pub committed_units: i32,
    pub released_units: i32,
}
#[derive(sqlx::FromRow)]
struct Original {
    job_id: Uuid,
    tenant_id: String,
    request_id: String,
    work_item_id: Uuid,
    attempt_execution_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    output_id: Uuid,
    provider_account_id: Uuid,
    execution_profile_id: Uuid,
    original_receipt_id: Uuid,
    hold_id: Uuid,
    reservation_id: Uuid,
    command_hash: String,
    quote_hash: String,
    execution_binding_sha256: String,
    command_json: Value,
    api_profile: String,
    billable_units: i32,
    request_dimensions_json: Value,
    limit_5h: i32,
    remaining_5h: i32,
    limit_7d: i32,
    remaining_7d: i32,
}
const PLAN: &str = "SELECT runtime_binding_json,authorization_id,dispatch_nonce,job_id,submission_id,executor_execution_id,output_id,provider_account_id,execution_profile_id,command_json,command_hash,quote_hash,execution_binding_sha256,billable_units,api_profile,state,remote_operation_id,submit_stdout_sha256 FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=$1";

impl PostgresExecutionSettlementStore {
    pub async fn authorize_dreamina_video_supplement(
        &self,
        request: &DreaminaVideoSupplementAuthorizationRequest,
    ) -> Result<DreaminaVideoSupplementPlan, ImageGatewayError> {
        if request.job_id.is_nil()
            || request.submission_id.is_nil()
            || request.executor_execution_id.is_nil()
            || !valid_sha256(&request.expected_command_sha256)
            || !valid_sha256(&request.expected_quote_hash)
            || request.operator_reference.trim().is_empty()
            || request.operator_reference.len() > 255
            || request.operator_reference.chars().any(char::is_control)
        {
            return Err(conflict());
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SELECT 1 FROM jobs WHERE job_id=$1 FOR UPDATE")
            .bind(request.job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?
            .ok_or_else(conflict)?;
        let existing: Option<(Uuid,Uuid,Uuid,String,String,String,Value)> = sqlx::query_as("SELECT authorization_id,submission_id,executor_execution_id,command_hash,quote_hash,operator_reference,runtime_binding_json FROM dreamina_operator_video_supplement_authorizations WHERE job_id=$1").bind(request.job_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if let Some((id, sub, exec, command, quote, reference, runtime)) = existing {
            if runtime != request.runtime_binding_json {
                return Err(conflict());
            }
            if (sub, exec, command, quote, reference)
                != (
                    request.submission_id,
                    request.executor_execution_id,
                    request.expected_command_sha256.clone(),
                    request.expected_quote_hash.clone(),
                    request.operator_reference.clone(),
                )
            {
                return Err(conflict());
            }
            let plan = load_plan(&mut tx, id).await?;
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(plan);
        }
        let row = load_original(&mut tx, request.job_id).await?;
        if row.submission_id != request.submission_id
            || row.executor_execution_id != request.executor_execution_id
            || row.command_hash != request.expected_command_sha256
            || row.quote_hash != request.expected_quote_hash
        {
            return Err(conflict());
        }
        validate_pricing(&row)?;
        validate_runtime_binding(
            &mut tx,
            &request.runtime_binding_json,
            row.execution_profile_id,
            row.provider_account_id,
        )
        .await?;
        let now = super::database_now(&mut tx).await?;
        let id = Uuid::new_v4();
        sqlx::query(r#"INSERT INTO dreamina_operator_video_supplement_authorizations (authorization_id,job_id,submission_id,executor_execution_id,output_id,original_receipt_id,hold_id,provider_account_id,execution_profile_id,dispatch_nonce,command_hash,quote_hash,execution_binding_sha256,command_json,api_profile,billable_units,operator_reference,original_evidence_json,state,created_at_ms,updated_at_ms,runtime_binding_json) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,dreamina_video_supplement_original_snapshot($2),'authorized',$18,$18,$19)"#)
            .bind(id).bind(row.job_id).bind(row.submission_id).bind(row.executor_execution_id).bind(row.output_id).bind(row.original_receipt_id).bind(row.hold_id).bind(row.provider_account_id).bind(row.execution_profile_id).bind(Uuid::new_v4()).bind(&row.command_hash).bind(&row.quote_hash).bind(&row.execution_binding_sha256).bind(&row.command_json).bind(&row.api_profile).bind(row.billable_units).bind(&request.operator_reference).bind(now).bind(&request.runtime_binding_json).execute(&mut *tx).await.map_err(storage_conflict)?;
        assert_consistent(&mut tx, row.job_id).await?;
        let plan = load_plan(&mut tx, id).await?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(plan)
    }
    pub async fn load_dreamina_video_supplement(
        &self,
        id: Uuid,
    ) -> Result<DreaminaVideoSupplementPlan, ImageGatewayError> {
        sqlx::query_as(PLAN)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(unavailable)?
            .ok_or_else(conflict)
    }
    /// Only the successful authorized->dispatching mutation permits a launch.
    /// A crash after this commit is uncertain and never grants a second permit.
    pub async fn claim_dreamina_video_supplement_dispatch(
        &self,
        id: Uuid,
        nonce: Uuid,
        runtime_binding_json: &Value,
    ) -> Result<Option<DreaminaVideoSupplementPlan>, ImageGatewayError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let plan = lock_plan(&mut tx, id, nonce).await?;
        if plan.runtime_binding_json != *runtime_binding_json {
            return Err(conflict());
        }
        if plan.state != "authorized" {
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(None);
        }
        validate_runtime_binding(
            &mut tx,
            runtime_binding_json,
            plan.execution_profile_id,
            plan.provider_account_id,
        )
        .await?;
        let now = super::database_now(&mut tx).await?;
        sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET state='dispatching',dispatched_at_ms=$2,updated_at_ms=$2 WHERE authorization_id=$1 AND state='authorized'").bind(id).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
        let result = load_plan(&mut tx, id).await?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(Some(result))
    }
    pub async fn record_dreamina_video_supplement_accepted(
        &self,
        request: &DreaminaVideoSupplementAcceptedRequest,
    ) -> Result<(), ImageGatewayError> {
        if !valid_sha256(&request.submit_stdout_sha256)
            || !valid_remote_id(&request.remote_operation_id)
        {
            return Err(conflict());
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let plan = lock_plan(&mut tx, request.authorization_id, request.dispatch_nonce).await?;
        if let Some(remote) = plan.remote_operation_id {
            if remote != request.remote_operation_id
                || plan.submit_stdout_sha256.as_deref()
                    != Some(request.submit_stdout_sha256.as_str())
            {
                return Err(conflict());
            }
            tx.commit().await.map_err(storage_conflict)?;
            return Ok(());
        }
        if !matches!(plan.state.as_str(), "dispatching" | "uncertain") {
            return Err(conflict());
        }
        let now = super::database_now(&mut tx).await?;
        sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET state='accepted',remote_operation_id=$2,submit_stdout_sha256=$3,error_code=NULL,updated_at_ms=$4 WHERE authorization_id=$1").bind(request.authorization_id).bind(&request.remote_operation_id).bind(&request.submit_stdout_sha256).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
        tx.commit().await.map_err(storage_conflict)
    }
    pub async fn record_dreamina_video_supplement_uncertain(
        &self,
        request: &DreaminaVideoSupplementUncertainRequest,
    ) -> Result<(), ImageGatewayError> {
        if request.error_code.is_empty()
            || request.error_code.len() > 128
            || !request
                .error_code
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        {
            return Err(conflict());
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let plan = lock_plan(&mut tx, request.authorization_id, request.dispatch_nonce).await?;
        if !matches!(
            plan.state.as_str(),
            "dispatching" | "accepted" | "uncertain"
        ) {
            return Err(conflict());
        }
        let now = super::database_now(&mut tx).await?;
        sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET state='uncertain',error_code=$2,updated_at_ms=$3 WHERE authorization_id=$1").bind(request.authorization_id).bind(&request.error_code).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
        tx.commit().await.map_err(storage_conflict)
    }
    pub async fn publish_dreamina_video_supplement(
        &self,
        evidence_root: &Path,
        request: &DreaminaVideoSupplementPublishRequest,
    ) -> Result<DreaminaVideoSupplementResolution, ImageGatewayError> {
        publish(self, evidence_root, request).await
    }
}
async fn load_plan(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<DreaminaVideoSupplementPlan, ImageGatewayError> {
    sqlx::query_as(PLAN)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unavailable)?
        .ok_or_else(conflict)
}
async fn lock_plan(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    nonce: Uuid,
) -> Result<DreaminaVideoSupplementPlan, ImageGatewayError> {
    // Parent first matches callback/supplement_auth lock order.
    let job:Uuid=sqlx::query_scalar("SELECT job_id FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=$1").bind(id).fetch_optional(&mut **tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
    sqlx::query("SELECT 1 FROM jobs WHERE job_id=$1 FOR UPDATE")
        .bind(job)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SELECT 1 FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=$1 FOR UPDATE").bind(id).execute(&mut **tx).await.map_err(unavailable)?;
    let plan = load_plan(tx, id).await?;
    if plan.dispatch_nonce != nonce {
        return Err(conflict());
    }
    Ok(plan)
}
async fn load_original(
    tx: &mut Transaction<'_, Postgres>,
    job: Uuid,
) -> Result<Original, ImageGatewayError> {
    sqlx::query_as(r#"SELECT job.job_id,job.tenant_id,job.request_id,work.work_item_id,attempt.execution_id AS attempt_execution_id,submission.submission_id,submission.executor_execution_id,output.output_id,submission.provider_account_id,submission.execution_profile_id,receipt.receipt_id AS original_receipt_id,hold.hold_id,quota.reservation_id,submission.command_hash,quote.quote_hash,intent.execution_binding_sha256,payload.command_json,admission.api_profile,job.billable_units,quote.request_dimensions_json,quota.limit_5h,quota.remaining_5h,quota.limit_7d,quota.remaining_7d
    FROM jobs job JOIN work_items work ON work.job_id=job.job_id JOIN job_attempts attempt ON attempt.execution_id=work.execution_id AND attempt.work_item_id=work.work_item_id AND attempt.lease_epoch=work.lease_epoch JOIN job_outputs output ON output.job_id=job.job_id AND output.output_index=0 JOIN provider_submissions submission ON submission.job_id=job.job_id AND submission.output_id=output.output_id AND submission.work_item_id=work.work_item_id JOIN executor_executions execution ON execution.submission_id=submission.submission_id AND execution.executor_execution_id=submission.executor_execution_id JOIN executor_terminal_reductions reduction ON reduction.submission_id=submission.submission_id AND reduction.executor_execution_id=submission.executor_execution_id JOIN provider_receipts receipt ON receipt.receipt_id=reduction.provider_receipt_id JOIN provider_remote_submit_intents intent ON intent.submission_id=submission.submission_id AND intent.executor_execution_id=submission.executor_execution_id JOIN provider_submit_recoveries recovery ON recovery.submission_id=submission.submission_id AND recovery.executor_execution_id=submission.executor_execution_id JOIN customer_billing_holds hold ON hold.job_id=job.job_id JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id JOIN job_payloads payload ON payload.job_id=job.job_id JOIN admission_sessions admission ON admission.session_id=payload.admission_session_id
    WHERE job.job_id=$1 AND payload.command_schema=$2 AND recovery.recovery_owner IS NULL AND recovery.recovery_lease_expires_at_ms IS NULL
    FOR UPDATE OF work,attempt,output,submission,execution,reduction,receipt,intent,recovery,hold,quota"#).bind(job).bind(DREAMINA_SUBMIT_COMMAND_SCHEMA).fetch_optional(&mut **tx).await.map_err(unavailable)?.ok_or_else(conflict)
}
fn validate_pricing(row: &Original) -> Result<(), ImageGatewayError> {
    let command =
        parse_submit_command(&serde_json::to_vec(&row.command_json).map_err(|_| conflict())?)
            .map_err(|_| conflict())?;
    let facts = dreamina_pricing_facts(&command, &row.api_profile).map_err(|_| conflict())?;
    let mut dimensions = serde_json::to_value(&facts.dimensions).map_err(|_| conflict())?;
    dimensions
        .as_object_mut()
        .ok_or_else(conflict)?
        .insert("processing_mode".into(), json!("synchronous"));
    if facts.media_kind != "video"
        || facts.output_count != 1
        || i64::from(facts.output_billable_units) != i64::from(row.billable_units)
        || dimensions != row.request_dimensions_json
    {
        return Err(conflict());
    }
    // The authorized models use second-based pricing; multiframe has a distinct
    // millisecond contract and is intentionally outside this operator workflow.
    if !matches!(
        facts.provider_model_id.as_str(),
        "seedance2.0mini" | "seedance2.0fast_vip"
    ) {
        return Err(conflict());
    }
    Ok(())
}
fn validate_video_metadata(
    command: &Value,
    evidence: &evidence::VerifiedEvidence,
) -> Result<(), ImageGatewayError> {
    let requested_duration_ms = command
        .get("duration")
        .and_then(Value::as_u64)
        .and_then(|seconds| seconds.checked_mul(1_000))
        .ok_or_else(conflict)?;
    let resolution = match command.get("video_resolution").and_then(Value::as_str) {
        Some("720p") => 720,
        Some("1080p") => 1080,
        _ => return Err(conflict()),
    };
    let (ratio_width, ratio_height) = command
        .get("ratio")
        .and_then(Value::as_str)
        .and_then(|ratio| ratio.split_once(':'))
        .and_then(|(width, height)| Some((width.parse::<u64>().ok()?, height.parse::<u64>().ok()?)))
        .filter(|(width, height)| *width > 0 && *height > 0)
        .ok_or_else(conflict)?;
    // Allow at most 100ms of frame/audio padding. Billing and public duration
    // continue to use the original frozen requested duration.
    if evidence.video_duration_ms.abs_diff(requested_duration_ms) > 100
        || evidence.video_width.min(evidence.video_height) != resolution
        || u64::from(evidence.video_width) * ratio_height
            != u64::from(evidence.video_height) * ratio_width
    {
        return Err(conflict());
    }
    Ok(())
}
async fn publish(
    store: &PostgresExecutionSettlementStore,
    root: &Path,
    request: &DreaminaVideoSupplementPublishRequest,
) -> Result<DreaminaVideoSupplementResolution, ImageGatewayError> {
    if !valid_sha256(&request.reviewed_stdout_sha256) {
        return Err(conflict());
    }
    let pool = &store.pool;
    let verified = evidence::load(root, request)?;
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let (tenant,currency):(String,String)=sqlx::query_as("SELECT job.tenant_id,hold.currency FROM jobs job JOIN customer_billing_holds hold USING(job_id) JOIN dreamina_operator_video_supplement_authorizations supplement_auth USING(job_id) WHERE supplement_auth.authorization_id=$1").bind(request.authorization_id).fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::usage::quota_lock_id(&tenant))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("budget:{tenant}:{currency}"))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SELECT 1 FROM billing_accounts WHERE tenant_id=$1 AND currency=$2 FOR UPDATE")
        .bind(&tenant)
        .bind(&currency)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let plan = lock_plan(&mut tx, request.authorization_id, request.dispatch_nonce).await?;
    if plan.job_id != verified.job_id
        || plan.submission_id != verified.submission_id
        || plan.executor_execution_id != verified.executor_execution_id
        || plan.command_hash != verified.provider_command_sha256
        || plan.quote_hash != verified.quote_hash
        || plan.remote_operation_id.as_deref() != Some(verified.remote_operation_id.as_str())
        || plan.submit_stdout_sha256.as_deref() != Some(verified.submit_stdout_sha256.as_str())
    {
        return Err(conflict());
    }
    let input_sha256=sha256_hex(&serde_json::to_vec(&json!({"authorization_id":request.authorization_id,"dispatch_nonce":request.dispatch_nonce,"stdout_sha256":verified.stdout_sha256,"audit_sha256":verified.execution_audit_sha256,"review_sha256":verified.review_sha256,"retrieved_video_sha256":verified.retrieved_video_sha256})).map_err(|_|conflict())?);
    let existing:Option<String>=sqlx::query_scalar("SELECT input_sha256 FROM dreamina_operator_video_supplement_resolutions WHERE authorization_id=$1").bind(request.authorization_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if let Some(existing) = existing {
        if existing != input_sha256 {
            return Err(conflict());
        }
        assert_consistent(&mut tx, plan.job_id).await?;
        let result = load_resolution(&mut tx, request.authorization_id).await?;
        tx.commit().await.map_err(storage_conflict)?;
        return Ok(result);
    }
    if !matches!(plan.state.as_str(), "accepted" | "uncertain") {
        return Err(conflict());
    }
    let row = load_original(&mut tx, plan.job_id).await?;
    validate_pricing(&row)?;
    validate_video_metadata(&row.command_json, &verified)?;
    assert_consistent(&mut tx, plan.job_id).await?;
    let now = super::database_now(&mut tx).await?;
    let resolution_id = Uuid::new_v4();
    let artifact = store
        .artifact_store
        .put(
            crate::artifacts::ArtifactIdentity {
                artifact_id: plan.dispatch_nonce,
                tenant_id: row.tenant_id.clone(),
                job_id: row.job_id,
                work_item_id: row.work_item_id,
                execution_id: plan.authorization_id,
                lease_epoch: 1,
                output_index: 0,
                media_type: "video/mp4".into(),
            },
            &verified.video_bytes,
        )
        .await
        .map_err(|_| {
            ImageGatewayError::service_unavailable("supplement artifact storage unavailable")
        })?;
    if artifact.sha256_hex != verified.retrieved_video_sha256
        || artifact.byte_size != verified.retrieved_video_byte_size as u64
    {
        return Err(conflict());
    }
    sqlx::query(r#"INSERT INTO dreamina_operator_video_supplement_resolutions (resolution_id,authorization_id,job_id,submission_id,executor_execution_id,output_id,original_receipt_id,hold_id,remote_operation_id,command_hash,quote_hash,execution_binding_sha256,billable_units,reviewed_stdout_sha256,submit_stdout_sha256,execution_audit_sha256,review_sha256,input_sha256,video_url,video_url_sha256,retrieved_video_sha256,retrieved_video_byte_size,expires_at_ms,operator_reference,created_at_ms,artifact_id,storage_backend,storage_namespace,object_key,video_width,video_height,video_duration_ms) SELECT $1,authorization_id,job_id,submission_id,executor_execution_id,output_id,original_receipt_id,hold_id,remote_operation_id,command_hash,quote_hash,execution_binding_sha256,billable_units,$3,submit_stdout_sha256,$4,$5,$6,$7,$8,$9,$10,NULL,operator_reference,$11,$12,$13,$14,$15,$16,$17,$18 FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=$2"#).bind(resolution_id).bind(request.authorization_id).bind(&verified.stdout_sha256).bind(&verified.execution_audit_sha256).bind(&verified.review_sha256).bind(&input_sha256).bind(verified.result.video_url()).bind(&verified.video_url_sha256).bind(&verified.retrieved_video_sha256).bind(verified.retrieved_video_byte_size).bind(now).bind(artifact.identity.artifact_id).bind(&artifact.storage_backend).bind(store.artifact_store.storage_identity()).bind(&artifact.object_key).bind(i32::try_from(verified.video_width).map_err(|_| conflict())?).bind(i32::try_from(verified.video_height).map_err(|_| conflict())?).bind(i64::try_from(verified.video_duration_ms).map_err(|_| conflict())?).execute(&mut *tx).await.map_err(storage_conflict)?;
    let size = row
        .command_json
        .get("video_resolution")
        .and_then(Value::as_str)
        .ok_or_else(conflict)?;
    sqlx::query("INSERT INTO job_response_projections (job_id,api_profile,operation,response_schema,created_at_seconds,output_format,quality,size,background,stream,limit_5h,remaining_5h,limit_7d,remaining_7d,artifact_count,remote_result_count,created_at_ms) VALUES ($1,$2,'video_generation','dreamina-cli.videos.response.v1',$3,'mp4','auto',$4,'opaque',FALSE,$5,$6,$7,$8,1,0,$9)").bind(row.job_id).bind(&row.api_profile).bind(now/1000).bind(size).bind(row.limit_5h).bind(row.remaining_5h).bind(row.limit_7d).bind(row.remaining_7d).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
    transition(&mut tx, &row, now).await?;
    persist_customer_usage_facts_with_operator_video_supplement(
        &mut tx,
        &CustomerUsageOutput {
            job_id: row.job_id,
            output_id: row.output_id,
            provider_id: "dreamina-cli",
            provider_account_id: Some(row.provider_account_id),
            operation: "video_generation",
            billing_metric: "video_second",
            billing_unit: "second",
            output_billable_units: row.billable_units,
            terminal_outcome: "succeeded",
        },
        CustomerUsageAuthority::Durable {
            submission_id: row.submission_id,
            receipt_id: row.original_receipt_id,
        },
        resolution_id,
        now,
    )
    .await
    .map_err(|_| conflict())?;
    let rating = settle_customer_quote(&mut tx, row.job_id, &row.tenant_id)
        .await
        .map_err(|_| conflict())?;
    let payload = json!({"resolution_id":resolution_id,"authorization_id":request.authorization_id,"submission_id":row.submission_id,"executor_execution_id":row.executor_execution_id,"original_receipt_id":row.original_receipt_id,"remote_operation_id":verified.remote_operation_id,"original_effect_certainty":"unknown_remote_effect","video_url_sha256":verified.video_url_sha256,"retrieved_video_sha256":verified.retrieved_video_sha256,"artifact_id":artifact.identity.artifact_id,"video_width":verified.video_width,"video_height":verified.video_height,"video_duration_ms":verified.video_duration_ms,"delivered_count":1,"committed_units":row.billable_units,"currency":rating.currency,"captured_micros":rating.total_amount_micros,"reviewed_stdout_sha256":verified.stdout_sha256});
    for table in ["job_events", "outbox_events"] {
        sqlx::query(AssertSqlSafe(format!("INSERT INTO {table} (event_id,job_id,event_type,semantic_key,payload_json,created_at_ms) VALUES ($1,$2,'job.succeeded','job.operator_dreamina_video_supplement_success',$3,$4)"))).bind(Uuid::new_v4()).bind(row.job_id).bind(&payload).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
    }
    sqlx::query("UPDATE dreamina_operator_video_supplement_authorizations SET state='succeeded',error_code=NULL,updated_at_ms=$2 WHERE authorization_id=$1").bind(request.authorization_id).bind(now).execute(&mut *tx).await.map_err(storage_conflict)?;
    assert_consistent(&mut tx, row.job_id).await?;
    let result = load_resolution(&mut tx, request.authorization_id).await?;
    tx.commit().await.map_err(storage_conflict)?;
    Ok(result)
}
async fn transition(
    tx: &mut Transaction<'_, Postgres>,
    row: &Original,
    now: i64,
) -> Result<(), ImageGatewayError> {
    for (query, id) in [
        (
            "UPDATE jobs SET state='succeeded',charged_units=$3,last_error_code=NULL,updated_at_ms=$2 WHERE job_id=$1 AND state='uncertain' AND charged_units=0",
            row.job_id,
        ),
        (
            "UPDATE work_items SET state='succeeded',updated_at_ms=$2 WHERE work_item_id=$1 AND state='uncertain' AND $3>0",
            row.work_item_id,
        ),
        (
            "UPDATE job_attempts SET state='succeeded',error_code=NULL,updated_at_ms=$2 WHERE execution_id=$1 AND state='uncertain' AND $3>0",
            row.attempt_execution_id,
        ),
        (
            "UPDATE job_outputs SET state='succeeded',error_code=NULL,finished_at_ms=$2,updated_at_ms=$2 WHERE output_id=$1 AND state='uncertain' AND $3>0",
            row.output_id,
        ),
        (
            "UPDATE quota_reservations SET state='committed',committed_units=$3,updated_at_ms=$2 WHERE reservation_id=$1 AND state IN ('reserved','expired') AND committed_units=0 AND released_units=0",
            row.reservation_id,
        ),
    ] {
        if sqlx::query(query)
            .bind(id)
            .bind(now)
            .bind(row.billable_units)
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
            .bind(row.job_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(unavailable)?;
    let affected=sqlx::query("UPDATE idempotency_requests SET state='succeeded',terminal_outcome='succeeded',updated_at_ms=$2 WHERE job_id=$1 AND state='uncertain' AND terminal_outcome='uncertain'").bind(row.job_id).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?.rows_affected();
    if expected == 0 || affected != expected as u64 {
        return Err(conflict());
    }
    sqlx::query("INSERT INTO usage_events (event_id,tenant_id,job_id,request_id,operation,units,outcome,billing_metric,billing_unit,created_at_ms) VALUES ($1,$2,$3,$4,'video_generation',$5,'charged','video_second','second',$6)").bind(Uuid::new_v4()).bind(&row.tenant_id).bind(row.job_id).bind(&row.request_id).bind(row.billable_units).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?;
    sqlx::query("INSERT INTO metering_events (event_id,tenant_id,job_id,reservation_id,request_id,operation,event_type,units,outcome,billing_metric,billing_unit,created_at_ms) VALUES ($1,$2,$3,$4,$5,'video_generation','operator_video_supplement_success',$6,'succeeded','video_second','second',$7)").bind(Uuid::new_v4()).bind(&row.tenant_id).bind(row.job_id).bind(row.reservation_id).bind(&row.request_id).bind(row.billable_units).bind(now).execute(&mut **tx).await.map_err(storage_conflict)?;
    Ok(())
}
async fn assert_consistent(
    tx: &mut Transaction<'_, Postgres>,
    job: Uuid,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SELECT assert_operator_dreamina_video_supplement($1)")
        .bind(job)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?;
    Ok(())
}
async fn load_resolution(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<DreaminaVideoSupplementResolution, ImageGatewayError> {
    sqlx::query_as("SELECT resolution.artifact_id,resolution.resolution_id,resolution.authorization_id,resolution.job_id,job.request_id,resolution.submission_id,resolution.executor_execution_id,resolution.original_receipt_id,resolution.hold_id,resolution.remote_operation_id,resolution.reviewed_stdout_sha256,resolution.quote_hash,resolution.video_url_sha256,resolution.retrieved_video_sha256,resolution.retrieved_video_byte_size,resolution.video_width,resolution.video_height,resolution.video_duration_ms,hold.currency,hold.captured_micros,hold.released_micros,quota.committed_units,quota.released_units FROM dreamina_operator_video_supplement_resolutions resolution JOIN jobs job USING(job_id) JOIN customer_billing_holds hold USING(hold_id) JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id WHERE resolution.authorization_id=$1").bind(id).fetch_one(&mut **tx).await.map_err(unavailable)
}
fn valid_sha256(v: &str) -> bool {
    v.len() == 64
        && v.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn valid_remote_id(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 255
        && !v.contains("://")
        && v.as_bytes()[0].is_ascii_alphanumeric()
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:@/-".contains(&b))
}
fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Dreamina video supplement conflicts with frozen authority or verified evidence",
        None,
        "dreamina_video_supplement_conflict",
    )
}
fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error=%error,"video supplement unavailable");
    ImageGatewayError::service_unavailable("Dreamina video supplement unavailable")
}
fn storage_conflict(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error=%error,"video supplement rejected");
    conflict()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeBinding {
    execution_profile_id: Uuid,
    provider_account_id: Uuid,
    upstream_identity_sha256: String,
    credential_revision: i64,
    credential_ref_sha256: String,
    credential_auth_sha256: String,
    account_home_sha256: String,
    cli_path: String,
    cli_sha256: String,
    runner_path: String,
    runner_sha256: String,
}
async fn validate_runtime_binding(
    tx: &mut Transaction<'_, Postgres>,
    value: &Value,
    profile: Uuid,
    account: Uuid,
) -> Result<(), ImageGatewayError> {
    let binding: RuntimeBinding = serde_json::from_value(value.clone()).map_err(|_| conflict())?;
    if binding.execution_profile_id != profile
        || binding.provider_account_id != account
        || binding.credential_revision <= 0
        || [
            &binding.upstream_identity_sha256,
            &binding.credential_ref_sha256,
            &binding.credential_auth_sha256,
            &binding.account_home_sha256,
            &binding.cli_sha256,
            &binding.runner_sha256,
        ]
        .iter()
        .any(|v| !valid_sha256(v))
        || [&binding.cli_path, &binding.runner_path].iter().any(|v| {
            !Path::new(v).is_absolute()
                || v.chars().any(char::is_control)
                || Path::new(v).components().any(|v| {
                    matches!(
                        v,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    )
                })
        })
    {
        return Err(conflict());
    }
    let exists:Option<Uuid>=sqlx::query_scalar(r#"SELECT profile.execution_profile_id FROM provider_execution_profiles profile JOIN provider_accounts account USING(provider_account_id) JOIN provider_account_credential_heads head USING(provider_account_id) JOIN provider_account_environments environment USING(provider_account_id)
 WHERE profile.execution_profile_id=$1 AND account.provider_account_id=$2 AND profile.state='enabled' AND profile.provider_id='dreamina-cli' AND profile.operation_id='videos.generations'
 AND account.state='enabled' AND account.credential_revision=$3 AND profile.credential_revision=account.credential_revision AND profile.credential_ref=account.credential_ref
 AND encode(sha256(convert_to(account.credential_ref,'UTF8')),'hex')=$4 AND account.credential_auth_sha256=$5 AND head.active_revision=account.credential_revision AND head.lifecycle_state IN ('active','refresh_due')
 AND environment.state='active' AND environment.environment_kind='dreamina_home_v1' AND environment.upstream_identity_sha256=$6 AND encode(sha256(convert_to(environment.environment_ref,'UTF8')),'hex')=$7
 FOR SHARE OF profile,account,head,environment"#).bind(profile).bind(account).bind(binding.credential_revision).bind(&binding.credential_ref_sha256).bind(&binding.credential_auth_sha256).bind(&binding.upstream_identity_sha256).bind(&binding.account_home_sha256).fetch_optional(&mut **tx).await.map_err(unavailable)?;
    if exists.is_none() {
        return Err(conflict());
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct SupplementArtifact {
    artifact_id: Uuid,
    authorization_id: Uuid,
    tenant_id: String,
    job_id: Uuid,
    work_item_id: Uuid,
    storage_backend: String,
    storage_namespace: String,
    object_key: String,
    retrieved_video_sha256: String,
    retrieved_video_byte_size: i64,
}
pub(super) async fn load_artifact(
    store: &PostgresExecutionSettlementStore,
    tenant: &str,
    project: Option<&str>,
    actor: Option<Uuid>,
    artifact_id: Uuid,
) -> Result<Option<super::StoredVideoArtifact>, ImageGatewayError> {
    let row:Option<SupplementArtifact>=sqlx::query_as(r#"SELECT result.artifact_id,result.authorization_id,job.tenant_id,job.job_id,work.work_item_id,result.storage_backend,result.storage_namespace,result.object_key,result.retrieved_video_sha256,result.retrieved_video_byte_size
    FROM dreamina_operator_video_supplement_resolutions result JOIN jobs job USING(job_id)
    JOIN dreamina_operator_video_supplement_authorizations supplement_auth USING(authorization_id,job_id)
    JOIN work_items work ON work.job_id=job.job_id JOIN job_auth_attributions attribution ON attribution.job_id=job.job_id AND attribution.tenant_id=job.tenant_id
    WHERE result.artifact_id=$1 AND job.tenant_id=$2 AND job.state='succeeded' AND supplement_auth.state='succeeded'
      AND ($3::TEXT IS NULL OR attribution.project_id=$3) AND ($4::UUID IS NULL OR attribution.actor_user_id=$4)
      AND job.provider_id='dreamina-cli' AND job.operation='video_generation'"#).bind(artifact_id).bind(tenant).bind(project).bind(actor).fetch_optional(&store.pool).await.map_err(unavailable)?;
    let Some(row) = row else { return Ok(None) };
    if row.storage_namespace != store.artifact_store.storage_identity() {
        return Err(ImageGatewayError::artifact_integrity());
    }
    let artifact = crate::artifacts::ArtifactMetadata {
        identity: crate::artifacts::ArtifactIdentity {
            artifact_id: row.artifact_id,
            tenant_id: row.tenant_id,
            job_id: row.job_id,
            work_item_id: row.work_item_id,
            execution_id: row.authorization_id,
            lease_epoch: 1,
            output_index: 0,
            media_type: "video/mp4".into(),
        },
        storage_backend: row.storage_backend,
        object_key: row.object_key,
        sha256_hex: row.retrieved_video_sha256,
        byte_size: u64::try_from(row.retrieved_video_byte_size)
            .map_err(|_| ImageGatewayError::artifact_integrity())?,
    };
    let bytes = store
        .artifact_store
        .get(&artifact)
        .await
        .map_err(|error| match error {
            crate::artifacts::ArtifactReadError::Integrity => {
                ImageGatewayError::artifact_integrity()
            }
            crate::artifacts::ArtifactReadError::Unavailable => {
                ImageGatewayError::service_unavailable("supplement video artifact unavailable")
            }
        })?;
    if bytes.len() as u64 != artifact.byte_size || sha256_hex(&bytes) != artifact.sha256_hex {
        return Err(ImageGatewayError::artifact_integrity());
    }
    Ok(Some(super::StoredVideoArtifact {
        media_type: "video/mp4".into(),
        bytes,
    }))
}
