//! Operator classification of an already failed, fully released original video.
//! This module never invokes a provider or writes a financial settlement.
use std::path::Path;

use serde::Serialize;
use sqlx::AssertSqlSafe;
use uuid::Uuid;

use super::PostgresExecutionSettlementStore;
use crate::ImageGatewayError;

pub(super) mod evidence;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaManualVideoCancellationRequest {
    pub job_id: Uuid,
    pub request_id: String,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub provider_account_id: Uuid,
    pub upstream_task_id: String,
    pub query_stdout_sha256: String,
    pub operator_reference: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, sqlx::FromRow)]
pub struct DreaminaManualVideoCancellationProof {
    pub schema: String,
    pub authority_id: Uuid,
    pub job_id: Uuid,
    pub request_id: String,
    pub upstream_task_id: String,
    pub submission_id: Uuid,
    pub hold_id: Uuid,
    pub confirmed_at_ms: i64,
    pub currency: String,
    pub held_micros: i64,
    pub captured_micros: i64,
    pub released_micros: i64,
    /// Durable original release or operator cancellation event, not a refund ledger entry.
    pub release_event_id: Uuid,
}

const PROOF: &str = "SELECT 'factory.manual_cancellation.v1'::TEXT AS schema,
 authority.authority_id,authority.job_id,authority.request_id,authority.upstream_task_id,
 authority.submission_id,authority.hold_id,authority.confirmed_at_ms,hold.currency,
 hold.held_micros,hold.captured_micros,hold.released_micros,authority.release_event_id
 FROM dreamina_manual_video_cancellations authority
 JOIN customer_billing_holds hold ON hold.hold_id=authority.hold_id";

impl PostgresExecutionSettlementStore {
    /// Trusted operator-only entry point. Evidence must be in a private local
    /// directory; public clients cannot mint a proof by submitting a checkbox.
    pub async fn confirm_dreamina_manual_video_cancellation(
        &self,
        evidence_root: &Path,
        request: &DreaminaManualVideoCancellationRequest,
    ) -> Result<DreaminaManualVideoCancellationProof, ImageGatewayError> {
        if request.operator_reference.is_empty()
            || request.operator_reference.len() > 255
            || request.operator_reference.chars().any(char::is_control)
        {
            return Err(conflict());
        }
        let verified = evidence::load(
            evidence_root,
            &evidence::ExpectedIdentity {
                job_id: request.job_id,
                request_id: request.request_id.clone(),
                submission_id: request.submission_id,
                executor_execution_id: request.executor_execution_id,
                provider_account_id: request.provider_account_id,
                upstream_task_id: request.upstream_task_id.clone(),
                operator_reference: request.operator_reference.clone(),
                query_stdout_sha256: request.query_stdout_sha256.clone(),
            },
        )?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let exists: Option<Uuid> =
            sqlx::query_scalar("SELECT job_id FROM jobs WHERE job_id=$1 FOR UPDATE")
                .bind(request.job_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        if exists.is_none() {
            return Err(conflict());
        }
        let previous: Option<(Uuid, Uuid, Uuid, String, String, String, String, String, String, String)> = sqlx::query_as(
            "SELECT submission_id,executor_execution_id,provider_account_id,request_id,upstream_task_id,operator_reference,query_stdout_sha256,execution_audit_sha256,review_sha256,website_confirmation_sha256 FROM dreamina_manual_video_cancellations WHERE job_id=$1",
        ).bind(request.job_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if let Some(previous) = previous {
            if previous
                != (
                    request.submission_id,
                    request.executor_execution_id,
                    request.provider_account_id,
                    request.request_id.clone(),
                    request.upstream_task_id.clone(),
                    request.operator_reference.clone(),
                    verified.query_stdout_sha256.clone(),
                    verified.execution_audit_sha256.clone(),
                    verified.review_sha256.clone(),
                    verified.website_confirmation_sha256.clone(),
                )
            {
                return Err(conflict());
            }
        } else {
            let now = super::database_now(&mut tx).await?;
            let inserted = sqlx::query(
                "INSERT INTO dreamina_manual_video_cancellations
                 (authority_id,job_id,request_id,submission_id,executor_execution_id,provider_account_id,upstream_task_id,
                  hold_id,quote_id,original_receipt_id,rated_usage_id,release_event_id,quote_hash,command_hash,execution_binding_sha256,
                  query_stdout_sha256,execution_audit_sha256,review_sha256,website_confirmation_sha256,
                  upstream_identity_sha256,account_home_sha256,operator_reference,queried_at_ms,website_confirmed_at_ms,confirmed_at_ms,original_snapshot)
                 SELECT $1,job.job_id,$3,$4,$5,$6,$7,hold.hold_id,hold.quote_id,receipt.receipt_id,rating.rated_usage_id,event.event_id,
                  quote.quote_hash,submission.command_hash,intent.execution_binding_sha256,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'{}'::JSONB
                 FROM jobs job JOIN provider_submissions submission ON submission.job_id=job.job_id AND submission.submission_id=$4 AND submission.executor_execution_id=$5
                 JOIN provider_remote_tasks remote ON remote.submission_id=submission.submission_id AND remote.remote_operation_id=$7 AND remote.provider_account_id=$6
                 JOIN provider_remote_submit_intents intent ON intent.submission_id=submission.submission_id
                 JOIN executor_terminal_reductions reduction ON reduction.submission_id=submission.submission_id
                 JOIN provider_receipts receipt ON receipt.receipt_id=reduction.provider_receipt_id
                 JOIN customer_billing_holds hold ON hold.job_id=job.job_id
                 JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id
                 JOIN customer_rated_usage rating ON rating.job_id=job.job_id
                 JOIN work_items work ON work.job_id=job.job_id
                 JOIN job_events event ON event.job_id=job.job_id AND event.event_type='job.failed' AND event.semantic_key='work.' || work.work_item_id::TEXT || '.executor-terminal'
                 WHERE job.job_id=$2 AND job.request_id=$3",
            ).bind(Uuid::new_v4()).bind(request.job_id).bind(&request.request_id)
                .bind(request.submission_id).bind(request.executor_execution_id).bind(request.provider_account_id).bind(&request.upstream_task_id)
                .bind(&verified.query_stdout_sha256).bind(&verified.execution_audit_sha256).bind(&verified.review_sha256).bind(&verified.website_confirmation_sha256)
                .bind(&verified.upstream_identity_sha256).bind(&verified.account_home_sha256).bind(&request.operator_reference)
                .bind(verified.queried_at_ms).bind(verified.website_confirmed_at_ms).bind(now)
                .execute(&mut *tx).await.map_err(storage_conflict)?;
            if inserted.rows_affected() != 1 {
                return Err(conflict());
            }
        }
        sqlx::query("SELECT assert_dreamina_manual_cancellation($1)")
            .bind(request.job_id)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?;
        let proof = sqlx::query_as(AssertSqlSafe(format!("{PROOF} WHERE authority.job_id=$1")))
            .bind(request.job_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(storage_conflict)?;
        Ok(proof)
    }

    pub async fn project_dreamina_manual_video_cancellation(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Option<DreaminaManualVideoCancellationProof>, ImageGatewayError> {
        let proof = sqlx::query_as(AssertSqlSafe(format!(
            "{PROOF} JOIN job_auth_attributions attribution ON attribution.job_id=authority.job_id
             WHERE authority.job_id=$1 AND attribution.tenant_id=$2 AND attribution.project_id=$3
              AND ($4::UUID IS NULL OR attribution.actor_user_id=$4)"
        )))
        .bind(job_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(actor_user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        if proof.is_some() {
            return Ok(proof);
        }
        super::supplement_video_cancellation::load_scoped(
            &self.pool,
            tenant_id,
            project_id,
            actor_user_id,
            job_id,
        )
        .await
    }
}

fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Dreamina manual cancellation conflicts with original zero-charge authority",
        None,
        "dreamina_manual_video_cancellation_conflict",
    )
}

fn storage_conflict(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(error = %error, "manual video cancellation evidence conflict");
    conflict()
}

fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(error = %error, "manual video cancellation storage unavailable");
    ImageGatewayError::service_unavailable("Dreamina manual cancellation storage unavailable")
}
