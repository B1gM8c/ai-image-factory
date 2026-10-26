//! A trusted operator customer-charge waiver for a cancelled accepted supplement
//! head. Provider receipts and unknown ancestor effects remain unchanged.
use super::{
    PostgresExecutionSettlementStore,
    manual_video_cancellation::DreaminaManualVideoCancellationProof,
};
use crate::{
    ImageGatewayError,
    pricing::{
        customer_usage::{
            CustomerUsageAuthority, CustomerUsageOutput,
            persist_customer_usage_facts_with_operator_supplement_cancellation,
        },
        postgres_rating::settle_customer_quote,
    },
};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use std::path::Path;
use uuid::Uuid;
mod evidence;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaSupplementVideoCancellationRequest {
    pub authorization_id: Uuid,
    pub dispatch_nonce: Uuid,
    pub reviewed_stdout_sha256: String,
    pub operator_reference: String,
}
#[derive(sqlx::FromRow)]
struct Head {
    job_id: Uuid,
    request_id: String,
    tenant_id: String,
    currency: String,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_account_id: Uuid,
    output_id: Uuid,
    original_receipt_id: Uuid,
    hold_id: Uuid,
    quote_id: Uuid,
    reservation_id: Uuid,
    billable_units: i32,
    remote_operation_id: String,
    quote_hash: String,
    command_hash: String,
    submit_stdout_sha256: String,
}
const PROOF: &str = "SELECT 'factory.manual_cancellation.v1'::TEXT AS schema, cancellation.cancellation_id AS authority_id,cancellation.job_id,cancellation.request_id,cancellation.upstream_task_id,cancellation.submission_id,cancellation.hold_id,cancellation.confirmed_at_ms,hold.currency,hold.held_micros,hold.captured_micros,hold.released_micros,cancellation.release_event_id FROM dreamina_operator_video_supplement_cancellations cancellation JOIN customer_billing_holds hold ON hold.hold_id=cancellation.hold_id WHERE cancellation.finalized_snapshot IS NOT NULL";

impl PostgresExecutionSettlementStore {
    /// A finalized cancellation stops operator queries without rewriting provider state.
    pub(crate) async fn is_dreamina_video_supplement_cancelled(
        &self,
        authorization_id: Uuid,
        dispatch_nonce: Uuid,
    ) -> Result<bool, ImageGatewayError> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_cancellations cancellation
             JOIN dreamina_operator_video_supplement_authorizations head USING(authorization_id)
             WHERE cancellation.authorization_id=$1 AND cancellation.dispatch_nonce=$2
              AND cancellation.finalized_snapshot IS NOT NULL
              AND head.dispatch_nonce=cancellation.dispatch_nonce AND head.job_id=cancellation.job_id
              AND head.remote_operation_id=cancellation.upstream_task_id
              AND NOT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations child
               WHERE child.previous_authorization_id=head.authorization_id))",
        )
        .bind(authorization_id)
        .bind(dispatch_nonce)
        .fetch_one(&self.pool)
        .await
        .map_err(unavailable)
    }

    pub async fn confirm_dreamina_supplement_video_cancellation(
        &self,
        root: &Path,
        request: &DreaminaSupplementVideoCancellationRequest,
    ) -> Result<DreaminaManualVideoCancellationProof, ImageGatewayError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let (tenant,currency):(String,String)=sqlx::query_as("SELECT job.tenant_id,hold.currency FROM dreamina_operator_video_supplement_authorizations head JOIN jobs job USING(job_id) JOIN customer_billing_holds hold ON hold.hold_id=head.hold_id WHERE head.authorization_id=$1 AND head.dispatch_nonce=$2").bind(request.authorization_id).bind(request.dispatch_nonce).fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
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
        sqlx::query("SELECT 1 FROM jobs WHERE job_id=(SELECT job_id FROM dreamina_operator_video_supplement_authorizations WHERE authorization_id=$1) FOR UPDATE").bind(request.authorization_id).execute(&mut *tx).await.map_err(unavailable)?;
        let head:Head=sqlx::query_as("SELECT head.job_id,job.request_id,job.tenant_id,hold.currency,head.submission_id,head.executor_execution_id,head.provider_account_id,head.output_id,head.original_receipt_id,head.hold_id,hold.quote_id,job.reservation_id,head.billable_units,head.remote_operation_id,head.quote_hash,head.command_hash,head.submit_stdout_sha256 FROM dreamina_operator_video_supplement_authorizations head JOIN jobs job USING(job_id) JOIN customer_billing_holds hold ON hold.hold_id=head.hold_id WHERE head.authorization_id=$1 AND head.dispatch_nonce=$2 AND head.remote_operation_id IS NOT NULL AND head.submit_stdout_sha256 IS NOT NULL AND NOT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations child WHERE child.previous_authorization_id=head.authorization_id) FOR UPDATE OF head,hold").bind(request.authorization_id).bind(request.dispatch_nonce).fetch_optional(&mut *tx).await.map_err(unavailable)?.ok_or_else(conflict)?;
        let verified = evidence::load(
            root,
            &evidence::ExpectedIdentity {
                base: super::manual_video_cancellation::evidence::ExpectedIdentity {
                    job_id: head.job_id,
                    request_id: head.request_id.clone(),
                    submission_id: head.submission_id,
                    executor_execution_id: head.executor_execution_id,
                    provider_account_id: head.provider_account_id,
                    upstream_task_id: head.remote_operation_id.clone(),
                    operator_reference: request.operator_reference.clone(),
                    query_stdout_sha256: request.reviewed_stdout_sha256.clone(),
                },
                authorization_id: request.authorization_id,
                dispatch_nonce: request.dispatch_nonce,
                quote_hash: head.quote_hash.clone(),
                command_hash: head.command_hash.clone(),
                submit_stdout_sha256: head.submit_stdout_sha256.clone(),
            },
        )?;
        let existing:Option<(String,String,String,String,String)>=sqlx::query_as("SELECT query_stdout_sha256,execution_audit_sha256,review_sha256,head_review_sha256,operator_reference FROM dreamina_operator_video_supplement_cancellations WHERE authorization_id=$1").bind(request.authorization_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if let Some(existing) = existing {
            if existing
                != (
                    verified.base.query_stdout_sha256,
                    verified.base.execution_audit_sha256,
                    verified.base.review_sha256,
                    verified.head_review_sha256,
                    request.operator_reference.clone(),
                )
            {
                return Err(conflict());
            }
        } else {
            sqlx::query("SELECT assert_operator_dreamina_video_supplement($1)")
                .bind(head.job_id)
                .execute(&mut *tx)
                .await
                .map_err(rejected)?;
            let now = super::database_now(&mut tx).await?;
            let id = Uuid::new_v4();
            let event = Uuid::new_v4();
            sqlx::query("INSERT INTO dreamina_operator_video_supplement_cancellations (cancellation_id,authorization_id,dispatch_nonce,job_id,request_id,submission_id,executor_execution_id,provider_account_id,output_id,original_receipt_id,hold_id,quote_id,upstream_task_id,quote_hash,command_hash,submit_stdout_sha256,query_stdout_sha256,execution_audit_sha256,review_sha256,website_confirmation_sha256,head_review_sha256,upstream_identity_sha256,account_home_sha256,operator_reference,queried_at_ms,website_confirmed_at_ms,confirmed_at_ms,release_event_id,reason,original_effect_certainty,customer_charge_waived) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,'operator_supplement_cancelled','unknown_remote_effect',TRUE)")
    .bind(id).bind(request.authorization_id).bind(request.dispatch_nonce).bind(head.job_id).bind(&head.request_id).bind(head.submission_id).bind(head.executor_execution_id).bind(head.provider_account_id).bind(head.output_id).bind(head.original_receipt_id).bind(head.hold_id).bind(head.quote_id).bind(&head.remote_operation_id).bind(&head.quote_hash).bind(&head.command_hash).bind(&head.submit_stdout_sha256).bind(&verified.base.query_stdout_sha256).bind(&verified.base.execution_audit_sha256).bind(&verified.base.review_sha256).bind(&verified.base.website_confirmation_sha256).bind(&verified.head_review_sha256).bind(&verified.base.upstream_identity_sha256).bind(&verified.base.account_home_sha256).bind(&request.operator_reference).bind(verified.base.queried_at_ms).bind(verified.base.website_confirmed_at_ms).bind(now).bind(event).execute(&mut *tx).await.map_err(rejected)?;
            persist_customer_usage_facts_with_operator_supplement_cancellation(
                &mut tx,
                &CustomerUsageOutput {
                    job_id: head.job_id,
                    output_id: head.output_id,
                    provider_id: "dreamina-cli",
                    provider_account_id: Some(head.provider_account_id),
                    operation: "video_generation",
                    billing_metric: "video_second",
                    billing_unit: "second",
                    output_billable_units: head.billable_units,
                    terminal_outcome: "failed",
                },
                CustomerUsageAuthority::Durable {
                    submission_id: head.submission_id,
                    receipt_id: head.original_receipt_id,
                },
                id,
                now,
            )
            .await
            .map_err(|_| conflict())?;
            let rating = settle_customer_quote(&mut tx, head.job_id, &head.tenant_id)
                .await
                .map_err(|error| {
                    tracing::warn!(?error, "supplement cancellation rating rejected");
                    conflict()
                })?;
            if rating.total_amount_micros != 0
                || rating.currency != head.currency
                || rating.ledger_transaction_id.is_some()
            {
                return Err(conflict());
            }
            if sqlx::query("UPDATE quota_reservations SET state='released',committed_units=0,released_units=requested_units,updated_at_ms=$2 WHERE reservation_id=$1 AND state IN ('reserved','expired') AND committed_units=0 AND released_units=0").bind(head.reservation_id).bind(now).execute(&mut *tx).await.map_err(rejected)?.rows_affected()!=1 {return Err(conflict());}
            let payload = json!({"cancellation_id":id,"authorization_id":request.authorization_id,"dispatch_nonce":request.dispatch_nonce,"reason":"operator_supplement_cancelled","original_effect_certainty":"unknown_remote_effect","customer_charge_waived":true,"captured_micros":0,"rated_usage_id":rating.rated_usage_id});
            for (table, event_id) in [("job_events", event), ("outbox_events", Uuid::new_v4())] {
                sqlx::query(AssertSqlSafe(format!("INSERT INTO {table} (event_id,job_id,event_type,semantic_key,payload_json,created_at_ms) VALUES ($1,$2,'job.operator_cancelled','job.operator_dreamina_video_supplement_cancelled',$3,$4)"))).bind(event_id).bind(head.job_id).bind(&payload).bind(now).execute(&mut *tx).await.map_err(rejected)?;
            }
            sqlx::query("UPDATE dreamina_operator_video_supplement_cancellations SET finalized_snapshot=dreamina_supplement_cancel_snapshot(job_id) WHERE cancellation_id=$1 AND finalized_snapshot IS NULL").bind(id).execute(&mut *tx).await.map_err(rejected)?;
        }
        sqlx::query("SELECT assert_dreamina_supplement_cancel($1)")
            .bind(head.job_id)
            .execute(&mut *tx)
            .await
            .map_err(rejected)?;
        let proof = sqlx::query_as(AssertSqlSafe(format!("{PROOF} AND cancellation.job_id=$1")))
            .bind(head.job_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(rejected)?;
        Ok(proof)
    }
}
pub(super) async fn load_scoped(
    pool: &PgPool,
    tenant: &str,
    project: &str,
    actor: Option<Uuid>,
    job: Uuid,
) -> Result<Option<DreaminaManualVideoCancellationProof>, ImageGatewayError> {
    sqlx::query_as(AssertSqlSafe(format!("{PROOF} AND cancellation.job_id=$1 AND EXISTS(SELECT 1 FROM job_auth_attributions attribution WHERE attribution.job_id=cancellation.job_id AND attribution.tenant_id=$2 AND attribution.project_id=$3 AND ($4::UUID IS NULL OR attribution.actor_user_id=$4))"))).bind(job).bind(tenant).bind(project).bind(actor).fetch_optional(pool).await.map_err(unavailable)
}
fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Supplement cancellation conflicts with active head or verified zero-charge authority",
        None,
        "dreamina_supplement_cancellation_conflict",
    )
}
fn rejected(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(%error,"supplement cancellation rejected");
    conflict()
}
fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(%error,"supplement cancellation unavailable");
    ImageGatewayError::service_unavailable("Supplement cancellation unavailable")
}
