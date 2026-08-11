use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::ImageGatewayError;

use super::{
    customer_usage::{
        CustomerUsageAuthority, CustomerUsageFactError, CustomerUsageOutput,
        persist_customer_usage_facts,
    },
    postgres_rating::{CustomerRatingStoreError, settle_customer_quote},
};

#[derive(sqlx::FromRow)]
struct InlineSettlementContext {
    tenant_id: String,
    operation: String,
    provider_id: String,
    provider_account_id: Uuid,
    economics_contract_version: i16,
    job_state: String,
    charged_units: i32,
    requested_units: i32,
    quota_state: String,
    committed_units: i32,
    released_units: i32,
    attempt_state: String,
}

#[derive(sqlx::FromRow)]
struct InlineOutputRow {
    output_id: Uuid,
    billing_metric: String,
    billing_unit: String,
    billable_units: i32,
}

pub(crate) async fn settle_inline_customer_quote(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    attempt_execution_id: Uuid,
    expected_terminal_outcome: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let contract_version: i16 =
        sqlx::query_scalar("SELECT economics_contract_version FROM jobs WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| settlement_unavailable("load_contract", job_id, error))?
            .ok_or_else(|| ImageGatewayError::internal("customer settlement job missing"))?;
    if contract_version != 4 {
        return Ok(());
    }
    let context = load_inline_context(tx, job_id, attempt_execution_id).await?;
    debug_assert_eq!(context.economics_contract_version, contract_version);
    let canonical_outcome = match context.attempt_state.as_str() {
        "succeeded" => "succeeded",
        "failed" => "failed",
        _ => {
            return Err(ImageGatewayError::internal(
                "inline customer settlement requires a terminal attempt",
            ));
        }
    };
    if canonical_outcome != expected_terminal_outcome {
        return Err(ImageGatewayError::internal(
            "inline customer settlement outcome conflicts with attempt",
        ));
    }
    validate_terminal_economics(&context, canonical_outcome)?;

    let already_rated: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM customer_rated_usage WHERE job_id = $1)")
            .bind(job_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(|error| settlement_unavailable("load_rating", job_id, error))?;
    if !already_rated {
        let outputs: Vec<InlineOutputRow> = sqlx::query_as(
            r#"
            SELECT output.output_id, job.billing_metric, job.billing_unit,
                   output.billable_units
            FROM job_outputs output
            JOIN jobs job ON job.job_id = output.job_id
            WHERE output.job_id = $1
            ORDER BY output.output_index
            "#,
        )
        .bind(job_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| settlement_unavailable("load_outputs", job_id, error))?;
        if outputs.is_empty() {
            return Err(ImageGatewayError::internal(
                "inline customer settlement has no outputs",
            ));
        }
        for output in outputs {
            persist_customer_usage_facts(
                tx,
                &CustomerUsageOutput {
                    job_id,
                    output_id: output.output_id,
                    provider_id: &context.provider_id,
                    provider_account_id: Some(context.provider_account_id),
                    operation: &context.operation,
                    billing_metric: &output.billing_metric,
                    billing_unit: &output.billing_unit,
                    output_billable_units: output.billable_units,
                    terminal_outcome: canonical_outcome,
                },
                CustomerUsageAuthority::Inline {
                    attempt_execution_id,
                },
                now,
            )
            .await
            .map_err(map_usage_fact_error)?;
        }
    }
    settle_customer_quote(tx, job_id, &context.tenant_id)
        .await
        .map_err(map_rating_error)?;
    Ok(())
}

pub async fn reconcile_inline_customer_settlement(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<(), ImageGatewayError> {
    if job_id.is_nil() {
        return Err(ImageGatewayError::config("job id must not be nil"));
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| settlement_unavailable("begin_reconcile", job_id, error))?;
    let attempts: Vec<(Uuid, String)> = sqlx::query_as(
        r#"
        SELECT attempt.execution_id, attempt.state
        FROM job_attempts attempt
        JOIN work_items work
          ON work.work_item_id = attempt.work_item_id
         AND work.job_id = $1
         AND work.execution_id = attempt.execution_id
        WHERE work.job_id = $1
          AND attempt.state IN ('succeeded', 'failed')
          AND NOT EXISTS (
              SELECT 1 FROM provider_submissions submission
              WHERE submission.job_id = work.job_id
          )
        FOR UPDATE OF attempt, work
        "#,
    )
    .bind(job_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|error| settlement_unavailable("load_reconcile_attempt", job_id, error))?;
    let [(execution_id, attempt_state)] = attempts.as_slice() else {
        return Err(ImageGatewayError::config(
            "job does not have exactly one terminal inline attempt",
        ));
    };
    let outcome = if attempt_state == "succeeded" {
        "succeeded"
    } else {
        "failed"
    };
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(&mut *tx)
            .await
            .map_err(|error| settlement_unavailable("load_reconcile_time", job_id, error))?;
    settle_inline_customer_quote(&mut tx, job_id, *execution_id, outcome, now).await?;
    tx.commit()
        .await
        .map_err(|error| settlement_unavailable("commit_reconcile", job_id, error))
}

async fn load_inline_context(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    attempt_execution_id: Uuid,
) -> Result<InlineSettlementContext, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT job.tenant_id, job.operation, profile.provider_id,
               profile.provider_account_id, job.economics_contract_version,
               job.state AS job_state, job.charged_units, job.requested_units,
               quota.state AS quota_state, quota.committed_units,
               quota.released_units,
               attempt.state AS attempt_state
        FROM jobs job
        JOIN quota_reservations quota
          ON quota.reservation_id = job.reservation_id
         AND quota.job_id = job.job_id
         AND quota.tenant_id = job.tenant_id
        JOIN work_items work ON work.job_id = job.job_id
        JOIN job_attempts attempt
          ON attempt.work_item_id = work.work_item_id
         AND attempt.execution_id = $2
        JOIN provider_execution_profiles profile
          ON profile.execution_profile_id = work.execution_profile_id
        WHERE job.job_id = $1
          AND work.execution_id = attempt.execution_id
          AND NOT EXISTS (
              SELECT 1 FROM provider_submissions submission
              WHERE submission.job_id = job.job_id
          )
        FOR UPDATE OF job, quota, work, attempt
        "#,
    )
    .bind(job_id)
    .bind(attempt_execution_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| settlement_unavailable("load_context", job_id, error))?
    .ok_or_else(|| ImageGatewayError::internal("inline customer settlement authority missing"))
}

fn validate_terminal_economics(
    context: &InlineSettlementContext,
    canonical_outcome: &str,
) -> Result<(), ImageGatewayError> {
    let is_consistent = match canonical_outcome {
        "succeeded" => {
            context.job_state == "succeeded"
                && context.quota_state == "committed"
                && context.requested_units > 0
                && context.charged_units == context.requested_units
                && context.committed_units == context.requested_units
                && context.released_units == 0
        }
        "failed" => {
            context.job_state == "failed"
                && context.quota_state == "released"
                && context.requested_units > 0
                && context.charged_units == 0
                && context.committed_units == 0
                && context.released_units == context.requested_units
        }
        _ => false,
    };
    if is_consistent {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "inline customer settlement terminal economics conflict",
        ))
    }
}

fn map_usage_fact_error(error: CustomerUsageFactError) -> ImageGatewayError {
    match error {
        CustomerUsageFactError::Conflict => {
            ImageGatewayError::internal("customer usage facts conflict")
        }
        CustomerUsageFactError::Unavailable => {
            ImageGatewayError::service_unavailable("customer usage facts unavailable")
        }
    }
}

fn map_rating_error(error: CustomerRatingStoreError) -> ImageGatewayError {
    match error {
        CustomerRatingStoreError::InvalidInput | CustomerRatingStoreError::Conflict => {
            ImageGatewayError::internal("customer quote settlement conflicts")
        }
        CustomerRatingStoreError::Unavailable => {
            ImageGatewayError::service_unavailable("customer quote settlement unavailable")
        }
    }
}

fn settlement_unavailable(
    stage: &'static str,
    job_id: Uuid,
    error: sqlx::Error,
) -> ImageGatewayError {
    tracing::warn!(
        error = %error,
        %job_id,
        customer.settlement.stage = stage,
        "inline customer settlement database operation failed"
    );
    ImageGatewayError::service_unavailable("customer settlement unavailable")
}
