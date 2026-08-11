use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::official_metering::{
    OPENAI_GPT_IMAGE_2_CALCULATOR_SOURCE, gpt_image_2_output_tokens_from_dimensions,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum CustomerUsageAuthority {
    Durable {
        submission_id: Uuid,
        receipt_id: Uuid,
    },
    Inline {
        attempt_execution_id: Uuid,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct CustomerUsageOutput<'a> {
    pub job_id: Uuid,
    pub output_id: Uuid,
    pub provider_id: &'a str,
    pub provider_account_id: Option<Uuid>,
    pub operation: &'a str,
    pub billing_metric: &'a str,
    pub billing_unit: &'a str,
    pub output_billable_units: i32,
    pub terminal_outcome: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum CustomerUsageFactError {
    #[error("customer usage fact conflicts with frozen pricing evidence")]
    Conflict,
    #[error("customer usage fact storage is unavailable")]
    Unavailable,
}

#[derive(sqlx::FromRow)]
struct FrozenCustomerUsageBasis {
    metric: String,
    unit: String,
    quantity_source: String,
}

#[derive(sqlx::FromRow)]
struct FrozenCustomerUsageContext {
    provider_model_id: Option<String>,
    request_dimensions_json: Value,
}

pub(crate) async fn persist_customer_usage_facts(
    tx: &mut Transaction<'_, Postgres>,
    output: &CustomerUsageOutput<'_>,
    authority: CustomerUsageAuthority,
    now: i64,
) -> Result<(), CustomerUsageFactError> {
    let image_output = output.billing_metric == "output"
        && output.billing_unit == "output"
        && output.output_billable_units == 1;
    let video_output = output.billing_metric == "video_second"
        && output.billing_unit == "second"
        && output.output_billable_units > 0;
    if !image_output && !video_output {
        return Err(CustomerUsageFactError::Conflict);
    }

    let context: FrozenCustomerUsageContext = sqlx::query_as(
        r#"
        SELECT provider_model_id, request_dimensions_json
        FROM customer_price_quotes
        WHERE job_id = $1
        "#,
    )
    .bind(output.job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| CustomerUsageFactError::Unavailable)?
    .ok_or(CustomerUsageFactError::Conflict)?;
    let partition_key = format!("output:{}", output.output_id);
    let bases: Vec<FrozenCustomerUsageBasis> = sqlx::query_as(
        r#"
        SELECT DISTINCT metric, unit, quantity_source
        FROM customer_price_quote_lines
        WHERE job_id = $1
          AND partition_key = $2
          AND terminal_outcome = $3
        ORDER BY metric, unit, quantity_source
        "#,
    )
    .bind(output.job_id)
    .bind(&partition_key)
    .bind(output.terminal_outcome)
    .fetch_all(&mut **tx)
    .await
    .map_err(|_| CustomerUsageFactError::Unavailable)?;
    if bases.is_empty() {
        return Err(CustomerUsageFactError::Conflict);
    }

    for basis in bases {
        let (quantity, confidence, evidence_path, basis_name) = match (
            basis.metric.as_str(),
            basis.unit.as_str(),
            basis.quantity_source.as_str(),
        ) {
            ("image_output", "image", "request_derived") => (
                i64::from(output.output_billable_units),
                "exact",
                "job_outputs.billable_units",
                "admitted_output_quantity",
            ),
            ("image_input", "image", "request_derived")
                if output.provider_id == "grok-cli" && video_output =>
            {
                (
                    frozen_input_image_count(&context.request_dimensions_json)?,
                    "exact",
                    "customer_price_quotes.request_dimensions_json",
                    "admitted_input_image_count",
                )
            }
            ("image_output_token", "token", "official_lookup")
                if output.provider_id == "openai-codex"
                    && context.provider_model_id.as_deref() == Some("gpt-image-2") =>
            {
                let quantity = if output.terminal_outcome == "succeeded" {
                    i64::try_from(
                        gpt_image_2_output_tokens_from_dimensions(&context.request_dimensions_json)
                            .map_err(|_| CustomerUsageFactError::Conflict)?,
                    )
                    .map_err(|_| CustomerUsageFactError::Conflict)?
                } else {
                    0
                };
                (
                    quantity,
                    "estimated",
                    OPENAI_GPT_IMAGE_2_CALCULATOR_SOURCE,
                    "official_gpt_image_2_output_token_calculator",
                )
            }
            ("video_requested_second", "second", "request_derived")
                if video_output
                    && frozen_video_duration_seconds(&context.request_dimensions_json)?
                        == i64::from(output.output_billable_units) =>
            {
                (
                    i64::from(output.output_billable_units),
                    "exact",
                    "job_outputs.billable_units",
                    "admitted_video_duration_seconds",
                )
            }
            ("video_output_second", "second", "request_derived")
                if video_output
                    && frozen_video_duration_seconds(&context.request_dimensions_json)?
                        == i64::from(output.output_billable_units) =>
            {
                (
                    i64::from(output.output_billable_units),
                    "exact",
                    "job_outputs.billable_units",
                    "legacy_admitted_video_duration_seconds",
                )
            }
            _ => return Err(CustomerUsageFactError::Conflict),
        };
        let metadata = merge_usage_metadata(
            &context.request_dimensions_json,
            output.operation,
            output.billing_metric,
            output.billing_unit,
            basis_name,
        )?;
        let (submission_id, receipt_id, attempt_execution_id, authority_key) = match authority {
            CustomerUsageAuthority::Durable {
                submission_id,
                receipt_id,
            } => (
                Some(submission_id),
                Some(receipt_id),
                None,
                receipt_id.to_string(),
            ),
            CustomerUsageAuthority::Inline {
                attempt_execution_id,
            } => (
                None,
                None,
                Some(attempt_execution_id),
                format!("inline:{attempt_execution_id}:{}", output.output_id),
            ),
        };
        let semantic_key = format!(
            "{authority_key}:{}:{}:{}:v1",
            basis.metric, basis.unit, basis.quantity_source
        );
        let exact_replay: bool = sqlx::query_scalar(
            r#"
            WITH inserted AS (
                INSERT INTO provider_usage_facts (
                    usage_fact_id, semantic_key, job_id, output_id, submission_id,
                    receipt_id, attempt_execution_id, provider_id, provider_account_id,
                    execution_surface, fact_domain, metric, quantity, unit,
                    quantity_source, confidence, evidence_path, metadata_json,
                    billing_partition_key, terminal_outcome, created_at_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'provider_cli',
                        'customer_billable', $10, $11, $12, $13, $14, $15,
                        $16, $17, $18, $19)
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
                      AND existing.submission_id IS NOT DISTINCT FROM $5
                      AND existing.receipt_id IS NOT DISTINCT FROM $6
                      AND existing.attempt_execution_id IS NOT DISTINCT FROM $7
                      AND existing.provider_id = $8
                      AND existing.provider_account_id IS NOT DISTINCT FROM $9
                      AND existing.execution_surface = 'provider_cli'
                      AND existing.fact_domain = 'customer_billable'
                      AND existing.metric = $10
                      AND existing.quantity = $11
                      AND existing.unit = $12
                      AND existing.quantity_source = $13
                      AND existing.confidence = $14
                      AND existing.evidence_path = $15
                      AND existing.metadata_json = $16
                      AND existing.billing_partition_key = $17
                      AND existing.terminal_outcome = $18
                )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(semantic_key)
        .bind(output.job_id)
        .bind(output.output_id)
        .bind(submission_id)
        .bind(receipt_id)
        .bind(attempt_execution_id)
        .bind(output.provider_id)
        .bind(output.provider_account_id)
        .bind(&basis.metric)
        .bind(quantity)
        .bind(&basis.unit)
        .bind(&basis.quantity_source)
        .bind(confidence)
        .bind(evidence_path)
        .bind(metadata)
        .bind(&partition_key)
        .bind(output.terminal_outcome)
        .bind(now)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| {
            tracing::warn!(
                error = %error,
                job_id = %output.job_id,
                output_id = %output.output_id,
                "customer usage fact persistence failed"
            );
            CustomerUsageFactError::Unavailable
        })?;
        if !exact_replay {
            return Err(CustomerUsageFactError::Conflict);
        }
    }
    Ok(())
}

fn frozen_video_duration_seconds(dimensions: &Value) -> Result<i64, CustomerUsageFactError> {
    dimensions
        .get("duration")
        .and_then(Value::as_str)
        .and_then(|duration| duration.parse::<i64>().ok())
        .filter(|duration| (4..=15).contains(duration))
        .ok_or(CustomerUsageFactError::Conflict)
}

fn frozen_input_image_count(dimensions: &Value) -> Result<i64, CustomerUsageFactError> {
    dimensions
        .get("input_image_count")
        .and_then(Value::as_str)
        .and_then(|count| count.parse::<i64>().ok())
        .filter(|count| (1..=7).contains(count))
        .ok_or(CustomerUsageFactError::Conflict)
}

fn merge_usage_metadata(
    dimensions: &Value,
    operation: &str,
    billing_metric: &str,
    billing_unit: &str,
    basis: &str,
) -> Result<Value, CustomerUsageFactError> {
    let mut metadata = dimensions
        .as_object()
        .cloned()
        .ok_or(CustomerUsageFactError::Conflict)?;
    metadata.insert("operation".to_string(), json!(operation));
    metadata.insert("billing_metric".to_string(), json!(billing_metric));
    metadata.insert("billing_unit".to_string(), json!(billing_unit));
    metadata.insert("basis".to_string(), json!(basis));
    Ok(Value::Object(metadata))
}
