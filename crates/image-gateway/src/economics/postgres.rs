use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{Acquire, PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    EconomicReceipt, EconomicReceiptOutcome, EconomicSettlement, EconomicSettlementError,
    EconomicSettlementStore, ProviderReceiptRecord, evidence_hash,
};
use crate::admission::{AdmissionError, AttachJob};

const MAX_OUTPUTS_PER_JOB: i32 = 10;
const OUTPUT_BILLING_METRIC: &str = "output";
const OUTPUT_BILLING_UNIT: &str = "output";
const REQUEST_BILLING_METRIC: &str = "request";
const REQUEST_BILLING_UNIT: &str = "request";
const VIDEO_SECOND_BILLING_METRIC: &str = "video_second";
const VIDEO_SECOND_BILLING_UNIT: &str = "second";

#[derive(Clone)]
pub struct PostgresEconomicSettlementStore {
    pool: PgPool,
}

impl PostgresEconomicSettlementStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct LockedJob {
    tenant_id: String,
    operation: String,
    provider_id: String,
    model: String,
    requested_units: i32,
    output_count: i32,
    billable_units: i32,
    billing_metric: String,
    billing_unit: String,
    economics_contract_version: i16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BillingDimension {
    Output,
    Request,
    VideoSecond,
}

impl BillingDimension {
    fn from_job(job: &LockedJob) -> Result<Self, AdmissionError> {
        let dimension = match (job.billing_metric.as_str(), job.billing_unit.as_str()) {
            (OUTPUT_BILLING_METRIC, OUTPUT_BILLING_UNIT) => Self::Output,
            (REQUEST_BILLING_METRIC, REQUEST_BILLING_UNIT) => Self::Request,
            (VIDEO_SECOND_BILLING_METRIC, VIDEO_SECOND_BILLING_UNIT) => Self::VideoSecond,
            _ => return Err(AdmissionError::InvalidCommand),
        };
        let dimensions_valid = job.requested_units == job.billable_units
            && match dimension {
                Self::Output => {
                    (1..=MAX_OUTPUTS_PER_JOB).contains(&job.output_count)
                        && job.output_count == job.billable_units
                }
                Self::Request => job.output_count == 1 && job.billable_units == 1,
                Self::VideoSecond => job.output_count == 1 && job.billable_units > 0,
            };
        if dimensions_valid {
            Ok(dimension)
        } else {
            Err(AdmissionError::InvalidCommand)
        }
    }

    const fn contract_version(self) -> i16 {
        match self {
            Self::Output | Self::Request => 2,
            Self::VideoSecond => 3,
        }
    }

    const fn output_billable_units(self, job_billable_units: i32) -> i32 {
        match self {
            Self::Output => 1,
            Self::Request | Self::VideoSecond => job_billable_units,
        }
    }
}

#[derive(sqlx::FromRow)]
struct PriceVersion {
    price_version_id: Uuid,
    billing_metric: String,
    billing_unit: String,
    currency: String,
    success_micros: i64,
    failed_micros: i64,
    no_effect_micros: i64,
}

#[derive(sqlx::FromRow)]
struct FrozenQuote {
    quote_id: Uuid,
    price_version_id: Uuid,
    currency: String,
    output_count: i32,
    billable_units: i64,
    billing_metric: String,
    billing_unit: String,
    price_billing_metric: String,
    price_billing_unit: String,
    success_micros: i64,
    failed_micros: i64,
    no_effect_micros: i64,
    max_total_micros: i64,
    quote_hash: String,
}

#[derive(sqlx::FromRow)]
struct FrozenOutputSet {
    output_count: i64,
    min_index: Option<i32>,
    max_index: Option<i32>,
    min_billable_units: Option<i32>,
    max_billable_units: Option<i32>,
    hold_count: i64,
    holds_match_weights: Option<bool>,
    held_total: i64,
}

#[derive(sqlx::FromRow)]
struct SubmissionIdentity {
    output_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    provider_id: String,
    submission_state: String,
    submission_error_code: Option<String>,
    result_manifest_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct LockedEconomicOutput {
    economics_contract_version: i16,
    output_state: String,
    billable_units: i32,
    hold_state: String,
    held_micros: i64,
    quote_id: Uuid,
    currency: String,
    billing_metric: String,
    billing_unit: String,
    quote_billable_units: i64,
    success_micros: i64,
    failed_micros: i64,
    no_effect_micros: i64,
}

#[derive(sqlx::FromRow)]
struct StoredReceipt {
    receipt_id: Uuid,
    outcome: String,
    receipt_schema: String,
    payload_hash: String,
    evidence: serde_json::Value,
    provider_cost_micros: Option<i64>,
    provider_cost_currency: Option<String>,
}

pub(crate) async fn admit_job_outputs(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    api_profile: &str,
    now: i64,
) -> Result<(), AdmissionError> {
    let job = lock_job(tx, request.job_id).await?;
    let dimension = validate_job_dimensions(&job, request)?;
    let target_contract_version = dimension.contract_version();
    if job.economics_contract_version == target_contract_version {
        return validate_locked_economics(tx, request.job_id, &job).await;
    }
    if job.economics_contract_version != 1 {
        return Err(AdmissionError::InvalidOwner);
    }

    let existing_outputs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM job_outputs WHERE job_id = $1")
            .bind(request.job_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(unavailable)?;
    if existing_outputs != 0 {
        return Err(AdmissionError::InvalidOwner);
    }

    let price = select_price(tx, api_profile, &job).await?;
    let max_unit_micros = price
        .success_micros
        .max(price.failed_micros)
        .max(price.no_effect_micros);
    let max_total_micros = max_unit_micros
        .checked_mul(i64::from(job.billable_units))
        .ok_or(AdmissionError::InvalidCommand)?;
    let quote_id = Uuid::new_v4();
    let quote_hash = quote_hash(request.job_id, &price, &job, max_total_micros);

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("budget:{}:{}", job.tenant_id, price.currency))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts
          (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
           refunded_micros, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 0, 0, 0, 0, $3, $3)
        ON CONFLICT (tenant_id, currency) DO NOTHING
        "#,
    )
    .bind(&job.tenant_id)
    .bind(&price.currency)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    let reserved = sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = held_micros + $3, updated_at_ms = $4
        WHERE tenant_id = $1 AND currency = $2
          AND (
                held_micros::NUMERIC + captured_micros::NUMERIC
                - refunded_micros::NUMERIC + $3::NUMERIC
              )
              <= credit_limit_micros::NUMERIC
        "#,
    )
    .bind(&job.tenant_id)
    .bind(&price.currency)
    .bind(max_total_micros)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if reserved != 1 {
        return Err(AdmissionError::BillingLimitExceeded);
    }

    sqlx::query(
        r#"
        INSERT INTO price_quotes
          (quote_id, job_id, price_version_id, currency, output_count, billable_units,
           billing_metric, billing_unit, success_micros, failed_micros, no_effect_micros,
           max_total_micros, quote_hash, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        "#,
    )
    .bind(quote_id)
    .bind(request.job_id)
    .bind(price.price_version_id)
    .bind(&price.currency)
    .bind(job.output_count)
    .bind(job.billable_units)
    .bind(&job.billing_metric)
    .bind(&job.billing_unit)
    .bind(price.success_micros)
    .bind(price.failed_micros)
    .bind(price.no_effect_micros)
    .bind(max_total_micros)
    .bind(&quote_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;

    let output_billable_units = dimension.output_billable_units(job.billable_units);
    let output_hold_micros = max_unit_micros
        .checked_mul(i64::from(output_billable_units))
        .ok_or(AdmissionError::InvalidCommand)?;
    for output_index in 0..job.output_count {
        let output_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO job_outputs
              (output_id, job_id, output_index, billable_units, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, 'pending', $5, $5)
            "#,
        )
        .bind(output_id)
        .bind(request.job_id)
        .bind(output_index)
        .bind(output_billable_units)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            r#"
            INSERT INTO output_holds
              (output_id, job_id, quote_id, tenant_id, currency, held_micros,
               state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, 'held', $7, $7)
            "#,
        )
        .bind(output_id)
        .bind(request.job_id)
        .bind(quote_id)
        .bind(&job.tenant_id)
        .bind(&price.currency)
        .bind(output_hold_micros)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }

    let changed = sqlx::query(
        "UPDATE jobs SET economics_contract_version = $2, updated_at_ms = $3 WHERE job_id = $1 AND economics_contract_version = 1",
    )
    .bind(request.job_id)
    .bind(target_contract_version)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if changed != 1 {
        return Err(AdmissionError::InvalidOwner);
    }
    Ok(())
}

pub(crate) async fn validate_admitted_job_outputs(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
) -> Result<(), AdmissionError> {
    let job = lock_job(tx, request.job_id).await?;
    let dimension = validate_job_dimensions(&job, request)?;
    if job.economics_contract_version != dimension.contract_version() {
        return Err(AdmissionError::InvalidOwner);
    }
    validate_locked_economics(tx, request.job_id, &job).await
}

async fn lock_job(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<LockedJob, AdmissionError> {
    sqlx::query_as(
        r#"
        SELECT tenant_id, operation, provider_id, model, requested_units,
               output_count, billable_units, billing_metric, billing_unit,
               economics_contract_version
        FROM jobs
        WHERE job_id = $1 AND state IN ('reserved', 'queued', 'running', 'succeeded', 'failed', 'uncertain')
        FOR UPDATE
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(AdmissionError::InvalidOwner)
}

fn validate_job_dimensions(
    job: &LockedJob,
    request: &AttachJob,
) -> Result<BillingDimension, AdmissionError> {
    let dimension = BillingDimension::from_job(job)?;
    if let Some(command_count) = request.command_json.get("n") {
        let command_count = command_count
            .as_u64()
            .and_then(|value| i32::try_from(value).ok())
            .ok_or(AdmissionError::InvalidCommand)?;
        if command_count != job.output_count {
            return Err(AdmissionError::InvalidCommand);
        }
    }
    Ok(dimension)
}

async fn select_price(
    tx: &mut Transaction<'_, Postgres>,
    api_profile: &str,
    job: &LockedJob,
) -> Result<PriceVersion, AdmissionError> {
    let price: PriceVersion = sqlx::query_as(
        r#"
        WITH requested_profile AS (
            SELECT COALESCE(
                (SELECT pricing_api_profile
                 FROM api_profile_pricing_aliases
                 WHERE api_profile = $1),
                $1
            ) AS pricing_api_profile
        )
        SELECT price_version_id, billing_metric, billing_unit, currency,
               success_micros, failed_micros, no_effect_micros
        FROM price_versions, requested_profile
        WHERE state = 'active'
          AND api_profile IN ($1, requested_profile.pricing_api_profile, '*')
          AND operation IN ($2, '*')
          AND provider_id IN ($3, '*')
          AND model IN ($4, '*')
          AND billing_metric = $5
          AND billing_unit = $6
        ORDER BY
          CASE
            WHEN api_profile = $1 THEN 2
            WHEN api_profile = requested_profile.pricing_api_profile THEN 1
            ELSE 0
          END DESC,
          ((operation <> '*')::INT + (provider_id <> '*')::INT
           + (model <> '*')::INT) DESC,
          version DESC, price_version_id
        LIMIT 1
        FOR SHARE
        "#,
    )
    .bind(api_profile)
    .bind(&job.operation)
    .bind(&job.provider_id)
    .bind(&job.model)
    .bind(&job.billing_metric)
    .bind(&job.billing_unit)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(AdmissionError::PricingUnavailable)?;
    if job.billing_metric == VIDEO_SECOND_BILLING_METRIC && price.success_micros <= 0 {
        return Err(AdmissionError::PricingUnavailable);
    }
    Ok(price)
}

async fn validate_locked_economics(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    job: &LockedJob,
) -> Result<(), AdmissionError> {
    let quote: FrozenQuote = sqlx::query_as(
        r#"
        SELECT q.quote_id, q.price_version_id, q.currency, q.output_count,
               q.billable_units, q.billing_metric, q.billing_unit,
               p.billing_metric AS price_billing_metric,
               p.billing_unit AS price_billing_unit,
               q.success_micros, q.failed_micros, q.no_effect_micros,
               q.max_total_micros, q.quote_hash
        FROM price_quotes q
        JOIN price_versions p ON p.price_version_id = q.price_version_id
        WHERE q.job_id = $1
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(AdmissionError::InvalidOwner)?;
    let price = PriceVersion {
        price_version_id: quote.price_version_id,
        billing_metric: quote.price_billing_metric.clone(),
        billing_unit: quote.price_billing_unit.clone(),
        currency: quote.currency.clone(),
        success_micros: quote.success_micros,
        failed_micros: quote.failed_micros,
        no_effect_micros: quote.no_effect_micros,
    };
    let expected_hash = quote_hash(job_id, &price, job, quote.max_total_micros);
    let legacy_v2_hash =
        legacy_v2_quote_hash(job_id, &price, quote.output_count, quote.max_total_micros);
    let hash_valid = quote.quote_hash == expected_hash
        || (job.economics_contract_version == 2
            && job.billing_metric == OUTPUT_BILLING_METRIC
            && job.billing_unit == OUTPUT_BILLING_UNIT
            && quote.quote_hash == legacy_v2_hash);
    let expected_max_total_micros = quote
        .success_micros
        .max(quote.failed_micros)
        .max(quote.no_effect_micros)
        .checked_mul(i64::from(job.billable_units));
    if quote.output_count != job.output_count
        || quote.billable_units != i64::from(job.billable_units)
        || quote.billing_metric != job.billing_metric
        || quote.billing_unit != job.billing_unit
        || quote.price_billing_metric != job.billing_metric
        || quote.price_billing_unit != job.billing_unit
        || expected_max_total_micros != Some(quote.max_total_micros)
        || !hash_valid
    {
        return Err(AdmissionError::InvalidOwner);
    }
    let expected_output_billable_units =
        BillingDimension::from_job(job)?.output_billable_units(job.billable_units);
    let outputs: FrozenOutputSet = sqlx::query_as(
        r#"
        SELECT COUNT(*)::BIGINT AS output_count,
               MIN(o.output_index) AS min_index,
               MAX(o.output_index) AS max_index,
               MIN(o.billable_units) AS min_billable_units,
               MAX(o.billable_units) AS max_billable_units,
               COUNT(h.output_id)::BIGINT AS hold_count,
               BOOL_AND(
                   h.held_micros = o.billable_units::BIGINT
                       * GREATEST($3::BIGINT, $4::BIGINT, $5::BIGINT)
               ) AS holds_match_weights,
               COALESCE(SUM(h.held_micros), 0)::BIGINT AS held_total
        FROM job_outputs o
        LEFT JOIN output_holds h
          ON h.output_id = o.output_id AND h.job_id = o.job_id AND h.quote_id = $2
        WHERE o.job_id = $1
        "#,
    )
    .bind(job_id)
    .bind(quote.quote_id)
    .bind(quote.success_micros)
    .bind(quote.failed_micros)
    .bind(quote.no_effect_micros)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if outputs.output_count != i64::from(job.output_count)
        || outputs.hold_count != outputs.output_count
        || outputs.min_index != Some(0)
        || outputs.max_index != Some(job.output_count - 1)
        || outputs.min_billable_units != Some(expected_output_billable_units)
        || outputs.max_billable_units != Some(expected_output_billable_units)
        || outputs.holds_match_weights != Some(true)
        || outputs.held_total != quote.max_total_micros
    {
        return Err(AdmissionError::InvalidOwner);
    }
    Ok(())
}

fn quote_hash(
    job_id: Uuid,
    price: &PriceVersion,
    job: &LockedJob,
    max_total_micros: i64,
) -> String {
    hash_quote_parts([
        job_id.to_string(),
        price.price_version_id.to_string(),
        price.billing_metric.clone(),
        price.billing_unit.clone(),
        price.currency.clone(),
        job.output_count.to_string(),
        job.billable_units.to_string(),
        price.success_micros.to_string(),
        price.failed_micros.to_string(),
        price.no_effect_micros.to_string(),
        max_total_micros.to_string(),
    ])
}

fn legacy_v2_quote_hash(
    job_id: Uuid,
    price: &PriceVersion,
    output_count: i32,
    max_total_micros: i64,
) -> String {
    hash_quote_parts([
        job_id.to_string(),
        price.price_version_id.to_string(),
        price.currency.clone(),
        output_count.to_string(),
        price.success_micros.to_string(),
        price.failed_micros.to_string(),
        price.no_effect_micros.to_string(),
        max_total_micros.to_string(),
    ])
}

fn hash_quote_parts<const N: usize>(parts: [String; N]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    hex::encode(hasher.finalize())
}

#[async_trait]
impl EconomicSettlementStore for PostgresEconomicSettlementStore {
    async fn settle(
        &self,
        receipt: &EconomicReceipt,
    ) -> Result<EconomicSettlement, EconomicSettlementError> {
        let mut tx = self.pool.begin().await.map_err(economic_unavailable)?;
        let settlement = settle_receipt_in_transaction(&mut tx, receipt).await?;
        tx.commit().await.map_err(economic_unavailable)?;
        Ok(settlement)
    }
}

pub(crate) async fn settle_receipt_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    receipt: &EconomicReceipt,
) -> Result<EconomicSettlement, EconomicSettlementError> {
    let mut tx = transaction.begin().await.map_err(economic_unavailable)?;
    validate_receipt(receipt)?;
    let snapshot = load_submission_identity(&mut tx, receipt.submission_id).await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("budget:{}", snapshot.tenant_id))
        .execute(&mut *tx)
        .await
        .map_err(economic_unavailable)?;
    let locked_job: Option<Uuid> =
        sqlx::query_scalar(
            "SELECT job_id FROM jobs WHERE job_id = $1 AND economics_contract_version IN (2, 3) FOR UPDATE",
        )
            .bind(snapshot.job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(economic_unavailable)?;
    if locked_job.is_none() {
        return Err(EconomicSettlementError::Conflict);
    }
    let output: LockedEconomicOutput = sqlx::query_as(
        r#"
            SELECT j.economics_contract_version, o.state AS output_state, o.billable_units,
                   h.state AS hold_state, h.held_micros,
                   q.quote_id, q.currency, q.billing_metric, q.billing_unit,
                   q.billable_units AS quote_billable_units,
                   q.success_micros, q.failed_micros, q.no_effect_micros
            FROM job_outputs o
            JOIN output_holds h ON h.output_id = o.output_id AND h.job_id = o.job_id
            JOIN price_quotes q ON q.quote_id = h.quote_id AND q.job_id = o.job_id
            JOIN jobs j ON j.job_id = o.job_id
            WHERE o.output_id = $1 AND o.job_id = $2
              AND q.output_count = j.output_count
              AND q.billable_units = j.billable_units
              AND q.billing_metric = j.billing_metric
              AND q.billing_unit = j.billing_unit
            FOR UPDATE OF o, h
            "#,
    )
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(economic_unavailable)?
    .ok_or(EconomicSettlementError::Conflict)?;
    validate_locked_output(&output)?;
    let locked: SubmissionIdentity = sqlx::query_as(
        r#"
            SELECT output_id, job_id, tenant_id, provider_id,
                   state AS submission_state, error_code AS submission_error_code,
                   result_manifest_id
            FROM provider_submissions
            WHERE submission_id = $1
            FOR UPDATE
            "#,
    )
    .bind(receipt.submission_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(economic_unavailable)?
    .ok_or(EconomicSettlementError::Conflict)?;
    if locked.output_id != snapshot.output_id
        || locked.job_id != snapshot.job_id
        || locked.tenant_id != snapshot.tenant_id
        || locked.provider_id != snapshot.provider_id
    {
        return Err(EconomicSettlementError::Conflict);
    }
    validate_provider_evidence(&locked, receipt)?;

    if let Some(stored) = load_stored_receipt(&mut tx, receipt.submission_id).await? {
        let result = replay_settlement(&mut tx, &stored, receipt).await?;
        tx.commit().await.map_err(economic_unavailable)?;
        return Ok(result);
    }
    if output.hold_state != "held" || !matches!(output.output_state.as_str(), "pending" | "running")
    {
        return Err(EconomicSettlementError::Conflict);
    }

    let now = economic_database_now(&mut tx).await?;
    let receipt_id = Uuid::new_v4();
    let semantic_key = receipt_semantic_key(receipt);
    sqlx::query(
        r#"
            INSERT INTO provider_receipts
              (receipt_id, semantic_key, submission_id, output_id, job_id, provider_id,
               outcome, receipt_schema, payload_hash, evidence,
               provider_cost_micros, provider_cost_currency, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
    )
    .bind(receipt_id)
    .bind(&semantic_key)
    .bind(receipt.submission_id)
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .bind(&snapshot.provider_id)
    .bind(receipt.outcome.as_str())
    .bind(&receipt.receipt_schema)
    .bind(&receipt.payload_hash)
    .bind(&receipt.evidence)
    .bind(Option::<i64>::None)
    .bind(Option::<String>::None)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(economic_unavailable)?;

    let meter_event_id = Uuid::new_v4();
    let fact_kind = if receipt.outcome == EconomicReceiptOutcome::Uncertain {
        "uncertain_observation"
    } else {
        "output_terminal"
    };
    let quantity = if receipt.outcome == EconomicReceiptOutcome::Uncertain {
        0
    } else {
        i64::from(output.billable_units)
    };
    sqlx::query(
        r#"
            INSERT INTO economic_metering_events
              (meter_event_id, semantic_key, output_id, job_id, submission_id, receipt_id,
               fact_kind, metric, quantity, unit, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
    )
    .bind(meter_event_id)
    .bind(format!("meter:{semantic_key}"))
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .bind(receipt.submission_id)
    .bind(receipt_id)
    .bind(fact_kind)
    .bind(&output.billing_metric)
    .bind(quantity)
    .bind(&output.billing_unit)
    .bind(receipt.outcome.as_str())
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(economic_unavailable)?;

    if receipt.outcome == EconomicReceiptOutcome::Uncertain {
        sqlx::query(
            r#"
                UPDATE job_outputs
                SET state = 'uncertain', started_at_ms = COALESCE(started_at_ms, $2),
                    finished_at_ms = $2, updated_at_ms = $2,
                    error_code = 'provider_outcome_uncertain'
                WHERE output_id = $1 AND state IN ('pending', 'running')
                "#,
        )
        .bind(snapshot.output_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(economic_unavailable)?;
        let settlement = EconomicSettlement {
            receipt_id,
            meter_event_id,
            rated_usage_id: None,
            customer_ledger_transaction_id: None,
            outcome: receipt.outcome,
        };
        tx.commit().await.map_err(economic_unavailable)?;
        return Ok(settlement);
    }

    let unit_price_micros = match receipt.outcome {
        EconomicReceiptOutcome::Succeeded => output.success_micros,
        EconomicReceiptOutcome::Failed => output.failed_micros,
        EconomicReceiptOutcome::NoEffect => output.no_effect_micros,
        EconomicReceiptOutcome::Uncertain => unreachable!(),
    };
    let amount_micros = unit_price_micros
        .checked_mul(quantity)
        .ok_or(EconomicSettlementError::Conflict)?;
    if amount_micros > output.held_micros {
        return Err(EconomicSettlementError::Conflict);
    }
    let rated_usage_id = Uuid::new_v4();
    sqlx::query(
        r#"
            INSERT INTO rated_usage
              (rated_usage_id, semantic_key, meter_event_id, output_id, job_id, quote_id,
               outcome, quantity, unit_price_micros, amount_micros, currency, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
    )
    .bind(rated_usage_id)
    .bind(format!("rating:{semantic_key}"))
    .bind(meter_event_id)
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .bind(output.quote_id)
    .bind(receipt.outcome.as_str())
    .bind(quantity)
    .bind(unit_price_micros)
    .bind(amount_micros)
    .bind(&output.currency)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(economic_unavailable)?;
    settle_hold_and_account(&mut tx, &snapshot, &output, amount_micros, now).await?;
    let customer_ledger_transaction_id = if amount_micros == 0 {
        None
    } else {
        Some(
            insert_ledger_pair(
                &mut tx,
                &format!("customer-charge:{semantic_key}"),
                snapshot.output_id,
                snapshot.job_id,
                receipt.submission_id,
                receipt_id,
                "customer_charge",
                &output.currency,
                amount_micros,
                &format!(
                    "tenant:{}:{}:receivable",
                    snapshot.tenant_id, output.currency
                ),
                "tenant",
                &snapshot.tenant_id,
                "receivable",
                &format!("platform:{}:revenue", output.currency),
                "platform",
                "platform",
                "revenue",
                now,
            )
            .await?,
        )
    };
    let (output_state, error_code) = match receipt.outcome {
        EconomicReceiptOutcome::Succeeded => ("succeeded", None),
        EconomicReceiptOutcome::Failed => ("failed", Some("provider_failed")),
        EconomicReceiptOutcome::NoEffect => ("failed", Some("provider_no_effect")),
        EconomicReceiptOutcome::Uncertain => unreachable!(),
    };
    let changed = sqlx::query(
        r#"
            UPDATE job_outputs
            SET state = $2, started_at_ms = COALESCE(started_at_ms, $3),
                finished_at_ms = $3, updated_at_ms = $3, error_code = $4
            WHERE output_id = $1 AND state IN ('pending', 'running')
            "#,
    )
    .bind(snapshot.output_id)
    .bind(output_state)
    .bind(now)
    .bind(error_code)
    .execute(&mut *tx)
    .await
    .map_err(economic_unavailable)?
    .rows_affected();
    if changed != 1 {
        return Err(EconomicSettlementError::Conflict);
    }

    let settlement = EconomicSettlement {
        receipt_id,
        meter_event_id,
        rated_usage_id: Some(rated_usage_id),
        customer_ledger_transaction_id,
        outcome: receipt.outcome,
    };
    tx.commit().await.map_err(economic_unavailable)?;
    Ok(settlement)
}

pub(crate) async fn record_v4_provider_receipt_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    receipt: &EconomicReceipt,
) -> Result<ProviderReceiptRecord, EconomicSettlementError> {
    let mut tx = transaction.begin().await.map_err(economic_unavailable)?;
    validate_receipt(receipt)?;
    let snapshot = load_submission_identity(&mut tx, receipt.submission_id).await?;
    let contract_version: Option<i16> = sqlx::query_scalar(
        "SELECT economics_contract_version FROM jobs WHERE job_id = $1 AND tenant_id = $2",
    )
    .bind(snapshot.job_id)
    .bind(&snapshot.tenant_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(economic_unavailable)?;
    if contract_version != Some(4) {
        return Err(EconomicSettlementError::Conflict);
    }
    let locked: SubmissionIdentity = sqlx::query_as(
        r#"
        SELECT output_id, job_id, tenant_id, provider_id,
               state AS submission_state, error_code AS submission_error_code,
               result_manifest_id
        FROM provider_submissions
        WHERE submission_id = $1
        FOR UPDATE
        "#,
    )
    .bind(receipt.submission_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(economic_unavailable)?
    .ok_or(EconomicSettlementError::Conflict)?;
    if locked.output_id != snapshot.output_id
        || locked.job_id != snapshot.job_id
        || locked.tenant_id != snapshot.tenant_id
        || locked.provider_id != snapshot.provider_id
    {
        return Err(EconomicSettlementError::Conflict);
    }
    validate_provider_evidence(&locked, receipt)?;

    if let Some(stored) = load_stored_receipt(&mut tx, receipt.submission_id).await? {
        validate_stored_receipt(&stored, receipt)?;
        validate_v4_provider_cost_ledger(&mut tx, stored.receipt_id).await?;
        let record = ProviderReceiptRecord {
            receipt_id: stored.receipt_id,
            outcome: receipt.outcome,
        };
        tx.commit().await.map_err(economic_unavailable)?;
        return Ok(record);
    }

    let now = economic_database_now(&mut tx).await?;
    let receipt_id = Uuid::new_v4();
    let semantic_key = receipt_semantic_key(receipt);
    sqlx::query(
        r#"
        INSERT INTO provider_receipts (
            receipt_id, semantic_key, submission_id, output_id, job_id,
            provider_id, outcome, receipt_schema, payload_hash, evidence,
            provider_cost_micros, provider_cost_currency, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#,
    )
    .bind(receipt_id)
    .bind(&semantic_key)
    .bind(receipt.submission_id)
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .bind(&snapshot.provider_id)
    .bind(receipt.outcome.as_str())
    .bind(&receipt.receipt_schema)
    .bind(&receipt.payload_hash)
    .bind(&receipt.evidence)
    .bind(Option::<i64>::None)
    .bind(Option::<String>::None)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(economic_unavailable)?;
    let record = ProviderReceiptRecord {
        receipt_id,
        outcome: receipt.outcome,
    };
    tx.commit().await.map_err(economic_unavailable)?;
    Ok(record)
}

fn validate_locked_output(output: &LockedEconomicOutput) -> Result<(), EconomicSettlementError> {
    let billable_units = i64::from(output.billable_units);
    let dimensions_valid = billable_units > 0
        && match (output.billing_metric.as_str(), output.billing_unit.as_str()) {
            (OUTPUT_BILLING_METRIC, OUTPUT_BILLING_UNIT) => {
                output.economics_contract_version == 2 && output.billable_units == 1
            }
            (REQUEST_BILLING_METRIC, REQUEST_BILLING_UNIT) => {
                output.economics_contract_version == 2
                    && output.billable_units == 1
                    && output.quote_billable_units == 1
            }
            (VIDEO_SECOND_BILLING_METRIC, VIDEO_SECOND_BILLING_UNIT) => {
                output.economics_contract_version == 3
                    && billable_units == output.quote_billable_units
            }
            _ => false,
        };
    let expected_hold = output
        .success_micros
        .max(output.failed_micros)
        .max(output.no_effect_micros)
        .checked_mul(billable_units);
    if dimensions_valid && expected_hold == Some(output.held_micros) {
        Ok(())
    } else {
        Err(EconomicSettlementError::Conflict)
    }
}

pub(super) fn validate_receipt(receipt: &EconomicReceipt) -> Result<(), EconomicSettlementError> {
    let schema_valid = !receipt.receipt_schema.is_empty()
        && receipt.receipt_schema.len() <= 128
        && !receipt
            .receipt_schema
            .bytes()
            .any(|byte| byte.is_ascii_control());
    let hash_valid = receipt.payload_hash.len() == 64
        && receipt
            .payload_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
    let evidence_hash_matches =
        evidence_hash(&receipt.evidence).is_ok_and(|expected| expected == receipt.payload_hash);
    if schema_valid && hash_valid && evidence_hash_matches && receipt.evidence.is_object() {
        Ok(())
    } else {
        Err(EconomicSettlementError::InvalidInput)
    }
}

async fn load_submission_identity(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<SubmissionIdentity, EconomicSettlementError> {
    sqlx::query_as(
        r#"
        SELECT output_id, job_id, tenant_id, provider_id,
               state AS submission_state, error_code AS submission_error_code,
               result_manifest_id
        FROM provider_submissions
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(economic_unavailable)?
    .ok_or(EconomicSettlementError::NotReady)
}

fn validate_provider_evidence(
    submission: &SubmissionIdentity,
    receipt: &EconomicReceipt,
) -> Result<(), EconomicSettlementError> {
    let valid = match receipt.outcome {
        EconomicReceiptOutcome::Succeeded => {
            submission.submission_state == "succeeded" && submission.result_manifest_id.is_some()
        }
        EconomicReceiptOutcome::Failed => {
            submission.submission_state == "failed"
                && submission.result_manifest_id.is_none()
                && submission.submission_error_code.as_deref() != Some("provider_no_effect")
        }
        EconomicReceiptOutcome::NoEffect => {
            submission.result_manifest_id.is_none()
                && ((submission.submission_state == "failed"
                    && submission.submission_error_code.as_deref() == Some("provider_no_effect"))
                    || (submission.submission_state == "canceled"
                        && submission.submission_error_code.is_some()))
        }
        EconomicReceiptOutcome::Uncertain => {
            submission.submission_state == "uncertain" && submission.result_manifest_id.is_none()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(EconomicSettlementError::NotReady)
    }
}

async fn load_stored_receipt(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<StoredReceipt>, EconomicSettlementError> {
    sqlx::query_as(
        r#"
        SELECT receipt_id, outcome, receipt_schema, payload_hash, evidence,
               provider_cost_micros, provider_cost_currency
        FROM provider_receipts
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(economic_unavailable)
}

async fn replay_settlement(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredReceipt,
    receipt: &EconomicReceipt,
) -> Result<EconomicSettlement, EconomicSettlementError> {
    validate_stored_receipt(stored, receipt)?;
    let row: Option<(Uuid, Option<Uuid>, Option<Uuid>)> = sqlx::query_as(
        r#"
        SELECT m.meter_event_id, r.rated_usage_id,
               (SELECT transaction_id FROM ledger_transactions
                 WHERE source_output_id = m.output_id AND transaction_type = 'customer_charge')
        FROM economic_metering_events m
        LEFT JOIN rated_usage r ON r.meter_event_id = m.meter_event_id
        WHERE m.receipt_id = $1
        "#,
    )
    .bind(stored.receipt_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(economic_unavailable)?;
    let Some((meter_event_id, rated_usage_id, customer_ledger_transaction_id)) = row else {
        return Err(EconomicSettlementError::Conflict);
    };
    Ok(EconomicSettlement {
        receipt_id: stored.receipt_id,
        meter_event_id,
        rated_usage_id,
        customer_ledger_transaction_id,
        outcome: receipt.outcome,
    })
}

fn validate_stored_receipt(
    stored: &StoredReceipt,
    receipt: &EconomicReceipt,
) -> Result<(), EconomicSettlementError> {
    if stored.outcome == receipt.outcome.as_str()
        && stored.receipt_schema == receipt.receipt_schema
        && stored.payload_hash == receipt.payload_hash
        && stored.evidence == receipt.evidence
        && stored.provider_cost_micros.is_none()
        && stored.provider_cost_currency.is_none()
    {
        Ok(())
    } else {
        Err(EconomicSettlementError::Conflict)
    }
}

async fn validate_v4_provider_cost_ledger(
    tx: &mut Transaction<'_, Postgres>,
    receipt_id: Uuid,
) -> Result<(), EconomicSettlementError> {
    let legacy_transaction_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT transaction.transaction_id
        FROM ledger_transactions transaction
        WHERE transaction.source_receipt_id = $1
          AND transaction.transaction_type = 'provider_cost'
          AND transaction.source_provider_cost_observation_id IS NULL
          AND transaction.source_provider_cost_allocation_line_id IS NULL
        "#,
    )
    .bind(receipt_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(economic_unavailable)?;
    if legacy_transaction_id.is_none() {
        Ok(())
    } else {
        Err(EconomicSettlementError::Conflict)
    }
}

async fn settle_hold_and_account(
    tx: &mut Transaction<'_, Postgres>,
    submission: &SubmissionIdentity,
    output: &LockedEconomicOutput,
    captured_micros: i64,
    now: i64,
) -> Result<(), EconomicSettlementError> {
    let released_micros = output
        .held_micros
        .checked_sub(captured_micros)
        .ok_or(EconomicSettlementError::Conflict)?;
    let hold_changed = sqlx::query(
        r#"
        UPDATE output_holds
        SET state = 'settled', captured_micros = $2, released_micros = $3,
            updated_at_ms = $4
        WHERE output_id = $1 AND state = 'held'
        "#,
    )
    .bind(submission.output_id)
    .bind(captured_micros)
    .bind(released_micros)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(economic_unavailable)?
    .rows_affected();
    let account_changed = sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = held_micros - $3,
            captured_micros = captured_micros + $4,
            updated_at_ms = $5
        WHERE tenant_id = $1 AND currency = $2 AND held_micros >= $3
        "#,
    )
    .bind(&submission.tenant_id)
    .bind(&output.currency)
    .bind(output.held_micros)
    .bind(captured_micros)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(economic_unavailable)?
    .rows_affected();
    if hold_changed == 1 && account_changed == 1 {
        Ok(())
    } else {
        Err(EconomicSettlementError::Conflict)
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_ledger_pair(
    tx: &mut Transaction<'_, Postgres>,
    semantic_key: &str,
    output_id: Uuid,
    job_id: Uuid,
    submission_id: Uuid,
    receipt_id: Uuid,
    transaction_type: &str,
    currency: &str,
    amount_micros: i64,
    debit_key: &str,
    debit_owner_type: &str,
    debit_owner_id: &str,
    debit_type: &str,
    credit_key: &str,
    credit_owner_type: &str,
    credit_owner_id: &str,
    credit_type: &str,
    now: i64,
) -> Result<Uuid, EconomicSettlementError> {
    let debit_id = ensure_ledger_account(
        tx,
        debit_key,
        debit_owner_type,
        debit_owner_id,
        debit_type,
        currency,
        now,
    )
    .await?;
    let credit_id = ensure_ledger_account(
        tx,
        credit_key,
        credit_owner_type,
        credit_owner_id,
        credit_type,
        currency,
        now,
    )
    .await?;
    let transaction_id = Uuid::new_v4();
    let payload_hash = hex::encode(Sha256::digest(
        format!("{semantic_key}:{currency}:{amount_micros}").as_bytes(),
    ));
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions
          (transaction_id, semantic_key, source_output_id, source_job_id,
           source_submission_id, source_receipt_id,
           transaction_type, currency, payload_hash, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(transaction_id)
    .bind(semantic_key)
    .bind(output_id)
    .bind(job_id)
    .bind(submission_id)
    .bind(receipt_id)
    .bind(transaction_type)
    .bind(currency)
    .bind(payload_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(economic_unavailable)?;
    for (posting_no, account_id, amount) in [
        (1_i16, debit_id, amount_micros),
        (2_i16, credit_id, -amount_micros),
    ] {
        sqlx::query(
            r#"
            INSERT INTO ledger_postings
              (transaction_id, posting_no, account_id, currency, amount_micros, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(transaction_id)
        .bind(posting_no)
        .bind(account_id)
        .bind(currency)
        .bind(amount)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(economic_unavailable)?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, $2)",
    )
    .bind(transaction_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(economic_unavailable)?;
    Ok(transaction_id)
}

async fn ensure_ledger_account(
    tx: &mut Transaction<'_, Postgres>,
    account_key: &str,
    owner_type: &str,
    owner_id: &str,
    account_type: &str,
    currency: &str,
    now: i64,
) -> Result<Uuid, EconomicSettlementError> {
    let candidate = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO ledger_accounts
          (account_id, account_key, owner_type, owner_id, account_type, currency, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (account_key) DO NOTHING
        "#,
    )
    .bind(candidate)
    .bind(account_key)
    .bind(owner_type)
    .bind(owner_id)
    .bind(account_type)
    .bind(currency)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(economic_unavailable)?;
    let row: Option<(Uuid, String, String, String, String)> = sqlx::query_as(
        r#"
        SELECT account_id, owner_type, owner_id, account_type, currency
        FROM ledger_accounts WHERE account_key = $1
        "#,
    )
    .bind(account_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(economic_unavailable)?;
    match row {
        Some((id, stored_owner_type, stored_owner_id, stored_type, stored_currency))
            if stored_owner_type == owner_type
                && stored_owner_id == owner_id
                && stored_type == account_type
                && stored_currency == currency =>
        {
            Ok(id)
        }
        _ => Err(EconomicSettlementError::Conflict),
    }
}

fn receipt_semantic_key(receipt: &EconomicReceipt) -> String {
    let value = format!(
        "{}:{}:{}",
        receipt.submission_id,
        receipt.outcome.as_str(),
        receipt.payload_hash
    );
    format!("receipt:{}", hex::encode(Sha256::digest(value.as_bytes())))
}

async fn economic_database_now(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<i64, EconomicSettlementError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(economic_unavailable)
}

fn economic_unavailable(_: sqlx::Error) -> EconomicSettlementError {
    EconomicSettlementError::Unavailable
}

fn unavailable(_: sqlx::Error) -> AdmissionError {
    AdmissionError::Unavailable
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locked_job(
        output_count: i32,
        billable_units: i32,
        billing_metric: &str,
        billing_unit: &str,
        economics_contract_version: i16,
    ) -> LockedJob {
        LockedJob {
            tenant_id: "tenant-a".to_owned(),
            operation: "generation".to_owned(),
            provider_id: "provider-a".to_owned(),
            model: "model-a".to_owned(),
            requested_units: billable_units,
            output_count,
            billable_units,
            billing_metric: billing_metric.to_owned(),
            billing_unit: billing_unit.to_owned(),
            economics_contract_version,
        }
    }

    fn price(billing_metric: &str, billing_unit: &str) -> PriceVersion {
        PriceVersion {
            price_version_id: Uuid::from_u128(1),
            billing_metric: billing_metric.to_owned(),
            billing_unit: billing_unit.to_owned(),
            currency: "USD".to_owned(),
            success_micros: 7,
            failed_micros: 3,
            no_effect_micros: 0,
        }
    }

    #[test]
    fn billing_dimensions_keep_output_cardinality_separate_from_quantity() {
        let image = locked_job(3, 3, OUTPUT_BILLING_METRIC, OUTPUT_BILLING_UNIT, 2);
        let video = locked_job(
            1,
            6,
            VIDEO_SECOND_BILLING_METRIC,
            VIDEO_SECOND_BILLING_UNIT,
            3,
        );

        let image_dimension = BillingDimension::from_job(&image).unwrap();
        let video_dimension = BillingDimension::from_job(&video).unwrap();
        assert_eq!(image_dimension.contract_version(), 2);
        assert_eq!(
            image_dimension.output_billable_units(image.billable_units),
            1
        );
        assert_eq!(video_dimension.contract_version(), 3);
        assert_eq!(
            video_dimension.output_billable_units(video.billable_units),
            6
        );

        let invalid = locked_job(
            6,
            6,
            VIDEO_SECOND_BILLING_METRIC,
            VIDEO_SECOND_BILLING_UNIT,
            3,
        );
        assert!(BillingDimension::from_job(&invalid).is_err());
    }

    #[test]
    fn quote_hash_binds_metric_unit_cardinality_and_billable_quantity() {
        let image = locked_job(6, 6, OUTPUT_BILLING_METRIC, OUTPUT_BILLING_UNIT, 2);
        let video_six = locked_job(
            1,
            6,
            VIDEO_SECOND_BILLING_METRIC,
            VIDEO_SECOND_BILLING_UNIT,
            3,
        );
        let video_ten = locked_job(
            1,
            10,
            VIDEO_SECOND_BILLING_METRIC,
            VIDEO_SECOND_BILLING_UNIT,
            3,
        );
        let job_id = Uuid::from_u128(2);

        let image_hash = quote_hash(
            job_id,
            &price(OUTPUT_BILLING_METRIC, OUTPUT_BILLING_UNIT),
            &image,
            42,
        );
        let video_six_hash = quote_hash(
            job_id,
            &price(VIDEO_SECOND_BILLING_METRIC, VIDEO_SECOND_BILLING_UNIT),
            &video_six,
            42,
        );
        let video_ten_hash = quote_hash(
            job_id,
            &price(VIDEO_SECOND_BILLING_METRIC, VIDEO_SECOND_BILLING_UNIT),
            &video_ten,
            70,
        );

        assert_ne!(image_hash, video_six_hash);
        assert_ne!(video_six_hash, video_ten_hash);
    }

    #[test]
    fn settlement_capture_uses_the_outputs_billable_weight() {
        let video = LockedEconomicOutput {
            economics_contract_version: 3,
            output_state: "running".to_owned(),
            billable_units: 6,
            hold_state: "held".to_owned(),
            held_micros: 42,
            quote_id: Uuid::from_u128(3),
            currency: "USD".to_owned(),
            billing_metric: VIDEO_SECOND_BILLING_METRIC.to_owned(),
            billing_unit: VIDEO_SECOND_BILLING_UNIT.to_owned(),
            quote_billable_units: 6,
            success_micros: 7,
            failed_micros: 3,
            no_effect_micros: 0,
        };

        assert!(validate_locked_output(&video).is_ok());
        let wrong_contract = LockedEconomicOutput {
            economics_contract_version: 2,
            ..video
        };
        assert!(validate_locked_output(&wrong_contract).is_err());
    }
}
