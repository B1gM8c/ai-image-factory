use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{Acquire, PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    EconomicReceipt, EconomicReceiptOutcome, EconomicSettlement, EconomicSettlementError,
    EconomicSettlementStore, evidence_hash,
};
use crate::admission::{AdmissionError, AttachJob};

const MAX_IMAGE_OUTPUTS: i32 = 10;

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
    economics_contract_version: i16,
}

#[derive(sqlx::FromRow)]
struct PriceVersion {
    price_version_id: Uuid,
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
    success_micros: i64,
    failed_micros: i64,
    no_effect_micros: i64,
    max_total_micros: i64,
    quote_hash: String,
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
    output_state: String,
    hold_state: String,
    held_micros: i64,
    quote_id: Uuid,
    currency: String,
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
    validate_output_count(&job, request)?;
    if job.economics_contract_version == 2 {
        return validate_locked_economics(tx, request.job_id, &job).await;
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
        .checked_mul(i64::from(job.requested_units))
        .ok_or(AdmissionError::InvalidCommand)?;
    let quote_id = Uuid::new_v4();
    let quote_hash = quote_hash(
        request.job_id,
        &price,
        job.requested_units,
        max_total_micros,
    );

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("budget:{}:{}", job.tenant_id, price.currency))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts
          (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 0, 0, 0, $3, $3)
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
          AND (held_micros::NUMERIC + captured_micros::NUMERIC + $3::NUMERIC)
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
          (quote_id, job_id, price_version_id, currency, output_count,
           success_micros, failed_micros, no_effect_micros, max_total_micros,
           quote_hash, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(quote_id)
    .bind(request.job_id)
    .bind(price.price_version_id)
    .bind(&price.currency)
    .bind(job.requested_units)
    .bind(price.success_micros)
    .bind(price.failed_micros)
    .bind(price.no_effect_micros)
    .bind(max_total_micros)
    .bind(&quote_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;

    for output_index in 0..job.requested_units {
        let output_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO job_outputs
              (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, 'pending', $4, $4)
            "#,
        )
        .bind(output_id)
        .bind(request.job_id)
        .bind(output_index)
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
        .bind(max_unit_micros)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }

    let changed = sqlx::query(
        "UPDATE jobs SET economics_contract_version = 2, updated_at_ms = $2 WHERE job_id = $1 AND economics_contract_version = 1",
    )
    .bind(request.job_id)
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
    validate_output_count(&job, request)?;
    if job.economics_contract_version != 2 {
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

fn validate_output_count(job: &LockedJob, request: &AttachJob) -> Result<(), AdmissionError> {
    if !(1..=MAX_IMAGE_OUTPUTS).contains(&job.requested_units) {
        return Err(AdmissionError::InvalidCommand);
    }
    if let Some(command_count) = request.command_json.get("n") {
        let command_count = command_count
            .as_u64()
            .and_then(|value| i32::try_from(value).ok())
            .ok_or(AdmissionError::InvalidCommand)?;
        if command_count != job.requested_units {
            return Err(AdmissionError::InvalidCommand);
        }
    }
    Ok(())
}

async fn select_price(
    tx: &mut Transaction<'_, Postgres>,
    api_profile: &str,
    job: &LockedJob,
) -> Result<PriceVersion, AdmissionError> {
    sqlx::query_as(
        r#"
        SELECT price_version_id, currency, success_micros, failed_micros, no_effect_micros
        FROM price_versions
        WHERE state = 'active'
          AND api_profile IN ($1, '*')
          AND operation IN ($2, '*')
          AND provider_id IN ($3, '*')
          AND model IN ($4, '*')
        ORDER BY
          ((api_profile <> '*')::INT + (operation <> '*')::INT
           + (provider_id <> '*')::INT + (model <> '*')::INT) DESC,
          version DESC, price_version_id
        LIMIT 1
        FOR SHARE
        "#,
    )
    .bind(api_profile)
    .bind(&job.operation)
    .bind(&job.provider_id)
    .bind(&job.model)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(AdmissionError::PricingUnavailable)
}

async fn validate_locked_economics(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    job: &LockedJob,
) -> Result<(), AdmissionError> {
    let quote: FrozenQuote = sqlx::query_as(
        r#"
        SELECT quote_id, price_version_id, currency, output_count, success_micros,
               failed_micros, no_effect_micros, max_total_micros, quote_hash
        FROM price_quotes
        WHERE job_id = $1
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(AdmissionError::InvalidOwner)?;
    let expected_hash = quote_hash(
        job_id,
        &PriceVersion {
            price_version_id: quote.price_version_id,
            currency: quote.currency.clone(),
            success_micros: quote.success_micros,
            failed_micros: quote.failed_micros,
            no_effect_micros: quote.no_effect_micros,
        },
        quote.output_count,
        quote.max_total_micros,
    );
    if quote.output_count != job.requested_units || quote.quote_hash != expected_hash {
        return Err(AdmissionError::InvalidOwner);
    }
    let (output_count, min_index, max_index, hold_count, held_total): (
        i64,
        Option<i32>,
        Option<i32>,
        i64,
        i64,
    ) = sqlx::query_as(
        r#"
        SELECT COUNT(*)::BIGINT, MIN(o.output_index), MAX(o.output_index),
               COUNT(h.output_id)::BIGINT, COALESCE(SUM(h.held_micros), 0)::BIGINT
        FROM job_outputs o
        LEFT JOIN output_holds h
          ON h.output_id = o.output_id AND h.job_id = o.job_id AND h.quote_id = $2
        WHERE o.job_id = $1
        "#,
    )
    .bind(job_id)
    .bind(quote.quote_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if output_count != i64::from(job.requested_units)
        || hold_count != output_count
        || min_index != Some(0)
        || max_index != Some(job.requested_units - 1)
        || held_total != quote.max_total_micros
    {
        return Err(AdmissionError::InvalidOwner);
    }
    Ok(())
}

fn quote_hash(
    job_id: Uuid,
    price: &PriceVersion,
    output_count: i32,
    max_total_micros: i64,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        job_id.to_string(),
        price.price_version_id.to_string(),
        price.currency.clone(),
        output_count.to_string(),
        price.success_micros.to_string(),
        price.failed_micros.to_string(),
        price.no_effect_micros.to_string(),
        max_total_micros.to_string(),
    ] {
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
        sqlx::query_scalar("SELECT job_id FROM jobs WHERE job_id = $1 FOR UPDATE")
            .bind(snapshot.job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(economic_unavailable)?;
    if locked_job.is_none() {
        return Err(EconomicSettlementError::Conflict);
    }
    let output: LockedEconomicOutput = sqlx::query_as(
        r#"
            SELECT o.state AS output_state, h.state AS hold_state, h.held_micros,
                   q.quote_id, q.currency, q.success_micros, q.failed_micros,
                   q.no_effect_micros
            FROM job_outputs o
            JOIN output_holds h ON h.output_id = o.output_id AND h.job_id = o.job_id
            JOIN price_quotes q ON q.quote_id = h.quote_id AND q.job_id = o.job_id
            WHERE o.output_id = $1 AND o.job_id = $2
            FOR UPDATE OF o, h
            "#,
    )
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(economic_unavailable)?
    .ok_or(EconomicSettlementError::Conflict)?;
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
    .bind(
        receipt
            .provider_cost
            .as_ref()
            .map(|cost| cost.amount_micros),
    )
    .bind(receipt.provider_cost.as_ref().map(|cost| &cost.currency))
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
    let quantity = i64::from(receipt.outcome != EconomicReceiptOutcome::Uncertain);
    sqlx::query(
        r#"
            INSERT INTO economic_metering_events
              (meter_event_id, semantic_key, output_id, job_id, submission_id, receipt_id,
               fact_kind, metric, quantity, unit, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'image_output', $8, 'output', $9, $10)
            "#,
    )
    .bind(meter_event_id)
    .bind(format!("meter:{semantic_key}"))
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .bind(receipt.submission_id)
    .bind(receipt_id)
    .bind(fact_kind)
    .bind(quantity)
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
    if unit_price_micros > output.held_micros {
        return Err(EconomicSettlementError::Conflict);
    }
    let rated_usage_id = Uuid::new_v4();
    sqlx::query(
        r#"
            INSERT INTO rated_usage
              (rated_usage_id, semantic_key, meter_event_id, output_id, job_id, quote_id,
               outcome, quantity, unit_price_micros, amount_micros, currency, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $8, $9, $10)
            "#,
    )
    .bind(rated_usage_id)
    .bind(format!("rating:{semantic_key}"))
    .bind(meter_event_id)
    .bind(snapshot.output_id)
    .bind(snapshot.job_id)
    .bind(output.quote_id)
    .bind(receipt.outcome.as_str())
    .bind(unit_price_micros)
    .bind(&output.currency)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(economic_unavailable)?;
    settle_hold_and_account(&mut tx, &snapshot, &output, unit_price_micros, now).await?;
    let customer_ledger_transaction_id = if unit_price_micros == 0 {
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
                unit_price_micros,
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
    if let Some(cost) = &receipt.provider_cost
        && cost.amount_micros > 0
    {
        insert_ledger_pair(
            &mut tx,
            &format!("provider-cost:{semantic_key}"),
            snapshot.output_id,
            snapshot.job_id,
            receipt.submission_id,
            receipt_id,
            "provider_cost",
            &cost.currency,
            cost.amount_micros,
            &format!("platform:{}:provider-expense", cost.currency),
            "platform",
            "platform",
            "expense",
            &format!(
                "provider:{}:{}:payable",
                snapshot.provider_id, cost.currency
            ),
            "provider",
            &snapshot.provider_id,
            "payable",
            now,
        )
        .await?;
    }
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
    let cost_valid = receipt.provider_cost.as_ref().is_none_or(|cost| {
        cost.amount_micros >= 0
            && cost.currency.len() == 3
            && cost.currency.bytes().all(|byte| byte.is_ascii_uppercase())
    });
    let evidence_hash_matches =
        evidence_hash(&receipt.evidence).is_ok_and(|expected| expected == receipt.payload_hash);
    if schema_valid
        && hash_valid
        && evidence_hash_matches
        && receipt.evidence.is_object()
        && cost_valid
    {
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
    let stored_cost = stored
        .provider_cost_micros
        .zip(stored.provider_cost_currency.clone());
    let requested_cost = receipt
        .provider_cost
        .as_ref()
        .map(|cost| (cost.amount_micros, cost.currency.clone()));
    if stored.outcome != receipt.outcome.as_str()
        || stored.receipt_schema != receipt.receipt_schema
        || stored.payload_hash != receipt.payload_hash
        || stored.evidence != receipt.evidence
        || stored_cost != requested_cost
    {
        return Err(EconomicSettlementError::Conflict);
    }
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
