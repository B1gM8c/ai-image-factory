use std::collections::BTreeMap;

use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::credit_grants::settle_credit_grant_reservations;

use super::{
    FrozenQuoteLine, FrozenQuotePlan, FrozenRatingPlan, UsageFact, rate_frozen_customer_quote,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredCustomerRating {
    pub rated_usage_id: Uuid,
    pub job_id: Uuid,
    pub currency: String,
    pub total_amount_micros: i64,
    pub rating_hash: String,
    pub ledger_transaction_id: Option<Uuid>,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub(crate) enum CustomerRatingStoreError {
    #[error("customer rating input is invalid")]
    InvalidInput,
    #[error("customer rating conflicts with immutable economic facts")]
    Conflict,
    #[error("customer rating storage is unavailable")]
    Unavailable,
}

#[derive(sqlx::FromRow)]
struct QuoteRow {
    quote_id: Uuid,
    price_book_id: Uuid,
    price_book_version_id: Uuid,
    currency: String,
    is_free: bool,
    max_total_micros: i64,
    quote_hash: String,
}

#[derive(sqlx::FromRow)]
struct QuoteLineRow {
    quote_line_id: Uuid,
    price_component_id: Uuid,
    component_key: String,
    partition_key: String,
    terminal_outcome: String,
    metric: String,
    unit: String,
    unit_size: i64,
    unit_price_micros: i64,
    rate_adjustment_numerator: i64,
    rate_adjustment_denominator: i64,
    reservation_quantity_source: String,
    reservation_confidence: String,
    quantity_source: String,
    required_confidence: String,
    rounding_mode: String,
    dimensions_json: Value,
    max_quantity: i64,
    max_amount_micros: i64,
}

#[derive(sqlx::FromRow)]
struct UsageFactRow {
    usage_fact_id: Uuid,
    output_id: Uuid,
    billing_partition_key: String,
    provider_id: String,
    provider_account_id: Option<Uuid>,
    execution_surface: String,
    fact_domain: String,
    metric: String,
    unit: String,
    quantity: i64,
    terminal_outcome: String,
    quantity_source: String,
    confidence: String,
    metadata_json: Value,
}

#[derive(sqlx::FromRow)]
struct HoldRow {
    hold_id: Uuid,
    held_micros: i64,
    captured_micros: i64,
    released_micros: i64,
    grant_held_micros: i64,
    account_held_micros: i64,
    grant_captured_micros: i64,
    account_captured_micros: i64,
    grant_released_micros: i64,
    account_released_micros: i64,
    state: String,
}

#[derive(sqlx::FromRow)]
struct ExistingRatingRow {
    rated_usage_id: Uuid,
    job_id: Uuid,
    total_amount_micros: i64,
    currency: String,
    fact_set_hash: String,
    rating_hash: String,
}

#[derive(sqlx::FromRow)]
struct ExistingLedgerRow {
    transaction_id: Uuid,
    posting_count: i64,
    posting_sum_micros: i64,
    receivable_micros: Option<i64>,
    revenue_micros: Option<i64>,
    is_sealed: bool,
}

pub(crate) async fn settle_customer_quote(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    tenant_id: &str,
) -> Result<StoredCustomerRating, CustomerRatingStoreError> {
    sqlx::query("SAVEPOINT settle_customer_quote")
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    let result = settle_customer_quote_inner(tx, job_id, tenant_id).await;
    match result {
        Ok(stored) => {
            sqlx::query("RELEASE SAVEPOINT settle_customer_quote")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            Ok(stored)
        }
        Err(error) => {
            sqlx::query("ROLLBACK TO SAVEPOINT settle_customer_quote")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            sqlx::query("RELEASE SAVEPOINT settle_customer_quote")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            Err(error)
        }
    }
}

async fn settle_customer_quote_inner(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    tenant_id: &str,
) -> Result<StoredCustomerRating, CustomerRatingStoreError> {
    if job_id.is_nil() || tenant_id.trim().is_empty() {
        return Err(CustomerRatingStoreError::InvalidInput);
    }
    let currency: String = sqlx::query_scalar(
        "SELECT currency FROM customer_price_quotes WHERE job_id = $1 AND tenant_id = $2",
    )
    .bind(job_id)
    .bind(tenant_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(CustomerRatingStoreError::Conflict)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("budget:{tenant_id}:{currency}"))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;

    let contract_version: i16 = sqlx::query_scalar(
        "SELECT economics_contract_version FROM jobs WHERE job_id = $1 AND tenant_id = $2 FOR UPDATE",
    )
    .bind(job_id)
    .bind(tenant_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(CustomerRatingStoreError::Conflict)?;
    if contract_version != 4 {
        return Err(CustomerRatingStoreError::Conflict);
    }

    let quote: QuoteRow = sqlx::query_as(
        r#"
        SELECT quote_id, price_book_id, price_book_version_id, currency,
               is_free, max_total_micros, quote_hash
        FROM customer_price_quotes
        WHERE job_id = $1 AND tenant_id = $2
        FOR SHARE
        "#,
    )
    .bind(job_id)
    .bind(tenant_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if quote.currency != currency {
        return Err(CustomerRatingStoreError::Conflict);
    }
    let quote_lines: Vec<QuoteLineRow> = sqlx::query_as(
        r#"
        SELECT quote_line_id, price_component_id, component_key,
               partition_key, terminal_outcome, metric, unit, unit_size,
               unit_price_micros, rate_adjustment_numerator,
               rate_adjustment_denominator, reservation_quantity_source,
               reservation_confidence, quantity_source, required_confidence,
               rounding_mode, dimensions_json, max_quantity,
               max_amount_micros
        FROM customer_price_quote_lines
        WHERE quote_id = $1 AND job_id = $2
        ORDER BY partition_key, terminal_outcome, component_key,
                 price_component_id
        FOR SHARE
        "#,
    )
    .bind(quote.quote_id)
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    if quote_lines.is_empty() {
        return Err(CustomerRatingStoreError::Conflict);
    }
    let frozen_quote = FrozenQuotePlan {
        price_book_id: quote.price_book_id,
        price_book_version_id: quote.price_book_version_id,
        currency: quote.currency.clone(),
        is_free: quote.is_free,
        max_total_micros: quote.max_total_micros.to_string(),
        quote_hash: quote.quote_hash.clone(),
        lines: quote_lines.iter().map(QuoteLineRow::frozen).collect(),
    };
    let usage_rows: Vec<UsageFactRow> = sqlx::query_as(
        r#"
        SELECT usage_fact_id, output_id, billing_partition_key, provider_id,
               provider_account_id, execution_surface, fact_domain, metric, unit,
               quantity, terminal_outcome, quantity_source, confidence,
               metadata_json
        FROM provider_usage_facts
        WHERE job_id = $1 AND fact_domain = 'customer_billable'
        ORDER BY usage_fact_id
        FOR SHARE
        "#,
    )
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    let facts = usage_rows
        .into_iter()
        .map(UsageFactRow::usage_fact)
        .collect::<Vec<_>>();
    let rating = rate_frozen_customer_quote(&frozen_quote, &facts)
        .map_err(|_| CustomerRatingStoreError::Conflict)?;
    let total_amount_micros = parse_i64(&rating.total_amount_micros)?;

    let hold: HoldRow = sqlx::query_as(
        r#"
        SELECT hold_id, held_micros, captured_micros, released_micros,
               grant_held_micros, account_held_micros,
               grant_captured_micros, account_captured_micros,
               grant_released_micros, account_released_micros,
               state
        FROM customer_billing_holds
        WHERE quote_id = $1 AND job_id = $2 AND tenant_id = $3
        FOR UPDATE
        "#,
    )
    .bind(quote.quote_id)
    .bind(job_id)
    .bind(tenant_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if let Some(existing) = load_existing_rating(tx, quote.quote_id, job_id).await? {
        return validate_replay(
            tx,
            tenant_id,
            &quote,
            &hold,
            &rating,
            total_amount_micros,
            existing,
        )
        .await;
    }
    if hold.state != "held" || total_amount_micros > hold.held_micros {
        return Err(CustomerRatingStoreError::Conflict);
    }

    let now = database_now(tx).await?;
    let funding = settle_credit_grant_reservations(
        tx,
        hold.hold_id,
        tenant_id,
        &quote.currency,
        hold.grant_held_micros,
        total_amount_micros,
        now,
    )
    .await
    .map_err(map_credit_grant)?;
    let rated_usage_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO customer_rated_usage (
            rated_usage_id, semantic_key, quote_id, job_id, fact_set_hash,
            total_amount_micros, currency, rating_hash, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(rated_usage_id)
    .bind(format!(
        "customer-rating:v4:{job_id}:{}",
        rating.fact_set_hash
    ))
    .bind(quote.quote_id)
    .bind(job_id)
    .bind(&rating.fact_set_hash)
    .bind(total_amount_micros)
    .bind(&quote.currency)
    .bind(&rating.rating_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_conflict)?;

    let quote_line_ids = quote_lines
        .iter()
        .map(|line| (line.identity_key(), line.quote_line_id))
        .collect::<BTreeMap<_, _>>();
    for line in &rating.lines {
        let quote_line_id = quote_line_ids
            .get(&(
                line.partition_key.clone(),
                line.terminal_outcome.clone(),
                line.component_key.clone(),
                line.price_component_id,
            ))
            .copied()
            .ok_or(CustomerRatingStoreError::Conflict)?;
        let rated_usage_line_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO customer_rated_usage_lines (
                rated_usage_line_id, rated_usage_id, quote_id, job_id,
                quote_line_id, actual_quantity, amount_micros, created_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(rated_usage_line_id)
        .bind(rated_usage_id)
        .bind(quote.quote_id)
        .bind(job_id)
        .bind(quote_line_id)
        .bind(parse_i64(&line.actual_quantity)?)
        .bind(parse_i64(&line.amount_micros)?)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(map_conflict)?;
        for usage_fact_id in &line.usage_fact_ids {
            sqlx::query(
                r#"
                INSERT INTO customer_rated_usage_fact_links (
                    rated_usage_line_id, usage_fact_id, linked_at_ms
                )
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(rated_usage_line_id)
            .bind(usage_fact_id)
            .bind(now)
            .execute(&mut **tx)
            .await
            .map_err(map_conflict)?;
        }
    }

    let released_micros = hold
        .held_micros
        .checked_sub(total_amount_micros)
        .ok_or(CustomerRatingStoreError::Conflict)?;
    let grant_released_micros = hold
        .grant_held_micros
        .checked_sub(funding.grant_micros)
        .ok_or(CustomerRatingStoreError::Conflict)?;
    let account_released_micros = hold
        .account_held_micros
        .checked_sub(funding.account_micros)
        .ok_or(CustomerRatingStoreError::Conflict)?;
    let account_changed = sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = held_micros - $3,
            captured_micros = captured_micros + $4,
            updated_at_ms = $5
        WHERE tenant_id = $1 AND currency = $2
          AND held_micros >= $3
        "#,
    )
    .bind(tenant_id)
    .bind(&quote.currency)
    .bind(hold.account_held_micros)
    .bind(funding.account_micros)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    let hold_changed = sqlx::query(
        r#"
        UPDATE customer_billing_holds
        SET captured_micros = $2, released_micros = $3,
            grant_captured_micros = $4,
            account_captured_micros = $5,
            grant_released_micros = $6,
            account_released_micros = $7,
            state = 'settled', updated_at_ms = $8
        WHERE hold_id = $1 AND state = 'held'
          AND captured_micros = 0 AND released_micros = 0
          AND grant_captured_micros = 0
          AND account_captured_micros = 0
          AND grant_released_micros = 0
          AND account_released_micros = 0
        "#,
    )
    .bind(hold.hold_id)
    .bind(total_amount_micros)
    .bind(released_micros)
    .bind(funding.grant_micros)
    .bind(funding.account_micros)
    .bind(grant_released_micros)
    .bind(account_released_micros)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_conflict)?
    .rows_affected();
    if account_changed != 1 || hold_changed != 1 {
        return Err(CustomerRatingStoreError::Conflict);
    }

    let ledger_transaction_id = if total_amount_micros == 0 {
        None
    } else {
        Some(
            insert_job_ledger_pair(
                tx,
                job_id,
                tenant_id,
                &quote.currency,
                total_amount_micros,
                &rating.rating_hash,
                now,
            )
            .await?,
        )
    };
    validate_deferred_contracts(tx).await?;
    Ok(StoredCustomerRating {
        rated_usage_id,
        job_id,
        currency: quote.currency,
        total_amount_micros,
        rating_hash: rating.rating_hash,
        ledger_transaction_id,
    })
}

async fn load_existing_rating(
    tx: &mut Transaction<'_, Postgres>,
    quote_id: Uuid,
    job_id: Uuid,
) -> Result<Option<ExistingRatingRow>, CustomerRatingStoreError> {
    sqlx::query_as(
        r#"
        SELECT rated_usage_id, job_id, total_amount_micros, currency,
               fact_set_hash, rating_hash
        FROM customer_rated_usage
        WHERE quote_id = $1 AND job_id = $2
        FOR SHARE
        "#,
    )
    .bind(quote_id)
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

#[allow(clippy::too_many_arguments)]
async fn validate_replay(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    quote: &QuoteRow,
    hold: &HoldRow,
    rating: &FrozenRatingPlan,
    total_amount_micros: i64,
    existing: ExistingRatingRow,
) -> Result<StoredCustomerRating, CustomerRatingStoreError> {
    let expected_released_micros = hold
        .held_micros
        .checked_sub(total_amount_micros)
        .ok_or(CustomerRatingStoreError::Conflict)?;
    let expected_grant_captured_micros = hold.grant_held_micros.min(total_amount_micros);
    let expected_account_captured_micros = total_amount_micros
        .checked_sub(expected_grant_captured_micros)
        .ok_or(CustomerRatingStoreError::Conflict)?;
    let expected_grant_released_micros = hold
        .grant_held_micros
        .checked_sub(expected_grant_captured_micros)
        .ok_or(CustomerRatingStoreError::Conflict)?;
    let expected_account_released_micros = hold
        .account_held_micros
        .checked_sub(expected_account_captured_micros)
        .ok_or(CustomerRatingStoreError::Conflict)?;
    if existing.total_amount_micros != total_amount_micros
        || existing.currency != quote.currency
        || existing.fact_set_hash != rating.fact_set_hash
        || existing.rating_hash != rating.rating_hash
        || hold.state != "settled"
        || hold.captured_micros != total_amount_micros
        || hold.released_micros != expected_released_micros
        || hold.grant_captured_micros != expected_grant_captured_micros
        || hold.account_captured_micros != expected_account_captured_micros
        || hold.grant_released_micros != expected_grant_released_micros
        || hold.account_released_micros != expected_account_released_micros
    {
        return Err(CustomerRatingStoreError::Conflict);
    }
    let ledger: Option<ExistingLedgerRow> = sqlx::query_as(
        r#"
        SELECT transaction.transaction_id,
               COUNT(posting.posting_no)::BIGINT AS posting_count,
               COALESCE(SUM(posting.amount_micros), 0)::BIGINT
                   AS posting_sum_micros,
               MAX(posting.amount_micros) FILTER (
                   WHERE account.account_key = $2
               ) AS receivable_micros,
               MIN(posting.amount_micros) FILTER (
                   WHERE account.account_key = $3
               ) AS revenue_micros,
               BOOL_OR(seal.transaction_id IS NOT NULL) AS is_sealed
        FROM ledger_transactions transaction
        LEFT JOIN ledger_postings posting
          ON posting.transaction_id = transaction.transaction_id
        LEFT JOIN ledger_accounts account
          ON account.account_id = posting.account_id
         AND account.currency = posting.currency
        LEFT JOIN ledger_transaction_seals seal
          ON seal.transaction_id = transaction.transaction_id
        WHERE transaction.source_job_id = $1
          AND transaction.transaction_type = 'customer_job_charge'
        GROUP BY transaction.transaction_id
        "#,
    )
    .bind(existing.job_id)
    .bind(format!("tenant:{tenant_id}:{}:receivable", quote.currency))
    .bind(format!("platform:{}:revenue", quote.currency))
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let ledger_transaction_id = match (total_amount_micros, ledger) {
        (0, None) => None,
        (0, Some(_)) | (_, None) => return Err(CustomerRatingStoreError::Conflict),
        (amount, Some(ledger))
            if ledger.posting_count == 2
                && ledger.posting_sum_micros == 0
                && ledger.receivable_micros == Some(amount)
                && ledger.revenue_micros == Some(-amount)
                && ledger.is_sealed =>
        {
            Some(ledger.transaction_id)
        }
        (_, Some(_)) => return Err(CustomerRatingStoreError::Conflict),
    };
    let account_exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM billing_accounts
            WHERE tenant_id = $1 AND currency = $2
        )
        "#,
    )
    .bind(tenant_id)
    .bind(&quote.currency)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if !account_exists {
        return Err(CustomerRatingStoreError::Conflict);
    }
    Ok(StoredCustomerRating {
        rated_usage_id: existing.rated_usage_id,
        job_id: existing.job_id,
        currency: existing.currency,
        total_amount_micros,
        rating_hash: existing.rating_hash,
        ledger_transaction_id,
    })
}

async fn insert_job_ledger_pair(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    tenant_id: &str,
    currency: &str,
    amount_micros: i64,
    rating_hash: &str,
    now: i64,
) -> Result<Uuid, CustomerRatingStoreError> {
    let debit_id = ensure_ledger_account(
        tx,
        &format!("tenant:{tenant_id}:{currency}:receivable"),
        "tenant",
        tenant_id,
        "receivable",
        currency,
        now,
    )
    .await?;
    let credit_id = ensure_ledger_account(
        tx,
        &format!("platform:{currency}:revenue"),
        "platform",
        "platform",
        "revenue",
        currency,
        now,
    )
    .await?;
    let transaction_id = Uuid::new_v4();
    let semantic_key = format!("customer-job-charge:v4:{job_id}:{rating_hash}");
    let payload_hash = hex::encode(Sha256::digest(
        format!("{semantic_key}:{currency}:{amount_micros}").as_bytes(),
    ));
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions (
            transaction_id, semantic_key, source_job_id, transaction_type,
            currency, payload_hash, created_at_ms
        )
        VALUES ($1, $2, $3, 'customer_job_charge', $4, $5, $6)
        "#,
    )
    .bind(transaction_id)
    .bind(semantic_key)
    .bind(job_id)
    .bind(currency)
    .bind(payload_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_conflict)?;
    for (posting_no, account_id, amount) in [
        (1_i16, debit_id, amount_micros),
        (2_i16, credit_id, -amount_micros),
    ] {
        sqlx::query(
            r#"
            INSERT INTO ledger_postings (
                transaction_id, posting_no, account_id, currency,
                amount_micros, created_at_ms
            )
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
        .map_err(map_conflict)?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, $2)",
    )
    .bind(transaction_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_conflict)?;
    Ok(transaction_id)
}

#[allow(clippy::too_many_arguments)]
async fn ensure_ledger_account(
    tx: &mut Transaction<'_, Postgres>,
    account_key: &str,
    owner_type: &str,
    owner_id: &str,
    account_type: &str,
    currency: &str,
    now: i64,
) -> Result<Uuid, CustomerRatingStoreError> {
    let candidate = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO ledger_accounts (
            account_id, account_key, owner_type, owner_id,
            account_type, currency, created_at_ms
        )
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
    .map_err(unavailable)?;
    let row: Option<(Uuid, String, String, String, String)> = sqlx::query_as(
        r#"
        SELECT account_id, owner_type, owner_id, account_type, currency
        FROM ledger_accounts
        WHERE account_key = $1
        "#,
    )
    .bind(account_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    match row {
        Some((id, stored_owner_type, stored_owner_id, stored_type, stored_currency))
            if stored_owner_type == owner_type
                && stored_owner_id == owner_id
                && stored_type == account_type
                && stored_currency == currency =>
        {
            Ok(id)
        }
        _ => Err(CustomerRatingStoreError::Conflict),
    }
}

async fn validate_deferred_contracts(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), CustomerRatingStoreError> {
    sqlx::query(
        r#"
        SET CONSTRAINTS
            customer_rated_usage_validate_total,
            customer_rated_usage_lines_validate_total,
            customer_rated_usage_validate_fact_set,
            customer_rated_usage_lines_validate_fact_set,
            customer_rated_usage_fact_links_validate_fact_set,
            customer_billing_holds_validate_rating,
            customer_rated_usage_validate_hold,
            credit_grants_validate_events,
            credit_grant_events_validate_all,
            credit_grant_reservations_validate_events,
            customer_billing_holds_validate_grant_split,
            ledger_transactions_balance_guard,
            ledger_postings_balance_guard,
            ledger_transaction_seals_balance_guard,
            ledger_transactions_validate_credit_grant
        IMMEDIATE
        "#,
    )
    .execute(&mut **tx)
    .await
    .map_err(map_conflict)?;
    sqlx::query(
        r#"
        SET CONSTRAINTS
            customer_rated_usage_validate_total,
            customer_rated_usage_lines_validate_total,
            customer_rated_usage_validate_fact_set,
            customer_rated_usage_lines_validate_fact_set,
            customer_rated_usage_fact_links_validate_fact_set,
            customer_billing_holds_validate_rating,
            customer_rated_usage_validate_hold,
            credit_grants_validate_events,
            credit_grant_events_validate_all,
            credit_grant_reservations_validate_events,
            customer_billing_holds_validate_grant_split,
            ledger_transactions_balance_guard,
            ledger_postings_balance_guard,
            ledger_transaction_seals_balance_guard,
            ledger_transactions_validate_credit_grant
        DEFERRED
        "#,
    )
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, CustomerRatingStoreError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn map_credit_grant(error: crate::ImageGatewayError) -> CustomerRatingStoreError {
    if error.status_code().is_server_error() {
        CustomerRatingStoreError::Unavailable
    } else {
        CustomerRatingStoreError::Conflict
    }
}

fn parse_i64(value: &str) -> Result<i64, CustomerRatingStoreError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or(CustomerRatingStoreError::InvalidInput)
}

fn map_conflict(error: sqlx::Error) -> CustomerRatingStoreError {
    match error.as_database_error().and_then(|error| error.code()) {
        Some(code)
            if matches!(
                code.as_ref(),
                "23503" | "23505" | "23514" | "55000" | "P0001"
            ) =>
        {
            CustomerRatingStoreError::Conflict
        }
        _ => CustomerRatingStoreError::Unavailable,
    }
}

fn unavailable(_: sqlx::Error) -> CustomerRatingStoreError {
    CustomerRatingStoreError::Unavailable
}

impl QuoteLineRow {
    fn frozen(&self) -> FrozenQuoteLine {
        FrozenQuoteLine {
            price_component_id: self.price_component_id,
            component_key: self.component_key.clone(),
            partition_key: self.partition_key.clone(),
            terminal_outcome: self.terminal_outcome.clone(),
            metric: self.metric.clone(),
            unit: self.unit.clone(),
            unit_size: self.unit_size.to_string(),
            unit_price_micros: self.unit_price_micros.to_string(),
            rate_adjustment_numerator: self.rate_adjustment_numerator.to_string(),
            rate_adjustment_denominator: self.rate_adjustment_denominator.to_string(),
            reservation_quantity_source: self.reservation_quantity_source.clone(),
            reservation_confidence: self.reservation_confidence.clone(),
            quantity_source: self.quantity_source.clone(),
            required_confidence: self.required_confidence.clone(),
            rounding_mode: self.rounding_mode.clone(),
            dimensions: self.dimensions_json.clone(),
            max_quantity: self.max_quantity.to_string(),
            max_amount_micros: self.max_amount_micros.to_string(),
        }
    }

    fn identity_key(&self) -> (String, String, String, Uuid) {
        (
            self.partition_key.clone(),
            self.terminal_outcome.clone(),
            self.component_key.clone(),
            self.price_component_id,
        )
    }
}

#[cfg(test)]
mod tests;

impl UsageFactRow {
    fn usage_fact(self) -> UsageFact {
        UsageFact {
            usage_fact_id: self.usage_fact_id,
            partition_key: self.billing_partition_key,
            authority_key: format!("output:{}", self.output_id),
            provider_id: self.provider_id,
            provider_account_id: self.provider_account_id,
            execution_surface: self.execution_surface,
            fact_domain: self.fact_domain,
            metric: self.metric,
            unit: self.unit,
            quantity: self.quantity.to_string(),
            outcome: self.terminal_outcome,
            quantity_source: self.quantity_source,
            confidence: self.confidence,
            dimensions: self.metadata_json,
        }
    }
}
