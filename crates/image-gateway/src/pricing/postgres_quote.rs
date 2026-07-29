use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    credit_grants::reserve_credit_grants,
    project_limits::{ProjectHardBudgetError, enforce_project_hard_budget},
};

use super::{FrozenQuotePlan, ResolvedPriceVersion};

#[derive(Clone, Debug)]
pub(crate) struct CustomerQuoteContext {
    pub job_id: Uuid,
    pub tenant_id: String,
    pub project_id: String,
    pub api_profile: String,
    pub operation: String,
    pub provider_id: Option<String>,
    pub provider_model_id: Option<String>,
    pub public_model_id: String,
    pub media_kind: String,
    pub service_tier: String,
    pub execution_surface: String,
    pub request_dimensions: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredCustomerQuote {
    pub quote_id: Uuid,
    pub hold_id: Uuid,
    pub job_id: Uuid,
    pub currency: String,
    pub max_total_micros: i64,
    pub quote_hash: String,
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub(crate) enum CustomerQuoteStoreError {
    #[error("customer quote input is invalid")]
    InvalidInput,
    #[error("stored customer quote conflicts with the requested quote")]
    Conflict,
    #[error("customer billing limit exceeded")]
    BillingLimitExceeded,
    #[error("project hard spend limit exceeded")]
    ProjectBudgetExceeded,
    #[error("project hard spend limit currency does not match the quote")]
    ProjectBudgetCurrencyMismatch,
    #[error("customer quote storage is unavailable")]
    Unavailable,
}

#[derive(sqlx::FromRow)]
struct StoredQuoteRow {
    quote_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    project_id: String,
    price_book_id: Uuid,
    price_book_version_id: Uuid,
    api_profile: String,
    operation: String,
    provider_id: Option<String>,
    provider_model_id: Option<String>,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
    execution_surface: String,
    request_dimensions_json: Value,
    billing_mode: String,
    is_free: bool,
    currency: String,
    max_total_micros: i64,
    quote_hash: String,
    created_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct StoredQuoteLineRow {
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
    quantity_source: String,
    required_confidence: String,
    rounding_mode: String,
    reservation_quantity_source: String,
    reservation_confidence: String,
    dimensions_json: Value,
    max_quantity: i64,
    max_amount_micros: i64,
}

#[derive(sqlx::FromRow)]
struct StoredHoldRow {
    hold_id: Uuid,
    quote_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    currency: String,
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
    created_at_ms: i64,
    updated_at_ms: i64,
}

pub(crate) async fn persist_customer_quote(
    tx: &mut Transaction<'_, Postgres>,
    context: &CustomerQuoteContext,
    resolved: &ResolvedPriceVersion,
    plan: &FrozenQuotePlan,
) -> Result<StoredCustomerQuote, CustomerQuoteStoreError> {
    sqlx::query("SAVEPOINT persist_customer_quote")
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    let result = persist_customer_quote_inner(tx, context, resolved, plan).await;
    match result {
        Ok(stored) => {
            sqlx::query("RELEASE SAVEPOINT persist_customer_quote")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            Ok(stored)
        }
        Err(error) => {
            sqlx::query("ROLLBACK TO SAVEPOINT persist_customer_quote")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            sqlx::query("RELEASE SAVEPOINT persist_customer_quote")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            Err(error)
        }
    }
}

async fn persist_customer_quote_inner(
    tx: &mut Transaction<'_, Postgres>,
    context: &CustomerQuoteContext,
    resolved: &ResolvedPriceVersion,
    plan: &FrozenQuotePlan,
) -> Result<StoredCustomerQuote, CustomerQuoteStoreError> {
    validate_input(context, resolved, plan)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!(
            "budget:{}:{}",
            context.tenant_id, resolved.currency
        ))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    let admitted_at_ms = lock_quote_job(tx, context).await?;
    if let Some(existing) = load_quote(tx, context.job_id).await? {
        let hold = load_hold(tx, existing.quote_id, context.job_id)
            .await?
            .ok_or(CustomerQuoteStoreError::Conflict)?;
        validate_replay(
            tx,
            context,
            resolved,
            plan,
            admitted_at_ms,
            &existing,
            &hold,
        )
        .await?;
        return Ok(existing.into_stored(hold.hold_id));
    }

    let quote_id = Uuid::new_v4();
    let max_total_micros = parse_i64(&plan.max_total_micros)?;
    enforce_project_hard_budget(
        tx,
        &context.project_id,
        &resolved.currency,
        max_total_micros,
    )
    .await
    .map_err(map_project_budget)?;
    sqlx::query(
        r#"
        INSERT INTO customer_price_quotes (
            quote_id, job_id, tenant_id, project_id,
            price_book_id, price_book_version_id,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, request_dimensions_json,
            billing_mode, is_free, currency,
            max_total_micros, quote_hash, created_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, 'customer_rate',
            $16, $17, $18, $19, $20
        )
        "#,
    )
    .bind(quote_id)
    .bind(context.job_id)
    .bind(&context.tenant_id)
    .bind(&context.project_id)
    .bind(resolved.price_book_id)
    .bind(resolved.version.price_book_version_id)
    .bind(&context.api_profile)
    .bind(&context.operation)
    .bind(&context.provider_id)
    .bind(&context.provider_model_id)
    .bind(&context.public_model_id)
    .bind(&context.media_kind)
    .bind(&context.service_tier)
    .bind(&context.execution_surface)
    .bind(&context.request_dimensions)
    .bind(resolved.version.is_free)
    .bind(&resolved.currency)
    .bind(max_total_micros)
    .bind(&plan.quote_hash)
    .bind(admitted_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(map_insert)?;

    for line in &plan.lines {
        sqlx::query(
            r#"
            INSERT INTO customer_price_quote_lines (
                quote_line_id, quote_id, job_id, price_component_id,
                component_key, partition_key, terminal_outcome,
                metric, unit, unit_size, unit_price_micros,
                rate_adjustment_numerator, rate_adjustment_denominator,
                quantity_source, required_confidence, rounding_mode,
                reservation_quantity_source, reservation_confidence,
                dimensions_json, max_quantity, max_amount_micros, created_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(quote_id)
        .bind(context.job_id)
        .bind(line.price_component_id)
        .bind(&line.component_key)
        .bind(&line.partition_key)
        .bind(&line.terminal_outcome)
        .bind(&line.metric)
        .bind(&line.unit)
        .bind(parse_i64(&line.unit_size)?)
        .bind(parse_i64(&line.unit_price_micros)?)
        .bind(parse_i64(&line.rate_adjustment_numerator)?)
        .bind(parse_i64(&line.rate_adjustment_denominator)?)
        .bind(&line.quantity_source)
        .bind(&line.required_confidence)
        .bind(&line.rounding_mode)
        .bind(&line.reservation_quantity_source)
        .bind(&line.reservation_confidence)
        .bind(&line.dimensions)
        .bind(parse_i64(&line.max_quantity)?)
        .bind(parse_i64(&line.max_amount_micros)?)
        .bind(admitted_at_ms)
        .execute(&mut **tx)
        .await
        .map_err(map_insert)?;
    }
    sqlx::query(
        r#"
        SET CONSTRAINTS
            customer_price_quotes_validate_total,
            customer_price_quote_lines_validate_total
        IMMEDIATE
        "#,
    )
    .execute(&mut **tx)
    .await
    .map_err(map_insert)?;
    sqlx::query(
        r#"
        SET CONSTRAINTS
            customer_price_quotes_validate_total,
            customer_price_quote_lines_validate_total
        DEFERRED
        "#,
    )
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;

    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros, held_micros,
            captured_micros, refunded_micros, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 0, 0, 0, 0, $3, $3)
        ON CONFLICT (tenant_id, currency) DO NOTHING
        "#,
    )
    .bind(&context.tenant_id)
    .bind(&resolved.currency)
    .bind(admitted_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    let hold_id = Uuid::new_v4();
    let funding_now = database_now(tx).await?;
    let funding = reserve_credit_grants(
        tx,
        hold_id,
        &context.tenant_id,
        &resolved.currency,
        max_total_micros,
        funding_now,
    )
    .await
    .map_err(map_credit_grant)?;
    let reserved = sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = held_micros + $3, updated_at_ms = $4
        WHERE tenant_id = $1 AND currency = $2
          AND (
              held_micros::NUMERIC + captured_micros::NUMERIC
              - refunded_micros::NUMERIC
              + $3::NUMERIC
          ) <= credit_limit_micros::NUMERIC
        "#,
    )
    .bind(&context.tenant_id)
    .bind(&resolved.currency)
    .bind(funding.account_micros)
    .bind(funding_now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if reserved != 1 {
        return Err(CustomerQuoteStoreError::BillingLimitExceeded);
    }

    sqlx::query(
        r#"
        INSERT INTO customer_billing_holds (
            hold_id, quote_id, job_id, tenant_id, currency,
            held_micros, grant_held_micros, account_held_micros,
            state, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            'held', $9, $9
        )
        "#,
    )
    .bind(hold_id)
    .bind(quote_id)
    .bind(context.job_id)
    .bind(&context.tenant_id)
    .bind(&resolved.currency)
    .bind(max_total_micros)
    .bind(funding.grant_micros)
    .bind(funding.account_micros)
    .bind(admitted_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(map_insert)?;

    Ok(StoredCustomerQuote {
        quote_id,
        hold_id,
        job_id: context.job_id,
        currency: resolved.currency.clone(),
        max_total_micros,
        quote_hash: plan.quote_hash.clone(),
    })
}

async fn lock_quote_job(
    tx: &mut Transaction<'_, Postgres>,
    context: &CustomerQuoteContext,
) -> Result<i64, CustomerQuoteStoreError> {
    sqlx::query_scalar(
        r#"
        SELECT attribution.admitted_at_ms
        FROM jobs job
        JOIN job_auth_attributions attribution
          ON attribution.job_id = job.job_id
         AND attribution.tenant_id = job.tenant_id
        WHERE job.job_id = $1
          AND job.tenant_id = $2
          AND attribution.project_id = $3
        FOR UPDATE OF job
        "#,
    )
    .bind(context.job_id)
    .bind(&context.tenant_id)
    .bind(&context.project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(CustomerQuoteStoreError::Conflict)
}

async fn load_quote(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Option<StoredQuoteRow>, CustomerQuoteStoreError> {
    sqlx::query_as(
        r#"
        SELECT quote_id, job_id, tenant_id, project_id,
               price_book_id, price_book_version_id,
               api_profile, operation, provider_id, provider_model_id,
               public_model_id, media_kind, service_tier,
               execution_surface, request_dimensions_json,
               billing_mode, is_free, currency,
               max_total_micros, quote_hash, created_at_ms
        FROM customer_price_quotes
        WHERE job_id = $1
        FOR SHARE
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_hold(
    tx: &mut Transaction<'_, Postgres>,
    quote_id: Uuid,
    job_id: Uuid,
) -> Result<Option<StoredHoldRow>, CustomerQuoteStoreError> {
    sqlx::query_as(
        r#"
        SELECT hold_id, quote_id, job_id, tenant_id, currency,
               held_micros, captured_micros, released_micros,
               grant_held_micros, account_held_micros,
               grant_captured_micros, account_captured_micros,
               grant_released_micros, account_released_micros,
               state,
               created_at_ms, updated_at_ms
        FROM customer_billing_holds
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

async fn validate_replay(
    tx: &mut Transaction<'_, Postgres>,
    context: &CustomerQuoteContext,
    resolved: &ResolvedPriceVersion,
    plan: &FrozenQuotePlan,
    admitted_at_ms: i64,
    existing: &StoredQuoteRow,
    hold: &StoredHoldRow,
) -> Result<(), CustomerQuoteStoreError> {
    let max_total_micros = parse_i64(&plan.max_total_micros)?;
    if existing.job_id != context.job_id
        || existing.tenant_id != context.tenant_id
        || existing.project_id != context.project_id
        || existing.price_book_id != resolved.price_book_id
        || existing.price_book_version_id != resolved.version.price_book_version_id
        || existing.api_profile != context.api_profile
        || existing.operation != context.operation
        || existing.provider_id != context.provider_id
        || existing.provider_model_id != context.provider_model_id
        || existing.public_model_id != context.public_model_id
        || existing.media_kind != context.media_kind
        || existing.service_tier != context.service_tier
        || existing.execution_surface != context.execution_surface
        || existing.request_dimensions_json != context.request_dimensions
        || existing.billing_mode != "customer_rate"
        || existing.is_free != resolved.version.is_free
        || existing.currency != resolved.currency
        || existing.max_total_micros != max_total_micros
        || existing.quote_hash != plan.quote_hash
        || existing.created_at_ms != admitted_at_ms
        || hold.quote_id != existing.quote_id
        || hold.job_id != context.job_id
        || hold.tenant_id != context.tenant_id
        || hold.currency != resolved.currency
        || hold.held_micros != max_total_micros
        || hold.grant_held_micros.checked_add(hold.account_held_micros) != Some(max_total_micros)
        || !valid_hold_state(hold)
        || hold.created_at_ms != admitted_at_ms
        || hold.updated_at_ms < admitted_at_ms
    {
        return Err(CustomerQuoteStoreError::Conflict);
    }

    let rows: Vec<StoredQuoteLineRow> = sqlx::query_as(
        r#"
        SELECT price_component_id, component_key, partition_key,
               terminal_outcome, metric, unit, unit_size,
               unit_price_micros, rate_adjustment_numerator,
               rate_adjustment_denominator, quantity_source, required_confidence,
               rounding_mode, reservation_quantity_source,
               reservation_confidence, dimensions_json, max_quantity,
               max_amount_micros
        FROM customer_price_quote_lines
        WHERE quote_id = $1 AND job_id = $2
        ORDER BY partition_key, terminal_outcome, component_key,
                 price_component_id
        FOR SHARE
        "#,
    )
    .bind(existing.quote_id)
    .bind(context.job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    if rows.len() != plan.lines.len()
        || rows.iter().zip(&plan.lines).any(|(row, line)| {
            row.price_component_id != line.price_component_id
                || row.component_key != line.component_key
                || row.partition_key != line.partition_key
                || row.terminal_outcome != line.terminal_outcome
                || row.metric != line.metric
                || row.unit != line.unit
                || Some(row.unit_size) != line.unit_size.parse().ok()
                || Some(row.unit_price_micros) != line.unit_price_micros.parse().ok()
                || Some(row.rate_adjustment_numerator)
                    != line.rate_adjustment_numerator.parse().ok()
                || Some(row.rate_adjustment_denominator)
                    != line.rate_adjustment_denominator.parse().ok()
                || row.quantity_source != line.quantity_source
                || row.required_confidence != line.required_confidence
                || row.rounding_mode != line.rounding_mode
                || row.reservation_quantity_source != line.reservation_quantity_source
                || row.reservation_confidence != line.reservation_confidence
                || row.dimensions_json != line.dimensions
                || Some(row.max_quantity) != line.max_quantity.parse().ok()
                || Some(row.max_amount_micros) != line.max_amount_micros.parse().ok()
        })
    {
        return Err(CustomerQuoteStoreError::Conflict);
    }
    Ok(())
}

fn valid_hold_state(hold: &StoredHoldRow) -> bool {
    match hold.state.as_str() {
        "held" => {
            hold.captured_micros == 0
                && hold.released_micros == 0
                && hold.grant_captured_micros == 0
                && hold.account_captured_micros == 0
                && hold.grant_released_micros == 0
                && hold.account_released_micros == 0
        }
        "settled" => {
            hold.captured_micros
                .checked_add(hold.released_micros)
                .is_some_and(|total| total == hold.held_micros)
                && hold
                    .grant_captured_micros
                    .checked_add(hold.grant_released_micros)
                    == Some(hold.grant_held_micros)
                && hold
                    .account_captured_micros
                    .checked_add(hold.account_released_micros)
                    == Some(hold.account_held_micros)
        }
        "released" => {
            hold.captured_micros == 0
                && hold.released_micros == hold.held_micros
                && hold.grant_captured_micros == 0
                && hold.grant_released_micros == hold.grant_held_micros
                && hold.account_captured_micros == 0
                && hold.account_released_micros == hold.account_held_micros
        }
        _ => false,
    }
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, CustomerQuoteStoreError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn map_credit_grant(error: crate::ImageGatewayError) -> CustomerQuoteStoreError {
    if error.status_code().is_server_error() {
        CustomerQuoteStoreError::Unavailable
    } else {
        CustomerQuoteStoreError::Conflict
    }
}

fn map_project_budget(error: ProjectHardBudgetError) -> CustomerQuoteStoreError {
    match error {
        ProjectHardBudgetError::Exceeded => CustomerQuoteStoreError::ProjectBudgetExceeded,
        ProjectHardBudgetError::CurrencyMismatch => {
            CustomerQuoteStoreError::ProjectBudgetCurrencyMismatch
        }
        ProjectHardBudgetError::Unavailable => CustomerQuoteStoreError::Unavailable,
    }
}

fn validate_input(
    context: &CustomerQuoteContext,
    resolved: &ResolvedPriceVersion,
    plan: &FrozenQuotePlan,
) -> Result<(), CustomerQuoteStoreError> {
    let required = [
        context.tenant_id.as_str(),
        context.project_id.as_str(),
        context.api_profile.as_str(),
        context.operation.as_str(),
        context.public_model_id.as_str(),
        context.media_kind.as_str(),
        context.service_tier.as_str(),
        context.execution_surface.as_str(),
    ];
    if context.job_id.is_nil()
        || required.iter().any(|value| value.trim().is_empty())
        || plan.price_book_id != resolved.price_book_id
        || plan.price_book_version_id != resolved.version.price_book_version_id
        || plan.currency != resolved.currency
        || plan.lines.is_empty()
        || resolved.purpose != "customer_sale"
        || resolved.version.billing_mode != "customer_rate"
        || resolved.version.state != "active"
        || !context.request_dimensions.is_object()
    {
        return Err(CustomerQuoteStoreError::InvalidInput);
    }
    parse_i64(&plan.max_total_micros)?;
    Ok(())
}

fn parse_i64(value: &str) -> Result<i64, CustomerQuoteStoreError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or(CustomerQuoteStoreError::InvalidInput)
}

fn map_insert(error: sqlx::Error) -> CustomerQuoteStoreError {
    let code = error
        .as_database_error()
        .and_then(|error| error.code())
        .map(|code| code.into_owned());
    let constraint = error
        .as_database_error()
        .and_then(|error| error.constraint())
        .map(str::to_owned);
    let message = error
        .as_database_error()
        .map(|error| error.message().to_owned());
    tracing::warn!(
        database.code = code.as_deref(),
        database.constraint = constraint.as_deref(),
        database.message = message.as_deref(),
        "customer quote database write failed"
    );
    match code.as_deref() {
        Some(code) if matches!(code, "23503" | "23505" | "23514" | "55000" | "P0001") => {
            CustomerQuoteStoreError::Conflict
        }
        _ => CustomerQuoteStoreError::Unavailable,
    }
}

fn unavailable(_: sqlx::Error) -> CustomerQuoteStoreError {
    CustomerQuoteStoreError::Unavailable
}

impl StoredQuoteRow {
    fn into_stored(self, hold_id: Uuid) -> StoredCustomerQuote {
        StoredCustomerQuote {
            quote_id: self.quote_id,
            hold_id,
            job_id: self.job_id,
            currency: self.currency,
            max_total_micros: self.max_total_micros,
            quote_hash: self.quote_hash,
        }
    }
}
