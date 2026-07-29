use image_provider_contracts::ProviderCostObservationV1;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    ResolvedPriceVersion, UsageFact, aggregate_provider_reported_cost, usd_ticks_to_ledger_micros,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredProviderCost {
    pub(crate) provider_cost_observation_id: Uuid,
    pub(crate) usage_fact_id: Uuid,
    pub(crate) receipt_id: Uuid,
    pub(crate) currency: String,
    pub(crate) native_quantity: String,
    pub(crate) amount_micros: i64,
    pub(crate) ledger_transaction_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ProviderCostStoreError {
    #[error("provider cost input is invalid")]
    InvalidInput,
    #[error("provider cost conflicts with immutable economic evidence")]
    Conflict,
    #[error("provider cost storage is unavailable")]
    Unavailable,
}

#[derive(sqlx::FromRow)]
struct SourceReceiptRow {
    receipt_id: Uuid,
    job_id: Uuid,
    output_id: Uuid,
    submission_id: Uuid,
    provider_id: String,
    provider_account_id: Option<Uuid>,
    outcome: String,
}

#[derive(sqlx::FromRow)]
struct ObservationRow {
    provider_cost_observation_id: Uuid,
    observation_key: String,
    provider_id: String,
    provider_account_id: Uuid,
    execution_surface: String,
    provider_operation_id: String,
    price_book_version_id: Uuid,
    fact_set_hash: String,
    currency: String,
    native_unit: String,
    native_quantity: String,
    authority: String,
    confidence: String,
    evidence_hash: String,
    evidence_path: String,
    amount_micros: i64,
    rounding_mode: String,
    rounding_delta_native_atoms: i64,
}

#[derive(sqlx::FromRow)]
struct FactRow {
    usage_fact_id: Uuid,
    semantic_key: String,
    job_id: Uuid,
    output_id: Uuid,
    submission_id: Uuid,
    receipt_id: Uuid,
    provider_id: String,
    provider_account_id: Option<Uuid>,
    execution_surface: String,
    fact_domain: String,
    metric: String,
    quantity: i64,
    unit: String,
    quantity_source: String,
    confidence: String,
    evidence_path: Option<String>,
    metadata_json: Value,
    billing_partition_key: String,
    terminal_outcome: String,
}

#[derive(sqlx::FromRow)]
struct LedgerRow {
    transaction_id: Uuid,
    semantic_key: String,
    currency: String,
    payload_hash: String,
    posting_count: i64,
    posting_sum_micros: i64,
    positive_amount_micros: Option<i64>,
    negative_amount_micros: Option<i64>,
    has_expense_account: bool,
    has_payable_account: bool,
    is_sealed: bool,
}

#[derive(sqlx::FromRow)]
struct ExecutorProviderCostEvidenceRow {
    scope: String,
    provider_id: String,
    execution_surface: String,
    provider_operation_id: String,
    currency: String,
    native_unit: String,
    native_quantity: String,
    authority: String,
    confidence: String,
    evidence_hash: String,
    evidence_path: String,
}

struct PreparedProviderCost {
    observation_key: String,
    fact_id: Uuid,
    fact_semantic_key: String,
    fact_metadata: Value,
    native_quantity: i64,
    fact_set_hash: String,
    amount_micros: i64,
    rounding_delta_native_atoms: i64,
    ledger_semantic_key: String,
    ledger_payload_hash: String,
}

pub(crate) async fn apply_executor_provider_reported_cost(
    tx: &mut Transaction<'_, Postgres>,
    receipt_id: Uuid,
    resolved: &ResolvedPriceVersion,
    source_manifest_id: Uuid,
) -> Result<StoredProviderCost, ProviderCostStoreError> {
    if receipt_id.is_nil() || source_manifest_id.is_nil() {
        return Err(ProviderCostStoreError::InvalidInput);
    }
    let observation = load_executor_provider_cost_observation(tx, source_manifest_id).await?;
    sqlx::query("SAVEPOINT apply_provider_reported_cost")
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    let result = apply_provider_reported_cost_inner(
        tx,
        receipt_id,
        resolved,
        source_manifest_id,
        &observation,
    )
    .await;
    match result {
        Ok(stored) => {
            sqlx::query("RELEASE SAVEPOINT apply_provider_reported_cost")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            Ok(stored)
        }
        Err(error) => {
            sqlx::query("ROLLBACK TO SAVEPOINT apply_provider_reported_cost")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            sqlx::query("RELEASE SAVEPOINT apply_provider_reported_cost")
                .execute(&mut **tx)
                .await
                .map_err(unavailable)?;
            Err(error)
        }
    }
}

async fn load_executor_provider_cost_observation(
    tx: &mut Transaction<'_, Postgres>,
    source_manifest_id: Uuid,
) -> Result<ProviderCostObservationV1, ProviderCostStoreError> {
    let row = sqlx::query_as::<_, ExecutorProviderCostEvidenceRow>(
        r#"
        SELECT scope, provider_id, execution_surface,
               provider_operation_id, currency, native_unit,
               native_quantity::TEXT, authority, confidence,
               evidence_hash, evidence_path
        FROM executor_provider_cost_evidence
        WHERE manifest_id = $1
        "#,
    )
    .bind(source_manifest_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderCostStoreError::Conflict)?;
    let valid_scope = matches!(
        (row.scope.as_str(), row.execution_surface.as_str()),
        ("api_response", "provider_api") | ("cli_invocation", "provider_cli")
    );
    if !valid_scope
        || row.currency != "USD"
        || row.native_unit != "usd_tick"
        || row.authority != "provider_reported"
        || row.confidence != "exact"
    {
        return Err(ProviderCostStoreError::Conflict);
    }
    let native_quantity = row
        .native_quantity
        .parse::<u128>()
        .map_err(|_| ProviderCostStoreError::Conflict)?;
    let evidence_hash: [u8; 32] = hex::decode(row.evidence_hash)
        .map_err(|_| ProviderCostStoreError::Conflict)?
        .try_into()
        .map_err(|_| ProviderCostStoreError::Conflict)?;
    ProviderCostObservationV1::provider_reported_usd_ticks_from_evidence_hash(
        row.provider_id,
        row.execution_surface,
        row.provider_operation_id,
        native_quantity,
        evidence_hash,
        row.evidence_path,
    )
    .map_err(|_| ProviderCostStoreError::Conflict)
}

async fn apply_provider_reported_cost_inner(
    tx: &mut Transaction<'_, Postgres>,
    receipt_id: Uuid,
    resolved: &ResolvedPriceVersion,
    source_manifest_id: Uuid,
    observation: &ProviderCostObservationV1,
) -> Result<StoredProviderCost, ProviderCostStoreError> {
    let source = load_source_receipt(tx, receipt_id).await?;
    let provider_account_id = source
        .provider_account_id
        .ok_or(ProviderCostStoreError::Conflict)?;
    if source.provider_id != observation.provider_id {
        return Err(ProviderCostStoreError::Conflict);
    }
    let operation_lock = format!(
        "provider-cost:v1:{}:{}:{}:{}",
        observation.provider_id,
        provider_account_id,
        observation.execution_surface,
        observation.provider_operation_id
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(operation_lock)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;

    if let Some(existing) =
        load_observation_by_operation(tx, observation, provider_account_id).await?
    {
        let facts = load_observation_facts(tx, existing.provider_cost_observation_id).await?;
        let [fact] = facts.as_slice() else {
            return Err(ProviderCostStoreError::Conflict);
        };
        let prepared = prepare_cost(
            resolved,
            observation,
            &source,
            provider_account_id,
            fact.usage_fact_id,
        )?;
        return validate_replay(
            tx,
            receipt_id,
            resolved,
            observation,
            &source,
            provider_account_id,
            &prepared,
            source_manifest_id,
            existing,
            &facts,
        )
        .await;
    }

    let prepared = prepare_cost(
        resolved,
        observation,
        &source,
        provider_account_id,
        Uuid::new_v4(),
    )?;
    let now = database_now(tx).await?;
    insert_usage_fact(
        tx,
        receipt_id,
        observation,
        &source,
        provider_account_id,
        &prepared,
        now,
    )
    .await?;
    let observation_id = Uuid::new_v4();
    insert_observation(
        tx,
        observation_id,
        resolved,
        observation,
        provider_account_id,
        &prepared,
        now,
    )
    .await?;
    insert_observation_source(tx, observation_id, source_manifest_id, now).await?;
    insert_fact_link(
        tx,
        observation_id,
        observation,
        provider_account_id,
        prepared.fact_id,
        now,
    )
    .await?;
    insert_receipt_link(tx, observation_id, observation, receipt_id, now).await?;
    let ledger_transaction_id = if prepared.amount_micros == 0 {
        None
    } else {
        Some(insert_provider_cost_ledger(tx, observation_id, observation, &prepared, now).await?)
    };
    validate_deferred_provider_cost_contracts(tx).await?;
    Ok(StoredProviderCost {
        provider_cost_observation_id: observation_id,
        usage_fact_id: prepared.fact_id,
        receipt_id,
        currency: observation.currency.clone(),
        native_quantity: prepared.native_quantity.to_string(),
        amount_micros: prepared.amount_micros,
        ledger_transaction_id,
    })
}

fn prepare_cost(
    resolved: &ResolvedPriceVersion,
    observation: &ProviderCostObservationV1,
    source: &SourceReceiptRow,
    provider_account_id: Uuid,
    fact_id: Uuid,
) -> Result<PreparedProviderCost, ProviderCostStoreError> {
    let native_quantity = i64::try_from(observation.native_quantity)
        .map_err(|_| ProviderCostStoreError::InvalidInput)?;
    let observation_key = account_scoped_observation_key(observation, provider_account_id);
    let fact_semantic_key =
        provider_cost_fact_semantic_key(observation, provider_account_id, source.receipt_id);
    let fact_metadata = json!({
        "schema": "provider_cost_fact.v1",
        "observation_key": observation_key,
        "provider_operation_id": observation.provider_operation_id,
        "evidence_hash": hex::encode(observation.evidence_hash),
    });
    let fact = UsageFact {
        usage_fact_id: fact_id,
        partition_key: "provider-cost".to_string(),
        authority_key: format!(
            "{}:{}:{}",
            observation.provider_id,
            observation.execution_surface,
            observation.provider_operation_id
        ),
        provider_id: observation.provider_id.clone(),
        provider_account_id: Some(provider_account_id),
        execution_surface: observation.execution_surface.clone(),
        fact_domain: "provider_actual".to_string(),
        metric: "provider_reported_cost".to_string(),
        unit: observation.native_unit.as_str().to_string(),
        quantity: native_quantity.to_string(),
        outcome: source.outcome.clone(),
        quantity_source: observation.authority.as_str().to_string(),
        confidence: observation.confidence.as_str().to_string(),
        dimensions: fact_metadata.clone(),
    };
    let aggregate = aggregate_provider_reported_cost(resolved, &[fact])
        .map_err(|_| ProviderCostStoreError::Conflict)?;
    if aggregate.provider_id != observation.provider_id
        || aggregate.execution_surface != observation.execution_surface
        || aggregate.currency != observation.currency
        || aggregate.quantity != native_quantity.to_string()
    {
        return Err(ProviderCostStoreError::Conflict);
    }
    let conversion =
        usd_ticks_to_ledger_micros(&aggregate).map_err(|_| ProviderCostStoreError::Conflict)?;
    let amount_micros = parse_nonnegative_i64(&conversion.amount_micros)?;
    let rounding_delta_native_atoms = conversion
        .rounding_delta_native_atoms
        .parse::<i64>()
        .map_err(|_| ProviderCostStoreError::InvalidInput)?;
    let ledger_semantic_key = format!("provider-cost-observation:v1:{observation_key}");
    let ledger_payload_hash = ledger_payload_hash(
        &ledger_semantic_key,
        &observation.currency,
        amount_micros,
        observation.provider_id.as_str(),
    );
    Ok(PreparedProviderCost {
        observation_key,
        fact_id,
        fact_semantic_key,
        fact_metadata,
        native_quantity,
        fact_set_hash: aggregate.fact_set_hash,
        amount_micros,
        rounding_delta_native_atoms,
        ledger_semantic_key,
        ledger_payload_hash,
    })
}

async fn load_source_receipt(
    tx: &mut Transaction<'_, Postgres>,
    receipt_id: Uuid,
) -> Result<SourceReceiptRow, ProviderCostStoreError> {
    sqlx::query_as(
        r#"
        SELECT receipt.receipt_id, receipt.job_id, receipt.output_id,
               receipt.submission_id,
               receipt.provider_id, submission.provider_account_id,
               receipt.outcome
        FROM provider_receipts receipt
        JOIN provider_submissions submission
          ON submission.submission_id = receipt.submission_id
         AND submission.output_id = receipt.output_id
         AND submission.job_id = receipt.job_id
         AND submission.provider_id = receipt.provider_id
        WHERE receipt.receipt_id = $1
        FOR SHARE OF receipt, submission
        "#,
    )
    .bind(receipt_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderCostStoreError::Conflict)
}

async fn load_observation_by_operation(
    tx: &mut Transaction<'_, Postgres>,
    observation: &ProviderCostObservationV1,
    provider_account_id: Uuid,
) -> Result<Option<ObservationRow>, ProviderCostStoreError> {
    sqlx::query_as(
        r#"
        SELECT provider_cost_observation_id, observation_key, provider_id,
               provider_account_id, execution_surface, provider_operation_id,
               price_book_version_id, fact_set_hash, currency, native_unit,
               native_quantity::TEXT AS native_quantity, authority, confidence,
               evidence_hash, evidence_path, amount_micros, rounding_mode,
               rounding_delta_native_atoms
        FROM provider_cost_observations
        WHERE provider_id = $1
          AND provider_account_id = $2
          AND execution_surface = $3
          AND provider_operation_id = $4
        FOR SHARE
        "#,
    )
    .bind(&observation.provider_id)
    .bind(provider_account_id)
    .bind(&observation.execution_surface)
    .bind(&observation.provider_operation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_observation_facts(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
) -> Result<Vec<FactRow>, ProviderCostStoreError> {
    sqlx::query_as(
        r#"
        SELECT fact.usage_fact_id, fact.semantic_key, fact.job_id,
               fact.output_id, fact.submission_id, fact.receipt_id,
               fact.provider_id, fact.provider_account_id,
               fact.execution_surface, fact.fact_domain, fact.metric,
               fact.quantity, fact.unit, fact.quantity_source,
               fact.confidence, fact.evidence_path, fact.metadata_json,
               fact.billing_partition_key, fact.terminal_outcome
        FROM provider_cost_observation_fact_links link
        JOIN provider_usage_facts fact
          ON fact.usage_fact_id = link.usage_fact_id
        WHERE link.provider_cost_observation_id = $1
        ORDER BY fact.usage_fact_id
        "#,
    )
    .bind(observation_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn insert_usage_fact(
    tx: &mut Transaction<'_, Postgres>,
    receipt_id: Uuid,
    observation: &ProviderCostObservationV1,
    source: &SourceReceiptRow,
    provider_account_id: Uuid,
    prepared: &PreparedProviderCost,
    now: i64,
) -> Result<(), ProviderCostStoreError> {
    sqlx::query(
        r#"
        INSERT INTO provider_usage_facts (
            usage_fact_id, semantic_key, job_id, output_id, submission_id,
            receipt_id, provider_id, provider_account_id, execution_surface,
            fact_domain, metric, quantity, unit, quantity_source, confidence,
            evidence_path, metadata_json, created_at_ms,
            billing_partition_key, terminal_outcome
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            'provider_actual', 'provider_reported_cost', $10, 'usd_tick',
            'provider_reported', 'exact', $11, $12, $13,
            'provider-cost', $14
        )
        "#,
    )
    .bind(prepared.fact_id)
    .bind(&prepared.fact_semantic_key)
    .bind(source.job_id)
    .bind(source.output_id)
    .bind(source.submission_id)
    .bind(receipt_id)
    .bind(&observation.provider_id)
    .bind(provider_account_id)
    .bind(&observation.execution_surface)
    .bind(prepared.native_quantity)
    .bind(&observation.evidence_path)
    .bind(&prepared.fact_metadata)
    .bind(now)
    .bind(&source.outcome)
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_observation(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    resolved: &ResolvedPriceVersion,
    observation: &ProviderCostObservationV1,
    provider_account_id: Uuid,
    prepared: &PreparedProviderCost,
    now: i64,
) -> Result<(), ProviderCostStoreError> {
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observations (
            provider_cost_observation_id, observation_key,
            provider_id, provider_account_id, execution_surface,
            provider_operation_id, purpose, price_book_version_id,
            fact_set_hash, currency, native_unit, native_quantity,
            authority, confidence, evidence_hash, evidence_path,
            amount_micros, rounding_mode, rounding_delta_native_atoms,
            created_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, 'provider_actual', $7,
            $8, $9, 'usd_tick', $10, 'provider_reported', 'exact',
            $11, $12, $13, 'half_up_after_aggregate', $14, $15
        )
        "#,
    )
    .bind(observation_id)
    .bind(&prepared.observation_key)
    .bind(&observation.provider_id)
    .bind(provider_account_id)
    .bind(&observation.execution_surface)
    .bind(&observation.provider_operation_id)
    .bind(resolved.version.price_book_version_id)
    .bind(&prepared.fact_set_hash)
    .bind(&observation.currency)
    .bind(prepared.native_quantity)
    .bind(hex::encode(observation.evidence_hash))
    .bind(&observation.evidence_path)
    .bind(prepared.amount_micros)
    .bind(prepared.rounding_delta_native_atoms)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
    Ok(())
}

async fn insert_observation_source(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    source_manifest_id: Uuid,
    now: i64,
) -> Result<(), ProviderCostStoreError> {
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_sources (
            provider_cost_observation_id, source_kind,
            executor_provider_cost_evidence_manifest_id,
            legacy_reason, created_at_ms
        )
        VALUES ($1, 'executor_verified', $2, NULL, $3)
        "#,
    )
    .bind(observation_id)
    .bind(source_manifest_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
    Ok(())
}

async fn insert_fact_link(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    observation: &ProviderCostObservationV1,
    provider_account_id: Uuid,
    fact_id: Uuid,
    now: i64,
) -> Result<(), ProviderCostStoreError> {
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_fact_links (
            provider_cost_observation_id, usage_fact_id, provider_id,
            provider_account_id, execution_surface, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(observation_id)
    .bind(fact_id)
    .bind(&observation.provider_id)
    .bind(provider_account_id)
    .bind(&observation.execution_surface)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
    Ok(())
}

async fn insert_receipt_link(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    observation: &ProviderCostObservationV1,
    receipt_id: Uuid,
    now: i64,
) -> Result<(), ProviderCostStoreError> {
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_receipts (
            provider_cost_observation_id, receipt_id, provider_id, created_at_ms
        )
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(observation_id)
    .bind(receipt_id)
    .bind(&observation.provider_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
    Ok(())
}

async fn insert_provider_cost_ledger(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    observation: &ProviderCostObservationV1,
    prepared: &PreparedProviderCost,
    now: i64,
) -> Result<Uuid, ProviderCostStoreError> {
    let expense_key = format!("platform:{}:provider-expense", observation.currency);
    let payable_key = format!(
        "provider:{}:{}:payable",
        observation.provider_id, observation.currency
    );
    let expense_id = ensure_ledger_account(
        tx,
        &expense_key,
        "platform",
        "platform",
        "expense",
        &observation.currency,
        now,
    )
    .await?;
    let payable_id = ensure_ledger_account(
        tx,
        &payable_key,
        "provider",
        &observation.provider_id,
        "payable",
        &observation.currency,
        now,
    )
    .await?;
    let transaction_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions (
            transaction_id, semantic_key,
            source_provider_cost_observation_id,
            transaction_type, currency, payload_hash, created_at_ms
        )
        VALUES ($1, $2, $3, 'provider_cost', $4, $5, $6)
        "#,
    )
    .bind(transaction_id)
    .bind(&prepared.ledger_semantic_key)
    .bind(observation_id)
    .bind(&observation.currency)
    .bind(&prepared.ledger_payload_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
    for (posting_no, account_id, amount) in [
        (1_i16, expense_id, prepared.amount_micros),
        (2_i16, payable_id, -prepared.amount_micros),
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
        .bind(&observation.currency)
        .bind(amount)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(map_database_error)?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, $2)",
    )
    .bind(transaction_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
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
) -> Result<Uuid, ProviderCostStoreError> {
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
    let stored: Option<(Uuid, String, String, String, String)> = sqlx::query_as(
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
    match stored {
        Some((id, stored_owner_type, stored_owner_id, stored_type, stored_currency))
            if stored_owner_type == owner_type
                && stored_owner_id == owner_id
                && stored_type == account_type
                && stored_currency == currency =>
        {
            Ok(id)
        }
        _ => Err(ProviderCostStoreError::Conflict),
    }
}

#[allow(clippy::too_many_arguments)]
async fn validate_replay(
    tx: &mut Transaction<'_, Postgres>,
    receipt_id: Uuid,
    resolved: &ResolvedPriceVersion,
    observation: &ProviderCostObservationV1,
    source: &SourceReceiptRow,
    provider_account_id: Uuid,
    prepared: &PreparedProviderCost,
    source_manifest_id: Uuid,
    existing: ObservationRow,
    facts: &[FactRow],
) -> Result<StoredProviderCost, ProviderCostStoreError> {
    let evidence_hash = hex::encode(observation.evidence_hash);
    if existing.observation_key != prepared.observation_key
        || existing.provider_id != observation.provider_id
        || existing.provider_account_id != provider_account_id
        || existing.execution_surface != observation.execution_surface
        || existing.provider_operation_id != observation.provider_operation_id
        || existing.price_book_version_id != resolved.version.price_book_version_id
        || existing.fact_set_hash != prepared.fact_set_hash
        || existing.currency != observation.currency
        || existing.native_unit != observation.native_unit.as_str()
        || existing.native_quantity != prepared.native_quantity.to_string()
        || existing.authority != observation.authority.as_str()
        || existing.confidence != observation.confidence.as_str()
        || existing.evidence_hash != evidence_hash
        || existing.evidence_path != observation.evidence_path
        || existing.amount_micros != prepared.amount_micros
        || existing.rounding_mode != "half_up_after_aggregate"
        || existing.rounding_delta_native_atoms != prepared.rounding_delta_native_atoms
    {
        return Err(ProviderCostStoreError::Conflict);
    }
    let stored_source_manifest_id: Option<Uuid> = sqlx::query_scalar(
        r#"
        SELECT executor_provider_cost_evidence_manifest_id
        FROM provider_cost_observation_sources
        WHERE provider_cost_observation_id = $1
          AND source_kind = 'executor_verified'
        "#,
    )
    .bind(existing.provider_cost_observation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .flatten();
    if stored_source_manifest_id != Some(source_manifest_id) {
        return Err(ProviderCostStoreError::Conflict);
    }
    let [fact] = facts else {
        return Err(ProviderCostStoreError::Conflict);
    };
    if fact.semantic_key != prepared.fact_semantic_key
        || fact.job_id != source.job_id
        || fact.output_id != source.output_id
        || fact.submission_id != source.submission_id
        || fact.receipt_id != receipt_id
        || fact.provider_id != observation.provider_id
        || fact.provider_account_id != Some(provider_account_id)
        || fact.execution_surface != observation.execution_surface
        || fact.fact_domain != "provider_actual"
        || fact.metric != "provider_reported_cost"
        || fact.quantity != prepared.native_quantity
        || fact.unit != "usd_tick"
        || fact.quantity_source != "provider_reported"
        || fact.confidence != "exact"
        || fact.evidence_path.as_deref() != Some(observation.evidence_path.as_str())
        || fact.metadata_json != prepared.fact_metadata
        || fact.billing_partition_key != "provider-cost"
        || fact.terminal_outcome != source.outcome
    {
        return Err(ProviderCostStoreError::Conflict);
    }
    let receipt_links: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT receipt_id
        FROM provider_cost_observation_receipts
        WHERE provider_cost_observation_id = $1
        ORDER BY receipt_id
        "#,
    )
    .bind(existing.provider_cost_observation_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    if receipt_links != vec![receipt_id] {
        return Err(ProviderCostStoreError::Conflict);
    }
    let ledger_transaction_id = validate_replay_ledger(
        tx,
        existing.provider_cost_observation_id,
        observation,
        prepared,
    )
    .await?;
    Ok(StoredProviderCost {
        provider_cost_observation_id: existing.provider_cost_observation_id,
        usage_fact_id: fact.usage_fact_id,
        receipt_id,
        currency: existing.currency,
        native_quantity: existing.native_quantity,
        amount_micros: existing.amount_micros,
        ledger_transaction_id,
    })
}

async fn validate_replay_ledger(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    observation: &ProviderCostObservationV1,
    prepared: &PreparedProviderCost,
) -> Result<Option<Uuid>, ProviderCostStoreError> {
    let ledger: Option<LedgerRow> = sqlx::query_as(
        r#"
        SELECT transaction.transaction_id, transaction.semantic_key,
               transaction.currency, transaction.payload_hash,
               COUNT(posting.posting_no) AS posting_count,
               COALESCE(SUM(posting.amount_micros), 0)::BIGINT
                   AS posting_sum_micros,
               MAX(posting.amount_micros) FILTER (
                   WHERE posting.amount_micros > 0
               ) AS positive_amount_micros,
               MIN(posting.amount_micros) FILTER (
                   WHERE posting.amount_micros < 0
               ) AS negative_amount_micros,
               COALESCE(BOOL_OR(
                   account.account_key = $2
                   AND account.account_type = 'expense'
               ), FALSE) AS has_expense_account,
               COALESCE(BOOL_OR(
                   account.account_key = $3
                   AND account.account_type = 'payable'
               ), FALSE) AS has_payable_account,
               EXISTS (
                   SELECT 1
                   FROM ledger_transaction_seals seal
                   WHERE seal.transaction_id = transaction.transaction_id
               ) AS is_sealed
        FROM ledger_transactions transaction
        LEFT JOIN ledger_postings posting
          ON posting.transaction_id = transaction.transaction_id
        LEFT JOIN ledger_accounts account
          ON account.account_id = posting.account_id
         AND account.currency = posting.currency
        WHERE transaction.source_provider_cost_observation_id = $1
          AND transaction.transaction_type = 'provider_cost'
        GROUP BY transaction.transaction_id
        "#,
    )
    .bind(observation_id)
    .bind(format!(
        "platform:{}:provider-expense",
        observation.currency
    ))
    .bind(format!(
        "provider:{}:{}:payable",
        observation.provider_id, observation.currency
    ))
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    if prepared.amount_micros == 0 {
        return if ledger.is_none() {
            Ok(None)
        } else {
            Err(ProviderCostStoreError::Conflict)
        };
    }
    let Some(ledger) = ledger else {
        return Err(ProviderCostStoreError::Conflict);
    };
    if ledger.semantic_key != prepared.ledger_semantic_key
        || ledger.currency != observation.currency
        || ledger.payload_hash != prepared.ledger_payload_hash
        || ledger.posting_count != 2
        || ledger.posting_sum_micros != 0
        || ledger.positive_amount_micros != Some(prepared.amount_micros)
        || ledger.negative_amount_micros != Some(-prepared.amount_micros)
        || !ledger.has_expense_account
        || !ledger.has_payable_account
        || !ledger.is_sealed
    {
        return Err(ProviderCostStoreError::Conflict);
    }
    Ok(Some(ledger.transaction_id))
}

async fn validate_deferred_provider_cost_contracts(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), ProviderCostStoreError> {
    sqlx::query(
        r#"
        SET CONSTRAINTS
            provider_cost_observations_validate_fact_set,
            provider_cost_observation_fact_links_validate_fact_set,
            provider_cost_observations_require_source,
            provider_cost_observation_sources_validate,
            provider_cost_observation_fact_links_validate_source,
            provider_cost_observation_receipts_validate_source,
            ledger_transactions_provider_cost_amount_guard,
            ledger_postings_provider_cost_amount_guard,
            ledger_transaction_seals_provider_cost_amount_guard,
            ledger_transactions_balance_guard,
            ledger_postings_balance_guard,
            ledger_transaction_seals_balance_guard
        IMMEDIATE
        "#,
    )
    .execute(&mut **tx)
    .await
    .map_err(map_database_error)?;
    sqlx::query(
        r#"
        SET CONSTRAINTS
            provider_cost_observations_validate_fact_set,
            provider_cost_observation_fact_links_validate_fact_set,
            provider_cost_observations_require_source,
            provider_cost_observation_sources_validate,
            provider_cost_observation_fact_links_validate_source,
            provider_cost_observation_receipts_validate_source,
            ledger_transactions_provider_cost_amount_guard,
            ledger_postings_provider_cost_amount_guard,
            ledger_transaction_seals_provider_cost_amount_guard,
            ledger_transactions_balance_guard,
            ledger_postings_balance_guard,
            ledger_transaction_seals_balance_guard
        DEFERRED
        "#,
    )
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

fn ledger_payload_hash(
    semantic_key: &str,
    currency: &str,
    amount_micros: i64,
    provider_id: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"provider-cost-ledger-v1\0");
    let amount = amount_micros.to_string();
    for field in [
        semantic_key.as_bytes(),
        currency.as_bytes(),
        amount.as_bytes(),
        provider_id.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    hex::encode(digest.finalize())
}

fn provider_cost_fact_semantic_key(
    observation: &ProviderCostObservationV1,
    provider_account_id: Uuid,
    receipt_id: Uuid,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"provider-cost-fact-v1\0");
    for field in [
        observation.provider_id.as_bytes(),
        provider_account_id.as_bytes(),
        observation.execution_surface.as_bytes(),
        observation.provider_operation_id.as_bytes(),
        receipt_id.as_bytes(),
        b"provider-cost",
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("provider-cost-fact:v1:{}", hex::encode(digest.finalize()))
}

fn account_scoped_observation_key(
    observation: &ProviderCostObservationV1,
    provider_account_id: Uuid,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"provider-cost-observation-account-v1\0");
    for field in [
        provider_account_id.as_bytes().as_slice(),
        observation.canonical_sha256_v1().as_slice(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    hex::encode(digest.finalize())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ProviderCostStoreError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn parse_nonnegative_i64(value: &str) -> Result<i64, ProviderCostStoreError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or(ProviderCostStoreError::InvalidInput)
}

fn map_database_error(error: sqlx::Error) -> ProviderCostStoreError {
    match error.as_database_error().and_then(|error| error.code()) {
        Some(code)
            if matches!(
                code.as_ref(),
                "23503" | "23505" | "23514" | "23P01" | "55000" | "P0001"
            ) =>
        {
            ProviderCostStoreError::Conflict
        }
        _ => ProviderCostStoreError::Unavailable,
    }
}

fn unavailable(_: sqlx::Error) -> ProviderCostStoreError {
    ProviderCostStoreError::Unavailable
}
