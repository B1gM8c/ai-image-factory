use sqlx::FromRow;
use uuid::Uuid;

use super::{PostgresAdminReadStore, unavailable};
use crate::admin_read::{
    AdminReadError, AdminReadScope, JobCustomerHold, JobCustomerQuote, JobCustomerRating,
    JobEconomicsSnapshot, JobLedgerTransaction, JobProviderCost, JobQuoteLine, JobUsageFact,
};

impl PostgresAdminReadStore {
    pub(super) async fn read_job_economics(
        &self,
        scope: &AdminReadScope,
        job_id: Uuid,
        project_id: Option<&str>,
    ) -> Result<JobEconomicsSnapshot, AdminReadError> {
        let (mut tx, as_of_ms) = self.begin_current_snapshot().await?;
        let tenant_ids = scope.tenant_ids().map(|ids| ids.to_vec());
        let actor_user_id = scope.actor_user_id_for_project(project_id)?;
        let contract = sqlx::query_as::<_, JobContractRow>(
            r#"
            SELECT job.economics_contract_version
            FROM jobs job
            WHERE job.job_id = $1
              AND ($2::TEXT[] IS NULL OR job.tenant_id = ANY($2))
              AND (
                $3::UUID IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM job_auth_attributions attribution
                    WHERE attribution.job_id = job.job_id
                      AND (
                        attribution.actor_user_id = $3
                        OR attribution.credential_owner_user_id = $3
                      )
                )
              )
              AND (
                $4::TEXT IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM job_auth_attributions attribution
                    WHERE attribution.job_id = job.job_id
                      AND attribution.project_id = $4
                )
              )
            "#,
        )
        .bind(job_id)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .bind(project_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or(AdminReadError::NotFound)?;

        let quote_row = sqlx::query_as::<_, QuoteRow>(
            r#"
            SELECT quote.quote_id::TEXT,
                   quote.price_book_version_id::TEXT,
                   quote.public_model_id,
                   quote.media_kind,
                   quote.service_tier,
                   quote.currency,
                   quote.is_free,
                   quote.max_total_micros::TEXT,
                   quote.created_at_ms
            FROM customer_price_quotes quote
            WHERE quote.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;

        let quote_lines = sqlx::query_as::<_, QuoteLineRow>(
            r#"
            SELECT line.component_key,
                   line.partition_key,
                   line.terminal_outcome,
                   line.metric,
                   line.unit,
                   line.unit_size::TEXT,
                   line.unit_price_micros::TEXT,
                   line.reservation_quantity_source,
                   line.reservation_confidence,
                   line.max_quantity::TEXT,
                   line.max_amount_micros::TEXT,
                   rated_line.actual_quantity::TEXT AS actual_quantity,
                   rated_line.amount_micros::TEXT AS actual_amount_micros
            FROM customer_price_quote_lines line
            LEFT JOIN customer_rated_usage_lines rated_line
              ON rated_line.quote_line_id = line.quote_line_id
            WHERE line.job_id = $1
            ORDER BY line.terminal_outcome, line.partition_key, line.component_key
            "#,
        )
        .bind(job_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();

        let customer_quote = quote_row.map(|row| row.into_quote(quote_lines));
        let customer_hold = sqlx::query_as::<_, HoldRow>(
            r#"
            SELECT state,
                   currency,
                   held_micros::TEXT,
                   captured_micros::TEXT,
                   released_micros::TEXT,
                   created_at_ms,
                   updated_at_ms
            FROM customer_billing_holds
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .map(Into::into);

        let usage_facts = sqlx::query_as::<_, UsageFactRow>(
            r#"
            SELECT metric,
                   quantity::TEXT,
                   unit,
                   quantity_source,
                   confidence,
                   billing_partition_key,
                   terminal_outcome,
                   created_at_ms
            FROM provider_usage_facts
            WHERE job_id = $1
              AND metric <> 'provider_reported_cost'
            ORDER BY created_at_ms, metric, usage_fact_id
            "#,
        )
        .bind(job_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();

        let customer_rating = sqlx::query_as::<_, RatingRow>(
            r#"
            SELECT currency, total_amount_micros::TEXT, created_at_ms
            FROM customer_rated_usage
            WHERE job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .map(Into::into);

        let ledger_transactions = sqlx::query_as::<_, LedgerTransactionRow>(
            r#"
            SELECT transaction.transaction_id::TEXT,
                   transaction.transaction_type,
                   transaction.currency,
                   COALESCE(
                       SUM(posting.amount_micros) FILTER (
                           WHERE posting.amount_micros > 0
                       ),
                       0
                   )::TEXT AS amount_micros,
                   transaction.created_at_ms,
                   seal.sealed_at_ms
            FROM ledger_transactions transaction
            LEFT JOIN ledger_transactions reversed_transaction
              ON reversed_transaction.transaction_id =
                 transaction.reverses_transaction_id
            LEFT JOIN ledger_postings posting
              ON posting.transaction_id = transaction.transaction_id
            LEFT JOIN ledger_transaction_seals seal
              ON seal.transaction_id = transaction.transaction_id
            WHERE COALESCE(
                transaction.source_job_id,
                reversed_transaction.source_job_id
            ) = $1
            GROUP BY transaction.transaction_id, seal.sealed_at_ms
            ORDER BY transaction.created_at_ms, transaction.transaction_id
            "#,
        )
        .bind(job_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();

        let provider_costs = sqlx::query_as::<_, ProviderCostRow>(
            r#"
            WITH observation_scope AS (
                SELECT link.provider_cost_observation_id,
                       COUNT(DISTINCT fact.job_id)::BIGINT AS job_count
                FROM provider_cost_observation_fact_links link
                JOIN provider_usage_facts fact
                  ON fact.usage_fact_id = link.usage_fact_id
                GROUP BY link.provider_cost_observation_id
            ),
            job_observation_ids AS (
                SELECT DISTINCT link.provider_cost_observation_id
                FROM provider_cost_observation_fact_links link
                JOIN provider_usage_facts fact
                  ON fact.usage_fact_id = link.usage_fact_id
                WHERE fact.job_id = $1
            )
            SELECT observation.provider_cost_observation_id::TEXT AS cost_id,
                   CASE
                       WHEN source.source_kind = 'executor_verified'
                       THEN 'provider_actual'
                       ELSE 'legacy_unverified'
                   END::TEXT AS cost_basis,
                   CASE
                       WHEN scope.job_count = 1 THEN 'attributed'
                       ELSE 'shared'
                   END::TEXT AS attribution_state,
                   observation.currency,
                   observation.amount_micros::TEXT AS observed_amount_micros,
                   (
                       CASE
                           WHEN scope.job_count = 1 THEN observation.amount_micros
                       END
                   )::TEXT AS attributed_amount_micros,
                   observation.authority,
                   observation.confidence,
                   observation.price_book_version_id::TEXT,
                   transaction.transaction_id::TEXT,
                   seal.sealed_at_ms,
                   observation.created_at_ms
            FROM job_observation_ids job_observation
            JOIN provider_cost_observations observation
              ON observation.provider_cost_observation_id =
                 job_observation.provider_cost_observation_id
            JOIN provider_cost_observation_sources source
              ON source.provider_cost_observation_id =
                 observation.provider_cost_observation_id
            JOIN observation_scope scope
              ON scope.provider_cost_observation_id =
                 observation.provider_cost_observation_id
            LEFT JOIN ledger_transactions transaction
              ON transaction.source_provider_cost_observation_id =
                 observation.provider_cost_observation_id
             AND transaction.transaction_type = 'provider_cost'
            LEFT JOIN ledger_transaction_seals seal
              ON seal.transaction_id = transaction.transaction_id

            UNION ALL

            SELECT line.provider_cost_allocation_line_id::TEXT AS cost_id,
                   'provider_allocated'::TEXT AS cost_basis,
                   'attributed'::TEXT AS attribution_state,
                   pool.currency,
                   line.amount_micros::TEXT AS observed_amount_micros,
                   line.amount_micros::TEXT AS attributed_amount_micros,
                   'allocation_policy'::TEXT AS authority,
                   'exact'::TEXT AS confidence,
                   pool.price_book_version_id::TEXT,
                   transaction.transaction_id::TEXT,
                   seal.sealed_at_ms,
                   line.created_at_ms
            FROM provider_cost_allocation_lines line
            JOIN provider_cost_allocation_pools pool
              ON pool.provider_cost_allocation_pool_id =
                 line.provider_cost_allocation_pool_id
            LEFT JOIN ledger_transactions transaction
              ON transaction.source_provider_cost_allocation_line_id =
                 line.provider_cost_allocation_line_id
             AND transaction.transaction_type = 'provider_cost'
            LEFT JOIN ledger_transaction_seals seal
              ON seal.transaction_id = transaction.transaction_id
            WHERE line.job_id = $1

            UNION ALL

            SELECT receipt.receipt_id::TEXT AS cost_id,
                   'legacy_unverified'::TEXT AS cost_basis,
                   'attributed'::TEXT AS attribution_state,
                   receipt.provider_cost_currency AS currency,
                   receipt.provider_cost_micros::TEXT AS observed_amount_micros,
                   receipt.provider_cost_micros::TEXT AS attributed_amount_micros,
                   'receipt_payload'::TEXT AS authority,
                   'unverified'::TEXT AS confidence,
                   NULL::TEXT AS price_book_version_id,
                   transaction.transaction_id::TEXT,
                   seal.sealed_at_ms,
                   receipt.created_at_ms
            FROM provider_receipts receipt
            LEFT JOIN ledger_transactions transaction
              ON transaction.source_receipt_id = receipt.receipt_id
             AND transaction.transaction_type = 'provider_cost'
            LEFT JOIN ledger_transaction_seals seal
              ON seal.transaction_id = transaction.transaction_id
            WHERE receipt.job_id = $1
              AND receipt.provider_cost_micros IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM provider_cost_observation_receipts observation_receipt
                  WHERE observation_receipt.receipt_id = receipt.receipt_id
              )
            ORDER BY created_at_ms, cost_id
            "#,
        )
        .bind(job_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();

        let economics_state = economics_state(
            contract.economics_contract_version,
            customer_quote.is_some(),
            !usage_facts.is_empty(),
            customer_rating.is_some(),
        );
        tx.commit().await.map_err(unavailable)?;
        Ok(JobEconomicsSnapshot {
            as_of_ms,
            job_id: job_id.to_string(),
            economics_contract_version: contract.economics_contract_version,
            economics_state: economics_state.to_string(),
            customer_quote,
            customer_hold,
            usage_facts,
            customer_rating,
            ledger_transactions,
            provider_costs,
        })
    }
}

fn economics_state(
    contract_version: i16,
    has_quote: bool,
    has_usage: bool,
    has_rating: bool,
) -> &'static str {
    if contract_version != 4 {
        "legacy_contract"
    } else if has_rating {
        "rated"
    } else if has_usage {
        "metered"
    } else if has_quote {
        "quoted"
    } else {
        "awaiting_quote"
    }
}

#[derive(FromRow)]
struct JobContractRow {
    economics_contract_version: i16,
}

#[derive(FromRow)]
struct QuoteRow {
    quote_id: String,
    price_book_version_id: String,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
    currency: String,
    is_free: bool,
    max_total_micros: String,
    created_at_ms: i64,
}

impl QuoteRow {
    fn into_quote(self, lines: Vec<JobQuoteLine>) -> JobCustomerQuote {
        JobCustomerQuote {
            quote_id: self.quote_id,
            price_book_version_id: self.price_book_version_id,
            public_model_id: self.public_model_id,
            media_kind: self.media_kind,
            service_tier: self.service_tier,
            currency: self.currency,
            is_free: self.is_free,
            max_total_micros: self.max_total_micros,
            created_at_ms: self.created_at_ms,
            lines,
        }
    }
}

#[derive(FromRow)]
struct QuoteLineRow {
    component_key: String,
    partition_key: String,
    terminal_outcome: String,
    metric: String,
    unit: String,
    unit_size: String,
    unit_price_micros: String,
    reservation_quantity_source: String,
    reservation_confidence: String,
    max_quantity: String,
    max_amount_micros: String,
    actual_quantity: Option<String>,
    actual_amount_micros: Option<String>,
}

impl From<QuoteLineRow> for JobQuoteLine {
    fn from(row: QuoteLineRow) -> Self {
        Self {
            component_key: row.component_key,
            partition_key: row.partition_key,
            terminal_outcome: row.terminal_outcome,
            metric: row.metric,
            unit: row.unit,
            unit_size: row.unit_size,
            unit_price_micros: row.unit_price_micros,
            reservation_quantity_source: row.reservation_quantity_source,
            reservation_confidence: row.reservation_confidence,
            max_quantity: row.max_quantity,
            max_amount_micros: row.max_amount_micros,
            actual_quantity: row.actual_quantity,
            actual_amount_micros: row.actual_amount_micros,
        }
    }
}

#[derive(FromRow)]
struct HoldRow {
    state: String,
    currency: String,
    held_micros: String,
    captured_micros: String,
    released_micros: String,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl From<HoldRow> for JobCustomerHold {
    fn from(row: HoldRow) -> Self {
        Self {
            state: row.state,
            currency: row.currency,
            held_micros: row.held_micros,
            captured_micros: row.captured_micros,
            released_micros: row.released_micros,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        }
    }
}

#[derive(FromRow)]
struct UsageFactRow {
    metric: String,
    quantity: String,
    unit: String,
    quantity_source: String,
    confidence: String,
    billing_partition_key: String,
    terminal_outcome: String,
    created_at_ms: i64,
}

impl From<UsageFactRow> for JobUsageFact {
    fn from(row: UsageFactRow) -> Self {
        Self {
            metric: row.metric,
            quantity: row.quantity,
            unit: row.unit,
            quantity_source: row.quantity_source,
            confidence: row.confidence,
            billing_partition_key: row.billing_partition_key,
            terminal_outcome: row.terminal_outcome,
            created_at_ms: row.created_at_ms,
        }
    }
}

#[derive(FromRow)]
struct RatingRow {
    currency: String,
    total_amount_micros: String,
    created_at_ms: i64,
}

impl From<RatingRow> for JobCustomerRating {
    fn from(row: RatingRow) -> Self {
        Self {
            currency: row.currency,
            total_amount_micros: row.total_amount_micros,
            created_at_ms: row.created_at_ms,
        }
    }
}

#[derive(FromRow)]
struct LedgerTransactionRow {
    transaction_id: String,
    transaction_type: String,
    currency: String,
    amount_micros: String,
    created_at_ms: i64,
    sealed_at_ms: Option<i64>,
}

impl From<LedgerTransactionRow> for JobLedgerTransaction {
    fn from(row: LedgerTransactionRow) -> Self {
        Self {
            transaction_id: row.transaction_id,
            transaction_type: row.transaction_type,
            currency: row.currency,
            amount_micros: row.amount_micros,
            created_at_ms: row.created_at_ms,
            sealed_at_ms: row.sealed_at_ms,
        }
    }
}

#[derive(FromRow)]
struct ProviderCostRow {
    cost_id: String,
    cost_basis: String,
    attribution_state: String,
    currency: String,
    observed_amount_micros: String,
    attributed_amount_micros: Option<String>,
    authority: String,
    confidence: String,
    price_book_version_id: Option<String>,
    transaction_id: Option<String>,
    sealed_at_ms: Option<i64>,
    created_at_ms: i64,
}

impl From<ProviderCostRow> for JobProviderCost {
    fn from(row: ProviderCostRow) -> Self {
        Self {
            cost_id: row.cost_id,
            cost_basis: row.cost_basis,
            attribution_state: row.attribution_state,
            currency: row.currency,
            observed_amount_micros: row.observed_amount_micros,
            attributed_amount_micros: row.attributed_amount_micros,
            authority: row.authority,
            confidence: row.confidence,
            price_book_version_id: row.price_book_version_id,
            transaction_id: row.transaction_id,
            sealed_at_ms: row.sealed_at_ms,
            created_at_ms: row.created_at_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::economics_state;

    #[test]
    fn economics_state_never_presents_legacy_jobs_as_v4_rated() {
        assert_eq!(economics_state(3, true, true, true), "legacy_contract");
        assert_eq!(economics_state(4, false, false, false), "awaiting_quote");
        assert_eq!(economics_state(4, true, false, false), "quoted");
        assert_eq!(economics_state(4, true, true, false), "metered");
        assert_eq!(economics_state(4, true, true, true), "rated");
    }
}
