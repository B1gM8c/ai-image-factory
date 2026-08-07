use sqlx::{FromRow, PgPool, Postgres, QueryBuilder, Transaction};
use uuid::Uuid;

use super::{
    AdminReadError, AdminReadScope, AdminReadStore, AuditLogActor, AuditLogItem, AuditLogProject,
    AuditLogResource, AuditLogsQuery, AuditLogsSnapshot, BillingAccountSnapshot, BillingSnapshot,
    BlockedTerminalReduction, JobCursor, JobEconomicsSnapshot, JobListItem, JobsQuery,
    JobsSnapshot, LedgerAggregate, MAX_AUDIT_LOG_PAGE_SIZE, MAX_AUDIT_LOG_WINDOW_MS,
    MAX_BILLING_WINDOW_MS, MAX_JOBS_PAGE_SIZE, MAX_JOBS_WINDOW_MS, MAX_OVERVIEW_WINDOW_MS,
    MAX_REQUEST_LOG_WINDOW_MS, MAX_SCHEDULER_WINDOW_MS, OverviewSnapshot,
    ProviderAccountConcurrency, ProviderAccountConcurrencySnapshot, ProviderAccountView,
    ProviderAccountsSnapshot, ProviderCostAggregate, ProviderCostCoverage, ProviderQueuePressure,
    ProviderStateCount, RatedUsageAggregate, RequestLogCursor, RequestLogItem,
    RequestLogVisibility, RequestLogsQuery, RequestLogsSnapshot, SchedulerActiveJob,
    SchedulerCapacity, SchedulerSnapshot, StageCount, StateCount, UpstreamQuotaObservation,
    UpstreamQuotaWindow, UsageAggregate, UsageAnalysisQuery, UsageAnalysisSnapshot, WorkStateCount,
    unknown_upstream_quota,
};

mod job_economics;

#[derive(Clone)]
pub struct PostgresAdminReadStore {
    pool: PgPool,
}

impl PostgresAdminReadStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub(super) async fn begin_snapshot(
        &self,
        window_ms: i64,
        max_window_ms: i64,
    ) -> Result<(Transaction<'_, Postgres>, ReadWindow), AdminReadError> {
        validate_window(window_ms, max_window_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let as_of_ms: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM transaction_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        let from_ms = as_of_ms
            .checked_sub(window_ms)
            .ok_or_else(|| invalid("window underflows the database clock"))?;
        Ok((
            tx,
            ReadWindow {
                as_of_ms,
                from_ms,
                to_ms: as_of_ms,
            },
        ))
    }

    async fn begin_current_snapshot(
        &self,
    ) -> Result<(Transaction<'_, Postgres>, i64), AdminReadError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let as_of_ms = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM transaction_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        Ok((tx, as_of_ms))
    }

    async fn begin_anchored_snapshot(
        &self,
        window_ms: i64,
        max_window_ms: i64,
        requested_to_ms: Option<i64>,
    ) -> Result<(Transaction<'_, Postgres>, ReadWindow), AdminReadError> {
        validate_window(window_ms, max_window_ms)?;
        let (tx, as_of_ms) = self.begin_current_snapshot().await?;
        let to_ms = requested_to_ms.unwrap_or(as_of_ms);
        if to_ms <= 0 {
            return Err(invalid("to_ms must be positive"));
        }
        if to_ms > as_of_ms {
            return Err(invalid("to_ms cannot be later than the database clock"));
        }
        let from_ms = to_ms
            .checked_sub(window_ms)
            .ok_or_else(|| invalid("window underflows the anchored upper bound"))?;
        Ok((
            tx,
            ReadWindow {
                as_of_ms,
                from_ms,
                to_ms,
            },
        ))
    }
}

#[async_trait::async_trait]
impl AdminReadStore for PostgresAdminReadStore {
    async fn overview(&self, window_ms: i64) -> Result<OverviewSnapshot, AdminReadError> {
        self.overview_scoped(&AdminReadScope::Platform, window_ms)
            .await
    }

    async fn overview_scoped(
        &self,
        scope: &AdminReadScope,
        window_ms: i64,
    ) -> Result<OverviewSnapshot, AdminReadError> {
        let (mut tx, window) = self
            .begin_snapshot(window_ms, MAX_OVERVIEW_WINDOW_MS)
            .await?;
        let tenant_ids = scope.tenant_ids().map(|ids| ids.to_vec());
        let actor_user_id = scope.actor_user_id();
        let job_states = sqlx::query_as::<_, StateCountRow>(
            r#"
            SELECT state, COUNT(*)::TEXT AS count
            FROM jobs
            WHERE created_at_ms >= $1 AND created_at_ms < $2
              AND ($3::TEXT[] IS NULL OR tenant_id = ANY($3))
              AND (
                $4::UUID IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions attribution
                  WHERE attribution.job_id = jobs.job_id
                    AND (
                      attribution.actor_user_id = $4
                      OR attribution.credential_owner_user_id = $4
                    )
                )
              )
            GROUP BY state
            ORDER BY state
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let charged_usage = sqlx::query_as::<_, UsageRow>(
            r#"
            SELECT NULL::TEXT AS tenant_id, billing_metric, billing_unit, outcome,
                   SUM(units::NUMERIC)::TEXT AS quantity
            FROM usage_events usage
            WHERE created_at_ms >= $1 AND created_at_ms < $2
              AND ($3::TEXT[] IS NULL OR tenant_id = ANY($3))
              AND (
                $4::UUID IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions attribution
                  WHERE attribution.job_id = usage.job_id
                    AND (
                      attribution.actor_user_id = $4
                      OR attribution.credential_owner_user_id = $4
                    )
                )
              )
            GROUP BY billing_metric, billing_unit, outcome
            ORDER BY billing_metric, billing_unit, outcome
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let sealed_ledger = sqlx::query_as::<_, LedgerRow>(
            r#"
            SELECT NULL::TEXT AS tenant_id, transaction.transaction_type,
                   transaction.currency,
                   SUM(posting.amount_micros::NUMERIC)::TEXT AS amount_micros,
                   COUNT(DISTINCT transaction.transaction_id)::TEXT AS transaction_count
            FROM ledger_transaction_seals seal
            JOIN ledger_transactions transaction
              ON transaction.transaction_id = seal.transaction_id
            LEFT JOIN ledger_transactions reversed_transaction
              ON reversed_transaction.transaction_id =
                 transaction.reverses_transaction_id
            JOIN ledger_postings posting
              ON posting.transaction_id = transaction.transaction_id
             AND posting.amount_micros > 0
            JOIN jobs job
              ON job.job_id = COALESCE(
                  transaction.source_job_id,
                  reversed_transaction.source_job_id
              )
            WHERE seal.sealed_at_ms >= $1 AND seal.sealed_at_ms < $2
              AND transaction.transaction_type IN (
                  'customer_charge',
                  'customer_job_charge',
                  'customer_refund',
                  'provider_cost'
              )
              AND ($3::TEXT[] IS NULL OR job.tenant_id = ANY($3))
              AND (
                $4::UUID IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions attribution
                  WHERE attribution.job_id = job.job_id
                    AND (
                      attribution.actor_user_id = $4
                      OR attribution.credential_owner_user_id = $4
                    )
                )
              )
            GROUP BY transaction.transaction_type, transaction.currency
            ORDER BY transaction.transaction_type, transaction.currency
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let elapsed = sqlx::query_as::<_, ElapsedRow>(
            r#"
            SELECT COUNT(*)::TEXT AS samples,
                   percentile_disc(0.95) WITHIN GROUP (
                       ORDER BY finished_at_ms - created_at_ms
                   )::BIGINT AS p95_ms
            FROM jobs
            WHERE created_at_ms >= $1 AND created_at_ms < $2
              AND finished_at_ms IS NOT NULL
              AND finished_at_ms >= created_at_ms
              AND ($3::TEXT[] IS NULL OR tenant_id = ANY($3))
              AND (
                $4::UUID IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions attribution
                  WHERE attribution.job_id = jobs.job_id
                    AND (
                      attribution.actor_user_id = $4
                      OR attribution.credential_owner_user_id = $4
                    )
                )
              )
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(OverviewSnapshot {
            as_of_ms: window.as_of_ms,
            from_ms: window.from_ms,
            to_ms: window.to_ms,
            job_states,
            charged_usage,
            sealed_ledger,
            terminal_job_elapsed_p95_ms: elapsed.p95_ms,
            terminal_job_elapsed_samples: elapsed.samples,
        })
    }

    async fn billing(&self, window_ms: i64) -> Result<BillingSnapshot, AdminReadError> {
        self.billing_scoped(&AdminReadScope::Platform, window_ms, None)
            .await
    }

    async fn billing_scoped(
        &self,
        scope: &AdminReadScope,
        window_ms: i64,
        project_id: Option<&str>,
    ) -> Result<BillingSnapshot, AdminReadError> {
        let (mut tx, window) = self
            .begin_snapshot(window_ms, MAX_BILLING_WINDOW_MS)
            .await?;
        let tenant_ids = scope.tenant_ids().map(|ids| ids.to_vec());
        let actor_user_id = scope.actor_user_id_for_project(project_id)?;
        let account_snapshots = sqlx::query_as::<_, BillingAccountRow>(
            r#"
            SELECT tenant_id, currency,
                   credit_limit_micros::NUMERIC::TEXT AS credit_limit_micros,
                   held_micros::NUMERIC::TEXT AS held_micros,
                   captured_micros::NUMERIC::TEXT AS captured_micros,
                   refunded_micros::NUMERIC::TEXT AS refunded_micros,
                   (credit_limit_micros::NUMERIC - held_micros::NUMERIC
                    - captured_micros::NUMERIC
                    + refunded_micros::NUMERIC)::TEXT AS available_micros,
                   control_version::NUMERIC::TEXT AS control_version,
                   updated_at_ms
            FROM billing_accounts
            WHERE ($1::TEXT[] IS NULL OR tenant_id = ANY($1))
              AND $2::TEXT IS NULL
            ORDER BY tenant_id, currency
            "#,
        )
        .bind(&tenant_ids)
        .bind(project_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let charged_usage = sqlx::query_as::<_, UsageRow>(
            r#"
            WITH usage_lines AS (
                SELECT usage.tenant_id, usage.billing_metric,
                       usage.billing_unit, usage.outcome,
                       usage.units::NUMERIC AS quantity,
                       usage.job_id, usage.created_at_ms
                FROM usage_events usage
                LEFT JOIN jobs job ON job.job_id = usage.job_id
                WHERE job.job_id IS NULL
                   OR job.economics_contract_version IN (1, 2, 3)

                UNION ALL

                SELECT quote.tenant_id,
                       quote_line.metric AS billing_metric,
                       quote_line.unit AS billing_unit,
                       quote_line.terminal_outcome AS outcome,
                       rated_line.actual_quantity::NUMERIC AS quantity,
                       rated.job_id,
                       rated_line.created_at_ms
                FROM customer_rated_usage_lines rated_line
                JOIN customer_rated_usage rated
                  ON rated.rated_usage_id = rated_line.rated_usage_id
                JOIN customer_price_quotes quote
                  ON quote.quote_id = rated.quote_id
                 AND quote.job_id = rated.job_id
                JOIN customer_price_quote_lines quote_line
                  ON quote_line.quote_line_id = rated_line.quote_line_id
                 AND quote_line.quote_id = rated.quote_id
                 AND quote_line.job_id = rated.job_id
            )
            SELECT tenant_id, billing_metric, billing_unit, outcome,
                   SUM(quantity)::TEXT AS quantity
            FROM usage_lines
            WHERE created_at_ms >= $1 AND created_at_ms < $2
              AND ($3::TEXT[] IS NULL OR tenant_id = ANY($3))
              AND (
                $4::UUID IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions attribution
                  WHERE attribution.job_id = usage_lines.job_id
                    AND (
                      attribution.actor_user_id = $4
                      OR attribution.credential_owner_user_id = $4
                    )
                )
              )
              AND (
                $5::TEXT IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions scoped_attribution
                  WHERE scoped_attribution.job_id = usage_lines.job_id
                    AND scoped_attribution.project_id = $5
                )
              )
            GROUP BY tenant_id, billing_metric, billing_unit, outcome
            ORDER BY tenant_id, billing_metric, billing_unit, outcome
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .bind(project_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let rated_usage = sqlx::query_as::<_, RatedUsageRow>(
            r#"
            WITH rating_lines AS (
                SELECT job.tenant_id,
                       meter.metric AS billing_metric,
                       meter.unit AS billing_unit,
                       rated.outcome,
                       rated.currency,
                       rated.quantity::NUMERIC AS quantity,
                       rated.amount_micros::NUMERIC AS amount_micros,
                       rated.job_id,
                       rated.created_at_ms
                FROM rated_usage rated
                JOIN economic_metering_events meter
                  ON meter.meter_event_id = rated.meter_event_id
                JOIN jobs job ON job.job_id = rated.job_id

                UNION ALL

                SELECT quote.tenant_id,
                       quote_line.metric AS billing_metric,
                       quote_line.unit AS billing_unit,
                       quote_line.terminal_outcome AS outcome,
                       rated.currency,
                       rated_line.actual_quantity::NUMERIC AS quantity,
                       rated_line.amount_micros::NUMERIC AS amount_micros,
                       rated.job_id,
                       rated_line.created_at_ms
                FROM customer_rated_usage_lines rated_line
                JOIN customer_rated_usage rated
                  ON rated.rated_usage_id = rated_line.rated_usage_id
                JOIN customer_price_quotes quote
                  ON quote.quote_id = rated.quote_id
                 AND quote.job_id = rated.job_id
                JOIN customer_price_quote_lines quote_line
                  ON quote_line.quote_line_id = rated_line.quote_line_id
                 AND quote_line.quote_id = rated.quote_id
                 AND quote_line.job_id = rated.job_id
            )
            SELECT tenant_id, billing_metric, billing_unit, outcome, currency,
                   SUM(quantity)::TEXT AS quantity,
                   SUM(amount_micros)::TEXT AS amount_micros
            FROM rating_lines
            WHERE created_at_ms >= $1 AND created_at_ms < $2
              AND ($3::TEXT[] IS NULL OR tenant_id = ANY($3))
              AND (
                $4::UUID IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions attribution
                  WHERE attribution.job_id = rating_lines.job_id
                    AND (
                      attribution.actor_user_id = $4
                      OR attribution.credential_owner_user_id = $4
                    )
                )
              )
              AND (
                $5::TEXT IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions scoped_attribution
                  WHERE scoped_attribution.job_id = rating_lines.job_id
                    AND scoped_attribution.project_id = $5
                )
              )
            GROUP BY tenant_id, billing_metric, billing_unit,
                     outcome, currency
            ORDER BY tenant_id, billing_metric, billing_unit,
                     outcome, currency
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .bind(project_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let sealed_ledger = sqlx::query_as::<_, LedgerRow>(
            r#"
            WITH observation_attribution AS (
                SELECT link.provider_cost_observation_id,
                       CASE
                           WHEN COUNT(DISTINCT job.tenant_id) = 1
                           THEN MIN(job.tenant_id)
                       END AS tenant_id,
                       CASE
                           WHEN COUNT(DISTINCT job_attribution.project_id) = 1
                           THEN MIN(job_attribution.project_id)
                       END AS project_id
                FROM provider_cost_observation_fact_links link
                JOIN provider_usage_facts fact
                  ON fact.usage_fact_id = link.usage_fact_id
                JOIN jobs job ON job.job_id = fact.job_id
                LEFT JOIN job_auth_attributions job_attribution
                  ON job_attribution.job_id = fact.job_id
                GROUP BY link.provider_cost_observation_id
            )
            SELECT COALESCE(job.tenant_id, observation_attribution.tenant_id)
                       AS tenant_id,
                   transaction.transaction_type,
                   transaction.currency,
                   SUM(posting.amount_micros::NUMERIC)::TEXT AS amount_micros,
                   COUNT(DISTINCT transaction.transaction_id)::TEXT AS transaction_count
            FROM ledger_transaction_seals seal
            JOIN ledger_transactions transaction
              ON transaction.transaction_id = seal.transaction_id
            LEFT JOIN ledger_transactions reversed_transaction
              ON reversed_transaction.transaction_id =
                 transaction.reverses_transaction_id
            JOIN ledger_postings posting
              ON posting.transaction_id = transaction.transaction_id
             AND posting.amount_micros > 0
            LEFT JOIN jobs job
              ON job.job_id = COALESCE(
                  transaction.source_job_id,
                  reversed_transaction.source_job_id
              )
            LEFT JOIN job_auth_attributions transaction_attribution
              ON transaction_attribution.job_id = COALESCE(
                  transaction.source_job_id,
                  reversed_transaction.source_job_id
              )
            LEFT JOIN observation_attribution
              ON observation_attribution.provider_cost_observation_id =
                 transaction.source_provider_cost_observation_id
            WHERE seal.sealed_at_ms >= $1 AND seal.sealed_at_ms < $2
              AND transaction.transaction_type IN (
                  'customer_charge',
                  'customer_job_charge',
                  'customer_refund',
                  'provider_cost'
              )
              AND (
                  $3::TEXT[] IS NULL
                  OR COALESCE(
                      job.tenant_id, observation_attribution.tenant_id
                  ) = ANY($3)
              )
              AND (
                $4::UUID IS NULL
                OR EXISTS (
                  SELECT 1 FROM job_auth_attributions attribution
                  WHERE attribution.job_id = COALESCE(
                      transaction.source_job_id,
                      reversed_transaction.source_job_id
                  )
                    AND (
                      attribution.actor_user_id = $4
                      OR attribution.credential_owner_user_id = $4
                    )
                )
              )
              AND (
                $5::TEXT IS NULL
                OR COALESCE(
                    transaction_attribution.project_id,
                    observation_attribution.project_id
                ) = $5
              )
            GROUP BY COALESCE(
                         job.tenant_id, observation_attribution.tenant_id
                     ),
                     transaction.transaction_type, transaction.currency
            ORDER BY tenant_id, transaction.transaction_type,
                     transaction.currency
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .bind(&tenant_ids)
        .bind(actor_user_id)
        .bind(project_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let (provider_costs, provider_cost_coverage) = if matches!(scope, AdminReadScope::Platform)
        {
            let costs = sqlx::query_as::<_, ProviderCostRow>(
                r#"
                    WITH observation_attribution AS (
                        SELECT link.provider_cost_observation_id,
                               CASE
                                   WHEN COUNT(DISTINCT job.tenant_id) = 1
                                   THEN MIN(job.tenant_id)
                               END AS tenant_id,
                               CASE
                                   WHEN COUNT(DISTINCT job_attribution.project_id) = 1
                                   THEN MIN(job_attribution.project_id)
                               END AS project_id,
                               CASE
                                   WHEN COUNT(DISTINCT fact.terminal_outcome) = 1
                                   THEN MIN(fact.terminal_outcome)
                                   ELSE 'mixed'
                               END AS outcome,
                               COUNT(DISTINCT fact.receipt_id)::NUMERIC
                                   AS linked_receipts
                        FROM provider_cost_observation_fact_links link
                        JOIN provider_usage_facts fact
                          ON fact.usage_fact_id = link.usage_fact_id
                        JOIN jobs job ON job.job_id = fact.job_id
                        LEFT JOIN job_auth_attributions job_attribution
                          ON job_attribution.job_id = fact.job_id
                        GROUP BY link.provider_cost_observation_id
                    ),
                    cost_transactions AS (
                        SELECT transaction.transaction_id,
                               COALESCE(
                                   job.tenant_id,
                                   observation_attribution.tenant_id
                               ) AS tenant_id,
                               COALESCE(
                                   transaction_attribution.project_id,
                                   observation_attribution.project_id
                               ) AS project_id,
                               COALESCE(
                                   observation.provider_id,
                                   allocation_pool.provider_id,
                                   receipt.provider_id,
                                   'unknown'
                               ) AS provider_id,
                               COALESCE(
                                   receipt.outcome,
                                   observation_attribution.outcome,
                                   job.state,
                                   'unknown'
                               ) AS outcome,
                               CASE
                                   WHEN transaction.source_provider_cost_observation_id
                                        IS NOT NULL
                                        AND observation_source.source_kind =
                                            'executor_verified'
                                   THEN 'provider_actual'
                                   WHEN transaction.source_provider_cost_allocation_line_id
                                        IS NOT NULL
                                   THEN 'provider_allocated'
                                   ELSE 'legacy_unverified'
                               END AS cost_basis,
                               CASE
                                   WHEN COALESCE(
                                       job.tenant_id,
                                       observation_attribution.tenant_id
                                   ) IS NULL
                                   THEN 'unattributed'
                                   ELSE 'attributed'
                               END AS attribution_state,
                               transaction.currency,
                               SUM(posting.amount_micros::NUMERIC)
                                   AS amount_micros,
                               COALESCE(
                                   observation_attribution.linked_receipts,
                                   CASE
                                       WHEN transaction.source_receipt_id IS NULL
                                       THEN 0::NUMERIC
                                       ELSE 1::NUMERIC
                                   END
                               ) AS linked_receipts
                        FROM ledger_transaction_seals seal
                        JOIN ledger_transactions transaction
                          ON transaction.transaction_id = seal.transaction_id
                        JOIN ledger_postings posting
                          ON posting.transaction_id = transaction.transaction_id
                         AND posting.amount_micros > 0
                        LEFT JOIN jobs job
                          ON job.job_id = transaction.source_job_id
                        LEFT JOIN job_auth_attributions transaction_attribution
                          ON transaction_attribution.job_id =
                             transaction.source_job_id
                        LEFT JOIN provider_receipts receipt
                          ON receipt.receipt_id = transaction.source_receipt_id
                        LEFT JOIN provider_cost_observations observation
                          ON observation.provider_cost_observation_id =
                             transaction.source_provider_cost_observation_id
                        LEFT JOIN provider_cost_observation_sources
                                  observation_source
                          ON observation_source.provider_cost_observation_id =
                             transaction.source_provider_cost_observation_id
                        LEFT JOIN provider_cost_allocation_pools allocation_pool
                          ON allocation_pool.provider_cost_allocation_pool_id =
                             transaction.source_provider_cost_allocation_pool_id
                        LEFT JOIN observation_attribution
                          ON observation_attribution.provider_cost_observation_id =
                             transaction.source_provider_cost_observation_id
                        WHERE seal.sealed_at_ms >= $1
                          AND seal.sealed_at_ms < $2
                          AND transaction.transaction_type = 'provider_cost'
                          AND (
                              $3::TEXT IS NULL
                              OR COALESCE(
                                  transaction_attribution.project_id,
                                  observation_attribution.project_id
                              ) = $3
                          )
                        GROUP BY transaction.transaction_id,
                                 job.tenant_id,
                                 observation_attribution.tenant_id,
                                 transaction_attribution.project_id,
                                 observation_attribution.project_id,
                                 observation.provider_id,
                                 allocation_pool.provider_id,
                                 receipt.provider_id,
                                 receipt.outcome,
                                 observation_attribution.outcome,
                                 job.state,
                                 observation_source.source_kind,
                                 observation_attribution.linked_receipts,
                                 transaction.source_receipt_id
                    )
                    SELECT tenant_id, provider_id, outcome, cost_basis,
                           attribution_state, currency,
                           SUM(amount_micros)::TEXT AS amount_micros,
                           COUNT(*)::TEXT AS transaction_count,
                           SUM(linked_receipts)::TEXT AS linked_receipts
                    FROM cost_transactions
                    GROUP BY tenant_id, provider_id, outcome, cost_basis,
                             attribution_state, currency
                    ORDER BY cost_basis, provider_id, tenant_id,
                             outcome, currency
                    "#,
            )
            .bind(window.from_ms)
            .bind(window.to_ms)
            .bind(project_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?
            .into_iter()
            .map(Into::into)
            .collect();
            let coverage = sqlx::query_as::<_, ProviderCoverageRow>(
                r#"
                    WITH terminal AS (
                        SELECT receipt.receipt_id, receipt.job_id,
                               receipt.output_id
                        FROM provider_receipts receipt
                        JOIN jobs job ON job.job_id = receipt.job_id
                        LEFT JOIN job_auth_attributions job_attribution
                          ON job_attribution.job_id = receipt.job_id
                        WHERE receipt.created_at_ms >= $1
                          AND receipt.created_at_ms < $2
                          AND (
                              $3::TEXT IS NULL
                              OR job_attribution.project_id = $3
                          )
                    ),
                    covered AS (
                        SELECT terminal.receipt_id
                        FROM terminal
                        WHERE EXISTS (
                            SELECT 1
                            FROM provider_cost_observation_receipts link
                            JOIN provider_cost_observation_sources source
                              ON source.provider_cost_observation_id =
                                 link.provider_cost_observation_id
                             AND source.source_kind = 'executor_verified'
                            WHERE link.receipt_id = terminal.receipt_id
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM ledger_transactions transaction
                            JOIN ledger_transaction_seals seal
                              ON seal.transaction_id = transaction.transaction_id
                            WHERE transaction.transaction_type = 'provider_cost'
                              AND transaction.source_receipt_id =
                                  terminal.receipt_id
                              AND transaction.source_provider_cost_observation_id
                                  IS NULL
                              AND transaction.source_provider_cost_allocation_line_id
                                  IS NULL
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM provider_cost_allocation_lines line
                            JOIN provider_cost_allocation_pools pool
                              ON pool.provider_cost_allocation_pool_id =
                                 line.provider_cost_allocation_pool_id
                             AND pool.state = 'closed'
                            WHERE line.job_id = terminal.job_id
                              AND (
                                  line.output_id IS NULL
                                  OR line.output_id = terminal.output_id
                              )
                        )
                    ),
                    window_cost_transactions AS (
                        SELECT transaction.transaction_id,
                               transaction.source_job_id,
                               transaction.source_provider_cost_observation_id,
                               transaction.source_provider_cost_allocation_line_id,
                               CASE
                                   WHEN transaction.source_provider_cost_observation_id
                                        IS NOT NULL
                                        AND observation_source.source_kind =
                                            'executor_verified'
                                   THEN 'provider_actual'
                                   WHEN transaction.source_provider_cost_allocation_line_id
                                        IS NOT NULL
                                   THEN 'provider_allocated'
                                   ELSE 'legacy_unverified'
                               END AS cost_basis
                        FROM ledger_transactions transaction
                        JOIN ledger_transaction_seals seal
                          ON seal.transaction_id = transaction.transaction_id
                        LEFT JOIN provider_cost_observation_sources
                                  observation_source
                          ON observation_source.provider_cost_observation_id =
                             transaction.source_provider_cost_observation_id
                        WHERE transaction.transaction_type = 'provider_cost'
                          AND seal.sealed_at_ms >= $1
                          AND seal.sealed_at_ms < $2
                          AND (
                              $3::TEXT IS NULL
                              OR EXISTS (
                                  SELECT 1
                                  FROM job_auth_attributions scoped_attribution
                                  WHERE scoped_attribution.job_id =
                                        transaction.source_job_id
                                    AND scoped_attribution.project_id = $3
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM provider_cost_observation_fact_links scoped_link
                                  JOIN provider_usage_facts scoped_fact
                                    ON scoped_fact.usage_fact_id =
                                       scoped_link.usage_fact_id
                                  JOIN jobs scoped_job
                                    ON scoped_job.job_id =
                                       scoped_fact.job_id
                                  JOIN job_auth_attributions scoped_attribution
                                    ON scoped_attribution.job_id =
                                       scoped_fact.job_id
                                  WHERE scoped_link.provider_cost_observation_id =
                                        transaction.source_provider_cost_observation_id
                                    AND scoped_attribution.project_id = $3
                              )
                          )
                    ),
                    actual_jobs AS (
                        SELECT DISTINCT observation.provider_id,
                               observation.provider_account_id,
                               fact.job_id
                        FROM window_cost_transactions transaction
                        JOIN provider_cost_observations observation
                          ON observation.provider_cost_observation_id =
                             transaction.source_provider_cost_observation_id
                        JOIN provider_cost_observation_fact_links link
                          ON link.provider_cost_observation_id =
                             transaction.source_provider_cost_observation_id
                        JOIN provider_usage_facts fact
                          ON fact.usage_fact_id = link.usage_fact_id
                        WHERE transaction.cost_basis = 'provider_actual'
                    ),
                    allocated_jobs AS (
                        SELECT DISTINCT line.provider_id,
                               line.provider_account_id, line.job_id
                        FROM window_cost_transactions transaction
                        JOIN provider_cost_allocation_lines line
                          ON line.provider_cost_allocation_line_id =
                             transaction.source_provider_cost_allocation_line_id
                        WHERE transaction.cost_basis = 'provider_allocated'
                    ),
                    unattributed_actual AS (
                        SELECT transaction.transaction_id
                        FROM window_cost_transactions transaction
                        LEFT JOIN LATERAL (
                            SELECT COUNT(DISTINCT job.tenant_id) AS tenants
                            FROM provider_cost_observation_fact_links link
                            JOIN provider_usage_facts fact
                              ON fact.usage_fact_id = link.usage_fact_id
                            JOIN jobs job ON job.job_id = fact.job_id
                            WHERE link.provider_cost_observation_id =
                                  transaction.source_provider_cost_observation_id
                        ) attribution ON TRUE
                        WHERE transaction.cost_basis = 'provider_actual'
                          AND COALESCE(attribution.tenants, 0) <> 1
                    )
                    SELECT
                        (SELECT COUNT(*) FROM terminal)::TEXT
                            AS terminal_receipts,
                        (SELECT COUNT(DISTINCT receipt_id) FROM covered)::TEXT
                            AS covered_receipts,
                        (
                            (SELECT COUNT(*) FROM terminal)
                            - (SELECT COUNT(DISTINCT receipt_id) FROM covered)
                        )::TEXT AS uncovered_receipts,
                        COUNT(*) FILTER (
                            WHERE cost_basis = 'provider_actual'
                        )::TEXT AS provider_actual_transactions,
                        COUNT(*) FILTER (
                            WHERE cost_basis = 'provider_allocated'
                        )::TEXT AS provider_allocated_transactions,
                        COUNT(*) FILTER (
                            WHERE cost_basis = 'legacy_unverified'
                        )::TEXT AS legacy_unverified_transactions,
                        (SELECT COUNT(*) FROM unattributed_actual)::TEXT
                            AS unattributed_transactions,
                        (
                            SELECT COUNT(*)
                            FROM actual_jobs
                            JOIN allocated_jobs USING (
                                provider_id, provider_account_id, job_id
                            )
                        )::TEXT AS authority_conflicts
                    FROM window_cost_transactions
                    "#,
            )
            .bind(window.from_ms)
            .bind(window.to_ms)
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?
            .into();
            (costs, coverage)
        } else {
            (Vec::new(), ProviderCostCoverage::empty())
        };
        tx.commit().await.map_err(unavailable)?;
        Ok(BillingSnapshot {
            as_of_ms: window.as_of_ms,
            from_ms: window.from_ms,
            to_ms: window.to_ms,
            account_snapshots,
            charged_usage,
            rated_usage,
            sealed_ledger,
            provider_costs,
            provider_cost_coverage,
        })
    }

    async fn usage_analysis_scoped(
        &self,
        scope: &AdminReadScope,
        query: UsageAnalysisQuery,
    ) -> Result<UsageAnalysisSnapshot, AdminReadError> {
        self.usage_analysis_scoped_impl(scope, query).await
    }

    async fn provider_accounts(&self) -> Result<ProviderAccountsSnapshot, AdminReadError> {
        let (mut tx, as_of_ms) = self.begin_current_snapshot().await?;
        let accounts = sqlx::query_as::<_, ProviderAccountRow>(
            r#"
            WITH profile_runtime AS (
                SELECT account.provider_account_id, account.account_key,
                       account.provider_id, account.state AS account_state,
                       pool.state AS credential_pool_state,
                       profile.execution_profile_id, profile.profile_key,
                       profile.operation_id, profile.completion_mode,
                       profile.state AS profile_state,
                       policy.state AS resource_policy_state,
                       control.lifecycle_state AS scheduling_state,
                       control.control_version,
                       control.desired_max_concurrency AS max_concurrency,
                       policy.allocated_count,
                       (
                           profile.state = 'enabled'
                           AND pool.state = 'enabled'
                           AND account.state = 'enabled'
                           AND policy.state = 'enabled'
                           AND control.lifecycle_state = 'active'
                           AND account.credential_ref = profile.credential_ref
                           AND account.credential_revision = profile.credential_revision
                           AND policy.credential_pool_id = profile.credential_pool_id
                           AND policy.provider_account_id = profile.provider_account_id
                           AND policy.provider_id = profile.provider_id
                           AND control.desired_max_concurrency BETWEEN 1 AND policy.max_concurrency
                       ) AS runnable,
                       COUNT(*) FILTER (
                           WHERE runtime.runtime_role = 'submit'
                             AND runtime.state = 'active'
                             AND runtime.lease_expires_at_ms > $1
                       )::BIGINT AS active_submitters,
                       COUNT(*) FILTER (
                           WHERE runtime.runtime_role = 'poll'
                             AND runtime.state = 'active'
                             AND runtime.lease_expires_at_ms > $1
                       )::BIGINT AS active_pollers,
                       COUNT(*) FILTER (
                           WHERE runtime.runtime_role = 'submit'
                             AND runtime.state = 'draining'
                             AND runtime.lease_expires_at_ms > $1
                       )::BIGINT AS draining_submitters,
                       COUNT(*) FILTER (
                           WHERE runtime.runtime_role = 'poll'
                             AND runtime.state = 'draining'
                             AND runtime.lease_expires_at_ms > $1
                       )::BIGINT AS draining_pollers
                FROM provider_execution_profiles profile
                JOIN provider_credential_pools pool
                  ON pool.credential_pool_id = profile.credential_pool_id
                 AND pool.provider_id = profile.provider_id
                JOIN provider_accounts account
                  ON account.provider_account_id = profile.provider_account_id
                 AND account.credential_pool_id = profile.credential_pool_id
                 AND account.provider_id = profile.provider_id
                JOIN executor_resource_policies policy
                  ON policy.resource_policy_id = profile.resource_policy_id
                 AND policy.revision = profile.resource_policy_revision
                JOIN provider_account_execution_controls control
                  ON control.provider_account_id = profile.provider_account_id
                LEFT JOIN provider_runtime_leases runtime
                  ON runtime.execution_profile_id = profile.execution_profile_id
                GROUP BY account.provider_account_id, account.account_key,
                         account.provider_id, account.state, account.credential_ref,
                         account.credential_revision, pool.state,
                         profile.execution_profile_id, profile.profile_key,
                         profile.operation_id, profile.completion_mode, profile.state,
                         profile.credential_ref, profile.credential_revision,
                         profile.credential_pool_id, profile.provider_account_id,
                         policy.max_concurrency, policy.allocated_count, policy.state,
                         control.lifecycle_state, control.control_version,
                         control.desired_max_concurrency,
                         policy.credential_pool_id, policy.provider_account_id,
                         policy.provider_id
            )
            SELECT profile_runtime.provider_account_id,
                   profile_runtime.account_key, profile_runtime.provider_id,
                   environment.display_name, environment.account_email,
                   environment.state AS environment_state,
                   quota.status AS quota_status, quota.plan_type,
                   quota.credits_balance, quota.credits_unlimited,
                   quota.observed_at_ms AS quota_observed_at_ms,
                   COALESCE((
                       SELECT jsonb_agg(jsonb_build_object(
                           'limit_id', quota_window.limit_id,
                           'limit_name', quota_window.limit_name,
                           'window_role', quota_window.window_role,
                           'window_duration_mins', quota_window.window_duration_mins,
                           'used_percent', quota_window.used_percent,
                           'resets_at_ms', quota_window.resets_at_ms
                       ) ORDER BY quota_window.window_duration_mins NULLS LAST,
                                  quota_window.limit_id, quota_window.window_role)
                       FROM provider_account_quota_windows quota_window
                       WHERE quota_window.provider_account_id = profile_runtime.provider_account_id
                   ), '[]'::JSONB) AS quota_windows,
                   profile_runtime.account_state,
                   profile_runtime.credential_pool_state,
                   credential_head.lifecycle_state AS credential_lifecycle_state,
                   credential_head.refresh_strategy AS credential_refresh_strategy,
                   credential_head.active_revision AS operational_credential_revision,
                   credential_revision.access_expires_at_ms AS credential_access_expires_at_ms,
                   credential_head.next_refresh_at_ms AS credential_next_refresh_at_ms,
                   credential_head.last_success_at_ms AS credential_last_success_at_ms,
                   credential_head.consecutive_failures AS credential_consecutive_failures,
                   credential_head.last_error_code AS credential_last_error_code,
                   profile_runtime.execution_profile_id,
                   profile_runtime.profile_key,
                   profile_runtime.operation_id,
                   profile_runtime.completion_mode,
                   profile_runtime.profile_state,
                   profile_runtime.resource_policy_state,
                   profile_runtime.scheduling_state,
                   profile_runtime.control_version,
                   CASE WHEN profile_runtime.runnable THEN 'configured' ELSE 'blocked' END
                       AS configuration_status,
                   CASE
                       WHEN profile_runtime.completion_mode = 'inline' THEN 'unobserved'
                       WHEN NOT profile_runtime.runnable THEN 'blocked'
                       WHEN profile_runtime.active_submitters > 0
                         AND profile_runtime.active_pollers > 0 THEN 'active'
                       WHEN profile_runtime.draining_submitters > 0
                         OR profile_runtime.draining_pollers > 0 THEN 'draining'
                       ELSE 'configured'
                   END AS runtime_status,
                   profile_runtime.max_concurrency::TEXT AS max_concurrency,
                   profile_runtime.allocated_count::TEXT AS allocated_count,
                   GREATEST(profile_runtime.max_concurrency::NUMERIC
                     - profile_runtime.allocated_count::NUMERIC, 0)::TEXT
                       AS available_capacity,
                   profile_runtime.active_submitters::TEXT AS active_submitters,
                   profile_runtime.active_pollers::TEXT AS active_pollers,
                   profile_runtime.draining_submitters::TEXT AS draining_submitters,
                   profile_runtime.draining_pollers::TEXT AS draining_pollers
            FROM profile_runtime
            LEFT JOIN provider_account_environments environment
              ON environment.provider_account_id = profile_runtime.provider_account_id
             AND environment.provider_id = profile_runtime.provider_id
            LEFT JOIN provider_account_quota_snapshots quota
              ON quota.provider_account_id = profile_runtime.provider_account_id
            JOIN provider_account_credential_heads credential_head
              ON credential_head.provider_account_id = profile_runtime.provider_account_id
            JOIN provider_account_credential_revisions credential_revision
              ON credential_revision.provider_account_id = credential_head.provider_account_id
             AND credential_revision.revision = credential_head.active_revision
            WHERE profile_runtime.execution_profile_id = (
                SELECT candidate.execution_profile_id
                FROM profile_runtime candidate
                WHERE candidate.provider_account_id = profile_runtime.provider_account_id
                ORDER BY CASE candidate.operation_id
                             WHEN 'images.generations' THEN 0
                             WHEN 'images.edits' THEN 1
                             WHEN 'videos.generations' THEN 2
                             ELSE 3
                         END,
                         candidate.profile_key,
                         candidate.execution_profile_id
                LIMIT 1
            )
            ORDER BY profile_runtime.provider_id, profile_runtime.account_key,
                     profile_runtime.profile_key
            "#,
        )
        .bind(as_of_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderAccountsSnapshot { as_of_ms, accounts })
    }

    async fn provider_account_concurrency(
        &self,
        provider_account_ids: Option<&[Uuid]>,
    ) -> Result<ProviderAccountConcurrencySnapshot, AdminReadError> {
        let (mut tx, as_of_ms) = self.begin_current_snapshot().await?;
        let provider_account_ids = provider_account_ids.map(<[Uuid]>::to_vec);
        let accounts = sqlx::query_as::<_, ProviderAccountConcurrencyRow>(
            r#"
            WITH ranked_runtime AS (
                SELECT profile.provider_account_id,
                       control.desired_max_concurrency AS max_concurrency,
                       policy.allocated_count,
                       ROW_NUMBER() OVER (
                           PARTITION BY profile.provider_account_id
                           ORDER BY CASE profile.operation_id
                                        WHEN 'images.generations' THEN 0
                                        WHEN 'images.edits' THEN 1
                                        WHEN 'videos.generations' THEN 2
                                        ELSE 3
                                    END,
                                    profile.profile_key,
                                    profile.execution_profile_id
                       ) AS runtime_rank
                FROM provider_execution_profiles profile
                JOIN executor_resource_policies policy
                  ON policy.resource_policy_id = profile.resource_policy_id
                 AND policy.revision = profile.resource_policy_revision
                JOIN provider_account_execution_controls control
                  ON control.provider_account_id = profile.provider_account_id
                WHERE $1::UUID[] IS NULL
                   OR profile.provider_account_id = ANY($1)
            )
            SELECT provider_account_id,
                   max_concurrency::TEXT AS max_concurrency,
                   allocated_count::TEXT AS allocated_count,
                   GREATEST(max_concurrency::NUMERIC - allocated_count::NUMERIC, 0)::TEXT
                       AS available_capacity
            FROM ranked_runtime
            WHERE runtime_rank = 1
            ORDER BY provider_account_id
            "#,
        )
        .bind(provider_account_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let queue = sqlx::query_as::<_, ProviderQueuePressureRow>(
            r#"
            SELECT
                (SELECT COUNT(*) FROM work_items WHERE state = 'ready')::TEXT
                    AS queued_work_items,
                (
                    SELECT COUNT(*)
                    FROM project_batch_requests
                    WHERE state = 'pending'
                )::TEXT AS pending_batch_requests
            "#,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?
        .into();
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderAccountConcurrencySnapshot {
            as_of_ms,
            accounts,
            queue,
        })
    }

    async fn scheduler(&self, window_ms: i64) -> Result<SchedulerSnapshot, AdminReadError> {
        let (mut tx, window) = self
            .begin_snapshot(window_ms, MAX_SCHEDULER_WINDOW_MS)
            .await?;
        let work_items = sqlx::query_as::<_, WorkStateRow>(
            r#"
            SELECT state,
                   CASE
                       WHEN state = 'ready' AND available_at_ms <= $1 THEN 'due'
                       WHEN state = 'ready' THEN 'delayed'
                       ELSE NULL
                   END AS ready_timing,
                   COUNT(*)::TEXT AS count
            FROM work_items
            WHERE state IN ('ready', 'leased', 'running', 'awaiting_executor')
              AND (
                  state <> 'awaiting_executor'
                  OR EXISTS (
                      SELECT 1
                      FROM provider_submissions submission
                      LEFT JOIN executor_terminal_reductions reduction
                        ON reduction.submission_id = submission.submission_id
                       AND reduction.executor_execution_id =
                           submission.executor_execution_id
                      WHERE submission.work_item_id = work_items.work_item_id
                        AND (
                            reduction.submission_id IS NULL
                            OR reduction.state IN ('ready', 'leased')
                        )
                  )
              )
            GROUP BY state, ready_timing
            ORDER BY state, ready_timing
            "#,
        )
        .bind(window.as_of_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let expired_leases = count_scalar(
            &mut tx,
            r#"
            SELECT COUNT(*)::TEXT
            FROM work_items
            WHERE state IN ('leased', 'running')
              AND lease_expires_at_ms <= $1
            "#,
            window.as_of_ms,
        )
        .await?;
        let provider_polls_due = count_scalar(
            &mut tx,
            "SELECT COUNT(*)::TEXT FROM provider_remote_tasks WHERE state = 'provider_waiting' AND next_poll_at_ms IS NOT NULL AND next_poll_at_ms <= $1",
            window.as_of_ms,
        )
        .await?;
        let pending_terminal_reductions = count_scalar(
            &mut tx,
            "SELECT COUNT(*)::TEXT FROM executor_terminal_reductions WHERE state IN ('ready', 'leased') AND created_at_ms <= $1",
            window.as_of_ms,
        )
        .await?;
        let blocked_terminal_reductions = count_scalar(
            &mut tx,
            "SELECT COUNT(*)::TEXT FROM executor_terminal_reductions WHERE state = 'blocked' AND blocked_at_ms <= $1",
            window.as_of_ms,
        )
        .await?;
        let blocked_terminals = sqlx::query_as::<_, BlockedTerminalReductionRow>(
            r#"
            SELECT reduction.submission_id,
                   reduction.executor_execution_id,
                   submission.job_id,
                   job.request_id,
                   submission.provider_id,
                   submission.model,
                   reduction.resolved_state,
                   reduction.blocked_error_code AS error_code,
                   reduction.blocked_at_ms,
                   reduction.blocked_by
            FROM executor_terminal_reductions reduction
            JOIN provider_submissions submission
              ON submission.submission_id = reduction.submission_id
             AND submission.executor_execution_id = reduction.executor_execution_id
            JOIN jobs job
              ON job.job_id = submission.job_id
            WHERE reduction.state = 'blocked'
              AND reduction.blocked_at_ms <= $1
            ORDER BY reduction.blocked_at_ms DESC, reduction.submission_id
            LIMIT 50
            "#,
        )
        .bind(window.as_of_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let active_jobs = sqlx::query_as::<_, SchedulerActiveJobRow>(
            r#"
            SELECT job.job_id,
                   job.request_id,
                   attribution.tenant_id AS organization_id,
                   organization.display_name AS organization_name,
                   attribution.project_id,
                   project.name AS project_name,
                   actor.display_name AS user_display_name,
                   actor.normalized_email AS user_email,
                   service_account.name AS service_account_name,
                   api_key.name AS api_key_name,
                   job.operation,
                   job.provider_id,
                   job.model,
                   job.state AS job_state,
                   work.state AS work_state,
                   profile.provider_account_id,
                   COALESCE(environment.display_name, account.account_key)
                       AS provider_account_name,
                   COALESCE((
                       SELECT COUNT(*)::TEXT
                       FROM job_attempts attempt
                       WHERE attempt.work_item_id = work.work_item_id
                   ), '0') AS attempt_count,
                   work.available_at_ms,
                   work.lease_expires_at_ms,
                   job.created_at_ms,
                   job.started_at_ms
            FROM work_items work
            JOIN jobs job ON job.job_id = work.job_id
            LEFT JOIN job_auth_attributions attribution
              ON attribution.job_id = job.job_id
            LEFT JOIN identity_organizations organization
              ON organization.organization_id = attribution.tenant_id
            LEFT JOIN gateway_projects project
              ON project.id = attribution.project_id
             AND project.tenant_id = attribution.tenant_id
            LEFT JOIN gateway_service_accounts service_account
              ON service_account.id = attribution.service_account_id
             AND service_account.project_id = attribution.project_id
             AND service_account.tenant_id = attribution.tenant_id
            LEFT JOIN gateway_api_keys api_key
              ON api_key.id = attribution.api_key_id
             AND api_key.service_account_id = attribution.service_account_id
             AND api_key.project_id = attribution.project_id
             AND api_key.tenant_id = attribution.tenant_id
            LEFT JOIN identity_users actor
              ON actor.user_id = COALESCE(
                  attribution.actor_user_id,
                  attribution.credential_owner_user_id
              )
            LEFT JOIN provider_execution_profiles profile
              ON profile.execution_profile_id = work.execution_profile_id
            LEFT JOIN provider_accounts account
              ON account.provider_account_id = profile.provider_account_id
            LEFT JOIN provider_account_environments environment
              ON environment.provider_account_id = profile.provider_account_id
             AND environment.provider_id = profile.provider_id
            WHERE work.state IN ('ready', 'leased', 'running', 'awaiting_executor')
              AND (
                  work.state <> 'awaiting_executor'
                  OR EXISTS (
                      SELECT 1
                      FROM provider_submissions submission
                      LEFT JOIN executor_terminal_reductions reduction
                        ON reduction.submission_id = submission.submission_id
                       AND reduction.executor_execution_id =
                           submission.executor_execution_id
                      WHERE submission.work_item_id = work.work_item_id
                        AND (
                            reduction.submission_id IS NULL
                            OR reduction.state IN ('ready', 'leased')
                        )
                  )
              )
              AND job.state IN (
                  'accepted', 'reserved', 'queued', 'leased', 'running',
                  'provider_waiting', 'artifact_ready'
              )
            ORDER BY CASE
                         WHEN work.state IN ('leased', 'running', 'awaiting_executor') THEN 0
                         ELSE 1
                     END,
                     work.created_at_ms DESC,
                     job.job_id DESC
            LIMIT 100
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let capacity_reconciliations_due = count_scalar(
            &mut tx,
            "SELECT COUNT(*)::TEXT FROM provider_capacity_reconciliations WHERE state = 'active' AND available_at_ms <= $1",
            window.as_of_ms,
        )
        .await?;
        let (artifact_retention_due, artifact_retention_deleting, artifact_retention_failures): (
            String,
            String,
            String,
        ) = sqlx::query_as(
            r#"
            SELECT
              (
                (SELECT COUNT(*) FROM job_artifact_retention
                 WHERE state = 'available' AND expires_at_ms <= $1)
                +
                (SELECT COUNT(*) FROM job_artifact_retention
                 WHERE state = 'expired' AND purge_after_ms <= $1)
                +
                (SELECT COUNT(*) FROM job_artifact_retention
                 WHERE state = 'deleting' AND lease_expires_at_ms <= $1)
              )::TEXT,
              (SELECT COUNT(*)::TEXT FROM job_artifact_retention
               WHERE state = 'deleting' AND lease_expires_at_ms > $1),
              (SELECT COUNT(*)::TEXT FROM job_artifact_retention
               WHERE last_error_code IS NOT NULL)
            "#,
        )
        .bind(window.as_of_ms)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        let recent_uncertain = sqlx::query_as::<_, StageCountRow>(
            r#"
            SELECT stage, COUNT(*)::TEXT AS count
            FROM (
                SELECT 'job'::TEXT AS stage
                FROM jobs
                WHERE state = 'uncertain'
                  AND updated_at_ms >= $1 AND updated_at_ms < $2
                UNION ALL
                SELECT 'work_item'::TEXT AS stage
                FROM work_items
                WHERE state = 'uncertain'
                  AND updated_at_ms >= $1 AND updated_at_ms < $2
                UNION ALL
                SELECT 'provider_task'::TEXT AS stage
                FROM provider_remote_tasks
                WHERE state = 'uncertain'
                  AND terminal_at_ms >= $1 AND terminal_at_ms < $2
            ) uncertain
            GROUP BY stage
            ORDER BY stage
            "#,
        )
        .bind(window.from_ms)
        .bind(window.to_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        let capacity = sqlx::query_as::<_, SchedulerCapacityRow>(
            r#"
            SELECT account.provider_account_id, account.account_key,
                   policy.provider_id, environment.display_name,
                   environment.account_email,
                   control.desired_max_concurrency::TEXT AS max_concurrency,
                   policy.allocated_count::TEXT AS allocated_count,
                   GREATEST(control.desired_max_concurrency::NUMERIC
                     - policy.allocated_count::NUMERIC, 0)::TEXT
                       AS available_capacity
            FROM executor_resource_policies policy
            JOIN provider_accounts account
              ON account.provider_account_id = policy.provider_account_id
             AND account.credential_pool_id = policy.credential_pool_id
             AND account.provider_id = policy.provider_id
            JOIN provider_account_execution_controls control
              ON control.provider_account_id = policy.provider_account_id
            LEFT JOIN provider_account_environments environment
              ON environment.provider_account_id = account.provider_account_id
             AND environment.provider_id = account.provider_id
            WHERE policy.state = 'enabled'
            ORDER BY policy.provider_id, account.account_key
            "#,
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?
        .into_iter()
        .map(Into::into)
        .collect();
        tx.commit().await.map_err(unavailable)?;
        Ok(SchedulerSnapshot {
            as_of_ms: window.as_of_ms,
            from_ms: window.from_ms,
            to_ms: window.to_ms,
            work_items,
            expired_leases,
            provider_polls_due,
            pending_terminal_reductions,
            blocked_terminal_reductions,
            blocked_terminals,
            active_jobs,
            capacity_reconciliations_due,
            artifact_retention_due,
            artifact_retention_deleting,
            artifact_retention_failures,
            recent_uncertain,
            capacity,
        })
    }

    async fn jobs(&self, query: JobsQuery) -> Result<JobsSnapshot, AdminReadError> {
        self.jobs_scoped(&AdminReadScope::Platform, query).await
    }

    async fn jobs_scoped(
        &self,
        scope: &AdminReadScope,
        query: JobsQuery,
    ) -> Result<JobsSnapshot, AdminReadError> {
        validate_jobs_query(&query)?;
        let actor_user_id = scope.actor_user_id_for_project(query.project_id.as_deref())?;
        let (mut tx, window) = self
            .begin_anchored_snapshot(query.window_ms, MAX_JOBS_WINDOW_MS, query.to_ms)
            .await?;
        if query
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.created_at_ms >= window.to_ms)
        {
            return Err(invalid("cursor must precede the database snapshot"));
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT job.job_id, job.tenant_id,
                   attribution.project_id, attribution.service_account_id,
                   attribution.api_key_id, attribution.auth_kind,
                   attribution.actor_user_id,
                   attribution.credential_owner_user_id,
                   job.request_id, job.operation,
                   job.provider_id, job.model, job.state AS job_state,
                   work.state AS work_state,
                   COALESCE((
                       SELECT jsonb_agg(
                           jsonb_build_object(
                               'stage', grouped.stage,
                               'state', grouped.state,
                               'count', grouped.count
                           )
                           ORDER BY grouped.stage, grouped.state
                       )
                       FROM (
                           SELECT 'submission'::TEXT AS stage, submission.state,
                                  COUNT(*)::TEXT AS count
                           FROM provider_submissions submission
                           WHERE submission.job_id = job.job_id
                           GROUP BY submission.state
                           UNION ALL
                           SELECT 'remote_task'::TEXT AS stage, task.state,
                                  COUNT(*)::TEXT AS count
                           FROM provider_submissions submission
                           JOIN provider_remote_tasks task
                             ON task.submission_id = submission.submission_id
                            AND task.executor_execution_id = submission.executor_execution_id
                           WHERE submission.job_id = job.job_id
                           GROUP BY task.state
                       ) grouped
                   ), '[]'::JSONB) AS provider_states,
                   job.output_count::TEXT AS output_count,
                   job.billable_units::TEXT AS billable_units,
                   job.billing_metric, job.billing_unit,
                   job.charged_units::TEXT AS charged_units,
                   job.created_at_ms, job.started_at_ms, job.finished_at_ms,
                   job.last_error_code
            FROM jobs job
            LEFT JOIN work_items work ON work.job_id = job.job_id
            LEFT JOIN job_auth_attributions attribution
              ON attribution.job_id = job.job_id
            WHERE job.created_at_ms >= "#,
        );
        builder.push_bind(window.from_ms);
        builder.push(" AND job.created_at_ms < ");
        builder.push_bind(window.to_ms);
        if let Some(tenant_ids) = scope.tenant_ids() {
            builder.push(" AND job.tenant_id = ANY(");
            builder.push_bind(tenant_ids);
            builder.push(")");
        }
        if let Some(actor_user_id) = actor_user_id {
            builder.push(" AND (attribution.actor_user_id = ");
            builder.push_bind(actor_user_id);
            builder.push(" OR attribution.credential_owner_user_id = ");
            builder.push_bind(actor_user_id);
            builder.push(")");
        }
        if let Some(provider_id) = normalized_filter(query.provider_id.as_deref()) {
            builder.push(" AND job.provider_id = ");
            builder.push_bind(provider_id);
        }
        if let Some(state) = normalized_filter(query.state.as_deref()) {
            builder.push(" AND job.state = ");
            builder.push_bind(state);
        }
        if let Some(operation) = normalized_filter(query.operation.as_deref()) {
            builder.push(" AND job.operation = ");
            builder.push_bind(operation);
        }
        if let Some(model) = normalized_filter(query.model.as_deref()) {
            builder.push(" AND job.model = ");
            builder.push_bind(model);
        }
        if let Some(project_id) = normalized_filter(query.project_id.as_deref()) {
            builder.push(" AND attribution.project_id = ");
            builder.push_bind(project_id);
        }
        if let Some(api_key_id) = normalized_filter(query.api_key_id.as_deref()) {
            builder.push(" AND attribution.api_key_id = ");
            builder.push_bind(api_key_id);
        }
        if let Some(request_or_job_id) = normalized_filter(query.request_or_job_id.as_deref()) {
            if let Ok(job_id) = Uuid::parse_str(request_or_job_id) {
                builder.push(" AND (job.request_id = ");
                builder.push_bind(request_or_job_id);
                builder.push(" OR job.job_id = ");
                builder.push_bind(job_id);
                builder.push(")");
            } else {
                builder.push(" AND job.request_id = ");
                builder.push_bind(request_or_job_id);
            }
        }
        if let Some(cursor) = &query.cursor {
            builder.push(" AND (job.created_at_ms, job.job_id) < (");
            builder.push_bind(cursor.created_at_ms);
            builder.push(", ");
            builder.push_bind(cursor.job_id);
            builder.push(")");
        }
        builder.push(" ORDER BY job.created_at_ms DESC, job.job_id DESC LIMIT ");
        builder.push_bind(i64::from(query.limit) + 1);

        let mut rows = builder
            .build_query_as::<JobRow>()
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        let has_more = rows.len() > query.limit as usize;
        if has_more {
            rows.truncate(query.limit as usize);
        }
        let next_cursor = has_more.then(|| {
            let last = rows.last().expect("nonzero page limit leaves a cursor row");
            JobCursor {
                created_at_ms: last.created_at_ms,
                job_id: last.job_id,
            }
        });
        let items = rows
            .into_iter()
            .map(JobRow::into_item)
            .collect::<Result<Vec<_>, _>>()?;
        tx.commit().await.map_err(unavailable)?;
        Ok(JobsSnapshot {
            as_of_ms: window.as_of_ms,
            from_ms: window.from_ms,
            to_ms: window.to_ms,
            items,
            next_cursor,
        })
    }

    async fn request_logs(
        &self,
        query: RequestLogsQuery,
    ) -> Result<RequestLogsSnapshot, AdminReadError> {
        self.request_logs_scoped(&AdminReadScope::Platform, query)
            .await
    }

    async fn request_logs_scoped(
        &self,
        scope: &AdminReadScope,
        query: RequestLogsQuery,
    ) -> Result<RequestLogsSnapshot, AdminReadError> {
        validate_request_logs_query(&query)?;
        let actor_user_id = match query.visibility {
            RequestLogVisibility::Mine => {
                scope.actor_user_id_for_project(query.project_id.as_deref())?
            }
            RequestLogVisibility::Project => {
                if let Some(project_id) = query.project_id.as_deref() {
                    scope.ensure_project_access(project_id)?;
                } else if matches!(scope, AdminReadScope::User { .. }) {
                    return Err(invalid("project visibility requires project_id"));
                }
                None
            }
        };
        let (mut tx, window) = self
            .begin_anchored_snapshot(query.window_ms, MAX_REQUEST_LOG_WINDOW_MS, query.to_ms)
            .await?;
        if query
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.created_at_ms >= window.to_ms)
        {
            return Err(invalid("cursor must precede the database snapshot"));
        }

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT observation.request_id, observation.source,
                   observation.method, observation.route_pattern,
                   observation.request_path, observation.status_code,
                   observation.duration_ms, observation.error_code,
                   observation.idempotency_key_digest,
                   COALESCE(observation.tenant_id, job.tenant_id) AS tenant_id,
                   COALESCE(observation.project_id, attribution.project_id) AS project_id,
                   COALESCE(
                     observation.service_account_id,
                     attribution.service_account_id
                   ) AS service_account_id,
                   COALESCE(observation.api_key_id, attribution.api_key_id) AS api_key_id,
                   COALESCE(
                     observation.actor_user_id,
                     attribution.actor_user_id
                   ) AS actor_user_id,
                   COALESCE(
                     observation.credential_owner_user_id,
                     attribution.credential_owner_user_id
                   ) AS credential_owner_user_id,
                   COALESCE(observation.auth_kind, attribution.auth_kind) AS auth_kind,
                   observation.content_captured,
                   COALESCE(observation.job_id, job.job_id) AS job_id,
                   job.operation, job.provider_id, job.model,
                   job.state AS job_state, work.state AS work_state,
                   job.output_count::TEXT AS output_count,
                   job.billable_units::TEXT AS billable_units,
                   job.billing_unit,
                   tier.requested_service_tier,
                   tier.project_service_tier,
                   tier.effective_service_tier,
                   tier.fallback_reason AS service_tier_fallback_reason,
                   observation.created_at_ms, observation.completed_at_ms
            FROM gateway_request_observations observation
            LEFT JOIN jobs job ON job.request_id = observation.request_id
            LEFT JOIN work_items work ON work.job_id = job.job_id
            LEFT JOIN job_auth_attributions attribution
              ON attribution.job_id = job.job_id
            LEFT JOIN job_service_tier_decisions tier
              ON tier.job_id = job.job_id
            WHERE observation.created_at_ms >= "#,
        );
        builder.push_bind(window.from_ms);
        builder.push(" AND observation.created_at_ms < ");
        builder.push_bind(window.to_ms);
        if let Some(tenant_ids) = scope.tenant_ids() {
            builder.push(" AND COALESCE(observation.tenant_id, job.tenant_id) = ANY(");
            builder.push_bind(tenant_ids);
            builder.push(")");
        }
        if let Some(actor_user_id) = actor_user_id {
            builder.push(
                " AND COALESCE(observation.actor_user_id, attribution.actor_user_id, observation.credential_owner_user_id, attribution.credential_owner_user_id) = ",
            );
            builder.push_bind(actor_user_id);
        }
        if let Some(project_id) = normalized_filter(query.project_id.as_deref()) {
            builder.push(" AND COALESCE(observation.project_id, attribution.project_id) = ");
            builder.push_bind(project_id);
        }
        if let Some(source) = normalized_filter(query.source.as_deref()) {
            builder.push(" AND observation.source = ");
            builder.push_bind(source);
        }
        if let Some(status) = normalized_filter(query.status.as_deref()) {
            match status {
                "succeeded" => {
                    builder.push(
                        " AND observation.status_code < 400 AND COALESCE(job.state, 'succeeded') NOT IN ('failed', 'uncertain')",
                    );
                }
                "failed" => {
                    builder.push(
                        " AND (observation.status_code >= 400 OR job.state IN ('failed', 'uncertain'))",
                    );
                }
                "in_progress" => {
                    builder.push(" AND job.state IN ('queued', 'running')");
                }
                _ => return Err(invalid("status must be succeeded, failed, or in_progress")),
            };
        }
        if let Some(provider_id) = normalized_filter(query.provider_id.as_deref()) {
            builder.push(" AND job.provider_id = ");
            builder.push_bind(provider_id);
        }
        if let Some(model) = normalized_filter(query.model.as_deref()) {
            builder.push(" AND job.model = ");
            builder.push_bind(model);
        }
        if let Some(api_key_id) = normalized_filter(query.api_key_id.as_deref()) {
            builder.push(" AND COALESCE(observation.api_key_id, attribution.api_key_id) = ");
            builder.push_bind(api_key_id);
        }
        if let Some(request_or_job_id) = normalized_filter(query.request_or_job_id.as_deref()) {
            if let Ok(job_id) = Uuid::parse_str(request_or_job_id) {
                builder.push(" AND (observation.request_id = ");
                builder.push_bind(request_or_job_id);
                builder.push(" OR COALESCE(observation.job_id, job.job_id) = ");
                builder.push_bind(job_id);
                builder.push(")");
            } else {
                builder.push(" AND observation.request_id = ");
                builder.push_bind(request_or_job_id);
            }
        }
        if let Some(cursor) = &query.cursor {
            builder.push(" AND (observation.created_at_ms, observation.request_id) < (");
            builder.push_bind(cursor.created_at_ms);
            builder.push(", ");
            builder.push_bind(&cursor.request_id);
            builder.push(")");
        }
        builder
            .push(" ORDER BY observation.created_at_ms DESC, observation.request_id DESC LIMIT ");
        builder.push_bind(i64::from(query.limit) + 1);

        let mut rows = builder
            .build_query_as::<RequestLogRow>()
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        let has_more = rows.len() > query.limit as usize;
        if has_more {
            rows.truncate(query.limit as usize);
        }
        let next_cursor = has_more.then(|| {
            let last = rows.last().expect("nonzero page limit leaves a cursor row");
            RequestLogCursor {
                created_at_ms: last.created_at_ms,
                request_id: last.request_id.clone(),
            }
        });
        let items = rows
            .into_iter()
            .map(RequestLogRow::into_item)
            .collect::<Result<Vec<_>, _>>()?;
        tx.commit().await.map_err(unavailable)?;
        Ok(RequestLogsSnapshot {
            as_of_ms: window.as_of_ms,
            from_ms: window.from_ms,
            to_ms: window.to_ms,
            items,
            next_cursor,
        })
    }

    async fn audit_logs(&self, query: AuditLogsQuery) -> Result<AuditLogsSnapshot, AdminReadError> {
        validate_audit_logs_query(&query)?;
        let (mut tx, window) = self
            .begin_anchored_snapshot(query.window_ms, MAX_AUDIT_LOG_WINDOW_MS, query.to_ms)
            .await?;
        let after = if let Some(event_id) = query.after {
            Some(
                sqlx::query_as::<_, (i64, Uuid)>(
                    r#"
                    SELECT created_at_ms, event_id
                    FROM identity_audit_events
                    WHERE event_id = $1
                    "#,
                )
                .bind(event_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?
                .ok_or(AdminReadError::NotFound)?,
            )
        } else {
            None
        };

        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT audit.event_id, audit.actor_user_id,
                   actor.normalized_email AS actor_email,
                   actor.display_name AS actor_display_name,
                   audit.session_id, audit.request_id, audit.action,
                   audit.resource_type, audit.resource_id, audit.outcome,
                   audit.reason_code, audit.metadata, audit.created_at_ms,
                   COALESCE(
                     CASE
                       WHEN audit.resource_type = 'project' THEN audit.resource_id
                       ELSE NULL
                     END,
                     audit.metadata ->> 'project_id'
                   ) AS project_id,
                   project.name AS project_name,
                   project.tenant_id AS organization_id
            FROM identity_audit_events audit
            LEFT JOIN identity_users actor
              ON actor.user_id = audit.actor_user_id
            LEFT JOIN gateway_projects project
              ON project.id = COALESCE(
                   CASE
                     WHEN audit.resource_type = 'project' THEN audit.resource_id
                     ELSE NULL
                   END,
                   audit.metadata ->> 'project_id'
                 )
            WHERE audit.created_at_ms >= "#,
        );
        builder.push_bind(window.from_ms);
        builder.push(" AND audit.created_at_ms < ");
        builder.push_bind(window.to_ms);
        if let Some(event_type) = query.event_type.as_deref() {
            builder.push(" AND audit.action = ");
            builder.push_bind(event_type);
        }
        if let Some(outcome) = query.outcome.as_deref() {
            builder.push(" AND audit.outcome = ");
            builder.push_bind(outcome);
        }
        if let Some(actor_user_id) = query.actor_user_id {
            builder.push(" AND audit.actor_user_id = ");
            builder.push_bind(actor_user_id);
        }
        if let Some(project_id) = query.project_id.as_deref() {
            builder.push(
                r#"
                AND COALESCE(
                  CASE
                    WHEN audit.resource_type = 'project' THEN audit.resource_id
                    ELSE NULL
                  END,
                  audit.metadata ->> 'project_id'
                ) = "#,
            );
            builder.push_bind(project_id);
        }
        if let Some(resource_type) = query.resource_type.as_deref() {
            builder.push(" AND audit.resource_type = ");
            builder.push_bind(resource_type);
        }
        if let Some(request_id) = query.request_id.as_deref() {
            builder.push(" AND audit.request_id = ");
            builder.push_bind(request_id);
        }
        if let Some(search) = query.query.as_deref() {
            let pattern = format!("%{search}%");
            builder.push(
                r#"
                AND (
                  audit.action ILIKE "#,
            );
            builder.push_bind(pattern.clone());
            builder.push(" OR audit.resource_id ILIKE ");
            builder.push_bind(pattern.clone());
            builder.push(" OR audit.request_id ILIKE ");
            builder.push_bind(pattern.clone());
            builder.push(" OR actor.normalized_email ILIKE ");
            builder.push_bind(pattern.clone());
            builder.push(" OR actor.display_name ILIKE ");
            builder.push_bind(pattern);
            builder.push(")");
        }
        if let Some((created_at_ms, event_id)) = after {
            builder.push(" AND (audit.created_at_ms, audit.event_id) < (");
            builder.push_bind(created_at_ms);
            builder.push(", ");
            builder.push_bind(event_id);
            builder.push(")");
        }
        builder.push(" ORDER BY audit.created_at_ms DESC, audit.event_id DESC LIMIT ");
        builder.push_bind(i64::from(query.limit) + 1);

        let mut rows = builder
            .build_query_as::<AuditLogRow>()
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        let has_more = rows.len() > query.limit as usize;
        if has_more {
            rows.truncate(query.limit as usize);
        }
        let data = rows
            .into_iter()
            .map(AuditLogRow::into_item)
            .collect::<Vec<_>>();
        let first_id = data.first().map(|item| item.id.clone());
        let last_id = data.last().map(|item| item.id.clone());
        let next_after = has_more.then(|| {
            last_id
                .clone()
                .expect("a non-empty limited audit page has a last id")
        });
        tx.commit().await.map_err(unavailable)?;
        Ok(AuditLogsSnapshot {
            object: "list".to_string(),
            as_of_ms: window.as_of_ms,
            from_ms: window.from_ms,
            to_ms: window.to_ms,
            data,
            first_id,
            last_id,
            has_more,
            next_after,
        })
    }

    async fn job_economics(&self, job_id: Uuid) -> Result<JobEconomicsSnapshot, AdminReadError> {
        self.job_economics_scoped(&AdminReadScope::Platform, job_id, None)
            .await
    }

    async fn job_economics_scoped(
        &self,
        scope: &AdminReadScope,
        job_id: Uuid,
        project_id: Option<String>,
    ) -> Result<JobEconomicsSnapshot, AdminReadError> {
        self.read_job_economics(scope, job_id, project_id.as_deref())
            .await
    }
}

#[derive(Clone, Copy)]
pub(super) struct ReadWindow {
    pub(super) as_of_ms: i64,
    pub(super) from_ms: i64,
    pub(super) to_ms: i64,
}

#[derive(FromRow)]
struct StateCountRow {
    state: String,
    count: String,
}

impl From<StateCountRow> for StateCount {
    fn from(row: StateCountRow) -> Self {
        Self {
            state: row.state,
            count: row.count,
        }
    }
}

#[derive(FromRow)]
struct UsageRow {
    tenant_id: Option<String>,
    billing_metric: String,
    billing_unit: String,
    outcome: String,
    quantity: String,
}

impl From<UsageRow> for UsageAggregate {
    fn from(row: UsageRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            billing_metric: row.billing_metric,
            billing_unit: row.billing_unit,
            outcome: row.outcome,
            quantity: row.quantity,
        }
    }
}

#[derive(FromRow)]
struct LedgerRow {
    tenant_id: Option<String>,
    transaction_type: String,
    currency: String,
    amount_micros: String,
    transaction_count: String,
}

impl From<LedgerRow> for LedgerAggregate {
    fn from(row: LedgerRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            transaction_type: row.transaction_type,
            currency: row.currency,
            amount_micros: row.amount_micros,
            transaction_count: row.transaction_count,
        }
    }
}

#[derive(FromRow)]
struct ElapsedRow {
    samples: String,
    p95_ms: Option<i64>,
}

#[derive(FromRow)]
struct BillingAccountRow {
    tenant_id: String,
    currency: String,
    credit_limit_micros: String,
    held_micros: String,
    captured_micros: String,
    refunded_micros: String,
    available_micros: String,
    control_version: String,
    updated_at_ms: i64,
}

impl From<BillingAccountRow> for BillingAccountSnapshot {
    fn from(row: BillingAccountRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            currency: row.currency,
            credit_limit_micros: row.credit_limit_micros,
            held_micros: row.held_micros,
            captured_micros: row.captured_micros,
            refunded_micros: row.refunded_micros,
            available_micros: row.available_micros,
            control_version: row.control_version,
            updated_at_ms: row.updated_at_ms,
        }
    }
}

#[derive(FromRow)]
struct RatedUsageRow {
    tenant_id: String,
    billing_metric: String,
    billing_unit: String,
    outcome: String,
    currency: String,
    quantity: String,
    amount_micros: String,
}

impl From<RatedUsageRow> for RatedUsageAggregate {
    fn from(row: RatedUsageRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            billing_metric: row.billing_metric,
            billing_unit: row.billing_unit,
            outcome: row.outcome,
            currency: row.currency,
            quantity: row.quantity,
            amount_micros: row.amount_micros,
        }
    }
}

#[derive(FromRow)]
struct ProviderCostRow {
    tenant_id: Option<String>,
    provider_id: String,
    outcome: String,
    cost_basis: String,
    attribution_state: String,
    currency: String,
    amount_micros: String,
    transaction_count: String,
    linked_receipts: String,
}

impl From<ProviderCostRow> for ProviderCostAggregate {
    fn from(row: ProviderCostRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            provider_id: row.provider_id,
            outcome: row.outcome,
            cost_basis: row.cost_basis,
            attribution_state: row.attribution_state,
            currency: row.currency,
            amount_micros: row.amount_micros,
            transaction_count: row.transaction_count,
            linked_receipts: row.linked_receipts,
        }
    }
}

#[derive(FromRow)]
struct ProviderCoverageRow {
    terminal_receipts: String,
    covered_receipts: String,
    uncovered_receipts: String,
    provider_actual_transactions: String,
    provider_allocated_transactions: String,
    legacy_unverified_transactions: String,
    unattributed_transactions: String,
    authority_conflicts: String,
}

impl From<ProviderCoverageRow> for ProviderCostCoverage {
    fn from(row: ProviderCoverageRow) -> Self {
        Self {
            terminal_receipts: row.terminal_receipts,
            covered_receipts: row.covered_receipts,
            uncovered_receipts: row.uncovered_receipts,
            provider_actual_transactions: row.provider_actual_transactions,
            provider_allocated_transactions: row.provider_allocated_transactions,
            legacy_unverified_transactions: row.legacy_unverified_transactions,
            unattributed_transactions: row.unattributed_transactions,
            authority_conflicts: row.authority_conflicts,
        }
    }
}

#[derive(FromRow)]
struct ProviderAccountRow {
    provider_account_id: Uuid,
    account_key: String,
    provider_id: String,
    display_name: Option<String>,
    account_email: Option<String>,
    environment_state: Option<String>,
    quota_status: Option<String>,
    plan_type: Option<String>,
    credits_balance: Option<String>,
    credits_unlimited: Option<bool>,
    quota_observed_at_ms: Option<i64>,
    quota_windows: serde_json::Value,
    account_state: String,
    credential_pool_state: String,
    credential_lifecycle_state: String,
    credential_refresh_strategy: String,
    operational_credential_revision: i64,
    credential_access_expires_at_ms: Option<i64>,
    credential_next_refresh_at_ms: Option<i64>,
    credential_last_success_at_ms: Option<i64>,
    credential_consecutive_failures: i32,
    credential_last_error_code: Option<String>,
    execution_profile_id: Uuid,
    profile_key: String,
    operation_id: String,
    completion_mode: String,
    profile_state: String,
    resource_policy_state: String,
    scheduling_state: String,
    control_version: i64,
    configuration_status: String,
    runtime_status: String,
    max_concurrency: String,
    allocated_count: String,
    available_capacity: String,
    active_submitters: String,
    active_pollers: String,
    draining_submitters: String,
    draining_pollers: String,
}

impl From<ProviderAccountRow> for ProviderAccountView {
    fn from(row: ProviderAccountRow) -> Self {
        let upstream_quota = row
            .quota_status
            .as_ref()
            .map_or_else(unknown_upstream_quota, |_| UpstreamQuotaObservation {
                status: row
                    .quota_status
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                limit: None,
                remaining: None,
                reset_at_ms: None,
                observed_at_ms: row.quota_observed_at_ms,
                plan_type: row.plan_type.clone(),
                credits_balance: row.credits_balance.clone(),
                credits_unlimited: row.credits_unlimited,
                windows: serde_json::from_value::<Vec<UpstreamQuotaWindow>>(
                    row.quota_windows.clone(),
                )
                .unwrap_or_default(),
            });
        Self {
            provider_account_id: row.provider_account_id.to_string(),
            account_key: row.account_key,
            provider_id: row.provider_id,
            display_name: row.display_name,
            account_email: row.account_email,
            environment_state: row.environment_state,
            account_state: row.account_state,
            credential_pool_state: row.credential_pool_state,
            credential_lifecycle_state: row.credential_lifecycle_state,
            credential_refresh_strategy: row.credential_refresh_strategy,
            operational_credential_revision: row.operational_credential_revision,
            credential_access_expires_at_ms: row.credential_access_expires_at_ms,
            credential_next_refresh_at_ms: row.credential_next_refresh_at_ms,
            credential_last_success_at_ms: row.credential_last_success_at_ms,
            credential_consecutive_failures: row.credential_consecutive_failures,
            credential_last_error_code: row.credential_last_error_code,
            execution_profile_id: row.execution_profile_id.to_string(),
            profile_key: row.profile_key,
            operation_id: row.operation_id,
            completion_mode: row.completion_mode,
            profile_state: row.profile_state,
            resource_policy_state: row.resource_policy_state,
            scheduling_state: row.scheduling_state,
            control_version: row.control_version,
            configuration_status: row.configuration_status,
            runtime_status: row.runtime_status,
            max_concurrency: row.max_concurrency,
            allocated_count: row.allocated_count,
            available_capacity: row.available_capacity,
            active_submitters: row.active_submitters,
            active_pollers: row.active_pollers,
            draining_submitters: row.draining_submitters,
            draining_pollers: row.draining_pollers,
            upstream_quota,
        }
    }
}

#[derive(FromRow)]
struct ProviderAccountConcurrencyRow {
    provider_account_id: Uuid,
    max_concurrency: String,
    allocated_count: String,
    available_capacity: String,
}

impl From<ProviderAccountConcurrencyRow> for ProviderAccountConcurrency {
    fn from(row: ProviderAccountConcurrencyRow) -> Self {
        Self {
            provider_account_id: row.provider_account_id.to_string(),
            max_concurrency: row.max_concurrency,
            allocated_count: row.allocated_count,
            available_capacity: row.available_capacity,
        }
    }
}

#[derive(FromRow)]
struct ProviderQueuePressureRow {
    queued_work_items: String,
    pending_batch_requests: String,
}

impl From<ProviderQueuePressureRow> for ProviderQueuePressure {
    fn from(row: ProviderQueuePressureRow) -> Self {
        Self {
            queued_work_items: row.queued_work_items,
            pending_batch_requests: row.pending_batch_requests,
        }
    }
}

#[derive(FromRow)]
struct WorkStateRow {
    state: String,
    ready_timing: Option<String>,
    count: String,
}

impl From<WorkStateRow> for WorkStateCount {
    fn from(row: WorkStateRow) -> Self {
        Self {
            state: row.state,
            ready_timing: row.ready_timing,
            count: row.count,
        }
    }
}

#[derive(FromRow)]
struct StageCountRow {
    stage: String,
    count: String,
}

impl From<StageCountRow> for StageCount {
    fn from(row: StageCountRow) -> Self {
        Self {
            stage: row.stage,
            count: row.count,
        }
    }
}

#[derive(FromRow)]
struct SchedulerCapacityRow {
    provider_account_id: Uuid,
    account_key: String,
    provider_id: String,
    display_name: Option<String>,
    account_email: Option<String>,
    max_concurrency: String,
    allocated_count: String,
    available_capacity: String,
}

#[derive(FromRow)]
struct BlockedTerminalReductionRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    job_id: Uuid,
    request_id: String,
    provider_id: String,
    model: String,
    resolved_state: String,
    error_code: String,
    blocked_at_ms: i64,
    blocked_by: String,
}

#[derive(FromRow)]
struct SchedulerActiveJobRow {
    job_id: Uuid,
    request_id: String,
    organization_id: Option<String>,
    organization_name: Option<String>,
    project_id: Option<String>,
    project_name: Option<String>,
    user_display_name: Option<String>,
    user_email: Option<String>,
    service_account_name: Option<String>,
    api_key_name: Option<String>,
    operation: String,
    provider_id: String,
    model: String,
    job_state: String,
    work_state: Option<String>,
    provider_account_id: Option<Uuid>,
    provider_account_name: Option<String>,
    attempt_count: String,
    available_at_ms: Option<i64>,
    lease_expires_at_ms: Option<i64>,
    created_at_ms: i64,
    started_at_ms: Option<i64>,
}

impl From<BlockedTerminalReductionRow> for BlockedTerminalReduction {
    fn from(row: BlockedTerminalReductionRow) -> Self {
        Self {
            submission_id: row.submission_id.to_string(),
            executor_execution_id: row.executor_execution_id.to_string(),
            job_id: row.job_id.to_string(),
            request_id: row.request_id,
            provider_id: row.provider_id,
            model: row.model,
            resolved_state: row.resolved_state,
            error_code: row.error_code,
            blocked_at_ms: row.blocked_at_ms,
            blocked_by: row.blocked_by,
        }
    }
}

impl From<SchedulerActiveJobRow> for SchedulerActiveJob {
    fn from(row: SchedulerActiveJobRow) -> Self {
        Self {
            job_id: row.job_id.to_string(),
            request_id: row.request_id,
            organization_id: row.organization_id,
            organization_name: row.organization_name,
            project_id: row.project_id,
            project_name: row.project_name,
            user_display_name: row.user_display_name,
            user_email: row.user_email,
            service_account_name: row.service_account_name,
            api_key_name: row.api_key_name,
            operation: row.operation,
            provider_id: row.provider_id,
            model: row.model,
            job_state: row.job_state,
            work_state: row.work_state,
            provider_account_id: row.provider_account_id.map(|id| id.to_string()),
            provider_account_name: row.provider_account_name,
            attempt_count: row.attempt_count,
            available_at_ms: row.available_at_ms,
            lease_expires_at_ms: row.lease_expires_at_ms,
            created_at_ms: row.created_at_ms,
            started_at_ms: row.started_at_ms,
        }
    }
}

impl From<SchedulerCapacityRow> for SchedulerCapacity {
    fn from(row: SchedulerCapacityRow) -> Self {
        Self {
            provider_account_id: row.provider_account_id.to_string(),
            account_key: row.account_key,
            provider_id: row.provider_id,
            display_name: row.display_name,
            account_email: row.account_email,
            max_concurrency: row.max_concurrency,
            allocated_count: row.allocated_count,
            available_capacity: row.available_capacity,
        }
    }
}

#[derive(FromRow)]
struct RequestLogRow {
    request_id: String,
    source: String,
    method: String,
    route_pattern: String,
    request_path: String,
    status_code: i16,
    duration_ms: i64,
    error_code: Option<String>,
    idempotency_key_digest: Option<String>,
    tenant_id: Option<String>,
    project_id: Option<String>,
    service_account_id: Option<String>,
    api_key_id: Option<String>,
    actor_user_id: Option<Uuid>,
    credential_owner_user_id: Option<Uuid>,
    auth_kind: Option<String>,
    content_captured: bool,
    job_id: Option<Uuid>,
    operation: Option<String>,
    provider_id: Option<String>,
    model: Option<String>,
    job_state: Option<String>,
    work_state: Option<String>,
    output_count: Option<String>,
    billable_units: Option<String>,
    billing_unit: Option<String>,
    requested_service_tier: Option<String>,
    project_service_tier: Option<String>,
    effective_service_tier: Option<String>,
    service_tier_fallback_reason: Option<String>,
    created_at_ms: i64,
    completed_at_ms: i64,
}

#[derive(FromRow)]
struct AuditLogRow {
    event_id: Uuid,
    actor_user_id: Option<Uuid>,
    actor_email: Option<String>,
    actor_display_name: Option<String>,
    session_id: Option<Uuid>,
    request_id: Option<String>,
    action: String,
    resource_type: Option<String>,
    resource_id: Option<String>,
    outcome: String,
    reason_code: Option<String>,
    metadata: serde_json::Value,
    created_at_ms: i64,
    project_id: Option<String>,
    project_name: Option<String>,
    organization_id: Option<String>,
}

impl AuditLogRow {
    fn into_item(self) -> AuditLogItem {
        let actor_type = if self.actor_user_id.is_some() {
            "user"
        } else {
            "system"
        };
        AuditLogItem {
            id: self.event_id.to_string(),
            object: "audit_log".to_string(),
            event_type: self.action,
            effective_at: self.created_at_ms / 1_000,
            actor: AuditLogActor {
                actor_type: actor_type.to_string(),
                user_id: self.actor_user_id,
                email: self.actor_email,
                display_name: self.actor_display_name,
                session_id: self.session_id,
                ip_address: None,
            },
            project: self.project_id.map(|id| AuditLogProject {
                id,
                name: self.project_name,
                organization_id: self.organization_id,
            }),
            resource: AuditLogResource {
                resource_type: self.resource_type,
                id: self.resource_id,
            },
            request_id: self.request_id,
            outcome: self.outcome,
            reason_code: self.reason_code,
            details: self.metadata,
        }
    }
}

impl RequestLogRow {
    fn into_item(self) -> Result<RequestLogItem, AdminReadError> {
        let status_code = u16::try_from(self.status_code)
            .map_err(|_| invalid("stored request status code is invalid"))?;
        Ok(RequestLogItem {
            request_id: self.request_id,
            source: self.source,
            method: self.method,
            route_pattern: self.route_pattern,
            request_path: self.request_path,
            status_code,
            duration_ms: self.duration_ms,
            error_code: self.error_code,
            idempotency_key_digest: self.idempotency_key_digest,
            tenant_id: self.tenant_id,
            project_id: self.project_id,
            service_account_id: self.service_account_id,
            api_key_id: self.api_key_id,
            actor_user_id: self.actor_user_id,
            credential_owner_user_id: self.credential_owner_user_id,
            auth_kind: self.auth_kind,
            content_captured: self.content_captured,
            job_id: self.job_id.map(|job_id| job_id.to_string()),
            operation: self.operation,
            provider_id: self.provider_id,
            model: self.model,
            job_state: self.job_state,
            work_state: self.work_state,
            output_count: self.output_count,
            billable_units: self.billable_units,
            billing_unit: self.billing_unit,
            requested_service_tier: self.requested_service_tier,
            project_service_tier: self.project_service_tier,
            effective_service_tier: self.effective_service_tier,
            service_tier_fallback_reason: self.service_tier_fallback_reason,
            created_at_ms: self.created_at_ms,
            completed_at_ms: self.completed_at_ms,
        })
    }
}

#[derive(FromRow)]
struct JobRow {
    job_id: Uuid,
    tenant_id: String,
    project_id: Option<String>,
    service_account_id: Option<String>,
    api_key_id: Option<String>,
    auth_kind: Option<String>,
    actor_user_id: Option<Uuid>,
    credential_owner_user_id: Option<Uuid>,
    request_id: String,
    operation: String,
    provider_id: String,
    model: String,
    job_state: String,
    work_state: Option<String>,
    provider_states: serde_json::Value,
    output_count: String,
    billable_units: String,
    billing_metric: String,
    billing_unit: String,
    charged_units: String,
    created_at_ms: i64,
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
    last_error_code: Option<String>,
}

impl JobRow {
    fn into_item(self) -> Result<JobListItem, AdminReadError> {
        let provider_states: Vec<ProviderStateCount> = serde_json::from_value(self.provider_states)
            .map_err(|error| {
                tracing::error!(error = %error, "admin job provider state projection failed");
                AdminReadError::Unavailable
            })?;
        Ok(JobListItem {
            job_id: self.job_id.to_string(),
            tenant_id: self.tenant_id,
            project_id: self.project_id,
            service_account_id: self.service_account_id,
            api_key_id: self.api_key_id,
            auth_kind: self.auth_kind,
            actor_user_id: self.actor_user_id,
            credential_owner_user_id: self.credential_owner_user_id,
            request_id: self.request_id,
            operation: self.operation,
            provider_id: self.provider_id,
            model: self.model,
            job_state: self.job_state,
            work_state: self.work_state,
            provider_states,
            output_count: self.output_count,
            billable_units: self.billable_units,
            billing_metric: self.billing_metric,
            billing_unit: self.billing_unit,
            charged_units: self.charged_units,
            created_at_ms: self.created_at_ms,
            started_at_ms: self.started_at_ms,
            finished_at_ms: self.finished_at_ms,
            last_error_code: self.last_error_code,
        })
    }
}

async fn count_scalar(
    tx: &mut Transaction<'_, Postgres>,
    sql: &'static str,
    as_of_ms: i64,
) -> Result<String, AdminReadError> {
    sqlx::query_scalar(sql)
        .bind(as_of_ms)
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

fn validate_window(window_ms: i64, max_window_ms: i64) -> Result<(), AdminReadError> {
    if window_ms <= 0 {
        return Err(invalid("window_ms must be positive"));
    }
    if window_ms > max_window_ms {
        return Err(invalid(format!(
            "window_ms exceeds the maximum of {max_window_ms}"
        )));
    }
    Ok(())
}

fn validate_jobs_query(query: &JobsQuery) -> Result<(), AdminReadError> {
    validate_window(query.window_ms, MAX_JOBS_WINDOW_MS)?;
    if query.limit == 0 || query.limit > MAX_JOBS_PAGE_SIZE {
        return Err(invalid(format!(
            "limit must be between 1 and {MAX_JOBS_PAGE_SIZE}"
        )));
    }
    validate_simple_filter(query.provider_id.as_deref(), "provider_id", 128)?;
    validate_simple_filter(query.state.as_deref(), "state", 64)?;
    validate_simple_filter(query.operation.as_deref(), "operation", 64)?;
    validate_text_filter(query.model.as_deref(), "model", 255)?;
    validate_text_filter(query.request_or_job_id.as_deref(), "request_or_job_id", 512)?;
    Ok(())
}

fn validate_request_logs_query(query: &RequestLogsQuery) -> Result<(), AdminReadError> {
    validate_window(query.window_ms, MAX_REQUEST_LOG_WINDOW_MS)?;
    if query.limit == 0 || query.limit > MAX_JOBS_PAGE_SIZE {
        return Err(invalid(format!(
            "limit must be between 1 and {MAX_JOBS_PAGE_SIZE}"
        )));
    }
    validate_simple_filter(query.source.as_deref(), "source", 32)?;
    validate_simple_filter(query.status.as_deref(), "status", 32)?;
    validate_simple_filter(query.provider_id.as_deref(), "provider_id", 128)?;
    validate_text_filter(query.model.as_deref(), "model", 255)?;
    validate_text_filter(query.project_id.as_deref(), "project_id", 128)?;
    validate_text_filter(query.api_key_id.as_deref(), "api_key_id", 128)?;
    validate_text_filter(query.request_or_job_id.as_deref(), "request_or_job_id", 512)?;
    if let Some(cursor) = &query.cursor {
        validate_text_filter(Some(&cursor.request_id), "cursor_request_id", 255)?;
    }
    Ok(())
}

fn validate_audit_logs_query(query: &AuditLogsQuery) -> Result<(), AdminReadError> {
    validate_window(query.window_ms, MAX_AUDIT_LOG_WINDOW_MS)?;
    if query.limit == 0 || query.limit > MAX_AUDIT_LOG_PAGE_SIZE {
        return Err(invalid(format!(
            "limit must be between 1 and {MAX_AUDIT_LOG_PAGE_SIZE}"
        )));
    }
    validate_text_filter(query.event_type.as_deref(), "event_type", 128)?;
    validate_simple_filter(query.outcome.as_deref(), "outcome", 16)?;
    if query
        .outcome
        .as_deref()
        .is_some_and(|outcome| !matches!(outcome, "success" | "denied" | "failure"))
    {
        return Err(invalid("outcome must be success, denied, or failure"));
    }
    validate_text_filter(query.project_id.as_deref(), "project_id", 128)?;
    validate_simple_filter(query.resource_type.as_deref(), "resource_type", 64)?;
    validate_text_filter(query.request_id.as_deref(), "request_id", 255)?;
    validate_text_filter(query.query.as_deref(), "query", 255)?;
    Ok(())
}

fn validate_simple_filter(
    value: Option<&str>,
    field: &str,
    max_len: usize,
) -> Result<(), AdminReadError> {
    let Some(value) = value else {
        return Ok(());
    };
    validate_text_filter(Some(value), field, max_len)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(invalid(format!("{field} contains unsupported characters")));
    }
    Ok(())
}

fn validate_text_filter(
    value: Option<&str>,
    field: &str,
    max_len: usize,
) -> Result<(), AdminReadError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_empty() || value.trim() != value {
        return Err(invalid(format!("{field} must be nonblank and trimmed")));
    }
    if value.len() > max_len || value.chars().any(char::is_control) {
        return Err(invalid(format!("{field} is invalid")));
    }
    Ok(())
}

fn normalized_filter(value: Option<&str>) -> Option<&str> {
    value
}

fn invalid(message: impl Into<String>) -> AdminReadError {
    AdminReadError::InvalidQuery(message.into())
}

fn unavailable(error: sqlx::Error) -> AdminReadError {
    tracing::error!(error = %error, "PostgreSQL admin read failed");
    AdminReadError::Unavailable
}
