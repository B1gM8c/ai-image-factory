use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

const CHECK_VERSION: i16 = 3;
const SCANNER_VERSION: &str = "billing-integrity-v3";
const STALE_HOLD_AFTER_MS: i64 = 24 * 60 * 60 * 1_000;
const PROVIDER_COST_AUTHORITY_GRACE_MS: i64 = 24 * 60 * 60 * 1_000;
const CHECK_SET: [&str; 8] = [
    "billing_account_counters",
    "terminal_hold_lifecycle",
    "customer_charge_coverage",
    "customer_refund_coverage",
    "customer_charge_attribution",
    "provider_cost_obligation_coverage",
    "provider_cost_obligation_aging",
    "provider_cost_authority",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BillingIntegrityActor {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListBillingIntegrityRunsRequest {
    pub after: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingIntegrityRunView {
    pub object: &'static str,
    pub run_id: String,
    pub check_version: i16,
    pub scanner_version: String,
    pub check_set: Vec<String>,
    pub scope_type: String,
    pub scope_id: Option<String>,
    pub actor_kind: String,
    pub state: String,
    pub initiated_by_user_id: Option<String>,
    pub as_of_ms: i64,
    pub started_at_ms: i64,
    pub completed_at_ms: i64,
    pub critical_count: i32,
    pub warning_count: i32,
    pub finding_count: i32,
    pub summary: Value,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingIntegrityFindingView {
    pub object: &'static str,
    pub finding_id: String,
    pub run_id: String,
    pub severity: String,
    pub category: String,
    pub finding_code: String,
    pub tenant_id: Option<String>,
    pub currency: Option<String>,
    pub resource_type: String,
    pub resource_id: String,
    pub expected: Value,
    pub actual: Value,
    pub details: Value,
    pub detected_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingIntegrityRunDetail {
    #[serde(flatten)]
    pub run: BillingIntegrityRunView,
    pub findings: Vec<BillingIntegrityFindingView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingIntegrityRunList {
    pub object: &'static str,
    pub data: Vec<BillingIntegrityRunView>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[async_trait]
pub trait BillingIntegrityService: Send + Sync + 'static {
    async fn run(
        &self,
        actor: BillingIntegrityActor,
    ) -> Result<BillingIntegrityRunDetail, ImageGatewayError>;

    async fn list_runs(
        &self,
        request: ListBillingIntegrityRunsRequest,
    ) -> Result<BillingIntegrityRunList, ImageGatewayError>;

    async fn get_run(&self, run_id: Uuid) -> Result<BillingIntegrityRunDetail, ImageGatewayError>;
}

#[derive(Clone)]
pub struct PostgresBillingIntegrityService {
    pool: PgPool,
}

impl PostgresBillingIntegrityService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BillingIntegrityService for PostgresBillingIntegrityService {
    async fn run(
        &self,
        actor: BillingIntegrityActor,
    ) -> Result<BillingIntegrityRunDetail, ImageGatewayError> {
        let started_at_ms = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
        let acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtextextended('billing-integrity-run', 0))",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(unavailable)?;
        if !acquired {
            return Err(ImageGatewayError::conflict(
                "A billing integrity scan is already running",
                None,
                "billing_integrity_run_in_progress",
            ));
        }

        let mut findings = Vec::new();
        findings.extend(scan_account_balances(&mut transaction, started_at_ms).await?);
        findings.extend(scan_stale_holds(&mut transaction, started_at_ms).await?);
        findings.extend(scan_customer_charges(&mut transaction, started_at_ms).await?);
        findings.extend(scan_customer_refunds(&mut transaction, started_at_ms).await?);
        findings.extend(scan_charge_attributions(&mut transaction, started_at_ms).await?);
        findings
            .extend(scan_provider_cost_obligation_coverage(&mut transaction, started_at_ms).await?);
        findings
            .extend(scan_provider_cost_obligation_aging(&mut transaction, started_at_ms).await?);
        findings.extend(scan_provider_cost_authority(&mut transaction, started_at_ms).await?);
        findings.sort_by(|left, right| left.finding_key.cmp(&right.finding_key));

        let critical_count = count_findings(&findings, "critical")?;
        let warning_count = count_findings(&findings, "warning")?;
        let finding_count = i32::try_from(findings.len())
            .map_err(|_| ImageGatewayError::internal("billing integrity finding count overflow"))?;
        let completed_at_ms = now_ms()?;
        let run_id = Uuid::new_v4();
        let summary = json!({
            "checks": CHECK_SET,
            "repair_mode": "disabled",
            "snapshot_isolation": "repeatable_read",
            "stale_hold_after_ms": STALE_HOLD_AFTER_MS.to_string(),
            "provider_cost_authority_grace_ms":
                PROVIDER_COST_AUTHORITY_GRACE_MS.to_string()
        });

        sqlx::query(
            r#"
            INSERT INTO billing_integrity_runs (
                run_id, check_version, scanner_version, check_set,
                scope_type, scope_id, state, actor_kind,
                initiated_by_user_id, session_id, as_of_ms,
                started_at_ms, completed_at_ms, critical_count,
                warning_count, finding_count, summary
            )
            VALUES (
                $1, $2, $3, $4, 'platform', NULL, 'completed', 'manual',
                $5, $6, $7, $7, $8, $9, $10, $11, $12
            )
            "#,
        )
        .bind(run_id)
        .bind(CHECK_VERSION)
        .bind(SCANNER_VERSION)
        .bind(CHECK_SET.as_slice())
        .bind(actor.user_id)
        .bind(actor.session_id)
        .bind(started_at_ms)
        .bind(completed_at_ms)
        .bind(critical_count)
        .bind(warning_count)
        .bind(finding_count)
        .bind(&summary)
        .execute(&mut *transaction)
        .await
        .map_err(unavailable)?;

        for finding in &findings {
            insert_finding(&mut transaction, run_id, finding, completed_at_ms).await?;
        }
        insert_audit(
            &mut transaction,
            run_id,
            actor,
            critical_count,
            warning_count,
            completed_at_ms,
        )
        .await?;
        transaction.commit().await.map_err(unavailable)?;

        Ok(BillingIntegrityRunDetail {
            run: BillingIntegrityRunView {
                object: "billing.integrity_run",
                run_id: run_id.to_string(),
                check_version: CHECK_VERSION,
                scanner_version: SCANNER_VERSION.to_string(),
                check_set: CHECK_SET.iter().map(ToString::to_string).collect(),
                scope_type: "platform".to_string(),
                scope_id: None,
                actor_kind: "manual".to_string(),
                state: "completed".to_string(),
                initiated_by_user_id: Some(actor.user_id.to_string()),
                as_of_ms: started_at_ms,
                started_at_ms,
                completed_at_ms,
                critical_count,
                warning_count,
                finding_count,
                summary,
            },
            findings: findings
                .into_iter()
                .map(|finding| finding.into_view(run_id, completed_at_ms))
                .collect(),
        })
    }

    async fn list_runs(
        &self,
        request: ListBillingIntegrityRunsRequest,
    ) -> Result<BillingIntegrityRunList, ImageGatewayError> {
        let limit = request.limit.unwrap_or(25);
        if !(1..=100).contains(&limit) {
            return Err(ImageGatewayError::invalid_request(
                "limit must be between 1 and 100",
                Some("limit".to_string()),
                "invalid_limit",
            ));
        }
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ImageGatewayError::internal("billing integrity page size overflow"))?;
        let after = request
            .after
            .as_deref()
            .map(parse_uuid_cursor)
            .transpose()?;
        let mut rows = sqlx::query_as::<_, BillingIntegrityRunRow>(
            r#"
            SELECT run.run_id, run.check_version, run.scanner_version,
                   run.check_set, run.scope_type, run.scope_id,
                   run.actor_kind, run.state,
                   run.initiated_by_user_id, run.as_of_ms,
                   run.started_at_ms, run.completed_at_ms,
                   run.critical_count, run.warning_count, run.finding_count,
                   run.summary
            FROM billing_integrity_runs run
            LEFT JOIN billing_integrity_runs cursor ON cursor.run_id = $1
            WHERE $1::UUID IS NULL
               OR (run.completed_at_ms, run.run_id)
                    < (cursor.completed_at_ms, cursor.run_id)
            ORDER BY run.completed_at_ms DESC, run.run_id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_after = has_more
            .then(|| rows.last().map(|row| row.run_id.to_string()))
            .flatten();
        Ok(BillingIntegrityRunList {
            object: "list",
            data: rows
                .into_iter()
                .map(BillingIntegrityRunRow::into_view)
                .collect(),
            has_more,
            next_after,
        })
    }

    async fn get_run(&self, run_id: Uuid) -> Result<BillingIntegrityRunDetail, ImageGatewayError> {
        let run = sqlx::query_as::<_, BillingIntegrityRunRow>(
            r#"
            SELECT run_id, check_version, scanner_version, check_set,
                   scope_type, scope_id, actor_kind, state,
                   initiated_by_user_id, as_of_ms, started_at_ms,
                   completed_at_ms, critical_count, warning_count,
                   finding_count, summary
            FROM billing_integrity_runs
            WHERE run_id = $1
            "#,
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Billing integrity run was not found",
                None,
                "billing_integrity_run_not_found",
            )
        })?;
        let findings = sqlx::query_as::<_, BillingIntegrityFindingRow>(
            r#"
            SELECT finding_id, run_id, severity, category, finding_code,
                   tenant_id, currency, resource_type, resource_id,
                   expected, actual, details, detected_at_ms
            FROM billing_integrity_findings
            WHERE run_id = $1
            ORDER BY
                CASE severity WHEN 'critical' THEN 0 ELSE 1 END,
                category, finding_code, finding_key
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(BillingIntegrityRunDetail {
            run: run.into_view(),
            findings: findings
                .into_iter()
                .map(BillingIntegrityFindingRow::into_view)
                .collect(),
        })
    }
}

#[derive(Clone, Debug)]
struct FindingDraft {
    finding_id: Uuid,
    finding_key: String,
    severity: &'static str,
    category: &'static str,
    finding_code: &'static str,
    tenant_id: Option<String>,
    currency: Option<String>,
    resource_type: &'static str,
    resource_id: String,
    expected: Value,
    actual: Value,
    details: Value,
}

impl FindingDraft {
    fn into_view(self, run_id: Uuid, detected_at_ms: i64) -> BillingIntegrityFindingView {
        BillingIntegrityFindingView {
            object: "billing.integrity_finding",
            finding_id: self.finding_id.to_string(),
            run_id: run_id.to_string(),
            severity: self.severity.to_string(),
            category: self.category.to_string(),
            finding_code: self.finding_code.to_string(),
            tenant_id: self.tenant_id,
            currency: self.currency,
            resource_type: self.resource_type.to_string(),
            resource_id: self.resource_id,
            expected: self.expected,
            actual: self.actual,
            details: self.details,
            detected_at_ms,
        }
    }
}

#[derive(FromRow)]
struct AccountBalanceRow {
    tenant_id: String,
    currency: String,
    account_held_micros: i64,
    account_captured_micros: i64,
    account_refunded_micros: i64,
    expected_held_micros: i64,
    expected_captured_micros: i64,
    expected_refunded_micros: i64,
}

async fn scan_account_balances(
    transaction: &mut Transaction<'_, Postgres>,
    _: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let rows = sqlx::query_as::<_, AccountBalanceRow>(
        r#"
        WITH open_hold_totals AS (
            SELECT tenant_id, currency,
                   SUM(held_micros)::NUMERIC AS held_micros
            FROM output_holds
            WHERE state = 'held'
            GROUP BY tenant_id, currency
            UNION ALL
            SELECT tenant_id, currency,
                   SUM(held_micros)::NUMERIC
            FROM customer_billing_holds
            WHERE state = 'held'
            GROUP BY tenant_id, currency
        ),
        expected_holds AS (
            SELECT tenant_id, currency,
                   COALESCE(SUM(held_micros), 0)::BIGINT AS held_micros
            FROM open_hold_totals
            GROUP BY tenant_id, currency
        ),
        expected_charges AS (
            SELECT account.owner_id AS tenant_id, account.currency,
                   COALESCE(SUM(posting.amount_micros), 0)::BIGINT
                       AS captured_micros
            FROM ledger_accounts account
            JOIN ledger_postings posting
              ON posting.account_id = account.account_id
             AND posting.currency = account.currency
            JOIN ledger_transactions transaction
              ON transaction.transaction_id = posting.transaction_id
             AND transaction.transaction_type IN (
                 'customer_charge', 'customer_job_charge'
             )
            JOIN ledger_transaction_seals seal
              ON seal.transaction_id = transaction.transaction_id
            WHERE account.owner_type = 'tenant'
              AND account.account_type = 'receivable'
            GROUP BY account.owner_id, account.currency
        ),
        expected_refunds AS (
            SELECT tenant_id, currency,
                   COALESCE(SUM(amount_micros::NUMERIC), 0)::BIGINT
                       AS refunded_micros
            FROM customer_refunds
            GROUP BY tenant_id, currency
        )
        SELECT account.tenant_id, account.currency,
               account.held_micros AS account_held_micros,
               account.captured_micros AS account_captured_micros,
               account.refunded_micros AS account_refunded_micros,
               COALESCE(expected_holds.held_micros, 0)::BIGINT
                   AS expected_held_micros,
               COALESCE(expected_charges.captured_micros, 0)::BIGINT
                   AS expected_captured_micros,
               COALESCE(expected_refunds.refunded_micros, 0)::BIGINT
                   AS expected_refunded_micros
        FROM billing_accounts account
        LEFT JOIN expected_holds
          ON expected_holds.tenant_id = account.tenant_id
         AND expected_holds.currency = account.currency
        LEFT JOIN expected_charges
          ON expected_charges.tenant_id = account.tenant_id
         AND expected_charges.currency = account.currency
        LEFT JOIN expected_refunds
          ON expected_refunds.tenant_id = account.tenant_id
         AND expected_refunds.currency = account.currency
        WHERE account.held_micros <> COALESCE(expected_holds.held_micros, 0)
           OR account.captured_micros
                <> COALESCE(expected_charges.captured_micros, 0)
           OR account.refunded_micros
                <> COALESCE(expected_refunds.refunded_micros, 0)
        ORDER BY account.tenant_id, account.currency
        "#,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| FindingDraft {
            finding_id: Uuid::new_v4(),
            finding_key: format!(
                "billing_account_counter_mismatch:{}:{}",
                row.tenant_id, row.currency
            ),
            severity: "critical",
            category: "account_balance",
            finding_code: "billing_account_counter_mismatch",
            tenant_id: Some(row.tenant_id.clone()),
            currency: Some(row.currency.clone()),
            resource_type: "billing_account",
            resource_id: format!("{}:{}", row.tenant_id, row.currency),
            expected: json!({
                "held_micros": row.expected_held_micros.to_string(),
                "captured_micros": row.expected_captured_micros.to_string(),
                "refunded_micros": row.expected_refunded_micros.to_string(),
            }),
            actual: json!({
                "held_micros": row.account_held_micros.to_string(),
                "captured_micros": row.account_captured_micros.to_string(),
                "refunded_micros": row.account_refunded_micros.to_string(),
            }),
            details: json!({
                "held_authority": ["output_holds", "customer_billing_holds"],
                "captured_authority": "sealed_customer_receivable_postings",
                "refunded_authority": "immutable_customer_refunds",
            }),
        })
        .collect())
}

#[derive(FromRow)]
struct StaleHoldRow {
    hold_kind: String,
    hold_id: String,
    tenant_id: String,
    currency: String,
    held_micros: i64,
    job_id: Uuid,
    job_state: String,
    updated_at_ms: i64,
}

async fn scan_stale_holds(
    transaction: &mut Transaction<'_, Postgres>,
    scan_at_ms: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let cutoff = scan_at_ms
        .checked_sub(STALE_HOLD_AFTER_MS)
        .ok_or_else(|| ImageGatewayError::internal("billing integrity cutoff underflow"))?;
    let rows = sqlx::query_as::<_, StaleHoldRow>(
        r#"
        SELECT 'output_hold'::TEXT AS hold_kind,
               hold.output_id::TEXT AS hold_id,
               hold.tenant_id, hold.currency, hold.held_micros,
               hold.job_id, job.state AS job_state, hold.updated_at_ms
        FROM output_holds hold
        JOIN jobs job ON job.job_id = hold.job_id
        WHERE hold.state = 'held'
          AND job.state IN ('succeeded', 'failed', 'uncertain')
          AND hold.updated_at_ms < $1
        UNION ALL
        SELECT 'customer_billing_hold'::TEXT,
               hold.hold_id::TEXT,
               hold.tenant_id, hold.currency, hold.held_micros,
               hold.job_id, job.state, hold.updated_at_ms
        FROM customer_billing_holds hold
        JOIN jobs job ON job.job_id = hold.job_id
        WHERE hold.state = 'held'
          AND job.state IN ('succeeded', 'failed', 'uncertain')
          AND hold.updated_at_ms < $1
        ORDER BY tenant_id, currency, hold_kind, hold_id
        "#,
    )
    .bind(cutoff)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| FindingDraft {
            finding_id: Uuid::new_v4(),
            finding_key: format!("stale_terminal_hold:{}:{}", row.hold_kind, row.hold_id),
            severity: "warning",
            category: "hold_lifecycle",
            finding_code: "stale_terminal_hold",
            tenant_id: Some(row.tenant_id),
            currency: Some(row.currency),
            resource_type: "billing_hold",
            resource_id: row.hold_id,
            expected: json!({
                "state": "settled_or_released",
                "maximum_age_ms": STALE_HOLD_AFTER_MS.to_string(),
            }),
            actual: json!({
                "state": "held",
                "held_micros": row.held_micros.to_string(),
                "updated_at_ms": row.updated_at_ms,
            }),
            details: json!({
                "hold_kind": row.hold_kind,
                "job_id": row.job_id,
                "job_state": row.job_state,
            }),
        })
        .collect())
}

#[derive(FromRow)]
struct CustomerChargeRow {
    contract_kind: String,
    resource_id: String,
    tenant_id: String,
    currency: String,
    expected_amount_micros: i64,
    transaction_count: i64,
    posting_count: i64,
    posting_sum_micros: i64,
    receivable_micros: Option<i64>,
    revenue_micros: Option<i64>,
    sealed_count: i64,
}

async fn scan_customer_charges(
    transaction: &mut Transaction<'_, Postgres>,
    _: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let rows = sqlx::query_as::<_, CustomerChargeRow>(
        r#"
        WITH charges AS (
            SELECT 'output_v2'::TEXT AS contract_kind,
                   usage.output_id::TEXT AS resource_id,
                   job.tenant_id, usage.currency,
                   usage.amount_micros AS expected_amount_micros,
                   transaction.transaction_id
            FROM rated_usage usage
            JOIN jobs job ON job.job_id = usage.job_id
            LEFT JOIN ledger_transactions transaction
              ON transaction.transaction_type = 'customer_charge'
             AND transaction.source_output_id = usage.output_id
            UNION ALL
            SELECT 'job_v4'::TEXT,
                   usage.job_id::TEXT,
                   job.tenant_id, usage.currency,
                   usage.total_amount_micros,
                   transaction.transaction_id
            FROM customer_rated_usage usage
            JOIN jobs job ON job.job_id = usage.job_id
            LEFT JOIN ledger_transactions transaction
              ON transaction.transaction_type = 'customer_job_charge'
             AND transaction.source_job_id = usage.job_id
        )
        SELECT charge.contract_kind, charge.resource_id,
               charge.tenant_id, charge.currency,
               charge.expected_amount_micros,
               COUNT(DISTINCT charge.transaction_id)::BIGINT AS transaction_count,
               COUNT(posting.posting_no)::BIGINT AS posting_count,
               COALESCE(SUM(posting.amount_micros), 0)::BIGINT
                   AS posting_sum_micros,
               MAX(posting.amount_micros) FILTER (
                   WHERE account.owner_type = 'tenant'
                     AND account.owner_id = charge.tenant_id
                     AND account.account_type = 'receivable'
               ) AS receivable_micros,
               MIN(posting.amount_micros) FILTER (
                   WHERE account.owner_type = 'platform'
                     AND account.account_type = 'revenue'
               ) AS revenue_micros,
               COUNT(DISTINCT seal.transaction_id)::BIGINT AS sealed_count
        FROM charges charge
        LEFT JOIN ledger_postings posting
          ON posting.transaction_id = charge.transaction_id
        LEFT JOIN ledger_accounts account
          ON account.account_id = posting.account_id
         AND account.currency = posting.currency
        LEFT JOIN ledger_transaction_seals seal
          ON seal.transaction_id = charge.transaction_id
        GROUP BY charge.contract_kind, charge.resource_id,
                 charge.tenant_id, charge.currency,
                 charge.expected_amount_micros
        HAVING (
            charge.expected_amount_micros = 0
            AND COUNT(DISTINCT charge.transaction_id) <> 0
        ) OR (
            charge.expected_amount_micros > 0
            AND (
                COUNT(DISTINCT charge.transaction_id) <> 1
                OR COUNT(posting.posting_no) <> 2
                OR COALESCE(SUM(posting.amount_micros), 0) <> 0
                OR MAX(posting.amount_micros) FILTER (
                    WHERE account.owner_type = 'tenant'
                      AND account.owner_id = charge.tenant_id
                      AND account.account_type = 'receivable'
                ) IS DISTINCT FROM charge.expected_amount_micros
                OR MIN(posting.amount_micros) FILTER (
                    WHERE account.owner_type = 'platform'
                      AND account.account_type = 'revenue'
                ) IS DISTINCT FROM -charge.expected_amount_micros
                OR COUNT(DISTINCT seal.transaction_id) <> 1
            )
        )
        ORDER BY charge.tenant_id, charge.currency,
                 charge.contract_kind, charge.resource_id
        "#,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| FindingDraft {
            finding_id: Uuid::new_v4(),
            finding_key: format!(
                "customer_charge_mismatch:{}:{}",
                row.contract_kind, row.resource_id
            ),
            severity: "critical",
            category: "customer_charge",
            finding_code: "customer_charge_mismatch",
            tenant_id: Some(row.tenant_id),
            currency: Some(row.currency),
            resource_type: "rated_usage",
            resource_id: row.resource_id,
            expected: json!({
                "amount_micros": row.expected_amount_micros.to_string(),
                "transaction_count": if row.expected_amount_micros == 0 { 0 } else { 1 },
                "posting_count": if row.expected_amount_micros == 0 { 0 } else { 2 },
                "posting_sum_micros": "0",
                "sealed_count": if row.expected_amount_micros == 0 { 0 } else { 1 },
            }),
            actual: json!({
                "transaction_count": row.transaction_count,
                "posting_count": row.posting_count,
                "posting_sum_micros": row.posting_sum_micros.to_string(),
                "receivable_micros": row.receivable_micros.map(|value| value.to_string()),
                "revenue_micros": row.revenue_micros.map(|value| value.to_string()),
                "sealed_count": row.sealed_count,
            }),
            details: json!({
                "contract_kind": row.contract_kind,
            }),
        })
        .collect())
}

#[derive(FromRow)]
struct CustomerRefundIntegrityRow {
    refund_transaction_id: Uuid,
    tenant_id: Option<String>,
    currency: Option<String>,
    finding_code: String,
    evidence_count: i64,
    refund_transaction_type: Option<String>,
    reverses_transaction_id: Option<Uuid>,
    original_transaction_type: Option<String>,
    payload_matches: bool,
    refund_sealed_count: i64,
    original_sealed_count: i64,
    refund_posting_count: i64,
    refund_posting_sum_micros: i64,
    refund_receivable_micros: i64,
    refund_revenue_micros: i64,
    expected_amount_micros: Option<i64>,
    cumulative_refunded_micros: i64,
    original_receivable_micros: i64,
}

async fn scan_customer_refunds(
    transaction: &mut Transaction<'_, Postgres>,
    _: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let rows = sqlx::query_as::<_, CustomerRefundIntegrityRow>(
        r#"
        WITH candidates AS (
            SELECT transaction_id AS refund_transaction_id
            FROM ledger_transactions
            WHERE transaction_type = 'customer_refund'
            UNION
            SELECT refund_transaction_id
            FROM customer_refunds
        ),
        refund_posting_shapes AS (
            SELECT candidate.refund_transaction_id,
                   COUNT(posting.posting_no)::BIGINT AS posting_count,
                   COALESCE(SUM(posting.amount_micros), 0)::BIGINT
                       AS posting_sum_micros,
                   COALESCE(SUM(posting.amount_micros) FILTER (
                       WHERE account.owner_type = 'tenant'
                         AND account.account_type = 'receivable'
                   ), 0)::BIGINT AS receivable_micros,
                   COALESCE(SUM(posting.amount_micros) FILTER (
                       WHERE account.owner_type = 'platform'
                         AND account.owner_id = 'platform'
                         AND account.account_type = 'revenue'
                   ), 0)::BIGINT AS revenue_micros
            FROM candidates candidate
            LEFT JOIN ledger_postings posting
              ON posting.transaction_id = candidate.refund_transaction_id
            LEFT JOIN ledger_accounts account
              ON account.account_id = posting.account_id
             AND account.currency = posting.currency
            GROUP BY candidate.refund_transaction_id
        ),
        original_posting_shapes AS (
            SELECT refund.original_transaction_id,
                   refund.tenant_id,
                   COUNT(posting.posting_no)::BIGINT AS posting_count,
                   COALESCE(SUM(posting.amount_micros), 0)::BIGINT
                       AS posting_sum_micros,
                   COALESCE(SUM(posting.amount_micros) FILTER (
                       WHERE account.owner_type = 'tenant'
                         AND account.owner_id = refund.tenant_id
                         AND account.account_type = 'receivable'
                   ), 0)::BIGINT AS receivable_micros
            FROM customer_refunds refund
            LEFT JOIN ledger_postings posting
              ON posting.transaction_id = refund.original_transaction_id
            LEFT JOIN ledger_accounts account
              ON account.account_id = posting.account_id
             AND account.currency = posting.currency
            GROUP BY refund.original_transaction_id, refund.tenant_id
        ),
        cumulative_refunds AS (
            SELECT original_transaction_id,
                   SUM(amount_micros::NUMERIC)::BIGINT AS refunded_micros
            FROM customer_refunds
            GROUP BY original_transaction_id
        )
        SELECT candidate.refund_transaction_id,
               refund.tenant_id,
               COALESCE(refund.currency, refund_transaction.currency)
                   AS currency,
               CASE
                   WHEN refund.refund_id IS NULL
                   THEN 'customer_refund_evidence_missing'
                   ELSE 'customer_refund_mismatch'
               END AS finding_code,
               CASE WHEN refund.refund_id IS NULL THEN 0 ELSE 1 END::BIGINT
                   AS evidence_count,
               refund_transaction.transaction_type
                   AS refund_transaction_type,
               refund_transaction.reverses_transaction_id,
               original_transaction.transaction_type
                   AS original_transaction_type,
               COALESCE(
                   refund_transaction.payload_hash = refund.request_hash,
                   FALSE
               ) AS payload_matches,
               CASE WHEN refund_seal.transaction_id IS NULL THEN 0 ELSE 1 END::BIGINT
                   AS refund_sealed_count,
               CASE WHEN original_seal.transaction_id IS NULL THEN 0 ELSE 1 END::BIGINT
                   AS original_sealed_count,
               refund_shape.posting_count AS refund_posting_count,
               refund_shape.posting_sum_micros
                   AS refund_posting_sum_micros,
               refund_shape.receivable_micros AS refund_receivable_micros,
               refund_shape.revenue_micros AS refund_revenue_micros,
               refund.amount_micros AS expected_amount_micros,
               COALESCE(cumulative.refunded_micros, 0)::BIGINT
                   AS cumulative_refunded_micros,
               COALESCE(original_shape.receivable_micros, 0)::BIGINT
                   AS original_receivable_micros
        FROM candidates candidate
        LEFT JOIN customer_refunds refund
          ON refund.refund_transaction_id = candidate.refund_transaction_id
        LEFT JOIN ledger_transactions refund_transaction
          ON refund_transaction.transaction_id =
             candidate.refund_transaction_id
        LEFT JOIN ledger_transactions original_transaction
          ON original_transaction.transaction_id =
             COALESCE(
                 refund.original_transaction_id,
                 refund_transaction.reverses_transaction_id
             )
        LEFT JOIN ledger_transaction_seals refund_seal
          ON refund_seal.transaction_id = candidate.refund_transaction_id
        LEFT JOIN ledger_transaction_seals original_seal
          ON original_seal.transaction_id =
             COALESCE(
                 refund.original_transaction_id,
                 refund_transaction.reverses_transaction_id
             )
        JOIN refund_posting_shapes refund_shape
          ON refund_shape.refund_transaction_id =
             candidate.refund_transaction_id
        LEFT JOIN original_posting_shapes original_shape
          ON original_shape.original_transaction_id =
             refund.original_transaction_id
         AND original_shape.tenant_id = refund.tenant_id
        LEFT JOIN cumulative_refunds cumulative
          ON cumulative.original_transaction_id =
             refund.original_transaction_id
        WHERE refund.refund_id IS NULL
           OR refund_transaction.transaction_type IS DISTINCT FROM
              'customer_refund'
           OR refund_transaction.currency IS DISTINCT FROM refund.currency
           OR refund_transaction.reverses_transaction_id IS DISTINCT FROM
              refund.original_transaction_id
           OR refund_transaction.payload_hash IS DISTINCT FROM
              refund.request_hash
           OR original_transaction.transaction_type NOT IN (
              'customer_charge', 'customer_job_charge'
           )
           OR original_transaction.currency IS DISTINCT FROM refund.currency
           OR refund_seal.transaction_id IS NULL
           OR original_seal.transaction_id IS NULL
           OR refund_shape.posting_count <> 2
           OR refund_shape.posting_sum_micros <> 0
           OR refund_shape.receivable_micros <> -refund.amount_micros
           OR refund_shape.revenue_micros <> refund.amount_micros
           OR original_shape.posting_count <> 2
           OR original_shape.posting_sum_micros <> 0
           OR original_shape.receivable_micros <= 0
           OR cumulative.refunded_micros > original_shape.receivable_micros
        ORDER BY candidate.refund_transaction_id
        "#,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;

    Ok(rows
        .into_iter()
        .map(|row| FindingDraft {
            finding_id: Uuid::new_v4(),
            finding_key: format!("{}:{}", row.finding_code, row.refund_transaction_id),
            severity: "critical",
            category: "customer_refund",
            finding_code: if row.finding_code == "customer_refund_evidence_missing" {
                "customer_refund_evidence_missing"
            } else {
                "customer_refund_mismatch"
            },
            tenant_id: row.tenant_id,
            currency: row.currency,
            resource_type: "ledger_transaction",
            resource_id: row.refund_transaction_id.to_string(),
            expected: json!({
                "evidence_count": 1,
                "refund_transaction_type": "customer_refund",
                "refund_sealed_count": 1,
                "original_transaction_type": [
                    "customer_charge",
                    "customer_job_charge"
                ],
                "original_sealed_count": 1,
                "payload_matches": true,
                "posting_count": 2,
                "posting_sum_micros": "0",
                "cumulative_refund_not_above_original": true,
            }),
            actual: json!({
                "evidence_count": row.evidence_count,
                "refund_transaction_type": row.refund_transaction_type,
                "reverses_transaction_id":
                    row.reverses_transaction_id.map(|value| value.to_string()),
                "original_transaction_type": row.original_transaction_type,
                "refund_sealed_count": row.refund_sealed_count,
                "original_sealed_count": row.original_sealed_count,
                "payload_matches": row.payload_matches,
                "posting_count": row.refund_posting_count,
                "posting_sum_micros":
                    row.refund_posting_sum_micros.to_string(),
                "receivable_micros":
                    row.refund_receivable_micros.to_string(),
                "revenue_micros": row.refund_revenue_micros.to_string(),
                "expected_amount_micros":
                    row.expected_amount_micros.map(|value| value.to_string()),
                "cumulative_refunded_micros":
                    row.cumulative_refunded_micros.to_string(),
                "original_receivable_micros":
                    row.original_receivable_micros.to_string(),
            }),
            details: json!({
                "authority": "immutable_customer_refunds_and_sealed_ledger",
            }),
        })
        .collect())
}

#[derive(FromRow)]
struct MissingAttributionRow {
    job_id: Uuid,
    tenant_id: String,
    economics_contract_version: i16,
    currency: String,
}

async fn scan_charge_attributions(
    transaction: &mut Transaction<'_, Postgres>,
    _: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let rows = sqlx::query_as::<_, MissingAttributionRow>(
        r#"
        WITH charged_jobs AS (
            SELECT usage.job_id, usage.currency
            FROM rated_usage usage
            WHERE usage.amount_micros > 0
            UNION
            SELECT usage.job_id, usage.currency
            FROM customer_rated_usage usage
            WHERE usage.total_amount_micros > 0
        )
        SELECT job.job_id, job.tenant_id, job.economics_contract_version,
               charged.currency
        FROM charged_jobs charged
        JOIN jobs job ON job.job_id = charged.job_id
        LEFT JOIN job_auth_attributions attribution
          ON attribution.job_id = job.job_id
        WHERE attribution.job_id IS NULL
        ORDER BY job.created_at_ms, job.job_id
        "#,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| FindingDraft {
            finding_id: Uuid::new_v4(),
            finding_key: format!("customer_charge_attribution_missing:{}", row.job_id),
            severity: if row.economics_contract_version >= 4 {
                "critical"
            } else {
                "warning"
            },
            category: "attribution",
            finding_code: "customer_charge_attribution_missing",
            tenant_id: Some(row.tenant_id),
            currency: Some(row.currency),
            resource_type: "job",
            resource_id: row.job_id.to_string(),
            expected: json!({
                "job_auth_attribution": "present",
            }),
            actual: json!({
                "job_auth_attribution": "missing",
            }),
            details: json!({
                "economics_contract_version": row.economics_contract_version,
                "severity_policy": "v4_is_critical_legacy_is_warning",
            }),
        })
        .collect())
}

#[derive(FromRow)]
struct MissingProviderCostAuthorityRow {
    usage_fact_id: Uuid,
    job_id: Uuid,
    receipt_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    execution_surface: String,
    quantity: i64,
    unit: String,
    created_at_ms: i64,
}

async fn scan_provider_cost_authority(
    transaction: &mut Transaction<'_, Postgres>,
    scan_at_ms: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let cutoff = scan_at_ms
        .checked_sub(PROVIDER_COST_AUTHORITY_GRACE_MS)
        .ok_or_else(|| ImageGatewayError::internal("provider cost authority cutoff underflow"))?;
    let rows = sqlx::query_as::<_, MissingProviderCostAuthorityRow>(
        r#"
        SELECT fact.usage_fact_id, fact.job_id, fact.receipt_id,
               fact.provider_id, fact.provider_account_id,
               fact.execution_surface, fact.quantity, fact.unit,
               fact.created_at_ms
        FROM provider_usage_facts fact
        LEFT JOIN provider_cost_authority_claims claim
          ON claim.authority_kind = 'provider_actual'
         AND claim.source_usage_fact_id = fact.usage_fact_id
        WHERE fact.fact_domain = 'provider_actual'
          AND fact.metric = 'provider_reported_cost'
          AND fact.provider_account_id IS NOT NULL
          AND fact.created_at_ms < $1
          AND claim.claim_id IS NULL
        ORDER BY fact.created_at_ms, fact.usage_fact_id
        "#,
    )
    .bind(cutoff)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| FindingDraft {
            finding_id: Uuid::new_v4(),
            finding_key: format!("provider_cost_authority_missing:{}", row.usage_fact_id),
            severity: "critical",
            category: "provider_cost",
            finding_code: "provider_cost_authority_missing",
            tenant_id: None,
            currency: Some("USD".to_string()),
            resource_type: "provider_usage_fact",
            resource_id: row.usage_fact_id.to_string(),
            expected: json!({
                "authority_kind": "provider_actual",
                "maximum_unclaimed_age_ms":
                    PROVIDER_COST_AUTHORITY_GRACE_MS.to_string(),
            }),
            actual: json!({
                "authority_kind": "missing",
                "quantity": row.quantity.to_string(),
                "unit": row.unit,
                "created_at_ms": row.created_at_ms,
            }),
            details: json!({
                "job_id": row.job_id,
                "receipt_id": row.receipt_id,
                "provider_id": row.provider_id,
                "provider_account_id": row.provider_account_id,
                "execution_surface": row.execution_surface,
                "scope_policy":
                    "only_explicit_provider_actual_facts_are_checked",
            }),
        })
        .collect())
}

#[derive(FromRow)]
struct MissingProviderCostObligationRow {
    receipt_id: Uuid,
    submission_id: Uuid,
    output_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    provider_id: String,
    outcome: String,
    created_at_ms: i64,
}

async fn scan_provider_cost_obligation_coverage(
    transaction: &mut Transaction<'_, Postgres>,
    _: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let rows = sqlx::query_as::<_, MissingProviderCostObligationRow>(
        r#"
        SELECT receipt.receipt_id, receipt.submission_id,
               receipt.output_id, receipt.job_id, job.tenant_id,
               receipt.provider_id, receipt.outcome, receipt.created_at_ms
        FROM provider_receipts receipt
        JOIN jobs job ON job.job_id = receipt.job_id
        LEFT JOIN provider_cost_obligations obligation
          ON obligation.receipt_id = receipt.receipt_id
        WHERE obligation.receipt_id IS NULL
        ORDER BY receipt.created_at_ms, receipt.receipt_id
        "#,
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| FindingDraft {
            finding_id: Uuid::new_v4(),
            finding_key: format!("provider_cost_obligation_missing:{}", row.receipt_id),
            severity: "critical",
            category: "provider_cost",
            finding_code: "provider_cost_obligation_missing",
            tenant_id: Some(row.tenant_id),
            currency: None,
            resource_type: "provider_receipt",
            resource_id: row.receipt_id.to_string(),
            expected: json!({
                "provider_cost_obligation": "present",
                "identity_scope": "receipt",
            }),
            actual: json!({
                "provider_cost_obligation": "missing",
                "receipt_outcome": row.outcome,
                "created_at_ms": row.created_at_ms,
            }),
            details: json!({
                "job_id": row.job_id,
                "output_id": row.output_id,
                "submission_id": row.submission_id,
                "provider_id": row.provider_id,
            }),
        })
        .collect())
}

#[derive(FromRow)]
struct OverdueProviderCostObligationRow {
    receipt_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    provider_id: String,
    provider_account_id: Option<Uuid>,
    currency: Option<String>,
    state: String,
    expected_authority_kind: Option<String>,
    pending_reason_code: Option<String>,
    due_at_ms: i64,
    escalate_at_ms: i64,
    pending_since_ms: Option<i64>,
    review_attempt_count: i32,
}

async fn scan_provider_cost_obligation_aging(
    transaction: &mut Transaction<'_, Postgres>,
    scan_at_ms: i64,
) -> Result<Vec<FindingDraft>, ImageGatewayError> {
    let rows = sqlx::query_as::<_, OverdueProviderCostObligationRow>(
        r#"
        SELECT obligation.receipt_id, obligation.job_id, job.tenant_id,
               obligation.provider_id, obligation.provider_account_id,
               obligation.currency, obligation.state,
               obligation.expected_authority_kind,
               obligation.pending_reason_code,
               obligation.due_at_ms, obligation.escalate_at_ms,
               obligation.pending_since_ms,
               obligation.review_attempt_count
        FROM provider_cost_obligations obligation
        JOIN jobs job ON job.job_id = obligation.job_id
        WHERE obligation.state IN ('expected', 'pending')
          AND obligation.due_at_ms <= $1
        ORDER BY obligation.due_at_ms, obligation.receipt_id
        "#,
    )
    .bind(scan_at_ms)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let unresolved_policy = matches!(
                row.pending_reason_code.as_deref(),
                Some("policy_unresolved" | "legacy_unbound_account")
            );
            let escalated = row.escalate_at_ms <= scan_at_ms;
            FindingDraft {
                finding_id: Uuid::new_v4(),
                finding_key: format!("provider_cost_obligation_overdue:{}", row.receipt_id),
                severity: if unresolved_policy || escalated {
                    "critical"
                } else {
                    "warning"
                },
                category: "provider_cost",
                finding_code: "provider_cost_obligation_overdue",
                tenant_id: Some(row.tenant_id),
                currency: row.currency,
                resource_type: "provider_cost_obligation",
                resource_id: row.receipt_id.to_string(),
                expected: json!({
                    "state": "settled_or_evidence_backed_waived",
                    "due_at_ms": row.due_at_ms,
                    "escalate_at_ms": row.escalate_at_ms,
                }),
                actual: json!({
                    "state": row.state,
                    "expected_authority_kind": row.expected_authority_kind,
                    "pending_reason_code": row.pending_reason_code,
                    "pending_since_ms": row.pending_since_ms,
                    "review_attempt_count": row.review_attempt_count,
                }),
                details: json!({
                    "job_id": row.job_id,
                    "provider_id": row.provider_id,
                    "provider_account_id": row.provider_account_id,
                    "severity_policy": if unresolved_policy {
                        "classification_gap_is_critical_after_due"
                    } else if escalated {
                        "evidence_pending_is_critical_after_escalation"
                    } else {
                        "evidence_pending_is_warning_before_escalation"
                    },
                }),
            }
        })
        .collect())
}

async fn insert_finding(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    finding: &FindingDraft,
    detected_at_ms: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO billing_integrity_findings (
            finding_id, run_id, finding_key, severity, category, finding_code,
            tenant_id, currency, resource_type, resource_id,
            expected, actual, details, detected_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14
        )
        "#,
    )
    .bind(finding.finding_id)
    .bind(run_id)
    .bind(&finding.finding_key)
    .bind(finding.severity)
    .bind(finding.category)
    .bind(finding.finding_code)
    .bind(&finding.tenant_id)
    .bind(&finding.currency)
    .bind(finding.resource_type)
    .bind(&finding.resource_id)
    .bind(&finding.expected)
    .bind(&finding.actual)
    .bind(&finding.details)
    .bind(detected_at_ms)
    .execute(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn insert_audit(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    actor: BillingIntegrityActor,
    critical_count: i32,
    warning_count: i32,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events (
            event_id, actor_user_id, session_id, request_id, action,
            resource_type, resource_id, outcome, reason_code, metadata,
            created_at_ms
        )
        VALUES (
            $1, $2, $3, NULL, 'billing.integrity.run',
            'billing_integrity_run', $4, 'success', NULL, $5, $6
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor.user_id)
    .bind(actor.session_id)
    .bind(run_id.to_string())
    .bind(json!({
        "check_version": CHECK_VERSION,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "repair_mode": "disabled",
    }))
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(())
}

#[derive(FromRow)]
struct BillingIntegrityRunRow {
    run_id: Uuid,
    check_version: i16,
    scanner_version: String,
    check_set: Vec<String>,
    scope_type: String,
    scope_id: Option<String>,
    actor_kind: String,
    state: String,
    initiated_by_user_id: Option<Uuid>,
    as_of_ms: i64,
    started_at_ms: i64,
    completed_at_ms: i64,
    critical_count: i32,
    warning_count: i32,
    finding_count: i32,
    summary: Value,
}

impl BillingIntegrityRunRow {
    fn into_view(self) -> BillingIntegrityRunView {
        BillingIntegrityRunView {
            object: "billing.integrity_run",
            run_id: self.run_id.to_string(),
            check_version: self.check_version,
            scanner_version: self.scanner_version,
            check_set: self.check_set,
            scope_type: self.scope_type,
            scope_id: self.scope_id,
            actor_kind: self.actor_kind,
            state: self.state,
            initiated_by_user_id: self.initiated_by_user_id.map(|value| value.to_string()),
            as_of_ms: self.as_of_ms,
            started_at_ms: self.started_at_ms,
            completed_at_ms: self.completed_at_ms,
            critical_count: self.critical_count,
            warning_count: self.warning_count,
            finding_count: self.finding_count,
            summary: self.summary,
        }
    }
}

#[derive(FromRow)]
struct BillingIntegrityFindingRow {
    finding_id: Uuid,
    run_id: Uuid,
    severity: String,
    category: String,
    finding_code: String,
    tenant_id: Option<String>,
    currency: Option<String>,
    resource_type: String,
    resource_id: String,
    expected: Value,
    actual: Value,
    details: Value,
    detected_at_ms: i64,
}

impl BillingIntegrityFindingRow {
    fn into_view(self) -> BillingIntegrityFindingView {
        BillingIntegrityFindingView {
            object: "billing.integrity_finding",
            finding_id: self.finding_id.to_string(),
            run_id: self.run_id.to_string(),
            severity: self.severity,
            category: self.category,
            finding_code: self.finding_code,
            tenant_id: self.tenant_id,
            currency: self.currency,
            resource_type: self.resource_type,
            resource_id: self.resource_id,
            expected: self.expected,
            actual: self.actual,
            details: self.details,
            detected_at_ms: self.detected_at_ms,
        }
    }
}

fn count_findings(findings: &[FindingDraft], severity: &str) -> Result<i32, ImageGatewayError> {
    i32::try_from(
        findings
            .iter()
            .filter(|finding| finding.severity == severity)
            .count(),
    )
    .map_err(|_| ImageGatewayError::internal("billing integrity severity count overflow"))
}

fn parse_uuid_cursor(value: &str) -> Result<Uuid, ImageGatewayError> {
    Uuid::parse_str(value).map_err(|_| {
        ImageGatewayError::invalid_request(
            "after must be a billing integrity run UUID",
            Some("after".to_string()),
            "invalid_cursor",
        )
    })
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ImageGatewayError::internal("system clock is before the Unix epoch"))?
            .as_millis(),
    )
    .map_err(|_| ImageGatewayError::internal("system clock overflow"))
}

fn unavailable(error: impl std::fmt::Display) -> ImageGatewayError {
    tracing::error!(error = %error, "billing integrity database operation failed");
    ImageGatewayError::service_unavailable("Billing integrity service is unavailable")
}
