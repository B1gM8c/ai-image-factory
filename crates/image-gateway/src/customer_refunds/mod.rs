use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    ImageGatewayError, admission::idempotency_key_digest, credit_grants::restore_credit_grants,
};

const IDEMPOTENCY_PROFILE: &str = "admin-billing-v1";
const IDEMPOTENCY_OPERATION: &str = "customer-refund";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CustomerRefundActor {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateCustomerRefundRequest {
    pub amount_micros: String,
    pub reason_code: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListCustomerChargesRequest {
    pub tenant_id: Option<String>,
    pub state: Option<String>,
    pub after: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CustomerChargeView {
    pub object: &'static str,
    pub transaction_id: String,
    pub job_id: String,
    pub tenant_id: String,
    pub currency: String,
    pub amount_micros: String,
    pub refunded_micros: String,
    pub remaining_refundable_micros: String,
    pub refund_state: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CustomerRefundView {
    pub object: &'static str,
    pub refund_id: String,
    pub original_transaction_id: String,
    pub refund_transaction_id: String,
    pub tenant_id: String,
    pub currency: String,
    pub amount_micros: String,
    pub grant_restored_micros: String,
    pub account_refunded_micros: String,
    pub refunded_total_micros: String,
    pub remaining_refundable_micros: String,
    pub reason_code: String,
    pub reason: String,
    pub actor_user_id: String,
    pub session_id: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CustomerChargeDetail {
    #[serde(flatten)]
    pub charge: CustomerChargeView,
    pub refunds: Vec<CustomerRefundView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct CustomerChargeList {
    pub object: &'static str,
    pub data: Vec<CustomerChargeView>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[async_trait]
pub trait CustomerRefundService: Send + Sync + 'static {
    async fn list_charges(
        &self,
        request: ListCustomerChargesRequest,
    ) -> Result<CustomerChargeList, ImageGatewayError>;

    async fn get_charge(
        &self,
        transaction_id: Uuid,
    ) -> Result<CustomerChargeDetail, ImageGatewayError>;

    async fn create_refund(
        &self,
        transaction_id: Uuid,
        idempotency_key: &str,
        actor: CustomerRefundActor,
        request: CreateCustomerRefundRequest,
    ) -> Result<CustomerRefundView, ImageGatewayError>;
}

#[derive(Clone)]
pub struct PostgresCustomerRefundService {
    pool: PgPool,
}

impl PostgresCustomerRefundService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CustomerRefundService for PostgresCustomerRefundService {
    async fn list_charges(
        &self,
        request: ListCustomerChargesRequest,
    ) -> Result<CustomerChargeList, ImageGatewayError> {
        let tenant_id = normalize_optional_tenant_id(request.tenant_id)?;
        let state = normalize_state(request.state.as_deref())?;
        let limit = request.limit.unwrap_or(25);
        if !(1..=100).contains(&limit) {
            return Err(ImageGatewayError::invalid_request(
                "limit must be between 1 and 100",
                Some("limit".to_string()),
                "invalid_limit",
            ));
        }
        let after = request.after.as_deref().map(parse_uuid).transpose()?;
        let after_created_at_ms = match after {
            Some(transaction_id) => Some(
                sqlx::query_scalar::<_, i64>(
                    r#"
                    SELECT created_at_ms
                    FROM ledger_transactions
                    WHERE transaction_id = $1
                      AND transaction_type IN ('customer_charge', 'customer_job_charge')
                    "#,
                )
                .bind(transaction_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(unavailable)?
                .ok_or_else(invalid_cursor)?,
            ),
            None => None,
        };
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ImageGatewayError::internal("customer charge page size overflow"))?;
        let mut rows = sqlx::query_as::<_, CustomerChargeRow>(
            r#"
            SELECT transaction.transaction_id,
                   transaction.source_job_id AS job_id,
                   account.owner_id AS tenant_id,
                   transaction.currency,
                   posting.amount_micros,
                   COALESCE(refunds.refunded_micros, 0)::BIGINT
                       AS refunded_micros,
                   transaction.created_at_ms
            FROM ledger_transactions transaction
            JOIN ledger_transaction_seals seal
              ON seal.transaction_id = transaction.transaction_id
            JOIN ledger_postings posting
              ON posting.transaction_id = transaction.transaction_id
            JOIN ledger_accounts account
              ON account.account_id = posting.account_id
             AND account.currency = posting.currency
             AND account.owner_type = 'tenant'
             AND account.account_type = 'receivable'
            LEFT JOIN LATERAL (
                SELECT SUM(refund.amount_micros)::BIGINT AS refunded_micros
                FROM customer_refunds refund
                WHERE refund.original_transaction_id =
                      transaction.transaction_id
            ) refunds ON TRUE
            WHERE transaction.transaction_type IN (
                    'customer_charge', 'customer_job_charge'
                  )
              AND posting.amount_micros > 0
              AND ($1::TEXT IS NULL OR account.owner_id = $1)
              AND (
                    $2 = 'all'
                    OR ($2 = 'refundable'
                        AND COALESCE(refunds.refunded_micros, 0)
                            < posting.amount_micros)
                    OR ($2 = 'partially_refunded'
                        AND COALESCE(refunds.refunded_micros, 0) > 0
                        AND COALESCE(refunds.refunded_micros, 0)
                            < posting.amount_micros)
                    OR ($2 = 'fully_refunded'
                        AND COALESCE(refunds.refunded_micros, 0)
                            = posting.amount_micros)
                  )
              AND (
                    $3::BIGINT IS NULL
                    OR (transaction.created_at_ms, transaction.transaction_id)
                        < ($3, $4)
                  )
            ORDER BY transaction.created_at_ms DESC,
                     transaction.transaction_id DESC
            LIMIT $5
            "#,
        )
        .bind(tenant_id.as_deref())
        .bind(state)
        .bind(after_created_at_ms)
        .bind(after)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_after = has_more
            .then(|| rows.last().map(|row| row.transaction_id.to_string()))
            .flatten();
        Ok(CustomerChargeList {
            object: "list",
            data: rows.into_iter().map(CustomerChargeRow::into_view).collect(),
            has_more,
            next_after,
        })
    }

    async fn get_charge(
        &self,
        transaction_id: Uuid,
    ) -> Result<CustomerChargeDetail, ImageGatewayError> {
        let charge = load_charge(&self.pool, transaction_id).await?;
        let rows = sqlx::query_as::<_, CustomerRefundRow>(
            r#"
            SELECT refund_id, original_transaction_id,
                   refund_transaction_id, tenant_id, currency,
                   amount_micros, grant_restored_micros,
                   account_refunded_micros, reason_code, reason,
                   idempotency_key_digest, request_hash,
                   actor_user_id, session_id, created_at_ms
            FROM customer_refunds
            WHERE original_transaction_id = $1
            ORDER BY created_at_ms, refund_id
            "#,
        )
        .bind(transaction_id)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let mut running_total = 0_i64;
        let refunds = rows
            .into_iter()
            .map(|row| {
                running_total = running_total.saturating_add(row.amount_micros);
                row.into_view(charge.amount_micros, running_total)
            })
            .collect();
        Ok(CustomerChargeDetail {
            charge: charge.into_view(),
            refunds,
        })
    }

    async fn create_refund(
        &self,
        transaction_id: Uuid,
        idempotency_key: &str,
        actor: CustomerRefundActor,
        request: CreateCustomerRefundRequest,
    ) -> Result<CustomerRefundView, ImageGatewayError> {
        let amount_micros = parse_positive_amount(&request.amount_micros)?;
        let reason_code = normalize_reason_code(request.reason_code)?;
        let reason = normalize_reason(request.reason)?;
        let idempotency_key_digest = idempotency_key_digest(
            &transaction_id.to_string(),
            IDEMPOTENCY_PROFILE,
            IDEMPOTENCY_OPERATION,
            idempotency_key,
        )
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
        let request_hash = request_hash(transaction_id, amount_micros, &reason_code, &reason)?;
        let now = now_ms()?;

        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        let source = load_source(&mut transaction, transaction_id).await?;
        let posting = load_source_posting(&mut transaction, transaction_id).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("budget:{}:{}", posting.tenant_id, source.currency))
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
        let locked_source = load_source_for_update(&mut transaction, transaction_id).await?;
        let locked_posting = load_source_posting(&mut transaction, transaction_id).await?;
        if locked_source != source || locked_posting != posting {
            return Err(ImageGatewayError::conflict(
                "The customer charge changed while refunding",
                None,
                "customer_charge_changed",
            ));
        }

        if let Some(existing) =
            load_idempotent_refund(&mut transaction, transaction_id, &idempotency_key_digest)
                .await?
        {
            if existing.request_hash != request_hash {
                return Err(ImageGatewayError::idempotency_conflict());
            }
            let refunded_total = refund_total(&mut transaction, transaction_id).await?;
            transaction.commit().await.map_err(unavailable)?;
            return Ok(existing.into_view(posting.amount_micros, refunded_total));
        }

        let refunded_before = refund_total(&mut transaction, transaction_id).await?;
        let remaining_before = posting
            .amount_micros
            .checked_sub(refunded_before)
            .ok_or_else(|| ImageGatewayError::internal("customer refund total is invalid"))?;
        if amount_micros > remaining_before {
            return Err(ImageGatewayError::conflict(
                "Refund amount exceeds the remaining refundable amount",
                Some("amount_micros".to_string()),
                "refund_amount_exceeds_remaining",
            ));
        }
        let funding =
            load_charge_funding(&mut transaction, source.source_job_id, &posting, &source).await?;
        let refunded_funding = refunded_funding_total(&mut transaction, transaction_id).await?;
        let account_remaining = funding
            .account_micros
            .checked_sub(refunded_funding.account_micros)
            .ok_or_else(|| ImageGatewayError::internal("account refund funding is invalid"))?;
        let grant_remaining = funding
            .grant_micros
            .checked_sub(refunded_funding.grant_micros)
            .ok_or_else(|| ImageGatewayError::internal("grant refund funding is invalid"))?;
        let account_refunded_micros = amount_micros.min(account_remaining);
        let grant_restored_micros = amount_micros
            .checked_sub(account_refunded_micros)
            .ok_or_else(|| ImageGatewayError::internal("refund funding split underflow"))?;
        if grant_restored_micros > grant_remaining {
            return Err(ImageGatewayError::conflict(
                "Refund funding exceeds the original charge funding",
                Some("amount_micros".to_string()),
                "refund_funding_exceeds_remaining",
            ));
        }

        sqlx::query(
            r#"
            SELECT 1
            FROM billing_accounts
            WHERE tenant_id = $1 AND currency = $2
            FOR UPDATE
            "#,
        )
        .bind(&posting.tenant_id)
        .bind(&source.currency)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::conflict(
                "The customer charge has no billing account",
                None,
                "billing_account_missing",
            )
        })?;

        let refund_id = Uuid::new_v4();
        let refund_transaction_id = Uuid::new_v4();
        let semantic_key = format!("customer-refund:v1:{transaction_id}:{idempotency_key_digest}");
        sqlx::query(
            r#"
            INSERT INTO ledger_transactions (
                transaction_id, semantic_key, transaction_type,
                currency, payload_hash, created_at_ms,
                reverses_transaction_id
            )
            VALUES ($1, $2, 'customer_refund', $3, $4, $5, $6)
            "#,
        )
        .bind(refund_transaction_id)
        .bind(semantic_key)
        .bind(&source.currency)
        .bind(&request_hash)
        .bind(now)
        .bind(transaction_id)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;
        for (posting_no, account_id, amount) in [
            (1_i16, posting.receivable_account_id, -amount_micros),
            (2_i16, posting.revenue_account_id, amount_micros),
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
            .bind(refund_transaction_id)
            .bind(posting_no)
            .bind(account_id)
            .bind(&source.currency)
            .bind(amount)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(mutation_error)?;
        }
        sqlx::query(
            "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, $2)",
        )
        .bind(refund_transaction_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;
        sqlx::query(
            r#"
            INSERT INTO customer_refunds (
                refund_id, original_transaction_id,
                refund_transaction_id, tenant_id, currency,
                amount_micros, grant_restored_micros,
                account_refunded_micros, reason_code, reason,
                idempotency_key_digest, request_hash,
                actor_user_id, session_id, created_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15
            )
            "#,
        )
        .bind(refund_id)
        .bind(transaction_id)
        .bind(refund_transaction_id)
        .bind(&posting.tenant_id)
        .bind(&source.currency)
        .bind(amount_micros)
        .bind(grant_restored_micros)
        .bind(account_refunded_micros)
        .bind(&reason_code)
        .bind(&reason)
        .bind(&idempotency_key_digest)
        .bind(&request_hash)
        .bind(actor.user_id)
        .bind(actor.session_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;
        let restored = restore_credit_grants(
            &mut transaction,
            source.source_job_id,
            refund_id,
            &posting.tenant_id,
            &source.currency,
            grant_restored_micros,
            now,
        )
        .await?;
        if restored != grant_restored_micros {
            return Err(ImageGatewayError::conflict(
                "Credit grant restoration does not match the refund",
                None,
                "credit_grant_restoration_conflict",
            ));
        }
        let updated_accounts = sqlx::query(
            r#"
            UPDATE billing_accounts
            SET refunded_micros = refunded_micros + $3,
                updated_at_ms = $4
            WHERE tenant_id = $1 AND currency = $2
            "#,
        )
        .bind(&posting.tenant_id)
        .bind(&source.currency)
        .bind(account_refunded_micros)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?
        .rows_affected();
        if updated_accounts != 1 {
            return Err(ImageGatewayError::conflict(
                "The customer charge has no billing account",
                None,
                "billing_account_missing",
            ));
        }
        insert_audit(
            &mut transaction,
            transaction_id,
            refund_id,
            refund_transaction_id,
            &posting.tenant_id,
            &source.currency,
            amount_micros,
            &reason_code,
            &reason,
            actor,
            now,
        )
        .await?;
        sqlx::query(
            r#"
            SET CONSTRAINTS
                ledger_transactions_balance_guard,
                ledger_postings_balance_guard,
                ledger_transaction_seals_balance_guard,
                ledger_transactions_require_customer_refund_evidence,
                customer_refunds_validate,
                customer_refunds_validate_account_total,
                billing_accounts_validate_refund_total,
                credit_grants_validate_events,
                credit_grant_events_validate_all,
                customer_refunds_validate_grant_split,
                ledger_transactions_validate_credit_grant
            IMMEDIATE
            "#,
        )
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;
        transaction.commit().await.map_err(unavailable)?;

        Ok(CustomerRefundView {
            object: "billing.customer_refund",
            refund_id: refund_id.to_string(),
            original_transaction_id: transaction_id.to_string(),
            refund_transaction_id: refund_transaction_id.to_string(),
            tenant_id: posting.tenant_id,
            currency: source.currency,
            amount_micros: amount_micros.to_string(),
            grant_restored_micros: grant_restored_micros.to_string(),
            account_refunded_micros: account_refunded_micros.to_string(),
            refunded_total_micros: refunded_before.saturating_add(amount_micros).to_string(),
            remaining_refundable_micros: remaining_before.saturating_sub(amount_micros).to_string(),
            reason_code,
            reason,
            actor_user_id: actor.user_id.to_string(),
            session_id: actor.session_id.to_string(),
            created_at_ms: now,
        })
    }
}

#[derive(Clone, Debug, Eq, FromRow, PartialEq)]
struct SourceTransactionRow {
    currency: String,
    source_job_id: Uuid,
}

#[derive(Clone, Debug, Eq, FromRow, PartialEq)]
struct SourcePostingRow {
    tenant_id: String,
    amount_micros: i64,
    receivable_account_id: Uuid,
    revenue_account_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RefundFunding {
    grant_micros: i64,
    account_micros: i64,
}

#[derive(Clone, Debug, FromRow)]
struct CustomerChargeRow {
    transaction_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    currency: String,
    amount_micros: i64,
    refunded_micros: i64,
    created_at_ms: i64,
}

impl CustomerChargeRow {
    fn into_view(self) -> CustomerChargeView {
        let remaining = self.amount_micros.saturating_sub(self.refunded_micros);
        CustomerChargeView {
            object: "billing.customer_charge",
            transaction_id: self.transaction_id.to_string(),
            job_id: self.job_id.to_string(),
            tenant_id: self.tenant_id,
            currency: self.currency,
            amount_micros: self.amount_micros.to_string(),
            refunded_micros: self.refunded_micros.to_string(),
            remaining_refundable_micros: remaining.to_string(),
            refund_state: refund_state(self.amount_micros, self.refunded_micros).to_string(),
            created_at_ms: self.created_at_ms,
        }
    }
}

#[derive(Clone, Debug, FromRow)]
struct CustomerRefundRow {
    refund_id: Uuid,
    original_transaction_id: Uuid,
    refund_transaction_id: Uuid,
    tenant_id: String,
    currency: String,
    amount_micros: i64,
    grant_restored_micros: i64,
    account_refunded_micros: i64,
    reason_code: String,
    reason: String,
    #[allow(dead_code)]
    idempotency_key_digest: String,
    request_hash: String,
    actor_user_id: Uuid,
    session_id: Uuid,
    created_at_ms: i64,
}

impl CustomerRefundRow {
    fn into_view(self, original_amount: i64, refunded_total: i64) -> CustomerRefundView {
        CustomerRefundView {
            object: "billing.customer_refund",
            refund_id: self.refund_id.to_string(),
            original_transaction_id: self.original_transaction_id.to_string(),
            refund_transaction_id: self.refund_transaction_id.to_string(),
            tenant_id: self.tenant_id,
            currency: self.currency,
            amount_micros: self.amount_micros.to_string(),
            grant_restored_micros: self.grant_restored_micros.to_string(),
            account_refunded_micros: self.account_refunded_micros.to_string(),
            refunded_total_micros: refunded_total.to_string(),
            remaining_refundable_micros: original_amount.saturating_sub(refunded_total).to_string(),
            reason_code: self.reason_code,
            reason: self.reason,
            actor_user_id: self.actor_user_id.to_string(),
            session_id: self.session_id.to_string(),
            created_at_ms: self.created_at_ms,
        }
    }
}

async fn load_source(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
) -> Result<SourceTransactionRow, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT currency, source_job_id
        FROM ledger_transactions
        WHERE transaction_id = $1
          AND transaction_type IN ('customer_charge', 'customer_job_charge')
          AND EXISTS (
              SELECT 1
              FROM ledger_transaction_seals seal
              WHERE seal.transaction_id = ledger_transactions.transaction_id
          )
        "#,
    )
    .bind(transaction_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or_else(charge_not_found)
}

async fn load_source_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
) -> Result<SourceTransactionRow, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT currency, source_job_id
        FROM ledger_transactions
        WHERE transaction_id = $1
          AND transaction_type IN ('customer_charge', 'customer_job_charge')
          AND EXISTS (
              SELECT 1
              FROM ledger_transaction_seals seal
              WHERE seal.transaction_id = ledger_transactions.transaction_id
          )
        FOR UPDATE
        "#,
    )
    .bind(transaction_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or_else(charge_not_found)
}

async fn load_source_posting(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
) -> Result<SourcePostingRow, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT receivable.owner_id AS tenant_id,
               receivable_posting.amount_micros,
               receivable.account_id AS receivable_account_id,
               revenue.account_id AS revenue_account_id
        FROM ledger_postings receivable_posting
        JOIN ledger_accounts receivable
          ON receivable.account_id = receivable_posting.account_id
         AND receivable.currency = receivable_posting.currency
         AND receivable.owner_type = 'tenant'
         AND receivable.account_type = 'receivable'
        JOIN ledger_postings revenue_posting
          ON revenue_posting.transaction_id =
             receivable_posting.transaction_id
         AND revenue_posting.posting_no <>
             receivable_posting.posting_no
         AND revenue_posting.amount_micros =
             -receivable_posting.amount_micros
        JOIN ledger_accounts revenue
          ON revenue.account_id = revenue_posting.account_id
         AND revenue.currency = revenue_posting.currency
         AND revenue.owner_type = 'platform'
         AND revenue.owner_id = 'platform'
         AND revenue.account_type = 'revenue'
        WHERE receivable_posting.transaction_id = $1
          AND receivable_posting.amount_micros > 0
        "#,
    )
    .bind(transaction_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or_else(|| {
        ImageGatewayError::conflict(
            "The customer charge has invalid ledger postings",
            None,
            "customer_charge_invalid",
        )
    })
}

async fn load_charge_funding(
    transaction: &mut Transaction<'_, Postgres>,
    source_job_id: Uuid,
    posting: &SourcePostingRow,
    source: &SourceTransactionRow,
) -> Result<RefundFunding, ImageGatewayError> {
    let funding: Option<(i64, i64)> = sqlx::query_as(
        r#"
        SELECT grant_captured_micros, account_captured_micros
        FROM customer_billing_holds
        WHERE job_id = $1
          AND tenant_id = $2
          AND currency = $3
          AND state = 'settled'
        FOR SHARE
        "#,
    )
    .bind(source_job_id)
    .bind(&posting.tenant_id)
    .bind(&source.currency)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?;
    let funding = match funding {
        Some((grant_micros, account_micros)) => RefundFunding {
            grant_micros,
            account_micros,
        },
        None => RefundFunding {
            grant_micros: 0,
            account_micros: posting.amount_micros,
        },
    };
    if funding.grant_micros.checked_add(funding.account_micros) != Some(posting.amount_micros) {
        return Err(ImageGatewayError::conflict(
            "The customer charge funding split is invalid",
            None,
            "customer_charge_funding_invalid",
        ));
    }
    Ok(funding)
}

async fn refunded_funding_total(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
) -> Result<RefundFunding, ImageGatewayError> {
    let (grant_micros, account_micros): (i64, i64) = sqlx::query_as(
        r#"
        SELECT COALESCE(SUM(grant_restored_micros::NUMERIC), 0)::BIGINT,
               COALESCE(SUM(account_refunded_micros::NUMERIC), 0)::BIGINT
        FROM customer_refunds
        WHERE original_transaction_id = $1
        "#,
    )
    .bind(transaction_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(RefundFunding {
        grant_micros,
        account_micros,
    })
}

async fn load_idempotent_refund(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
    digest: &str,
) -> Result<Option<CustomerRefundRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT refund_id, original_transaction_id,
               refund_transaction_id, tenant_id, currency,
               amount_micros, grant_restored_micros,
               account_refunded_micros, reason_code, reason,
               idempotency_key_digest, request_hash,
               actor_user_id, session_id, created_at_ms
        FROM customer_refunds
        WHERE original_transaction_id = $1
          AND idempotency_key_digest = $2
        "#,
    )
    .bind(transaction_id)
    .bind(digest)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)
}

async fn refund_total(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(SUM(amount_micros::NUMERIC), 0)::BIGINT
        FROM customer_refunds
        WHERE original_transaction_id = $1
        "#,
    )
    .bind(transaction_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unavailable)
}

async fn load_charge(
    pool: &PgPool,
    transaction_id: Uuid,
) -> Result<CustomerChargeRow, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT transaction.transaction_id,
               transaction.source_job_id AS job_id,
               account.owner_id AS tenant_id,
               transaction.currency,
               posting.amount_micros,
               COALESCE(refunds.refunded_micros, 0)::BIGINT
                   AS refunded_micros,
               transaction.created_at_ms
        FROM ledger_transactions transaction
        JOIN ledger_transaction_seals seal
          ON seal.transaction_id = transaction.transaction_id
        JOIN ledger_postings posting
          ON posting.transaction_id = transaction.transaction_id
        JOIN ledger_accounts account
          ON account.account_id = posting.account_id
         AND account.currency = posting.currency
         AND account.owner_type = 'tenant'
         AND account.account_type = 'receivable'
        LEFT JOIN LATERAL (
            SELECT SUM(refund.amount_micros)::BIGINT AS refunded_micros
            FROM customer_refunds refund
            WHERE refund.original_transaction_id =
                  transaction.transaction_id
        ) refunds ON TRUE
        WHERE transaction.transaction_id = $1
          AND transaction.transaction_type IN (
                'customer_charge', 'customer_job_charge'
              )
          AND posting.amount_micros > 0
        "#,
    )
    .bind(transaction_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)?
    .ok_or_else(charge_not_found)
}

#[allow(clippy::too_many_arguments)]
async fn insert_audit(
    transaction: &mut Transaction<'_, Postgres>,
    original_transaction_id: Uuid,
    refund_id: Uuid,
    refund_transaction_id: Uuid,
    tenant_id: &str,
    currency: &str,
    amount_micros: i64,
    reason_code: &str,
    reason: &str,
    actor: CustomerRefundActor,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events (
            event_id, actor_user_id, session_id, request_id,
            action, resource_type, resource_id, outcome,
            reason_code, metadata, created_at_ms
        )
        VALUES (
            $1, $2, $3, NULL,
            'billing.customer_refund.create',
            'ledger_transaction', $4, 'success',
            $5, $6, $7
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor.user_id)
    .bind(actor.session_id)
    .bind(original_transaction_id.to_string())
    .bind(reason_code)
    .bind(json!({
        "refund_id": refund_id,
        "refund_transaction_id": refund_transaction_id,
        "tenant_id": tenant_id,
        "currency": currency,
        "amount_micros": amount_micros.to_string(),
        "reason": reason,
    }))
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(mutation_error)?;
    Ok(())
}

fn refund_state(amount_micros: i64, refunded_micros: i64) -> &'static str {
    if refunded_micros <= 0 {
        "not_refunded"
    } else if refunded_micros >= amount_micros {
        "fully_refunded"
    } else {
        "partially_refunded"
    }
}

fn normalize_state(value: Option<&str>) -> Result<&str, ImageGatewayError> {
    let state = value.unwrap_or("all");
    match state {
        "all" | "refundable" | "partially_refunded" | "fully_refunded" => Ok(state),
        _ => Err(ImageGatewayError::invalid_request(
            "state is not supported",
            Some("state".to_string()),
            "invalid_state",
        )),
    }
}

fn normalize_optional_tenant_id(
    tenant_id: Option<String>,
) -> Result<Option<String>, ImageGatewayError> {
    tenant_id
        .map(|value| {
            let value = value.trim();
            if value.is_empty() || value.len() > 255 {
                return Err(ImageGatewayError::invalid_request(
                    "tenant_id must contain between 1 and 255 characters",
                    Some("tenant_id".to_string()),
                    "invalid_tenant_id",
                ));
            }
            Ok(value.to_string())
        })
        .transpose()
}

fn normalize_reason_code(reason_code: String) -> Result<String, ImageGatewayError> {
    let reason_code = reason_code.trim();
    match reason_code {
        "customer_request"
        | "service_failure"
        | "billing_correction"
        | "fraud_dispute"
        | "provider_refund_pass_through"
        | "other" => Ok(reason_code.to_string()),
        _ => Err(ImageGatewayError::invalid_request(
            "reason_code is not supported",
            Some("reason_code".to_string()),
            "invalid_refund_reason_code",
        )),
    }
}

fn normalize_reason(reason: String) -> Result<String, ImageGatewayError> {
    let reason = reason.trim();
    if !(3..=500).contains(&reason.chars().count()) {
        return Err(ImageGatewayError::invalid_request(
            "reason must contain between 3 and 500 characters",
            Some("reason".to_string()),
            "invalid_refund_reason",
        ));
    }
    Ok(reason.to_string())
}

fn parse_positive_amount(value: &str) -> Result<i64, ImageGatewayError> {
    let amount = value.parse::<i64>().map_err(|_| {
        ImageGatewayError::invalid_request(
            "amount_micros must be a positive integer string",
            Some("amount_micros".to_string()),
            "invalid_refund_amount",
        )
    })?;
    if amount <= 0 {
        return Err(ImageGatewayError::invalid_request(
            "amount_micros must be greater than zero",
            Some("amount_micros".to_string()),
            "invalid_refund_amount",
        ));
    }
    Ok(amount)
}

fn parse_uuid(value: &str) -> Result<Uuid, ImageGatewayError> {
    Uuid::parse_str(value).map_err(|_| invalid_cursor())
}

fn request_hash(
    transaction_id: Uuid,
    amount_micros: i64,
    reason_code: &str,
    reason: &str,
) -> Result<String, ImageGatewayError> {
    let bytes = serde_json::to_vec(&(
        "billing.customer_refund.v1",
        transaction_id,
        amount_micros,
        reason_code,
        reason,
    ))
    .map_err(|_| ImageGatewayError::internal("failed to hash customer refund request"))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn invalid_cursor() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "after must identify an existing customer charge",
        Some("after".to_string()),
        "invalid_cursor",
    )
}

fn charge_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Customer charge not found",
        Some("transaction_id".to_string()),
        "customer_charge_not_found",
    )
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

fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(%error, "customer refund store unavailable");
    ImageGatewayError::service_unavailable("Customer refund service is unavailable")
}

fn mutation_error(error: sqlx::Error) -> ImageGatewayError {
    if let sqlx::Error::Database(database) = &error
        && matches!(
            database.code().as_deref(),
            Some("23503" | "23505" | "23514" | "55000" | "P0001")
        )
    {
        tracing::error!(%error, "customer refund integrity violation");
        return ImageGatewayError::internal("Customer refund integrity validation failed");
    }
    unavailable(error)
}
