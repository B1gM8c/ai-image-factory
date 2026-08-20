use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::idempotency_key_digest,
    credit_grants::{
        CreateCreditGrantRequest, CreditGrantActor, CreditGrantList, CreditGrantService,
        CreditGrantSummary, CreditGrantView, ListCreditGrantsRequest, RevokeCreditGrantRequest,
    },
};

const IDEMPOTENCY_PROFILE: &str = "admin-billing-v1";
const ISSUE_OPERATION: &str = "issue";
const REVOKE_OPERATION: &str = "revoke";

#[derive(Clone)]
pub struct PostgresCreditGrantService {
    pool: PgPool,
}

impl PostgresCreditGrantService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn expire_due(&self, limit: u32) -> Result<u64, ImageGatewayError> {
        if !(1..=1_000).contains(&limit) {
            return Err(ImageGatewayError::invalid_request(
                "limit must be between 1 and 1000",
                Some("limit".to_string()),
                "invalid_limit",
            ));
        }
        let candidates = sqlx::query_as::<_, ExpiringGrantRow>(
            r#"
            SELECT grant_id, tenant_id, currency
            FROM credit_grants
            WHERE state = 'active'
              AND expires_at_ms <=
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
              AND reserved_micros = 0
              AND available_micros > 0
            ORDER BY expires_at_ms, grant_id
            LIMIT $1
            "#,
        )
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;

        let mut expired = 0_u64;
        for candidate in candidates {
            let mut transaction = self.pool.begin().await.map_err(unavailable)?;
            lock_wallet(&mut transaction, &candidate.tenant_id, &candidate.currency).await?;
            let now = database_now(&mut transaction).await?;
            let Some(locked) = load_grant_for_update(&mut transaction, candidate.grant_id).await?
            else {
                transaction.commit().await.map_err(unavailable)?;
                continue;
            };
            if locked.tenant_id != candidate.tenant_id
                || locked.currency != candidate.currency
                || locked.state != "active"
                || locked.expires_at_ms > now
                || locked.reserved_micros != 0
                || locked.available_micros <= 0
            {
                transaction.commit().await.map_err(unavailable)?;
                continue;
            }
            let amount_micros = locked.available_micros;
            let sequence = locked
                .control_version
                .checked_add(1)
                .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
            let changed = sqlx::query(
                r#"
                UPDATE credit_grants
                SET expired_micros = expired_micros + $2,
                    state = 'expired',
                    control_version = $3,
                    updated_at_ms = $4
                WHERE grant_id = $1
                  AND state = 'active'
                  AND control_version = $5
                  AND expires_at_ms <= $4
                  AND reserved_micros = 0
                  AND available_micros = $2
                "#,
            )
            .bind(locked.grant_id)
            .bind(amount_micros)
            .bind(sequence)
            .bind(now)
            .bind(locked.control_version)
            .execute(&mut *transaction)
            .await
            .map_err(mutation_error)?
            .rows_affected();
            if changed != 1 {
                transaction.commit().await.map_err(unavailable)?;
                continue;
            }
            let event_id = Uuid::new_v4();
            let payload_hash =
                terminal_event_hash("expired", locked.grant_id, sequence, amount_micros);
            insert_event(
                &mut transaction,
                GrantEvent {
                    event_id,
                    grant_id: locked.grant_id,
                    tenant_id: &locked.tenant_id,
                    currency: &locked.currency,
                    sequence,
                    event_type: "expired",
                    amount_micros,
                    reservation_id: None,
                    hold_id: None,
                    refund_id: None,
                    related_event_id: None,
                    payload_hash: &payload_hash,
                    occurred_at_ms: now,
                },
            )
            .await?;
            insert_grant_ledger_pair(
                &mut transaction,
                event_id,
                locked.grant_id,
                &locked.tenant_id,
                &locked.currency,
                amount_micros,
                "credit_grant_expired",
                &payload_hash,
                LedgerDirection::Retire,
                None,
                now,
            )
            .await?;
            validate_contracts(&mut transaction).await?;
            transaction.commit().await.map_err(unavailable)?;
            expired = expired.saturating_add(1);
        }
        Ok(expired)
    }
}

#[derive(Clone, Debug, FromRow)]
struct ExpiringGrantRow {
    grant_id: Uuid,
    tenant_id: String,
    currency: String,
}

#[async_trait]
impl CreditGrantService for PostgresCreditGrantService {
    async fn list(
        &self,
        request: ListCreditGrantsRequest,
    ) -> Result<CreditGrantList, ImageGatewayError> {
        let organization_id = normalize_optional_organization_id(request.organization_id)?;
        let currency = normalize_currency(request.currency.as_deref().unwrap_or("USD"))?;
        let state = normalize_state(request.state.as_deref())?;
        let after = request.after.as_deref().map(parse_cursor).transpose()?;
        let limit = request.limit.unwrap_or(25);
        if !(1..=100).contains(&limit) {
            return Err(ImageGatewayError::invalid_request(
                "limit must be between 1 and 100",
                Some("limit".to_string()),
                "invalid_limit",
            ));
        }
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ImageGatewayError::internal("credit grant page size overflow"))?;

        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
        let as_of_ms = database_now(&mut transaction).await?;
        let mut rows = sqlx::query_as::<_, CreditGrantRow>(
            r#"
            SELECT credit_grant.grant_id, credit_grant.semantic_key,
                   credit_grant.tenant_id,
                   organization.display_name AS organization_display_name,
                   credit_grant.currency, credit_grant.source_kind,
                   credit_grant.source_reference, credit_grant.received_at_ms,
                   credit_grant.expires_at_ms,
                   credit_grant.original_amount_micros,
                   credit_grant.reserved_micros,
                   credit_grant.consumed_micros,
                   credit_grant.restored_micros,
                   credit_grant.expired_micros,
                   credit_grant.revoked_micros,
                   credit_grant.available_micros,
                   credit_grant.state, credit_grant.control_version,
                   credit_grant.created_at_ms, credit_grant.updated_at_ms
            FROM credit_grants credit_grant
            JOIN identity_organizations organization
              ON organization.organization_id = credit_grant.tenant_id
            WHERE ($1::TEXT IS NULL OR credit_grant.tenant_id = $1)
              AND credit_grant.currency = $2
              AND (
                    $3 = 'all'
                    OR CASE
                         WHEN credit_grant.state = 'revoked' THEN 'revoked'
                         WHEN credit_grant.state = 'expired'
                           OR credit_grant.expires_at_ms <= $4
                           THEN 'expired'
                         WHEN credit_grant.available_micros = 0 THEN 'exhausted'
                         WHEN credit_grant.available_micros
                                = credit_grant.original_amount_micros
                           AND credit_grant.reserved_micros = 0
                           AND credit_grant.consumed_micros
                                = credit_grant.restored_micros
                           THEN 'active'
                         ELSE 'consuming'
                       END = $3
                  )
              AND (
                    $5::BIGINT IS NULL
                    OR (credit_grant.received_at_ms, credit_grant.grant_id)
                         < ($5, $6)
                  )
            ORDER BY credit_grant.received_at_ms DESC, credit_grant.grant_id DESC
            LIMIT $7
            "#,
        )
        .bind(organization_id.as_deref())
        .bind(&currency)
        .bind(&state)
        .bind(as_of_ms)
        .bind(after.as_ref().map(|cursor| cursor.received_at_ms))
        .bind(after.as_ref().map(|cursor| cursor.grant_id))
        .bind(fetch_limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(unavailable)?;
        let summary = load_summary(
            &mut transaction,
            organization_id.as_deref(),
            &currency,
            as_of_ms,
        )
        .await?;
        transaction.commit().await.map_err(unavailable)?;

        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_after = has_more
            .then(|| rows.last().map(CreditGrantRow::cursor))
            .flatten();
        Ok(CreditGrantList {
            object: "list",
            as_of_ms,
            organization_id,
            currency,
            summary,
            data: rows
                .into_iter()
                .map(|row| row.into_view(as_of_ms))
                .collect(),
            has_more,
            next_after,
        })
    }

    async fn get(&self, grant_id: Uuid) -> Result<CreditGrantView, ImageGatewayError> {
        if grant_id.is_nil() {
            return Err(grant_not_found());
        }
        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
        let now = database_now(&mut transaction).await?;
        let row = load_grant(&mut transaction, grant_id)
            .await?
            .ok_or_else(grant_not_found)?;
        transaction.commit().await.map_err(unavailable)?;
        Ok(row.into_view(now))
    }

    async fn create(
        &self,
        idempotency_key: &str,
        actor: CreditGrantActor,
        request: CreateCreditGrantRequest,
    ) -> Result<CreditGrantView, ImageGatewayError> {
        let tenant_id = normalize_organization_id(request.organization_id)?;
        let currency = normalize_currency(&request.currency)?;
        let amount_micros = parse_positive_amount(&request.amount_micros)?;
        let source_reference = normalize_source_reference(request.source_reference)?;
        let reason = normalize_reason(request.reason)?;
        let idempotency_key_digest = idempotency_key_digest(
            &tenant_id,
            IDEMPOTENCY_PROFILE,
            ISSUE_OPERATION,
            idempotency_key,
        )
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
        let request_hash = hash_request(
            ISSUE_OPERATION,
            json!({
                "tenant_id": tenant_id,
                "currency": currency,
                "amount_micros": amount_micros.to_string(),
                "expires_at_ms": request.expires_at_ms,
                "source_kind": "promotional",
                "source_reference": source_reference,
                "reason": reason,
            }),
        )?;

        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        lock_wallet(&mut transaction, &tenant_id, &currency).await?;
        let now = database_now(&mut transaction).await?;
        if request.expires_at_ms <= now {
            return Err(ImageGatewayError::invalid_request(
                "expires_at_ms must be in the future",
                Some("expires_at_ms".to_string()),
                "invalid_credit_grant_expiration",
            ));
        }
        if let Some(existing) = load_operation(
            &mut transaction,
            &tenant_id,
            ISSUE_OPERATION,
            &idempotency_key_digest,
        )
        .await?
        {
            if existing.request_hash != request_hash {
                return Err(ImageGatewayError::idempotency_conflict());
            }
            let row = load_grant_for_update(&mut transaction, existing.grant_id)
                .await?
                .ok_or_else(grant_not_found)?;
            transaction.commit().await.map_err(unavailable)?;
            return Ok(row.into_view(now));
        }

        require_organization(&mut transaction, &tenant_id).await?;
        ensure_billing_account(&mut transaction, &tenant_id, &currency, now).await?;

        let grant_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let semantic_key = format!(
            "credit-grant:v1:{tenant_id}:{currency}:{}",
            hex::encode(Sha256::digest(source_reference.as_bytes()))
        );
        sqlx::query(
            r#"
            INSERT INTO credit_grants (
                grant_id, semantic_key, tenant_id, currency,
                source_kind, source_reference, received_at_ms,
                expires_at_ms, original_amount_micros,
                state, control_version, created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, $3, $4, 'promotional', $5, $6, $7, $8,
                'active', 1, $6, $6
            )
            "#,
        )
        .bind(grant_id)
        .bind(semantic_key)
        .bind(&tenant_id)
        .bind(&currency)
        .bind(&source_reference)
        .bind(now)
        .bind(request.expires_at_ms)
        .bind(amount_micros)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;
        insert_event(
            &mut transaction,
            GrantEvent {
                event_id,
                grant_id,
                tenant_id: &tenant_id,
                currency: &currency,
                sequence: 1,
                event_type: "issued",
                amount_micros,
                reservation_id: None,
                hold_id: None,
                refund_id: None,
                related_event_id: None,
                payload_hash: &request_hash,
                occurred_at_ms: now,
            },
        )
        .await?;
        insert_grant_ledger_pair(
            &mut transaction,
            event_id,
            grant_id,
            &tenant_id,
            &currency,
            amount_micros,
            "credit_grant_issued",
            &request_hash,
            LedgerDirection::Issue,
            None,
            now,
        )
        .await?;
        insert_operation(
            &mut transaction,
            GrantOperation {
                grant_id,
                event_id,
                tenant_id: &tenant_id,
                currency: &currency,
                operation: ISSUE_OPERATION,
                idempotency_key_digest: &idempotency_key_digest,
                request_hash: &request_hash,
                actor,
                reason: &reason,
                now,
            },
        )
        .await?;
        insert_audit(
            &mut transaction,
            "billing.credit_grant.issue",
            grant_id,
            &tenant_id,
            &currency,
            amount_micros,
            actor,
            &reason,
            now,
        )
        .await?;
        validate_contracts(&mut transaction).await?;
        let row = load_grant_for_update(&mut transaction, grant_id)
            .await?
            .ok_or_else(grant_not_found)?;
        transaction.commit().await.map_err(unavailable)?;
        Ok(row.into_view(now))
    }

    async fn revoke(
        &self,
        grant_id: Uuid,
        idempotency_key: &str,
        actor: CreditGrantActor,
        request: RevokeCreditGrantRequest,
    ) -> Result<CreditGrantView, ImageGatewayError> {
        if grant_id.is_nil() {
            return Err(grant_not_found());
        }
        let reason = normalize_reason(request.reason)?;
        let preflight = load_grant_from_pool(&self.pool, grant_id)
            .await?
            .ok_or_else(grant_not_found)?;
        let idempotency_key_digest = idempotency_key_digest(
            &grant_id.to_string(),
            IDEMPOTENCY_PROFILE,
            REVOKE_OPERATION,
            idempotency_key,
        )
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
        let request_hash = hash_request(
            REVOKE_OPERATION,
            json!({"grant_id": grant_id, "reason": reason}),
        )?;

        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        lock_wallet(&mut transaction, &preflight.tenant_id, &preflight.currency).await?;
        let now = database_now(&mut transaction).await?;
        if let Some(existing) = load_operation(
            &mut transaction,
            &preflight.tenant_id,
            REVOKE_OPERATION,
            &idempotency_key_digest,
        )
        .await?
        {
            if existing.request_hash != request_hash || existing.grant_id != grant_id {
                return Err(ImageGatewayError::idempotency_conflict());
            }
            let row = load_grant_for_update(&mut transaction, grant_id)
                .await?
                .ok_or_else(grant_not_found)?;
            transaction.commit().await.map_err(unavailable)?;
            return Ok(row.into_view(now));
        }

        let locked = load_grant_for_update(&mut transaction, grant_id)
            .await?
            .ok_or_else(grant_not_found)?;
        if locked.tenant_id != preflight.tenant_id || locked.currency != preflight.currency {
            return Err(ImageGatewayError::conflict(
                "Credit grant wallet changed while revoking",
                None,
                "credit_grant_wallet_conflict",
            ));
        }
        if locked.state != "active" || locked.expires_at_ms <= now {
            return Err(ImageGatewayError::conflict(
                "Credit grant is no longer revocable",
                None,
                "credit_grant_not_revocable",
            ));
        }
        let open_reservations: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM customer_billing_hold_grant_reservations
                WHERE grant_id = $1 AND state = 'reserved'
            )
            "#,
        )
        .bind(grant_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(unavailable)?;
        if open_reservations {
            return Err(ImageGatewayError::conflict(
                "Credit grant has active billing holds",
                None,
                "credit_grant_has_active_holds",
            ));
        }
        let amount_micros = locked.available_micros;
        if amount_micros <= 0 {
            return Err(ImageGatewayError::conflict(
                "Credit grant has no available balance to revoke",
                None,
                "credit_grant_not_revocable",
            ));
        }
        let sequence = locked
            .control_version
            .checked_add(1)
            .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
        let changed = sqlx::query(
            r#"
            UPDATE credit_grants
            SET revoked_micros = revoked_micros + $2,
                state = 'revoked',
                control_version = $3,
                updated_at_ms = $4
            WHERE grant_id = $1
              AND state = 'active'
              AND control_version = $5
              AND available_micros = $2
            "#,
        )
        .bind(grant_id)
        .bind(amount_micros)
        .bind(sequence)
        .bind(now)
        .bind(locked.control_version)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?
        .rows_affected();
        if changed != 1 {
            return Err(ImageGatewayError::conflict(
                "Credit grant changed while revoking",
                None,
                "credit_grant_version_conflict",
            ));
        }

        let event_id = Uuid::new_v4();
        insert_event(
            &mut transaction,
            GrantEvent {
                event_id,
                grant_id,
                tenant_id: &locked.tenant_id,
                currency: &locked.currency,
                sequence,
                event_type: "revoked",
                amount_micros,
                reservation_id: None,
                hold_id: None,
                refund_id: None,
                related_event_id: None,
                payload_hash: &request_hash,
                occurred_at_ms: now,
            },
        )
        .await?;
        insert_grant_ledger_pair(
            &mut transaction,
            event_id,
            grant_id,
            &locked.tenant_id,
            &locked.currency,
            amount_micros,
            "credit_grant_revoked",
            &request_hash,
            LedgerDirection::Retire,
            None,
            now,
        )
        .await?;
        insert_operation(
            &mut transaction,
            GrantOperation {
                grant_id,
                event_id,
                tenant_id: &locked.tenant_id,
                currency: &locked.currency,
                operation: REVOKE_OPERATION,
                idempotency_key_digest: &idempotency_key_digest,
                request_hash: &request_hash,
                actor,
                reason: &reason,
                now,
            },
        )
        .await?;
        insert_audit(
            &mut transaction,
            "billing.credit_grant.revoke",
            grant_id,
            &locked.tenant_id,
            &locked.currency,
            amount_micros,
            actor,
            &reason,
            now,
        )
        .await?;
        validate_contracts(&mut transaction).await?;
        let row = load_grant_for_update(&mut transaction, grant_id)
            .await?
            .ok_or_else(grant_not_found)?;
        transaction.commit().await.map_err(unavailable)?;
        Ok(row.into_view(now))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CreditGrantFundingSplit {
    pub grant_micros: i64,
    pub account_micros: i64,
}

#[derive(Clone, Debug, FromRow)]
struct ReservableGrantRow {
    grant_id: Uuid,
    control_version: i64,
    available_micros: i64,
}

pub(crate) async fn reserve_credit_grants(
    transaction: &mut Transaction<'_, Postgres>,
    hold_id: Uuid,
    tenant_id: &str,
    currency: &str,
    requested_micros: i64,
    now: i64,
) -> Result<CreditGrantFundingSplit, ImageGatewayError> {
    if hold_id.is_nil() || tenant_id.trim().is_empty() || requested_micros < 0 {
        return Err(ImageGatewayError::conflict(
            "Credit grant reservation input is invalid",
            None,
            "credit_grant_reservation_conflict",
        ));
    }
    if requested_micros == 0 {
        return Ok(CreditGrantFundingSplit {
            grant_micros: 0,
            account_micros: 0,
        });
    }
    let grants = sqlx::query_as::<_, ReservableGrantRow>(
        r#"
        SELECT grant_id, control_version, available_micros
        FROM credit_grants
        WHERE tenant_id = $1
          AND currency = $2
          AND source_kind = 'promotional'
          AND state = 'active'
          AND expires_at_ms > $3
          AND available_micros > 0
        ORDER BY expires_at_ms, grant_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(currency)
    .bind(now)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;

    let mut remaining = requested_micros;
    let mut grant_micros = 0_i64;
    for grant in grants {
        if remaining == 0 {
            break;
        }
        let allocation = remaining.min(grant.available_micros);
        if allocation <= 0 {
            continue;
        }
        let sequence = grant
            .control_version
            .checked_add(1)
            .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
        let changed = sqlx::query(
            r#"
            UPDATE credit_grants
            SET reserved_micros = reserved_micros + $2,
                control_version = $3,
                updated_at_ms = $4
            WHERE grant_id = $1
              AND state = 'active'
              AND control_version = $5
              AND expires_at_ms > $4
              AND available_micros >= $2
            "#,
        )
        .bind(grant.grant_id)
        .bind(allocation)
        .bind(sequence)
        .bind(now)
        .bind(grant.control_version)
        .execute(&mut **transaction)
        .await
        .map_err(mutation_error)?
        .rows_affected();
        if changed != 1 {
            return Err(ImageGatewayError::conflict(
                "Credit grant changed while reserving",
                None,
                "credit_grant_reservation_conflict",
            ));
        }
        let reservation_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO customer_billing_hold_grant_reservations (
                grant_reservation_id, hold_id, grant_id,
                tenant_id, currency, reserved_micros,
                state, created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                'reserved', $7, $7
            )
            "#,
        )
        .bind(reservation_id)
        .bind(hold_id)
        .bind(grant.grant_id)
        .bind(tenant_id)
        .bind(currency)
        .bind(allocation)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(mutation_error)?;
        let payload_hash = funding_event_hash(
            "reserved",
            grant.grant_id,
            reservation_id,
            hold_id,
            allocation,
        );
        insert_event(
            transaction,
            GrantEvent {
                event_id: Uuid::new_v4(),
                grant_id: grant.grant_id,
                tenant_id,
                currency,
                sequence,
                event_type: "reserved",
                amount_micros: allocation,
                reservation_id: Some(reservation_id),
                hold_id: Some(hold_id),
                refund_id: None,
                related_event_id: None,
                payload_hash: &payload_hash,
                occurred_at_ms: now,
            },
        )
        .await?;
        grant_micros = grant_micros
            .checked_add(allocation)
            .ok_or_else(|| ImageGatewayError::internal("credit grant reservation overflow"))?;
        remaining = remaining
            .checked_sub(allocation)
            .ok_or_else(|| ImageGatewayError::internal("credit grant reservation underflow"))?;
    }
    Ok(CreditGrantFundingSplit {
        grant_micros,
        account_micros: remaining,
    })
}

#[derive(Clone, Debug, FromRow)]
struct GrantReservationRow {
    reservation_id: Uuid,
    grant_id: Uuid,
    reserved_micros: i64,
    grant_control_version: i64,
}

pub(crate) async fn settle_credit_grant_reservations(
    transaction: &mut Transaction<'_, Postgres>,
    hold_id: Uuid,
    tenant_id: &str,
    currency: &str,
    expected_grant_held_micros: i64,
    total_amount_micros: i64,
    now: i64,
) -> Result<CreditGrantFundingSplit, ImageGatewayError> {
    if expected_grant_held_micros < 0 || total_amount_micros < 0 {
        return Err(ImageGatewayError::conflict(
            "Credit grant settlement input is invalid",
            None,
            "credit_grant_settlement_conflict",
        ));
    }
    let reservations = sqlx::query_as::<_, GrantReservationRow>(
        r#"
        SELECT reservation.grant_reservation_id AS reservation_id,
               reservation.grant_id,
               reservation.reserved_micros,
               grant_row.control_version AS grant_control_version
        FROM customer_billing_hold_grant_reservations reservation
        JOIN credit_grants grant_row
          ON grant_row.grant_id = reservation.grant_id
         AND grant_row.tenant_id = reservation.tenant_id
         AND grant_row.currency = reservation.currency
        WHERE reservation.hold_id = $1
          AND reservation.tenant_id = $2
          AND reservation.currency = $3
          AND reservation.state = 'reserved'
        ORDER BY grant_row.expires_at_ms, grant_row.grant_id
        FOR UPDATE OF reservation, grant_row
        "#,
    )
    .bind(hold_id)
    .bind(tenant_id)
    .bind(currency)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    let reserved_total = reservations
        .iter()
        .try_fold(0_i64, |total, row| total.checked_add(row.reserved_micros));
    if reserved_total != Some(expected_grant_held_micros) {
        return Err(ImageGatewayError::conflict(
            "Credit grant reservations do not match the billing hold",
            None,
            "credit_grant_settlement_conflict",
        ));
    }

    let target_grant_capture = expected_grant_held_micros.min(total_amount_micros);
    let mut remaining_capture = target_grant_capture;
    for reservation in reservations {
        let consumed = remaining_capture.min(reservation.reserved_micros);
        let released = reservation
            .reserved_micros
            .checked_sub(consumed)
            .ok_or_else(|| ImageGatewayError::internal("credit grant settlement underflow"))?;
        let mut sequence = reservation.grant_control_version;
        if consumed > 0 {
            let next_sequence = sequence
                .checked_add(1)
                .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
            let changed = sqlx::query(
                r#"
                UPDATE credit_grants
                SET reserved_micros = reserved_micros - $2,
                    consumed_micros = consumed_micros + $2,
                    control_version = $3,
                    updated_at_ms = $4
                WHERE grant_id = $1
                  AND control_version = $5
                  AND reserved_micros >= $2
                "#,
            )
            .bind(reservation.grant_id)
            .bind(consumed)
            .bind(next_sequence)
            .bind(now)
            .bind(sequence)
            .execute(&mut **transaction)
            .await
            .map_err(mutation_error)?
            .rows_affected();
            if changed != 1 {
                return Err(ImageGatewayError::conflict(
                    "Credit grant changed while settling",
                    None,
                    "credit_grant_settlement_conflict",
                ));
            }
            sequence = next_sequence;
        }
        if released > 0 {
            let next_sequence = sequence
                .checked_add(1)
                .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
            let changed = sqlx::query(
                r#"
                UPDATE credit_grants
                SET reserved_micros = reserved_micros - $2,
                    control_version = $3,
                    updated_at_ms = $4
                WHERE grant_id = $1
                  AND control_version = $5
                  AND reserved_micros >= $2
                "#,
            )
            .bind(reservation.grant_id)
            .bind(released)
            .bind(next_sequence)
            .bind(now)
            .bind(sequence)
            .execute(&mut **transaction)
            .await
            .map_err(mutation_error)?
            .rows_affected();
            if changed != 1 {
                return Err(ImageGatewayError::conflict(
                    "Credit grant changed while settling",
                    None,
                    "credit_grant_settlement_conflict",
                ));
            }
            sequence = next_sequence;
        }
        let reservation_state = if consumed > 0 { "consumed" } else { "released" };
        let changed = sqlx::query(
            r#"
            UPDATE customer_billing_hold_grant_reservations
            SET consumed_micros = $2,
                released_micros = $3,
                state = $4,
                updated_at_ms = $5
            WHERE grant_reservation_id = $1
              AND state = 'reserved'
            "#,
        )
        .bind(reservation.reservation_id)
        .bind(consumed)
        .bind(released)
        .bind(reservation_state)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(mutation_error)?
        .rows_affected();
        if changed != 1 {
            return Err(ImageGatewayError::conflict(
                "Credit grant reservation changed while settling",
                None,
                "credit_grant_settlement_conflict",
            ));
        }

        let final_sequence = sequence;
        sequence = reservation.grant_control_version;
        if consumed > 0 {
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
            let event_id = Uuid::new_v4();
            let payload_hash = funding_event_hash(
                "consumed",
                reservation.grant_id,
                reservation.reservation_id,
                hold_id,
                consumed,
            );
            insert_event(
                transaction,
                GrantEvent {
                    event_id,
                    grant_id: reservation.grant_id,
                    tenant_id,
                    currency,
                    sequence,
                    event_type: "consumed",
                    amount_micros: consumed,
                    reservation_id: Some(reservation.reservation_id),
                    hold_id: Some(hold_id),
                    refund_id: None,
                    related_event_id: None,
                    payload_hash: &payload_hash,
                    occurred_at_ms: now,
                },
            )
            .await?;
            insert_grant_ledger_pair(
                transaction,
                event_id,
                reservation.grant_id,
                tenant_id,
                currency,
                consumed,
                "credit_grant_consumed",
                &payload_hash,
                LedgerDirection::Consume,
                None,
                now,
            )
            .await?;
        }
        if released > 0 {
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
            let payload_hash = funding_event_hash(
                "released",
                reservation.grant_id,
                reservation.reservation_id,
                hold_id,
                released,
            );
            insert_event(
                transaction,
                GrantEvent {
                    event_id: Uuid::new_v4(),
                    grant_id: reservation.grant_id,
                    tenant_id,
                    currency,
                    sequence,
                    event_type: "released",
                    amount_micros: released,
                    reservation_id: Some(reservation.reservation_id),
                    hold_id: Some(hold_id),
                    refund_id: None,
                    related_event_id: None,
                    payload_hash: &payload_hash,
                    occurred_at_ms: now,
                },
            )
            .await?;
        }
        if sequence != final_sequence {
            return Err(ImageGatewayError::internal(
                "credit grant event sequence is invalid",
            ));
        }
        remaining_capture = remaining_capture
            .checked_sub(consumed)
            .ok_or_else(|| ImageGatewayError::internal("credit grant settlement underflow"))?;
    }
    if remaining_capture != 0 {
        return Err(ImageGatewayError::conflict(
            "Credit grant capture does not match reservations",
            None,
            "credit_grant_settlement_conflict",
        ));
    }
    Ok(CreditGrantFundingSplit {
        grant_micros: target_grant_capture,
        account_micros: total_amount_micros
            .checked_sub(target_grant_capture)
            .ok_or_else(|| ImageGatewayError::internal("credit grant settlement underflow"))?,
    })
}

#[derive(Clone, Debug, FromRow)]
struct RestorableConsumptionRow {
    consumption_event_id: Uuid,
    grant_id: Uuid,
    tenant_id: String,
    currency: String,
    consumed_micros: i64,
    restored_micros: i64,
    expires_at_ms: i64,
    state: String,
    control_version: i64,
    consumption_transaction_id: Uuid,
}

pub(crate) async fn restore_credit_grants(
    transaction: &mut Transaction<'_, Postgres>,
    source_job_id: Uuid,
    refund_id: Uuid,
    tenant_id: &str,
    currency: &str,
    requested_micros: i64,
    now: i64,
) -> Result<i64, ImageGatewayError> {
    if requested_micros < 0 {
        return Err(ImageGatewayError::conflict(
            "Credit grant restoration input is invalid",
            None,
            "credit_grant_restoration_conflict",
        ));
    }
    if requested_micros == 0 {
        return Ok(0);
    }
    let consumptions = sqlx::query_as::<_, RestorableConsumptionRow>(
        r#"
        SELECT consumption.grant_event_id AS consumption_event_id,
               consumption.grant_id,
               consumption.tenant_id,
               consumption.currency,
               consumption.amount_micros AS consumed_micros,
               COALESCE(SUM(restoration.amount_micros), 0)::BIGINT
                   AS restored_micros,
               grant_row.expires_at_ms,
               grant_row.state,
               grant_row.control_version,
               ledger.transaction_id AS consumption_transaction_id
        FROM credit_grant_events consumption
        JOIN customer_billing_hold_grant_reservations reservation
          ON reservation.grant_reservation_id =
             consumption.grant_reservation_id
        JOIN customer_billing_holds hold
          ON hold.hold_id = reservation.hold_id
        JOIN credit_grants grant_row
          ON grant_row.grant_id = consumption.grant_id
        JOIN ledger_transactions ledger
          ON ledger.source_credit_grant_event_id =
             consumption.grant_event_id
         AND ledger.transaction_type = 'credit_grant_consumed'
        LEFT JOIN credit_grant_events restoration
          ON restoration.related_grant_event_id =
             consumption.grant_event_id
         AND restoration.event_type IN (
             'restored_available', 'restored_expired'
         )
        WHERE consumption.event_type = 'consumed'
          AND hold.job_id = $1
          AND consumption.tenant_id = $2
          AND consumption.currency = $3
        GROUP BY consumption.grant_event_id, consumption.grant_id,
                 consumption.tenant_id, consumption.currency,
                 consumption.amount_micros, consumption.event_sequence,
                 grant_row.expires_at_ms, grant_row.state,
                 grant_row.control_version,
                 ledger.transaction_id
        HAVING consumption.amount_micros >
               COALESCE(SUM(restoration.amount_micros), 0)
        ORDER BY grant_row.expires_at_ms DESC,
                 consumption.event_sequence DESC,
                 consumption.grant_event_id DESC
        "#,
    )
    .bind(source_job_id)
    .bind(tenant_id)
    .bind(currency)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;

    let mut remaining = requested_micros;
    let mut restored_total = 0_i64;
    for consumption in consumptions {
        if remaining == 0 {
            break;
        }
        if consumption.tenant_id != tenant_id || consumption.currency != currency {
            return Err(ImageGatewayError::conflict(
                "Credit grant restoration wallet does not match",
                None,
                "credit_grant_restoration_conflict",
            ));
        }
        let restorable = consumption
            .consumed_micros
            .checked_sub(consumption.restored_micros)
            .ok_or_else(|| ImageGatewayError::internal("credit grant restoration underflow"))?;
        let amount = remaining.min(restorable);
        if amount <= 0 {
            continue;
        }
        let available = consumption.state == "active" && consumption.expires_at_ms > now;
        let sequence = consumption
            .control_version
            .checked_add(1)
            .ok_or_else(|| ImageGatewayError::internal("credit grant version overflow"))?;
        let expired_increment = if available { 0 } else { amount };
        let changed = sqlx::query(
            r#"
            UPDATE credit_grants
            SET restored_micros = restored_micros + $2,
                expired_micros = expired_micros + $3,
                control_version = $4,
                updated_at_ms = $5
            WHERE grant_id = $1
              AND control_version = $6
            "#,
        )
        .bind(consumption.grant_id)
        .bind(amount)
        .bind(expired_increment)
        .bind(sequence)
        .bind(now)
        .bind(consumption.control_version)
        .execute(&mut **transaction)
        .await
        .map_err(mutation_error)?
        .rows_affected();
        if changed != 1 {
            return Err(ImageGatewayError::conflict(
                "Credit grant changed while restoring",
                None,
                "credit_grant_restoration_conflict",
            ));
        }
        let event_type = if available {
            "restored_available"
        } else {
            "restored_expired"
        };
        let event_id = Uuid::new_v4();
        let payload_hash = restoration_event_hash(
            event_type,
            consumption.grant_id,
            consumption.consumption_event_id,
            refund_id,
            amount,
        );
        insert_event(
            transaction,
            GrantEvent {
                event_id,
                grant_id: consumption.grant_id,
                tenant_id,
                currency,
                sequence,
                event_type,
                amount_micros: amount,
                reservation_id: None,
                hold_id: None,
                refund_id: Some(refund_id),
                related_event_id: Some(consumption.consumption_event_id),
                payload_hash: &payload_hash,
                occurred_at_ms: now,
            },
        )
        .await?;
        insert_grant_ledger_pair(
            transaction,
            event_id,
            consumption.grant_id,
            tenant_id,
            currency,
            amount,
            "credit_grant_restored",
            &payload_hash,
            LedgerDirection::Restore,
            Some(consumption.consumption_transaction_id),
            now,
        )
        .await?;
        restored_total = restored_total
            .checked_add(amount)
            .ok_or_else(|| ImageGatewayError::internal("credit grant restoration overflow"))?;
        remaining = remaining
            .checked_sub(amount)
            .ok_or_else(|| ImageGatewayError::internal("credit grant restoration underflow"))?;
    }
    Ok(restored_total)
}

#[derive(Clone, Debug, FromRow)]
struct CreditGrantRow {
    grant_id: Uuid,
    #[allow(dead_code)]
    semantic_key: String,
    tenant_id: String,
    #[sqlx(default)]
    organization_display_name: Option<String>,
    currency: String,
    source_kind: String,
    source_reference: String,
    received_at_ms: i64,
    expires_at_ms: i64,
    original_amount_micros: i64,
    reserved_micros: i64,
    consumed_micros: i64,
    restored_micros: i64,
    expired_micros: i64,
    revoked_micros: i64,
    available_micros: i64,
    state: String,
    control_version: i64,
    #[allow(dead_code)]
    created_at_ms: i64,
    #[allow(dead_code)]
    updated_at_ms: i64,
}

impl CreditGrantRow {
    fn effective_available(&self, as_of_ms: i64) -> i64 {
        if self.state == "active" && self.expires_at_ms > as_of_ms {
            self.available_micros
        } else {
            0
        }
    }

    fn effective_expired(&self, as_of_ms: i64) -> i64 {
        if self.state != "revoked" && self.expires_at_ms <= as_of_ms {
            self.expired_micros.saturating_add(self.available_micros)
        } else {
            self.expired_micros
        }
    }

    fn display_state(&self, as_of_ms: i64) -> &'static str {
        if self.state == "revoked" {
            "revoked"
        } else if self.state == "expired" || self.expires_at_ms <= as_of_ms {
            "expired"
        } else if self.available_micros == 0 {
            "exhausted"
        } else if self.available_micros == self.original_amount_micros
            && self.reserved_micros == 0
            && self.consumed_micros == self.restored_micros
        {
            "active"
        } else {
            "consuming"
        }
    }

    fn into_view(self, as_of_ms: i64) -> CreditGrantView {
        let available_micros = self.effective_available(as_of_ms);
        let expired_micros = self.effective_expired(as_of_ms);
        let state = self.display_state(as_of_ms);
        CreditGrantView {
            object: "billing.credit_grant",
            grant_id: self.grant_id.to_string(),
            organization_id: self.tenant_id,
            organization_display_name: self.organization_display_name,
            currency: self.currency,
            source_kind: self.source_kind,
            source_reference: self.source_reference,
            original_amount_micros: self.original_amount_micros.to_string(),
            available_micros: available_micros.to_string(),
            reserved_micros: self.reserved_micros.to_string(),
            consumed_micros: self.consumed_micros.to_string(),
            restored_micros: self.restored_micros.to_string(),
            expired_micros: expired_micros.to_string(),
            revoked_micros: self.revoked_micros.to_string(),
            state: state.to_string(),
            received_at_ms: self.received_at_ms,
            expires_at_ms: self.expires_at_ms,
        }
    }

    fn cursor(&self) -> String {
        format!("{}:{}", self.received_at_ms, self.grant_id)
    }
}

#[derive(Clone, Debug)]
struct CreditGrantCursor {
    received_at_ms: i64,
    grant_id: Uuid,
}

#[derive(Clone, Debug, FromRow)]
struct CreditGrantOperationRow {
    grant_id: Uuid,
    request_hash: String,
}

struct GrantEvent<'a> {
    event_id: Uuid,
    grant_id: Uuid,
    tenant_id: &'a str,
    currency: &'a str,
    sequence: i64,
    event_type: &'a str,
    amount_micros: i64,
    reservation_id: Option<Uuid>,
    hold_id: Option<Uuid>,
    refund_id: Option<Uuid>,
    related_event_id: Option<Uuid>,
    payload_hash: &'a str,
    occurred_at_ms: i64,
}

struct GrantOperation<'a> {
    grant_id: Uuid,
    event_id: Uuid,
    tenant_id: &'a str,
    currency: &'a str,
    operation: &'a str,
    idempotency_key_digest: &'a str,
    request_hash: &'a str,
    actor: CreditGrantActor,
    reason: &'a str,
    now: i64,
}

enum LedgerDirection {
    Issue,
    Consume,
    Restore,
    Retire,
}

async fn load_summary(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Option<&str>,
    currency: &str,
    as_of_ms: i64,
) -> Result<CreditGrantSummary, ImageGatewayError> {
    let row: (i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT COALESCE(SUM(original_amount_micros), 0)::BIGINT,
               COALESCE(SUM(
                 CASE
                   WHEN state = 'active' AND expires_at_ms > $3
                   THEN available_micros
                   ELSE 0
                 END
               ), 0)::BIGINT,
               COALESCE(SUM(reserved_micros), 0)::BIGINT,
               COALESCE(SUM(consumed_micros), 0)::BIGINT,
               COALESCE(SUM(restored_micros), 0)::BIGINT,
               COALESCE(SUM(
                 expired_micros
                 + CASE
                     WHEN state <> 'revoked' AND expires_at_ms <= $3
                     THEN available_micros
                     ELSE 0
                   END
               ), 0)::BIGINT,
               COALESCE(SUM(revoked_micros), 0)::BIGINT
        FROM credit_grants
        WHERE ($1::TEXT IS NULL OR tenant_id = $1)
          AND currency = $2
        "#,
    )
    .bind(tenant_id)
    .bind(currency)
    .bind(as_of_ms)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(CreditGrantSummary {
        original_amount_micros: row.0.to_string(),
        available_micros: row.1.to_string(),
        reserved_micros: row.2.to_string(),
        consumed_micros: row.3.to_string(),
        restored_micros: row.4.to_string(),
        expired_micros: row.5.to_string(),
        revoked_micros: row.6.to_string(),
    })
}

async fn load_grant(
    transaction: &mut Transaction<'_, Postgres>,
    grant_id: Uuid,
) -> Result<Option<CreditGrantRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT grant_id, semantic_key, tenant_id, currency,
               source_kind, source_reference, received_at_ms,
               expires_at_ms, original_amount_micros,
               reserved_micros, consumed_micros, restored_micros,
               expired_micros, revoked_micros, available_micros,
               state, control_version, created_at_ms, updated_at_ms
        FROM credit_grants
        WHERE grant_id = $1
        "#,
    )
    .bind(grant_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)
}

async fn load_grant_from_pool(
    pool: &PgPool,
    grant_id: Uuid,
) -> Result<Option<CreditGrantRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT grant_id, semantic_key, tenant_id, currency,
               source_kind, source_reference, received_at_ms,
               expires_at_ms, original_amount_micros,
               reserved_micros, consumed_micros, restored_micros,
               expired_micros, revoked_micros, available_micros,
               state, control_version, created_at_ms, updated_at_ms
        FROM credit_grants
        WHERE grant_id = $1
        "#,
    )
    .bind(grant_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)
}

async fn load_grant_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    grant_id: Uuid,
) -> Result<Option<CreditGrantRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT grant_id, semantic_key, tenant_id, currency,
               source_kind, source_reference, received_at_ms,
               expires_at_ms, original_amount_micros,
               reserved_micros, consumed_micros, restored_micros,
               expired_micros, revoked_micros, available_micros,
               state, control_version, created_at_ms, updated_at_ms
        FROM credit_grants
        WHERE grant_id = $1
        FOR UPDATE
        "#,
    )
    .bind(grant_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)
}

async fn lock_wallet(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    currency: &str,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("budget:{tenant_id}:{currency}"))
        .execute(&mut **transaction)
        .await
        .map_err(unavailable)?;
    Ok(())
}

async fn require_organization(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
) -> Result<(), ImageGatewayError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM identity_organizations WHERE organization_id = $1)",
    )
    .bind(tenant_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unavailable)?;
    if exists {
        Ok(())
    } else {
        Err(ImageGatewayError::not_found(
            "Organization was not found",
            Some("organization_id".to_string()),
            "organization_not_found",
        ))
    }
}

async fn ensure_billing_account(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    currency: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
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
    .bind(tenant_id)
    .bind(currency)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn insert_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: GrantEvent<'_>,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO credit_grant_events (
            grant_event_id, grant_id, tenant_id, currency,
            event_sequence, event_type, amount_micros,
            grant_reservation_id, hold_id, refund_id,
            related_grant_event_id, payload_hash,
            occurred_at_ms, created_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12, $13, $13
        )
        "#,
    )
    .bind(event.event_id)
    .bind(event.grant_id)
    .bind(event.tenant_id)
    .bind(event.currency)
    .bind(event.sequence)
    .bind(event.event_type)
    .bind(event.amount_micros)
    .bind(event.reservation_id)
    .bind(event.hold_id)
    .bind(event.refund_id)
    .bind(event.related_event_id)
    .bind(event.payload_hash)
    .bind(event.occurred_at_ms)
    .execute(&mut **transaction)
    .await
    .map_err(mutation_error)?;
    Ok(())
}

async fn insert_operation(
    transaction: &mut Transaction<'_, Postgres>,
    operation: GrantOperation<'_>,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO credit_grant_operations (
            operation_id, grant_id, grant_event_id,
            tenant_id, currency, operation,
            idempotency_key_digest, request_hash,
            actor_user_id, actor_session_id, reason, created_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11, $12
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(operation.grant_id)
    .bind(operation.event_id)
    .bind(operation.tenant_id)
    .bind(operation.currency)
    .bind(operation.operation)
    .bind(operation.idempotency_key_digest)
    .bind(operation.request_hash)
    .bind(operation.actor.user_id)
    .bind(operation.actor.session_id)
    .bind(operation.reason)
    .bind(operation.now)
    .execute(&mut **transaction)
    .await
    .map_err(mutation_error)?;
    Ok(())
}

async fn load_operation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    operation: &str,
    idempotency_key_digest: &str,
) -> Result<Option<CreditGrantOperationRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT grant_id, request_hash
        FROM credit_grant_operations
        WHERE tenant_id = $1
          AND operation = $2
          AND idempotency_key_digest = $3
        FOR SHARE
        "#,
    )
    .bind(tenant_id)
    .bind(operation)
    .bind(idempotency_key_digest)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)
}

#[allow(clippy::too_many_arguments)]
async fn insert_grant_ledger_pair(
    transaction: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    grant_id: Uuid,
    tenant_id: &str,
    currency: &str,
    amount_micros: i64,
    transaction_type: &str,
    payload_hash: &str,
    direction: LedgerDirection,
    reverses_transaction_id: Option<Uuid>,
    now: i64,
) -> Result<Uuid, ImageGatewayError> {
    let liability_id = ensure_ledger_account(
        transaction,
        &format!("tenant:{tenant_id}:{currency}:credit_liability"),
        "tenant",
        tenant_id,
        "credit_liability",
        currency,
        now,
    )
    .await?;
    let secondary_id = match direction {
        LedgerDirection::Issue | LedgerDirection::Retire => {
            ensure_ledger_account(
                transaction,
                &format!("platform:{currency}:credit-grant-promotional-expense"),
                "platform",
                "platform",
                "expense",
                currency,
                now,
            )
            .await?
        }
        LedgerDirection::Consume | LedgerDirection::Restore => {
            ensure_ledger_account(
                transaction,
                &format!("tenant:{tenant_id}:{currency}:receivable"),
                "tenant",
                tenant_id,
                "receivable",
                currency,
                now,
            )
            .await?
        }
    };
    let transaction_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions (
            transaction_id, semantic_key, transaction_type,
            currency, payload_hash, created_at_ms,
            reverses_transaction_id, source_credit_grant_event_id
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(transaction_id)
    .bind(format!(
        "{transaction_type}:v1:{grant_id}:{event_id}:{payload_hash}"
    ))
    .bind(transaction_type)
    .bind(currency)
    .bind(payload_hash)
    .bind(now)
    .bind(reverses_transaction_id)
    .bind(event_id)
    .execute(&mut **transaction)
    .await
    .map_err(mutation_error)?;
    let postings = match direction {
        LedgerDirection::Issue => [
            (1_i16, secondary_id, amount_micros),
            (2_i16, liability_id, -amount_micros),
        ],
        LedgerDirection::Consume => [
            (1_i16, liability_id, amount_micros),
            (2_i16, secondary_id, -amount_micros),
        ],
        LedgerDirection::Restore => [
            (1_i16, secondary_id, amount_micros),
            (2_i16, liability_id, -amount_micros),
        ],
        LedgerDirection::Retire => [
            (1_i16, liability_id, amount_micros),
            (2_i16, secondary_id, -amount_micros),
        ],
    };
    for (posting_no, account_id, amount) in postings {
        sqlx::query(
            r#"
            INSERT INTO ledger_postings (
                transaction_id, posting_no, account_id,
                currency, amount_micros, created_at_ms
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
        .execute(&mut **transaction)
        .await
        .map_err(mutation_error)?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, $2)",
    )
    .bind(transaction_id)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(mutation_error)?;
    Ok(transaction_id)
}

#[allow(clippy::too_many_arguments)]
async fn ensure_ledger_account(
    transaction: &mut Transaction<'_, Postgres>,
    account_key: &str,
    owner_type: &str,
    owner_id: &str,
    account_type: &str,
    currency: &str,
    now: i64,
) -> Result<Uuid, ImageGatewayError> {
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
    .execute(&mut **transaction)
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
    .fetch_optional(&mut **transaction)
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
        _ => Err(ImageGatewayError::conflict(
            "Ledger account conflicts with credit grant funding",
            None,
            "credit_grant_ledger_account_conflict",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_audit(
    transaction: &mut Transaction<'_, Postgres>,
    action: &str,
    grant_id: Uuid,
    tenant_id: &str,
    currency: &str,
    amount_micros: i64,
    actor: CreditGrantActor,
    reason: &str,
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
            $1, $2, $3, NULL, $4, 'credit_grant',
            $5, 'success', NULL, $6, $7
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor.user_id)
    .bind(actor.session_id)
    .bind(action)
    .bind(grant_id.to_string())
    .bind(json!({
        "organization_id": tenant_id,
        "currency": currency,
        "amount_micros": amount_micros.to_string(),
        "reason": reason,
    }))
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn validate_contracts(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut **transaction)
        .await
        .map_err(mutation_error)?;
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut **transaction)
        .await
        .map_err(unavailable)?;
    Ok(())
}

async fn database_now(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **transaction)
        .await
        .map_err(unavailable)
}

fn normalize_optional_organization_id(
    value: Option<String>,
) -> Result<Option<String>, ImageGatewayError> {
    value.map(normalize_organization_id).transpose()
}

fn normalize_organization_id(value: String) -> Result<String, ImageGatewayError> {
    let normalized = value.trim();
    if normalized.is_empty() || normalized.len() > 128 {
        return Err(ImageGatewayError::invalid_request(
            "organization_id must contain between 1 and 128 characters",
            Some("organization_id".to_string()),
            "invalid_organization_id",
        ));
    }
    Ok(normalized.to_string())
}

fn normalize_currency(value: &str) -> Result<String, ImageGatewayError> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.len() != 3 || !normalized.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return Err(ImageGatewayError::invalid_request(
            "currency must be an uppercase ISO-4217 code",
            Some("currency".to_string()),
            "invalid_currency",
        ));
    }
    Ok(normalized)
}

fn normalize_state(value: Option<&str>) -> Result<String, ImageGatewayError> {
    let normalized = value.unwrap_or("all").trim().to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "all" | "active" | "consuming" | "exhausted" | "expired" | "revoked"
    ) {
        Ok(normalized)
    } else {
        Err(ImageGatewayError::invalid_request(
            "state is invalid",
            Some("state".to_string()),
            "invalid_credit_grant_state",
        ))
    }
}

fn parse_positive_amount(value: &str) -> Result<i64, ImageGatewayError> {
    value
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|amount| *amount > 0)
        .ok_or_else(|| {
            ImageGatewayError::invalid_request(
                "amount_micros must be a positive integer string",
                Some("amount_micros".to_string()),
                "invalid_amount_micros",
            )
        })
}

fn normalize_source_reference(value: String) -> Result<String, ImageGatewayError> {
    normalize_bounded(value, "source_reference", 1, 512)
}

fn normalize_reason(value: String) -> Result<String, ImageGatewayError> {
    normalize_bounded(value, "reason", 1, 500)
}

fn normalize_bounded(
    value: String,
    field: &str,
    min: usize,
    max: usize,
) -> Result<String, ImageGatewayError> {
    let normalized = value.trim();
    if normalized.len() < min || normalized.len() > max || normalized.chars().any(char::is_control)
    {
        return Err(ImageGatewayError::invalid_request(
            format!("{field} must contain between {min} and {max} visible characters"),
            Some(field.to_string()),
            "invalid_credit_grant_field",
        ));
    }
    Ok(normalized.to_string())
}

fn hash_request(operation: &str, payload: serde_json::Value) -> Result<String, ImageGatewayError> {
    let canonical = serde_json::to_vec(&json!({
        "operation": operation,
        "payload": payload,
    }))
    .map_err(|_| ImageGatewayError::internal("credit grant request hash failed"))?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

fn funding_event_hash(
    event_type: &str,
    grant_id: Uuid,
    reservation_id: Uuid,
    hold_id: Uuid,
    amount_micros: i64,
) -> String {
    hex::encode(Sha256::digest(
        format!(
            "credit-grant-funding:v1:{event_type}:{grant_id}:{reservation_id}:{hold_id}:{amount_micros}"
        )
        .as_bytes(),
    ))
}

fn restoration_event_hash(
    event_type: &str,
    grant_id: Uuid,
    consumption_event_id: Uuid,
    refund_id: Uuid,
    amount_micros: i64,
) -> String {
    hex::encode(Sha256::digest(
        format!(
            "credit-grant-restoration:v1:{event_type}:{grant_id}:{consumption_event_id}:{refund_id}:{amount_micros}"
        )
        .as_bytes(),
    ))
}

fn terminal_event_hash(
    event_type: &str,
    grant_id: Uuid,
    sequence: i64,
    amount_micros: i64,
) -> String {
    hex::encode(Sha256::digest(
        format!("credit-grant-terminal:v1:{event_type}:{grant_id}:{sequence}:{amount_micros}")
            .as_bytes(),
    ))
}

fn parse_cursor(value: &str) -> Result<CreditGrantCursor, ImageGatewayError> {
    let (received_at_ms, grant_id) = value.split_once(':').ok_or_else(invalid_cursor)?;
    let received_at_ms = received_at_ms
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(invalid_cursor)?;
    let grant_id = Uuid::parse_str(grant_id).map_err(|_| invalid_cursor())?;
    Ok(CreditGrantCursor {
        received_at_ms,
        grant_id,
    })
}

fn grant_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Credit grant was not found",
        Some("grant_id".to_string()),
        "credit_grant_not_found",
    )
}

fn invalid_cursor() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "after cursor is invalid",
        Some("after".to_string()),
        "invalid_cursor",
    )
}

fn mutation_error(error: sqlx::Error) -> ImageGatewayError {
    match error.as_database_error().and_then(|error| error.code()) {
        Some(code)
            if matches!(
                code.as_ref(),
                "23503" | "23505" | "23514" | "55000" | "P0001"
            ) =>
        {
            ImageGatewayError::conflict(
                "Credit grant state conflicts with the requested operation",
                None,
                "credit_grant_conflict",
            )
        }
        _ => unavailable(error),
    }
}

fn unavailable(_: sqlx::Error) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Credit grants are unavailable")
}
