use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BillingControlActor {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct UpdateBillingAccountLimitRequest {
    pub credit_limit_micros: String,
    pub expected_control_version: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListBillingAccountsRequest {
    pub currency: Option<String>,
    pub query: Option<String>,
    pub after: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingAccountControlView {
    pub object: &'static str,
    pub tenant_id: String,
    pub currency: String,
    pub configured: bool,
    pub credit_limit_micros: String,
    pub held_micros: String,
    pub captured_micros: String,
    pub refunded_micros: String,
    pub available_micros: String,
    pub control_version: String,
    pub updated_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingOrganizationAccountView {
    pub organization_id: String,
    pub display_name: String,
    pub organization_kind: String,
    pub account: BillingAccountControlView,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct BillingAccountControlList {
    pub object: &'static str,
    pub data: Vec<BillingOrganizationAccountView>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[async_trait]
pub trait BillingAccountControlService: Send + Sync + 'static {
    async fn list_accounts(
        &self,
        request: ListBillingAccountsRequest,
    ) -> Result<BillingAccountControlList, ImageGatewayError>;

    async fn get_account(
        &self,
        tenant_id: &str,
        currency: &str,
    ) -> Result<BillingAccountControlView, ImageGatewayError>;

    async fn update_limit(
        &self,
        tenant_id: &str,
        currency: &str,
        actor: BillingControlActor,
        request: UpdateBillingAccountLimitRequest,
    ) -> Result<BillingAccountControlView, ImageGatewayError>;
}

#[derive(Clone)]
pub struct PostgresBillingAccountControlService {
    pool: PgPool,
}

impl PostgresBillingAccountControlService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl BillingAccountControlService for PostgresBillingAccountControlService {
    async fn list_accounts(
        &self,
        request: ListBillingAccountsRequest,
    ) -> Result<BillingAccountControlList, ImageGatewayError> {
        let currency = normalize_currency(request.currency.as_deref().unwrap_or("USD"))?;
        let query = normalize_search_query(request.query)?;
        let after = request.after;
        if let Some(after) = &after {
            validate_tenant_id(after)?;
        }
        let limit = request.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) {
            return Err(ImageGatewayError::invalid_request(
                "limit must be between 1 and 100",
                Some("limit".to_string()),
                "invalid_limit",
            ));
        }
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ImageGatewayError::internal("billing account page size overflow"))?;
        let escaped_query = query.as_deref().map(escape_like_pattern);
        let mut rows = sqlx::query_as::<_, BillingOrganizationAccountRow>(
            r#"
            SELECT organization.organization_id,
                   organization.display_name,
                   organization.organization_kind,
                   account.credit_limit_micros,
                   account.held_micros,
                   account.captured_micros,
                   account.refunded_micros,
                   account.control_version,
                   account.updated_at_ms
            FROM identity_organizations organization
            LEFT JOIN billing_accounts account
              ON account.tenant_id = organization.organization_id
             AND account.currency = $1
            WHERE ($2::TEXT IS NULL OR organization.organization_id > $2)
              AND (
                    $3::TEXT IS NULL
                    OR organization.organization_id ILIKE '%' || $3 || '%' ESCAPE '\'
                    OR organization.display_name ILIKE '%' || $3 || '%' ESCAPE '\'
              )
            ORDER BY organization.organization_id
            LIMIT $4
            "#,
        )
        .bind(&currency)
        .bind(after.as_deref())
        .bind(escaped_query.as_deref())
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_after = has_more
            .then(|| rows.last().map(|row| row.organization_id.clone()))
            .flatten();
        Ok(BillingAccountControlList {
            object: "list",
            data: rows
                .into_iter()
                .map(|row| row.into_view(&currency))
                .collect(),
            has_more,
            next_after,
        })
    }

    async fn get_account(
        &self,
        tenant_id: &str,
        currency: &str,
    ) -> Result<BillingAccountControlView, ImageGatewayError> {
        validate_tenant_id(tenant_id)?;
        let currency = normalize_currency(currency)?;
        require_organization(&self.pool, tenant_id).await?;
        let account = sqlx::query_as::<_, BillingAccountRow>(
            r#"
            SELECT tenant_id, currency, credit_limit_micros,
                   held_micros, captured_micros, refunded_micros,
                   control_version, updated_at_ms
            FROM billing_accounts
            WHERE tenant_id = $1 AND currency = $2
            "#,
        )
        .bind(tenant_id)
        .bind(&currency)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(account
            .map(BillingAccountRow::into_view)
            .unwrap_or_else(|| empty_view(tenant_id, currency)))
    }

    async fn update_limit(
        &self,
        tenant_id: &str,
        currency: &str,
        actor: BillingControlActor,
        request: UpdateBillingAccountLimitRequest,
    ) -> Result<BillingAccountControlView, ImageGatewayError> {
        validate_tenant_id(tenant_id)?;
        let currency = normalize_currency(currency)?;
        let credit_limit_micros =
            parse_nonnegative(&request.credit_limit_micros, "credit_limit_micros")?;
        let expected_control_version = parse_nonnegative(
            &request.expected_control_version,
            "expected_control_version",
        )?;
        let reason = normalize_reason(request.reason)?;
        let now = now_ms()?;

        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("billing-account-control:{tenant_id}:{currency}"))
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;
        require_organization_in_transaction(&mut transaction, tenant_id).await?;

        let existing = account_for_update(&mut transaction, tenant_id, &currency).await?;
        let updated = match existing {
            None => {
                if expected_control_version != 0 {
                    return Err(version_conflict());
                }
                sqlx::query(
                    r#"
                    INSERT INTO billing_accounts (
                        tenant_id, currency, credit_limit_micros,
                        held_micros, captured_micros, refunded_micros,
                        created_at_ms, updated_at_ms, control_version
                    )
                    VALUES ($1, $2, $3, 0, 0, 0, $4, $4, 1)
                    "#,
                )
                .bind(tenant_id)
                .bind(&currency)
                .bind(credit_limit_micros)
                .bind(now)
                .execute(&mut *transaction)
                .await
                .map_err(unavailable)?;
                insert_change(
                    &mut transaction,
                    tenant_id,
                    &currency,
                    0,
                    credit_limit_micros,
                    1,
                    actor,
                    &reason,
                    now,
                )
                .await?;
                BillingAccountRow {
                    tenant_id: tenant_id.to_string(),
                    currency: currency.clone(),
                    credit_limit_micros,
                    held_micros: 0,
                    captured_micros: 0,
                    refunded_micros: 0,
                    control_version: 1,
                    updated_at_ms: now,
                }
            }
            Some(existing) => {
                if existing.control_version != expected_control_version {
                    return Err(version_conflict());
                }
                if existing.credit_limit_micros == credit_limit_micros {
                    transaction.commit().await.map_err(unavailable)?;
                    return Ok(existing.into_view());
                }
                let committed = i128::from(existing.held_micros)
                    .checked_add(i128::from(existing.captured_micros))
                    .and_then(|value| value.checked_sub(i128::from(existing.refunded_micros)))
                    .ok_or_else(|| ImageGatewayError::internal("billing account total overflow"))?;
                if i128::from(credit_limit_micros) < committed {
                    return Err(ImageGatewayError::conflict(
                        "Credit limit cannot be lower than held and captured spend",
                        Some("credit_limit_micros".to_string()),
                        "credit_limit_below_committed_spend",
                    ));
                }
                let control_version = existing.control_version.checked_add(1).ok_or_else(|| {
                    ImageGatewayError::internal("billing account control version overflow")
                })?;
                insert_change(
                    &mut transaction,
                    tenant_id,
                    &currency,
                    existing.credit_limit_micros,
                    credit_limit_micros,
                    control_version,
                    actor,
                    &reason,
                    now,
                )
                .await?;
                let updated_rows = sqlx::query(
                    r#"
                    UPDATE billing_accounts
                    SET credit_limit_micros = $3,
                        control_version = $4,
                        updated_at_ms = $5
                    WHERE tenant_id = $1
                      AND currency = $2
                      AND control_version = $6
                    "#,
                )
                .bind(tenant_id)
                .bind(&currency)
                .bind(credit_limit_micros)
                .bind(control_version)
                .bind(now)
                .bind(expected_control_version)
                .execute(&mut *transaction)
                .await
                .map_err(unavailable)?
                .rows_affected();
                if updated_rows != 1 {
                    return Err(version_conflict());
                }
                BillingAccountRow {
                    credit_limit_micros,
                    control_version,
                    updated_at_ms: now,
                    ..existing
                }
            }
        };

        insert_audit(
            &mut transaction,
            tenant_id,
            &currency,
            actor,
            &reason,
            &updated,
            now,
        )
        .await?;
        transaction.commit().await.map_err(unavailable)?;
        Ok(updated.into_view())
    }
}

#[derive(Clone, Debug, FromRow)]
struct BillingAccountRow {
    tenant_id: String,
    currency: String,
    credit_limit_micros: i64,
    held_micros: i64,
    captured_micros: i64,
    refunded_micros: i64,
    control_version: i64,
    updated_at_ms: i64,
}

#[derive(Clone, Debug, FromRow)]
struct BillingOrganizationAccountRow {
    organization_id: String,
    display_name: String,
    organization_kind: String,
    credit_limit_micros: Option<i64>,
    held_micros: Option<i64>,
    captured_micros: Option<i64>,
    refunded_micros: Option<i64>,
    control_version: Option<i64>,
    updated_at_ms: Option<i64>,
}

impl BillingOrganizationAccountRow {
    fn into_view(self, currency: &str) -> BillingOrganizationAccountView {
        let account = match (
            self.credit_limit_micros,
            self.held_micros,
            self.captured_micros,
            self.refunded_micros,
            self.control_version,
            self.updated_at_ms,
        ) {
            (
                Some(credit_limit_micros),
                Some(held_micros),
                Some(captured_micros),
                Some(refunded_micros),
                Some(control_version),
                Some(updated_at_ms),
            ) => BillingAccountRow {
                tenant_id: self.organization_id.clone(),
                currency: currency.to_string(),
                credit_limit_micros,
                held_micros,
                captured_micros,
                refunded_micros,
                control_version,
                updated_at_ms,
            }
            .into_view(),
            _ => empty_view(&self.organization_id, currency.to_string()),
        };
        BillingOrganizationAccountView {
            organization_id: self.organization_id,
            display_name: self.display_name,
            organization_kind: self.organization_kind,
            account,
        }
    }
}

impl BillingAccountRow {
    fn into_view(self) -> BillingAccountControlView {
        let available = self
            .credit_limit_micros
            .saturating_sub(self.held_micros)
            .saturating_sub(self.captured_micros)
            .saturating_add(self.refunded_micros);
        BillingAccountControlView {
            object: "billing.account",
            tenant_id: self.tenant_id,
            currency: self.currency,
            configured: true,
            credit_limit_micros: self.credit_limit_micros.to_string(),
            held_micros: self.held_micros.to_string(),
            captured_micros: self.captured_micros.to_string(),
            refunded_micros: self.refunded_micros.to_string(),
            available_micros: available.to_string(),
            control_version: self.control_version.to_string(),
            updated_at_ms: Some(self.updated_at_ms),
        }
    }
}

fn empty_view(tenant_id: &str, currency: String) -> BillingAccountControlView {
    BillingAccountControlView {
        object: "billing.account",
        tenant_id: tenant_id.to_string(),
        currency,
        configured: false,
        credit_limit_micros: "0".to_string(),
        held_micros: "0".to_string(),
        captured_micros: "0".to_string(),
        refunded_micros: "0".to_string(),
        available_micros: "0".to_string(),
        control_version: "0".to_string(),
        updated_at_ms: None,
    }
}

async fn account_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    currency: &str,
) -> Result<Option<BillingAccountRow>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT tenant_id, currency, credit_limit_micros,
               held_micros, captured_micros, refunded_micros,
               control_version, updated_at_ms
        FROM billing_accounts
        WHERE tenant_id = $1 AND currency = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(currency)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)
}

#[allow(clippy::too_many_arguments)]
async fn insert_change(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    currency: &str,
    previous_credit_limit_micros: i64,
    new_credit_limit_micros: i64,
    control_version: i64,
    actor: BillingControlActor,
    reason: &str,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO billing_account_limit_changes (
            change_id, tenant_id, currency,
            previous_credit_limit_micros, new_credit_limit_micros,
            control_version, actor_user_id, session_id,
            reason, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(tenant_id)
    .bind(currency)
    .bind(previous_credit_limit_micros)
    .bind(new_credit_limit_micros)
    .bind(control_version)
    .bind(actor.user_id)
    .bind(actor.session_id)
    .bind(reason)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn insert_audit(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    currency: &str,
    actor: BillingControlActor,
    reason: &str,
    account: &BillingAccountRow,
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
            $1, $2, $3, NULL, 'billing.account.credit_limit.update',
            'billing_account', $4, 'success', NULL, $5, $6
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor.user_id)
    .bind(actor.session_id)
    .bind(format!("{tenant_id}:{currency}"))
    .bind(json!({
        "tenant_id": tenant_id,
        "currency": currency,
        "credit_limit_micros": account.credit_limit_micros.to_string(),
        "control_version": account.control_version.to_string(),
        "reason": reason,
    }))
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn require_organization(pool: &PgPool, tenant_id: &str) -> Result<(), ImageGatewayError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM identity_organizations WHERE organization_id = $1)",
    )
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .map_err(unavailable)?;
    if exists {
        Ok(())
    } else {
        Err(organization_not_found())
    }
}

async fn require_organization_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
) -> Result<(), ImageGatewayError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM identity_organizations WHERE organization_id = $1)",
    )
    .bind(tenant_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unavailable)?;
    if exists {
        Ok(())
    } else {
        Err(organization_not_found())
    }
}

fn validate_tenant_id(value: &str) -> Result<(), ImageGatewayError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b'/')
    {
        return Err(ImageGatewayError::invalid_request(
            "tenant_id is invalid",
            Some("tenant_id".to_string()),
            "invalid_identifier",
        ));
    }
    Ok(())
}

fn normalize_currency(value: &str) -> Result<String, ImageGatewayError> {
    let currency = value.trim().to_ascii_uppercase();
    if currency.len() != 3 || !currency.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return Err(ImageGatewayError::invalid_request(
            "currency must be a three-letter ISO 4217 code",
            Some("currency".to_string()),
            "invalid_currency",
        ));
    }
    Ok(currency)
}

fn parse_nonnegative(value: &str, param: &str) -> Result<i64, ImageGatewayError> {
    let value = value.parse::<i64>().map_err(|_| {
        ImageGatewayError::invalid_request(
            format!("{param} must be a non-negative integer string"),
            Some(param.to_string()),
            "invalid_billing_account_control",
        )
    })?;
    if value < 0 {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} must be non-negative"),
            Some(param.to_string()),
            "invalid_billing_account_control",
        ));
    }
    Ok(value)
}

fn normalize_reason(value: String) -> Result<String, ImageGatewayError> {
    let reason = value.trim().to_string();
    if !(3..=500).contains(&reason.chars().count()) {
        return Err(ImageGatewayError::invalid_request(
            "reason must contain between 3 and 500 characters",
            Some("reason".to_string()),
            "invalid_billing_account_control_reason",
        ));
    }
    Ok(reason)
}

fn normalize_search_query(value: Option<String>) -> Result<Option<String>, ImageGatewayError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.chars().count() > 128 || value.chars().any(char::is_control) {
        return Err(ImageGatewayError::invalid_request(
            "query must contain at most 128 visible characters",
            Some("query".to_string()),
            "invalid_query",
        ));
    }
    Ok(Some(value.to_string()))
}

fn escape_like_pattern(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn organization_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Organization was not found",
        Some("tenant_id".to_string()),
        "organization_not_found",
    )
}

fn version_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Billing account changed; reload it before saving",
        Some("expected_control_version".to_string()),
        "billing_account_control_version_conflict",
    )
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock is before Unix epoch"))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| ImageGatewayError::internal("system clock is out of range"))
}

fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(%error, "billing account control store unavailable");
    ImageGatewayError::service_unavailable("Billing account control is unavailable")
}
