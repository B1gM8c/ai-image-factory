use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

const DEFAULT_ALERT_THRESHOLD_PERCENT: i16 = 100;
const MAX_ALERT_THRESHOLD_COUNT: usize = 20;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectSpendLimitType {
    #[default]
    Soft,
    Hard,
}

impl ProjectSpendLimitType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Soft => "soft",
            Self::Hard => "hard",
        }
    }
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProjectSpendBudgetRequest {
    pub currency: String,
    pub monthly_budget_micros: String,
    #[serde(default)]
    pub limit_type: ProjectSpendLimitType,
    pub alert_thresholds: Vec<i16>,
    pub expected_control_version: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectSpendAlertEventView {
    #[schema(value_type = String)]
    pub event_id: Uuid,
    pub threshold_percent: i16,
    pub spend_micros: String,
    pub notification_state: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectSpendBudgetView {
    pub object: &'static str,
    pub project_id: String,
    pub organization_id: String,
    pub configured: bool,
    pub currency: Option<String>,
    pub monthly_budget_micros: Option<String>,
    pub spend_micros: String,
    pub reserved_micros: String,
    pub remaining_micros: Option<String>,
    pub usage_basis_points: Option<String>,
    pub limit_type: ProjectSpendLimitType,
    pub period_kind: &'static str,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
    pub alert_thresholds: Vec<i16>,
    pub alert_events: Vec<ProjectSpendAlertEventView>,
    pub control_version: String,
    pub updated_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectSpendNotificationView {
    #[schema(value_type = String)]
    pub delivery_id: Uuid,
    #[schema(value_type = String)]
    pub event_id: Uuid,
    pub project_id: String,
    pub project_name: String,
    pub currency: String,
    pub threshold_percent: i16,
    pub monthly_budget_micros: String,
    pub spend_micros: String,
    pub created_at_ms: i64,
    pub read_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectSpendNotificationList {
    pub object: &'static str,
    pub data: Vec<ProjectSpendNotificationView>,
    pub unread_count: i64,
}

#[async_trait]
pub trait ProjectSpendBudgetService: Send + Sync + 'static {
    async fn get_budget(
        &self,
        project_id: &str,
    ) -> Result<ProjectSpendBudgetView, ImageGatewayError>;

    async fn update_budget(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        request: UpdateProjectSpendBudgetRequest,
    ) -> Result<ProjectSpendBudgetView, ImageGatewayError>;

    async fn list_notifications(
        &self,
        recipient_user_id: Uuid,
        limit: usize,
    ) -> Result<ProjectSpendNotificationList, ImageGatewayError>;

    async fn mark_notification_read(
        &self,
        recipient_user_id: Uuid,
        delivery_id: Uuid,
    ) -> Result<ProjectSpendNotificationView, ImageGatewayError>;

    async fn evaluate_pending(&self, limit: usize) -> Result<usize, ImageGatewayError>;
}

#[derive(Clone)]
pub struct PostgresProjectSpendBudgetService {
    pool: PgPool,
}

impl PostgresProjectSpendBudgetService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProjectSpendBudgetService for PostgresProjectSpendBudgetService {
    async fn get_budget(
        &self,
        project_id: &str,
    ) -> Result<ProjectSpendBudgetView, ImageGatewayError> {
        validate_project_id(project_id)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let project = project_identity(&mut tx, project_id).await?;
        let period = current_utc_month(&mut tx).await.map_err(unavailable)?;
        evaluate_project(&mut tx, project_id, &period).await?;
        let view = read_budget(&mut tx, project, period).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(view)
    }

    async fn update_budget(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        request: UpdateProjectSpendBudgetRequest,
    ) -> Result<ProjectSpendBudgetView, ImageGatewayError> {
        validate_project_id(project_id)?;
        let currency = validate_currency(request.currency)?;
        let monthly_budget_micros =
            parse_positive_money(&request.monthly_budget_micros, "monthly_budget_micros")?;
        let limit_type = request.limit_type;
        let expected_control_version = parse_nonnegative_i64(
            &request.expected_control_version,
            "expected_control_version",
        )?;
        let thresholds = normalize_thresholds(request.alert_thresholds)?;
        let now_ms = now_ms()?;

        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let project = project_identity(&mut tx, project_id).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("project-spend-budget:{project_id}"))
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;

        let current_version = sqlx::query_scalar::<_, i64>(
            "SELECT control_version FROM project_spend_budgets WHERE project_id = $1 FOR UPDATE",
        )
        .bind(project_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;

        match current_version {
            Some(version) if version != expected_control_version => {
                return Err(version_conflict());
            }
            None if expected_control_version != 0 => {
                return Err(version_conflict());
            }
            Some(_) => {
                sqlx::query(
                    r#"
                    UPDATE project_spend_budgets
                    SET currency = $2,
                        monthly_budget_micros = $3,
                        limit_type = $4,
                        control_version = control_version + 1,
                        updated_by_user_id = $5,
                        updated_at_ms = $6
                    WHERE project_id = $1
                    "#,
                )
                .bind(project_id)
                .bind(&currency)
                .bind(monthly_budget_micros)
                .bind(limit_type.as_str())
                .bind(actor_user_id)
                .bind(now_ms)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
            }
            None => {
                sqlx::query(
                    r#"
                    INSERT INTO project_spend_budgets(
                        project_id, organization_id, currency,
                        monthly_budget_micros, limit_type, created_by_user_id,
                        updated_by_user_id, created_at_ms, updated_at_ms
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $6, $7, $7)
                    "#,
                )
                .bind(project_id)
                .bind(&project.organization_id)
                .bind(&currency)
                .bind(monthly_budget_micros)
                .bind(limit_type.as_str())
                .bind(actor_user_id)
                .bind(now_ms)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
            }
        }

        if limit_type == ProjectSpendLimitType::Hard {
            let period = current_utc_month(&mut tx).await.map_err(unavailable)?;
            let exposure = current_exposure(&mut tx, project_id, &currency, &period)
                .await
                .map_err(unavailable)?;
            if exposure.total_micros() > i128::from(monthly_budget_micros) {
                return Err(ImageGatewayError::conflict(
                    "The hard limit cannot be lower than current settled spend and active reservations",
                    Some("monthly_budget_micros".to_string()),
                    "project_hard_limit_below_current_exposure",
                ));
            }
        }

        sqlx::query("DELETE FROM project_spend_alert_thresholds WHERE project_id = $1")
            .bind(project_id)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        for threshold in thresholds {
            sqlx::query(
                r#"
                INSERT INTO project_spend_alert_thresholds(
                    project_id, threshold_percent, created_at_ms
                )
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(project_id)
            .bind(threshold)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }

        let period = current_utc_month(&mut tx).await.map_err(unavailable)?;
        evaluate_project(&mut tx, project_id, &period).await?;
        sqlx::query("DELETE FROM project_spend_evaluation_queue WHERE project_id = $1")
            .bind(project_id)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let view = read_budget(&mut tx, project, period).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(view)
    }

    async fn list_notifications(
        &self,
        recipient_user_id: Uuid,
        limit: usize,
    ) -> Result<ProjectSpendNotificationList, ImageGatewayError> {
        let limit = limit.clamp(1, 100) as i64;
        let rows = sqlx::query_as::<_, NotificationRow>(
            r#"
            SELECT delivery.delivery_id,
                   event.event_id,
                   event.project_id,
                   project.name AS project_name,
                   event.currency,
                   event.threshold_percent,
                   event.monthly_budget_micros,
                   event.spend_micros,
                   delivery.created_at_ms,
                   delivery.read_at_ms
            FROM project_spend_notification_deliveries delivery
            JOIN project_spend_alert_events event
              ON event.event_id = delivery.event_id
            JOIN gateway_projects project
              ON project.id = event.project_id
             AND project.tenant_id = event.organization_id
            WHERE delivery.recipient_user_id = $1
              AND delivery.channel = 'in_app'
              AND delivery.state = 'delivered'
            ORDER BY delivery.created_at_ms DESC, delivery.delivery_id DESC
            LIMIT $2
            "#,
        )
        .bind(recipient_user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let unread_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM project_spend_notification_deliveries
            WHERE recipient_user_id = $1
              AND channel = 'in_app'
              AND state = 'delivered'
              AND read_at_ms IS NULL
            "#,
        )
        .bind(recipient_user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(ProjectSpendNotificationList {
            object: "list",
            data: rows.into_iter().map(Into::into).collect(),
            unread_count,
        })
    }

    async fn mark_notification_read(
        &self,
        recipient_user_id: Uuid,
        delivery_id: Uuid,
    ) -> Result<ProjectSpendNotificationView, ImageGatewayError> {
        let now_ms = now_ms()?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let event_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            UPDATE project_spend_notification_deliveries
            SET read_at_ms = COALESCE(read_at_ms, $3)
            WHERE delivery_id = $1
              AND recipient_user_id = $2
              AND channel = 'in_app'
              AND state = 'delivered'
            RETURNING event_id
            "#,
        )
        .bind(delivery_id)
        .bind(recipient_user_id)
        .bind(now_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or_else(notification_not_found)?;
        sqlx::query(
            r#"
            UPDATE project_spend_alert_events event
            SET notification_state = 'acknowledged',
                acknowledged_at_ms = $2,
                acknowledged_by_user_id = $3
            WHERE event.event_id = $1
              AND event.notification_state = 'pending'
              AND NOT EXISTS (
                  SELECT 1
                  FROM project_spend_notification_deliveries delivery
                  WHERE delivery.event_id = event.event_id
                    AND delivery.channel = 'in_app'
                    AND delivery.state = 'delivered'
                    AND delivery.read_at_ms IS NULL
              )
            "#,
        )
        .bind(event_id)
        .bind(now_ms)
        .bind(recipient_user_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        let row = read_notification(&mut tx, recipient_user_id, delivery_id).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(row.into())
    }

    async fn evaluate_pending(&self, limit: usize) -> Result<usize, ImageGatewayError> {
        let limit = limit.clamp(1, 100);
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let project_ids = sqlx::query_scalar::<_, String>(
            r#"
            SELECT project_id
            FROM project_spend_evaluation_queue
            ORDER BY requested_at_ms, project_id
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        if project_ids.is_empty() {
            tx.rollback().await.map_err(unavailable)?;
            return Ok(0);
        }

        let period = current_utc_month(&mut tx).await.map_err(unavailable)?;
        for project_id in &project_ids {
            evaluate_project(&mut tx, project_id, &period).await?;
        }
        sqlx::query(
            "DELETE FROM project_spend_evaluation_queue WHERE project_id = ANY($1::TEXT[])",
        )
        .bind(&project_ids)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(project_ids.len())
    }
}

#[derive(Clone, Debug, FromRow)]
struct ProjectIdentity {
    project_id: String,
    organization_id: String,
}

#[derive(Clone, Copy, Debug)]
struct BudgetPeriod {
    start_ms: i64,
    end_ms: i64,
}

#[derive(Debug, FromRow)]
struct BudgetRow {
    currency: String,
    monthly_budget_micros: i64,
    limit_type: String,
    control_version: i64,
    updated_at_ms: i64,
}

#[derive(Clone, Copy, Debug, FromRow)]
struct ProjectSpendExposure {
    spend_micros: i64,
    reserved_micros: i64,
}

impl ProjectSpendExposure {
    fn total_micros(self) -> i128 {
        i128::from(self.spend_micros) + i128::from(self.reserved_micros)
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum ProjectHardBudgetError {
    #[error("project hard spend limit exceeded")]
    Exceeded,
    #[error("project hard spend limit currency does not match the quote")]
    CurrencyMismatch,
    #[error("project hard spend limit store is unavailable")]
    Unavailable,
}

#[derive(Debug, FromRow)]
struct AlertEventRow {
    event_id: Uuid,
    threshold_percent: i16,
    spend_micros: i64,
    notification_state: String,
    created_at_ms: i64,
}

#[derive(Debug, FromRow)]
struct NotificationRow {
    delivery_id: Uuid,
    event_id: Uuid,
    project_id: String,
    project_name: String,
    currency: String,
    threshold_percent: i16,
    monthly_budget_micros: i64,
    spend_micros: i64,
    created_at_ms: i64,
    read_at_ms: Option<i64>,
}

impl From<NotificationRow> for ProjectSpendNotificationView {
    fn from(row: NotificationRow) -> Self {
        Self {
            delivery_id: row.delivery_id,
            event_id: row.event_id,
            project_id: row.project_id,
            project_name: row.project_name,
            currency: row.currency,
            threshold_percent: row.threshold_percent,
            monthly_budget_micros: row.monthly_budget_micros.to_string(),
            spend_micros: row.spend_micros.to_string(),
            created_at_ms: row.created_at_ms,
            read_at_ms: row.read_at_ms,
        }
    }
}

async fn project_identity(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<ProjectIdentity, ImageGatewayError> {
    sqlx::query_as::<_, ProjectIdentity>(
        r#"
        SELECT id AS project_id, tenant_id AS organization_id
        FROM gateway_projects
        WHERE id = $1 AND archived_at IS NULL
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or_else(project_not_found)
}

async fn current_utc_month(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<BudgetPeriod, sqlx::Error> {
    let (start_ms, end_ms) = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT
            (EXTRACT(EPOCH FROM DATE_TRUNC(
                'month', transaction_timestamp() AT TIME ZONE 'UTC'
            )) * 1000)::BIGINT,
            (EXTRACT(EPOCH FROM (
                DATE_TRUNC('month', transaction_timestamp() AT TIME ZONE 'UTC')
                + INTERVAL '1 month'
            )) * 1000)::BIGINT
        "#,
    )
    .fetch_one(&mut **tx)
    .await?;
    Ok(BudgetPeriod { start_ms, end_ms })
}

async fn current_exposure(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
    currency: &str,
    period: &BudgetPeriod,
) -> Result<ProjectSpendExposure, sqlx::Error> {
    sqlx::query_as::<_, ProjectSpendExposure>(
        r#"
        SELECT
          (
            SELECT COALESCE(SUM(amount_micros), 0)::BIGINT
            FROM (
                SELECT rated.amount_micros::NUMERIC AS amount_micros
                FROM rated_usage rated
                JOIN jobs job ON job.job_id = rated.job_id
                JOIN job_auth_attributions attribution
                  ON attribution.job_id = rated.job_id
                WHERE attribution.project_id = $1
                  AND job.economics_contract_version IN (1, 2, 3)
                  AND rated.currency = $2
                  AND rated.created_at_ms >= $3
                  AND rated.created_at_ms < $4

                UNION ALL

                SELECT rated.total_amount_micros::NUMERIC AS amount_micros
                FROM customer_rated_usage rated
                JOIN customer_price_quotes quote
                  ON quote.quote_id = rated.quote_id
                 AND quote.job_id = rated.job_id
                JOIN jobs job ON job.job_id = rated.job_id
                WHERE quote.project_id = $1
                  AND job.economics_contract_version = 4
                  AND rated.currency = $2
                  AND rated.created_at_ms >= $3
                  AND rated.created_at_ms < $4
            ) charged
          ) AS spend_micros,
          (
            SELECT COALESCE(SUM(hold.held_micros::NUMERIC), 0)::BIGINT
            FROM customer_price_quotes quote
            JOIN customer_billing_holds hold
              ON hold.quote_id = quote.quote_id
             AND hold.job_id = quote.job_id
            WHERE quote.project_id = $1
              AND quote.currency = $2
              AND hold.state = 'held'
          ) AS reserved_micros
        "#,
    )
    .bind(project_id)
    .bind(currency)
    .bind(period.start_ms)
    .bind(period.end_ms)
    .fetch_one(&mut **tx)
    .await
}

pub(crate) async fn enforce_project_hard_budget(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
    currency: &str,
    new_reservation_micros: i64,
) -> Result<(), ProjectHardBudgetError> {
    if new_reservation_micros < 0 {
        return Err(ProjectHardBudgetError::Unavailable);
    }
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("project-spend-budget:{project_id}"))
        .execute(&mut **tx)
        .await
        .map_err(|_| ProjectHardBudgetError::Unavailable)?;
    let budget = sqlx::query_as::<_, (String, i64, String)>(
        r#"
        SELECT currency, monthly_budget_micros, limit_type
        FROM project_spend_budgets
        WHERE project_id = $1
        FOR SHARE
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|_| ProjectHardBudgetError::Unavailable)?;
    let Some((budget_currency, monthly_budget_micros, limit_type)) = budget else {
        return Ok(());
    };
    if limit_type == "soft" {
        return Ok(());
    }
    if limit_type != "hard" {
        return Err(ProjectHardBudgetError::Unavailable);
    }
    if budget_currency != currency {
        return Err(ProjectHardBudgetError::CurrencyMismatch);
    }
    let period = current_utc_month(tx)
        .await
        .map_err(|_| ProjectHardBudgetError::Unavailable)?;
    let exposure = current_exposure(tx, project_id, currency, &period)
        .await
        .map_err(|_| ProjectHardBudgetError::Unavailable)?;
    if exposure.total_micros() + i128::from(new_reservation_micros)
        > i128::from(monthly_budget_micros)
    {
        return Err(ProjectHardBudgetError::Exceeded);
    }
    Ok(())
}

async fn evaluate_project(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
    period: &BudgetPeriod,
) -> Result<(), ImageGatewayError> {
    let Some(budget) = sqlx::query_as::<_, BudgetRow>(
        r#"
        SELECT currency, monthly_budget_micros, limit_type, control_version, updated_at_ms
        FROM project_spend_budgets
        WHERE project_id = $1
        FOR SHARE
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    else {
        return Ok(());
    };
    let organization_id = sqlx::query_scalar::<_, String>(
        "SELECT organization_id FROM project_spend_budgets WHERE project_id = $1",
    )
    .bind(project_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    let spend_micros = current_exposure(tx, project_id, &budget.currency, period)
        .await
        .map_err(unavailable)?
        .spend_micros;
    let now_ms = now_ms()?;
    sqlx::query(
        r#"
        INSERT INTO project_spend_alert_events(
            event_id, project_id, organization_id, currency,
            period_start_ms, period_end_ms, threshold_percent,
            budget_control_version, monthly_budget_micros, spend_micros,
            created_at_ms
        )
        SELECT gen_random_uuid(), $1, $2, $3, $4, $5,
               threshold.threshold_percent, $6, $7, $8, $9
        FROM project_spend_alert_thresholds threshold
        WHERE threshold.project_id = $1
          AND $8::NUMERIC * 100 >=
              $7::NUMERIC * threshold.threshold_percent::NUMERIC
        ON CONFLICT (
            project_id, currency, period_start_ms,
            threshold_percent, budget_control_version
        ) DO NOTHING
        "#,
    )
    .bind(project_id)
    .bind(&organization_id)
    .bind(&budget.currency)
    .bind(period.start_ms)
    .bind(period.end_ms)
    .bind(budget.control_version)
    .bind(budget.monthly_budget_micros)
    .bind(spend_micros)
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;

    sqlx::query(
        r#"
        INSERT INTO project_spend_notification_deliveries(
            delivery_id, event_id, recipient_user_id,
            state, attempt_count, next_attempt_at_ms,
            created_at_ms, delivered_at_ms
        )
        SELECT gen_random_uuid(), event.event_id, recipient.user_id,
               'delivered', 1, $5, $5, $5
        FROM project_spend_alert_events event
        JOIN (
            SELECT membership.user_id
            FROM identity_project_memberships membership
            WHERE membership.project_id = $1
              AND membership.organization_id = $2
              AND membership.role = 'owner'
              AND membership.state = 'active'

            UNION

            SELECT membership.user_id
            FROM identity_organization_memberships membership
            WHERE membership.organization_id = $2
              AND membership.role = 'owner'
              AND membership.state = 'active'
        ) recipient ON TRUE
        WHERE event.project_id = $1
          AND event.currency = $3
          AND event.period_start_ms = $4
          AND event.budget_control_version = $6
        ON CONFLICT (event_id, recipient_user_id, channel) DO NOTHING
        "#,
    )
    .bind(project_id)
    .bind(&organization_id)
    .bind(&budget.currency)
    .bind(period.start_ms)
    .bind(now_ms)
    .bind(budget.control_version)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn read_notification(
    tx: &mut Transaction<'_, Postgres>,
    recipient_user_id: Uuid,
    delivery_id: Uuid,
) -> Result<NotificationRow, ImageGatewayError> {
    sqlx::query_as::<_, NotificationRow>(
        r#"
        SELECT delivery.delivery_id,
               event.event_id,
               event.project_id,
               project.name AS project_name,
               event.currency,
               event.threshold_percent,
               event.monthly_budget_micros,
               event.spend_micros,
               delivery.created_at_ms,
               delivery.read_at_ms
        FROM project_spend_notification_deliveries delivery
        JOIN project_spend_alert_events event
          ON event.event_id = delivery.event_id
        JOIN gateway_projects project
          ON project.id = event.project_id
         AND project.tenant_id = event.organization_id
        WHERE delivery.delivery_id = $1
          AND delivery.recipient_user_id = $2
          AND delivery.channel = 'in_app'
          AND delivery.state = 'delivered'
        "#,
    )
    .bind(delivery_id)
    .bind(recipient_user_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or_else(notification_not_found)
}

async fn read_budget(
    tx: &mut Transaction<'_, Postgres>,
    project: ProjectIdentity,
    period: BudgetPeriod,
) -> Result<ProjectSpendBudgetView, ImageGatewayError> {
    let budget = sqlx::query_as::<_, BudgetRow>(
        r#"
        SELECT currency, monthly_budget_micros, limit_type, control_version, updated_at_ms
        FROM project_spend_budgets
        WHERE project_id = $1
        "#,
    )
    .bind(&project.project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;

    let Some(budget) = budget else {
        return Ok(ProjectSpendBudgetView {
            object: "organization.project.spend_budget",
            project_id: project.project_id,
            organization_id: project.organization_id,
            configured: false,
            currency: None,
            monthly_budget_micros: None,
            spend_micros: "0".to_string(),
            reserved_micros: "0".to_string(),
            remaining_micros: None,
            usage_basis_points: None,
            limit_type: ProjectSpendLimitType::Soft,
            period_kind: "calendar_month_utc",
            period_start_ms: period.start_ms,
            period_end_ms: period.end_ms,
            alert_thresholds: Vec::new(),
            alert_events: Vec::new(),
            control_version: "0".to_string(),
            updated_at_ms: None,
        });
    };

    let exposure = current_exposure(tx, &project.project_id, &budget.currency, &period)
        .await
        .map_err(unavailable)?;
    let limit_type = match budget.limit_type.as_str() {
        "soft" => ProjectSpendLimitType::Soft,
        "hard" => ProjectSpendLimitType::Hard,
        _ => {
            return Err(ImageGatewayError::service_unavailable(
                "Project spend budget has an invalid limit type",
            ));
        }
    };
    let thresholds = sqlx::query_scalar::<_, i16>(
        r#"
        SELECT threshold_percent
        FROM project_spend_alert_thresholds
        WHERE project_id = $1
        ORDER BY threshold_percent
        "#,
    )
    .bind(&project.project_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    let events = sqlx::query_as::<_, AlertEventRow>(
        r#"
        SELECT event_id, threshold_percent, spend_micros,
               notification_state, created_at_ms
        FROM project_spend_alert_events
        WHERE project_id = $1
          AND currency = $2
          AND period_start_ms = $3
          AND budget_control_version = $4
        ORDER BY threshold_percent, created_at_ms
        "#,
    )
    .bind(&project.project_id)
    .bind(&budget.currency)
    .bind(period.start_ms)
    .bind(budget.control_version)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;

    let exposure_micros = exposure
        .spend_micros
        .saturating_add(exposure.reserved_micros);
    let remaining_micros = budget.monthly_budget_micros.saturating_sub(exposure_micros);
    let usage_basis_points = ((i128::from(exposure_micros) * 10_000)
        / i128::from(budget.monthly_budget_micros))
    .min(i128::from(i64::MAX));

    Ok(ProjectSpendBudgetView {
        object: "organization.project.spend_budget",
        project_id: project.project_id,
        organization_id: project.organization_id,
        configured: true,
        currency: Some(budget.currency),
        monthly_budget_micros: Some(budget.monthly_budget_micros.to_string()),
        spend_micros: exposure.spend_micros.to_string(),
        reserved_micros: exposure.reserved_micros.to_string(),
        remaining_micros: Some(remaining_micros.to_string()),
        usage_basis_points: Some(usage_basis_points.to_string()),
        limit_type,
        period_kind: "calendar_month_utc",
        period_start_ms: period.start_ms,
        period_end_ms: period.end_ms,
        alert_thresholds: thresholds,
        alert_events: events
            .into_iter()
            .map(|event| ProjectSpendAlertEventView {
                event_id: event.event_id,
                threshold_percent: event.threshold_percent,
                spend_micros: event.spend_micros.to_string(),
                notification_state: event.notification_state,
                created_at_ms: event.created_at_ms,
            })
            .collect(),
        control_version: budget.control_version.to_string(),
        updated_at_ms: Some(budget.updated_at_ms),
    })
}

fn normalize_thresholds(mut thresholds: Vec<i16>) -> Result<Vec<i16>, ImageGatewayError> {
    thresholds.push(DEFAULT_ALERT_THRESHOLD_PERCENT);
    thresholds.sort_unstable();
    thresholds.dedup();
    if thresholds.len() > MAX_ALERT_THRESHOLD_COUNT
        || thresholds.iter().any(|value| !(1..=100).contains(value))
    {
        return Err(ImageGatewayError::invalid_request(
            "alert_thresholds must contain at most 20 unique percentages from 1 through 100",
            Some("alert_thresholds".to_string()),
            "invalid_project_spend_alert_thresholds",
        ));
    }
    Ok(thresholds)
}

fn validate_currency(currency: String) -> Result<String, ImageGatewayError> {
    let currency = currency.trim().to_ascii_uppercase();
    if currency.len() != 3 || !currency.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return Err(ImageGatewayError::invalid_request(
            "currency must be a three-letter ISO 4217 code",
            Some("currency".to_string()),
            "invalid_currency",
        ));
    }
    Ok(currency)
}

fn parse_positive_money(value: &str, param: &str) -> Result<i64, ImageGatewayError> {
    let parsed = value.parse::<i64>().map_err(|_| {
        ImageGatewayError::invalid_request(
            format!("{param} must be a positive integer string"),
            Some(param.to_string()),
            "invalid_project_spend_budget",
        )
    })?;
    if parsed <= 0 {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} must be greater than zero"),
            Some(param.to_string()),
            "invalid_project_spend_budget",
        ));
    }
    Ok(parsed)
}

fn parse_nonnegative_i64(value: &str, param: &str) -> Result<i64, ImageGatewayError> {
    let parsed = value.parse::<i64>().map_err(|_| {
        ImageGatewayError::invalid_request(
            format!("{param} must be a non-negative integer string"),
            Some(param.to_string()),
            "invalid_control_version",
        )
    })?;
    if parsed < 0 {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} must be non-negative"),
            Some(param.to_string()),
            "invalid_control_version",
        ));
    }
    Ok(parsed)
}

fn validate_project_id(value: &str) -> Result<(), ImageGatewayError> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b'/')
    {
        return Err(ImageGatewayError::invalid_request(
            "project_id is invalid",
            Some("project_id".to_string()),
            "invalid_identifier",
        ));
    }
    Ok(())
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock is before Unix epoch"))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| ImageGatewayError::internal("system clock is out of range"))
}

fn project_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Project was not found",
        Some("project_id".to_string()),
        "project_not_found",
    )
}

fn notification_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Notification was not found",
        Some("delivery_id".to_string()),
        "notification_not_found",
    )
}

fn version_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Project spend budget changed; reload it before saving",
        Some("expected_control_version".to_string()),
        "project_spend_budget_version_conflict",
    )
}

fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(%error, "project spend budget store unavailable");
    ImageGatewayError::service_unavailable("Project spend budget state is unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thresholds_are_deduplicated_sorted_and_include_one_hundred_percent() {
        assert_eq!(
            normalize_thresholds(vec![90, 50, 90]).unwrap(),
            vec![50, 90, 100]
        );
    }

    #[test]
    fn threshold_validation_rejects_out_of_range_values() {
        assert!(normalize_thresholds(vec![0]).is_err());
        assert!(normalize_thresholds(vec![101]).is_err());
    }

    #[test]
    fn currency_is_normalized_to_uppercase() {
        assert_eq!(validate_currency("usd".to_string()).unwrap(), "USD");
    }
}
