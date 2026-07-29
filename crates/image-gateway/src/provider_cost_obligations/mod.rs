use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

#[derive(Clone, Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ListProviderCostObligationsRequest {
    pub after: Option<String>,
    pub limit: Option<usize>,
    pub state: Option<String>,
    pub urgency: Option<String>,
    pub provider_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderCostObligationSummary {
    pub open: i64,
    pub overdue: i64,
    pub escalated: i64,
    pub settled: i64,
    pub waived: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct ProviderCostObligationView {
    pub object: &'static str,
    pub receipt_id: String,
    pub submission_id: String,
    pub output_id: String,
    pub job_id: String,
    pub tenant_id: String,
    pub provider_id: String,
    pub provider_account_id: Option<String>,
    pub receipt_outcome: String,
    pub state: String,
    pub urgency: String,
    pub expected_authority_kind: Option<String>,
    pub settlement_claim_id: Option<String>,
    pub currency: Option<String>,
    pub pending_reason_code: Option<String>,
    pub waiver_reason_code: Option<String>,
    pub due_at_ms: i64,
    pub escalate_at_ms: i64,
    pub pending_since_ms: Option<i64>,
    pub last_reviewed_at_ms: Option<i64>,
    pub next_review_at_ms: Option<i64>,
    pub review_attempt_count: i32,
    pub control_version: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub settled_at_ms: Option<i64>,
    pub waived_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct ProviderCostObligationEventView {
    pub event_id: String,
    pub control_version: String,
    pub previous_state: Option<String>,
    pub state: String,
    pub event_kind: String,
    pub details: Value,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct ProviderCostObligationDetail {
    #[serde(flatten)]
    pub obligation: ProviderCostObligationView,
    pub events: Vec<ProviderCostObligationEventView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct ProviderCostObligationList {
    pub object: &'static str,
    pub as_of_ms: i64,
    pub summary: ProviderCostObligationSummary,
    pub data: Vec<ProviderCostObligationView>,
    pub has_more: bool,
    pub next_after: Option<String>,
}

#[async_trait]
pub trait ProviderCostObligationService: Send + Sync + 'static {
    async fn list(
        &self,
        request: ListProviderCostObligationsRequest,
    ) -> Result<ProviderCostObligationList, ImageGatewayError>;

    async fn get(
        &self,
        receipt_id: Uuid,
    ) -> Result<ProviderCostObligationDetail, ImageGatewayError>;
}

#[derive(Clone)]
pub struct PostgresProviderCostObligationService {
    pool: PgPool,
}

impl PostgresProviderCostObligationService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProviderCostObligationService for PostgresProviderCostObligationService {
    async fn list(
        &self,
        request: ListProviderCostObligationsRequest,
    ) -> Result<ProviderCostObligationList, ImageGatewayError> {
        let limit = request.limit.unwrap_or(25);
        if !(1..=100).contains(&limit) {
            return Err(ImageGatewayError::invalid_request(
                "limit must be between 1 and 100",
                Some("limit".to_string()),
                "invalid_limit",
            ));
        }
        let state = normalize_state(request.state.as_deref())?;
        let urgency = normalize_urgency(request.urgency.as_deref())?;
        let provider_id = normalize_provider_id(request.provider_id)?;
        let after = request
            .after
            .as_deref()
            .map(parse_uuid_cursor)
            .transpose()?;
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ImageGatewayError::internal("provider cost page size overflow"))?;
        let as_of_ms = now_ms()?;

        let summary = sqlx::query_as::<_, ProviderCostObligationSummaryRow>(
            r#"
            SELECT
                COUNT(*) FILTER (
                    WHERE state IN ('expected', 'pending')
                )::BIGINT AS open,
                COUNT(*) FILTER (
                    WHERE state IN ('expected', 'pending')
                      AND due_at_ms <= $1
                )::BIGINT AS overdue,
                COUNT(*) FILTER (
                    WHERE state IN ('expected', 'pending')
                      AND escalate_at_ms <= $1
                )::BIGINT AS escalated,
                COUNT(*) FILTER (WHERE state = 'settled')::BIGINT AS settled,
                COUNT(*) FILTER (WHERE state = 'waived')::BIGINT AS waived
            FROM provider_cost_obligations
            "#,
        )
        .bind(as_of_ms)
        .fetch_one(&self.pool)
        .await
        .map_err(unavailable)?
        .into_view();

        let mut rows = sqlx::query_as::<_, ProviderCostObligationRow>(
            r#"
            SELECT obligation.receipt_id, obligation.submission_id,
                   obligation.output_id, obligation.job_id, job.tenant_id,
                   obligation.provider_id, obligation.provider_account_id,
                   receipt.outcome AS receipt_outcome,
                   obligation.state, obligation.expected_authority_kind,
                   obligation.settlement_claim_id, obligation.currency,
                   obligation.pending_reason_code,
                   obligation.waiver_reason_code,
                   obligation.due_at_ms, obligation.escalate_at_ms,
                   obligation.pending_since_ms,
                   obligation.last_reviewed_at_ms,
                   obligation.next_review_at_ms,
                   obligation.review_attempt_count,
                   obligation.control_version,
                   obligation.created_at_ms, obligation.updated_at_ms,
                   obligation.settled_at_ms, obligation.waived_at_ms
            FROM provider_cost_obligations obligation
            JOIN provider_receipts receipt
              ON receipt.receipt_id = obligation.receipt_id
            JOIN jobs job ON job.job_id = obligation.job_id
            LEFT JOIN provider_cost_obligations cursor
              ON cursor.receipt_id = $1
            WHERE (
                    $2 = 'all'
                    OR ($2 = 'open' AND obligation.state IN ('expected', 'pending'))
                    OR obligation.state = $2
                  )
              AND (
                    $3 = 'all'
                    OR ($3 = 'overdue'
                        AND obligation.state IN ('expected', 'pending')
                        AND obligation.due_at_ms <= $4)
                    OR ($3 = 'escalated'
                        AND obligation.state IN ('expected', 'pending')
                        AND obligation.escalate_at_ms <= $4)
                  )
              AND ($5::TEXT IS NULL OR obligation.provider_id = $5)
              AND (
                    $1::UUID IS NULL
                    OR (
                        CASE WHEN obligation.state IN ('expected', 'pending')
                             THEN 0 ELSE 1 END,
                        obligation.due_at_ms,
                        obligation.receipt_id
                    ) > (
                        CASE WHEN cursor.state IN ('expected', 'pending')
                             THEN 0 ELSE 1 END,
                        cursor.due_at_ms,
                        cursor.receipt_id
                    )
                  )
            ORDER BY
                CASE WHEN obligation.state IN ('expected', 'pending')
                     THEN 0 ELSE 1 END,
                obligation.due_at_ms,
                obligation.receipt_id
            LIMIT $6
            "#,
        )
        .bind(after)
        .bind(state)
        .bind(urgency)
        .bind(as_of_ms)
        .bind(provider_id)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_after = has_more
            .then(|| rows.last().map(|row| row.receipt_id.to_string()))
            .flatten();
        Ok(ProviderCostObligationList {
            object: "list",
            as_of_ms,
            summary,
            data: rows
                .into_iter()
                .map(|row| row.into_view(as_of_ms))
                .collect(),
            has_more,
            next_after,
        })
    }

    async fn get(
        &self,
        receipt_id: Uuid,
    ) -> Result<ProviderCostObligationDetail, ImageGatewayError> {
        let as_of_ms = now_ms()?;
        let row = sqlx::query_as::<_, ProviderCostObligationRow>(
            r#"
            SELECT obligation.receipt_id, obligation.submission_id,
                   obligation.output_id, obligation.job_id, job.tenant_id,
                   obligation.provider_id, obligation.provider_account_id,
                   receipt.outcome AS receipt_outcome,
                   obligation.state, obligation.expected_authority_kind,
                   obligation.settlement_claim_id, obligation.currency,
                   obligation.pending_reason_code,
                   obligation.waiver_reason_code,
                   obligation.due_at_ms, obligation.escalate_at_ms,
                   obligation.pending_since_ms,
                   obligation.last_reviewed_at_ms,
                   obligation.next_review_at_ms,
                   obligation.review_attempt_count,
                   obligation.control_version,
                   obligation.created_at_ms, obligation.updated_at_ms,
                   obligation.settled_at_ms, obligation.waived_at_ms
            FROM provider_cost_obligations obligation
            JOIN provider_receipts receipt
              ON receipt.receipt_id = obligation.receipt_id
            JOIN jobs job ON job.job_id = obligation.job_id
            WHERE obligation.receipt_id = $1
            "#,
        )
        .bind(receipt_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Provider cost obligation was not found",
                None,
                "provider_cost_obligation_not_found",
            )
        })?;
        let events = sqlx::query_as::<_, ProviderCostObligationEventRow>(
            r#"
            SELECT event_id, control_version, previous_state,
                   state, event_kind, details, created_at_ms
            FROM provider_cost_obligation_events
            WHERE receipt_id = $1
            ORDER BY event_id
            "#,
        )
        .bind(receipt_id)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(ProviderCostObligationDetail {
            obligation: row.into_view(as_of_ms),
            events: events
                .into_iter()
                .map(ProviderCostObligationEventRow::into_view)
                .collect(),
        })
    }
}

#[derive(FromRow)]
struct ProviderCostObligationSummaryRow {
    open: i64,
    overdue: i64,
    escalated: i64,
    settled: i64,
    waived: i64,
}

impl ProviderCostObligationSummaryRow {
    fn into_view(self) -> ProviderCostObligationSummary {
        ProviderCostObligationSummary {
            open: self.open,
            overdue: self.overdue,
            escalated: self.escalated,
            settled: self.settled,
            waived: self.waived,
        }
    }
}

#[derive(FromRow)]
struct ProviderCostObligationRow {
    receipt_id: Uuid,
    submission_id: Uuid,
    output_id: Uuid,
    job_id: Uuid,
    tenant_id: String,
    provider_id: String,
    provider_account_id: Option<Uuid>,
    receipt_outcome: String,
    state: String,
    expected_authority_kind: Option<String>,
    settlement_claim_id: Option<i64>,
    currency: Option<String>,
    pending_reason_code: Option<String>,
    waiver_reason_code: Option<String>,
    due_at_ms: i64,
    escalate_at_ms: i64,
    pending_since_ms: Option<i64>,
    last_reviewed_at_ms: Option<i64>,
    next_review_at_ms: Option<i64>,
    review_attempt_count: i32,
    control_version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
    settled_at_ms: Option<i64>,
    waived_at_ms: Option<i64>,
}

impl ProviderCostObligationRow {
    fn into_view(self, as_of_ms: i64) -> ProviderCostObligationView {
        let urgency = if matches!(self.state.as_str(), "settled" | "waived") {
            "resolved"
        } else if self.escalate_at_ms <= as_of_ms {
            "escalated"
        } else if self.due_at_ms <= as_of_ms {
            "overdue"
        } else {
            "within_sla"
        };
        ProviderCostObligationView {
            object: "billing.provider_cost_obligation",
            receipt_id: self.receipt_id.to_string(),
            submission_id: self.submission_id.to_string(),
            output_id: self.output_id.to_string(),
            job_id: self.job_id.to_string(),
            tenant_id: self.tenant_id,
            provider_id: self.provider_id,
            provider_account_id: self.provider_account_id.map(|id| id.to_string()),
            receipt_outcome: self.receipt_outcome,
            state: self.state,
            urgency: urgency.to_string(),
            expected_authority_kind: self.expected_authority_kind,
            settlement_claim_id: self.settlement_claim_id.map(|id| id.to_string()),
            currency: self.currency,
            pending_reason_code: self.pending_reason_code,
            waiver_reason_code: self.waiver_reason_code,
            due_at_ms: self.due_at_ms,
            escalate_at_ms: self.escalate_at_ms,
            pending_since_ms: self.pending_since_ms,
            last_reviewed_at_ms: self.last_reviewed_at_ms,
            next_review_at_ms: self.next_review_at_ms,
            review_attempt_count: self.review_attempt_count,
            control_version: self.control_version.to_string(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            settled_at_ms: self.settled_at_ms,
            waived_at_ms: self.waived_at_ms,
        }
    }
}

#[derive(FromRow)]
struct ProviderCostObligationEventRow {
    event_id: i64,
    control_version: i64,
    previous_state: Option<String>,
    state: String,
    event_kind: String,
    details: Value,
    created_at_ms: i64,
}

impl ProviderCostObligationEventRow {
    fn into_view(self) -> ProviderCostObligationEventView {
        ProviderCostObligationEventView {
            event_id: self.event_id.to_string(),
            control_version: self.control_version.to_string(),
            previous_state: self.previous_state,
            state: self.state,
            event_kind: self.event_kind,
            details: self.details,
            created_at_ms: self.created_at_ms,
        }
    }
}

fn normalize_state(state: Option<&str>) -> Result<&str, ImageGatewayError> {
    let state = state.unwrap_or("open");
    if matches!(
        state,
        "all" | "open" | "expected" | "pending" | "settled" | "waived"
    ) {
        Ok(state)
    } else {
        Err(ImageGatewayError::invalid_request(
            "state must be all, open, expected, pending, settled, or waived",
            Some("state".to_string()),
            "invalid_provider_cost_obligation_state",
        ))
    }
}

fn normalize_urgency(urgency: Option<&str>) -> Result<&str, ImageGatewayError> {
    let urgency = urgency.unwrap_or("all");
    if matches!(urgency, "all" | "overdue" | "escalated") {
        Ok(urgency)
    } else {
        Err(ImageGatewayError::invalid_request(
            "urgency must be all, overdue, or escalated",
            Some("urgency".to_string()),
            "invalid_provider_cost_obligation_urgency",
        ))
    }
}

fn normalize_provider_id(provider_id: Option<String>) -> Result<Option<String>, ImageGatewayError> {
    let Some(provider_id) = provider_id else {
        return Ok(None);
    };
    if provider_id.is_empty()
        || provider_id.len() > 128
        || !provider_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(ImageGatewayError::invalid_request(
            "provider_id is invalid",
            Some("provider_id".to_string()),
            "invalid_provider_id",
        ));
    }
    Ok(Some(provider_id))
}

fn parse_uuid_cursor(value: &str) -> Result<Uuid, ImageGatewayError> {
    Uuid::parse_str(value).map_err(|_| {
        ImageGatewayError::invalid_request(
            "after cursor is invalid",
            Some("after".to_string()),
            "invalid_cursor",
        )
    })
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock is before Unix epoch"))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| ImageGatewayError::internal("current timestamp overflow"))
}

fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(?error, "provider cost obligation query failed");
    ImageGatewayError::service_unavailable("Provider cost obligations are unavailable")
}
