use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgConnection, PgPool, Postgres, Transaction};
use uuid::Uuid;

const DRAFT_SEMANTIC_KEY_PREFIX: &str = "provider-cost-allocation-draft:v1:";
const PREVIEW_HASH_DOMAIN: &[u8] = b"provider-cost-allocation-preview:v1";
const CLOSE_REQUEST_HASH_DOMAIN: &[u8] = b"provider-cost-allocation-close-request:v1";
const CLOSE_IDEMPOTENCY_DOMAIN: &[u8] = b"provider-cost-allocation-close-idempotency:v1";

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProviderCostAllocationError {
    #[error("{message}")]
    InvalidInput {
        message: String,
        field: Option<&'static str>,
    },
    #[error("{message}")]
    Conflict { message: String },
    #[error("provider cost allocation pool was not found")]
    NotFound,
    #[error("provider cost allocation storage is unavailable")]
    Unavailable,
}

impl ProviderCostAllocationError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::InvalidInput { .. } => 400,
            Self::Conflict { .. } => 409,
            Self::NotFound => 404,
            Self::Unavailable => 503,
        }
    }

    fn invalid(field: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
            field: Some(field),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PreviewProviderCostAllocationRequest {
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub price_book_version_id: Uuid,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
    pub currency: String,
    pub total_amount_micros: String,
    pub allocation_basis: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CreateProviderCostAllocationDraftRequest {
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub price_book_version_id: Uuid,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
    pub currency: String,
    pub total_amount_micros: String,
    pub allocation_basis: String,
    pub expected_preview_hash: String,
    pub idempotency_key: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderCostAllocationActor {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CloseProviderCostAllocationRequest {
    pub expected_control_version: i64,
    pub expected_snapshot_hash: String,
    pub source_kind: String,
    pub source_reference: String,
    pub source_evidence_hash: String,
}

impl CreateProviderCostAllocationDraftRequest {
    fn preview_request(&self) -> PreviewProviderCostAllocationRequest {
        PreviewProviderCostAllocationRequest {
            provider_id: self.provider_id.clone(),
            provider_account_id: self.provider_account_id,
            price_book_version_id: self.price_book_version_id,
            period_start_ms: self.period_start_ms,
            period_end_ms: self.period_end_ms,
            currency: self.currency.clone(),
            total_amount_micros: self.total_amount_micros.clone(),
            allocation_basis: self.allocation_basis.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ListProviderCostAllocationsRequest {
    pub provider_id: Option<String>,
    pub provider_account_id: Option<Uuid>,
    pub currency: Option<String>,
    pub state: Option<String>,
    pub after: Option<Uuid>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProviderCostAllocationLinePreview {
    pub job_id: Uuid,
    pub output_id: Option<Uuid>,
    pub basis_receipt_id: Uuid,
    pub basis_receipt_payload_hash: String,
    pub basis_quote_id: Uuid,
    pub basis_quote_hash: String,
    pub basis_quantity: String,
    pub basis_unit: String,
    pub amount_micros: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProviderCostAllocationPreview {
    pub object: &'static str,
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub price_book_version_id: Uuid,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
    pub currency: String,
    pub total_amount_micros: String,
    pub allocation_basis: String,
    pub candidate_count: usize,
    pub allocated_amount_micros: String,
    pub residual_amount_micros: String,
    pub preview_hash: String,
    pub lines: Vec<ProviderCostAllocationLinePreview>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProviderCostAllocationSummary {
    pub object: &'static str,
    pub provider_cost_allocation_pool_id: Uuid,
    pub semantic_key: String,
    pub provider_id: String,
    pub provider_account_id: Uuid,
    pub price_book_version_id: Uuid,
    pub period_start_ms: i64,
    pub period_end_ms: i64,
    pub currency: String,
    pub total_amount_micros: String,
    pub residual_amount_micros: String,
    pub allocated_amount_micros: String,
    pub allocation_basis: String,
    pub state: String,
    pub control_version: i64,
    pub candidate_count: i64,
    pub created_at_ms: i64,
    pub closed_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProviderCostAllocationLine {
    pub provider_cost_allocation_line_id: Uuid,
    pub job_id: Uuid,
    pub output_id: Option<Uuid>,
    pub basis_receipt_id: Uuid,
    pub basis_receipt_payload_hash: String,
    pub basis_quote_id: Uuid,
    pub basis_quote_hash: String,
    pub basis_quantity: String,
    pub basis_unit: String,
    pub amount_micros: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProviderCostAllocationClosure {
    pub source_kind: String,
    pub source_reference: String,
    pub source_evidence_hash: String,
    pub closed_by_user_id: Uuid,
    pub closed_by_session_id: Uuid,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProviderCostAllocationDetail {
    #[serde(flatten)]
    pub pool: ProviderCostAllocationSummary,
    pub preview_hash: String,
    pub lines: Vec<ProviderCostAllocationLine>,
    pub closure: Option<ProviderCostAllocationClosure>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ProviderCostAllocationList {
    pub object: &'static str,
    pub as_of_ms: i64,
    pub data: Vec<ProviderCostAllocationSummary>,
    pub has_more: bool,
    pub next_after: Option<Uuid>,
}

#[async_trait]
pub trait ProviderCostAllocationService: Send + Sync + 'static {
    async fn preview(
        &self,
        request: PreviewProviderCostAllocationRequest,
    ) -> Result<ProviderCostAllocationPreview, ProviderCostAllocationError>;

    async fn list(
        &self,
        request: ListProviderCostAllocationsRequest,
    ) -> Result<ProviderCostAllocationList, ProviderCostAllocationError>;

    async fn get(
        &self,
        pool_id: Uuid,
    ) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError>;

    async fn create_draft(
        &self,
        request: CreateProviderCostAllocationDraftRequest,
    ) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError>;

    async fn close(
        &self,
        pool_id: Uuid,
        idempotency_key: &str,
        actor: ProviderCostAllocationActor,
        request: CloseProviderCostAllocationRequest,
    ) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError>;
}

#[derive(Clone)]
pub struct PostgresProviderCostAllocationService {
    pool: PgPool,
}

impl PostgresProviderCostAllocationService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProviderCostAllocationService for PostgresProviderCostAllocationService {
    async fn preview(
        &self,
        request: PreviewProviderCostAllocationRequest,
    ) -> Result<ProviderCostAllocationPreview, ProviderCostAllocationError> {
        let request = normalize_preview_request(request)?;
        let mut connection = self.pool.acquire().await.map_err(unavailable)?;
        build_preview(&mut connection, &request).await
    }

    async fn list(
        &self,
        request: ListProviderCostAllocationsRequest,
    ) -> Result<ProviderCostAllocationList, ProviderCostAllocationError> {
        let limit = request.limit.unwrap_or(25);
        if !(1..=100).contains(&limit) {
            return Err(ProviderCostAllocationError::invalid(
                "limit",
                "limit must be between 1 and 100",
            ));
        }
        let provider_id = request.provider_id.map(normalize_provider_id).transpose()?;
        let currency = request.currency.map(normalize_currency).transpose()?;
        let state = request.state.map(normalize_state).transpose()?;
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ProviderCostAllocationError::invalid("limit", "limit is too large"))?;

        let as_of_ms = database_now_pool(&self.pool).await?;
        let mut rows = sqlx::query_as::<_, PoolSummaryRow>(
            r#"
            SELECT
                pool.provider_cost_allocation_pool_id,
                pool.semantic_key, pool.provider_id,
                pool.provider_account_id, pool.price_book_version_id,
                pool.period_start_ms, pool.period_end_ms, pool.currency,
                pool.total_amount_micros, pool.residual_amount_micros,
                COALESCE(SUM(line.amount_micros::NUMERIC), 0)::BIGINT
                    AS allocated_amount_micros,
                pool.allocation_basis, pool.state, pool.control_version,
                COUNT(line.provider_cost_allocation_line_id)::BIGINT
                    AS candidate_count,
                pool.candidate_snapshot_hash,
                pool.created_at_ms, pool.closed_at_ms
            FROM provider_cost_allocation_pools pool
            LEFT JOIN provider_cost_allocation_lines line
              ON line.provider_cost_allocation_pool_id =
                 pool.provider_cost_allocation_pool_id
            WHERE ($1::TEXT IS NULL OR pool.provider_id = $1)
              AND ($2::UUID IS NULL OR pool.provider_account_id = $2)
              AND ($3::TEXT IS NULL OR pool.currency = $3)
              AND ($4::TEXT IS NULL OR pool.state = $4)
              AND (
                    $5::UUID IS NULL
                    OR pool.provider_cost_allocation_pool_id > $5
                  )
            GROUP BY pool.provider_cost_allocation_pool_id
            ORDER BY pool.provider_cost_allocation_pool_id
            LIMIT $6
            "#,
        )
        .bind(provider_id)
        .bind(request.provider_account_id)
        .bind(currency)
        .bind(state)
        .bind(request.after)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;

        let has_more = rows.len() > limit;
        if has_more {
            rows.pop();
        }
        let data = rows
            .into_iter()
            .map(PoolSummaryRow::into_summary)
            .collect::<Vec<_>>();
        let next_after = has_more
            .then(|| {
                data.last()
                    .map(|item| item.provider_cost_allocation_pool_id)
            })
            .flatten();
        Ok(ProviderCostAllocationList {
            object: "list",
            as_of_ms,
            data,
            has_more,
            next_after,
        })
    }

    async fn get(
        &self,
        pool_id: Uuid,
    ) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError> {
        load_detail(&self.pool, pool_id).await
    }

    async fn create_draft(
        &self,
        request: CreateProviderCostAllocationDraftRequest,
    ) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_sha256("expected_preview_hash", &request.expected_preview_hash)?;
        let preview_request = normalize_preview_request(request.preview_request())?;
        let semantic_key = draft_semantic_key(&request.idempotency_key);

        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&semantic_key)
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;

        if let Some(existing_id) = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT provider_cost_allocation_pool_id
            FROM provider_cost_allocation_pools
            WHERE semantic_key = $1
            "#,
        )
        .bind(&semantic_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unavailable)?
        {
            let detail = load_detail_connection(&mut transaction, existing_id).await?;
            if request_matches_existing(&preview_request, &request.expected_preview_hash, &detail) {
                transaction.rollback().await.map_err(unavailable)?;
                return Ok(detail);
            }
            return Err(ProviderCostAllocationError::conflict(
                "the idempotency key was already used with a different request",
            ));
        }

        let preview = build_preview(&mut transaction, &preview_request).await?;
        if preview.preview_hash != request.expected_preview_hash {
            return Err(ProviderCostAllocationError::conflict(
                "the candidate set changed after preview",
            ));
        }

        let pool_id = Uuid::new_v4();
        let created_at_ms = database_now_connection(&mut transaction).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_cost_allocation_pools (
                provider_cost_allocation_pool_id, semantic_key,
                provider_id, provider_account_id, price_book_version_id,
                period_start_ms, period_end_ms, currency,
                total_amount_micros, residual_amount_micros,
                allocation_basis, state, control_version,
                candidate_snapshot_hash, created_at_ms, closed_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, 'draft', 1, $12, $13, NULL
            )
            "#,
        )
        .bind(pool_id)
        .bind(&semantic_key)
        .bind(&preview.provider_id)
        .bind(preview.provider_account_id)
        .bind(preview.price_book_version_id)
        .bind(preview.period_start_ms)
        .bind(preview.period_end_ms)
        .bind(&preview.currency)
        .bind(parse_nonnegative_micros(
            "total_amount_micros",
            &preview.total_amount_micros,
        )?)
        .bind(parse_nonnegative_micros(
            "residual_amount_micros",
            &preview.residual_amount_micros,
        )?)
        .bind(&preview.allocation_basis)
        .bind(&preview.preview_hash)
        .bind(created_at_ms)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;

        for line in &preview.lines {
            sqlx::query(
                r#"
                INSERT INTO provider_cost_allocation_lines (
                    provider_cost_allocation_line_id,
                    provider_cost_allocation_pool_id,
                    provider_id, provider_account_id,
                    job_id, output_id, basis_usage_fact_id,
                    basis_receipt_id, basis_receipt_payload_hash,
                    basis_quote_id, basis_quote_hash,
                    basis_quantity, basis_unit,
                    amount_micros, created_at_ms
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, NULL,
                    $7, $8, $9, $10,
                    1, $11, $12, $13
                )
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(pool_id)
            .bind(&preview.provider_id)
            .bind(preview.provider_account_id)
            .bind(line.job_id)
            .bind(line.output_id)
            .bind(line.basis_receipt_id)
            .bind(&line.basis_receipt_payload_hash)
            .bind(line.basis_quote_id)
            .bind(&line.basis_quote_hash)
            .bind(&line.basis_unit)
            .bind(parse_nonnegative_micros(
                "amount_micros",
                &line.amount_micros,
            )?)
            .bind(created_at_ms)
            .execute(&mut *transaction)
            .await
            .map_err(mutation_error)?;
        }

        sqlx::query(
            r#"
            SET CONSTRAINTS
                provider_cost_allocation_pools_validate,
                provider_cost_allocation_lines_validate
            IMMEDIATE
            "#,
        )
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;

        let detail = load_detail_connection(&mut transaction, pool_id).await?;
        transaction.commit().await.map_err(mutation_error)?;
        Ok(detail)
    }

    async fn close(
        &self,
        pool_id: Uuid,
        idempotency_key: &str,
        actor: ProviderCostAllocationActor,
        request: CloseProviderCostAllocationRequest,
    ) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError> {
        validate_idempotency_key(idempotency_key)?;
        let request = normalize_close_request(request)?;
        let idempotency_digest = close_idempotency_digest(pool_id, idempotency_key);
        let request_hash = close_request_hash(pool_id, &request);

        let mut transaction = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("provider-cost-allocation-close:v1:{pool_id}"))
            .execute(&mut *transaction)
            .await
            .map_err(unavailable)?;

        let pool = load_close_pool_for_update(&mut transaction, pool_id).await?;
        if pool.state == "closed" {
            let replay = load_close_replay(&mut transaction, pool_id).await?;
            if replay.idempotency_key_digest != idempotency_digest
                || replay.request_hash != request_hash
            {
                return Err(ProviderCostAllocationError::conflict(
                    "the provider cost allocation was closed by a different request",
                ));
            }
            let detail = load_detail_connection(&mut transaction, pool_id).await?;
            transaction.commit().await.map_err(unavailable)?;
            return Ok(detail);
        }
        if pool.state != "draft"
            || pool.control_version != request.expected_control_version
            || pool.candidate_snapshot_hash != request.expected_snapshot_hash
        {
            return Err(ProviderCostAllocationError::conflict(
                "the provider cost allocation draft changed before close",
            ));
        }
        if pool.allocation_basis != "successful_output" {
            return Err(ProviderCostAllocationError::conflict(
                "only output-based provider cost allocations can be closed",
            ));
        }
        if pool.residual_amount_micros != 0 {
            return Err(ProviderCostAllocationError::conflict(
                "provider cost allocation has an unresolved residual",
            ));
        }

        let lines = load_close_lines_for_update(&mut transaction, pool_id).await?;
        if lines.is_empty() {
            return Err(ProviderCostAllocationError::conflict(
                "provider cost allocation has no eligible outputs",
            ));
        }
        lock_basis_receipts(&mut transaction, &lines).await?;

        let preview_request = PreviewProviderCostAllocationRequest {
            provider_id: pool.provider_id.clone(),
            provider_account_id: pool.provider_account_id,
            price_book_version_id: pool.price_book_version_id,
            period_start_ms: pool.period_start_ms,
            period_end_ms: pool.period_end_ms,
            currency: pool.currency.clone(),
            total_amount_micros: pool.total_amount_micros.to_string(),
            allocation_basis: pool.allocation_basis.clone(),
        };
        let current = build_preview(&mut transaction, &preview_request).await?;
        if current.preview_hash != pool.candidate_snapshot_hash {
            return Err(ProviderCostAllocationError::conflict(
                "provider cost evidence changed after the draft was created",
            ));
        }

        let now = database_now_connection(&mut transaction).await?;
        sqlx::query(
            r#"
            INSERT INTO provider_cost_allocation_closures (
                provider_cost_allocation_pool_id,
                idempotency_key_digest, request_hash,
                candidate_snapshot_hash,
                source_kind, source_reference, source_evidence_hash,
                source_period_start_ms, source_period_end_ms,
                source_currency, source_amount_micros,
                closed_by_user_id, closed_by_session_id, created_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13, $14
            )
            "#,
        )
        .bind(pool_id)
        .bind(&idempotency_digest)
        .bind(&request_hash)
        .bind(&pool.candidate_snapshot_hash)
        .bind(&request.source_kind)
        .bind(&request.source_reference)
        .bind(&request.source_evidence_hash)
        .bind(pool.period_start_ms)
        .bind(pool.period_end_ms)
        .bind(&pool.currency)
        .bind(pool.total_amount_micros)
        .bind(actor.user_id)
        .bind(actor.session_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?;

        let updated = sqlx::query(
            r#"
            UPDATE provider_cost_allocation_pools
            SET state = 'closed',
                control_version = control_version + 1,
                closed_at_ms = $2
            WHERE provider_cost_allocation_pool_id = $1
              AND state = 'draft'
              AND control_version = $3
              AND candidate_snapshot_hash = $4
            "#,
        )
        .bind(pool_id)
        .bind(now)
        .bind(request.expected_control_version)
        .bind(&request.expected_snapshot_hash)
        .execute(&mut *transaction)
        .await
        .map_err(mutation_error)?
        .rows_affected();
        if updated != 1 {
            return Err(ProviderCostAllocationError::conflict(
                "the provider cost allocation draft changed before close",
            ));
        }

        insert_allocation_ledgers(&mut transaction, &pool, &lines, now).await?;
        sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
            .execute(&mut *transaction)
            .await
            .map_err(mutation_error)?;
        verify_closed_allocation(&mut transaction, pool_id).await?;

        let detail = load_detail_connection(&mut transaction, pool_id).await?;
        transaction.commit().await.map_err(mutation_error)?;
        Ok(detail)
    }
}

#[derive(Clone, Debug, FromRow)]
struct PriceVersionContext {
    purpose: String,
    scope_type: String,
    organization_id: Option<String>,
    project_id: Option<String>,
    book_provider_id: Option<String>,
    book_currency: String,
    version_state: String,
    api_profile: String,
    operation: String,
    version_provider_id: Option<String>,
    provider_model_id: Option<String>,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
    execution_surface: String,
    billing_mode: String,
    effective_from_ms: i64,
    effective_until_ms: Option<i64>,
}

#[derive(Clone, Debug, FromRow)]
struct CandidateRow {
    candidate_id: Uuid,
    job_id: Uuid,
    output_id: Option<Uuid>,
    basis_receipt_id: Uuid,
    basis_receipt_payload_hash: String,
    basis_quote_id: Uuid,
    basis_quote_hash: String,
}

#[derive(Clone, Debug, FromRow)]
struct PoolSummaryRow {
    provider_cost_allocation_pool_id: Uuid,
    semantic_key: String,
    provider_id: String,
    provider_account_id: Uuid,
    price_book_version_id: Uuid,
    period_start_ms: i64,
    period_end_ms: i64,
    currency: String,
    total_amount_micros: i64,
    residual_amount_micros: i64,
    allocated_amount_micros: i64,
    allocation_basis: String,
    state: String,
    control_version: i64,
    candidate_count: i64,
    candidate_snapshot_hash: String,
    created_at_ms: i64,
    closed_at_ms: Option<i64>,
}

impl PoolSummaryRow {
    fn into_summary(self) -> ProviderCostAllocationSummary {
        ProviderCostAllocationSummary {
            object: "billing.provider_cost_allocation",
            provider_cost_allocation_pool_id: self.provider_cost_allocation_pool_id,
            semantic_key: self.semantic_key,
            provider_id: self.provider_id,
            provider_account_id: self.provider_account_id,
            price_book_version_id: self.price_book_version_id,
            period_start_ms: self.period_start_ms,
            period_end_ms: self.period_end_ms,
            currency: self.currency,
            total_amount_micros: self.total_amount_micros.to_string(),
            residual_amount_micros: self.residual_amount_micros.to_string(),
            allocated_amount_micros: self.allocated_amount_micros.to_string(),
            allocation_basis: self.allocation_basis,
            state: self.state,
            control_version: self.control_version,
            candidate_count: self.candidate_count,
            created_at_ms: self.created_at_ms,
            closed_at_ms: self.closed_at_ms,
        }
    }
}

#[derive(Clone, Debug, FromRow)]
struct AllocationLineRow {
    provider_cost_allocation_line_id: Uuid,
    job_id: Uuid,
    output_id: Option<Uuid>,
    basis_receipt_id: Uuid,
    basis_receipt_payload_hash: String,
    basis_quote_id: Uuid,
    basis_quote_hash: String,
    basis_quantity: String,
    basis_unit: String,
    amount_micros: i64,
    created_at_ms: i64,
}

#[derive(Clone, Debug, FromRow)]
struct ProviderCostAllocationClosureRow {
    source_kind: String,
    source_reference: String,
    source_evidence_hash: String,
    closed_by_user_id: Uuid,
    closed_by_session_id: Uuid,
    created_at_ms: i64,
}

#[derive(Clone, Debug, FromRow)]
struct ClosePoolRow {
    provider_cost_allocation_pool_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    price_book_version_id: Uuid,
    period_start_ms: i64,
    period_end_ms: i64,
    currency: String,
    total_amount_micros: i64,
    residual_amount_micros: i64,
    allocation_basis: String,
    state: String,
    control_version: i64,
    candidate_snapshot_hash: String,
}

#[derive(Clone, Debug, FromRow)]
struct CloseLineRow {
    provider_cost_allocation_line_id: Uuid,
    job_id: Uuid,
    output_id: Option<Uuid>,
    amount_micros: i64,
    basis_receipt_id: Uuid,
}

#[derive(Clone, Debug, FromRow)]
struct CloseReplayRow {
    idempotency_key_digest: String,
    request_hash: String,
}

#[derive(Clone, Copy, Debug, FromRow)]
struct CloseVerificationRow {
    line_count: i64,
    positive_line_count: i64,
    claim_count: i64,
    ledger_count: i64,
    seal_count: i64,
    settled_obligation_count: i64,
}

impl ProviderCostAllocationClosureRow {
    fn into_closure(self) -> ProviderCostAllocationClosure {
        ProviderCostAllocationClosure {
            source_kind: self.source_kind,
            source_reference: self.source_reference,
            source_evidence_hash: self.source_evidence_hash,
            closed_by_user_id: self.closed_by_user_id,
            closed_by_session_id: self.closed_by_session_id,
            created_at_ms: self.created_at_ms,
        }
    }
}

async fn load_close_pool_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    pool_id: Uuid,
) -> Result<ClosePoolRow, ProviderCostAllocationError> {
    sqlx::query_as(
        r#"
        SELECT provider_cost_allocation_pool_id,
               provider_id, provider_account_id, price_book_version_id,
               period_start_ms, period_end_ms, currency,
               total_amount_micros, residual_amount_micros,
               allocation_basis, state, control_version,
               candidate_snapshot_hash
        FROM provider_cost_allocation_pools
        WHERE provider_cost_allocation_pool_id = $1
        FOR UPDATE
        "#,
    )
    .bind(pool_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderCostAllocationError::NotFound)
}

async fn load_close_replay(
    transaction: &mut Transaction<'_, Postgres>,
    pool_id: Uuid,
) -> Result<CloseReplayRow, ProviderCostAllocationError> {
    sqlx::query_as(
        r#"
        SELECT idempotency_key_digest, request_hash
        FROM provider_cost_allocation_closures
        WHERE provider_cost_allocation_pool_id = $1
        "#,
    )
    .bind(pool_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or_else(|| {
        ProviderCostAllocationError::conflict(
            "closed provider cost allocation lacks close evidence",
        )
    })
}

async fn load_close_lines_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    pool_id: Uuid,
) -> Result<Vec<CloseLineRow>, ProviderCostAllocationError> {
    sqlx::query_as(
        r#"
        SELECT provider_cost_allocation_line_id, job_id, output_id,
               amount_micros, basis_receipt_id
        FROM provider_cost_allocation_lines
        WHERE provider_cost_allocation_pool_id = $1
        ORDER BY basis_receipt_id, provider_cost_allocation_line_id
        FOR UPDATE
        "#,
    )
    .bind(pool_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)
}

async fn lock_basis_receipts(
    transaction: &mut Transaction<'_, Postgres>,
    lines: &[CloseLineRow],
) -> Result<(), ProviderCostAllocationError> {
    let receipt_ids = lines
        .iter()
        .map(|line| line.basis_receipt_id)
        .collect::<Vec<_>>();
    let locked = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT receipt_id
        FROM provider_receipts
        WHERE receipt_id = ANY($1)
        ORDER BY receipt_id
        FOR UPDATE
        "#,
    )
    .bind(&receipt_ids)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    if locked.len() != receipt_ids.len() {
        return Err(ProviderCostAllocationError::conflict(
            "provider cost allocation receipt snapshot is incomplete",
        ));
    }
    Ok(())
}

async fn insert_allocation_ledgers(
    transaction: &mut Transaction<'_, Postgres>,
    pool: &ClosePoolRow,
    lines: &[CloseLineRow],
    now: i64,
) -> Result<(), ProviderCostAllocationError> {
    if !lines.iter().any(|line| line.amount_micros > 0) {
        return Ok(());
    }
    let expense_key = format!("platform:{}:provider-expense", pool.currency);
    let payable_key = format!("provider:{}:{}:payable", pool.provider_id, pool.currency);
    let expense_id = ensure_allocation_ledger_account(
        transaction,
        &expense_key,
        "platform",
        "platform",
        "expense",
        &pool.currency,
        now,
    )
    .await?;
    let payable_id = ensure_allocation_ledger_account(
        transaction,
        &payable_key,
        "provider",
        &pool.provider_id,
        "payable",
        &pool.currency,
        now,
    )
    .await?;

    for line in lines.iter().filter(|line| line.amount_micros > 0) {
        let output_id = line.output_id.ok_or_else(|| {
            ProviderCostAllocationError::conflict("output-based allocation line has no output")
        })?;
        let transaction_id = Uuid::new_v4();
        let semantic_key = format!(
            "provider-cost-allocation-line:v1:{}",
            line.provider_cost_allocation_line_id
        );
        let payload_hash = provider_cost_ledger_payload_hash(
            &semantic_key,
            &pool.currency,
            line.amount_micros,
            &pool.provider_id,
        );
        sqlx::query(
            r#"
            INSERT INTO ledger_transactions (
                transaction_id, semantic_key,
                source_output_id, source_job_id,
                source_provider_cost_allocation_pool_id,
                source_provider_cost_allocation_line_id,
                transaction_type, currency, payload_hash, created_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                'provider_cost', $7, $8, $9
            )
            "#,
        )
        .bind(transaction_id)
        .bind(&semantic_key)
        .bind(output_id)
        .bind(line.job_id)
        .bind(pool.provider_cost_allocation_pool_id)
        .bind(line.provider_cost_allocation_line_id)
        .bind(&pool.currency)
        .bind(&payload_hash)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(mutation_error)?;

        for (posting_no, account_id, amount_micros) in [
            (1_i16, expense_id, line.amount_micros),
            (2_i16, payable_id, -line.amount_micros),
        ] {
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
            .bind(&pool.currency)
            .bind(amount_micros)
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
    }
    Ok(())
}

async fn ensure_allocation_ledger_account(
    transaction: &mut Transaction<'_, Postgres>,
    account_key: &str,
    owner_type: &str,
    owner_id: &str,
    account_type: &str,
    currency: &str,
    now: i64,
) -> Result<Uuid, ProviderCostAllocationError> {
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
    .map_err(mutation_error)?;
    let stored = sqlx::query_as::<_, (Uuid, String, String, String, String)>(
        r#"
        SELECT account_id, owner_type, owner_id, account_type, currency
        FROM ledger_accounts
        WHERE account_key = $1
        FOR SHARE
        "#,
    )
    .bind(account_key)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderCostAllocationError::Unavailable)?;
    if stored.1 != owner_type
        || stored.2 != owner_id
        || stored.3 != account_type
        || stored.4 != currency
    {
        return Err(ProviderCostAllocationError::conflict(
            "provider cost ledger account identity is invalid",
        ));
    }
    Ok(stored.0)
}

async fn verify_closed_allocation(
    transaction: &mut Transaction<'_, Postgres>,
    pool_id: Uuid,
) -> Result<(), ProviderCostAllocationError> {
    let verified = sqlx::query_as::<_, CloseVerificationRow>(
        r#"
        SELECT
            (SELECT COUNT(*)::BIGINT
             FROM provider_cost_allocation_lines line
             WHERE line.provider_cost_allocation_pool_id = $1)
                AS line_count,
            (SELECT COUNT(*)::BIGINT
             FROM provider_cost_allocation_lines line
             WHERE line.provider_cost_allocation_pool_id = $1
               AND line.amount_micros > 0)
                AS positive_line_count,
            (SELECT COUNT(*)::BIGINT
             FROM provider_cost_authority_claims claim
             WHERE claim.source_provider_cost_allocation_pool_id = $1
               AND claim.authority_kind = 'provider_allocated')
                AS claim_count,
            (SELECT COUNT(*)::BIGINT
             FROM ledger_transactions ledger_tx
             WHERE ledger_tx.source_provider_cost_allocation_pool_id = $1
               AND ledger_tx.transaction_type = 'provider_cost')
                AS ledger_count,
            (SELECT COUNT(*)::BIGINT
             FROM ledger_transaction_seals seal
             JOIN ledger_transactions ledger_tx
               ON ledger_tx.transaction_id = seal.transaction_id
             WHERE ledger_tx.source_provider_cost_allocation_pool_id = $1
               AND ledger_tx.transaction_type = 'provider_cost')
                AS seal_count,
            (SELECT COUNT(*)::BIGINT
             FROM provider_cost_obligations obligation
             JOIN provider_cost_authority_claims claim
               ON claim.claim_id = obligation.settlement_claim_id
             WHERE claim.source_provider_cost_allocation_pool_id = $1
               AND obligation.state = 'settled')
                AS settled_obligation_count
        "#,
    )
    .bind(pool_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(unavailable)?;
    if verified.line_count == 0
        || verified.claim_count != verified.line_count
        || verified.ledger_count != verified.positive_line_count
        || verified.seal_count != verified.positive_line_count
        || verified.settled_obligation_count != verified.line_count
    {
        return Err(ProviderCostAllocationError::conflict(
            "provider cost allocation close did not produce exact accounting coverage",
        ));
    }
    Ok(())
}

impl AllocationLineRow {
    fn into_line(self) -> ProviderCostAllocationLine {
        ProviderCostAllocationLine {
            provider_cost_allocation_line_id: self.provider_cost_allocation_line_id,
            job_id: self.job_id,
            output_id: self.output_id,
            basis_receipt_id: self.basis_receipt_id,
            basis_receipt_payload_hash: self.basis_receipt_payload_hash,
            basis_quote_id: self.basis_quote_id,
            basis_quote_hash: self.basis_quote_hash,
            basis_quantity: self.basis_quantity,
            basis_unit: self.basis_unit,
            amount_micros: self.amount_micros.to_string(),
            created_at_ms: self.created_at_ms,
        }
    }
}

async fn build_preview(
    connection: &mut PgConnection,
    request: &PreviewProviderCostAllocationRequest,
) -> Result<ProviderCostAllocationPreview, ProviderCostAllocationError> {
    let now = database_now_connection(connection).await?;
    if request.period_end_ms > now {
        return Err(ProviderCostAllocationError::invalid(
            "period_end_ms",
            "period_end_ms must not be in the future",
        ));
    }
    validate_provider_account(connection, request).await?;
    let context = load_price_version_context(connection, request.price_book_version_id).await?;
    validate_price_version(request, &context)?;
    let candidates = load_candidates(connection, request, &context).await?;
    build_preview_from_candidates(request, candidates)
}

async fn validate_provider_account(
    connection: &mut PgConnection,
    request: &PreviewProviderCostAllocationRequest,
) -> Result<(), ProviderCostAllocationError> {
    let matches: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM provider_accounts
            WHERE provider_account_id = $1
              AND provider_id = $2
        )
        "#,
    )
    .bind(request.provider_account_id)
    .bind(&request.provider_id)
    .fetch_one(connection)
    .await
    .map_err(unavailable)?;
    if !matches {
        return Err(ProviderCostAllocationError::invalid(
            "provider_account_id",
            "provider account does not belong to provider_id",
        ));
    }
    Ok(())
}

async fn load_price_version_context(
    connection: &mut PgConnection,
    price_book_version_id: Uuid,
) -> Result<PriceVersionContext, ProviderCostAllocationError> {
    sqlx::query_as::<_, PriceVersionContext>(
        r#"
        SELECT
            book.purpose, book.scope_type,
            book.organization_id, book.project_id,
            book.provider_id AS book_provider_id,
            book.currency AS book_currency,
            version.state AS version_state,
            version.api_profile, version.operation,
            version.provider_id AS version_provider_id,
            version.provider_model_id, version.public_model_id,
            version.media_kind, version.service_tier,
            version.execution_surface, version.billing_mode,
            version.effective_from_ms, version.effective_until_ms
        FROM price_book_versions version
        JOIN price_books book
          ON book.price_book_id = version.price_book_id
        WHERE version.price_book_version_id = $1
        "#,
    )
    .bind(price_book_version_id)
    .fetch_optional(connection)
    .await
    .map_err(unavailable)?
    .ok_or_else(|| {
        ProviderCostAllocationError::invalid(
            "price_book_version_id",
            "price book version does not exist",
        )
    })
}

fn validate_price_version(
    request: &PreviewProviderCostAllocationRequest,
    context: &PriceVersionContext,
) -> Result<(), ProviderCostAllocationError> {
    let effective_provider = context
        .version_provider_id
        .as_deref()
        .or(context.book_provider_id.as_deref());
    if context.purpose != "provider_allocated"
        || context.billing_mode != "subscription_allocation"
        || !matches!(context.version_state.as_str(), "active" | "retired")
        || effective_provider != Some(request.provider_id.as_str())
        || context.book_currency != request.currency
    {
        return Err(ProviderCostAllocationError::invalid(
            "price_book_version_id",
            "price book version is not a matching provider allocation subscription",
        ));
    }
    if request.period_start_ms < context.effective_from_ms
        || context
            .effective_until_ms
            .is_some_and(|until| request.period_end_ms > until)
    {
        return Err(ProviderCostAllocationError::invalid(
            "price_book_version_id",
            "allocation period is outside the price version effective interval",
        ));
    }
    Ok(())
}

async fn load_candidates(
    connection: &mut PgConnection,
    request: &PreviewProviderCostAllocationRequest,
    context: &PriceVersionContext,
) -> Result<Vec<CandidateRow>, ProviderCostAllocationError> {
    sqlx::query_as::<_, CandidateRow>(
        r#"
        WITH eligible AS (
            SELECT
                CASE
                    WHEN $6 = 'successful_output' THEN receipt.output_id
                    ELSE receipt.job_id
                END AS candidate_id,
                receipt.job_id,
                CASE
                    WHEN $6 = 'successful_output' THEN receipt.output_id
                    ELSE NULL
                END AS output_id,
                receipt.receipt_id AS basis_receipt_id,
                receipt.payload_hash AS basis_receipt_payload_hash,
                quote.quote_id AS basis_quote_id,
                quote.quote_hash AS basis_quote_hash
            FROM provider_receipts receipt
            JOIN provider_submissions submission
              ON submission.submission_id = receipt.submission_id
             AND submission.output_id = receipt.output_id
             AND submission.job_id = receipt.job_id
             AND submission.provider_id = receipt.provider_id
            JOIN customer_price_quotes quote
              ON quote.job_id = receipt.job_id
             AND quote.tenant_id = submission.tenant_id
            WHERE receipt.provider_id = $1
              AND submission.provider_id = $1
              AND submission.provider_account_id = $2
              AND receipt.outcome = 'succeeded'
              AND receipt.created_at_ms >= $3
              AND receipt.created_at_ms < $4
              AND quote.provider_id = $1
              AND (
                    $7 = '*'
                    OR quote.api_profile = $7
                    OR EXISTS (
                        SELECT 1
                        FROM api_profile_pricing_aliases alias
                        WHERE alias.api_profile = quote.api_profile
                          AND alias.pricing_api_profile = $7
                    )
                  )
              AND ($8 = '*' OR quote.operation = $8)
              AND ($9::TEXT IS NULL OR quote.provider_model_id = $9)
              AND ($10 = '*' OR quote.public_model_id = $10)
              AND quote.media_kind = $11
              AND ($12 = '*' OR quote.service_tier = $12)
              AND quote.execution_surface = $13
              AND (
                    $14 = 'platform'
                    OR (
                        $14 = 'organization'
                        AND quote.tenant_id = $15
                    )
                    OR (
                        $14 = 'project'
                        AND quote.tenant_id = $15
                        AND quote.project_id = $16
                    )
                  )
              AND NOT EXISTS (
                    SELECT 1
                    FROM provider_cost_authority_claims claim
                    WHERE claim.source_receipt_id = receipt.receipt_id
                  )
        )
        SELECT DISTINCT ON (candidate_id)
               candidate_id, job_id, output_id,
               basis_receipt_id, basis_receipt_payload_hash,
               basis_quote_id, basis_quote_hash
        FROM eligible
        ORDER BY candidate_id, job_id, output_id NULLS FIRST,
                 basis_receipt_id, basis_quote_id
        "#,
    )
    .bind(&request.provider_id)
    .bind(request.provider_account_id)
    .bind(request.period_start_ms)
    .bind(request.period_end_ms)
    .bind(&request.currency)
    .bind(&request.allocation_basis)
    .bind(&context.api_profile)
    .bind(&context.operation)
    .bind(context.provider_model_id.as_deref())
    .bind(&context.public_model_id)
    .bind(&context.media_kind)
    .bind(&context.service_tier)
    .bind(&context.execution_surface)
    .bind(&context.scope_type)
    .bind(context.organization_id.as_deref())
    .bind(context.project_id.as_deref())
    .fetch_all(connection)
    .await
    .map_err(unavailable)
}

fn build_preview_from_candidates(
    request: &PreviewProviderCostAllocationRequest,
    mut candidates: Vec<CandidateRow>,
) -> Result<ProviderCostAllocationPreview, ProviderCostAllocationError> {
    candidates.sort_by_key(|candidate| candidate.candidate_id);
    let candidate_count = candidates.len();
    let (base, remainder) = if candidate_count == 0 {
        (0_i128, 0_usize)
    } else {
        let count = i128::try_from(candidate_count).map_err(|_| {
            ProviderCostAllocationError::conflict("candidate count exceeds integer capacity")
        })?;
        let total = i128::from(parse_nonnegative_micros(
            "total_amount_micros",
            &request.total_amount_micros,
        )?);
        let base = total / count;
        let remainder = usize::try_from(total % count).map_err(|_| {
            ProviderCostAllocationError::conflict("allocation remainder exceeds integer capacity")
        })?;
        (base, remainder)
    };

    let basis_unit = match request.allocation_basis.as_str() {
        "successful_job" => "job",
        "successful_output" => "output",
        _ => unreachable!("allocation basis was normalized"),
    };
    let mut allocated = 0_i128;
    let mut lines = Vec::with_capacity(candidate_count);
    for (index, candidate) in candidates.into_iter().enumerate() {
        let amount = base + i128::from(index < remainder);
        allocated = allocated
            .checked_add(amount)
            .ok_or_else(|| ProviderCostAllocationError::conflict("allocated amount overflow"))?;
        let amount_micros = i64::try_from(amount)
            .map_err(|_| ProviderCostAllocationError::conflict("line amount exceeds BIGINT"))?;
        lines.push(ProviderCostAllocationLinePreview {
            job_id: candidate.job_id,
            output_id: candidate.output_id,
            basis_receipt_id: candidate.basis_receipt_id,
            basis_receipt_payload_hash: candidate.basis_receipt_payload_hash,
            basis_quote_id: candidate.basis_quote_id,
            basis_quote_hash: candidate.basis_quote_hash,
            basis_quantity: "1".to_string(),
            basis_unit: basis_unit.to_string(),
            amount_micros: amount_micros.to_string(),
        });
    }
    let allocated_amount_micros = i64::try_from(allocated)
        .map_err(|_| ProviderCostAllocationError::conflict("allocated amount exceeds BIGINT"))?;
    let residual_amount_micros =
        parse_nonnegative_micros("total_amount_micros", &request.total_amount_micros)?
            .checked_sub(allocated_amount_micros)
            .ok_or_else(|| ProviderCostAllocationError::conflict("allocation is not conserved"))?;
    let residual_amount_micros = residual_amount_micros.to_string();
    let preview_hash = preview_hash(request, &lines, &residual_amount_micros);
    Ok(ProviderCostAllocationPreview {
        object: "billing.provider_cost_allocation_preview",
        provider_id: request.provider_id.clone(),
        provider_account_id: request.provider_account_id,
        price_book_version_id: request.price_book_version_id,
        period_start_ms: request.period_start_ms,
        period_end_ms: request.period_end_ms,
        currency: request.currency.clone(),
        total_amount_micros: request.total_amount_micros.clone(),
        allocation_basis: request.allocation_basis.clone(),
        candidate_count,
        allocated_amount_micros: allocated_amount_micros.to_string(),
        residual_amount_micros,
        preview_hash,
        lines,
    })
}

async fn load_detail(
    pool: &PgPool,
    pool_id: Uuid,
) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError> {
    let mut connection = pool.acquire().await.map_err(unavailable)?;
    load_detail_connection(&mut connection, pool_id).await
}

async fn load_detail_connection(
    connection: &mut PgConnection,
    pool_id: Uuid,
) -> Result<ProviderCostAllocationDetail, ProviderCostAllocationError> {
    let pool_row = sqlx::query_as::<_, PoolSummaryRow>(
        r#"
        SELECT
            pool.provider_cost_allocation_pool_id,
            pool.semantic_key, pool.provider_id,
            pool.provider_account_id, pool.price_book_version_id,
            pool.period_start_ms, pool.period_end_ms, pool.currency,
            pool.total_amount_micros, pool.residual_amount_micros,
            COALESCE(SUM(line.amount_micros::NUMERIC), 0)::BIGINT
                AS allocated_amount_micros,
            pool.allocation_basis, pool.state, pool.control_version,
            COUNT(line.provider_cost_allocation_line_id)::BIGINT
                AS candidate_count,
            pool.candidate_snapshot_hash,
            pool.created_at_ms, pool.closed_at_ms
        FROM provider_cost_allocation_pools pool
        LEFT JOIN provider_cost_allocation_lines line
          ON line.provider_cost_allocation_pool_id =
             pool.provider_cost_allocation_pool_id
        WHERE pool.provider_cost_allocation_pool_id = $1
        GROUP BY pool.provider_cost_allocation_pool_id
        "#,
    )
    .bind(pool_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderCostAllocationError::NotFound)?;
    let preview_hash = pool_row.candidate_snapshot_hash.clone();
    let pool = pool_row.into_summary();
    let lines = sqlx::query_as::<_, AllocationLineRow>(
        r#"
        SELECT
            provider_cost_allocation_line_id, job_id, output_id,
            basis_receipt_id, basis_receipt_payload_hash,
            basis_quote_id, basis_quote_hash,
            basis_quantity::TEXT AS basis_quantity,
            basis_unit, amount_micros, created_at_ms
        FROM provider_cost_allocation_lines
        WHERE provider_cost_allocation_pool_id = $1
        ORDER BY COALESCE(output_id, job_id), job_id,
                 provider_cost_allocation_line_id
        "#,
    )
    .bind(pool_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(unavailable)?
    .into_iter()
    .map(AllocationLineRow::into_line)
    .collect::<Vec<_>>();
    let closure = sqlx::query_as::<_, ProviderCostAllocationClosureRow>(
        r#"
        SELECT source_kind, source_reference, source_evidence_hash,
               closed_by_user_id, closed_by_session_id, created_at_ms
        FROM provider_cost_allocation_closures
        WHERE provider_cost_allocation_pool_id = $1
        "#,
    )
    .bind(pool_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(unavailable)?
    .map(ProviderCostAllocationClosureRow::into_closure);
    Ok(ProviderCostAllocationDetail {
        pool,
        preview_hash,
        lines,
        closure,
    })
}

fn request_matches_existing(
    request: &PreviewProviderCostAllocationRequest,
    expected_preview_hash: &str,
    detail: &ProviderCostAllocationDetail,
) -> bool {
    detail.pool.provider_id == request.provider_id
        && detail.pool.provider_account_id == request.provider_account_id
        && detail.pool.price_book_version_id == request.price_book_version_id
        && detail.pool.period_start_ms == request.period_start_ms
        && detail.pool.period_end_ms == request.period_end_ms
        && detail.pool.currency == request.currency
        && detail.pool.total_amount_micros == request.total_amount_micros
        && detail.pool.allocation_basis == request.allocation_basis
        && detail.preview_hash == expected_preview_hash
}

fn preview_hash(
    request: &PreviewProviderCostAllocationRequest,
    lines: &[ProviderCostAllocationLinePreview],
    residual_amount_micros: &str,
) -> String {
    let mut digest = Sha256::new();
    hash_field(&mut digest, PREVIEW_HASH_DOMAIN);
    hash_field(&mut digest, request.provider_id.as_bytes());
    hash_field(&mut digest, request.provider_account_id.as_bytes());
    hash_field(&mut digest, request.price_book_version_id.as_bytes());
    hash_field(&mut digest, &request.period_start_ms.to_be_bytes());
    hash_field(&mut digest, &request.period_end_ms.to_be_bytes());
    hash_field(&mut digest, request.currency.as_bytes());
    hash_field(&mut digest, request.total_amount_micros.as_bytes());
    hash_field(&mut digest, request.allocation_basis.as_bytes());
    hash_field(&mut digest, residual_amount_micros.as_bytes());
    hash_field(
        &mut digest,
        &u64::try_from(lines.len()).unwrap_or(u64::MAX).to_be_bytes(),
    );
    for line in lines {
        hash_field(&mut digest, line.job_id.as_bytes());
        match line.output_id {
            Some(output_id) => {
                hash_field(&mut digest, b"output");
                hash_field(&mut digest, output_id.as_bytes());
            }
            None => hash_field(&mut digest, b"job"),
        }
        hash_field(&mut digest, line.basis_receipt_id.as_bytes());
        hash_field(&mut digest, line.basis_receipt_payload_hash.as_bytes());
        hash_field(&mut digest, line.basis_quote_id.as_bytes());
        hash_field(&mut digest, line.basis_quote_hash.as_bytes());
        hash_field(&mut digest, line.basis_quantity.as_bytes());
        hash_field(&mut digest, line.basis_unit.as_bytes());
        hash_field(&mut digest, line.amount_micros.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn draft_semantic_key(idempotency_key: &str) -> String {
    let digest = Sha256::digest(idempotency_key.as_bytes());
    format!("{DRAFT_SEMANTIC_KEY_PREFIX}{}", hex::encode(digest))
}

fn normalize_preview_request(
    mut request: PreviewProviderCostAllocationRequest,
) -> Result<PreviewProviderCostAllocationRequest, ProviderCostAllocationError> {
    request.provider_id = normalize_provider_id(request.provider_id)?;
    request.currency = normalize_currency(request.currency)?;
    request.allocation_basis = normalize_basis(request.allocation_basis)?;
    if request.period_start_ms < 0 {
        return Err(ProviderCostAllocationError::invalid(
            "period_start_ms",
            "period_start_ms must be nonnegative",
        ));
    }
    if request.period_end_ms <= request.period_start_ms {
        return Err(ProviderCostAllocationError::invalid(
            "period_end_ms",
            "period_end_ms must be greater than period_start_ms",
        ));
    }
    request.total_amount_micros =
        parse_nonnegative_micros("total_amount_micros", &request.total_amount_micros)?.to_string();
    Ok(request)
}

fn parse_nonnegative_micros(
    field: &'static str,
    value: &str,
) -> Result<i64, ProviderCostAllocationError> {
    if value.is_empty()
        || value.starts_with('+')
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ProviderCostAllocationError::invalid(
            field,
            format!("{field} must be a canonical nonnegative BIGINT string"),
        ));
    }
    value.parse::<i64>().map_err(|_| {
        ProviderCostAllocationError::invalid(
            field,
            format!("{field} must fit a signed 64-bit integer"),
        )
    })
}

fn normalize_provider_id(value: String) -> Result<String, ProviderCostAllocationError> {
    let value = value.trim().to_string();
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(ProviderCostAllocationError::invalid(
            "provider_id",
            "provider_id is invalid",
        ));
    }
    Ok(value)
}

fn normalize_currency(value: String) -> Result<String, ProviderCostAllocationError> {
    let value = value.trim().to_ascii_uppercase();
    if value.len() != 3 || !value.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return Err(ProviderCostAllocationError::invalid(
            "currency",
            "currency must be a three-letter ISO code",
        ));
    }
    Ok(value)
}

fn normalize_basis(value: String) -> Result<String, ProviderCostAllocationError> {
    let value = value.trim().to_string();
    if !matches!(value.as_str(), "successful_job" | "successful_output") {
        return Err(ProviderCostAllocationError::invalid(
            "allocation_basis",
            "allocation_basis must be successful_job or successful_output",
        ));
    }
    Ok(value)
}

fn normalize_state(value: String) -> Result<String, ProviderCostAllocationError> {
    let value = value.trim().to_ascii_lowercase();
    if !matches!(value.as_str(), "draft" | "closed") {
        return Err(ProviderCostAllocationError::invalid(
            "state",
            "state must be draft or closed",
        ));
    }
    Ok(value)
}

fn validate_idempotency_key(value: &str) -> Result<(), ProviderCostAllocationError> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > 255
        || value.chars().any(char::is_control)
    {
        return Err(ProviderCostAllocationError::invalid(
            "idempotency_key",
            "idempotency_key must contain 1 to 255 visible characters",
        ));
    }
    Ok(())
}

fn validate_sha256(field: &'static str, value: &str) -> Result<(), ProviderCostAllocationError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProviderCostAllocationError::invalid(
            field,
            format!("{field} must be a lowercase SHA-256 digest"),
        ));
    }
    Ok(())
}

fn normalize_close_request(
    mut request: CloseProviderCostAllocationRequest,
) -> Result<CloseProviderCostAllocationRequest, ProviderCostAllocationError> {
    if request.expected_control_version <= 0 {
        return Err(ProviderCostAllocationError::invalid(
            "expected_control_version",
            "expected_control_version must be positive",
        ));
    }
    validate_sha256("expected_snapshot_hash", &request.expected_snapshot_hash)?;
    validate_sha256("source_evidence_hash", &request.source_evidence_hash)?;
    request.source_kind = request.source_kind.trim().to_ascii_lowercase();
    if !matches!(
        request.source_kind.as_str(),
        "provider_invoice" | "provider_contract" | "provider_subscription" | "provider_statement"
    ) {
        return Err(ProviderCostAllocationError::invalid(
            "source_kind",
            "source_kind must identify an upstream invoice, contract, subscription, or statement",
        ));
    }
    request.source_reference = request.source_reference.trim().to_string();
    if request.source_reference.is_empty()
        || request.source_reference.len() > 512
        || request.source_reference.chars().any(char::is_control)
    {
        return Err(ProviderCostAllocationError::invalid(
            "source_reference",
            "source_reference must contain 1 to 512 visible characters",
        ));
    }
    Ok(request)
}

fn close_idempotency_digest(pool_id: Uuid, idempotency_key: &str) -> String {
    let mut digest = Sha256::new();
    hash_field(&mut digest, CLOSE_IDEMPOTENCY_DOMAIN);
    hash_field(&mut digest, pool_id.as_bytes());
    hash_field(&mut digest, idempotency_key.as_bytes());
    hex::encode(digest.finalize())
}

fn close_request_hash(pool_id: Uuid, request: &CloseProviderCostAllocationRequest) -> String {
    let mut digest = Sha256::new();
    hash_field(&mut digest, CLOSE_REQUEST_HASH_DOMAIN);
    hash_field(&mut digest, pool_id.as_bytes());
    hash_field(&mut digest, &request.expected_control_version.to_be_bytes());
    hash_field(&mut digest, request.expected_snapshot_hash.as_bytes());
    hash_field(&mut digest, request.source_kind.as_bytes());
    hash_field(&mut digest, request.source_reference.as_bytes());
    hash_field(&mut digest, request.source_evidence_hash.as_bytes());
    hex::encode(digest.finalize())
}

fn provider_cost_ledger_payload_hash(
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

async fn database_now_pool(pool: &PgPool) -> Result<i64, ProviderCostAllocationError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(unavailable)
}

async fn database_now_connection(
    connection: &mut PgConnection,
) -> Result<i64, ProviderCostAllocationError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(connection)
        .await
        .map_err(unavailable)
}

fn mutation_error(error: sqlx::Error) -> ProviderCostAllocationError {
    match error
        .as_database_error()
        .and_then(|database| database.code())
    {
        Some(code)
            if matches!(
                code.as_ref(),
                "23503" | "23505" | "23514" | "23P01" | "40001" | "40P01" | "55000" | "P0001"
            ) =>
        {
            ProviderCostAllocationError::conflict(
                "provider cost allocation conflicts with current economic facts",
            )
        }
        _ => ProviderCostAllocationError::Unavailable,
    }
}

fn unavailable(_: sqlx::Error) -> ProviderCostAllocationError {
    ProviderCostAllocationError::Unavailable
}
