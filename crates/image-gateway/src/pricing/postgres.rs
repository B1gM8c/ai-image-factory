use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::ImageGatewayError;

mod official;

use super::{
    ApplyOfficialPriceSnapshotRequest, CreatePriceBookRequest, CreatePriceBookVersionRequest,
    CreatePriceRollbackDraftRequest, OfficialPriceCatalogs, OfficialPriceSnapshotPreview,
    PriceBookCatalog, PriceBookVersionDraft, PriceBookVersionView, PriceBookView,
    PriceComponentDraft, PriceComponentView, PricePreviewRequest, PricePreviewResult,
    PricePublishReadiness, PriceResolutionError, PriceResolutionRequest, PriceResolver,
    PriceRollbackDraftResult, PricingAdminService, PricingCoverageSnapshot, PricingTransitionActor,
    RatingError, ResolvedPriceVersion, TransitionPriceBookVersionRequest,
    UpdatePriceBookVersionRequest, aggregate_provider_reported_cost, rate_usage,
    usd_ticks_to_ledger_micros,
};

const PRICE_RESOLUTION_QUERY: &str = r#"
    WITH requested_profile AS (
        SELECT COALESCE(
            (
                SELECT pricing_api_profile
                FROM api_profile_pricing_aliases
                WHERE api_profile = $6
            ),
            $6
        ) AS pricing_api_profile
    )
    SELECT book.price_book_id, book.price_book_key, book.purpose,
           book.scope_type, book.organization_id, book.project_id,
           book.provider_id AS book_provider_id, book.currency,
           version.price_book_version_id, version.version,
           version.api_profile, version.operation, version.provider_id,
           version.provider_model_id, version.public_model_id,
           version.media_kind, version.service_tier,
           version.execution_surface, version.billing_mode,
           version.is_free, version.state,
           version.effective_from_ms, version.effective_until_ms,
           version.source_kind, version.source_url,
           version.source_checked_at_ms, version.notes,
           version.control_version, version.created_at_ms,
           version.updated_at_ms,
           CASE book.scope_type
               WHEN 'project' THEN 3
               WHEN 'organization' THEN 2
               ELSE 1
           END AS scope_rank,
           CASE WHEN book.provider_id = $4 THEN 1 ELSE 0 END
               AS book_provider_rank,
           CASE WHEN version.provider_id = $4 THEN 1 ELSE 0 END
               AS version_provider_rank,
           CASE
               WHEN version.api_profile = $6 THEN 2
               WHEN version.api_profile = requested_profile.pricing_api_profile THEN 1
               ELSE 0
           END AS api_profile_rank,
           CASE WHEN version.operation = $7 THEN 1 ELSE 0 END
               AS operation_rank,
           CASE WHEN version.provider_model_id = $8 THEN 1 ELSE 0 END
               AS provider_model_rank,
           CASE WHEN version.public_model_id = $9 THEN 1 ELSE 0 END
               AS public_model_rank,
           CASE WHEN version.service_tier = $11 THEN 1 ELSE 0 END
               AS service_tier_rank
    FROM price_books AS book
    JOIN price_book_versions AS version
      ON version.price_book_id = book.price_book_id
    CROSS JOIN requested_profile
    WHERE book.state = 'active'
      AND book.purpose = $1
      AND book.currency = $5
      AND (
          book.scope_type = 'platform'
          OR (
              book.scope_type = 'organization'
              AND book.organization_id = $2
          )
          OR (
              book.scope_type = 'project'
              AND book.organization_id = $2
              AND book.project_id = $3
          )
      )
      AND (
          book.provider_id = $4
          OR book.provider_id IS NULL
      )
      AND (
          version.state = 'active'
          OR ($15 AND version.state = 'retired')
      )
      AND version.effective_from_ms <= $14
      AND (
          version.effective_until_ms IS NULL
          OR $14 < version.effective_until_ms
      )
      AND version.api_profile IN (
          $6,
          requested_profile.pricing_api_profile,
          '*'
      )
      AND (version.operation = $7 OR version.operation = '*')
      AND (version.provider_id = $4 OR version.provider_id IS NULL)
      AND (
          version.provider_model_id = $8
          OR version.provider_model_id IS NULL
      )
      AND (
          version.public_model_id = $9
          OR version.public_model_id = '*'
      )
      AND version.media_kind = $10
      AND (
          version.service_tier = $11
          OR version.service_tier = '*'
      )
      AND version.execution_surface = $12
      AND version.billing_mode = $13
    ORDER BY scope_rank DESC, book_provider_rank DESC,
             version_provider_rank DESC, api_profile_rank DESC,
             operation_rank DESC, provider_model_rank DESC,
             public_model_rank DESC, service_tier_rank DESC,
             book.price_book_id, version.price_book_version_id
"#;

#[derive(Clone)]
pub struct PostgresPricingAdminService {
    pool: PgPool,
}

impl PostgresPricingAdminService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn version(
        &self,
        price_book_version_id: Uuid,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        let row = sqlx::query_as::<_, PriceBookVersionRow>(
            r#"
            SELECT price_book_version_id, price_book_id, version, api_profile,
                   operation, provider_id, provider_model_id, public_model_id,
                   media_kind, service_tier, execution_surface, billing_mode,
                   is_free, state, effective_from_ms,
                   effective_until_ms, source_kind, source_url,
                   source_checked_at_ms, notes, control_version,
                   created_at_ms, updated_at_ms
            FROM price_book_versions
            WHERE price_book_version_id = $1
            "#,
        )
        .bind(price_book_version_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(version_not_found)?;
        let components = component_rows(&self.pool, &[price_book_version_id]).await?;
        Ok(row.into_view(
            components
                .get(&price_book_version_id)
                .cloned()
                .unwrap_or_default(),
        ))
    }
}

#[async_trait]
impl PricingAdminService for PostgresPricingAdminService {
    async fn catalog(&self) -> Result<PriceBookCatalog, ImageGatewayError> {
        let book_rows = sqlx::query_as::<_, PriceBookRow>(
            r#"
            SELECT price_book_id, price_book_key, display_name, purpose,
                   scope_type, organization_id, project_id, provider_id,
                   currency, state, control_version, created_at_ms, updated_at_ms
            FROM price_books
            ORDER BY state, purpose, display_name, price_book_id
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        let version_rows = sqlx::query_as::<_, PriceBookVersionRow>(
            r#"
            SELECT price_book_version_id, price_book_id, version, api_profile,
                   operation, provider_id, provider_model_id, public_model_id,
                   media_kind, service_tier, execution_surface, billing_mode,
                   is_free, state, effective_from_ms,
                   effective_until_ms, source_kind, source_url,
                   source_checked_at_ms, notes, control_version,
                   created_at_ms, updated_at_ms
            FROM price_book_versions
            ORDER BY price_book_id, version DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_unavailable)?;
        let version_ids = version_rows
            .iter()
            .map(|row| row.price_book_version_id)
            .collect::<Vec<_>>();
        let mut components = component_rows(&self.pool, &version_ids).await?;
        let mut versions_by_book: HashMap<Uuid, Vec<PriceBookVersionView>> = HashMap::new();
        for row in version_rows {
            let version_components = components
                .remove(&row.price_book_version_id)
                .unwrap_or_default();
            versions_by_book
                .entry(row.price_book_id)
                .or_default()
                .push(row.into_view(version_components));
        }
        let price_books = book_rows
            .into_iter()
            .map(|row| {
                let versions = versions_by_book
                    .remove(&row.price_book_id)
                    .unwrap_or_default();
                row.into_view(versions)
            })
            .collect();
        Ok(PriceBookCatalog {
            as_of_ms: now_ms()?,
            price_books,
        })
    }

    async fn coverage(&self) -> Result<PricingCoverageSnapshot, ImageGatewayError> {
        let catalog = self.catalog().await?;
        super::coverage::load(&self.pool, &catalog).await
    }

    async fn publish_readiness(
        &self,
        price_book_version_id: Uuid,
    ) -> Result<PricePublishReadiness, ImageGatewayError> {
        let mut connection = self.pool.acquire().await.map_err(store_unavailable)?;
        super::readiness::evaluate_on(&mut connection, price_book_version_id).await
    }

    async fn publish_readiness_as(
        &self,
        price_book_version_id: Uuid,
        actor: PricingTransitionActor,
    ) -> Result<PricePublishReadiness, ImageGatewayError> {
        let mut readiness = self.publish_readiness(price_book_version_id).await?;
        let importer = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT applied_by_user_id
            FROM pricing_source_snapshot_applications
            WHERE price_book_version_id = $1
            LIMIT 1
            "#,
        )
        .bind(price_book_version_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_unavailable)?;
        if importer == Some(actor.user_id) {
            readiness.ready = false;
            readiness
                .blocking_reasons
                .push("maker_checker_required".to_string());
        }
        Ok(readiness)
    }

    async fn create_price_book(
        &self,
        request: CreatePriceBookRequest,
    ) -> Result<PriceBookView, ImageGatewayError> {
        validate_book(&request)?;
        let now = now_ms()?;
        let price_book_id = Uuid::new_v4();
        let row = sqlx::query_as::<_, PriceBookRow>(
            r#"
            INSERT INTO price_books (
                price_book_id, price_book_key, display_name, purpose,
                scope_type, organization_id, project_id, provider_id,
                currency, state, control_version, created_at_ms, updated_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                    'active', 1, $10, $10)
            RETURNING price_book_id, price_book_key, display_name, purpose,
                      scope_type, organization_id, project_id, provider_id,
                      currency, state, control_version, created_at_ms, updated_at_ms
            "#,
        )
        .bind(price_book_id)
        .bind(request.price_book_key)
        .bind(request.display_name)
        .bind(request.purpose)
        .bind(request.scope_type)
        .bind(request.organization_id)
        .bind(request.project_id)
        .bind(request.provider_id)
        .bind(request.currency)
        .bind(now)
        .fetch_one(&self.pool)
        .await
        .map_err(map_book_write)?;
        Ok(row.into_view(Vec::new()))
    }

    async fn create_version(
        &self,
        price_book_id: Uuid,
        request: CreatePriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        validate_draft(&request.draft)?;
        let mut transaction = self.pool.begin().await.map_err(store_unavailable)?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM price_books WHERE price_book_id = $1 AND state = 'active' FOR UPDATE)",
        )
        .bind(price_book_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_unavailable)?;
        if !exists {
            return Err(book_not_found());
        }
        let version: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(version), 0)::INTEGER + 1 FROM price_book_versions WHERE price_book_id = $1",
        )
        .bind(price_book_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_unavailable)?;
        let version_id = Uuid::new_v4();
        insert_version_row(
            &mut transaction,
            version_id,
            price_book_id,
            version,
            &request.draft,
        )
        .await?;
        replace_components(&mut transaction, version_id, &request.draft.components).await?;
        transaction.commit().await.map_err(store_unavailable)?;
        self.version(version_id).await
    }

    async fn update_draft_version(
        &self,
        price_book_version_id: Uuid,
        request: UpdatePriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        validate_control_version(request.expected_control_version)?;
        validate_draft(&request.draft)?;
        let now = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(store_unavailable)?;
        let updated = sqlx::query(
            r#"
            UPDATE price_book_versions
            SET api_profile = $2, operation = $3, provider_id = $4,
                provider_model_id = $5, public_model_id = $6,
                media_kind = $7, service_tier = $8,
                execution_surface = $9, billing_mode = $10, is_free = $11,
                effective_from_ms = $12, source_kind = $13, source_url = $14,
                source_checked_at_ms = $15, notes = $16,
                control_version = control_version + 1, updated_at_ms = $17
            WHERE price_book_version_id = $1
              AND state = 'draft'
              AND control_version = $18
            "#,
        )
        .bind(price_book_version_id)
        .bind(&request.draft.api_profile)
        .bind(&request.draft.operation)
        .bind(&request.draft.provider_id)
        .bind(&request.draft.provider_model_id)
        .bind(&request.draft.public_model_id)
        .bind(&request.draft.media_kind)
        .bind(&request.draft.service_tier)
        .bind(&request.draft.execution_surface)
        .bind(&request.draft.billing_mode)
        .bind(request.draft.is_free)
        .bind(request.draft.effective_from_ms)
        .bind(&request.draft.source_kind)
        .bind(&request.draft.source_url)
        .bind(request.draft.source_checked_at_ms)
        .bind(&request.draft.notes)
        .bind(now)
        .bind(request.expected_control_version)
        .execute(&mut *transaction)
        .await
        .map_err(map_version_write)?;
        if updated.rows_affected() != 1 {
            return Err(version_conflict());
        }
        sqlx::query("DELETE FROM price_components WHERE price_book_version_id = $1")
            .bind(price_book_version_id)
            .execute(&mut *transaction)
            .await
            .map_err(store_unavailable)?;
        replace_components(
            &mut transaction,
            price_book_version_id,
            &request.draft.components,
        )
        .await?;
        transaction.commit().await.map_err(store_unavailable)?;
        self.version(price_book_version_id).await
    }

    async fn publish_version(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        self.publish_version_internal(price_book_version_id, request, None)
            .await
    }

    async fn publish_version_as(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
        actor: PricingTransitionActor,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        self.publish_version_internal(price_book_version_id, request, Some(actor))
            .await
    }

    async fn retire_version(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        self.retire_version_internal(price_book_version_id, request, None)
            .await
    }

    async fn retire_version_as(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
        actor: PricingTransitionActor,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        self.retire_version_internal(price_book_version_id, request, Some(actor))
            .await
    }

    async fn create_rollback_draft(
        &self,
        source_version_id: Uuid,
        request: CreatePriceRollbackDraftRequest,
        actor: PricingTransitionActor,
    ) -> Result<PriceRollbackDraftResult, ImageGatewayError> {
        self.create_rollback_draft_internal(source_version_id, request, actor)
            .await
    }

    async fn preview(
        &self,
        request: PricePreviewRequest,
    ) -> Result<PricePreviewResult, ImageGatewayError> {
        let resolved = self
            .resolve_price_version(&request.resolution)
            .await
            .map_err(map_resolution)?;
        if resolved.version.billing_mode == "provider_reported" {
            let native_cost = aggregate_provider_reported_cost(&resolved, &request.usage_facts)
                .map_err(map_rating)?;
            let ledger_conversion = usd_ticks_to_ledger_micros(&native_cost).map_err(map_rating)?;
            return Ok(PricePreviewResult {
                price_book_version_id: resolved.version.price_book_version_id,
                purpose: resolved.purpose,
                is_simulation: true,
                billing_mode: resolved.version.billing_mode,
                currency: resolved.currency,
                fact_set_hash: native_cost.fact_set_hash.clone(),
                total_amount_micros: Some(ledger_conversion.amount_micros.clone()),
                native_cost: Some(native_cost),
                ledger_conversion: Some(ledger_conversion),
                lines: Vec::new(),
            });
        }

        let rating = rate_usage(&resolved, &request.usage_facts).map_err(map_rating)?;
        Ok(PricePreviewResult {
            price_book_version_id: rating.price_book_version_id,
            purpose: resolved.purpose,
            is_simulation: true,
            billing_mode: resolved.version.billing_mode,
            currency: rating.currency,
            fact_set_hash: rating.fact_set_hash,
            total_amount_micros: Some(rating.total_amount_micros),
            native_cost: None,
            ledger_conversion: None,
            lines: rating.lines,
        })
    }

    async fn official_catalogs(&self) -> Result<OfficialPriceCatalogs, ImageGatewayError> {
        official::catalogs(&self.pool).await
    }

    async fn observe_official_catalog(
        &self,
        catalog_key: &str,
        actor_user_id: Uuid,
        actor_session_id: Uuid,
    ) -> Result<OfficialPriceSnapshotPreview, ImageGatewayError> {
        official::observe_catalog(&self.pool, catalog_key, actor_user_id, actor_session_id).await
    }

    async fn apply_official_snapshot(
        &self,
        snapshot_id: Uuid,
        request: ApplyOfficialPriceSnapshotRequest,
        actor_user_id: Uuid,
        actor_session_id: Uuid,
    ) -> Result<OfficialPriceSnapshotPreview, ImageGatewayError> {
        official::apply_snapshot(
            &self.pool,
            snapshot_id,
            request,
            actor_user_id,
            actor_session_id,
        )
        .await
    }
}

impl PostgresPricingAdminService {
    async fn create_rollback_draft_internal(
        &self,
        source_version_id: Uuid,
        request: CreatePriceRollbackDraftRequest,
        actor: PricingTransitionActor,
    ) -> Result<PriceRollbackDraftResult, ImageGatewayError> {
        let now = now_ms()?;
        if request.effective_from_ms <= 0 {
            return Err(invalid(
                "effective_from_ms",
                "Rollback effective time must be positive",
            ));
        }
        let effective_from_ms = request.effective_from_ms.max(now);
        let source = self.version(source_version_id).await?;
        if source.state == "draft" {
            return Err(ImageGatewayError::conflict(
                "Only immutable published price versions can be used as rollback sources",
                Some("source_version_id".to_string()),
                "rollback_source_not_published",
            ));
        }
        let draft = rollback_draft(&source, effective_from_ms);

        let mut transaction = self.pool.begin().await.map_err(store_unavailable)?;
        let source_book_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT price_book_id
            FROM price_book_versions
            WHERE price_book_version_id = $1
              AND state IN ('active', 'retired')
            FOR SHARE
            "#,
        )
        .bind(source_version_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(version_conflict)?;
        sqlx::query("SELECT price_book_id FROM price_books WHERE price_book_id = $1 FOR UPDATE")
            .bind(source_book_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(store_unavailable)?;
        let version: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(version), 0)::INTEGER + 1 FROM price_book_versions WHERE price_book_id = $1",
        )
        .bind(source_book_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_unavailable)?;
        let rollback_version_id = Uuid::new_v4();
        insert_version_row(
            &mut transaction,
            rollback_version_id,
            source_book_id,
            version,
            &draft,
        )
        .await?;
        replace_components(&mut transaction, rollback_version_id, &draft.components).await?;
        sqlx::query(
            r#"
            INSERT INTO price_book_version_rollbacks (
                rollback_version_id, source_version_id,
                created_by_user_id, created_by_session_id, created_at_ms
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(rollback_version_id)
        .bind(source_version_id)
        .bind(actor.user_id)
        .bind(actor.session_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(store_unavailable)?;
        insert_price_transition_audit(
            &mut transaction,
            actor,
            "pricing.price_book_version.rollback_draft.create",
            rollback_version_id,
            "success",
            None,
            serde_json::json!({
                "source_version_id": source_version_id,
                "effective_from_ms": effective_from_ms,
            }),
            now,
        )
        .await?;
        transaction.commit().await.map_err(store_unavailable)?;

        Ok(PriceRollbackDraftResult {
            source_version_id,
            draft: self.version(rollback_version_id).await?,
        })
    }

    async fn publish_version_internal(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
        actor: Option<PricingTransitionActor>,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        validate_control_version(request.expected_control_version)?;
        let now = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(store_unavailable)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await
            .map_err(store_unavailable)?;
        let official_origin =
            official_import_origin(&mut transaction, price_book_version_id).await?;
        if let (Some(actor), Some((snapshot_id, importer_user_id))) = (actor, official_origin)
            && actor.user_id == importer_user_id
        {
            insert_price_transition_audit(
                &mut transaction,
                actor,
                "pricing.price_book_version.publish",
                price_book_version_id,
                "denied",
                Some("maker_checker_required"),
                serde_json::json!({
                    "expected_control_version": request.expected_control_version,
                    "source_snapshot_id": snapshot_id,
                    "importer_user_id": importer_user_id,
                }),
                now,
            )
            .await?;
            transaction.commit().await.map_err(store_unavailable)?;
            return Err(ImageGatewayError::conflict(
                "Official pricing drafts must be reviewed and published by another platform owner",
                Some("price_book_version_id".to_string()),
                "maker_checker_required",
            ));
        }
        let (selector_lock_key, purpose, requested_effective_from_ms) =
            sqlx::query_as::<_, (i64, String, i64)>(
                r#"
            SELECT hashtextextended(
                jsonb_build_array(
                    version.price_book_id, version.api_profile,
                    version.operation, version.provider_id,
                    version.provider_model_id, version.public_model_id,
                    version.media_kind, version.service_tier,
                    version.execution_surface, version.billing_mode
                )::TEXT,
                0
            ),
            book.purpose,
            version.effective_from_ms
            FROM price_book_versions version
            JOIN price_books book
              ON book.price_book_id = version.price_book_id
            WHERE version.price_book_version_id = $1
              AND version.state = 'draft'
              AND version.control_version = $2
            FOR UPDATE OF version
            "#,
            )
            .bind(price_book_version_id)
            .bind(request.expected_control_version)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_unavailable)?
            .ok_or_else(version_conflict)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(selector_lock_key)
            .execute(&mut *transaction)
            .await
            .map_err(store_unavailable)?;
        let effective_from_ms = if purpose == "customer_sale" {
            requested_effective_from_ms.max(now)
        } else {
            requested_effective_from_ms
        };
        if effective_from_ms != requested_effective_from_ms {
            let clamped = sqlx::query(
                r#"
                UPDATE price_book_versions
                SET effective_from_ms = $2,
                    updated_at_ms = $2
                WHERE price_book_version_id = $1
                  AND state = 'draft'
                  AND control_version = $3
                "#,
            )
            .bind(price_book_version_id)
            .bind(effective_from_ms)
            .bind(request.expected_control_version)
            .execute(&mut *transaction)
            .await
            .map_err(store_unavailable)?;
            if clamped.rows_affected() != 1 {
                return Err(version_conflict());
            }
        }

        let readiness =
            super::readiness::evaluate_on(&mut transaction, price_book_version_id).await?;
        if !readiness.ready {
            return Err(invalid(
                "price_book_version_id",
                format!(
                    "Price book version is not publishable: {}",
                    readiness.blocking_reasons.join(", ")
                ),
            ));
        }
        let surface_contracts =
            super::readiness::binding_snapshots_on(&mut transaction, price_book_version_id).await?;
        persist_surface_contract_bindings(
            &mut transaction,
            price_book_version_id,
            &surface_contracts,
            now,
        )
        .await?;

        sqlx::query(
            r#"
            WITH draft AS (
                SELECT price_book_id, api_profile, operation, provider_id,
                       provider_model_id, public_model_id, media_kind,
                       service_tier, execution_surface, billing_mode,
                       effective_from_ms
                FROM price_book_versions
                WHERE price_book_version_id = $1
            ),
            predecessor AS (
                SELECT current.price_book_version_id
                FROM price_book_versions AS current
                CROSS JOIN draft
                WHERE current.state = 'active'
                  AND current.price_book_id = draft.price_book_id
                  AND ROW(
                      current.api_profile, current.operation,
                      current.provider_id, current.provider_model_id,
                      current.public_model_id, current.media_kind,
                      current.service_tier, current.execution_surface,
                      current.billing_mode
                  ) IS NOT DISTINCT FROM ROW(
                      draft.api_profile, draft.operation,
                      draft.provider_id, draft.provider_model_id,
                      draft.public_model_id, draft.media_kind,
                      draft.service_tier, draft.execution_surface,
                      draft.billing_mode
                  )
                  AND current.effective_from_ms < draft.effective_from_ms
                ORDER BY current.effective_from_ms DESC
                LIMIT 1
                FOR UPDATE OF current
            )
            UPDATE price_book_versions AS current
            SET effective_until_ms = draft.effective_from_ms,
                control_version = current.control_version + 1,
                updated_at_ms = $2
            FROM draft, predecessor
            WHERE current.price_book_version_id =
                  predecessor.price_book_version_id
              AND (
                  current.effective_until_ms IS NULL
                  OR current.effective_until_ms > draft.effective_from_ms
              )
            "#,
        )
        .bind(price_book_version_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(map_publish)?;

        let successor_start = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            WITH draft AS (
                SELECT price_book_id, api_profile, operation, provider_id,
                       provider_model_id, public_model_id, media_kind,
                       service_tier, execution_surface, billing_mode,
                       effective_from_ms
                FROM price_book_versions
                WHERE price_book_version_id = $1
            )
            SELECT MIN(current.effective_from_ms)
            FROM price_book_versions AS current
            CROSS JOIN draft
            WHERE current.state = 'active'
              AND current.price_book_id = draft.price_book_id
              AND ROW(
                  current.api_profile, current.operation,
                  current.provider_id, current.provider_model_id,
                  current.public_model_id, current.media_kind,
                  current.service_tier, current.execution_surface,
                  current.billing_mode
              ) IS NOT DISTINCT FROM ROW(
                  draft.api_profile, draft.operation,
                  draft.provider_id, draft.provider_model_id,
                  draft.public_model_id, draft.media_kind,
                  draft.service_tier, draft.execution_surface,
                  draft.billing_mode
              )
              AND current.effective_from_ms > draft.effective_from_ms
            "#,
        )
        .bind(price_book_version_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(store_unavailable)?;
        let updated = sqlx::query(
            r#"
            UPDATE price_book_versions AS version
            SET state = 'active', control_version = control_version + 1,
                effective_until_ms = $4, updated_at_ms = $2
            WHERE version.price_book_version_id = $1
              AND version.state = 'draft'
              AND version.control_version = $3
              AND (
                  version.billing_mode = 'provider_reported'
                  OR EXISTS (
                      SELECT 1 FROM price_components AS component
                      WHERE component.price_book_version_id =
                            version.price_book_version_id
                  )
              )
            "#,
        )
        .bind(price_book_version_id)
        .bind(now)
        .bind(request.expected_control_version)
        .bind(successor_start)
        .execute(&mut *transaction)
        .await
        .map_err(map_publish)?;
        if updated.rows_affected() != 1 {
            return Err(version_conflict());
        }
        if let Some(actor) = actor {
            insert_price_transition_audit(
                &mut transaction,
                actor,
                "pricing.price_book_version.publish",
                price_book_version_id,
                "success",
                None,
                serde_json::json!({
                    "expected_control_version": request.expected_control_version,
                    "requested_effective_from_ms": requested_effective_from_ms,
                    "effective_from_ms": effective_from_ms,
                    "published_at_ms": now,
                    "source_snapshot_id": official_origin.map(|origin| origin.0),
                }),
                now,
            )
            .await?;
        }
        transaction.commit().await.map_err(store_unavailable)?;
        self.version(price_book_version_id).await
    }

    async fn retire_version_internal(
        &self,
        price_book_version_id: Uuid,
        request: TransitionPriceBookVersionRequest,
        actor: Option<PricingTransitionActor>,
    ) -> Result<PriceBookVersionView, ImageGatewayError> {
        validate_control_version(request.expected_control_version)?;
        let now = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(store_unavailable)?;
        let (effective_from_ms, successor_start, selector_lock_key) =
            sqlx::query_as::<_, (i64, Option<i64>, i64)>(
                r#"
                SELECT effective_from_ms, effective_until_ms,
                       hashtextextended(
                           jsonb_build_array(
                               price_book_id, api_profile, operation, provider_id,
                               provider_model_id, public_model_id, media_kind,
                               service_tier, execution_surface, billing_mode
                           )::TEXT,
                           0
                       )
                FROM price_book_versions
                WHERE price_book_version_id = $1
                  AND state = 'active'
                  AND control_version = $2
                FOR UPDATE
                "#,
            )
            .bind(price_book_version_id)
            .bind(request.expected_control_version)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(store_unavailable)?
            .ok_or_else(version_conflict)?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(selector_lock_key)
            .execute(&mut *transaction)
            .await
            .map_err(store_unavailable)?;
        let currently_effective =
            effective_from_ms <= now && successor_start.is_none_or(|until_ms| now < until_ms);
        if currently_effective {
            if let Some(actor) = actor {
                insert_price_transition_audit(
                    &mut transaction,
                    actor,
                    "pricing.price_book_version.retire",
                    price_book_version_id,
                    "denied",
                    Some("replacement_price_required"),
                    serde_json::json!({
                        "expected_control_version": request.expected_control_version,
                        "effective_from_ms": effective_from_ms,
                        "effective_until_ms": successor_start,
                    }),
                    now,
                )
                .await?;
                transaction.commit().await.map_err(store_unavailable)?;
            }
            return Err(ImageGatewayError::conflict(
                "The currently effective price must be replaced by a reviewed version; direct retirement would create a pricing gap",
                Some("price_book_version_id".to_string()),
                "replacement_price_required",
            ));
        }

        let updated = sqlx::query(
            r#"
            UPDATE price_book_versions
            SET state = 'retired',
                effective_until_ms = CASE
                    WHEN effective_until_ms IS NOT NULL
                         AND effective_until_ms <= $2
                    THEN effective_until_ms
                    ELSE GREATEST($2, effective_from_ms + 1)
                END,
                control_version = control_version + 1,
                updated_at_ms = $2
            WHERE price_book_version_id = $1
              AND state = 'active'
              AND control_version = $3
            "#,
        )
        .bind(price_book_version_id)
        .bind(now)
        .bind(request.expected_control_version)
        .execute(&mut *transaction)
        .await
        .map_err(map_version_write)?;
        if updated.rows_affected() != 1 {
            return Err(version_conflict());
        }

        if effective_from_ms > now {
            sqlx::query(
                r#"
                WITH target AS (
                    SELECT price_book_id, api_profile, operation, provider_id,
                           provider_model_id, public_model_id, media_kind,
                           service_tier, execution_surface, billing_mode,
                           effective_from_ms
                    FROM price_book_versions
                    WHERE price_book_version_id = $1
                ),
                predecessor AS (
                    SELECT current.price_book_version_id
                    FROM price_book_versions AS current
                    CROSS JOIN target
                    WHERE current.state = 'active'
                      AND current.price_book_id = target.price_book_id
                      AND ROW(
                          current.api_profile, current.operation,
                          current.provider_id, current.provider_model_id,
                          current.public_model_id, current.media_kind,
                          current.service_tier, current.execution_surface,
                          current.billing_mode
                      ) IS NOT DISTINCT FROM ROW(
                          target.api_profile, target.operation,
                          target.provider_id, target.provider_model_id,
                          target.public_model_id, target.media_kind,
                          target.service_tier, target.execution_surface,
                          target.billing_mode
                      )
                      AND current.effective_from_ms < target.effective_from_ms
                    ORDER BY current.effective_from_ms DESC
                    LIMIT 1
                    FOR UPDATE OF current
                )
                UPDATE price_book_versions AS current
                SET effective_until_ms = $2,
                    control_version = current.control_version + 1,
                    updated_at_ms = $3
                FROM predecessor
                WHERE current.price_book_version_id =
                      predecessor.price_book_version_id
                "#,
            )
            .bind(price_book_version_id)
            .bind(successor_start)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(map_publish)?;
        }
        if let Some(actor) = actor {
            insert_price_transition_audit(
                &mut transaction,
                actor,
                if effective_from_ms > now {
                    "pricing.price_book_version.cancel_schedule"
                } else {
                    "pricing.price_book_version.retire"
                },
                price_book_version_id,
                "success",
                None,
                serde_json::json!({
                    "expected_control_version": request.expected_control_version,
                    "effective_from_ms": effective_from_ms,
                    "effective_until_ms": successor_start,
                }),
                now,
            )
            .await?;
        }
        transaction.commit().await.map_err(store_unavailable)?;
        self.version(price_book_version_id).await
    }
}

async fn insert_price_transition_audit(
    transaction: &mut Transaction<'_, Postgres>,
    actor: PricingTransitionActor,
    action: &str,
    price_book_version_id: Uuid,
    outcome: &str,
    reason_code: Option<&str>,
    metadata: Value,
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
            $1, $2, $3, NULL, $4, 'price_book_version',
            $5, $6, $7, $8, $9
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor.user_id)
    .bind(actor.session_id)
    .bind(action)
    .bind(price_book_version_id.to_string())
    .bind(outcome)
    .bind(reason_code)
    .bind(metadata)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(store_unavailable)?;
    Ok(())
}

async fn official_import_origin(
    transaction: &mut Transaction<'_, Postgres>,
    price_book_version_id: Uuid,
) -> Result<Option<(Uuid, Uuid)>, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT snapshot_id, applied_by_user_id
        FROM pricing_source_snapshot_applications
        WHERE price_book_version_id = $1
        LIMIT 1
        "#,
    )
    .bind(price_book_version_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(store_unavailable)
}

#[async_trait]
impl PriceResolver for PostgresPricingAdminService {
    async fn resolve_price_version(
        &self,
        request: &PriceResolutionRequest,
    ) -> Result<ResolvedPriceVersion, PriceResolutionError> {
        validate_resolution_request(request)?;
        let rows = sqlx::query_as::<_, PriceResolutionRow>(PRICE_RESOLUTION_QUERY)
            .bind(&request.purpose)
            .bind(&request.organization_id)
            .bind(&request.project_id)
            .bind(&request.provider_id)
            .bind(&request.currency)
            .bind(&request.api_profile)
            .bind(&request.operation)
            .bind(&request.provider_model_id)
            .bind(&request.public_model_id)
            .bind(&request.media_kind)
            .bind(&request.service_tier)
            .bind(&request.execution_surface)
            .bind(&request.billing_mode)
            .bind(request.at_ms)
            .bind(false)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| PriceResolutionError::StoreUnavailable)?;
        let selected = select_resolution_row(&rows)?;
        let selected_id = selected.price_book_version_id;
        let mut components = component_rows(&self.pool, &[selected_id])
            .await
            .map_err(|_| PriceResolutionError::StoreUnavailable)?;
        let version_components = components.remove(&selected_id).unwrap_or_default();
        Ok(selected.clone().into_resolution(version_components))
    }
}

pub(crate) async fn resolve_price_version_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    request: &PriceResolutionRequest,
) -> Result<ResolvedPriceVersion, PriceResolutionError> {
    resolve_price_version_in_transaction_with_history(transaction, request, false).await
}

pub(crate) async fn resolve_provider_actual_price_version_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    request: &PriceResolutionRequest,
) -> Result<ResolvedPriceVersion, PriceResolutionError> {
    if request.purpose != "provider_actual" || request.billing_mode != "provider_reported" {
        return Err(PriceResolutionError::InvalidRequest);
    }
    resolve_price_version_in_transaction_with_history(transaction, request, true).await
}

async fn resolve_price_version_in_transaction_with_history(
    transaction: &mut Transaction<'_, Postgres>,
    request: &PriceResolutionRequest,
    include_retired: bool,
) -> Result<ResolvedPriceVersion, PriceResolutionError> {
    validate_resolution_request(request)?;
    let rows = sqlx::query_as::<_, PriceResolutionRow>(PRICE_RESOLUTION_QUERY)
        .bind(&request.purpose)
        .bind(&request.organization_id)
        .bind(&request.project_id)
        .bind(&request.provider_id)
        .bind(&request.currency)
        .bind(&request.api_profile)
        .bind(&request.operation)
        .bind(&request.provider_model_id)
        .bind(&request.public_model_id)
        .bind(&request.media_kind)
        .bind(&request.service_tier)
        .bind(&request.execution_surface)
        .bind(&request.billing_mode)
        .bind(request.at_ms)
        .bind(include_retired)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|_| PriceResolutionError::StoreUnavailable)?;
    let selected = select_resolution_row(&rows)?.clone();
    let components = sqlx::query_as::<_, PriceComponentRow>(
        r#"
        SELECT price_component_id, price_book_version_id, component_key,
               metric, unit, unit_size, unit_price_micros, outcome,
               quantity_source, required_confidence, rounding_mode,
               dimensions_json, created_at_ms
        FROM price_components
        WHERE price_book_version_id = $1
        ORDER BY component_key
        "#,
    )
    .bind(selected.price_book_version_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| PriceResolutionError::StoreUnavailable)?
    .into_iter()
    .map(PriceComponentRow::into_view)
    .collect();
    Ok(selected.into_resolution(components))
}

fn select_resolution_row(
    rows: &[PriceResolutionRow],
) -> Result<&PriceResolutionRow, PriceResolutionError> {
    let Some(selected) = rows.first() else {
        return Err(PriceResolutionError::NotFound);
    };
    if rows
        .get(1)
        .is_some_and(|candidate| candidate.rank() == selected.rank())
    {
        return Err(PriceResolutionError::Ambiguous);
    }
    Ok(selected)
}

async fn insert_version_row(
    transaction: &mut Transaction<'_, Postgres>,
    version_id: Uuid,
    price_book_id: Uuid,
    version: i32,
    draft: &PriceBookVersionDraft,
) -> Result<(), ImageGatewayError> {
    let now = now_ms()?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier, execution_surface,
            billing_mode, is_free, state, effective_from_ms, source_kind,
            source_url, source_checked_at_ms, notes, control_version,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, 'draft', $14, $15, $16, $17, $18,
                1, $19, $19)
        "#,
    )
    .bind(version_id)
    .bind(price_book_id)
    .bind(version)
    .bind(&draft.api_profile)
    .bind(&draft.operation)
    .bind(&draft.provider_id)
    .bind(&draft.provider_model_id)
    .bind(&draft.public_model_id)
    .bind(&draft.media_kind)
    .bind(&draft.service_tier)
    .bind(&draft.execution_surface)
    .bind(&draft.billing_mode)
    .bind(draft.is_free)
    .bind(draft.effective_from_ms)
    .bind(&draft.source_kind)
    .bind(&draft.source_url)
    .bind(draft.source_checked_at_ms)
    .bind(&draft.notes)
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(map_version_write)?;
    Ok(())
}

async fn persist_surface_contract_bindings(
    transaction: &mut Transaction<'_, Postgres>,
    price_book_version_id: Uuid,
    snapshots: &[super::surface_contract::ContractBindingSnapshot],
    now: i64,
) -> Result<(), ImageGatewayError> {
    for snapshot in snapshots {
        sqlx::query(
            r#"
            INSERT INTO pricing_surface_contract_revisions (
                contract_key, revision, contract_hash,
                contract_schema_version, api_profile, operation,
                provider_id, provider_model_id, public_model_id,
                media_kind, service_tier, execution_surface,
                normalizer_key, normalizer_revision, contract_json,
                created_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16
            )
            ON CONFLICT (contract_key, revision) DO NOTHING
            "#,
        )
        .bind(&snapshot.contract_key)
        .bind(snapshot.revision)
        .bind(&snapshot.contract_hash)
        .bind(snapshot.contract_schema_version)
        .bind(&snapshot.api_profile)
        .bind(&snapshot.operation)
        .bind(&snapshot.provider_id)
        .bind(&snapshot.provider_model_id)
        .bind(&snapshot.public_model_id)
        .bind(&snapshot.media_kind)
        .bind(&snapshot.service_tier)
        .bind(&snapshot.execution_surface)
        .bind(&snapshot.normalizer_key)
        .bind(snapshot.normalizer_revision)
        .bind(&snapshot.contract_json)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(store_unavailable)?;

        let stored_hash = sqlx::query_scalar::<_, String>(
            r#"
            SELECT contract_hash
            FROM pricing_surface_contract_revisions
            WHERE contract_key = $1 AND revision = $2
            "#,
        )
        .bind(&snapshot.contract_key)
        .bind(snapshot.revision)
        .fetch_one(&mut **transaction)
        .await
        .map_err(store_unavailable)?;
        if stored_hash != snapshot.contract_hash {
            return Err(invalid(
                "price_book_version_id",
                "Pricing surface contract revision drifted",
            ));
        }

        sqlx::query(
            r#"
            INSERT INTO price_book_version_surface_contract_bindings (
                price_book_version_id, contract_key, contract_revision,
                contract_hash, bound_at_ms
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (price_book_version_id, contract_key) DO NOTHING
            "#,
        )
        .bind(price_book_version_id)
        .bind(&snapshot.contract_key)
        .bind(snapshot.revision)
        .bind(&snapshot.contract_hash)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(store_unavailable)?;
    }
    Ok(())
}

async fn replace_components(
    transaction: &mut Transaction<'_, Postgres>,
    version_id: Uuid,
    components: &[PriceComponentDraft],
) -> Result<(), ImageGatewayError> {
    for component in components {
        let unit_size = parse_nonnegative_i64(&component.unit_size, "unit_size", false)?;
        let unit_price_micros =
            parse_nonnegative_i64(&component.unit_price_micros, "unit_price_micros", true)?;
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, required_confidence, rounding_mode,
                dimensions_json, created_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_id)
        .bind(&component.component_key)
        .bind(&component.metric)
        .bind(&component.unit)
        .bind(unit_size)
        .bind(unit_price_micros)
        .bind(&component.outcome)
        .bind(&component.quantity_source)
        .bind(&component.required_confidence)
        .bind(&component.rounding_mode)
        .bind(&component.dimensions)
        .bind(now_ms()?)
        .execute(&mut **transaction)
        .await
        .map_err(map_component_write)?;
    }
    Ok(())
}

async fn component_rows(
    pool: &PgPool,
    version_ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<PriceComponentView>>, ImageGatewayError> {
    if version_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query_as::<_, PriceComponentRow>(
        r#"
        SELECT price_component_id, price_book_version_id, component_key,
               metric, unit, unit_size, unit_price_micros, outcome,
               quantity_source, required_confidence, rounding_mode,
               dimensions_json, created_at_ms
        FROM price_components
        WHERE price_book_version_id = ANY($1)
        ORDER BY price_book_version_id, component_key
        "#,
    )
    .bind(version_ids)
    .fetch_all(pool)
    .await
    .map_err(store_unavailable)?;
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.price_book_version_id)
            .or_insert_with(Vec::new)
            .push(row.into_view());
    }
    Ok(grouped)
}

fn rollback_draft(source: &PriceBookVersionView, effective_from_ms: i64) -> PriceBookVersionDraft {
    PriceBookVersionDraft {
        api_profile: source.api_profile.clone(),
        operation: source.operation.clone(),
        provider_id: source.provider_id.clone(),
        provider_model_id: source.provider_model_id.clone(),
        public_model_id: source.public_model_id.clone(),
        media_kind: source.media_kind.clone(),
        service_tier: source.service_tier.clone(),
        execution_surface: source.execution_surface.clone(),
        billing_mode: source.billing_mode.clone(),
        is_free: source.is_free,
        effective_from_ms,
        source_kind: source.source_kind.clone(),
        source_url: source.source_url.clone(),
        source_checked_at_ms: source.source_checked_at_ms,
        notes: Some(format!(
            "Rollback draft cloned from immutable price version v{} ({})",
            source.version, source.price_book_version_id
        )),
        components: source
            .components
            .iter()
            .map(|component| PriceComponentDraft {
                component_key: component.component_key.clone(),
                metric: component.metric.clone(),
                unit: component.unit.clone(),
                unit_size: component.unit_size.clone(),
                unit_price_micros: component.unit_price_micros.clone(),
                outcome: component.outcome.clone(),
                quantity_source: component.quantity_source.clone(),
                required_confidence: component.required_confidence.clone(),
                rounding_mode: component.rounding_mode.clone(),
                dimensions: component.dimensions.clone(),
            })
            .collect(),
    }
}

fn validate_book(request: &CreatePriceBookRequest) -> Result<(), ImageGatewayError> {
    validate_nonempty(&request.price_book_key, 128, "price_book_key")?;
    validate_nonempty(&request.display_name, 255, "display_name")?;
    if request.currency.len() != 3
        || !request
            .currency
            .bytes()
            .all(|character| character.is_ascii_uppercase())
    {
        return Err(invalid(
            "currency",
            "Currency must be a 3-letter uppercase code",
        ));
    }
    Ok(())
}

fn validate_draft(draft: &PriceBookVersionDraft) -> Result<(), ImageGatewayError> {
    validate_nonempty(&draft.api_profile, 128, "api_profile")?;
    validate_nonempty(&draft.operation, 128, "operation")?;
    validate_nonempty(&draft.public_model_id, 255, "public_model_id")?;
    validate_nonempty(&draft.service_tier, 64, "service_tier")?;
    if !matches!(
        draft.service_tier.as_str(),
        "standard" | "flex" | "priority" | "*"
    ) {
        return Err(invalid(
            "service_tier",
            "Service tier must be standard, flex, priority, or *",
        ));
    }
    validate_nonempty(&draft.execution_surface, 64, "execution_surface")?;
    validate_nonempty(&draft.billing_mode, 64, "billing_mode")?;
    if !draft.components.iter().all(|component| {
        component.dimensions.is_object()
            && !component.component_key.trim().is_empty()
            && component.component_key.len() <= 128
    }) {
        return Err(invalid(
            "components",
            "Each component requires a key and object dimensions",
        ));
    }
    Ok(())
}

fn validate_nonempty(value: &str, max: usize, param: &str) -> Result<(), ImageGatewayError> {
    if value.trim().is_empty() || value.len() > max {
        return Err(invalid(param, format!("{param} is invalid")));
    }
    Ok(())
}

fn validate_control_version(value: i64) -> Result<(), ImageGatewayError> {
    if value <= 0 {
        return Err(invalid(
            "expected_control_version",
            "expected_control_version must be positive",
        ));
    }
    Ok(())
}

fn validate_resolution_request(
    request: &PriceResolutionRequest,
) -> Result<(), PriceResolutionError> {
    let valid_scope = request.project_id.is_none() || request.organization_id.is_some();
    let required = [
        request.purpose.as_str(),
        request.currency.as_str(),
        request.api_profile.as_str(),
        request.operation.as_str(),
        request.public_model_id.as_str(),
        request.media_kind.as_str(),
        request.service_tier.as_str(),
        request.execution_surface.as_str(),
        request.billing_mode.as_str(),
    ];
    if !valid_scope || request.at_ms < 0 || required.iter().any(|value| value.trim().is_empty()) {
        return Err(PriceResolutionError::InvalidRequest);
    }
    Ok(())
}

fn parse_nonnegative_i64(
    value: &str,
    param: &str,
    allow_zero: bool,
) -> Result<i64, ImageGatewayError> {
    let parsed = value
        .parse::<i64>()
        .ok()
        .filter(|parsed| *parsed > 0 || (allow_zero && *parsed == 0))
        .ok_or_else(|| invalid(param, format!("{param} is invalid")))?;
    Ok(parsed)
}

fn invalid(param: &str, message: impl Into<String>) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        message,
        Some(param.to_string()),
        "invalid_pricing_configuration",
    )
}

fn book_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found("Price book not found", None, "price_book_not_found")
}

fn version_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Price book version not found",
        None,
        "price_book_version_not_found",
    )
}

fn version_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Price book version changed or is no longer in the expected state",
        Some("expected_control_version".to_string()),
        "price_book_version_conflict",
    )
}

fn store_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Pricing state unavailable")
}

fn map_book_write(error: sqlx::Error) -> ImageGatewayError {
    match database_code(&error).as_deref() {
        Some("23505") => ImageGatewayError::conflict(
            "Price book key already exists",
            Some("price_book_key".to_string()),
            "price_book_conflict",
        ),
        Some("23503" | "23514") => invalid("scope", "Price book scope is invalid"),
        _ => store_unavailable(error),
    }
}

fn map_version_write(error: sqlx::Error) -> ImageGatewayError {
    match database_code(&error).as_deref() {
        Some("23503") => book_not_found(),
        Some("23505" | "23P01") => ImageGatewayError::conflict(
            "A matching active price version already exists",
            None,
            "active_price_version_conflict",
        ),
        Some("23514") => invalid("version", "Price book version is invalid"),
        Some("55000") => version_conflict(),
        _ => store_unavailable(error),
    }
}

fn map_component_write(error: sqlx::Error) -> ImageGatewayError {
    match database_code(&error).as_deref() {
        Some("23505") => invalid("components", "Component keys must be unique"),
        Some("23514") => invalid("components", "Price component is invalid"),
        Some("55000") => version_conflict(),
        _ => store_unavailable(error),
    }
}

fn map_publish(error: sqlx::Error) -> ImageGatewayError {
    if matches!(database_code(&error).as_deref(), Some("23505" | "23P01")) {
        return ImageGatewayError::conflict(
            "A published matching version already covers this effective time",
            None,
            "active_price_version_conflict",
        );
    }
    map_version_write(error)
}

fn map_resolution(error: PriceResolutionError) -> ImageGatewayError {
    match error {
        PriceResolutionError::InvalidRequest => {
            invalid("resolution", "Price resolution request is invalid")
        }
        PriceResolutionError::NotFound => ImageGatewayError::not_found(
            "No published price matches the preview request",
            None,
            "price_version_not_found",
        ),
        PriceResolutionError::Ambiguous => ImageGatewayError::conflict(
            "Multiple published prices match the preview request",
            None,
            "price_resolution_ambiguous",
        ),
        PriceResolutionError::StoreUnavailable => store_unavailable(error),
    }
}

fn map_rating(error: RatingError) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        format!("Price preview failed: {error}"),
        Some("usage_facts".to_string()),
        "invalid_pricing_preview",
    )
}

fn database_code(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .map(|code| code.into_owned())
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("System clock is invalid"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| ImageGatewayError::internal("System clock is invalid"))
}

#[derive(FromRow)]
struct PriceBookRow {
    price_book_id: Uuid,
    price_book_key: String,
    display_name: String,
    purpose: String,
    scope_type: String,
    organization_id: Option<String>,
    project_id: Option<String>,
    provider_id: Option<String>,
    currency: String,
    state: String,
    control_version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl PriceBookRow {
    fn into_view(self, versions: Vec<PriceBookVersionView>) -> PriceBookView {
        PriceBookView {
            price_book_id: self.price_book_id,
            price_book_key: self.price_book_key,
            display_name: self.display_name,
            purpose: self.purpose,
            scope_type: self.scope_type,
            organization_id: self.organization_id,
            project_id: self.project_id,
            provider_id: self.provider_id,
            currency: self.currency,
            state: self.state,
            control_version: self.control_version.to_string(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            versions,
        }
    }
}

#[derive(FromRow)]
struct PriceBookVersionRow {
    price_book_version_id: Uuid,
    price_book_id: Uuid,
    version: i32,
    api_profile: String,
    operation: String,
    provider_id: Option<String>,
    provider_model_id: Option<String>,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
    execution_surface: String,
    billing_mode: String,
    is_free: bool,
    state: String,
    effective_from_ms: i64,
    effective_until_ms: Option<i64>,
    source_kind: String,
    source_url: Option<String>,
    source_checked_at_ms: Option<i64>,
    notes: Option<String>,
    control_version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl PriceBookVersionRow {
    fn into_view(self, components: Vec<PriceComponentView>) -> PriceBookVersionView {
        PriceBookVersionView {
            price_book_version_id: self.price_book_version_id,
            price_book_id: self.price_book_id,
            version: self.version,
            api_profile: self.api_profile,
            operation: self.operation,
            provider_id: self.provider_id,
            provider_model_id: self.provider_model_id,
            public_model_id: self.public_model_id,
            media_kind: self.media_kind,
            service_tier: self.service_tier,
            execution_surface: self.execution_surface,
            billing_mode: self.billing_mode,
            is_free: self.is_free,
            state: self.state,
            effective_from_ms: self.effective_from_ms,
            effective_until_ms: self.effective_until_ms,
            source_kind: self.source_kind,
            source_url: self.source_url,
            source_checked_at_ms: self.source_checked_at_ms,
            notes: self.notes,
            control_version: self.control_version.to_string(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            components,
        }
    }
}

#[derive(FromRow)]
struct PriceComponentRow {
    price_component_id: Uuid,
    price_book_version_id: Uuid,
    component_key: String,
    metric: String,
    unit: String,
    unit_size: i64,
    unit_price_micros: i64,
    outcome: String,
    quantity_source: String,
    required_confidence: String,
    rounding_mode: String,
    dimensions_json: Value,
    created_at_ms: i64,
}

#[derive(Clone, FromRow)]
struct PriceResolutionRow {
    price_book_id: Uuid,
    price_book_key: String,
    purpose: String,
    scope_type: String,
    organization_id: Option<String>,
    project_id: Option<String>,
    book_provider_id: Option<String>,
    currency: String,
    price_book_version_id: Uuid,
    version: i32,
    api_profile: String,
    operation: String,
    provider_id: Option<String>,
    provider_model_id: Option<String>,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
    execution_surface: String,
    billing_mode: String,
    is_free: bool,
    state: String,
    effective_from_ms: i64,
    effective_until_ms: Option<i64>,
    source_kind: String,
    source_url: Option<String>,
    source_checked_at_ms: Option<i64>,
    notes: Option<String>,
    control_version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
    scope_rank: i32,
    book_provider_rank: i32,
    version_provider_rank: i32,
    api_profile_rank: i32,
    operation_rank: i32,
    provider_model_rank: i32,
    public_model_rank: i32,
    service_tier_rank: i32,
}

impl PriceResolutionRow {
    fn rank(&self) -> [i32; 8] {
        [
            self.scope_rank,
            self.book_provider_rank,
            self.version_provider_rank,
            self.api_profile_rank,
            self.operation_rank,
            self.provider_model_rank,
            self.public_model_rank,
            self.service_tier_rank,
        ]
    }

    fn into_resolution(self, components: Vec<PriceComponentView>) -> ResolvedPriceVersion {
        ResolvedPriceVersion {
            price_book_id: self.price_book_id,
            price_book_key: self.price_book_key,
            purpose: self.purpose,
            scope_type: self.scope_type,
            organization_id: self.organization_id,
            project_id: self.project_id,
            provider_id: self.book_provider_id,
            currency: self.currency,
            version: PriceBookVersionView {
                price_book_version_id: self.price_book_version_id,
                price_book_id: self.price_book_id,
                version: self.version,
                api_profile: self.api_profile,
                operation: self.operation,
                provider_id: self.provider_id,
                provider_model_id: self.provider_model_id,
                public_model_id: self.public_model_id,
                media_kind: self.media_kind,
                service_tier: self.service_tier,
                execution_surface: self.execution_surface,
                billing_mode: self.billing_mode,
                is_free: self.is_free,
                state: self.state,
                effective_from_ms: self.effective_from_ms,
                effective_until_ms: self.effective_until_ms,
                source_kind: self.source_kind,
                source_url: self.source_url,
                source_checked_at_ms: self.source_checked_at_ms,
                notes: self.notes,
                control_version: self.control_version.to_string(),
                created_at_ms: self.created_at_ms,
                updated_at_ms: self.updated_at_ms,
                components,
            },
        }
    }
}

impl PriceComponentRow {
    fn into_view(self) -> PriceComponentView {
        PriceComponentView {
            price_component_id: self.price_component_id,
            component_key: self.component_key,
            metric: self.metric,
            unit: self.unit,
            unit_size: self.unit_size.to_string(),
            unit_price_micros: self.unit_price_micros.to_string(),
            outcome: self.outcome,
            quantity_source: self.quantity_source,
            required_confidence: self.required_confidence,
            rounding_mode: self.rounding_mode,
            dimensions: self.dimensions_json,
            created_at_ms: self.created_at_ms,
        }
    }
}
