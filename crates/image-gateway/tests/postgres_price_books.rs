use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use gpt_image_2_gateway::{
    database::{connect_test_pool_with_search_path, run_migrations},
    pricing::{
        ApplyOfficialPriceSnapshotRequest, CreatePriceBookRequest, CreatePriceBookVersionRequest,
        CreatePriceRollbackDraftRequest, PostgresPricingAdminService, PriceBookVersionDraft,
        PriceComponentDraft, PricePreviewRequest, PriceResolutionError, PriceResolutionRequest,
        PriceResolver, PricingAdminService, PricingTransitionActor,
        TransitionPriceBookVersionRequest, UpdatePriceBookVersionRequest, UsageFact,
    },
};
use serde_json::json;
use sha2::Digest;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn published_price_books_are_immutable_and_fail_closed() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = price_book_lifecycle_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn pricing_service_enforces_draft_publish_retire_lifecycle() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = pricing_service_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn authenticated_price_transitions_are_atomically_audited() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = authenticated_price_transition_audit_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn scheduled_price_cutovers_are_atomic_and_time_aware() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = scheduled_price_cutover_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn rollback_clones_history_and_preserves_continuous_resolution() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = price_rollback_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn price_resolution_is_scope_aware_and_fails_on_equal_precedence() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = price_resolution_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn price_resolution_preserves_api_profile_alias_precedence() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = price_profile_alias_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn provider_actual_and_benchmark_billing_modes_cannot_be_conflated() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = billing_mode_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn provider_allocated_cost_pools_conserve_and_require_evidence() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = provider_allocated_cost_pool_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn pricing_preview_uses_the_published_scope_and_fact_authority() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = pricing_preview_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn official_price_snapshots_are_idempotent_and_only_create_reviewable_drafts() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = official_price_snapshot_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn official_price_imports_require_a_distinct_publisher() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = official_price_maker_checker_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn pricing_coverage_fails_closed_when_models_have_no_platform_route() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = pricing_coverage_without_routes_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn customer_price_publication_requires_a_real_surface_and_known_dimensions() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = customer_price_publish_readiness_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn customer_token_price_publication_requires_authoritative_terminal_facts() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = async {
        run_migrations(&test_schema.pool)
            .await
            .map_err(|error| format!("migrations should succeed: {error:?}"))?;
        seed_codex_pricing_surface(&test_schema.pool).await?;
        let service = PostgresPricingAdminService::new(test_schema.pool.clone());
        let book = service
            .create_price_book(CreatePriceBookRequest {
                price_book_key: "codex.customer.token-unavailable".to_string(),
                display_name: "Codex token customer price".to_string(),
                purpose: "customer_sale".to_string(),
                scope_type: "platform".to_string(),
                organization_id: None,
                project_id: None,
                provider_id: Some("openai-codex".to_string()),
                currency: "USD".to_string(),
            })
            .await
            .map_err(|error| format!("{error:?}"))?;
        let version = service
            .create_version(
                book.price_book_id,
                CreatePriceBookVersionRequest {
                    draft: codex_token_customer_draft(),
                },
            )
            .await
            .map_err(|error| format!("{error:?}"))?;

        let readiness = service
            .publish_readiness(version.price_book_version_id)
            .await
            .map_err(|error| format!("{error:?}"))?;
        require(
            !readiness.ready
                && readiness.matching_surface_count == 1
                && readiness.metering_status == "incompatible"
                && readiness
                    .blocking_reasons
                    .contains(&"metering_contract_incompatible".to_string()),
            "a Codex token customer price must remain blocked until terminal output facts are authoritative",
        )?;
        require(
            service
                .publish_version(
                    version.price_book_version_id,
                    TransitionPriceBookVersionRequest {
                        expected_control_version: 1,
                    },
                )
                .await
                .is_err(),
            "an incompatible Codex token customer price must not publish",
        )
    }
    .await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

async fn customer_price_publish_readiness_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "dreamina.customer.readiness".to_string(),
            display_name: "Dreamina customer readiness".to_string(),
            purpose: "customer_sale".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("dreamina-cli".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("{error:?}"))?;
    let direct_active_insert = sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, billing_mode, is_free, state,
            effective_from_ms, source_kind, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 999, 'dreamina-cli-images-v1', 'generation',
            'dreamina-cli', '5.0', '5.0', 'image', 'standard',
            'provider_cli', 'customer_rate', FALSE, 'active',
            1, 'manual', 1, 1
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(book.price_book_id)
    .execute(pool)
    .await;
    require(
        direct_active_insert.is_err(),
        "direct insertion of an unbound active customer price must fail closed",
    )?;
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: dreamina_customer_draft("9", Vec::new()),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;

    let blocked = service
        .publish_readiness(version.price_book_version_id)
        .await
        .map_err(|error| format!("{error:?}"))?;
    require(
        !blocked.ready
            && blocked
                .blocking_reasons
                .contains(&"platform_surface_missing".to_string()),
        "a customer price without a real platform surface must not publish",
    )?;

    seed_dreamina_pricing_surface(pool).await?;
    let ready = service
        .publish_readiness(version.price_book_version_id)
        .await
        .map_err(|error| format!("{error:?}"))?;
    require(
        ready.ready
            && ready.matching_surface_count == 1
            && ready.request_dimensions == ["height", "ratio", "resolution_type", "width"],
        "the exact Dreamina surface and its request dimensions must be reported",
    )?;
    let before_publish = system_now_ms()?;
    let published = service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("ready customer price should publish: {error:?}"))?;
    let after_publish = system_now_ms()?;
    require(
        published.effective_from_ms >= before_publish
            && published.effective_from_ms <= after_publish,
        "a customer sale price must become effective at publication time instead of backdating",
    )?;

    let bindings: Vec<(String, i64, String, serde_json::Value)> = sqlx::query_as(
        r#"
        SELECT binding.contract_key, binding.contract_revision,
               binding.contract_hash, contract.contract_json
        FROM price_book_version_surface_contract_bindings binding
        JOIN pricing_surface_contract_revisions contract
          ON contract.contract_key = binding.contract_key
         AND contract.revision = binding.contract_revision
         AND contract.contract_hash = binding.contract_hash
        WHERE binding.price_book_version_id = $1
        ORDER BY binding.contract_key
        "#,
    )
    .bind(version.price_book_version_id)
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    let binding_is_exact = bindings.len() == 1
        && bindings[0].1 == 2
        && bindings[0].2.len() == 64
        && bindings[0]
            .2
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        && bindings[0].3["exact_surface"]["api_profile"] == "dreamina-cli-images-v1"
        && bindings[0].3["contract"]["provider_id"] == "dreamina-cli"
        && bindings[0].3["exact_surface"]["provider_model_id"] == "5.0"
        && bindings[0].3["exact_surface"]["public_model_id"] == "5.0"
        && bindings[0].3["contract"]["metering_bases"][0]["customer_sale_required"] == true;
    require(
        binding_is_exact,
        &format!(
            "publishing must persist one exact, lowercase-SHA256 surface contract snapshot: {bindings:?}"
        ),
    )?;
    let mutation = sqlx::query(
        r#"
        UPDATE pricing_surface_contract_revisions
        SET normalizer_revision = normalizer_revision + 1
        WHERE contract_key = $1 AND revision = $2
        "#,
    )
    .bind(&bindings[0].0)
    .bind(bindings[0].1)
    .execute(pool)
    .await;
    require(
        mutation.is_err(),
        "a published surface contract revision must remain immutable",
    )?;

    let invalid_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "dreamina.customer.unknown-dimension".to_string(),
            display_name: "Dreamina unknown dimension".to_string(),
            purpose: "customer_sale".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("dreamina-cli".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("{error:?}"))?;
    let invalid = service
        .create_version(
            invalid_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: dreamina_customer_draft(
                    "9",
                    vec![PriceComponentDraft {
                        component_key: "image-output-unknown-dimension".to_string(),
                        metric: "image_output".to_string(),
                        unit: "image".to_string(),
                        unit_size: "1".to_string(),
                        unit_price_micros: "10".to_string(),
                        outcome: "succeeded".to_string(),
                        quantity_source: "request_derived".to_string(),
                        required_confidence: "exact".to_string(),
                        rounding_mode: "exact".to_string(),
                        dimensions: json!({"unsupported": "value"}),
                    }],
                ),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    let invalid_readiness = service
        .publish_readiness(invalid.price_book_version_id)
        .await
        .map_err(|error| format!("{error:?}"))?;
    require(
        !invalid_readiness.ready
            && invalid_readiness
                .blocking_reasons
                .contains(&"component_dimension_unsupported".to_string()),
        "unknown request dimensions must fail closed",
    )
}

fn dreamina_customer_draft(
    succeeded_unit_price_micros: &str,
    mut additional_components: Vec<PriceComponentDraft>,
) -> PriceBookVersionDraft {
    additional_components.extend(
        ["succeeded", "failed", "no_effect"]
            .into_iter()
            .map(|outcome| PriceComponentDraft {
                component_key: format!("image-output-{outcome}"),
                metric: "image_output".to_string(),
                unit: "image".to_string(),
                unit_size: "1".to_string(),
                unit_price_micros: if outcome == "succeeded" {
                    succeeded_unit_price_micros.to_string()
                } else {
                    "0".to_string()
                },
                outcome: outcome.to_string(),
                quantity_source: "request_derived".to_string(),
                required_confidence: "exact".to_string(),
                rounding_mode: "exact".to_string(),
                dimensions: json!({}),
            }),
    );
    PriceBookVersionDraft {
        api_profile: "dreamina-cli-images-v1".to_string(),
        operation: "generation".to_string(),
        provider_id: Some("dreamina-cli".to_string()),
        provider_model_id: Some("5.0".to_string()),
        public_model_id: "5.0".to_string(),
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_cli".to_string(),
        billing_mode: "customer_rate".to_string(),
        is_free: false,
        effective_from_ms: 1,
        source_kind: "official_document".to_string(),
        source_url: Some("https://www.volcengine.com/docs/82379/1544106".to_string()),
        source_checked_at_ms: Some(1),
        notes: None,
        components: additional_components,
    }
}

fn codex_token_customer_draft() -> PriceBookVersionDraft {
    PriceBookVersionDraft {
        api_profile: "openai-images-v1".to_string(),
        operation: "generation".to_string(),
        provider_id: Some("openai-codex".to_string()),
        provider_model_id: Some("gpt-image-2".to_string()),
        public_model_id: "gpt-image-2".to_string(),
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_cli".to_string(),
        billing_mode: "customer_rate".to_string(),
        is_free: false,
        effective_from_ms: 1,
        source_kind: "official_document".to_string(),
        source_url: Some("https://developers.openai.com/api/docs/pricing".to_string()),
        source_checked_at_ms: Some(1),
        notes: Some(
            "Official lookup is a provider benchmark, not an authoritative CLI customer charge."
                .to_string(),
        ),
        components: vec![PriceComponentDraft {
            component_key: "image-output-token-any".to_string(),
            metric: "image_output_token".to_string(),
            unit: "token".to_string(),
            unit_size: "1000000".to_string(),
            unit_price_micros: "30000000".to_string(),
            outcome: "any".to_string(),
            quantity_source: "official_lookup".to_string(),
            required_confidence: "estimated".to_string(),
            rounding_mode: "ceil".to_string(),
            dimensions: json!({}),
        }],
    }
}

async fn pricing_coverage_without_routes_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids,
            source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
        )
        VALUES (
            'openai-codex', 'gpt-image-2', 'gpt-image-2', 'image',
            'GPT Image 2', 'supported', 'enabled',
            ARRAY['images.generations'], 'adapter_contract',
            1, 1, '{}'::JSONB
        )
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("coverage model fixture should insert: {error:?}"))?;
    let coverage = PostgresPricingAdminService::new(pool.clone())
        .coverage()
        .await
        .map_err(|error| format!("pricing coverage should load: {error:?}"))?;
    require(
        coverage.summary.surfaces > 0,
        "adapter-supported models must be represented even before routes exist",
    )?;
    require(
        coverage.rows.iter().all(|row| {
            row.route_status == "missing"
                && row.customer_price_status == "missing"
                && row.readiness == "blocked"
                && row
                    .blocking_reasons
                    .contains(&"platform_route_missing".to_string())
        }),
        "coverage must not claim route or price readiness without a platform route mapping",
    )
}

async fn official_price_snapshot_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let actor_user_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO identity_users (
            user_id, normalized_email, display_name, roles, scopes,
            authz_version, failed_login_count, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, 'pricing-import@test.local', 'Pricing importer',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(actor_user_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let service = PostgresPricingAdminService::new(pool.clone());
    let catalogs = service
        .official_catalogs()
        .await
        .map_err(|error| format!("official catalogs should load: {error:?}"))?;
    require(
        catalogs.catalogs.iter().any(|catalog| {
            catalog.catalog_key == "openai-api-pricing"
                && catalog.available
                && catalog.item_count == 1
        }),
        "OpenAI official catalog must be available with one reviewed model",
    )?;
    require(
        catalogs.catalogs.iter().any(|catalog| {
            catalog.source_provider_id == "volcengine-ark"
                && !catalog.available
                && catalog.item_count == 0
        }),
        "unverified Volcengine pricing must remain unavailable",
    )?;

    let existing_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "provider_benchmark.xai.grok-imagine-image-quality.usd".to_string(),
            display_name: "Existing xAI benchmark".to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("existing benchmark book should be created: {error:?}"))?;
    let existing_draft = service
        .create_version(
            existing_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: xai_image_draft("12345"),
            },
        )
        .await
        .map_err(|error| format!("existing benchmark draft should be created: {error:?}"))?;
    service
        .publish_version(
            existing_draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("existing benchmark should publish: {error:?}"))?;

    let first = service
        .observe_official_catalog("xai-imagine-pricing", actor_user_id, Uuid::new_v4())
        .await
        .map_err(|error| format!("xAI official catalog should be observed: {error:?}"))?;
    require(
        first.snapshot.state == "observed"
            && first.snapshot.item_count == 4
            && first
                .sync_run
                .as_ref()
                .is_some_and(|run| run.state == "changed" && run.evidence_sha256.len() == 64)
            && first.differences.iter().any(|diff| {
                diff.item_key == "grok-imagine-image-quality"
                    && diff.status == "changed"
                    && diff
                        .component_differences
                        .iter()
                        .any(|component| component.status != "unchanged")
            }),
        "preview must retain source evidence and show the exact changed xAI component",
    )?;

    let concurrent_service = PostgresPricingAdminService::new(pool.clone());
    let (repeated, concurrent) = tokio::join!(
        service.observe_official_catalog("xai-imagine-pricing", actor_user_id, Uuid::new_v4(),),
        concurrent_service.observe_official_catalog(
            "xai-imagine-pricing",
            actor_user_id,
            Uuid::new_v4(),
        ),
    );
    let repeated =
        repeated.map_err(|error| format!("repeated observation should succeed: {error:?}"))?;
    let concurrent =
        concurrent.map_err(|error| format!("concurrent observation should succeed: {error:?}"))?;
    require(
        repeated.snapshot.snapshot_id == first.snapshot.snapshot_id
            && concurrent.snapshot.snapshot_id == first.snapshot.snapshot_id
            && repeated.snapshot.content_sha256 == first.snapshot.content_sha256
            && repeated
                .sync_run
                .as_ref()
                .is_some_and(|run| run.state == "unchanged")
            && concurrent
                .sync_run
                .as_ref()
                .is_some_and(|run| run.state == "unchanged")
            && repeated.sync_run.as_ref().map(|run| run.sync_run_id)
                != concurrent.sync_run.as_ref().map(|run| run.sync_run_id),
        "concurrent checks must reuse one immutable snapshot but retain distinct unchanged sync runs",
    )?;
    let sync_run_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pricing_source_sync_runs WHERE catalog_key = $1")
            .bind("xai-imagine-pricing")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        sync_run_count == 3,
        "every initial or repeated source check must remain auditable",
    )?;
    expect_sqlstate(
        sqlx::query(
            "UPDATE pricing_source_snapshots SET normalized_payload = '{}'::JSONB WHERE snapshot_id = $1",
        )
        .bind(first.snapshot.snapshot_id)
        .execute(pool)
        .await,
        "55000",
        "mutating immutable official source evidence",
    )?;
    let first_sync_run_id = first
        .sync_run
        .as_ref()
        .map(|run| run.sync_run_id)
        .ok_or_else(|| "initial sync run was not returned".to_string())?;
    expect_sqlstate(
        sqlx::query("DELETE FROM pricing_source_sync_runs WHERE sync_run_id = $1")
            .bind(first_sync_run_id)
            .execute(pool)
            .await,
        "55000",
        "deleting immutable official source sync history",
    )?;
    let catalogs_after_sync = service
        .official_catalogs()
        .await
        .map_err(|error| format!("official catalogs should include sync state: {error:?}"))?;
    require(
        catalogs_after_sync.catalogs.iter().any(|catalog| {
            catalog.catalog_key == "xai-imagine-pricing"
                && catalog
                    .latest_sync_run
                    .as_ref()
                    .is_some_and(|run| run.state == "unchanged")
        }),
        "catalog list must expose the latest versioned sync result",
    )?;

    let applied = service
        .apply_official_snapshot(
            first.snapshot.snapshot_id,
            ApplyOfficialPriceSnapshotRequest {
                item_keys: vec!["grok-imagine-image-quality".to_string()],
            },
            actor_user_id,
            Uuid::new_v4(),
        )
        .await
        .map_err(|error| format!("selected official item should apply: {error:?}"))?;
    require(
        applied.snapshot.state == "partially_applied"
            && applied.applications.len() == 1
            && applied.applications[0].action == "created_draft",
        "applying one changed item must create one reviewable draft",
    )?;
    let imported_state: String = sqlx::query_scalar(
        "SELECT state FROM price_book_versions WHERE price_book_version_id = $1",
    )
    .bind(applied.applications[0].price_book_version_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        imported_state == "draft",
        "official price import must never publish a version",
    )?;

    let reapplied = service
        .apply_official_snapshot(
            first.snapshot.snapshot_id,
            ApplyOfficialPriceSnapshotRequest {
                item_keys: vec!["grok-imagine-image-quality".to_string()],
            },
            actor_user_id,
            Uuid::new_v4(),
        )
        .await
        .map_err(|error| format!("reapplying the same item should be idempotent: {error:?}"))?;
    let imported_versions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM price_book_versions WHERE price_book_id = $1")
            .bind(existing_book.price_book_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        reapplied.applications.len() == 1 && imported_versions == 2,
        "reapplying one snapshot item must not create duplicate versions",
    )?;

    let completed = service
        .apply_official_snapshot(
            first.snapshot.snapshot_id,
            ApplyOfficialPriceSnapshotRequest {
                item_keys: vec![
                    "grok-imagine-image".to_string(),
                    "grok-imagine-video-1.5".to_string(),
                    "grok-imagine-video".to_string(),
                ],
            },
            actor_user_id,
            Uuid::new_v4(),
        )
        .await
        .map_err(|error| format!("remaining official items should apply: {error:?}"))?;
    require(
        completed.snapshot.state == "applied"
            && completed.applications.len() == 4
            && completed
                .differences
                .iter()
                .all(|diff| diff.status == "draft_exists"),
        "all catalog items must end as explicit drafts awaiting publication",
    )?;
    let active_imports: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pricing_source_snapshot_applications AS application
        JOIN price_book_versions AS version
          ON version.price_book_version_id = application.price_book_version_id
        WHERE application.snapshot_id = $1 AND version.state = 'active'
        "#,
    )
    .bind(first.snapshot.snapshot_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        active_imports == 0,
        "the import workflow must not activate any newly imported version",
    )?;

    let mut previous_payload: serde_json::Value = sqlx::query_scalar(
        "SELECT normalized_payload FROM pricing_source_snapshots WHERE snapshot_id = $1",
    )
    .bind(first.snapshot.snapshot_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let items = previous_payload
        .get_mut("items")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| "official source snapshot did not contain an item array".to_string())?;
    let mut removed_item = items
        .first()
        .cloned()
        .ok_or_else(|| "official source snapshot did not contain an item".to_string())?;
    removed_item["item_key"] = json!("retired-model-fixture");
    removed_item["price_book_key"] = json!("provider_benchmark.xai.retired-model-fixture.usd");
    removed_item["display_name"] = json!("Retired official model fixture");
    removed_item["provider_model_id"] = json!("retired-model-fixture");
    removed_item["public_model_id"] = json!("retired-model-fixture");
    items.push(removed_item);
    let previous_hash = hex::encode(sha2::Sha256::digest(
        serde_json::to_vec(&previous_payload)
            .map_err(|error| format!("previous source fixture should encode: {error}"))?,
    ));
    let previous_snapshot_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO pricing_source_snapshots (
            snapshot_id, catalog_key, source_provider_id, currency,
            source_url, source_checked_at_ms, source_revision,
            parser_version, content_sha256, state, item_count,
            normalized_payload, created_by_user_id, created_at_ms, updated_at_ms
        )
        SELECT
            $1, catalog_key, source_provider_id, currency, source_url,
            source_checked_at_ms, source_revision, parser_version, $2,
            'observed', item_count + 1, $3, created_by_user_id,
            created_at_ms + 1000, updated_at_ms + 1000
        FROM pricing_source_snapshots
        WHERE snapshot_id = $4
        "#,
    )
    .bind(previous_snapshot_id)
    .bind(previous_hash)
    .bind(previous_payload)
    .bind(first.snapshot.snapshot_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let removal_preview = service
        .observe_official_catalog("xai-imagine-pricing", actor_user_id, Uuid::new_v4())
        .await
        .map_err(|error| format!("catalog removal should be observed: {error:?}"))?;
    require(
        removal_preview.sync_run.as_ref().is_some_and(|run| {
            run.state == "changed" && run.previous_snapshot_id == Some(previous_snapshot_id)
        }) && removal_preview.differences.iter().any(|difference| {
            difference.item_key == "retired-model-fixture"
                && difference.status == "removed"
                && difference
                    .component_differences
                    .iter()
                    .all(|component| component.status == "removed")
        }),
        "a model removed from the next official snapshot must remain visible for manual review",
    )
}

async fn authenticated_price_transition_audit_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let actor = PricingTransitionActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    sqlx::query(
        r#"
        INSERT INTO identity_users (
            user_id, normalized_email, display_name, roles, scopes,
            authz_version, failed_login_count, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, 'pricing-publisher@test.local', 'Pricing publisher',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(actor.user_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.transition-audit.usd".to_string(),
            display_name: "xAI transition audit".to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("price book should be created: {error:?}"))?;
    let draft = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: xai_image_draft("20000"),
            },
        )
        .await
        .map_err(|error| format!("price version should be created: {error:?}"))?;
    let published = service
        .publish_version_as(
            draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
            actor,
        )
        .await
        .map_err(|error| format!("authenticated publish should succeed: {error:?}"))?;
    let replacement = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: scheduled_xai_image_draft(2, "30000"),
            },
        )
        .await
        .map_err(|error| format!("replacement draft should be created: {error:?}"))?;
    service
        .publish_version(
            replacement.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("replacement draft should publish: {error:?}"))?;
    service
        .retire_version_as(
            published.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 3,
            },
            actor,
        )
        .await
        .map_err(|error| format!("authenticated retirement should succeed: {error:?}"))?;

    let transitions = sqlx::query_as::<_, (String, Uuid, Uuid, String, serde_json::Value)>(
        r#"
        SELECT action, actor_user_id, session_id, outcome, metadata
        FROM identity_audit_events
        WHERE resource_type = 'price_book_version'
          AND resource_id = $1
        ORDER BY created_at_ms, action
        "#,
    )
    .bind(draft.price_book_version_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    require(
        transitions.len() == 2
            && transitions
                .iter()
                .all(|(_, user_id, session_id, outcome, _)| {
                    *user_id == actor.user_id
                        && *session_id == actor.session_id
                        && outcome == "success"
                })
            && transitions
                .iter()
                .any(|(action, _, _, _, _)| action == "pricing.price_book_version.publish")
            && transitions
                .iter()
                .any(|(action, _, _, _, _)| action == "pricing.price_book_version.retire"),
        "authenticated price transitions must commit with actor and session audit records",
    )?;
    let publish_metadata = transitions
        .iter()
        .find(|(action, _, _, _, _)| action == "pricing.price_book_version.publish")
        .map(|(_, _, _, _, metadata)| metadata)
        .ok_or_else(|| "publish audit metadata should exist".to_string())?;
    require(
        publish_metadata["requested_effective_from_ms"] == json!(1)
            && publish_metadata["effective_from_ms"] == json!(1)
            && publish_metadata["published_at_ms"]
                .as_i64()
                .is_some_and(|published_at_ms| published_at_ms > 1),
        "provider benchmark audit must preserve the requested official effective time and record publication time",
    )
}

async fn official_price_maker_checker_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let importer = PricingTransitionActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    let publisher = PricingTransitionActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    for (actor, email, name) in [
        (importer, "pricing-maker@test.local", "Pricing maker"),
        (publisher, "pricing-checker@test.local", "Pricing checker"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO identity_users (
                user_id, normalized_email, display_name, roles, scopes,
                authz_version, failed_login_count, created_at_ms, updated_at_ms
            )
            VALUES (
                $1, $2, $3, ARRAY['platform_owner'], ARRAY['admin:*'],
                1, 0, 1, 1
            )
            "#,
        )
        .bind(actor.user_id)
        .bind(email)
        .bind(name)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    }

    let service = PostgresPricingAdminService::new(pool.clone());
    let observed = service
        .observe_official_catalog("xai-imagine-pricing", importer.user_id, importer.session_id)
        .await
        .map_err(|error| format!("official catalog should be observed: {error:?}"))?;
    let applied = service
        .apply_official_snapshot(
            observed.snapshot.snapshot_id,
            ApplyOfficialPriceSnapshotRequest {
                item_keys: vec!["grok-imagine-video-1.5".to_string()],
            },
            importer.user_id,
            importer.session_id,
        )
        .await
        .map_err(|error| format!("official item should create a draft: {error:?}"))?;
    let version_id = applied
        .applications
        .first()
        .ok_or_else(|| "official import did not create an application".to_string())?
        .price_book_version_id;

    let importer_readiness = service
        .publish_readiness_as(version_id, importer)
        .await
        .map_err(|error| format!("importer readiness should load: {error:?}"))?;
    require(
        !importer_readiness.ready
            && importer_readiness
                .blocking_reasons
                .contains(&"maker_checker_required".to_string()),
        "the official-price importer must not pass its own publication review",
    )?;
    let denied = service
        .publish_version_as(
            version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
            importer,
        )
        .await
        .expect_err("the official-price importer must not publish its own draft");
    require(
        denied.status_code().as_u16() == 409,
        "maker-checker denial must return conflict",
    )?;

    let reviewer_readiness = service
        .publish_readiness_as(version_id, publisher)
        .await
        .map_err(|error| format!("reviewer readiness should load: {error:?}"))?;
    require(
        reviewer_readiness.ready,
        "a distinct platform owner must be able to review the official draft",
    )?;
    let published = service
        .publish_version_as(
            version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
            publisher,
        )
        .await
        .map_err(|error| format!("distinct publisher should succeed: {error:?}"))?;
    require(
        published.state == "active",
        "reviewed official price must become active",
    )?;

    let audits = sqlx::query_as::<_, (Uuid, String, String, Option<String>)>(
        r#"
        SELECT actor_user_id, action, outcome, reason_code
        FROM identity_audit_events
        WHERE resource_type = 'price_book_version'
          AND resource_id = $1
        ORDER BY created_at_ms, outcome
        "#,
    )
    .bind(version_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    require(
        audits.iter().any(|(user_id, action, outcome, reason)| {
            *user_id == importer.user_id
                && action == "pricing.price_book_version.publish"
                && outcome == "denied"
                && reason.as_deref() == Some("maker_checker_required")
        }) && audits.iter().any(|(user_id, action, outcome, reason)| {
            *user_id == publisher.user_id
                && action == "pricing.price_book_version.publish"
                && outcome == "success"
                && reason.is_none()
        }),
        "maker-checker denial and successful publication must both remain auditable",
    )
}

#[tokio::test]
async fn customer_quote_contract_freezes_components_and_enforces_hold_transitions() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = customer_quote_contract_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

async fn customer_quote_contract_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
            organization_id, display_name, organization_kind,
            created_at_ms, updated_at_ms
        )
        VALUES ('org-quote', 'Quote organization', 'system', 1, 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ('project-quote', 'org-quote', 'Quote project', 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let price_book_id = Uuid::new_v4();
    let price_book_version_id = Uuid::new_v4();
    let price_component_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose,
            scope_type, currency, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, 'customer.quote.usd', 'Customer quote',
                'customer_sale', 'platform', 'USD', 'active', 1, 1)
        "#,
    )
    .bind(price_book_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, public_model_id, media_kind,
            service_tier, execution_surface, billing_mode, is_free,
            state, effective_from_ms, source_kind, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 1, 'openai-images-v1', 'generation',
                'gpt-image-2', 'image', 'standard', 'provider_cli',
                'customer_rate', FALSE, 'draft', 1, 'manual', 1, 1)
        "#,
    )
    .bind(price_book_version_id)
    .bind(price_book_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_components (
            price_component_id, price_book_version_id, component_key,
            metric, unit, unit_size, unit_price_micros, outcome,
            quantity_source, required_confidence, rounding_mode,
            dimensions_json, created_at_ms
        )
        VALUES ($1, $2, 'image.output', 'image_output', 'image',
                1, 20000, 'succeeded', 'request_derived', 'exact',
                'exact', '{"resolution":"1k"}', 1)
        "#,
    )
    .bind(price_component_id)
    .bind(price_book_version_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    bind_test_surface_contract(
        pool,
        price_book_version_id,
        "generation",
        "openai-codex",
        "gpt-image-2",
        "provider_cli",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE price_book_versions
        SET state = 'active', control_version = 2, updated_at_ms = 2
        WHERE price_book_version_id = $1
        "#,
    )
    .bind(price_book_version_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let job_id = insert_quote_job(pool, "request-quote-valid").await?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros, held_micros,
            captured_micros, created_at_ms, updated_at_ms
        )
        VALUES ('org-quote', 'USD', 40000, 0, 0, 1, 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let quote_id = Uuid::new_v4();
    let quote_line_id = Uuid::new_v4();
    let hold_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    insert_customer_quote(
        &mut tx,
        quote_id,
        job_id,
        price_book_id,
        price_book_version_id,
        40_000,
    )
    .await?;
    insert_customer_quote_line(
        &mut tx,
        quote_line_id,
        quote_id,
        job_id,
        price_component_id,
        2,
        40_000,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO customer_billing_holds (
            hold_id, quote_id, job_id, tenant_id, currency,
            held_micros, account_held_micros,
            state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 'org-quote', 'USD', 40000, 40000, 'held', 3, 3)
        "#,
    )
    .bind(hold_id)
    .bind(quote_id)
    .bind(job_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit()
        .await
        .map_err(|error| format!("valid customer quote should commit: {error:?}"))?;

    expect_sqlstate(
        sqlx::query(
            "UPDATE customer_price_quote_lines SET max_amount_micros = 1 WHERE quote_line_id = $1",
        )
        .bind(quote_line_id)
        .execute(pool)
        .await,
        "P0001",
        "frozen customer quote line mutation",
    )?;
    expect_sqlstate(
        sqlx::query(
            r#"
        UPDATE customer_billing_holds
        SET captured_micros = 40000, account_captured_micros = 40000,
            state = 'settled', updated_at_ms = 4
        WHERE hold_id = $1
        "#,
        )
        .bind(hold_id)
        .execute(pool)
        .await,
        "23514",
        "hold settlement without an immutable terminal rating",
    )?;
    sqlx::query(
        r#"
        UPDATE customer_billing_holds
        SET released_micros = 40000, account_released_micros = 40000,
            state = 'released', updated_at_ms = 4
        WHERE hold_id = $1
        "#,
    )
    .bind(hold_id)
    .execute(pool)
    .await
    .map_err(|error| format!("unused hold release should succeed: {error:?}"))?;
    expect_sqlstate(
        sqlx::query(
            r#"
            UPDATE customer_billing_holds
            SET state = 'settled', captured_micros = 40000,
                account_captured_micros = 40000,
                released_micros = 0, account_released_micros = 0,
                updated_at_ms = 5
            WHERE hold_id = $1
            "#,
        )
        .bind(hold_id)
        .execute(pool)
        .await,
        "55000",
        "released hold terminal transition",
    )?;

    let forged_job_id = insert_quote_job(pool, "request-quote-forged").await?;
    let forged_quote_id = Uuid::new_v4();
    let mut forged = pool.begin().await.map_err(debug_error)?;
    insert_customer_quote(
        &mut forged,
        forged_quote_id,
        forged_job_id,
        price_book_id,
        price_book_version_id,
        1,
    )
    .await?;
    insert_customer_quote_line(
        &mut forged,
        Uuid::new_v4(),
        forged_quote_id,
        forged_job_id,
        price_component_id,
        2,
        40_000,
    )
    .await?;
    expect_sqlstate(
        forged.commit().await,
        "23514",
        "quote total forged independently from frozen lines",
    )
}

async fn insert_quote_job(pool: &PgPool, request_id: &str) -> TestResult<Uuid> {
    let job_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO jobs (
            job_id, tenant_id, request_id, operation, provider_id, model,
            state, requested_units, output_count, billable_units,
            billing_metric, billing_unit, economics_contract_version,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'org-quote', $2, 'generation', 'openai-codex',
                'gpt-image-2', 'reserved', 2, 2, 2,
                'output', 'output', 4, 1, 1)
        "#,
    )
    .bind(job_id)
    .bind(request_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions (
            job_id, tenant_id, project_id, auth_kind, admitted_at_ms
        )
        VALUES ($1, 'org-quote', 'project-quote', 'legacy', 3)
        "#,
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(job_id)
}

async fn insert_customer_quote(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    quote_id: Uuid,
    job_id: Uuid,
    price_book_id: Uuid,
    price_book_version_id: Uuid,
    max_total_micros: i64,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO customer_price_quotes (
            quote_id, job_id, tenant_id, project_id,
            price_book_id, price_book_version_id,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, request_dimensions_json,
            billing_mode, is_free, currency,
            max_total_micros, quote_hash, created_at_ms
        )
        VALUES ($1, $2, 'org-quote', 'project-quote', $3, $4,
                'openai-images-v1', 'generation', 'openai-codex',
                'gpt-image-2', 'gpt-image-2', 'image', 'standard',
                'provider_cli', '{"resolution":"1k"}'::JSONB,
                'customer_rate', FALSE, 'USD',
                $5, repeat('a', 64), 3)
        "#,
    )
    .bind(quote_id)
    .bind(job_id)
    .bind(price_book_id)
    .bind(price_book_version_id)
    .bind(max_total_micros)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn insert_customer_quote_line(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    quote_line_id: Uuid,
    quote_id: Uuid,
    job_id: Uuid,
    price_component_id: Uuid,
    max_quantity: i64,
    max_amount_micros: i64,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO customer_price_quote_lines (
            quote_line_id, quote_id, job_id, price_component_id,
            component_key, partition_key, terminal_outcome,
            metric, unit, unit_size, unit_price_micros,
            quantity_source, required_confidence, rounding_mode,
            reservation_quantity_source, reservation_confidence,
            dimensions_json, max_quantity, max_amount_micros, created_at_ms
        )
        VALUES ($1, $2, $3, $4, 'image.output', 'output:0', 'succeeded',
                'image_output', 'image', 1, 20000,
                'request_derived', 'exact', 'exact',
                'request_derived', 'exact',
                '{"resolution":"1k"}', $5, $6, 3)
        "#,
    )
    .bind(quote_line_id)
    .bind(quote_id)
    .bind(job_id)
    .bind(price_component_id)
    .bind(max_quantity)
    .bind(max_amount_micros)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn pricing_preview_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let service = PostgresPricingAdminService::new(pool.clone());
    publish_resolution_book(
        &service,
        "xai.preview.platform",
        "platform",
        None,
        None,
        "20000",
    )
    .await?;
    let resolution = resolution_request(None, None, 100);
    let exact_fact = UsageFact {
        usage_fact_id: Uuid::new_v4(),
        partition_key: "output:0".to_string(),
        authority_key: "output:0".to_string(),
        provider_id: "xai-grok".to_string(),
        provider_account_id: Some(Uuid::new_v4()),
        execution_surface: "provider_api".to_string(),
        fact_domain: "provider_benchmark".to_string(),
        metric: "image_output".to_string(),
        unit: "image".to_string(),
        quantity: "2".to_string(),
        outcome: "succeeded".to_string(),
        quantity_source: "provider_reported".to_string(),
        confidence: "exact".to_string(),
        dimensions: json!({"resolution": "1k"}),
    };
    let preview = service
        .preview(PricePreviewRequest {
            resolution: resolution.clone(),
            usage_facts: vec![exact_fact.clone()],
        })
        .await
        .map_err(|error| format!("published preview should rate: {error:?}"))?;
    require(
        preview.purpose == "provider_benchmark"
            && preview.billing_mode == "published_rate"
            && preview.total_amount_micros.as_deref() == Some("40000")
            && preview.lines.len() == 1,
        "preview must resolve and rate the immutable published version",
    )?;

    let mut derived_fact = exact_fact;
    derived_fact.usage_fact_id = Uuid::new_v4();
    derived_fact.quantity_source = "request_derived".to_string();
    derived_fact.confidence = "bounded".to_string();
    let error = service
        .preview(PricePreviewRequest {
            resolution,
            usage_facts: vec![derived_fact],
        })
        .await
        .expect_err("insufficient fact authority must fail preview");
    require(
        error.status_code().as_u16() == 400,
        "invalid preview facts must be rejected as a client configuration error",
    )
}

async fn billing_mode_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let service = PostgresPricingAdminService::new(pool.clone());
    let actual_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.actual.usd".to_string(),
            display_name: "xAI provider reported actual cost".to_string(),
            purpose: "provider_actual".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("actual book should be created: {error:?}"))?;
    let mut actual_draft = xai_image_draft("50000");
    actual_draft.billing_mode = "provider_reported".to_string();
    actual_draft.components.clear();
    let actual_version = service
        .create_version(
            actual_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: actual_draft,
            },
        )
        .await
        .map_err(|error| format!("actual draft should be created: {error:?}"))?;
    let actual_version = service
        .publish_version(
            actual_version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("provider-reported actual version should publish: {error:?}"))?;
    require(
        actual_version.state == "active"
            && actual_version.billing_mode == "provider_reported"
            && actual_version.components.is_empty(),
        "provider-reported actual cost must retain native facts without rate components",
    )?;

    let benchmark_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.invalid-benchmark.usd".to_string(),
            display_name: "Invalid xAI benchmark".to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("benchmark book should be created: {error:?}"))?;
    let mut benchmark_draft = xai_image_draft("50000");
    benchmark_draft.billing_mode = "provider_reported".to_string();
    benchmark_draft.components.clear();
    let benchmark_version = service
        .create_version(
            benchmark_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: benchmark_draft,
            },
        )
        .await
        .map_err(|error| format!("invalid benchmark draft should be stored: {error:?}"))?;
    let error = service
        .publish_version(
            benchmark_version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .expect_err("benchmark must not publish as provider-reported actual cost");
    require(
        error.status_code().as_u16() == 400,
        "incompatible purpose and billing mode must be rejected as invalid configuration",
    )?;

    let contract_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.invalid-contract-actual.usd".to_string(),
            display_name: "Invalid derived xAI actual cost".to_string(),
            purpose: "provider_actual".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("contract actual book should be created: {error:?}"))?;
    let mut contract_draft = xai_image_draft("50000");
    contract_draft.billing_mode = "contract_rate".to_string();
    contract_draft.components[0].quantity_source = "request_derived".to_string();
    contract_draft.components[0].required_confidence = "bounded".to_string();
    let contract_version = service
        .create_version(
            contract_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: contract_draft,
            },
        )
        .await
        .map_err(|error| format!("derived actual draft should be stored: {error:?}"))?;
    let error = service
        .publish_version(
            contract_version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .expect_err("derived quantities must not publish as provider actual cost");
    require(
        error.status_code().as_u16() == 400,
        "provider actual cost must require exact provider-reported quantities",
    )
}

async fn provider_allocated_cost_pool_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;

    let credential_pool_id = Uuid::new_v4();
    let provider_account_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools (
            credential_pool_id, pool_key, provider_id, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'pricing-allocation-pool', 'xai-grok', 'enabled', 1, 1)
        "#,
    )
    .bind(credential_pool_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_accounts (
            provider_account_id, credential_pool_id, provider_id,
            account_key, credential_ref, credential_revision,
            credential_auth_sha256, state, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 'xai-grok', 'pricing-allocation-account',
            'vault.pricing.allocation', 1, repeat('a', 64),
            'enabled', 1, 1
        )
        "#,
    )
    .bind(provider_account_id)
    .bind(credential_pool_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.subscription.allocated.usd".to_string(),
            display_name: "xAI subscription allocated cost".to_string(),
            purpose: "provider_allocated".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("allocated price book should be created: {error:?}"))?;
    let mut draft = xai_image_draft("1");
    draft.billing_mode = "subscription_allocation".to_string();
    for component in &mut draft.components {
        component.unit_price_micros = "0".to_string();
    }
    let version = service
        .create_version(book.price_book_id, CreatePriceBookVersionRequest { draft })
        .await
        .map_err(|error| format!("allocated version should be created: {error:?}"))?;
    let version = service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("allocated version should publish: {error:?}"))?;

    let allocation_pool_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_cost_allocation_pools (
            provider_cost_allocation_pool_id, semantic_key,
            provider_id, provider_account_id, price_book_version_id,
            period_start_ms, period_end_ms, currency,
            total_amount_micros, residual_amount_micros,
            allocation_basis, candidate_snapshot_hash,
            state, control_version,
            created_at_ms, closed_at_ms
        )
        VALUES (
            $1, 'xai.subscription.2026-07', 'xai-grok', $2, $3,
            1, 1000, 'USD', 100, 0,
            'successful_job',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'draft', 1, 1, NULL
        )
        "#,
    )
    .bind(allocation_pool_id)
    .bind(provider_account_id)
    .bind(version.price_book_version_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    expect_sqlstate(
        sqlx::query(
            r#"
            UPDATE provider_cost_allocation_pools
            SET state = 'closed', residual_amount_micros = 99,
                control_version = 2, closed_at_ms = 2
            WHERE provider_cost_allocation_pool_id = $1
            "#,
        )
        .bind(allocation_pool_id)
        .execute(pool)
        .await,
        "55000",
        "provider allocation residual mutation below the sealed value",
    )?;
    expect_sqlstate(
        sqlx::query(
            r#"
            UPDATE provider_cost_allocation_pools
            SET state = 'closed', residual_amount_micros = 101,
                control_version = 2, closed_at_ms = 2
            WHERE provider_cost_allocation_pool_id = $1
            "#,
        )
        .bind(allocation_pool_id)
        .execute(pool)
        .await,
        "55000",
        "provider allocation residual mutation above the sealed value",
    )?;

    expect_sqlstate(
        sqlx::query(
            r#"
        UPDATE provider_cost_allocation_pools
        SET state = 'closed', residual_amount_micros = 0,
            control_version = 2, closed_at_ms = 2
        WHERE provider_cost_allocation_pool_id = $1
        "#,
        )
        .bind(allocation_pool_id)
        .execute(pool)
        .await,
        "23514",
        "under-conserved provider allocation close",
    )?;

    let evidence_pool_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_cost_allocation_pools (
            provider_cost_allocation_pool_id, semantic_key,
            provider_id, provider_account_id, price_book_version_id,
            period_start_ms, period_end_ms, currency,
            total_amount_micros, residual_amount_micros,
            allocation_basis, candidate_snapshot_hash,
            state, control_version,
            created_at_ms, closed_at_ms
        )
        VALUES (
            $1, 'xai.subscription.2026-08', 'xai-grok', $2, $3,
            2000, 3000, 'USD', 100, 100,
            'successful_job',
            'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            'draft', 1, 1, NULL
        )
        "#,
    )
    .bind(evidence_pool_id)
    .bind(provider_account_id)
    .bind(version.price_book_version_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let unrelated_job_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO jobs (
            job_id, tenant_id, request_id, operation, provider_id, model,
            state, requested_units, output_count, billable_units,
            billing_metric, billing_unit, economics_contract_version,
            created_at_ms, updated_at_ms
        )
        VALUES (
            $1, 'org-allocation', 'allocation-without-evidence',
            'generation', 'xai-grok', 'grok-imagine-image',
            'reserved', 1, 1, 1, 'output', 'output', 4, 1, 1
        )
        "#,
    )
    .bind(unrelated_job_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    expect_sqlstate(
        sqlx::query(
            r#"
            INSERT INTO provider_cost_allocation_lines (
                provider_cost_allocation_line_id,
                provider_cost_allocation_pool_id,
                provider_id, provider_account_id, job_id, output_id,
                basis_usage_fact_id, basis_quantity, basis_unit,
                amount_micros, created_at_ms
            )
            VALUES (
                $1, $2, 'xai-grok', $3, $4, NULL,
                NULL, 1, 'job', 1, 1
            )
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(evidence_pool_id)
        .bind(provider_account_id)
        .bind(unrelated_job_id)
        .execute(pool)
        .await,
        "23514",
        "allocation line without immutable provider evidence",
    )
}

async fn price_resolution_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
            organization_id, display_name, organization_kind,
            created_at_ms, updated_at_ms
        )
        VALUES ('org-pricing', 'Pricing organization', 'system', 1, 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ('project-pricing', 'org-pricing', 'Pricing project', 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let service = PostgresPricingAdminService::new(pool.clone());
    publish_resolution_book(&service, "xai.platform", "platform", None, None, "20000").await?;
    publish_resolution_book(
        &service,
        "xai.organization",
        "organization",
        Some("org-pricing"),
        None,
        "30000",
    )
    .await?;
    publish_resolution_book(
        &service,
        "xai.project",
        "project",
        Some("org-pricing"),
        Some("project-pricing"),
        "40000",
    )
    .await?;

    let project_request = resolution_request(Some("org-pricing"), Some("project-pricing"), 100);
    let project_price = service
        .resolve_price_version(&project_request)
        .await
        .map_err(|error| format!("project price should resolve: {error:?}"))?;
    require(
        project_price.scope_type == "project"
            && project_price.version.components[0].unit_price_micros == "40000",
        "project price must override organization and platform prices",
    )?;

    let organization_price = service
        .resolve_price_version(&resolution_request(Some("org-pricing"), None, 100))
        .await
        .map_err(|error| format!("organization price should resolve: {error:?}"))?;
    require(
        organization_price.scope_type == "organization"
            && organization_price.version.components[0].unit_price_micros == "30000",
        "organization price must override the platform price",
    )?;

    let platform_price = service
        .resolve_price_version(&resolution_request(Some("another-org"), None, 100))
        .await
        .map_err(|error| format!("platform price should resolve: {error:?}"))?;
    require(
        platform_price.scope_type == "platform"
            && platform_price.version.components[0].unit_price_micros == "20000",
        "platform price must be the final fallback",
    )?;

    publish_resolution_book(
        &service,
        "xai.project.duplicate",
        "project",
        Some("org-pricing"),
        Some("project-pricing"),
        "50000",
    )
    .await?;
    require(
        service.resolve_price_version(&project_request).await
            == Err(PriceResolutionError::Ambiguous),
        "equal-precedence project prices must fail closed",
    )
}

async fn price_profile_alias_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    seed_dreamina_pricing_surface(pool).await?;
    let service = PostgresPricingAdminService::new(pool.clone());
    publish_dreamina_resolution_book(
        &service,
        "dreamina.customer.alias",
        "dreamina-cli-images-v1",
        "generation",
        Some("5.0"),
        "7",
    )
    .await?;

    let mut request = PriceResolutionRequest {
        purpose: "customer_sale".to_string(),
        organization_id: Some("tenant-a".to_string()),
        project_id: Some("project-a".to_string()),
        provider_id: Some("dreamina-cli".to_string()),
        currency: "USD".to_string(),
        api_profile: "volcengine-ark-images-v3".to_string(),
        operation: "generation".to_string(),
        provider_model_id: Some("5.0".to_string()),
        public_model_id: "doubao-seedream-5-0-lite".to_string(),
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_cli".to_string(),
        billing_mode: "customer_rate".to_string(),
        at_ms: system_now_ms()?,
    };
    let aliased = service
        .resolve_price_version(&request)
        .await
        .map_err(|error| format!("Ark alias price should resolve: {error:?}"))?;
    require(
        aliased.version.api_profile == "dreamina-cli-images-v1"
            && succeeded_rate(&aliased.version.components) == Some("7"),
        "Ark must use the Dreamina price source without changing its request identity",
    )?;

    publish_dreamina_resolution_book(
        &service,
        "dreamina.customer.ark-override",
        "volcengine-ark-images-v3",
        "*",
        None,
        "11",
    )
    .await?;
    request.at_ms = system_now_ms()?;
    let exact = service
        .resolve_price_version(&request)
        .await
        .map_err(|error| format!("Ark-specific price should resolve: {error:?}"))?;
    require(
        exact.version.api_profile == "volcengine-ark-images-v3"
            && succeeded_rate(&exact.version.components) == Some("11"),
        "an exact Ark profile price must beat a more specific Dreamina alias price",
    )
}

async fn seed_dreamina_pricing_surface(pool: &PgPool) -> TestResult {
    let route_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids,
            source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
        )
        VALUES (
            'dreamina-cli', '5.0', '5.0', 'image',
            'Dreamina Image 5.0', 'supported', 'enabled',
            ARRAY['images.generations'], 'adapter_contract',
            1, 1, '{}'::JSONB
        )
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes (
            route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind,
            selection_strategy, state, created_at_ms
        )
        VALUES (
            $1, 1, $2, 'Dreamina pricing test route', 'dreamina-cli',
            'images.generations', 'dreamina-cli.submit.v1',
            'account', 'quota_aware_least_loaded', 'enabled', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(format!("dreamina-pricing-{}", route_id.simple()))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id,
            command_schema, api_profile, public_model_id,
            provider_model_id, execution_model_id, media_kind, created_at_ms
        )
        VALUES
          ($1, 1, 'dreamina-cli', 'images.generations',
           'dreamina-cli.submit.v1', 'dreamina-cli-images-v1', '5.0',
           '5.0', '5.0', 'image', 1),
          ($1, 1, 'dreamina-cli', 'images.generations',
           'dreamina-cli.submit.v1', 'volcengine-ark-images-v3',
           'doubao-seedream-5-0-lite', '5.0', '5.0', 'image', 1)
        "#,
    )
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_platform_provider_routes (
            provider_id, operation_id, command_schema,
            route_id, route_revision, state, created_at_ms, updated_at_ms
        )
        VALUES (
            'dreamina-cli', 'images.generations', 'dreamina-cli.submit.v1',
            $1, 1, 'enabled', 1, 1
        )
        "#,
    )
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_codex_pricing_surface(pool: &PgPool) -> TestResult {
    let route_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids,
            source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
        )
        VALUES (
            'openai-codex', 'gpt-image-2', 'gpt-image-2', 'image',
            'GPT Image 2', 'supported', 'enabled',
            ARRAY['images.generations'], 'adapter_contract',
            1, 1, '{}'::JSONB
        )
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_routes (
            route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind,
            selection_strategy, state, created_at_ms
        )
        VALUES (
            $1, 1, $2, 'Codex pricing test route', 'openai-codex',
            'images.generations', 'openai.images.generation.v1',
            'account', 'quota_aware_least_loaded', 'enabled', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(format!("codex-pricing-{}", route_id.simple()))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_route_model_mappings (
            route_id, route_revision, provider_id, operation_id,
            command_schema, api_profile, public_model_id,
            provider_model_id, execution_model_id, media_kind, created_at_ms
        )
        VALUES (
            $1, 1, 'openai-codex', 'images.generations',
            'openai.images.generation.v1', 'openai-images-v1',
            'gpt-image-2', 'gpt-image-2', 'gpt-image-2', 'image', 1
        )
        "#,
    )
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_platform_provider_routes (
            provider_id, operation_id, command_schema,
            route_id, route_revision, state, created_at_ms, updated_at_ms
        )
        VALUES (
            'openai-codex', 'images.generations', 'openai.images.generation.v1',
            $1, 1, 'enabled', 1, 1
        )
        "#,
    )
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

fn succeeded_rate(components: &[gpt_image_2_gateway::pricing::PriceComponentView]) -> Option<&str> {
    components
        .iter()
        .find(|component| component.outcome == "succeeded")
        .map(|component| component.unit_price_micros.as_str())
}

async fn publish_dreamina_resolution_book(
    service: &PostgresPricingAdminService,
    key: &str,
    api_profile: &str,
    operation: &str,
    provider_model_id: Option<&str>,
    unit_price_micros: &str,
) -> TestResult {
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: key.to_string(),
            display_name: key.to_string(),
            purpose: "customer_sale".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("dreamina-cli".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("Dreamina price book should be created: {error:?}"))?;
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
                    api_profile: api_profile.to_string(),
                    operation: operation.to_string(),
                    provider_id: Some("dreamina-cli".to_string()),
                    provider_model_id: provider_model_id.map(str::to_string),
                    public_model_id: "*".to_string(),
                    media_kind: "image".to_string(),
                    service_tier: "standard".to_string(),
                    execution_surface: "provider_cli".to_string(),
                    billing_mode: "customer_rate".to_string(),
                    is_free: false,
                    effective_from_ms: 1,
                    source_kind: "official_document".to_string(),
                    source_url: Some("https://www.volcengine.com/docs/82379/1544106".to_string()),
                    source_checked_at_ms: Some(1),
                    notes: None,
                    components: ["succeeded", "failed", "no_effect"]
                        .into_iter()
                        .map(|outcome| PriceComponentDraft {
                            component_key: format!("image-output-{outcome}"),
                            metric: "image_output".to_string(),
                            unit: "image".to_string(),
                            unit_size: "1".to_string(),
                            unit_price_micros: if outcome == "succeeded" {
                                unit_price_micros.to_string()
                            } else {
                                "0".to_string()
                            },
                            outcome: outcome.to_string(),
                            quantity_source: "request_derived".to_string(),
                            required_confidence: "exact".to_string(),
                            rounding_mode: "exact".to_string(),
                            dimensions: json!({}),
                        })
                        .collect(),
                },
            },
        )
        .await
        .map_err(|error| format!("Dreamina price version should be created: {error:?}"))?;
    service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("Dreamina price version should publish: {error:?}"))?;
    Ok(())
}

async fn publish_resolution_book(
    service: &PostgresPricingAdminService,
    key: &str,
    scope_type: &str,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    unit_price_micros: &str,
) -> TestResult {
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: key.to_string(),
            display_name: key.to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: scope_type.to_string(),
            organization_id: organization_id.map(str::to_string),
            project_id: project_id.map(str::to_string),
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("resolution price book should be created: {error:?}"))?;
    let draft = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: xai_image_draft(unit_price_micros),
            },
        )
        .await
        .map_err(|error| format!("resolution draft should be created: {error:?}"))?;
    service
        .publish_version(
            draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("resolution draft should publish: {error:?}"))?;
    Ok(())
}

fn resolution_request(
    organization_id: Option<&str>,
    project_id: Option<&str>,
    at_ms: i64,
) -> PriceResolutionRequest {
    PriceResolutionRequest {
        purpose: "provider_benchmark".to_string(),
        organization_id: organization_id.map(str::to_string),
        project_id: project_id.map(str::to_string),
        provider_id: Some("xai-grok".to_string()),
        currency: "USD".to_string(),
        api_profile: "xai-images-v1".to_string(),
        operation: "image_generation".to_string(),
        provider_model_id: Some("grok-imagine-image-quality".to_string()),
        public_model_id: "grok-imagine-image-quality".to_string(),
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_api".to_string(),
        billing_mode: "published_rate".to_string(),
        at_ms,
    }
}

async fn pricing_service_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let service = PostgresPricingAdminService::new(pool.clone());
    let price_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.benchmark.usd".to_string(),
            display_name: "xAI official benchmark".to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("price book should be created: {error:?}"))?;
    let draft = service
        .create_version(
            price_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: xai_image_draft("20000"),
            },
        )
        .await
        .map_err(|error| format!("draft should be created: {error:?}"))?;
    require(draft.state == "draft", "new version must be a draft")?;
    require(draft.control_version == "1", "new draft version must be 1")?;

    let updated = service
        .update_draft_version(
            draft.price_book_version_id,
            UpdatePriceBookVersionRequest {
                expected_control_version: 1,
                draft: xai_image_draft("50000"),
            },
        )
        .await
        .map_err(|error| format!("draft should update: {error:?}"))?;
    require(
        updated.control_version == "2",
        "draft update must advance control version",
    )?;
    require(
        updated.components[0].unit_price_micros == "50000",
        "updated component price must be returned",
    )?;

    let stale_error = service
        .update_draft_version(
            draft.price_book_version_id,
            UpdatePriceBookVersionRequest {
                expected_control_version: 1,
                draft: xai_image_draft("70000"),
            },
        )
        .await
        .expect_err("stale update must fail");
    require(
        stale_error.status_code().as_u16() == 409,
        "stale update must return conflict",
    )?;

    let published = service
        .publish_version(
            draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 2,
            },
        )
        .await
        .map_err(|error| format!("draft should publish: {error:?}"))?;
    require(
        published.state == "active" && published.control_version == "3",
        "publish must activate and advance control version",
    )?;

    let active_update_error = service
        .update_draft_version(
            draft.price_book_version_id,
            UpdatePriceBookVersionRequest {
                expected_control_version: 3,
                draft: xai_image_draft("70000"),
            },
        )
        .await
        .expect_err("active version must not be editable");
    require(
        active_update_error.status_code().as_u16() == 409,
        "active update must return conflict",
    )?;

    let retirement_error = service
        .retire_version(
            draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 3,
            },
        )
        .await
        .expect_err("currently effective version must not retire without replacement");
    require(
        retirement_error.status_code().as_u16() == 409,
        "unsafe current retirement must return conflict",
    )?;

    let catalog = service
        .catalog()
        .await
        .map_err(|error| format!("catalog should load: {error:?}"))?;
    let catalog_book = catalog
        .price_books
        .iter()
        .find(|book| book.price_book_id == price_book.price_book_id)
        .ok_or_else(|| "catalog is missing the created price book".to_string())?;
    require(
        catalog_book.versions.len() == 1 && catalog_book.versions[0].state == "active",
        "blocked retirement must leave the current price unchanged",
    )
}

async fn scheduled_price_cutover_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let service = PostgresPricingAdminService::new(pool.clone());
    let price_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.scheduled-cutover.usd".to_string(),
            display_name: "xAI scheduled cutover".to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("price book should be created: {error:?}"))?;

    let first = create_scheduled_version(&service, price_book.price_book_id, 100, "20000").await?;
    let second = create_scheduled_version(&service, price_book.price_book_id, 200, "50000").await?;
    let middle = create_scheduled_version(&service, price_book.price_book_id, 175, "35000").await?;

    let catalog = service
        .catalog()
        .await
        .map_err(|error| format!("catalog should load: {error:?}"))?;
    let versions = &catalog
        .price_books
        .iter()
        .find(|book| book.price_book_id == price_book.price_book_id)
        .ok_or_else(|| "catalog is missing the scheduled price book".to_string())?
        .versions;
    let version = |id| {
        versions
            .iter()
            .find(|version| version.price_book_version_id == id)
            .unwrap_or_else(|| panic!("missing price version {id}"))
    };
    require(
        version(first).effective_until_ms == Some(175)
            && version(middle).effective_until_ms == Some(200)
            && version(second).effective_until_ms.is_none(),
        "scheduled publication must atomically form adjacent effective intervals",
    )?;

    for (at_ms, expected_price) in [(150, "20000"), (180, "35000"), (250, "50000")] {
        let mut request = resolution_request(None, None, at_ms);
        request.execution_surface = "provider_api".to_string();
        let resolved = service
            .resolve_price_version(&request)
            .await
            .map_err(|error| format!("scheduled price should resolve at {at_ms}: {error:?}"))?;
        require(
            resolved.version.components[0].unit_price_micros == expected_price,
            &format!("unexpected price at {at_ms}"),
        )?;
    }

    let duplicate = service
        .create_version(
            price_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: scheduled_xai_image_draft(200, "70000"),
            },
        )
        .await
        .map_err(|error| format!("duplicate-time draft should be created: {error:?}"))?;
    let conflict = service
        .publish_version(
            duplicate.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .expect_err("duplicate effective time must fail closed");
    require(
        conflict.status_code().as_u16() == 409,
        "duplicate effective time must return conflict",
    )?;

    let cancellation_book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.scheduled-cancellation.usd".to_string(),
            display_name: "xAI scheduled cancellation".to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok-cancellation".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("cancellation price book should be created: {error:?}"))?;
    let current = create_scheduled_version_for_provider(
        &service,
        cancellation_book.price_book_id,
        "xai-grok-cancellation",
        1,
        "20000",
    )
    .await?;
    let future_start = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock should be valid: {error}"))?
            .as_millis(),
    )
    .map_err(|error| format!("system time should fit i64: {error}"))?
        + 60_000;
    let future = create_scheduled_version_for_provider(
        &service,
        cancellation_book.price_book_id,
        "xai-grok-cancellation",
        future_start,
        "50000",
    )
    .await?;
    let cancelled = service
        .retire_version(
            future,
            TransitionPriceBookVersionRequest {
                expected_control_version: 2,
            },
        )
        .await
        .map_err(|error| format!("future version should be cancellable: {error:?}"))?;
    require(
        cancelled.state == "retired",
        "cancelled future version must be retired",
    )?;
    let catalog = service
        .catalog()
        .await
        .map_err(|error| format!("catalog should reload after cancellation: {error:?}"))?;
    let cancellation_versions = &catalog
        .price_books
        .iter()
        .find(|book| book.price_book_id == cancellation_book.price_book_id)
        .expect("cancellation price book")
        .versions;
    require(
        cancellation_versions
            .iter()
            .find(|version| version.price_book_version_id == current)
            .expect("current cancellation version")
            .effective_until_ms
            .is_none(),
        "cancelling a future cutover must restore the predecessor interval",
    )?;
    let mut request = resolution_request(None, None, future_start + 1);
    request.provider_id = Some("xai-grok-cancellation".to_string());
    request.execution_surface = "provider_api".to_string();
    let resolved = service
        .resolve_price_version(&request)
        .await
        .map_err(|error| format!("predecessor should resolve after cancellation: {error:?}"))?;
    require(
        resolved.version.price_book_version_id == current,
        "cancelled future version must not leave a pricing gap",
    )
}

async fn price_rollback_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    let actor = PricingTransitionActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    sqlx::query(
        r#"
        INSERT INTO identity_users (
            user_id, normalized_email, display_name, roles, scopes,
            authz_version, failed_login_count, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, 'pricing-rollback@test.local', 'Pricing rollback operator',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(actor.user_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock should be valid: {error}"))?
            .as_millis(),
    )
    .map_err(|error| format!("system time should fit i64: {error}"))?;
    let first_start = now + 60_000;
    let second_start = first_start + 60_000;
    let rollback_start = second_start + 60_000;
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "xai.rollback.usd".to_string(),
            display_name: "xAI rollback".to_string(),
            purpose: "provider_benchmark".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("rollback price book should be created: {error:?}"))?;
    let first =
        create_scheduled_version(&service, book.price_book_id, first_start, "20000").await?;
    let second =
        create_scheduled_version(&service, book.price_book_id, second_start, "50000").await?;
    let rollback = service
        .create_rollback_draft(
            first,
            CreatePriceRollbackDraftRequest {
                effective_from_ms: rollback_start,
            },
            actor,
        )
        .await
        .map_err(|error| format!("historical version should clone: {error:?}"))?;
    require(
        rollback.source_version_id == first
            && rollback.draft.state == "draft"
            && rollback.draft.components[0].unit_price_micros == "20000",
        "rollback must clone the immutable source into a new draft",
    )?;

    let before = service
        .resolve_price_version(&resolution_request(None, None, rollback_start - 1))
        .await
        .map_err(|error| format!("current price should resolve before rollback: {error:?}"))?;
    require(
        before.version.price_book_version_id == second,
        "creating a rollback draft must not alter the current price",
    )?;
    service
        .publish_version(
            rollback.draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("rollback draft should publish: {error:?}"))?;
    let after = service
        .resolve_price_version(&resolution_request(None, None, rollback_start))
        .await
        .map_err(|error| format!("rollback price should resolve at cutover: {error:?}"))?;
    require(
        after.version.price_book_version_id == rollback.draft.price_book_version_id
            && after.version.components[0].unit_price_micros == "20000",
        "rollback publication must atomically restore the historical rate",
    )?;

    let catalog = service
        .catalog()
        .await
        .map_err(|error| format!("rollback catalog should load: {error:?}"))?;
    let versions = &catalog
        .price_books
        .iter()
        .find(|catalog_book| catalog_book.price_book_id == book.price_book_id)
        .ok_or_else(|| "catalog is missing the rollback price book".to_string())?
        .versions;
    require(
        versions
            .iter()
            .find(|version| version.price_book_version_id == first)
            .is_some_and(|version| version.effective_until_ms == Some(second_start))
            && versions
                .iter()
                .find(|version| version.price_book_version_id == second)
                .is_some_and(|version| version.effective_until_ms == Some(rollback_start)),
        "rollback must preserve history and form adjacent effective intervals",
    )?;
    expect_sqlstate(
        sqlx::query(
            "UPDATE price_book_version_rollbacks SET source_version_id = $2 WHERE rollback_version_id = $1",
        )
        .bind(rollback.draft.price_book_version_id)
        .bind(second)
        .execute(pool)
        .await,
        "55000",
        "mutating immutable rollback lineage",
    )
}

async fn create_scheduled_version(
    service: &PostgresPricingAdminService,
    price_book_id: Uuid,
    effective_from_ms: i64,
    unit_price_micros: &str,
) -> Result<Uuid, String> {
    create_scheduled_version_for_provider(
        service,
        price_book_id,
        "xai-grok",
        effective_from_ms,
        unit_price_micros,
    )
    .await
}

async fn create_scheduled_version_for_provider(
    service: &PostgresPricingAdminService,
    price_book_id: Uuid,
    provider_id: &str,
    effective_from_ms: i64,
    unit_price_micros: &str,
) -> Result<Uuid, String> {
    let mut draft = scheduled_xai_image_draft(effective_from_ms, unit_price_micros);
    draft.provider_id = Some(provider_id.to_string());
    let draft = service
        .create_version(price_book_id, CreatePriceBookVersionRequest { draft })
        .await
        .map_err(|error| format!("scheduled draft should be created: {error:?}"))?;
    let published = service
        .publish_version(
            draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("scheduled draft should publish: {error:?}"))?;
    require(
        published.state == "active",
        "scheduled version must be published",
    )?;
    Ok(published.price_book_version_id)
}

fn scheduled_xai_image_draft(
    effective_from_ms: i64,
    unit_price_micros: &str,
) -> PriceBookVersionDraft {
    let mut draft = xai_image_draft(unit_price_micros);
    draft.effective_from_ms = effective_from_ms;
    draft
}

fn xai_image_draft(unit_price_micros: &str) -> PriceBookVersionDraft {
    PriceBookVersionDraft {
        api_profile: "xai-images-v1".to_string(),
        operation: "image_generation".to_string(),
        provider_id: Some("xai-grok".to_string()),
        provider_model_id: Some("grok-imagine-image-quality".to_string()),
        public_model_id: "grok-imagine-image-quality".to_string(),
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_api".to_string(),
        billing_mode: "published_rate".to_string(),
        is_free: false,
        effective_from_ms: 1,
        source_kind: "official_document".to_string(),
        source_url: Some("https://docs.x.ai/developers/pricing".to_string()),
        source_checked_at_ms: Some(1),
        notes: None,
        components: vec![PriceComponentDraft {
            component_key: "image_output_1k".to_string(),
            metric: "image_output".to_string(),
            unit: "image".to_string(),
            unit_size: "1".to_string(),
            unit_price_micros: unit_price_micros.to_string(),
            outcome: "succeeded".to_string(),
            quantity_source: "provider_reported".to_string(),
            required_confidence: "exact".to_string(),
            rounding_mode: "exact".to_string(),
            dimensions: json!({"resolution": "1k"}),
        }],
    }
}

async fn price_book_lifecycle_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;

    let price_book_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO price_books (
            price_book_id, price_book_key, display_name, purpose, scope_type,
            provider_id, currency, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, 'openai.customer.usd', 'OpenAI customer USD',
                'customer_sale', 'platform', NULL, 'USD', 'active', 1, 1)
        "#,
    )
    .bind(price_book_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let version_one_id = Uuid::new_v4();
    insert_version(pool, version_one_id, price_book_id, 1).await?;
    let component_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO price_components (
            price_component_id, price_book_version_id, component_key,
            metric, unit, unit_size, unit_price_micros, outcome,
            quantity_source, rounding_mode, created_at_ms
        )
        VALUES ($1, $2, 'image_output_token',
                'image_output_token', 'token', 1000000, 30000000,
                'succeeded', 'provider_reported', 'ceil', 1)
        "#,
    )
    .bind(component_id)
    .bind(version_one_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    expect_sqlstate(
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, rounding_mode, created_at_ms
            )
            VALUES ($1, $2, 'invalid_provider_cost',
                    'provider_reported_cost', 'usd_tick', 10000000000, 1000000,
                    'any', 'provider_reported', 'exact', 1)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_one_id)
        .execute(pool)
        .await,
        "23514",
        "provider cost component with a non-cost quantity source",
    )?;

    sqlx::query(
        "UPDATE price_components SET unit_price_micros = 31000000 WHERE price_component_id = $1",
    )
    .bind(component_id)
    .execute(pool)
    .await
    .map_err(|error| format!("draft component should be editable: {error}"))?;
    bind_test_surface_contract(
        pool,
        version_one_id,
        "image_generation",
        "openai-codex",
        "gpt-image-2",
        "provider_api",
    )
    .await?;
    sqlx::query(
        "UPDATE price_book_versions SET state = 'active', updated_at_ms = 2 WHERE price_book_version_id = $1",
    )
    .bind(version_one_id)
    .execute(pool)
    .await
    .map_err(|error| format!("draft version should publish: {error}"))?;

    expect_sqlstate(
        sqlx::query(
            "UPDATE price_components SET unit_price_micros = 32000000 WHERE price_component_id = $1",
        )
        .bind(component_id)
        .execute(pool)
        .await,
        "55000",
        "published component mutation",
    )?;
    expect_sqlstate(
        sqlx::query("DELETE FROM price_book_versions WHERE price_book_version_id = $1")
            .bind(version_one_id)
            .execute(pool)
            .await,
        "55000",
        "published version deletion",
    )?;
    expect_sqlstate(
        sqlx::query("UPDATE price_books SET currency = 'CNY' WHERE price_book_id = $1")
            .bind(price_book_id)
            .execute(pool)
            .await,
        "55000",
        "published price book semantic mutation",
    )?;
    expect_sqlstate(
        sqlx::query("TRUNCATE provider_usage_facts")
            .execute(pool)
            .await,
        "0A000",
        "provider usage fact truncate with dependent immutable rating facts",
    )?;

    let version_two_id = Uuid::new_v4();
    insert_version(pool, version_two_id, price_book_id, 2).await?;
    expect_sqlstate(
        sqlx::query(
            "UPDATE price_components SET price_book_version_id = $2 WHERE price_component_id = $1",
        )
        .bind(component_id)
        .bind(version_two_id)
        .execute(pool)
        .await,
        "55000",
        "published component reparenting",
    )?;
    expect_sqlstate(
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, rounding_mode, created_at_ms
            )
            VALUES ($1, $2, 'invalid_metric_unit',
                    'video_output_token', 'second', 1, 1,
                    'succeeded', 'provider_reported', 'exact', 1)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_two_id)
        .execute(pool)
        .await,
        "23514",
        "invalid metric and unit combination",
    )?;
    sqlx::query(
        r#"
        INSERT INTO price_components (
            price_component_id, price_book_version_id, component_key,
            metric, unit, unit_size, unit_price_micros, outcome,
            quantity_source, rounding_mode, created_at_ms
        )
        VALUES ($1, $2, 'image_output_token',
                'image_output_token', 'token', 1000000, 30000000,
                'succeeded', 'provider_reported', 'ceil', 1)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(version_two_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    bind_test_surface_contract(
        pool,
        version_two_id,
        "image_generation",
        "openai-codex",
        "gpt-image-2",
        "provider_api",
    )
    .await?;
    expect_sqlstate(
        sqlx::query(
            "UPDATE price_book_versions SET state = 'active', updated_at_ms = 3 WHERE price_book_version_id = $1",
        )
        .bind(version_two_id)
        .execute(pool)
        .await,
        "23P01",
        "overlapping active version",
    )?;

    sqlx::query(
        r#"
        UPDATE price_book_versions
        SET state = 'retired', effective_until_ms = 10, updated_at_ms = 10
        WHERE price_book_version_id = $1
        "#,
    )
    .bind(version_one_id)
    .execute(pool)
    .await
    .map_err(|error| format!("active version should retire: {error}"))?;
    sqlx::query(
        "UPDATE price_book_versions SET state = 'active', updated_at_ms = 11 WHERE price_book_version_id = $1",
    )
    .bind(version_two_id)
    .execute(pool)
    .await
    .map_err(|error| format!("replacement version should publish: {error}"))?;

    expect_sqlstate(
        sqlx::query(
            r#"
            INSERT INTO price_components (
                price_component_id, price_book_version_id, component_key,
                metric, unit, unit_size, unit_price_micros, outcome,
                quantity_source, rounding_mode, created_at_ms
            )
            VALUES ($1, $2, 'invalid_provider_cost',
                    'provider_reported_cost', 'usd_tick', 10000000000, 1000000,
                    'any', 'provider_reported', 'exact', 1)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(version_two_id)
        .execute(pool)
        .await,
        "55000",
        "component insert after publication",
    )?;

    expect_sqlstate(
        sqlx::query(
            r#"
            INSERT INTO price_books (
                price_book_id, price_book_key, display_name, purpose,
                scope_type, currency, state, created_at_ms, updated_at_ms
            )
            VALUES ($1, 'invalid.actual', 'Invalid actual cost',
                    'provider_actual', 'platform', 'USD', 'active', 1, 1)
            "#,
        )
        .bind(Uuid::new_v4())
        .execute(pool)
        .await,
        "23514",
        "provider cost book without provider",
    )
}

async fn insert_version(
    pool: &PgPool,
    version_id: Uuid,
    price_book_id: Uuid,
    version: i32,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO price_book_versions (
            price_book_version_id, price_book_id, version,
            api_profile, operation, provider_id, provider_model_id,
            public_model_id, media_kind, service_tier,
            execution_surface, billing_mode, is_free, state,
            effective_from_ms, source_kind, source_url,
            source_checked_at_ms, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 'openai-images-v1', 'image_generation',
                'openai-codex', 'gpt-image-2', 'gpt-image-2',
                'image', 'standard', 'provider_api', 'customer_rate', FALSE,
                'draft', 1, 'official_document',
                'https://developers.openai.com/api/docs/pricing', 1, 1, 1)
        "#,
    )
    .bind(version_id)
    .bind(price_book_id)
    .bind(version)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn bind_test_surface_contract(
    pool: &PgPool,
    price_book_version_id: Uuid,
    operation: &str,
    provider_id: &str,
    provider_model_id: &str,
    execution_surface: &str,
) -> TestResult {
    let contract_key = format!("test.surface.{price_book_version_id}");
    let contract_hash = hex::encode(sha2::Sha256::digest(contract_key.as_bytes()));
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
            $1, 1, $2, 1, 'openai-images-v1', $3,
            $4, $5, 'gpt-image-2', 'image', 'standard', $6,
            'test.normalizer', 1, '{}'::JSONB, 1
        )
        "#,
    )
    .bind(&contract_key)
    .bind(&contract_hash)
    .bind(operation)
    .bind(provider_id)
    .bind(provider_model_id)
    .bind(execution_surface)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO price_book_version_surface_contract_bindings (
            price_book_version_id, contract_key, contract_revision,
            contract_hash, bound_at_ms
        )
        VALUES ($1, $2, 1, $3, 1)
        "#,
    )
    .bind(price_book_version_id)
    .bind(contract_key)
    .bind(contract_hash)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

fn expect_sqlstate<T>(
    result: Result<T, sqlx::Error>,
    expected: &str,
    operation: &str,
) -> TestResult {
    let error = match result {
        Ok(_) => return Err(format!("{operation} unexpectedly succeeded")),
        Err(error) => error,
    };
    let actual = error
        .as_database_error()
        .and_then(|error| error.code())
        .map(|code| code.into_owned());
    if actual.as_deref() != Some(expected) {
        return Err(format!(
            "{operation} returned SQLSTATE {actual:?}, expected {expected}: {error}"
        ));
    }
    Ok(())
}

fn debug_error(error: sqlx::Error) -> String {
    format!("{error:?}")
}

fn system_now_ms() -> TestResult<i64> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("{error:?}"))?
            .as_millis(),
    )
    .map_err(|error| format!("{error:?}"))
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new() -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL price book test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("price_book_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, 4, &name)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because database {database_name:?} is not a test database"
            ));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}
