use super::*;
use gpt_image_2_gateway::pricing::PriceBookVersionView;
use std::collections::BTreeSet;

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

#[tokio::test]
async fn dreamina_workflow_prices_publish_one_binding_per_exact_surface() -> TestResult {
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = workflow_binding_case(&schema.pool).await;
    result.and(schema.cleanup().await)
}

async fn workflow_binding_case(pool: &PgPool) -> TestResult {
    run_migrations(pool).await.map_err(debug_error)?;
    seed_dreamina_pricing_surface(pool).await?;
    seed_video_surface(pool).await?;
    sqlx::raw_sql(
        "INSERT INTO identity_organizations
         (organization_id, display_name, organization_kind, created_at_ms, updated_at_ms)
         VALUES ('org-workflow', 'Workflow test', 'system', 1, 1);
         INSERT INTO gateway_projects (id, tenant_id, name, created_at)
         VALUES ('project-workflow', 'org-workflow', 'Workflow test', 1);",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "test.dreamina.workflow.usd".into(),
            display_name: "Synthetic workflow prices".into(),
            purpose: "customer_sale".into(),
            scope_type: "platform".into(),
            organization_id: None,
            project_id: None,
            provider_id: Some("dreamina-cli".into()),
            currency: "USD".into(),
        })
        .await
        .map_err(debug_error)?;

    for video in [true, false] {
        let mut single = workflow_draft(video);
        single
            .components
            .retain(|component| component.dimensions.get("workflow").is_none());
        let legacy = publish_fixture(&service, book.price_book_id, single).await?;
        let legacy_binding = bindings(pool, legacy.price_book_version_id).await?;
        require(legacy_binding.len() == 1, "single workflow must bind once")?;
        require(
            legacy_binding[0].4.get("contracts").is_none(),
            "a single workflow must retain its original snapshot format",
        )?;

        let mut mixed = workflow_draft(video);
        require(
            mixed.components.len() == if video { 20 } else { 6 },
            "fixture must cover all terminal fallbacks and resolution overrides",
        )?;
        mixed.effective_from_ms = legacy.effective_from_ms + 1;
        let published = publish_fixture(&service, book.price_book_id, mixed).await?;
        let bound = bindings(pool, published.price_book_version_id).await?;
        require(
            bound.len() == 1,
            "multiple workflows must bind exactly once per API/model surface",
        )?;
        let (key, revision, hash, normalizer, snapshot) = &bound[0];
        require(
            key.starts_with("dreamina-cli.workflow-set:")
                && *revision > 0
                && hash.len() == 64
                && normalizer == "dreamina-cli.workflow-set.v1"
                && snapshot["schema_version"] == 1,
            "multi-workflow publication must persist the versioned workflow set",
        )?;
        let contracts = snapshot["contracts"]
            .as_array()
            .ok_or("workflow set must contain complete contract snapshots")?;
        let expected = if video {
            BTreeSet::from([
                "dreamina-cli.submit.video.v1",
                "image2video",
                "frames2video",
                "multimodal2video",
            ])
        } else {
            BTreeSet::from(["dreamina-cli.submit.image.v1", "image2image"])
        };
        let actual = contracts
            .iter()
            .filter_map(|contract| contract["contract"]["normalizer_key"].as_str())
            .collect::<BTreeSet<_>>();
        require(
            contracts.len() == expected.len()
                && actual == expected
                && contracts.contains(&legacy_binding[0].4)
                && contracts.iter().all(|contract| {
                    contract["exact_surface"] == snapshot["exact_surface"]
                        && contract["contract"]["metering_bases"].is_array()
                        && contract["contract"]["constraints"].is_array()
                }),
            "workflow set must preserve every complete contract including the legacy snapshot",
        )?;
        require(
            bindings(pool, legacy.price_book_version_id).await? == legacy_binding,
            "publishing a workflow set must not alter the historical binding or contract",
        )?;
        expect_sqlstate(
            sqlx::query(
                "UPDATE price_book_version_surface_contract_bindings
                 SET bound_at_ms = bound_at_ms + 1 WHERE price_book_version_id = $1",
            )
            .bind(legacy.price_book_version_id)
            .execute(pool)
            .await,
            "55000",
            "historical workflow binding mutation",
        )?;
        expect_sqlstate(
            sqlx::query(
                "UPDATE pricing_surface_contract_revisions
                 SET normalizer_revision = normalizer_revision + 1
                 WHERE contract_key = $1 AND revision = $2",
            )
            .bind(&legacy_binding[0].0)
            .bind(legacy_binding[0].1)
            .execute(pool)
            .await,
            "55000",
            "historical workflow contract mutation",
        )?;
        for workflow in expected {
            assert_quote_binding(pool, book.price_book_id, &published, workflow).await?;
        }
        let mut repriced = workflow_draft(video);
        repriced.effective_from_ms = published.effective_from_ms + 1;
        repriced.components[0].unit_price_micros = "104000".into();
        let next = publish_fixture(&service, book.price_book_id, repriced).await?;
        require(
            bindings(pool, next.price_book_version_id).await? == bound,
            "repricing the same workflow set must reuse its immutable contract",
        )?;
    }
    Ok(())
}

type Binding = (String, i64, String, String, serde_json::Value);

async fn bindings(pool: &PgPool, version: Uuid) -> TestResult<Vec<Binding>> {
    sqlx::query_as(
        "SELECT binding.contract_key, binding.contract_revision, binding.contract_hash,
                contract.normalizer_key, contract.contract_json
         FROM price_book_version_surface_contract_bindings binding
         JOIN pricing_surface_contract_revisions contract
           ON contract.contract_key = binding.contract_key
          AND contract.revision = binding.contract_revision
          AND contract.contract_hash = binding.contract_hash
         WHERE binding.price_book_version_id = $1 ORDER BY binding.contract_key",
    )
    .bind(version)
    .fetch_all(pool)
    .await
    .map_err(debug_error)
}

async fn publish_fixture(
    service: &PostgresPricingAdminService,
    book: Uuid,
    draft: PriceBookVersionDraft,
) -> TestResult<PriceBookVersionView> {
    let version = service
        .create_version(book, CreatePriceBookVersionRequest { draft })
        .await
        .map_err(debug_error)?;
    let ready = service
        .publish_readiness(version.price_book_version_id)
        .await
        .map_err(debug_error)?;
    require(
        ready.ready,
        &format!("workflow draft must be ready: {ready:?}"),
    )?;
    service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("ready workflow draft must publish: {error:?}"))
}

fn workflow_draft(video: bool) -> PriceBookVersionDraft {
    let workflows: &[Option<&str>] = if video {
        &[
            None,
            Some("image2video"),
            Some("frames2video"),
            Some("multimodal2video"),
        ]
    } else {
        &[None, Some("image2image")]
    };
    let mut components = Vec::new();
    for workflow in workflows {
        let label = workflow.unwrap_or("text");
        for (outcome, resolution, rate) in [
            ("succeeded", None, "103000"),
            ("failed", None, "0"),
            ("no_effect", None, "0"),
            ("succeeded", Some("720p"), "231000"),
            ("succeeded", Some("1080p"), "569000"),
        ] {
            if !video && resolution.is_some() {
                continue;
            }
            let mut dimensions = json!({});
            if let Some(workflow) = workflow {
                dimensions["workflow"] = json!(workflow);
            }
            if let Some(resolution) = resolution {
                dimensions["resolution"] = json!(resolution);
            }
            components.push(PriceComponentDraft {
                component_key: format!("{label}.{outcome}.{}", resolution.unwrap_or("base")),
                metric: if video {
                    "video_requested_second"
                } else {
                    "image_output"
                }
                .into(),
                unit: if video { "second" } else { "image" }.into(),
                unit_size: "1".into(),
                unit_price_micros: rate.into(),
                outcome: outcome.into(),
                quantity_source: "request_derived".into(),
                required_confidence: "exact".into(),
                rounding_mode: "exact".into(),
                dimensions,
            });
        }
    }
    PriceBookVersionDraft {
        api_profile: if video {
            "volcengine-ark-content-generation-v3"
        } else {
            "volcengine-ark-images-v3"
        }
        .into(),
        operation: if video {
            "video_generation"
        } else {
            "generation"
        }
        .into(),
        provider_id: Some("dreamina-cli".into()),
        provider_model_id: Some(if video { "seedance2.5" } else { "5.0" }.into()),
        public_model_id: if video {
            "doubao-seedance-2-5-260628"
        } else {
            "doubao-seedream-5-0-lite"
        }
        .into(),
        media_kind: if video { "video" } else { "image" }.into(),
        service_tier: "standard".into(),
        execution_surface: "provider_cli".into(),
        billing_mode: "customer_rate".into(),
        is_free: false,
        effective_from_ms: 1,
        source_kind: "manual".into(),
        source_url: None,
        source_checked_at_ms: None,
        notes: Some("Synthetic workflow publication regression fixture".into()),
        components,
    }
}

async fn seed_video_surface(pool: &PgPool) -> TestResult {
    let route = Uuid::new_v4();
    sqlx::raw_sql(AssertSqlSafe(format!(
        "INSERT INTO provider_models
         (provider_id, model_id, execution_model_id, media_kind, display_name,
          adapter_state, lifecycle_state, operation_ids, source_kind,
          first_seen_at_ms, last_seen_at_ms, metadata_json)
         VALUES ('dreamina-cli', 'seedance2.5', 'seedance2.5', 'video', 'Synthetic video',
                 'supported', 'enabled', ARRAY['videos.generations'], 'adapter_contract', 1, 1, '{{}}');
         INSERT INTO provider_routes
         (route_id, revision, route_key, display_name, provider_id, operation_id,
          command_schema, route_kind, selection_strategy, state, created_at_ms)
         VALUES ('{route}', 1, 'workflow-video-test', 'Synthetic video', 'dreamina-cli',
                 'videos.generations', 'dreamina-cli.submit.v1', 'account',
                 'quota_aware_least_loaded', 'enabled', 1);
         INSERT INTO provider_route_model_mappings
         (route_id, route_revision, provider_id, operation_id, command_schema,
          api_profile, public_model_id, provider_model_id, execution_model_id, media_kind, created_at_ms)
         VALUES ('{route}', 1, 'dreamina-cli', 'videos.generations', 'dreamina-cli.submit.v1',
                 'volcengine-ark-content-generation-v3', 'doubao-seedance-2-5-260628',
                 'seedance2.5', 'seedance2.5', 'video', 1);
         INSERT INTO gateway_platform_provider_routes
         (provider_id, operation_id, command_schema, route_id, route_revision, state, created_at_ms, updated_at_ms)
         VALUES ('dreamina-cli', 'videos.generations', 'dreamina-cli.submit.v1', '{route}', 1, 'enabled', 1, 1);"
    )))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn assert_quote_binding(
    pool: &PgPool,
    book: Uuid,
    version: &PriceBookVersionView,
    workflow: &str,
) -> TestResult {
    let mut tx = pool.begin().await.map_err(debug_error)?;
    let job = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO jobs
         (job_id, tenant_id, request_id, operation, provider_id, model, state,
          requested_units, output_count, billable_units, billing_metric, billing_unit,
          economics_contract_version, created_at_ms, updated_at_ms)
         VALUES ($1, 'org-workflow', $2, $3, 'dreamina-cli', $4, 'reserved',
                 1, 1, 1, 'output', 'output', 4, 1, 1)",
    )
    .bind(job)
    .bind(format!("workflow-quote-{job}"))
    .bind(&version.operation)
    .bind(&version.public_model_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "INSERT INTO job_auth_attributions
         (job_id, tenant_id, project_id, auth_kind, admitted_at_ms)
         VALUES ($1, 'org-workflow', 'project-workflow', 'legacy', $2)",
    )
    .bind(job)
    .bind(version.effective_from_ms)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    let mut dimensions = if version.media_kind == "video" {
        json!({"duration":"5", "resolution":"480p", "ratio":"16:9"})
    } else {
        json!({"resolution_type":"2k", "ratio":"1:1"})
    };
    if !workflow.starts_with("dreamina-cli.submit.") {
        dimensions["workflow"] = json!(workflow);
        dimensions["input_image_count"] = json!(if workflow == "frames2video" { "2" } else { "1" });
    }
    // The existing require_customer_quote_surface_contract BEFORE INSERT trigger
    // resolves the published exact surface. Roll back after exercising that guard;
    // settlement and quote-line arithmetic are tested separately.
    sqlx::query(
        "INSERT INTO customer_price_quotes
         (quote_id, job_id, tenant_id, project_id, price_book_id, price_book_version_id,
          api_profile, operation, provider_id, provider_model_id, public_model_id,
          media_kind, service_tier, execution_surface, request_dimensions_json,
          billing_mode, is_free, currency, max_total_micros, quote_hash, created_at_ms)
         VALUES ($1, $2, 'org-workflow', 'project-workflow', $3, $4, $5, $6,
                 'dreamina-cli', $7, $8, $9, 'standard', 'provider_cli', $10,
                 'customer_rate', FALSE, 'USD', 103000, repeat('a',64), $11)",
    )
    .bind(Uuid::new_v4())
    .bind(job)
    .bind(book)
    .bind(version.price_book_version_id)
    .bind(&version.api_profile)
    .bind(&version.operation)
    .bind(&version.provider_model_id)
    .bind(&version.public_model_id)
    .bind(&version.media_kind)
    .bind(dimensions)
    .bind(version.effective_from_ms)
    .execute(&mut *tx)
    .await
    .map_err(|error| format!("workflow {workflow} quote binding must resolve: {error:?}"))?;
    tx.rollback().await.map_err(debug_error)
}
