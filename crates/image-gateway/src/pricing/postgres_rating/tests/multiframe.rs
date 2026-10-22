use super::*;
use crate::pricing::customer_usage::{
    CustomerUsageAuthority, CustomerUsageOutput, persist_customer_usage_facts,
};
use crate::pricing::postgres_quote::{CustomerQuoteContext, persist_customer_quote};
use crate::pricing::{QuoteQuantity, plan_customer_quote_with_adjustment};

#[tokio::test]
async fn multiframe_milliseconds_persist_and_settle_without_rounding_to_quota_seconds() -> TestResult
{
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = run(&schema.pool).await;
    result.and(schema.cleanup().await)
}

async fn run(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|e| format!("migrations: {e:?}"))?;
    seed_identity(pool).await?;
    let route = Uuid::new_v4();
    let profile = Uuid::new_v4();
    let credential_pool = Uuid::new_v4();
    let account = Uuid::new_v4();
    let policy = Uuid::new_v4();
    // This is a synthetic pricing authority fixture, not an upstream execution test.
    // Only generated UUIDs are interpolated; no request or environment text enters SQL.
    sqlx::raw_sql(AssertSqlSafe(format!(r#"
        INSERT INTO provider_models (provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids, source_kind,
            first_seen_at_ms, last_seen_at_ms, metadata_json)
        VALUES ('dreamina-cli', 'multiframe2video', 'multiframe2video', 'video', 'Multiframe test',
            'supported', 'enabled', ARRAY['videos.generations'], 'adapter_contract', 1, 1, '{{}}');
        INSERT INTO provider_routes (route_id, revision, route_key, display_name, provider_id,
            operation_id, command_schema, route_kind, selection_strategy, state, created_at_ms)
        VALUES ('{route}', 1, 'multiframe-test', 'Multiframe test', 'dreamina-cli',
            'videos.generations', 'dreamina-cli.submit.v1', 'account', 'quota_aware_least_loaded', 'enabled', 1);
        INSERT INTO provider_route_model_mappings (route_id, route_revision, provider_id,
            operation_id, command_schema, api_profile, public_model_id, provider_model_id,
            execution_model_id, media_kind, created_at_ms)
        VALUES ('{route}', 1, 'dreamina-cli', 'videos.generations', 'dreamina-cli.submit.v1',
            'dreamina-cli-videos-v1', 'multiframe2video', 'multiframe2video', 'multiframe2video', 'video', 1);
        INSERT INTO gateway_platform_provider_routes (provider_id, operation_id, command_schema,
            route_id, route_revision, state, created_at_ms, updated_at_ms)
        VALUES ('dreamina-cli', 'videos.generations', 'dreamina-cli.submit.v1', '{route}', 1, 'enabled', 1, 1);
        INSERT INTO provider_credential_pools (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
        VALUES ('{credential_pool}', 'multiframe-test', 'dreamina-cli', 'enabled', 1, 1);
        INSERT INTO provider_accounts (provider_account_id, credential_pool_id, provider_id, account_key,
            credential_ref, credential_revision, credential_auth_sha256, state, created_at_ms, updated_at_ms)
        VALUES ('{account}', '{credential_pool}', 'dreamina-cli', 'multiframe-test', 'test.multiframe', 1, repeat('e',64), 'enabled', 1, 1);
        INSERT INTO executor_resource_policies (resource_policy_id, revision, credential_pool_id,
            provider_account_id, provider_id, execution_class, max_concurrency, state, created_at_ms)
        VALUES ('{policy}', 1, '{credential_pool}', '{account}', 'dreamina-cli', 'inline', 1, 'enabled', 1);
        INSERT INTO provider_execution_profiles (execution_profile_id, profile_key, provider_id,
            command_schema, operation_id, operation_descriptor_revision, operation_descriptor_sha256_v1,
            completion_mode, idempotency_mode, adapter_revision, credential_pool_id, provider_account_id,
            credential_ref, credential_revision, resource_policy_id, resource_policy_revision, state, created_at_ms, updated_at_ms)
        VALUES ('{profile}', 'multiframe-test', 'dreamina-cli', 'rating-test-v1', 'videos.generations',
            'test-pricing-inline-v1', repeat('f',64), 'inline', 'submission_bound', 'rating-test-adapter-v1',
            '{credential_pool}', '{account}', 'test.multiframe', 1, '{policy}', 1, 'enabled', 1, 1);
        INSERT INTO billing_accounts (tenant_id,currency,credit_limit_micros,held_micros,captured_micros,created_at_ms,updated_at_ms)
        VALUES ('org-rating','USD',10000000,0,0,1,1);
    "#))).execute(pool).await.map_err(debug_error)?;
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "test.multiframe.usd".into(),
            display_name: "Synthetic exact-ms test".into(),
            purpose: "customer_sale".into(),
            scope_type: "platform".into(),
            organization_id: None,
            project_id: None,
            provider_id: Some("dreamina-cli".into()),
            currency: "USD".into(),
        })
        .await
        .map_err(|e| format!("book: {e:?}"))?;
    let draft = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
                    api_profile: "dreamina-cli-videos-v1".into(),
                    operation: "video_generation".into(),
                    provider_id: Some("dreamina-cli".into()),
                    provider_model_id: Some("multiframe2video".into()),
                    public_model_id: "multiframe2video".into(),
                    media_kind: "video".into(),
                    service_tier: "standard".into(),
                    execution_surface: "provider_cli".into(),
                    billing_mode: "customer_rate".into(),
                    is_free: false,
                    effective_from_ms: 1,
                    source_kind: "manual".into(),
                    source_url: None,
                    source_checked_at_ms: None,
                    notes: Some("Synthetic test rate; not an official or production price".into()),
                    components: ["succeeded", "failed", "no_effect"]
                        .into_iter()
                        .map(|outcome| PriceComponentDraft {
                            component_key: format!("multiframe.{outcome}"),
                            metric: "video_requested_millisecond".into(),
                            unit: "millisecond".into(),
                            unit_size: "1000".into(),
                            unit_price_micros: if outcome == "succeeded" {
                                "1000000"
                            } else {
                                "0"
                            }
                            .into(),
                            outcome: outcome.into(),
                            quantity_source: "request_derived".into(),
                            required_confidence: "exact".into(),
                            rounding_mode: "exact".into(),
                            dimensions: json!({"workflow":"multiframe2video"}),
                        })
                        .collect(),
                },
            },
        )
        .await
        .map_err(|e| format!("draft: {e:?}"))?;
    let published = service
        .publish_version(
            draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|e| format!("publish: {e:?}"))?;
    let binding: (String, i64) = sqlx::query_as("SELECT contract_key, contract_revision FROM price_book_version_surface_contract_bindings WHERE price_book_version_id=$1")
        .bind(published.price_book_version_id).fetch_one(pool).await.map_err(debug_error)?;
    require(
        binding
            .0
            .starts_with("dreamina-cli.multiframe2video.pricing-surface:")
            && binding.1 == 1,
        "wrong workflow binding",
    )?;
    let resolved = ResolvedPriceVersion {
        price_book_id: book.price_book_id,
        price_book_key: book.price_book_key,
        purpose: book.purpose,
        scope_type: book.scope_type,
        organization_id: None,
        project_id: None,
        provider_id: Some("dreamina-cli".into()),
        currency: "USD".into(),
        version: published,
    };

    let job = Uuid::new_v4();
    let output = Uuid::new_v4();
    let work = Uuid::new_v4();
    let attempt = Uuid::new_v4();
    let execution = Uuid::new_v4();
    let submission = Uuid::new_v4();
    let receipt = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::raw_sql(AssertSqlSafe(format!(r#"
        INSERT INTO jobs (job_id,tenant_id,request_id,operation,provider_id,model,state,
            requested_units,output_count,billable_units,billing_metric,billing_unit,economics_contract_version,created_at_ms,updated_at_ms)
        VALUES ('{job}','org-rating','multiframe-ms','video_generation','dreamina-cli','multiframe2video',
            'reserved',3,1,3,'video_second','second',4,1,1);
        INSERT INTO job_auth_attributions (job_id,tenant_id,project_id,auth_kind,admitted_at_ms)
        VALUES ('{job}','org-rating','project-rating','legacy',{admitted});
        INSERT INTO job_outputs (output_id,job_id,output_index,state,created_at_ms,updated_at_ms,billable_units)
        VALUES ('{output}','{job}',0,'pending',1,1,3);
        INSERT INTO work_items (work_item_id,job_id,kind,state,available_at_ms,execution_profile_id,created_at_ms,updated_at_ms)
        VALUES ('{work}','{job}','video_generation','ready',1,'{profile}',1,1);
        INSERT INTO job_attempts (attempt_id,execution_id,work_item_id,lease_epoch,worker_id,state,created_at_ms,updated_at_ms)
        VALUES ('{attempt}','{attempt}','{work}',1,'test','claimed',1,1);
        INSERT INTO provider_submissions (submission_id,executor_execution_id,output_id,job_id,tenant_id,provider_id,model,
            work_item_id,created_by_execution_id,created_by_lease_epoch,command_schema,command_hash,execution_profile_id,
            credential_pool_id,provider_account_id,credential_ref,credential_revision,adapter_revision,resource_policy_id,
            resource_policy_revision,operation_id,operation_descriptor_revision,operation_descriptor_sha256_v1,
            completion_mode,idempotency_mode,operation_binding_version,state,prepared_at_ms,updated_at_ms)
        VALUES ('{submission}','{execution}','{output}','{job}','org-rating','dreamina-cli','multiframe2video','{work}',
            '{attempt}',1,'rating-test-v1',repeat('b',64),'{profile}','{credential_pool}','{account}','test.multiframe',1,
            'rating-test-adapter-v1','{policy}',1,'videos.generations','test-pricing-inline-v1',repeat('f',64),
            'inline','submission_bound',2,'prepared',1,1);
        INSERT INTO executor_executions (executor_execution_id,submission_id,state,created_at_ms,updated_at_ms)
        VALUES ('{execution}','{submission}','prepared',1,1);
        INSERT INTO executor_artifact_authorities (authority_id,executor_execution_id,submission_id,output_id,job_id,
            storage_backend,storage_namespace,object_key,sha256_hex,byte_size,media_type,created_at_ms)
        VALUES ('{execution}','{execution}','{submission}','{output}','{job}','filesystem-v1','filesystem-v1:test:multiframe','test/video',repeat('a',64),1,'video/mp4',2);
        INSERT INTO executor_result_manifests (manifest_id,executor_execution_id,submission_id,created_at_ms,artifact_authority_id)
        VALUES ('{submission}','{execution}','{submission}',2,'{execution}');
        INSERT INTO provider_receipts (receipt_id,semantic_key,submission_id,output_id,job_id,provider_id,outcome,receipt_schema,payload_hash,evidence,created_at_ms)
        VALUES ('{receipt}','test:{receipt}','{submission}','{output}','{job}','dreamina-cli','succeeded','rating-test-v1',repeat('c',64),'{{"source":"synthetic-pricing-test"}}',2);
    "#, admitted=resolved.version.effective_from_ms))).execute(&mut *tx).await.map_err(debug_error)?;
    let dimensions = json!({"workflow":"multiframe2video","duration_ms":"2500","input_image_count":"2","resolution":"720p"});
    let quantities = vec![QuoteQuantity {
        partition_key: format!("output:{output}"),
        metric: "video_requested_millisecond".into(),
        unit: "millisecond".into(),
        max_quantity: "2500".into(),
        reservation_quantity_source: "request_derived".into(),
        reservation_confidence: "exact".into(),
        settlement_quantity_source: "request_derived".into(),
        settlement_confidence: "exact".into(),
        dimensions: dimensions.clone(),
    }];
    let plan = plan_customer_quote_with_adjustment(&resolved, &quantities, None)
        .map_err(|e| format!("quote: {e:?}"))?;
    require(
        plan.max_total_micros == "2500000",
        "quote rounded duration to quota seconds",
    )?;
    persist_customer_quote(
        &mut tx,
        &CustomerQuoteContext {
            job_id: job,
            tenant_id: "org-rating".into(),
            project_id: "project-rating".into(),
            api_profile: "dreamina-cli-videos-v1".into(),
            operation: "video_generation".into(),
            provider_id: Some("dreamina-cli".into()),
            provider_model_id: Some("multiframe2video".into()),
            public_model_id: "multiframe2video".into(),
            media_kind: "video".into(),
            service_tier: "standard".into(),
            execution_surface: "provider_cli".into(),
            request_dimensions: dimensions,
        },
        &resolved,
        &plan,
    )
    .await
    .map_err(|e| format!("persist quote: {e:?}"))?;
    let usage = CustomerUsageOutput {
        job_id: job,
        output_id: output,
        provider_id: "dreamina-cli",
        provider_account_id: Some(account),
        operation: "video_generation",
        billing_metric: "video_second",
        billing_unit: "second",
        output_billable_units: 3,
        terminal_outcome: "succeeded",
    };
    for _ in 0..2 {
        persist_customer_usage_facts(
            &mut tx,
            &usage,
            CustomerUsageAuthority::Durable {
                submission_id: submission,
                receipt_id: receipt,
            },
            3,
        )
        .await
        .map_err(|e| format!("persist usage: {e:?}"))?;
    }
    tx.commit().await.map_err(debug_error)?;
    let fact: (i64, String, i64) = sqlx::query_as("SELECT quantity, unit, (SELECT COUNT(*) FROM provider_usage_facts WHERE job_id=$1) FROM provider_usage_facts WHERE job_id=$1")
        .bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        fact == (2500, "millisecond".into(), 1),
        "usage fact was rounded or duplicated",
    )?;
    let first = settle(pool, job).await?;
    let replay = settle(pool, job).await?;
    require(
        first.rated_usage_id == replay.rated_usage_id,
        "settlement replay created another rating",
    )?;
    let (captured, held): (i64, i64) = sqlx::query_as("SELECT captured_micros, held_micros FROM billing_accounts WHERE tenant_id='org-rating' AND currency='USD'")
        .fetch_one(pool).await.map_err(debug_error)?;
    require(
        captured == 2_500_000 && held == 0,
        "real settlement charged quota ceiling or charged twice",
    )?;
    Ok(())
}
