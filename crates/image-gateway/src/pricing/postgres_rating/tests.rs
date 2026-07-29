use std::env;

use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

use image_provider_contracts::ProviderCostObservationV1;

use crate::admin_read::{AdminReadStore, PostgresAdminReadStore};
use crate::credit_grants::{
    CreateCreditGrantRequest, CreditGrantActor, CreditGrantService, PostgresCreditGrantService,
    reserve_credit_grants,
};
use crate::customer_refunds::{
    CreateCustomerRefundRequest, CustomerRefundActor, CustomerRefundService,
    ListCustomerChargesRequest, PostgresCustomerRefundService,
};
use crate::database::{connect_test_pool_with_search_path, run_migrations};
use crate::pricing::provider_cost::{
    ProviderCostStoreError, apply_executor_provider_reported_cost,
};
use crate::pricing::{
    CreatePriceBookRequest, CreatePriceBookVersionRequest, PostgresPricingAdminService,
    PriceBookVersionDraft, PriceComponentDraft, PricingAdminService, ResolvedPriceVersion,
    TransitionPriceBookVersionRequest,
};

use super::{CustomerRatingStoreError, StoredCustomerRating, settle_customer_quote};

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn settlement_is_atomic_balanced_and_idempotent() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = settlement_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn credit_grant_and_account_funding_settle_and_refund_atomically() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = credit_grant_funding_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn provider_cost_authority_is_receipt_scoped_and_fact_unique() -> TestResult {
    let Some(test_schema) = TestSchema::new().await? else {
        return Ok(());
    };

    let result = provider_cost_authority_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    result.and(cleanup)
}

async fn provider_cost_authority_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    seed_identity(pool).await?;
    seed_codex_pricing_surface(pool).await?;
    let execution = seed_execution_binding(pool).await?;
    let customer_pricing = seed_pricing(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros, held_micros,
            captured_micros, created_at_ms, updated_at_ms
        )
        VALUES ('org-rating', 'USD', 200000, 40000, 0, 1, 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let job =
        seed_customer_job(pool, &customer_pricing, &execution, 1, "provider-authority").await?;
    require(
        provider_cost_obligation_snapshot(pool, job.receipt_id).await?
            == ("pending".to_string(), None, None, false, 1, 1),
        "a new provider receipt did not create exactly one pending cost obligation",
    )?;

    let pricing = PostgresPricingAdminService::new(pool.clone());
    let actual_book = pricing
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "openai.actual.authority.usd".to_string(),
            display_name: "OpenAI actual authority".to_string(),
            purpose: "provider_actual".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("openai-codex".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("actual cost book should be created: {error:?}"))?;
    let mut actual_draft = provider_cost_draft("provider_reported");
    actual_draft.components.clear();
    let actual_version = pricing
        .create_version(
            actual_book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: actual_draft,
            },
        )
        .await
        .map_err(|error| format!("actual cost version should be created: {error:?}"))?;
    let actual_version = pricing
        .publish_version(
            actual_version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("actual cost version should publish: {error:?}"))?;
    let actual_resolved = ResolvedPriceVersion {
        price_book_id: actual_book.price_book_id,
        price_book_key: actual_book.price_book_key.clone(),
        purpose: actual_book.purpose.clone(),
        scope_type: actual_book.scope_type.clone(),
        organization_id: actual_book.organization_id.clone(),
        project_id: actual_book.project_id.clone(),
        provider_id: actual_book.provider_id.clone(),
        currency: actual_book.currency.clone(),
        version: actual_version.clone(),
    };

    let receipt_id: Uuid = sqlx::query_scalar(
        r#"
        SELECT receipt_id
        FROM provider_usage_facts
        WHERE job_id = $1
        LIMIT 1
        "#,
    )
    .bind(job.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;

    let observation = ProviderCostObservationV1::provider_reported_usd_ticks(
        "openai-codex",
        "provider_cli",
        "provider-operation-1",
        200_000_000,
        br#"{"usage":{"cost_in_usd_ticks":200000000}}"#,
        "response.usage.cost_in_usd_ticks",
    )
    .map_err(|error| format!("provider observation should be valid: {error:?}"))?;
    seed_provider_cost_evidence(pool, &job, &observation).await?;
    let mut actual_tx = pool.begin().await.map_err(debug_error)?;
    let stored = apply_executor_provider_reported_cost(
        &mut actual_tx,
        receipt_id,
        &actual_resolved,
        job.manifest_id,
    )
    .await
    .map_err(|error| format!("provider cost should persist: {error:?}"))?;
    actual_tx
        .commit()
        .await
        .map_err(|error| format!("draft allocation must not block actual cost: {error:?}"))?;
    require(
        stored.amount_micros == 20_000
            && stored.native_quantity == "200000000"
            && stored.ledger_transaction_id.is_some(),
        format!("provider cost conversion is incorrect: {stored:?}"),
    )?;
    require(
        provider_cost_obligation_snapshot(pool, receipt_id).await?
            == (
                "settled".to_string(),
                Some("provider_actual".to_string()),
                Some("USD".to_string()),
                true,
                2,
                2,
            ),
        "provider actual authority did not atomically settle its receipt obligation",
    )?;
    let mut replay_tx = pool.begin().await.map_err(debug_error)?;
    let replayed = apply_executor_provider_reported_cost(
        &mut replay_tx,
        receipt_id,
        &actual_resolved,
        job.manifest_id,
    )
    .await
    .map_err(|error| format!("provider cost replay should succeed: {error:?}"))?;
    replay_tx.commit().await.map_err(debug_error)?;
    require(
        replayed == stored,
        format!("provider cost replay drifted: {replayed:?}"),
    )?;
    let stored_source: (String, Option<Uuid>) = sqlx::query_as(
        r#"
        SELECT source_kind, executor_provider_cost_evidence_manifest_id
        FROM provider_cost_observation_sources
        WHERE provider_cost_observation_id = $1
        "#,
    )
    .bind(stored.provider_cost_observation_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        stored_source == ("executor_verified".to_string(), Some(job.manifest_id)),
        format!("provider cost source was not sealed to executor evidence: {stored_source:?}"),
    )?;

    let mut conflicting_tx = pool.begin().await.map_err(debug_error)?;
    let conflict = apply_executor_provider_reported_cost(
        &mut conflicting_tx,
        receipt_id,
        &actual_resolved,
        Uuid::new_v4(),
    )
    .await;
    require(
        conflict == Err(ProviderCostStoreError::Conflict),
        format!("unknown executor evidence should conflict: {conflict:?}"),
    )?;
    conflicting_tx.rollback().await.map_err(debug_error)?;

    let authority_counts_before: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM provider_usage_facts
             WHERE fact_domain = 'provider_actual')::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_observations)::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_observation_receipts)::BIGINT,
            (SELECT COUNT(*) FROM ledger_transactions
             WHERE transaction_type = 'provider_cost')::BIGINT
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let different_operation = ProviderCostObservationV1::provider_reported_usd_ticks(
        "openai-codex",
        "provider_cli",
        "provider-operation-2",
        200_000_000,
        br#"{"usage":{"cost_in_usd_ticks":200000000},"operation":2}"#,
        "response.usage.cost_in_usd_ticks",
    )
    .map_err(|error| format!("different operation should parse: {error:?}"))?;
    let conflicting_source_job = seed_customer_job(
        pool,
        &customer_pricing,
        &execution,
        1,
        "provider-authority-conflicting-source",
    )
    .await?;
    seed_provider_cost_evidence(pool, &conflicting_source_job, &different_operation).await?;
    let mut duplicate_receipt_tx = pool.begin().await.map_err(debug_error)?;
    let duplicate_receipt = apply_executor_provider_reported_cost(
        &mut duplicate_receipt_tx,
        receipt_id,
        &actual_resolved,
        conflicting_source_job.manifest_id,
    )
    .await;
    require(
        duplicate_receipt == Err(ProviderCostStoreError::Conflict),
        format!("one receipt must not claim two provider costs: {duplicate_receipt:?}"),
    )?;
    let authority_counts_after_savepoint: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM provider_usage_facts
             WHERE fact_domain = 'provider_actual')::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_observations)::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_observation_receipts)::BIGINT,
            (SELECT COUNT(*) FROM ledger_transactions
             WHERE transaction_type = 'provider_cost')::BIGINT
        "#,
    )
    .fetch_one(&mut *duplicate_receipt_tx)
    .await
    .map_err(|error| format!("provider-cost savepoint left the transaction unusable: {error:?}"))?;
    require(
        authority_counts_after_savepoint == authority_counts_before,
        format!(
            "provider-cost savepoint left rows behind: \
             before={authority_counts_before:?}, after={authority_counts_after_savepoint:?}"
        ),
    )?;
    duplicate_receipt_tx.rollback().await.map_err(debug_error)?;
    let authority_counts_after: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM provider_usage_facts
             WHERE fact_domain = 'provider_actual')::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_observations)::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_observation_receipts)::BIGINT,
            (SELECT COUNT(*) FROM ledger_transactions
             WHERE transaction_type = 'provider_cost')::BIGINT
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        authority_counts_after == authority_counts_before,
        format!(
            "a rejected receipt claim left provider-cost rows behind: \
             before={authority_counts_before:?}, after={authority_counts_after:?}"
        ),
    )?;

    let second_execution = seed_execution_binding_named(pool, "secondary").await?;
    sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = 80000, updated_at_ms = 4
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let second_job = seed_customer_job(
        pool,
        &customer_pricing,
        &second_execution,
        1,
        "provider-authority-secondary",
    )
    .await?;
    let second_account_observation = ProviderCostObservationV1::provider_reported_usd_ticks(
        "openai-codex",
        "provider_cli",
        "provider-operation-1",
        200_000_000,
        br#"{"usage":{"cost_in_usd_ticks":200000000}}"#,
        "response.usage.cost_in_usd_ticks",
    )
    .map_err(|error| format!("second-account observation should parse: {error:?}"))?;
    seed_provider_cost_evidence(pool, &second_job, &second_account_observation).await?;
    let mut second_account_tx = pool.begin().await.map_err(debug_error)?;
    let second_account_cost = apply_executor_provider_reported_cost(
        &mut second_account_tx,
        second_job.receipt_id,
        &actual_resolved,
        second_job.manifest_id,
    )
    .await
    .map_err(|error| {
        format!("the same operation id on another account should persist: {error:?}")
    })?;
    second_account_tx.commit().await.map_err(debug_error)?;
    require(
        second_account_cost.provider_cost_observation_id != stored.provider_cost_observation_id
            && second_account_cost.usage_fact_id != stored.usage_fact_id
            && second_account_cost.amount_micros == stored.amount_micros,
        format!(
            "provider operation identity was not scoped to its account: \
             first={stored:?}, second={second_account_cost:?}"
        ),
    )?;
    let operation_accounts: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(DISTINCT provider_account_id)::BIGINT
        FROM provider_cost_observations
        WHERE provider_id = 'openai-codex'
          AND execution_surface = 'provider_cli'
          AND provider_operation_id = 'provider-operation-1'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        operation_accounts == 2,
        format!("provider operation should exist once per account: {operation_accounts}"),
    )?;

    let duplicate_observation_id = Uuid::new_v4();
    let mut duplicate_tx = pool.begin().await.map_err(debug_error)?;
    insert_provider_cost_observation(
        &mut duplicate_tx,
        duplicate_observation_id,
        actual_version.price_book_version_id,
        stored.usage_fact_id,
        "provider-operation-2",
        200_000_000,
        20_000,
    )
    .await?;
    let duplicate_error = sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_fact_links (
            provider_cost_observation_id, usage_fact_id,
            provider_id, provider_account_id, execution_surface,
            created_at_ms
        )
        VALUES ($1, $2, 'openai-codex', $3, 'provider_cli', 4)
        "#,
    )
    .bind(duplicate_observation_id)
    .bind(stored.usage_fact_id)
    .bind(execution.provider_account_id)
    .execute(&mut *duplicate_tx)
    .await
    .expect_err("the same provider cost fact must not be claimed twice");
    require(
        database_code(&duplicate_error).as_deref() == Some("23505"),
        format!("unexpected duplicate provider fact error: {duplicate_error:?}"),
    )?;
    duplicate_tx.rollback().await.map_err(debug_error)?;

    let mut invalid_fact_tx = pool.begin().await.map_err(debug_error)?;
    let invalid_fact_id = clone_provider_usage_fact(
        &mut invalid_fact_tx,
        stored.usage_fact_id,
        "provider-cost-invalid-link",
        "provider_actual",
        "image_output",
        1,
        "image",
    )
    .await?;
    let invalid_observation_id = Uuid::new_v4();
    insert_provider_cost_observation(
        &mut invalid_fact_tx,
        invalid_observation_id,
        actual_version.price_book_version_id,
        invalid_fact_id,
        "provider-operation-invalid-link",
        1,
        0,
    )
    .await?;
    let invalid_link_error = sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_fact_links (
            provider_cost_observation_id, usage_fact_id,
            provider_id, provider_account_id, execution_surface,
            created_at_ms
        )
        VALUES ($1, $2, 'openai-codex', $3, 'provider_cli', 4)
        "#,
    )
    .bind(invalid_observation_id)
    .bind(invalid_fact_id)
    .bind(execution.provider_account_id)
    .execute(&mut *invalid_fact_tx)
    .await
    .expect_err("a non-cost fact must not enter provider actual authority");
    require(
        database_code(&invalid_link_error).as_deref() == Some("23514"),
        format!("unexpected invalid provider fact error: {invalid_link_error:?}"),
    )?;
    invalid_fact_tx.rollback().await.map_err(debug_error)?;

    sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = 120000, updated_at_ms = 5
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let wrong_ledger_job = seed_customer_job(
        pool,
        &customer_pricing,
        &execution,
        1,
        "provider-authority-wrong-ledger",
    )
    .await?;
    let wrong_ledger_source_fact_id: Uuid = sqlx::query_scalar(
        r#"
        SELECT usage_fact_id
        FROM provider_usage_facts
        WHERE job_id = $1 AND fact_domain = 'customer_billable'
        LIMIT 1
        "#,
    )
    .bind(wrong_ledger_job.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let mut wrong_ledger_tx = pool.begin().await.map_err(debug_error)?;
    let wrong_ledger_fact_id = clone_provider_usage_fact(
        &mut wrong_ledger_tx,
        wrong_ledger_source_fact_id,
        "provider-cost-wrong-ledger",
        "provider_actual",
        "provider_reported_cost",
        100_000_000,
        "usd_tick",
    )
    .await?;
    let wrong_ledger_observation_id = Uuid::new_v4();
    let wrong_ledger_observation =
        ProviderCostObservationV1::provider_reported_usd_ticks_from_evidence_hash(
            "openai-codex",
            "provider_cli",
            "provider-operation-wrong-ledger",
            100_000_000,
            [0xbb; 32],
            "test://provider-cost",
        )
        .map_err(|error| format!("wrong-ledger observation should parse: {error:?}"))?;
    seed_provider_cost_evidence(pool, &wrong_ledger_job, &wrong_ledger_observation).await?;
    insert_provider_cost_observation(
        &mut wrong_ledger_tx,
        wrong_ledger_observation_id,
        actual_version.price_book_version_id,
        wrong_ledger_fact_id,
        "provider-operation-wrong-ledger",
        100_000_000,
        10_000,
    )
    .await?;
    link_provider_cost_evidence(
        &mut wrong_ledger_tx,
        wrong_ledger_observation_id,
        wrong_ledger_fact_id,
        wrong_ledger_job.receipt_id,
        execution.provider_account_id,
    )
    .await?;
    link_provider_cost_source(
        &mut wrong_ledger_tx,
        wrong_ledger_observation_id,
        wrong_ledger_job.manifest_id,
    )
    .await?;
    insert_wrong_provider_cost_ledger(
        &mut wrong_ledger_tx,
        wrong_ledger_observation_id,
        "openai-codex",
        10_000,
    )
    .await?;
    let wrong_account_error = sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *wrong_ledger_tx)
        .await
        .expect_err("provider cost must use the canonical expense and payable accounts");
    require(
        database_code(&wrong_account_error).as_deref() == Some("23514"),
        format!("unexpected provider ledger account error: {wrong_account_error:?}"),
    )?;
    wrong_ledger_tx.rollback().await.map_err(debug_error)?;

    sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = 200000, updated_at_ms = 6
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let zero_cost_job = seed_customer_job(
        pool,
        &customer_pricing,
        &execution,
        1,
        "provider-authority-zero-cost",
    )
    .await?;
    let submicro_cost_job = seed_customer_job(
        pool,
        &customer_pricing,
        &execution,
        1,
        "provider-authority-submicro-cost",
    )
    .await?;
    let zero_cost_observation = ProviderCostObservationV1::provider_reported_usd_ticks(
        "openai-codex",
        "provider_cli",
        "provider-operation-zero",
        0,
        br#"{"usage":{"cost_in_usd_ticks":0}}"#,
        "response.usage.cost_in_usd_ticks",
    )
    .map_err(|error| format!("zero-cost observation should parse: {error:?}"))?;
    let submicro_cost_observation = ProviderCostObservationV1::provider_reported_usd_ticks(
        "openai-codex",
        "provider_cli",
        "provider-operation-submicro",
        1,
        br#"{"usage":{"cost_in_usd_ticks":1}}"#,
        "response.usage.cost_in_usd_ticks",
    )
    .map_err(|error| format!("sub-micro observation should parse: {error:?}"))?;
    seed_provider_cost_evidence(pool, &zero_cost_job, &zero_cost_observation).await?;
    seed_provider_cost_evidence(pool, &submicro_cost_job, &submicro_cost_observation).await?;
    let mut mismatched_source_tx = pool.begin().await.map_err(debug_error)?;
    let mismatched_source = apply_executor_provider_reported_cost(
        &mut mismatched_source_tx,
        zero_cost_job.receipt_id,
        &actual_resolved,
        submicro_cost_job.manifest_id,
    )
    .await;
    require(
        mismatched_source == Err(ProviderCostStoreError::Conflict),
        format!("a cost observation accepted another execution's evidence: {mismatched_source:?}"),
    )?;
    mismatched_source_tx.rollback().await.map_err(debug_error)?;
    let mut rounding_tx = pool.begin().await.map_err(debug_error)?;
    let zero_cost = apply_executor_provider_reported_cost(
        &mut rounding_tx,
        zero_cost_job.receipt_id,
        &actual_resolved,
        zero_cost_job.manifest_id,
    )
    .await
    .map_err(|error| format!("exact zero provider cost should persist: {error:?}"))?;
    let submicro_cost = apply_executor_provider_reported_cost(
        &mut rounding_tx,
        submicro_cost_job.receipt_id,
        &actual_resolved,
        submicro_cost_job.manifest_id,
    )
    .await
    .map_err(|error| format!("positive sub-micro provider cost should persist: {error:?}"))?;
    rounding_tx.commit().await.map_err(debug_error)?;
    require(
        zero_cost.native_quantity == "0"
            && zero_cost.amount_micros == 0
            && zero_cost.ledger_transaction_id.is_none(),
        format!("exact zero provider cost lost its semantics: {zero_cost:?}"),
    )?;
    require(
        submicro_cost.native_quantity == "1"
            && submicro_cost.amount_micros == 0
            && submicro_cost.ledger_transaction_id.is_none(),
        format!("sub-micro provider cost was confused with exact zero: {submicro_cost:?}"),
    )?;
    for receipt_id in [zero_cost_job.receipt_id, submicro_cost_job.receipt_id] {
        require(
            provider_cost_obligation_snapshot(pool, receipt_id).await?
                == (
                    "settled".to_string(),
                    Some("provider_actual".to_string()),
                    Some("USD".to_string()),
                    true,
                    2,
                    2,
                ),
            "zero or sub-micro provider authority remained falsely pending",
        )?;
    }
    let persisted_rounding: Vec<(String, i64, i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT observation.provider_operation_id,
               observation.native_quantity::BIGINT,
               observation.amount_micros,
               observation.rounding_delta_native_atoms,
               COUNT(receipt_link.receipt_id)::BIGINT
        FROM provider_cost_observations observation
        JOIN provider_cost_observation_fact_links fact_link
          ON fact_link.provider_cost_observation_id =
             observation.provider_cost_observation_id
        JOIN provider_cost_observation_receipts receipt_link
          ON receipt_link.provider_cost_observation_id =
             observation.provider_cost_observation_id
        WHERE observation.provider_operation_id IN (
            'provider-operation-zero',
            'provider-operation-submicro'
        )
        GROUP BY observation.provider_cost_observation_id
        ORDER BY observation.provider_operation_id
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    require(
        persisted_rounding
            == vec![
                ("provider-operation-submicro".to_string(), 1, 0, -1, 1),
                ("provider-operation-zero".to_string(), 0, 0, 0, 1),
            ],
        format!("provider-cost rounding evidence is incomplete: {persisted_rounding:?}"),
    )?;

    let duplicate_authority = sqlx::query(
        r#"
        INSERT INTO provider_cost_authority_claims (
            provider_id, provider_account_id, job_id, currency,
            authority_kind, authority_period,
            source_provider_cost_observation_id, source_usage_fact_id,
            source_receipt_id, created_at_ms
        )
        SELECT provider_id, provider_account_id, job_id, currency,
               authority_kind, authority_period,
               source_provider_cost_observation_id, source_usage_fact_id,
               source_receipt_id, created_at_ms + 1
        FROM provider_cost_authority_claims
        WHERE source_receipt_id = $1
        "#,
    )
    .bind(receipt_id)
    .execute(pool)
    .await
    .expect_err("one receipt and usage fact must have exactly one cost authority");
    require(
        database_code(&duplicate_authority).as_deref() == Some("23505"),
        format!("unexpected duplicate provider authority error: {duplicate_authority:?}"),
    )?;

    let claims: Vec<(String, String, Uuid)> = sqlx::query_as(
        r#"
        SELECT authority_kind, authority_period::TEXT, source_receipt_id
        FROM provider_cost_authority_claims
        WHERE job_id = $1
        ORDER BY authority_kind
        "#,
    )
    .bind(job.job_id)
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    require(
        claims
            == vec![(
                "provider_actual".to_string(),
                "[2,3)".to_string(),
                receipt_id,
            )],
        format!("provider authority projection is incorrect: {claims:?}"),
    )
}

async fn credit_grant_funding_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    seed_identity(pool).await?;
    seed_codex_pricing_surface(pool).await?;
    let execution = seed_execution_binding(pool).await?;
    let pricing = seed_pricing(pool).await?;
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros, held_micros,
            captured_micros, refunded_micros, created_at_ms, updated_at_ms
        )
        VALUES ('org-rating', 'USD', 200000, 0, 0, 0, $1, $1)
        "#,
    )
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let grant_service = PostgresCreditGrantService::new(pool.clone());
    let actor = CreditGrantActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    for (idempotency_key, amount_micros, expires_at_ms, source_reference) in [
        (
            "mixed-funding-grant-early",
            "4000",
            now + 43_200_000,
            "mixed-funding-early",
        ),
        (
            "mixed-funding-grant-late",
            "6000",
            now + 86_400_000,
            "mixed-funding-late",
        ),
    ] {
        grant_service
            .create(
                idempotency_key,
                actor,
                CreateCreditGrantRequest {
                    organization_id: "org-rating".to_string(),
                    currency: "USD".to_string(),
                    amount_micros: amount_micros.to_string(),
                    expires_at_ms,
                    source_reference: source_reference.to_string(),
                    reason: "Mixed funding test credit".to_string(),
                },
            )
            .await
            .map_err(|error| format!("{error:?}"))?;
    }

    let job =
        seed_customer_job_with_credit_grants(pool, &pricing, &execution, 1, "rating-credit-grant")
            .await?;
    let settled = settle(pool, job.job_id).await?;
    require(
        settled.total_amount_micros == 20_000,
        "mixed-funded job should charge 20000 micros",
    )?;
    let funding: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT grant_held_micros, account_held_micros,
               grant_captured_micros, account_captured_micros,
               grant_released_micros, account_released_micros
        FROM customer_billing_holds
        WHERE job_id = $1
        "#,
    )
    .bind(job.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        funding == (10_000, 30_000, 10_000, 10_000, 0, 20_000),
        format!("unexpected mixed funding split: {funding:?}"),
    )?;
    let account: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT held_micros, captured_micros, refunded_micros
        FROM billing_accounts
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        account == (0, 10_000, 0),
        format!("hard-credit account captured the wrong amount: {account:?}"),
    )?;

    let refund_service = PostgresCustomerRefundService::new(pool.clone());
    let refund = refund_service
        .create_refund(
            settled
                .ledger_transaction_id
                .ok_or_else(|| "mixed-funded charge ledger is missing".to_string())?,
            "mixed-funding-refund",
            CustomerRefundActor {
                user_id: actor.user_id,
                session_id: actor.session_id,
            },
            CreateCustomerRefundRequest {
                amount_micros: "15000".to_string(),
                reason_code: "service_failure".to_string(),
                reason: "Mixed funding partial refund".to_string(),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    require(
        refund.account_refunded_micros == "10000" && refund.grant_restored_micros == "5000",
        format!("refund did not reverse hard credit before grants: {refund:?}"),
    )?;
    let replay = refund_service
        .create_refund(
            settled
                .ledger_transaction_id
                .ok_or_else(|| "mixed-funded charge ledger is missing".to_string())?,
            "mixed-funding-refund",
            CustomerRefundActor {
                user_id: actor.user_id,
                session_id: actor.session_id,
            },
            CreateCustomerRefundRequest {
                amount_micros: "15000".to_string(),
                reason_code: "service_failure".to_string(),
                reason: "Mixed funding partial refund".to_string(),
            },
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
    require(
        replay == refund,
        "mixed funding refund replay changed facts",
    )?;
    let grants: Vec<(i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT consumed_micros, restored_micros, available_micros
        FROM credit_grants
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        ORDER BY expires_at_ms
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    require(
        grants == vec![(4_000, 0, 0), (6_000, 5_000, 5_000)],
        format!("FEFO consumption or reverse restoration is invalid: {grants:?}"),
    )?;
    let account: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT held_micros, captured_micros, refunded_micros
        FROM billing_accounts
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        account == (0, 10_000, 10_000),
        format!("hard-credit refund counters are invalid: {account:?}"),
    )
}

async fn insert_provider_cost_observation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    observation_id: Uuid,
    price_book_version_id: Uuid,
    usage_fact_id: Uuid,
    provider_operation_id: &str,
    native_quantity: i64,
    amount_micros: i64,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observations (
            provider_cost_observation_id, observation_key,
            provider_id, provider_account_id, execution_surface,
            provider_operation_id, purpose, price_book_version_id,
            fact_set_hash, currency, native_unit, native_quantity,
            authority, confidence, evidence_hash, evidence_path,
            amount_micros, rounding_mode, rounding_delta_native_atoms,
            created_at_ms
        )
        SELECT
            $1, encode(sha256($1::TEXT::BYTEA), 'hex'),
            'openai-codex', fact.provider_account_id, 'provider_cli',
            $3, 'provider_actual', $2,
            encode(sha256(uuid_send(fact.usage_fact_id)), 'hex'),
            'USD', 'usd_tick', $5::BIGINT,
            'provider_reported', 'exact', repeat('b', 64),
            'test://provider-cost', $6::BIGINT,
            'half_up_after_aggregate',
            $6::BIGINT * 10000 - $5::BIGINT, 3
        FROM provider_usage_facts fact
        WHERE fact.usage_fact_id = $4
        "#,
    )
    .bind(observation_id)
    .bind(price_book_version_id)
    .bind(provider_operation_id)
    .bind(usage_fact_id)
    .bind(native_quantity)
    .bind(amount_micros)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn clone_provider_usage_fact(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source_fact_id: Uuid,
    semantic_suffix: &str,
    fact_domain: &str,
    metric: &str,
    quantity: i64,
    unit: &str,
) -> TestResult<Uuid> {
    let usage_fact_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_usage_facts (
            usage_fact_id, semantic_key, job_id, output_id, submission_id,
            receipt_id, provider_id, provider_account_id, execution_surface,
            fact_domain, metric, quantity, unit, quantity_source, confidence,
            evidence_path, metadata_json, created_at_ms,
            billing_partition_key, terminal_outcome
        )
        SELECT
            $2, fact.semantic_key || ':' || $3,
            fact.job_id, fact.output_id, fact.submission_id,
            fact.receipt_id, fact.provider_id, fact.provider_account_id,
            fact.execution_surface, $4, $5, $6, $7,
            'provider_reported', 'exact',
            'test://provider-cost-adversarial', '{}'::JSONB, 4,
            'provider-cost', fact.terminal_outcome
        FROM provider_usage_facts fact
        WHERE fact.usage_fact_id = $1
        "#,
    )
    .bind(source_fact_id)
    .bind(usage_fact_id)
    .bind(semantic_suffix)
    .bind(fact_domain)
    .bind(metric)
    .bind(quantity)
    .bind(unit)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    Ok(usage_fact_id)
}

async fn link_provider_cost_evidence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    observation_id: Uuid,
    usage_fact_id: Uuid,
    receipt_id: Uuid,
    provider_account_id: Uuid,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_fact_links (
            provider_cost_observation_id, usage_fact_id,
            provider_id, provider_account_id, execution_surface,
            created_at_ms
        )
        VALUES ($1, $2, 'openai-codex', $3, 'provider_cli', 4)
        "#,
    )
    .bind(observation_id)
    .bind(usage_fact_id)
    .bind(provider_account_id)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_receipts (
            provider_cost_observation_id, receipt_id, provider_id, created_at_ms
        )
        VALUES ($1, $2, 'openai-codex', 4)
        "#,
    )
    .bind(observation_id)
    .bind(receipt_id)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn link_provider_cost_source(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    observation_id: Uuid,
    source_manifest_id: Uuid,
) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO provider_cost_observation_sources (
            provider_cost_observation_id, source_kind,
            executor_provider_cost_evidence_manifest_id,
            legacy_reason, created_at_ms
        )
        VALUES ($1, 'executor_verified', $2, NULL, 4)
        "#,
    )
    .bind(observation_id)
    .bind(source_manifest_id)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn insert_wrong_provider_cost_ledger(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    observation_id: Uuid,
    provider_id: &str,
    amount_micros: i64,
) -> TestResult {
    let wrong_expense_id = Uuid::new_v4();
    let wrong_payable_id = Uuid::new_v4();
    for (account_id, account_key, owner_type, owner_id, account_type) in [
        (
            wrong_expense_id,
            format!("platform:USD:wrong-provider-expense:{observation_id}"),
            "platform",
            "platform".to_string(),
            "expense",
        ),
        (
            wrong_payable_id,
            format!("provider:{provider_id}:USD:wrong-payable:{observation_id}"),
            "provider",
            provider_id.to_string(),
            "payable",
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO ledger_accounts (
                account_id, account_key, owner_type, owner_id,
                account_type, currency, created_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, 'USD', 4)
            "#,
        )
        .bind(account_id)
        .bind(account_key)
        .bind(owner_type)
        .bind(owner_id)
        .bind(account_type)
        .execute(&mut **tx)
        .await
        .map_err(debug_error)?;
    }

    let transaction_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions (
            transaction_id, semantic_key,
            source_provider_cost_observation_id,
            transaction_type, currency, payload_hash, created_at_ms
        )
        SELECT
            $1,
            'provider-cost-observation:v1:' || observation.observation_key,
            observation.provider_cost_observation_id,
            'provider_cost', 'USD',
            provider_cost_ledger_payload_hash(
                'provider-cost-observation:v1:' || observation.observation_key,
                'USD', observation.amount_micros, observation.provider_id
            ),
            4
        FROM provider_cost_observations observation
        WHERE observation.provider_cost_observation_id = $2
        "#,
    )
    .bind(transaction_id)
    .bind(observation_id)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    for (posting_no, account_id, amount) in [
        (1_i16, wrong_expense_id, amount_micros),
        (2_i16, wrong_payable_id, -amount_micros),
    ] {
        sqlx::query(
            r#"
            INSERT INTO ledger_postings (
                transaction_id, posting_no, account_id,
                currency, amount_micros, created_at_ms
            )
            VALUES ($1, $2, $3, 'USD', $4, 4)
            "#,
        )
        .bind(transaction_id)
        .bind(posting_no)
        .bind(account_id)
        .bind(amount)
        .execute(&mut **tx)
        .await
        .map_err(debug_error)?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, 4)",
    )
    .bind(transaction_id)
    .execute(&mut **tx)
    .await
    .map_err(debug_error)?;
    Ok(())
}

fn provider_cost_draft(billing_mode: &str) -> PriceBookVersionDraft {
    PriceBookVersionDraft {
        api_profile: "openai-images-v1".to_string(),
        operation: "generation".to_string(),
        provider_id: Some("openai-codex".to_string()),
        provider_model_id: Some("gpt-image-2".to_string()),
        public_model_id: "gpt-image-2".to_string(),
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_cli".to_string(),
        billing_mode: billing_mode.to_string(),
        is_free: false,
        effective_from_ms: 1,
        source_kind: "official_document".to_string(),
        source_url: Some("https://developers.openai.com/api/docs/pricing".to_string()),
        source_checked_at_ms: Some(1),
        notes: None,
        components: vec![PriceComponentDraft {
            component_key: "image.output".to_string(),
            metric: "image_output".to_string(),
            unit: "image".to_string(),
            unit_size: "1".to_string(),
            unit_price_micros: "20000".to_string(),
            outcome: "succeeded".to_string(),
            quantity_source: "provider_reported".to_string(),
            required_confidence: "exact".to_string(),
            rounding_mode: "exact".to_string(),
            dimensions: json!({}),
        }],
    }
}

async fn settlement_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    seed_identity(pool).await?;
    seed_codex_pricing_surface(pool).await?;
    let execution = seed_execution_binding(pool).await?;
    let pricing = seed_pricing(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros, held_micros,
            captured_micros, created_at_ms, updated_at_ms
        )
        VALUES ('org-rating', 'USD', 200000, 40000, 0, 1, 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let succeeded = seed_customer_job(pool, &pricing, &execution, 1, "rating-success").await?;
    let first = settle(pool, succeeded.job_id).await?;
    require(
        first.total_amount_micros == 20_000,
        "one image should charge 20000 micros",
    )?;
    let after_first = economic_snapshot(pool, succeeded.job_id).await?;
    require(
        after_first
            == EconomicSnapshot {
                account_held_micros: 0,
                account_captured_micros: 20_000,
                account_refunded_micros: 0,
                hold_captured_micros: 20_000,
                hold_released_micros: 20_000,
                rating_count: 1,
                rating_line_count: 1,
                fact_link_count: 1,
                ledger_transaction_count: 1,
                ledger_posting_count: 2,
                ledger_posting_sum_micros: 0,
            },
        format!("unexpected first settlement snapshot: {after_first:?}"),
    )?;
    let economics = PostgresAdminReadStore::new(pool.clone())
        .job_economics(succeeded.job_id)
        .await
        .map_err(|error| format!("job economics projection failed: {error:?}"))?;
    let quote = economics
        .customer_quote
        .as_ref()
        .ok_or_else(|| "rated job economics omitted the frozen quote".to_string())?;
    require(
        economics.economics_contract_version == 4
            && economics.economics_state == "rated"
            && quote.max_total_micros == "40000"
            && quote.lines.len() == 1
            && quote.lines[0].actual_quantity.as_deref() == Some("1")
            && quote.lines[0].actual_amount_micros.as_deref() == Some("20000"),
        format!("rated job economics projection is incomplete: {economics:?}"),
    )?;
    require(
        economics.customer_hold.as_ref().is_some_and(|hold| {
            hold.state == "settled"
                && hold.captured_micros == "20000"
                && hold.released_micros == "20000"
        }) && economics
            .customer_rating
            .as_ref()
            .is_some_and(|rating| rating.total_amount_micros == "20000")
            && economics.usage_facts.len() == 1
            && economics.ledger_transactions.len() == 1
            && economics.ledger_transactions[0].transaction_type == "customer_job_charge"
            && economics.ledger_transactions[0].amount_micros == "20000"
            && economics.ledger_transactions[0].sealed_at_ms.is_some(),
        format!("rated settlement evidence is incomplete: {economics:?}"),
    )?;

    let replay = settle(pool, succeeded.job_id).await?;
    require(
        replay == first,
        format!("replay returned a different economic result: {replay:?}"),
    )?;
    require(
        economic_snapshot(pool, succeeded.job_id).await? == after_first,
        "replay changed immutable economic facts",
    )?;
    let late_fact_error = sqlx::query(
        r#"
        INSERT INTO provider_usage_facts (
            usage_fact_id, semantic_key, job_id, output_id, submission_id,
            receipt_id, provider_id, provider_account_id,
            execution_surface, fact_domain, metric, quantity, unit, quantity_source,
            confidence, metadata_json, billing_partition_key,
            terminal_outcome, created_at_ms
        )
        SELECT $2, $3, fact.job_id, fact.output_id, fact.submission_id,
               fact.receipt_id, fact.provider_id, fact.provider_account_id,
               fact.execution_surface, fact.fact_domain, fact.metric, 0, fact.unit,
               fact.quantity_source, fact.confidence, fact.metadata_json,
               fact.billing_partition_key, fact.terminal_outcome, 5
        FROM provider_usage_facts fact
        WHERE fact.job_id = $1
        LIMIT 1
        "#,
    )
    .bind(succeeded.job_id)
    .bind(Uuid::new_v4())
    .bind(format!("late-rating-fact:{}", succeeded.job_id))
    .execute(pool)
    .await
    .expect_err("a customer-rated job must reject late non-cost facts");
    require(
        database_code(&late_fact_error).as_deref() == Some("55000"),
        format!("unexpected late-fact error: {late_fact_error:?}"),
    )?;
    let cross_contract_charge = sqlx::query(
        r#"
        INSERT INTO ledger_transactions (
            transaction_id, semantic_key, source_output_id, source_job_id,
            source_submission_id, source_receipt_id, transaction_type,
            currency, payload_hash, created_at_ms
        )
        SELECT $2, $3, receipt.output_id, receipt.job_id,
               receipt.submission_id, receipt.receipt_id, 'customer_charge',
               'USD', repeat('e', 64), 5
        FROM provider_receipts receipt
        WHERE receipt.job_id = $1
        LIMIT 1
        "#,
    )
    .bind(succeeded.job_id)
    .bind(Uuid::new_v4())
    .bind(format!("invalid-output-charge:{}", succeeded.job_id))
    .execute(pool)
    .await
    .expect_err("v4 jobs must reject legacy output-level customer charges");
    require(
        database_code(&cross_contract_charge).as_deref() == Some("23514"),
        format!("unexpected cross-contract charge error: {cross_contract_charge:?}"),
    )?;

    sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = held_micros + 40000, updated_at_ms = 5
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let exceeds_quote = seed_customer_job(pool, &pricing, &execution, 3, "rating-over-max").await?;
    let before_failure = economic_snapshot(pool, exceeds_quote.job_id).await?;
    let mut tx = pool.begin().await.map_err(debug_error)?;
    let error = settle_customer_quote(&mut tx, exceeds_quote.job_id, "org-rating")
        .await
        .expect_err("usage above the frozen quote must fail closed");
    require(
        error == CustomerRatingStoreError::Conflict,
        format!("unexpected over-maximum error: {error:?}"),
    )?;
    tx.commit()
        .await
        .map_err(|error| format!("outer transaction should remain usable: {error:?}"))?;
    require(
        economic_snapshot(pool, exceeds_quote.job_id).await? == before_failure,
        "failed settlement leaked a partial rating, hold, account, or ledger mutation",
    )?;

    verify_customer_refunds(
        pool,
        first
            .ledger_transaction_id
            .ok_or_else(|| "customer charge ledger transaction is missing".to_string())?,
        succeeded.job_id,
        &pricing,
        &execution,
    )
    .await
}

async fn verify_customer_refunds(
    pool: &PgPool,
    original_transaction_id: Uuid,
    original_job_id: Uuid,
    pricing: &PricingFixture,
    execution: &ExecutionFixture,
) -> TestResult {
    let service = PostgresCustomerRefundService::new(pool.clone());
    let actor = CustomerRefundActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    let first_request = CreateCustomerRefundRequest {
        amount_micros: "7500".to_string(),
        reason_code: "service_failure".to_string(),
        reason: "partial service credit".to_string(),
    };
    let first = service
        .create_refund(
            original_transaction_id,
            "refund-partial-1",
            actor,
            first_request.clone(),
        )
        .await
        .map_err(|error| format!("first refund should succeed: {error:?}"))?;
    require(
        first.amount_micros == "7500"
            && first.refunded_total_micros == "7500"
            && first.remaining_refundable_micros == "12500",
        format!("unexpected first refund: {first:?}"),
    )?;
    let replay = service
        .create_refund(
            original_transaction_id,
            "refund-partial-1",
            actor,
            first_request,
        )
        .await
        .map_err(|error| format!("refund replay should succeed: {error:?}"))?;
    require(
        first == replay,
        "refund replay returned a different resource",
    )?;

    let idempotency_conflict = service
        .create_refund(
            original_transaction_id,
            "refund-partial-1",
            actor,
            CreateCustomerRefundRequest {
                amount_micros: "1".to_string(),
                reason_code: "service_failure".to_string(),
                reason: "different refund body".to_string(),
            },
        )
        .await
        .expect_err("same idempotency key with a different body must conflict");
    require(
        idempotency_conflict.status_code() == axum::http::StatusCode::CONFLICT,
        format!("unexpected idempotency conflict: {idempotency_conflict:?}"),
    )?;

    let second = service
        .create_refund(
            original_transaction_id,
            "refund-partial-2",
            actor,
            CreateCustomerRefundRequest {
                amount_micros: "12500".to_string(),
                reason_code: "billing_correction".to_string(),
                reason: "complete the customer credit".to_string(),
            },
        )
        .await
        .map_err(|error| format!("second refund should succeed: {error:?}"))?;
    require(
        second.refunded_total_micros == "20000" && second.remaining_refundable_micros == "0",
        format!("unexpected second refund: {second:?}"),
    )?;
    let over_refund = service
        .create_refund(
            original_transaction_id,
            "refund-over",
            actor,
            CreateCustomerRefundRequest {
                amount_micros: "1".to_string(),
                reason_code: "other".to_string(),
                reason: "must not exceed the charge".to_string(),
            },
        )
        .await
        .expect_err("refunds must not exceed the original charge");
    require(
        over_refund.status_code() == axum::http::StatusCode::CONFLICT,
        format!("unexpected over-refund error: {over_refund:?}"),
    )?;

    let detail = service
        .get_charge(original_transaction_id)
        .await
        .map_err(|error| format!("charge detail should be readable: {error:?}"))?;
    require(
        detail.charge.amount_micros == "20000"
            && detail.charge.refunded_micros == "20000"
            && detail.charge.refund_state == "fully_refunded"
            && detail.refunds.len() == 2,
        format!("unexpected charge detail: {detail:?}"),
    )?;
    let listed = service
        .list_charges(ListCustomerChargesRequest {
            tenant_id: Some("org-rating".to_string()),
            state: Some("fully_refunded".to_string()),
            limit: Some(100),
            ..Default::default()
        })
        .await
        .map_err(|error| format!("refunded charge list should succeed: {error:?}"))?;
    require(
        listed
            .data
            .iter()
            .any(|charge| charge.transaction_id == original_transaction_id.to_string()),
        "fully refunded charge is absent from the filtered list",
    )?;

    let (captured_micros, refunded_micros): (i64, i64) = sqlx::query_as(
        r#"
        SELECT captured_micros, refunded_micros
        FROM billing_accounts
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        captured_micros == 20_000 && refunded_micros == 20_000,
        "refund must restore net exposure without rewriting gross captured spend",
    )?;
    let (rating_total, refund_transactions, refund_postings, refund_sum): (i64, i64, i64, i64) =
        sqlx::query_as(
            r#"
            SELECT usage.total_amount_micros,
                   COUNT(DISTINCT transaction.transaction_id)::BIGINT,
                   COUNT(posting.posting_no)::BIGINT,
                   COALESCE(SUM(posting.amount_micros), 0)::BIGINT
            FROM customer_rated_usage usage
            LEFT JOIN ledger_transactions transaction
              ON transaction.reverses_transaction_id = $2
             AND transaction.transaction_type = 'customer_refund'
            LEFT JOIN ledger_postings posting
              ON posting.transaction_id = transaction.transaction_id
            WHERE usage.job_id = $1
            GROUP BY usage.total_amount_micros
            "#,
        )
        .bind(original_job_id)
        .bind(original_transaction_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    require(
        rating_total == 20_000
            && refund_transactions == 2
            && refund_postings == 4
            && refund_sum == 0,
        "refund must preserve gross usage and append balanced reversal transactions",
    )?;

    let immutable_error =
        sqlx::query("UPDATE customer_refunds SET reason = 'tampered' WHERE refund_id = $1")
            .bind(Uuid::parse_str(&first.refund_id).map_err(|error| error.to_string())?)
            .execute(pool)
            .await
            .expect_err("customer refund evidence must be immutable");
    require(
        database_code(&immutable_error).as_deref() == Some("P0001"),
        format!("unexpected refund immutability error: {immutable_error:?}"),
    )?;

    let (receivable_account_id, revenue_account_id): (Uuid, Uuid) = sqlx::query_as(
        r#"
        SELECT
            (MIN(posting.account_id::TEXT) FILTER (
                WHERE account.owner_type = 'tenant'
                  AND account.account_type = 'receivable'
            ))::UUID,
            (MIN(posting.account_id::TEXT) FILTER (
                WHERE account.owner_type = 'platform'
                  AND account.account_type = 'revenue'
            ))::UUID
        FROM ledger_postings posting
        JOIN ledger_accounts account
          ON account.account_id = posting.account_id
         AND account.currency = posting.currency
        WHERE posting.transaction_id = $1
        "#,
    )
    .bind(original_transaction_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let orphan_transaction_id = Uuid::new_v4();
    let mut orphan_tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions (
            transaction_id, semantic_key, transaction_type,
            currency, payload_hash, created_at_ms,
            reverses_transaction_id
        )
        VALUES (
            $1, $2, 'customer_refund',
            'USD', repeat('a', 64), 5, $3
        )
        "#,
    )
    .bind(orphan_transaction_id)
    .bind(format!(
        "customer-refund:test-orphan:{orphan_transaction_id}"
    ))
    .bind(original_transaction_id)
    .execute(&mut *orphan_tx)
    .await
    .map_err(debug_error)?;
    for (posting_no, account_id, amount_micros) in [
        (1_i16, receivable_account_id, -1_i64),
        (2_i16, revenue_account_id, 1_i64),
    ] {
        sqlx::query(
            r#"
            INSERT INTO ledger_postings (
                transaction_id, posting_no, account_id,
                currency, amount_micros, created_at_ms
            )
            VALUES ($1, $2, $3, 'USD', $4, 5)
            "#,
        )
        .bind(orphan_transaction_id)
        .bind(posting_no)
        .bind(account_id)
        .bind(amount_micros)
        .execute(&mut *orphan_tx)
        .await
        .map_err(debug_error)?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, 5)",
    )
    .bind(orphan_transaction_id)
    .execute(&mut *orphan_tx)
    .await
    .map_err(debug_error)?;
    let orphan_error = sqlx::query(
        "SET CONSTRAINTS ledger_transactions_require_customer_refund_evidence IMMEDIATE",
    )
    .execute(&mut *orphan_tx)
    .await
    .expect_err("a customer refund ledger transaction must have immutable refund evidence");
    require(
        database_code(&orphan_error).as_deref() == Some("23514"),
        format!("unexpected orphan refund error: {orphan_error:?}"),
    )?;
    orphan_tx.rollback().await.map_err(debug_error)?;

    sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = held_micros + 40000, updated_at_ms = 6
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let concurrent_job =
        seed_customer_job(pool, pricing, execution, 1, "rating-refund-race").await?;
    let concurrent_charge = settle(pool, concurrent_job.job_id)
        .await?
        .ledger_transaction_id
        .ok_or_else(|| "concurrent refund charge is missing".to_string())?;
    let left = service.create_refund(
        concurrent_charge,
        "refund-race-left",
        actor,
        CreateCustomerRefundRequest {
            amount_micros: "15000".to_string(),
            reason_code: "service_failure".to_string(),
            reason: "concurrent refund left".to_string(),
        },
    );
    let right = service.create_refund(
        concurrent_charge,
        "refund-race-right",
        actor,
        CreateCustomerRefundRequest {
            amount_micros: "15000".to_string(),
            reason_code: "service_failure".to_string(),
            reason: "concurrent refund right".to_string(),
        },
    );
    let (left, right) = tokio::join!(left, right);
    require(
        usize::from(left.is_ok()) + usize::from(right.is_ok()) == 1,
        format!("exactly one competing refund must succeed: left={left:?}, right={right:?}"),
    )?;
    let failed = left.err().or_else(|| right.err()).ok_or_else(|| {
        "one concurrent refund should have failed after the source lock".to_string()
    })?;
    require(
        failed.status_code() == axum::http::StatusCode::CONFLICT,
        format!("unexpected concurrent refund error: {failed:?}"),
    )
}

async fn settle(pool: &PgPool, job_id: Uuid) -> TestResult<StoredCustomerRating> {
    let mut tx = pool.begin().await.map_err(debug_error)?;
    let stored = settle_customer_quote(&mut tx, job_id, "org-rating")
        .await
        .map_err(|error| format!("customer quote should settle: {error:?}"))?;
    tx.commit()
        .await
        .map_err(|error| format!("settlement should commit: {error:?}"))?;
    Ok(stored)
}

#[derive(Clone, Copy)]
struct PricingFixture {
    price_book_id: Uuid,
    price_book_version_id: Uuid,
    price_component_id: Uuid,
    effective_from_ms: i64,
}

#[derive(Clone, Copy)]
struct JobFixture {
    job_id: Uuid,
    receipt_id: Uuid,
    manifest_id: Uuid,
    executor_execution_id: Uuid,
    submission_id: Uuid,
}

#[derive(Clone)]
struct ExecutionFixture {
    execution_profile_id: Uuid,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    credential_ref: String,
    resource_policy_id: Uuid,
}

async fn seed_identity(pool: &PgPool) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
            organization_id, display_name, organization_kind,
            created_at_ms, updated_at_ms
        )
        VALUES ('org-rating', 'Rating organization', 'system', 1, 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO gateway_projects (id, tenant_id, name, created_at)
        VALUES ('project-rating', 'org-rating', 'Rating project', 1)
        "#,
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_pricing(pool: &PgPool) -> TestResult<PricingFixture> {
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: "customer.rating.usd".to_string(),
            display_name: "Customer rating".to_string(),
            purpose: "customer_sale".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("openai-codex".to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(|error| format!("customer price book should be created: {error:?}"))?;
    let draft = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
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
                    notes: None,
                    components: ["succeeded", "failed", "no_effect"]
                        .into_iter()
                        .map(|outcome| PriceComponentDraft {
                            component_key: format!("image.output.{outcome}"),
                            metric: "image_output".to_string(),
                            unit: "image".to_string(),
                            unit_size: "1".to_string(),
                            unit_price_micros: if outcome == "succeeded" {
                                "20000".to_string()
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
        .map_err(|error| format!("customer price version should be created: {error:?}"))?;
    let published = service
        .publish_version(
            draft.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(|error| format!("customer price version should publish: {error:?}"))?;
    let price_component_id = published
        .components
        .iter()
        .find(|component| component.outcome == "succeeded")
        .ok_or_else(|| "published customer price version lost its component".to_string())?
        .price_component_id;
    Ok(PricingFixture {
        price_book_id: book.price_book_id,
        price_book_version_id: published.price_book_version_id,
        price_component_id,
        effective_from_ms: published.effective_from_ms,
    })
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
            $1, 1, $2, 'Codex rating test route', 'openai-codex',
            'images.generations', 'openai.images.generation.v1',
            'account', 'quota_aware_least_loaded', 'enabled', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(format!("codex-rating-{}", route_id.simple()))
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

async fn seed_execution_binding(pool: &PgPool) -> TestResult<ExecutionFixture> {
    seed_execution_binding_named(pool, "primary").await
}

async fn seed_execution_binding_named(pool: &PgPool, suffix: &str) -> TestResult<ExecutionFixture> {
    let fixture = ExecutionFixture {
        execution_profile_id: Uuid::new_v4(),
        credential_pool_id: Uuid::new_v4(),
        provider_account_id: Uuid::new_v4(),
        credential_ref: format!("rating-test.openai-codex.{suffix}"),
        resource_policy_id: Uuid::new_v4(),
    };
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools (
            credential_pool_id, pool_key, provider_id, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'openai-codex', 'enabled', 1, 1)
        "#,
    )
    .bind(fixture.credential_pool_id)
    .bind(format!("rating-test-pool-{suffix}"))
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
        VALUES ($1, $2, 'openai-codex', $3, $4, 1,
                repeat('e', 64), 'enabled', 1, 1)
        "#,
    )
    .bind(fixture.provider_account_id)
    .bind(fixture.credential_pool_id)
    .bind(format!("rating-test-account-{suffix}"))
    .bind(&fixture.credential_ref)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_resource_policies (
            resource_policy_id, revision, credential_pool_id,
            provider_account_id, provider_id, execution_class,
            max_concurrency, state, created_at_ms
        )
        VALUES ($1, 1, $2, $3, 'openai-codex', 'inline',
                10, 'enabled', 1)
        "#,
    )
    .bind(fixture.resource_policy_id)
    .bind(fixture.credential_pool_id)
    .bind(fixture.provider_account_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles (
            execution_profile_id, profile_key, provider_id,
            command_schema, operation_id, operation_descriptor_revision,
            operation_descriptor_sha256_v1, completion_mode,
            idempotency_mode, adapter_revision, credential_pool_id,
            provider_account_id, credential_ref, credential_revision,
            resource_policy_id, resource_policy_revision, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'openai-codex',
                'rating-test-v1', 'images.generations',
                'openai-codex/images.generations/v1', repeat('f', 64),
                'inline', 'submission_bound', 'rating-test-adapter-v1',
                $3, $4, $5, 1, $6, 1, 'enabled', 1, 1)
        "#,
    )
    .bind(fixture.execution_profile_id)
    .bind(format!("rating-test-profile-{suffix}"))
    .bind(fixture.credential_pool_id)
    .bind(fixture.provider_account_id)
    .bind(&fixture.credential_ref)
    .bind(fixture.resource_policy_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(fixture)
}

async fn seed_customer_job(
    pool: &PgPool,
    pricing: &PricingFixture,
    execution: &ExecutionFixture,
    actual_quantity: i64,
    request_id: &str,
) -> TestResult<JobFixture> {
    seed_customer_job_inner(pool, pricing, execution, actual_quantity, request_id, false).await
}

async fn seed_customer_job_with_credit_grants(
    pool: &PgPool,
    pricing: &PricingFixture,
    execution: &ExecutionFixture,
    actual_quantity: i64,
    request_id: &str,
) -> TestResult<JobFixture> {
    seed_customer_job_inner(pool, pricing, execution, actual_quantity, request_id, true).await
}

async fn seed_customer_job_inner(
    pool: &PgPool,
    pricing: &PricingFixture,
    execution: &ExecutionFixture,
    actual_quantity: i64,
    request_id: &str,
    use_credit_grants: bool,
) -> TestResult<JobFixture> {
    let job_id = Uuid::new_v4();
    let output_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let created_by_execution_id = Uuid::new_v4();
    let executor_execution_id = Uuid::new_v4();
    let submission_id = Uuid::new_v4();
    let receipt_id = Uuid::new_v4();
    let quote_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO jobs (
            job_id, tenant_id, request_id, operation, provider_id, model,
            state, requested_units, output_count, billable_units,
            billing_metric, billing_unit, economics_contract_version,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'org-rating', $2, 'generation', 'openai-codex',
                'gpt-image-2', 'reserved', 2, 2, 2,
                'output', 'output', 4, 1, 1)
        "#,
    )
    .bind(job_id)
    .bind(request_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions (
            job_id, tenant_id, project_id, auth_kind, admitted_at_ms
        )
        VALUES ($1, 'org-rating', 'project-rating', 'legacy', $2)
        "#,
    )
    .bind(job_id)
    .bind(pricing.effective_from_ms)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_outputs (
            output_id, job_id, output_index, state,
            created_at_ms, updated_at_ms, billable_units
        )
        VALUES ($1, $2, 0, 'pending', 1, 1, 1)
        "#,
    )
    .bind(output_id)
    .bind(job_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO work_items (
            work_item_id, job_id, kind, state, available_at_ms,
            execution_profile_id, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'generation', 'ready', 1, $3, 1, 1)
        "#,
    )
    .bind(work_item_id)
    .bind(job_id)
    .bind(execution.execution_profile_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts (
            attempt_id, execution_id, work_item_id, lease_epoch,
            worker_id, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 1, 'rating-test', 'claimed', 1, 1)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(created_by_execution_id)
    .bind(work_item_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_submissions (
            submission_id, executor_execution_id, output_id, job_id,
            tenant_id, provider_id, model, work_item_id,
            created_by_execution_id, created_by_lease_epoch,
            command_schema, command_hash,
            execution_profile_id, credential_pool_id, provider_account_id,
            credential_ref, credential_revision, adapter_revision,
            resource_policy_id, resource_policy_revision,
            operation_id, operation_descriptor_revision,
            operation_descriptor_sha256_v1, completion_mode,
            idempotency_mode, operation_binding_version, state,
            prepared_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, $4, 'org-rating', 'openai-codex',
                'gpt-image-2', $5, $6, 1, 'rating-test-v1',
                repeat('b', 64), $7, $8, $9, $10, 1,
                'rating-test-adapter-v1', $11, 1,
                'images.generations', 'openai-codex/images.generations/v1',
                repeat('f', 64), 'inline', 'submission_bound', 2,
                'prepared', 1, 1)
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(output_id)
    .bind(job_id)
    .bind(work_item_id)
    .bind(created_by_execution_id)
    .bind(execution.execution_profile_id)
    .bind(execution.credential_pool_id)
    .bind(execution.provider_account_id)
    .bind(&execution.credential_ref)
    .bind(execution.resource_policy_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_executions (
            executor_execution_id, submission_id, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'prepared', 1, 1)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_artifact_authorities (
            authority_id, executor_execution_id, submission_id,
            output_id, job_id, storage_backend, storage_namespace,
            object_key, sha256_hex, byte_size, media_type, created_at_ms
        )
        VALUES (
            $1, $1, $2, $3, $4, 'filesystem-v1', $5, $6,
            repeat('a', 64), 1, 'image/png', 2
        )
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .bind(output_id)
    .bind(job_id)
    .bind(format!("filesystem-v1:rating:{executor_execution_id}"))
    .bind(format!("rating/{executor_execution_id}"))
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO executor_result_manifests (
            manifest_id, executor_execution_id, submission_id,
            created_at_ms, artifact_authority_id
        )
        VALUES ($1, $2, $1, 2, $2)
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_receipts (
            receipt_id, semantic_key, submission_id, output_id, job_id,
            provider_id, outcome, receipt_schema, payload_hash,
            evidence, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, 'openai-codex', 'succeeded',
                'rating-test-v1', repeat('c', 64),
                '{"source":"rating-test"}', 2)
        "#,
    )
    .bind(receipt_id)
    .bind(format!("rating-receipt:{receipt_id}"))
    .bind(submission_id)
    .bind(output_id)
    .bind(job_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
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
        VALUES ($1, $2, 'org-rating', 'project-rating', $3, $4,
                'openai-images-v1', 'generation', 'openai-codex',
                'gpt-image-2', 'gpt-image-2', 'image', 'standard',
                'provider_cli',
                '{"quality":"medium","size":"1024x1024"}'::JSONB,
                'customer_rate', FALSE, 'USD',
                40000, repeat('d', 64), $5)
        "#,
    )
    .bind(quote_id)
    .bind(job_id)
    .bind(pricing.price_book_id)
    .bind(pricing.price_book_version_id)
    .bind(pricing.effective_from_ms)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
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
        VALUES ($1, $2, $3, $4, 'image.output.succeeded', 'output:0',
                'succeeded', 'image_output', 'image', 1, 20000,
                'request_derived', 'exact', 'exact',
                'request_derived', 'exact', '{}'::JSONB,
                2, 40000, 3)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(quote_id)
    .bind(job_id)
    .bind(pricing.price_component_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    let hold_id = Uuid::new_v4();
    let (grant_held_micros, account_held_micros) = if use_credit_grants {
        let funding_now: i64 = sqlx::query_scalar(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(debug_error)?;
        let funding =
            reserve_credit_grants(&mut tx, hold_id, "org-rating", "USD", 40_000, funding_now)
                .await
                .map_err(|error| format!("{error:?}"))?;
        sqlx::query(
            r#"
            UPDATE billing_accounts
            SET held_micros = held_micros + $1,
                updated_at_ms = $2
            WHERE tenant_id = 'org-rating' AND currency = 'USD'
            "#,
        )
        .bind(funding.account_micros)
        .bind(funding_now)
        .execute(&mut *tx)
        .await
        .map_err(debug_error)?;
        (funding.grant_micros, funding.account_micros)
    } else {
        (0, 40_000)
    };
    sqlx::query(
        r#"
        INSERT INTO customer_billing_holds (
            hold_id, quote_id, job_id, tenant_id, currency,
            held_micros, grant_held_micros, account_held_micros,
            state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 'org-rating', 'USD',
                40000, $4, $5, 'held', 3, 3)
        "#,
    )
    .bind(hold_id)
    .bind(quote_id)
    .bind(job_id)
    .bind(grant_held_micros)
    .bind(account_held_micros)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_usage_facts (
            usage_fact_id, semantic_key, job_id, output_id, submission_id,
            receipt_id, provider_id, provider_account_id,
            execution_surface, fact_domain, metric, quantity,
            unit, quantity_source, confidence, metadata_json,
            billing_partition_key, terminal_outcome, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'openai-codex', $7,
                'provider_cli', 'customer_billable', 'image_output', $8, 'image',
                'request_derived', 'exact', $9, 'output:0',
                'succeeded', 3)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(format!("rating-fact:{receipt_id}"))
    .bind(job_id)
    .bind(output_id)
    .bind(submission_id)
    .bind(receipt_id)
    .bind(execution.provider_account_id)
    .bind(actual_quantity)
    .bind(json!({"resolution": "1k"}))
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit()
        .await
        .map_err(|error| format!("rating fixture should commit: {error:?}"))?;
    Ok(JobFixture {
        job_id,
        receipt_id,
        manifest_id: submission_id,
        executor_execution_id,
        submission_id,
    })
}

async fn seed_provider_cost_evidence(
    pool: &PgPool,
    job: &JobFixture,
    observation: &ProviderCostObservationV1,
) -> TestResult {
    let scope = match observation.execution_surface.as_str() {
        "provider_api" => "api_response",
        "provider_cli" => "cli_invocation",
        other => return Err(format!("unsupported test provider cost surface: {other}")),
    };
    sqlx::query(
        r#"
        INSERT INTO executor_provider_cost_evidence (
            manifest_id, executor_execution_id, submission_id,
            scope, provider_id, execution_surface,
            provider_operation_id, currency, native_unit, native_quantity,
            authority, confidence, evidence_hash, evidence_path,
            created_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10::NUMERIC, $11, $12, $13, $14, 2
        )
        "#,
    )
    .bind(job.manifest_id)
    .bind(job.executor_execution_id)
    .bind(job.submission_id)
    .bind(scope)
    .bind(&observation.provider_id)
    .bind(&observation.execution_surface)
    .bind(&observation.provider_operation_id)
    .bind(&observation.currency)
    .bind(observation.native_unit.as_str())
    .bind(observation.native_quantity.to_string())
    .bind(observation.authority.as_str())
    .bind(observation.confidence.as_str())
    .bind(hex::encode(observation.evidence_hash))
    .bind(&observation.evidence_path)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EconomicSnapshot {
    account_held_micros: i64,
    account_captured_micros: i64,
    account_refunded_micros: i64,
    hold_captured_micros: i64,
    hold_released_micros: i64,
    rating_count: i64,
    rating_line_count: i64,
    fact_link_count: i64,
    ledger_transaction_count: i64,
    ledger_posting_count: i64,
    ledger_posting_sum_micros: i64,
}

async fn provider_cost_obligation_snapshot(
    pool: &PgPool,
    receipt_id: Uuid,
) -> TestResult<(String, Option<String>, Option<String>, bool, i64, i64)> {
    sqlx::query_as(
        r#"
        SELECT obligation.state,
               obligation.expected_authority_kind,
               obligation.currency,
               obligation.settlement_claim_id IS NOT NULL,
               obligation.control_version,
               COUNT(event.event_id)::BIGINT
        FROM provider_cost_obligations obligation
        JOIN provider_cost_obligation_events event
          ON event.receipt_id = obligation.receipt_id
        WHERE obligation.receipt_id = $1
        GROUP BY obligation.receipt_id
        "#,
    )
    .bind(receipt_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn economic_snapshot(pool: &PgPool, job_id: Uuid) -> TestResult<EconomicSnapshot> {
    let (account_held_micros, account_captured_micros, account_refunded_micros): (i64, i64, i64) =
        sqlx::query_as(
            r#"
        SELECT held_micros, captured_micros, refunded_micros
        FROM billing_accounts
        WHERE tenant_id = 'org-rating' AND currency = 'USD'
        "#,
        )
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    let (hold_captured_micros, hold_released_micros): (i64, i64) = sqlx::query_as(
        r#"
        SELECT captured_micros, released_micros
        FROM customer_billing_holds
        WHERE job_id = $1
        "#,
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let rating_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM customer_rated_usage WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let rating_line_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM customer_rated_usage_lines line
        JOIN customer_rated_usage rating
          ON rating.rated_usage_id = line.rated_usage_id
        WHERE rating.job_id = $1
        "#,
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let fact_link_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM customer_rated_usage_fact_links link
        JOIN customer_rated_usage_lines line
          ON line.rated_usage_line_id = link.rated_usage_line_id
        JOIN customer_rated_usage rating
          ON rating.rated_usage_id = line.rated_usage_id
        WHERE rating.job_id = $1
        "#,
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let (ledger_transaction_count, ledger_posting_count, ledger_posting_sum_micros): (
        i64,
        i64,
        i64,
    ) = sqlx::query_as(
        r#"
        SELECT COUNT(DISTINCT transaction.transaction_id)::BIGINT,
               COUNT(posting.posting_no)::BIGINT,
               COALESCE(SUM(posting.amount_micros), 0)::BIGINT
        FROM ledger_transactions transaction
        LEFT JOIN ledger_postings posting
          ON posting.transaction_id = transaction.transaction_id
        WHERE transaction.source_job_id = $1
          AND transaction.transaction_type = 'customer_job_charge'
        "#,
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    Ok(EconomicSnapshot {
        account_held_micros,
        account_captured_micros,
        account_refunded_micros,
        hold_captured_micros,
        hold_released_micros,
        rating_count,
        rating_line_count,
        fact_link_count,
        ledger_transaction_count,
        ledger_posting_count,
        ledger_posting_sum_micros,
    })
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn debug_error(error: sqlx::Error) -> String {
    format!("{error:?}")
}

fn database_code(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Database(database) => database.code().map(|code| code.into_owned()),
        _ => None,
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
            eprintln!("skipping PostgreSQL customer rating test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("customer_rating_test_{}", Uuid::new_v4().simple());
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
