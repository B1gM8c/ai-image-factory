use std::env;

use gpt_image_2_gateway::{
    database::{connect_test_pool_with_search_path, run_migrations},
    pricing::{
        CreatePriceBookRequest, CreatePriceBookVersionRequest, PostgresPricingAdminService,
        PriceBookVersionDraft, PriceComponentDraft, PricingAdminService, ResolvedPriceVersion,
        TransitionPriceBookVersionRequest,
    },
    provider_cost_allocations::{
        CloseProviderCostAllocationRequest, CreateProviderCostAllocationDraftRequest,
        ListProviderCostAllocationsRequest, PostgresProviderCostAllocationService,
        PreviewProviderCostAllocationRequest, ProviderCostAllocationActor,
        ProviderCostAllocationService,
    },
};
use image_provider_contracts::ProviderCostObservationV1;
use serde_json::json;
use sqlx::{AssertSqlSafe, FromRow, PgPool};
use uuid::Uuid;

pub use gpt_image_2_gateway::pricing::{
    UsageFact, aggregate_provider_reported_cost, usd_ticks_to_ledger_micros,
};

#[path = "../src/pricing/provider_cost.rs"]
mod provider_cost_store;

use provider_cost_store::{ProviderCostStoreError, apply_executor_provider_reported_cost};

type TestResult<T = ()> = Result<T, String>;

const PROVIDER_ID: &str = "openai-codex";
const TENANT_A: &str = "allocation-org-a";
const PROJECT_A: &str = "allocation-project-a";
const TENANT_B: &str = "allocation-org-b";
const PROJECT_B: &str = "allocation-project-b";
const PERIOD_START_MS: i64 = 10;
const PERIOD_END_MS: i64 = 1_000;
const ACTOR_USER_ID: Uuid = Uuid::from_u128(0x901);
const ACTOR_SESSION_ID: Uuid = Uuid::from_u128(0x902);

#[tokio::test]
async fn draft_is_conserved_idempotent_queryable_and_has_no_settlement_side_effects() -> TestResult
{
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = draft_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn draft_rejects_candidate_drift() -> TestResult {
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = candidate_drift_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn preview_isolated_by_authority_and_every_price_surface_dimension() -> TestResult {
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = eligibility_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn output_close_is_atomic_idempotent_and_receipt_exact() -> TestResult {
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = close_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn close_rejects_job_basis_and_candidate_drift_without_side_effects() -> TestResult {
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = close_rejection_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn concurrent_close_replays_one_committed_result() -> TestResult {
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = concurrent_close_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn close_and_provider_actual_race_has_one_receipt_authority() -> TestResult {
    let Some(schema) = TestSchema::new().await? else {
        return Ok(());
    };
    let result = close_actual_race_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn draft_case(pool: &PgPool) -> TestResult {
    let fixture = seed_base(pool).await?;
    let jobs = [
        seed_job(
            pool,
            &fixture.customer_pricing,
            &fixture.primary_execution,
            JobSeed::matching(
                Uuid::from_u128(0x101),
                Uuid::from_u128(0x201),
                "allocation-remainder-1",
                100,
            ),
        )
        .await?,
        seed_job(
            pool,
            &fixture.customer_pricing,
            &fixture.primary_execution,
            JobSeed::matching(
                Uuid::from_u128(0x102),
                Uuid::from_u128(0x202),
                "allocation-remainder-2",
                200,
            ),
        )
        .await?,
        seed_job(
            pool,
            &fixture.customer_pricing,
            &fixture.primary_execution,
            JobSeed::matching(
                Uuid::from_u128(0x103),
                Uuid::from_u128(0x203),
                "allocation-remainder-3",
                300,
            ),
        )
        .await?,
    ];
    let service = PostgresProviderCostAllocationService::new(pool.clone());
    let request = preview_request(
        fixture.primary_execution.provider_account_id,
        fixture.allocated_version_id,
        10,
        "successful_job",
    );
    let preview = service
        .preview(request.clone())
        .await
        .map_err(debug_error)?;
    require(
        preview.candidate_count == 3
            && preview.allocated_amount_micros == "10"
            && preview.residual_amount_micros == "0"
            && preview
                .lines
                .iter()
                .map(|line| line.job_id)
                .eq(jobs.iter().map(|job| job.job_id))
            && preview
                .lines
                .iter()
                .map(|line| line.amount_micros.as_str())
                .eq(["4", "3", "3"])
            && preview
                .lines
                .iter()
                .all(|line| line.output_id.is_none() && line.basis_unit == "job"),
        format!("largest-remainder preview was not deterministic: {preview:?}"),
    )?;
    let output_preview = service
        .preview(preview_request(
            fixture.primary_execution.provider_account_id,
            fixture.allocated_version_id,
            10,
            "successful_output",
        ))
        .await
        .map_err(debug_error)?;
    require(
        output_preview.candidate_count == 3
            && output_preview
                .lines
                .iter()
                .map(|line| line.output_id)
                .eq(jobs.iter().map(|job| Some(job.output_id))),
        "successful_output did not deduplicate and order by output UUID",
    )?;

    let before = side_effect_snapshot(pool).await?;
    require(
        before
            == SideEffectSnapshot {
                authority_claims: 0,
                provider_cost_ledgers: 0,
                settled_obligations: 0,
                pending_obligations: 3,
            },
        format!("fixture started with unexpected economic authority: {before:?}"),
    )?;
    let create = create_request(&request, &preview.preview_hash, "draft-idempotency-1");
    let created = service
        .create_draft(create.clone())
        .await
        .map_err(debug_error)?;
    require(
        created.pool.state == "draft"
            && created.pool.candidate_count == 3
            && created.pool.allocated_amount_micros == "10"
            && created.pool.residual_amount_micros == "0"
            && created.preview_hash == preview.preview_hash
            && created.lines.len() == 3,
        format!("draft did not persist its complete line set: {created:?}"),
    )?;
    let replay = service
        .create_draft(create.clone())
        .await
        .map_err(debug_error)?;
    require(
        replay == created,
        "same idempotency key and body did not return the original draft",
    )?;
    let mut drifted_body = create;
    drifted_body.total_amount_micros = "11".to_string();
    let conflict = service
        .create_draft(drifted_body)
        .await
        .expect_err("idempotency key reuse with a different body must conflict");
    require(
        conflict.status_code() == 409,
        format!("idempotency body drift returned the wrong status: {conflict:?}"),
    )?;

    let loaded = service
        .get(created.pool.provider_cost_allocation_pool_id)
        .await
        .map_err(debug_error)?;
    let listed = service
        .list(ListProviderCostAllocationsRequest {
            provider_id: Some(PROVIDER_ID.to_string()),
            provider_account_id: Some(fixture.primary_execution.provider_account_id),
            currency: Some("usd".to_string()),
            state: Some("draft".to_string()),
            after: None,
            limit: Some(1),
        })
        .await
        .map_err(debug_error)?;
    require(
        loaded == created
            && listed.data.len() == 1
            && listed.data[0].provider_cost_allocation_pool_id
                == created.pool.provider_cost_allocation_pool_id
            && !listed.has_more,
        "draft list/get did not replay persisted state",
    )?;
    let after = side_effect_snapshot(pool).await?;
    require(
        after == before,
        format!(
            "draft creation created authority, ledger, or obligation settlement: \
             before={before:?}, after={after:?}"
        ),
    )?;
    let invalid = service
        .preview(PreviewProviderCostAllocationRequest {
            allocation_basis: "provider_usage_fact".to_string(),
            ..request
        })
        .await
        .expect_err("unsupported allocation basis must be rejected");
    require(
        invalid.status_code() == 400,
        "unsupported allocation basis did not map to 400",
    )
}

async fn close_case(pool: &PgPool) -> TestResult {
    let fixture = seed_base(pool).await?;
    seed_actor(pool).await?;
    let jobs = [
        seed_job(
            pool,
            &fixture.customer_pricing,
            &fixture.primary_execution,
            JobSeed::matching(
                Uuid::from_u128(0x111),
                Uuid::from_u128(0x211),
                "allocation-close-1",
                100,
            ),
        )
        .await?,
        seed_job(
            pool,
            &fixture.customer_pricing,
            &fixture.primary_execution,
            JobSeed::matching(
                Uuid::from_u128(0x112),
                Uuid::from_u128(0x212),
                "allocation-close-2",
                200,
            ),
        )
        .await?,
        seed_job(
            pool,
            &fixture.customer_pricing,
            &fixture.primary_execution,
            JobSeed::matching(
                Uuid::from_u128(0x113),
                Uuid::from_u128(0x213),
                "allocation-close-3",
                300,
            ),
        )
        .await?,
    ];
    let service = PostgresProviderCostAllocationService::new(pool.clone());
    let request = preview_request(
        fixture.primary_execution.provider_account_id,
        fixture.allocated_version_id,
        1,
        "successful_output",
    );
    let preview = service
        .preview(request.clone())
        .await
        .map_err(debug_error)?;
    let draft = service
        .create_draft(create_request(
            &request,
            &preview.preview_hash,
            "close-draft",
        ))
        .await
        .map_err(debug_error)?;
    let direct_claim = sqlx::query(
        r#"
        INSERT INTO provider_cost_authority_claims (
            provider_id, provider_account_id, job_id, currency,
            authority_kind, authority_period, source_receipt_id,
            source_provider_cost_allocation_pool_id,
            source_provider_cost_allocation_line_id, created_at_ms
        )
        SELECT line.provider_id, line.provider_account_id, line.job_id,
               pool.currency, 'provider_allocated',
               int8range(pool.period_start_ms, pool.period_end_ms, '[)'),
               line.basis_receipt_id,
               line.provider_cost_allocation_pool_id,
               line.provider_cost_allocation_line_id, 99
        FROM provider_cost_allocation_lines line
        JOIN provider_cost_allocation_pools pool
          ON pool.provider_cost_allocation_pool_id =
             line.provider_cost_allocation_pool_id
        WHERE line.provider_cost_allocation_pool_id = $1
        ORDER BY line.provider_cost_allocation_line_id
        LIMIT 1
        "#,
    )
    .bind(draft.pool.provider_cost_allocation_pool_id)
    .execute(pool)
    .await
    .expect_err("a draft line must not create provider cost authority directly");
    require(
        database_code(&direct_claim).as_deref() == Some("23514"),
        format!("draft authority bypass failed unexpectedly: {direct_claim:?}"),
    )?;
    let close = close_request(&draft.preview_hash);
    let closed = service
        .close(
            draft.pool.provider_cost_allocation_pool_id,
            "close-idempotency",
            test_actor(),
            close.clone(),
        )
        .await
        .map_err(debug_error)?;
    require(
        closed.pool.state == "closed"
            && closed.pool.control_version == 2
            && closed.closure.as_ref().is_some_and(|closure| {
                closure.source_kind == "provider_subscription"
                    && closure.source_reference == "subscription:test"
                    && closure.closed_by_user_id == ACTOR_USER_ID
            }),
        format!("close did not persist immutable evidence: {closed:?}"),
    )?;

    let coverage: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM provider_cost_authority_claims
             WHERE source_provider_cost_allocation_pool_id = $1)::BIGINT,
            (SELECT COUNT(*) FROM ledger_transactions
             WHERE source_provider_cost_allocation_pool_id = $1)::BIGINT,
            (SELECT COUNT(*) FROM ledger_postings posting
             JOIN ledger_transactions ledger_tx
               ON ledger_tx.transaction_id = posting.transaction_id
             WHERE ledger_tx.source_provider_cost_allocation_pool_id = $1)::BIGINT,
            (SELECT COUNT(*) FROM ledger_transaction_seals seal
             JOIN ledger_transactions ledger_tx
               ON ledger_tx.transaction_id = seal.transaction_id
             WHERE ledger_tx.source_provider_cost_allocation_pool_id = $1)::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_obligations obligation
             JOIN provider_cost_authority_claims claim
               ON claim.claim_id = obligation.settlement_claim_id
             WHERE claim.source_provider_cost_allocation_pool_id = $1
               AND obligation.state = 'settled')::BIGINT,
            (SELECT COALESCE(SUM(posting.amount_micros), 0)
             FROM ledger_postings posting
             JOIN ledger_transactions ledger_tx
               ON ledger_tx.transaction_id = posting.transaction_id
             WHERE ledger_tx.source_provider_cost_allocation_pool_id = $1)::BIGINT
        "#,
    )
    .bind(draft.pool.provider_cost_allocation_pool_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        coverage == (3, 1, 2, 1, 3, 0),
        format!("zero-value lines did not retain exact claim coverage: {coverage:?}"),
    )?;
    let receipt_claims: Vec<Uuid> = sqlx::query_scalar(
        r#"
        SELECT source_receipt_id
        FROM provider_cost_authority_claims
        WHERE source_provider_cost_allocation_pool_id = $1
        ORDER BY source_receipt_id
        "#,
    )
    .bind(draft.pool.provider_cost_allocation_pool_id)
    .fetch_all(pool)
    .await
    .map_err(debug_error)?;
    let mut expected_receipts = jobs.iter().map(|job| job.receipt_id).collect::<Vec<_>>();
    expected_receipts.sort();
    require(
        receipt_claims == expected_receipts,
        "allocation claims were not bound to the exact receipt set",
    )?;

    let replay = service
        .close(
            draft.pool.provider_cost_allocation_pool_id,
            "close-idempotency",
            test_actor(),
            close,
        )
        .await
        .map_err(debug_error)?;
    require(
        replay == closed,
        "close replay did not return the original result",
    )?;
    let conflict = service
        .close(
            draft.pool.provider_cost_allocation_pool_id,
            "different-close-key",
            test_actor(),
            CloseProviderCostAllocationRequest {
                source_reference: "subscription:other".to_string(),
                ..close_request(&draft.preview_hash)
            },
        )
        .await
        .expect_err("a second close command must not replace immutable evidence");
    require(
        conflict.status_code() == 409,
        "second close command returned the wrong status",
    )?;

    let duplicate = sqlx::query(
        r#"
        INSERT INTO provider_cost_authority_claims (
            provider_id, provider_account_id, job_id, currency,
            authority_kind, authority_period, source_receipt_id,
            source_provider_cost_allocation_pool_id,
            source_provider_cost_allocation_line_id, created_at_ms
        )
        SELECT provider_id, provider_account_id, job_id, 'EUR',
               'provider_allocated', int8range($2, $3, '[)'),
               basis_receipt_id, provider_cost_allocation_pool_id,
               provider_cost_allocation_line_id, 999
        FROM provider_cost_allocation_lines
        WHERE provider_cost_allocation_pool_id = $1
        ORDER BY provider_cost_allocation_line_id
        LIMIT 1
        "#,
    )
    .bind(draft.pool.provider_cost_allocation_pool_id)
    .bind(PERIOD_START_MS)
    .bind(PERIOD_END_MS)
    .execute(pool)
    .await
    .expect_err("one receipt must not be claimable again in another currency");
    require(
        database_code(&duplicate).as_deref() == Some("23505")
            || database_code(&duplicate).as_deref() == Some("23514"),
        format!("duplicate receipt authority failed unexpectedly: {duplicate:?}"),
    )
}

async fn close_rejection_case(pool: &PgPool) -> TestResult {
    let fixture = seed_base(pool).await?;
    seed_actor(pool).await?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x121),
            Uuid::from_u128(0x221),
            "allocation-close-reject-1",
            100,
        ),
    )
    .await?;
    let service = PostgresProviderCostAllocationService::new(pool.clone());

    let job_request = preview_request(
        fixture.primary_execution.provider_account_id,
        fixture.allocated_version_id,
        10,
        "successful_job",
    );
    let job_preview = service
        .preview(job_request.clone())
        .await
        .map_err(debug_error)?;
    let job_draft = service
        .create_draft(create_request(
            &job_request,
            &job_preview.preview_hash,
            "close-job-draft",
        ))
        .await
        .map_err(debug_error)?;
    let unsupported = service
        .close(
            job_draft.pool.provider_cost_allocation_pool_id,
            "close-job",
            test_actor(),
            close_request(&job_draft.preview_hash),
        )
        .await
        .expect_err("successful_job close must fail until receipt coverage is exact");
    require(
        unsupported.status_code() == 409,
        "successful_job close returned the wrong status",
    )?;

    let drift_version_id = create_allocated_version(
        pool,
        "close-drift",
        TENANT_A,
        PriceSurface::matching(),
        false,
    )
    .await?;
    let output_request = PreviewProviderCostAllocationRequest {
        allocation_basis: "successful_output".to_string(),
        price_book_version_id: drift_version_id,
        ..job_request
    };
    let output_preview = service
        .preview(output_request.clone())
        .await
        .map_err(debug_error)?;
    let output_draft = service
        .create_draft(create_request(
            &output_request,
            &output_preview.preview_hash,
            "close-drift-draft",
        ))
        .await
        .map_err(debug_error)?;
    let before = side_effect_snapshot(pool).await?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x122),
            Uuid::from_u128(0x222),
            "allocation-close-reject-2",
            200,
        ),
    )
    .await?;
    let drift = service
        .close(
            output_draft.pool.provider_cost_allocation_pool_id,
            "close-drift",
            test_actor(),
            close_request(&output_draft.preview_hash),
        )
        .await
        .expect_err("new receipt after draft must invalidate close");
    require(
        drift.status_code() == 409,
        "candidate drift returned the wrong status",
    )?;
    let state: (String, i64, i64) = sqlx::query_as(
        r#"
        SELECT state, control_version,
               (SELECT COUNT(*) FROM provider_cost_allocation_closures
                WHERE provider_cost_allocation_pool_id = $1)::BIGINT
        FROM provider_cost_allocation_pools
        WHERE provider_cost_allocation_pool_id = $1
        "#,
    )
    .bind(output_draft.pool.provider_cost_allocation_pool_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let after = side_effect_snapshot(pool).await?;
    require(
        state == ("draft".to_string(), 1, 0)
            && after.authority_claims == before.authority_claims
            && after.provider_cost_ledgers == before.provider_cost_ledgers
            && after.settled_obligations == before.settled_obligations,
        format!(
            "rejected close leaked side effects: state={state:?}, before={before:?}, after={after:?}"
        ),
    )
}

async fn concurrent_close_case(pool: &PgPool) -> TestResult {
    let fixture = seed_base(pool).await?;
    seed_actor(pool).await?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x131),
            Uuid::from_u128(0x231),
            "allocation-concurrent-close",
            100,
        ),
    )
    .await?;
    let service = PostgresProviderCostAllocationService::new(pool.clone());
    let request = preview_request(
        fixture.primary_execution.provider_account_id,
        fixture.allocated_version_id,
        10,
        "successful_output",
    );
    let preview = service
        .preview(request.clone())
        .await
        .map_err(debug_error)?;
    let draft = service
        .create_draft(create_request(
            &request,
            &preview.preview_hash,
            "concurrent-close-draft",
        ))
        .await
        .map_err(debug_error)?;
    let first = service.clone();
    let second = service.clone();
    let first_request = close_request(&draft.preview_hash);
    let second_request = first_request.clone();
    let pool_id = draft.pool.provider_cost_allocation_pool_id;
    let (left, right) = tokio::join!(
        first.close(pool_id, "concurrent-close", test_actor(), first_request),
        second.close(pool_id, "concurrent-close", test_actor(), second_request),
    );
    let left = left.map_err(debug_error)?;
    let right = right.map_err(debug_error)?;
    require(left == right, "concurrent close did not replay one result")?;
    let counts: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM provider_cost_allocation_closures
             WHERE provider_cost_allocation_pool_id = $1)::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_authority_claims
             WHERE source_provider_cost_allocation_pool_id = $1)::BIGINT,
            (SELECT COUNT(*) FROM ledger_transactions
             WHERE source_provider_cost_allocation_pool_id = $1)::BIGINT
        "#,
    )
    .bind(pool_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        counts == (1, 1, 1),
        format!("concurrent close duplicated side effects: {counts:?}"),
    )
}

async fn close_actual_race_case(pool: &PgPool) -> TestResult {
    let fixture = seed_base(pool).await?;
    seed_actor(pool).await?;
    let job = seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x141),
            Uuid::from_u128(0x241),
            "allocation-close-actual-race",
            100,
        ),
    )
    .await?;
    let actual_price = prepare_actual_cost(pool, &job).await?;
    let service = PostgresProviderCostAllocationService::new(pool.clone());
    let request = preview_request(
        fixture.primary_execution.provider_account_id,
        fixture.allocated_version_id,
        10,
        "successful_output",
    );
    let preview = service
        .preview(request.clone())
        .await
        .map_err(debug_error)?;
    let draft = service
        .create_draft(create_request(
            &request,
            &preview.preview_hash,
            "close-actual-race-draft",
        ))
        .await
        .map_err(debug_error)?;
    let pool_id = draft.pool.provider_cost_allocation_pool_id;
    let actual_pool = pool.clone();
    let actual_job = job;
    let (close_result, actual_result) = tokio::join!(
        service.close(
            pool_id,
            "close-actual-race",
            test_actor(),
            close_request(&draft.preview_hash),
        ),
        apply_prepared_actual_cost(&actual_pool, &actual_job, &actual_price),
    );
    let close_won = close_result.is_ok();
    let actual_won = actual_result.is_ok();
    require(
        close_won ^ actual_won,
        format!(
            "receipt race must have exactly one winner: \
             close={close_result:?}, actual={actual_result:?}"
        ),
    )?;
    if let Err(error) = &close_result {
        require(
            error.status_code() == 409,
            format!("losing close returned the wrong status: {error:?}"),
        )?;
    }
    if let Err(error) = &actual_result {
        require(
            *error == ProviderCostStoreError::Conflict,
            format!("losing provider actual returned the wrong error: {error:?}"),
        )?;
    }

    let outcome: (String, i64, i64, i64, i64, String) = sqlx::query_as(
        r#"
        SELECT
            pool.state,
            (SELECT COUNT(*) FROM provider_cost_allocation_closures closure
             WHERE closure.provider_cost_allocation_pool_id = pool.provider_cost_allocation_pool_id)::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_authority_claims claim
             WHERE claim.source_receipt_id = $2)::BIGINT,
            (SELECT COUNT(*) FROM ledger_transactions ledger_tx
             WHERE ledger_tx.transaction_type = 'provider_cost'
               AND ledger_tx.source_job_id = $3)::BIGINT,
            (SELECT COUNT(*) FROM provider_cost_obligations obligation
             WHERE obligation.receipt_id = $2
               AND obligation.state = 'settled'
               AND obligation.settlement_claim_id IS NOT NULL)::BIGINT,
            (SELECT authority_kind FROM provider_cost_authority_claims
             WHERE source_receipt_id = $2)
        FROM provider_cost_allocation_pools pool
        WHERE pool.provider_cost_allocation_pool_id = $1
        "#,
    )
    .bind(pool_id)
    .bind(job.receipt_id)
    .bind(job.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let expected_state = if close_won { "closed" } else { "draft" };
    let expected_closures = i64::from(close_won);
    let expected_authority = if close_won {
        "provider_allocated"
    } else {
        "provider_actual"
    };
    require(
        outcome
            == (
                expected_state.to_string(),
                expected_closures,
                1,
                1,
                1,
                expected_authority.to_string(),
            ),
        format!("receipt race leaked or duplicated economic state: {outcome:?}"),
    )
}

async fn candidate_drift_case(pool: &PgPool) -> TestResult {
    let fixture = seed_base(pool).await?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x301),
            Uuid::from_u128(0x401),
            "allocation-drift-1",
            100,
        ),
    )
    .await?;
    let service = PostgresProviderCostAllocationService::new(pool.clone());
    let request = preview_request(
        fixture.primary_execution.provider_account_id,
        fixture.allocated_version_id,
        9,
        "successful_job",
    );
    let preview = service
        .preview(request.clone())
        .await
        .map_err(debug_error)?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x302),
            Uuid::from_u128(0x402),
            "allocation-drift-2",
            200,
        ),
    )
    .await?;
    let conflict = service
        .create_draft(create_request(
            &request,
            &preview.preview_hash,
            "candidate-drift",
        ))
        .await
        .expect_err("draft creation must reject candidate-set drift");
    require(
        conflict.status_code() == 409,
        format!("candidate drift returned the wrong status: {conflict:?}"),
    )?;
    let pool_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_cost_allocation_pools")
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    require(
        pool_count == 0,
        "candidate drift left a partial draft behind",
    )
}

async fn eligibility_case(pool: &PgPool) -> TestResult {
    let fixture = seed_base(pool).await?;
    let secondary_execution = seed_execution_binding(pool, "secondary").await?;
    let matching = seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x501),
            Uuid::from_u128(0x601),
            "allocation-eligible",
            100,
        ),
    )
    .await?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &secondary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x502),
            Uuid::from_u128(0x602),
            "allocation-other-account",
            110,
        ),
    )
    .await?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed {
            tenant_id: TENANT_B,
            project_id: PROJECT_B,
            job_id: Uuid::from_u128(0x503),
            output_id: Uuid::from_u128(0x603),
            request_id: "allocation-other-tenant",
            receipt_created_at_ms: 120,
        },
    )
    .await?;
    seed_job(
        pool,
        &fixture.customer_pricing,
        &fixture.primary_execution,
        JobSeed::matching(
            Uuid::from_u128(0x504),
            Uuid::from_u128(0x604),
            "allocation-outside-period",
            PERIOD_END_MS + 10,
        ),
    )
    .await?;

    let service = PostgresProviderCostAllocationService::new(pool.clone());
    let valid_request = preview_request(
        fixture.primary_execution.provider_account_id,
        fixture.allocated_version_id,
        1_000,
        "successful_job",
    );
    let valid = service
        .preview(valid_request.clone())
        .await
        .map_err(debug_error)?;
    require(
        valid.candidate_count == 1 && valid.lines[0].job_id == matching.job_id,
        format!("account/tenant/period filters admitted an unrelated receipt: {valid:?}"),
    )?;

    let wrong_versions = [
        (
            "provider_model_id",
            create_allocated_version(
                pool,
                "wrong-provider-model",
                TENANT_A,
                PriceSurface {
                    provider_model_id: "gpt-image-other",
                    ..PriceSurface::matching()
                },
                false,
            )
            .await?,
        ),
        (
            "public_model_id",
            create_allocated_version(
                pool,
                "wrong-public-model",
                TENANT_A,
                PriceSurface {
                    public_model_id: "public-image-other",
                    ..PriceSurface::matching()
                },
                false,
            )
            .await?,
        ),
        (
            "media_kind",
            create_allocated_version(
                pool,
                "wrong-media",
                TENANT_A,
                PriceSurface {
                    media_kind: "video",
                    ..PriceSurface::matching()
                },
                false,
            )
            .await?,
        ),
        (
            "operation",
            create_allocated_version(
                pool,
                "wrong-operation",
                TENANT_A,
                PriceSurface {
                    operation: "edit",
                    ..PriceSurface::matching()
                },
                false,
            )
            .await?,
        ),
        (
            "service_tier",
            create_allocated_version(
                pool,
                "wrong-tier",
                TENANT_A,
                PriceSurface {
                    service_tier: "priority",
                    ..PriceSurface::matching()
                },
                false,
            )
            .await?,
        ),
        (
            "execution_surface",
            create_allocated_version(
                pool,
                "wrong-surface",
                TENANT_A,
                PriceSurface {
                    execution_surface: "provider_api",
                    ..PriceSurface::matching()
                },
                false,
            )
            .await?,
        ),
    ];
    for (dimension, version_id) in wrong_versions {
        let excluded = service
            .preview(PreviewProviderCostAllocationRequest {
                price_book_version_id: version_id,
                ..valid_request.clone()
            })
            .await
            .map_err(debug_error)?;
        require(
            excluded.candidate_count == 0,
            format!("{dimension} mismatch admitted a receipt: {excluded:?}"),
        )?;
    }

    let wrong_currency = service
        .preview(PreviewProviderCostAllocationRequest {
            currency: "EUR".to_string(),
            ..valid_request.clone()
        })
        .await
        .expect_err("currency mismatch must fail closed");
    require(
        wrong_currency.status_code() == 400,
        "currency mismatch did not return 400",
    )?;
    let wrong_provider = service
        .preview(PreviewProviderCostAllocationRequest {
            provider_id: "other-provider".to_string(),
            ..valid_request.clone()
        })
        .await
        .expect_err("provider mismatch must fail closed");
    require(
        wrong_provider.status_code() == 400,
        "provider mismatch did not return 400",
    )?;
    let foreign_account = seed_foreign_provider_account(pool).await?;
    let wrong_account_provider = service
        .preview(PreviewProviderCostAllocationRequest {
            provider_account_id: foreign_account,
            ..valid_request.clone()
        })
        .await
        .expect_err("an account owned by another provider must fail closed");
    require(
        wrong_account_provider.status_code() == 400,
        "cross-provider account mismatch did not return 400",
    )?;

    seed_actual_cost(pool, &matching).await?;
    let after_actual = service.preview(valid_request).await.map_err(debug_error)?;
    let authority_kind: String = sqlx::query_scalar(
        "SELECT authority_kind FROM provider_cost_authority_claims WHERE job_id = $1",
    )
    .bind(matching.job_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        after_actual.candidate_count == 0 && authority_kind == "provider_actual",
        format!(
            "existing provider actual authority did not exclude its job: \
             authority={authority_kind}, preview={after_actual:?}"
        ),
    )
}

#[derive(Clone)]
struct BaseFixture {
    customer_pricing: CustomerPricing,
    primary_execution: ExecutionFixture,
    allocated_version_id: Uuid,
}

#[derive(Clone, Copy)]
struct CustomerPricing {
    price_book_id: Uuid,
    price_book_version_id: Uuid,
    price_component_id: Uuid,
    effective_from_ms: i64,
}

#[derive(Clone)]
struct ExecutionFixture {
    execution_profile_id: Uuid,
    credential_pool_id: Uuid,
    provider_account_id: Uuid,
    credential_ref: String,
    resource_policy_id: Uuid,
}

#[derive(Clone, Copy)]
struct JobFixture {
    job_id: Uuid,
    output_id: Uuid,
    receipt_id: Uuid,
    manifest_id: Uuid,
    executor_execution_id: Uuid,
    submission_id: Uuid,
    provider_account_id: Uuid,
}

#[derive(Clone, Copy)]
struct JobSeed {
    tenant_id: &'static str,
    project_id: &'static str,
    job_id: Uuid,
    output_id: Uuid,
    request_id: &'static str,
    receipt_created_at_ms: i64,
}

impl JobSeed {
    fn matching(
        job_id: Uuid,
        output_id: Uuid,
        request_id: &'static str,
        receipt_created_at_ms: i64,
    ) -> Self {
        Self {
            tenant_id: TENANT_A,
            project_id: PROJECT_A,
            job_id,
            output_id,
            request_id,
            receipt_created_at_ms,
        }
    }
}

#[derive(Clone, Copy)]
struct PriceSurface {
    api_profile: &'static str,
    operation: &'static str,
    provider_model_id: &'static str,
    public_model_id: &'static str,
    media_kind: &'static str,
    service_tier: &'static str,
    execution_surface: &'static str,
}

impl PriceSurface {
    fn matching() -> Self {
        Self {
            api_profile: "openai-images-v1",
            operation: "generation",
            provider_model_id: "gpt-image-2",
            public_model_id: "gpt-image-2",
            media_kind: "image",
            service_tier: "standard",
            execution_surface: "provider_cli",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, FromRow, PartialEq)]
struct SideEffectSnapshot {
    authority_claims: i64,
    provider_cost_ledgers: i64,
    settled_obligations: i64,
    pending_obligations: i64,
}

async fn seed_base(pool: &PgPool) -> TestResult<BaseFixture> {
    run_migrations(pool)
        .await
        .map_err(|error| format!("migrations should succeed: {error:?}"))?;
    seed_identity(pool).await?;
    seed_pricing_surface(pool).await?;
    let customer_pricing = seed_customer_pricing(pool).await?;
    let primary_execution = seed_execution_binding(pool, "primary").await?;
    let allocated_version_id =
        create_allocated_version(pool, "main", TENANT_A, PriceSurface::matching(), true).await?;
    Ok(BaseFixture {
        customer_pricing,
        primary_execution,
        allocated_version_id,
    })
}

fn preview_request(
    provider_account_id: Uuid,
    price_book_version_id: Uuid,
    total_amount_micros: i64,
    allocation_basis: &str,
) -> PreviewProviderCostAllocationRequest {
    PreviewProviderCostAllocationRequest {
        provider_id: PROVIDER_ID.to_string(),
        provider_account_id,
        price_book_version_id,
        period_start_ms: PERIOD_START_MS,
        period_end_ms: PERIOD_END_MS,
        currency: "USD".to_string(),
        total_amount_micros: total_amount_micros.to_string(),
        allocation_basis: allocation_basis.to_string(),
    }
}

fn create_request(
    preview: &PreviewProviderCostAllocationRequest,
    expected_preview_hash: &str,
    idempotency_key: &str,
) -> CreateProviderCostAllocationDraftRequest {
    CreateProviderCostAllocationDraftRequest {
        provider_id: preview.provider_id.clone(),
        provider_account_id: preview.provider_account_id,
        price_book_version_id: preview.price_book_version_id,
        period_start_ms: preview.period_start_ms,
        period_end_ms: preview.period_end_ms,
        currency: preview.currency.clone(),
        total_amount_micros: preview.total_amount_micros.clone(),
        allocation_basis: preview.allocation_basis.clone(),
        expected_preview_hash: expected_preview_hash.to_string(),
        idempotency_key: idempotency_key.to_string(),
    }
}

fn close_request(snapshot_hash: &str) -> CloseProviderCostAllocationRequest {
    CloseProviderCostAllocationRequest {
        expected_control_version: 1,
        expected_snapshot_hash: snapshot_hash.to_string(),
        source_kind: "provider_subscription".to_string(),
        source_reference: "subscription:test".to_string(),
        source_evidence_hash: "a".repeat(64),
    }
}

fn test_actor() -> ProviderCostAllocationActor {
    ProviderCostAllocationActor {
        user_id: ACTOR_USER_ID,
        session_id: ACTOR_SESSION_ID,
    }
}

async fn side_effect_snapshot(pool: &PgPool) -> TestResult<SideEffectSnapshot> {
    sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM provider_cost_authority_claims)::BIGINT
                AS authority_claims,
            (SELECT COUNT(*) FROM ledger_transactions
             WHERE transaction_type = 'provider_cost')::BIGINT
                AS provider_cost_ledgers,
            (SELECT COUNT(*) FROM provider_cost_obligations
             WHERE settlement_claim_id IS NOT NULL)::BIGINT
                AS settled_obligations,
            (SELECT COUNT(*) FROM provider_cost_obligations
             WHERE state = 'pending'
               AND settlement_claim_id IS NULL)::BIGINT
                AS pending_obligations
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)
}

async fn seed_identity(pool: &PgPool) -> TestResult {
    for (tenant, project) in [(TENANT_A, PROJECT_A), (TENANT_B, PROJECT_B)] {
        sqlx::query(
            r#"
            INSERT INTO identity_organizations (
                organization_id, display_name, organization_kind,
                created_at_ms, updated_at_ms
            )
            VALUES ($1, $2, 'system', 1, 1)
            "#,
        )
        .bind(tenant)
        .bind(format!("{tenant} display"))
        .execute(pool)
        .await
        .map_err(debug_error)?;
        sqlx::query(
            r#"
            INSERT INTO gateway_projects (id, tenant_id, name, created_at)
            VALUES ($1, $2, $3, 1)
            "#,
        )
        .bind(project)
        .bind(tenant)
        .bind(format!("{project} display"))
        .execute(pool)
        .await
        .map_err(debug_error)?;
    }
    Ok(())
}

async fn seed_actor(pool: &PgPool) -> TestResult {
    sqlx::query(
        r#"
        INSERT INTO identity_users (
            user_id, normalized_email, display_name, roles, scopes,
            authz_version, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, 'allocation-admin@example.test', 'Allocation Admin',
            ARRAY['admin'], ARRAY['billing:write'], 1, 1, 1
        )
        "#,
    )
    .bind(ACTOR_USER_ID)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_pricing_surface(pool: &PgPool) -> TestResult {
    let route_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id, model_id, execution_model_id, media_kind,
            display_name, adapter_state, lifecycle_state, operation_ids,
            source_kind, first_seen_at_ms, last_seen_at_ms, metadata_json
        )
        VALUES (
            $1, 'gpt-image-2', 'gpt-image-2', 'image',
            'GPT Image 2', 'supported', 'enabled',
            ARRAY['images.generations'], 'adapter_contract',
            1, 1, '{}'::JSONB
        )
        "#,
    )
    .bind(PROVIDER_ID)
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
            $1, 1, $2, 'Allocation test route', $3,
            'images.generations', 'openai.images.generation.v1',
            'account', 'quota_aware_least_loaded', 'enabled', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(format!("allocation-route-{}", route_id.simple()))
    .bind(PROVIDER_ID)
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
            $1, 1, $2, 'images.generations',
            'openai.images.generation.v1', 'openai-images-v1',
            'gpt-image-2', 'gpt-image-2', 'gpt-image-2', 'image', 1
        )
        "#,
    )
    .bind(route_id)
    .bind(PROVIDER_ID)
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
            $1, 'images.generations', 'openai.images.generation.v1',
            $2, 1, 'enabled', 1, 1
        )
        "#,
    )
    .bind(PROVIDER_ID)
    .bind(route_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(())
}

async fn seed_customer_pricing(pool: &PgPool) -> TestResult<CustomerPricing> {
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: format!("allocation.customer.{}", Uuid::new_v4().simple()),
            display_name: "Allocation customer price".to_string(),
            purpose: "customer_sale".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some(PROVIDER_ID.to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(debug_error)?;
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: PriceBookVersionDraft {
                    api_profile: "openai-images-v1".to_string(),
                    operation: "generation".to_string(),
                    provider_id: Some(PROVIDER_ID.to_string()),
                    provider_model_id: Some("gpt-image-2".to_string()),
                    public_model_id: "gpt-image-2".to_string(),
                    media_kind: "image".to_string(),
                    service_tier: "standard".to_string(),
                    execution_surface: "provider_cli".to_string(),
                    billing_mode: "customer_rate".to_string(),
                    is_free: false,
                    effective_from_ms: 1,
                    source_kind: "official_document".to_string(),
                    source_url: Some("https://openai.com/api/pricing/".to_string()),
                    source_checked_at_ms: Some(1),
                    notes: None,
                    components: ["succeeded", "failed", "no_effect"]
                        .into_iter()
                        .map(customer_price_component)
                        .collect(),
                },
            },
        )
        .await
        .map_err(debug_error)?;
    let published = service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(debug_error)?;
    let price_component_id = published
        .components
        .iter()
        .find(|component| component.outcome == "succeeded")
        .ok_or_else(|| "published customer price lost its succeeded component".to_string())?
        .price_component_id;
    Ok(CustomerPricing {
        price_book_id: book.price_book_id,
        price_book_version_id: published.price_book_version_id,
        price_component_id,
        effective_from_ms: published.effective_from_ms,
    })
}

async fn create_allocated_version(
    pool: &PgPool,
    suffix: &str,
    organization_id: &str,
    surface: PriceSurface,
    publish_through_service: bool,
) -> TestResult<Uuid> {
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: format!("allocation.provider.{suffix}.{}", Uuid::new_v4().simple()),
            display_name: format!("Provider allocation {suffix}"),
            purpose: "provider_allocated".to_string(),
            scope_type: "organization".to_string(),
            organization_id: Some(organization_id.to_string()),
            project_id: None,
            provider_id: Some(PROVIDER_ID.to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(debug_error)?;
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: provider_price_draft(surface, "subscription_allocation", true),
            },
        )
        .await
        .map_err(debug_error)?;
    if publish_through_service {
        return service
            .publish_version(
                version.price_book_version_id,
                TransitionPriceBookVersionRequest {
                    expected_control_version: 1,
                },
            )
            .await
            .map(|published| published.price_book_version_id)
            .map_err(debug_error);
    }

    sqlx::query(
        r#"
        UPDATE price_book_versions
        SET state = 'active', control_version = control_version + 1,
            updated_at_ms = updated_at_ms + 1
        WHERE price_book_version_id = $1
          AND state = 'draft'
        "#,
    )
    .bind(version.price_book_version_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(version.price_book_version_id)
}

fn provider_price_draft(
    surface: PriceSurface,
    billing_mode: &str,
    with_component: bool,
) -> PriceBookVersionDraft {
    PriceBookVersionDraft {
        api_profile: surface.api_profile.to_string(),
        operation: surface.operation.to_string(),
        provider_id: Some(PROVIDER_ID.to_string()),
        provider_model_id: Some(surface.provider_model_id.to_string()),
        public_model_id: surface.public_model_id.to_string(),
        media_kind: surface.media_kind.to_string(),
        service_tier: surface.service_tier.to_string(),
        execution_surface: surface.execution_surface.to_string(),
        billing_mode: billing_mode.to_string(),
        is_free: false,
        effective_from_ms: 1,
        source_kind: "provider_contract".to_string(),
        source_url: Some("https://openai.com/api/pricing/".to_string()),
        source_checked_at_ms: Some(1),
        notes: None,
        components: with_component
            .then(|| vec![price_component("provider_reported")])
            .unwrap_or_default(),
    }
}

fn price_component(quantity_source: &str) -> PriceComponentDraft {
    PriceComponentDraft {
        component_key: "image.output.succeeded".to_string(),
        metric: "image_output".to_string(),
        unit: "image".to_string(),
        unit_size: "1".to_string(),
        unit_price_micros: "20000".to_string(),
        outcome: "succeeded".to_string(),
        quantity_source: quantity_source.to_string(),
        required_confidence: "exact".to_string(),
        rounding_mode: "exact".to_string(),
        dimensions: json!({}),
    }
}

fn customer_price_component(outcome: &str) -> PriceComponentDraft {
    PriceComponentDraft {
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
    }
}

async fn seed_execution_binding(pool: &PgPool, suffix: &str) -> TestResult<ExecutionFixture> {
    let fixture = ExecutionFixture {
        execution_profile_id: Uuid::new_v4(),
        credential_pool_id: Uuid::new_v4(),
        provider_account_id: Uuid::new_v4(),
        credential_ref: format!("allocation.{suffix}.credential"),
        resource_policy_id: Uuid::new_v4(),
    };
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools (
            credential_pool_id, pool_key, provider_id, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 'enabled', 1, 1)
        "#,
    )
    .bind(fixture.credential_pool_id)
    .bind(format!("allocation-pool-{suffix}"))
    .bind(PROVIDER_ID)
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
        VALUES ($1, $2, $3, $4, $5, 1,
                repeat('e', 64), 'enabled', 1, 1)
        "#,
    )
    .bind(fixture.provider_account_id)
    .bind(fixture.credential_pool_id)
    .bind(PROVIDER_ID)
    .bind(format!("allocation-account-{suffix}"))
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
        VALUES ($1, 1, $2, $3, $4, 'inline', 10, 'enabled', 1)
        "#,
    )
    .bind(fixture.resource_policy_id)
    .bind(fixture.credential_pool_id)
    .bind(fixture.provider_account_id)
    .bind(PROVIDER_ID)
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
        VALUES ($1, $2, $3, 'allocation-test-v1', 'images.generations',
                'openai-codex/images.generations/v1', repeat('f', 64),
                'inline', 'submission_bound', 'allocation-test-adapter-v1',
                $4, $5, $6, 1, $7, 1, 'enabled', 1, 1)
        "#,
    )
    .bind(fixture.execution_profile_id)
    .bind(format!("allocation-profile-{suffix}"))
    .bind(PROVIDER_ID)
    .bind(fixture.credential_pool_id)
    .bind(fixture.provider_account_id)
    .bind(&fixture.credential_ref)
    .bind(fixture.resource_policy_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(fixture)
}

async fn seed_foreign_provider_account(pool: &PgPool) -> TestResult<Uuid> {
    let credential_pool_id = Uuid::new_v4();
    let provider_account_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools (
            credential_pool_id, pool_key, provider_id, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'other-provider', 'enabled', 1, 1)
        "#,
    )
    .bind(credential_pool_id)
    .bind(format!(
        "allocation-foreign-pool-{}",
        Uuid::new_v4().simple()
    ))
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
        VALUES ($1, $2, 'other-provider', $3, $4, 1,
                repeat('e', 64), 'enabled', 1, 1)
        "#,
    )
    .bind(provider_account_id)
    .bind(credential_pool_id)
    .bind(format!(
        "allocation-foreign-account-{}",
        provider_account_id.simple()
    ))
    .bind(format!("allocation.foreign.{provider_account_id}"))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(provider_account_id)
}

async fn seed_job(
    pool: &PgPool,
    pricing: &CustomerPricing,
    execution: &ExecutionFixture,
    seed: JobSeed,
) -> TestResult<JobFixture> {
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
        VALUES ($1, $2, $3, 'generation', $4,
                'gpt-image-2', 'reserved', 1, 1, 1,
                'output', 'output', 4, 1, 1)
        "#,
    )
    .bind(seed.job_id)
    .bind(seed.tenant_id)
    .bind(seed.request_id)
    .bind(PROVIDER_ID)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_auth_attributions (
            job_id, tenant_id, project_id, auth_kind, admitted_at_ms
        )
        VALUES ($1, $2, $3, 'legacy', $4)
        "#,
    )
    .bind(seed.job_id)
    .bind(seed.tenant_id)
    .bind(seed.project_id)
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
    .bind(seed.output_id)
    .bind(seed.job_id)
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
    .bind(seed.job_id)
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
        VALUES ($1, $2, $3, 1, 'allocation-test', 'claimed', 1, 1)
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
        VALUES ($1, $2, $3, $4, $5, $6,
                'gpt-image-2', $7, $8, 1, 'allocation-test-v1',
                repeat('b', 64), $9, $10, $11, $12, 1,
                'allocation-test-adapter-v1', $13, 1,
                'images.generations', 'openai-codex/images.generations/v1',
                repeat('f', 64), 'inline', 'submission_bound', 2,
                'prepared', 1, 1)
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(seed.output_id)
    .bind(seed.job_id)
    .bind(seed.tenant_id)
    .bind(PROVIDER_ID)
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
    .bind(seed.output_id)
    .bind(seed.job_id)
    .bind(format!("filesystem-v1:allocation:{executor_execution_id}"))
    .bind(format!("allocation/{executor_execution_id}"))
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
        VALUES ($1, $2, $3, $4, $5, $6, 'succeeded',
                'allocation-test-v1', repeat('c', 64),
                '{"source":"allocation-test"}', $7)
        "#,
    )
    .bind(receipt_id)
    .bind(format!("allocation-receipt:{receipt_id}"))
    .bind(submission_id)
    .bind(seed.output_id)
    .bind(seed.job_id)
    .bind(PROVIDER_ID)
    .bind(seed.receipt_created_at_ms)
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
        VALUES ($1, $2, $3, $4, $5, $6,
                'openai-images-v1', 'generation', $7,
                'gpt-image-2', 'gpt-image-2', 'image', 'standard',
                'provider_cli', '{}'::JSONB,
                'customer_rate', FALSE, 'USD',
                20000, repeat('d', 64), $8)
        "#,
    )
    .bind(quote_id)
    .bind(seed.job_id)
    .bind(seed.tenant_id)
    .bind(seed.project_id)
    .bind(pricing.price_book_id)
    .bind(pricing.price_book_version_id)
    .bind(PROVIDER_ID)
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
                1, 20000, 3)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(quote_id)
    .bind(seed.job_id)
    .bind(pricing.price_component_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit()
        .await
        .map_err(|error| format!("job fixture should commit: {error:?}"))?;
    Ok(JobFixture {
        job_id: seed.job_id,
        output_id: seed.output_id,
        receipt_id,
        manifest_id: submission_id,
        executor_execution_id,
        submission_id,
        provider_account_id: execution.provider_account_id,
    })
}

async fn seed_actual_cost(pool: &PgPool, job: &JobFixture) -> TestResult {
    let resolved = prepare_actual_cost(pool, job).await?;
    apply_prepared_actual_cost(pool, job, &resolved)
        .await
        .map_err(debug_error)
}

async fn prepare_actual_cost(pool: &PgPool, job: &JobFixture) -> TestResult<ResolvedPriceVersion> {
    let service = PostgresPricingAdminService::new(pool.clone());
    let book = service
        .create_price_book(CreatePriceBookRequest {
            price_book_key: format!("allocation.actual.{}", Uuid::new_v4().simple()),
            display_name: "Allocation actual cost".to_string(),
            purpose: "provider_actual".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some(PROVIDER_ID.to_string()),
            currency: "USD".to_string(),
        })
        .await
        .map_err(debug_error)?;
    let version = service
        .create_version(
            book.price_book_id,
            CreatePriceBookVersionRequest {
                draft: provider_price_draft(PriceSurface::matching(), "provider_reported", false),
            },
        )
        .await
        .map_err(debug_error)?;
    let version = service
        .publish_version(
            version.price_book_version_id,
            TransitionPriceBookVersionRequest {
                expected_control_version: 1,
            },
        )
        .await
        .map_err(debug_error)?;
    let resolved = ResolvedPriceVersion {
        price_book_id: book.price_book_id,
        price_book_key: book.price_book_key,
        purpose: book.purpose,
        scope_type: book.scope_type,
        organization_id: book.organization_id,
        project_id: book.project_id,
        provider_id: book.provider_id,
        currency: book.currency,
        version,
    };
    let observation = ProviderCostObservationV1::provider_reported_usd_ticks(
        PROVIDER_ID,
        "provider_cli",
        "allocation-provider-operation",
        200_000_000,
        br#"{"usage":{"cost_in_usd_ticks":200000000}}"#,
        "response.usage.cost_in_usd_ticks",
    )
    .map_err(debug_error)?;
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
            $1, $2, $3, 'cli_invocation', $4, 'provider_cli',
            $5, 'USD', 'usd_tick', $6::NUMERIC,
            'provider_reported', 'exact', $7, $8, 2
        )
        "#,
    )
    .bind(job.manifest_id)
    .bind(job.executor_execution_id)
    .bind(job.submission_id)
    .bind(PROVIDER_ID)
    .bind(&observation.provider_operation_id)
    .bind(observation.native_quantity.to_string())
    .bind(hex::encode(observation.evidence_hash))
    .bind(&observation.evidence_path)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(resolved)
}

async fn apply_prepared_actual_cost(
    pool: &PgPool,
    job: &JobFixture,
    resolved: &ResolvedPriceVersion,
) -> Result<(), ProviderCostStoreError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| ProviderCostStoreError::Unavailable)?;
    let stored =
        apply_executor_provider_reported_cost(&mut tx, job.receipt_id, resolved, job.manifest_id)
            .await?;
    tx.commit()
        .await
        .map_err(map_provider_cost_database_error)?;
    if stored.amount_micros == 20_000
        && stored.ledger_transaction_id.is_some()
        && job.provider_account_id != Uuid::nil()
    {
        Ok(())
    } else {
        Err(ProviderCostStoreError::Conflict)
    }
}

fn map_provider_cost_database_error(error: sqlx::Error) -> ProviderCostStoreError {
    match database_code(&error).as_deref() {
        Some("23505" | "23514" | "23P01" | "40001") => ProviderCostStoreError::Conflict,
        _ => ProviderCostStoreError::Unavailable,
    }
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn database_code(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .map(|code| code.into_owned())
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
            eprintln!("skipping PostgreSQL provider allocation test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("provider_cost_allocation_test_{}", Uuid::new_v4().simple());
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
