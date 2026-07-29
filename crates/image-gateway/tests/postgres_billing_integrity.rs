use std::env;

use gpt_image_2_gateway::{
    BillingIntegrityService, PostgresBillingIntegrityService,
    billing_integrity::{BillingIntegrityActor, ListBillingIntegrityRunsRequest},
    customer_refunds::{
        CreateCustomerRefundRequest, CustomerRefundActor, CustomerRefundService,
        PostgresCustomerRefundService,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
    provider_cost_obligations::{
        ListProviderCostObligationsRequest, PostgresProviderCostObligationService,
        ProviderCostObligationService,
    },
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn billing_integrity_runs_are_snapshot_scoped_audited_and_immutable() -> TestResult {
    let Some(schema) = TestSchema::new(4).await? else {
        return Ok(());
    };
    let result = billing_integrity_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn billing_integrity_flags_only_explicit_provider_actual_facts_without_authority()
-> TestResult {
    let Some(schema) = TestSchema::new(4).await? else {
        return Ok(());
    };
    let result = provider_cost_authority_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn provider_cost_obligations_preserve_uncertainty_and_require_evidence() -> TestResult {
    let Some(schema) = TestSchema::new(4).await? else {
        return Ok(());
    };
    let result = provider_cost_obligation_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn billing_integrity_detects_provider_receipts_without_cost_obligations() -> TestResult {
    let Some(schema) = TestSchema::new(4).await? else {
        return Ok(());
    };
    let result = provider_cost_obligation_coverage_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn billing_integrity_detects_customer_refund_transactions_without_evidence() -> TestResult {
    let Some(schema) = TestSchema::new(4).await? else {
        return Ok(());
    };
    let result = customer_refund_coverage_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn billing_integrity_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("billing integrity migrations failed: {error:?}"))?;
    let actor = BillingIntegrityActor {
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
            $1, $2, 'Billing integrity operator',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(actor.user_id)
    .bind(format!("billing-integrity-{}@test.local", actor.user_id))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let tenant_id = format!("billing-integrity-{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros,
            held_micros, captured_micros,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'USD', 1000000, 100000, 0, 1, 1)
        "#,
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let service = PostgresBillingIntegrityService::new(pool.clone());
    let first = service.run(actor).await.map_err(debug_error)?;
    require(
        first.run.state == "completed"
            && first.run.scope_type == "platform"
            && first.run.actor_kind == "manual"
            && first.run.critical_count == 1
            && first.run.warning_count == 0
            && first.run.finding_count == 1
            && first.findings.len() == 1
            && first.findings[0].finding_code == "billing_account_counter_mismatch"
            && first.findings[0].tenant_id.as_deref() == Some(tenant_id.as_str()),
        "account-counter mismatch was not preserved as one critical finding",
    )?;
    require(
        first.findings[0].finding_id != Uuid::nil().to_string(),
        "returned finding did not retain its persisted identity",
    )?;
    let audit_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM identity_audit_events
        WHERE action = 'billing.integrity.run'
          AND resource_id = $1
          AND actor_user_id = $2
          AND session_id = $3
        "#,
    )
    .bind(&first.run.run_id)
    .bind(actor.user_id)
    .bind(actor.session_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        audit_count == 1,
        "completed integrity run omitted its identity audit",
    )?;

    sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = 0, updated_at_ms = 2
        WHERE tenant_id = $1 AND currency = 'USD'
        "#,
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let clean = service.run(actor).await.map_err(debug_error)?;
    require(
        clean.run.finding_count == 0 && clean.findings.is_empty(),
        "clean snapshot invented an integrity finding",
    )?;

    let first_page = service
        .list_runs(ListBillingIntegrityRunsRequest {
            after: None,
            limit: Some(1),
        })
        .await
        .map_err(debug_error)?;
    require(
        first_page.data.len() == 1 && first_page.has_more && first_page.next_after.is_some(),
        "integrity run history did not expose a stable first keyset page",
    )?;
    let second_page = service
        .list_runs(ListBillingIntegrityRunsRequest {
            after: first_page.next_after,
            limit: Some(1),
        })
        .await
        .map_err(debug_error)?;
    require(
        second_page.data.len() == 1 && !second_page.has_more,
        "integrity run history cursor skipped or duplicated a run",
    )?;
    let loaded = service
        .get_run(Uuid::parse_str(&first.run.run_id).map_err(debug_error)?)
        .await
        .map_err(debug_error)?;
    require(
        loaded.run.run_id == first.run.run_id
            && loaded.findings.len() == 1
            && loaded.findings[0].finding_id == first.findings[0].finding_id,
        "integrity run detail did not replay immutable findings",
    )?;

    let mut lock = pool.begin().await.map_err(debug_error)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('billing-integrity-run', 0))")
        .execute(&mut *lock)
        .await
        .map_err(debug_error)?;
    let concurrent = service
        .run(actor)
        .await
        .expect_err("a second platform integrity scan must not overlap");
    require(
        concurrent.status_code().as_u16() == 409,
        "overlapping integrity scan did not return conflict",
    )?;
    lock.rollback().await.map_err(debug_error)?;

    let run_mutation =
        sqlx::query("UPDATE billing_integrity_runs SET summary = '{}'::JSONB WHERE run_id = $1")
            .bind(Uuid::parse_str(&first.run.run_id).map_err(debug_error)?)
            .execute(pool)
            .await
            .expect_err("integrity run evidence must be immutable");
    expect_sqlstate(run_mutation, "55000", "integrity run mutation")?;
    let finding_mutation = sqlx::query(
        "UPDATE billing_integrity_findings SET details = '{}'::JSONB WHERE finding_id = $1",
    )
    .bind(Uuid::parse_str(&first.findings[0].finding_id).map_err(debug_error)?)
    .execute(pool)
    .await
    .expect_err("integrity finding evidence must be immutable");
    expect_sqlstate(finding_mutation, "55000", "integrity finding mutation")
}

async fn customer_refund_coverage_case(pool: &PgPool) -> TestResult {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    run_migrations(pool)
        .await
        .map_err(|error| format!("customer refund coverage migrations failed: {error:?}"))?;
    let actor = BillingIntegrityActor {
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
            $1, $2, 'Customer refund integrity operator',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(actor.user_id)
    .bind(format!(
        "customer-refund-integrity-{}@test.local",
        actor.user_id
    ))
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let tenant_id = format!("customer-refund-integrity-{}", Uuid::new_v4().simple());
    let job_id = Uuid::new_v4();
    let charge_transaction_id = Uuid::new_v4();
    let receivable_account_id = Uuid::new_v4();
    let revenue_account_id = Uuid::new_v4();
    let mut fixture = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO billing_accounts (
            tenant_id, currency, credit_limit_micros,
            held_micros, captured_micros, refunded_micros,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'USD', 1000000, 0, 20000, 0, 1, 1)
        "#,
    )
    .bind(&tenant_id)
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO jobs (
            job_id, tenant_id, request_id, operation, provider_id, model,
            state, requested_units, charged_units,
            created_at_ms, updated_at_ms,
            output_count, billable_units, billing_metric, billing_unit
        )
        VALUES (
            $1, $2, $3, 'generation', 'xai', 'grok-imagine-image',
            'succeeded', 1, 1, 1, 1, 1, 1, 'output', 'output'
        )
        "#,
    )
    .bind(job_id)
    .bind(&tenant_id)
    .bind(format!("customer-refund-integrity-{job_id}"))
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO ledger_accounts (
            account_id, account_key, owner_type, owner_id,
            account_type, currency, created_at_ms
        )
        VALUES
            ($1, $2, 'tenant', $3, 'receivable', 'USD', 1),
            ($4, $5, 'platform', 'platform', 'revenue', 'USD', 1)
        "#,
    )
    .bind(receivable_account_id)
    .bind(format!("tenant:{tenant_id}:receivable:USD"))
    .bind(&tenant_id)
    .bind(revenue_account_id)
    .bind(format!("platform:revenue:USD:{}", Uuid::new_v4().simple()))
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "ALTER TABLE ledger_transactions \
         DISABLE TRIGGER ledger_transactions_validate_economics_contract",
    )
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions (
            transaction_id, semantic_key, source_job_id,
            transaction_type, currency, payload_hash, created_at_ms
        )
        VALUES (
            $1, $2, $3, 'customer_job_charge',
            'USD', repeat('a', 64), 1
        )
        "#,
    )
    .bind(charge_transaction_id)
    .bind(format!("customer-job-charge:{charge_transaction_id}"))
    .bind(job_id)
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO ledger_postings (
            transaction_id, posting_no, account_id,
            currency, amount_micros, created_at_ms
        )
        VALUES
            ($1, 1, $2, 'USD', 20000, 1),
            ($1, 2, $3, 'USD', -20000, 1)
        "#,
    )
    .bind(charge_transaction_id)
    .bind(receivable_account_id)
    .bind(revenue_account_id)
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms)
        VALUES ($1, 1)
        "#,
    )
    .bind(charge_transaction_id)
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *fixture)
        .await
        .map_err(debug_error)?;
    sqlx::query(
        "ALTER TABLE ledger_transactions \
         ENABLE TRIGGER ledger_transactions_validate_economics_contract",
    )
    .execute(&mut *fixture)
    .await
    .map_err(debug_error)?;
    fixture.commit().await.map_err(debug_error)?;

    let refund = PostgresCustomerRefundService::new(pool.clone())
        .create_refund(
            charge_transaction_id,
            "integrity-orphan-refund",
            CustomerRefundActor {
                user_id: actor.user_id,
                session_id: actor.session_id,
            },
            CreateCustomerRefundRequest {
                amount_micros: "5000".to_string(),
                reason_code: "billing_correction".to_string(),
                reason: "Integrity coverage test".to_string(),
            },
        )
        .await
        .map_err(debug_error)?;
    let refund_transaction_id =
        Uuid::parse_str(&refund.refund_transaction_id).map_err(debug_error)?;

    sqlx::query("ALTER TABLE customer_refunds DISABLE TRIGGER USER")
        .execute(pool)
        .await
        .map_err(debug_error)?;
    sqlx::query("DELETE FROM customer_refunds WHERE refund_transaction_id = $1")
        .bind(refund_transaction_id)
        .execute(pool)
        .await
        .map_err(debug_error)?;
    sqlx::query("ALTER TABLE customer_refunds ENABLE TRIGGER USER")
        .execute(pool)
        .await
        .map_err(debug_error)?;

    let scan = PostgresBillingIntegrityService::new(pool.clone())
        .run(actor)
        .await
        .map_err(debug_error)?;
    require(
        scan.findings.iter().any(|finding| {
            finding.category == "customer_refund"
                && finding.finding_code == "customer_refund_evidence_missing"
                && finding.resource_type == "ledger_transaction"
                && finding.resource_id == refund.refund_transaction_id
        }),
        "integrity scan did not detect an orphan customer refund transaction",
    )
}

async fn provider_cost_authority_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("provider authority migrations failed: {error:?}"))?;
    let actor = BillingIntegrityActor {
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
            $1, $2, 'Provider cost integrity operator',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(actor.user_id)
    .bind(format!(
        "provider-cost-integrity-{}@test.local",
        actor.user_id
    ))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let fixture = seed_orphan_provider_actual_fact(pool, "succeeded").await?;

    let service = PostgresBillingIntegrityService::new(pool.clone());
    let result = service.run(actor).await.map_err(debug_error)?;
    require(
        result.run.critical_count == 2
            && result.run.warning_count == 0
            && result.run.finding_count == 2
            && result
                .run
                .check_set
                .iter()
                .any(|check| check == "provider_cost_authority")
            && result
                .run
                .check_set
                .iter()
                .any(|check| check == "provider_cost_obligation_aging"),
        "provider cost scans did not retain their check-set contract",
    )?;
    let finding = result
        .findings
        .iter()
        .find(|finding| finding.finding_code == "provider_cost_authority_missing")
        .ok_or_else(|| "provider actual authority finding is missing".to_string())?;
    require(
        finding.finding_code == "provider_cost_authority_missing"
            && finding.category == "provider_cost"
            && finding.resource_type == "provider_usage_fact"
            && finding.resource_id == fixture.usage_fact_id.to_string()
            && finding.currency.as_deref() == Some("USD")
            && finding
                .actual
                .get("authority_kind")
                .and_then(|value| value.as_str())
                == Some("missing"),
        "provider actual authority finding lost its immutable evidence",
    )
}

async fn provider_cost_obligation_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("provider obligation migrations failed: {error:?}"))?;
    let user_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO identity_users (
            user_id, normalized_email, display_name, roles, scopes,
            authz_version, failed_login_count, created_at_ms, updated_at_ms
        )
        VALUES (
            $1, $2, 'Provider cost obligation operator',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(user_id)
    .bind(format!("provider-obligation-{user_id}@test.local"))
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let uncertain = seed_orphan_provider_actual_fact(pool, "uncertain").await?;
    let uncertain_state: (String, Option<String>, i64, i64) = sqlx::query_as(
        r#"
        SELECT state, pending_reason_code, due_at_ms, escalate_at_ms
        FROM provider_cost_obligations
        WHERE receipt_id = $1
        "#,
    )
    .bind(uncertain.receipt_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        uncertain_state
            == (
                "pending".to_string(),
                Some("provider_outcome_uncertain".to_string()),
                86_400_001,
                172_800_001,
            ),
        "an uncertain provider receipt did not retain a pending cost obligation",
    )?;

    let fake_waiver = sqlx::query(
        r#"
        UPDATE provider_cost_obligations
        SET state = 'waived',
            pending_reason_code = NULL,
            waiver_reason_code = 'confirmed_no_effect',
            waiver_source_kind = 'provider_receipt',
            waiver_source_id = $2,
            waiver_evidence_hash = repeat('e', 64),
            waived_by_user_id = $3,
            waived_by_session_id = $4,
            waived_at_ms = 2,
            updated_at_ms = 2,
            control_version = control_version + 1
        WHERE receipt_id = $1
        "#,
    )
    .bind(uncertain.receipt_id)
    .bind(uncertain.receipt_id.to_string())
    .bind(user_id)
    .bind(session_id)
    .execute(pool)
    .await
    .expect_err("uncertain provider outcome must not be waived as confirmed no-effect");
    expect_sqlstate(fake_waiver, "23514", "uncertain receipt waiver")?;

    let stale_review = sqlx::query(
        r#"
        UPDATE provider_cost_obligations
        SET last_reviewed_at_ms = 2,
            next_review_at_ms = 3,
            review_attempt_count = review_attempt_count + 1,
            updated_at_ms = 2
        WHERE receipt_id = $1
        "#,
    )
    .bind(uncertain.receipt_id)
    .execute(pool)
    .await
    .expect_err("provider cost review must use optimistic control versioning");
    expect_sqlstate(stale_review, "40001", "stale provider cost review")?;

    let no_effect = seed_orphan_provider_actual_fact(pool, "no_effect").await?;
    sqlx::query(
        r#"
        UPDATE provider_cost_obligations
        SET state = 'waived',
            pending_reason_code = NULL,
            waiver_reason_code = 'confirmed_no_effect',
            waiver_source_kind = 'provider_receipt',
            waiver_source_id = $2,
            waiver_evidence_hash = repeat('f', 64),
            waived_by_user_id = $3,
            waived_by_session_id = $4,
            waived_at_ms = 2,
            updated_at_ms = 2,
            control_version = control_version + 1
        WHERE receipt_id = $1
        "#,
    )
    .bind(no_effect.receipt_id)
    .bind(no_effect.receipt_id.to_string())
    .bind(user_id)
    .bind(session_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let waived: (String, i64, i64) = sqlx::query_as(
        r#"
        SELECT obligation.state, obligation.control_version,
               COUNT(event.event_id)::BIGINT
        FROM provider_cost_obligations obligation
        JOIN provider_cost_obligation_events event
          ON event.receipt_id = obligation.receipt_id
        WHERE obligation.receipt_id = $1
        GROUP BY obligation.receipt_id
        "#,
    )
    .bind(no_effect.receipt_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        waived == ("waived".to_string(), 2, 2),
        "evidence-backed no-effect waiver did not produce one terminal event",
    )?;

    let obligation_service = PostgresProviderCostObligationService::new(pool.clone());
    let queue = obligation_service
        .list(ListProviderCostObligationsRequest {
            after: None,
            limit: Some(25),
            state: None,
            urgency: None,
            provider_id: Some("xai".to_string()),
        })
        .await
        .map_err(debug_error)?;
    require(
        queue.summary.open == 1
            && queue.summary.overdue == 1
            && queue.summary.escalated == 1
            && queue.summary.settled == 0
            && queue.summary.waived == 1
            && queue.data.len() == 1
            && queue.data[0].receipt_id == uncertain.receipt_id.to_string()
            && queue.data[0].urgency == "escalated",
        "provider cost queue did not separate open work from terminal history",
    )?;
    let detail = obligation_service
        .get(no_effect.receipt_id)
        .await
        .map_err(debug_error)?;
    require(
        detail.obligation.state == "waived"
            && detail.events.len() == 2
            && detail.events[0].event_kind == "created"
            && detail.events[1].event_kind == "waived",
        "provider cost detail did not replay its immutable lifecycle",
    )?;
    let invalid_filter = obligation_service
        .list(ListProviderCostObligationsRequest {
            state: Some("mystery".to_string()),
            ..ListProviderCostObligationsRequest::default()
        })
        .await
        .expect_err("unknown provider cost state must fail closed");
    require(
        invalid_filter.status_code().as_u16() == 400,
        "unknown provider cost state did not return invalid request",
    )?;

    let terminal_mutation = sqlx::query(
        r#"
        UPDATE provider_cost_obligations
        SET updated_at_ms = 3, control_version = 3
        WHERE receipt_id = $1
        "#,
    )
    .bind(no_effect.receipt_id)
    .execute(pool)
    .await
    .expect_err("terminal provider cost obligations must be immutable");
    expect_sqlstate(
        terminal_mutation,
        "55000",
        "terminal provider cost obligation mutation",
    )?;
    let event_mutation = sqlx::query(
        r#"
        UPDATE provider_cost_obligation_events
        SET details = '{}'::JSONB
        WHERE receipt_id = $1
        "#,
    )
    .bind(no_effect.receipt_id)
    .execute(pool)
    .await
    .expect_err("provider cost obligation events must be immutable");
    expect_sqlstate(event_mutation, "55000", "provider cost event mutation")
}

async fn provider_cost_obligation_coverage_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("provider obligation coverage migrations failed: {error:?}"))?;
    let actor = BillingIntegrityActor {
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
            $1, $2, 'Provider cost coverage operator',
            ARRAY['platform_owner'], ARRAY['admin:*'], 1, 0, 1, 1
        )
        "#,
    )
    .bind(actor.user_id)
    .bind(format!(
        "provider-cost-coverage-{}@test.local",
        actor.user_id
    ))
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "ALTER TABLE provider_receipts DISABLE TRIGGER provider_receipts_create_cost_obligation",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "ALTER TABLE provider_receipts DISABLE TRIGGER provider_receipts_require_cost_obligation",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let fixture = seed_orphan_provider_actual_fact(pool, "succeeded").await?;
    sqlx::query(
        "ALTER TABLE provider_receipts ENABLE TRIGGER provider_receipts_create_cost_obligation",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        "ALTER TABLE provider_receipts ENABLE TRIGGER provider_receipts_require_cost_obligation",
    )
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let result = PostgresBillingIntegrityService::new(pool.clone())
        .run(actor)
        .await
        .map_err(debug_error)?;
    let finding = result
        .findings
        .iter()
        .find(|finding| finding.finding_code == "provider_cost_obligation_missing")
        .ok_or_else(|| "provider cost obligation coverage finding is missing".to_string())?;
    require(
        result.run.critical_count == 2
            && result.run.warning_count == 0
            && finding.category == "provider_cost"
            && finding.resource_type == "provider_receipt"
            && finding.resource_id == fixture.receipt_id.to_string()
            && finding.tenant_id.as_deref() == Some("provider-cost-integrity")
            && result
                .run
                .check_set
                .iter()
                .any(|check| check == "provider_cost_obligation_coverage"),
        "provider receipt coverage scan lost its identity or severity contract",
    )
}

#[derive(Clone, Copy)]
struct ProviderActualFixture {
    receipt_id: Uuid,
    usage_fact_id: Uuid,
}

async fn seed_orphan_provider_actual_fact(
    pool: &PgPool,
    outcome: &str,
) -> TestResult<ProviderActualFixture> {
    let credential_pool_id = Uuid::new_v4();
    let provider_account_id = Uuid::new_v4();
    let resource_policy_id = Uuid::new_v4();
    let execution_profile_id = Uuid::new_v4();
    let credential_ref = format!("billing-integrity.{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools (
            credential_pool_id, pool_key, provider_id, state,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, 'xai', 'enabled', 1, 1)
        "#,
    )
    .bind(credential_pool_id)
    .bind(format!(
        "billing-integrity-pool-{}",
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
        VALUES ($1, $2, 'xai', $3, $4, 1,
                repeat('a', 64), 'enabled', 1, 1)
        "#,
    )
    .bind(provider_account_id)
    .bind(credential_pool_id)
    .bind(format!("billing-integrity-{}", Uuid::new_v4().simple()))
    .bind(&credential_ref)
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
        VALUES ($1, 1, $2, $3, 'xai', 'inline', 1, 'enabled', 1)
        "#,
    )
    .bind(resource_policy_id)
    .bind(credential_pool_id)
    .bind(provider_account_id)
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
        VALUES (
            $1, $2, 'xai', 'billing-integrity-v1',
            'images.generations', 'xai/images.generations/v1',
            repeat('b', 64), 'inline', 'submission_bound',
            'billing-integrity-v1', $3, $4, $5, 1, $6, 1,
            'enabled', 1, 1
        )
        "#,
    )
    .bind(execution_profile_id)
    .bind(format!(
        "billing-integrity-profile-{}",
        Uuid::new_v4().simple()
    ))
    .bind(credential_pool_id)
    .bind(provider_account_id)
    .bind(&credential_ref)
    .bind(resource_policy_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let job_id = Uuid::new_v4();
    let output_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let created_by_execution_id = Uuid::new_v4();
    let executor_execution_id = Uuid::new_v4();
    let submission_id = Uuid::new_v4();
    let receipt_id = Uuid::new_v4();
    let usage_fact_id = Uuid::new_v4();
    let mut tx = pool.begin().await.map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO jobs (
            job_id, tenant_id, request_id, operation, provider_id, model,
            state, requested_units, output_count, billable_units,
            billing_metric, billing_unit, economics_contract_version,
            created_at_ms, updated_at_ms
        )
        VALUES (
            $1, 'provider-cost-integrity', $2, 'generation', 'xai',
            'grok-imagine-image', 'reserved', 1, 1, 1,
            'output', 'output', 4, 1, 1
        )
        "#,
    )
    .bind(job_id)
    .bind(format!("provider-cost-integrity-{job_id}"))
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
    .bind(execution_profile_id)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts (
            attempt_id, execution_id, work_item_id, lease_epoch,
            worker_id, state, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 1, 'billing-integrity', 'claimed', 1, 1)
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
        VALUES (
            $1, $2, $3, $4, 'provider-cost-integrity', 'xai',
            'grok-imagine-image', $5, $6, 1, 'billing-integrity-v1',
            repeat('c', 64), $7, $8, $9, $10, 1,
            'billing-integrity-v1', $11, 1,
            'images.generations', 'xai/images.generations/v1',
            repeat('b', 64), 'inline', 'submission_bound', 2,
            'prepared', 1, 1
        )
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(output_id)
    .bind(job_id)
    .bind(work_item_id)
    .bind(created_by_execution_id)
    .bind(execution_profile_id)
    .bind(credential_pool_id)
    .bind(provider_account_id)
    .bind(&credential_ref)
    .bind(resource_policy_id)
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
        INSERT INTO provider_receipts (
            receipt_id, semantic_key, submission_id, output_id, job_id,
            provider_id, outcome, receipt_schema, payload_hash,
            evidence, created_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, 'xai', $6,
            'billing-integrity-v1', repeat('d', 64),
            '{"source":"billing-integrity-test"}', 1
        )
        "#,
    )
    .bind(receipt_id)
    .bind(format!("provider-cost-integrity-receipt:{receipt_id}"))
    .bind(submission_id)
    .bind(output_id)
    .bind(job_id)
    .bind(outcome)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO provider_usage_facts (
            usage_fact_id, semantic_key, job_id, output_id, submission_id,
            receipt_id, provider_id, provider_account_id,
            execution_surface, fact_domain, metric, quantity,
            unit, quantity_source, confidence, evidence_path,
            metadata_json, billing_partition_key,
            terminal_outcome, created_at_ms
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, 'xai', $7,
            'provider_api', 'provider_actual', 'provider_reported_cost', 1,
            'usd_tick', 'provider_reported', 'exact',
            'test://provider-cost/orphan',
            '{"schema":"provider_cost_fact.v1"}',
            'provider-cost', $8, 1
        )
        "#,
    )
    .bind(usage_fact_id)
    .bind(format!("provider-cost-integrity-fact:{usage_fact_id}"))
    .bind(job_id)
    .bind(output_id)
    .bind(submission_id)
    .bind(receipt_id)
    .bind(provider_account_id)
    .bind(outcome)
    .execute(&mut *tx)
    .await
    .map_err(debug_error)?;
    tx.commit().await.map_err(debug_error)?;
    Ok(ProviderActualFixture {
        receipt_id,
        usage_fact_id,
    })
}

fn expect_sqlstate(error: sqlx::Error, expected: &str, operation: &str) -> TestResult {
    let actual = error
        .as_database_error()
        .and_then(|error| error.code())
        .map(|code| code.into_owned());
    require(
        actual.as_deref() == Some(expected),
        &format!("{operation} returned SQLSTATE {actual:?}, expected {expected}"),
    )
}

fn require(condition: bool, message: &str) -> TestResult {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL billing integrity test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("billing_integrity_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(debug_error)?;
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
