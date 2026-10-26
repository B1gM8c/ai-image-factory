use super::*;
use gpt_image_2_gateway::settlement::{VideoPendingStage, VideoResultStatus};
use serde_json::Value;

async fn accepted(
    pool: &PgPool,
    fixture: &Fixture,
    plan: &DreaminaVideoSupplementPlan,
) -> TestResult<(DreaminaVideoSupplementPlan, Publish)> {
    let service = store(pool);
    service
        .claim_dreamina_video_supplement_dispatch(
            plan.authorization_id,
            plan.dispatch_nonce,
            &plan.runtime_binding_json,
        )
        .await
        .map_err(debug_error)?
        .ok_or("missing schema154 supplement permit")?;
    let (receipt, publish) = evidence(fixture, plan)?;
    service
        .record_dreamina_video_supplement_accepted(&receipt)
        .await
        .map_err(debug_error)?;
    let plan = service
        .load_dreamina_video_supplement(plan.authorization_id)
        .await
        .map_err(debug_error)?;
    Ok((plan, publish))
}

async fn snapshot(pool: &PgPool, job: Uuid) -> TestResult<Value> {
    let mut result = serde_json::Map::new();
    for table in [
        "jobs",
        "work_items",
        "job_outputs",
        "provider_submissions",
        "provider_receipts",
        "provider_remote_submit_intents",
        "executor_terminal_reductions",
        "customer_price_quotes",
        "customer_price_quote_lines",
        "customer_billing_holds",
        "customer_billing_hold_grant_reservations",
        "customer_rated_usage",
        "customer_rated_usage_lines",
        "provider_usage_facts",
        "ledger_transactions",
        "ledger_postings",
        "ledger_transaction_seals",
        "artifacts",
        "job_response_projections",
        "dreamina_operator_video_supplement_authorizations",
        "dreamina_operator_video_supplement_resolutions",
    ] {
        let rows: Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY to_jsonb(item)::TEXT),'[]'::JSONB) FROM {table} item WHERE dreamina_video_supplement_mutation_job(to_jsonb(item))=$1"
        )))
        .bind(job)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        result.insert(table.to_owned(), rows);
    }
    let observations: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(observation) ORDER BY observation.observation_id),'[]'::JSONB) FROM dreamina_operator_video_supplement_observations observation JOIN dreamina_operator_video_supplement_authorizations head USING(authorization_id) WHERE head.job_id=$1",
    )
    .bind(job)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    result.insert("supplement_observations".to_owned(), observations);
    Ok(Value::Object(result))
}

#[tokio::test]
async fn manual_cancellation_upgrade_preserves_populated_schema154_and_active_successor()
-> TestResult {
    let Ok(url) = env::var("TEST_DATABASE_URL") else {
        return Ok(());
    };
    // The common quote fixture owns fixed organization/project identities, so
    // exercise both populated histories in separate real migration schemas.
    for delivered in [true, false] {
        populated_upgrade_case(&url, delivered).await?;
    }
    Ok(())
}

async fn populated_upgrade_case(url: &str, delivered: bool) -> TestResult {
    let schema = format!("image_gateway_cancel_upgrade_{}", Uuid::new_v4().simple());
    let pool = connect_test_pool_with_search_path(url, 24, &schema)
        .await
        .map_err(debug_error)?;
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .map_err(debug_error)?;
    require(
        database_name.to_ascii_lowercase().contains("test"),
        "refusing cancellation upgrade test in non-test database",
    )?;
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&pool)
        .await
        .map_err(debug_error)?;
    let db = TestDatabase { schema, pool };
    let result = async {
        let migrations = sqlx::migrate!("./migrations");
        let old = sqlx::migrate::Migrator::with_migrations(
            migrations.iter().filter(|migration| migration.version <= 154).cloned().collect(),
        );
        old.run(&db.pool).await.map_err(debug_error)?;
        seed_execution_profiles(&db.pool).await?;
        let service = store(&db.pool);
        let fixture = if delivered {
            fixture(&db.pool, "seedance2.0mini").await?
        } else {
            fixture_with_audio(&db.pool, "seedance2.0fast_vip", 2).await?
        };
        let initial = service.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
        let head = if delivered {
            initial.clone()
        } else {
            service.claim_dreamina_video_supplement_dispatch(initial.authorization_id, initial.dispatch_nonce, &initial.runtime_binding_json)
                .await.map_err(debug_error)?.ok_or("missing schema154 unknown parent permit")?;
            service.record_dreamina_video_supplement_uncertain(&Uncertain {
                authorization_id: initial.authorization_id, dispatch_nonce: initial.dispatch_nonce,
                error_code: "schema154_unknown_effect".to_owned(),
            }).await.map_err(debug_error)?;
            let mut successor_request = fixture.request.clone();
            successor_request.operator_reference = "offline-new-human-instruction:schema154-successor".to_owned();
            service.authorize_dreamina_video_supplement_successor(initial.authorization_id, &successor_request).await.map_err(debug_error)?
        };
        let (head, publish) = accepted(&db.pool, &fixture, &head).await?;
        let observed_at = database_now(&db.pool).await?;
        let query = serde_json::to_vec(&json!({
            "submit_id":head.remote_operation_id,"gen_status":"querying",
            "queue_info":{"queue_idx":610,"queue_length":233061,"queue_status":"Queueing"},
            "estimated_remaining_seconds":5860
        })).map_err(debug_error)?;
        let resolution = if delivered {
            let resolution = service.publish_dreamina_video_supplement(&fixture.root, &publish).await.map_err(debug_error)?;
            require(resolution.captured_micros == 800_000 && resolution.released_micros == 0,
                "schema154 delivered fixture did not settle original quote")?;
            Some(resolution)
        } else {
            service.record_dreamina_video_supplement_observation(head.authorization_id, head.dispatch_nonce, &query, observed_at).await.map_err(debug_error)?;
            None
        };
        let before = snapshot(&db.pool, head.job_id).await?;

        migrations.run(&db.pool).await.map_err(debug_error)?;
        let version: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations WHERE success")
            .fetch_one(&db.pool).await.map_err(debug_error)?;
        require(version == 156, "cancellation upgrade did not reach schema156")?;
        require(snapshot(&db.pool, head.job_id).await? == before,
            "155/156 migration rewrote existing authority, result, quote, hold, ledger or queue observation")?;
        let cancellations: (i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_manual_video_cancellations),(SELECT count(*) FROM dreamina_operator_video_supplement_cancellations)")
            .fetch_one(&db.pool).await.map_err(debug_error)?;
        require(cancellations == (0,0), "upgrade classified an existing job as cancelled")?;
        if let Some(resolution) = resolution {
            require(matches!(service.video_status("executord-process-smoke", head.job_id).await.map_err(debug_error)?,
                Some(VideoResultStatus::Succeeded { artifact_id, .. }) if artifact_id == resolution.artifact_id),
                "existing successful delivery disappeared after cancellation upgrade")?;
            return Ok(());
        }
        require(matches!(service.project_video_status_with_metadata("executord-process-smoke", "project-test", None, head.job_id).await.map_err(debug_error)?,
            Some((VideoResultStatus::Pending { stage: VideoPendingStage::Processing, provider_progress: Some(ref progress), .. }, _))
                if progress.observed_at_ms == observed_at && progress.progress.queue_position == Some(610)),
            "existing successor queue observation disappeared after upgrade")?;
        require(service.load_dreamina_video_supplement(initial.authorization_id).await.is_err(),
            "upgrade reopened superseded ancestor")?;
        service.record_dreamina_video_supplement_observation(head.authorization_id, head.dispatch_nonce, &query, observed_at + 1).await.map_err(debug_error)?;
        assert_pending(&db.pool, head.job_id).await?;
        let frozen: Value = sqlx::query_scalar("SELECT dreamina_video_supplement_original_snapshot($1)")
            .bind(head.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        let evidence = manual_cancellation::cancellation_evidence(&db.pool, &head).await?;
        let proof = service.confirm_dreamina_supplement_video_cancellation(&evidence.root, &evidence.request).await.map_err(debug_error)?;
        require(proof.job_id == head.job_id && proof.captured_micros == 0 && proof.released_micros == 1_200_000,
            "upgraded successor cancellation changed original quote or failed to release hold")?;
        require(service.confirm_dreamina_supplement_video_cancellation(&evidence.root, &evidence.request).await.map_err(debug_error)? == proof,
            "upgraded cancellation replay minted new settlement")?;
        let after: Value = sqlx::query_scalar("SELECT dreamina_video_supplement_original_snapshot($1)")
            .bind(head.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(after == frozen, "cancellation rewrote upgraded original execution or frozen quote")?;
        Ok(())
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}
