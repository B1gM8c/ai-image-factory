use super::*;
use gpt_image_2_gateway::DreaminaSupplementVideoCancellationRequest as Cancel;
use serde_json::Value;

pub(super) struct CancellationEvidence {
    pub(super) root: PathBuf,
    pub(super) request: Cancel,
    _directory: tempfile::TempDir,
}

async fn accept(
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
        .ok_or("supplement cancellation fixture did not acquire one dispatch permit")?;
    let (accepted, publish) = evidence(fixture, plan)?;
    service
        .record_dreamina_video_supplement_accepted(&accepted)
        .await
        .map_err(debug_error)?;
    let accepted = service
        .load_dreamina_video_supplement(plan.authorization_id)
        .await
        .map_err(debug_error)?;
    Ok((accepted, publish))
}

pub(super) async fn cancellation_evidence(
    pool: &PgPool,
    plan: &DreaminaVideoSupplementPlan,
) -> TestResult<CancellationEvidence> {
    let directory = tempfile::tempdir().map_err(debug_error)?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .map_err(debug_error)?;
    let root = fs::canonicalize(directory.path()).map_err(debug_error)?;
    let request_id: String = sqlx::query_scalar("SELECT request_id FROM jobs WHERE job_id=$1")
        .bind(plan.job_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
    let observed_at_ms = database_now(pool).await?;
    let remote = plan
        .remote_operation_id
        .as_ref()
        .ok_or("missing accepted upstream id")?;
    let query = serde_json::to_vec(&json!({
        "submit_id":remote,"gen_status":"fail","fail_reason":"Canceled by user"
    }))
    .map_err(debug_error)?;
    let request = Cancel {
        authorization_id: plan.authorization_id,
        dispatch_nonce: plan.dispatch_nonce,
        reviewed_stdout_sha256: sha256(&query),
        operator_reference: "offline-only:head-website-cancellation".into(),
    };
    let audit = serde_json::to_vec(&json!({
        "schema_version":1,"job_id":plan.job_id,"request_id":request_id,
        "submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,
        "provider_account_id":plan.provider_account_id,"upstream_task_id":remote,
        "upstream_identity_sha256":plan.runtime_binding_json["upstream_identity_sha256"],
        "account_home_sha256":plan.runtime_binding_json["account_home_sha256"],
        "argv":["query_result","--submit_id",remote],"stdout_sha256":request.reviewed_stdout_sha256,
        "exit_code":0,"executed_at_ms":observed_at_ms
    }))
    .map_err(debug_error)?;
    let mut screenshot = std::io::Cursor::new(Vec::new());
    RgbaImage::new(2, 2)
        .write_to(&mut screenshot, ImageFormat::Png)
        .map_err(debug_error)?;
    let screenshot = screenshot.into_inner();
    let review = serde_json::to_vec(&json!({
        "schema":"factory.manual_cancellation.review.v1","job_id":plan.job_id,"request_id":request_id,
        "submission_id":plan.submission_id,"executor_execution_id":plan.executor_execution_id,
        "provider_account_id":plan.provider_account_id,"upstream_task_id":remote,
        "operator_reference":request.operator_reference,"website_cancelled":true,
        "query_stdout_sha256":request.reviewed_stdout_sha256,"execution_audit_sha256":sha256(&audit),
        "website_confirmation_sha256":sha256(&screenshot),"confirmed_at_ms":observed_at_ms
    })).map_err(debug_error)?;
    let head = serde_json::to_vec(&json!({
        "schema":"factory.supplement_cancellation.review.v1",
        "authorization_id":plan.authorization_id,"dispatch_nonce":plan.dispatch_nonce,
        "job_id":plan.job_id,"request_id":request_id,"submission_id":plan.submission_id,
        "executor_execution_id":plan.executor_execution_id,"provider_account_id":plan.provider_account_id,
        "upstream_task_id":remote,"quote_hash":plan.quote_hash,"command_hash":plan.command_hash,
        "submit_stdout_sha256":plan.submit_stdout_sha256.as_ref().ok_or("missing submit stdout hash")?,
        "operator_reference":request.operator_reference,"query_stdout_sha256":request.reviewed_stdout_sha256,
        "base_review_sha256":sha256(&review),"original_effect_certainty":"unknown_remote_effect",
        "customer_charge_waived":true
    })).map_err(debug_error)?;
    for (name, bytes) in [
        ("query-stdout.json", query),
        ("execution-audit.json", audit),
        ("review.json", review),
        ("website-confirmation.png", screenshot),
        ("head-review.json", head),
    ] {
        write(&root, name, &bytes)?;
    }
    Ok(CancellationEvidence {
        root,
        request,
        _directory: directory,
    })
}

#[tokio::test]
async fn cancelled_head_poll_is_terminal_without_runtime_or_observation_writes() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let service = store(&db.pool);
        let plan = service
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        let (plan, _) = accept(&db.pool, &fixture, &plan).await?;
        let cancellation = cancellation_evidence(&db.pool, &plan).await?;
        service
            .confirm_dreamina_supplement_video_cancellation(&cancellation.root, &cancellation.request)
            .await
            .map_err(debug_error)?;
        let before = original_evidence(&db.pool, plan.job_id).await?;
        let observation_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM dreamina_operator_video_supplement_observations WHERE authorization_id=$1",
        ).bind(plan.authorization_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        // Each invocation is a new process. With no provider runtime environment and
        // nonexistent /offline executables, reaching Runtime::load/exec cannot succeed.
        for _ in 0..2 {
            let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_factoryctl"))
                .env_clear()
                .env("DATABASE_URL", env::var("TEST_DATABASE_URL").map_err(debug_error)?)
                .env("GATEWAY_DATABASE_SCHEMA", &db.schema)
                .env("GATEWAY_ARTIFACT_ROOT", &fixture.root)
                .args(["poll-dreamina-video-supplement", &plan.authorization_id.to_string()])
                .arg(&cancellation.root)
                .output().await.map_err(debug_error)?;
            require(output.status.success(), format!("cancelled poll failed: {}", String::from_utf8_lossy(&output.stderr)))?;
            let receipt: Value = serde_json::from_slice(&output.stdout).map_err(debug_error)?;
            require(receipt["state"] == "cancelled"
                && receipt["authorization_id"] == plan.authorization_id.to_string()
                && receipt["remote_operation_id"].as_str() == plan.remote_operation_id.as_deref(),
                "cancelled poll returned a different terminal identity")?;
        }
        let after_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM dreamina_operator_video_supplement_observations WHERE authorization_id=$1",
        ).bind(plan.authorization_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(after_count == observation_count, "cancelled poll wrote a provider observation")?;
        let directory = cancellation.root.join(plan.authorization_id.simple().to_string());
        require(fs::read_dir(directory).map_err(debug_error)?.next().is_none(),
            "cancelled poll created query/publication evidence")?;
        require(original_evidence(&db.pool, plan.job_id).await? == before,
            "cancelled poll changed original provider evidence or quote")?;
        assert_released(&db.pool, plan.job_id, 0).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

async fn original_evidence(pool: &PgPool, job: Uuid) -> TestResult<Value> {
    let mut result = serde_json::Map::new();
    for table in [
        "provider_submissions",
        "executor_executions",
        "provider_remote_submit_intents",
        "provider_submit_recoveries",
        "executor_resolution_decisions",
        "executor_terminal_reductions",
        "provider_receipts",
        "customer_price_quotes",
        "customer_price_quote_lines",
        "dreamina_operator_video_supplement_authorizations",
    ] {
        let rows: Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY to_jsonb(item)::TEXT),'[]'::JSONB) FROM {table} item WHERE dreamina_video_supplement_mutation_job(to_jsonb(item))=$1"
        ))).bind(job).fetch_one(pool).await.map_err(debug_error)?;
        result.insert(table.to_owned(), rows);
    }
    Ok(Value::Object(result))
}

async fn money_snapshot(pool: &PgPool, job: Uuid) -> TestResult<Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('hold',to_jsonb(hold),'account',to_jsonb(account),'quota',to_jsonb(quota),'reservations',(SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY grant_reservation_id),'[]'::JSONB) FROM customer_billing_hold_grant_reservations item WHERE item.hold_id=hold.hold_id),'grants',(SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY grant_id),'[]'::JSONB) FROM credit_grants item WHERE grant_id IN (SELECT grant_id FROM customer_billing_hold_grant_reservations WHERE hold_id=hold.hold_id)),'grant_events',(SELECT COALESCE(jsonb_agg(to_jsonb(item) ORDER BY grant_event_id),'[]'::JSONB) FROM credit_grant_events item WHERE item.hold_id=hold.hold_id)) FROM customer_billing_holds hold JOIN billing_accounts account ON account.tenant_id=hold.tenant_id AND account.currency=hold.currency JOIN jobs job ON job.job_id=hold.job_id JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id WHERE job.job_id=$1")
        .bind(job).fetch_one(pool).await.map_err(debug_error)
}

async fn assert_released(pool: &PgPool, job: Uuid, grant: i64) -> TestResult {
    let amount: (i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT held_micros,captured_micros,released_micros,grant_held_micros,grant_released_micros,account_held_micros,account_released_micros FROM customer_billing_holds WHERE job_id=$1"
    ).bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        amount
            == (
                1_200_000,
                0,
                1_200_000,
                grant,
                grant,
                1_200_000 - grant,
                1_200_000 - grant,
            ),
        format!("cancellation changed original quote or lost funding release: {amount:?}"),
    )?;
    let quota: (String, i32, i32, i32) = sqlx::query_as(
        "SELECT quota.state,quota.requested_units,quota.committed_units,quota.released_units FROM jobs job JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id WHERE job.job_id=$1"
    ).bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        quota == ("released".into(), 10, 0, 10),
        "cancellation did not release the original quota once",
    )?;
    let count: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM dreamina_operator_video_supplement_cancellations WHERE job_id=$1),(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1 AND total_amount_micros=0),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),(SELECT count(*) FROM customer_billing_hold_grant_reservations reservation JOIN customer_billing_holds hold USING(hold_id) WHERE hold.job_id=$1 AND (reservation.consumed_micros<>0 OR reservation.released_micros<>reservation.reserved_micros OR reservation.state<>'released'))"
    ).bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        count == (1, 1, 0, 0),
        format!("cancellation duplicated money or did not release grants: {count:?}"),
    )?;
    let raw: (String, String) = sqlx::query_as(
        "SELECT submission.state,receipt.outcome FROM provider_submissions submission JOIN provider_receipts receipt USING(submission_id) WHERE submission.job_id=$1"
    ).bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        raw == ("uncertain".into(), "uncertain".into()),
        "customer cancellation forged a provider terminal outcome",
    )?;
    let parents: (String, String, String, String) = sqlx::query_as(
        "SELECT job.state,work.state,attempt.state,output.state FROM jobs job JOIN work_items work USING(job_id) JOIN job_attempts attempt ON attempt.execution_id=work.execution_id JOIN job_outputs output ON output.job_id=job.job_id WHERE job.job_id=$1"
    ).bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        parents
            == (
                "uncertain".into(),
                "uncertain".into(),
                "uncertain".into(),
                "uncertain".into(),
            ),
        "cancellation rewrote canonical parent states",
    )?;
    let facts: (i64, i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE fact_domain='customer_billable' AND terminal_outcome='failed' AND quantity=0),count(*) FILTER (WHERE fact_domain='provider_cost'),count(*) FILTER (WHERE fact_domain='customer_billable' AND (terminal_outcome<>'failed' OR quantity<>0 OR metadata_json->>'reason' IS DISTINCT FROM 'operator_supplement_cancelled' OR metadata_json->>'original_effect_certainty' IS DISTINCT FROM 'unknown_remote_effect' OR metadata_json->>'customer_charge_waived' IS DISTINCT FROM 'true' OR NOT EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_cancellations cancellation WHERE cancellation.job_id=fact.job_id AND cancellation.cancellation_id::TEXT=fact.metadata_json->>'operator_supplement_cancellation_id'))) FROM provider_usage_facts fact WHERE job_id=$1"
    ).bind(job).fetch_one(pool).await.map_err(debug_error)?;
    require(
        facts.0 > 0 && facts.1 == 0 && facts.2 == 0,
        "cancellation did not use customer-only zero economic facts",
    )
}

#[tokio::test]
async fn supplement_cancel_is_atomic_idempotent_and_preserves_unknown_ancestors() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture_with_funding(&db.pool, "seedance2.0fast_vip", 0, 400_000).await?;
        let service = store(&db.pool);
        let parent = service.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
        service.claim_dreamina_video_supplement_dispatch(parent.authorization_id,parent.dispatch_nonce,&parent.runtime_binding_json).await.map_err(debug_error)?.ok_or("missing unknown parent permit")?;
        service.record_dreamina_video_supplement_uncertain(&Uncertain { authorization_id:parent.authorization_id,dispatch_nonce:parent.dispatch_nonce,error_code:"offline_unknown_effect".into() }).await.map_err(debug_error)?;
        let mut child_request = fixture.request.clone();
        child_request.operator_reference = "offline-new-human-instruction:cancel-head".into();
        let child = service.authorize_dreamina_video_supplement_successor(parent.authorization_id,&child_request).await.map_err(debug_error)?;
        let (child,publish) = accept(&db.pool,&fixture,&child).await?;
        let evidence = cancellation_evidence(&db.pool,&child).await?;
        let unauthorized = sqlx::query("INSERT INTO provider_usage_facts (usage_fact_id,semantic_key,job_id,output_id,submission_id,receipt_id,provider_id,provider_account_id,execution_surface,fact_domain,metric,quantity,unit,quantity_source,confidence,evidence_path,metadata_json,billing_partition_key,terminal_outcome,created_at_ms) SELECT $1,$2,head.job_id,head.output_id,head.submission_id,head.original_receipt_id,'dreamina-cli',head.provider_account_id,'provider_cli','customer_billable','video_requested_second',0,'second','request_derived','exact','dreamina_operator_video_supplement_cancellations.cancellation_id',quote.request_dimensions_json || jsonb_build_object('operation','video_generation','billing_metric','video_second','billing_unit','second','basis','operator_supplement_cancelled','operator_supplement_cancellation_id',$3::TEXT,'reason','operator_supplement_cancelled','original_effect_certainty','unknown_remote_effect','customer_charge_waived',TRUE),'output:' || head.output_id::TEXT,'failed',1 FROM dreamina_operator_video_supplement_authorizations head JOIN customer_billing_holds hold ON hold.hold_id=head.hold_id JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id WHERE head.authorization_id=$4")
            .bind(Uuid::new_v4()).bind(format!("unauthorized:{}",Uuid::new_v4())).bind(Uuid::new_v4().to_string()).bind(child.authorization_id).execute(&db.pool).await.expect_err("customer failed fact without cancellation authority must fail");
        require(unauthorized.as_database_error().is_some_and(|error|error.message().contains("canonical authority")),format!("unauthorized zero fact failed for unrelated reason: {unauthorized:?}"))?;
        let before = original_evidence(&db.pool,child.job_id).await?;
        let mut stale = db.pool.begin().await.map_err(debug_error)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").execute(&mut *stale).await.map_err(debug_error)?;
        sqlx::query("SELECT job_id FROM jobs WHERE job_id=$1").bind(child.job_id).fetch_one(&mut *stale).await.map_err(debug_error)?;
        let (first,second) = tokio::join!(service.confirm_dreamina_supplement_video_cancellation(&evidence.root,&evidence.request),service.confirm_dreamina_supplement_video_cancellation(&evidence.root,&evidence.request));
        let proof = first.map_err(debug_error)?;
        require(proof == second.map_err(debug_error)?, "concurrent cancellation minted two authorities")?;
        require(proof.job_id==child.job_id && proof.upstream_task_id==child.remote_operation_id.clone().ok_or("missing upstream id")? && proof.captured_micros==0 && proof.released_micros==1_200_000,"wrong cancellation proof identity or amount")?;
        require(store(&db.pool).confirm_dreamina_supplement_video_cancellation(&evidence.root,&evidence.request).await.map_err(debug_error)?==proof,"restart changed cancellation proof")?;
        require(matches!(service.video_status("executord-process-smoke",child.job_id).await.map_err(debug_error)?,Some(gpt_image_2_gateway::settlement::VideoResultStatus::Failed { error_code:Some(ref code),.. }) if code=="operator_supplement_cancelled"),"cancelled head kept showing queue progress")?;
        require(matches!(service.project_video_status("executord-process-smoke","project-test",None,child.job_id).await.map_err(debug_error)?,Some(gpt_image_2_gateway::settlement::VideoResultStatus::Failed { .. })),"scoped cancelled task did not project terminal failure")?;
        require(matches!(service.project_video_status_with_metadata("executord-process-smoke","project-test",None,child.job_id).await.map_err(debug_error)?,Some((gpt_image_2_gateway::settlement::VideoResultStatus::Failed { .. },_))),"Ark cancelled task remained running")?;
        assert_released(&db.pool,child.job_id,400_000).await?;
        require(original_evidence(&db.pool,child.job_id).await?==before,"cancellation rewrote original/ancestor provider evidence or frozen quote")?;
        require(sqlx::query("UPDATE jobs SET updated_at_ms=updated_at_ms WHERE job_id=$1").bind(child.job_id).execute(&mut *stale).await.is_err(),"old RR writer escaped terminal cancellation fence")?;
        stale.rollback().await.map_err(debug_error)?;
        for statement in ["UPDATE dreamina_operator_video_supplement_authorizations SET updated_at_ms=updated_at_ms WHERE job_id=$1","UPDATE customer_billing_holds SET updated_at_ms=updated_at_ms WHERE job_id=$1","UPDATE provider_receipts SET created_at_ms=created_at_ms WHERE job_id=$1"] {
            require(sqlx::query(statement).bind(child.job_id).execute(&db.pool).await.is_err(),format!("late mutation escaped cancellation: {statement}"))?;
        }
        require(service.publish_dreamina_video_supplement(&fixture.root,&publish).await.is_err(),"canceled head published late success")?;
        let mut next = child_request.clone(); next.operator_reference="offline-must-not-reopen-canceled-chain".into();
        require(service.authorize_dreamina_video_supplement_successor(child.authorization_id,&next).await.is_err(),"canceled head received successor")?;
        for (tenant,project,actor) in [("other","project-test",None),("executord-process-smoke","other",None),("executord-process-smoke","project-test",Some(Uuid::new_v4()))] {
            require(service.project_dreamina_manual_video_cancellation(tenant,project,actor,child.job_id).await.map_err(debug_error)?.is_none(),"cancellation proof escaped original scope")?;
        }
        require(service.project_dreamina_manual_video_cancellation("executord-process-smoke","project-test",None,child.job_id).await.map_err(debug_error)?==Some(proof),"original task scope lost confirmed head cancellation")?;
        assert_released(&db.pool,child.job_id,400_000).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_cancel_commit_failure_rolls_back_then_recovers_once() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture_with_funding(&db.pool,"seedance2.0fast_vip",0,400_000).await?;
        let service = store(&db.pool);
        let plan = service.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
        let (plan,_) = accept(&db.pool,&fixture,&plan).await?;
        let evidence = cancellation_evidence(&db.pool,&plan).await?;
        let before = original_evidence(&db.pool,plan.job_id).await?;
        let money = money_snapshot(&db.pool,plan.job_id).await?;
        sqlx::query("CREATE FUNCTION cancellation_test_abort() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'offline commit failure'; END; $$ LANGUAGE plpgsql").execute(&db.pool).await.map_err(debug_error)?;
        sqlx::query("CREATE CONSTRAINT TRIGGER cancellation_test_abort AFTER INSERT ON dreamina_operator_video_supplement_cancellations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cancellation_test_abort()").execute(&db.pool).await.map_err(debug_error)?;
        require(service.confirm_dreamina_supplement_video_cancellation(&evidence.root,&evidence.request).await.is_err(),"forced cancellation commit failure was ignored")?;
        sqlx::query("DROP TRIGGER cancellation_test_abort ON dreamina_operator_video_supplement_cancellations").execute(&db.pool).await.map_err(debug_error)?;
        require(original_evidence(&db.pool,plan.job_id).await?==before,"rollback changed unknown original evidence")?;
        assert_pending(&db.pool,plan.job_id).await?;
        require(money_snapshot(&db.pool,plan.job_id).await?==money,"commit rollback changed account, grant or original hold balances")?;
        let counts: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM dreamina_operator_video_supplement_cancellations WHERE job_id=$1),(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1)").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts==(0,0,0),"rolled back cancellation leaked authority, rating or usage")?;
        require(service.project_dreamina_manual_video_cancellation("executord-process-smoke","project-test",None,plan.job_id).await.map_err(debug_error)?.is_none(),"rolled back cancellation leaked proof")?;
        service.confirm_dreamina_supplement_video_cancellation(&evidence.root,&evidence.request).await.map_err(debug_error)?;
        for statement in ["DELETE FROM dreamina_operator_video_supplement_cancellations WHERE job_id=$1","UPDATE dreamina_operator_video_supplement_cancellations SET operator_reference=operator_reference WHERE job_id=$1"] {
            require(sqlx::query(statement).bind(plan.job_id).execute(&db.pool).await.is_err(),"confirmed cancellation authority was mutable")?;
        }
        require(sqlx::query("TRUNCATE dreamina_operator_video_supplement_cancellations").execute(&db.pool).await.is_err(),"confirmed cancellation authority could be truncated")?;
        assert_released(&db.pool,plan.job_id,400_000).await
    }.await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_cancel_requires_accepted_head_and_exact_same_account_evidence() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&db.pool, "seedance2.0fast_vip").await?;
        let service = store(&db.pool);
        let plan = service
            .authorize_dreamina_video_supplement(&fixture.request)
            .await
            .map_err(debug_error)?;
        let (accepted, _) = evidence(&fixture, &plan)?;
        let mut pretend = plan.clone();
        pretend.remote_operation_id = Some(accepted.remote_operation_id.clone());
        pretend.submit_stdout_sha256 = Some(accepted.submit_stdout_sha256.clone());
        let before = cancellation_evidence(&db.pool, &pretend).await?;
        require(
            service
                .confirm_dreamina_supplement_video_cancellation(&before.root, &before.request)
                .await
                .is_err(),
            "unsubmitted head acquired cancellation settlement",
        )?;
        let (plan, _) = accept(&db.pool, &fixture, &plan).await?;
        let evidence = cancellation_evidence(&db.pool, &plan).await?;
        for field in ["authorization_id", "dispatch_nonce"] {
            let mut changed: Value = serde_json::from_slice(
                &fs::read(evidence.root.join("head-review.json")).map_err(debug_error)?,
            )
            .map_err(debug_error)?;
            let original = changed[field].clone();
            changed[field] = json!(Uuid::new_v4());
            write(
                &evidence.root,
                "head-review.json",
                &serde_json::to_vec(&changed).map_err(debug_error)?,
            )?;
            require(
                service
                    .confirm_dreamina_supplement_video_cancellation(
                        &evidence.root,
                        &evidence.request,
                    )
                    .await
                    .is_err(),
                "cross-head cancellation review was accepted",
            )?;
            changed[field] = original;
            write(
                &evidence.root,
                "head-review.json",
                &serde_json::to_vec(&changed).map_err(debug_error)?,
            )?;
        }
        let mut other_account = plan.clone();
        other_account.provider_account_id = Uuid::new_v4();
        let wrong = cancellation_evidence(&db.pool, &other_account).await?;
        require(
            service
                .confirm_dreamina_supplement_video_cancellation(&wrong.root, &wrong.request)
                .await
                .is_err(),
            "other account screenshot/audit authorized original customer release",
        )?;
        let mut wrong_nonce = evidence.request.clone();
        wrong_nonce.dispatch_nonce = Uuid::new_v4();
        require(
            service
                .confirm_dreamina_supplement_video_cancellation(&evidence.root, &wrong_nonce)
                .await
                .is_err(),
            "wrong cancellation dispatch nonce succeeded",
        )?;
        assert_pending(&db.pool, plan.job_id).await?;
        service
            .confirm_dreamina_supplement_video_cancellation(&evidence.root, &evidence.request)
            .await
            .map_err(debug_error)?;
        assert_released(&db.pool, plan.job_id, 0).await
    }
    .await;
    let cleanup = db.cleanup().await;
    result.and(cleanup)
}

#[tokio::test]
async fn supplement_cancel_and_success_share_one_customer_economic_authority() -> TestResult {
    for success_first in [true, false] {
        let Some(db) = TestDatabase::new().await? else {
            return Ok(());
        };
        let result = async {
            let fixture = fixture(&db.pool,"seedance2.0fast_vip").await?;
            let service = store(&db.pool);
            let plan = service.authorize_dreamina_video_supplement(&fixture.request).await.map_err(debug_error)?;
            let (plan,publish) = accept(&db.pool,&fixture,&plan).await?;
            let evidence = cancellation_evidence(&db.pool,&plan).await?;
            if success_first {
                service.publish_dreamina_video_supplement(&fixture.root,&publish).await.map_err(debug_error)?;
                require(service.confirm_dreamina_supplement_video_cancellation(&evidence.root,&evidence.request).await.is_err(),"already successful nonzero capture was canceled/refunded")?;
            } else {
                let (canceled,succeeded)=tokio::join!(service.confirm_dreamina_supplement_video_cancellation(&evidence.root,&evidence.request),service.publish_dreamina_video_supplement(&fixture.root,&publish));
                require(usize::from(canceled.is_ok())+usize::from(succeeded.is_ok())==1,"cancellation and success both won or both failed")?;
            }
            let row:(i64,i64,i64,i64,i64,i64)=sqlx::query_as("SELECT hold.captured_micros,hold.released_micros,(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM dreamina_operator_video_supplement_cancellations WHERE job_id=$1),(SELECT count(*) FROM dreamina_operator_video_supplement_resolutions WHERE job_id=$1),(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1) FROM customer_billing_holds hold WHERE job_id=$1").bind(plan.job_id).fetch_one(&db.pool).await.map_err(debug_error)?;
            require(row==(0,1_200_000,1,1,0,0) || row==(1_200_000,0,1,0,1,1),format!("customer settlement had two authorities or inconsistent charge/release: {row:?}"))?;
            Ok(())
        }.await;
        let cleanup = db.cleanup().await;
        result.and(cleanup)?;
    }
    Ok(())
}
