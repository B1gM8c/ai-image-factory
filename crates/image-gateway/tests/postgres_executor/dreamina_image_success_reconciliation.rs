use super::*;
use gpt_image_2_gateway::{DreaminaImageSuccessResolutionRequest, resolve_dreamina_image_success};
use image_provider_dreamina_cli::parse_submit_command;
use std::path::{Path, PathBuf};

const PRICE: i64 = 30_000;
// Exercise the database evidence fence independently of the Rust preflight.
const ORIGINAL_EVIDENCE_ASSERTION: &str = r#"SELECT assert_operator_dreamina_image_success_evidence(
    jsonb_populate_record(NULL::dreamina_operator_image_success_resolutions, jsonb_build_object(
        'job_id',submission.job_id,'submission_id',submission.submission_id,
        'executor_execution_id',submission.executor_execution_id,'output_id',submission.output_id,
        'original_receipt_id',receipt.receipt_id,'hold_id',hold.hold_id,
        'remote_operation_id',task.remote_operation_id,'command_hash',submission.command_hash,
        'execution_binding_sha256',intent.execution_binding_sha256,'quote_hash',quote.quote_hash,
        'created_at_ms',receipt.created_at_ms)))
    FROM provider_submissions submission
    JOIN executor_terminal_reductions reduction USING(submission_id,executor_execution_id)
    JOIN provider_receipts receipt ON receipt.receipt_id=reduction.provider_receipt_id
    JOIN provider_remote_submit_intents intent ON intent.submission_id=submission.submission_id
        AND intent.executor_execution_id=submission.executor_execution_id
    JOIN provider_remote_tasks task ON task.submission_id=submission.submission_id
        AND task.executor_execution_id=submission.executor_execution_id
    JOIN customer_billing_holds hold ON hold.job_id=submission.job_id
    JOIN customer_price_quotes quote ON quote.quote_id=hold.quote_id
    WHERE submission.job_id=$1"#;

struct HistoricalFourImageFailure(&'static str);
impl ProviderPollDriver for HistoricalFourImageFailure {
    fn provider_id(&self) -> &'static str {
        DREAMINA_PROVIDER_ID
    }
    async fn poll<S: ArtifactSink>(
        &self,
        _: &ProviderPollDriverCall,
        _: &mut S,
    ) -> Result<PollObservation, ProviderFailure> {
        Err(ProviderFailure::new(
            ProviderFailureClass::Permanent,
            self.0,
            EffectCertainty::UnknownRemoteEffect,
            RetryDirective::Never,
        )
        .expect("static fixture failure"))
    }
}

pub(super) struct Fixture {
    _evidence: tempfile::TempDir,
    pub(super) evidence_root: PathBuf,
    _artifacts: tempfile::TempDir,
    pub(super) blobs: Arc<FilesystemArtifactBlobStore>,
    pub(super) request: DreaminaImageSuccessResolutionRequest,
}

pub(super) fn write_private(root: &Path, name: &str, bytes: &[u8]) -> TestResult {
    fs::write(root.join(name), bytes).map_err(debug_error)?;
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o600)).map_err(debug_error)
}

async fn fixture(pool: &PgPool) -> TestResult<Fixture> {
    fixture_with_funding(pool, 0).await
}

async fn fixture_with_funding(pool: &PgPool, grant_micros: i64) -> TestResult<Fixture> {
    fixture_with_quote_dimensions(
        pool,
        grant_micros,
        json!({"resolution_type":"2k","ratio":"1:1","processing_mode":"synchronous"}),
    )
    .await
}

async fn fixture_with_quote_dimensions(
    pool: &PgPool,
    grant_micros: i64,
    dimensions: serde_json::Value,
) -> TestResult<Fixture> {
    fixture_with_mode(pool, grant_micros, dimensions, false).await
}

pub(super) async fn fixture_with_mode(
    pool: &PgPool,
    grant_micros: i64,
    dimensions: serde_json::Value,
    protocol_mode: bool,
) -> TestResult<Fixture> {
    let quote_max_micros = if dimensions
        .get("processing_mode")
        .and_then(serde_json::Value::as_str)
        == Some("batch")
    {
        PRICE / 2
    } else {
        PRICE
    };
    let work = seed_dreamina_generation_lease_with_model(
        pool,
        "operator-image-resolution-worker",
        DREAMINA_IMAGES_API_PROFILE,
        1,
        if protocol_mode { "5.0Pro" } else { "5.0" },
    )
    .await?;
    seed_v4_customer_quote_with_funding(
        pool,
        &work,
        V4CustomerQuoteIdentity {
            dimensions,
            provider_model_id: if protocol_mode { "5.0Pro" } else { "5.0" },
            ..V4CustomerQuoteIdentity::dreamina()
        },
        V4CustomerQuoteBasis {
            metric: "image_output",
            unit: "image",
            unit_size: 1,
            unit_price_micros: PRICE,
            quantity_source: "request_derived",
            confidence: "exact",
            max_quantity: 1,
            max_amount_micros: quote_max_micros,
        },
        grant_micros,
    )
    .await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let prepared = executor
        .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    require(prepared.len() == 1, "one original output required")?;
    seed_terminal_quota(pool, &work).await?;
    let scope = ExecutorClaimScope {
        execution_profile_id: DREAMINA_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    };
    let lease = executor
        .claim_prepared(&scope, "operator-image-resolution-executor", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("missing prepared lease")?;
    executor.start(&lease).await.map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(0, 1).map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new(
            &lease.command_hash,
            parse_submit_command(&serde_json::to_vec(&work.command_json).map_err(debug_error)?)
                .map_err(debug_error)?,
        )
        .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("original-submit-{}", lease.submission_id.simple()),
        command.output(),
        command.identity(),
        60_000,
    );
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    tasks
        .reserve_submit(&reservation)
        .await
        .map_err(debug_error)?;
    tasks
        .start_submit(&reservation)
        .await
        .map_err(debug_error)?;
    let binding = tasks
        .load_submit_intent(lease.submission_id)
        .await
        .map_err(debug_error)?
        .ok_or("missing original intent")?
        .execution_binding_sha256;
    let remote_operation_id = format!("original-image-{}", work.job_id.simple());
    tasks
        .record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: remote_operation_id.clone(),
            provider_request_id: None,
            event_identity: "original-image-submit-receipt".into(),
            execution_binding_sha256: binding.clone(),
        })
        .await
        .map_err(debug_error)?;
    tasks
        .attach(&RemoteTaskAttach {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: remote_operation_id.clone(),
            provider_request_id: None,
            event_identity: "original-image-attach".into(),
            execution_binding_sha256: binding,
            poll_after_ms: 0,
            recovery_fence: None,
        })
        .await
        .map_err(debug_error)?;
    let artifacts = tempfile::tempdir().map_err(debug_error)?;
    let blobs = Arc::new(FilesystemArtifactBlobStore::new(artifacts.path()).map_err(debug_error)?);
    let poller = ProviderPollOrchestrator::new(
        tasks,
        HistoricalFourImageFailure(if protocol_mode {
            "dreamina_poll_protocol_invalid"
        } else {
            "dreamina_poll_artifact_invalid"
        }),
        gpt_image_2_gateway::FilesystemProviderArtifactStagerFactory::new(
            Arc::clone(&blobs),
            1024 * 1024,
        )
        .map_err(debug_error)?,
        ProviderPollOrchestratorConfig {
            scope: ProviderTaskClaimScope {
                provider_id: DREAMINA_PROVIDER_ID.into(),
                provider_account_id: DREAMINA_ACCOUNT_ID,
                execution_profile_id: DREAMINA_PROFILE_ID,
            },
            owner: "historical-four-image-poller".into(),
            lease_ms: 5_000,
            heartbeat_interval: Duration::from_secs(1),
            max_materializations: 1,
        },
    )
    .map_err(debug_error)?;
    require(
        matches!(poller.run_once().await.map_err(debug_error)?, ProviderPollRun::Observed(ref task) if task.state == ProviderTaskState::Uncertain),
        "original poll must become uncertain",
    )?;
    let reductions = PostgresExecutorTerminalStore::new(pool.clone());
    let terminal = reductions
        .claim_terminal("original-image-uncertain-reducer", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("missing uncertain reduction")?;
    let completed = reductions
        .complete_terminal(&terminal, None)
        .await
        .map_err(debug_error)?;
    require(
        completed.parent_state == ExecutorParentTerminalState::Uncertain,
        "original reducer must preserve uncertain",
    )?;
    // Reproduce a pre-147 terminal order, then upgrade through the real migration.
    // New post-147 acceptances get a budget and are outside this narrow recovery.
    run_migrations(pool).await.map_err(debug_error)?;
    let budget_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM accepted_completion_budgets WHERE submission_id=$1",
    )
    .bind(lease.submission_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        budget_count == 0,
        "migration reopened a terminal historical image",
    )?;
    let evidence = tempfile::tempdir().map_err(debug_error)?;
    fs::set_permissions(evidence.path(), fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    let evidence_root = fs::canonicalize(evidence.path()).map_err(debug_error)?;
    let result_count = if protocol_mode { 1 } else { 4 };
    let images = (0..result_count).map(|index| json!({"item_id":format!("original-item-{index}"),"image_url":format!("https://media.example.test/original-{index}.png"),"width":1,"height":1})).collect::<Vec<_>>();
    let result_json = json!({"images":images,"videos":[]});
    let stdout = serde_json::to_vec(&json!({"submit_id":remote_operation_id,"gen_status":"success","result_json":if protocol_mode { result_json } else { json!(serde_json::to_string(&result_json).map_err(debug_error)?) }})).map_err(debug_error)?;
    let request = DreaminaImageSuccessResolutionRequest {
        job_id: work.job_id,
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        reviewed_stdout_sha256: sha256(&stdout),
        operator_reference: "offline:original-four-images-reviewed".into(),
    };
    let selected = png_bytes([10, 20, 30, 255]);
    let mut audit_value = json!({"schema_version":1,"job_id":work.job_id,"submission_id":lease.submission_id,"executor_execution_id":lease.executor_execution_id,
        "remote_operation_id":remote_operation_id,"argv":["query_result","--submit_id",remote_operation_id,"--download_dir","/existing/original-download"],"stdout_sha256":request.reviewed_stdout_sha256,"exit_code":0,"executed_at_ms":database_now(pool).await?,"downloaded_images":(0..4).map(|i|json!({"source_index":i,"filename":format!("{}_image_{}.png",remote_operation_id,i+1),"sha256":sha256(&selected),"byte_size":selected.len()})).collect::<Vec<_>>()});
    if protocol_mode {
        audit_value["schema_version"] = json!(2);
        audit_value["evidence_mode"] = json!("query_only_http_retrieval_v2");
        audit_value["argv"] = json!(["query_result", "--submit_id", remote_operation_id]);
        audit_value["downloaded_images"] = json!([]);
    }
    let audit = serde_json::to_vec(&audit_value).map_err(debug_error)?;
    let quote_hash: String =
        sqlx::query_scalar("SELECT quote_hash FROM customer_price_quotes WHERE job_id=$1")
            .bind(work.job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let mut review = json!({"schema_version":1,"stdout_sha256":request.reviewed_stdout_sha256,"execution_audit_sha256":sha256(&audit),
        "selected_sha256":sha256(&selected),"selected_source_index":0,"selected_item_id":"original-item-0","quote_hash":quote_hash,"provider_command_sha256":lease.command_hash});
    if protocol_mode {
        let url_sha256 = sha256(b"https://media.example.test/original-0.png");
        let retrieval = serde_json::to_vec(&json!({"schema_version":2,"evidence_mode":"query_only_http_retrieval_v2","job_id":work.job_id,"submission_id":lease.submission_id,
            "executor_execution_id":lease.executor_execution_id,"remote_operation_id":remote_operation_id,"source_index":0,"item_id":"original-item-0","image_url_sha256":url_sha256,
            "method":"GET","http_status":200,"redirect_count":0,"retrieved_at_ms":database_now(pool).await?,"media_type":"image/png","sha256":sha256(&selected),"byte_size":selected.len(),"width":1,"height":1})).map_err(debug_error)?;
        let attribution: serde_json::Value =
            sqlx::query_scalar("SELECT to_jsonb(a) FROM job_auth_attributions a WHERE job_id=$1")
                .bind(work.job_id)
                .fetch_one(pool)
                .await
                .map_err(debug_error)?;
        review["schema_version"] = json!(2);
        review["evidence_mode"] = json!("query_only_http_retrieval_v2");
        review["tenant_id"] = attribution["tenant_id"].clone();
        review["auth_attribution"] = attribution;
        review["retrieval_audit_sha256"] = json!(sha256(&retrieval));
        review["image_url_sha256"] = json!(url_sha256);
        write_private(&evidence_root, "retrieval-audit.json", &retrieval)?;
    }
    let review = serde_json::to_vec(&review).map_err(debug_error)?;
    write_private(&evidence_root, "reviewed-query.json", &stdout)?;
    write_private(&evidence_root, "execution-audit.json", &audit)?;
    write_private(&evidence_root, "review.json", &review)?;
    write_private(&evidence_root, "selected.png", &selected)?;
    Ok(Fixture {
        _evidence: evidence,
        evidence_root,
        _artifacts: artifacts,
        blobs,
        request,
    })
}

pub(super) async fn immutable_snapshot(pool: &PgPool, job_id: Uuid) -> TestResult<String> {
    let value: serde_json::Value = sqlx::query_scalar(r#"SELECT jsonb_build_object(
        'quotes',(SELECT jsonb_agg(to_jsonb(t) ORDER BY quote_id) FROM customer_price_quotes t WHERE job_id=$1),
        'submissions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM provider_submissions t WHERE job_id=$1),
        'executions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY executor_execution_id) FROM executor_executions t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'decisions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY decision_id) FROM executor_resolution_decisions t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'reductions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM executor_terminal_reductions t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'receipts',(SELECT jsonb_agg(to_jsonb(t) ORDER BY receipt_id) FROM provider_receipts t WHERE job_id=$1),
        'tasks',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM provider_remote_tasks t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'intents',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM provider_remote_submit_intents t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'allocations',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM executor_capacity_allocations t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)))"#)
        .bind(job_id).fetch_one(pool).await.map_err(debug_error)?;
    Ok(sha256(&serde_json::to_vec(&value).map_err(debug_error)?))
}

pub(super) async fn rollback_snapshot(
    pool: &PgPool,
    job_id: Uuid,
) -> TestResult<serde_json::Value> {
    sqlx::query_scalar(r#"SELECT jsonb_build_object('job',to_jsonb(job),'quota',to_jsonb(quota),'hold',to_jsonb(hold),'account',to_jsonb(account),
        'overlay',(SELECT count(*) FROM dreamina_operator_image_success_resolutions WHERE job_id=$1),
        'artifact',(SELECT count(*) FROM artifacts WHERE job_id=$1),'projection',(SELECT count(*) FROM job_response_projections WHERE job_id=$1),
        'retention',(SELECT count(*) FROM job_artifact_retention WHERE job_id=$1),'usage',(SELECT count(*) FROM usage_events WHERE job_id=$1),
        'facts',(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),'rating',(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),
        'ledger',(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),'events',(SELECT count(*) FROM job_events WHERE job_id=$1),
        'outbox',(SELECT count(*) FROM outbox_events WHERE job_id=$1)) FROM jobs job JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id
        JOIN customer_billing_holds hold ON hold.job_id=job.job_id JOIN billing_accounts account ON account.tenant_id=hold.tenant_id AND account.currency=hold.currency WHERE job.job_id=$1"#)
        .bind(job_id).fetch_one(pool).await.map_err(debug_error)
}

#[tokio::test]
async fn original_four_image_success_resolves_once_and_preserves_canonical_evidence() -> TestResult
{
    let Some(database) =
        super::accepted_image_completion_budget::database_before_image_completion_budget().await?
    else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&database.pool).await?;
        let before=immutable_snapshot(&database.pool,fixture.request.job_id).await?;
        sqlx::query(ORIGINAL_EVIDENCE_ASSERTION).bind(fixture.request.job_id)
            .execute(&database.pool).await.map_err(debug_error)?;
        let (a,b)=tokio::join!(
            resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request),
            resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request));
        let a=a.map_err(debug_error)?;let b=b.map_err(debug_error)?;
        require(a==b && a.captured_micros==PRICE && a.released_micros==0 && a.committed_units==1 && a.released_units==0,"concurrent original resolution changed identity or frozen one-image price")?;
        require(before==immutable_snapshot(&database.pool,fixture.request.job_id).await?,"operator resolution mutated canonical execution evidence")?;
        let state:(i64,i64,i64,i64,i64,i64,i64)=sqlx::query_as(r#"SELECT
            (SELECT count(*) FROM artifacts WHERE job_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),
            (SELECT sum(quantity)::BIGINT FROM provider_usage_facts WHERE job_id=$1),
            (SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM provider_receipts WHERE job_id=$1),
            (SELECT count(*) FROM executor_result_manifests WHERE submission_id=$2),
            (SELECT COALESCE(sum(posting.amount_micros),0)::BIGINT FROM ledger_postings posting JOIN ledger_transactions transaction USING(transaction_id) WHERE transaction.source_job_id=$1)"#)
            .bind(fixture.request.job_id).bind(fixture.request.submission_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        require(state==(1,1,1,1,1,0,0),format!("resolution ledger/delivery count drifted: {state:?}"))?;
        let mut changed=fixture.request.clone();changed.operator_reference.push_str("-changed");
        require(resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&changed).await.is_err(),"changed input was accepted as replay")?;
        let settlement=PostgresExecutionSettlementStore::new(database.pool.clone(),fixture.blobs.clone());
        require(matches!(settlement.generation_status(fixture.request.job_id).await.map_err(debug_error)?,gpt_image_2_gateway::GenerationResultStatus::Succeeded(ref result) if result.images.len()==1),"customer cannot read the single resolved image")
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn original_image_resolution_requires_exact_synchronous_frozen_dimensions() -> TestResult {
    for dimensions in [
        json!({"resolution_type":"2k","ratio":"1:1"}),
        json!({"resolution_type":"2k","ratio":"1:1","processing_mode":"batch"}),
        json!({"resolution_type":"2k","ratio":"1:1","processing_mode":"unknown"}),
        json!({"resolution_type":"2k","ratio":"1:1","processing_mode":"synchronous","extra":"unexpected"}),
    ] {
        let Some(database) =
            super::accepted_image_completion_budget::database_before_image_completion_budget()
                .await?
        else {
            return Ok(());
        };
        let result = async {
            let fixture = fixture_with_quote_dimensions(&database.pool, 0, dimensions).await?;
            let before = rollback_snapshot(&database.pool, fixture.request.job_id).await?;
            let frozen = immutable_snapshot(&database.pool, fixture.request.job_id).await?;
            require(
                sqlx::query(ORIGINAL_EVIDENCE_ASSERTION)
                    .bind(fixture.request.job_id)
                    .execute(&database.pool)
                    .await
                    .is_err(),
                "database accepted incompatible frozen image dimensions",
            )?;
            require(
                resolve_dreamina_image_success(
                    &database.pool,
                    fixture.blobs.as_ref(),
                    &fixture.evidence_root,
                    &fixture.request,
                )
                .await
                .is_err(),
                "operator accepted incompatible frozen image dimensions",
            )?;
            require(
                before == rollback_snapshot(&database.pool, fixture.request.job_id).await?
                    && frozen == immutable_snapshot(&database.pool, fixture.request.job_id).await?,
                "rejected image dimensions changed frozen quote, funds or evidence",
            )
        }
        .await;
        combine(result, database.cleanup().await)?;
    }
    Ok(())
}

#[tokio::test]
async fn original_image_resolution_rolls_back_all_database_effects_on_each_failure() -> TestResult {
    let Some(database) =
        super::accepted_image_completion_budget::database_before_image_completion_budget().await?
    else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&database.pool).await?;
        let before=rollback_snapshot(&database.pool,fixture.request.job_id).await?;
        sqlx::query("CREATE FUNCTION reject_operator_image_fixture() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'injected operator reconciliation failure'; END; $$ LANGUAGE plpgsql").execute(&database.pool).await.map_err(debug_error)?;
        for (table,deferred) in [("artifacts",false),("job_response_projections",false),("provider_usage_facts",false),("customer_rated_usage",false),("ledger_postings",false),("dreamina_operator_image_success_resolutions",true)] {
            let trigger=if deferred {format!("CREATE CONSTRAINT TRIGGER reject_operator_image_fixture AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_operator_image_fixture()")}
                else {format!("CREATE TRIGGER reject_operator_image_fixture BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION reject_operator_image_fixture()")};
            sqlx::query(AssertSqlSafe(trigger)).execute(&database.pool).await.map_err(debug_error)?;
            let resolved=resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request).await;
            sqlx::query(AssertSqlSafe(format!("DROP TRIGGER reject_operator_image_fixture ON {table}"))).execute(&database.pool).await.map_err(debug_error)?;
            require(resolved.is_err(),format!("injected {table} failure was ignored"))?;
            require(before==rollback_snapshot(&database.pool,fixture.request.job_id).await?,format!("injected {table} failure leaked database effects"))?;
        }
        let result=resolve_dreamina_image_success(&database.pool,fixture.blobs.as_ref(),&fixture.evidence_root,&fixture.request).await.map_err(debug_error)?;
        require(result.captured_micros==PRICE,"successful retry drifted after database rollback")
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn original_image_grant_and_mixed_funding_are_atomic_and_settle_once() -> TestResult {
    for grant_micros in [PRICE, PRICE / 2] {
        let Some(database) =
            super::accepted_image_completion_budget::database_before_image_completion_budget()
                .await?
        else {
            return Ok(());
        };
        let result = async {
            let fixture = fixture_with_funding(&database.pool, grant_micros).await?;
            let before = rollback_snapshot(&database.pool, fixture.request.job_id).await?;
            let funding_before = operator_funding_snapshot(&database.pool, fixture.request.job_id).await?;
            let canonical = immutable_snapshot(&database.pool, fixture.request.job_id).await?;
            sqlx::query("CREATE FUNCTION reject_image_funding_commit() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'injected image grant commit failure'; END; $$ LANGUAGE plpgsql")
                .execute(&database.pool).await.map_err(debug_error)?;
            sqlx::query("CREATE CONSTRAINT TRIGGER reject_image_funding_commit AFTER INSERT ON dreamina_operator_image_success_resolutions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_image_funding_commit()")
                .execute(&database.pool).await.map_err(debug_error)?;
            let failed = resolve_dreamina_image_success(&database.pool, fixture.blobs.as_ref(), &fixture.evidence_root, &fixture.request).await;
            sqlx::query("DROP TRIGGER reject_image_funding_commit ON dreamina_operator_image_success_resolutions")
                .execute(&database.pool).await.map_err(debug_error)?;
            require(failed.is_err(), "image grant commit failure was ignored")?;
            require(before == rollback_snapshot(&database.pool, fixture.request.job_id).await?
                && funding_before == operator_funding_snapshot(&database.pool, fixture.request.job_id).await?, "image rollback changed grant/account/ledger state")?;
            let (first, concurrent) = tokio::join!(
                resolve_dreamina_image_success(&database.pool, fixture.blobs.as_ref(), &fixture.evidence_root, &fixture.request),
                resolve_dreamina_image_success(&database.pool, fixture.blobs.as_ref(), &fixture.evidence_root, &fixture.request)
            );
            let first = first.map_err(debug_error)?;
            require(first == concurrent.map_err(debug_error)? && first.captured_micros == PRICE && first.committed_units == 1 && first.released_micros == 0, "image grant replay changed frozen price or identity")?;
            assert_operator_funding_settled(&database.pool, fixture.request.job_id, PRICE, grant_micros).await?;
            let after = operator_funding_snapshot(&database.pool, fixture.request.job_id).await?;
            require(first == resolve_dreamina_image_success(&database.pool, fixture.blobs.as_ref(), &fixture.evidence_root, &fixture.request).await.map_err(debug_error)?
                && after == operator_funding_snapshot(&database.pool, fixture.request.job_id).await?, "image grant replay repeated consumption")?;
            require(canonical == immutable_snapshot(&database.pool, fixture.request.job_id).await?, "image grant recovery mutated original execution")
        }.await;
        combine(result, database.cleanup().await)?;
    }
    Ok(())
}

#[tokio::test]
async fn original_image_resolution_rejects_modified_or_unprotected_evidence() -> TestResult {
    let Some(database) =
        super::accepted_image_completion_budget::database_before_image_completion_budget().await?
    else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&database.pool).await?;
        let before = rollback_snapshot(&database.pool, fixture.request.job_id).await?;
        for (name, key, value) in [
            ("review.json", "selected_source_index", json!(1)),
            ("review.json", "selected_item_id", json!("original-item-1")),
            ("review.json", "quote_hash", json!("b".repeat(64))),
            (
                "review.json",
                "provider_command_sha256",
                json!("b".repeat(64)),
            ),
            (
                "execution-audit.json",
                "argv",
                json!(["text2image", "--prompt", "forbidden"]),
            ),
        ] {
            let original = fs::read(fixture.evidence_root.join(name)).map_err(debug_error)?;
            let mut changed: serde_json::Value =
                serde_json::from_slice(&original).map_err(debug_error)?;
            changed[key] = value;
            write_private(
                &fixture.evidence_root,
                name,
                &serde_json::to_vec(&changed).map_err(debug_error)?,
            )?;
            require(
                resolve_dreamina_image_success(
                    &database.pool,
                    fixture.blobs.as_ref(),
                    &fixture.evidence_root,
                    &fixture.request,
                )
                .await
                .is_err(),
                format!("modified {key} accepted"),
            )?;
            write_private(&fixture.evidence_root, name, &original)?;
        }
        let original = fs::read(fixture.evidence_root.join("selected.png")).map_err(debug_error)?;
        write_private(&fixture.evidence_root, "selected.png", b"not a PNG")?;
        require(
            resolve_dreamina_image_success(
                &database.pool,
                fixture.blobs.as_ref(),
                &fixture.evidence_root,
                &fixture.request,
            )
            .await
            .is_err(),
            "invalid image accepted",
        )?;
        write_private(&fixture.evidence_root, "selected.png", &original)?;
        fs::set_permissions(
            fixture.evidence_root.join("selected.png"),
            fs::Permissions::from_mode(0o644),
        )
        .map_err(debug_error)?;
        require(
            resolve_dreamina_image_success(
                &database.pool,
                fixture.blobs.as_ref(),
                &fixture.evidence_root,
                &fixture.request,
            )
            .await
            .is_err(),
            "unprotected image accepted",
        )?;
        require(
            before == rollback_snapshot(&database.pool, fixture.request.job_id).await?,
            "bad evidence changed database",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn original_image_success_dependency_mutations_are_rejected_and_rolled_back() -> TestResult {
    let Some(database) =
        super::accepted_image_completion_budget::database_before_image_completion_budget().await?
    else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&database.pool).await?;
        resolve_dreamina_image_success(
            &database.pool,
            fixture.blobs.as_ref(),
            &fixture.evidence_root,
            &fixture.request,
        )
        .await
        .map_err(debug_error)?;
        let job_id = fixture.request.job_id;
        let canonical_before = immutable_snapshot(&database.pool, job_id).await?;
        let settlement_before = rollback_snapshot(&database.pool, job_id).await?;
        let dependency_snapshot_sql = r#"SELECT jsonb_build_object(
            'payload',(SELECT to_jsonb(payload) FROM job_payloads payload WHERE job_id=$1),
            'attempts',(SELECT jsonb_agg(to_jsonb(attempt) ORDER BY attempt_id)
                FROM job_attempts attempt JOIN work_items work USING(work_item_id) WHERE work.job_id=$1),
            'usage',(SELECT jsonb_agg(to_jsonb(usage) ORDER BY event_id) FROM usage_events usage WHERE job_id=$1),
            'metering',(SELECT jsonb_agg(to_jsonb(meter) ORDER BY event_id) FROM metering_events meter WHERE job_id=$1))"#;
        let dependencies_before: serde_json::Value = sqlx::query_scalar(dependency_snapshot_sql)
            .bind(job_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        for (label, mutation) in [
            (
                "reviewed payload update",
                r#"UPDATE job_payloads SET command_json=jsonb_set(command_json,'{prompt}','"changed after review"'::JSONB) WHERE job_id=$1"#,
            ),
            (
                "reviewed payload deletion",
                "DELETE FROM job_payloads WHERE job_id=$1",
            ),
            (
                "second parent attempt insertion",
                r#"INSERT INTO job_attempts
                    (attempt_id,execution_id,work_item_id,lease_epoch,worker_id,state,
                     started_at_ms,finished_at_ms,error_code,created_at_ms,updated_at_ms)
                    SELECT $2,$3,attempt.work_item_id,attempt.lease_epoch+1,attempt.worker_id,
                           attempt.state,attempt.started_at_ms,attempt.finished_at_ms,
                           attempt.error_code,attempt.created_at_ms,attempt.updated_at_ms
                    FROM job_attempts attempt JOIN work_items work USING(work_item_id)
                    WHERE work.job_id=$1"#,
            ),
            (
                "charged usage moved away from resolved job",
                "UPDATE usage_events SET job_id=NULL WHERE job_id=$1",
            ),
            (
                "operator meter moved away from resolved job",
                "UPDATE metering_events SET job_id=NULL WHERE job_id=$1 AND event_type='operator_image_success'",
            ),
        ] {
            let mut tx = database.pool.begin().await.map_err(debug_error)?;
            let mutation_result = if label == "second parent attempt insertion" {
                sqlx::query(mutation)
                    .bind(job_id)
                    .bind(Uuid::new_v4())
                    .bind(Uuid::new_v4())
                    .execute(&mut *tx)
                    .await
            } else {
                sqlx::query(mutation)
                    .bind(job_id)
                    .execute(&mut *tx)
                    .await
            };
            let rejected = match mutation_result {
                Err(_) => true,
                Ok(changed) => {
                    require(changed.rows_affected() == 1, format!("{label} missed its target"))?;
                    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
                        .execute(&mut *tx)
                        .await
                        .is_err()
                }
            };
            tx.rollback().await.map_err(debug_error)?;
            require(rejected, format!("{label} bypassed the image success guard"))?;
            require(
                canonical_before == immutable_snapshot(&database.pool, job_id).await?,
                format!("{label} changed immutable execution evidence"),
            )?;
            require(
                settlement_before == rollback_snapshot(&database.pool, job_id).await?,
                format!("{label} leaked settlement changes"),
            )?;
            let dependencies_after: serde_json::Value = sqlx::query_scalar(dependency_snapshot_sql)
                .bind(job_id)
                .fetch_one(&database.pool)
                .await
                .map_err(debug_error)?;
            require(
                dependencies_before == dependencies_after,
                format!("{label} leaked dependency changes"),
            )?;
            sqlx::query("SELECT assert_operator_dreamina_image_success($1)")
                .bind(job_id)
                .execute(&database.pool)
                .await
                .map_err(debug_error)?;
        }
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}
