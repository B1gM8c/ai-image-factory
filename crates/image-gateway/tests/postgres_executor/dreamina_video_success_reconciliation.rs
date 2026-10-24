use super::*;
use gpt_image_2_gateway::{DreaminaVideoSuccessResolutionRequest, resolve_dreamina_video_success};
use image_provider_dreamina_cli::parse_submit_command;
use std::path::{Path, PathBuf};

const PRICE: i64 = 1_200_000;
const VIDEO_URL: &str = "https://media.example.test/original%2Fvideo.mp4?token=A%2Bb";

struct HistoricalVideoProtocolFailure(&'static str);
impl ProviderPollDriver for HistoricalVideoProtocolFailure {
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

struct Fixture {
    _evidence: tempfile::TempDir,
    evidence_root: PathBuf,
    _artifacts: tempfile::TempDir,
    request: DreaminaVideoSuccessResolutionRequest,
}

fn write_private(root: &Path, name: &str, bytes: &[u8]) -> TestResult {
    fs::write(root.join(name), bytes).map_err(debug_error)?;
    fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o600)).map_err(debug_error)
}

async fn fixture(pool: &PgPool) -> TestResult<Fixture> {
    fixture_with_error(pool, "dreamina_poll_protocol_invalid").await
}

async fn fixture_with_error(pool: &PgPool, error: &'static str) -> TestResult<Fixture> {
    let work = seed_video_ten_seconds(pool).await?;
    seed_v4_customer_quote_with_basis(
        pool,
        &work,
        V4CustomerQuoteIdentity {
            dimensions: json!({"duration":"10","ratio":"3:4","resolution":"720p"}),
            ..V4CustomerQuoteIdentity::dreamina_video()
        },
        V4CustomerQuoteBasis {
            metric: "video_requested_second",
            unit: "second",
            unit_size: 1,
            unit_price_micros: PRICE / 10,
            quantity_source: "request_derived",
            confidence: "exact",
            max_quantity: 10,
            max_amount_micros: PRICE,
        },
    )
    .await?;
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let prepared = executor
        .prepare_and_handoff(&work, DREAMINA_VIDEO_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    require(prepared.len() == 1, "one original output required")?;
    seed_terminal_quota(pool, &work).await?;
    let scope = ExecutorClaimScope {
        execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    };
    let lease = executor
        .claim_prepared(&scope, "operator-video-resolution-executor", 60_000)
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
        600_000,
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
    let remote_operation_id = format!("original-video-{}", work.job_id.simple());
    tasks
        .record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: remote_operation_id.clone(),
            provider_request_id: None,
            event_identity: "original-video-submit-receipt".into(),
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
            event_identity: "original-video-attach".into(),
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
        HistoricalVideoProtocolFailure(error),
        gpt_image_2_gateway::FilesystemProviderArtifactStagerFactory::new(
            Arc::clone(&blobs),
            1024 * 1024,
        )
        .map_err(debug_error)?,
        ProviderPollOrchestratorConfig {
            scope: ProviderTaskClaimScope {
                provider_id: DREAMINA_PROVIDER_ID.into(),
                provider_account_id: DREAMINA_ACCOUNT_ID,
                execution_profile_id: DREAMINA_VIDEO_PROFILE_ID,
            },
            owner: "historical-video-protocol-poller".into(),
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
        .claim_terminal("original-video-uncertain-reducer", 60_000)
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
    let evidence = tempfile::tempdir().map_err(debug_error)?;
    fs::set_permissions(evidence.path(), fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
    let evidence_root = fs::canonicalize(evidence.path()).map_err(debug_error)?;
    let stdout = serde_json::to_vec(&json!({"submit_id":remote_operation_id,"gen_status":"success","queue_info":{"queue_status":"Finish"},
        "result_json":{"images":[],"videos":[{"video_url":VIDEO_URL,"duration":10,"width":720,"height":960}]}})).map_err(debug_error)?;
    let request = DreaminaVideoSuccessResolutionRequest {
        job_id: work.job_id,
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        reviewed_stdout_sha256: sha256(&stdout),
        operator_reference: "offline:original-video-reviewed".into(),
    };
    let audit = serde_json::to_vec(&json!({"schema_version":1,"job_id":work.job_id,"submission_id":lease.submission_id,"executor_execution_id":lease.executor_execution_id,
        "remote_operation_id":remote_operation_id,"argv":["query_result","--submit_id",remote_operation_id],"stdout_sha256":request.reviewed_stdout_sha256,"exit_code":0,"executed_at_ms":database_now(pool).await?})).map_err(debug_error)?;
    let quote_hash: String =
        sqlx::query_scalar("SELECT quote_hash FROM customer_price_quotes WHERE job_id=$1")
            .bind(work.job_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let review = serde_json::to_vec(&json!({"schema_version":1,"stdout_sha256":request.reviewed_stdout_sha256,"execution_audit_sha256":sha256(&audit),
        "video_url_sha256":sha256(VIDEO_URL.as_bytes()),"quote_hash":quote_hash,"provider_command_sha256":lease.command_hash})).map_err(debug_error)?;
    write_private(&evidence_root, "reviewed-query.json", &stdout)?;
    write_private(&evidence_root, "execution-audit.json", &audit)?;
    write_private(&evidence_root, "review.json", &review)?;
    Ok(Fixture {
        _evidence: evidence,
        evidence_root,
        _artifacts: artifacts,
        request,
    })
}

async fn immutable_snapshot(pool: &PgPool, job_id: Uuid) -> TestResult<String> {
    let value: serde_json::Value = sqlx::query_scalar(r#"SELECT jsonb_build_object(
        'submissions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM provider_submissions t WHERE job_id=$1),
        'executions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY executor_execution_id) FROM executor_executions t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'decisions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY decision_id) FROM executor_resolution_decisions t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'reductions',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM executor_terminal_reductions t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'receipts',(SELECT jsonb_agg(to_jsonb(t) ORDER BY receipt_id) FROM provider_receipts t WHERE job_id=$1),
        'tasks',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM provider_remote_tasks t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'intents',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM provider_remote_submit_intents t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'allocations',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM executor_capacity_allocations t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'budgets',(SELECT jsonb_agg(to_jsonb(t) ORDER BY submission_id) FROM accepted_completion_budgets t WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1)),
        'capacity',(SELECT jsonb_agg(to_jsonb(t) ORDER BY resource_policy_id,revision) FROM executor_resource_policies t WHERE (resource_policy_id,revision) IN (SELECT resource_policy_id,resource_policy_revision FROM executor_capacity_allocations WHERE submission_id IN (SELECT submission_id FROM provider_submissions WHERE job_id=$1))))"#)
        .bind(job_id).fetch_one(pool).await.map_err(debug_error)?;
    Ok(sha256(&serde_json::to_vec(&value).map_err(debug_error)?))
}

async fn rollback_snapshot(pool: &PgPool, job_id: Uuid) -> TestResult<serde_json::Value> {
    sqlx::query_scalar(r#"SELECT jsonb_build_object('job',to_jsonb(job),'quota',to_jsonb(quota),'hold',to_jsonb(hold),'account',to_jsonb(account),
        'overlay',(SELECT count(*) FROM dreamina_operator_video_success_resolutions WHERE job_id=$1),
        'artifact',(SELECT count(*) FROM artifacts WHERE job_id=$1),'projection',(SELECT count(*) FROM job_response_projections WHERE job_id=$1),
        'retention',(SELECT count(*) FROM job_artifact_retention WHERE job_id=$1),'usage',(SELECT count(*) FROM usage_events WHERE job_id=$1),
        'facts',(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),'rating',(SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),
        'ledger',(SELECT count(*) FROM ledger_transactions WHERE source_job_id=$1),'events',(SELECT count(*) FROM job_events WHERE job_id=$1),
        'outbox',(SELECT count(*) FROM outbox_events WHERE job_id=$1)) FROM jobs job JOIN quota_reservations quota ON quota.reservation_id=job.reservation_id
        JOIN customer_billing_holds hold ON hold.job_id=job.job_id JOIN billing_accounts account ON account.tenant_id=hold.tenant_id AND account.currency=hold.currency WHERE job.job_id=$1"#)
        .bind(job_id).fetch_one(pool).await.map_err(debug_error)
}

#[tokio::test]
async fn original_video_success_resolves_once_and_preserves_canonical_evidence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&database.pool).await?;
        let before=immutable_snapshot(&database.pool,fixture.request.job_id).await?;
        let (a,b)=tokio::join!(
            resolve_dreamina_video_success(&database.pool,&fixture.evidence_root,&fixture.request),
            resolve_dreamina_video_success(&database.pool,&fixture.evidence_root,&fixture.request));
        let a=a.map_err(debug_error)?;let b=b.map_err(debug_error)?;
        require(a==b && a.captured_micros==PRICE && a.released_micros==0 && a.committed_units==10 && a.released_units==0,"concurrent original resolution changed identity or frozen ten-second price")?;
        require(before==immutable_snapshot(&database.pool,fixture.request.job_id).await?,"operator resolution mutated canonical execution evidence")?;
        let state:(i64,i64,i64,i64,i64,i64,i64)=sqlx::query_as(r#"SELECT
            (SELECT count(*) FROM artifacts WHERE job_id=$1),(SELECT count(*) FROM provider_usage_facts WHERE job_id=$1),
            (SELECT sum(quantity)::BIGINT FROM provider_usage_facts WHERE job_id=$1),
            (SELECT count(*) FROM customer_rated_usage WHERE job_id=$1),(SELECT count(*) FROM provider_receipts WHERE job_id=$1),
            (SELECT count(*) FROM executor_result_manifests WHERE submission_id=$2),
            (SELECT COALESCE(sum(posting.amount_micros),0)::BIGINT FROM ledger_postings posting JOIN ledger_transactions transaction USING(transaction_id) WHERE transaction.source_job_id=$1)"#)
            .bind(fixture.request.job_id).bind(fixture.request.submission_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        require(state==(0,1,10,1,1,0,0),format!("resolution ledger/delivery count drifted: {state:?}"))?;
        let mut changed=fixture.request.clone();changed.operator_reference.push_str("-changed");
        require(resolve_dreamina_video_success(&database.pool,&fixture.evidence_root,&changed).await.is_err(),"changed input was accepted as replay")?;
        let settlement=PostgresExecutionSettlementStore::new(database.pool.clone(),Arc::new(InMemoryArtifactBlobStore::default()));
        let tenant_id:String=sqlx::query_scalar("SELECT tenant_id FROM jobs WHERE job_id=$1").bind(fixture.request.job_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        for status in [
            settlement.video_status(&tenant_id,fixture.request.job_id).await.map_err(debug_error)?,
            settlement.project_video_status(&tenant_id,"project-test",None,fixture.request.job_id).await.map_err(debug_error)?,
            settlement.project_video_status_with_metadata(&tenant_id,"project-test",None,fixture.request.job_id).await.map_err(debug_error)?.map(|(status,_)|status),
        ] {
            require(matches!(status,Some(gpt_image_2_gateway::settlement::VideoResultStatus::SucceededRemote{ref result,duration_ms:10_000,..}) if result.video_url()==VIDEO_URL && result.expires_at_ms().is_none()),"customer cannot read the original resolved video")?;
        }
        for (tenant,project,actor) in [("other-tenant","project-test",None),(tenant_id.as_str(),"other-project",None),(tenant_id.as_str(),"project-test",Some(Uuid::new_v4()))] {
            require(settlement.project_video_status(tenant,project,actor,fixture.request.job_id).await.map_err(debug_error)?.is_none(),"audited video crossed authorization scope")?;
            require(settlement.project_video_status_with_metadata(tenant,project,actor,fixture.request.job_id).await.map_err(debug_error)?.is_none(),"audited metadata crossed authorization scope")?;
        }
        Ok(())
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn original_video_resolution_rolls_back_all_database_effects_on_each_failure() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let fixture=fixture(&database.pool).await?;
        let before=rollback_snapshot(&database.pool,fixture.request.job_id).await?;
        sqlx::query("CREATE FUNCTION reject_operator_video_fixture() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'injected operator reconciliation failure'; END; $$ LANGUAGE plpgsql").execute(&database.pool).await.map_err(debug_error)?;
        for (table,deferred) in [("job_response_projections",false),("usage_events",false),("metering_events",false),("provider_usage_facts",false),("customer_rated_usage",false),("ledger_postings",false),("job_events",false),("outbox_events",false),("dreamina_operator_video_success_resolutions",true)] {
            let trigger=if deferred {format!("CREATE CONSTRAINT TRIGGER reject_operator_video_fixture AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_operator_video_fixture()")}
                else {format!("CREATE TRIGGER reject_operator_video_fixture BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION reject_operator_video_fixture()")};
            sqlx::query(AssertSqlSafe(trigger)).execute(&database.pool).await.map_err(debug_error)?;
            let resolved=resolve_dreamina_video_success(&database.pool,&fixture.evidence_root,&fixture.request).await;
            sqlx::query(AssertSqlSafe(format!("DROP TRIGGER reject_operator_video_fixture ON {table}"))).execute(&database.pool).await.map_err(debug_error)?;
            require(resolved.is_err(),format!("injected {table} failure was ignored"))?;
            require(before==rollback_snapshot(&database.pool,fixture.request.job_id).await?,format!("injected {table} failure leaked database effects"))?;
        }
        let result=resolve_dreamina_video_success(&database.pool,&fixture.evidence_root,&fixture.request).await.map_err(debug_error)?;
        require(result.captured_micros==PRICE,"successful retry drifted after database rollback")
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn original_video_resolution_rejects_modified_or_unprotected_evidence() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&database.pool).await?;
        let before = rollback_snapshot(&database.pool, fixture.request.job_id).await?;
        for (name, key, value) in [
            ("review.json", "video_url_sha256", json!("b".repeat(64))),
            ("review.json", "quote_hash", json!("b".repeat(64))),
            (
                "review.json",
                "provider_command_sha256",
                json!("b".repeat(64)),
            ),
            (
                "execution-audit.json",
                "argv",
                json!(["text2video", "--prompt", "forbidden"]),
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
                resolve_dreamina_video_success(
                    &database.pool,
                    &fixture.evidence_root,
                    &fixture.request,
                )
                .await
                .is_err(),
                format!("modified {key} accepted"),
            )?;
            write_private(&fixture.evidence_root, name, &original)?;
        }
        fs::set_permissions(
            fixture.evidence_root.join("reviewed-query.json"),
            fs::Permissions::from_mode(0o644),
        )
        .map_err(debug_error)?;
        require(
            resolve_dreamina_video_success(
                &database.pool,
                &fixture.evidence_root,
                &fixture.request,
            )
            .await
            .is_err(),
            "unprotected query evidence accepted",
        )?;
        require(
            before == rollback_snapshot(&database.pool, fixture.request.job_id).await?,
            "bad evidence changed database",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

async fn seed_video_ten_seconds(pool: &PgPool) -> TestResult<WorkLease> {
    let job_id = Uuid::new_v4();
    let owner_token = Uuid::new_v4();
    let request_id = format!("request-{}", Uuid::new_v4().simple());
    let plan = DreaminaVideoAdmissionPlan::new(DreaminaVideoGenerationRequest {
        prompt: "animate a provider-native pricing lighthouse".to_string(),
        model_version: Some("seedance2.0fast".to_string()),
        ratio: Some("3:4".to_string()),
        duration: Some(10),
        video_resolution: "720p".to_string(),
    })
    .map_err(debug_error)?;
    let claim = plan.claim_for_profile(
        DREAMINA_VIDEOS_API_PROFILE,
        owner_token,
        "executord-process-smoke",
        "project-test",
        request_id.clone(),
        None,
        i64::MAX,
    );
    let attach = plan.attach(
        AdmissionTicket {
            session_id: Uuid::new_v4(),
            owner_token,
            request_hash: claim.request_hash.clone(),
        },
        job_id,
        "dreamina-video-terminal-test",
    );
    seed_generation_lease(
        pool,
        "operator-video-worker",
        GenerationLeaseSeed {
            job_id,
            admission_session_id: attach.ticket.session_id,
            owner_token,
            api_profile: claim.api_profile,
            provider_id: plan.provider_id().to_string(),
            model: plan.provider_model().to_string(),
            request_id,
            request_hash: claim.request_hash,
            command_schema: attach.command_schema,
            command_json: attach.command_json,
            operation: VIDEO_GENERATION_OPERATION.to_string(),
            work_kind: "video_single".to_string(),
            output_count: 1,
            billable_units: u32::from(plan.duration()),
            output_billable_units: u32::from(plan.duration()),
            billing_metric: "video_second".to_string(),
            billing_unit: "second".to_string(),
        },
    )
    .await
}

#[tokio::test]
async fn operator_video_success_rejects_other_uncertain_error_scope() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture_with_error(&database.pool, "dreamina_poll_video_url_invalid").await?;
        let before = rollback_snapshot(&database.pool, fixture.request.job_id).await?;
        require(
            resolve_dreamina_video_success(
                &database.pool,
                &fixture.evidence_root,
                &fixture.request,
            )
            .await
            .is_err(),
            "unreviewed uncertain scope was broadened",
        )?;
        require(
            before == rollback_snapshot(&database.pool, fixture.request.job_id).await?,
            "rejected uncertain scope changed settlement",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn delivered_video_payload_and_usage_cannot_be_moved_or_rewritten() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&database.pool).await?;
        resolve_dreamina_video_success(&database.pool, &fixture.evidence_root, &fixture.request)
            .await.map_err(debug_error)?;
        let job_id = fixture.request.job_id;
        let before = rollback_snapshot(&database.pool, job_id).await?;
        let payload: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(p) FROM job_payloads p WHERE job_id=$1")
            .bind(job_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        for statement in [
            "UPDATE job_payloads SET command_json=jsonb_set(command_json, '{prompt}', '\"rewritten\"'::jsonb) WHERE job_id=$1",
            "DELETE FROM job_payloads WHERE job_id=$1",
            "UPDATE usage_events SET job_id=NULL WHERE job_id=$1",
            "UPDATE metering_events SET job_id=NULL WHERE job_id=$1",
        ] {
            let mut tx=database.pool.begin().await.map_err(debug_error)?;
            let mutation=sqlx::query(statement).bind(job_id).execute(&mut *tx).await;
            let rejected=match mutation {
                Err(_) => true,
                Ok(changed) => {
                    require(changed.rows_affected()>0, "guard test did not mutate a row")?;
                    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE").execute(&mut *tx).await.is_err()
                }
            };
            tx.rollback().await.map_err(debug_error)?;
            require(rejected, format!("audited video dependency escaped: {statement}"))?;
            require(before==rollback_snapshot(&database.pool,job_id).await?, "rejected change leaked financial state")?;
            let after:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(p) FROM job_payloads p WHERE job_id=$1")
                .bind(job_id).fetch_one(&database.pool).await.map_err(debug_error)?;
            require(after==payload, "rejected change rewrote payload")?;
        }
        Ok(())
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn delivered_video_preserves_original_events_and_rejects_duplicate_success() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = fixture(&database.pool).await?;
        resolve_dreamina_video_success(&database.pool, &fixture.evidence_root, &fixture.request)
            .await
            .map_err(debug_error)?;
        let job_id = fixture.request.job_id;
        let before = rollback_snapshot(&database.pool, job_id).await?;
        let event_snapshot = r#"SELECT jsonb_build_object(
            'events',(SELECT jsonb_agg(to_jsonb(event) ORDER BY event_id) FROM job_events event WHERE job_id=$1),
            'outbox',(SELECT jsonb_agg(to_jsonb(event) ORDER BY event_id) FROM outbox_events event WHERE job_id=$1))"#;
        let original_events: serde_json::Value = sqlx::query_scalar(event_snapshot)
            .bind(job_id)
            .fetch_one(&database.pool)
            .await
            .map_err(debug_error)?;
        for table in ["job_events", "outbox_events"] {
            for statement in [
                format!("UPDATE {table} SET event_type='job.failed' WHERE job_id=$1 AND semantic_key LIKE 'work.%.executor-terminal'"),
                format!("UPDATE {table} SET payload_json=jsonb_set(payload_json, '{{terminal_state}}', '\"failed\"'::jsonb) WHERE job_id=$1 AND semantic_key LIKE 'work.%.executor-terminal'"),
                format!("UPDATE {table} SET payload_json=jsonb_set(payload_json, '{{error_code}}', '\"rewritten\"'::jsonb) WHERE job_id=$1 AND semantic_key LIKE 'work.%.executor-terminal'"),
                format!("INSERT INTO {table} (event_id,job_id,event_type,semantic_key,payload_json,created_at_ms) SELECT gen_random_uuid(),job_id,'job.succeeded','unexpected-success',payload_json,created_at_ms FROM {table} WHERE job_id=$1 AND semantic_key='job.operator_dreamina_video_success'"),
            ] {
                let mut tx = database.pool.begin().await.map_err(debug_error)?;
                let mutation = sqlx::query(AssertSqlSafe(statement.clone()))
                    .bind(job_id)
                    .execute(&mut *tx)
                    .await;
                let rejected = match mutation {
                    Err(_) => true,
                    Ok(changed) => {
                        require(changed.rows_affected() == 1, "event guard test must mutate one row")?;
                        sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
                            .execute(&mut *tx)
                            .await
                            .is_err()
                    }
                };
                tx.rollback().await.map_err(debug_error)?;
                require(rejected, format!("audited video event escaped: {statement}"))?;
                require(
                    before == rollback_snapshot(&database.pool, job_id).await?,
                    "rejected event mutation changed financial state",
                )?;
                let after: serde_json::Value = sqlx::query_scalar(event_snapshot)
                    .bind(job_id)
                    .fetch_one(&database.pool)
                    .await
                    .map_err(debug_error)?;
                require(after == original_events, "rejected mutation changed original events")?;
            }
        }
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}
