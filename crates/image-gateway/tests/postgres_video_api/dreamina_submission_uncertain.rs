use super::*;
use gpt_image_2_gateway::{
    ProviderRuntimeProfile, ProviderSubmitFailureKind, ProviderSubmitIntent,
    ProviderSubmitIntentState, ProviderTaskObservation, ProviderTaskObservationOutcome,
    ProviderTaskObservationSource, ProviderTaskStore, RemoteTaskAttach, RemoteTaskSubmitFailure,
    RemoteTaskSubmitReceipt, RemoteTaskSubmitReservation, executor::ExecutorSubmissionLease,
};
use image_provider_dreamina_cli::{DreaminaSubmitPayloadV1, parse_submit_command};
use image_provider_sdk::{OutputSlot, ProviderTaskProgress, SingleOutputCommand};

struct Fixture {
    _root: TempDir,
    app: axum::Router,
    tenant: String,
    key: String,
    other_key: String,
    sibling_key: String,
    profile: ProviderRuntimeProfile,
    tasks: PostgresProviderTaskStore,
    settlement: Arc<PostgresExecutionSettlementStore>,
}

impl Fixture {
    async fn new(pool: &PgPool) -> TestResult<Self> {
        let root = TempDir::new().map_err(debug_error)?;
        let home = root.path().join("home");
        fs::create_dir(&home).map_err(debug_error)?;
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).map_err(debug_error)?;
        let provisioned = provision_dreamina_video_execution_profile(
            pool,
            &DreaminaExecutionProfileProvisioning {
                profile_key: "submission-uncertain".into(),
                credential_pool_key: "submission-uncertain".into(),
                provider_account_key: "submission-uncertain".into(),
                credential_ref: "private:offline-fixture".into(),
                credential_revision: 1,
                credential_auth_sha256: "a".repeat(64),
                max_concurrency: 8,
            },
        )
        .await
        .map_err(debug_error)?;
        let tasks = PostgresProviderTaskStore::new(pool.clone());
        let profile = tasks
            .load_active_runtime_profile("submission-uncertain")
            .await
            .map_err(debug_error)?;
        for role in [ProviderRuntimeRole::Submit, ProviderRuntimeRole::Poll] {
            tasks
                .register_runtime(
                    &ProviderRuntimeRegistration {
                        runtime_id: Uuid::new_v4(),
                        execution_profile_id: provisioned.execution_profile_id,
                        role,
                        runtime_owner: format!("offline-{role:?}"),
                    },
                    60_000,
                )
                .await
                .map_err(debug_error)?;
        }
        let keys = Arc::new(PostgresApiKeyStore::new(
            pool.clone(),
            ApiKeyKeyring::new(1, [(1, vec![0x59; 32])]).map_err(debug_error)?,
        ));
        let project = keys
            .create_project("Submission uncertainty")
            .await
            .map_err(debug_error)?;
        let other_project = keys
            .create_project("Other tenant")
            .await
            .map_err(debug_error)?;
        let sibling = format!("proj_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO gateway_projects (id, tenant_id, name, created_at)
            VALUES ($1, $2, 'Sibling project', 1)",
        )
        .bind(&sibling)
        .bind(&project.id)
        .execute(pool)
        .await
        .map_err(debug_error)?;
        let owner = keys
            .create_service_account(
                &project.id,
                "Owner",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let other = keys
            .create_service_account(
                &other_project.id,
                "Other",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        let sibling = keys
            .create_service_account(
                &sibling,
                "Sibling",
                ApiKeyPermissionMode::All,
                ApiKeyPermissions::default(),
            )
            .await
            .map_err(debug_error)?;
        seed_route(
            pool,
            &project.id,
            &owner.id,
            &owner.api_key.id,
            provisioned.provider_account_id,
            provisioned.execution_profile_id,
            &home,
        )
        .await?;
        seed_economics(pool, &project.id).await?;
        let blobs = Arc::new(FilesystemArtifactBlobStore::new(root.path()).map_err(debug_error)?);
        let settlement = Arc::new(PostgresExecutionSettlementStore::new(
            pool.clone(),
            blobs.clone(),
        ));
        let app = build_router_with_external_execution_and_control_plane_and_runtime_events_and_model_routing(
            config(), ExternalImageGatewayComponents {
                usage_store: Arc::new(PostgresUsageStore::new(pool.clone())), api_key_store: keys,
                admission_store: Arc::new(PostgresAdmissionStore::new(pool.clone())),
                settlement_store: settlement.clone(), input_blob_store: blobs,
                provider_readiness_store: Arc::new(tasks.clone()),
            }, None, None, None, None,
            Some(Arc::new(PostgresModelRoutingStore::new(pool.clone()))),
        ).map_err(debug_error)?;
        Ok(Self {
            _root: root,
            app,
            tenant: project.id,
            key: owner.api_key.value,
            other_key: other.api_key.value,
            sibling_key: sibling.api_key.value,
            profile,
            tasks,
            settlement,
        })
    }

    async fn sending(
        &self,
        pool: &PgPool,
        identity: &str,
        timeout: i64,
    ) -> TestResult<(ExecutorSubmissionLease, ProviderSubmitIntent)> {
        let body = json!({"model":MODEL, "content":[{"type":"text","text":"offline fixture"}],
            "duration":5,"resolution":"480p","ratio":"16:9"});
        let (status, created) = json_request(
            self.app.clone(),
            Method::POST,
            TASKS,
            &self.key,
            Some(identity),
            Some(&body),
        )
        .await?;
        require(
            status == StatusCode::OK,
            format!("fixture admission: {status} {created}"),
        )?;
        let id = created["id"].as_str().ok_or("task id missing")?;
        let job = Uuid::parse_str(id.strip_prefix("cgt-").ok_or("task prefix missing")?)
            .map_err(debug_error)?;
        let executor = Arc::new(PostgresExecutorSubmissionStore::new(pool.clone()));
        let worker = Workerd::new_handoff_only_with_contract(
            identity.into(),
            Arc::new(PostgresAdmissionStore::new(pool.clone())),
            Arc::new(PostgresExecutionContextStore::new(pool.clone())),
            executor.clone(),
            self.profile.execution_profile_id(),
            DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
            Duration::from_secs(30),
            AdmissionContract::CustomerPricingV4,
        )
        .map_err(debug_error)?;
        require(
            worker.run_once().await.map_err(debug_error)? == Some(job),
            "handoff missing",
        )?;
        let lease = executor
            .claim_prepared(&self.profile.executor_claim_scope(), identity, 60_000)
            .await
            .map_err(debug_error)?
            .ok_or("prepared fixture missing")?;
        require(lease.job_id == job, "claim selected another fixture")?;
        executor.start(&lease).await.map_err(debug_error)?;
        let command: Value =
            sqlx::query_scalar("SELECT command_json FROM job_payloads WHERE job_id=$1")
                .bind(job)
                .fetch_one(pool)
                .await
                .map_err(debug_error)?;
        let command = SingleOutputCommand::new(
            OutputSlot::new(0, 1).map_err(debug_error)?,
            DreaminaSubmitPayloadV1::new(
                &lease.command_hash,
                parse_submit_command(&serde_json::to_vec(&command).map_err(debug_error)?)
                    .map_err(debug_error)?
                    .into_single_output(),
            )
            .map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        let reservation = RemoteTaskSubmitReservation::new(
            &lease,
            identity.into(),
            command.output(),
            command.identity(),
            timeout,
        );
        self.tasks
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        self.tasks
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let intent = self
            .tasks
            .load_submit_intent(lease.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("submit intent missing")?;
        Ok((lease, intent))
    }

    async fn quarantine(
        &self,
        pool: &PgPool,
        identity: &str,
        reason: &str,
    ) -> TestResult<ExecutorSubmissionLease> {
        let (lease, intent) = self.sending(pool, identity, 200).await?;
        self.tasks
            .record_submit_failure(&failure(&lease, &intent, reason))
            .await
            .map_err(debug_error)?;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let resolved = self
            .tasks
            .resolve_due_submit_deadline(&self.profile.claim_scope())
            .await
            .map_err(debug_error)?
            .ok_or("deadline did not resolve")?;
        require(
            resolved.submission_id == lease.submission_id
                && resolved.state == ProviderSubmitIntentState::DeadlineQuarantined
                && resolved.remote_operation_id.is_none()
                && resolved.failure_error_code.as_deref() == Some(reason),
            "quarantine lost submit evidence",
        )?;
        let reducer = PostgresExecutorTerminalStore::new(pool.clone());
        let terminal = reducer
            .claim_terminal(identity, 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("uncertain terminal missing")?;
        require(
            terminal.job_id == lease.job_id,
            "terminal belongs to another fixture",
        )?;
        reducer
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        let state: (String, String, String, i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT j.state, w.state, s.state, h.held_micros, h.captured_micros, h.released_micros,
                (SELECT COUNT(*) FROM ledger_transactions WHERE source_job_id=j.job_id),
                (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id=j.job_id)
             FROM jobs j JOIN work_items w USING(job_id)
             JOIN provider_submissions s USING(job_id) JOIN customer_billing_holds h USING(job_id)
             WHERE j.job_id=$1",
        )
        .bind(lease.job_id)
        .fetch_one(pool)
        .await
        .map_err(debug_error)?;
        require(
            state
                == (
                    "uncertain".into(),
                    "uncertain".into(),
                    "uncertain".into(),
                    5 * UNIT_PRICE_MICROS,
                    0,
                    0,
                    0,
                    1,
                ),
            format!("uncertainty changed frozen quote/hold or charged the job: {state:?}"),
        )?;
        Ok(lease)
    }

    async fn get(&self, job: Uuid) -> TestResult<Value> {
        let (status, body) = json_request(
            self.app.clone(),
            Method::GET,
            &format!("/v1/dreamina/videos/{job}"),
            &self.key,
            None,
            None,
        )
        .await?;
        require(
            status == StatusCode::OK,
            format!("native GET: {status} {body}"),
        )?;
        Ok(body)
    }
}

fn failure(
    lease: &ExecutorSubmissionLease,
    intent: &ProviderSubmitIntent,
    reason: &str,
) -> RemoteTaskSubmitFailure {
    RemoteTaskSubmitFailure {
        submission_id: lease.submission_id,
        executor_execution_id: lease.executor_execution_id,
        executor_owner: lease.executor_owner.clone(),
        executor_lease_epoch: lease.executor_lease_epoch,
        kind: ProviderSubmitFailureKind::OutcomeUnknown,
        event_identity: format!("{}-failure", lease.submission_id),
        error_code: reason.into(),
        execution_binding_sha256: intent.execution_binding_sha256.clone(),
        recovery_fence: None,
    }
}

// Snapshot complete rows, including holds, frozen prices and receipt identities.
// Authentication bookkeeping is deliberately outside the business-state proof.
async fn business_snapshot(pool: &PgPool) -> TestResult<Value> {
    let mut snapshot = serde_json::Map::new();
    for table in [
        "jobs",
        "work_items",
        "job_outputs",
        "provider_submissions",
        "executor_executions",
        "executor_terminal_reductions",
        "provider_remote_submit_intents",
        "provider_submit_recoveries",
        "provider_receipts",
        "provider_submit_quarantined_receipts",
        "provider_remote_tasks",
        "provider_task_observations",
        "job_response_projections",
        "billing_accounts",
        "customer_billing_holds",
        "customer_price_quotes",
        "customer_price_quote_lines",
        "price_quotes",
        "provider_usage_facts",
        "customer_rated_usage",
        "ledger_transactions",
        "quota_reservations",
        "executor_capacity_allocations",
    ] {
        let rows: Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text), '[]'::jsonb) FROM {table} t")))
            .fetch_one(pool).await.map_err(debug_error)?;
        snapshot.insert(table.into(), rows);
    }
    Ok(Value::Object(snapshot))
}

#[tokio::test]
async fn submission_reasons_are_safe_scoped_and_read_only_after_deadline() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = Fixture::new(&db.pool).await?;
        for (index, reason) in [
            "dreamina_cli_permission_denied",
            "provider_submit_timeout",
            "provider_submit_cli_failed",
            "provider_submit_deadline",
            "fixture_private_submit_detail",
        ]
        .into_iter()
        .enumerate()
        {
            let lease = fixture
                .quarantine(&db.pool, &format!("safe-reason-{index}"), reason)
                .await?;
            let before = business_snapshot(&db.pool).await?;
            let body = fixture.get(lease.job_id).await?;
            require(
                body["status"] == "uncertain"
                    && body["error"]["code"] == "generation_uncertain"
                    && body.get("content").is_none()
                    && body.get("provider_progress").is_none(),
                format!("unsafe or incomplete submission diagnostic: {body}"),
            )?;
            if reason == "fixture_private_submit_detail" {
                require(body.get("stage").is_none()
                    && body["error"].get("reason_code").is_none()
                    && !body.to_string().contains(reason),
                    format!("private submit reason leaked: {body}"))?;
            } else {
                require(body["stage"] == "submission_uncertain"
                    && body["error"]["reason_code"] == reason
                    && body["error"]["message"].as_str().is_some_and(|v| v.contains("do not resubmit")),
                    format!("safe submission diagnostic missing: {body}"))?;
            }
            for key in [&fixture.other_key, &fixture.sibling_key] {
                let (status, denied) = json_request(
                    fixture.app.clone(),
                    Method::GET,
                    &format!("/v1/dreamina/videos/{}", lease.job_id),
                    key,
                    None,
                    None,
                )
                .await?;
                require(
                    status == StatusCode::BAD_REQUEST
                        && denied["error"]["code"] == "not_found"
                        && !denied.to_string().contains(reason),
                    "native GET crossed tenant/project",
                )?;
            }
            for (tenant, project, actor) in [
                ("other", fixture.tenant.as_str(), None),
                (fixture.tenant.as_str(), "other", None),
                (
                    fixture.tenant.as_str(),
                    fixture.tenant.as_str(),
                    Some(Uuid::new_v4()),
                ),
            ] {
                require(
                    fixture
                        .settlement
                        .project_video_status_with_metadata(tenant, project, actor, lease.job_id)
                        .await
                        .map_err(debug_error)?
                        .is_none(),
                    "metadata read crossed tenant/project/actor",
                )?;
                require(
                    fixture
                        .settlement
                        .project_video_status(tenant, project, actor, lease.job_id)
                        .await
                        .map_err(debug_error)?
                        .is_none(),
                    "status read crossed tenant/project/actor",
                )?;
            }
            let legacy = fixture
                .settlement
                .video_status(&fixture.tenant, lease.job_id)
                .await
                .map_err(debug_error)?;
            require(
                matches!(legacy, Some(gpt_image_2_gateway::settlement::VideoResultStatus::Uncertain {
                submission_error_code: Some(ref code), .. }) if code == reason),
                "legacy tenant status lost bound submit reason",
            )?;
            require(
                business_snapshot(&db.pool).await? == before,
                "GET changed business state",
            )?;
        }
        Ok(())
    }
    .await;
    combine(result, db.cleanup().await)
}

#[tokio::test]
async fn independent_submission_evidence_cannot_relabel_attached_provider_queue() -> TestResult {
    let _guard = postgres_video_test_lock().lock().await;
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let fixture = Fixture::new(&db.pool).await?;
        let first = fixture
            .quarantine(
                &db.pool,
                "permission-task",
                "dreamina_cli_permission_denied",
            )
            .await?;
        let (second, intent) = fixture.sending(&db.pool, "queued-task", 60_000).await?;
        let mut cross_bound = failure(&second, &intent, "provider_submit_timeout");
        cross_bound.executor_execution_id = first.executor_execution_id;
        let before_rejected = business_snapshot(&db.pool).await?;
        require(
            fixture
                .tasks
                .record_submit_failure(&cross_bound)
                .await
                .is_err(),
            "cross-execution submit evidence was accepted",
        )?;
        require(
            business_snapshot(&db.pool).await? == before_rejected,
            "rejected cross-binding mutated business state",
        )?;
        fixture
            .tasks
            .record_submit_receipt(&RemoteTaskSubmitReceipt {
                submission_id: second.submission_id,
                executor_execution_id: second.executor_execution_id,
                executor_owner: second.executor_owner.clone(),
                executor_lease_epoch: second.executor_lease_epoch,
                remote_operation_id: "offline-queued-operation".into(),
                provider_request_id: None,
                event_identity: "queued-receipt".into(),
                execution_binding_sha256: intent.execution_binding_sha256.clone(),
            })
            .await
            .map_err(debug_error)?;
        fixture
            .tasks
            .attach(&RemoteTaskAttach {
                submission_id: second.submission_id,
                executor_execution_id: second.executor_execution_id,
                executor_owner: second.executor_owner.clone(),
                executor_lease_epoch: second.executor_lease_epoch,
                remote_operation_id: "offline-queued-operation".into(),
                provider_request_id: None,
                event_identity: "queued-attach".into(),
                execution_binding_sha256: intent.execution_binding_sha256,
                poll_after_ms: 0,
                recovery_fence: None,
            })
            .await
            .map_err(debug_error)?;
        let poll = fixture
            .tasks
            .claim_due(&fixture.profile.claim_scope(), "queue-observer", 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("attached poll missing")?;
        fixture
            .tasks
            .record_observation(
                &poll,
                &ProviderTaskObservation {
                    event_identity: "queued-observation".into(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
                    progress: Some(ProviderTaskProgress {
                        provider_status: "Queueing".into(),
                        queue_position: Some(12),
                        queue_total: Some(50),
                        estimated_remaining_seconds: Some(100),
                    }),
                },
            )
            .await
            .map_err(debug_error)?;
        let before = business_snapshot(&db.pool).await?;
        let queued = fixture.get(second.job_id).await?;
        require(
            queued["status"] == "running"
                && queued["stage"] == "provider_queued"
                && queued["provider_progress"]["queue_position"] == 12
                && queued.get("error").is_none(),
            format!("attached queue became manual submission uncertainty: {queued}"),
        )?;
        let permission = fixture.get(first.job_id).await?;
        require(
            permission["error"]["reason_code"] == "dreamina_cli_permission_denied"
                && permission["stage"] == "submission_uncertain",
            "independent task lost its own evidence",
        )?;
        require(
            business_snapshot(&db.pool).await? == before,
            "independent GET changed business state",
        )?;
        // Even a matching whitelist code from a real provider task is not a
        // missing-submit receipt: attachment authority keeps the old shape.
        let poll = fixture
            .tasks
            .claim_due(&fixture.profile.claim_scope(), "uncertain-observer", 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("second attached poll missing")?;
        fixture
            .tasks
            .record_observation(
                &poll,
                &ProviderTaskObservation {
                    event_identity: "attached-uncertain".into(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Uncertain {
                        error_code: "dreamina_cli_permission_denied".into(),
                    },
                    progress: None,
                },
            )
            .await
            .map_err(debug_error)?;
        let reducer = PostgresExecutorTerminalStore::new(db.pool.clone());
        let terminal = reducer
            .claim_terminal("attached-reducer", 30_000)
            .await
            .map_err(debug_error)?
            .ok_or("attached uncertainty terminal missing")?;
        require(terminal.job_id == second.job_id, "wrong attached reduction")?;
        reducer
            .complete_terminal(&terminal, None)
            .await
            .map_err(debug_error)?;
        let before = business_snapshot(&db.pool).await?;
        let attached = fixture.get(second.job_id).await?;
        require(
            attached["status"] == "uncertain"
                && attached["error"]["code"] == "generation_uncertain"
                && attached.get("stage").is_none()
                && attached["error"].get("reason_code").is_none(),
            format!("attached uncertainty was mislabeled as submit uncertainty: {attached}"),
        )?;
        require(
            business_snapshot(&db.pool).await? == before,
            "attached uncertainty GET wrote business state",
        )?;
        Ok(())
    }
    .await;
    combine(result, db.cleanup().await)
}
