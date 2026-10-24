use super::*;
use image_provider_dreamina_cli::{DreaminaSubmitPayloadV1, parse_native_image_batch_command};

fn executor_scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id: DREAMINA_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        command_schema: DREAMINA_SUBMIT_COMMAND_SCHEMA.into(),
        adapter_revision: DREAMINA_ADAPTER_REVISION.into(),
    }
}
fn poll_scope() -> ProviderTaskClaimScope {
    ProviderTaskClaimScope {
        execution_profile_id: DREAMINA_PROFILE_ID,
        provider_id: DREAMINA_PROVIDER_ID.into(),
        provider_account_id: DREAMINA_ACCOUNT_ID,
    }
}
async fn prepare(pool: &PgPool, count: i32, schema_version: i32) -> TestResult<WorkLease> {
    let work = seed_work_lease_for_runtime_profile_with_command(pool, "offline-batch-worker", DREAMINA_PROVIDER_ID, "dreamina-image-4.0", DREAMINA_SUBMIT_COMMAND_SCHEMA,
        json!({"operation":"text2image", "schema_version":schema_version,"prompt":"offline fixture", "model_version":"4.0", "ratio":"1:1", "resolution_type":"2k", "generate_num":count,"poll":0})).await?;
    PostgresExecutorSubmissionStore::new(pool.clone())
        .prepare_and_handoff(&work, DREAMINA_PROFILE_ID)
        .await
        .map_err(debug_error)?;
    Ok(work)
}
async fn launch(pool: &PgPool) -> TestResult<ExecutorSubmissionLease> {
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let lease = executor
        .claim_prepared(&executor_scope(), "offline-batch-executor", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or("leader missing")?;
    executor.start(&lease).await.map_err(debug_error)?;
    Ok(lease)
}
async fn attach(
    pool: &PgPool,
    work: &WorkLease,
    lease: &ExecutorSubmissionLease,
) -> TestResult<ProviderTaskLease> {
    attach_with_timeout(pool, work, lease, 60_000).await
}
async fn attach_with_timeout(
    pool: &PgPool,
    work: &WorkLease,
    lease: &ExecutorSubmissionLease,
    timeout_ms: i64,
) -> TestResult<ProviderTaskLease> {
    let tasks = PostgresProviderTaskStore::new(pool.clone());
    let reservation = reservation_for(work, lease, timeout_ms)?;
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
        .ok_or("intent missing")?
        .execution_binding_sha256;
    let operation = format!("offline-native-{}", lease.submission_id.simple());
    tasks
        .record_submit_receipt(&RemoteTaskSubmitReceipt {
            submission_id: lease.submission_id,
            executor_execution_id: lease.executor_execution_id,
            executor_owner: lease.executor_owner.clone(),
            executor_lease_epoch: lease.executor_lease_epoch,
            remote_operation_id: operation.clone(),
            provider_request_id: None,
            event_identity: "offline-receipt".into(),
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
            remote_operation_id: operation,
            provider_request_id: None,
            event_identity: "offline-attach".into(),
            execution_binding_sha256: binding,
            poll_after_ms: 0,
            recovery_fence: None,
        })
        .await
        .map_err(debug_error)?;
    tasks
        .claim_due(&poll_scope(), "offline-batch-poller", 60_000)
        .await
        .map_err(debug_error)?
        .ok_or_else(|| "poll lease missing".into())
}
fn reservation_for(
    work: &WorkLease,
    lease: &ExecutorSubmissionLease,
    timeout_ms: i64,
) -> TestResult<RemoteTaskSubmitReservation> {
    let source = serde_json::to_vec(&work.command_json).map_err(debug_error)?;
    let batch = parse_native_image_batch_command(&source).map_err(debug_error)?;
    let command = SingleOutputCommand::new(
        OutputSlot::new(
            0,
            work.command_json["generate_num"].as_u64().unwrap() as u32,
        )
        .map_err(debug_error)?,
        DreaminaSubmitPayloadV1::new_native_image_batch(&lease.command_hash, batch)
            .map_err(debug_error)?,
    )
    .map_err(debug_error)?;
    Ok(RemoteTaskSubmitReservation::new(
        lease,
        format!("offline-native-{}", lease.submission_id.simple()),
        command.output(),
        command.identity(),
        timeout_ms,
    ))
}

fn authority(execution_id: Uuid, digest: char) -> ProviderArtifactAuthority {
    let id = execution_id.simple().to_string();
    ProviderArtifactAuthority::new(
        "filesystem-v1".into(),
        "filesystem-v1:offline-batch-test".into(),
        format!("executor-objects/{}/{}", &id[..2], id),
        digest.to_string().repeat(64),
        128,
        "image/png".into(),
    )
    .unwrap()
}
async fn assert_states(pool: &PgPool, job_id: Uuid, expected: &[&str]) -> TestResult {
    let states: Vec<String> = sqlx::query_scalar("SELECT submission.state FROM provider_submissions submission JOIN job_outputs output USING (output_id, job_id) WHERE submission.job_id = $1 ORDER BY output.output_index").bind(job_id).fetch_all(pool).await.map_err(debug_error)?;
    assert_eq!(states, expected);
    Ok(())
}

#[tokio::test]
async fn native_batch_only_leader_claims_and_database_rejects_follower_launch() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        let work = prepare(&db.pool, 10, 2).await?;
        let tasks = PostgresProviderTaskStore::new(db.pool.clone());
        assert_eq!(tasks.summarize_execution_queue_readiness(60_000).await.map_err(debug_error)?.prepared_executions, 1);
        let executor = PostgresExecutorSubmissionStore::new(db.pool.clone());
        let scope = executor_scope();
        let (a,b) = tokio::join!(executor.claim_prepared(&scope,"claimer-a",60_000), executor.claim_prepared(&scope,"claimer-b",60_000));
        let leases: Vec<_> = [a.map_err(debug_error)?, b.map_err(debug_error)?].into_iter().flatten().collect();
        assert_eq!(leases.len(), 1); assert_eq!(leases[0].output_index,0);
        assert_eq!(tasks.summarize_execution_queue_readiness(60_000).await.map_err(debug_error)?.prepared_executions, 0);
        let forged = sqlx::query("UPDATE executor_executions SET state='leased',executor_owner='forged',lease_epoch=1,lease_expires_at_ms=$2+60000,leased_at_ms=$2,updated_at_ms=$2 WHERE executor_execution_id=(SELECT member.executor_execution_id FROM dreamina_native_batch_members member JOIN provider_submissions submission USING(submission_id) WHERE submission.job_id=$1 AND member.output_index=1)").bind(work.job_id).bind(database_now(&db.pool).await?).execute(&db.pool).await;
        assert!(forged.is_err());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dreamina_native_batch_members").fetch_one(&db.pool).await.map_err(debug_error)?; assert_eq!(count,10);
        Ok(())
    }.await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn frozen_version_one_keeps_legacy_fanout() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        prepare(&db.pool, 3, 1).await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM dreamina_native_batches")
            .fetch_one(&db.pool)
            .await
            .map_err(debug_error)?;
        assert_eq!(count, 0);
        let executor = PostgresExecutorSubmissionStore::new(db.pool.clone());
        // The profile permits two concurrent actual invocations, as before.
        for index in 0..2 {
            let lease = executor
                .claim_prepared(&executor_scope(), &format!("legacy-{index}"), 60000)
                .await
                .map_err(debug_error)?
                .ok_or("legacy output missing")?;
            assert_eq!(lease.output_index, index);
        }
        Ok(())
    }
    .await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn leader_local_failure_resolves_all_unlaunched_followers_atomically() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        let work = prepare(&db.pool, 3, 2).await?;
        let lease = launch(&db.pool).await?;
        PostgresExecutorSubmissionStore::new(db.pool.clone())
            .record_outcome(
                &lease,
                &ExecutorSubmissionOutcome::Failed {
                    error_code: "offline_rejected".into(),
                },
            )
            .await
            .map_err(debug_error)?;
        assert_states(&db.pool, work.job_id, &["failed", "failed", "failed"]).await?;
        let facts: i64 = sqlx::query_scalar("SELECT count(*) FROM executor_terminal_reductions")
            .fetch_one(&db.pool)
            .await
            .map_err(debug_error)?;
        assert_eq!(facts, 3);
        let calls: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_remote_submit_intents")
            .fetch_one(&db.pool)
            .await
            .map_err(debug_error)?;
        assert_eq!(calls, 0);
        Ok(())
    }
    .await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn partial_result_metadata_survives_store_restart_and_rejects_changed_mapping() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        let work = prepare(&db.pool, 3, 2).await?;
        let leader = launch(&db.pool).await?;
        let lease = attach(&db.pool, &work, &leader).await?;
        let tasks = PostgresProviderTaskStore::new(db.pool.clone());
        let members = tasks
            .native_batch_members(&lease)
            .await
            .map_err(debug_error)?
            .ok_or("members missing")?;
        tasks
            .publish_native_batch_artifact(
                &lease,
                1,
                &authority(members[1].executor_execution_id, 'b'),
            )
            .await
            .map_err(debug_error)?;
        tasks
            .record_native_batch_failure(&lease, 2, "dreamina_native_batch_result_missing")
            .await
            .map_err(debug_error)?;
        drop(tasks);
        let recovered = PostgresProviderTaskStore::new(db.pool.clone());
        let members = recovered
            .native_batch_members(&lease)
            .await
            .map_err(debug_error)?
            .ok_or("recovered members missing")?;
        assert!(members[1].committed_artifact.is_some());
        assert_eq!(
            members[2].committed_failure_code.as_deref(),
            Some("dreamina_native_batch_result_missing")
        );
        recovered
            .publish_native_batch_artifact(
                &lease,
                1,
                &authority(members[1].executor_execution_id, 'b'),
            )
            .await
            .map_err(debug_error)?;
        assert!(
            recovered
                .publish_native_batch_artifact(
                    &lease,
                    1,
                    &authority(members[1].executor_execution_id, 'c')
                )
                .await
                .is_err()
        );
        recovered
            .record_observation(
                &lease,
                &ProviderTaskObservation {
                    event_identity: "offline-provider-failed".into(),
                    source: ProviderTaskObservationSource::Poll,
                    outcome: ProviderTaskObservationOutcome::Failed {
                        error_code: "offline_provider_failure".into(),
                    },
                },
            )
            .await
            .map_err(debug_error)?;
        assert_states(&db.pool, work.job_id, &["failed", "succeeded", "failed"]).await?;
        let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_remote_tasks")
            .fetch_one(&db.pool)
            .await
            .map_err(debug_error)?;
        assert_eq!(operations, 1);
        Ok(())
    }
    .await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn unknown_submit_receipt_restart_never_resends_and_deadline_resolves_all_members()
-> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        let work = prepare(&db.pool, 3, 2).await?;
        let leader = launch(&db.pool).await?;
        let tasks = PostgresProviderTaskStore::new(db.pool.clone());
        let reservation = reservation_for(&work, &leader, 2000)?;
        tasks
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        tasks
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let binding = tasks
            .load_submit_intent(leader.submission_id)
            .await
            .map_err(debug_error)?
            .ok_or("intent missing")?
            .execution_binding_sha256;
        tasks
            .record_submit_failure(&RemoteTaskSubmitFailure {
                submission_id: leader.submission_id,
                executor_execution_id: leader.executor_execution_id,
                executor_owner: leader.executor_owner.clone(),
                executor_lease_epoch: leader.executor_lease_epoch,
                kind: ProviderSubmitFailureKind::OutcomeUnknown,
                event_identity: "offline-lost-receipt".into(),
                error_code: "offline_submit_receipt_lost".into(),
                execution_binding_sha256: binding,
                recovery_fence: None,
            })
            .await
            .map_err(debug_error)?;
        drop(tasks);
        let recovered = PostgresProviderTaskStore::new(db.pool.clone());
        assert!(!matches!(
            recovered
                .acquire_submit(&reservation)
                .await
                .map_err(debug_error)?,
            ProviderSubmitAcquire::Dispatch(_)
        ));
        assert!(
            PostgresExecutorSubmissionStore::new(db.pool.clone())
                .claim_prepared(&executor_scope(), "no-resend-after-crash", 60000)
                .await
                .map_err(debug_error)?
                .is_none()
        );
        let deadline: i64 = sqlx::query_scalar(
            "SELECT provider_deadline_at_ms FROM provider_submit_recoveries WHERE submission_id=$1",
        )
        .bind(leader.submission_id)
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        sleep_until_database_time(&db.pool, deadline + 1).await?;
        assert!(
            recovered
                .resolve_due_submit_deadline(&poll_scope())
                .await
                .map_err(debug_error)?
                .is_some()
        );
        assert_states(
            &db.pool,
            work.job_id,
            &["uncertain", "uncertain", "uncertain"],
        )
        .await?;
        let intents: i64 =
            sqlx::query_scalar("SELECT count(*) FROM provider_remote_submit_intents")
                .fetch_one(&db.pool)
                .await
                .map_err(debug_error)?;
        assert_eq!(intents, 1);
        let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_remote_tasks")
            .fetch_one(&db.pool)
            .await
            .map_err(debug_error)?;
        assert_eq!(operations, 0);
        Ok(())
    }
    .await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn remote_task_timeout_preserves_published_member_and_never_requeues_followers() -> TestResult
{
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        let work = prepare(&db.pool, 3, 2).await?;
        let leader = launch(&db.pool).await?;
        let lease = attach_with_timeout(&db.pool, &work, &leader, 2000).await?;
        let tasks = PostgresProviderTaskStore::new(db.pool.clone());
        let members = tasks
            .native_batch_members(&lease)
            .await
            .map_err(debug_error)?
            .ok_or("members missing")?;
        tasks
            .publish_native_batch_artifact(
                &lease,
                1,
                &authority(members[1].executor_execution_id, 'b'),
            )
            .await
            .map_err(debug_error)?;
        let deadline: i64 = sqlx::query_scalar(
            "SELECT provider_deadline_at_ms FROM provider_remote_tasks WHERE submission_id=$1",
        )
        .bind(leader.submission_id)
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        sleep_until_database_time(&db.pool, deadline + 1).await?;
        assert!(
            tasks
                .resolve_due_remote_task_deadline(&poll_scope())
                .await
                .map_err(debug_error)?
                .is_some()
        );
        assert_states(
            &db.pool,
            work.job_id,
            &["uncertain", "succeeded", "uncertain"],
        )
        .await?;
        assert!(
            PostgresExecutorSubmissionStore::new(db.pool.clone())
                .claim_prepared(&executor_scope(), "no-resubmit-after-timeout", 60000)
                .await
                .map_err(debug_error)?
                .is_none()
        );
        let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_remote_tasks")
            .fetch_one(&db.pool)
            .await
            .map_err(debug_error)?;
        assert_eq!(operations, 1);
        Ok(())
    }
    .await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn native_batch_success_requires_every_member_result_and_membership_is_immutable()
-> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        let work = prepare(&db.pool, 2, 2).await?;
        let leader = launch(&db.pool).await?;
        let lease = attach(&db.pool, &work, &leader).await?;
        let tasks = PostgresProviderTaskStore::new(db.pool.clone());
        let members = tasks
            .native_batch_members(&lease)
            .await
            .map_err(debug_error)?
            .ok_or("members missing")?;
        let direct_manifest = sqlx::query("INSERT INTO executor_result_manifests (manifest_id, artifact_authority_id, executor_execution_id, submission_id, created_at_ms) VALUES ($1, $2, $2, $1, $3)")
            .bind(leader.submission_id).bind(leader.executor_execution_id).bind(database_now(&db.pool).await?)
            .execute(&db.pool).await.expect_err("leader-first manifest must be rejected");
        assert!(matches!(direct_manifest, sqlx::Error::Database(error) if error.message().contains("native leader artifact requires every follower result first")));
        let immutable = sqlx::query(
            "UPDATE dreamina_native_batch_members SET output_index=2 WHERE submission_id=$1",
        )
        .bind(members[1].submission_id)
        .execute(&db.pool)
        .await;
        assert!(immutable.is_err());
        assert!(
            tasks
                .publish_artifact_authority(&lease, &authority(leader.executor_execution_id, 'a'))
                .await
                .is_err()
        );
        assert_states(&db.pool, work.job_id, &["provider_waiting", "prepared"]).await?;
        tasks
            .record_native_batch_failure(&lease, 1, "dreamina_native_batch_result_missing")
            .await
            .map_err(debug_error)?;
        let publication = tasks
            .publish_artifact_authority(&lease, &authority(leader.executor_execution_id, 'a'))
            .await
            .map_err(debug_error)?;
        let terminal = ProviderTaskObservation {
            event_identity: "offline-leader-success".into(),
            source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::ArtifactReady {
                artifact_ref: format!("manifest:{}", publication.manifest().manifest_id().simple()),
                publication,
            },
        };
        assert!(
            tasks
                .record_native_batch_failure(&lease, 0, "dreamina_native_batch_result_missing")
                .await
                .is_err()
        );
        tasks
            .record_observation(&lease, &terminal)
            .await
            .map_err(debug_error)?;
        assert_states(&db.pool, work.job_id, &["succeeded", "failed"]).await?;
        let immutable =
            sqlx::query("DELETE FROM dreamina_native_batch_results WHERE submission_id=$1")
                .bind(members[1].submission_id)
                .execute(&db.pool)
                .await;
        assert!(immutable.is_err());
        Ok(())
    }
    .await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn leader_last_published_batch_recovers_after_crash_at_deadline_without_resubmission()
-> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        seed_dreamina_execution_profile(&db.pool).await?;
        let work = prepare(&db.pool, 3, 2).await?;
        let leader = launch(&db.pool).await?;
        let lease = attach_with_timeout(&db.pool, &work, &leader, 2000).await?;
        let tasks = PostgresProviderTaskStore::new(db.pool.clone());
        let members = tasks
            .native_batch_members(&lease)
            .await
            .map_err(debug_error)?
            .ok_or("members missing")?;
        tasks
            .publish_native_batch_artifact(
                &lease,
                1,
                &authority(members[1].executor_execution_id, 'b'),
            )
            .await
            .map_err(debug_error)?;
        // An incomplete batch must reject both direct authority and manifest writes.
        assert!(
            tasks
                .publish_artifact_authority(&lease, &authority(leader.executor_execution_id, 'a'))
                .await
                .is_err()
        );
        tasks
            .record_native_batch_failure(&lease, 2, "dreamina_native_batch_result_missing")
            .await
            .map_err(debug_error)?;
        tasks
            .publish_artifact_authority(&lease, &authority(leader.executor_execution_id, 'a'))
            .await
            .map_err(debug_error)?;
        drop(tasks); // crash before the leader terminal observation
        let deadline: i64 = sqlx::query_scalar(
            "SELECT provider_deadline_at_ms FROM provider_remote_tasks WHERE submission_id=$1",
        )
        .bind(leader.submission_id)
        .fetch_one(&db.pool)
        .await
        .map_err(debug_error)?;
        sleep_until_database_time(&db.pool, deadline + 1).await?;
        let recovered = PostgresProviderTaskStore::new(db.pool.clone());
        assert!(
            recovered
                .resolve_due_remote_task_deadline(&poll_scope())
                .await
                .map_err(debug_error)?
                .is_some()
        );
        assert_states(&db.pool, work.job_id, &["succeeded", "succeeded", "failed"]).await?;
        assert!(
            recovered
                .resolve_due_remote_task_deadline(&poll_scope())
                .await
                .map_err(debug_error)?
                .is_none()
        );
        assert!(
            PostgresExecutorSubmissionStore::new(db.pool.clone())
                .claim_prepared(
                    &executor_scope(),
                    "no-resubmit-after-complete-publication",
                    60000
                )
                .await
                .map_err(debug_error)?
                .is_none()
        );
        let operations: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_remote_tasks")
            .fetch_one(&db.pool)
            .await
            .map_err(debug_error)?;
        assert_eq!(operations, 1);
        let reductions: i64 =
            sqlx::query_scalar("SELECT count(*) FROM executor_terminal_reductions")
                .fetch_one(&db.pool)
                .await
                .map_err(debug_error)?;
        assert_eq!(reductions, 3);
        Ok(())
    }
    .await;
    db.cleanup().await?;
    result
}
