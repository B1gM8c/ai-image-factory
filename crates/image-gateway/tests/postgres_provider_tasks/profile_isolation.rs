use super::*;

async fn seed_sibling_profile(pool: &PgPool) -> TestResult<ProviderTaskClaimScope> {
    let execution_profile_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles
          (execution_profile_id, profile_key, provider_id, command_schema,
           operation_id, operation_descriptor_revision, operation_descriptor_sha256_v1,
           completion_mode, idempotency_mode, adapter_revision, credential_pool_id,
           provider_account_id, credential_ref, credential_revision, resource_policy_id,
           resource_policy_revision, state, created_at_ms, updated_at_ms)
        SELECT $1, 'provider-sibling-video-profile', provider_id, command_schema,
               'videos.generations', 'provider-test/videos.generations/v1', operation_descriptor_sha256_v1,
               completion_mode, idempotency_mode, adapter_revision, credential_pool_id,
               provider_account_id, credential_ref, credential_revision, resource_policy_id,
               resource_policy_revision, state, created_at_ms, updated_at_ms
        FROM provider_execution_profiles WHERE execution_profile_id = $2
        "#,
    )
    .bind(execution_profile_id)
    .bind(PROFILE_ID)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    Ok(ProviderTaskClaimScope {
        execution_profile_id,
        ..claim_scope()
    })
}

#[tokio::test]
async fn poll_claim_filters_execution_profile_before_candidate_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let sibling = seed_sibling_profile(&database.pool).await?;
        // A full earlier window from the same account must not starve this profile.
        for index in 0..64 {
            seed_attached_remote_task(
                &database.pool,
                &store,
                &format!("profile-poll-other-{index}"),
                &format!("profile-poll-other-{index}"),
                300_000,
                0,
            )
            .await?;
        }
        let target = seed_attached_remote_task_for_runtime_profile(
            &database.pool,
            &store,
            "profile-poll-target",
            "profile-poll-target",
            300_000,
            0,
            sibling.execution_profile_id,
            "provider-test",
            "model-test",
            "provider-command-v1",
            "provider-test-adapter-v1",
        )
        .await?;
        let claimed = store
            .claim_due(&sibling, "sibling-poller", 5_000)
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "sibling poll was starved by another profile".to_string())?;
        require(
            claimed.task.submission_id == target.submission_id
                && claimed.context().execution_profile_id() == sibling.execution_profile_id,
            "poll claimant crossed its execution profile",
        )?;
        require(
            store.claim_due(&sibling, "sibling-poller", 5_000).await.map_err(debug_error)?.is_none(),
            "poll claimant consumed another profile's remaining tasks",
        )?;
        let other = store.claim_due(&claim_scope(), "other-poller", 5_000).await.map_err(debug_error)?
            .ok_or_else(|| "other profile's polls disappeared".to_string())?;
        require(other.context().execution_profile_id() == PROFILE_ID, "reverse poll claim crossed profiles")?;
        let allocations: i32 = sqlx::query_scalar(
            "SELECT allocated_count FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
        )
        .bind(POLICY_ID)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(allocations == 65, "profile claims changed the shared account capacity count")
    }.await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn submit_recovery_filters_execution_profile_before_candidate_window_and_replay() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let sibling = seed_sibling_profile(&database.pool).await?;
        for index in 0..64 {
            let lease = seed_running_submission_with_lease(
                &database.pool,
                &format!("profile-recovery-other-{index}"),
                200,
            )
            .await?;
            let reservation = reservation_request(&lease);
            store
                .reserve_submit(&reservation)
                .await
                .map_err(debug_error)?;
            store
                .start_submit(&reservation)
                .await
                .map_err(debug_error)?;
        }
        let target = seed_running_submission_for_profile(
            &database.pool,
            "profile-recovery-target",
            200,
            sibling.execution_profile_id,
            "provider-test-adapter-v1",
        )
        .await?;
        let reservation = reservation_request(&target);
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        sleep_until_database_time(&database.pool, target.executor_lease_expires_at_ms + 20).await?;
        let claimed = store
            .claim_submit_recovery(
                &sibling,
                "sibling-recoverer",
                "profile-recovery-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "sibling recovery was starved by another profile".to_string())?;
        require(
            claimed.intent.submission_id == target.submission_id
                && claimed.context().execution_profile_id() == sibling.execution_profile_id,
            "recovery claimant crossed its execution profile",
        )?;
        require(
            matches!(
                store
                    .claim_submit_recovery(
                        &claim_scope(),
                        "sibling-recoverer",
                        "profile-recovery-claim",
                        5_000
                    )
                    .await,
                Err(ProviderTaskStoreError::Conflict)
            ),
            "recovery command replay crossed execution profiles",
        )?;
        let replay = store
            .claim_submit_recovery(
                &sibling,
                "sibling-recoverer",
                "profile-recovery-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "same-profile replay disappeared".to_string())?;
        require(
            replay.intent.submission_id == target.submission_id,
            "same-profile replay changed its target",
        )?;
        require(
            store
                .claim_submit_recovery(
                    &sibling,
                    "sibling-recoverer",
                    "profile-recovery-empty",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "recovery claimant consumed another profile's remaining tasks",
        )?;
        let other = store
            .claim_submit_recovery(
                &claim_scope(),
                "other-recoverer",
                "profile-other-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "other profile's recoveries disappeared".to_string())?;
        require(
            other.context().execution_profile_id() == PROFILE_ID,
            "reverse recovery claim crossed profiles",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn deadline_and_capacity_claims_keep_execution_profile_scope() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let sibling = seed_sibling_profile(&database.pool).await?;
        let submit = seed_running_submission(&database.pool, "profile-submit-deadline").await?;
        let mut reservation = reservation_request(&submit);
        reservation.provider_timeout_ms = 100;
        store
            .reserve_submit(&reservation)
            .await
            .map_err(debug_error)?;
        store
            .start_submit(&reservation)
            .await
            .map_err(debug_error)?;
        let remote = seed_attached_remote_task(
            &database.pool,
            &store,
            "profile-remote-deadline",
            "profile-remote-deadline",
            100,
            0,
        )
        .await?;
        let deadline: i64 = sqlx::query_scalar(
            "SELECT MAX(provider_deadline_at_ms) FROM provider_submit_recoveries",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        sleep_until_database_time(&database.pool, deadline + 20).await?;
        require(
            store
                .resolve_due_submit_deadline(&sibling)
                .await
                .map_err(debug_error)?
                .is_none()
                && store
                    .resolve_due_remote_task_deadline(&sibling)
                    .await
                    .map_err(debug_error)?
                    .is_none(),
            "deadline resolver consumed another profile's task",
        )?;
        let resolved_submit = store
            .resolve_due_submit_deadline(&claim_scope())
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "matching submit deadline disappeared".to_string())?;
        require(
            resolved_submit.submission_id == submit.submission_id,
            "wrong submit deadline resolved",
        )?;
        let resolved_remote = store
            .resolve_due_remote_task_deadline(&claim_scope())
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "matching remote deadline disappeared".to_string())?;
        require(
            resolved_remote.submission_id == remote.submission_id,
            "wrong remote deadline resolved",
        )?;
        require(
            store
                .claim_due_capacity_reconciliation(
                    &sibling,
                    "profile-reconciler",
                    "profile-capacity-empty",
                    5_000,
                )
                .await
                .map_err(debug_error)?
                .is_none(),
            "capacity claimant consumed another profile's task",
        )?;
        let lease = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "profile-reconciler",
                "profile-capacity-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "matching reconciliation disappeared".to_string())?;
        require(
            matches!(
                store
                    .claim_due_capacity_reconciliation(
                        &sibling,
                        "profile-reconciler",
                        "profile-capacity-claim",
                        5_000
                    )
                    .await,
                Err(ProviderTaskStoreError::Conflict)
            ),
            "capacity command replay crossed execution profiles",
        )?;
        let replay = store
            .claim_due_capacity_reconciliation(
                &claim_scope(),
                "profile-reconciler",
                "profile-capacity-claim",
                5_000,
            )
            .await
            .map_err(debug_error)?
            .ok_or_else(|| "same-profile capacity replay disappeared".to_string())?;
        require(
            replay == lease,
            "same-profile capacity replay changed its lease",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}
