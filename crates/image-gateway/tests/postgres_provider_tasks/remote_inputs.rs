use super::*;
use gpt_image_2_gateway::ExecutorLaunchContextStore;

#[tokio::test]
async fn remote_submit_inputs_keep_job_session_binding_across_fresh_and_recovery() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let executor = seed_running_submission_with_lease(&database.pool, "input-recovery", 500).await?;
        let other = seed_running_submission_with_lease(&database.pool, "other-input-job", 500).await?;
        let session: Uuid = sqlx::query_scalar("SELECT admission_session_id FROM job_payloads WHERE job_id = $1")
            .bind(executor.job_id).fetch_one(&database.pool).await.map_err(debug_error)?;
        for job in [executor.job_id, other.job_id] {
            sqlx::query(
                "INSERT INTO job_input_manifests (job_id, admission_session_id, manifest_schema, manifest_hash, input_count, created_at_ms) SELECT job_id, admission_session_id, 'remote-input-test.v1', repeat('a', 64), CASE WHEN job_id = $2 THEN 3 ELSE 1 END, created_at_ms FROM job_payloads WHERE job_id = $1"
            ).bind(job).bind(executor.job_id).execute(&database.pool).await.map_err(debug_error)?;
        }
        for (job, role, index, media) in [
            (executor.job_id, "image", 0_i16, "image/png"),
            (executor.job_id, "video", 1, "video/mp4"),
            (executor.job_id, "audio", 2, "audio/wav"),
            (other.job_id, "image", 0, "image/png"),
        ] {
            sqlx::query(
                "INSERT INTO job_input_objects (input_id, job_id, admission_session_id, role, input_index, media_type, storage_backend, object_key, sha256_hex, byte_size, created_at_ms) SELECT $1, job_id, admission_session_id, $2, $3, $4, 'test', $5, repeat('b', 64), 42, created_at_ms FROM job_payloads WHERE job_id = $6"
            ).bind(Uuid::new_v4()).bind(role).bind(index).bind(media)
                .bind(format!("inputs/{job}/{index}" )).bind(job)
                .execute(&database.pool).await.map_err(debug_error)?;
        }
        let fresh = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .load_launch_context(&executor).await.map_err(debug_error)?;
        require(fresh.inputs().len() == 3
            && fresh.inputs().iter().all(|input| input.blob().key.admission_session_id == session)
            && fresh.inputs().iter().map(|input| (input.role(), input.index())).collect::<Vec<_>>()
                == vec![("image", 0), ("video", 1), ("audio", 2)], "fresh inputs escaped job/session order")?;
        let store = PostgresProviderTaskStore::new(database.pool.clone());
        let reservation = reservation_request(&executor);
        store.reserve_submit(&reservation).await.map_err(debug_error)?;
        store.start_submit(&reservation).await.map_err(debug_error)?;
        sleep_until_database_time(&database.pool, executor.executor_lease_expires_at_ms + 20).await?;
        let recovery = store.claim_submit_recovery(&claim_scope(), "input-recovery-worker", "input-recovery-claim", 10_000)
            .await.map_err(debug_error)?.ok_or_else(|| "input recovery not claimable".to_owned())?;
        require(recovery.intent.submission_id == executor.submission_id
            && recovery.inputs() == fresh.inputs(), "recovery changed or lost frozen job inputs")?;
        let replay = store.claim_submit_recovery(&claim_scope(), "input-recovery-worker", "input-recovery-claim", 10_000)
            .await.map_err(debug_error)?.ok_or_else(|| "input recovery replay not claimable".to_owned())?;
        require(replay == recovery, "recovery claim replay changed frozen inputs")
    }.await;
    database.cleanup().await?;
    result
}
