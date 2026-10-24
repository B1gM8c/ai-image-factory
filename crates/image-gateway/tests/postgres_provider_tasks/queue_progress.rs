use super::*;
use image_provider_sdk::ProviderTaskProgress;

#[tokio::test]
async fn progress_survives_restart_and_rejects_changed_replay_without_resubmission() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresProviderTaskStore::new(db.pool.clone());
        let submitted = seed_attached_remote_task(&db.pool, &store, "progress-worker", "progress-task", 60_000, 0).await?;
        let lease = store.claim_due(&claim_scope(), "progress-poller", 60_000).await.map_err(debug_error)?.ok_or("progress lease missing")?;
        let progress = ProviderTaskProgress {
            provider_status: "Queueing".to_owned(), queue_position: Some(3341),
            queue_total: Some(476_961), estimated_remaining_seconds: Some(27_131),
        };
        let observation = ProviderTaskObservation {
            event_identity: "poll:1:queue-progress".to_owned(), source: ProviderTaskObservationSource::Poll,
            outcome: ProviderTaskObservationOutcome::Waiting { poll_after_ms: 0 },
            progress: Some(progress.clone()),
        };
        store.record_observation(&lease, &observation).await.map_err(debug_error)?;
        let read = || sqlx::query_as::<_, (serde_json::Value, i64, i64)>(
            "SELECT observation.progress_json, observation.observed_at_ms, task.provider_deadline_at_ms FROM provider_remote_tasks task JOIN provider_task_observations observation ON observation.observation_id=task.state_observation_id AND observation.submission_id=task.submission_id WHERE task.submission_id=$1 AND observation.observed_state='provider_waiting'"
        ).bind(submitted.submission_id).fetch_one(&db.pool);
        let first = read().await.map_err(debug_error)?;
        require(first.0 == json!({"provider_status":"Queueing","queue_position":3341,"queue_total":476961,"estimated_remaining_seconds":27131}), "progress payload changed")?;
        require(first.1 > 0 && first.2 == lease.context().provider_deadline_at_ms(), "progress changed the frozen provider deadline")?;
        let restarted = PostgresProviderTaskStore::new(db.pool.clone());
        restarted.record_observation(&lease, &observation).await.map_err(debug_error)?;
        require(read().await.map_err(debug_error)? == first, "replay rewrote progress or observation time")?;
        for reported in [None, Some(ProviderTaskProgress {queue_position:Some(3340), ..progress.clone()})] {
            let changed = ProviderTaskObservation {progress:reported, ..observation.clone()};
            require(restarted.record_observation(&lease, &changed).await == Err(ProviderTaskStoreError::Conflict), "replay accepted changed progress")?;
        }
        let counts: (i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM provider_task_observations WHERE submission_id=$1 AND event_identity='poll:1:queue-progress'), (SELECT count(*) FROM provider_remote_submit_intents WHERE submission_id=$1)")
            .bind(submitted.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(counts == (1,1), "progress replay duplicated observations or submission")?;
        let next = restarted.claim_due(&claim_scope(), "progress-next", 60_000).await.map_err(debug_error)?.ok_or("next progress lease missing")?;
        let next_observation=ProviderTaskObservation {event_identity:"poll:2:queue-progress".to_owned(),progress:Some(ProviderTaskProgress{queue_position:Some(3339),estimated_remaining_seconds:None,..progress}),..observation};
        restarted.record_observation(&next,&next_observation).await.map_err(debug_error)?;
        let latest=read().await.map_err(debug_error)?;
        require(latest.0["queue_position"]==3339 && latest.0["estimated_remaining_seconds"].is_null() && latest.2==first.2, "latest state pointer retained stale progress or invented an estimate")?;
        require(sqlx::query("UPDATE provider_task_observations SET progress_json=NULL WHERE submission_id=$1").bind(submitted.submission_id).execute(&db.pool).await.is_err(), "progress evidence became mutable")?;
        Ok(())
    }.await;
    db.cleanup().await?;
    result
}

#[tokio::test]
async fn invalid_optional_progress_never_creates_terminal_or_billing_effects() -> TestResult {
    let Some(db) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result=async {
        let store=PostgresProviderTaskStore::new(db.pool.clone());
        let submitted=seed_attached_remote_task(&db.pool,&store,"invalid-progress-worker","invalid-progress",60_000,0).await?;
        let lease=store.claim_due(&claim_scope(),"invalid-progress-poller",60_000).await.map_err(debug_error)?.ok_or("lease missing")?;
        let invalid=ProviderTaskObservation {event_identity:"invalid-queue-progress".to_owned(),source:ProviderTaskObservationSource::Poll,outcome:ProviderTaskObservationOutcome::Waiting{poll_after_ms:0},progress:Some(ProviderTaskProgress{provider_status:"Queueing".to_owned(),queue_position:Some(100),queue_total:Some(1),estimated_remaining_seconds:Some(60)})};
        require(store.record_observation(&lease,&invalid).await==Err(ProviderTaskStoreError::InvalidInput),"invalid progress entered observations")?;
        let loaded=store.load(submitted.submission_id).await.map_err(debug_error)?.ok_or("task missing")?;
        require(loaded.state==ProviderTaskState::ProviderWaiting,"invalid metadata terminalized task")?;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM provider_task_observations WHERE submission_id=$1 AND event_identity='invalid-queue-progress'").bind(submitted.submission_id).fetch_one(&db.pool).await.map_err(debug_error)?;
        require(count==0,"invalid progress was persisted")?;
        for (index, progress) in [
            json!({"provider_status": "Queueing", "queue_position": -1}),
            json!({"provider_status": "Queueing", "queue_total": "many"}),
            json!({"provider_status": "Queueing", "estimated_remaining_seconds": 1.5}),
            json!({"provider_status": "Queueing", "prompt": "not progress"}),
            json!({"provider_status": "Queueing", "queue_position": 10, "queue_total": 1}),
        ].into_iter().enumerate() {
            let rejected = sqlx::query(
                "INSERT INTO provider_task_observations (observation_id,submission_id,executor_execution_id,event_identity,source,observed_state,effect_certainty,next_poll_at_ms,poll_owner,poll_lease_epoch,payload_hash,observed_at_ms,progress_json) SELECT $1,task.submission_id,task.executor_execution_id,$2,'poll','provider_waiting','not_applicable',observation.observed_at_ms,$3,$4,$5,observation.observed_at_ms,$6 FROM provider_remote_tasks task JOIN provider_task_observations observation ON observation.observation_id=task.state_observation_id WHERE task.submission_id=$7"
            ).bind(Uuid::new_v4()).bind(format!("invalid-direct-progress-{index}"))
                .bind(&lease.poll_owner).bind(lease.poll_lease_epoch).bind("a".repeat(64))
                .bind(progress).bind(submitted.submission_id).execute(&db.pool).await;
            require(rejected.err().as_ref().and_then(sqlx::Error::as_database_error)
                .and_then(|error|error.constraint())==Some("provider_task_progress_payload_check"),
                "database progress guard did not reject malformed metadata")?;
        }
        Ok(())
    }.await;
    db.cleanup().await?;
    result
}
