use super::*;
use crate::provider_tasks::additional_image_artifact_id;

#[derive(sqlx::FromRow)]
struct AdditionalRow {
    source_index: i32,
    artifact_id: Uuid,
    storage_backend: String,
    storage_namespace: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
    media_type: String,
}

pub(super) async fn publish(
    store: &PostgresProviderTaskStore,
    lease: &ProviderTaskLease,
    authorities: &[(usize, ProviderArtifactAuthority)],
) -> Result<(), ProviderTaskStoreError> {
    validate_lease(lease, 1)?;
    if lease.task.provider_id != "dreamina-cli" || authorities.len() > 9 {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    for (offset, (source_index, authority)) in authorities.iter().enumerate() {
        let id = additional_image_artifact_id(lease.task.executor_execution_id, *source_index)
            .ok_or(ProviderTaskStoreError::InvalidInput)?;
        if *source_index != offset + 1
            || authority.object_key != executor_object_key(id)
            || !matches!(
                authority.media_type.as_str(),
                "image/png" | "image/jpeg" | "image/webp"
            )
        {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
    }
    let mut tx = store.pool.begin().await.map_err(unavailable)?;
    let (job_id, completion_deadline, now) = lock_image_task(&mut tx, lease).await?;
    let prepared: Option<String> = sqlx::query_scalar("SELECT cleanup_state FROM dreamina_image_stage_intents WHERE submission_id = $1 AND executor_execution_id = $2 AND job_id = $3 FOR UPDATE")
        .bind(lease.task.submission_id).bind(lease.task.executor_execution_id).bind(job_id)
        .fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if !matches!(prepared.as_deref(), Some("available" | "deleted")) {
        return Err(ProviderTaskStoreError::Conflict);
    }
    let expected_count = authorities.len() as i32 + 1;
    let existing: Option<(i32, String)> = sqlx::query_as(
        "SELECT returned_count, cleanup_state FROM dreamina_image_result_sets WHERE submission_id = $1 AND executor_execution_id = $2 AND job_id = $3",
    ).bind(lease.task.submission_id).bind(lease.task.executor_execution_id).bind(job_id)
        .fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if let Some((returned_count, cleanup_state)) = existing {
        let rows: Vec<AdditionalRow> = sqlx::query_as(
            "SELECT source_index, artifact_id, storage_backend, storage_namespace, object_key, sha256_hex, byte_size, media_type FROM dreamina_image_additional_artifacts WHERE submission_id = $1 ORDER BY source_index",
        ).bind(lease.task.submission_id).fetch_all(&mut *tx).await.map_err(unavailable)?;
        if returned_count != expected_count
            || cleanup_state != "available"
            || rows.len() != authorities.len()
            || rows
                .iter()
                .zip(authorities)
                .any(|(row, (index, authority))| {
                    usize::try_from(row.source_index).ok() != Some(*index)
                        || Some(row.artifact_id)
                            != additional_image_artifact_id(
                                lease.task.executor_execution_id,
                                *index,
                            )
                        || row.storage_backend != authority.storage_backend
                        || row.storage_namespace != authority.storage_namespace
                        || row.object_key != authority.object_key
                        || row.sha256_hex != authority.sha256_hex
                        || u64::try_from(row.byte_size).ok() != Some(authority.byte_size)
                        || row.media_type != authority.media_type
                })
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
    } else {
        // Failed or interrupted leaders also have bounded, independently recoverable cleanup.
        let cleanup_after: i64 = sqlx::query_scalar(
            "SELECT $1::BIGINT + retain_for_ms + read_drain_ms FROM artifact_retention_policies WHERE policy_key = 'default'",
        ).bind(completion_deadline).fetch_one(&mut *tx).await.map_err(unavailable)?;
        sqlx::query("INSERT INTO dreamina_image_result_sets (submission_id, executor_execution_id, job_id, returned_count, created_at_ms, cleanup_after_ms) VALUES ($1,$2,$3,$4,$5,$6)")
            .bind(lease.task.submission_id).bind(lease.task.executor_execution_id).bind(job_id)
            .bind(expected_count).bind(now).bind(cleanup_after).execute(&mut *tx).await.map_err(storage_conflict)?;
        for (index, authority) in authorities {
            sqlx::query("INSERT INTO dreamina_image_additional_artifacts (submission_id,executor_execution_id,job_id,source_index,artifact_id,storage_backend,storage_namespace,object_key,sha256_hex,byte_size,media_type) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
                .bind(lease.task.submission_id).bind(lease.task.executor_execution_id).bind(job_id)
                .bind(*index as i32).bind(additional_image_artifact_id(lease.task.executor_execution_id,*index).ok_or(ProviderTaskStoreError::InvalidInput)?)
                .bind(&authority.storage_backend).bind(&authority.storage_namespace).bind(&authority.object_key)
                .bind(&authority.sha256_hex).bind(i64::try_from(authority.byte_size).map_err(|_| ProviderTaskStoreError::InvalidInput)?)
                .bind(&authority.media_type).execute(&mut *tx).await.map_err(storage_conflict)?;
        }
    }
    if prepared.as_deref() == Some("available") {
        let affected = sqlx::query("UPDATE dreamina_image_stage_intents SET cleanup_state = 'deleted' WHERE submission_id = $1 AND cleanup_state = 'available'")
            .bind(lease.task.submission_id).execute(&mut *tx).await.map_err(unavailable)?.rows_affected();
        if affected != 1 {
            return Err(ProviderTaskStoreError::Conflict);
        }
    }
    tx.commit().await.map_err(storage_conflict)
}

async fn lock_image_task(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ProviderTaskLease,
) -> Result<(Uuid, i64, i64), ProviderTaskStoreError> {
    let locked: Option<(Uuid, i64, i64)> = sqlx::query_as(
        r#"SELECT submission.job_id,
              provider_task_completion_deadline(task.submission_id, task.provider_deadline_at_ms),
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
           FROM provider_remote_tasks task
           JOIN provider_submissions submission USING (submission_id, executor_execution_id)
           JOIN jobs job ON job.job_id = submission.job_id
           WHERE task.submission_id = $1 AND task.executor_execution_id = $2
             AND task.provider_id = 'dreamina-cli' AND task.state = 'provider_waiting'
             AND task.poll_owner = $3 AND task.poll_lease_epoch = $4
             AND task.poll_lease_expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
             AND provider_task_completion_deadline(task.submission_id, task.provider_deadline_at_ms)
                 > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
             AND job.operation IN ('generation','edit') AND job.output_count = 1
             AND NOT EXISTS (SELECT 1 FROM dreamina_native_batches batch WHERE batch.job_id = job.job_id)
             AND NOT EXISTS (SELECT 1 FROM provider_remote_video_results video WHERE video.submission_id = task.submission_id)
           FOR UPDATE OF task"#,
    ).bind(lease.task.submission_id).bind(lease.task.executor_execution_id)
        .bind(&lease.poll_owner).bind(lease.poll_lease_epoch)
        .fetch_optional(&mut **tx).await.map_err(unavailable)?;
    locked.ok_or(ProviderTaskStoreError::StaleLease)
}

pub(super) async fn prepare(
    store: &PostgresProviderTaskStore,
    lease: &ProviderTaskLease,
) -> Result<(), ProviderTaskStoreError> {
    validate_lease(lease, 1)?;
    let mut tx = store.pool.begin().await.map_err(unavailable)?;
    let (job_id, deadline, _) = lock_image_task(&mut tx, lease).await?;
    let existing: Option<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT executor_execution_id, job_id, cleanup_state FROM dreamina_image_stage_intents WHERE submission_id = $1 FOR UPDATE",
    ).bind(lease.task.submission_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
    if let Some((execution_id, stored_job, state)) = existing {
        if execution_id != lease.task.executor_execution_id
            || stored_job != job_id
            || state == "deleting"
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        if state == "deleted" {
            let sealed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dreamina_image_result_sets WHERE submission_id = $1 AND cleanup_state = 'available')")
                .bind(lease.task.submission_id).fetch_one(&mut *tx).await.map_err(unavailable)?;
            if !sealed {
                return Err(ProviderTaskStoreError::Conflict);
            }
        }
    } else {
        sqlx::query("INSERT INTO dreamina_image_stage_intents(submission_id,executor_execution_id,job_id,cleanup_after_ms) SELECT $1,$2,$3,$4::BIGINT + retain_for_ms + read_drain_ms FROM artifact_retention_policies WHERE policy_key = 'default'")
            .bind(lease.task.submission_id).bind(lease.task.executor_execution_id).bind(job_id).bind(deadline)
            .execute(&mut *tx).await.map_err(storage_conflict)?;
    }
    tx.commit().await.map_err(unavailable)
}
