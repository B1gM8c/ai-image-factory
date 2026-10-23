use super::*;
use crate::provider_tasks::NativeBatchMember;

#[derive(sqlx::FromRow)]
struct MemberRow {
    output_index: i32,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    output_id: Uuid,
    job_id: Uuid,
    sha256_hex: Option<String>,
    byte_size: Option<i64>,
    media_type: Option<String>,
    error_code: Option<String>,
}

impl PostgresProviderTaskStore {
    pub async fn native_batch_members(
        &self,
        lease: &ProviderTaskLease,
    ) -> Result<Option<Vec<NativeBatchMember>>, ProviderTaskStoreError> {
        validate_lease(lease, 1)?;
        let rows: Vec<MemberRow> = sqlx::query_as(
            r#"SELECT member.output_index, member.submission_id, member.executor_execution_id,
                     submission.output_id, submission.job_id, authority.sha256_hex,
                     authority.byte_size, authority.media_type, result.error_code
               FROM dreamina_native_batches batch
               JOIN dreamina_native_batch_members member USING (leader_submission_id)
               JOIN provider_submissions submission ON submission.submission_id = member.submission_id
               LEFT JOIN executor_artifact_authorities authority
                 ON authority.executor_execution_id = member.executor_execution_id
               LEFT JOIN dreamina_native_batch_results result ON result.submission_id = member.submission_id
               WHERE batch.leader_submission_id = $1
               ORDER BY member.output_index"#,
        )
        .bind(lease.task.submission_id)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        if rows.is_empty() {
            return Ok(None);
        }
        rows.into_iter()
            .map(|row| {
                let committed_artifact = match (row.sha256_hex, row.byte_size, row.media_type) {
                    (Some(sha256_hex), Some(byte_size), Some(media_type)) => {
                        Some(ProviderArtifactPublication {
                            manifest: ExecutorResultManifest::new(
                                row.submission_id,
                                row.executor_execution_id,
                            )
                            .ok_or(ProviderTaskStoreError::Conflict)?,
                            sha256_hex,
                            byte_size: u64::try_from(byte_size)
                                .map_err(|_| ProviderTaskStoreError::Conflict)?,
                            media_type,
                        })
                    }
                    (None, None, None) => None,
                    _ => return Err(ProviderTaskStoreError::Conflict),
                };
                Ok(NativeBatchMember {
                    output_index: row.output_index,
                    submission_id: row.submission_id,
                    executor_execution_id: row.executor_execution_id,
                    output_id: row.output_id,
                    job_id: row.job_id,
                    committed_artifact,
                    committed_failure_code: row.error_code,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub async fn publish_native_batch_artifact(
        &self,
        lease: &ProviderTaskLease,
        output_index: i32,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ProviderArtifactPublication, ProviderTaskStoreError> {
        validate_lease(lease, 1)?;
        if !(1..=9).contains(&output_index) {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let member = lock_member(&mut tx, lease, output_index).await?;
        let manifest = ExecutorResultManifest::new(member.0, member.1)
            .ok_or(ProviderTaskStoreError::Conflict)?;
        if authority.object_key != executor_object_key(member.1) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let existing: Option<(String, String, String, i64, String)> = sqlx::query_as(
            "SELECT storage_backend, storage_namespace, sha256_hex, byte_size, media_type FROM executor_artifact_authorities WHERE executor_execution_id = $1 AND object_key = $2",
        ).bind(member.1).bind(&authority.object_key).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if let Some((backend, namespace, sha256_hex, byte_size, media_type)) = existing {
            if backend != authority.storage_backend
                || namespace != authority.storage_namespace
                || sha256_hex != authority.sha256_hex
                || u64::try_from(byte_size).ok() != Some(authority.byte_size)
                || media_type != authority.media_type
            {
                return Err(ProviderTaskStoreError::Conflict);
            }
        } else {
            sqlx::query("INSERT INTO executor_artifact_authorities (authority_id, executor_execution_id, submission_id, output_id, job_id, storage_backend, storage_namespace, object_key, sha256_hex, byte_size, media_type, created_at_ms) VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)")
                .bind(member.1).bind(member.0).bind(member.2).bind(member.3)
                .bind(&authority.storage_backend).bind(&authority.storage_namespace).bind(&authority.object_key)
                .bind(&authority.sha256_hex).bind(i64::try_from(authority.byte_size).map_err(|_| ProviderTaskStoreError::InvalidInput)?)
                .bind(&authority.media_type).bind(member.4).execute(&mut *tx).await.map_err(storage_conflict)?;
        }
        sqlx::query("INSERT INTO executor_result_manifests (manifest_id, artifact_authority_id, executor_execution_id, submission_id, created_at_ms) VALUES ($1, $2, $2, $1, $3) ON CONFLICT (manifest_id) DO NOTHING")
            .bind(member.0).bind(member.1).bind(member.4).execute(&mut *tx).await.map_err(storage_conflict)?;
        persist_result(
            &mut tx,
            member.0,
            member.1,
            "succeeded",
            Some(member.0),
            None,
            member.4,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderArtifactPublication {
            manifest,
            sha256_hex: authority.sha256_hex.clone(),
            byte_size: authority.byte_size,
            media_type: authority.media_type.clone(),
        })
    }

    pub async fn record_native_batch_failure(
        &self,
        lease: &ProviderTaskLease,
        output_index: i32,
        error_code: &str,
    ) -> Result<(), ProviderTaskStoreError> {
        validate_lease(lease, 1)?;
        if !(1..=9).contains(&output_index) || !crate::executor::error_code_is_valid(error_code) {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let member = lock_member(&mut tx, lease, output_index).await?;
        persist_result(
            &mut tx,
            member.0,
            member.1,
            "failed",
            None,
            Some(error_code),
            member.4,
        )
        .await?;
        tx.commit().await.map_err(unavailable)
    }
}

async fn lock_member(
    tx: &mut Transaction<'_, Postgres>,
    lease: &ProviderTaskLease,
    output_index: i32,
) -> Result<(Uuid, Uuid, Uuid, Uuid, i64), ProviderTaskStoreError> {
    let task = sqlx::query_scalar::<_, bool>(
        r#"SELECT task.state = 'provider_waiting' AND task.poll_owner = $2
           AND task.poll_lease_epoch = $3 AND task.poll_lease_expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
           AND task.provider_deadline_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
           FROM provider_remote_tasks task WHERE task.submission_id = $1 AND task.executor_execution_id = $4 FOR UPDATE"#,
    ).bind(lease.task.submission_id).bind(&lease.poll_owner).bind(lease.poll_lease_epoch)
        .bind(lease.task.executor_execution_id).fetch_optional(&mut **tx).await.map_err(unavailable)?;
    if task != Some(true) {
        return Err(ProviderTaskStoreError::StaleLease);
    }
    let (submission_id, execution_id, output_id, job_id): (Uuid, Uuid, Uuid, Uuid) = sqlx::query_as(
        r#"SELECT member.submission_id, member.executor_execution_id, submission.output_id, submission.job_id
           FROM dreamina_native_batch_members member JOIN provider_submissions submission USING (submission_id)
           WHERE member.leader_submission_id = $1 AND member.output_index = $2 FOR UPDATE OF submission"#,
    ).bind(lease.task.submission_id).bind(output_index).fetch_optional(&mut **tx).await.map_err(unavailable)?
        .ok_or(ProviderTaskStoreError::InvalidInput)?;
    let now = database_now(tx).await?;
    Ok((submission_id, execution_id, output_id, job_id, now))
}

async fn persist_result(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
    execution_id: Uuid,
    state: &str,
    manifest_id: Option<Uuid>,
    error_code: Option<&str>,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    let existing: Option<(String, Option<Uuid>, Option<String>)> = sqlx::query_as(
        "SELECT resolved_state, result_manifest_id, error_code FROM dreamina_native_batch_results WHERE submission_id = $1",
    ).bind(submission_id).fetch_optional(&mut **tx).await.map_err(unavailable)?;
    if let Some((existing_state, existing_manifest, existing_error)) = existing {
        return if existing_state == state
            && existing_manifest == manifest_id
            && existing_error.as_deref() == error_code
        {
            Ok(())
        } else {
            Err(ProviderTaskStoreError::Conflict)
        };
    }
    sqlx::query("INSERT INTO dreamina_native_batch_results (submission_id, executor_execution_id, resolved_state, result_manifest_id, error_code, observed_at_ms) VALUES ($1, $2, $3, $4, $5, $6)")
        .bind(submission_id).bind(execution_id).bind(state).bind(manifest_id).bind(error_code).bind(now)
        .execute(&mut **tx).await.map_err(storage_conflict)?;
    Ok(())
}
