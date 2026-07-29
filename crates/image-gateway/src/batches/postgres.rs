use std::{
    collections::HashSet,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use super::{
    BatchExecutionSnapshot, BatchFileBlob, BatchFileBlobError, BatchFileBlobStore,
    BatchFinalizationLease, BatchRequestCounts, BatchRequestLease, BatchRequestState,
    BatchRequestSuccess, BatchResultRole, BatchService, BatchStatus, BatchWorkTarget,
    CreateProjectBatch, CreateProjectFile, DEFAULT_BATCH_RETENTION_SECONDS, MAX_BATCH_FILE_BYTES,
    MAX_BATCH_REQUESTS, MAX_BATCH_RESULT_FILE_BYTES, MAX_BATCH_STORED_RESULT_BYTES, MAX_FILE_BYTES,
    MAX_FILE_RETENTION_SECONDS, MIN_FILE_RETENTION_SECONDS, ProjectBatch, ProjectBatchPage,
    ProjectFile, ProjectFileCleanupLease, ProjectFilePage, ProjectFilePurpose, ProjectScope,
    ValidatedBatchLine,
};
use crate::ImageGatewayError;

const BATCH_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Clone)]
pub struct PostgresBatchService {
    pool: PgPool,
    blobs: Arc<dyn BatchFileBlobStore>,
}

impl PostgresBatchService {
    pub fn new(pool: PgPool, blobs: Arc<dyn BatchFileBlobStore>) -> Self {
        Self { pool, blobs }
    }

    async fn create_file_owned(
        &self,
        scope: &ProjectScope,
        filename: &str,
        purpose: ProjectFilePurpose,
        bytes: &[u8],
        expires_after: Option<Duration>,
    ) -> Result<ProjectFile, ImageGatewayError> {
        validate_scope(scope)?;
        validate_filename(filename)?;
        validate_file_size(purpose, bytes.len() as u64)?;
        let expires_after = normalize_file_retention(purpose, expires_after)?;
        ensure_project_exists(&self.pool, scope).await?;

        let file_uuid = Uuid::new_v4();
        let file_id = format!("file-{}", file_uuid.simple());
        let blob = self
            .blobs
            .put(file_uuid, bytes)
            .await
            .map_err(blob_write_error)?;
        let now = now_ms()?;
        let expires_at_ms = expires_after
            .map(duration_ms)
            .transpose()?
            .map(|duration| now.saturating_add(duration));

        let byte_size = i64::try_from(blob.byte_size).map_err(|_| {
            ImageGatewayError::internal("file byte size does not fit in PostgreSQL")
        })?;
        let persist = async {
            let mut transaction = self.pool.begin().await.map_err(database_error)?;
            let limits: Option<(i64, i32)> = sqlx::query_as(
                r#"
                SELECT file_storage_limit_bytes, file_storage_limit_count
                FROM gateway_projects
                WHERE id = $1 AND tenant_id = $2
                FOR UPDATE
                "#,
            )
            .bind(&scope.project_id)
            .bind(&scope.tenant_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(database_error)?;
            let (limit_bytes, limit_count) = limits.ok_or_else(project_not_found)?;
            let (used_bytes, used_count): (i64, i64) = sqlx::query_as(
                r#"
                SELECT COALESCE(SUM(byte_size), 0)::BIGINT, COUNT(*)::BIGINT
                FROM project_files
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND cleanup_completed_at_ms IS NULL
                "#,
            )
            .bind(&scope.tenant_id)
            .bind(&scope.project_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(database_error)?;
            if used_count >= i64::from(limit_count)
                || used_bytes
                    .checked_add(byte_size)
                    .is_none_or(|total| total > limit_bytes)
            {
                return Err(project_file_capacity_exceeded(limit_bytes, limit_count));
            }
            sqlx::query(
                r#"
                INSERT INTO project_files
                  (file_id, tenant_id, project_id, purpose, filename,
                   storage_backend, object_key, sha256_hex, byte_size, state,
                   expires_at_ms, created_at_ms, updated_at_ms)
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, 'active', $10, $11, $11
                )
                "#,
            )
            .bind(&file_id)
            .bind(&scope.tenant_id)
            .bind(&scope.project_id)
            .bind(purpose.as_str())
            .bind(filename)
            .bind(&blob.storage_backend)
            .bind(&blob.object_key)
            .bind(&blob.sha256_hex)
            .bind(byte_size)
            .bind(expires_at_ms)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            transaction.commit().await.map_err(database_error)
        }
        .await;

        if let Err(error) = persist {
            if self.blobs.delete(file_uuid, &blob).await.is_err() {
                let _ = self
                    .record_failed_upload_cleanup(
                        scope,
                        &file_id,
                        filename,
                        purpose,
                        &blob,
                        expires_at_ms,
                        now,
                    )
                    .await;
            }
            return Err(error);
        }
        Ok(ProjectFile {
            id: file_id,
            tenant_id: scope.tenant_id.clone(),
            project_id: scope.project_id.clone(),
            purpose,
            filename: filename.to_string(),
            bytes: blob.byte_size,
            sha256_hex: blob.sha256_hex,
            created_at_ms: now,
            expires_at_ms,
            deleted_at_ms: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_failed_upload_cleanup(
        &self,
        scope: &ProjectScope,
        file_id: &str,
        filename: &str,
        purpose: ProjectFilePurpose,
        blob: &BatchFileBlob,
        expires_at_ms: Option<i64>,
        now: i64,
    ) -> Result<(), ImageGatewayError> {
        sqlx::query(
            r#"
            INSERT INTO project_files
              (file_id, tenant_id, project_id, purpose, filename,
               storage_backend, object_key, sha256_hex, byte_size, state,
               expires_at_ms, deleted_at_ms, created_at_ms, updated_at_ms)
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, 'deleted',
                $10, $11, $11, $11
            )
            ON CONFLICT (file_id) DO NOTHING
            "#,
        )
        .bind(file_id)
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(purpose.as_str())
        .bind(filename)
        .bind(&blob.storage_backend)
        .bind(&blob.object_key)
        .bind(&blob.sha256_hex)
        .bind(i64::try_from(blob.byte_size).map_err(|_| {
            ImageGatewayError::internal("file byte size does not fit in PostgreSQL")
        })?)
        .bind(expires_at_ms)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    async fn assert_finalization_lease(
        &self,
        lease: &BatchFinalizationLease,
    ) -> Result<BatchRetentionRow, ImageGatewayError> {
        sqlx::query_as::<_, BatchRetentionRow>(
            r#"
            SELECT output_retention_seconds, status
            FROM project_batches
            WHERE tenant_id = $1
              AND project_id = $2
              AND batch_id = $3
              AND lease_owner = $4
              AND lease_epoch = $5
              AND lease_expires_at_ms > $6
              AND status IN ('finalizing', 'cancelling')
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now_ms()?)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or_else(batch_lease_conflict)
    }

    async fn existing_output_file(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        role: BatchResultRole,
    ) -> Result<Option<ProjectFile>, ImageGatewayError> {
        let row = sqlx::query_as::<_, ProjectFileRow>(
            r#"
            SELECT file.file_id, file.tenant_id, file.project_id, file.purpose,
                   file.filename, file.storage_backend, file.object_key,
                   file.sha256_hex, file.byte_size, file.created_at_ms,
                   file.expires_at_ms, file.deleted_at_ms
            FROM project_batch_output_files output
            JOIN project_files file
              ON file.file_id = output.file_id
             AND file.project_id = output.project_id
             AND file.tenant_id = output.tenant_id
            WHERE output.tenant_id = $1
              AND output.project_id = $2
              AND output.batch_id = $3
              AND output.role = $4
              AND file.state = 'active'
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(role.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?;
        row.map(ProjectFileRow::into_view).transpose()
    }

    async fn attach_output_file(
        &self,
        lease: &BatchFinalizationLease,
        role: BatchResultRole,
        file: &ProjectFile,
    ) -> Result<ProjectFile, ImageGatewayError> {
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let valid: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM project_batches
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND batch_id = $3
                  AND lease_owner = $4
                  AND lease_epoch = $5
                  AND lease_expires_at_ms > $6
                  AND status IN ('finalizing', 'cancelling')
                FOR UPDATE
            )
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now_ms()?)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        if !valid {
            return Err(batch_lease_conflict());
        }
        let inserted = sqlx::query(
            r#"
            INSERT INTO project_batch_output_files
              (batch_id, tenant_id, project_id, role, file_id, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (batch_id, role) DO NOTHING
            "#,
        )
        .bind(&lease.batch_id)
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(role.as_str())
        .bind(&file.id)
        .bind(now_ms()?)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        transaction.commit().await.map_err(database_error)?;

        if inserted == 1 {
            return Ok(file.clone());
        }
        let existing = self
            .existing_output_file(&lease.scope, &lease.batch_id, role)
            .await?
            .ok_or_else(|| ImageGatewayError::internal("batch output role lost its file"))?;
        let _ = self.delete_file(&lease.scope, &file.id).await;
        Ok(existing)
    }
}

#[async_trait]
impl BatchService for PostgresBatchService {
    async fn create_file(
        &self,
        scope: &ProjectScope,
        request: CreateProjectFile<'_>,
    ) -> Result<ProjectFile, ImageGatewayError> {
        self.create_file_owned(
            scope,
            request.filename,
            request.purpose,
            request.bytes,
            request.expires_after,
        )
        .await
    }

    async fn get_file(
        &self,
        scope: &ProjectScope,
        file_id: &str,
    ) -> Result<ProjectFile, ImageGatewayError> {
        validate_scope(scope)?;
        validate_prefixed_uuid(file_id, "file-")?;
        load_file_row(&self.pool, scope, file_id, now_ms()?)
            .await?
            .into_view()
    }

    async fn read_file(
        &self,
        scope: &ProjectScope,
        file_id: &str,
    ) -> Result<Vec<u8>, ImageGatewayError> {
        validate_scope(scope)?;
        let file_uuid = validate_prefixed_uuid(file_id, "file-")?;
        let row = load_file_row(&self.pool, scope, file_id, now_ms()?).await?;
        self.blobs
            .get(file_uuid, &row.blob()?)
            .await
            .map_err(blob_read_error)
    }

    async fn list_files(
        &self,
        scope: &ProjectScope,
        purpose: Option<ProjectFilePurpose>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectFilePage, ImageGatewayError> {
        validate_scope(scope)?;
        validate_limit(limit, 10_000)?;
        if let Some(cursor) = after {
            validate_prefixed_uuid(cursor, "file-")?;
            ensure_file_cursor(&self.pool, scope, cursor).await?;
        }
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ImageGatewayError::internal("file page size overflow"))?;
        let mut rows = sqlx::query_as::<_, ProjectFileRow>(
            r#"
            SELECT file.file_id, file.tenant_id, file.project_id, file.purpose,
                   file.filename, file.storage_backend, file.object_key,
                   file.sha256_hex, file.byte_size, file.created_at_ms,
                   file.expires_at_ms, file.deleted_at_ms
            FROM project_files file
            LEFT JOIN project_files cursor
              ON cursor.file_id = $4
             AND cursor.tenant_id = $1
             AND cursor.project_id = $2
            WHERE file.tenant_id = $1
              AND file.project_id = $2
              AND file.state = 'active'
              AND (file.expires_at_ms IS NULL OR file.expires_at_ms > $5)
              AND ($3::TEXT IS NULL OR file.purpose = $3)
              AND (
                    $4::TEXT IS NULL
                    OR (file.created_at_ms, file.file_id)
                       < (cursor.created_at_ms, cursor.file_id)
                  )
            ORDER BY file.created_at_ms DESC, file.file_id DESC
            LIMIT $6
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(purpose.map(ProjectFilePurpose::as_str))
        .bind(after)
        .bind(now_ms()?)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_after = has_more
            .then(|| rows.last().map(|row| row.file_id.clone()))
            .flatten();
        Ok(ProjectFilePage {
            data: rows
                .into_iter()
                .map(ProjectFileRow::into_view)
                .collect::<Result<_, _>>()?,
            has_more,
            next_after,
        })
    }

    async fn delete_file(
        &self,
        scope: &ProjectScope,
        file_id: &str,
    ) -> Result<ProjectFile, ImageGatewayError> {
        validate_scope(scope)?;
        validate_prefixed_uuid(file_id, "file-")?;
        let now = now_ms()?;
        let cleanup_owner = format!("delete-api:{}", Uuid::new_v4().simple());
        let cleanup_expires_at_ms = now.saturating_add(60_000);
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let row = sqlx::query_as::<_, ProjectFileRow>(
            r#"
            SELECT file_id, tenant_id, project_id, purpose, filename,
                   storage_backend, object_key, sha256_hex, byte_size,
                   created_at_ms, expires_at_ms, deleted_at_ms
            FROM project_files
            WHERE tenant_id = $1
              AND project_id = $2
              AND file_id = $3
              AND state = 'active'
            FOR UPDATE
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(file_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        .ok_or_else(file_not_found)?;
        let in_use: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM project_batches
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND input_file_id = $3
                  AND status IN (
                      'validating', 'in_progress', 'finalizing', 'cancelling'
                  )
            )
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(file_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        if in_use {
            return Err(ImageGatewayError::conflict(
                "The file is referenced by a non-terminal batch",
                Some("file_id".to_string()),
                "file_in_use",
            ));
        }
        let cleanup_epoch: i64 = sqlx::query_scalar(
            r#"
            UPDATE project_files
            SET state = 'deleted',
                deleted_at_ms = $4,
                updated_at_ms = $4,
                cleanup_lease_owner = $5,
                cleanup_lease_epoch = cleanup_lease_epoch + 1,
                cleanup_lease_expires_at_ms = $6
            WHERE tenant_id = $1 AND project_id = $2 AND file_id = $3
            RETURNING cleanup_lease_epoch
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(file_id)
        .bind(now)
        .bind(&cleanup_owner)
        .bind(cleanup_expires_at_ms)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;

        let lease = ProjectFileCleanupLease {
            scope: scope.clone(),
            file_id: file_id.to_string(),
            blob: row.blob()?,
            lease_owner: cleanup_owner,
            lease_epoch: cleanup_epoch,
            lease_expires_at_ms: cleanup_expires_at_ms,
        };
        match self.delete_file_blob(&lease).await {
            Ok(()) => {
                let _ = self.complete_file_cleanup(&lease).await;
            }
            Err(_) => {
                let _ = self.release_file_cleanup(&lease).await;
            }
        }
        let mut file = row.into_view()?;
        file.deleted_at_ms = Some(now);
        Ok(file)
    }

    async fn claim_file_cleanup(
        &self,
        worker_id: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<ProjectFileCleanupLease>, ImageGatewayError> {
        validate_worker(worker_id)?;
        validate_limit(limit, 1_000)?;
        let now = now_ms()?;
        let lease_expires_at_ms = now.saturating_add(valid_lease_ms(lease_duration)?);
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let candidates: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT file.file_id
            FROM project_files file
            WHERE file.cleanup_completed_at_ms IS NULL
              AND (
                    file.cleanup_lease_owner IS NULL
                    OR file.cleanup_lease_expires_at_ms <= $1
                  )
              AND (
                    file.state = 'deleted'
                    OR (
                        file.state = 'active'
                        AND file.expires_at_ms <= $1
                    )
                  )
              AND NOT EXISTS (
                    SELECT 1
                    FROM project_batches batch
                    WHERE batch.tenant_id = file.tenant_id
                      AND batch.project_id = file.project_id
                      AND batch.input_file_id = file.file_id
                      AND batch.status IN (
                          'validating', 'in_progress', 'finalizing', 'cancelling'
                      )
                  )
            ORDER BY
                COALESCE(file.deleted_at_ms, file.expires_at_ms),
                file.file_id
            FOR UPDATE OF file SKIP LOCKED
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(i64::try_from(limit).map_err(|_| {
            ImageGatewayError::internal("file cleanup claim size does not fit in PostgreSQL")
        })?)
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        if candidates.is_empty() {
            transaction.commit().await.map_err(database_error)?;
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, ProjectFileCleanupLeaseRow>(
            r#"
            UPDATE project_files file
            SET state = 'deleted',
                deleted_at_ms = COALESCE(file.deleted_at_ms, $1),
                cleanup_lease_owner = $2,
                cleanup_lease_epoch = file.cleanup_lease_epoch + 1,
                cleanup_lease_expires_at_ms = $3,
                updated_at_ms = $1
            WHERE file.file_id = ANY($4)
              AND NOT EXISTS (
                    SELECT 1
                    FROM project_batches batch
                    WHERE batch.tenant_id = file.tenant_id
                      AND batch.project_id = file.project_id
                      AND batch.input_file_id = file.file_id
                      AND batch.status IN (
                          'validating', 'in_progress', 'finalizing', 'cancelling'
                      )
                  )
            RETURNING file.file_id, file.tenant_id, file.project_id,
                      file.storage_backend, file.object_key, file.sha256_hex,
                      file.byte_size, file.cleanup_lease_owner,
                      file.cleanup_lease_epoch, file.cleanup_lease_expires_at_ms
            "#,
        )
        .bind(now)
        .bind(worker_id)
        .bind(lease_expires_at_ms)
        .bind(&candidates)
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        rows.into_iter()
            .map(ProjectFileCleanupLeaseRow::into_lease)
            .collect()
    }

    async fn delete_file_blob(
        &self,
        lease: &ProjectFileCleanupLease,
    ) -> Result<(), ImageGatewayError> {
        let file_uuid = validate_prefixed_uuid(&lease.file_id, "file-")?;
        let current: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM project_files
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND file_id = $3
                  AND state = 'deleted'
                  AND cleanup_completed_at_ms IS NULL
                  AND cleanup_lease_owner = $4
                  AND cleanup_lease_epoch = $5
                  AND cleanup_lease_expires_at_ms > $6
            )
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.file_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now_ms()?)
        .fetch_one(&self.pool)
        .await
        .map_err(database_error)?;
        if !current {
            return Err(file_cleanup_lease_conflict());
        }
        self.blobs
            .delete(file_uuid, &lease.blob)
            .await
            .map_err(blob_write_error)
    }

    async fn complete_file_cleanup(
        &self,
        lease: &ProjectFileCleanupLease,
    ) -> Result<(), ImageGatewayError> {
        let now = now_ms()?;
        let updated = sqlx::query(
            r#"
            UPDATE project_files
            SET cleanup_completed_at_ms = $6,
                cleanup_lease_owner = NULL,
                cleanup_lease_expires_at_ms = NULL,
                updated_at_ms = $6
            WHERE tenant_id = $1
              AND project_id = $2
              AND file_id = $3
              AND state = 'deleted'
              AND cleanup_completed_at_ms IS NULL
              AND cleanup_lease_owner = $4
              AND cleanup_lease_epoch = $5
              AND cleanup_lease_expires_at_ms > $6
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.file_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(database_error)?
        .rows_affected();
        if updated == 1 {
            Ok(())
        } else {
            Err(file_cleanup_lease_conflict())
        }
    }

    async fn release_file_cleanup(
        &self,
        lease: &ProjectFileCleanupLease,
    ) -> Result<(), ImageGatewayError> {
        let updated = sqlx::query(
            r#"
            UPDATE project_files
            SET cleanup_lease_owner = NULL,
                cleanup_lease_expires_at_ms = NULL,
                updated_at_ms = $6
            WHERE tenant_id = $1
              AND project_id = $2
              AND file_id = $3
              AND cleanup_completed_at_ms IS NULL
              AND cleanup_lease_owner = $4
              AND cleanup_lease_epoch = $5
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.file_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now_ms()?)
        .execute(&self.pool)
        .await
        .map_err(database_error)?
        .rows_affected();
        if updated == 1 {
            Ok(())
        } else {
            Err(file_cleanup_lease_conflict())
        }
    }

    async fn create_batch(
        &self,
        scope: &ProjectScope,
        request: CreateProjectBatch,
    ) -> Result<ProjectBatch, ImageGatewayError> {
        validate_scope(scope)?;
        let prepared = validate_batch_request(&request)?;
        let now = now_ms()?;
        let expires_at_ms = now.saturating_add(BATCH_WINDOW_MS);
        let batch_id = format!("batch-{}", Uuid::new_v4().simple());
        let mut transaction = self.pool.begin().await.map_err(database_error)?;

        let input_purpose: Option<String> = sqlx::query_scalar(
            r#"
            SELECT purpose
            FROM project_files
            WHERE tenant_id = $1
              AND project_id = $2
              AND file_id = $3
              AND state = 'active'
              AND (expires_at_ms IS NULL OR expires_at_ms > $4)
            FOR SHARE
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(&request.input_file_id)
        .bind(now)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        if input_purpose.as_deref() != Some(ProjectFilePurpose::Batch.as_str()) {
            return Err(ImageGatewayError::invalid_request(
                "input_file_id must reference an active batch file in this project",
                Some("input_file_id".to_string()),
                "invalid_batch_input_file",
            ));
        }

        sqlx::query(
            r#"
            INSERT INTO project_batches
              (batch_id, tenant_id, project_id, input_file_id, endpoint, model,
               completion_window, status, metadata, auth_snapshot, route_snapshot,
               request_count_total, output_retention_seconds,
               created_at_ms, expires_at_ms, updated_at_ms)
            VALUES (
               $1, $2, $3, $4, $5, $6, $7, 'validating', $8, $9, $10,
               $11, $12, $13, $14, $13
            )
            "#,
        )
        .bind(&batch_id)
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(&request.input_file_id)
        .bind(&request.endpoint)
        .bind(&prepared.model)
        .bind(&request.completion_window)
        .bind(&request.metadata)
        .bind(&request.safe_auth_snapshot)
        .bind(&request.route_snapshot)
        .bind(i32::try_from(request.lines.len()).map_err(|_| {
            ImageGatewayError::invalid_request(
                "batch has too many requests",
                Some("input_file_id".to_string()),
                "batch_too_large",
            )
        })?)
        .bind(prepared.output_retention_seconds)
        .bind(now)
        .bind(expires_at_ms)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;

        for chunk in request.lines.chunks(1_000) {
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO project_batch_requests \
                 (request_id, tenant_id, project_id, batch_id, ordinal, custom_id, \
                  method, request_url, model, request_body, request_hash, state, \
                  available_at_ms, created_at_ms, updated_at_ms) ",
            );
            builder.push_values(chunk, |mut row, line| {
                row.push_bind(Uuid::new_v4())
                    .push_bind(&scope.tenant_id)
                    .push_bind(&scope.project_id)
                    .push_bind(&batch_id)
                    .push_bind(i32::try_from(line.ordinal).unwrap_or(i32::MAX))
                    .push_bind(&line.custom_id)
                    .push_bind(&line.method)
                    .push_bind(&line.url)
                    .push_bind(&line.model)
                    .push_bind(&line.body)
                    .push_bind(request_hash(line))
                    .push_bind("pending")
                    .push_bind(now)
                    .push_bind(now)
                    .push_bind(now);
            });
            builder
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(batch_insert_error)?;
        }
        transaction.commit().await.map_err(database_error)?;
        self.get_batch(scope, &batch_id).await
    }

    async fn mark_batch_validated(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError> {
        validate_batch_id(batch_id)?;
        let now = now_ms()?;
        let changed = sqlx::query(
            r#"
            UPDATE project_batches
            SET status = 'in_progress',
                validated_at_ms = COALESCE(validated_at_ms, $4),
                in_progress_at_ms = COALESCE(in_progress_at_ms, $4),
                control_version = control_version + 1,
                updated_at_ms = $4
            WHERE tenant_id = $1
              AND project_id = $2
              AND batch_id = $3
              AND status = 'validating'
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(database_error)?
        .rows_affected();
        if changed == 0 {
            let batch = self.get_batch(scope, batch_id).await?;
            if batch.status != BatchStatus::InProgress {
                return Err(batch_state_conflict("batch cannot enter in_progress"));
            }
            return Ok(batch);
        }
        self.get_batch(scope, batch_id).await
    }

    async fn fail_batch_validation(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        errors: Value,
    ) -> Result<ProjectBatch, ImageGatewayError> {
        validate_batch_id(batch_id)?;
        if !matches!(errors, Value::Object(_) | Value::Array(_)) {
            return Err(ImageGatewayError::invalid_request(
                "errors must be a JSON object or array",
                Some("errors".to_string()),
                "invalid_batch_errors",
            ));
        }
        let now = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let total: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT request_count_total
            FROM project_batches
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
              AND status = 'validating'
            FOR UPDATE
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        let Some(total) = total else {
            return Err(batch_state_conflict("batch validation is no longer active"));
        };
        sqlx::query(
            r#"
            UPDATE project_batch_requests
            SET state = 'failed', error = $4, completed_at_ms = $5, updated_at_ms = $5
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
              AND state = 'pending'
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(&errors)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        sqlx::query(
            r#"
            UPDATE project_batches
            SET status = 'failed', errors = $4, request_count_failed = $5,
                failed_at_ms = $6, control_version = control_version + 1,
                updated_at_ms = $6
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(errors)
        .bind(total)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        self.get_batch(scope, batch_id).await
    }

    async fn get_batch(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError> {
        validate_scope(scope)?;
        validate_batch_id(batch_id)?;
        load_batch_row(&self.pool, scope, batch_id)
            .await?
            .into_view()
    }

    async fn list_batches(
        &self,
        scope: &ProjectScope,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectBatchPage, ImageGatewayError> {
        validate_scope(scope)?;
        validate_limit(limit, 100)?;
        if let Some(cursor) = after {
            validate_batch_id(cursor)?;
            ensure_batch_cursor(&self.pool, scope, cursor).await?;
        }
        let fetch_limit = i64::try_from(limit + 1)
            .map_err(|_| ImageGatewayError::internal("batch page size overflow"))?;
        let mut rows = sqlx::query_as::<_, BatchRow>(
            r#"
            SELECT batch.batch_id, batch.tenant_id, batch.project_id,
                   batch.input_file_id, batch.endpoint, batch.model,
                   batch.completion_window, batch.status, batch.metadata,
                   batch.errors, batch.request_count_total,
                   batch.request_count_completed, batch.request_count_failed,
                   batch.request_count_cancelled, batch.created_at_ms,
                   batch.in_progress_at_ms, batch.finalizing_at_ms,
                   batch.completed_at_ms, batch.failed_at_ms,
                   batch.expires_at_ms, batch.cancel_requested_at_ms,
                   batch.cancelled_at_ms,
                   output.file_id AS output_file_id,
                   error.file_id AS error_file_id
            FROM project_batches batch
            LEFT JOIN project_batch_output_files output
              ON output.batch_id = batch.batch_id
             AND output.project_id = batch.project_id
             AND output.tenant_id = batch.tenant_id
             AND output.role = 'output'
            LEFT JOIN project_batch_output_files error
              ON error.batch_id = batch.batch_id
             AND error.project_id = batch.project_id
             AND error.tenant_id = batch.tenant_id
             AND error.role = 'error'
            LEFT JOIN project_batches cursor
              ON cursor.batch_id = $3
             AND cursor.tenant_id = $1
             AND cursor.project_id = $2
            WHERE batch.tenant_id = $1
              AND batch.project_id = $2
              AND (
                    $3::TEXT IS NULL
                    OR (batch.created_at_ms, batch.batch_id)
                       < (cursor.created_at_ms, cursor.batch_id)
                  )
            ORDER BY batch.created_at_ms DESC, batch.batch_id DESC
            LIMIT $4
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(after)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let next_after = has_more
            .then(|| rows.last().map(|row| row.batch_id.clone()))
            .flatten();
        Ok(ProjectBatchPage {
            data: rows
                .into_iter()
                .map(BatchRow::into_view)
                .collect::<Result<_, _>>()?,
            has_more,
            next_after,
        })
    }

    async fn list_runnable_batches(
        &self,
        limit: usize,
    ) -> Result<Vec<BatchWorkTarget>, ImageGatewayError> {
        validate_limit(limit, 1_000)?;
        let rows = sqlx::query_as::<_, BatchWorkTargetRow>(
            r#"
            SELECT tenant_id, project_id, batch_id, status, expires_at_ms
            FROM project_batches
            WHERE status IN ('validating', 'in_progress', 'finalizing', 'cancelling')
            ORDER BY updated_at_ms, batch_id
            LIMIT $1
            "#,
        )
        .bind(i64::try_from(limit).map_err(|_| {
            ImageGatewayError::internal("batch recovery scan size does not fit in PostgreSQL")
        })?)
        .fetch_all(&self.pool)
        .await
        .map_err(database_error)?;
        rows.into_iter()
            .map(BatchWorkTargetRow::into_target)
            .collect()
    }

    async fn load_execution_snapshot(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<BatchExecutionSnapshot, ImageGatewayError> {
        validate_scope(scope)?;
        validate_batch_id(batch_id)?;
        let row = sqlx::query_as::<_, BatchExecutionSnapshotRow>(
            r#"
            SELECT auth_snapshot, route_snapshot
            FROM project_batches
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or_else(batch_not_found)?;
        validate_safe_snapshot(&row.auth_snapshot, "auth_snapshot")?;
        validate_safe_snapshot(&row.route_snapshot, "route_snapshot")?;
        Ok(BatchExecutionSnapshot {
            scope: scope.clone(),
            batch_id: batch_id.to_string(),
            safe_auth_snapshot: row.auth_snapshot,
            route_snapshot: row.route_snapshot,
        })
    }

    async fn claim_requests(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        worker_id: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<BatchRequestLease>, ImageGatewayError> {
        validate_scope(scope)?;
        validate_batch_id(batch_id)?;
        validate_worker(worker_id)?;
        validate_limit(limit, 1_000)?;
        let now = now_ms()?;
        let expires = now.saturating_add(valid_lease_ms(lease_duration)?);
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let batch: Option<(String, i64)> = sqlx::query_as(
            r#"
            SELECT status, expires_at_ms
            FROM project_batches
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            FOR UPDATE
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        let Some((status, expires_at_ms)) = batch else {
            return Err(batch_not_found());
        };
        if BatchStatus::from_str(&status)? != BatchStatus::InProgress || expires_at_ms <= now {
            transaction.commit().await.map_err(database_error)?;
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, BatchRequestLeaseRow>(
            r#"
            WITH candidates AS (
                SELECT request_id
                FROM project_batch_requests
                WHERE tenant_id = $1
                  AND project_id = $2
                  AND batch_id = $3
                  AND available_at_ms <= $4
                  AND (
                        state = 'pending'
                        OR (state = 'leased' AND lease_expires_at_ms <= $4)
                      )
                ORDER BY
                    CASE WHEN state = 'pending' THEN 0 ELSE 1 END,
                    available_at_ms,
                    COALESCE(lease_expires_at_ms, available_at_ms),
                    ordinal
                FOR UPDATE SKIP LOCKED
                LIMIT $5
            )
            UPDATE project_batch_requests request
            SET state = 'leased',
                lease_owner = $6,
                lease_epoch = request.lease_epoch + 1,
                lease_expires_at_ms = $7,
                attempt_count = request.attempt_count + 1,
                started_at_ms = COALESCE(request.started_at_ms, $4),
                updated_at_ms = $4
            FROM candidates
            WHERE request.request_id = candidates.request_id
              AND request.tenant_id = $1
              AND request.project_id = $2
              AND request.batch_id = $3
            RETURNING request.request_id, request.ordinal, request.custom_id,
                      request.method, request.request_url, request.model,
                      request.request_body, request.request_hash,
                      request.attempt_count,
                      request.lease_owner, request.lease_epoch,
                      request.lease_expires_at_ms
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(now)
        .bind(i64::try_from(limit).map_err(|_| {
            ImageGatewayError::internal("batch claim size does not fit in PostgreSQL")
        })?)
        .bind(worker_id)
        .bind(expires)
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        rows.into_iter()
            .map(|row| row.into_lease(scope, batch_id))
            .collect()
    }

    async fn complete_request(
        &self,
        lease: &BatchRequestLease,
        result: BatchRequestSuccess,
    ) -> Result<(), ImageGatewayError> {
        if !(100..=599).contains(&result.status_code) {
            return Err(ImageGatewayError::invalid_request(
                "response status must be between 100 and 599",
                Some("status_code".to_string()),
                "invalid_batch_response",
            ));
        }
        finish_request(
            &self.pool,
            lease,
            FinishedRequest::Completed(result),
            now_ms()?,
        )
        .await
    }

    async fn fail_request(
        &self,
        lease: &BatchRequestLease,
        error: Value,
    ) -> Result<(), ImageGatewayError> {
        if !error.is_object() {
            return Err(ImageGatewayError::invalid_request(
                "batch request error must be a JSON object",
                Some("error".to_string()),
                "invalid_batch_error",
            ));
        }
        finish_request(&self.pool, lease, FinishedRequest::Failed(error), now_ms()?).await
    }

    async fn retry_request(
        &self,
        lease: &BatchRequestLease,
        error: Value,
        delay: Duration,
    ) -> Result<(), ImageGatewayError> {
        if !error.is_object() {
            return Err(ImageGatewayError::invalid_request(
                "batch request retry error must be a JSON object",
                Some("error".to_string()),
                "invalid_batch_error",
            ));
        }
        let now = now_ms()?;
        let delay_ms = i64::try_from(delay.as_millis()).map_err(|_| {
            ImageGatewayError::invalid_request(
                "batch request retry delay is too large",
                Some("delay".to_string()),
                "invalid_batch_retry_delay",
            )
        })?;
        let available_at = now.checked_add(delay_ms).ok_or_else(|| {
            ImageGatewayError::invalid_request(
                "batch request retry delay is too large",
                Some("delay".to_string()),
                "invalid_batch_retry_delay",
            )
        })?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let status = lock_batch_status(&mut transaction, &lease.scope, &lease.batch_id).await?;
        if status != BatchStatus::InProgress {
            return Err(batch_lease_conflict());
        }
        let changed = sqlx::query(
            r#"
            UPDATE project_batch_requests
            SET state = 'pending',
                available_at_ms = $7,
                last_error = $8,
                lease_owner = NULL,
                lease_expires_at_ms = NULL,
                updated_at_ms = $9
            WHERE tenant_id = $1
              AND project_id = $2
              AND batch_id = $3
              AND request_id = $4
              AND state = 'leased'
              AND lease_owner = $5
              AND lease_epoch = $6
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .bind(lease.request_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(available_at)
        .bind(error)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        if changed != 1 {
            return Err(batch_lease_conflict());
        }
        sqlx::query(
            r#"
            UPDATE project_batches
            SET updated_at_ms = $4
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)
    }

    async fn cancel_batch(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError> {
        validate_batch_id(batch_id)?;
        let now = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let status = lock_batch_status(&mut transaction, scope, batch_id).await?;
        if status == BatchStatus::Cancelled {
            transaction.commit().await.map_err(database_error)?;
            return self.get_batch(scope, batch_id).await;
        }
        if status.is_terminal() {
            return Err(ImageGatewayError::conflict(
                "The batch is already terminal and cannot be cancelled",
                Some("batch_id".to_string()),
                "batch_not_cancellable",
            ));
        }
        let cancelled = sqlx::query(
            r#"
            UPDATE project_batch_requests
            SET state = 'cancelled', completed_at_ms = $4, updated_at_ms = $4
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
              AND state = 'pending'
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        sqlx::query(
            r#"
            UPDATE project_batches
            SET status = 'cancelling',
                cancel_requested_at_ms = COALESCE(cancel_requested_at_ms, $4),
                request_count_cancelled = request_count_cancelled + $5,
                lease_owner = NULL,
                lease_epoch = lease_epoch + 1,
                lease_expires_at_ms = NULL,
                control_version = control_version + 1,
                updated_at_ms = $4
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(now)
        .bind(
            i32::try_from(cancelled).map_err(|_| {
                ImageGatewayError::internal("cancelled batch request count overflow")
            })?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        self.get_batch(scope, batch_id).await
    }

    async fn expire_batch(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<ProjectBatch, ImageGatewayError> {
        validate_batch_id(batch_id)?;
        let now = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let state = lock_batch_state(&mut transaction, scope, batch_id).await?;
        let status = state.status;
        if status == BatchStatus::Expired {
            transaction.commit().await.map_err(database_error)?;
            return self.get_batch(scope, batch_id).await;
        }
        if status.is_terminal() {
            return Err(batch_state_conflict("terminal batch cannot expire again"));
        }
        if state.expires_at_ms > now {
            return Err(batch_state_conflict(
                "batch completion window has not expired",
            ));
        }
        let cancelled = sqlx::query(
            r#"
            UPDATE project_batch_requests
            SET state = 'cancelled', completed_at_ms = $4, updated_at_ms = $4
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
              AND state = 'pending'
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        sqlx::query(
            r#"
            UPDATE project_batches
            SET request_count_cancelled = request_count_cancelled + $4,
                control_version = control_version + 1,
                updated_at_ms = $5
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(
            i32::try_from(cancelled)
                .map_err(|_| ImageGatewayError::internal("expired batch request count overflow"))?,
        )
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        self.get_batch(scope, batch_id).await
    }

    async fn claim_finalization(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        worker_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<BatchFinalizationLease>, ImageGatewayError> {
        validate_scope(scope)?;
        validate_batch_id(batch_id)?;
        validate_worker(worker_id)?;
        let now = now_ms()?;
        let expires = now.saturating_add(valid_lease_ms(lease_duration)?);
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let state = lock_batch_state(&mut transaction, scope, batch_id).await?;
        let status = state.status;
        if !matches!(
            status,
            BatchStatus::InProgress | BatchStatus::Finalizing | BatchStatus::Cancelling
        ) {
            transaction.commit().await.map_err(database_error)?;
            return Ok(None);
        }

        let naturally_expired =
            state.expires_at_ms <= now && state.cancel_requested_at_ms.is_none();
        if status == BatchStatus::Cancelling || naturally_expired {
            let cancelled = sqlx::query(
                r#"
                UPDATE project_batch_requests
                SET state = 'cancelled', completed_at_ms = $4, updated_at_ms = $4,
                    lease_owner = NULL, lease_expires_at_ms = NULL
                WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
                  AND state = 'leased'
                  AND lease_expires_at_ms <= $4
                "#,
            )
            .bind(&scope.tenant_id)
            .bind(&scope.project_id)
            .bind(batch_id)
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?
            .rows_affected();
            if cancelled > 0 {
                sqlx::query(
                    r#"
                    UPDATE project_batches
                    SET request_count_cancelled = request_count_cancelled + $4,
                        updated_at_ms = $5
                    WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
                    "#,
                )
                .bind(&scope.tenant_id)
                .bind(&scope.project_id)
                .bind(batch_id)
                .bind(i32::try_from(cancelled).map_err(|_| {
                    ImageGatewayError::internal("cancelled batch request count overflow")
                })?)
                .bind(now)
                .execute(&mut *transaction)
                .await
                .map_err(database_error)?;
            }
        }

        let outstanding: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM project_batch_requests
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
              AND state IN ('pending', 'leased')
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        if outstanding != 0 {
            transaction.commit().await.map_err(database_error)?;
            return Ok(None);
        }

        let row = sqlx::query_as::<_, BatchFinalizationRow>(
            r#"
            UPDATE project_batches
            SET status = CASE WHEN status = 'in_progress' THEN 'finalizing' ELSE status END,
                finalizing_at_ms = COALESCE(finalizing_at_ms, $4),
                lease_owner = $5,
                lease_epoch = lease_epoch + 1,
                lease_expires_at_ms = $6,
                control_version = control_version + 1,
                updated_at_ms = $4
            WHERE tenant_id = $1
              AND project_id = $2
              AND batch_id = $3
              AND status IN ('in_progress', 'finalizing', 'cancelling')
              AND (
                    lease_owner IS NULL
                    OR lease_expires_at_ms <= $4
                    OR lease_owner = $5
                  )
            RETURNING lease_epoch, lease_expires_at_ms, status
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(now)
        .bind(worker_id)
        .bind(expires)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        transaction.commit().await.map_err(database_error)?;
        Ok(row.map(|row| BatchFinalizationLease {
            scope: scope.clone(),
            batch_id: batch_id.to_string(),
            lease_owner: worker_id.to_string(),
            lease_epoch: row.lease_epoch,
            lease_expires_at_ms: row.lease_expires_at_ms,
            cancelling: row.status == "cancelling",
        }))
    }

    async fn generate_result_jsonl(
        &self,
        scope: &ProjectScope,
        batch_id: &str,
        role: BatchResultRole,
    ) -> Result<Vec<u8>, ImageGatewayError> {
        self.get_batch(scope, batch_id).await?;
        let requested_state = match role {
            BatchResultRole::Output => BatchRequestState::Completed,
            BatchResultRole::Error => BatchRequestState::Failed,
        };
        let mut rows = sqlx::query_as::<_, BatchResultRow>(
            r#"
            SELECT request_id, custom_id, response_status_code,
                   response_request_id, response_body, error
            FROM project_batch_requests
            WHERE tenant_id = $1
              AND project_id = $2
              AND batch_id = $3
              AND state = $4
            ORDER BY ordinal
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(batch_id)
        .bind(match requested_state {
            BatchRequestState::Completed => "completed",
            BatchRequestState::Failed => "failed",
            _ => unreachable!(),
        })
        .fetch(&self.pool);
        let mut output = Vec::new();
        while let Some(row) = rows.try_next().await.map_err(database_error)? {
            let value = match role {
                BatchResultRole::Output => json!({
                    "id": format!("batch_req_{}", row.request_id.simple()),
                    "custom_id": row.custom_id,
                    "response": {
                        "status_code": row.response_status_code,
                        "request_id": row.response_request_id,
                        "body": row.response_body,
                    },
                    "error": Value::Null,
                }),
                BatchResultRole::Error => json!({
                    "id": format!("batch_req_{}", row.request_id.simple()),
                    "custom_id": row.custom_id,
                    "response": Value::Null,
                    "error": row.error,
                }),
            };
            serde_json::to_writer(&mut output, &value)
                .map_err(|_| ImageGatewayError::internal("failed to encode batch JSONL"))?;
            output.push(b'\n');
            if output.len() > MAX_BATCH_RESULT_FILE_BYTES {
                return Err(ImageGatewayError::internal(
                    "batch result exceeded the materialization limit",
                ));
            }
        }
        Ok(output)
    }

    async fn materialize_result_files(
        &self,
        lease: &BatchFinalizationLease,
    ) -> Result<(Option<ProjectFile>, Option<ProjectFile>), ImageGatewayError> {
        let retention = self.assert_finalization_lease(lease).await?;
        let retention = Duration::from_secs(
            u64::try_from(retention.output_retention_seconds)
                .map_err(|_| ImageGatewayError::internal("invalid output retention"))?,
        );
        let output = if let Some(existing) = self
            .existing_output_file(&lease.scope, &lease.batch_id, BatchResultRole::Output)
            .await?
        {
            Some(existing)
        } else {
            let bytes = self
                .generate_result_jsonl(&lease.scope, &lease.batch_id, BatchResultRole::Output)
                .await?;
            if bytes.is_empty() {
                None
            } else {
                let file = self
                    .create_file_owned(
                        &lease.scope,
                        &format!("{}_output.jsonl", lease.batch_id),
                        ProjectFilePurpose::BatchOutput,
                        &bytes,
                        Some(retention),
                    )
                    .await?;
                self.assert_finalization_lease(lease).await?;
                Some(
                    self.attach_output_file(lease, BatchResultRole::Output, &file)
                        .await?,
                )
            }
        };
        let error = if let Some(existing) = self
            .existing_output_file(&lease.scope, &lease.batch_id, BatchResultRole::Error)
            .await?
        {
            Some(existing)
        } else {
            let bytes = self
                .generate_result_jsonl(&lease.scope, &lease.batch_id, BatchResultRole::Error)
                .await?;
            if bytes.is_empty() {
                None
            } else {
                let file = self
                    .create_file_owned(
                        &lease.scope,
                        &format!("{}_errors.jsonl", lease.batch_id),
                        ProjectFilePurpose::BatchOutput,
                        &bytes,
                        Some(retention),
                    )
                    .await?;
                self.assert_finalization_lease(lease).await?;
                Some(
                    self.attach_output_file(lease, BatchResultRole::Error, &file)
                        .await?,
                )
            }
        };
        Ok((output, error))
    }

    async fn finalize_batch(
        &self,
        lease: &BatchFinalizationLease,
    ) -> Result<ProjectBatch, ImageGatewayError> {
        let now = now_ms()?;
        let mut transaction = self.pool.begin().await.map_err(database_error)?;
        let outstanding: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM project_batch_requests
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
              AND state IN ('pending', 'leased')
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(database_error)?;
        if outstanding != 0 {
            return Err(batch_state_conflict(
                "batch still has requests awaiting a terminal result",
            ));
        }
        let changed = sqlx::query(
            r#"
            UPDATE project_batches
            SET status = CASE
                    WHEN status = 'cancelling' THEN 'cancelled'
                    WHEN expires_at_ms <= $6 AND cancel_requested_at_ms IS NULL THEN 'expired'
                    ELSE 'completed'
                END,
                completed_at_ms = CASE
                    WHEN status = 'cancelling'
                      OR (expires_at_ms <= $6 AND cancel_requested_at_ms IS NULL)
                        THEN completed_at_ms
                    ELSE $6
                END,
                cancelled_at_ms = CASE
                    WHEN status = 'cancelling' THEN $6
                    ELSE cancelled_at_ms
                END,
                lease_owner = NULL,
                lease_expires_at_ms = NULL,
                control_version = control_version + 1,
                updated_at_ms = $6
            WHERE tenant_id = $1
              AND project_id = $2
              AND batch_id = $3
              AND lease_owner = $4
              AND lease_epoch = $5
              AND lease_expires_at_ms > $6
              AND status IN ('finalizing', 'cancelling')
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .bind(&lease.lease_owner)
        .bind(lease.lease_epoch)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        if changed != 1 {
            return Err(batch_lease_conflict());
        }
        transaction.commit().await.map_err(database_error)?;
        self.get_batch(&lease.scope, &lease.batch_id).await
    }
}

#[derive(Debug)]
struct PreparedBatch {
    model: String,
    output_retention_seconds: i32,
}

fn validate_batch_request(
    request: &CreateProjectBatch,
) -> Result<PreparedBatch, ImageGatewayError> {
    validate_prefixed_uuid(&request.input_file_id, "file-")?;
    if request.endpoint.trim().is_empty() || request.endpoint.len() > 256 {
        return Err(ImageGatewayError::invalid_request(
            "endpoint must contain 1 to 256 characters",
            Some("endpoint".to_string()),
            "invalid_batch_endpoint",
        ));
    }
    if request.completion_window != "24h" {
        return Err(ImageGatewayError::invalid_request(
            "completion_window must be '24h'",
            Some("completion_window".to_string()),
            "invalid_completion_window",
        ));
    }
    if request.lines.is_empty() || request.lines.len() > MAX_BATCH_REQUESTS {
        return Err(ImageGatewayError::invalid_request(
            "batch must contain between 1 and 50000 requests",
            Some("input_file_id".to_string()),
            "batch_too_large",
        ));
    }
    validate_metadata(&request.metadata)?;
    validate_safe_snapshot(&request.safe_auth_snapshot, "auth_snapshot")?;
    validate_safe_snapshot(&request.route_snapshot, "route_snapshot")?;
    let output_retention_seconds =
        i32::try_from(request.output_retention.as_secs()).map_err(|_| {
            ImageGatewayError::invalid_request(
                "output retention is out of range",
                Some("output_expires_after".to_string()),
                "invalid_expiration",
            )
        })?;
    if !(MIN_FILE_RETENTION_SECONDS..=MAX_FILE_RETENTION_SECONDS)
        .contains(&(output_retention_seconds as u32))
    {
        return Err(ImageGatewayError::invalid_request(
            "output retention must be between 1 hour and 30 days",
            Some("output_expires_after".to_string()),
            "invalid_expiration",
        ));
    }

    let mut custom_ids = HashSet::with_capacity(request.lines.len());
    let model = request
        .lines
        .first()
        .map(|line| line.model.trim().to_string())
        .unwrap_or_default();
    for (expected_ordinal, line) in request.lines.iter().enumerate() {
        validate_batch_line(line, expected_ordinal, &request.endpoint, &model)?;
        if !custom_ids.insert(line.custom_id.as_str()) {
            return Err(ImageGatewayError::invalid_request(
                "custom_id must be unique within a batch",
                Some("input_file_id".to_string()),
                "duplicate_custom_id",
            ));
        }
    }
    Ok(PreparedBatch {
        model,
        output_retention_seconds,
    })
}

fn validate_batch_line(
    line: &ValidatedBatchLine,
    expected_ordinal: usize,
    endpoint: &str,
    model: &str,
) -> Result<(), ImageGatewayError> {
    if usize::try_from(line.ordinal).ok() != Some(expected_ordinal) {
        return Err(ImageGatewayError::invalid_request(
            "batch request ordinals must be contiguous and zero-based",
            Some("input_file_id".to_string()),
            "invalid_batch_ordinal",
        ));
    }
    if line.custom_id.trim().is_empty() || line.custom_id.len() > 256 {
        return Err(ImageGatewayError::invalid_request(
            "custom_id must contain 1 to 256 characters",
            Some("input_file_id".to_string()),
            "invalid_custom_id",
        ));
    }
    if line.method != "POST" {
        return Err(ImageGatewayError::invalid_request(
            "batch request method must be POST",
            Some("input_file_id".to_string()),
            "invalid_batch_method",
        ));
    }
    if line.url != endpoint {
        return Err(ImageGatewayError::invalid_request(
            "batch request URL must match the batch endpoint",
            Some("input_file_id".to_string()),
            "batch_endpoint_mismatch",
        ));
    }
    if line.model.trim().is_empty() || line.model.len() > 256 || line.model != model {
        return Err(ImageGatewayError::invalid_request(
            "all batch requests must use the same model",
            Some("input_file_id".to_string()),
            "batch_model_mismatch",
        ));
    }
    if !line.body.is_object() {
        return Err(ImageGatewayError::invalid_request(
            "batch request body must be a JSON object",
            Some("input_file_id".to_string()),
            "invalid_jsonl",
        ));
    }
    Ok(())
}

fn validate_metadata(metadata: &Value) -> Result<(), ImageGatewayError> {
    let Some(values) = metadata.as_object() else {
        return Err(ImageGatewayError::invalid_request(
            "metadata must be a JSON object",
            Some("metadata".to_string()),
            "invalid_metadata",
        ));
    };
    if values.len() > 16
        || values.iter().any(|(key, value)| {
            key.is_empty() || key.len() > 64 || value.as_str().is_none_or(|value| value.len() > 512)
        })
    {
        return Err(ImageGatewayError::invalid_request(
            "metadata supports at most 16 string pairs with bounded keys and values",
            Some("metadata".to_string()),
            "invalid_metadata",
        ));
    }
    Ok(())
}

fn validate_safe_snapshot(snapshot: &Value, param: &str) -> Result<(), ImageGatewayError> {
    if !snapshot.is_object() || contains_secret(snapshot) {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} must be a credential-free JSON object"),
            Some(param.to_string()),
            "unsafe_batch_snapshot",
        ));
    }
    Ok(())
}

fn contains_secret(value: &Value) -> bool {
    match value {
        Value::Object(values) => values.iter().any(|(key, value)| {
            let key = key.to_ascii_lowercase();
            matches!(
                key.as_str(),
                "authorization"
                    | "bearer"
                    | "headers"
                    | "api_key"
                    | "token"
                    | "access_token"
                    | "refresh_token"
                    | "id_token"
                    | "session_token"
                    | "cookie"
                    | "set-cookie"
                    | "password"
                    | "secret"
                    | "client_secret"
                    | "private_key"
            ) || contains_secret(value)
        }),
        Value::Array(values) => values.iter().any(contains_secret),
        Value::String(value) => {
            let lower = value.to_ascii_lowercase();
            lower.starts_with("bearer ") || lower.starts_with("sk-")
        }
        _ => false,
    }
}

fn request_hash(line: &ValidatedBatchLine) -> String {
    let bytes = serde_json::to_vec(&json!({
        "method": line.method,
        "url": line.url,
        "body": line.body,
    }))
    .unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

enum FinishedRequest {
    Completed(BatchRequestSuccess),
    Failed(Value),
}

fn finished_request_bytes(result: &FinishedRequest) -> Result<i64, ImageGatewayError> {
    let value = match result {
        FinishedRequest::Completed(result) => &result.body,
        FinishedRequest::Failed(error) => error,
    };
    let bytes = serde_json::to_vec(value)
        .map_err(|_| ImageGatewayError::internal("failed to measure batch result"))?;
    i64::try_from(bytes.len())
        .map_err(|_| ImageGatewayError::internal("batch result size does not fit in PostgreSQL"))
}

async fn finish_request(
    pool: &PgPool,
    lease: &BatchRequestLease,
    result: FinishedRequest,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let mut transaction = pool.begin().await.map_err(database_error)?;
    let state = lock_batch_state(&mut transaction, &lease.scope, &lease.batch_id).await?;
    if !matches!(
        state.status,
        BatchStatus::InProgress | BatchStatus::Cancelling
    ) {
        return Err(batch_lease_conflict());
    }
    let mut result = result;
    let mut result_bytes = finished_request_bytes(&result)?;
    if state
        .result_bytes
        .checked_add(result_bytes)
        .is_none_or(|total| total > MAX_BATCH_STORED_RESULT_BYTES as i64)
    {
        result = FinishedRequest::Failed(json!({
            "code": "batch_result_capacity_exceeded",
            "message": "The batch reached its stored result byte limit",
            "param": Value::Null,
            "type": "server_error",
        }));
        result_bytes = finished_request_bytes(&result)?;
    }
    let (response_status, response_request_id, response_body, error, terminal_state) = match result
    {
        FinishedRequest::Completed(result) => (
            Some(i16::try_from(result.status_code).map_err(|_| {
                ImageGatewayError::invalid_request(
                    "invalid batch response status",
                    Some("status_code".to_string()),
                    "invalid_batch_response",
                )
            })?),
            result.request_id,
            Some(result.body),
            None,
            "completed",
        ),
        FinishedRequest::Failed(error) => (None, None, None, Some(error), "failed"),
    };
    let changed = sqlx::query(
        r#"
        UPDATE project_batch_requests
        SET state = $8,
            response_status_code = $9,
            response_request_id = $10,
            response_body = $11,
            error = $12,
            lease_owner = NULL,
            lease_expires_at_ms = NULL,
            completed_at_ms = $7,
            updated_at_ms = $7
        WHERE tenant_id = $1
          AND project_id = $2
          AND batch_id = $3
          AND request_id = $4
          AND state = 'leased'
          AND lease_owner = $5
          AND lease_epoch = $6
        "#,
    )
    .bind(&lease.scope.tenant_id)
    .bind(&lease.scope.project_id)
    .bind(&lease.batch_id)
    .bind(lease.request_id)
    .bind(&lease.lease_owner)
    .bind(lease.lease_epoch)
    .bind(now)
    .bind(terminal_state)
    .bind(response_status)
    .bind(response_request_id)
    .bind(response_body)
    .bind(error)
    .execute(&mut *transaction)
    .await
    .map_err(database_error)?
    .rows_affected();
    if changed != 1 {
        return Err(batch_lease_conflict());
    }
    if terminal_state == "completed" {
        sqlx::query(
            r#"
            UPDATE project_batches
            SET request_count_completed = request_count_completed + 1,
                result_bytes = result_bytes + $5,
                updated_at_ms = $4
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .bind(now)
        .bind(result_bytes)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
    } else {
        sqlx::query(
            r#"
            UPDATE project_batches
            SET request_count_failed = request_count_failed + 1,
                result_bytes = result_bytes + $5,
                updated_at_ms = $4
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
            "#,
        )
        .bind(&lease.scope.tenant_id)
        .bind(&lease.scope.project_id)
        .bind(&lease.batch_id)
        .bind(now)
        .bind(result_bytes)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
    }
    transaction.commit().await.map_err(database_error)
}

async fn lock_batch_status(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    scope: &ProjectScope,
    batch_id: &str,
) -> Result<BatchStatus, ImageGatewayError> {
    Ok(lock_batch_state(transaction, scope, batch_id).await?.status)
}

struct LockedBatchState {
    status: BatchStatus,
    expires_at_ms: i64,
    cancel_requested_at_ms: Option<i64>,
    result_bytes: i64,
}

async fn lock_batch_state(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    scope: &ProjectScope,
    batch_id: &str,
) -> Result<LockedBatchState, ImageGatewayError> {
    let row: Option<(String, i64, Option<i64>, i64)> = sqlx::query_as(
        r#"
        SELECT status, expires_at_ms, cancel_requested_at_ms, result_bytes
        FROM project_batches
        WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
        FOR UPDATE
        "#,
    )
    .bind(&scope.tenant_id)
    .bind(&scope.project_id)
    .bind(batch_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?;
    let (status, expires_at_ms, cancel_requested_at_ms, result_bytes) =
        row.ok_or_else(batch_not_found)?;
    Ok(LockedBatchState {
        status: BatchStatus::from_str(&status)?,
        expires_at_ms,
        cancel_requested_at_ms,
        result_bytes,
    })
}

#[derive(FromRow)]
struct ProjectFileRow {
    file_id: String,
    tenant_id: String,
    project_id: String,
    purpose: String,
    filename: String,
    storage_backend: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
    created_at_ms: i64,
    expires_at_ms: Option<i64>,
    deleted_at_ms: Option<i64>,
}

#[derive(FromRow)]
struct ProjectFileCleanupLeaseRow {
    file_id: String,
    tenant_id: String,
    project_id: String,
    storage_backend: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
    cleanup_lease_owner: String,
    cleanup_lease_epoch: i64,
    cleanup_lease_expires_at_ms: i64,
}

impl ProjectFileCleanupLeaseRow {
    fn into_lease(self) -> Result<ProjectFileCleanupLease, ImageGatewayError> {
        Ok(ProjectFileCleanupLease {
            scope: ProjectScope::new(self.tenant_id, self.project_id),
            file_id: self.file_id,
            blob: BatchFileBlob {
                storage_backend: self.storage_backend,
                object_key: self.object_key,
                sha256_hex: self.sha256_hex,
                byte_size: u64::try_from(self.byte_size)
                    .map_err(|_| ImageGatewayError::internal("stored file size is invalid"))?,
            },
            lease_owner: self.cleanup_lease_owner,
            lease_epoch: self.cleanup_lease_epoch,
            lease_expires_at_ms: self.cleanup_lease_expires_at_ms,
        })
    }
}

impl ProjectFileRow {
    fn blob(&self) -> Result<BatchFileBlob, ImageGatewayError> {
        Ok(BatchFileBlob {
            storage_backend: self.storage_backend.clone(),
            object_key: self.object_key.clone(),
            sha256_hex: self.sha256_hex.clone(),
            byte_size: u64::try_from(self.byte_size)
                .map_err(|_| ImageGatewayError::internal("stored file size is invalid"))?,
        })
    }

    fn into_view(self) -> Result<ProjectFile, ImageGatewayError> {
        Ok(ProjectFile {
            id: self.file_id,
            tenant_id: self.tenant_id,
            project_id: self.project_id,
            purpose: ProjectFilePurpose::from_str(&self.purpose)?,
            filename: self.filename,
            bytes: u64::try_from(self.byte_size)
                .map_err(|_| ImageGatewayError::internal("stored file size is invalid"))?,
            sha256_hex: self.sha256_hex,
            created_at_ms: self.created_at_ms,
            expires_at_ms: self.expires_at_ms,
            deleted_at_ms: self.deleted_at_ms,
        })
    }
}

#[derive(FromRow)]
struct BatchRow {
    batch_id: String,
    tenant_id: String,
    project_id: String,
    input_file_id: String,
    endpoint: String,
    model: String,
    completion_window: String,
    status: String,
    metadata: Value,
    errors: Option<Value>,
    request_count_total: i32,
    request_count_completed: i32,
    request_count_failed: i32,
    request_count_cancelled: i32,
    created_at_ms: i64,
    in_progress_at_ms: Option<i64>,
    finalizing_at_ms: Option<i64>,
    completed_at_ms: Option<i64>,
    failed_at_ms: Option<i64>,
    expires_at_ms: i64,
    cancel_requested_at_ms: Option<i64>,
    cancelled_at_ms: Option<i64>,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
}

impl BatchRow {
    fn into_view(self) -> Result<ProjectBatch, ImageGatewayError> {
        Ok(ProjectBatch {
            id: self.batch_id,
            tenant_id: self.tenant_id,
            project_id: self.project_id,
            input_file_id: self.input_file_id,
            endpoint: self.endpoint,
            model: self.model,
            completion_window: self.completion_window,
            status: BatchStatus::from_str(&self.status)?,
            metadata: self.metadata,
            errors: self.errors,
            request_counts: BatchRequestCounts {
                total: nonnegative_u32(self.request_count_total)?,
                completed: nonnegative_u32(self.request_count_completed)?,
                failed: nonnegative_u32(self.request_count_failed)?,
                cancelled: nonnegative_u32(self.request_count_cancelled)?,
            },
            output_file_id: self.output_file_id,
            error_file_id: self.error_file_id,
            created_at_ms: self.created_at_ms,
            in_progress_at_ms: self.in_progress_at_ms,
            finalizing_at_ms: self.finalizing_at_ms,
            completed_at_ms: self.completed_at_ms,
            failed_at_ms: self.failed_at_ms,
            expires_at_ms: self.expires_at_ms,
            cancel_requested_at_ms: self.cancel_requested_at_ms,
            cancelled_at_ms: self.cancelled_at_ms,
        })
    }
}

#[derive(FromRow)]
struct BatchRequestLeaseRow {
    request_id: Uuid,
    ordinal: i32,
    custom_id: String,
    method: String,
    request_url: String,
    model: String,
    request_body: Value,
    request_hash: String,
    attempt_count: i32,
    lease_owner: Option<String>,
    lease_epoch: i64,
    lease_expires_at_ms: Option<i64>,
}

impl BatchRequestLeaseRow {
    fn into_lease(
        self,
        scope: &ProjectScope,
        batch_id: &str,
    ) -> Result<BatchRequestLease, ImageGatewayError> {
        Ok(BatchRequestLease {
            scope: scope.clone(),
            batch_id: batch_id.to_string(),
            request_id: self.request_id,
            ordinal: u32::try_from(self.ordinal)
                .map_err(|_| ImageGatewayError::internal("stored batch ordinal is invalid"))?,
            custom_id: self.custom_id,
            method: self.method,
            url: self.request_url,
            model: self.model,
            body: self.request_body,
            request_hash: self.request_hash,
            attempt_count: u32::try_from(self.attempt_count).map_err(|_| {
                ImageGatewayError::internal("stored batch attempt count is invalid")
            })?,
            lease_owner: self
                .lease_owner
                .ok_or_else(|| ImageGatewayError::internal("claimed batch request has no owner"))?,
            lease_epoch: self.lease_epoch,
            lease_expires_at_ms: self.lease_expires_at_ms.ok_or_else(|| {
                ImageGatewayError::internal("claimed batch request has no expiry")
            })?,
        })
    }
}

#[derive(FromRow)]
struct BatchFinalizationRow {
    lease_epoch: i64,
    lease_expires_at_ms: i64,
    status: String,
}

#[derive(FromRow)]
struct BatchResultRow {
    request_id: Uuid,
    custom_id: String,
    response_status_code: Option<i16>,
    response_request_id: Option<String>,
    response_body: Option<Value>,
    error: Option<Value>,
}

#[derive(FromRow)]
struct BatchRetentionRow {
    output_retention_seconds: i32,
    #[allow(dead_code)]
    status: String,
}

#[derive(FromRow)]
struct BatchWorkTargetRow {
    tenant_id: String,
    project_id: String,
    batch_id: String,
    status: String,
    expires_at_ms: i64,
}

impl BatchWorkTargetRow {
    fn into_target(self) -> Result<BatchWorkTarget, ImageGatewayError> {
        Ok(BatchWorkTarget {
            scope: ProjectScope::new(self.tenant_id, self.project_id),
            batch_id: self.batch_id,
            status: BatchStatus::from_str(&self.status)?,
            expires_at_ms: self.expires_at_ms,
        })
    }
}

#[derive(FromRow)]
struct BatchExecutionSnapshotRow {
    auth_snapshot: Value,
    route_snapshot: Value,
}

async fn load_file_row(
    pool: &PgPool,
    scope: &ProjectScope,
    file_id: &str,
    now: i64,
) -> Result<ProjectFileRow, ImageGatewayError> {
    sqlx::query_as::<_, ProjectFileRow>(
        r#"
        SELECT file_id, tenant_id, project_id, purpose, filename,
               storage_backend, object_key, sha256_hex, byte_size,
               created_at_ms, expires_at_ms, deleted_at_ms
        FROM project_files
        WHERE tenant_id = $1
          AND project_id = $2
          AND file_id = $3
          AND state = 'active'
          AND (expires_at_ms IS NULL OR expires_at_ms > $4)
        "#,
    )
    .bind(&scope.tenant_id)
    .bind(&scope.project_id)
    .bind(file_id)
    .bind(now)
    .fetch_optional(pool)
    .await
    .map_err(database_error)?
    .ok_or_else(file_not_found)
}

async fn load_batch_row(
    pool: &PgPool,
    scope: &ProjectScope,
    batch_id: &str,
) -> Result<BatchRow, ImageGatewayError> {
    sqlx::query_as::<_, BatchRow>(
        r#"
        SELECT batch.batch_id, batch.tenant_id, batch.project_id,
               batch.input_file_id, batch.endpoint, batch.model,
               batch.completion_window, batch.status, batch.metadata,
               batch.errors, batch.request_count_total,
               batch.request_count_completed, batch.request_count_failed,
               batch.request_count_cancelled, batch.created_at_ms,
               batch.in_progress_at_ms, batch.finalizing_at_ms,
               batch.completed_at_ms, batch.failed_at_ms,
               batch.expires_at_ms, batch.cancel_requested_at_ms,
               batch.cancelled_at_ms,
               output.file_id AS output_file_id,
               error.file_id AS error_file_id
        FROM project_batches batch
        LEFT JOIN project_batch_output_files output
          ON output.batch_id = batch.batch_id
         AND output.project_id = batch.project_id
         AND output.tenant_id = batch.tenant_id
         AND output.role = 'output'
        LEFT JOIN project_batch_output_files error
          ON error.batch_id = batch.batch_id
         AND error.project_id = batch.project_id
         AND error.tenant_id = batch.tenant_id
         AND error.role = 'error'
        WHERE batch.tenant_id = $1
          AND batch.project_id = $2
          AND batch.batch_id = $3
        "#,
    )
    .bind(&scope.tenant_id)
    .bind(&scope.project_id)
    .bind(batch_id)
    .fetch_optional(pool)
    .await
    .map_err(database_error)?
    .ok_or_else(batch_not_found)
}

async fn ensure_project_exists(
    pool: &PgPool,
    scope: &ProjectScope,
) -> Result<(), ImageGatewayError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM gateway_projects WHERE id = $1 AND tenant_id = $2)",
    )
    .bind(&scope.project_id)
    .bind(&scope.tenant_id)
    .fetch_one(pool)
    .await
    .map_err(database_error)?;
    if exists {
        Ok(())
    } else {
        Err(ImageGatewayError::not_found(
            "Project not found",
            Some("project_id".to_string()),
            "project_not_found",
        ))
    }
}

async fn ensure_file_cursor(
    pool: &PgPool,
    scope: &ProjectScope,
    cursor: &str,
) -> Result<(), ImageGatewayError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM project_files
            WHERE tenant_id = $1 AND project_id = $2 AND file_id = $3
        )
        "#,
    )
    .bind(&scope.tenant_id)
    .bind(&scope.project_id)
    .bind(cursor)
    .fetch_one(pool)
    .await
    .map_err(database_error)?;
    if exists {
        Ok(())
    } else {
        Err(invalid_cursor())
    }
}

async fn ensure_batch_cursor(
    pool: &PgPool,
    scope: &ProjectScope,
    cursor: &str,
) -> Result<(), ImageGatewayError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM project_batches
            WHERE tenant_id = $1 AND project_id = $2 AND batch_id = $3
        )
        "#,
    )
    .bind(&scope.tenant_id)
    .bind(&scope.project_id)
    .bind(cursor)
    .fetch_one(pool)
    .await
    .map_err(database_error)?;
    if exists {
        Ok(())
    } else {
        Err(invalid_cursor())
    }
}

fn validate_file_size(
    purpose: ProjectFilePurpose,
    byte_size: u64,
) -> Result<(), ImageGatewayError> {
    let limit = if purpose == ProjectFilePurpose::Batch {
        MAX_BATCH_FILE_BYTES
    } else {
        MAX_FILE_BYTES
    };
    if byte_size == 0 {
        return Err(ImageGatewayError::invalid_request(
            "file must not be empty",
            Some("file".to_string()),
            "invalid_file",
        ));
    }
    if byte_size > limit {
        return Err(ImageGatewayError::payload_too_large(format!(
            "file exceeds the {} byte limit for this purpose",
            limit
        )));
    }
    Ok(())
}

fn normalize_file_retention(
    purpose: ProjectFilePurpose,
    retention: Option<Duration>,
) -> Result<Option<Duration>, ImageGatewayError> {
    let retention = retention.or_else(|| {
        matches!(
            purpose,
            ProjectFilePurpose::Batch | ProjectFilePurpose::BatchOutput
        )
        .then(|| Duration::from_secs(u64::from(DEFAULT_BATCH_RETENTION_SECONDS)))
    });
    if let Some(retention) = retention
        && !(u64::from(MIN_FILE_RETENTION_SECONDS)..=u64::from(MAX_FILE_RETENTION_SECONDS))
            .contains(&retention.as_secs())
    {
        return Err(ImageGatewayError::invalid_request(
            "file expiration must be between 1 hour and 30 days",
            Some("expires_after".to_string()),
            "invalid_expiration",
        ));
    }
    Ok(retention)
}

fn validate_scope(scope: &ProjectScope) -> Result<(), ImageGatewayError> {
    if scope.tenant_id.trim().is_empty() || scope.project_id.trim().is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "tenant_id and project_id are required",
            Some("project_id".to_string()),
            "invalid_project_scope",
        ));
    }
    Ok(())
}

fn validate_filename(filename: &str) -> Result<(), ImageGatewayError> {
    if filename.trim().is_empty()
        || filename.len() > 512
        || filename.chars().any(|character| character.is_control())
    {
        return Err(ImageGatewayError::invalid_request(
            "filename must contain 1 to 512 non-control characters",
            Some("file".to_string()),
            "invalid_filename",
        ));
    }
    Ok(())
}

fn validate_worker(worker_id: &str) -> Result<(), ImageGatewayError> {
    if worker_id.trim().is_empty() || worker_id.len() > 256 {
        return Err(ImageGatewayError::invalid_request(
            "worker_id must contain 1 to 256 characters",
            None,
            "invalid_worker_id",
        ));
    }
    Ok(())
}

fn validate_limit(limit: usize, maximum: usize) -> Result<(), ImageGatewayError> {
    if (1..=maximum).contains(&limit) {
        Ok(())
    } else {
        Err(ImageGatewayError::invalid_request(
            format!("limit must be between 1 and {maximum}"),
            Some("limit".to_string()),
            "invalid_limit",
        ))
    }
}

fn validate_batch_id(batch_id: &str) -> Result<Uuid, ImageGatewayError> {
    validate_prefixed_uuid(batch_id, "batch-")
}

fn validate_prefixed_uuid(value: &str, prefix: &str) -> Result<Uuid, ImageGatewayError> {
    value
        .strip_prefix(prefix)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Resource not found",
                None,
                if prefix == "file-" {
                    "file_not_found"
                } else {
                    "batch_not_found"
                },
            )
        })
}

fn valid_lease_ms(duration: Duration) -> Result<i64, ImageGatewayError> {
    let duration = duration_ms(duration)?;
    if !(1_000..=65 * 60 * 1_000).contains(&duration) {
        return Err(ImageGatewayError::invalid_request(
            "lease duration must be between 1 second and 65 minutes",
            None,
            "invalid_lease_duration",
        ));
    }
    Ok(duration)
}

fn duration_ms(duration: Duration) -> Result<i64, ImageGatewayError> {
    i64::try_from(duration.as_millis()).map_err(|_| {
        ImageGatewayError::invalid_request("duration is too large", None, "invalid_duration")
    })
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock is before the Unix epoch"))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| ImageGatewayError::internal("system clock does not fit in milliseconds"))
}

fn nonnegative_u32(value: i32) -> Result<u32, ImageGatewayError> {
    u32::try_from(value).map_err(|_| ImageGatewayError::internal("stored count is invalid"))
}

fn file_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found("File not found", None, "file_not_found")
}

fn project_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Project not found",
        Some("project_id".to_string()),
        "project_not_found",
    )
}

fn project_file_capacity_exceeded(limit_bytes: i64, limit_count: i32) -> ImageGatewayError {
    ImageGatewayError::conflict(
        format!(
            "Project file storage capacity exceeded (maximum {limit_bytes} bytes and {limit_count} files)"
        ),
        Some("file".to_string()),
        "project_file_capacity_exceeded",
    )
}

fn batch_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found("Batch not found", None, "batch_not_found")
}

fn invalid_cursor() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "Pagination cursor is invalid for this project",
        Some("after".to_string()),
        "invalid_cursor",
    )
}

fn batch_state_conflict(message: &'static str) -> ImageGatewayError {
    ImageGatewayError::conflict(message, None, "batch_state_conflict")
}

fn batch_lease_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Batch lease is stale or no longer owned by this worker",
        None,
        "batch_lease_conflict",
    )
}

fn file_cleanup_lease_conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "File cleanup lease is stale or no longer owned by this worker",
        None,
        "file_cleanup_lease_conflict",
    )
}

fn blob_write_error(error: BatchFileBlobError) -> ImageGatewayError {
    match error {
        BatchFileBlobError::Integrity => {
            ImageGatewayError::internal("batch file blob failed integrity verification")
        }
        BatchFileBlobError::Unavailable => {
            ImageGatewayError::service_unavailable("batch file storage unavailable")
        }
    }
}

fn blob_read_error(error: BatchFileBlobError) -> ImageGatewayError {
    match error {
        BatchFileBlobError::Integrity => {
            ImageGatewayError::internal("stored batch file failed integrity verification")
        }
        BatchFileBlobError::Unavailable => {
            ImageGatewayError::service_unavailable("batch file storage unavailable")
        }
    }
}

fn batch_insert_error(error: sqlx::Error) -> ImageGatewayError {
    if let sqlx::Error::Database(database) = &error
        && database.code().as_deref() == Some("23505")
    {
        return ImageGatewayError::invalid_request(
            "custom_id or ordinal is duplicated within the batch",
            Some("input_file_id".to_string()),
            "duplicate_custom_id",
        );
    }
    database_error(error)
}

fn database_error(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error = %error, "batch persistence operation failed");
    ImageGatewayError::service_unavailable("batch persistence unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_lease_accepts_maximum_provider_timeout_with_grace() {
        assert_eq!(
            valid_lease_ms(Duration::from_secs(60 * 60 + 90)).unwrap(),
            3_690_000
        );
    }

    #[test]
    fn batch_lease_rejects_duration_above_operational_ceiling() {
        let error = valid_lease_ms(Duration::from_secs(65 * 60 + 1)).unwrap_err();
        assert_eq!(error.error_code(), Some("invalid_lease_duration"));
    }
}
