use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use super::{
    ImageSize, MediaAsset, MediaScope, SCHEMA_VERSION, SegmentError, SegmentStatus, SegmentStore,
    SegmentTimings, SegmentWork, Segmentation, SourceState, now_ms, source_unavailable,
};
use crate::{ImageGatewayError, input_blobs::InputBlobRef};

const MAX_PROJECT_SOURCES: i64 = 64;
const MAX_PROJECT_METADATA: i64 = 4096;
const MAX_PROJECT_ASSET_BYTES: i64 = 512 * 1024 * 1024;
const MAX_ACTIVE_RESULTS: i64 = 64;
const MAX_PROJECT_ACTIVE_RESULTS: i64 = 16;
const QUEUE_WAIT_MS: i64 = 5 * 60 * 1_000;

#[derive(Clone)]
pub struct PostgresSegmentStore {
    pool: PgPool,
}

#[derive(Deserialize, Serialize)]
struct AssetMetadata {
    image: ImageSize,
    blob: InputBlobRef,
}

#[derive(FromRow)]
struct AssetRow {
    asset_id: Uuid,
    tenant_id: String,
    project_id: String,
    owner_id: String,
    digest: String,
    metadata: serde_json::Value,
    byte_size: i64,
    source_state: SourceState,
    expires_at_ms: i64,
}

#[derive(FromRow)]
struct ResultRow {
    result_json: serde_json::Value,
    status: String,
    queue_deadline_at_ms: i64,
    lease_deadline_at_ms: Option<i64>,
}

#[derive(FromRow)]
struct WorkRow {
    result_id: Uuid,
    lease_token: Uuid,
    result_json: serde_json::Value,
    asset_id: Uuid,
    tenant_id: String,
    project_id: String,
    owner_id: String,
    digest: String,
    metadata: serde_json::Value,
    byte_size: i64,
    source_state: SourceState,
    expires_at_ms: i64,
}

impl PostgresSegmentStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn record_worker_heartbeat(
        &self,
        analyzer_key: &str,
        release_terminal_sources: bool,
    ) -> Result<(), ImageGatewayError> {
        let now = now_ms();
        sqlx::query("INSERT INTO media_segment_worker_heartbeats (analyzer_key, observed_at_ms, release_terminal_sources) VALUES ($1, $2, $3) ON CONFLICT (analyzer_key, release_terminal_sources) DO UPDATE SET observed_at_ms = EXCLUDED.observed_at_ms")
            .bind(analyzer_key).bind(now).bind(release_terminal_sources).execute(&self.pool).await.map_err(unavailable)?;
        sqlx::query("DELETE FROM media_segment_worker_heartbeats WHERE observed_at_ms < $1")
            .bind(now - super::ASSET_TTL_MS)
            .execute(&self.pool)
            .await
            .map_err(unavailable)?;
        Ok(())
    }
}

#[async_trait]
impl SegmentStore for PostgresSegmentStore {
    async fn worker_heartbeat(
        &self,
        analyzer_key: &str,
    ) -> Result<Option<super::SegmentWorkerHeartbeat>, ImageGatewayError> {
        // Analysis liveness is config-specific, but every segmentd maintains
        // shared source storage. Old analyzer revisions must not hide a live
        // source-deleting worker during a configuration rollback.
        sqlx::query_as(
            r#"SELECT observed_at_ms, release_terminal_sources,
                (SELECT COUNT(DISTINCT release_terminal_sources) > 1
                 FROM media_segment_worker_heartbeats WHERE observed_at_ms >= $2)
                    AS configuration_mismatch
               FROM media_segment_worker_heartbeats WHERE analyzer_key = $1
               ORDER BY observed_at_ms DESC LIMIT 1"#,
        )
        .bind(analyzer_key)
        .bind(now_ms() - super::WORKER_HEARTBEAT_TTL_MS)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)
    }

    async fn result_for_analyzer(
        &self,
        scope: &MediaScope,
        asset_id: Uuid,
        analyzer_key: &str,
    ) -> Result<Option<Segmentation>, ImageGatewayError> {
        let row = sqlx::query_as::<_, ResultRow>(
            r#"SELECT result.result_json, result.status, result.queue_deadline_at_ms, result.lease_deadline_at_ms
               FROM media_segment_results result
               JOIN media_segment_assets asset ON asset.asset_id = result.asset_id
               WHERE result.asset_id = $1 AND result.analyzer_key = $2
                 AND result.tenant_id = $3 AND result.project_id = $4 AND result.owner_id = $5
                 AND asset.expires_at_ms > $6
               ORDER BY result.created_at_ms DESC LIMIT 1"#,
        ).bind(asset_id).bind(analyzer_key).bind(&scope.tenant_id).bind(&scope.project_id).bind(&scope.owner_id).bind(now_ms())
            .fetch_optional(&self.pool).await.map_err(unavailable)?;
        row.map(|row| result_from_row(row, now_ms())).transpose()
    }

    async fn find_asset(
        &self,
        scope: &MediaScope,
        digest: &str,
    ) -> Result<Option<MediaAsset>, ImageGatewayError> {
        let now = now_ms();
        let row = sqlx::query_as::<_, AssetRow>(
            r#"
            SELECT asset_id, tenant_id, project_id, owner_id, digest,
                   metadata, byte_size, source_state, expires_at_ms
            FROM media_segment_assets
            WHERE tenant_id = $1 AND project_id = $2 AND owner_id = $3
              AND digest = $4 AND expires_at_ms > $5
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(&scope.owner_id)
        .bind(digest)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(asset_from_row).transpose()
    }

    async fn insert_asset(&self, asset: &MediaAsset) -> Result<MediaAsset, ImageGatewayError> {
        let byte_size = i64::try_from(asset.blob.byte_size)
            .map_err(|_| ImageGatewayError::internal("Media asset size overflow"))?;
        let metadata = serde_json::to_value(AssetMetadata {
            image: asset.image.clone(),
            blob: asset.blob.clone(),
        })
        .map_err(|_| ImageGatewayError::internal("Media asset metadata encoding failed"))?;
        let now = now_ms();
        if byte_size <= 0 || asset.expires_at_ms <= now {
            return Err(ImageGatewayError::conflict(
                "Media asset expired before it could be registered",
                None,
                "asset_expired",
            ));
        }

        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "media-segment-assets:{}:{}",
                asset.scope.tenant_id, asset.scope.project_id
            ))
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;

        // Project lock bounds concurrent admissions; the asset row lock also
        // serializes source restoration with cleanup and enqueue.
        let existing = asset_by_digest_any_age(&mut tx, &asset.scope, &asset.digest).await?;
        if let Some(row) = &existing {
            if row.expires_at_ms <= now {
                return Err(ImageGatewayError::conflict(
                    "An expired copy of this media asset is pending cleanup; retry shortly",
                    None,
                    "asset_expired",
                ));
            }
            match row.source_state {
                SourceState::Retained => {
                    tx.commit().await.map_err(unavailable)?;
                    return asset_from_row(existing.expect("existing asset"));
                }
                SourceState::Releasing => return Err(source_unavailable(row.source_state)),
                SourceState::Released => {}
            }
        }

        let (metadata_count, asset_count, stored_bytes): (i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT COUNT(*)::BIGINT,
                   COUNT(*) FILTER (WHERE source_state <> 'released')::BIGINT,
                   COALESCE(SUM(byte_size) FILTER (WHERE source_state <> 'released'), 0)::BIGINT
            FROM media_segment_assets
            WHERE tenant_id = $1 AND project_id = $2
            "#,
        )
        .bind(&asset.scope.tenant_id)
        .bind(&asset.scope.project_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if (existing.is_none() && metadata_count >= MAX_PROJECT_METADATA)
            || asset_count >= MAX_PROJECT_SOURCES
            || stored_bytes.saturating_add(byte_size) > MAX_PROJECT_ASSET_BYTES
        {
            return Err(ImageGatewayError::conflict(
                "Media asset storage limit reached for this project",
                None,
                "media_asset_capacity_exceeded",
            ));
        }

        if let Some(row) = existing {
            let id = row.asset_id;
            let expires_at_ms = row.expires_at_ms;
            sqlx::query("UPDATE media_segment_assets SET metadata = $2, byte_size = $3, source_state = 'retained', source_waiting_for_result = true WHERE asset_id = $1")
                .bind(id).bind(metadata).bind(byte_size).execute(&mut *tx).await.map_err(unavailable)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(MediaAsset {
                id,
                expires_at_ms,
                ..asset.clone()
            });
        }

        let inserted = sqlx::query(
            r#"
            INSERT INTO media_segment_assets
                (asset_id, tenant_id, project_id, owner_id, digest, metadata,
                 byte_size, expires_at_ms, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tenant_id, project_id, owner_id, digest) DO NOTHING
            "#,
        )
        .bind(asset.id)
        .bind(&asset.scope.tenant_id)
        .bind(&asset.scope.project_id)
        .bind(&asset.scope.owner_id)
        .bind(&asset.digest)
        .bind(metadata)
        .bind(byte_size)
        .bind(asset.expires_at_ms)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;

        if inserted.rows_affected() == 1 {
            tx.commit().await.map_err(unavailable)?;
            return Ok(asset.clone());
        }
        let winner = asset_by_digest_any_age(&mut tx, &asset.scope, &asset.digest).await?;
        tx.commit().await.map_err(unavailable)?;
        match winner {
            Some(row) if row.expires_at_ms > now => asset_from_row(row),
            Some(_) => Err(ImageGatewayError::conflict(
                "An expired copy of this media asset is pending cleanup; retry shortly",
                None,
                "asset_expired",
            )),
            None => Err(ImageGatewayError::service_unavailable(
                "Media asset registration lost its uniqueness race",
            )),
        }
    }

    async fn get_asset(
        &self,
        scope: &MediaScope,
        id: Uuid,
    ) -> Result<Option<MediaAsset>, ImageGatewayError> {
        let row = sqlx::query_as::<_, AssetRow>(
            r#"
            SELECT asset_id, tenant_id, project_id, owner_id, digest,
                   metadata, byte_size, source_state, expires_at_ms
            FROM media_segment_assets
            WHERE asset_id = $1 AND tenant_id = $2 AND project_id = $3
              AND owner_id = $4 AND expires_at_ms > $5
            "#,
        )
        .bind(id)
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(&scope.owner_id)
        .bind(now_ms())
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(asset_from_row).transpose()
    }

    async fn cached(
        &self,
        scope: &MediaScope,
        cache_key: &str,
    ) -> Result<Option<Segmentation>, ImageGatewayError> {
        let row = sqlx::query_as::<_, ResultRow>(
            r#"
            SELECT result.result_json, result.status,
                   result.queue_deadline_at_ms, result.lease_deadline_at_ms
            FROM media_segment_results result
            JOIN media_segment_assets asset ON asset.asset_id = result.asset_id
            WHERE result.tenant_id = $1 AND result.project_id = $2
              AND result.owner_id = $3 AND result.cache_key = $4
              AND asset.expires_at_ms > $5
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(&scope.owner_id)
        .bind(cache_key)
        .bind(now_ms())
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(|row| result_from_row(row, now_ms())).transpose()
    }

    async fn get(
        &self,
        scope: &MediaScope,
        id: Uuid,
    ) -> Result<Option<Segmentation>, ImageGatewayError> {
        let row = sqlx::query_as::<_, ResultRow>(
            r#"
            SELECT result.result_json, result.status,
                   result.queue_deadline_at_ms, result.lease_deadline_at_ms
            FROM media_segment_results result
            JOIN media_segment_assets asset ON asset.asset_id = result.asset_id
            WHERE result.result_id = $1 AND result.tenant_id = $2
              AND result.project_id = $3 AND result.owner_id = $4
              AND asset.expires_at_ms > $5
            "#,
        )
        .bind(id)
        .bind(&scope.tenant_id)
        .bind(&scope.project_id)
        .bind(&scope.owner_id)
        .bind(now_ms())
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(|row| result_from_row(row, now_ms())).transpose()
    }

    async fn enqueue(
        &self,
        asset: &MediaAsset,
        cache_key: &str,
        analyzer_key: &str,
    ) -> Result<Segmentation, ImageGatewayError> {
        if let Some(result) = self.cached(&asset.scope, cache_key).await? {
            return Ok(result);
        }
        let now = now_ms();
        let mut tx = self.pool.begin().await.map_err(unavailable)?;

        let source_state: Option<SourceState> = sqlx::query_scalar(
            r#"
                SELECT source_state FROM media_segment_assets
                WHERE asset_id = $1 AND tenant_id = $2 AND project_id = $3
                  AND owner_id = $4 AND expires_at_ms > $5
                FOR UPDATE
            "#,
        )
        .bind(asset.id)
        .bind(&asset.scope.tenant_id)
        .bind(&asset.scope.project_id)
        .bind(&asset.scope.owner_id)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if source_state.is_none() {
            return Err(ImageGatewayError::conflict(
                "Media asset is missing or expired",
                None,
                "asset_expired",
            ));
        }

        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('media-segments-queue-v1', 0))")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        if let Some(row) = result_by_cache(&mut tx, &asset.scope, cache_key).await? {
            tx.commit().await.map_err(unavailable)?;
            return result_from_row(row, now);
        }
        if source_state != Some(SourceState::Retained) {
            return Err(source_unavailable(
                source_state.expect("existing source state"),
            ));
        }

        let (active, project_active): (i64, i64) = sqlx::query_as(
            r#"
            SELECT COUNT(*)::BIGINT,
                   COUNT(*) FILTER (
                       WHERE tenant_id = $1 AND project_id = $2
                   )::BIGINT
            FROM media_segment_results
            WHERE (status = 'processing' AND lease_deadline_at_ms > $3)
               OR (status = 'queued' AND queue_deadline_at_ms > $3)
            "#,
        )
        .bind(&asset.scope.tenant_id)
        .bind(&asset.scope.project_id)
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if active >= MAX_ACTIVE_RESULTS || project_active >= MAX_PROJECT_ACTIVE_RESULTS {
            return Err(ImageGatewayError::queue_overloaded_for("bbox sidecar"));
        }

        let id = Uuid::new_v4();
        let result = Segmentation {
            object: "media.segmentation".into(),
            id: format!("seg_{}", id.simple()),
            asset_id: format!("img_{}", asset.id.simple()),
            status: SegmentStatus::Processing,
            schema_version: SCHEMA_VERSION.into(),
            image: asset.image.clone(),
            groups: Vec::new(),
            error: None,
        };
        let result_json = serde_json::to_value(&result)
            .map_err(|_| ImageGatewayError::internal("Segmentation encoding failed"))?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO media_segment_results
                (result_id, asset_id, tenant_id, project_id, owner_id,
                 cache_key, analyzer_key, status, lease_token,
                 lease_deadline_at_ms, queue_deadline_at_ms, result_json,
                 timings_json, created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'queued', NULL, NULL,
                    $8, $9, NULL, $10, $10)
            ON CONFLICT (tenant_id, project_id, owner_id, cache_key) DO NOTHING
            "#,
        )
        .bind(id)
        .bind(asset.id)
        .bind(&asset.scope.tenant_id)
        .bind(&asset.scope.project_id)
        .bind(&asset.scope.owner_id)
        .bind(cache_key)
        .bind(analyzer_key)
        .bind(now.saturating_add(QUEUE_WAIT_MS))
        .bind(result_json)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        if inserted.rows_affected() == 0 {
            let row = result_by_cache(&mut tx, &asset.scope, cache_key)
                .await?
                .ok_or_else(|| {
                    ImageGatewayError::service_unavailable(
                        "Segmentation enqueue lost its uniqueness race",
                    )
                })?;
            tx.commit().await.map_err(unavailable)?;
            return result_from_row(row, now);
        }

        sqlx::query(
            "UPDATE media_segment_assets SET source_waiting_for_result = false WHERE asset_id = $1",
        )
        .bind(asset.id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query("SELECT pg_notify('media_segments_ready', '')")
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(result)
    }

    async fn claim(
        &self,
        analyzer_key: &str,
        lease_ms: i64,
    ) -> Result<Option<SegmentWork>, ImageGatewayError> {
        if lease_ms <= 0 {
            return Err(ImageGatewayError::internal(
                "Segmentation lease duration must be positive",
            ));
        }
        let now = now_ms();
        let lease_token = Uuid::new_v4();
        let row = sqlx::query_as::<_, WorkRow>(
            r#"
            WITH candidate AS (
                SELECT result.result_id
                FROM media_segment_results result
                JOIN media_segment_assets asset ON asset.asset_id = result.asset_id
                WHERE result.status = 'queued' AND result.analyzer_key = $1
                  AND result.queue_deadline_at_ms > $2
                  AND asset.expires_at_ms > $2
                  AND asset.source_state = 'retained'
                ORDER BY result.created_at_ms, result.result_id
                FOR UPDATE OF result SKIP LOCKED
                LIMIT 1
            )
            UPDATE media_segment_results result
            SET status = 'processing', lease_token = $3,
                lease_deadline_at_ms = $4, updated_at_ms = $2
            FROM candidate, media_segment_assets asset
            WHERE result.result_id = candidate.result_id
              AND asset.asset_id = result.asset_id
              AND asset.expires_at_ms > $2
            RETURNING result.result_id, result.lease_token, result.result_json,
                      asset.asset_id, asset.tenant_id, asset.project_id,
                      asset.owner_id, asset.digest, asset.metadata,
                      asset.byte_size, asset.source_state, asset.expires_at_ms
            "#,
        )
        .bind(analyzer_key)
        .bind(now)
        .bind(lease_token)
        .bind(now.saturating_add(lease_ms))
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(work_from_row).transpose()
    }

    async fn finish(
        &self,
        work: &SegmentWork,
        result: &Segmentation,
        timings: &SegmentTimings,
    ) -> Result<bool, ImageGatewayError> {
        if result.id != work.result.id || result.asset_id != work.result.asset_id {
            return Err(ImageGatewayError::internal(
                "Segmentation terminal result identity changed",
            ));
        }
        let status = match result.status {
            SegmentStatus::Completed => "completed",
            SegmentStatus::Failed => "failed",
            SegmentStatus::Processing => {
                return Err(ImageGatewayError::internal(
                    "Segmentation terminal result is still processing",
                ));
            }
        };
        let result_json = serde_json::to_value(result)
            .map_err(|_| ImageGatewayError::internal("Segmentation encoding failed"))?;
        let timings_json = serde_json::to_value(timings)
            .map_err(|_| ImageGatewayError::internal("Segmentation timings encoding failed"))?;
        let now = now_ms();
        let updated = sqlx::query(
            r#"
            UPDATE media_segment_results
            SET status = $4, lease_token = NULL, lease_deadline_at_ms = NULL,
                result_json = $5, timings_json = $6, updated_at_ms = $7
            WHERE result_id = $1 AND asset_id = $2 AND lease_token = $3
              AND status = 'processing' AND lease_deadline_at_ms > $7
            "#,
        )
        .bind(work.id)
        .bind(work.asset.id)
        .bind(work.lease_token)
        .bind(status)
        .bind(result_json)
        .bind(timings_json)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn expire_leases(&self) -> Result<u64, ImageGatewayError> {
        let now = now_ms();
        let updated = sqlx::query(
            r#"
            UPDATE media_segment_results
            SET status = 'failed', lease_token = NULL,
                lease_deadline_at_ms = NULL, updated_at_ms = $1,
                result_json = (result_json - 'status' - 'groups' - 'error')
                    || jsonb_build_object(
                        'status', 'failed',
                        'groups', '[]'::JSONB,
                        'error', CASE WHEN status = 'queued'
                            THEN jsonb_build_object(
                                'code', 'bbox_queue_timeout',
                                'message', '图片分析排队超时，请稍后再试'
                            )
                            ELSE jsonb_build_object(
                                'code', 'bbox_lease_expired',
                                'message', '图片分析执行超时，请稍后再试'
                            )
                        END
                    )
            WHERE (status = 'queued' AND queue_deadline_at_ms <= $1)
               OR (status = 'processing' AND lease_deadline_at_ms <= $1)
            "#,
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(updated.rows_affected())
    }

    async fn claim_sources_for_release(
        &self,
        limit: i64,
        release_terminal: bool,
    ) -> Result<Vec<MediaAsset>, ImageGatewayError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let candidates: Vec<Uuid> = sqlx::query_scalar(
            r#"
                SELECT asset.asset_id
                FROM media_segment_assets asset
                WHERE asset.source_state <> 'released'
                  AND (asset.source_state = 'releasing' OR asset.expires_at_ms <= $1
                       OR ($3 AND NOT asset.source_waiting_for_result AND EXISTS (SELECT 1 FROM media_segment_results result WHERE result.asset_id = asset.asset_id)))
                  AND NOT EXISTS (
                      SELECT 1 FROM media_segment_results result
                      WHERE result.asset_id = asset.asset_id AND result.status IN ('queued', 'processing')
                  )
                ORDER BY asset.source_release_attempt_at_ms, asset.expires_at_ms, asset.asset_id
                FOR UPDATE OF asset SKIP LOCKED
                LIMIT $2
            "#,
        ).bind(now_ms()).bind(limit.min(256)).bind(release_terminal)
            .fetch_all(&mut *tx).await.map_err(unavailable)?;
        // A separate statement takes a fresh READ COMMITTED snapshot after the
        // asset locks. An enqueue that committed just before those locks must
        // be visible; a one-statement CTE could miss that newly active result.
        let rows = sqlx::query_as::<_, AssetRow>(
            r#"
            UPDATE media_segment_assets asset
            SET source_state = 'releasing', source_release_attempt_at_ms = $2
            WHERE asset.asset_id = ANY($1)
              AND NOT EXISTS (SELECT 1 FROM media_segment_results result
                  WHERE result.asset_id = asset.asset_id AND result.status IN ('queued', 'processing'))
            RETURNING asset.asset_id, asset.tenant_id, asset.project_id,
                      asset.owner_id, asset.digest, asset.metadata,
                      asset.byte_size, asset.source_state, asset.expires_at_ms
            "#,
        ).bind(candidates).bind(now_ms())
            .fetch_all(&mut *tx).await.map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        rows.into_iter().map(asset_from_row).collect()
    }

    async fn finish_source_release(&self, asset: &MediaAsset) -> Result<bool, ImageGatewayError> {
        let updated = sqlx::query(
            r#"UPDATE media_segment_assets SET source_state = 'released'
               WHERE asset_id = $1 AND source_state = 'releasing'
                 AND metadata #>> '{blob,key,admission_session_id}' = $2"#,
        )
        .bind(asset.id)
        .bind(asset.blob.key.admission_session_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn expired_assets(&self, limit: i64) -> Result<Vec<MediaAsset>, ImageGatewayError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as::<_, AssetRow>(
            r#"
            SELECT asset.asset_id, asset.tenant_id, asset.project_id,
                   asset.owner_id, asset.digest, asset.metadata,
                   asset.byte_size, asset.source_state, asset.expires_at_ms
            FROM media_segment_assets asset
            WHERE asset.expires_at_ms <= $1
              AND asset.source_state = 'released'
              AND NOT EXISTS (
                  SELECT 1 FROM media_segment_results result
                  WHERE result.asset_id = asset.asset_id
                    AND result.status IN ('queued', 'processing')
              )
            ORDER BY asset.expires_at_ms, asset.asset_id
            LIMIT $2
            "#,
        )
        .bind(now_ms())
        .bind(limit.min(256))
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        rows.into_iter().map(asset_from_row).collect()
    }

    async fn delete_expired_asset(&self, id: Uuid) -> Result<(), ImageGatewayError> {
        sqlx::query(
            r#"
            DELETE FROM media_segment_assets asset
            WHERE asset.asset_id = $1 AND asset.expires_at_ms <= $2
              AND asset.source_state = 'released'
              AND NOT EXISTS (
                  SELECT 1 FROM media_segment_results result
                  WHERE result.asset_id = asset.asset_id
                    AND result.status IN ('queued', 'processing')
              )
            "#,
        )
        .bind(id)
        .bind(now_ms())
        .execute(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(())
    }
}

async fn asset_by_digest_any_age(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &MediaScope,
    digest: &str,
) -> Result<Option<AssetRow>, ImageGatewayError> {
    sqlx::query_as::<_, AssetRow>(
        r#"
        SELECT asset_id, tenant_id, project_id, owner_id, digest,
               metadata, byte_size, source_state, expires_at_ms
        FROM media_segment_assets
        WHERE tenant_id = $1 AND project_id = $2 AND owner_id = $3
          AND digest = $4
        FOR UPDATE
        "#,
    )
    .bind(&scope.tenant_id)
    .bind(&scope.project_id)
    .bind(&scope.owner_id)
    .bind(digest)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn result_by_cache(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    scope: &MediaScope,
    cache_key: &str,
) -> Result<Option<ResultRow>, ImageGatewayError> {
    sqlx::query_as::<_, ResultRow>(
        r#"
        SELECT result_json, status, queue_deadline_at_ms, lease_deadline_at_ms
        FROM media_segment_results
        WHERE tenant_id = $1 AND project_id = $2 AND owner_id = $3
          AND cache_key = $4
        "#,
    )
    .bind(&scope.tenant_id)
    .bind(&scope.project_id)
    .bind(&scope.owner_id)
    .bind(cache_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

fn asset_from_row(row: AssetRow) -> Result<MediaAsset, ImageGatewayError> {
    let metadata: AssetMetadata = serde_json::from_value(row.metadata)
        .map_err(|_| ImageGatewayError::service_unavailable("Media asset metadata is invalid"))?;
    let stored_size = u64::try_from(row.byte_size)
        .map_err(|_| ImageGatewayError::service_unavailable("Media asset size is invalid"))?;
    if metadata.blob.byte_size != stored_size {
        return Err(ImageGatewayError::service_unavailable(
            "Media asset metadata size does not match",
        ));
    }
    Ok(MediaAsset {
        id: row.asset_id,
        scope: MediaScope {
            tenant_id: row.tenant_id,
            project_id: row.project_id,
            owner_id: row.owner_id,
        },
        digest: row.digest,
        image: metadata.image,
        blob: metadata.blob,
        source_state: row.source_state,
        expires_at_ms: row.expires_at_ms,
    })
}

fn result_from_row(row: ResultRow, now: i64) -> Result<Segmentation, ImageGatewayError> {
    let mut result: Segmentation = serde_json::from_value(row.result_json)
        .map_err(|_| ImageGatewayError::service_unavailable("Segmentation result is invalid"))?;
    let error = match row.status.as_str() {
        "queued" if row.queue_deadline_at_ms <= now => Some(SegmentError {
            code: "bbox_queue_timeout".into(),
            message: "图片分析排队超时，请稍后再试".into(),
        }),
        "processing"
            if row
                .lease_deadline_at_ms
                .is_some_and(|deadline| deadline <= now) =>
        {
            Some(SegmentError {
                code: "bbox_lease_expired".into(),
                message: "图片分析执行超时，请稍后再试".into(),
            })
        }
        "queued" | "processing" | "completed" | "failed" => None,
        _ => {
            return Err(ImageGatewayError::service_unavailable(
                "Segmentation status is invalid",
            ));
        }
    };
    if let Some(error) = error {
        result.status = SegmentStatus::Failed;
        result.groups.clear();
        result.error = Some(error);
    }
    Ok(result)
}

fn work_from_row(row: WorkRow) -> Result<SegmentWork, ImageGatewayError> {
    let result = serde_json::from_value(row.result_json)
        .map_err(|_| ImageGatewayError::service_unavailable("Segmentation result is invalid"))?;
    let asset = asset_from_row(AssetRow {
        asset_id: row.asset_id,
        tenant_id: row.tenant_id,
        project_id: row.project_id,
        owner_id: row.owner_id,
        digest: row.digest,
        metadata: row.metadata,
        byte_size: row.byte_size,
        source_state: row.source_state,
        expires_at_ms: row.expires_at_ms,
    })?;
    Ok(SegmentWork {
        id: row.result_id,
        lease_token: row.lease_token,
        asset,
        result,
    })
}

fn unavailable(error: sqlx::Error) -> ImageGatewayError {
    tracing::warn!(error = %error, "media segmentation store unavailable");
    ImageGatewayError::service_unavailable("Media segmentation store unavailable")
}
