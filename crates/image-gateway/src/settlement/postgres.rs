use std::sync::Arc;

use async_trait::async_trait;
use image_provider_dreamina_cli::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaSubmitRequestV1, parse_submit_command,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    ExecutionSettlementStore, GenerationResultLookup, GenerationResultStatus, StoredVideoArtifact,
    VideoPendingStage, VideoProviderProgress, VideoRequestMetadata, VideoResultStatus,
    validate_generation_result,
};
use crate::{
    ImageGatewayError,
    admission::WorkLease,
    artifacts::{
        ArtifactBlobStore, ArtifactIdentity, ArtifactMetadata, ArtifactReadError,
        GenerationResultManifest, hydrate_generation_result,
    },
    pricing::inline_settlement::settle_inline_customer_quote,
    usage::{UsageReservation, UsageSnapshot},
};

mod additional_images;
mod failure;
mod image_status;
pub(super) mod image_success_reconciliation;
pub(super) mod image_supplementation;
pub(super) mod manual_video_cancellation;
mod results;
pub(super) mod supplement_video_cancellation;
pub(super) mod video_success_reconciliation;
pub(super) mod video_supplementation;

use results::{
    GenerationManifestLookup, generation_result_is_expired, load_generation_manifest,
    persist_generation_result, validate_completed_result,
};

#[derive(Clone)]
pub struct PostgresExecutionSettlementStore {
    pool: PgPool,
    artifact_store: Arc<dyn ArtifactBlobStore>,
}

impl PostgresExecutionSettlementStore {
    pub fn new(pool: PgPool, artifact_store: Arc<dyn ArtifactBlobStore>) -> Self {
        Self {
            pool,
            artifact_store,
        }
    }
}

#[async_trait]
impl ExecutionSettlementStore for PostgresExecutionSettlementStore {
    fn artifact_storage_identity(&self) -> String {
        self.artifact_store.storage_identity()
    }

    async fn succeed(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        result: &GenerationResultManifest,
    ) -> Result<UsageSnapshot, ImageGatewayError> {
        validate_generation_result(lease, reservation, result)?;
        let mut tx = self.pool.begin().await.map_err(settlement_unavailable)?;
        lock_tenant_quota(&mut tx, &reservation.charge.tenant_id).await?;
        let quota_job = lock_quota_and_job(&mut tx, reservation).await?;
        validate_reservation_handle(&quota_job, reservation)?;
        let work_attempt = lock_work_and_attempt(&mut tx, lease).await?;
        validate_lease_identity(&work_attempt, lease)?;
        let idempotency = lock_idempotency(&mut tx, lease.job_id).await?;
        let now = database_now(&mut tx).await?;

        if is_completed(&quota_job, &work_attempt) {
            validate_completed_state(&quota_job, &work_attempt, &idempotency)?;
            validate_completed_result(&mut tx, result).await?;
            settle_inline_customer_quote(
                &mut tx,
                lease.job_id,
                lease.execution_id,
                "succeeded",
                now,
            )
            .await?;
            tx.commit().await.map_err(settlement_unavailable)?;
            return Ok(reservation.snapshot.clone());
        }

        validate_active_state(&quota_job, &work_attempt, &idempotency, lease, now)?;
        persist_generation_result(&mut tx, result, now).await?;
        transition_work(&mut tx, lease, now).await?;
        transition_attempt(&mut tx, lease, now).await?;
        transition_idempotency(&mut tx, lease.job_id, now, idempotency.len()).await?;
        insert_usage_event(&mut tx, &quota_job, now).await?;
        commit_quota(&mut tx, &quota_job, now).await?;
        succeed_job(&mut tx, &quota_job, now).await?;
        insert_metering_events(&mut tx, &quota_job, now).await?;
        append_succeeded_events(&mut tx, lease, now).await?;
        settle_inline_customer_quote(&mut tx, lease.job_id, lease.execution_id, "succeeded", now)
            .await?;

        tx.commit().await.map_err(settlement_unavailable)?;
        Ok(reservation.snapshot.clone())
    }

    async fn fail(
        &self,
        lease: &WorkLease,
        reservation: &UsageReservation,
        error_code: &'static str,
    ) -> Result<(), ImageGatewayError> {
        failure::settle(&self.pool, lease, reservation, error_code).await
    }

    async fn load_generation_result(
        &self,
        job_id: Uuid,
    ) -> Result<GenerationResultLookup, ImageGatewayError> {
        let manifest = match load_generation_manifest(&self.pool, job_id).await? {
            GenerationManifestLookup::Available(manifest) => manifest,
            GenerationManifestLookup::Expired => return Ok(GenerationResultLookup::Expired),
            GenerationManifestLookup::Missing => return Ok(GenerationResultLookup::Missing),
        };
        match hydrate_generation_result(self.artifact_store.as_ref(), manifest).await {
            Ok(result) => Ok(GenerationResultLookup::Available(result)),
            Err(error) => {
                if generation_result_is_expired(&self.pool, job_id).await? {
                    Ok(GenerationResultLookup::Expired)
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn generation_status(
        &self,
        job_id: Uuid,
    ) -> Result<GenerationResultStatus, ImageGatewayError> {
        let state: Option<(String, String, Option<String>)> = sqlx::query_as(
            r#"
            SELECT j.state AS job_state, w.state AS work_state, j.last_error_code
            FROM jobs j
            JOIN work_items w ON w.job_id = j.job_id
            WHERE j.job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        match state {
            Some((job_state, _, _)) if job_state == "succeeded" => {
                match self.load_generation_result(job_id).await? {
                    GenerationResultLookup::Available(result) => {
                        Ok(GenerationResultStatus::Succeeded(result))
                    }
                    GenerationResultLookup::Expired => Ok(GenerationResultStatus::Expired),
                    GenerationResultLookup::Missing => Err(ImageGatewayError::artifact_integrity()),
                }
            }
            Some((job_state, work_state, error_code))
                if job_state == "failed" || work_state == "failed" =>
            {
                Ok(GenerationResultStatus::Failed { error_code })
            }
            Some((_, work_state, _)) if work_state == "uncertain" => {
                Ok(GenerationResultStatus::Uncertain)
            }
            Some(_) => Ok(GenerationResultStatus::Pending),
            None => Err(ImageGatewayError::internal("generation job not found")),
        }
    }

    async fn project_image_status(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Option<super::ImageTaskProjection>, ImageGatewayError> {
        image_status::project(self, tenant_id, project_id, actor_user_id, job_id).await
    }

    async fn validate_complete_image_result_budget(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<(), ImageGatewayError> {
        if image_supplementation::projection::validate_budget(
            self,
            tenant_id,
            project_id,
            actor_user_id,
            job_id,
        )
        .await?
        {
            return Ok(());
        }
        additional_images::validate_budget(self, tenant_id, project_id, actor_user_id, job_id).await
    }

    async fn project_additional_image_results(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Vec<super::AdditionalImageResult>, ImageGatewayError> {
        if let Some(images) = image_supplementation::projection::project(
            self,
            tenant_id,
            project_id,
            actor_user_id,
            job_id,
        )
        .await?
        {
            return Ok(images);
        }
        additional_images::project(self, tenant_id, project_id, actor_user_id, job_id).await
    }

    async fn project_image_job_by_idempotency_key(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        operation: &str,
        key_digest: &str,
    ) -> Result<Option<Uuid>, ImageGatewayError> {
        image_status::lookup(
            self,
            tenant_id,
            project_id,
            actor_user_id,
            image_api_contracts::dreamina::DREAMINA_IMAGES_API_PROFILE,
            operation,
            key_digest,
        )
        .await
    }

    async fn project_video_job_by_idempotency_key(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        key_digest: &str,
    ) -> Result<Option<super::VideoIdempotencyProjection>, ImageGatewayError> {
        let Some(job_id) = image_status::lookup(
            self,
            tenant_id,
            project_id,
            actor_user_id,
            image_api_contracts::dreamina::DREAMINA_VIDEOS_API_PROFILE,
            crate::admission::VIDEO_GENERATION_OPERATION,
            key_digest,
        )
        .await?
        else {
            return Ok(None);
        };
        image_status::video_lookup_projection(self, tenant_id, project_id, actor_user_id, job_id)
            .await
    }

    async fn video_status(
        &self,
        tenant_id: &str,
        job_id: Uuid,
    ) -> Result<Option<VideoResultStatus>, ImageGatewayError> {
        let row: Option<VideoStatusRow> = sqlx::query_as(
            r#"
            SELECT j.state, w.state, j.last_error_code, j.model, j.billable_units,
                   COALESCE(operator_supplement.artifact_id, artifact.artifact_id) AS artifact_id, COALESCE(operator_video.video_url, remote.video_url) AS video_url,
                   CASE WHEN operator_video.resolution_id IS NOT NULL THEN operator_video.expires_at_ms ELSE remote.expires_at_ms END AS expires_at_ms, payload.command_json,
                   progress_observation.progress_json, progress_observation.observed_at_ms,
                   CASE WHEN j.state = 'uncertain' AND w.state = 'uncertain'
                         AND submit_intent.submission_id IS NOT NULL
                         AND progress_submission.state = 'uncertain'
                         AND progress_task.submission_id IS NULL
                        THEN COALESCE(submit_intent.failure_error_code, j.last_error_code)
                   END AS submission_error_code
            FROM jobs j
            JOIN work_items w ON w.job_id = j.job_id
            LEFT JOIN job_payloads payload
              ON payload.job_id = j.job_id AND payload.command_schema = $3
             AND j.provider_id = 'dreamina-cli'
            LEFT JOIN job_outputs progress_output
              ON progress_output.job_id = j.job_id AND progress_output.output_index = 0
             AND j.provider_id = 'dreamina-cli'
            LEFT JOIN provider_submissions progress_submission
              ON progress_submission.output_id = progress_output.output_id
             AND progress_submission.job_id = j.job_id
             AND progress_submission.tenant_id = j.tenant_id
             AND progress_submission.work_item_id = w.work_item_id
            LEFT JOIN provider_remote_submit_intents submit_intent
              ON submit_intent.submission_id = progress_submission.submission_id
             AND submit_intent.executor_execution_id = progress_submission.executor_execution_id
             AND submit_intent.provider_id = progress_submission.provider_id
             AND submit_intent.provider_account_id = progress_submission.provider_account_id
             AND submit_intent.remote_operation_id IS NULL
             AND submit_intent.state IN ('outcome_unknown', 'deadline_quarantined')
            LEFT JOIN provider_remote_tasks progress_task
              ON progress_task.submission_id = progress_submission.submission_id
             AND progress_task.executor_execution_id = progress_submission.executor_execution_id
             AND progress_task.provider_id = j.provider_id
            LEFT JOIN LATERAL (
                SELECT observation.progress_json || jsonb_build_object(
                        'generation_started_at_ms', CASE
                            WHEN observation.progress_json ->> 'provider_status' = 'Generating'
                             AND jsonb_typeof(observation.progress_json -> 'estimated_remaining_seconds') = 'number'
                            THEN (
                            SELECT baseline.observed_at_ms
                            FROM provider_task_observations baseline
                            WHERE baseline.submission_id = observation.submission_id
                              AND baseline.executor_execution_id = observation.executor_execution_id
                              AND baseline.source = 'poll'
                              AND baseline.observed_state = 'provider_waiting'
                              AND baseline.progress_json IS NOT NULL
                              AND baseline.progress_json ->> 'provider_status' = 'Generating'
                              AND jsonb_typeof(baseline.progress_json -> 'estimated_remaining_seconds') = 'number'
                              AND baseline.observed_at_ms <= observation.observed_at_ms
                            ORDER BY baseline.observed_at_ms ASC,
                                     baseline.poll_lease_epoch ASC, baseline.observation_id ASC
                            LIMIT 1
                        ) END
                    ) AS progress_json, observation.observed_at_ms
                FROM provider_task_observations observation
                WHERE observation.submission_id = progress_task.submission_id
                  AND observation.executor_execution_id = progress_task.executor_execution_id
                  AND observation.source = 'poll'
                  AND observation.observed_state = 'provider_waiting'
                  AND observation.progress_json IS NOT NULL
                ORDER BY observation.observed_at_ms DESC,
                         observation.poll_lease_epoch DESC, observation.observation_id DESC
                LIMIT 1
            ) progress_observation ON TRUE
            LEFT JOIN artifacts artifact
              ON artifact.job_id = j.job_id
             AND artifact.output_index = 0
             AND artifact.state = 'ready'
             AND artifact.media_type = 'video/mp4'
            LEFT JOIN provider_submissions submission
              ON submission.job_id = j.job_id
             AND submission.tenant_id = j.tenant_id
             AND submission.work_item_id = w.work_item_id
             AND submission.state = 'succeeded'
             AND submission.remote_result_id IS NOT NULL
            LEFT JOIN executor_terminal_reductions reduction
              ON reduction.submission_id = submission.submission_id
             AND reduction.executor_execution_id = submission.executor_execution_id
             AND reduction.state = 'completed'
             AND reduction.resolved_state = 'succeeded'
            LEFT JOIN provider_remote_video_results remote
              ON remote.remote_result_id = reduction.remote_result_id
             AND remote.remote_result_id = submission.remote_result_id
             AND remote.submission_id = submission.submission_id
             AND remote.executor_execution_id = submission.executor_execution_id
             AND j.provider_id = 'dreamina-cli'
             AND EXISTS (
                 SELECT 1 FROM job_response_projections projection
                 WHERE projection.job_id = j.job_id
                   AND projection.artifact_count = 0
                   AND projection.remote_result_count = 1
             )
            LEFT JOIN dreamina_operator_video_supplement_resolutions operator_supplement
              ON operator_supplement.job_id = j.job_id
             AND operator_supplement.output_id = progress_output.output_id
             AND j.provider_id = 'dreamina-cli' AND j.state = 'succeeded'
            LEFT JOIN dreamina_operator_video_success_resolutions operator_video
              ON operator_video.job_id = j.job_id
             AND operator_video.evidence_mode = 'legacy_poll_v1'
             AND operator_video.submission_id = progress_submission.submission_id
             AND operator_video.executor_execution_id = progress_submission.executor_execution_id
             AND operator_video.output_id = progress_output.output_id
             AND j.provider_id = 'dreamina-cli' AND j.state = 'succeeded'
            WHERE j.job_id = $1 AND j.tenant_id = $2
              AND j.operation = 'video_generation'
              AND j.economics_contract_version IN (3, 4)
            "#,
        )
        .bind(job_id)
        .bind(tenant_id)
        .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        let Some(row) = row else { return Ok(None) };
        let status = video_result_status(row)?;
        Ok(Some(
            video_supplementation::progress::project(self, tenant_id, None, None, job_id, status)
                .await?,
        ))
    }

    async fn project_dreamina_manual_video_cancellation(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Option<super::DreaminaManualVideoCancellationProof>, ImageGatewayError> {
        PostgresExecutionSettlementStore::project_dreamina_manual_video_cancellation(
            self,
            tenant_id,
            project_id,
            actor_user_id,
            job_id,
        )
        .await
    }

    async fn project_video_status(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Option<VideoResultStatus>, ImageGatewayError> {
        let row: Option<VideoStatusRow> = sqlx::query_as(
            r#"
                SELECT j.state, w.state, j.last_error_code, j.model, j.billable_units,
                       COALESCE(operator_supplement.artifact_id, artifact.artifact_id) AS artifact_id, COALESCE(operator_video.video_url, remote.video_url) AS video_url,
                   CASE WHEN operator_video.resolution_id IS NOT NULL THEN operator_video.expires_at_ms ELSE remote.expires_at_ms END AS expires_at_ms, payload.command_json,
                       progress_observation.progress_json, progress_observation.observed_at_ms,
                   CASE WHEN j.state = 'uncertain' AND w.state = 'uncertain'
                         AND submit_intent.submission_id IS NOT NULL
                         AND progress_submission.state = 'uncertain'
                         AND progress_task.submission_id IS NULL
                        THEN COALESCE(submit_intent.failure_error_code, j.last_error_code)
                   END AS submission_error_code
                FROM jobs j
                JOIN work_items w ON w.job_id = j.job_id
                JOIN job_auth_attributions attribution
                  ON attribution.job_id = j.job_id
                LEFT JOIN job_payloads payload
                  ON payload.job_id = j.job_id AND payload.command_schema = $5
                 AND j.provider_id = 'dreamina-cli'
                LEFT JOIN job_outputs progress_output
                  ON progress_output.job_id = j.job_id AND progress_output.output_index = 0
                 AND j.provider_id = 'dreamina-cli'
                LEFT JOIN provider_submissions progress_submission
                  ON progress_submission.output_id = progress_output.output_id
                 AND progress_submission.job_id = j.job_id
                 AND progress_submission.tenant_id = j.tenant_id
                 AND progress_submission.work_item_id = w.work_item_id
                LEFT JOIN provider_remote_submit_intents submit_intent
              ON submit_intent.submission_id = progress_submission.submission_id
             AND submit_intent.executor_execution_id = progress_submission.executor_execution_id
             AND submit_intent.provider_id = progress_submission.provider_id
             AND submit_intent.provider_account_id = progress_submission.provider_account_id
             AND submit_intent.remote_operation_id IS NULL
             AND submit_intent.state IN ('outcome_unknown', 'deadline_quarantined')
                LEFT JOIN provider_remote_tasks progress_task
                  ON progress_task.submission_id = progress_submission.submission_id
                 AND progress_task.executor_execution_id = progress_submission.executor_execution_id
                 AND progress_task.provider_id = j.provider_id
                LEFT JOIN LATERAL (
                    SELECT observation.progress_json || jsonb_build_object(
                        'generation_started_at_ms', CASE
                            WHEN observation.progress_json ->> 'provider_status' = 'Generating'
                             AND jsonb_typeof(observation.progress_json -> 'estimated_remaining_seconds') = 'number'
                            THEN (
                            SELECT baseline.observed_at_ms
                            FROM provider_task_observations baseline
                            WHERE baseline.submission_id = observation.submission_id
                              AND baseline.executor_execution_id = observation.executor_execution_id
                              AND baseline.source = 'poll'
                              AND baseline.observed_state = 'provider_waiting'
                              AND baseline.progress_json IS NOT NULL
                              AND baseline.progress_json ->> 'provider_status' = 'Generating'
                              AND jsonb_typeof(baseline.progress_json -> 'estimated_remaining_seconds') = 'number'
                              AND baseline.observed_at_ms <= observation.observed_at_ms
                            ORDER BY baseline.observed_at_ms ASC,
                                     baseline.poll_lease_epoch ASC, baseline.observation_id ASC
                            LIMIT 1
                        ) END
                    ) AS progress_json, observation.observed_at_ms
                    FROM provider_task_observations observation
                    WHERE observation.submission_id = progress_task.submission_id
                      AND observation.executor_execution_id = progress_task.executor_execution_id
                      AND observation.source = 'poll'
                      AND observation.observed_state = 'provider_waiting'
                      AND observation.progress_json IS NOT NULL
                    ORDER BY observation.observed_at_ms DESC,
                             observation.poll_lease_epoch DESC, observation.observation_id DESC
                    LIMIT 1
                ) progress_observation ON TRUE
                LEFT JOIN artifacts artifact
                  ON artifact.job_id = j.job_id
                 AND artifact.output_index = 0
                 AND artifact.state = 'ready'
                 AND artifact.media_type = 'video/mp4'
                LEFT JOIN provider_submissions submission
                  ON submission.job_id = j.job_id
                 AND submission.tenant_id = j.tenant_id
                 AND submission.work_item_id = w.work_item_id
                 AND submission.state = 'succeeded'
                 AND submission.remote_result_id IS NOT NULL
                LEFT JOIN executor_terminal_reductions reduction
                  ON reduction.submission_id = submission.submission_id
                 AND reduction.executor_execution_id = submission.executor_execution_id
                 AND reduction.state = 'completed'
                 AND reduction.resolved_state = 'succeeded'
                LEFT JOIN provider_remote_video_results remote
                  ON remote.remote_result_id = reduction.remote_result_id
                 AND remote.remote_result_id = submission.remote_result_id
                 AND remote.submission_id = submission.submission_id
                 AND remote.executor_execution_id = submission.executor_execution_id
                 AND j.provider_id = 'dreamina-cli'
                 AND EXISTS (
                     SELECT 1 FROM job_response_projections projection
                     WHERE projection.job_id = j.job_id
                       AND projection.artifact_count = 0
                       AND projection.remote_result_count = 1
                 )
                LEFT JOIN dreamina_operator_video_supplement_resolutions operator_supplement
                  ON operator_supplement.job_id = j.job_id
                 AND operator_supplement.output_id = progress_output.output_id
                 AND j.provider_id = 'dreamina-cli' AND j.state = 'succeeded'
                LEFT JOIN dreamina_operator_video_success_resolutions operator_video
                  ON operator_video.job_id = j.job_id
                 AND operator_video.evidence_mode = 'legacy_poll_v1'
                 AND operator_video.submission_id = progress_submission.submission_id
                 AND operator_video.executor_execution_id = progress_submission.executor_execution_id
                 AND operator_video.output_id = progress_output.output_id
                 AND j.provider_id = 'dreamina-cli' AND j.state = 'succeeded'
                WHERE j.job_id = $1 AND j.tenant_id = $2
                  AND attribution.project_id = $3
                  AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
                  AND j.operation = 'video_generation'
                  AND j.economics_contract_version IN (3, 4)
                "#,
        )
        .bind(job_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(actor_user_id)
        .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        let Some(row) = row else { return Ok(None) };
        let status = video_result_status(row)?;
        Ok(Some(
            video_supplementation::progress::project(
                self,
                tenant_id,
                Some(project_id),
                actor_user_id,
                job_id,
                status,
            )
            .await?,
        ))
    }

    async fn project_video_released_settlement(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Option<image_api_contracts::dreamina::DreaminaReleasedSettlement>, ImageGatewayError>
    {
        let row: Option<(Uuid, Uuid, String, Uuid, String, String, String, String)> =
            sqlx::query_as(
                r#"
            SELECT resolution.resolution_id, job.job_id, job.request_id,
                   hold.hold_id, hold.currency, hold.held_micros::TEXT,
                   hold.released_micros::TEXT, hold.captured_micros::TEXT
            FROM dreamina_operator_rejection_resolutions resolution
            JOIN jobs job ON job.job_id = resolution.job_id
            JOIN job_auth_attributions attribution
              ON attribution.job_id = job.job_id AND attribution.tenant_id = job.tenant_id
            JOIN customer_billing_holds hold
              ON hold.hold_id = resolution.hold_id AND hold.job_id = job.job_id
             AND hold.tenant_id = job.tenant_id
            WHERE job.job_id = $1 AND job.tenant_id = $2
              AND attribution.project_id = $3
              AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
              AND job.provider_id = 'dreamina-cli' AND job.operation = 'video_generation'
              AND job.economics_contract_version = 4 AND job.output_count = 1
              AND job.charged_units = 0
              AND job.state = 'failed'
              AND job.last_error_code = 'dreamina_compliance_confirmation_required'
              AND resolution.reason = job.last_error_code
              AND hold.state = 'released' AND hold.captured_micros = 0
              AND hold.released_micros = hold.held_micros
              AND resolution.released_micros = hold.released_micros
            "#,
            )
            .bind(job_id)
            .bind(tenant_id)
            .bind(project_id)
            .bind(actor_user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(settlement_unavailable)?;
        Ok(row.map(
            |(
                resolution_audit_id,
                job_id,
                request_id,
                hold_id,
                currency,
                held_micros,
                released_micros,
                captured_micros,
            )| image_api_contracts::dreamina::DreaminaReleasedSettlement {
                state: "released".to_owned(),
                resolution_audit_id: resolution_audit_id.to_string(),
                job_id: job_id.to_string(),
                request_id,
                hold_id: hold_id.to_string(),
                currency,
                held_micros,
                released_micros,
                captured_micros,
            },
        ))
    }

    async fn project_video_status_with_metadata(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        job_id: Uuid,
    ) -> Result<Option<(VideoResultStatus, VideoRequestMetadata)>, ImageGatewayError> {
        let row: Option<VideoStatusRow> = sqlx::query_as(
            r#"
                SELECT j.state, w.state, j.last_error_code, j.model, j.billable_units,
                       COALESCE(operator_supplement.artifact_id, artifact.artifact_id) AS artifact_id, COALESCE(operator_video.video_url, remote.video_url) AS video_url,
                   CASE WHEN operator_video.resolution_id IS NOT NULL THEN operator_video.expires_at_ms ELSE remote.expires_at_ms END AS expires_at_ms, payload.command_json,
                       progress_observation.progress_json, progress_observation.observed_at_ms,
                   CASE WHEN j.state = 'uncertain' AND w.state = 'uncertain'
                         AND submit_intent.submission_id IS NOT NULL
                         AND progress_submission.state = 'uncertain'
                         AND progress_task.submission_id IS NULL
                        THEN COALESCE(submit_intent.failure_error_code, j.last_error_code)
                   END AS submission_error_code
                FROM jobs j
                JOIN work_items w ON w.job_id = j.job_id
                JOIN job_auth_attributions attribution
                  ON attribution.job_id = j.job_id
                LEFT JOIN job_payloads payload
                  ON payload.job_id = j.job_id
                 AND payload.command_schema = $5
                 AND j.provider_id = 'dreamina-cli'
                LEFT JOIN job_outputs progress_output
                  ON progress_output.job_id = j.job_id AND progress_output.output_index = 0
                 AND j.provider_id = 'dreamina-cli'
                LEFT JOIN provider_submissions progress_submission
                  ON progress_submission.output_id = progress_output.output_id
                 AND progress_submission.job_id = j.job_id
                 AND progress_submission.tenant_id = j.tenant_id
                 AND progress_submission.work_item_id = w.work_item_id
                LEFT JOIN provider_remote_submit_intents submit_intent
              ON submit_intent.submission_id = progress_submission.submission_id
             AND submit_intent.executor_execution_id = progress_submission.executor_execution_id
             AND submit_intent.provider_id = progress_submission.provider_id
             AND submit_intent.provider_account_id = progress_submission.provider_account_id
             AND submit_intent.remote_operation_id IS NULL
             AND submit_intent.state IN ('outcome_unknown', 'deadline_quarantined')
                LEFT JOIN provider_remote_tasks progress_task
                  ON progress_task.submission_id = progress_submission.submission_id
                 AND progress_task.executor_execution_id = progress_submission.executor_execution_id
                 AND progress_task.provider_id = j.provider_id
                LEFT JOIN LATERAL (
                    SELECT observation.progress_json || jsonb_build_object(
                        'generation_started_at_ms', CASE
                            WHEN observation.progress_json ->> 'provider_status' = 'Generating'
                             AND jsonb_typeof(observation.progress_json -> 'estimated_remaining_seconds') = 'number'
                            THEN (
                            SELECT baseline.observed_at_ms
                            FROM provider_task_observations baseline
                            WHERE baseline.submission_id = observation.submission_id
                              AND baseline.executor_execution_id = observation.executor_execution_id
                              AND baseline.source = 'poll'
                              AND baseline.observed_state = 'provider_waiting'
                              AND baseline.progress_json IS NOT NULL
                              AND baseline.progress_json ->> 'provider_status' = 'Generating'
                              AND jsonb_typeof(baseline.progress_json -> 'estimated_remaining_seconds') = 'number'
                              AND baseline.observed_at_ms <= observation.observed_at_ms
                            ORDER BY baseline.observed_at_ms ASC,
                                     baseline.poll_lease_epoch ASC, baseline.observation_id ASC
                            LIMIT 1
                        ) END
                    ) AS progress_json, observation.observed_at_ms
                    FROM provider_task_observations observation
                    WHERE observation.submission_id = progress_task.submission_id
                      AND observation.executor_execution_id = progress_task.executor_execution_id
                      AND observation.source = 'poll'
                      AND observation.observed_state = 'provider_waiting'
                      AND observation.progress_json IS NOT NULL
                    ORDER BY observation.observed_at_ms DESC,
                             observation.poll_lease_epoch DESC, observation.observation_id DESC
                    LIMIT 1
                ) progress_observation ON TRUE
                LEFT JOIN artifacts artifact
                  ON artifact.job_id = j.job_id
                 AND artifact.output_index = 0
                 AND artifact.state = 'ready'
                 AND artifact.media_type = 'video/mp4'
                LEFT JOIN provider_submissions submission
                  ON submission.job_id = j.job_id
                 AND submission.tenant_id = j.tenant_id
                 AND submission.work_item_id = w.work_item_id
                 AND submission.state = 'succeeded'
                 AND submission.remote_result_id IS NOT NULL
                LEFT JOIN executor_terminal_reductions reduction
                  ON reduction.submission_id = submission.submission_id
                 AND reduction.executor_execution_id = submission.executor_execution_id
                 AND reduction.state = 'completed'
                 AND reduction.resolved_state = 'succeeded'
                LEFT JOIN provider_remote_video_results remote
                  ON remote.remote_result_id = reduction.remote_result_id
                 AND remote.remote_result_id = submission.remote_result_id
                 AND remote.submission_id = submission.submission_id
                 AND remote.executor_execution_id = submission.executor_execution_id
                 AND j.provider_id = 'dreamina-cli'
                 AND EXISTS (
                     SELECT 1 FROM job_response_projections projection
                     WHERE projection.job_id = j.job_id
                       AND projection.artifact_count = 0
                       AND projection.remote_result_count = 1
                 )
                LEFT JOIN dreamina_operator_video_supplement_resolutions operator_supplement
                  ON operator_supplement.job_id = j.job_id
                 AND operator_supplement.output_id = progress_output.output_id
                 AND j.provider_id = 'dreamina-cli' AND j.state = 'succeeded'
                LEFT JOIN dreamina_operator_video_success_resolutions operator_video
                  ON operator_video.job_id = j.job_id
                 AND operator_video.evidence_mode = 'legacy_poll_v1'
                 AND operator_video.submission_id = progress_submission.submission_id
                 AND operator_video.executor_execution_id = progress_submission.executor_execution_id
                 AND operator_video.output_id = progress_output.output_id
                 AND j.provider_id = 'dreamina-cli' AND j.state = 'succeeded'
                WHERE j.job_id = $1 AND j.tenant_id = $2
                  AND attribution.project_id = $3
                  AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
                  AND j.operation = 'video_generation'
                  AND j.economics_contract_version IN (3, 4)
                "#,
        )
        .bind(job_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(actor_user_id)
        .bind(DREAMINA_SUBMIT_COMMAND_SCHEMA)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        let Some(row) = row else { return Ok(None) };
        let metadata = video_request_metadata(row.8.as_ref())?;
        let status = video_supplementation::progress::project(
            self,
            tenant_id,
            Some(project_id),
            actor_user_id,
            job_id,
            video_result_status(row)?,
        )
        .await?;
        Ok(Some((status, metadata)))
    }

    async fn load_video_artifact(
        &self,
        tenant_id: &str,
        artifact_id: Uuid,
    ) -> Result<Option<StoredVideoArtifact>, ImageGatewayError> {
        if let Some(video) =
            video_supplementation::load_artifact(self, tenant_id, None, None, artifact_id).await?
        {
            return Ok(Some(video));
        }
        let row: Option<(
            Uuid,
            String,
            Uuid,
            Uuid,
            Uuid,
            i64,
            i32,
            String,
            String,
            String,
            String,
            i64,
            bool,
        )> = sqlx::query_as(
            r#"
                SELECT artifact.artifact_id, artifact.tenant_id, artifact.job_id,
                       artifact.work_item_id, artifact.execution_id, artifact.lease_epoch,
                       artifact.output_index, artifact.media_type, artifact.storage_backend,
                       artifact.object_key, artifact.sha256_hex, artifact.byte_size,
                       retention.state = 'available'
                         AND retention.expires_at_ms >
                           (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::BIGINT
                         AS retention_available
                FROM artifacts artifact
                JOIN jobs job ON job.job_id = artifact.job_id
                JOIN job_artifact_retention retention ON retention.job_id = artifact.job_id
                WHERE artifact.artifact_id = $1 AND artifact.tenant_id = $2
                  AND artifact.state = 'ready'
                  AND job.operation = 'video_generation'
                  AND job.economics_contract_version IN (3, 4)
                "#,
        )
        .bind(artifact_id)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        let Some((
            artifact_id,
            tenant_id,
            job_id,
            work_item_id,
            execution_id,
            lease_epoch,
            output_index,
            media_type,
            storage_backend,
            object_key,
            sha256_hex,
            byte_size,
            retention_available,
        )) = row
        else {
            return Ok(None);
        };
        if !retention_available {
            return Err(ImageGatewayError::artifact_expired());
        }
        if media_type != "video/mp4" {
            return Err(ImageGatewayError::artifact_integrity());
        }
        let metadata = ArtifactMetadata {
            identity: ArtifactIdentity {
                artifact_id,
                tenant_id,
                job_id,
                work_item_id,
                execution_id,
                lease_epoch,
                output_index: u32::try_from(output_index)
                    .map_err(|_| ImageGatewayError::artifact_integrity())?,
                media_type: media_type.clone(),
            },
            storage_backend,
            object_key,
            sha256_hex,
            byte_size: u64::try_from(byte_size)
                .map_err(|_| ImageGatewayError::artifact_integrity())?,
        };
        let bytes = match self.artifact_store.get(&metadata).await {
            Ok(bytes) => bytes,
            Err(error) => {
                if generation_result_is_expired(&self.pool, job_id).await? {
                    return Err(ImageGatewayError::artifact_expired());
                }
                return Err(match error {
                    ArtifactReadError::Integrity => ImageGatewayError::artifact_integrity(),
                    ArtifactReadError::Unavailable => {
                        ImageGatewayError::service_unavailable("artifact storage unavailable")
                    }
                });
            }
        };
        Ok(Some(StoredVideoArtifact { media_type, bytes }))
    }

    async fn load_project_video_artifact(
        &self,
        tenant_id: &str,
        project_id: &str,
        actor_user_id: Option<Uuid>,
        artifact_id: Uuid,
    ) -> Result<Option<StoredVideoArtifact>, ImageGatewayError> {
        if let Some(video) = video_supplementation::load_artifact(
            self,
            tenant_id,
            Some(project_id),
            actor_user_id,
            artifact_id,
        )
        .await?
        {
            return Ok(Some(video));
        }
        let belongs_to_project: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM artifacts artifact
                JOIN jobs job ON job.job_id = artifact.job_id
                JOIN job_auth_attributions attribution
                  ON attribution.job_id = job.job_id
                WHERE artifact.artifact_id = $1
                  AND artifact.tenant_id = $2
                  AND attribution.project_id = $3
                  AND ($4::UUID IS NULL OR attribution.actor_user_id = $4)
                  AND artifact.state = 'ready'
                  AND job.operation = 'video_generation'
                  AND job.economics_contract_version IN (3, 4)
            )
            "#,
        )
        .bind(artifact_id)
        .bind(tenant_id)
        .bind(project_id)
        .bind(actor_user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(settlement_unavailable)?;
        if !belongs_to_project {
            return Ok(None);
        }
        self.load_video_artifact(tenant_id, artifact_id).await
    }
}

type VideoStatusRow = (
    String,
    String,
    Option<String>,
    String,
    i32,
    Option<Uuid>,
    Option<String>,
    Option<i64>,
    Option<serde_json::Value>,
    Option<serde_json::Value>,
    Option<i64>,
    Option<String>,
);

#[derive(serde::Deserialize)]
struct StoredProviderProgress {
    generation_started_at_ms: Option<i64>,
    provider_status: String,
    queue_position: Option<u64>,
    queue_total: Option<u64>,
    estimated_remaining_seconds: Option<u64>,
}

fn video_provider_progress(
    value: Option<serde_json::Value>,
    observed_at_ms: Option<i64>,
) -> Option<VideoProviderProgress> {
    let observed_at_ms = observed_at_ms.filter(|value| *value >= 0)?;
    let stored: StoredProviderProgress = serde_json::from_value(value?).ok()?;
    let progress = image_provider_sdk::ProviderTaskProgress {
        provider_status: stored.provider_status,
        queue_position: stored.queue_position,
        queue_total: stored.queue_total,
        estimated_remaining_seconds: stored.estimated_remaining_seconds,
    };
    progress.validate().ok()?;
    let estimated_percent =
        estimated_generation_percent(&progress, stored.generation_started_at_ms, observed_at_ms);
    Some(VideoProviderProgress {
        estimated_percent,
        progress,
        observed_at_ms,
    })
}

// This is an estimate since our first observed Generating report, not a provider
// percentage or a wall-clock animation. Only a new durable report changes it.
fn estimated_generation_percent(
    progress: &image_provider_sdk::ProviderTaskProgress,
    generation_started_at_ms: Option<i64>,
    observed_at_ms: i64,
) -> Option<u8> {
    if progress.provider_status != "Generating" {
        return None;
    }
    let start = generation_started_at_ms.filter(|start| *start >= 0)?;
    let elapsed_ms = observed_at_ms
        .checked_sub(start)
        .filter(|elapsed| *elapsed >= 0)? as u128;
    let remaining_ms = u128::from(progress.estimated_remaining_seconds?) * 1_000;
    // A zero ETA is not completion evidence; only artifact publication completes a task.
    let denominator = elapsed_ms + remaining_ms;
    if denominator == 0 {
        return None;
    }
    Some(((elapsed_ms * 100 / denominator).min(99)) as u8)
}

fn video_request_metadata(
    command: Option<&serde_json::Value>,
) -> Result<VideoRequestMetadata, ImageGatewayError> {
    let Some(command) = command else {
        return Ok(VideoRequestMetadata::default());
    };
    let bytes = serde_json::to_vec(command).map_err(|_| ImageGatewayError::artifact_integrity())?;
    let request =
        parse_submit_command(&bytes).map_err(|_| ImageGatewayError::artifact_integrity())?;
    let (model, resolution, ratio) = match request {
        DreaminaSubmitRequestV1::TextToVideo(request) => (
            request.model().as_str(),
            request.resolution().as_str(),
            Some(request.ratio().as_str()),
        ),
        DreaminaSubmitRequestV1::ImageToVideo(request) => (
            request.options().model().as_str(),
            request.options().resolution().as_str(),
            request.options().ratio().map(|ratio| ratio.as_str()),
        ),
        DreaminaSubmitRequestV1::FramesToVideo(request) => (
            request.options().model().as_str(),
            request.options().resolution().as_str(),
            request.options().ratio().map(|ratio| ratio.as_str()),
        ),
        DreaminaSubmitRequestV1::MultiframeToVideo(request) => {
            ("multiframe2video", request.resolution().as_str(), None)
        }
        DreaminaSubmitRequestV1::MultimodalToVideo(request) => (
            request.model().as_str(),
            request.resolution().as_str(),
            Some(request.ratio().as_str()),
        ),
        _ => return Err(ImageGatewayError::artifact_integrity()),
    };
    Ok(VideoRequestMetadata {
        provider_model: Some(model.to_owned()),
        resolution: Some(resolution.to_owned()),
        ratio: ratio.map(str::to_owned),
    })
}

fn video_result_status(row: VideoStatusRow) -> Result<VideoResultStatus, ImageGatewayError> {
    let (
        job_state,
        work_state,
        error_code,
        model,
        billable_units,
        artifact_id,
        remote_url,
        remote_expires_at_ms,
        command,
        progress,
        progress_observed_at_ms,
        submission_error_code,
    ) = row;
    let duration_ms = requested_video_duration_ms(billable_units, command.as_ref())?;
    if job_state == "succeeded" {
        match (artifact_id, remote_url) {
            (Some(artifact_id), None) if remote_expires_at_ms.is_none() => {
                Ok(VideoResultStatus::Succeeded {
                    model,
                    duration_ms,
                    artifact_id,
                })
            }
            (None, Some(url)) => {
                let result = image_provider_sdk::RemoteVideoResult::new(url, remote_expires_at_ms)
                    .map_err(|_| ImageGatewayError::artifact_integrity())?;
                Ok(VideoResultStatus::SucceededRemote {
                    model,
                    duration_ms,
                    result,
                })
            }
            _ => Err(ImageGatewayError::artifact_integrity()),
        }
    } else if job_state == "failed" || work_state == "failed" {
        Ok(VideoResultStatus::Failed {
            model,
            duration_ms,
            error_code,
        })
    } else if work_state == "uncertain" {
        Ok(VideoResultStatus::Uncertain {
            model,
            duration_ms,
            provider_progress: video_provider_progress(progress, progress_observed_at_ms),
            submission_error_code,
        })
    } else {
        let stage = match work_state.as_str() {
            "ready" => VideoPendingStage::Queued,
            "leased" | "running" => VideoPendingStage::Dispatching,
            _ => VideoPendingStage::Processing,
        };
        Ok(VideoResultStatus::Pending {
            model,
            duration_ms,
            stage,
            provider_progress: video_provider_progress(progress, progress_observed_at_ms),
        })
    }
}

fn requested_video_duration_ms(
    quota_seconds: i32,
    command: Option<&serde_json::Value>,
) -> Result<u32, ImageGatewayError> {
    let quota_seconds = u32::try_from(quota_seconds)
        .ok()
        .filter(|seconds| *seconds > 0)
        .ok_or_else(ImageGatewayError::artifact_integrity)?;
    let Some(command) = command else {
        return quota_seconds
            .checked_mul(1_000)
            .ok_or_else(ImageGatewayError::artifact_integrity);
    };
    let bytes = serde_json::to_vec(command).map_err(|_| ImageGatewayError::artifact_integrity())?;
    let request =
        parse_submit_command(&bytes).map_err(|_| ImageGatewayError::artifact_integrity())?;
    let duration_ms = match request {
        DreaminaSubmitRequestV1::TextToVideo(request) => {
            u32::from(request.duration_seconds()) * 1_000
        }
        DreaminaSubmitRequestV1::ImageToVideo(request) => {
            u32::from(request.options().duration_seconds()) * 1_000
        }
        DreaminaSubmitRequestV1::FramesToVideo(request) => {
            u32::from(request.options().duration_seconds()) * 1_000
        }
        DreaminaSubmitRequestV1::MultimodalToVideo(request) => {
            u32::from(request.duration_seconds()) * 1_000
        }
        DreaminaSubmitRequestV1::MultiframeToVideo(request) => request
            .transitions()
            .iter()
            .map(|transition| transition.duration_ms())
            .sum(),
        _ => return Err(ImageGatewayError::artifact_integrity()),
    };
    if !(1..=152_000).contains(&duration_ms) || duration_ms.div_ceil(1_000) != quota_seconds {
        return Err(ImageGatewayError::artifact_integrity());
    }
    Ok(duration_ms)
}

#[derive(sqlx::FromRow)]
struct LockedQuotaJob {
    reservation_id: Uuid,
    quota_tenant_id: String,
    quota_request_id: String,
    quota_job_id: Uuid,
    job_job_id: Uuid,
    requested_units: i32,
    committed_units: i32,
    released_units: i32,
    quota_state: String,
    expires_at_ms: i64,
    job_tenant_id: String,
    job_request_id: String,
    operation: String,
    provider_id: String,
    model: String,
    job_state: String,
    job_requested_units: i32,
    charged_units: i32,
    job_reservation_id: Uuid,
    last_error_code: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LockedWorkAttempt {
    work_item_id: Uuid,
    job_id: Uuid,
    work_state: String,
    work_lease_epoch: i64,
    lease_owner: Option<String>,
    lease_expires_at_ms: Option<i64>,
    work_execution_id: Option<Uuid>,
    attempt_execution_id: Uuid,
    attempt_work_item_id: Uuid,
    attempt_lease_epoch: i64,
    worker_id: String,
    attempt_state: String,
    attempt_error_code: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LockedIdempotency {
    state: String,
    terminal_outcome: Option<String>,
}

async fn lock_tenant_quota(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(tenant_id))
        .execute(&mut **tx)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("quota lock unavailable"))?;
    Ok(())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ImageGatewayError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(settlement_unavailable)
}

async fn lock_quota_and_job(
    tx: &mut Transaction<'_, Postgres>,
    reservation: &UsageReservation,
) -> Result<LockedQuotaJob, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT
          qr.reservation_id,
          qr.tenant_id AS quota_tenant_id,
          qr.request_id AS quota_request_id,
          qr.job_id AS quota_job_id,
          j.job_id AS job_job_id,
          qr.requested_units,
          qr.committed_units,
          qr.released_units,
          qr.state AS quota_state,
          qr.expires_at_ms,
          j.tenant_id AS job_tenant_id,
          j.request_id AS job_request_id,
          j.operation,
          j.provider_id,
          j.model,
          j.state AS job_state,
          j.requested_units AS job_requested_units,
          j.charged_units,
          j.reservation_id AS job_reservation_id,
          j.last_error_code
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.reservation_id = $1
        FOR UPDATE OF qr, j
        "#,
    )
    .bind(reservation.reservation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(settlement_unavailable)?
    .ok_or_else(|| ImageGatewayError::internal("reservation not found"))
}

fn validate_reservation_handle(
    locked: &LockedQuotaJob,
    reservation: &UsageReservation,
) -> Result<(), ImageGatewayError> {
    let units_match = u32::try_from(locked.requested_units)
        .is_ok_and(|units| units == reservation.charge.billable_units);
    if locked.reservation_id != reservation.reservation_id
        || locked.quota_tenant_id != reservation.charge.tenant_id
        || locked.job_tenant_id != reservation.charge.tenant_id
        || locked.quota_job_id != reservation.job_id
        || locked.job_job_id != reservation.job_id
        || locked.quota_request_id != reservation.charge.request_id
        || locked.job_request_id != reservation.charge.request_id
        || locked.operation != reservation.charge.operation
        || locked.provider_id != reservation.charge.provider_id
        || locked.model != reservation.charge.model
        || locked.job_reservation_id != reservation.reservation_id
        || locked.job_requested_units != locked.requested_units
        || !units_match
    {
        return Err(ImageGatewayError::internal(
            "reservation handle does not match stored settlement state",
        ));
    }
    Ok(())
}

async fn lock_work_and_attempt(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
) -> Result<LockedWorkAttempt, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT
          w.work_item_id,
          w.job_id,
          w.state AS work_state,
          w.lease_epoch AS work_lease_epoch,
          w.lease_owner,
          w.lease_expires_at_ms,
          w.execution_id AS work_execution_id,
          a.execution_id AS attempt_execution_id,
          a.work_item_id AS attempt_work_item_id,
          a.lease_epoch AS attempt_lease_epoch,
          a.worker_id,
          a.state AS attempt_state,
          a.error_code AS attempt_error_code
        FROM work_items w
        JOIN job_attempts a
          ON a.work_item_id = w.work_item_id
         AND a.execution_id = $2
        WHERE w.work_item_id = $1
        FOR UPDATE OF w, a
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.execution_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(settlement_unavailable)?
    .ok_or_else(|| ImageGatewayError::internal("work lease is stale or invalid"))
}

fn validate_lease_identity(
    locked: &LockedWorkAttempt,
    lease: &WorkLease,
) -> Result<(), ImageGatewayError> {
    if locked.work_item_id != lease.work_item_id
        || locked.job_id != lease.job_id
        || locked.work_lease_epoch != lease.lease_epoch
        || locked.work_execution_id != Some(lease.execution_id)
        || locked.attempt_execution_id != lease.execution_id
        || locked.attempt_work_item_id != lease.work_item_id
        || locked.attempt_lease_epoch != lease.lease_epoch
        || locked.worker_id != lease.worker_id
    {
        return Err(ImageGatewayError::internal(
            "work lease is stale or invalid",
        ));
    }
    Ok(())
}

async fn lock_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<Vec<LockedIdempotency>, ImageGatewayError> {
    sqlx::query_as(
        "SELECT state, terminal_outcome FROM idempotency_requests WHERE job_id = $1 FOR UPDATE",
    )
    .bind(job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(settlement_unavailable)
}

fn is_completed(quota_job: &LockedQuotaJob, work: &LockedWorkAttempt) -> bool {
    quota_job.quota_state == "committed"
        && quota_job.job_state == "succeeded"
        && work.work_state == "succeeded"
}

fn validate_completed_state(
    quota_job: &LockedQuotaJob,
    work: &LockedWorkAttempt,
    idempotency: &[LockedIdempotency],
) -> Result<(), ImageGatewayError> {
    let units = quota_job.requested_units;
    let work_complete = work.lease_owner.is_none()
        && work.lease_expires_at_ms.is_none()
        && work.attempt_state == "succeeded";
    let quota_complete = quota_job.committed_units == units
        && quota_job.released_units == 0
        && quota_job.charged_units == units;
    let idempotency_complete = idempotency.iter().all(|row| {
        row.state == "succeeded" && row.terminal_outcome.as_deref() == Some("succeeded")
    });
    if work_complete && quota_complete && idempotency_complete {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "settlement state is partially completed",
        ))
    }
}

fn validate_active_state(
    quota_job: &LockedQuotaJob,
    work: &LockedWorkAttempt,
    idempotency: &[LockedIdempotency],
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let lease_active = work.work_state == "running"
        && work.lease_owner.as_deref() == Some(lease.worker_id.as_str())
        && work
            .lease_expires_at_ms
            .is_some_and(|expires| expires > now)
        && work.attempt_state == "running";
    let quota_active = quota_job.quota_state == "reserved"
        && quota_job.committed_units == 0
        && quota_job.released_units == 0
        && quota_job.expires_at_ms > now;
    let job_active = matches!(
        quota_job.job_state.as_str(),
        "reserved" | "running" | "artifact_ready"
    ) && quota_job.charged_units == 0;
    let idempotency_active = idempotency
        .iter()
        .all(|row| row.state == "accepted" && row.terminal_outcome.is_none());
    if lease_active && quota_active && job_active && idempotency_active {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "settlement state is stale or invalid",
        ))
    }
}

async fn transition_work(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE work_items
        SET state = 'succeeded', lease_owner = NULL, lease_expires_at_ms = NULL,
            updated_at_ms = $6
        WHERE work_item_id = $1 AND job_id = $2 AND lease_epoch = $3
          AND lease_owner = $4 AND execution_id = $5
          AND state = 'running' AND lease_expires_at_ms > $6
        "#,
    )
    .bind(lease.work_item_id)
    .bind(lease.job_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .bind(lease.execution_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "work settlement")
}

async fn transition_attempt(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE job_attempts
        SET state = 'succeeded', finished_at_ms = $5, error_code = NULL, updated_at_ms = $5
        WHERE execution_id = $1 AND work_item_id = $2 AND lease_epoch = $3
          AND worker_id = $4 AND state = 'running'
        "#,
    )
    .bind(lease.execution_id)
    .bind(lease.work_item_id)
    .bind(lease.lease_epoch)
    .bind(&lease.worker_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "attempt settlement")
}

async fn transition_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    now: i64,
    expected_rows: usize,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE idempotency_requests
        SET state = 'succeeded', terminal_outcome = 'succeeded', updated_at_ms = $2
        WHERE job_id = $1 AND state = 'accepted'
        "#,
    )
    .bind(job_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    if result.rows_affected() == expected_rows as u64 {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(
            "idempotency settlement did not update every row",
        ))
    }
}

async fn insert_usage_event(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO usage_events
          (event_id, tenant_id, job_id, request_id, operation, units, outcome, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, 'charged', $7)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(&locked.quota_tenant_id)
    .bind(locked.quota_job_id)
    .bind(&locked.quota_request_id)
    .bind(&locked.operation)
    .bind(locked.requested_units)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    Ok(())
}

async fn commit_quota(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE quota_reservations
        SET committed_units = requested_units, state = 'committed', updated_at_ms = $2
        WHERE reservation_id = $1 AND tenant_id = $3 AND job_id = $4 AND state = 'reserved'
        "#,
    )
    .bind(locked.reservation_id)
    .bind(now)
    .bind(&locked.quota_tenant_id)
    .bind(locked.quota_job_id)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "quota settlement")
}

async fn succeed_job(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET state = 'succeeded', charged_units = $5, finished_at_ms = $4, updated_at_ms = $4
        WHERE job_id = $1 AND tenant_id = $2 AND reservation_id = $3 AND state = $6
        "#,
    )
    .bind(locked.quota_job_id)
    .bind(&locked.quota_tenant_id)
    .bind(locked.reservation_id)
    .bind(now)
    .bind(locked.requested_units)
    .bind(&locked.job_state)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    require_one_row(result, "job settlement")
}

async fn insert_metering_events(
    tx: &mut Transaction<'_, Postgres>,
    locked: &LockedQuotaJob,
    now: i64,
) -> Result<(), ImageGatewayError> {
    for event_type in ["quota_committed", "job_succeeded"] {
        sqlx::query(
            r#"
            INSERT INTO metering_events
              (event_id, tenant_id, job_id, reservation_id, request_id, operation,
               event_type, units, outcome, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'succeeded', $9)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&locked.quota_tenant_id)
        .bind(locked.quota_job_id)
        .bind(locked.reservation_id)
        .bind(&locked.quota_request_id)
        .bind(&locked.operation)
        .bind(event_type)
        .bind(locked.requested_units)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(settlement_unavailable)?;
    }
    Ok(())
}

async fn append_succeeded_events(
    tx: &mut Transaction<'_, Postgres>,
    lease: &WorkLease,
    now: i64,
) -> Result<(), ImageGatewayError> {
    let semantic_key = format!("work.{}.succeeded", lease.work_item_id);
    let payload = json!({
        "execution_id": lease.execution_id.to_string(),
        "lease_epoch": lease.lease_epoch,
    });
    sqlx::query(
        r#"
        INSERT INTO job_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, 'job.succeeded', $3, $4, $5)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(lease.job_id)
    .bind(&semantic_key)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO outbox_events
          (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms)
        VALUES ($1, $2, 'job.succeeded', $3, $4, $5)
        ON CONFLICT (job_id, semantic_key) DO NOTHING
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(lease.job_id)
    .bind(&semantic_key)
    .bind(&payload)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(settlement_unavailable)?;
    Ok(())
}

fn require_one_row(
    result: sqlx::postgres::PgQueryResult,
    transition: &str,
) -> Result<(), ImageGatewayError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(ImageGatewayError::internal(format!(
            "{transition} did not update exactly one row"
        )))
    }
}

fn quota_lock_id(tenant_id: &str) -> i64 {
    let mut hasher = Sha256::new();
    hasher.update(b"quota:");
    hasher.update(tenant_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    i64::from_be_bytes(bytes)
}

fn settlement_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("execution settlement unavailable")
}

#[cfg(test)]
mod tests {
    #[test]
    fn generation_percent_uses_only_observations_and_can_revise_downward() {
        let progress = |status: &str, eta: Option<u64>, start: Option<i64>, observed: i64| {
            video_provider_progress(
                Some(serde_json::json!({
                    "provider_status": status,
                    "estimated_remaining_seconds": eta,
                    "generation_started_at_ms": start,
                })),
                Some(observed),
            )
            .unwrap()
            .estimated_percent
        };
        assert_eq!(
            progress("Generating", Some(60), Some(1_000), 1_000),
            Some(0)
        );
        assert_eq!(
            progress("Generating", Some(30), Some(1_000), 31_000),
            Some(50)
        );
        assert_eq!(
            progress("Generating", Some(120), Some(1_000), 61_000),
            Some(33)
        );
        assert_eq!(
            progress("Generating", Some(0), Some(1_000), 61_000),
            Some(99)
        );
        assert_eq!(progress("Generating", Some(0), Some(1_000), 1_000), None);
        assert_eq!(progress("Generating", None, Some(1_000), 61_000), None);
        assert_eq!(progress("Generating", Some(30), None, 61_000), None);
        assert_eq!(progress("Generating", Some(30), Some(-1), 61_000), None);
        assert_eq!(progress("Generating", Some(30), Some(62_000), 61_000), None);
        assert_eq!(progress("Queueing", Some(30), Some(1_000), 61_000), None);
        assert_eq!(progress("Processing", Some(30), Some(1_000), 61_000), None);
        // Re-reading the identical observation has no wall-clock input to advance it.
        assert_eq!(
            progress("Generating", Some(30), Some(1_000), 31_000),
            Some(50)
        );
    }

    use super::*;
    use image_provider_dreamina_cli::{
        FramesToVideoRequestV1, ImageToVideoRequestV1, MultimodalToVideoRequestV1,
        ReferenceVideoOptionsV1, StagedImageV1, TextToVideoRequestV1, VideoModelVersion,
        VideoRatio, VideoResolution, encode_submit_command,
    };

    fn multiframe_command(durations_ms: &[u32]) -> serde_json::Value {
        use image_provider_dreamina_cli::{
            MultiframeToVideoRequestV1, StagedImageV1, VideoResolution, VideoTransitionV1,
            encode_submit_command,
        };
        let images = (0..=durations_ms.len())
            .map(|index| {
                StagedImageV1::new(format!("frame-{index}.png"), "a".repeat(64), 1600, 900).unwrap()
            })
            .collect();
        let transitions = durations_ms
            .iter()
            .map(|duration| VideoTransitionV1::new("move smoothly", *duration).unwrap())
            .collect();
        let request =
            MultiframeToVideoRequestV1::new(images, transitions, VideoResolution::P720).unwrap();
        serde_json::from_slice(&encode_submit_command(
            DreaminaSubmitRequestV1::MultiframeToVideo(request),
        ))
        .unwrap()
    }

    #[test]
    fn requested_multiframe_duration_preserves_milliseconds_and_checks_quota() {
        let command = multiframe_command(&[2_500]);
        assert_eq!(
            requested_video_duration_ms(3, Some(&command)).unwrap(),
            2_500
        );
        assert!(requested_video_duration_ms(2, Some(&command)).is_err());
        assert_eq!(requested_video_duration_ms(3, None).unwrap(), 3_000);
        assert!(requested_video_duration_ms(3, Some(&json!({}))).is_err());
        assert!(requested_video_duration_ms(0, None).is_err());
        let status = video_result_status((
            "running".into(),
            "provider_waiting".into(),
            None,
            "multiframe2video".into(),
            3,
            None,
            None,
            None,
            Some(command),
            None,
            None,
            None,
        ))
        .unwrap();
        assert!(matches!(
            status,
            VideoResultStatus::Pending {
                duration_ms: 2_500,
                ..
            }
        ));
    }

    fn queued_video_row(job_state: &str, work_state: &str) -> VideoStatusRow {
        (
            job_state.to_owned(),
            work_state.to_owned(),
            (job_state == "failed").then(|| "provider_failed".to_owned()),
            "seedance2.0fast".to_owned(),
            10,
            None,
            (job_state == "succeeded").then(|| "https://media.example.test/video.mp4".to_owned()),
            None,
            None,
            Some(json!({
                "provider_status":"Queueing", "queue_position":3341,
                "queue_total":476961, "estimated_remaining_seconds":27131
            })),
            Some(1_792_828_821_077),
            None,
        )
    }

    #[test]
    fn video_queue_progress_keeps_the_durable_time_for_pending_and_uncertain_only() {
        for (job_state, work_state) in [("reserved", "running"), ("uncertain", "uncertain")] {
            let status = video_result_status(queued_video_row(job_state, work_state)).unwrap();
            let progress = match status {
                VideoResultStatus::Pending {
                    provider_progress, ..
                }
                | VideoResultStatus::Uncertain {
                    provider_progress, ..
                } => provider_progress.unwrap(),
                _ => panic!("expected nonterminal projection"),
            };
            assert_eq!(progress.progress.provider_status, "Queueing");
            assert_eq!(progress.progress.queue_position, Some(3341));
            assert_eq!(progress.progress.queue_total, Some(476961));
            assert_eq!(progress.progress.estimated_remaining_seconds, Some(27131));
            assert_eq!(progress.observed_at_ms, 1_792_828_821_077);
        }
        assert!(matches!(
            video_result_status(queued_video_row("failed", "failed")).unwrap(),
            VideoResultStatus::Failed { .. }
        ));
        assert!(matches!(
            video_result_status(queued_video_row("succeeded", "succeeded")).unwrap(),
            VideoResultStatus::SucceededRemote { .. }
        ));
    }

    #[test]
    fn malformed_optional_queue_data_does_not_hide_the_video_state() {
        for value in [
            json!(null),
            json!([]),
            json!({"provider_status":"Queueing", "queue_position":"3341"}),
            json!({"provider_status":"Queueing", "queue_position":5,"queue_total":4}),
            json!({"provider_status":"bad\nstatus"}),
        ] {
            let mut row = queued_video_row("reserved", "running");
            row.9 = Some(value);
            let status = video_result_status(row).unwrap();
            assert!(matches!(
                status,
                VideoResultStatus::Pending {
                    provider_progress: None,
                    ..
                }
            ));
        }
        let mut row = queued_video_row("reserved", "running");
        row.10 = None;
        assert!(matches!(
            video_result_status(row).unwrap(),
            VideoResultStatus::Pending {
                provider_progress: None,
                ..
            }
        ));
    }

    #[test]
    fn requested_multiframe_duration_supports_the_exact_152_second_limit() {
        let command = multiframe_command(&[8_000; 19]);
        assert_eq!(
            requested_video_duration_ms(152, Some(&command)).unwrap(),
            152_000
        );
        assert!(requested_video_duration_ms(151, Some(&command)).is_err());
    }
    #[test]
    fn successful_video_requires_exactly_one_delivery_source() {
        let url = "https://media.example.com/video.mp4?signature=a%2Bb";
        let row = |artifact, remote| {
            (
                "succeeded".to_owned(),
                "succeeded".to_owned(),
                None,
                "seedance2.5".to_owned(),
                5,
                artifact,
                remote,
                None,
                None,
                None,
                None,
                None,
            )
        };
        let artifact = Uuid::new_v4();
        assert!(
            matches!(video_result_status(row(Some(artifact), None)).unwrap(),
            VideoResultStatus::Succeeded { artifact_id, .. } if artifact_id == artifact)
        );
        let VideoResultStatus::SucceededRemote { result, .. } =
            video_result_status(row(None, Some(url.to_owned()))).unwrap()
        else {
            panic!("expected remote delivery");
        };
        assert_eq!(result.video_url(), url);
        assert_eq!(result.expires_at_ms(), None);
        assert!(video_result_status(row(None, None)).is_err());
        assert!(video_result_status(row(Some(artifact), Some(url.to_owned()))).is_err());
        assert!(video_result_status(row(None, Some("file:///tmp/video.mp4".to_owned()))).is_err());
    }

    #[test]
    fn remote_multiframe_success_keeps_exact_milliseconds() {
        let url = "https://media.example.com/video.mp4?signature=a%2Bb";
        let status = video_result_status((
            "succeeded".into(),
            "succeeded".into(),
            None,
            "multiframe2video".into(),
            3,
            None,
            Some(url.to_owned()),
            None,
            Some(multiframe_command(&[2_500])),
            None,
            None,
            None,
        ))
        .unwrap();
        let VideoResultStatus::SucceededRemote {
            duration_ms,
            result,
            ..
        } = status
        else {
            panic!("expected remote delivery");
        };
        assert_eq!(duration_ms, 2_500);
        assert_eq!(result.video_url(), url);
    }

    #[test]
    fn video_metadata_comes_from_the_frozen_dreamina_command() {
        for (model, resolution, duration) in [
            (VideoModelVersion::Seedance2_0Fast, VideoResolution::P720, 5),
            (
                VideoModelVersion::Seedance2_0Vip,
                VideoResolution::P1080,
                15,
            ),
            (VideoModelVersion::Seedance2_5, VideoResolution::P480, 5),
            (VideoModelVersion::Seedance2_5, VideoResolution::P1080, 30),
        ] {
            let request = TextToVideoRequestV1::new(
                "an orange paper airplane",
                model,
                VideoRatio::R16x9,
                duration,
                resolution,
            )
            .unwrap();
            let command: serde_json::Value = serde_json::from_slice(&encode_submit_command(
                DreaminaSubmitRequestV1::TextToVideo(request),
            ))
            .unwrap();
            assert_eq!(command["operation"], "text2video");
            assert_eq!(command["schema_version"], 1);
            let metadata = video_request_metadata(Some(&command)).unwrap();
            assert_eq!(metadata.provider_model.as_deref(), Some(model.as_str()));
            assert_eq!(metadata.resolution.as_deref(), Some(resolution.as_str()));
            assert_eq!(metadata.ratio.as_deref(), Some("16:9"));
        }
    }

    #[test]
    fn missing_video_metadata_remains_unknown() {
        assert_eq!(
            video_request_metadata(None).unwrap(),
            VideoRequestMetadata::default()
        );
    }

    #[test]
    fn malformed_frozen_video_metadata_fails_closed() {
        for command in [json!(null), json!(480), json!(true), json!([]), json!({})] {
            assert!(video_request_metadata(Some(&command)).is_err());
        }
    }

    #[test]
    fn multiframe_metadata_preserves_its_fixed_workflow_model_and_unknown_ratio() {
        let command = multiframe_command(&[2_500]);
        assert_eq!(
            video_request_metadata(Some(&command)).unwrap(),
            VideoRequestMetadata {
                provider_model: Some("multiframe2video".to_owned()),
                resolution: Some("720p".to_owned()),
                ratio: None,
            }
        );
    }

    #[test]
    fn reference_and_multimodal_metadata_preserve_the_requested_model_and_ratio() {
        let first = StagedImageV1::new("first.png", "a".repeat(64), 1600, 900).unwrap();
        let last = StagedImageV1::new("last.png", "b".repeat(64), 1600, 900).unwrap();
        for (model, ratio) in [
            (VideoModelVersion::Seedance2_5, None),
            (VideoModelVersion::Seedance2_0Fast, Some(VideoRatio::R16x9)),
        ] {
            let options =
                ReferenceVideoOptionsV1::new("camera pan", model, ratio, 5, VideoResolution::P720)
                    .unwrap();
            for request in [
                DreaminaSubmitRequestV1::ImageToVideo(
                    ImageToVideoRequestV1::new(first.clone(), options.clone()).unwrap(),
                ),
                DreaminaSubmitRequestV1::FramesToVideo(
                    FramesToVideoRequestV1::new(first.clone(), last.clone(), options.clone())
                        .unwrap(),
                ),
            ] {
                let command = serde_json::from_slice(&encode_submit_command(request)).unwrap();
                assert_eq!(
                    video_request_metadata(Some(&command)).unwrap(),
                    VideoRequestMetadata {
                        provider_model: Some(model.as_str().to_owned()),
                        resolution: Some("720p".to_owned()),
                        ratio: ratio.map(|ratio| ratio.as_str().to_owned()),
                    }
                );
                assert_eq!(
                    requested_video_duration_ms(5, Some(&command)).unwrap(),
                    5_000
                );
            }
        }
        let request = MultimodalToVideoRequestV1::new(
            vec![first],
            vec![],
            vec![],
            "camera pan",
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            5,
            VideoResolution::P720,
        )
        .unwrap();
        let command = serde_json::from_slice(&encode_submit_command(
            DreaminaSubmitRequestV1::MultimodalToVideo(request),
        ))
        .unwrap();
        assert_eq!(
            video_request_metadata(Some(&command)).unwrap(),
            VideoRequestMetadata {
                provider_model: Some("seedance2.5".to_owned()),
                resolution: Some("720p".to_owned()),
                ratio: Some("16:9".to_owned()),
            }
        );
    }
}
