use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use image_scheduler_policy::{ScopeWeight, next_finish_tag};

use super::{AttachedRunningWork, LockedAdmissionSession};
use crate::admission::{
    AdmissionContract, AdmissionError, AttachJob, AttachedWork, WorkLease, attach_operation,
    provider_command_hash, validate_attach_request,
};

const MAX_SCHEDULE_PRIORITY: u8 = 3;

#[derive(sqlx::FromRow)]
struct AttachedReadyWork {
    command_schema: String,
    command_json: Value,
    payload_hash: String,
    work_item_id: Uuid,
}

#[derive(sqlx::FromRow)]
struct StoredInputManifest {
    admission_session_id: Uuid,
    manifest_schema: String,
    manifest_hash: String,
    input_count: i16,
}

#[derive(sqlx::FromRow)]
struct StoredInputObject {
    input_id: Uuid,
    admission_session_id: Uuid,
    role: String,
    input_index: i16,
    media_type: String,
    storage_backend: String,
    object_key: String,
    sha256_hex: String,
    byte_size: i64,
}

pub(super) async fn replay_attached_work(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
) -> Result<AttachedWork, AdmissionError> {
    let existing: AttachedReadyWork = sqlx::query_as(
        r#"
        SELECT p.command_schema, p.command_json, p.request_hash AS payload_hash,
               w.work_item_id
        FROM job_payloads p
        JOIN work_items w ON w.job_id = p.job_id
        WHERE p.job_id = $1 AND p.admission_session_id = $2
        FOR UPDATE OF w
        "#,
    )
    .bind(request.job_id)
    .bind(request.ticket.session_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(AdmissionError::InvalidOwner)?;
    if existing.command_schema != request.command_schema
        || existing.command_json != request.command_json
        || existing.payload_hash != provider_command_hash(request)?
        || !stored_inputs_match(tx, request).await?
    {
        return Err(AdmissionError::InvalidOwner);
    }
    Ok(AttachedWork {
        work_item_id: existing.work_item_id,
        job_id: request.job_id,
    })
}

async fn stored_inputs_match(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
) -> Result<bool, AdmissionError> {
    let stored: Option<StoredInputManifest> = sqlx::query_as(
        r#"
        SELECT admission_session_id, manifest_schema, manifest_hash, input_count
        FROM job_input_manifests
        WHERE job_id = $1
        "#,
    )
    .bind(request.job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    let Some(expected) = request.input_manifest.as_ref() else {
        return Ok(stored.is_none());
    };
    let Some(stored) = stored else {
        return Ok(false);
    };
    if stored.admission_session_id != request.ticket.session_id
        || stored.manifest_schema != expected.manifest_schema
        || stored.manifest_hash != expected.manifest_hash
        || usize::try_from(stored.input_count).ok() != Some(expected.inputs.len())
    {
        return Ok(false);
    }
    let objects: Vec<StoredInputObject> = sqlx::query_as(
        r#"
        SELECT input_id, admission_session_id, role, input_index, media_type,
               storage_backend, object_key, sha256_hex, byte_size
        FROM job_input_objects
        WHERE job_id = $1
        ORDER BY CASE role WHEN 'image' THEN 0 ELSE 1 END, input_index
        "#,
    )
    .bind(request.job_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(unavailable)?;
    if objects.len() != expected.inputs.len() {
        return Ok(false);
    }
    Ok(objects.iter().zip(&expected.inputs).all(|(stored, input)| {
        stored.input_id == input.blob.key.input_id
            && stored.admission_session_id == input.blob.key.admission_session_id
            && stored.role == input.role.as_str()
            && u16::try_from(stored.input_index).ok() == Some(input.index)
            && stored.media_type == input.media_type
            && stored.storage_backend == input.blob.storage_backend
            && stored.object_key == input.blob.object_key
            && stored.sha256_hex == input.blob.sha256_hex
            && u64::try_from(stored.byte_size).ok() == Some(input.blob.byte_size)
    }))
}

pub(super) async fn attach_and_start_work(
    pool: &PgPool,
    request: AttachJob,
    worker_id: &str,
    lease_duration_ms: i64,
) -> Result<WorkLease, AdmissionError> {
    validate_attach_request(&request)?;
    let payload_hash = provider_command_hash(&request)?;
    if request.contract != AdmissionContract::LegacyV1 {
        return Err(AdmissionError::InvalidCommand);
    }
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let now = database_now(&mut tx).await?;
    let session: Option<LockedAdmissionSession> = sqlx::query_as(
        r#"
            SELECT tenant_id, api_profile, operation, request_id, state, idempotency_key_digest,
                   request_hash, deadline_at_ms, job_id
            FROM admission_sessions
            WHERE session_id = $1 AND owner_token = $2
            FOR UPDATE
            "#,
    )
    .bind(request.ticket.session_id)
    .bind(request.ticket.owner_token)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    let Some(session) = session else {
        return Err(AdmissionError::InvalidOwner);
    };
    let expected_operation = attach_operation(&request)?;
    if session.request_hash != request.ticket.request_hash
        || session.operation != expected_operation
        || !job_matches_admission_identity(
            &mut tx,
            request.job_id,
            &session.tenant_id,
            expected_operation,
            &session.request_id,
        )
        .await?
    {
        return Err(AdmissionError::InvalidOwner);
    }

    if session.state == "attached" && session.job_id == Some(request.job_id) {
        bind_quota_reservation(&mut tx, &request, &session, now).await?;
        let existing: Option<AttachedRunningWork> = sqlx::query_as(
            r#"
            SELECT p.command_schema, p.command_json, p.request_hash AS payload_hash,
                   w.work_item_id, w.state AS work_state, w.lease_epoch, w.lease_owner,
                   w.lease_expires_at_ms, w.execution_id
            FROM job_payloads p
            JOIN work_items w ON w.job_id = p.job_id
            WHERE p.job_id = $1 AND p.admission_session_id = $2
            FOR UPDATE OF w
            "#,
        )
        .bind(request.job_id)
        .bind(request.ticket.session_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some(existing) = existing else {
            return Err(AdmissionError::InvalidOwner);
        };
        if existing.command_schema != request.command_schema
            || existing.command_json != request.command_json
            || existing.payload_hash != payload_hash
            || existing.work_state != "running"
            || existing.lease_owner.as_deref() != Some(worker_id)
            || existing
                .lease_expires_at_ms
                .is_none_or(|deadline| deadline <= now)
        {
            return Err(AdmissionError::InvalidOwner);
        }
        let execution_id = existing.execution_id.ok_or(AdmissionError::InvalidOwner)?;
        tx.commit().await.map_err(unavailable)?;
        return Ok(WorkLease {
            work_item_id: existing.work_item_id,
            job_id: request.job_id,
            execution_id,
            lease_epoch: existing.lease_epoch,
            worker_id: worker_id.to_string(),
            command_schema: existing.command_schema,
            command_json: existing.command_json,
        });
    }

    if session.state != "receiving" || session.job_id.is_some() {
        return Err(AdmissionError::InvalidOwner);
    }
    if session.deadline_at_ms <= now {
        abort_receiving_session(&mut tx, request.ticket.session_id, now).await?;
        tx.commit().await.map_err(unavailable)?;
        return Err(AdmissionError::Expired);
    }

    bind_quota_reservation(&mut tx, &request, &session, now).await?;
    persist_inputs(&mut tx, &request, now).await?;

    sqlx::query(
        r#"
        INSERT INTO job_payloads
          (job_id, admission_session_id, command_schema, command_json, request_hash, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(request.job_id)
    .bind(request.ticket.session_id)
    .bind(&request.command_schema)
    .bind(&request.command_json)
    .bind(&payload_hash)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;

    let schedule = reserve_schedule_slot(&mut tx, &request, now).await?;
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let lease_epoch = 1_i64;
    let lease_expires_at_ms = now.saturating_add(lease_duration_ms.max(1));
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id,
           schedule_scope, schedule_weight, schedule_priority, schedule_cost,
           schedule_finish_tag, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'running', $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $4, $4)
        "#,
    )
    .bind(work_item_id)
    .bind(request.job_id)
    .bind(&request.work_kind)
    .bind(now)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(lease_expires_at_ms)
    .bind(execution_id)
    .bind(&schedule.scope)
    .bind(schedule.weight)
    .bind(schedule.priority)
    .bind(schedule.cost)
    .bind(schedule.finish_tag)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id, state,
           started_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, 'running', $6, $6, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;

    sqlx::query(
        "UPDATE admission_sessions SET state = 'attached', job_id = $2, updated_at_ms = $3 WHERE session_id = $1",
    )
    .bind(request.ticket.session_id)
    .bind(request.job_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    if session.idempotency_key_digest.is_some() {
        sqlx::query(
            "UPDATE idempotency_requests SET state = 'accepted', job_id = $2, updated_at_ms = $3 WHERE session_id = $1",
        )
        .bind(request.ticket.session_id)
        .bind(request.job_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    }
    append_event_pair(
        &mut tx,
        request.job_id,
        "job.accepted",
        "job.accepted",
        json!({}),
        now,
    )
    .await?;
    tx.commit().await.map_err(unavailable)?;
    Ok(WorkLease {
        work_item_id,
        job_id: request.job_id,
        execution_id,
        lease_epoch,
        worker_id: worker_id.to_string(),
        command_schema: request.command_schema,
        command_json: request.command_json,
    })
}

pub(super) async fn bind_quota_reservation(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    session: &LockedAdmissionSession,
    now: i64,
) -> Result<(), AdmissionError> {
    let bound = sqlx::query(
        r#"
        UPDATE quota_reservations
        SET admission_session_id = COALESCE(admission_session_id, $2),
            updated_at_ms = CASE
                WHEN admission_session_id IS NULL THEN $5
                ELSE updated_at_ms
            END
        WHERE job_id = $1 AND tenant_id = $3 AND request_id = $4
          AND state = 'reserved'
          AND (admission_session_id IS NULL OR admission_session_id = $2)
        "#,
    )
    .bind(request.job_id)
    .bind(request.ticket.session_id)
    .bind(&session.tenant_id)
    .bind(&session.request_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if bound != 1 {
        return Err(AdmissionError::InvalidOwner);
    }
    Ok(())
}

pub(super) async fn persist_inputs(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    now: i64,
) -> Result<(), AdmissionError> {
    let Some(manifest) = request.input_manifest.as_ref() else {
        return Ok(());
    };
    sqlx::query(
        r#"
        INSERT INTO job_input_manifests
          (job_id, admission_session_id, manifest_schema, manifest_hash, input_count, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(request.job_id)
    .bind(request.ticket.session_id)
    .bind(&manifest.manifest_schema)
    .bind(&manifest.manifest_hash)
    .bind(i16::try_from(manifest.inputs.len()).map_err(|_| AdmissionError::InvalidCommand)?)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    for input in &manifest.inputs {
        sqlx::query(
            r#"
            INSERT INTO job_input_objects
              (input_id, job_id, admission_session_id, role, input_index, media_type,
               storage_backend, object_key, sha256_hex, byte_size, created_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(input.blob.key.input_id)
        .bind(request.job_id)
        .bind(request.ticket.session_id)
        .bind(input.role.as_str())
        .bind(i16::try_from(input.index).map_err(|_| AdmissionError::InvalidCommand)?)
        .bind(&input.media_type)
        .bind(&input.blob.storage_backend)
        .bind(&input.blob.object_key)
        .bind(&input.blob.sha256_hex)
        .bind(i64::try_from(input.blob.byte_size).map_err(|_| AdmissionError::InvalidCommand)?)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }
    Ok(())
}

pub(super) async fn claim_work(
    pool: &PgPool,
    target_job_id: Option<Uuid>,
    worker_id: &str,
    lease_duration_ms: i64,
    contract: Option<AdmissionContract>,
    command_schema: Option<&str>,
    execution_profile_id: Option<Uuid>,
) -> Result<Option<WorkLease>, AdmissionError> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    if let Some(execution_profile_id) = execution_profile_id {
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('work-claim-profile:' || $1::TEXT, 0))",
        )
        .bind(execution_profile_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    }
    let now = database_now(&mut tx).await?;
    let row: Option<(Uuid, Uuid, i64, String, Value)> = sqlx::query_as(
        r#"
        SELECT w.work_item_id, w.job_id, w.lease_epoch, p.command_schema, p.command_json
        FROM work_items w
        JOIN jobs j ON j.job_id = w.job_id
        JOIN job_payloads p ON p.job_id = w.job_id
        WHERE w.state = 'ready' AND w.available_at_ms <= $1
          AND ($2::UUID IS NULL OR w.job_id = $2)
          AND ($3::SMALLINT IS NULL OR j.economics_contract_version = $3)
          AND ($4::TEXT IS NULL OR p.command_schema = $4)
          AND ($5::UUID IS NULL OR w.execution_profile_id IS NULL OR w.execution_profile_id = $5)
          AND (
            $5::UUID IS NULL
            OR EXISTS (
              SELECT 1
              FROM provider_execution_profiles claim_profile
              JOIN executor_resource_policies claim_policy
                ON claim_policy.resource_policy_id = claim_profile.resource_policy_id
               AND claim_policy.revision = claim_profile.resource_policy_revision
               AND claim_policy.provider_account_id = claim_profile.provider_account_id
              JOIN provider_account_execution_controls claim_control
                ON claim_control.provider_account_id = claim_profile.provider_account_id
              WHERE claim_profile.execution_profile_id = $5
                AND (
                  SELECT COUNT(*)
                  FROM work_items active_work
                  WHERE active_work.execution_profile_id = claim_profile.execution_profile_id
                    AND active_work.state IN ('leased', 'running')
                ) < LEAST(
                  claim_policy.max_concurrency,
                  claim_control.desired_max_concurrency
                )
            )
          )
          AND (
            $5::UUID IS NULL
            OR NOT EXISTS (
              SELECT 1
              FROM provider_execution_profiles configured_profile
              JOIN provider_account_model_configurations model_config
                ON model_config.provider_account_id = configured_profile.provider_account_id
               AND model_config.provider_id = configured_profile.provider_id
               AND model_config.mode = 'allowlist'
              WHERE configured_profile.execution_profile_id = $5
            )
            OR EXISTS (
              SELECT 1
              FROM provider_execution_profiles configured_profile
              JOIN provider_account_model_bindings model_binding
                ON model_binding.provider_account_id = configured_profile.provider_account_id
               AND model_binding.provider_id = configured_profile.provider_id
              JOIN provider_models configured_model
                ON configured_model.provider_id = model_binding.provider_id
               AND configured_model.model_id = model_binding.model_id
               AND configured_model.media_kind = model_binding.media_kind
               AND configured_model.execution_model_id = j.model
               AND configured_profile.operation_id = ANY(configured_model.operation_ids)
              WHERE configured_profile.execution_profile_id = $5
            )
          )
          AND (
            NOT EXISTS (
                SELECT 1 FROM job_provider_route_attributions route
                WHERE route.job_id = w.job_id
            )
            OR ($5::UUID IS NOT NULL AND $5 = (
                SELECT member.execution_profile_id
                FROM job_provider_route_attributions route
                JOIN provider_routes route_config
                  ON route_config.route_id = route.route_id
                 AND route_config.revision = route.route_revision
                 AND route_config.provider_id = route.provider_id
                 AND route_config.operation_id = route.operation_id
                 AND route_config.command_schema = route.command_schema
                JOIN provider_route_members member
                  ON member.route_id = route.route_id
                 AND member.route_revision = route.route_revision
                 AND member.provider_id = route.provider_id
                 AND member.operation_id = route.operation_id
                 AND member.command_schema = route.command_schema
                JOIN provider_execution_profiles profile
                  ON profile.execution_profile_id = member.execution_profile_id
                 AND profile.provider_account_id = member.provider_account_id
                 AND profile.provider_id = member.provider_id
                 AND profile.operation_id = member.operation_id
                 AND profile.command_schema = member.command_schema
                JOIN provider_accounts account
                 ON account.provider_account_id = profile.provider_account_id
                 AND account.provider_id = profile.provider_id
                JOIN provider_account_environments environment
                  ON environment.provider_account_id = account.provider_account_id
                 AND environment.provider_id = account.provider_id
                JOIN provider_credential_pools pool
                  ON pool.credential_pool_id = profile.credential_pool_id
                 AND pool.provider_id = profile.provider_id
                JOIN executor_resource_policies policy
                  ON policy.resource_policy_id = profile.resource_policy_id
                 AND policy.revision = profile.resource_policy_revision
                 AND policy.provider_account_id = profile.provider_account_id
                JOIN provider_account_execution_controls control
                  ON control.provider_account_id = profile.provider_account_id
                LEFT JOIN LATERAL (
                  SELECT MAX(quota_window.used_percent) AS highest_used_percent,
                         COUNT(*) > 0 AS has_fresh_quota,
                         BOOL_OR(
                           quota_window.used_percent >= 100
                           OR quota_window.used_percent
                              > 100 - member.minimum_remaining_percent
                         ) AS exhausted
                  FROM provider_account_quota_snapshots quota_snapshot
                  JOIN provider_account_quota_windows quota_window
                    ON quota_window.provider_account_id = quota_snapshot.provider_account_id
                   AND quota_window.provider_id = quota_snapshot.provider_id
                   AND quota_window.observed_at_ms = quota_snapshot.observed_at_ms
                  WHERE quota_snapshot.provider_account_id = member.provider_account_id
                    AND quota_snapshot.status = 'observed'
                    AND quota_snapshot.observed_at_ms >= $1 - route_config.quota_freshness_ms
                    AND (quota_window.resets_at_ms IS NULL OR quota_window.resets_at_ms > $1)
                ) quota ON TRUE
                WHERE route.job_id = w.job_id
                  AND member.state = 'enabled'
                  AND profile.state = 'enabled'
                  AND account.state = 'enabled'
                  AND environment.state = 'active'
                  AND pool.state = 'enabled'
                  AND policy.state = 'enabled'
                  AND (
                    NOT EXISTS (
                      SELECT 1 FROM provider_account_model_configurations model_config
                      WHERE model_config.provider_account_id = profile.provider_account_id
                        AND model_config.provider_id = profile.provider_id
                        AND model_config.mode = 'allowlist'
                    )
                    OR EXISTS (
                      SELECT 1
                      FROM provider_account_model_bindings model_binding
                      JOIN provider_models configured_model
                        ON configured_model.provider_id = model_binding.provider_id
                       AND configured_model.model_id = model_binding.model_id
                       AND configured_model.media_kind = model_binding.media_kind
                       AND configured_model.execution_model_id = j.model
                       AND profile.operation_id = ANY(configured_model.operation_ids)
                      WHERE model_binding.provider_account_id = profile.provider_account_id
                        AND model_binding.provider_id = profile.provider_id
                    )
                  )
                  AND (
                    control.lifecycle_state = 'active'
                    OR w.execution_profile_id = profile.execution_profile_id
                  )
                  AND policy.allocated_count
                      < LEAST(policy.max_concurrency, control.desired_max_concurrency)
                  AND (
                    route_config.unknown_quota_policy = 'allow'
                    OR COALESCE(quota.has_fresh_quota, FALSE)
                  )
                  AND NOT COALESCE(quota.exhausted, FALSE)
                ORDER BY member.priority DESC,
                         CASE WHEN COALESCE(quota.has_fresh_quota, FALSE) THEN 0 ELSE 1 END ASC,
                         CASE WHEN route_config.selection_strategy = 'quota_aware_least_loaded'
                           THEN COALESCE(quota.highest_used_percent, 50)
                         END ASC NULLS LAST,
                         CASE WHEN route_config.selection_strategy = 'quota_aware_least_loaded'
                           THEN policy.allocated_count::NUMERIC
                                / control.desired_max_concurrency::NUMERIC
                         END ASC NULLS LAST,
                         -LN(
                           (
                             (
                               ('x' || SUBSTR(
                                 md5(
                                   route.route_id::TEXT || ':' || route.route_revision::TEXT
                                   || ':' || w.job_id::TEXT || ':'
                                   || member.execution_profile_id::TEXT
                                 ),
                                 1,
                                 15
                               ))::BIT(60)::BIGINT + 1
                             )::NUMERIC / 1152921504606846977::NUMERIC
                           )
                         ) / member.weight::NUMERIC,
                         member.execution_profile_id
                LIMIT 1
            ))
          )
        ORDER BY
          (w.schedule_finish_tag -
             ((GREATEST($1 - w.created_at_ms, 0) / 30000) * 250000)),
          w.schedule_priority DESC,
          w.available_at_ms, w.created_at_ms, w.work_item_id
        FOR UPDATE OF w SKIP LOCKED LIMIT 1
        "#,
    )
    .bind(now)
    .bind(target_job_id)
    .bind(contract.map(|contract| match contract {
        AdmissionContract::LegacyV1 => 1_i16,
        AdmissionContract::OutputEconomicsV2 => 2_i16,
        AdmissionContract::MediaEconomicsV3 => 3_i16,
        AdmissionContract::CustomerPricingV4 => 4_i16,
    }))
    .bind(command_schema)
    .bind(execution_profile_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    let Some((work_item_id, job_id, previous_epoch, command_schema, command_json)) = row else {
        tx.commit().await.map_err(unavailable)?;
        return Ok(None);
    };
    let lease_epoch = previous_epoch + 1;
    let execution_id = Uuid::new_v4();
    sqlx::query(
        r#"
        UPDATE work_items SET state = 'leased', lease_epoch = $2, lease_owner = $3,
          lease_expires_at_ms = $4, execution_id = $5, updated_at_ms = $6,
          execution_profile_id = COALESCE(execution_profile_id, $7)
        WHERE work_item_id = $1
          AND ($7::UUID IS NULL OR execution_profile_id IS NULL OR execution_profile_id = $7)
        "#,
    )
    .bind(work_item_id)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(now.saturating_add(lease_duration_ms.max(1)))
    .bind(execution_id)
    .bind(now)
    .bind(execution_profile_id)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id, state,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, $5, 'claimed', $6, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(lease_epoch)
    .bind(worker_id)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    tx.commit().await.map_err(unavailable)?;
    Ok(Some(WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch,
        worker_id: worker_id.to_string(),
        command_schema,
        command_json,
    }))
}

pub(super) async fn transition_active(
    pool: &PgPool,
    lease: &WorkLease,
    from: &str,
    to: &str,
    error_code: Option<&str>,
) -> Result<(), AdmissionError> {
    let mut tx = pool.begin().await.map_err(unavailable)?;
    let now = database_now(&mut tx).await?;
    let terminal = matches!(to, "succeeded" | "failed" | "uncertain");
    let changed = if terminal {
        sqlx::query(
            r#"
            UPDATE work_items SET state = $5, lease_owner = NULL, lease_expires_at_ms = NULL,
              updated_at_ms = $6
            WHERE work_item_id = $1 AND lease_epoch = $2 AND lease_owner = $3
              AND execution_id = $4 AND state = $7 AND lease_expires_at_ms > $6
              AND job_id = $8
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.execution_id)
        .bind(to)
        .bind(now)
        .bind(from)
        .bind(lease.job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected()
    } else {
        sqlx::query(
            r#"
            UPDATE work_items SET state = $5, updated_at_ms = $6
            WHERE work_item_id = $1 AND lease_epoch = $2 AND lease_owner = $3
              AND execution_id = $4 AND state = $7 AND lease_expires_at_ms > $6
              AND job_id = $8
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .bind(lease.execution_id)
        .bind(to)
        .bind(now)
        .bind(from)
        .bind(lease.job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected()
    };
    if changed != 1 {
        return Err(AdmissionError::StaleLease);
    }
    sqlx::query(
        r#"
        UPDATE job_attempts SET state = $2,
          started_at_ms = CASE WHEN $2 = 'running' THEN COALESCE(started_at_ms, $3) ELSE started_at_ms END,
          finished_at_ms = CASE WHEN $2 IN ('succeeded', 'failed', 'uncertain') THEN $3 ELSE NULL END,
          error_code = $4, updated_at_ms = $3
        WHERE execution_id = $1 AND lease_epoch = $5
        "#,
    )
    .bind(lease.execution_id).bind(to).bind(now).bind(error_code).bind(lease.lease_epoch)
    .execute(&mut *tx).await.map_err(unavailable)?;
    if terminal {
        sqlx::query(
            "UPDATE idempotency_requests SET state = $2, terminal_outcome = $2, updated_at_ms = $3 WHERE job_id = $1",
        )
        .bind(lease.job_id).bind(to).bind(now)
        .execute(&mut *tx).await.map_err(unavailable)?;
        let semantic_key = format!("work.{}.{}", lease.work_item_id, to);
        append_event_pair(
            &mut tx,
            lease.job_id,
            &format!("job.{to}"),
            &semantic_key,
            json!({"execution_id": lease.execution_id.to_string(), "lease_epoch": lease.lease_epoch}),
            now,
        )
        .await?;
    }
    tx.commit().await.map_err(unavailable)?;
    Ok(())
}

pub(super) async fn append_event_pair(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    event_type: &str,
    semantic_key: &str,
    payload: Value,
    now: i64,
) -> Result<(), AdmissionError> {
    sqlx::query(
        "INSERT INTO job_events (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (job_id, semantic_key) DO NOTHING",
    )
    .bind(Uuid::new_v4()).bind(job_id).bind(event_type).bind(semantic_key)
    .bind(&payload).bind(now).execute(&mut **tx).await.map_err(unavailable)?;
    sqlx::query(
        "INSERT INTO outbox_events (event_id, job_id, event_type, semantic_key, payload_json, created_at_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (job_id, semantic_key) DO NOTHING",
    )
    .bind(Uuid::new_v4()).bind(job_id).bind(event_type).bind(semantic_key)
    .bind(&payload).bind(now).execute(&mut **tx).await.map_err(unavailable)?;
    Ok(())
}

pub(super) async fn job_matches_admission_identity(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    tenant_id: &str,
    operation: &str,
    request_id: &str,
) -> Result<bool, AdmissionError> {
    let found: Option<(String, String, String)> = sqlx::query_as(
        "SELECT tenant_id, operation, request_id FROM jobs WHERE job_id = $1 FOR UPDATE",
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(found
        .as_ref()
        .is_some_and(|(found_tenant, found_operation, found_request_id)| {
            found_tenant == tenant_id
                && found_operation == operation
                && found_request_id == request_id
        }))
}

pub(super) async fn abort_receiving_session(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    now: i64,
) -> Result<(), AdmissionError> {
    sqlx::query(
        "UPDATE admission_sessions SET state = 'aborted', updated_at_ms = $2 WHERE session_id = $1 AND state = 'receiving'",
    )
    .bind(session_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        "UPDATE idempotency_requests SET state = 'aborted', updated_at_ms = $2 WHERE session_id = $1 AND state = 'receiving'",
    )
    .bind(session_id)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub(super) async fn database_now(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<i64, AdmissionError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

pub(super) struct ScheduledWork {
    pub(super) scope: String,
    pub(super) weight: i32,
    pub(super) priority: i16,
    pub(super) cost: i64,
    pub(super) finish_tag: i64,
}

pub(super) async fn reserve_schedule_slot(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    now: i64,
) -> Result<ScheduledWork, AdmissionError> {
    let Some(weight) = ScopeWeight::new(request.schedule_weight) else {
        return Err(AdmissionError::InvalidCommand);
    };
    if request.schedule_scope.is_empty()
        || request.schedule_priority > MAX_SCHEDULE_PRIORITY
        || request.schedule_cost == 0
        || request.schedule_cost > i64::MAX as u64
    {
        return Err(AdmissionError::InvalidCommand);
    }

    let weight = i32::try_from(weight.value()).map_err(|_| AdmissionError::InvalidCommand)?;
    sqlx::query(
        r#"
        INSERT INTO scheduler_scopes (scope_key, weight, next_finish_tag, updated_at_ms)
        VALUES ($1, $2, 0, $3)
        ON CONFLICT (scope_key) DO UPDATE
        SET weight = EXCLUDED.weight, updated_at_ms = EXCLUDED.updated_at_ms
        "#,
    )
    .bind(&request.schedule_scope)
    .bind(weight)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;

    let previous_finish: i64 = sqlx::query_scalar(
        "SELECT next_finish_tag FROM scheduler_scopes WHERE scope_key = $1 FOR UPDATE",
    )
    .bind(&request.schedule_scope)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    let previous_finish =
        u64::try_from(previous_finish).map_err(|_| AdmissionError::Unavailable)?;
    let finish_tag = next_finish_tag(
        previous_finish,
        request.schedule_cost,
        ScopeWeight::new(u32::try_from(weight).map_err(|_| AdmissionError::InvalidCommand)?)
            .ok_or(AdmissionError::InvalidCommand)?,
    );
    let finish_tag = i64::try_from(finish_tag).unwrap_or(i64::MAX);

    sqlx::query(
        "UPDATE scheduler_scopes SET next_finish_tag = $2, updated_at_ms = $3 WHERE scope_key = $1",
    )
    .bind(&request.schedule_scope)
    .bind(finish_tag)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;

    Ok(ScheduledWork {
        scope: request.schedule_scope.clone(),
        weight,
        priority: i16::from(request.schedule_priority),
        cost: request.schedule_cost as i64,
        finish_tag,
    })
}

pub(super) fn unavailable(error: impl std::fmt::Display) -> AdmissionError {
    tracing::error!(error = %error, "PostgreSQL admission operation failed");
    AdmissionError::Unavailable
}
