use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::artifacts::executor_object_key;
use crate::executor::{
    ExecutorResultManifest, ExecutorSubmissionError, release_capacity_allocation,
};

use super::{
    ProviderArtifactAuthority, ProviderRemoteTask, ProviderSubmitFailureKind, ProviderSubmitIntent,
    ProviderSubmitIntentState, ProviderSubmitStart, ProviderTaskClaimScope, ProviderTaskLease,
    ProviderTaskObservation, ProviderTaskObservationOutcome, ProviderTaskObservationSource,
    ProviderTaskState, ProviderTaskStore, ProviderTaskStoreError, RemoteTaskAttach,
    RemoteTaskSubmitFailure, RemoteTaskSubmitReceipt, RemoteTaskSubmitReservation,
    VerifiedCallbackWakeup,
};

const MAX_POLL_AFTER_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_LEASE_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Clone)]
pub struct PostgresProviderTaskStore {
    pool: PgPool,
}

impl PostgresProviderTaskStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct TaskRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    remote_operation_id: String,
    provider_request_id: Option<String>,
    state: String,
    artifact_ref: Option<String>,
    error_code: Option<String>,
    next_poll_at_ms: Option<i64>,
    cancel_requested: bool,
    poll_lease_epoch: i64,
    state_observation_id: Uuid,
}

#[derive(sqlx::FromRow)]
struct ClaimRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    remote_operation_id: String,
    provider_request_id: Option<String>,
    state: String,
    artifact_ref: Option<String>,
    error_code: Option<String>,
    next_poll_at_ms: Option<i64>,
    cancel_requested: bool,
    poll_lease_epoch: i64,
    state_observation_id: Uuid,
    poll_owner: String,
    poll_lease_expires_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct ExistingObservation {
    observation_id: Uuid,
    source: String,
    observed_state: String,
    artifact_ref: Option<String>,
    error_code: Option<String>,
    effect_certainty: String,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<String>,
    poll_lease_epoch: Option<i64>,
    payload_hash: String,
}

#[derive(sqlx::FromRow)]
struct ResolutionDecisionRow {
    resolved_state: String,
    result_manifest_id: Option<Uuid>,
    error_code: Option<String>,
    provider_task_observation_id: Option<Uuid>,
    provider_submit_intent_id: Option<Uuid>,
}

#[derive(sqlx::FromRow)]
struct SubmitIntentRow {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    provider_id: String,
    provider_account_id: Uuid,
    submit_owner: String,
    submit_lease_epoch: i64,
    idempotency_key: String,
    state: String,
    remote_operation_id: Option<String>,
    provider_request_id: Option<String>,
    send_started_at_ms: Option<i64>,
    receipt_event_identity: Option<String>,
    failure_event_identity: Option<String>,
    failure_error_code: Option<String>,
    updated_at_ms: i64,
}

#[derive(sqlx::FromRow)]
struct SubmitParentRow {
    provider_id: String,
    provider_account_id: Uuid,
    execution_state: String,
    submission_state: String,
    executor_owner: Option<String>,
    lease_epoch: i64,
    launch_owner: Option<String>,
    launch_lease_epoch: Option<i64>,
    allocation_state: String,
}

#[async_trait]
impl ProviderTaskStore for PostgresProviderTaskStore {
    async fn reserve_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        validate_reservation(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let binding: Option<(
            String,
            Uuid,
            String,
            Option<String>,
            i64,
            Option<i64>,
            Option<i64>,
        )> = sqlx::query_as(
            r#"
                SELECT submission.provider_id, submission.provider_account_id,
                       submission.state, execution.executor_owner, execution.lease_epoch,
                       execution.launch_lease_epoch, execution.lease_expires_at_ms
                FROM executor_executions execution
                JOIN provider_submissions submission
                  ON submission.executor_execution_id = execution.executor_execution_id
                 AND submission.submission_id = execution.submission_id
                WHERE execution.executor_execution_id = $1
                  AND execution.submission_id = $2
                FOR UPDATE OF execution, submission
                "#,
        )
        .bind(request.executor_execution_id)
        .bind(request.submission_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let Some((
            provider_id,
            provider_account_id,
            submission_state,
            owner,
            epoch,
            launch_epoch,
            expires_at_ms,
        )) = binding
        else {
            return Err(ProviderTaskStoreError::NotFound);
        };
        if let Some(existing) = load_submit_intent_in(&mut tx, request.submission_id).await? {
            let matches = existing.executor_execution_id == request.executor_execution_id
                && existing.submit_owner == request.executor_owner
                && existing.submit_lease_epoch == request.executor_lease_epoch
                && existing.idempotency_key == request.idempotency_key;
            if !matches {
                return Err(ProviderTaskStoreError::Conflict);
            }
            let intent = submit_intent_from_row(existing)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(intent);
        }
        let now = database_now(&mut tx).await?;
        if submission_state != "running"
            || owner.as_deref() != Some(request.executor_owner.as_str())
            || epoch != request.executor_lease_epoch
            || launch_epoch != Some(request.executor_lease_epoch)
            || expires_at_ms.is_none_or(|value| value <= now)
        {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        sqlx::query(
            r#"
            INSERT INTO provider_remote_submit_intents
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'reserved', $8, $8)
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(&provider_id)
        .bind(provider_account_id)
        .bind(&request.executor_owner)
        .bind(request.executor_lease_epoch)
        .bind(&request.idempotency_key)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        tx.commit().await.map_err(unavailable)?;
        Ok(intent)
    }

    async fn start_submit(
        &self,
        request: &RemoteTaskSubmitReservation,
    ) -> Result<ProviderSubmitStart, ProviderTaskStoreError> {
        validate_reservation(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        if !lock_live_submit_fence(&mut tx, request).await? {
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let row = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if !submit_intent_matches_reservation(&row, request) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let intent = submit_intent_from_row(row)?;
        if intent.state != ProviderSubmitIntentState::Reserved {
            tx.commit().await.map_err(unavailable)?;
            return Ok(ProviderSubmitStart::Existing(intent));
        }
        let now = database_now(&mut tx).await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = 'sending', send_started_at_ms = $3, updated_at_ms = $3
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'reserved'
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        tx.commit().await.map_err(unavailable)?;
        Ok(ProviderSubmitStart::Acquired(intent))
    }

    async fn record_submit_failure(
        &self,
        request: &RemoteTaskSubmitFailure,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        validate_submit_failure(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_submit_parent(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
        )
        .await?;
        let row = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if row.executor_execution_id != request.executor_execution_id
            || row.submit_owner != request.executor_owner
            || row.submit_lease_epoch != request.executor_lease_epoch
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let target_state = match request.kind {
            ProviderSubmitFailureKind::Rejected => "rejected",
            ProviderSubmitFailureKind::OutcomeUnknown => "outcome_unknown",
        };
        if row.state != "sending" {
            let intent = submit_intent_from_row(row)?;
            let compatible_state = match request.kind {
                ProviderSubmitFailureKind::Rejected => {
                    intent.state == ProviderSubmitIntentState::Rejected
                }
                ProviderSubmitFailureKind::OutcomeUnknown => matches!(
                    intent.state,
                    ProviderSubmitIntentState::OutcomeUnknown
                        | ProviderSubmitIntentState::OperationKnown
                        | ProviderSubmitIntentState::Attached
                ),
            };
            let replay = compatible_state
                && intent.failure_event_identity.as_deref() == Some(&request.event_identity)
                && intent.failure_error_code.as_deref() == Some(&request.error_code);
            if replay {
                tx.commit().await.map_err(unavailable)?;
                return Ok(intent);
            }
            return Err(ProviderTaskStoreError::Conflict);
        }
        if !submit_parent_accepts_evidence(&parent, &row) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = $3, failure_event_identity = $4,
                    failure_error_code = $5, updated_at_ms = $6
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'sending'
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(target_state)
            .bind(&request.event_identity)
            .bind(&request.error_code)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        if request.kind == ProviderSubmitFailureKind::Rejected {
            resolve_submit_terminal(&mut tx, &intent, "failed", now).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(intent)
    }

    async fn record_submit_receipt(
        &self,
        request: &RemoteTaskSubmitReceipt,
    ) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
        validate_submit_receipt(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_submit_parent(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
        )
        .await?;
        let row = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if row.executor_execution_id != request.executor_execution_id
            || row.submit_owner != request.executor_owner
            || row.submit_lease_epoch != request.executor_lease_epoch
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        if !matches!(row.state.as_str(), "sending" | "outcome_unknown") {
            let intent = submit_intent_from_row(row)?;
            let replay = matches!(
                intent.state,
                ProviderSubmitIntentState::OperationKnown | ProviderSubmitIntentState::Attached
            ) && intent.remote_operation_id.as_deref()
                == Some(&request.remote_operation_id)
                && intent.provider_request_id == request.provider_request_id
                && intent.receipt_event_identity.as_deref() == Some(&request.event_identity);
            if replay {
                tx.commit().await.map_err(unavailable)?;
                return Ok(intent);
            }
            return Err(ProviderTaskStoreError::Conflict);
        }
        if !submit_parent_accepts_evidence(&parent, &row) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = 'operation_known', remote_operation_id = $3,
                    provider_request_id = $4, receipt_event_identity = $5,
                    updated_at_ms = $6
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state IN ('sending', 'outcome_unknown')
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(&request.remote_operation_id)
            .bind(&request.provider_request_id)
            .bind(&request.event_identity)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        let intent = submit_intent_from_row(
            load_submit_intent_in(&mut tx, request.submission_id)
                .await?
                .ok_or(ProviderTaskStoreError::NotFound)?,
        )?;
        tx.commit().await.map_err(unavailable)?;
        Ok(intent)
    }

    async fn load_submit_intent(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderSubmitIntent>, ProviderTaskStoreError> {
        if submission_id.is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        load_submit_intent(&self.pool, submission_id)
            .await?
            .map(submit_intent_from_row)
            .transpose()
    }

    async fn attach(
        &self,
        request: &RemoteTaskAttach,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        validate_attach(request)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let parent = lock_submit_parent(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
        )
        .await?;
        let intent = load_submit_intent_in(&mut tx, request.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::Conflict)?;
        if intent.executor_execution_id != request.executor_execution_id
            || intent.provider_id != parent.provider_id
            || intent.provider_account_id != parent.provider_account_id
            || intent.submit_owner != request.executor_owner
            || intent.submit_lease_epoch != request.executor_lease_epoch
            || !matches!(intent.state.as_str(), "operation_known" | "attached")
            || intent.remote_operation_id.as_deref() != Some(request.remote_operation_id.as_str())
            || intent.provider_request_id != request.provider_request_id
        {
            return Err(ProviderTaskStoreError::Conflict);
        }
        if let Some(existing) = load_task_in(&mut tx, request.submission_id).await? {
            let task = task_from_row(existing)?;
            if task.executor_execution_id == request.executor_execution_id
                && task.remote_operation_id == request.remote_operation_id
                && task.provider_request_id == request.provider_request_id
            {
                tx.commit().await.map_err(unavailable)?;
                return Ok(task);
            }
            return Err(ProviderTaskStoreError::Conflict);
        }
        if intent.state != "operation_known" {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        if !submit_parent_accepts_evidence(&parent, &intent) {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let next_poll_at_ms = now + request.poll_after_ms;
        let observation_id = Uuid::new_v4();
        let payload_hash = observation_hash(
            "submit_attach",
            "provider_waiting",
            None,
            None,
            "not_applicable",
            Some(next_poll_at_ms),
            None,
            None,
        );
        require_one(
            sqlx::query(
                r#"
                UPDATE provider_remote_submit_intents
                SET state = 'attached', updated_at_ms = $3
                WHERE submission_id = $1 AND executor_execution_id = $2
                  AND state = 'operation_known'
                "#,
            )
            .bind(request.submission_id)
            .bind(request.executor_execution_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_conflict)?,
            ProviderTaskStoreError::Conflict,
        )?;
        sqlx::query(
            r#"
            INSERT INTO provider_remote_tasks
              (submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, submit_owner, submit_lease_epoch,
               state, effect_certainty, next_poll_at_ms, state_observation_id,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                    'provider_waiting', 'not_applicable', $9, $10, $11, $11)
            "#,
        )
        .bind(request.submission_id)
        .bind(request.executor_execution_id)
        .bind(&parent.provider_id)
        .bind(parent.provider_account_id)
        .bind(&request.remote_operation_id)
        .bind(&request.provider_request_id)
        .bind(&request.executor_owner)
        .bind(request.executor_lease_epoch)
        .bind(next_poll_at_ms)
        .bind(observation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        insert_observation(
            &mut tx,
            observation_id,
            request.submission_id,
            request.executor_execution_id,
            &request.event_identity,
            "submit_attach",
            "provider_waiting",
            None,
            None,
            "not_applicable",
            Some(next_poll_at_ms),
            None,
            None,
            &payload_hash,
            now,
        )
        .await?;
        require_one(
            sqlx::query(
                r#"
                UPDATE executor_executions
                SET state = 'provider_waiting', executor_owner = NULL,
                    lease_expires_at_ms = NULL, updated_at_ms = $5
                WHERE executor_execution_id = $1 AND submission_id = $2
                  AND state = 'running' AND executor_owner = $3 AND lease_epoch = $4
                "#,
            )
            .bind(request.executor_execution_id)
            .bind(request.submission_id)
            .bind(&request.executor_owner)
            .bind(request.executor_lease_epoch)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?,
            ProviderTaskStoreError::Conflict,
        )?;
        require_one(
            sqlx::query(
                "UPDATE provider_submissions SET state = 'provider_waiting', updated_at_ms = $2 WHERE submission_id = $1 AND state = 'running'",
            )
            .bind(request.submission_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?,
            ProviderTaskStoreError::Conflict,
        )?;
        heartbeat_capacity(
            &mut tx,
            request.executor_execution_id,
            request.submission_id,
            now,
        )
        .await?;
        let task = task_from_row(load_task_in(&mut tx, request.submission_id).await?.unwrap())?;
        tx.commit().await.map_err(unavailable)?;
        Ok(task)
    }

    async fn load(
        &self,
        submission_id: Uuid,
    ) -> Result<Option<ProviderRemoteTask>, ProviderTaskStoreError> {
        if submission_id.is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        load_task(&self.pool, submission_id)
            .await?
            .map(task_from_row)
            .transpose()
    }

    async fn claim_due(
        &self,
        scope: &ProviderTaskClaimScope,
        owner: &str,
        lease_ms: i64,
    ) -> Result<Option<ProviderTaskLease>, ProviderTaskStoreError> {
        validate_scope(scope)?;
        validate_owner_and_lease(owner, lease_ms)?;
        let row: Option<ClaimRow> = sqlx::query_as(
            r#"
            WITH db_clock AS (
              SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
            ), candidate AS (
              SELECT task.submission_id, clock.now_ms
              FROM provider_remote_tasks task
              CROSS JOIN db_clock clock
              JOIN executor_capacity_allocations allocation
                ON allocation.executor_execution_id = task.executor_execution_id
               AND allocation.submission_id = task.submission_id
               AND allocation.state = 'held'
              WHERE task.provider_id = $1 AND task.provider_account_id = $2
                AND task.state = 'provider_waiting'
                AND task.next_poll_at_ms <= clock.now_ms
                AND (task.poll_owner IS NULL OR task.poll_lease_expires_at_ms <= clock.now_ms)
              ORDER BY task.next_poll_at_ms, task.submission_id
              FOR UPDATE OF task SKIP LOCKED
              LIMIT 1
            ), claimed AS (
              UPDATE provider_remote_tasks task
              SET poll_owner = $3, poll_lease_epoch = task.poll_lease_epoch + 1,
                  poll_lease_expires_at_ms = candidate.now_ms + $4,
                  poll_claimed_at_ms = candidate.now_ms, updated_at_ms = candidate.now_ms
              FROM candidate
              WHERE task.submission_id = candidate.submission_id
              RETURNING task.*
            )
            SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
                   remote_operation_id, provider_request_id, state, artifact_ref, error_code,
                   next_poll_at_ms, cancel_requested, poll_lease_epoch,
                   state_observation_id, poll_owner, poll_lease_expires_at_ms
            FROM claimed
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .bind(owner)
        .bind(lease_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(lease_from_row).transpose()
    }

    async fn heartbeat(
        &self,
        lease: &ProviderTaskLease,
        lease_ms: i64,
    ) -> Result<ProviderTaskLease, ProviderTaskStoreError> {
        validate_lease(lease, lease_ms)?;
        let expires: Option<i64> = sqlx::query_scalar(
            r#"
            UPDATE provider_remote_tasks
            SET poll_lease_expires_at_ms = GREATEST(
                  poll_lease_expires_at_ms,
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT + $5
                ),
                updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND poll_owner = $3 AND poll_lease_epoch = $4
              AND state = 'provider_waiting'
              AND poll_lease_expires_at_ms >
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            RETURNING poll_lease_expires_at_ms
            "#,
        )
        .bind(lease.task.submission_id)
        .bind(lease.task.executor_execution_id)
        .bind(&lease.poll_owner)
        .bind(lease.poll_lease_epoch)
        .bind(lease_ms)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(ProviderTaskLease {
            poll_lease_expires_at_ms: expires.ok_or(ProviderTaskStoreError::StaleLease)?,
            ..lease.clone()
        })
    }

    async fn request_cancel(
        &self,
        submission_id: Uuid,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        if submission_id.is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let row: Option<TaskRow> = sqlx::query_as(
            r#"
            UPDATE provider_remote_tasks
            SET cancel_requested = TRUE,
                cancel_requested_at_ms =
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT,
                next_poll_at_ms = LEAST(
                  next_poll_at_ms,
                  floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
                ),
                updated_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            WHERE submission_id = $1 AND state = 'provider_waiting'
              AND cancel_requested = FALSE
            RETURNING submission_id, executor_execution_id, provider_id,
                      provider_account_id, remote_operation_id, provider_request_id,
                      state, artifact_ref, error_code, next_poll_at_ms,
                      cancel_requested, poll_lease_epoch, state_observation_id
            "#,
        )
        .bind(submission_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        if let Some(row) = row {
            return task_from_row(row);
        }
        self.load(submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)
    }

    async fn record_observation(
        &self,
        lease: &ProviderTaskLease,
        observation: &ProviderTaskObservation,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        validate_lease(lease, 1)?;
        validate_observation(observation)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let now = database_now(&mut tx).await?;
        let mut values = observation_values(observation, now);
        let payload_hash = observation_hash(
            values.source,
            values.state,
            values.artifact_ref,
            values.error_code,
            values.effect_certainty,
            values.next_poll_at_ms,
            Some(&lease.poll_owner),
            Some(lease.poll_lease_epoch),
        );
        let persisted = insert_or_load_observation(
            &mut tx,
            &NewObservation {
                submission_id: lease.task.submission_id,
                executor_execution_id: lease.task.executor_execution_id,
                event_identity: &observation.event_identity,
                source: values.source,
                state: values.state,
                artifact_ref: values.artifact_ref,
                error_code: values.error_code,
                effect_certainty: values.effect_certainty,
                next_poll_at_ms: values.next_poll_at_ms,
                poll_owner: Some(&lease.poll_owner),
                poll_lease_epoch: Some(lease.poll_lease_epoch),
                payload_hash: &payload_hash,
                now,
            },
        )
        .await?;
        values.next_poll_at_ms = persisted.next_poll_at_ms;
        let observation_id = persisted.observation_id;
        let changed = sqlx::query(
            r#"
            UPDATE provider_remote_tasks
            SET state = $5, artifact_ref = $6, error_code = $7,
                effect_certainty = $8, next_poll_at_ms = $9,
                poll_owner = NULL, poll_lease_expires_at_ms = NULL,
                poll_claimed_at_ms = NULL, state_observation_id = $10,
                updated_at_ms = $11,
                terminal_at_ms = CASE WHEN $5 = 'provider_waiting' THEN NULL ELSE $11 END
            WHERE submission_id = $1 AND executor_execution_id = $2
              AND poll_owner = $3 AND poll_lease_epoch = $4
              AND state = 'provider_waiting' AND poll_lease_expires_at_ms > $11
            "#,
        )
        .bind(lease.task.submission_id)
        .bind(lease.task.executor_execution_id)
        .bind(&lease.poll_owner)
        .bind(lease.poll_lease_epoch)
        .bind(values.state)
        .bind(values.artifact_ref)
        .bind(values.error_code)
        .bind(values.effect_certainty)
        .bind(values.next_poll_at_ms)
        .bind(observation_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .rows_affected();
        if changed != 1 {
            let existing = load_task_in(&mut tx, lease.task.submission_id).await?;
            if existing.as_ref().is_some_and(|row| {
                row.state == values.state
                    && row.artifact_ref.as_deref() == values.artifact_ref
                    && row.error_code.as_deref() == values.error_code
            }) {
                let task = task_from_row(existing.unwrap())?;
                tx.commit().await.map_err(unavailable)?;
                return Ok(task);
            }
            return Err(ProviderTaskStoreError::StaleLease);
        }
        let row = load_task_in(&mut tx, lease.task.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if matches!(row.state.as_str(), "failed" | "uncertain" | "canceled") {
            resolve_remote_terminal(&mut tx, &row, None, now).await?;
        } else {
            heartbeat_capacity(
                &mut tx,
                lease.task.executor_execution_id,
                lease.task.submission_id,
                now,
            )
            .await?;
        }
        let task = task_from_row(row)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(task)
    }

    async fn resolve_artifact(
        &self,
        submission_id: Uuid,
        manifest: &ExecutorResultManifest,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        if submission_id.is_nil() || manifest.manifest_id().is_nil() {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let row = load_task_in(&mut tx, submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if row.state != "artifact_ready" {
            return Err(ProviderTaskStoreError::Conflict);
        }
        let now = database_now(&mut tx).await?;
        resolve_remote_terminal(&mut tx, &row, Some(manifest), now).await?;
        let task = task_from_row(row)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(task)
    }

    async fn publish_artifact_authority(
        &self,
        lease: &ProviderTaskLease,
        authority: &ProviderArtifactAuthority,
    ) -> Result<ExecutorResultManifest, ProviderTaskStoreError> {
        validate_lease(lease, 1)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let task = load_task_in(&mut tx, lease.task.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        let now = database_now(&mut tx).await?;
        let live_poll_fence: Option<(Uuid, Uuid)> = sqlx::query_as(
            r#"
            SELECT submission.output_id, submission.job_id
            FROM provider_remote_tasks task
            JOIN provider_submissions submission
              ON submission.submission_id = task.submission_id
             AND submission.executor_execution_id = task.executor_execution_id
            WHERE task.submission_id = $1
              AND task.executor_execution_id = $2
              AND task.state = 'provider_waiting'
              AND task.poll_owner = $3 AND task.poll_lease_epoch = $4
              AND task.poll_lease_expires_at_ms > $5
            "#,
        )
        .bind(lease.task.submission_id)
        .bind(lease.task.executor_execution_id)
        .bind(&lease.poll_owner)
        .bind(lease.poll_lease_epoch)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let (output_id, job_id) = live_poll_fence.ok_or(ProviderTaskStoreError::StaleLease)?;
        let manifest = ExecutorResultManifest::new(task.submission_id, task.executor_execution_id)
            .ok_or(ProviderTaskStoreError::InvalidInput)?;
        if authority.object_key != executor_object_key(manifest.artifact_authority_id()) {
            return Err(ProviderTaskStoreError::Conflict);
        }

        let inserted = sqlx::query(
            r#"
            INSERT INTO executor_artifact_authorities
              (authority_id, executor_execution_id, submission_id, output_id, job_id,
               storage_backend, storage_namespace, object_key, sha256_hex, byte_size,
               media_type, created_at_ms)
            VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (authority_id) DO NOTHING
            "#,
        )
        .bind(manifest.artifact_authority_id())
        .bind(task.submission_id)
        .bind(output_id)
        .bind(job_id)
        .bind(&authority.storage_backend)
        .bind(&authority.storage_namespace)
        .bind(&authority.object_key)
        .bind(&authority.sha256_hex)
        .bind(i64::try_from(authority.byte_size).map_err(|_| ProviderTaskStoreError::InvalidInput)?)
        .bind(&authority.media_type)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?
        .rows_affected();
        if inserted == 0 {
            let matches: Option<bool> = sqlx::query_scalar(
                r#"
                SELECT storage_backend = $2
                   AND storage_namespace = $3
                   AND object_key = $4
                   AND sha256_hex = $5
                   AND byte_size = $6
                   AND media_type = $7
                   AND executor_execution_id = $1
                   AND submission_id = $8
                FROM executor_artifact_authorities
                WHERE authority_id = $1
                "#,
            )
            .bind(manifest.artifact_authority_id())
            .bind(&authority.storage_backend)
            .bind(&authority.storage_namespace)
            .bind(&authority.object_key)
            .bind(&authority.sha256_hex)
            .bind(
                i64::try_from(authority.byte_size)
                    .map_err(|_| ProviderTaskStoreError::InvalidInput)?,
            )
            .bind(&authority.media_type)
            .bind(task.submission_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
            if matches != Some(true) {
                return Err(ProviderTaskStoreError::Conflict);
            }
        }
        sqlx::query(
            r#"
            INSERT INTO executor_result_manifests
              (manifest_id, artifact_authority_id, executor_execution_id,
               submission_id, created_at_ms)
            VALUES ($1, $2, $2, $1, $3)
            ON CONFLICT (manifest_id) DO NOTHING
            "#,
        )
        .bind(manifest.manifest_id())
        .bind(manifest.artifact_authority_id())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(manifest)
    }

    async fn record_verified_callback(
        &self,
        callback: &VerifiedCallbackWakeup,
    ) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
        if callback.submission_id.is_nil() || !valid_identifier(&callback.event_identity, 255) {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let task = load_task_in(&mut tx, callback.submission_id)
            .await?
            .ok_or(ProviderTaskStoreError::NotFound)?;
        if task.state != "provider_waiting" {
            let task = task_from_row(task)?;
            tx.commit().await.map_err(unavailable)?;
            return Ok(task);
        }
        let now = database_now(&mut tx).await?;
        let payload_hash = observation_hash(
            "verified_callback",
            "provider_waiting",
            None,
            None,
            "not_applicable",
            Some(now),
            None,
            None,
        );
        let persisted = insert_or_load_observation(
            &mut tx,
            &NewObservation {
                submission_id: task.submission_id,
                executor_execution_id: task.executor_execution_id,
                event_identity: &callback.event_identity,
                source: "verified_callback",
                state: "provider_waiting",
                artifact_ref: None,
                error_code: None,
                effect_certainty: "not_applicable",
                next_poll_at_ms: Some(now),
                poll_owner: None,
                poll_lease_epoch: None,
                payload_hash: &payload_hash,
                now,
            },
        )
        .await?;
        let observation_id = persisted.observation_id;
        let callback_poll_at: i64 = sqlx::query_scalar(
            "SELECT next_poll_at_ms FROM provider_task_observations WHERE observation_id = $1",
        )
        .bind(observation_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            r#"
            UPDATE provider_remote_tasks
            SET next_poll_at_ms = LEAST(next_poll_at_ms, $3),
                last_wakeup_observation_id = $2, updated_at_ms = $4
            WHERE submission_id = $1
              AND last_wakeup_observation_id IS DISTINCT FROM $2
            "#,
        )
        .bind(callback.submission_id)
        .bind(observation_id)
        .bind(callback_poll_at)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(storage_conflict)?;
        heartbeat_capacity(&mut tx, task.executor_execution_id, task.submission_id, now).await?;
        let task = task_from_row(
            load_task_in(&mut tx, callback.submission_id)
                .await?
                .unwrap(),
        )?;
        tx.commit().await.map_err(unavailable)?;
        Ok(task)
    }
}

struct ObservationValues<'a> {
    source: &'static str,
    state: &'static str,
    artifact_ref: Option<&'a str>,
    error_code: Option<&'a str>,
    effect_certainty: &'static str,
    next_poll_at_ms: Option<i64>,
}

async fn resolve_submit_terminal(
    tx: &mut Transaction<'_, Postgres>,
    intent: &ProviderSubmitIntent,
    resolved_state: &str,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    if resolved_state != "failed" || intent.state != ProviderSubmitIntentState::Rejected {
        return Err(ProviderTaskStoreError::Conflict);
    }
    let error_code = intent
        .failure_error_code
        .as_deref()
        .ok_or(ProviderTaskStoreError::Conflict)?;

    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, provider_submit_intent_id,
           resolved_state, result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_submit_outcome',
                NULL, NULL, $2, $3, NULL, $4, $5)
        "#,
    )
    .bind(intent.executor_execution_id)
    .bind(intent.submission_id)
    .bind(resolved_state)
    .bind(error_code)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $3, executor_owner = NULL, lease_expires_at_ms = NULL,
                resolution_decision_id = $1, finished_at_ms = $4,
                updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'running'
            "#,
        )
        .bind(intent.executor_execution_id)
        .bind(intent.submission_id)
        .bind(resolved_state)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $3, resolution_decision_id = $1,
                finished_at_ms = $4, updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'running'
            "#,
        )
        .bind(intent.executor_execution_id)
        .bind(intent.submission_id)
        .bind(resolved_state)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    release_capacity_allocation(
        tx,
        intent.executor_execution_id,
        intent.submission_id,
        resolved_state,
        "remote_submit_outcome",
        now,
    )
    .await
    .map_err(map_executor_error)
}

async fn resolve_remote_terminal(
    tx: &mut Transaction<'_, Postgres>,
    task: &TaskRow,
    manifest: Option<&ExecutorResultManifest>,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    let (resolved_state, result_manifest_id, error_code) = match task.state.as_str() {
        "artifact_ready" => {
            let manifest = manifest.ok_or(ProviderTaskStoreError::Conflict)?;
            let authority_is_bound: Option<bool> = sqlx::query_scalar(
                r#"
                SELECT TRUE
                FROM executor_result_manifests manifest
                WHERE manifest.manifest_id = $1
                  AND manifest.artifact_authority_id = $2
                  AND manifest.executor_execution_id = $3
                  AND manifest.submission_id = $4
                "#,
            )
            .bind(manifest.manifest_id())
            .bind(manifest.artifact_authority_id())
            .bind(task.executor_execution_id)
            .bind(task.submission_id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unavailable)?;
            if authority_is_bound.is_none() {
                return Err(ProviderTaskStoreError::Conflict);
            }
            ("succeeded", Some(manifest.manifest_id()), None)
        }
        "failed" => (
            "failed",
            None,
            Some(
                task.error_code
                    .as_deref()
                    .ok_or(ProviderTaskStoreError::Conflict)?,
            ),
        ),
        "uncertain" => (
            "uncertain",
            None,
            Some(
                task.error_code
                    .as_deref()
                    .ok_or(ProviderTaskStoreError::Conflict)?,
            ),
        ),
        "canceled" => (
            "canceled",
            None,
            Some(
                task.error_code
                    .as_deref()
                    .ok_or(ProviderTaskStoreError::Conflict)?,
            ),
        ),
        _ => return Err(ProviderTaskStoreError::Conflict),
    };

    let existing: Option<ResolutionDecisionRow> = sqlx::query_as(
        r#"
            SELECT resolved_state, result_manifest_id, error_code,
                   provider_task_observation_id, provider_submit_intent_id
            FROM executor_resolution_decisions
            WHERE decision_id = $1
            "#,
    )
    .bind(task.executor_execution_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    if let Some(existing) = existing {
        return if existing.resolved_state == resolved_state
            && existing.result_manifest_id == result_manifest_id
            && existing.error_code.as_deref() == error_code
            && existing.provider_task_observation_id == Some(task.state_observation_id)
            && existing.provider_submit_intent_id.is_none()
        {
            Ok(())
        } else {
            Err(ProviderTaskStoreError::Conflict)
        };
    }

    sqlx::query(
        r#"
        INSERT INTO executor_resolution_decisions
          (decision_id, executor_execution_id, submission_id, source,
           observation_id, provider_task_observation_id, resolved_state,
           result_manifest_id, error_code, decided_at_ms)
        VALUES ($1, $1, $2, 'remote_provider_observation',
                NULL, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(task.executor_execution_id)
    .bind(task.submission_id)
    .bind(task.state_observation_id)
    .bind(resolved_state)
    .bind(result_manifest_id)
    .bind(error_code)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;

    require_one(
        sqlx::query(
            r#"
            UPDATE executor_executions
            SET state = $3, resolution_decision_id = $1,
                finished_at_ms = $4, updated_at_ms = $4, error_code = $5
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
              AND executor_owner IS NULL AND lease_expires_at_ms IS NULL
            "#,
        )
        .bind(task.executor_execution_id)
        .bind(task.submission_id)
        .bind(resolved_state)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    require_one(
        sqlx::query(
            r#"
            UPDATE provider_submissions
            SET state = $3, result_manifest_id = $4,
                resolution_decision_id = $1, finished_at_ms = $5,
                updated_at_ms = $5, error_code = $6
            WHERE executor_execution_id = $1 AND submission_id = $2
              AND state = 'provider_waiting'
            "#,
        )
        .bind(task.executor_execution_id)
        .bind(task.submission_id)
        .bind(resolved_state)
        .bind(result_manifest_id)
        .bind(now)
        .bind(error_code)
        .execute(&mut **tx)
        .await
        .map_err(storage_conflict)?,
        ProviderTaskStoreError::Conflict,
    )?;
    release_capacity_allocation(
        tx,
        task.executor_execution_id,
        task.submission_id,
        resolved_state,
        "remote_provider_observation",
        now,
    )
    .await
    .map_err(map_executor_error)
}

fn observation_values<'a>(
    observation: &'a ProviderTaskObservation,
    now: i64,
) -> ObservationValues<'a> {
    let source = match observation.source {
        ProviderTaskObservationSource::Poll => "poll",
        ProviderTaskObservationSource::Cancel => "cancel",
    };
    match &observation.outcome {
        ProviderTaskObservationOutcome::Waiting { poll_after_ms } => ObservationValues {
            source,
            state: "provider_waiting",
            artifact_ref: None,
            error_code: None,
            effect_certainty: "not_applicable",
            next_poll_at_ms: Some(now + poll_after_ms),
        },
        ProviderTaskObservationOutcome::ArtifactReady { artifact_ref } => ObservationValues {
            source,
            state: "artifact_ready",
            artifact_ref: Some(artifact_ref),
            error_code: None,
            effect_certainty: "not_applicable",
            next_poll_at_ms: None,
        },
        ProviderTaskObservationOutcome::Failed { error_code } => ObservationValues {
            source,
            state: "failed",
            artifact_ref: None,
            error_code: Some(error_code),
            effect_certainty: "not_applicable",
            next_poll_at_ms: None,
        },
        ProviderTaskObservationOutcome::Canceled { error_code } => ObservationValues {
            source,
            state: "canceled",
            artifact_ref: None,
            error_code: Some(error_code),
            effect_certainty: "confirmed_no_effect",
            next_poll_at_ms: None,
        },
        ProviderTaskObservationOutcome::Uncertain { error_code } => ObservationValues {
            source,
            state: "uncertain",
            artifact_ref: None,
            error_code: Some(error_code),
            effect_certainty: "unknown_remote_effect",
            next_poll_at_ms: None,
        },
    }
}

struct PersistedObservation {
    observation_id: Uuid,
    next_poll_at_ms: Option<i64>,
}

struct NewObservation<'a> {
    submission_id: Uuid,
    executor_execution_id: Uuid,
    event_identity: &'a str,
    source: &'a str,
    state: &'a str,
    artifact_ref: Option<&'a str>,
    error_code: Option<&'a str>,
    effect_certainty: &'a str,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<&'a str>,
    poll_lease_epoch: Option<i64>,
    payload_hash: &'a str,
    now: i64,
}

async fn insert_or_load_observation(
    tx: &mut Transaction<'_, Postgres>,
    observation: &NewObservation<'_>,
) -> Result<PersistedObservation, ProviderTaskStoreError> {
    let observation_id = Uuid::new_v4();
    let inserted = sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id, event_identity,
           source, observed_state, artifact_ref, error_code, effect_certainty,
           next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (submission_id, event_identity) DO NOTHING
        "#,
    )
    .bind(observation_id)
    .bind(observation.submission_id)
    .bind(observation.executor_execution_id)
    .bind(observation.event_identity)
    .bind(observation.source)
    .bind(observation.state)
    .bind(observation.artifact_ref)
    .bind(observation.error_code)
    .bind(observation.effect_certainty)
    .bind(observation.next_poll_at_ms)
    .bind(observation.poll_owner)
    .bind(observation.poll_lease_epoch)
    .bind(observation.payload_hash)
    .bind(observation.now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?
    .rows_affected();
    if inserted == 1 {
        return Ok(PersistedObservation {
            observation_id,
            next_poll_at_ms: observation.next_poll_at_ms,
        });
    }
    let existing: ExistingObservation = sqlx::query_as(
        r#"
        SELECT observation_id, source, observed_state, artifact_ref, error_code,
               effect_certainty, next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash
        FROM provider_task_observations
        WHERE submission_id = $1 AND event_identity = $2
        "#,
    )
    .bind(observation.submission_id)
    .bind(observation.event_identity)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    let stable_payload_matches = existing.source == observation.source
        && existing.observed_state == observation.state
        && existing.artifact_ref.as_deref() == observation.artifact_ref
        && existing.error_code.as_deref() == observation.error_code
        && existing.effect_certainty == observation.effect_certainty
        && existing.poll_owner.as_deref() == observation.poll_owner
        && existing.poll_lease_epoch == observation.poll_lease_epoch;
    let time_dependent_replay = observation.source == "verified_callback"
        || (observation.state == "provider_waiting" && stable_payload_matches);
    let exact_replay = existing.next_poll_at_ms == observation.next_poll_at_ms
        && existing.payload_hash == observation.payload_hash;
    if stable_payload_matches && (time_dependent_replay || exact_replay) {
        Ok(PersistedObservation {
            observation_id: existing.observation_id,
            next_poll_at_ms: existing.next_poll_at_ms,
        })
    } else {
        Err(ProviderTaskStoreError::Conflict)
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_observation(
    tx: &mut Transaction<'_, Postgres>,
    observation_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    event_identity: &str,
    source: &str,
    state: &str,
    artifact_ref: Option<&str>,
    error_code: Option<&str>,
    effect_certainty: &str,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<&str>,
    poll_lease_epoch: Option<i64>,
    payload_hash: &str,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    sqlx::query(
        r#"
        INSERT INTO provider_task_observations
          (observation_id, submission_id, executor_execution_id, event_identity,
           source, observed_state, artifact_ref, error_code, effect_certainty,
           next_poll_at_ms, poll_owner, poll_lease_epoch, payload_hash, observed_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        "#,
    )
    .bind(observation_id)
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(event_identity)
    .bind(source)
    .bind(state)
    .bind(artifact_ref)
    .bind(error_code)
    .bind(effect_certainty)
    .bind(next_poll_at_ms)
    .bind(poll_owner)
    .bind(poll_lease_epoch)
    .bind(payload_hash)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_err(storage_conflict)?;
    Ok(())
}

async fn load_task(
    pool: &PgPool,
    submission_id: Uuid,
) -> Result<Option<TaskRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, state, artifact_ref, error_code,
               next_poll_at_ms, cancel_requested, poll_lease_epoch, state_observation_id
        FROM provider_remote_tasks
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)
}

async fn load_submit_intent_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<SubmitIntentRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key, state,
               remote_operation_id, provider_request_id, send_started_at_ms,
               receipt_event_identity, failure_event_identity,
               failure_error_code, updated_at_ms
        FROM provider_remote_submit_intents
        WHERE submission_id = $1
        FOR UPDATE
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

async fn load_submit_intent(
    pool: &PgPool,
    submission_id: Uuid,
) -> Result<Option<SubmitIntentRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               submit_owner, submit_lease_epoch, idempotency_key, state,
               remote_operation_id, provider_request_id, send_started_at_ms,
               receipt_event_identity, failure_event_identity,
               failure_error_code, updated_at_ms
        FROM provider_remote_submit_intents
        WHERE submission_id = $1
        "#,
    )
    .bind(submission_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)
}

async fn lock_submit_parent(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
) -> Result<SubmitParentRow, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission.provider_id, submission.provider_account_id,
               execution.state AS execution_state,
               submission.state AS submission_state,
               execution.executor_owner, execution.lease_epoch,
               execution.launch_owner, execution.launch_lease_epoch,
               allocation.state AS allocation_state
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        JOIN executor_capacity_allocations allocation
          ON allocation.executor_execution_id = execution.executor_execution_id
         AND allocation.submission_id = execution.submission_id
        WHERE execution.executor_execution_id = $1
          AND execution.submission_id = $2
        FOR UPDATE OF execution, submission, allocation
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(ProviderTaskStoreError::NotFound)
}

fn submit_parent_accepts_evidence(parent: &SubmitParentRow, intent: &SubmitIntentRow) -> bool {
    parent.execution_state == "running"
        && parent.submission_state == "running"
        && parent.executor_owner.as_deref() == Some(intent.submit_owner.as_str())
        && parent.lease_epoch == intent.submit_lease_epoch
        && parent.launch_owner.as_deref() == Some(intent.submit_owner.as_str())
        && parent.launch_lease_epoch == Some(intent.submit_lease_epoch)
        && parent.allocation_state == "held"
}

async fn lock_live_submit_fence(
    tx: &mut Transaction<'_, Postgres>,
    request: &RemoteTaskSubmitReservation,
) -> Result<bool, ProviderTaskStoreError> {
    let row: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        JOIN executor_capacity_allocations allocation
          ON allocation.executor_execution_id = execution.executor_execution_id
         AND allocation.submission_id = execution.submission_id
        WHERE execution.executor_execution_id = $1
          AND execution.submission_id = $2
          AND execution.state = 'running' AND submission.state = 'running'
          AND execution.executor_owner = $3 AND execution.lease_epoch = $4
          AND execution.launch_owner = $3 AND execution.launch_lease_epoch = $4
          AND execution.lease_expires_at_ms >
              floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
          AND allocation.state = 'held'
        FOR UPDATE OF execution, submission, allocation
        "#,
    )
    .bind(request.executor_execution_id)
    .bind(request.submission_id)
    .bind(&request.executor_owner)
    .bind(request.executor_lease_epoch)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(row.unwrap_or(false))
}

fn submit_intent_matches_reservation(
    row: &SubmitIntentRow,
    request: &RemoteTaskSubmitReservation,
) -> bool {
    row.executor_execution_id == request.executor_execution_id
        && row.submit_owner == request.executor_owner
        && row.submit_lease_epoch == request.executor_lease_epoch
        && row.idempotency_key == request.idempotency_key
}

fn submit_intent_from_row(
    row: SubmitIntentRow,
) -> Result<ProviderSubmitIntent, ProviderTaskStoreError> {
    let state = match row.state.as_str() {
        "reserved" => ProviderSubmitIntentState::Reserved,
        "sending" => ProviderSubmitIntentState::Sending,
        "outcome_unknown" => ProviderSubmitIntentState::OutcomeUnknown,
        "operation_known" => ProviderSubmitIntentState::OperationKnown,
        "attached" => ProviderSubmitIntentState::Attached,
        "rejected" => ProviderSubmitIntentState::Rejected,
        _ => return Err(ProviderTaskStoreError::Conflict),
    };
    Ok(ProviderSubmitIntent {
        submission_id: row.submission_id,
        executor_execution_id: row.executor_execution_id,
        provider_id: row.provider_id,
        provider_account_id: row.provider_account_id,
        submit_owner: row.submit_owner,
        submit_lease_epoch: row.submit_lease_epoch,
        idempotency_key: row.idempotency_key,
        state,
        remote_operation_id: row.remote_operation_id,
        provider_request_id: row.provider_request_id,
        send_started_at_ms: row.send_started_at_ms,
        receipt_event_identity: row.receipt_event_identity,
        failure_event_identity: row.failure_event_identity,
        failure_error_code: row.failure_error_code,
        updated_at_ms: row.updated_at_ms,
    })
}

async fn load_task_in(
    tx: &mut Transaction<'_, Postgres>,
    submission_id: Uuid,
) -> Result<Option<TaskRow>, ProviderTaskStoreError> {
    sqlx::query_as(
        r#"
        SELECT submission_id, executor_execution_id, provider_id, provider_account_id,
               remote_operation_id, provider_request_id, state, artifact_ref, error_code,
               next_poll_at_ms, cancel_requested, poll_lease_epoch, state_observation_id
        FROM provider_remote_tasks
        WHERE submission_id = $1
        FOR UPDATE
        "#,
    )
    .bind(submission_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)
}

fn task_from_row(row: TaskRow) -> Result<ProviderRemoteTask, ProviderTaskStoreError> {
    Ok(ProviderRemoteTask {
        submission_id: row.submission_id,
        executor_execution_id: row.executor_execution_id,
        provider_id: row.provider_id,
        provider_account_id: row.provider_account_id,
        remote_operation_id: row.remote_operation_id,
        provider_request_id: row.provider_request_id,
        state: parse_state(&row.state)?,
        artifact_ref: row.artifact_ref,
        error_code: row.error_code,
        next_poll_at_ms: row.next_poll_at_ms,
        cancel_requested: row.cancel_requested,
        poll_lease_epoch: row.poll_lease_epoch,
    })
}

fn lease_from_row(row: ClaimRow) -> Result<ProviderTaskLease, ProviderTaskStoreError> {
    let poll_owner = row.poll_owner.clone();
    let poll_lease_epoch = row.poll_lease_epoch;
    let poll_lease_expires_at_ms = row.poll_lease_expires_at_ms;
    Ok(ProviderTaskLease {
        task: task_from_row(TaskRow {
            submission_id: row.submission_id,
            executor_execution_id: row.executor_execution_id,
            provider_id: row.provider_id,
            provider_account_id: row.provider_account_id,
            remote_operation_id: row.remote_operation_id,
            provider_request_id: row.provider_request_id,
            state: row.state,
            artifact_ref: row.artifact_ref,
            error_code: row.error_code,
            next_poll_at_ms: row.next_poll_at_ms,
            cancel_requested: row.cancel_requested,
            poll_lease_epoch,
            state_observation_id: row.state_observation_id,
        })?,
        poll_owner,
        poll_lease_epoch,
        poll_lease_expires_at_ms,
    })
}

fn parse_state(value: &str) -> Result<ProviderTaskState, ProviderTaskStoreError> {
    match value {
        "provider_waiting" => Ok(ProviderTaskState::ProviderWaiting),
        "artifact_ready" => Ok(ProviderTaskState::ArtifactReady),
        "failed" => Ok(ProviderTaskState::Failed),
        "canceled" => Ok(ProviderTaskState::Canceled),
        "uncertain" => Ok(ProviderTaskState::Uncertain),
        _ => Err(ProviderTaskStoreError::Conflict),
    }
}

fn validate_attach(value: &RemoteTaskAttach) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.remote_operation_id, 255)
        || value
            .provider_request_id
            .as_deref()
            .is_some_and(|id| !valid_identifier(id, 255))
        || !valid_identifier(&value.event_identity, 255)
        || !(0..=MAX_POLL_AFTER_MS).contains(&value.poll_after_ms)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_reservation(value: &RemoteTaskSubmitReservation) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.idempotency_key, 255)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_submit_failure(value: &RemoteTaskSubmitFailure) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.event_identity, 255)
        || !valid_simple_identifier(&value.error_code, 128)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_submit_receipt(value: &RemoteTaskSubmitReceipt) -> Result<(), ProviderTaskStoreError> {
    if value.submission_id.is_nil()
        || value.executor_execution_id.is_nil()
        || value.submission_id == value.executor_execution_id
        || value.executor_lease_epoch <= 0
        || !valid_owner(&value.executor_owner)
        || !valid_identifier(&value.remote_operation_id, 255)
        || value
            .provider_request_id
            .as_deref()
            .is_some_and(|id| !valid_identifier(id, 255))
        || !valid_identifier(&value.event_identity, 255)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_scope(value: &ProviderTaskClaimScope) -> Result<(), ProviderTaskStoreError> {
    if value.provider_account_id.is_nil() || !valid_simple_identifier(&value.provider_id, 128) {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_owner_and_lease(owner: &str, lease_ms: i64) -> Result<(), ProviderTaskStoreError> {
    if valid_owner(owner) && (1..=MAX_LEASE_MS).contains(&lease_ms) {
        Ok(())
    } else {
        Err(ProviderTaskStoreError::InvalidInput)
    }
}

fn validate_lease(lease: &ProviderTaskLease, lease_ms: i64) -> Result<(), ProviderTaskStoreError> {
    if lease.task.submission_id.is_nil()
        || lease.task.executor_execution_id.is_nil()
        || lease.poll_lease_epoch <= 0
        || !valid_owner(&lease.poll_owner)
        || !(1..=MAX_LEASE_MS).contains(&lease_ms)
    {
        Err(ProviderTaskStoreError::InvalidInput)
    } else {
        Ok(())
    }
}

fn validate_observation(value: &ProviderTaskObservation) -> Result<(), ProviderTaskStoreError> {
    if !valid_identifier(&value.event_identity, 255) {
        return Err(ProviderTaskStoreError::InvalidInput);
    }
    let valid = match &value.outcome {
        ProviderTaskObservationOutcome::Waiting { poll_after_ms } => {
            (0..=MAX_POLL_AFTER_MS).contains(poll_after_ms)
        }
        ProviderTaskObservationOutcome::ArtifactReady { artifact_ref } => {
            valid_identifier(artifact_ref, 512)
        }
        ProviderTaskObservationOutcome::Failed { error_code }
        | ProviderTaskObservationOutcome::Canceled { error_code }
        | ProviderTaskObservationOutcome::Uncertain { error_code } => {
            valid_simple_identifier(error_code, 128)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ProviderTaskStoreError::InvalidInput)
    }
}

fn valid_owner(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value.contains("://")
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphanumeric()
                || (index > 0 && matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-'))
        })
}

fn valid_simple_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[allow(clippy::too_many_arguments)]
fn observation_hash(
    source: &str,
    state: &str,
    artifact_ref: Option<&str>,
    error_code: Option<&str>,
    effect_certainty: &str,
    next_poll_at_ms: Option<i64>,
    poll_owner: Option<&str>,
    poll_lease_epoch: Option<i64>,
) -> String {
    let mut hash = Sha256::new();
    for value in [
        source,
        state,
        artifact_ref.unwrap_or(""),
        error_code.unwrap_or(""),
        effect_certainty,
        poll_owner.unwrap_or(""),
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(next_poll_at_ms.unwrap_or(-1).to_be_bytes());
    hash.update(poll_lease_epoch.unwrap_or(-1).to_be_bytes());
    hex::encode(hash.finalize())
}

async fn database_now(tx: &mut Transaction<'_, Postgres>) -> Result<i64, ProviderTaskStoreError> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
        .map_err(unavailable)
}

async fn heartbeat_capacity(
    tx: &mut Transaction<'_, Postgres>,
    executor_execution_id: Uuid,
    submission_id: Uuid,
    now: i64,
) -> Result<(), ProviderTaskStoreError> {
    require_one(
        sqlx::query(
            r#"
            UPDATE executor_capacity_allocations
            SET last_heartbeat_at_ms = GREATEST(last_heartbeat_at_ms, $3)
            WHERE executor_execution_id = $1 AND submission_id = $2 AND state = 'held'
            "#,
        )
        .bind(executor_execution_id)
        .bind(submission_id)
        .bind(now)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?,
        ProviderTaskStoreError::Conflict,
    )
}

fn require_one(
    result: sqlx::postgres::PgQueryResult,
    error: ProviderTaskStoreError,
) -> Result<(), ProviderTaskStoreError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(error)
    }
}

fn unavailable(_: sqlx::Error) -> ProviderTaskStoreError {
    ProviderTaskStoreError::Unavailable
}

fn storage_conflict(error: sqlx::Error) -> ProviderTaskStoreError {
    match error.as_database_error().and_then(|error| error.code()) {
        Some(code) if matches!(code.as_ref(), "23503" | "23505" | "23514" | "P0001") => {
            ProviderTaskStoreError::Conflict
        }
        _ => ProviderTaskStoreError::Unavailable,
    }
}

fn map_executor_error(error: ExecutorSubmissionError) -> ProviderTaskStoreError {
    match error {
        ExecutorSubmissionError::Unavailable => ProviderTaskStoreError::Unavailable,
        ExecutorSubmissionError::InvalidInput => ProviderTaskStoreError::InvalidInput,
        ExecutorSubmissionError::Conflict | ExecutorSubmissionError::StaleLease => {
            ProviderTaskStoreError::Conflict
        }
    }
}
