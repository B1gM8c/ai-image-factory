use std::env;

use async_trait::async_trait;

use gpt_image_2_gateway::{
    GenerationJob, PostgresReconciliationStore, PostgresUsageStore, ReconciliationStore,
    UsageCharge, UsageLimits, UsageReservation, UsageStore,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionStore, AdmissionTicket, AttachJob,
        ClaimAdmission, GenerationCommandV1, PostgresAdmissionStore, WorkLease,
    },
    artifacts::InMemoryArtifactBlobStore,
    database::{connect_test_pool_with_search_path, run_migrations},
    input_blobs::{
        InputBlobDeleteError, InputBlobKey, InputBlobReadError, InputBlobRef, InputBlobStore,
        InputBlobWriteError,
    },
    reconcile_input_cleanup,
};
use serde_json::to_value;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn expired_claimed_work_requeues_but_expired_running_work_becomes_uncertain() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let admission = PostgresAdmissionStore::new(database.pool.clone());
        let claimed = ready_lease(&database.pool, &admission, "e").await?;
        expire_lease(&database.pool, &claimed).await?;

        let running = ready_lease(&database.pool, &admission, "f").await?;
        admission
            .start(&running)
            .await
            .map_err(|error| format!("start failed: {error}"))?;
        expire_lease(&database.pool, &running).await?;

        let reconciler = PostgresReconciliationStore::new(database.pool.clone());
        let outcome = reconciler
            .reconcile_expired_work(10)
            .await
            .map_err(|error| format!("reconciliation failed: {error:?}"))?;
        require(
            outcome.requeued == 1 && outcome.uncertain == 1,
            format!("unexpected reconciliation outcome: {outcome:?}"),
        )?;

        let replacement = admission
            .claim_job(claimed.job_id, "replacement-worker", 60_000)
            .await
            .map_err(|error| format!("replacement claim failed: {error}"))?
            .ok_or_else(|| "claimed-but-not-started work was not requeued".to_string())?;
        require(
            replacement.lease_epoch == claimed.lease_epoch + 1
                && replacement.execution_id != claimed.execution_id,
            "replacement lease was not fenced",
        )?;
        require(
            admission
                .claim_job(running.job_id, "unsafe-retry", 60_000)
                .await
                .map_err(|error| format!("uncertain claim check failed: {error}"))?
                .is_none(),
            "expired running work was made retryable",
        )?;

        let states: (String, String, String, String) = sqlx::query_as(
            r#"
            SELECT w.state, a.state, i.state, qr.state
            FROM work_items w
            JOIN job_attempts a ON a.execution_id = w.execution_id
            JOIN idempotency_requests i ON i.job_id = w.job_id
            JOIN quota_reservations qr ON qr.job_id = w.job_id
            WHERE w.job_id = $1
            "#,
        )
        .bind(running.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("state query failed: {error}"))?;
        require(
            states
                == (
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "uncertain".to_string(),
                    "reserved".to_string(),
                ),
            format!("running expiry lost uncertainty/economic hold: {states:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_reconcilers_transition_an_expired_attempt_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let admission = PostgresAdmissionStore::new(database.pool.clone());
        let running = ready_lease(&database.pool, &admission, "g").await?;
        admission
            .start(&running)
            .await
            .map_err(|error| format!("start failed: {error}"))?;
        expire_lease(&database.pool, &running).await?;

        let left = PostgresReconciliationStore::new(database.pool.clone());
        let right = PostgresReconciliationStore::new(database.pool.clone());
        let (left, right) = tokio::join!(
            left.reconcile_expired_work(1),
            right.reconcile_expired_work(1)
        );
        let left = left.map_err(|error| format!("left reconciler failed: {error:?}"))?;
        let right = right.map_err(|error| format!("right reconciler failed: {error:?}"))?;
        require(
            left.uncertain + right.uncertain == 1 && left.requeued + right.requeued == 0,
            format!("concurrent reconcilers duplicated transition: {left:?} {right:?}"),
        )?;
        let event_counts: (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              (SELECT COUNT(*) FROM job_events
               WHERE job_id = $1 AND event_type = 'job.uncertain'),
              (SELECT COUNT(*) FROM outbox_events
               WHERE job_id = $1 AND event_type = 'job.uncertain')
            "#,
        )
        .bind(running.job_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("event count query failed: {error}"))?;
        require(
            event_counts == (1, 1),
            format!("uncertainty events were not exactly once: {event_counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn orphaned_reservation_is_released_and_terminalized_atomically() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let orphan = orphan_reservation(&database.pool, "orphan-atomic").await?;
        age_orphan(&database.pool, &orphan, 120_000).await?;

        let reconciler = PostgresReconciliationStore::new(database.pool.clone());
        let outcome = reconciler
            .reconcile_orphan_reservations(60_000, 10)
            .await
            .map_err(|error| format!("orphan reconciliation failed: {error:?}"))?;
        require(
            outcome.orphaned == 1,
            format!("orphan was not reconciled: {outcome:?}"),
        )?;
        let duplicate = reconciler
            .reconcile_orphan_reservations(60_000, 10)
            .await
            .map_err(|error| format!("duplicate reconciliation failed: {error:?}"))?;
        require(
            duplicate.orphaned == 0,
            format!("terminal orphan was reconciled twice: {duplicate:?}"),
        )?;

        assert_orphan_terminal_state(&database.pool, &orphan).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_reconcilers_release_an_orphan_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let orphan = orphan_reservation(&database.pool, "orphan-concurrent").await?;
        age_orphan(&database.pool, &orphan, 120_000).await?;

        let left = PostgresReconciliationStore::new(database.pool.clone());
        let right = PostgresReconciliationStore::new(database.pool.clone());
        let (left, right) = tokio::join!(
            left.reconcile_orphan_reservations(60_000, 1),
            right.reconcile_orphan_reservations(60_000, 1)
        );
        let left = left.map_err(|error| format!("left reconciler failed: {error:?}"))?;
        let right = right.map_err(|error| format!("right reconciler failed: {error:?}"))?;
        require(
            left.orphaned + right.orphaned == 1,
            format!("concurrent orphan transition was duplicated: {left:?} {right:?}"),
        )?;

        assert_orphan_terminal_state(&database.pool, &orphan).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn aborted_session_with_reserved_quota_finishes_orphan_recovery() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let orphan = orphan_reservation(&database.pool, "orphan-half-aborted").await?;
        age_orphan(&database.pool, &orphan, 120_000).await?;
        sqlx::query("UPDATE admission_sessions SET state = 'aborted' WHERE session_id = $1")
            .bind(orphan.ticket.session_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("half-aborted admission update failed: {error}"))?;
        sqlx::query("UPDATE idempotency_requests SET state = 'aborted' WHERE session_id = $1")
            .bind(orphan.ticket.session_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("half-aborted idempotency update failed: {error}"))?;

        let outcome = PostgresReconciliationStore::new(database.pool.clone())
            .reconcile_orphan_reservations(60_000, 1)
            .await
            .map_err(|error| format!("half-aborted orphan recovery failed: {error:?}"))?;
        require(
            outcome.orphaned == 1,
            format!("half-aborted orphan was not recovered: {outcome:?}"),
        )?;
        assert_orphan_terminal_state(&database.pool, &orphan).await
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn input_cleanup_deletes_expired_unattached_session_blobs_once() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let ticket = expired_edit_session(&database.pool, "input-expired").await?;
        let blobs = InMemoryArtifactBlobStore::default();
        let blob = blobs
            .put(
                InputBlobKey {
                    admission_session_id: ticket.session_id,
                    input_id: Uuid::new_v4(),
                },
                b"staged edit input",
            )
            .await
            .map_err(|error| format!("input staging failed: {error:?}"))?;
        let reconciler = PostgresReconciliationStore::new(database.pool.clone());
        let outcome =
            reconcile_input_cleanup(&reconciler, &blobs, "cleanup-expired", 0, 60_000, 10)
                .await
                .map_err(|error| format!("input cleanup failed: {error:?}"))?;
        require(
            outcome.claimed == 1 && outcome.completed == 1 && outcome.failed == 0,
            format!("unexpected input cleanup outcome: {outcome:?}"),
        )?;
        require(
            blobs.get(&blob).await == Err(InputBlobReadError::Integrity),
            "expired session input blob was not deleted",
        )?;
        let state: (String, Option<String>, Option<i64>, String, String) = sqlx::query_as(
            r#"
            SELECT s.input_cleanup_state, s.input_cleanup_owner,
                   s.input_cleanup_completed_at_ms, s.state, i.state
            FROM admission_sessions s
            JOIN idempotency_requests i ON i.session_id = s.session_id
            WHERE s.session_id = $1
            "#,
        )
        .bind(ticket.session_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("cleanup state query failed: {error}"))?;
        require(
            state.0 == "complete"
                && state.1.is_none()
                && state.2.is_some()
                && state.3 == "aborted"
                && state.4 == "aborted",
            format!("cleanup completion was not persisted: {state:?}"),
        )?;
        let duplicate =
            reconcile_input_cleanup(&reconciler, &blobs, "cleanup-expired", 0, 60_000, 10)
                .await
                .map_err(|error| format!("duplicate cleanup failed: {error:?}"))?;
        require(
            duplicate.claimed == 0,
            format!("completed cleanup was reclaimed: {duplicate:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn failed_input_delete_is_retried_after_cleanup_lease_expiry() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let ticket = expired_edit_session(&database.pool, "input-retry").await?;
        let blobs = InMemoryArtifactBlobStore::default();
        let blob = blobs
            .put(
                InputBlobKey {
                    admission_session_id: ticket.session_id,
                    input_id: Uuid::new_v4(),
                },
                b"retryable staged input",
            )
            .await
            .map_err(|error| format!("input staging failed: {error:?}"))?;
        let reconciler = PostgresReconciliationStore::new(database.pool.clone());
        let failed = reconcile_input_cleanup(
            &reconciler,
            &UnavailableDeleteStore,
            "cleanup-first",
            0,
            60_000,
            10,
        )
        .await
        .map_err(|error| format!("failed cleanup pass errored: {error:?}"))?;
        require(
            failed.claimed == 1 && failed.completed == 0 && failed.failed == 1,
            format!("delete failure was not retained for retry: {failed:?}"),
        )?;
        sqlx::query(
            "UPDATE admission_sessions SET input_cleanup_lease_expires_at_ms = 0 WHERE session_id = $1",
        )
        .bind(ticket.session_id)
        .execute(&database.pool)
        .await
        .map_err(|error| format!("cleanup lease expiry failed: {error}"))?;
        let retried = reconcile_input_cleanup(
            &reconciler,
            &blobs,
            "cleanup-second",
            0,
            60_000,
            10,
        )
        .await
        .map_err(|error| format!("cleanup retry failed: {error:?}"))?;
        require(
            retried.claimed == 1 && retried.completed == 1 && retried.failed == 0,
            format!("expired cleanup lease was not recovered: {retried:?}"),
        )?;
        require(
            blobs.get(&blob).await == Err(InputBlobReadError::Integrity),
            "retried input cleanup did not delete the blob",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn terminal_edit_inputs_are_cleanup_candidates() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let ticket = terminal_edit_session(&database.pool, "input-terminal").await?;
        let blobs = InMemoryArtifactBlobStore::default();
        let blob = blobs
            .put(
                InputBlobKey {
                    admission_session_id: ticket.session_id,
                    input_id: Uuid::new_v4(),
                },
                b"terminal edit input",
            )
            .await
            .map_err(|error| format!("terminal input staging failed: {error:?}"))?;
        let reconciler = PostgresReconciliationStore::new(database.pool.clone());
        let outcome =
            reconcile_input_cleanup(&reconciler, &blobs, "cleanup-terminal", 0, 60_000, 10)
                .await
                .map_err(|error| format!("terminal input cleanup failed: {error:?}"))?;
        require(
            outcome.completed == 1 && outcome.failed == 0,
            format!("terminal edit was not cleaned: {outcome:?}"),
        )?;
        require(
            blobs.get(&blob).await == Err(InputBlobReadError::Integrity),
            "terminal edit blob remained readable",
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_input_cleanup_claims_have_one_owner() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let ticket = expired_edit_session(&database.pool, "input-concurrent").await?;
        let left = PostgresReconciliationStore::new(database.pool.clone());
        let right = PostgresReconciliationStore::new(database.pool.clone());
        let (left_claims, right_claims) = tokio::join!(
            left.claim_input_cleanup("cleanup-left", 0, 60_000, 1),
            right.claim_input_cleanup("cleanup-right", 0, 60_000, 1),
        );
        let left_claims = left_claims.map_err(|error| format!("left claim failed: {error:?}"))?;
        let right_claims =
            right_claims.map_err(|error| format!("right claim failed: {error:?}"))?;
        require(
            left_claims.len() + right_claims.len() == 1,
            format!("cleanup session had multiple owners: {left_claims:?} {right_claims:?}"),
        )?;
        let (owner, session_id) = if let Some(session_id) = left_claims.first() {
            ("cleanup-left", *session_id)
        } else {
            ("cleanup-right", right_claims[0])
        };
        require(
            session_id == ticket.session_id,
            "wrong cleanup session claimed",
        )?;
        left.complete_input_cleanup(owner, session_id)
            .await
            .map_err(|error| format!("cleanup completion failed: {error:?}"))
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn uncertain_edit_inputs_are_never_cleanup_candidates() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let admission = PostgresAdmissionStore::new(database.pool.clone());
        let lease = ready_lease(&database.pool, &admission, "u").await?;
        sqlx::query("UPDATE admission_sessions SET operation = 'edit' WHERE job_id = $1")
            .bind(lease.job_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("uncertain session operation update failed: {error}"))?;
        sqlx::query("UPDATE jobs SET operation = 'edit' WHERE job_id = $1")
            .bind(lease.job_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("uncertain job operation update failed: {error}"))?;
        admission
            .start(&lease)
            .await
            .map_err(|error| format!("uncertain work start failed: {error}"))?;
        expire_lease(&database.pool, &lease).await?;
        let reconciler = PostgresReconciliationStore::new(database.pool.clone());
        let work = reconciler
            .reconcile_expired_work(1)
            .await
            .map_err(|error| format!("uncertain work reconciliation failed: {error:?}"))?;
        require(work.uncertain == 1, "edit work did not become uncertain")?;
        let claims = reconciler
            .claim_input_cleanup("cleanup-uncertain", 0, 60_000, 10)
            .await
            .map_err(|error| format!("uncertain cleanup claim failed: {error:?}"))?;
        require(
            claims.is_empty(),
            format!("uncertain edit input was claimed for deletion: {claims:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

struct OrphanFixture {
    reservation: UsageReservation,
    ticket: AdmissionTicket,
}

async fn expired_edit_session(pool: &PgPool, key: &str) -> TestResult<AdmissionTicket> {
    let admission = PostgresAdmissionStore::new(pool.clone());
    let claim = admission
        .claim(ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: format!("tenant_{}", Uuid::new_v4().simple()),
            project_id: format!("project-{key}"),
            api_profile: "openai-images-v1".to_string(),
            operation: "edit".to_string(),
            request_id: format!("req_{}", Uuid::new_v4().simple()),
            idempotency_key_digest: Some("c".repeat(64)),
            request_hash: "d".repeat(64),
            deadline_at_ms: i64::MAX,
        })
        .await
        .map_err(|error| format!("edit admission failed: {error}"))?;
    let AdmissionClaim::Owner(ticket) = claim else {
        return Err(format!("unexpected edit claim: {claim:?}"));
    };
    sqlx::query(
        "UPDATE admission_sessions SET deadline_at_ms = 0, updated_at_ms = 0 WHERE session_id = $1",
    )
    .bind(ticket.session_id)
    .execute(pool)
    .await
    .map_err(|error| format!("edit session expiry failed: {error}"))?;
    Ok(ticket)
}

async fn terminal_edit_session(pool: &PgPool, key: &str) -> TestResult<AdmissionTicket> {
    let ticket = expired_edit_session(pool, key).await?;
    let tenant_id: String =
        sqlx::query_scalar("SELECT tenant_id FROM admission_sessions WHERE session_id = $1")
            .bind(ticket.session_id)
            .fetch_one(pool)
            .await
            .map_err(|error| format!("terminal session tenant query failed: {error}"))?;
    let request_id: String =
        sqlx::query_scalar("SELECT request_id FROM admission_sessions WHERE session_id = $1")
            .bind(ticket.session_id)
            .fetch_one(pool)
            .await
            .map_err(|error| format!("terminal session request query failed: {error}"))?;
    let reservation = PostgresUsageStore::new(pool.clone())
        .reserve(UsageCharge {
            tenant_id,
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation: "edit",
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            units: 1,
            limits: UsageLimits {
                five_hour_image_limit: 10,
                seven_day_image_limit: 20,
            },
        })
        .await
        .map_err(|error| format!("terminal edit reserve failed: {error:?}"))?;
    sqlx::query(
        r#"
        UPDATE admission_sessions
        SET state = 'attached', job_id = $2, updated_at_ms = 0
        WHERE session_id = $1
        "#,
    )
    .bind(ticket.session_id)
    .bind(reservation.job_id)
    .execute(pool)
    .await
    .map_err(|error| format!("terminal admission update failed: {error}"))?;
    sqlx::query(
        "UPDATE jobs SET state = 'failed', finished_at_ms = 0, updated_at_ms = 0 WHERE job_id = $1",
    )
    .bind(reservation.job_id)
    .execute(pool)
    .await
    .map_err(|error| format!("terminal job update failed: {error}"))?;
    Ok(ticket)
}

struct UnavailableDeleteStore;

#[async_trait]
impl InputBlobStore for UnavailableDeleteStore {
    async fn put(&self, _: InputBlobKey, _: &[u8]) -> Result<InputBlobRef, InputBlobWriteError> {
        Err(InputBlobWriteError::Unavailable)
    }

    async fn get(&self, _: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
        Err(InputBlobReadError::Unavailable)
    }

    async fn delete(&self, _: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
        Err(InputBlobDeleteError::Unavailable)
    }

    async fn delete_session(&self, _: Uuid) -> Result<(), InputBlobDeleteError> {
        Err(InputBlobDeleteError::Unavailable)
    }
}

async fn orphan_reservation(pool: &PgPool, key: &str) -> TestResult<OrphanFixture> {
    let request_id = format!("req_{}", Uuid::new_v4().simple());
    let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
    let admission = PostgresAdmissionStore::new(pool.clone());
    let claim = admission
        .claim(ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: tenant_id.clone(),
            project_id: format!("project-{key}"),
            api_profile: "openai-images-v1".to_string(),
            operation: "generation".to_string(),
            request_id: request_id.clone(),
            idempotency_key_digest: Some("b".repeat(64)),
            request_hash: "a".repeat(64),
            deadline_at_ms: i64::MAX,
        })
        .await
        .map_err(|error| format!("admission failed: {error}"))?;
    let AdmissionClaim::Owner(ticket) = claim else {
        return Err(format!("unexpected claim: {claim:?}"));
    };
    let reservation = PostgresUsageStore::new(pool.clone())
        .reserve(UsageCharge {
            tenant_id,
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation: "generation",
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            units: 1,
            limits: UsageLimits {
                five_hour_image_limit: 10,
                seven_day_image_limit: 20,
            },
        })
        .await
        .map_err(|error| format!("reserve failed: {error:?}"))?;
    Ok(OrphanFixture {
        reservation,
        ticket,
    })
}

async fn age_orphan(pool: &PgPool, orphan: &OrphanFixture, age_ms: i64) -> TestResult {
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("database clock query failed: {error}"))?;
    let old = now.saturating_sub(age_ms);
    sqlx::query(
        r#"
        UPDATE admission_sessions
        SET created_at_ms = $2, updated_at_ms = $2
        WHERE session_id = $1
        "#,
    )
    .bind(orphan.ticket.session_id)
    .bind(old)
    .execute(pool)
    .await
    .map_err(|error| format!("admission aging failed: {error}"))?;
    sqlx::query(
        r#"
        UPDATE quota_reservations
        SET created_at_ms = $2, updated_at_ms = $2
        WHERE reservation_id = $1
        "#,
    )
    .bind(orphan.reservation.reservation_id)
    .bind(old)
    .execute(pool)
    .await
    .map_err(|error| format!("reservation aging failed: {error}"))?;
    sqlx::query(
        r#"
        UPDATE jobs
        SET created_at_ms = $2, updated_at_ms = $2
        WHERE job_id = $1
        "#,
    )
    .bind(orphan.reservation.job_id)
    .bind(old)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| format!("job aging failed: {error}"))
}

async fn assert_orphan_terminal_state(pool: &PgPool, orphan: &OrphanFixture) -> TestResult {
    let states: (
        String,
        i32,
        i32,
        String,
        Option<String>,
        String,
        String,
        Option<String>,
    ) = sqlx::query_as(
        r#"
            SELECT qr.state, qr.requested_units, qr.released_units,
                   j.state, j.last_error_code, s.state, i.state, i.terminal_outcome
            FROM quota_reservations qr
            JOIN jobs j ON j.job_id = qr.job_id AND j.reservation_id = qr.reservation_id
            JOIN admission_sessions s ON s.session_id = $3
            JOIN idempotency_requests i ON i.session_id = s.session_id
            WHERE qr.reservation_id = $1 AND j.job_id = $2
            "#,
    )
    .bind(orphan.reservation.reservation_id)
    .bind(orphan.reservation.job_id)
    .bind(orphan.ticket.session_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("orphan state query failed: {error}"))?;
    require(
        states
            == (
                "released".to_string(),
                1,
                1,
                "failed".to_string(),
                Some("orphaned_admission".to_string()),
                "aborted".to_string(),
                "aborted".to_string(),
                None,
            ),
        format!("orphan transition was not atomic: {states:?}"),
    )?;

    let effects: (i64, i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*) FROM metering_events
           WHERE job_id = $1 AND event_type = 'quota_released'
             AND outcome = 'orphaned_admission'),
          (SELECT COUNT(*) FROM metering_events
           WHERE job_id = $1 AND event_type = 'job_failed'
             AND outcome = 'orphaned_admission'),
          (SELECT COUNT(*) FROM usage_events
           WHERE tenant_id = $2 AND request_id = $3 AND outcome = 'charged'),
          (SELECT COUNT(*) FROM job_events
           WHERE job_id = $1 AND event_type = 'job.failed'
             AND semantic_key = 'job.orphaned_reservation'),
          (SELECT COUNT(*) FROM outbox_events
           WHERE job_id = $1 AND event_type = 'job.failed'
             AND semantic_key = 'job.orphaned_reservation')
        "#,
    )
    .bind(orphan.reservation.job_id)
    .bind(&orphan.reservation.charge.tenant_id)
    .bind(&orphan.reservation.charge.request_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("orphan effects query failed: {error}"))?;
    require(
        effects == (1, 1, 0, 1, 1),
        format!("orphan effects were not exactly once: {effects:?}"),
    )
}

async fn ready_lease(
    pool: &PgPool,
    admission: &PostgresAdmissionStore,
    key: &str,
) -> TestResult<WorkLease> {
    let request_id = format!("req_{}", Uuid::new_v4().simple());
    let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
    let job = GenerationJob {
        request_id: request_id.clone(),
        model: "gpt-image-2".to_string(),
        prompt: "reconciliation fixture".to_string(),
        moderation: "auto".to_string(),
        n: 1,
        size: "auto".to_string(),
        quality: "high".to_string(),
        output_format: "png".to_string(),
        output_compression: None,
        background: "opaque".to_string(),
        stream: false,
        partial_images: 0,
    };
    let command =
        GenerationCommandV1::from_generation_job(&job, "openai-images-v1", "openai-codex");
    let request_hash = command.request_hash_hex();
    let reservation = PostgresUsageStore::new(pool.clone())
        .reserve(UsageCharge {
            tenant_id: tenant_id.clone(),
            request_id: request_id.clone(),
            admission_session_id: None,
            operation: "generation",
            provider_id: "openai-codex".to_string(),
            model: "gpt-image-2".to_string(),
            units: 1,
            limits: UsageLimits {
                five_hour_image_limit: 10,
                seven_day_image_limit: 20,
            },
        })
        .await
        .map_err(|error| format!("reserve failed: {error:?}"))?;
    let claim = admission
        .claim(ClaimAdmission {
            owner_token: Uuid::new_v4(),
            tenant_id: tenant_id.clone(),
            project_id: format!("project-{key}"),
            api_profile: "openai-images-v1".to_string(),
            operation: "generation".to_string(),
            request_id,
            idempotency_key_digest: Some(key.repeat(64)),
            request_hash,
            deadline_at_ms: i64::MAX,
        })
        .await
        .map_err(|error| format!("admission failed: {error}"))?;
    let AdmissionClaim::Owner(ticket) = claim else {
        return Err(format!("unexpected claim: {claim:?}"));
    };
    admission
        .attach(AttachJob {
            ticket,
            job_id: reservation.job_id,
            command_schema: "openai.images.generation.v1".to_string(),
            command_json: to_value(command).map_err(|error| error.to_string())?,
            input_manifest: None,
            work_kind: "image_batch".to_string(),
            schedule_scope: format!("tenant:{tenant_id}"),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: 1,
            contract: AdmissionContract::LegacyV1,
        })
        .await
        .map_err(|error| format!("attach failed: {error}"))?;
    admission
        .claim_job(reservation.job_id, "original-worker", 60_000)
        .await
        .map_err(|error| format!("claim work failed: {error}"))?
        .ok_or_else(|| "ready work not claimable".to_string())
}

async fn expire_lease(pool: &PgPool, lease: &WorkLease) -> TestResult {
    sqlx::query(
        "UPDATE work_items SET lease_expires_at_ms = 0 WHERE work_item_id = $1 AND execution_id = $2",
    )
    .bind(lease.work_item_id)
    .bind(lease.execution_id)
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| format!("lease expiry failed: {error}"))
}

struct TestDatabase {
    schema: String,
    pool: PgPool,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Some(url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping reconciliation test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_reconcile_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 4, &schema)
            .await
            .map_err(|error| format!("test database connection failed: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("database name query failed: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("schema creation failed: {error}"))?;
        run_migrations(&pool)
            .await
            .map_err(|error| format!("migration failed: {error:?}"))?;
        Ok(Some(Self { schema, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("schema cleanup failed: {error}"));
        self.pool.close().await;
        result.map(|_| ())
    }
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn combine(primary: TestResult, cleanup: TestResult) -> TestResult {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(format!("{primary}; cleanup also failed: {cleanup}")),
    }
}
