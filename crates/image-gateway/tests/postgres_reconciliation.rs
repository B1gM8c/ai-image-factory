use std::env;

use gpt_image_2_gateway::{
    GenerationJob, PostgresReconciliationStore, PostgresUsageStore, ReconciliationStore,
    UsageCharge, UsageLimits, UsageReservation, UsageStore,
    admission::{
        AdmissionClaim, AdmissionStore, AdmissionTicket, AttachJob, ClaimAdmission,
        GenerationCommandV1, PostgresAdmissionStore, WorkLease,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
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

struct OrphanFixture {
    reservation: UsageReservation,
    ticket: AdmissionTicket,
}

async fn orphan_reservation(pool: &PgPool, key: &str) -> TestResult<OrphanFixture> {
    let request_id = format!("req_{}", Uuid::new_v4().simple());
    let tenant_id = format!("tenant_{}", Uuid::new_v4().simple());
    let admission = PostgresAdmissionStore::new(pool.clone());
    let claim = admission
        .claim(ClaimAdmission {
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
            work_kind: "image_batch".to_string(),
            schedule_scope: format!("tenant:{tenant_id}"),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: 1,
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
