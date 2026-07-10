use std::env;

use gpt_image_2_gateway::{
    admission::{
        AdmissionClaim, AdmissionError, AdmissionStore, AdmissionTicket, AttachJob, ClaimAdmission,
        PostgresAdmissionStore, WorkOutcome,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
};
use serde_json::json;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn migration_creates_durable_admission_tables() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        for table in [
            "admission_sessions",
            "idempotency_requests",
            "job_payloads",
            "work_items",
            "job_attempts",
            "job_events",
            "outbox_events",
        ] {
            let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(table)
                .fetch_one(&database.pool)
                .await
                .map_err(|error| format!("failed to inspect {table}: {error}"))?;
            require(exists, format!("migration did not create {table}"))?;
        }
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_same_key_claims_have_one_owner_and_conflicting_hash_is_rejected() -> TestResult
{
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let request = claim_request(Some("a".repeat(64)), "b".repeat(64));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let store = store.clone();
            let request = request.clone();
            tasks.push(tokio::spawn(async move { store.claim(request).await }));
        }
        let mut owners = 0;
        let mut in_progress = 0;
        for task in tasks {
            match task.await.map_err(|error| format!("claim task failed: {error}"))?
                .map_err(|error| format!("claim failed: {error}"))?
            {
                AdmissionClaim::Owner(_) => owners += 1,
                AdmissionClaim::InProgress { .. } => in_progress += 1,
                other => return Err(format!("unexpected concurrent outcome: {other:?}")),
            }
        }
        require(owners == 1, format!("expected one owner, got {owners}"))?;
        require(
            in_progress == 99,
            format!("expected 99 challengers, got {in_progress}"),
        )?;

        let conflict = store
            .claim(claim_request(Some("a".repeat(64)), "c".repeat(64)))
            .await
            .map_err(|error| format!("conflict claim failed: {error}"))?;
        require(
            matches!(conflict, AdmissionClaim::Conflict { job_id: None }),
            format!("different hash was not rejected: {conflict:?}"),
        )?;
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM admission_sessions), (SELECT COUNT(*) FROM idempotency_requests)",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to count identities: {error}"))?;
        require(counts == (1, 1), format!("unexpected identity counts: {counts:?}"))
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn no_key_claims_are_independent() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        for _ in 0..2 {
            let outcome = store
                .claim(claim_request(None, "d".repeat(64)))
                .await
                .map_err(|error| format!("unkeyed claim failed: {error}"))?;
            require(
                matches!(outcome, AdmissionClaim::Owner(_)),
                "unkeyed claim did not create an owner",
            )?;
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM admission_sessions")
            .fetch_one(&database.pool)
            .await
            .map_err(|error| format!("failed to count sessions: {error}"))?;
        require(count == 2, format!("expected two sessions, got {count}"))
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn expired_new_claim_is_rejected_without_persisting_admission() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let mut request = claim_request(Some("4".repeat(64)), "1".repeat(64));
        request.deadline_at_ms = 0;

        require(
            matches!(store.claim(request).await, Err(AdmissionError::Expired)),
            "expired claim was accepted",
        )?;
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM admission_sessions), (SELECT COUNT(*) FROM idempotency_requests)",
        )
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to count expired admission rows: {error}"))?;
        require(
            counts == (0, 0),
            format!("expired claim persisted rows: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn owner_expiring_before_attach_is_aborted() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let store = PostgresAdmissionStore::new(database.pool.clone());
        let ticket = match store
            .claim(claim_request(Some("3".repeat(64)), "2".repeat(64)))
            .await
            .map_err(|error| format!("owner claim failed: {error}"))?
        {
            AdmissionClaim::Owner(ticket) => ticket,
            other => return Err(format!("expected owner, got {other:?}")),
        };
        sqlx::query("UPDATE admission_sessions SET deadline_at_ms = 0 WHERE session_id = $1")
            .bind(ticket.session_id)
            .execute(&database.pool)
            .await
            .map_err(|error| format!("failed to expire admission: {error}"))?;
        let job_id = insert_job(&database.pool, "tenant-a").await?;

        require(
            matches!(
                store.attach(attach_request(ticket.clone(), job_id)).await,
                Err(AdmissionError::Expired)
            ),
            "expired owner attached a job",
        )?;
        let states: (String, String) = sqlx::query_as(
            r#"
            SELECT admission_sessions.state, idempotency_requests.state
            FROM admission_sessions
            JOIN idempotency_requests USING (session_id)
            WHERE admission_sessions.session_id = $1
            "#,
        )
        .bind(ticket.session_id)
        .fetch_one(&database.pool)
        .await
        .map_err(|error| format!("failed to read expired admission state: {error}"))?;
        require(
            states == ("aborted".to_string(), "aborted".to_string()),
            format!("expired admission was not aborted: {states:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn owner_attachment_and_lease_epoch_are_fenced() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = attachment_and_fencing_case(&database).await;
    combine(result, database.cleanup().await)
}

async fn attachment_and_fencing_case(database: &TestDatabase) -> TestResult {
    let store = PostgresAdmissionStore::new(database.pool.clone());
    let ticket = match store
        .claim(claim_request(Some("e".repeat(64)), "f".repeat(64)))
        .await
        .map_err(|error| format!("owner claim failed: {error}"))?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        other => return Err(format!("expected owner, got {other:?}")),
    };
    let job_id = insert_job(&database.pool, "tenant-a").await?;
    let forged = AdmissionTicket {
        owner_token: Uuid::new_v4(),
        ..ticket.clone()
    };
    let forged_result = store.attach(attach_request(forged, job_id)).await;
    require(
        matches!(forged_result, Err(AdmissionError::InvalidOwner)),
        "forged owner attached a job",
    )?;

    let attached = store
        .attach(attach_request(ticket, job_id))
        .await
        .map_err(|error| format!("valid owner failed to attach: {error}"))?;
    let lease = store
        .claim_ready("worker-a", 30_000)
        .await
        .map_err(|error| format!("work claim failed: {error}"))?
        .ok_or_else(|| "attached work was not ready".to_string())?;
    require(
        lease.work_item_id == attached.work_item_id && lease.job_id == job_id,
        "claimed the wrong work item",
    )?;
    require(
        store
            .claim_ready("worker-b", 30_000)
            .await
            .map_err(|error| format!("second claim failed: {error}"))?
            .is_none(),
        "second worker claimed leased work",
    )?;
    store
        .start(&lease)
        .await
        .map_err(|error| format!("valid lease did not start: {error}"))?;

    let stale = gpt_image_2_gateway::admission::WorkLease {
        lease_epoch: lease.lease_epoch + 1,
        ..lease.clone()
    };
    require(
        matches!(
            store.heartbeat(&stale, 30_000).await,
            Err(AdmissionError::StaleLease)
        ),
        "stale heartbeat succeeded",
    )?;
    require(
        matches!(
            store.settle(&stale, WorkOutcome::Succeeded, None).await,
            Err(AdmissionError::StaleLease)
        ),
        "stale settlement succeeded",
    )?;
    store
        .settle(&lease, WorkOutcome::Succeeded, None)
        .await
        .map_err(|error| format!("valid settlement failed: {error}"))?;
    require(
        matches!(
            store.settle(&lease, WorkOutcome::Succeeded, None).await,
            Err(AdmissionError::StaleLease)
        ),
        "duplicate settlement was not fenced",
    )?;

    let state: String =
        sqlx::query_scalar("SELECT state FROM idempotency_requests WHERE job_id = $1")
            .bind(job_id)
            .fetch_one(&database.pool)
            .await
            .map_err(|error| format!("failed to read idempotency state: {error}"))?;
    let terminal_outbox: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_events WHERE job_id = $1 AND event_type = 'job.succeeded'",
    )
    .bind(job_id)
    .fetch_one(&database.pool)
    .await
    .map_err(|error| format!("failed to count terminal outbox: {error}"))?;
    require(state == "succeeded", format!("unexpected state {state}"))?;
    require(
        terminal_outbox == 1,
        format!("terminal outbox count {terminal_outbox}"),
    )
}

fn claim_request(key: Option<String>, request_hash: String) -> ClaimAdmission {
    ClaimAdmission {
        tenant_id: "tenant-a".to_string(),
        project_id: "project-a".to_string(),
        api_profile: "openai-images-v1".to_string(),
        operation: "generation".to_string(),
        request_id: format!("req_{}", Uuid::new_v4().simple()),
        idempotency_key_digest: key,
        request_hash,
        deadline_at_ms: i64::MAX,
    }
}

fn attach_request(ticket: AdmissionTicket, job_id: Uuid) -> AttachJob {
    AttachJob {
        ticket,
        job_id,
        command_schema: "openai.images.generation.v1".to_string(),
        command_json: json!({"prompt": "durable"}),
        work_kind: "image_batch".to_string(),
    }
}

async fn insert_job(pool: &PgPool, tenant_id: &str) -> TestResult<Uuid> {
    let job_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, charged_units, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'generation', 'openai-codex', 'gpt-image-2',
                'reserved', 1, 0, 1, 1)
        "#,
    )
    .bind(job_id)
    .bind(tenant_id)
    .bind(format!("req_{}", Uuid::new_v4().simple()))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert test job: {error}"))?;
    Ok(job_id)
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
            eprintln!("skipping PostgreSQL admission test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_admission_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 16, &schema)
            .await
            .map_err(|error| format!("failed to connect to test database: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create test schema: {error}"))?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("failed to migrate test schema: {error:?}"));
        }
        Ok(Some(Self { schema, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to drop test schema: {error}"));
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
