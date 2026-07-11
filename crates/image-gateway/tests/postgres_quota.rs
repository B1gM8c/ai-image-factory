use std::{env, str::FromStr, time::Duration};

use gpt_image_2_gateway::{
    ImageGatewayError, PostgresUsageStore, UsageCharge, UsageLimits, UsageStore,
    database::run_migrations,
};
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{task::JoinHandle, time::timeout};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const DATABASE_URL: &str = "TEST_DATABASE_URL";
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn reserve_persists_selected_provider_and_snapshot_model() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = async {
        let pool = database.pool("quota_provider_model").await?;
        let store = PostgresUsageStore::new(pool.clone());
        let mut charge = test_charge("tenant_provider_model", "request_provider_model");
        charge.model = "gpt-image-2-2026-04-21".to_string();
        let reservation = store
            .reserve(charge)
            .await
            .map_err(|error| format!("provider/model reserve failed: {error:?}"))?;
        let identity: (String, String) =
            sqlx::query_as("SELECT provider_id, model FROM jobs WHERE job_id = $1")
                .bind(reservation.job_id)
                .fetch_one(&pool)
                .await
                .map_err(|error| format!("failed to read provider/model identity: {error}"))?;
        require(
            identity
                == (
                    "openai-codex".to_string(),
                    "gpt-image-2-2026-04-21".to_string(),
                ),
            format!("unexpected persisted provider/model identity: {identity:?}"),
        )
    }
    .await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn commit_and_reserve_are_linearized_by_the_tenant_lock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = commit_linearization_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn release_and_replacement_reserve_are_linearized_by_the_tenant_lock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = release_linearization_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn commit_rejects_forged_job_id_without_mutation() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = forged_commit_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn release_rejects_forged_request_units_and_operation_without_mutation() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = forged_release_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn reserve_uses_postgres_time_after_waiting_for_the_tenant_lock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = reserve_timestamp_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn commit_uses_postgres_time_after_waiting_for_the_tenant_lock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = transition_timestamp_case(&database, TimestampTransition::Commit).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn release_uses_postgres_time_after_waiting_for_the_tenant_lock() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = transition_timestamp_case(&database, TimestampTransition::Release).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn active_work_keeps_an_expired_timestamp_reserved_and_counted() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = active_work_expiry_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

async fn active_work_expiry_case(database: &TestDatabase) -> TestResult {
    let tenant_id = format!("tenant_active_expiry_{}", Uuid::new_v4().simple());
    let pool = database.pool("quota_active_expiry").await?;
    let store = PostgresUsageStore::new(pool.clone());
    let reservation = store
        .reserve(test_charge(&tenant_id, "request_active"))
        .await
        .map_err(|error| format!("active reservation failed: {error:?}"))?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'image_batch', 'ready', 0, 0, 0)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(reservation.job_id)
    .execute(&pool)
    .await
    .map_err(|error| format!("failed to attach active work: {error}"))?;
    sqlx::query("UPDATE quota_reservations SET expires_at_ms = 0 WHERE reservation_id = $1")
        .bind(reservation.reservation_id)
        .execute(&pool)
        .await
        .map_err(|error| format!("failed to age active reservation: {error}"))?;

    let denied = store
        .reserve(test_charge(&tenant_id, "request_replacement"))
        .await
        .expect_err("active work must continue to consume quota");
    require(
        denied.status_code() == axum::http::StatusCode::TOO_MANY_REQUESTS,
        format!("active work replacement should return 429, got {denied:?}"),
    )?;
    let state: String =
        sqlx::query_scalar("SELECT state FROM quota_reservations WHERE reservation_id = $1")
            .bind(reservation.reservation_id)
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to inspect active reservation: {error}"))?;
    require(
        state == "reserved",
        format!("active work reservation was swept to {state}"),
    )
}

#[derive(Clone, Copy)]
enum TimestampTransition {
    Commit,
    Release,
}

async fn reserve_timestamp_case(database: &TestDatabase) -> TestResult {
    let tenant_id = format!("tenant_timestamp_{}", Uuid::new_v4().simple());
    let guard_pool = database.pool("quota_guard_timestamp").await?;
    let mut guard = guard_pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin timestamp guard transaction: {error}"))?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(&tenant_id))
        .execute(&mut *guard)
        .await
        .map_err(|error| format!("failed to acquire timestamp guard lock: {error}"))?;

    let application = database.application_name("quota_timestamp");
    let store = PostgresUsageStore::new(database.pool_with_application_name(&application).await?);
    let monitor = database.pool("quota_monitor_timestamp").await?;
    let reserve_tenant = tenant_id.clone();
    let reserve = tokio::spawn(async move {
        store
            .reserve(test_charge(&reserve_tenant, "request_timestamp"))
            .await
    });

    if let Err(error) = wait_for_advisory_lock(&monitor, &application).await {
        guard.rollback().await.ok();
        abort_and_await(reserve).await;
        return Err(error);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let lower_bound_ms = postgres_now_ms(&monitor).await?;
    guard
        .commit()
        .await
        .map_err(|error| format!("failed to release timestamp guard lock: {error}"))?;
    let reservation = join_gateway(reserve, "timestamped reserve").await?;

    let created_at_ms: i64 = sqlx::query_scalar(
        "SELECT created_at_ms FROM quota_reservations WHERE reservation_id = $1",
    )
    .bind(reservation.reservation_id)
    .fetch_one(&monitor)
    .await
    .map_err(|error| format!("failed to read reservation timestamp: {error}"))?;
    require(
        created_at_ms >= lower_bound_ms,
        format!(
            "reservation timestamp {created_at_ms} predates PostgreSQL lock-wait lower bound {lower_bound_ms}"
        ),
    )
}

async fn transition_timestamp_case(
    database: &TestDatabase,
    transition: TimestampTransition,
) -> TestResult {
    let role = match transition {
        TimestampTransition::Commit => "commit_timestamp",
        TimestampTransition::Release => "release_timestamp",
    };
    let tenant_id = format!("tenant_{role}_{}", Uuid::new_v4().simple());
    let initial_store = PostgresUsageStore::new(database.pool("quota_timestamp_initial").await?);
    let reservation = initial_store
        .reserve(test_charge(&tenant_id, "request_timestamp_transition"))
        .await
        .map_err(|error| format!("initial timestamp reserve should succeed: {error:?}"))?;
    let reservation_id = reservation.reservation_id;

    let guard_pool = database.pool("quota_guard_transition_timestamp").await?;
    let mut guard = guard_pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin timestamp guard transaction: {error}"))?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(&tenant_id))
        .execute(&mut *guard)
        .await
        .map_err(|error| format!("failed to acquire timestamp guard lock: {error}"))?;

    let application = database.application_name(role);
    let store = PostgresUsageStore::new(database.pool_with_application_name(&application).await?);
    let monitor = database.pool("quota_monitor_transition_timestamp").await?;
    let operation = tokio::spawn(async move {
        match transition {
            TimestampTransition::Commit => store.commit(&reservation).await.map(|_| ()),
            TimestampTransition::Release => store.release(&reservation, "provider_failed").await,
        }
    });

    if let Err(error) = wait_for_advisory_lock(&monitor, &application).await {
        guard.rollback().await.ok();
        abort_and_await(operation).await;
        return Err(error);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let lower_bound_ms = postgres_now_ms(&monitor).await?;
    guard
        .commit()
        .await
        .map_err(|error| format!("failed to release timestamp guard lock: {error}"))?;
    join_gateway(operation, role).await?;

    let updated_at_ms: i64 = sqlx::query_scalar(
        "SELECT updated_at_ms FROM quota_reservations WHERE reservation_id = $1",
    )
    .bind(reservation_id)
    .fetch_one(&monitor)
    .await
    .map_err(|error| format!("failed to read transition timestamp: {error}"))?;
    require(
        updated_at_ms >= lower_bound_ms,
        format!(
            "{role} timestamp {updated_at_ms} predates PostgreSQL lock-wait lower bound {lower_bound_ms}"
        ),
    )
}

async fn postgres_now_ms(pool: &PgPool) -> TestResult<i64> {
    sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to read PostgreSQL clock: {error}"))
}

async fn commit_linearization_case(database: &TestDatabase) -> TestResult {
    let tenant_id = format!("tenant_commit_{}", Uuid::new_v4().simple());
    let initial_store = PostgresUsageStore::new(database.pool("quota_initial").await?);
    let reservation = initial_store
        .reserve(test_charge(&tenant_id, "request_a"))
        .await
        .map_err(|error| format!("initial reserve should succeed: {error:?}"))?;

    let guard_pool = database.pool("quota_guard_commit").await?;
    let mut guard = guard_pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin guard transaction: {error}"))?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(&tenant_id))
        .execute(&mut *guard)
        .await
        .map_err(|error| format!("failed to acquire guard advisory lock: {error}"))?;

    let commit_application = database.application_name("quota_commit");
    let commit_store = PostgresUsageStore::new(
        database
            .pool_with_application_name(&commit_application)
            .await?,
    );
    let monitor = database.pool("quota_monitor_commit").await?;
    let reserve_application = database.application_name("quota_reserve_b");
    let reserve_store = PostgresUsageStore::new(
        database
            .pool_with_application_name(&reserve_application)
            .await?,
    );
    let commit_reservation = reservation.clone();
    let commit = tokio::spawn(async move { commit_store.commit(&commit_reservation).await });

    if let Err(error) = wait_for_advisory_lock(&monitor, &commit_application).await {
        guard.rollback().await.ok();
        abort_and_await(commit).await;
        return Err(format!(
            "commit must wait on the tenant advisory lock: {error}"
        ));
    }

    let reserve_tenant = tenant_id.clone();
    let reserve = tokio::spawn(async move {
        reserve_store
            .reserve(test_charge(&reserve_tenant, "request_b"))
            .await
    });

    if let Err(error) = wait_for_advisory_lock(&monitor, &reserve_application).await {
        guard.rollback().await.ok();
        abort_and_await(commit).await;
        abort_and_await(reserve).await;
        return Err(format!(
            "second reserve must wait on the tenant advisory lock: {error}"
        ));
    }

    if let Err(error) = guard.commit().await {
        abort_and_await(commit).await;
        abort_and_await(reserve).await;
        return Err(format!("failed to release guard advisory lock: {error}"));
    }
    if let Err(error) = join_gateway(commit, "commit").await {
        abort_and_await(reserve).await;
        return Err(error);
    }
    let denied = join_gateway_error(reserve, "second reserve").await?;
    require(
        denied.status_code() == axum::http::StatusCode::TOO_MANY_REQUESTS,
        format!("second reserve should return 429, got {denied:?}"),
    )?;

    let total: i64 = sqlx::query_scalar(
        r#"
        SELECT
          COALESCE((
            SELECT SUM(units)
            FROM usage_events
            WHERE tenant_id = $1
          ), 0)::BIGINT
          +
          COALESCE((
            SELECT SUM(requested_units - committed_units - released_units)
            FROM quota_reservations
            WHERE tenant_id = $1 AND state = 'reserved' AND expires_at_ms > $2
          ), 0)::BIGINT
        "#,
    )
    .bind(&tenant_id)
    .bind(now_ms())
    .fetch_one(&monitor)
    .await
    .map_err(|error| format!("failed to read aggregate quota snapshot: {error}"))?;
    require(
        total == 1,
        format!("committed plus active reserved units should equal 1, got {total}"),
    )
}

async fn release_linearization_case(database: &TestDatabase) -> TestResult {
    let tenant_id = format!("tenant_release_{}", Uuid::new_v4().simple());
    let initial_store = PostgresUsageStore::new(database.pool("quota_release_initial").await?);
    let reservation = initial_store
        .reserve(test_charge(&tenant_id, "request_a"))
        .await
        .map_err(|error| format!("initial reserve should succeed: {error:?}"))?;

    let guard_pool = database.pool("quota_guard_release").await?;
    let mut guard = guard_pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin guard transaction: {error}"))?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quota_lock_id(&tenant_id))
        .execute(&mut *guard)
        .await
        .map_err(|error| format!("failed to acquire guard advisory lock: {error}"))?;

    let release_application = database.application_name("quota_release");
    let release_store = PostgresUsageStore::new(
        database
            .pool_with_application_name(&release_application)
            .await?,
    );
    let monitor = database.pool("quota_monitor_release").await?;
    let replacement_application = database.application_name("quota_replacement");
    let replacement_store = PostgresUsageStore::new(
        database
            .pool_with_application_name(&replacement_application)
            .await?,
    );
    let release_reservation = reservation.clone();
    let release = tokio::spawn(async move {
        release_store
            .release(&release_reservation, "provider_failed")
            .await
    });

    if let Err(error) = wait_for_advisory_lock(&monitor, &release_application).await {
        guard.rollback().await.ok();
        abort_and_await(release).await;
        return Err(format!(
            "release must wait on the tenant advisory lock: {error}"
        ));
    }

    let replacement_tenant = tenant_id.clone();
    let replacement = tokio::spawn(async move {
        replacement_store
            .reserve(test_charge(&replacement_tenant, "request_b"))
            .await
    });
    if let Err(error) = wait_for_advisory_lock(&monitor, &replacement_application).await {
        guard.rollback().await.ok();
        abort_and_await(release).await;
        abort_and_await(replacement).await;
        return Err(format!(
            "replacement reserve must wait on the tenant advisory lock: {error}"
        ));
    }

    if let Err(error) = guard.commit().await {
        abort_and_await(release).await;
        abort_and_await(replacement).await;
        return Err(format!("failed to release guard advisory lock: {error}"));
    }
    if let Err(error) = join_gateway(release, "release").await {
        abort_and_await(replacement).await;
        return Err(error);
    }
    join_gateway(replacement, "replacement reserve").await?;
    Ok(())
}

async fn forged_commit_case(database: &TestDatabase) -> TestResult {
    let tenant_id = format!("tenant_forged_commit_{}", Uuid::new_v4().simple());
    let pool = database.pool("quota_forged_commit").await?;
    let store = PostgresUsageStore::new(pool.clone());
    let reservation = store
        .reserve(test_charge(&tenant_id, "request_a"))
        .await
        .map_err(|error| format!("initial reserve should succeed: {error:?}"))?;
    let before = quota_state(&pool, &tenant_id, reservation.reservation_id).await?;

    let mut forged = reservation.clone();
    forged.job_id = Uuid::new_v4();
    require(
        store.commit(&forged).await.is_err(),
        "commit must reject a forged job_id".to_string(),
    )?;

    let after = quota_state(&pool, &tenant_id, reservation.reservation_id).await?;
    require(
        after == before,
        format!("forged commit mutated database state\nbefore: {before:?}\nafter: {after:?}"),
    )
}

async fn forged_release_case(database: &TestDatabase) -> TestResult {
    let tenant_id = format!("tenant_forged_release_{}", Uuid::new_v4().simple());
    let pool = database.pool("quota_forged_release").await?;
    let store = PostgresUsageStore::new(pool.clone());
    let reservation = store
        .reserve(test_charge(&tenant_id, "request_a"))
        .await
        .map_err(|error| format!("initial reserve should succeed: {error:?}"))?;
    let before = quota_state(&pool, &tenant_id, reservation.reservation_id).await?;

    let mut forged = reservation.clone();
    forged.charge.request_id = "forged_request".to_string();
    forged.charge.operation = "edit";
    forged.charge.units = 99;
    require(
        store.release(&forged, "provider_failed").await.is_err(),
        "release must reject forged request metadata and units".to_string(),
    )?;

    let after = quota_state(&pool, &tenant_id, reservation.reservation_id).await?;
    require(
        after == before,
        format!("forged release mutated database state\nbefore: {before:?}\nafter: {after:?}"),
    )
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct QuotaState {
    reservation_state: String,
    requested_units: i32,
    committed_units: i32,
    released_units: i32,
    reservation_job_id: Uuid,
    reservation_request_id: String,
    job_state: String,
    charged_units: i32,
    last_error_code: Option<String>,
    usage_count: i64,
    usage_units: i64,
    metering_count: i64,
    metering_units: i64,
}

async fn quota_state(
    pool: &PgPool,
    tenant_id: &str,
    reservation_id: Uuid,
) -> TestResult<QuotaState> {
    sqlx::query_as(
        r#"
        SELECT
          qr.state AS reservation_state,
          qr.requested_units,
          qr.committed_units,
          qr.released_units,
          qr.job_id AS reservation_job_id,
          qr.request_id AS reservation_request_id,
          j.state AS job_state,
          j.charged_units,
          j.last_error_code,
          (SELECT COUNT(*) FROM usage_events WHERE tenant_id = $1) AS usage_count,
          COALESCE((SELECT SUM(units) FROM usage_events WHERE tenant_id = $1), 0)::BIGINT AS usage_units,
          (SELECT COUNT(*) FROM metering_events WHERE tenant_id = $1) AS metering_count,
          COALESCE((SELECT SUM(units) FROM metering_events WHERE tenant_id = $1), 0)::BIGINT AS metering_units
        FROM quota_reservations qr
        JOIN jobs j
          ON j.job_id = qr.job_id
         AND j.tenant_id = qr.tenant_id
         AND j.reservation_id = qr.reservation_id
        WHERE qr.reservation_id = $2 AND qr.tenant_id = $1
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read quota state: {error}"))
}

async fn wait_for_advisory_lock(pool: &PgPool, application_name: &str) -> TestResult {
    timeout(WAIT_TIMEOUT, async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_stat_activity
                  WHERE application_name = $1
                    AND state = 'active'
                    AND wait_event_type = 'Lock'
                    AND wait_event = 'advisory'
                )
                "#,
            )
            .bind(application_name)
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to inspect pg_stat_activity: {error}"))?;
            if waiting {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| format!("{application_name:?} did not enter advisory-lock wait within 5s"))?
}

async fn join_gateway<T>(
    mut task: JoinHandle<Result<T, ImageGatewayError>>,
    operation: &str,
) -> TestResult<T> {
    match timeout(WAIT_TIMEOUT, &mut task).await {
        Ok(result) => result
            .map_err(|error| format!("{operation} task failed: {error}"))?
            .map_err(|error| format!("{operation} should succeed: {error:?}")),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(format!("watchdog timed out waiting for {operation}"))
        }
    }
}

async fn join_gateway_error<T>(
    mut task: JoinHandle<Result<T, ImageGatewayError>>,
    operation: &str,
) -> TestResult<ImageGatewayError> {
    let result = match timeout(WAIT_TIMEOUT, &mut task).await {
        Ok(result) => result.map_err(|error| format!("{operation} task failed: {error}"))?,
        Err(_) => {
            task.abort();
            let _ = task.await;
            return Err(format!("watchdog timed out waiting for {operation}"));
        }
    };
    match result {
        Ok(_) => Err(format!(
            "{operation} should fail after the committed unit consumes the quota"
        )),
        Err(error) => Ok(error),
    }
}

async fn abort_and_await<T>(task: JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

fn test_charge(tenant_id: &str, request_id: &str) -> UsageCharge {
    UsageCharge {
        tenant_id: tenant_id.to_string(),
        request_id: request_id.to_string(),
        operation: "generation",
        provider_id: "openai-codex".to_string(),
        model: "gpt-image-2".to_string(),
        units: 1,
        limits: UsageLimits {
            five_hour_image_limit: 1,
            seven_day_image_limit: 1,
        },
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

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn require(condition: bool, message: String) -> TestResult {
    if condition { Ok(()) } else { Err(message) }
}

struct TestDatabase {
    database_url: String,
    schema: String,
    setup_pool: PgPool,
    application_prefix: String,
}

impl TestDatabase {
    async fn new() -> TestResult<Option<Self>> {
        let Ok(database_url) = env::var(DATABASE_URL) else {
            if env::var_os("CI").is_some() {
                return Err(format!("{DATABASE_URL} must be set when CI is present"));
            }
            eprintln!("skipping PostgreSQL quota test: {DATABASE_URL} is not set");
            return Ok(None);
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let schema = format!("image_gateway_quota_test_{suffix}");
        let application_prefix = format!("quota_test_{}", &suffix[..12]);
        let setup_pool = connect_pool(&database_url, &schema, &application_prefix).await?;

        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&setup_pool)
            .await
            .map_err(|error| format!("failed to identify test database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            setup_pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}, which does not contain 'test'"
            ));
        }

        let isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&setup_pool)
            .await
            .map_err(|error| format!("failed to inspect transaction isolation: {error}"))?;
        if isolation != "read committed" {
            setup_pool.close().await;
            return Err(format!(
                "quota tests require read committed transaction isolation, got {isolation:?}"
            ));
        }

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&setup_pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {schema}: {error}"))?;
        if let Err(error) = run_migrations(&setup_pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&setup_pool)
                .await;
            setup_pool.close().await;
            return Err(format!("failed to migrate isolated schema: {error:?}"));
        }

        Ok(Some(Self {
            database_url,
            schema,
            setup_pool,
            application_prefix,
        }))
    }

    fn application_name(&self, role: &str) -> String {
        format!("{}_{}", self.application_prefix, role)
    }

    async fn pool(&self, role: &str) -> TestResult<PgPool> {
        self.pool_with_application_name(&self.application_name(role))
            .await
    }

    async fn pool_with_application_name(&self, application_name: &str) -> TestResult<PgPool> {
        connect_pool(&self.database_url, &self.schema, application_name).await
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.schema
        )))
        .execute(&self.setup_pool)
        .await
        .map_err(|error| format!("failed to clean isolated schema {}: {error}", self.schema));
        self.setup_pool.close().await;
        result.map(|_| ())
    }
}

async fn connect_pool(
    database_url: &str,
    schema: &str,
    application_name: &str,
) -> TestResult<PgPool> {
    let options = PgConnectOptions::from_str(database_url)
        .map_err(|error| format!("invalid test database URL: {error}"))?
        .application_name(application_name)
        .options([("search_path", schema)]);
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| format!("test database should be reachable: {error}"))
}
