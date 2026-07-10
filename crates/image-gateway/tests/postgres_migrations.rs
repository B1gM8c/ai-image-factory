use std::{env, time::Duration};

use gpt_image_2_gateway::{
    ApiKeyKeyring, ApiKeyStore, ImageGatewayError, PostgresApiKeyStore, PostgresUsageStore,
    UsageCharge, UsageLimits, UsageStore,
    database::{
        connect_pool, connect_test_pool_with_search_path, run_migrations, verify_migrations,
    },
};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::time::timeout;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const REQUIRED_COLUMNS: [(&str, &str); 15] = [
    ("usage_events", "tenant_id"),
    ("quota_reservations", "tenant_id"),
    ("quota_reservations", "job_id"),
    ("quota_reservations", "committed_units"),
    ("jobs", "tenant_id"),
    ("jobs", "operation"),
    ("jobs", "provider_id"),
    ("jobs", "model"),
    ("jobs", "reservation_id"),
    ("jobs", "created_at_ms"),
    ("jobs", "updated_at_ms"),
    ("jobs", "last_error_code"),
    ("jobs", "last_error_message"),
    ("gateway_api_keys", "hash_algorithm"),
    ("gateway_api_keys", "pepper_version"),
];

const REQUIRED_INDEXES: [&str; 5] = [
    "usage_events_tenant_created_at_ms_idx",
    "gateway_api_keys_project_id_idx",
    "quota_reservations_active_tenant_idx",
    "jobs_tenant_state_created_idx",
    "metering_events_tenant_created_idx",
];

#[tokio::test]
async fn legacy_schema_without_sqlx_metadata_migrates_from_zero() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = legacy_schema_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn concurrent_fresh_migrations_are_repeatable() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = concurrent_migration_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn verification_fails_closed_for_invalid_migration_metadata() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = verification_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn both_stores_share_one_connection_pool() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = shared_pool_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn default_pool_pins_public_despite_url_search_path_options() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = default_pool_case(&test_schema).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

async fn default_pool_case(test_schema: &TestSchema) -> TestResult {
    let database_url = env::var("TEST_DATABASE_URL")
        .map_err(|_| "TEST_DATABASE_URL disappeared during test".to_string())?;
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let injected_url = format!(
        "{database_url}{separator}options=-csearch_path%3D{}",
        test_schema.name
    );
    let pool = connect_pool(&injected_url, 1)
        .await
        .map_err(|error| format!("default pool should connect: {error:?}"))?;
    let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&pool)
        .await
        .map_err(|error| format!("failed to read default pool schema: {error}"))?;
    pool.close().await;
    require(
        current_schema == "public",
        &format!("default pool resolved to {current_schema:?}, expected public"),
    )
}

async fn legacy_schema_case(pool: &PgPool) -> TestResult {
    sqlx::raw_sql(
        r#"
        CREATE TABLE usage_events (
            event_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            units INTEGER NOT NULL CHECK (units > 0),
            outcome TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        );

        CREATE TABLE quota_reservations (
            reservation_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            requested_units INTEGER NOT NULL CHECK (requested_units > 0),
            started_units INTEGER NOT NULL DEFAULT 0,
            released_units INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            expires_at_ms BIGINT NOT NULL
        );

        CREATE TABLE jobs (
            job_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            state TEXT NOT NULL,
            requested_units INTEGER NOT NULL,
            charged_units INTEGER NOT NULL DEFAULT 0,
            queue_entered_at_ms BIGINT,
            started_at_ms BIGINT,
            finished_at_ms BIGINT
        );
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to create legacy schema: {error}"))?;

    require(
        !migration_table_exists(pool).await?,
        "legacy schema must start without _sqlx_migrations",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "legacy schema migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "legacy schema verification should succeed",
    )?;
    assert_expected_schema(pool).await
}

async fn concurrent_migration_case(pool: &PgPool) -> TestResult {
    let (first, second) = tokio::join!(run_migrations(pool), run_migrations(pool));
    gateway_result(first, "first concurrent migration should succeed")?;
    gateway_result(second, "second concurrent migration should succeed")?;
    gateway_result(
        run_migrations(pool).await,
        "repeated migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "fresh schema verification should succeed",
    )?;
    assert_expected_schema(pool).await
}

async fn verification_case(pool: &PgPool) -> TestResult {
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a missing migration table",
    )?;
    require(
        !migration_table_exists(pool).await?,
        "verification must not create the migration table",
    )?;

    gateway_result(
        run_migrations(pool).await,
        "initial migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "current migrations should verify",
    )?;

    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create pending state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a pending migration",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "pending migration should be restorable",
    )?;

    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 0")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create missing state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a missing migration",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "missing migration should be restorable",
    )?;

    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create dirty state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject an unsuccessful migration",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET success = true WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to restore dirty state: {error}"))?;

    let checksum: Vec<u8> =
        sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 1")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to read migration checksum: {error}"))?;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(vec![0_u8])
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create checksum mismatch: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a checksum mismatch",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(checksum)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to restore migration checksum: {error}"))?;

    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (999, 'extra', true, $1, 0)",
    )
    .bind(vec![0_u8])
    .execute(pool)
    .await
    .map_err(|error| format!("failed to create extra migration state: {error}"))?;
    gateway_result(
        verify_migrations(pool).await,
        "verification must tolerate a newer applied migration",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 999")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to alter newer migration state: {error}"))?;
    gateway_result(
        verify_migrations(pool).await,
        "verification must only enforce embedded migration metadata",
    )?;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 999")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to remove extra migration: {error}"))?;

    gateway_result(
        verify_migrations(pool).await,
        "restored migration metadata should verify",
    )
}

async fn shared_pool_case(pool: &PgPool) -> TestResult {
    gateway_result(
        run_migrations(pool).await,
        "store schema migration should succeed",
    )?;
    let usage_store = PostgresUsageStore::new(pool.clone());
    let api_key_store = PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x22; 32])]).expect("test keyring must be valid"),
    );
    let held_connection = pool
        .acquire()
        .await
        .map_err(|error| format!("failed to acquire sole test connection: {error}"))?;

    require(
        timeout(Duration::from_millis(100), pool.acquire())
            .await
            .is_err(),
        "max_connections(1) must prevent a second pool connection",
    )?;
    require(
        timeout(
            Duration::from_millis(100),
            usage_store.reserve(test_charge("usage-blocked")),
        )
        .await
        .is_err(),
        "usage store must use the shared pool",
    )?;
    require(
        timeout(
            Duration::from_millis(100),
            api_key_store.create_service_account("proj_blocked", "Blocked"),
        )
        .await
        .is_err(),
        "API key store must use the shared pool",
    )?;

    drop(held_connection);
    let (usage_result, api_key_result) = tokio::join!(
        usage_store.reserve(test_charge("usage-ready")),
        api_key_store.create_service_account("proj_ready", "Ready"),
    );
    usage_result.map_err(|error| format!("usage store should be usable: {error:?}"))?;
    api_key_result.map_err(|error| format!("API key store should be usable: {error:?}"))?;
    Ok(())
}

async fn assert_expected_schema(pool: &PgPool) -> TestResult {
    require(
        migration_versions(pool).await? == vec![0, 1, 2, 3, 4],
        "applied migration versions must be exactly [0, 1, 2, 3, 4]",
    )?;

    for (table, column) in REQUIRED_COLUMNS {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2)",
        )
        .bind(table)
        .bind(column)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to query {table}.{column}: {error}"))?;
        require(exists, &format!("{table}.{column} must exist"))?;
    }

    for index in REQUIRED_INDEXES {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1)",
        )
        .bind(index)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to query index {index}: {error}"))?;
        require(exists, &format!("index {index} must exist"))?;
    }
    Ok(())
}

async fn migration_table_exists(pool: &PgPool) -> TestResult<bool> {
    sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to inspect migration table: {error}"))
}

async fn migration_versions(pool: &PgPool) -> TestResult<Vec<i64>> {
    sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .map_err(|error| format!("failed to query migration versions: {error}"))
}

fn gateway_result(result: Result<(), ImageGatewayError>, context: &str) -> TestResult {
    result.map_err(|error| format!("{context}: {error:?}"))
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}

fn test_charge(request_id: &str) -> UsageCharge {
    UsageCharge {
        tenant_id: "proj_test".to_string(),
        request_id: request_id.to_string(),
        operation: "generation",
        provider_id: "openai-codex".to_string(),
        model: "gpt-image-2".to_string(),
        units: 1,
        limits: UsageLimits {
            five_hour_image_limit: 10,
            seven_day_image_limit: 10,
        },
    }
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set when CI is present".to_string());
            }
            eprintln!("skipping PostgreSQL migration test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("image_gateway_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify test database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}, which does not contain 'test'"
            ));
        }

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {name}: {error}"))?;
        let setup = async {
            let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(&pool)
                .await
                .map_err(|error| format!("failed to inspect current schema: {error}"))?;
            require(
                current_schema == name,
                &format!(
                    "test connection search_path resolved to {current_schema:?}, expected {name:?}"
                ),
            )
        }
        .await;
        if let Err(error) = setup {
            let cleanup = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{name}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return match cleanup {
                Ok(_) => Err(error),
                Err(cleanup_error) => Err(format!(
                    "{error}; additionally failed to clean isolated schema {name}: {cleanup_error}"
                )),
            };
        }
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to clean isolated schema {}: {error}", self.name));
        self.pool.close().await;
        result.map(|_| ())
    }
}
