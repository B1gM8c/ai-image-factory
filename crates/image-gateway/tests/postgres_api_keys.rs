use std::{env, str::FromStr, time::Duration};

use gpt_image_2_gateway::{
    ApiKeyKeyring, ApiKeyStore, ImageGatewayError, PostgresApiKeyStore, database::run_migrations,
};
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::{sync::Barrier, task::JoinHandle, time::timeout};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const DATABASE_URL: &str = "TEST_DATABASE_URL";
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn queued_revoke_wins_over_queued_authentication() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = revoke_authentication_race(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn authentication_stays_rejected_after_delete() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = sequential_authentication_and_delete(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn pepper_rotation_preserves_only_configured_versions() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = pepper_rotation_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn legacy_sha_key_authenticates_during_migration_window() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = legacy_key_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn concurrent_authentication_does_not_deadlock_on_last_used_update() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = concurrent_authentication_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

async fn revoke_authentication_race(database: &TestDatabase) -> TestResult {
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    let keyring = test_keyring();
    let setup_store = PostgresApiKeyStore::new(database.pool("setup").await?, keyring.clone());
    let account = setup_store
        .create_service_account(&project_id, "Race test")
        .await
        .map_err(|error| format!("failed to create API key: {error:?}"))?;
    let key_id = account.api_key.id;
    let key_value = account.api_key.value;

    let guard_pool = database.pool("guard").await?;
    let mut guard = guard_pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin row-lock guard: {error}"))?;
    sqlx::query("SELECT id FROM gateway_api_keys WHERE id = $1 FOR UPDATE")
        .bind(&key_id)
        .fetch_one(&mut *guard)
        .await
        .map_err(|error| format!("failed to lock API key row: {error}"))?;

    let monitor = database.pool("monitor").await?;
    let revoke_application = database.application_name("revoke");
    let revoke_store = PostgresApiKeyStore::new(
        database
            .pool_with_application_name(&revoke_application)
            .await?,
        keyring.clone(),
    );
    let revoke_project = project_id.clone();
    let revoke_key_id = key_id.clone();
    let revoke = tokio::spawn(async move {
        revoke_store
            .delete_project_api_key(&revoke_project, &revoke_key_id)
            .await
    });

    if let Err(error) = wait_for_database_lock(&monitor, &revoke_application).await {
        guard.rollback().await.ok();
        abort_and_await(revoke).await;
        return Err(format!("revoke must wait on the API key row lock: {error}"));
    }

    let authenticate_application = database.application_name("authenticate");
    let authenticate_pool = match database
        .pool_with_application_name(&authenticate_application)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            guard.rollback().await.ok();
            abort_and_await(revoke).await;
            return Err(error);
        }
    };
    let authenticate_store = PostgresApiKeyStore::new(authenticate_pool, keyring);
    let authenticate =
        tokio::spawn(async move { authenticate_store.authenticate(&key_value).await });

    if let Err(error) = wait_for_database_lock(&monitor, &authenticate_application).await {
        guard.rollback().await.ok();
        abort_and_await(revoke).await;
        abort_and_await(authenticate).await;
        return Err(format!(
            "authenticate mutation must wait behind the queued revoke: {error}"
        ));
    }

    if let Err(error) = guard.commit().await {
        abort_and_await(revoke).await;
        abort_and_await(authenticate).await;
        return Err(format!("failed to release API key row lock: {error}"));
    }

    let revoke_result = join_gateway(revoke, "revoke").await;
    let authenticate_result = join_gateway(authenticate, "authenticate").await;
    revoke_result?;
    let authenticated = authenticate_result?;
    require(
        authenticated.is_none(),
        "authentication must return None after the earlier queued revoke commits".to_string(),
    )
}

async fn sequential_authentication_and_delete(database: &TestDatabase) -> TestResult {
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    let store = PostgresApiKeyStore::new(database.pool("sequential").await?, test_keyring());
    let account = store
        .create_service_account(&project_id, "Sequential test")
        .await
        .map_err(|error| format!("failed to create API key: {error:?}"))?;

    require(
        store
            .authenticate(&account.api_key.value)
            .await
            .map_err(|error| format!("initial authentication failed: {error:?}"))?
            .is_some(),
        "new API key should authenticate".to_string(),
    )?;
    let version_before: String =
        sqlx::query_scalar("SELECT xmin::TEXT FROM gateway_api_keys WHERE id = $1")
            .bind(&account.api_key.id)
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to read key row version: {error}"))?;
    require(
        store
            .authenticate(&account.api_key.value)
            .await
            .map_err(|error| format!("repeat authentication failed: {error:?}"))?
            .is_some(),
        "repeat API key authentication failed".to_string(),
    )?;
    let version_after: String =
        sqlx::query_scalar("SELECT xmin::TEXT FROM gateway_api_keys WHERE id = $1")
            .bind(&account.api_key.id)
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to reread key row version: {error}"))?;
    require(
        version_after == version_before,
        "last_used_at coalescing still created a row version per request".to_string(),
    )?;
    store
        .delete_project_api_key(&project_id, &account.api_key.id)
        .await
        .map_err(|error| format!("failed to delete API key: {error:?}"))?;
    require(
        store
            .authenticate(&account.api_key.value)
            .await
            .map_err(|error| format!("post-delete authentication failed: {error:?}"))?
            .is_none(),
        "deleted API key must stay rejected".to_string(),
    )
}

async fn pepper_rotation_case(database: &TestDatabase) -> TestResult {
    let project_v1 = format!("proj_{}", Uuid::new_v4().simple());
    let v1_store = PostgresApiKeyStore::new(database.pool("pepper_v1").await?, keyring_v1());
    let v1_account = v1_store
        .create_service_account(&project_v1, "Pepper v1")
        .await
        .map_err(|error| format!("failed to create v1 key: {error:?}"))?;

    let metadata: (String, Option<i32>, String) = sqlx::query_as(
        "SELECT hash_algorithm, pepper_version, key_hash FROM gateway_api_keys WHERE id = $1",
    )
    .bind(&v1_account.api_key.id)
    .fetch_one(&database.setup_pool)
    .await
    .map_err(|error| format!("failed to inspect v1 key metadata: {error}"))?;
    require(
        metadata.0 == "hmac-sha256-v1" && metadata.1 == Some(1),
        format!("new key did not use versioned HMAC: {metadata:?}"),
    )?;
    require(
        metadata.2 != legacy_digest(&v1_account.api_key.value),
        "new key was stored as an unpeppered SHA-256 digest".to_string(),
    )?;

    let rotated = PostgresApiKeyStore::new(database.pool("pepper_v2").await?, keyring_v2());
    require(
        rotated
            .authenticate(&v1_account.api_key.value)
            .await
            .map_err(|error| format!("rotated keyring failed to verify v1 key: {error:?}"))?
            .is_some(),
        "rotated keyring did not retain v1 verification".to_string(),
    )?;
    let project_v2 = format!("proj_{}", Uuid::new_v4().simple());
    let v2_account = rotated
        .create_service_account(&project_v2, "Pepper v2")
        .await
        .map_err(|error| format!("failed to create v2 key: {error:?}"))?;
    let v2_version: Option<i32> =
        sqlx::query_scalar("SELECT pepper_version FROM gateway_api_keys WHERE id = $1")
            .bind(&v2_account.api_key.id)
            .fetch_one(&database.setup_pool)
            .await
            .map_err(|error| format!("failed to inspect v2 key version: {error}"))?;
    require(
        v2_version == Some(2),
        format!("rotated key was not created with v2: {v2_version:?}"),
    )?;

    let retired =
        PostgresApiKeyStore::new(database.pool("pepper_retired").await?, keyring_v2_only());
    require(
        retired
            .authenticate(&v1_account.api_key.value)
            .await
            .map_err(|error| format!("retired keyring v1 check failed: {error:?}"))?
            .is_none(),
        "v1 key remained valid after v1 pepper was removed".to_string(),
    )?;
    require(
        retired
            .authenticate(&v2_account.api_key.value)
            .await
            .map_err(|error| format!("retired keyring v2 check failed: {error:?}"))?
            .is_some(),
        "v2 key stopped authenticating after v1 retirement".to_string(),
    )
}

async fn legacy_key_case(database: &TestDatabase) -> TestResult {
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    let service_account_id = format!("svc_acct_{}", Uuid::new_v4().simple());
    let key_id = format!("key_{}", Uuid::new_v4().simple());
    let bearer = format!("sk-gw-legacy-{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO gateway_service_accounts (id, project_id, name, role, created_at) VALUES ($1, $2, 'Legacy', 'member', 1)",
    )
    .bind(&service_account_id)
    .bind(&project_id)
    .execute(&database.setup_pool)
    .await
    .map_err(|error| format!("failed to insert legacy service account: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO gateway_api_keys
          (id, project_id, service_account_id, name, key_hash, redacted_value, created_at)
        VALUES ($1, $2, $3, 'Legacy Key', $4, 'sk-gw-...legacy', 1)
        "#,
    )
    .bind(&key_id)
    .bind(&project_id)
    .bind(&service_account_id)
    .bind(legacy_digest(&bearer))
    .execute(&database.setup_pool)
    .await
    .map_err(|error| format!("failed to insert legacy API key: {error}"))?;

    let disabled =
        PostgresApiKeyStore::new(database.pool("legacy_disabled").await?, test_keyring());
    require(
        disabled
            .authenticate(&bearer)
            .await
            .map_err(|error| format!("default legacy rejection failed: {error:?}"))?
            .is_none(),
        "legacy key authenticated without the migration switch".to_string(),
    )?;

    let store = PostgresApiKeyStore::new(
        database.pool("legacy").await?,
        test_keyring().with_legacy_sha256(true),
    );
    let auth = store
        .authenticate(&bearer)
        .await
        .map_err(|error| format!("legacy authentication failed: {error:?}"))?;
    require(
        auth.is_some_and(|context| context.project_id == project_id),
        "legacy key was not accepted during migration window".to_string(),
    )
}

async fn concurrent_authentication_case(database: &TestDatabase) -> TestResult {
    const CLIENTS: usize = 16;
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    let keyring = test_keyring();
    let setup = PostgresApiKeyStore::new(database.pool("auth_setup").await?, keyring.clone());
    let account = setup
        .create_service_account(&project_id, "Concurrent auth")
        .await
        .map_err(|error| format!("failed to create concurrent auth key: {error:?}"))?;
    sqlx::query("UPDATE gateway_api_keys SET last_used_at = NULL WHERE id = $1")
        .bind(&account.api_key.id)
        .execute(&database.setup_pool)
        .await
        .map_err(|error| format!("failed to reset last_used_at: {error}"))?;

    let barrier = std::sync::Arc::new(Barrier::new(CLIENTS));
    let mut tasks = Vec::new();
    for index in 0..CLIENTS {
        let store = PostgresApiKeyStore::new(
            database.pool(&format!("auth_{index}")).await?,
            keyring.clone(),
        );
        let bearer = account.api_key.value.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.authenticate(&bearer).await
        }));
    }

    timeout(WAIT_TIMEOUT, async {
        for task in tasks {
            let authenticated = task
                .await
                .map_err(|error| format!("authentication task failed: {error}"))?
                .map_err(|error| format!("concurrent authentication failed: {error:?}"))?;
            require(
                authenticated.is_some(),
                "concurrent valid key was rejected".to_string(),
            )?;
        }
        Ok(())
    })
    .await
    .map_err(|_| "concurrent authentication deadlocked".to_string())?
}

async fn wait_for_database_lock(pool: &PgPool, application_name: &str) -> TestResult {
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
                    AND cardinality(pg_blocking_pids(pid)) > 0
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
    .map_err(|_| format!("{application_name:?} did not enter lock wait within 5s"))?
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

async fn abort_and_await<T>(task: JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

fn require(condition: bool, message: String) -> TestResult {
    if condition { Ok(()) } else { Err(message) }
}

fn test_keyring() -> ApiKeyKeyring {
    keyring_v1()
}

fn keyring_v1() -> ApiKeyKeyring {
    ApiKeyKeyring::new(1, [(1, vec![0x11; 32])]).expect("v1 test keyring must be valid")
}

fn keyring_v2() -> ApiKeyKeyring {
    ApiKeyKeyring::new(2, [(1, vec![0x11; 32]), (2, vec![0x22; 32])])
        .expect("rotated test keyring must be valid")
}

fn keyring_v2_only() -> ApiKeyKeyring {
    ApiKeyKeyring::new(2, [(2, vec![0x22; 32])]).expect("v2 test keyring must be valid")
}

fn legacy_digest(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
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
            eprintln!("skipping PostgreSQL API key test: {DATABASE_URL} is not set");
            return Ok(None);
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let schema = format!("image_gateway_api_key_test_{suffix}");
        let application_prefix = format!("api_key_test_{}", &suffix[..12]);
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
