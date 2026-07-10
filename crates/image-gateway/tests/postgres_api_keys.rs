use std::{env, str::FromStr, time::Duration};

use gpt_image_2_gateway::{
    ApiKeyStore, ImageGatewayError, PostgresApiKeyStore, database::run_migrations,
};
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

async fn revoke_authentication_race(database: &TestDatabase) -> TestResult {
    let project_id = format!("proj_{}", Uuid::new_v4().simple());
    let setup_store = PostgresApiKeyStore::new(database.pool("setup").await?);
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
    let authenticate_store = PostgresApiKeyStore::new(authenticate_pool);
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
    let store = PostgresApiKeyStore::new(database.pool("sequential").await?);
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
