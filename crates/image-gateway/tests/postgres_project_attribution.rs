use std::{collections::HashSet, env, str::FromStr, time::Duration};

use axum::http::StatusCode;
use gpt_image_2_gateway::{
    ApiKeyKeyring, ApiKeyPermissionMode, ApiKeyPermissions, ApiKeyStore, ImageGatewayError,
    PostgresApiKeyStore, PostgresUsageStore, UsageCharge, UsageLimits, UsageStore,
    database::run_migrations,
};
use image_provider_contracts::BillingMetric;
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
async fn api_key_reserve_persists_full_attribution_and_usage_job_id() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = api_key_attribution_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn legacy_reserve_persists_legacy_attribution_without_credential_ids() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = legacy_attribution_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn revoke_before_reserve_rejects_stale_authentication_context() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = revoke_before_reserve_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn reserve_before_revoke_preserves_attribution_and_revokes_future_authentication()
-> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = reserve_before_revoke_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn persisted_job_attribution_rejects_update_and_delete() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = immutable_attribution_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn same_second_api_key_pagination_is_complete_and_project_scoped() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };

    let result = stable_key_pagination_case(&database).await;
    let cleanup = database.cleanup().await;
    cleanup?;
    result
}

async fn api_key_attribution_case(database: &TestDatabase) -> TestResult {
    let pool = database.pool("api_key_attribution").await?;
    let key_store = PostgresApiKeyStore::new(pool.clone(), test_keyring());
    let project = key_store
        .create_project("Attribution project")
        .await
        .map_err(debug_gateway("failed to create project"))?;
    let account = key_store
        .create_service_account(
            &project.id,
            "Attribution service account",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_gateway("failed to create service account"))?;
    let auth = key_store
        .authenticate(&account.api_key.value)
        .await
        .map_err(debug_gateway("failed to authenticate API key"))?
        .ok_or_else(|| "new API key did not authenticate".to_string())?;

    let mut charge = test_charge(&auth.tenant_id, "request_api_key_attribution");
    charge.attribution = Some(auth.attribution());
    let usage_store = PostgresUsageStore::new(pool.clone());
    let reservation = usage_store
        .reserve(charge)
        .await
        .map_err(debug_gateway("failed to reserve attributed usage"))?;

    let attribution: (String, String, Option<String>, Option<String>, String) = sqlx::query_as(
        r#"
        SELECT tenant_id, project_id, service_account_id, api_key_id, auth_kind
        FROM job_auth_attributions
        WHERE job_id = $1
        "#,
    )
    .bind(reservation.job_id)
    .fetch_one(&pool)
    .await
    .map_err(|error| format!("failed to read persisted attribution: {error}"))?;
    require(
        attribution
            == (
                auth.tenant_id.clone(),
                project.id.clone(),
                Some(account.id.clone()),
                Some(account.api_key.id.clone()),
                "api_key".to_string(),
            ),
        format!("unexpected API key attribution: {attribution:?}"),
    )?;

    usage_store
        .commit(&reservation)
        .await
        .map_err(debug_gateway("failed to commit attributed usage"))?;
    let usage_job_ids: Vec<Option<Uuid>> = sqlx::query_scalar(
        "SELECT job_id FROM usage_events WHERE request_id = $1 ORDER BY created_at_ms, event_id",
    )
    .bind(&reservation.charge.request_id)
    .fetch_all(&pool)
    .await
    .map_err(|error| format!("failed to read usage event job IDs: {error}"))?;
    require(
        usage_job_ids == vec![Some(reservation.job_id)],
        format!("usage event did not retain reservation job ID: {usage_job_ids:?}"),
    )
}

async fn legacy_attribution_case(database: &TestDatabase) -> TestResult {
    let pool = database.pool("legacy_attribution").await?;
    let key_store = PostgresApiKeyStore::new(pool.clone(), test_keyring());
    let account = key_store
        .create_service_account(
            "proj_default",
            "Legacy attribution fixture",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_gateway("failed to create attribution fixture"))?;
    let auth = key_store
        .authenticate(&account.api_key.value)
        .await
        .map_err(debug_gateway("failed to authenticate attribution fixture"))?
        .ok_or_else(|| "attribution fixture key did not authenticate".to_string())?;
    let mut legacy = auth.attribution();
    legacy.project_id = "proj_default".to_string();
    legacy.service_account_id = None;
    legacy.api_key_id = None;
    legacy.credential_authz_version = None;

    let mut charge = test_charge("tenant_default", "request_legacy_attribution");
    charge.attribution = Some(legacy);
    let reservation = PostgresUsageStore::new(pool.clone())
        .reserve(charge)
        .await
        .map_err(debug_gateway("failed to reserve legacy usage"))?;
    let attribution: (String, Option<String>, Option<String>, String) = sqlx::query_as(
        r#"
        SELECT project_id, service_account_id, api_key_id, auth_kind
        FROM job_auth_attributions
        WHERE job_id = $1
        "#,
    )
    .bind(reservation.job_id)
    .fetch_one(&pool)
    .await
    .map_err(|error| format!("failed to read legacy attribution: {error}"))?;
    require(
        attribution == ("proj_default".to_string(), None, None, "legacy".to_string()),
        format!("unexpected legacy attribution: {attribution:?}"),
    )
}

async fn revoke_before_reserve_case(database: &TestDatabase) -> TestResult {
    let pool = database.pool("revoke_before_reserve").await?;
    let key_store = PostgresApiKeyStore::new(pool.clone(), test_keyring());
    let project = key_store
        .create_project("Revoke before reserve")
        .await
        .map_err(debug_gateway("failed to create project"))?;
    let account = key_store
        .create_service_account(
            &project.id,
            "Soon revoked",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_gateway("failed to create service account"))?;
    let auth = key_store
        .authenticate(&account.api_key.value)
        .await
        .map_err(debug_gateway("failed to authenticate before revoke"))?
        .ok_or_else(|| "new key did not authenticate before revoke".to_string())?;
    let mut charge = test_charge(&auth.tenant_id, "request_revoke_before_reserve");
    charge.attribution = Some(auth.attribution());
    let jobs_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .map_err(|error| format!("failed to count jobs before revoke: {error}"))?;

    key_store
        .delete_service_account(&project.id, &account.id)
        .await
        .map_err(debug_gateway("failed to revoke service account"))?;
    let error = PostgresUsageStore::new(pool.clone())
        .reserve(charge)
        .await
        .expect_err("stale authentication context must not reserve usage");
    require(
        error.status_code() == StatusCode::UNAUTHORIZED,
        format!(
            "stale attribution returned {}, not 401",
            error.status_code()
        ),
    )?;
    let jobs_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .map_err(|error| format!("failed to count jobs after rejected reserve: {error}"))?;
    require(
        jobs_after == jobs_before,
        format!("rejected reserve created jobs: before={jobs_before}, after={jobs_after}"),
    )
}

async fn reserve_before_revoke_case(database: &TestDatabase) -> TestResult {
    let reserve_pool = database.pool("reserve_before_revoke").await?;
    let keyring = test_keyring();
    let key_store = PostgresApiKeyStore::new(reserve_pool.clone(), keyring.clone());
    let project = key_store
        .create_project("Reserve before revoke")
        .await
        .map_err(debug_gateway("failed to create project"))?;
    let account = key_store
        .create_service_account(
            &project.id,
            "Admitted then revoked",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_gateway("failed to create service account"))?;
    let auth = key_store
        .authenticate(&account.api_key.value)
        .await
        .map_err(debug_gateway("failed to authenticate before reserve"))?
        .ok_or_else(|| "new key did not authenticate before reserve".to_string())?;
    let mut charge = test_charge(&auth.tenant_id, "request_reserve_before_revoke");
    charge.attribution = Some(auth.attribution());
    let reservation = PostgresUsageStore::new(reserve_pool.clone())
        .reserve(charge)
        .await
        .map_err(debug_gateway("reserve should commit before revoke"))?;

    let guard_pool = database.pool("revoke_guard").await?;
    let mut guard = guard_pool
        .begin()
        .await
        .map_err(|error| format!("failed to start revoke guard: {error}"))?;
    sqlx::query(
        r#"
        SELECT credential.id
        FROM gateway_api_keys credential
        JOIN gateway_service_accounts account
          ON account.id = credential.service_account_id
         AND account.project_id = credential.project_id
         AND account.tenant_id = credential.tenant_id
        JOIN gateway_projects project
          ON project.id = credential.project_id
         AND project.tenant_id = credential.tenant_id
        WHERE credential.id = $1
        FOR SHARE OF credential, account, project
        "#,
    )
    .bind(&account.api_key.id)
    .fetch_one(&mut *guard)
    .await
    .map_err(|error| format!("failed to lock admitted credential: {error}"))?;

    let revoke_application = database.application_name("queued_revoke");
    let revoke_store = PostgresApiKeyStore::new(
        database
            .pool_with_application_name(&revoke_application)
            .await?,
        keyring,
    );
    let revoke_project = project.id.clone();
    let revoke_account = account.id.clone();
    let revoke = tokio::spawn(async move {
        revoke_store
            .delete_service_account(&revoke_project, &revoke_account)
            .await
    });
    if let Err(error) = wait_for_database_lock(&database.setup_pool, &revoke_application).await {
        guard.rollback().await.ok();
        abort_and_await(revoke).await;
        return Err(format!(
            "revoke did not wait for the shared credential lock: {error}"
        ));
    }

    assert_attribution_identity(
        &database.setup_pool,
        reservation.job_id,
        &project.id,
        &account.id,
        &account.api_key.id,
    )
    .await?;
    guard
        .commit()
        .await
        .map_err(|error| format!("failed to release revoke guard: {error}"))?;
    join_gateway(revoke, "queued revoke").await?;

    require(
        key_store
            .authenticate(&account.api_key.value)
            .await
            .map_err(debug_gateway("post-revoke authentication failed"))?
            .is_none(),
        "revoked key authenticated after reserve".to_string(),
    )?;
    assert_attribution_identity(
        &database.setup_pool,
        reservation.job_id,
        &project.id,
        &account.id,
        &account.api_key.id,
    )
    .await
}

async fn immutable_attribution_case(database: &TestDatabase) -> TestResult {
    let pool = database.pool("immutable_attribution").await?;
    let key_store = PostgresApiKeyStore::new(pool.clone(), test_keyring());
    let project = key_store
        .create_project("Immutable attribution")
        .await
        .map_err(debug_gateway("failed to create project"))?;
    let account = key_store
        .create_service_account(
            &project.id,
            "Immutable attribution",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_gateway("failed to create service account"))?;
    let auth = key_store
        .authenticate(&account.api_key.value)
        .await
        .map_err(debug_gateway("failed to authenticate API key"))?
        .ok_or_else(|| "new key did not authenticate".to_string())?;
    let mut charge = test_charge(&auth.tenant_id, "request_immutable_attribution");
    charge.attribution = Some(auth.attribution());
    let reservation = PostgresUsageStore::new(pool.clone())
        .reserve(charge)
        .await
        .map_err(debug_gateway("failed to reserve attributed usage"))?;

    let update_error = sqlx::query(
        "UPDATE job_auth_attributions SET admitted_at_ms = admitted_at_ms + 1 WHERE job_id = $1",
    )
    .bind(reservation.job_id)
    .execute(&pool)
    .await
    .expect_err("attribution UPDATE must be rejected");
    require_sqlstate(&update_error, "55000", "attribution UPDATE")?;

    let delete_error = sqlx::query("DELETE FROM job_auth_attributions WHERE job_id = $1")
        .bind(reservation.job_id)
        .execute(&pool)
        .await
        .expect_err("attribution DELETE must be rejected");
    require_sqlstate(&delete_error, "55000", "attribution DELETE")
}

async fn stable_key_pagination_case(database: &TestDatabase) -> TestResult {
    let pool = database.pool("stable_key_pagination").await?;
    let key_store = PostgresApiKeyStore::new(pool.clone(), test_keyring());
    let project = key_store
        .create_project("Pagination project")
        .await
        .map_err(debug_gateway("failed to create pagination project"))?;
    let other_project = key_store
        .create_project("Other pagination project")
        .await
        .map_err(debug_gateway("failed to create other project"))?;
    let account = key_store
        .create_service_account(
            &project.id,
            "Pagination account",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_gateway("failed to create pagination account"))?;
    let other_account = key_store
        .create_service_account(
            &other_project.id,
            "Other pagination account",
            ApiKeyPermissionMode::All,
            ApiKeyPermissions::default(),
        )
        .await
        .map_err(debug_gateway("failed to create other pagination account"))?;

    sqlx::query("DELETE FROM gateway_api_keys WHERE service_account_id = $1")
        .bind(&account.id)
        .execute(&pool)
        .await
        .map_err(|error| format!("failed to remove generated pagination fixture key: {error}"))?;

    let created_at = 1_800_000_000_i64;
    let key_ids = (0..101)
        .map(|index| format!("key_page_{index:03}"))
        .collect::<Vec<_>>();
    let key_hashes = (0..101)
        .map(|index| format!("{:064x}", index + 1))
        .collect::<Vec<_>>();
    sqlx::query(
        r#"
        INSERT INTO gateway_api_keys
          (id, project_id, tenant_id, service_account_id, name, key_hash,
           hash_algorithm, pepper_version, redacted_value, created_at)
        SELECT fixture.id, $1, $2, $3, 'Secret Key', fixture.key_hash,
               'hmac-sha256-v1', 1, 'sk-test...fixture', $4
        FROM UNNEST($5::TEXT[], $6::TEXT[]) AS fixture(id, key_hash)
        "#,
    )
    .bind(&project.id)
    .bind(&project.id)
    .bind(&account.id)
    .bind(created_at)
    .bind(&key_ids)
    .bind(&key_hashes)
    .execute(&pool)
    .await
    .map_err(|error| format!("failed to insert same-second API keys: {error}"))?;

    let mut after = None;
    let mut listed = Vec::new();
    loop {
        let page = key_store
            .list_project_api_keys(&project.id, after.as_deref(), 17)
            .await
            .map_err(debug_gateway("failed to list API key page"))?;
        listed.extend(page.data.into_iter().map(|key| key.id));
        if !page.has_more {
            break;
        }
        after = page.last_id;
        require(
            after.is_some(),
            "paginated response omitted last_id while has_more was true".to_string(),
        )?;
    }

    let unique = listed.iter().cloned().collect::<HashSet<_>>();
    require(
        listed.len() == 101 && unique.len() == 101 && unique == key_ids.into_iter().collect(),
        format!(
            "same-second pagination was incomplete or duplicated: rows={}, unique={}",
            listed.len(),
            unique.len()
        ),
    )?;

    let error = key_store
        .list_project_api_keys(&project.id, Some(&other_account.api_key.id), 17)
        .await
        .expect_err("cross-project cursor must be rejected");
    require(
        error.status_code() == StatusCode::BAD_REQUEST,
        format!(
            "cross-project cursor returned {}, not 400",
            error.status_code()
        ),
    )
}

fn test_charge(tenant_id: &str, request_id: &str) -> UsageCharge {
    UsageCharge {
        tenant_id: tenant_id.to_string(),
        attribution: None,
        request_id: request_id.to_string(),
        admission_session_id: None,
        operation: "generation",
        provider_id: "openai-codex".to_string(),
        model: "gpt-image-2".to_string(),
        output_count: 1,
        billable_units: 1,
        billing_metric: BillingMetric::Output,
        limits: UsageLimits {
            five_hour_image_limit: 100,
            seven_day_image_limit: 100,
        },
    }
}

async fn assert_attribution_identity(
    pool: &PgPool,
    job_id: Uuid,
    project_id: &str,
    service_account_id: &str,
    api_key_id: &str,
) -> TestResult {
    let stored: (String, String, String) = sqlx::query_as(
        r#"
        SELECT project_id, service_account_id, api_key_id
        FROM job_auth_attributions
        WHERE job_id = $1 AND auth_kind = 'api_key'
        "#,
    )
    .bind(job_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read admitted attribution: {error}"))?;
    require(
        stored
            == (
                project_id.to_string(),
                service_account_id.to_string(),
                api_key_id.to_string(),
            ),
        format!("admitted attribution changed: {stored:?}"),
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

fn require_sqlstate(error: &sqlx::Error, expected: &str, operation: &str) -> TestResult {
    let actual = error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .map(|code| code.into_owned());
    require(
        actual.as_deref() == Some(expected),
        format!("{operation} returned SQLSTATE {actual:?}, expected {expected}"),
    )
}

fn debug_gateway(context: &'static str) -> impl FnOnce(ImageGatewayError) -> String + Copy {
    move |error| format!("{context}: {error:?}")
}

fn require(condition: bool, message: String) -> TestResult {
    if condition { Ok(()) } else { Err(message) }
}

fn test_keyring() -> ApiKeyKeyring {
    ApiKeyKeyring::new(1, [(1, vec![0x41; 32])]).expect("test keyring must be valid")
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
            eprintln!("skipping PostgreSQL project attribution test: {DATABASE_URL} is not set");
            return Ok(None);
        };
        let suffix = Uuid::new_v4().simple().to_string();
        let schema = format!("image_gateway_project_attribution_test_{suffix}");
        let application_prefix = format!("project_attr_test_{}", &suffix[..12]);
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
