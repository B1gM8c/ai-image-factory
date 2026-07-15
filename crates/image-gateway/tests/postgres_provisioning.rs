use std::env;

use gpt_image_2_gateway::{
    CodexExecutionProfileProvisioning, CodexProfileProvisioningError,
    ExecutorExecutionProfileStore, PostgresExecutorSubmissionStore,
    database::{connect_test_pool_with_search_path, run_migrations},
    provision_codex_execution_profile,
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn exact_enabled_provisioning_is_repeatable() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = fixture("repeatable");
        let first = provision_codex_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let replay = provision_codex_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        require(
            first == replay,
            "exact provisioning changed durable identities",
        )?;
        let loaded = PostgresExecutorSubmissionStore::new(database.pool.clone())
            .load_execution_profile(&provisioning.profile_key)
            .await
            .map_err(debug_error)?;
        require(
            loaded.execution_profile_id == first.execution_profile_id
                && loaded.credential_pool_id == first.credential_pool_id
                && loaded.provider_account_id == first.provider_account_id
                && loaded.resource_policy_id == first.resource_policy_id
                && loaded.resource_policy_revision == first.resource_policy_revision
                && loaded.credential_ref == provisioning.credential_ref
                && loaded.credential_revision == provisioning.credential_revision
                && loaded.credential_auth_sha256 == provisioning.credential_auth_sha256
                && loaded.max_concurrency == provisioning.max_concurrency,
            format!("runtime loader changed the provisioned profile: {loaded:?}"),
        )?;
        let counts: (i64, i64, i64, i64, i32) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles),
                   (SELECT allocated_count FROM executor_resource_policies
                    WHERE resource_policy_id = $1 AND revision = $2)
            "#,
        )
        .bind(first.resource_policy_id)
        .bind(first.resource_policy_revision)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1, 0),
            format!("exact replay changed the durable graph: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn disabled_identity_at_every_layer_remains_a_kill_switch() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        for layer in ["pool", "account", "policy", "profile"] {
            let provisioning = fixture(&format!("disabled-{layer}"));
            let provisioned = provision_codex_execution_profile(&database.pool, &provisioning)
                .await
                .map_err(debug_error)?;
            match layer {
                "pool" => {
                    sqlx::query(
                        "UPDATE provider_credential_pools SET state = 'disabled' WHERE credential_pool_id = $1",
                    )
                    .bind(provisioned.credential_pool_id)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                "account" => {
                    sqlx::query(
                        "UPDATE provider_accounts SET state = 'disabled' WHERE provider_account_id = $1",
                    )
                    .bind(provisioned.provider_account_id)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                "policy" => {
                    sqlx::query(
                        "UPDATE executor_resource_policies SET state = 'disabled' WHERE resource_policy_id = $1 AND revision = $2",
                    )
                    .bind(provisioned.resource_policy_id)
                    .bind(provisioned.resource_policy_revision)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                "profile" => {
                    sqlx::query(
                        "UPDATE provider_execution_profiles SET state = 'disabled' WHERE execution_profile_id = $1",
                    )
                    .bind(provisioned.execution_profile_id)
                    .execute(&database.pool)
                    .await
                    .map_err(debug_error)?;
                }
                _ => unreachable!(),
            }
            require(
                provision_codex_execution_profile(&database.pool, &provisioning).await
                    == Err(CodexProfileProvisioningError::Conflict),
                format!("disabled {layer} identity was implicitly re-enabled"),
            )?;
            let state: String = match layer {
                "pool" => sqlx::query_scalar(
                    "SELECT state FROM provider_credential_pools WHERE credential_pool_id = $1",
                )
                .bind(provisioned.credential_pool_id)
                .fetch_one(&database.pool)
                .await,
                "account" => sqlx::query_scalar(
                    "SELECT state FROM provider_accounts WHERE provider_account_id = $1",
                )
                .bind(provisioned.provider_account_id)
                .fetch_one(&database.pool)
                .await,
                "policy" => sqlx::query_scalar(
                    "SELECT state FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = $2",
                )
                .bind(provisioned.resource_policy_id)
                .bind(provisioned.resource_policy_revision)
                .fetch_one(&database.pool)
                .await,
                "profile" => sqlx::query_scalar(
                    "SELECT state FROM provider_execution_profiles WHERE execution_profile_id = $1",
                )
                .bind(provisioned.execution_profile_id)
                .fetch_one(&database.pool)
                .await,
                _ => unreachable!(),
            }
            .map_err(debug_error)?;
            require(
                state == "disabled",
                format!("disabled {layer} identity changed state to {state}"),
            )?;
        }
        Ok(())
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn conflicting_credential_identity_rolls_back_without_partial_rows() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = fixture("conflict");
        let first = provision_codex_execution_profile(&database.pool, &provisioning)
            .await
            .map_err(debug_error)?;
        let mut conflicting = provisioning.clone();
        conflicting.credential_auth_sha256 = "b".repeat(64);
        require(
            provision_codex_execution_profile(&database.pool, &conflicting).await
                == Err(CodexProfileProvisioningError::Conflict),
            "credential digest drift did not fail closed",
        )?;
        let stored: (String, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT account.credential_auth_sha256,
                   (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            FROM provider_accounts account
            WHERE account.provider_account_id = $1
            "#,
        )
        .bind(first.provider_account_id)
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            stored == ("a".repeat(64), 1, 1, 1, 1),
            format!("conflicting provisioning leaked partial state: {stored:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_exact_provisioning_has_one_durable_identity_graph() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let provisioning = fixture("concurrent");
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let pool = database.pool.clone();
            let provisioning = provisioning.clone();
            tasks.push(tokio::spawn(async move {
                provision_codex_execution_profile(&pool, &provisioning).await
            }));
        }
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.map_err(debug_error)?.map_err(debug_error)?);
        }
        let first = results
            .first()
            .ok_or_else(|| "concurrent provisioning returned no result".to_string())?;
        require(
            results.iter().all(|result| result == first),
            format!("concurrent provisioning returned divergent identities: {results:?}"),
        )?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1),
            format!("concurrent provisioning duplicated durable rows: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn concurrent_conflicting_provisioning_commits_one_complete_winner() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let first = fixture("concurrent-conflict");
        let mut second = first.clone();
        second.credential_auth_sha256 = "b".repeat(64);
        let first_task = {
            let pool = database.pool.clone();
            tokio::spawn(async move { provision_codex_execution_profile(&pool, &first).await })
        };
        let second_task = {
            let pool = database.pool.clone();
            tokio::spawn(async move { provision_codex_execution_profile(&pool, &second).await })
        };
        let results = [
            first_task.await.map_err(debug_error)?,
            second_task.await.map_err(debug_error)?,
        ];
        require(
            results.iter().filter(|result| result.is_ok()).count() == 1
                && results
                    .iter()
                    .filter(|result| {
                        result.as_ref().err() == Some(&CodexProfileProvisioningError::Conflict)
                    })
                    .count()
                    == 1,
            format!("concurrent conflicting provisioning did not choose one winner: {results:?}"),
        )?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1),
            format!("concurrent conflict produced a hybrid graph: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

#[tokio::test]
async fn late_profile_conflict_rolls_back_new_dependency_rows() -> TestResult {
    let Some(database) = TestDatabase::new().await? else {
        return Ok(());
    };
    let result = async {
        let existing = fixture("existing-profile");
        provision_codex_execution_profile(&database.pool, &existing)
            .await
            .map_err(debug_error)?;
        let mut conflicting = fixture("new-dependencies");
        conflicting.profile_key = existing.profile_key;
        require(
            provision_codex_execution_profile(&database.pool, &conflicting).await
                == Err(CodexProfileProvisioningError::Conflict),
            "late profile identity conflict did not fail closed",
        )?;
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT (SELECT COUNT(*) FROM provider_credential_pools),
                   (SELECT COUNT(*) FROM provider_accounts),
                   (SELECT COUNT(*) FROM executor_resource_policies),
                   (SELECT COUNT(*) FROM provider_execution_profiles)
            "#,
        )
        .fetch_one(&database.pool)
        .await
        .map_err(debug_error)?;
        require(
            counts == (1, 1, 1, 1),
            format!("late profile conflict leaked dependency rows: {counts:?}"),
        )
    }
    .await;
    combine(result, database.cleanup().await)
}

fn fixture(suffix: &str) -> CodexExecutionProfileProvisioning {
    CodexExecutionProfileProvisioning {
        profile_key: format!("openai-codex-generation-v1-{suffix}"),
        credential_pool_key: format!("openai-codex-pool-{suffix}"),
        provider_account_key: format!("openai-codex-account-{suffix}"),
        credential_ref: format!("mounted.openai-codex.{suffix}.1"),
        credential_revision: 1,
        credential_auth_sha256: "a".repeat(64),
        max_concurrency: 3,
    }
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
            eprintln!("skipping PostgreSQL provisioning test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_provisioning_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&url, 16, &schema)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!("refusing DDL in non-test database {database_name}"));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        if let Err(error) = run_migrations(&pool).await {
            let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return Err(format!("migration failed: {error:?}"));
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
        .map_err(debug_error);
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

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}
