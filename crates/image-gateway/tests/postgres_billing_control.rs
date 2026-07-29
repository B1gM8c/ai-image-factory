use std::env;

use gpt_image_2_gateway::{
    BillingAccountControlService, PostgresBillingAccountControlService,
    billing_control::{
        BillingControlActor, ListBillingAccountsRequest, UpdateBillingAccountLimitRequest,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn billing_account_limits_are_versioned_audited_and_database_guarded() -> TestResult {
    let Some(schema) = TestSchema::new(8).await? else {
        return Ok(());
    };
    let result = billing_control_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn billing_control_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("billing control migrations failed: {error:?}"))?;
    let tenant_id = format!("billing-control-{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
            organization_id, display_name, organization_kind,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'Billing control test', 'system', 1, 1)
        "#,
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let second_tenant_id = format!("{tenant_id}-z");
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
            organization_id, display_name, organization_kind,
            created_at_ms, updated_at_ms
        )
        VALUES ($1, 'Billing control test second', 'system', 1, 1)
        "#,
    )
    .bind(&second_tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let service = PostgresBillingAccountControlService::new(pool.clone());
    let actor = BillingControlActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    let empty = service
        .get_account(&tenant_id, "usd")
        .await
        .map_err(debug_error)?;
    require(
        !empty.configured
            && empty.currency == "USD"
            && empty.credit_limit_micros == "0"
            && empty.control_version == "0",
        "fresh organization invented a configured billing account",
    )?;
    let first_page = service
        .list_accounts(ListBillingAccountsRequest {
            currency: Some("usd".to_string()),
            query: Some("billing-control-".to_string()),
            after: None,
            limit: Some(1),
        })
        .await
        .map_err(debug_error)?;
    require(
        first_page.data.len() == 1
            && first_page.has_more
            && first_page.next_after.as_deref() == Some(tenant_id.as_str()),
        "organization billing accounts did not use stable keyset pagination",
    )?;
    let second_page = service
        .list_accounts(ListBillingAccountsRequest {
            currency: Some("USD".to_string()),
            query: Some("billing-control-".to_string()),
            after: first_page.next_after,
            limit: Some(1),
        })
        .await
        .map_err(debug_error)?;
    require(
        second_page.data.len() == 1
            && second_page.data[0].organization_id == second_tenant_id
            && !second_page.has_more,
        "organization billing account cursor skipped or duplicated a row",
    )?;

    let created = service
        .update_limit(
            &tenant_id,
            "usd",
            actor,
            limit_request("5000000", "0", "Initial production credit approval"),
        )
        .await
        .map_err(debug_error)?;
    require(
        created.configured
            && created.currency == "USD"
            && created.credit_limit_micros == "5000000"
            && created.available_micros == "5000000"
            && created.control_version == "1",
        "first credit-limit control did not create version one",
    )?;
    let configured_accounts = service
        .list_accounts(ListBillingAccountsRequest {
            currency: Some("USD".to_string()),
            query: Some(tenant_id.clone()),
            after: None,
            limit: Some(10),
        })
        .await
        .map_err(debug_error)?;
    require(
        configured_accounts.data.iter().any(|item| {
            item.organization_id == tenant_id
                && item.account.configured
                && item.account.control_version == "1"
        }),
        "organization billing account list did not project configured control state",
    )?;
    let initial_history: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM billing_account_limit_changes WHERE tenant_id = $1",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let initial_audit: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM identity_audit_events
        WHERE action = 'billing.account.credit_limit.update'
          AND resource_id = $1
        "#,
    )
    .bind(format!("{tenant_id}:USD"))
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        initial_history == 1 && initial_audit == 1,
        "credit-limit creation omitted immutable history or identity audit",
    )?;

    let stale = service
        .update_limit(
            &tenant_id,
            "USD",
            actor,
            limit_request("6000000", "0", "Stale operator update"),
        )
        .await
        .expect_err("stale credit-limit control version must conflict");
    require(
        stale.status_code().as_u16() == 409,
        "stale credit-limit update did not return conflict",
    )?;

    let first_service = service.clone();
    let second_service = service.clone();
    let first_tenant = tenant_id.clone();
    let second_tenant = tenant_id.clone();
    let first = tokio::spawn(async move {
        first_service
            .update_limit(
                &first_tenant,
                "USD",
                actor,
                limit_request("7000000", "1", "Concurrent approval A"),
            )
            .await
    });
    let second = tokio::spawn(async move {
        second_service
            .update_limit(
                &second_tenant,
                "USD",
                actor,
                limit_request("8000000", "1", "Concurrent approval B"),
            )
            .await
    });
    let first = first.await.map_err(debug_error)?;
    let second = second.await.map_err(debug_error)?;
    require(
        matches!(
            (&first, &second),
            (Ok(account), Err(error)) | (Err(error), Ok(account))
                if account.control_version == "2" && error.status_code().as_u16() == 409
        ),
        "two concurrent credit-limit updates both committed or lost conflict semantics",
    )?;

    sqlx::query(
        r#"
        UPDATE billing_accounts
        SET held_micros = 1000000,
            captured_micros = 2000000
        WHERE tenant_id = $1 AND currency = 'USD'
        "#,
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    let below_committed = service
        .update_limit(
            &tenant_id,
            "USD",
            actor,
            limit_request("2999999", "2", "Unsafe credit reduction"),
        )
        .await
        .expect_err("credit limit below held and captured spend must conflict");
    require(
        below_committed.status_code().as_u16() == 409,
        "credit limit below committed spend was accepted",
    )?;

    let direct_update = sqlx::query(
        r#"
        UPDATE billing_accounts
        SET credit_limit_micros = credit_limit_micros + 1
        WHERE tenant_id = $1 AND currency = 'USD'
        "#,
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .expect_err("direct credit-limit update must be rejected");
    require(
        direct_update
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref()
            == Some("55000"),
        "direct credit-limit update did not fail with the invariant SQLSTATE",
    )?;

    let history_update = sqlx::query(
        r#"
        UPDATE billing_account_limit_changes
        SET reason = 'Tampered reason'
        WHERE tenant_id = $1 AND control_version = 1
        "#,
    )
    .bind(&tenant_id)
    .execute(pool)
    .await
    .expect_err("credit-limit history must be immutable");
    require(
        history_update
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref()
            == Some("55000"),
        "credit-limit history mutation did not fail with the invariant SQLSTATE",
    )?;

    let final_account = service
        .get_account(&tenant_id, "USD")
        .await
        .map_err(debug_error)?;
    let final_history: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM billing_account_limit_changes WHERE tenant_id = $1",
    )
    .bind(&tenant_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    let final_audit: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM identity_audit_events
        WHERE action = 'billing.account.credit_limit.update'
          AND resource_id = $1
        "#,
    )
    .bind(format!("{tenant_id}:USD"))
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        final_account.control_version == "2"
            && final_account.held_micros == "1000000"
            && final_account.captured_micros == "2000000"
            && final_history == 2
            && final_audit == 2,
        "rejected writes changed the account or emitted false control evidence",
    )
}

fn limit_request(
    credit_limit_micros: &str,
    expected_control_version: &str,
    reason: &str,
) -> UpdateBillingAccountLimitRequest {
    UpdateBillingAccountLimitRequest {
        credit_limit_micros: credit_limit_micros.to_string(),
        expected_control_version: expected_control_version.to_string(),
        reason: reason.to_string(),
    }
}

fn require(condition: bool, message: &str) -> TestResult {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
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
                return Err("TEST_DATABASE_URL must be set in CI".to_string());
            }
            eprintln!("skipping PostgreSQL billing control test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("billing_control_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(debug_error)?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(debug_error)?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because database {database_name:?} is not a test database"
            ));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(debug_error);
        self.pool.close().await;
        result.map(|_| ())
    }
}
