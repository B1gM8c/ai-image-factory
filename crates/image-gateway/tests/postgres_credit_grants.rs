use std::env;

use gpt_image_2_gateway::{
    CreditGrantService, PostgresCreditGrantService,
    credit_grants::{
        CreateCreditGrantRequest, CreditGrantActor, ListCreditGrantsRequest,
        RevokeCreditGrantRequest,
    },
    database::{connect_test_pool_with_search_path, run_migrations},
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

#[tokio::test]
async fn credit_grant_issue_replay_revoke_and_ledger_are_atomic() -> TestResult {
    let Some(schema) = TestSchema::new(4).await? else {
        return Ok(());
    };
    let result = credit_grant_lifecycle_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    cleanup?;
    result
}

async fn credit_grant_lifecycle_case(pool: &PgPool) -> TestResult {
    run_migrations(pool).await.map_err(debug_error)?;
    let now: i64 =
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let tenant_id = format!("org_credit_grant_{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO identity_organizations (
            organization_id, display_name, organization_kind,
            owner_user_id, created_at_ms, updated_at_ms
        )
        VALUES ($1, 'Credit Grant Test', 'system', NULL, $2, $2)
        "#,
    )
    .bind(&tenant_id)
    .bind(now)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let service = PostgresCreditGrantService::new(pool.clone());
    let actor = CreditGrantActor {
        user_id: Uuid::new_v4(),
        session_id: Uuid::new_v4(),
    };
    let request = CreateCreditGrantRequest {
        organization_id: tenant_id.clone(),
        currency: "usd".to_string(),
        amount_micros: "25000000".to_string(),
        expires_at_ms: now + 86_400_000,
        source_reference: "launch-credit-001".to_string(),
        reason: "Launch promotion".to_string(),
    };
    let issued = service
        .create("issue-key", actor, request.clone())
        .await
        .map_err(debug_error)?;
    require(issued.organization_id == tenant_id, "tenant must match")?;
    require(issued.currency == "USD", "currency must normalize")?;
    require(
        issued.available_micros == "25000000",
        "issued grant must be fully available",
    )?;
    require(issued.state == "active", "issued grant must be active")?;

    let replay = service
        .create("issue-key", actor, request.clone())
        .await
        .map_err(debug_error)?;
    require(replay == issued, "exact issue replay must be stable")?;
    let conflicting = service
        .create(
            "issue-key",
            actor,
            CreateCreditGrantRequest {
                amount_micros: "25000001".to_string(),
                ..request
            },
        )
        .await;
    require(
        conflicting
            .as_ref()
            .is_err_and(|error| error.status_code().as_u16() == 409),
        "same idempotency key with a different body must conflict",
    )?;

    let listed = service
        .list(ListCreditGrantsRequest {
            organization_id: Some(tenant_id.clone()),
            currency: Some("USD".to_string()),
            ..ListCreditGrantsRequest::default()
        })
        .await
        .map_err(debug_error)?;
    require(listed.data.len() == 1, "list must contain one grant")?;
    require(
        listed.data[0].organization_display_name.as_deref() == Some("Credit Grant Test"),
        "platform list must include the authoritative organization name",
    )?;
    require(
        listed.summary.available_micros == "25000000",
        "summary must expose the available balance",
    )?;

    let grant_id = Uuid::parse_str(&issued.grant_id).map_err(debug_error)?;
    let revoked = service
        .revoke(
            grant_id,
            "revoke-key",
            actor,
            RevokeCreditGrantRequest {
                reason: "Promotion canceled".to_string(),
            },
        )
        .await
        .map_err(debug_error)?;
    require(revoked.state == "revoked", "grant must be revoked")?;
    require(
        revoked.available_micros == "0" && revoked.revoked_micros == "25000000",
        "revocation must retire the full available balance",
    )?;
    let replayed_revoke = service
        .revoke(
            grant_id,
            "revoke-key",
            actor,
            RevokeCreditGrantRequest {
                reason: "Promotion canceled".to_string(),
            },
        )
        .await
        .map_err(debug_error)?;
    require(
        replayed_revoke == revoked,
        "exact revocation replay must be stable",
    )?;

    let expiring = service
        .create(
            "expiring-grant-key",
            actor,
            CreateCreditGrantRequest {
                organization_id: tenant_id.clone(),
                currency: "USD".to_string(),
                amount_micros: "3000000".to_string(),
                expires_at_ms: now + 250,
                source_reference: "expiring-credit-001".to_string(),
                reason: "Short expiration test".to_string(),
            },
        )
        .await
        .map_err(debug_error)?;
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    require(
        service.expire_due(10).await.map_err(debug_error)? == 1,
        "expiration pass must retire one due grant",
    )?;
    let expired = service
        .get(Uuid::parse_str(&expiring.grant_id).map_err(debug_error)?)
        .await
        .map_err(debug_error)?;
    require(
        expired.state == "expired"
            && expired.available_micros == "0"
            && expired.expired_micros == "3000000",
        format!("expired grant has invalid counters: {expired:?}").as_str(),
    )?;

    let (transactions, balanced, sealed): (i64, bool, bool) = sqlx::query_as(
        r#"
        SELECT COUNT(DISTINCT transaction.transaction_id)::BIGINT,
               COALESCE(BOOL_AND(balance.amount_micros = 0), FALSE),
               COALESCE(BOOL_AND(seal.transaction_id IS NOT NULL), FALSE)
        FROM ledger_transactions transaction
        JOIN LATERAL (
            SELECT SUM(posting.amount_micros)::BIGINT AS amount_micros
            FROM ledger_postings posting
            WHERE posting.transaction_id = transaction.transaction_id
        ) balance ON TRUE
        LEFT JOIN ledger_transaction_seals seal
          ON seal.transaction_id = transaction.transaction_id
        WHERE transaction.source_credit_grant_event_id IS NOT NULL
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        transactions == 4,
        "issue, revoke, issue, and expiration need four ledger entries",
    )?;
    require(balanced, "credit grant ledger entries must balance")?;
    require(sealed, "credit grant ledger entries must be sealed")?;

    let mutation = sqlx::query("DELETE FROM credit_grant_events WHERE grant_id = $1")
        .bind(grant_id)
        .execute(pool)
        .await;
    require(
        mutation.is_err(),
        "credit grant events must remain append-only",
    )?;
    Ok(())
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
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
            eprintln!("skipping PostgreSQL credit grant test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("image_gateway_test_{}", Uuid::new_v4().simple());
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
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(debug_error)?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::raw_sql(AssertSqlSafe(format!(
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
