use super::*;
use crate::{
    DreaminaExecutionProfileProvisioning,
    database::{connect_test_pool_with_search_path, run_migrations},
    provision_dreamina_execution_profile,
};
use sqlx::AssertSqlSafe;

#[tokio::test]
async fn dreamina_login_session_preserves_original_account_and_serializes_reauthorization() {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "TEST_DATABASE_URL required in CI"
        );
        eprintln!("skipping Dreamina login PostgreSQL test: TEST_DATABASE_URL absent");
        return;
    };
    let schema = format!("dreamina_login_test_{}", Uuid::new_v4().simple());
    let pool = connect_test_pool_with_search_path(&url, 4, &schema)
        .await
        .unwrap();
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        database.to_ascii_lowercase().contains("test"),
        "refusing non-test database"
    );
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
        .execute(&pool)
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let service = PostgresProviderManagementService::new(
        pool.clone(),
        "/tmp/dreamina-login-test-unused-home".into(),
        "/not/a/real/codex".into(),
    );
    let mut accounts = Vec::new();
    for suffix in ["a", "b"] {
        let provisioned = provision_dreamina_execution_profile(
            &pool,
            &DreaminaExecutionProfileProvisioning {
                profile_key: format!("dreamina-login-{suffix}"),
                credential_pool_key: format!("dreamina-login-pool-{suffix}"),
                provider_account_key: format!("dreamina-login-account-{suffix}"),
                credential_ref: format!("managed.dreamina-login.{suffix}.1"),
                credential_revision: 1,
                credential_auth_sha256: "c".repeat(64),
                max_concurrency: 1,
            },
        )
        .await
        .unwrap();
        accounts.push(provisioned.provider_account_id);
    }
    let request = |provider_account_id| StartProviderLoginRequest {
        provider_id: DREAMINA_PROVIDER_ID.into(),
        display_name: "  Original account  ".into(),
        operation_ids: vec!["images.generations".into()],
        provider_account_id,
        login_method: CodexLoginMethod::DeviceCode,
        max_concurrency: 7,
    };
    let now = 1_792_953_083_054_i64;
    for (index, account) in [None, None, Some(accounts[0]), Some(accounts[1])]
        .into_iter()
        .enumerate()
    {
        let session = Uuid::new_v4();
        let account_key = format!("dreamina-login-session-{index}");
        let home = PathBuf::from(format!("/tmp/dreamina-login-unused-{session}"));
        service
            .insert_dreamina_login_session(session, &account_key, &home, &request(account), now)
            .await
            .unwrap();
        let saved: (
            String,
            String,
            String,
            String,
            String,
            i32,
            Option<Uuid>,
            i64,
            i64,
            i64,
        ) = sqlx::query_as(
            "SELECT provider_id, account_key, display_name, environment_ref, login_method, \
                 max_concurrency, provider_account_id, expires_at_ms, created_at_ms, updated_at_ms \
                 FROM provider_account_login_sessions WHERE login_session_id = $1",
        )
        .bind(session)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            saved,
            (
                DREAMINA_PROVIDER_ID.into(),
                account_key,
                "Original account".into(),
                home.to_string_lossy().into_owned(),
                "device_code".into(),
                7,
                account,
                now + LOGIN_TTL.as_millis() as i64,
                now,
                now,
            )
        );
    }
    let duplicate = service
        .insert_dreamina_login_session(
            Uuid::new_v4(),
            "dreamina-login-duplicate",
            Path::new("/tmp/dreamina-login-unused-duplicate"),
            &request(Some(accounts[0])),
            now,
        )
        .await
        .expect_err("same original account must not start a second active authorization");
    assert_eq!(
        map_login_session_insert(duplicate).error_code(),
        Some("provider_account_reauthorization_in_progress")
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM provider_account_login_sessions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 4,
        "rejected duplicate must not leave a partial session"
    );
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}
