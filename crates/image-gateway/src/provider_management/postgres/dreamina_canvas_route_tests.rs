use super::*;
use crate::{
    database::{connect_test_pool_with_search_path, run_migrations},
    dreamina_canvas::{CANVAS_IMAGE_MODEL, CANVAS_PROVIDER_ID, CANVAS_SUBMIT_COMMAND_SCHEMA},
    executor::{
        ExecutionProfileProvisioning, provision_dreamina_canvas_execution_profile,
        validate_dreamina_canvas_home,
    },
};
use sqlx::AssertSqlSafe;

#[tokio::test]
async fn dreamina_canvas_provisioned_account_routes_only_its_compatible_model() {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "TEST_DATABASE_URL required in CI"
        );
        eprintln!("skipping Canvas PostgreSQL test: TEST_DATABASE_URL absent");
        return;
    };
    let schema = format!("canvas_provision_test_{}", Uuid::new_v4().simple());
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

    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("canvas");
    let other_home = root.path().join("other");
    create_private_directory(&home).unwrap();
    create_private_directory(&other_home).unwrap();
    let provisioning = ExecutionProfileProvisioning {
        profile_key: "canvas-test".into(),
        credential_pool_key: "canvas-test-pool".into(),
        provider_account_key: "canvas-test-account".into(),
        credential_ref: "managed.canvas-test.1".into(),
        credential_revision: 1,
        credential_auth_sha256: "c".repeat(64),
        max_concurrency: 1,
    };
    let (first, second) = tokio::join!(
        provision_dreamina_canvas_execution_profile(&pool, &provisioning, &home),
        provision_dreamina_canvas_execution_profile(&pool, &provisioning, &home),
    );
    let account = first.unwrap();
    assert_eq!(
        account.execution_profile_id,
        second.unwrap().execution_profile_id
    );
    let validate_home = |path| {
        validate_dreamina_canvas_home(
            &pool,
            account.provider_account_id,
            path,
            &provisioning.credential_auth_sha256,
        )
    };
    validate_home(&home).await.unwrap();
    assert!(validate_home(&other_home).await.is_err());

    let service = PostgresProviderManagementService::new(
        pool.clone(),
        root.path().into(),
        "/not/a/real/codex".into(),
    );
    let mut request: CreateProviderRouteRequest = serde_json::from_value(serde_json::json!({
        "route_key": "canvas-test-route",
        "display_name": "Canvas test",
        "provider_id": CANVAS_PROVIDER_ID,
        "operation_id": "images.generations",
        "members": [{ "provider_account_id": account.provider_account_id }],
    }))
    .unwrap();
    let route = service.create_route(request.clone()).await.unwrap();
    assert_eq!(route.command_schema, CANVAS_SUBMIT_COMMAND_SCHEMA);
    let [mapping] = route.model_mappings.as_slice() else {
        panic!("Canvas must advertise exactly one model mapping");
    };
    assert_eq!(mapping.api_profile, "dreamina-cli-images-v1");
    assert_eq!(mapping.public_model_id, "5.5");
    assert_eq!(mapping.execution_model_id, CANVAS_IMAGE_MODEL);

    request.route_key = "native-invalid".into();
    request.provider_id = DREAMINA_PROVIDER_ID.into();
    let error = service.create_route(request).await.unwrap_err();
    assert_eq!(error.error_code(), Some("invalid_provider_route_members"));

    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .execute(&pool)
        .await
        .unwrap();
}
