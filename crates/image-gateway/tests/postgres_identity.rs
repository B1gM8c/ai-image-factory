use std::{env, sync::Arc};

use factory_identity::{
    AccessTokenCodec, AuthPolicy, IdentityError, IdentityService, LoginRequest, RefreshRequest,
    RefreshTokenKeyring,
};
use gpt_image_2_gateway::{
    database::{connect_test_pool_with_search_path, run_migrations},
    identity::{PostgresIdentityMaintenanceStore, PostgresIdentityRepository},
};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgJ6r5c63M0tPZV05C
Y0U72GBHm9iqV7QaUgFxk/9dBn+hRANCAAT5ufmoZxTrAkeOwJFSjVcbQ1Pvl2sw
892/nV1rvRJwDokKy+s00P46StleDgXLe9hOly8yM81frZfcMeI1krz+
-----END PRIVATE KEY-----
"#;
const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE+bn5qGcU6wJHjsCRUo1XG0NT75dr
MPPdv51da70ScA6JCsvrNND+OkrZXg4Fy3vYTpcvMjPNX62X3DHiNZK8/g==
-----END PUBLIC KEY-----
"#;
const PASSWORD: &str = "correct horse battery staple";

#[tokio::test]
async fn postgres_identity_rotation_logout_and_throttle_are_authoritative() -> TestResult {
    let Some(schema) = TestSchema::new(8).await? else {
        return Ok(());
    };
    let result = identity_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn identity_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("identity migrations failed: {error:?}"))?;
    let policy = AuthPolicy {
        max_account_login_attempts: 3,
        max_global_login_attempts: 100,
        ..AuthPolicy::default()
    };
    let access_tokens = AccessTokenCodec::new(
        "test-key",
        PRIVATE_KEY,
        [("test-key".to_string(), PUBLIC_KEY.to_vec())],
        "https://identity.test",
        "urn:aif:admin",
        &policy,
    )
    .map_err(|error| format!("access token codec failed: {error}"))?;
    let refresh_tokens = RefreshTokenKeyring::new(1, [(1, vec![7; 32])])
        .map_err(|error| format!("refresh keyring failed: {error}"))?;
    let service = Arc::new(
        IdentityService::new(
            Arc::new(PostgresIdentityRepository::new(pool.clone())),
            access_tokens,
            refresh_tokens,
            policy.clone(),
        )
        .map_err(|error| format!("identity service failed: {error}"))?,
    );
    let inserted = service
        .bootstrap_admin(
            "owner@example.com".to_string(),
            "Platform Owner".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(|error| format!("bootstrap failed: {error}"))?;
    require(inserted, "bootstrap must insert the first owner")?;

    let owner = service
        .list_users(None, 100)
        .await
        .map_err(|error| format!("owner access list failed: {error}"))?
        .into_iter()
        .find(|user| user.email == "owner@example.com")
        .ok_or_else(|| "bootstrapped owner must be listed".to_string())?;
    assert_personal_workspace(&owner)?;

    let member = service
        .create_member_user(
            "member@example.com".to_string(),
            "Member User".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(|error| format!("member creation failed: {error}"))?;
    require(
        member.roles == ["member"] && member.scopes == ["workspace:read", "workspace:write"],
        "member creation must use the fixed workspace role and scopes",
    )?;
    assert_personal_workspace(&member)?;
    require(
        owner.organizations[0].organization_id != member.organizations[0].organization_id,
        "two users must have different personal workspaces",
    )?;
    require(
        owner.projects[0].project_id != member.projects[0].project_id,
        "two users must have different default projects",
    )?;
    require(
        service
            .get_user_access(member.user_id)
            .await
            .map_err(|error| format!("member access lookup failed: {error}"))?
            == Some(member.clone()),
        "single-user access lookup must return the requested user",
    )?;
    require(
        service
            .get_user_access(Uuid::new_v4())
            .await
            .map_err(|error| format!("missing user lookup failed: {error}"))?
            .is_none(),
        "single-user access lookup must return None for an unknown user",
    )?;
    let listed = service
        .list_users(None, 100)
        .await
        .map_err(|error| format!("user access list failed: {error}"))?;
    require(
        listed.len() == 2
            && listed
                .iter()
                .all(|user| user.organizations.len() == 1 && user.projects.len() == 1),
        "user listing must include each personal workspace and default project",
    )?;
    let after_member = service
        .list_users(Some("member@example.com"), 100)
        .await
        .map_err(|error| format!("user cursor list failed: {error}"))?;
    require(
        after_member.len() == 1 && after_member[0].email == owner.email,
        "user listing cursor must be exclusive and repository-backed",
    )?;
    assert_provider_account_ownership_defaults(pool).await?;
    let member_login = login(&service, "member@example.com", PASSWORD, &policy).await?;
    let member_principal = service
        .authenticate_access(&member_login.access_token)
        .await
        .map_err(|error| format!("member access authentication failed: {error}"))?;
    require(
        member_principal.email == member.email
            && member_principal.display_name == member.display_name
            && member_principal.organizations == member.organizations
            && member_principal.projects == member.projects,
        "authenticated principal must include reusable workspace memberships",
    )?;
    service
        .logout(&member_login.access_token)
        .await
        .map_err(|error| format!("member logout failed: {error}"))?;

    let first = login(&service, "owner@example.com", PASSWORD, &policy).await?;
    require(
        first.refresh_expires_in <= policy.session_idle_ttl_seconds,
        "refresh cookie lifetime must not exceed the current idle expiry",
    )?;
    let rotated = service
        .refresh(RefreshRequest {
            refresh_token: first.refresh_token.clone(),
            client_id: policy.client_id.clone(),
        })
        .await
        .map_err(|error| format!("refresh failed: {error}"))?;
    require(
        rotated.refresh_token != first.refresh_token,
        "refresh must rotate the opaque secret",
    )?;
    let replay = service
        .refresh(RefreshRequest {
            refresh_token: first.refresh_token,
            client_id: policy.client_id.clone(),
        })
        .await;
    require(
        matches!(replay, Err(IdentityError::InvalidAuthentication)),
        "consumed refresh replay must fail",
    )?;
    require(
        service
            .authenticate_access(&rotated.access_token)
            .await
            .is_err(),
        "refresh replay must revoke the successor session",
    )?;

    let second = login(&service, "owner@example.com", PASSWORD, &policy).await?;
    service
        .logout_refresh(&second.refresh_token)
        .await
        .map_err(|error| format!("refresh logout failed: {error}"))?;
    service
        .logout_refresh(&second.refresh_token)
        .await
        .map_err(|error| format!("repeated refresh logout failed: {error}"))?;
    require(
        service
            .authenticate_access(&second.access_token)
            .await
            .is_err(),
        "refresh logout must revoke access immediately",
    )?;

    let third = login(&service, "owner@example.com", PASSWORD, &policy).await?;
    sqlx::query(
        "UPDATE identity_users SET authz_version = authz_version + 1, updated_at_ms = updated_at_ms + 1 WHERE normalized_email = 'owner@example.com'",
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to change authorization version: {error}"))?;
    require(
        service
            .authenticate_access(&third.access_token)
            .await
            .is_err(),
        "authorization version change must invalidate access",
    )?;
    let stale_refresh = service
        .refresh(RefreshRequest {
            refresh_token: third.refresh_token,
            client_id: policy.client_id.clone(),
        })
        .await;
    require(
        matches!(stale_refresh, Err(IdentityError::InvalidAuthentication)),
        "authorization version change must revoke the refresh family",
    )?;

    let concurrent = login(&service, "owner@example.com", PASSWORD, &policy).await?;
    let concurrent_request = RefreshRequest {
        refresh_token: concurrent.refresh_token,
        client_id: policy.client_id.clone(),
    };
    let (left, right) = tokio::join!(
        service.refresh(concurrent_request.clone()),
        service.refresh(concurrent_request),
    );
    let outcomes = [left, right];
    require(
        outcomes.iter().filter(|outcome| outcome.is_ok()).count() == 1,
        "concurrent PostgreSQL refresh must create exactly one successor",
    )?;
    require(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(IdentityError::InvalidAuthentication)))
            .count()
            == 1,
        "concurrent PostgreSQL refresh must reject the replay",
    )?;
    let winner = outcomes.into_iter().find_map(Result::ok).ok_or_else(|| {
        "concurrent PostgreSQL refresh did not return its single winner".to_string()
    })?;
    require(
        service
            .authenticate_access(&winner.access_token)
            .await
            .is_err(),
        "concurrent refresh replay must revoke the winning successor family",
    )?;

    assert_lineage_constraints(pool).await?;

    for _ in 0..4 {
        let result = service
            .login(LoginRequest {
                email: "missing@example.com".to_string(),
                password: "x".repeat(32),
                client_id: policy.client_id.clone(),
            })
            .await;
        require(
            matches!(result, Err(IdentityError::InvalidAuthentication)),
            "unknown identity attempts must remain generic",
        )?;
    }
    let blocked_accounts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM identity_login_throttles WHERE dimension = 'account' AND blocked_until_ms IS NOT NULL",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect account throttle: {error}"))?;
    require(
        blocked_accounts == 1,
        "shared account throttle must block the fourth attempt",
    )?;

    sqlx::query("DELETE FROM identity_login_throttles")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to reset login throttles: {error}"))?;
    let bounded_policy = AuthPolicy {
        max_account_login_attempts: 3,
        max_global_login_attempts: 3,
        ..AuthPolicy::default()
    };
    let bounded_service = IdentityService::new(
        Arc::new(PostgresIdentityRepository::new(pool.clone())),
        AccessTokenCodec::new(
            "bounded-key",
            PRIVATE_KEY,
            [("bounded-key".to_string(), PUBLIC_KEY.to_vec())],
            "https://identity-bounded.test",
            "urn:aif:admin",
            &bounded_policy,
        )
        .map_err(|error| format!("bounded access token codec failed: {error}"))?,
        RefreshTokenKeyring::new(1, [(1, vec![8; 32])])
            .map_err(|error| format!("bounded refresh keyring failed: {error}"))?,
        bounded_policy.clone(),
    )
    .map_err(|error| format!("bounded identity service failed: {error}"))?;
    for index in 0..20 {
        let result = bounded_service
            .login(LoginRequest {
                email: format!("missing-{index}@example.com"),
                password: "x".repeat(32),
                client_id: bounded_policy.client_id.clone(),
            })
            .await;
        require(
            matches!(result, Err(IdentityError::InvalidAuthentication)),
            "bounded random identity attempt must remain generic",
        )?;
    }
    let throttle_counts: (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE dimension = 'global'), count(*) FILTER (WHERE dimension = 'account') FROM identity_login_throttles",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect bounded login throttles: {error}"))?;
    require(
        throttle_counts == (1, 3),
        &format!(
            "global rejection must stop random-account write amplification: {throttle_counts:?}"
        ),
    )?;

    assert_identity_maintenance(pool, &winner.session.id).await
}

fn assert_personal_workspace(user: &factory_identity::IdentityUserAccess) -> TestResult {
    require(
        user.organizations.len() == 1
            && user.organizations[0].is_personal
            && user.organizations[0].role == "owner",
        "user must own exactly one personal workspace",
    )?;
    require(
        user.projects.len() == 1
            && user.projects[0].is_default
            && user.projects[0].role == "owner"
            && user.projects[0].organization_id == user.organizations[0].organization_id,
        "user must own one default project in the personal workspace",
    )
}

async fn assert_provider_account_ownership_defaults(pool: &PgPool) -> TestResult {
    let credential_pool_id = Uuid::new_v4();
    let provider_account_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools
          (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
        VALUES ($1, 'identity-test-pool', 'identity-test', 'enabled', 1, 1)
        "#,
    )
    .bind(credential_pool_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed identity provider pool: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_accounts
          (provider_account_id, credential_pool_id, provider_id, account_key,
           credential_ref, credential_revision, credential_auth_sha256,
           state, created_at_ms, updated_at_ms)
        VALUES (
            $1, $2, 'identity-test', 'identity-test-account',
            'identity-test-ref', 1,
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'enabled', 1, 1
        )
        "#,
    )
    .bind(provider_account_id)
    .bind(credential_pool_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed identity provider account: {error}"))?;
    let ownership: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT tenant_id, owner_user_id FROM provider_accounts WHERE provider_account_id = $1",
    )
    .bind(provider_account_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect provider account ownership: {error}"))?;
    require(
        ownership == ("tenant_default".to_string(), None),
        "unattributed provider accounts must remain in tenant_default with no owner",
    )
}

async fn assert_lineage_constraints(pool: &PgPool) -> TestResult {
    let (parent_id, parent_session, issued_at): (Uuid, Uuid, i64) = sqlx::query_as(
        r#"
        SELECT parent.token_id, parent.session_id, parent.issued_at_ms
        FROM identity_refresh_tokens parent
        WHERE EXISTS (
            SELECT 1 FROM identity_refresh_tokens child
            WHERE child.parent_token_id = parent.token_id
        )
        ORDER BY parent.issued_at_ms, parent.token_id
        LIMIT 1
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("refresh lineage must contain a consumed parent: {error}"))?;
    let branch = sqlx::query(
        "INSERT INTO identity_refresh_tokens (token_id, session_id, parent_token_id, secret_hash, pepper_version, issued_at_ms, expires_at_ms) VALUES ($1, $2, $3, $4, 1, $5, $6)",
    )
    .bind(Uuid::new_v4())
    .bind(parent_session)
    .bind(parent_id)
    .bind(vec![31_u8; 32])
    .bind(issued_at.saturating_add(1))
    .bind(issued_at.saturating_add(2))
    .execute(pool)
    .await;
    require(
        branch.is_err(),
        "database must reject a forked refresh lineage",
    )?;

    let foreign_parent: Uuid = sqlx::query_scalar(
        "SELECT token_id FROM identity_refresh_tokens WHERE session_id <> $1 ORDER BY issued_at_ms, token_id LIMIT 1",
    )
    .bind(parent_session)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("refresh lineage test requires two session families: {error}"))?;
    let cross_family = sqlx::query(
        "INSERT INTO identity_refresh_tokens (token_id, session_id, parent_token_id, secret_hash, pepper_version, issued_at_ms, expires_at_ms) VALUES ($1, $2, $3, $4, 1, $5, $6)",
    )
    .bind(Uuid::new_v4())
    .bind(parent_session)
    .bind(foreign_parent)
    .bind(vec![32_u8; 32])
    .bind(issued_at.saturating_add(1))
    .bind(issued_at.saturating_add(2))
    .execute(pool)
    .await;
    require(
        cross_family.is_err(),
        "database must reject a cross-family refresh parent",
    )
}

async fn assert_identity_maintenance(pool: &PgPool, session_id: &str) -> TestResult {
    let session_id = session_id
        .parse::<Uuid>()
        .map_err(|error| format!("winner session id is invalid: {error}"))?;
    sqlx::query(
        "UPDATE identity_session_families SET created_at_ms = 1, last_seen_at_ms = 1, idle_expires_at_ms = 2, absolute_expires_at_ms = 3, revoked_at_ms = 2, revoke_reason = COALESCE(revoke_reason, 'maintenance_test') WHERE session_id = $1",
    )
    .bind(session_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to age identity session: {error}"))?;
    sqlx::query(
        "UPDATE identity_login_throttles SET window_started_at_ms = 1, updated_at_ms = 1, blocked_until_ms = NULL",
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to age identity throttles: {error}"))?;
    sqlx::query("UPDATE identity_audit_events SET created_at_ms = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to age identity audit events: {error}"))?;

    let outcome = PostgresIdentityMaintenanceStore::new(pool.clone())
        .purge_expired(1, 1, 1, 1_000)
        .await
        .map_err(|error| format!("identity maintenance failed: {error:?}"))?;
    require(
        outcome.session_families >= 1 && outcome.login_throttles >= 1 && outcome.audit_events >= 1,
        &format!("identity maintenance did not purge every bounded category: {outcome:?}"),
    )?;
    let remaining_tokens: i64 =
        sqlx::query_scalar("SELECT count(*) FROM identity_refresh_tokens WHERE session_id = $1")
            .bind(session_id)
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to inspect purged refresh lineage: {error}"))?;
    require(
        remaining_tokens == 0,
        "session-family purge must cascade to refresh lineage",
    )
}

async fn login(
    service: &IdentityService,
    email: &str,
    password: &str,
    policy: &AuthPolicy,
) -> TestResult<factory_identity::TokenPair> {
    service
        .login(LoginRequest {
            email: email.to_string(),
            password: password.to_string(),
            client_id: policy.client_id.clone(),
        })
        .await
        .map_err(|error| format!("login failed: {error}"))
}

fn require(condition: bool, message: &str) -> TestResult {
    condition.then_some(()).ok_or_else(|| message.to_string())
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
            eprintln!("skipping PostgreSQL identity test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("identity_test_{}", Uuid::new_v4().simple());
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
                "refusing schema DDL because database {database_name:?} is not a test database"
            ));
        }
        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create test schema: {error}"))?;
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to drop test schema: {error}"));
        self.pool.close().await;
        result.map(|_| ())
    }
}
