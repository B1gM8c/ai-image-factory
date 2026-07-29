use std::{env, sync::Arc};

use factory_identity::{AccessTokenCodec, AuthPolicy, IdentityService, RefreshTokenKeyring};
use gpt_image_2_gateway::{
    database::{connect_test_pool_with_search_path, run_migrations},
    identity::PostgresIdentityRepository,
    project_governance::{
        AddProjectMemberRequest, PostgresProjectGovernanceService, ProjectGovernanceService,
        ProjectMemberRole, UpdateProjectMemberRequest,
    },
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
async fn project_membership_changes_preserve_owner_default_and_authz_invariants() -> TestResult {
    let Some(schema) = TestSchema::new(8).await? else {
        return Ok(());
    };
    let result = project_governance_case(&schema.pool).await;
    let cleanup = schema.cleanup().await;
    result.and(cleanup)
}

async fn project_governance_case(pool: &PgPool) -> TestResult {
    run_migrations(pool)
        .await
        .map_err(|error| format!("project governance migrations failed: {error:?}"))?;
    let identity = identity_service(pool)?;
    identity
        .bootstrap_admin(
            "project-owner@governance.test".to_string(),
            "Project Owner".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let owner = identity
        .list_users(None, 10)
        .await
        .map_err(debug_error)?
        .into_iter()
        .find(|user| user.email == "project-owner@governance.test")
        .ok_or_else(|| "bootstrapped project owner is missing".to_string())?;
    let owner_project = owner
        .projects
        .first()
        .ok_or_else(|| "bootstrapped owner has no default project".to_string())?
        .clone();
    let target = identity
        .create_member_user(
            "target-member@governance.test".to_string(),
            "Target Member".to_string(),
            PASSWORD.to_string(),
        )
        .await
        .map_err(debug_error)?;
    let service = PostgresProjectGovernanceService::new(pool.clone());

    let initial = service
        .list_members(&owner_project.project_id)
        .await
        .map_err(debug_error)?;
    require(
        initial.data.len() == 1
            && initial.data[0].user_id == owner.user_id
            && initial.data[0].role == "owner"
            && initial.data[0].state == "active",
        "fresh project did not have exactly one active owner",
    )?;

    let target_authz_before: i64 =
        sqlx::query_scalar("SELECT authz_version FROM identity_users WHERE user_id = $1")
            .bind(target.user_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    let added = service
        .add_member(
            &owner_project.project_id,
            owner.user_id,
            AddProjectMemberRequest {
                email: " TARGET-MEMBER@governance.test ".to_string(),
                role: ProjectMemberRole::Member,
            },
        )
        .await
        .map_err(debug_error)?;
    require(
        added.user_id == target.user_id
            && added.role == "member"
            && added.state == "active"
            && !added.is_default,
        "added member did not preserve the requested project-local role",
    )?;
    let target_authz_after_add: i64 =
        sqlx::query_scalar("SELECT authz_version FROM identity_users WHERE user_id = $1")
            .bind(target.user_id)
            .fetch_one(pool)
            .await
            .map_err(debug_error)?;
    require(
        target_authz_after_add == target_authz_before + 1,
        "adding project access did not invalidate the target user's authorization version",
    )?;

    let last_owner = service
        .remove_member(&owner_project.project_id, owner.user_id, owner.user_id)
        .await
        .expect_err("the only project owner must not be removable");
    require(
        last_owner.status_code().as_u16() == 409,
        "last-owner protection did not use conflict semantics",
    )?;

    service
        .update_member(
            &owner_project.project_id,
            target.user_id,
            owner.user_id,
            UpdateProjectMemberRequest {
                role: ProjectMemberRole::Owner,
            },
        )
        .await
        .map_err(debug_error)?;

    let fallback_project_id = format!("proj_{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO gateway_projects(id, tenant_id, name, created_at, archived_at)
        VALUES ($1, $2, 'Fallback project', 1, NULL)
        "#,
    )
    .bind(&fallback_project_id)
    .bind(&owner_project.organization_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;
    sqlx::query(
        r#"
        INSERT INTO identity_project_memberships(
            organization_id, project_id, user_id, role, state,
            is_default, created_at_ms, updated_at_ms
        )
        VALUES ($1, $2, $3, 'owner', 'active', FALSE, 1, 1)
        "#,
    )
    .bind(&owner_project.organization_id)
    .bind(&fallback_project_id)
    .bind(owner.user_id)
    .execute(pool)
    .await
    .map_err(debug_error)?;

    let removed_owner = service
        .remove_member(&owner_project.project_id, owner.user_id, target.user_id)
        .await
        .map_err(debug_error)?;
    let fallback_is_default: bool = sqlx::query_scalar(
        r#"
        SELECT is_default
        FROM identity_project_memberships
        WHERE organization_id = $1 AND project_id = $2 AND user_id = $3
        "#,
    )
    .bind(&owner_project.organization_id)
    .bind(&fallback_project_id)
    .bind(owner.user_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        removed_owner.state == "disabled" && !removed_owner.is_default && fallback_is_default,
        "removing a default project membership did not move the default atomically",
    )?;

    let still_last_owner = service
        .update_member(
            &owner_project.project_id,
            target.user_id,
            target.user_id,
            UpdateProjectMemberRequest {
                role: ProjectMemberRole::Member,
            },
        )
        .await
        .expect_err("the remaining owner must not be demoted");
    require(
        still_last_owner.status_code().as_u16() == 409,
        "last-owner demotion was not rejected",
    )?;

    service
        .add_member(
            &owner_project.project_id,
            target.user_id,
            AddProjectMemberRequest {
                email: owner.email.clone(),
                role: ProjectMemberRole::Owner,
            },
        )
        .await
        .map_err(debug_error)?;
    let removed_target = service
        .remove_member(&owner_project.project_id, target.user_id, owner.user_id)
        .await
        .map_err(debug_error)?;
    require(
        removed_target.state == "disabled" && !removed_target.is_default,
        "removed project member remained active or default",
    )?;

    let final_members = service
        .list_members(&owner_project.project_id)
        .await
        .map_err(debug_error)?;
    require(
        final_members.data.len() == 2
            && final_members
                .data
                .iter()
                .any(|member| member.user_id == owner.user_id && member.state == "active")
            && final_members
                .data
                .iter()
                .any(|member| member.user_id == target.user_id && member.state == "disabled"),
        "member listing lost active or disabled lifecycle state",
    )?;
    let audit_count: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM identity_audit_events
        WHERE resource_type = 'project'
          AND resource_id = $1
          AND action LIKE 'project.member.%'
          AND outcome = 'success'
        "#,
    )
    .bind(&owner_project.project_id)
    .fetch_one(pool)
    .await
    .map_err(debug_error)?;
    require(
        audit_count == 5,
        "successful project membership mutations were not captured exactly once",
    )
}

fn identity_service(pool: &PgPool) -> TestResult<Arc<IdentityService>> {
    let policy = AuthPolicy::default();
    let access_tokens = AccessTokenCodec::new(
        "project-governance-test",
        PRIVATE_KEY,
        [("project-governance-test".to_string(), PUBLIC_KEY.to_vec())],
        "https://identity.project-governance.test",
        "urn:aif:admin",
        &policy,
    )
    .map_err(debug_error)?;
    let refresh_tokens = RefreshTokenKeyring::new(1, [(1, vec![0x6a; 32])]).map_err(debug_error)?;
    IdentityService::new(
        Arc::new(PostgresIdentityRepository::new(pool.clone())),
        access_tokens,
        refresh_tokens,
        policy,
    )
    .map(Arc::new)
    .map_err(debug_error)
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
            eprintln!("skipping PostgreSQL project governance test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("project_governance_test_{}", Uuid::new_v4().simple());
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
