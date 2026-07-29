use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ImageGatewayError;

const MAX_EMAIL_CHARS: usize = 254;

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AddProjectMemberRequest {
    #[schema(max_length = 254)]
    pub email: String,
    #[schema(value_type = String)]
    pub role: ProjectMemberRole,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateProjectMemberRequest {
    #[schema(value_type = String)]
    pub role: ProjectMemberRole,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectMemberRole {
    Owner,
    Member,
}

impl ProjectMemberRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Member => "member",
        }
    }
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectMemberView {
    pub object: &'static str,
    #[schema(value_type = String)]
    pub user_id: Uuid,
    pub email: String,
    pub display_name: String,
    pub role: String,
    pub state: String,
    pub is_default: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProjectMemberList {
    pub object: &'static str,
    pub data: Vec<ProjectMemberView>,
}

#[async_trait]
pub trait ProjectGovernanceService: Send + Sync + 'static {
    async fn list_members(&self, project_id: &str) -> Result<ProjectMemberList, ImageGatewayError>;

    async fn add_member(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        request: AddProjectMemberRequest,
    ) -> Result<ProjectMemberView, ImageGatewayError>;

    async fn update_member(
        &self,
        project_id: &str,
        target_user_id: Uuid,
        actor_user_id: Uuid,
        request: UpdateProjectMemberRequest,
    ) -> Result<ProjectMemberView, ImageGatewayError>;

    async fn remove_member(
        &self,
        project_id: &str,
        target_user_id: Uuid,
        actor_user_id: Uuid,
    ) -> Result<ProjectMemberView, ImageGatewayError>;
}

#[derive(Clone)]
pub struct PostgresProjectGovernanceService {
    pool: PgPool,
}

impl PostgresProjectGovernanceService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ProjectGovernanceService for PostgresProjectGovernanceService {
    async fn list_members(&self, project_id: &str) -> Result<ProjectMemberList, ImageGatewayError> {
        validate_project_id(project_id)?;
        let project = project_context(&self.pool, project_id).await?;
        let rows = sqlx::query_as::<_, MemberRow>(
            r#"
            SELECT membership.user_id,
                   user_record.normalized_email AS email,
                   user_record.display_name,
                   membership.role,
                   membership.state,
                   membership.is_default,
                   membership.created_at_ms,
                   membership.updated_at_ms
            FROM identity_project_memberships membership
            JOIN identity_users user_record
              ON user_record.user_id = membership.user_id
            WHERE membership.organization_id = $1
              AND membership.project_id = $2
            ORDER BY
                CASE membership.state WHEN 'active' THEN 0 ELSE 1 END,
                CASE membership.role WHEN 'owner' THEN 0 ELSE 1 END,
                LOWER(user_record.display_name),
                user_record.normalized_email,
                membership.user_id
            "#,
        )
        .bind(&project.organization_id)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(ProjectMemberList {
            object: "list",
            data: rows.into_iter().map(Into::into).collect(),
        })
    }

    async fn add_member(
        &self,
        project_id: &str,
        actor_user_id: Uuid,
        request: AddProjectMemberRequest,
    ) -> Result<ProjectMemberView, ImageGatewayError> {
        validate_project_id(project_id)?;
        let email = normalize_email(request.email)?;
        let role = request.role.as_str();
        let now_ms = now_ms()?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let project = lock_project(&mut tx, project_id).await?;
        lock_project_membership_changes(&mut tx, project_id).await?;
        let target_user_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT user_id
            FROM identity_users
            WHERE normalized_email = $1
              AND disabled_at_ms IS NULL
            FOR UPDATE
            "#,
        )
        .bind(&email)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        .ok_or_else(user_not_found)?;

        sqlx::query(
            r#"
            INSERT INTO identity_organization_memberships(
                organization_id, user_id, role, state,
                created_at_ms, updated_at_ms
            )
            VALUES ($1, $2, 'member', 'active', $3, $3)
            ON CONFLICT (organization_id, user_id) DO UPDATE
            SET state = 'active',
                updated_at_ms = EXCLUDED.updated_at_ms
            "#,
        )
        .bind(&project.organization_id)
        .bind(target_user_id)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;

        sqlx::query(
            r#"
            INSERT INTO identity_project_memberships(
                organization_id, project_id, user_id, role, state,
                is_default, created_at_ms, updated_at_ms
            )
            VALUES ($1, $2, $3, $4, 'active', FALSE, $5, $5)
            ON CONFLICT (organization_id, project_id, user_id) DO UPDATE
            SET role = EXCLUDED.role,
                state = 'active',
                updated_at_ms = EXCLUDED.updated_at_ms
            "#,
        )
        .bind(&project.organization_id)
        .bind(project_id)
        .bind(target_user_id)
        .bind(role)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;

        bump_authorization_version(&mut tx, target_user_id, now_ms).await?;
        insert_audit(
            &mut tx,
            actor_user_id,
            "project.member.add",
            project_id,
            serde_json::json!({
                "target_user_id": target_user_id,
                "role": role,
            }),
            now_ms,
        )
        .await?;
        let member = read_member(
            &mut tx,
            &project.organization_id,
            project_id,
            target_user_id,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(member.into())
    }

    async fn update_member(
        &self,
        project_id: &str,
        target_user_id: Uuid,
        actor_user_id: Uuid,
        request: UpdateProjectMemberRequest,
    ) -> Result<ProjectMemberView, ImageGatewayError> {
        validate_project_id(project_id)?;
        let role = request.role.as_str();
        let now_ms = now_ms()?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let project = lock_project(&mut tx, project_id).await?;
        lock_project_membership_changes(&mut tx, project_id).await?;
        let current_role = active_member_role(
            &mut tx,
            &project.organization_id,
            project_id,
            target_user_id,
        )
        .await?;
        if current_role == "owner" && role != "owner" {
            ensure_another_owner(&mut tx, &project.organization_id, project_id).await?;
        }
        sqlx::query(
            r#"
            UPDATE identity_project_memberships
            SET role = $4,
                updated_at_ms = $5
            WHERE organization_id = $1
              AND project_id = $2
              AND user_id = $3
              AND state = 'active'
            "#,
        )
        .bind(&project.organization_id)
        .bind(project_id)
        .bind(target_user_id)
        .bind(role)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        bump_authorization_version(&mut tx, target_user_id, now_ms).await?;
        insert_audit(
            &mut tx,
            actor_user_id,
            "project.member.role.update",
            project_id,
            serde_json::json!({
                "target_user_id": target_user_id,
                "previous_role": current_role,
                "role": role,
            }),
            now_ms,
        )
        .await?;
        let member = read_member(
            &mut tx,
            &project.organization_id,
            project_id,
            target_user_id,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(member.into())
    }

    async fn remove_member(
        &self,
        project_id: &str,
        target_user_id: Uuid,
        actor_user_id: Uuid,
    ) -> Result<ProjectMemberView, ImageGatewayError> {
        validate_project_id(project_id)?;
        let now_ms = now_ms()?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let project = lock_project(&mut tx, project_id).await?;
        lock_project_membership_changes(&mut tx, project_id).await?;
        let current_role = active_member_role(
            &mut tx,
            &project.organization_id,
            project_id,
            target_user_id,
        )
        .await?;
        if current_role == "owner" {
            ensure_another_owner(&mut tx, &project.organization_id, project_id).await?;
        }
        let was_default = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT is_default
            FROM identity_project_memberships
            WHERE organization_id = $1
              AND project_id = $2
              AND user_id = $3
              AND state = 'active'
            FOR UPDATE
            "#,
        )
        .bind(&project.organization_id)
        .bind(project_id)
        .bind(target_user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            r#"
            UPDATE identity_project_memberships
            SET state = 'disabled',
                is_default = FALSE,
                updated_at_ms = $4
            WHERE organization_id = $1
              AND project_id = $2
              AND user_id = $3
              AND state = 'active'
            "#,
        )
        .bind(&project.organization_id)
        .bind(project_id)
        .bind(target_user_id)
        .bind(now_ms)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        if was_default {
            sqlx::query(
                r#"
                UPDATE identity_project_memberships
                SET is_default = TRUE,
                    updated_at_ms = GREATEST(updated_at_ms, $2)
                WHERE (organization_id, project_id, user_id) = (
                    SELECT membership.organization_id,
                           membership.project_id,
                           membership.user_id
                    FROM identity_project_memberships membership
                    JOIN gateway_projects project
                      ON project.id = membership.project_id
                     AND project.tenant_id = membership.organization_id
                    WHERE membership.user_id = $1
                      AND membership.state = 'active'
                      AND project.archived_at IS NULL
                    ORDER BY membership.created_at_ms, membership.project_id
                    LIMIT 1
                )
                "#,
            )
            .bind(target_user_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        bump_authorization_version(&mut tx, target_user_id, now_ms).await?;
        insert_audit(
            &mut tx,
            actor_user_id,
            "project.member.remove",
            project_id,
            serde_json::json!({
                "target_user_id": target_user_id,
                "previous_role": current_role,
            }),
            now_ms,
        )
        .await?;
        let member = read_member(
            &mut tx,
            &project.organization_id,
            project_id,
            target_user_id,
        )
        .await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(member.into())
    }
}

#[derive(Debug, FromRow)]
struct ProjectContext {
    organization_id: String,
}

#[derive(Debug, FromRow)]
struct MemberRow {
    user_id: Uuid,
    email: String,
    display_name: String,
    role: String,
    state: String,
    is_default: bool,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl From<MemberRow> for ProjectMemberView {
    fn from(row: MemberRow) -> Self {
        Self {
            object: "organization.project.user",
            user_id: row.user_id,
            email: row.email,
            display_name: row.display_name,
            role: row.role,
            state: row.state,
            is_default: row.is_default,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        }
    }
}

async fn project_context(
    pool: &PgPool,
    project_id: &str,
) -> Result<ProjectContext, ImageGatewayError> {
    sqlx::query_as::<_, ProjectContext>(
        r#"
        SELECT tenant_id AS organization_id
        FROM gateway_projects
        WHERE id = $1
          AND archived_at IS NULL
        "#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)?
    .ok_or_else(project_not_found)
}

async fn lock_project(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<ProjectContext, ImageGatewayError> {
    sqlx::query_as::<_, ProjectContext>(
        r#"
        SELECT tenant_id AS organization_id
        FROM gateway_projects
        WHERE id = $1
          AND archived_at IS NULL
        FOR SHARE
        "#,
    )
    .bind(project_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or_else(project_not_found)
}

async fn lock_project_membership_changes(
    tx: &mut Transaction<'_, Postgres>,
    project_id: &str,
) -> Result<(), ImageGatewayError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("project-members:{project_id}"))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

async fn active_member_role(
    tx: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    project_id: &str,
    target_user_id: Uuid,
) -> Result<String, ImageGatewayError> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT role
        FROM identity_project_memberships
        WHERE organization_id = $1
          AND project_id = $2
          AND user_id = $3
          AND state = 'active'
        FOR UPDATE
        "#,
    )
    .bind(organization_id)
    .bind(project_id)
    .bind(target_user_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or_else(member_not_found)
}

async fn ensure_another_owner(
    tx: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    project_id: &str,
) -> Result<(), ImageGatewayError> {
    let owner_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM identity_project_memberships
        WHERE organization_id = $1
          AND project_id = $2
          AND state = 'active'
          AND role = 'owner'
        "#,
    )
    .bind(organization_id)
    .bind(project_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if owner_count <= 1 {
        return Err(ImageGatewayError::conflict(
            "A project must retain at least one active owner",
            Some("role".to_string()),
            "last_project_owner",
        ));
    }
    Ok(())
}

async fn bump_authorization_version(
    tx: &mut Transaction<'_, Postgres>,
    user_id: Uuid,
    now_ms: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        UPDATE identity_users
        SET authz_version = authz_version + 1,
            updated_at_ms = GREATEST(updated_at_ms, $2)
        WHERE user_id = $1
        "#,
    )
    .bind(user_id)
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn insert_audit(
    tx: &mut Transaction<'_, Postgres>,
    actor_user_id: Uuid,
    action: &str,
    project_id: &str,
    metadata: serde_json::Value,
    now_ms: i64,
) -> Result<(), ImageGatewayError> {
    sqlx::query(
        r#"
        INSERT INTO identity_audit_events(
            event_id, actor_user_id, action, resource_type,
            resource_id, outcome, metadata, created_at_ms
        )
        VALUES ($1, $2, $3, 'project', $4, 'success', $5, $6)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(actor_user_id)
    .bind(action)
    .bind(project_id)
    .bind(metadata)
    .bind(now_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn read_member(
    tx: &mut Transaction<'_, Postgres>,
    organization_id: &str,
    project_id: &str,
    user_id: Uuid,
) -> Result<MemberRow, ImageGatewayError> {
    sqlx::query_as::<_, MemberRow>(
        r#"
        SELECT membership.user_id,
               user_record.normalized_email AS email,
               user_record.display_name,
               membership.role,
               membership.state,
               membership.is_default,
               membership.created_at_ms,
               membership.updated_at_ms
        FROM identity_project_memberships membership
        JOIN identity_users user_record
          ON user_record.user_id = membership.user_id
        WHERE membership.organization_id = $1
          AND membership.project_id = $2
          AND membership.user_id = $3
        "#,
    )
    .bind(organization_id)
    .bind(project_id)
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or_else(member_not_found)
}

fn normalize_email(email: String) -> Result<String, ImageGatewayError> {
    let normalized = email.trim().to_lowercase();
    if normalized.is_empty()
        || normalized.chars().count() > MAX_EMAIL_CHARS
        || normalized.chars().any(char::is_control)
        || !normalized.contains('@')
    {
        return Err(ImageGatewayError::invalid_request(
            "email must be a valid account email",
            Some("email".to_string()),
            "invalid_project_member_email",
        ));
    }
    Ok(normalized)
}

fn validate_project_id(project_id: &str) -> Result<(), ImageGatewayError> {
    if project_id.is_empty() || project_id.len() > 256 || project_id.chars().any(char::is_control) {
        return Err(project_not_found());
    }
    Ok(())
}

fn now_ms() -> Result<i64, ImageGatewayError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ImageGatewayError::internal("system clock unavailable"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| ImageGatewayError::internal("system clock unavailable"))
}

fn project_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Project was not found",
        Some("project_id".to_string()),
        "project_not_found",
    )
}

fn user_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "User was not found or is disabled",
        Some("email".to_string()),
        "project_member_user_not_found",
    )
}

fn member_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Project member was not found",
        Some("user_id".to_string()),
        "project_member_not_found",
    )
}

fn unavailable(_: sqlx::Error) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Project governance state unavailable")
}
