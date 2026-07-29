use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::{
    ImageGatewayError,
    api_keys::ProjectServiceAccountDeleted,
    auth::{ApiKeyPermissionMode, ApiKeyPermissions},
    service_tiers::ProjectServiceTier,
};

use super::{
    AppState,
    sessions::{authenticate_identity, private_json},
};

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateProjectRequest {
    #[schema(min_length = 1, max_length = 256)]
    pub(crate) organization_id: String,
    #[schema(min_length = 1, max_length = 128)]
    pub(crate) name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateProjectRequest {
    #[schema(min_length = 1, max_length = 128)]
    pub(crate) name: String,
    pub(crate) service_tier: ProjectServiceTier,
    pub(crate) user_api_keys_disabled: bool,
    #[schema(minimum = 1)]
    pub(crate) expected_settings_version: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateServiceAccountRequest {
    #[schema(min_length = 1, max_length = 128)]
    pub(crate) name: String,
    #[schema(value_type = Option<String>)]
    pub(crate) route_id: Option<String>,
    pub(crate) permission_mode: ApiKeyPermissionMode,
    pub(crate) permissions: ApiKeyPermissions,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateUserApiKeyRequest {
    #[schema(min_length = 1, max_length = 128)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) permission_mode: ApiKeyPermissionMode,
    #[serde(default)]
    pub(crate) permissions: ApiKeyPermissions,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateApiKeyRequest {
    #[schema(min_length = 1, max_length = 128)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) permission_mode: ApiKeyPermissionMode,
    #[serde(default)]
    pub(crate) permissions: ApiKeyPermissions,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListApiKeysQuery {
    after: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListProjectsQuery {
    after: Option<String>,
    limit: Option<usize>,
}

pub(super) async fn create_project(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<CreateProjectRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_workspace(&headers, &state, "workspace:write").await?;
    let Json(body) = parse_body(body)?;
    let organization_owner = principal.organizations.iter().any(|membership| {
        membership.organization_id == body.organization_id && membership.role == "owner"
    });
    if !is_platform_owner(&principal) && !organization_owner {
        return Err(organization_not_found());
    }
    let project = state
        .api_key_store
        .create_project_for_tenant(
            &body.organization_id,
            organization_owner.then_some(principal.user_id),
            &body.name,
        )
        .await?;
    tracing::info!(actor.user_id = %principal.user_id, project.id = %project.id, "project created");
    Ok(private_json(project))
}

pub(super) async fn list_projects(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListProjectsQuery>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_workspace(&headers, &state, "workspace:read").await?;
    let projects = if is_platform_owner(&principal) {
        state
            .api_key_store
            .list_projects(query.after.as_deref(), query.limit.unwrap_or(20))
            .await?
    } else {
        let project_ids = principal
            .projects
            .iter()
            .map(|membership| membership.project_id.clone())
            .collect::<Vec<_>>();
        state
            .api_key_store
            .list_projects_for_ids(
                &project_ids,
                query.after.as_deref(),
                query.limit.unwrap_or(20),
            )
            .await?
    };
    Ok(private_json(projects))
}

pub(super) async fn get_project(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    Ok(private_json(
        state.api_key_store.get_project(&project_id).await?,
    ))
}

pub(super) async fn update_project(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<UpdateProjectRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(body) = parse_body(body)?;
    let project = state
        .api_key_store
        .update_project_settings(
            &project_id,
            principal.user_id,
            &body.name,
            body.service_tier,
            body.user_api_keys_disabled,
            body.expected_settings_version,
        )
        .await?;
    tracing::info!(
        project.id = %project_id,
        actor.user_id = %principal.user_id,
        service_tier = body.service_tier.as_str(),
        user_api_keys_disabled = body.user_api_keys_disabled,
        "project settings updated"
    );
    Ok(private_json(project))
}

pub(super) async fn create_project_service_account(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<CreateServiceAccountRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let Json(body) = parse_body(body)?;
    let service_account = if let Some(route_id) = body.route_id {
        let route_id = uuid::Uuid::parse_str(&route_id).map_err(|_| {
            ImageGatewayError::invalid_request(
                "route_id is invalid",
                Some("route_id".to_string()),
                "invalid_identifier",
            )
        })?;
        state
            .api_key_store
            .create_service_account_with_route_for_actor(
                &project_id,
                &body.name,
                route_id,
                principal.user_id,
                body.permission_mode,
                body.permissions,
            )
            .await?
    } else {
        state
            .api_key_store
            .create_service_account_for_actor(
                &project_id,
                &body.name,
                principal.user_id,
                body.permission_mode,
                body.permissions,
            )
            .await?
    };
    tracing::info!(
        project.id = %project_id,
        service_account.id = %service_account.id,
        actor.user_id = %principal.user_id,
        "project service account created"
    );
    Ok(private_json(service_account))
}

pub(super) async fn create_user_api_key(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<CreateUserApiKeyRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let Json(body) = parse_body(body)?;
    let created = state
        .api_key_store
        .create_user_api_key(
            &project_id,
            principal.user_id,
            &principal.display_name,
            &principal.email,
            body.name.as_deref().unwrap_or("Secret Key"),
            body.permission_mode,
            body.permissions,
        )
        .await?;
    tracing::info!(
        project.id = %project_id,
        api_key.id = %created.id,
        actor.user_id = %principal.user_id,
        "personal project API key created"
    );
    Ok(private_json(created))
}

pub(super) async fn list_project_api_keys(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    Query(query): Query<ListApiKeysQuery>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let keys = if principal_can_manage_shared_credentials(&principal, &project_id) {
        state
            .api_key_store
            .list_project_api_keys(
                &project_id,
                query.after.as_deref(),
                query.limit.unwrap_or(20),
            )
            .await?
    } else {
        state
            .api_key_store
            .list_project_api_keys_for_user(
                &project_id,
                principal.user_id,
                query.after.as_deref(),
                query.limit.unwrap_or(20),
            )
            .await?
    };
    Ok(private_json(keys))
}

pub(super) async fn delete_project_api_key(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, api_key_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let deleted = if principal_can_manage_shared_credentials(&principal, &project_id) {
        state
            .api_key_store
            .delete_project_api_key_for_actor(&project_id, &api_key_id, principal.user_id)
            .await?
    } else {
        state
            .api_key_store
            .delete_user_project_api_key(&project_id, &api_key_id, principal.user_id)
            .await?
    };
    tracing::info!(
        project.id = %project_id,
        api_key.id = %api_key_id,
        actor.user_id = %principal.user_id,
        "project API key deleted"
    );
    Ok(private_json(deleted))
}

pub(super) async fn update_project_api_key(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, api_key_id)): Path<(String, String)>,
    body: Result<Json<UpdateApiKeyRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let Json(body) = parse_body(body)?;
    let can_manage_shared_credentials =
        principal_can_manage_shared_credentials(&principal, &project_id);
    let updated = state
        .api_key_store
        .update_project_api_key(
            &project_id,
            &api_key_id,
            principal.user_id,
            can_manage_shared_credentials,
            &body.name,
            body.permission_mode,
            body.permissions,
        )
        .await?;
    tracing::info!(
        project.id = %project_id,
        api_key.id = %api_key_id,
        actor.user_id = %principal.user_id,
        "project API key updated"
    );
    Ok(private_json(updated))
}

pub(super) async fn rotate_project_api_key(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, api_key_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let can_manage_shared_credentials =
        principal_can_manage_shared_credentials(&principal, &project_id);
    let rotated = state
        .api_key_store
        .rotate_project_api_key(
            &project_id,
            &api_key_id,
            principal.user_id,
            can_manage_shared_credentials,
        )
        .await?;
    tracing::info!(
        project.id = %project_id,
        api_key.id = %api_key_id,
        replacement_api_key.id = %rotated.api_key.id,
        actor.user_id = %principal.user_id,
        "project API key rotated"
    );
    Ok(private_json(rotated))
}

pub(super) async fn delete_project_service_account(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, service_account_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_project_owner(&headers, &state, &project_id).await?;
    let deleted: ProjectServiceAccountDeleted = state
        .api_key_store
        .delete_service_account_for_actor(&project_id, &service_account_id, principal.user_id)
        .await?;
    tracing::info!(
        project.id = %project_id,
        service_account.id = %service_account_id,
        actor.user_id = %principal.user_id,
        "project service account deleted"
    );
    Ok(private_json(deleted))
}

pub(super) async fn authorize_project(
    headers: &HeaderMap,
    state: &Arc<AppState>,
    project_id: &str,
    required_scope: &str,
) -> Result<factory_identity::AuthenticatedPrincipal, ImageGatewayError> {
    let principal = authorize_workspace(headers, state, required_scope).await?;
    if is_platform_owner(&principal) {
        return Ok(principal);
    }
    let tenant_id = state
        .api_key_store
        .project_tenant(project_id)
        .await?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Project was not found",
                Some("project_id".to_string()),
                "project_not_found",
            )
        })?;
    let has_project_access = principal.projects.iter().any(|membership| {
        membership.project_id == project_id && membership.organization_id == tenant_id
    });
    if !has_project_access {
        return Err(ImageGatewayError::not_found(
            "Project was not found",
            Some("project_id".to_string()),
            "project_not_found",
        ));
    }
    Ok(principal)
}

pub(super) async fn authorize_project_owner(
    headers: &HeaderMap,
    state: &Arc<AppState>,
    project_id: &str,
) -> Result<factory_identity::AuthenticatedPrincipal, ImageGatewayError> {
    let principal = authorize_project(headers, state, project_id, "workspace:write").await?;
    if is_platform_owner(&principal) {
        return Ok(principal);
    }
    if principal_can_manage_shared_credentials(&principal, project_id) {
        return Ok(principal);
    }
    Err(ImageGatewayError::forbidden(
        "Project owner permission is required",
    ))
}

fn principal_can_manage_shared_credentials(
    principal: &factory_identity::AuthenticatedPrincipal,
    project_id: &str,
) -> bool {
    if is_platform_owner(principal) {
        return true;
    }
    let Some(project_membership) = principal
        .projects
        .iter()
        .find(|membership| membership.project_id == project_id)
    else {
        return false;
    };
    project_membership.role == "owner"
        || principal.organizations.iter().any(|membership| {
            membership.organization_id == project_membership.organization_id
                && membership.role == "owner"
        })
}

async fn authorize_workspace(
    headers: &HeaderMap,
    state: &Arc<AppState>,
    required_scope: &str,
) -> Result<factory_identity::AuthenticatedPrincipal, ImageGatewayError> {
    let principal = authenticate_identity(headers, state).await?;
    if is_platform_owner(&principal) || principal.has_scope(required_scope) {
        return Ok(principal);
    }
    Err(ImageGatewayError::forbidden(
        "Workspace permission is required",
    ))
}

fn is_platform_owner(principal: &factory_identity::AuthenticatedPrincipal) -> bool {
    principal.roles.iter().any(|role| role == "platform_owner")
        && principal.scopes.iter().any(|scope| scope == "admin:*")
}

fn organization_not_found() -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Organization was not found",
        Some("organization_id".to_string()),
        "organization_not_found",
    )
}

fn parse_body<T>(
    body: Result<Json<T>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<T>, ImageGatewayError> {
    body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })
}
