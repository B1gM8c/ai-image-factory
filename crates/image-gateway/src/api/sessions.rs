use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use factory_identity::{
    AuthenticatedPrincipal, IdentityError, LoginRequest, OrganizationMembership, ProjectMembership,
    PublicUser, RefreshRequest,
};
use serde::{Deserialize, Serialize};

use crate::{
    ImageGatewayError,
    auth::{authorize_admin, bearer_token},
};

use super::AppState;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LoginBody {
    email: String,
    password: String,
    client_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RefreshBody {
    refresh_token: String,
    client_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LogoutBody {
    refresh_token: String,
}

#[derive(Serialize)]
struct PrincipalResponse {
    user: PublicUser,
    session_id: String,
    authz_version: i64,
    organizations: Vec<OrganizationMembership>,
    projects: Vec<ProjectMembership>,
    capabilities: Vec<&'static str>,
}

pub(super) async fn login(
    State(state): State<Arc<AppState>>,
    body: Result<Json<LoginBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let Json(body) = parse_json(body)?;
    let service = identity_service(&state)?;
    let tokens = service
        .login(LoginRequest {
            email: body.email,
            password: body.password,
            client_id: body.client_id,
        })
        .await
        .map_err(map_login_error)?;
    Ok(private_json(tokens))
}

pub(super) async fn refresh(
    State(state): State<Arc<AppState>>,
    body: Result<Json<RefreshBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let Json(body) = parse_json(body)?;
    let service = identity_service(&state)?;
    let tokens = service
        .refresh(RefreshRequest {
            refresh_token: body.refresh_token,
            client_id: body.client_id,
        })
        .await
        .map_err(map_login_error)?;
    Ok(private_json(tokens))
}

pub(super) async fn logout(
    State(state): State<Arc<AppState>>,
    body: Result<Json<LogoutBody>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let Json(body) = parse_json(body)?;
    if body.refresh_token.is_empty() || body.refresh_token.len() > 256 {
        return Err(ImageGatewayError::identity_credentials());
    }
    identity_service(&state)?
        .logout_refresh(&body.refresh_token)
        .await
        .map_err(map_identity_error)?;
    Ok(private_empty())
}

pub(super) async fn me(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let principal = authenticate_identity(&headers, &state).await?;
    let capabilities = console_capabilities(&principal);
    Ok(private_json(PrincipalResponse {
        user: PublicUser {
            id: principal.user_id.to_string(),
            email: principal.email,
            display_name: principal.display_name,
            roles: principal.roles,
            scopes: principal.scopes,
        },
        session_id: principal.session_id.to_string(),
        authz_version: principal.authz_version,
        organizations: principal.organizations,
        projects: principal.projects,
        capabilities,
    }))
}

pub(super) async fn authorize_admin_scope(
    headers: &HeaderMap,
    state: &Arc<AppState>,
    required_scope: &str,
) -> Result<(), ImageGatewayError> {
    let token = bearer_token(headers)?;
    if looks_like_jwt(token) {
        let principal = authenticate_identity_token(token, state).await?;
        if principal.has_scope(required_scope) {
            return Ok(());
        }
        return Err(ImageGatewayError::forbidden("Insufficient admin scope"));
    }

    if state.legacy_admin_auth_enabled {
        authorize_admin(headers, &state.config)
    } else {
        Err(ImageGatewayError::authentication())
    }
}

pub(super) async fn authorize_platform_owner(
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<AuthenticatedPrincipal, ImageGatewayError> {
    authorize_platform_owner_scope(headers, state, "admin:*").await
}

pub(super) async fn authorize_platform_owner_scope(
    headers: &HeaderMap,
    state: &Arc<AppState>,
    required_scope: &str,
) -> Result<AuthenticatedPrincipal, ImageGatewayError> {
    let token = bearer_token(headers)?;
    if !looks_like_jwt(token) {
        return Err(ImageGatewayError::authentication());
    }
    let principal = authenticate_identity_token(token, state).await?;
    if principal.roles.iter().any(|role| role == "platform_owner")
        && principal.has_scope(required_scope)
    {
        Ok(principal)
    } else {
        Err(ImageGatewayError::forbidden(
            "Platform owner scope is required for this operation",
        ))
    }
}

pub(super) async fn authenticate_identity(
    headers: &HeaderMap,
    state: &Arc<AppState>,
) -> Result<AuthenticatedPrincipal, ImageGatewayError> {
    let token = bearer_token(headers)?;
    if !looks_like_jwt(token) {
        return Err(ImageGatewayError::authentication());
    }
    authenticate_identity_token(token, state).await
}

async fn authenticate_identity_token(
    token: &str,
    state: &Arc<AppState>,
) -> Result<AuthenticatedPrincipal, ImageGatewayError> {
    identity_service(state)?
        .authenticate_access(token)
        .await
        .map_err(map_identity_error)
}

pub(super) fn identity_service(
    state: &Arc<AppState>,
) -> Result<&factory_identity::IdentityService, ImageGatewayError> {
    state
        .identity_service
        .as_deref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("Identity service is not enabled"))
}

fn console_capabilities(principal: &AuthenticatedPrincipal) -> Vec<&'static str> {
    if principal.roles.iter().any(|role| role == "platform_owner")
        && principal.scopes.iter().any(|scope| scope == "admin:*")
    {
        return vec![
            "console:read",
            "projects:read",
            "api-keys:manage",
            "billing:read",
            "billing:refund",
            "providers:manage",
            "scheduler:read",
            "system:read",
            "system:update",
            "users:manage",
        ];
    }

    let mut capabilities = vec!["console:read", "projects:read"];
    if principal.has_scope("workspace:write") {
        capabilities.push("api-keys:create");
    }
    let manages_shared_credentials = principal
        .projects
        .iter()
        .any(|membership| membership.role == "owner")
        || principal
            .organizations
            .iter()
            .any(|membership| membership.role == "owner");
    if principal.has_scope("workspace:write") && manages_shared_credentials {
        capabilities.push("api-keys:manage");
    }
    if principal.has_scope("workspace:read") {
        capabilities.push("billing:read");
    }
    capabilities
}

fn parse_json<T>(
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

fn map_login_error(error: IdentityError) -> ImageGatewayError {
    match error {
        IdentityError::InvalidAuthentication | IdentityError::InvalidInput => {
            ImageGatewayError::identity_credentials()
        }
        other => map_identity_error(other),
    }
}

pub(super) fn map_identity_error(error: IdentityError) -> ImageGatewayError {
    match error {
        IdentityError::InvalidAuthentication => ImageGatewayError::identity_authentication(),
        IdentityError::Forbidden => ImageGatewayError::forbidden("Insufficient admin scope"),
        IdentityError::InvalidInput => ImageGatewayError::invalid_request(
            "Identity request is invalid",
            None,
            "invalid_identity_request",
        ),
        IdentityError::Conflict => ImageGatewayError::invalid_request(
            "Identity request conflicts with existing state",
            None,
            "identity_conflict",
        ),
        IdentityError::Unavailable => {
            ImageGatewayError::service_unavailable("Identity service is unavailable")
        }
        IdentityError::Configuration | IdentityError::Crypto => {
            ImageGatewayError::internal("Identity service failed")
        }
    }
}

pub(super) fn private_json<T: Serialize>(value: T) -> Response {
    let mut response = Json(value).into_response();
    set_private_headers(&mut response);
    response
}

fn private_empty() -> Response {
    let mut response = StatusCode::NO_CONTENT.into_response();
    set_private_headers(&mut response);
    response
}

fn set_private_headers(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
}

fn looks_like_jwt(token: &str) -> bool {
    token.bytes().filter(|byte| *byte == b'.').count() == 2
}

#[cfg(test)]
mod tests {
    use factory_identity::IdentityError;

    use super::{looks_like_jwt, map_identity_error, map_login_error};

    #[test]
    fn classifies_only_compact_three_segment_tokens_as_jwt() {
        assert!(looks_like_jwt("header.payload.signature"));
        assert!(!looks_like_jwt("break-glass-secret"));
        assert!(!looks_like_jwt("one.dot"));
        assert!(!looks_like_jwt("too.many.jwt.dots"));
    }

    #[test]
    fn identity_failures_do_not_report_api_key_errors() {
        assert_eq!(
            map_login_error(IdentityError::InvalidAuthentication).error_code(),
            Some("invalid_credentials")
        );
        assert_eq!(
            map_identity_error(IdentityError::InvalidAuthentication).error_code(),
            Some("invalid_token")
        );
    }
}
