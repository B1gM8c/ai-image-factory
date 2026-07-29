use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::Response,
};
use factory_identity::AuthenticatedPrincipal;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    credit_grants::{
        CreateCreditGrantRequest, CreditGrantActor, CreditGrantService, ListCreditGrantsRequest,
        OrganizationCreditGrantList, RevokeCreditGrantRequest,
    },
};

use super::{
    AppState,
    sessions::{authenticate_identity, authorize_platform_owner_scope, private_json},
};

const READ_SCOPE: &str = "billing:read";
const WRITE_SCOPE: &str = "admin:*";

pub(super) async fn list_credit_grants(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListCreditGrantsRequest>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner_scope(&headers, &state, READ_SCOPE).await?;
    Ok(private_json(credit_grants(&state)?.list(query).await?))
}

pub(super) async fn list_organization_credit_grants(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(organization_id): Path<String>,
    Query(mut query): Query<ListCreditGrantsRequest>,
) -> Result<Response, ImageGatewayError> {
    let principal = authenticate_identity(&headers, &state).await?;
    if !can_list_organization_credit_grants(&principal, &organization_id) {
        return Err(ImageGatewayError::not_found(
            "Organization was not found",
            Some("organization_id".to_string()),
            "organization_not_found",
        ));
    }
    query.organization_id = Some(organization_id.clone());
    let grants = credit_grants(&state)?.list(query).await?;
    Ok(private_json(OrganizationCreditGrantList::from_admin(
        grants,
        organization_id,
    )))
}

pub(super) async fn get_credit_grant(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(grant_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_platform_owner_scope(&headers, &state, READ_SCOPE).await?;
    Ok(private_json(credit_grants(&state)?.get(grant_id).await?))
}

pub(super) async fn create_credit_grant(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<CreateCreditGrantRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner_scope(&headers, &state, WRITE_SCOPE).await?;
    let idempotency_key = idempotency_key(&headers)?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid credit grant request body",
            None,
            "invalid_request_body",
        )
    })?;
    Ok(private_json(
        credit_grants(&state)?
            .create(
                idempotency_key,
                CreditGrantActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
                request,
            )
            .await?,
    ))
}

pub(super) async fn revoke_credit_grant(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(grant_id): Path<Uuid>,
    body: Result<Json<RevokeCreditGrantRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let principal = authorize_platform_owner_scope(&headers, &state, WRITE_SCOPE).await?;
    let idempotency_key = idempotency_key(&headers)?;
    let Json(request) = body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid credit grant revocation request body",
            None,
            "invalid_request_body",
        )
    })?;
    Ok(private_json(
        credit_grants(&state)?
            .revoke(
                grant_id,
                idempotency_key,
                CreditGrantActor {
                    user_id: principal.user_id,
                    session_id: principal.session_id,
                },
                request,
            )
            .await?,
    ))
}

fn can_list_organization_credit_grants(
    principal: &AuthenticatedPrincipal,
    organization_id: &str,
) -> bool {
    let platform_owner = principal.roles.iter().any(|role| role == "platform_owner")
        && principal.has_scope("admin:*");
    let organization_owner = principal.has_scope("workspace:read")
        && principal.organizations.iter().any(|membership| {
            membership.organization_id == organization_id && membership.role == "owner"
        });
    platform_owner || organization_owner
}

fn idempotency_key(headers: &HeaderMap) -> Result<&str, ImageGatewayError> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ImageGatewayError::invalid_idempotency_key)
}

fn credit_grants(state: &AppState) -> Result<&Arc<dyn CreditGrantService>, ImageGatewayError> {
    state.credit_grant_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("Credit grant service is not configured")
    })
}

#[cfg(test)]
mod tests {
    use factory_identity::{AuthenticatedPrincipal, OrganizationMembership};
    use uuid::Uuid;

    use super::can_list_organization_credit_grants;

    #[test]
    fn organization_grants_require_platform_or_matching_organization_owner() {
        let platform_owner = principal(vec!["platform_owner"], vec!["admin:*"], vec![]);
        assert!(can_list_organization_credit_grants(
            &platform_owner,
            "org-target"
        ));

        let platform_owner_without_scope =
            principal(vec!["platform_owner"], vec!["workspace:read"], vec![]);
        assert!(!can_list_organization_credit_grants(
            &platform_owner_without_scope,
            "org-target"
        ));

        let organization_owner = principal(
            vec!["member"],
            vec!["workspace:read"],
            vec![membership("org-target", "owner")],
        );
        assert!(can_list_organization_credit_grants(
            &organization_owner,
            "org-target"
        ));

        let owner_without_scope = principal(
            vec!["member"],
            vec![],
            vec![membership("org-target", "owner")],
        );
        assert!(!can_list_organization_credit_grants(
            &owner_without_scope,
            "org-target"
        ));

        let organization_member = principal(
            vec!["member"],
            vec!["workspace:read"],
            vec![membership("org-target", "member")],
        );
        assert!(!can_list_organization_credit_grants(
            &organization_member,
            "org-target"
        ));

        let foreign_owner = principal(
            vec!["member"],
            vec!["workspace:read"],
            vec![membership("org-other", "owner")],
        );
        assert!(!can_list_organization_credit_grants(
            &foreign_owner,
            "org-target"
        ));
    }

    fn principal(
        roles: Vec<&str>,
        scopes: Vec<&str>,
        organizations: Vec<OrganizationMembership>,
    ) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            user_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            email: "member@example.test".to_string(),
            display_name: "Member".to_string(),
            roles: roles.into_iter().map(str::to_string).collect(),
            scopes: scopes.into_iter().map(str::to_string).collect(),
            authz_version: 1,
            organizations,
            projects: vec![],
        }
    }

    fn membership(organization_id: &str, role: &str) -> OrganizationMembership {
        OrganizationMembership {
            organization_id: organization_id.to_string(),
            display_name: "Organization".to_string(),
            role: role.to_string(),
            is_personal: false,
        }
    }
}
