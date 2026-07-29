use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
    response::Response,
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    provider_management::{
        BindApiKeyRouteRequest, CreateProviderRouteRequest, ProviderModelView, ProviderRouteView,
        StartCodexLoginRequest, StartProviderLoginRequest, StartProviderReauthorizationRequest,
        UpdateGrokVideoOutputRequest, UpdateProviderAccountModelConfigurationRequest,
        UpdateProviderAccountModelsRequest, UpdateProviderAccountSchedulingRequest,
        UpdateProviderRouteRequest,
    },
};

use super::{
    AppState,
    sessions::{authenticate_identity, authorize_admin_scope, private_json},
};

#[derive(Serialize)]
struct ConsoleProviderRoutesSnapshot {
    as_of_ms: i64,
    routes: Vec<ConsoleProviderRoute>,
}

#[derive(Serialize)]
struct ConsoleProviderRoute {
    route_id: Uuid,
    display_name: String,
    provider_id: String,
    operation_id: String,
    route_kind: String,
}

#[derive(Serialize)]
struct ConsoleProviderModelsSnapshot {
    as_of_ms: i64,
    models: Vec<ConsoleProviderModel>,
}

#[derive(Serialize)]
struct ConsoleProviderModel {
    provider_id: String,
    provider_display_name: String,
    model_id: String,
    display_name: String,
    media_kind: String,
    operation_ids: Vec<String>,
    discovery_source: String,
    adapter_state: String,
    lifecycle_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    observed_account_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    routable_account_count: Option<i64>,
    latest_cli_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_observed_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_successful_refresh_at_ms: Option<i64>,
    availability: String,
}

pub(super) async fn start_codex_login(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<StartCodexLoginRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    let session = management(&state)?.start_codex_login(body).await?;
    Ok(private_json(session))
}

pub(super) async fn managed_cli_providers(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(
        management(&state)?.managed_cli_providers().await?,
    ))
}

pub(super) async fn list_provider_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(management(&state)?.provider_models().await?))
}

pub(super) async fn list_console_provider_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let principal = authenticate_identity(&headers, &state).await?;
    if !principal.has_scope("workspace:read") && !principal.has_scope("admin:*") {
        return Err(ImageGatewayError::forbidden(
            "Workspace read permission is required",
        ));
    }
    let include_operational = principal.has_scope("admin:*");
    let snapshot = management(&state)?.provider_models().await?;
    Ok(private_json(ConsoleProviderModelsSnapshot {
        as_of_ms: snapshot.as_of_ms,
        models: snapshot
            .models
            .into_iter()
            .map(|model| console_provider_model(model, include_operational))
            .collect(),
    }))
}

pub(super) async fn start_provider_model_refresh(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(
        management(&state)?
            .start_provider_model_refresh(provider_account_id)
            .await?,
    ))
}

pub(super) async fn provider_model_refresh(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(refresh_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(
        management(&state)?
            .provider_model_refresh(refresh_id)
            .await?,
    ))
}

pub(super) async fn provider_account_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(
        management(&state)?
            .provider_account_models(provider_account_id)
            .await?,
    ))
}

pub(super) async fn update_provider_account_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
    body: Result<Json<UpdateProviderAccountModelsRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?
            .update_provider_account_models(provider_account_id, body)
            .await?,
    ))
}

pub(super) async fn update_provider_account_model_configuration(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
    body: Result<
        Json<UpdateProviderAccountModelConfigurationRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?
            .update_provider_account_model_configuration(provider_account_id, body)
            .await?,
    ))
}

pub(super) async fn start_provider_login(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<StartProviderLoginRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?.start_provider_login(body).await?,
    ))
}

pub(super) async fn provider_login_session(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(login_session_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let session = management(&state)?.login_session(login_session_id).await?;
    Ok(private_json(session))
}

pub(super) async fn start_provider_reauthorization(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
    body: Result<
        Json<StartProviderReauthorizationRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?
            .start_provider_reauthorization(provider_account_id, body)
            .await?,
    ))
}

pub(super) async fn refresh_provider_quota(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    management(&state)?
        .refresh_provider_quota(provider_account_id)
        .await?;
    Ok(private_json(serde_json::json!({ "refreshed": true })))
}

pub(super) async fn update_provider_account_scheduling(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
    body: Result<
        Json<UpdateProviderAccountSchedulingRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?
            .update_account_scheduling(provider_account_id, body)
            .await?,
    ))
}

pub(super) async fn grok_video_output(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(
        management(&state)?
            .grok_video_output(provider_account_id)
            .await?,
    ))
}

pub(super) async fn update_grok_video_output(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(provider_account_id): Path<Uuid>,
    body: Result<Json<UpdateGrokVideoOutputRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?
            .update_grok_video_output(provider_account_id, body)
            .await?,
    ))
}

pub(super) async fn list_provider_routes(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(management(&state)?.list_routes().await?))
}

pub(super) async fn list_console_provider_routes(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let principal = authenticate_identity(&headers, &state).await?;
    if !principal.has_scope("workspace:read") && !principal.has_scope("admin:*") {
        return Err(ImageGatewayError::forbidden(
            "Workspace read permission is required",
        ));
    }
    let snapshot = management(&state)?.list_routes().await?;
    Ok(private_json(ConsoleProviderRoutesSnapshot {
        as_of_ms: snapshot.as_of_ms,
        routes: snapshot
            .routes
            .into_iter()
            .filter_map(console_provider_route)
            .collect(),
    }))
}

fn console_provider_route(route: ProviderRouteView) -> Option<ConsoleProviderRoute> {
    if route.state != "enabled" || route.route_kind != "group" {
        return None;
    }
    Some(ConsoleProviderRoute {
        route_id: route.route_id,
        display_name: route.display_name,
        provider_id: route.provider_id,
        operation_id: route.operation_id,
        route_kind: route.route_kind,
    })
}

fn console_provider_model(
    model: ProviderModelView,
    include_operational: bool,
) -> ConsoleProviderModel {
    ConsoleProviderModel {
        provider_id: model.provider_id,
        provider_display_name: model.provider_display_name,
        model_id: model.model_id,
        display_name: model.display_name,
        media_kind: model.media_kind,
        operation_ids: model.operation_ids,
        discovery_source: model.discovery_source,
        adapter_state: model.adapter_state,
        lifecycle_state: model.lifecycle_state,
        observed_account_count: include_operational.then_some(model.observed_account_count),
        routable_account_count: include_operational.then_some(model.routable_account_count),
        latest_cli_version: model.latest_cli_version,
        last_observed_at_ms: include_operational
            .then_some(model.last_observed_at_ms)
            .flatten(),
        last_successful_refresh_at_ms: include_operational
            .then_some(model.last_successful_refresh_at_ms)
            .flatten(),
        availability: model.availability,
    }
}

pub(super) async fn create_provider_route(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<CreateProviderRouteRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(management(&state)?.create_route(body).await?))
}

pub(super) async fn update_provider_route(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(route_id): Path<Uuid>,
    body: Result<Json<UpdateProviderRouteRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?.update_route(route_id, body).await?,
    ))
}

pub(super) async fn bind_api_key_route(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, api_key_id)): Path<(String, String)>,
    body: Result<Json<BindApiKeyRouteRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    let Json(body) = parse_body(body)?;
    Ok(private_json(
        management(&state)?
            .bind_api_key_route(&project_id, &api_key_id, body.route_id)
            .await?,
    ))
}

pub(super) async fn get_api_key_route(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, api_key_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    authorize_admin_scope(&headers, &state, "admin:*").await?;
    Ok(private_json(
        management(&state)?
            .api_key_route(&project_id, &api_key_id)
            .await?,
    ))
}

fn management(
    state: &AppState,
) -> Result<&Arc<dyn crate::provider_management::ProviderManagementService>, ImageGatewayError> {
    state.provider_management_service.as_ref().ok_or_else(|| {
        ImageGatewayError::service_unavailable("provider management is not configured")
    })
}

fn parse_body<T>(
    body: Result<Json<T>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<T>, ImageGatewayError> {
    body.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid provider management request body",
            None,
            "invalid_request_body",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_model_catalog_omits_platform_operational_counts() {
        let member = serde_json::to_value(console_provider_model(sample_model(), false)).unwrap();
        assert!(member.get("observed_account_count").is_none());
        assert!(member.get("routable_account_count").is_none());
        assert!(member.get("last_observed_at_ms").is_none());
        assert!(member.get("last_successful_refresh_at_ms").is_none());

        let admin = serde_json::to_value(console_provider_model(sample_model(), true)).unwrap();
        assert_eq!(admin["observed_account_count"], 3);
        assert_eq!(admin["routable_account_count"], 2);
        assert_eq!(admin["last_observed_at_ms"], 10);
        assert_eq!(admin["last_successful_refresh_at_ms"], 11);
    }

    #[test]
    fn console_route_catalog_exposes_only_enabled_account_groups() {
        assert!(console_provider_route(sample_route("account", "enabled")).is_none());
        assert!(console_provider_route(sample_route("group", "disabled")).is_none());

        let group =
            serde_json::to_value(console_provider_route(sample_route("group", "enabled")).unwrap())
                .unwrap();
        assert_eq!(group["display_name"], "Production group");
        assert_eq!(group["provider_id"], "provider");
        assert_eq!(group["route_kind"], "group");
        assert!(group.get("revision").is_none());
        assert!(group.get("member_count").is_none());
        assert!(group.get("model_count").is_none());
    }

    fn sample_model() -> ProviderModelView {
        ProviderModelView {
            provider_id: "provider".to_string(),
            provider_display_name: "Provider".to_string(),
            model_id: "model".to_string(),
            display_name: "Model".to_string(),
            media_kind: "image".to_string(),
            operation_ids: vec!["images.generations".to_string()],
            discovery_source: "adapter_contract".to_string(),
            adapter_state: "supported".to_string(),
            lifecycle_state: "enabled".to_string(),
            observed_account_count: 3,
            routable_account_count: 2,
            latest_cli_version: Some("1.0.0".to_string()),
            last_observed_at_ms: Some(10),
            last_successful_refresh_at_ms: Some(11),
            availability: "routable".to_string(),
        }
    }

    fn sample_route(route_kind: &str, state: &str) -> ProviderRouteView {
        ProviderRouteView {
            route_id: Uuid::new_v4(),
            revision: 3,
            route_key: "route.production".to_string(),
            display_name: "Production group".to_string(),
            provider_id: "provider".to_string(),
            operation_id: "images.generations".to_string(),
            command_schema: "provider.images.v1".to_string(),
            route_kind: route_kind.to_string(),
            selection_strategy: "quota_aware_least_loaded".to_string(),
            quota_freshness_ms: 60_000,
            unknown_quota_policy: "allow".to_string(),
            state: state.to_string(),
            members: Vec::new(),
            model_mappings: Vec::new(),
            created_at_ms: 1,
        }
    }
}
