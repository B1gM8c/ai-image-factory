use std::sync::Arc;

use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};

use crate::{
    ImageGatewayError,
    auth::{ApiKeyCapability, AuthContext},
    media_segments::{
        MAX_ASSET_BYTES, MediaScope, SegmentRequest, SegmentStatus, Segmentation,
        SidecarCapabilities,
    },
};

use super::{AppState, authenticate_image_request, sessions::private_json};

const MULTIPART_OVERHEAD_BYTES: usize = 1024 * 1024;
pub(super) const ASSET_UPLOAD_BODY_LIMIT: usize = MAX_ASSET_BYTES + MULTIPART_OVERHEAD_BYTES;
pub(super) const SEGMENT_REQUEST_BODY_LIMIT: usize = 4 * 1024;

pub(super) async fn capabilities(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesRead)?;
    let capabilities = state.media_segments_service.as_ref().map_or(
        SidecarCapabilities {
            supports_bbox_sidecar: false,
            supports_mask_sidecar: false,
        },
        |service| service.capabilities(),
    );
    Ok(private_json(capabilities))
}

pub(super) async fn register_asset(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    multipart: Result<Multipart, axum::extract::multipart::MultipartRejection>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesWrite)?;
    let service = service(&state)?;
    // Bound multipart aggregation itself, not only the later hash/decode work.
    let permit = service.try_acquire_registration()?;
    let mut multipart = multipart.map_err(|_| invalid_upload(StatusCode::BAD_REQUEST))?;

    let mut image = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| invalid_upload(error.status()))?
    {
        match field.name() {
            Some("image") if image.is_none() => {
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|error| invalid_upload(error.status()))?;
                if bytes.len() > MAX_ASSET_BYTES {
                    return Err(ImageGatewayError::payload_too_large(
                        "image must not exceed 20 MiB",
                    ));
                }
                image = Some(bytes.to_vec());
            }
            Some("image") => {
                return Err(ImageGatewayError::invalid_request(
                    "image must be supplied exactly once",
                    Some("image".to_owned()),
                    "duplicate_image",
                ));
            }
            Some(name) => return Err(ImageGatewayError::unknown_parameter(name)),
            None => {
                return Err(ImageGatewayError::invalid_request(
                    "Every multipart field must have a name",
                    None,
                    "invalid_image_upload",
                ));
            }
        }
    }
    let image = image.ok_or_else(|| {
        ImageGatewayError::invalid_request(
            "image is required",
            Some("image".to_owned()),
            "missing_image",
        )
    })?;
    let asset = service
        .register_asset_with_permit(&scope(&auth), image, permit)
        .await?;
    Ok(private_json(asset))
}

fn invalid_upload(status: StatusCode) -> ImageGatewayError {
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        ImageGatewayError::payload_too_large("image must not exceed 20 MiB")
    } else {
        ImageGatewayError::invalid_request(
            "The multipart image upload could not be read",
            Some("image".into()),
            "invalid_image_upload",
        )
    }
}

pub(super) async fn request_segments(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<SegmentRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    let Json(request) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    auth.require_api_key_capability(if request.cached_only {
        ApiKeyCapability::ImagesRead
    } else {
        ApiKeyCapability::ImagesWrite
    })?;
    let result = service(&state)?.request(&scope(&auth), request).await?;
    Ok(post_response(result))
}

pub(super) async fn get_segments(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::ImagesRead)?;
    let result = service(&state)?.get(&scope(&auth), &id).await?;
    Ok(get_response(result))
}

fn service(
    state: &AppState,
) -> Result<&crate::media_segments::MediaSegmentsService, ImageGatewayError> {
    state
        .media_segments_service
        .as_deref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("Media segmentation is unavailable"))
}

fn scope(auth: &AuthContext) -> MediaScope {
    MediaScope {
        tenant_id: auth.tenant_id.clone(),
        project_id: auth.project_id.clone(),
        owner_id: auth
            .actor_user_id
            .or(auth.credential_owner_user_id)
            .map(|id| id.to_string())
            .unwrap_or_default(),
    }
}

fn post_response(result: Segmentation) -> Response {
    let processing = result.status == SegmentStatus::Processing;
    let mut response = private_json(result);
    if processing {
        *response.status_mut() = StatusCode::ACCEPTED;
        set_retry_after(&mut response);
    }
    response
}

fn get_response(result: Segmentation) -> Response {
    let processing = result.status == SegmentStatus::Processing;
    let mut response = private_json(result);
    if processing {
        set_retry_after(&mut response);
    }
    response
}

fn set_retry_after(response: &mut Response) {
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        auth::{ApiKeyPermissionMode, ApiKeyPermissions},
        media_segments::ImageSize,
        service_tiers::ProjectServiceTier,
    };
    use uuid::Uuid;

    fn auth(actor: Option<Uuid>, owner: Option<Uuid>) -> AuthContext {
        AuthContext {
            tenant_id: "tenant".to_owned(),
            project_id: "project".to_owned(),
            project_service_tier: ProjectServiceTier::Default,
            service_account_id: None,
            api_key_id: None,
            credential_authz_version: None,
            credential_owner_user_id: owner,
            actor_user_id: actor,
            actor_session_id: None,
            actor_authz_version: None,
            api_key_permission_mode: ApiKeyPermissionMode::All,
            api_key_permissions: ApiKeyPermissions::default(),
            route: None,
            is_admin: false,
        }
    }

    fn segmentation(status: SegmentStatus) -> Segmentation {
        Segmentation {
            object: "media.segmentation".to_owned(),
            id: "seg_00000000000000000000000000000000".to_owned(),
            asset_id: "img_00000000000000000000000000000000".to_owned(),
            status,
            schema_version: "1.0".to_owned(),
            image: ImageSize {
                width: 1,
                height: 1,
                coordinate_system: "pixel_xyxy".to_owned(),
            },
            groups: Vec::new(),
            error: None,
        }
    }

    #[test]
    fn scope_prefers_actor_then_credential_owner() {
        let actor = Uuid::new_v4();
        let owner = Uuid::new_v4();
        assert_eq!(
            scope(&auth(Some(actor), Some(owner))).owner_id,
            actor.to_string()
        );
        assert_eq!(scope(&auth(None, Some(owner))).owner_id, owner.to_string());
        assert_eq!(scope(&auth(None, None)).owner_id, "");
    }

    #[test]
    fn processing_responses_have_the_contract_status_and_retry_header() {
        let post = post_response(segmentation(SegmentStatus::Processing));
        assert_eq!(post.status(), StatusCode::ACCEPTED);
        assert_eq!(post.headers().get(header::RETRY_AFTER).unwrap(), "2");

        let get = get_response(segmentation(SegmentStatus::Processing));
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.headers().get(header::RETRY_AFTER).unwrap(), "2");

        let completed = post_response(segmentation(SegmentStatus::Completed));
        assert_eq!(completed.status(), StatusCode::OK);
        assert!(completed.headers().get(header::RETRY_AFTER).is_none());
    }
}
