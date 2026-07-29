use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    Json,
    body::Body,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use image_api_contracts::xai::{
    XAI_VIDEOS_API_PROFILE, XaiGeneratedVideo, XaiStartDeferredResponse, XaiVideoGenerationRequest,
    XaiVideoResponse, XaiVideoWorkflow,
};
use image_provider_contracts::BillingMetric;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionClaim, AdmissionContract, AdmissionError, AdmissionTicket, CustomerPricingIntent,
        XaiVideoAdmissionError, XaiVideoAdmissionInput, XaiVideoAdmissionIntent,
        idempotency_key_digest,
    },
    auth::{ApiKeyCapability, AuthContext},
    generator::InputImage,
    input_blobs::{InputBlobKey, InputBlobRef, InputBlobWriteError},
    settlement::{StoredVideoArtifact, VideoResultStatus},
    usage::{UsageCharge, UsageLimits, UsageReservation},
};

use super::{
    AppState, GenerationExecutionMode, RequestId, authenticate_image_request,
    edit_input::decode_data_url_image, resolve_request_model,
};

const RETRY_DELAY: Duration = Duration::from_millis(25);
const RETRY_ATTEMPTS: usize = 3;

pub(super) async fn create_video(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    body: Result<Json<XaiVideoGenerationRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<XaiStartDeferredResponse>, ImageGatewayError> {
    let mut auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosWrite)?;
    let Json(mut request) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    let default_model = if request.image.is_some() {
        "grok-imagine-video-1.5-preview"
    } else {
        "grok-imagine-video"
    };
    if let Some(resolved) = resolve_request_model(
        &state,
        &mut auth,
        image_provider_grok_cli::PROVIDER_ID,
        "videos.generations",
        XAI_VIDEOS_API_PROFILE,
        request.model.as_deref(),
        default_model,
    )
    .await?
    {
        request.model = Some(resolved.provider_model_id);
    }
    create_video_with_auth(&state, auth, &headers, request_id.0, request)
        .await
        .map(Json)
}

pub(super) async fn create_video_with_auth(
    state: &Arc<AppState>,
    auth: AuthContext,
    headers: &HeaderMap,
    request_id: String,
    request: XaiVideoGenerationRequest,
) -> Result<XaiStartDeferredResponse, ImageGatewayError> {
    if state.generation_execution_mode != GenerationExecutionMode::External {
        return Err(ImageGatewayError::service_unavailable(
            "video generation requires external execution",
        ));
    }
    let intent = XaiVideoAdmissionIntent::new(request).map_err(video_admission_error)?;
    let decoded = decode_video_inputs(intent.source_command(), state.config.max_upload_bytes)?;
    preflight_grok_binding(&intent, &decoded)?;
    let idempotency_key_digest = video_idempotency_digest(&headers, &auth)?;
    let contract = video_admission_contract(state.config.generation_admission_contract);
    let mut claim = intent.claim(
        Uuid::new_v4(),
        auth.tenant_id.clone(),
        auth.project_id.clone(),
        request_id.clone(),
        idempotency_key_digest,
        admission_deadline(&state),
    );
    if contract == AdmissionContract::CustomerPricingV4 {
        claim.request_hash = crate::service_tiers::request_hash_with_project_service_tier(
            &claim.request_hash,
            auth.project_service_tier,
        );
    }
    let ticket = match claim_with_retry(&state, claim)
        .await
        .map_err(admission_error)?
    {
        AdmissionClaim::Owner(ticket) => ticket,
        AdmissionClaim::InProgress { .. } => {
            return Err(ImageGatewayError::idempotency_in_progress());
        }
        AdmissionClaim::Existing { job_id, .. } => return Ok(start_response(job_id)),
        AdmissionClaim::Conflict { .. } => {
            return Err(ImageGatewayError::idempotency_conflict());
        }
    };

    let plan = match stage_and_bind(&state, &ticket, intent, &decoded).await {
        Ok(plan) => plan,
        Err(error) => {
            rollback_session(&state, &ticket).await?;
            return Err(error);
        }
    };
    let reservation = match reserve_with_retry(
        &state,
        UsageCharge {
            tenant_id: auth.tenant_id.clone(),
            attribution: Some(auth.attribution()),
            request_id,
            admission_session_id: Some(ticket.session_id),
            operation: crate::admission::VIDEO_GENERATION_OPERATION,
            provider_id: plan.provider_id().to_owned(),
            model: plan.provider_model().to_owned(),
            output_count: plan.output_count(),
            billable_units: plan.billing_units(),
            billing_metric: BillingMetric::VideoSecond,
            limits: UsageLimits {
                five_hour_image_limit: state.config.five_hour_video_second_limit,
                seven_day_image_limit: state.config.seven_day_video_second_limit,
            },
        },
    )
    .await
    {
        Ok(reservation) => reservation,
        Err(error) => {
            rollback_session(&state, &ticket).await?;
            return Err(error);
        }
    };
    let service_tier_decision = crate::service_tiers::ServiceTierDecision::for_default_only_project(
        auth.project_service_tier,
    );
    let mut attach = plan.attach(
        ticket.clone(),
        reservation.job_id,
        format!("tenant:{}", auth.tenant_id),
        contract,
    );
    if contract == AdmissionContract::CustomerPricingV4 {
        attach.customer_pricing = Some(CustomerPricingIntent {
            public_model_id: auth
                .route
                .as_ref()
                .map(|route| route.public_model_id.clone())
                .or_else(|| plan.source_command().model.clone())
                .unwrap_or_else(|| plan.provider_model().to_owned()),
            provider_model_id: plan.provider_model().to_owned(),
            execution_model_id: plan.provider_model().to_owned(),
            provider_command_hash: None,
            media_kind: "video".to_owned(),
            service_tier: service_tier_decision.effective.pricing_key().to_owned(),
            service_tier_decision,
            execution_surface: "provider_cli".to_owned(),
            currency: "USD".to_owned(),
            pricing_dimensions: video_pricing_dimensions(&plan)?,
            processing_mode: crate::admission::PricingProcessingMode::Synchronous,
        });
    }
    if let Err(error) = attach_with_retry(&state, attach).await {
        if !matches!(error, AdmissionError::Unavailable) {
            rollback_reservation(&state, &ticket, &reservation).await?;
        }
        return Err(admission_error(error));
    }
    Ok(start_response(reservation.job_id))
}

fn video_admission_contract(
    configured: crate::config::GenerationAdmissionContract,
) -> AdmissionContract {
    match configured {
        crate::config::GenerationAdmissionContract::CustomerPricingV4 => {
            AdmissionContract::CustomerPricingV4
        }
        crate::config::GenerationAdmissionContract::LegacyV1
        | crate::config::GenerationAdmissionContract::OutputEconomicsV2 => {
            AdmissionContract::MediaEconomicsV3
        }
    }
}

fn video_pricing_dimensions(
    plan: &crate::admission::XaiVideoAdmissionPlan,
) -> Result<BTreeMap<String, String>, ImageGatewayError> {
    let command = plan.source_command();
    let input_image_count = if command.image.is_some() {
        1
    } else {
        command.reference_images.len()
    };
    let mut dimensions = BTreeMap::from([
        ("duration".to_owned(), command.duration.to_string()),
        (
            "input_image_count".to_owned(),
            enum_or_integer_wire_value(input_image_count)?,
        ),
        (
            "resolution".to_owned(),
            enum_or_integer_wire_value(command.resolution)?,
        ),
    ]);
    if plan.provider_model() == "grok-imagine-video" {
        dimensions.insert(
            "aspect_ratio".to_owned(),
            enum_or_integer_wire_value(
                command
                    .aspect_ratio
                    .unwrap_or(image_api_contracts::xai::XaiVideoAspectRatio::R16x9),
            )?,
        );
    }
    Ok(dimensions)
}

fn enum_or_integer_wire_value(value: impl serde::Serialize) -> Result<String, ImageGatewayError> {
    let value = serde_json::to_value(value).map_err(|_| {
        ImageGatewayError::service_unavailable("video pricing normalization failed")
    })?;
    match value {
        serde_json::Value::String(value) => Ok(value),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        _ => Err(ImageGatewayError::service_unavailable(
            "video pricing normalization failed",
        )),
    }
}

pub(super) async fn get_video(
    Path(request_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Json<XaiVideoResponse>, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::VideosRead)?;
    let job_id = parse_public_uuid(&request_id, "request_id")?;
    let status = state
        .settlement_store
        .project_video_status(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            job_id,
        )
        .await?
        .ok_or_else(|| video_not_found("request_id"))?;
    Ok(Json(video_response(status)))
}

pub(super) async fn get_video_content(
    Path(file_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    get_video_content_with_auth(&state, &auth, &file_id).await
}

pub(super) async fn get_video_content_with_auth(
    state: &Arc<AppState>,
    auth: &crate::auth::AuthContext,
    file_id: &str,
) -> Result<Response, ImageGatewayError> {
    auth.require_api_key_capability(ApiKeyCapability::VideosRead)?;
    let artifact_id = parse_public_uuid(&file_id, "file_id")?;
    let StoredVideoArtifact { media_type, bytes } = state
        .settlement_store
        .load_project_video_artifact(
            &auth.tenant_id,
            &auth.project_id,
            auth.actor_user_id,
            artifact_id,
        )
        .await?
        .ok_or_else(|| video_not_found("file_id"))?;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, media_type)
        .header(header::CONTENT_LENGTH, bytes.len().to_string())
        .header(header::CACHE_CONTROL, "private, no-store")
        .body(Body::from(bytes))
        .map_err(|_| ImageGatewayError::internal("failed to build video response"))
}

#[derive(Clone)]
struct DecodedVideoInput {
    filename: String,
    media_type: String,
    bytes: Vec<u8>,
}

fn decode_video_inputs(
    command: &image_api_contracts::xai::XaiVideoGenerationCommandV1,
    max_upload_bytes: usize,
) -> Result<Vec<DecodedVideoInput>, ImageGatewayError> {
    let urls = match command.workflow() {
        XaiVideoWorkflow::TextToVideo => Vec::new(),
        XaiVideoWorkflow::ImageToVideo => vec![(
            "image".to_owned(),
            command
                .image
                .as_ref()
                .and_then(|image| image.url.as_deref())
                .ok_or_else(|| unsupported_video_input("image"))?,
        )],
        XaiVideoWorkflow::ReferenceToVideo => command
            .reference_images
            .iter()
            .enumerate()
            .map(|(index, image)| {
                image
                    .url
                    .as_deref()
                    .map(|url| (format!("reference_images[{index}]"), url))
                    .ok_or_else(|| unsupported_video_input("reference_images"))
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    let mut total_bytes = 0;
    urls.into_iter()
        .enumerate()
        .map(|(index, (param, url))| {
            let InputImage {
                content_type,
                bytes,
                ..
            } = decode_data_url_image(&param, url, false, &mut total_bytes, max_upload_bytes)?;
            let media_type = content_type.ok_or_else(|| {
                ImageGatewayError::internal("decoded video input has no media type")
            })?;
            let extension = match media_type.as_str() {
                "image/png" => "png",
                "image/jpeg" => "jpg",
                "image/webp" => "webp",
                _ => return Err(ImageGatewayError::artifact_integrity()),
            };
            Ok(DecodedVideoInput {
                filename: format!("input-{index}.{extension}"),
                media_type,
                bytes,
            })
        })
        .collect()
}

fn preflight_grok_binding(
    intent: &XaiVideoAdmissionIntent,
    inputs: &[DecodedVideoInput],
) -> Result<(), ImageGatewayError> {
    let projected = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            XaiVideoAdmissionInput::new(
                input.filename.clone(),
                InputBlobRef {
                    key: InputBlobKey {
                        admission_session_id: Uuid::nil(),
                        input_id: Uuid::from_u128(index as u128 + 1),
                    },
                    storage_backend: "preflight".to_owned(),
                    object_key: format!("preflight/{index}"),
                    sha256_hex: hex::encode(Sha256::digest(&input.bytes)),
                    byte_size: input.bytes.len() as u64,
                },
                input.media_type.clone(),
            )
            .map_err(video_admission_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    intent
        .clone()
        .bind_grok_cli(projected)
        .map(|_| ())
        .map_err(video_admission_error)
}

async fn stage_and_bind(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
    intent: XaiVideoAdmissionIntent,
    inputs: &[DecodedVideoInput],
) -> Result<crate::admission::XaiVideoAdmissionPlan, ImageGatewayError> {
    let mut staged = Vec::with_capacity(inputs.len());
    for input in inputs {
        let blob = state
            .input_blob_store
            .put(
                InputBlobKey {
                    admission_session_id: ticket.session_id,
                    input_id: Uuid::new_v4(),
                },
                &input.bytes,
            )
            .await
            .map_err(map_input_write_error)?;
        staged.push(
            XaiVideoAdmissionInput::new(input.filename.clone(), blob, input.media_type.clone())
                .map_err(video_admission_error)?,
        );
    }
    intent.bind_grok_cli(staged).map_err(video_admission_error)
}

pub(super) fn video_response(status: VideoResultStatus) -> XaiVideoResponse {
    match status {
        VideoResultStatus::Pending { model, .. } | VideoResultStatus::Uncertain { model, .. } => {
            XaiVideoResponse {
                status: "pending".to_owned(),
                error: None,
                model: Some(model),
                progress: None,
                usage: None,
                video: None,
            }
        }
        VideoResultStatus::Succeeded {
            model,
            duration,
            artifact_id,
        } => XaiVideoResponse {
            status: "done".to_owned(),
            error: None,
            model: Some(model),
            progress: Some(100),
            usage: None,
            video: Some(XaiGeneratedVideo {
                duration,
                respect_moderation: true,
                file_output: None,
                storage_error: None,
                url: Some(format!("/v1/files/{artifact_id}/content")),
            }),
        },
        VideoResultStatus::Failed { error_code, .. } => XaiVideoResponse {
            status: "failed".to_owned(),
            error: Some(map_terminal_error(error_code.as_deref())),
            model: None,
            progress: None,
            usage: None,
            video: None,
        },
    }
}

fn map_terminal_error(error_code: Option<&str>) -> image_api_contracts::xai::XaiVideoError {
    let code = match error_code {
        Some("permission_denied" | "authentication_failed") => "permission_denied",
        Some("invalid_argument" | "executor_command_rejected") => "invalid_argument",
        Some("failed_precondition") => "failed_precondition",
        Some("service_unavailable" | "timeout" | "grok_cli_failed") => "service_unavailable",
        _ => "internal_error",
    };
    image_api_contracts::xai::XaiVideoError {
        code: code.to_owned(),
        message: "Video generation failed".to_owned(),
    }
}

fn video_admission_error(error: XaiVideoAdmissionError) -> ImageGatewayError {
    match error {
        XaiVideoAdmissionError::InvalidRequest(error) => ImageGatewayError::invalid_request(
            error.to_string(),
            Some(error.parameter().to_owned()),
            "invalid_value",
        ),
        XaiVideoAdmissionError::UnsupportedBinding(error) => ImageGatewayError::unsupported(
            error.parameter().unwrap_or("request"),
            error.to_string(),
        ),
        XaiVideoAdmissionError::InvalidInputManifest => ImageGatewayError::invalid_request(
            "Video input manifest is invalid",
            Some("image".to_owned()),
            "invalid_value",
        ),
        XaiVideoAdmissionError::InvalidProviderCommand => {
            ImageGatewayError::internal("failed to encode durable video command")
        }
    }
}

fn unsupported_video_input(param: &str) -> ImageGatewayError {
    ImageGatewayError::unsupported(
        param,
        "Grok CLI video inputs currently require base64 data URLs; file_id is retained by the official DTO but is not bound",
    )
}

fn video_idempotency_digest(
    headers: &HeaderMap,
    auth: &AuthContext,
) -> Result<Option<String>, ImageGatewayError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ImageGatewayError::invalid_idempotency_key())?;
    let scope = auth.actor_user_id.map_or_else(
        || auth.project_id.clone(),
        |user_id| format!("{}:user:{user_id}", auth.project_id),
    );
    idempotency_key_digest(
        &scope,
        XAI_VIDEOS_API_PROFILE,
        crate::admission::VIDEO_GENERATION_OPERATION,
        key,
    )
    .map(Some)
    .map_err(|_| ImageGatewayError::invalid_idempotency_key())
}

async fn claim_with_retry(
    state: &Arc<AppState>,
    claim: crate::admission::ClaimAdmission,
) -> Result<AdmissionClaim, AdmissionError> {
    for attempt in 0..RETRY_ATTEMPTS {
        match state.admission_store.claim(claim.clone()).await {
            Err(AdmissionError::Unavailable) if attempt + 1 < RETRY_ATTEMPTS => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            result => return result,
        }
    }
    Err(AdmissionError::Unavailable)
}

async fn reserve_with_retry(
    state: &Arc<AppState>,
    charge: UsageCharge,
) -> Result<UsageReservation, ImageGatewayError> {
    let mut last_error = ImageGatewayError::service_unavailable("quota state unavailable");
    for attempt in 0..RETRY_ATTEMPTS {
        match state.usage_store.reserve(charge.clone()).await {
            Ok(reservation) => return Ok(reservation),
            Err(error)
                if error.error_code() == Some("service_unavailable")
                    && attempt + 1 < RETRY_ATTEMPTS =>
            {
                last_error = error;
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error)
}

async fn attach_with_retry(
    state: &Arc<AppState>,
    attach: crate::admission::AttachJob,
) -> Result<(), AdmissionError> {
    for attempt in 0..RETRY_ATTEMPTS {
        match state.admission_store.attach(attach.clone()).await {
            Ok(_) => return Ok(()),
            Err(AdmissionError::Unavailable) if attempt + 1 < RETRY_ATTEMPTS => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(AdmissionError::Unavailable)
}

async fn rollback_session(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
) -> Result<(), ImageGatewayError> {
    state
        .input_blob_store
        .delete_session(ticket.session_id)
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("input cleanup unavailable"))?;
    state
        .admission_store
        .abort(ticket)
        .await
        .map_err(admission_error)
}

async fn rollback_reservation(
    state: &Arc<AppState>,
    ticket: &AdmissionTicket,
    reservation: &UsageReservation,
) -> Result<(), ImageGatewayError> {
    state
        .usage_store
        .release(reservation, "admission_attach_failed")
        .await?;
    rollback_session(state, ticket).await
}

fn map_input_write_error(_: InputBlobWriteError) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("video input storage unavailable")
}

fn admission_error(error: AdmissionError) -> ImageGatewayError {
    match error {
        AdmissionError::Expired => ImageGatewayError::timeout(),
        AdmissionError::BillingLimitExceeded => ImageGatewayError::billing_limit_exceeded(),
        AdmissionError::ProjectBudgetExceeded => ImageGatewayError::project_budget_exceeded(),
        AdmissionError::PricingUnavailable => {
            ImageGatewayError::service_unavailable("video pricing is unavailable")
        }
        AdmissionError::Unavailable => {
            ImageGatewayError::service_unavailable("durable video admission is unavailable")
        }
        AdmissionError::InvalidOwner
        | AdmissionError::StaleLease
        | AdmissionError::InvalidCommand => {
            ImageGatewayError::internal("durable video admission integrity check failed")
        }
    }
}

fn admission_deadline(state: &AppState) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let budget = state
        .config
        .queue_timeout
        .saturating_add(Duration::from_secs(5))
        .as_millis()
        .min(i64::MAX as u128) as i64;
    now.saturating_add(budget)
}

fn start_response(job_id: Uuid) -> XaiStartDeferredResponse {
    XaiStartDeferredResponse {
        request_id: job_id.to_string(),
    }
}

fn parse_public_uuid(value: &str, param: &str) -> Result<Uuid, ImageGatewayError> {
    Uuid::parse_str(value).map_err(|_| video_not_found(param))
}

fn video_not_found(param: &str) -> ImageGatewayError {
    ImageGatewayError::not_found(
        "Video request or file not found",
        Some(param.to_owned()),
        "not_found",
    )
}

#[cfg(test)]
mod tests {
    use image_api_contracts::xai::XaiVideoRequestError;

    use super::*;

    #[test]
    fn terminal_video_status_uses_official_async_shape() {
        let artifact_id = Uuid::new_v4();
        let response = video_response(VideoResultStatus::Succeeded {
            model: "grok-imagine-video-1.5-preview".to_owned(),
            duration: 6,
            artifact_id,
        });
        assert_eq!(response.status, "done");
        assert_eq!(response.progress, Some(100));
        assert_eq!(
            response.video.unwrap().url,
            Some(format!("/v1/files/{artifact_id}/content"))
        );
    }

    #[test]
    fn uncertain_internal_state_remains_pending_at_the_xai_boundary() {
        let response = video_response(VideoResultStatus::Uncertain {
            model: "grok-imagine-video-1.5-preview".to_owned(),
            duration: 6,
        });
        assert_eq!(response.status, "pending");
        assert!(response.error.is_none());
    }

    #[test]
    fn failed_status_omits_model_like_the_official_contract() {
        let response = video_response(VideoResultStatus::Failed {
            model: "grok-imagine-video-1.5-preview".to_owned(),
            duration: 6,
            error_code: Some("grok_cli_failed".to_owned()),
        });
        assert_eq!(response.status, "failed");
        assert!(response.model.is_none());
        assert!(response.video.is_none());
        assert_eq!(response.error.unwrap().code, "service_unavailable");
    }

    #[test]
    fn official_request_errors_keep_their_parameter() {
        let error = video_admission_error(XaiVideoAdmissionError::InvalidRequest(
            XaiVideoRequestError::InvalidDuration,
        ));
        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), Some("invalid_value"));
    }

    #[test]
    fn billing_limit_is_distinct_from_video_rate_limiting() {
        let error = admission_error(AdmissionError::BillingLimitExceeded);
        assert_eq!(error.status_code(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error.error_code(), Some("billing_limit_exceeded"));
    }
}
