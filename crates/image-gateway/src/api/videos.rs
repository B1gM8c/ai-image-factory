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
    input_blobs::{InputBlobKey, InputBlobWriteError},
    settlement::{StoredVideoArtifact, VideoResultStatus},
    usage::{UsageCharge, UsageLimits, UsageReservation},
};

use super::{
    AppState, GenerationExecutionMode, RequestId, authenticate_image_request,
    resolve_request_model,
    video_inputs::{DecodedVideoInput, decode_video_inputs_v1, decode_video_inputs_v2},
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
    // Keep the public API's historical defaults stable.  The V2 model is an
    // explicit routed/canary surface and must not become the implicit model
    // merely because the V2 executor is installed.
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
    let api_version = select_video_api_version(
        auth.route
            .as_ref()
            .map(|route| route.command_schema.as_str()),
        &request,
    )?;
    let (intent, decoded) = match api_version {
        VideoApiVersion::V1 => {
            let intent = XaiVideoAdmissionIntent::new(request).map_err(video_admission_error)?;
            let decoded =
                decode_video_inputs_v1(intent.source_command(), state.config.max_upload_bytes)?;
            preflight_grok_binding_v1(&intent, &decoded)?;
            (intent, decoded)
        }
        VideoApiVersion::V2 => {
            let intent = XaiVideoAdmissionIntent::new_v2(request).map_err(video_admission_error)?;
            preflight_grok_binding_v2(&intent)?;
            let decoded = decode_video_inputs_v2(
                intent.source_command_v2().ok_or_else(|| {
                    ImageGatewayError::internal("video V2 intent missing source command")
                })?,
                state.config.max_upload_bytes,
            )
            .await?;
            (intent, decoded)
        }
    };
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VideoApiVersion {
    V1,
    V2,
}

fn select_video_api_version(
    route_schema: Option<&str>,
    request: &XaiVideoGenerationRequest,
) -> Result<VideoApiVersion, ImageGatewayError> {
    let version = match route_schema {
        None => VideoApiVersion::V1,
        Some(image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA) => VideoApiVersion::V1,
        Some(image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2) => {
            VideoApiVersion::V2
        }
        Some(schema) => {
            return Err(ImageGatewayError::invalid_request(
                format!("video route command schema is unsupported: {schema}"),
                Some("model".to_owned()),
                "invalid_value",
            ));
        }
    };

    let model = request.model.as_deref();
    let model_allowed = match version {
        VideoApiVersion::V1 => model.is_none_or(|model| {
            matches!(
                model,
                "grok-imagine-video" | "grok-imagine-video-1.5-preview"
            )
        }),
        VideoApiVersion::V2 => model.is_some_and(|model| model == "grok-imagine-video-1.5"),
    };
    if !model_allowed {
        return Err(ImageGatewayError::invalid_request(
            "video model does not match the selected route command schema",
            Some("model".to_owned()),
            "invalid_value",
        ));
    }
    Ok(version)
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
    let (duration, resolution, input_image_count, aspect_ratio, workflow) =
        if let Some(command) = plan.source_command_v2() {
            (
                command.duration,
                command.resolution,
                usize::from(command.image.is_some())
                    + usize::from(command.last_frame.is_some())
                    + command.reference_images.len(),
                command.aspect_ratio,
                command.workflow(),
            )
        } else {
            let command = plan.source_command();
            (
                command.duration,
                command.resolution,
                usize::from(command.image.is_some()) + command.reference_images.len(),
                command.aspect_ratio,
                command.workflow(),
            )
        };
    let mut dimensions = BTreeMap::from([
        ("duration".to_owned(), duration.to_string()),
        (
            "input_image_count".to_owned(),
            enum_or_integer_wire_value(input_image_count)?,
        ),
        (
            "resolution".to_owned(),
            enum_or_integer_wire_value(resolution)?,
        ),
    ]);
    if matches!(
        workflow,
        XaiVideoWorkflow::TextToVideo | XaiVideoWorkflow::ReferenceToVideo
    ) {
        dimensions.insert(
            "aspect_ratio".to_owned(),
            enum_or_integer_wire_value(
                aspect_ratio.unwrap_or(image_api_contracts::xai::XaiVideoAspectRatio::R16x9),
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

fn preflight_grok_binding_v2(intent: &XaiVideoAdmissionIntent) -> Result<(), ImageGatewayError> {
    let command = intent
        .source_command_v2()
        .ok_or_else(|| ImageGatewayError::internal("video V2 intent missing source command"))?;
    match image_provider_grok_cli::GrokVideoGenerationPayloadV2::preflight(command) {
        Ok(()) => Ok(()),
        Err(image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedFileId) => {
            Err(ImageGatewayError::unsupported(
                &unsupported_file_id_parameter(command),
                "xAI file_id inputs are not supported by Grok CLI",
            ))
        }
        Err(image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedOutput) => {
            Err(ImageGatewayError::unsupported(
                "output",
                "video output delivery is not supported by Grok CLI",
            ))
        }
        Err(image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedStorageOptions) => {
            Err(ImageGatewayError::unsupported(
                "storage_options",
                "video storage options are not supported by Grok CLI",
            ))
        }
        Err(error) => Err(video_admission_error(
            XaiVideoAdmissionError::UnsupportedBindingV2(error),
        )),
    }
}

fn preflight_grok_binding_v1(
    intent: &XaiVideoAdmissionIntent,
    inputs: &[DecodedVideoInput],
) -> Result<(), ImageGatewayError> {
    let projected = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            XaiVideoAdmissionInput::new(
                input.filename.clone(),
                crate::input_blobs::InputBlobRef {
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

fn unsupported_file_id_parameter(
    command: &image_api_contracts::xai::XaiVideoGenerationCommandV2,
) -> String {
    if command
        .image
        .as_ref()
        .is_some_and(|image| image.file_id.is_some())
    {
        return "image".to_owned();
    }
    if command
        .last_frame
        .as_ref()
        .is_some_and(|image| image.file_id.is_some())
    {
        return "last_frame".to_owned();
    }
    if let Some(index) = command
        .reference_images
        .iter()
        .position(|image| image.file_id.is_some())
    {
        return format!("reference_images[{index}]");
    }
    "image".to_owned()
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
        Some(
            "service_unavailable"
            | "timeout"
            | "grok_cli_failed"
            | "grok_credential_refresh_pending",
        ) => "service_unavailable",
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
        XaiVideoAdmissionError::UnsupportedBindingV2(error) => ImageGatewayError::unsupported(
            match error {
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::InvalidSourceCommand
                | image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::InputManifestMismatch =>
                    "request",
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::ModelRequired
                | image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedModel =>
                    "model",
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedGenerateAudio =>
                    "generate_audio",
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedReferenceAudioUrl
                | image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::InvalidVoiceId =>
                    "reference_audios",
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedFileId => {
                    "image"
                }
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedOutput => {
                    "output"
                }
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedStorageOptions => {
                    "storage_options"
                }
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedDuration => {
                    "duration"
                }
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedResolution => {
                    "resolution"
                }
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::UnsupportedAspectRatio => {
                    "aspect_ratio"
                }
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::InputCountExceeded => {
                    "reference_images"
                }
                image_provider_grok_cli::XaiGrokVideoProjectionErrorV2::InvalidRequest(_) => {
                    "prompt"
                }
            },
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
    use image_api_contracts::xai::{
        XaiVideoAspectRatio, XaiVideoImageUrl, XaiVideoOutput, XaiVideoRequestError,
        XaiVideoResolution, XaiVideoStorageOptions,
    };

    use crate::admission::XaiVideoAdmissionPlan;
    use crate::input_blobs::InputBlobRef;

    fn version_request(model: Option<&str>) -> XaiVideoGenerationRequest {
        XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            generate_audio: None,
            image: None,
            last_frame: None,
            model: model.map(str::to_owned),
            output: None,
            prompt: Some("a paper boat on a lake".to_owned()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        }
    }

    #[test]
    fn video_route_schema_selects_v1_or_v2_and_rejects_crossed_models() {
        assert_eq!(
            select_video_api_version(None, &version_request(None)).unwrap(),
            VideoApiVersion::V1
        );
        assert_eq!(
            select_video_api_version(
                Some(image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA),
                &version_request(Some("grok-imagine-video")),
            )
            .unwrap(),
            VideoApiVersion::V1
        );
        assert_eq!(
            select_video_api_version(
                Some(image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2),
                &version_request(Some("grok-imagine-video-1.5")),
            )
            .unwrap(),
            VideoApiVersion::V2
        );
        assert!(
            select_video_api_version(
                Some(image_provider_grok_cli::GROK_VIDEO_GENERATION_COMMAND_SCHEMA),
                &version_request(Some("grok-imagine-video-1.5")),
            )
            .is_err()
        );
        assert!(
            select_video_api_version(Some("unknown.video.schema"), &version_request(None),)
                .is_err()
        );
    }

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
    fn credential_refresh_pending_is_retryable_at_the_xai_boundary() {
        let error = map_terminal_error(Some("grok_credential_refresh_pending"));
        assert_eq!(error.code, "service_unavailable");
    }

    #[test]
    fn v2_capability_rejection_precedes_input_fetch() {
        let request = XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(8),
            generate_audio: Some(false),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("https://127.0.0.1/private.png".to_owned()),
            }),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: Some("wind in grass".to_owned()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        };
        let intent = XaiVideoAdmissionIntent::new_v2(request).unwrap();
        let error = preflight_grok_binding_v2(&intent).unwrap_err();
        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), Some("unsupported_parameter"));
    }

    #[test]
    fn v2_delivery_rejections_are_provider_scoped_and_precede_input_fetch() {
        let mut output = XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(8),
            generate_audio: Some(true),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("https://127.0.0.1/private.png".to_owned()),
            }),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: Some(XaiVideoOutput {
                upload_url: "https://upload.example/video".to_owned(),
            }),
            prompt: Some("wind in grass".to_owned()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        };
        let intent = XaiVideoAdmissionIntent::new_v2(output.clone()).unwrap();
        let error = preflight_grok_binding_v2(&intent).unwrap_err();
        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), Some("unsupported_parameter"));
        assert!(format!("{error:?}").contains("output"));

        output.output = None;
        output.storage_options = Some(XaiVideoStorageOptions {
            expires_after: Some(3_600),
            filename: "video.mp4".to_owned(),
            public_url: None,
        });
        let intent = XaiVideoAdmissionIntent::new_v2(output).unwrap();
        let error = preflight_grok_binding_v2(&intent).unwrap_err();
        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), Some("unsupported_parameter"));
        assert!(format!("{error:?}").contains("storage_options"));
    }

    #[test]
    fn v2_file_id_errors_identify_the_source_slot_before_fetch() {
        let mut cases = Vec::new();
        let mut image = XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            generate_audio: Some(true),
            image: Some(XaiVideoImageUrl {
                file_id: Some("file-image".to_owned()),
                url: None,
            }),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: Some("move".to_owned()),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: None,
        };
        cases.push((image.clone(), "image"));
        image.image = None;
        image.last_frame = Some(XaiVideoImageUrl {
            file_id: Some("file-last".to_owned()),
            url: None,
        });
        cases.push((image.clone(), "last_frame"));
        image.last_frame = None;
        image.reference_images = vec![
            XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/png;base64,iVBORw0KGgo=".to_owned()),
            },
            XaiVideoImageUrl {
                file_id: Some("file-ref".to_owned()),
                url: None,
            },
        ];
        cases.push((image, "reference_images[1]"));

        for (request, expected_parameter) in cases {
            let intent = XaiVideoAdmissionIntent::new_v2(request).expect("valid V2 request");
            let error = preflight_grok_binding_v2(&intent).expect_err("file_id must be rejected");
            assert_eq!(error.error_code(), Some("unsupported_parameter"));
            assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
            assert_eq!(
                expected_parameter,
                unsupported_file_id_parameter(intent.source_command_v2().unwrap())
            );
            assert!(format!("{error:?}").contains(expected_parameter));
        }
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

    #[test]
    fn grok_text_video_pricing_keeps_aspect_ratio_for_preview_model() {
        let plan = XaiVideoAdmissionPlan::for_grok_cli(
            XaiVideoGenerationRequest {
                aspect_ratio: Some(XaiVideoAspectRatio::R16x9),
                duration: Some(6),
                generate_audio: None,
                image: None,
                last_frame: None,
                model: Some("grok-imagine-video-1.5-preview".to_owned()),
                output: None,
                prompt: Some("a sunrise over the ocean".to_owned()),
                reference_audios: Vec::new(),
                reference_images: Vec::new(),
                resolution: Some(XaiVideoResolution::P480),
                storage_options: None,
                user: None,
            },
            Vec::new(),
        )
        .expect("valid Grok text-to-video plan");

        assert_eq!(
            video_pricing_dimensions(&plan).expect("pricing dimensions"),
            BTreeMap::from([
                ("aspect_ratio".to_owned(), "16:9".to_owned()),
                ("duration".to_owned(), "6".to_owned()),
                ("input_image_count".to_owned(), "0".to_owned()),
                ("resolution".to_owned(), "480p".to_owned()),
            ])
        );
    }

    #[test]
    fn grok_image_video_pricing_omits_forbidden_aspect_ratio() {
        let session_id = Uuid::new_v4();
        let plan = XaiVideoAdmissionPlan::for_grok_cli(
            XaiVideoGenerationRequest {
                aspect_ratio: None,
                duration: Some(10),
                generate_audio: None,
                image: Some(XaiVideoImageUrl {
                    file_id: None,
                    url: Some("data:image/png;base64,AA==".to_owned()),
                }),
                last_frame: None,
                model: Some("grok-imagine-video-1.5-preview".to_owned()),
                output: None,
                prompt: Some("the camera slowly moves forward".to_owned()),
                reference_audios: Vec::new(),
                reference_images: Vec::new(),
                resolution: Some(XaiVideoResolution::P720),
                storage_options: None,
                user: None,
            },
            vec![
                XaiVideoAdmissionInput::new(
                    "input.png",
                    InputBlobRef {
                        key: InputBlobKey {
                            admission_session_id: session_id,
                            input_id: Uuid::new_v4(),
                        },
                        storage_backend: "test".to_owned(),
                        object_key: "input.png".to_owned(),
                        sha256_hex: "a".repeat(64),
                        byte_size: 1,
                    },
                    "image/png",
                )
                .expect("valid staged first frame"),
            ],
        )
        .expect("valid Grok image-to-video plan");

        assert_eq!(
            video_pricing_dimensions(&plan).expect("pricing dimensions"),
            BTreeMap::from([
                ("duration".to_owned(), "10".to_owned()),
                ("input_image_count".to_owned(), "1".to_owned()),
                ("resolution".to_owned(), "720p".to_owned()),
            ])
        );
    }
}
