use std::{collections::BTreeMap, sync::Arc};

use axum::{Json, http::HeaderMap, response::IntoResponse};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use image_api_contracts::xai::{
    XAI_IMAGES_API_PROFILE, XaiImageData, XaiImageGenerationRequest, XaiImagesResponse,
};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionContract, CustomerPricingIntent, XaiImageAdmissionError, XaiImageAdmissionPlan,
        idempotency_key_digest,
    },
    auth::AuthContext,
};

use super::{
    AppState,
    dreamina::{require_external_execution, submit_image_generation},
    images::{admission_deadline, generation_admission_contract},
};

pub(super) async fn create_image(
    state: &Arc<AppState>,
    auth: &AuthContext,
    headers: &HeaderMap,
    request_id: String,
    value: Value,
    public_model_id: String,
    execution_model_id: String,
) -> Result<axum::response::Response, ImageGatewayError> {
    require_external_execution(state)?;
    let request: XaiImageGenerationRequest = serde_json::from_value(value).map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid xAI image request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    let plan = XaiImageAdmissionPlan::for_grok_cli(request).map_err(admission_error)?;
    let idempotency_key_digest = idempotency_digest(headers, auth)?;
    let contract = generation_admission_contract(state.config.generation_admission_contract);
    let mut claim = plan.claim(
        Uuid::new_v4(),
        auth.tenant_id.clone(),
        auth.project_id.clone(),
        request_id.clone(),
        idempotency_key_digest,
        admission_deadline(&state.config),
    );
    if contract == AdmissionContract::CustomerPricingV4 {
        claim.request_hash = crate::service_tiers::request_hash_with_project_service_tier(
            &claim.request_hash,
            auth.project_service_tier,
        );
    }
    let output_count = plan.source_command().n;
    let service_tier_decision = crate::service_tiers::ServiceTierDecision::for_default_only_project(
        auth.project_service_tier,
    );
    let result = submit_image_generation(
        state,
        auth,
        request_id,
        claim,
        plan.provider_id(),
        plan.provider_model(),
        output_count,
        |ticket, job_id, schedule_scope| {
            let mut attach = plan.attach(ticket, job_id, schedule_scope, contract);
            if contract == AdmissionContract::CustomerPricingV4 {
                attach.customer_pricing = Some(CustomerPricingIntent {
                    public_model_id: public_model_id.clone(),
                    provider_model_id: plan.provider_model().to_string(),
                    execution_model_id: execution_model_id.clone(),
                    provider_command_hash: None,
                    media_kind: "image".to_string(),
                    service_tier: service_tier_decision.effective.pricing_key().to_string(),
                    service_tier_decision,
                    execution_surface: "provider_cli".to_string(),
                    currency: "USD".to_string(),
                    pricing_dimensions: BTreeMap::from([
                        (
                            "aspect_ratio".to_string(),
                            enum_wire_value(plan.source_command().aspect_ratio),
                        ),
                        (
                            "resolution".to_string(),
                            enum_wire_value(plan.source_command().resolution),
                        ),
                    ]),
                    processing_mode: crate::admission::PricingProcessingMode::Synchronous,
                });
            }
            attach
        },
    )
    .await?;
    let data = result
        .images
        .into_iter()
        .map(|image| {
            let mime_type = crate::artifacts::media_type_from_bytes(&image.bytes)
                .map_err(|_| ImageGatewayError::artifact_integrity())?;
            Ok(XaiImageData {
                b64_json: Some(STANDARD.encode(image.bytes)),
                file_output: None,
                mime_type: Some(mime_type.to_owned()),
                revised_prompt: None,
                storage_error: None,
                url: None,
            })
        })
        .collect::<Result<Vec<_>, ImageGatewayError>>()?;
    Ok(Json(XaiImagesResponse { data, usage: None }).into_response())
}

fn enum_wire_value(value: impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .expect("xAI pricing dimensions serialize as string enums")
}

fn idempotency_digest(
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
        XAI_IMAGES_API_PROFILE,
        crate::admission::GENERATION_OPERATION,
        key,
    )
    .map(Some)
    .map_err(|_| ImageGatewayError::invalid_idempotency_key())
}

fn admission_error(error: XaiImageAdmissionError) -> ImageGatewayError {
    match error {
        XaiImageAdmissionError::InvalidRequest(error) => ImageGatewayError::invalid_request(
            error.to_string(),
            Some(error.parameter().to_owned()),
            "invalid_value",
        ),
        XaiImageAdmissionError::UnsupportedBinding(error) => ImageGatewayError::unsupported(
            error.parameter().unwrap_or("request"),
            error.to_string(),
        ),
        XaiImageAdmissionError::InvalidProviderCommand => {
            ImageGatewayError::internal("failed to encode durable xAI image command")
        }
    }
}
