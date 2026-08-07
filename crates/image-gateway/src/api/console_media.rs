use std::{collections::BTreeSet, sync::Arc};

use axum::{
    Json,
    extract::{Extension, Path, Request, State},
    http::HeaderMap,
    response::Response,
};
use image_api_contracts::{
    ark::{ARK_IMAGES_API_PROFILE, ArkImageGenerationRequest, ArkSequentialImageGenerationOptions},
    dreamina::{DREAMINA_IMAGES_API_PROFILE, DreaminaImageGenerationRequest},
};
use image_provider_contracts::SpatialEditMode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    ImageGatewayError,
    auth::ApiKeyCapability,
    model_routing::{PublicModelRoute, ResolvedModelRoute},
};

use super::{
    AppState, IMAGE_EDIT_ROUTE_OPERATION, IMAGE_GENERATION_ROUTE_OPERATION, RequestId, ark,
    authenticate_project_media_request, dreamina, filter_project_models,
    images::{self, ConsoleSpatialEditMode, generate_with_resolved_auth},
    list_project_media_models, resolve_surface_model,
    sessions::private_json,
};

const OPENAI_IMAGES_API_PROFILE: &str = "openai-images-v1";
const XAI_IMAGES_API_PROFILE: &str = "xai-images-v1";
const SPATIAL_EDIT_MODE_HEADER: &str = "x-ai-factory-spatial-edit-mode";

#[derive(Serialize)]
struct ConsoleMediaModels {
    object: &'static str,
    data: Vec<ConsoleMediaModel>,
}

#[derive(Serialize)]
struct ConsoleMediaModel {
    id: String,
    provider: String,
    api_profile: String,
    media_kind: String,
    operation: String,
    created: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_prompt_chars: Option<usize>,
    supports_edit: bool,
    spatial_edit_mode: SpatialEditMode,
    max_reference_images: u32,
    controls: ConsoleImageControls,
}

#[derive(Serialize)]
struct ConsoleImageControls {
    aspect_ratio: ConsoleChoiceControl,
    count: ConsoleRangeControl,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolution: Option<ConsoleChoiceControl>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quality: Option<ConsoleChoiceControl>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_format: Option<ConsoleChoiceControl>,
    #[serde(skip_serializing_if = "Option::is_none")]
    background: Option<ConsoleChoiceControl>,
}

#[derive(Serialize)]
struct ConsoleChoiceControl {
    default: &'static str,
    options: &'static [&'static str],
}

#[derive(Serialize)]
struct ConsoleRangeControl {
    default: u32,
    min: u32,
    max: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConsoleImageGenerationRequest {
    model: String,
    prompt: String,
    #[serde(default)]
    count: Option<u32>,
    #[serde(default)]
    aspect_ratio: Option<String>,
    #[serde(default)]
    resolution: Option<String>,
    #[serde(default)]
    quality: Option<String>,
    #[serde(default)]
    output_format: Option<String>,
    #[serde(default)]
    background: Option<String>,
}

pub(super) async fn image_models(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_project_media_request(
        &headers,
        &state,
        &project_id,
        "workspace:read",
        ApiKeyCapability::ModelsRead,
    )
    .await?;
    let generation_models =
        list_project_media_models(&state, &auth, IMAGE_GENERATION_ROUTE_OPERATION).await?;
    let edit_models = list_project_media_models(&state, &auth, IMAGE_EDIT_ROUTE_OPERATION).await?;
    let edit_models = filter_project_models(&state, &project_id, edit_models).await?;
    let edit_public_capabilities = edit_models
        .iter()
        .map(|model| {
            (
                model.provider_id.clone(),
                model.api_profile.clone(),
                model.id.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let edit_provider_capabilities = edit_models
        .into_iter()
        .filter_map(|model| {
            model
                .provider_model_id
                .map(|provider_model_id| (model.provider_id, model.api_profile, provider_model_id))
        })
        .collect::<BTreeSet<_>>();
    let models = prefer_official_dreamina_aliases(
        filter_project_models(&state, &project_id, generation_models).await?,
    )
    .into_iter()
    .filter(is_console_image_profile)
    .filter_map(|model| {
        let supports_edit = supports_image_edit(
            &model,
            &edit_public_capabilities,
            &edit_provider_capabilities,
        );
        console_model(model, supports_edit)
    })
    .collect();
    Ok(private_json(ConsoleMediaModels {
        object: "list",
        data: models,
    }))
}

pub(super) async fn edit_image(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    mut request: Request,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_project_media_request(
        &headers,
        &state,
        &project_id,
        "workspace:write",
        ApiKeyCapability::ImagesWrite,
    )
    .await?;
    if let Some(mode) = console_spatial_edit_mode(request.headers())? {
        request.extensions_mut().insert(mode);
    }
    images::edit_with_resolved_auth(&state, auth, request).await
}

fn console_spatial_edit_mode(
    headers: &HeaderMap,
) -> Result<Option<ConsoleSpatialEditMode>, ImageGatewayError> {
    let Some(value) = headers.get(SPATIAL_EDIT_MODE_HEADER) else {
        return Ok(None);
    };
    match value.to_str().ok() {
        Some("semantic_mask") => Ok(Some(ConsoleSpatialEditMode::SemanticMask)),
        _ => Err(ImageGatewayError::invalid_request(
            "x-ai-factory-spatial-edit-mode must be semantic_mask",
            Some("spatial_edit_mode".to_owned()),
            "invalid_value",
        )),
    }
}

pub(super) async fn generate_image(
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    body: Result<Json<ConsoleImageGenerationRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Response, ImageGatewayError> {
    let mut auth = authenticate_project_media_request(
        &headers,
        &state,
        &project_id,
        "workspace:write",
        ApiKeyCapability::ImagesWrite,
    )
    .await?;
    let Json(request) = body.map_err(|error| {
        ImageGatewayError::invalid_request(
            format!("Invalid JSON request: {error}"),
            None,
            "invalid_json",
        )
    })?;
    if request.model.trim().is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "model is required for console generation",
            Some("model".to_owned()),
            "invalid_request",
        ));
    }
    let resolved = resolve_surface_model(
        &state,
        &mut auth,
        IMAGE_GENERATION_ROUTE_OPERATION,
        &[
            OPENAI_IMAGES_API_PROFILE,
            XAI_IMAGES_API_PROFILE,
            DREAMINA_IMAGES_API_PROFILE,
            ARK_IMAGES_API_PROFILE,
        ],
        &request.model,
    )
    .await?
    .ok_or_else(|| ImageGatewayError::model_not_found(&request.model))?;
    match console_image_request(request, &resolved)? {
        ConsoleImageDispatchRequest::Standard(value) => {
            generate_with_resolved_auth(&state, auth, &headers, request_id.0, value, resolved).await
        }
        ConsoleImageDispatchRequest::Dreamina(request) => {
            dreamina::create_image_with_resolved_auth(
                &state,
                auth,
                &headers,
                request_id.0,
                request,
                resolved,
            )
            .await
        }
        ConsoleImageDispatchRequest::Ark(request) => {
            let response = ark::create_image_with_resolved_auth(
                &state,
                auth,
                &headers,
                request_id.0,
                request,
                resolved,
            )
            .await?;
            Ok(private_json(response))
        }
    }
}

fn is_console_image_profile(model: &PublicModelRoute) -> bool {
    matches!(
        model.api_profile.as_str(),
        OPENAI_IMAGES_API_PROFILE
            | XAI_IMAGES_API_PROFILE
            | DREAMINA_IMAGES_API_PROFILE
            | ARK_IMAGES_API_PROFILE
    )
}

fn supports_image_edit(
    model: &PublicModelRoute,
    public_capabilities: &BTreeSet<(String, String, String)>,
    provider_capabilities: &BTreeSet<(String, String, String)>,
) -> bool {
    public_capabilities.contains(&(
        model.provider_id.clone(),
        model.api_profile.clone(),
        model.id.clone(),
    )) || model
        .provider_model_id
        .as_ref()
        .is_some_and(|provider_model_id| {
            provider_capabilities.contains(&(
                model.provider_id.clone(),
                model.api_profile.clone(),
                provider_model_id.clone(),
            ))
        })
}

fn prefer_official_dreamina_aliases(models: Vec<PublicModelRoute>) -> Vec<PublicModelRoute> {
    let ark_routes = models
        .iter()
        .filter(|model| model.api_profile == ARK_IMAGES_API_PROFILE)
        .filter_map(|model| {
            model.provider_model_id.as_ref().map(|provider_model_id| {
                (
                    model.provider_id.clone(),
                    model.operation_id.clone(),
                    provider_model_id.clone(),
                )
            })
        })
        .collect::<BTreeSet<_>>();
    models
        .into_iter()
        .filter(|model| {
            model.api_profile != DREAMINA_IMAGES_API_PROFILE
                || !model
                    .provider_model_id
                    .as_ref()
                    .is_some_and(|provider_model_id| {
                        ark_routes.contains(&(
                            model.provider_id.clone(),
                            model.operation_id.clone(),
                            provider_model_id.clone(),
                        ))
                    })
        })
        .collect()
}

fn console_model(model: PublicModelRoute, supports_edit: bool) -> Option<ConsoleMediaModel> {
    let controls = controls_for_model(&model.api_profile, model.provider_model_id.as_deref())?;
    let max_prompt_chars = (model.provider_id == image_provider_grok_cli::PROVIDER_ID
        || model.api_profile == XAI_IMAGES_API_PROFILE)
        .then_some(image_provider_grok_cli::MAX_PROMPT_CHARS);
    let max_reference_images = match (supports_edit, model.api_profile.as_str()) {
        (true, OPENAI_IMAGES_API_PROFILE) => 16,
        (true, XAI_IMAGES_API_PROFILE) => {
            u32::try_from(image_provider_grok_cli::MAX_IMAGE_EDIT_REFERENCES)
                .expect("Grok reference image limit fits u32")
        }
        _ => 0,
    };
    let spatial_edit_mode = spatial_edit_mode_for_model(&model, supports_edit);
    Some(ConsoleMediaModel {
        id: model.id,
        provider: model.provider_id,
        api_profile: model.api_profile,
        media_kind: model.media_kind,
        operation: model.operation_id,
        created: model.created_at_ms.div_euclid(1_000),
        max_prompt_chars,
        supports_edit,
        spatial_edit_mode,
        max_reference_images,
        controls,
    })
}

fn spatial_edit_mode_for_model(model: &PublicModelRoute, supports_edit: bool) -> SpatialEditMode {
    if !supports_edit {
        return SpatialEditMode::Unsupported;
    }
    match model.provider_id.as_str() {
        image_provider_contracts::openai_codex::PROVIDER_ID => {
            image_provider_contracts::openai_codex::operation("images.edits")
                .map_or(SpatialEditMode::Unsupported, |operation| {
                    operation.spatial_edit_mode
                })
        }
        image_provider_grok_cli::PROVIDER_ID => {
            image_provider_grok_cli::GROK_IMAGE_EDIT_OPERATION_V1.spatial_edit_mode
        }
        _ => SpatialEditMode::Unsupported,
    }
}

fn controls_for_model(
    api_profile: &str,
    provider_model_id: Option<&str>,
) -> Option<ConsoleImageControls> {
    const STANDARD_RATIOS: &[&str] = &["1:1", "3:4", "4:3", "9:16", "16:9"];
    const DREAMINA_RATIOS: &[&str] = &["21:9", "16:9", "3:2", "4:3", "1:1", "3:4", "2:3", "9:16"];
    let xai = api_profile == XAI_IMAGES_API_PROFILE;
    if api_profile == DREAMINA_IMAGES_API_PROFILE || api_profile == ARK_IMAGES_API_PROFILE {
        let provider_model_id = provider_model_id?;
        let native = api_profile == DREAMINA_IMAGES_API_PROFILE;
        let resolutions = match (native, provider_model_id) {
            (true, "3.0" | "3.1") => &["1k", "2k"][..],
            (true, "5.0Pro") => &["1k", "2k", "4k"][..],
            (true, "4.0" | "4.1" | "4.5" | "4.6" | "4.7" | "5.0") => &["2k", "4k"][..],
            (false, "5.0" | "5.0Pro") => &["2k", "4k"][..],
            _ => return None,
        };
        return Some(ConsoleImageControls {
            aspect_ratio: ConsoleChoiceControl {
                default: if native { "16:9" } else { "1:1" },
                options: DREAMINA_RATIOS,
            },
            count: ConsoleRangeControl {
                default: 1,
                min: 1,
                max: 10,
            },
            resolution: Some(ConsoleChoiceControl {
                default: "2k",
                options: resolutions,
            }),
            quality: None,
            output_format: None,
            background: None,
        });
    }
    if !matches!(
        api_profile,
        OPENAI_IMAGES_API_PROFILE | XAI_IMAGES_API_PROFILE
    ) {
        return None;
    }
    Some(ConsoleImageControls {
        aspect_ratio: ConsoleChoiceControl {
            default: "1:1",
            options: STANDARD_RATIOS,
        },
        count: ConsoleRangeControl {
            default: 1,
            min: 1,
            max: if xai { 1 } else { 4 },
        },
        resolution: xai.then_some(ConsoleChoiceControl {
            default: "1k",
            options: &["1k"],
        }),
        quality: (!xai).then_some(ConsoleChoiceControl {
            default: "auto",
            options: &["auto", "high", "medium", "low"],
        }),
        output_format: (!xai).then_some(ConsoleChoiceControl {
            default: "png",
            options: &["png", "jpeg", "webp"],
        }),
        background: (!xai).then_some(ConsoleChoiceControl {
            default: "auto",
            options: &["auto", "opaque"],
        }),
    })
}

enum ConsoleImageDispatchRequest {
    Standard(Value),
    Dreamina(DreaminaImageGenerationRequest),
    Ark(ArkImageGenerationRequest),
}

fn console_image_request(
    request: ConsoleImageGenerationRequest,
    resolved: &ResolvedModelRoute,
) -> Result<ConsoleImageDispatchRequest, ImageGatewayError> {
    let controls = controls_for_model(&resolved.api_profile, Some(&resolved.provider_model_id))
        .ok_or_else(|| {
            ImageGatewayError::unsupported(
                "model",
                "model is not supported by the console images workflow",
            )
        })?;
    let count = request.count.unwrap_or(controls.count.default);
    if !(controls.count.min..=controls.count.max).contains(&count) {
        return Err(ImageGatewayError::invalid_request(
            format!(
                "count must be between {} and {} for this model",
                controls.count.min, controls.count.max
            ),
            Some("count".to_owned()),
            "invalid_value",
        ));
    }
    let aspect_ratio = request
        .aspect_ratio
        .unwrap_or_else(|| controls.aspect_ratio.default.to_owned());
    if !controls
        .aspect_ratio
        .options
        .contains(&aspect_ratio.as_str())
    {
        return Err(ImageGatewayError::invalid_request(
            "aspect_ratio is not supported by this model",
            Some("aspect_ratio".to_owned()),
            "invalid_value",
        ));
    }
    match resolved.api_profile.as_str() {
        OPENAI_IMAGES_API_PROFILE => {
            let quality = validated_choice(request.quality, controls.quality.as_ref(), "quality")?;
            let output_format = validated_choice(
                request.output_format,
                controls.output_format.as_ref(),
                "output_format",
            )?;
            let background = validated_choice(
                request.background,
                controls.background.as_ref(),
                "background",
            )?;
            Ok(ConsoleImageDispatchRequest::Standard(json!({
                "model": request.model,
                "prompt": request.prompt,
                "n": count,
                "size": aspect_ratio,
                "quality": quality,
                "output_format": output_format,
                "background": background,
                "response_format": "b64_json"
            })))
        }
        XAI_IMAGES_API_PROFILE => {
            reject_unsupported(request.quality, "quality")?;
            reject_unsupported(request.output_format, "output_format")?;
            reject_unsupported(request.background, "background")?;
            let resolution = request
                .resolution
                .unwrap_or_else(|| controls.resolution.as_ref().unwrap().default.to_owned());
            if !controls
                .resolution
                .as_ref()
                .is_some_and(|control| control.options.contains(&resolution.as_str()))
            {
                return Err(ImageGatewayError::invalid_request(
                    "resolution is not supported by this model",
                    Some("resolution".to_owned()),
                    "invalid_value",
                ));
            }
            Ok(ConsoleImageDispatchRequest::Standard(json!({
                "model": request.model,
                "prompt": request.prompt,
                "n": count,
                "aspect_ratio": aspect_ratio,
                "resolution": resolution,
                "response_format": "b64_json"
            })))
        }
        DREAMINA_IMAGES_API_PROFILE => {
            reject_unsupported(request.quality, "quality")?;
            reject_unsupported(request.output_format, "output_format")?;
            reject_unsupported(request.background, "background")?;
            let resolution = validated_resolution(request.resolution, &controls)?;
            Ok(ConsoleImageDispatchRequest::Dreamina(
                DreaminaImageGenerationRequest {
                    prompt: request.prompt,
                    model_version: Some(resolved.provider_model_id.clone()),
                    ratio: Some(aspect_ratio),
                    resolution_type: resolution,
                    width: None,
                    height: None,
                    generate_num: Some(count as u8),
                },
            ))
        }
        ARK_IMAGES_API_PROFILE => {
            reject_unsupported(request.quality, "quality")?;
            reject_unsupported(request.output_format, "output_format")?;
            reject_unsupported(request.background, "background")?;
            let resolution = validated_resolution(request.resolution, &controls)?;
            let sequential = (count > 1).then_some("auto".to_owned());
            let sequential_options = (count > 1).then_some(ArkSequentialImageGenerationOptions {
                max_images: Some(count as u8),
            });
            Ok(ConsoleImageDispatchRequest::Ark(
                ArkImageGenerationRequest {
                    model: request.model,
                    prompt: request.prompt,
                    image: None,
                    response_format: Some("b64_json".to_owned()),
                    size: Some(ark_image_size(&resolution, &aspect_ratio)?.to_owned()),
                    seed: None,
                    guidance_scale: None,
                    watermark: None,
                    optimize_prompt: None,
                    optimize_prompt_options: None,
                    sequential_image_generation: sequential,
                    sequential_image_generation_options: sequential_options,
                    tools: None,
                    output_format: None,
                    stream: Some(false),
                },
            ))
        }
        _ => Err(ImageGatewayError::service_unavailable(
            "model route does not match the console images surface",
        )),
    }
}

fn validated_resolution(
    value: Option<String>,
    controls: &ConsoleImageControls,
) -> Result<String, ImageGatewayError> {
    validated_choice(value, controls.resolution.as_ref(), "resolution")
}

fn ark_image_size<'a>(
    resolution: &'a str,
    aspect_ratio: &str,
) -> Result<&'a str, ImageGatewayError> {
    let size = match (resolution, aspect_ratio) {
        ("2k", "1:1") => "2K",
        ("2k", "21:9") => "2048x878",
        ("2k", "16:9") => "2048x1152",
        ("2k", "3:2") => "2048x1365",
        ("2k", "4:3") => "2048x1536",
        ("2k", "3:4") => "1536x2048",
        ("2k", "2:3") => "1365x2048",
        ("2k", "9:16") => "1152x2048",
        ("4k", "1:1") => "4K",
        ("4k", "21:9") => "4096x1755",
        ("4k", "16:9") => "4096x2304",
        ("4k", "3:2") => "4096x2731",
        ("4k", "4:3") => "4096x3072",
        ("4k", "3:4") => "3072x4096",
        ("4k", "2:3") => "2731x4096",
        ("4k", "9:16") => "2304x4096",
        _ => {
            return Err(ImageGatewayError::invalid_request(
                "resolution and aspect_ratio are not supported by this model",
                Some("resolution".to_owned()),
                "invalid_value",
            ));
        }
    };
    Ok(size)
}

fn validated_choice(
    value: Option<String>,
    control: Option<&ConsoleChoiceControl>,
    param: &str,
) -> Result<String, ImageGatewayError> {
    let control = control.ok_or_else(|| {
        ImageGatewayError::unsupported(param, format!("{param} is not supported by this model"))
    })?;
    let value = value.unwrap_or_else(|| control.default.to_owned());
    if !control.options.contains(&value.as_str()) {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} is not supported by this model"),
            Some(param.to_owned()),
            "invalid_value",
        ));
    }
    Ok(value)
}

fn reject_unsupported(value: Option<String>, param: &str) -> Result<(), ImageGatewayError> {
    if value.is_some() {
        return Err(ImageGatewayError::unsupported(
            param,
            format!("{param} is not supported by this model"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use uuid::Uuid;

    #[test]
    fn console_spatial_edit_fallback_requires_explicit_semantic_mask_header() {
        let mut headers = HeaderMap::new();
        assert_eq!(console_spatial_edit_mode(&headers).unwrap(), None);

        headers.insert(
            SPATIAL_EDIT_MODE_HEADER,
            HeaderValue::from_static("semantic_mask"),
        );
        assert_eq!(
            console_spatial_edit_mode(&headers).unwrap(),
            Some(ConsoleSpatialEditMode::SemanticMask)
        );

        headers.insert(
            SPATIAL_EDIT_MODE_HEADER,
            HeaderValue::from_static("visual_region"),
        );
        assert!(console_spatial_edit_mode(&headers).is_err());
    }

    #[test]
    fn console_catalog_serializes_grok_semantic_mask_capability() {
        let model = PublicModelRoute {
            id: "grok-imagine-image-quality".to_owned(),
            provider_model_id: Some("grok-imagine-image-quality".to_owned()),
            api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
            provider_id: image_provider_grok_cli::PROVIDER_ID.to_owned(),
            operation_id: IMAGE_GENERATION_ROUTE_OPERATION.to_owned(),
            media_kind: "image".to_owned(),
            created_at_ms: 0,
        };
        let model = console_model(model, true).expect("Grok console model");
        let value = serde_json::to_value(model).unwrap();

        assert_eq!(value["spatial_edit_mode"], "semantic_mask");
        assert_eq!(value["max_prompt_chars"], 1_024);
    }

    #[test]
    fn provider_model_binding_enables_edits_for_public_aliases() {
        let model = PublicModelRoute {
            id: "gpt-image-2-2026-04-21".to_owned(),
            provider_model_id: Some("gpt-image-2".to_owned()),
            api_profile: OPENAI_IMAGES_API_PROFILE.to_owned(),
            provider_id: image_provider_contracts::openai_codex::PROVIDER_ID.to_owned(),
            operation_id: IMAGE_GENERATION_ROUTE_OPERATION.to_owned(),
            media_kind: "image".to_owned(),
            created_at_ms: 0,
        };
        let public_capabilities = BTreeSet::new();
        let provider_capabilities = BTreeSet::from([(
            image_provider_contracts::openai_codex::PROVIDER_ID.to_owned(),
            OPENAI_IMAGES_API_PROFILE.to_owned(),
            "gpt-image-2".to_owned(),
        )]);

        assert!(supports_image_edit(
            &model,
            &public_capabilities,
            &provider_capabilities
        ));
    }

    #[test]
    fn openai_console_request_preserves_supported_advanced_controls() {
        let request = ConsoleImageGenerationRequest {
            model: "public-image".to_owned(),
            prompt: "a studio portrait".to_owned(),
            count: Some(2),
            aspect_ratio: Some("3:4".to_owned()),
            resolution: None,
            quality: Some("high".to_owned()),
            output_format: Some("webp".to_owned()),
            background: Some("opaque".to_owned()),
        };
        let ConsoleImageDispatchRequest::Standard(value) =
            console_image_request(request, &resolved_route(OPENAI_IMAGES_API_PROFILE))
                .expect("valid OpenAI console request")
        else {
            panic!("OpenAI request was dispatched to the wrong adapter");
        };
        assert_eq!(value["quality"], "high");
        assert_eq!(value["output_format"], "webp");
        assert_eq!(value["background"], "opaque");
        assert_eq!(value["n"], 2);
        assert_eq!(value["size"], "3:4");
    }

    #[test]
    fn xai_console_request_rejects_openai_only_controls() {
        let request = ConsoleImageGenerationRequest {
            model: "public-image".to_owned(),
            prompt: "a studio portrait".to_owned(),
            count: Some(1),
            aspect_ratio: Some("1:1".to_owned()),
            resolution: Some("1k".to_owned()),
            quality: Some("high".to_owned()),
            output_format: None,
            background: None,
        };
        assert!(
            console_image_request(request, &resolved_route(XAI_IMAGES_API_PROFILE)).is_err(),
            "xAI request silently accepted an OpenAI-only quality control"
        );
    }

    #[test]
    fn dreamina_console_request_uses_resolved_provider_model_and_cli_controls() {
        let request = ConsoleImageGenerationRequest {
            model: "public-dreamina".to_owned(),
            prompt: "a wide cinematic scene".to_owned(),
            count: Some(3),
            aspect_ratio: Some("21:9".to_owned()),
            resolution: Some("4k".to_owned()),
            quality: None,
            output_format: None,
            background: None,
        };
        let ConsoleImageDispatchRequest::Dreamina(projected) =
            console_image_request(request, &resolved_route(DREAMINA_IMAGES_API_PROFILE))
                .expect("valid Dreamina console request")
        else {
            panic!("Dreamina request was dispatched to the wrong adapter");
        };
        assert_eq!(projected.model_version.as_deref(), Some("5.0Pro"));
        assert_eq!(projected.ratio.as_deref(), Some("21:9"));
        assert_eq!(projected.resolution_type, "4k");
        assert_eq!(projected.generate_num, Some(3));
    }

    #[test]
    fn ark_console_request_preserves_official_profile_and_projects_geometry() {
        let request = ConsoleImageGenerationRequest {
            model: "doubao-seedream-5-0-260128".to_owned(),
            prompt: "a wide cinematic scene".to_owned(),
            count: Some(2),
            aspect_ratio: Some("16:9".to_owned()),
            resolution: Some("2k".to_owned()),
            quality: None,
            output_format: None,
            background: None,
        };
        let ConsoleImageDispatchRequest::Ark(projected) =
            console_image_request(request, &resolved_route(ARK_IMAGES_API_PROFILE))
                .expect("valid Ark console request")
        else {
            panic!("Ark request was dispatched to the wrong adapter");
        };
        assert_eq!(projected.model, "doubao-seedream-5-0-260128");
        assert_eq!(projected.size.as_deref(), Some("2048x1152"));
        assert_eq!(
            projected.sequential_image_generation.as_deref(),
            Some("auto")
        );
        assert_eq!(
            projected
                .sequential_image_generation_options
                .and_then(|options| options.max_images),
            Some(2)
        );
    }

    #[test]
    fn official_ark_alias_hides_the_duplicate_native_dreamina_alias() {
        let native = public_model("5.0Pro", DREAMINA_IMAGES_API_PROFILE, "5.0Pro");
        let ark = public_model(
            "doubao-seedream-5-0-260128",
            ARK_IMAGES_API_PROFILE,
            "5.0Pro",
        );
        let preferred = prefer_official_dreamina_aliases(vec![native, ark]);
        assert_eq!(preferred.len(), 1);
        assert_eq!(preferred[0].api_profile, ARK_IMAGES_API_PROFILE);
    }

    fn resolved_route(api_profile: &str) -> ResolvedModelRoute {
        let (provider_id, provider_model_id) = match api_profile {
            OPENAI_IMAGES_API_PROFILE => ("openai-codex", "gpt-image-2"),
            XAI_IMAGES_API_PROFILE => ("grok-cli", "grok-imagine-image"),
            DREAMINA_IMAGES_API_PROFILE | ARK_IMAGES_API_PROFILE => ("dreamina-cli", "5.0Pro"),
            _ => panic!("unsupported test profile"),
        };
        ResolvedModelRoute {
            public_model_id: "public-image".to_owned(),
            api_profile: api_profile.to_owned(),
            provider_id: provider_id.to_owned(),
            operation_id: IMAGE_GENERATION_ROUTE_OPERATION.to_owned(),
            command_schema: "test.images.v1".to_owned(),
            provider_model_id: provider_model_id.to_owned(),
            execution_model_id: provider_model_id.to_owned(),
            media_kind: "image".to_owned(),
            route_id: Uuid::new_v4(),
            route_revision: 1,
        }
    }

    fn public_model(
        public_model_id: &str,
        api_profile: &str,
        provider_model_id: &str,
    ) -> PublicModelRoute {
        PublicModelRoute {
            id: public_model_id.to_owned(),
            provider_model_id: Some(provider_model_id.to_owned()),
            api_profile: api_profile.to_owned(),
            provider_id: image_provider_dreamina_cli::PROVIDER_ID.to_owned(),
            operation_id: IMAGE_GENERATION_ROUTE_OPERATION.to_owned(),
            media_kind: "image".to_owned(),
            created_at_ms: 0,
        }
    }
}
