use axum::{Json, response::Html};
use image_provider_contracts::openai_codex;
use serde::Serialize;
use serde_json::{Value, json};
use utoipa::{Modify, OpenApi, ToSchema};

use crate::{
    api_keys::{
        CreatedProjectApiKey, ProjectApiKey, ProjectApiKeyDeleted, ProjectApiKeyList,
        ProjectApiKeyOwner, ProjectApiKeyServiceAccountOwner, ProjectServiceAccount,
    },
    models::{
        HealthResponse, ImageData, ImageStreamEvent, ImagesResponse, ModelData, ModelsResponse,
    },
};

pub fn scalar_docs_html() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
  <head>
	      <title>AI Image Factory API Reference</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      body { margin: 0; }
    </style>
  </head>
  <body>
    <div id="app"></div>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
    <script>
      Scalar.createApiReference('#app', {
        url: '/openapi.json',
        theme: 'default',
        hideClientButton: false,
        telemetry: false
      })
    </script>
  </body>
</html>"#,
    )
}

pub fn openapi_json() -> Json<Value> {
    let mut value = serde_json::to_value(ApiDoc::openapi()).unwrap_or_else(|_| json!({}));
    patch_generated_schema(&mut value);
    Json(value)
}

#[derive(OpenApi)]
#[openapi(
    paths(
        create_image,
        edit_image,
        list_models,
        create_project_service_account,
        list_project_api_keys,
        delete_project_api_key,
        healthz,
    ),
    components(schemas(
        ImageGenerationRequestDoc,
        ImageEditRequestDoc,
        ImageReferenceDoc,
        ImageModelDoc,
        ImageQualityDoc,
        OutputFormatDoc,
        BackgroundDoc,
        ResponseFormatDoc,
        ModerationDoc,
        StyleDoc,
        CreateServiceAccountRequestDoc,
        ErrorResponseDoc,
        ErrorBodyDoc,
        ImagesResponse,
        ImageData,
        ImageStreamEvent,
        ModelsResponse,
        ModelData,
        HealthResponse,
        ProjectServiceAccount,
        CreatedProjectApiKey,
        ProjectApiKeyList,
        ProjectApiKey,
        ProjectApiKeyOwner,
        ProjectApiKeyServiceAccountOwner,
        ProjectApiKeyDeleted,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "Images"),
        (name = "Models"),
        (name = "Admin"),
        (name = "System"),
    ),
    servers((url = "/")),
    info(
        title = "AI Image Factory API",
        version = env!("CARGO_PKG_VERSION"),
        description = "OpenAI Images API-compatible platform gateway. The first active provider is gpt-image-2 through isolated native Codex CLI execution."
    )
)]
struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "BearerAuth",
                SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
            );
        }
    }
}

#[utoipa::path(
    post,
    path = "/v1/images/generations",
    tag = "Images",
    security(("BearerAuth" = [])),
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Optional 1-255 character visible ASCII key. Replays never execute the provider twice; durable response replay is not yet available.")
    ),
    request_body(content = ImageGenerationRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Base64 encoded generated images or final-only SSE when stream=true", content(
            (ImagesResponse = "application/json"),
            (ImageStreamEvent = "text/event-stream")
        )),
        (status = 400, description = "Invalid image request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 409, description = "Idempotency key is in progress, conflicts with another body, or its result cannot yet be replayed", body = ErrorResponseDoc),
        (status = 429, description = "Gateway queue or quota limit reached", body = ErrorResponseDoc),
        (status = 502, description = "Codex CLI image backend failed", body = ErrorResponseDoc),
        (status = 504, description = "Codex CLI image backend timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_image() {}

#[utoipa::path(
    post,
    path = "/v1/images/edits",
    tag = "Images",
    security(("BearerAuth" = [])),
    request_body(
        description = "Multipart image upload or JSON base64/data URL references. Remote URLs and file_id are gateway limitations when using native Codex CLI.",
        content(
            (ImageEditRequestDoc = "application/json"),
            (ImageEditRequestDoc = "multipart/form-data")
        )
    ),
    responses(
        (status = 200, description = "Base64 encoded edited images or final-only SSE when stream=true", content(
            (ImagesResponse = "application/json"),
            (ImageStreamEvent = "text/event-stream")
        )),
        (status = 400, description = "Invalid image edit request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc),
        (status = 413, description = "Upload payload is too large", body = ErrorResponseDoc),
        (status = 415, description = "Unsupported content type", body = ErrorResponseDoc),
        (status = 429, description = "Gateway queue or quota limit reached", body = ErrorResponseDoc),
        (status = 502, description = "Codex CLI image backend failed", body = ErrorResponseDoc),
        (status = 504, description = "Codex CLI image backend timed out", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn edit_image() {}

#[utoipa::path(
    get,
    path = "/v1/models",
    tag = "Models",
    security(("BearerAuth" = [])),
    responses(
        (status = 200, description = "Supported model list", body = ModelsResponse),
        (status = 401, description = "Invalid authentication", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_models() {}

#[utoipa::path(
    post,
    path = "/v1/organization/projects/{project_id}/service_accounts",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(("project_id" = String, Path, description = "Project id")),
    request_body(content = CreateServiceAccountRequestDoc, content_type = "application/json"),
    responses(
        (status = 200, description = "Created service account and one-time API key value", body = ProjectServiceAccount),
        (status = 400, description = "Invalid admin request", body = ErrorResponseDoc),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn create_project_service_account() {}

#[utoipa::path(
    get,
    path = "/v1/organization/projects/{project_id}/api_keys",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("after" = Option<String>, Query, description = "Cursor API key id"),
        ("limit" = Option<usize>, Query, minimum = 1, maximum = 100, description = "Page size")
    ),
    responses(
        (status = 200, description = "Project API keys", body = ProjectApiKeyList),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn list_project_api_keys() {}

#[utoipa::path(
    delete,
    path = "/v1/organization/projects/{project_id}/api_keys/{api_key_id}",
    tag = "Admin",
    security(("BearerAuth" = [])),
    params(
        ("project_id" = String, Path, description = "Project id"),
        ("api_key_id" = String, Path, description = "API key id")
    ),
    responses(
        (status = 200, description = "Deletion confirmation", body = ProjectApiKeyDeleted),
        (status = 401, description = "Invalid admin authentication", body = ErrorResponseDoc),
        (status = 404, description = "API key not found", body = ErrorResponseDoc),
        (status = 503, description = "API key store unavailable", body = ErrorResponseDoc)
    )
)]
#[allow(dead_code)]
async fn delete_project_api_key() {}

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "System",
    responses((status = 200, description = "Gateway is alive", body = HealthResponse))
)]
#[allow(dead_code)]
async fn healthz() {}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ImageGenerationRequest)]
#[allow(dead_code)]
struct ImageGenerationRequestDoc {
    #[schema(inline)]
    model: Option<ImageModelDoc>,
    #[schema(min_length = 1, max_length = 32000)]
    prompt: String,
    #[schema(minimum = 1, maximum = 10)]
    n: Option<u32>,
    #[schema(default = "auto", example = "1024x1024")]
    size: Option<String>,
    #[schema(inline)]
    quality: Option<ImageQualityDoc>,
    #[schema(inline)]
    output_format: Option<OutputFormatDoc>,
    #[schema(minimum = 0, maximum = 100)]
    output_compression: Option<u16>,
    #[schema(inline)]
    background: Option<BackgroundDoc>,
    #[schema(inline)]
    response_format: Option<ResponseFormatDoc>,
    user: Option<String>,
    #[schema(inline)]
    moderation: Option<ModerationDoc>,
    stream: Option<bool>,
    #[schema(minimum = 0, maximum = 3)]
    partial_images: Option<u32>,
    #[schema(inline)]
    style: Option<StyleDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ImageEditRequest)]
#[allow(dead_code)]
struct ImageEditRequestDoc {
    #[schema(inline)]
    model: Option<ImageModelDoc>,
    #[schema(min_length = 1, max_length = 32000)]
    prompt: String,
    #[schema(min_items = 1, max_items = 16)]
    image: Option<Vec<ImageReferenceDoc>>,
    #[schema(min_items = 1, max_items = 16)]
    images: Option<Vec<ImageReferenceDoc>>,
    mask: Option<ImageReferenceDoc>,
    #[schema(minimum = 1, maximum = 10)]
    n: Option<u32>,
    size: Option<String>,
    #[schema(inline)]
    quality: Option<ImageQualityDoc>,
    #[schema(inline)]
    output_format: Option<OutputFormatDoc>,
    #[schema(minimum = 0, maximum = 100)]
    output_compression: Option<u16>,
    #[schema(inline)]
    background: Option<BackgroundDoc>,
    #[schema(inline)]
    response_format: Option<ResponseFormatDoc>,
    user: Option<String>,
    #[schema(inline)]
    moderation: Option<ModerationDoc>,
    stream: Option<bool>,
    #[schema(minimum = 0, maximum = 3)]
    partial_images: Option<u32>,
    #[schema(inline)]
    style: Option<StyleDoc>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = ImageReference)]
#[allow(dead_code)]
struct ImageReferenceDoc {
    image_url: Option<String>,
    b64_json: Option<String>,
    mime_type: Option<String>,
    file_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(as = CreateServiceAccountRequest)]
#[allow(dead_code)]
struct CreateServiceAccountRequestDoc {
    #[schema(min_length = 1, max_length = 128)]
    name: String,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ErrorResponseDoc {
    error: ErrorBodyDoc,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
struct ErrorBodyDoc {
    message: String,
    #[serde(rename = "type")]
    error_type: String,
    #[schema(nullable)]
    param: Option<String>,
    #[schema(nullable)]
    code: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
enum ImageModelDoc {
    #[serde(rename = "gpt-image-2")]
    GptImage2,
    #[serde(rename = "gpt-image-2-2026-04-21")]
    GptImage2Snapshot,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum ImageQualityDoc {
    Auto,
    Low,
    Medium,
    High,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum OutputFormatDoc {
    Png,
    Jpeg,
    Webp,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum BackgroundDoc {
    Auto,
    Opaque,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum ResponseFormatDoc {
    B64Json,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum ModerationDoc {
    Auto,
}

#[derive(Debug, Serialize, ToSchema)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
enum StyleDoc {
    Vivid,
    Natural,
}

fn patch_generated_schema(value: &mut Value) {
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "model",
        openai_codex::MODELS,
    );
    patch_property_enum(value, "ImageGenerationRequest", "moderation", &["auto"]);
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "quality",
        &["auto", "low", "medium", "high"],
    );
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "output_format",
        &["png", "jpeg", "webp"],
    );
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "background",
        &["auto", "opaque"],
    );
    patch_property_enum(
        value,
        "ImageGenerationRequest",
        "response_format",
        &["b64_json"],
    );
    patch_property_enum(value, "ImageEditRequest", "model", openai_codex::MODELS);
    patch_property_enum(value, "ImageEditRequest", "moderation", &["auto"]);
    patch_property_enum(
        value,
        "ImageEditRequest",
        "quality",
        &["auto", "low", "medium", "high"],
    );
    patch_property_enum(
        value,
        "ImageEditRequest",
        "output_format",
        &["png", "jpeg", "webp"],
    );
    patch_property_enum(value, "ImageEditRequest", "background", &["auto", "opaque"]);
    patch_property_enum(value, "ImageEditRequest", "response_format", &["b64_json"]);
    if let Some(size_schema) = value
        .pointer_mut("/components/schemas/ImageGenerationRequest/properties/size")
        .and_then(Value::as_object_mut)
    {
        size_schema.insert(
            "description".to_string(),
            json!("auto, WIDTHxHEIGHT, or gateway aspect-ratio extension W:H such as 1:1, 4:3, or 16:9."),
        );
    }
    if let Some(edit_schema) = value
        .pointer_mut("/components/schemas/ImageEditRequest")
        .and_then(Value::as_object_mut)
    {
        edit_schema.insert(
            "anyOf".to_string(),
            json!([
                { "required": ["image"] },
                { "required": ["images"] }
            ]),
        );
    }
    if let Some(reference_schema) = value
        .pointer_mut("/components/schemas/ImageReference")
        .and_then(Value::as_object_mut)
    {
        reference_schema.insert(
            "oneOf".to_string(),
            json!([
                { "required": ["image_url"] },
                { "required": ["b64_json"] },
                { "required": ["file_id"] }
            ]),
        );
        if let Some(properties) = reference_schema
            .get_mut("properties")
            .and_then(Value::as_object_mut)
        {
            if let Some(image_url) = properties
                .get_mut("image_url")
                .and_then(Value::as_object_mut)
            {
                image_url.insert(
                    "description".to_string(),
                    json!("Base64 data URL is supported by the gateway. Official OpenAI also supports remote URLs; native Codex CLI gateway rejects remote URL fetching unless implemented with SSRF controls."),
                );
            }
            if let Some(b64_json) = properties
                .get_mut("b64_json")
                .and_then(Value::as_object_mut)
            {
                b64_json.insert(
                    "description".to_string(),
                    json!("Raw base64 image bytes supported by the gateway for JSON API clients. Use mime_type to declare image/png, image/jpeg, or image/webp; if omitted, the gateway infers the type from image magic bytes."),
                );
            }
            if let Some(mime_type) = properties
                .get_mut("mime_type")
                .and_then(Value::as_object_mut)
            {
                mime_type.insert(
                    "description".to_string(),
                    json!("MIME type for b64_json. Images support image/png, image/jpeg, or image/webp; masks must be image/png."),
                );
                mime_type.insert(
                    "enum".to_string(),
                    json!(["image/png", "image/jpeg", "image/webp"]),
                );
            }
            if let Some(file_id) = properties.get_mut("file_id").and_then(Value::as_object_mut) {
                file_id.insert(
                    "description".to_string(),
                    json!("Official OpenAI file_id reference. Native Codex CLI gateway rejects it because it cannot access OpenAI Files."),
                );
            }
        }
    }
}

fn patch_property_enum(value: &mut Value, schema: &str, property: &str, values: &[&str]) {
    let pointer = format!("/components/schemas/{schema}/properties/{property}");
    if let Some(property_schema) = value.pointer_mut(&pointer).and_then(Value::as_object_mut) {
        property_schema.insert("enum".to_string(), json!(values));
    }
}
