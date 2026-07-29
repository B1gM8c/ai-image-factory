use std::sync::Arc;

use axum::{
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
use serde::{Deserialize, Serialize};

use crate::{
    ImageGatewayError,
    auth::{ApiKeyCapability, AuthContext},
    batches::{
        BatchService, CreateProjectFile, MAX_BATCH_FILE_BYTES, ProjectFile, ProjectFilePurpose,
        ProjectScope,
    },
};

use super::{
    AppState, admin::authorize_project, authenticate_image_request, sessions::private_json,
};

const DEFAULT_LIST_LIMIT: usize = 20;
const MAX_LIST_LIMIT: usize = 10_000;

#[derive(Debug, Default, Deserialize)]
pub(super) struct ListFilesQuery {
    purpose: Option<String>,
    after: Option<String>,
    limit: Option<usize>,
    order: Option<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct FileObject {
    id: String,
    object: &'static str,
    bytes: u64,
    created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<i64>,
    filename: String,
    purpose: ProjectFilePurpose,
}

#[derive(Debug, Serialize)]
struct FileList {
    object: &'static str,
    data: Vec<FileObject>,
    first_id: Option<String>,
    last_id: Option<String>,
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct DeletedFileObject {
    id: String,
    object: &'static str,
    deleted: bool,
}

pub(super) async fn create_file(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    multipart: Multipart,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::FilesWrite)?;
    create_file_for_scope(&state, scope_from_auth(&auth), multipart).await
}

pub(super) async fn create_console_file(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    multipart: Multipart,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    create_file_for_scope(&state, scope, multipart).await
}

async fn create_file_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    mut multipart: Multipart,
) -> Result<Response, ImageGatewayError> {
    let mut purpose = None;
    let mut upload = None;
    while let Some(field) = multipart.next_field().await.map_err(|_| {
        ImageGatewayError::invalid_request(
            "The multipart file upload could not be read",
            Some("file".to_string()),
            "invalid_file_upload",
        )
    })? {
        match field.name() {
            Some("purpose") => {
                if purpose.is_some() {
                    return Err(duplicate_field("purpose"));
                }
                let value = field.text().await.map_err(|_| {
                    ImageGatewayError::invalid_request(
                        "purpose must be valid UTF-8 text",
                        Some("purpose".to_string()),
                        "invalid_file_purpose",
                    )
                })?;
                purpose = Some(value.parse::<ProjectFilePurpose>()?);
            }
            Some("file") => {
                if upload.is_some() {
                    return Err(duplicate_field("file"));
                }
                let filename = safe_filename(field.file_name().unwrap_or("batch.jsonl"))?;
                let bytes = field.bytes().await.map_err(|_| {
                    ImageGatewayError::invalid_request(
                        "The uploaded file could not be read",
                        Some("file".to_string()),
                        "invalid_file_upload",
                    )
                })?;
                upload = Some((filename, bytes));
            }
            Some(name) => return Err(ImageGatewayError::unknown_parameter(name)),
            None => {
                return Err(ImageGatewayError::invalid_request(
                    "Every multipart field must have a name",
                    None,
                    "invalid_file_upload",
                ));
            }
        }
    }

    let purpose = purpose.ok_or_else(|| {
        ImageGatewayError::invalid_request(
            "purpose is required",
            Some("purpose".to_string()),
            "missing_required_parameter",
        )
    })?;
    let (filename, bytes) = upload.ok_or_else(|| {
        ImageGatewayError::invalid_request(
            "file is required",
            Some("file".to_string()),
            "missing_required_parameter",
        )
    })?;
    if purpose != ProjectFilePurpose::Batch {
        return Err(ImageGatewayError::invalid_request(
            "Only batch input files are supported",
            Some("purpose".to_string()),
            "unsupported_file_purpose",
        ));
    }
    if bytes.is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "The uploaded file must not be empty",
            Some("file".to_string()),
            "invalid_file",
        ));
    }
    if bytes.len() as u64 > MAX_BATCH_FILE_BYTES {
        return Err(ImageGatewayError::payload_too_large(format!(
            "Batch input files must not exceed {MAX_BATCH_FILE_BYTES} bytes"
        )));
    }
    if !filename.to_ascii_lowercase().ends_with(".jsonl") {
        return Err(ImageGatewayError::invalid_request(
            "Batch input files must use the .jsonl extension",
            Some("file".to_string()),
            "invalid_file",
        ));
    }

    let file = batch_service(state)?
        .create_file(
            &scope,
            CreateProjectFile {
                filename: &filename,
                purpose,
                bytes: bytes.as_ref(),
                expires_after: None,
            },
        )
        .await?;
    Ok(private_json(file_object(file)))
}

pub(super) async fn list_files(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListFilesQuery>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::FilesRead)?;
    list_files_for_scope(&state, scope_from_auth(&auth), query).await
}

pub(super) async fn list_console_files(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(project_id): Path<String>,
    Query(query): Query<ListFilesQuery>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    list_files_for_scope(&state, scope, query).await
}

async fn list_files_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    query: ListFilesQuery,
) -> Result<Response, ImageGatewayError> {
    if query.order.as_deref().is_some_and(|order| order != "desc") {
        return Err(ImageGatewayError::unsupported(
            "order",
            "Only descending file order is currently supported",
        ));
    }
    let purpose = query
        .purpose
        .as_deref()
        .map(str::parse::<ProjectFilePurpose>)
        .transpose()?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let page = batch_service(state)?
        .list_files(&scope, purpose, query.after.as_deref(), limit)
        .await?;
    let data = page.data.into_iter().map(file_object).collect::<Vec<_>>();
    Ok(private_json(FileList {
        first_id: data.first().map(|file| file.id.clone()),
        last_id: data.last().map(|file| file.id.clone()),
        data,
        has_more: page.has_more,
        object: "list",
    }))
}

pub(super) async fn get_file(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::FilesRead)?;
    get_file_for_scope(&state, scope_from_auth(&auth), &file_id).await
}

pub(super) async fn get_console_file(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, file_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    get_file_for_scope(&state, scope, &file_id).await
}

async fn get_file_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    file_id: &str,
) -> Result<Response, ImageGatewayError> {
    let file = batch_service(state)?.get_file(&scope, file_id).await?;
    Ok(private_json(file_object(file)))
}

pub(super) async fn delete_file(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    auth.require_api_key_capability(ApiKeyCapability::FilesWrite)?;
    delete_file_for_scope(&state, scope_from_auth(&auth), &file_id).await
}

pub(super) async fn delete_console_file(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, file_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:write").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    delete_file_for_scope(&state, scope, &file_id).await
}

async fn delete_file_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    file_id: &str,
) -> Result<Response, ImageGatewayError> {
    let deleted = batch_service(state)?.delete_file(&scope, file_id).await?;
    Ok(private_json(DeletedFileObject {
        id: deleted.id,
        object: "file",
        deleted: true,
    }))
}

pub(super) async fn get_file_content(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
) -> Result<Response, ImageGatewayError> {
    let auth = authenticate_image_request(&headers, &state).await?;
    if !file_id.starts_with("file-") {
        return super::videos::get_video_content_with_auth(&state, &auth, &file_id).await;
    }
    auth.require_api_key_capability(ApiKeyCapability::FilesRead)?;
    get_file_content_for_scope(&state, scope_from_auth(&auth), &file_id).await
}

pub(super) async fn get_console_file_content(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((project_id, file_id)): Path<(String, String)>,
) -> Result<Response, ImageGatewayError> {
    authorize_project(&headers, &state, &project_id, "workspace:read").await?;
    let scope = console_project_scope(&state, &project_id).await?;
    get_file_content_for_scope(&state, scope, &file_id).await
}

async fn get_file_content_for_scope(
    state: &Arc<AppState>,
    scope: ProjectScope,
    file_id: &str,
) -> Result<Response, ImageGatewayError> {
    let service = batch_service(state)?;
    let file = service.get_file(&scope, file_id).await?;
    let bytes = service.read_file(&scope, file_id).await?;
    let media_type = if file.filename.to_ascii_lowercase().ends_with(".jsonl") {
        "application/jsonl; charset=utf-8"
    } else {
        "application/octet-stream"
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, media_type)
        .header(header::CONTENT_LENGTH, bytes.len().to_string())
        .header(
            header::CONTENT_DISPOSITION,
            format!(
                "attachment; filename=\"{}\"",
                disposition_filename(&file.filename)
            ),
        )
        .header(header::CACHE_CONTROL, "private, no-store")
        .body(axum::body::Body::from(bytes))
        .map_err(|_| ImageGatewayError::internal("failed to build file response"))
}

pub(super) fn file_object(file: ProjectFile) -> FileObject {
    FileObject {
        id: file.id,
        object: "file",
        bytes: file.bytes,
        created_at: file.created_at_ms.div_euclid(1_000),
        expires_at: file.expires_at_ms.map(|value| value.div_euclid(1_000)),
        filename: file.filename,
        purpose: file.purpose,
    }
}

pub(super) fn batch_service(
    state: &Arc<AppState>,
) -> Result<&Arc<dyn BatchService>, ImageGatewayError> {
    state
        .batch_service
        .as_ref()
        .ok_or_else(|| ImageGatewayError::service_unavailable("Files API is unavailable"))
}

pub(super) async fn console_project_scope(
    state: &Arc<AppState>,
    project_id: &str,
) -> Result<ProjectScope, ImageGatewayError> {
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
    Ok(ProjectScope::new(tenant_id, project_id))
}

fn scope_from_auth(auth: &AuthContext) -> ProjectScope {
    ProjectScope::new(auth.tenant_id.clone(), auth.project_id.clone())
}

fn duplicate_field(field: &str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        format!("The multipart field '{field}' may only be provided once"),
        Some(field.to_string()),
        "duplicate_parameter",
    )
}

fn safe_filename(value: &str) -> Result<String, ImageGatewayError> {
    let filename = value
        .rsplit(['/', '\\'])
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ImageGatewayError::invalid_request(
                "The uploaded filename is invalid",
                Some("file".to_string()),
                "invalid_file",
            )
        })?;
    if filename.len() > 512 || filename.chars().any(char::is_control) {
        return Err(ImageGatewayError::invalid_request(
            "The uploaded filename is invalid",
            Some("file".to_string()),
            "invalid_file",
        ));
    }
    Ok(filename.to_string())
}

fn disposition_filename(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '"' | '\\' | '\r' | '\n' => '_',
            value if value.is_control() => '_',
            value if value.is_ascii() => value,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::safe_filename;

    #[test]
    fn uploaded_filename_discards_untrusted_path_components() {
        assert_eq!(
            safe_filename(r"C:\Users\operator\batch.jsonl").unwrap(),
            "batch.jsonl"
        );
        assert_eq!(
            safe_filename("../../private/batch.jsonl").unwrap(),
            "batch.jsonl"
        );
        assert!(safe_filename("../").is_err());
    }
}
