use std::sync::Arc;

use axum::{
    body::to_bytes,
    extract::{FromRequest, Multipart, Request},
    http::header,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

use crate::{ImageGatewayError, generator::InputImage, models::EditForm};

use super::AppState;

pub(super) async fn parse_edit_request(
    request: Request,
    state: &Arc<AppState>,
) -> Result<EditForm, ImageGatewayError> {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    if content_type.starts_with("multipart/form-data") {
        let multipart = Multipart::from_request(request, state).await.map_err(|_| {
            ImageGatewayError::invalid_request("Invalid multipart body", None, "invalid_multipart")
        })?;
        return parse_edit_form(multipart, state.config.max_upload_bytes).await;
    }

    if content_type.starts_with("application/json") {
        let bytes = to_bytes(request.into_body(), state.config.max_upload_bytes)
            .await
            .map_err(|_| ImageGatewayError::payload_too_large("JSON request body is too large"))?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
            ImageGatewayError::invalid_request(
                format!("Invalid JSON request: {error}"),
                None,
                "invalid_json",
            )
        })?;
        return parse_edit_json(value, state.config.max_upload_bytes);
    }

    Err(ImageGatewayError::unsupported_media_type(
        "Expected multipart/form-data or application/json",
    ))
}

async fn parse_edit_form(
    mut multipart: Multipart,
    max_upload_bytes: usize,
) -> Result<EditForm, ImageGatewayError> {
    let mut form = EditForm::default();
    let mut total_bytes = 0usize;

    while let Some(field) = multipart.next_field().await.map_err(|_| {
        ImageGatewayError::invalid_request("Invalid multipart body", None, "invalid_multipart")
    })? {
        let name = field.name().unwrap_or("").to_string();
        let filename = field.file_name().map(|value| value.to_string());
        let content_type = field.content_type().map(|value| value.to_string());

        match name.as_str() {
            "model" => form.model = Some(read_text_field(field).await?),
            "prompt" => form.prompt = Some(read_text_field(field).await?),
            "n" => {
                form.n = Some(read_text_field(field).await?.parse().map_err(|_| {
                    ImageGatewayError::invalid_request(
                        "n must be an integer",
                        Some("n".to_string()),
                        "invalid_type",
                    )
                })?)
            }
            "size" => form.size = Some(read_text_field(field).await?),
            "quality" => form.quality = Some(read_text_field(field).await?),
            "output_format" => form.output_format = Some(read_text_field(field).await?),
            "output_compression" => {
                form.output_compression =
                    Some(read_text_field(field).await?.parse().map_err(|_| {
                        ImageGatewayError::invalid_request(
                            "output_compression must be an integer",
                            Some("output_compression".to_string()),
                            "invalid_type",
                        )
                    })?)
            }
            "background" => form.background = Some(read_text_field(field).await?),
            "response_format" => form.response_format = Some(read_text_field(field).await?),
            "user" => form.user = Some(read_text_field(field).await?),
            "moderation" => form.moderation = Some(read_text_field(field).await?),
            "stream" => {
                form.stream = Some(parse_bool_field("stream", &read_text_field(field).await?)?)
            }
            "partial_images" => {
                form.partial_images = Some(read_text_field(field).await?.parse().map_err(|_| {
                    ImageGatewayError::invalid_request(
                        "partial_images must be an integer",
                        Some("partial_images".to_string()),
                        "invalid_type",
                    )
                })?)
            }
            "style" => form.style = Some(read_text_field(field).await?),
            "input_fidelity" => form.input_fidelity = Some(read_text_field(field).await?),
            "image" | "image[]" => {
                ensure_image_content_type(content_type.as_deref(), false)?;
                let bytes = read_bytes_field(field, &mut total_bytes, max_upload_bytes).await?;
                ensure_image_magic(content_type.as_deref(), &bytes, false)?;
                form.images.push(InputImage {
                    filename,
                    content_type,
                    bytes,
                });
            }
            "mask" => {
                if form.mask.is_some() {
                    return Err(ImageGatewayError::invalid_request(
                        "mask must be provided only once",
                        Some("mask".to_string()),
                        "invalid_value",
                    ));
                }
                ensure_image_content_type(content_type.as_deref(), true)?;
                let bytes = read_bytes_field(field, &mut total_bytes, max_upload_bytes).await?;
                ensure_image_magic(content_type.as_deref(), &bytes, true)?;
                form.mask = Some(InputImage {
                    filename,
                    content_type,
                    bytes,
                });
            }
            other => return Err(ImageGatewayError::unknown_parameter(other)),
        }
    }

    Ok(form)
}

fn parse_edit_json(value: Value, max_upload_bytes: usize) -> Result<EditForm, ImageGatewayError> {
    let object = value.as_object().ok_or_else(|| {
        ImageGatewayError::invalid_request(
            "Request body must be a JSON object",
            None,
            "invalid_json",
        )
    })?;
    let allowed = [
        "model",
        "prompt",
        "n",
        "size",
        "quality",
        "output_format",
        "output_compression",
        "background",
        "response_format",
        "user",
        "moderation",
        "stream",
        "partial_images",
        "style",
        "input_fidelity",
        "images",
        "image",
        "mask",
    ];
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ImageGatewayError::unknown_parameter(key));
        }
    }

    let mut form = EditForm {
        model: optional_string(object, "model")?,
        prompt: optional_string(object, "prompt")?,
        size: optional_string(object, "size")?,
        quality: optional_string(object, "quality")?,
        output_format: optional_string(object, "output_format")?,
        background: optional_string(object, "background")?,
        response_format: optional_string(object, "response_format")?,
        user: optional_string(object, "user")?,
        moderation: optional_string(object, "moderation")?,
        style: optional_string(object, "style")?,
        input_fidelity: optional_string(object, "input_fidelity")?,
        ..Default::default()
    };

    if let Some(value) = object.get("n") {
        form.n = Some(json_u32("n", value)?);
    }
    if let Some(value) = object.get("output_compression") {
        form.output_compression = Some(json_u16("output_compression", value)?);
    }
    if let Some(value) = object.get("stream") {
        form.stream = Some(json_bool("stream", value)?);
    }
    if let Some(value) = object.get("partial_images") {
        form.partial_images = Some(json_u32("partial_images", value)?);
    }

    let mut total_bytes = 0usize;
    if let Some(value) = object.get("images") {
        form.images.extend(parse_json_image_refs(
            "images",
            value,
            false,
            &mut total_bytes,
            max_upload_bytes,
        )?);
    }
    if let Some(value) = object.get("image") {
        form.images.extend(parse_json_image_refs(
            "image",
            value,
            false,
            &mut total_bytes,
            max_upload_bytes,
        )?);
    }
    if let Some(value) = object.get("mask") {
        let mut masks =
            parse_json_image_refs("mask", value, true, &mut total_bytes, max_upload_bytes)?;
        if masks.len() != 1 {
            return Err(ImageGatewayError::invalid_request(
                "mask must reference exactly one image",
                Some("mask".to_string()),
                "invalid_value",
            ));
        }
        form.mask = masks.pop();
    }

    Ok(form)
}

fn parse_json_image_refs(
    param: &str,
    value: &Value,
    mask: bool,
    total_bytes: &mut usize,
    max_upload_bytes: usize,
) -> Result<Vec<InputImage>, ImageGatewayError> {
    match value {
        Value::Array(items) => items
            .iter()
            .map(|item| parse_json_image_ref(param, item, mask, total_bytes, max_upload_bytes))
            .collect(),
        Value::Object(_) => Ok(vec![parse_json_image_ref(
            param,
            value,
            mask,
            total_bytes,
            max_upload_bytes,
        )?]),
        _ => Err(ImageGatewayError::invalid_request(
            format!("{param} must be an object or array of objects"),
            Some(param.to_string()),
            "invalid_type",
        )),
    }
}

fn parse_json_image_ref(
    param: &str,
    value: &Value,
    mask: bool,
    total_bytes: &mut usize,
    max_upload_bytes: usize,
) -> Result<InputImage, ImageGatewayError> {
    let object = value.as_object().ok_or_else(|| {
        ImageGatewayError::invalid_request(
            format!("{param} entries must be objects"),
            Some(param.to_string()),
            "invalid_type",
        )
    })?;
    let has_file_id = object.get("file_id").is_some();
    let image_url = object.get("image_url");
    let b64_json = object.get("b64_json");
    let reference_count = usize::from(has_file_id)
        + usize::from(image_url.is_some())
        + usize::from(b64_json.is_some());
    if reference_count != 1 {
        return Err(ImageGatewayError::invalid_request(
            format!("{param} entries must include exactly one of image_url, b64_json, or file_id"),
            Some(param.to_string()),
            "invalid_value",
        ));
    }
    if has_file_id {
        return Err(ImageGatewayError::unsupported(
            param,
            "file_id image references require OpenAI Files access and are not available in the native Codex CLI gateway",
        ));
    }
    if let Some(image_url) = image_url {
        let Some(image_url) = image_url.as_str() else {
            return Err(ImageGatewayError::invalid_request(
                "image_url must be a string",
                Some(param.to_string()),
                "invalid_type",
            ));
        };
        return decode_data_url_image(param, image_url, mask, total_bytes, max_upload_bytes);
    }

    let Some(encoded) = b64_json.and_then(Value::as_str) else {
        return Err(ImageGatewayError::invalid_request(
            "b64_json must be a string",
            Some(param.to_string()),
            "invalid_type",
        ));
    };
    let content_type =
        optional_string(object, "mime_type")?.or(optional_string(object, "content_type")?);
    decode_base64_image(
        param,
        encoded,
        content_type.as_deref(),
        mask,
        total_bytes,
        max_upload_bytes,
    )
}

pub(super) fn decode_data_url_image(
    param: &str,
    value: &str,
    mask: bool,
    total_bytes: &mut usize,
    max_upload_bytes: usize,
) -> Result<InputImage, ImageGatewayError> {
    if !value.starts_with("data:") {
        return Err(ImageGatewayError::unsupported(
            param,
            "remote image_url fetching is not enabled; use multipart upload or a base64 data URL",
        ));
    }
    let Some((metadata, encoded)) = value[5..].split_once(',') else {
        return Err(ImageGatewayError::invalid_request(
            "Invalid image data URL",
            Some(param.to_string()),
            "invalid_image_url",
        ));
    };
    let mut parts = metadata.split(';');
    let content_type = parts.next().unwrap_or("");
    if !parts.any(|part| part.eq_ignore_ascii_case("base64")) {
        return Err(ImageGatewayError::invalid_request(
            "Image data URL must be base64 encoded",
            Some(param.to_string()),
            "invalid_image_url",
        ));
    }
    ensure_image_content_type(Some(content_type), mask)?;
    let bytes = decode_base64_bytes(param, encoded, total_bytes, max_upload_bytes)?;
    ensure_image_magic(Some(content_type), &bytes, mask)?;
    Ok(InputImage {
        filename: None,
        content_type: Some(content_type.to_string()),
        bytes,
    })
}

fn decode_base64_image(
    param: &str,
    encoded: &str,
    content_type: Option<&str>,
    mask: bool,
    total_bytes: &mut usize,
    max_upload_bytes: usize,
) -> Result<InputImage, ImageGatewayError> {
    let bytes = decode_base64_bytes(param, encoded, total_bytes, max_upload_bytes)?;
    let content_type = match content_type {
        Some(content_type) => {
            ensure_image_content_type(Some(content_type), mask)?;
            ensure_image_magic(Some(content_type), &bytes, mask)?;
            content_type.to_string()
        }
        None => {
            let Some(inferred) = infer_image_content_type(&bytes) else {
                return Err(ImageGatewayError::invalid_request(
                    "Image bytes do not match supported image formats",
                    Some(if mask { "mask" } else { "image" }.to_string()),
                    "invalid_image_format",
                ));
            };
            ensure_image_content_type(Some(inferred), mask)?;
            inferred.to_string()
        }
    };

    Ok(InputImage {
        filename: None,
        content_type: Some(content_type),
        bytes,
    })
}

fn decode_base64_bytes(
    param: &str,
    encoded: &str,
    total_bytes: &mut usize,
    max_upload_bytes: usize,
) -> Result<Vec<u8>, ImageGatewayError> {
    if let Some(estimated_bytes) = estimated_base64_decoded_len(encoded)
        && total_bytes
            .checked_add(estimated_bytes)
            .is_none_or(|total| total > max_upload_bytes)
    {
        return Err(ImageGatewayError::payload_too_large(
            "JSON image payload is too large",
        ));
    }
    let bytes = STANDARD.decode(encoded).map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid base64 image data",
            Some(param.to_string()),
            "invalid_image_url",
        )
    })?;
    *total_bytes += bytes.len();
    if *total_bytes > max_upload_bytes {
        return Err(ImageGatewayError::payload_too_large(
            "JSON image payload is too large",
        ));
    }
    Ok(bytes)
}

fn estimated_base64_decoded_len(encoded: &str) -> Option<usize> {
    let padding = encoded
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| **byte == b'=')
        .count()
        .min(2);
    encoded
        .len()
        .checked_mul(3)?
        .checked_div(4)?
        .checked_sub(padding)
}

fn optional_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<String>, ImageGatewayError> {
    object
        .get(key)
        .map(|value| {
            value
                .as_str()
                .map(|value| value.to_string())
                .ok_or_else(|| {
                    ImageGatewayError::invalid_request(
                        format!("{key} must be a string"),
                        Some(key.to_string()),
                        "invalid_type",
                    )
                })
        })
        .transpose()
}

fn json_u32(key: &str, value: &Value) -> Result<u32, ImageGatewayError> {
    value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            ImageGatewayError::invalid_request(
                format!("{key} must be an integer"),
                Some(key.to_string()),
                "invalid_type",
            )
        })
}

fn json_u16(key: &str, value: &Value) -> Result<u16, ImageGatewayError> {
    value
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| {
            ImageGatewayError::invalid_request(
                format!("{key} must be an integer"),
                Some(key.to_string()),
                "invalid_type",
            )
        })
}

fn json_bool(key: &str, value: &Value) -> Result<bool, ImageGatewayError> {
    value.as_bool().ok_or_else(|| {
        ImageGatewayError::invalid_request(
            format!("{key} must be true or false"),
            Some(key.to_string()),
            "invalid_type",
        )
    })
}

fn parse_bool_field(name: &str, value: &str) -> Result<bool, ImageGatewayError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ImageGatewayError::invalid_request(
            format!("{name} must be true or false"),
            Some(name.to_string()),
            "invalid_type",
        )),
    }
}

fn ensure_image_magic(
    content_type: Option<&str>,
    bytes: &[u8],
    mask: bool,
) -> Result<(), ImageGatewayError> {
    let valid = match content_type {
        Some("image/png") => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        Some("image/jpeg") => {
            bytes.len() >= 3 && bytes[0] == 0xff && bytes[1] == 0xd8 && bytes[2] == 0xff
        }
        Some("image/webp") => {
            bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP"
        }
        _ => false,
    };

    if valid {
        Ok(())
    } else {
        Err(ImageGatewayError::invalid_request(
            "Image bytes do not match the declared content type",
            Some(if mask { "mask" } else { "image" }.to_string()),
            "invalid_image_format",
        ))
    }
}

fn infer_image_content_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.len() >= 3 && bytes[0] == 0xff && bytes[1] == 0xd8 && bytes[2] == 0xff {
        Some("image/jpeg")
    } else if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

async fn read_text_field(
    field: axum::extract::multipart::Field<'_>,
) -> Result<String, ImageGatewayError> {
    field.text().await.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid multipart text field",
            None,
            "invalid_multipart",
        )
    })
}

async fn read_bytes_field(
    field: axum::extract::multipart::Field<'_>,
    total_bytes: &mut usize,
    max_upload_bytes: usize,
) -> Result<Vec<u8>, ImageGatewayError> {
    let bytes = field.bytes().await.map_err(|_| {
        ImageGatewayError::invalid_request(
            "Invalid multipart file field",
            None,
            "invalid_multipart",
        )
    })?;
    *total_bytes += bytes.len();
    if *total_bytes > max_upload_bytes {
        return Err(ImageGatewayError::payload_too_large(
            "Multipart payload is too large",
        ));
    }
    Ok(bytes.to_vec())
}

fn ensure_image_content_type(
    content_type: Option<&str>,
    mask: bool,
) -> Result<(), ImageGatewayError> {
    let Some(content_type) = content_type else {
        return Err(ImageGatewayError::invalid_request(
            "Image file must include a supported content type",
            Some(if mask { "mask" } else { "image" }.to_string()),
            "invalid_image_format",
        ));
    };

    let allowed = if mask {
        content_type == "image/png"
    } else {
        matches!(content_type, "image/png" | "image/jpeg" | "image/webp")
    };

    if allowed {
        Ok(())
    } else {
        Err(ImageGatewayError::invalid_request(
            "Unsupported image file format",
            Some(if mask { "mask" } else { "image" }.to_string()),
            "invalid_image_format",
        ))
    }
}
