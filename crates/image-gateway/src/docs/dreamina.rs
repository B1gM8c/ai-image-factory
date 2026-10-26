use serde_json::{Value, json};

// Reuse the generated text schemas while keeping each native workflow's allowed fields closed.
pub(super) fn patch_schemas(document: &mut Value) {
    let schemas = &mut document["components"]["schemas"];
    let image_input = json!({
        "type": "string", "minLength": 1,
        "description": "Public HTTPS or base64 data URL containing PNG/JPEG/WebP; local paths and file IDs are unsupported. Maximum 32 MiB per image, subject to the configured total upload limit."
    });
    let video_input = json!({
        "type": "string", "minLength": 1,
        "description": "Public HTTPS or base64 data URL containing non-fragmented MP4 with one H264/AVC video track. HEVC, VP9 and fragmented MP4 are unsupported. Maximum 256 MiB, subject to the configured total upload limit."
    });
    let audio_input = json!({
        "type": "string", "minLength": 1,
        "description": "Public HTTPS or base64 data URL containing MP3 or PCM WAV (audio/mpeg, audio/wav or audio/x-wav). Maximum 32 MiB, subject to the configured total upload limit."
    });
    let mut text_image = workflow(
        schemas["DreaminaImageGenerationRequest"].clone(),
        "text2image",
    );
    text_image["properties"]["model_version"]["default"] = json!("5.0");
    text_image["properties"]["ratio"]["description"] = json!(
        "Defaults to 16:9 when width/height are omitted. Supply ratio or both width and height, never both forms."
    );
    text_image["properties"]["resolution_type"]["description"] = json!(
        "3.0/3.1 support 1k/2k; 4.x and 5.0 support 2k/4k; 5.0Pro supports 1.5k/2k/4k. Custom width/height must fit the selected resolution bucket."
    );
    let mut edit_image = text_image.clone();
    edit_image["properties"]["workflow"] = json!({"type": "string", "enum": ["image2image"]});
    edit_image["properties"]["model_version"] = json!({
        "type": ["string", "null"], "default": "5.0",
        "enum": [null, "4.0", "4.1", "4.5", "4.6", "4.7", "5.0", "5.0Pro"]
    });
    edit_image["properties"]["resolution_type"] = json!({
        "type": "string", "enum": ["1.5k", "2k", "4k"],
        "description": "1.5k requires 5.0Pro; all other listed models support 2k/4k."
    });
    edit_image["properties"]["images"] =
        json!({"type": "array", "minItems": 1, "maxItems": 10, "items": image_input});
    edit_image["required"] = json!(["workflow", "prompt", "resolution_type", "images"]);
    let upscale = workflow(
        json!({
            "type": "object", "required": ["workflow", "image", "resolution_type"],
            "properties": {
                "image": image_input,
                "resolution_type": {"type": "string", "enum": ["2k", "4k", "8k"]}
            },
            "description": "Image upscaling uses its fixed workflow model; prompt, model_version, ratio and generate_num are not accepted."
        }),
        "image_upscale",
    );

    let video_base = schemas["DreaminaVideoGenerationRequest"].clone();
    let mut text_video = workflow(video_base.clone(), "text2video");
    let video_models = json!({
        "type": ["string", "null"], "default": "seedance2.0fast",
        "enum": [null, "seedance2.0", "seedance2.0fast", "seedance2.0_vip", "seedance2.0fast_vip", "seedance2.0mini", "seedance2.5"]
    });
    text_video["properties"]["model_version"] = video_models;
    text_video["properties"]["ratio"]["description"] = json!("Defaults to 16:9.");
    text_video["properties"]["duration"]["description"] = json!(
        "Integer seconds, default 5. Seedance 2.5 supports 4-30; Seedance 2.0 variants support 4-15."
    );
    text_video["properties"]["video_resolution"]["description"] = json!(
        "Seedance 2.5 supports 480p/720p/1080p; seedance2.0_vip supports 720p/1080p/4k; other listed models support 720p only."
    );
    let mut first_video = workflow(video_base, "image2video");
    first_video["properties"]["model_version"]["default"] = json!("seedance2.0_vip");
    first_video["properties"]["image"] = image_input.clone();
    first_video["properties"]["ratio"]["description"] = json!(
        "Omit to preserve the source image geometry. Seedance 2.5 rejects an explicit ratio. Seedance 1.x accepts a ratio only when it matches the input image."
    );
    first_video["properties"]["duration"]["description"] = json!(
        "Integer seconds, default 5. seedance1.0fast: 5-10; seedance1.5pro: 5-12; Seedance 2.0: 4-15; Seedance 2.5: 4-30."
    );
    first_video["properties"]["video_resolution"] =
        text_video["properties"]["video_resolution"].clone();
    first_video["required"] = json!(["workflow", "prompt", "video_resolution", "image"]);
    let mut frames_video = first_video.clone();
    frames_video["properties"]
        .as_object_mut()
        .unwrap()
        .remove("image");
    frames_video["properties"]["workflow"] = json!({"type": "string", "enum": ["frames2video"]});
    frames_video["properties"]["first"] = image_input.clone();
    frames_video["properties"]["last"] = image_input.clone();
    frames_video["properties"]["model_version"] = json!({
        "type": ["string", "null"], "default": "seedance2.0_vip",
        "enum": [null, "seedance1.5pro", "seedance2.0", "seedance2.0fast", "seedance2.0_vip", "seedance2.0fast_vip", "seedance2.0mini", "seedance2.5"]
    });
    frames_video["required"] = json!(["workflow", "prompt", "video_resolution", "first", "last"]);

    let seconds =
        json!({"type": "number", "minimum": 1, "maximum": 8, "multipleOf": 0.001, "default": 3});
    let mut optional_seconds = seconds.clone();
    optional_seconds["type"] = json!(["number", "null"]);
    let mut multiframe = workflow(
        json!({
            "type": "object", "required": ["workflow", "images", "video_resolution"],
            "description": "Ordered 2-20 images with one transition per adjacent pair. Choose exactly one form: transitions objects; transition_prompt plus optional equal-length transition_duration; or exactly two images with prompt plus optional duration. Each segment is 1-8 seconds (default 3), at exact millisecond precision; total duration must be at least 2 seconds. model_version and ratio are not accepted.",
            "properties": {
                "images": {"type": "array", "minItems": 2, "maxItems": 20, "items": image_input},
                "video_resolution": {"type": "string", "enum": ["720p", "1080p"]},
                "transitions": {"type": ["array", "null"], "minItems": 1, "maxItems": 19, "items": {
                    "type": "object", "additionalProperties": false, "required": ["prompt"],
                    "properties": {"prompt": {"type": "string", "minLength": 1}, "duration": optional_seconds}
                }},
                "transition_prompt": {"type": ["array", "null"], "minItems": 1, "maxItems": 19, "items": {"type": "string", "minLength": 1}},
                "transition_duration": {"type": ["array", "null"], "minItems": 1, "maxItems": 19, "items": seconds},
                "prompt": {"type": ["string", "null"], "minLength": 1},
                "duration": optional_seconds
            }
        }),
        "multiframe2video",
    );
    multiframe["oneOf"] = json!([
        {"required": ["transitions"], "properties": {"transitions": {"type": "array"}, "transition_prompt": {"type": "null"}, "transition_duration": {"type": "null"}, "prompt": {"type": "null"}, "duration": {"type": "null"}}},
        {"required": ["transition_prompt"], "properties": {"transition_prompt": {"type": "array"}, "transitions": {"type": "null"}, "prompt": {"type": "null"}, "duration": {"type": "null"}}},
        {"required": ["prompt"], "properties": {"prompt": {"type": "string"}, "images": {"maxItems": 2}, "transitions": {"type": "null"}, "transition_prompt": {"type": "null"}, "transition_duration": {"type": "null"}}}
    ]);
    let mut multimodal = text_video.clone();
    multimodal["properties"]["workflow"] = json!({"type": "string", "enum": ["multimodal2video"]});
    multimodal["properties"]["model_version"]["default"] = json!("seedance2.0_vip");
    multimodal["properties"]["prompt"] = json!({"type": "string", "default": ""});
    multimodal["properties"]["image"] =
        json!({"type": "array", "maxItems": 30, "items": image_input});
    multimodal["properties"]["video"] =
        json!({"type": "array", "maxItems": 10, "items": video_input});
    multimodal["properties"]["audio"] =
        json!({"type": "array", "maxItems": 10, "items": audio_input});
    multimodal["required"] = json!(["workflow", "video_resolution"]);
    multimodal["anyOf"] = json!([
        {"required": ["image"], "properties": {"image": {"minItems": 1}}},
        {"required": ["video"], "properties": {"video": {"minItems": 1}}},
        {"required": ["audio"], "properties": {"audio": {"minItems": 1}}}
    ]);
    multimodal["description"] = json!(
        "At least one reference is required. Seedance 2.5 allows up to 30 images, 10 videos, 10 audios and 50 inputs total, including audio-only input. Seedance 2.0 allows 9 images, 3 videos, 3 audios and 12 inputs total, and requires an image or video. Each video/audio must be at least 2 seconds; each item and the sum within each timed-media category must be at most 30 seconds for 2.5, or 15 seconds for 2.0. Durations are measured from the input bytes."
    );

    for (name, variants) in [
        (
            "DreaminaImageGenerationRequest",
            vec![
                ("DreaminaTextToImageRequest", text_image),
                ("DreaminaImageToImageRequest", edit_image),
                ("DreaminaImageUpscaleRequest", upscale),
            ],
        ),
        (
            "DreaminaVideoGenerationRequest",
            vec![
                ("DreaminaTextToVideoRequest", text_video),
                ("DreaminaImageToVideoRequest", first_video),
                ("DreaminaFramesToVideoRequest", frames_video),
                ("DreaminaMultiframeToVideoRequest", multiframe),
                ("DreaminaMultimodalToVideoRequest", multimodal),
            ],
        ),
    ] {
        let refs: Vec<_> = variants
            .iter()
            .map(|(name, _)| json!({"$ref": format!("#/components/schemas/{name}")}))
            .collect();
        schemas[name] = json!({"oneOf": refs, "description": "Select a workflow. Omitting workflow retains the legacy text-generation request. Unknown fields are rejected."});
        for (name, schema) in variants {
            schemas[name] = schema;
        }
    }

    for (name, unsupported) in [
        (
            "ArkImageGenerationRequest",
            &[
                "seed",
                "guidance_scale",
                "watermark",
                "optimize_prompt",
                "optimize_prompt_options",
                "tools",
                "output_format",
            ][..],
        ),
        (
            "ArkContentGenerationTaskRequest",
            &[
                "safety_identifier",
                "callback_url",
                "return_last_frame",
                "service_tier",
                "execution_expires_after",
                "priority",
                "generate_audio",
                "draft",
                "camera_fixed",
                "watermark",
                "seed",
                "frames",
                "tools",
            ][..],
        ),
    ] {
        for field in unsupported {
            schemas[name]["properties"][field]["description"] = json!(
                "Represented for Ark wire compatibility. Any non-null value is rejected with unsupported_parameter by the current Dreamina binding."
            );
        }
    }
    let ark_image = &mut schemas["ArkImageGenerationRequest"]["properties"];
    ark_image["image"]["description"] = json!(
        "Omit for text-to-image generation. For image editing, supply one image URL or an array of 1-10 image URLs. HTTPS URLs and PNG, JPEG or WebP data URLs are supported."
    );
    ark_image["model"]["description"] = json!(
        "Supported aliases: doubao-seedream-5-0 and doubao-seedream-5-0-lite (including -260128) map to 5.0 Lite; doubao-seedream-5-0-pro (including -260628) maps to 5.0Pro. Each accepts an optional six-digit date suffix. Model-specific resolution limits still apply."
    );
    ark_image["response_format"]["description"] =
        json!("Omit or use b64_json. url is represented but unsupported.");
    ark_image["stream"]["description"] = json!("Omit or use false. true is unsupported.");
    let ark_video = &mut schemas["ArkContentGenerationTaskRequest"]["properties"];
    ark_video["model"]["description"] = json!(
        "Supported aliases: doubao-seedance-2-0, doubao-seedance-2-0-fast, doubao-seedance-2-0-mini and doubao-seedance-2-5, each with an optional six-digit date suffix. Other native models use the Dreamina endpoint."
    );
    ark_video["resolution"]["description"] = json!(
        "Defaults to 720p. Seedance 2.5 accepts 480p/720p/1080p; the supported Ark Seedance 2.0 aliases accept 720p."
    );
    ark_video["ratio"]["description"] = json!(
        "Defaults to 16:9 for text or multimodal input. For first/last frames, omit or use adaptive to preserve image geometry. Seedance 2.5 first/last-frame requests reject concrete ratios. adaptive is unsupported for text and multimodal input."
    );
    for name in ["DreaminaVideoTaskDoc", "ArkContentGenerationTaskDoc"] {
        schemas[name]["properties"]["status"] = json!({
            "type": "string",
            "enum": ["running", "succeeded", "failed", "uncertain"],
            "description": "uncertain means the provider outcome is unknown and reconciliation is required; it is not a confirmed failure or permission to retry or create a replacement task. The error code is generation_uncertain. A confirmed one-time safety rejection is failed with dreamina_compliance_confirmation_required; complete the required confirmation on Dreamina Web before any future request. The rejected task is never retried."
        });
    }
    schemas["DreaminaReleasedSettlement"] = json!({
        "type": "object",
        "description": "Read-only receipt from an audited operator compliance-rejection resolution joined to the fully released customer billing hold. Monetary micros are exact decimal strings. Absence of this receipt is not proof of release.",
        "required": ["state", "resolution_audit_id", "job_id", "request_id", "hold_id", "currency", "held_micros", "released_micros", "captured_micros"],
        "properties": {
            "state": {"type": "string", "enum": ["released"]},
            "resolution_audit_id": {"type": "string", "format": "uuid"},
            "job_id": {"type": "string", "format": "uuid"},
            "request_id": {"type": "string"},
            "hold_id": {"type": "string", "format": "uuid"},
            "currency": {"type": "string", "example": "USD"},
            "held_micros": {"type": "string", "pattern": "^[0-9]+$", "example": "1200000"},
            "released_micros": {"type": "string", "pattern": "^[0-9]+$", "example": "1200000"},
            "captured_micros": {"type": "string", "enum": ["0"]}
        }
    });
    schemas["DreaminaVideoTaskDoc"]["properties"]["settlement"] = json!({
        "$ref": "#/components/schemas/DreaminaReleasedSettlement",
        "description": "Optional; omitted unless this failed Dreamina task has an audited full zero-charge hold release. Uses the same tenant, project and actor visibility as the task. Never infer release from status alone."
    });
    schemas["DreaminaProviderProgress"] = json!({
        "type": "object", "additionalProperties": false,
        "required": ["provider_status", "observed_at_ms"],
        "description": "Latest durable upstream observation. Queue position is reported verbatim, not a calculated number of people ahead. Missing values are unknown. A GET never queries the CLI or refreshes observed_at_ms. An uncertain task may retain a historical observation; terminal success or failure omits progress.",
        "properties": {
            "provider_status": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,64}$", "example": "Queueing"},
            "queue_position": {"type": "integer", "minimum": 0, "maximum": 4294967295_u64, "example": 3341},
            "queue_total": {"type": "integer", "minimum": 0, "maximum": 4294967295_u64, "example": 476961},
            "estimated_remaining_seconds": {"type": "integer", "minimum": 0, "maximum": 2592000, "example": 27131, "description": "Provider estimate at observed_at_ms, not a guarantee or a precise countdown."},
            "observed_at_ms": {"type": "integer", "format": "int64", "minimum": 0}
        }
    });
    schemas["DreaminaCancellation"] = json!({
        "type": "object", "additionalProperties": false,
        "required": ["supported", "reason"],
        "properties": {
            "supported": {"type": "boolean", "enum": [false]},
            "reason": {"type": "string", "enum": ["provider_cancellation_unsupported"]}
        },
        "description": "Official CLI 1.4.21 has no upstream cancellation command. Stopping local polling does not cancel generation, prove a refund or authorize releasing a hold. No cancellation POST is advertised."
    });
    let task = &mut schemas["DreaminaVideoTaskDoc"]["properties"];
    task["stage"] = json!({
        "type": "string", "enum": ["queued", "dispatching", "processing", "provider_queued"],
        "description": "Optional while running. queued is Factory's local queue; provider_queued requires a Queueing upstream observation. processing is Factory tracking, not proof the provider started rendering. Unknown provider states do not establish a rendering phase."
    });
    task["provider_progress"] = json!({"$ref": "#/components/schemas/DreaminaProviderProgress"});
    task["cancellation"] = json!({"$ref": "#/components/schemas/DreaminaCancellation"});
    patch_manual_cancellation(schemas);
    patch_image_tasks(document);
}

fn patch_manual_cancellation(schemas: &mut Value) {
    schemas["DreaminaManualCancellation"] = json!({
        "type":"object", "additionalProperties":false,
        "description":"Trusted operator classification after same-account website cancellation, a Failed CLI query, and the original completed full zero-charge release. Task status remains failed. Ordinary failed status, a user request, or absence of this proof never authorizes a refund. release_event_id identifies the durable original release or operator cancellation audit event. A supplement cancellation waives the customer charge while the original provider effect remains unknown; no provider failure or absence of upstream cost is asserted. No refund ledger entry is created.",
        "required":["schema","status","authority_id","job_id","request_id","upstream_task_id","submission_id","hold_id","confirmed_at_ms","currency","held_micros","captured_micros","released_micros","release_event_id"],
        "properties":{
            "schema":{"type":"string","enum":["factory.manual_cancellation.v1"]},
            "status":{"type":"string","enum":["confirmed"]},
            "authority_id":{"type":"string","format":"uuid"},
            "job_id":{"type":"string","format":"uuid"}, "request_id":{"type":"string"},
            "upstream_task_id":{"type":"string"}, "submission_id":{"type":"string","format":"uuid"},
            "hold_id":{"type":"string","format":"uuid"}, "confirmed_at_ms":{"type":"integer","format":"int64","minimum":1},
            "currency":{"type":"string","enum":["USD"]},
            "held_micros":{"type":"string","pattern":"^[1-9][0-9]*$"},
            "captured_micros":{"type":"string","enum":["0"]},
            "released_micros":{"type":"string","pattern":"^[1-9][0-9]*$","description":"Exactly equals held_micros."},
            "release_event_id":{"type":"string","format":"uuid"}
        }
    });
    schemas["DreaminaVideoTaskDoc"]["properties"]["manual_cancellation"] =
        json!({"$ref":"#/components/schemas/DreaminaManualCancellation"});
    schemas["ArkContentGenerationTaskMetadata"] = json!({
        "type":"object", "additionalProperties":false,
        "description":"Read-only Factory tracking from the same scoped task query. Queue values and observed_at_ms are the durable upstream report, not refreshed or calculated by GET. Missing progress is unknown; an uncertain task may retain an earlier observation.",
        "properties":{
            "stage":schemas["DreaminaVideoTaskDoc"]["properties"]["stage"].clone(),
            "provider_progress":{"$ref":"#/components/schemas/DreaminaProviderProgress"},
            "manual_cancellation":{"$ref":"#/components/schemas/DreaminaManualCancellation"}
        }
    });
    schemas["ArkContentGenerationTaskDoc"]["properties"]["metadata"] =
        json!({"$ref":"#/components/schemas/ArkContentGenerationTaskMetadata"});
}

fn patch_image_tasks(document: &mut Value) {
    let schemas = &mut document["components"]["schemas"];
    schemas["DreaminaImageRequestMetadata"] = json!({
        "type": "object", "additionalProperties": false,
        "description": "Safe specifications frozen at admission. No prompt, input location or credential is returned.",
        "properties": {
            "generate_num": {"type": "integer", "minimum": 1, "maximum": 10},
            "model_version": {"type": "string"},
            "workflow": {"type": "string", "enum": ["text2image", "image2image", "image_upscale"]},
            "resolution_type": {"type": "string"}, "ratio": {"type": "string"},
            "width": {"type": "integer", "minimum": 1}, "height": {"type": "integer", "minimum": 1}
        }
    });
    schemas["DreaminaImageTask"] = json!({
        "type": "object", "additionalProperties": false, "required": ["id", "status", "cancellation"],
        "description": "Read-only image task. Success includes the existing frozen images response payload. uncertain requires reconciliation; it neither authorizes a replacement task nor proves a refund. Terminal states omit historical progress.",
        "properties": {
            "id": {"type": "string", "format": "uuid"},
            "status": {"type": "string", "enum": ["running", "succeeded", "failed", "uncertain", "expired"]},
            "stage": schemas["DreaminaVideoTaskDoc"]["properties"]["stage"].clone(),
            "provider_progress": {"$ref": "#/components/schemas/DreaminaProviderProgress"},
            "cancellation": {"$ref": "#/components/schemas/DreaminaCancellation"},
            "model": {"type": "string"}, "request_id": {"type": "string"},
            "request": {"$ref": "#/components/schemas/DreaminaImageRequestMetadata"},
            "error": {"type": "object", "additionalProperties": false, "required": ["code", "message"],
                "properties": {"code": {"type": "string"}, "message": {"type": "string"}}},
            "created": {"type": "integer", "format": "int64"},
            "data": {"type": "array", "items": {"type": "object", "additionalProperties": false,
                "required": ["b64_json"], "properties": {"b64_json": {"type": "string"}}}},
            "output_counts": {"type": "object", "additionalProperties": false,
                "required": ["requested", "delivered", "failed"],
                "description": "Present only on success. requested remains the original quantity; delivered equals data.length and requested equals delivered + failed. A partial native batch is returned only after all outputs and the original quote are settled; failed units are released. Missing blobs or uncertain outputs do not authorize partial success.",
                "properties": {"requested": {"type": "integer", "minimum": 1, "maximum": 10},
                    "delivered": {"type": "integer", "minimum": 1, "maximum": 10},
                    "failed": {"type": "integer", "minimum": 0, "maximum": 9}}},
            "output_format": {"type": "string"}, "quality": {"type": "string"},
            "size": {"type": "string"}, "background": {"type": "string"}
        }
    });
    let task_response = json!({"description": "Current task snapshot. GET never invokes CLI, changes state or refreshes the observation time.",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/DreaminaImageTask"}}}});
    let errors = json!({"description": "Invalid parameter or task not found",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ErrorResponseDoc"}}}});
    let paths = &mut document["paths"];
    let post = &mut paths["/v1/dreamina/images/generations"]["post"];
    post["parameters"].as_array_mut().expect("native image parameters").push(json!({
        "name": "Prefer", "in": "header", "required": false,
        "schema": {"type": "string", "example": "respond-async"},
        "description": "respond-async returns 202 and Location after durable admission. Same Idempotency-Key returns the original task's current snapshot without resubmitting or waiting. Omitting this preference retains the synchronous 200 response. The frozen request, processing mode and price do not change."
    }));
    let mut accepted = task_response.clone();
    accepted["headers"] = json!({"Location": {"description": "Original task status URL", "schema": {"type": "string"}}});
    post["responses"]["202"] = accepted;
    let security = json!([{"BearerAuth": []}]);
    paths["/v1/dreamina/images/{task_id}"]["get"] = json!({
        "tags": ["Dreamina CLI"], "operationId": "get_dreamina_image", "security": security,
        "summary": "Read an original Dreamina image task",
        "description": "Requires images:read and the original tenant/project/actor scope. Existing native tasks remain readable, including uncertain tasks.",
        "parameters": [{"name": "task_id", "in": "path", "required": true, "schema": {"type": "string", "format": "uuid"}}],
        "responses": {"200": task_response, "400": errors, "404": {"description": "Task not found in the authenticated scope"}, "401": {"description": "Invalid authentication"}, "403": {"description": "Missing image read capability"}}
    });
    paths["/v1/dreamina/images/by-idempotency-key"]["get"] = json!({
        "tags": ["Dreamina CLI"], "operationId": "get_dreamina_image_by_idempotency_key", "security": security,
        "summary": "Find the original image task by its idempotency key",
        "description": "Read-only lookup in the authenticated tenant/project/actor scope. Send the original key only in Idempotency-Key. Never resubmit to recover an ID. Receiving identities return idempotency_in_progress; conflicting identities return idempotency_conflict.",
        "parameters": [
            {"name": "Idempotency-Key", "in": "header", "required": true, "schema": {"type": "string"}},
            {"name": "operation", "in": "query", "required": false,
                "schema": {"type": "string", "enum": ["generation", "edit"], "default": "generation"}}
        ],
        "responses": {"200": task_response, "400": errors, "404": {"description": "Task not found in the authenticated scope"}, "401": {"description": "Invalid authentication"}, "403": {"description": "Missing image read capability"}, "409": {"description": "Idempotency identity is receiving or conflicts"}}
    });
}

fn workflow(mut schema: Value, name: &str) -> Value {
    schema["additionalProperties"] = json!(false);
    schema["properties"]["workflow"] = json!({"type": "string", "enum": [name]});
    for field in ["session", "poll"] {
        schema["properties"][field] = json!({
            "type": "integer", "enum": [0], "default": 0,
            "description": "Historical compatibility only: omit or send integer 0. Factory manages CLI sessions and polling; all other values are rejected."
        });
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_image_tasks_document_async_and_read_only_original_key_lookup() {
        let axum::Json(document) = crate::docs::openapi_json();
        let paths = &document["paths"];
        let post = &paths["/v1/dreamina/images/generations"]["post"];
        assert!(post["responses"].get("200").is_some());
        assert_eq!(
            post["responses"]["202"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/DreaminaImageTask"
        );
        assert!(
            post["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value["name"] == "Prefer")
        );
        let lookup = &paths["/v1/dreamina/images/by-idempotency-key"]["get"];
        assert_eq!(lookup["parameters"][0]["name"], "Idempotency-Key");
        assert_eq!(lookup["parameters"][0]["in"], "header");
        assert_eq!(
            lookup["parameters"][1]["schema"]["enum"],
            json!(["generation", "edit"])
        );
        assert_eq!(lookup["parameters"][1]["schema"]["default"], "generation");
        assert!(paths["/v1/dreamina/images/{task_id}"]["get"].is_object());
        let schemas = &document["components"]["schemas"];
        assert_eq!(schemas["DreaminaImageTask"]["additionalProperties"], false);
        assert_eq!(
            schemas["DreaminaImageRequestMetadata"]["additionalProperties"],
            false
        );
        assert!(
            schemas["DreaminaImageRequestMetadata"]["properties"]
                .get("prompt")
                .is_none()
        );
        assert!(
            schemas["DreaminaImageRequestMetadata"]["properties"]
                .get("images")
                .is_none()
        );
        assert!(
            document["paths"]["/v1/images/generations"]["post"]["responses"]
                .get("202")
                .is_none()
        );
    }

    #[test]
    fn manual_cancellation_is_a_read_only_proof_without_a_new_task_status() {
        let axum::Json(document) = crate::docs::openapi_json();
        let schemas = &document["components"]["schemas"];
        assert_eq!(
            schemas["DreaminaManualCancellation"]["properties"]["status"]["enum"],
            json!(["confirmed"])
        );
        assert_eq!(
            schemas["DreaminaManualCancellation"]["properties"]["captured_micros"]["enum"],
            json!(["0"])
        );
        assert_eq!(
            schemas["ArkContentGenerationTaskDoc"]["properties"]["metadata"]["$ref"],
            "#/components/schemas/ArkContentGenerationTaskMetadata"
        );
        assert_eq!(
            schemas["ArkContentGenerationTaskMetadata"]["properties"]["provider_progress"]["$ref"],
            "#/components/schemas/DreaminaProviderProgress"
        );
        for name in ["DreaminaVideoTaskDoc", "ArkContentGenerationTaskDoc"] {
            assert_eq!(
                schemas[name]["properties"]["status"]["enum"],
                json!(["running", "succeeded", "failed", "uncertain"])
            );
        }
        assert_eq!(
            schemas["DreaminaCancellation"]["properties"]["supported"]["enum"],
            json!([false])
        );
    }

    #[test]
    fn video_progress_documents_real_observation_and_unsupported_cancellation() {
        let axum::Json(document) = crate::docs::openapi_json();
        let schemas = &document["components"]["schemas"];
        let progress = &schemas["DreaminaProviderProgress"];
        assert_eq!(
            progress["required"],
            json!(["provider_status", "observed_at_ms"])
        );
        assert!(
            progress["description"]
                .as_str()
                .unwrap()
                .contains("GET never queries")
        );
        let task = &schemas["DreaminaVideoTaskDoc"];
        for name in ["stage", "provider_progress", "cancellation"] {
            assert!(!task["required"].as_array().unwrap().contains(&json!(name)));
        }
        assert_eq!(
            schemas["DreaminaCancellation"]["properties"]["supported"]["enum"],
            json!([false])
        );
        assert!(
            schemas["ArkContentGenerationTaskDoc"]["properties"]
                .get("provider_progress")
                .is_none()
        );
    }

    #[test]
    fn native_openapi_exposes_eight_closed_workflow_shapes() {
        let axum::Json(document) = crate::docs::openapi_json();
        let schemas = &document["components"]["schemas"];
        for (request, variants) in [
            (
                "DreaminaImageGenerationRequest",
                &[
                    "DreaminaTextToImageRequest",
                    "DreaminaImageToImageRequest",
                    "DreaminaImageUpscaleRequest",
                ][..],
            ),
            (
                "DreaminaVideoGenerationRequest",
                &[
                    "DreaminaTextToVideoRequest",
                    "DreaminaImageToVideoRequest",
                    "DreaminaFramesToVideoRequest",
                    "DreaminaMultiframeToVideoRequest",
                    "DreaminaMultimodalToVideoRequest",
                ][..],
            ),
        ] {
            let refs = schemas[request]["oneOf"].as_array().unwrap();
            assert_eq!(refs.len(), variants.len());
            for (reference, variant) in refs.iter().zip(variants) {
                assert_eq!(reference["$ref"], format!("#/components/schemas/{variant}"));
                let schema = &schemas[variant];
                assert_eq!(schema["additionalProperties"], false);
                let legacy = variant.contains("TextTo");
                assert_eq!(
                    schema["required"]
                        .as_array()
                        .unwrap()
                        .contains(&json!("workflow")),
                    !legacy
                );
                for field in ["session", "poll"] {
                    assert_eq!(schema["properties"][field]["enum"], json!([0]));
                    assert_eq!(schema["properties"][field]["type"], "integer");
                }
            }
        }
        let upscale = &schemas["DreaminaImageUpscaleRequest"]["properties"];
        assert_eq!(
            upscale["resolution_type"]["enum"],
            json!(["2k", "4k", "8k"])
        );
        assert!(upscale.get("prompt").is_none());
        assert!(upscale.get("model_version").is_none());
        let multiframe = &schemas["DreaminaMultiframeToVideoRequest"];
        assert_eq!(multiframe["oneOf"].as_array().unwrap().len(), 3);
        assert_eq!(multiframe["properties"]["images"]["maxItems"], 20);
        assert_eq!(
            multiframe["properties"]["duration"]["multipleOf"],
            json!(0.001)
        );
        assert!(multiframe["properties"].get("ratio").is_none());
        assert!(multiframe["properties"].get("model_version").is_none());
        assert!(
            !schemas["DreaminaTextToVideoRequest"]["properties"]["model_version"]["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("seedance1.0fast"))
        );
        assert!(
            schemas["DreaminaImageToVideoRequest"]["properties"]["model_version"]
                .to_string()
                .contains("seedance1.0fast")
        );
    }

    #[test]
    fn ark_openapi_explains_media_dispatch_and_unbound_controls() {
        let axum::Json(document) = crate::docs::openapi_json();
        let description = document["paths"]["/api/v3/contents/generations/tasks"]["post"]["requestBody"]["description"].as_str().unwrap();
        for supported in [
            "image2video",
            "frames2video",
            "multimodal2video",
            "H264/AVC",
            "PCM WAV",
            "4-30",
        ] {
            assert!(description.contains(supported), "missing {supported}");
        }
        assert!(!description.contains("accepts exactly one text content item"));
        let schemas = &document["components"]["schemas"];
        for field in ["callback_url", "generate_audio", "frames", "tools"] {
            assert!(
                schemas["ArkContentGenerationTaskRequest"]["properties"][field]["description"]
                    .as_str()
                    .unwrap()
                    .contains("unsupported_parameter")
            );
        }
        assert!(
            schemas["ArkContentGenerationTaskRequest"]["properties"]["ratio"]["description"]
                .as_str()
                .unwrap()
                .contains("adaptive")
        );
        assert!(
            schemas["ArkImageGenerationRequest"]["properties"]["image"]["description"]
                .as_str()
                .unwrap()
                .contains("1-10")
        );
        let content_items = schemas["ArkContentItemDoc"]["oneOf"].as_array().unwrap();
        let first_frame = content_items
            .iter()
            .find(|item| item["properties"]["type"]["enum"] == json!(["image_url"]))
            .unwrap();
        assert!(
            !first_frame["required"]
                .as_array()
                .unwrap()
                .contains(&json!("role"))
        );
        let models = document["paths"]["/v1/models"]["get"]["responses"]["200"]["description"]
            .as_str()
            .unwrap();
        assert!(models.contains("routing policy"));
        assert!(models.contains("not a per-workflow capability matrix"));
    }

    #[test]
    fn video_response_openapi_preserves_fractional_seconds() {
        let axum::Json(document) = crate::docs::openapi_json();
        let schemas = &document["components"]["schemas"];
        for name in ["DreaminaVideoContentDoc", "ArkContentGenerationTaskDoc"] {
            let duration = &schemas[name]["properties"]["duration"];
            assert!(duration["type"].to_string().contains("number"));
            assert!(
                duration["description"]
                    .as_str()
                    .unwrap()
                    .contains("JSON integers (5)")
            );
            assert!(duration["description"].as_str().unwrap().contains("2.501"));
        }
    }

    #[test]
    fn video_response_openapi_documents_uncertain_and_non_retryable_compliance_failure() {
        let axum::Json(document) = crate::docs::openapi_json();
        let schemas = &document["components"]["schemas"];
        for name in ["DreaminaVideoTaskDoc", "ArkContentGenerationTaskDoc"] {
            let status = &schemas[name]["properties"]["status"];
            assert_eq!(
                status["enum"],
                json!(["running", "succeeded", "failed", "uncertain"])
            );
            let description = status["description"].as_str().unwrap();
            assert!(description.contains("outcome is unknown"));
            assert!(description.contains("not a confirmed failure"));
            assert!(description.contains("generation_uncertain"));
            assert!(description.contains("dreamina_compliance_confirmation_required"));
            assert!(description.contains("never retried"));
        }
    }

    #[test]
    fn dreamina_release_receipt_openapi_is_optional_and_preserves_money_precision() {
        let axum::Json(document) = crate::docs::openapi_json();
        let schemas = &document["components"]["schemas"];
        let task = &schemas["DreaminaVideoTaskDoc"];
        assert!(
            !task["required"]
                .as_array()
                .unwrap()
                .contains(&json!("settlement"))
        );
        assert_eq!(
            task["properties"]["settlement"]["$ref"],
            "#/components/schemas/DreaminaReleasedSettlement"
        );
        let receipt = &schemas["DreaminaReleasedSettlement"];
        assert_eq!(receipt["properties"]["state"]["enum"], json!(["released"]));
        for field in ["held_micros", "released_micros", "captured_micros"] {
            assert_eq!(receipt["properties"][field]["type"], "string");
        }
        assert!(
            schemas["ArkContentGenerationTaskDoc"]["properties"]
                .get("settlement")
                .is_none()
        );
    }
}
