use serde::Deserialize;
use thiserror::Error;

use image_provider_sdk::{OpaqueProviderId, RemoteVideoResult};

pub const MAX_RECEIPT_BYTES: usize = 64 * 1024;
pub const MAX_FAIL_REASON_CHARS: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptedStatus {
    Querying,
    Success,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedReceipt {
    submit_id: OpaqueProviderId,
    status: AcceptedStatus,
}

impl AcceptedReceipt {
    pub fn submit_id(&self) -> &str {
        self.submit_id.as_str()
    }

    pub fn status(&self) -> AcceptedStatus {
        self.status
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DreaminaQueryStatusV1 {
    Querying,
    Success,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaQueueInfoV1 {
    pub queue_idx: u64,
    pub queue_length: u64,
    pub queue_status: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreaminaQueryReceiptV1 {
    submit_id: OpaqueProviderId,
    status: DreaminaQueryStatusV1,
    failure_code: Option<&'static str>,
    remote_video_result: Option<RemoteVideoResult>,
    image_result_count: Option<usize>,
    result_metadata_present: bool,
    queue_info: Option<DreaminaQueueInfoV1>,
    estimated_remaining_seconds: Option<u64>,
}

impl DreaminaQueryReceiptV1 {
    pub fn submit_id(&self) -> &str {
        self.submit_id.as_str()
    }

    pub fn status(&self) -> DreaminaQueryStatusV1 {
        self.status
    }

    /// Allowlisted terminal reason only; raw provider text is never retained.
    pub fn failure_code(&self) -> Option<&'static str> {
        self.failure_code
    }

    /// Only the CLI's single-video result view can establish remote delivery.
    /// Missing, ambiguous, or malformed video metadata remains unavailable.
    pub fn remote_video_result(&self) -> Option<&RemoteVideoResult> {
        self.remote_video_result.as_ref()
    }

    /// Count of validated image metadata with an explicitly empty video set.
    /// Image URLs and downloaded paths are neither retained nor exposed.
    /// A downloaded path never authorizes reading a file; the gateway must bind
    /// independently verified fresh files to this submission before delivery.
    pub fn image_result_count(&self) -> Option<usize> {
        self.image_result_count
    }

    /// Distinguishes an older receipt without result metadata from a present
    /// but malformed result. Malformed metadata must never authorize a shorter
    /// image set as a legacy single-file result.
    pub fn has_result_metadata(&self) -> bool {
        self.result_metadata_present
    }

    pub fn queue_info(&self) -> Option<&DreaminaQueueInfoV1> {
        self.queue_info.as_ref()
    }

    pub fn estimated_remaining_seconds(&self) -> Option<u64> {
        self.estimated_remaining_seconds
    }
}

#[derive(Debug, Error)]
pub enum ReceiptError {
    #[error("Dreamina receipt exceeds the 64 KiB limit: {actual} bytes")]
    InputTooLarge { actual: usize },
    #[error("Dreamina receipt is not one complete JSON object: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("Dreamina receipt is missing gen_status")]
    MissingStatus,
    #[error("Dreamina receipt has unknown gen_status: {0}")]
    UnknownStatus(String),
    #[error("Dreamina receipt has an empty submit_id")]
    EmptySubmitId,
    #[error("Dreamina receipt has an invalid durable submit_id")]
    InvalidSubmitId,
    #[error("Dreamina generation failed: {reason}")]
    GenerationFailed { reason: String },
}

#[derive(Deserialize)]
struct RawReceipt {
    submit_id: Option<String>,
    gen_status: Option<String>,
    fail_reason: Option<String>,
    #[serde(default, deserialize_with = "present_result_json")]
    result_json: Option<serde_json::Value>,
    queue_info: Option<serde_json::Value>,
    estimated_remaining_seconds: Option<serde_json::Value>,
}

fn present_result_json<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<serde_json::Value>, D::Error> {
    // Unlike Option's normal deserializer, preserve an explicit JSON null.
    serde_json::Value::deserialize(deserializer).map(Some)
}

pub fn parse_receipt(input: &[u8]) -> Result<AcceptedReceipt, ReceiptError> {
    let raw = parse_raw_receipt(input)?;

    match raw.gen_status.as_deref() {
        Some("querying") => accept(raw.submit_id, AcceptedStatus::Querying),
        Some("success") => accept(raw.submit_id, AcceptedStatus::Success),
        Some("fail") => Err(ReceiptError::GenerationFailed {
            reason: sanitize_fail_reason(raw.fail_reason.as_deref()),
        }),
        Some(status) => Err(ReceiptError::UnknownStatus(sanitize_text(status))),
        None => Err(ReceiptError::MissingStatus),
    }
}

pub fn parse_query_receipt(input: &[u8]) -> Result<DreaminaQueryReceiptV1, ReceiptError> {
    let raw = parse_raw_receipt(input)?;
    let status = match raw.gen_status.as_deref() {
        Some("querying") => DreaminaQueryStatusV1::Querying,
        Some("success") => DreaminaQueryStatusV1::Success,
        Some("fail") => DreaminaQueryStatusV1::Failed,
        Some(status) => return Err(ReceiptError::UnknownStatus(sanitize_text(status))),
        None => return Err(ReceiptError::MissingStatus),
    };
    let failure_code = (status == DreaminaQueryStatusV1::Failed)
        .then(|| known_failure_code(raw.fail_reason.as_deref()));
    let result_metadata_present = raw.result_json.is_some();
    let result = (status == DreaminaQueryStatusV1::Success)
        .then(|| parse_result_json(raw.result_json))
        .flatten();
    let remote_video_result = single_remote_video(result.as_ref());
    let image_result_count = image_result_count(result.as_ref());
    // Progress is advisory. Invalid optional metadata must not turn a confirmed
    // pending task into a failed query or change any terminal receipt semantics.
    let querying = status == DreaminaQueryStatusV1::Querying;
    let queue_info = querying
        .then(|| parse_queue_info(raw.queue_info.as_ref()))
        .flatten();
    let estimated_remaining_seconds = querying
        .then(|| {
            raw.estimated_remaining_seconds
                .as_ref()?
                .as_u64()
                .filter(|seconds| *seconds <= i64::MAX as u64)
        })
        .flatten();
    let submit_id = parse_submit_id(raw.submit_id)?;
    Ok(DreaminaQueryReceiptV1 {
        submit_id,
        status,
        failure_code,
        remote_video_result,
        image_result_count,
        result_metadata_present,
        queue_info,
        estimated_remaining_seconds,
    })
}

fn parse_queue_info(value: Option<&serde_json::Value>) -> Option<DreaminaQueueInfoV1> {
    // CLI 1.4.21 task.QueueInfo uses int32 counters and a string queue status.
    // Priority and debug_info are intentionally excluded from the public view.
    let value = value?.as_object()?;
    let queue_idx = value.get("queue_idx")?.as_u64()?;
    let queue_length = value.get("queue_length")?.as_u64()?;
    if queue_idx > queue_length || queue_length > i32::MAX as u64 {
        return None;
    }
    let queue_status = value.get("queue_status")?.as_str()?;
    if queue_status.is_empty()
        || queue_status.len() > 64
        || !queue_status
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return None;
    }
    Some(DreaminaQueueInfoV1 {
        queue_idx,
        queue_length,
        queue_status: queue_status.to_owned(),
    })
}

fn parse_result_json(
    result: Option<serde_json::Value>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    // The outer receipt is limited to 64 KiB. Decode at most one string layer.
    match result? {
        serde_json::Value::Object(result) => Some(result),
        serde_json::Value::String(result) => serde_json::from_str(&result).ok(),
        _ => None,
    }
}

fn image_result_count(
    result: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<usize> {
    let result = result?;
    let images = result.get("images")?.as_array()?;
    if !(1..=10).contains(&images.len()) || !result.get("videos")?.as_array()?.is_empty() {
        return None;
    }
    let mut downloaded = None;
    let mut item_ids = Vec::with_capacity(images.len());
    let mut sources = Vec::with_capacity(images.len());
    for image in images {
        let image = image.as_object()?;
        let item_id = image.get("item_id")?.as_str()?;
        let (is_downloaded, source) = match (image.get("image_url"), image.get("path")) {
            (Some(url), None) => {
                let url = url.as_str()?;
                if !url.starts_with("https://")
                    || url.len() <= "https://".len()
                    || url.len() > 8192
                    || url.chars().any(|ch| ch.is_control() || ch.is_whitespace())
                {
                    return None;
                }
                (false, url)
            }
            (None, Some(path)) => {
                let path = path.as_str()?;
                if !valid_downloaded_image_path(path) {
                    return None;
                }
                (true, path)
            }
            _ => return None,
        };
        if item_id.is_empty()
            || item_id.len() > 255
            || item_id
                .chars()
                .any(|ch| ch.is_control() || ch.is_whitespace())
            || item_ids.contains(&item_id)
            || sources.contains(&source)
            || downloaded
                .replace(is_downloaded)
                .is_some_and(|previous| previous != is_downloaded)
            || image.get("width")?.as_u64()? == 0
            || image.get("height")?.as_u64()? == 0
        {
            return None;
        }
        item_ids.push(item_id);
        sources.push(source);
    }
    Some(images.len())
}

fn valid_downloaded_image_path(path: &str) -> bool {
    // CLI download receipts describe local files, not fetchable URLs. This is
    // metadata validation only; never open a receipt-supplied path here.
    !path.is_empty()
        && path.len() <= 8192
        && !path.chars().any(char::is_control)
        && !path.contains(['\\', ':'])
        && path.split('/').enumerate().all(|(index, component)| {
            (!component.is_empty() || index == 0) && !matches!(component, "." | "..")
        })
        && path
            .rsplit_once('.')
            .is_some_and(|(_, extension)| matches!(extension, "png" | "jpg" | "jpeg" | "webp"))
}

fn single_remote_video(
    result: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Option<RemoteVideoResult> {
    // CLI 1.4.21 taskResultOutput.result_json contains queryResultMediaOutput;
    // its video entries expose video_url. No TTL is present in that output type.
    let result = result?;
    if let Some(images) = result.get("images")
        && !images.is_null()
        && !images.as_array()?.is_empty()
    {
        return None;
    }
    let videos = result.get("videos")?.as_array()?;
    let [video] = videos.as_slice() else {
        return None;
    };
    let url = video.as_object()?.get("video_url")?.as_str()?;
    RemoteVideoResult::new(url.to_owned(), None).ok()
}

fn parse_raw_receipt(input: &[u8]) -> Result<RawReceipt, ReceiptError> {
    if input.len() > MAX_RECEIPT_BYTES {
        return Err(ReceiptError::InputTooLarge {
            actual: input.len(),
        });
    }

    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let raw = RawReceipt::deserialize(&mut deserializer).map_err(ReceiptError::InvalidJson)?;
    deserializer.end().map_err(ReceiptError::InvalidJson)?;
    Ok(raw)
}

fn accept(
    submit_id: Option<String>,
    status: AcceptedStatus,
) -> Result<AcceptedReceipt, ReceiptError> {
    parse_submit_id(submit_id).map(|submit_id| AcceptedReceipt { submit_id, status })
}

fn parse_submit_id(submit_id: Option<String>) -> Result<OpaqueProviderId, ReceiptError> {
    let submit_id = submit_id.filter(|value| !value.trim().is_empty());
    match submit_id {
        Some(submit_id) => {
            OpaqueProviderId::new(submit_id).map_err(|_| ReceiptError::InvalidSubmitId)
        }
        None => Err(ReceiptError::EmptySubmitId),
    }
}

fn known_failure_code(reason: Option<&str>) -> &'static str {
    // Match complete provider messages, not keywords inside arbitrary diagnostics.
    // The copyright wording is observed in the provider UI; different CLI wording
    // deliberately remains generic until its exact contract is established.
    match reason.filter(|value| value.chars().count() <= MAX_FAIL_REASON_CHARS) {
        Some(value) if value.trim() == "生成的视频里包含受版权保护元素" => {
            "dreamina_copyright_rejected"
        }
        _ => "dreamina_generation_failed",
    }
}

fn sanitize_fail_reason(reason: Option<&str>) -> String {
    let reason = sanitize_text(reason.unwrap_or_default());
    if reason.is_empty() {
        "Dreamina returned no failure reason".to_owned()
    } else {
        reason
    }
}

fn sanitize_text(value: &str) -> String {
    let mut sanitized = String::new();
    let mut pending_space = false;
    let mut chars = 0;

    for character in value.chars() {
        if character.is_control() || character.is_whitespace() {
            pending_space = !sanitized.is_empty();
            continue;
        }
        if pending_space {
            if chars == MAX_FAIL_REASON_CHARS {
                break;
            }
            sanitized.push(' ');
            chars += 1;
            pending_space = false;
        }
        if chars == MAX_FAIL_REASON_CHARS {
            break;
        }
        sanitized.push(character);
        chars += 1;
    }

    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn result_metadata_presence_preserves_absence_but_marks_null_and_malformed_values() {
        let legacy =
            parse_query_receipt(br#"{"submit_id":"task-1","gen_status":"success"}"#).unwrap();
        assert!(!legacy.has_result_metadata());
        assert_eq!(legacy.image_result_count(), None);
        for result in [
            json!(null),
            json!(false),
            json!(7),
            json!("{broken"),
            json!({}),
            json!({"images":[{}, {}, {}, {}],"videos":[]}),
        ] {
            let receipt = parse_query_receipt(
                &serde_json::to_vec(
                    &json!({"submit_id":"task-1","gen_status":"success","result_json":result}),
                )
                .unwrap(),
            )
            .unwrap();
            assert!(receipt.has_result_metadata());
            assert_eq!(receipt.image_result_count(), None);
        }
    }

    fn query_with_progress(
        status: &str,
        queue_info: serde_json::Value,
        estimated_remaining_seconds: serde_json::Value,
    ) -> DreaminaQueryReceiptV1 {
        parse_query_receipt(
            &serde_json::to_vec(&json!({
                "submit_id": "task-queue-fixture",
                "gen_status": status,
                "queue_info": queue_info,
                "estimated_remaining_seconds": estimated_remaining_seconds,
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn queue_info_fixture() -> serde_json::Value {
        // Shape verified from a CLI 1.4.21 pending video query; ID and values are synthetic.
        json!({"queue_idx": 123, "priority": 7, "queue_status": "Queueing", "queue_length": 456})
    }

    #[test]
    fn query_progress_preserves_provider_values_without_exposing_debug_or_priority() {
        let mut queue = queue_info_fixture();
        queue["debug_info"] = json!("internal diagnostic");
        let receipt = query_with_progress("querying", queue, json!(27_131));
        assert_eq!(receipt.status(), DreaminaQueryStatusV1::Querying);
        assert_eq!(
            receipt.queue_info(),
            Some(&DreaminaQueueInfoV1 {
                queue_idx: 123,
                queue_length: 456,
                queue_status: "Queueing".to_owned(),
            })
        );
        assert_eq!(receipt.estimated_remaining_seconds(), Some(27_131));
        let debug = format!("{receipt:?}");
        assert!(!debug.contains("priority"));
        assert!(!debug.contains("internal diagnostic"));
    }

    #[test]
    fn query_progress_is_optional_and_never_attached_to_terminal_status() {
        let legacy =
            parse_query_receipt(br#"{"submit_id":"task-queue-fixture","gen_status":"querying"}"#)
                .unwrap();
        assert!(legacy.queue_info().is_none());
        assert!(legacy.estimated_remaining_seconds().is_none());
        for status in ["success", "fail"] {
            let receipt = query_with_progress(status, queue_info_fixture(), json!(12));
            assert!(receipt.queue_info().is_none());
            assert!(receipt.estimated_remaining_seconds().is_none());
        }
    }

    #[test]
    fn malformed_queue_metadata_does_not_change_confirmed_pending_status() {
        for queue in [
            json!(null),
            json!([]),
            json!("Queueing"),
            json!({}),
            json!({"queue_idx": 1, "queue_status": "Queueing"}),
            json!({"queue_idx": "1", "queue_length": 2, "queue_status": "Queueing"}),
            json!({"queue_idx": true, "queue_length": 2, "queue_status": "Queueing"}),
            json!({"queue_idx": -1, "queue_length": 2, "queue_status": "Queueing"}),
            json!({"queue_idx": 1.5, "queue_length": 2, "queue_status": "Queueing"}),
            json!({"queue_idx": 3, "queue_length": 2, "queue_status": "Queueing"}),
            json!({"queue_idx": 1, "queue_length": -2, "queue_status": "Queueing"}),
            json!({"queue_idx": 1, "queue_length": 2_147_483_648_u64, "queue_status": "Queueing"}),
            json!({"queue_idx": 1, "queue_length": 2, "queue_status": ""}),
            json!({"queue_idx": 1, "queue_length": 2, "queue_status": "  "}),
            json!({"queue_idx": 1, "queue_length": 2, "queue_status": "<script>"}),
            json!({"queue_idx": 1, "queue_length": 2, "queue_status": "Queueing\n"}),
            json!({"queue_idx": 1, "queue_length": 2, "queue_status": "x".repeat(65)}),
        ] {
            let receipt = query_with_progress("querying", queue, json!(12));
            assert_eq!(receipt.status(), DreaminaQueryStatusV1::Querying);
            assert!(receipt.queue_info().is_none());
            assert_eq!(receipt.estimated_remaining_seconds(), Some(12));
        }
    }

    #[test]
    fn malformed_estimate_does_not_discard_valid_queue_metadata() {
        for estimate in [
            json!(null),
            json!("12"),
            json!(true),
            json!(-1),
            json!(0.5),
            json!(u64::MAX),
            json!({"seconds": 12}),
        ] {
            let receipt = query_with_progress("querying", queue_info_fixture(), estimate);
            assert_eq!(receipt.status(), DreaminaQueryStatusV1::Querying);
            assert!(receipt.queue_info().is_some());
            assert!(receipt.estimated_remaining_seconds().is_none());
        }
    }

    #[test]
    fn query_progress_accepts_zero_and_exact_cli_integer_limits() {
        for (index, length) in [(0, 0), (0, 1), (i32::MAX, i32::MAX)] {
            let receipt = query_with_progress(
                "querying",
                json!({"queue_idx": index, "queue_length": length, "queue_status": "Queueing"}),
                json!(0),
            );
            assert_eq!(receipt.queue_info().unwrap().queue_idx, index as u64);
            assert_eq!(receipt.queue_info().unwrap().queue_length, length as u64);
            assert_eq!(receipt.estimated_remaining_seconds(), Some(0));
        }
    }

    #[test]
    fn query_video_view_preserves_the_exact_url_without_inventing_an_expiry() {
        // Fixture follows the CLI 1.4.21 static Go output types; it is not a live receipt.
        let url = "https://media.example.test/clip%2Fone.mp4?token=a%2Bb&expires=123";
        let result = json!({"images": [], "videos": [{"video_url": url, "duration": 5}]});
        for result in [result.clone(), json!(result.to_string())] {
            let bytes = serde_json::to_vec(&json!({
                "submit_id": "task-1", "gen_status": "success", "result_json": result,
            }))
            .unwrap();
            let receipt = parse_query_receipt(&bytes).unwrap();
            let result = receipt.remote_video_result().unwrap();
            assert_eq!(result.video_url(), url);
            assert_eq!(result.expires_at_ms(), None);
            assert!(receipt.image_result_count().is_none());
            assert!(!format!("{receipt:?}").contains("token="));
        }
    }

    fn image_result(count: usize) -> serde_json::Value {
        json!({
            "images": (0..count).map(|index| json!({
                "item_id": format!("item-{index}"),
                "image_url": format!("https://media.example.test/{index}.png?token=private"),
                "width": 2560, "height": 1440,
            })).collect::<Vec<_>>(),
            "videos": [],
        })
    }

    fn downloaded_image_result(count: usize) -> serde_json::Value {
        let mut result = image_result(count);
        for (index, image) in result["images"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
        {
            let image = image.as_object_mut().unwrap();
            image.remove("image_url");
            image.insert(
                "path".to_owned(),
                json!(format!(
                    "/tmp/dreamina-fixture/task-1_image_{}.png",
                    index + 1
                )),
            );
        }
        result
    }

    fn query_with_result(status: &str, result: serde_json::Value) -> DreaminaQueryReceiptV1 {
        parse_query_receipt(
            &serde_json::to_vec(&json!({
                "submit_id": "task-1", "gen_status": status, "result_json": result,
            }))
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn object_and_string_image_metadata_expose_only_the_bounded_count() {
        for count in [1, 4, 10] {
            for result in [image_result(count), downloaded_image_result(count)] {
                for result in [result.clone(), json!(result.to_string())] {
                    let receipt = query_with_result("success", result.clone());
                    assert_eq!(receipt.image_result_count(), Some(count));
                    assert!(receipt.remote_video_result().is_none());
                    let debug = format!("{receipt:?}");
                    assert!(!debug.contains("private"));
                    assert!(!debug.contains("dreamina-fixture"));
                    for status in ["querying", "fail"] {
                        assert!(
                            query_with_result(status, result.clone())
                                .image_result_count()
                                .is_none()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn downloaded_image_paths_remain_untrusted_bounded_metadata() {
        for path in [
            "task-1_image_1.png",
            "/tmp/task-1_image_1.jpg",
            "out/task-1_image_1.webp",
            "/tmp/valid directory/task-1_image_1.png",
        ] {
            let mut result = downloaded_image_result(1);
            result["images"][0]["path"] = json!(path);
            assert_eq!(
                query_with_result("success", result).image_result_count(),
                Some(1)
            );
        }
        for path in [
            "",
            "/",
            "../task-1_image_1.png",
            "out/../task-1_image_1.png",
            "./task-1_image_1.png",
            "/tmp//task-1_image_1.png",
            "task-1_image_1.png/",
            "/tmp/task-1_image_1.mp4",
            "/tmp/task-1_image_1.png\n",
            "https://media.example.test/task-1_image_1.png",
            "file:///tmp/task-1_image_1.png",
            "C:\\tmp\\task-1_image_1.png",
        ] {
            let mut result = downloaded_image_result(1);
            result["images"][0]["path"] = json!(path);
            assert!(
                query_with_result("success", result)
                    .image_result_count()
                    .is_none()
            );
        }
        let mut too_long = downloaded_image_result(1);
        too_long["images"][0]["path"] = json!(format!("/{}.png", "x".repeat(8192)));
        assert!(
            query_with_result("success", too_long)
                .image_result_count()
                .is_none()
        );
    }

    #[test]
    fn image_count_rejects_duplicate_and_mixed_source_metadata() {
        let mut invalid = Vec::new();
        let mut dual_source = downloaded_image_result(4);
        dual_source["images"][0]["image_url"] = json!("https://media.example.test/image.png");
        invalid.push(dual_source);
        let mut mixed = downloaded_image_result(4);
        mixed["images"][0] = image_result(1)["images"][0].clone();
        invalid.push(mixed);
        for result in [image_result(4), downloaded_image_result(4)] {
            let mut duplicate_item = result.clone();
            duplicate_item["images"][1]["item_id"] = result["images"][0]["item_id"].clone();
            invalid.push(duplicate_item);
            let source_field = if result["images"][0].get("path").is_some() {
                "path"
            } else {
                "image_url"
            };
            let mut duplicate_source = result.clone();
            duplicate_source["images"][1][source_field] = result["images"][0][source_field].clone();
            invalid.push(duplicate_source);
        }
        for (field, value) in [
            ("path", json!(null)),
            ("path", json!(7)),
            ("item_id", json!("bad\nitem")),
            ("item_id", json!("x".repeat(256))),
            ("width", json!(-1)),
            ("height", json!(1.5)),
        ] {
            let mut result = downloaded_image_result(4);
            result["images"][0][field] = value;
            invalid.push(result);
        }
        for result in invalid {
            for result in [result.clone(), json!(result.to_string())] {
                assert!(
                    query_with_result("success", result)
                        .image_result_count()
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn malformed_or_ambiguous_result_metadata_never_establishes_media_delivery() {
        let mut invalid_images = vec![image_result(0), image_result(11)];
        for (field, value) in [
            ("item_id", json!("")),
            ("item_id", json!(7)),
            ("image_url", json!("http://media.example.test/image.png")),
            ("width", json!(0)),
            ("height", json!("1440")),
        ] {
            let mut result = image_result(4);
            result["images"][0][field] = value;
            invalid_images.push(result);
        }
        let mut with_video = image_result(4);
        with_video["videos"] = json!([{"video_url":"https://media.example.test/a.mp4"}]);
        invalid_images.push(with_video);
        let mut missing_videos = image_result(4);
        missing_videos.as_object_mut().unwrap().remove("videos");
        invalid_images.push(missing_videos);
        for result in invalid_images {
            for result in [result.clone(), json!(result.to_string())] {
                let receipt = query_with_result("success", result);
                assert!(receipt.image_result_count().is_none());
                assert!(receipt.remote_video_result().is_none());
            }
        }
        for result in [
            json!("{broken"),
            json!("{} trailing"),
            json!("null"),
            json!(serde_json::to_string(&image_result(4).to_string()).unwrap()),
            json!("[]"),
            json!(null),
        ] {
            let receipt = query_with_result("success", result);
            assert!(receipt.image_result_count().is_none());
            assert!(receipt.remote_video_result().is_none());
        }
        let oversized = serde_json::to_vec(&json!({
            "submit_id":"task-1", "gen_status":"success", "result_json":"x".repeat(MAX_RECEIPT_BYTES),
        })).unwrap();
        assert!(matches!(
            parse_query_receipt(&oversized),
            Err(ReceiptError::InputTooLarge { .. })
        ));
    }

    #[test]
    fn query_video_view_requires_one_unambiguous_https_url() {
        for result_json in [
            json!(null),
            json!({}),
            json!({"videos": []}),
            json!({"videos": [{"video_url": "https://media.example.test/1.mp4"}, {"video_url": "https://media.example.test/2.mp4"}]}),
            json!({"images": [{"image_url": "https://media.example.test/1.png"}], "videos": [{"video_url": "https://media.example.test/1.mp4"}]}),
            json!({"videos": [{"download_url": "https://media.example.test/1.mp4"}]}),
            json!({"videos": [{"video_url": "http://media.example.test/1.mp4"}]}),
            json!({"videos": [{"video_url": "https://name:secret@media.example.test/1.mp4"}]}),
            json!({"videos": [{"video_url": "https://media.example.test/1.mp4#fragment"}]}),
            json!({"videos": [{"video_url": "https://media.example.test/line\nbreak"}]}),
            json!({"videos": [{"video_url": "https://media.example.test/".to_owned() + &"x".repeat(8192)}]}),
        ] {
            let receipt = parse_query_receipt(
                &serde_json::to_vec(&json!({
                    "submit_id": "task-1", "gen_status": "success", "result_json": result_json
                }))
                .unwrap(),
            )
            .unwrap();
            assert!(receipt.remote_video_result().is_none());
        }
        for status in ["querying", "fail"] {
            let receipt = parse_query_receipt(
                &serde_json::to_vec(&json!({
                    "submit_id": "task-1", "gen_status": status,
                    "result_json": {"videos": [{"video_url": "https://media.example.test/1.mp4"}]}
                }))
                .unwrap(),
            )
            .unwrap();
            assert!(receipt.remote_video_result().is_none());
        }
    }
}
