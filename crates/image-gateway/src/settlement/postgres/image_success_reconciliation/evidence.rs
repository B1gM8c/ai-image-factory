use std::{fs::File, io::Read, os::unix::fs::MetadataExt, path::Path};

use rustix::fs::{Mode, OFlags, open, openat};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::{DreaminaImageSuccessResolutionRequest, conflict};
use crate::{ImageGatewayError, artifacts::sha256_hex};

const MAX_RECEIPT_BYTES: u64 = 1024 * 1024;
const MAX_IMAGE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionAudit {
    schema_version: u16,
    job_id: Uuid,
    submission_id: Uuid,
    executor_execution_id: Uuid,
    remote_operation_id: String,
    argv: Vec<String>,
    stdout_sha256: String,
    exit_code: i32,
    executed_at_ms: i64,
    downloaded_images: Vec<DownloadedImage>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DownloadedImage {
    source_index: usize,
    filename: String,
    sha256: String,
    byte_size: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    schema_version: u16,
    stdout_sha256: String,
    execution_audit_sha256: String,
    selected_sha256: String,
    selected_source_index: u32,
    selected_item_id: String,
    quote_hash: String,
    provider_command_sha256: String,
}

pub(super) struct VerifiedEvidence {
    pub stdout_sha256: String,
    pub execution_audit_sha256: String,
    pub review_sha256: String,
    pub selected_sha256: String,
    pub selected_item_id: String,
    pub remote_operation_id: String,
    pub quote_hash: String,
    pub provider_command_sha256: String,
    pub width: u32,
    pub height: u32,
    pub bytes: Vec<u8>,
}

/// Operator-reviewed files are not a cryptographic provider attestation. Pin
/// every byte and require a private directory owned by the invoking actor.
pub(super) fn load(
    root: &Path,
    request: &DreaminaImageSuccessResolutionRequest,
) -> Result<VerifiedEvidence, ImageGatewayError> {
    if !root.is_absolute() || std::fs::canonicalize(root).ok().as_deref() != Some(root) {
        return Err(conflict());
    }
    let directory: File = open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private_metadata(&directory, true)?;
    let stdout = read(&directory, "reviewed-query.json", MAX_RECEIPT_BYTES)?;
    let audit_bytes = read(&directory, "execution-audit.json", MAX_RECEIPT_BYTES)?;
    let review_bytes = read(&directory, "review.json", MAX_RECEIPT_BYTES)?;
    let bytes = read(&directory, "selected.png", MAX_IMAGE_BYTES)?;
    let review: Review = serde_json::from_slice(&review_bytes).map_err(|_| conflict())?;
    let audit: ExecutionAudit = serde_json::from_slice(&audit_bytes).map_err(|_| conflict())?;
    let stdout_sha256 = sha256_hex(&stdout);
    let selected_sha256 = sha256_hex(&bytes);
    let execution_audit_sha256 = sha256_hex(&audit_bytes);
    let query_download = audit.argv.len() == 5
        && audit.argv[..3] == ["query_result", "--submit_id", &audit.remote_operation_id]
        && audit.argv[3] == "--download_dir"
        && Path::new(&audit.argv[4]).is_absolute();
    if review.schema_version != 1
        || audit.schema_version != 1
        || stdout_sha256 != request.reviewed_stdout_sha256
        || review.stdout_sha256 != stdout_sha256
        || audit.stdout_sha256 != stdout_sha256
        || review.execution_audit_sha256 != execution_audit_sha256
        || review.selected_sha256 != selected_sha256
        || review.selected_source_index != 0
        || audit.job_id != request.job_id
        || audit.submission_id != request.submission_id
        || audit.executor_execution_id != request.executor_execution_id
        || audit.exit_code != 0
        || audit.executed_at_ms <= 0
        || !query_download
        || !super::valid_sha256(&review.quote_hash)
        || !super::valid_sha256(&review.provider_command_sha256)
    {
        return Err(conflict());
    }
    // Bind the selected bytes to the reviewed download execution, rather than
    // accepting any PNG with the same dimensions and a newly supplied hash.
    if audit.downloaded_images.len() != 4 {
        return Err(conflict());
    }
    for (index, downloaded) in audit.downloaded_images.iter().enumerate() {
        if downloaded.source_index != index
            || downloaded.filename
                != format!("{}_image_{}.png", audit.remote_operation_id, index + 1)
            || !super::valid_sha256(&downloaded.sha256)
            || downloaded.byte_size == 0
            || downloaded.byte_size > MAX_IMAGE_BYTES
        {
            return Err(conflict());
        }
    }
    if audit.downloaded_images[0].sha256 != selected_sha256
        || audit.downloaded_images[0].byte_size != bytes.len() as u64
    {
        return Err(conflict());
    }
    let receipt: Value = serde_json::from_slice(&stdout).map_err(|_| conflict())?;
    if receipt.get("submit_id").and_then(Value::as_str) != Some(audit.remote_operation_id.as_str())
        || receipt.get("gen_status").and_then(Value::as_str) != Some("success")
    {
        return Err(conflict());
    }
    let result = match receipt.get("result_json") {
        Some(Value::String(value)) => {
            serde_json::from_str::<Value>(value).map_err(|_| conflict())?
        }
        Some(Value::Object(value)) => Value::Object(value.clone()),
        _ => return Err(conflict()),
    };
    let images = result
        .get("images")
        .and_then(Value::as_array)
        .ok_or_else(conflict)?;
    if images.len() != 4
        || result
            .get("videos")
            .and_then(Value::as_array)
            .is_none_or(|v| !v.is_empty())
    {
        return Err(conflict());
    }
    let download_dir = &audit.argv[4];
    let valid_download_dir = !download_dir.chars().any(char::is_control)
        && (download_dir == "/"
            || (download_dir.starts_with('/')
                && download_dir[1..]
                    .split('/')
                    .all(|part| !part.is_empty() && part != "." && part != "..")));
    let mut item_ids = std::collections::HashSet::new();
    for (index, image) in images.iter().enumerate() {
        let item = image
            .get("item_id")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty() && v.len() <= 255 && !v.chars().any(char::is_control))
            .ok_or_else(conflict)?;
        let url = image.get("image_url");
        let path = image.get("path");
        if !item_ids.insert(item) || (url.is_none() && path.is_none()) {
            return Err(conflict());
        }
        if let Some(url) = url {
            let url = url.as_str().ok_or_else(conflict)?;
            if !url.starts_with("https://") || url.chars().any(char::is_control) {
                return Err(conflict());
            }
        }
        if let Some(path) = path {
            // CLI downloads replace URLs with local paths. Bind their spelling
            // to the audited files; only the evidence package's PNG is opened.
            let path = path.as_str().ok_or_else(conflict)?;
            let filename = &audit.downloaded_images[index].filename;
            if !valid_download_dir
                || filename.contains(['/', '\\'])
                || filename.chars().any(char::is_control)
                || path != format!("{}/{}", download_dir.trim_end_matches('/'), filename)
            {
                return Err(conflict());
            }
        }
    }
    let selected = &images[0];
    if selected.get("item_id").and_then(Value::as_str) != Some(review.selected_item_id.as_str()) {
        return Err(conflict());
    }
    let width = selected
        .get("width")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(conflict)?;
    let height = selected
        .get("height")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(conflict)?;
    if width == 0
        || height == 0
        || width > 8192
        || height > 8192
        || u64::from(width) * u64::from(height) > 16 * 1024 * 1024
        || image::guess_format(&bytes).ok() != Some(image::ImageFormat::Png)
    {
        return Err(conflict());
    }
    if image::ImageReader::with_format(std::io::Cursor::new(&bytes), image::ImageFormat::Png)
        .into_dimensions()
        .ok()
        != Some((width, height))
    {
        return Err(conflict());
    }
    // Fully decode pixels; a PNG signature and IHDR alone cannot authorize billing.
    let decoded = image::load_from_memory_with_format(&bytes, image::ImageFormat::Png)
        .map_err(|_| conflict())?;
    if decoded.width() != width || decoded.height() != height {
        return Err(conflict());
    }
    Ok(VerifiedEvidence {
        stdout_sha256,
        execution_audit_sha256,
        review_sha256: sha256_hex(&review_bytes),
        selected_sha256,
        selected_item_id: review.selected_item_id,
        remote_operation_id: audit.remote_operation_id,
        quote_hash: review.quote_hash,
        provider_command_sha256: review.provider_command_sha256,
        width,
        height,
        bytes,
    })
}

fn private_metadata(file: &File, directory: bool) -> Result<(), ImageGatewayError> {
    let metadata = file.metadata().map_err(|_| conflict())?;
    // SAFETY: geteuid has no preconditions.
    if metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
        || (directory && !metadata.is_dir())
        || (!directory && (!metadata.is_file() || metadata.nlink() != 1))
    {
        return Err(conflict());
    }
    Ok(())
}

fn read(directory: &File, name: &str, maximum: u64) -> Result<Vec<u8>, ImageGatewayError> {
    let mut file: File = openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(|_| conflict())?;
    private_metadata(&file, false)?;
    let size = file.metadata().map_err(|_| conflict())?.len();
    if size == 0 || size > maximum {
        return Err(conflict());
    }
    let mut bytes = Vec::with_capacity(size as usize);
    file.by_ref()
        .take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| conflict())?;
    if bytes.len() as u64 != size {
        return Err(conflict());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::os::unix::fs::PermissionsExt;

    struct Bundle {
        _directory: tempfile::TempDir,
        root: std::path::PathBuf,
        request: DreaminaImageSuccessResolutionRequest,
        receipt: Value,
        audit: Value,
        review: Value,
        image: Vec<u8>,
    }
    impl Bundle {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
            let root = std::fs::canonicalize(directory.path()).unwrap();
            let request = DreaminaImageSuccessResolutionRequest {
                job_id: Uuid::new_v4(),
                submission_id: Uuid::new_v4(),
                executor_execution_id: Uuid::new_v4(),
                reviewed_stdout_sha256: String::new(),
                operator_reference: "unit:reviewed".into(),
            };
            let image = {
                let mut cursor = std::io::Cursor::new(Vec::new());
                image::DynamicImage::new_rgb8(1, 1)
                    .write_to(&mut cursor, image::ImageFormat::Png)
                    .unwrap();
                cursor.into_inner()
            };
            let receipt = json!({"submit_id":"original-remote-id","gen_status":"success","result_json":serde_json::to_string(&json!({"images":(0..4).map(|i|json!({"item_id":format!("item-{i}"),"image_url":format!("https://media.example.test/{i}.png"),"width":1,"height":1})).collect::<Vec<_>>(),"videos":[]})).unwrap()});
            let audit = json!({"schema_version":1,"job_id":request.job_id,"submission_id":request.submission_id,"executor_execution_id":request.executor_execution_id,"remote_operation_id":"original-remote-id","argv":["query_result","--submit_id","original-remote-id","--download_dir","/existing/original-download"],"stdout_sha256":"","exit_code":0,"executed_at_ms":1,"downloaded_images":(0..4).map(|i|json!({"source_index":i,"filename":format!("original-remote-id_image_{}.png",i+1),"sha256":sha256_hex(&image),"byte_size":image.len()})).collect::<Vec<_>>()});
            let review = json!({"schema_version":1,"stdout_sha256":"","execution_audit_sha256":"","selected_sha256":"","selected_source_index":0,"selected_item_id":"item-0","quote_hash":"a".repeat(64),"provider_command_sha256":"b".repeat(64)});
            Self {
                _directory: directory,
                root,
                request,
                receipt,
                audit,
                review,
                image,
            }
        }
        fn with_download_paths(object_result: bool) -> Self {
            let mut bundle = Self::new();
            let download_dir = bundle.root.join("unretained-original-download");
            assert!(!download_dir.exists());
            bundle.audit["argv"][4] = json!(download_dir.to_str().unwrap());
            let mut result: Value =
                serde_json::from_str(bundle.receipt["result_json"].as_str().unwrap()).unwrap();
            for (index, image) in result["images"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .enumerate()
            {
                image.as_object_mut().unwrap().remove("image_url");
                image["path"] = json!(format!(
                    "{}/{}",
                    download_dir.display(),
                    bundle.audit["downloaded_images"][index]["filename"]
                        .as_str()
                        .unwrap()
                ));
            }
            bundle.receipt["result_json"] = if object_result {
                result
            } else {
                json!(serde_json::to_string(&result).unwrap())
            };
            bundle
        }
        fn save(&mut self) {
            let stdout = serde_json::to_vec(&self.receipt).unwrap();
            self.request.reviewed_stdout_sha256 = sha256_hex(&stdout);
            self.audit["stdout_sha256"] = json!(self.request.reviewed_stdout_sha256);
            let audit = serde_json::to_vec(&self.audit).unwrap();
            self.review["stdout_sha256"] = json!(self.request.reviewed_stdout_sha256);
            self.review["execution_audit_sha256"] = json!(sha256_hex(&audit));
            self.review["selected_sha256"] = json!(sha256_hex(&self.image));
            for (name, bytes) in [
                ("reviewed-query.json", stdout),
                ("execution-audit.json", audit),
                ("review.json", serde_json::to_vec(&self.review).unwrap()),
                ("selected.png", self.image.clone()),
            ] {
                std::fs::write(self.root.join(name), bytes).unwrap();
                std::fs::set_permissions(
                    self.root.join(name),
                    std::fs::Permissions::from_mode(0o600),
                )
                .unwrap();
            }
        }
    }

    #[test]
    fn accepts_only_original_query_and_first_fully_decoded_png() {
        let mut bundle = Bundle::new();
        bundle.save();
        let verified = load(&bundle.root, &bundle.request).unwrap();
        assert_eq!((verified.width, verified.height), (1, 1));
        assert_eq!(verified.selected_item_id, "item-0");
        bundle.audit["argv"] = json!([
            "query_result",
            "--submit_id",
            "original-remote-id",
            "--download_dir",
            "/existing/original-download"
        ]);
        bundle.save();
        assert!(load(&bundle.root, &bundle.request).is_ok());
        for argv in [
            json!(["text2image", "--prompt", "new generation"]),
            json!(["query_result", "--submit_id", "different-id"]),
            json!([
                "query_result",
                "--submit_id",
                "original-remote-id",
                "--download_dir",
                "relative"
            ]),
        ] {
            bundle.audit["argv"] = argv;
            bundle.save();
            assert!(load(&bundle.root, &bundle.request).is_err());
        }
    }

    #[test]
    fn accepts_object_or_single_string_without_rewriting_reviewed_bytes() {
        for object_result in [false, true] {
            let mut bundle = Bundle::new();
            if object_result {
                bundle.receipt["result_json"] =
                    serde_json::from_str(bundle.receipt["result_json"].as_str().unwrap()).unwrap();
            }
            bundle.save();
            let stdout = std::fs::read(bundle.root.join("reviewed-query.json")).unwrap();
            let verified = load(&bundle.root, &bundle.request).unwrap();
            assert_eq!(verified.stdout_sha256, sha256_hex(&stdout));
            assert_eq!(verified.selected_item_id, "item-0");
            assert_eq!((verified.width, verified.height), (1, 1));
            assert_eq!(
                std::fs::read(bundle.root.join("reviewed-query.json")).unwrap(),
                stdout
            );
        }
    }

    #[test]
    fn accepts_reviewed_download_paths_without_original_directory_or_raw_changes() {
        for object_result in [false, true] {
            let mut bundle = Bundle::with_download_paths(object_result);
            bundle.save();
            let stdout = std::fs::read(bundle.root.join("reviewed-query.json")).unwrap();
            let verified = load(&bundle.root, &bundle.request).unwrap();
            assert_eq!(verified.stdout_sha256, sha256_hex(&stdout));
            assert_eq!(verified.selected_item_id, "item-0");
            assert_eq!(verified.bytes, bundle.image);
            assert_eq!(
                std::fs::read(bundle.root.join("reviewed-query.json")).unwrap(),
                stdout
            );
            assert!(!Path::new(bundle.audit["argv"][4].as_str().unwrap()).exists());
        }
    }

    #[test]
    fn rejects_rehashed_receipt_paths_outside_each_reviewed_canonical_filename() {
        for object_result in [false, true] {
            for index in 0..4 {
                for mutation in 0..12 {
                    let mut bundle = Bundle::with_download_paths(true);
                    let dir = bundle.audit["argv"][4].as_str().unwrap();
                    let filename = bundle.audit["downloaded_images"][index]["filename"]
                        .as_str()
                        .unwrap();
                    let path = match mutation {
                        0 => json!(format!("{dir}-neighbor/{filename}")),
                        1 => json!(format!("{dir}/nested/{filename}")),
                        2 => json!(format!("{dir}/different-remote-id_image_{}.png", index + 1)),
                        3 => json!(format!(
                            "{dir}/original-remote-id_image_{}.png",
                            (index + 1) % 4 + 1
                        )),
                        4 => json!(format!("{dir}/../{filename}")),
                        5 => json!(format!("{dir}/./{filename}")),
                        6 => json!(format!("{dir}//{filename}")),
                        7 => json!(filename),
                        8 => json!(format!("{dir}/{filename}\n")),
                        9 => json!(format!("{dir}/{filename}\u{85}")),
                        10 => Value::Null,
                        _ => json!(1),
                    };
                    bundle.receipt["result_json"]["images"][index]["path"] = path;
                    if !object_result {
                        bundle.receipt["result_json"] =
                            json!(serde_json::to_string(&bundle.receipt["result_json"]).unwrap());
                    }
                    bundle.save();
                    assert!(
                        load(&bundle.root, &bundle.request).is_err(),
                        "accepted mutation {mutation} at index {index}"
                    );
                }
            }
        }
    }

    #[test]
    fn rejects_missing_or_invalid_media_fields_without_falling_back_to_another_field() {
        for mutation in 0..5 {
            let mut bundle = Bundle::with_download_paths(true);
            let image = bundle.receipt["result_json"]["images"][2]
                .as_object_mut()
                .unwrap();
            match mutation {
                0 => {
                    image.insert(
                        "image_url".into(),
                        json!("https://media.example.test/2.png"),
                    );
                    image.insert("path".into(), Value::Null);
                }
                1 => {
                    image.insert(
                        "image_url".into(),
                        json!("https://media.example.test/2.png"),
                    );
                    image.insert("path".into(), json!("/different/file.png"));
                }
                2 => {
                    image.insert("image_url".into(), Value::Null);
                }
                3 => {
                    image.insert("image_url".into(), json!("http://media.example.test/2.png"));
                }
                _ => {
                    image.remove("path");
                }
            }
            bundle.save();
            assert!(load(&bundle.root, &bundle.request).is_err());
        }
    }

    #[test]
    fn rejects_unsafe_download_directories_even_when_receipt_paths_match_and_are_rehashed() {
        for dir in [
            "relative",
            "/safe/../safe",
            "/safe/./nested",
            "/safe//nested",
            "/safe/",
            "/safe/\n",
            "/safe/\u{85}",
        ] {
            let mut bundle = Bundle::with_download_paths(true);
            bundle.audit["argv"][4] = json!(dir);
            for (index, image) in bundle.receipt["result_json"]["images"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .enumerate()
            {
                image["path"] = json!(format!("{dir}/original-remote-id_image_{}.png", index + 1));
            }
            bundle.save();
            assert!(load(&bundle.root, &bundle.request).is_err());
        }
    }

    #[test]
    fn rejects_audited_filenames_that_are_not_single_safe_components() {
        for remote in ["remote/../other", "remote\\other", "remote\u{85}other"] {
            let mut bundle = Bundle::with_download_paths(true);
            bundle.audit["remote_operation_id"] = json!(remote);
            bundle.audit["argv"][2] = json!(remote);
            bundle.receipt["submit_id"] = json!(remote);
            let dir = bundle.audit["argv"][4].as_str().unwrap().to_owned();
            for index in 0..4 {
                let filename = format!("{remote}_image_{}.png", index + 1);
                bundle.audit["downloaded_images"][index]["filename"] = json!(filename);
                bundle.receipt["result_json"]["images"][index]["path"] =
                    json!(format!("{dir}/{filename}"));
            }
            bundle.save();
            assert!(load(&bundle.root, &bundle.request).is_err());
        }
    }

    #[test]
    fn rejects_non_object_or_nested_result_json_even_when_rehashed() {
        let original = Bundle::new().receipt["result_json"].clone();
        for result in [
            Value::Null,
            json!([]),
            json!("null"),
            json!("[]"),
            json!(serde_json::to_string(&original).unwrap()),
        ] {
            let mut bundle = Bundle::new();
            bundle.receipt["result_json"] = result;
            bundle.save();
            assert!(load(&bundle.root, &bundle.request).is_err());
        }
    }

    #[test]
    fn rejects_semantically_equal_result_without_original_raw_hash_binding() {
        let mut bundle = Bundle::new();
        bundle.save();
        let original_sha256 = bundle.request.reviewed_stdout_sha256.clone();
        bundle.receipt["result_json"] =
            serde_json::from_str(bundle.receipt["result_json"].as_str().unwrap()).unwrap();
        let changed_stdout = serde_json::to_vec(&bundle.receipt).unwrap();
        assert_ne!(sha256_hex(&changed_stdout), original_sha256);
        std::fs::write(bundle.root.join("reviewed-query.json"), changed_stdout).unwrap();
        assert!(load(&bundle.root, &bundle.request).is_err());
    }

    #[test]
    fn rejects_rehashed_bad_pixels_wrong_selection_or_unprotected_files() {
        for change in 0..5 {
            let mut bundle = Bundle::new();
            match change {
                0 => bundle.image.truncate(24),
                1 => bundle.review["selected_source_index"] = json!(1),
                2 => bundle.review["selected_item_id"] = json!("item-1"),
                3 => bundle.audit["submission_id"] = json!(Uuid::new_v4()),
                _ => bundle.receipt["result_json"] = json!({"images":[],"videos":[]}),
            }
            bundle.save();
            assert!(
                load(&bundle.root, &bundle.request).is_err(),
                "accepted mutation {change}"
            );
        }
        let mut bundle = Bundle::new();
        bundle.save();
        std::fs::set_permissions(
            bundle.root.join("selected.png"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(load(&bundle.root, &bundle.request).is_err());
        std::fs::set_permissions(
            bundle.root.join("selected.png"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        std::fs::hard_link(
            bundle.root.join("selected.png"),
            bundle.root.join("second-link.png"),
        )
        .unwrap();
        assert!(load(&bundle.root, &bundle.request).is_err());
    }

    #[test]
    fn rejects_rehashed_same_size_pixels_without_reviewed_download_binding() {
        let mut bundle = Bundle::new();
        let mut encoded = std::io::Cursor::new(Vec::new());
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(1, 1, image::Rgb([255, 0, 0])))
            .write_to(&mut encoded, image::ImageFormat::Png)
            .unwrap();
        bundle.image = encoded.into_inner();
        bundle.save();
        assert!(load(&bundle.root, &bundle.request).is_err());

        let mut bundle = Bundle::new();
        bundle.audit["downloaded_images"][0]["filename"] = json!("unrelated.png");
        bundle.save();
        assert!(load(&bundle.root, &bundle.request).is_err());

        let mut bundle = Bundle::new();
        bundle.audit["argv"] = json!(["query_result", "--submit_id", "original-remote-id"]);
        bundle.save();
        assert!(load(&bundle.root, &bundle.request).is_err());

        let mut bundle = Bundle::new();
        bundle.image.truncate(bundle.image.len() / 2);
        bundle.audit["downloaded_images"][0]["sha256"] = json!(sha256_hex(&bundle.image));
        bundle.audit["downloaded_images"][0]["byte_size"] = json!(bundle.image.len());
        bundle.save();
        assert!(load(&bundle.root, &bundle.request).is_err());
    }
}
