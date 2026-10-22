use super::*;
use crate::input_blobs::{InputBlobDeleteError, InputBlobKey, InputBlobRef, InputBlobWriteError};
use async_trait::async_trait;
use std::{
    fs,
    io::Cursor,
    os::unix::fs::{PermissionsExt, symlink},
    sync::atomic::{AtomicUsize, Ordering},
};
use uuid::Uuid;

struct TestStore {
    bytes: Vec<u8>,
    reads: AtomicUsize,
}
#[async_trait]
impl InputBlobStore for TestStore {
    async fn put(&self, _: InputBlobKey, _: &[u8]) -> Result<InputBlobRef, InputBlobWriteError> {
        unreachable!()
    }
    async fn get(&self, _: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.bytes.clone())
    }
    async fn delete(&self, _: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
        unreachable!()
    }
    async fn delete_session(&self, _: Uuid) -> Result<(), InputBlobDeleteError> {
        unreachable!()
    }
}

fn workspace() -> (tempfile::TempDir, WorkingDirectory) {
    let temp = tempfile::tempdir().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let workspace = WorkingDirectory::new_private(temp.path()).unwrap();
    (temp, workspace)
}

fn png() -> Vec<u8> {
    let mut output = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(2, 3)
        .write_to(&mut output, image::ImageFormat::Png)
        .unwrap();
    output.into_inner()
}

fn input(bytes: &[u8], media_type: &str, role: &str, index: u16) -> ExecutorInputObject {
    ExecutorInputObject::new(
        InputBlobRef {
            key: InputBlobKey {
                admission_session_id: Uuid::new_v4(),
                input_id: Uuid::new_v4(),
            },
            storage_backend: "test".into(),
            object_key: "inputs/frozen-object".into(),
            sha256_hex: hex::encode(Sha256::digest(bytes)),
            byte_size: bytes.len() as u64,
        },
        role,
        index,
        media_type,
    )
    .unwrap()
}

fn image_file() -> GatedCliInputFile {
    GatedCliInputFile::image("image-0.png", 1024 * 1024, 2, 3).unwrap()
}

#[tokio::test]
async fn text_submission_needs_no_blob_store_or_input_files() {
    let (temp, directory) = workspace();
    stage_inputs(
        None,
        &[],
        vec![],
        &directory,
        Instant::now(),
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn frozen_image_is_published_privately_and_replay_compares_existing_bytes() {
    let (temp, directory) = workspace();
    let bytes = png();
    let input = input(&bytes, "image/png", "image", 0);
    let concrete = Arc::new(TestStore {
        bytes: bytes.clone(),
        reads: AtomicUsize::new(0),
    });
    let store: Arc<dyn InputBlobStore> = concrete.clone();
    for _ in 0..2 {
        stage_inputs(
            Some(&store),
            std::slice::from_ref(&input),
            vec![image_file()],
            &directory,
            Instant::now(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    }
    let published = temp.path().join("image-0.png");
    assert_eq!(fs::read(&published).unwrap(), bytes);
    assert_eq!(
        fs::metadata(&published).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(concrete.reads.load(Ordering::SeqCst), 2);
    fs::write(&published, b"different").unwrap();
    let failure = stage_inputs(
        Some(&store),
        &[input],
        vec![image_file()],
        &directory,
        Instant::now(),
        Duration::from_secs(1),
    )
    .await
    .unwrap_err();
    assert_eq!(failure.effect(), EffectCertainty::NoRemoteEffect);
    assert_eq!(fs::read(published).unwrap(), b"different");
}

#[tokio::test]
async fn corrupted_store_bytes_fail_before_any_file_is_published() {
    let (temp, directory) = workspace();
    let input = input(&png(), "image/png", "image", 0);
    let store: Arc<dyn InputBlobStore> = Arc::new(TestStore {
        bytes: b"changed".to_vec(),
        reads: AtomicUsize::new(0),
    });
    let error = stage_inputs(
        Some(&store),
        &[input],
        vec![image_file()],
        &directory,
        Instant::now(),
        Duration::from_secs(1),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), "provider_submit_input_integrity_failed");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn expired_budget_and_missing_store_fail_without_blob_reads() {
    let (_temp, directory) = workspace();
    let bytes = png();
    let input = input(&bytes, "image/png", "image", 0);
    let concrete = Arc::new(TestStore {
        bytes,
        reads: AtomicUsize::new(0),
    });
    let store: Arc<dyn InputBlobStore> = concrete.clone();
    let failure = stage_inputs(
        Some(&store),
        std::slice::from_ref(&input),
        vec![image_file()],
        &directory,
        Instant::now(),
        Duration::ZERO,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.code(), "provider_submit_deadline_elapsed");
    assert_eq!(concrete.reads.load(Ordering::SeqCst), 0);
    let failure = stage_inputs(
        None,
        &[input],
        vec![image_file()],
        &directory,
        Instant::now(),
        Duration::from_secs(1),
    )
    .await
    .unwrap_err();
    assert_eq!(failure.code(), "provider_submit_input_store_unavailable");
}

#[test]
fn names_counts_sessions_and_duplicate_objects_are_rejected() {
    for filename in ["../outside", "/absolute", ".hidden", "a/b", "", "a\0b"] {
        assert!(GatedCliInputFile::image(filename, 100, 2, 3).is_err());
    }
    let one = input(&png(), "image/png", "image", 0);
    let mut two = input(&png(), "image/png", "image", 1);
    two.blob.key.admission_session_id = one.blob.key.admission_session_id;
    two.blob.object_key = "inputs/second-object".into();
    let files = [
        image_file(),
        GatedCliInputFile::image("image-1.png", 1024, 2, 3).unwrap(),
    ];
    assert!(validate_manifest(&[one.clone(), two.clone()], &files).is_ok());
    assert!(validate_manifest(std::slice::from_ref(&one), &[]).is_err());
    assert!(validate_manifest(&[one.clone(), two.clone()], &[image_file(), image_file()]).is_err());
    two.blob.key.admission_session_id = Uuid::new_v4();
    assert!(validate_manifest(&[one.clone(), two.clone()], &files).is_err());
    two.blob.key.admission_session_id = one.blob.key.admission_session_id;
    two.blob.key.input_id = one.blob.key.input_id;
    assert!(validate_manifest(&[one.clone(), two.clone()], &files).is_err());
    two.blob.key.input_id = Uuid::new_v4();
    two.blob.object_key = one.blob.object_key.clone();
    assert!(validate_manifest(&[one.clone(), two], &files).is_err());
    assert!(
        validate_manifest(
            &[one],
            &[GatedCliInputFile::image("image.png", 1, 2, 3).unwrap()]
        )
        .is_err()
    );
}

#[test]
fn media_size_digest_and_canonical_dimensions_must_match() {
    let bytes = png();
    let mut object = input(&bytes, "image/png", "image", 0);
    assert!(validate_bytes(&object, &image_file(), &bytes).is_ok());
    assert!(
        validate_bytes(
            &object,
            &GatedCliInputFile::image("image.png", 1024, 3, 2).unwrap(),
            &bytes
        )
        .is_err()
    );
    object.media_type = "image/jpeg".into();
    assert!(validate_bytes(&object, &image_file(), &bytes).is_err());
    object.media_type = "image/png".into();
    object.blob.byte_size += 1;
    assert!(validate_bytes(&object, &image_file(), &bytes).is_err());
    object.blob.byte_size -= 1;
    object.blob.sha256_hex = "a".repeat(64);
    assert!(validate_bytes(&object, &image_file(), &bytes).is_err());
}

#[test]
fn staging_rejects_symlinks_hardlinks_and_replaced_directory() {
    let (temp, directory) = workspace();
    let bound = InputDirectory::open(&directory).unwrap();
    let outside = tempfile::NamedTempFile::new().unwrap();
    fs::write(outside.path(), b"keep").unwrap();
    symlink(outside.path(), temp.path().join("image-0.png")).unwrap();
    assert!(bound.publish(&image_file(), b"new").is_err());
    assert_eq!(fs::read(outside.path()).unwrap(), b"keep");
    fs::remove_file(temp.path().join("image-0.png")).unwrap();
    fs::hard_link(outside.path(), temp.path().join("image-0.png")).unwrap();
    assert!(bound.publish(&image_file(), b"keep").is_err());
    let old = temp.path().with_extension("old");
    fs::rename(temp.path(), &old).unwrap();
    fs::create_dir(temp.path()).unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    assert!(bound.publish(&image_file(), b"keep").is_err());
    fs::remove_dir_all(old).unwrap();
}

#[test]
fn video_requires_real_video_track_and_exact_frozen_duration() {
    let config = mp4::Mp4Config {
        major_brand: "isom".parse().unwrap(),
        minor_version: 512,
        compatible_brands: vec!["isom".parse().unwrap(), "avc1".parse().unwrap()],
        timescale: 1000,
    };
    let mut writer = mp4::Mp4Writer::write_start(Cursor::new(Vec::new()), &config).unwrap();
    writer
        .add_track(
            &mp4::AvcConfig {
                width: 16,
                height: 16,
                seq_param_set: vec![0x67, 0x42, 0x00, 0x1f],
                pic_param_set: vec![0x68, 0xce, 0x3c, 0x80],
            }
            .into(),
        )
        .unwrap();
    writer
        .write_sample(
            1,
            &mp4::Mp4Sample {
                start_time: 0,
                duration: 1000,
                rendering_offset: 0,
                is_sync: true,
                bytes: vec![0, 0, 0, 1, 0x65].into(),
            },
        )
        .unwrap();
    writer.write_end().unwrap();
    let bytes = writer.into_writer().into_inner();
    let duration_ms = probe_input(&bytes, MediaInputKind::Video)
        .unwrap()
        .duration_ms()
        .unwrap();
    let object = input(&bytes, "video/mp4", "video", 0);
    assert!(
        validate_bytes(
            &object,
            &GatedCliInputFile::video("video.mp4", MAX_INPUT_BYTES, duration_ms).unwrap(),
            &bytes
        )
        .is_ok()
    );
    assert!(
        validate_bytes(
            &object,
            &GatedCliInputFile::video("video.mp4", MAX_INPUT_BYTES, duration_ms + 1).unwrap(),
            &bytes
        )
        .is_err()
    );
    let fake = b"\0\0\0\x18ftypisom\0\0\0\0isommp42";
    let object = input(fake, "video/mp4", "video", 0);
    assert!(
        validate_bytes(
            &object,
            &GatedCliInputFile::video("video.mp4", MAX_INPUT_BYTES, 1000).unwrap(),
            fake
        )
        .is_err()
    );
}

#[test]
fn audio_requires_decoded_bytes_and_exact_frozen_duration_and_mime() {
    let bytes = include_bytes!("../../../media_inputs/fixtures/tone.mp3");
    let object = input(bytes, "audio/mpeg", "audio", 0);
    let file =
        GatedCliInputFile::audio("audio-0.mp3", MediaInputKind::Audio.max_bytes(), 2000).unwrap();
    assert!(validate_bytes(&object, &file, bytes).is_ok());
    let wrong_duration =
        GatedCliInputFile::audio("audio-0.mp3", MediaInputKind::Audio.max_bytes(), 2001).unwrap();
    assert!(validate_bytes(&object, &wrong_duration, bytes).is_err());
    let wrong_mime = input(bytes, "audio/wav", "audio", 0);
    assert!(validate_bytes(&wrong_mime, &file, bytes).is_err());
    let damaged = &bytes[..bytes.len() - 1];
    let damaged_object = input(damaged, "audio/mpeg", "audio", 0);
    assert!(validate_bytes(&damaged_object, &file, damaged).is_err());
}
