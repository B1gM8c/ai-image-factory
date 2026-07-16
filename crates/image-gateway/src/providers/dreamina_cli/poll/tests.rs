use std::{
    fs,
    io::Cursor,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};

use image::{DynamicImage, ImageFormat, Rgba, RgbaImage};
use image_cli_runtime::{ATTEMPT_WORKSPACE_LOCK_FILENAME, WorkingDirectory};
use image_provider_dreamina_cli::DreaminaCliQueryPolicyV1;
use image_provider_sdk::{
    DurableArtifactRef, EffectCertainty, PollObservation, RemoteOperationRef, RetryDirective,
};
use image_provider_test_support::RecordingArtifactSink;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::{
    executor::ExecutorExecutionProfile,
    provider_tasks::{ProviderAccountHomeCapability, ProviderRuntimeProfile},
};

#[tokio::test]
async fn pending_and_terminal_failure_require_an_empty_download_directory() {
    let pending = Fixture::new(r#"printf '{"submit_id":"%s","gen_status":"querying"}' "$submit""#);
    let operation = pending.operation();
    let mut sink = pending.sink([0_u8; 32]);
    let observation = pending
        .driver(1024 * 1024)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap();
    assert_eq!(
        observation,
        PollObservation::Pending {
            next_poll_after_ms: Some(DEFAULT_POLL_AFTER_MS)
        }
    );
    assert!(sink.bytes().is_empty());
    assert_eq!(sink.finalize_count(), 0);
    pending.assert_workspace_has_no_attempts();

    let failed = Fixture::new(
        r#"printf '{"submit_id":"%s","gen_status":"fail","fail_reason":"denied"}' "$submit""#,
    );
    let operation = failed.operation();
    let mut sink = failed.sink([0_u8; 32]);
    let observation = failed
        .driver(1024 * 1024)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap();
    let PollObservation::Failed(failure) = observation else {
        panic!("expected terminal failure");
    };
    assert_eq!(failure.code(), "dreamina_generation_failed");
    assert_eq!(failure.effect(), EffectCertainty::NoRemoteEffect);
    assert_eq!(failure.retry(), RetryDirective::Never);
    assert!(sink.bytes().is_empty());
    assert_eq!(sink.finalize_count(), 0);
    failed.assert_workspace_has_no_attempts();
}

#[tokio::test]
async fn success_streams_one_valid_image_and_verifies_the_sink_manifest() {
    let fixture = Fixture::new(
        r#"/bin/cp "$HOME/source.png" "$download/result.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit""#,
    );
    let bytes = png_bytes([10, 20, 30, 255]);
    fs::write(fixture.account_home.join("source.png"), &bytes).unwrap();
    let sha256: [u8; 32] = Sha256::digest(&bytes).into();
    let operation = fixture.operation();
    let mut sink = fixture.sink(sha256);
    let observation = fixture
        .driver(1024 * 1024)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap();

    let PollObservation::Completed(completed) = observation else {
        panic!("expected completed observation");
    };
    assert_eq!(completed.artifact().media_type(), "image/png");
    assert_eq!(completed.artifact().byte_size(), bytes.len() as u64);
    assert_eq!(completed.artifact().sha256(), &sha256);
    assert_eq!(sink.bytes(), bytes);
    assert_eq!(sink.finalize_count(), 1);
    assert!(
        sink.chunk_sizes()
            .iter()
            .all(|size| *size <= image_cli_runtime::STREAM_BUFFER_BYTES)
    );
    fixture.assert_workspace_has_no_attempts();
}

#[tokio::test]
async fn mismatched_receipts_and_multiple_outputs_fail_closed() {
    let mismatch = Fixture::new(r#"printf '{"submit_id":"another-task","gen_status":"querying"}'"#);
    let operation = mismatch.operation();
    let mut sink = mismatch.sink([0_u8; 32]);
    let failure = mismatch
        .driver(1024)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap_err();
    assert_eq!(failure.code(), "dreamina_poll_receipt_mismatch");
    assert_eq!(failure.retry(), RetryDirective::Never);
    assert!(sink.bytes().is_empty());
    mismatch.assert_workspace_has_no_attempts();

    let multiple = Fixture::new(
        r#"printf x > "$download/one.png"
printf y > "$download/two.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit""#,
    );
    let operation = multiple.operation();
    let mut sink = multiple.sink([0_u8; 32]);
    let failure = multiple
        .driver(1024)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap_err();
    assert_eq!(failure.code(), "dreamina_poll_artifact_invalid");
    assert_eq!(failure.retry(), RetryDirective::Never);
    assert!(sink.bytes().is_empty());
    multiple.assert_workspace_has_no_attempts();

    let invalid_media = Fixture::new(
        r#"printf 'not-an-image' > "$download/result.bin"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit""#,
    );
    let operation = invalid_media.operation();
    let mut sink = invalid_media.sink([0_u8; 32]);
    let failure = invalid_media
        .driver(1024)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap_err();
    assert_eq!(failure.code(), "dreamina_poll_media_invalid");
    assert_eq!(sink.bytes(), b"not-an-image");
    assert_eq!(sink.finalize_count(), 0);
    invalid_media.assert_workspace_has_no_attempts();

    let oversized = Fixture::new(
        r#"printf '12345' > "$download/result.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit""#,
    );
    let operation = oversized.operation();
    let mut sink = oversized.sink([0_u8; 32]);
    let failure = oversized
        .driver(4)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap_err();
    assert_eq!(failure.code(), "dreamina_poll_artifact_invalid");
    assert!(sink.bytes().is_empty());
    oversized.assert_workspace_has_no_attempts();
}

#[tokio::test]
async fn success_rejects_a_sink_manifest_that_does_not_match_streamed_bytes() {
    let fixture = Fixture::new(
        r#"/bin/cp "$HOME/source.png" "$download/result.png"
printf '{"submit_id":"%s","gen_status":"success"}' "$submit""#,
    );
    let bytes = png_bytes([10, 20, 30, 255]);
    fs::write(fixture.account_home.join("source.png"), &bytes).unwrap();
    let operation = fixture.operation();
    let mut sink = fixture.sink([9_u8; 32]);
    let failure = fixture
        .driver(1024 * 1024)
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap_err();

    assert_eq!(failure.code(), "dreamina_poll_manifest_mismatch");
    assert_eq!(sink.bytes(), bytes);
    assert_eq!(sink.finalize_count(), 1);
    fixture.assert_workspace_has_no_attempts();
}

#[tokio::test]
async fn canceling_a_poll_future_terminates_the_query_process_and_cleans_the_attempt() {
    let fixture = Fixture::new(
        r#"printf '%s' "$$" > "$HOME/query.pid"
/bin/sleep 30"#,
    );
    let operation = fixture.operation();
    let driver = fixture.driver(1024);
    let task = tokio::spawn(async move {
        let mut sink = RecordingArtifactSink::new(
            DurableArtifactRef::new("test", "dreamina-cancel").unwrap(),
            [0_u8; 32],
        );
        driver.poll_operation(&operation, &mut sink).await
    });
    let pid_path = fixture.account_home.join("query.pid");
    let pid = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(value) = fs::read_to_string(&pid_path)
                && let Ok(pid) = value.parse::<u32>()
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("query process starts");

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    wait_for_process_exit(pid).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if workspace_has_no_attempts(&fixture.workspace) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("canceled attempt is cleaned");
}

#[tokio::test]
async fn replacing_the_workspace_root_fails_before_the_query_process_starts() {
    let fixture = Fixture::new(
        r#"printf called > "$HOME/query-called"
printf '{"submit_id":"%s","gen_status":"querying"}' "$submit""#,
    );
    let driver = fixture.driver(1024);
    let moved = fixture._root.path().join("moved-workspace");
    fs::rename(&fixture.workspace, &moved).unwrap();
    fs::create_dir(&fixture.workspace).unwrap();
    fs::set_permissions(&fixture.workspace, fs::Permissions::from_mode(0o700)).unwrap();
    let operation = fixture.operation();
    let mut sink = fixture.sink([0_u8; 32]);

    let failure = driver
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap_err();

    assert_eq!(failure.code(), "dreamina_poll_workspace_invalid");
    assert!(!fixture.account_home.join("query-called").exists());
    assert!(workspace_has_no_attempts(&fixture.workspace));
}

#[test]
fn driver_rejects_unsafe_workspace_and_unbounded_artifact_limits() {
    let fixture = Fixture::new("printf '{}'");
    assert!(matches!(
        DreaminaCliPollDriverV1::new(
            fixture.policy(),
            fixture.binding.clone(),
            MAX_ARTIFACT_BYTES + 1,
        ),
        Err(DreaminaCliPollDriverConfigError::InvalidArtifactLimit)
    ));

    fs::set_permissions(&fixture.workspace, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        DreaminaCliPollDriverV1::new(fixture.policy(), fixture.binding, 1024),
        Err(DreaminaCliPollDriverConfigError::Workspace(
            AttemptWorkspaceError::Integrity
        ))
    ));
}

#[test]
fn driver_cleans_crash_left_attempts_and_exclusively_owns_the_workspace() {
    let fixture = Fixture::new("printf '{}'");
    let crashed = fixture
        .workspace
        .join(format!("{DREAMINA_POLL_ATTEMPT_PREFIX}crash-left"));
    fs::create_dir(&crashed).unwrap();
    fs::set_permissions(&crashed, fs::Permissions::from_mode(0o700)).unwrap();
    fs::create_dir(crashed.join("nested")).unwrap();
    fs::write(crashed.join("nested/artifact.bin"), b"stale").unwrap();

    let owner = fixture.driver(1024);

    assert!(!crashed.exists());
    assert!(matches!(
        DreaminaCliPollDriverV1::new(fixture.policy(), fixture.binding.clone(), 1024),
        Err(DreaminaCliPollDriverConfigError::Workspace(
            AttemptWorkspaceError::AlreadyLocked
        ))
    ));
    drop(owner);
    if let Err(error) =
        DreaminaCliPollDriverV1::new(fixture.policy(), fixture.binding.clone(), 1024)
    {
        panic!("workspace lock was not reacquired: {error:?}");
    }
}

#[tokio::test]
async fn runtime_profile_composition_revalidates_private_account_home_before_spawn() {
    let fixture = Fixture::new(
        r#"printf called > "$HOME/query-called"
printf '{"submit_id":"%s","gen_status":"querying"}' "$submit""#,
    );
    let profile = ProviderRuntimeProfile::new(runtime_profile()).unwrap();
    let capability = ProviderAccountHomeCapability::new(
        PROVIDER_ID,
        profile.credential_pool_id(),
        profile.provider_account_id(),
        profile.credential_ref(),
        profile.credential_revision(),
        profile.credential_auth_sha256(),
        &fixture.account_home,
    )
    .unwrap();
    let driver = DreaminaCliPollDriverV1::from_runtime_profile(
        &profile,
        &capability,
        fixture.process_config(1024),
    )
    .unwrap();
    fs::set_permissions(&fixture.account_home, fs::Permissions::from_mode(0o755)).unwrap();
    let operation = fixture.operation();
    let mut sink = fixture.sink([0_u8; 32]);

    let failure = driver
        .poll_operation(&operation, &mut sink)
        .await
        .unwrap_err();

    assert_eq!(failure.code(), "dreamina_poll_process_invalid");
    assert!(!fixture.account_home.join("query-called").exists());
    fixture.assert_workspace_has_no_attempts();
}

#[test]
fn runtime_profile_composition_rejects_descriptor_drift_before_workspace_lock() {
    let fixture = Fixture::new("printf '{}'");
    let mut changed = runtime_profile();
    changed.operation_descriptor_sha256_v1 = "f".repeat(64);
    let profile = ProviderRuntimeProfile::new(changed).unwrap();
    let capability = ProviderAccountHomeCapability::new(
        PROVIDER_ID,
        profile.credential_pool_id(),
        profile.provider_account_id(),
        profile.credential_ref(),
        profile.credential_revision(),
        profile.credential_auth_sha256(),
        &fixture.account_home,
    )
    .unwrap();

    assert!(matches!(
        DreaminaCliPollDriverV1::from_runtime_profile(
            &profile,
            &capability,
            fixture.process_config(1024),
        ),
        Err(DreaminaCliPollDriverConfigError::ProfileMismatch)
    ));
    assert!(
        !fixture
            .workspace
            .join(ATTEMPT_WORKSPACE_LOCK_FILENAME)
            .exists()
    );
}

#[test]
fn media_detection_accepts_only_supported_image_signatures() {
    assert_eq!(
        image_media_type(b"\x89PNG\r\n\x1a\nrest"),
        Some("image/png")
    );
    assert_eq!(
        image_media_type(&[0xff, 0xd8, 0xff, 0x00]),
        Some("image/jpeg")
    );
    assert_eq!(image_media_type(b"RIFF1234WEBP"), Some("image/webp"));
    assert_eq!(image_media_type(b"....ftypisom"), None);
    assert_eq!(image_media_type(b"not-media"), None);
}

#[test]
fn poll_binding_accepts_only_explicitly_supported_image_models() {
    for model in [
        "dreamina-image-3.0",
        "dreamina-image-3.1",
        "dreamina-image-4.0",
        "dreamina-image-4.1",
        "dreamina-image-4.5",
        "dreamina-image-4.6",
        "dreamina-image-4.7",
        "dreamina-image-5.0",
    ] {
        assert!(supported_image_model(model));
    }
    assert!(!supported_image_model("dreamina-image-5.1"));
    assert!(!supported_image_model("dreamina-image-"));
    assert!(!supported_image_model("seedance2.0"));
}

struct Fixture {
    _root: TempDir,
    executable: PathBuf,
    executable_sha256: [u8; 32],
    workspace: PathBuf,
    account_home: PathBuf,
    binding: DreaminaCliRuntimeBindingV1,
}

impl Fixture {
    fn new(action: &str) -> Self {
        let root = TempDir::new().unwrap();
        let workspace = root.path().join("workspace");
        let account_home = root.path().join("account-home");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&account_home).unwrap();
        fs::set_permissions(&workspace, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&account_home, fs::Permissions::from_mode(0o700)).unwrap();
        let executable = root.path().join("dreamina");
        let script = format!(
            r#"#!/bin/sh
download=
submit=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --submit_id) shift; submit=$1 ;;
    --download_dir) shift; download=$1 ;;
  esac
  shift
done
{action}
"#
        );
        fs::write(&executable, script.as_bytes()).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o500)).unwrap();
        let executable_sha256 = Sha256::digest(script.as_bytes()).into();
        let binding =
            DreaminaCliRuntimeBindingV1::new(Uuid::new_v4(), Uuid::new_v4(), "a".repeat(64))
                .unwrap();
        Self {
            _root: root,
            executable,
            executable_sha256,
            workspace,
            account_home,
            binding,
        }
    }

    fn policy(&self) -> DreaminaCliQueryPolicyV1 {
        DreaminaCliQueryPolicyV1::new(
            &self.executable,
            self.executable_sha256,
            WorkingDirectory::new(&self.workspace).unwrap(),
            WorkingDirectory::new(&self.account_home).unwrap(),
            Duration::from_secs(5),
            Duration::from_millis(50),
        )
        .unwrap()
    }

    fn driver(&self, max_artifact_bytes: u64) -> DreaminaCliPollDriverV1 {
        DreaminaCliPollDriverV1::new(self.policy(), self.binding.clone(), max_artifact_bytes)
            .unwrap()
    }

    fn process_config(&self, max_artifact_bytes: u64) -> DreaminaCliPollProcessConfig {
        DreaminaCliPollProcessConfig::new(
            &self.executable,
            self.executable_sha256,
            WorkingDirectory::new_private(&self.workspace).unwrap(),
            Duration::from_secs(5),
            Duration::from_millis(50),
            max_artifact_bytes,
        )
    }

    fn operation(&self) -> RemoteOperationRef {
        RemoteOperationRef::new(PROVIDER_ID, Uuid::new_v4().to_string(), "task-1").unwrap()
    }

    fn sink(&self, sha256: [u8; 32]) -> RecordingArtifactSink {
        RecordingArtifactSink::new(
            DurableArtifactRef::new("test", Uuid::new_v4().simple().to_string()).unwrap(),
            sha256,
        )
    }

    fn assert_workspace_has_no_attempts(&self) {
        assert!(workspace_has_no_attempts(&self.workspace));
    }
}

fn workspace_has_no_attempts(path: &Path) -> bool {
    fs::read_dir(path).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .as_encoded_bytes()
            .starts_with(DREAMINA_POLL_ATTEMPT_PREFIX.as_bytes())
    })
}

fn runtime_profile() -> ExecutorExecutionProfile {
    let operation = DREAMINA_IMAGE_GENERATION_OPERATION_V1;
    ExecutorExecutionProfile {
        execution_profile_id: Uuid::from_u128(1),
        profile_key: "dreamina-image-test".to_owned(),
        provider_id: PROVIDER_ID.to_owned(),
        command_schema: operation.command_schema.to_owned(),
        operation_id: operation.id.to_owned(),
        operation_descriptor_revision: operation.descriptor_revision.to_owned(),
        operation_descriptor_sha256_v1: operation.canonical_sha256_v1_hex(),
        completion_mode: operation.completion.as_str().to_owned(),
        idempotency_mode: operation.idempotency.as_str().to_owned(),
        adapter_revision: ADAPTER_REVISION.to_owned(),
        credential_pool_id: Uuid::from_u128(2),
        provider_account_id: Uuid::from_u128(3),
        credential_ref: "vault.dreamina.1".to_owned(),
        credential_revision: 1,
        credential_auth_sha256: "c".repeat(64),
        resource_policy_id: Uuid::from_u128(4),
        resource_policy_revision: 1,
        max_concurrency: 2,
    }
}

fn png_bytes(color: [u8; 4]) -> Vec<u8> {
    let image = DynamicImage::ImageRgba8(RgbaImage::from_pixel(8, 8, Rgba(color)));
    let mut bytes = Vec::new();
    image
        .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
        .unwrap();
    bytes
}

async fn wait_for_process_exit(pid: u32) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
            if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("query process exits after cancellation");
}
