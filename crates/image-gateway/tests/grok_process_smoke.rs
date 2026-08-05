use std::{
    fs,
    io::Cursor,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use gpt_image_2_gateway::{
    ProviderUploadService, ProxyConfig,
    admission::{XaiImageAdmissionPlan, XaiVideoAdmissionInput, XaiVideoAdmissionPlan},
    artifacts::InMemoryArtifactBlobStore,
    executor::{
        DurableRunner, DurableRunnerResult, ExecutorArtifactSink, ExecutorInputObject,
        ExecutorLaunchContext, ExecutorLaunchContextStore, ExecutorResultManifest,
        ExecutorSubmissionError, ExecutorSubmissionLease, GrokProcessSupervisor,
        JournaledDurableRunner, RunnerError, RunnerLaunchAuthority, RunnerOutcome,
        grok_auth_file_sha256,
    },
    input_blobs::{InputBlobKey, InputBlobStore},
    runner::FilesystemRunnerJournal,
};
use image_api_contracts::xai::{
    XaiImageAspectRatio, XaiImageGenerationRequest, XaiImageResolution, XaiImageResponseFormat,
    XaiVideoGenerationRequest, XaiVideoImageUrl, XaiVideoResolution,
};
use image_provider_grok_cli::{
    ADAPTER_REVISION, GROK_IMAGE_GENERATION_COMMAND_SCHEMA, GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
    VIDEO_ADAPTER_REVISION,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn detached_grok_runner_replays_sealed_output_without_a_second_cli_launch() {
    let temp = TempDir::new().unwrap();
    let credentials = private_credentials(temp.path());
    let invocations = temp.path().join("invocations");
    let fake_grok = fake_grok(temp.path(), &invocations);
    let helper = PathBuf::from(env!("CARGO_BIN_EXE_grok-runner"));
    let journal = Arc::new(
        FilesystemRunnerJournal::new(temp.path().join("runner-journal"))
            .expect("private runner journal"),
    );
    let (command, command_hash) = image_command();
    let lease = lease(command_hash);
    let context = ExecutorLaunchContext::new(
        "grok-process-smoke",
        image_api_contracts::xai::XAI_IMAGES_API_PROFILE,
        0,
        lease.command_schema.clone(),
        lease.command_hash.clone(),
        command,
    )
    .expect("valid launch context");
    let context_available = Arc::new(AtomicBool::new(true));
    let published = Arc::new(Mutex::new(Vec::new()));
    let publish_attempts = Arc::new(AtomicUsize::new(0));
    let supervisor = GrokProcessSupervisor::new(
        Arc::clone(&journal),
        helper,
        fake_grok,
        &credentials,
        &grok_auth_file_sha256(&credentials).unwrap(),
        Duration::from_secs(5),
        Duration::from_millis(10),
        Duration::from_secs(2),
        &ProxyConfig::default(),
    )
    .expect("valid Grok process supervisor");
    let runner = JournaledDurableRunner::new(
        GatedContextStore {
            context,
            available: Arc::clone(&context_available),
        },
        journal,
        supervisor,
        FailFirstArtifactSink {
            bytes: Arc::clone(&published),
            attempts: Arc::clone(&publish_attempts),
        },
    );

    let first = runner
        .start_or_attach(lease.clone(), RunnerLaunchAuthority::AllowLaunch)
        .await;
    assert!(
        matches!(first, DurableRunnerResult::Retryable { .. }),
        "{first:?}"
    );
    context_available.store(false, Ordering::SeqCst);
    let completed = runner
        .start_or_attach(lease.clone(), RunnerLaunchAuthority::AttachOnly)
        .await;
    let DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(manifest)) = &completed else {
        panic!("sealed Grok image outcome should succeed: {completed:?}");
    };
    let provider_cost = manifest
        .provider_reported_cost()
        .expect("Grok image cost evidence should survive process and journal replay");
    assert_eq!(provider_cost.observation().native_quantity, 200_000_000);
    assert_eq!(
        provider_cost.observation().provider_operation_id,
        "headless-1"
    );
    assert_eq!(
        runner
            .start_or_attach(lease, RunnerLaunchAuthority::AttachOnly)
            .await,
        completed
    );

    assert_eq!(fs::read_to_string(invocations).unwrap(), "1\n");
    assert_eq!(publish_attempts.load(Ordering::SeqCst), 2);
    let bytes = published.lock().unwrap().clone();
    let decoded = image::load_from_memory(&bytes).expect("sealed artifact must decode");
    assert_eq!((decoded.width(), decoded.height()), (1, 1));
}

#[tokio::test]
async fn detached_video_runner_stages_input_replays_mp4_and_cleans_cli_files() {
    let temp = TempDir::new().unwrap();
    let credentials = private_credentials(temp.path());
    let invocations = temp.path().join("video-invocations");
    let expected_video = minimal_mp4();
    let fake_grok = fake_grok_video(temp.path(), &invocations, &expected_video);
    let helper = PathBuf::from(env!("CARGO_BIN_EXE_grok-runner"));
    let journal_root = temp.path().join("video-runner-journal");
    let journal =
        Arc::new(FilesystemRunnerJournal::new(&journal_root).expect("private runner journal"));
    let blobs = Arc::new(InMemoryArtifactBlobStore::default());
    let input_bytes = jpeg();
    let blob = blobs
        .put(
            InputBlobKey {
                admission_session_id: Uuid::new_v4(),
                input_id: Uuid::new_v4(),
            },
            &input_bytes,
        )
        .await
        .unwrap();
    let plan = XaiVideoAdmissionPlan::for_grok_cli(
        XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some("data:image/jpeg;base64,AA==".to_owned()),
            }),
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: None,
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: Some("grok-video-smoke".to_owned()),
        },
        vec![XaiVideoAdmissionInput::new("input.jpg", blob.clone(), "image/jpeg").unwrap()],
    )
    .unwrap();
    assert_eq!(plan.output_count(), 1);
    assert_eq!(plan.billing_units(), 6);
    assert_eq!(plan.schedule_cost(), 6);
    let command = plan.command_json().clone();
    let command_hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
    let lease = video_lease(command_hash);
    let context = ExecutorLaunchContext::new(
        "grok-video-process-smoke",
        image_api_contracts::xai::XAI_VIDEOS_API_PROFILE,
        0,
        lease.command_schema.clone(),
        lease.command_hash.clone(),
        command,
    )
    .unwrap()
    .with_inputs(vec![
        ExecutorInputObject::new(blob, "image", 0, "image/jpeg").unwrap(),
    ])
    .unwrap();
    let available = Arc::new(AtomicBool::new(true));
    let published = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let provider_artifacts = temp.path().join("provider-artifacts");
    fs::create_dir(&provider_artifacts).unwrap();
    let provider_uploads = Arc::new(
        ProviderUploadService::new(&provider_artifacts, Some("http://127.0.0.1:8787")).unwrap(),
    );
    let supervisor = GrokProcessSupervisor::new(
        Arc::clone(&journal),
        helper,
        fake_grok,
        &credentials,
        &grok_auth_file_sha256(&credentials).unwrap(),
        Duration::from_secs(5),
        Duration::from_millis(10),
        Duration::from_secs(2),
        &ProxyConfig::default(),
    )
    .unwrap()
    .with_input_blobs(blobs)
    .with_local_video_uploads(provider_uploads);
    let runner = JournaledDurableRunner::new(
        GatedContextStore {
            context,
            available: available.clone(),
        },
        journal,
        supervisor,
        FailFirstArtifactSink {
            bytes: published.clone(),
            attempts: attempts.clone(),
        },
    );

    let first = runner
        .start_or_attach(lease.clone(), RunnerLaunchAuthority::AllowLaunch)
        .await;
    assert!(
        matches!(first, DurableRunnerResult::Retryable { .. }),
        "{first:?}"
    );
    available.store(false, Ordering::SeqCst);
    let completed = runner
        .start_or_attach(lease.clone(), RunnerLaunchAuthority::AttachOnly)
        .await;
    let DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(manifest)) = completed else {
        panic!("sealed Grok video outcome should succeed: {completed:?}");
    };
    let provider_cost = manifest
        .provider_reported_cost()
        .expect("Grok video cost evidence should survive process and journal replay");
    assert_eq!(provider_cost.observation().native_quantity, 300_000_000);
    assert_eq!(
        provider_cost.observation().provider_operation_id,
        "headless-video-1"
    );
    assert_eq!(fs::read_to_string(invocations).unwrap(), "1\n");
    assert_eq!(*published.lock().unwrap(), expected_video);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let execution_root = journal_root.join(lease.executor_execution_id.simple().to_string());
    assert_eq!(
        fs::read_dir(execution_root.join("provider-home"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        fs::read_dir(execution_root.join("runtime-home"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        fs::read_dir(execution_root.join("provider-workspaces/attempt"))
            .unwrap()
            .count(),
        0
    );
}

struct GatedContextStore {
    context: ExecutorLaunchContext,
    available: Arc<AtomicBool>,
}

#[async_trait]
impl ExecutorLaunchContextStore for GatedContextStore {
    async fn load_launch_context(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<ExecutorLaunchContext, ExecutorSubmissionError> {
        if self.available.load(Ordering::SeqCst)
            && self.context.command_schema() == lease.command_schema
            && self.context.command_hash() == lease.command_hash
            && self.context.output_index() == lease.output_index
        {
            Ok(self.context.clone())
        } else {
            Err(ExecutorSubmissionError::Unavailable)
        }
    }
}

struct FailFirstArtifactSink {
    bytes: Arc<Mutex<Vec<u8>>>,
    attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl ExecutorArtifactSink for FailFirstArtifactSink {
    async fn publish(
        &self,
        _lease: &ExecutorSubmissionLease,
        bytes: &[u8],
    ) -> Result<ExecutorResultManifest, RunnerError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(RunnerError::Unavailable);
        }
        *self.bytes.lock().map_err(|_| RunnerError::Internal)? = bytes.to_vec();
        ExecutorResultManifest::new(Uuid::new_v4(), Uuid::new_v4()).ok_or(RunnerError::Internal)
    }
}

fn private_credentials(root: &Path) -> PathBuf {
    let credentials = root.join("credentials");
    fs::create_dir(&credentials).unwrap();
    fs::set_permissions(&credentials, fs::Permissions::from_mode(0o700)).unwrap();
    let auth = credentials.join("auth.json");
    fs::write(&auth, b"{}").unwrap();
    fs::set_permissions(auth, fs::Permissions::from_mode(0o600)).unwrap();
    credentials
}

fn fake_grok(root: &Path, invocations: &Path) -> PathBuf {
    let image_path = root.join("source.jpg");
    let mut image_bytes = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut image_bytes, image::ImageFormat::Jpeg)
        .unwrap();
    fs::write(&image_path, image_bytes.into_inner()).unwrap();
    let executable = root.join("fake-grok");
    let script = format!(
        r#"#!/bin/sh
cwd=""
session=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --cwd) cwd="$2"; shift 2 ;;
    --session-id) session="$2"; shift 2 ;;
    *) shift ;;
  esac
done
/bin/cat >/dev/null
printf '1\n' >> '{}'
encoded=$(printf '%s' "$cwd" | /usr/bin/sed 's/%/%25/g; s|/|%2F|g')
session_dir="$GROK_HOME/sessions/$encoded/$session"
/bin/mkdir -p "$session_dir/images"
/bin/cp '{}' "$session_dir/images/1.jpg"
artifact="$session_dir/images/1.jpg"
printf '%s\n' '{{"type":"assistant","tool_calls":[{{"name":"image_gen","id":"call-1","arguments":"{{\"aspect_ratio\":\"1:1\",\"prompt\":\"draw a lighthouse\"}}"}}]}}' > "$session_dir/chat_history.jsonl"
printf '{{"type":"tool_result","tool_call_id":"call-1","content":"{{\\"path\\":\\"%s\\",\\"filename\\":\\"1.jpg\\",\\"session_folder\\":\\"images\\"}}"}}\n' "$artifact" >> "$session_dir/chat_history.jsonl"
printf '{{"type":"end","sessionId":"%s","requestId":"headless-1","stopReason":"end_turn","total_cost_usd_ticks":200000000}}\n' "$session"
"#,
        invocations.display(),
        image_path.display(),
    );
    fs::write(&executable, script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    executable
}

fn fake_grok_video(root: &Path, invocations: &Path, video: &[u8]) -> PathBuf {
    let video_path = root.join("source.mp4");
    fs::write(&video_path, video).unwrap();
    let executable = root.join("fake-grok-video");
    let script = format!(
        r#"#!/bin/sh
cwd=""
session=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --cwd) cwd="$2"; shift 2 ;;
    --session-id) session="$2"; shift 2 ;;
    *) shift ;;
  esac
done
/bin/cat >/dev/null
test -f "$cwd/input.jpg" || exit 71
printf '1\n' >> '{}'
encoded=$(printf '%s' "$cwd" | /usr/bin/sed 's/%/%25/g; s|/|%2F|g')
session_dir="$GROK_HOME/sessions/$encoded/$session"
/bin/mkdir -p "$session_dir/videos"
/bin/cp '{}' "$session_dir/videos/1.mp4"
artifact="$session_dir/videos/1.mp4"
printf '{{"type":"assistant","tool_calls":[{{"name":"image_to_video","id":"call-1","arguments":"{{\\"duration\\":6,\\"image\\":\\"%s/input.jpg\\",\\"prompt\\":null,\\"resolution_name\\":\\"480p\\"}}"}}]}}\n' "$cwd" > "$session_dir/chat_history.jsonl"
printf '{{"type":"tool_result","tool_call_id":"call-1","content":"{{\\"path\\":\\"%s\\",\\"filename\\":\\"1.mp4\\",\\"session_folder\\":\\"videos\\"}}"}}\n' "$artifact" >> "$session_dir/chat_history.jsonl"
printf '{{"type":"end","sessionId":"%s","requestId":"headless-video-1","stopReason":"end_turn","total_cost_usd_ticks":300000000}}\n' "$session"
"#,
        invocations.display(),
        video_path.display(),
    );
    fs::write(&executable, script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    executable
}

fn image_command() -> (Value, String) {
    let plan = XaiImageAdmissionPlan::for_grok_cli(XaiImageGenerationRequest {
        aspect_ratio: Some(XaiImageAspectRatio::R1x1),
        model: Some("grok-imagine-image-quality".to_owned()),
        n: Some(1),
        prompt: "draw a lighthouse".to_owned(),
        resolution: Some(XaiImageResolution::R1k),
        response_format: Some(XaiImageResponseFormat::B64Json),
        storage_options: None,
        user: Some("grok-process-smoke".to_owned()),
    })
    .unwrap();
    let command = plan.command_json().clone();
    let command_hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
    (command, command_hash)
}

fn lease(command_hash: String) -> ExecutorSubmissionLease {
    ExecutorSubmissionLease {
        submission_id: Uuid::new_v4(),
        executor_execution_id: Uuid::new_v4(),
        output_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        tenant_id: "grok-process-smoke".to_owned(),
        provider_id: image_provider_grok_cli::PROVIDER_ID.to_owned(),
        model: "grok-imagine-image-quality".to_owned(),
        work_item_id: Uuid::new_v4(),
        output_index: 0,
        command_schema: GROK_IMAGE_GENERATION_COMMAND_SCHEMA.to_owned(),
        command_hash,
        execution_profile_id: Uuid::new_v4(),
        adapter_revision: ADAPTER_REVISION.to_owned(),
        executor_owner: "grok-process-smoke".to_owned(),
        executor_lease_epoch: 1,
        executor_lease_expires_at_ms: i64::MAX,
    }
}

fn video_lease(command_hash: String) -> ExecutorSubmissionLease {
    ExecutorSubmissionLease {
        model: "grok-imagine-video-1.5-preview".to_owned(),
        command_schema: GROK_VIDEO_GENERATION_COMMAND_SCHEMA.to_owned(),
        adapter_revision: VIDEO_ADAPTER_REVISION.to_owned(),
        ..lease(command_hash)
    }
}

fn jpeg() -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(2, 2)
        .write_to(&mut bytes, image::ImageFormat::Jpeg)
        .unwrap();
    bytes.into_inner()
}

fn minimal_mp4() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&24_u32.to_be_bytes());
    bytes.extend_from_slice(b"ftypisom\0\0\0\0isommp42");
    bytes.extend_from_slice(&16_u32.to_be_bytes());
    bytes.extend_from_slice(b"moov");
    bytes.extend_from_slice(&8_u32.to_be_bytes());
    bytes.extend_from_slice(b"free");
    bytes.extend_from_slice(&9_u32.to_be_bytes());
    bytes.extend_from_slice(b"mdat\0");
    bytes
}
