use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use image_api_contracts::xai::{
    XaiImageAspectRatio, XaiImageGenerationRequest, XaiImageResolution, XaiImageResponseFormat,
    XaiVideoGenerationRequest, XaiVideoImageUrl, XaiVideoResolution,
};
use image_provider_grok_cli::{
    ADAPTER_REVISION, GROK_IMAGE_GENERATION_COMMAND_SCHEMA, GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2, VIDEO_ADAPTER_REVISION, VIDEO_ADAPTER_REVISION_V2,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::admission::{XaiImageAdmissionPlan, XaiVideoAdmissionInput, XaiVideoAdmissionPlan};
use crate::artifacts::{InMemoryArtifactBlobStore, media_type_from_bytes};
use crate::executor::{
    DurableRunner, DurableRunnerResult, ExecutorArtifactSink, ExecutorInputObject,
    ExecutorLaunchContextStore, ExecutorResultManifest, ExecutorSubmissionError,
    JournaledDurableRunner, RunnerError, RunnerLaunchAuthority, RunnerOutcome,
    XAI_IMAGES_API_PROFILE,
};
use crate::input_blobs::{InputBlobKey, InputBlobStore};

#[tokio::test]
#[ignore = "runs a real Grok CLI image generation and consumes membership allowance"]
async fn xai_generation_runs_through_the_real_durable_grok_supervisor() {
    let source_home = env::var("GROK_SMOKE_CREDENTIAL_HOME")
        .expect("GROK_SMOKE_CREDENTIAL_HOME must explicitly select the logged-in Grok home");
    let grok_executable = env::var("GROK_SMOKE_EXECUTABLE")
        .expect("GROK_SMOKE_EXECUTABLE must explicitly select the Grok CLI executable");
    let helper_executable = env::var("GROK_SMOKE_HELPER_EXECUTABLE")
        .expect("GROK_SMOKE_HELPER_EXECUTABLE must select the built grok-runner binary");
    let temp = TempDir::new().unwrap();
    let credentials = private_credentials(temp.path(), Path::new(&source_home));
    let journal = Arc::new(
        FilesystemRunnerJournal::new(temp.path().join("runner-journal"))
            .expect("private runner journal"),
    );
    let (command, command_hash) = image_command();
    let lease = lease(command_hash);
    let context = ExecutorLaunchContext {
        request_id: "grok-live-smoke".to_owned(),
        api_profile: XAI_IMAGES_API_PROFILE.to_owned(),
        output_index: 0,
        command_schema: lease.command_schema.clone(),
        command_hash: lease.command_hash.clone(),
        command_json: command,
        inputs: Vec::new(),
    };
    let credential_digest = grok_auth_file_sha256(&credentials).unwrap();
    let supervisor = GrokProcessSupervisor::new(
        Arc::clone(&journal),
        &helper_executable,
        &grok_executable,
        &credentials,
        &credential_digest,
        Duration::from_secs(15 * 60),
        Duration::from_millis(50),
        Duration::from_secs(5),
        &ProxyConfig::default(),
    )
    .expect("valid live Grok supervisor");
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let runner = JournaledDurableRunner::new(
        LiveContextStore(context),
        Arc::clone(&journal),
        supervisor,
        LiveArtifactSink(Arc::clone(&bytes)),
    );
    let first = runner
        .start_or_attach(lease.clone(), RunnerLaunchAuthority::AllowLaunch)
        .await;
    assert!(matches!(
        first,
        DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(_))
    ));
    let replay = runner
        .start_or_attach(lease, RunnerLaunchAuthority::AttachOnly)
        .await;
    assert_eq!(replay, first);
    let bytes = bytes.lock().unwrap().clone();
    let decoded = image::load_from_memory(&bytes).expect("Grok artifact decodes as an image");
    assert!(decoded.width() > 0 && decoded.height() > 0);
}

#[tokio::test]
#[ignore = "runs one real 6-second 480p Grok CLI video generation and consumes membership allowance"]
async fn xai_image_to_video_runs_through_the_real_durable_grok_supervisor() {
    let source_home = env::var("GROK_SMOKE_CREDENTIAL_HOME")
        .expect("GROK_SMOKE_CREDENTIAL_HOME must explicitly select the logged-in Grok home");
    let grok_executable = env::var("GROK_SMOKE_EXECUTABLE")
        .expect("GROK_SMOKE_EXECUTABLE must explicitly select the Grok CLI executable");
    let helper_executable = env::var("GROK_SMOKE_HELPER_EXECUTABLE")
        .expect("GROK_SMOKE_HELPER_EXECUTABLE must select the built grok-runner binary");
    let temp = TempDir::new().unwrap();
    let credentials = private_credentials(temp.path(), Path::new(&source_home));
    let journal_root = temp.path().join("video-runner-journal");
    let journal =
        Arc::new(FilesystemRunnerJournal::new(&journal_root).expect("private runner journal"));
    let blobs = Arc::new(InMemoryArtifactBlobStore::default());
    let input_bytes = video_source_image();
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
            generate_audio: None,
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some(format!(
                    "data:image/jpeg;base64,{}",
                    STANDARD.encode(&input_bytes)
                )),
            }),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: Some(
                "A slow cinematic push-in; the blue square gently rotates while the white background remains still"
                    .to_owned(),
            ),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: Some("grok-durable-video-smoke".to_owned()),
        },
        vec![XaiVideoAdmissionInput::new(
            "input.jpg",
            blob.clone(),
            "image/jpeg",
        )
        .unwrap()],
    )
    .unwrap();
    assert_eq!(plan.billing_units(), 6);
    assert_eq!(plan.schedule_cost(), 6);
    let command = plan.command_json().clone();
    let command_hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
    let lease = video_lease(command_hash);
    let context = ExecutorLaunchContext::new(
        "grok-live-video-smoke",
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
    let supervisor = GrokProcessSupervisor::new(
        Arc::clone(&journal),
        &helper_executable,
        &grok_executable,
        &credentials,
        &grok_auth_file_sha256(&credentials).unwrap(),
        Duration::from_secs(15 * 60),
        Duration::from_millis(100),
        Duration::from_secs(5),
        &ProxyConfig::default(),
    )
    .unwrap()
    .with_input_blobs(blobs)
    .with_local_video_uploads(live_video_uploads().1);
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let runner = JournaledDurableRunner::new(
        LiveContextStore(context),
        journal,
        supervisor,
        LiveArtifactSink(bytes.clone()),
    );

    let first = runner
        .start_or_attach(lease.clone(), RunnerLaunchAuthority::AllowLaunch)
        .await;
    assert!(
        matches!(
            first,
            DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(_))
        ),
        "unexpected Grok video outcome: {first:?}"
    );
    assert_eq!(
        runner
            .start_or_attach(lease.clone(), RunnerLaunchAuthority::AttachOnly)
            .await,
        first
    );
    let bytes = bytes.lock().unwrap().clone();
    assert_eq!(media_type_from_bytes(&bytes).unwrap(), "video/mp4");
    eprintln!(
        "verified Grok video: {} bytes sha256={}",
        bytes.len(),
        sha256(&bytes)
    );
    let execution_root = journal_root.join(lease.executor_execution_id.simple().to_string());
    assert_absent_or_empty(&execution_root.join("provider-home"));
    assert_absent_or_empty(&execution_root.join("provider-workspaces/attempt"));
}

#[tokio::test]
#[ignore = "runs one real 6-second 480p Grok CLI 1.0.34 V2 video generation and consumes membership allowance"]
async fn xai_image_to_video_v2_runs_through_the_real_durable_grok_supervisor() {
    let source_home = env::var("GROK_SMOKE_CREDENTIAL_HOME")
        .expect("GROK_SMOKE_CREDENTIAL_HOME must explicitly select the logged-in Grok home");
    let grok_executable = env::var("GROK_SMOKE_EXECUTABLE")
        .expect("GROK_SMOKE_EXECUTABLE must explicitly select the Grok CLI executable");
    let helper_executable = env::var("GROK_SMOKE_HELPER_EXECUTABLE")
        .expect("GROK_SMOKE_HELPER_EXECUTABLE must select the built grok-runner binary");
    let temp = TempDir::new().unwrap();
    let credentials = private_credentials(temp.path(), Path::new(&source_home));
    // Keep the journal outside the credential TempDir so a real-provider
    // failure leaves an inspectable, auth-free path for diagnosis.
    let journal_root =
        env::temp_dir().join(format!("grok-v2-live-smoke-{}", Uuid::new_v4().simple()));
    let journal =
        Arc::new(FilesystemRunnerJournal::new(&journal_root).expect("private runner journal"));
    let blobs = Arc::new(InMemoryArtifactBlobStore::default());
    let input_bytes = video_source_image();
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
    let plan = XaiVideoAdmissionPlan::for_grok_cli_v2(
        XaiVideoGenerationRequest {
            aspect_ratio: None,
            duration: Some(6),
            generate_audio: Some(true),
            image: Some(XaiVideoImageUrl {
                file_id: None,
                url: Some(format!(
                    "data:image/jpeg;base64,{}",
                    STANDARD.encode(&input_bytes)
                )),
            }),
            last_frame: None,
            model: Some("grok-imagine-video-1.5".to_owned()),
            output: None,
            prompt: Some(
                "A slow cinematic push-in; the blue square gently rotates while the white background remains still"
                    .to_owned(),
            ),
            reference_audios: Vec::new(),
            reference_images: Vec::new(),
            resolution: Some(XaiVideoResolution::P480),
            storage_options: None,
            user: Some("grok-durable-video-v2-smoke".to_owned()),
        },
        vec![XaiVideoAdmissionInput::new(
            "input.jpg",
            blob.clone(),
            "image/jpeg",
        )
        .unwrap()],
    )
    .unwrap();
    assert_eq!(plan.provider_model(), "grok-imagine-video-1.5");
    assert_eq!(
        plan.command_schema(),
        GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2
    );
    assert_eq!(plan.adapter_revision(), VIDEO_ADAPTER_REVISION_V2);
    assert_eq!(plan.billing_units(), 6);
    assert_eq!(plan.schedule_cost(), 6);
    let command = plan.command_json().clone();
    let command_hash = hex::encode(Sha256::digest(serde_json::to_vec(&command).unwrap()));
    let lease = video_lease_v2(command_hash);
    let (provider_artifact_root, local_video_uploads) = live_video_uploads();
    let provider_output = local_video_uploads
        .issue_grok_video_output(&lease, Duration::from_secs(15 * 60))
        .expect("shared Gateway provider upload service must issue a video output ticket")
        .expect("shared Gateway provider upload service must expose a video output ticket");
    let context = ExecutorLaunchContext::new(
        "grok-live-video-v2-smoke",
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
    let supervisor = GrokProcessSupervisor::new(
        Arc::clone(&journal),
        &helper_executable,
        &grok_executable,
        &credentials,
        &grok_auth_file_sha256(&credentials).unwrap(),
        Duration::from_secs(15 * 60),
        Duration::from_millis(100),
        Duration::from_secs(5),
        &ProxyConfig::default(),
    )
    .unwrap()
    .with_input_blobs(blobs)
    .with_local_video_uploads(Arc::clone(&local_video_uploads));
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let runner = JournaledDurableRunner::new(
        LiveContextStore(context),
        journal,
        supervisor,
        LiveArtifactSink(bytes.clone()),
    );

    let started = Instant::now();
    let first = runner
        .start_or_attach(lease.clone(), RunnerLaunchAuthority::AllowLaunch)
        .await;
    let elapsed_ms = started.elapsed().as_millis();
    if !matches!(
        first,
        DurableRunnerResult::Terminal(RunnerOutcome::Succeeded(_))
    ) {
        panic!(
            "unexpected Grok V2 video outcome after {elapsed_ms}ms: {first:?}; journal={}",
            journal_root.display()
        );
    }
    let provider_ticket_root = provider_artifact_root
        .join(".provider-uploads")
        .join(&provider_output.access_key_id);
    let provider_object = provider_ticket_root.join("object.mp4");
    assert!(
        provider_object.is_file(),
        "shared Gateway provider upload object is missing"
    );
    let provider_entries_before_replay = directory_entries(&provider_ticket_root);
    assert_eq!(
        runner
            .start_or_attach(lease.clone(), RunnerLaunchAuthority::AttachOnly)
            .await,
        first
    );
    assert_eq!(
        provider_entries_before_replay,
        directory_entries(&provider_ticket_root),
        "Grok V2 replay must not create another provider object"
    );
    let bytes = bytes.lock().unwrap().clone();
    assert_eq!(media_type_from_bytes(&bytes).unwrap(), "video/mp4");
    assert!(!bytes.is_empty());
    let uploaded = fs::read(&provider_object)
        .expect("shared Gateway provider upload object must be readable by the smoke UID");
    assert_eq!(uploaded.len(), bytes.len());
    assert_eq!(sha256(&uploaded), sha256(&bytes));
    eprintln!(
        "verified Grok V2 video first execution: elapsed_ms={} bytes={} sha256={}",
        elapsed_ms,
        bytes.len(),
        sha256(&bytes)
    );
    let execution_root = journal_root.join(lease.executor_execution_id.simple().to_string());
    assert_absent_or_empty(&execution_root.join("provider-home"));
    assert_absent_or_empty(&execution_root.join("provider-workspaces/attempt"));
    fs::remove_dir_all(&journal_root).expect("remove successful V2 smoke journal");
}

fn assert_absent_or_empty(path: &Path) {
    match fs::read_dir(path) {
        Ok(entries) => assert_eq!(entries.count(), 0, "{} is not empty", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to inspect {}: {error}", path.display()),
    }
}

fn live_video_uploads() -> (PathBuf, Arc<ProviderUploadService>) {
    let artifact_root = env::var("GROK_SMOKE_PROVIDER_UPLOAD_ARTIFACT_ROOT")
        .expect("GROK_SMOKE_PROVIDER_UPLOAD_ARTIFACT_ROOT must select the existing artifact root shared with the running Gateway and the same Unix UID");
    let public_base_url = env::var("GROK_SMOKE_PROVIDER_UPLOAD_PUBLIC_BASE_URL")
        .expect("GROK_SMOKE_PROVIDER_UPLOAD_PUBLIC_BASE_URL must select the publicly reachable HTTPS Gateway origin");
    let parsed_url = reqwest::Url::parse(public_base_url.trim())
        .expect("GROK_SMOKE_PROVIDER_UPLOAD_PUBLIC_BASE_URL must be a valid HTTPS Gateway origin");
    assert_eq!(
        parsed_url.scheme(),
        "https",
        "GROK_SMOKE_PROVIDER_UPLOAD_PUBLIC_BASE_URL must use HTTPS for the running public Gateway"
    );
    let artifact_root = fs::canonicalize(&artifact_root).expect(
        "GROK_SMOKE_PROVIDER_UPLOAD_ARTIFACT_ROOT must point to an existing Gateway artifact root",
    );
    let uploads = ProviderUploadService::new(&artifact_root, Some(public_base_url.trim()))
        .expect("shared Gateway provider upload artifact root and HTTPS public origin are invalid");
    (artifact_root, Arc::new(uploads))
}

fn directory_entries(path: &Path) -> Vec<String> {
    let mut entries = fs::read_dir(path)
        .expect("shared Gateway provider upload ticket directory must be readable")
        .map(|entry| {
            entry
                .expect("shared Gateway provider upload ticket entry must be readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    entries.sort_unstable();
    entries
}

struct LiveContextStore(ExecutorLaunchContext);

#[async_trait]
impl ExecutorLaunchContextStore for LiveContextStore {
    async fn load_launch_context(
        &self,
        lease: &ExecutorSubmissionLease,
    ) -> Result<ExecutorLaunchContext, ExecutorSubmissionError> {
        if self.0.command_schema() == lease.command_schema
            && self.0.command_hash() == lease.command_hash
            && self.0.output_index() == lease.output_index
        {
            Ok(self.0.clone())
        } else {
            Err(ExecutorSubmissionError::Conflict)
        }
    }
}

struct LiveArtifactSink(Arc<Mutex<Vec<u8>>>);

#[async_trait]
impl ExecutorArtifactSink for LiveArtifactSink {
    async fn publish(
        &self,
        _lease: &ExecutorSubmissionLease,
        bytes: &[u8],
    ) -> Result<ExecutorResultManifest, RunnerError> {
        *self.0.lock().map_err(|_| RunnerError::Internal)? = bytes.to_vec();
        ExecutorResultManifest::new(Uuid::new_v4(), Uuid::new_v4()).ok_or(RunnerError::Internal)
    }
}

fn private_credentials(root: &Path, source_home: &Path) -> PathBuf {
    let credentials = root.join("credentials");
    fs::create_dir(&credentials).unwrap();
    fs::set_permissions(&credentials, fs::Permissions::from_mode(0o700)).unwrap();
    let source = source_home.join(private_auth::AUTH_FILE);
    let target = credentials.join(private_auth::AUTH_FILE);
    fs::copy(source, &target).expect("copy explicit Grok smoke credentials");
    fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
    credentials
}

fn image_command() -> (Value, String) {
    let plan = XaiImageAdmissionPlan::for_grok_cli(XaiImageGenerationRequest {
        aspect_ratio: Some(XaiImageAspectRatio::R1x1),
        model: Some("grok-imagine-image-quality".to_owned()),
        n: Some(1),
        prompt: "smoke test: one solid cobalt-blue square centered on a white background"
            .to_owned(),
        resolution: Some(XaiImageResolution::R1k),
        response_format: Some(XaiImageResponseFormat::B64Json),
        storage_options: None,
        user: Some("grok-durable-supervisor-smoke".to_owned()),
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
        tenant_id: "grok-live-smoke".to_owned(),
        provider_id: image_provider_grok_cli::PROVIDER_ID.to_owned(),
        model: "grok-imagine-image-quality".to_owned(),
        work_item_id: Uuid::new_v4(),
        output_index: 0,
        command_schema: GROK_IMAGE_GENERATION_COMMAND_SCHEMA.to_owned(),
        command_hash,
        execution_profile_id: Uuid::new_v4(),
        adapter_revision: ADAPTER_REVISION.to_owned(),
        executor_owner: "grok-live-smoke".to_owned(),
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

fn video_lease_v2(command_hash: String) -> ExecutorSubmissionLease {
    ExecutorSubmissionLease {
        model: "grok-imagine-video-1.5".to_owned(),
        command_schema: GROK_VIDEO_GENERATION_COMMAND_SCHEMA_V2.to_owned(),
        adapter_revision: VIDEO_ADAPTER_REVISION_V2.to_owned(),
        ..lease(command_hash)
    }
}

fn video_source_image() -> Vec<u8> {
    let mut image = image::RgbImage::from_pixel(512, 512, image::Rgb([250, 250, 250]));
    for y in 156..356 {
        for x in 156..356 {
            image.put_pixel(x, y, image::Rgb([20, 75, 200]));
        }
    }
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(image)
        .write_to(&mut bytes, image::ImageFormat::Jpeg)
        .unwrap();
    bytes.into_inner()
}
