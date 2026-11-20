use std::time::Duration;

use image_provider_dreamina_cli::{StagedImageV1, StagedInputKindV1, StagedTimedMediaV1};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    ImageGatewayError,
    admission::{
        AdmissionTicket, AttachInputObject, DreaminaStagedInputs, DreaminaWorkflowAdmissionPlan,
        DreaminaWorkflowIntent, EditInputRoleV1,
    },
    input_blobs::{InputBlobKey, InputBlobStore},
    media_inputs::{MediaInputError, MediaInputKind, probe_input},
};

use super::{dreamina::dreamina_admission_error, video_inputs::decode_media_source};

// An application resource bound, independent of the upstream per-model input count.
const MAX_TOTAL_INPUT_BYTES: usize = 256 * 1024 * 1024;
const ADMISSION_CLEANUP_MARGIN_MS: i64 = 5_000;

pub(super) fn remaining_preparation_budget(admission_deadline_ms: i64) -> Duration {
    let now_ms = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    let now_ms = i64::try_from(now_ms).unwrap_or(i64::MAX);
    let remaining_ms = admission_deadline_ms
        .saturating_sub(now_ms)
        .saturating_sub(ADMISSION_CLEANUP_MARGIN_MS);
    Duration::from_millis(u64::try_from(remaining_ms).unwrap_or_default())
}

/// Download, probe and persist one input at a time. Recovery uses these frozen objects.
pub(super) async fn stage_workflow_inputs(
    store: &dyn InputBlobStore,
    ticket: &AdmissionTicket,
    intent: DreaminaWorkflowIntent,
    max_upload_bytes: usize,
    budget: Duration,
) -> Result<DreaminaWorkflowAdmissionPlan, ImageGatewayError> {
    // Individual downloads retain their own shorter timeout and single read retry.
    // The aggregate budget also covers decoding and persisting all inputs.
    let started = Instant::now();
    let result = if budget.is_zero() {
        Err(preparation_timed_out())
    } else {
        match tokio::time::timeout(
            budget,
            stage_inputs(store, ticket, intent, max_upload_bytes),
        )
        .await
        {
            Ok(result) if started.elapsed() < budget => result,
            _ => Err(preparation_timed_out()),
        }
    };
    if result.is_err() {
        // Cleanup must remain outside the expired staging future. If the client
        // disconnects, the existing admission reconciler also cleans this session.
        store.delete_session(ticket.session_id).await.map_err(|_| {
            ImageGatewayError::service_unavailable("Dreamina input cleanup is unavailable")
        })?;
    }
    result
}

fn preparation_timed_out() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "Dreamina reference material preparation exceeded the time limit",
        None,
        "invalid_image_url",
    )
}

async fn stage_inputs(
    store: &dyn InputBlobStore,
    ticket: &AdmissionTicket,
    intent: DreaminaWorkflowIntent,
    max_upload_bytes: usize,
) -> Result<DreaminaWorkflowAdmissionPlan, ImageGatewayError> {
    let total_limit = max_upload_bytes.min(MAX_TOTAL_INPUT_BYTES);
    let mut total_bytes = 0;
    let mut staged = DreaminaStagedInputs::default();
    for (index, source) in intent.sources().iter().enumerate() {
        let (kind, role, media_types): (_, _, &[&str]) = match source.kind {
            StagedInputKindV1::Image => (
                MediaInputKind::Image,
                EditInputRoleV1::Image,
                &["image/png", "image/jpeg", "image/webp"],
            ),
            StagedInputKindV1::Video => (
                MediaInputKind::Video,
                EditInputRoleV1::Video,
                &["video/mp4"],
            ),
            StagedInputKindV1::Audio => (
                MediaInputKind::Audio,
                EditInputRoleV1::Audio,
                &["audio/mpeg", "audio/wav", "audio/x-wav"],
            ),
        };
        let decoded = decode_media_source(
            source.parameter,
            &source.url,
            (kind.max_bytes() as usize).min(total_limit),
            total_limit,
            media_types,
            &mut total_bytes,
        )
        .await?;
        let declared_type = decoded.media_type;
        let (bytes, metadata, sha256) = tokio::task::spawn_blocking(move || {
            let metadata = probe_input(&decoded.bytes, kind)?;
            let sha256 = hex::encode(Sha256::digest(&decoded.bytes));
            Ok::<_, MediaInputError>((decoded.bytes, metadata, sha256))
        })
        .await
        .map_err(|_| ImageGatewayError::service_unavailable("Dreamina input probe unavailable"))?
        .map_err(|error| media_error(error, source.parameter))?;
        let expected_type = if declared_type == "audio/x-wav" {
            "audio/wav"
        } else {
            &declared_type
        };
        if metadata.media_type() != expected_type {
            return Err(ImageGatewayError::invalid_request(
                "input content does not match its declared media type",
                Some(source.parameter.to_owned()),
                "invalid_media",
            ));
        }
        let extension = match metadata.media_type() {
            "image/png" => "png",
            "image/jpeg" => "jpg",
            "image/webp" => "webp",
            "video/mp4" => "mp4",
            "audio/mpeg" => "mp3",
            "audio/wav" => "wav",
            _ => return Err(media_error(MediaInputError::Unsupported, source.parameter)),
        };
        let filename = format!("input-{index}.{extension}");
        match kind {
            MediaInputKind::Image => {
                let (width, height) = metadata
                    .dimensions()
                    .ok_or_else(|| media_error(MediaInputError::Invalid, source.parameter))?;
                staged.images.push(
                    StagedImageV1::new(filename, sha256.clone(), width, height)
                        .map_err(|_| media_error(MediaInputError::Invalid, source.parameter))?,
                );
            }
            MediaInputKind::Video | MediaInputKind::Audio => {
                let duration = metadata
                    .duration_ms()
                    .ok_or_else(|| media_error(MediaInputError::Invalid, source.parameter))?;
                let input = StagedTimedMediaV1::new(filename, sha256.clone(), duration)
                    .map_err(|_| media_error(MediaInputError::Invalid, source.parameter))?;
                if kind == MediaInputKind::Video {
                    staged.videos.push(input);
                } else {
                    staged.audios.push(input);
                }
            }
        }
        let byte_size = bytes.len() as u64;
        let blob = store
            .put(
                InputBlobKey {
                    admission_session_id: ticket.session_id,
                    input_id: Uuid::new_v4(),
                },
                &bytes,
            )
            .await
            .map_err(|_| {
                ImageGatewayError::service_unavailable("Dreamina input storage unavailable")
            })?;
        if blob.key.admission_session_id != ticket.session_id
            || blob.sha256_hex != sha256
            || blob.byte_size != byte_size
        {
            return Err(ImageGatewayError::service_unavailable(
                "Dreamina input storage integrity check failed",
            ));
        }
        staged.objects.push(AttachInputObject {
            blob,
            role,
            index: index as u16,
            media_type: metadata.media_type().to_owned(),
        });
    }
    intent.bind(staged).map_err(dreamina_admission_error)
}

fn media_error(error: MediaInputError, parameter: &str) -> ImageGatewayError {
    match error {
        MediaInputError::LimitExceeded => {
            ImageGatewayError::payload_too_large("Dreamina input exceeds the media resource limit")
        }
        MediaInputError::Invalid | MediaInputError::Unsupported => {
            ImageGatewayError::invalid_request(
                match error {
                    MediaInputError::Unsupported => "supported inputs are PNG/JPEG/WebP images, non-fragmented MP4 with H264/AVC video, and MP3 or PCM WAV audio".to_owned(),
                    _ => error.to_string(),
                },
                Some(parameter.to_owned()),
                "invalid_media",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::*;
    use crate::{
        artifacts::InMemoryArtifactBlobStore,
        input_blobs::{
            InputBlobDeleteError, InputBlobReadError, InputBlobRef, InputBlobWriteError,
        },
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    #[derive(Default)]
    struct SecondWriteStalls {
        inner: InMemoryArtifactBlobStore,
        started: AtomicUsize,
        first: Mutex<Option<InputBlobRef>>,
    }

    #[async_trait::async_trait]
    impl InputBlobStore for SecondWriteStalls {
        async fn put(
            &self,
            key: InputBlobKey,
            bytes: &[u8],
        ) -> Result<InputBlobRef, InputBlobWriteError> {
            if self.started.fetch_add(1, Ordering::Relaxed) != 0 {
                return pending().await;
            }
            let blob = self.inner.put(key, bytes).await?;
            *self.first.lock().unwrap() = Some(blob.clone());
            Ok(blob)
        }

        async fn get(&self, blob: &InputBlobRef) -> Result<Vec<u8>, InputBlobReadError> {
            self.inner.get(blob).await
        }

        async fn delete(&self, blob: &InputBlobRef) -> Result<(), InputBlobDeleteError> {
            self.inner.delete(blob).await
        }

        async fn delete_session(&self, session_id: Uuid) -> Result<(), InputBlobDeleteError> {
            self.inner.delete_session(session_id).await
        }
    }

    fn image_source() -> (String, Vec<u8>) {
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(32, 18)
            .write_to(&mut bytes, image::ImageFormat::Png)
            .unwrap();
        let bytes = bytes.into_inner();
        (
            format!("data:image/png;base64,{}", STANDARD.encode(&bytes)),
            bytes,
        )
    }

    fn video_intent(first: String, last: String) -> DreaminaWorkflowIntent {
        DreaminaWorkflowIntent::video(
            serde_json::from_value(serde_json::json!({
                "workflow":"frames2video", "first":first, "last":last, "prompt":"move",
                "model_version":"seedance2.5", "video_resolution":"480p"
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn ticket() -> AdmissionTicket {
        AdmissionTicket {
            session_id: Uuid::new_v4(),
            owner_token: Uuid::new_v4(),
            request_hash: "a".repeat(64),
        }
    }

    #[tokio::test]
    async fn preparation_probes_real_dimensions_freezes_sha_and_keeps_two_logical_frames() {
        let store = InMemoryArtifactBlobStore::default();
        let ticket = ticket();
        let (source, bytes) = image_source();
        let plan = stage_workflow_inputs(
            &store,
            &ticket,
            video_intent(source.clone(), source),
            1024 * 1024,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        let descriptors = plan.typed_request().staged_inputs();
        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].dimensions, Some((32, 18)));
        assert_eq!(descriptors[0].sha256, hex::encode(Sha256::digest(&bytes)));
        assert_eq!(descriptors[0].sha256, descriptors[1].sha256);
        assert_ne!(descriptors[0].filename, descriptors[1].filename);
        let attach = plan.attach(ticket, Uuid::new_v4(), "test");
        let manifest = attach.input_manifest.unwrap();
        assert_eq!(manifest.inputs.len(), 2);
        assert_ne!(
            manifest.inputs[0].blob.key.input_id,
            manifest.inputs[1].blob.key.input_id
        );
        for input in manifest.inputs {
            assert_eq!(store.get(&input.blob).await.unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn preparation_rejects_wrong_mime_local_paths_and_cleans_session_inputs() {
        let (source, _) = image_source();
        for invalid in [
            source.replace("image/png", "image/jpeg"),
            "file:///tmp/a.png".to_owned(),
            "https://127.0.0.1/a.png".to_owned(),
            "data:image/png;base64,aW52YWxpZA==".to_owned(),
        ] {
            let store = InMemoryArtifactBlobStore::default();
            let ticket = ticket();
            let previous = store
                .put(
                    InputBlobKey {
                        admission_session_id: ticket.session_id,
                        input_id: Uuid::new_v4(),
                    },
                    b"previous staged input",
                )
                .await
                .unwrap();
            assert!(
                stage_workflow_inputs(
                    &store,
                    &ticket,
                    video_intent(source.clone(), invalid),
                    1024 * 1024,
                    Duration::from_secs(5),
                )
                .await
                .is_err()
            );
            assert!(store.get(&previous).await.is_err());
        }
    }

    #[tokio::test]
    async fn preparation_checks_total_size_across_inputs() {
        let store = InMemoryArtifactBlobStore::default();
        let (source, bytes) = image_source();
        assert!(
            stage_workflow_inputs(
                &store,
                &ticket(),
                video_intent(source.clone(), source),
                bytes.len() + 4,
                Duration::from_secs(5),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn aggregate_timeout_cleans_partial_staging_and_never_yields_a_plan() {
        let store = SecondWriteStalls::default();
        let ticket = ticket();
        let (source, bytes) = image_source();
        let unrelated = store
            .inner
            .put(
                InputBlobKey {
                    admission_session_id: Uuid::new_v4(),
                    input_id: Uuid::new_v4(),
                },
                &bytes,
            )
            .await
            .unwrap();
        let result = stage_workflow_inputs(
            &store,
            &ticket,
            video_intent(source.clone(), source),
            1024 * 1024,
            Duration::from_secs(1),
        )
        .await;
        let error = result.err().expect("stalled input cannot be attached");
        assert_eq!(error.status_code().as_u16(), 400);
        assert_eq!(error.error_code(), Some("invalid_image_url"));
        assert_eq!(store.started.load(Ordering::Relaxed), 2);
        let first = store.first.lock().unwrap().take().unwrap();
        assert_eq!(first.key.admission_session_id, ticket.session_id);
        assert!(store.get(&first).await.is_err());
        assert_eq!(store.get(&unrelated).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn expired_admission_does_not_stage_any_inputs() {
        let store = SecondWriteStalls::default();
        let (source, _) = image_source();
        let result = stage_workflow_inputs(
            &store,
            &ticket(),
            video_intent(source.clone(), source),
            1024 * 1024,
            remaining_preparation_budget(0),
        )
        .await;
        assert_eq!(
            result.err().unwrap().error_code(),
            Some("invalid_image_url")
        );
        assert_eq!(store.started.load(Ordering::Relaxed), 0);
    }
}
