use gpt_image_2_gateway::artifacts::{
    ArtifactBlobStore, ArtifactIdentity, ArtifactReadError, FilesystemArtifactBlobStore,
    validate_artifact_root_isolated,
};
use uuid::Uuid;

#[tokio::test]
async fn filesystem_store_round_trips_verified_bytes() {
    let root = tempfile::tempdir().expect("artifact tempdir");
    let store = FilesystemArtifactBlobStore::new(root.path()).expect("artifact store");
    let bytes = b"opaque generated image";

    let artifact = store.put(identity(), bytes).await.expect("store artifact");
    let loaded = store.get(&artifact).await.expect("load artifact");

    assert_eq!(loaded, bytes);
    assert_eq!(artifact.byte_size, bytes.len() as u64);
    assert_eq!(artifact.sha256_hex.len(), 64);
}

#[tokio::test]
async fn filesystem_store_rejects_tampered_bytes() {
    let root = tempfile::tempdir().expect("artifact tempdir");
    let store = FilesystemArtifactBlobStore::new(root.path()).expect("artifact store");
    let artifact = store
        .put(identity(), b"original image")
        .await
        .expect("store artifact");

    std::fs::write(root.path().join(&artifact.object_key), b"tampered").expect("tamper artifact");

    assert_eq!(
        store.get(&artifact).await,
        Err(ArtifactReadError::Integrity)
    );
}

fn identity() -> ArtifactIdentity {
    ArtifactIdentity {
        artifact_id: Uuid::new_v4(),
        tenant_id: "tenant-artifact".to_string(),
        job_id: Uuid::new_v4(),
        work_item_id: Uuid::new_v4(),
        execution_id: Uuid::new_v4(),
        lease_epoch: 1,
        output_index: 0,
        media_type: "image/png".to_string(),
    }
}

#[test]
fn artifact_root_must_not_overlap_codex_home() {
    let root = tempfile::tempdir().expect("storage tempdir");
    let codex_home = root.path().join("codex-home");
    let nested_artifacts = codex_home.join("artifacts");
    let sibling_artifacts = root.path().join("artifacts");
    std::fs::create_dir_all(&nested_artifacts).expect("nested artifact root");
    std::fs::create_dir_all(&sibling_artifacts).expect("sibling artifact root");

    assert!(validate_artifact_root_isolated(&nested_artifacts, &codex_home).is_err());
    assert!(validate_artifact_root_isolated(&codex_home, &nested_artifacts).is_err());
    assert!(validate_artifact_root_isolated(&sibling_artifacts, &codex_home).is_ok());
}
