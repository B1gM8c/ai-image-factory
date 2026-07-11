use gpt_image_2_gateway::{
    artifacts::FilesystemArtifactBlobStore,
    input_blobs::{InputBlobKey, InputBlobReadError, InputBlobStore},
};
use uuid::Uuid;

#[tokio::test]
async fn filesystem_input_blob_round_trips_without_execution_identity() {
    let root = tempfile::tempdir().expect("input blob tempdir");
    let store = FilesystemArtifactBlobStore::new(root.path()).expect("input blob store");
    let key = InputBlobKey {
        admission_session_id: Uuid::new_v4(),
        input_id: Uuid::new_v4(),
    };

    let blob = store
        .put(key.clone(), b"durable edit input")
        .await
        .expect("store edit input");

    assert_eq!(blob.key, key);
    assert_eq!(
        store.get(&blob).await.expect("load edit input"),
        b"durable edit input"
    );
    store.delete(&blob).await.expect("delete edit input");
    store
        .delete(&blob)
        .await
        .expect("delete missing edit input");
    assert_eq!(store.get(&blob).await, Err(InputBlobReadError::Integrity));
}

#[cfg(unix)]
#[test]
fn filesystem_store_rejects_symlinked_objects_directory() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().expect("artifact root tempdir");
    let target = tempfile::tempdir().expect("symlink target tempdir");
    symlink(target.path(), root.path().join("objects")).expect("objects symlink");

    assert!(FilesystemArtifactBlobStore::new(root.path()).is_err());
}

#[cfg(unix)]
#[test]
fn filesystem_store_rejects_symlinked_inputs_directory() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().expect("artifact root tempdir");
    let target = tempfile::tempdir().expect("symlink target tempdir");
    symlink(target.path(), root.path().join("inputs")).expect("inputs symlink");

    assert!(FilesystemArtifactBlobStore::new(root.path()).is_err());
}

#[tokio::test]
async fn filesystem_session_cleanup_is_idempotent_and_scoped() {
    let root = tempfile::tempdir().expect("input blob tempdir");
    let store = FilesystemArtifactBlobStore::new(root.path()).expect("input blob store");
    let session = Uuid::new_v4();
    let other_session = Uuid::new_v4();
    let first = store
        .put(
            InputBlobKey {
                admission_session_id: session,
                input_id: Uuid::new_v4(),
            },
            b"first",
        )
        .await
        .unwrap();
    let second = store
        .put(
            InputBlobKey {
                admission_session_id: session,
                input_id: Uuid::new_v4(),
            },
            b"second",
        )
        .await
        .unwrap();
    let other = store
        .put(
            InputBlobKey {
                admission_session_id: other_session,
                input_id: Uuid::new_v4(),
            },
            b"other",
        )
        .await
        .unwrap();

    store.delete_session(session).await.unwrap();
    store.delete_session(session).await.unwrap();

    assert_eq!(store.get(&first).await, Err(InputBlobReadError::Integrity));
    assert_eq!(store.get(&second).await, Err(InputBlobReadError::Integrity));
    assert_eq!(store.get(&other).await.unwrap(), b"other");
}

#[cfg(unix)]
#[tokio::test]
async fn filesystem_session_cleanup_rejects_symlink_without_touching_target() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().expect("input blob tempdir");
    let store = FilesystemArtifactBlobStore::new(root.path()).expect("input blob store");
    let target = tempfile::tempdir().expect("session symlink target");
    let sentinel = target.path().join("keep");
    std::fs::write(&sentinel, b"keep").expect("write sentinel");
    let session = Uuid::new_v4();
    let session_path = root
        .path()
        .join("inputs")
        .join(session.simple().to_string());
    symlink(target.path(), &session_path).expect("session symlink");

    assert!(store.delete_session(session).await.is_err());
    assert_eq!(std::fs::read(sentinel).expect("read sentinel"), b"keep");
    assert!(
        std::fs::symlink_metadata(session_path)
            .expect("session symlink remains")
            .file_type()
            .is_symlink()
    );
}

#[tokio::test]
async fn filesystem_input_blob_detects_tampering() {
    let root = tempfile::tempdir().expect("input blob tempdir");
    let store = FilesystemArtifactBlobStore::new(root.path()).expect("input blob store");
    let blob = store
        .put(
            InputBlobKey {
                admission_session_id: Uuid::new_v4(),
                input_id: Uuid::new_v4(),
            },
            b"original",
        )
        .await
        .unwrap();
    std::fs::write(root.path().join(&blob.object_key), b"tampered").unwrap();

    assert_eq!(store.get(&blob).await, Err(InputBlobReadError::Integrity));
}
