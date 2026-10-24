use std::{
    convert::Infallible,
    ffi::CString,
    fs,
    os::unix::{
        ffi::OsStrExt,
        fs::{PermissionsExt, symlink},
    },
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::{
    AsyncOutputSealError, AsyncOutputSink, FreshOutputDirectory, MAX_BATCH_OUTPUT_FILES,
    OutputError, STREAM_BUFFER_BYTES, WorkingDirectory,
};

#[derive(Debug, Default)]
struct RecordingSink {
    bytes: Vec<u8>,
    largest_write: usize,
    writes: Arc<AtomicUsize>,
    replace: Option<(PathBuf, PathBuf)>,
}

impl AsyncOutputSink for RecordingSink {
    type Error = Infallible;

    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), Self::Error> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.bytes.extend_from_slice(chunk);
        self.largest_write = self.largest_write.max(chunk.len());
        if let Some((current, detached)) = self.replace.take() {
            fs::rename(&current, detached).expect("detach provider file");
            fs::write(current, b"replacement").expect("replace provider file");
        }
        Ok(())
    }
}

fn fresh(max_bytes: u64) -> (TempDir, FreshOutputDirectory) {
    let directory = TempDir::new().expect("output directory");
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
        .expect("private permissions");
    let working = WorkingDirectory::new(directory.path()).expect("working directory");
    let output = FreshOutputDirectory::new(&working, max_bytes).expect("fresh capability");
    (directory, output)
}

#[tokio::test]
async fn batch_output_seals_ten_files_in_filename_order_with_bounded_chunks() {
    let (directory, output) = fresh(512 * 1024);
    let payload = vec![b'x'; STREAM_BUFFER_BYTES * 2 + 17];
    for ordinal in (0..MAX_BATCH_OUTPUT_FILES).rev() {
        fs::write(
            directory.path().join(format!("image-{ordinal:02}.png")),
            &payload,
        )
        .expect("provider artifact");
    }
    let files = output
        .verify_files(MAX_BATCH_OUTPUT_FILES)
        .expect("batch snapshot");
    assert_eq!(files.len(), MAX_BATCH_OUTPUT_FILES);
    let expected_hash = Sha256::digest(&payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    for (ordinal, file) in files.into_iter().enumerate() {
        let name = PathBuf::from(format!("image-{ordinal:02}.png"));
        assert_eq!(file.relative_filename(), name);
        let (sealed, sink) = file
            .seal_to_async_sink(RecordingSink::default())
            .await
            .expect("sealed artifact");
        assert_eq!(sealed.relative_filename, name);
        assert_eq!(sealed.sha256_hex, expected_hash);
        assert_eq!(sealed.byte_size, payload.len() as u64);
        assert_eq!(sink.bytes, payload);
        assert!(sink.largest_write <= STREAM_BUFFER_BYTES);
    }
}

#[test]
fn batch_output_bounds_file_count_and_allows_partial_results() {
    for invalid in [0, MAX_BATCH_OUTPUT_FILES + 1] {
        let (_directory, output) = fresh(1024);
        assert!(matches!(
            output.verify_files(invalid),
            Err(OutputError::InvalidFileLimit)
        ));
    }
    let (_directory, output) = fresh(1024);
    assert!(matches!(output.verify_files(10), Err(OutputError::Missing)));
    let (directory, output) = fresh(1024);
    for ordinal in 0..=MAX_BATCH_OUTPUT_FILES {
        fs::write(directory.path().join(format!("{ordinal}")), b"image").expect("artifact");
    }
    assert!(matches!(
        output.verify_files(10),
        Err(OutputError::TooManyEntries)
    ));
    let (directory, output) = fresh(1024);
    fs::write(directory.path().join("only.png"), b"image").expect("artifact");
    assert_eq!(output.verify_files(10).expect("partial results").len(), 1);
    let (directory, output) = fresh(1024);
    for name in ["a", "b"] {
        fs::write(directory.path().join(name), b"image").expect("artifact");
    }
    assert!(matches!(
        output.verify_files(1),
        Err(OutputError::TooManyEntries)
    ));
}

#[test]
fn batch_output_validates_every_artifact_before_returning_any_handles() {
    for shape in [
        "empty",
        "oversized",
        "directory",
        "fifo",
        "symlink",
        "hardlink",
    ] {
        let (directory, output) = fresh(4);
        fs::write(directory.path().join("a-good"), b"good").expect("valid artifact");
        let outside = TempDir::new().expect("outside directory");
        let bad = directory.path().join("z-bad");
        match shape {
            "empty" => fs::write(&bad, b"").expect("empty artifact"),
            "oversized" => fs::write(&bad, b"12345").expect("oversized artifact"),
            "directory" => fs::create_dir(&bad).expect("nested directory"),
            "fifo" => {
                let path = CString::new(bad.as_os_str().as_bytes()).expect("FIFO path");
                assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
            }
            "symlink" | "hardlink" => {
                let target = outside.path().join("target");
                fs::write(&target, b"data").expect("outside artifact");
                if shape == "symlink" {
                    symlink(target, &bad).expect("symbolic link");
                } else {
                    fs::hard_link(target, &bad).expect("hard link");
                }
            }
            _ => unreachable!(),
        }
        assert!(output.verify_files(2).is_err(), "invalid shape: {shape}");
    }
}

#[tokio::test]
async fn batch_output_rejects_other_artifact_replacement_during_streaming() {
    let (directory, output) = fresh(1024);
    for name in ["a", "b"] {
        fs::write(directory.path().join(name), b"original").expect("provider artifact");
    }
    let outside = TempDir::new().expect("outside directory");
    let mut files = output.verify_files(2).expect("snapshot").into_iter();
    let sink = RecordingSink {
        replace: Some((directory.path().join("b"), outside.path().join("detached"))),
        ..RecordingSink::default()
    };
    assert!(matches!(
        files
            .next()
            .expect("first output")
            .seal_to_async_sink(sink)
            .await,
        Err(AsyncOutputSealError::Output(OutputError::ChangedDuringRead))
    ));
}

#[tokio::test]
async fn batch_output_rejects_replaced_added_or_removed_entries_before_writing() {
    for mutation in ["replace", "add", "remove", "permissions"] {
        let (directory, output) = fresh(1024);
        for name in ["a", "b"] {
            fs::write(directory.path().join(name), b"original").expect("artifact");
        }
        let mut files = output.verify_files(2).expect("snapshot").into_iter();
        let outside = TempDir::new().expect("outside directory");
        match mutation {
            "replace" => {
                fs::rename(directory.path().join("b"), outside.path().join("detached"))
                    .expect("detach artifact");
                fs::write(directory.path().join("b"), b"replaced").expect("replace artifact");
            }
            "add" => fs::write(directory.path().join("c"), b"added").expect("add artifact"),
            "remove" => fs::remove_file(directory.path().join("b")).expect("remove artifact"),
            "permissions" => {
                fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755))
                    .expect("unsafe permissions")
            }
            _ => unreachable!(),
        }
        let writes = Arc::new(AtomicUsize::new(0));
        let sink = RecordingSink {
            writes: Arc::clone(&writes),
            ..RecordingSink::default()
        };
        assert!(
            files
                .next()
                .expect("first output")
                .seal_to_async_sink(sink)
                .await
                .is_err()
        );
        assert_eq!(writes.load(Ordering::SeqCst), 0, "mutation: {mutation}");
    }
}

#[tokio::test]
async fn batch_output_keeps_directory_binding_after_path_replacement() {
    let root = TempDir::new().expect("root directory");
    let original = root.path().join("original");
    fs::create_dir(&original).expect("original directory");
    fs::set_permissions(&original, fs::Permissions::from_mode(0o700)).expect("private directory");
    let working = WorkingDirectory::new(&original).expect("working directory");
    let output = FreshOutputDirectory::new(&working, 1024).expect("fresh capability");
    let moved = root.path().join("moved");
    fs::rename(&original, &moved).expect("move bound directory");
    fs::create_dir(&original).expect("replacement directory");
    fs::write(original.join("a"), b"wrong").expect("replacement artifact");
    fs::write(moved.join("a"), b"right").expect("bound artifact");
    let file = output
        .verify_files(1)
        .expect("bound snapshot")
        .pop()
        .expect("output");
    let (_, sink) = file
        .seal_to_async_sink(RecordingSink::default())
        .await
        .expect("seal bound artifact");
    assert_eq!(sink.bytes, b"right");
}

#[test]
fn batch_output_orders_unicode_names_by_raw_bytes() {
    let (directory, output) = fresh(1024);
    for name in ["中", "é", "z"] {
        fs::write(directory.path().join(name), b"image").expect("artifact");
    }
    let files = output.verify_files(3).expect("snapshot");
    let names = files
        .iter()
        .map(|file| file.relative_filename().as_os_str().as_bytes())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![b"z".as_slice(), "é".as_bytes(), "中".as_bytes()]
    );
}

// Linux permits filenames which macOS's filesystem rejects as invalid Unicode.
#[cfg(target_os = "linux")]
#[test]
fn batch_output_orders_non_utf8_names_by_raw_bytes() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};

    let (directory, output) = fresh(1024);
    let names = [
        OsString::from_vec(vec![0xff]),
        OsString::from_vec(vec![0xfe]),
    ];
    for name in &names {
        fs::write(directory.path().join(name), b"image").expect("artifact");
    }
    let files = output.verify_files(2).expect("snapshot");
    assert_eq!(files[0].relative_filename().as_os_str(), names[1]);
    assert_eq!(files[1].relative_filename().as_os_str(), names[0]);
}
