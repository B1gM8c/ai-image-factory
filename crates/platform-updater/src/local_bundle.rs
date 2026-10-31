//! Explicit root-authorized local build provenance. This is not an attestation.
use super::*;

const MAX_PIN_BYTES: u64 = 16 * 1024;
const MAX_BUILD_RECORD_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalBundlePin {
    pub schema_version: u32,
    pub source: String,
    pub version: String,
    pub commit_sha: String,
    pub target_triple: String,
    pub manifest_path: PathBuf,
    pub manifest_sha256: String,
    pub manifest_bytes: u64,
    pub bundle_path: PathBuf,
    pub bundle_sha256: String,
    pub bundle_bytes: u64,
    pub build_record_path: PathBuf,
    pub build_record_sha256: String,
    pub build_record_bytes: u64,
    #[serde(skip)]
    pin_path: PathBuf,
    #[serde(skip)]
    pin_sha256: String,
}

pub(crate) fn validate_source_selection(candidate: bool, local: bool) -> Result<(), UpdaterError> {
    if candidate && local {
        return Err(UpdaterError::Config(
            "AIF_UPDATE_CANDIDATE_PIN and AIF_UPDATE_LOCAL_BUNDLE_PIN are mutually exclusive"
                .into(),
        ));
    }
    Ok(())
}

impl LocalBundlePin {
    pub fn from_file(path: &Path) -> Result<Self, UpdaterError> {
        let bytes = read_protected(path, MAX_PIN_BYTES)?;
        Self::from_bytes(path, &bytes)
    }

    fn from_bytes(path: &Path, bytes: &[u8]) -> Result<Self, UpdaterError> {
        let mut pin: Self = serde_json::from_slice(bytes)
            .map_err(|_| UpdaterError::Config("invalid local build pin JSON".into()))?;
        pin.pin_path = path.to_owned();
        pin.pin_sha256 = sha256_hex(bytes);
        pin.validate()?;
        Ok(pin)
    }

    fn validate(&self) -> Result<(), UpdaterError> {
        validate_release_token(&self.version, "local build version")?;
        if self.schema_version != 1
            || self.source != "local_build"
            || !self.version.starts_with('v')
            || self.commit_sha.len() != 40
            || !self.commit_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
            || !matches!(
                self.target_triple.as_str(),
                "x86_64-unknown-linux-gnu" | "aarch64-unknown-linux-gnu"
            )
            || !(1..=MAX_MANIFEST_BYTES).contains(&self.manifest_bytes)
            || !(1..=MAX_RELEASE_BYTES).contains(&self.bundle_bytes)
            || !(1..=MAX_BUILD_RECORD_BYTES).contains(&self.build_record_bytes)
        {
            return Err(UpdaterError::InvalidRelease(
                "local build pin identity or size is invalid".into(),
            ));
        }
        for path in [
            &self.pin_path,
            &self.manifest_path,
            &self.bundle_path,
            &self.build_record_path,
        ] {
            if !path.is_absolute()
                || path
                    .components()
                    .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
            {
                return Err(UpdaterError::Config(
                    "local build paths must be absolute without traversal".into(),
                ));
            }
        }
        let paths: BTreeSet<_> = [
            &self.pin_path,
            &self.manifest_path,
            &self.bundle_path,
            &self.build_record_path,
        ]
        .into_iter()
        .collect();
        if paths.len() != 4 {
            return Err(UpdaterError::InvalidRelease(
                "local build inputs must be distinct files".into(),
            ));
        }
        for digest in [
            &self.manifest_sha256,
            &self.bundle_sha256,
            &self.build_record_sha256,
        ] {
            validate_sha256(digest)?;
        }
        Ok(())
    }

    // Called for every Check/Apply/staging pass, not just daemon startup.
    pub fn verify_source(&self, target: &str) -> Result<(), UpdaterError> {
        self.verify_current_pin(&Self::from_file(&self.pin_path)?)?;
        if self.target_triple != target {
            return Err(UpdaterError::InvalidRelease(
                "local build target differs from updater target".into(),
            ));
        }
        let record = read_protected(&self.build_record_path, self.build_record_bytes)?;
        self.verify_build_record(&record)
    }

    fn verify_current_pin(&self, current: &Self) -> Result<(), UpdaterError> {
        if current != self {
            return Err(UpdaterError::InvalidRelease(
                "local build pin changed; restart updater and Check again".into(),
            ));
        }
        Ok(())
    }

    fn verify_build_record(&self, bytes: &[u8]) -> Result<(), UpdaterError> {
        if bytes.len() as u64 != self.build_record_bytes
            || sha256_hex(bytes) != self.build_record_sha256
        {
            return Err(UpdaterError::InvalidRelease(
                "local build record differs from pin".into(),
            ));
        }
        let record: Value = serde_json::from_slice(bytes)
            .map_err(|_| UpdaterError::InvalidRelease("invalid local build record JSON".into()))?;
        if record["schema_version"] != 1
            || record["source"] != "local_build"
            || record["commit_sha"] != self.commit_sha
            || record["clean_worktree"] != true
            || record["target_triple"] != self.target_triple
            || record["manifest_sha256"] != self.manifest_sha256
            || record["manifest_bytes"].as_u64() != Some(self.manifest_bytes)
            || record["bundle_sha256"] != self.bundle_sha256
            || record["bundle_bytes"].as_u64() != Some(self.bundle_bytes)
        {
            return Err(UpdaterError::InvalidRelease(
                "local build record does not bind the pinned source and outputs".into(),
            ));
        }
        Ok(())
    }

    pub fn verify_manifest(&self, manifest: &ReleaseManifest) -> Result<(), UpdaterError> {
        if manifest.release_version != self.version
            || manifest.commit_sha != self.commit_sha
            || manifest.target_triple != self.target_triple
            || manifest.bundle_sha256 != self.bundle_sha256
            || manifest.bundle_bytes != self.bundle_bytes
        {
            return Err(UpdaterError::InvalidRelease(
                "local manifest differs from pinned source and bundle".into(),
            ));
        }
        Ok(())
    }

    pub async fn copy_assets(&self, destination: &Path, target: &str) -> Result<(), UpdaterError> {
        self.verify_source(target)?;
        let prefix = format!("ai-image-factory-{}-{}", self.version, self.target_triple);
        for (source, name, digest, bytes) in [
            (
                &self.manifest_path,
                format!("{prefix}.manifest.json"),
                &self.manifest_sha256,
                self.manifest_bytes,
            ),
            (
                &self.bundle_path,
                format!("{prefix}.tar.gz"),
                &self.bundle_sha256,
                self.bundle_bytes,
            ),
        ] {
            let file = open_protected(source, bytes)?;
            copy_asset(file, &destination.join(name), digest, bytes).await?;
        }
        self.verify_source(target)
    }

    pub fn receipt(&self) -> Value {
        json!({"source":"local_build", "immutable":false, "latest_version":self.version,
            "commit_sha":self.commit_sha, "target_triple":self.target_triple,
            "pin_sha256":self.pin_sha256, "manifest_sha256":self.manifest_sha256,
            "manifest_bytes":self.manifest_bytes, "bundle_sha256":self.bundle_sha256,
            "bundle_bytes":self.bundle_bytes, "build_record_sha256":self.build_record_sha256,
            "build_record_bytes":self.build_record_bytes})
    }
}

fn open_protected(path: &Path, limit: u64) -> Result<std::fs::File, UpdaterError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err(UpdaterError::Config(
            "local build path must be absolute without traversal".into(),
        ));
    }
    for ancestor in path.ancestors() {
        let metadata = std::fs::symlink_metadata(ancestor)?;
        if metadata.uid() != 0
            || metadata.mode() & 0o022 != 0
            || metadata.file_type().is_symlink()
            || (ancestor != path && !metadata.is_dir())
            || (ancestor == path && !metadata.is_file())
        {
            return Err(UpdaterError::Config(
                "local build inputs must be root-protected without symlinks".into(),
            ));
        }
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != 0
        || metadata.mode() & 0o022 != 0
        || metadata.nlink() != 1
        || metadata.len() == 0
        || metadata.len() > limit
    {
        return Err(UpdaterError::InvalidRelease(
            "local build input must be a bounded protected regular file".into(),
        ));
    }
    Ok(file)
}

fn read_protected(path: &Path, limit: u64) -> Result<Vec<u8>, UpdaterError> {
    let mut file = open_protected(path, limit)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(UpdaterError::InvalidRelease(
            "local build input exceeds size limit".into(),
        ));
    }
    Ok(bytes)
}

async fn copy_asset(
    file: std::fs::File,
    destination: &Path,
    digest: &str,
    bytes: u64,
) -> Result<(), UpdaterError> {
    candidate::copy_bounded_output(tokio::fs::File::from_std(file), destination, bytes).await?;
    verify_file_digest(destination, digest, bytes).await
}

impl Updater {
    pub async fn prepare_local(&self) -> Result<(), UpdaterError> {
        let pin = self.config.local_bundle.as_ref().ok_or_else(|| {
            UpdaterError::Config("prepare-local requires AIF_UPDATE_LOCAL_BUNDLE_PIN".into())
        })?;
        pin.verify_source(&self.config.target_triple)?;
        let staged = self.stage_release(None, &pin.version).await?;
        self.verify_schema_contract(&staged.manifest).await?;
        println!("{}", pin.receipt());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin_json() -> Value {
        json!({"schema_version":1,"source":"local_build","version":"v1.2.3",
            "commit_sha":"0123456789abcdef0123456789abcdef01234567",
            "target_triple":"x86_64-unknown-linux-gnu",
            "manifest_path":"/var/lib/aif-local/manifest.json","manifest_sha256":"b".repeat(64),"manifest_bytes":12,
            "bundle_path":"/var/lib/aif-local/bundle.tar.gz","bundle_sha256":"a".repeat(64),"bundle_bytes":42,
            "build_record_path":"/var/lib/aif-local/build.json","build_record_sha256":"c".repeat(64),"build_record_bytes":10})
    }

    fn pin() -> LocalBundlePin {
        LocalBundlePin::from_bytes(
            Path::new("/var/lib/aif-local/pin.json"),
            &serde_json::to_vec(&pin_json()).unwrap(),
        )
        .unwrap()
    }

    fn record(pin: &LocalBundlePin) -> Value {
        json!({"schema_version":1,"source":"local_build","commit_sha":pin.commit_sha,
            "clean_worktree":true,"target_triple":pin.target_triple,
            "manifest_sha256":pin.manifest_sha256,"manifest_bytes":pin.manifest_bytes,
            "bundle_sha256":pin.bundle_sha256,"bundle_bytes":pin.bundle_bytes,
            "checks":{"updater_tests":"passed"}})
    }

    #[test]
    fn local_pin_requires_explicit_identity_and_bounded_distinct_paths() {
        let base = pin_json();
        for (key, value) in [
            ("source", json!("actions_candidate")),
            ("schema_version", json!(2)),
            ("version", json!("../v1")),
            ("commit_sha", json!("main")),
            ("target_triple", json!("aarch64-apple-darwin")),
            ("manifest_path", json!("relative.json")),
            ("bundle_path", json!("/var/lib/../bundle")),
            ("build_record_path", base["manifest_path"].clone()),
            ("bundle_bytes", json!(0)),
            ("manifest_bytes", json!(MAX_MANIFEST_BYTES + 1)),
            ("build_record_bytes", json!(MAX_BUILD_RECORD_BYTES + 1)),
            ("bundle_sha256", json!("unverified")),
            ("run_id", json!(123)),
        ] {
            let mut input = base.clone();
            input[key] = value;
            assert!(
                LocalBundlePin::from_bytes(
                    Path::new("/var/lib/aif-local/pin.json"),
                    &serde_json::to_vec(&input).unwrap()
                )
                .is_err(),
                "{key}"
            );
        }
        assert!(validate_source_selection(true, true).is_err());
        for selection in [(false, false), (true, false), (false, true)] {
            validate_source_selection(selection.0, selection.1).unwrap();
        }
    }

    #[test]
    fn changed_pin_and_stale_check_receipt_are_rejected() {
        let pin = pin();
        pin.verify_current_pin(&pin).unwrap();
        let receipt = pin.receipt();
        assert!(candidate::receipt_matches(&receipt, &pin.receipt()));
        for key in receipt.as_object().unwrap().keys() {
            let mut stale = receipt.clone();
            stale.as_object_mut().unwrap().remove(key);
            assert!(!candidate::receipt_matches(&stale, &pin.receipt()), "{key}");
            stale[key] = json!("changed");
            assert!(!candidate::receipt_matches(&stale, &pin.receipt()), "{key}");
        }
        let mut changed = pin.clone();
        changed.bundle_sha256 = "d".repeat(64);
        assert!(pin.verify_current_pin(&changed).is_err());
        assert!(!candidate::receipt_matches(&receipt, &changed.receipt()));
        changed = pin.clone();
        changed.pin_sha256 = "e".repeat(64);
        assert!(pin.verify_current_pin(&changed).is_err());
        assert!(!candidate::receipt_matches(
            &json!({"source":"actions_candidate"}),
            &receipt
        ));
        assert_eq!(receipt["immutable"], false);
        assert!(receipt.get("run_id").is_none());
        assert!(receipt.get("tag_object_sha").is_none());
    }

    #[test]
    fn build_record_and_manifest_must_bind_same_source_and_outputs() {
        let mut pin = pin();
        let base = record(&pin);
        let bytes = serde_json::to_vec(&base).unwrap();
        pin.build_record_sha256 = sha256_hex(&bytes);
        pin.build_record_bytes = bytes.len() as u64;
        pin.verify_build_record(&bytes).unwrap();
        let mut tampered = bytes.clone();
        tampered.push(b' ');
        assert!(pin.verify_build_record(&tampered).is_err());
        for (key, value) in [
            ("clean_worktree", json!(false)),
            ("commit_sha", json!("f".repeat(40))),
            ("bundle_bytes", json!(43)),
            ("source", json!("actions_candidate")),
        ] {
            let mut changed = base.clone();
            changed[key] = value;
            let bytes = serde_json::to_vec(&changed).unwrap();
            pin.build_record_sha256 = sha256_hex(&bytes);
            pin.build_record_bytes = bytes.len() as u64;
            assert!(pin.verify_build_record(&bytes).is_err(), "{key}");
        }
        let mut manifest = crate::tests::valid_manifest();
        pin.verify_manifest(&manifest).unwrap();
        manifest.commit_sha = "f".repeat(40);
        assert!(pin.verify_manifest(&manifest).is_err());
        manifest = crate::tests::valid_manifest();
        manifest.bundle_bytes += 1;
        assert!(pin.verify_manifest(&manifest).is_err());
    }

    #[tokio::test]
    async fn staging_rejects_same_size_byte_swap_short_long_and_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let expected = b"original bundle";
        for (name, bytes) in [
            ("valid", expected.as_slice()),
            ("swap", b"modified bundle".as_slice()),
            ("short", b"short".as_slice()),
            ("long", b"original bundle extra".as_slice()),
        ] {
            std::fs::write(&source, bytes).unwrap();
            let result = copy_asset(
                std::fs::File::open(&source).unwrap(),
                &dir.path().join(name),
                &sha256_hex(expected),
                expected.len() as u64,
            )
            .await;
            assert_eq!(result.is_ok(), name == "valid", "{name}");
        }
        std::fs::write(&source, expected).unwrap();
        assert!(
            copy_asset(
                std::fs::File::open(&source).unwrap(),
                &dir.path().join("valid"),
                &sha256_hex(expected),
                expected.len() as u64
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn unsafe_local_inputs_fail_closed_without_opening_special_files() {
        let dir = tempfile::tempdir().unwrap();
        let directory = std::fs::canonicalize(dir.path()).unwrap();
        let source = directory.join("source");
        std::fs::write(&source, b"value").unwrap();
        let linked = directory.join("link");
        symlink(&source, &linked).unwrap();
        assert!(open_protected(&linked, 10).is_err());
        assert!(open_protected(&directory, 10).is_err());
        assert!(open_protected(Path::new("relative"), 10).is_err());
        let fifo = directory.join("fifo");
        let name = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(open_protected(&fifo, 10).is_err());
        assert!(LocalBundlePin::from_file(&source).is_err());
    }
}
