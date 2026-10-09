//! Explicit operator pin for an attested Actions candidate, never a Release fallback.
use super::*;
use tokio::io::AsyncWriteExt;

pub(crate) fn receipt_matches(receipt: &Value, expected: &Value) -> bool {
    expected.as_object().is_some_and(|fields| {
        fields
            .iter()
            .all(|(key, value)| receipt.get(key) == Some(value))
    })
}

pub(crate) async fn copy_bounded_output<R: AsyncRead + Unpin>(
    mut reader: R,
    path: &Path,
    limit: u64,
) -> std::io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await?;
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let length = reader.read(&mut buffer).await?;
        if length == 0 {
            break;
        }
        total += length as u64;
        if total > limit {
            return Err(std::io::Error::other(
                "candidate archive exceeds pinned size",
            ));
        }
        file.write_all(&buffer[..length]).await?;
    }
    file.sync_all().await?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CandidatePin {
    pub version: String,
    pub commit_sha: String,
    pub tag_object_sha: String,
    pub run_id: u64,
    pub run_attempt: u64,
    pub artifact_id: u64,
    pub artifact_sha256: String,
    pub artifact_bytes: u64,
    pub manifest_sha256: String,
    pub bundle_sha256: String,
}

impl CandidatePin {
    pub fn from_file(path: &Path) -> Result<Self, UpdaterError> {
        if !path.is_absolute() {
            return Err(UpdaterError::Config(
                "candidate pin path must be absolute".into(),
            ));
        }
        for ancestor in path.ancestors() {
            let metadata = std::fs::symlink_metadata(ancestor)?;
            if metadata.uid() != 0
                || metadata.mode() & 0o022 != 0
                || metadata.file_type().is_symlink()
                || (ancestor != path && !metadata.is_dir())
            {
                return Err(UpdaterError::Config(
                    "candidate pin must be root-protected without symlinks".into(),
                ));
            }
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > 4096 {
            return Err(UpdaterError::Config(
                "candidate pin must be a bounded regular file".into(),
            ));
        }
        let mut bytes = Vec::new();
        Read::by_ref(&mut file).take(4097).read_to_end(&mut bytes)?;
        let pin: Self = serde_json::from_slice(&bytes)
            .map_err(|_| UpdaterError::Config("invalid candidate pin JSON".into()))?;
        pin.validate()?;
        Ok(pin)
    }

    fn validate(&self) -> Result<(), UpdaterError> {
        validate_release_token(&self.version, "candidate version")?;
        if !self.version.starts_with('v')
            || self.commit_sha.len() != 40
            || self.tag_object_sha.len() != 40
            || !self
                .tag_object_sha
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || !self.commit_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
            || self.run_id == 0
            || self.run_attempt == 0
            || self.artifact_id == 0
            || self.artifact_bytes == 0
            || self.artifact_bytes > MAX_RELEASE_BYTES
        {
            return Err(UpdaterError::InvalidRelease(
                "candidate requires an exact tag, commit, run attempt and artifact".into(),
            ));
        }
        validate_sha256(&self.manifest_sha256)?;
        validate_sha256(&self.artifact_sha256)?;
        validate_sha256(&self.bundle_sha256)
    }

    pub fn check_run(&self, run: &Value, repository: &str) -> Result<(), UpdaterError> {
        if run["id"].as_u64() != Some(self.run_id)
            || run["run_attempt"].as_u64() != Some(self.run_attempt)
            || run["head_sha"].as_str() != Some(&self.commit_sha)
            || run["head_branch"].as_str() != Some(&self.version)
            || run["repository"]["full_name"].as_str() != Some(repository)
            || run["head_repository"]["full_name"].as_str() != Some(repository)
            || run["path"].as_str() != Some(".github/workflows/release.yml")
            || run["event"].as_str() != Some("workflow_dispatch")
            || run["status"].as_str() != Some("completed")
            || run["conclusion"].as_str() != Some("success")
        {
            return Err(UpdaterError::InvalidRelease(
                "candidate run identity or completion mismatch".into(),
            ));
        }
        Ok(())
    }

    pub fn check_artifact(&self, artifact: &Value, target: &str) -> Result<(), UpdaterError> {
        if artifact["id"].as_u64() != Some(self.artifact_id)
            || artifact["name"].as_str() != Some(&format!("release-{target}"))
            || artifact["expired"].as_bool() != Some(false)
            || artifact["size_in_bytes"].as_u64() != Some(self.artifact_bytes)
            || artifact["digest"].as_str() != Some(&format!("sha256:{}", self.artifact_sha256))
            || artifact["workflow_run"]["id"].as_u64() != Some(self.run_id)
            || artifact["workflow_run"]["head_sha"].as_str() != Some(&self.commit_sha)
        {
            return Err(UpdaterError::InvalidRelease(
                "candidate artifact identity mismatch".into(),
            ));
        }
        Ok(())
    }

    pub fn check_attestation(
        &self,
        verified: &Value,
        repository: &str,
    ) -> Result<(), UpdaterError> {
        let invocation = format!(
            "https://github.com/{repository}/actions/runs/{}/attempts/{}",
            self.run_id, self.run_attempt
        );
        let identity = format!(
            "https://github.com/{repository}/.github/workflows/release.yml@refs/tags/{}",
            self.version
        );
        let valid = verified.as_array().is_some_and(|items| {
            items.iter().any(|item| {
                let certificate = &item["verificationResult"]["signature"]["certificate"];
                certificate["runInvocationURI"].as_str() == Some(&invocation)
                    && certificate["buildSignerURI"].as_str() == Some(&identity)
                    && certificate["buildSignerDigest"].as_str() == Some(&self.commit_sha)
                    && certificate["sourceRepositoryDigest"].as_str() == Some(&self.commit_sha)
                    && certificate["sourceRepositoryRef"].as_str()
                        == Some(&format!("refs/tags/{}", self.version))
                    && certificate["runnerEnvironment"].as_str() == Some("github-hosted")
                    && certificate["buildTrigger"].as_str() == Some("workflow_dispatch")
                    && certificate["issuer"].as_str()
                        == Some("https://token.actions.githubusercontent.com")
            })
        });
        if !valid {
            return Err(UpdaterError::InvalidRelease(
                "candidate attestation does not bind the pinned Actions invocation".into(),
            ));
        }
        Ok(())
    }
}

impl Updater {
    pub(crate) async fn download_candidate(
        &self,
        pin: &CandidatePin,
        destination: &Path,
    ) -> Result<(), UpdaterError> {
        let archive = destination.join("candidate.zip");
        let endpoint = format!(
            "repos/{}/actions/artifacts/{}/zip",
            self.config.repository, pin.artifact_id
        );
        run_trusted_output(
            &self.config.gh_executable,
            ["api", &endpoint],
            &github_environment(),
            Some((&archive, pin.artifact_bytes)),
        )
        .await?;
        verify_file_digest(&archive, &pin.artifact_sha256, pin.artifact_bytes).await?;
        let prefix = format!(
            "ai-image-factory-{}-{}",
            pin.version, self.config.target_triple
        );
        // Extract only the two exact regular members; no paths, links, overwrite or wildcard extraction.
        // Debian/Ubuntu python3 is an alias. Validate and execute the same
        // canonical binary; do not weaken the shared no-symlink trust policy.
        let python = std::fs::canonicalize("/usr/bin/python3")?;
        run_trusted(&python, [OsStr::new("-c"), OsStr::new(r#"
import pathlib, shutil, stat, sys, zipfile
archive, directory, prefix = sys.argv[1:]
with zipfile.ZipFile(archive) as zipped:
    expected = {prefix + '.manifest.json': 8 * 1024 * 1024, prefix + '.tar.gz': 4 * 1024**3}
    entries = zipped.infolist()
    if len(entries) != 2 or {entry.filename for entry in entries} != set(expected):
        raise SystemExit('candidate archive member mismatch')
    for entry in entries:
        kind = stat.S_IFMT(entry.external_attr >> 16)
        if kind not in (0, stat.S_IFREG) or not 0 < entry.file_size <= expected[entry.filename]:
            raise SystemExit('candidate archive member type or size rejected')
        with zipped.open(entry) as source, (pathlib.Path(directory) / entry.filename).open('xb') as target:
            shutil.copyfileobj(source, target, 65536)
"#), archive.as_os_str(), destination.as_os_str(), OsStr::new(&prefix)], &BTreeMap::new()).await?;
        Ok(())
    }

    pub(crate) fn source_receipt(&self, version: &str) -> Value {
        match &self.config.candidate {
            Some(pin) => json!({"latest_version": version, "source": "actions_candidate",
                "immutable": false, "run_id": pin.run_id, "run_attempt": pin.run_attempt,
                "artifact_id": pin.artifact_id, "commit_sha": pin.commit_sha,
                "tag_object_sha": pin.tag_object_sha, "artifact_sha256": pin.artifact_sha256,
                "artifact_bytes": pin.artifact_bytes,
                "manifest_sha256": pin.manifest_sha256, "bundle_sha256": pin.bundle_sha256}),
            None => {
                json!({"latest_version": version, "source": "github_release", "immutable": true})
            }
        }
    }

    /// Staging only: no command insertion, policy change, service change or current switch.
    pub async fn prepare_candidate(&self) -> Result<(), UpdaterError> {
        let pin = self.config.candidate.as_ref().ok_or_else(|| {
            UpdaterError::Config("prepare-candidate requires an operator pin".into())
        })?;
        self.verify_candidate(pin).await?;
        let staged = self.stage_release(None, &pin.version).await?;
        self.verify_schema_contract(&staged.manifest).await?;
        println!(
            "{}",
            json!({"source": "actions_candidate", "release_dir": staged.release_dir,
            "manifest_sha256": staged.manifest_sha256, "commit_sha": staged.manifest.commit_sha})
        );
        Ok(())
    }

    pub(crate) async fn github_json(&self, endpoint: &str) -> Result<Value, UpdaterError> {
        let output = run_trusted(
            &self.config.gh_executable,
            [OsStr::new("api"), OsStr::new(endpoint)],
            &github_environment(),
        )
        .await?;
        serde_json::from_slice(&output.stdout)
            .map_err(|_| UpdaterError::InvalidRelease("invalid GitHub metadata".into()))
    }

    pub(crate) async fn verify_candidate(&self, pin: &CandidatePin) -> Result<(), UpdaterError> {
        let base = format!("repos/{}", self.config.repository);
        let reference = self
            .github_json(&format!("{base}/git/ref/tags/{}", pin.version))
            .await?;
        let object = reference["object"]["sha"]
            .as_str()
            .filter(|sha| sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or_else(|| {
                UpdaterError::InvalidRelease("candidate tag object is invalid".into())
            })?;
        if reference["object"]["type"].as_str() != Some("tag") {
            return Err(UpdaterError::InvalidRelease(
                "candidate requires an annotated signed tag".into(),
            ));
        }
        if object != pin.tag_object_sha {
            return Err(UpdaterError::InvalidRelease(
                "candidate tag object changed".into(),
            ));
        }
        let tag = self
            .github_json(&format!("{base}/git/tags/{object}"))
            .await?;
        if tag["verification"]["verified"].as_bool() != Some(true)
            || tag["verification"]["reason"].as_str() != Some("valid")
            || tag["object"]["type"].as_str() != Some("commit")
            || tag["object"]["sha"].as_str() != Some(&pin.commit_sha)
            || tag["tag"].as_str() != Some(&pin.version)
        {
            return Err(UpdaterError::InvalidRelease(
                "candidate signed tag verification failed".into(),
            ));
        }
        pin.check_run(
            &self
                .github_json(&format!("{base}/actions/runs/{}", pin.run_id))
                .await?,
            &self.config.repository,
        )?;
        pin.check_artifact(
            &self
                .github_json(&format!("{base}/actions/artifacts/{}", pin.artifact_id))
                .await?,
            &self.config.target_triple,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin() -> CandidatePin {
        CandidatePin {
            version: "v0.1.0-20261009.r2.test".into(),
            commit_sha: "a".repeat(40),
            tag_object_sha: "d".repeat(40),
            run_id: 123,
            run_attempt: 1,
            artifact_id: 456,
            artifact_sha256: "e".repeat(64),
            artifact_bytes: 100,
            manifest_sha256: "b".repeat(64),
            bundle_sha256: "c".repeat(64),
        }
    }

    #[test]
    fn candidate_pin_is_exact_and_bounded() {
        let base = pin();
        assert!(base.validate().is_ok());
        for bad in ["../tag", "vtag/child", "tag"] {
            let mut candidate = base.clone();
            candidate.version = bad.into();
            assert!(candidate.validate().is_err());
        }
        for index in 0..6 {
            let mut candidate = base.clone();
            match index {
                0 => candidate.commit_sha = "main".into(),
                1 => candidate.run_id = 0,
                2 => candidate.run_attempt = 0,
                3 => candidate.artifact_id = 0,
                4 => candidate.manifest_sha256 = "wrong".into(),
                _ => candidate.bundle_sha256 = "wrong".into(),
            }
            assert!(candidate.validate().is_err());
        }
        assert!(CandidatePin::from_file(Path::new("relative.json")).is_err());
        let temporary = tempfile::NamedTempFile::new().unwrap();
        assert!(CandidatePin::from_file(temporary.path()).is_err());
    }

    #[test]
    fn check_receipt_requires_every_pinned_identity_before_apply() {
        let expected = json!({"source":"actions_candidate", "immutable":false,
            "latest_version":"v1", "commit_sha":"a", "tag_object_sha":"b",
            "run_id":123, "run_attempt":1, "artifact_id":456,
            "artifact_sha256":"c", "artifact_bytes":100,
            "manifest_sha256":"d", "bundle_sha256":"e"});
        assert!(receipt_matches(&expected, &expected));
        for key in expected.as_object().unwrap().keys() {
            let mut changed = expected.clone();
            changed.as_object_mut().unwrap().remove(key);
            assert!(!receipt_matches(&changed, &expected), "missing {key}");
            changed[key] = json!("different");
            assert!(!receipt_matches(&changed, &expected), "changed {key}");
        }
        assert!(!receipt_matches(
            &json!({"source":"github_release"}),
            &expected
        ));
        assert!(!receipt_matches(&Value::Null, &expected));
    }

    #[test]
    fn candidate_run_rejects_cross_repo_ref_workflow_attempt_and_failed_runs() {
        let pin = pin();
        let run = json!({"id":123, "run_attempt":1, "head_sha":pin.commit_sha,
            "head_branch":pin.version, "repository":{"full_name":"owner/repo"},
            "head_repository":{"full_name":"owner/repo"},
            "path":".github/workflows/release.yml", "event":"workflow_dispatch",
            "status":"completed", "conclusion":"success"});
        assert!(pin.check_run(&run, "owner/repo").is_ok());
        for (path, value) in [
            ("/id", json!(124)),
            ("/run_attempt", json!(2)),
            ("/head_sha", json!("d".repeat(40))),
            ("/head_branch", json!("main")),
            ("/repository/full_name", json!("other/repo")),
            ("/head_repository/full_name", json!("fork/repo")),
            ("/path", json!(".github/workflows/untrusted.yml")),
            ("/event", json!("pull_request")),
            ("/status", json!("in_progress")),
            ("/conclusion", json!("failure")),
        ] {
            let mut invalid = run.clone();
            *invalid.pointer_mut(path).unwrap() = value;
            assert!(pin.check_run(&invalid, "owner/repo").is_err(), "{path}");
        }
    }

    #[test]
    fn candidate_artifact_is_pinned_to_run_commit_and_architecture() {
        let pin = pin();
        let artifact = json!({"id":456, "name":"release-x86_64-unknown-linux-gnu",
            "expired":false, "size_in_bytes":100,"digest":format!("sha256:{}",pin.artifact_sha256),
            "workflow_run":{"id":123,"head_sha":pin.commit_sha}});
        assert!(
            pin.check_artifact(&artifact, "x86_64-unknown-linux-gnu")
                .is_ok()
        );
        for (path, value) in [
            ("/id", json!(457)),
            ("/name", json!("release-aarch64-unknown-linux-gnu")),
            ("/expired", json!(true)),
            ("/workflow_run/id", json!(124)),
            ("/workflow_run/head_sha", json!("d".repeat(40))),
        ] {
            let mut invalid = artifact.clone();
            *invalid.pointer_mut(path).unwrap() = value;
            assert!(
                pin.check_artifact(&invalid, "x86_64-unknown-linux-gnu")
                    .is_err()
            );
        }
    }

    #[test]
    fn attestation_binds_certificate_invocation_not_claimed_predicate() {
        let pin = pin();
        let certificate = json!({
            "runInvocationURI":"https://github.com/owner/repo/actions/runs/123/attempts/1",
            "buildSignerURI":format!("https://github.com/owner/repo/.github/workflows/release.yml@refs/tags/{}", pin.version),
            "buildSignerDigest":pin.commit_sha, "sourceRepositoryDigest":pin.commit_sha,
            "sourceRepositoryRef":format!("refs/tags/{}",pin.version),
            "runnerEnvironment":"github-hosted", "buildTrigger":"workflow_dispatch",
            "issuer":"https://token.actions.githubusercontent.com"});
        let verified = json!([{"verificationResult":{"signature":{"certificate":certificate}}}]);
        assert!(pin.check_attestation(&verified, "owner/repo").is_ok());
        for key in certificate.as_object().unwrap().keys() {
            let mut changed = verified.clone();
            changed[0]["verificationResult"]["signature"]["certificate"][key] = json!("wrong");
            assert!(
                pin.check_attestation(&changed, "owner/repo").is_err(),
                "{key}"
            );
        }
        assert!(
            pin.check_attestation(
                &json!([{"verificationResult":{"statement":{"predicate":certificate}}}]),
                "owner/repo"
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn download_output_is_bounded_and_never_overwrites() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("candidate.zip");
        copy_bounded_output(&b"abc"[..], &path, 3).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"abc");
        assert!(copy_bounded_output(&b"def"[..], &path, 3).await.is_err());
        assert!(
            copy_bounded_output(&b"abcd"[..], &temporary.path().join("oversized.zip"), 3)
                .await
                .is_err()
        );
    }
}
