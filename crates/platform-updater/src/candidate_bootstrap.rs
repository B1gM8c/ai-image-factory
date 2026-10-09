//! Fixed-updater maintenance only; application activation remains owner Check/Apply.
use super::*;

const FIXED: &str = "/usr/libexec/ai-image-factory/updated";
const HELPER: &str = "/usr/libexec/ai-image-factory/upgrade-updater";
const SYSTEMCTL: &str = "/usr/bin/systemctl";
const LOCK_TIMEOUT: Duration = Duration::from_secs(30);
const PREPARE_TIMEOUT: Duration = Duration::from_secs(1800);
const HANDOFF_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, PartialEq)]
struct Snapshot {
    current: PathBuf,
    schema: i64,
    services: Vec<u8>,
}

impl Updater {
    async fn candidate_idle_snapshot(&self) -> Result<Snapshot, UpdaterError> {
        if self.config.apply_enabled || !self.pending_recovery_descriptor_ids()?.is_empty() {
            return Err(UpdaterError::Config(
                "candidate bootstrap requires Apply disabled and no recovery descriptors".into(),
            ));
        }
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM platform_update_commands WHERE status IN ('queued','running','restoring','restore_required'))",
        ).fetch_one(&self.pool).await?;
        if pending {
            return Err(UpdaterError::Config(
                "candidate bootstrap requires no pending update or recovery command".into(),
            ));
        }
        require_runtime_apply_disabled("ai-image-factory-gateway.service").await?;
        require_runtime_apply_disabled("ai-image-factory-updater.service").await?;
        let (schema, successful): (i64, bool) = sqlx::query_as(
            "SELECT COALESCE(MAX(version),-1), COALESCE(BOOL_AND(success),FALSE) FROM _sqlx_migrations",
        ).fetch_one(&self.pool).await?;
        if !successful {
            return Err(UpdaterError::InvalidRelease(
                "migration history is not clean".into(),
            ));
        }
        let current = read_current_release(
            &self.config.release_root.join("current"),
            &self.config.release_root.join("releases"),
        )?;
        let listed = run_trusted(
            Path::new(SYSTEMCTL),
            [
                "list-units",
                "--all",
                "--plain",
                "--full",
                "--no-legend",
                "--no-pager",
                "--type=service",
                "ai-image-factory*",
            ],
            &BTreeMap::new(),
        )
        .await?;
        let text = String::from_utf8(listed.stdout)
            .map_err(|_| UpdaterError::Config("invalid service inventory".into()))?;
        let mut units: Vec<_> = text
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|unit| {
                *unit != "ai-image-factory-updater.service"
                    && !unit.starts_with("ai-image-factory-recovery-")
                    && !unit.starts_with("ai-image-factory-updater-recover@")
            })
            .collect();
        units.sort_unstable();
        if !units.contains(&"ai-image-factory-gateway.service")
            || units.iter().any(|unit| {
                !unit.starts_with("ai-image-factory-")
                    || !unit.ends_with(".service")
                    || !unit
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"._@:-".contains(&byte))
            })
        {
            return Err(UpdaterError::Config(
                "candidate bootstrap service inventory is incomplete".into(),
            ));
        }
        let mut args = vec![
            "show",
            "--no-pager",
            "--property=Id,MainPID,InvocationID,NRestarts,ActiveState,SubState",
        ];
        args.extend(units);
        let services = run_trusted(Path::new(SYSTEMCTL), args, &BTreeMap::new())
            .await?
            .stdout;
        Ok(Snapshot {
            current,
            schema,
            services,
        })
    }

    /// The existing trusted gh must verify this binary before invoking it.
    pub async fn bootstrap_candidate(&self) -> Result<(), UpdaterError> {
        if unsafe { libc::geteuid() } != 0
            || self.config.release_root != Path::new(DEFAULT_RELEASE_ROOT)
        {
            return Err(UpdaterError::Config(
                "bootstrap-candidate requires the root-managed production layout".into(),
            ));
        }
        let pin = self
            .config
            .candidate
            .as_ref()
            .ok_or_else(|| UpdaterError::Config("candidate pin required".into()))?;
        let maintenance = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.config.journal_root.join("candidate-maintenance.lock"))?;
        maintenance
            .try_lock_exclusive()
            .map_err(|_| UpdaterError::Config("candidate maintenance already active".into()))?;
        let host_lock = self.acquire_host_lock()?;
        let cluster = timeout(LOCK_TIMEOUT, DatabaseAdvisoryLock::acquire(&self.pool))
            .await
            .map_err(|_| UpdaterError::LeaseLost)??;
        let mut cluster_loss = cluster.loss_receiver();
        // Each acquire_key starts its own transaction on a separate pool connection.
        // Releasing the cluster transaction must not release the enqueue fence.
        let enqueue_lock = timeout(LOCK_TIMEOUT, async {
            let enqueue_key: i64 =
                sqlx::query_scalar("SELECT hashtextextended('platform-system-update',0)")
                    .fetch_one(&self.pool)
                    .await?;
            DatabaseAdvisoryLock::acquire_key(&self.pool, enqueue_key).await
        })
        .await
        .map_err(|_| UpdaterError::LeaseLost)??;
        let mut enqueue_loss = enqueue_lock.loss_receiver();
        let (before, previous, old_bytes, old_digest, staged) = tokio::select! {
            biased;
            _ = wait_for_loss(&mut cluster_loss) => return Err(UpdaterError::LeaseLost),
            _ = wait_for_loss(&mut enqueue_loss) => return Err(UpdaterError::LeaseLost),
            result = timeout(PREPARE_TIMEOUT, async {
        let before = self.candidate_idle_snapshot().await?;
        let previous = before.current.join("bin/updated");
        validate_trusted_executable(&previous)?;
        validate_trusted_executable(Path::new(FIXED))?;
        let old_bytes = tokio::fs::read(&previous).await?;
        let old_digest = sha256_hex(&old_bytes);
        verify_file_digest(Path::new(FIXED), &old_digest, old_bytes.len() as u64).await?;
        self.verify_candidate(pin).await?;
        let staged = self.stage_release(None, &pin.version).await?;
        self.verify_schema_contract(&staged.manifest).await?;
        if self.candidate_idle_snapshot().await? != before {
            return Err(UpdaterError::Config(
                "production changed during candidate preparation".into(),
            ));
        }
        let binary = staged
            .manifest
            .files
            .iter()
            .find(|file| file.path == "bin/updated")
            .ok_or_else(|| UpdaterError::InvalidRelease("candidate updater is missing".into()))?;
        verify_file_digest(Path::new("/proc/self/exe"), &binary.sha256, binary.bytes).await?;
                Ok::<_, UpdaterError>((before, previous, old_bytes, old_digest, staged))
            }) => result.map_err(|_| UpdaterError::Command("candidate preparation timed out".into()))??,
        };
        let binary = staged
            .manifest
            .files
            .iter()
            .find(|file| file.path == "bin/updated")
            .ok_or_else(|| UpdaterError::InvalidRelease("candidate updater is missing".into()))?;
        // recover-pending and the daemon need these locks. Never hold them over systemctl start.
        // The independent maintenance mutex and Gateway enqueue fence remain held.
        timeout(LOCK_TIMEOUT, cluster.release())
            .await
            .map_err(|_| UpdaterError::LeaseLost)??;
        drop(host_lock);
        let candidate = staged.release_dir.join("bin/updated");
        let mut replacement_started = false;
        let replacement = tokio::select! {
            biased;
            _ = wait_for_loss(&mut enqueue_loss) => Err(UpdaterError::LeaseLost),
            _ = tokio::time::sleep(HANDOFF_TIMEOUT) => Err(UpdaterError::Command("candidate handoff timed out".into())),
            result = async {
            replacement_started = true;
            run_trusted(Path::new(HELPER), [candidate.as_os_str()], &BTreeMap::new()).await?;
            verify_running_updater(&binary.sha256, binary.bytes, self.config.poll_interval).await?;
            if self.candidate_idle_snapshot().await? != before {
                return Err(UpdaterError::Config("bootstrap changed application current, schema or business services".into()));
            }
            Ok::<_, UpdaterError>(())
            } => result,
        };
        // A lost fence discovered while releasing it is a failed handoff too.
        let replacement = match replacement {
            Ok(()) => timeout(LOCK_TIMEOUT, enqueue_lock.release())
                .await
                .map_err(|_| UpdaterError::LeaseLost)
                .and_then(|result| result),
            Err(error) => Err(error),
        };
        if let Err(error) = replacement {
            if !replacement_started {
                return Err(error);
            }
            timeout(HANDOFF_TIMEOUT, async {
            run_trusted(Path::new(HELPER), [previous.as_os_str()], &BTreeMap::new())
                .await
                .map_err(|_| {
                    UpdaterError::RestoreRequired(
                        "fixed updater rollback failed; Apply must remain disabled".into(),
                    )
                })?;
            verify_running_updater(
                &old_digest,
                old_bytes.len() as u64,
                self.config.poll_interval,
            )
            .await?;
            if self.candidate_idle_snapshot().await? != before {
                return Err(UpdaterError::RestoreRequired("bootstrap rollback did not preserve application state; Apply must remain disabled".into()));
            }
            Ok::<_, UpdaterError>(())
            }).await.map_err(|_| UpdaterError::RestoreRequired("fixed updater rollback timed out; Apply must remain disabled".into()))??;
            return Err(error);
        }
        println!(
            "{}",
            json!({"source":"actions_candidate", "fixed_updater_verified":true,
            "application_unchanged":true, "current":before.current, "schema":before.schema,
            "candidate":pin.version, "pending_owner_check":true})
        );
        Ok(())
    }
}

async fn updater_identity(digest: &str, bytes: u64) -> Result<(u32, String), UpdaterError> {
    let output = run_trusted(
        Path::new(SYSTEMCTL),
        [
            "show",
            "ai-image-factory-updater.service",
            "--property=MainPID,ActiveState,SubState,NRestarts,InvocationID",
        ],
        &BTreeMap::new(),
    )
    .await?;
    let text = String::from_utf8(output.stdout)
        .map_err(|_| UpdaterError::Config("invalid updater state".into()))?;
    let fields: BTreeMap<_, _> = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    let pid: u32 = fields
        .get("MainPID")
        .and_then(|value| value.parse().ok())
        .filter(|pid| *pid > 0)
        .ok_or_else(|| UpdaterError::Config("updater has no live PID".into()))?;
    let invocation = fields
        .get("InvocationID")
        .filter(|id| id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| UpdaterError::Config("updater invocation missing".into()))?
        .to_string();
    if fields.get("ActiveState") != Some(&"active")
        || fields.get("SubState") != Some(&"running")
        || fields.get("NRestarts") != Some(&"0")
    {
        return Err(UpdaterError::Config(
            "fixed updater is not stably running".into(),
        ));
    }
    let executable = PathBuf::from(format!("/proc/{pid}/exe"));
    if std::fs::read_link(&executable)? != Path::new(FIXED) {
        return Err(UpdaterError::Config(
            "updater PID is not executing the fixed binary".into(),
        ));
    }
    verify_file_digest(&executable, digest, bytes).await?;
    verify_file_digest(Path::new(FIXED), digest, bytes).await?;
    Ok((pid, invocation))
}

async fn verify_running_updater(
    digest: &str,
    bytes: u64,
    interval: Duration,
) -> Result<(), UpdaterError> {
    let before = updater_identity(digest, bytes).await?;
    tokio::time::sleep(interval.saturating_mul(2) + Duration::from_secs(1)).await;
    if updater_identity(digest, bytes).await? != before {
        return Err(UpdaterError::Config(
            "updater invocation changed during bootstrap verification".into(),
        ));
    }
    let filter = format!("_SYSTEMD_INVOCATION_ID={}", before.1);
    let logs = run_trusted(
        Path::new("/usr/bin/journalctl"),
        ["--no-pager", "--quiet", "--output=cat", &filter],
        &BTreeMap::new(),
    )
    .await?;
    let messages = String::from_utf8_lossy(&logs.stdout);
    if messages.contains("ERROR") || messages.contains("system update pass failed") {
        return Err(UpdaterError::Config(
            "updater logged errors after lock handoff; owner Check not ready".into(),
        ));
    }
    Ok(())
}

async fn require_runtime_apply_disabled(unit: &str) -> Result<(), UpdaterError> {
    let output = run_trusted(
        Path::new(SYSTEMCTL),
        ["show", unit, "--value", "--property=MainPID"],
        &BTreeMap::new(),
    )
    .await?;
    let pid: u32 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| {
            UpdaterError::Config("maintenance requires live Gateway and updater".into())
        })?;
    // Never log or export the environment: inspect only the non-secret Apply flag.
    let mut file = std::fs::File::open(format!("/proc/{pid}/environ"))?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1024 * 1024
        || bytes.split(|byte| *byte == 0).any(|entry| {
            entry
                .strip_prefix(b"AIF_UPDATE_APPLY_ENABLED=")
                .is_some_and(|value| value != b"false")
        })
    {
        return Err(UpdaterError::Config(
            "running Gateway/updater must have Apply disabled before maintenance".into(),
        ));
    }
    Ok(())
}
