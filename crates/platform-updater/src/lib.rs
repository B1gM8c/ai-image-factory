use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsStr,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt, symlink},
    path::{Component, Path, PathBuf},
    process::{Output, Stdio},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{
    AssertSqlSafe, FromRow, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::watch,
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout},
};
use uuid::Uuid;

const DEFAULT_RELEASE_ROOT: &str = "/opt/ai-image-factory";
const DEFAULT_JOURNAL_ROOT: &str = "/var/lib/ai-image-factory/updater";
const DEFAULT_BACKUP_ROOT: &str = "/var/lib/ai-image-factory/backups";
pub const UPDATER_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(90);
const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_COMMAND_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_ARCHIVE_ENTRIES: usize = 20_000;
const MAX_RELEASE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_RELEASE_METADATA_BYTES: u64 = 16 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum UpdaterError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("release metadata is invalid: {0}")]
    InvalidRelease(String),
    #[error("trusted command failed: {0}")]
    Command(String),
    #[error("update lease was lost")]
    LeaseLost,
    #[error("update requires operator recovery: {0}")]
    RestoreRequired(String),
}

#[derive(Clone, Debug)]
pub struct UpdaterConfig {
    database_url: String,
    migration_database_url: Option<String>,
    database_schema: String,
    repository: String,
    target_triple: String,
    release_root: PathBuf,
    journal_root: PathBuf,
    gh_executable: PathBuf,
    tar_executable: PathBuf,
    artifact_root: PathBuf,
    backup_root: PathBuf,
    apply_enabled: bool,
    attestation_workflow: String,
    quiesce_hook: Option<PathBuf>,
    resume_hook: Option<PathBuf>,
    backup_hook: Option<PathBuf>,
    recover_hook: Option<PathBuf>,
    activate_hook: Option<PathBuf>,
    verify_hook: Option<PathBuf>,
    poll_interval: Duration,
    lease_duration: Duration,
}

impl UpdaterConfig {
    pub fn from_env() -> Result<Self, UpdaterError> {
        let database_url = required_env("AIF_UPDATER_DATABASE_URL")?;
        let database_schema =
            optional_env("GATEWAY_DATABASE_SCHEMA").unwrap_or_else(|| "public".to_string());
        validate_database_schema(&database_schema)?;
        let repository = validate_repository(required_env("AIF_UPDATE_GITHUB_REPOSITORY")?)?;
        let target_triple =
            validate_release_token(&required_env("AIF_RELEASE_TARGET")?, "AIF_RELEASE_TARGET")?;
        let release_root = absolute_env_path("AIF_RELEASE_ROOT", DEFAULT_RELEASE_ROOT)?;
        let journal_root = absolute_env_path("AIF_UPDATE_JOURNAL_ROOT", DEFAULT_JOURNAL_ROOT)?;
        let gh_executable = absolute_env_path("AIF_UPDATE_GH_EXECUTABLE", "/usr/bin/gh")?;
        let tar_executable = absolute_env_path("AIF_UPDATE_TAR_EXECUTABLE", "/usr/bin/tar")?;
        validate_trusted_executable(&gh_executable)?;
        validate_trusted_executable(&tar_executable)?;
        let artifact_root = absolute_env_path(
            "GATEWAY_ARTIFACT_ROOT",
            "/var/lib/ai-image-factory/artifacts",
        )?;
        let backup_root = absolute_env_path("AIF_BACKUP_ROOT", DEFAULT_BACKUP_ROOT)?;
        let apply_enabled = boolean_env("AIF_UPDATE_APPLY_ENABLED", false)?;
        let migration_database_url = optional_env("AIF_MIGRATOR_DATABASE_URL");
        if apply_enabled && migration_database_url.is_none() {
            return Err(UpdaterError::Config(
                "AIF_MIGRATOR_DATABASE_URL is required when automatic apply is enabled".to_string(),
            ));
        }
        let attestation_workflow =
            optional_env("AIF_UPDATE_ATTESTATION_WORKFLOW").unwrap_or_default();
        if apply_enabled && attestation_workflow.is_empty() {
            return Err(UpdaterError::Config(
                "AIF_UPDATE_ATTESTATION_WORKFLOW is required when automatic apply is enabled"
                    .to_string(),
            ));
        }
        let attestation_workflow = if attestation_workflow.is_empty() {
            String::new()
        } else {
            normalize_workflow_identity(&repository, &attestation_workflow)?
        };
        let quiesce_hook = optional_absolute_env_path("AIF_UPDATE_QUIESCE_HOOK")?;
        let resume_hook = optional_absolute_env_path("AIF_UPDATE_RESUME_HOOK")?;
        let backup_hook = optional_absolute_env_path("AIF_UPDATE_BACKUP_HOOK")?;
        let recover_hook = optional_absolute_env_path("AIF_UPDATE_RECOVER_HOOK")?;
        let activate_hook = optional_absolute_env_path("AIF_UPDATE_ACTIVATE_HOOK")?;
        let verify_hook = optional_absolute_env_path("AIF_UPDATE_VERIFY_HOOK")?;
        for hook in [
            &quiesce_hook,
            &resume_hook,
            &backup_hook,
            &recover_hook,
            &activate_hook,
            &verify_hook,
        ]
        .into_iter()
        .flatten()
        {
            validate_trusted_executable(hook)?;
        }
        Ok(Self {
            database_url,
            migration_database_url,
            database_schema,
            repository,
            target_triple,
            release_root,
            journal_root,
            gh_executable,
            tar_executable,
            artifact_root,
            backup_root,
            apply_enabled,
            attestation_workflow,
            quiesce_hook,
            resume_hook,
            backup_hook,
            recover_hook,
            activate_hook,
            verify_hook,
            poll_interval: duration_env("AIF_UPDATE_POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL)?,
            lease_duration: duration_env("AIF_UPDATE_LEASE_MS", DEFAULT_LEASE_DURATION)?,
        })
    }

    fn apply_hooks(&self) -> Result<ApplyHooks, UpdaterError> {
        Ok(ApplyHooks {
            quiesce: required_hook(&self.quiesce_hook, "AIF_UPDATE_QUIESCE_HOOK")?,
            resume: required_hook(&self.resume_hook, "AIF_UPDATE_RESUME_HOOK")?,
            backup: required_hook(&self.backup_hook, "AIF_UPDATE_BACKUP_HOOK")?,
            recover: required_hook(&self.recover_hook, "AIF_UPDATE_RECOVER_HOOK")?,
            activate: required_hook(&self.activate_hook, "AIF_UPDATE_ACTIVATE_HOOK")?,
            verify: required_hook(&self.verify_hook, "AIF_UPDATE_VERIFY_HOOK")?,
        })
    }
}

#[derive(Clone, Debug)]
struct ApplyHooks {
    quiesce: PathBuf,
    resume: PathBuf,
    backup: PathBuf,
    recover: PathBuf,
    activate: PathBuf,
    verify: PathBuf,
}

#[derive(Clone)]
pub struct Updater {
    config: Arc<UpdaterConfig>,
    pool: PgPool,
    owner_id: String,
}

impl Updater {
    pub async fn from_config(config: UpdaterConfig) -> Result<Self, UpdaterError> {
        let pool = connect_pool(&config.database_url, &config.database_schema).await?;
        Ok(Self {
            config: Arc::new(config),
            pool,
            owner_id: format!("updated-{}", Uuid::new_v4().simple()),
        })
    }

    pub async fn run(self) -> Result<(), UpdaterError> {
        tokio::fs::create_dir_all(&self.config.journal_root).await?;
        let mut poll = interval(self.config.poll_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = poll.tick() => {
                    if let Err(error) = self.run_once().await {
                        tracing::error!(?error, "system update pass failed");
                    }
                }
                _ = shutdown_signal() => return Ok(()),
            }
        }
    }

    pub async fn run_once(&self) -> Result<bool, UpdaterError> {
        let _host_lock = self.acquire_host_lock()?;
        if let Some(claim) = self.claim_expired_recovery().await? {
            let heartbeat = self.start_heartbeat(&claim);
            let result = self.execute_recovery_takeover(&claim).await;
            heartbeat.stop().await;
            if let Err(error) = result {
                tracing::error!(
                    command.id = %claim.command_id,
                    ?error,
                    "system update recovery takeover failed"
                );
            }
            return Ok(true);
        }
        let Some(claim) = self.claim_next().await? else {
            return Ok(false);
        };
        let heartbeat = self.start_heartbeat(&claim);
        let result = match claim.action.as_str() {
            "check" => self.execute_check(&claim).await,
            "apply" => self.execute_apply(&claim).await,
            action => Err(UpdaterError::InvalidRelease(format!(
                "unsupported persisted action {action}"
            ))),
        };
        heartbeat.stop().await;
        if let Err(error) = result {
            tracing::error!(command.id = %claim.command_id, ?error, "system update command failed");
            if !matches!(error, UpdaterError::RestoreRequired(_)) {
                self.fail_command(&claim, "update_failed", &error.to_string())
                    .await?;
            }
        }
        Ok(true)
    }

    pub async fn recover_command(&self, command_id: &str) -> Result<(), UpdaterError> {
        let command_id = Uuid::parse_str(command_id)
            .map_err(|_| UpdaterError::Config("recovery command id must be a UUID".to_string()))?;
        let _host_lock = self.acquire_host_lock()?;
        let claim = self
            .claim_manual_recovery(command_id)
            .await?
            .ok_or_else(|| {
                UpdaterError::RestoreRequired(
                    "the requested command is not awaiting operator recovery".to_string(),
                )
            })?;
        let heartbeat = self.start_heartbeat(&claim);
        let result = self.execute_recovery_takeover(&claim).await;
        heartbeat.stop().await;
        result
    }

    fn acquire_host_lock(&self) -> Result<std::fs::File, UpdaterError> {
        std::fs::create_dir_all(&self.config.journal_root)?;
        let path = self.config.journal_root.join("updated.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        file.try_lock_exclusive().map_err(|error| {
            UpdaterError::Config(format!(
                "another updater owns the host lock {}: {error}",
                path.display()
            ))
        })?;
        Ok(file)
    }

    fn migration_database_url(&self) -> &str {
        self.config
            .migration_database_url
            .as_deref()
            .expect("apply configuration validates AIF_MIGRATOR_DATABASE_URL")
    }

    async fn execute_check(&self, claim: &ClaimedCommand) -> Result<(), UpdaterError> {
        self.record_phase(claim, "preflight", "started", json!({}))
            .await?;
        let release = self.latest_release().await?;
        self.verify_release(&release.tag_name).await?;
        let staged = self.stage_release(claim, &release.tag_name).await?;
        self.verify_schema_contract(&staged.manifest).await?;
        self.record_phase(
            claim,
            "staged",
            "succeeded",
            json!({"manifest_sha256": staged.manifest_sha256}),
        )
        .await?;
        self.finish_check(claim, &release, &staged.manifest).await?;
        self.append_journal(
            claim,
            "verified",
            json!({"release": release.tag_name, "immutable": true}),
        )?;
        Ok(())
    }

    async fn execute_apply(&self, claim: &ClaimedCommand) -> Result<(), UpdaterError> {
        if !self.config.apply_enabled {
            return Err(UpdaterError::Config(
                "AIF_UPDATE_APPLY_ENABLED must be enabled before apply commands can run"
                    .to_string(),
            ));
        }
        let hooks = self.config.apply_hooks()?;
        let version = claim.target_version.as_deref().ok_or_else(|| {
            UpdaterError::InvalidRelease("apply command is missing target_version".to_string())
        })?;
        validate_release_token(version, "target_version")?;
        self.record_phase(
            claim,
            "preflight",
            "started",
            json!({"target_version": version}),
        )
        .await?;
        self.verify_release(version).await?;
        let staged = self.stage_release(claim, version).await?;
        self.verify_schema_contract(&staged.manifest).await?;
        self.record_phase(
            claim,
            "staged",
            "succeeded",
            json!({"manifest_sha256": staged.manifest_sha256}),
        )
        .await?;

        let current_link = self.config.release_root.join("current");
        let previous_release =
            read_current_release(&current_link, &self.config.release_root.join("releases"))?;
        let mut recovery = RecoveryDescriptor::new(claim, &staged, previous_release.clone());
        self.persist_recovery_descriptor(&recovery)?;
        self.assert_lease(claim).await?;
        self.record_phase(claim, "quiescing", "started", json!({}))
            .await?;
        let pre_migration_context = self.update_context(claim, &staged, None);
        if let Err(error) = run_hook(&hooks.quiesce, &pre_migration_context).await {
            return self
                .resume_previous(claim, &staged, &hooks, &previous_release, error)
                .await;
        }
        if let Err(error) = self
            .record_phase(claim, "quiesced", "succeeded", json!({}))
            .await
        {
            return self
                .resume_previous(claim, &staged, &hooks, &previous_release, error)
                .await;
        }

        let backup_token = match self.prepare_recovery_point(claim, &staged, &hooks).await {
            Ok(token) => token,
            Err(error) => {
                return self
                    .resume_previous(claim, &staged, &hooks, &previous_release, error)
                    .await;
            }
        };
        recovery.backup_token = Some(backup_token.clone());
        self.persist_recovery_descriptor(&recovery)?;
        if let Err(error) = self
            .record_phase(
                claim,
                "recovery_ready",
                "succeeded",
                json!({"backup_token_sha256": sha256_hex(backup_token.as_bytes())}),
            )
            .await
        {
            return self
                .resume_previous(claim, &staged, &hooks, &previous_release, error)
                .await;
        }

        if let Err(error) = self.assert_lease(claim).await {
            return self
                .resume_previous(claim, &staged, &hooks, &previous_release, error)
                .await;
        }
        let migration = run_trusted(
            &staged.release_dir.join("bin/factoryctl"),
            [OsStr::new("migrate")],
            &self.update_context(claim, &staged, Some(&backup_token)),
        )
        .await;
        if let Err(error) = migration {
            return self
                .recover_after_migration_boundary(
                    claim,
                    &staged,
                    &hooks,
                    &previous_release,
                    &backup_token,
                    error,
                )
                .await;
        }
        if let Err(error) = self
            .record_phase(claim, "migrated", "succeeded", json!({}))
            .await
        {
            return self
                .recover_after_migration_boundary(
                    claim,
                    &staged,
                    &hooks,
                    &previous_release,
                    &backup_token,
                    error,
                )
                .await;
        }

        let switch = match self.assert_lease(claim).await {
            Ok(()) => atomic_switch(&current_link, &staged.release_dir),
            Err(error) => Err(error),
        };
        if let Err(error) = switch {
            return self
                .recover_after_migration_boundary(
                    claim,
                    &staged,
                    &hooks,
                    &previous_release,
                    &backup_token,
                    error,
                )
                .await;
        }
        if let Err(error) = self
            .record_phase(claim, "switched", "succeeded", json!({}))
            .await
        {
            return self
                .recover_after_migration_boundary(
                    claim,
                    &staged,
                    &hooks,
                    &previous_release,
                    &backup_token,
                    error,
                )
                .await;
        }

        let activation = match self.assert_lease(claim).await {
            Ok(()) => run_hook(
                &hooks.activate,
                &self.update_context(claim, &staged, Some(&backup_token)),
            )
            .await
            .map(|_| ()),
            Err(error) => Err(error),
        };
        if let Err(error) = activation {
            return self
                .recover_after_migration_boundary(
                    claim,
                    &staged,
                    &hooks,
                    &previous_release,
                    &backup_token,
                    error,
                )
                .await;
        }
        let verification = match self.assert_lease(claim).await {
            Ok(()) => run_hook(
                &hooks.verify,
                &self.update_context(claim, &staged, Some(&backup_token)),
            )
            .await
            .map(|_| ()),
            Err(error) => Err(error),
        };
        if let Err(error) = verification {
            return self
                .recover_after_migration_boundary(
                    claim,
                    &staged,
                    &hooks,
                    &previous_release,
                    &backup_token,
                    error,
                )
                .await;
        }
        if let Err(error) = self.finish_apply(claim, &staged.manifest).await {
            return self
                .recover_after_migration_boundary(
                    claim,
                    &staged,
                    &hooks,
                    &previous_release,
                    &backup_token,
                    error,
                )
                .await;
        }
        if let Err(error) = self.remove_recovery_descriptor(claim) {
            tracing::error!(
                command.id = %claim.command_id,
                ?error,
                "system update succeeded but the recovery descriptor could not be removed"
            );
        }
        if let Err(error) = self.append_journal(
            claim,
            "verified",
            json!({"release": version, "commit": staged.manifest.commit_sha}),
        ) {
            tracing::error!(
                command.id = %claim.command_id,
                ?error,
                "system update succeeded but the local journal could not be appended"
            );
        }
        Ok(())
    }

    async fn prepare_recovery_point(
        &self,
        claim: &ClaimedCommand,
        staged: &StagedRelease,
        hooks: &ApplyHooks,
    ) -> Result<String, UpdaterError> {
        self.assert_lease(claim).await?;
        let backup = run_hook(&hooks.backup, &self.update_context(claim, staged, None)).await?;
        let backup_token = parse_backup_token(&backup.stdout)?;
        Ok(backup_token)
    }

    async fn resume_previous(
        &self,
        claim: &ClaimedCommand,
        staged: &StagedRelease,
        hooks: &ApplyHooks,
        previous_release: &Path,
        cause: UpdaterError,
    ) -> Result<(), UpdaterError> {
        let mut context = self.update_context(claim, staged, None);
        context.insert(
            "AIF_UPDATE_PREVIOUS_RELEASE".to_string(),
            previous_release.to_string_lossy().into_owned(),
        );
        match run_hook(&hooks.resume, &context).await {
            Ok(_) => {
                self.remove_recovery_descriptor(claim)?;
                Err(cause)
            }
            Err(resume_error) => {
                let message = format!(
                    "pre-migration update failed ({cause}) and old services could not resume: {resume_error}"
                );
                self.set_recovery_state(claim, "restore_required", "failed", &message)
                    .await?;
                Err(UpdaterError::RestoreRequired(message))
            }
        }
    }

    async fn verify_schema_contract(&self, manifest: &ReleaseManifest) -> Result<(), UpdaterError> {
        let (current_version, all_successful): (i64, bool) = sqlx::query_as(
            r#"
            SELECT COALESCE(MAX(version), -1),
                   COALESCE(BOOL_AND(success), FALSE)
            FROM _sqlx_migrations
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        if !all_successful {
            return Err(UpdaterError::InvalidRelease(
                "database migration history contains an unsuccessful migration".to_string(),
            ));
        }
        if current_version < manifest.min_schema_version
            || current_version > manifest.target_schema_version
        {
            return Err(UpdaterError::InvalidRelease(format!(
                "database schema {current_version} is outside release compatibility {}..={}",
                manifest.min_schema_version, manifest.target_schema_version
            )));
        }
        Ok(())
    }

    async fn recover_after_migration_boundary(
        &self,
        claim: &ClaimedCommand,
        staged: &StagedRelease,
        hooks: &ApplyHooks,
        previous_release: &Path,
        backup_token: &str,
        cause: UpdaterError,
    ) -> Result<(), UpdaterError> {
        self.mark_restoring(claim, &cause.to_string()).await?;
        self.append_journal(
            claim,
            "restoring",
            json!({
                "error": cause.to_string(),
                "previous_release": previous_release,
                "backup_token_sha256": sha256_hex(backup_token.as_bytes())
            }),
        )?;
        if let Err(recovery_error) =
            atomic_switch(&self.config.release_root.join("current"), previous_release)
        {
            self.mark_restore_required(claim, &cause, &recovery_error)
                .await?;
            return Err(UpdaterError::RestoreRequired(format!(
                "automatic recovery could not restore the previous release pointer: {recovery_error}"
            )));
        }
        let mut context = self.update_context(claim, staged, Some(backup_token));
        context.insert(
            "AIF_UPDATE_PREVIOUS_RELEASE".to_string(),
            previous_release.to_string_lossy().into_owned(),
        );
        match run_hook(&hooks.recover, &context).await {
            Ok(_) => {
                self.append_journal(
                    claim,
                    "restored",
                    json!({"previous_release": previous_release}),
                )?;
                self.mark_restored(claim, &cause.to_string()).await?;
                self.remove_recovery_descriptor(claim)?;
                Err(UpdaterError::RestoreRequired(format!(
                    "new release failed and the previous recovery point was restored: {cause}"
                )))
            }
            Err(recovery_error) => {
                self.append_journal(
                    claim,
                    "restore_required",
                    json!({
                        "update_error": cause.to_string(),
                        "recovery_error": recovery_error.to_string()
                    }),
                )?;
                self.mark_restore_required(claim, &cause, &recovery_error)
                    .await?;
                Err(UpdaterError::RestoreRequired(format!(
                    "automatic recovery failed: {recovery_error}"
                )))
            }
        }
    }

    async fn assert_lease(&self, claim: &ClaimedCommand) -> Result<(), UpdaterError> {
        let owns_lease = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM platform_update_commands
                WHERE command_id = $1
                  AND lease_owner = $2
                  AND lease_epoch = $3
                  AND status IN ('running', 'restoring')
                  AND lease_expires_at_ms >
                      floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            )
            "#,
        )
        .bind(claim.command_id)
        .bind(&self.owner_id)
        .bind(claim.lease_epoch)
        .fetch_one(&self.pool)
        .await?;
        if owns_lease {
            Ok(())
        } else {
            Err(UpdaterError::LeaseLost)
        }
    }

    fn update_context(
        &self,
        claim: &ClaimedCommand,
        staged: &StagedRelease,
        backup_token: Option<&str>,
    ) -> BTreeMap<String, String> {
        let mut context = hook_context(claim, staged, backup_token);
        context.insert(
            "DATABASE_URL".to_string(),
            self.migration_database_url().to_string(),
        );
        context.insert(
            "GATEWAY_DATABASE_SCHEMA".to_string(),
            self.config.database_schema.clone(),
        );
        context.insert(
            "GATEWAY_ARTIFACT_ROOT".to_string(),
            self.config.artifact_root.to_string_lossy().into_owned(),
        );
        context.insert(
            "AIF_BACKUP_ROOT".to_string(),
            self.config.backup_root.to_string_lossy().into_owned(),
        );
        context.insert("AIF_UPDATE_LEASE_OWNER".to_string(), self.owner_id.clone());
        context.insert(
            "AIF_UPDATE_LEASE_EPOCH".to_string(),
            claim.lease_epoch.to_string(),
        );
        context
    }

    async fn latest_release(&self) -> Result<GitHubRelease, UpdaterError> {
        let endpoint = format!("repos/{}/releases/latest", self.config.repository);
        let output = run_trusted(
            &self.config.gh_executable,
            [OsStr::new("api"), OsStr::new(&endpoint)],
            &github_environment(),
        )
        .await?;
        let release: GitHubRelease = serde_json::from_slice(&output.stdout)
            .map_err(|error| UpdaterError::InvalidRelease(error.to_string()))?;
        validate_github_release(&release, None)?;
        Ok(release)
    }

    async fn verify_release(&self, version: &str) -> Result<(), UpdaterError> {
        let endpoint = format!("repos/{}/releases/tags/{version}", self.config.repository);
        let output = run_trusted(
            &self.config.gh_executable,
            [OsStr::new("api"), OsStr::new(&endpoint)],
            &github_environment(),
        )
        .await?;
        let release: GitHubRelease = serde_json::from_slice(&output.stdout)
            .map_err(|error| UpdaterError::InvalidRelease(error.to_string()))?;
        validate_github_release(&release, Some(version))?;
        run_trusted(
            &self.config.gh_executable,
            [
                OsStr::new("release"),
                OsStr::new("verify"),
                OsStr::new(version),
                OsStr::new("--repo"),
                OsStr::new(&self.config.repository),
            ],
            &github_environment(),
        )
        .await?;
        Ok(())
    }

    async fn stage_release(
        &self,
        claim: &ClaimedCommand,
        version: &str,
    ) -> Result<StagedRelease, UpdaterError> {
        tokio::fs::create_dir_all(self.config.release_root.join("staging")).await?;
        tokio::fs::create_dir_all(self.config.release_root.join("releases")).await?;
        let temp = tempfile::Builder::new()
            .prefix("update-")
            .tempdir_in(self.config.release_root.join("staging"))?;
        let manifest_name = format!(
            "ai-image-factory-{version}-{}.manifest.json",
            self.config.target_triple
        );
        let bundle_name = format!(
            "ai-image-factory-{version}-{}.tar.gz",
            self.config.target_triple
        );
        run_trusted(
            &self.config.gh_executable,
            [
                OsStr::new("release"),
                OsStr::new("download"),
                OsStr::new(version),
                OsStr::new("--repo"),
                OsStr::new(&self.config.repository),
                OsStr::new("--pattern"),
                OsStr::new(&manifest_name),
                OsStr::new("--pattern"),
                OsStr::new(&bundle_name),
                OsStr::new("--dir"),
                temp.path().as_os_str(),
            ],
            &github_environment(),
        )
        .await?;
        let manifest_path = temp.path().join(&manifest_name);
        let bundle_path = temp.path().join(&bundle_name);
        let manifest_metadata = tokio::fs::metadata(&manifest_path).await?;
        if manifest_metadata.len() == 0 || manifest_metadata.len() > MAX_MANIFEST_BYTES {
            return Err(UpdaterError::InvalidRelease(
                "release manifest size is outside the supported range".to_string(),
            ));
        }
        run_trusted(
            &self.config.gh_executable,
            [
                OsStr::new("release"),
                OsStr::new("verify-asset"),
                OsStr::new(version),
                manifest_path.as_os_str(),
                OsStr::new("--repo"),
                OsStr::new(&self.config.repository),
            ],
            &github_environment(),
        )
        .await?;
        let manifest_bytes = tokio::fs::read(&manifest_path).await?;
        let manifest: ReleaseManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|error| UpdaterError::InvalidRelease(error.to_string()))?;
        manifest.validate(version, &self.config.target_triple)?;

        let source_ref = format!("refs/tags/{version}");
        for asset in [&manifest_path, &bundle_path] {
            if asset != &manifest_path {
                run_trusted(
                    &self.config.gh_executable,
                    [
                        OsStr::new("release"),
                        OsStr::new("verify-asset"),
                        OsStr::new(version),
                        asset.as_os_str(),
                        OsStr::new("--repo"),
                        OsStr::new(&self.config.repository),
                    ],
                    &github_environment(),
                )
                .await?;
            }
            run_trusted(
                &self.config.gh_executable,
                [
                    OsStr::new("attestation"),
                    OsStr::new("verify"),
                    asset.as_os_str(),
                    OsStr::new("--repo"),
                    OsStr::new(&self.config.repository),
                    OsStr::new("--signer-workflow"),
                    OsStr::new(&self.config.attestation_workflow),
                    OsStr::new("--source-ref"),
                    OsStr::new(&source_ref),
                    OsStr::new("--source-digest"),
                    OsStr::new(&manifest.commit_sha),
                    OsStr::new("--deny-self-hosted-runners"),
                ],
                &github_environment(),
            )
            .await?;
        }
        verify_file_digest(&bundle_path, &manifest.bundle_sha256, manifest.bundle_bytes).await?;
        validate_archive(&self.config.tar_executable, &bundle_path).await?;

        let unpacked = temp.path().join("unpacked");
        tokio::fs::create_dir(&unpacked).await?;
        run_trusted(
            &self.config.tar_executable,
            [
                OsStr::new("-xzf"),
                bundle_path.as_os_str(),
                OsStr::new("-C"),
                unpacked.as_os_str(),
                OsStr::new("--no-same-owner"),
                OsStr::new("--no-same-permissions"),
            ],
            &BTreeMap::new(),
        )
        .await?;
        normalize_release_permissions(&unpacked, &manifest)?;
        verify_release_tree(&unpacked, &manifest).await?;
        verify_release_identity(&unpacked, &manifest).await?;

        let releases_dir = self.config.release_root.join("releases");
        let release_dir = releases_dir.join(version);
        if tokio::fs::try_exists(&release_dir).await? {
            verify_release_tree(&release_dir, &manifest).await?;
            verify_release_identity(&release_dir, &manifest).await?;
        } else {
            sync_tree(&unpacked)?;
            tokio::fs::rename(&unpacked, &release_dir).await?;
            sync_directory(&releases_dir)?;
        }
        let manifest_sha256 = sha256_hex(&manifest_bytes);
        self.append_journal(
            claim,
            "staged",
            json!({
                "release_dir": release_dir,
                "manifest_sha256": manifest_sha256
            }),
        )?;
        Ok(StagedRelease {
            _temp: temp,
            release_dir,
            manifest,
            manifest_sha256,
        })
    }

    async fn claim_next(&self) -> Result<Option<ClaimedCommand>, UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let lease_expires = now + duration_ms(self.config.lease_duration)?;
        let command = sqlx::query_as::<_, ClaimedCommand>(
            r#"
            WITH candidate AS (
                SELECT command_id
                FROM platform_update_commands
                WHERE status = 'queued'
                   OR (
                       status = 'running'
                       AND phase IN ('queued', 'preflight', 'staged')
                       AND lease_expires_at_ms < $1
                   )
                ORDER BY requested_at_ms, command_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE platform_update_commands command
            SET status = 'running',
                phase = CASE
                    WHEN command.phase = 'queued' THEN 'preflight'
                    ELSE command.phase
                END,
                lease_owner = $2,
                lease_epoch = command.lease_epoch + 1,
                lease_expires_at_ms = $3,
                attempt_count = command.attempt_count + 1,
                started_at_ms = COALESCE(command.started_at_ms, $1),
                updated_at_ms = $1
            FROM candidate
            WHERE command.command_id = candidate.command_id
            RETURNING command.command_id, command.action, command.target_version,
                      command.lease_epoch
            "#,
        )
        .bind(now)
        .bind(&self.owner_id)
        .bind(lease_expires)
        .fetch_optional(&self.pool)
        .await?;
        Ok(command)
    }

    async fn claim_expired_recovery(&self) -> Result<Option<ClaimedCommand>, UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let lease_expires = now + duration_ms(self.config.lease_duration)?;
        let command = sqlx::query_as::<_, ClaimedCommand>(
            r#"
            WITH candidate AS (
                SELECT command_id
                FROM platform_update_commands
                WHERE action = 'apply'
                  AND status IN ('running', 'restoring')
                  AND phase IN (
                      'quiescing', 'quiesced', 'recovery_ready', 'migrated',
                      'switched', 'restoring'
                  )
                  AND lease_expires_at_ms < $1
                ORDER BY requested_at_ms, command_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE platform_update_commands command
            SET status = 'restoring',
                phase = 'restoring',
                failure_code = 'expired_unsafe_update_lease',
                failure_message =
                    'Updater lease expired after the release crossed a mutation boundary',
                lease_owner = $2,
                lease_epoch = command.lease_epoch + 1,
                lease_expires_at_ms = $3,
                attempt_count = command.attempt_count + 1,
                updated_at_ms = $1
            FROM candidate
            WHERE command.command_id = candidate.command_id
            RETURNING command.command_id, command.action, command.target_version,
                      command.lease_epoch
            "#,
        )
        .bind(now)
        .bind(&self.owner_id)
        .bind(lease_expires)
        .fetch_optional(&self.pool)
        .await?;
        Ok(command)
    }

    async fn claim_manual_recovery(
        &self,
        command_id: Uuid,
    ) -> Result<Option<ClaimedCommand>, UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let lease_expires = now + duration_ms(self.config.lease_duration)?;
        let command = sqlx::query_as::<_, ClaimedCommand>(
            r#"
            UPDATE platform_update_commands
            SET status = 'restoring',
                phase = 'restoring',
                failure_code = NULL,
                failure_message = NULL,
                lease_owner = $2,
                lease_epoch = lease_epoch + 1,
                lease_expires_at_ms = $3,
                attempt_count = attempt_count + 1,
                updated_at_ms = $1
            WHERE command_id = $4
              AND action = 'apply'
              AND status = 'restore_required'
            RETURNING command_id, action, target_version, lease_epoch
            "#,
        )
        .bind(now)
        .bind(&self.owner_id)
        .bind(lease_expires)
        .bind(command_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(command)
    }

    async fn execute_recovery_takeover(&self, claim: &ClaimedCommand) -> Result<(), UpdaterError> {
        let result = self.try_recovery_takeover(claim).await;
        if let Err(error) = result {
            let message = format!("automatic recovery takeover failed: {error}");
            self.set_recovery_state(claim, "restore_required", "failed", &message)
                .await?;
            self.append_journal(
                claim,
                "restore_required",
                json!({"reason": "automatic_recovery_takeover_failed", "error": error.to_string()}),
            )?;
            return Err(UpdaterError::RestoreRequired(message));
        }
        Ok(())
    }

    async fn try_recovery_takeover(&self, claim: &ClaimedCommand) -> Result<(), UpdaterError> {
        let hooks = self.config.apply_hooks()?;
        let descriptor = self.read_recovery_descriptor(claim)?;
        descriptor.validate(claim, &self.config)?;
        verify_release_tree(&descriptor.release_dir, &descriptor.manifest).await?;
        verify_release_identity(&descriptor.release_dir, &descriptor.manifest).await?;
        atomic_switch(
            &self.config.release_root.join("current"),
            &descriptor.previous_release,
        )?;

        let context = self.recovery_update_context(claim, &descriptor);
        if descriptor.backup_token.is_some() {
            run_hook(&hooks.recover, &context).await?;
        } else {
            run_hook(&hooks.resume, &context).await?;
        }
        self.append_journal(
            claim,
            "restored",
            json!({
                "reason": "expired_update_lease",
                "previous_release": descriptor.previous_release,
                "used_backup": descriptor.backup_token.is_some()
            }),
        )?;
        self.mark_restored(
            claim,
            "Updater restarted after an expired unsafe lease and restored the previous release",
        )
        .await?;
        self.remove_recovery_descriptor(claim)?;
        Ok(())
    }

    fn recovery_update_context(
        &self,
        claim: &ClaimedCommand,
        descriptor: &RecoveryDescriptor,
    ) -> BTreeMap<String, String> {
        let mut context = hook_context_parts(
            claim,
            &descriptor.release_dir,
            &descriptor.manifest,
            descriptor.backup_token.as_deref(),
        );
        context.insert(
            "DATABASE_URL".to_string(),
            self.migration_database_url().to_string(),
        );
        context.insert(
            "GATEWAY_DATABASE_SCHEMA".to_string(),
            self.config.database_schema.clone(),
        );
        context.insert(
            "GATEWAY_ARTIFACT_ROOT".to_string(),
            self.config.artifact_root.to_string_lossy().into_owned(),
        );
        context.insert(
            "AIF_BACKUP_ROOT".to_string(),
            self.config.backup_root.to_string_lossy().into_owned(),
        );
        context.insert(
            "AIF_UPDATE_PREVIOUS_RELEASE".to_string(),
            descriptor.previous_release.to_string_lossy().into_owned(),
        );
        context.insert("AIF_UPDATE_LEASE_OWNER".to_string(), self.owner_id.clone());
        context.insert(
            "AIF_UPDATE_LEASE_EPOCH".to_string(),
            claim.lease_epoch.to_string(),
        );
        context
    }

    fn recovery_descriptor_path(&self, claim: &ClaimedCommand) -> PathBuf {
        self.config
            .journal_root
            .join("recovery")
            .join(format!("{}.json", claim.command_id))
    }

    fn persist_recovery_descriptor(
        &self,
        descriptor: &RecoveryDescriptor,
    ) -> Result<(), UpdaterError> {
        let directory = self.config.journal_root.join("recovery");
        std::fs::create_dir_all(&directory)?;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        let final_path = directory.join(format!("{}.json", descriptor.command_id));
        let temporary_path =
            directory.join(format!(".{}.{}.tmp", descriptor.command_id, Uuid::new_v4()));
        let bytes = serde_json::to_vec(descriptor)
            .map_err(|error| UpdaterError::InvalidRelease(error.to_string()))?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_MANIFEST_BYTES + MAX_RELEASE_METADATA_BYTES
        {
            return Err(UpdaterError::InvalidRelease(
                "recovery descriptor size is outside the supported range".to_string(),
            ));
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary_path, &final_path)?;
        sync_directory(&directory)?;
        Ok(())
    }

    fn read_recovery_descriptor(
        &self,
        claim: &ClaimedCommand,
    ) -> Result<RecoveryDescriptor, UpdaterError> {
        let path = self.recovery_descriptor_path(claim);
        let metadata = std::fs::symlink_metadata(&path)?;
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || (metadata.uid() != 0 && metadata.uid() != effective_uid)
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.len() == 0
            || metadata.len() > MAX_MANIFEST_BYTES + MAX_RELEASE_METADATA_BYTES
        {
            return Err(UpdaterError::InvalidRelease(
                "recovery descriptor is not a protected regular file".to_string(),
            ));
        }
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| UpdaterError::InvalidRelease(error.to_string()))
    }

    fn remove_recovery_descriptor(&self, claim: &ClaimedCommand) -> Result<(), UpdaterError> {
        let path = self.recovery_descriptor_path(claim);
        match std::fs::remove_file(path) {
            Ok(()) => sync_directory(&self.config.journal_root.join("recovery")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn start_heartbeat(&self, claim: &ClaimedCommand) -> Heartbeat {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let pool = self.pool.clone();
        let owner = self.owner_id.clone();
        let command_id = claim.command_id;
        let lease_epoch = claim.lease_epoch;
        let lease_duration = self.config.lease_duration;
        let mut ticker = interval((lease_duration / 3).max(Duration::from_secs(1)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let Ok(now) = database_now_ms(&pool).await else {
                            continue;
                        };
                        let Ok(expires) = duration_ms(lease_duration).map(|lease| now + lease) else {
                            continue;
                        };
                        let heartbeat = sqlx::query(
                            r#"
                            UPDATE platform_update_commands
                            SET lease_expires_at_ms = $1, updated_at_ms = $2
                            WHERE command_id = $3
                              AND lease_owner = $4
                              AND lease_epoch = $5
                              AND status IN ('running', 'restoring')
                            "#,
                        )
                        .bind(expires)
                        .bind(now)
                        .bind(command_id)
                        .bind(&owner)
                        .bind(lease_epoch)
                        .execute(&pool)
                        .await;
                        match heartbeat {
                            Ok(result) if result.rows_affected() == 1 => {}
                            Ok(_) => {
                                tracing::error!(%command_id, "system update lease heartbeat lost ownership");
                                break;
                            }
                            Err(error) => {
                                tracing::error!(%command_id, ?error, "system update lease heartbeat failed");
                                break;
                            }
                        }
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        Heartbeat { stop_tx, task }
    }

    async fn record_phase(
        &self,
        claim: &ClaimedCommand,
        phase: &str,
        outcome: &str,
        details: Value,
    ) -> Result<(), UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            UPDATE platform_update_commands
            SET phase = $1,
                progress = progress || $2,
                updated_at_ms = $3
            WHERE command_id = $4
              AND lease_owner = $5
              AND lease_epoch = $6
              AND status = 'running'
            "#,
        )
        .bind(phase)
        .bind(&details)
        .bind(now)
        .bind(claim.command_id)
        .bind(&self.owner_id)
        .bind(claim.lease_epoch)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(UpdaterError::LeaseLost);
        }
        sqlx::query(
            r#"
            INSERT INTO platform_update_events(
                event_id, command_id, phase, outcome, details, created_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(claim.command_id)
        .bind(phase)
        .bind(outcome)
        .bind(details)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        if let Err(error) = self.append_journal(claim, phase, json!({"outcome": outcome})) {
            tracing::error!(
                command.id = %claim.command_id,
                ?error,
                phase,
                "system update database event committed but local journal append failed"
            );
        }
        Ok(())
    }

    async fn finish_check(
        &self,
        claim: &ClaimedCommand,
        release: &GitHubRelease,
        manifest: &ReleaseManifest,
    ) -> Result<(), UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE platform_release_state
            SET latest_version = $1,
                latest_commit_sha = $2,
                latest_verified = TRUE,
                last_checked_at_ms = $3,
                last_error_code = NULL,
                last_error_message = NULL,
                updated_at_ms = $3
            WHERE singleton = TRUE
            "#,
        )
        .bind(&release.tag_name)
        .bind(&manifest.commit_sha)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let result = sqlx::query(
            r#"
            UPDATE platform_update_commands
            SET status = 'succeeded', phase = 'verified',
                completed_at_ms = $1, updated_at_ms = $1,
                lease_owner = NULL, lease_expires_at_ms = NULL,
                progress = progress || $2
            WHERE command_id = $3
              AND lease_owner = $4
              AND lease_epoch = $5
            "#,
        )
        .bind(now)
        .bind(json!({"latest_version": release.tag_name, "immutable": true}))
        .bind(claim.command_id)
        .bind(&self.owner_id)
        .bind(claim.lease_epoch)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(UpdaterError::LeaseLost);
        }
        tx.commit().await?;
        Ok(())
    }

    async fn finish_apply(
        &self,
        claim: &ClaimedCommand,
        manifest: &ReleaseManifest,
    ) -> Result<(), UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            UPDATE platform_release_state
            SET previous_version = current_version,
                previous_commit_sha = current_commit_sha,
                current_version = $1,
                current_commit_sha = $2,
                latest_version = $1,
                latest_commit_sha = $2,
                latest_verified = TRUE,
                last_applied_at_ms = $3,
                last_error_code = NULL,
                last_error_message = NULL,
                updated_at_ms = $3
            WHERE singleton = TRUE
            "#,
        )
        .bind(&manifest.release_version)
        .bind(&manifest.commit_sha)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let result = sqlx::query(
            r#"
            UPDATE platform_update_commands
            SET status = 'succeeded', phase = 'verified',
                completed_at_ms = $1, updated_at_ms = $1,
                lease_owner = NULL, lease_expires_at_ms = NULL,
                progress = progress || $2
            WHERE command_id = $3
              AND lease_owner = $4
              AND lease_epoch = $5
            "#,
        )
        .bind(now)
        .bind(json!({
            "release_version": manifest.release_version,
            "commit_sha": manifest.commit_sha,
            "migration_version": manifest.migration_version
        }))
        .bind(claim.command_id)
        .bind(&self.owner_id)
        .bind(claim.lease_epoch)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(UpdaterError::LeaseLost);
        }
        tx.commit().await?;
        Ok(())
    }

    async fn fail_command(
        &self,
        claim: &ClaimedCommand,
        code: &str,
        message: &str,
    ) -> Result<(), UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let result = sqlx::query(
            r#"
            UPDATE platform_update_commands
            SET status = 'failed', phase = 'failed',
                failure_code = $1, failure_message = $2,
                completed_at_ms = $3, updated_at_ms = $3,
                lease_owner = NULL, lease_expires_at_ms = NULL
            WHERE command_id = $4
              AND lease_owner = $5
              AND lease_epoch = $6
            "#,
        )
        .bind(code)
        .bind(limit_message(message))
        .bind(now)
        .bind(claim.command_id)
        .bind(&self.owner_id)
        .bind(claim.lease_epoch)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(UpdaterError::LeaseLost);
        }
        sqlx::query(
            r#"
            UPDATE platform_release_state
            SET last_error_code = $1, last_error_message = $2, updated_at_ms = $3
            WHERE singleton = TRUE
            "#,
        )
        .bind(code)
        .bind(limit_message(message))
        .bind(now)
        .execute(&self.pool)
        .await?;
        self.append_journal(claim, "failed", json!({"error": message}))?;
        Ok(())
    }

    async fn mark_restoring(
        &self,
        claim: &ClaimedCommand,
        message: &str,
    ) -> Result<(), UpdaterError> {
        self.set_recovery_state(claim, "restoring", "restoring", message)
            .await
    }

    async fn mark_restored(
        &self,
        claim: &ClaimedCommand,
        message: &str,
    ) -> Result<(), UpdaterError> {
        self.set_recovery_state(claim, "restored", "restored", message)
            .await
    }

    async fn mark_restore_required(
        &self,
        claim: &ClaimedCommand,
        update_error: &UpdaterError,
        recovery_error: &UpdaterError,
    ) -> Result<(), UpdaterError> {
        self.set_recovery_state(
            claim,
            "restore_required",
            "failed",
            &format!("update: {update_error}; recovery: {recovery_error}"),
        )
        .await
    }

    async fn set_recovery_state(
        &self,
        claim: &ClaimedCommand,
        status: &str,
        phase: &str,
        message: &str,
    ) -> Result<(), UpdaterError> {
        let now = database_now_ms(&self.pool).await?;
        let result = sqlx::query(
            r#"
            UPDATE platform_update_commands
            SET status = $1, phase = $2,
                failure_code = 'post_migration_update_failed',
                failure_message = $3, updated_at_ms = $4,
                completed_at_ms = CASE
                    WHEN $1 IN ('restored', 'restore_required') THEN $4
                    ELSE completed_at_ms
                END,
                lease_owner = CASE WHEN $1 = 'restoring' THEN lease_owner ELSE NULL END,
                lease_expires_at_ms = CASE
                    WHEN $1 = 'restoring' THEN lease_expires_at_ms
                    ELSE NULL
                END
            WHERE command_id = $5
              AND lease_owner = $6
              AND lease_epoch = $7
            "#,
        )
        .bind(status)
        .bind(phase)
        .bind(limit_message(message))
        .bind(now)
        .bind(claim.command_id)
        .bind(&self.owner_id)
        .bind(claim.lease_epoch)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(UpdaterError::LeaseLost);
        }
        Ok(())
    }

    fn append_journal(
        &self,
        claim: &ClaimedCommand,
        phase: &str,
        details: Value,
    ) -> Result<(), UpdaterError> {
        std::fs::create_dir_all(&self.config.journal_root)?;
        let path = self.config.journal_root.join("events.jsonl");
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let entry = JournalEntry {
            command_id: claim.command_id,
            action: &claim.action,
            target_version: claim.target_version.as_deref(),
            phase,
            details,
            created_at_ms: local_now_ms()?,
        };
        serde_json::to_writer(&mut file, &entry)
            .map_err(|error| UpdaterError::InvalidRelease(error.to_string()))?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }
}

struct Heartbeat {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Heartbeat {
    async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.task.await;
    }
}

#[derive(Clone, Debug, FromRow)]
struct ClaimedCommand {
    command_id: Uuid,
    action: String,
    target_version: Option<String>,
    lease_epoch: i64,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    draft: bool,
    prerelease: bool,
    #[serde(default)]
    immutable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReleaseIdentity {
    schema_version: u32,
    release_version: String,
    commit_sha: String,
    target_triple: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub schema_version: u32,
    pub updater_protocol_version: u32,
    pub release_version: String,
    pub commit_sha: String,
    pub target_triple: String,
    pub migration_version: i64,
    pub min_schema_version: i64,
    pub target_schema_version: i64,
    pub rollback_mode: String,
    pub bundle_sha256: String,
    pub bundle_bytes: u64,
    pub files: Vec<ReleaseFile>,
}

impl ReleaseManifest {
    fn validate(&self, version: &str, target_triple: &str) -> Result<(), UpdaterError> {
        if self.schema_version != 1 {
            return Err(UpdaterError::InvalidRelease(
                "unsupported release manifest schema".to_string(),
            ));
        }
        if self.updater_protocol_version != UPDATER_PROTOCOL_VERSION {
            return Err(UpdaterError::InvalidRelease(
                "release requires an unsupported updater protocol".to_string(),
            ));
        }
        if self.release_version != version {
            return Err(UpdaterError::InvalidRelease(
                "manifest release version does not match the requested release".to_string(),
            ));
        }
        if self.target_triple != target_triple {
            return Err(UpdaterError::InvalidRelease(
                "manifest target does not match this updater".to_string(),
            ));
        }
        if !is_full_commit_sha(&self.commit_sha) {
            return Err(UpdaterError::InvalidRelease(
                "manifest commit_sha is invalid".to_string(),
            ));
        }
        if self.min_schema_version < 0
            || self.target_schema_version < self.min_schema_version
            || self.migration_version != self.target_schema_version
            || self.rollback_mode != "backup_restore"
        {
            return Err(UpdaterError::InvalidRelease(
                "manifest schema compatibility contract is invalid".to_string(),
            ));
        }
        validate_sha256(&self.bundle_sha256)?;
        if self.bundle_bytes == 0 || self.bundle_bytes > MAX_RELEASE_BYTES {
            return Err(UpdaterError::InvalidRelease(
                "bundle size is outside the supported range".to_string(),
            ));
        }
        if self.files.is_empty() || self.files.len() > MAX_ARCHIVE_ENTRIES {
            return Err(UpdaterError::InvalidRelease(
                "manifest file count is outside the supported range".to_string(),
            ));
        }
        let mut paths = BTreeSet::new();
        let mut total_bytes = 0_u64;
        for file in &self.files {
            validate_relative_path(Path::new(&file.path))?;
            validate_sha256(&file.sha256)?;
            validate_release_file_mode(&file.path, file.mode)?;
            total_bytes = total_bytes.checked_add(file.bytes).ok_or_else(|| {
                UpdaterError::InvalidRelease("manifest file sizes overflow".to_string())
            })?;
            if file.bytes > MAX_RELEASE_BYTES
                || total_bytes > MAX_RELEASE_BYTES
                || !paths.insert(file.path.as_str())
            {
                return Err(UpdaterError::InvalidRelease(
                    "manifest contains an invalid or duplicate file".to_string(),
                ));
            }
        }
        for required in [
            "bin/gpt-image-2-gateway",
            "bin/factoryctl",
            "bin/workerd",
            "bin/executord",
            "bin/reducerd",
            "bin/reconcilerd",
            "bin/provider-submitd",
            "bin/provider-pollerd",
            "bin/webhookd",
            "bin/codex-runner",
            "bin/grok-runner",
            "bin/remote-submit-runner",
            "bin/updated",
            "admin/server.js",
            "release.json",
        ] {
            if !paths.contains(required) {
                return Err(UpdaterError::InvalidRelease(format!(
                    "manifest is missing {required}"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReleaseFile {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub mode: u32,
}

struct StagedRelease {
    _temp: TempDir,
    release_dir: PathBuf,
    manifest: ReleaseManifest,
    manifest_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecoveryDescriptor {
    schema_version: u32,
    command_id: Uuid,
    target_version: String,
    previous_release: PathBuf,
    release_dir: PathBuf,
    manifest: ReleaseManifest,
    backup_token: Option<String>,
}

impl RecoveryDescriptor {
    fn new(claim: &ClaimedCommand, staged: &StagedRelease, previous_release: PathBuf) -> Self {
        Self {
            schema_version: 1,
            command_id: claim.command_id,
            target_version: staged.manifest.release_version.clone(),
            previous_release,
            release_dir: staged.release_dir.clone(),
            manifest: staged.manifest.clone(),
            backup_token: None,
        }
    }

    fn validate(&self, claim: &ClaimedCommand, config: &UpdaterConfig) -> Result<(), UpdaterError> {
        if self.schema_version != 1
            || self.command_id != claim.command_id
            || claim.target_version.as_deref() != Some(self.target_version.as_str())
        {
            return Err(UpdaterError::InvalidRelease(
                "recovery descriptor does not match the claimed command".to_string(),
            ));
        }
        self.manifest
            .validate(&self.target_version, &config.target_triple)?;
        if let Some(token) = &self.backup_token {
            validate_backup_token(token)?;
        }
        let releases_root = std::fs::canonicalize(config.release_root.join("releases"))?;
        let previous_release = std::fs::canonicalize(&self.previous_release)?;
        let release_dir = std::fs::canonicalize(&self.release_dir)?;
        if previous_release.parent() != Some(releases_root.as_path())
            || release_dir.parent() != Some(releases_root.as_path())
            || release_dir.file_name() != Some(OsStr::new(&self.target_version))
        {
            return Err(UpdaterError::InvalidRelease(
                "recovery descriptor references an unmanaged release path".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct JournalEntry<'a> {
    command_id: Uuid,
    action: &'a str,
    target_version: Option<&'a str>,
    phase: &'a str,
    details: Value,
    created_at_ms: i64,
}

async fn connect_pool(database_url: &str, schema: &str) -> Result<PgPool, UpdaterError> {
    let options = PgConnectOptions::from_str(database_url)
        .map_err(|error| UpdaterError::Config(format!("invalid DATABASE_URL: {error}")))?;
    let search_path = format!("SET search_path TO \"{}\"", schema.replace('"', "\"\""));
    Ok(PgPoolOptions::new()
        .max_connections(3)
        .after_connect(move |connection, _| {
            let search_path = search_path.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(search_path))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?)
}

async fn database_now_ms(pool: &PgPool) -> Result<i64, UpdaterError> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT",
    )
    .fetch_one(pool)
    .await?)
}

async fn run_hook(
    executable: &Path,
    context: &BTreeMap<String, String>,
) -> Result<Output, UpdaterError> {
    validate_trusted_executable(executable)?;
    run_trusted(executable, std::iter::empty::<&OsStr>(), context).await
}

async fn run_trusted<I, S>(
    executable: &Path,
    args: I,
    context: &BTreeMap<String, String>,
) -> Result<Output, UpdaterError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    validate_trusted_executable(executable)?;
    let mut command = Command::new(executable);
    command
        .args(args)
        .env_clear()
        .envs(context)
        .process_group(0)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let process_group = child.id().ok_or_else(|| {
        UpdaterError::Command(format!(
            "{} did not expose a child process id",
            executable.display()
        ))
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        UpdaterError::Command("trusted command stdout was not captured".to_string())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        UpdaterError::Command("trusted command stderr was not captured".to_string())
    })?;
    let output = match timeout(DEFAULT_COMMAND_TIMEOUT, async {
        tokio::try_join!(
            child.wait(),
            read_bounded_output(stdout),
            read_bounded_output(stderr)
        )
    })
    .await
    {
        Ok(Ok((status, stdout, stderr))) => Output {
            status,
            stdout,
            stderr,
        },
        Ok(Err(error)) => {
            terminate_process_group(process_group);
            let _ = child.wait().await;
            return Err(UpdaterError::Command(format!(
                "{} output could not be captured safely: {error}",
                executable.display()
            )));
        }
        Err(_) => {
            terminate_process_group(process_group);
            let _ = child.wait().await;
            return Err(UpdaterError::Command(format!(
                "{} exceeded the trusted command timeout",
                executable.display()
            )));
        }
    };
    if !output.status.success() {
        tracing::error!(
            executable = %executable.display(),
            status = %output.status,
            stderr_bytes = output.stderr.len(),
            stderr_sha256 = %sha256_hex(&output.stderr),
            "trusted command exited unsuccessfully"
        );
        return Err(UpdaterError::Command(format!(
            "{} exited unsuccessfully with {}",
            executable.display(),
            output.status
        )));
    }
    Ok(output)
}

async fn read_bounded_output<R>(mut reader: R) -> std::io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > MAX_COMMAND_OUTPUT_BYTES {
            return Err(std::io::Error::other(
                "trusted command output exceeded the configured limit",
            ));
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

fn terminate_process_group(process_group: u32) {
    let Ok(process_group) = i32::try_from(process_group) else {
        tracing::error!(process_group, "trusted command process id overflow");
        return;
    };
    let kill_result = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    if kill_result != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            tracing::error!(
                ?error,
                process_group,
                "failed to terminate trusted command process group"
            );
        }
    }
}

fn github_environment() -> BTreeMap<String, String> {
    [
        "GH_TOKEN",
        "GH_ENTERPRISE_TOKEN",
        "GH_HOST",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    ]
    .into_iter()
    .filter_map(|name| env::var(name).ok().map(|value| (name.to_string(), value)))
    .chain([
        (
            "HOME".to_string(),
            "/var/lib/ai-image-factory/updater".to_string(),
        ),
        ("LANG".to_string(), "C.UTF-8".to_string()),
        ("LC_ALL".to_string(), "C.UTF-8".to_string()),
        ("NO_COLOR".to_string(), "1".to_string()),
    ])
    .collect()
}

async fn validate_archive(tar: &Path, bundle: &Path) -> Result<(), UpdaterError> {
    let names = run_trusted(
        tar,
        [OsStr::new("-tzf"), bundle.as_os_str()],
        &BTreeMap::new(),
    )
    .await?;
    let names = String::from_utf8(names.stdout)
        .map_err(|_| UpdaterError::InvalidRelease("archive paths are not UTF-8".to_string()))?;
    let paths: Vec<_> = names.lines().collect();
    if paths.is_empty() || paths.len() > MAX_ARCHIVE_ENTRIES {
        return Err(UpdaterError::InvalidRelease(
            "archive entry count is outside the supported range".to_string(),
        ));
    }
    for path in paths {
        validate_relative_path(Path::new(path.trim_end_matches('/')))?;
    }
    let verbose = run_trusted(
        tar,
        [OsStr::new("-tvzf"), bundle.as_os_str()],
        &BTreeMap::new(),
    )
    .await?;
    let verbose = String::from_utf8(verbose.stdout)
        .map_err(|_| UpdaterError::InvalidRelease("archive listing is not UTF-8".to_string()))?;
    for line in verbose.lines() {
        if !matches!(line.as_bytes().first(), Some(b'-' | b'd')) {
            return Err(UpdaterError::InvalidRelease(
                "archive contains links or special files".to_string(),
            ));
        }
    }
    Ok(())
}

async fn verify_release_tree(root: &Path, manifest: &ReleaseManifest) -> Result<(), UpdaterError> {
    let root_metadata = std::fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(UpdaterError::InvalidRelease(
            "release root must be a regular directory".to_string(),
        ));
    }
    let mut actual = BTreeMap::new();
    collect_regular_files(root, root, &mut actual)?;
    if actual.len() != manifest.files.len() {
        return Err(UpdaterError::InvalidRelease(
            "release tree contains missing or extra files".to_string(),
        ));
    }
    for expected in &manifest.files {
        let path = root.join(&expected.path);
        let (actual_size, actual_mode) = actual.get(&expected.path).ok_or_else(|| {
            UpdaterError::InvalidRelease(format!("release is missing {}", expected.path))
        })?;
        verify_file_digest(&path, &expected.sha256, expected.bytes).await?;
        if *actual_size != expected.bytes {
            return Err(UpdaterError::InvalidRelease(format!(
                "release file size mismatch for {}",
                expected.path
            )));
        }
        if *actual_mode != expected.mode {
            return Err(UpdaterError::InvalidRelease(format!(
                "release file mode mismatch for {}",
                expected.path
            )));
        }
    }
    Ok(())
}

async fn verify_release_identity(
    root: &Path,
    manifest: &ReleaseManifest,
) -> Result<(), UpdaterError> {
    let path = root.join("release.json");
    let metadata = std::fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > MAX_RELEASE_METADATA_BYTES
    {
        return Err(UpdaterError::InvalidRelease(
            "release.json must be a bounded regular file".to_string(),
        ));
    }
    let bytes = tokio::fs::read(path).await?;
    let identity: ReleaseIdentity = serde_json::from_slice(&bytes)
        .map_err(|error| UpdaterError::InvalidRelease(error.to_string()))?;
    if identity.schema_version != 1
        || identity.release_version != manifest.release_version
        || identity.commit_sha != manifest.commit_sha
        || identity.target_triple != manifest.target_triple
    {
        return Err(UpdaterError::InvalidRelease(
            "release.json does not match the verified manifest".to_string(),
        ));
    }
    Ok(())
}

fn collect_regular_files(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<String, (u64, u32)>,
) -> Result<(), UpdaterError> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            return Err(UpdaterError::InvalidRelease(
                "release tree contains links or special files".to_string(),
            ));
        }
        let mode = metadata.permissions().mode();
        if mode & 0o6000 != 0 {
            return Err(UpdaterError::InvalidRelease(
                "release tree contains setuid or setgid permissions".to_string(),
            ));
        }
        if metadata.is_dir() {
            collect_regular_files(root, &path, files)?;
        } else {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| UpdaterError::InvalidRelease("release path escaped root".to_string()))?
                .to_string_lossy()
                .replace('\\', "/");
            validate_relative_path(Path::new(&relative))?;
            files.insert(relative, (metadata.len(), mode & 0o7777));
        }
    }
    Ok(())
}

fn validate_release_file_mode(path: &str, mode: u32) -> Result<(), UpdaterError> {
    let expected = if path.starts_with("bin/") {
        0o755
    } else {
        0o644
    };
    if mode != expected {
        return Err(UpdaterError::InvalidRelease(format!(
            "release file {path} must use mode {expected:o}"
        )));
    }
    Ok(())
}

fn normalize_release_permissions(
    root: &Path,
    manifest: &ReleaseManifest,
) -> Result<(), UpdaterError> {
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755))?;
    for entry in walk_release_tree(root)? {
        let metadata = std::fs::symlink_metadata(&entry)?;
        if metadata.is_dir() {
            std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o755))?;
        }
    }
    for file in &manifest.files {
        validate_release_file_mode(&file.path, file.mode)?;
        std::fs::set_permissions(
            root.join(&file.path),
            std::fs::Permissions::from_mode(file.mode),
        )?;
    }
    Ok(())
}

fn walk_release_tree(root: &Path) -> Result<Vec<PathBuf>, UpdaterError> {
    let mut pending = vec![root.to_path_buf()];
    let mut entries = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
                return Err(UpdaterError::InvalidRelease(
                    "release tree contains links or special files".to_string(),
                ));
            }
            if metadata.is_dir() {
                pending.push(path.clone());
            }
            entries.push(path);
        }
    }
    Ok(entries)
}

async fn verify_file_digest(
    path: &Path,
    expected_sha256: &str,
    expected_bytes: u64,
) -> Result<(), UpdaterError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut digest = Sha256::new();
    let mut actual_bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        actual_bytes = actual_bytes
            .checked_add(
                u64::try_from(read)
                    .map_err(|_| UpdaterError::InvalidRelease("file size overflow".to_string()))?,
            )
            .ok_or_else(|| UpdaterError::InvalidRelease("file size overflow".to_string()))?;
        if actual_bytes > expected_bytes || actual_bytes > MAX_RELEASE_BYTES {
            return Err(UpdaterError::InvalidRelease(format!(
                "file size mismatch for {}",
                path.display()
            )));
        }
        digest.update(&buffer[..read]);
    }
    if actual_bytes != expected_bytes || hex::encode(digest.finalize()) != expected_sha256 {
        return Err(UpdaterError::InvalidRelease(format!(
            "digest mismatch for {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), UpdaterError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(UpdaterError::InvalidRelease(
            "release path must be relative".to_string(),
        ));
    }
    for component in path.components() {
        match component {
            Component::Normal(value)
                if !value.is_empty()
                    && value
                        .as_encoded_bytes()
                        .iter()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"._-/@".contains(byte)) => {}
            _ => {
                return Err(UpdaterError::InvalidRelease(
                    "release path contains unsupported components".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_trusted_executable(path: &Path) -> Result<(), UpdaterError> {
    if !path.is_absolute() {
        return Err(UpdaterError::Config(format!(
            "trusted executable must be absolute: {}",
            path.display()
        )));
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        UpdaterError::Config(format!(
            "trusted executable {} cannot be inspected: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(UpdaterError::Config(format!(
            "trusted executable must be a regular non-writable file: {}",
            path.display()
        )));
    }
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != 0 && metadata.uid() != effective_uid {
        return Err(UpdaterError::Config(format!(
            "trusted executable must be owned by root or the updater user: {}",
            path.display()
        )));
    }
    let mut parent = path.parent();
    while let Some(directory) = parent {
        let metadata = std::fs::symlink_metadata(directory).map_err(|error| {
            UpdaterError::Config(format!(
                "trusted executable parent {} cannot be inspected: {error}",
                directory.display()
            ))
        })?;
        if metadata.file_type().is_symlink()
            || !metadata.is_dir()
            || metadata.permissions().mode() & 0o022 != 0
            || (metadata.uid() != 0 && metadata.uid() != effective_uid)
        {
            return Err(UpdaterError::Config(format!(
                "trusted executable parent is not protected: {}",
                directory.display()
            )));
        }
        parent = directory.parent();
    }
    Ok(())
}

fn atomic_switch(current: &Path, target: &Path) -> Result<(), UpdaterError> {
    if !target.is_absolute() || !target.is_dir() {
        return Err(UpdaterError::InvalidRelease(
            "release switch target is not an absolute directory".to_string(),
        ));
    }
    let parent = current
        .parent()
        .ok_or_else(|| UpdaterError::InvalidRelease("current symlink has no parent".to_string()))?;
    let temporary = parent.join(format!(".current-{}", Uuid::new_v4().simple()));
    symlink(target, &temporary)?;
    std::fs::rename(&temporary, current)?;
    sync_directory(parent)?;
    Ok(())
}

fn read_current_release(current: &Path, releases_root: &Path) -> Result<PathBuf, UpdaterError> {
    let metadata = std::fs::symlink_metadata(current)?;
    if !metadata.file_type().is_symlink() {
        return Err(UpdaterError::InvalidRelease(
            "current release pointer is not a symlink".to_string(),
        ));
    }
    let target = std::fs::read_link(current)?;
    let absolute = if target.is_absolute() {
        target
    } else {
        current
            .parent()
            .ok_or_else(|| UpdaterError::InvalidRelease("invalid current symlink".to_string()))?
            .join(target)
    };
    if !absolute.is_dir() {
        return Err(UpdaterError::InvalidRelease(
            "current release target does not exist".to_string(),
        ));
    }
    let releases_root = std::fs::canonicalize(releases_root)?;
    let absolute = std::fs::canonicalize(absolute)?;
    if absolute.parent() != Some(releases_root.as_path()) {
        return Err(UpdaterError::InvalidRelease(
            "current release target is outside the managed releases directory".to_string(),
        ));
    }
    Ok(absolute)
}

fn sync_tree(path: &Path) -> Result<(), UpdaterError> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        let metadata = std::fs::symlink_metadata(&child)?;
        if metadata.is_dir() {
            sync_tree(&child)?;
        } else if metadata.is_file() {
            OpenOptions::new().read(true).open(&child)?.sync_all()?;
        } else {
            return Err(UpdaterError::InvalidRelease(
                "release tree contains links or special files".to_string(),
            ));
        }
    }
    sync_directory(path)
}

fn sync_directory(path: &Path) -> Result<(), UpdaterError> {
    let directory = OpenOptions::new().read(true).open(path)?;
    directory.sync_all()?;
    Ok(())
}

fn hook_context(
    claim: &ClaimedCommand,
    staged: &StagedRelease,
    backup_token: Option<&str>,
) -> BTreeMap<String, String> {
    hook_context_parts(claim, &staged.release_dir, &staged.manifest, backup_token)
}

fn hook_context_parts(
    claim: &ClaimedCommand,
    release_dir: &Path,
    manifest: &ReleaseManifest,
    backup_token: Option<&str>,
) -> BTreeMap<String, String> {
    let mut context = BTreeMap::from([
        (
            "AIF_UPDATE_COMMAND_ID".to_string(),
            claim.command_id.to_string(),
        ),
        (
            "AIF_UPDATE_RELEASE_VERSION".to_string(),
            manifest.release_version.clone(),
        ),
        (
            "AIF_UPDATE_RELEASE_COMMIT".to_string(),
            manifest.commit_sha.clone(),
        ),
        (
            "AIF_UPDATE_RELEASE_DIR".to_string(),
            release_dir.to_string_lossy().into_owned(),
        ),
        (
            "AIF_UPDATE_MIGRATION_VERSION".to_string(),
            manifest.migration_version.to_string(),
        ),
    ]);
    if let Some(token) = backup_token {
        context.insert("AIF_UPDATE_BACKUP_TOKEN".to_string(), token.to_string());
    }
    context
}

fn parse_backup_token(stdout: &[u8]) -> Result<String, UpdaterError> {
    let value = String::from_utf8(stdout.to_vec())
        .map_err(|_| UpdaterError::Command("backup hook output is not UTF-8".to_string()))?;
    let token = value.lines().next().unwrap_or_default().trim().to_string();
    validate_backup_token(&token)?;
    Ok(token)
}

fn validate_backup_token(token: &str) -> Result<(), UpdaterError> {
    if token.is_empty()
        || token.len() > 512
        || !token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(UpdaterError::Command(
            "backup hook must print one opaque visible-ASCII recovery token".to_string(),
        ));
    }
    Ok(())
}

fn validate_repository(value: String) -> Result<String, UpdaterError> {
    let parts: Vec<_> = value.split('/').collect();
    if parts.len() != 2
        || parts.iter().any(|part| {
            part.is_empty()
                || matches!(*part, "." | "..")
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
    {
        return Err(UpdaterError::Config(
            "AIF_UPDATE_GITHUB_REPOSITORY must use owner/repository".to_string(),
        ));
    }
    Ok(value)
}

fn validate_release_token(value: &str, name: &str) -> Result<String, UpdaterError> {
    if value.is_empty()
        || value.len() > 100
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(UpdaterError::InvalidRelease(format!(
            "{name} contains unsupported characters"
        )));
    }
    Ok(value.to_string())
}

fn normalize_workflow_identity(repository: &str, value: &str) -> Result<String, UpdaterError> {
    let repository_prefix = format!("{repository}/");
    let workflow_path = value.strip_prefix(&repository_prefix).unwrap_or(value);
    if workflow_path.len() > 200
        || !workflow_path
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
        || !(workflow_path.ends_with(".yml") || workflow_path.ends_with(".yaml"))
        || !workflow_path.starts_with(".github/workflows/")
        || workflow_path.starts_with('/')
        || workflow_path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
        || (value.contains('/') && value != workflow_path && !value.starts_with(&repository_prefix))
    {
        return Err(UpdaterError::Config(
            "AIF_UPDATE_ATTESTATION_WORKFLOW must identify a workflow in the configured repository"
                .to_string(),
        ));
    }
    Ok(format!("{repository_prefix}{workflow_path}"))
}

fn validate_github_release(
    release: &GitHubRelease,
    expected_tag: Option<&str>,
) -> Result<(), UpdaterError> {
    validate_release_token(&release.tag_name, "release tag")?;
    if release.draft || release.prerelease || !release.immutable {
        return Err(UpdaterError::InvalidRelease(
            "release must be published, non-prerelease, and immutable".to_string(),
        ));
    }
    if expected_tag.is_some_and(|tag| tag != release.tag_name) {
        return Err(UpdaterError::InvalidRelease(
            "GitHub release tag does not match the requested version".to_string(),
        ));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), UpdaterError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(UpdaterError::InvalidRelease(
            "SHA-256 digest is invalid".to_string(),
        ));
    }
    Ok(())
}

fn is_full_commit_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_database_schema(value: &str) -> Result<(), UpdaterError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(UpdaterError::Config(
            "GATEWAY_DATABASE_SCHEMA is invalid".to_string(),
        ));
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, UpdaterError> {
    optional_env(name).ok_or_else(|| UpdaterError::Config(format!("{name} is required")))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn boolean_env(name: &str, default: bool) -> Result<bool, UpdaterError> {
    let Some(value) = optional_env(name) else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(UpdaterError::Config(format!("{name} must be a boolean"))),
    }
}

fn absolute_env_path(name: &str, default: &str) -> Result<PathBuf, UpdaterError> {
    let path = PathBuf::from(optional_env(name).unwrap_or_else(|| default.to_string()));
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(UpdaterError::Config(format!("{name} must be absolute")))
    }
}

fn optional_absolute_env_path(name: &str) -> Result<Option<PathBuf>, UpdaterError> {
    optional_env(name)
        .map(|value| {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                Ok(path)
            } else {
                Err(UpdaterError::Config(format!("{name} must be absolute")))
            }
        })
        .transpose()
}

fn required_hook(path: &Option<PathBuf>, name: &str) -> Result<PathBuf, UpdaterError> {
    path.clone().ok_or_else(|| {
        UpdaterError::Config(format!("{name} is required before apply commands can run"))
    })
}

fn duration_env(name: &str, default: Duration) -> Result<Duration, UpdaterError> {
    let Some(value) = optional_env(name) else {
        return Ok(default);
    };
    let millis = value
        .parse::<u64>()
        .map_err(|_| UpdaterError::Config(format!("{name} must be milliseconds")))?;
    if millis == 0 {
        return Err(UpdaterError::Config(format!("{name} must be positive")));
    }
    Ok(Duration::from_millis(millis))
}

fn duration_ms(value: Duration) -> Result<i64, UpdaterError> {
    i64::try_from(value.as_millis())
        .map_err(|_| UpdaterError::Config("duration exceeds supported range".to_string()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex::encode(digest.finalize())
}

fn limit_message(value: &str) -> &str {
    let mut end = value.len().min(2_000);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn local_now_ms() -> Result<i64, UpdaterError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| UpdaterError::Config("system clock is before unix epoch".to_string()))?
        .as_millis();
    i64::try_from(millis)
        .map_err(|_| UpdaterError::Config("system clock exceeds supported range".to_string()))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_manifest() -> ReleaseManifest {
        ReleaseManifest {
            schema_version: 1,
            updater_protocol_version: 1,
            release_version: "v1.2.3".to_string(),
            commit_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
            target_triple: "x86_64-unknown-linux-gnu".to_string(),
            migration_version: 114,
            min_schema_version: 113,
            target_schema_version: 114,
            rollback_mode: "backup_restore".to_string(),
            bundle_sha256: "a".repeat(64),
            bundle_bytes: 42,
            files: vec![
                release_file("bin/gpt-image-2-gateway"),
                release_file("bin/factoryctl"),
                release_file("bin/workerd"),
                release_file("bin/executord"),
                release_file("bin/reducerd"),
                release_file("bin/reconcilerd"),
                release_file("bin/provider-submitd"),
                release_file("bin/provider-pollerd"),
                release_file("bin/webhookd"),
                release_file("bin/codex-runner"),
                release_file("bin/grok-runner"),
                release_file("bin/remote-submit-runner"),
                release_file("bin/updated"),
                release_file("admin/server.js"),
                release_file("release.json"),
            ],
        }
    }

    fn release_file(path: &str) -> ReleaseFile {
        ReleaseFile {
            path: path.to_string(),
            sha256: "b".repeat(64),
            bytes: 1,
            mode: if path.starts_with("bin/") {
                0o755
            } else {
                0o644
            },
        }
    }

    #[test]
    fn manifest_binds_version_target_and_required_files() {
        valid_manifest()
            .validate("v1.2.3", "x86_64-unknown-linux-gnu")
            .unwrap();
        assert!(
            valid_manifest()
                .validate("v1.2.4", "x86_64-unknown-linux-gnu")
                .is_err()
        );
        assert!(
            valid_manifest()
                .validate("v1.2.3", "aarch64-unknown-linux-gnu")
                .is_err()
        );
        let mut short_commit = valid_manifest();
        short_commit.commit_sha = "0123456".to_string();
        assert!(
            short_commit
                .validate("v1.2.3", "x86_64-unknown-linux-gnu")
                .is_err()
        );
    }

    #[test]
    fn release_paths_reject_traversal_and_links() {
        for invalid in [
            "",
            "/etc/passwd",
            "../escape",
            "bin/../../escape",
            "./bin/app",
        ] {
            assert!(
                validate_relative_path(Path::new(invalid)).is_err(),
                "{invalid}"
            );
        }
        validate_relative_path(Path::new("bin/gpt-image-2-gateway")).unwrap();
        validate_relative_path(Path::new("admin/.next/static/app.js")).unwrap();
    }

    #[test]
    fn backup_token_is_bounded_and_shell_opaque() {
        assert_eq!(parse_backup_token(b"backup-123\n").unwrap(), "backup-123");
        assert!(parse_backup_token(b"\n").is_err());
        assert!(parse_backup_token(b"token with spaces\n").is_err());
    }

    #[test]
    fn workflow_identity_is_pinned_to_the_configured_repository() {
        assert_eq!(
            normalize_workflow_identity("owner/repository", ".github/workflows/release.yml")
                .unwrap(),
            "owner/repository/.github/workflows/release.yml"
        );
        assert_eq!(
            normalize_workflow_identity(
                "owner/repository",
                "owner/repository/.github/workflows/release.yaml"
            )
            .unwrap(),
            "owner/repository/.github/workflows/release.yaml"
        );
        assert!(
            normalize_workflow_identity(
                "owner/repository",
                "other/repository/.github/workflows/release.yml"
            )
            .is_err()
        );
        assert!(normalize_workflow_identity("owner/repository", "release.yml").is_err());
    }

    #[test]
    fn github_release_must_be_exact_and_immutable() {
        let release = GitHubRelease {
            tag_name: "v1.2.3".to_string(),
            draft: false,
            prerelease: false,
            immutable: true,
        };
        validate_github_release(&release, Some("v1.2.3")).unwrap();
        assert!(validate_github_release(&release, Some("v1.2.4")).is_err());

        let mutable = GitHubRelease {
            immutable: false,
            ..release
        };
        assert!(validate_github_release(&mutable, Some("v1.2.3")).is_err());
    }
}
