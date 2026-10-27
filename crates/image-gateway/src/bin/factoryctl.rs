use std::{
    env, fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use uuid::Uuid;

use gpt_image_2_gateway::executor::provision_grok_video_v2_execution_profile;
use gpt_image_2_gateway::{
    BlockedTerminalRequeueError, CodexExecutionProfileProvisioning, CodexProfileProvisioningError,
    DreaminaComplianceRejectionResolutionRequest, DreaminaImageSuccessResolutionRequest,
    DreaminaManualVideoCancellationRequest, DreaminaSupplementVideoCancellationRequest,
    DreaminaVideoSuccessResolutionRequest, DreaminaVideoSupplementAuthorizationRequest,
    ImageGatewayError, OperationalCredentialRefresher, PostgresExecutionSettlementStore,
    PostgresExecutorTerminalStore, PostgresReconciliationStore, codex_auth_file_sha256,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, run_migrations, verify_migrations,
    },
    dispatch_dreamina_video_supplement_operator, grok_auth_file_sha256,
    poll_dreamina_video_supplement_operator, prepare_dreamina_video_supplement_operator,
    prepare_dreamina_video_supplement_successor_operator,
    provider_management::{PostgresProviderManagementService, ProviderManagementService},
    provision_codex_cli_edit_execution_profile, provision_codex_execution_profile,
    provision_grok_edit_execution_profile_replacement, provision_grok_execution_profile,
    provision_grok_image_execution_profile_replacement, provision_grok_video_execution_profile,
    provision_grok_video_execution_profile_replacement, reconcile_execution_profile_routes,
    reconcile_inline_customer_settlement, recover_dreamina_video_supplement_acceptance_operator,
    resolve_dreamina_compliance_rejection, resolve_dreamina_image_success,
    resolve_dreamina_video_success,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Migrate,
    VerifyMigrations,
    ProvisionCodexProfile,
    ProvisionCodexCliEditProfile,
    ProvisionGrokProfile,
    ProvisionGrokVideoProfile,
    ProvisionGrokVideoV2Profile,
    ProvisionGrokVideoProfileReplacement {
        source_profile_key: String,
        replacement_profile_key: String,
    },
    ProvisionGrokImageProfileReplacement {
        source_profile_key: String,
        replacement_profile_key: String,
    },
    ProvisionGrokEditProfileReplacement {
        source_profile_key: String,
        replacement_profile_key: String,
    },
    ReconcileExecutionProfileRoutes,
    ReconcileDreaminaProfiles,
    ReconcileInlineCustomerSettlement {
        job_id: Uuid,
    },
    ResolveDreaminaComplianceRejection {
        journal_root: PathBuf,
        request: DreaminaComplianceRejectionResolutionRequest,
    },
    ResolveDreaminaImageSuccess {
        evidence_root: PathBuf,
        request: DreaminaImageSuccessResolutionRequest,
    },
    ResolveDreaminaVideoSuccess {
        evidence_root: PathBuf,
        request: DreaminaVideoSuccessResolutionRequest,
    },
    ConfirmDreaminaVideoCancellation {
        evidence_root: PathBuf,
        request: DreaminaManualVideoCancellationRequest,
    },
    ConfirmDreaminaVideoSupplementCancellation {
        evidence_root: PathBuf,
        request: DreaminaSupplementVideoCancellationRequest,
    },
    PrepareDreaminaVideoSupplement {
        evidence_root: PathBuf,
        request: DreaminaVideoSupplementAuthorizationRequest,
    },
    PrepareDreaminaVideoSupplementSuccessor {
        previous_authorization_id: Uuid,
        evidence_root: PathBuf,
        request: DreaminaVideoSupplementAuthorizationRequest,
    },
    DispatchDreaminaVideoSupplement {
        evidence_root: PathBuf,
        authorization_id: Uuid,
    },
    PollDreaminaVideoSupplement {
        evidence_root: PathBuf,
        authorization_id: Uuid,
    },
    RecoverDreaminaVideoSupplementAcceptance {
        authorization_id: Uuid,
        evidence_root: PathBuf,
        recovery_root: PathBuf,
        reviewed_recovery_sha256: String,
    },
    TerminalizeUnstartedJob {
        job_id: Uuid,
    },
    CancelUnlaunchedJob {
        job_id: Uuid,
    },
    RefreshProviderQuota {
        provider_account_id: Uuid,
    },
    RefreshCodexCredential {
        provider_account_id: Uuid,
        expected_revision: i64,
        expected_fingerprint_sha256: String,
    },
    RetryBlockedTerminal {
        submission_id: Uuid,
        repair_revision: String,
    },
    BootstrapAdmin {
        email: String,
        display_name: String,
    },
}

fn parse_command<I, S>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut args = args.into_iter();
    let command = args.next().ok_or_else(|| {
        "missing command: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-codex-cli-edit-profile`, `provision-grok-profile`, `provision-grok-video-profile`, `provision-grok-video-v2-profile`, `provision-grok-image-profile-replacement`, `provision-grok-edit-profile-replacement`, `provision-grok-video-profile-replacement`, `reconcile-execution-profile-routes`, `reconcile-dreamina-profiles`, `reconcile-inline-customer-settlement`, `terminalize-unstarted-job`, `cancel-unlaunched-job`, `refresh-provider-quota`, `refresh-codex-credential`, `resolve-dreamina-compliance-rejection`, `resolve-dreamina-image-success`, `resolve-dreamina-video-success`, `confirm-dreamina-video-cancellation`, `confirm-dreamina-video-supplement-cancellation`, `prepare-dreamina-video-supplement`, `prepare-dreamina-video-supplement-successor`, `dispatch-dreamina-video-supplement`, `poll-dreamina-video-supplement`, `recover-dreamina-video-supplement-acceptance`, or `retry-blocked-terminal`"
            .to_string()
    })?;
    let command = match command.as_ref() {
        "migrate" => Command::Migrate,
        "verify-migrations" => Command::VerifyMigrations,
        "bootstrap-admin" => {
            let email = required_argument(&mut args, "bootstrap-admin", "email")?;
            let display_name = required_argument(&mut args, "bootstrap-admin", "display name")?;
            Command::BootstrapAdmin {
                email,
                display_name,
            }
        }
        "provision-codex-profile" => Command::ProvisionCodexProfile,
        "provision-codex-cli-edit-profile" => Command::ProvisionCodexCliEditProfile,
        "provision-grok-profile" => Command::ProvisionGrokProfile,
        "provision-grok-video-profile" => Command::ProvisionGrokVideoProfile,
        "provision-grok-video-v2-profile" => Command::ProvisionGrokVideoV2Profile,
        "provision-grok-video-profile-replacement" => {
            let source_profile_key = required_argument(
                &mut args,
                "provision-grok-video-profile-replacement",
                "source profile key",
            )?;
            let replacement_profile_key = required_argument(
                &mut args,
                "provision-grok-video-profile-replacement",
                "replacement profile key",
            )?;
            Command::ProvisionGrokVideoProfileReplacement {
                source_profile_key,
                replacement_profile_key,
            }
        }
        "provision-grok-image-profile-replacement" => {
            Command::ProvisionGrokImageProfileReplacement {
                source_profile_key: required_argument(
                    &mut args,
                    "provision-grok-image-profile-replacement",
                    "source profile key",
                )?,
                replacement_profile_key: required_argument(
                    &mut args,
                    "provision-grok-image-profile-replacement",
                    "replacement profile key",
                )?,
            }
        }
        "provision-grok-edit-profile-replacement" => Command::ProvisionGrokEditProfileReplacement {
            source_profile_key: required_argument(
                &mut args,
                "provision-grok-edit-profile-replacement",
                "source profile key",
            )?,
            replacement_profile_key: required_argument(
                &mut args,
                "provision-grok-edit-profile-replacement",
                "replacement profile key",
            )?,
        },
        "reconcile-execution-profile-routes" => Command::ReconcileExecutionProfileRoutes,
        "reconcile-dreamina-profiles" => Command::ReconcileDreaminaProfiles,
        "reconcile-inline-customer-settlement" => {
            let job_id =
                required_argument(&mut args, "reconcile-inline-customer-settlement", "job id")?
                    .parse()
                    .map_err(|_| {
                        "reconcile-inline-customer-settlement requires a UUID job id".to_string()
                    })?;
            Command::ReconcileInlineCustomerSettlement { job_id }
        }
        "resolve-dreamina-compliance-rejection" => {
            let mut identity = |name: &str| -> Result<Uuid, String> {
                required_argument(&mut args, "resolve-dreamina-compliance-rejection", name)?
                    .parse()
                    .map_err(|_| {
                        format!("resolve-dreamina-compliance-rejection requires a UUID {name}")
                    })
            };
            let job_id = identity("job id")?;
            let submission_id = identity("submission id")?;
            let executor_execution_id = identity("executor execution id")?;
            let reviewed_stdout_sha256 = required_argument(
                &mut args,
                "resolve-dreamina-compliance-rejection",
                "reviewed stdout SHA-256",
            )?;
            let operator_reference = required_argument(
                &mut args,
                "resolve-dreamina-compliance-rejection",
                "operator reference",
            )?;
            let journal_root = PathBuf::from(required_argument(
                &mut args,
                "resolve-dreamina-compliance-rejection",
                "existing journal root",
            )?);
            Command::ResolveDreaminaComplianceRejection {
                journal_root,
                request: DreaminaComplianceRejectionResolutionRequest {
                    job_id,
                    submission_id,
                    executor_execution_id,
                    reviewed_stdout_sha256,
                    operator_reference,
                },
            }
        }
        "resolve-dreamina-image-success" => {
            let name = "resolve-dreamina-image-success";
            let mut identity = |field: &str| -> Result<Uuid, String> {
                required_argument(&mut args, name, field)?
                    .parse()
                    .map_err(|_| format!("{name} requires a UUID {field}"))
            };
            let job_id = identity("job id")?;
            let submission_id = identity("submission id")?;
            let executor_execution_id = identity("executor execution id")?;
            let reviewed_stdout_sha256 =
                required_argument(&mut args, name, "reviewed stdout SHA-256")?;
            let operator_reference = required_argument(&mut args, name, "operator reference")?;
            let evidence_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "protected evidence directory",
            )?);
            Command::ResolveDreaminaImageSuccess {
                evidence_root,
                request: DreaminaImageSuccessResolutionRequest {
                    job_id,
                    submission_id,
                    executor_execution_id,
                    reviewed_stdout_sha256,
                    operator_reference,
                },
            }
        }
        "resolve-dreamina-video-success" => {
            let name = "resolve-dreamina-video-success";
            let mut identity = |field: &str| -> Result<Uuid, String> {
                required_argument(&mut args, name, field)?
                    .parse()
                    .map_err(|_| format!("{name} requires a UUID {field}"))
            };
            let job_id = identity("job id")?;
            let submission_id = identity("submission id")?;
            let executor_execution_id = identity("executor execution id")?;
            let reviewed_stdout_sha256 =
                required_argument(&mut args, name, "reviewed stdout SHA-256")?;
            let operator_reference = required_argument(&mut args, name, "operator reference")?;
            let evidence_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "protected evidence directory",
            )?);
            Command::ResolveDreaminaVideoSuccess {
                evidence_root,
                request: DreaminaVideoSuccessResolutionRequest {
                    job_id,
                    submission_id,
                    executor_execution_id,
                    reviewed_stdout_sha256,
                    operator_reference,
                },
            }
        }
        "confirm-dreamina-video-cancellation" => {
            let name = "confirm-dreamina-video-cancellation";
            let job_id = required_argument(&mut args, name, "job id")?
                .parse()
                .map_err(|_| format!("{name} requires a UUID job id"))?;
            let request_id = required_argument(&mut args, name, "original request id")?;
            let mut identity = |field: &str| -> Result<Uuid, String> {
                required_argument(&mut args, name, field)?
                    .parse()
                    .map_err(|_| format!("{name} requires a UUID {field}"))
            };
            let submission_id = identity("submission id")?;
            let executor_execution_id = identity("executor execution id")?;
            let provider_account_id = identity("provider account id")?;
            let upstream_task_id = required_argument(&mut args, name, "upstream task id")?;
            let query_stdout_sha256 = required_argument(&mut args, name, "query stdout SHA-256")?;
            let operator_reference = required_argument(&mut args, name, "operator reference")?;
            let evidence_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "protected evidence directory",
            )?);
            Command::ConfirmDreaminaVideoCancellation {
                evidence_root,
                request: DreaminaManualVideoCancellationRequest {
                    job_id,
                    request_id,
                    submission_id,
                    executor_execution_id,
                    provider_account_id,
                    upstream_task_id,
                    query_stdout_sha256,
                    operator_reference,
                },
            }
        }
        "confirm-dreamina-video-supplement-cancellation" => {
            let name = "confirm-dreamina-video-supplement-cancellation";
            let authorization_id = required_argument(&mut args, name, "authorization id")?
                .parse()
                .map_err(|_| format!("{name} requires a UUID authorization id"))?;
            let dispatch_nonce = required_argument(&mut args, name, "dispatch nonce")?
                .parse()
                .map_err(|_| format!("{name} requires a UUID dispatch nonce"))?;
            let reviewed_stdout_sha256 =
                required_argument(&mut args, name, "reviewed query stdout SHA-256")?;
            let operator_reference = required_argument(&mut args, name, "operator reference")?;
            let evidence_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "protected evidence directory",
            )?);
            Command::ConfirmDreaminaVideoSupplementCancellation {
                evidence_root,
                request: DreaminaSupplementVideoCancellationRequest {
                    authorization_id,
                    dispatch_nonce,
                    reviewed_stdout_sha256,
                    operator_reference,
                },
            }
        }
        "prepare-dreamina-video-supplement" | "prepare-dreamina-video-supplement-successor" => {
            let name = command.as_ref();
            let mut identity = |field: &str| -> Result<Uuid, String> {
                required_argument(&mut args, name, field)?
                    .parse()
                    .map_err(|_| format!("{name} requires a UUID {field}"))
            };
            let previous_authorization_id = if name.ends_with("-successor") {
                Some(identity("previous authorization id")?)
            } else {
                None
            };
            let job_id = identity("job id")?;
            let submission_id = identity("submission id")?;
            let executor_execution_id = identity("executor execution id")?;
            let expected_command_sha256 =
                required_argument(&mut args, name, "expected command SHA-256")?;
            let expected_quote_hash = required_argument(&mut args, name, "expected quote SHA-256")?;
            let operator_reference = required_argument(&mut args, name, "operator reference")?;
            let evidence_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "protected evidence root",
            )?);
            let request = DreaminaVideoSupplementAuthorizationRequest {
                runtime_binding_json: serde_json::Value::Null,
                job_id,
                submission_id,
                executor_execution_id,
                expected_command_sha256,
                expected_quote_hash,
                operator_reference,
            };
            match previous_authorization_id {
                Some(previous_authorization_id) => {
                    Command::PrepareDreaminaVideoSupplementSuccessor {
                        previous_authorization_id,
                        evidence_root,
                        request,
                    }
                }
                None => Command::PrepareDreaminaVideoSupplement {
                    evidence_root,
                    request,
                },
            }
        }
        "recover-dreamina-video-supplement-acceptance" => {
            let name = command.as_ref();
            let authorization_id = required_argument(&mut args, name, "authorization id")?
                .parse()
                .map_err(|_| format!("{name} requires a UUID authorization id"))?;
            let evidence_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "existing protected evidence root",
            )?);
            let recovery_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "protected recovery bundle root",
            )?);
            let reviewed_recovery_sha256 =
                required_argument(&mut args, name, "reviewed recovery SHA-256")?;
            Command::RecoverDreaminaVideoSupplementAcceptance {
                authorization_id,
                evidence_root,
                recovery_root,
                reviewed_recovery_sha256,
            }
        }
        "dispatch-dreamina-video-supplement" | "poll-dreamina-video-supplement" => {
            let name = command.as_ref();
            let authorization_id = required_argument(&mut args, name, "authorization id")?
                .parse()
                .map_err(|_| format!("{name} requires a UUID authorization id"))?;
            let evidence_root = PathBuf::from(required_argument(
                &mut args,
                name,
                "protected evidence root",
            )?);
            if name == "dispatch-dreamina-video-supplement" {
                Command::DispatchDreaminaVideoSupplement {
                    evidence_root,
                    authorization_id,
                }
            } else {
                Command::PollDreaminaVideoSupplement {
                    evidence_root,
                    authorization_id,
                }
            }
        }
        "terminalize-unstarted-job" => {
            let job_id = required_argument(&mut args, "terminalize-unstarted-job", "job id")?
                .parse()
                .map_err(|_| "terminalize-unstarted-job requires a UUID job id".to_string())?;
            Command::TerminalizeUnstartedJob { job_id }
        }
        "cancel-unlaunched-job" => {
            let job_id = required_argument(&mut args, "cancel-unlaunched-job", "job id")?
                .parse()
                .map_err(|_| "cancel-unlaunched-job requires a UUID job id".to_string())?;
            Command::CancelUnlaunchedJob { job_id }
        }
        "refresh-provider-quota" => {
            let provider_account_id =
                required_argument(&mut args, "refresh-provider-quota", "provider account id")?
                    .parse()
                    .map_err(|_| {
                        "refresh-provider-quota requires a UUID provider account id".to_string()
                    })?;
            Command::RefreshProviderQuota {
                provider_account_id,
            }
        }
        "refresh-codex-credential" => {
            let name = "refresh-codex-credential";
            let provider_account_id = required_argument(&mut args, name, "provider account id")?
                .parse::<Uuid>()
                .ok()
                .filter(|id| !id.is_nil())
                .ok_or_else(|| format!("{name} requires a non-nil UUID provider account id"))?;
            let expected_revision = required_argument(&mut args, name, "expected revision")?
                .parse::<i64>()
                .ok()
                .filter(|revision| *revision > 0)
                .ok_or_else(|| format!("{name} requires a positive expected revision"))?;
            let expected_fingerprint_sha256 =
                required_argument(&mut args, name, "expected fingerprint SHA-256")?;
            if expected_fingerprint_sha256.len() != 64
                || !expected_fingerprint_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            {
                return Err(format!("{name} requires a lowercase SHA-256 fingerprint"));
            }
            // Never echo an accidentally supplied credential argument.
            if args.next().is_some() {
                return Err(format!("{name} accepts exactly three arguments"));
            }
            Command::RefreshCodexCredential {
                provider_account_id,
                expected_revision,
                expected_fingerprint_sha256,
            }
        }
        "retry-blocked-terminal" => {
            let submission_id =
                required_argument(&mut args, "retry-blocked-terminal", "submission id")?
                    .parse()
                    .map_err(|_| {
                        "retry-blocked-terminal requires a UUID submission id".to_string()
                    })?;
            let reason = required_argument(&mut args, "retry-blocked-terminal", "block reason")?;
            if reason != "canonical_conflict" {
                return Err("retry-blocked-terminal only accepts canonical_conflict".to_string());
            }
            let repair_revision =
                required_argument(&mut args, "retry-blocked-terminal", "repair revision")?;
            Command::RetryBlockedTerminal {
                submission_id,
                repair_revision,
            }
        }
        value => {
            return Err(format!(
                "unknown command `{value}`: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-codex-cli-edit-profile`, `provision-grok-profile`, `provision-grok-video-profile`, `provision-grok-video-v2-profile`, `provision-grok-image-profile-replacement`, `provision-grok-edit-profile-replacement`, `provision-grok-video-profile-replacement`, `reconcile-execution-profile-routes`, `reconcile-dreamina-profiles`, `reconcile-inline-customer-settlement`, `terminalize-unstarted-job`, `cancel-unlaunched-job`, `refresh-provider-quota`, `refresh-codex-credential`, `resolve-dreamina-compliance-rejection`, `resolve-dreamina-image-success`, `resolve-dreamina-video-success`, `confirm-dreamina-video-cancellation`, `confirm-dreamina-video-supplement-cancellation`, `prepare-dreamina-video-supplement`, `prepare-dreamina-video-supplement-successor`, `dispatch-dreamina-video-supplement`, `poll-dreamina-video-supplement`, `recover-dreamina-video-supplement-acceptance`, or `retry-blocked-terminal`"
            ));
        }
    };
    if let Some(extra) = args.next() {
        return Err(format!(
            "unexpected argument `{}`: command accepts no arguments",
            extra.as_ref(),
        ));
    }
    Ok(command)
}

fn required_argument<I, S>(args: &mut I, command: &str, name: &str) -> Result<String, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    args.next()
        .map(|value| value.as_ref().trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{command} requires {name}"))
}

fn init_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

fn require_cancellation_operator_uid(uid: libc::uid_t) -> Result<(), ImageGatewayError> {
    if uid != 0 {
        return Err(ImageGatewayError::config(
            "manual video cancellation confirmation requires the root operator",
        ));
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    let command = parse_command(env::args().skip(1)).map_err(ImageGatewayError::config)?;
    if matches!(
        command,
        Command::ConfirmDreaminaVideoCancellation { .. }
            | Command::ConfirmDreaminaVideoSupplementCancellation { .. }
    ) {
        require_cancellation_operator_uid(unsafe { libc::geteuid() })?;
    }
    // This command emits only its sanitized receipt/error, including storage failures.
    if !matches!(command, Command::RefreshCodexCredential { .. }) {
        init_tracing();
    }
    let provisioning = match &command {
        Command::Migrate | Command::VerifyMigrations => None,
        Command::ProvisionCodexProfile | Command::ProvisionCodexCliEditProfile => {
            let credential_home = provider_credential_home("EXECUTOR_CODEX_CREDENTIAL_HOME")?;
            Some(provisioning_from_env(codex_auth_file_sha256(
                credential_home,
            )?)?)
        }
        Command::ProvisionGrokProfile
        | Command::ProvisionGrokVideoProfile
        | Command::ProvisionGrokVideoV2Profile => {
            let credential_home = provider_credential_home("EXECUTOR_GROK_CREDENTIAL_HOME")?;
            Some(provisioning_from_env(grok_auth_file_sha256(
                credential_home,
            )?)?)
        }
        Command::BootstrapAdmin { .. }
        | Command::ProvisionGrokImageProfileReplacement { .. }
        | Command::ProvisionGrokEditProfileReplacement { .. }
        | Command::ProvisionGrokVideoProfileReplacement { .. }
        | Command::ReconcileExecutionProfileRoutes
        | Command::ReconcileDreaminaProfiles
        | Command::ReconcileInlineCustomerSettlement { .. }
        | Command::ResolveDreaminaComplianceRejection { .. }
        | Command::ResolveDreaminaImageSuccess { .. }
        | Command::ResolveDreaminaVideoSuccess { .. }
        | Command::ConfirmDreaminaVideoCancellation { .. }
        | Command::ConfirmDreaminaVideoSupplementCancellation { .. }
        | Command::PrepareDreaminaVideoSupplement { .. }
        | Command::PrepareDreaminaVideoSupplementSuccessor { .. }
        | Command::DispatchDreaminaVideoSupplement { .. }
        | Command::PollDreaminaVideoSupplement { .. }
        | Command::RecoverDreaminaVideoSupplementAcceptance { .. }
        | Command::TerminalizeUnstartedJob { .. }
        | Command::CancelUnlaunchedJob { .. }
        | Command::RefreshProviderQuota { .. }
        | Command::RefreshCodexCredential { .. }
        | Command::RetryBlockedTerminal { .. } => None,
    };
    let database_url = database_url_from_env()?;
    let database_schema = database_schema_from_env()?;
    let pool =
        connect_pool_with_schema(&database_url, DEFAULT_MAX_CONNECTIONS, &database_schema).await?;
    match command {
        Command::Migrate => {
            run_migrations(&pool).await?;
            println!("database migrations complete");
        }
        Command::VerifyMigrations => {
            verify_migrations(&pool).await?;
            println!("database migration identities verified (read-only)");
        }
        Command::ProvisionCodexProfile => {
            verify_migrations(&pool).await?;
            let provisioning = provisioning.ok_or_else(|| {
                ImageGatewayError::config("Codex execution profile provisioning is missing")
            })?;
            let provisioned = provision_codex_execution_profile(&pool, &provisioning)
                .await
                .map_err(|error| map_provisioning_error("Codex", error))?;
            println!(
                "Codex execution profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ProvisionCodexCliEditProfile => {
            verify_migrations(&pool).await?;
            let provisioning = provisioning.ok_or_else(|| {
                ImageGatewayError::config("Codex CLI edit profile provisioning is missing")
            })?;
            let provisioned = provision_codex_cli_edit_execution_profile(&pool, &provisioning)
                .await
                .map_err(|error| map_provisioning_error("Codex CLI edit", error))?;
            println!(
                "Codex CLI edit execution profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ProvisionGrokProfile => {
            verify_migrations(&pool).await?;
            let provisioning = provisioning.ok_or_else(|| {
                ImageGatewayError::config("Grok execution profile provisioning is missing")
            })?;
            let provisioned = provision_grok_execution_profile(&pool, &provisioning)
                .await
                .map_err(|error| map_provisioning_error("Grok", error))?;
            println!(
                "Grok execution profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ProvisionGrokVideoProfile => {
            verify_migrations(&pool).await?;
            let provisioning = provisioning.ok_or_else(|| {
                ImageGatewayError::config("Grok video execution profile provisioning is missing")
            })?;
            let provisioned = provision_grok_video_execution_profile(&pool, &provisioning)
                .await
                .map_err(|error| map_provisioning_error("Grok video", error))?;
            println!(
                "Grok video execution profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ProvisionGrokVideoV2Profile => {
            verify_migrations(&pool).await?;
            let provisioning = provisioning.ok_or_else(|| {
                ImageGatewayError::config("Grok video V2 execution profile provisioning is missing")
            })?;
            let provisioned = provision_grok_video_v2_execution_profile(&pool, &provisioning)
                .await
                .map_err(|error| map_provisioning_error("Grok video V2", error))?;
            println!(
                "Grok video V2 execution profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ProvisionGrokVideoProfileReplacement {
            source_profile_key,
            replacement_profile_key,
        } => {
            verify_migrations(&pool).await?;
            let provisioned = provision_grok_video_execution_profile_replacement(
                &pool,
                &source_profile_key,
                &replacement_profile_key,
            )
            .await
            .map_err(|error| map_provisioning_error("Grok video replacement", error))?;
            println!(
                "Grok video replacement profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ProvisionGrokImageProfileReplacement {
            source_profile_key,
            replacement_profile_key,
        } => {
            verify_migrations(&pool).await?;
            let provisioned = provision_grok_image_execution_profile_replacement(
                &pool,
                &source_profile_key,
                &replacement_profile_key,
            )
            .await
            .map_err(|error| map_provisioning_error("Grok image replacement", error))?;
            println!(
                "Grok image replacement profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ProvisionGrokEditProfileReplacement {
            source_profile_key,
            replacement_profile_key,
        } => {
            verify_migrations(&pool).await?;
            let provisioned = provision_grok_edit_execution_profile_replacement(
                &pool,
                &source_profile_key,
                &replacement_profile_key,
            )
            .await
            .map_err(|error| map_provisioning_error("Grok edit replacement", error))?;
            println!(
                "Grok edit replacement profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
        Command::ReconcileExecutionProfileRoutes => {
            verify_migrations(&pool).await?;
            let report = reconcile_execution_profile_routes(&pool).await?;
            println!(
                "execution profile routes reconciled: inspected={}, revised={}, unresolved={}, api_key_bindings_moved={}, project_bindings_moved={}, platform_bindings_moved={}",
                report.inspected_routes,
                report.revised_routes,
                report.unresolved_routes,
                report.api_key_bindings_moved,
                report.project_bindings_moved,
                report.platform_bindings_moved,
            );
        }
        Command::ReconcileDreaminaProfiles => {
            verify_migrations(&pool).await?;
            let created =
                PostgresProviderManagementService::reconcile_dreamina_video_profiles(&pool).await?;
            println!("Dreamina video profiles reconciled: {created} created");
        }
        Command::ReconcileInlineCustomerSettlement { job_id } => {
            verify_migrations(&pool).await?;
            reconcile_inline_customer_settlement(&pool, job_id).await?;
            println!("inline customer settlement reconciled: {job_id}");
        }
        Command::ResolveDreaminaComplianceRejection {
            journal_root,
            request,
        } => {
            verify_migrations(&pool).await?;
            let receipt =
                resolve_dreamina_compliance_rejection(&pool, &journal_root, &request).await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "resolution receipt serialization failed"
                ))?
            );
        }
        Command::ResolveDreaminaImageSuccess {
            evidence_root,
            request,
        } => {
            verify_migrations(&pool).await?;
            let blobs = gpt_image_2_gateway::FilesystemArtifactBlobStore::new(
                gpt_image_2_gateway::artifacts::artifact_root_from_env()?,
            )?;
            let receipt =
                resolve_dreamina_image_success(&pool, &blobs, &evidence_root, &request).await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "resolution receipt serialization failed"
                ))?
            );
        }
        Command::ResolveDreaminaVideoSuccess {
            evidence_root,
            request,
        } => {
            verify_migrations(&pool).await?;
            let receipt = resolve_dreamina_video_success(&pool, &evidence_root, &request).await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "resolution receipt serialization failed"
                ))?
            );
        }
        Command::ConfirmDreaminaVideoCancellation {
            evidence_root,
            request,
        } => {
            verify_migrations(&pool).await?;
            let store = PostgresExecutionSettlementStore::new(
                pool.clone(),
                Arc::new(gpt_image_2_gateway::artifacts::InMemoryArtifactBlobStore::default()),
            );
            let receipt = store
                .confirm_dreamina_manual_video_cancellation(&evidence_root, &request)
                .await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "cancellation receipt serialization failed"
                ))?
            );
        }
        Command::ConfirmDreaminaVideoSupplementCancellation {
            evidence_root,
            request,
        } => {
            verify_migrations(&pool).await?;
            let store = PostgresExecutionSettlementStore::new(
                pool.clone(),
                Arc::new(gpt_image_2_gateway::artifacts::InMemoryArtifactBlobStore::default()),
            );
            let receipt = store
                .confirm_dreamina_supplement_video_cancellation(&evidence_root, &request)
                .await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "cancellation receipt serialization failed"
                ))?
            );
        }
        Command::PrepareDreaminaVideoSupplement {
            evidence_root,
            request,
        } => {
            verify_migrations(&pool).await?;
            let receipt =
                prepare_dreamina_video_supplement_operator(&pool, &evidence_root, &request).await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "operator receipt serialization failed"
                ))?
            );
        }
        Command::PrepareDreaminaVideoSupplementSuccessor {
            previous_authorization_id,
            evidence_root,
            request,
        } => {
            verify_migrations(&pool).await?;
            let receipt = prepare_dreamina_video_supplement_successor_operator(
                &pool,
                &evidence_root,
                previous_authorization_id,
                &request,
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "operator receipt serialization failed"
                ))?
            );
        }
        Command::DispatchDreaminaVideoSupplement {
            evidence_root,
            authorization_id,
        } => {
            verify_migrations(&pool).await?;
            let receipt = dispatch_dreamina_video_supplement_operator(
                &pool,
                &evidence_root,
                authorization_id,
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "operator receipt serialization failed"
                ))?
            );
        }
        Command::PollDreaminaVideoSupplement {
            evidence_root,
            authorization_id,
        } => {
            verify_migrations(&pool).await?;
            let receipt =
                poll_dreamina_video_supplement_operator(&pool, &evidence_root, authorization_id)
                    .await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "operator receipt serialization failed"
                ))?
            );
        }
        Command::RecoverDreaminaVideoSupplementAcceptance {
            authorization_id,
            evidence_root,
            recovery_root,
            reviewed_recovery_sha256,
        } => {
            verify_migrations(&pool).await?;
            let receipt = recover_dreamina_video_supplement_acceptance_operator(
                &pool,
                &evidence_root,
                authorization_id,
                &recovery_root,
                &reviewed_recovery_sha256,
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string(&receipt).map_err(|_| ImageGatewayError::internal(
                    "operator receipt serialization failed"
                ))?
            );
        }
        Command::TerminalizeUnstartedJob { job_id } => {
            verify_migrations(&pool).await?;
            let outcome = PostgresReconciliationStore::new(pool.clone())
                .terminalize_unstarted_job(job_id)
                .await?;
            println!(
                "unstarted job terminalized: job_id={}, released_units={}, released_micros={}",
                outcome.job_id, outcome.released_units, outcome.released_micros
            );
        }
        Command::CancelUnlaunchedJob { job_id } => {
            verify_migrations(&pool).await?;
            let outcome = PostgresReconciliationStore::new(pool.clone())
                .cancel_unlaunched_job(job_id)
                .await?;
            println!(
                "unlaunched job cancellation recorded: job_id={}, canceled_outputs={}",
                outcome.job_id, outcome.canceled_outputs
            );
        }
        Command::RefreshProviderQuota {
            provider_account_id,
        } => {
            verify_migrations(&pool).await?;
            PostgresProviderManagementService::from_env(pool.clone())
                .await?
                .refresh_provider_quota(provider_account_id)
                .await?;
            println!("provider quota and credential refreshed: {provider_account_id}");
        }
        Command::RefreshCodexCredential {
            provider_account_id,
            expected_revision,
            expected_fingerprint_sha256,
        } => {
            verify_migrations(&pool).await?;
            let receipt = refresh_codex_credential(
                &pool,
                provider_account_id,
                expected_revision,
                &expected_fingerprint_sha256,
            )
            .await?;
            println!("{receipt}");
        }
        Command::RetryBlockedTerminal {
            submission_id,
            repair_revision,
        } => {
            verify_migrations(&pool).await?;
            let outcome = PostgresExecutorTerminalStore::new(pool.clone())
                .requeue_blocked_canonical_conflict(submission_id, &repair_revision, "factoryctl")
                .await
                .map_err(map_blocked_terminal_requeue_error)?;
            println!(
                "blocked terminal reduction requeued: submission_id={}, repair_revision={}, already_requeued={}",
                outcome.submission_id, outcome.repair_revision, outcome.already_requeued
            );
        }
        Command::BootstrapAdmin {
            email,
            display_name,
        } => {
            verify_migrations(&pool).await?;
            let identity = gpt_image_2_gateway::identity::service_from_env(pool.clone())
                .await?
                .ok_or_else(|| {
                    ImageGatewayError::config(
                        "GATEWAY_IDENTITY_ENABLED must be true to bootstrap an admin",
                    )
                })?;
            let password = prompt_new_password()?;
            if !identity
                .bootstrap_admin(email, display_name, password)
                .await
                .map_err(map_identity_error)?
            {
                return Err(ImageGatewayError::config(
                    "an identity user with that email already exists",
                ));
            }
            println!("administrator bootstrapped");
        }
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct CodexCredentialHead {
    provider_id: String,
    revision: i64,
    fingerprint_sha256: String,
    environment_ref: String,
    account_state: String,
    environment_state: String,
    lifecycle_state: String,
    refresh_strategy: String,
    material_kind: String,
}

async fn refresh_codex_credential(
    pool: &sqlx::PgPool,
    provider_account_id: Uuid,
    expected_revision: i64,
    expected_fingerprint_sha256: &str,
) -> Result<serde_json::Value, ImageGatewayError> {
    let current = sqlx::query_as::<_, CodexCredentialHead>(
        r#"
        SELECT account.provider_id, head.active_revision AS revision,
               revision.material_fingerprint_sha256 AS fingerprint_sha256,
               environment.environment_ref, account.state AS account_state,
               environment.state AS environment_state, head.lifecycle_state,
               head.refresh_strategy, revision.material_kind
        FROM provider_accounts account
        JOIN provider_account_credential_heads head USING (provider_account_id)
        JOIN provider_account_credential_revisions revision
          ON revision.provider_account_id = head.provider_account_id
         AND revision.revision = head.active_revision
        JOIN provider_account_environments environment
          ON environment.provider_account_id = account.provider_account_id
         AND environment.provider_id = account.provider_id
        WHERE account.provider_account_id = $1
        "#,
    )
    .bind(provider_account_id)
    .fetch_optional(pool)
    .await
    .map_err(|_| ImageGatewayError::service_unavailable("credential storage is unavailable"))?
    .ok_or_else(|| ImageGatewayError::config("managed credential account is unavailable"))?;
    if current.provider_id != "openai-codex" {
        return Err(ImageGatewayError::config(
            "refresh-codex-credential only accepts openai-codex accounts",
        ));
    }
    let receipt = |result: &str, revision: i64, fingerprint: &str| {
        serde_json::json!({
            "result": result,
            "provider_account_id": provider_account_id,
            "previous_revision": expected_revision,
            "previous_fingerprint_sha256": expected_fingerprint_sha256,
            "current_revision": revision,
            "current_fingerprint_sha256": fingerprint,
        })
    };
    if current.revision != expected_revision
        || current.fingerprint_sha256 != expected_fingerprint_sha256
    {
        return Ok(receipt(
            "already_changed",
            current.revision,
            &current.fingerprint_sha256,
        ));
    }
    if current.account_state != "enabled"
        || current.environment_state != "active"
        || !matches!(
            current.lifecycle_state.as_str(),
            "active" | "refresh_due" | "refreshing"
        )
        || current.refresh_strategy != "broker_managed"
        || current.material_kind != "auth_file"
    {
        return Err(ImageGatewayError::config(
            "Codex credential is not eligible for broker refresh",
        ));
    }
    let root = existing_private_directory(&absolute_env_path("GATEWAY_PROVIDER_HOME_ROOT")?)?;
    let home = existing_private_directory(Path::new(&current.environment_ref))?;
    if home == root || !home.starts_with(&root) {
        return Err(ImageGatewayError::config(
            "Codex credential environment is outside the managed root",
        ));
    }
    let auth = fs::symlink_metadata(home.join("auth.json"))
        .map_err(|_| ImageGatewayError::config("Codex credential material is unavailable"))?;
    if !auth.is_file() || auth.uid() != unsafe { libc::geteuid() } || auth.mode() & 0o077 != 0 {
        return Err(ImageGatewayError::config(
            "Codex credential material is unsafe",
        ));
    }
    let executable_env = if env::var_os("GATEWAY_MANAGED_CODEX_EXECUTABLE").is_some() {
        "GATEWAY_MANAGED_CODEX_EXECUTABLE"
    } else {
        "EXECUTOR_CODEX_EXECUTABLE"
    };
    let executable = existing_trusted_executable(&absolute_env_path(executable_env)?)?;
    // Construction only: from_env also expires logins and reconciles global routes.
    let refreshed = PostgresProviderManagementService::new(pool.clone(), root, executable)
        .refresh_after_authentication_rejection(
            provider_account_id,
            expected_revision,
            expected_fingerprint_sha256,
        )
        .await
        .map_err(|error| match error {
            gpt_image_2_gateway::CredentialResolveError::ReauthorizationRequired => {
                ImageGatewayError::service_unavailable("Codex credential requires reauthorization")
            }
            _ => ImageGatewayError::service_unavailable("Codex credential refresh failed"),
        })?;
    Ok(receipt(
        "refreshed",
        refreshed.revision,
        &refreshed.material_fingerprint_sha256,
    ))
}

fn existing_private_directory(path: &Path) -> Result<PathBuf, ImageGatewayError> {
    let invalid =
        || ImageGatewayError::config("Codex credential directory is unavailable or unsafe");
    if !path.is_absolute() {
        return Err(invalid());
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| invalid())?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid());
    }
    let canonical = path.canonicalize().map_err(|_| invalid())?;
    validate_trusted_ancestors(path)?;
    validate_trusted_ancestors(&canonical)?;
    Ok(canonical)
}

fn existing_trusted_executable(path: &Path) -> Result<PathBuf, ImageGatewayError> {
    let invalid = || ImageGatewayError::config("managed Codex executable is unavailable or unsafe");
    let metadata = fs::symlink_metadata(path).map_err(|_| invalid())?;
    if !path.is_absolute()
        || !metadata.is_file()
        || metadata.mode() & 0o022 != 0
        || metadata.mode() & 0o111 == 0
        || ![0, unsafe { libc::geteuid() }].contains(&metadata.uid())
    {
        return Err(invalid());
    }
    let canonical = path.canonicalize().map_err(|_| invalid())?;
    validate_trusted_ancestors(path)?;
    validate_trusted_ancestors(&canonical)?;
    Ok(canonical)
}

fn validate_trusted_ancestors(path: &Path) -> Result<(), ImageGatewayError> {
    let invalid = || ImageGatewayError::config("Codex credential path has an unsafe ancestor");
    let trusted_owners = [0, unsafe { libc::geteuid() }];
    for parent in path.ancestors().skip(1) {
        let entry = fs::symlink_metadata(parent).map_err(|_| invalid())?;
        let directory = fs::metadata(parent).map_err(|_| invalid())?;
        if !trusted_owners.contains(&entry.uid())
            || !trusted_owners.contains(&directory.uid())
            || !directory.is_dir()
            // Sticky system temp directories protect their trusted-owned children.
            || (directory.mode() & 0o022 != 0 && directory.mode() & 0o1000 == 0)
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn prompt_new_password() -> Result<String, ImageGatewayError> {
    let password = rpassword::prompt_password("New administrator password: ")
        .map_err(|_| ImageGatewayError::config("failed to read administrator password"))?;
    let confirmation = rpassword::prompt_password("Confirm administrator password: ")
        .map_err(|_| ImageGatewayError::config("failed to read password confirmation"))?;
    if password != confirmation {
        return Err(ImageGatewayError::config(
            "password confirmation does not match",
        ));
    }
    Ok(password)
}

fn map_identity_error(error: factory_identity::IdentityError) -> ImageGatewayError {
    match error {
        factory_identity::IdentityError::InvalidInput => {
            ImageGatewayError::config("administrator identity input is invalid")
        }
        factory_identity::IdentityError::Conflict => {
            ImageGatewayError::config("administrator identity already exists")
        }
        factory_identity::IdentityError::Unavailable => {
            ImageGatewayError::service_unavailable("identity storage is unavailable")
        }
        _ => ImageGatewayError::internal("administrator bootstrap failed"),
    }
}

fn map_blocked_terminal_requeue_error(error: BlockedTerminalRequeueError) -> ImageGatewayError {
    match error {
        BlockedTerminalRequeueError::InvalidInput => {
            ImageGatewayError::config("blocked terminal requeue input is invalid")
        }
        BlockedTerminalRequeueError::Conflict => ImageGatewayError::conflict(
            "blocked terminal reduction does not match safe canonical evidence",
            None,
            "blocked_terminal_requeue_conflict",
        ),
        BlockedTerminalRequeueError::Unavailable => ImageGatewayError::service_unavailable(
            "blocked terminal reduction storage is unavailable",
        ),
    }
}

fn provisioning_from_env(
    credential_auth_sha256: String,
) -> Result<CodexExecutionProfileProvisioning, ImageGatewayError> {
    Ok(CodexExecutionProfileProvisioning {
        profile_key: required_env("EXECUTOR_PROFILE_KEY")?,
        credential_pool_key: required_env("EXECUTOR_CREDENTIAL_POOL_KEY")?,
        provider_account_key: required_env("EXECUTOR_PROVIDER_ACCOUNT_KEY")?,
        credential_ref: required_env("EXECUTOR_CREDENTIAL_REF")?,
        credential_revision: positive_i64_env("EXECUTOR_CREDENTIAL_REVISION")?,
        credential_auth_sha256,
        max_concurrency: positive_i32_env("EXECUTOR_MAX_CONCURRENCY")?,
    })
}

fn required_env(name: &str) -> Result<String, ImageGatewayError> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ImageGatewayError::config(format!("{name} is required")))
}

fn absolute_env_path(name: &str) -> Result<PathBuf, ImageGatewayError> {
    let path = PathBuf::from(required_env(name)?);
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(ImageGatewayError::config(format!(
            "{name} must be an absolute path"
        )))
    }
}

fn provider_credential_home(fallback: &str) -> Result<PathBuf, ImageGatewayError> {
    match env::var("EXECUTOR_CREDENTIAL_HOME")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        Some(value) => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                Ok(path)
            } else {
                Err(ImageGatewayError::config(
                    "EXECUTOR_CREDENTIAL_HOME must be an absolute path",
                ))
            }
        }
        None => absolute_env_path(fallback),
    }
}

fn positive_i64_env(name: &str) -> Result<i64, ImageGatewayError> {
    required_env(name)?
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ImageGatewayError::config(format!("{name} must be positive")))
}

fn positive_i32_env(name: &str) -> Result<i32, ImageGatewayError> {
    required_env(name)?
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ImageGatewayError::config(format!("{name} must be positive")))
}

fn map_provisioning_error(
    provider: &str,
    error: CodexProfileProvisioningError,
) -> ImageGatewayError {
    match error {
        CodexProfileProvisioningError::InvalidInput => ImageGatewayError::config(format!(
            "{provider} execution profile provisioning input is invalid"
        )),
        CodexProfileProvisioningError::Conflict => ImageGatewayError::config(format!(
            "{provider} execution profile provisioning conflicts with durable identity"
        )),
        CodexProfileProvisioningError::Unavailable => ImageGatewayError::service_unavailable(
            format!("{provider} execution profile provisioning storage is unavailable"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::require_cancellation_operator_uid;
    use super::{Command, init_tracing, parse_command};
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn tracing_initialization_is_idempotent() {
        init_tracing();
        init_tracing();
    }

    #[test]
    fn accepts_exactly_migrate() {
        assert_eq!(parse_command(["migrate"]), Ok(Command::Migrate));
    }

    #[test]
    fn accepts_exactly_grok_video_v2_profile_provisioning() {
        assert_eq!(
            parse_command(["provision-grok-video-v2-profile"]),
            Ok(Command::ProvisionGrokVideoV2Profile)
        );
        assert!(parse_command(["provision-grok-video-v2-profile", "extra"]).is_err());
    }

    #[test]
    fn accepts_exactly_read_only_migration_verification() {
        assert_eq!(
            parse_command(["verify-migrations"]),
            Ok(Command::VerifyMigrations)
        );
        assert!(parse_command(["verify-migrations", "extra"]).is_err());
    }

    #[test]
    fn accepts_bootstrap_admin_identity() {
        assert_eq!(
            parse_command(["bootstrap-admin", "owner@example.com", "Platform Owner"]),
            Ok(Command::BootstrapAdmin {
                email: "owner@example.com".to_string(),
                display_name: "Platform Owner".to_string(),
            })
        );
    }

    #[test]
    fn accepts_exactly_provision_codex_profile() {
        assert_eq!(
            parse_command(["provision-codex-profile"]),
            Ok(Command::ProvisionCodexProfile)
        );
    }

    #[test]
    fn accepts_only_explicit_codex_cli_edit_profile_command() {
        assert_eq!(
            parse_command(["provision-codex-cli-edit-profile"]),
            Ok(Command::ProvisionCodexCliEditProfile)
        );
        assert!(parse_command(["provision-codex-cli-edit-profile", "unexpected"]).is_err());
    }

    #[test]
    fn accepts_exactly_provision_grok_profile() {
        assert_eq!(
            parse_command(["provision-grok-profile"]),
            Ok(Command::ProvisionGrokProfile)
        );
    }

    #[test]
    fn accepts_exactly_provision_grok_video_profile() {
        assert_eq!(
            parse_command(["provision-grok-video-profile"]),
            Ok(Command::ProvisionGrokVideoProfile)
        );
    }

    #[test]
    fn accepts_grok_video_profile_replacement() {
        assert_eq!(
            parse_command([
                "provision-grok-video-profile-replacement",
                "managed.grok.videos.account.runtime-v1",
                "managed.grok.videos.account.runtime-v2",
            ]),
            Ok(Command::ProvisionGrokVideoProfileReplacement {
                source_profile_key: "managed.grok.videos.account.runtime-v1".to_string(),
                replacement_profile_key: "managed.grok.videos.account.runtime-v2".to_string(),
            })
        );
    }

    #[test]
    fn accepts_grok_image_and_edit_profile_replacements() {
        assert!(matches!(
            parse_command([
                "provision-grok-image-profile-replacement",
                "managed.grok.images.old",
                "managed.grok.images.v105",
            ]),
            Ok(Command::ProvisionGrokImageProfileReplacement { .. })
        ));
        assert!(matches!(
            parse_command([
                "provision-grok-edit-profile-replacement",
                "managed.grok.edits.old",
                "managed.grok.edits.v105",
            ]),
            Ok(Command::ProvisionGrokEditProfileReplacement { .. })
        ));
    }

    #[test]
    fn accepts_exactly_reconcile_execution_profile_routes() {
        assert_eq!(
            parse_command(["reconcile-execution-profile-routes"]),
            Ok(Command::ReconcileExecutionProfileRoutes)
        );
    }

    #[test]
    fn accepts_exactly_reconcile_dreamina_profiles() {
        assert_eq!(
            parse_command(["reconcile-dreamina-profiles"]),
            Ok(Command::ReconcileDreaminaProfiles)
        );
    }

    #[test]
    fn accepts_inline_customer_settlement_job() {
        let job_id = uuid::Uuid::new_v4();
        assert_eq!(
            parse_command(["reconcile-inline-customer-settlement", &job_id.to_string()]),
            Ok(Command::ReconcileInlineCustomerSettlement { job_id })
        );
    }

    #[test]
    fn rejects_invalid_inline_customer_settlement_job() {
        assert_eq!(
            parse_command(["reconcile-inline-customer-settlement", "not-a-uuid"]),
            Err("reconcile-inline-customer-settlement requires a UUID job id".to_string())
        );
    }

    #[test]
    fn compliance_resolution_requires_all_reviewed_identity_arguments() {
        let job = uuid::Uuid::new_v4().to_string();
        let submission = uuid::Uuid::new_v4().to_string();
        let execution = uuid::Uuid::new_v4().to_string();
        let digest = "a".repeat(64);
        let args = [
            "resolve-dreamina-compliance-rejection",
            &job,
            &submission,
            &execution,
            &digest,
            "operator-ticket-1",
            "/private/journal",
        ];
        assert!(matches!(
            parse_command(args),
            Ok(Command::ResolveDreaminaComplianceRejection { .. })
        ));
        assert!(parse_command(&args[..6]).is_err());
        let mut extra = args.to_vec();
        extra.push("retry");
        assert!(parse_command(extra).is_err());
    }

    #[test]
    fn video_cancellation_confirmation_requires_original_identity_and_private_evidence() {
        let job = Uuid::new_v4().to_string();
        let submission = Uuid::new_v4().to_string();
        let execution = Uuid::new_v4().to_string();
        let account = Uuid::new_v4().to_string();
        let digest = "a".repeat(64);
        let args = [
            "confirm-dreamina-video-cancellation",
            &job,
            "original-request-id",
            &submission,
            &execution,
            &account,
            "original-upstream-task-id",
            &digest,
            "operator-cancellation-ticket",
            "/private/cancellation-evidence",
        ];
        let Command::ConfirmDreaminaVideoCancellation {
            evidence_root,
            request,
        } = parse_command(args).unwrap()
        else {
            panic!("expected confirm-only manual cancellation command");
        };
        assert_eq!(request.job_id.to_string(), job);
        assert_eq!(request.request_id, "original-request-id");
        assert_eq!(request.submission_id.to_string(), submission);
        assert_eq!(request.executor_execution_id.to_string(), execution);
        assert_eq!(request.provider_account_id.to_string(), account);
        assert_eq!(request.upstream_task_id, "original-upstream-task-id");
        assert_eq!(request.query_stdout_sha256, digest);
        assert_eq!(request.operator_reference, "operator-cancellation-ticket");
        assert_eq!(
            evidence_root,
            PathBuf::from("/private/cancellation-evidence")
        );
        for length in 1..args.len() {
            assert!(parse_command(&args[..length]).is_err());
        }
        let mut extra = args.to_vec();
        extra.push("cancel");
        assert!(parse_command(extra).is_err());
        for index in [1, 3, 4, 5] {
            let mut invalid = args;
            invalid[index] = "not-a-uuid";
            assert!(parse_command(invalid).is_err());
        }
    }

    #[test]
    fn video_cancellation_confirmation_requires_root_operator() {
        assert!(require_cancellation_operator_uid(0).is_ok());
        assert!(require_cancellation_operator_uid(501).is_err());
    }

    #[test]
    fn video_supplement_cancellation_requires_saved_authority_and_private_evidence() {
        let authorization = Uuid::new_v4().to_string();
        let nonce = Uuid::new_v4().to_string();
        let digest = "a".repeat(64);
        let args = [
            "confirm-dreamina-video-supplement-cancellation",
            &authorization,
            &nonce,
            &digest,
            "operator-supplement-cancellation-ticket",
            "/private/supplement-cancellation-evidence",
        ];
        let Command::ConfirmDreaminaVideoSupplementCancellation {
            evidence_root,
            request,
        } = parse_command(args).unwrap()
        else {
            panic!("expected confirm-only supplement cancellation command");
        };
        assert_eq!(request.authorization_id.to_string(), authorization);
        assert_eq!(request.dispatch_nonce.to_string(), nonce);
        assert_eq!(request.reviewed_stdout_sha256, digest);
        assert_eq!(
            request.operator_reference,
            "operator-supplement-cancellation-ticket"
        );
        assert_eq!(
            evidence_root,
            PathBuf::from("/private/supplement-cancellation-evidence")
        );
        for length in 1..args.len() {
            assert!(parse_command(&args[..length]).is_err());
        }
        let mut extra = args.to_vec();
        extra.push("cancel");
        assert!(parse_command(extra).is_err());
        for index in [1, 2] {
            let mut invalid = args;
            invalid[index] = "not-a-uuid";
            assert!(parse_command(invalid).is_err());
        }
    }

    #[test]
    fn video_supplement_prepare_requires_all_original_identity_and_evidence_arguments() {
        let job = uuid::Uuid::new_v4().to_string();
        let submission = uuid::Uuid::new_v4().to_string();
        let execution = uuid::Uuid::new_v4().to_string();
        let digest = "a".repeat(64);
        let args = [
            "prepare-dreamina-video-supplement",
            &job,
            &submission,
            &execution,
            &digest,
            &digest,
            "operator-ticket-one-shot",
            "/private/evidence",
        ];
        let Ok(Command::PrepareDreaminaVideoSupplement {
            evidence_root,
            request,
        }) = parse_command(args)
        else {
            panic!("expected the explicit prepare command");
        };
        assert_eq!(request.job_id.to_string(), job);
        assert_eq!(request.submission_id.to_string(), submission);
        assert_eq!(request.executor_execution_id.to_string(), execution);
        assert_eq!(request.expected_command_sha256, digest);
        assert_eq!(request.expected_quote_hash, digest);
        assert_eq!(request.runtime_binding_json, serde_json::Value::Null);
        assert_eq!(evidence_root, std::path::PathBuf::from("/private/evidence"));
        for len in 1..args.len() {
            assert!(parse_command(&args[..len]).is_err());
        }
        let mut extra = args.to_vec();
        extra.push("retry");
        assert!(parse_command(extra).is_err());
    }

    #[test]
    fn video_supplement_successor_requires_explicit_previous_authority() {
        let previous = uuid::Uuid::new_v4().to_string();
        let job = uuid::Uuid::new_v4().to_string();
        let submission = uuid::Uuid::new_v4().to_string();
        let execution = uuid::Uuid::new_v4().to_string();
        let digest = "a".repeat(64);
        let args = [
            "prepare-dreamina-video-supplement-successor",
            &previous,
            &job,
            &submission,
            &execution,
            &digest,
            &digest,
            "new-human-authorization",
            "/private/new-evidence",
        ];
        let Command::PrepareDreaminaVideoSupplementSuccessor {
            previous_authorization_id,
            request,
            ..
        } = parse_command(args).unwrap()
        else {
            panic!("expected explicit successor prepare");
        };
        assert_eq!(previous_authorization_id.to_string(), previous);
        assert_eq!(request.job_id.to_string(), job);
        for length in 1..args.len() {
            assert!(parse_command(&args[..length]).is_err());
        }
        let mut extra = args.to_vec();
        extra.push("retry");
        assert!(parse_command(extra).is_err());
    }

    #[test]
    fn video_supplement_dispatch_and_poll_require_only_saved_authorization() {
        let id = uuid::Uuid::new_v4();
        for command in [
            "dispatch-dreamina-video-supplement",
            "poll-dreamina-video-supplement",
        ] {
            let args = [command, &id.to_string(), "/private/evidence"];
            let parsed = parse_command(args).unwrap();
            assert!(match parsed {
                Command::DispatchDreaminaVideoSupplement {
                    authorization_id, ..
                } => command.starts_with("dispatch-") && authorization_id == id,
                Command::PollDreaminaVideoSupplement {
                    authorization_id, ..
                } => command.starts_with("poll-") && authorization_id == id,
                _ => false,
            });
            assert!(parse_command(&args[..2]).is_err());
            assert!(parse_command([command, "not-a-uuid", "/private/evidence"]).is_err());
            assert!(
                parse_command([command, &id.to_string(), "/private/evidence", "retry"]).is_err()
            );
        }
    }

    #[test]
    fn video_supplement_acceptance_recovery_requires_exact_authority_and_reviewed_bundle() {
        let id = Uuid::new_v4();
        let digest = "a".repeat(64);
        let args = [
            "recover-dreamina-video-supplement-acceptance",
            &id.to_string(),
            "/private/original-evidence",
            "/private/recovery-bundle",
            &digest,
        ];
        assert_eq!(
            parse_command(args),
            Ok(Command::RecoverDreaminaVideoSupplementAcceptance {
                authorization_id: id,
                evidence_root: PathBuf::from("/private/original-evidence"),
                recovery_root: PathBuf::from("/private/recovery-bundle"),
                reviewed_recovery_sha256: digest.clone(),
            })
        );
        for length in 1..args.len() {
            assert!(parse_command(&args[..length]).is_err());
        }
        let mut invalid = args.to_vec();
        invalid[1] = "not-a-uuid";
        assert!(parse_command(invalid).is_err());
        let mut extra = args.to_vec();
        extra.push("retry");
        assert!(parse_command(extra).is_err());
    }

    #[test]
    fn accepts_unstarted_job_terminalization() {
        let job_id = uuid::Uuid::new_v4();
        assert_eq!(
            parse_command(["terminalize-unstarted-job", &job_id.to_string()]),
            Ok(Command::TerminalizeUnstartedJob { job_id })
        );
        assert_eq!(
            parse_command(["terminalize-unstarted-job", "not-a-uuid"]),
            Err("terminalize-unstarted-job requires a UUID job id".to_string())
        );
    }

    #[test]
    fn accepts_unlaunched_job_cancellation() {
        let job_id = uuid::Uuid::new_v4();
        assert_eq!(
            parse_command(["cancel-unlaunched-job", &job_id.to_string()]),
            Ok(Command::CancelUnlaunchedJob { job_id })
        );
        assert_eq!(
            parse_command(["cancel-unlaunched-job", "not-a-uuid"]),
            Err("cancel-unlaunched-job requires a UUID job id".to_string())
        );
    }

    #[test]
    fn accepts_provider_quota_refresh() {
        let provider_account_id = uuid::Uuid::new_v4();
        assert_eq!(
            parse_command(["refresh-provider-quota", &provider_account_id.to_string()]),
            Ok(Command::RefreshProviderQuota {
                provider_account_id,
            })
        );
        assert_eq!(
            parse_command(["refresh-provider-quota", "not-a-uuid"]),
            Err("refresh-provider-quota requires a UUID provider account id".to_string())
        );
        assert_eq!(
            parse_command([
                "refresh-provider-quota",
                &provider_account_id.to_string(),
                "again",
            ]),
            Err("unexpected argument `again`: command accepts no arguments".to_string())
        );
    }

    #[test]
    fn codex_credential_refresh_requires_exact_reviewed_identity() {
        let account = uuid::Uuid::new_v4();
        let account_text = account.to_string();
        let hash = "a".repeat(64);
        let valid = ["refresh-codex-credential", &account_text, "2", &hash];
        assert_eq!(
            parse_command(valid),
            Ok(Command::RefreshCodexCredential {
                provider_account_id: account,
                expected_revision: 2,
                expected_fingerprint_sha256: hash.clone(),
            })
        );
        for invalid in ["not-a-uuid", "00000000-0000-0000-0000-000000000000"] {
            let mut args = valid;
            args[1] = invalid;
            assert!(parse_command(args).is_err());
        }
        for invalid in ["0", "-1", "no", "9223372036854775808"] {
            let mut args = valid;
            args[2] = invalid;
            assert!(parse_command(args).is_err());
        }
        for invalid in [
            "",
            "short",
            &"g".repeat(64),
            &"A".repeat(64),
            &"a".repeat(65),
        ] {
            let mut args = valid;
            args[3] = invalid;
            assert!(parse_command(args).is_err());
        }
        for len in 1..valid.len() {
            assert!(parse_command(&valid[..len]).is_err());
        }
        let mut extra = valid.to_vec();
        extra.push("secret-do-not-print");
        let error = parse_command(extra).unwrap_err();
        assert!(!error.contains("secret-do-not-print"));
    }

    #[test]
    fn codex_credential_directory_validation_never_repairs_permissions_or_symlinks() {
        use std::{
            fs,
            os::unix::fs::{PermissionsExt, symlink},
        };
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        assert!(super::existing_private_directory(directory.path()).is_ok());
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(super::existing_private_directory(directory.path()).is_err());
        assert_eq!(
            fs::metadata(directory.path()).unwrap().permissions().mode() & 0o777,
            0o755
        );
        let missing = directory.path().join("missing");
        assert!(super::existing_private_directory(&missing).is_err());
        assert!(!missing.exists());
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let link = directory.path().join("link");
        symlink(directory.path(), &link).unwrap();
        assert!(super::existing_private_directory(&link).is_err());
        assert!(super::existing_private_directory(std::path::Path::new("relative")).is_err());
    }

    #[test]
    fn codex_credential_paths_reject_unsafe_executables_and_ancestors() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        let executable = parent.join("codex");
        std::fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
        for mode in [0o644, 0o777] {
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(super::existing_trusted_executable(&executable).is_err());
        }
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(super::existing_trusted_executable(&executable).is_ok());
        let link = parent.join("codex-link");
        symlink(&executable, &link).unwrap();
        assert!(super::existing_trusted_executable(&link).is_err());

        let home = parent.join("home");
        std::fs::create_dir(&home).unwrap();
        std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700)).unwrap();
        for mode in [0o775, 0o777] {
            std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(mode)).unwrap();
            assert!(super::existing_trusted_executable(&executable).is_err());
            assert!(super::existing_private_directory(&home).is_err());
        }
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o1777)).unwrap();
        assert!(super::existing_trusted_executable(&executable).is_ok());
        assert!(super::existing_private_directory(&home).is_ok());
    }

    #[test]
    fn accepts_only_canonical_conflict_terminal_requeue() {
        let submission_id = uuid::Uuid::new_v4();
        assert_eq!(
            parse_command([
                "retry-blocked-terminal",
                &submission_id.to_string(),
                "canonical_conflict",
                "credit-grant-partial-settle-v1",
            ]),
            Ok(Command::RetryBlockedTerminal {
                submission_id,
                repair_revision: "credit-grant-partial-settle-v1".to_string(),
            })
        );
        assert_eq!(
            parse_command([
                "retry-blocked-terminal",
                &submission_id.to_string(),
                "artifact_integrity",
                "credit-grant-partial-settle-v1",
            ]),
            Err("retry-blocked-terminal only accepts canonical_conflict".to_string())
        );
    }

    #[test]
    fn rejects_missing_command() {
        assert_eq!(
            parse_command([] as [&str; 0]),
            Err("missing command: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-codex-cli-edit-profile`, `provision-grok-profile`, `provision-grok-video-profile`, `provision-grok-video-v2-profile`, `provision-grok-image-profile-replacement`, `provision-grok-edit-profile-replacement`, `provision-grok-video-profile-replacement`, `reconcile-execution-profile-routes`, `reconcile-dreamina-profiles`, `reconcile-inline-customer-settlement`, `terminalize-unstarted-job`, `cancel-unlaunched-job`, `refresh-provider-quota`, `refresh-codex-credential`, `resolve-dreamina-compliance-rejection`, `resolve-dreamina-image-success`, `resolve-dreamina-video-success`, `confirm-dreamina-video-cancellation`, `confirm-dreamina-video-supplement-cancellation`, `prepare-dreamina-video-supplement`, `prepare-dreamina-video-supplement-successor`, `dispatch-dreamina-video-supplement`, `poll-dreamina-video-supplement`, `recover-dreamina-video-supplement-acceptance`, or `retry-blocked-terminal`".to_string())
        );
    }

    #[test]
    fn rejects_unknown_command() {
        assert_eq!(
            parse_command(["status"]),
            Err(
                "unknown command `status`: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-codex-cli-edit-profile`, `provision-grok-profile`, `provision-grok-video-profile`, `provision-grok-video-v2-profile`, `provision-grok-image-profile-replacement`, `provision-grok-edit-profile-replacement`, `provision-grok-video-profile-replacement`, `reconcile-execution-profile-routes`, `reconcile-dreamina-profiles`, `reconcile-inline-customer-settlement`, `terminalize-unstarted-job`, `cancel-unlaunched-job`, `refresh-provider-quota`, `refresh-codex-credential`, `resolve-dreamina-compliance-rejection`, `resolve-dreamina-image-success`, `resolve-dreamina-video-success`, `confirm-dreamina-video-cancellation`, `confirm-dreamina-video-supplement-cancellation`, `prepare-dreamina-video-supplement`, `prepare-dreamina-video-supplement-successor`, `dispatch-dreamina-video-supplement`, `poll-dreamina-video-supplement`, `recover-dreamina-video-supplement-acceptance`, or `retry-blocked-terminal`"
                    .to_string()
            )
        );
    }

    #[test]
    fn rejects_extra_arguments() {
        assert_eq!(
            parse_command(["migrate", "now"]),
            Err("unexpected argument `now`: command accepts no arguments".to_string())
        );
    }

    #[test]
    fn rejects_incomplete_bootstrap_admin_identity() {
        assert_eq!(
            parse_command(["bootstrap-admin", "owner@example.com"]),
            Err("bootstrap-admin requires display name".to_string())
        );
    }
}
