use std::{env, path::PathBuf};

use gpt_image_2_gateway::{
    CodexExecutionProfileProvisioning, CodexProfileProvisioningError, ImageGatewayError,
    codex_auth_file_sha256,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, run_migrations, verify_migrations,
    },
    grok_auth_file_sha256,
    provider_management::PostgresProviderManagementService,
    provision_codex_execution_profile, provision_grok_execution_profile,
    provision_grok_video_execution_profile,
};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Migrate,
    ProvisionCodexProfile,
    ProvisionGrokProfile,
    ProvisionGrokVideoProfile,
    ReconcileDreaminaProfiles,
    BootstrapAdmin { email: String, display_name: String },
}

fn parse_command<I, S>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut args = args.into_iter();
    let command = args.next().ok_or_else(|| {
        "missing command: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-grok-profile`, `provision-grok-video-profile`, or `reconcile-dreamina-profiles`"
            .to_string()
    })?;
    let command = match command.as_ref() {
        "migrate" => Command::Migrate,
        "bootstrap-admin" => {
            let email = required_argument(&mut args, "email")?;
            let display_name = required_argument(&mut args, "display name")?;
            Command::BootstrapAdmin {
                email,
                display_name,
            }
        }
        "provision-codex-profile" => Command::ProvisionCodexProfile,
        "provision-grok-profile" => Command::ProvisionGrokProfile,
        "provision-grok-video-profile" => Command::ProvisionGrokVideoProfile,
        "reconcile-dreamina-profiles" => Command::ReconcileDreaminaProfiles,
        value => {
            return Err(format!(
                "unknown command `{value}`: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-grok-profile`, `provision-grok-video-profile`, or `reconcile-dreamina-profiles`"
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

fn required_argument<I, S>(args: &mut I, name: &str) -> Result<String, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    args.next()
        .map(|value| value.as_ref().trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("bootstrap-admin requires {name}"))
}

fn init_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[tokio::main]
async fn main() -> Result<(), ImageGatewayError> {
    init_tracing();
    let command = parse_command(env::args().skip(1)).map_err(ImageGatewayError::config)?;
    let provisioning = match &command {
        Command::Migrate => None,
        Command::ProvisionCodexProfile => {
            let credential_home = provider_credential_home("EXECUTOR_CODEX_CREDENTIAL_HOME")?;
            Some(provisioning_from_env(codex_auth_file_sha256(
                credential_home,
            )?)?)
        }
        Command::ProvisionGrokProfile | Command::ProvisionGrokVideoProfile => {
            let credential_home = provider_credential_home("EXECUTOR_GROK_CREDENTIAL_HOME")?;
            Some(provisioning_from_env(grok_auth_file_sha256(
                credential_home,
            )?)?)
        }
        Command::BootstrapAdmin { .. } | Command::ReconcileDreaminaProfiles => None,
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
        Command::ReconcileDreaminaProfiles => {
            verify_migrations(&pool).await?;
            let created =
                PostgresProviderManagementService::reconcile_dreamina_video_profiles(&pool).await?;
            println!("Dreamina video profiles reconciled: {created} created");
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
    use super::{Command, init_tracing, parse_command};

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
    fn accepts_exactly_reconcile_dreamina_profiles() {
        assert_eq!(
            parse_command(["reconcile-dreamina-profiles"]),
            Ok(Command::ReconcileDreaminaProfiles)
        );
    }

    #[test]
    fn rejects_missing_command() {
        assert_eq!(
            parse_command([] as [&str; 0]),
            Err("missing command: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-grok-profile`, `provision-grok-video-profile`, or `reconcile-dreamina-profiles`".to_string())
        );
    }

    #[test]
    fn rejects_unknown_command() {
        assert_eq!(
            parse_command(["status"]),
            Err(
                "unknown command `status`: expected `migrate`, `bootstrap-admin`, `provision-codex-profile`, `provision-grok-profile`, `provision-grok-video-profile`, or `reconcile-dreamina-profiles`"
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
