use std::{env, path::PathBuf};

use gpt_image_2_gateway::{
    CodexExecutionProfileProvisioning, CodexProfileProvisioningError, ImageGatewayError,
    codex_auth_file_sha256,
    database::{
        DEFAULT_MAX_CONNECTIONS, connect_pool_with_schema, database_schema_from_env,
        database_url_from_env, run_migrations, verify_migrations,
    },
    provision_codex_execution_profile,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Command {
    Migrate,
    ProvisionCodexProfile,
}

fn parse_command<I, S>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut args = args.into_iter();
    let command = args.next().ok_or_else(|| {
        "missing command: expected `migrate` or `provision-codex-profile`".to_string()
    })?;
    let command = match command.as_ref() {
        "migrate" => Command::Migrate,
        "provision-codex-profile" => Command::ProvisionCodexProfile,
        value => {
            return Err(format!(
                "unknown command `{value}`: expected `migrate` or `provision-codex-profile`"
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
    let provisioning = match command {
        Command::Migrate => None,
        Command::ProvisionCodexProfile => {
            let credential_home = absolute_env_path("EXECUTOR_CODEX_CREDENTIAL_HOME")?;
            Some(provisioning_from_env(codex_auth_file_sha256(
                credential_home,
            )?)?)
        }
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
                .map_err(map_provisioning_error)?;
            println!(
                "Codex execution profile provisioned: {}",
                provisioned.execution_profile_id
            );
        }
    }
    Ok(())
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

fn map_provisioning_error(error: CodexProfileProvisioningError) -> ImageGatewayError {
    match error {
        CodexProfileProvisioningError::InvalidInput => {
            ImageGatewayError::config("Codex execution profile provisioning input is invalid")
        }
        CodexProfileProvisioningError::Conflict => ImageGatewayError::config(
            "Codex execution profile provisioning conflicts with durable identity",
        ),
        CodexProfileProvisioningError::Unavailable => ImageGatewayError::service_unavailable(
            "Codex execution profile provisioning storage is unavailable",
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
    fn accepts_exactly_provision_codex_profile() {
        assert_eq!(
            parse_command(["provision-codex-profile"]),
            Ok(Command::ProvisionCodexProfile)
        );
    }

    #[test]
    fn rejects_missing_command() {
        assert_eq!(
            parse_command([] as [&str; 0]),
            Err("missing command: expected `migrate` or `provision-codex-profile`".to_string())
        );
    }

    #[test]
    fn rejects_unknown_command() {
        assert_eq!(
            parse_command(["status"]),
            Err(
                "unknown command `status`: expected `migrate` or `provision-codex-profile`"
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
}
