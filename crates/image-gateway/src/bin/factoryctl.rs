use std::env;

use gpt_image_2_gateway::{
    ImageGatewayError,
    database::{DEFAULT_MAX_CONNECTIONS, connect_pool, run_migrations},
};

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Migrate,
}

fn parse_command<I, S>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut args = args.into_iter();
    let command = args
        .next()
        .ok_or_else(|| "missing command: expected `migrate`".to_string())?;
    if command.as_ref() != "migrate" {
        return Err(format!(
            "unknown command `{}`: expected `migrate`",
            command.as_ref()
        ));
    }
    if let Some(extra) = args.next() {
        return Err(format!(
            "unexpected argument `{}`: `migrate` accepts no arguments",
            extra.as_ref()
        ));
    }
    Ok(Command::Migrate)
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
    match command {
        Command::Migrate => {
            let database_url = env::var("DATABASE_URL")
                .ok()
                .filter(|url| !url.trim().is_empty())
                .or_else(|| env::var("GATEWAY_DATABASE_URL").ok())
                .filter(|url| !url.trim().is_empty())
                .ok_or_else(|| {
                    ImageGatewayError::config("DATABASE_URL or GATEWAY_DATABASE_URL is required")
                })?;
            let pool = connect_pool(&database_url, DEFAULT_MAX_CONNECTIONS).await?;
            run_migrations(&pool).await?;
            println!("database migrations complete");
        }
    }
    Ok(())
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
    fn rejects_missing_command() {
        assert_eq!(
            parse_command([] as [&str; 0]),
            Err("missing command: expected `migrate`".to_string())
        );
    }

    #[test]
    fn rejects_unknown_command() {
        assert_eq!(
            parse_command(["status"]),
            Err("unknown command `status`: expected `migrate`".to_string())
        );
    }

    #[test]
    fn rejects_extra_arguments() {
        assert_eq!(
            parse_command(["migrate", "now"]),
            Err("unexpected argument `now`: `migrate` accepts no arguments".to_string())
        );
    }
}
