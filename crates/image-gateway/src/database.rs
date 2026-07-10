use std::{fmt::Display, str::FromStr};

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::ImageGatewayError;

pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn connect_pool(
    database_url: &str,
    max_connections: u32,
) -> Result<PgPool, ImageGatewayError> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await
        .map_err(connection_error)
}

pub async fn connect_pool_with_search_path(
    database_url: &str,
    max_connections: u32,
    search_path: &str,
) -> Result<PgPool, ImageGatewayError> {
    if !is_simple_identifier(search_path) {
        return Err(ImageGatewayError::config(
            "database search_path must be a simple identifier",
        ));
    }
    let connect_options = PgConnectOptions::from_str(database_url)
        .map_err(connection_error)?
        .options([("search_path", search_path)]);
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(connect_options)
        .await
        .map_err(connection_error)
}

pub async fn run_migrations(pool: &PgPool) -> Result<(), ImageGatewayError> {
    MIGRATOR.run(pool).await.map_err(|error| {
        tracing::error!(error = %error, "database migration failed");
        ImageGatewayError::service_unavailable("failed to run database migrations")
    })
}

pub async fn verify_migrations(pool: &PgPool) -> Result<(), ImageGatewayError> {
    let migration_table: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations')::TEXT")
            .fetch_one(pool)
            .await
            .map_err(verification_query_error)?;
    if migration_table.is_none() {
        return Err(verification_error("migration metadata table is missing"));
    }

    let applied: Vec<(i64, bool, Vec<u8>)> =
        sqlx::query_as("SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(pool)
            .await
            .map_err(verification_query_error)?;

    for (version, success, checksum) in &applied {
        if !success {
            return Err(verification_error(format!(
                "migration version {version} is unsuccessful"
            )));
        }
        let Some(expected) = MIGRATOR
            .iter()
            .find(|migration| migration.version == *version)
        else {
            return Err(verification_error(format!(
                "unexpected migration version {version}"
            )));
        };
        if checksum.as_slice() != expected.checksum.as_ref() {
            return Err(verification_error(format!(
                "migration version {version} checksum mismatch"
            )));
        }
    }

    for expected in MIGRATOR.iter() {
        if !applied
            .iter()
            .any(|(version, _, _)| *version == expected.version)
        {
            return Err(verification_error(format!(
                "migration version {} is missing or pending",
                expected.version
            )));
        }
    }

    Ok(())
}

fn is_simple_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn verification_query_error(error: sqlx::Error) -> ImageGatewayError {
    verification_error(error)
}

fn connection_error(error: sqlx::Error) -> ImageGatewayError {
    tracing::error!(error = %error, "PostgreSQL connection failed");
    ImageGatewayError::service_unavailable("PostgreSQL is unavailable")
}

fn verification_error(error: impl Display) -> ImageGatewayError {
    tracing::error!(error = %error, "database migration verification failed");
    ImageGatewayError::service_unavailable("failed to verify database migrations")
}
