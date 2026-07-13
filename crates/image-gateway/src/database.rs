use std::{env, fmt::Display, str::FromStr};

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::ImageGatewayError;

pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;
pub const DEFAULT_DATABASE_SCHEMA: &str = "public";

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn connect_pool(
    database_url: &str,
    max_connections: u32,
) -> Result<PgPool, ImageGatewayError> {
    connect_pool_in_schema(database_url, max_connections, DEFAULT_DATABASE_SCHEMA).await
}

pub async fn connect_pool_with_schema(
    database_url: &str,
    max_connections: u32,
    schema: &str,
) -> Result<PgPool, ImageGatewayError> {
    connect_pool_in_schema(database_url, max_connections, schema).await
}

#[doc(hidden)]
pub async fn connect_test_pool_with_search_path(
    database_url: &str,
    max_connections: u32,
    search_path: &str,
) -> Result<PgPool, ImageGatewayError> {
    connect_pool_in_schema(database_url, max_connections, search_path).await
}

async fn connect_pool_in_schema(
    database_url: &str,
    max_connections: u32,
    schema: &str,
) -> Result<PgPool, ImageGatewayError> {
    if !is_simple_identifier(schema) {
        return Err(ImageGatewayError::config(
            "database search_path must be a simple identifier",
        ));
    }
    let connect_options = PgConnectOptions::from_str(database_url)
        .map_err(connection_error)?
        .options([("search_path", schema)]);
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

pub fn database_url_from_env() -> Result<String, ImageGatewayError> {
    let primary = env::var("DATABASE_URL").ok();
    let fallback = env::var("GATEWAY_DATABASE_URL").ok();
    resolve_database_url(primary.as_deref(), fallback.as_deref())
        .map(str::to_owned)
        .ok_or_else(|| {
            ImageGatewayError::config("DATABASE_URL or GATEWAY_DATABASE_URL is required")
        })
}

pub fn database_schema_from_env() -> Result<String, ImageGatewayError> {
    let schema = env::var("GATEWAY_DATABASE_SCHEMA").ok();
    resolve_database_schema(schema.as_deref()).map(str::to_owned)
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

    let latest_known_version = MIGRATOR
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap_or_default();

    for (version, success, checksum) in &applied {
        if *version > latest_known_version {
            return Err(verification_error(format!(
                "database migration version {version} is newer than this binary"
            )));
        }
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

fn resolve_database_url<'a>(
    primary: Option<&'a str>,
    fallback: Option<&'a str>,
) -> Option<&'a str> {
    primary
        .filter(|url| !url.trim().is_empty())
        .or_else(|| fallback.filter(|url| !url.trim().is_empty()))
}

fn resolve_database_schema(schema: Option<&str>) -> Result<&str, ImageGatewayError> {
    let schema = schema.unwrap_or(DEFAULT_DATABASE_SCHEMA);
    if is_simple_identifier(schema) {
        Ok(schema)
    } else {
        Err(ImageGatewayError::config(
            "GATEWAY_DATABASE_SCHEMA must be a simple identifier",
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::{resolve_database_schema, resolve_database_url};

    #[test]
    fn database_url_uses_nonblank_primary_then_nonblank_fallback() {
        assert_eq!(
            resolve_database_url(Some("postgres://primary"), Some("postgres://fallback")),
            Some("postgres://primary")
        );
        assert_eq!(
            resolve_database_url(Some(" \t"), Some("postgres://fallback")),
            Some("postgres://fallback")
        );
        assert_eq!(resolve_database_url(Some(""), Some(" \n")), None);
    }

    #[test]
    fn database_schema_defaults_to_public_and_rejects_unsafe_identifiers() {
        assert_eq!(resolve_database_schema(None).unwrap(), "public");
        assert_eq!(
            resolve_database_schema(Some("tenant_a")).unwrap(),
            "tenant_a"
        );
        assert!(resolve_database_schema(Some("tenant-a")).is_err());
        assert!(resolve_database_schema(Some("public, attacker")).is_err());
        assert!(resolve_database_schema(Some(" ")).is_err());
    }
}
