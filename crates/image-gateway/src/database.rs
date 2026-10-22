use std::{env, fmt::Display, str::FromStr, time::Duration};

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::ImageGatewayError;

pub const DEFAULT_MAX_CONNECTIONS: u32 = 10;
pub const ADMIN_READ_MAX_CONNECTIONS: u32 = 3;
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

pub async fn connect_admin_read_pool_with_schema(
    database_url: &str,
    schema: &str,
) -> Result<PgPool, ImageGatewayError> {
    if !is_simple_identifier(schema) {
        return Err(ImageGatewayError::config(
            "database search_path must be a simple identifier",
        ));
    }
    let connect_options = PgConnectOptions::from_str(database_url)
        .map_err(connection_error)?
        .application_name("ai-image-factory-admin-read")
        .options([
            ("search_path", schema),
            ("default_transaction_read_only", "on"),
            ("statement_timeout", "500ms"),
            ("lock_timeout", "100ms"),
            ("idle_in_transaction_session_timeout", "2s"),
        ]);
    PgPoolOptions::new()
        .max_connections(ADMIN_READ_MAX_CONNECTIONS)
        .acquire_timeout(Duration::from_secs(1))
        .connect_with(connect_options)
        .await
        .map_err(connection_error)
}

/// The optional sidecar has its own fault budget; never apply it to image jobs.
pub async fn connect_media_segments_pool_with_schema(
    database_url: &str,
    schema: &str,
) -> Result<PgPool, ImageGatewayError> {
    if !is_simple_identifier(schema) {
        return Err(ImageGatewayError::config(
            "database search_path must be a simple identifier",
        ));
    }
    let connect_options = PgConnectOptions::from_str(database_url)
        .map_err(connection_error)?
        .application_name("ai-image-factory-media-segments")
        .options([
            ("search_path", schema),
            ("statement_timeout", "4s"),
            ("lock_timeout", "2s"),
            ("idle_in_transaction_session_timeout", "10s"),
        ]);
    PgPoolOptions::new()
        .max_connections(3)
        .acquire_timeout(Duration::from_secs(1))
        .connect_with(connect_options)
        .await
        .map_err(connection_error)
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

pub fn admin_read_database_url_from_env(primary_url: &str) -> String {
    resolve_admin_read_database_url(
        env::var("GATEWAY_ADMIN_READ_DATABASE_URL").ok().as_deref(),
        primary_url,
    )
    .to_owned()
}

pub fn database_schema_from_env() -> Result<String, ImageGatewayError> {
    let schema = env::var("GATEWAY_DATABASE_SCHEMA").ok();
    resolve_database_schema(schema.as_deref()).map(str::to_owned)
}

/// Per-process pool budget for workerd, provider-submitd and provider-pollerd.
/// This does not change account or generation concurrency.
pub fn worker_database_max_connections_from_env() -> Result<u32, ImageGatewayError> {
    let value = match env::var("GATEWAY_WORKER_DB_MAX_CONNECTIONS") {
        Ok(value) => Some(value),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            return Err(ImageGatewayError::config(
                "GATEWAY_WORKER_DB_MAX_CONNECTIONS must be valid UTF-8",
            ));
        }
    };
    resolve_worker_database_max_connections(value.as_deref())
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

fn resolve_admin_read_database_url<'a>(configured: Option<&'a str>, primary: &'a str) -> &'a str {
    configured
        .filter(|url| !url.trim().is_empty())
        .unwrap_or(primary)
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

fn resolve_worker_database_max_connections(value: Option<&str>) -> Result<u32, ImageGatewayError> {
    let max_connections = value
        .map(str::parse::<u32>)
        .transpose()
        .map_err(|_| {
            ImageGatewayError::config("GATEWAY_WORKER_DB_MAX_CONNECTIONS must be an integer")
        })?
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);
    if !(1..=64).contains(&max_connections) {
        return Err(ImageGatewayError::config(
            "GATEWAY_WORKER_DB_MAX_CONNECTIONS must be between 1 and 64",
        ));
    }
    Ok(max_connections)
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
    use super::{
        connect_pool, resolve_admin_read_database_url, resolve_database_schema,
        resolve_database_url, resolve_worker_database_max_connections,
    };

    #[test]
    fn worker_database_pool_budget_preserves_default_and_validates_overrides() {
        assert_eq!(resolve_worker_database_max_connections(None).unwrap(), 10);
        for value in [1, 2, 10, 64] {
            assert_eq!(
                resolve_worker_database_max_connections(Some(&value.to_string())).unwrap(),
                value
            );
        }
        for value in ["", " ", "two", "1.5", "-1", "0", "65", "4294967296"] {
            assert!(
                resolve_worker_database_max_connections(Some(value)).is_err(),
                "accepted invalid connection budget {value:?}"
            );
        }
    }

    #[tokio::test]
    async fn six_worker_pools_bound_connections_and_wait_without_exceeding_budget() {
        let Some(database_url) = std::env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            assert!(
                std::env::var_os("CI").is_none(),
                "TEST_DATABASE_URL must be set in CI"
            );
            eprintln!("skipping worker pool budget test: TEST_DATABASE_URL is not set");
            return;
        };
        let max_connections = resolve_worker_database_max_connections(Some("2")).unwrap();
        let mut pools = Vec::new();
        let mut connections = Vec::new();
        let mut backend_pids = std::collections::HashSet::new();
        for _ in 0..6 {
            let pool = connect_pool(&database_url, max_connections).await.unwrap();
            // Cloned stores and heartbeat tasks share each process's budget.
            for _ in 0..2 {
                let mut connection = pool.clone().acquire().await.unwrap();
                let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *connection)
                    .await
                    .unwrap();
                backend_pids.insert(pid);
                connections.push(connection);
            }
            pools.push(pool);
        }
        assert_eq!(backend_pids.len(), 12);
        let saturated = pools.last().unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), saturated.acquire())
                .await
                .is_err()
        );
        assert_eq!(pools.iter().map(sqlx::PgPool::size).sum::<u32>(), 12);
        drop(connections.pop());
        let mut resumed =
            tokio::time::timeout(std::time::Duration::from_secs(5), saturated.acquire())
                .await
                .unwrap()
                .unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *resumed)
            .await
            .unwrap();
        assert!(backend_pids.contains(&pid));
        drop(resumed);
        drop(connections);
        for pool in pools {
            pool.close().await;
        }
    }

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

    #[test]
    fn admin_read_database_url_prefers_a_nonblank_dedicated_role_url() {
        assert_eq!(
            resolve_admin_read_database_url(
                Some("postgres://admin-reader"),
                "postgres://gateway-writer"
            ),
            "postgres://admin-reader"
        );
        assert_eq!(
            resolve_admin_read_database_url(Some("  "), "postgres://gateway-writer"),
            "postgres://gateway-writer"
        );
    }
}
