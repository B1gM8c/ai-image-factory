use std::{io, str::FromStr};

use gpt_image_2_gateway::database::run_migrations;
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

use crate::{BenchResult, config::BenchConfig};

pub const APPLICATION_NAME: &str = "ai-image-provider-submit-bench";

pub struct BenchDatabase {
    schema: String,
    pub pool: PgPool,
}

impl BenchDatabase {
    pub async fn create(config: &BenchConfig) -> BenchResult<Self> {
        let schema = format!("provider_submit_bench_{}", Uuid::new_v4().simple());
        let options = PgConnectOptions::from_str(&config.database_url)?
            .application_name(APPLICATION_NAME)
            .options([("search_path", schema.as_str())]);
        let max_connections = u32::try_from(config.claimants + 8)?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(io::Error::other(format!(
                "refusing benchmark DDL in non-test database {database_name}"
            ))
            .into());
        }
        let create_schema = format!("CREATE SCHEMA \"{schema}\"");
        if let Err(error) = sqlx::query(AssertSqlSafe(create_schema))
            .execute(&pool)
            .await
        {
            pool.close().await;
            return Err(error.into());
        }
        if let Err(error) = run_migrations(&pool).await {
            let drop_schema = format!("DROP SCHEMA \"{schema}\" CASCADE");
            let _ = sqlx::query(AssertSqlSafe(drop_schema)).execute(&pool).await;
            pool.close().await;
            return Err(io::Error::other(format!("migration failed: {error:?}")).into());
        }
        Ok(Self { schema, pool })
    }

    pub async fn cleanup(self) -> BenchResult {
        let drop_schema = format!("DROP SCHEMA \"{}\" CASCADE", self.schema);
        let result = sqlx::query(AssertSqlSafe(drop_schema))
            .execute(&self.pool)
            .await;
        self.pool.close().await;
        result?;
        Ok(())
    }
}
