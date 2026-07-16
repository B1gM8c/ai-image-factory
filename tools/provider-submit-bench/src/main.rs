mod config;
mod database;
mod fixture;
mod report;
mod workload;

use std::{error::Error, io};

use config::BenchConfig;
use database::BenchDatabase;
use fixture::{analyze_scheduler_tables, seed_prepared_queue, seed_recovery_queue};
use workload::run_workload;

type BenchResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> BenchResult {
    let config = BenchConfig::from_env()?;
    let database = BenchDatabase::create(&config).await?;
    let benchmark = async {
        seed_prepared_queue(&database.pool, &config).await?;
        seed_recovery_queue(&database.pool, &config).await?;
        analyze_scheduler_tables(&database.pool).await?;
        run_workload(&database.pool, &config).await
    }
    .await;
    let cleanup = database.cleanup().await;

    let report = match (benchmark, cleanup) {
        (Ok(report), Ok(())) => report,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(error),
        (Err(error), Err(cleanup)) => {
            return Err(io::Error::other(format!(
                "benchmark failed: {error}; cleanup failed: {cleanup}"
            ))
            .into());
        }
    };
    let json = serde_json::to_string_pretty(&report)?;
    if let Some(output) = &config.output {
        tokio::fs::write(output, format!("{json}\n")).await?;
    }
    println!("{json}");
    Ok(())
}
