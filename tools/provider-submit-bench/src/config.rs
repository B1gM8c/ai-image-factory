use std::{env, io, path::PathBuf};

use serde::Serialize;

use crate::BenchResult;

const ACK_VALUE: &str = "isolated-test-database-v1";

#[derive(Clone, Debug)]
pub struct BenchConfig {
    pub database_url: String,
    pub queue_rows: usize,
    pub recovery_percent: u8,
    pub claimants: usize,
    pub seed_concurrency: usize,
    pub recovery_seed_lease_ms: i64,
    pub recovery_claim_lease_ms: i64,
    pub fresh_claim_lease_ms: i64,
    pub provider_timeout_ms: i64,
    pub sample_interval_ms: u64,
    pub output: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReportConfig {
    pub queue_rows: usize,
    pub recovery_rows: usize,
    pub fresh_rows: usize,
    pub recovery_percent: u8,
    pub claimants: usize,
    pub seed_concurrency: usize,
    pub recovery_seed_lease_ms: i64,
    pub recovery_claim_lease_ms: i64,
    pub fresh_claim_lease_ms: i64,
    pub provider_timeout_ms: i64,
    pub wait_sample_interval_ms: u64,
}

impl BenchConfig {
    pub fn from_env() -> BenchResult<Self> {
        if env::var("PROVIDER_SUBMIT_BENCH_ACK").as_deref() != Ok(ACK_VALUE) {
            return Err(invalid(format!(
                "PROVIDER_SUBMIT_BENCH_ACK must equal {ACK_VALUE}"
            )));
        }
        let database_url = required("TEST_DATABASE_URL")?;
        let queue_rows = parse_bounded("PROVIDER_SUBMIT_BENCH_QUEUE_ROWS", 4_096, 128, 1_000_000)?;
        let recovery_percent =
            parse_bounded("PROVIDER_SUBMIT_BENCH_RECOVERY_PERCENT", 20_u8, 1_u8, 99_u8)?;
        let claimants = parse_bounded("PROVIDER_SUBMIT_BENCH_CLAIMANTS", 64, 1, 64)?;
        let seed_concurrency = parse_bounded("PROVIDER_SUBMIT_BENCH_SEED_CONCURRENCY", 32, 1, 64)?;
        let recovery_seed_lease_ms = parse_bounded(
            "PROVIDER_SUBMIT_BENCH_RECOVERY_SEED_LEASE_MS",
            15_000_i64,
            5_000_i64,
            60_000_i64,
        )?;
        let recovery_claim_lease_ms = parse_bounded(
            "PROVIDER_SUBMIT_BENCH_RECOVERY_CLAIM_LEASE_MS",
            60_000_i64,
            1_000_i64,
            86_400_000_i64,
        )?;
        let fresh_claim_lease_ms = parse_bounded(
            "PROVIDER_SUBMIT_BENCH_FRESH_CLAIM_LEASE_MS",
            60_000_i64,
            1_000_i64,
            86_400_000_i64,
        )?;
        let provider_timeout_ms = parse_bounded(
            "PROVIDER_SUBMIT_BENCH_PROVIDER_TIMEOUT_MS",
            300_000_i64,
            60_000_i64,
            3_600_000_i64,
        )?;
        if provider_timeout_ms <= recovery_seed_lease_ms {
            return Err(invalid(
                "provider timeout must exceed the recovery seed lease",
            ));
        }
        let sample_interval_ms = parse_bounded(
            "PROVIDER_SUBMIT_BENCH_SAMPLE_INTERVAL_MS",
            2_u64,
            1_u64,
            100_u64,
        )?;
        let output = env::var_os("PROVIDER_SUBMIT_BENCH_OUTPUT")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        Ok(Self {
            database_url,
            queue_rows,
            recovery_percent,
            claimants,
            seed_concurrency,
            recovery_seed_lease_ms,
            recovery_claim_lease_ms,
            fresh_claim_lease_ms,
            provider_timeout_ms,
            sample_interval_ms,
            output,
        })
    }

    pub fn recovery_rows(&self) -> usize {
        self.queue_rows * usize::from(self.recovery_percent) / 100
    }

    pub fn fresh_rows(&self) -> usize {
        self.queue_rows - self.recovery_rows()
    }

    pub fn report(&self) -> ReportConfig {
        ReportConfig {
            queue_rows: self.queue_rows,
            recovery_rows: self.recovery_rows(),
            fresh_rows: self.fresh_rows(),
            recovery_percent: self.recovery_percent,
            claimants: self.claimants,
            seed_concurrency: self.seed_concurrency,
            recovery_seed_lease_ms: self.recovery_seed_lease_ms,
            recovery_claim_lease_ms: self.recovery_claim_lease_ms,
            fresh_claim_lease_ms: self.fresh_claim_lease_ms,
            provider_timeout_ms: self.provider_timeout_ms,
            wait_sample_interval_ms: self.sample_interval_ms,
        }
    }
}

fn required(name: &str) -> BenchResult<String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid(format!("{name} is required")))
}

fn parse_bounded<T>(name: &str, default: T, min: T, max: T) -> BenchResult<T>
where
    T: Copy + Ord + std::str::FromStr + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    parse_bounded_value(name, env::var(name).ok().as_deref(), default, min, max)
}

fn parse_bounded_value<T>(
    name: &str,
    value: Option<&str>,
    default: T,
    min: T,
    max: T,
) -> BenchResult<T>
where
    T: Copy + Ord + std::str::FromStr + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    let parsed = match value {
        Some(value) => value
            .parse()
            .map_err(|error| invalid(format!("{name} is invalid: {error}")))?,
        None => default,
    };
    if parsed < min || parsed > max {
        return Err(invalid(format!("{name} must be between {min} and {max}")));
    }
    Ok(parsed)
}

fn invalid(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_values_accept_defaults_and_edges() {
        assert_eq!(
            parse_bounded_value("x", None, 8_u64, 1, 64).expect("default"),
            8
        );
        assert_eq!(
            parse_bounded_value("x", Some("64"), 8_u64, 1, 64).expect("edge"),
            64
        );
    }

    #[test]
    fn bounded_values_reject_invalid_and_out_of_range_inputs() {
        assert!(parse_bounded_value("x", Some("bad"), 8_u64, 1, 64).is_err());
        assert!(parse_bounded_value("x", Some("0"), 8_u64, 1, 64).is_err());
        assert!(parse_bounded_value("x", Some("65"), 8_u64, 1, 64).is_err());
    }
}
