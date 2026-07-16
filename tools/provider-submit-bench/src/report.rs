use std::collections::BTreeMap;

use serde::Serialize;

use crate::config::ReportConfig;

#[derive(Debug, Serialize)]
pub struct BenchmarkReport {
    pub schema: &'static str,
    pub generated_at_database_ms: i64,
    pub configuration: ReportConfig,
    pub environment: EnvironmentReport,
    pub workload: WorkloadReport,
    pub correctness: CorrectnessReport,
}

#[derive(Debug, Serialize)]
pub struct EnvironmentReport {
    pub postgres_version: String,
    pub postgres_version_num: i32,
    pub target_os: &'static str,
    pub target_arch: &'static str,
    pub logical_cpus: usize,
    pub migration_version: i64,
    pub wal_measurement_scope: &'static str,
}

#[derive(Debug, Serialize)]
pub struct WorkloadReport {
    pub duration_ms: f64,
    pub throughput_ops_per_second: f64,
    pub acquired_total: usize,
    pub acquired_recovery: usize,
    pub acquired_fresh: usize,
    pub empty_cycles: u64,
    pub first_recovery_ordinal: Option<u64>,
    pub first_fresh_ordinal: Option<u64>,
    pub successful_acquire_latency_us: LatencyReport,
    pub recovery_acquire_latency_us: LatencyReport,
    pub fresh_acquire_latency_us: LatencyReport,
    pub wal_bytes_delta: i64,
    pub deadlocks_delta: i64,
    pub wait_sampling: WaitSamplingReport,
}

#[derive(Debug, Default, Serialize)]
pub struct LatencyReport {
    pub samples: usize,
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

impl LatencyReport {
    pub fn from_samples(mut samples: Vec<u64>) -> Self {
        if samples.is_empty() {
            return Self::default();
        }
        samples.sort_unstable();
        Self {
            samples: samples.len(),
            min: samples[0],
            p50: percentile(&samples, 50),
            p95: percentile(&samples, 95),
            p99: percentile(&samples, 99),
            max: samples[samples.len() - 1],
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WaitSamplingReport {
    pub samples: u64,
    pub sampled_waiting_backends: u64,
    pub lock_wait_samples: u64,
    pub events: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub struct CorrectnessReport {
    pub expected_total: usize,
    pub leased_fresh_rows: i64,
    pub running_recovery_rows: i64,
    pub claimed_recovery_rows: i64,
    pub recovery_claim_commands: i64,
    pub held_capacity_rows: i64,
    pub allocated_capacity_count: i32,
    pub distinct_fresh_owners: i64,
    pub distinct_recovery_owners: i64,
    pub remaining_prepared_rows: i64,
    pub deadline_quarantined_rows: i64,
    pub exact_once_projection: bool,
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let rank = (samples.len() * percentile).div_ceil(100);
    samples[rank.saturating_sub(1).min(samples.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_report_uses_nearest_rank_percentiles() {
        let report = LatencyReport::from_samples((1..=100).collect());
        assert_eq!(report.min, 1);
        assert_eq!(report.p50, 50);
        assert_eq!(report.p95, 95);
        assert_eq!(report.p99, 99);
        assert_eq!(report.max, 100);
    }

    #[test]
    fn empty_latency_report_is_explicitly_zeroed() {
        assert_eq!(LatencyReport::from_samples(Vec::new()).samples, 0);
    }
}
