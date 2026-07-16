use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use gpt_image_2_gateway::{
    ExecutorSubmissionStore, PostgresExecutorSubmissionStore, PostgresProviderTaskStore,
    ProviderTaskClaimScope, ProviderTaskStore,
};
use sqlx::PgPool;
use tokio::task::JoinSet;

use crate::{
    BenchResult,
    config::BenchConfig,
    database::APPLICATION_NAME,
    fixture::{ACCOUNT_ID, POLICY_ID, PROVIDER_ID, executor_scope},
    report::{
        BenchmarkReport, CorrectnessReport, EnvironmentReport, LatencyReport, WaitSamplingReport,
        WorkloadReport,
    },
};

#[derive(Clone, Copy)]
enum AcquiredKind {
    Recovery,
    Fresh,
}

struct AcquiredSample {
    kind: AcquiredKind,
    latency_us: u64,
    ordinal: u64,
}

#[derive(Default)]
struct WorkerReport {
    samples: Vec<AcquiredSample>,
    empty_cycles: u64,
}

#[derive(Default)]
struct WaitAccumulator {
    samples: u64,
    sampled_waiting_backends: u64,
    lock_wait_samples: u64,
    events: BTreeMap<String, u64>,
}

pub async fn run_workload(pool: &PgPool, config: &BenchConfig) -> BenchResult<BenchmarkReport> {
    let before = statistics_snapshot(pool).await?;
    let stop_sampler = Arc::new(AtomicBool::new(false));
    let wait_accumulator = Arc::new(Mutex::new(WaitAccumulator::default()));
    let sampler = tokio::spawn(sample_waits(
        pool.clone(),
        config.sample_interval_ms,
        stop_sampler.clone(),
        wait_accumulator.clone(),
    ));

    let acquired = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicU64::new(0));
    let ordinals = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut workers = JoinSet::new();
    for worker_index in 0..config.claimants {
        workers.spawn(run_claimant(
            pool.clone(),
            config.clone(),
            worker_index,
            acquired.clone(),
            attempts.clone(),
            ordinals.clone(),
        ));
    }
    let mut worker_reports = Vec::with_capacity(config.claimants);
    while let Some(result) = workers.join_next().await {
        worker_reports
            .push(result.map_err(|error| io::Error::other(format!("claimant failed: {error}")))??);
    }
    let elapsed = start.elapsed();
    stop_sampler.store(true, Ordering::Release);
    sampler
        .await
        .map_err(|error| io::Error::other(format!("wait sampler failed: {error}")))??;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let after = statistics_snapshot(pool).await?;
    let correctness = correctness_report(pool, config).await?;
    if !correctness.exact_once_projection {
        return Err(io::Error::other("post-workload exact-once projection failed").into());
    }

    let mut all = Vec::with_capacity(config.queue_rows);
    let mut recovery = Vec::with_capacity(config.recovery_rows());
    let mut fresh = Vec::with_capacity(config.fresh_rows());
    let mut empty_cycles = 0_u64;
    let mut first_recovery_ordinal = None;
    let mut first_fresh_ordinal = None;
    for report in worker_reports {
        empty_cycles += report.empty_cycles;
        for sample in report.samples {
            all.push(sample.latency_us);
            match sample.kind {
                AcquiredKind::Recovery => {
                    recovery.push(sample.latency_us);
                    first_recovery_ordinal = Some(
                        first_recovery_ordinal
                            .map_or(sample.ordinal, |current: u64| current.min(sample.ordinal)),
                    );
                }
                AcquiredKind::Fresh => {
                    fresh.push(sample.latency_us);
                    first_fresh_ordinal = Some(
                        first_fresh_ordinal
                            .map_or(sample.ordinal, |current: u64| current.min(sample.ordinal)),
                    );
                }
            }
        }
    }
    let wait = Arc::try_unwrap(wait_accumulator)
        .map_err(|_| io::Error::other("wait sampler retained shared state"))?
        .into_inner()
        .map_err(|_| io::Error::other("wait sampler state was poisoned"))?;
    let duration_seconds = elapsed.as_secs_f64();
    let generated_at_database_ms = database_now(pool).await?;
    Ok(BenchmarkReport {
        schema: "ai-image-factory.provider-submit-benchmark.v1",
        generated_at_database_ms,
        configuration: config.report(),
        environment: environment_report(pool).await?,
        workload: WorkloadReport {
            duration_ms: duration_seconds * 1_000.0,
            throughput_ops_per_second: config.queue_rows as f64 / duration_seconds,
            acquired_total: all.len(),
            acquired_recovery: recovery.len(),
            acquired_fresh: fresh.len(),
            empty_cycles,
            first_recovery_ordinal,
            first_fresh_ordinal,
            successful_acquire_latency_us: LatencyReport::from_samples(all),
            recovery_acquire_latency_us: LatencyReport::from_samples(recovery),
            fresh_acquire_latency_us: LatencyReport::from_samples(fresh),
            wal_bytes_delta: after.wal_bytes - before.wal_bytes,
            deadlocks_delta: after.deadlocks - before.deadlocks,
            wait_sampling: WaitSamplingReport {
                samples: wait.samples,
                sampled_waiting_backends: wait.sampled_waiting_backends,
                lock_wait_samples: wait.lock_wait_samples,
                events: wait.events,
            },
        },
        correctness,
    })
}

async fn run_claimant(
    pool: PgPool,
    config: BenchConfig,
    worker_index: usize,
    acquired: Arc<AtomicUsize>,
    attempts: Arc<AtomicU64>,
    ordinals: Arc<AtomicU64>,
) -> BenchResult<WorkerReport> {
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let provider = PostgresProviderTaskStore::new(pool);
    let provider_scope = ProviderTaskClaimScope {
        provider_id: PROVIDER_ID.to_string(),
        provider_account_id: ACCOUNT_ID,
    };
    let mut report = WorkerReport::default();
    while acquired.load(Ordering::Acquire) < config.queue_rows {
        let attempt = attempts.fetch_add(1, Ordering::Relaxed);
        let owner = format!("bench-{worker_index}-{attempt}");
        let started = Instant::now();
        if provider
            .resolve_due_submit_deadline(&provider_scope)
            .await?
            .is_some()
        {
            return Err(io::Error::other("benchmark unexpectedly resolved a deadline").into());
        }
        let command_id = format!("claim-{worker_index}-{attempt}");
        let kind = if provider
            .claim_submit_recovery(
                &provider_scope,
                &owner,
                &command_id,
                config.recovery_claim_lease_ms,
            )
            .await?
            .is_some()
        {
            Some(AcquiredKind::Recovery)
        } else if executor
            .claim_prepared(&executor_scope(), &owner, config.fresh_claim_lease_ms)
            .await?
            .is_some()
        {
            Some(AcquiredKind::Fresh)
        } else {
            None
        };
        if let Some(kind) = kind {
            let previous = acquired.fetch_add(1, Ordering::AcqRel);
            if previous >= config.queue_rows {
                return Err(io::Error::other("claim count exceeded seeded queue").into());
            }
            report.samples.push(AcquiredSample {
                kind,
                latency_us: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                ordinal: ordinals.fetch_add(1, Ordering::Relaxed),
            });
        } else {
            report.empty_cycles += 1;
            tokio::task::yield_now().await;
        }
    }
    Ok(report)
}

async fn sample_waits(
    pool: PgPool,
    interval_ms: u64,
    stop: Arc<AtomicBool>,
    accumulator: Arc<Mutex<WaitAccumulator>>,
) -> BenchResult {
    while !stop.load(Ordering::Acquire) {
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            r#"
            SELECT wait_event_type, wait_event, COUNT(*)
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND application_name = $1
              AND state = 'active'
              AND wait_event_type IS NOT NULL
            GROUP BY wait_event_type, wait_event
            "#,
        )
        .bind(APPLICATION_NAME)
        .fetch_all(&pool)
        .await?;
        {
            let mut accumulator = accumulator
                .lock()
                .map_err(|_| io::Error::other("wait sampler state was poisoned"))?;
            accumulator.samples += 1;
            for (wait_type, wait_event, count) in rows {
                let count = u64::try_from(count)?;
                accumulator.sampled_waiting_backends += count;
                if wait_type == "Lock" {
                    accumulator.lock_wait_samples += count;
                }
                *accumulator
                    .events
                    .entry(format!("{wait_type}:{wait_event}"))
                    .or_default() += count;
            }
        }
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }
    Ok(())
}

struct StatisticsSnapshot {
    wal_bytes: i64,
    deadlocks: i64,
}

async fn statistics_snapshot(pool: &PgPool) -> BenchResult<StatisticsSnapshot> {
    sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(pool)
        .await?;
    let wal_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::TEXT")
        .fetch_one(pool)
        .await?;
    let wal_bytes: i64 =
        sqlx::query_scalar("SELECT pg_wal_lsn_diff($1::pg_lsn, '0/0'::pg_lsn)::BIGINT")
            .bind(wal_lsn)
            .fetch_one(pool)
            .await?;
    let deadlocks: i64 = sqlx::query_scalar(
        "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await?;
    Ok(StatisticsSnapshot {
        wal_bytes,
        deadlocks,
    })
}

async fn environment_report(pool: &PgPool) -> BenchResult<EnvironmentReport> {
    let postgres_version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(pool)
        .await?;
    let postgres_version_num: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::INTEGER")
            .fetch_one(pool)
            .await?;
    let migration_version: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM _sqlx_migrations WHERE success")
            .fetch_one(pool)
            .await?;
    Ok(EnvironmentReport {
        postgres_version,
        postgres_version_num,
        target_os: std::env::consts::OS,
        target_arch: std::env::consts::ARCH,
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        migration_version,
        wal_measurement_scope: "cluster-wide LSN delta; upper bound if unrelated writers exist",
    })
}

async fn correctness_report(pool: &PgPool, config: &BenchConfig) -> BenchResult<CorrectnessReport> {
    let leased_fresh_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        WHERE execution.state = 'leased' AND submission.state = 'prepared'
          AND submission.execution_profile_id = $1
        "#,
    )
    .bind(executor_scope().execution_profile_id)
    .fetch_one(pool)
    .await?;
    let running_recovery_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM executor_executions execution
        JOIN provider_submit_recoveries recovery
          ON recovery.executor_execution_id = execution.executor_execution_id
         AND recovery.submission_id = execution.submission_id
        WHERE execution.state = 'running' AND recovery.state = 'active'
        "#,
    )
    .fetch_one(pool)
    .await?;
    let claimed_recovery_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_submit_recoveries WHERE recovery_owner IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    let recovery_claim_commands: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_submit_recovery_commands WHERE command_kind = 'claim'",
    )
    .fetch_one(pool)
    .await?;
    let held_capacity_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM executor_capacity_allocations WHERE state = 'held'",
    )
    .fetch_one(pool)
    .await?;
    let allocated_capacity_count: i32 = sqlx::query_scalar(
        "SELECT allocated_count FROM executor_resource_policies WHERE resource_policy_id = $1 AND revision = 1",
    )
    .bind(POLICY_ID)
    .fetch_one(pool)
    .await?;
    let distinct_fresh_owners: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(DISTINCT execution.executor_owner)
        FROM executor_executions execution
        JOIN provider_submissions submission
          ON submission.executor_execution_id = execution.executor_execution_id
         AND submission.submission_id = execution.submission_id
        WHERE execution.state = 'leased' AND submission.state = 'prepared'
          AND submission.execution_profile_id = $1
        "#,
    )
    .bind(executor_scope().execution_profile_id)
    .fetch_one(pool)
    .await?;
    let distinct_recovery_owners: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT recovery_owner) FROM provider_submit_recoveries WHERE recovery_owner IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    let remaining_prepared_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM executor_executions WHERE state = 'prepared'")
            .fetch_one(pool)
            .await?;
    let deadline_quarantined_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM provider_remote_submit_intents WHERE state = 'deadline_quarantined'",
    )
    .fetch_one(pool)
    .await?;
    let expected_fresh = i64::try_from(config.fresh_rows())?;
    let expected_recovery = i64::try_from(config.recovery_rows())?;
    let expected_total = i64::try_from(config.queue_rows)?;
    let exact_once_projection = leased_fresh_rows == expected_fresh
        && running_recovery_rows == expected_recovery
        && claimed_recovery_rows == expected_recovery
        && recovery_claim_commands == expected_recovery
        && held_capacity_rows == expected_total
        && i64::from(allocated_capacity_count) == expected_total
        && distinct_fresh_owners == expected_fresh
        && distinct_recovery_owners == expected_recovery
        && remaining_prepared_rows == 0
        && deadline_quarantined_rows == 0;
    Ok(CorrectnessReport {
        expected_total: config.queue_rows,
        leased_fresh_rows,
        running_recovery_rows,
        claimed_recovery_rows,
        recovery_claim_commands,
        held_capacity_rows,
        allocated_capacity_count,
        distinct_fresh_owners,
        distinct_recovery_owners,
        remaining_prepared_rows,
        deadline_quarantined_rows,
        exact_once_projection,
    })
}

async fn database_now(pool: &PgPool) -> BenchResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await?,
    )
}
