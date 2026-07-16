use std::{io, time::Duration};

use gpt_image_2_gateway::{
    ExecutorClaimScope, ExecutorHandoffStore, ExecutorSubmissionStore,
    PostgresExecutorSubmissionStore, PostgresProviderTaskStore, ProviderSubmitAcquire,
    ProviderTaskStore, RemoteTaskSubmitReservation, admission::WorkLease,
};
use image_provider_sdk::{OutputSlot, SingleOutputCommand};
use image_provider_test_support::TestPayload;
use serde_json::json;
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{BenchResult, config::BenchConfig};

const PROFILE_ID: Uuid = Uuid::from_u128(0x10000000000040008000000000000001);
const POOL_ID: Uuid = Uuid::from_u128(0x10000000000040008000000000000002);
pub const ACCOUNT_ID: Uuid = Uuid::from_u128(0x10000000000040008000000000000003);
pub const POLICY_ID: Uuid = Uuid::from_u128(0x10000000000040008000000000000004);
pub const PROVIDER_ID: &str = "provider-test";
const MODEL: &str = "model-test";
const COMMAND_SCHEMA: &str = "provider-command-v1";
const ADAPTER_REVISION: &str = "provider-test-adapter-v1";

pub fn executor_scope() -> ExecutorClaimScope {
    ExecutorClaimScope {
        execution_profile_id: PROFILE_ID,
        provider_id: PROVIDER_ID.to_string(),
        command_schema: COMMAND_SCHEMA.to_string(),
        adapter_revision: ADAPTER_REVISION.to_string(),
    }
}

pub async fn seed_prepared_queue(pool: &PgPool, config: &BenchConfig) -> BenchResult {
    seed_execution_profile(pool, config.queue_rows).await?;
    let mut tasks = JoinSet::new();
    for index in 0..config.queue_rows {
        if tasks.len() >= config.seed_concurrency {
            join_fixture_task(&mut tasks).await?;
        }
        let pool = pool.clone();
        tasks.spawn(async move { seed_prepared_submission(&pool, index).await });
    }
    while !tasks.is_empty() {
        join_fixture_task(&mut tasks).await?;
    }
    let prepared: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM executor_executions WHERE state = 'prepared'")
            .fetch_one(pool)
            .await?;
    if prepared != i64::try_from(config.queue_rows)? {
        return Err(io::Error::other(format!(
            "prepared queue mismatch: expected {}, found {prepared}",
            config.queue_rows
        ))
        .into());
    }
    Ok(())
}

pub async fn seed_recovery_queue(pool: &PgPool, config: &BenchConfig) -> BenchResult {
    let mut tasks = JoinSet::new();
    for index in 0..config.recovery_rows() {
        if tasks.len() >= config.seed_concurrency {
            join_fixture_task(&mut tasks).await?;
        }
        let pool = pool.clone();
        let config = config.clone();
        tasks.spawn(async move { seed_recovery(&pool, &config, index).await });
    }
    while !tasks.is_empty() {
        join_fixture_task(&mut tasks).await?;
    }
    wait_until_all_recoveries_due(pool, config.recovery_rows()).await
}

pub async fn analyze_scheduler_tables(pool: &PgPool) -> BenchResult {
    sqlx::raw_sql(
        r#"
        ANALYZE executor_executions;
        ANALYZE provider_submissions;
        ANALYZE executor_capacity_allocations;
        ANALYZE provider_remote_submit_intents;
        ANALYZE provider_submit_recoveries;
        ANALYZE provider_submit_recovery_commands;
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_execution_profile(pool: &PgPool, max_concurrency: usize) -> BenchResult {
    let now = database_now(pool).await?;
    let max_concurrency = i32::try_from(max_concurrency)?;
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO provider_credential_pools
          (credential_pool_id, pool_key, provider_id, state, created_at_ms, updated_at_ms)
        VALUES ($1, 'provider-submit-bench', $2, 'enabled', $3, $3)
        "#,
    )
    .bind(POOL_ID)
    .bind(PROVIDER_ID)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO provider_accounts
          (provider_account_id, credential_pool_id, provider_id, account_key,
           credential_ref, credential_revision, credential_auth_sha256,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 'provider-submit-bench',
                'test-vault.provider-submit-bench.1', 1, $4,
                'enabled', $5, $5)
        "#,
    )
    .bind(ACCOUNT_ID)
    .bind(POOL_ID)
    .bind(PROVIDER_ID)
    .bind("1".repeat(64))
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO executor_resource_policies
          (resource_policy_id, revision, credential_pool_id, provider_account_id,
           provider_id, execution_class, max_concurrency, state, created_at_ms)
        VALUES ($1, 1, $2, $3, $4, 'remote-task', $5, 'enabled', $6)
        "#,
    )
    .bind(POLICY_ID)
    .bind(POOL_ID)
    .bind(ACCOUNT_ID)
    .bind(PROVIDER_ID)
    .bind(max_concurrency)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO provider_execution_profiles
          (execution_profile_id, profile_key, provider_id, command_schema,
           operation_id, operation_descriptor_revision,
           operation_descriptor_sha256_v1, completion_mode, idempotency_mode,
           adapter_revision, credential_pool_id, provider_account_id,
           credential_ref, credential_revision, resource_policy_id,
           resource_policy_revision, state, created_at_ms, updated_at_ms)
        VALUES ($1, 'provider-submit-bench', $2, $3,
                'images.generations', 'provider-test/images.generations/v1',
                $4, 'remote_task', 'submission_bound', $5, $6, $7,
                'test-vault.provider-submit-bench.1', 1, $8, 1,
                'enabled', $9, $9)
        "#,
    )
    .bind(PROFILE_ID)
    .bind(PROVIDER_ID)
    .bind(COMMAND_SCHEMA)
    .bind("2".repeat(64))
    .bind(ADAPTER_REVISION)
    .bind(POOL_ID)
    .bind(ACCOUNT_ID)
    .bind(POLICY_ID)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn seed_prepared_submission(pool: &PgPool, index: usize) -> BenchResult {
    let job_id = Uuid::new_v4();
    let output_id = Uuid::new_v4();
    let work_item_id = Uuid::new_v4();
    let execution_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let owner = format!("seed-{index}");
    let request_id = format!("bench-request-{}", Uuid::new_v4().simple());
    let command = json!({
        "schema_version": 1,
        "operation": "generation",
        "n": 1,
        "prompt": "provider submit scheduler benchmark"
    });
    let now = database_now(pool).await?;
    let mut tx = pool.begin().await?;
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, economics_contract_version, created_at_ms, updated_at_ms)
        VALUES ($1, 'provider-submit-bench', $2, 'generation', $3, $4,
                'reserved', 1, 2, $5, $5)
        "#,
    )
    .bind(job_id)
    .bind(&request_id)
    .bind(PROVIDER_ID)
    .bind(MODEL)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO job_outputs
          (output_id, job_id, output_index, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 0, 'pending', $3, $3)
        "#,
    )
    .bind(output_id)
    .bind(job_id)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO admission_sessions
          (session_id, owner_token, tenant_id, project_id, api_profile, operation,
           request_id, request_hash, state, job_id, deadline_at_ms,
           created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'provider-submit-bench', 'provider-submit-bench',
                'openai-images-v1', 'generation', $3, $4, 'attached', $5,
                $6, $7, $7)
        "#,
    )
    .bind(session_id)
    .bind(Uuid::new_v4())
    .bind(&request_id)
    .bind("d".repeat(64))
    .bind(job_id)
    .bind(now + 300_000)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO job_payloads
          (job_id, admission_session_id, command_schema, command_json,
           request_hash, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(job_id)
    .bind(session_id)
    .bind(COMMAND_SCHEMA)
    .bind(&command)
    .bind("d".repeat(64))
    .bind(now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO work_items
          (work_item_id, job_id, kind, state, available_at_ms, lease_epoch,
           lease_owner, lease_expires_at_ms, execution_id, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'generation', 'leased', $3, 1, $4, $5, $6, $3, $3)
        "#,
    )
    .bind(work_item_id)
    .bind(job_id)
    .bind(now)
    .bind(&owner)
    .bind(now + 300_000)
    .bind(execution_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO job_attempts
          (attempt_id, execution_id, work_item_id, lease_epoch, worker_id,
           state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, 1, $4, 'claimed', $5, $5)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(execution_id)
    .bind(work_item_id)
    .bind(&owner)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let lease = WorkLease {
        work_item_id,
        job_id,
        execution_id,
        lease_epoch: 1,
        worker_id: owner,
        command_schema: COMMAND_SCHEMA.to_string(),
        command_json: command,
    };
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    if executor
        .prepare_and_handoff(&lease, PROFILE_ID)
        .await?
        .len()
        != 1
    {
        return Err(io::Error::other("handoff did not create exactly one submission").into());
    }
    Ok(())
}

async fn seed_recovery(pool: &PgPool, config: &BenchConfig, index: usize) -> BenchResult {
    let executor = PostgresExecutorSubmissionStore::new(pool.clone());
    let provider = PostgresProviderTaskStore::new(pool.clone());
    let owner = format!("recovery-seed-{index}");
    let lease = executor
        .claim_prepared(&executor_scope(), &owner, config.recovery_seed_lease_ms)
        .await?
        .ok_or_else(|| io::Error::other("prepared recovery seed was not claimable"))?;
    executor.start(&lease).await?;
    let output = OutputSlot::new(u32::try_from(lease.output_index)?, 1)?;
    let command = SingleOutputCommand::new(
        output,
        TestPayload::bound_to(lease.submission_id.as_bytes(), lease.command_hash.clone()),
    )?;
    let reservation = RemoteTaskSubmitReservation::new(
        &lease,
        format!("bench-submit-{}", lease.submission_id.simple()),
        output,
        command.identity(),
        config.provider_timeout_ms,
    );
    if !matches!(
        provider.acquire_submit(&reservation).await?,
        ProviderSubmitAcquire::Dispatch(_)
    ) {
        return Err(io::Error::other("recovery seed did not acquire submit dispatch").into());
    }
    Ok(())
}

async fn wait_until_all_recoveries_due(pool: &PgPool, expected: usize) -> BenchResult {
    let expected = i64::try_from(expected)?;
    let (count, latest_due): (i64, Option<i64>) = sqlx::query_as(
        r#"
        SELECT COUNT(*), MAX(next_recovery_at_ms)
        FROM provider_submit_recoveries
        WHERE state = 'active'
        "#,
    )
    .fetch_one(pool)
    .await?;
    if count != expected {
        return Err(io::Error::other(format!(
            "recovery queue mismatch: expected {expected}, found {count}"
        ))
        .into());
    }
    if let Some(latest_due) = latest_due {
        let now = database_now(pool).await?;
        if latest_due > now {
            tokio::time::sleep(Duration::from_millis(u64::try_from(latest_due - now)? + 10)).await;
        }
    }
    let due: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM provider_submit_recoveries
        WHERE state = 'active'
          AND GREATEST(
                next_recovery_at_ms,
                COALESCE(recovery_lease_expires_at_ms, next_recovery_at_ms)
              ) <= floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT
        "#,
    )
    .fetch_one(pool)
    .await?;
    if due != expected {
        return Err(io::Error::other(format!(
            "not all recoveries became due: expected {expected}, found {due}"
        ))
        .into());
    }
    Ok(())
}

async fn database_now(pool: &PgPool) -> BenchResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await?,
    )
}

fn join_error(error: tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("fixture task failed: {error}"))
}

async fn join_fixture_task(tasks: &mut JoinSet<BenchResult>) -> BenchResult {
    tasks
        .join_next()
        .await
        .ok_or_else(|| io::Error::other("fixture task set ended unexpectedly"))?
        .map_err(join_error)??;
    Ok(())
}
