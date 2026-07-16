use std::{env, time::Duration};

use gpt_image_2_gateway::{
    ApiKeyKeyring, ApiKeyStore, ImageGatewayError, PostgresApiKeyStore, PostgresUsageStore,
    UsageCharge, UsageLimits, UsageStore,
    database::{
        connect_pool, connect_test_pool_with_search_path, run_migrations, verify_migrations,
    },
};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::time::timeout;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, String>;

const REQUIRED_COLUMNS: [(&str, &str); 170] = [
    ("usage_events", "tenant_id"),
    ("quota_reservations", "tenant_id"),
    ("quota_reservations", "job_id"),
    ("quota_reservations", "committed_units"),
    ("jobs", "tenant_id"),
    ("jobs", "operation"),
    ("jobs", "provider_id"),
    ("jobs", "model"),
    ("jobs", "reservation_id"),
    ("jobs", "created_at_ms"),
    ("jobs", "updated_at_ms"),
    ("jobs", "last_error_code"),
    ("jobs", "last_error_message"),
    ("gateway_api_keys", "hash_algorithm"),
    ("gateway_api_keys", "pepper_version"),
    ("job_response_projections", "response_schema"),
    ("job_response_projections", "created_at_seconds"),
    ("job_response_projections", "artifact_count"),
    ("artifacts", "execution_id"),
    ("artifacts", "output_index"),
    ("artifacts", "sha256_hex"),
    ("quota_reservations", "limit_5h"),
    ("quota_reservations", "remaining_5h"),
    ("quota_reservations", "limit_7d"),
    ("quota_reservations", "remaining_7d"),
    ("quota_reservations", "admission_session_id"),
    ("admission_sessions", "input_cleanup_state"),
    ("admission_sessions", "input_cleanup_owner"),
    ("admission_sessions", "input_cleanup_lease_expires_at_ms"),
    ("admission_sessions", "input_cleanup_completed_at_ms"),
    ("job_input_manifests", "manifest_schema"),
    ("job_input_manifests", "manifest_hash"),
    ("job_input_objects", "role"),
    ("job_input_objects", "object_key"),
    ("job_input_objects", "sha256_hex"),
    ("job_response_projections", "operation"),
    ("executor_artifact_authorities", "authority_id"),
    ("executor_artifact_authorities", "storage_namespace"),
    ("executor_artifact_authorities", "sha256_hex"),
    ("executor_result_manifests", "artifact_authority_id"),
    ("executor_executions", "launch_owner"),
    ("executor_executions", "resolution_decision_id"),
    ("provider_submissions", "resolution_decision_id"),
    ("executor_runner_observations", "payload_hash"),
    ("executor_resolution_decisions", "source"),
    ("executor_resolution_decisions", "resolution_fingerprint"),
    ("provider_submissions", "execution_profile_id"),
    ("provider_submissions", "adapter_revision"),
    ("work_items", "execution_profile_id"),
    ("provider_execution_profiles", "credential_ref"),
    ("provider_accounts", "credential_ref"),
    ("provider_accounts", "credential_auth_sha256"),
    ("executor_resource_policies", "allocated_count"),
    ("executor_capacity_allocations", "state"),
    ("executor_capacity_allocations", "release_decision_id"),
    ("executor_capacity_allocations", "release_reconciliation_id"),
    ("work_items", "handed_off_at_ms"),
    ("job_attempts", "handed_off_at_ms"),
    ("executor_terminal_reductions", "submission_id"),
    ("executor_terminal_reductions", "executor_execution_id"),
    ("executor_terminal_reductions", "resolution_decision_id"),
    ("executor_terminal_reductions", "resolved_state"),
    ("executor_terminal_reductions", "state"),
    ("executor_terminal_reductions", "lease_owner"),
    ("executor_terminal_reductions", "lease_epoch"),
    ("executor_terminal_reductions", "lease_expires_at_ms"),
    ("executor_terminal_reductions", "completion_owner"),
    ("executor_terminal_reductions", "provider_receipt_id"),
    ("executor_terminal_reductions", "customer_artifact_id"),
    ("executor_terminal_reductions", "quota_reservation_id"),
    ("provider_remote_tasks", "remote_operation_id"),
    ("provider_remote_tasks", "state"),
    ("provider_remote_tasks", "poll_lease_epoch"),
    ("provider_remote_tasks", "state_observation_id"),
    ("provider_remote_tasks", "attach_recovery_owner"),
    ("provider_remote_tasks", "attach_recovery_lease_epoch"),
    ("provider_task_observations", "event_identity"),
    ("provider_task_observations", "payload_hash"),
    ("provider_task_observations", "result_manifest_id"),
    ("provider_task_observations", "artifact_sha256_hex"),
    ("provider_task_observations", "artifact_byte_size"),
    ("provider_task_observations", "artifact_media_type"),
    ("provider_remote_submit_intents", "idempotency_key"),
    ("provider_remote_submit_intents", "state"),
    ("provider_remote_submit_intents", "provider_request_id"),
    ("provider_remote_submit_intents", "send_started_at_ms"),
    ("provider_remote_submit_intents", "receipt_event_identity"),
    ("provider_remote_submit_intents", "failure_event_identity"),
    ("provider_remote_submit_intents", "failure_error_code"),
    ("provider_submit_recoveries", "submission_id"),
    ("provider_submit_recoveries", "invocation_attempt"),
    ("provider_submit_recoveries", "provider_timeout_ms"),
    ("provider_submit_recoveries", "provider_deadline_at_ms"),
    ("provider_submit_recoveries", "next_recovery_at_ms"),
    ("provider_submit_recoveries", "recovery_owner"),
    ("provider_submit_recoveries", "recovery_lease_epoch"),
    ("provider_remote_tasks", "provider_deadline_at_ms"),
    ("provider_remote_tasks", "deadline_quarantine_id"),
    ("provider_remote_task_quarantines", "quarantine_id"),
    ("provider_remote_task_quarantines", "submission_id"),
    ("provider_remote_task_quarantines", "executor_execution_id"),
    ("provider_remote_task_quarantines", "provider_id"),
    ("provider_remote_task_quarantines", "provider_account_id"),
    ("provider_remote_task_quarantines", "remote_operation_id"),
    (
        "provider_remote_task_quarantines",
        "provider_deadline_at_ms",
    ),
    ("provider_remote_task_quarantines", "error_code"),
    ("provider_remote_task_quarantines", "quarantined_at_ms"),
    (
        "executor_resolution_decisions",
        "provider_remote_task_quarantine_id",
    ),
    ("provider_submit_recovery_commands", "provider_id"),
    ("provider_submit_recovery_commands", "provider_account_id"),
    ("provider_submit_recovery_commands", "command_owner"),
    ("provider_submit_recovery_commands", "command_id"),
    ("provider_submit_recovery_commands", "command_kind"),
    ("provider_submit_recovery_commands", "request_duration_ms"),
    ("provider_submit_recovery_commands", "submission_id"),
    ("provider_submit_recovery_commands", "executor_execution_id"),
    ("provider_submit_recovery_commands", "recovery_lease_epoch"),
    ("provider_submit_recovery_commands", "claim_claimed_at_ms"),
    (
        "provider_submit_recovery_commands",
        "claim_lease_expires_at_ms",
    ),
    ("provider_submit_recovery_commands", "intent_state"),
    (
        "provider_submit_recovery_commands",
        "intent_remote_operation_id",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_provider_request_id",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_send_started_at_ms",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_receipt_event_identity",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_failure_event_identity",
    ),
    (
        "provider_submit_recovery_commands",
        "intent_failure_error_code",
    ),
    ("provider_submit_recovery_commands", "intent_updated_at_ms"),
    ("provider_submit_recovery_commands", "created_at_ms"),
    ("provider_capacity_reconciliations", "reconciliation_id"),
    ("provider_capacity_reconciliations", "submission_id"),
    ("provider_capacity_reconciliations", "executor_execution_id"),
    ("provider_capacity_reconciliations", "provider_id"),
    ("provider_capacity_reconciliations", "provider_account_id"),
    (
        "provider_capacity_reconciliations",
        "provider_deadline_at_ms",
    ),
    ("provider_capacity_reconciliations", "state"),
    ("provider_capacity_reconciliations", "available_at_ms"),
    ("provider_capacity_reconciliations", "reconciliation_owner"),
    (
        "provider_capacity_reconciliations",
        "reconciliation_lease_epoch",
    ),
    ("provider_capacity_reconciliations", "evidence_revision"),
    (
        "provider_capacity_reconciliations",
        "claimed_evidence_revision",
    ),
    ("provider_capacity_reconciliations", "last_command_kind"),
    ("provider_capacity_reconciliations", "last_command_id"),
    ("provider_capacity_reconciliations", "last_command_owner"),
    (
        "provider_capacity_reconciliations",
        "last_command_lease_epoch",
    ),
    (
        "provider_capacity_reconciliations",
        "claim_command_claimed_at_ms",
    ),
    (
        "provider_capacity_reconciliations",
        "claim_command_lease_expires_at_ms",
    ),
    ("provider_capacity_reconciliations", "evidence_kind"),
    ("provider_capacity_reconciliations", "remote_operation_id"),
    ("provider_capacity_reconciliations", "remote_terminal_state"),
    ("provider_capacity_reconciliations", "event_identity"),
    ("provider_capacity_reconciliations", "payload_hash"),
    ("provider_capacity_reconciliations", "created_at_ms"),
    ("provider_capacity_reconciliations", "updated_at_ms"),
    ("provider_capacity_reconciliations", "released_at_ms"),
    (
        "executor_resolution_decisions",
        "provider_task_observation_id",
    ),
    ("executor_resolution_decisions", "provider_submit_intent_id"),
    ("provider_execution_profiles", "operation_id"),
    (
        "provider_execution_profiles",
        "operation_descriptor_revision",
    ),
    (
        "provider_execution_profiles",
        "operation_descriptor_sha256_v1",
    ),
    ("provider_execution_profiles", "completion_mode"),
    ("provider_execution_profiles", "idempotency_mode"),
    ("provider_submissions", "operation_id"),
    ("provider_submissions", "operation_descriptor_revision"),
    ("provider_submissions", "operation_descriptor_sha256_v1"),
    ("provider_submissions", "completion_mode"),
    ("provider_submissions", "idempotency_mode"),
    ("provider_submissions", "operation_binding_version"),
    ("provider_remote_submit_intents", "provider_command_sha256"),
    ("provider_remote_submit_intents", "execution_binding_sha256"),
    ("provider_remote_submit_intents", "provider_timeout_ms"),
];

const REQUIRED_INDEXES: [&str; 26] = [
    "usage_events_tenant_created_at_ms_idx",
    "gateway_api_keys_project_id_idx",
    "quota_reservations_active_tenant_idx",
    "jobs_tenant_state_created_idx",
    "metering_events_tenant_created_idx",
    "artifacts_job_output_uidx",
    "artifacts_execution_output_uidx",
    "job_input_objects_session_idx",
    "admission_input_cleanup_pending_idx",
    "admission_input_cleanup_lease_idx",
    "executor_executions_pending_evidence_idx",
    "executor_capacity_allocations_held_execution_idx",
    "executor_capacity_allocations_orphan_idx",
    "executor_resource_policies_enabled_account_uidx",
    "provider_remote_tasks_poll_claim_idx",
    "provider_submit_recovery_commands_pkey",
    "provider_submit_recovery_commands_transition_uidx",
    "provider_remote_task_quarantines_pkey",
    "provider_remote_tasks_deadline_claim_idx",
    "provider_task_observations_manifest_uidx",
    "provider_submit_intents_remote_operation_uidx",
    "provider_submit_recoveries_claim_idx",
    "provider_submit_recoveries_deadline_idx",
    "provider_capacity_reconciliations_claim_idx",
    "provider_capacity_reconciliations_remote_operation_idx",
    "provider_capacity_reconciliations_claim_command_idx",
];

#[tokio::test]
async fn legacy_schema_without_sqlx_metadata_migrates_from_zero() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = legacy_schema_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn concurrent_fresh_migrations_are_repeatable() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = concurrent_migration_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn verification_fails_closed_for_invalid_migration_metadata() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };

    let result = verification_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn both_stores_share_one_connection_pool() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = shared_pool_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn default_pool_pins_public_despite_url_search_path_options() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = default_pool_case(&test_schema).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn execution_context_migration_requires_legacy_active_jobs_to_be_drained() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = execution_context_upgrade_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn artifact_authority_migration_rejects_untrusted_existing_manifests() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = artifact_authority_upgrade_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn observation_migration_rejects_existing_projection_splits() -> TestResult {
    let Some(test_schema) = TestSchema::new(1).await? else {
        return Ok(());
    };

    let result = observation_resolution_upgrade_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn execution_profile_migration_waits_for_old_writers_before_drain_check() -> TestResult {
    let Some(test_schema) = TestSchema::new(3).await? else {
        return Ok(());
    };

    let result = execution_profile_upgrade_race_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

#[tokio::test]
async fn economic_ledger_is_balanced_at_commit_and_append_only() -> TestResult {
    let Some(test_schema) = TestSchema::new(2).await? else {
        return Ok(());
    };
    let result = economic_ledger_case(&test_schema.pool).await;
    let cleanup = test_schema.cleanup().await;
    cleanup?;
    result
}

async fn economic_ledger_case(pool: &PgPool) -> TestResult {
    gateway_result(
        run_migrations(pool).await,
        "economic migration should succeed",
    )?;
    let debit_id = Uuid::new_v4();
    let credit_id = Uuid::new_v4();
    for (account_id, key, account_type) in [
        (debit_id, "test:receivable", "receivable"),
        (credit_id, "test:revenue", "revenue"),
    ] {
        sqlx::query(
            r#"
            INSERT INTO ledger_accounts
              (account_id, account_key, owner_type, owner_id, account_type, currency, created_at_ms)
            VALUES ($1, $2, 'platform', 'test', $3, 'USD', 1)
            "#,
        )
        .bind(account_id)
        .bind(key)
        .bind(account_type)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to seed ledger account: {error}"))?;
    }

    let empty_transaction_id = Uuid::new_v4();
    let mut empty = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin empty ledger transaction: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions
          (transaction_id, semantic_key, transaction_type, currency, payload_hash, created_at_ms)
        VALUES ($1, $2, 'adjustment', 'USD', $3, 1)
        "#,
    )
    .bind(empty_transaction_id)
    .bind(format!("empty:{empty_transaction_id}"))
    .bind("1".repeat(64))
    .execute(&mut *empty)
    .await
    .map_err(|error| format!("failed to stage empty ledger transaction: {error}"))?;
    require(
        empty.commit().await.is_err(),
        "empty ledger transaction committed",
    )?;

    let transaction_id = Uuid::new_v4();
    let mut balanced = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin balanced ledger transaction: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO ledger_transactions
          (transaction_id, semantic_key, transaction_type, currency, payload_hash, created_at_ms)
        VALUES ($1, $2, 'adjustment', 'USD', $3, 1)
        "#,
    )
    .bind(transaction_id)
    .bind(format!("balanced:{transaction_id}"))
    .bind("2".repeat(64))
    .execute(&mut *balanced)
    .await
    .map_err(|error| format!("failed to stage balanced ledger transaction: {error}"))?;
    for (posting_no, account_id, amount) in [(1_i16, debit_id, 9_i64), (2, credit_id, -9)] {
        sqlx::query(
            r#"
            INSERT INTO ledger_postings
              (transaction_id, posting_no, account_id, currency, amount_micros, created_at_ms)
            VALUES ($1, $2, $3, 'USD', $4, 1)
            "#,
        )
        .bind(transaction_id)
        .bind(posting_no)
        .bind(account_id)
        .bind(amount)
        .execute(&mut *balanced)
        .await
        .map_err(|error| format!("failed to stage balanced posting: {error}"))?;
    }
    sqlx::query(
        "INSERT INTO ledger_transaction_seals (transaction_id, sealed_at_ms) VALUES ($1, 1)",
    )
    .bind(transaction_id)
    .execute(&mut *balanced)
    .await
    .map_err(|error| format!("failed to seal balanced ledger transaction: {error}"))?;
    balanced
        .commit()
        .await
        .map_err(|error| format!("balanced ledger transaction was rejected: {error}"))?;
    require(
        sqlx::query("UPDATE ledger_postings SET amount_micros = 10 WHERE transaction_id = $1")
            .bind(transaction_id)
            .execute(pool)
            .await
            .is_err(),
        "append-only ledger posting was mutated",
    )?;
    require(
        sqlx::query(
            r#"
            INSERT INTO ledger_postings
              (transaction_id, posting_no, account_id, currency, amount_micros, created_at_ms)
            VALUES ($1, 3, $2, 'USD', 1, 2)
            "#,
        )
        .bind(transaction_id)
        .bind(debit_id)
        .execute(pool)
        .await
        .is_err(),
        "sealed ledger transaction accepted another posting",
    )
}

async fn execution_context_upgrade_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0006 migration failed: {error}"))?;
    }
    let job_id = Uuid::new_v4();
    let reservation_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO jobs
          (job_id, tenant_id, request_id, operation, provider_id, model, state,
           requested_units, reservation_id, created_at_ms, updated_at_ms)
        VALUES ($1, 'tenant_upgrade', 'request_upgrade', 'generation',
                'openai-codex', 'gpt-image-2', 'reserved', 1, $2, 1, 1)
        "#,
    )
    .bind(job_id)
    .bind(reservation_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert legacy job: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO quota_reservations
          (reservation_id, tenant_id, request_id, job_id, requested_units,
           state, created_at_ms, updated_at_ms, expires_at_ms)
        VALUES ($1, 'tenant_upgrade', 'request_upgrade', $2, 1,
                'reserved', 1, 1, 9999999999999)
        "#,
    )
    .bind(reservation_id)
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to insert legacy reservation: {error}"))?;

    require(
        sqlx::raw_sql(include_str!("../migrations/0006_execution_context.sql"))
            .execute(pool)
            .await
            .is_err(),
        "0006 must reject an active legacy reservation without a quota snapshot",
    )?;
    let snapshot_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'quota_reservations' AND column_name = 'limit_5h')",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect rolled-back migration: {error}"))?;
    require(
        !snapshot_column_exists,
        "failed 0006 migration must roll back its schema changes",
    )?;

    sqlx::query("UPDATE jobs SET state = 'failed' WHERE job_id = $1")
        .bind(job_id)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to terminalize legacy job: {error}"))?;
    sqlx::query(
        "UPDATE quota_reservations SET state = 'released', released_units = requested_units WHERE reservation_id = $1",
    )
    .bind(reservation_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to release legacy reservation: {error}"))?;
    sqlx::raw_sql(include_str!("../migrations/0006_execution_context.sql"))
        .execute(pool)
        .await
        .map_err(|error| format!("0006 should accept a drained legacy queue: {error}"))?;
    let snapshots: (Option<i32>, Option<i32>, Option<i32>, Option<i32>) = sqlx::query_as(
        "SELECT limit_5h, remaining_5h, limit_7d, remaining_7d FROM quota_reservations WHERE reservation_id = $1",
    )
    .bind(reservation_id)
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to read migrated legacy reservation: {error}"))?;
    require(
        snapshots == (None, None, None, None),
        "terminal legacy snapshots must remain consistently NULL",
    )
}

async fn artifact_authority_upgrade_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
        include_str!("../migrations/0006_execution_context.sql"),
        include_str!("../migrations/0007_edit_inputs.sql"),
        include_str!("../migrations/0008_provider_submissions.sql"),
        include_str!("../migrations/0009_economic_kernel.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0010 migration failed: {error}"))?;
    }
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'executor_result_manifests'::regclass
                  AND contype = 'f'
            LOOP
                EXECUTE format(
                    'ALTER TABLE executor_result_manifests DROP CONSTRAINT %I',
                    constraint_name
                );
            END LOOP;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to isolate legacy manifest fixture: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO executor_result_manifests
          (manifest_id, executor_execution_id, submission_id, storage_backend,
           object_key, sha256_hex, byte_size, media_type, created_at_ms)
        VALUES ($1, $2, $3, 'legacy', 'legacy/object', $4, 1, 'image/png', 1)
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed untrusted legacy manifest: {error}"))?;

    require(
        sqlx::raw_sql(include_str!(
            "../migrations/0010_executor_artifact_authority.sql"
        ))
        .execute(pool)
        .await
        .is_err(),
        "0010 accepted caller-supplied legacy artifact metadata",
    )?;
    let authority_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('executor_artifact_authorities') IS NOT NULL")
            .fetch_one(pool)
            .await
            .map_err(|error| {
                format!("failed to inspect rolled-back authority migration: {error}")
            })?;
    require(
        !authority_table_exists,
        "failed 0010 migration did not roll back its schema changes",
    )
}

async fn observation_resolution_upgrade_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
        include_str!("../migrations/0006_execution_context.sql"),
        include_str!("../migrations/0007_edit_inputs.sql"),
        include_str!("../migrations/0008_provider_submissions.sql"),
        include_str!("../migrations/0009_economic_kernel.sql"),
        include_str!("../migrations/0010_executor_artifact_authority.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0011 migration failed: {error}"))?;
    }
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'provider_submissions'::regclass
                  AND contype = 'f'
            LOOP
                EXECUTE format(
                    'ALTER TABLE provider_submissions DROP CONSTRAINT %I',
                    constraint_name
                );
            END LOOP;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to isolate projection split fixture: {error}"))?;
    let submission_id = Uuid::new_v4();
    let executor_execution_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO provider_submissions
          (submission_id, executor_execution_id, output_id, job_id,
           tenant_id, provider_id, model, work_item_id,
           created_by_execution_id, created_by_lease_epoch,
           command_schema, command_hash, state,
           prepared_at_ms, started_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'tenant', 'provider', 'model', $5,
                $6, 1, 'command-v1', $7, 'running', 1, 1, 1)
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed split submission: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO executor_executions
          (executor_execution_id, submission_id, state, executor_owner,
           lease_epoch, lease_expires_at_ms, created_at_ms, leased_at_ms,
           updated_at_ms)
        VALUES ($1, $2, 'leased', 'executor', 1, 9999999999999, 1, 1, 1)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .execute(pool)
    .await
    .map_err(|error| format!("failed to seed split execution: {error}"))?;

    require(
        sqlx::raw_sql(include_str!(
            "../migrations/0011_executor_observation_resolution.sql"
        ))
        .execute(pool)
        .await
        .is_err(),
        "0011 accepted an existing executor/submission projection split",
    )?;
    let launch_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'executor_executions' AND column_name = 'launch_owner')",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to inspect rolled-back observation migration: {error}"))?;
    require(
        !launch_column_exists,
        "failed 0011 migration did not roll back its schema changes",
    )
}

async fn execution_profile_upgrade_race_case(pool: &PgPool) -> TestResult {
    for migration in [
        include_str!("../migrations/0000_legacy_reconciliation.sql"),
        include_str!("../migrations/0001_usage.sql"),
        include_str!("../migrations/0002_durable_admission.sql"),
        include_str!("../migrations/0003_durable_scheduling.sql"),
        include_str!("../migrations/0004_api_key_hmac.sql"),
        include_str!("../migrations/0005_artifact_replay.sql"),
        include_str!("../migrations/0006_execution_context.sql"),
        include_str!("../migrations/0007_edit_inputs.sql"),
        include_str!("../migrations/0008_provider_submissions.sql"),
        include_str!("../migrations/0009_economic_kernel.sql"),
        include_str!("../migrations/0010_executor_artifact_authority.sql"),
        include_str!("../migrations/0011_executor_observation_resolution.sql"),
        include_str!("../migrations/0012_executor_pending_evidence_index.sql"),
    ] {
        sqlx::raw_sql(migration)
            .execute(pool)
            .await
            .map_err(|error| format!("pre-0013 migration failed: {error}"))?;
    }
    sqlx::raw_sql(
        r#"
        DO $$
        DECLARE constraint_name TEXT;
        BEGIN
            FOR constraint_name IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'provider_submissions'::regclass
                  AND contype = 'f'
            LOOP
                EXECUTE format(
                    'ALTER TABLE provider_submissions DROP CONSTRAINT %I',
                    constraint_name
                );
            END LOOP;
        END;
        $$;
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to isolate old executor writer: {error}"))?;

    let submission_id = Uuid::new_v4();
    let executor_execution_id = Uuid::new_v4();
    let mut old_writer = pool
        .begin()
        .await
        .map_err(|error| format!("failed to begin old writer: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO provider_submissions
          (submission_id, executor_execution_id, output_id, job_id,
           tenant_id, provider_id, model, work_item_id,
           created_by_execution_id, created_by_lease_epoch,
           command_schema, command_hash, state, prepared_at_ms, updated_at_ms)
        VALUES ($1, $2, $3, $4, 'tenant', 'provider', 'model', $5,
                $6, 1, 'command-v1', $7, 'prepared', 1, 1)
        "#,
    )
    .bind(submission_id)
    .bind(executor_execution_id)
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind("a".repeat(64))
    .execute(&mut *old_writer)
    .await
    .map_err(|error| format!("failed to stage old submission: {error}"))?;
    sqlx::query(
        r#"
        INSERT INTO executor_executions
          (executor_execution_id, submission_id, state, created_at_ms, updated_at_ms)
        VALUES ($1, $2, 'prepared', 1, 1)
        "#,
    )
    .bind(executor_execution_id)
    .bind(submission_id)
    .execute(&mut *old_writer)
    .await
    .map_err(|error| format!("failed to stage old execution: {error}"))?;

    let migration_pool = pool.clone();
    let mut migration = tokio::spawn(async move {
        sqlx::raw_sql(include_str!(
            "../migrations/0013_executor_execution_profiles.sql"
        ))
        .execute(&migration_pool)
        .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    require(
        !migration.is_finished(),
        "0013 did not serialize with the in-flight old executor writer",
    )?;
    old_writer
        .commit()
        .await
        .map_err(|error| format!("failed to commit old writer: {error}"))?;
    let migration_result = timeout(Duration::from_secs(5), &mut migration)
        .await
        .map_err(|_| "0013 did not finish after old writer committed".to_string())?
        .map_err(|error| format!("0013 task failed: {error}"))?;
    require(
        migration_result.is_err(),
        "0013 missed the active row committed by an old executor writer",
    )?;
    let profile_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('provider_execution_profiles') IS NOT NULL")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to inspect rolled-back 0013: {error}"))?;
    require(
        !profile_table_exists,
        "failed 0013 migration did not roll back its schema changes",
    )
}

async fn default_pool_case(test_schema: &TestSchema) -> TestResult {
    let database_url = env::var("TEST_DATABASE_URL")
        .map_err(|_| "TEST_DATABASE_URL disappeared during test".to_string())?;
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let injected_url = format!(
        "{database_url}{separator}options=-csearch_path%3D{}",
        test_schema.name
    );
    let pool = connect_pool(&injected_url, 1)
        .await
        .map_err(|error| format!("default pool should connect: {error:?}"))?;
    let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&pool)
        .await
        .map_err(|error| format!("failed to read default pool schema: {error}"))?;
    pool.close().await;
    require(
        current_schema == "public",
        &format!("default pool resolved to {current_schema:?}, expected public"),
    )
}

async fn legacy_schema_case(pool: &PgPool) -> TestResult {
    sqlx::raw_sql(
        r#"
        CREATE TABLE usage_events (
            event_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            operation TEXT NOT NULL,
            units INTEGER NOT NULL CHECK (units > 0),
            outcome TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
        );

        CREATE TABLE quota_reservations (
            reservation_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            requested_units INTEGER NOT NULL CHECK (requested_units > 0),
            started_units INTEGER NOT NULL DEFAULT 0,
            released_units INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            expires_at_ms BIGINT NOT NULL
        );

        CREATE TABLE jobs (
            job_id UUID PRIMARY KEY,
            request_id TEXT NOT NULL,
            state TEXT NOT NULL,
            requested_units INTEGER NOT NULL,
            charged_units INTEGER NOT NULL DEFAULT 0,
            queue_entered_at_ms BIGINT,
            started_at_ms BIGINT,
            finished_at_ms BIGINT
        );
        "#,
    )
    .execute(pool)
    .await
    .map_err(|error| format!("failed to create legacy schema: {error}"))?;

    require(
        !migration_table_exists(pool).await?,
        "legacy schema must start without _sqlx_migrations",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "legacy schema migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "legacy schema verification should succeed",
    )?;
    assert_expected_schema(pool).await
}

async fn concurrent_migration_case(pool: &PgPool) -> TestResult {
    let (first, second) = tokio::join!(run_migrations(pool), run_migrations(pool));
    gateway_result(first, "first concurrent migration should succeed")?;
    gateway_result(second, "second concurrent migration should succeed")?;
    gateway_result(
        run_migrations(pool).await,
        "repeated migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "fresh schema verification should succeed",
    )?;
    assert_expected_schema(pool).await
}

async fn verification_case(pool: &PgPool) -> TestResult {
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a missing migration table",
    )?;
    require(
        !migration_table_exists(pool).await?,
        "verification must not create the migration table",
    )?;

    gateway_result(
        run_migrations(pool).await,
        "initial migration should succeed",
    )?;
    gateway_result(
        verify_migrations(pool).await,
        "current migrations should verify",
    )?;

    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create pending state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a pending migration",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "pending migration should be restorable",
    )?;

    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 0")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create missing state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a missing migration",
    )?;
    gateway_result(
        run_migrations(pool).await,
        "missing migration should be restorable",
    )?;

    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create dirty state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject an unsuccessful migration",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET success = true WHERE version = 1")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to restore dirty state: {error}"))?;

    let checksum: Vec<u8> =
        sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 1")
            .fetch_one(pool)
            .await
            .map_err(|error| format!("failed to read migration checksum: {error}"))?;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(vec![0_u8])
        .execute(pool)
        .await
        .map_err(|error| format!("failed to create checksum mismatch: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a checksum mismatch",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(checksum)
        .execute(pool)
        .await
        .map_err(|error| format!("failed to restore migration checksum: {error}"))?;

    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (999, 'extra', true, $1, 0)",
    )
    .bind(vec![0_u8])
    .execute(pool)
    .await
    .map_err(|error| format!("failed to create extra migration state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject a database newer than the running binary",
    )?;
    sqlx::query("UPDATE _sqlx_migrations SET success = false WHERE version = 999")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to alter newer migration state: {error}"))?;
    require(
        verify_migrations(pool).await.is_err(),
        "verification must reject unsuccessful future migration metadata",
    )?;
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 999")
        .execute(pool)
        .await
        .map_err(|error| format!("failed to remove extra migration: {error}"))?;

    gateway_result(
        verify_migrations(pool).await,
        "restored migration metadata should verify",
    )
}

async fn shared_pool_case(pool: &PgPool) -> TestResult {
    gateway_result(
        run_migrations(pool).await,
        "store schema migration should succeed",
    )?;
    let usage_store = PostgresUsageStore::new(pool.clone());
    let api_key_store = PostgresApiKeyStore::new(
        pool.clone(),
        ApiKeyKeyring::new(1, [(1, vec![0x22; 32])]).expect("test keyring must be valid"),
    );
    let held_connection = pool
        .acquire()
        .await
        .map_err(|error| format!("failed to acquire sole test connection: {error}"))?;

    require(
        timeout(Duration::from_millis(100), pool.acquire())
            .await
            .is_err(),
        "max_connections(1) must prevent a second pool connection",
    )?;
    require(
        timeout(
            Duration::from_millis(100),
            usage_store.reserve(test_charge("usage-blocked")),
        )
        .await
        .is_err(),
        "usage store must use the shared pool",
    )?;
    require(
        timeout(
            Duration::from_millis(100),
            api_key_store.create_service_account("proj_blocked", "Blocked"),
        )
        .await
        .is_err(),
        "API key store must use the shared pool",
    )?;

    drop(held_connection);
    let (usage_result, api_key_result) = tokio::join!(
        usage_store.reserve(test_charge("usage-ready")),
        api_key_store.create_service_account("proj_ready", "Ready"),
    );
    usage_result.map_err(|error| format!("usage store should be usable: {error:?}"))?;
    api_key_result.map_err(|error| format!("API key store should be usable: {error:?}"))?;
    Ok(())
}

async fn assert_expected_schema(pool: &PgPool) -> TestResult {
    require(
        migration_versions(pool).await?
            == vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26,
            ],
        "applied migration versions must be exactly [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]",
    )?;

    for (table, column) in REQUIRED_COLUMNS {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2)",
        )
        .bind(table)
        .bind(column)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to query {table}.{column}: {error}"))?;
        require(exists, &format!("{table}.{column} must exist"))?;
    }

    for index in REQUIRED_INDEXES {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1)",
        )
        .bind(index)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to query index {index}: {error}"))?;
        require(exists, &format!("index {index} must exist"))?;
    }

    let recovery_deadline_constraint: Option<String> = sqlx::query_scalar(
        r#"
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'provider_submit_recoveries'::regclass
          AND conname = 'provider_submit_recoveries_lease_deadline_check'
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| format!("failed to query recovery deadline constraint: {error}"))?;
    require(
        recovery_deadline_constraint.is_some_and(|definition| {
            definition.contains("recovery_lease_expires_at_ms <= provider_deadline_at_ms")
        }),
        "provider recovery leases must be bounded by the absolute provider deadline",
    )?;
    let provider_heartbeat_triggers: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM pg_trigger
        WHERE NOT tgisinternal
          AND tgrelid IN (
            'executor_capacity_allocations'::regclass,
            'provider_remote_tasks'::regclass
          )
          AND tgname IN (
              'executor_capacity_allocations_heartbeat_time_guard',
              'executor_capacity_submit_deadline_hold_guard',
              'provider_remote_task_recovery_deadline_guard'
          )
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| format!("failed to query provider heartbeat triggers: {error}"))?;
    require(
        provider_heartbeat_triggers == 3,
        "provider heartbeat, capacity quarantine, and attach deadline guards must exist",
    )?;
    for (index, expression) in [
        (
            "provider_submit_recoveries_claim_idx",
            "greatest(next_recovery_at_ms, coalesce(recovery_lease_expires_at_ms, next_recovery_at_ms))",
        ),
        (
            "provider_remote_tasks_poll_claim_idx",
            "greatest(next_poll_at_ms, coalesce(poll_lease_expires_at_ms, next_poll_at_ms))",
        ),
    ] {
        let definition: String = sqlx::query_scalar(
            "SELECT lower(indexdef) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1",
        )
        .bind(index)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to inspect index {index}: {error}"))?;
        require(
            definition.contains(expression),
            &format!("index {index} must preserve the effective due expression"),
        )?;
    }
    Ok(())
}

async fn migration_table_exists(pool: &PgPool) -> TestResult<bool> {
    sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(pool)
        .await
        .map_err(|error| format!("failed to inspect migration table: {error}"))
}

async fn migration_versions(pool: &PgPool) -> TestResult<Vec<i64>> {
    sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .map_err(|error| format!("failed to query migration versions: {error}"))
}

fn gateway_result(result: Result<(), ImageGatewayError>, context: &str) -> TestResult {
    result.map_err(|error| format!("{context}: {error:?}"))
}

fn require(condition: bool, message: &str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.to_string())
    }
}

fn test_charge(request_id: &str) -> UsageCharge {
    UsageCharge {
        tenant_id: "proj_test".to_string(),
        request_id: request_id.to_string(),
        admission_session_id: None,
        operation: "generation",
        provider_id: "openai-codex".to_string(),
        model: "gpt-image-2".to_string(),
        units: 1,
        limits: UsageLimits {
            five_hour_image_limit: 10,
            seven_day_image_limit: 10,
        },
    }
}

struct TestSchema {
    name: String,
    pool: PgPool,
}

impl TestSchema {
    async fn new(max_connections: u32) -> TestResult<Option<Self>> {
        let Some(database_url) = env::var("TEST_DATABASE_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err("TEST_DATABASE_URL must be set when CI is present".to_string());
            }
            eprintln!("skipping PostgreSQL migration test: TEST_DATABASE_URL is not set");
            return Ok(None);
        };
        let name = format!("image_gateway_test_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, max_connections, &name)
            .await
            .map_err(|error| format!("test database should be reachable: {error:?}"))?;
        let database_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .map_err(|error| format!("failed to identify test database: {error}"))?;
        if !database_name.to_ascii_lowercase().contains("test") {
            pool.close().await;
            return Err(format!(
                "refusing schema DDL because current_database() is {database_name:?}, which does not contain 'test'"
            ));
        }

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{name}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {name}: {error}"))?;
        let setup = async {
            let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(&pool)
                .await
                .map_err(|error| format!("failed to inspect current schema: {error}"))?;
            require(
                current_schema == name,
                &format!(
                    "test connection search_path resolved to {current_schema:?}, expected {name:?}"
                ),
            )
        }
        .await;
        if let Err(error) = setup {
            let cleanup = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{name}\" CASCADE")))
                .execute(&pool)
                .await;
            pool.close().await;
            return match cleanup {
                Ok(_) => Err(error),
                Err(cleanup_error) => Err(format!(
                    "{error}; additionally failed to clean isolated schema {name}: {cleanup_error}"
                )),
            };
        }
        Ok(Some(Self { name, pool }))
    }

    async fn cleanup(self) -> TestResult {
        let result = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA \"{}\" CASCADE",
            self.name
        )))
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to clean isolated schema {}: {error}", self.name));
        self.pool.close().await;
        result.map(|_| ())
    }
}
