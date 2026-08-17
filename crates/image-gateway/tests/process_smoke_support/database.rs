use std::{env, path::Path, time::Duration};

use gpt_image_2_gateway::{
    CodexExecutionProfileProvisioning,
    database::{connect_test_pool_with_search_path, run_migrations},
    provision_codex_execution_profile,
};
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, PgPool};
use tokio::time::timeout;
use uuid::Uuid;

use super::{TestResult, require};

const DATABASE_ENV: &str = "TEST_DATABASE_URL";

pub(crate) struct TestDatabase {
    database_url: String,
    schema: String,
    pool: PgPool,
}

pub(crate) struct ExecutionProfile {
    pub(crate) profile_key: String,
    pub(crate) credential_ref: String,
}

impl TestDatabase {
    pub(crate) async fn new() -> TestResult<Option<Self>> {
        let Some(database_url) = env::var(DATABASE_ENV)
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            if env::var_os("CI").is_some() {
                return Err(format!("{DATABASE_ENV} must be set when CI is present"));
            }
            eprintln!("skipping process smoke test: {DATABASE_ENV} is not set");
            return Ok(None);
        };
        let schema = format!("image_gateway_process_smoke_{}", Uuid::new_v4().simple());
        let pool = connect_test_pool_with_search_path(&database_url, 4, &schema)
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

        sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .map_err(|error| format!("failed to create isolated schema {schema}: {error}"))?;
        let setup = async {
            let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(&pool)
                .await
                .map_err(|error| format!("failed to inspect current schema: {error}"))?;
            require(
                current_schema == schema,
                format!(
                    "database helper resolved search_path to {current_schema:?}, expected {schema:?}"
                ),
            )?;
            run_migrations(&pool)
                .await
                .map_err(|error| format!("failed to migrate isolated schema: {error:?}"))
        }
        .await;
        if let Err(error) = setup {
            let _ = drop_schema(&pool, &schema).await;
            pool.close().await;
            return Err(error);
        }

        Ok(Some(Self {
            database_url,
            schema,
            pool,
        }))
    }

    pub(crate) fn database_url(&self) -> &str {
        &self.database_url
    }

    pub(crate) fn schema(&self) -> &str {
        &self.schema
    }

    pub(crate) async fn configure_v2_pricing(
        &self,
        output_count: usize,
        success_micros: i64,
    ) -> TestResult {
        let output_count =
            i64::try_from(output_count).map_err(|_| "V2 output count exceeds i64".to_string())?;
        let credit_limit_micros = success_micros
            .checked_mul(output_count)
            .ok_or_else(|| "V2 billing credit overflowed".to_string())?;
        require(
            output_count > 0 && success_micros > 0,
            "V2 process smoke requires positive outputs and pricing",
        )?;
        let now: i64 = sqlx::query_scalar(
            "SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read database time for V2 pricing: {error}"))?;
        let price_version_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO price_versions
              (price_version_id, price_key, version, api_profile, operation, provider_id, model,
               currency, success_micros, failed_micros, no_effect_micros, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, $2, 1, 'openai-images-v1', 'generation', 'openai-codex',
                    'gpt-image-2', 'USD', $3, 0, 0, 'active', $4, $4)
            "#,
        )
        .bind(price_version_id)
        .bind(format!("process-smoke-v2-{price_version_id}"))
        .bind(success_micros)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to configure exact V2 price: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO billing_accounts
              (tenant_id, currency, credit_limit_micros, held_micros, captured_micros,
               created_at_ms, updated_at_ms)
            VALUES ('tenant_default', 'USD', $1, 0, 0, $2, $2)
            "#,
        )
        .bind(credit_limit_micros)
        .bind(now)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(|error| format!("failed to configure V2 billing account: {error}"))
    }

    pub(crate) async fn provision_codex_execution_profile(
        &self,
        credential_home: &Path,
        credential_auth_sha256: String,
    ) -> TestResult<ExecutionProfile> {
        let suffix = Uuid::new_v4().simple().to_string();
        let profile_key = format!("process-smoke-profile-{suffix}");
        let credential_ref = format!("process-smoke-credential-{suffix}");
        let provisioning = CodexExecutionProfileProvisioning {
            profile_key: profile_key.clone(),
            credential_pool_key: format!("process-smoke-pool-{suffix}"),
            provider_account_key: format!("process-smoke-account-{suffix}"),
            credential_ref: credential_ref.clone(),
            credential_revision: 1,
            credential_auth_sha256,
            max_concurrency: 2,
        };
        let provisioned = provision_codex_execution_profile(&self.pool, &provisioning)
            .await
            .map_err(|error| format!("production Codex profile provisioning failed: {error}"))?;
        let now: i64 = sqlx::query_scalar(
            "SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read database time for V2 account: {error}"))?;
        sqlx::query(
            r#"
            INSERT INTO provider_account_environments
              (provider_account_id, provider_id, environment_kind, environment_ref,
               upstream_identity_sha256, display_name, account_email, state,
               created_at_ms, updated_at_ms)
            VALUES ($1, 'openai-codex', 'codex_home_v1', $2, $3,
                    'Process smoke Codex account', NULL, 'active', $4, $4)
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind(credential_home.to_string_lossy().as_ref())
        .bind(&provisioning.credential_auth_sha256)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to configure V2 credential environment: {error}"))?;
        let next_refresh_at_ms = now
            .checked_add(3_600_000)
            .ok_or_else(|| "V2 credential refresh deadline overflowed".to_string())?;
        let updated = sqlx::query(
            r#"
            UPDATE provider_account_credential_heads
            SET refresh_after_ms = $2, next_refresh_at_ms = $2, updated_at_ms = $3
            WHERE provider_account_id = $1
            "#,
        )
        .bind(provisioned.provider_account_id)
        .bind(next_refresh_at_ms)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|error| format!("failed to defer V2 credential refresh: {error}"))?;
        require(
            updated.rows_affected() == 1,
            format!(
                "expected one V2 credential head, updated {}",
                updated.rows_affected()
            ),
        )?;
        Ok(ExecutionProfile {
            profile_key,
            credential_ref,
        })
    }

    pub(crate) async fn wait_for_generation_work_count(&self, expected: i64) -> TestResult {
        timeout(Duration::from_secs(5), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM work_items WHERE state IN ('ready', 'leased', 'running')",
                )
                .fetch_one(&self.pool)
                .await
                .map_err(|error| format!("failed to count generation work: {error}"))?;
                if count >= expected {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| format!("durable queue did not reach {expected} active work items"))?
    }

    pub(crate) async fn process_state_diagnostics(&self) -> TestResult<String> {
        sqlx::query_scalar(
            r#"
            SELECT jsonb_pretty(jsonb_build_object(
                'jobs', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'state', state, 'error_code', last_error_code
                    ) ORDER BY created_at_ms), '[]'::jsonb)
                    FROM jobs
                ),
                'work_items', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'state', state, 'lease_epoch', lease_epoch
                    ) ORDER BY created_at_ms), '[]'::jsonb)
                    FROM work_items
                ),
                'job_outputs', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'output_index', output_index, 'state', state, 'error_code', error_code
                    ) ORDER BY output_index), '[]'::jsonb)
                    FROM job_outputs
                ),
                'provider_submissions', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'state', state, 'error_code', error_code
                    ) ORDER BY prepared_at_ms), '[]'::jsonb)
                    FROM provider_submissions
                ),
                'executor_executions', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'state', state, 'error_code', error_code
                    ) ORDER BY created_at_ms), '[]'::jsonb)
                    FROM executor_executions
                ),
                'terminal_reductions', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'resolved_state', resolved_state, 'state', state
                    ) ORDER BY created_at_ms), '[]'::jsonb)
                    FROM executor_terminal_reductions
                ),
                'credential_heads', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                        'lifecycle_state', lifecycle_state,
                        'refresh_strategy', refresh_strategy,
                        'next_refresh_at_ms', next_refresh_at_ms,
                        'lease_owner', lease_owner,
                        'last_error_code', last_error_code
                    ) ORDER BY created_at_ms), '[]'::jsonb)
                    FROM provider_account_credential_heads
                )
            ))::TEXT
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read process smoke state diagnostics: {error}"))
    }

    pub(crate) async fn assert_transitions(&self, request_id: &str) -> TestResult {
        let transition: TransitionRow = sqlx::query_as(
            r#"
            SELECT j.job_id, qr.reservation_id,
                   j.state AS job_state,
                   j.requested_units AS job_requested_units,
                   j.charged_units,
                   j.finished_at_ms IS NOT NULL AS job_finished,
                   qr.state AS reservation_state,
                   qr.requested_units AS reservation_requested_units,
                   qr.committed_units,
                   qr.released_units
            FROM jobs j
            JOIN quota_reservations qr
              ON qr.reservation_id = j.reservation_id
             AND qr.job_id = j.job_id
             AND qr.tenant_id = j.tenant_id
            WHERE j.request_id = $1 AND j.tenant_id = 'tenant_default'
            "#,
        )
        .bind(request_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read job and reservation transition: {error}"))?;
        require(
            transition.job_state == "succeeded"
                && transition.job_requested_units == 1
                && transition.charged_units == 1
                && transition.job_finished
                && transition.reservation_state == "committed"
                && transition.reservation_requested_units == 1
                && transition.committed_units == 1
                && transition.released_units == 0,
            format!("unexpected succeeded/committed transition: {transition:?}"),
        )?;

        let charged: (i64, i64, Option<String>, Option<String>) = sqlx::query_as(
            r#"
            SELECT COUNT(*), COALESCE(SUM(units), 0)::BIGINT, MIN(outcome), MIN(operation)
            FROM usage_events
            WHERE request_id = $1 AND tenant_id = 'tenant_default'
            "#,
        )
        .bind(request_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read charged usage transition: {error}"))?;
        require(
            charged
                == (
                    1,
                    1,
                    Some("charged".to_string()),
                    Some("generation".to_string()),
                ),
            format!("unexpected charged usage transition: {charged:?}"),
        )?;

        let metering: Vec<MeteringRow> = sqlx::query_as(
            r#"
            SELECT me.event_type, me.units, me.outcome, me.job_id, me.reservation_id
            FROM metering_events me
            JOIN jobs j
              ON j.job_id = me.job_id
             AND j.tenant_id = me.tenant_id
             AND j.request_id = me.request_id
            JOIN quota_reservations qr
              ON qr.reservation_id = me.reservation_id
             AND qr.job_id = j.job_id
             AND qr.tenant_id = me.tenant_id
             AND qr.request_id = me.request_id
            WHERE me.request_id = $1
              AND me.tenant_id = 'tenant_default'
              AND j.job_id = $2
              AND qr.reservation_id = $3
            ORDER BY me.event_type
            "#,
        )
        .bind(request_id)
        .bind(transition.job_id)
        .bind(transition.reservation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| format!("failed to read metering transitions: {error}"))?;
        let expected = vec![
            MeteringRow::expected(
                "job_succeeded",
                "succeeded",
                transition.job_id,
                transition.reservation_id,
            ),
            MeteringRow::expected(
                "quota_committed",
                "succeeded",
                transition.job_id,
                transition.reservation_id,
            ),
            MeteringRow::expected(
                "quota_reserved",
                "reserved",
                transition.job_id,
                transition.reservation_id,
            ),
        ];
        require(
            metering == expected,
            format!("unexpected authoritative metering transitions: {metering:?}"),
        )?;

        let durable: DurableTransitionRow = sqlx::query_as(
            r#"
            SELECT a.state AS admission_state,
                   p.command_schema,
                   p.command_json ->> 'model' AS command_model,
                   p.command_json ->> 'source_api_profile' AS source_api_profile,
                   w.state AS work_state,
                   w.lease_epoch,
                   w.execution_id IS NOT NULL AS has_execution_id,
                   ja.state AS attempt_state,
                   ja.worker_id,
                   i.state AS idempotency_state,
                   (SELECT COUNT(*) FROM job_response_projections rp
                    WHERE rp.job_id = a.job_id) AS projection_count,
                   (SELECT COUNT(*) FROM artifacts ar
                    WHERE ar.job_id = a.job_id AND ar.state = 'ready') AS artifact_count
            FROM admission_sessions a
            JOIN idempotency_requests i ON i.session_id = a.session_id
            JOIN job_payloads p ON p.admission_session_id = a.session_id
            JOIN work_items w ON w.job_id = p.job_id
            JOIN job_attempts ja
              ON ja.work_item_id = w.work_item_id
             AND ja.execution_id = w.execution_id
             AND ja.lease_epoch = w.lease_epoch
            WHERE a.job_id = $1
            "#,
        )
        .bind(transition.job_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read durable execution transition: {error}"))?;
        require(
            durable.admission_state == "attached"
                && durable.command_schema == "openai.images.generation.v1"
                && durable.command_model.as_deref() == Some("gpt-image-2")
                && durable.source_api_profile.as_deref() == Some("openai-images-v1")
                && durable.work_state == "succeeded"
                && durable.lease_epoch == 1
                && durable.has_execution_id
                && durable.attempt_state == "succeeded"
                && durable.worker_id == "process-smoke-workerd/lane-0"
                && durable.idempotency_state == "succeeded"
                && durable.projection_count == 1
                && durable.artifact_count == 1,
            format!("unexpected durable execution transition: {durable:?}"),
        )?;

        let event_types: Vec<String> = sqlx::query_scalar(
            "SELECT event_type FROM job_events WHERE job_id = $1 ORDER BY event_type",
        )
        .bind(transition.job_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| format!("failed to read durable job events: {error}"))?;
        let outbox_types: Vec<String> = sqlx::query_scalar(
            "SELECT event_type FROM outbox_events WHERE job_id = $1 ORDER BY event_type",
        )
        .bind(transition.job_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|error| format!("failed to read durable outbox events: {error}"))?;
        let expected_events = vec!["job.accepted".to_string(), "job.succeeded".to_string()];
        require(
            event_types == expected_events && outbox_types == expected_events,
            format!("unexpected durable events: job={event_types:?}, outbox={outbox_types:?}"),
        )
    }

    pub(crate) async fn assert_edit_transitions(&self, request_id: &str) -> TestResult {
        let row: EditTransitionRow = sqlx::query_as(
            r#"
            SELECT j.state AS job_state, j.operation, qr.state AS quota_state,
                   a.state AS admission_state, p.command_schema,
                   w.state AS work_state, ja.state AS attempt_state,
                   COALESCE(i.state, '') AS idempotency_state,
                   rp.operation AS projection_operation,
                   (SELECT COUNT(*) FROM job_input_manifests m WHERE m.job_id = j.job_id) AS manifest_count,
                   (SELECT COUNT(*) FROM job_input_objects o WHERE o.job_id = j.job_id) AS input_count,
                   (SELECT COUNT(*) FROM artifacts ar WHERE ar.job_id = j.job_id AND ar.state = 'ready') AS artifact_count,
                   (SELECT COUNT(*) FROM usage_events ue
                    WHERE ue.request_id = j.request_id AND ue.operation = 'edit' AND ue.outcome = 'charged') AS charge_count
            FROM jobs j
            JOIN quota_reservations qr ON qr.reservation_id = j.reservation_id AND qr.job_id = j.job_id
            JOIN admission_sessions a ON a.job_id = j.job_id
            JOIN job_payloads p ON p.job_id = j.job_id
            JOIN work_items w ON w.job_id = j.job_id
            JOIN job_attempts ja ON ja.work_item_id = w.work_item_id AND ja.execution_id = w.execution_id
            LEFT JOIN idempotency_requests i ON i.job_id = j.job_id
            JOIN job_response_projections rp ON rp.job_id = j.job_id
            WHERE j.request_id = $1
            "#,
        )
        .bind(request_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read edit durable transition: {error}"))?;
        require(
            row.job_state == "succeeded"
                && row.operation == "edit"
                && row.quota_state == "committed"
                && row.admission_state == "attached"
                && row.command_schema == "openai.images.edit.v1"
                && row.work_state == "succeeded"
                && row.attempt_state == "succeeded"
                && row.idempotency_state == "succeeded"
                && row.projection_operation == "edit"
                && row.manifest_count == 1
                && row.input_count == 1
                && row.artifact_count == 1
                && row.charge_count == 1,
            format!("unexpected durable edit transition: {row:?}"),
        )
    }

    pub(crate) async fn assert_v2_generation_graph(
        &self,
        request_id: &str,
        expected_profile: &ExecutionProfile,
        fixtures: &[Vec<u8>],
        expected_outputs: usize,
        success_micros: i64,
    ) -> TestResult {
        require(
            fixtures.len() == expected_outputs,
            "V2 fixture count must match expected outputs",
        )?;
        let expected_outputs = i32::try_from(expected_outputs)
            .map_err(|_| "V2 expected output count exceeds i32".to_string())?;
        let expected_total = success_micros
            .checked_mul(i64::from(expected_outputs))
            .ok_or_else(|| "V2 expected charge overflowed".to_string())?;
        let expected_sha256 = fixtures
            .iter()
            .map(|fixture| hex::encode(Sha256::digest(fixture)))
            .collect::<Vec<_>>();
        let expected_byte_size = fixtures
            .iter()
            .map(|fixture| {
                i64::try_from(fixture.len())
                    .map_err(|_| "V2 fixture byte size exceeds i64".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let graph: V2GenerationGraph = sqlx::query_as(
            r#"
            SELECT job.economics_contract_version, job.state AS job_state,
                   job.requested_units, job.charged_units,
                   quota.state AS quota_state, quota.requested_units AS quota_requested_units,
                   quota.committed_units, quota.released_units,
                   admission.state AS admission_state, payload.command_schema,
                   work.state AS work_state, attempt.state AS attempt_state,
                   attempt.worker_id, idempotency.state AS idempotency_state,
                   projection.operation AS projection_operation,
                   projection.response_schema AS projection_response_schema,
                   projection.size AS projection_size,
                   projection.artifact_count AS projection_artifact_count,
                   quote.output_count AS quote_output_count,
                   quote.success_micros AS quote_success_micros,
                   quote.max_total_micros,
                   account.credit_limit_micros, account.held_micros AS account_held_micros,
                   account.captured_micros AS account_captured_micros,
                   profile.profile_key, profile.credential_ref,
                   profile.credential_revision, profile.adapter_revision,
                   policy.allocated_count AS policy_allocated_count,
                   jsonb_build_object(
                     'jobs', (SELECT COUNT(*) FROM jobs),
                     'admissions', (SELECT COUNT(*) FROM admission_sessions),
                     'payloads', (SELECT COUNT(*) FROM job_payloads),
                     'work_items', (SELECT COUNT(*) FROM work_items),
                     'attempts', (SELECT COUNT(*) FROM job_attempts),
                     'quotas', (SELECT COUNT(*) FROM quota_reservations),
                     'outputs', (SELECT COUNT(*) FROM job_outputs),
                     'quotes', (SELECT COUNT(*) FROM price_quotes),
                     'holds', (SELECT COUNT(*) FROM output_holds),
                     'submissions', (SELECT COUNT(*) FROM provider_submissions),
                     'attachments', (SELECT COUNT(*) FROM provider_submission_attachments),
                     'executions', (SELECT COUNT(*) FROM executor_executions),
                     'manifests', (SELECT COUNT(*) FROM executor_result_manifests),
                     'authorities', (SELECT COUNT(*) FROM executor_artifact_authorities),
                     'observations', (SELECT COUNT(*) FROM executor_runner_observations),
                     'decisions', (SELECT COUNT(*) FROM executor_resolution_decisions),
                     'allocations', (SELECT COUNT(*) FROM executor_capacity_allocations),
                     'reductions', (SELECT COUNT(*) FROM executor_terminal_reductions),
                     'receipts', (SELECT COUNT(*) FROM provider_receipts),
                     'metering', (SELECT COUNT(*) FROM economic_metering_events),
                     'ratings', (SELECT COUNT(*) FROM rated_usage),
                     'rated_amount_micros', (SELECT COALESCE(SUM(amount_micros), 0) FROM rated_usage),
                     'artifacts', (SELECT COUNT(*) FROM artifacts),
                     'projections', (SELECT COUNT(*) FROM job_response_projections),
                     'usage_charges', (SELECT COUNT(*) FROM usage_events WHERE outcome = 'charged'),
                     'usage_units', (SELECT COALESCE(SUM(units), 0) FROM usage_events WHERE outcome = 'charged'),
                     'idempotency', (SELECT COUNT(*) FROM idempotency_requests),
                     'accepted_events', (SELECT COUNT(*) FROM job_events WHERE job_id = job.job_id AND event_type = 'job.accepted'),
                     'success_events', (SELECT COUNT(*) FROM job_events WHERE job_id = job.job_id AND event_type = 'job.succeeded'),
                     'accepted_outbox', (SELECT COUNT(*) FROM outbox_events WHERE job_id = job.job_id AND event_type = 'job.accepted'),
                     'success_outbox', (SELECT COUNT(*) FROM outbox_events WHERE job_id = job.job_id AND event_type = 'job.succeeded'),
                     'customer_transactions', (SELECT COUNT(*) FROM ledger_transactions WHERE transaction_type = 'customer_charge'),
                     'provider_transactions', (SELECT COUNT(*) FROM ledger_transactions WHERE transaction_type = 'provider_cost'),
                     'ledger_postings', (SELECT COUNT(*) FROM ledger_postings),
                     'ledger_seals', (SELECT COUNT(*) FROM ledger_transaction_seals),
                     'ledger_accounts', (SELECT COUNT(*) FROM ledger_accounts)
                   ) AS counts,
                   COALESCE((
                     SELECT BOOL_AND(
                       output.state = 'succeeded'
                       AND output.output_index BETWEEN 0 AND $2 - 1
                       AND hold.state = 'settled'
                       AND hold.held_micros = $3
                       AND hold.captured_micros = $3
                       AND hold.released_micros = 0
                       AND submission.state = 'succeeded'
                       AND submission.execution_profile_id = profile.execution_profile_id
                       AND submission.credential_pool_id = profile.credential_pool_id
                       AND submission.provider_account_id = profile.provider_account_id
                       AND submission.credential_ref = $7
                       AND submission.credential_revision = profile.credential_revision
                       AND submission.adapter_revision = profile.adapter_revision
                       AND submission.resource_policy_id = profile.resource_policy_id
                       AND submission.resource_policy_revision = profile.resource_policy_revision
                       AND attachment.attempt_execution_id = attempt.execution_id
                       AND execution.state = 'succeeded'
                       AND manifest.manifest_id = submission.submission_id
                       AND manifest.executor_execution_id = execution.executor_execution_id
                       AND manifest.artifact_authority_id = execution.executor_execution_id
                       AND authority.authority_id = execution.executor_execution_id
                       AND authority.output_id = output.output_id
                       AND authority.storage_backend = 'filesystem-v1'
                       AND authority.storage_namespace LIKE 'filesystem-v1:%'
                       AND authority.object_key = 'executor-objects/'
                           || SUBSTRING(REPLACE(authority.authority_id::TEXT, '-', ''), 1, 2)
                           || '/' || REPLACE(authority.authority_id::TEXT, '-', '')
                       AND authority.sha256_hex = ($4::TEXT[])[output.output_index + 1]
                       AND authority.byte_size = ($5::BIGINT[])[output.output_index + 1]
                       AND authority.media_type = 'image/png'
                       AND observation.observation_id = execution.executor_execution_id
                       AND observation.observed_state = 'succeeded'
                       AND observation.result_manifest_id = manifest.manifest_id
                       AND decision.decision_id = execution.executor_execution_id
                       AND decision.source = 'active_runner_observation'
                       AND decision.resolved_state = 'succeeded'
                       AND decision.result_manifest_id = manifest.manifest_id
                       AND allocation.allocation_id = execution.executor_execution_id
                       AND allocation.state = 'released'
                       AND reduction.state = 'completed'
                       AND reduction.resolved_state = 'succeeded'
                       AND receipt.outcome = 'succeeded'
                       AND receipt.receipt_schema = 'executor.resolution.v1'
                       AND receipt.provider_cost_micros IS NULL
                       AND receipt.evidence ->> 'executor_execution_id' = execution.executor_execution_id::TEXT
                       AND receipt.evidence ->> 'resolution_decision_id' = decision.decision_id::TEXT
                       AND receipt.evidence ->> 'submission_id' = submission.submission_id::TEXT
                       AND receipt.evidence ->> 'resolved_state' = 'succeeded'
                       AND receipt.evidence #>> '{artifact,authority_id}' = authority.authority_id::TEXT
                       AND receipt.evidence #>> '{artifact,sha256_hex}' =
                           ($4::TEXT[])[output.output_index + 1]
                       AND receipt.evidence #>> '{artifact,byte_size}' = authority.byte_size::TEXT
                       AND receipt.evidence #>> '{artifact,media_type}' = authority.media_type
                       AND meter.fact_kind = 'output_terminal'
                       AND meter.quantity = 1
                       AND meter.outcome = 'succeeded'
                       AND rating.outcome = 'succeeded'
                       AND rating.quantity = 1
                       AND rating.unit_price_micros = $3
                       AND rating.amount_micros = $3
                       AND artifact.artifact_id = output.output_id
                       AND artifact.execution_id = attempt.execution_id
                       AND artifact.output_index = output.output_index
                       AND artifact.state = 'ready'
                       AND artifact.storage_backend = 'filesystem-v1'
                       AND artifact.object_key = 'objects/'
                           || SUBSTRING(REPLACE(output.output_id::TEXT, '-', ''), 1, 2)
                           || '/' || REPLACE(output.output_id::TEXT, '-', '')
                       AND artifact.sha256_hex = ($4::TEXT[])[output.output_index + 1]
                       AND artifact.byte_size = ($5::BIGINT[])[output.output_index + 1]
                       AND artifact.media_type = 'image/png'
                     )
                     FROM job_outputs output
                     JOIN output_holds hold
                       ON hold.output_id = output.output_id AND hold.job_id = output.job_id
                     JOIN provider_submissions submission
                       ON submission.output_id = output.output_id AND submission.job_id = output.job_id
                     JOIN provider_submission_attachments attachment
                       ON attachment.submission_id = submission.submission_id
                     JOIN executor_executions execution
                       ON execution.executor_execution_id = submission.executor_execution_id
                      AND execution.submission_id = submission.submission_id
                     JOIN executor_result_manifests manifest
                       ON manifest.manifest_id = submission.result_manifest_id
                     JOIN executor_artifact_authorities authority
                       ON authority.authority_id = manifest.artifact_authority_id
                     JOIN executor_runner_observations observation
                       ON observation.executor_execution_id = execution.executor_execution_id
                     JOIN executor_resolution_decisions decision
                       ON decision.decision_id = execution.resolution_decision_id
                     JOIN executor_capacity_allocations allocation
                       ON allocation.executor_execution_id = execution.executor_execution_id
                     JOIN executor_terminal_reductions reduction
                       ON reduction.submission_id = submission.submission_id
                     JOIN provider_receipts receipt
                       ON receipt.submission_id = submission.submission_id
                     JOIN economic_metering_events meter
                       ON meter.receipt_id = receipt.receipt_id
                     JOIN rated_usage rating ON rating.meter_event_id = meter.meter_event_id
                     JOIN artifacts artifact ON artifact.artifact_id = output.output_id
                     WHERE output.job_id = job.job_id
                   ), FALSE) AS output_invariants,
                   NOT EXISTS (
                     SELECT 1
                     FROM ledger_transactions ledger
                     LEFT JOIN ledger_postings posting
                       ON posting.transaction_id = ledger.transaction_id
                     LEFT JOIN ledger_accounts ledger_account
                       ON ledger_account.account_id = posting.account_id
                     LEFT JOIN ledger_transaction_seals seal
                       ON seal.transaction_id = ledger.transaction_id
                     GROUP BY ledger.transaction_id
                     HAVING ledger.transaction_type <> 'customer_charge'
                        OR ledger.source_job_id IS DISTINCT FROM job.job_id
                        OR COUNT(DISTINCT ledger.source_output_id) <> 1
                        OR COUNT(DISTINCT posting.posting_no) <> 2
                        OR COALESCE(SUM(posting.amount_micros), 0) <> 0
                        OR MIN(posting.amount_micros) <> -$3
                        OR MAX(posting.amount_micros) <> $3
                        OR NOT COALESCE(BOOL_OR(
                             ledger_account.owner_type = 'tenant'
                             AND ledger_account.owner_id = 'tenant_default'
                             AND ledger_account.account_type = 'receivable'
                             AND posting.amount_micros = $3
                           ), FALSE)
                        OR NOT COALESCE(BOOL_OR(
                             ledger_account.owner_type = 'platform'
                             AND ledger_account.owner_id = 'platform'
                             AND ledger_account.account_type = 'revenue'
                             AND posting.amount_micros = -$3
                           ), FALSE)
                        OR COUNT(DISTINCT seal.transaction_id) <> 1
                   ) AS ledger_balanced
            FROM jobs job
            JOIN quota_reservations quota
              ON quota.reservation_id = job.reservation_id AND quota.job_id = job.job_id
            JOIN admission_sessions admission ON admission.job_id = job.job_id
            JOIN job_payloads payload ON payload.job_id = job.job_id
            JOIN work_items work ON work.job_id = job.job_id
            JOIN job_attempts attempt
              ON attempt.work_item_id = work.work_item_id
             AND attempt.execution_id = work.execution_id
             AND attempt.lease_epoch = work.lease_epoch
            JOIN idempotency_requests idempotency ON idempotency.job_id = job.job_id
            JOIN price_quotes quote
              ON quote.job_id = job.job_id
            JOIN billing_accounts account
              ON account.tenant_id = job.tenant_id AND account.currency = quote.currency
            JOIN provider_execution_profiles profile
              ON profile.execution_profile_id = work.execution_profile_id
            JOIN executor_resource_policies policy
              ON policy.resource_policy_id = profile.resource_policy_id
             AND policy.revision = profile.resource_policy_revision
            JOIN job_response_projections projection ON projection.job_id = job.job_id
            WHERE job.request_id = $1
            "#,
        )
        .bind(request_id)
        .bind(expected_outputs)
        .bind(success_micros)
        .bind(&expected_sha256)
        .bind(&expected_byte_size)
        .bind(&expected_profile.profile_key)
        .bind(&expected_profile.credential_ref)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| format!("failed to read V2 generation graph: {error}"))?;
        let expected_counts = serde_json::json!({
            "jobs": 1,
            "admissions": 1,
            "payloads": 1,
            "work_items": 1,
            "attempts": 1,
            "quotas": 1,
            "outputs": expected_outputs,
            "quotes": 1,
            "holds": expected_outputs,
            "submissions": expected_outputs,
            "attachments": expected_outputs,
            "executions": expected_outputs,
            "manifests": expected_outputs,
            "authorities": expected_outputs,
            "observations": expected_outputs,
            "decisions": expected_outputs,
            "allocations": expected_outputs,
            "reductions": expected_outputs,
            "receipts": expected_outputs,
            "metering": expected_outputs,
            "ratings": expected_outputs,
            "rated_amount_micros": expected_total,
            "artifacts": expected_outputs,
            "projections": 1,
            "usage_charges": expected_outputs,
            "usage_units": expected_outputs,
            "idempotency": 1,
            "accepted_events": 1,
            "success_events": 1,
            "accepted_outbox": 1,
            "success_outbox": 1,
            "customer_transactions": expected_outputs,
            "provider_transactions": 0,
            "ledger_postings": expected_outputs * 2,
            "ledger_seals": expected_outputs,
            "ledger_accounts": 2
        });
        require(
            graph.economics_contract_version == 2
                && graph.job_state == "succeeded"
                && graph.requested_units == expected_outputs
                && graph.charged_units == expected_outputs
                && graph.quota_state == "committed"
                && graph.quota_requested_units == expected_outputs
                && graph.committed_units == expected_outputs
                && graph.released_units == 0
                && graph.admission_state == "attached"
                && graph.command_schema == "openai.images.generation.v1"
                && graph.work_state == "succeeded"
                && graph.attempt_state == "succeeded"
                && graph.worker_id == "process-smoke-v2-workerd/lane-0"
                && graph.idempotency_state == "succeeded"
                && graph.projection_operation == "generation"
                && graph.projection_response_schema == "openai.images.response.v1"
                && graph.projection_size == "auto"
                && graph.projection_artifact_count == expected_outputs
                && graph.quote_output_count == expected_outputs
                && graph.quote_success_micros == success_micros
                && graph.max_total_micros == expected_total
                && graph.credit_limit_micros == expected_total
                && graph.account_held_micros == 0
                && graph.account_captured_micros == expected_total
                && graph.profile_key == expected_profile.profile_key
                && graph.credential_ref == expected_profile.credential_ref
                && graph.credential_revision == 1
                && graph.adapter_revision == "openai-codex-generation-v1"
                && graph.policy_allocated_count == 0
                && graph.counts == expected_counts
                && graph.output_invariants
                && graph.ledger_balanced,
            format!("unexpected V2 generation graph: {graph:#?}"),
        )
    }

    pub(crate) async fn cleanup(self) -> TestResult {
        let result = timeout(
            Duration::from_secs(5),
            drop_schema(&self.pool, &self.schema),
        )
        .await
        .map_err(|_| format!("timed out cleaning isolated schema {}", self.schema))?;
        self.pool.close().await;
        result
    }
}

async fn drop_schema(pool: &PgPool, schema: &str) -> TestResult {
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| format!("failed to clean isolated schema {schema}: {error}"))
}

#[derive(Debug, sqlx::FromRow)]
struct TransitionRow {
    job_id: Uuid,
    reservation_id: Uuid,
    job_state: String,
    job_requested_units: i32,
    charged_units: i32,
    job_finished: bool,
    reservation_state: String,
    reservation_requested_units: i32,
    committed_units: i32,
    released_units: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct EditTransitionRow {
    job_state: String,
    operation: String,
    quota_state: String,
    admission_state: String,
    command_schema: String,
    work_state: String,
    attempt_state: String,
    idempotency_state: String,
    projection_operation: String,
    manifest_count: i64,
    input_count: i64,
    artifact_count: i64,
    charge_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct V2GenerationGraph {
    economics_contract_version: i16,
    job_state: String,
    requested_units: i32,
    charged_units: i32,
    quota_state: String,
    quota_requested_units: i32,
    committed_units: i32,
    released_units: i32,
    admission_state: String,
    command_schema: String,
    work_state: String,
    attempt_state: String,
    worker_id: String,
    idempotency_state: String,
    projection_operation: String,
    projection_response_schema: String,
    projection_size: String,
    projection_artifact_count: i32,
    quote_output_count: i32,
    quote_success_micros: i64,
    max_total_micros: i64,
    credit_limit_micros: i64,
    account_held_micros: i64,
    account_captured_micros: i64,
    profile_key: String,
    credential_ref: String,
    credential_revision: i64,
    adapter_revision: String,
    policy_allocated_count: i32,
    counts: serde_json::Value,
    output_invariants: bool,
    ledger_balanced: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct DurableTransitionRow {
    admission_state: String,
    command_schema: String,
    command_model: Option<String>,
    source_api_profile: Option<String>,
    work_state: String,
    lease_epoch: i64,
    has_execution_id: bool,
    attempt_state: String,
    worker_id: String,
    idempotency_state: String,
    projection_count: i64,
    artifact_count: i64,
}

#[derive(Debug, Eq, PartialEq, sqlx::FromRow)]
struct MeteringRow {
    event_type: String,
    units: i32,
    outcome: String,
    job_id: Option<Uuid>,
    reservation_id: Option<Uuid>,
}

impl MeteringRow {
    fn expected(event_type: &str, outcome: &str, job_id: Uuid, reservation_id: Uuid) -> Self {
        Self {
            event_type: event_type.to_string(),
            units: 1,
            outcome: outcome.to_string(),
            job_id: Some(job_id),
            reservation_id: Some(reservation_id),
        }
    }
}
