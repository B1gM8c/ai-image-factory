//! Reclaim execution-local payloads after canonical terminal reduction completes.
//! Customer artifacts and immutable launch/result markers remain untouched.
use std::path::{Path, PathBuf};

use sqlx::PgPool;
use uuid::Uuid;

use crate::ImageGatewayError;

#[path = "cleanup_queue.rs"]
mod cleanup_queue;

pub struct RunnerOutputRetention {
    after: Uuid,
    cleanup_owner: String,
    cleanup_claims: u8,
}

impl Default for RunnerOutputRetention {
    fn default() -> Self {
        Self {
            after: Uuid::nil(),
            cleanup_owner: format!("runner-cleanup-{}", Uuid::new_v4().simple()),
            cleanup_claims: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct RunnerCleanupOutcome {
    pub candidates: u32,
    pub reclaimed: u32,
    pub blocked: u32,
    pub deleted_bytes: u64,
    pub pending: i64,
    pub blocked_pending: i64,
    pub oldest_pending_age_ms: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct CompletedCandidate {
    executor_execution_id: Uuid,
    submission_id: Uuid,
    execution_profile_id: Uuid,
    resolution_decision_id: Uuid,
    state: String,
    error_code: Option<String>,
    sha256_hex: Option<String>,
    byte_size: Option<i64>,
}

// Completed reductions and their canonical decisions are immutable. The local runner lock
// serializes filesystem deletion; retaining its markers preserves launch-once recovery.
// A completed child may belong to a failed multi-output parent and still be safely reclaimed.
const COMPLETED_CANDIDATES: &str = r#"
SELECT e.executor_execution_id, e.submission_id, p.execution_profile_id,
       d.decision_id AS resolution_decision_id, e.state,
       e.error_code, a.sha256_hex, a.byte_size
FROM executor_executions e
JOIN provider_submissions s ON s.executor_execution_id = e.executor_execution_id
    AND s.submission_id = e.submission_id
    AND s.resolution_decision_id = e.resolution_decision_id AND s.state = e.state
JOIN provider_execution_profiles p ON p.execution_profile_id = s.execution_profile_id
    AND p.provider_id = s.provider_id AND p.command_schema = s.command_schema
    AND p.adapter_revision = s.adapter_revision
JOIN jobs j ON j.job_id = s.job_id AND j.state IN ('succeeded', 'failed')
JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = j.job_id
    AND o.state = e.state
JOIN executor_resolution_decisions d ON d.decision_id = e.resolution_decision_id
    AND d.executor_execution_id = e.executor_execution_id AND d.submission_id = s.submission_id
    AND d.resolved_state = e.state AND d.source = 'active_runner_observation'
JOIN executor_runner_observations v ON v.observation_id = d.observation_id
    AND v.executor_execution_id = e.executor_execution_id AND v.submission_id = s.submission_id
    AND v.observed_state = e.state AND v.resolution_fingerprint = d.resolution_fingerprint
JOIN executor_terminal_reductions r ON r.executor_execution_id = e.executor_execution_id
    AND r.submission_id = s.submission_id AND r.resolution_decision_id = d.decision_id
    AND r.resolved_state = e.state AND r.state = 'completed'
    AND r.lease_owner IS NULL AND r.lease_expires_at_ms IS NULL
    AND r.completed_at_ms <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
    AND r.provider_receipt_id IS NOT NULL AND r.quota_reservation_id IS NOT NULL
LEFT JOIN executor_result_manifests m ON m.manifest_id = s.result_manifest_id
    AND m.manifest_id = d.result_manifest_id AND m.manifest_id = v.result_manifest_id
    AND m.executor_execution_id = e.executor_execution_id AND m.submission_id = s.submission_id
LEFT JOIN executor_artifact_authorities a ON a.authority_id = m.artifact_authority_id
    AND a.executor_execution_id = e.executor_execution_id AND a.submission_id = s.submission_id
    AND a.output_id = o.output_id AND a.job_id = j.job_id
LEFT JOIN artifacts f ON f.artifact_id = r.customer_artifact_id AND f.artifact_id = o.output_id
    AND f.job_id = j.job_id AND f.output_index = o.output_index AND f.state = 'ready'
    AND f.sha256_hex = a.sha256_hex AND f.byte_size = a.byte_size AND f.media_type = a.media_type
WHERE e.state IN ('succeeded', 'failed')
    AND e.executor_owner IS NULL AND e.lease_expires_at_ms IS NULL
    AND e.finished_at_ms IS NOT NULL
    AND r.completed_at_ms >= e.finished_at_ms
    AND e.finished_at_ms <= (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
    AND (
        (e.state = 'succeeded' AND f.artifact_id IS NOT NULL
            AND e.error_code IS NULL AND s.error_code IS NULL AND o.error_code IS NULL
            AND d.error_code IS NULL AND v.error_code IS NULL)
        OR
        (e.state = 'failed' AND e.error_code = s.error_code AND e.error_code = o.error_code
            AND e.error_code = d.error_code AND e.error_code = v.error_code
            AND s.result_manifest_id IS NULL AND d.result_manifest_id IS NULL
            AND v.result_manifest_id IS NULL AND r.customer_artifact_id IS NULL)
    )
    AND e.executor_execution_id = ANY($1)
ORDER BY e.executor_execution_id
"#;

#[derive(sqlx::FromRow)]
struct Candidate {
    executor_execution_id: Uuid,
    submission_id: Uuid,
    sha256_hex: String,
    byte_size: i64,
}

const CANDIDATES: &str = r#"
SELECT e.executor_execution_id, e.submission_id, a.sha256_hex, a.byte_size
FROM executor_executions e
JOIN provider_submissions s ON s.executor_execution_id = e.executor_execution_id
    AND s.submission_id = e.submission_id
JOIN jobs j ON j.job_id = s.job_id
JOIN job_outputs o ON o.output_id = s.output_id AND o.job_id = j.job_id
JOIN executor_result_manifests m ON m.manifest_id = s.result_manifest_id
    AND m.executor_execution_id = e.executor_execution_id AND m.submission_id = s.submission_id
JOIN executor_artifact_authorities a ON a.authority_id = m.artifact_authority_id
    AND a.executor_execution_id = e.executor_execution_id AND a.submission_id = s.submission_id
    AND a.output_id = o.output_id AND a.job_id = j.job_id
JOIN artifacts f ON f.job_id = j.job_id AND f.output_index = o.output_index
    AND f.artifact_id = o.output_id
    AND f.sha256_hex = a.sha256_hex AND f.byte_size = a.byte_size AND f.media_type = a.media_type
JOIN job_artifact_retention r ON r.job_id = j.job_id AND r.state = 'deleted'
WHERE e.state = 'succeeded' AND s.state = 'succeeded' AND j.state = 'succeeded'
    AND o.state = 'succeeded' AND e.executor_owner IS NULL AND e.lease_expires_at_ms IS NULL
    AND e.finished_at_ms < (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - 86400000
    AND e.executor_execution_id > $1
ORDER BY e.executor_execution_id
LIMIT $2
"#;

fn reclaim_completed_in_roots(
    roots: &[PathBuf],
    row: &CompletedCandidate,
) -> Result<u64, super::process::ProcessSpoolError> {
    use super::process::{ProcessSpoolError, TerminalPayloadCleanup, reclaim_terminal_payload};

    let expected = match (
        row.state.as_str(),
        row.error_code.as_deref(),
        row.sha256_hex.as_deref(),
        row.byte_size,
    ) {
        ("succeeded", None, Some(sha256_hex), Some(byte_size)) => {
            TerminalPayloadCleanup::Succeeded {
                sha256_hex,
                byte_size,
            }
        }
        ("failed", Some(error_code), _, _) => TerminalPayloadCleanup::Failed { error_code },
        _ => return Err(ProcessSpoolError::Integrity),
    };
    let mut found = false;
    let mut deleted_bytes = 0_u64;
    for root in roots {
        // Directory names are deployment choices; profiles may share a custom root.
        // The cleanup implementation binds spec/profile/terminal under its runner lock.
        let path = root.join(row.executor_execution_id.simple().to_string());
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(ProcessSpoolError::Unavailable),
            Ok(_) => found = true,
        }
        deleted_bytes = deleted_bytes.saturating_add(reclaim_terminal_payload(
            root,
            row.executor_execution_id,
            row.submission_id,
            row.execution_profile_id,
            expected,
        )?);
    }
    if found {
        Ok(deleted_bytes)
    } else {
        // Missing markers are not a durable cleanup completion receipt.
        Err(ProcessSpoolError::Unavailable)
    }
}

impl RunnerOutputRetention {
    /// Bounded, fenced queue claims followed by idempotent cleanup under the execution lock.
    /// No parent payload, customer object, economic record, or launch marker is deleted.
    pub async fn reconcile_completed(
        &mut self,
        pool: &PgPool,
        root: &Path,
        limit: u32,
    ) -> Result<RunnerCleanupOutcome, ImageGatewayError> {
        let root = root.to_path_buf();
        let roots = tokio::task::spawn_blocking(move || super::process::retention_roots(&root))
            .await
            .map_err(|_| ImageGatewayError::config("runner retention roots task failed"))?
            .map_err(|_| ImageGatewayError::config("runner retention roots invalid"))?;
        let mut outcome = RunnerCleanupOutcome::default();
        for _ in 0..limit.clamp(1, 100) {
            self.cleanup_claims = self.cleanup_claims.wrapping_add(1);
            let oldest_first = self.cleanup_claims.is_multiple_of(8);
            let Some(claim) = cleanup_queue::claim(pool, &self.cleanup_owner, oldest_first).await?
            else {
                break;
            };
            outcome.candidates += 1;
            let cleanup_queue::CleanupClaim::Ready(lease, row) = claim else {
                outcome.blocked += 1;
                continue;
            };
            let roots = roots.clone();
            let worker =
                tokio::task::spawn_blocking(move || reclaim_completed_in_roots(&roots, &row)).await;
            match worker {
                Ok(Ok(bytes)) => {
                    cleanup_queue::complete(pool, &lease, bytes).await?;
                    outcome.deleted_bytes = outcome.deleted_bytes.saturating_add(bytes);
                    outcome.reclaimed += u32::from(bytes > 0);
                }
                Ok(Err(error)) => {
                    cleanup_queue::retry(pool, &lease, "runner_payload_retained").await?;
                    outcome.blocked += 1;
                    tracing::warn!(execution_id = %lease.executor_execution_id, ?error, "runner payload cleanup deferred");
                }
                Err(error) => {
                    cleanup_queue::retry(pool, &lease, "runner_cleanup_worker_failed").await?;
                    outcome.blocked += 1;
                    tracing::error!(execution_id = %lease.executor_execution_id, ?error, "runner payload cleanup worker failed");
                }
            }
        }
        let status = cleanup_queue::status(pool).await?;
        outcome.pending = status.pending;
        outcome.blocked_pending = status.blocked_pending;
        outcome.oldest_pending_age_ms = status.oldest_pending_age_ms;
        Ok(outcome)
    }

    /// Keyset cursor advances even across retained/invalid/missing files, and wraps after a pass.
    /// No database writes, recursive scans, content reads, or provider calls are performed.
    pub async fn reconcile(
        &mut self,
        pool: &PgPool,
        root: &Path,
        limit: u32,
    ) -> Result<u64, ImageGatewayError> {
        let rows = sqlx::query_as::<_, Candidate>(CANDIDATES)
            .bind(self.after)
            .bind(i64::from(limit.clamp(1, 100)))
            .fetch_all(pool)
            .await
            .map_err(|_| ImageGatewayError::config("runner retention query failed"))?;
        if rows.is_empty() {
            self.after = Uuid::nil();
            return Ok(0);
        }
        self.after = rows.last().expect("nonempty batch").executor_execution_id;
        let root = root.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let roots = super::process::retention_roots(&root)
                .map_err(|_| ImageGatewayError::config("runner retention roots invalid"))?;
            let mut reclaimed = 0;
            for row in rows {
                for root in &roots {
                match super::process::reclaim_output(root, row.executor_execution_id, row.submission_id, &row.sha256_hex, row.byte_size) {
                    Ok(bytes) => reclaimed += bytes,
                    Err(error) => tracing::warn!(execution_id = %row.executor_execution_id, ?error, "runner output retained"),
                }
                }
            }
            Ok(reclaimed)
        }).await.map_err(|_| ImageGatewayError::config("runner retention task failed"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Connection, Executor};

    #[test]
    fn completed_cleanup_finds_shared_custom_roots_and_never_completes_missing_payloads() {
        use crate::executor::{ExecutorResultManifest, ExecutorSubmissionLease, RunnerOutcome};
        use crate::runner::{FilesystemRunnerJournal, process};

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("shared-custom-directory");
        let empty = temp.path().join("unrelated-custom-directory");
        let journal = FilesystemRunnerJournal::new(&root).unwrap();
        FilesystemRunnerJournal::new(&empty).unwrap();
        for _ in 0..2 {
            let lease = ExecutorSubmissionLease {
                submission_id: Uuid::new_v4(),
                executor_execution_id: Uuid::new_v4(),
                output_id: Uuid::new_v4(),
                job_id: Uuid::new_v4(),
                tenant_id: "cleanup-test".into(),
                provider_id: "openai-codex".into(),
                model: "gpt-image-2".into(),
                work_item_id: Uuid::new_v4(),
                output_index: 0,
                command_schema: "openai.images.generation.v1".into(),
                command_hash: "a".repeat(64),
                execution_profile_id: Uuid::new_v4(),
                adapter_revision: "openai-codex-generation-v1".into(),
                executor_owner: "cleanup-test".into(),
                executor_lease_epoch: 1,
                executor_lease_expires_at_ms: i64::MAX,
            };
            journal.start_or_attach(&lease).unwrap();
            let spool = process::ExecutionSpool::for_lease(&journal, &lease).unwrap();
            spool
                .prepare_request(br#"{"prompt":"local-only fixture"}"#)
                .unwrap();
            let lock = spool.acquire_runner_lock().unwrap();
            let mut identity = lock.identity().unwrap();
            spool.publish_process(&lock, &identity).unwrap();
            let bytes = b"durable result";
            let hash = process::sha256(bytes);
            spool.publish_output(bytes).unwrap();
            spool
                .publish_terminal(
                    &lock,
                    &process::ProcessTerminal::Succeeded {
                        helper_nonce: identity.nonce.clone(),
                        sha256_hex: hash.clone(),
                        byte_size: bytes.len() as u64,
                        provider_reported_cost: None,
                    },
                )
                .unwrap();
            journal.commit_launch(&lease).unwrap();
            journal
                .publish_terminal(
                    &lease,
                    &RunnerOutcome::Succeeded(
                        ExecutorResultManifest::new(
                            lease.submission_id,
                            lease.executor_execution_id,
                        )
                        .unwrap(),
                    ),
                )
                .unwrap();
            drop(lock);
            // Model an exited helper without launching any process or provider.
            identity.start_token = format!("retired:{}", identity.start_token);
            let path = root.join(lease.executor_execution_id.simple().to_string());
            std::fs::write(
                path.join("process.json"),
                serde_json::to_vec(&identity).unwrap(),
            )
            .unwrap();
            let mut row = CompletedCandidate {
                executor_execution_id: lease.executor_execution_id,
                submission_id: lease.submission_id,
                execution_profile_id: lease.execution_profile_id,
                resolution_decision_id: Uuid::new_v4(),
                state: "succeeded".into(),
                error_code: None,
                sha256_hex: Some(hash),
                byte_size: Some(bytes.len() as i64),
            };
            assert_eq!(
                reclaim_completed_in_roots(std::slice::from_ref(&empty), &row),
                Err(process::ProcessSpoolError::Unavailable)
            );
            row.execution_profile_id = Uuid::new_v4();
            assert_eq!(
                reclaim_completed_in_roots(std::slice::from_ref(&root), &row),
                Err(process::ProcessSpoolError::Integrity)
            );
            assert!(path.join("output.bin").is_file());
            row.execution_profile_id = lease.execution_profile_id;
            let roots = vec![empty.clone(), root.clone()];
            assert!(reclaim_completed_in_roots(&roots, &row).unwrap() > bytes.len() as u64);
            assert_eq!(reclaim_completed_in_roots(&roots, &row), Ok(0));
            assert!(!path.join("output.bin").exists());
            assert!(!path.join("provider-request.json").exists());
            assert!(path.join("terminal.json").is_file());
        }
    }

    #[tokio::test]
    async fn runner_retention_query_matches_migrated_schema() {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "TEST_DATABASE_URL required in CI"
            );
            return;
        };
        let schema = format!("runner_retention_test_{}", Uuid::new_v4().simple());
        let pool = crate::database::connect_test_pool_with_search_path(&url, 2, &schema)
            .await
            .unwrap();
        let database: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            database.to_ascii_lowercase().contains("test"),
            "test database required"
        );
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&pool)
            .await
            .unwrap();
        let migrated = crate::database::run_migrations(&pool).await;
        let result = sqlx::query_as::<_, Candidate>(CANDIDATES)
            .bind(Uuid::nil())
            .bind(100_i64)
            .fetch_all(&pool)
            .await;
        let completed_result = sqlx::query_as::<_, CompletedCandidate>(COMPLETED_CANDIDATES)
            .bind(Vec::<Uuid>::new())
            .fetch_all(&pool)
            .await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema}\" CASCADE"
        )))
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        migrated.unwrap();
        assert!(result.unwrap().is_empty());
        assert!(completed_result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn completed_runner_query_requires_canonical_settled_results_and_handles_mixed_outputs() {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "TEST_DATABASE_URL required in CI"
            );
            return;
        };
        let mut connection = sqlx::PgConnection::connect(&url).await.unwrap();
        let mut tx = connection.begin().await.unwrap();
        tx.execute(r#"
            CREATE TEMP TABLE executor_executions (executor_execution_id uuid, submission_id uuid,
                resolution_decision_id uuid, state text, error_code text, executor_owner text,
                lease_expires_at_ms bigint, finished_at_ms bigint);
            CREATE TEMP TABLE provider_submissions (executor_execution_id uuid, submission_id uuid,
                resolution_decision_id uuid, execution_profile_id uuid, provider_id text,
                command_schema text, adapter_revision text, job_id uuid, output_id uuid,
                result_manifest_id uuid, state text, error_code text);
            CREATE TEMP TABLE provider_execution_profiles (execution_profile_id uuid, profile_key text,
                provider_id text, command_schema text, adapter_revision text);
            CREATE TEMP TABLE jobs (job_id uuid, state text);
            CREATE TEMP TABLE job_outputs (output_id uuid, job_id uuid, output_index integer,
                state text, error_code text);
            CREATE TEMP TABLE executor_resolution_decisions (decision_id uuid, executor_execution_id uuid,
                submission_id uuid, resolved_state text, source text, observation_id uuid,
                resolution_fingerprint text, result_manifest_id uuid, error_code text);
            CREATE TEMP TABLE executor_runner_observations (observation_id uuid, executor_execution_id uuid,
                submission_id uuid, observed_state text, resolution_fingerprint text,
                result_manifest_id uuid, error_code text);
            CREATE TEMP TABLE executor_terminal_reductions (executor_execution_id uuid, submission_id uuid,
                resolution_decision_id uuid, resolved_state text, state text, lease_owner text,
                lease_expires_at_ms bigint, completed_at_ms bigint, provider_receipt_id uuid,
                quota_reservation_id uuid, customer_artifact_id uuid);
            CREATE TEMP TABLE executor_result_manifests (manifest_id uuid, executor_execution_id uuid,
                submission_id uuid, artifact_authority_id uuid);
            CREATE TEMP TABLE executor_artifact_authorities (authority_id uuid, executor_execution_id uuid,
                submission_id uuid, output_id uuid, job_id uuid, sha256_hex text, byte_size bigint,
                media_type text);
            CREATE TEMP TABLE artifacts (artifact_id uuid, job_id uuid, output_index integer,
                state text, sha256_hex text, byte_size bigint, media_type text);
            INSERT INTO executor_executions SELECT md5(i::text)::uuid, md5(('submission' || i)::text)::uuid,
                md5(i::text)::uuid, CASE WHEN i = 3 THEN 'failed' ELSE 'succeeded' END,
                CASE WHEN i = 3 THEN 'provider_failed' END, NULL, NULL,
                (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint - 1000
                FROM generate_series(1, 4) i;
            INSERT INTO provider_execution_profiles VALUES (md5('profile')::uuid, 'managed.codex.edits.test',
                'openai-codex', 'command-v1', 'adapter-v1');
            INSERT INTO provider_submissions SELECT executor_execution_id, submission_id,
                resolution_decision_id, md5('profile')::uuid, 'openai-codex', 'command-v1', 'adapter-v1',
                submission_id, submission_id, CASE WHEN state = 'succeeded' THEN submission_id END,
                state, error_code FROM executor_executions;
            INSERT INTO jobs SELECT submission_id,
                CASE WHEN executor_execution_id = md5('4')::uuid THEN 'running'
                     WHEN executor_execution_id = md5('1')::uuid THEN 'succeeded' ELSE 'failed' END
                FROM executor_executions;
            INSERT INTO job_outputs SELECT submission_id, submission_id, 0, state, error_code
                FROM executor_executions;
            INSERT INTO executor_resolution_decisions SELECT resolution_decision_id,
                executor_execution_id, submission_id, state, 'active_runner_observation',
                executor_execution_id, CASE WHEN state = 'succeeded' THEN 'manifest:' || submission_id::text
                ELSE 'error:' || error_code END, CASE WHEN state = 'succeeded' THEN submission_id END,
                error_code FROM executor_executions;
            INSERT INTO executor_runner_observations SELECT observation_id, executor_execution_id,
                submission_id, resolved_state, resolution_fingerprint, result_manifest_id, error_code
                FROM executor_resolution_decisions;
            INSERT INTO executor_terminal_reductions SELECT executor_execution_id, submission_id,
                resolution_decision_id, state, 'completed', NULL, NULL, finished_at_ms,
                submission_id, submission_id, CASE WHEN state = 'succeeded' THEN submission_id END
                FROM executor_executions;
            INSERT INTO executor_result_manifests SELECT submission_id, executor_execution_id,
                submission_id, executor_execution_id FROM executor_executions WHERE state = 'succeeded';
            INSERT INTO executor_artifact_authorities SELECT executor_execution_id, executor_execution_id,
                submission_id, submission_id, submission_id, repeat('a', 64), 8, 'image/png'
                FROM executor_executions WHERE state = 'succeeded';
            INSERT INTO artifacts SELECT submission_id, submission_id, 0, 'ready', repeat('a', 64),
                8, 'image/png' FROM executor_executions WHERE state = 'succeeded';
        "#).await.unwrap();
        let ids: Vec<Uuid> =
            sqlx::query_scalar("SELECT executor_execution_id FROM executor_executions")
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        let rows = sqlx::query_as::<_, CompletedCandidate>(COMPLETED_CANDIDATES)
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        let seen: Vec<_> = rows
            .iter()
            .map(|row| (row.executor_execution_id, row.state.as_str()))
            .collect();
        assert_eq!(
            seen.len(),
            3,
            "successful and failed children of terminal parents are reclaimable immediately"
        );
        assert_eq!(
            seen.iter()
                .filter(|(_, state)| *state == "succeeded")
                .count(),
            2
        );
        assert_eq!(
            seen.iter().filter(|(_, state)| *state == "failed").count(),
            1
        );
        for update in [
            "UPDATE jobs SET state = 'uncertain'",
            "UPDATE executor_executions SET executor_owner = 'active'",
            "UPDATE executor_executions SET lease_expires_at_ms = 1",
            "UPDATE executor_executions SET finished_at_ms = NULL",
            "UPDATE executor_executions SET finished_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint + 86400000",
            "UPDATE executor_executions SET state = 'uncertain'",
            "UPDATE provider_submissions SET state = 'uncertain'",
            "UPDATE job_outputs SET state = 'uncertain'",
            "UPDATE executor_terminal_reductions SET state = 'leased'",
            "UPDATE executor_terminal_reductions SET lease_owner = 'active'",
            "UPDATE executor_terminal_reductions SET lease_expires_at_ms = 1",
            "UPDATE executor_terminal_reductions SET completed_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint + 86400000",
            "UPDATE executor_terminal_reductions SET completed_at_ms = 1",
            "UPDATE executor_terminal_reductions SET provider_receipt_id = NULL",
            "UPDATE executor_terminal_reductions SET quota_reservation_id = NULL",
            "UPDATE executor_terminal_reductions SET resolution_decision_id = md5('different')::uuid",
            "UPDATE executor_executions SET resolution_decision_id = md5('different')::uuid",
            "UPDATE executor_resolution_decisions SET source = 'executor_lease_expired'",
            "UPDATE executor_resolution_decisions SET observation_id = md5('different')::uuid",
            "UPDATE executor_resolution_decisions SET resolved_state = 'uncertain'",
            "UPDATE executor_runner_observations SET resolution_fingerprint = 'error:different'",
            "UPDATE provider_execution_profiles SET provider_id = 'different'",
            "UPDATE provider_execution_profiles SET command_schema = 'different'",
            "UPDATE provider_execution_profiles SET adapter_revision = 'different'",
            "DELETE FROM executor_runner_observations",
            "DELETE FROM executor_resolution_decisions",
            "DELETE FROM executor_terminal_reductions",
        ] {
            tx.execute("SAVEPOINT negative_case").await.unwrap();
            tx.execute(update).await.unwrap();
            let rows = sqlx::query_as::<_, CompletedCandidate>(COMPLETED_CANDIDATES)
                .bind(&ids)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            assert!(
                rows.is_empty(),
                "unsafe completed candidate admitted by {update}"
            );
            tx.execute("ROLLBACK TO SAVEPOINT negative_case")
                .await
                .unwrap();
        }
        for update in [
            "UPDATE artifacts SET sha256_hex = repeat('b', 64)",
            "UPDATE artifacts SET byte_size = 7",
            "UPDATE artifacts SET media_type = 'image/jpeg'",
            "UPDATE artifacts SET state = 'missing'",
            "UPDATE job_outputs SET error_code = 'unexpected' WHERE state = 'succeeded'",
            "UPDATE executor_terminal_reductions SET customer_artifact_id = NULL WHERE resolved_state = 'succeeded'",
            "DELETE FROM executor_result_manifests",
            "DELETE FROM executor_artifact_authorities",
            "DELETE FROM artifacts",
        ] {
            tx.execute("SAVEPOINT success_negative_case").await.unwrap();
            tx.execute(update).await.unwrap();
            let rows = sqlx::query_as::<_, CompletedCandidate>(COMPLETED_CANDIDATES)
                .bind(&ids)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                1,
                "success without durable artifact admitted by {update}"
            );
            assert_eq!(rows[0].state, "failed");
            tx.execute("ROLLBACK TO SAVEPOINT success_negative_case")
                .await
                .unwrap();
        }
        for update in [
            "UPDATE executor_executions SET error_code = 'different' WHERE state = 'failed'",
            "UPDATE provider_submissions SET error_code = 'different' WHERE state = 'failed'",
            "UPDATE job_outputs SET error_code = 'different' WHERE state = 'failed'",
            "UPDATE executor_resolution_decisions SET error_code = 'different' WHERE resolved_state = 'failed'",
            "UPDATE executor_runner_observations SET error_code = 'different' WHERE observed_state = 'failed'",
        ] {
            tx.execute("SAVEPOINT failure_negative_case").await.unwrap();
            tx.execute(update).await.unwrap();
            let rows = sqlx::query_as::<_, CompletedCandidate>(COMPLETED_CANDIDATES)
                .bind(&ids)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            assert_eq!(
                rows.len(),
                2,
                "unmatched failure evidence admitted by {update}"
            );
            assert!(rows.iter().all(|row| row.state == "succeeded"));
            tx.execute("ROLLBACK TO SAVEPOINT failure_negative_case")
                .await
                .unwrap();
        }
        tx.rollback().await.unwrap();
    }

    // Temporary tables exercise the real PostgreSQL predicate without weakening production
    // state-machine constraints to manufacture negative cases.
    #[tokio::test]
    async fn runner_retention_query_requires_all_success_retained_authority_and_grace() {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "TEST_DATABASE_URL required in CI"
            );
            return;
        };
        let mut connection = sqlx::PgConnection::connect(&url).await.unwrap();
        let mut tx = connection.begin().await.unwrap();
        tx.execute(r#"
            CREATE TEMP TABLE executor_executions (executor_execution_id uuid, submission_id uuid,
                state text, executor_owner text, lease_expires_at_ms bigint, finished_at_ms bigint);
            CREATE TEMP TABLE provider_submissions (executor_execution_id uuid, submission_id uuid,
                job_id uuid, output_id uuid, result_manifest_id uuid, state text);
            CREATE TEMP TABLE jobs (job_id uuid, state text);
            CREATE TEMP TABLE job_outputs (output_id uuid, job_id uuid, output_index integer, state text);
            CREATE TEMP TABLE executor_result_manifests (manifest_id uuid, executor_execution_id uuid,
                submission_id uuid, artifact_authority_id uuid);
            CREATE TEMP TABLE executor_artifact_authorities (authority_id uuid, executor_execution_id uuid,
                submission_id uuid, output_id uuid, job_id uuid, sha256_hex text, byte_size bigint, media_type text);
            CREATE TEMP TABLE artifacts (artifact_id uuid, job_id uuid, output_index integer,
                sha256_hex text, byte_size bigint, media_type text);
            CREATE TEMP TABLE job_artifact_retention (job_id uuid, state text);
            INSERT INTO executor_executions SELECT md5(i::text)::uuid, md5(i::text)::uuid,
                'succeeded', NULL, NULL, 1 FROM generate_series(1, 3) i;
            INSERT INTO provider_submissions SELECT executor_execution_id, submission_id,
                submission_id, submission_id, submission_id, 'succeeded' FROM executor_executions;
            INSERT INTO jobs SELECT submission_id, 'succeeded' FROM executor_executions;
            INSERT INTO job_outputs SELECT job_id, job_id, 0, 'succeeded' FROM jobs;
            INSERT INTO executor_result_manifests SELECT submission_id, executor_execution_id,
                submission_id, submission_id FROM executor_executions;
            INSERT INTO executor_artifact_authorities SELECT submission_id, executor_execution_id,
                submission_id, submission_id, submission_id, repeat('a', 64), 8, 'image/png' FROM executor_executions;
            INSERT INTO artifacts SELECT job_id, job_id, 0, repeat('a', 64), 8, 'image/png' FROM jobs;
            INSERT INTO job_artifact_retention SELECT job_id, 'deleted' FROM jobs;
        "#).await.unwrap();
        let mut cursor = Uuid::nil();
        let mut seen = Vec::new();
        loop {
            let rows = sqlx::query_as::<_, Candidate>(CANDIDATES)
                .bind(cursor)
                .bind(1_i64)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            if rows.is_empty() {
                break;
            }
            cursor = rows[0].executor_execution_id;
            seen.push(cursor);
        }
        assert_eq!(
            seen.len(),
            3,
            "keyset batches must not starve later outputs"
        );
        for update in [
            "UPDATE jobs SET state = 'uncertain'",
            "UPDATE job_outputs SET state = 'failed'",
            "UPDATE provider_submissions SET state = 'uncertain'",
            "UPDATE executor_executions SET state = 'failed'",
            "UPDATE executor_executions SET executor_owner = 'active'",
            "UPDATE executor_executions SET lease_expires_at_ms = 1",
            "UPDATE executor_executions SET finished_at_ms = (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint",
            "UPDATE job_artifact_retention SET state = 'available'",
            "UPDATE job_artifact_retention SET state = 'deleting'",
            "UPDATE artifacts SET sha256_hex = repeat('b', 64)",
            "UPDATE artifacts SET byte_size = 7",
            "DELETE FROM executor_result_manifests",
            "DELETE FROM executor_artifact_authorities",
            "DELETE FROM artifacts",
        ] {
            tx.execute("SAVEPOINT negative_case").await.unwrap();
            tx.execute(update).await.unwrap();
            let rows = sqlx::query_as::<_, Candidate>(CANDIDATES)
                .bind(Uuid::nil())
                .bind(100_i64)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
            assert!(rows.is_empty(), "unsafe candidate admitted by {update}");
            tx.execute("ROLLBACK TO SAVEPOINT negative_case")
                .await
                .unwrap();
        }
        tx.rollback().await.unwrap();
    }
}
