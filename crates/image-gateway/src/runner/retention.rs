//! Reclaim duplicate successful runner outputs after official artifact retention completes.
//! This deliberately does not reclaim failed/uncertain evidence or change durable state.
use std::path::Path;

use sqlx::PgPool;
use uuid::Uuid;

use crate::ImageGatewayError;

#[derive(Default)]
pub struct RunnerOutputRetention {
    after: Uuid,
}

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

impl RunnerOutputRetention {
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
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema}\" CASCADE"
        )))
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        migrated.unwrap();
        assert!(result.unwrap().is_empty());
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
