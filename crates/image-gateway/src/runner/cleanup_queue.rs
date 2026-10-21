use sqlx::PgPool;
use uuid::Uuid;

use super::{COMPLETED_CANDIDATES, CompletedCandidate};
use crate::ImageGatewayError;

const LEASE_MS: i64 = 60_000;

#[derive(Clone, sqlx::FromRow)]
pub(super) struct CleanupLease {
    pub executor_execution_id: Uuid,
    submission_id: Uuid,
    execution_profile_id: Uuid,
    resolution_decision_id: Uuid,
    resolved_state: String,
    lease_epoch: i64,
    attempts: i32,
    lease_owner: String,
}

pub(super) enum CleanupClaim {
    Ready(CleanupLease, CompletedCandidate),
    Deferred,
}

#[derive(sqlx::FromRow)]
pub(super) struct QueueStatus {
    pub pending: i64,
    pub blocked_pending: i64,
    pub oldest_pending_age_ms: Option<i64>,
}

pub(super) async fn status(pool: &PgPool) -> Result<QueueStatus, ImageGatewayError> {
    sqlx::query_as(
        r#"
        SELECT COUNT(*) AS pending,
               COUNT(*) FILTER (WHERE last_error_code IS NOT NULL) AS blocked_pending,
               MAX(GREATEST(0, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
                   - eligible_at_ms)) AS oldest_pending_age_ms
        FROM executor_local_cleanup WHERE state IN ('pending', 'cleaning')
        "#,
    )
    .fetch_one(pool)
    .await
    .map_err(unavailable)
}

// A durable completion marker removes already-cleaned executions from the hot path.
// Expired owners may be replaced, while owner/epoch/expiry fence every write-back.
pub(super) async fn claim(
    pool: &PgPool,
    owner: &str,
    oldest_first: bool,
) -> Result<Option<CleanupClaim>, ImageGatewayError> {
    let Some(lease) = claim_lease(pool, owner, oldest_first).await? else {
        return Ok(None);
    };
    let candidate: Option<CompletedCandidate> = sqlx::query_as(COMPLETED_CANDIDATES)
        .bind(vec![lease.executor_execution_id])
        .fetch_optional(pool)
        .await
        .map_err(unavailable)?;
    match candidate {
        Some(candidate)
            if candidate.submission_id == lease.submission_id
                && candidate.execution_profile_id == lease.execution_profile_id
                && candidate.resolution_decision_id == lease.resolution_decision_id
                && candidate.state == lease.resolved_state =>
        {
            Ok(Some(CleanupClaim::Ready(lease, candidate)))
        }
        _ => {
            retry(pool, &lease, "terminal_evidence_pending").await?;
            Ok(Some(CleanupClaim::Deferred))
        }
    }
}

async fn claim_lease(
    pool: &PgPool,
    owner: &str,
    oldest_first: bool,
) -> Result<Option<CleanupLease>, ImageGatewayError> {
    let lease: Option<CleanupLease> = sqlx::query_as(
        r#"
        WITH clock AS (
            SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
        ), due AS (
            SELECT queue.executor_execution_id
            FROM executor_local_cleanup queue, clock
            WHERE (queue.state = 'pending' AND queue.next_attempt_at_ms <= clock.now_ms)
               OR (queue.state = 'cleaning' AND queue.lease_expires_at_ms <= clock.now_ms)
            -- A migration backfill must not put fresh completions behind historical rows.
            -- Deferred rows leave the due set until their bounded backoff expires.
            -- One in eight claims serves the oldest due row, so a busy stream also
            -- makes steady progress through historical cleanup and expired owners.
            ORDER BY CASE WHEN $3 THEN queue.eligible_at_ms END ASC,
                     queue.eligible_at_ms DESC,
                     queue.executor_execution_id
            FOR UPDATE OF queue SKIP LOCKED
            LIMIT 1
        )
        UPDATE executor_local_cleanup queue
        SET state = 'cleaning', lease_owner = $1, lease_epoch = lease_epoch + 1,
            lease_expires_at_ms = clock.now_ms + $2, attempts = attempts + 1,
            updated_at_ms = clock.now_ms
        FROM due, clock
        WHERE queue.executor_execution_id = due.executor_execution_id
        RETURNING queue.executor_execution_id, queue.submission_id, queue.execution_profile_id,
                  queue.resolution_decision_id, queue.resolved_state, queue.lease_epoch,
                  queue.attempts, queue.lease_owner
        "#,
    )
    .bind(owner)
    .bind(LEASE_MS)
    .bind(oldest_first)
    .fetch_optional(pool)
    .await
    .map_err(unavailable)?;
    Ok(lease)
}

pub(super) async fn complete(
    pool: &PgPool,
    lease: &CleanupLease,
    deleted_bytes: u64,
) -> Result<(), ImageGatewayError> {
    let deleted_bytes = i64::try_from(deleted_bytes)
        .map_err(|_| ImageGatewayError::internal("runner cleanup byte count is invalid"))?;
    let changed = sqlx::query(
        r#"
        WITH clock AS (
            SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
        )
        UPDATE executor_local_cleanup queue
        SET state = 'cleaned', lease_owner = NULL, lease_expires_at_ms = NULL,
            last_error_code = NULL, completed_at_ms = clock.now_ms,
            deleted_bytes = $4, updated_at_ms = clock.now_ms
        FROM clock
        WHERE queue.executor_execution_id = $1 AND queue.state = 'cleaning'
          AND queue.lease_owner = $2 AND queue.lease_epoch = $3
          AND queue.lease_expires_at_ms > clock.now_ms
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(&lease.lease_owner)
    .bind(lease.lease_epoch)
    .bind(deleted_bytes)
    .execute(pool)
    .await
    .map_err(unavailable)?
    .rows_affected();
    require_fenced_update(changed)
}

pub(super) async fn retry(
    pool: &PgPool,
    lease: &CleanupLease,
    error_code: &'static str,
) -> Result<(), ImageGatewayError> {
    let exponent = lease.attempts.saturating_sub(1).clamp(0, 6) as u32;
    let jitter_ms = (lease.executor_execution_id.as_u128() % 1001) as i64;
    let retry_delay_ms = (5_000_i64 * 2_i64.pow(exponent)).min(300_000) + jitter_ms;
    let changed = sqlx::query(
        r#"
        WITH clock AS (
            SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
        )
        UPDATE executor_local_cleanup queue
        SET state = 'pending', lease_owner = NULL, lease_expires_at_ms = NULL,
            next_attempt_at_ms = GREATEST(queue.eligible_at_ms, clock.now_ms + $4),
            last_error_code = $5, updated_at_ms = clock.now_ms
        FROM clock
        WHERE queue.executor_execution_id = $1 AND queue.state = 'cleaning'
          AND queue.lease_owner = $2 AND queue.lease_epoch = $3
          AND queue.lease_expires_at_ms > clock.now_ms
        "#,
    )
    .bind(lease.executor_execution_id)
    .bind(&lease.lease_owner)
    .bind(lease.lease_epoch)
    .bind(retry_delay_ms)
    .bind(error_code)
    .execute(pool)
    .await
    .map_err(unavailable)?
    .rows_affected();
    require_fenced_update(changed)
}

fn require_fenced_update(changed: u64) -> Result<(), ImageGatewayError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(ImageGatewayError::service_unavailable(
            "runner cleanup lease is stale",
        ))
    }
}

fn unavailable(error: impl std::fmt::Display) -> ImageGatewayError {
    tracing::error!(error = %error, "runner cleanup queue unavailable");
    ImageGatewayError::service_unavailable("runner cleanup queue unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn durable_cleanup_queue_fences_owners_reclaims_expiry_and_skips_cleaned_rows() {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "TEST_DATABASE_URL required in CI"
            );
            return;
        };
        let schema = format!("runner_cleanup_queue_test_{}", Uuid::new_v4().simple());
        let pool = crate::database::connect_test_pool_with_search_path(&url, 1, &schema)
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
        crate::database::run_migrations(&pool).await.unwrap();

        // A temporary queue copies the real schema checks/indexes and uses its real trigger.
        // It omits FK data setup so expiry/fencing negative cases cannot weaken durable facts.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TEMP TABLE executor_local_cleanup (LIKE \"{schema}\".executor_local_cleanup INCLUDING ALL)"
        ))).execute(&pool).await.unwrap();
        sqlx::query("CREATE TRIGGER queue_guard BEFORE UPDATE OR DELETE ON executor_local_cleanup FOR EACH ROW EXECUTE FUNCTION protect_executor_local_cleanup()")
            .execute(&pool).await.unwrap();
        sqlx::query("CREATE TRIGGER queue_truncate_guard BEFORE TRUNCATE ON executor_local_cleanup FOR EACH STATEMENT EXECUTE FUNCTION protect_executor_local_cleanup()")
            .execute(&pool).await.unwrap();
        sqlx::query(r#"
            INSERT INTO executor_local_cleanup (
                executor_execution_id, submission_id, execution_profile_id, resolution_decision_id,
                resolved_state, state, eligible_at_ms, next_attempt_at_ms, lease_owner, lease_epoch,
                lease_expires_at_ms, attempts, created_at_ms, updated_at_ms
            ) SELECT md5(i::text)::uuid, md5(('submission' || i)::text)::uuid,
                md5('profile')::uuid, md5(i::text)::uuid, 'succeeded',
                CASE WHEN i = 4 THEN 'cleaning' ELSE 'pending' END, i::bigint,
                CASE WHEN i = 3 THEN (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint + 86400000 ELSE i::bigint END,
                CASE WHEN i = 4 THEN 'expired-owner' END, CASE WHEN i = 4 THEN 2 ELSE 0 END,
                CASE WHEN i = 4 THEN 0 END, CASE WHEN i = 4 THEN 2 ELSE 0 END, 1, 1
            FROM generate_series(1, 4) i
        "#).execute(&pool).await.unwrap();
        let expired = claim_lease(&pool, "owner-a", false).await.unwrap().unwrap();
        assert_eq!(
            expired.lease_epoch, 3,
            "expired lease must increment its epoch"
        );
        let first = claim_lease(&pool, "owner-b", false).await.unwrap().unwrap();
        let newest_pending: Uuid = sqlx::query_scalar("SELECT md5('2')::uuid")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            first.executor_execution_id, newest_pending,
            "fresh completions take priority over an older due backlog"
        );
        let second = claim_lease(&pool, "owner-c", false).await.unwrap().unwrap();
        assert_ne!(first.executor_execution_id, expired.executor_execution_id);
        assert_ne!(first.executor_execution_id, second.executor_execution_id);
        assert!(
            claim_lease(&pool, "other", false).await.unwrap().is_none(),
            "live leases and future rows are skipped"
        );

        let mut stale = expired.clone();
        stale.lease_epoch -= 1;
        stale.lease_owner = "expired-owner".to_owned();
        assert!(complete(&pool, &stale, 1000).await.is_err());
        assert!(retry(&pool, &stale, "stale_owner").await.is_err());
        complete(&pool, &expired, 111).await.unwrap();
        retry(&pool, &first, "runner_payload_retained")
            .await
            .unwrap();
        complete(&pool, &second, 222).await.unwrap();
        assert!(
            complete(&pool, &first, 999).await.is_err(),
            "retry invalidates completion by the old claim"
        );
        assert!(
            claim_lease(&pool, "other", false).await.unwrap().is_none(),
            "cleaned rows never reenter the hot path"
        );
        let (pending, cleaned, bytes): (i64, i64, i64) = sqlx::query_as(
            "SELECT COUNT(*) FILTER (WHERE state = 'pending'), COUNT(*) FILTER (WHERE state = 'cleaned'), COALESCE(SUM(deleted_bytes), 0)::bigint FROM executor_local_cleanup"
        ).fetch_one(&pool).await.unwrap();
        assert_eq!((pending, cleaned, bytes), (2, 2, 333));
        let status = status(&pool).await.unwrap();
        assert_eq!((status.pending, status.blocked_pending), (2, 1));
        assert!(status.oldest_pending_age_ms.is_some_and(|age| age > 0));
        let delayed: bool = sqlx::query_scalar(
            "SELECT next_attempt_at_ms > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint AND last_error_code = 'runner_payload_retained' FROM executor_local_cleanup WHERE executor_execution_id = $1"
        ).bind(first.executor_execution_id).fetch_one(&pool).await.unwrap();
        assert!(
            delayed,
            "blocked cleanup must back off rather than starve later rows"
        );
        assert!(
            sqlx::query(
                "UPDATE executor_local_cleanup SET deleted_bytes = 999 WHERE state = 'cleaned'"
            )
            .execute(&pool)
            .await
            .is_err()
        );
        assert!(
            sqlx::query("DELETE FROM executor_local_cleanup")
                .execute(&pool)
                .await
                .is_err()
        );
        assert!(
            sqlx::query("TRUNCATE executor_local_cleanup")
                .execute(&pool)
                .await
                .is_err()
        );
        assert!(sqlx::query("UPDATE executor_local_cleanup SET execution_profile_id = md5('different')::uuid WHERE state = 'pending'")
            .execute(&pool).await.is_err());

        sqlx::query(r#"
            INSERT INTO executor_local_cleanup (executor_execution_id, submission_id, execution_profile_id,
                resolution_decision_id, resolved_state, eligible_at_ms, next_attempt_at_ms, created_at_ms, updated_at_ms)
            VALUES (md5('5')::uuid, md5('submission5')::uuid, md5('profile')::uuid,
                md5('5')::uuid, 'failed', 1, 1, 1, 1)
        "#).execute(&pool).await.unwrap();
        assert!(
            matches!(
                claim(&pool, "evidence-check", false).await.unwrap(),
                Some(CleanupClaim::Deferred)
            ),
            "queue identity alone cannot authorize deletion without canonical terminal facts"
        );
        assert!(claim_lease(&pool, "other", false).await.unwrap().is_none());
        sqlx::query(r#"
            INSERT INTO executor_local_cleanup (executor_execution_id, submission_id, execution_profile_id,
                resolution_decision_id, resolved_state, eligible_at_ms, next_attempt_at_ms, created_at_ms, updated_at_ms)
            SELECT md5(('fair' || i)::text)::uuid, md5(('fair-submission' || i)::text)::uuid,
                md5('profile')::uuid, md5(('fair' || i)::text)::uuid, 'failed', i, i, i, i
            FROM generate_series(10, 11) i
        "#).execute(&pool).await.unwrap();
        let oldest = claim_lease(&pool, "oldest-quota", true)
            .await
            .unwrap()
            .unwrap();
        let expected_oldest: Uuid = sqlx::query_scalar("SELECT md5('fair10')::uuid")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            oldest.executor_execution_id, expected_oldest,
            "oldest quota prevents a continuous fresh stream from starving history"
        );
        complete(&pool, &oldest, 0).await.unwrap();
        let newest = claim_lease(&pool, "fresh-quota", false)
            .await
            .unwrap()
            .unwrap();
        complete(&pool, &newest, 0).await.unwrap();

        // Exercise the migrated enqueue function on small temporary facts. It must be
        // atomic with completion, idempotent, and exclude uncertain/non-runner outcomes.
        use sqlx::Executor;
        pool.execute(r#"
            CREATE TEMP TABLE executor_executions (executor_execution_id uuid, submission_id uuid,
                resolution_decision_id uuid, state text);
            CREATE TEMP TABLE provider_submissions (executor_execution_id uuid, submission_id uuid,
                resolution_decision_id uuid, execution_profile_id uuid, state text);
            CREATE TEMP TABLE executor_resolution_decisions (decision_id uuid, executor_execution_id uuid,
                submission_id uuid, source text);
            CREATE TEMP TABLE executor_terminal_reductions (executor_execution_id uuid, submission_id uuid,
                resolution_decision_id uuid, resolved_state text, state text);
            CREATE TRIGGER enqueue_test AFTER INSERT OR UPDATE OF state ON executor_terminal_reductions
                FOR EACH ROW EXECUTE FUNCTION enqueue_executor_local_cleanup();
            INSERT INTO executor_executions SELECT md5(('enqueue' || i)::text)::uuid,
                md5(('enqueue-submission' || i)::text)::uuid, md5(('enqueue' || i)::text)::uuid,
                CASE WHEN i = 2 THEN 'failed' WHEN i = 3 THEN 'uncertain' ELSE 'succeeded' END
                FROM generate_series(1, 4) i;
            INSERT INTO provider_submissions SELECT executor_execution_id, submission_id,
                resolution_decision_id, md5('profile')::uuid, state FROM executor_executions;
            INSERT INTO executor_resolution_decisions SELECT resolution_decision_id, executor_execution_id,
                submission_id, CASE WHEN executor_execution_id = md5('enqueue4')::uuid
                    THEN 'executor_lease_expired' ELSE 'active_runner_observation' END FROM executor_executions;
            INSERT INTO executor_terminal_reductions SELECT executor_execution_id, submission_id,
                resolution_decision_id, state, 'pending' FROM executor_executions;
        "#).await.unwrap();
        let queued_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executor_local_cleanup")
            .fetch_one(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE executor_terminal_reductions SET state = 'completed'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE executor_terminal_reductions SET state = 'completed'")
            .execute(&pool)
            .await
            .unwrap();
        let queued_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executor_local_cleanup")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            queued_after - queued_before,
            2,
            "exact succeeded/failed completions enqueue once, uncertain and non-runner facts do not"
        );
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema}\" CASCADE"
        )))
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }
}
