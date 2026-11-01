use super::*;

impl PostgresProviderTaskStore {
    /// Stop scheduling originals that cannot be queried. This only changes the
    /// queue disposition: even a frozen allocation's heartbeat stays untouched.
    pub async fn classify_capacity_reconciliation(
        &self,
        scope: &ProviderTaskClaimScope,
    ) -> Result<u64, ProviderTaskStoreError> {
        validate_scope(scope)?;
        let result = sqlx::query(
            r#"
            WITH candidates AS MATERIALIZED (
              SELECT reconciliation.submission_id,
                     CASE WHEN
                       EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id)
                       OR EXISTS(SELECT 1 FROM dreamina_operator_image_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id)
                     THEN 'original_evidence_frozen' ELSE 'missing_original_id' END AS reason,
                     floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT AS now_ms
              FROM provider_capacity_reconciliations reconciliation
              JOIN provider_submissions submission USING (submission_id, executor_execution_id)
              JOIN provider_remote_submit_intents intent USING (submission_id, executor_execution_id)
              WHERE reconciliation.provider_id = $1 AND reconciliation.provider_account_id = $2
                AND submission.execution_profile_id = $3
                AND reconciliation.state = 'active' AND reconciliation.review_status = 'auto'
                AND (reconciliation.reconciliation_owner IS NULL OR reconciliation.available_at_ms <= floor(extract(epoch FROM statement_timestamp()) * 1000)::BIGINT)
                AND (intent.remote_operation_id IS NULL
                  OR EXISTS(SELECT 1 FROM dreamina_operator_video_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id)
                  OR EXISTS(SELECT 1 FROM dreamina_operator_image_supplement_authorizations auth WHERE auth.submission_id = reconciliation.submission_id))
              ORDER BY reconciliation.available_at_ms, reconciliation.submission_id
              LIMIT 64 FOR UPDATE OF reconciliation SKIP LOCKED
            )
            UPDATE provider_capacity_reconciliations reconciliation
            SET review_status = 'manual_required', manual_reason = candidate.reason,
                reconciliation_owner = NULL, claimed_evidence_revision = NULL,
                available_at_ms = CASE WHEN reconciliation.reconciliation_owner IS NULL THEN reconciliation.available_at_ms ELSE GREATEST(reconciliation.available_at_ms, candidate.now_ms) END,
                updated_at_ms = CASE WHEN reconciliation.reconciliation_owner IS NULL THEN reconciliation.updated_at_ms ELSE candidate.now_ms END,
                last_command_kind = CASE WHEN reconciliation.reconciliation_owner IS NULL THEN reconciliation.last_command_kind ELSE 'defer' END,
                last_command_id = CASE WHEN reconciliation.reconciliation_owner IS NULL THEN reconciliation.last_command_id ELSE $4 END,
                last_command_owner = CASE WHEN reconciliation.reconciliation_owner IS NULL THEN reconciliation.last_command_owner ELSE reconciliation.reconciliation_owner END,
                last_command_lease_epoch = CASE WHEN reconciliation.reconciliation_owner IS NULL THEN reconciliation.last_command_lease_epoch ELSE reconciliation.reconciliation_lease_epoch END,
                claim_command_claimed_at_ms = NULL, claim_command_lease_expires_at_ms = NULL
            FROM candidates candidate
            WHERE reconciliation.submission_id = candidate.submission_id
            "#,
        )
        .bind(&scope.provider_id)
        .bind(scope.provider_account_id)
        .bind(scope.execution_profile_id)
        .bind(Uuid::new_v4().to_string())
        .execute(&self.pool)
        .await
        .map_err(storage_conflict)?;
        Ok(result.rows_affected())
    }

    /// Persist the query budget with the fenced lease. Repeated unavailable or
    /// unclassified replies stop automatic work; they never release capacity.
    pub async fn defer_capacity_query(
        &self,
        lease: &ProviderCapacityReconciliationLease,
        retry_after_ms: i64,
        failure_increment: u8,
    ) -> Result<(), ProviderTaskStoreError> {
        validate_lease(lease)?;
        if !(1..=MAX_RETRY_AFTER_MS).contains(&retry_after_ms) || failure_increment > 3 {
            return Err(ProviderTaskStoreError::InvalidInput);
        }
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        lock_held_capacity(
            &mut tx,
            lease.reconciliation.executor_execution_id,
            lease.reconciliation.submission_id,
        )
        .await?;
        let now = database_now(&mut tx).await?;
        require_one(sqlx::query(
            r#"
            UPDATE provider_capacity_reconciliations
            SET reconciliation_owner = NULL, claimed_evidence_revision = NULL,
                available_at_ms = CASE WHEN evidence_revision = $5 THEN $6 + $7 ELSE $6 END,
                last_command_kind = 'defer', last_command_id = $8,
                last_command_owner = $3, last_command_lease_epoch = $4,
                claim_command_claimed_at_ms = NULL, claim_command_lease_expires_at_ms = NULL,
                updated_at_ms = $6,
                query_failures = CASE WHEN CAST($9 AS INTEGER) > 0 AND evidence_revision = $5 THEN LEAST(query_failures + $9, 3) ELSE 0 END,
                review_status = CASE WHEN CAST($9 AS INTEGER) > 0 AND evidence_revision = $5 AND query_failures + $9 >= 3 THEN 'manual_required' ELSE 'auto' END,
                manual_reason = CASE WHEN CAST($9 AS INTEGER) > 0 AND evidence_revision = $5 AND query_failures + $9 >= 3 THEN 'query_unavailable' ELSE NULL END
            WHERE reconciliation_id = $1 AND submission_id = $2
              AND reconciliation_owner = $3 AND reconciliation_lease_epoch = $4
              AND state = 'active' AND review_status = 'auto' AND available_at_ms > $6
            "#,
        )
        .bind(lease.reconciliation.reconciliation_id)
        .bind(lease.reconciliation.submission_id)
        .bind(&lease.reconciliation_owner)
        .bind(lease.reconciliation_lease_epoch)
        .bind(lease.claimed_evidence_revision)
        .bind(now).bind(retry_after_ms).bind(Uuid::new_v4().to_string()).bind(i32::from(failure_increment))
        .execute(&mut *tx).await.map_err(storage_conflict)?, ProviderTaskStoreError::StaleLease)?;
        // Deliberately no allocation update: the query queue's lease is enough.
        tx.commit().await.map_err(storage_conflict)
    }
}
