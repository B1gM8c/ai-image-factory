//! A reviewed historical capacity exception, never evidence of provider failure.
//! No caller is scheduled: only the explicit root operator can apply a manifest.
use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::ImageGatewayError;

const RISK: &str = "unknown_original_may_still_occupy_upstream_capacity";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityRiskWaiverManifest {
    pub schema_version: u32,
    pub authorization_id: Uuid,
    pub provider_account_id: Uuid,
    pub resource_policy_id: Uuid,
    pub resource_policy_revision: i64,
    pub operator_reference: String,
    pub actor: String,
    pub risk_acknowledgement: String,
    pub observed_before_ms: i64,
    pub expires_at_ms: i64,
    pub originals: Vec<CapacityRiskWaiverOriginal>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityRiskWaiverOriginal {
    pub allocation_id: Uuid,
    pub submission_id: Uuid,
    pub executor_execution_id: Uuid,
    pub original_record_sha256: String,
}

#[derive(Debug, Serialize)]
pub struct CapacityRiskWaiverReceipt {
    pub authorization_id: Uuid,
    pub manifest_sha256: String,
    pub applied: bool,
    pub reviewed_originals: usize,
    pub already_applied: usize,
    pub raw_allocated_count: i32,
    pub waived_held_count: i32,
    pub effective_allocated_count: i32,
}

fn conflict() -> ImageGatewayError {
    ImageGatewayError::conflict(
        "Capacity waiver manifest does not match the current reviewed originals",
        None,
        "capacity_risk_waiver_conflict",
    )
}

fn unavailable(_: sqlx::Error) -> ImageGatewayError {
    // SQL errors can contain private row data. The operator must not print them.
    ImageGatewayError::config("capacity risk waiver storage check failed; no unverified success")
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn safe_text(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

pub fn parse_capacity_risk_waiver_manifest(
    bytes: &[u8],
    reviewed_sha256: &str,
) -> Result<CapacityRiskWaiverManifest, ImageGatewayError> {
    if bytes.len() > 32_768
        || !digest(reviewed_sha256)
        || hex::encode(Sha256::digest(bytes)) != reviewed_sha256
    {
        return Err(conflict());
    }
    let manifest: CapacityRiskWaiverManifest = serde_json::from_slice(bytes)
        .map_err(|_| ImageGatewayError::config("invalid capacity risk waiver manifest"))?;
    let mut allocations = HashSet::new();
    let mut submissions = HashSet::new();
    if manifest.schema_version != 1
        || manifest.authorization_id.is_nil()
        || manifest.provider_account_id.is_nil()
        || manifest.resource_policy_id.is_nil()
        || manifest.resource_policy_revision < 1
        || !safe_text(&manifest.operator_reference, 256)
        || !safe_text(&manifest.actor, 128)
        || manifest.risk_acknowledgement != RISK
        || manifest.observed_before_ms <= 0
        || manifest.expires_at_ms <= manifest.observed_before_ms
        || manifest.originals.is_empty()
        || manifest.originals.len() > 15
        || manifest.originals.iter().any(|row| {
            row.allocation_id.is_nil()
                || row.submission_id.is_nil()
                || row.allocation_id != row.executor_execution_id
                || !digest(&row.original_record_sha256)
                || !allocations.insert(row.allocation_id)
                || !submissions.insert(row.submission_id)
        })
    {
        return Err(conflict());
    }
    Ok(manifest)
}

/// Locks and revalidates exactly the supplied set in one transaction. Dry-run
/// always rolls back; apply is idempotent only for the same manifest bytes.
pub async fn review_or_apply_capacity_risk_waiver(
    pool: &PgPool,
    manifest_bytes: &[u8],
    reviewed_sha256: &str,
    apply: bool,
) -> Result<CapacityRiskWaiverReceipt, ImageGatewayError> {
    let manifest = parse_capacity_risk_waiver_manifest(manifest_bytes, reviewed_sha256)?;
    let mut tx = pool.begin().await.map_err(unavailable)?;
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SET LOCAL statement_timeout = '15s'")
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let now: i64 =
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::BIGINT")
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
    if manifest.observed_before_ms > now
        || manifest.expires_at_ms < now
        || manifest.expires_at_ms > now + 86_400_000
    {
        return Err(conflict());
    }
    let mut originals = manifest.originals.clone();
    originals.sort_by_key(|row| row.allocation_id);
    // Lock every original before the shared policy, using the same order as
    // normal capacity reconciliation. Do not update any original evidence.
    for row in &originals {
        let found: Option<Uuid> = sqlx::query_scalar(
            "SELECT a.allocation_id FROM executor_executions e
             JOIN provider_submissions s USING (executor_execution_id, submission_id)
             JOIN executor_capacity_allocations a USING (executor_execution_id, submission_id)
             WHERE a.allocation_id=$1 AND s.submission_id=$2 AND e.executor_execution_id=$3
               AND s.provider_account_id=$4 AND a.resource_policy_id=$5 AND a.resource_policy_revision=$6
             FOR UPDATE OF e, s, a")
            .bind(row.allocation_id).bind(row.submission_id).bind(row.executor_execution_id)
            .bind(manifest.provider_account_id).bind(manifest.resource_policy_id)
            .bind(manifest.resource_policy_revision).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if found.is_none() {
            return Err(conflict());
        }
        sqlx::query(
            "SELECT 1 FROM provider_remote_submit_intents WHERE submission_id=$1 FOR UPDATE",
        )
        .bind(row.submission_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            "SELECT 1 FROM provider_capacity_reconciliations WHERE submission_id=$1 FOR UPDATE",
        )
        .bind(row.submission_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    }
    let raw: Option<i32> = sqlx::query_scalar(
        "SELECT allocated_count FROM executor_resource_policies
         WHERE resource_policy_id=$1 AND revision=$2 AND provider_account_id=$3 FOR UPDATE",
    )
    .bind(manifest.resource_policy_id)
    .bind(manifest.resource_policy_revision)
    .bind(manifest.provider_account_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    let raw = raw.ok_or_else(conflict)?;
    // Serialize authorization reuse even if a malformed concurrent command
    // names a different account/policy. The lock has no persistent side effect.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(manifest.authorization_id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let reused: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM executor_capacity_risk_waivers
         WHERE authorization_id=$1 AND authorization_manifest_sha256<>$2)",
    )
    .bind(manifest.authorization_id)
    .bind(reviewed_sha256)
    .fetch_one(&mut *tx)
    .await
    .map_err(unavailable)?;
    if reused {
        return Err(conflict());
    }
    let mut already_applied = 0;
    for row in &originals {
        let existing: Option<(Uuid, String)> = sqlx::query_as(
            "SELECT authorization_id, authorization_manifest_sha256 FROM executor_capacity_risk_waivers WHERE allocation_id=$1")
            .bind(row.allocation_id).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        if let Some((authorization, hash)) = existing {
            if authorization != manifest.authorization_id || hash != reviewed_sha256 {
                return Err(conflict());
            }
            already_applied += 1;
            continue;
        }
        let eligible: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM executor_capacity_allocations a
             JOIN provider_submissions s USING (submission_id,executor_execution_id)
             JOIN executor_executions e USING (submission_id,executor_execution_id)
             JOIN provider_remote_submit_intents i USING (submission_id,executor_execution_id)
             JOIN provider_capacity_reconciliations r USING (submission_id,executor_execution_id)
             WHERE a.allocation_id=$1 AND a.state='held' AND a.acquired_at_ms<=$2
               AND e.state='uncertain' AND s.state='uncertain' AND s.provider_id='dreamina-cli'
               AND i.state='deadline_quarantined' AND i.remote_operation_id IS NULL
               AND r.state='active' AND r.evidence_revision=0
               AND executor_capacity_original_record_sha256($1)=$3)",
        )
        .bind(row.allocation_id)
        .bind(manifest.observed_before_ms)
        .bind(&row.original_record_sha256)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if !eligible {
            return Err(conflict());
        }
        if !apply {
            continue;
        }
        sqlx::query(
            "INSERT INTO executor_capacity_risk_waivers
             (allocation_id,submission_id,executor_execution_id,provider_account_id,resource_policy_id,
              resource_policy_revision,original_record_sha256,authorization_id,authorization_manifest_sha256,
              operator_reference,actor,risk_category,authorized_before_ms,created_at_ms)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(row.allocation_id).bind(row.submission_id).bind(row.executor_execution_id)
            .bind(manifest.provider_account_id).bind(manifest.resource_policy_id).bind(manifest.resource_policy_revision)
            .bind(&row.original_record_sha256).bind(manifest.authorization_id).bind(reviewed_sha256)
            .bind(&manifest.operator_reference).bind(&manifest.actor).bind(RISK)
            .bind(manifest.observed_before_ms).bind(now).execute(&mut *tx).await.map_err(unavailable)?;
    }
    sqlx::query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let mut waived: i32 = sqlx::query_scalar("SELECT executor_waived_held_count($1,$2)")
        .bind(manifest.resource_policy_id)
        .bind(manifest.resource_policy_revision)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
    if !apply {
        waived += (originals.len() - already_applied) as i32;
    }
    if apply {
        tx.commit().await.map_err(unavailable)?;
    } else {
        tx.rollback().await.map_err(unavailable)?;
    }
    Ok(CapacityRiskWaiverReceipt {
        authorization_id: manifest.authorization_id,
        manifest_sha256: reviewed_sha256.into(),
        applied: apply,
        reviewed_originals: originals.len(),
        already_applied,
        raw_allocated_count: raw,
        waived_held_count: waived,
        effective_allocated_count: raw - waived,
    })
}
