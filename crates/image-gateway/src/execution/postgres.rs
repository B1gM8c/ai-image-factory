use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::{ExecutionContextError, ExecutionContextStore, GenerationExecutionContext};
use crate::{
    admission::{
        GENERATION_COMMAND_SCHEMA, GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION,
        GenerationCommandV1, WorkLease,
    },
    artifacts::GENERATION_RESPONSE_SCHEMA,
    generator::GenerationJob,
    usage::{UsageCharge, UsageLimits, UsageReservation, UsageSnapshot},
};

#[derive(Clone)]
pub struct PostgresExecutionContextStore {
    pool: PgPool,
}

impl PostgresExecutionContextStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct ExecutionRow {
    job_id: Uuid,
    tenant_id: String,
    request_id: String,
    operation: String,
    provider_id: String,
    model: String,
    requested_units: i32,
    reservation_id: Uuid,
    command_schema: String,
    command_json: Value,
    request_hash: String,
    limit_5h: Option<i32>,
    remaining_5h: Option<i32>,
    limit_7d: Option<i32>,
    remaining_7d: Option<i32>,
    quota_state: String,
    quota_expires_at_ms: i64,
    job_state: String,
    database_now_ms: i64,
}

#[async_trait]
impl ExecutionContextStore for PostgresExecutionContextStore {
    async fn load_generation(
        &self,
        lease: &WorkLease,
    ) -> Result<GenerationExecutionContext, ExecutionContextError> {
        let row: ExecutionRow = sqlx::query_as(
            r#"
            SELECT j.job_id, j.tenant_id, j.request_id, j.operation, j.provider_id,
                   j.model, j.requested_units, j.reservation_id,
                   p.command_schema, p.command_json, p.request_hash,
                   qr.limit_5h, qr.remaining_5h, qr.limit_7d, qr.remaining_7d,
                   qr.state AS quota_state, qr.expires_at_ms AS quota_expires_at_ms,
                   j.state AS job_state,
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS database_now_ms
            FROM work_items w
            JOIN job_attempts a
              ON a.work_item_id = w.work_item_id
             AND a.execution_id = w.execution_id
             AND a.lease_epoch = w.lease_epoch
            JOIN job_payloads p ON p.job_id = w.job_id
            JOIN jobs j ON j.job_id = w.job_id
            JOIN quota_reservations qr
              ON qr.job_id = j.job_id
             AND qr.reservation_id = j.reservation_id
             AND qr.tenant_id = j.tenant_id
             AND qr.request_id = j.request_id
            WHERE w.work_item_id = $1 AND w.job_id = $2 AND w.execution_id = $3
              AND w.lease_epoch = $4 AND w.lease_owner = $5
              AND a.worker_id = $5
              AND ((w.state = 'leased' AND a.state = 'claimed')
                OR (w.state = 'running' AND a.state = 'running'))
              AND w.lease_expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.job_id)
        .bind(lease.execution_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| ExecutionContextError::Unavailable)?
        .ok_or(ExecutionContextError::Unavailable)?;

        let (reservation, reservation_valid) = reservation_from_row(&row);
        if !reservation_valid {
            return Err(ExecutionContextError::Invalid { reservation });
        }

        if lease.command_schema != row.command_schema
            || lease.command_json != row.command_json
            || row.command_schema != GENERATION_COMMAND_SCHEMA
            || row.operation != GENERATION_OPERATION
        {
            return Err(ExecutionContextError::Invalid { reservation });
        }
        let command: GenerationCommandV1 =
            serde_json::from_value(row.command_json).map_err(|_| {
                ExecutionContextError::Invalid {
                    reservation: reservation.clone(),
                }
            })?;
        if command.schema_version != GENERATION_COMMAND_SCHEMA_VERSION
            || command.operation != GENERATION_OPERATION
            || command.request_hash_hex() != row.request_hash
            || command.provider_id != row.provider_id
            || command.model != row.model
            || i32::try_from(command.n).ok() != Some(row.requested_units)
        {
            return Err(ExecutionContextError::Invalid { reservation });
        }
        Ok(GenerationExecutionContext {
            job: GenerationJob {
                request_id: row.request_id,
                model: command.model,
                prompt: command.prompt,
                n: command.n,
                size: command.size,
                quality: command.quality,
                output_format: command.output_format,
                output_compression: command.output_compression,
                background: command.background,
                stream: command.stream,
                partial_images: command.partial_images,
            },
            reservation,
            api_profile: command.source_api_profile,
            response_schema: GENERATION_RESPONSE_SCHEMA.to_string(),
        })
    }
}

fn reservation_from_row(row: &ExecutionRow) -> (UsageReservation, bool) {
    let limit_5h = row.limit_5h.and_then(|value| u32::try_from(value).ok());
    let remaining_5h = row.remaining_5h.and_then(|value| u32::try_from(value).ok());
    let limit_7d = row.limit_7d.and_then(|value| u32::try_from(value).ok());
    let remaining_7d = row.remaining_7d.and_then(|value| u32::try_from(value).ok());
    let units = u32::try_from(row.requested_units).ok();
    let valid = limit_5h.is_some()
        && remaining_5h.is_some()
        && limit_7d.is_some()
        && remaining_7d.is_some()
        && units.is_some()
        && row.quota_state == "reserved"
        && row.quota_expires_at_ms > row.database_now_ms
        && matches!(row.job_state.as_str(), "reserved" | "running");
    let snapshot = UsageSnapshot {
        limit_5h: limit_5h.unwrap_or_default(),
        remaining_5h: remaining_5h.unwrap_or_default(),
        limit_7d: limit_7d.unwrap_or_default(),
        remaining_7d: remaining_7d.unwrap_or_default(),
    };
    (
        UsageReservation {
            reservation_id: row.reservation_id,
            job_id: row.job_id,
            charge: UsageCharge {
                tenant_id: row.tenant_id.clone(),
                request_id: row.request_id.clone(),
                operation: GENERATION_OPERATION,
                provider_id: row.provider_id.clone(),
                model: row.model.clone(),
                units: units.unwrap_or_default(),
                limits: UsageLimits {
                    five_hour_image_limit: snapshot.limit_5h,
                    seven_day_image_limit: snapshot.limit_7d,
                },
            },
            snapshot,
        },
        valid,
    )
}
