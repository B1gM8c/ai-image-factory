use std::collections::HashSet;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    EditExecutionContext, ExecutionContextError, ExecutionContextStore, GenerationExecutionContext,
    PersistedEditInput,
};
use crate::{
    admission::{
        EDIT_COMMAND_SCHEMA, EDIT_COMMAND_SCHEMA_VERSION, EDIT_INPUT_MANIFEST_SCHEMA,
        EDIT_OPERATION, EditCommandV1, EditInputDescriptorV1, EditInputRoleV1,
        GENERATION_COMMAND_SCHEMA, GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION,
        GenerationCommandV1, WorkLease,
    },
    artifacts::GENERATION_RESPONSE_SCHEMA,
    generator::GenerationJob,
    input_blobs::{InputBlobKey, InputBlobRef},
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
    quota_admission_session_id: Option<Uuid>,
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

#[derive(sqlx::FromRow)]
struct EditExecutionRow {
    #[sqlx(flatten)]
    execution: ExecutionRow,
    payload_session_id: Uuid,
    admission_tenant_id: String,
    admission_request_id: String,
    admission_operation: String,
    admission_api_profile: String,
    admission_request_hash: String,
    admission_state: String,
    admission_job_id: Option<Uuid>,
    quota_session_id: Option<Uuid>,
    quota_requested_units: i32,
    manifest_session_id: Uuid,
    manifest_schema: String,
    manifest_hash: String,
    manifest_input_count: i16,
    input_id: Uuid,
    input_session_id: Uuid,
    input_role: String,
    input_index: i16,
    input_media_type: String,
    storage_backend: String,
    object_key: String,
    input_sha256_hex: String,
    input_byte_size: i64,
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
                   qr.admission_session_id AS quota_admission_session_id,
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

        let (reservation, reservation_valid) = reservation_from_row(&row, GENERATION_OPERATION);
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
                moderation: command.moderation.unwrap_or_else(|| "auto".to_string()),
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

    async fn load_edit(
        &self,
        lease: &WorkLease,
    ) -> Result<EditExecutionContext, ExecutionContextError> {
        let rows: Vec<EditExecutionRow> = sqlx::query_as(
            r#"
            SELECT j.job_id, j.tenant_id, j.request_id, j.operation, j.provider_id,
                   j.model, j.requested_units, j.reservation_id,
                   qr.admission_session_id AS quota_admission_session_id,
                   p.command_schema, p.command_json, p.request_hash,
                   qr.limit_5h, qr.remaining_5h, qr.limit_7d, qr.remaining_7d,
                   qr.state AS quota_state, qr.expires_at_ms AS quota_expires_at_ms,
                   j.state AS job_state,
                   floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT AS database_now_ms,
                   p.admission_session_id AS payload_session_id,
                   s.tenant_id AS admission_tenant_id,
                   s.request_id AS admission_request_id,
                   s.operation AS admission_operation,
                   s.api_profile AS admission_api_profile,
                   s.request_hash AS admission_request_hash,
                   s.state AS admission_state,
                   s.job_id AS admission_job_id,
                   qr.admission_session_id AS quota_session_id,
                   qr.requested_units AS quota_requested_units,
                   m.admission_session_id AS manifest_session_id,
                   m.manifest_schema, m.manifest_hash,
                   m.input_count AS manifest_input_count,
                   i.input_id, i.admission_session_id AS input_session_id,
                   i.role AS input_role, i.input_index,
                   i.media_type AS input_media_type,
                   i.storage_backend, i.object_key,
                   i.sha256_hex AS input_sha256_hex,
                   i.byte_size AS input_byte_size
            FROM work_items w
            JOIN job_attempts a
              ON a.work_item_id = w.work_item_id
             AND a.execution_id = w.execution_id
             AND a.lease_epoch = w.lease_epoch
            JOIN job_payloads p ON p.job_id = w.job_id
            JOIN jobs j ON j.job_id = w.job_id
            JOIN admission_sessions s ON s.session_id = p.admission_session_id
            JOIN quota_reservations qr
              ON qr.job_id = j.job_id
             AND qr.reservation_id = j.reservation_id
             AND qr.tenant_id = j.tenant_id
             AND qr.request_id = j.request_id
            JOIN job_input_manifests m ON m.job_id = j.job_id
            JOIN job_input_objects i ON i.job_id = m.job_id
            WHERE w.work_item_id = $1 AND w.job_id = $2 AND w.execution_id = $3
              AND w.lease_epoch = $4 AND w.lease_owner = $5
              AND a.worker_id = $5
              AND ((w.state = 'leased' AND a.state = 'claimed')
                OR (w.state = 'running' AND a.state = 'running'))
              AND w.lease_expires_at_ms > floor(extract(epoch FROM clock_timestamp()) * 1000)::BIGINT
            ORDER BY CASE i.role WHEN 'image' THEN 0 ELSE 1 END, i.input_index
            "#,
        )
        .bind(lease.work_item_id)
        .bind(lease.job_id)
        .bind(lease.execution_id)
        .bind(lease.lease_epoch)
        .bind(&lease.worker_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| ExecutionContextError::Unavailable)?;
        let first = rows.first().ok_or(ExecutionContextError::Unavailable)?;
        let (reservation, reservation_valid) =
            reservation_from_row(&first.execution, EDIT_OPERATION);
        if !reservation_valid {
            return Err(ExecutionContextError::Invalid { reservation });
        }

        let command: EditCommandV1 = serde_json::from_value(first.execution.command_json.clone())
            .map_err(|_| invalid(&reservation))?;
        if !valid_edit_envelope(lease, first, &command, rows.len()) {
            return Err(ExecutionContextError::Invalid { reservation });
        }
        let inputs = rebuild_edit_inputs(&rows, &command).map_err(|_| invalid(&reservation))?;
        Ok(EditExecutionContext {
            command,
            inputs,
            reservation,
            response_schema: GENERATION_RESPONSE_SCHEMA.to_string(),
        })
    }
}

fn reservation_from_row(row: &ExecutionRow, operation: &'static str) -> (UsageReservation, bool) {
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
                admission_session_id: row.quota_admission_session_id,
                operation,
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

fn valid_edit_envelope(
    lease: &WorkLease,
    row: &EditExecutionRow,
    command: &EditCommandV1,
    row_count: usize,
) -> bool {
    let execution = &row.execution;
    lease.command_schema == execution.command_schema
        && lease.command_json == execution.command_json
        && execution.command_schema == EDIT_COMMAND_SCHEMA
        && execution.operation == EDIT_OPERATION
        && command.schema_version == EDIT_COMMAND_SCHEMA_VERSION
        && command.operation == EDIT_OPERATION
        && command.request_hash_hex() == execution.request_hash
        && command.provider_id == execution.provider_id
        && command.model == execution.model
        && i32::try_from(command.n).ok() == Some(execution.requested_units)
        && row.payload_session_id == row.manifest_session_id
        && row.quota_session_id == Some(row.payload_session_id)
        && row.quota_requested_units == execution.requested_units
        && row.admission_job_id == Some(execution.job_id)
        && row.admission_tenant_id == execution.tenant_id
        && row.admission_request_id == execution.request_id
        && row.admission_operation == EDIT_OPERATION
        && row.admission_api_profile == command.source_api_profile
        && row.admission_request_hash == execution.request_hash
        && row.admission_state == "attached"
        && row.manifest_schema == EDIT_INPUT_MANIFEST_SCHEMA
        && row.manifest_hash == command.input_manifest_hash_hex()
        && usize::try_from(row.manifest_input_count).ok() == Some(command.inputs.len())
        && row_count == command.inputs.len()
        && (1..=17).contains(&row_count)
}

fn rebuild_edit_inputs(
    rows: &[EditExecutionRow],
    command: &EditCommandV1,
) -> Result<Vec<PersistedEditInput>, ()> {
    let mut positions = HashSet::new();
    let mut input_ids = HashSet::new();
    let mut object_keys = HashSet::new();
    let mut image_count = 0_usize;
    let mut mask_count = 0_usize;
    for row in rows {
        let role = parse_role(&row.input_role).ok_or(())?;
        let index = u16::try_from(row.input_index).map_err(|_| ())?;
        let byte_size = u64::try_from(row.input_byte_size).map_err(|_| ())?;
        if row.input_session_id != row.payload_session_id
            || row.storage_backend.is_empty()
            || row.object_key.is_empty()
            || !is_sha256(&row.input_sha256_hex)
            || byte_size == 0
            || !positions.insert((role.as_str(), index))
            || !input_ids.insert(row.input_id)
            || !object_keys.insert((row.storage_backend.as_str(), row.object_key.as_str()))
            || !valid_input_shape(role, index, &row.input_media_type)
        {
            return Err(());
        }
        match role {
            EditInputRoleV1::Image => image_count += 1,
            EditInputRoleV1::Mask => mask_count += 1,
        }
    }
    if !(1..=16).contains(&image_count) || mask_count > 1 {
        return Err(());
    }

    let mut descriptor_positions = HashSet::new();
    if command
        .inputs
        .iter()
        .any(|input| !descriptor_positions.insert((input.role.as_str(), input.index)))
    {
        return Err(());
    }

    command
        .inputs
        .iter()
        .map(|descriptor| persisted_input_for_descriptor(rows, descriptor))
        .collect()
}

fn persisted_input_for_descriptor(
    rows: &[EditExecutionRow],
    descriptor: &EditInputDescriptorV1,
) -> Result<PersistedEditInput, ()> {
    let row = rows
        .iter()
        .find(|row| {
            row.input_role == descriptor.role.as_str()
                && u16::try_from(row.input_index).ok() == Some(descriptor.index)
        })
        .ok_or(())?;
    let byte_size = u64::try_from(row.input_byte_size).map_err(|_| ())?;
    if descriptor.byte_size != byte_size
        || descriptor.media_type != row.input_media_type
        || descriptor.sha256_hex != row.input_sha256_hex
    {
        return Err(());
    }
    Ok(PersistedEditInput {
        blob: InputBlobRef {
            key: InputBlobKey {
                admission_session_id: row.input_session_id,
                input_id: row.input_id,
            },
            storage_backend: row.storage_backend.clone(),
            object_key: row.object_key.clone(),
            sha256_hex: row.input_sha256_hex.clone(),
            byte_size,
        },
        role: descriptor.role,
        index: descriptor.index,
        media_type: descriptor.media_type.clone(),
    })
}

fn parse_role(role: &str) -> Option<EditInputRoleV1> {
    match role {
        "image" => Some(EditInputRoleV1::Image),
        "mask" => Some(EditInputRoleV1::Mask),
        _ => None,
    }
}

fn valid_input_shape(role: EditInputRoleV1, index: u16, media_type: &str) -> bool {
    match role {
        EditInputRoleV1::Image => {
            index < 16 && matches!(media_type, "image/png" | "image/jpeg" | "image/webp")
        }
        EditInputRoleV1::Mask => index == 0 && media_type == "image/png",
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn invalid(reservation: &UsageReservation) -> ExecutionContextError {
    ExecutionContextError::Invalid {
        reservation: reservation.clone(),
    }
}
