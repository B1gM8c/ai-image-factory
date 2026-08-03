use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::admission::{
    AdmissionError, AttachJob, CustomerPricingIntent, EDIT_COMMAND_SCHEMA,
    EDIT_COMMAND_SCHEMA_VERSION, EDIT_OPERATION, EditCommandV1, GENERATION_COMMAND_SCHEMA,
    GENERATION_COMMAND_SCHEMA_VERSION, GENERATION_OPERATION, GenerationCommandV1,
    VIDEO_GENERATION_OPERATION, provider_command_hash,
};
use image_api_contracts::{
    ark::{ARK_CONTENT_GENERATION_API_PROFILE, ARK_IMAGES_API_PROFILE},
    dreamina::{DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE},
    xai::{XAI_IMAGES_API_PROFILE, XAI_VIDEOS_API_PROFILE, XaiVideoAspectRatio},
};
use image_provider_contracts::provider::openai_codex::{
    MODEL_GPT_IMAGE_2 as CODEX_PRICING_MODEL_ID,
    MODEL_GPT_IMAGE_2_SNAPSHOT as CODEX_SNAPSHOT_MODEL_ID, PROVIDER_ID as CODEX_PROVIDER_ID,
};
use image_provider_dreamina_cli::{
    DREAMINA_SUBMIT_COMMAND_SCHEMA, DreaminaSubmitRequestV1, PROVIDER_ID as DREAMINA_PROVIDER_ID,
    parse_submit_command,
};
use image_provider_grok_cli::{
    GROK_IMAGE_EDIT_COMMAND_SCHEMA, GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
    GROK_VIDEO_GENERATION_COMMAND_SCHEMA, GrokVideoGenerationRequestV1,
    PROVIDER_ID as GROK_PROVIDER_ID, parse_image_edit_payload, parse_image_generation_payload,
    parse_video_generation_payload,
};

use super::{
    PriceBookVersionView, PriceResolutionError, PriceResolutionRequest, QuoteError, QuoteQuantity,
    QuoteRateAdjustment, ResolvedPriceVersion,
    official_metering::gpt_image_2_output_tokens_from_dimensions,
    plan_customer_quote_with_adjustment,
    postgres::resolve_price_version_in_transaction,
    postgres_quote::{CustomerQuoteContext, CustomerQuoteStoreError, persist_customer_quote},
    rating::confidence_satisfies,
    surface_contract::{
        DimensionValue, PricingSurfaceContract, SurfaceRequest, find_contract_for_pricing,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CustomerMeteringContract {
    Exact,
    Incompatible,
}

pub(super) fn pricing_operation_for_route(
    provider_id: &str,
    route_operation: &str,
    command_schema: &str,
    media_kind: &str,
) -> Option<&'static str> {
    match (provider_id, route_operation, command_schema, media_kind) {
        (CODEX_PROVIDER_ID, "images.generations", GENERATION_COMMAND_SCHEMA, "image")
        | (GROK_PROVIDER_ID, "images.generations", GROK_IMAGE_GENERATION_COMMAND_SCHEMA, "image")
        | (DREAMINA_PROVIDER_ID, "images.generations", DREAMINA_SUBMIT_COMMAND_SCHEMA, "image") => {
            Some(GENERATION_OPERATION)
        }
        (CODEX_PROVIDER_ID, "images.edits", EDIT_COMMAND_SCHEMA, "image") => Some(EDIT_OPERATION),
        (GROK_PROVIDER_ID, "images.edits", GROK_IMAGE_EDIT_COMMAND_SCHEMA, "image") => {
            Some(EDIT_OPERATION)
        }
        (DREAMINA_PROVIDER_ID, "videos.generations", DREAMINA_SUBMIT_COMMAND_SCHEMA, "video") => {
            Some(VIDEO_GENERATION_OPERATION)
        }
        (GROK_PROVIDER_ID, "videos.generations", GROK_VIDEO_GENERATION_COMMAND_SCHEMA, "video") => {
            Some(VIDEO_GENERATION_OPERATION)
        }
        _ => None,
    }
}

pub(super) fn pricing_dimension_keys_for_route(
    provider_id: &str,
    route_operation: &str,
    command_schema: &str,
    media_kind: &str,
) -> Option<&'static [&'static str]> {
    match (provider_id, route_operation, command_schema, media_kind) {
        (CODEX_PROVIDER_ID, "images.generations", GENERATION_COMMAND_SCHEMA, "image")
        | (CODEX_PROVIDER_ID, "images.edits", EDIT_COMMAND_SCHEMA, "image") => {
            Some(&["quality", "size"])
        }
        (GROK_PROVIDER_ID, "images.generations", GROK_IMAGE_GENERATION_COMMAND_SCHEMA, "image") => {
            Some(&["aspect_ratio", "resolution"])
        }
        (GROK_PROVIDER_ID, "images.edits", GROK_IMAGE_EDIT_COMMAND_SCHEMA, "image") => {
            Some(&["aspect_ratio", "resolution"])
        }
        (DREAMINA_PROVIDER_ID, "images.generations", DREAMINA_SUBMIT_COMMAND_SCHEMA, "image") => {
            Some(&["height", "ratio", "resolution_type", "width"])
        }
        (DREAMINA_PROVIDER_ID, "videos.generations", DREAMINA_SUBMIT_COMMAND_SCHEMA, "video") => {
            Some(&["duration", "ratio", "resolution"])
        }
        (GROK_PROVIDER_ID, "videos.generations", GROK_VIDEO_GENERATION_COMMAND_SCHEMA, "video") => {
            Some(&[
                "aspect_ratio",
                "duration",
                "input_image_count",
                "resolution",
            ])
        }
        _ => None,
    }
}

pub(super) fn customer_metering_contract(
    version: &PriceBookVersionView,
    _provider_id: &str,
) -> CustomerMeteringContract {
    if version.components.is_empty() {
        return CustomerMeteringContract::Incompatible;
    }
    let outcomes = version
        .components
        .iter()
        .map(|component| component.outcome.as_str())
        .collect::<BTreeSet<_>>();
    if !outcomes.contains("any")
        && !["succeeded", "failed", "no_effect"]
            .iter()
            .all(|outcome| outcomes.contains(outcome))
    {
        return CustomerMeteringContract::Incompatible;
    }

    for component in &version.components {
        let actual_confidence = match (
            component.metric.as_str(),
            component.unit.as_str(),
            component.quantity_source.as_str(),
        ) {
            ("image_output", "image", "request_derived")
            | ("image_input", "image", "request_derived")
            | ("video_requested_second", "second", "request_derived")
            | ("video_output_second", "second", "request_derived") => "exact",
            _ => return CustomerMeteringContract::Incompatible,
        };
        if !confidence_satisfies(actual_confidence, &component.required_confidence) {
            return CustomerMeteringContract::Incompatible;
        }
    }
    CustomerMeteringContract::Exact
}

#[derive(sqlx::FromRow)]
struct LockedPricingJob {
    tenant_id: String,
    project_id: String,
    admitted_at_ms: i64,
    operation: String,
    provider_id: String,
    model: String,
    output_count: i32,
    billable_units: i32,
    billing_metric: String,
    billing_unit: String,
    economics_contract_version: i16,
}

struct CommandPricingFacts {
    provider_id: String,
    provider_model_id: String,
    command_execution_model_id: Option<String>,
    api_profile: String,
    operation: String,
    output_count: i32,
    billable_units: i32,
    output_billable_units: i32,
    billing_metric: String,
    billing_unit: String,
    media_kind: String,
    pricing_dimensions: BTreeMap<String, String>,
}

pub(crate) async fn admit_customer_pricing_v4(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    api_profile: &str,
) -> Result<(), AdmissionError> {
    let intent = pricing_intent(request)?;
    let command_facts = command_pricing_facts(request, api_profile)?;
    let tenant_id: String = sqlx::query_scalar("SELECT tenant_id FROM jobs WHERE job_id = $1")
        .bind(request.job_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unavailable)?
        .ok_or(AdmissionError::InvalidOwner)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("budget:{tenant_id}:{}", intent.currency))
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;

    let job = lock_pricing_job(tx, request.job_id)
        .await
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                job.id = %request.job_id,
                "customer pricing job lock failed"
            );
        })?;
    validate_job(&job, intent, api_profile, &command_facts)?;
    validate_model_mapping(tx, request, intent, api_profile).await?;
    persist_and_validate_service_tier_decision(tx, request.job_id, intent, job.admitted_at_ms)
        .await
        .inspect_err(|error| {
            tracing::warn!(
                ?error,
                job.id = %request.job_id,
                "customer pricing service tier binding failed"
            );
        })?;
    if job.economics_contract_version == 4 {
        return validate_customer_pricing_v4_with_facts(
            tx,
            request,
            intent,
            api_profile,
            &command_facts,
        )
        .await;
    }
    if job.economics_contract_version != 1 {
        return Err(AdmissionError::InvalidOwner);
    }
    let legacy_state: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
          (SELECT COUNT(*) FROM job_outputs WHERE job_id = $1),
          (SELECT COUNT(*) FROM price_quotes WHERE job_id = $1),
          (SELECT COUNT(*) FROM output_holds WHERE job_id = $1),
          (SELECT COUNT(*) FROM customer_price_quotes WHERE job_id = $1)
        "#,
    )
    .bind(request.job_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if legacy_state != (0, 0, 0, 0) {
        return Err(AdmissionError::InvalidOwner);
    }

    let mut output_ids = Vec::with_capacity(job.output_count as usize);
    for output_index in 0..job.output_count {
        let output_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO job_outputs (
                output_id, job_id, output_index, billable_units,
                state, created_at_ms, updated_at_ms
            )
            VALUES ($1, $2, $3, $4, 'pending', $5, $5)
            "#,
        )
        .bind(output_id)
        .bind(request.job_id)
        .bind(output_index)
        .bind(command_facts.output_billable_units)
        .bind(job.admitted_at_ms)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
        output_ids.push(output_id);
    }
    let changed = sqlx::query(
        r#"
        UPDATE jobs
        SET economics_contract_version = 4, updated_at_ms = $2
        WHERE job_id = $1 AND economics_contract_version = 1
        "#,
    )
    .bind(request.job_id)
    .bind(job.admitted_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if changed != 1 {
        return Err(AdmissionError::InvalidOwner);
    }

    let price_public_model_id = customer_price_public_model_id(&job.provider_id, intent).to_owned();
    let resolution = PriceResolutionRequest {
        purpose: "customer_sale".to_string(),
        organization_id: Some(job.tenant_id.clone()),
        project_id: Some(job.project_id.clone()),
        provider_id: Some(job.provider_id.clone()),
        currency: intent.currency.clone(),
        api_profile: api_profile.to_string(),
        operation: job.operation.clone(),
        provider_model_id: Some(intent.provider_model_id.clone()),
        public_model_id: price_public_model_id.clone(),
        media_kind: intent.media_kind.clone(),
        service_tier: intent.service_tier.clone(),
        execution_surface: intent.execution_surface.clone(),
        billing_mode: "customer_rate".to_string(),
        at_ms: job.admitted_at_ms,
    };
    let resolved = resolve_price_version_in_transaction(tx, &resolution)
        .await
        .map_err(map_resolution)?;
    if customer_metering_contract(&resolved.version, &job.provider_id)
        != CustomerMeteringContract::Exact
    {
        return Err(AdmissionError::PricingUnavailable);
    }
    let contract = find_contract_for_pricing(
        &command_facts.provider_id,
        &command_facts.operation,
        &request.command_schema,
        &command_facts.media_kind,
    )
    .ok_or(AdmissionError::PricingUnavailable)?;
    if !customer_metering_bases_complete(&resolved.version, contract) {
        return Err(AdmissionError::PricingUnavailable);
    }
    let pricing_dimensions = serde_json::to_value(&intent.pricing_dimensions)
        .map_err(|_| AdmissionError::PricingUnavailable)?;
    let quantities = quote_quantities(&resolved, &output_ids, &pricing_dimensions)?;
    let adjustment = match intent.processing_mode {
        crate::admission::PricingProcessingMode::Synchronous => None,
        crate::admission::PricingProcessingMode::Batch => {
            Some(QuoteRateAdjustment::BATCH_FIFTY_PERCENT)
        }
    };
    let plan = plan_customer_quote_with_adjustment(&resolved, &quantities, adjustment)
        .map_err(map_quote)?;
    let outcomes = plan
        .lines
        .iter()
        .map(|line| line.terminal_outcome.as_str())
        .collect::<BTreeSet<_>>();
    if outcomes != BTreeSet::from(["failed", "no_effect", "succeeded"]) {
        return Err(AdmissionError::PricingUnavailable);
    }
    persist_customer_quote(
        tx,
        &CustomerQuoteContext {
            job_id: request.job_id,
            tenant_id: job.tenant_id,
            project_id: job.project_id,
            api_profile: api_profile.to_string(),
            operation: job.operation,
            provider_id: Some(job.provider_id),
            provider_model_id: Some(intent.provider_model_id.clone()),
            public_model_id: price_public_model_id,
            media_kind: intent.media_kind.clone(),
            service_tier: intent.service_tier.clone(),
            execution_surface: intent.execution_surface.clone(),
            request_dimensions: pricing_request_dimensions(intent)?,
        },
        &resolved,
        &plan,
    )
    .await
    .map_err(|error| {
        tracing::warn!(
            ?error,
            job.id = %request.job_id,
            "customer quote persistence failed"
        );
        map_store(error)
    })?;
    Ok(())
}

fn quote_quantities(
    resolved: &ResolvedPriceVersion,
    output_ids: &[Uuid],
    dimensions: &serde_json::Value,
) -> Result<Vec<QuoteQuantity>, AdmissionError> {
    let provider_id = resolved
        .provider_id
        .as_deref()
        .or(resolved.version.provider_id.as_deref());
    let provider_model_id = resolved.version.provider_model_id.as_deref();
    let mut bases = BTreeSet::new();
    for component in &resolved.version.components {
        let basis = match (
            component.metric.as_str(),
            component.unit.as_str(),
            component.quantity_source.as_str(),
        ) {
            ("image_output", "image", "request_derived") => {
                ("image_output", "image", 1_u64, "request_derived", "exact")
            }
            ("image_input", "image", "request_derived") => (
                "image_input",
                "image",
                input_image_count(dimensions)?,
                "request_derived",
                "exact",
            ),
            ("image_output_token", "token", "official_lookup")
                if provider_id == Some("openai-codex")
                    && provider_model_id == Some("gpt-image-2") =>
            {
                (
                    "image_output_token",
                    "token",
                    gpt_image_2_output_tokens_from_dimensions(dimensions)
                        .map_err(|_| AdmissionError::PricingUnavailable)?,
                    "official_lookup",
                    "estimated",
                )
            }
            ("video_requested_second", "second", "request_derived") => (
                "video_requested_second",
                "second",
                video_duration_seconds(dimensions)?,
                "request_derived",
                "exact",
            ),
            // Immutable versions published before migration 0068 used this legacy name.
            ("video_output_second", "second", "request_derived") => (
                "video_output_second",
                "second",
                video_duration_seconds(dimensions)?,
                "request_derived",
                "exact",
            ),
            _ => return Err(AdmissionError::PricingUnavailable),
        };
        if !confidence_satisfies(basis.4, &component.required_confidence) {
            return Err(AdmissionError::PricingUnavailable);
        }
        bases.insert(basis);
    }
    if bases.is_empty() {
        return Err(AdmissionError::PricingUnavailable);
    }

    Ok(output_ids
        .iter()
        .flat_map(|output_id| {
            bases.iter().map(
                move |(metric, unit, quantity, source, confidence)| QuoteQuantity {
                    partition_key: format!("output:{output_id}"),
                    metric: (*metric).to_string(),
                    unit: (*unit).to_string(),
                    max_quantity: quantity.to_string(),
                    reservation_quantity_source: (*source).to_string(),
                    reservation_confidence: (*confidence).to_string(),
                    settlement_quantity_source: (*source).to_string(),
                    settlement_confidence: (*confidence).to_string(),
                    dimensions: dimensions.clone(),
                },
            )
        })
        .collect())
}

fn video_duration_seconds(dimensions: &serde_json::Value) -> Result<u64, AdmissionError> {
    dimensions
        .get("duration")
        .and_then(serde_json::Value::as_str)
        .and_then(|duration| duration.parse::<u64>().ok())
        .filter(|duration| (4..=15).contains(duration))
        .ok_or(AdmissionError::PricingUnavailable)
}

fn input_image_count(dimensions: &serde_json::Value) -> Result<u64, AdmissionError> {
    dimensions
        .get("input_image_count")
        .and_then(serde_json::Value::as_str)
        .and_then(|count| count.parse::<u64>().ok())
        .filter(|count| (1..=7).contains(count))
        .ok_or(AdmissionError::PricingUnavailable)
}

fn customer_metering_bases_complete(
    version: &PriceBookVersionView,
    contract: &PricingSurfaceContract,
) -> bool {
    contract
        .metering_bases
        .iter()
        .filter(|basis| basis.customer_sale_required)
        .all(|basis| {
            version.components.iter().any(|component| {
                component.metric == basis.metric
                    && component.unit == basis.unit
                    && component.quantity_source == basis.quantity_source
            })
        })
}

pub(crate) async fn validate_customer_pricing_v4(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    api_profile: &str,
) -> Result<(), AdmissionError> {
    let intent = pricing_intent(request)?;
    let command_facts = command_pricing_facts(request, api_profile)?;
    validate_pricing_dimensions(intent, &command_facts)?;
    validate_model_mapping(tx, request, intent, api_profile).await?;
    validate_existing_service_tier_decision(tx, request.job_id, intent).await?;
    validate_customer_pricing_v4_with_facts(tx, request, intent, api_profile, &command_facts).await
}

async fn persist_and_validate_service_tier_decision(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    intent: &CustomerPricingIntent,
    created_at_ms: i64,
) -> Result<(), AdmissionError> {
    let decision = &intent.service_tier_decision;
    sqlx::query(
        r#"
        INSERT INTO job_service_tier_decisions (
            job_id, requested_service_tier, project_service_tier,
            effective_service_tier, fallback_reason, created_at_ms
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (job_id) DO NOTHING
        "#,
    )
    .bind(job_id)
    .bind(decision.requested.as_str())
    .bind(decision.project_default.as_str())
    .bind(decision.effective.as_str())
    .bind(decision.fallback_reason)
    .bind(created_at_ms)
    .execute(&mut **tx)
    .await
    .map_err(unavailable)?;
    validate_existing_service_tier_decision(tx, job_id, intent).await
}

async fn validate_existing_service_tier_decision(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    intent: &CustomerPricingIntent,
) -> Result<(), AdmissionError> {
    let decision = &intent.service_tier_decision;
    let valid: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM job_service_tier_decisions
            WHERE job_id = $1
              AND requested_service_tier = $2
              AND project_service_tier = $3
              AND effective_service_tier = $4
              AND fallback_reason IS NOT DISTINCT FROM $5::TEXT
        )
        "#,
    )
    .bind(job_id)
    .bind(decision.requested.as_str())
    .bind(decision.project_default.as_str())
    .bind(decision.effective.as_str())
    .bind(decision.fallback_reason)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if valid {
        Ok(())
    } else {
        Err(AdmissionError::InvalidOwner)
    }
}

async fn validate_customer_pricing_v4_with_facts(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    intent: &CustomerPricingIntent,
    api_profile: &str,
    command_facts: &CommandPricingFacts,
) -> Result<(), AdmissionError> {
    validate_command_pricing_identity(intent, api_profile, command_facts)?;
    let request_dimensions = pricing_request_dimensions(intent)?;
    let valid: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM jobs job
          JOIN job_auth_attributions attribution
            ON attribution.job_id = job.job_id
           AND attribution.tenant_id = job.tenant_id
          JOIN customer_price_quotes quote
            ON quote.job_id = job.job_id
           AND quote.tenant_id = job.tenant_id
           AND quote.project_id = attribution.project_id
           AND quote.created_at_ms = attribution.admitted_at_ms
          JOIN customer_billing_holds hold
            ON hold.quote_id = quote.quote_id
           AND hold.job_id = quote.job_id
          WHERE job.job_id = $1
            AND job.economics_contract_version = 4
            AND job.output_count = $9
            AND job.billable_units = $15
            AND job.provider_id = $10
            AND job.model = $14
            AND job.operation = $12
            AND job.billing_metric = $16
            AND job.billing_unit = $17
            AND quote.api_profile = $2
            AND quote.operation = job.operation
            AND quote.provider_id = job.provider_id
            AND quote.provider_model_id = $3
            AND quote.public_model_id = $4
            AND quote.media_kind = $5
            AND quote.service_tier = $6
            AND quote.execution_surface = $7
            AND quote.currency = $8
            AND quote.request_dimensions_json = $13::JSONB
            AND hold.state = 'held'
            AND (
              SELECT COUNT(*) FROM job_outputs WHERE job_id = job.job_id
            ) = $9
            AND NOT EXISTS (
              SELECT 1
              FROM job_outputs output
              CROSS JOIN (
                VALUES ('succeeded'), ('failed'), ('no_effect')
              ) AS expected(outcome)
              WHERE output.job_id = job.job_id
                AND (
                  output.billable_units <> $18
                  OR NOT EXISTS (
                  SELECT 1
                  FROM customer_price_quote_lines line
                  WHERE line.quote_id = quote.quote_id
                    AND line.job_id = quote.job_id
                    AND line.partition_key = 'output:' || output.output_id::TEXT
                    AND line.terminal_outcome = expected.outcome
                  )
                )
            )
            AND NOT EXISTS (SELECT 1 FROM price_quotes WHERE job_id = job.job_id)
            AND NOT EXISTS (SELECT 1 FROM output_holds WHERE job_id = job.job_id)
        )
        "#,
    )
    .bind(request.job_id)
    .bind(api_profile)
    .bind(&intent.provider_model_id)
    .bind(customer_price_public_model_id(
        &command_facts.provider_id,
        intent,
    ))
    .bind(&intent.media_kind)
    .bind(&intent.service_tier)
    .bind(&intent.execution_surface)
    .bind(&intent.currency)
    .bind(command_facts.output_count)
    .bind(&command_facts.provider_id)
    .bind(&command_facts.provider_model_id)
    .bind(&command_facts.operation)
    .bind(request_dimensions)
    .bind(&intent.execution_model_id)
    .bind(command_facts.billable_units)
    .bind(&command_facts.billing_metric)
    .bind(&command_facts.billing_unit)
    .bind(command_facts.output_billable_units)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if valid {
        Ok(())
    } else {
        Err(AdmissionError::InvalidOwner)
    }
}

async fn lock_pricing_job(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
) -> Result<LockedPricingJob, AdmissionError> {
    sqlx::query_as(
        r#"
        SELECT job.tenant_id, attribution.project_id,
               attribution.admitted_at_ms, job.operation,
               job.provider_id, job.model, job.output_count,
               job.billable_units, job.billing_metric,
               job.billing_unit, job.economics_contract_version
        FROM jobs job
        JOIN job_auth_attributions attribution
          ON attribution.job_id = job.job_id
         AND attribution.tenant_id = job.tenant_id
        WHERE job.job_id = $1
          AND attribution.project_id IS NOT NULL
          AND job.state IN ('reserved', 'queued', 'running')
        FOR UPDATE OF job
        "#,
    )
    .bind(job_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unavailable)?
    .ok_or(AdmissionError::InvalidOwner)
}

fn validate_job(
    job: &LockedPricingJob,
    intent: &CustomerPricingIntent,
    api_profile: &str,
    command_facts: &CommandPricingFacts,
) -> Result<(), AdmissionError> {
    validate_pricing_dimensions(intent, command_facts)?;
    validate_command_pricing_identity(intent, api_profile, command_facts)?;
    if job.output_count <= 0
        || job.output_count > 10
        || job.billable_units != command_facts.billable_units
        || job.billing_metric != command_facts.billing_metric
        || job.billing_unit != command_facts.billing_unit
        || job.provider_id != command_facts.provider_id
        || job.model != intent.execution_model_id
        || job.operation != command_facts.operation
        || command_facts.output_count != job.output_count
        || api_profile.trim().is_empty()
        || intent.public_model_id.trim().is_empty()
        || intent.provider_model_id.trim().is_empty()
        || intent.execution_model_id.trim().is_empty()
        || intent.media_kind != command_facts.media_kind
        || intent.service_tier != "standard"
        || intent.service_tier != intent.service_tier_decision.effective.pricing_key()
        || intent.execution_surface != "provider_cli"
        || intent.currency != "USD"
        || intent.pricing_dimensions.is_empty()
        || intent.pricing_dimensions.len() > 16
        || intent.pricing_dimensions.iter().any(|(key, value)| {
            key.is_empty()
                || key.len() > 64
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || value.is_empty()
                || value.len() > 128
                || value.chars().any(char::is_control)
        })
    {
        return Err(AdmissionError::PricingUnavailable);
    }
    Ok(())
}

fn validate_pricing_dimensions(
    intent: &CustomerPricingIntent,
    command_facts: &CommandPricingFacts,
) -> Result<(), AdmissionError> {
    if intent.pricing_dimensions != command_facts.pricing_dimensions {
        return Err(AdmissionError::PricingUnavailable);
    }
    Ok(())
}

fn pricing_request_dimensions(
    intent: &CustomerPricingIntent,
) -> Result<serde_json::Value, AdmissionError> {
    let mut dimensions = serde_json::to_value(&intent.pricing_dimensions)
        .map_err(|_| AdmissionError::PricingUnavailable)?;
    dimensions
        .as_object_mut()
        .ok_or(AdmissionError::PricingUnavailable)?
        .insert(
            "processing_mode".to_string(),
            serde_json::Value::String(intent.processing_mode.as_str().to_string()),
        );
    Ok(dimensions)
}

fn validate_command_pricing_identity(
    intent: &CustomerPricingIntent,
    api_profile: &str,
    command_facts: &CommandPricingFacts,
) -> Result<(), AdmissionError> {
    if command_facts.provider_model_id != intent.provider_model_id
        || command_facts
            .command_execution_model_id
            .as_deref()
            .is_some_and(|model| model != intent.execution_model_id)
        || command_facts.api_profile != api_profile
        || command_facts.operation.trim().is_empty()
    {
        return Err(AdmissionError::PricingUnavailable);
    }
    Ok(())
}

fn customer_price_public_model_id<'a>(
    provider_id: &str,
    intent: &'a CustomerPricingIntent,
) -> &'a str {
    if provider_id == CODEX_PROVIDER_ID
        && intent.provider_model_id == CODEX_PRICING_MODEL_ID
        && intent.execution_model_id == CODEX_SNAPSHOT_MODEL_ID
    {
        CODEX_PRICING_MODEL_ID
    } else {
        &intent.public_model_id
    }
}

fn command_pricing_facts(
    request: &AttachJob,
    session_api_profile: &str,
) -> Result<CommandPricingFacts, AdmissionError> {
    let provider_command_hash = provider_command_hash(request)?;
    let (
        provider_id,
        provider_model_id,
        api_profile,
        operation,
        count,
        billable_units,
        output_billable_units,
        billing_metric,
        billing_unit,
        media_kind,
        pricing_dimensions,
    ) = match request.command_schema.as_str() {
        EDIT_COMMAND_SCHEMA => {
            let command: EditCommandV1 = serde_json::from_value(request.command_json.clone())
                .map_err(|_| AdmissionError::InvalidCommand)?;
            if command.schema_version != EDIT_COMMAND_SCHEMA_VERSION
                || command.operation != EDIT_OPERATION
                || command.provider_id != CODEX_PROVIDER_ID
                || command.model.trim().is_empty()
                || command.source_api_profile.trim().is_empty()
                || command.request_hash_hex() != provider_command_hash
            {
                return Err(AdmissionError::InvalidCommand);
            }
            (
                command.provider_id,
                command.model,
                command.source_api_profile,
                command.operation,
                command.n,
                command.n,
                1,
                "output".to_string(),
                "output".to_string(),
                "image".to_string(),
                BTreeMap::from([
                    ("quality".to_string(), command.quality),
                    ("size".to_string(), command.size),
                ]),
            )
        }
        GENERATION_COMMAND_SCHEMA => {
            let command: GenerationCommandV1 = serde_json::from_value(request.command_json.clone())
                .map_err(|_| AdmissionError::InvalidCommand)?;
            if command.schema_version != GENERATION_COMMAND_SCHEMA_VERSION
                || command.operation != GENERATION_OPERATION
                || command.provider_id != CODEX_PROVIDER_ID
                || command.model.trim().is_empty()
                || command.source_api_profile.trim().is_empty()
                || command.request_hash_hex() != provider_command_hash
            {
                return Err(AdmissionError::InvalidCommand);
            }
            (
                command.provider_id,
                command.model,
                command.source_api_profile,
                command.operation,
                command.n,
                command.n,
                1,
                "output".to_string(),
                "output".to_string(),
                "image".to_string(),
                BTreeMap::from([
                    ("quality".to_string(), command.quality),
                    ("size".to_string(), command.size),
                ]),
            )
        }
        GROK_IMAGE_GENERATION_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            let payload = parse_image_generation_payload(&bytes)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            if payload.source_command_sha256() != provider_command_hash {
                return Err(AdmissionError::InvalidCommand);
            }
            let command = payload.source_command();
            let provider_model_id = command
                .model
                .as_deref()
                .filter(|model| !model.trim().is_empty())
                .ok_or(AdmissionError::InvalidCommand)?;
            (
                GROK_PROVIDER_ID.to_string(),
                provider_model_id.to_string(),
                XAI_IMAGES_API_PROFILE.to_string(),
                GENERATION_OPERATION.to_string(),
                command.n,
                command.n,
                1,
                "output".to_string(),
                "output".to_string(),
                "image".to_string(),
                BTreeMap::from([
                    (
                        "aspect_ratio".to_string(),
                        serialized_string_dimension(command.aspect_ratio)?,
                    ),
                    (
                        "resolution".to_string(),
                        serialized_string_dimension(command.resolution)?,
                    ),
                ]),
            )
        }
        GROK_IMAGE_EDIT_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            let payload =
                parse_image_edit_payload(&bytes).map_err(|_| AdmissionError::InvalidCommand)?;
            if payload.source_command_sha256() != provider_command_hash {
                return Err(AdmissionError::InvalidCommand);
            }
            (
                GROK_PROVIDER_ID.to_string(),
                "grok-imagine-image-quality".to_string(),
                XAI_IMAGES_API_PROFILE.to_string(),
                EDIT_OPERATION.to_string(),
                1,
                1,
                1,
                "output".to_string(),
                "output".to_string(),
                "image".to_string(),
                BTreeMap::from([
                    (
                        "aspect_ratio".to_string(),
                        payload.request().aspect_ratio().as_str().to_string(),
                    ),
                    ("resolution".to_string(), "1k".to_string()),
                ]),
            )
        }
        GROK_VIDEO_GENERATION_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            let payload = parse_video_generation_payload(&bytes)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            if payload.source_command_sha256() != provider_command_hash {
                return Err(AdmissionError::InvalidCommand);
            }
            let command = payload.source_command();
            if command.schema_version != 1
                || command.operation != "videos.generations"
                || command.model.as_deref().is_none_or(str::is_empty)
            {
                return Err(AdmissionError::InvalidCommand);
            }
            let (provider_model_id, input_image_count) = match payload.request() {
                GrokVideoGenerationRequestV1::ImageToVideo(_) => {
                    ("grok-imagine-video-1.5-preview", 1_usize)
                }
                GrokVideoGenerationRequestV1::ReferenceToVideo(_) => {
                    ("grok-imagine-video", command.reference_images.len())
                }
            };
            let duration = u32::from(command.duration);
            let mut dimensions = BTreeMap::from([
                ("duration".to_owned(), duration.to_string()),
                (
                    "input_image_count".to_owned(),
                    input_image_count.to_string(),
                ),
                (
                    "resolution".to_owned(),
                    serialized_string_dimension(command.resolution)?,
                ),
            ]);
            if provider_model_id == "grok-imagine-video" {
                dimensions.insert(
                    "aspect_ratio".to_owned(),
                    serialized_string_dimension(
                        command.aspect_ratio.unwrap_or(XaiVideoAspectRatio::R16x9),
                    )?,
                );
            }
            (
                GROK_PROVIDER_ID.to_owned(),
                provider_model_id.to_owned(),
                XAI_VIDEOS_API_PROFILE.to_owned(),
                VIDEO_GENERATION_OPERATION.to_owned(),
                1,
                duration,
                duration,
                "video_second".to_owned(),
                "second".to_owned(),
                "video".to_owned(),
                dimensions,
            )
        }
        DREAMINA_SUBMIT_COMMAND_SCHEMA => {
            let bytes = serde_json::to_vec(&request.command_json)
                .map_err(|_| AdmissionError::InvalidCommand)?;
            if hex::encode(Sha256::digest(&bytes)) != provider_command_hash {
                return Err(AdmissionError::InvalidCommand);
            }
            match parse_submit_command(&bytes).map_err(|_| AdmissionError::InvalidCommand)? {
                DreaminaSubmitRequestV1::TextToImage(command)
                    if matches!(
                        session_api_profile,
                        DREAMINA_IMAGES_API_PROFILE | ARK_IMAGES_API_PROFILE
                    ) =>
                {
                    let provider_model_id = command.model().as_str().to_string();
                    let mut dimensions = BTreeMap::from([(
                        "resolution_type".to_string(),
                        command.resolution().as_str().to_string(),
                    )]);
                    if let Some(ratio) = command.ratio() {
                        dimensions.insert("ratio".to_string(), ratio.as_str().to_string());
                    } else {
                        dimensions.insert(
                            "width".to_string(),
                            command
                                .width()
                                .ok_or(AdmissionError::InvalidCommand)?
                                .to_string(),
                        );
                        dimensions.insert(
                            "height".to_string(),
                            command
                                .height()
                                .ok_or(AdmissionError::InvalidCommand)?
                                .to_string(),
                        );
                    }
                    let count = u32::from(command.generate_num());
                    (
                        DREAMINA_PROVIDER_ID.to_string(),
                        provider_model_id,
                        session_api_profile.to_string(),
                        GENERATION_OPERATION.to_string(),
                        count,
                        count,
                        1,
                        "output".to_string(),
                        "output".to_string(),
                        "image".to_string(),
                        dimensions,
                    )
                }
                DreaminaSubmitRequestV1::TextToVideo(command)
                    if matches!(
                        session_api_profile,
                        DREAMINA_VIDEOS_API_PROFILE | ARK_CONTENT_GENERATION_API_PROFILE
                    ) =>
                {
                    let provider_model_id = command.model().as_str().to_string();
                    let duration = u32::from(command.duration_seconds());
                    (
                        DREAMINA_PROVIDER_ID.to_string(),
                        provider_model_id,
                        session_api_profile.to_string(),
                        VIDEO_GENERATION_OPERATION.to_string(),
                        1,
                        duration,
                        duration,
                        "video_second".to_string(),
                        "second".to_string(),
                        "video".to_string(),
                        BTreeMap::from([
                            ("duration".to_string(), duration.to_string()),
                            ("ratio".to_string(), command.ratio().as_str().to_string()),
                            (
                                "resolution".to_string(),
                                command.resolution().as_str().to_string(),
                            ),
                        ]),
                    )
                }
                _ => return Err(AdmissionError::PricingUnavailable),
            }
        }
        _ => return Err(AdmissionError::PricingUnavailable),
    };
    let output_count = i32::try_from(count)
        .ok()
        .filter(|count| (1..=10).contains(count))
        .ok_or(AdmissionError::InvalidCommand)?;
    let billable_units =
        i32::try_from(billable_units).map_err(|_| AdmissionError::InvalidCommand)?;
    let output_billable_units =
        i32::try_from(output_billable_units).map_err(|_| AdmissionError::InvalidCommand)?;
    if billable_units <= 0
        || output_billable_units <= 0
        || billable_units != output_count * output_billable_units
    {
        return Err(AdmissionError::InvalidCommand);
    }
    let command_execution_model_id =
        (provider_id == CODEX_PROVIDER_ID).then(|| provider_model_id.clone());
    let provider_model_id =
        if provider_id == CODEX_PROVIDER_ID && provider_model_id == CODEX_SNAPSHOT_MODEL_ID {
            CODEX_PRICING_MODEL_ID.to_owned()
        } else {
            provider_model_id
        };
    let facts = CommandPricingFacts {
        provider_id,
        provider_model_id,
        command_execution_model_id,
        api_profile,
        operation,
        output_count,
        billable_units,
        output_billable_units,
        billing_metric,
        billing_unit,
        media_kind,
        pricing_dimensions,
    };
    let contract = find_contract_for_pricing(
        &facts.provider_id,
        &facts.operation,
        &request.command_schema,
        &facts.media_kind,
    )
    .ok_or(AdmissionError::PricingUnavailable)?;
    if !contract.api_profiles.contains(&facts.api_profile.as_str()) {
        return Err(AdmissionError::PricingUnavailable);
    }
    let dimensions = facts
        .pricing_dimensions
        .iter()
        .map(|(key, value)| DimensionValue { key, value })
        .collect::<Vec<_>>();
    contract
        .validate(&SurfaceRequest {
            provider_model_id: &facts.provider_model_id,
            dimensions: &dimensions,
            output_count: u32::try_from(facts.output_count)
                .map_err(|_| AdmissionError::InvalidCommand)?,
        })
        .map_err(|_| AdmissionError::PricingUnavailable)?;
    Ok(facts)
}

fn serialized_string_dimension(value: impl serde::Serialize) -> Result<String, AdmissionError> {
    serde_json::to_value(value)
        .map_err(|_| AdmissionError::InvalidCommand)?
        .as_str()
        .map(str::to_owned)
        .ok_or(AdmissionError::InvalidCommand)
}

async fn validate_model_mapping(
    tx: &mut Transaction<'_, Postgres>,
    request: &AttachJob,
    intent: &CustomerPricingIntent,
    api_profile: &str,
) -> Result<(), AdmissionError> {
    let valid: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM jobs job
          JOIN job_provider_route_attributions attribution
            ON attribution.job_id = job.job_id
           AND attribution.tenant_id = job.tenant_id
           AND attribution.provider_id = job.provider_id
           AND attribution.command_schema = $2
          JOIN provider_route_model_mappings mapping
            ON mapping.route_id = attribution.route_id
           AND mapping.route_revision = attribution.route_revision
           AND mapping.provider_id = attribution.provider_id
           AND mapping.operation_id = attribution.operation_id
           AND mapping.command_schema = attribution.command_schema
          WHERE job.job_id = $1
            AND mapping.api_profile = $3
            AND mapping.public_model_id = $4
            AND mapping.provider_model_id = $5
            AND mapping.execution_model_id = $6
            AND mapping.media_kind = $7
        )
        "#,
    )
    .bind(request.job_id)
    .bind(&request.command_schema)
    .bind(api_profile)
    .bind(&intent.public_model_id)
    .bind(&intent.provider_model_id)
    .bind(&intent.execution_model_id)
    .bind(&intent.media_kind)
    .fetch_one(&mut **tx)
    .await
    .map_err(unavailable)?;
    if valid {
        Ok(())
    } else {
        Err(AdmissionError::PricingUnavailable)
    }
}

fn pricing_intent(request: &AttachJob) -> Result<&CustomerPricingIntent, AdmissionError> {
    if request.contract != crate::admission::AdmissionContract::CustomerPricingV4 {
        return Err(AdmissionError::InvalidOwner);
    }
    request
        .customer_pricing
        .as_ref()
        .ok_or(AdmissionError::PricingUnavailable)
}

fn map_resolution(error: PriceResolutionError) -> AdmissionError {
    match error {
        PriceResolutionError::StoreUnavailable => AdmissionError::Unavailable,
        PriceResolutionError::InvalidRequest
        | PriceResolutionError::NotFound
        | PriceResolutionError::Ambiguous => AdmissionError::PricingUnavailable,
    }
}

fn map_quote(_: QuoteError) -> AdmissionError {
    AdmissionError::PricingUnavailable
}

fn map_store(error: CustomerQuoteStoreError) -> AdmissionError {
    match error {
        CustomerQuoteStoreError::Unavailable => AdmissionError::Unavailable,
        CustomerQuoteStoreError::BillingLimitExceeded => AdmissionError::BillingLimitExceeded,
        CustomerQuoteStoreError::ProjectBudgetExceeded => AdmissionError::ProjectBudgetExceeded,
        CustomerQuoteStoreError::ProjectBudgetCurrencyMismatch => {
            AdmissionError::PricingUnavailable
        }
        CustomerQuoteStoreError::InvalidInput | CustomerQuoteStoreError::Conflict => {
            AdmissionError::InvalidOwner
        }
    }
}

fn unavailable(_: sqlx::Error) -> AdmissionError {
    AdmissionError::Unavailable
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::{
        AdmissionContract, AdmissionTicket, DreaminaImageAdmissionPlan, DreaminaVideoAdmissionPlan,
        EditInputDescriptorV1, EditInputRoleV1, XaiImageAdmissionPlan, XaiVideoAdmissionInput,
        XaiVideoAdmissionPlan,
    };
    use crate::generator::EditJob;
    use crate::input_blobs::{InputBlobKey, InputBlobRef};
    use image_api_contracts::dreamina::{
        DreaminaImageGenerationRequest, DreaminaVideoGenerationRequest,
    };
    use image_api_contracts::xai::{
        XaiImageAspectRatio, XaiImageGenerationRequest, XaiImageResolution, XaiImageResponseFormat,
        XaiVideoGenerationRequest, XaiVideoImageUrl, XaiVideoResolution,
    };
    use serde_json::json;

    #[test]
    fn route_operations_map_only_to_supported_v4_pricing_facts() {
        assert_eq!(
            pricing_operation_for_route(
                CODEX_PROVIDER_ID,
                "images.generations",
                GENERATION_COMMAND_SCHEMA,
                "image",
            ),
            Some(GENERATION_OPERATION)
        );
        assert_eq!(
            pricing_operation_for_route(
                CODEX_PROVIDER_ID,
                "images.edits",
                EDIT_COMMAND_SCHEMA,
                "image",
            ),
            Some(EDIT_OPERATION)
        );
        assert_eq!(
            pricing_operation_for_route(
                GROK_PROVIDER_ID,
                "images.generations",
                GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
                "image",
            ),
            Some(GENERATION_OPERATION)
        );
        assert_eq!(
            pricing_operation_for_route(
                GROK_PROVIDER_ID,
                "images.edits",
                GROK_IMAGE_EDIT_COMMAND_SCHEMA,
                "image",
            ),
            Some(EDIT_OPERATION)
        );
        assert_eq!(
            pricing_operation_for_route(
                GROK_PROVIDER_ID,
                "videos.generations",
                "grok-cli.videos.generate.v1",
                "video",
            ),
            Some(VIDEO_GENERATION_OPERATION)
        );
        assert_eq!(
            pricing_operation_for_route(
                DREAMINA_PROVIDER_ID,
                "videos.generations",
                DREAMINA_SUBMIT_COMMAND_SCHEMA,
                "video",
            ),
            Some(VIDEO_GENERATION_OPERATION)
        );
    }

    #[test]
    fn customer_metering_contract_reuses_admission_supported_facts() {
        let exact = metering_version("image_output", "image", "request_derived", "any", "exact");
        assert_eq!(
            customer_metering_contract(&exact, GROK_PROVIDER_ID),
            CustomerMeteringContract::Exact
        );

        let token_lookup = metering_version(
            "image_output_token",
            "token",
            "official_lookup",
            "any",
            "estimated",
        );
        assert_eq!(
            customer_metering_contract(&token_lookup, CODEX_PROVIDER_ID),
            CustomerMeteringContract::Incompatible
        );

        let unsupported = metering_version("text_input_token", "token", "reported", "any", "exact");
        assert_eq!(
            customer_metering_contract(&unsupported, CODEX_PROVIDER_ID),
            CustomerMeteringContract::Incompatible
        );

        let incomplete_terminal = metering_version(
            "image_output",
            "image",
            "request_derived",
            "succeeded",
            "exact",
        );
        assert_eq!(
            customer_metering_contract(&incomplete_terminal, GROK_PROVIDER_ID),
            CustomerMeteringContract::Incompatible
        );
    }

    fn metering_version(
        metric: &str,
        unit: &str,
        quantity_source: &str,
        outcome: &str,
        required_confidence: &str,
    ) -> PriceBookVersionView {
        PriceBookVersionView {
            price_book_version_id: Uuid::new_v4(),
            price_book_id: Uuid::new_v4(),
            version: 1,
            api_profile: "test-profile".to_string(),
            operation: GENERATION_OPERATION.to_string(),
            provider_id: Some(CODEX_PROVIDER_ID.to_string()),
            provider_model_id: Some("gpt-image-2".to_string()),
            public_model_id: "gpt-image-2".to_string(),
            media_kind: "image".to_string(),
            service_tier: "standard".to_string(),
            execution_surface: "provider_cli".to_string(),
            billing_mode: "customer_rate".to_string(),
            is_free: false,
            state: "active".to_string(),
            effective_from_ms: 1,
            effective_until_ms: None,
            source_kind: "manual".to_string(),
            source_url: None,
            source_checked_at_ms: None,
            notes: None,
            control_version: "1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 1,
            components: vec![crate::pricing::PriceComponentView {
                price_component_id: Uuid::new_v4(),
                component_key: "test-rate".to_string(),
                metric: metric.to_string(),
                unit: unit.to_string(),
                unit_size: "1".to_string(),
                unit_price_micros: "1".to_string(),
                outcome: outcome.to_string(),
                quantity_source: quantity_source.to_string(),
                required_confidence: required_confidence.to_string(),
                rounding_mode: "exact".to_string(),
                dimensions: json!({}),
                created_at_ms: 1,
            }],
        }
    }

    #[test]
    fn codex_edit_pricing_dimensions_are_bound_to_the_signed_command() {
        let command = EditCommandV1::from_edit_job(
            &EditJob {
                request_id: "req-edit-pricing".to_string(),
                model: "gpt-image-2".to_string(),
                prompt: "replace the sky".to_string(),
                moderation: "auto".to_string(),
                images: Vec::new(),
                mask: None,
                n: 2,
                size: "1536x1024".to_string(),
                quality: "high".to_string(),
                output_format: "png".to_string(),
                output_compression: None,
                background: "auto".to_string(),
                stream: false,
                partial_images: 0,
            },
            vec![EditInputDescriptorV1 {
                byte_size: 123,
                index: 0,
                media_type: "image/png".to_string(),
                role: EditInputRoleV1::Image,
                sha256_hex: "1".repeat(64),
            }],
            "openai-images-v1",
            CODEX_PROVIDER_ID,
        );
        let request_hash = command.request_hash_hex();
        let request = AttachJob {
            ticket: AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: Uuid::new_v4(),
                request_hash,
            },
            job_id: Uuid::new_v4(),
            command_schema: EDIT_COMMAND_SCHEMA.to_string(),
            command_json: serde_json::to_value(command).expect("edit command serializes"),
            input_manifest: None,
            work_kind: "image_batch".to_string(),
            schedule_scope: "tenant-a".to_string(),
            schedule_weight: 1,
            schedule_priority: 1,
            schedule_cost: 2,
            contract: AdmissionContract::CustomerPricingV4,
            customer_pricing: None,
        };

        let facts =
            command_pricing_facts(&request, "openai-images-v1").expect("signed edit pricing facts");
        assert_eq!(facts.provider_id, CODEX_PROVIDER_ID);
        assert_eq!(facts.provider_model_id, "gpt-image-2");
        assert_eq!(facts.api_profile, "openai-images-v1");
        assert_eq!(facts.operation, EDIT_OPERATION);
        assert_eq!(facts.output_count, 2);
        assert_eq!(
            facts.pricing_dimensions,
            BTreeMap::from([
                ("quality".to_string(), "high".to_string()),
                ("size".to_string(), "1536x1024".to_string()),
            ])
        );
    }

    #[test]
    fn grok_pricing_dimensions_are_bound_to_the_signed_source_command() {
        let plan = XaiImageAdmissionPlan::for_grok_cli(XaiImageGenerationRequest {
            aspect_ratio: Some(XaiImageAspectRatio::R16x9),
            model: Some("grok-imagine-image-quality".to_string()),
            n: Some(1),
            prompt: "pricing boundary".to_string(),
            resolution: Some(XaiImageResolution::R1k),
            response_format: Some(XaiImageResponseFormat::B64Json),
            storage_options: None,
            user: None,
        })
        .expect("valid Grok image plan");
        let claim = plan.claim(
            Uuid::new_v4(),
            "tenant-a",
            "project-a",
            "req-pricing-boundary",
            None,
            i64::MAX,
        );
        let request = plan.attach(
            AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: claim.owner_token,
                request_hash: claim.request_hash,
            },
            Uuid::new_v4(),
            "tenant-a",
            AdmissionContract::CustomerPricingV4,
        );

        let facts = command_pricing_facts(&request, XAI_IMAGES_API_PROFILE)
            .expect("signed Grok pricing facts");
        assert_eq!(facts.provider_id, GROK_PROVIDER_ID);
        assert_eq!(facts.provider_model_id, "grok-imagine-image-quality");
        assert_eq!(facts.api_profile, XAI_IMAGES_API_PROFILE);
        assert_eq!(facts.operation, GENERATION_OPERATION);
        assert_eq!(facts.output_count, 1);
        assert_eq!(
            facts.pricing_dimensions,
            BTreeMap::from([
                ("aspect_ratio".to_string(), "16:9".to_string()),
                ("resolution".to_string(), "1k".to_string()),
            ])
        );

        let mut intent = CustomerPricingIntent {
            public_model_id: "grok-imagine-image-quality".to_string(),
            provider_model_id: "grok-imagine-image-quality".to_string(),
            execution_model_id: "grok-imagine-image-quality".to_string(),
            provider_command_hash: None,
            media_kind: "image".to_string(),
            service_tier: "standard".to_string(),
            service_tier_decision:
                crate::service_tiers::ServiceTierDecision::for_default_only_project(
                    crate::service_tiers::ProjectServiceTier::Default,
                ),
            execution_surface: "provider_cli".to_string(),
            currency: "USD".to_string(),
            pricing_dimensions: facts.pricing_dimensions.clone(),
            processing_mode: crate::admission::PricingProcessingMode::Synchronous,
        };
        assert!(validate_pricing_dimensions(&intent, &facts).is_ok());
        assert!(validate_command_pricing_identity(&intent, XAI_IMAGES_API_PROFILE, &facts).is_ok());
        assert!(matches!(
            validate_command_pricing_identity(&intent, "openai-images-v1", &facts),
            Err(AdmissionError::PricingUnavailable)
        ));
        intent
            .pricing_dimensions
            .insert("aspect_ratio".to_string(), "1:1".to_string());
        assert!(matches!(
            validate_pricing_dimensions(&intent, &facts),
            Err(AdmissionError::PricingUnavailable)
        ));
    }

    #[test]
    fn dreamina_video_pricing_is_bound_to_signed_duration_and_geometry() {
        let plan = DreaminaVideoAdmissionPlan::new(DreaminaVideoGenerationRequest {
            prompt: "pricing boundary".to_string(),
            model_version: Some("seedance2.0fast".to_string()),
            ratio: Some("9:16".to_string()),
            duration: Some(8),
            video_resolution: "720p".to_string(),
        })
        .expect("valid Dreamina video plan");
        let claim = plan.claim(
            Uuid::new_v4(),
            "tenant-a",
            "project-a",
            "req-video-pricing-boundary",
            None,
            i64::MAX,
        );
        let request = plan.attach(
            AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: claim.owner_token,
                request_hash: claim.request_hash,
            },
            Uuid::new_v4(),
            "tenant-a",
        );

        let facts = command_pricing_facts(&request, DREAMINA_VIDEOS_API_PROFILE)
            .expect("signed Dreamina video pricing facts");
        assert_eq!(facts.provider_id, DREAMINA_PROVIDER_ID);
        assert_eq!(facts.provider_model_id, "seedance2.0fast");
        assert_eq!(facts.api_profile, DREAMINA_VIDEOS_API_PROFILE);
        assert_eq!(facts.operation, VIDEO_GENERATION_OPERATION);
        assert_eq!(facts.output_count, 1);
        assert_eq!(facts.billable_units, 8);
        assert_eq!(facts.output_billable_units, 8);
        assert_eq!(facts.billing_metric, "video_second");
        assert_eq!(facts.billing_unit, "second");
        assert_eq!(facts.media_kind, "video");
        assert_eq!(
            facts.pricing_dimensions,
            BTreeMap::from([
                ("duration".to_string(), "8".to_string()),
                ("ratio".to_string(), "9:16".to_string()),
                ("resolution".to_string(), "720p".to_string()),
            ])
        );
    }

    #[test]
    fn grok_video_pricing_binds_input_count_duration_and_resolution() {
        let input = XaiVideoAdmissionInput::new(
            "input.png",
            InputBlobRef {
                key: InputBlobKey {
                    admission_session_id: Uuid::new_v4(),
                    input_id: Uuid::new_v4(),
                },
                storage_backend: "test".to_owned(),
                object_key: "input.png".to_owned(),
                sha256_hex: "a".repeat(64),
                byte_size: 1,
            },
            "image/png",
        )
        .expect("valid staged video input");
        let plan = XaiVideoAdmissionPlan::for_grok_cli(
            XaiVideoGenerationRequest {
                aspect_ratio: None,
                duration: Some(6),
                image: Some(XaiVideoImageUrl {
                    file_id: None,
                    url: Some("data:image/png;base64,AA==".to_owned()),
                }),
                model: Some("grok-imagine-video-1.5".to_owned()),
                output: None,
                prompt: Some("pricing boundary".to_owned()),
                reference_images: Vec::new(),
                resolution: Some(XaiVideoResolution::P480),
                storage_options: None,
                user: None,
            },
            vec![input],
        )
        .expect("valid Grok video plan");
        let claim = plan.claim(
            Uuid::new_v4(),
            "tenant-a",
            "project-a",
            "req-grok-video-pricing-boundary",
            None,
            i64::MAX,
        );
        let request = plan.attach(
            AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: claim.owner_token,
                request_hash: claim.request_hash,
            },
            Uuid::new_v4(),
            "tenant-a",
            AdmissionContract::CustomerPricingV4,
        );

        let facts = command_pricing_facts(&request, XAI_VIDEOS_API_PROFILE)
            .expect("signed Grok video pricing facts");
        assert_eq!(facts.provider_id, GROK_PROVIDER_ID);
        assert_eq!(facts.provider_model_id, "grok-imagine-video-1.5-preview");
        assert_eq!(facts.api_profile, XAI_VIDEOS_API_PROFILE);
        assert_eq!(facts.operation, VIDEO_GENERATION_OPERATION);
        assert_eq!(facts.output_count, 1);
        assert_eq!(facts.billable_units, 6);
        assert_eq!(facts.output_billable_units, 6);
        assert_eq!(facts.billing_metric, "video_second");
        assert_eq!(facts.billing_unit, "second");
        assert_eq!(facts.media_kind, "video");
        assert_eq!(
            facts.pricing_dimensions,
            BTreeMap::from([
                ("duration".to_owned(), "6".to_owned()),
                ("input_image_count".to_owned(), "1".to_owned()),
                ("resolution".to_owned(), "480p".to_owned()),
            ])
        );
    }

    #[test]
    fn grok_video_quote_reserves_input_images_and_requested_seconds_once() {
        let mut version =
            metering_version("image_input", "image", "request_derived", "any", "exact");
        version.api_profile = XAI_VIDEOS_API_PROFILE.to_owned();
        version.operation = VIDEO_GENERATION_OPERATION.to_owned();
        version.provider_id = Some(GROK_PROVIDER_ID.to_owned());
        version.provider_model_id = Some("grok-imagine-video-1.5-preview".to_owned());
        version.public_model_id = "grok-imagine-video-1.5".to_owned();
        version.media_kind = "video".to_owned();
        version.components.push(crate::pricing::PriceComponentView {
            price_component_id: Uuid::new_v4(),
            component_key: "video-seconds".to_owned(),
            metric: "video_requested_second".to_owned(),
            unit: "second".to_owned(),
            unit_size: "1".to_owned(),
            unit_price_micros: "1".to_owned(),
            outcome: "any".to_owned(),
            quantity_source: "request_derived".to_owned(),
            required_confidence: "exact".to_owned(),
            rounding_mode: "exact".to_owned(),
            dimensions: json!({"resolution": "480p"}),
            created_at_ms: 1,
        });
        let contract = find_contract_for_pricing(
            GROK_PROVIDER_ID,
            VIDEO_GENERATION_OPERATION,
            GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
            "video",
        )
        .expect("Grok video pricing surface");
        assert!(customer_metering_bases_complete(&version, contract));
        let mut incomplete = version.clone();
        incomplete
            .components
            .retain(|component| component.metric != "image_input");
        assert!(!customer_metering_bases_complete(&incomplete, contract));

        let resolved = ResolvedPriceVersion {
            price_book_id: Uuid::new_v4(),
            price_book_key: "test.grok.video".to_owned(),
            purpose: "customer_sale".to_owned(),
            scope_type: "platform".to_owned(),
            organization_id: None,
            project_id: None,
            provider_id: Some(GROK_PROVIDER_ID.to_owned()),
            currency: "USD".to_owned(),
            version,
        };
        let output_id = Uuid::new_v4();
        let quantities = quote_quantities(
            &resolved,
            &[output_id],
            &json!({
                "duration": "6",
                "input_image_count": "1",
                "resolution": "480p"
            }),
        )
        .expect("Grok video quote quantities");
        assert_eq!(quantities.len(), 2);
        assert!(quantities.iter().all(|item| {
            item.partition_key == format!("output:{output_id}")
                && item.reservation_confidence == "exact"
        }));
        assert!(quantities.iter().any(|item| {
            item.metric == "image_input" && item.unit == "image" && item.max_quantity == "1"
        }));
        assert!(quantities.iter().any(|item| {
            item.metric == "video_requested_second"
                && item.unit == "second"
                && item.max_quantity == "6"
        }));
    }

    #[test]
    fn dreamina_pricing_identity_preserves_native_and_ark_api_profiles() {
        let plan = DreaminaImageAdmissionPlan::new(DreaminaImageGenerationRequest {
            prompt: "pricing boundary".to_string(),
            model_version: Some("5.0".to_string()),
            ratio: Some("16:9".to_string()),
            resolution_type: "2k".to_string(),
            width: None,
            height: None,
            generate_num: Some(2),
        })
        .expect("valid Dreamina image plan");
        let claim = plan.claim(
            Uuid::new_v4(),
            "tenant-a",
            "project-a",
            "req-dreamina-pricing-boundary",
            None,
            i64::MAX,
        );
        let mut request = plan.attach(
            AdmissionTicket {
                session_id: Uuid::new_v4(),
                owner_token: claim.owner_token,
                request_hash: claim.request_hash,
            },
            Uuid::new_v4(),
            "tenant-a",
        );
        request.contract = AdmissionContract::CustomerPricingV4;

        for profile in [DREAMINA_IMAGES_API_PROFILE, ARK_IMAGES_API_PROFILE] {
            let facts =
                command_pricing_facts(&request, profile).expect("signed Dreamina pricing facts");
            assert_eq!(facts.provider_id, DREAMINA_PROVIDER_ID);
            assert_eq!(facts.provider_model_id, "5.0");
            assert_eq!(facts.api_profile, profile);
            assert_eq!(facts.operation, GENERATION_OPERATION);
            assert_eq!(facts.output_count, 2);
            assert_eq!(
                facts.pricing_dimensions,
                BTreeMap::from([
                    ("ratio".to_string(), "16:9".to_string()),
                    ("resolution_type".to_string(), "2k".to_string()),
                ])
            );
        }
    }

    #[test]
    fn pricing_dimensions_follow_each_signed_provider_route() {
        assert_eq!(
            pricing_dimension_keys_for_route(
                CODEX_PROVIDER_ID,
                "images.generations",
                GENERATION_COMMAND_SCHEMA,
                "image",
            ),
            Some(["quality", "size"].as_slice())
        );
        assert_eq!(
            pricing_dimension_keys_for_route(
                GROK_PROVIDER_ID,
                "images.generations",
                GROK_IMAGE_GENERATION_COMMAND_SCHEMA,
                "image",
            ),
            Some(["aspect_ratio", "resolution"].as_slice())
        );
        assert_eq!(
            pricing_dimension_keys_for_route(
                DREAMINA_PROVIDER_ID,
                "images.generations",
                DREAMINA_SUBMIT_COMMAND_SCHEMA,
                "image",
            ),
            Some(["height", "ratio", "resolution_type", "width"].as_slice())
        );
        assert_eq!(
            pricing_dimension_keys_for_route(
                DREAMINA_PROVIDER_ID,
                "videos.generations",
                DREAMINA_SUBMIT_COMMAND_SCHEMA,
                "video",
            ),
            Some(["duration", "ratio", "resolution"].as_slice())
        );
        assert_eq!(
            pricing_dimension_keys_for_route(
                GROK_PROVIDER_ID,
                "videos.generations",
                GROK_VIDEO_GENERATION_COMMAND_SCHEMA,
                "video",
            ),
            Some(
                [
                    "aspect_ratio",
                    "duration",
                    "input_image_count",
                    "resolution"
                ]
                .as_slice()
            )
        );
    }
}
