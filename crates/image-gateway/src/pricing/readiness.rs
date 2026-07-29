use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;
use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::ImageGatewayError;

use super::{
    PriceBookVersionView, PriceComponentView, PricePublishReadiness,
    admission::{
        CustomerMeteringContract, customer_metering_contract, pricing_operation_for_route,
    },
    coverage::{CoverageSurfaceRow, load_aliases_on, load_surfaces_on},
    surface_contract::{ContractBindingSnapshot, ExactSurfaceIdentity, find_contract},
};

#[derive(FromRow)]
struct PublishCandidateRow {
    price_book_id: Uuid,
    purpose: String,
    scope_type: String,
    organization_id: Option<String>,
    project_id: Option<String>,
    book_provider_id: Option<String>,
    currency: String,
    price_book_version_id: Uuid,
    version: i32,
    api_profile: String,
    operation: String,
    provider_id: Option<String>,
    provider_model_id: Option<String>,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
    execution_surface: String,
    billing_mode: String,
    is_free: bool,
    state: String,
    effective_from_ms: i64,
    effective_until_ms: Option<i64>,
    source_kind: String,
    source_url: Option<String>,
    source_checked_at_ms: Option<i64>,
    notes: Option<String>,
    control_version: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(FromRow)]
struct PublishComponentRow {
    price_component_id: Uuid,
    component_key: String,
    metric: String,
    unit: String,
    unit_size: i64,
    unit_price_micros: i64,
    outcome: String,
    quantity_source: String,
    required_confidence: String,
    rounding_mode: String,
    dimensions_json: Value,
    created_at_ms: i64,
}

#[derive(FromRow)]
struct ActiveSelectorRow {
    book_provider_id: Option<String>,
    api_profile: String,
    operation: String,
    provider_id: Option<String>,
    provider_model_id: Option<String>,
    public_model_id: String,
    media_kind: String,
    service_tier: String,
    execution_surface: String,
    billing_mode: String,
}

const PUBLISH_CANDIDATE_SQL: &str = r#"
    SELECT book.price_book_id, book.purpose, book.scope_type,
           book.organization_id, book.project_id,
           book.provider_id AS book_provider_id, book.currency,
           version.price_book_version_id, version.version,
           version.api_profile, version.operation, version.provider_id,
           version.provider_model_id, version.public_model_id,
           version.media_kind, version.service_tier,
           version.execution_surface, version.billing_mode,
           version.is_free, version.state, version.effective_from_ms,
           version.effective_until_ms, version.source_kind,
           version.source_url, version.source_checked_at_ms,
           version.notes, version.control_version,
           version.created_at_ms, version.updated_at_ms
    FROM price_books book
    JOIN price_book_versions version
      ON version.price_book_id = book.price_book_id
    WHERE version.price_book_version_id = $1
      AND book.state = 'active'
"#;

pub(super) async fn evaluate_on(
    connection: &mut PgConnection,
    price_book_version_id: Uuid,
) -> Result<PricePublishReadiness, ImageGatewayError> {
    let candidate = sqlx::query_as::<_, PublishCandidateRow>(PUBLISH_CANDIDATE_SQL)
        .bind(price_book_version_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Price book version not found",
                None,
                "price_book_version_not_found",
            )
        })?;
    let components = sqlx::query_as::<_, PublishComponentRow>(
        r#"
        SELECT price_component_id, component_key, metric, unit, unit_size,
               unit_price_micros, outcome, quantity_source,
               required_confidence, rounding_mode, dimensions_json,
               created_at_ms
        FROM price_components
        WHERE price_book_version_id = $1
        ORDER BY component_key
        "#,
    )
    .bind(price_book_version_id)
    .fetch_all(&mut *connection)
    .await
    .map_err(store_unavailable)?
    .into_iter()
    .map(PublishComponentRow::into_view)
    .collect();
    let version = candidate.version_view(components);
    let surfaces = load_surfaces_on(connection).await?;
    let aliases = load_aliases_on(connection).await?;
    let matches = matching_surfaces(
        candidate.book_provider_id.as_deref(),
        &version,
        &surfaces,
        &aliases,
    );
    let mut blocking_reasons = Vec::new();
    let mut warnings = Vec::new();

    if version.state != "draft" {
        blocking_reasons.push("version_not_draft".to_string());
    }
    validate_source(&version, &mut blocking_reasons);
    validate_price_shape(
        &candidate.purpose,
        &version,
        &mut blocking_reasons,
        &mut warnings,
    );

    let (metering_status, request_dimensions) = if candidate.purpose == "customer_sale" {
        if candidate.currency != "USD" {
            blocking_reasons.push("customer_currency_not_usd".to_string());
        }
        if matches.is_empty() {
            blocking_reasons.push("platform_surface_missing".to_string());
        }
        let mut incompatible = false;
        let mut dimensions = BTreeSet::new();
        for surface in &matches {
            match customer_metering_contract(&version, &surface.provider_id) {
                CustomerMeteringContract::Exact => {}
                CustomerMeteringContract::Incompatible => incompatible = true,
            }
            match contract_snapshot_for_surface(&version, surface) {
                Ok((contract, _)) => {
                    dimensions.extend(
                        contract
                            .dimensions
                            .iter()
                            .map(|dimension| dimension.key.to_string()),
                    );
                    if version.components.iter().any(|component| {
                        contract
                            .validate_component_selector(&component.dimensions)
                            .is_err()
                    }) {
                        blocking_reasons.push("component_dimension_value_unsupported".to_string());
                    }
                    if version.components.iter().any(|component| {
                        !contract.metering_bases.iter().any(|basis| {
                            basis.metric == component.metric
                                && basis.unit == component.unit
                                && basis.quantity_source == component.quantity_source
                        })
                    }) {
                        blocking_reasons.push("metering_basis_not_in_surface_contract".to_string());
                    }
                    if !has_required_customer_metering_bases(&version, contract) {
                        blocking_reasons
                            .push("required_customer_metering_basis_missing".to_string());
                    }
                }
                Err(reason) => blocking_reasons.push(reason.to_string()),
            }
        }
        if incompatible {
            blocking_reasons.push("metering_contract_incompatible".to_string());
        }
        validate_component_dimensions(&version, &dimensions, &mut blocking_reasons, &mut warnings);
        (
            if incompatible {
                "incompatible"
            } else if matches.is_empty() {
                "missing"
            } else {
                "exact"
            }
            .to_string(),
            dimensions.into_iter().collect(),
        )
    } else {
        ("not_applicable".to_string(), Vec::new())
    };
    if has_cross_book_resolution_conflict(connection, &candidate, &version, &matches, &aliases)
        .await?
    {
        blocking_reasons.push("active_price_resolution_conflict".to_string());
    }

    blocking_reasons.sort();
    blocking_reasons.dedup();
    warnings.sort();
    warnings.dedup();
    Ok(PricePublishReadiness {
        price_book_version_id,
        price_book_id: candidate.price_book_id,
        purpose: candidate.purpose,
        ready: blocking_reasons.is_empty(),
        matching_surface_count: matches.len() as i64,
        metering_status,
        request_dimensions,
        blocking_reasons,
        warnings,
    })
}

pub(super) async fn binding_snapshots_on(
    connection: &mut PgConnection,
    price_book_version_id: Uuid,
) -> Result<Vec<ContractBindingSnapshot>, ImageGatewayError> {
    let candidate = sqlx::query_as::<_, PublishCandidateRow>(PUBLISH_CANDIDATE_SQL)
        .bind(price_book_version_id)
        .fetch_optional(&mut *connection)
        .await
        .map_err(store_unavailable)?
        .ok_or_else(|| {
            ImageGatewayError::not_found(
                "Price book version not found",
                None,
                "price_book_version_not_found",
            )
        })?;
    if candidate.purpose != "customer_sale" {
        return Ok(Vec::new());
    }
    let version = candidate.version_view(Vec::new());
    let surfaces = load_surfaces_on(connection).await?;
    let aliases = load_aliases_on(connection).await?;
    let matches = matching_surfaces(
        candidate.book_provider_id.as_deref(),
        &version,
        &surfaces,
        &aliases,
    );
    let mut snapshots = BTreeMap::new();
    for surface in matches {
        let (_, snapshot) = contract_snapshot_for_surface(&version, surface).map_err(|reason| {
            ImageGatewayError::invalid_request(
                "Pricing surface contract is unavailable",
                Some("price_book_version_id".to_string()),
                reason,
            )
        })?;
        snapshots.insert((snapshot.contract_key.clone(), snapshot.revision), snapshot);
    }
    if snapshots.is_empty() {
        return Err(ImageGatewayError::invalid_request(
            "Pricing surface contract is unavailable",
            Some("price_book_version_id".to_string()),
            "pricing_surface_contract_missing",
        ));
    }
    Ok(snapshots.into_values().collect())
}

fn contract_snapshot_for_surface(
    version: &PriceBookVersionView,
    surface: &CoverageSurfaceRow,
) -> Result<
    (
        &'static super::surface_contract::PricingSurfaceContract,
        ContractBindingSnapshot,
    ),
    &'static str,
> {
    let command_schema = surface
        .command_schema
        .as_deref()
        .ok_or("pricing_surface_contract_missing")?;
    let api_profile = surface
        .api_profile
        .as_deref()
        .ok_or("pricing_surface_contract_missing")?;
    let public_model_id = surface
        .public_model_id
        .as_deref()
        .ok_or("pricing_surface_contract_missing")?;
    let contract = find_contract(
        &surface.provider_id,
        &surface.operation,
        command_schema,
        &surface.media_kind,
    )
    .ok_or("pricing_surface_contract_missing")?;
    let snapshot = contract
        .binding_snapshot(ExactSurfaceIdentity {
            api_profile,
            provider_model_id: &surface.provider_model_id,
            public_model_id,
            service_tier: if version.service_tier == "*" {
                "standard"
            } else {
                &version.service_tier
            },
            execution_surface: &version.execution_surface,
        })
        .map_err(|error| match error {
            super::surface_contract::ValidationError::UnsupportedSurface => {
                "pricing_surface_contract_unsupported"
            }
            _ => "pricing_surface_contract_identity_mismatch",
        })?;
    Ok((contract, snapshot))
}

async fn has_cross_book_resolution_conflict(
    connection: &mut PgConnection,
    candidate: &PublishCandidateRow,
    version: &PriceBookVersionView,
    surfaces: &[&CoverageSurfaceRow],
    aliases: &HashMap<String, String>,
) -> Result<bool, ImageGatewayError> {
    if surfaces.is_empty() {
        return Ok(false);
    }
    let active = sqlx::query_as::<_, ActiveSelectorRow>(
        r#"
        SELECT book.provider_id AS book_provider_id,
               version.api_profile, version.operation, version.provider_id,
               version.provider_model_id, version.public_model_id,
               version.media_kind, version.service_tier,
               version.execution_surface, version.billing_mode
        FROM price_books book
        JOIN price_book_versions version
          ON version.price_book_id = book.price_book_id
        WHERE book.state = 'active'
          AND version.state = 'active'
          AND book.price_book_id <> $1
          AND book.purpose = $2
          AND book.currency = $3
          AND book.scope_type = $4
          AND book.organization_id IS NOT DISTINCT FROM $5
          AND book.project_id IS NOT DISTINCT FROM $6
          AND (
            version.effective_until_ms IS NULL
            OR version.effective_until_ms > $7
          )
        "#,
    )
    .bind(candidate.price_book_id)
    .bind(&candidate.purpose)
    .bind(&candidate.currency)
    .bind(&candidate.scope_type)
    .bind(&candidate.organization_id)
    .bind(&candidate.project_id)
    .bind(candidate.effective_from_ms)
    .fetch_all(&mut *connection)
    .await
    .map_err(store_unavailable)?;

    Ok(surfaces.iter().any(|surface| {
        let candidate_rank = selector_rank(
            candidate.book_provider_id.as_deref(),
            version,
            surface,
            aliases,
        );
        active.iter().any(|active| {
            active.billing_mode == version.billing_mode
                && active.execution_surface == version.execution_surface
                && active.matches_surface(surface, aliases)
                && active.rank(surface, aliases) == candidate_rank
        })
    }))
}

impl PublishCandidateRow {
    fn version_view(&self, components: Vec<PriceComponentView>) -> PriceBookVersionView {
        PriceBookVersionView {
            price_book_version_id: self.price_book_version_id,
            price_book_id: self.price_book_id,
            version: self.version,
            api_profile: self.api_profile.clone(),
            operation: self.operation.clone(),
            provider_id: self.provider_id.clone(),
            provider_model_id: self.provider_model_id.clone(),
            public_model_id: self.public_model_id.clone(),
            media_kind: self.media_kind.clone(),
            service_tier: self.service_tier.clone(),
            execution_surface: self.execution_surface.clone(),
            billing_mode: self.billing_mode.clone(),
            is_free: self.is_free,
            state: self.state.clone(),
            effective_from_ms: self.effective_from_ms,
            effective_until_ms: self.effective_until_ms,
            source_kind: self.source_kind.clone(),
            source_url: self.source_url.clone(),
            source_checked_at_ms: self.source_checked_at_ms,
            notes: self.notes.clone(),
            control_version: self.control_version.to_string(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
            components,
        }
    }
}

impl PublishComponentRow {
    fn into_view(self) -> PriceComponentView {
        PriceComponentView {
            price_component_id: self.price_component_id,
            component_key: self.component_key,
            metric: self.metric,
            unit: self.unit,
            unit_size: self.unit_size.to_string(),
            unit_price_micros: self.unit_price_micros.to_string(),
            outcome: self.outcome,
            quantity_source: self.quantity_source,
            required_confidence: self.required_confidence,
            rounding_mode: self.rounding_mode,
            dimensions: self.dimensions_json,
            created_at_ms: self.created_at_ms,
        }
    }
}

impl ActiveSelectorRow {
    fn matches_surface(
        &self,
        surface: &CoverageSurfaceRow,
        aliases: &HashMap<String, String>,
    ) -> bool {
        let Some(command_schema) = surface.command_schema.as_deref() else {
            return false;
        };
        let Some(api_profile) = surface.api_profile.as_deref() else {
            return false;
        };
        let Some(public_model_id) = surface.public_model_id.as_deref() else {
            return false;
        };
        let pricing_profile = aliases
            .get(api_profile)
            .map(String::as_str)
            .unwrap_or(api_profile);
        self.book_provider_id
            .as_deref()
            .is_none_or(|provider| provider == surface.provider_id)
            && self
                .provider_id
                .as_deref()
                .is_none_or(|provider| provider == surface.provider_id)
            && self
                .provider_model_id
                .as_deref()
                .is_none_or(|model| model == surface.provider_model_id)
            && self.media_kind == surface.media_kind
            && matches!(self.service_tier.as_str(), "standard" | "*")
            && matches_value(&self.public_model_id, public_model_id)
            && (self.api_profile == "*"
                || self.api_profile == api_profile
                || self.api_profile == pricing_profile)
            && pricing_operation_for_route(
                &surface.provider_id,
                &surface.operation,
                command_schema,
                &surface.media_kind,
            )
            .is_some_and(|operation| matches_value(&self.operation, operation))
    }

    fn rank(&self, surface: &CoverageSurfaceRow, aliases: &HashMap<String, String>) -> [u8; 7] {
        selector_rank_parts(
            self.book_provider_id.as_deref(),
            self.provider_id.as_deref(),
            &self.api_profile,
            &self.operation,
            self.provider_model_id.as_deref(),
            &self.public_model_id,
            &self.service_tier,
            surface,
            aliases,
        )
    }
}

fn selector_rank(
    book_provider_id: Option<&str>,
    version: &PriceBookVersionView,
    surface: &CoverageSurfaceRow,
    aliases: &HashMap<String, String>,
) -> [u8; 7] {
    selector_rank_parts(
        book_provider_id,
        version.provider_id.as_deref(),
        &version.api_profile,
        &version.operation,
        version.provider_model_id.as_deref(),
        &version.public_model_id,
        &version.service_tier,
        surface,
        aliases,
    )
}

#[allow(clippy::too_many_arguments)]
fn selector_rank_parts(
    book_provider_id: Option<&str>,
    provider_id: Option<&str>,
    api_profile: &str,
    operation: &str,
    provider_model_id: Option<&str>,
    public_model_id: &str,
    service_tier: &str,
    surface: &CoverageSurfaceRow,
    aliases: &HashMap<String, String>,
) -> [u8; 7] {
    let surface_profile = surface.api_profile.as_deref().unwrap_or_default();
    let pricing_profile = aliases
        .get(surface_profile)
        .map(String::as_str)
        .unwrap_or(surface_profile);
    let pricing_operation = surface
        .command_schema
        .as_deref()
        .and_then(|command_schema| {
            pricing_operation_for_route(
                &surface.provider_id,
                &surface.operation,
                command_schema,
                &surface.media_kind,
            )
        })
        .unwrap_or_default();
    [
        u8::from(book_provider_id == Some(surface.provider_id.as_str())),
        u8::from(provider_id == Some(surface.provider_id.as_str())),
        if api_profile == surface_profile {
            2
        } else if api_profile == pricing_profile {
            1
        } else {
            0
        },
        u8::from(operation == pricing_operation),
        u8::from(provider_model_id == Some(surface.provider_model_id.as_str())),
        u8::from(public_model_id == surface.public_model_id.as_deref().unwrap_or_default()),
        u8::from(service_tier == "standard"),
    ]
}

fn matching_surfaces<'a>(
    book_provider_id: Option<&str>,
    version: &PriceBookVersionView,
    surfaces: &'a [CoverageSurfaceRow],
    aliases: &HashMap<String, String>,
) -> Vec<&'a CoverageSurfaceRow> {
    surfaces
        .iter()
        .filter(|surface| {
            let Some(route_id) = surface.route_id else {
                return false;
            };
            let Some(command_schema) = surface.command_schema.as_deref() else {
                return false;
            };
            let Some(api_profile) = surface.api_profile.as_deref() else {
                return false;
            };
            let Some(public_model_id) = surface.public_model_id.as_deref() else {
                return false;
            };
            let _ = route_id;
            let pricing_profile = aliases
                .get(api_profile)
                .map(String::as_str)
                .unwrap_or(api_profile);
            book_provider_id.is_none_or(|provider| provider == surface.provider_id)
                && version
                    .provider_id
                    .as_deref()
                    .is_none_or(|provider| provider == surface.provider_id)
                && version
                    .provider_model_id
                    .as_deref()
                    .is_none_or(|model| model == surface.provider_model_id)
                && version.media_kind == surface.media_kind
                && matches!(version.service_tier.as_str(), "standard" | "*")
                && matches_value(&version.public_model_id, public_model_id)
                && (version.api_profile == "*"
                    || version.api_profile == api_profile
                    || version.api_profile == pricing_profile)
                && pricing_operation_for_route(
                    &surface.provider_id,
                    &surface.operation,
                    command_schema,
                    &surface.media_kind,
                )
                .is_some_and(|operation| matches_value(&version.operation, operation))
        })
        .collect()
}

fn validate_source(version: &PriceBookVersionView, blocking_reasons: &mut Vec<String>) {
    if matches!(
        version.source_kind.as_str(),
        "official_document" | "provider_contract"
    ) && (version.source_url.as_deref().is_none_or(str::is_empty)
        || version.source_checked_at_ms.is_none())
    {
        blocking_reasons.push("source_evidence_missing".to_string());
    }
}

fn validate_price_shape(
    purpose: &str,
    version: &PriceBookVersionView,
    blocking_reasons: &mut Vec<String>,
    warnings: &mut Vec<String>,
) {
    let valid_mode = match purpose {
        "customer_sale" => version.billing_mode == "customer_rate",
        "provider_actual" => matches!(
            version.billing_mode.as_str(),
            "provider_reported" | "contract_rate"
        ),
        "provider_estimated" => matches!(
            version.billing_mode.as_str(),
            "published_rate" | "contract_rate"
        ),
        "provider_allocated" => matches!(
            version.billing_mode.as_str(),
            "subscription_allocation" | "membership_points"
        ),
        "provider_benchmark" => version.billing_mode == "published_rate",
        _ => false,
    };
    if !valid_mode {
        blocking_reasons.push("billing_mode_mismatch".to_string());
    }
    if purpose == "customer_sale" && version.execution_surface != "provider_cli" {
        blocking_reasons.push("execution_surface_mismatch".to_string());
    }
    if version.billing_mode == "provider_reported" {
        if !version.components.is_empty() {
            blocking_reasons.push("provider_reported_components_present".to_string());
        }
        return;
    }
    if version.components.is_empty() {
        blocking_reasons.push("price_components_missing".to_string());
        return;
    }

    let parsed_prices = version
        .components
        .iter()
        .map(|component| component.unit_price_micros.parse::<u128>())
        .collect::<Result<Vec<_>, _>>();
    let Ok(parsed_prices) = parsed_prices else {
        blocking_reasons.push("component_price_invalid".to_string());
        return;
    };
    if version.is_free {
        if parsed_prices.iter().any(|price| *price != 0) {
            blocking_reasons.push("free_price_has_nonzero_component".to_string());
        }
    } else if purpose != "provider_allocated"
        && !version
            .components
            .iter()
            .zip(parsed_prices.iter())
            .any(|(component, price)| {
                matches!(component.outcome.as_str(), "succeeded" | "any") && *price > 0
            })
    {
        blocking_reasons.push("paid_price_has_no_positive_success_rate".to_string());
    }
    if version.components.iter().any(|component| {
        component.outcome == "any"
            && component
                .unit_price_micros
                .parse::<u128>()
                .is_ok_and(|price| price > 0)
    }) {
        warnings.push("all_outcomes_share_rate".to_string());
    }
}

fn validate_component_dimensions(
    version: &PriceBookVersionView,
    allowed_dimensions: &BTreeSet<String>,
    blocking_reasons: &mut Vec<String>,
    warnings: &mut Vec<String>,
) {
    let mut selectors = BTreeSet::new();
    let mut bases = BTreeSet::new();
    for component in &version.components {
        let Some(dimensions) = component.dimensions.as_object() else {
            blocking_reasons.push("component_dimensions_invalid".to_string());
            continue;
        };
        if dimensions
            .keys()
            .any(|dimension| !allowed_dimensions.contains(dimension))
        {
            blocking_reasons.push("component_dimension_unsupported".to_string());
        }
        let selector = (
            component.metric.as_str(),
            component.unit.as_str(),
            component.quantity_source.as_str(),
            component.outcome.as_str(),
            component.dimensions.to_string(),
        );
        if !selectors.insert(selector) {
            blocking_reasons.push("component_selector_ambiguous".to_string());
        }
        bases.insert((
            component.metric.as_str(),
            component.unit.as_str(),
            component.quantity_source.as_str(),
        ));
    }
    for (index, left) in version.components.iter().enumerate() {
        for right in version.components.iter().skip(index + 1) {
            if selectors_can_tie(left, right) {
                blocking_reasons.push("component_selector_ambiguous".to_string());
            }
        }
    }
    for (metric, unit, quantity_source) in bases {
        for outcome in ["succeeded", "failed", "no_effect"] {
            let covered = version.components.iter().any(|component| {
                component.metric == metric
                    && component.unit == unit
                    && component.quantity_source == quantity_source
                    && (component.outcome == "any" || component.outcome == outcome)
                    && component
                        .dimensions
                        .as_object()
                        .is_some_and(serde_json::Map::is_empty)
            });
            if !covered {
                blocking_reasons.push("request_dimension_fallback_missing".to_string());
            }
        }
    }
    if version.components.iter().any(|component| {
        component
            .dimensions
            .as_object()
            .is_some_and(|value| !value.is_empty())
    }) {
        warnings.push("dimension_overrides_present".to_string());
    }
}

fn has_required_customer_metering_bases(
    version: &PriceBookVersionView,
    contract: &super::surface_contract::PricingSurfaceContract,
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

fn selectors_can_tie(left: &PriceComponentView, right: &PriceComponentView) -> bool {
    if left.metric != right.metric
        || left.unit != right.unit
        || left.quantity_source != right.quantity_source
        || !(left.outcome == right.outcome || left.outcome == "any" || right.outcome == "any")
    {
        return false;
    }
    let (Some(left_dimensions), Some(right_dimensions)) =
        (left.dimensions.as_object(), right.dimensions.as_object())
    else {
        return false;
    };
    left_dimensions.len() == right_dimensions.len()
        && left_dimensions.iter().all(|(key, left_value)| {
            right_dimensions
                .get(key)
                .is_none_or(|right_value| right_value == left_value)
        })
}

fn matches_value(candidate: &str, expected: &str) -> bool {
    candidate == "*" || candidate == expected
}

fn store_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Pricing readiness unavailable")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;
    use uuid::Uuid;

    use super::{
        PriceBookVersionView, PriceComponentView, has_required_customer_metering_bases,
        selectors_can_tie, validate_component_dimensions, validate_price_shape,
    };

    fn component(outcome: &str, dimensions: serde_json::Value) -> PriceComponentView {
        PriceComponentView {
            price_component_id: Uuid::new_v4(),
            component_key: format!("output_image_{outcome}"),
            metric: "image_output".to_string(),
            unit: "image".to_string(),
            unit_size: "1".to_string(),
            unit_price_micros: "1000000".to_string(),
            outcome: outcome.to_string(),
            quantity_source: "request_derived".to_string(),
            required_confidence: "exact".to_string(),
            rounding_mode: "exact".to_string(),
            dimensions,
            created_at_ms: 0,
        }
    }

    fn version(components: Vec<PriceComponentView>) -> PriceBookVersionView {
        PriceBookVersionView {
            price_book_version_id: Uuid::new_v4(),
            price_book_id: Uuid::new_v4(),
            version: 1,
            api_profile: "openai-images-v1".to_string(),
            operation: "generation".to_string(),
            provider_id: Some("openai-codex".to_string()),
            provider_model_id: Some("gpt-image-2".to_string()),
            public_model_id: "gpt-image-2".to_string(),
            media_kind: "image".to_string(),
            service_tier: "standard".to_string(),
            execution_surface: "provider_cli".to_string(),
            billing_mode: "customer_rate".to_string(),
            is_free: false,
            state: "draft".to_string(),
            effective_from_ms: 0,
            effective_until_ms: None,
            source_kind: "platform_override".to_string(),
            source_url: None,
            source_checked_at_ms: None,
            notes: None,
            control_version: "1".to_string(),
            created_at_ms: 0,
            updated_at_ms: 0,
            components,
        }
    }

    #[test]
    fn crossing_dimension_selectors_are_rejected_as_ambiguous() {
        let quality = component("succeeded", json!({"quality": "high"}));
        let size = component("succeeded", json!({"size": "1024x1024"}));

        assert!(selectors_can_tie(&quality, &size));
    }

    #[test]
    fn fallback_and_specific_selector_have_distinct_precedence() {
        let fallback = component("succeeded", json!({}));
        let quality = component("succeeded", json!({"quality": "high"}));

        assert!(!selectors_can_tie(&fallback, &quality));
    }

    #[test]
    fn mutually_exclusive_values_do_not_conflict() {
        let high = component("succeeded", json!({"quality": "high"}));
        let low = component("succeeded", json!({"quality": "low"}));

        assert!(!selectors_can_tie(&high, &low));
    }

    #[test]
    fn grok_video_requires_input_images_and_requested_seconds() {
        let contract = super::find_contract(
            "grok-cli",
            "videos.generations",
            "grok-cli.videos.generate.v1",
            "video",
        )
        .expect("Grok video pricing contract");
        let image_input = PriceComponentView {
            metric: "image_input".to_string(),
            unit: "image".to_string(),
            component_key: "input_image_any".to_string(),
            outcome: "any".to_string(),
            ..component("any", json!({}))
        };
        let requested_seconds = PriceComponentView {
            metric: "video_requested_second".to_string(),
            unit: "second".to_string(),
            component_key: "requested_seconds_any".to_string(),
            outcome: "any".to_string(),
            ..component("any", json!({}))
        };

        assert!(!has_required_customer_metering_bases(
            &version(vec![requested_seconds.clone()]),
            contract,
        ));
        assert!(has_required_customer_metering_bases(
            &version(vec![image_input, requested_seconds]),
            contract,
        ));
    }

    #[test]
    fn terminal_outcomes_split_across_dimensions_require_fallbacks() {
        let version = version(vec![
            component("succeeded", json!({"quality": "high"})),
            component("failed", json!({"quality": "low"})),
            component("no_effect", json!({"quality": "medium"})),
        ]);
        let mut blocking_reasons = Vec::new();
        let mut warnings = Vec::new();

        validate_component_dimensions(
            &version,
            &BTreeSet::from(["quality".to_string(), "size".to_string()]),
            &mut blocking_reasons,
            &mut warnings,
        );

        assert!(blocking_reasons.contains(&"request_dimension_fallback_missing".to_string()));
    }

    #[test]
    fn terminal_fallbacks_allow_specific_dimension_overrides() {
        let mut components = ["succeeded", "failed", "no_effect"]
            .map(|outcome| component(outcome, json!({})))
            .to_vec();
        components.push(component("succeeded", json!({"quality": "high"})));
        let version = version(components);
        let mut blocking_reasons = Vec::new();
        let mut warnings = Vec::new();

        validate_component_dimensions(
            &version,
            &BTreeSet::from(["quality".to_string(), "size".to_string()]),
            &mut blocking_reasons,
            &mut warnings,
        );

        assert!(blocking_reasons.is_empty());
        assert!(warnings.contains(&"dimension_overrides_present".to_string()));
    }

    #[test]
    fn allocated_subscription_uses_pool_total_instead_of_a_fake_unit_price() {
        let mut allocated = version(vec![PriceComponentView {
            unit_price_micros: "0".to_string(),
            ..component("succeeded", json!({}))
        }]);
        allocated.billing_mode = "subscription_allocation".to_string();
        let mut blocking_reasons = Vec::new();
        let mut warnings = Vec::new();

        validate_price_shape(
            "provider_allocated",
            &allocated,
            &mut blocking_reasons,
            &mut warnings,
        );

        assert!(blocking_reasons.is_empty());
    }

    #[test]
    fn zero_customer_rate_still_fails_closed_when_not_marked_free() {
        let customer = version(vec![PriceComponentView {
            unit_price_micros: "0".to_string(),
            ..component("succeeded", json!({}))
        }]);
        let mut blocking_reasons = Vec::new();
        let mut warnings = Vec::new();

        validate_price_shape(
            "customer_sale",
            &customer,
            &mut blocking_reasons,
            &mut warnings,
        );

        assert_eq!(
            blocking_reasons,
            ["paid_price_has_no_positive_success_rate".to_string()]
        );
    }
}
