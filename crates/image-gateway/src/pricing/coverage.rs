use std::collections::{BTreeMap, BTreeSet, HashMap};

use sqlx::{FromRow, PgConnection, PgPool};

use crate::ImageGatewayError;

use super::{
    PriceBookCatalog, PriceBookVersionView, PriceBookView, PricingCoverageRow,
    PricingCoverageSnapshot, PricingCoverageSummary, PricingMeteringBasis,
    admission::{
        CustomerMeteringContract, customer_metering_contract, pricing_operation_for_route,
    },
    surface_contract::find_contract,
};

#[derive(Clone, Debug, FromRow)]
pub(super) struct CoverageSurfaceRow {
    pub(super) provider_id: String,
    pub(super) provider_model_id: String,
    pub(super) provider_model_display_name: String,
    pub(super) media_kind: String,
    pub(super) operation: String,
    pub(super) command_schema: Option<String>,
    pub(super) api_profile: Option<String>,
    pub(super) public_model_id: Option<String>,
    pub(super) route_id: Option<uuid::Uuid>,
    pub(super) routable_account_count: i64,
}

#[derive(Clone, Debug, FromRow)]
pub(super) struct ApiProfileAliasRow {
    api_profile: String,
    pricing_api_profile: String,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct MatchRank {
    book_provider: u8,
    version_provider: u8,
    api_profile: u8,
    operation: u8,
    provider_model: u8,
    public_model: u8,
    service_tier: u8,
}

struct MatchResult<'a> {
    status: &'static str,
    currencies: Vec<String>,
    versions: Vec<(&'a PriceBookView, &'a PriceBookVersionView)>,
}

pub(super) async fn load(
    pool: &PgPool,
    catalog: &PriceBookCatalog,
) -> Result<PricingCoverageSnapshot, ImageGatewayError> {
    let surfaces = load_surfaces(pool).await?;
    let aliases = load_aliases(pool).await?;

    Ok(build_snapshot(catalog, &surfaces, &aliases))
}

pub(super) async fn load_surfaces(
    pool: &PgPool,
) -> Result<Vec<CoverageSurfaceRow>, ImageGatewayError> {
    let mut connection = pool.acquire().await.map_err(store_unavailable)?;
    load_surfaces_on(&mut connection).await
}

pub(super) async fn load_surfaces_on(
    connection: &mut PgConnection,
) -> Result<Vec<CoverageSurfaceRow>, ImageGatewayError> {
    sqlx::query_as::<_, CoverageSurfaceRow>(
        r#"
        WITH model_operations AS (
            SELECT model.provider_id,
                   model.model_id AS provider_model_id,
                   model.display_name AS provider_model_display_name,
                   model.media_kind,
                   operation.operation_id
            FROM provider_models model
            CROSS JOIN LATERAL unnest(model.operation_ids)
                AS operation(operation_id)
            WHERE model.adapter_state = 'supported'
              AND model.lifecycle_state = 'enabled'
        ),
        platform_mappings AS (
            SELECT mapping.provider_id, mapping.operation_id,
                   mapping.provider_model_id, mapping.media_kind,
                   mapping.api_profile, mapping.public_model_id,
                   mapping.route_id, mapping.route_revision,
                   mapping.command_schema
            FROM gateway_platform_provider_routes binding
            JOIN provider_route_model_mappings mapping
              ON mapping.route_id = binding.route_id
             AND mapping.route_revision = binding.route_revision
             AND mapping.provider_id = binding.provider_id
             AND mapping.operation_id = binding.operation_id
             AND mapping.command_schema = binding.command_schema
            WHERE binding.state = 'enabled'
        )
        SELECT model.provider_id,
               model.provider_model_id,
               model.provider_model_display_name,
               model.media_kind,
               model.operation_id AS operation,
               mapping.command_schema,
               mapping.api_profile,
               mapping.public_model_id,
               mapping.route_id,
               COALESCE(capacity.routable_account_count, 0)::BIGINT
                   AS routable_account_count
        FROM model_operations model
        LEFT JOIN platform_mappings mapping
          ON mapping.provider_id = model.provider_id
         AND mapping.operation_id = model.operation_id
         AND mapping.provider_model_id = model.provider_model_id
         AND mapping.media_kind = model.media_kind
        LEFT JOIN LATERAL (
          SELECT count(DISTINCT member.provider_account_id)::BIGINT
                   AS routable_account_count
          FROM provider_route_members member
          JOIN provider_execution_profiles profile
            ON profile.execution_profile_id = member.execution_profile_id
           AND profile.provider_account_id = member.provider_account_id
           AND profile.provider_id = member.provider_id
           AND profile.operation_id = member.operation_id
           AND profile.command_schema = member.command_schema
          JOIN provider_accounts account
            ON account.provider_account_id = member.provider_account_id
          JOIN provider_account_environments environment
            ON environment.provider_account_id = member.provider_account_id
          JOIN executor_resource_policies policy
            ON policy.provider_account_id = member.provider_account_id
          WHERE member.route_id = mapping.route_id
            AND member.route_revision = mapping.route_revision
            AND member.state = 'enabled'
            AND profile.state = 'enabled'
            AND account.state = 'enabled'
            AND environment.state = 'active'
            AND policy.state = 'enabled'
            AND (
              NOT EXISTS (
                SELECT 1
                FROM provider_account_model_configurations configuration
                WHERE configuration.provider_account_id =
                      member.provider_account_id
                  AND configuration.provider_id = model.provider_id
                  AND configuration.mode = 'allowlist'
              )
              OR EXISTS (
                SELECT 1
                FROM provider_account_model_bindings binding
                WHERE binding.provider_account_id =
                      member.provider_account_id
                  AND binding.provider_id = model.provider_id
                  AND binding.model_id = model.provider_model_id
                  AND binding.media_kind = model.media_kind
              )
            )
        ) capacity ON mapping.route_id IS NOT NULL
        ORDER BY CASE model.provider_id
                   WHEN 'openai-codex' THEN 0
                   WHEN 'grok-cli' THEN 1
                   WHEN 'dreamina-cli' THEN 2
                   ELSE 3
                 END,
                 model.media_kind, model.provider_model_id,
                 model.operation_id, mapping.api_profile,
                 mapping.public_model_id
        "#,
    )
    .fetch_all(connection)
    .await
    .map_err(store_unavailable)
}

pub(super) async fn load_aliases(
    pool: &PgPool,
) -> Result<HashMap<String, String>, ImageGatewayError> {
    let mut connection = pool.acquire().await.map_err(store_unavailable)?;
    load_aliases_on(&mut connection).await
}

pub(super) async fn load_aliases_on(
    connection: &mut PgConnection,
) -> Result<HashMap<String, String>, ImageGatewayError> {
    Ok(sqlx::query_as::<_, ApiProfileAliasRow>(
        "SELECT api_profile, pricing_api_profile FROM api_profile_pricing_aliases",
    )
    .fetch_all(connection)
    .await
    .map_err(store_unavailable)?
    .into_iter()
    .map(|row| (row.api_profile, row.pricing_api_profile))
    .collect())
}

fn build_snapshot(
    catalog: &PriceBookCatalog,
    surfaces: &[CoverageSurfaceRow],
    aliases: &HashMap<String, String>,
) -> PricingCoverageSnapshot {
    let rows = surfaces
        .iter()
        .map(|surface| coverage_row(catalog, surface, aliases))
        .collect::<Vec<_>>();
    let summary = PricingCoverageSummary {
        surfaces: rows.len() as i64,
        routable_surfaces: rows
            .iter()
            .filter(|row| row.route_status == "routable")
            .count() as i64,
        sale_priced_surfaces: rows
            .iter()
            .filter(|row| row.customer_price_status == "ready")
            .count() as i64,
        actual_cost_surfaces: rows
            .iter()
            .filter(|row| row.provider_cost_status == "provider_actual")
            .count() as i64,
        benchmark_only_surfaces: rows
            .iter()
            .filter(|row| row.provider_cost_status == "benchmark_only")
            .count() as i64,
        blocked_surfaces: rows.iter().filter(|row| row.readiness == "blocked").count() as i64,
    };
    PricingCoverageSnapshot {
        as_of_ms: catalog.as_of_ms,
        scope: "platform_baseline".to_string(),
        summary,
        rows,
    }
}

fn coverage_row(
    catalog: &PriceBookCatalog,
    surface: &CoverageSurfaceRow,
    aliases: &HashMap<String, String>,
) -> PricingCoverageRow {
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
        });
    let pricing_dimensions = surface
        .command_schema
        .as_deref()
        .and_then(|command_schema| {
            super::admission::pricing_dimension_keys_for_route(
                &surface.provider_id,
                &surface.operation,
                command_schema,
                &surface.media_kind,
            )
        })
        .unwrap_or_default()
        .iter()
        .map(|dimension| (*dimension).to_string())
        .collect();
    let customer_metering_bases = surface
        .command_schema
        .as_deref()
        .and_then(|command_schema| {
            find_contract(
                &surface.provider_id,
                &surface.operation,
                command_schema,
                &surface.media_kind,
            )
        })
        .map(|contract| {
            contract
                .metering_bases
                .iter()
                .filter(|basis| basis.customer_sale_required)
                .map(|basis| PricingMeteringBasis {
                    metric: basis.metric.to_string(),
                    unit: basis.unit.to_string(),
                    quantity_source: basis.quantity_source.to_string(),
                    confidence: basis.confidence.to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    let route_status = if surface.route_id.is_none() {
        "missing"
    } else if surface.routable_account_count == 0 {
        "unavailable"
    } else {
        "routable"
    };
    let customer = resolve_matches(
        catalog,
        surface,
        aliases,
        "customer_sale",
        &["customer_rate"],
        pricing_operation,
        Some("USD"),
    );
    let provider_actual = resolve_matches(
        catalog,
        surface,
        aliases,
        "provider_actual",
        &["provider_reported"],
        pricing_operation,
        Some("USD"),
    );
    let provider_allocated = resolve_matches(
        catalog,
        surface,
        aliases,
        "provider_allocated",
        &["subscription_allocation", "membership_points"],
        pricing_operation,
        None,
    );
    let provider_estimated = resolve_matches(
        catalog,
        surface,
        aliases,
        "provider_estimated",
        &["published_rate", "contract_rate"],
        pricing_operation,
        None,
    );
    let provider_benchmark = resolve_matches(
        catalog,
        surface,
        aliases,
        "provider_benchmark",
        &["published_rate", "contract_rate"],
        pricing_operation,
        None,
    );

    let metering_status = metering_status(&customer, &surface.provider_id);
    let emits_actual_cost = surface.provider_id == "grok-cli";
    let (provider_cost_status, provider_cost_currencies) = if provider_actual.status == "ready" {
        ("provider_actual", provider_actual.currencies.clone())
    } else if provider_actual.status == "ambiguous" {
        ("ambiguous", provider_actual.currencies.clone())
    } else if emits_actual_cost {
        ("actual_price_missing", Vec::new())
    } else if provider_allocated.status == "ready" {
        ("provider_allocated", provider_allocated.currencies.clone())
    } else if provider_estimated.status == "ready" {
        ("provider_estimated", provider_estimated.currencies.clone())
    } else if provider_benchmark.status == "ready" {
        ("benchmark_only", provider_benchmark.currencies.clone())
    } else {
        ("not_emitted", Vec::new())
    };
    let source_status = source_status([
        &customer,
        &provider_actual,
        &provider_allocated,
        &provider_estimated,
        &provider_benchmark,
    ]);

    let mut blocking_reasons = Vec::new();
    match route_status {
        "missing" => blocking_reasons.push("platform_route_missing".to_string()),
        "unavailable" => blocking_reasons.push("routable_account_missing".to_string()),
        _ => {}
    }
    if surface.route_id.is_some() && pricing_operation.is_none() {
        blocking_reasons.push("pricing_admission_unsupported".to_string());
    }
    match customer.status {
        "missing" => blocking_reasons.push("customer_price_missing".to_string()),
        "ambiguous" => blocking_reasons.push("customer_price_ambiguous".to_string()),
        _ => {}
    }
    match metering_status {
        "missing" => blocking_reasons.push("metering_contract_missing".to_string()),
        "ambiguous" => blocking_reasons.push("metering_contract_ambiguous".to_string()),
        "incompatible" => blocking_reasons.push("metering_contract_incompatible".to_string()),
        "estimated" => blocking_reasons.push("metering_not_exact".to_string()),
        _ => {}
    }
    if provider_cost_status == "actual_price_missing" {
        blocking_reasons.push("provider_actual_price_missing".to_string());
    }
    let readiness = if !blocking_reasons.is_empty() {
        "blocked"
    } else if matches!(
        provider_cost_status,
        "not_emitted" | "benchmark_only" | "provider_estimated" | "ambiguous"
    ) || source_status == "manual"
    {
        "warning"
    } else {
        "ready"
    };

    PricingCoverageRow {
        provider_id: surface.provider_id.clone(),
        provider_display_name: provider_label(&surface.provider_id).to_string(),
        provider_model_id: surface.provider_model_id.clone(),
        provider_model_display_name: surface.provider_model_display_name.clone(),
        public_model_id: surface.public_model_id.clone(),
        api_profile: surface.api_profile.clone(),
        operation: surface.operation.clone(),
        pricing_operation: pricing_operation.map(str::to_string),
        pricing_dimensions,
        customer_metering_bases,
        media_kind: surface.media_kind.clone(),
        route_status: route_status.to_string(),
        routable_account_count: surface.routable_account_count,
        customer_price_status: customer.status.to_string(),
        customer_price_currencies: customer.currencies,
        metering_status: metering_status.to_string(),
        provider_cost_status: provider_cost_status.to_string(),
        provider_cost_currencies,
        source_status: source_status.to_string(),
        readiness: readiness.to_string(),
        blocking_reasons,
    }
}

fn resolve_matches<'a>(
    catalog: &'a PriceBookCatalog,
    surface: &CoverageSurfaceRow,
    aliases: &HashMap<String, String>,
    purpose: &str,
    billing_modes: &[&str],
    pricing_operation: Option<&str>,
    required_currency: Option<&str>,
) -> MatchResult<'a> {
    let Some(api_profile) = surface.api_profile.as_deref() else {
        return MatchResult {
            status: "missing",
            currencies: Vec::new(),
            versions: Vec::new(),
        };
    };
    let Some(public_model_id) = surface.public_model_id.as_deref() else {
        return MatchResult {
            status: "missing",
            currencies: Vec::new(),
            versions: Vec::new(),
        };
    };
    let Some(pricing_operation) = pricing_operation else {
        return MatchResult {
            status: "missing",
            currencies: Vec::new(),
            versions: Vec::new(),
        };
    };
    let pricing_profile = aliases
        .get(api_profile)
        .map(String::as_str)
        .unwrap_or(api_profile);
    let expected_provider_id =
        benchmark_provider_id(purpose, &surface.provider_id).unwrap_or(&surface.provider_id);
    let mut candidates: BTreeMap<String, Vec<(MatchRank, &PriceBookView, &PriceBookVersionView)>> =
        BTreeMap::new();

    for book in &catalog.price_books {
        if book.state != "active"
            || book.purpose != purpose
            || book.scope_type != "platform"
            || required_currency.is_some_and(|currency| book.currency != currency)
            || !matches_provider(book.provider_id.as_deref(), expected_provider_id)
        {
            continue;
        }
        for version in &book.versions {
            if !version_is_current(version, catalog.as_of_ms)
                || !billing_modes.contains(&version.billing_mode.as_str())
                || version.execution_surface != "provider_cli"
                || version.media_kind != surface.media_kind
                || !matches_provider(version.provider_id.as_deref(), expected_provider_id)
                || !matches_optional(
                    version.provider_model_id.as_deref(),
                    &surface.provider_model_id,
                )
                || !matches_value(&version.public_model_id, public_model_id)
                || !matches_value(&version.operation, pricing_operation)
                || !matches_value(&version.service_tier, "standard")
            {
                continue;
            }
            let api_profile_rank = if version.api_profile == api_profile {
                2
            } else if version.api_profile == pricing_profile {
                1
            } else if version.api_profile == "*" {
                0
            } else {
                continue;
            };
            candidates.entry(book.currency.clone()).or_default().push((
                MatchRank {
                    book_provider: u8::from(
                        book.provider_id.as_deref() == Some(expected_provider_id),
                    ),
                    version_provider: u8::from(
                        version.provider_id.as_deref() == Some(expected_provider_id),
                    ),
                    api_profile: api_profile_rank,
                    operation: u8::from(version.operation == pricing_operation),
                    provider_model: u8::from(
                        version.provider_model_id.as_deref()
                            == Some(surface.provider_model_id.as_str()),
                    ),
                    public_model: u8::from(version.public_model_id == public_model_id),
                    service_tier: u8::from(version.service_tier == "standard"),
                },
                book,
                version,
            ));
        }
    }

    let mut ambiguous = false;
    let mut currencies = Vec::new();
    let mut versions = Vec::new();
    for (currency, currency_candidates) in candidates {
        let Some(max_rank) = currency_candidates
            .iter()
            .map(|candidate| candidate.0)
            .max()
        else {
            continue;
        };
        let top = currency_candidates
            .into_iter()
            .filter(|candidate| candidate.0 == max_rank)
            .collect::<Vec<_>>();
        if top.len() != 1 {
            ambiguous = true;
            currencies.push(currency);
            continue;
        }
        currencies.push(currency);
        versions.push((top[0].1, top[0].2));
    }
    MatchResult {
        status: if ambiguous {
            "ambiguous"
        } else if versions.is_empty() {
            "missing"
        } else {
            "ready"
        },
        currencies,
        versions,
    }
}

fn metering_status(customer: &MatchResult<'_>, provider_id: &str) -> &'static str {
    if customer.status == "ambiguous" {
        return "ambiguous";
    }
    if customer.status != "ready" || customer.versions.is_empty() {
        return "missing";
    }
    for (_, version) in &customer.versions {
        match customer_metering_contract(version, provider_id) {
            CustomerMeteringContract::Exact => {}
            CustomerMeteringContract::Incompatible => return "incompatible",
        }
    }
    "exact"
}

fn source_status<const N: usize>(matches: [&MatchResult<'_>; N]) -> &'static str {
    let source_kinds = matches
        .into_iter()
        .flat_map(|result| {
            result
                .versions
                .iter()
                .map(|(_, version)| version.source_kind.as_str())
        })
        .collect::<BTreeSet<_>>();
    if source_kinds.is_empty() {
        "missing"
    } else if source_kinds
        .iter()
        .all(|source| matches!(*source, "official_document" | "provider_contract"))
    {
        "verified"
    } else {
        "manual"
    }
}

fn version_is_current(version: &PriceBookVersionView, as_of_ms: i64) -> bool {
    version.state == "active"
        && version.effective_from_ms <= as_of_ms
        && version
            .effective_until_ms
            .is_none_or(|until| as_of_ms < until)
}

fn matches_provider(candidate: Option<&str>, expected: &str) -> bool {
    candidate.is_none_or(|candidate| candidate == expected)
}

fn matches_optional(candidate: Option<&str>, expected: &str) -> bool {
    candidate.is_none_or(|candidate| candidate == expected)
}

fn matches_value(candidate: &str, expected: &str) -> bool {
    candidate == "*" || candidate == expected
}

fn benchmark_provider_id<'a>(purpose: &str, runtime_provider_id: &'a str) -> Option<&'a str> {
    match (purpose, runtime_provider_id) {
        ("provider_benchmark", "grok-cli") => Some("xai-grok"),
        _ => None,
    }
}

fn provider_label(provider_id: &str) -> &str {
    match provider_id {
        "openai-codex" => "Codex",
        "grok-cli" => "Grok",
        "dreamina-cli" => "即梦",
        _ => provider_id,
    }
}

fn store_unavailable(_: impl std::fmt::Display) -> ImageGatewayError {
    ImageGatewayError::service_unavailable("Pricing coverage unavailable")
}

#[cfg(test)]
mod tests {
    use super::benchmark_provider_id;

    #[test]
    fn runtime_provider_alias_is_limited_to_official_benchmark_matching() {
        assert_eq!(
            benchmark_provider_id("provider_benchmark", "grok-cli"),
            Some("xai-grok")
        );
        assert_eq!(benchmark_provider_id("provider_actual", "grok-cli"), None);
        assert_eq!(
            benchmark_provider_id("provider_benchmark", "openai-codex"),
            None
        );
    }
}
