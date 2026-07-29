use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use super::{PriceComponentView, ResolvedPriceVersion};

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, PartialEq)]
pub struct UsageFact {
    #[schema(value_type = String)]
    pub usage_fact_id: Uuid,
    pub partition_key: String,
    pub authority_key: String,
    pub provider_id: String,
    #[schema(value_type = Option<String>)]
    pub provider_account_id: Option<Uuid>,
    pub execution_surface: String,
    pub fact_domain: String,
    pub metric: String,
    pub unit: String,
    pub quantity: String,
    pub outcome: String,
    pub quantity_source: String,
    pub confidence: String,
    #[serde(default = "empty_object")]
    pub dimensions: Value,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct RatedLine {
    #[schema(value_type = String)]
    pub price_component_id: Uuid,
    pub component_key: String,
    pub partition_key: String,
    #[schema(value_type = Vec<String>)]
    pub usage_fact_ids: Vec<Uuid>,
    pub metric: String,
    pub unit: String,
    pub outcome: String,
    pub quantity: String,
    pub amount_micros: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct RatingResult {
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    pub currency: String,
    pub fact_set_hash: String,
    pub total_amount_micros: String,
    pub lines: Vec<RatedLine>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct ProviderReportedCostAggregate {
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    pub provider_id: String,
    pub execution_surface: String,
    pub currency: String,
    pub unit: String,
    pub fact_set_hash: String,
    pub quantity: String,
    #[schema(value_type = Vec<String>)]
    pub usage_fact_ids: Vec<Uuid>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct LedgerMoneyConversion {
    pub currency: String,
    pub native_unit: String,
    pub native_quantity: String,
    pub amount_micros: String,
    pub rounding_mode: String,
    pub rounding_delta_native_atoms: String,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RatingError {
    #[error("draft price versions cannot rate usage")]
    DraftVersion,
    #[error("at least one immutable usage fact is required")]
    EmptyFacts,
    #[error("usage quantity is invalid")]
    InvalidQuantity,
    #[error("usage dimensions must be an object")]
    InvalidDimensions,
    #[error("usage partition key is invalid")]
    InvalidPartition,
    #[error("usage authority key is invalid")]
    InvalidAuthorityKey,
    #[error("the same usage fact was supplied more than once")]
    DuplicateFact,
    #[error("usage provider does not match the resolved price")]
    ProviderMismatch,
    #[error("usage execution surface does not match the resolved price")]
    ExecutionSurfaceMismatch,
    #[error("usage fact domain does not match the resolved price purpose")]
    FactDomainMismatch,
    #[error("usage fact authority is insufficient for this price purpose")]
    InvalidAuthority,
    #[error("no price component matches {metric}/{unit}")]
    MissingRate { metric: String, unit: String },
    #[error("multiple equally specific price components match {metric}/{unit}")]
    AmbiguousRate { metric: String, unit: String },
    #[error("provider benchmark is missing the required {metric}/{unit} usage fact")]
    IncompleteBenchmark { metric: String, unit: String },
    #[error(
        "provider benchmark authority {authority_key} contains conflicting sources for {metric}/{unit}"
    )]
    ConflictingAuthority {
        authority_key: String,
        metric: String,
        unit: String,
    },
    #[error("price component is invalid")]
    InvalidComponent,
    #[error("paid price versions cannot silently produce a zero charge")]
    UnexpectedZeroCharge,
    #[error("provider-reported cost must retain its native monetary scale")]
    ProviderReportedCostRequiresAggregation,
    #[error("the resolved version is not a provider-reported actual-cost version")]
    NotProviderReportedCost,
    #[error("exact rounding cannot represent this amount in micros")]
    InexactAmount,
    #[error("rated amount overflowed")]
    Overflow,
}

struct RatingGroup<'a> {
    component: &'a PriceComponentView,
    partition_key: String,
    outcome: String,
    quantity: i128,
    usage_fact_ids: Vec<Uuid>,
}

pub fn rate_usage(
    resolved: &ResolvedPriceVersion,
    facts: &[UsageFact],
) -> Result<RatingResult, RatingError> {
    if resolved.version.state == "draft" {
        return Err(RatingError::DraftVersion);
    }
    if facts.is_empty() {
        return Err(RatingError::EmptyFacts);
    }
    if resolved.version.billing_mode == "provider_reported" {
        return Err(RatingError::ProviderReportedCostRequiresAggregation);
    }
    validate_price_components(resolved)?;

    let expected_provider = resolved
        .provider_id
        .as_deref()
        .or(resolved.version.provider_id.as_deref());
    let mut fact_ids = HashSet::with_capacity(facts.len());
    let mut groups: BTreeMap<(String, Uuid, String, String), RatingGroup<'_>> = BTreeMap::new();
    let required_benchmark_metrics = benchmark_metric_contract(resolved);
    let mut observed_benchmark_metrics = BTreeSet::new();
    let mut benchmark_authorities: BTreeMap<(String, String, String), String> = BTreeMap::new();
    for fact in facts {
        validate_fact(resolved, fact, expected_provider, &mut fact_ids)?;
        if fact.metric == "provider_reported_cost" {
            return Err(RatingError::ProviderReportedCostRequiresAggregation);
        }
        let quantity = parse_quantity(&fact.quantity)?;
        let fact_dimensions = fact
            .dimensions
            .as_object()
            .ok_or(RatingError::InvalidDimensions)?;
        let component = select_rate_component(
            resolved,
            &fact.metric,
            &fact.unit,
            &fact.quantity_source,
            &fact.confidence,
            &fact.outcome,
            fact_dimensions,
        )?
        .ok_or_else(|| RatingError::MissingRate {
            metric: fact.metric.clone(),
            unit: fact.unit.clone(),
        })?;
        if resolved.purpose == "provider_benchmark" {
            observed_benchmark_metrics.insert((fact.metric.clone(), fact.unit.clone()));
            let authority_key = (
                fact.authority_key.clone(),
                fact.metric.clone(),
                fact.unit.clone(),
            );
            if benchmark_authorities
                .insert(authority_key, fact.quantity_source.clone())
                .is_some_and(|authority| authority != fact.quantity_source)
            {
                return Err(RatingError::ConflictingAuthority {
                    authority_key: fact.authority_key.clone(),
                    metric: fact.metric.clone(),
                    unit: fact.unit.clone(),
                });
            }
        }

        let key = (
            component.component_key.clone(),
            component.price_component_id,
            fact.partition_key.clone(),
            fact.outcome.clone(),
        );
        let group = groups.entry(key).or_insert_with(|| RatingGroup {
            component,
            partition_key: fact.partition_key.clone(),
            outcome: fact.outcome.clone(),
            quantity: 0,
            usage_fact_ids: Vec::new(),
        });
        group.quantity = group
            .quantity
            .checked_add(quantity)
            .ok_or(RatingError::Overflow)?;
        group.usage_fact_ids.push(fact.usage_fact_id);
    }
    if let Some((metric, unit)) = required_benchmark_metrics
        .iter()
        .find(|required| !observed_benchmark_metrics.contains(*required))
    {
        return Err(RatingError::IncompleteBenchmark {
            metric: metric.clone(),
            unit: unit.clone(),
        });
    }

    let mut lines = Vec::with_capacity(groups.len());
    let mut total = 0_i128;
    for (_, mut group) in groups {
        group.usage_fact_ids.sort_unstable();
        let amount = rate_component(group.component, group.quantity)?;
        total = total.checked_add(amount).ok_or(RatingError::Overflow)?;
        lines.push(RatedLine {
            price_component_id: group.component.price_component_id,
            component_key: group.component.component_key.clone(),
            partition_key: group.partition_key,
            usage_fact_ids: group.usage_fact_ids,
            metric: group.component.metric.clone(),
            unit: group.component.unit.clone(),
            outcome: group.outcome,
            quantity: group.quantity.to_string(),
            amount_micros: amount.to_string(),
        });
    }
    if total == 0 && !resolved.version.is_free {
        return Err(RatingError::UnexpectedZeroCharge);
    }
    Ok(RatingResult {
        price_book_version_id: resolved.version.price_book_version_id,
        currency: resolved.currency.clone(),
        fact_set_hash: fact_set_hash(fact_ids.into_iter()),
        total_amount_micros: total.to_string(),
        lines,
    })
}

fn benchmark_metric_contract(resolved: &ResolvedPriceVersion) -> BTreeSet<(String, String)> {
    if resolved.purpose != "provider_benchmark" {
        return BTreeSet::new();
    }
    resolved
        .version
        .components
        .iter()
        .map(|component| (component.metric.clone(), component.unit.clone()))
        .collect()
}

pub fn aggregate_provider_reported_cost(
    resolved: &ResolvedPriceVersion,
    facts: &[UsageFact],
) -> Result<ProviderReportedCostAggregate, RatingError> {
    if resolved.version.state == "draft" {
        return Err(RatingError::DraftVersion);
    }
    if resolved.purpose != "provider_actual"
        || resolved.version.billing_mode != "provider_reported"
        || resolved.currency != "USD"
        || !resolved.version.components.is_empty()
    {
        return Err(RatingError::NotProviderReportedCost);
    }
    if facts.is_empty() {
        return Err(RatingError::EmptyFacts);
    }
    let expected_provider = resolved
        .provider_id
        .as_deref()
        .or(resolved.version.provider_id.as_deref());
    let mut fact_ids = HashSet::with_capacity(facts.len());
    let mut authority_keys = HashSet::with_capacity(facts.len());
    let mut quantity = 0_i128;
    for fact in facts {
        validate_fact(resolved, fact, expected_provider, &mut fact_ids)?;
        if fact.metric != "provider_reported_cost"
            || fact.unit != "usd_tick"
            || fact.quantity_source != "provider_reported"
            || fact.confidence != "exact"
        {
            return Err(RatingError::InvalidAuthority);
        }
        if !authority_keys.insert(fact.authority_key.clone()) {
            return Err(RatingError::DuplicateFact);
        }
        quantity = quantity
            .checked_add(parse_quantity(&fact.quantity)?)
            .ok_or(RatingError::Overflow)?;
    }
    let mut usage_fact_ids = fact_ids.into_iter().collect::<Vec<_>>();
    usage_fact_ids.sort_unstable();
    Ok(ProviderReportedCostAggregate {
        price_book_version_id: resolved.version.price_book_version_id,
        provider_id: expected_provider
            .ok_or(RatingError::ProviderMismatch)?
            .to_string(),
        execution_surface: resolved.version.execution_surface.clone(),
        currency: resolved.currency.clone(),
        unit: "usd_tick".to_string(),
        fact_set_hash: fact_set_hash(usage_fact_ids.iter().copied()),
        quantity: quantity.to_string(),
        usage_fact_ids,
    })
}

pub fn usd_ticks_to_ledger_micros(
    aggregate: &ProviderReportedCostAggregate,
) -> Result<LedgerMoneyConversion, RatingError> {
    const USD_TICKS_PER_MICRO: i128 = 10_000;

    if aggregate.currency != "USD" || aggregate.unit != "usd_tick" {
        return Err(RatingError::InvalidAuthority);
    }
    let ticks = parse_quantity(&aggregate.quantity)?;
    let micros = ticks / USD_TICKS_PER_MICRO;
    let remainder = ticks % USD_TICKS_PER_MICRO;
    let rounded_micros = micros
        .checked_add(i128::from(
            remainder.checked_mul(2).ok_or(RatingError::Overflow)? >= USD_TICKS_PER_MICRO,
        ))
        .ok_or(RatingError::Overflow)?;
    let represented_ticks = rounded_micros
        .checked_mul(USD_TICKS_PER_MICRO)
        .ok_or(RatingError::Overflow)?;
    let rounding_delta = represented_ticks
        .checked_sub(ticks)
        .ok_or(RatingError::Overflow)?;

    Ok(LedgerMoneyConversion {
        currency: aggregate.currency.clone(),
        native_unit: aggregate.unit.clone(),
        native_quantity: aggregate.quantity.clone(),
        amount_micros: rounded_micros.to_string(),
        rounding_mode: "half_up_after_aggregate".to_string(),
        rounding_delta_native_atoms: rounding_delta.to_string(),
    })
}

pub(super) fn validate_price_components(
    resolved: &ResolvedPriceVersion,
) -> Result<(), RatingError> {
    let mut has_positive = false;
    for component in &resolved.version.components {
        let price = component
            .unit_price_micros
            .parse::<i64>()
            .ok()
            .filter(|value| *value >= 0)
            .ok_or(RatingError::InvalidComponent)?;
        has_positive |= price > 0;
        if resolved.version.is_free && price != 0 {
            return Err(RatingError::InvalidComponent);
        }
    }
    if resolved.version.components.is_empty() || (!resolved.version.is_free && !has_positive) {
        return Err(RatingError::InvalidComponent);
    }
    Ok(())
}

fn validate_fact(
    resolved: &ResolvedPriceVersion,
    fact: &UsageFact,
    expected_provider: Option<&str>,
    seen: &mut HashSet<Uuid>,
) -> Result<(), RatingError> {
    if !seen.insert(fact.usage_fact_id) {
        return Err(RatingError::DuplicateFact);
    }
    if fact.partition_key.trim().is_empty()
        || fact.partition_key.len() > 128
        || fact.partition_key.chars().any(char::is_control)
    {
        return Err(RatingError::InvalidPartition);
    }
    if fact.authority_key.trim().is_empty()
        || fact.authority_key.len() > 512
        || fact.authority_key.chars().any(char::is_control)
    {
        return Err(RatingError::InvalidAuthorityKey);
    }
    if expected_provider.is_some_and(|provider| provider != fact.provider_id) {
        return Err(RatingError::ProviderMismatch);
    }
    if resolved.version.execution_surface != fact.execution_surface {
        return Err(RatingError::ExecutionSurfaceMismatch);
    }
    if expected_fact_domain(&resolved.purpose) != fact.fact_domain {
        return Err(RatingError::FactDomainMismatch);
    }
    if resolved.purpose == "provider_actual"
        && (fact.quantity_source != "provider_reported" || fact.confidence != "exact")
    {
        return Err(RatingError::InvalidAuthority);
    }
    Ok(())
}

fn expected_fact_domain(purpose: &str) -> &str {
    if purpose == "customer_sale" {
        "customer_billable"
    } else {
        purpose
    }
}

pub(super) fn confidence_satisfies(actual: &str, required: &str) -> bool {
    if required == "any" {
        return matches!(actual, "exact" | "bounded" | "estimated");
    }
    let rank = |value| match value {
        "exact" => Some(3),
        "bounded" => Some(2),
        "estimated" => Some(1),
        _ => None,
    };
    matches!((rank(actual), rank(required)), (Some(actual), Some(required)) if actual >= required)
}

pub(super) fn parse_quantity(value: &str) -> Result<i128, RatingError> {
    value
        .parse::<i128>()
        .ok()
        .filter(|quantity| *quantity >= 0)
        .ok_or(RatingError::InvalidQuantity)
}

pub(super) fn rate_component(
    component: &PriceComponentView,
    quantity: i128,
) -> Result<i128, RatingError> {
    rate_terms(
        &component.unit_size,
        &component.unit_price_micros,
        &component.rounding_mode,
        quantity,
    )
}

pub(super) fn rate_terms(
    unit_size: &str,
    unit_price_micros: &str,
    rounding_mode: &str,
    quantity: i128,
) -> Result<i128, RatingError> {
    let unit_size = unit_size
        .parse::<i128>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(RatingError::InvalidComponent)?;
    let unit_price = unit_price_micros
        .parse::<i128>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or(RatingError::InvalidComponent)?;
    let numerator = quantity
        .checked_mul(unit_price)
        .ok_or(RatingError::Overflow)?;
    let quotient = numerator / unit_size;
    let remainder = numerator % unit_size;
    match rounding_mode {
        "floor" => Ok(quotient),
        "ceil" => quotient
            .checked_add(i128::from(remainder != 0))
            .ok_or(RatingError::Overflow),
        "half_up" => quotient
            .checked_add(i128::from(
                remainder.checked_mul(2).ok_or(RatingError::Overflow)? >= unit_size,
            ))
            .ok_or(RatingError::Overflow),
        "exact" if remainder == 0 => Ok(quotient),
        "exact" => Err(RatingError::InexactAmount),
        _ => Err(RatingError::InvalidComponent),
    }
}

pub(super) fn select_rate_component<'a>(
    resolved: &'a ResolvedPriceVersion,
    metric: &str,
    unit: &str,
    quantity_source: &str,
    confidence: &str,
    outcome: &str,
    fact_dimensions: &serde_json::Map<String, Value>,
) -> Result<Option<&'a PriceComponentView>, RatingError> {
    let mut matches = resolved
        .version
        .components
        .iter()
        .filter_map(|component| {
            let dimensions = component.dimensions.as_object()?;
            let matches = component.metric == metric
                && component.unit == unit
                && component.quantity_source == quantity_source
                && confidence_satisfies(confidence, &component.required_confidence)
                && (component.outcome == "any" || component.outcome == outcome)
                && dimensions
                    .iter()
                    .all(|(key, value)| fact_dimensions.get(key) == Some(value));
            matches.then_some((dimensions.len(), component))
        })
        .collect::<Vec<_>>();
    matches.sort_by(|(left_specificity, left), (right_specificity, right)| {
        right_specificity
            .cmp(left_specificity)
            .then_with(|| left.component_key.cmp(&right.component_key))
    });
    let Some((best_specificity, component)) = matches.first().copied() else {
        return Ok(None);
    };
    if matches
        .get(1)
        .is_some_and(|(specificity, _)| *specificity == best_specificity)
    {
        return Err(RatingError::AmbiguousRate {
            metric: metric.to_string(),
            unit: unit.to_string(),
        });
    }
    Ok(Some(component))
}

pub(super) fn fact_set_hash(ids: impl IntoIterator<Item = Uuid>) -> String {
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    let mut digest = Sha256::new();
    for id in ids {
        digest.update(id.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn empty_object() -> Value {
    serde_json::json!({})
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::pricing::{PriceBookVersionView, PriceComponentView};

    #[test]
    fn openai_image_tokens_use_native_per_million_rate() {
        let price = resolved(
            "provider_benchmark",
            "published_rate",
            false,
            vec![component(
                "image_output",
                "image_output_token",
                "token",
                "1000000",
                "30000000",
                "exact",
                json!({}),
            )],
        );
        let result = rate_usage(
            &price,
            &[fact(
                "provider_benchmark",
                "image_output_token",
                "token",
                "100000",
                "provider_reported",
                "exact",
                json!({}),
            )],
        )
        .expect("token usage should rate");
        assert_eq!(result.total_amount_micros, "3000000");
    }

    #[test]
    fn xai_usd_ticks_remain_exact_native_atoms() {
        let price = resolved("provider_actual", "provider_reported", false, Vec::new());
        let result = aggregate_provider_reported_cost(
            &price,
            &[fact(
                "provider_actual",
                "provider_reported_cost",
                "usd_tick",
                "500000001",
                "provider_reported",
                "exact",
                json!({}),
            )],
        )
        .expect("USD ticks should aggregate without conversion");
        assert_eq!(result.quantity, "500000001");
        assert_eq!(result.unit, "usd_tick");
    }

    #[test]
    fn xai_ticks_convert_once_after_aggregation_and_retain_rounding_delta() {
        let price = resolved("provider_actual", "provider_reported", false, Vec::new());
        let first = fact(
            "provider_actual",
            "provider_reported_cost",
            "usd_tick",
            "4999",
            "provider_reported",
            "exact",
            json!({}),
        );
        let mut second = fact(
            "provider_actual",
            "provider_reported_cost",
            "usd_tick",
            "4999",
            "provider_reported",
            "exact",
            json!({}),
        );
        second.authority_key = "operation:1".to_string();
        let aggregate = aggregate_provider_reported_cost(&price, &[first, second])
            .expect("USD ticks should aggregate");
        let converted =
            usd_ticks_to_ledger_micros(&aggregate).expect("aggregate should convert to micros");

        assert_eq!(converted.native_quantity, "9998");
        assert_eq!(converted.amount_micros, "1");
        assert_eq!(converted.rounding_mode, "half_up_after_aggregate");
        assert_eq!(converted.rounding_delta_native_atoms, "2");
    }

    #[test]
    fn provider_reported_cost_rejects_duplicate_upstream_authority() {
        let price = resolved("provider_actual", "provider_reported", false, Vec::new());
        let first = fact(
            "provider_actual",
            "provider_reported_cost",
            "usd_tick",
            "10",
            "provider_reported",
            "exact",
            json!({}),
        );
        let mut duplicate = first.clone();
        duplicate.usage_fact_id = Uuid::new_v4();
        duplicate.partition_key = "another-partition".to_string();

        assert_eq!(
            aggregate_provider_reported_cost(&price, &[first, duplicate]),
            Err(RatingError::DuplicateFact)
        );
    }

    #[test]
    fn the_most_specific_dimension_rate_wins() {
        let price = resolved(
            "customer_sale",
            "customer_rate",
            false,
            vec![
                component(
                    "image_default",
                    "image_output",
                    "image",
                    "1",
                    "20000",
                    "exact",
                    json!({}),
                ),
                component(
                    "image_2k",
                    "image_output",
                    "image",
                    "1",
                    "70000",
                    "exact",
                    json!({"resolution": "2k"}),
                ),
            ],
        );
        let result = rate_usage(
            &price,
            &[fact(
                "customer_billable",
                "image_output",
                "image",
                "1",
                "request_derived",
                "exact",
                json!({"resolution": "2k"}),
            )],
        )
        .expect("specific rate should win");
        assert_eq!(result.total_amount_micros, "70000");
        assert_eq!(result.lines[0].component_key, "image_2k");
    }

    #[test]
    fn facts_are_deduplicated_by_immutable_identity() {
        let price = resolved(
            "customer_sale",
            "customer_rate",
            false,
            vec![component(
                "image",
                "image_output",
                "image",
                "1",
                "20000",
                "exact",
                json!({}),
            )],
        );
        let duplicate = fact(
            "customer_billable",
            "image_output",
            "image",
            "1",
            "request_derived",
            "exact",
            json!({}),
        );
        assert_eq!(
            rate_usage(&price, &[duplicate.clone(), duplicate]),
            Err(RatingError::DuplicateFact)
        );
    }

    #[test]
    fn quantities_in_the_same_partition_are_aggregated_before_rounding() {
        let price = resolved(
            "provider_benchmark",
            "published_rate",
            false,
            vec![component(
                "tokens",
                "image_output_token",
                "token",
                "1000",
                "1",
                "ceil",
                json!({}),
            )],
        );
        let result = rate_usage(
            &price,
            &[
                fact(
                    "provider_benchmark",
                    "image_output_token",
                    "token",
                    "1",
                    "provider_reported",
                    "exact",
                    json!({}),
                ),
                fact(
                    "provider_benchmark",
                    "image_output_token",
                    "token",
                    "1",
                    "provider_reported",
                    "exact",
                    json!({}),
                ),
            ],
        )
        .expect("facts should aggregate before rounding");
        assert_eq!(result.total_amount_micros, "1");
        assert_eq!(result.lines[0].quantity, "2");
    }

    #[test]
    fn separate_partitions_are_rounded_independently() {
        let price = resolved(
            "provider_benchmark",
            "published_rate",
            false,
            vec![component(
                "tokens",
                "image_output_token",
                "token",
                "1000",
                "1",
                "ceil",
                json!({}),
            )],
        );
        let mut first = fact(
            "provider_benchmark",
            "image_output_token",
            "token",
            "1",
            "provider_reported",
            "exact",
            json!({}),
        );
        first.partition_key = "output:0".to_string();
        let mut second = fact(
            "provider_benchmark",
            "image_output_token",
            "token",
            "1",
            "provider_reported",
            "exact",
            json!({}),
        );
        second.partition_key = "output:1".to_string();

        let result =
            rate_usage(&price, &[first, second]).expect("partitions should rate independently");

        assert_eq!(result.total_amount_micros, "2");
        assert_eq!(result.lines.len(), 2);
        assert_eq!(result.lines[0].quantity, "1");
        assert_eq!(result.lines[1].quantity, "1");
    }

    #[test]
    fn provider_benchmark_requires_every_declared_metric() {
        let price = resolved(
            "provider_benchmark",
            "published_rate",
            false,
            vec![
                component(
                    "text-input",
                    "text_input_token",
                    "token",
                    "1000000",
                    "5000000",
                    "exact",
                    json!({}),
                ),
                component(
                    "image-output",
                    "image_output_token",
                    "token",
                    "1000000",
                    "30000000",
                    "exact",
                    json!({}),
                ),
            ],
        );

        assert_eq!(
            rate_usage(
                &price,
                &[fact(
                    "provider_benchmark",
                    "image_output_token",
                    "token",
                    "7024",
                    "provider_reported",
                    "exact",
                    json!({}),
                )],
            ),
            Err(RatingError::IncompleteBenchmark {
                metric: "text_input_token".to_string(),
                unit: "token".to_string(),
            })
        );
    }

    #[test]
    fn provider_benchmark_rejects_multiple_authorities_for_one_metric() {
        let price = resolved(
            "provider_benchmark",
            "published_rate",
            false,
            vec![
                component(
                    "image-output-reported",
                    "image_output_token",
                    "token",
                    "1000000",
                    "30000000",
                    "exact",
                    json!({}),
                ),
                PriceComponentView {
                    quantity_source: "official_lookup".to_string(),
                    ..component(
                        "image-output-lookup",
                        "image_output_token",
                        "token",
                        "1000000",
                        "30000000",
                        "exact",
                        json!({}),
                    )
                },
            ],
        );

        let reported = fact(
            "provider_benchmark",
            "image_output_token",
            "token",
            "7024",
            "provider_reported",
            "exact",
            json!({}),
        );
        let mut lookup = fact(
            "provider_benchmark",
            "image_output_token",
            "token",
            "7024",
            "official_lookup",
            "exact",
            json!({}),
        );
        lookup.partition_key = "provider-output:0".to_string();
        assert_eq!(
            rate_usage(&price, &[reported, lookup]),
            Err(RatingError::ConflictingAuthority {
                authority_key: "output:0".to_string(),
                metric: "image_output_token".to_string(),
                unit: "token".to_string(),
            })
        );
    }

    #[test]
    fn provider_benchmark_accepts_explicit_zero_usage_for_required_metrics() {
        let price = resolved(
            "provider_benchmark",
            "published_rate",
            false,
            vec![
                PriceComponentView {
                    quantity_source: "request_derived".to_string(),
                    ..component(
                        "image-input",
                        "image_input",
                        "image",
                        "1",
                        "10000",
                        "exact",
                        json!({}),
                    )
                },
                PriceComponentView {
                    quantity_source: "request_derived".to_string(),
                    ..component(
                        "image-output",
                        "image_output",
                        "image",
                        "1",
                        "50000",
                        "exact",
                        json!({}),
                    )
                },
            ],
        );
        let result = rate_usage(
            &price,
            &[
                fact(
                    "provider_benchmark",
                    "image_input",
                    "image",
                    "0",
                    "request_derived",
                    "exact",
                    json!({}),
                ),
                fact(
                    "provider_benchmark",
                    "image_output",
                    "image",
                    "1",
                    "request_derived",
                    "exact",
                    json!({}),
                ),
            ],
        )
        .expect("zero usage should satisfy an explicit benchmark metric");

        assert_eq!(result.total_amount_micros, "50000");
        assert_eq!(result.lines.len(), 2);
    }

    #[test]
    fn empty_facts_and_silent_zero_prices_fail_closed() {
        let paid = resolved(
            "customer_sale",
            "customer_rate",
            false,
            vec![component(
                "image",
                "image_output",
                "image",
                "1",
                "0",
                "exact",
                json!({}),
            )],
        );
        assert_eq!(rate_usage(&paid, &[]), Err(RatingError::EmptyFacts));
        assert_eq!(
            rate_usage(
                &paid,
                &[fact(
                    "customer_billable",
                    "image_output",
                    "image",
                    "1",
                    "request_derived",
                    "exact",
                    json!({}),
                )],
            ),
            Err(RatingError::InvalidComponent)
        );
    }

    #[test]
    fn provider_actual_rejects_request_derived_quantities() {
        let price = resolved(
            "provider_actual",
            "contract_rate",
            false,
            vec![component(
                "seconds",
                "video_output_second",
                "second",
                "1",
                "80000",
                "exact",
                json!({}),
            )],
        );
        assert_eq!(
            rate_usage(
                &price,
                &[fact(
                    "provider_actual",
                    "video_output_second",
                    "second",
                    "5",
                    "request_derived",
                    "bounded",
                    json!({}),
                )],
            ),
            Err(RatingError::InvalidAuthority)
        );
    }

    fn fact(
        fact_domain: &str,
        metric: &str,
        unit: &str,
        quantity: &str,
        quantity_source: &str,
        confidence: &str,
        dimensions: Value,
    ) -> UsageFact {
        UsageFact {
            usage_fact_id: Uuid::new_v4(),
            partition_key: "request".to_string(),
            authority_key: "output:0".to_string(),
            provider_id: "xai-grok".to_string(),
            provider_account_id: Some(Uuid::new_v4()),
            execution_surface: "provider_api".to_string(),
            fact_domain: fact_domain.to_string(),
            metric: metric.to_string(),
            unit: unit.to_string(),
            quantity: quantity.to_string(),
            outcome: "succeeded".to_string(),
            quantity_source: quantity_source.to_string(),
            confidence: confidence.to_string(),
            dimensions,
        }
    }

    fn component(
        key: &str,
        metric: &str,
        unit: &str,
        unit_size: &str,
        unit_price_micros: &str,
        rounding_mode: &str,
        dimensions: Value,
    ) -> PriceComponentView {
        let quantity_source = if metric == "video_output_second" {
            "provider_reported"
        } else if metric == "image_output" {
            "request_derived"
        } else {
            "provider_reported"
        };
        PriceComponentView {
            price_component_id: Uuid::new_v4(),
            component_key: key.to_string(),
            metric: metric.to_string(),
            unit: unit.to_string(),
            unit_size: unit_size.to_string(),
            unit_price_micros: unit_price_micros.to_string(),
            outcome: "succeeded".to_string(),
            quantity_source: quantity_source.to_string(),
            required_confidence: "exact".to_string(),
            rounding_mode: rounding_mode.to_string(),
            dimensions,
            created_at_ms: 1,
        }
    }

    fn resolved(
        purpose: &str,
        billing_mode: &str,
        is_free: bool,
        components: Vec<PriceComponentView>,
    ) -> ResolvedPriceVersion {
        let price_book_id = Uuid::new_v4();
        ResolvedPriceVersion {
            price_book_id,
            price_book_key: "test".to_string(),
            purpose: purpose.to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: Some("xai-grok".to_string()),
            currency: "USD".to_string(),
            version: PriceBookVersionView {
                price_book_version_id: Uuid::new_v4(),
                price_book_id,
                version: 1,
                api_profile: "test".to_string(),
                operation: "test".to_string(),
                provider_id: Some("xai-grok".to_string()),
                provider_model_id: None,
                public_model_id: "test".to_string(),
                media_kind: "image".to_string(),
                service_tier: "standard".to_string(),
                execution_surface: "provider_api".to_string(),
                billing_mode: billing_mode.to_string(),
                is_free,
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
                components,
            },
        }
    }
}
