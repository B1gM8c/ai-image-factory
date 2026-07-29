use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use super::{
    PriceComponentView, RatingError, ResolvedPriceVersion, UsageFact,
    rating::{
        confidence_satisfies, fact_set_hash, parse_quantity, rate_component, rate_terms,
        select_rate_component, validate_price_components,
    },
};

const TERMINAL_OUTCOMES: [&str; 3] = ["succeeded", "failed", "no_effect"];

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QuoteQuantity {
    pub partition_key: String,
    pub metric: String,
    pub unit: String,
    pub max_quantity: String,
    pub reservation_quantity_source: String,
    pub reservation_confidence: String,
    pub settlement_quantity_source: String,
    pub settlement_confidence: String,
    #[serde(default = "empty_object")]
    pub dimensions: Value,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct FrozenQuoteLine {
    #[schema(value_type = String)]
    pub price_component_id: Uuid,
    pub component_key: String,
    pub partition_key: String,
    pub terminal_outcome: String,
    pub metric: String,
    pub unit: String,
    pub unit_size: String,
    pub unit_price_micros: String,
    pub rate_adjustment_numerator: String,
    pub rate_adjustment_denominator: String,
    pub reservation_quantity_source: String,
    pub reservation_confidence: String,
    pub quantity_source: String,
    pub required_confidence: String,
    pub rounding_mode: String,
    pub dimensions: Value,
    pub max_quantity: String,
    pub max_amount_micros: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct FrozenQuotePlan {
    #[schema(value_type = String)]
    pub price_book_id: Uuid,
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    pub currency: String,
    pub is_free: bool,
    pub max_total_micros: String,
    pub quote_hash: String,
    pub lines: Vec<FrozenQuoteLine>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct FrozenRatedLine {
    #[schema(value_type = String)]
    pub price_component_id: Uuid,
    pub component_key: String,
    pub partition_key: String,
    pub terminal_outcome: String,
    #[schema(value_type = Vec<String>)]
    pub usage_fact_ids: Vec<Uuid>,
    pub metric: String,
    pub unit: String,
    pub actual_quantity: String,
    pub amount_micros: String,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct FrozenRatingPlan {
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    pub currency: String,
    pub fact_set_hash: String,
    pub rating_hash: String,
    pub total_amount_micros: String,
    pub lines: Vec<FrozenRatedLine>,
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum QuoteError {
    #[error("only an active customer-sale customer-rate version can quote a customer request")]
    InvalidPricePurpose,
    #[error("at least one maximum usage quantity is required")]
    EmptyQuantities,
    #[error("quote partition key is invalid")]
    InvalidPartition,
    #[error("quote dimensions must be an object")]
    InvalidDimensions,
    #[error("maximum quantities merged into one rate require one reservation authority")]
    InconsistentReservationAuthority,
    #[error("at least one immutable usage fact is required")]
    EmptyFacts,
    #[error("the same immutable usage fact was supplied more than once")]
    DuplicateFact,
    #[error("usage facts do not cover every frozen billing partition")]
    IncompletePartitions,
    #[error("usage facts do not cover every frozen component for the terminal outcome")]
    IncompleteComponents,
    #[error("one billing partition cannot contain multiple terminal outcomes")]
    InconsistentPartitionOutcome,
    #[error("usage exceeds the frozen maximum quantity")]
    QuantityExceedsQuote,
    #[error("multiple frozen quote lines match one immutable usage fact")]
    AmbiguousFrozenLine,
    #[error("no price component matches {metric}/{unit}")]
    MissingRate { metric: String, unit: String },
    #[error("paid price versions cannot silently produce a zero maximum charge")]
    UnexpectedZeroQuote,
    #[error("the requested price adjustment cannot be represented exactly")]
    InexactAdjustment,
    #[error(transparent)]
    Rating(#[from] RatingError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuoteRateAdjustment {
    pub numerator: u64,
    pub denominator: u64,
}

impl QuoteRateAdjustment {
    pub const IDENTITY: Self = Self {
        numerator: 1,
        denominator: 1,
    };
    pub const BATCH_FIFTY_PERCENT: Self = Self {
        numerator: 1,
        denominator: 2,
    };
}

struct QuoteGroup<'a> {
    component: &'a PriceComponentView,
    partition_key: String,
    terminal_outcome: String,
    reservation_quantity_source: String,
    reservation_confidence: String,
    quantity: i128,
}

struct FrozenRatingGroup<'a> {
    line: &'a FrozenQuoteLine,
    quantity: i128,
    usage_fact_ids: Vec<Uuid>,
}

pub fn plan_customer_quote(
    resolved: &ResolvedPriceVersion,
    quantities: &[QuoteQuantity],
) -> Result<FrozenQuotePlan, QuoteError> {
    plan_customer_quote_with_adjustment(resolved, quantities, None)
}

pub fn plan_customer_quote_with_adjustment(
    resolved: &ResolvedPriceVersion,
    quantities: &[QuoteQuantity],
    adjustment: Option<QuoteRateAdjustment>,
) -> Result<FrozenQuotePlan, QuoteError> {
    if resolved.purpose != "customer_sale"
        || resolved.version.billing_mode != "customer_rate"
        || resolved.version.state != "active"
    {
        return Err(QuoteError::InvalidPricePurpose);
    }
    if quantities.is_empty() {
        return Err(QuoteError::EmptyQuantities);
    }
    validate_price_components(resolved)?;

    let mut partitions: BTreeMap<&str, Vec<(&QuoteQuantity, i128)>> = BTreeMap::new();
    for quantity in quantities {
        if quantity.partition_key.trim().is_empty()
            || quantity.partition_key.len() > 128
            || quantity.partition_key.chars().any(char::is_control)
        {
            return Err(QuoteError::InvalidPartition);
        }
        let dimensions = quantity
            .dimensions
            .as_object()
            .ok_or(QuoteError::InvalidDimensions)?;
        let parsed_quantity = parse_quantity(&quantity.max_quantity)?;
        if parsed_quantity == 0 {
            continue;
        }
        let mut matched_any_outcome = false;
        for outcome in TERMINAL_OUTCOMES {
            matched_any_outcome |= select_rate_component(
                resolved,
                &quantity.metric,
                &quantity.unit,
                &quantity.settlement_quantity_source,
                &quantity.settlement_confidence,
                outcome,
                dimensions,
            )?
            .is_some();
        }
        if !matched_any_outcome {
            return Err(QuoteError::MissingRate {
                metric: quantity.metric.clone(),
                unit: quantity.unit.clone(),
            });
        }
        partitions
            .entry(quantity.partition_key.as_str())
            .or_default()
            .push((quantity, parsed_quantity));
    }
    if partitions.is_empty() {
        return Err(QuoteError::EmptyQuantities);
    }

    let mut groups: BTreeMap<(String, String, String, Uuid), QuoteGroup<'_>> = BTreeMap::new();
    for (partition_key, partition_quantities) in partitions {
        for terminal_outcome in TERMINAL_OUTCOMES {
            for (quantity, parsed_quantity) in &partition_quantities {
                let dimensions = quantity
                    .dimensions
                    .as_object()
                    .ok_or(QuoteError::InvalidDimensions)?;
                let Some(component) = select_rate_component(
                    resolved,
                    &quantity.metric,
                    &quantity.unit,
                    &quantity.settlement_quantity_source,
                    &quantity.settlement_confidence,
                    terminal_outcome,
                    dimensions,
                )?
                else {
                    continue;
                };
                let key = (
                    partition_key.to_string(),
                    terminal_outcome.to_string(),
                    component.component_key.clone(),
                    component.price_component_id,
                );
                let group = groups.entry(key).or_insert_with(|| QuoteGroup {
                    component,
                    partition_key: partition_key.to_string(),
                    terminal_outcome: terminal_outcome.to_string(),
                    reservation_quantity_source: quantity.reservation_quantity_source.clone(),
                    reservation_confidence: quantity.reservation_confidence.clone(),
                    quantity: 0,
                });
                if group.reservation_quantity_source != quantity.reservation_quantity_source
                    || group.reservation_confidence != quantity.reservation_confidence
                {
                    return Err(QuoteError::InconsistentReservationAuthority);
                }
                group.quantity = group
                    .quantity
                    .checked_add(*parsed_quantity)
                    .ok_or(RatingError::Overflow)?;
            }
        }
    }

    let adjustment = adjustment.unwrap_or(QuoteRateAdjustment::IDENTITY);
    let mut partition_totals: BTreeMap<(String, String), i128> = BTreeMap::new();
    let mut lines = Vec::with_capacity(groups.len());
    for (_, group) in groups {
        let adjusted_component = adjusted_component(group.component, adjustment)?;
        let amount = rate_component(&adjusted_component, group.quantity)?;
        let total = partition_totals
            .entry((group.partition_key.clone(), group.terminal_outcome.clone()))
            .or_default();
        *total = total.checked_add(amount).ok_or(RatingError::Overflow)?;
        lines.push(FrozenQuoteLine {
            price_component_id: group.component.price_component_id,
            component_key: group.component.component_key.clone(),
            partition_key: group.partition_key,
            terminal_outcome: group.terminal_outcome,
            metric: group.component.metric.clone(),
            unit: group.component.unit.clone(),
            unit_size: group.component.unit_size.clone(),
            unit_price_micros: group.component.unit_price_micros.clone(),
            rate_adjustment_numerator: adjustment.numerator.to_string(),
            rate_adjustment_denominator: adjustment.denominator.to_string(),
            reservation_quantity_source: group.reservation_quantity_source,
            reservation_confidence: group.reservation_confidence,
            quantity_source: group.component.quantity_source.clone(),
            required_confidence: group.component.required_confidence.clone(),
            rounding_mode: group.component.rounding_mode.clone(),
            dimensions: group.component.dimensions.clone(),
            max_quantity: group.quantity.to_string(),
            max_amount_micros: amount.to_string(),
        });
    }
    lines.sort_by(|left, right| {
        left.partition_key
            .cmp(&right.partition_key)
            .then_with(|| left.terminal_outcome.cmp(&right.terminal_outcome))
            .then_with(|| left.component_key.cmp(&right.component_key))
            .then_with(|| left.price_component_id.cmp(&right.price_component_id))
    });

    let mut max_by_partition: BTreeMap<&str, i128> = BTreeMap::new();
    for ((partition, _), amount) in &partition_totals {
        let max = max_by_partition.entry(partition.as_str()).or_default();
        *max = (*max).max(*amount);
    }
    let max_total_micros = max_by_partition
        .values()
        .try_fold(0_i128, |total, amount| {
            total.checked_add(*amount).ok_or(RatingError::Overflow)
        })?;
    if max_total_micros == 0 && !resolved.version.is_free {
        return Err(QuoteError::UnexpectedZeroQuote);
    }
    let quote_hash = quote_hash(resolved, max_total_micros, &lines, quantities);
    Ok(FrozenQuotePlan {
        price_book_id: resolved.price_book_id,
        price_book_version_id: resolved.version.price_book_version_id,
        currency: resolved.currency.clone(),
        is_free: resolved.version.is_free,
        max_total_micros: max_total_micros.to_string(),
        quote_hash,
        lines,
    })
}

fn adjusted_component(
    component: &PriceComponentView,
    adjustment: QuoteRateAdjustment,
) -> Result<PriceComponentView, QuoteError> {
    if adjustment.numerator == 0 || adjustment.denominator == 0 {
        return Err(QuoteError::InexactAdjustment);
    }
    let unit_price = component
        .unit_price_micros
        .parse::<u128>()
        .map_err(|_| QuoteError::InexactAdjustment)?;
    let adjusted = unit_price
        .checked_mul(u128::from(adjustment.numerator))
        .ok_or(QuoteError::Rating(RatingError::Overflow))?;
    let denominator = u128::from(adjustment.denominator);
    if adjusted % denominator != 0 {
        return Err(QuoteError::InexactAdjustment);
    }
    let mut adjusted_component = component.clone();
    adjusted_component.unit_price_micros = (adjusted / denominator).to_string();
    Ok(adjusted_component)
}

pub fn rate_frozen_customer_quote(
    quote: &FrozenQuotePlan,
    facts: &[UsageFact],
) -> Result<FrozenRatingPlan, QuoteError> {
    if facts.is_empty() {
        return Err(QuoteError::EmptyFacts);
    }
    let expected_partitions = quote
        .lines
        .iter()
        .map(|line| line.partition_key.as_str())
        .collect::<BTreeSet<_>>();
    let mut actual_partitions = BTreeSet::new();
    let mut partition_outcomes = BTreeMap::new();
    let mut fact_ids = HashSet::with_capacity(facts.len());
    let mut groups: BTreeMap<(String, String, String, Uuid), FrozenRatingGroup<'_>> =
        BTreeMap::new();

    for fact in facts {
        if !fact_ids.insert(fact.usage_fact_id) {
            return Err(QuoteError::DuplicateFact);
        }
        if fact.partition_key.trim().is_empty()
            || fact.partition_key.len() > 128
            || fact.partition_key.chars().any(char::is_control)
        {
            return Err(QuoteError::Rating(RatingError::InvalidPartition));
        }
        actual_partitions.insert(fact.partition_key.as_str());
        if partition_outcomes
            .insert(fact.partition_key.as_str(), fact.outcome.as_str())
            .is_some_and(|outcome| outcome != fact.outcome)
        {
            return Err(QuoteError::InconsistentPartitionOutcome);
        }
        let quantity = parse_quantity(&fact.quantity)?;
        let dimensions = fact
            .dimensions
            .as_object()
            .ok_or(QuoteError::InvalidDimensions)?;
        let mut matches = quote
            .lines
            .iter()
            .filter_map(|line| {
                let line_dimensions = line.dimensions.as_object()?;
                let matches = line.partition_key == fact.partition_key
                    && line.terminal_outcome == fact.outcome
                    && line.metric == fact.metric
                    && line.unit == fact.unit
                    && line.quantity_source == fact.quantity_source
                    && confidence_satisfies(&fact.confidence, &line.required_confidence)
                    && line_dimensions
                        .iter()
                        .all(|(key, value)| dimensions.get(key) == Some(value));
                matches.then_some((line_dimensions.len(), line))
            })
            .collect::<Vec<_>>();
        matches.sort_by(|(left_specificity, left), (right_specificity, right)| {
            right_specificity
                .cmp(left_specificity)
                .then_with(|| left.component_key.cmp(&right.component_key))
                .then_with(|| left.price_component_id.cmp(&right.price_component_id))
        });
        let Some((best_specificity, line)) = matches.first().copied() else {
            return Err(QuoteError::MissingRate {
                metric: fact.metric.clone(),
                unit: fact.unit.clone(),
            });
        };
        if matches
            .get(1)
            .is_some_and(|(specificity, _)| *specificity == best_specificity)
        {
            return Err(QuoteError::AmbiguousFrozenLine);
        }
        let key = (
            line.partition_key.clone(),
            line.terminal_outcome.clone(),
            line.component_key.clone(),
            line.price_component_id,
        );
        let group = groups.entry(key).or_insert_with(|| FrozenRatingGroup {
            line,
            quantity: 0,
            usage_fact_ids: Vec::new(),
        });
        group.quantity = group
            .quantity
            .checked_add(quantity)
            .ok_or(RatingError::Overflow)?;
        group.usage_fact_ids.push(fact.usage_fact_id);
    }

    if actual_partitions != expected_partitions {
        return Err(QuoteError::IncompletePartitions);
    }
    let expected_groups = quote
        .lines
        .iter()
        .filter(|line| {
            partition_outcomes
                .get(line.partition_key.as_str())
                .is_some_and(|outcome| *outcome == line.terminal_outcome)
        })
        .map(|line| {
            (
                line.partition_key.clone(),
                line.terminal_outcome.clone(),
                line.component_key.clone(),
                line.price_component_id,
            )
        })
        .collect::<BTreeSet<_>>();
    let actual_groups = groups.keys().cloned().collect::<BTreeSet<_>>();
    if actual_groups != expected_groups {
        return Err(QuoteError::IncompleteComponents);
    }

    let mut total = 0_i128;
    let mut lines = Vec::with_capacity(groups.len());
    for (_, mut group) in groups {
        let maximum = parse_quantity(&group.line.max_quantity)?;
        if group.quantity > maximum {
            return Err(QuoteError::QuantityExceedsQuote);
        }
        let adjusted_unit_price = adjusted_unit_price(
            &group.line.unit_price_micros,
            &group.line.rate_adjustment_numerator,
            &group.line.rate_adjustment_denominator,
        )?;
        let amount = rate_terms(
            &group.line.unit_size,
            &adjusted_unit_price,
            &group.line.rounding_mode,
            group.quantity,
        )?;
        total = total.checked_add(amount).ok_or(RatingError::Overflow)?;
        group.usage_fact_ids.sort_unstable();
        lines.push(FrozenRatedLine {
            price_component_id: group.line.price_component_id,
            component_key: group.line.component_key.clone(),
            partition_key: group.line.partition_key.clone(),
            terminal_outcome: group.line.terminal_outcome.clone(),
            usage_fact_ids: group.usage_fact_ids,
            metric: group.line.metric.clone(),
            unit: group.line.unit.clone(),
            actual_quantity: group.quantity.to_string(),
            amount_micros: amount.to_string(),
        });
    }
    let quote_maximum = parse_quantity(&quote.max_total_micros)?;
    if total > quote_maximum {
        return Err(QuoteError::QuantityExceedsQuote);
    }
    if total == 0
        && !quote.is_free
        && lines
            .iter()
            .any(|line| line.terminal_outcome == "succeeded")
    {
        return Err(QuoteError::UnexpectedZeroQuote);
    }
    let facts_hash = fact_set_hash(fact_ids);
    let rating_hash = frozen_rating_hash(quote, &facts_hash, total, &lines);
    Ok(FrozenRatingPlan {
        price_book_version_id: quote.price_book_version_id,
        currency: quote.currency.clone(),
        fact_set_hash: facts_hash,
        rating_hash,
        total_amount_micros: total.to_string(),
        lines,
    })
}

fn quote_hash(
    resolved: &ResolvedPriceVersion,
    max_total_micros: i128,
    lines: &[FrozenQuoteLine],
    quantities: &[QuoteQuantity],
) -> String {
    let mut digest = Sha256::new();
    hash_part(&mut digest, resolved.price_book_id.as_bytes());
    hash_part(
        &mut digest,
        resolved.version.price_book_version_id.as_bytes(),
    );
    hash_part(&mut digest, resolved.currency.as_bytes());
    hash_part(&mut digest, resolved.version.is_free.to_string().as_bytes());
    hash_part(&mut digest, max_total_micros.to_string().as_bytes());
    let mut frozen_quantities = quantities
        .iter()
        .map(|quantity| serde_json::to_vec(quantity).unwrap_or_default())
        .collect::<Vec<_>>();
    frozen_quantities.sort();
    for quantity in frozen_quantities {
        hash_part(&mut digest, &quantity);
    }
    for line in lines {
        hash_part(&mut digest, line.price_component_id.as_bytes());
        for part in [
            line.component_key.as_str(),
            line.partition_key.as_str(),
            line.terminal_outcome.as_str(),
            line.metric.as_str(),
            line.unit.as_str(),
            line.unit_size.as_str(),
            line.unit_price_micros.as_str(),
            line.rate_adjustment_numerator.as_str(),
            line.rate_adjustment_denominator.as_str(),
            line.reservation_quantity_source.as_str(),
            line.reservation_confidence.as_str(),
            line.quantity_source.as_str(),
            line.required_confidence.as_str(),
            line.rounding_mode.as_str(),
            line.max_quantity.as_str(),
            line.max_amount_micros.as_str(),
        ] {
            hash_part(&mut digest, part.as_bytes());
        }
        let dimensions = serde_json::to_vec(&line.dimensions).unwrap_or_default();
        hash_part(&mut digest, &dimensions);
    }
    hex::encode(digest.finalize())
}

fn adjusted_unit_price(
    unit_price_micros: &str,
    numerator: &str,
    denominator: &str,
) -> Result<String, QuoteError> {
    let unit_price = unit_price_micros
        .parse::<u128>()
        .map_err(|_| QuoteError::InexactAdjustment)?;
    let numerator = numerator
        .parse::<u128>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(QuoteError::InexactAdjustment)?;
    let denominator = denominator
        .parse::<u128>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(QuoteError::InexactAdjustment)?;
    let adjusted = unit_price
        .checked_mul(numerator)
        .ok_or(QuoteError::Rating(RatingError::Overflow))?;
    if adjusted % denominator != 0 {
        return Err(QuoteError::InexactAdjustment);
    }
    Ok((adjusted / denominator).to_string())
}

fn frozen_rating_hash(
    quote: &FrozenQuotePlan,
    fact_set_hash: &str,
    total_amount_micros: i128,
    lines: &[FrozenRatedLine],
) -> String {
    let mut digest = Sha256::new();
    hash_part(&mut digest, quote.quote_hash.as_bytes());
    hash_part(&mut digest, fact_set_hash.as_bytes());
    hash_part(&mut digest, total_amount_micros.to_string().as_bytes());
    for line in lines {
        hash_part(&mut digest, line.price_component_id.as_bytes());
        for part in [
            line.component_key.as_str(),
            line.partition_key.as_str(),
            line.terminal_outcome.as_str(),
            line.metric.as_str(),
            line.unit.as_str(),
            line.actual_quantity.as_str(),
            line.amount_micros.as_str(),
        ] {
            hash_part(&mut digest, part.as_bytes());
        }
        for usage_fact_id in &line.usage_fact_ids {
            hash_part(&mut digest, usage_fact_id.as_bytes());
        }
    }
    hex::encode(digest.finalize())
}

fn hash_part(digest: &mut Sha256, part: &[u8]) {
    digest.update((part.len() as u64).to_be_bytes());
    digest.update(part);
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
    fn batch_adjustment_freezes_half_price_without_changing_sync_quotes() {
        let price = resolved(vec![component(
            "output",
            "image_output",
            "image",
            "1",
            "50000",
            "succeeded",
            "request_derived",
            "exact",
            json!({}),
        )]);
        let quantities = [quantity(
            "output:0",
            "image_output",
            "image",
            "1",
            json!({}),
        )];
        let synchronous = plan_customer_quote(&price, &quantities).expect("sync quote should plan");
        let batch = plan_customer_quote_with_adjustment(
            &price,
            &quantities,
            Some(QuoteRateAdjustment::BATCH_FIFTY_PERCENT),
        )
        .expect("batch quote should plan");

        assert_eq!(synchronous.max_total_micros, "50000");
        assert_eq!(batch.max_total_micros, "25000");
        assert_eq!(batch.lines[0].unit_price_micros, "50000");
        assert_eq!(batch.lines[0].rate_adjustment_numerator, "1");
        assert_eq!(batch.lines[0].rate_adjustment_denominator, "2");
        let rating = rate_frozen_customer_quote(
            &batch,
            &[usage_fact(
                "output:0",
                "image_output",
                "image",
                "1",
                json!({}),
            )],
        )
        .expect("batch quote should settle with the frozen adjustment");
        assert_eq!(rating.total_amount_micros, "25000");
        assert_ne!(synchronous.quote_hash, batch.quote_hash);
    }

    #[test]
    fn batch_adjustment_rejects_unrepresentable_sub_micro_prices() {
        let price = resolved(vec![component(
            "output",
            "image_output",
            "image",
            "1",
            "1",
            "succeeded",
            "request_derived",
            "exact",
            json!({}),
        )]);
        let result = plan_customer_quote_with_adjustment(
            &price,
            &[quantity(
                "output:0",
                "image_output",
                "image",
                "1",
                json!({}),
            )],
            Some(QuoteRateAdjustment::BATCH_FIFTY_PERCENT),
        );

        assert_eq!(result, Err(QuoteError::InexactAdjustment));
    }

    #[test]
    fn openai_multi_component_tokens_reserve_the_success_total() {
        let price = resolved(vec![
            component(
                "text_input",
                "text_input_token",
                "token",
                "1000000",
                "5000000",
                "any",
                "provider_reported",
                "half_up",
                json!({}),
            ),
            component(
                "cached_text",
                "cached_text_input_token",
                "token",
                "1000000",
                "1250000",
                "any",
                "provider_reported",
                "half_up",
                json!({}),
            ),
            component(
                "image_output",
                "image_output_token",
                "token",
                "1000000",
                "30000000",
                "succeeded",
                "provider_reported",
                "half_up",
                json!({}),
            ),
        ]);
        let quote = plan_customer_quote(
            &price,
            &[
                quantity("output:0", "text_input_token", "token", "100000", json!({})),
                quantity(
                    "output:0",
                    "cached_text_input_token",
                    "token",
                    "20000",
                    json!({}),
                ),
                quantity(
                    "output:0",
                    "image_output_token",
                    "token",
                    "100000",
                    json!({}),
                ),
            ],
        )
        .expect("multi-component quote should plan");

        assert_eq!(quote.max_total_micros, "3525000");
        assert_eq!(
            quote
                .lines
                .iter()
                .filter(|line| line.terminal_outcome == "succeeded")
                .count(),
            3
        );
        assert!(quote.lines.iter().all(|line| {
            line.reservation_quantity_source == "official_lookup"
                && line.reservation_confidence == "bounded"
                && line.quantity_source == "provider_reported"
                && line.required_confidence == "exact"
        }));
    }

    #[test]
    fn each_output_partition_contributes_its_own_rounding_upper_bound() {
        let price = resolved(vec![component(
            "output",
            "image_output",
            "image",
            "3",
            "1",
            "succeeded",
            "request_derived",
            "ceil",
            json!({}),
        )]);
        let quote = plan_customer_quote(
            &price,
            &[
                quantity("output:0", "image_output", "image", "1", json!({})),
                quantity("output:1", "image_output", "image", "1", json!({})),
            ],
        )
        .expect("partitioned quote should plan");

        assert_eq!(quote.max_total_micros, "2");
    }

    #[test]
    fn frozen_rating_uses_terminal_facts_and_rounds_each_partition() {
        let price = resolved(vec![component(
            "output",
            "image_output",
            "image",
            "3",
            "1",
            "succeeded",
            "request_derived",
            "ceil",
            json!({}),
        )]);
        let quote = plan_customer_quote(
            &price,
            &[
                quantity("output:0", "image_output", "image", "1", json!({})),
                quantity("output:1", "image_output", "image", "1", json!({})),
            ],
        )
        .expect("partitioned quote should plan");
        let facts = vec![
            usage_fact("output:0", "image_output", "image", "1", json!({})),
            usage_fact("output:1", "image_output", "image", "1", json!({})),
        ];

        let rating = rate_frozen_customer_quote(&quote, &facts).expect("frozen quote should rate");

        assert_eq!(rating.total_amount_micros, "2");
        assert_eq!(rating.lines.len(), 2);
        assert_eq!(rating.fact_set_hash.len(), 64);
        assert_eq!(rating.rating_hash.len(), 64);
    }

    #[test]
    fn frozen_rating_fails_when_a_partition_is_missing_or_exceeds_its_maximum() {
        let price = resolved(vec![component(
            "output",
            "image_output",
            "image",
            "1",
            "20000",
            "succeeded",
            "request_derived",
            "exact",
            json!({}),
        )]);
        let quote = plan_customer_quote(
            &price,
            &[
                quantity("output:0", "image_output", "image", "1", json!({})),
                quantity("output:1", "image_output", "image", "1", json!({})),
            ],
        )
        .expect("partitioned quote should plan");

        assert_eq!(
            rate_frozen_customer_quote(
                &quote,
                &[usage_fact(
                    "output:0",
                    "image_output",
                    "image",
                    "1",
                    json!({})
                )]
            ),
            Err(QuoteError::IncompletePartitions)
        );
        assert_eq!(
            rate_frozen_customer_quote(
                &quote,
                &[
                    usage_fact("output:0", "image_output", "image", "2", json!({})),
                    usage_fact("output:1", "image_output", "image", "1", json!({})),
                ]
            ),
            Err(QuoteError::QuantityExceedsQuote)
        );
    }

    #[test]
    fn frozen_rating_requires_every_component_for_the_selected_outcome() {
        let price = resolved(vec![
            component(
                "image",
                "image_output",
                "image",
                "1",
                "20000",
                "succeeded",
                "request_derived",
                "exact",
                json!({}),
            ),
            component(
                "tokens",
                "image_output_token",
                "token",
                "1000000",
                "30000000",
                "succeeded",
                "provider_reported",
                "exact",
                json!({}),
            ),
        ]);
        let quote = plan_customer_quote(
            &price,
            &[
                quantity("output:0", "image_output", "image", "1", json!({})),
                quantity("output:0", "image_output_token", "token", "1000", json!({})),
            ],
        )
        .expect("multi-component quote should plan");

        assert_eq!(
            rate_frozen_customer_quote(
                &quote,
                &[usage_fact(
                    "output:0",
                    "image_output",
                    "image",
                    "1",
                    json!({})
                )]
            ),
            Err(QuoteError::IncompleteComponents)
        );
    }

    #[test]
    fn paid_quote_can_release_the_full_hold_for_a_zero_cost_failure() {
        let price = resolved(vec![
            component(
                "success",
                "image_output",
                "image",
                "1",
                "20000",
                "succeeded",
                "request_derived",
                "exact",
                json!({}),
            ),
            component(
                "failure",
                "image_output",
                "image",
                "1",
                "0",
                "failed",
                "request_derived",
                "exact",
                json!({}),
            ),
        ]);
        let quote = plan_customer_quote(
            &price,
            &[quantity(
                "output:0",
                "image_output",
                "image",
                "1",
                json!({}),
            )],
        )
        .expect("paid success path should reserve funds");
        let mut failed_fact = usage_fact("output:0", "image_output", "image", "1", json!({}));
        failed_fact.outcome = "failed".to_string();

        let rating = rate_frozen_customer_quote(&quote, &[failed_fact])
            .expect("an explicitly free failure outcome should settle");

        assert_eq!(rating.total_amount_micros, "0");
        assert_eq!(rating.lines.len(), 1);
        assert_eq!(rating.lines[0].terminal_outcome, "failed");
    }

    #[test]
    fn dimension_specific_price_is_frozen() {
        let price = resolved(vec![
            component(
                "default",
                "image_output",
                "image",
                "1",
                "20000",
                "succeeded",
                "request_derived",
                "exact",
                json!({}),
            ),
            component(
                "2k",
                "image_output",
                "image",
                "1",
                "70000",
                "succeeded",
                "request_derived",
                "exact",
                json!({"resolution": "2k"}),
            ),
        ]);
        let quote = plan_customer_quote(
            &price,
            &[quantity(
                "output:0",
                "image_output",
                "image",
                "1",
                json!({"resolution": "2k"}),
            )],
        )
        .expect("specific dimension should quote");

        assert_eq!(quote.max_total_micros, "70000");
        assert!(quote.lines.iter().any(|line| line.component_key == "2k"));
    }

    #[test]
    fn quote_hash_is_stable_when_request_quantities_are_reordered() {
        let price = resolved(vec![
            component(
                "request",
                "request",
                "request",
                "1",
                "100",
                "any",
                "request_derived",
                "exact",
                json!({}),
            ),
            component(
                "output",
                "image_output",
                "image",
                "1",
                "200",
                "succeeded",
                "request_derived",
                "exact",
                json!({}),
            ),
        ]);
        let left = vec![
            quantity("output:0", "request", "request", "1", json!({})),
            quantity("output:0", "image_output", "image", "1", json!({})),
        ];
        let right = vec![left[1].clone(), left[0].clone()];

        let first = plan_customer_quote(&price, &left).expect("first quote");
        let second = plan_customer_quote(&price, &right).expect("second quote");
        assert_eq!(first.quote_hash, second.quote_hash);
        assert_eq!(first.max_total_micros, second.max_total_micros);
    }

    #[test]
    fn quote_hash_binds_request_dimensions_even_when_the_rate_is_identical() {
        let price = resolved(vec![component(
            "output",
            "image_output",
            "image",
            "1",
            "200",
            "succeeded",
            "request_derived",
            "exact",
            json!({}),
        )]);
        let first = plan_customer_quote(
            &price,
            &[quantity(
                "output:0",
                "image_output",
                "image",
                "1",
                json!({"resolution": "1k"}),
            )],
        )
        .expect("1k quote");
        let second = plan_customer_quote(
            &price,
            &[quantity(
                "output:0",
                "image_output",
                "image",
                "1",
                json!({"resolution": "2k"}),
            )],
        )
        .expect("2k quote");

        assert_eq!(first.max_total_micros, second.max_total_micros);
        assert_ne!(first.quote_hash, second.quote_hash);
    }

    #[test]
    fn provider_cost_versions_cannot_quote_customers() {
        let mut price = resolved(vec![component(
            "output",
            "image_output",
            "image",
            "1",
            "200",
            "succeeded",
            "request_derived",
            "exact",
            json!({}),
        )]);
        price.purpose = "provider_benchmark".to_string();
        price.version.billing_mode = "published_rate".to_string();

        assert_eq!(
            plan_customer_quote(
                &price,
                &[quantity(
                    "output:0",
                    "image_output",
                    "image",
                    "1",
                    json!({})
                )]
            ),
            Err(QuoteError::InvalidPricePurpose)
        );
    }

    #[test]
    fn unconfigured_nonzero_metric_fails_closed() {
        let price = resolved(vec![component(
            "image_output",
            "image_output",
            "image",
            "1",
            "20000",
            "succeeded",
            "request_derived",
            "exact",
            json!({}),
        )]);
        let error = plan_customer_quote(
            &price,
            &[quantity(
                "request",
                "text_input_token",
                "token",
                "100",
                json!({}),
            )],
        )
        .expect_err("an omitted nonzero metric must not become an implicit free tier");
        assert_eq!(
            error,
            QuoteError::MissingRate {
                metric: "text_input_token".to_string(),
                unit: "token".to_string(),
            }
        );
    }

    fn quantity(
        partition_key: &str,
        metric: &str,
        unit: &str,
        max_quantity: &str,
        dimensions: Value,
    ) -> QuoteQuantity {
        let quantity_source = if metric.ends_with("_token") {
            "provider_reported"
        } else {
            "request_derived"
        };
        QuoteQuantity {
            partition_key: partition_key.to_string(),
            metric: metric.to_string(),
            unit: unit.to_string(),
            max_quantity: max_quantity.to_string(),
            reservation_quantity_source: if metric.ends_with("_token") {
                "official_lookup".to_string()
            } else {
                "request_derived".to_string()
            },
            reservation_confidence: if metric.ends_with("_token") {
                "bounded".to_string()
            } else {
                "exact".to_string()
            },
            settlement_quantity_source: quantity_source.to_string(),
            settlement_confidence: "exact".to_string(),
            dimensions,
        }
    }

    fn usage_fact(
        partition_key: &str,
        metric: &str,
        unit: &str,
        quantity: &str,
        dimensions: Value,
    ) -> UsageFact {
        UsageFact {
            usage_fact_id: Uuid::new_v4(),
            partition_key: partition_key.to_string(),
            authority_key: partition_key.to_string(),
            provider_id: "openai-codex".to_string(),
            provider_account_id: Some(Uuid::new_v4()),
            execution_surface: "provider_api".to_string(),
            fact_domain: "customer_billable".to_string(),
            metric: metric.to_string(),
            unit: unit.to_string(),
            quantity: quantity.to_string(),
            outcome: "succeeded".to_string(),
            quantity_source: "request_derived".to_string(),
            confidence: "exact".to_string(),
            dimensions,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn component(
        key: &str,
        metric: &str,
        unit: &str,
        unit_size: &str,
        unit_price_micros: &str,
        outcome: &str,
        quantity_source: &str,
        rounding_mode: &str,
        dimensions: Value,
    ) -> PriceComponentView {
        PriceComponentView {
            price_component_id: Uuid::new_v4(),
            component_key: key.to_string(),
            metric: metric.to_string(),
            unit: unit.to_string(),
            unit_size: unit_size.to_string(),
            unit_price_micros: unit_price_micros.to_string(),
            outcome: outcome.to_string(),
            quantity_source: quantity_source.to_string(),
            required_confidence: "exact".to_string(),
            rounding_mode: rounding_mode.to_string(),
            dimensions,
            created_at_ms: 1,
        }
    }

    fn resolved(components: Vec<PriceComponentView>) -> ResolvedPriceVersion {
        let price_book_id = Uuid::new_v4();
        ResolvedPriceVersion {
            price_book_id,
            price_book_key: "customer.images.usd".to_string(),
            purpose: "customer_sale".to_string(),
            scope_type: "platform".to_string(),
            organization_id: None,
            project_id: None,
            provider_id: None,
            currency: "USD".to_string(),
            version: PriceBookVersionView {
                price_book_version_id: Uuid::new_v4(),
                price_book_id,
                version: 1,
                api_profile: "openai-images-v1".to_string(),
                operation: "generation".to_string(),
                provider_id: None,
                provider_model_id: None,
                public_model_id: "gpt-image-2".to_string(),
                media_kind: "image".to_string(),
                service_tier: "standard".to_string(),
                execution_surface: "provider_api".to_string(),
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
                components,
            },
        }
    }
}
