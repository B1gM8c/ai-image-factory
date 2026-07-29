use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{PriceBookVersionDraft, PriceComponentDraft};

const CHECKED_AT_MS: i64 = 1_784_822_400_000;

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OfficialPriceCatalogDescriptor {
    pub catalog_key: String,
    pub source_provider_id: String,
    pub display_name: String,
    pub currency: String,
    pub source_url: String,
    pub retrieval_method: String,
    pub source_checked_at_ms: Option<i64>,
    pub source_revision: Option<String>,
    pub parser_version: String,
    pub item_count: usize,
    pub available: bool,
    pub unavailable_reason: Option<String>,
    pub latest_sync_run: Option<OfficialPriceSyncRunSummary>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OfficialPriceCatalogs {
    pub as_of_ms: i64,
    pub catalogs: Vec<OfficialPriceCatalogDescriptor>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ApplyOfficialPriceSnapshotRequest {
    pub item_keys: Vec<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OfficialPriceSnapshotSummary {
    #[schema(value_type = String)]
    pub snapshot_id: Uuid,
    pub catalog_key: String,
    pub source_provider_id: String,
    pub currency: String,
    pub source_url: String,
    pub source_checked_at_ms: i64,
    pub source_revision: Option<String>,
    pub parser_version: String,
    pub content_sha256: String,
    pub state: String,
    pub item_count: i32,
    #[schema(value_type = String)]
    pub created_by_user_id: Uuid,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct OfficialPriceSnapshotDiffView {
    pub item_key: String,
    pub display_name: String,
    pub public_model_id: String,
    pub media_kind: String,
    pub target_provider_id: String,
    pub component_count: usize,
    pub status: String,
    #[schema(value_type = Option<String>)]
    pub price_book_id: Option<Uuid>,
    #[schema(value_type = Option<String>)]
    pub price_book_version_id: Option<Uuid>,
    pub existing_version: Option<i32>,
    pub existing_state: Option<String>,
    pub component_differences: Vec<OfficialPriceComponentDiffView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct OfficialPriceComponentDiffView {
    pub component_key: String,
    pub status: String,
    pub previous: Option<PriceComponentDraft>,
    pub observed: Option<PriceComponentDraft>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OfficialPriceSnapshotApplicationView {
    pub item_key: String,
    pub action: String,
    #[schema(value_type = String)]
    pub price_book_id: Uuid,
    #[schema(value_type = String)]
    pub price_book_version_id: Uuid,
    #[schema(value_type = String)]
    pub applied_by_user_id: Uuid,
    pub applied_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq)]
pub struct OfficialPriceSnapshotPreview {
    pub snapshot: OfficialPriceSnapshotSummary,
    pub sync_run: Option<OfficialPriceSyncRunSummary>,
    pub differences: Vec<OfficialPriceSnapshotDiffView>,
    pub applications: Vec<OfficialPriceSnapshotApplicationView>,
}

#[derive(Clone, Debug, Serialize, ToSchema, PartialEq, Eq)]
pub struct OfficialPriceSyncRunSummary {
    #[schema(value_type = String)]
    pub sync_run_id: Uuid,
    pub catalog_key: String,
    pub source_provider_id: String,
    pub retrieval_method: String,
    pub parser_version: String,
    pub source_checked_at_ms: i64,
    pub source_revision: Option<String>,
    pub evidence_sha256: String,
    pub normalized_content_sha256: Option<String>,
    pub state: String,
    #[schema(value_type = Option<String>)]
    pub previous_snapshot_id: Option<Uuid>,
    #[schema(value_type = Option<String>)]
    pub snapshot_id: Option<Uuid>,
    pub failure_code: Option<String>,
    #[schema(value_type = String)]
    pub initiated_by_user_id: Uuid,
    pub created_at_ms: i64,
    pub completed_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct OfficialPriceCatalog {
    pub catalog_key: String,
    pub source_provider_id: String,
    pub display_name: String,
    pub currency: String,
    pub source_url: String,
    pub source_checked_at_ms: i64,
    pub source_revision: Option<String>,
    pub parser_version: String,
    pub items: Vec<OfficialPriceItem>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct OfficialPriceItem {
    pub item_key: String,
    pub price_book_key: String,
    pub display_name: String,
    pub target_provider_id: String,
    pub api_profile: String,
    pub operation: String,
    pub provider_model_id: String,
    pub public_model_id: String,
    pub media_kind: String,
    pub service_tier: String,
    pub execution_surface: String,
    pub components: Vec<PriceComponentDraft>,
}

impl OfficialPriceCatalog {
    pub(super) fn descriptor(&self) -> OfficialPriceCatalogDescriptor {
        OfficialPriceCatalogDescriptor {
            catalog_key: self.catalog_key.clone(),
            source_provider_id: self.source_provider_id.clone(),
            display_name: self.display_name.clone(),
            currency: self.currency.clone(),
            source_url: self.source_url.clone(),
            retrieval_method: "curated_manifest".to_string(),
            source_checked_at_ms: Some(self.source_checked_at_ms),
            source_revision: self.source_revision.clone(),
            parser_version: self.parser_version.clone(),
            item_count: self.items.len(),
            available: true,
            unavailable_reason: None,
            latest_sync_run: None,
        }
    }

    pub(super) fn draft(&self, item: &OfficialPriceItem) -> PriceBookVersionDraft {
        PriceBookVersionDraft {
            api_profile: item.api_profile.clone(),
            operation: item.operation.clone(),
            provider_id: Some(item.target_provider_id.clone()),
            provider_model_id: Some(item.provider_model_id.clone()),
            public_model_id: item.public_model_id.clone(),
            media_kind: item.media_kind.clone(),
            service_tier: item.service_tier.clone(),
            execution_surface: item.execution_surface.clone(),
            billing_mode: "published_rate".to_string(),
            is_free: false,
            effective_from_ms: self.source_checked_at_ms,
            source_kind: "official_document".to_string(),
            source_url: Some(self.source_url.clone()),
            source_checked_at_ms: Some(self.source_checked_at_ms),
            notes: Some(format!(
                "Observed from official catalog {}{}; review before publishing",
                self.catalog_key,
                self.source_revision
                    .as_deref()
                    .map(|revision| format!(" ({revision})"))
                    .unwrap_or_default()
            )),
            components: item.components.clone(),
        }
    }
}

pub(super) fn catalogs() -> Vec<OfficialPriceCatalog> {
    vec![openai_catalog(), xai_catalog()]
}

pub(super) fn catalog(catalog_key: &str) -> Option<OfficialPriceCatalog> {
    catalogs()
        .into_iter()
        .find(|catalog| catalog.catalog_key == catalog_key)
}

pub(super) fn descriptors() -> Vec<OfficialPriceCatalogDescriptor> {
    let mut descriptors = catalogs()
        .into_iter()
        .map(|catalog| catalog.descriptor())
        .collect::<Vec<_>>();
    descriptors.push(OfficialPriceCatalogDescriptor {
        catalog_key: "volcengine-ark-public-pricing".to_string(),
        source_provider_id: "volcengine-ark".to_string(),
        display_name: "火山方舟图片与视频模型".to_string(),
        currency: "CNY".to_string(),
        source_url: "https://www.volcengine.com/docs/82379/seedream?lang=zh".to_string(),
        retrieval_method: "curated_manifest".to_string(),
        source_checked_at_ms: Some(CHECKED_AT_MS),
        source_revision: None,
        parser_version: "curated-v1".to_string(),
        item_count: 0,
        available: false,
        unavailable_reason: Some(
            "尚未从火山方舟官方价格页核验到完整、可审计的逐模型单价，暂不允许导入".to_string(),
        ),
        latest_sync_run: None,
    });
    descriptors
}

fn openai_catalog() -> OfficialPriceCatalog {
    OfficialPriceCatalog {
        catalog_key: "openai-api-pricing".to_string(),
        source_provider_id: "openai".to_string(),
        display_name: "OpenAI API 图片模型".to_string(),
        currency: "USD".to_string(),
        source_url: "https://developers.openai.com/api/docs/pricing".to_string(),
        source_checked_at_ms: CHECKED_AT_MS,
        source_revision: Some("observed-2026-07-24".to_string()),
        parser_version: "curated-v1".to_string(),
        items: vec![OfficialPriceItem {
            item_key: "gpt-image-2".to_string(),
            price_book_key: "provider_benchmark.openai.gpt-image-2.usd".to_string(),
            display_name: "OpenAI GPT Image 2 官方 API 基准价".to_string(),
            target_provider_id: "openai-codex".to_string(),
            api_profile: "openai-images-v1".to_string(),
            operation: "*".to_string(),
            provider_model_id: "gpt-image-2".to_string(),
            public_model_id: "gpt-image-2".to_string(),
            media_kind: "image".to_string(),
            service_tier: "standard".to_string(),
            execution_surface: "provider_cli".to_string(),
            components: vec![
                token_component(
                    "text-input",
                    "text_input_token",
                    "5000000",
                    "provider_reported",
                ),
                token_component(
                    "cached-text-input",
                    "cached_text_input_token",
                    "1250000",
                    "provider_reported",
                ),
                token_component(
                    "image-input",
                    "image_input_token",
                    "8000000",
                    "provider_reported",
                ),
                token_component(
                    "cached-image-input",
                    "cached_image_input_token",
                    "2000000",
                    "provider_reported",
                ),
                token_component(
                    "image-output-reported",
                    "image_output_token",
                    "30000000",
                    "provider_reported",
                ),
                token_component(
                    "image-output-official-lookup",
                    "image_output_token",
                    "30000000",
                    "official_lookup",
                ),
            ],
        }],
    }
}

fn xai_catalog() -> OfficialPriceCatalog {
    OfficialPriceCatalog {
        catalog_key: "xai-imagine-pricing".to_string(),
        source_provider_id: "xai".to_string(),
        display_name: "xAI Imagine 图片与视频模型".to_string(),
        currency: "USD".to_string(),
        source_url: "https://docs.x.ai/developers/pricing".to_string(),
        source_checked_at_ms: CHECKED_AT_MS,
        source_revision: Some("docs-updated-2026-07-03".to_string()),
        parser_version: "curated-v1".to_string(),
        items: vec![
            xai_image_item(
                "grok-imagine-image-quality",
                "Grok Imagine Image Quality 官方基准价",
                "10000",
                "50000",
                "70000",
            ),
            xai_image_item(
                "grok-imagine-image",
                "Grok Imagine Image 官方基准价",
                "2000",
                "20000",
                "20000",
            ),
            xai_video_item(
                "grok-imagine-video-1.5",
                "Grok Imagine Video 1.5 官方基准价",
                vec![image_component(
                    "image-input",
                    "image_input",
                    "10000",
                    json!({}),
                )],
                &[("480p", "80000"), ("720p", "140000"), ("1080p", "250000")],
            ),
            xai_video_item(
                "grok-imagine-video",
                "Grok Imagine Video 官方基准价",
                vec![
                    image_component("image-input", "image_input", "2000", json!({})),
                    media_second_component("video-input", "video_input_second", "10000", json!({})),
                ],
                &[("480p", "50000"), ("720p", "70000")],
            ),
        ],
    }
}

fn xai_image_item(
    model: &str,
    display_name: &str,
    input_price_micros: &str,
    output_1k_price_micros: &str,
    output_2k_price_micros: &str,
) -> OfficialPriceItem {
    OfficialPriceItem {
        item_key: model.to_string(),
        price_book_key: format!("provider_benchmark.xai.{model}.usd"),
        display_name: display_name.to_string(),
        target_provider_id: "xai-grok".to_string(),
        api_profile: "xai-images-v1".to_string(),
        operation: "generation".to_string(),
        provider_model_id: model.to_string(),
        public_model_id: model.to_string(),
        media_kind: "image".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_cli".to_string(),
        components: vec![
            image_component("image-input", "image_input", input_price_micros, json!({})),
            image_component(
                "image-output-1k",
                "image_output",
                output_1k_price_micros,
                json!({"resolution": "1k"}),
            ),
            image_component(
                "image-output-2k",
                "image_output",
                output_2k_price_micros,
                json!({"resolution": "2k"}),
            ),
        ],
    }
}

fn xai_video_item(
    model: &str,
    display_name: &str,
    mut input_components: Vec<PriceComponentDraft>,
    output_prices: &[(&str, &str)],
) -> OfficialPriceItem {
    input_components.extend(output_prices.iter().map(|(resolution, price)| {
        media_second_component(
            &format!("video-output-{resolution}"),
            "video_output_second",
            price,
            json!({"resolution": resolution}),
        )
    }));
    OfficialPriceItem {
        item_key: model.to_string(),
        price_book_key: format!("provider_benchmark.xai.{model}.usd"),
        display_name: display_name.to_string(),
        target_provider_id: "xai-grok".to_string(),
        api_profile: "xai-videos-v1".to_string(),
        operation: "video_generation".to_string(),
        provider_model_id: model.to_string(),
        public_model_id: model.to_string(),
        media_kind: "video".to_string(),
        service_tier: "standard".to_string(),
        execution_surface: "provider_cli".to_string(),
        components: input_components,
    }
}

fn token_component(
    component_key: &str,
    metric: &str,
    unit_price_micros: &str,
    quantity_source: &str,
) -> PriceComponentDraft {
    component(
        component_key,
        metric,
        "token",
        "1000000",
        unit_price_micros,
        quantity_source,
        json!({}),
    )
}

fn image_component(
    component_key: &str,
    metric: &str,
    unit_price_micros: &str,
    dimensions: serde_json::Value,
) -> PriceComponentDraft {
    component(
        component_key,
        metric,
        "image",
        "1",
        unit_price_micros,
        "request_derived",
        dimensions,
    )
}

fn media_second_component(
    component_key: &str,
    metric: &str,
    unit_price_micros: &str,
    dimensions: serde_json::Value,
) -> PriceComponentDraft {
    component(
        component_key,
        metric,
        "second",
        "1",
        unit_price_micros,
        "media_inspected",
        dimensions,
    )
}

fn component(
    component_key: &str,
    metric: &str,
    unit: &str,
    unit_size: &str,
    unit_price_micros: &str,
    quantity_source: &str,
    dimensions: serde_json::Value,
) -> PriceComponentDraft {
    let required_confidence = match quantity_source {
        "official_lookup" => "estimated",
        _ => "exact",
    };
    PriceComponentDraft {
        component_key: component_key.to_string(),
        metric: metric.to_string(),
        unit: unit.to_string(),
        unit_size: unit_size.to_string(),
        unit_price_micros: unit_price_micros.to_string(),
        outcome: "succeeded".to_string(),
        quantity_source: quantity_source.to_string(),
        required_confidence: required_confidence.to_string(),
        rounding_mode: "half_up".to_string(),
        dimensions,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{OfficialPriceItem, catalog, descriptors};

    #[test]
    fn openai_gpt_image_2_catalog_preserves_official_token_rates() {
        let catalog = catalog("openai-api-pricing").expect("OpenAI catalog");
        let item = item(&catalog.items, "gpt-image-2");
        let rates = rates(item);

        assert_eq!(
            rates,
            BTreeMap::from([
                (
                    "cached-image-input",
                    ("cached_image_input_token", "token", "1000000", "2000000"),
                ),
                (
                    "cached-text-input",
                    ("cached_text_input_token", "token", "1000000", "1250000"),
                ),
                (
                    "image-input",
                    ("image_input_token", "token", "1000000", "8000000"),
                ),
                (
                    "image-output-official-lookup",
                    ("image_output_token", "token", "1000000", "30000000"),
                ),
                (
                    "image-output-reported",
                    ("image_output_token", "token", "1000000", "30000000"),
                ),
                (
                    "text-input",
                    ("text_input_token", "token", "1000000", "5000000"),
                ),
            ]),
        );
        assert_eq!(
            item.components
                .iter()
                .find(|component| component.component_key == "image-output-official-lookup")
                .expect("lookup component")
                .quantity_source,
            "official_lookup"
        );
        let official_lookup = item
            .components
            .iter()
            .find(|component| component.quantity_source == "official_lookup")
            .expect("official lookup component");
        assert_eq!(official_lookup.required_confidence, "estimated");
    }

    #[test]
    fn xai_catalog_preserves_image_and_video_resolution_rates() {
        let catalog = catalog("xai-imagine-pricing").expect("xAI catalog");
        let image = item(&catalog.items, "grok-imagine-image-quality");
        assert_eq!(image.target_provider_id, "xai-grok");
        assert_eq!(image.operation, "generation");
        assert_eq!(image.execution_surface, "provider_cli");

        assert_eq!(
            rates(image),
            BTreeMap::from([
                ("image-input", ("image_input", "image", "1", "10000")),
                ("image-output-1k", ("image_output", "image", "1", "50000")),
                ("image-output-2k", ("image_output", "image", "1", "70000")),
            ])
        );
        assert_eq!(
            rates(item(&catalog.items, "grok-imagine-image")),
            BTreeMap::from([
                ("image-input", ("image_input", "image", "1", "2000")),
                ("image-output-1k", ("image_output", "image", "1", "20000")),
                ("image-output-2k", ("image_output", "image", "1", "20000")),
            ])
        );
        assert_eq!(
            rates(item(&catalog.items, "grok-imagine-video-1.5")),
            BTreeMap::from([
                ("image-input", ("image_input", "image", "1", "10000")),
                (
                    "video-output-1080p",
                    ("video_output_second", "second", "1", "250000"),
                ),
                (
                    "video-output-480p",
                    ("video_output_second", "second", "1", "80000"),
                ),
                (
                    "video-output-720p",
                    ("video_output_second", "second", "1", "140000"),
                ),
            ])
        );
        assert_eq!(
            rates(item(&catalog.items, "grok-imagine-video")),
            BTreeMap::from([
                ("image-input", ("image_input", "image", "1", "2000")),
                (
                    "video-input",
                    ("video_input_second", "second", "1", "10000"),
                ),
                (
                    "video-output-480p",
                    ("video_output_second", "second", "1", "50000"),
                ),
                (
                    "video-output-720p",
                    ("video_output_second", "second", "1", "70000"),
                ),
            ])
        );
        for model in ["grok-imagine-video-1.5", "grok-imagine-video"] {
            for component in &item(&catalog.items, model).components {
                if matches!(
                    component.metric.as_str(),
                    "video_input_second" | "video_output_second"
                ) {
                    assert_eq!(component.quantity_source, "media_inspected");
                }
            }
        }
    }

    #[test]
    fn unverified_volcengine_catalog_cannot_be_imported() {
        let descriptor = descriptors()
            .into_iter()
            .find(|descriptor| descriptor.source_provider_id == "volcengine-ark")
            .expect("Volcengine descriptor");
        assert!(!descriptor.available);
        assert_eq!(descriptor.item_count, 0);
        assert!(descriptor.unavailable_reason.is_some());
    }

    fn item<'a>(items: &'a [OfficialPriceItem], item_key: &str) -> &'a OfficialPriceItem {
        items
            .iter()
            .find(|item| item.item_key == item_key)
            .unwrap_or_else(|| panic!("missing official catalog item {item_key}"))
    }

    fn rates(item: &OfficialPriceItem) -> BTreeMap<&str, (&str, &str, &str, &str)> {
        item.components
            .iter()
            .map(|component| {
                (
                    component.component_key.as_str(),
                    (
                        component.metric.as_str(),
                        component.unit.as_str(),
                        component.unit_size.as_str(),
                        component.unit_price_micros.as_str(),
                    ),
                )
            })
            .collect()
    }
}
