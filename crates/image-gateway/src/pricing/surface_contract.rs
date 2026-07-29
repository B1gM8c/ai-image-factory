//! Versioned pricing surface contracts.
//!
//! The registry mirrors normalized admission facts. It deliberately does not model every public
//! API field: provider model identity, pricing dimensions, and output cardinality are the inputs
//! that price resolution must prove before a price version can be published.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::size::is_valid_gpt_image_2_size;

pub const REGISTRY_SCHEMA_VERSION: u16 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[allow(dead_code)]
pub enum SurfaceSupport {
    Supported,
    Unsupported { reason: &'static str },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ValueDomain {
    Enum(&'static [&'static str]),
    IntegerClosed { min: u32, max: u32 },
    StringPredicate(StringPredicate),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum StringPredicate {
    GptImage2SizeV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DimensionContract {
    pub key: &'static str,
    pub required: bool,
    pub domain: ValueDomain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Selector {
    ProviderModel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PresenceBranch {
    pub required: &'static [&'static str],
    pub forbidden: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ConditionalPresenceCase {
    pub selector_values: &'static [&'static str],
    pub required: &'static [&'static str],
    pub forbidden: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ConditionalEnumCase {
    pub selector_values: &'static [&'static str],
    pub allowed_values: &'static [&'static str],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ImageDimensionVariant {
    pub resolution: &'static str,
    pub minimum: u32,
    pub maximum: u32,
    pub max_pixels: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Constraint {
    ExactlyOnePresenceBranch(&'static [PresenceBranch]),
    ConditionalPresence {
        selector: Selector,
        cases: &'static [ConditionalPresenceCase],
    },
    ConditionalEnum {
        selector: Selector,
        field: &'static str,
        cases: &'static [ConditionalEnumCase],
    },
    ImageDimensions {
        resolution_field: &'static str,
        width_field: &'static str,
        height_field: &'static str,
        variants: &'static [ImageDimensionVariant],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum OutputCardinality {
    Fixed(u32),
    ClosedRange { min: u32, max: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct MeteringBasisContract {
    pub metric: &'static str,
    pub unit: &'static str,
    pub quantity_source: &'static str,
    pub confidence: &'static str,
    pub customer_sale_required: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PricingSurfaceContract {
    pub contract_id: &'static str,
    pub contract_version: u16,
    pub provider_id: &'static str,
    pub route_operation: &'static str,
    pub pricing_operation: &'static str,
    pub command_schema: &'static str,
    pub media_kind: &'static str,
    pub api_profiles: &'static [&'static str],
    pub provider_models: &'static [&'static str],
    pub dimensions: &'static [DimensionContract],
    pub constraints: &'static [Constraint],
    pub output_cardinality: OutputCardinality,
    pub metering_bases: &'static [MeteringBasisContract],
    pub normalizer_key: &'static str,
    pub normalizer_revision: u16,
    pub support: SurfaceSupport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactSurfaceIdentity<'a> {
    pub api_profile: &'a str,
    pub provider_model_id: &'a str,
    pub public_model_id: &'a str,
    pub service_tier: &'a str,
    pub execution_surface: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractBindingSnapshot {
    pub contract_key: String,
    pub revision: i64,
    pub contract_hash: String,
    pub contract_schema_version: i32,
    pub api_profile: String,
    pub operation: String,
    pub provider_id: String,
    pub provider_model_id: String,
    pub public_model_id: String,
    pub media_kind: String,
    pub service_tier: String,
    pub execution_surface: String,
    pub normalizer_key: String,
    pub normalizer_revision: i64,
    pub contract_json: Value,
}

#[derive(Serialize)]
struct CanonicalExactSurfaceContract<'a> {
    schema_version: u16,
    contract: &'a PricingSurfaceContract,
    exact_surface: CanonicalExactSurfaceIdentity<'a>,
}

#[derive(Serialize)]
struct CanonicalExactSurfaceIdentity<'a> {
    api_profile: &'a str,
    provider_model_id: &'a str,
    public_model_id: &'a str,
    service_tier: &'a str,
    execution_surface: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DimensionValue<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SurfaceRequest<'a> {
    pub provider_model_id: &'a str,
    pub dimensions: &'a [DimensionValue<'a>],
    pub output_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationError {
    UnsupportedSurface,
    SurfaceIdentityMismatch,
    UnsupportedProviderModel,
    InvalidOutputCount,
    DuplicateDimension(&'static str),
    UnknownDimension,
    MissingDimension(&'static str),
    InvalidDimension(&'static str),
    ConstraintViolation(&'static str),
}

impl PricingSurfaceContract {
    pub fn validate(&self, request: &SurfaceRequest<'_>) -> Result<(), ValidationError> {
        if matches!(self.support, SurfaceSupport::Unsupported { .. }) {
            return Err(ValidationError::UnsupportedSurface);
        }
        self.validate_shape(request)
    }

    /// Validates the request shape even for an unsupported surface.
    ///
    /// This is useful while developing a future pricing adapter without accidentally making the
    /// surface publishable.
    pub fn validate_shape(&self, request: &SurfaceRequest<'_>) -> Result<(), ValidationError> {
        if !self.provider_models.contains(&request.provider_model_id) {
            return Err(ValidationError::UnsupportedProviderModel);
        }
        if !self.output_cardinality.contains(request.output_count) {
            return Err(ValidationError::InvalidOutputCount);
        }
        for (index, value) in request.dimensions.iter().enumerate() {
            if request.dimensions[..index]
                .iter()
                .any(|candidate| candidate.key == value.key)
            {
                let key = self
                    .dimensions
                    .iter()
                    .find(|dimension| dimension.key == value.key)
                    .map(|dimension| dimension.key)
                    .unwrap_or("unknown");
                return Err(ValidationError::DuplicateDimension(key));
            }
            let Some(dimension) = self
                .dimensions
                .iter()
                .find(|dimension| dimension.key == value.key)
            else {
                return Err(ValidationError::UnknownDimension);
            };
            if !dimension.domain.contains(value.value) {
                return Err(ValidationError::InvalidDimension(dimension.key));
            }
        }
        for dimension in self.dimensions {
            if dimension.required && request.value(dimension.key).is_none() {
                return Err(ValidationError::MissingDimension(dimension.key));
            }
        }
        for constraint in self.constraints {
            constraint.validate(request)?;
        }
        Ok(())
    }

    pub fn validate_component_selector(&self, selector: &Value) -> Result<(), ValidationError> {
        let Some(selector) = selector.as_object() else {
            return Err(ValidationError::UnknownDimension);
        };
        for (key, value) in selector {
            let Some(dimension) = self
                .dimensions
                .iter()
                .find(|dimension| dimension.key == key)
            else {
                return Err(ValidationError::UnknownDimension);
            };
            let Some(value) = value.as_str() else {
                return Err(ValidationError::InvalidDimension(dimension.key));
            };
            if !dimension.domain.contains(value) {
                return Err(ValidationError::InvalidDimension(dimension.key));
            }
        }
        Ok(())
    }

    pub fn binding_snapshot(
        &self,
        identity: ExactSurfaceIdentity<'_>,
    ) -> Result<ContractBindingSnapshot, ValidationError> {
        if matches!(self.support, SurfaceSupport::Unsupported { .. }) {
            return Err(ValidationError::UnsupportedSurface);
        }
        if !self.api_profiles.contains(&identity.api_profile)
            || !self.provider_models.contains(&identity.provider_model_id)
            || identity.public_model_id.trim().is_empty()
            || identity.service_tier != "standard"
            || identity.execution_surface != "provider_cli"
        {
            return Err(ValidationError::SurfaceIdentityMismatch);
        }
        let canonical = CanonicalExactSurfaceContract {
            schema_version: REGISTRY_SCHEMA_VERSION,
            contract: self,
            exact_surface: CanonicalExactSurfaceIdentity {
                api_profile: identity.api_profile,
                provider_model_id: identity.provider_model_id,
                public_model_id: identity.public_model_id,
                service_tier: identity.service_tier,
                execution_surface: identity.execution_surface,
            },
        };
        let contract_json =
            serde_json::to_value(&canonical).expect("static pricing contract serialization");
        let canonical_bytes =
            serde_json::to_vec(&canonical).expect("static pricing contract serialization");
        let contract_hash = hex::encode(Sha256::digest(canonical_bytes));
        let identity_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(&canonical.exact_surface)
                .expect("pricing surface identity serialization"),
        ));
        Ok(ContractBindingSnapshot {
            contract_key: format!("{}:{}", self.contract_id, &identity_hash[..16]),
            revision: i64::from(self.contract_version),
            contract_hash,
            contract_schema_version: i32::from(REGISTRY_SCHEMA_VERSION),
            api_profile: identity.api_profile.to_string(),
            operation: self.pricing_operation.to_string(),
            provider_id: self.provider_id.to_string(),
            provider_model_id: identity.provider_model_id.to_string(),
            public_model_id: identity.public_model_id.to_string(),
            media_kind: self.media_kind.to_string(),
            service_tier: identity.service_tier.to_string(),
            execution_surface: identity.execution_surface.to_string(),
            normalizer_key: self.normalizer_key.to_string(),
            normalizer_revision: i64::from(self.normalizer_revision),
            contract_json,
        })
    }
}

impl SurfaceRequest<'_> {
    fn value(&self, key: &str) -> Option<&str> {
        self.dimensions
            .iter()
            .find(|dimension| dimension.key == key)
            .map(|dimension| dimension.value)
    }
}

impl ValueDomain {
    fn contains(self, value: &str) -> bool {
        match self {
            Self::Enum(values) => values.contains(&value),
            Self::IntegerClosed { min, max } => value
                .parse::<u32>()
                .is_ok_and(|value| (min..=max).contains(&value)),
            Self::StringPredicate(StringPredicate::GptImage2SizeV1) => {
                is_valid_gpt_image_2_size(value)
            }
        }
    }
}

impl OutputCardinality {
    pub const fn contains(self, count: u32) -> bool {
        match self {
            Self::Fixed(expected) => count == expected,
            Self::ClosedRange { min, max } => count >= min && count <= max,
        }
    }
}

impl Constraint {
    fn validate(self, request: &SurfaceRequest<'_>) -> Result<(), ValidationError> {
        match self {
            Self::ExactlyOnePresenceBranch(branches) => {
                let matches = branches
                    .iter()
                    .filter(|branch| branch.matches(request))
                    .count();
                if matches == 1 {
                    Ok(())
                } else {
                    Err(ValidationError::ConstraintViolation(
                        "exactly_one_presence_branch",
                    ))
                }
            }
            Self::ConditionalPresence { selector, cases } => {
                let selector =
                    selector
                        .value(request)
                        .ok_or(ValidationError::ConstraintViolation(
                            "conditional_selector_missing",
                        ))?;
                let case = cases
                    .iter()
                    .find(|case| case.selector_values.contains(&selector))
                    .ok_or(ValidationError::ConstraintViolation(
                        "conditional_presence_uncovered",
                    ))?;
                if case.required.iter().all(|key| request.value(key).is_some())
                    && case
                        .forbidden
                        .iter()
                        .all(|key| request.value(key).is_none())
                {
                    Ok(())
                } else {
                    Err(ValidationError::ConstraintViolation("conditional_presence"))
                }
            }
            Self::ConditionalEnum {
                selector,
                field,
                cases,
            } => {
                let selector =
                    selector
                        .value(request)
                        .ok_or(ValidationError::ConstraintViolation(
                            "conditional_selector_missing",
                        ))?;
                let value = request
                    .value(field)
                    .ok_or(ValidationError::MissingDimension(field))?;
                let case = cases
                    .iter()
                    .find(|case| case.selector_values.contains(&selector))
                    .ok_or(ValidationError::ConstraintViolation(
                        "conditional_enum_uncovered",
                    ))?;
                if case.allowed_values.contains(&value) {
                    Ok(())
                } else {
                    Err(ValidationError::ConstraintViolation("conditional_enum"))
                }
            }
            Self::ImageDimensions {
                resolution_field,
                width_field,
                height_field,
                variants,
            } => {
                let (Some(width), Some(height)) =
                    (request.value(width_field), request.value(height_field))
                else {
                    return Ok(());
                };
                let width = width.parse::<u32>().map_err(|_| {
                    ValidationError::ConstraintViolation("image_dimensions_integer")
                })?;
                let height = height.parse::<u32>().map_err(|_| {
                    ValidationError::ConstraintViolation("image_dimensions_integer")
                })?;
                let resolution =
                    request
                        .value(resolution_field)
                        .ok_or(ValidationError::ConstraintViolation(
                            "image_resolution_missing",
                        ))?;
                let variant = variants
                    .iter()
                    .find(|variant| variant.resolution == resolution)
                    .ok_or(ValidationError::ConstraintViolation(
                        "image_resolution_uncovered",
                    ))?;
                if (variant.minimum..=variant.maximum).contains(&width)
                    && (variant.minimum..=variant.maximum).contains(&height)
                    && u64::from(width) * u64::from(height) <= variant.max_pixels
                {
                    Ok(())
                } else {
                    Err(ValidationError::ConstraintViolation("image_dimensions"))
                }
            }
        }
    }
}

impl Selector {
    fn value<'a>(self, request: &'a SurfaceRequest<'a>) -> Option<&'a str> {
        match self {
            Self::ProviderModel => Some(request.provider_model_id),
        }
    }
}

impl PresenceBranch {
    fn matches(self, request: &SurfaceRequest<'_>) -> bool {
        self.required.iter().all(|key| request.value(key).is_some())
            && self
                .forbidden
                .iter()
                .all(|key| request.value(key).is_none())
    }
}

const CODEX_MODELS: &[&str] = &["gpt-image-2", "gpt-image-2-2026-04-21"];
const CODEX_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "quality",
        required: true,
        domain: ValueDomain::Enum(&["auto", "low", "medium", "high"]),
    },
    DimensionContract {
        key: "size",
        required: true,
        domain: ValueDomain::StringPredicate(StringPredicate::GptImage2SizeV1),
    },
];

const GROK_IMAGE_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "aspect_ratio",
        required: true,
        domain: ValueDomain::Enum(&[
            "auto", "1:1", "3:4", "4:3", "9:16", "16:9", "2:3", "3:2", "9:19.5", "19.5:9", "9:20",
            "20:9", "1:2", "2:1",
        ]),
    },
    DimensionContract {
        key: "resolution",
        required: true,
        domain: ValueDomain::Enum(&["1k"]),
    },
];

const DREAMINA_IMAGE_MODELS: &[&str] = &[
    "3.0", "3.1", "4.0", "4.1", "4.5", "4.6", "4.7", "5.0", "5.0Pro",
];
const DREAMINA_IMAGE_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "resolution_type",
        required: true,
        domain: ValueDomain::Enum(&["1k", "2k", "4k"]),
    },
    DimensionContract {
        key: "ratio",
        required: false,
        domain: ValueDomain::Enum(&["21:9", "16:9", "3:2", "4:3", "1:1", "3:4", "2:3", "9:16"]),
    },
    DimensionContract {
        key: "width",
        required: false,
        domain: ValueDomain::IntegerClosed {
            min: 512,
            max: 6_240,
        },
    },
    DimensionContract {
        key: "height",
        required: false,
        domain: ValueDomain::IntegerClosed {
            min: 512,
            max: 6_240,
        },
    },
];
const DREAMINA_IMAGE_GEOMETRY: &[PresenceBranch] = &[
    PresenceBranch {
        required: &["ratio"],
        forbidden: &["width", "height"],
    },
    PresenceBranch {
        required: &["width", "height"],
        forbidden: &["ratio"],
    },
];
const DREAMINA_IMAGE_RESOLUTIONS: &[ConditionalEnumCase] = &[
    ConditionalEnumCase {
        selector_values: &["3.0", "3.1"],
        allowed_values: &["1k", "2k"],
    },
    ConditionalEnumCase {
        selector_values: &["4.0", "4.1", "4.5", "4.6", "4.7", "5.0"],
        allowed_values: &["2k", "4k"],
    },
    ConditionalEnumCase {
        selector_values: &["5.0Pro"],
        allowed_values: &["1k", "2k", "4k"],
    },
];
const DREAMINA_CUSTOM_SIZES: &[ImageDimensionVariant] = &[
    ImageDimensionVariant {
        resolution: "1k",
        minimum: 512,
        maximum: 2_016,
        max_pixels: 1_763_584,
    },
    ImageDimensionVariant {
        resolution: "2k",
        minimum: 768,
        maximum: 3_072,
        max_pixels: 4_194_304,
    },
    ImageDimensionVariant {
        resolution: "4k",
        minimum: 1_536,
        maximum: 6_240,
        max_pixels: 16_777_216,
    },
];
const DREAMINA_IMAGE_CONSTRAINTS: &[Constraint] = &[
    Constraint::ExactlyOnePresenceBranch(DREAMINA_IMAGE_GEOMETRY),
    Constraint::ConditionalEnum {
        selector: Selector::ProviderModel,
        field: "resolution_type",
        cases: DREAMINA_IMAGE_RESOLUTIONS,
    },
    Constraint::ImageDimensions {
        resolution_field: "resolution_type",
        width_field: "width",
        height_field: "height",
        variants: DREAMINA_CUSTOM_SIZES,
    },
];

const DREAMINA_VIDEO_MODELS: &[&str] = &[
    "seedance2.0",
    "seedance2.0fast",
    "seedance2.0_vip",
    "seedance2.0fast_vip",
    "seedance2.0mini",
];
const DREAMINA_VIDEO_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "duration",
        required: true,
        domain: ValueDomain::IntegerClosed { min: 4, max: 15 },
    },
    DimensionContract {
        key: "ratio",
        required: true,
        domain: ValueDomain::Enum(&["1:1", "3:4", "16:9", "4:3", "9:16", "21:9"]),
    },
    DimensionContract {
        key: "resolution",
        required: true,
        domain: ValueDomain::Enum(&["720p", "1080p", "4k"]),
    },
];
const DREAMINA_VIDEO_RESOLUTIONS: &[ConditionalEnumCase] = &[
    ConditionalEnumCase {
        selector_values: &["seedance2.0_vip"],
        allowed_values: &["720p", "1080p", "4k"],
    },
    ConditionalEnumCase {
        selector_values: &[
            "seedance2.0",
            "seedance2.0fast",
            "seedance2.0fast_vip",
            "seedance2.0mini",
        ],
        allowed_values: &["720p"],
    },
];
const DREAMINA_VIDEO_CONSTRAINTS: &[Constraint] = &[Constraint::ConditionalEnum {
    selector: Selector::ProviderModel,
    field: "resolution",
    cases: DREAMINA_VIDEO_RESOLUTIONS,
}];

const GROK_VIDEO_MODELS: &[&str] = &[
    "grok-imagine-video",
    "grok-imagine-video-1.5",
    "grok-imagine-video-1.5-preview",
    "grok-imagine-video-1.5-2026-05-30",
];
const GROK_VIDEO_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "duration",
        required: true,
        domain: ValueDomain::Enum(&["6", "10"]),
    },
    DimensionContract {
        key: "resolution",
        required: true,
        domain: ValueDomain::Enum(&["480p", "720p"]),
    },
    DimensionContract {
        key: "input_image_count",
        required: true,
        domain: ValueDomain::IntegerClosed { min: 1, max: 7 },
    },
    DimensionContract {
        key: "aspect_ratio",
        required: false,
        domain: ValueDomain::Enum(&["1:1", "16:9", "9:16", "3:2", "2:3"]),
    },
];
const GROK_VIDEO_PRESENCE: &[ConditionalPresenceCase] = &[
    ConditionalPresenceCase {
        selector_values: &["grok-imagine-video"],
        required: &["aspect_ratio"],
        forbidden: &[],
    },
    ConditionalPresenceCase {
        selector_values: &[
            "grok-imagine-video-1.5",
            "grok-imagine-video-1.5-preview",
            "grok-imagine-video-1.5-2026-05-30",
        ],
        required: &[],
        forbidden: &["aspect_ratio"],
    },
];
const GROK_VIDEO_INPUT_COUNTS: &[ConditionalEnumCase] = &[
    ConditionalEnumCase {
        selector_values: &["grok-imagine-video"],
        allowed_values: &["2", "3", "4", "5", "6", "7"],
    },
    ConditionalEnumCase {
        selector_values: &[
            "grok-imagine-video-1.5",
            "grok-imagine-video-1.5-preview",
            "grok-imagine-video-1.5-2026-05-30",
        ],
        allowed_values: &["1"],
    },
];
const GROK_VIDEO_CONSTRAINTS: &[Constraint] = &[
    Constraint::ConditionalPresence {
        selector: Selector::ProviderModel,
        cases: GROK_VIDEO_PRESENCE,
    },
    Constraint::ConditionalEnum {
        selector: Selector::ProviderModel,
        field: "input_image_count",
        cases: GROK_VIDEO_INPUT_COUNTS,
    },
];

const IMAGE_OUTPUT_BASIS: &[MeteringBasisContract] = &[MeteringBasisContract {
    metric: "image_output",
    unit: "image",
    quantity_source: "request_derived",
    confidence: "exact",
    customer_sale_required: true,
}];
const CODEX_OUTPUT_BASES: &[MeteringBasisContract] = &[
    MeteringBasisContract {
        metric: "image_output",
        unit: "image",
        quantity_source: "request_derived",
        confidence: "exact",
        customer_sale_required: true,
    },
    MeteringBasisContract {
        metric: "image_output_token",
        unit: "token",
        quantity_source: "official_lookup",
        confidence: "estimated",
        customer_sale_required: false,
    },
];
const VIDEO_OUTPUT_BASES: &[MeteringBasisContract] = &[
    MeteringBasisContract {
        metric: "video_requested_second",
        unit: "second",
        quantity_source: "request_derived",
        confidence: "exact",
        customer_sale_required: true,
    },
    MeteringBasisContract {
        metric: "video_output_second",
        unit: "second",
        quantity_source: "request_derived",
        confidence: "exact",
        customer_sale_required: false,
    },
];
const GROK_VIDEO_BASES: &[MeteringBasisContract] = &[
    MeteringBasisContract {
        metric: "image_input",
        unit: "image",
        quantity_source: "request_derived",
        confidence: "exact",
        customer_sale_required: true,
    },
    MeteringBasisContract {
        metric: "video_requested_second",
        unit: "second",
        quantity_source: "request_derived",
        confidence: "exact",
        customer_sale_required: true,
    },
    MeteringBasisContract {
        metric: "video_output_second",
        unit: "second",
        quantity_source: "request_derived",
        confidence: "exact",
        customer_sale_required: false,
    },
];

const CONTRACTS: &[PricingSurfaceContract] = &[
    PricingSurfaceContract {
        contract_id: "openai-codex.images.generations.pricing-surface",
        contract_version: 2,
        provider_id: "openai-codex",
        route_operation: "images.generations",
        pricing_operation: "generation",
        command_schema: "openai.images.generation.v1",
        media_kind: "image",
        api_profiles: &["openai-images-v1"],
        provider_models: CODEX_MODELS,
        dimensions: CODEX_DIMENSIONS,
        constraints: &[],
        output_cardinality: OutputCardinality::ClosedRange { min: 1, max: 10 },
        metering_bases: CODEX_OUTPUT_BASES,
        normalizer_key: "openai.images.generation.v1",
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    },
    PricingSurfaceContract {
        contract_id: "openai-codex.images.edits.pricing-surface",
        contract_version: 2,
        provider_id: "openai-codex",
        route_operation: "images.edits",
        pricing_operation: "edit",
        command_schema: "openai.images.edit.v1",
        media_kind: "image",
        api_profiles: &["openai-images-v1"],
        provider_models: CODEX_MODELS,
        dimensions: CODEX_DIMENSIONS,
        constraints: &[],
        output_cardinality: OutputCardinality::ClosedRange { min: 1, max: 10 },
        metering_bases: CODEX_OUTPUT_BASES,
        normalizer_key: "openai.images.edit.v1",
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    },
    PricingSurfaceContract {
        contract_id: "grok-cli.images.generations.pricing-surface",
        contract_version: 2,
        provider_id: "grok-cli",
        route_operation: "images.generations",
        pricing_operation: "generation",
        command_schema: "grok-cli.images.generate.v1",
        media_kind: "image",
        api_profiles: &["xai-images-v1"],
        provider_models: &["grok-imagine-image", "grok-imagine-image-quality"],
        dimensions: GROK_IMAGE_DIMENSIONS,
        constraints: &[],
        output_cardinality: OutputCardinality::Fixed(1),
        metering_bases: IMAGE_OUTPUT_BASIS,
        normalizer_key: "grok-cli.images.generate.v1",
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    },
    PricingSurfaceContract {
        contract_id: "grok-cli.images.edits.pricing-surface",
        contract_version: 1,
        provider_id: "grok-cli",
        route_operation: "images.edits",
        pricing_operation: "edit",
        command_schema: "grok-cli.images.edit.v1",
        media_kind: "image",
        api_profiles: &["xai-images-v1"],
        provider_models: &["grok-imagine-image-quality"],
        dimensions: GROK_IMAGE_DIMENSIONS,
        constraints: &[],
        output_cardinality: OutputCardinality::Fixed(1),
        metering_bases: IMAGE_OUTPUT_BASIS,
        normalizer_key: "grok-cli.images.edit.v1",
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    },
    PricingSurfaceContract {
        contract_id: "dreamina-cli.images.generations.pricing-surface",
        contract_version: 2,
        provider_id: "dreamina-cli",
        route_operation: "images.generations",
        pricing_operation: "generation",
        command_schema: "dreamina-cli.submit.v1",
        media_kind: "image",
        api_profiles: &["dreamina-cli-images-v1", "volcengine-ark-images-v3"],
        provider_models: DREAMINA_IMAGE_MODELS,
        dimensions: DREAMINA_IMAGE_DIMENSIONS,
        constraints: DREAMINA_IMAGE_CONSTRAINTS,
        output_cardinality: OutputCardinality::ClosedRange { min: 1, max: 10 },
        metering_bases: IMAGE_OUTPUT_BASIS,
        normalizer_key: "dreamina-cli.submit.image.v1",
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    },
    PricingSurfaceContract {
        contract_id: "dreamina-cli.videos.generations.pricing-surface",
        contract_version: 2,
        provider_id: "dreamina-cli",
        route_operation: "videos.generations",
        pricing_operation: "video_generation",
        command_schema: "dreamina-cli.submit.v1",
        media_kind: "video",
        api_profiles: &[
            "dreamina-cli-videos-v1",
            "volcengine-ark-content-generation-v3",
        ],
        provider_models: DREAMINA_VIDEO_MODELS,
        dimensions: DREAMINA_VIDEO_DIMENSIONS,
        constraints: DREAMINA_VIDEO_CONSTRAINTS,
        output_cardinality: OutputCardinality::Fixed(1),
        metering_bases: VIDEO_OUTPUT_BASES,
        normalizer_key: "dreamina-cli.submit.video.v1",
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    },
    PricingSurfaceContract {
        contract_id: "grok-cli.videos.generations.pricing-surface",
        contract_version: 2,
        provider_id: "grok-cli",
        route_operation: "videos.generations",
        pricing_operation: "video_generation",
        command_schema: "grok-cli.videos.generate.v1",
        media_kind: "video",
        api_profiles: &["xai-videos-v1"],
        provider_models: GROK_VIDEO_MODELS,
        dimensions: GROK_VIDEO_DIMENSIONS,
        constraints: GROK_VIDEO_CONSTRAINTS,
        output_cardinality: OutputCardinality::Fixed(1),
        metering_bases: GROK_VIDEO_BASES,
        normalizer_key: "grok-cli.videos.generate.v1",
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    },
];

#[cfg(test)]
pub const fn registry() -> &'static [PricingSurfaceContract] {
    CONTRACTS
}

pub fn find_contract(
    provider_id: &str,
    route_operation: &str,
    command_schema: &str,
    media_kind: &str,
) -> Option<&'static PricingSurfaceContract> {
    CONTRACTS.iter().find(|contract| {
        contract.provider_id == provider_id
            && contract.route_operation == route_operation
            && contract.command_schema == command_schema
            && contract.media_kind == media_kind
    })
}

pub fn find_contract_for_pricing(
    provider_id: &str,
    pricing_operation: &str,
    command_schema: &str,
    media_kind: &str,
) -> Option<&'static PricingSurfaceContract> {
    CONTRACTS.iter().find(|contract| {
        contract.provider_id == provider_id
            && contract.pricing_operation == pricing_operation
            && contract.command_schema == command_schema
            && contract.media_kind == media_kind
    })
}

#[cfg(test)]
pub fn registry_hash() -> String {
    let canonical = serde_json::to_vec(&(REGISTRY_SCHEMA_VERSION, CONTRACTS))
        .expect("static pricing registry serialization");
    hex::encode(Sha256::digest(canonical))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract(id: &str) -> &'static PricingSurfaceContract {
        registry()
            .iter()
            .find(|contract| contract.contract_id == id)
            .expect("registered pricing surface")
    }

    fn value<'a>(key: &'a str, value: &'a str) -> DimensionValue<'a> {
        DimensionValue { key, value }
    }

    #[test]
    fn registry_identity_and_hash_are_deterministic() {
        assert_eq!(registry().len(), 7);
        assert_eq!(registry_hash(), registry_hash());
        assert_eq!(registry_hash().len(), 64);
        let generation = registry()[0]
            .binding_snapshot(ExactSurfaceIdentity {
                api_profile: "openai-images-v1",
                provider_model_id: "gpt-image-2",
                public_model_id: "gpt-image-2",
                service_tier: "standard",
                execution_surface: "provider_cli",
            })
            .unwrap();
        let edit = registry()[1]
            .binding_snapshot(ExactSurfaceIdentity {
                api_profile: "openai-images-v1",
                provider_model_id: "gpt-image-2",
                public_model_id: "gpt-image-2",
                service_tier: "standard",
                execution_surface: "provider_cli",
            })
            .unwrap();
        assert_eq!(generation.contract_hash.len(), 64);
        assert_ne!(generation.contract_hash, edit.contract_hash);
        assert_ne!(generation.contract_key, edit.contract_key);
    }

    #[test]
    fn codex_generation_accepts_dynamic_sizes_and_rejects_invalid_variants() {
        let contract = contract("openai-codex.images.generations.pricing-surface");
        for size in ["auto", "1024x1024", "3840x1280", "16:9"] {
            assert_eq!(
                contract.validate(&SurfaceRequest {
                    provider_model_id: "gpt-image-2",
                    dimensions: &[value("quality", "high"), value("size", size)],
                    output_count: 10,
                }),
                Ok(()),
                "{size}"
            );
        }
        for size in ["1025x1024", "3840x1264", "4:0", "100:1"] {
            assert_eq!(
                contract.validate(&SurfaceRequest {
                    provider_model_id: "gpt-image-2",
                    dimensions: &[value("quality", "high"), value("size", size)],
                    output_count: 1,
                }),
                Err(ValidationError::InvalidDimension("size")),
                "{size}"
            );
        }
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "gpt-image-2",
                dimensions: &[value("quality", "ultra"), value("size", "auto")],
                output_count: 1,
            }),
            Err(ValidationError::InvalidDimension("quality"))
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "gpt-image-2",
                dimensions: &[value("quality", "high"), value("size", "auto")],
                output_count: 11,
            }),
            Err(ValidationError::InvalidOutputCount)
        );
    }

    #[test]
    fn customer_sale_bases_are_explicit_and_exact() {
        for contract in registry() {
            let required = contract
                .metering_bases
                .iter()
                .filter(|basis| basis.customer_sale_required)
                .collect::<Vec<_>>();
            assert!(
                !required.is_empty(),
                "{} must name its customer sale basis",
                contract.contract_id
            );
            assert!(required.iter().all(|basis| {
                basis.quantity_source == "request_derived" && basis.confidence == "exact"
            }));
        }

        let grok_video = contract("grok-cli.videos.generations.pricing-surface");
        assert_eq!(
            grok_video
                .metering_bases
                .iter()
                .filter(|basis| basis.customer_sale_required)
                .map(|basis| basis.metric)
                .collect::<Vec<_>>(),
            vec!["image_input", "video_requested_second"]
        );

        let codex = contract("openai-codex.images.generations.pricing-surface");
        assert_eq!(
            codex
                .metering_bases
                .iter()
                .filter(|basis| basis.customer_sale_required)
                .map(|basis| basis.metric)
                .collect::<Vec<_>>(),
            vec!["image_output"]
        );
    }

    #[test]
    fn codex_edit_has_a_distinct_identity_with_the_same_pricing_shape() {
        let generation = contract("openai-codex.images.generations.pricing-surface");
        let edit = contract("openai-codex.images.edits.pricing-surface");

        assert_ne!(generation.command_schema, edit.command_schema);
        assert_ne!(generation.route_operation, edit.route_operation);
        assert_eq!(generation.dimensions, edit.dimensions);
        assert_eq!(
            edit.validate(&SurfaceRequest {
                provider_model_id: "gpt-image-2-2026-04-21",
                dimensions: &[value("quality", "medium"), value("size", "1536x1024")],
                output_count: 2,
            }),
            Ok(())
        );
    }

    #[test]
    fn grok_image_matches_the_current_cli_projection() {
        let contract = contract("grok-cli.images.generations.pricing-surface");
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "grok-imagine-image-quality",
                dimensions: &[value("aspect_ratio", "19.5:9"), value("resolution", "1k"),],
                output_count: 1,
            }),
            Ok(())
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "grok-imagine-image",
                dimensions: &[value("aspect_ratio", "1:1"), value("resolution", "1k"),],
                output_count: 1,
            }),
            Ok(())
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "grok-imagine-image-quality",
                dimensions: &[value("aspect_ratio", "1:1"), value("resolution", "2k"),],
                output_count: 1,
            }),
            Err(ValidationError::InvalidDimension("resolution"))
        );
    }

    #[test]
    fn grok_edit_keeps_a_distinct_billing_operation_with_the_same_shape() {
        let generation = contract("grok-cli.images.generations.pricing-surface");
        let edit = contract("grok-cli.images.edits.pricing-surface");
        let identity = ExactSurfaceIdentity {
            api_profile: "xai-images-v1",
            provider_model_id: "grok-imagine-image-quality",
            public_model_id: "grok-imagine-image-quality",
            service_tier: "standard",
            execution_surface: "provider_cli",
        };

        assert_eq!(generation.pricing_operation, "generation");
        assert_eq!(edit.pricing_operation, "edit");
        assert_ne!(generation.command_schema, edit.command_schema);
        assert_ne!(
            generation.binding_snapshot(identity).unwrap().contract_key,
            edit.binding_snapshot(identity).unwrap().contract_key
        );
        assert_eq!(generation.dimensions, edit.dimensions);
        assert_eq!(
            edit.validate(&SurfaceRequest {
                provider_model_id: "grok-imagine-image-quality",
                dimensions: &[value("aspect_ratio", "1:1"), value("resolution", "1k")],
                output_count: 1,
            }),
            Ok(())
        );
    }

    #[test]
    fn dreamina_image_enforces_geometry_models_and_resolution_specific_sizes() {
        let contract = contract("dreamina-cli.images.generations.pricing-surface");
        let ratio = SurfaceRequest {
            provider_model_id: "5.0",
            dimensions: &[value("resolution_type", "2k"), value("ratio", "16:9")],
            output_count: 3,
        };
        assert_eq!(contract.validate(&ratio), Ok(()));

        let custom = SurfaceRequest {
            provider_model_id: "5.0Pro",
            dimensions: &[
                value("resolution_type", "2k"),
                value("width", "1536"),
                value("height", "1024"),
            ],
            output_count: 2,
        };
        assert_eq!(contract.validate(&custom), Ok(()));

        assert_eq!(
            contract.validate(&SurfaceRequest {
                dimensions: &[
                    value("resolution_type", "2k"),
                    value("ratio", "16:9"),
                    value("width", "1536"),
                    value("height", "1024"),
                ],
                ..custom
            }),
            Err(ValidationError::ConstraintViolation(
                "exactly_one_presence_branch"
            ))
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "3.0",
                dimensions: &[value("resolution_type", "4k"), value("ratio", "1:1")],
                output_count: 1,
            }),
            Err(ValidationError::ConstraintViolation("conditional_enum"))
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                dimensions: &[
                    value("resolution_type", "2k"),
                    value("width", "3072"),
                    value("height", "3072"),
                ],
                ..custom
            }),
            Err(ValidationError::ConstraintViolation("image_dimensions"))
        );
    }

    #[test]
    fn dreamina_video_enforces_duration_and_model_resolution_pairing() {
        let contract = contract("dreamina-cli.videos.generations.pricing-surface");
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "seedance2.0fast",
                dimensions: &[
                    value("duration", "15"),
                    value("ratio", "21:9"),
                    value("resolution", "720p"),
                ],
                output_count: 1,
            }),
            Ok(())
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "seedance2.0fast",
                dimensions: &[
                    value("duration", "16"),
                    value("ratio", "16:9"),
                    value("resolution", "720p"),
                ],
                output_count: 1,
            }),
            Err(ValidationError::InvalidDimension("duration"))
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "seedance2.0",
                dimensions: &[
                    value("duration", "5"),
                    value("ratio", "16:9"),
                    value("resolution", "1080p"),
                ],
                output_count: 1,
            }),
            Err(ValidationError::ConstraintViolation("conditional_enum"))
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "seedance2.0_vip",
                dimensions: &[
                    value("duration", "5"),
                    value("ratio", "16:9"),
                    value("resolution", "4k"),
                ],
                output_count: 1,
            }),
            Ok(())
        );
    }

    #[test]
    fn grok_video_contract_covers_only_the_cli_executable_pricing_domain() {
        let contract = contract("grok-cli.videos.generations.pricing-surface");
        let image_to_video = SurfaceRequest {
            provider_model_id: "grok-imagine-video-1.5",
            dimensions: &[
                value("duration", "6"),
                value("input_image_count", "1"),
                value("resolution", "480p"),
            ],
            output_count: 1,
        };
        assert_eq!(contract.validate(&image_to_video), Ok(()));
        assert_eq!(
            contract.validate(&SurfaceRequest {
                dimensions: &[
                    value("duration", "6"),
                    value("input_image_count", "1"),
                    value("resolution", "480p"),
                    value("aspect_ratio", "16:9"),
                ],
                ..image_to_video
            }),
            Err(ValidationError::ConstraintViolation("conditional_presence"))
        );
        assert_eq!(
            contract.validate_shape(&SurfaceRequest {
                provider_model_id: "grok-imagine-video",
                dimensions: &[
                    value("duration", "10"),
                    value("input_image_count", "2"),
                    value("resolution", "720p"),
                    value("aspect_ratio", "2:3"),
                ],
                output_count: 1,
            }),
            Ok(())
        );
        assert_eq!(
            contract.validate(&SurfaceRequest {
                provider_model_id: "grok-imagine-video",
                dimensions: &[
                    value("duration", "10"),
                    value("input_image_count", "1"),
                    value("resolution", "720p"),
                    value("aspect_ratio", "2:3"),
                ],
                output_count: 1,
            }),
            Err(ValidationError::ConstraintViolation("conditional_enum"))
        );
    }
}
