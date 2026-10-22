use super::*;

const IMAGE_MODELS: &[&str] = &["4.0", "4.1", "4.5", "4.6", "4.7", "5.0", "5.0Pro"];
const REFERENCE_VIDEO_MODELS: &[&str] = &[
    "seedance1.0fast",
    "seedance1.5pro",
    "seedance2.0",
    "seedance2.0fast",
    "seedance2.0_vip",
    "seedance2.0fast_vip",
    "seedance2.0mini",
    "seedance2.5",
];
const FRAMES_VIDEO_MODELS: &[&str] = &[
    "seedance1.5pro",
    "seedance2.0",
    "seedance2.0fast",
    "seedance2.0_vip",
    "seedance2.0fast_vip",
    "seedance2.0mini",
    "seedance2.5",
];
const IMAGE_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "workflow",
        required: true,
        domain: ValueDomain::Enum(&["text2image", "image2image"]),
    },
    DimensionContract {
        key: "resolution_type",
        required: true,
        domain: ValueDomain::Enum(&["1.5k", "2k", "4k"]),
    },
    DREAMINA_IMAGE_DIMENSIONS[1],
    DREAMINA_IMAGE_DIMENSIONS[2],
    DREAMINA_IMAGE_DIMENSIONS[3],
    DimensionContract {
        key: "input_image_count",
        required: false,
        domain: ValueDomain::IntegerClosed { min: 1, max: 10 },
    },
];
const CUSTOM_SIZES: &[ImageDimensionVariant] = &[
    ImageDimensionVariant {
        resolution: "1.5k",
        minimum: 972,
        maximum: 2268,
        max_pixels: 2_359_296,
    },
    DREAMINA_CUSTOM_SIZES[1],
    DREAMINA_CUSTOM_SIZES[2],
];
const UPSCALE_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "workflow",
        required: true,
        domain: ValueDomain::Enum(&["image_upscale"]),
    },
    DimensionContract {
        key: "resolution_type",
        required: true,
        domain: ValueDomain::Enum(&["2k", "4k", "8k"]),
    },
    DimensionContract {
        key: "input_image_count",
        required: true,
        domain: ValueDomain::Enum(&["1"]),
    },
];
const VIDEO_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "workflow",
        required: true,
        domain: ValueDomain::Enum(&["image2video", "frames2video", "multimodal2video"]),
    },
    DREAMINA_VIDEO_DIMENSIONS[0],
    DimensionContract {
        required: false,
        ..DREAMINA_VIDEO_DIMENSIONS[1]
    },
    DREAMINA_VIDEO_DIMENSIONS[2],
    DimensionContract {
        key: "input_image_count",
        required: true,
        domain: ValueDomain::IntegerClosed { min: 0, max: 30 },
    },
    DimensionContract {
        key: "input_video_count",
        required: false,
        domain: ValueDomain::IntegerClosed { min: 0, max: 10 },
    },
    DimensionContract {
        key: "input_audio_count",
        required: false,
        domain: ValueDomain::IntegerClosed { min: 0, max: 10 },
    },
];

const MULTIFRAME_DIMENSIONS: &[DimensionContract] = &[
    DimensionContract {
        key: "workflow",
        required: true,
        domain: ValueDomain::Enum(&["multiframe2video"]),
    },
    DimensionContract {
        key: "duration_ms",
        required: true,
        domain: ValueDomain::IntegerClosed {
            min: 2_000,
            max: 152_000,
        },
    },
    DimensionContract {
        key: "resolution",
        required: true,
        domain: ValueDomain::Enum(&["720p", "1080p"]),
    },
    DimensionContract {
        key: "input_image_count",
        required: true,
        domain: ValueDomain::IntegerClosed { min: 2, max: 20 },
    },
];
const MULTIFRAME_BASIS: &[MeteringBasisContract] = &[MeteringBasisContract {
    metric: "video_requested_millisecond",
    unit: "millisecond",
    quantity_source: "request_derived",
    confidence: "exact",
    customer_sale_required: true,
}];

const fn contract(
    workflow: &'static str,
    id: &'static str,
    media: &'static str,
    models: &'static [&'static str],
    dimensions: &'static [DimensionContract],
) -> PricingSurfaceContract {
    let image = matches!(media.as_bytes(), b"image");
    PricingSurfaceContract {
        contract_id: id,
        contract_version: 1,
        provider_id: "dreamina-cli",
        route_operation: if image {
            "images.generations"
        } else {
            "videos.generations"
        },
        pricing_operation: if image {
            "generation"
        } else {
            "video_generation"
        },
        command_schema: "dreamina-cli.submit.v1",
        media_kind: media,
        api_profiles: if image {
            &["dreamina-cli-images-v1", "volcengine-ark-images-v3"]
        } else {
            &[
                "dreamina-cli-videos-v1",
                "volcengine-ark-content-generation-v3",
            ]
        },
        provider_models: models,
        dimensions,
        constraints: &[Constraint::DreaminaWorkflow],
        output_cardinality: if image {
            OutputCardinality::ClosedRange { min: 1, max: 10 }
        } else {
            OutputCardinality::Fixed(1)
        },
        metering_bases: if image {
            IMAGE_OUTPUT_BASIS
        } else {
            VIDEO_OUTPUT_BASES
        },
        normalizer_key: workflow,
        normalizer_revision: 1,
        support: SurfaceSupport::Supported,
    }
}

pub(super) const CONTRACTS: &[PricingSurfaceContract] = &[
    PricingSurfaceContract {
        metering_bases: MULTIFRAME_BASIS,
        ..contract(
            "multiframe2video",
            "dreamina-cli.multiframe2video.pricing-surface",
            "video",
            &["multiframe2video"],
            MULTIFRAME_DIMENSIONS,
        )
    },
    contract(
        "text2image",
        "dreamina-cli.text2image.extended.pricing-surface",
        "image",
        &["5.0Pro"],
        IMAGE_DIMENSIONS,
    ),
    contract(
        "image2image",
        "dreamina-cli.image2image.pricing-surface",
        "image",
        IMAGE_MODELS,
        IMAGE_DIMENSIONS,
    ),
    contract(
        "image_upscale",
        "dreamina-cli.image_upscale.pricing-surface",
        "image",
        &["image_upscale"],
        UPSCALE_DIMENSIONS,
    ),
    contract(
        "image2video",
        "dreamina-cli.image2video.pricing-surface",
        "video",
        REFERENCE_VIDEO_MODELS,
        VIDEO_DIMENSIONS,
    ),
    contract(
        "frames2video",
        "dreamina-cli.frames2video.pricing-surface",
        "video",
        FRAMES_VIDEO_MODELS,
        VIDEO_DIMENSIONS,
    ),
    contract(
        "multimodal2video",
        "dreamina-cli.multimodal2video.pricing-surface",
        "video",
        DREAMINA_VIDEO_MODELS,
        VIDEO_DIMENSIONS,
    ),
];

pub(super) fn validate(request: &SurfaceRequest<'_>) -> Result<(), ValidationError> {
    let fail = || ValidationError::ConstraintViolation("dreamina_workflow");
    let count = |key| {
        request
            .value(key)
            .and_then(|value| value.parse::<u32>().ok())
    };
    let workflow = request.value("workflow").ok_or_else(fail)?;
    match workflow {
        "text2image" | "image2image" => {
            let resolution = request.value("resolution_type").ok_or_else(fail)?;
            if (workflow == "text2image"
                && (request.provider_model_id != "5.0Pro"
                    || resolution != "1.5k"
                    || count("input_image_count").is_some()))
                || (workflow == "image2image"
                    && !count("input_image_count").is_some_and(|n| (1..=10).contains(&n)))
                || (resolution == "1.5k" && request.provider_model_id != "5.0Pro")
            {
                return Err(fail());
            }
            Constraint::ExactlyOnePresenceBranch(DREAMINA_IMAGE_GEOMETRY).validate(request)?;
            Constraint::ImageDimensions {
                resolution_field: "resolution_type",
                width_field: "width",
                height_field: "height",
                variants: CUSTOM_SIZES,
            }
            .validate(request)
        }
        "image_upscale" => {
            if request.output_count == 1 {
                Ok(())
            } else {
                Err(fail())
            }
        }
        "multiframe2video" => {
            let duration_ms = count("duration_ms").ok_or_else(fail)?;
            let transitions = count("input_image_count")
                .ok_or_else(fail)?
                .checked_sub(1)
                .ok_or_else(fail)?;
            if (transitions * 1000..=transitions * 8000).contains(&duration_ms) {
                Ok(())
            } else {
                Err(fail())
            }
        }
        "image2video" | "frames2video" | "multimodal2video" => {
            let model = request.provider_model_id;
            let duration = count("duration").ok_or_else(fail)?;
            let range = match model {
                "seedance1.0fast" => 5..=10,
                "seedance1.5pro" => 5..=12,
                "seedance2.5" => 4..=30,
                _ => 4..=15,
            };
            let resolution_ok = match model {
                "seedance2.5" => {
                    matches!(request.value("resolution"), Some("480p" | "720p" | "1080p"))
                }
                "seedance2.0_vip" => {
                    matches!(request.value("resolution"), Some("720p" | "1080p" | "4k"))
                }
                _ => request.value("resolution") == Some("720p"),
            };
            if !range.contains(&duration) || !resolution_ok {
                return Err(fail());
            }
            if workflow != "multimodal2video" {
                if count("input_image_count") != Some(if workflow == "image2video" { 1 } else { 2 })
                    || request.value("input_video_count").is_some()
                    || request.value("input_audio_count").is_some()
                    || (model == "seedance2.5" && request.value("ratio").is_some())
                {
                    return Err(fail());
                }
            } else {
                let images = count("input_image_count").ok_or_else(fail)?;
                let videos = count("input_video_count").ok_or_else(fail)?;
                let audios = count("input_audio_count").ok_or_else(fail)?;
                let total = images + videos + audios;
                let (max_images, max_timed, max_total) = if model == "seedance2.5" {
                    (30, 10, 50)
                } else {
                    (9, 3, 12)
                };
                if images > max_images
                    || videos > max_timed
                    || audios > max_timed
                    || total == 0
                    || total > max_total
                    || (model != "seedance2.5" && images + videos == 0)
                    || request.value("ratio").is_none()
                {
                    return Err(fail());
                }
            }
            Ok(())
        }
        _ => Err(fail()),
    }
}
