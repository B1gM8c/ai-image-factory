use std::collections::BTreeMap;

use image_api_contracts::{
    ark::{ARK_CONTENT_GENERATION_API_PROFILE, ARK_IMAGES_API_PROFILE},
    dreamina::{DREAMINA_IMAGES_API_PROFILE, DREAMINA_VIDEOS_API_PROFILE},
};
use image_provider_dreamina_cli::{DreaminaSubmitRequestV1, ImageResolution, TextToImageRequestV1};

use crate::admission::AdmissionError;

/// Facts derived only from the validated, immutable provider command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DreaminaPricingFacts {
    pub provider_model_id: String,
    pub output_count: u32,
    pub output_billable_units: u32,
    pub media_kind: &'static str,
    pub dimensions: BTreeMap<String, String>,
}

pub(crate) fn dreamina_pricing_facts(
    request: &DreaminaSubmitRequestV1,
    api_profile: &str,
) -> Result<DreaminaPricingFacts, AdmissionError> {
    let mut facts = match request {
        DreaminaSubmitRequestV1::TextToImage(command) => {
            let mut facts = image_facts(command);
            // The old immutable surface did not contain the CLI's 1.5k resolution.
            // Its existing model/geometry bindings remain unchanged.
            if command.resolution() == ImageResolution::K1_5 {
                facts
                    .dimensions
                    .insert("workflow".into(), "text2image".into());
            }
            facts
        }
        DreaminaSubmitRequestV1::ImageToImage(command) => {
            let mut facts = image_facts(command.parameters());
            facts
                .dimensions
                .insert("workflow".into(), "image2image".into());
            facts.dimensions.insert(
                "input_image_count".into(),
                command.images().len().to_string(),
            );
            facts
        }
        DreaminaSubmitRequestV1::ImageUpscale(command) => DreaminaPricingFacts {
            provider_model_id: "image_upscale".into(),
            output_count: 1,
            output_billable_units: 1,
            media_kind: "image",
            dimensions: BTreeMap::from([
                ("workflow".into(), "image_upscale".into()),
                ("input_image_count".into(), "1".into()),
                (
                    "resolution_type".into(),
                    command.resolution().as_str().into(),
                ),
            ]),
        },
        DreaminaSubmitRequestV1::TextToVideo(command) => video_facts(
            command.model().as_str(),
            command.duration_seconds(),
            command.resolution().as_str(),
            Some(command.ratio().as_str()),
        ),
        DreaminaSubmitRequestV1::ImageToVideo(command) => {
            let options = command.options();
            let mut facts = video_facts(
                options.model().as_str(),
                options.duration_seconds(),
                options.resolution().as_str(),
                options.ratio().map(|ratio| ratio.as_str()),
            );
            facts
                .dimensions
                .insert("workflow".into(), "image2video".into());
            facts
                .dimensions
                .insert("input_image_count".into(), "1".into());
            facts
        }
        DreaminaSubmitRequestV1::FramesToVideo(command) => {
            let options = command.options();
            let mut facts = video_facts(
                options.model().as_str(),
                options.duration_seconds(),
                options.resolution().as_str(),
                options.ratio().map(|ratio| ratio.as_str()),
            );
            facts
                .dimensions
                .insert("workflow".into(), "frames2video".into());
            facts
                .dimensions
                .insert("input_image_count".into(), "2".into());
            facts
        }
        DreaminaSubmitRequestV1::MultimodalToVideo(command) => {
            let mut facts = video_facts(
                command.model().as_str(),
                command.duration_seconds(),
                command.resolution().as_str(),
                Some(command.ratio().as_str()),
            );
            facts
                .dimensions
                .insert("workflow".into(), "multimodal2video".into());
            for (key, count) in [
                ("input_image_count", command.images().len()),
                ("input_video_count", command.videos().len()),
                ("input_audio_count", command.audios().len()),
            ] {
                facts.dimensions.insert(key.into(), count.to_string());
            }
            facts
        }
        DreaminaSubmitRequestV1::MultiframeToVideo(command) => {
            let duration_ms: u32 = command
                .transitions()
                .iter()
                .map(|transition| transition.duration_ms())
                .sum();
            DreaminaPricingFacts {
                provider_model_id: "multiframe2video".into(),
                output_count: 1,
                // This is the existing quota unit, not the quantity used for customer pricing.
                output_billable_units: duration_ms.div_ceil(1000),
                media_kind: "video",
                dimensions: BTreeMap::from([
                    ("workflow".into(), "multiframe2video".into()),
                    ("duration_ms".into(), duration_ms.to_string()),
                    (
                        "input_image_count".into(),
                        command.images().len().to_string(),
                    ),
                    ("resolution".into(), command.resolution().as_str().into()),
                ]),
            }
        }
    };
    let profile_matches = match facts.media_kind {
        "image" => matches!(
            api_profile,
            DREAMINA_IMAGES_API_PROFILE | ARK_IMAGES_API_PROFILE
        ),
        "video" => matches!(
            api_profile,
            DREAMINA_VIDEOS_API_PROFILE | ARK_CONTENT_GENERATION_API_PROFILE
        ),
        _ => false,
    };
    if !profile_matches {
        return Err(AdmissionError::PricingUnavailable);
    }
    // Preserve absent ratio for reference-driven video, rather than inventing a default.
    facts.dimensions.retain(|_, value| !value.is_empty());
    Ok(facts)
}

fn image_facts(command: &TextToImageRequestV1) -> DreaminaPricingFacts {
    let mut dimensions = BTreeMap::from([(
        "resolution_type".into(),
        command.resolution().as_str().into(),
    )]);
    if let Some(ratio) = command.ratio() {
        dimensions.insert("ratio".into(), ratio.as_str().into());
    } else if let (Some(width), Some(height)) = (command.width(), command.height()) {
        dimensions.insert("width".into(), width.to_string());
        dimensions.insert("height".into(), height.to_string());
    }
    DreaminaPricingFacts {
        provider_model_id: command.model().as_str().into(),
        output_count: u32::from(command.generate_num()),
        output_billable_units: 1,
        media_kind: "image",
        dimensions,
    }
}

fn video_facts(
    model: &str,
    duration: u8,
    resolution: &str,
    ratio: Option<&str>,
) -> DreaminaPricingFacts {
    let mut dimensions = BTreeMap::from([
        ("duration".into(), duration.to_string()),
        ("resolution".into(), resolution.into()),
    ]);
    if let Some(ratio) = ratio {
        dimensions.insert("ratio".into(), ratio.into());
    }
    DreaminaPricingFacts {
        provider_model_id: model.into(),
        output_count: 1,
        output_billable_units: u32::from(duration),
        media_kind: "video",
        dimensions,
    }
}

/// Components without a workflow remain the original text price group. Explicit groups
/// never inherit a missing outcome or rate from that legacy group.
pub(in crate::pricing) fn price_groups(
    version: &crate::pricing::PriceBookVersionView,
) -> Result<Vec<(Option<String>, crate::pricing::PriceBookVersionView)>, AdmissionError> {
    let mut groups = BTreeMap::<Option<String>, Vec<crate::pricing::PriceComponentView>>::new();
    for component in &version.components {
        let object = component
            .dimensions
            .as_object()
            .ok_or(AdmissionError::PricingUnavailable)?;
        let workflow = match object.get("workflow") {
            None => None,
            Some(value) => match value.as_str() {
                Some(
                    value @ ("text2image" | "image2image" | "image_upscale" | "image2video"
                    | "frames2video" | "multimodal2video" | "multiframe2video"),
                ) => Some(value.to_owned()),
                _ => return Err(AdmissionError::PricingUnavailable),
            },
        };
        groups.entry(workflow).or_default().push(component.clone());
    }
    Ok(groups
        .into_iter()
        .map(|(workflow, components)| {
            let mut group = version.clone();
            group.components = components;
            (workflow, group)
        })
        .collect())
}

pub(in crate::pricing) fn select_price_group(
    version: &crate::pricing::PriceBookVersionView,
    workflow: Option<&str>,
) -> Result<crate::pricing::PriceBookVersionView, AdmissionError> {
    price_groups(version)?
        .into_iter()
        .find(|(candidate, _)| candidate.as_deref() == workflow)
        .map(|(_, group)| group)
        .ok_or(AdmissionError::PricingUnavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pricing::surface_contract::{
        DimensionValue, SurfaceRequest, find_contract_for_workflow,
    };
    use image_provider_dreamina_cli::*;

    fn image(name: &str) -> StagedImageV1 {
        StagedImageV1::new(name, "a".repeat(64), 1600, 900).unwrap()
    }
    fn parameters(resolution: ImageResolution) -> TextToImageRequestV1 {
        TextToImageRequestV1::new(
            "p",
            ImageModelVersion::V5_0Pro,
            ImageRatio::R16x9,
            resolution,
            2,
        )
        .unwrap()
    }
    fn validate(command: DreaminaSubmitRequestV1, profile: &str) -> DreaminaPricingFacts {
        let facts = dreamina_pricing_facts(&command, profile).unwrap();
        let contract = find_contract_for_workflow(
            "dreamina-cli",
            if facts.media_kind == "image" {
                "generation"
            } else {
                "video_generation"
            },
            DREAMINA_SUBMIT_COMMAND_SCHEMA,
            facts.media_kind,
            facts.dimensions.get("workflow").map(String::as_str),
        )
        .unwrap();
        let dimensions = facts
            .dimensions
            .iter()
            .map(|(key, value)| DimensionValue { key, value })
            .collect::<Vec<_>>();
        contract
            .validate(&SurfaceRequest {
                provider_model_id: &facts.provider_model_id,
                dimensions: &dimensions,
                output_count: facts.output_count,
            })
            .unwrap();
        facts
    }

    #[test]
    fn images_keep_legacy_facts_and_isolate_extended_and_reference_workflows() {
        let legacy = validate(
            parameters(ImageResolution::K2).into(),
            DREAMINA_IMAGES_API_PROFILE,
        );
        assert!(!legacy.dimensions.contains_key("workflow"));
        assert!(!legacy.dimensions.contains_key("input_image_count"));
        let extended = validate(
            parameters(ImageResolution::K1_5).into(),
            DREAMINA_IMAGES_API_PROFILE,
        );
        assert_eq!(extended.dimensions["workflow"], "text2image");
        let reference = validate(
            ImageToImageRequestV1::new(
                vec![image("one.png"), image("two.png")],
                parameters(ImageResolution::K1_5),
            )
            .unwrap()
            .into(),
            ARK_IMAGES_API_PROFILE,
        );
        assert_eq!(reference.dimensions["workflow"], "image2image");
        assert_eq!(reference.dimensions["input_image_count"], "2");
        assert_eq!(reference.output_count, 2);
        let upscale = validate(
            ImageUpscaleRequestV1::new(image("one.png"), UpscaleResolution::K8).into(),
            DREAMINA_IMAGES_API_PROFILE,
        );
        assert_eq!(upscale.provider_model_id, "image_upscale");
        assert_eq!(upscale.output_count, 1);
    }

    #[test]
    fn reference_video_facts_preserve_legacy_models_and_absent_seedance25_ratio() {
        for (model, duration) in [
            (VideoModelVersion::Seedance1_0Fast, 10),
            (VideoModelVersion::Seedance1_5Pro, 12),
            (VideoModelVersion::Seedance2_5, 30),
        ] {
            let options =
                ReferenceVideoOptionsV1::new("p", model, None, duration, VideoResolution::P720)
                    .unwrap();
            let facts = validate(
                ImageToVideoRequestV1::new(image("first.png"), options.clone())
                    .unwrap()
                    .into(),
                DREAMINA_VIDEOS_API_PROFILE,
            );
            assert_eq!(facts.output_billable_units, u32::from(duration));
            assert_eq!(facts.dimensions["input_image_count"], "1");
            assert!(!facts.dimensions.contains_key("ratio"));
            if model != VideoModelVersion::Seedance1_0Fast {
                let frames = validate(
                    FramesToVideoRequestV1::new(image("first.png"), image("last.png"), options)
                        .unwrap()
                        .into(),
                    ARK_CONTENT_GENERATION_API_PROFILE,
                );
                assert_eq!(frames.dimensions["input_image_count"], "2");
            }
        }
    }

    #[test]
    fn multimodal_uses_actual_counts_without_enumerating_the_count_domain() {
        let images = (0..30).map(|n| image(&format!("image-{n}.png"))).collect();
        let timed = |prefix: &str| {
            (0..10)
                .map(|n| {
                    StagedTimedMediaV1::new(format!("{prefix}-{n}.bin"), "a".repeat(64), 2000)
                        .unwrap()
                })
                .collect()
        };
        let command = MultimodalToVideoRequestV1::new(
            images,
            timed("video"),
            timed("audio"),
            "",
            VideoModelVersion::Seedance2_5,
            VideoRatio::R16x9,
            30,
            VideoResolution::P1080,
        )
        .unwrap();
        let facts = validate(command.into(), ARK_CONTENT_GENERATION_API_PROFILE);
        assert_eq!(facts.dimensions["input_image_count"], "30");
        assert_eq!(facts.dimensions["input_video_count"], "10");
        assert_eq!(facts.dimensions["input_audio_count"], "10");
        assert_eq!(facts.output_billable_units, 30);
    }

    #[test]
    fn multiframe_preserves_exact_milliseconds_separately_from_quota_seconds() {
        let command = MultiframeToVideoRequestV1::new(
            vec![image("a.png"), image("b.png")],
            vec![VideoTransitionV1::new("p", 2500).unwrap()],
            VideoResolution::P720,
        )
        .unwrap();
        let facts = validate(command.into(), DREAMINA_VIDEOS_API_PROFILE);
        assert_eq!(facts.output_billable_units, 3);
        assert_eq!(facts.dimensions["duration_ms"], "2500");
        assert!(!facts.dimensions.contains_key("duration"));
        assert!(
            dreamina_pricing_facts(
                &parameters(ImageResolution::K2).into(),
                DREAMINA_VIDEOS_API_PROFILE
            )
            .is_err()
        );
    }
}
