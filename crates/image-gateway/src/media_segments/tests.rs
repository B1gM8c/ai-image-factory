use super::*;

fn pending() -> Segmentation {
    Segmentation {
        object: "media.segmentation".into(),
        id: "seg_00000000000000000000000000000001".into(),
        asset_id: "img_00000000000000000000000000000002".into(),
        status: SegmentStatus::Processing,
        schema_version: SCHEMA_VERSION.into(),
        image: ImageSize {
            width: 100,
            height: 80,
            coordinate_system: "pixel_xyxy".into(),
        },
        groups: vec![],
        error: None,
    }
}

fn candidate() -> BboxCandidate {
    BboxCandidate {
        groups: vec![CandidateGroup {
            parent_index: None,
            name: "橘猫".into(),
            category: "主体".into(),
            bbox_xyxy: [10, 10, 80, 70],
            confidence: 0.9,
            segments: vec![CandidateItem {
                name: "猫头".into(),
                bbox_xyxy: [9, 12, 60, 40],
                confidence: 0.8,
            }],
        }],
    }
}

#[test]
fn expands_parents_without_altering_children_and_produces_stable_ids() {
    let mut result = pending();
    validate_candidate(candidate(), &mut result).unwrap();
    assert_eq!(result.status, SegmentStatus::Completed);
    assert_eq!(result.groups[0].bbox_xyxy, [9, 10, 80, 70]);
    assert_eq!(result.groups[0].segments[0].bbox_xyxy, [9, 12, 60, 40]);
    let mut repeated = pending();
    validate_candidate(candidate(), &mut repeated).unwrap();
    assert_eq!(result, repeated);
    let json = serde_json::to_value(result).unwrap();
    assert_eq!(json["groups"][0]["名称"], "橘猫");
    assert_eq!(json["groups"][0]["类别"], "主体");
    assert_eq!(json["groups"][0]["mask_key"], "group_0");
}

#[test]
fn validates_nested_parents_in_any_array_order() {
    let mut data = candidate();
    let mut middle = data.groups[0].clone();
    middle.parent_index = Some(2);
    middle.bbox_xyxy = [20, 20, 40, 40];
    middle.segments.clear();
    let mut root = middle.clone();
    root.parent_index = None;
    root.bbox_xyxy = [30, 30, 50, 50];
    data.groups[0].parent_index = Some(1);
    data.groups.extend([middle, root]);
    let mut result = pending();
    validate_candidate(data, &mut result).unwrap();
    assert_eq!(result.groups[2].bbox_xyxy, [9, 10, 80, 70]);
    assert_eq!(result.groups[1].bbox_xyxy, [9, 10, 80, 70]);
    assert_eq!(
        result.groups[0].parent_group_id,
        Some(result.groups[1].group_id.clone())
    );
}

#[test]
fn rejects_cross_field_geometry_and_non_chinese_candidates() {
    let mutations: [fn(&mut BboxCandidate); 11] = [
        |c| c.groups[0].bbox_xyxy = [80, 0, 10, 50],
        |c| c.groups[0].bbox_xyxy[2] = 101,
        |c| c.groups[0].segments[0].bbox_xyxy[3] = 81,
        |c| c.groups[0].parent_index = Some(9),
        |c| c.groups[0].parent_index = Some(0),
        |c| c.groups[0].confidence = f64::NAN,
        |c| c.groups[0].name = "cat".into(),
        |c| c.groups[0].category = "subject".into(),
        |c| c.groups[0].segments[0].confidence = 1.1,
        |c| c.groups[0].name = "<b>猫</b>".into(),
        |c| c.groups[0].name = "猫\u{202e}".into(),
    ];
    for mutation in mutations {
        let mut data = candidate();
        mutation(&mut data);
        assert_eq!(
            validate_candidate(data, &mut pending())
                .unwrap_err()
                .error_code(),
            Some("bbox_invalid")
        );
    }
    let mut data = candidate();
    let mut second = data.groups[0].clone();
    second.parent_index = Some(0);
    data.groups[0].parent_index = Some(1);
    data.groups.push(second);
    assert!(validate_candidate(data, &mut pending()).is_err());
}

#[test]
fn rejects_truncated_image_and_decodes_real_size() {
    let mut bytes = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(4, 3)
        .write_to(&mut bytes, ImageFormat::Png)
        .unwrap();
    let (_, image) = decode_asset(bytes.get_ref().clone()).unwrap();
    assert_eq!((image.width, image.height), (4, 3));
    assert!(decode_asset(bytes.get_ref()[..26].to_vec()).is_err());
    assert!(decode_asset(b"not an image".to_vec()).is_err());
}

#[test]
fn exif_orientation_uses_the_same_pixels_and_dimensions_as_the_browser() {
    let mut encoded = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(4, 3)
        .write_to(&mut encoded, ImageFormat::Jpeg)
        .unwrap();
    // One little-endian TIFF IFD entry: orientation=6 (rotate 90 degrees).
    let exif = b"Exif\0\0II\x2a\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0\x06\0\0\0\0\0\0\0";
    let mut jpeg = vec![0xff, 0xd8, 0xff, 0xe1];
    jpeg.extend(((exif.len() + 2) as u16).to_be_bytes());
    jpeg.extend(exif);
    jpeg.extend(&encoded.get_ref()[2..]);
    let (pixels, size) = decode_asset(jpeg).unwrap();
    assert_eq!((size.width, size.height), (3, 4));
    assert_eq!(image::guess_format(&pixels).unwrap(), ImageFormat::Png);
    let decoded = image::load_from_memory(&pixels).unwrap();
    assert_eq!((decoded.width(), decoded.height()), (3, 4));
}

#[test]
fn request_defaults_are_cache_only_and_unknown_fields_are_rejected() {
    let request: SegmentRequest =
        serde_json::from_value(serde_json::json!({"asset_id":"img_test"})).unwrap();
    assert!(request.cached_only);
    assert!(request.expected_analyzer_key.is_none());
    assert_eq!(
        (
            request.language.as_str(),
            request.detail.as_str(),
            request.mask_format.as_str()
        ),
        ("zh-CN", "bbox", "none")
    );
    assert!(
        serde_json::from_value::<SegmentRequest>(
            serde_json::json!({"asset_id":"img_test", "provider":"grok"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<SegmentRequest>(serde_json::json!({
            "asset_id":"img_test", "expected_analyzer_key":null
        }))
        .is_err()
    );
}

#[test]
fn cache_identity_includes_model_effort_revision_and_options() {
    let config = AnalyzerConfig::default();
    for updated in [
        AnalyzerConfig {
            model: "other".into(),
            ..config.clone()
        },
        AnalyzerConfig {
            reasoning_effort: "low".into(),
            ..config.clone()
        },
        AnalyzerConfig {
            revision: "bbox-v2".into(),
            ..config.clone()
        },
        AnalyzerConfig {
            provider: "other".into(),
            ..config.clone()
        },
    ] {
        assert_ne!(analyzer_key(&config), analyzer_key(&updated));
    }
}

#[test]
fn registration_slots_fail_fast_and_release_without_buffering_a_third_upload() {
    let uploads = tokio::sync::Semaphore::new(2);
    let first = acquire_registration_slot(&uploads).unwrap();
    let _second = acquire_registration_slot(&uploads).unwrap();
    assert_eq!(
        acquire_registration_slot(&uploads)
            .err()
            .expect("third upload must fail before buffering")
            .error_code(),
        Some("service_unavailable")
    );
    drop(first);
    assert!(acquire_registration_slot(&uploads).is_ok());
}
