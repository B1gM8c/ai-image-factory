use super::types::*;
use crate::ImageGatewayError;

fn invalid() -> ImageGatewayError {
    ImageGatewayError::invalid_request(
        "The bbox result failed semantic validation",
        None,
        "bbox_invalid",
    )
}

fn validate_item(
    name: &str,
    bbox: [u32; 4],
    confidence: f64,
    size: &ImageSize,
) -> Result<(), ImageGatewayError> {
    let chinese = name.chars().any(|c| ('\u{3400}'..='\u{9fff}').contains(&c));
    if name.trim() != name
        || name.chars().count() > 80
        || !chinese
        || name.chars().any(char::is_control)
        || name
            .chars()
            .any(|c| matches!(c, '<' | '>' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
        || !confidence.is_finite()
        || !(0.0..=1.0).contains(&confidence)
        || bbox[0] >= bbox[2]
        || bbox[1] >= bbox[3]
        || bbox[2] > size.width
        || bbox[3] > size.height
    {
        return Err(invalid());
    }
    Ok(())
}

fn union(left: [u32; 4], right: [u32; 4]) -> [u32; 4] {
    [
        left[0].min(right[0]),
        left[1].min(right[1]),
        left[2].max(right[2]),
        left[3].max(right[3]),
    ]
}

/// Only containment is normalized, by expanding parents to their children.
/// Invalid bounds/order, dangling parents and cycles are rejected, never silently clamped.
pub fn validate_candidate(
    candidate: BboxCandidate,
    result: &mut Segmentation,
) -> Result<(), ImageGatewayError> {
    let groups = candidate.groups;
    if groups.is_empty() || groups.len() > 12 {
        return Err(invalid());
    }
    for (index, group) in groups.iter().enumerate() {
        validate_item(
            &group.name,
            group.bbox_xyxy,
            group.confidence,
            &result.image,
        )?;
        if !matches!(group.category.as_str(), "主体" | "部件" | "背景" | "装饰")
            || group.segments.len() > 4
        {
            return Err(invalid());
        }
        for segment in &group.segments {
            validate_item(
                &segment.name,
                segment.bbox_xyxy,
                segment.confidence,
                &result.image,
            )?;
        }
        let mut parent = group.parent_index;
        let mut seen = vec![false; groups.len()];
        seen[index] = true;
        while let Some(next) = parent {
            if next >= groups.len() || seen[next] {
                return Err(invalid());
            }
            seen[next] = true;
            parent = groups[next].parent_index;
        }
    }
    let mut boxes: Vec<_> = groups
        .iter()
        .map(|group| {
            group
                .segments
                .iter()
                .fold(group.bbox_xyxy, |bbox, item| union(bbox, item.bbox_xyxy))
        })
        .collect();
    for index in 0..groups.len() {
        let mut parent = groups[index].parent_index;
        while let Some(next) = parent {
            boxes[next] = union(boxes[next], boxes[index]);
            parent = groups[next].parent_index;
        }
    }
    let id = result.id.trim_start_matches("seg_");
    result.groups = groups
        .iter()
        .enumerate()
        .map(|(index, group)| SegmentGroup {
            group_id: format!("grp_{id}_{index}"),
            parent_group_id: group
                .parent_index
                .map(|parent| format!("grp_{id}_{parent}")),
            name: group.name.clone(),
            mask_key: format!("group_{index}"),
            category: group.category.clone(),
            ui_rank: index,
            bbox_xyxy: boxes[index],
            confidence: group.confidence,
            segments: group
                .segments
                .iter()
                .enumerate()
                .map(|(child, item)| SegmentItem {
                    segment_id: format!("segitem_{id}_{index}_{child}"),
                    name: item.name.clone(),
                    mask_key: format!("segment_{index}_{child}"),
                    bbox_xyxy: item.bbox_xyxy,
                    confidence: item.confidence,
                })
                .collect(),
        })
        .collect();
    result.status = SegmentStatus::Completed;
    result.error = None;
    Ok(())
}
