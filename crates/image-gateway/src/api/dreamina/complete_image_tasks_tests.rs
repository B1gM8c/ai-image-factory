use super::*;
use crate::settlement::AdditionalImageResult;
use serde_json::json;

fn task(status: &str, delivered: usize) -> DreaminaImageTask {
    let mut value = json!({
        "id":"original-task", "status":status,
        "cancellation":{"supported":false,"reason":"provider_cancellation_unsupported"},
        "request":{"generate_num":delivered.max(1)}
    });
    if delivered > 0 {
        value["data"] = json!(
            (0..delivered)
                .map(|_| json!({"b64_json":"b3JpZ2luYWw="}))
                .collect::<Vec<_>>()
        );
        value["output_counts"] = json!({"requested":delivered,"delivered":delivered,"failed":0});
    }
    serde_json::from_value(value).unwrap()
}

fn extra(index: usize) -> AdditionalImageResult {
    let bytes = vec![index as u8; index + 1];
    AdditionalImageResult {
        source_index: index,
        sha256: hex::encode(Sha256::digest(&bytes)),
        byte_size: bytes.len() as u64,
        bytes,
        mime_type: "image/png".to_owned(),
    }
}

#[test]
fn opt_in_complete_results_keep_one_billable_output_and_return_all_four() {
    let original = task("succeeded", 1);
    let legacy = serde_json::to_value(&original).unwrap();
    assert!(legacy.get("additional_images").is_none());
    assert!(legacy.get("returned_count").is_none());
    assert!(serde_json::from_value::<DreaminaImageTask>(legacy.clone()).is_ok());
    let result =
        serde_json::to_value(complete_image_task(original, (1..=3).map(extra).collect()).unwrap())
            .unwrap();
    assert_eq!(result["returned_count"], 4);
    assert_eq!(result["billable_units"], 1);
    assert_eq!(result["data"], legacy["data"]);
    assert_eq!(result["output_counts"], legacy["output_counts"]);
    assert_eq!(result["request"], legacy["request"]);
    assert_eq!(result["additional_images"].as_array().unwrap().len(), 3);
    for (offset, image) in result["additional_images"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        assert_eq!(image["source_index"], offset + 1);
        assert!(image.get("object_key").is_none());
        assert!(image.get("storage_namespace").is_none());
        assert_eq!(image.as_object().unwrap().len(), 5);
    }
}

#[test]
fn true_native_batch_retains_four_billable_outputs_without_free_extras() {
    let original = task("succeeded", 4);
    let result = complete_image_task(original, Vec::new()).unwrap();
    assert_eq!(result.returned_count, 4);
    assert_eq!(result.billable_units, 4);
    assert!(result.additional_images.is_empty());
    assert!(complete_image_task(task("succeeded", 4), vec![extra(1)]).is_err());
}

#[test]
fn incomplete_states_never_fabricate_result_counts_or_images() {
    for status in ["running", "failed", "uncertain", "expired"] {
        let result = complete_image_task(task(status, 0), Vec::new()).unwrap();
        assert_eq!(result.returned_count, 0);
        assert_eq!(result.billable_units, 0);
        assert!(result.additional_images.is_empty());
        assert!(complete_image_task(task(status, 0), vec![extra(1)]).is_err());
    }
}

#[test]
fn physical_results_reject_reordering_bad_hash_size_and_extra_slots() {
    for images in [
        vec![extra(2)],
        vec![extra(1), extra(1)],
        (1..=10).map(extra).collect(),
    ] {
        assert!(complete_image_task(task("succeeded", 1), images).is_err());
    }
    let mut image = extra(1);
    image.sha256 = "0".repeat(64);
    assert!(complete_image_task(task("succeeded", 1), vec![image]).is_err());
    let mut image = extra(1);
    image.byte_size += 1;
    assert!(complete_image_task(task("succeeded", 1), vec![image]).is_err());
    let mut image = extra(1);
    image.mime_type = "video/mp4".to_owned();
    assert!(complete_image_task(task("succeeded", 1), vec![image]).is_err());
}

#[test]
fn physical_result_budget_counts_base64_padding_and_exact_256_mib_boundary_without_copying() {
    use base64::Engine as _;
    for count in 1..50 {
        let encoded = base64::engine::general_purpose::STANDARD.encode(vec![42; count]);
        assert_eq!(complete_image_decoded_size(&encoded).unwrap(), count as u64);
    }
    assert!(complete_image_decoded_size("").is_err());
    assert!(complete_image_decoded_size("AAA").is_err());
    let max = crate::artifacts::MAX_ARTIFACT_BYTES;
    assert_eq!(complete_image_byte_total(max - 1, 1).unwrap(), max);
    assert_eq!(complete_image_byte_total(max, 0).unwrap(), max);
    assert!(complete_image_byte_total(max, 1).is_err());
    assert!(complete_image_byte_total(u64::MAX, 1).is_err());
}
