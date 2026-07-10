use std::collections::BTreeSet;

use serde_json::Value;

const UPSTREAM_CONTRACT: &str =
    include_str!("fixtures/openai_images/2026-07-10/upstream-contract.json");
const RUNTIME_STABLE: &str = include_str!("fixtures/openai_images/2026-07-10/runtime-stable.json");
const ERRORS_STABLE: &str = include_str!("fixtures/openai_images/2026-07-10/errors-stable.json");
const KNOWN_GAPS: &str = include_str!("fixtures/openai_images/2026-07-10/known-gaps.json");

fn parse_fixture(name: &str, contents: &str) -> Value {
    serde_json::from_str(contents).unwrap_or_else(|error| panic!("invalid {name}: {error}"))
}

#[test]
fn fixture_metadata_and_schema_are_internally_consistent() {
    let upstream = parse_fixture("upstream-contract.json", UPSTREAM_CONTRACT);
    let runtime = parse_fixture("runtime-stable.json", RUNTIME_STABLE);
    let errors = parse_fixture("errors-stable.json", ERRORS_STABLE);
    let gaps = parse_fixture("known-gaps.json", KNOWN_GAPS);

    assert_eq!(upstream["captured_on"], "2026-07-10");
    assert_eq!(upstream["facts"]["regular_response_timestamp"], "created");
    assert_eq!(upstream["facts"]["stream_event_timestamp"], "created_at");
    assert_eq!(upstream["facts"]["completed_stream_event_has_usage"], true);
    assert_eq!(upstream["facts"]["size_edge_multiple"], 16);
    assert_eq!(upstream["facts"]["size_max_edge"], 3840);
    assert_eq!(upstream["facts"]["size_max_ratio"], "3:1");
    assert_eq!(upstream["facts"]["size_min_pixels"], 655360);
    assert_eq!(upstream["facts"]["size_max_pixels"], 8294400);
    for source in upstream["sources"].as_array().expect("sources array") {
        let url = source["url"].as_str().expect("source URL");
        assert!(url.starts_with("https://developers.openai.com/"));
    }

    assert_eq!(
        runtime["excluded_dynamic_fields"],
        serde_json::json!(["b64_json", "created_at", "usage"])
    );
    let events = runtime["events"].as_array().expect("runtime events");
    assert_eq!(events.len(), 2);
    for event in events {
        for excluded in ["b64_json", "created_at", "usage"] {
            assert!(
                event.get(excluded).is_none(),
                "{excluded} leaked into snapshot"
            );
        }
        assert!(event["type"].as_str().unwrap().ends_with(".completed"));
        assert_eq!(event["output_format"], "png");
    }

    let scenarios = errors["scenarios"].as_array().expect("error scenarios");
    assert_eq!(scenarios.len(), 3);
    for scenario in scenarios {
        assert_eq!(scenario["status"], 400);
        assert_eq!(scenario["error"]["type"], "invalid_request_error");
        assert!(scenario["error"]["message"].as_str().is_some());
        assert!(scenario["error"]["param"].as_str().is_some());
        assert!(scenario["error"]["code"].as_str().is_some());
    }

    let gap_ids = gaps["gaps"]
        .as_array()
        .expect("known gaps")
        .iter()
        .map(|gap| gap["id"].as_str().expect("gap id"))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        gap_ids,
        BTreeSet::from([
            "input_fidelity_rejected",
            "partial_images_gt_zero_unsupported",
            "sse_usage_missing",
        ])
    );
}
