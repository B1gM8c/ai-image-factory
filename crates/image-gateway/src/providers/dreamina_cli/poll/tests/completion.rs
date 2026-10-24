use super::*;
use serde_json::json;

fn image_receipt(count: usize, stringify: bool) -> String {
    let result = json!({
        "images": (1..=count).map(|index| json!({
            "item_id": format!("item-{index}"),
            "image_url": format!("https://media.example.test/{index}.png?token=private"),
            "width":2560,"height":1440,
        })).collect::<Vec<_>>(),
        "videos":[],
    });
    json!({
        "submit_id":"task-1","gen_status":"success",
        "result_json": if stringify { json!(result.to_string()) } else { result },
    })
    .to_string()
}

#[tokio::test]
async fn single_request_delivers_only_image_one_from_a_verified_native_result() {
    for count in [1, 4, 10] {
        for stringify in [false, true] {
            let receipt = image_receipt(count, stringify);
            let fixture = Fixture::new(&format!(
                r#"i=1
while [ "$i" -le {count} ]; do
  /bin/cp "$HOME/source-$i.png" "$download/${{submit}}_image_$i.png"
  i=$((i + 1))
done
printf '%s' '{receipt}'"#
            ));
            let sources = (1..=count)
                .map(|index| png_bytes([index as u8, 20, 30, 255]))
                .collect::<Vec<_>>();
            for (index, bytes) in sources.iter().enumerate() {
                fs::write(
                    fixture
                        .account_home
                        .join(format!("source-{}.png", index + 1)),
                    bytes,
                )
                .unwrap();
            }
            let mut sink = fixture.sink(Sha256::digest(&sources[0]).into());
            let observation = fixture
                .driver(1024)
                .poll_operation(&fixture.operation(), false, &mut sink)
                .await
                .unwrap();
            assert!(matches!(observation, PollObservation::Completed(_)));
            assert_eq!(sink.bytes(), sources[0]);
            assert_eq!(sink.finalize_count(), 1);
            fixture.assert_queries_used_original_submit_id(1);
            fixture.assert_workspace_has_no_attempts();
        }
    }
}

#[tokio::test]
async fn extra_images_require_complete_matching_metadata_and_canonical_names() {
    let canonical = (1..=4)
        .map(|index| format!("task-1_image_{index}.png"))
        .collect::<Vec<_>>();
    let cases = [
        (image_receipt(3, true), canonical.clone()),
        (image_receipt(5, true), canonical.clone()),
        (
            r#"{"submit_id":"task-1","gen_status":"success","result_json":"{broken"}"#.to_owned(),
            canonical.clone(),
        ),
        (
            r#"{"submit_id":"task-1","gen_status":"success"}"#.to_owned(),
            canonical,
        ),
        (
            image_receipt(4, true),
            (1..=4)
                .map(|index| format!("other_image_{index}.png"))
                .collect(),
        ),
        (
            image_receipt(4, true),
            [
                "task-1_image_1.png",
                "task-1_image_2.png",
                "task-1_image_3.png",
                "task-1_image_5.png",
            ]
            .map(str::to_owned)
            .to_vec(),
        ),
        (
            image_receipt(4, true),
            [
                "task-1_image_1.png",
                "task-1_image_1.jpg",
                "task-1_image_2.png",
                "task-1_image_3.png",
            ]
            .map(str::to_owned)
            .to_vec(),
        ),
        (
            image_receipt(4, true),
            [
                "task-1_image_01.png",
                "task-1_image_2.png",
                "task-1_image_3.png",
                "task-1_image_4.png",
            ]
            .map(str::to_owned)
            .to_vec(),
        ),
        (
            image_receipt(4, true),
            [
                "task-1_image_1.png",
                "task-1_image_2.png",
                "task-1_image_3.png",
                "sidecar.json",
            ]
            .map(str::to_owned)
            .to_vec(),
        ),
    ];
    for (receipt, names) in cases {
        let copies = names
            .iter()
            .map(|name| format!("/bin/cp \"$HOME/source.png\" \"$download/{name}\"\n"))
            .collect::<String>();
        let fixture = Fixture::new(&format!("{copies}printf '%s' '{receipt}'"));
        fs::write(
            fixture.account_home.join("source.png"),
            png_bytes([1, 2, 3, 255]),
        )
        .unwrap();
        let mut sink = fixture.sink([0; 32]);
        let failure = fixture
            .driver(1024)
            .poll_operation(&fixture.operation(), false, &mut sink)
            .await
            .unwrap_err();
        assert_eq!(failure.code(), "dreamina_poll_artifact_invalid");
        assert_eq!(failure.retry(), RetryDirective::Never);
        assert!(sink.bytes().is_empty());
        assert_eq!(sink.finalize_count(), 0);
        fixture.assert_queries_used_original_submit_id(1);
        fixture.assert_workspace_has_no_attempts();
    }
}

#[tokio::test]
async fn unselected_outputs_still_require_safe_regular_bounded_files() {
    for unsafe_output in [
        r#"/bin/ln -s "$HOME/source.png" "$download/task-1_image_4.png""#,
        r#"/bin/mkdir "$download/task-1_image_4.png""#,
        r#": > "$download/task-1_image_4.png""#,
        r#"/usr/bin/head -c 1025 /dev/zero > "$download/task-1_image_4.png""#,
        r#"/bin/cp "$HOME/source.png" "$download/task-1_image_4.png"
printf sidecar > "$download/result.json""#,
    ] {
        let receipt = image_receipt(4, true);
        let fixture = Fixture::new(&format!(
            r#"i=1
while [ "$i" -le 3 ]; do
  /bin/cp "$HOME/source.png" "$download/${{submit}}_image_$i.png"
  i=$((i + 1))
done
{unsafe_output}
printf '%s' '{receipt}'"#
        ));
        fs::write(
            fixture.account_home.join("source.png"),
            png_bytes([1, 2, 3, 255]),
        )
        .unwrap();
        let mut sink = fixture.sink([0; 32]);
        let failure = fixture
            .driver(1024)
            .poll_operation(&fixture.operation(), false, &mut sink)
            .await
            .unwrap_err();
        let expected_code = if unsafe_output.starts_with("/bin/ln -s") {
            "dreamina_poll_workspace_unavailable"
        } else {
            "dreamina_poll_artifact_invalid"
        };
        assert_eq!(failure.code(), expected_code);
        assert!(sink.bytes().is_empty());
        assert_eq!(sink.finalize_count(), 0);
    }
}

#[tokio::test]
async fn single_selection_detects_replacement_of_an_unselected_sibling() {
    let receipt = image_receipt(4, true);
    let fixture = Fixture::new(&format!(
        r#"i=1
while [ "$i" -le 4 ]; do
  /bin/cp "$HOME/source.png" "$download/${{submit}}_image_$i.png"
  i=$((i + 1))
done
printf '%s' "$download" > "$HOME/download-path"
printf '%s' '{receipt}'"#
    ));
    let bytes = png_bytes([1, 2, 3, 255]);
    fs::write(fixture.account_home.join("source.png"), &bytes).unwrap();
    let mut sink = ReplacingSiblingSink {
        sink: fixture.sink(Sha256::digest(&bytes).into()),
        directory_marker: fixture.account_home.join("download-path"),
    };
    let failure = fixture
        .driver(1024)
        .poll_operation(&fixture.operation(), false, &mut sink)
        .await
        .unwrap_err();
    assert_eq!(failure.code(), "dreamina_poll_artifact_invalid");
    assert_eq!(sink.sink.finalize_count(), 0);
    fixture.assert_queries_used_original_submit_id(1);
    fixture.assert_workspace_has_no_attempts();
}

struct ReplacingSiblingSink {
    sink: RecordingArtifactSink,
    directory_marker: PathBuf,
}

impl ArtifactSink for ReplacingSiblingSink {
    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), ArtifactSinkError> {
        let path = PathBuf::from(fs::read_to_string(&self.directory_marker).unwrap())
            .join("task-1_image_4.png");
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"replacement").unwrap();
        self.sink.write_chunk(bytes).await
    }

    async fn finalize(
        &mut self,
        metadata: ArtifactMetadata<'_>,
    ) -> Result<image_provider_sdk::DurableArtifactManifest, ArtifactSinkError> {
        self.sink.finalize(metadata).await
    }
}

#[tokio::test]
async fn string_video_result_preserves_the_original_url_without_a_download() {
    let url = "https://media.example.test/clip%2Fone.mp4?token=a%2Bb&expires=123";
    let result = json!({"images":[],"videos":[{"video_url":url}]});
    let receipt =
        json!({"submit_id":"task-1","gen_status":"success","result_json":result.to_string()});
    let fixture = Fixture::new(&format!(
        "test -z \"$download\" || exit 21\nprintf '%s' '{receipt}'"
    ));
    let mut sink = fixture.sink([0; 32]);
    let observation = fixture
        .driver(1024)
        .poll_operation(&fixture.operation(), true, &mut sink)
        .await
        .unwrap();
    let PollObservation::CompletedRemoteVideo(completed) = observation else {
        panic!("expected remote video")
    };
    assert_eq!(completed.result().video_url(), url);
    assert!(sink.bytes().is_empty());
    assert_eq!(sink.finalize_count(), 0);
    fixture.assert_queries_used_original_submit_id(1);
    fixture.assert_workspace_has_no_attempts();
}
