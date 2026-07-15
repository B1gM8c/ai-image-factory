use std::{fs, io::Cursor, path::Path};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use image::{ImageBuffer, ImageFormat, Rgb};
use serde_json::Value;

use super::{SmokeFiles, TestResult, read_pid, require};

pub(crate) fn assert_response(
    body: &Value,
    headers: &reqwest::header::HeaderMap,
    fixture: &[u8],
) -> TestResult {
    require(
        body["created"].as_i64().is_some_and(|value| value > 0),
        "missing created metadata",
    )?;
    require(
        body["output_format"] == "png",
        "output_format metadata was not png",
    )?;
    require(body["quality"] == "low", "quality metadata was not low")?;
    require(body["size"] == "2x1", "size metadata was not 2x1")?;
    require(
        body["background"] == "opaque",
        "background metadata was not opaque",
    )?;
    require(
        header(headers, "openai-project")? == "proj_default",
        "unexpected project metadata",
    )?;
    require(
        header(headers, "x-image-units-limit-5h")? == "40",
        "unexpected 5h limit metadata",
    )?;
    require(
        header(headers, "x-image-units-remaining-5h")? == "39",
        "unexpected 5h remaining metadata",
    )?;

    let encoded = body["data"]
        .as_array()
        .filter(|data| data.len() == 1)
        .and_then(|data| data[0]["b64_json"].as_str())
        .ok_or_else(|| "response must contain exactly one b64_json image".to_string())?;
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|error| format!("response image was not valid base64: {error}"))?;
    require(
        decoded == fixture,
        "decoded response did not exactly match the opaque PNG fixture",
    )
}

pub(crate) fn assert_codex_outputs(files: &SmokeFiles, expected_parent_pid: u32) -> TestResult {
    let (prompt, request_dir) =
        codex_output_evidence(files, Some(expected_parent_pid), false, true, 1)?;
    assert_prompt_semantics(&prompt, &request_dir)
}

pub(crate) fn assert_executor_codex_outputs(
    files: &SmokeFiles,
    expected_invocations: usize,
) -> TestResult {
    let (prompt, request_dir) =
        codex_output_evidence(files, None, false, false, expected_invocations)?;
    assert_prompt_semantics(&prompt, &request_dir)
}

pub(crate) fn assert_codex_edit_outputs(
    files: &SmokeFiles,
    expected_parent_pid: u32,
) -> TestResult {
    let (prompt, request_dir) =
        codex_output_evidence(files, Some(expected_parent_pid), true, true, 1)?;
    require(
        prompt.contains("这是图生图编辑任务")
            && prompt.contains("用户编辑需求：process smoke opaque fixture"),
        format!("Codex edit prompt lost edit semantics: {prompt}"),
    )?;
    require(
        !Path::new(&request_dir).exists(),
        format!("cleaned edit request directory still exists: {request_dir}"),
    )
}

fn codex_output_evidence(
    files: &SmokeFiles,
    expected_parent_pid: Option<u32>,
    expects_image: bool,
    expects_cleanup: bool,
    expected_invocations: usize,
) -> TestResult<(String, String)> {
    let invocation_count = fs::read_to_string(&files.invocation_log)
        .map_err(|error| format!("failed to read fake Codex invocation log: {error}"))?
        .lines()
        .count();
    require(
        invocation_count == expected_invocations,
        format!(
            "fake Codex invocation count was {invocation_count}, expected {expected_invocations}"
        ),
    )?;
    let argv = read_nul_strings(&files.argv_log)?;
    let request_dir = argv_value(&argv, "--cd")?;
    assert_codex_invocation(&argv, &request_dir, expects_image)?;
    let prompt = fs::read_to_string(&files.stdin_log)
        .map_err(|error| format!("failed to read fake Codex stdin log: {error}"))?;
    let fake_pid = read_pid(&files.fake_pid_log)?;
    require(
        fake_pid > 0,
        "fake Codex PID log must contain a positive PID",
    )?;
    let fake_parent_pid = read_pid(&files.fake_parent_pid_log)?;
    if let Some(expected_parent_pid) = expected_parent_pid {
        require(
            fake_parent_pid == expected_parent_pid as i32,
            format!(
                "fake Codex parent PID was {fake_parent_pid}, expected workerd PID {expected_parent_pid}"
            ),
        )?;
    } else {
        require(
            fake_parent_pid > 0 && fake_parent_pid != fake_pid,
            "fake Codex must be launched by a distinct codex-runner process",
        )?;
    }
    require(
        Path::new(&request_dir).exists() != expects_cleanup,
        if expects_cleanup {
            format!("cleaned request directory still exists: {request_dir}")
        } else {
            format!("durable executor workspace is missing: {request_dir}")
        },
    )?;
    Ok((prompt, request_dir))
}

pub(crate) fn assert_artifact_bytes(files: &SmokeFiles, expected: &[u8]) -> TestResult {
    let artifact_files = artifact_files(files)?;
    require(
        artifact_files.len() == 1,
        format!("expected one durable artifact file, found {artifact_files:?}"),
    )?;
    let bytes = fs::read(&artifact_files[0])
        .map_err(|error| format!("failed to read durable artifact: {error}"))?;
    require(
        bytes == expected,
        "durable artifact bytes did not match fixture",
    )
}

pub(crate) fn tamper_artifact(files: &SmokeFiles) -> TestResult {
    let artifact_files = artifact_files(files)?;
    require(
        artifact_files.len() == 1,
        format!("expected one artifact to tamper, found {artifact_files:?}"),
    )?;
    fs::write(&artifact_files[0], b"tampered artifact")
        .map_err(|error| format!("failed to tamper artifact: {error}"))
}

fn artifact_files(files: &SmokeFiles) -> TestResult<Vec<std::path::PathBuf>> {
    let objects = files.artifact_root.join("objects");
    let mut artifact_files = Vec::new();
    for shard in fs::read_dir(&objects)
        .map_err(|error| format!("failed to read artifact objects: {error}"))?
    {
        let shard = shard.map_err(|error| format!("failed to read artifact shard: {error}"))?;
        for entry in fs::read_dir(shard.path())
            .map_err(|error| format!("failed to read artifact shard entries: {error}"))?
        {
            let entry = entry.map_err(|error| format!("failed to read artifact entry: {error}"))?;
            if entry
                .file_type()
                .map_err(|error| format!("failed to stat artifact entry: {error}"))?
                .is_file()
            {
                artifact_files.push(entry.path());
            }
        }
    }
    Ok(artifact_files)
}

pub(crate) fn opaque_png() -> TestResult<Vec<u8>> {
    let image = ImageBuffer::from_fn(2, 1, |x, _| {
        if x == 0 {
            Rgb([12_u8, 34, 56])
        } else {
            Rgb([210_u8, 180, 90])
        }
    });
    let mut cursor = Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, ImageFormat::Png)
        .map_err(|error| format!("failed to encode opaque PNG fixture: {error}"))?;
    Ok(cursor.into_inner())
}

pub(crate) fn alternate_opaque_png() -> TestResult<Vec<u8>> {
    let image = ImageBuffer::from_fn(2, 1, |x, _| {
        if x == 0 {
            Rgb([90_u8, 15, 200])
        } else {
            Rgb([5_u8, 240, 130])
        }
    });
    let mut cursor = Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, ImageFormat::Png)
        .map_err(|error| format!("failed to encode alternate opaque PNG fixture: {error}"))?;
    Ok(cursor.into_inner())
}

pub(crate) fn assert_prompt_semantics(prompt: &str, request_dir: &str) -> TestResult {
    for (description, required) in [
        (
            "original prompt",
            "用户原始需求：process smoke opaque fixture".to_string(),
        ),
        ("auto size", "尺寸 auto".to_string()),
        ("low quality", "质量 low".to_string()),
        ("PNG output format", "输出格式 png".to_string()),
        (
            "request-local final image",
            format!("{request_dir}/final.png"),
        ),
        (
            "no delegated AI CLI",
            "不要再启动 codex、openai 或其它 AI CLI 子进程".to_string(),
        ),
        (
            "no local image manipulation tools",
            "不要用 sips、ImageMagick、Python、Rust、ffmpeg、canvas 或其他本地图像处理工具"
                .to_string(),
        ),
        (
            "no local pixel manipulation",
            "裁切、拉伸、重采样、扩边、转绘或修改像素".to_string(),
        ),
    ] {
        require(
            prompt.contains(&required),
            format!("Codex prompt is missing {description} semantics: {prompt}"),
        )?;
    }
    Ok(())
}

pub(crate) fn header(headers: &reqwest::header::HeaderMap, name: &str) -> TestResult<String> {
    headers
        .get(name)
        .ok_or_else(|| format!("response header {name} is missing"))?
        .to_str()
        .map(str::to_string)
        .map_err(|error| format!("response header {name} was invalid: {error}"))
}

fn assert_codex_invocation(argv: &[String], request_dir: &str, expects_image: bool) -> TestResult {
    let expected_prefix = [
        "exec",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--disable",
        "plugins",
        "--disable",
        "apps",
        "--sandbox",
        "workspace-write",
        "--skip-git-repo-check",
        "--cd",
        request_dir,
    ]
    .map(str::to_string);
    require(
        argv.starts_with(&expected_prefix),
        format!(
            "unexpected Codex argv prefix:\nactual: {argv:?}\nexpected prefix: {expected_prefix:?}"
        ),
    )?;
    if expects_image {
        require(
            argv.len() == expected_prefix.len() + 3
                && argv[expected_prefix.len()] == "--image"
                && !argv[expected_prefix.len() + 1].is_empty()
                && argv[expected_prefix.len() + 2] == "-",
            format!("unexpected edit Codex argv: {argv:?}"),
        )?;
        require(
            !Path::new(&argv[expected_prefix.len() + 1]).exists(),
            "cleaned edit input file still exists",
        )
    } else {
        require(
            argv.len() == expected_prefix.len() + 1 && argv[expected_prefix.len()] == "-",
            format!("unexpected generation Codex argv: {argv:?}"),
        )
    }
}

fn argv_value(argv: &[String], flag: &str) -> TestResult<String> {
    argv.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| format!("fake Codex argv did not contain {flag}: {argv:?}"))
}

fn read_nul_strings(path: &Path) -> TestResult<Vec<String>> {
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read fake Codex argv log: {error}"))?;
    bytes
        .split(|byte| *byte == 0)
        .filter(|value| !value.is_empty())
        .map(|value| {
            String::from_utf8(value.to_vec())
                .map_err(|error| format!("fake Codex argv was not UTF-8: {error}"))
        })
        .collect()
}
