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

pub(crate) fn assert_codex_outputs(files: &SmokeFiles) -> TestResult {
    let argv = read_nul_strings(&files.argv_log)?;
    let request_dir = argv_value(&argv, "--cd")?;
    assert_codex_invocation(&argv, &request_dir)?;
    let prompt = fs::read_to_string(&files.stdin_log)
        .map_err(|error| format!("failed to read fake Codex stdin log: {error}"))?;
    assert_prompt_semantics(&prompt, &request_dir)?;
    let fake_pid = read_pid(&files.fake_pid_log)?;
    require(
        fake_pid > 0,
        "fake Codex PID log must contain a positive PID",
    )?;
    require(
        !Path::new(&request_dir).exists(),
        format!("cleaned request directory still exists: {request_dir}"),
    )
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

fn assert_codex_invocation(argv: &[String], request_dir: &str) -> TestResult {
    let expected = [
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
        "-",
    ]
    .map(str::to_string);
    require(
        argv == expected,
        format!("unexpected exact Codex argv:\nactual: {argv:?}\nexpected: {expected:?}"),
    )
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
