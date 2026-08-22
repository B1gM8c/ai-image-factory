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
        header(headers, "x-image-units-limit-5h")? == "2147483647",
        "unexpected 5h limit metadata",
    )?;
    require(
        header(headers, "x-image-units-remaining-5h")? == "2147483646",
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
        codex_output_evidence(files, None, false, true, expected_invocations)?;
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
    assert_codex_invocation(&argv, expected_parent_pid.is_none())?;
    let messages = fs::read_to_string(&files.stdin_log)
        .map_err(|error| format!("failed to read fake Codex stdin log: {error}"))?
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line)
                .map_err(|error| format!("fake Codex stdin contained invalid JSON: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let thread_start = rpc_request(&messages, "thread/start")?;
    let turn_start = rpc_request(&messages, "turn/start")?;
    require(
        thread_start.pointer("/params/model").is_none(),
        "Codex thread/start unexpectedly overrode the production orchestrator model",
    )?;
    let request_dir = thread_start
        .pointer("/params/cwd")
        .and_then(Value::as_str)
        .ok_or_else(|| "Codex thread/start omitted its cwd".to_string())?
        .to_string();
    require(
        turn_start.pointer("/params/cwd").and_then(Value::as_str) == Some(request_dir.as_str()),
        "Codex turn/start cwd did not match thread/start",
    )?;
    let developer_instructions = thread_start
        .pointer("/params/developerInstructions")
        .and_then(Value::as_str)
        .ok_or_else(|| "Codex thread/start omitted developerInstructions".to_string())?;
    require(
        developer_instructions.contains("code-mode exec")
            && developer_instructions.contains("tools.image_gen__imagegen")
            && developer_instructions.contains("generatedImage(result)")
            && developer_instructions.contains("exactly once"),
        format!(
            "Codex developer instructions did not force the exact image tool: {developer_instructions}"
        ),
    )?;
    let input = turn_start
        .pointer("/params/input")
        .and_then(Value::as_array)
        .ok_or_else(|| "Codex turn/start omitted its input array".to_string())?;
    let prompt = input
        .iter()
        .find(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .and_then(|item| item.get("text"))
        .and_then(Value::as_str)
        .ok_or_else(|| "Codex turn/start omitted its text prompt".to_string())?
        .to_string();
    let image_paths = input
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("localImage"))
        .map(|item| {
            item.get("path")
                .and_then(Value::as_str)
                .filter(|path| !path.is_empty())
                .ok_or_else(|| "Codex localImage omitted its path".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    require(
        image_paths.len() == usize::from(expects_image),
        format!(
            "Codex turn/start had {} localImage inputs, expected {}",
            image_paths.len(),
            usize::from(expects_image)
        ),
    )?;
    for image_path in image_paths {
        require(
            !Path::new(image_path).exists(),
            format!("cleaned edit input file still exists: {image_path}"),
        )?;
    }
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
            "no delegated AI CLI",
            "不要再启动 codex、openai 或其它 AI CLI 子进程".to_string(),
        ),
        (
            "single native image tool invocation",
            "必须只调用一次当前启用的 image_gen.imagegen 图像生成工具".to_string(),
        ),
        (
            "Factory-owned native artifact sealing",
            "由 Factory 从该工具的受控原生产物路径完成封存".to_string(),
        ),
        (
            "no local artifact manipulation tools",
            "不要用 shell、sips、ImageMagick、Python、Rust、ffmpeg、canvas 或其他本地工具"
                .to_string(),
        ),
        (
            "no local artifact or pixel manipulation",
            "复制、移动、重命名、删除、裁切、拉伸、重采样、扩边、转绘或修改图像生成工具产物"
                .to_string(),
        ),
    ] {
        require(
            prompt.contains(&required),
            format!("Codex prompt is missing {description} semantics: {prompt}"),
        )?;
    }
    for forbidden in [request_dir, "final.png", "sealed-output.bin"] {
        require(
            !prompt.contains(forbidden),
            format!("Codex prompt exposed a deprecated output path {forbidden}: {prompt}"),
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

fn assert_codex_invocation(argv: &[String], expects_runtime_home: bool) -> TestResult {
    let expected = [
        "app-server",
        "--listen",
        "stdio://",
        "--strict-config",
        "--enable",
        "image_generation",
        "--disable",
        "plugins",
        "--disable",
        "apps",
        "--disable",
        "shell_tool",
        "--disable",
        "unified_exec",
    ]
    .map(str::to_string)
    .to_vec();
    require(
        argv == expected,
        format!("unexpected Codex argv:\nactual: {argv:?}\nexpected: {expected:?}"),
    )?;
    if expects_runtime_home {
        require(
            !argv.iter().any(|argument| argument == "--add-dir"),
            format!("managed Codex argv exposed the Factory runtime output directory: {argv:?}"),
        )?;
    }
    Ok(())
}

fn rpc_request<'a>(messages: &'a [Value], method: &str) -> TestResult<&'a Value> {
    let matches = messages
        .iter()
        .filter(|message| message.get("method").and_then(Value::as_str) == Some(method))
        .collect::<Vec<_>>();
    require(
        matches.len() == 1,
        format!("Codex stdin contained {} {method} requests", matches.len()),
    )?;
    Ok(matches[0])
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
