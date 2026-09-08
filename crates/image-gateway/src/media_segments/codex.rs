use std::{
    convert::Infallible,
    ffi::OsString,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use image::ImageFormat;
use image_cli_runtime::{
    CliPolicy, CliRuntime, CommandSpec, ExitClassification, OutputContract, RuntimeError,
    SpawnEvidence, SpawnObserver, VerifiedExecutable, WorkingDirectory,
    default_exit_classification,
};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use super::{
    Analysis, AnalyzerConfig, BboxAnalyzer, BboxCandidate, ImageSize, MAX_ASSET_BYTES,
    PROMPT_REVISION, SegmentTimings,
};
use crate::ImageGatewayError;

const WALL_TIMEOUT: Duration = Duration::from_secs(90);
const TERMINATION_GRACE: Duration = Duration::from_secs(1);
const MAX_OUTPUT_BYTES: u64 = 256 * 1024;
const SCHEMA_FILENAME: &str = "bbox.schema.json";
const OUTPUT_FILENAME: &str = "bbox.json";

const DISABLED_FEATURES: &[&str] = &[
    "apps",
    "browser_use",
    "computer_use",
    "hooks",
    "image_generation",
    "memories",
    "multi_agent",
    "multi_agent_v2",
    "plugins",
    "shell_tool",
    "skill_search",
    "standalone_web_search",
    "unified_exec",
    "workspace_dependencies",
];
const ENABLED_FEATURES: &[&str] = &["skip_host_skill_discovery"];

#[derive(Clone, Debug)]
pub struct CodexBboxAnalyzer {
    executable: VerifiedExecutable,
    auth_home: WorkingDirectory,
    config: AnalyzerConfig,
    wall_timeout: Duration,
}

impl CodexBboxAnalyzer {
    pub fn new(
        executable: PathBuf,
        auth_home: PathBuf,
        config: AnalyzerConfig,
    ) -> Result<Self, ImageGatewayError> {
        Self::new_with_timeout(executable, auth_home, config, WALL_TIMEOUT)
    }

    fn new_with_timeout(
        executable: PathBuf,
        auth_home: PathBuf,
        config: AnalyzerConfig,
        wall_timeout: Duration,
    ) -> Result<Self, ImageGatewayError> {
        validate_config(&config)?;
        if wall_timeout.is_zero() {
            return Err(ImageGatewayError::config(
                "BBox analyzer timeout must be non-zero",
            ));
        }
        let executable = VerifiedExecutable::new(executable)
            .map_err(|_| ImageGatewayError::config("BBox analyzer executable is invalid"))?;
        let auth_home = WorkingDirectory::new_private(auth_home)
            .map_err(|_| ImageGatewayError::config("BBox analyzer auth home is invalid"))?;
        Ok(Self {
            executable,
            auth_home,
            config,
            wall_timeout,
        })
    }

    async fn analyze_inner(
        &self,
        bytes: &[u8],
        image: &ImageSize,
    ) -> Result<Analysis, ImageGatewayError> {
        validate_input(bytes, image)?;
        let extension = image_extension(bytes)?;
        let workspace = tempfile::Builder::new()
            .prefix("aif-bbox-")
            .tempdir()
            .map_err(|_| ImageGatewayError::service_unavailable("BBox workspace unavailable"))?;
        tokio::fs::set_permissions(workspace.path(), std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("BBox workspace unavailable"))?;
        let isolated_auth_path = workspace.path().join("codex-home");
        tokio::fs::create_dir(&isolated_auth_path)
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("BBox credentials unavailable"))?;
        tokio::fs::set_permissions(&isolated_auth_path, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|_| ImageGatewayError::service_unavailable("BBox credentials unavailable"))?;
        let auth_sha256 = crate::executor::codex_auth_file_sha256(self.auth_home.path())
            .map_err(|_| ImageGatewayError::service_unavailable("BBox credentials unavailable"))?;
        crate::executor::prepare_codex_auth_copy(
            &isolated_auth_path,
            self.auth_home.path(),
            &auth_sha256,
        )
        .map_err(|_| ImageGatewayError::service_unavailable("BBox credentials unavailable"))?;
        let isolated_auth = WorkingDirectory::new_private(&isolated_auth_path)
            .map_err(|_| ImageGatewayError::service_unavailable("BBox credentials unavailable"))?;
        let image_path = workspace.path().join(format!("input.{extension}"));
        let schema_path = workspace.path().join(SCHEMA_FILENAME);
        write_private_file(&image_path, bytes, "image").await?;
        let schema = serde_json::to_vec(&bbox_schema(image))
            .map_err(|_| ImageGatewayError::internal("BBox schema serialization failed"))?;
        write_private_file(&schema_path, &schema, "schema").await?;

        let working_directory = WorkingDirectory::new_private(workspace.path())
            .map_err(|_| ImageGatewayError::service_unavailable("BBox workspace unavailable"))?;
        let output = OutputContract::new(OUTPUT_FILENAME, MAX_OUTPUT_BYTES)
            .map_err(|_| ImageGatewayError::internal("BBox output contract is invalid"))?;
        let command = self.command(CodexCommandInput {
            working_directory,
            image_path: &image_path,
            schema_path: &schema_path,
            temp_path: workspace.path(),
            auth_home: isolated_auth,
            output,
            image,
        })?;
        let policy = StaticCodexPolicy { command };
        let runtime = CliRuntime::new(policy);
        let started = Instant::now();
        let mut observer = TimingObserver::new(started);
        let result = runtime
            .run_to_sink(&(), &mut observer, Vec::with_capacity(8 * 1024))
            .await
            .map_err(map_runtime_error)?;
        let completed = Instant::now();
        let spawned = observer.spawned.unwrap_or(started);
        let candidate = serde_json::from_slice::<BboxCandidate>(&result.sink).map_err(|_| {
            ImageGatewayError::service_unavailable("BBox analyzer output is invalid")
        })?;
        Ok(Analysis {
            candidate,
            timings: SegmentTimings {
                cli_start_ms: millis_between(started, spawned),
                bbox_ms: millis_between(spawned, completed),
                retries: 0,
                ..SegmentTimings::default()
            },
        })
    }

    fn command(&self, input: CodexCommandInput<'_>) -> Result<CommandSpec, ImageGatewayError> {
        let CodexCommandInput {
            working_directory,
            image_path,
            schema_path,
            temp_path,
            auth_home,
            output,
            image,
        } = input;
        let mut command = CommandSpec::new(
            self.executable.clone(),
            working_directory,
            output,
            self.wall_timeout,
            TERMINATION_GRACE,
        )
        .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?
        .require_directory(auth_home.clone());

        for argument in [
            "exec",
            "--strict-config",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--color",
            "never",
            "--model",
            self.config.model.as_str(),
            "--config",
        ] {
            command = command
                .arg(argument)
                .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?;
        }
        command = command
            .arg(format!(
                "model_reasoning_effort=\"{}\"",
                self.config.reasoning_effort
            ))
            .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?;
        command = command
            .arg("--config")
            .and_then(|command| command.arg("project_doc_max_bytes=0"))
            .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?;
        for feature in DISABLED_FEATURES {
            command = command
                .arg("--disable")
                .and_then(|command| command.arg(*feature))
                .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?;
        }
        for feature in ENABLED_FEATURES {
            command = command
                .arg("--enable")
                .and_then(|command| command.arg(*feature))
                .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?;
        }
        for argument in ["--image", "--output-schema", "--output-last-message"] {
            command = command
                .arg(argument)
                .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?;
            let value: OsString = match argument {
                "--image" => image_path.as_os_str().to_owned(),
                "--output-schema" => schema_path.as_os_str().to_owned(),
                "--output-last-message" => temp_path.join(OUTPUT_FILENAME).into_os_string(),
                _ => unreachable!(),
            };
            command = command
                .arg(value)
                .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))?;
        }
        command
            .arg("-")
            .and_then(|command| command.stdin(prompt(image).into_bytes()))
            .and_then(|command| command.env("CODEX_HOME", auth_home.path().as_os_str()))
            .and_then(|command| command.env("HOME", auth_home.path().as_os_str()))
            .and_then(|command| command.env("TMPDIR", temp_path.as_os_str()))
            .map_err(|_| ImageGatewayError::config("BBox analyzer command is invalid"))
    }
}

struct CodexCommandInput<'a> {
    working_directory: WorkingDirectory,
    image_path: &'a Path,
    schema_path: &'a Path,
    temp_path: &'a Path,
    auth_home: WorkingDirectory,
    output: OutputContract,
    image: &'a ImageSize,
}

#[async_trait]
impl BboxAnalyzer for CodexBboxAnalyzer {
    async fn analyze(
        &self,
        bytes: &[u8],
        image: &ImageSize,
    ) -> Result<Analysis, ImageGatewayError> {
        self.analyze_inner(bytes, image).await
    }
}

#[derive(Clone)]
struct StaticCodexPolicy {
    command: CommandSpec,
}

impl CliPolicy for StaticCodexPolicy {
    type Request = ();
    type Error = Infallible;

    fn command(&self, _request: &Self::Request) -> Result<CommandSpec, Self::Error> {
        Ok(self.command.clone())
    }

    fn classify_exit(&self, status: &std::process::ExitStatus) -> ExitClassification {
        default_exit_classification(status)
    }
}

struct TimingObserver {
    started: Instant,
    spawned: Option<Instant>,
}

impl TimingObserver {
    fn new(started: Instant) -> Self {
        Self {
            started,
            spawned: None,
        }
    }
}

impl SpawnObserver for TimingObserver {
    type Error = Infallible;

    fn observe_spawn(&mut self, _evidence: &SpawnEvidence) -> Result<(), Self::Error> {
        self.spawned = Some(Instant::now().max(self.started));
        Ok(())
    }
}

fn validate_config(config: &AnalyzerConfig) -> Result<(), ImageGatewayError> {
    let safe_name = |value: &str| {
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
    };
    if config.provider != "openai_codex"
        || !safe_name(&config.model)
        || !matches!(
            config.reasoning_effort.as_str(),
            "none" | "low" | "medium" | "high"
        )
        || config.revision != PROMPT_REVISION
    {
        return Err(ImageGatewayError::config(
            "BBox analyzer configuration is invalid",
        ));
    }
    Ok(())
}

fn validate_input(bytes: &[u8], image: &ImageSize) -> Result<(), ImageGatewayError> {
    if bytes.is_empty()
        || bytes.len() > MAX_ASSET_BYTES
        || image.width == 0
        || image.height == 0
        || image.coordinate_system != "pixel_xyxy"
    {
        return Err(ImageGatewayError::internal(
            "BBox analyzer received invalid image input",
        ));
    }
    Ok(())
}

fn image_extension(bytes: &[u8]) -> Result<&'static str, ImageGatewayError> {
    match image::guess_format(bytes) {
        Ok(ImageFormat::Png) => Ok("png"),
        Ok(ImageFormat::Jpeg) => Ok("jpg"),
        Ok(ImageFormat::WebP) => Ok("webp"),
        _ => Err(ImageGatewayError::internal(
            "BBox analyzer received an unsupported image",
        )),
    }
}

async fn write_private_file(
    path: &Path,
    bytes: &[u8],
    kind: &'static str,
) -> Result<(), ImageGatewayError> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(path).await.map_err(|_| {
        ImageGatewayError::service_unavailable(format!("BBox workspace {kind} file unavailable"))
    })?;
    file.write_all(bytes).await.map_err(|_| {
        ImageGatewayError::service_unavailable(format!("BBox workspace {kind} file unavailable"))
    })?;
    file.flush().await.map_err(|_| {
        ImageGatewayError::service_unavailable(format!("BBox workspace {kind} file unavailable"))
    })
}

fn bbox_schema(image: &ImageSize) -> Value {
    let coordinate_max = image.width.max(image.height);
    let bbox = || {
        json!({
            "type": "array",
            "items": {"type": "integer", "minimum": 0, "maximum": coordinate_max},
            "minItems": 4,
            "maxItems": 4
        })
    };
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "groups": {
                "type": "array",
                "minItems": 1,
                "maxItems": 12,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "parent_index": {"type": ["integer", "null"], "minimum": 0, "maximum": 11},
                        "名称": {"type": "string", "minLength": 1, "maxLength": 32},
                        "类别": {"type": "string", "enum": ["主体", "部件", "背景", "装饰"]},
                        "bbox_xyxy": bbox(),
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "segments": {
                            "type": "array",
                            "maxItems": 4,
                            "items": {
                                "type": "object",
                                "additionalProperties": false,
                                "properties": {
                                    "名称": {"type": "string", "minLength": 1, "maxLength": 32},
                                    "bbox_xyxy": bbox(),
                                    "confidence": {"type": "number", "minimum": 0, "maximum": 1}
                                },
                                "required": ["名称", "bbox_xyxy", "confidence"]
                            }
                        }
                    },
                    "required": ["parent_index", "名称", "类别", "bbox_xyxy", "confidence", "segments"]
                }
            }
        },
        "required": ["groups"]
    })
}

fn prompt(image: &ImageSize) -> String {
    format!(
        "分析附加的最终图片，并只输出符合给定 JSON Schema 的中文语义层级 bbox。\n\
         原图已经由服务端完整解码，真实尺寸是 {width}×{height} 像素；坐标系为 pixel_xyxy，\
         bbox=[x1,y1,x2,y2]，必须满足 0<=x1<x2<={width}、0<=y1<y2<={height}。\n\
         选择少量用户可能点击的可见主体、背景或装饰作为 groups；每组最多四个重要可见部件。\
         parent_index 是父 group 在 groups 数组里的下标，根节点必须为 null；不得形成自引用或环。\
         每个子框必须位于对应父框内。只框可见像素，边界尽量紧，不得根据常见构图猜测不可见区域。\n\
         名称和类别必须为中文，类别只能是主体、部件、背景、装饰。不要输出 ID、mask、mask_key 或图像尺寸。\n\
         不要调用任何工具，不要运行命令，不要读取其他文件，不要创建子代理，也不要解释过程；只返回 JSON。",
        width = image.width,
        height = image.height
    )
}

fn millis_between(start: Instant, end: Instant) -> u64 {
    end.saturating_duration_since(start)
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn map_runtime_error(error: RuntimeError) -> ImageGatewayError {
    if matches!(
        error,
        RuntimeError::Process(image_cli_runtime::ProcessError::TimedOut { .. })
    ) {
        ImageGatewayError::timeout()
    } else {
        ImageGatewayError::service_unavailable("BBox analyzer failed")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt,
        path::Path,
        time::{Duration, Instant},
    };

    use super::*;

    fn png() -> Vec<u8> {
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(8, 6)
            .write_to(&mut bytes, ImageFormat::Png)
            .unwrap();
        bytes.into_inner()
    }

    fn image_size() -> ImageSize {
        ImageSize {
            width: 8,
            height: 6,
            coordinate_system: "pixel_xyxy".into(),
        }
    }

    fn fake_analyzer(
        script: &str,
        timeout: Duration,
    ) -> (tempfile::TempDir, PathBuf, CodexBboxAnalyzer) {
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("fake-codex");
        fs::write(&executable, format!("#!/bin/sh\n{script}\n")).unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o555)).unwrap();
        let auth = root.path().join("auth");
        fs::create_dir(&auth).unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o700)).unwrap();
        let auth_file = auth.join("auth.json");
        fs::write(
            &auth_file,
            br#"{"auth_mode":"chatgpt","OPENAI_API_KEY":null,"tokens":{"id_token":"test-id","access_token":"test-access","refresh_token":"test-refresh","account_id":"test-account"}}"#,
        )
        .unwrap();
        fs::set_permissions(&auth_file, fs::Permissions::from_mode(0o600)).unwrap();
        let analyzer = CodexBboxAnalyzer::new_with_timeout(
            executable.clone(),
            auth,
            AnalyzerConfig::default(),
            timeout,
        )
        .unwrap();
        (root, executable, analyzer)
    }

    const PARSE_ARGS: &str = r#"
printf '%s\n' "$@" > "$0.args"
while IFS= read -r line || [ -n "$line" ]; do printf '%s\n' "$line"; done > "$0.stdin"
out=''
schema=''
image=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output-last-message) shift; out="$1" ;;
    --output-schema) shift; schema="$1" ;;
    --image) shift; image="$1" ;;
  esac
  shift
done
printf '%s\n%s\n%s\n' "$CODEX_HOME" "$HOME" "$TMPDIR" > "$0.env"
[ -s "$image" ] || exit 42
while IFS= read -r line || [ -n "$line" ]; do printf '%s\n' "$line"; done < "$schema" > "$0.schema"
"#;

    const VALID_JSON: &str = r#"{"groups":[{"parent_index":null,"名称":"人物","类别":"主体","bbox_xyxy":[0,0,8,6],"confidence":0.9,"segments":[{"名称":"脸部","bbox_xyxy":[1,1,7,5],"confidence":0.8}]}]}"#;

    #[tokio::test]
    async fn invokes_isolated_codex_exec_and_parses_bounded_output() {
        let script = format!("{PARSE_ARGS}\nprintf '%s' '{VALID_JSON}' > \"$out\"");
        let (_root, executable, analyzer) = fake_analyzer(&script, Duration::from_secs(2));
        let analysis = analyzer.analyze(&png(), &image_size()).await.unwrap();
        assert_eq!(analysis.candidate.groups.len(), 1);
        assert_eq!(analysis.candidate.groups[0].name, "人物");
        assert_eq!(analysis.timings.retries, 0);

        let args = fs::read_to_string(executable.with_extension("args")).unwrap();
        for required in [
            "exec",
            "--strict-config",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "read-only",
            "gpt-5.6-luna",
            "model_reasoning_effort=\"none\"",
            "project_doc_max_bytes=0",
            "--image",
            "--output-schema",
            "--output-last-message",
        ] {
            assert!(
                args.lines().any(|line| line == required),
                "missing {required}"
            );
        }
        for feature in DISABLED_FEATURES {
            assert!(
                args.lines().any(|line| line == *feature),
                "missing {feature}"
            );
        }
        for feature in ENABLED_FEATURES {
            assert!(
                args.lines().any(|line| line == *feature),
                "missing {feature}"
            );
        }
        let stdin = fs::read_to_string(executable.with_extension("stdin")).unwrap();
        assert!(stdin.contains("8×6"));
        assert!(stdin.contains("不要调用任何工具"));
        let schema: Value =
            serde_json::from_slice(&fs::read(executable.with_extension("schema")).unwrap())
                .unwrap();
        assert_eq!(schema["properties"]["groups"]["maxItems"], 12);
        assert_eq!(
            schema["properties"]["groups"]["items"]["properties"]["segments"]["maxItems"],
            4
        );
        assert!(schema.get("image").is_none());

        let env = fs::read_to_string(executable.with_extension("env")).unwrap();
        let values: Vec<_> = env.lines().collect();
        assert_eq!(values[0], values[1]);
        assert_ne!(values[0], values[2]);
        assert!(!Path::new(values[2]).exists(), "temporary workspace leaked");
    }

    #[tokio::test]
    async fn rejects_missing_invalid_and_oversized_output() {
        for body in [
            "exit 0".to_string(),
            format!("{PARSE_ARGS}\nprintf 'not json' > \"$out\""),
            format!(
                "{PARSE_ARGS}\ni=0; : > \"$out\"; while [ $i -lt 4097 ]; do printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' >> \"$out\"; i=$((i+1)); done"
            ),
        ] {
            let (_root, _executable, analyzer) = fake_analyzer(&body, Duration::from_secs(4));
            let error = analyzer.analyze(&png(), &image_size()).await.unwrap_err();
            assert_eq!(error.error_code(), Some("service_unavailable"));
        }
    }

    #[tokio::test]
    async fn timeout_terminates_process_group_and_reaps_leader() {
        let script = format!("{PARSE_ARGS}\n/bin/sleep 30 & echo $! > \"$0.pid\"; wait");
        let (_root, executable, analyzer) = fake_analyzer(&script, Duration::from_secs(1));
        let started = Instant::now();
        let error = analyzer.analyze(&png(), &image_size()).await.unwrap_err();
        assert_eq!(error.error_code(), Some("timeout"));
        assert!(started.elapsed() < Duration::from_secs(3));

        let pid: i32 = fs::read_to_string(executable.with_extension("pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let result = unsafe { libc::kill(pid, 0) };
            if result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "descendant process survived timeout"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn cancelling_analysis_drops_and_reaps_the_process_group() {
        let script = format!(
            "{PARSE_ARGS}\necho $$ > \"$0.leader\"; /bin/sleep 30 & echo $! > \"$0.pid\"; wait"
        );
        let (_root, executable, analyzer) = fake_analyzer(&script, Duration::from_secs(30));
        let task = tokio::spawn(async move { analyzer.analyze(&png(), &image_size()).await });
        let pid_path = executable.with_extension("pid");
        let leader_path = executable.with_extension("leader");
        let deadline = Instant::now() + Duration::from_secs(3);
        while (!pid_path.exists() || !leader_path.exists()) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let pid = read_pid(&pid_path);
        let leader = read_pid(&leader_path);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        wait_pid_gone(pid).await;
        wait_pid_gone(leader).await;
    }

    fn read_pid(path: &Path) -> i32 {
        fs::read_to_string(path).unwrap().trim().parse().unwrap()
    }

    async fn wait_pid_gone(pid: i32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let result = unsafe { libc::kill(pid, 0) };
            if result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return;
            }
            assert!(Instant::now() < deadline, "process {pid} survived cleanup");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn constructor_rejects_untrusted_configuration_and_paths() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        let auth = root.path().join("auth");
        fs::create_dir(&auth).unwrap();
        fs::set_permissions(&auth, fs::Permissions::from_mode(0o700)).unwrap();
        let auth_file = auth.join("auth.json");
        fs::write(
            &auth_file,
            br#"{"auth_mode":"chatgpt","OPENAI_API_KEY":null,"tokens":{"id_token":"test-id","access_token":"test-access","refresh_token":"test-refresh","account_id":"test-account"}}"#,
        )
        .unwrap();
        fs::set_permissions(&auth_file, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(CodexBboxAnalyzer::new(missing, auth.clone(), AnalyzerConfig::default()).is_err());

        let executable = root.path().join("fake");
        fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o555)).unwrap();
        let config = AnalyzerConfig {
            model: "../../unsafe".into(),
            ..AnalyzerConfig::default()
        };
        assert!(CodexBboxAnalyzer::new(executable, auth, config).is_err());
    }
}
