#!/usr/bin/env python3
"""Explicitly authorized, two-invocation native probe; never run by CI.

Uses synthetic inputs, not customer orders. No refresh is copied back to the
source credential. Reports only content-free observations; no billing inference.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import tempfile
import time
import uuid
import zlib

PIN = "9ba87444e1819e8f6104adbbf4676a870c204380aa5c3e1c38a926c4ea677238"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def png(color):
    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 256, 256, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress((b"\0" + bytes(color) * 256) * 256)) + chunk(b"IEND", b""))


def records(data):
    result = []
    for line in data.decode("utf-8", "replace").splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                result.append(value)
        except ValueError:
            pass
    return result


def markers(data):
    text = data.decode("utf-8", "replace").lower()
    # Only literals are exported, never surrounding error text.
    known = ["unexpected argument", "unrecognized option", "unknown field",
             "token expired", "token_expired", "unauthorized", "invalid_grant",
             "failed to refresh", "authentication", "rate limit", "too many requests",
             "model not found", "model_not_found", "permission denied",
             "connection refused", "certificate", "timed out", "failed to connect",
             "requires at least one", "too many images", "maximum", "invalid api key"]
    return [item for item in known if item in text]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", required=True, type=Path)
    parser.add_argument("--auth-file", required=True, type=Path)
    parser.add_argument("--execute-two-approved-invocations", action="store_true")
    args = parser.parse_args()
    if not args.execute_two_approved_invocations:
        parser.error("explicit approval flag required; this consumes account allowance")
    assert args.executable.is_file() and digest(args.executable.read_bytes()) == PIN
    assert args.auth_file.is_file() and not args.auth_file.is_symlink()
    auth_before = digest(args.auth_file.read_bytes())
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix="aif-native-ref-probe-") as temporary:
        root = Path(temporary)
        binary = root / "grok"
        shutil.copyfile(args.executable, binary)
        binary.chmod(0o500)
        for count in (3, 4):
            case = root / str(count)
            runtime, home, workspace = (case / name for name in ("runtime", "grok-home", "workspace"))
            for directory in (runtime, home, workspace):
                directory.mkdir(parents=True, mode=0o700)
            shutil.copyfile(args.auth_file, home / "auth.json")
            (home / "auth.json").chmod(0o600)
            (home / "config.toml").write_text("[cli]\nauto_update = false\nuse_leader = false\n[features]\ntelemetry = false\n")
            images = []
            for index, color in enumerate(((255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0))[:count]):
                path = workspace / f"reference-{index}.png"
                path.write_bytes(png(color))
                images.append(str(path))
            session = str(uuid.uuid4())
            tool_args = {"prompt": "Combine these solid color references into one simple geometric color grid. No text.",
                         "image": images, "aspect_ratio": "1:1"}
            prompt = ("Call image_edit exactly once with these JSON arguments. Do not retry or call any other tool. "
                      "After the tool result, stop immediately.\n" + json.dumps(tool_args))
            environment = {"PATH": "/usr/bin:/bin", "HOME": str(runtime), "GROK_HOME": str(home),
                           "TMPDIR": str(workspace), "NO_COLOR": "1", "TERM": "dumb",
                           "XDG_CONFIG_HOME": str(runtime / "config"), "XDG_CACHE_HOME": str(runtime / "cache"),
                           "XDG_DATA_HOME": str(runtime / "data")}
            command = [str(binary), "--cwd", str(workspace), "--no-memory", "--no-plan", "--no-subagents",
                       "--disable-web-search", "--always-approve", "--tools", "image_edit", "--max-turns", "1",
                       "--no-wait-for-background", "--session-id", session,
                       "--output-format", "streaming-json", "--prompt-file", "/dev/stdin"]
            print(json.dumps({"event": "starting", "reference_count": count, "session_id": session}), flush=True)
            started = time.monotonic()
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       cwd=workspace, env=environment, start_new_session=True)
            timed_out = False
            try:
                stdout, stderr = process.communicate(prompt.encode(), timeout=180)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
            history = []
            for path in home.glob("sessions/**/chat_history.jsonl"):
                if path.stat().st_size <= 1024 * 1024:
                    history.extend(records(path.read_bytes()))
            calls = []
            for record in history:
                if record.get("type") != "assistant":
                    continue
                for call in record.get("tool_calls", []):
                    if call.get("name") == "image_edit":
                        arguments = call.get("arguments", {})
                        if isinstance(arguments, str):
                            try:
                                arguments = json.loads(arguments)
                            except ValueError:
                                arguments = {}
                        calls.append({"tool": "image_edit", "reference_count": len(arguments.get("image", []))})
            artifacts = [{"bytes": p.stat().st_size, "sha256": digest(p.read_bytes())}
                         for p in home.glob("sessions/**/images/*") if p.is_file() and p.stat().st_size <= 32 * 1024 * 1024]
            result = {"event": "completed", "reference_count": count, "session_id": session,
                      "binary_sha256": PIN, "version": "1.0.5", "requested_outputs": 1,
                      "elapsed_seconds": round(time.monotonic() - started, 3), "exit_code": process.returncode,
                      "timed_out": timed_out, "tool_calls_observed": calls, "history_records": len(history),
                      "artifacts": artifacts, "stdout_sha256": digest(stdout), "stderr_sha256": digest(stderr),
                      "stdout_bytes": len(stdout), "stderr_bytes": len(stderr),
                      "error_markers": markers(stdout + stderr),
                      "source_credential_unchanged": digest(args.auth_file.read_bytes()) == auth_before,
                      "binary_unchanged": digest(binary.read_bytes()) == PIN}
            print(json.dumps(result), flush=True)
            shutil.rmtree(case)
    print(json.dumps({"event": "cleanup_complete", "isolated_credentials_removed": True}), flush=True)


if __name__ == "__main__":
    main()
