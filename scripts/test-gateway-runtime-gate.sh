#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly REPO_ROOT
readonly GATE="${REPO_ROOT}/deploy/hooks/verify-gateway-runtime"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/aif-gateway-gate.XXXXXXXX")"
trap 'rm -rf -- "$TEST_ROOT"' EXIT

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

mkdir -p \
  "$TEST_ROOT/bin" \
  "$TEST_ROOT/proc/101" \
  "$TEST_ROOT/proc/102" \
  "$TEST_ROOT/proc/103" \
  "$TEST_ROOT/releases/v1/bin"
if [[ "$(uname -m)" == x86_64 ]]; then
  TEST_TARGET_TRIPLE=aarch64-unknown-linux-gnu
  TEST_ELF_MACHINE=183
else
  TEST_TARGET_TRIPLE=x86_64-unknown-linux-gnu
  TEST_ELF_MACHINE=62
fi
ln -s "$TEST_ROOT/releases/v1" "$TEST_ROOT/current"
ln -s "$TEST_ROOT/releases/v1/bin/gpt-image-2-gateway" "$TEST_ROOT/proc/101/exe"
ln -s "$TEST_ROOT/releases/v1/bin/executord" "$TEST_ROOT/proc/102/exe"
: >"$TEST_ROOT/releases/v1/bin/gpt-image-2-gateway"
cat >"$TEST_ROOT/releases/v1/release.json" <<EOF
{"schema_version":1,"release_version":"test","commit_sha":"0000000000000000000000000000000000000000000000000000000000000000","target_triple":"${TEST_TARGET_TRIPLE}"}
EOF
python3 - "$TEST_ROOT/releases/v1/bin/grok" "$TEST_ROOT/releases/v1/bin/grok-v1" "$TEST_ROOT/releases/v1/bin/grok-v2" "$TEST_ELF_MACHINE" <<'PY'
import pathlib, struct, sys
machine = int(sys.argv[-1])
for name in sys.argv[1:-1]:
    data = bytearray(64)
    data[:4] = b'\x7fELF'; data[4] = 2; data[5] = 1
    struct.pack_into('<H', data, 18, machine)
    pathlib.Path(name).write_bytes(data)
PY
: >"$TEST_ROOT/releases/v1/bin/executord"
: >"$TEST_ROOT/releases/v1/bin/grok-runner"
chmod 0755 "$TEST_ROOT/releases/v1/bin/grok" "$TEST_ROOT/releases/v1/bin/grok-v1" "$TEST_ROOT/releases/v1/bin/grok-v2" "$TEST_ROOT/releases/v1/bin/grok-runner"
: >"$TEST_ROOT/releases/v1/bin/codex-runner"
: >"$TEST_ROOT/releases/v1/bin/codex-cli"
chmod 0755 "$TEST_ROOT/releases/v1/bin/codex-runner" "$TEST_ROOT/releases/v1/bin/codex-cli"
grok_sha256="$(sha256_file "$TEST_ROOT/releases/v1/bin/grok")"
grok_v2_sha256="$(sha256_file "$TEST_ROOT/releases/v1/bin/grok-v2")"
cat >"$TEST_ROOT/releases/v1/provider-manifest.json" <<EOF
{"schema_version":1,"provider":"xai-grok-cli","version":"1.0.5","version_output":"grok 1.0.5 (5115b46bc9)","target_triple":"aarch64-unknown-linux-gnu","binary_path":"bin/grok","binary_sha256":"${grok_sha256}","binary_bytes":64,"compatibility_revision":"grok-cli-1.0.5","image_adapter_revision":"grok-cli-1.0.5.agentic-media.v2","video_adapter_revision":"grok-api-1.0.5.direct-image-video.v5","runtimes":{"v1":{"generation":"v1","version":"1.0.5","version_output":"grok 1.0.5 (5115b46bc9)","target_triple":"aarch64-unknown-linux-gnu","binary_path":"bin/grok-v1","binary_sha256":"${grok_sha256}","binary_bytes":64,"elf_machine":183,"compatibility_revision":"grok-cli-1.0.5","image_adapter_revision":"grok-cli-1.0.5.agentic-media.v2","video_adapter_revision":"grok-api-1.0.5.direct-image-video.v5"},"v2":{"generation":"v2","version":"1.0.34","version_output":"grok 1.0.34 (3736acbc8658)","target_triple":"aarch64-unknown-linux-gnu","binary_path":"bin/grok-v2","binary_sha256":"${grok_v2_sha256}","binary_bytes":64,"elf_machine":183,"compatibility_revision":"grok-cli-1.0.34","image_adapter_revision":null,"video_adapter_revision":"grok-cli-1.0.34.agentic-video.v1"}}}
EOF
python3 - "$TEST_ROOT/releases/v1/provider-manifest.json" <<PY
import json, sys
p = sys.argv[1]
m = json.load(open(p))
m['source_repository'] = 'https://github.com/xai-org/grok-build'
m['target_triple'] = '${TEST_TARGET_TRIPLE}'
for runtime in m['runtimes'].values():
    runtime['target_triple'] = '${TEST_TARGET_TRIPLE}'
    runtime['elf_machine'] = int('${TEST_ELF_MACHINE}')
json.dump(m, open(p, 'w'))
PY
printf 'EXECUTOR_HELPER_EXECUTABLE=%s\0EXECUTOR_GROK_EXECUTABLE=%s\0' \
  "$TEST_ROOT/releases/v1/bin/grok-runner" "$TEST_ROOT/releases/v1/bin/grok-v1" >"$TEST_ROOT/proc/102/environ"
ln -s "$TEST_ROOT/releases/v1/bin/codex-runner" "$TEST_ROOT/proc/103/exe"
printf 'EXECUTOR_HELPER_EXECUTABLE=%s\0EXECUTOR_CODEX_EXECUTABLE=%s\0' \
  "$TEST_ROOT/releases/v1/bin/codex-runner" "$TEST_ROOT/releases/v1/bin/codex-cli" >"$TEST_ROOT/proc/103/environ"

cat >"$TEST_ROOT/bin/systemctl" <<'EOF'
#!/bin/bash
case "$*" in
  "show ai-image-factory-gateway.service --property=MainPID --value") echo "${MOCK_MAIN_PID:-101}" ;;
  "show ai-image-factory-executord@managed.service --property=MainPID --value") echo "${MOCK_EXECUTOR_PID:-102}" ;;
  "show ai-image-factory-executord@codex.service --property=MainPID --value") echo "103" ;;
  "show gpt-image-2-gateway.service --property=LoadState --value") echo loaded ;;
  "is-enabled --quiet gpt-image-2-gateway.service") [[ "${MOCK_LEGACY_ENABLED:-false}" == true ]] ;;
  "is-active --quiet gpt-image-2-gateway.service") [[ "${MOCK_LEGACY_ACTIVE:-false}" == true ]] ;;
  *) printf 'unexpected systemctl call: %s\n' "$*" >&2; exit 2 ;;
esac
EOF
cat >"$TEST_ROOT/bin/ss" <<'EOF'
#!/bin/bash
printf 'LISTEN 0 128 127.0.0.1:8789 0.0.0.0:* users:(("gateway",pid=%s,fd=9))\n' "${MOCK_OWNER_PID:-101}"
EOF
cat >"$TEST_ROOT/bin/sha256sum" <<'EOF'
#!/bin/bash
shasum -a 256 "$@"
EOF
chmod 0755 "$TEST_ROOT/bin/systemctl" "$TEST_ROOT/bin/ss" "$TEST_ROOT/bin/sha256sum"

cat >"$TEST_ROOT/nginx.conf" <<'EOF'
location /v1/ {
  proxy_pass http://127.0.0.1:8789;
}
location / {
  proxy_pass http://127.0.0.1:3010;
}
EOF

run_gate() {
  env \
    AIF_VERIFY_COMMAND_PATH="$TEST_ROOT/bin:/usr/bin:/bin" \
    AIF_VERIFY_GATEWAY_BASE_URL=http://127.0.0.1:8789 \
    AIF_VERIFY_CURRENT_RELEASE_LINK="$TEST_ROOT/current" \
    AIF_VERIFY_PROC_ROOT="$TEST_ROOT/proc" \
    AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service,ai-image-factory-executord@managed.service \
    AIF_VERIFY_NGINX_CONFIG_PATH="$TEST_ROOT/nginx.conf" \
    AIF_VERIFY_FORBIDDEN_NGINX_PORTS=8787 \
    "$@" \
    "$GATE"
}

run_gate >/dev/null

# Historical schema-1 manifests without runtimes remain accepted.
cp "$TEST_ROOT/releases/v1/provider-manifest.json" "$TEST_ROOT/provider-manifest.dual.json"
python3 - "$TEST_ROOT/releases/v1/provider-manifest.json" <<'PY'
import json, sys
p = sys.argv[1]
m = json.load(open(p))
m.pop('runtimes', None)
json.dump(m, open(p, 'w'))
PY
run_gate AIF_UPDATE_PROCESS_SCOPE=validation AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service >/dev/null
cp "$TEST_ROOT/releases/v1/provider-manifest.json" "$TEST_ROOT/provider-manifest.legacy.json"
for legacy_field in version binary_bytes target_triple; do
  python3 - "$TEST_ROOT/releases/v1/provider-manifest.json" "$legacy_field" <<'PY'
import json, sys
p, field = sys.argv[1:]; m = json.load(open(p)); m.pop(field, None); json.dump(m, open(p, 'w'))
PY
  if run_gate AIF_UPDATE_PROCESS_SCOPE=validation AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service >/dev/null 2>&1; then
    echo "expected legacy missing ${legacy_field} to fail" >&2
    exit 1
  fi
  cp "$TEST_ROOT/provider-manifest.legacy.json" "$TEST_ROOT/releases/v1/provider-manifest.json"
done
python3 - "$TEST_ROOT/releases/v1/provider-manifest.json" <<'PY'
import json, sys
p = sys.argv[1]; m = json.load(open(p)); m['target_triple'] = 'aarch64-unknown-linux-gnu' if m['target_triple'].startswith('x86_64') else 'x86_64-unknown-linux-gnu'; json.dump(m, open(p, 'w'))
PY
if run_gate AIF_UPDATE_PROCESS_SCOPE=validation AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service >/dev/null 2>&1; then
  echo "expected legacy target mismatch to fail" >&2
  exit 1
fi
cp "$TEST_ROOT/provider-manifest.legacy.json" "$TEST_ROOT/releases/v1/provider-manifest.json"
rm "$TEST_ROOT/provider-manifest.legacy.json"
cp "$TEST_ROOT/provider-manifest.dual.json" "$TEST_ROOT/releases/v1/provider-manifest.json"

# The actual executord precedence is generic override first; management's
# GATEWAY_MANAGED_GROK_EXECUTABLE must not influence process binding.
printf 'EXECUTOR_HELPER_EXECUTABLE=%s\0EXECUTOR_PROVIDER_EXECUTABLE=%s\0EXECUTOR_GROK_EXECUTABLE=%s\0GATEWAY_MANAGED_GROK_EXECUTABLE=/outside\0' \
  "$TEST_ROOT/releases/v1/bin/grok-runner" "$TEST_ROOT/releases/v1/bin/grok-v2" "$TEST_ROOT/releases/v1/bin/grok-v1" >"$TEST_ROOT/proc/102/environ"
run_gate >/dev/null

printf 'EXECUTOR_HELPER_EXECUTABLE=  %s  \0EXECUTOR_PROVIDER_EXECUTABLE=   \0EXECUTOR_GROK_EXECUTABLE=  %s  \0' \
  "$TEST_ROOT/releases/v1/bin/grok-runner" "$TEST_ROOT/releases/v1/bin/grok-v1" >"$TEST_ROOT/proc/102/environ"
run_gate >/dev/null

# A Codex executor may coexist with a Grok executor and is not forced onto a
# Grok binary merely because the release has a managed Grok path.
run_gate AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service,ai-image-factory-executord@managed.service,ai-image-factory-executord@codex.service >/dev/null

expect_manifest_reject() {
  python3 - "$TEST_ROOT/releases/v1/provider-manifest.json" "$1" <<'PY'
import json, sys
p, mutation = sys.argv[1:]; m = json.load(open(p))
if mutation == 'incomplete': m['runtimes'].pop('v2')
elif mutation == 'crossed': m['runtimes']['v2']['binary_path'] = 'bin/grok-v1'
elif mutation == 'adapter': m['runtimes']['v2']['video_adapter_revision'] = 'wrong'
elif mutation == 'target': m['runtimes']['v2']['target_triple'] = 'aarch64-unknown-linux-gnu' if m['runtimes']['v2']['target_triple'].startswith('x86_64') else 'x86_64-unknown-linux-gnu'
elif mutation == 'size': m['runtimes']['v1']['binary_bytes'] += 1
elif mutation == 'hash': m['runtimes']['v2']['binary_sha256'] = '0' * 64
elif mutation == 'version': m['runtimes']['v2']['version_output'] = 'grok wrong'
elif mutation == 'top': m['version_output'] = 'grok wrong'
json.dump(m, open(p, 'w'))
PY
  if run_gate >/dev/null 2>&1; then echo "expected manifest mutation ${1} to fail" >&2; exit 1; fi
  cp "$TEST_ROOT/provider-manifest.dual.json" "$TEST_ROOT/releases/v1/provider-manifest.json"
}
for mutation in incomplete crossed adapter target size hash version top; do expect_manifest_reject "$mutation"; done
cp "$TEST_ROOT/releases/v1/provider-manifest.json" "$TEST_ROOT/provider-manifest.dual.json"
python3 - "$TEST_ROOT/releases/v1/bin/grok-v2" "$TEST_ROOT/releases/v1/provider-manifest.json" <<'PY'
import hashlib, json, pathlib, sys
binary, manifest = sys.argv[1:]
data = bytearray(pathlib.Path(binary).read_bytes()); data[4] = 1; pathlib.Path(binary).write_bytes(data)
m = json.load(open(manifest)); m['runtimes']['v2']['binary_sha256'] = hashlib.sha256(data).hexdigest(); json.dump(m, open(manifest, 'w'))
PY
if run_gate >/dev/null 2>&1; then echo "expected invalid ELF runtime to fail" >&2; exit 1; fi
cp "$TEST_ROOT/provider-manifest.dual.json" "$TEST_ROOT/releases/v1/provider-manifest.json"
python3 - "$TEST_ROOT/releases/v1/bin/grok-v2" <<'PY'
import pathlib, sys
p = pathlib.Path(sys.argv[1]); data = bytearray(p.read_bytes()); data[4] = 2; p.write_bytes(data)
PY
mv "$TEST_ROOT/releases/v1/bin/grok-v2" "$TEST_ROOT/releases/v1/bin/grok-v2.saved"
ln -s "$TEST_ROOT/releases/v1/bin/grok-v1" "$TEST_ROOT/releases/v1/bin/grok-v2"
if run_gate >/dev/null 2>&1; then echo "expected symlink runtime to fail" >&2; exit 1; fi
rm "$TEST_ROOT/releases/v1/bin/grok-v2"; mv "$TEST_ROOT/releases/v1/bin/grok-v2.saved" "$TEST_ROOT/releases/v1/bin/grok-v2"
ln "$TEST_ROOT/releases/v1/bin/grok-v2" "$TEST_ROOT/releases/v1/bin/grok-v2.hardlink"
if run_gate >/dev/null 2>&1; then echo "expected hardlink runtime to fail" >&2; exit 1; fi
rm "$TEST_ROOT/releases/v1/bin/grok-v2.hardlink"

: >"$TEST_ROOT/releases/v1/bin/unknown-runner"
chmod 0755 "$TEST_ROOT/releases/v1/bin/unknown-runner"
printf 'EXECUTOR_HELPER_EXECUTABLE=%s\0EXECUTOR_GROK_EXECUTABLE=%s\0' \
  "$TEST_ROOT/releases/v1/bin/unknown-runner" "$TEST_ROOT/releases/v1/bin/grok-v1" >"$TEST_ROOT/proc/102/environ"
if run_gate >/dev/null 2>&1; then
  echo "expected unknown helper classification to fail" >&2
  exit 1
fi
printf 'EXECUTOR_HELPER_EXECUTABLE=%s\0EXECUTOR_GROK_EXECUTABLE=%s\0' \
  "$TEST_ROOT/releases/v1/bin/grok-runner" "$TEST_ROOT/releases/v1/bin/grok-v1" >"$TEST_ROOT/proc/102/environ"

run_gate \
  AIF_UPDATE_PROCESS_SCOPE=validation \
  AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service \
  MOCK_EXECUTOR_PID=0 >/dev/null

for scope in full unknown; do
  if run_gate \
    AIF_UPDATE_PROCESS_SCOPE="$scope" \
    AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service \
    MOCK_EXECUTOR_PID=0 >/dev/null 2>&1; then
    echo "expected ${scope} scope without a proven executor binding to fail" >&2
    exit 1
  fi
done
if run_gate AIF_UPDATE_PROCESS_SCOPE=unknown >/dev/null 2>&1; then
  echo "expected unknown scope to fail even with healthy gateway and executor" >&2
  exit 1
fi
if run_gate AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service >/dev/null 2>&1; then
  echo "expected default full scope without a proven executor binding to fail" >&2
  exit 1
fi
if run_gate AIF_UPDATE_PROCESS_SCOPE=full MOCK_EXECUTOR_PID=0 >/dev/null 2>&1; then
  echo "expected full scope with a stopped executor to fail" >&2
  exit 1
fi

if run_gate MOCK_OWNER_PID=202 >/dev/null 2>&1; then
  echo "expected mismatched port owner to fail" >&2
  exit 1
fi
if run_gate \
  AIF_UPDATE_PROCESS_SCOPE=validation \
  AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service \
  MOCK_OWNER_PID=202 >/dev/null 2>&1; then
  echo "expected validation scope to retain the gateway port-owner gate" >&2
  exit 1
fi

mkdir -p "$TEST_ROOT/releases/v0/bin"
: >"$TEST_ROOT/releases/v0/bin/gpt-image-2-gateway"
ln -sfn "$TEST_ROOT/releases/v0/bin/gpt-image-2-gateway" "$TEST_ROOT/proc/101/exe"
if run_gate >/dev/null 2>&1; then
  echo "expected stale executable to fail" >&2
  exit 1
fi
if run_gate \
  AIF_UPDATE_PROCESS_SCOPE=validation \
  AIF_VERIFY_RELEASE_UNITS=ai-image-factory-gateway.service >/dev/null 2>&1; then
  echo "expected validation scope to retain the gateway release-identity gate" >&2
  exit 1
fi
ln -sfn "$TEST_ROOT/releases/v1/bin/gpt-image-2-gateway" "$TEST_ROOT/proc/101/exe"

: >"$TEST_ROOT/releases/v0/bin/executord"
ln -sfn "$TEST_ROOT/releases/v0/bin/executord" "$TEST_ROOT/proc/102/exe"
if run_gate >/dev/null 2>&1; then
  echo "expected stale worker executable to fail" >&2
  exit 1
fi
ln -sfn "$TEST_ROOT/releases/v1/bin/executord" "$TEST_ROOT/proc/102/exe"

if run_gate MOCK_LEGACY_ACTIVE=true >/dev/null 2>&1; then
  echo "expected active legacy unit to fail" >&2
  exit 1
fi

sed -i.bak 's/127\.0\.0\.1:8789/127.0.0.1:8787/' "$TEST_ROOT/nginx.conf"
if run_gate >/dev/null 2>&1; then
  echo "expected stale nginx upstream to fail" >&2
  exit 1
fi

echo "gateway runtime gate tests passed"
