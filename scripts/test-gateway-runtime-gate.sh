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
  "$TEST_ROOT/releases/v1/bin"
ln -s "$TEST_ROOT/releases/v1" "$TEST_ROOT/current"
ln -s "$TEST_ROOT/releases/v1/bin/gpt-image-2-gateway" "$TEST_ROOT/proc/101/exe"
ln -s "$TEST_ROOT/releases/v1/bin/executord" "$TEST_ROOT/proc/102/exe"
: >"$TEST_ROOT/releases/v1/bin/gpt-image-2-gateway"
cat >"$TEST_ROOT/releases/v1/bin/grok" <<'EOF'
#!/bin/sh
printf 'grok 1.0.5 (5115b46bc9)\n'
EOF
: >"$TEST_ROOT/releases/v1/bin/executord"
chmod 0755 "$TEST_ROOT/releases/v1/bin/grok"
grok_sha256="$(sha256_file "$TEST_ROOT/releases/v1/bin/grok")"
cat >"$TEST_ROOT/releases/v1/provider-manifest.json" <<EOF
{"schema_version":1,"provider":"xai-grok-cli","version_output":"grok 1.0.5 (5115b46bc9)","binary_path":"bin/grok","binary_sha256":"${grok_sha256}","compatibility_revision":"grok-cli-1.0.5","image_adapter_revision":"grok-cli-1.0.5.agentic-media.v2","video_adapter_revision":"grok-api-1.0.5.direct-image-video.v4"}
EOF
printf 'GATEWAY_MANAGED_GROK_EXECUTABLE=%s\0' \
  "$TEST_ROOT/releases/v1/bin/grok" >"$TEST_ROOT/proc/102/environ"

cat >"$TEST_ROOT/bin/systemctl" <<'EOF'
#!/bin/bash
case "$*" in
  "show ai-image-factory-gateway.service --property=MainPID --value") echo "${MOCK_MAIN_PID:-101}" ;;
  "show ai-image-factory-executord@managed.service --property=MainPID --value") echo 102 ;;
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

if run_gate MOCK_OWNER_PID=202 >/dev/null 2>&1; then
  echo "expected mismatched port owner to fail" >&2
  exit 1
fi

mkdir -p "$TEST_ROOT/releases/v0/bin"
: >"$TEST_ROOT/releases/v0/bin/gpt-image-2-gateway"
ln -sfn "$TEST_ROOT/releases/v0/bin/gpt-image-2-gateway" "$TEST_ROOT/proc/101/exe"
if run_gate >/dev/null 2>&1; then
  echo "expected stale executable to fail" >&2
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
