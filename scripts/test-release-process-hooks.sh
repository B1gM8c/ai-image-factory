#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

readonly REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly VERIFY="${REPO_ROOT}/deploy/hooks/verify"
readonly QUIESCE="${REPO_ROOT}/deploy/hooks/quiesce"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/aif-release-hooks.XXXXXXXX")"
trap 'rm -rf -- "$TEST_ROOT"' EXIT

mkdir -p "$TEST_ROOT/bin"
: >"$TEST_ROOT/phase"

cat >"$TEST_ROOT/bin/systemctl" <<'EOF'
#!/bin/bash
phase="$(cat "$MOCK_PHASE_FILE")"
case "$1" in
  list-unit-files | list-units)
    exit 0
    ;;
  is-enabled)
    exit 1
    ;;
  is-active)
    [[ "${MOCK_INACTIVE_UNIT:-}" != "${@: -1}" ]]
    ;;
  stop)
    printf '%s\n' "$*" >"$MOCK_STOP_LOG"
    ;;
  show)
    unit="$2"
    property="${3#--property=}"
    case "$property" in
      MainPID)
        if [[ "${MOCK_QUIESCE_STUCK_UNIT:-}" == "$unit" ]]; then
          echo 999
        elif [[ "${MOCK_QUIESCE_MODE:-false}" == true ]]; then
          echo 0
        elif [[ "${MOCK_UNSTABLE:-false}" == true && "$phase" == stable-window ]]; then
          echo 202
        else
          echo 101
        fi
        ;;
      NRestarts)
        if [[ "${MOCK_UNSTABLE:-false}" == true && "$phase" == stable-window ]]; then
          echo 1
        else
          echo 0
        fi
        ;;
      ActiveState)
        if [[ "${MOCK_QUIESCE_STUCK_UNIT:-}" == "$unit" ]]; then
          echo activating
        else
          echo inactive
        fi
        ;;
      *)
        printf 'unexpected systemctl show property: %s\n' "$property" >&2
        exit 2
        ;;
    esac
    ;;
  *)
    printf 'unexpected systemctl call: %s\n' "$*" >&2
    exit 2
    ;;
esac
EOF

cat >"$TEST_ROOT/bin/curl" <<'EOF'
#!/bin/bash
exit 0
EOF

cat >"$TEST_ROOT/bin/sleep" <<'EOF'
#!/bin/bash
printf 'stable-window' >"$MOCK_PHASE_FILE"
EOF

cat >"$TEST_ROOT/bin/runtime-gate" <<'EOF'
#!/bin/bash
printf '%s\n' "${AIF_VERIFY_RELEASE_UNITS:-}" >>"$MOCK_GATE_LOG"
exit 0
EOF

chmod 0755 "$TEST_ROOT/bin/"*

run_verify() {
  : >"$TEST_ROOT/phase"
  env \
    AIF_VERIFY_COMMAND_PATH="$TEST_ROOT/bin:/usr/bin:/bin" \
    AIF_VERIFY_GATEWAY_RUNTIME_GATE="$TEST_ROOT/bin/runtime-gate" \
    AIF_VERIFY_MAX_ATTEMPTS=1 \
    AIF_VERIFY_STABILITY_SECONDS=12 \
    MOCK_PHASE_FILE="$TEST_ROOT/phase" \
    MOCK_GATE_LOG="$TEST_ROOT/gate.log" \
    MOCK_STOP_LOG="$TEST_ROOT/stop.log" \
    "$@" \
    "$VERIFY"
}

run_quiesce() {
  env \
    AIF_QUIESCE_COMMAND_PATH="$TEST_ROOT/bin:/usr/bin:/bin" \
    MOCK_QUIESCE_MODE=true \
    MOCK_PHASE_FILE="$TEST_ROOT/phase" \
    MOCK_STOP_LOG="$TEST_ROOT/stop.log" \
    "$@" \
    "$QUIESCE"
}

: >"$TEST_ROOT/gate.log"
run_verify >/dev/null
[[ "$(wc -l <"$TEST_ROOT/gate.log" | tr -d ' ')" == 2 ]]
grep -Fq 'ai-image-factory-gateway.service' "$TEST_ROOT/gate.log"

if run_verify MOCK_UNSTABLE=true >/dev/null 2>&1; then
  echo "expected a PID/restart change during the stability window to fail" >&2
  exit 1
fi

run_quiesce >/dev/null
grep -Fq 'stop ai-image-factory-processes.target' "$TEST_ROOT/stop.log"

if run_quiesce MOCK_QUIESCE_STUCK_UNIT=ai-image-factory-workerd.service >/dev/null 2>&1; then
  echo "expected quiesce to fail while a process unit still has a MainPID" >&2
  exit 1
fi

echo "release process hook tests passed"
