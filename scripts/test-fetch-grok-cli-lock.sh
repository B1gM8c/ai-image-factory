#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
readonly REPO_ROOT
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/aif-fetch-lock.XXXXXXXX")"
trap 'rm -rf -- "$TEST_ROOT"' EXIT

prepare_case() {
  local name="$1"
  local case_root="${TEST_ROOT}/${name}"
  mkdir -p "${case_root}/bin" "${case_root}/providers" "${case_root}/scripts"
  cp "${REPO_ROOT}/scripts/fetch-grok-cli.sh" "${case_root}/scripts/fetch-grok-cli.sh"
  cp "${REPO_ROOT}/providers/grok-cli.lock.json" "${case_root}/providers/grok-cli.lock.json"
  chmod 0755 "${case_root}/scripts/fetch-grok-cli.sh"
  cat >"${case_root}/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
output=""
while (($#)); do
  if [[ "$1" == "--output" ]]; then
    output="$2"
    shift 2
  else
    shift
  fi
done
if [[ -n "${FETCH_CURL_PAYLOAD:-}" ]]; then
  cp "$FETCH_CURL_PAYLOAD" "$output"
  exit 0
fi
: >"${FETCH_CURL_MARKER}"
exit 99
EOF
  chmod 0755 "${case_root}/bin/curl"
  printf '%s\n' "$case_root"
}

mutate_lock() {
  local lock_path="$1"
  local mutation="$2"
  LOCK_PATH="$lock_path" MUTATION="$mutation" node <<'NODE'
const fs = require("node:fs");

const lockPath = process.env.LOCK_PATH;
const lock = JSON.parse(fs.readFileSync(lockPath, "utf8"));
switch (process.env.MUTATION) {
  case "version":
    lock.version_output = "grok 1.0.5 (5115b46bc9)";
    break;
  case "machine":
    lock.artifacts["x86_64-unknown-linux-gnu"].elf_machine = 183;
    break;
  case "url":
    lock.artifacts["x86_64-unknown-linux-gnu"].url =
      "https://x.ai/cli/grok-1.0.34-linux-aarch64";
    break;
  case "cross-arch": {
    const bytes = fs.readFileSync(process.env.SYNTHETIC_PATH);
    const crypto = require("node:crypto");
    const artifact = lock.artifacts["aarch64-unknown-linux-gnu"];
    artifact.sha256 = crypto.createHash("sha256").update(bytes).digest("hex");
    artifact.bytes = bytes.length;
    break;
  }
  default:
    throw new Error(`unknown mutation: ${process.env.MUTATION}`);
}
fs.writeFileSync(lockPath, `${JSON.stringify(lock, null, 2)}\n`);
NODE
}

assert_rejected_before_download() {
  local mutation="$1"
  local case_root
  case_root="$(prepare_case "$mutation")"
  mutate_lock "${case_root}/providers/grok-cli.lock.json" "$mutation"
  local marker="${case_root}/curl-called"
  if PATH="${case_root}/bin:${PATH}" \
    FETCH_CURL_MARKER="$marker" \
    "${case_root}/scripts/fetch-grok-cli.sh" \
    v2 x86_64-unknown-linux-gnu "${case_root}/output/grok" \
    >"${case_root}/stdout" 2>"${case_root}/stderr"; then
    echo "expected ${mutation} lock mutation to be rejected" >&2
    exit 1
  fi
  if [[ -e "$marker" ]]; then
    echo "fetch downloaded before rejecting ${mutation} lock mutation" >&2
    exit 1
  fi
  if ! grep -Fxq 'fetch-grok-cli: provider lock is invalid' "${case_root}/stderr"; then
    echo "expected ${mutation} lock mutation to report provider lock validation failure" >&2
    cat "${case_root}/stderr" >&2
    exit 1
  fi
}

assert_valid_lock_reaches_download_stub() {
  local case_root
  case_root="$(prepare_case valid)"
  local marker="${case_root}/curl-called"
  local status=0
  if PATH="${case_root}/bin:${PATH}" \
    FETCH_CURL_MARKER="$marker" \
    "${case_root}/scripts/fetch-grok-cli.sh" \
    v2 x86_64-unknown-linux-gnu "${case_root}/output/grok" \
    >"${case_root}/stdout" 2>"${case_root}/stderr"; then
    echo "expected the download stub to return status 99" >&2
    exit 1
  else
    status=$?
  fi
  [[ "$status" -eq 99 ]] || {
    echo "expected the download stub to return status 99, got ${status}" >&2
    cat "${case_root}/stderr" >&2
    exit 1
  }
  [[ -e "$marker" ]] || {
    echo "expected the unchanged V2 lock to reach the download stub" >&2
    exit 1
  }
}

assert_valid_lock_reaches_download_stub
assert_rejected_before_download version
assert_rejected_before_download machine
assert_rejected_before_download url

assert_cross_arch_defers_native_version() {
  local case_root
  case_root="$(prepare_case cross-arch)"
  local synthetic="${case_root}/synthetic-aarch64"
  SYNTHETIC_PATH="$synthetic" node <<'NODE'
const fs = require("node:fs");
const bytes = Buffer.alloc(20);
bytes.set([0x7f, 0x45, 0x4c, 0x46]);
bytes[4] = 2;
bytes[5] = 1;
bytes.writeUInt16LE(183, 18);
fs.writeFileSync(process.env.SYNTHETIC_PATH, bytes, { mode: 0o755 });
NODE
  SYNTHETIC_PATH="$synthetic" \
    mutate_lock "${case_root}/providers/grok-cli.lock.json" cross-arch
  cat >"${case_root}/bin/uname" <<'EOF'
#!/usr/bin/env bash
set -Eeuo pipefail
case "$1" in
  -s) printf 'Linux\n' ;;
  -m) printf 'x86_64\n' ;;
  *) exit 2 ;;
esac
EOF
  chmod 0755 "${case_root}/bin/uname"
  PATH="${case_root}/bin:${PATH}" \
    FETCH_CURL_PAYLOAD="$synthetic" \
    "${case_root}/scripts/fetch-grok-cli.sh" \
    v2 aarch64-unknown-linux-gnu "${case_root}/output/grok" \
    >"${case_root}/stdout" 2>"${case_root}/stderr"
  cmp -s "$synthetic" "${case_root}/output/grok"
}

assert_cross_arch_defers_native_version
echo "fetch Grok lock regression tests passed"
