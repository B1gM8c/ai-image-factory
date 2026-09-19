#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
readonly REPO_ROOT
die() {
  printf 'fetch-grok-cli: %s\n' "$*" >&2
  exit 1
}

case "$#" in
  2)
    readonly RUNTIME_GENERATION="v1"
    readonly TARGET_TRIPLE="$1"
    readonly OUTPUT_PATH="$2"
    ;;
  3)
    readonly RUNTIME_GENERATION="$1"
    readonly TARGET_TRIPLE="$2"
    readonly OUTPUT_PATH="$3"
    ;;
  *)
    die "usage: scripts/fetch-grok-cli.sh [<v1|v2>] <target-triple> <output-path>"
    ;;
esac
case "$RUNTIME_GENERATION" in
  v1) readonly LOCK_FILE="${REPO_ROOT}/providers/grok-cli-v1.lock.json" ;;
  v2) readonly LOCK_FILE="${REPO_ROOT}/providers/grok-cli.lock.json" ;;
  *) die "unsupported runtime generation: ${RUNTIME_GENERATION}" ;;
esac
case "$TARGET_TRIPLE" in
  x86_64-unknown-linux-gnu | aarch64-unknown-linux-gnu) ;;
  *) die "unsupported target: ${TARGET_TRIPLE}" ;;
esac
[[ "$OUTPUT_PATH" = /* || "$OUTPUT_PATH" != -* ]] || die "output path is invalid"
for command in curl env node; do
  command -v "$command" >/dev/null 2>&1 || die "required command is unavailable: $command"
done
[[ -f "$LOCK_FILE" ]] || die "provider lock is missing"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

file_size() {
  env FILE_SIZE_PATH="$1" node -e \
    'process.stdout.write(String(require("node:fs").statSync(process.env.FILE_SIZE_PATH).size))'
}

metadata="$({
  env LOCK_FILE="$LOCK_FILE" RUNTIME_GENERATION="$RUNTIME_GENERATION" TARGET_TRIPLE="$TARGET_TRIPLE" node <<'NODE'
const fs = require("node:fs");
const lock = JSON.parse(fs.readFileSync(process.env.LOCK_FILE, "utf8"));
const artifact = lock.artifacts?.[process.env.TARGET_TRIPLE];
const expected = {
  v1: {
    version: "1.0.5",
    version_output: "grok 1.0.5 (5115b46bc9)",
    compatibility_revision: "grok-cli-1.0.5",
    image_adapter_revision: "grok-cli-1.0.5.agentic-media.v2",
    video_adapter_revision: "grok-api-1.0.5.direct-image-video.v5",
  },
  v2: {
    version: "1.0.34",
    version_output: "grok 1.0.34 (3736acbc8658)",
    compatibility_revision: "grok-cli-1.0.34",
    image_adapter_revision: null,
    video_adapter_revision: "grok-cli-1.0.34.agentic-video.v1",
  },
}[process.env.RUNTIME_GENERATION];
const expectedTarget = {
  "x86_64-unknown-linux-gnu": { elf_machine: 62, architecture: "x86_64" },
  "aarch64-unknown-linux-gnu": { elf_machine: 183, architecture: "aarch64" },
}[process.env.TARGET_TRIPLE];
const expectedUrl = expected && expectedTarget
  ? `https://x.ai/cli/grok-${expected.version}-linux-${expectedTarget.architecture}`
  : null;
if (
  !expected ||
  !expectedTarget ||
  lock.schema_version !== 1 ||
  lock.provider !== "xai-grok-cli" ||
  lock.version !== expected.version ||
  lock.version_output !== expected.version_output ||
  lock.compatibility_revision !== expected.compatibility_revision ||
  lock.image_adapter_revision !== expected.image_adapter_revision ||
  lock.video_adapter_revision !== expected.video_adapter_revision ||
  typeof lock.version !== "string" ||
  typeof lock.version_output !== "string" ||
  !artifact ||
  artifact.url !== expectedUrl ||
  !/^[0-9a-f]{64}$/.test(String(artifact.sha256)) ||
  !Number.isSafeInteger(artifact.bytes) ||
  artifact.elf_machine !== expectedTarget.elf_machine
) {
  throw new Error("provider lock is invalid");
}
for (const value of [
  artifact.url,
  artifact.sha256,
  artifact.bytes,
  expectedTarget.elf_machine,
  expected.version_output,
]) {
  process.stdout.write(`${value}\n`);
}
NODE
} 2>/dev/null)" || die "provider lock is invalid"
[[ "$(printf '%s\n' "$metadata" | wc -l | tr -d ' ')" -eq 5 ]] \
  || die "provider lock metadata is incomplete"
URL="$(printf '%s\n' "$metadata" | sed -n '1p')"
EXPECTED_SHA256="$(printf '%s\n' "$metadata" | sed -n '2p')"
EXPECTED_BYTES="$(printf '%s\n' "$metadata" | sed -n '3p')"
EXPECTED_MACHINE="$(printf '%s\n' "$metadata" | sed -n '4p')"
EXPECTED_VERSION_OUTPUT="$(printf '%s\n' "$metadata" | sed -n '5p')"
readonly URL
readonly EXPECTED_SHA256
readonly EXPECTED_BYTES
readonly EXPECTED_MACHINE
readonly EXPECTED_VERSION_OUTPUT

output_parent="$(dirname -- "$OUTPUT_PATH")"
mkdir -p -- "$output_parent"
temporary="$(mktemp "${output_parent}/.grok.XXXXXXXX")"
cleanup() {
  rm -f -- "$temporary"
}
trap cleanup EXIT

curl \
  --fail \
  --location \
  --proto '=https' \
  --show-error \
  --silent \
  --tlsv1.2 \
  --output "$temporary" \
  "$URL"
[[ "$(file_size "$temporary")" = "$EXPECTED_BYTES" ]] \
  || die "downloaded provider binary size does not match the lock"
[[ "$(sha256_file "$temporary")" = "$EXPECTED_SHA256" ]] \
  || die "downloaded provider binary digest does not match the lock"
env BINARY="$temporary" EXPECTED_MACHINE="$EXPECTED_MACHINE" node <<'NODE'
const fs = require("node:fs");
const fd = fs.openSync(process.env.BINARY, "r");
const header = Buffer.alloc(20);
try {
  if (fs.readSync(fd, header, 0, header.length, 0) !== header.length) {
    throw new Error("provider ELF header is truncated");
  }
} finally {
  fs.closeSync(fd);
}
if (
  !header.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46])) ||
  header[4] !== 2 ||
  header[5] !== 1 ||
  header.readUInt16LE(18) !== Number(process.env.EXPECTED_MACHINE)
) {
  throw new Error("provider binary architecture does not match the lock");
}
NODE
chmod 0755 "$temporary"
if [[ "$(uname -s)" == "Linux" ]]; then
  case "${TARGET_TRIPLE}:$(uname -m)" in
    x86_64-unknown-linux-gnu:x86_64|aarch64-unknown-linux-gnu:aarch64)
      [[ "$($temporary --version)" = "$EXPECTED_VERSION_OUTPUT" ]] \
        || die "provider binary version output does not match the lock"
      ;;
  esac
fi
mv -f -- "$temporary" "$OUTPUT_PATH"
trap - EXIT
printf 'provider=%s target=%s sha256=%s\n' \
  "$EXPECTED_VERSION_OUTPUT" "$TARGET_TRIPLE" "$EXPECTED_SHA256"
