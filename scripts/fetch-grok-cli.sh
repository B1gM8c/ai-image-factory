#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
readonly REPO_ROOT
readonly LOCK_FILE="${REPO_ROOT}/providers/grok-cli.lock.json"

die() {
  printf 'fetch-grok-cli: %s\n' "$*" >&2
  exit 1
}

[[ $# -eq 2 ]] || die "usage: scripts/fetch-grok-cli.sh <target-triple> <output-path>"
readonly TARGET_TRIPLE="$1"
readonly OUTPUT_PATH="$2"
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
  env LOCK_FILE="$LOCK_FILE" TARGET_TRIPLE="$TARGET_TRIPLE" node <<'NODE'
const fs = require("node:fs");
const lock = JSON.parse(fs.readFileSync(process.env.LOCK_FILE, "utf8"));
const artifact = lock.artifacts?.[process.env.TARGET_TRIPLE];
if (
  lock.schema_version !== 1 ||
  lock.provider !== "xai-grok-cli" ||
  typeof lock.version !== "string" ||
  typeof lock.version_output !== "string" ||
  !artifact ||
  !String(artifact.url).startsWith("https://x.ai/cli/") ||
  !/^[0-9a-f]{64}$/.test(String(artifact.sha256)) ||
  !Number.isSafeInteger(artifact.bytes) ||
  !Number.isSafeInteger(artifact.elf_machine)
) {
  throw new Error("provider lock is invalid");
}
for (const value of [
  artifact.url,
  artifact.sha256,
  artifact.bytes,
  artifact.elf_machine,
  lock.version_output,
]) {
  process.stdout.write(`${value}\n`);
}
NODE
} 2>/dev/null)" || die "provider lock is invalid"
[[ "$(printf '%s\n' "$metadata" | wc -l | tr -d ' ')" -eq 5 ]] \
  || die "provider lock metadata is incomplete"
readonly URL
readonly EXPECTED_SHA256
readonly EXPECTED_BYTES
readonly EXPECTED_MACHINE
readonly EXPECTED_VERSION_OUTPUT
URL="$(printf '%s\n' "$metadata" | sed -n '1p')"
EXPECTED_SHA256="$(printf '%s\n' "$metadata" | sed -n '2p')"
EXPECTED_BYTES="$(printf '%s\n' "$metadata" | sed -n '3p')"
EXPECTED_MACHINE="$(printf '%s\n' "$metadata" | sed -n '4p')"
EXPECTED_VERSION_OUTPUT="$(printf '%s\n' "$metadata" | sed -n '5p')"

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
  [[ "$($temporary --version)" = "$EXPECTED_VERSION_OUTPUT" ]] \
    || die "provider binary version output does not match the lock"
fi
mv -f -- "$temporary" "$OUTPUT_PATH"
trap - EXIT
printf 'provider=%s target=%s sha256=%s\n' \
  "$EXPECTED_VERSION_OUTPUT" "$TARGET_TRIPLE" "$EXPECTED_SHA256"
