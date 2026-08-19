#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'
umask 022

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
readonly REPO_ROOT

usage() {
  cat <<'EOF'
Usage:
  scripts/package-release.sh <release-version> <target-triple> <commit-sha> <output-dir>

Example:
  SOURCE_DATE_EPOCH=1735689600 \
    scripts/package-release.sh v1.2.3 x86_64-unknown-linux-gnu "$GITHUB_SHA" dist

The Rust binaries and Next.js standalone output must already be built.
GROK_PROVIDER_BINARY must point to the fetched, lock-verified Grok CLI.
EOF
}

die() {
  printf 'package-release: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

sha256_file() {
  sha256sum "$1" | awk '{print $1}'
}

file_size() {
  env FILE_SIZE_PATH="$1" node -e \
    'process.stdout.write(String(require("node:fs").statSync(process.env.FILE_SIZE_PATH).size))'
}

assert_target_binary() {
  env \
    TARGET_BINARY_PATH="$1" \
    TARGET_TRIPLE="$TARGET_TRIPLE" \
    node <<'NODE'
const fs = require("node:fs");

const path = process.env.TARGET_BINARY_PATH;
const expectedMachine = {
  "x86_64-unknown-linux-gnu": 62,
  "aarch64-unknown-linux-gnu": 183,
}[process.env.TARGET_TRIPLE];
const descriptor = fs.openSync(path, "r");
const header = Buffer.alloc(20);
try {
  if (fs.readSync(descriptor, header, 0, header.length, 0) !== header.length) {
    throw new Error(`release binary has a truncated ELF header: ${path}`);
  }
} finally {
  fs.closeSync(descriptor);
}
const isElf64LittleEndian =
  header.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46])) &&
  header[4] === 2 &&
  header[5] === 1;
if (!isElf64LittleEndian || header.readUInt16LE(18) !== expectedMachine) {
  throw new Error(
    `release binary does not match ${process.env.TARGET_TRIPLE}: ${path}`,
  );
}
NODE
}

assert_runtime_tree_architecture() {
  env \
    RUNTIME_TREE_PATH="$1" \
    TARGET_TRIPLE="$TARGET_TRIPLE" \
    node <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const root = process.env.RUNTIME_TREE_PATH;
const expectedMachine = {
  "x86_64-unknown-linux-gnu": 62,
  "aarch64-unknown-linux-gnu": 183,
}[process.env.TARGET_TRIPLE];
const machoMagics = new Set([
  "cefaedfe",
  "feedface",
  "cffaedfe",
  "feedfacf",
  "cafebabe",
  "bebafeca",
  "cafebabf",
  "bfbafeca",
]);

function inspect(file) {
  const descriptor = fs.openSync(file, "r");
  const header = Buffer.alloc(20);
  let bytesRead;
  try {
    bytesRead = fs.readSync(descriptor, header, 0, header.length, 0);
  } finally {
    fs.closeSync(descriptor);
  }
  const relative = path.relative(root, file);
  const magic = header.subarray(0, Math.min(bytesRead, 4)).toString("hex");
  const isElf = bytesRead >= 4 && magic === "7f454c46";
  if (isElf) {
    if (
      bytesRead < 20 ||
      header[4] !== 2 ||
      header[5] !== 1 ||
      header.readUInt16LE(18) !== expectedMachine
    ) {
      throw new Error(
        `admin runtime ELF does not match ${process.env.TARGET_TRIPLE}: ${relative}`,
      );
    }
    return;
  }
  if (
    path.extname(file) === ".node" ||
    machoMagics.has(magic) ||
    (bytesRead >= 2 && header.subarray(0, 2).toString("ascii") === "MZ")
  ) {
    throw new Error(
      `admin runtime contains a foreign native module: ${relative}`,
    );
  }
}

function visit(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) {
      throw new Error(`admin runtime contains a symlink: ${absolute}`);
    }
    if (entry.isDirectory()) {
      visit(absolute);
    } else if (entry.isFile()) {
      inspect(absolute);
    } else {
      throw new Error(`admin runtime contains a special file: ${absolute}`);
    }
  }
}

visit(root);
NODE
}

normalize_tree() {
  local root="$1"
  find "$root" -type d -exec chmod 0755 {} +
  find "$root" -type f -exec chmod 0644 {} +
  find "$root/bin" -type f -exec chmod 0755 {} +
  find "$root/ops/hooks" -type f -exec chmod 0755 {} +
  chmod 0755 \
    "$root/ops/install-release" \
    "$root/ops/upgrade-updater"
  chmod 0644 "$root/admin/server.js"
  normalize_mtime_tree "$root"
}

normalize_mtime_tree() {
  local root="$1"
  env \
    NORMALIZE_ROOT="$root" \
    NORMALIZE_EPOCH="$SOURCE_DATE_EPOCH" \
    node <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const root = process.env.NORMALIZE_ROOT;
const epoch = Number(process.env.NORMALIZE_EPOCH);

function visit(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`release contains symlink: ${absolute}`);
    if (entry.isDirectory()) visit(absolute);
    fs.utimesSync(absolute, epoch, epoch);
  }
  fs.utimesSync(directory, epoch, epoch);
}

visit(root);
NODE
}

write_sorted_file_list() {
  local root="$1"
  local destination="$2"
  env \
    FILE_LIST_ROOT="$root" \
    FILE_LIST_DESTINATION="$destination" \
    node <<'NODE'
const fs = require("node:fs");
const path = require("node:path");

const root = process.env.FILE_LIST_ROOT;
const destination = process.env.FILE_LIST_DESTINATION;
const files = [];

function visit(directory, prefix = "") {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolute = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`release contains symlink: ${relative}`);
    if (entry.isDirectory()) {
      visit(absolute, relative);
    } else if (entry.isFile()) {
      files.push(relative);
    } else {
      throw new Error(`release contains special file: ${relative}`);
    }
  }
}

visit(root);
files.sort((left, right) => Buffer.from(left).compare(Buffer.from(right)));
fs.writeFileSync(destination, Buffer.concat(files.map((file) => Buffer.from(`${file}\0`))));
NODE
}

create_deterministic_tarball() {
  local root="$1"
  local file_list="$2"
  local destination="$3"
  if tar --version 2>/dev/null | grep -q 'GNU tar'; then
    (
      cd "$root"
      tar \
        --null \
        --files-from="$file_list" \
        --sort=name \
        --format=posix \
        --pax-option=delete=atime,delete=ctime \
        --owner=0 \
        --group=0 \
        --numeric-owner \
        --mtime="@${SOURCE_DATE_EPOCH}" \
        --mode='u+rwX,go+rX,go-w' \
        -cf -
    ) | gzip -n -9 >"$destination"
  else
    # PAX on macOS records volatile ctime and com.apple.provenance metadata.
    # USTAR is sufficient for this release tree and keeps local verification
    # byte-for-byte reproducible with the already sorted, normalized inputs.
    (
      cd "$root"
      tar \
        --null \
        --files-from="$file_list" \
        --format=ustar \
        --no-acls \
        --no-fflags \
        --no-mac-metadata \
        --no-xattrs \
        --uid 0 \
        --gid 0 \
        --uname root \
        --gname root \
        -cf -
    ) | gzip -n -9 >"$destination"
  fi
}

[[ $# -eq 4 ]] || {
  usage >&2
  exit 2
}

readonly RELEASE_VERSION="$1"
readonly TARGET_TRIPLE="$2"
readonly COMMIT_SHA="$3"
readonly OUTPUT_DIR_INPUT="$4"

[[ "$RELEASE_VERSION" =~ ^[A-Za-z0-9._-]+$ ]] \
  || die "release version contains unsupported characters"
[[ "$COMMIT_SHA" =~ ^[0-9a-fA-F]{40}$ ]] \
  || die "commit SHA must contain exactly 40 hexadecimal characters"
case "$TARGET_TRIPLE" in
  x86_64-unknown-linux-gnu | aarch64-unknown-linux-gnu) ;;
  *) die "unsupported release target: $TARGET_TRIPLE" ;;
esac

require_command find
require_command env
require_command gzip
require_command node
require_command sha256sum
require_command stat
require_command tar

if [[ -z "${SOURCE_DATE_EPOCH:-}" ]]; then
  SOURCE_DATE_EPOCH="$(git -C "$REPO_ROOT" show -s --format=%ct "$COMMIT_SHA")"
fi
[[ "$SOURCE_DATE_EPOCH" =~ ^[0-9]+$ ]] \
  || die "SOURCE_DATE_EPOCH must be a non-negative integer"
export SOURCE_DATE_EPOCH

readonly RUST_RELEASE_DIR="${CARGO_TARGET_DIR:-${REPO_ROOT}/target}/${TARGET_TRIPLE}/release"
readonly NEXT_ROOT="${REPO_ROOT}/apps/admin-console"
readonly NEXT_STANDALONE="${NEXT_ROOT}/.next/standalone"
readonly NEXT_STATIC="${NEXT_ROOT}/.next/static"
readonly NEXT_PUBLIC="${NEXT_ROOT}/public"
OUTPUT_DIR="$(mkdir -p "$OUTPUT_DIR_INPUT" && cd "$OUTPUT_DIR_INPUT" && pwd -P)"
readonly OUTPUT_DIR
readonly ASSET_PREFIX="ai-image-factory-${RELEASE_VERSION}-${TARGET_TRIPLE}"
readonly BUNDLE_PATH="${OUTPUT_DIR}/${ASSET_PREFIX}.tar.gz"
readonly MANIFEST_PATH="${OUTPUT_DIR}/${ASSET_PREFIX}.manifest.json"
readonly GROK_LOCK_FILE="${REPO_ROOT}/providers/grok-cli.lock.json"
readonly GROK_PROVIDER_BINARY="${GROK_PROVIDER_BINARY:-}"

readonly -a GATEWAY_BINARIES=(
  codex-runner
  executord
  factoryctl
  gpt-image-2-gateway
  grok-runner
  provider-pollerd
  provider-submitd
  reconcilerd
  reducerd
  remote-submit-runner
  webhookd
  workerd
)

[[ -d "$NEXT_STANDALONE" ]] || die "Next.js standalone output is missing"
[[ -f "${NEXT_STANDALONE}/apps/admin-console/server.js" ]] \
  || die "Next.js standalone server entry is missing"
[[ -d "$NEXT_STATIC" ]] || die "Next.js static output is missing"
[[ -n "$GROK_PROVIDER_BINARY" && -f "$GROK_PROVIDER_BINARY" && -x "$GROK_PROVIDER_BINARY" ]] \
  || die "GROK_PROVIDER_BINARY is missing or not executable"
[[ -f "$GROK_LOCK_FILE" ]] || die "Grok provider lock is missing"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/aif-release.XXXXXXXX")"
readonly WORK_DIR
cleanup() {
  rm -rf -- "$WORK_DIR"
}
trap cleanup EXIT

readonly RELEASE_ROOT="${WORK_DIR}/release"
readonly ADMIN_RUNTIME_ROOT="${WORK_DIR}/admin-runtime"
mkdir -p \
  "${RELEASE_ROOT}/bin" \
  "${RELEASE_ROOT}/admin" \
  "${RELEASE_ROOT}/ops/hooks" \
  "${RELEASE_ROOT}/ops/systemd" \
  "${RELEASE_ROOT}/ops/docs" \
  "$ADMIN_RUNTIME_ROOT"

for binary in "${GATEWAY_BINARIES[@]}"; do
  source_path="${RUST_RELEASE_DIR}/${binary}"
  [[ -f "$source_path" && -x "$source_path" ]] \
    || die "required Rust binary is missing or not executable: $source_path"
  assert_target_binary "$source_path"
  install -m 0755 "$source_path" "${RELEASE_ROOT}/bin/${binary}"
done

readonly UPDATER_SOURCE="${RUST_RELEASE_DIR}/updated"
[[ -f "$UPDATER_SOURCE" && -x "$UPDATER_SOURCE" ]] \
  || die "required updater binary is missing or not executable: $UPDATER_SOURCE"
assert_target_binary "$UPDATER_SOURCE"
install -m 0755 "$UPDATER_SOURCE" "${RELEASE_ROOT}/bin/updated"

assert_target_binary "$GROK_PROVIDER_BINARY"
env \
  GROK_LOCK_FILE="$GROK_LOCK_FILE" \
  GROK_PROVIDER_BINARY="$GROK_PROVIDER_BINARY" \
  GROK_PROVIDER_DESTINATION="${RELEASE_ROOT}/bin/grok" \
  GROK_PROVIDER_MANIFEST="${RELEASE_ROOT}/provider-manifest.json" \
  TARGET_TRIPLE="$TARGET_TRIPLE" \
  node <<'NODE'
const crypto = require("node:crypto");
const fs = require("node:fs");

const lock = JSON.parse(fs.readFileSync(process.env.GROK_LOCK_FILE, "utf8"));
const artifact = lock.artifacts?.[process.env.TARGET_TRIPLE];
const bytes = fs.readFileSync(process.env.GROK_PROVIDER_BINARY);
const sha256 = crypto.createHash("sha256").update(bytes).digest("hex");
if (
  lock.schema_version !== 1 ||
  lock.provider !== "xai-grok-cli" ||
  !artifact ||
  sha256 !== artifact.sha256 ||
  bytes.length !== artifact.bytes ||
  !/^grok 1\.0\.5 \([0-9a-f]+\)$/.test(lock.version_output) ||
  lock.compatibility_revision !== "grok-cli-1.0.5" ||
  lock.image_adapter_revision !== "grok-cli-1.0.5.agentic-media.v2" ||
  lock.video_adapter_revision !== "grok-api-1.0.5.direct-image-video.v3"
) {
  throw new Error("Grok provider binary does not match the immutable provider lock");
}
fs.copyFileSync(process.env.GROK_PROVIDER_BINARY, process.env.GROK_PROVIDER_DESTINATION);
fs.chmodSync(process.env.GROK_PROVIDER_DESTINATION, 0o755);
const manifest = {
  schema_version: 1,
  provider: lock.provider,
  source_repository: lock.source_repository,
  version: lock.version,
  version_output: lock.version_output,
  target_triple: process.env.TARGET_TRIPLE,
  binary_path: "bin/grok",
  binary_sha256: sha256,
  binary_bytes: bytes.length,
  compatibility_revision: lock.compatibility_revision,
  image_adapter_revision: lock.image_adapter_revision,
  video_adapter_revision: lock.video_adapter_revision,
};
fs.writeFileSync(
  process.env.GROK_PROVIDER_MANIFEST,
  `${JSON.stringify(manifest, null, 2)}\n`,
  { mode: 0o644 },
);
NODE
[[ "$("${RELEASE_ROOT}/bin/grok" --version)" = "$(
  node -p "require('${GROK_LOCK_FILE}').version_output"
)" ]] || die "packaged Grok provider version output does not match the lock"

cp -aL "${REPO_ROOT}/deploy/hooks/." "${RELEASE_ROOT}/ops/hooks/"
cp -aL "${REPO_ROOT}/deploy/systemd/." "${RELEASE_ROOT}/ops/systemd/"
install -m 0755 \
  "${REPO_ROOT}/deploy/install-release" \
  "${RELEASE_ROOT}/ops/install-release"
install -m 0755 \
  "${REPO_ROOT}/deploy/upgrade-updater" \
  "${RELEASE_ROOT}/ops/upgrade-updater"
install -m 0644 \
  "${REPO_ROOT}/docs/operations/github-release-deployment.md" \
  "${RELEASE_ROOT}/ops/docs/github-release-deployment.md"
install -m 0644 \
  "${REPO_ROOT}/docs/operations/production-release.md" \
  "${RELEASE_ROOT}/ops/docs/production-release.md"

MIGRATION_PREFIX="$(
  find "${REPO_ROOT}/crates/image-gateway/migrations" -maxdepth 1 -type f -name '*.sql' \
    -exec basename {} \; \
    | sed -nE 's/^([0-9]{4})_.*/\1/p' \
    | LC_ALL=C sort \
    | tail -n 1
)"
readonly MIGRATION_PREFIX
[[ -n "$MIGRATION_PREFIX" ]] || die "no numbered database migration was found"
readonly MIGRATION_VERSION="$((10#${MIGRATION_PREFIX}))"

env \
  RELEASE_IDENTITY_PATH="${RELEASE_ROOT}/release.json" \
  RELEASE_VERSION="$RELEASE_VERSION" \
  TARGET_TRIPLE="$TARGET_TRIPLE" \
  COMMIT_SHA="$COMMIT_SHA" \
  node <<'NODE'
const fs = require("node:fs");

const identity = {
  schema_version: 1,
  release_version: process.env.RELEASE_VERSION,
  commit_sha: process.env.COMMIT_SHA.toLowerCase(),
  target_triple: process.env.TARGET_TRIPLE,
};
fs.writeFileSync(
  process.env.RELEASE_IDENTITY_PATH,
  `${JSON.stringify(identity, null, 2)}\n`,
  { mode: 0o644 },
);
NODE

# The updater intentionally accepts a narrow release-path alphabet. Next.js route
# groups contain parentheses, so the standalone tree is kept as one verified
# inner archive and expanded by the release-local bootstrap on first start.
cp -aL "${NEXT_STANDALONE}/." "$ADMIN_RUNTIME_ROOT/"
mkdir -p "${ADMIN_RUNTIME_ROOT}/apps/admin-console/.next"
cp -aL "${NEXT_STATIC}/." "${ADMIN_RUNTIME_ROOT}/apps/admin-console/.next/static/"
if [[ -d "$NEXT_PUBLIC" ]]; then
  mkdir -p "${ADMIN_RUNTIME_ROOT}/apps/admin-console/public"
  cp -aL "${NEXT_PUBLIC}/." "${ADMIN_RUNTIME_ROOT}/apps/admin-console/public/"
fi
assert_runtime_tree_architecture "$ADMIN_RUNTIME_ROOT"
find "$ADMIN_RUNTIME_ROOT" -type d -exec chmod 0755 {} +
find "$ADMIN_RUNTIME_ROOT" -type f -exec chmod 0644 {} +
normalize_mtime_tree "$ADMIN_RUNTIME_ROOT"

readonly ADMIN_FILE_LIST="${WORK_DIR}/admin-files.list"
write_sorted_file_list "$ADMIN_RUNTIME_ROOT" "$ADMIN_FILE_LIST"
create_deterministic_tarball \
  "$ADMIN_RUNTIME_ROOT" \
  "$ADMIN_FILE_LIST" \
  "${RELEASE_ROOT}/admin/standalone.tar.gz"
ADMIN_ARCHIVE_SHA256="$(sha256_file "${RELEASE_ROOT}/admin/standalone.tar.gz")"
readonly ADMIN_ARCHIVE_SHA256

cat >"${RELEASE_ROOT}/admin/server.js" <<EOF
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const releaseVersion = "${RELEASE_VERSION}";
const expectedArchiveSha256 = "${ADMIN_ARCHIVE_SHA256}";
const releaseAdminRoot = __dirname;
const archivePath = path.join(releaseAdminRoot, "standalone.tar.gz");
const runtimeRoot =
  process.env.AIF_ADMIN_RUNTIME_ROOT || "/var/lib/ai-image-factory/admin-runtime";
const runtimeDirectory = path.join(runtimeRoot, releaseVersion);
const readyMarker = path.join(runtimeDirectory, ".ready");

function sha256(filePath) {
  const digest = crypto.createHash("sha256");
  digest.update(fs.readFileSync(filePath));
  return digest.digest("hex");
}

function prepareRuntime() {
  if (fs.existsSync(readyMarker)) return;
  if (sha256(archivePath) !== expectedArchiveSha256) {
    throw new Error("admin standalone archive digest mismatch");
  }
  fs.mkdirSync(runtimeRoot, { recursive: true, mode: 0o700 });
  const temporary = fs.mkdtempSync(path.join(runtimeRoot, ".extract-"));
  try {
    const extraction = spawnSync(
      "/usr/bin/tar",
      ["-xzf", archivePath, "-C", temporary, "--no-same-owner", "--no-same-permissions"],
      { stdio: "inherit" },
    );
    if (extraction.status !== 0) {
      throw new Error("admin standalone archive extraction failed");
    }
    fs.writeFileSync(path.join(temporary, ".ready"), expectedArchiveSha256 + "\n", {
      mode: 0o600,
    });
    try {
      fs.renameSync(temporary, runtimeDirectory);
    } catch (error) {
      if (!fs.existsSync(readyMarker)) throw error;
      fs.rmSync(temporary, { recursive: true, force: true });
    }
  } catch (error) {
    fs.rmSync(temporary, { recursive: true, force: true });
    throw error;
  }
}

prepareRuntime();
const applicationRoot = path.join(runtimeDirectory, "apps/admin-console");
process.chdir(applicationRoot);
require(path.join(applicationRoot, "server.js"));
EOF

normalize_tree "$RELEASE_ROOT"

readonly RELEASE_FILE_LIST="${WORK_DIR}/release-files.list"
write_sorted_file_list "$RELEASE_ROOT" "$RELEASE_FILE_LIST"
while IFS= read -r -d '' relative_path; do
  [[ "$relative_path" =~ ^[A-Za-z0-9@._/-]+$ ]] \
    || die "outer release path is incompatible with updater policy: $relative_path"
done <"$RELEASE_FILE_LIST"

create_deterministic_tarball "$RELEASE_ROOT" "$RELEASE_FILE_LIST" "$BUNDLE_PATH"

BUNDLE_SHA256="$(sha256_file "$BUNDLE_PATH")"
readonly BUNDLE_SHA256
BUNDLE_BYTES="$(file_size "$BUNDLE_PATH")"
readonly BUNDLE_BYTES
readonly MIN_SCHEMA_VERSION="${MIN_SCHEMA_VERSION:-$((MIGRATION_VERSION - 1))}"
[[ "$MIN_SCHEMA_VERSION" =~ ^[0-9]+$ ]] \
  || die "MIN_SCHEMA_VERSION must be a non-negative integer"
((MIN_SCHEMA_VERSION <= MIGRATION_VERSION)) \
  || die "MIN_SCHEMA_VERSION cannot exceed the packaged migration version"

env \
  RELEASE_ROOT="$RELEASE_ROOT" \
  RELEASE_VERSION="$RELEASE_VERSION" \
  TARGET_TRIPLE="$TARGET_TRIPLE" \
  COMMIT_SHA="$COMMIT_SHA" \
  MIGRATION_VERSION="$MIGRATION_VERSION" \
  MIN_SCHEMA_VERSION="$MIN_SCHEMA_VERSION" \
  BUNDLE_SHA256="$BUNDLE_SHA256" \
  BUNDLE_BYTES="$BUNDLE_BYTES" \
  MANIFEST_PATH="$MANIFEST_PATH" \
  node <<'NODE'
const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const releaseRoot = process.env.RELEASE_ROOT;
const allowedPath = /^[A-Za-z0-9@._/-]+$/;

function collect(directory, prefix = "") {
  const entries = fs
    .readdirSync(directory, { withFileTypes: true })
    .sort((left, right) => left.name.localeCompare(right.name, "en"));
  const files = [];
  for (const entry of entries) {
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolute = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`release contains symlink: ${relative}`);
    if (entry.isDirectory()) {
      files.push(...collect(absolute, relative));
      continue;
    }
    if (!entry.isFile()) throw new Error(`release contains special file: ${relative}`);
    if (!allowedPath.test(relative)) throw new Error(`unsupported release path: ${relative}`);
    const bytes = fs.readFileSync(absolute);
    files.push({
      path: relative,
      sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
      bytes: bytes.length,
      mode: fs.statSync(absolute).mode & 0o7777,
    });
  }
  return files;
}

const manifest = {
  schema_version: 1,
  updater_protocol_version: 1,
  release_version: process.env.RELEASE_VERSION,
  commit_sha: process.env.COMMIT_SHA.toLowerCase(),
  target_triple: process.env.TARGET_TRIPLE,
  migration_version: Number(process.env.MIGRATION_VERSION),
  min_schema_version: Number(process.env.MIN_SCHEMA_VERSION),
  target_schema_version: Number(process.env.MIGRATION_VERSION),
  rollback_mode: "backup_restore",
  bundle_sha256: process.env.BUNDLE_SHA256,
  bundle_bytes: Number(process.env.BUNDLE_BYTES),
  files: collect(releaseRoot),
};
fs.writeFileSync(process.env.MANIFEST_PATH, `${JSON.stringify(manifest, null, 2)}\n`, {
  mode: 0o644,
});
NODE

[[ -s "$BUNDLE_PATH" ]] || die "release bundle was not created"
[[ -s "$MANIFEST_PATH" ]] || die "release manifest was not created"

printf 'bundle=%s\nmanifest=%s\nsha256=%s\nbytes=%s\n' \
  "$BUNDLE_PATH" \
  "$MANIFEST_PATH" \
  "$BUNDLE_SHA256" \
  "$BUNDLE_BYTES"
