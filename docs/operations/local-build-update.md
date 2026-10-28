# Explicit local build updates

Use this source only when the operator has authorized deployment of a locally
built release. It is recorded as `local_build`, not a signed GitHub candidate or
an immutable Release. Owner authentication, Check/Apply authorization, migration
checks, updater leases, backup/restore, admission and runtime verification remain
the same as the normal update path.

## Prepare the complete release

Build from a clean, committed checkout in an architecture-matched Linux build
environment. Record the exact commit, toolchain and actual test results. Build
the complete process set and package it with `scripts/package-release.sh`; a
gateway/factoryctl-only replacement is not sufficient across schema changes.

```sh
cargo build --offline --locked --release --target x86_64-unknown-linux-gnu --package gpt-image-2-gateway --bins
cargo build --offline --locked --release --target x86_64-unknown-linux-gnu --package ai-image-factory-updater --bin updated
```

The existing package script also requires architecture-matched Admin standalone
output and lock-verified provider binaries. Do not reuse unrelated dirty build
outputs. Keep the source, build/test logs and package hashes as local evidence.
No GitHub workflow, signed tag or attestation is implied by those records.

Copy the resulting manifest, bundle and build record into a dedicated directory
under `/var/lib/ai-image-factory/updater/local-builds/VERSION/`. Every ancestor
must be root-owned and not group/other-writable. Inputs must be regular files,
without symlinks or hardlinks. Use mode 0600 for the pin/build record and 0644 or
0600 for package inputs. Keep them available until the update and any recovery
are complete. This location must also be readable by the updater systemd sandbox.

Write a build record with the following identity fields plus the real build
commands, toolchain and test evidence. `clean_worktree:true` is an operator
statement about the checkout used to build; the updater cannot independently
prove a compiler executed that source.

```json
{
  "schema_version": 1,
  "source": "local_build",
  "commit_sha": "<40 hex>",
  "clean_worktree": true,
  "target_triple": "x86_64-unknown-linux-gnu",
  "manifest_sha256": "<64 hex>",
  "manifest_bytes": 123,
  "bundle_sha256": "<64 hex>",
  "bundle_bytes": 456,
  "checks": {"description": "actual commands and results"}
}
```

Then write the root-protected pin. Sizes and digests must come from the actual
files; the values below are placeholders, not a runnable pin.

```json
{
  "schema_version": 1,
  "source": "local_build",
  "version": "<unique forward-moving release version>",
  "commit_sha": "<40 hex>",
  "target_triple": "x86_64-unknown-linux-gnu",
  "manifest_path": "/var/lib/ai-image-factory/updater/local-builds/VERSION/manifest.json",
  "manifest_sha256": "<64 hex>",
  "manifest_bytes": 123,
  "bundle_path": "/var/lib/ai-image-factory/updater/local-builds/VERSION/bundle.tar.gz",
  "bundle_sha256": "<64 hex>",
  "bundle_bytes": 456,
  "build_record_path": "/var/lib/ai-image-factory/updater/local-builds/VERSION/build.json",
  "build_record_sha256": "<64 hex>",
  "build_record_bytes": 789
}
```

Set `AIF_UPDATE_LOCAL_BUNDLE_PIN` to the absolute pin path. Remove the old
`AIF_UPDATE_CANDIDATE_PIN` from all effective environment sources, including
systemd drop-ins; both pins are rejected together. Other updater settings and
site credentials remain unchanged. The local path does not invoke GitHub or
require an attestation workflow. Do not make a fake GitHub pin or wrapper.

## Prepare, then use owner Check/Apply

1. Keep Apply disabled in both gateway and updater. Check that there are no
   pending update commands or recovery descriptors. Independently verify active
   work/leases and protect unresolved original-task bindings before maintenance.
2. Before executing the new updater, verify its bytes against the reviewed local
   build and the package manifest. With the normal updater environment loaded,
   run that executable's `prepare-local` to stage and verify the complete tree.
3. If the fixed updater does not yet support this source, invoke the same
   verified executable's `bootstrap-local`. It reuses the existing maintenance
   locks and fixed-updater handoff and checks that application current, schema
   and business service identities have not changed. Do not rerun a completed
   bootstrap. This step does not migrate or activate the application.
4. Use the normal owner Check. Verify that its receipt says `source:local_build`
   and matches the reviewed version, commit, pin, build-record, manifest and
   bundle hashes/sizes. Check only stages; it does not activate the application.
5. Notify the operator before deployment. Enable Apply through the existing
   operational controls and use normal owner Apply for that exact checked
   version. The updater closes admission, quiesces, backs up, migrates, switches,
   validates and resumes the full runtime using the existing fenced flow.
6. Read back the completed Apply, schema/checksums, all process binary hashes,
   health and recovery point. Do not report business acceptance solely from a
   successful build or Check.

Each Check/Apply rereads the pin and verifies copied input bytes. Replacing the
pin while the daemon is running is rejected; restart the updater and run a fresh
Check. A prior Check cannot authorize a changed pin or bundle. Input failure
never falls back to GitHub. Tar safety, per-file digests/modes, ELF architecture,
Admin runtime, release identity and protected verified-manifest checks are
shared with existing sources.

Recovery continues through the real Apply command and its persisted descriptor.
Do not construct update database rows, alter `current` manually, or substitute a
random backup UUID for the existing recovery protocol. Restore schema/data and
artifacts together through the normal recovery service when that protocol
requires it; switching an old binary back after a migration is insufficient.
