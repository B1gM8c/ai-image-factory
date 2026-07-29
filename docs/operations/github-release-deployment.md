# GitHub Release Deployment

This document covers the Linux release artifact, systemd layout, and immutable
GitHub Release transport. It complements, but does not replace, the production
release runbook.

## Release Unit

Each protected `v*` tag produces two architecture assets:

```text
ai-image-factory-VERSION-x86_64-unknown-linux-gnu.tar.gz
ai-image-factory-VERSION-x86_64-unknown-linux-gnu.manifest.json
ai-image-factory-VERSION-aarch64-unknown-linux-gnu.tar.gz
ai-image-factory-VERSION-aarch64-unknown-linux-gnu.manifest.json
```

The archive contains one immutable process set:

```text
bin/
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
  updated
  webhookd
  workerd
admin/
  server.js
  standalone.tar.gz
release.json
```

`admin/standalone.tar.gz` is the architecture-matched Next.js standalone tree.
The small `admin/server.js` bootstrap verifies its embedded digest and expands
it beneath `/var/lib/ai-image-factory/admin-runtime/VERSION` on first start.
This preserves the updater's narrow outer-path policy while supporting Next.js
route-group paths.

The manifest records the exact tag, 40-character Git commit, target triple,
updater protocol, schema compatibility window, rollback contract, bundle
digest, and a sorted digest plus fixed Unix mode for every outer release file.
`release.json`
contains the same release identity and is the Gateway's runtime source of truth
after an atomic switch. `scripts/package-release.sh` normalizes ownership,
permissions, mtimes, tar ordering, and gzip headers.

## GitHub Repository Gates

Before creating the first tag:

1. Configure the repository remote and push the protected release commit.
2. Protect `v*` tags so only the release owner can create them.
3. Create a protected `production-release` environment with required approval.
4. Enable GitHub Immutable Releases.
5. Keep workflow permissions restricted; the release job alone receives
   `contents: write`, `id-token: write`, and `attestations: write`.
6. Keep every Action pinned to the complete commit SHA already recorded in
   `.github/workflows/release.yml`.

The workflow builds natively on `ubuntu-24.04` and `ubuntu-24.04-arm`. If the
ARM hosted runner is unavailable for the repository plan, replace only the
runner label with a hardened GitHub-hosted ARM runner. Automatic apply currently
rejects self-hosted attestation identities.

Create a release by pushing a protected tag:

```bash
git tag -s v1.0.0 -m "AI Image Factory v1.0.0"
git push origin v1.0.0
```

The workflow tests the workspace, builds both targets, creates a draft Release,
uploads every asset, emits GitHub provenance attestations, and publishes only
after all uploads succeed. Do not publish a partial draft manually.

Verify a published asset before installation:

```bash
gh release verify v1.0.0 --repo OWNER/REPOSITORY
gh release download v1.0.0 \
  --repo OWNER/REPOSITORY \
  --pattern 'ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.*'
gh release verify-asset v1.0.0 \
  ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.tar.gz \
  --repo OWNER/REPOSITORY
gh release verify-asset v1.0.0 \
  ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.manifest.json \
  --repo OWNER/REPOSITORY
gh attestation verify \
  ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.tar.gz \
  --repo OWNER/REPOSITORY \
  --signer-workflow OWNER/REPOSITORY/.github/workflows/release.yml \
  --source-ref refs/tags/v1.0.0 \
  --source-digest COMMIT_SHA \
  --deny-self-hosted-runners
gh attestation verify \
  ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.manifest.json \
  --repo OWNER/REPOSITORY \
  --signer-workflow OWNER/REPOSITORY/.github/workflows/release.yml \
  --source-ref refs/tags/v1.0.0 \
  --source-digest COMMIT_SHA \
  --deny-self-hosted-runners
```

## Host Layout

```text
/opt/ai-image-factory/
  current -> releases/VERSION
  releases/VERSION/
  staging/
/etc/ai-image-factory/
  app.env
  update-policy.env
  updater.env
/usr/libexec/ai-image-factory/
  updated
  hooks/
/var/lib/ai-image-factory/
  admin-runtime/
  artifacts/
  backups/
  credentials/
  runner/
  updater/
    recovery/
```

The updater executable is installed outside `/opt/ai-image-factory/current`.
It is not part of `ai-image-factory.target`, so stopping or switching the
application cannot terminate the process responsible for recovery. The release
still carries `bin/updated` as the reviewed candidate for an explicit,
supervisor-controlled updater upgrade.

`update-policy.env` is the shared, secretless contract consumed by both the
Gateway and updater. It pins the repository, target triple, release metadata
path, and apply gate so the two processes cannot report or apply different
release identities.

`updater.env` is root-only and contains two distinct database credentials:

- `AIF_UPDATER_DATABASE_URL` can lease update commands and update release state;
- `AIF_MIGRATOR_DATABASE_URL` is used only by backup, migration, and recovery.

Do not reuse the Gateway application's `DATABASE_URL`. Provision separate
PostgreSQL roles and grant the updater control role only the system-update
tables plus read access to `_sqlx_migrations`. The migrator role owns the
application schema and is not exposed to the Gateway or admin console.

## Bootstrap Installation

Create the service account, install the fixed deployment files, and create
state directories:

```bash
sudo useradd --system --home /var/lib/ai-image-factory \
  --shell /usr/sbin/nologin ai-image-factory
sudo install -D -m 0644 deploy/systemd/ai-image-factory.target \
  /etc/systemd/system/ai-image-factory.target
sudo install -m 0644 deploy/systemd/*.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/ai-image-factory-tmpfiles.conf \
  /etc/tmpfiles.d/ai-image-factory.conf
sudo systemd-tmpfiles --create /etc/tmpfiles.d/ai-image-factory.conf
sudo install -d -m 0755 /usr/libexec/ai-image-factory/hooks
sudo install -m 0755 deploy/hooks/* /usr/libexec/ai-image-factory/hooks/
sudo install -m 0755 deploy/upgrade-updater \
  /usr/libexec/ai-image-factory/upgrade-updater
sudo install -m 0755 deploy/install-release \
  /usr/libexec/ai-image-factory/install-release
sudo install -d -m 0750 /etc/ai-image-factory
sudo install -m 0600 deploy/systemd/app.env.example \
  /etc/ai-image-factory/app.env
sudo install -m 0600 deploy/systemd/update-policy.env.example \
  /etc/ai-image-factory/update-policy.env
sudo install -m 0600 deploy/systemd/updater.env.example \
  /etc/ai-image-factory/updater.env
```

Replace every placeholder and load secrets from the host's secret manager.
None of the example environment files is production-ready. Keep GitHub
credentials out of `update-policy.env`; it is intentionally shared with the
unprivileged Gateway. Keep both updater database URLs only in mode-0600
`updater.env`. Authenticate `gh` in the updater's protected home or use a
systemd credential available only to the updater.

After completing every Release, asset, and attestation check above, install the
first application release from the downloaded files:

```bash
sudo /usr/libexec/ai-image-factory/install-release \
  v1.0.0 \
  x86_64-unknown-linux-gnu \
  ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.tar.gz \
  ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.manifest.json
sudo systemctl daemon-reload
sudo systemctl enable --now ai-image-factory.target
sudo systemctl enable --now ai-image-factory-updater.service
```

The bootstrap installer is intentionally first-install only. It verifies the
manifest-to-bundle digest and size, rejects duplicate, escaping, linked, or
special archive members, verifies every file digest and fixed mode, validates
`release.json`, atomically installs the release, installs the initial fixed
updater, and only then creates `current`. It never replaces an existing release
or pointer; later changes must use the updater state machine.

Enable conditional daemons only when their corresponding feature is configured:

```bash
sudo systemctl enable ai-image-factory-provider-submitd.service
sudo systemctl enable ai-image-factory-provider-pollerd.service
sudo systemctl enable ai-image-factory-webhookd.service
sudo systemctl restart ai-image-factory.target
```

Additional executor profiles use escaped systemd instances:

```bash
sudo systemctl enable --now \
  "ai-image-factory-executord@$(systemd-escape 'PROFILE_KEY').service"
```

## Update Hooks And Recovery

The fixed hooks:

- stop and resume the application target;
- create one PostgreSQL plus artifact recovery point;
- activate the complete new process set;
- check required services, Gateway health/readiness/OpenAPI, and the admin login;
- restore both PostgreSQL and artifacts after a post-migration failure.

Database recovery uses a plain, clean SQL recovery image through
`psql --single-transaction`, so a failed restore cannot commit a partial
schema. Artifact recovery extracts into
a sibling temporary directory, verifies the archive and recovery digests, fsyncs
the restored tree, and then switches directories with same-filesystem renames.

Before quiescing services, the updater atomically writes a mode-0600 recovery
descriptor under `/var/lib/ai-image-factory/updater/recovery`. After a recovery
point exists, the descriptor also contains the opaque backup token. If the
updater process dies in an unsafe phase, its successor fences the expired lease,
restores the previous release and recovery point, and only then releases the
global update gate. If automatic recovery fails, `restore_required` blocks every
new check or apply command until an operator completes the recovery runbook.

For an operator retry, stop the polling daemon, invoke the fixed updater, and
then restart it:

```bash
sudo systemctl stop ai-image-factory-updater.service
sudo systemctl start ai-image-factory-updater-recover@COMMAND_ID.service
sudo journalctl -u ai-image-factory-updater-recover@COMMAND_ID.service
sudo systemctl start ai-image-factory-updater.service
```

The recovery template reads the same root-only environment files as the daemon.
Never place database URLs on the command line.

The updater receives repository access through the system GitHub CLI
configuration. Use a read-only token scoped only to Release contents and
attestations. The browser never receives this credential.

A check command downloads the architecture manifest and bundle, verifies the
immutable Release, asset digests, archive shape, provenance, source tag, source
commit, file modes, release identity, and schema contract before marking a
version as verified. A GitHub tag alone is never treated as installable.

The application update does not replace the running updater process. After a
release containing a required updater security fix has passed application
verification, upgrade the fixed binary through the supervisor-controlled
two-phase helper:

```bash
sudo /usr/libexec/ai-image-factory/upgrade-updater
```

The helper accepts only a root-owned candidate beneath the verified releases
tree, checks its protocol identity, atomically replaces the fixed binary,
restarts the updater, and restores the previous binary if startup fails.

Keep `AIF_UPDATE_APPLY_ENABLED=false` for initial deployment. Enable automatic
apply only after all of the following have passed on a production-shaped clone:

1. a fresh install and an upgrade from the immediately preceding schema;
2. a PostgreSQL plus artifact backup restore drill;
3. failure injection after quiesce, backup, migration, symlink switch,
   activation, and verification;
4. confirmation that the reverse proxy keeps public admission closed until the
   verify hook succeeds;
5. exact `gh release verify`, `verify-asset`, and attestation checks for both
   architectures.
6. a single-host failure-injection drill proving the `quiescing` lease takeover
   and explicit `restore_required` recovery command.

After those gates pass, set `AIF_UPDATE_APPLY_ENABLED=true` and restart both
processes that cache the policy:

```bash
sudo systemctl restart \
  ai-image-factory-gateway.service \
  ai-image-factory-updater.service
```

This implementation is intentionally single-host; do not enable automatic
apply on a multi-host fleet until rolling admission, leader election, and
per-host activation state are modeled.

## Local Validation

The packaging script consumes prebuilt Linux binaries and a completed Next.js
standalone build:

```bash
npm ci
npm run build:admin
cargo build --locked --release \
  --target x86_64-unknown-linux-gnu \
  --package gpt-image-2-gateway --bins
cargo build --locked --release \
  --target x86_64-unknown-linux-gnu \
  --package ai-image-factory-updater \
  --bin updated
SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)" \
  scripts/package-release.sh \
  v0.0.0-test \
  x86_64-unknown-linux-gnu \
  "$(git rev-parse HEAD)" \
  /tmp/ai-image-factory-release
```

Validate systemd and shell files on a Linux host:

```bash
systemd-analyze verify deploy/systemd/*.service deploy/systemd/*.target
shellcheck scripts/package-release.sh deploy/hooks/* deploy/install-release deploy/upgrade-updater
bash -n scripts/package-release.sh deploy/hooks/* deploy/install-release deploy/upgrade-updater
```
