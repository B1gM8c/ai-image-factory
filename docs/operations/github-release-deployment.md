# GitHub Release Deployment

This document covers the Linux release artifact, systemd layout, and immutable
GitHub Release transport. It complements, but does not replace, the production
release runbook.

## Release Unit

Each protected `v*` tag produces two assets per architecture plus one
architecture-independent bootstrap installer:

```text
ai-image-factory-VERSION-x86_64-unknown-linux-gnu.tar.gz
ai-image-factory-VERSION-x86_64-unknown-linux-gnu.manifest.json
ai-image-factory-VERSION-aarch64-unknown-linux-gnu.tar.gz
ai-image-factory-VERSION-aarch64-unknown-linux-gnu.manifest.json
install-release
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
ops/
  hooks/
  systemd/
  docs/github-release-deployment.md
  install-release
  upgrade-updater
release.json
```

`admin/standalone.tar.gz` is the architecture-matched Next.js standalone tree.
Every native `.node` module and every ELF within that inner archive must match
the release target; Mach-O and PE payloads are rejected. The small
`admin/server.js` bootstrap verifies its embedded digest and expands it beneath
`/var/lib/ai-image-factory/admin-runtime/VERSION` on first start. This preserves
the updater's narrow outer-path policy while supporting Next.js route-group
paths.

The manifest records the exact tag, 40-character Git commit, target triple,
updater protocol, schema compatibility window, rollback contract, bundle
digest, and a sorted digest plus fixed Unix mode for every outer release file.
Every file under `bin/` must also be a 64-bit little-endian ELF whose machine
type matches that target triple. The package builder, bootstrap installer, and
running updater enforce this independently.
`release.json`
contains the same release identity and is the Gateway's runtime source of truth
after an atomic switch. `scripts/package-release.sh` normalizes ownership,
permissions, mtimes, tar ordering, and gzip headers.

## GitHub Repository Gates

Before creating the first tag:

1. Configure the repository remote and push the protected release commit.
2. Protect `v*` tags so only the release owner can create them, and configure
   non-bypassable rules that restrict tag updates and deletions.
3. Create a protected `production-release` environment with required approval.
4. Enable GitHub Immutable Releases.
5. Create a fine-grained `RELEASE_POLICY_TOKEN` environment secret with only
   repository Administration read access. The release job uses it only to
   prove that immutable releases are enabled before creating a draft.
6. Keep workflow permissions restricted; the release job alone receives
   `contents: write`, `id-token: write`, and `attestations: write`.
7. Keep every Action pinned to the complete commit SHA already recorded in
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

Download the published assets, copy them into a root-owned directory, and verify
those protected copies. The installer refuses caller-writable paths, so the
bytes verified here are the same bytes it later opens as root:

```bash
release_version=v1.0.0
target_triple=x86_64-unknown-linux-gnu
asset_prefix="ai-image-factory-${release_version}-${target_triple}"
protected_assets="/opt/ai-image-factory/bootstrap/${release_version}"

gh release verify v1.0.0 --repo OWNER/REPOSITORY
gh release download v1.0.0 \
  --repo OWNER/REPOSITORY \
  --pattern 'ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.*'
gh release download v1.0.0 \
  --repo OWNER/REPOSITORY \
  --pattern install-release
sudo install -d -m 0755 "$protected_assets"
sudo install -m 0644 \
  "${asset_prefix}.tar.gz" \
  "${asset_prefix}.manifest.json" \
  "$protected_assets/"
sudo install -m 0755 install-release "$protected_assets/"

gh release verify-asset v1.0.0 \
  "${protected_assets}/${asset_prefix}.tar.gz" \
  --repo OWNER/REPOSITORY
gh release verify-asset v1.0.0 \
  "${protected_assets}/${asset_prefix}.manifest.json" \
  --repo OWNER/REPOSITORY
gh release verify-asset v1.0.0 \
  "${protected_assets}/install-release" \
  --repo OWNER/REPOSITORY
gh attestation verify \
  "${protected_assets}/${asset_prefix}.tar.gz" \
  --repo OWNER/REPOSITORY \
  --signer-workflow OWNER/REPOSITORY/.github/workflows/release.yml \
  --source-ref refs/tags/v1.0.0 \
  --source-digest COMMIT_SHA \
  --deny-self-hosted-runners
gh attestation verify \
  "${protected_assets}/${asset_prefix}.manifest.json" \
  --repo OWNER/REPOSITORY \
  --signer-workflow OWNER/REPOSITORY/.github/workflows/release.yml \
  --source-ref refs/tags/v1.0.0 \
  --source-digest COMMIT_SHA \
  --deny-self-hosted-runners
gh attestation verify \
  "${protected_assets}/install-release" \
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
  admin.env
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

`ai-image-factory.target` is the public boot target. It cannot start the
internal `ai-image-factory-processes.target` until
`ai-image-factory-recovery-gate.service` has recovered every protected local
descriptor. Hooks operate on the internal process target so recovery can start
and verify the previous release without recursively waiting on its own boot
gate. The recovery gate is a transient oneshot rather than a latched active
unit, so every later business-service start executes `recover-pending` again.
If the gate fails, its failure unit immediately quiesces the installation and
keeps admission closed. The failure unit does not load updater credentials or
run a configurable root hook; it executes only the fixed packaged quiesce hook.

Update hooks use two process scopes. Validation starts only Gateway and admin.
Before starting any side-effecting daemon, the updater persists the
lease-fenced `activating_full` or `restoring` phase. Reconcilers, executors,
provider submitters and pollers, reducers, webhooks, and workers start only in
the full scope after that durable transition. The updater commits terminal
`succeeded` or `restored` only after full verification and admission reopening
succeed. A database restore may temporarily block the row heartbeat while it
rebuilds the application schema. During that protected window, the host lock,
the independently probed PostgreSQL advisory lock, and the local recovery
descriptor remain authoritative; the restore transaction atomically reasserts
the same command owner and epoch before it commits.

`update-policy.env` is the shared, secretless contract consumed by both the
Gateway and updater. It pins the repository, target triple, release metadata
path, and apply gate so the two processes cannot report or apply different
release identities.

`admin.env` contains only the Gateway origin and admin BFF settings. The admin
service must never inherit `DATABASE_URL` or provider credentials from
`app.env`.

`updater.env` is root-only and contains two distinct database credentials:

- `AIF_UPDATER_DATABASE_URL` can lease update commands and update release state;
- `AIF_MIGRATOR_DATABASE_URL` is used only by backup, migration, and recovery.

Do not reuse the Gateway application's `DATABASE_URL`. Provision separate
PostgreSQL roles and grant the updater control role only the system-update
tables plus read access to `_sqlx_migrations`. The migrator role owns the
application schema and is not exposed to the Gateway or admin console.

## Bootstrap Installation

Run the installer directly from the protected copy verified above. It installs
the verified release tree first; every privileged systemd unit and hook is then
copied from that tree instead of from an arbitrary Git checkout:

```bash
sudo /opt/ai-image-factory/bootstrap/v1.0.0/install-release \
  v1.0.0 \
  x86_64-unknown-linux-gnu \
  /opt/ai-image-factory/bootstrap/v1.0.0/ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.tar.gz \
  /opt/ai-image-factory/bootstrap/v1.0.0/ai-image-factory-v1.0.0-x86_64-unknown-linux-gnu.manifest.json
sudo install -m 0755 \
  /opt/ai-image-factory/bootstrap/v1.0.0/install-release \
  /usr/libexec/ai-image-factory/install-release

id ai-image-factory >/dev/null 2>&1 || \
  sudo useradd --system --home /var/lib/ai-image-factory \
    --shell /usr/sbin/nologin ai-image-factory
sudo install -m 0644 \
  /opt/ai-image-factory/current/ops/systemd/*.target \
  /etc/systemd/system/
sudo install -m 0644 \
  /opt/ai-image-factory/current/ops/systemd/*.service \
  /etc/systemd/system/
sudo install -m 0644 \
  /opt/ai-image-factory/current/ops/systemd/ai-image-factory-tmpfiles.conf \
  /etc/tmpfiles.d/ai-image-factory.conf
sudo systemd-tmpfiles --create /etc/tmpfiles.d/ai-image-factory.conf
sudo install -d -m 0755 /usr/libexec/ai-image-factory/hooks
sudo install -m 0755 \
  /opt/ai-image-factory/current/ops/hooks/* \
  /usr/libexec/ai-image-factory/hooks/
sudo install -m 0755 \
  /opt/ai-image-factory/current/ops/upgrade-updater \
  /usr/libexec/ai-image-factory/upgrade-updater
sudo install -d -m 0750 /etc/ai-image-factory
sudo install -d -m 0750 /etc/ai-image-factory/update-hooks
sudo install -m 0600 \
  /opt/ai-image-factory/current/ops/systemd/admin.env.example \
  /etc/ai-image-factory/admin.env
sudo install -m 0600 \
  /opt/ai-image-factory/current/ops/systemd/app.env.example \
  /etc/ai-image-factory/app.env
sudo install -m 0600 \
  /opt/ai-image-factory/current/ops/systemd/update-policy.env.example \
  /etc/ai-image-factory/update-policy.env
sudo install -m 0600 \
  /opt/ai-image-factory/current/ops/systemd/updater.env.example \
  /etc/ai-image-factory/updater.env
sudo install -m 0644 \
  /opt/ai-image-factory/current/ops/docs/*.md \
  /usr/share/doc/ai-image-factory/
```

Replace every placeholder and load secrets from the host's secret manager.
None of the example environment files is production-ready. Keep GitHub
credentials out of `update-policy.env`; it is intentionally shared with the
unprivileged Gateway. Keep both updater database URLs only in mode-0600
`updater.env`. Authenticate `gh` in the updater's protected home:

```bash
sudo install -d -m 0700 /var/lib/ai-image-factory/updater/.config/gh
sudo env HOME=/var/lib/ai-image-factory/updater gh auth login
sudo env HOME=/var/lib/ai-image-factory/updater \
  gh auth status --active --hostname github.com
sudo env HOME=/var/lib/ai-image-factory/updater \
  gh api repos/OWNER/REPOSITORY/releases --silent
```

Use a read-only token limited to Release contents and attestations. A systemd
credential available only to the updater is also acceptable.

Before enabling automatic Apply, install root-owned, mode-0755 site scripts at
`/etc/ai-image-factory/update-hooks/admission-close` and `admission-open`, then
uncomment their absolute paths in `updater.env`. They must be idempotent and
must operate the real Internet-facing reverse proxy or load balancer. The
updater refuses every Apply command when either hook is absent; a no-op hook is
not a valid production configuration.

After replacing every placeholder and installing the admission hooks, load the
verified unit topology. Migrate and prove that the database reached the release
manifest's exact target before any application target is enabled:

```bash
export AIF_RELEASE_MANIFEST=/secure/path/to/ai-image-factory-vX.Y.Z-linux-x86_64.manifest.json
sudo systemctl daemon-reload
sudo systemd-run \
  --unit=ai-image-factory-bootstrap-migrate \
  --wait --collect --pipe \
  --property=Type=oneshot \
  --property=EnvironmentFile=/etc/ai-image-factory/updater.env \
  --setenv=AIF_RELEASE_MANIFEST="$AIF_RELEASE_MANIFEST" \
  /bin/sh -ceu '
    export DATABASE_URL="$AIF_MIGRATOR_DATABASE_URL"
    /opt/ai-image-factory/current/bin/factoryctl migrate
    expected="$(node -p \
      "require(process.env.AIF_RELEASE_MANIFEST).migration_version")"
    actual="$(PGOPTIONS="-c search_path=$GATEWAY_DATABASE_SCHEMA" \
      PGDATABASE="$AIF_MIGRATOR_DATABASE_URL" psql \
      --tuples-only --no-align \
      --command="SELECT COALESCE(MAX(version), -1) FROM _sqlx_migrations WHERE success")"
    test "$actual" = "$expected"
  '
sudo systemctl enable --now ai-image-factory.target
sudo systemctl enable --now ai-image-factory-updater.service
```

The bootstrap installer is intentionally first-install only. It verifies the
root ownership and write protection of its input directory and assets, verifies
the manifest-to-bundle digest and size, rejects duplicate, escaping, linked, or
special archive members, checks both outer binaries and inner admin native
modules against the target architecture, verifies every file digest and fixed
mode, validates `release.json`, atomically installs the release, installs the
initial fixed updater, and only then creates `current`. A retry may resume the
same fully verified release after a crash, but it never replaces different
existing content or a pointer to another release; later changes must use the
updater state machine.

Automatic application updates do not overwrite root-owned systemd units or
hooks. If a release changes `ops/`, review the diff and copy those files through
a separate privileged maintenance window before enabling application Apply.

Enable conditional daemons only when their corresponding feature is configured:

```bash
sudo systemctl enable ai-image-factory-provider-submitd.service
sudo systemctl enable ai-image-factory-provider-pollerd.service
sudo systemctl enable ai-image-factory-webhookd.service
sudo systemctl restart ai-image-factory-processes.target
```

Additional executor profiles use escaped systemd instances:

```bash
sudo systemctl enable --now \
  "ai-image-factory-executord@$(systemd-escape 'PROFILE_KEY').service"
```

## Update Hooks And Recovery

The fixed hooks and site admission hooks:

- close public admission before mutation and reopen it only after verification;
- stop and resume the internal application process target;
- create one PostgreSQL plus artifact recovery point;
- activate the complete new process set;
- check required services, Gateway health/readiness/OpenAPI, and the admin login;
- restore both PostgreSQL and artifacts after a post-migration failure.

Database recovery uses a plain, clean SQL recovery image through
`psql --single-transaction`, so a failed restore cannot commit a partial
schema. The restored update command and its recovery lease are reasserted in
that same transaction. A PostgreSQL transaction advisory lock serializes every
check, apply, startup recovery, and operator recovery across updater hosts; the
transaction is rolled back on cancellation so a lock cannot return to the pool.
Both the advisory-lock probe and command-lease heartbeat have a ten-second
deadline. A timeout cancels the current hook process group, closes admission,
and quiesces application processes.
Artifact recovery extracts into
a sibling temporary directory, verifies the archive and recovery digests, fsyncs
the restored tree, and then switches directories with same-filesystem renames.

Before closing admission, the updater atomically writes a mode-0600 recovery
descriptor under `/var/lib/ai-image-factory/updater/recovery`. After a recovery
point exists, the descriptor also contains the opaque backup token. If the
updater process dies in an unsafe phase, the boot recovery gate takes ownership
of the descriptor before any public application target can start, restores and
fully verifies the previous release, reopens admission, and only then releases
startup. The polling daemon uses the same recovery path for expired leases. If
automatic recovery fails, `restore_required` blocks every new check or apply
command and the boot gate remains failed until an operator completes the
recovery runbook.

Every application service explicitly requires and starts after the recovery
gate. During gate-owned recovery, only the fixed recovery hook may bypass that
requirement. It starts services in the declared pipeline order and deliberately
does not attach either application target; the original boot transaction does
that only after the gate exits successfully. Updater-owned direct starts retain
systemd ordering, and a successful full activation reattaches both application
targets before the command becomes terminal. Ordinary operator or boot starts
still execute the recovery gate. A failed, cancelled, or timed-out gate or
operator recovery runs the fixed fail-closed hook before systemd can expose the
application target.
The guarded updater attempts the configured site admission-close hook while its
trust and database fences are valid; the independent failure unit always
quiesces every application process without loading database credentials.

For an operator retry, stop the polling daemon, invoke the fixed updater, and
then restart it:

```bash
sudo systemctl stop ai-image-factory-updater.service
sudo systemctl start ai-image-factory-updater-recover@COMMAND_ID.service
sudo journalctl -u ai-image-factory-updater-recover@COMMAND_ID.service
sudo systemctl start ai-image-factory.target
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
4. failure-injection proof that the configured admission hooks keep public
   traffic closed until the verify hook succeeds and reopen it after rollback;
5. exact `gh release verify`, `verify-asset`, and attestation checks for both
   architectures.
6. a cold-boot failure-injection drill proving
   `ai-image-factory-recovery-gate.service` recovers unfinished descriptors
   before `ai-image-factory-processes.target`;
7. an explicit `restore_required` operator recovery command;
8. forced updater heartbeat and PostgreSQL advisory-lock loss during every
   mutating phase, proving the hook process group is terminated and admission
   remains closed;
9. proof that validation scope starts only Gateway and admin, that the leased
   `activating_full` or `restoring` phase precedes every side-effecting daemon,
   that service ordering is preserved, both application targets are active
   after full recovery, and terminal state follows full verification and
   admission reopening;
10. repeated stop/start of `ai-image-factory.target`, proving the transient
    recovery gate executes on every boot rather than relying on a previous
    successful gate result.

After those gates pass, set `AIF_UPDATE_APPLY_ENABLED=true` and restart both
processes that cache the policy:

```bash
sudo systemctl restart \
  ai-image-factory-gateway.service \
  ai-image-factory-updater.service
```

The database advisory lock prevents multiple updater hosts from mutating one
installation concurrently. Application activation is still intentionally a
single-host systemd transaction; do not enable automatic apply on a multi-host
application fleet until rolling admission and per-host activation state are
modeled.

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
