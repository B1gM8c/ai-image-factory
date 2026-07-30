# Security Policy

## Supported Versions

AI Image Factory is under active development. Security fixes are applied to the
latest revision of the default branch. Older commits and unpublished deployment
snapshots are not maintained as separate supported releases.

## Reporting a Vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's private vulnerability reporting for this repository. Include:

- affected revision and deployment topology
- a concise reproduction
- expected and observed behavior
- impact and required attacker access
- logs or screenshots with credentials and personal data removed
- any temporary mitigation already applied

If private vulnerability reporting is unavailable, contact the repository
owner through the private contact method on their GitHub profile and request a
secure reporting channel. Do not send secrets in the first message.

## Deployment Responsibilities

Operators must provide and protect:

- JWT signing keys and refresh-token peppers
- database, provider, object-storage, and webhook credentials
- TLS termination and trusted-origin configuration
- least-privilege database roles, backup, restore, and audit retention
- isolated credential homes and OS permissions for CLI accounts
- positive pricing and explicit activation for every billable production model

The repository's development defaults are not a production security profile.
