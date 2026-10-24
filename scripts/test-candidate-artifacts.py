"""Offline rejection tests; no network, credentials, providers or generation."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location('candidate', Path(__file__).with_name('verify-candidate.py'))
CANDIDATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CANDIDATE)


class CandidateTests(unittest.TestCase):
    def pin(self):
        return dict(version='v0.1.0-20261009.hotfix.abc', commit_sha='a' * 40,
                    tag_object_sha='b' * 40, run_id=1, run_attempt=2,
                    artifact_id=3, artifact_sha256='c' * 64, artifact_bytes=100)

    def test_pin_rejects_unknown_unbounded_or_nonexact_identity(self):
        CANDIDATE.validate_pin(self.pin())
        for key, value in [('version', '../tag'), ('commit_sha', 'main'), ('run_id', 0),
                           ('run_attempt', True), ('artifact_bytes', CANDIDATE.MAX_BYTES + 1),
                           ('artifact_sha256', 'wrong'), ('unknown_source', 'anything')]:
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                CANDIDATE.validate_pin(dict(self.pin(), **{key: value}))

    def test_origin_rejects_changed_api_identity(self):
        pin = self.pin()
        repo = 'owner/repo'
        base = 'repos/' + repo
        endpoints = [base + '/git/ref/tags/' + pin['version'],
                     base + '/git/tags/' + pin['tag_object_sha'],
                     base + '/actions/runs/1', base + '/actions/artifacts/3']
        values = [dict(object=dict(type='tag', sha=pin['tag_object_sha'],
                                  url='https://api.github.com/' + endpoints[1])),
                  dict(verification=dict(verified=True, reason='valid'), tag=pin['version'],
                       object=dict(type='commit', sha=pin['commit_sha'])),
                  dict(id=1, run_attempt=2, head_sha=pin['commit_sha'], head_branch=pin['version'],
                       path=CANDIDATE.WORKFLOW, event='workflow_dispatch', status='completed',
                       conclusion='success', repository=dict(full_name=repo), head_repository=dict(full_name=repo)),
                  dict(id=3, name='candidate-publication', expired=False, size_in_bytes=100,
                       digest='sha256:' + pin['artifact_sha256'],
                       workflow_run=dict(id=1, head_sha=pin['commit_sha']))]

        def verify(fixtures):
            expected_calls = []
            def api(_gh, command, endpoint):
                self.assertEqual(command, 'api')
                self.assertIn(endpoint, endpoints)
                expected_calls.append(endpoint)
                return fixtures[endpoints.index(endpoint)]
            with patch.object(CANDIDATE, 'gh_json', side_effect=api):
                CANDIDATE.verify_origin('fixture-gh', repo, pin, 'publication')
            self.assertEqual(expected_calls, endpoints)

        verify(values)
        for index, path, replacement in [
            (0, ('object', 'sha'), 'e' * 40),
            (1, ('verification', 'verified'), False),
            (1, ('object', 'sha'), 'e' * 40),
            (2, ('repository', 'full_name'), 'other/repo'),
            (2, ('head_repository', 'full_name'), 'fork/repo'),
            (2, ('run_attempt',), 3), (2, ('event',), 'pull_request'),
            (2, ('path',), '.github/workflows/other.yml'),
            (2, ('conclusion',), 'failure'),
            (3, ('id',), 4), (3, ('digest',), 'sha256:' + 'e' * 64),
            (3, ('workflow_run', 'id'), 2), (3, ('expired',), True),
        ]:
            changed = json.loads(json.dumps(values))
            parent = changed[index]
            for key in path[:-1]:
                parent = parent[key]
            parent[path[-1]] = replacement
            with self.subTest(index=index, path=path), self.assertRaises(RuntimeError):
                verify(changed)

    def test_certificate_requires_trusted_fields_not_predicate(self):
        pin = self.pin()
        cert = dict(runInvocationURI='https://github.com/owner/repo/actions/runs/1/attempts/2',
                    buildSignerURI=f'https://github.com/owner/repo/.github/workflows/release.yml@refs/tags/{pin["version"]}',
                    buildSignerDigest=pin['commit_sha'], sourceRepositoryDigest=pin['commit_sha'],
                    sourceRepositoryRef='refs/tags/' + pin['version'], runnerEnvironment='github-hosted',
                    buildTrigger='workflow_dispatch', issuer='https://token.actions.githubusercontent.com')
        result = [{'verificationResult': {'signature': {'certificate': cert}}}]
        CANDIDATE.verify_certificate(result, 'owner/repo', pin)
        for key in cert:
            changed = json.loads(json.dumps(result))
            changed[0]['verificationResult']['signature']['certificate'][key] = 'wrong'
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                CANDIDATE.verify_certificate(changed, 'owner/repo', pin)
        with self.assertRaises(RuntimeError):
            CANDIDATE.verify_certificate([{'verificationResult': {'statement': {'predicate': cert}}}], 'owner/repo', pin)

    def test_archive_rejects_traversal_duplicates_and_links(self):
        for entries in [[('../escape', b'x')], [('safe', b'x'), ('safe', b'y')]]:
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                archive = directory / 'candidate.zip'
                with zipfile.ZipFile(archive, 'w') as zipped:
                    for name, data in entries:
                        zipped.writestr(name, data)
                with self.assertRaises(RuntimeError):
                    CANDIDATE.extract_assets(archive, directory, {'safe'})
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            archive = directory / 'candidate.zip'
            with zipfile.ZipFile(archive, 'w') as zipped:
                item = zipfile.ZipInfo('safe')
                item.external_attr = 0o120777 << 16
                zipped.writestr(item, '../escape')
            with self.assertRaises(RuntimeError):
                CANDIDATE.extract_assets(archive, directory, {'safe'})

    def test_archive_preserves_exact_bytes_without_overwriting(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            archive = directory / 'candidate.zip'
            with zipfile.ZipFile(archive, 'w') as zipped:
                zipped.writestr('safe', b'verified bytes')
            CANDIDATE.extract_assets(archive, directory, {'safe'})
            self.assertEqual((directory / 'safe').read_bytes(), b'verified bytes')
            with self.assertRaises(FileExistsError):
                CANDIDATE.extract_assets(archive, directory, {'safe'})

    def test_publication_uses_only_five_exact_original_assets(self):
        names = CANDIDATE.expected_names(self.pin()['version'], 'publication')
        self.assertEqual(len(names), 5)
        self.assertIn('install-release', names)
        self.assertFalse(any('/' in name for name in names))


class ReleaseAssetGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workflow = Path(__file__).resolve().parents[1] / '.github/workflows/release.yml'
        step = workflow.read_text().split(
            '      - name: Verify release assets before attestation\n', 1)[1].split(
            '\n      - name: Attest release assets\n', 1)[0]
        run = textwrap.dedent(step.split('        run: |\n', 1)[1])
        schema = run[:run.index('if test "$PUBLICATION_MODE"')]
        validation = run[run.index('jq --exit-status \\\n'):run.index('test "$(stat --format=')]
        # Execute the workflow's schema, manifest and bundle hash gates verbatim.
        cls.gate = schema + validation

    def run_gate(self, migrations, target_schema=140, migration_version=None, **changes):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            migration_directory = directory / 'crates/image-gateway/migrations'
            migration_directory.mkdir(parents=True)
            for version in migrations:
                (migration_directory / f'{version:04d}_fixture.sql').write_text('-- fixture\n')
            bundle = directory / 'fixture.tar.gz'
            bundle.write_bytes(b'fixture bundle bytes')
            manifest = dict(
                schema_version=1, updater_protocol_version=1,
                release_version='v0.1.0-fixture', commit_sha='a' * 40,
                target_triple='x86_64-unknown-linux-gnu',
                migration_version=target_schema if migration_version is None else migration_version,
                min_schema_version=128, target_schema_version=target_schema,
                rollback_mode='backup_restore',
                files=[dict(path='bin/gateway', mode=493),
                       dict(path='release.json', mode=420)],
                bundle_sha256=hashlib.sha256(bundle.read_bytes()).hexdigest(),
            )
            manifest.update(changes)
            manifest_path = directory / 'fixture.manifest.json'
            manifest_path.write_text(json.dumps(manifest))
            return subprocess.run(
                ['bash', '-c', self.gate], cwd=directory,
                env=dict(os.environ, RELEASE_VERSION='v0.1.0-fixture', RELEASE_COMMIT='a' * 40,
                         target='x86_64-unknown-linux-gnu', manifest=str(manifest_path),
                         bundle=str(bundle)),
                capture_output=True, text=True, timeout=10,
            )

    def test_schema_follows_source_migrations(self):
        for version in [139, 140, 141]:
            with self.subTest(version=version):
                result = self.run_gate([128, version], target_schema=version)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_schema_rejects_missing_migrations_and_manifest_disagreement(self):
        for migrations, target, migration in [
            ([], 140, 140), ([140], 139, 139), ([140], 141, 141), ([140], 140, 139),
        ]:
            with self.subTest(migrations=migrations, target=target, migration=migration):
                result = self.run_gate(migrations, target_schema=target, migration_version=migration)
                self.assertNotEqual(result.returncode, 0)

    def test_existing_identity_files_modes_and_bundle_hash_gates_remain_enforced(self):
        for key, value in [
            ('schema_version', 2), ('updater_protocol_version', 2),
            ('release_version', 'wrong'), ('commit_sha', 'b' * 40),
            ('target_triple', 'aarch64-unknown-linux-gnu'),
            ('min_schema_version', 127), ('min_schema_version', 129),
            ('rollback_mode', 'none'), ('files', []),
            ('files', [dict(path='bin/gateway', mode=420)]),
            ('files', [dict(path='release.json', mode=493)]),
            ('bundle_sha256', '0' * 64),
        ]:
            with self.subTest(key=key, value=value):
                self.assertNotEqual(self.run_gate([140], **{key: value}).returncode, 0)


if __name__ == '__main__':
    unittest.main()
