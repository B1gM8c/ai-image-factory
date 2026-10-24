#!/usr/bin/env python3
"""Offline tests; no systemd, production configuration or generation calls."""
import hashlib
import importlib.machinery
import contextlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

HOOK = Path(__file__).resolve().parents[1] / 'deploy/hooks/sync-remote-runner-pins'
pins = importlib.machinery.SourceFileLoader('runner_pins', str(HOOK)).load_module()


class RunnerPinTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.release = self.root / 'factory'
        self.config = self.root / 'config'
        self.profiles = self.config / 'remote-providers'
        self.profiles.mkdir(parents=True)
        (self.release / 'verified-manifests').mkdir(parents=True)
        self.uid = os.getuid()
        self.calls = []
        self.states = {}
        self.old = self.add_release('v1', b'old runner')
        self.new = self.add_release('v2', b'new runner')
        self.select('v1')
        self.first = self.add_profile('managed.images', self.old)
        self.second = self.add_profile('managed.videos', self.old)

    def add_release(self, version, data):
        directory = self.release / 'releases' / version
        (directory / 'bin').mkdir(parents=True)
        runner = directory / 'bin/remote-submit-runner'
        runner.write_bytes(data)
        runner.chmod(0o755)
        identity = dict(schema_version=1, release_version=version, commit_sha='a' * 40,
                        target_triple='x86_64-unknown-linux-gnu')
        (directory / 'release.json').write_text(json.dumps(identity))
        manifest = dict(identity, files=[dict(path='bin/remote-submit-runner', mode=0o755,
                                             bytes=len(data), sha256=hashlib.sha256(data).hexdigest())])
        self.manifest(version).write_text(json.dumps(manifest))
        return hashlib.sha256(data).hexdigest()

    def manifest(self, version='v2'):
        return self.release / 'verified-manifests' / (version + '-x86_64-unknown-linux-gnu.manifest.json')

    def select(self, version):
        current = self.release / 'current'
        current.unlink(missing_ok=True)
        current.symlink_to('releases/' + version)

    def add_profile(self, name, digest, runner=None):
        path = self.profiles / (name + '.env')
        runner = runner or str(self.release / 'current/bin/remote-submit-runner')
        path.write_bytes((f'SECRET_VALUE=never-print-this\r\n'
                          f'{pins.RUNNER}={runner}\r\n'
                          f'  {pins.PIN} = "{digest}"  \r\n'
                          '# untouched\r\n').encode())
        path.chmod(0o640)
        return path

    def state(self, unit):
        self.calls.append(unit)
        return self.states.get(unit, dict(ActiveState='inactive', MainPID='0',
                                          UnitFileState='disabled', pids=[]))

    def run_sync(self, **kwargs):
        return pins.sync_pins(self.release, self.config, self.uid, state=self.state, **kwargs)

    def assert_pin(self, path, digest):
        self.assertEqual(pins.fields(path.read_bytes())[pins.PIN][0], digest.encode())

    def test_upgrade_rollback_idempotent_preserves_every_other_byte_and_permissions(self):
        before = self.first.read_bytes()
        metadata = self.first.stat()
        self.assertEqual(self.run_sync(), 0)
        self.assertEqual(self.calls, [])
        self.select('v2')
        self.assertEqual(self.run_sync(), 2)
        self.assertEqual(len(self.calls), 6)
        self.assertEqual(self.first.read_bytes(), before.replace(self.old.encode(), self.new.encode()))
        after = self.first.stat()
        self.assertEqual((after.st_uid, after.st_gid, stat.S_IMODE(after.st_mode)),
                         (metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode)))
        self.assertEqual(self.run_sync(), 0)
        self.select('v1')
        self.assertEqual(self.run_sync(), 2)
        self.assertEqual(self.first.read_bytes(), before)
        self.assertEqual(list(self.profiles.glob('.runner-pin-*')), [])

    def test_interruption_after_first_write_retries_without_running_consumers(self):
        self.select('v2')
        completed = []
        def interrupt(path, data, metadata):
            if completed:
                raise OSError('simulated interruption')
            pins.atomic_replace(path, data, metadata)
            completed.append(path)
        with self.assertRaises(OSError):
            self.run_sync(replace=interrupt)
        self.assert_pin(self.first, self.new)
        self.assert_pin(self.second, self.old)
        self.assertEqual(self.run_sync(), 1)
        self.assert_pin(self.second, self.new)

    def test_failure_after_partial_upgrade_restores_previous_selected_runner(self):
        self.select('v2')
        def interrupt(path, data, metadata):
            pins.atomic_replace(path, data, metadata)
            raise OSError('simulated process death')
        with self.assertRaises(OSError):
            self.run_sync(replace=interrupt)
        self.select('v1')
        self.assertEqual(self.run_sync(), 1)
        self.assert_pin(self.first, self.old)
        self.assert_pin(self.second, self.old)

    def test_active_or_cgroup_child_blocks_all_writes(self):
        self.select('v2')
        before = self.first.read_bytes()
        for changes in [dict(ActiveState='active'), dict(MainPID='42'), dict(pids=['43']),
                        dict(ActiveState='deactivating')]:
            with self.subTest(changes=changes):
                self.states['ai-image-factory-provider-pollerd@managed.videos.service'] = dict(
                    dict(ActiveState='inactive', MainPID='0', pids=[]), **changes)
                with self.assertRaisesRegex(ValueError, 'fully stopped'):
                    self.run_sync()
                self.assertEqual(self.first.read_bytes(), before)

    def test_check_is_read_only_and_current_pins_allow_active_consumers(self):
        self.states['ai-image-factory-provider-submitd@managed.images.service'] = dict(
            ActiveState='active', MainPID='42', pids=['42'])
        self.assertEqual(self.run_sync(check=True), 0)
        self.assertEqual(self.run_sync(), 0)
        self.select('v2')
        with self.assertRaisesRegex(ValueError, 'does not match'):
            self.run_sync(check=True)
        self.assert_pin(self.first, self.old)

    def test_custom_runner_and_no_managed_profiles_need_no_manifest(self):
        for path in (self.first, self.second):
            path.unlink()
        custom = self.add_profile('custom', self.old, runner='/site/custom-runner')
        before = custom.read_bytes()
        self.manifest('v1').unlink()
        self.assertEqual(self.run_sync(), 0)
        self.assertEqual(custom.read_bytes(), before)
        self.assertEqual(self.calls, [])

    def test_all_profiles_validated_before_any_write_and_duplicates_rejected(self):
        self.select('v2')
        for suffix in (f'{pins.PIN}={self.old}\n',
                       f'{pins.RUNNER}=/site/other\n'):
            with self.subTest(suffix=suffix):
                self.add_profile('managed.videos', self.old)
                self.second.write_bytes(self.second.read_bytes() + suffix.encode())
                with self.assertRaisesRegex(ValueError, 'duplicate'):
                    self.run_sync()
                self.assert_pin(self.first, self.old)

    def test_invalid_pin_is_rejected_without_secret_in_error(self):
        self.first.write_bytes(self.first.read_bytes().replace(self.old.encode(), b'never-print-this'))
        with self.assertRaisesRegex(ValueError, 'valid SHA256') as failure:
            self.run_sync()
        self.assertNotIn('never-print-this', str(failure.exception))

    def test_runner_and_manifest_tampering_fail_before_write(self):
        self.select('v2')
        original = self.manifest().read_bytes()
        mutations = [lambda m: m.update(commit_sha='b' * 40),
                     lambda m: m.update(release_version='v3'),
                     lambda m: m['files'][0].update(sha256='0' * 64),
                     lambda m: m['files'][0].update(bytes=1),
                     lambda m: m['files'][0].update(mode=0o644),
                     lambda m: m['files'].append(m['files'][0])]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                manifest = json.loads(original)
                mutate(manifest)
                self.manifest().write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    self.run_sync()
                self.assert_pin(self.first, self.old)
        self.manifest().write_bytes(original)
        runner = self.release / 'releases/v2/bin/remote-submit-runner'
        runner.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'mode mismatch'):
            self.run_sync()

    def test_symlinks_writable_paths_and_wrong_owner_are_rejected(self):
        self.select('v2')
        locations = [self.first, self.manifest(), self.release / 'releases/v2/bin/remote-submit-runner']
        for path in locations:
            with self.subTest(path=path):
                saved = path.with_suffix('.saved')
                path.rename(saved)
                path.symlink_to(saved)
                with self.assertRaises(ValueError):
                    self.run_sync()
                path.unlink()
                saved.rename(path)
                previous = stat.S_IMODE(path.stat().st_mode)
                path.chmod(previous | 0o020)
                with self.assertRaises(ValueError):
                    self.run_sync()
                path.chmod(previous)
        with self.assertRaisesRegex(ValueError, 'unprotected'):
            pins.sync_pins(self.release, self.config, self.uid + 1, state=self.state)
        self.assert_pin(self.first, self.old)

    def test_current_cannot_escape_or_follow_nested_symlink(self):
        current = self.release / 'current'
        current.unlink()
        current.symlink_to(self.root)
        with self.assertRaisesRegex(ValueError, 'outside'):
            self.run_sync()
        self.select('v2')
        directory = self.release / 'releases/v2/bin'
        directory.rename(directory.with_name('saved-bin'))
        directory.symlink_to(directory.with_name('saved-bin'), target_is_directory=True)
        with self.assertRaises(ValueError):
            self.run_sync()

    def test_pin_comment_and_no_final_newline_are_preserved(self):
        self.select('v2')
        comment = f'# {pins.PIN}={self.old}\n'.encode()
        content = comment + f'{pins.RUNNER}={self.release}/current/bin/remote-submit-runner\n'.encode()
        content += f'{pins.PIN}={self.old}'.encode()
        self.first.write_bytes(content)
        self.run_sync()
        self.assertEqual(self.first.read_bytes(), content[:-64] + self.new.encode())

    def test_disabled_legacy_defaults_do_not_block_profiles_or_mutate_app_env(self):
        app = self.config / 'app.env'
        app.write_bytes(self.first.read_bytes())
        before = app.read_bytes()
        self.select('v2')
        self.assertEqual(self.run_sync(), 2)
        self.assertEqual(app.read_bytes(), before)

    def test_enabled_legacy_requires_migration_only_if_pin_changes(self):
        app = self.config / 'app.env'
        app.write_bytes(self.first.read_bytes())
        self.states['ai-image-factory-provider-submitd.service'] = dict(
            ActiveState='inactive', MainPID='0', UnitFileState='enabled', pids=[])
        self.assertEqual(self.run_sync(), 0)
        self.select('v2')
        with self.assertRaisesRegex(ValueError, 'operator migration'):
            self.run_sync()
        self.assert_pin(self.first, self.old)

    def test_configuration_change_before_replace_is_detected(self):
        metadata = self.first.stat()
        self.first.write_bytes(self.first.read_bytes() + b'new-value\n')
        with self.assertRaisesRegex(ValueError, 'changed before'):
            pins.atomic_replace(self.first, b'replacement', metadata)
        self.assertIn(b'new-value', self.first.read_bytes())

    def test_systemd_state_reads_descendant_cgroups_and_rejects_failed_queries(self):
        output = ('ActiveState=inactive\nMainPID=0\nControlGroup=/unit\n'
                  'UnitFileState=enabled\nLoadState=loaded\n')
        with mock.patch.object(pins.subprocess, 'run', return_value=SimpleNamespace(
                returncode=0, stdout=output)), \
                mock.patch.object(Path, 'exists', return_value=True), \
                mock.patch.object(pins.os, 'walk', return_value=[
                    ('/sys/fs/cgroup/unit', ['child'], ['cgroup.procs']),
                    ('/sys/fs/cgroup/unit/child', [], ['cgroup.procs'])]), \
                mock.patch.object(Path, 'read_text', side_effect=['', '77\n']):
            self.assertEqual(pins.systemd_state('safe.service')['pids'], ['77'])
        with mock.patch.object(pins.subprocess, 'run', return_value=SimpleNamespace(
                returncode=1, stdout='')):
            with self.assertRaisesRegex(ValueError, 'cannot inspect'):
                pins.systemd_state('safe.service')

    def test_cli_does_not_echo_source_values_or_os_errors(self):
        for failure in (ValueError('never-print-this'), OSError('never-print-this'),
                        json.JSONDecodeError('never-print-this', 'secret', 0)):
            with self.subTest(failure=failure), \
                    mock.patch.object(pins.os, 'geteuid', return_value=0), \
                    mock.patch.object(pins, 'protected'), \
                    mock.patch.object(pins, 'sync_pins', side_effect=failure), \
                    mock.patch.object(pins.sys, 'argv', ['sync-remote-runner-pins', '--check']):
                output = io.StringIO()
                with contextlib.redirect_stderr(output):
                    self.assertEqual(pins.main(), 1)
                self.assertNotIn('never-print-this', output.getvalue())

    def test_empty_unmanaged_profile_and_disabled_legacy_are_ignored(self):
        self.first.unlink()
        self.second.unlink()
        (self.profiles / 'unmanaged.env').touch()
        (self.config / 'app.env').write_text(f'{pins.PIN}=legacy-placeholder\n')
        self.manifest('v1').unlink()
        self.assertEqual(self.run_sync(), 0)

    def test_manifest_atomic_publication_hardlink_can_survive_interruption(self):
        self.select('v2')
        os.link(self.manifest(), self.manifest().with_suffix('.publish-tmp'))
        self.assertEqual(self.manifest().stat().st_nlink, 2)
        self.assertEqual(self.run_sync(), 2)
        self.assertEqual(self.run_sync(check=True), 0)

    def test_hyphenated_profile_queries_the_escaped_systemd_instance(self):
        profile = self.add_profile('managed.image-edit', self.old)
        self.select('v2')
        escaped = r'ai-image-factory-provider-submitd@managed.image\x2dedit.service'
        self.states[escaped] = dict(ActiveState='active', MainPID='42', pids=['42'])
        with self.assertRaisesRegex(ValueError, 'fully stopped'):
            self.run_sync()
        self.assertIn(escaped, self.calls)
        self.assert_pin(profile, self.old)
        self.assert_pin(self.first, self.old)

    def test_leading_dot_profile_queries_the_escaped_systemd_instance(self):
        profile = self.add_profile('.managed', self.old)
        self.select('v2')
        escaped = r'ai-image-factory-provider-submitd@\x2emanaged.service'
        self.states[escaped] = dict(ActiveState='inactive', MainPID='0', pids=['42'])
        with self.assertRaisesRegex(ValueError, 'fully stopped'):
            self.run_sync()
        self.assertIn(escaped, self.calls)
        self.assert_pin(profile, self.old)

    def test_inherited_runner_and_explicit_pin_upgrade_without_rewriting_app_env(self):
        app = self.config / 'app.env'
        app.write_text(f'{pins.RUNNER}={self.release}/current/bin/remote-submit-runner\n')
        self.first.write_bytes(b'\n'.join(line for line in self.first.read_bytes().split(b'\n')
                                       if not line.startswith(pins.RUNNER.encode() + b'=')))
        app_before = app.read_bytes()
        self.assertEqual(self.run_sync(check=True), 0)
        self.select('v2')
        self.assertEqual(self.run_sync(), 2)
        self.assert_pin(self.first, self.new)
        self.assertNotIn(pins.RUNNER, pins.fields(self.first.read_bytes()))
        self.assertEqual(app.read_bytes(), app_before)

    def test_inherited_pin_is_overridden_in_profile_and_rollback_repairs_it(self):
        app = self.config / 'app.env'
        app.write_bytes(self.first.read_bytes())
        app_before = app.read_bytes()
        # One profile inherits both fields, the other inherits only the pin.
        self.first.write_bytes(b'SECRET_VALUE=never-print-this')
        self.second.write_text(f'{pins.RUNNER}={self.release}/current/bin/remote-submit-runner\n')
        self.assertEqual(self.run_sync(check=True), 0)
        self.select('v2')
        self.assertEqual(self.run_sync(), 2)
        self.assertEqual(self.first.read_bytes(), b'SECRET_VALUE=never-print-this\n' +
                         f'{pins.PIN}={self.new}\n'.encode())
        self.assert_pin(self.second, self.new)
        self.assertEqual(app.read_bytes(), app_before)
        self.select('v1')
        self.assertEqual(self.run_sync(), 2)
        self.assert_pin(self.first, self.old)
        self.assert_pin(self.second, self.old)
        self.assertEqual(app.read_bytes(), app_before)

    def test_external_override_ignores_app_canonical_runner_and_pin(self):
        app = self.config / 'app.env'
        app.write_bytes(self.first.read_bytes())
        for path in (self.first, self.second):
            path.write_text(f'{pins.RUNNER}=/site/custom-runner\n')
        before = self.first.read_bytes()
        self.manifest('v1').unlink()
        self.assertEqual(self.run_sync(), 0)
        self.assertEqual(self.first.read_bytes(), before)

    def test_missing_inherited_pin_rejects_preflight_before_any_write(self):
        app = self.config / 'app.env'
        app.write_text(f'{pins.RUNNER}={self.release}/current/bin/remote-submit-runner\n')
        self.first.write_bytes(b'SECRET_VALUE=never-print-this\n')
        self.select('v2')
        with self.assertRaisesRegex(ValueError, 'valid SHA256'):
            self.run_sync(check=True)
        self.assert_pin(self.second, self.old)


if __name__ == '__main__':
    unittest.main()
