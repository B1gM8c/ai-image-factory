"""Exercise the installer's actual manifest-store block without root or a network."""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


INSTALLER = Path(__file__).resolve().parents[1] / 'deploy/install-release'
SOURCE = INSTALLER.read_text()
BLOCK = SOURCE.split('# Verified manifest store. Keep the original bytes outside the signed release tree.\n', 1)[1]
BLOCK = BLOCK.split('\nPY\ninstall -m 0755', 1)[0]
NAMESPACE = {'__name__': 'installer_manifest_test'}
exec(compile(BLOCK, str(INSTALLER), 'exec'), NAMESPACE)
STORE = NAMESPACE['store_verified_manifest']


class InstallerManifestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name).resolve()
        self.root = base / 'installation'
        self.root.mkdir(mode=0o700)
        self.source = base / 'verified.manifest.json'
        self.version = 'v0.1.0-fixture'
        self.target = 'x86_64-unknown-linux-gnu'
        # Preserve arbitrary original formatting and historical manifest fields.
        self.data = ('{\n "release_version": "' + self.version + '",\n'
                     ' "target_triple": "' + self.target + '", "schema_version": 1\n}\n').encode()
        self.source.write_bytes(self.data)
        self.source.chmod(0o600)
        self.directory = self.root / 'verified-manifests'
        self.destination = self.directory / f'{self.version}-{self.target}.manifest.json'

    def store(self, **changes):
        arguments = dict(install_root=self.root, manifest_path=self.source,
                         version=self.version, target=self.target, expected_uid=os.geteuid())
        arguments.update(changes)
        STORE(**arguments)

    def test_store_runs_after_tree_validation_and_before_activation(self):
        validation = SOURCE.index('existing release paths do not match the manifest')
        publication = SOURCE.index('# Verified manifest store.')
        activation = SOURCE.index('install -m 0755 "${RELEASE_ROOT}/bin/updated"')
        self.assertLess(validation, publication)
        self.assertLess(publication, activation)
        self.assertNotIn('expected_uid=', BLOCK.split('if __name__ == "__main__":', 1)[1])

    def test_new_manifest_preserves_original_bytes_and_private_mode(self):
        self.store()
        self.assertEqual(self.destination.read_bytes(), self.data)
        self.assertEqual(stat.S_IMODE(self.destination.stat().st_mode), 0o600)
        self.assertEqual(self.destination.stat().st_uid, os.geteuid())
        self.assertEqual(self.destination.stat().st_nlink, 1)
        self.assertEqual(list(self.directory.iterdir()), [self.destination])

    def test_same_bytes_are_idempotent_without_replacing_the_file(self):
        self.store()
        identity = self.destination.stat().st_ino
        self.store()
        self.assertEqual(self.destination.stat().st_ino, identity)
        self.assertEqual(self.destination.read_bytes(), self.data)

    def test_conflicting_original_bytes_are_rejected_without_replacing(self):
        self.store()
        changed = json.loads(self.data)
        self.source.write_text(json.dumps(changed))
        with self.assertRaisesRegex(SystemExit, 'conflicts'):
            self.store()
        self.assertEqual(self.destination.read_bytes(), self.data)

    def test_symlinks_are_rejected_for_source_store_and_saved_manifest(self):
        outside = self.root.parent / 'outside'
        outside.mkdir(mode=0o700)
        self.directory.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(OSError):
            self.store()
        self.assertEqual(list(outside.iterdir()), [])
        self.directory.unlink()
        source_link = self.root.parent / 'source-link'
        source_link.symlink_to(self.source)
        with self.assertRaises(OSError):
            self.store(manifest_path=source_link)
        self.directory.mkdir(mode=0o700)
        self.destination.symlink_to(self.source)
        with self.assertRaises(OSError):
            self.store()
        self.assertEqual(self.source.read_bytes(), self.data)

    def test_wrong_owner_or_writable_paths_are_rejected(self):
        with self.assertRaisesRegex(SystemExit, 'root-protected'):
            self.store(expected_uid=os.geteuid() + 1)
        for path, bad_mode in [(self.source, 0o660), (self.root, 0o770)]:
            with self.subTest(path=path):
                original = path.stat().st_mode & 0o777
                path.chmod(bad_mode)
                with self.assertRaisesRegex(SystemExit, 'root-protected'):
                    self.store()
                path.chmod(original)
        self.directory.mkdir(mode=0o770)
        self.directory.chmod(0o770)
        with self.assertRaisesRegex(SystemExit, 'root-protected'):
            self.store()
        self.directory.chmod(0o700)
        self.store()
        self.destination.chmod(0o644)
        with self.assertRaisesRegex(SystemExit, 'root-protected'):
            self.store()

    def test_identity_mismatch_does_not_publish(self):
        for changes in [dict(version='../escape'), dict(version='other'), dict(target='other')]:
            with self.subTest(changes=changes), self.assertRaises(SystemExit):
                self.store(**changes)
        self.assertFalse(self.directory.exists())

    def test_file_is_synced_before_atomic_publication_and_directory_after(self):
        actual_fsync = os.fsync
        actual_link = os.link
        events = []

        def fsync(descriptor):
            kind = 'directory' if stat.S_ISDIR(os.fstat(descriptor).st_mode) else 'file'
            events.append(('fsync', kind))
            actual_fsync(descriptor)

        def link(*arguments, **keywords):
            self.assertIn(('fsync', 'file'), events)
            self.assertFalse(self.destination.exists())
            events.append(('publish', 'file'))
            return actual_link(*arguments, **keywords)

        with patch.object(os, 'fsync', side_effect=fsync), patch.object(os, 'link', side_effect=link):
            self.store()
        self.assertEqual(events, [('fsync', 'directory'), ('fsync', 'file'),
                                  ('publish', 'file'), ('fsync', 'directory')])
        events.clear()
        with patch.object(os, 'fsync', side_effect=fsync):
            self.store()
        self.assertEqual(events, [('fsync', 'directory')])

    def test_file_sync_failure_does_not_publish_or_leave_temporary_files(self):
        actual_fsync = os.fsync

        def fsync(descriptor):
            if stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise OSError('fixture file fsync failed')
            actual_fsync(descriptor)

        with patch.object(os, 'fsync', side_effect=fsync):
            with self.assertRaisesRegex(OSError, 'fixture file fsync failed'):
                self.store()
        self.assertEqual(list(self.directory.iterdir()), [])
        self.store()
        self.assertEqual(self.destination.read_bytes(), self.data)

    def test_directory_sync_failure_after_publication_is_retryable(self):
        actual_fsync = os.fsync

        def fsync(descriptor):
            if stat.S_ISDIR(os.fstat(descriptor).st_mode) and self.destination.exists():
                raise OSError('fixture directory fsync failed')
            actual_fsync(descriptor)

        with patch.object(os, 'fsync', side_effect=fsync):
            with self.assertRaisesRegex(OSError, 'fixture directory fsync failed'):
                self.store()
        self.assertEqual(self.destination.read_bytes(), self.data)
        self.store()
        self.assertEqual(self.destination.stat().st_nlink, 1)

    def test_process_interruption_after_link_publication_is_idempotent(self):
        interrupted = BLOCK.split('if __name__ == "__main__":', 1)[0] + '''
import sys
actual_link = os.link
def interrupted_link(*arguments, **keywords):
    actual_link(*arguments, **keywords)
    os._exit(73)
os.link = interrupted_link
store_verified_manifest(*sys.argv[1:], expected_uid=os.geteuid())
'''
        result = subprocess.run(
            [sys.executable, '-c', interrupted, str(self.root), str(self.source),
             self.version, self.target],
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 73, result.stderr)
        self.assertEqual(self.destination.stat().st_nlink, 2)
        self.store()
        self.assertEqual(self.destination.read_bytes(), self.data)


if __name__ == '__main__':
    unittest.main()
