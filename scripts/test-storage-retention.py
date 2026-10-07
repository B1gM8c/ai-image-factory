#!/usr/bin/env python3
import importlib.machinery
import json
from pathlib import Path
import tempfile
import unittest

HOOK = Path(__file__).resolve().parents[1] / 'deploy/hooks/retain-storage'
retention = importlib.machinery.SourceFileLoader('retention', str(HOOK)).load_module()


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.releases = self.root / 'releases'
        self.backups = self.root / 'backups'
        self.journal = self.root / 'journal'
        self.proc = self.root / 'proc'
        for path in [self.releases, self.backups, self.journal / 'recovery', self.proc]:
            path.mkdir(parents=True)
        events = []
        for index in range(6):
            version = f'v1.0.{index}'
            path = self.releases / version
            path.mkdir()
            (path / 'release.json').write_text(json.dumps({'release_version': version, 'commit_sha': str(index)}))
            backup = self.backups / f'id{index}-{version}'
            backup.mkdir()
            (backup / 'recovery.env').write_text(f'DATABASE_RECOVERY_FORMAT=2\nUPDATE_COMMAND_ID=id{index}\nRELEASE_VERSION={version}\nRELEASE_COMMIT={index}\n')
            events.append(json.dumps({'action': 'apply', 'phase': 'verified', 'target_version': version,
                                     'command_id': f'id{index}', 'created_at_ms': index * 1000,
                                     'details': {'commit': str(index)}}))
        (self.journal / 'events.jsonl').write_text('\n'.join(events))
        (self.root / 'current').symlink_to(self.releases / 'v1.0.5')
        self.env = dict(AIF_RELEASE_ROOT=str(self.root), AIF_UPDATE_JOURNAL_ROOT=str(self.journal),
                        AIF_BACKUP_ROOT=str(self.backups), AIF_UPDATE_RELEASE_DIR=str(self.releases / 'v1.0.5'),
                        AIF_UPDATE_PREVIOUS_RELEASE=str(self.releases / 'v1.0.4'),
                        AIF_UPDATE_BACKUP_TOKEN=str(self.backups / 'id5-v1.0.5'))

    def run_gc(self, now=1000000, configs=None):
        return retention.retain(self.env, now, self.proc, configs or [])

    def test_retention_idempotent_current_previous_and_three_latest(self):
        self.assertEqual(self.run_gc(), {'removed_releases': 3, 'removed_backups': 3})
        self.assertEqual(sorted(p.name for p in self.releases.iterdir()), ['v1.0.3', 'v1.0.4', 'v1.0.5'])
        self.assertEqual(self.run_gc(), {'removed_releases': 0, 'removed_backups': 0})

    def test_pending_recovery_blocks_all(self):
        (self.journal / 'recovery' / 'pending.json').write_text('{}')
        with self.assertRaises(ValueError):
            self.run_gc()
        self.assertEqual(len(list(self.releases.iterdir())), 6)

    def test_operator_previous_pointer_is_retained(self):
        (self.root / 'previous').symlink_to(self.releases / 'v1.0.0')
        self.assertEqual(self.run_gc(), {'removed_releases': 2, 'removed_backups': 2})
        self.assertTrue((self.root / 'previous').is_dir())
        self.assertTrue((self.backups / 'id0-v1.0.0').is_dir())

    def test_symlink_blocks_plan_before_deletion(self):
        (self.releases / 'v1.0.2' / 'escape').symlink_to(self.root)
        with self.assertRaises(ValueError):
            self.run_gc()
        self.assertEqual(len(list(self.releases.iterdir())), 6)

    def test_grace_period(self):
        self.assertEqual(self.run_gc(now=100), {'removed_releases': 0, 'removed_backups': 0})

    def test_config_reference_protects_release_and_backup(self):
        config = self.root / 'config'
        config.mkdir()
        (config / 'service').write_text(f'ExecStart={self.releases}/v1.0.0/bin/gateway')
        self.assertEqual(self.run_gc(configs=[config]), {'removed_releases': 2, 'removed_backups': 2})
        self.assertTrue((self.releases / 'v1.0.0').exists())

    def test_unknown_backup_is_retained(self):
        (self.backups / 'id0-v1.0.0' / 'recovery.env').write_text('DATABASE_RECOVERY_FORMAT=1')
        self.assertEqual(self.run_gc()['removed_backups'], 2)

    def test_current_mismatch_blocks(self):
        self.env['AIF_UPDATE_RELEASE_DIR'] = str(self.releases / 'v1.0.0')
        with self.assertRaises(ValueError):
            self.run_gc()

    def test_manifest_mismatch_aborts_entire_plan(self):
        (self.releases / 'v1.0.2' / 'release.json').write_text(
            json.dumps({'release_version': 'v1.0.2', 'commit_sha': 'wrong'}))
        with self.assertRaises(ValueError):
            self.run_gc()
        self.assertEqual(len(list(self.releases.iterdir())), 6)
        self.assertEqual(len(list(self.backups.iterdir())), 6)

    def test_unmatched_backup_command_is_retained(self):
        path = self.backups / 'id0-v1.0.0' / 'recovery.env'
        path.write_text(path.read_text().replace('UPDATE_COMMAND_ID=id0', 'UPDATE_COMMAND_ID=wrong'))
        self.assertEqual(self.run_gc()['removed_backups'], 2)
        self.assertTrue(path.exists())

    def test_writable_root_is_rejected(self):
        self.releases.chmod(0o777)
        with self.assertRaises(ValueError):
            self.run_gc()
        self.assertEqual(len(list(self.releases.iterdir())), 6)

    def test_process_mapping_protects(self):
        process = self.proc / '123'
        (process / 'fd').mkdir(parents=True)
        (process / 'exe').symlink_to(self.releases / 'v1.0.1' / 'bin/app')
        (process / 'cwd').symlink_to(self.root)
        (process / 'maps').write_text(f'abc {self.releases}/v1.0.2/lib.so\n')
        self.assertEqual(self.run_gc(), {'removed_releases': 1, 'removed_backups': 1})


if __name__ == '__main__':
    unittest.main()
