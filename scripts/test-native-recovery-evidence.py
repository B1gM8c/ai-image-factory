"""Local contracts for bounded native-recovery failure evidence."""
import ast
import importlib.util
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / 'scripts/test-native-recovery.py'
SPEC = importlib.util.spec_from_file_location('native_recovery_harness', HARNESS)
HARNESS_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HARNESS_MODULE)


class NativeRecoveryEvidenceTests(unittest.TestCase):
    def test_early_apply_failure_keeps_original_and_recovery_causes(self):
        command_id = '6f4ec29d-a3d7-4e93-93f5-c54574ef06da'
        events = [
            {'command_id': command_id, 'phase': 'recovery_ready', 'details': {'outcome': 'succeeded'}},
            {'command_id': command_id, 'phase': 'restoring',
             'details': {'error': 'factoryctl failed before candidate verify'}},
            {'command_id': command_id, 'phase': 'restore_required',
             'details': {'update_error': 'factoryctl failed before candidate verify',
                         'recovery_error': 'security fingerprint mismatch'}},
        ]
        result = HARNESS_MODULE.failure_event_summary(events, command_id)
        self.assertEqual(result['original_apply'], {
            'phase': 'recovery_ready', 'phase_basis': 'last_journal_phase_before_recovery',
            'message': 'factoryctl failed before candidate verify'})
        self.assertEqual(result['recover'], {
            'phase': 'restore_required', 'message': 'security fingerprint mismatch'})

    def test_real_early_recovery_path_has_no_impossible_fault_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory)
            (fixture / 'verify-runs.jsonl').write_text(json.dumps({
                'release': 'baseline', 'scope': None, 'exit_code': 0,
                'reader_denial_42501': True}) + '\n')
            markers = HARNESS_MODULE.fault_markers(fixture)
        self.assertFalse(markers['first_recovery_fault_observed'])
        self.assertFalse(markers['post_validation_fault_observed'])
        self.assertFalse(markers['candidate_validation_verify_seen'])

    def test_missing_journal_still_exports_empty_allowlisted_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'output'
            output.mkdir()
            HARNESS_MODULE.write(output / 'summary.json', json.dumps({
                'passed': False, 'error': 'original apply failure'}))
            with mock.patch.object(HARNESS_MODULE, 'STATE', root / 'missing-state'):
                HARNESS_MODULE.collect_recovery_evidence(output, {}, RuntimeError('original apply failure'))
            self.assertEqual((output / 'updater-events.jsonl').read_text(), '')
            summary = json.loads((output / 'summary.json').read_text())
        self.assertEqual(summary['error'], 'original apply failure')
        self.assertFalse(summary['failure_evidence']['updater_events']['present'])

    def test_malformed_journal_and_verify_runs_are_bounded_not_fatal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            journal = root / 'updater/events.jsonl'
            journal.parent.mkdir()
            journal.write_text('not-json\n' + json.dumps({
                'command_id': 'id', 'phase': 'staged', 'details': {}}) + '\n')
            fixture = root / 'fixture'
            fixture.mkdir()
            (fixture / 'verify-runs.jsonl').write_text('not-json\n')
            events, metadata = HARNESS_MODULE.bounded_updater_events(journal)
            markers = HARNESS_MODULE.fault_markers(fixture)
        self.assertEqual(metadata['invalid_lines'], 1)
        self.assertEqual([event['phase'] for event in events], ['staged'])
        self.assertEqual(markers['verify_runs'], [])
        self.assertFalse(markers['candidate_validation_verify_seen'])

    def test_missing_verify_runs_is_an_empty_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            markers = HARNESS_MODULE.fault_markers(Path(directory))
        self.assertEqual(markers['verify_runs'], [])
        self.assertFalse(markers['candidate_validation_verify_seen'])

    def test_database_down_does_not_hide_event_causes_or_original_error(self):
        command_id = '6f4ec29d-a3d7-4e93-93f5-c54574ef06da'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'output'
            output.mkdir()
            journal = root / 'updater/events.jsonl'
            journal.parent.mkdir()
            journal.write_text('\n'.join(json.dumps(event) for event in (
                {'command_id': command_id, 'phase': 'migrated', 'details': {}},
                {'command_id': command_id, 'phase': 'restoring',
                 'details': {'error': 'candidate verify failed'}},
                {'command_id': command_id, 'phase': 'restore_required',
                 'details': {'recovery_error': 'recovery SQL failed'}},
            )) + '\n')
            HARNESS_MODULE.write(output / 'summary.json', json.dumps({
                'passed': False, 'error': 'original harness assertion'}))
            diagnostics = {'failure_command': command_id, 'owner_environment': {'PGHOST': 'down'},
                           'fixture': root / 'missing-fixture'}
            with mock.patch.object(HARNESS_MODULE, 'STATE', root), \
                    mock.patch.object(HARNESS_MODULE, 'command_snapshot',
                                      side_effect=RuntimeError('database unavailable')):
                HARNESS_MODULE.collect_recovery_evidence(
                    output, diagnostics, RuntimeError('original harness assertion'))
            summary = json.loads((output / 'summary.json').read_text())
        evidence = summary['failure_evidence']
        self.assertEqual(summary['error'], 'original harness assertion')
        self.assertEqual(evidence['command_snapshot_error'], 'database unavailable')
        self.assertEqual(evidence['original_apply'], {
            'phase': 'migrated', 'phase_basis': 'last_journal_phase_before_recovery',
            'message': 'candidate verify failed'})
        self.assertEqual(evidence['recover'], {
            'phase': 'restore_required', 'message': 'recovery SQL failed'})
        self.assertTrue(
            evidence['markers']['automatic_recovery_seen_before_candidate_validation_verify'])
        self.assertFalse(evidence['markers']['first_recovery_fault_observed'])
        self.assertFalse(evidence['markers']['post_validation_fault_observed'])

    def test_bounded_events_sanitize_and_allowlist_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events.jsonl'
            original = list(HARNESS_MODULE.SECRET_VALUES)
            HARNESS_MODULE.SECRET_VALUES[:] = ['', 'fixture-secret']
            try:
                path.write_text(json.dumps({
                    'command_id': 'id', 'phase': 'restoring',
                    'details': {'error': 'fixture-secret postgresql://u:p@127.0.0.1/db'},
                    'private_environment': 'must-not-export'}) + '\n')
                events, metadata = HARNESS_MODULE.bounded_updater_events(path)
            finally:
                HARNESS_MODULE.SECRET_VALUES[:] = original
        rendered = json.dumps(events)
        self.assertNotIn('fixture-secret', rendered)
        self.assertNotIn('postgresql://', rendered)
        self.assertNotIn('private_environment', rendered)
        self.assertEqual(events[0]['details']['error'], '[REDACTED] [REDACTED-DSN]')
        self.assertEqual(metadata['events_exported'], 1)

    def test_export_failure_cannot_replace_original_error(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            HARNESS_MODULE.write(output / 'summary.json', json.dumps({
                'passed': False, 'error': 'original apply failure'}))
            with mock.patch.object(HARNESS_MODULE, 'collect_recovery_evidence',
                                   side_effect=RuntimeError('export failed')):
                result = HARNESS_MODULE.collect_recovery_evidence_best_effort(
                    output, {}, RuntimeError('original apply failure'))
            summary = json.loads((output / 'summary.json').read_text())
        self.assertEqual(result, 'export failed')
        self.assertEqual(summary['error'], 'original apply failure')
        self.assertEqual(summary['evidence_export_error'], 'export failed')

    def test_finally_collects_before_cleanup_and_wait_reads_failure_message(self):
        tree = ast.parse(HARNESS.read_text())
        functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
        main_try = next(node for node in ast.walk(functions['main']) if isinstance(node, ast.Try)
                        and node.finalbody)
        first_call = next(node for node in ast.walk(main_try.finalbody[0]) if isinstance(node, ast.Call))
        self.assertEqual(first_call.func.id, 'collect_recovery_evidence_best_effort')
        wait_source = ast.get_source_segment(HARNESS.read_text(), functions['wait_command'])
        snapshot_source = ast.get_source_segment(HARNESS.read_text(), functions['command_snapshot'])
        execute_source = ast.get_source_segment(HARNESS.read_text(), functions['execute'])
        self.assertIn('command_snapshot', wait_source)
        self.assertIn('failure_message', snapshot_source)
        self.assertIn("write(output / 'updater-events.jsonl'", execute_source)

    def test_workflow_public_artifact_allowlist_is_unchanged(self):
        workflow = (ROOT / '.github/workflows/recovery-rehearsal.yml').read_text()
        match = re.search(r'for name in (.*?)\; do', workflow, re.DOTALL)
        self.assertIsNotNone(match)
        exported = set(re.findall(r'[a-z][a-z0-9-]*\.(?:jsonl|json|log)', match.group(1)))
        self.assertEqual(exported, {
            'summary.json', 'systemd-effective.json', 'host-hook-provenance.json',
            'artifact-equivalence.json', 'initial-security.json', 'restored-security.json',
            'updater-events.jsonl', 'services.log'})


if __name__ == '__main__':
    unittest.main()
