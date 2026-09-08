"""Pure fixture checks for the read-only deployment gate; no real HTTP or CLI."""
from copy import deepcopy
import importlib.machinery
import importlib.util
from pathlib import Path
import unittest

path = Path(__file__).resolve().parents[1] / 'deploy/hooks/verify-media-segments'
loader = importlib.machinery.SourceFileLoader('media_gate', str(path))
spec = importlib.util.spec_from_loader(loader.name, loader)
gate = importlib.util.module_from_spec(spec)
loader.exec_module(gate)


class RuntimeGateTests(unittest.TestCase):
    def setUp(self):
        self.capabilities = dict.fromkeys(('supports_bbox_sidecar',
            'supports_terminal_source_release', 'supports_analyzer_key_pin'), True)
        self.worker = {'object': 'media.readiness', 'status': 'ready',
            'analyzer_key': 'a' * 64, 'heartbeat_age_ms': 12,
            'last_heartbeat_at_ms': 500000, 'heartbeat_ttl_ms': 150000,
            'source_release_enabled': True}
        self.gateway = {'status': 'ready', 'codex_quota_refresh': {
            'enabled': True, 'running': True, 'healthy': True, 'last_pass_at_ms': 500000}}

    def verify(self, release=True, quota=True):
        gate.validate_views(self.capabilities, self.worker, self.gateway, release, quota)

    def test_enabled_gate(self):
        self.verify()

    def test_source_and_quota_off_rollback(self):
        self.worker['source_release_enabled'] = False
        del self.gateway['codex_quota_refresh']
        self.verify(False, False)

    def test_capability_is_not_worker_liveness(self):
        self.worker['status'] = 'not_ready'
        with self.assertRaises(gate.GateError):
            self.verify()

    def test_stale_missing_future_or_malformed_heartbeat_fails(self):
        for key, value in [('heartbeat_age_ms', 150001), ('heartbeat_age_ms', -1),
            ('heartbeat_age_ms', True), ('last_heartbeat_at_ms', None),
            ('analyzer_key', 'account@example.com')]:
            with self.subTest(key=key, value=value):
                before = deepcopy(self.worker)
                self.worker[key] = value
                with self.assertRaises(gate.GateError):
                    self.verify()
                self.worker = before

    def test_absent_or_dead_quota_loop_fails(self):
        for value in [None, {'enabled': True, 'running': False, 'healthy': False}]:
            self.gateway['codex_quota_refresh'] = value
            with self.assertRaises(gate.GateError):
                self.verify()

    def test_old_protocol_is_rejected(self):
        del self.capabilities['supports_analyzer_key_pin']
        with self.assertRaises(gate.GateError):
            self.verify()

    def test_rollback_cannot_silently_leave_refresh_enabled(self):
        self.worker['source_release_enabled'] = False
        with self.assertRaises(gate.GateError):
            self.verify(False, False)

    def test_redirect_is_never_followed_with_key(self):
        with self.assertRaises(gate.GateError):
            gate.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://example.test')


if __name__ == '__main__':
    unittest.main()
