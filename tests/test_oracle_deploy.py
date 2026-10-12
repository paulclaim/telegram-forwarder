from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/oracle_deploy.py'
spec = importlib.util.spec_from_file_location('autodeploy', SCRIPT)
autodeploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(autodeploy)


class AutoDeployTests(unittest.TestCase):
    def setUp(self):
        self.work = tempfile.TemporaryDirectory()
        self.addCleanup(self.work.cleanup)
        root = Path(self.work.name)
        self.repo = root / 'repo'
        self.repo.mkdir()
        subprocess.run(['git', 'init', '-b', 'main', str(self.repo)], check=True, capture_output=True)
        self.target = root / 'production'
        self.target.mkdir()
        self.state = root / 'state'
        self.state.mkdir()
        self.config = dict(repo_url=str(self.repo), target=str(self.target),
                           state_dir=str(self.state), service='test.service', health_url='http://unused')
        self.commit('old')
        for name in ('server.py', 'automate.py', 'downloader.py', 'requirements.txt'):
            (self.target / name).write_bytes((self.repo / name).read_bytes())
        (self.target / '.tg-forwarder-root').write_text('telegram-forwarder-pro\n')
        python = self.target / '.venv/bin/python'
        python.parent.mkdir(parents=True)
        python.symlink_to(sys.executable)
        self.protected = ['config.json', 'pairs.json', 'watermarks.json', 'message_map.json',
                          'retry_queue.json', 'run_log.json', 'env', '.env', 'acct.session',
                          '.pairs_from_env.json', 'data_acct1/state.json', 'temp/media',
                          'secrets/key', 'backups/snapshot']
        for name in self.protected:
            path = self.target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('production-only')
        self.actual_run = autodeploy.run
        def safe_run(*args, **kwargs):
            if '-c' in args and args[-1].startswith('import telethon'):
                return ''
            return self.actual_run(*args, **kwargs)
        self.run_patch = patch.object(autodeploy, 'run', side_effect=safe_run)
        self.run_patch.start()
        self.addCleanup(self.run_patch.stop)
        self.status = patch.object(autodeploy, 'service_state', return_value='active')
        self.status.start()
        self.addCleanup(self.status.stop)
        self.actions = patch.object(autodeploy, 'service_action').start()
        self.addCleanup(patch.stopall)
        self.health = patch.object(autodeploy, 'wait_healthy').start()
        autodeploy.save(self.state / 'version.json', {'commit': self.sha(), 'mode': 'deployed'})

    def commit(self, content, invalid=False):
        (self.repo / 'server.py').write_text('broken syntax !' if invalid else f'VERSION = {content!r}\n')
        (self.repo / 'automate.py').write_text('pass\n')
        (self.repo / 'downloader.py').write_text(f'VERSION = {content!r}\n')
        (self.repo / 'requirements.txt').write_text('')
        subprocess.run(['git', '-C', str(self.repo), 'add', '.'], check=True, capture_output=True)
        subprocess.run(['git', '-C', str(self.repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-m', content],
                       check=True, capture_output=True)

    def sha(self):
        return subprocess.check_output(['git', '-C', str(self.repo), 'rev-parse', 'HEAD'], text=True).strip()

    def deploy(self):
        archive = self.state / 'input.tar'
        subprocess.run(['git', '-C', str(self.repo), 'archive', '--format=tar',
                        f'--output={archive}', 'HEAD'], check=True)
        autodeploy.apply_release(self.config, archive, self.sha())

    def assert_protected(self):
        for name in self.protected:
            self.assertEqual((self.target / name).read_text(), 'production-only', name)

    def test_no_commit_does_not_restart_or_modify(self):
        (self.target / 'server.py').write_text('local production change')
        self.deploy()
        self.assertEqual((self.target / 'server.py').read_text(), 'local production change')
        self.actions.assert_not_called()
        self.assert_protected()

    def test_new_commit_protects_runtime_and_removes_obsolete_code(self):
        (self.target / 'obsolete.py').write_text('pass')
        self.commit('new')
        # Even accidentally committed state must not overwrite production state.
        (self.repo / 'watermarks.json').write_text('wrong-state')
        (self.repo / 'acct.session').write_text('wrong-session')
        self.commit('new-with-state')
        self.deploy()
        self.assertIn('new-with-state', (self.target / 'server.py').read_text())
        self.assertIn('new-with-state', (self.target / 'downloader.py').read_text())
        self.assertFalse((self.target / 'obsolete.py').exists())
        self.assert_protected()
        self.assertEqual(self.actions.call_args_list, [unittest.mock.call('stop', 'test.service'),
                                                       unittest.mock.call('start', 'test.service')])
        self.health.assert_called_once()
        self.assertEqual(autodeploy.read_record(self.state)['mode'], 'deployed')

    def test_stopped_service_stays_stopped(self):
        self.commit('new')
        with patch.object(autodeploy, 'service_state', return_value='inactive'):
            self.deploy()
        self.actions.assert_not_called()
        self.health.assert_not_called()
        self.assertFalse(autodeploy.read_record(self.state)['service_running'])

    def test_failed_health_restores_code_and_version(self):
        record = autodeploy.read_record(self.state)
        self.commit('bad-start')
        self.health.side_effect = [RuntimeError('bad health'), None]
        with self.assertRaisesRegex(RuntimeError, 'bad health'):
            self.deploy()
        self.assertIn('old', (self.target / 'server.py').read_text())
        self.assertEqual(autodeploy.read_record(self.state), record)
        self.assert_protected()

    def test_syntax_failure_never_stops_service(self):
        self.commit('syntax-error', invalid=True)
        with self.assertRaises(SyntaxError):
            self.deploy()
        self.actions.assert_not_called()
        self.assertIn('old', (self.target / 'server.py').read_text())

    def test_missing_marker_refuses_deployment(self):
        self.commit('new')
        (self.target / '.tg-forwarder-root').unlink()
        with self.assertRaisesRegex(RuntimeError, 'marker'):
            self.deploy()
        self.actions.assert_not_called()

    def test_dependency_install_failure_does_not_touch_production(self):
        (self.repo / 'requirements.txt').write_text('changed-dependency')
        subprocess.run(['git', '-C', str(self.repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-am', 'dependencies'],
                       check=True, capture_output=True)
        record = autodeploy.read_record(self.state)
        current_side_effect = autodeploy.run.side_effect
        def fail_install(*args, **kwargs):
            if args[:3] == ('python3', '-m', 'venv'):
                raise RuntimeError('dependency preparation failed')
            return current_side_effect(*args, **kwargs)
        autodeploy.run.side_effect = fail_install
        with self.assertRaisesRegex(RuntimeError, 'dependency preparation'):
            self.deploy()
        self.actions.assert_not_called()
        self.assertTrue((self.target / '.venv/bin/python').exists())
        self.assertEqual(autodeploy.read_record(self.state), record)
        self.assert_protected()

    def test_dependency_venv_swap_is_rolled_back_on_bad_health(self):
        (self.repo / 'requirements.txt').write_text('changed-dependency')
        subprocess.run(['git', '-C', str(self.repo), '-c', 'user.name=Test', '-c',
                        'user.email=test@example.invalid', 'commit', '-am', 'dependencies'],
                       check=True, capture_output=True)
        current_side_effect = autodeploy.run.side_effect
        def simulate_venv(*args, **kwargs):
            if args[:3] == ('python3', '-m', 'venv'):
                executable = Path(args[3]) / 'bin/python'
                executable.parent.mkdir(parents=True)
                executable.symlink_to(sys.executable)
                return ''
            if args[0].endswith('/bin/pip'):
                return ''
            return current_side_effect(*args, **kwargs)
        autodeploy.run.side_effect = simulate_venv
        self.health.side_effect = [RuntimeError('bad health'), None]
        with self.assertRaisesRegex(RuntimeError, 'bad health'):
            self.deploy()
        self.assertFalse((self.target / '.venv').is_symlink())
        self.assertTrue((self.target / '.venv/bin/python').exists())
        self.assertEqual((self.target / 'requirements.txt').read_text(), '')
        self.assert_protected()


if __name__ == '__main__':
    unittest.main()
