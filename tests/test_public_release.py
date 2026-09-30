"""Release integration coverage with temporary data and no Telegram account."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from module import web

ROOT = Path(__file__).resolve().parent.parent


class UnifiedConsoleTests(unittest.TestCase):
    def setUp(self):
        self.login = web._flask_app.config.get('LOGIN_DISABLED', False)
        web._flask_app.config['LOGIN_DISABLED'] = True
        self.client = web._flask_app.test_client()

    def tearDown(self):
        web._flask_app.config['LOGIN_DISABLED'] = self.login

    def test_legacy_batch_bookmark_opens_unified_intake_without_modifying_queue(self):
        with mock.patch.object(web.batch_queue, 'dispatch_next') as dispatch:
            response = self.client.get('/batch')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/?view=imports')
        dispatch.assert_not_called()
        page = self.client.get(response.location).get_data(as_text=True)
        self.assertIn('id="panel-imports"', page)
        self.assertIn('id="batchTitle"', page)
        self.assertIn('id="import_csv_open"', page)
        self.assertIn('id="batch_chat_dialog"', page)
        self.assertEqual(page.count('id="panel-imports"'), 1)

    def test_batch_bookmark_keeps_authentication(self):
        web._flask_app.config['LOGIN_DISABLED'] = False
        response = self.client.get('/batch')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login?', response.location)

    def test_current_batch_reads_existing_api_and_dynamic_batch_size(self):
        expected = {'number': 2, 'batch_size': 10, 'records': [{'order_no': 11}]}
        with mock.patch.object(web.batch_queue, 'current_batch', return_value=expected.copy()):
            response = self.client.get('/api/batch/current')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['batch']['batch_size'], 10)
        self.assertEqual(response.get_json()['records'], [{'order_no': 11}])


class PrivateConfigTests(unittest.TestCase):
    def test_initialization_generates_unique_private_password_and_never_overwrites(self):
        import yaml
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root/'scripts').mkdir()
            shutil.copyfile(ROOT/'scripts/init_config.py', root/'scripts/init_config.py')
            shutil.copyfile(ROOT/'config.example.yaml', root/'config.example.yaml')
            result = subprocess.run([sys.executable, str(root/'scripts/init_config.py'), '--docker'], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            data = (root/'config.yaml').read_bytes()
            cfg = yaml.safe_load(data)
            self.assertNotEqual(cfg['web_login_secret'], 'CHANGE_ME')
            self.assertGreaterEqual(len(cfg['web_login_secret']), 32)
            self.assertNotIn(cfg['web_login_secret'], result.stdout + result.stderr)
            self.assertEqual(cfg['web_host'], '0.0.0.0')
            self.assertEqual(cfg['save_path'], '/app/downloads')
            if os.name == 'posix':
                self.assertEqual((root/'config.yaml').stat().st_mode & 0o777, 0o600)
            retry = subprocess.run([sys.executable, str(root/'scripts/init_config.py')], capture_output=True)
            self.assertNotEqual(retry.returncode, 0)
            self.assertEqual((root/'config.yaml').read_bytes(), data)
