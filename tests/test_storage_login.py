import copy
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from module import web

class StorageLoginTests(unittest.TestCase):
    def test_folder_change_preserves_login_and_connection(self):
        with TemporaryDirectory() as folder:
            config={'save_path':'/old', 'api_id':123, 'api_hash':'test-placeholder'}
            app=SimpleNamespace(config=copy.deepcopy(config),save_path='/old',telegram_ready=True,restart_required=False)
            with patch.dict(web._flask_app.config,LOGIN_DISABLED=True), patch.object(web,'web_application',app), patch.object(web,'_read_config',return_value=copy.deepcopy(config)), patch.object(web,'_write_config') as write:
                result=web._flask_app.test_client().post('/api/storage/path',json={'path':folder})
                self.assertEqual(result.status_code,200)
                self.assertTrue(app.telegram_ready)
                self.assertFalse(app.restart_required)
                self.assertEqual(app.save_path,folder)
                self.assertEqual(app.config['save_path'],folder)
                saved=write.call_args.args[0]
                self.assertEqual(saved['api_hash'],config['api_hash'])
                self.assertEqual(saved['save_path'],folder)
                invalid=web._flask_app.test_client().post('/api/storage/path',json={'path':str(Path(folder)/'absent')})
                self.assertEqual(invalid.status_code,400)
                self.assertEqual(write.call_count,1)

    def test_restart_needed_is_not_logged_out(self):
        app=SimpleNamespace(telegram_ready=True,restart_required=True)
        with patch.dict(web._flask_app.config,LOGIN_DISABLED=True),patch.object(web,'web_application',app),patch.object(web,'_read_config',return_value={}),patch.object(web,'get_channel_configs',return_value=[]),patch.object(web,'_storage_status',return_value={}):
            result=web._flask_app.test_client().get('/api/setup/status').json
            self.assertTrue(result['telegram_ready'])
            self.assertTrue(result['restart_required'])
            self.assertTrue(web._runtime_blocker())
