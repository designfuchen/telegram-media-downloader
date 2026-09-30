"""Saved settings must not masquerade as a running Telegram client."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from module import web

class SetupStatusTests(unittest.TestCase):
    def setUp(self):
        self.auth = patch.dict(web._flask_app.config, LOGIN_DISABLED=True)
        self.auth.start()
        self.client = web._flask_app.test_client()
    def tearDown(self):
        self.auth.stop()
    def test_preview_never_starts_even_with_ready_flag(self):
        with patch.object(web, 'web_application', SimpleNamespace(preview_mode=True, telegram_ready=True)):
            for url, body in [('/api/channel_state', {'chat_id':'-100123','action':'start'}), ('/set_download_state?state=continue', {})]:
                with patch.object(web, 'set_download_state') as change:
                    response = self.client.post(url, json=body)
                    self.assertEqual(response.status_code,409)
                    self.assertIn('演示', response.json['message'])
                    change.assert_not_called()
            self.assertEqual(self.client.get('/get_download_state').json['state'], 'paused')
    def test_saved_credentials_are_not_login(self):
        with patch.object(web,'web_application',SimpleNamespace()), patch.object(web,'_read_config',return_value={'api_id':123,'api_hash':'a'*32}), patch.object(web,'get_channel_configs',return_value=[{'enabled':True}]), patch.object(web,'_storage_status',return_value={'available':True,'writable':True}):
            result=self.client.get('/api/setup/status').json
            self.assertTrue(result['credentials_saved'])
            self.assertFalse(result['telegram_ready'])
            self.assertEqual(result['channel_count'],1)
            self.assertNotIn('api_hash',result)
    def test_live_client_ready(self):
        with patch.object(web,'web_application',SimpleNamespace(telegram_ready=True)):
            self.assertEqual(web._runtime_blocker(),'')
