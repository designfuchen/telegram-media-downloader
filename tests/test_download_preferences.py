import copy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import yaml
from module import web

class DownloadPreferencesTests(unittest.TestCase):
    def setUp(self):
        self.config=yaml.safe_load((Path(__file__).resolve().parents[1]/'config.example.yaml').read_text())
        self.app=SimpleNamespace(config=copy.deepcopy(self.config),telegram_ready=True,restart_required=False)

    def test_filters_apply_without_disconnect_or_changing_credentials(self):
        with patch.dict(web._flask_app.config,LOGIN_DISABLED=True),patch.object(web,'web_application',self.app),patch.object(web,'_read_config',side_effect=lambda:copy.deepcopy(self.config)),patch.object(web,'_write_config') as write:
            response=web._flask_app.test_client().post('/api/download/preferences',json={'media_types':['photo','video'],'file_formats':{'video':'mp4,mkv','audio':'all','document':'pdf'},'enable_download_txt':True})
            self.assertEqual(response.status_code,200,response.json)
            saved=write.call_args.args[0]
            self.assertEqual(saved['file_formats']['video'],['mp4','mkv'])
            self.assertEqual(self.app.media_types,['photo','video'])
            self.assertTrue(self.app.enable_download_txt)
            self.assertFalse(self.app.restart_required)
            self.assertTrue(self.app.telegram_ready)
            self.assertEqual(saved['api_hash'],self.config['api_hash'])
            self.assertEqual(saved['api_id'],self.config['api_id'])

    def test_text_only_is_valid_and_keeps_login(self):
        with patch.dict(web._flask_app.config,LOGIN_DISABLED=True),patch.object(web,'web_application',self.app),patch.object(web,'_read_config',return_value=self.config),patch.object(web,'_write_config') as write:
            response=web._flask_app.test_client().post('/api/download/preferences',json={'media_types':[],'enable_download_txt':True})
            self.assertEqual(response.status_code,200,response.json)
            self.assertEqual(write.call_args.args[0]['media_types'],[])
            self.assertTrue(self.app.enable_download_txt)
            self.assertTrue(self.app.telegram_ready)

    def test_empty_media_and_unrelated_fields_do_not_save(self):
        with patch.dict(web._flask_app.config,LOGIN_DISABLED=True),patch.object(web,'web_application',self.app),patch.object(web,'_read_config',return_value=self.config),patch.object(web,'_write_config') as write:
            for payload in [{'media_types':[]},{'api_hash':'bad'}]:
                self.assertEqual(web._flask_app.test_client().post('/api/download/preferences',json=payload).status_code,400)
            write.assert_not_called()

    def test_concurrency_requires_reconnect_without_logout(self):
        with patch.dict(web._flask_app.config,LOGIN_DISABLED=True),patch.object(web,'web_application',self.app),patch.object(web,'_read_config',return_value=self.config),patch.object(web,'_write_config'):
            response=web._flask_app.test_client().post('/api/download/preferences',json={'max_download_task':7})
            self.assertEqual(response.status_code,200,response.json)
            self.assertTrue(self.app.restart_required)
            self.assertTrue(self.app.telegram_ready)
