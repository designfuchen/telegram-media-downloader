"""Account changes must preserve files and never reuse another account's state."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from module.account_management import AccountLogout, account_identity, prepare_profile
from module import web

PROFILE = '11111111-2222-4333-8444-555555555555'

class ProfileTests(unittest.TestCase):
    def test_inherits_preferences_but_never_sessions_channels_or_other_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'sessions').mkdir()
            (root/'sessions/keep.session').write_bytes(b'original-session')
            config = {'api_id':12345, 'api_hash':'0'*32, 'media_types':['photo'],
                      'proxy':{'hostname':'localhost'}, 'save_path':str(root/'downloads'),
                      'chat': ['private-channel'], 'web_login_secret':'do-not-inherit',
                      'bot_token':'do-not-inherit', 'nas_host':'do-not-inherit', 'start_paused':False}
            prepare_profile(root, PROFILE, config)
            target = root/'accounts'/PROFILE
            data = json.loads((target/'bootstrap.json').read_text())
            self.assertEqual(data['api_id'],12345)
            self.assertEqual(data['media_types'],['photo'])
            self.assertTrue(data['start_paused'])
            self.assertTrue(data['save_path'].endswith('/Account-11111111'))
            self.assertFalse({'chat','web_login_secret','bot_token','nas_host'} & data.keys())
            self.assertFalse((target/'sessions').exists())
            self.assertFalse((target/'dbdata').exists())
            self.assertEqual((target/'bootstrap.json').stat().st_mode & 0o777,0o600)
            self.assertEqual((root/'sessions/keep.session').read_bytes(),b'original-session')
            with self.assertRaises(FileExistsError):prepare_profile(root, PROFILE, config)
            self.assertEqual(json.loads((target/'bootstrap.json').read_text()),data)

    def test_rejects_paths_and_symlinked_account_directory(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as elsewhere:
            with self.assertRaises(ValueError):prepare_profile(directory,'../../elsewhere',{})
            Path(directory,'accounts').symlink_to(elsewhere, target_is_directory=True)
            with self.assertRaises(ValueError):prepare_profile(directory,PROFILE,{})
            self.assertEqual(list(Path(elsewhere).iterdir()),[])

    def test_identity_masks_phone_and_does_not_include_credentials(self):
        result=account_identity(SimpleNamespace(id=42,first_name='Sample',last_name='Account',username='example',phone_number='1234567890'))
        self.assertEqual(result['display_name'],'Sample Account')
        self.assertEqual(result['phone_hint'],'•••• 7890')
        self.assertNotIn('1234567890', json.dumps(result))
        self.assertEqual(set(result),{'id','display_name','username','phone_hint'})

class LogoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_workers_stop_before_remote_logout_and_identity_clears(self):
        events=[]
        async def worker():
            try:await asyncio.Event().wait()
            finally:events.append('worker-stopped')
        async def log_out():
            self.assertEqual(events,['worker-stopped'])
            self.assertFalse(app.telegram_ready)
            events.append('logout')
        app=SimpleNamespace(telegram_ready=True,telegram_account={'id':'42'})
        task=asyncio.create_task(worker())
        await asyncio.sleep(0)
        client=SimpleNamespace(log_out=AsyncMock(side_effect=log_out))
        controller=AccountLogout(app,client,[task])
        await asyncio.gather(controller.run(),controller.run())
        self.assertIsNone(app.telegram_account)
        self.assertFalse(app.account_logging_out)
        client.log_out.assert_awaited_once()

    async def test_network_failure_preserves_identity_and_allows_retry(self):
        app=SimpleNamespace(telegram_ready=True,telegram_account={'id':'42'})
        client=SimpleNamespace(log_out=AsyncMock(side_effect=OSError('unreachable')))
        controller=AccountLogout(app,client,[])
        with self.assertRaises(OSError):await controller.run()
        self.assertEqual(app.telegram_account,{'id':'42'})
        self.assertFalse(app.telegram_ready)
        self.assertFalse(app.account_logging_out)
        client.log_out.side_effect=None
        await controller.run()
        self.assertIsNone(app.telegram_account)

class AccountAPITests(unittest.TestCase):
    def setUp(self):
        self.auth=patch.dict(web._flask_app.config,LOGIN_DISABLED=True)
        self.auth.start()
        self.client=web._flask_app.test_client()
    def tearDown(self):self.auth.stop()

    def test_account_requires_console_authentication(self):
        with patch.dict(web._flask_app.config,LOGIN_DISABLED=False):
            self.assertEqual(self.client.get('/api/account').status_code,302)
            self.assertEqual(self.client.post('/api/account/logout',json={}).status_code,302)

    def test_profile_creation_is_desktop_local_only(self):
        with patch.dict(os.environ,{'TMD_DESKTOP':'0'}):
            response=self.client.post('/api/account/profile',json={'id':PROFILE})
            self.assertEqual(response.status_code,409)
        with patch.dict(os.environ,{'TMD_DESKTOP':'1','TMD_DESKTOP_ROOT_DIR':'/unused'}):
            response=self.client.post('/api/account/profile',json={'id':PROFILE},base_url='https://localhost',environ_overrides={'REMOTE_ADDR':'192.0.2.1'})
            self.assertEqual(response.status_code,409)

    def test_logout_unavailable_never_changes_downloads(self):
        with patch.object(web,'web_application',SimpleNamespace()),patch.object(web,'set_download_state') as change,patch.dict(os.environ,{'TMD_DESKTOP':'1'}):
            self.assertEqual(self.client.post('/api/account/logout',json={}).status_code,409)
            change.assert_not_called()

    def test_logged_in_identity_returned_from_memory(self):
        with patch.object(web,'web_application',SimpleNamespace(telegram_ready=True,telegram_account={'display_name':'Example'})):
            response=self.client.get('/api/account')
            self.assertTrue(response.json['signed_in'])
            self.assertEqual(response.json['account']['display_name'],'Example')

    def test_logout_route_pauses_before_revocation_and_reports_completion(self):
        loop=asyncio.new_event_loop()
        thread=threading.Thread(target=loop.run_forever)
        thread.start()
        controller=SimpleNamespace(run=AsyncMock())
        app=SimpleNamespace(account_logout=controller,loop=loop)
        try:
            with patch.object(web,'web_application',app),patch.object(web,'set_download_state') as pause,patch.object(web,'_persist_download_state') as persist,patch.dict(os.environ,{'TMD_DESKTOP':'1'}):
                response=self.client.post('/api/account/logout',json={})
                self.assertEqual(response.status_code,200)
                self.assertTrue(response.json['ok'])
                pause.assert_called_once_with(web.DownloadState.StopDownload)
                persist.assert_called_once_with(True)
                controller.run.assert_awaited_once()
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join()
            loop.close()
