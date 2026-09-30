import asyncio
import time
import os
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
from pyrogram import errors, types
from module.browser_login import BrowserLogin
from module import web

class BrowserLoginTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client=SimpleNamespace(send_code=AsyncMock(return_value=SimpleNamespace(phone_code_hash='temporary')),sign_in=AsyncMock(return_value=types.User(id=1)),check_password=AsyncMock(return_value=types.User(id=1)))
        self.flow=BrowserLogin(self.client)
        self.task=asyncio.create_task(self.flow.authorize())
        await asyncio.sleep(0)
    async def asyncTearDown(self):
        if not self.task.done():self.task.cancel()
        await asyncio.gather(self.task,return_exceptions=True)
    async def test_phone_and_code_login(self):
        self.assertFalse((await self.flow.submit('phone','bad'))['ok'])
        self.assertTrue((await self.flow.submit('phone','+12025550123'))['ok'])
        self.assertEqual(self.flow.stage,'code')
        self.assertTrue((await self.flow.submit('code','12345'))['ok'])
        self.assertIsInstance(await self.task,types.User)
        self.assertIsNone(self.flow.phone)
        self.assertNotIn('12345',str(self.flow.status()))
    async def test_two_factor(self):
        await self.flow.submit('phone','+12025550123')
        self.client.sign_in.side_effect=errors.SessionPasswordNeeded()
        self.assertTrue((await self.flow.submit('code','12345'))['ok'])
        self.assertEqual(self.flow.stage,'password')
        self.assertTrue((await self.flow.submit('password','dummy test password'))['ok'])
    async def test_invalid_code_and_flood_wait_keep_flow(self):
        await self.flow.submit('phone','+12025550123')
        self.client.sign_in.side_effect=errors.PhoneCodeInvalid()
        self.assertFalse((await self.flow.submit('code','12345'))['ok'])
        self.assertEqual(self.flow.stage,'code')
        self.client.sign_in.side_effect=errors.FloodWait(20)
        self.assertFalse((await self.flow.submit('code','12345'))['ok'])
        self.assertGreater(self.flow.status()['retry_after'],0)
        count=self.client.sign_in.call_count
        await self.flow.submit('code','12345')
        self.assertEqual(count,self.client.sign_in.call_count)
    async def test_resend_uses_latest_hash_and_enforces_cooldown(self):
        self.client.resend_code = AsyncMock(return_value=SimpleNamespace(phone_code_hash='renewed', timeout=45))
        await self.flow.submit('phone', '+12025550123')
        self.assertGreater(self.flow.status()['resend_after'], 0)
        self.assertFalse((await self.flow.submit('resend', ''))['ok'])
        self.client.resend_code.assert_not_awaited()
        # Resend cooldown must not prevent entering an already received code.
        self.flow.resend_at = time.monotonic() - 1
        self.assertTrue((await self.flow.submit('resend', ''))['ok'])
        self.client.resend_code.assert_awaited_once_with('+12025550123', 'temporary')
        self.assertEqual(self.flow.code_hash, 'renewed')
        self.assertGreater(self.flow.status()['resend_after'], 0)
        self.assertTrue((await self.flow.submit('code', '12345'))['ok'])
        self.client.sign_in.assert_awaited_once_with('+12025550123', 'renewed', '12345')

    async def test_resend_flood_wait_and_wrong_stage(self):
        self.client.resend_code = AsyncMock(side_effect=errors.FloodWait(90))
        self.assertFalse((await self.flow.submit('resend', ''))['ok'])
        self.client.resend_code.assert_not_awaited()
        await self.flow.submit('phone', '+12025550123')
        self.flow.resend_at = time.monotonic() - 1
        self.assertFalse((await self.flow.submit('resend', ''))['ok'])
        self.assertGreaterEqual(self.flow.status()['resend_after'], 89)
        await self.flow.submit('resend', '')
        self.assertEqual(self.client.resend_code.await_count, 1)

    async def test_unregistered_phone_does_not_accept_terms(self):
        await self.flow.submit('phone','+12025550123')
        self.client.sign_in.return_value=False
        self.assertFalse((await self.flow.submit('code','12345'))['ok'])
        self.assertFalse(self.task.done())

class LocalLaunchTests(unittest.TestCase):
    def test_one_time_local_token_cannot_be_replayed_or_used_remotely(self):
        with patch.dict(web._flask_app.config,SECRET_KEY='test secret',LOGIN_DISABLED=False),patch.dict(os.environ,TMD_LOCAL_LAUNCH_TOKEN='test-token-'+'x'*40):
            client=web._flask_app.test_client()
            payload={'token':os.environ['TMD_LOCAL_LAUNCH_TOKEN']}
            self.assertEqual(client.post('/api/local-launch',json=payload,base_url='https://localhost',environ_base={'REMOTE_ADDR':'192.0.2.1'}).status_code,403)
            self.assertEqual(client.post('/api/local-launch',json=payload,headers={'Origin':'https://evil.example'}).status_code,403)
            self.assertEqual(client.post('/api/local-launch',json=payload).status_code,200)
            self.assertEqual(client.post('/api/local-launch',json=payload).status_code,403)
    def test_login_endpoint_requires_authenticated_console(self):
        with patch.dict(web._flask_app.config,LOGIN_DISABLED=False):
            self.assertIn(web._flask_app.test_client().post('/api/setup/login',json={'action':'phone','value':'+12025550123'}).status_code,(302,401))
