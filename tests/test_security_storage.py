"""Exercise tamper resistance, legacy migration, key failures and atomic writes."""
import asyncio
import base64
import copy
import io
import logging
import os
from pathlib import Path
import secrets
import tempfile
import unittest
from unittest.mock import patch
import yaml
from pyrogram.storage.file_storage import FileStorage
from module import config_io, secret_storage, web
from module.secure_session import ProtectedSessionStorage
from utils import crypto
from utils.log import redact, register_secrets
from loguru import logger

class VaultTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.environment = patch.dict(os.environ, TMD_SECRET_KEY=base64.b64encode(secrets.token_bytes(32)).decode())
        self.environment.start()
        secret_storage._keys.clear()

    def tearDown(self):
        secret_storage._keys.clear()
        self.environment.stop()
        self.directory.cleanup()

    def test_unicode_config_and_random_nonces(self):
        path = self.root/'config.yaml'
        payload = {'api_hash':'synthetic-secret-中文', 'proxy':{'password':'另一个测试值'}}
        config_io.dump(path, payload, yaml)
        first = path.read_bytes()
        config_io.dump(path, payload, yaml)
        self.assertNotEqual(first, path.read_bytes())
        self.assertNotIn(payload['api_hash'].encode(), first)
        self.assertEqual(config_io.load(path, yaml), payload)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_ruamel_loaded_config_can_be_saved_by_pyyaml_on_startup(self):
        from ruamel.yaml import YAML
        path = self.root/'config.yaml'
        document = YAML().load('api_id: 0\nproxy: {}\napi_min_interval: 1.2\nmedia_types: [photo, video]\nlabel: 中文\n')
        config_io.dump(path, document, yaml)
        self.assertEqual(config_io.load(path, yaml), {
            'api_id': 0, 'proxy': {}, 'api_min_interval': 1.2,
            'media_types': ['photo', 'video'], 'label': '中文'})

    def test_ciphertext_modification_and_wrong_context_are_rejected(self):
        key = secrets.token_bytes(32)
        original = crypto.encrypt('中文 🔒'.encode(), key, b'profile')
        modified = original[:-1] + bytes([original[-1] ^ 1])
        for data, context in [(modified,b'profile'),(original,b'other-profile')]:
            with self.assertRaises(ValueError):crypto.decrypt(data,key,context)
        self.assertEqual(crypto.decrypt(original,key,b'profile').decode(),'中文 🔒')

    def test_atomic_failure_keeps_original_and_cleans_temporary(self):
        path=self.root/'config.yaml'
        path.write_bytes(b'original')
        with patch.object(config_io.os,'replace',side_effect=OSError('simulated disk failure')):
            with self.assertRaises(OSError):config_io.atomic_write(path,b'new')
        self.assertEqual(path.read_bytes(),b'original')
        self.assertEqual(list(self.root.iterdir()),[path])

    def test_missing_key_never_falls_back_to_plaintext(self):
        path=self.root/'config.yaml'
        config_io.dump(path,{'api_hash':'synthetic-sensitive-data'},yaml)
        original=path.read_bytes()
        with patch.dict(os.environ,{},clear=True):
            secret_storage._keys.clear()
            with self.assertRaises(OSError):config_io.load(path,yaml)
        self.assertEqual(path.read_bytes(),original)
        with patch.dict(os.environ,{},clear=True):
            with self.assertRaises(OSError):config_io.dump(path,{},yaml)
        self.assertEqual(path.read_bytes(),original)

    async def test_legacy_session_migration_and_reopen_preserve_authorization(self):
        original=FileStorage('account',self.root)
        await original.open()
        auth=secrets.token_bytes(256)
        await original.dc_id(2)
        await original.api_id(1)
        await original.auth_key(auth)
        await original.user_id(123)
        await original.is_bot(False)
        await original.update_peers([(123,456,'user',['demo'], '12025550123')])
        await original.save()
        await original.close()
        protected=ProtectedSessionStorage('account',self.root)
        await protected.open()
        self.assertEqual(await protected.auth_key(),auth)
        self.assertEqual(await protected.user_id(),123)
        self.assertNotIn(auth,protected.database.read_bytes())
        self.assertNotIn(b'12025550123',protected.database.read_bytes())
        await protected.close()
        secret_storage._keys.clear()
        reopened=ProtectedSessionStorage('account',self.root)
        await reopened.open()
        self.assertEqual(await reopened.dc_id(),2)
        self.assertEqual(await reopened.api_id(),1)
        self.assertEqual(await reopened.auth_key(),auth)
        self.assertEqual((await reopened.get_peer_by_id(123)).user_id,123)
        await reopened.close()

    async def test_corrupt_session_is_not_overwritten(self):
        session=ProtectedSessionStorage('account',self.root)
        await session.open()
        await session.auth_key(secrets.token_bytes(256))
        await session.close()
        original=session.database.read_bytes()
        changed=original[:-1]+bytes([original[-1]^1])
        session.database.write_bytes(changed)
        with self.assertRaises(ValueError):await ProtectedSessionStorage('account',self.root).open()
        self.assertEqual(session.database.read_bytes(),changed)

    async def test_legacy_wal_migration(self):
        original=FileStorage('wal',self.root)
        await original.open()
        original.conn.execute('PRAGMA journal_mode=WAL')
        auth=secrets.token_bytes(256)
        await original.auth_key(auth)
        await original.save()
        await original.close()
        protected=ProtectedSessionStorage('wal',self.root)
        await protected.open()
        await protected.close()
        reopened=ProtectedSessionStorage('wal',self.root)
        await reopened.open()
        self.assertEqual(await reopened.auth_key(),auth)
        await reopened.close()

class TransportTests(unittest.TestCase):
    def test_plain_remote_http_and_spoofed_tls_headers_rejected(self):
        client=web._flask_app.test_client()
        for headers in [{},{'X-Forwarded-Proto':'https','X-Forwarded-For':'127.0.0.1'}]:
            response=client.post('/login',data={'password':'dummy'},headers=headers,environ_base={'REMOTE_ADDR':'192.0.2.1'})
            self.assertEqual(response.status_code,426)

    def test_forwarded_ip_does_not_bypass_login_throttle(self):
        client=web._flask_app.test_client()
        with patch.dict(web.login_attempts,clear=True):
            for attempt in range(web.LOGIN_ATTEMPT_LIMIT):
                response=client.post('/login',data={'password':'wrong'},headers={'X-Forwarded-For':f'198.51.100.{attempt}'})
                self.assertEqual(response.status_code,401)
            self.assertEqual(client.post('/login',data={'password':'wrong'},headers={'X-Forwarded-For':'203.0.113.1'}).status_code,429)

    def test_https_login_uses_secure_cookie_and_unicode_password(self):
        client=web._flask_app.test_client()
        with patch.dict(web.web_login_users,root='synthetic-中文密码'),patch.dict(web.login_attempts,clear=True):
            response=client.post('/login',base_url='https://example.com',data={'password':'synthetic-中文密码'},environ_base={'REMOTE_ADDR':'192.0.2.1'})
            self.assertEqual(response.status_code,200)
            self.assertIn('Secure',response.headers['Set-Cookie'])

    def test_only_explicit_tls_proxy_is_trusted(self):
        from types import SimpleNamespace
        application=SimpleNamespace(config={'web_trusted_proxies':['127.0.0.1']})
        with patch.object(web,'web_application',application):
            response=web._flask_app.test_client().get('/login',base_url='http://console.example',headers={'X-Forwarded-Proto':'https'})
            self.assertEqual(response.status_code,200)

class RedactionTests(unittest.TestCase):
    def test_both_logging_systems_hide_exception_credentials(self):
        sensitive='synthetic-sensitive-secret'
        register_secrets({'proxy':{'password':sensitive}})
        result=io.StringIO()
        sink=logger.add(result,diagnose=False,backtrace=False)
        try:
            try:raise ValueError('password='+sensitive)
            except ValueError:logger.exception('failure '+sensitive)
        finally:logger.remove(sink)
        self.assertNotIn(sensitive,result.getvalue())
        record=logging.getLogger('test').makeRecord('test',logging.ERROR,'demo',1,sensitive,(),None)
        self.assertNotIn(sensitive,record.getMessage())
        self.assertNotIn('+12025550123',redact('phone +12025550123'))

class WorkerSupervisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_unexpected_failure_restarts_but_cancellation_does_not(self):
        import media_downloader as main
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        application=SimpleNamespace(is_running=True)
        async def stop(*args):
            application.is_running=False
        work=AsyncMock(side_effect=[RuntimeError('synthetic worker failure'),None])
        with patch.object(main,'app',application),patch.object(main,'worker',work),patch.object(main.asyncio,'sleep',side_effect=stop):
            await main.supervised_worker(None)
            self.assertEqual(work.await_count,1)
        application.is_running=True
        async def finish(*args):
            application.is_running=False
        count=0
        async def twice(*args):
            nonlocal count
            count+=1
            if count==1:raise RuntimeError('synthetic failure')
            application.is_running=False
        with patch.object(main,'app',application),patch.object(main,'worker',side_effect=twice),patch.object(main.asyncio,'sleep',new=AsyncMock()):
            await main.supervised_worker(None)
            self.assertEqual(count,2)
        application.is_running=True
        cancelled=AsyncMock(side_effect=asyncio.CancelledError())
        with patch.object(main,'app',application),patch.object(main,'worker',cancelled):
            with self.assertRaises(asyncio.CancelledError):await main.supervised_worker(None)
            self.assertEqual(cancelled.await_count,1)

class BuildGateTests(unittest.TestCase):
    def test_public_release_rejects_missing_developer_id_before_creating_files(self):
        import subprocess
        import sys
        root=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'output'
            result=subprocess.run([sys.executable,str(root/'scripts/build_native_macos.py'),'--output',str(output),'--sing-box','/unused','--release'],capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertFalse(output.exists())
            self.assertIn('Developer ID Application',result.stderr)

class SchedulerSnapshotTests(unittest.TestCase):
    def test_snapshot_survives_concurrent_channel_growth(self):
        import threading
        from collections import deque
        from module.fair_scheduler import FairChannelScheduler
        scheduler=FairChannelScheduler()
        done=threading.Event()
        def grow():
            for index in range(10000):
                key=str(index)
                scheduler._queues[key]=deque([None])
                scheduler._priorities[key]='normal'
                scheduler._in_flight[key]=1
            done.set()
        thread=threading.Thread(target=grow)
        thread.start()
        try:
            for _ in range(30):
                result=scheduler.snapshot()
                self.assertEqual(result['active_channels'],len(result['channels']))
        finally:thread.join()
        self.assertTrue(done.is_set())

class EncryptedSessionBackupTests(unittest.IsolatedAsyncioTestCase):
    setUp = VaultTests.setUp
    tearDown = VaultTests.tearDown
    async def test_export_session_remains_encrypted_at_new_path(self):
        import sys
        scripts=str(Path(__file__).resolve().parents[1]/'scripts')
        sys.path.insert(0,scripts)
        try:
            from extract_channel_chat_ids import backup_session
            original=ProtectedSessionStorage('original',self.root)
            await original.open()
            auth=secrets.token_bytes(256)
            await original.auth_key(auth)
            await original.close()
            destination=self.root/'copy.session'
            backup_session(original.database,destination)
            self.assertNotIn(auth,destination.read_bytes())
            copied=ProtectedSessionStorage('copy',self.root)
            await copied.open()
            self.assertEqual(await copied.auth_key(),auth)
            await copied.close()
        finally:sys.path.remove(scripts)
