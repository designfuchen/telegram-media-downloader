"""Adding a channel must invalidate a previously cached empty queue immediately."""
import os
from pathlib import Path
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from module import web, download_tasks

class NativeChannelAddTests(unittest.TestCase):
    def test_add_replaces_cached_empty_queue_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, TMD_TASK_DB=str(Path(directory)/'tasks.db')), patch.dict(web._flask_app.config, LOGIN_DISABLED=True), patch.object(web, 'web_application', SimpleNamespace(telegram_ready=True)), patch.object(web, '_live_transfer_summary', return_value={'channels': {}}), patch.object(web._channel_refresh_executor, 'submit'):
            web._invalidate_channel_api_cache()
            key=(1,50,'','')
            web._channel_api_cache[key]=(time.monotonic(),{'total':0,'records':[]})
            client=web._flask_app.test_client()
            try:
                for _ in range(2):
                    response=client.post('/api/channel_library',json={'chat_id':'-1001234567890'})
                    self.assertEqual(response.status_code,200)
                    self.assertEqual(response.json['channel_count'],1)
                    queue=client.get('/api/channels?limit=50&page=1').json
                    self.assertEqual(queue['count'],1)
                    self.assertEqual(len(queue['data']),1)
            finally:
                web._invalidate_channel_api_cache()
                download_tasks._SCHEMA_READY.clear()
