"""Local native picker, cancellation, and remote-browser protection."""
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
from module import local_storage as storage, web


class NativeFolderTests(unittest.TestCase):
    def test_picker_returns_path_without_writing_files(self):
        with tempfile.TemporaryDirectory(prefix='tmd-folder-test-') as folder:
            with mock.patch.object(storage, 'native_picker_available', return_value=True), mock.patch.object(storage.subprocess, 'run') as run:
                run.return_value = subprocess.CompletedProcess([], 0, folder + '/\n', '')
                self.assertEqual(storage.choose_folder(), str(Path(folder).resolve()))
                self.assertEqual(list(Path(folder).iterdir()), [])
                command = run.call_args.args[0]
                self.assertEqual(command[0], '/usr/bin/osascript')

    def test_cancel_preserves_path_and_lock_is_released(self):
        with mock.patch.object(storage, 'native_picker_available', return_value=True), mock.patch.object(storage.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 1, '', 'User canceled. (-128)')
            self.assertIsNone(storage.choose_folder())
            self.assertIsNone(storage.choose_folder())

    def test_platform_and_timeout_errors_are_actionable(self):
        with mock.patch.object(storage, 'native_picker_available', return_value=False):
            with self.assertRaisesRegex(ValueError, '填写'):
                storage.choose_folder()
        with mock.patch.object(storage, 'native_picker_available', return_value=True), mock.patch.object(storage.subprocess, 'run', side_effect=subprocess.TimeoutExpired('osascript',120)):
            with self.assertRaisesRegex(ValueError, '重试'):
                storage.choose_folder()


class FolderEndpointTests(unittest.TestCase):
    def setUp(self):
        self.old = web._flask_app.config.get('LOGIN_DISABLED', False)
        web._flask_app.config['LOGIN_DISABLED'] = True
        self.client = web._flask_app.test_client()

    def tearDown(self):
        web._flask_app.config['LOGIN_DISABLED'] = self.old

    def test_folder_endpoint_rejects_remote_cross_origin_and_form_requests(self):
        with mock.patch.object(storage, 'choose_folder') as picker:
            for kwargs in [dict(base_url='https://localhost',environ_base={'REMOTE_ADDR':'192.0.2.1'}),
                           dict(base_url='https://example.com'),
                           dict(headers={'X-TMD-Local-Picker':'1','Origin':'https://example.com'}),
                           dict(headers={})]:
                data = {'json':{},'headers':{'X-TMD-Local-Picker':'1'}}
                data.update(kwargs)
                self.assertEqual(self.client.post('/api/storage/choose',**data).status_code,403)
            self.assertEqual(self.client.post('/api/storage/choose',data='path=x',headers={'X-TMD-Local-Picker':'1'}).status_code,403)
            picker.assert_not_called()

    def test_selection_and_cancel_do_not_save_configuration(self):
        with mock.patch.object(storage, 'choose_folder',return_value='/tmp/demo'):
            result=self.client.post('/api/storage/choose',json={},headers={'X-TMD-Local-Picker':'1'})
            self.assertEqual(result.json['path'],'/tmp/demo')
            self.assertFalse(result.json['cancelled'])
        with mock.patch.object(storage, 'choose_folder',return_value=None):
            self.assertTrue(self.client.post('/api/storage/choose',json={},headers={'X-TMD-Local-Picker':'1'}).json['cancelled'])

    def test_authentication_and_remote_capability(self):
        with mock.patch.object(storage,'native_picker_available',return_value=True):
            self.assertFalse(self.client.get('/api/storage/capabilities',base_url='https://localhost',environ_base={'REMOTE_ADDR':'192.0.2.1'}).json['native_picker'])
        web._flask_app.config['LOGIN_DISABLED']=False
        self.assertEqual(self.client.get('/api/storage/capabilities').status_code,302)
        self.assertEqual(self.client.post('/api/storage/choose',json={},headers={'X-TMD-Local-Picker':'1'}).status_code,302)


if __name__ == '__main__':
    unittest.main()
