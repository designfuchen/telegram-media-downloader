"""Regression tests for the public console boundary and shell-free uploads."""
import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock
from module import web
from module.cloud_drive import CloudDrive

class ConsoleBoundaryTests(unittest.TestCase):
    def test_cross_origin_writes_are_rejected_before_mutation(self):
        with patch.dict(web._flask_app.config, LOGIN_DISABLED=True), patch.object(web, '_write_config') as write:
            response=web._flask_app.test_client().post('/api/config',json={},headers={'Origin':'https://untrusted.example'})
            self.assertEqual(response.status_code,403)
            write.assert_not_called()
    def test_fetch_metadata_rejects_cross_site_without_origin(self):
        response=web._flask_app.test_client().post('/logout',headers={'Sec-Fetch-Site':'cross-site'})
        self.assertEqual(response.status_code,403)
    def test_read_response_has_security_headers(self):
        response=web._flask_app.test_client().get('/login')
        self.assertEqual(response.headers['X-Frame-Options'],'SAMEORIGIN')
        self.assertEqual(response.headers['X-Content-Type-Options'],'nosniff')
        self.assertIn('no-store',response.headers['Cache-Control'])
    def test_placeholder_password_cannot_start_web(self):
        app=SimpleNamespace(application_name='test',web_login_secret='CHANGE_ME',config={})
        with patch.object(web,'web_application',None), patch.object(web,'web_login_users',{}), patch.object(web.threading,'Thread') as thread:
            with self.assertRaises(ValueError):web.init_web(app)
            thread.assert_not_called()
    def test_malformed_login_is_not_server_error(self):
        with patch.dict(web.login_attempts,clear=True):
            response=web._flask_app.test_client().post('/login',data={'password':''})
            self.assertEqual(response.status_code,400)

class CloudCommandTests(unittest.TestCase):
    def test_remote_directory_is_literal_argument(self):
        directory='remote:folder/$(not-a-command)'
        with patch('module.cloud_drive.Popen') as process:
            CloudDrive.rclone_mkdir(SimpleNamespace(rclone_path='rclone'),directory)
            args,kwargs=process.call_args
            self.assertEqual(args[0],['rclone','mkdir',directory+'/'])
            self.assertFalse(kwargs.get('shell',False))
