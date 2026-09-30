"""Configuration and privacy checks without a real Telegram account or node."""
import base64
import copy
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
import requests
from module import network_setup as net, web


def encoded(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip('=')


class SSSetupTests(unittest.TestCase):
    def test_modern_legacy_and_ipv6_links(self):
        for link in ['ss://' + encoded('aes-256-gcm:demo-password') + '@example.com:443#Demo',
                     'ss://' + encoded('aes-256-gcm:demo-password@example.com:443') + '#Demo']:
            node = net.parse_ss_link(link)
            self.assertEqual((node['server'], node['server_port'], node['password']), ('example.com', 443, 'demo-password'))
        node = net.parse_ss_link('ss://aes-256-gcm:hello%3Aworld@[::1]:443#Demo')
        self.assertEqual(node['password'], 'hello:world')
        self.assertEqual(node['server'], '::1')

    def test_bad_links_fail_without_echoing_secrets(self):
        for link in ['https://example.com/subscription', 'ss://broken',
                     'ss://aes-256-gcm:demo-password@example.com:0',
                     'ss://aes-256-gcm:demo-password@example.com:443?plugin=v2ray-plugin']:
            with self.assertRaises(ValueError) as error:
                net.parse_ss_link(link)
            self.assertNotIn('demo-password', str(error.exception))

    def test_saved_ss_retained_and_direct_clears_hidden_exit(self):
        config = {'media_proxy_pool': [{'password': 'old'}]}
        net.apply_settings(config, {'network_mode': 'ss', 'ss_link': 'ss://' + encoded('aes-256-gcm:demo-password') + '@example.com:443'})
        self.assertEqual(config['proxy']['hostname'], '127.0.0.1')
        saved = copy.deepcopy(config['ss_proxy']['node'])
        net.apply_settings(config, {'network_mode': 'ss', 'ss_link': ''})
        self.assertEqual(config['ss_proxy']['node'], saved)
        net.apply_settings(config, {'network_mode': 'direct'})
        self.assertNotIn('proxy', config)
        self.assertFalse(config['ss_proxy']['enabled'])
        self.assertNotIn('media_proxy_pool', config)

    def test_recursive_redaction_preserves_secret_presence_and_original(self):
        source = {'api_id': 1, 'api_hash': 'a' * 32, 'web_login_secret': 'local-login',
                  'proxy': {'password': 'proxy-password'}, 'ss_proxy': {'enabled': True, 'node': {'password': 'node-password'}},
                  'remote_access': {'cloudflared_token': 'remote-secret'}, 'proxy_nodes': [{'link': 'private'}]}
        original = copy.deepcopy(source)
        result = web._public_config(source)
        self.assertTrue(result['secret_status']['web_login_secret'])
        self.assertTrue(result['secret_status']['proxy_password'])
        self.assertTrue(result['secret_status']['api_hash'])
        for secret in ['local-login', 'proxy-password', 'node-password', 'remote-secret']:
            self.assertNotIn(secret, repr(result))
        self.assertNotIn('proxy_nodes', result)
        self.assertEqual(source, original)

    def test_http_failure_does_not_expose_proxy_password(self):
        with mock.patch.object(net.requests, 'Session') as session:
            session.return_value.__enter__.return_value.get.side_effect = requests.exceptions.ProxyError('private-password')
            with self.assertRaises(ValueError) as error:
                net.probe({'proxy': {'hostname': 'localhost', 'port': 1080, 'password': 'private-password'}})
        self.assertNotIn('private-password', str(error.exception))

    @unittest.skipUnless(os.environ.get('TMD_SINGBOX_BINARY'), 'optional real sing-box runtime')
    def test_real_runtime_checks_configuration_and_releases_port(self):
        node = net.parse_ss_link('ss://' + encoded('aes-256-gcm:demo-password') + '@example.com:443')
        import socket
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
        with net.running_ss(node, port) as process:
            self.assertIsNone(process.poll())
            with socket.create_connection(('127.0.0.1', port), timeout=1):
                pass
        self.assertIsNotNone(process.poll())
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('127.0.0.1', port))


class WizardEndpointsTests(unittest.TestCase):
    def setUp(self):
        self.old = web._flask_app.config.get('LOGIN_DISABLED', False)
        web._flask_app.config['LOGIN_DISABLED'] = True
        self.client = web._flask_app.test_client()

    def tearDown(self):
        web._flask_app.config['LOGIN_DISABLED'] = self.old

    def test_new_setup_endpoints_require_login(self):
        web._flask_app.config['LOGIN_DISABLED'] = False
        for path in ['/api/import/template.csv', '/api/network/status']:
            self.assertEqual(self.client.get(path).status_code, 302)
        for path in ['/api/network/test', '/api/batch/import']:
            self.assertEqual(self.client.post(path, json={}).status_code, 302)

    def test_templates_excel_encoding_and_distinct_headers(self):
        for kind, header in [('channels', 'chat_id,start_message_id'), ('invites', 'order,invite_link')]:
            response = self.client.get('/api/import/template.csv?kind=' + kind)
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.data.startswith(b'\xef\xbb\xbf'))
            self.assertIn(header, response.data.decode('utf-8-sig'))
            self.assertIn('attachment', response.headers['Content-Disposition'])
        self.assertEqual(self.client.get('/api/import/template.csv?kind=bad').status_code, 400)

    def test_test_network_never_writes_or_changes_live_config(self):
        config = {'proxy': {'hostname': 'old', 'port': 1080}, 'media_proxy_pool': ['old']}
        before = copy.deepcopy(config)
        with mock.patch.object(web, '_read_config', return_value=config), mock.patch.object(net, 'probe', return_value='ok') as probe, mock.patch.object(web, '_write_config') as write:
            response = self.client.post('/api/network/test', json={'network_mode': 'direct'})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('proxy', probe.call_args.args[0])
        self.assertEqual(config, before)
        write.assert_not_called()

    def test_bad_invite_csv_does_not_mutate_batch_or_dispatch(self):
        with mock.patch.object(web.batch_queue, 'load_from_csv') as load, mock.patch.object(web.batch_queue, 'dispatch_next') as dispatch:
            response = self.client.post('/api/batch/import', json={'content': 'order,invite_link,title,chat_id\n1,https://example.com/private,Demo,\n'})
        self.assertEqual(response.status_code, 400)
        load.assert_not_called(); dispatch.assert_not_called()
        self.assertEqual(self.client.post('/api/batch/import', json=['wrong']).status_code, 400)

    def test_import_validated_invites_uses_private_file_without_dispatch(self):
        # Build the dummy URL at runtime; this is never a live invitation.
        link = 'https://t.me/' + '+' + 'demo'
        def load(path):
            self.assertEqual(Path(path).stat().st_mode & 0o777, 0o600)
            self.assertIn(link, Path(path).read_text())
            return {'loaded': 1, 'skipped': 0}
        with mock.patch.object(web.batch_queue, 'load_from_csv', side_effect=load), mock.patch.object(web.batch_queue, 'dispatch_next') as dispatch:
            response = self.client.post('/api/batch/import', json={'content': 'order,invite_link,title,chat_id\n1,' + link + ',Demo,\n'})
        self.assertEqual(response.status_code, 200)
        dispatch.assert_not_called()


class InitialSetupTests(unittest.TestCase):
    def test_first_start_waits_for_web_credentials_before_starting_proxy(self):
        import media_downloader as main
        application = mock.Mock()
        application.is_running = True
        application.config = {'api_id': 0, 'api_hash': ''}
        def reload():
            application.config = {'api_id': 1, 'api_hash': 'a' * 32}
        application.load_config.side_effect = reload
        with mock.patch.object(main.time, 'sleep') as sleep, mock.patch.object(net, 'ensure_started') as start:
            main.wait_for_initial_setup(application)
        sleep.assert_called_once_with(1)
        start.assert_called_once_with(application.config)
