"""Offline multi-format imports, safe preview, isolation and real runtime checks."""
import base64
import copy
import json
import os
import socket
import unittest
from unittest import mock
import yaml
from module import node_import as imp, network_setup as net, web

UID = '00000000-0000-4000-8000-000000000001'
KEY = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=')
PASSWORD = 'example-password-only'
BASE = {'server': 'example.com', 'port': 443}
NODES = [
    dict(BASE, type='ss', name='Demo SS', cipher='aes-128-gcm', password=PASSWORD),
    dict(BASE, type='vless', name='Demo REALITY', uuid=UID, tls=True, flow='xtls-rprx-vision', **{'reality-opts': {'public-key': KEY, 'short-id': '0123456789abcdef'}}),
    dict(BASE, type='vmess', name='Demo VMess', uuid=UID, tls=True, network='ws', **{'ws-opts': {'path': '/demo', 'headers': {'Host': 'example.com'}}}),
    dict(BASE, type='trojan', name='Demo Trojan', password=PASSWORD, network='grpc', **{'grpc-opts': {'grpc-service-name': 'demo'}}),
    dict(BASE, type='hysteria2', name='Demo HY2', password=PASSWORD, obfs='salamander', **{'obfs-password': PASSWORD}),
    dict(BASE, type='tuic', name='Demo TUIC', uuid=UID, password=PASSWORD),
    dict(BASE, type='socks5', name='Demo SOCKS', username='demo', password=PASSWORD),
    dict(BASE, type='http', name='Demo HTTP', username='demo', password=PASSWORD, tls=True),
]
LINKS = [
    'ss://' + base64.b64encode(('aes-128-gcm:' + PASSWORD).encode()).decode() + '@example.com:443#Demo',
    'vless://' + UID + '@example.com:443?security=tls&type=ws&path=%2Fdemo&host=example.com#Demo',
    'vmess://' + base64.b64encode(json.dumps({'v': '2', 'ps': 'Demo', 'add': 'example.com', 'port': '443', 'id': UID, 'aid': '0', 'net': 'ws', 'type': 'none', 'path': '/demo', 'host': 'example.com', 'tls': 'tls'}).encode()).decode(),
    'trojan://' + PASSWORD + '@example.com:443?type=grpc&serviceName=demo#Demo',
    'hy2://' + PASSWORD + '@example.com?obfs=salamander&obfs-password=' + PASSWORD + '#Demo',
    'tuic://' + UID + ':' + PASSWORD + '@example.com:443?congestion_control=bbr#Demo',
    'socks5://demo:' + PASSWORD + '@127.0.0.1:1080#Demo',
    'https://demo:' + PASSWORD + '@example.com:443#Demo',
]


class ImportTests(unittest.TestCase):
    def test_all_eight_protocols_yaml_json_single_and_list(self):
        for content, kind in [(yaml.safe_dump({'proxies': NODES}), 'yaml'), (json.dumps({'proxies': NODES}), 'json'), (json.dumps(NODES), 'json')]:
            result = imp.read_import(content)
            self.assertEqual(result['format'], kind)
            self.assertTrue(all(n['supported'] for n in result['nodes']), result)
            self.assertEqual(len(result['nodes']), 8)
        self.assertEqual(imp.select(yaml.safe_dump(NODES[0]))['password'], PASSWORD)

    def test_all_links_and_base64_lists_preserve_auth_transport(self):
        content = '\n'.join(LINKS)
        nodes = imp.read_import(content)['nodes']
        self.assertTrue(all(n['supported'] for n in nodes), nodes)
        self.assertEqual(nodes[1]['node']['transport']['headers']['Host'], 'example.com')
        self.assertEqual(nodes[2]['node']['transport']['path'], '/demo')
        self.assertEqual(nodes[3]['node']['transport']['service_name'], 'demo')
        self.assertEqual(nodes[4]['node']['server_port'], 443)
        self.assertEqual(nodes[5]['node']['password'], PASSWORD)
        self.assertEqual(imp.preview(base64.b64encode(content.encode()).decode())['format'], 'base64')
        encoded = base64.b64encode(content.encode()).decode()
        wrapped = '\n'.join(encoded[i:i+76] for i in range(0, len(encoded), 76))
        self.assertEqual(imp.preview(wrapped)['format'], 'base64')
        for link in LINKS:
            scheme, separator, body = link.partition('://')
            self.assertTrue(imp.preview(scheme.upper() + separator + body)['nodes'][0]['supported'])

    def test_selection_is_explicit_and_only_selected_node_is_saved(self):
        content = yaml.safe_dump({'proxies': NODES})
        with self.assertRaises(ValueError):
            imp.select(content)
        for value in [True, -2, 8, '0.5', {'bad': 1}]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                imp.select(content, value)
        config = {}
        net.apply_settings(config, {'network_mode': 'ss', 'node_content': content, 'node_index': 5})
        self.assertEqual(config['ss_proxy']['node']['type'], 'tuic')
        self.assertNotIn('proxies', config)
        before = copy.deepcopy(config)
        net.apply_settings(config, {'network_mode': 'ss', 'node_content': ''})
        self.assertEqual(config, before)
        public = web._public_config(config)
        self.assertEqual(public['network_status']['node_protocol'], 'tuic')
        for secret in [UID, PASSWORD, KEY]:
            self.assertNotIn(secret, repr(public))

    def test_preview_never_returns_credentials_and_partial_failure_keeps_indices(self):
        content = yaml.safe_dump({'proxies': [dict(BASE, type='ssr', password=PASSWORD)] + NODES})
        result = imp.preview(content)
        self.assertFalse(result['nodes'][0]['supported'])
        self.assertEqual(result['nodes'][1]['index'], 1)
        for secret in [UID, PASSWORD, KEY, 'password', 'public-key']:
            self.assertNotIn(secret, repr(result))
        self.assertEqual(imp.select(content, 1)['type'], 'ss')
        with self.assertRaises(ValueError):
            imp.select(content, 0)

    def test_unsafe_invalid_oversized_deep_duplicate_and_anchored_configs(self):
        invalid = ['', '# comment', 'proxies: [', '!!python/object/apply:os.system [bad]',
                   'proxies: []\nproxies: []', 'proxies: [ &x {type: ss}, *x ]',
                   '[' * 25 + '1' + ']' * 25, 'x' * (imp.LIMIT + 1),
                   'proxies:\n- ' * 1 + '{type: ss}\n---\nsecret: ' + PASSWORD]
        for content in invalid:
            with self.subTest(content=content[:30]), self.assertRaises(ValueError) as error:
                imp.preview(content)
            self.assertNotIn(PASSWORD, str(error.exception))
        with self.assertRaises(ValueError):
            imp.preview(yaml.safe_dump({'proxies': [dict(BASE, type='ss')] * 501}))

    def test_unsupported_security_transport_chains_and_malformed_fields_are_actionable(self):
        invalid = [dict(NODES[1], tls=False), dict(NODES[1], **{'skip-cert-verify': True}),
                   dict(NODES[1], network='xhttp'), dict(NODES[1], **{'dialer-proxy': 'chain'}),
                   dict(NODES[0], plugin='obfs'), dict(NODES[0], cipher={}),
                   dict(NODES[1], uuid=PASSWORD), dict(NODES[1], **{'reality-opts': {'public-key': PASSWORD}}),
                   dict(NODES[2], **{'ws-opts': {'headers': {'Host': 'bad\r\n'}}}),
                   dict(NODES[1], **{'reality-opts': {'public-key': KEY, 'short-id': 123}}),
                   dict(NODES[0], password=123), dict(NODES[0], type=[]), dict(NODES[0], port=True)]
        result = imp.preview(yaml.safe_dump({'proxies': json.loads(json.dumps(invalid))}))
        self.assertFalse(any(n['supported'] for n in result['nodes']))
        self.assertNotIn(PASSWORD, repr(result))
        for bad_link in ['https://example.com/sub?token=' + PASSWORD, LINKS[1].replace('#Demo', '&unknown=' + PASSWORD + '#Demo'),
                         'tuic://missing:' + PASSWORD + '@example.com:443', 'vless://' + UID + '@example.com:443?security=tls&type=ws&flow=xtls-rprx-vision']:
            result = imp.preview(bad_link)
            self.assertFalse(result['nodes'][0]['supported'])
            self.assertNotIn(PASSWORD, repr(result))

    def test_authenticated_preview_is_offline_and_candidate_test_never_writes(self):
        old = web._flask_app.config.get('LOGIN_DISABLED', False)
        try:
            web._flask_app.config['LOGIN_DISABLED'] = False
            self.assertEqual(web._flask_app.test_client().post('/api/network/import', json={'content': LINKS[0]}).status_code, 302)
            web._flask_app.config['LOGIN_DISABLED'] = True
            config = {'proxy': {'hostname': 'original', 'port': 1080}}
            before = copy.deepcopy(config)
            content = yaml.safe_dump({'proxies': NODES})
            with mock.patch.object(web, '_read_config', return_value=config), mock.patch.object(web, '_write_config') as write, mock.patch.object(net, 'probe', return_value='ok') as probe:
                client = web._flask_app.test_client()
                result = client.post('/api/network/import', json={'content': content})
                self.assertEqual(result.status_code, 200)
                self.assertIn('no-store', result.headers['Cache-Control'])
                self.assertNotIn(PASSWORD, result.get_data(as_text=True))
                probe.assert_not_called()
                response = client.post('/api/network/test', json={'network_mode': 'ss', 'node_content': content, 'node_index': 3})
                self.assertEqual(response.status_code, 200)
                self.assertEqual(probe.call_args.args[0]['ss_proxy']['node']['type'], 'trojan')
                self.assertEqual(config, before)
                write.assert_not_called()
                self.assertEqual(client.post('/api/network/import', json={'content': 'proxies: [password: '+PASSWORD}).status_code, 400)
                self.assertEqual(client.post('/api/network/import', json=[]).status_code, 400)
        finally:
            web._flask_app.config['LOGIN_DISABLED'] = old

    @unittest.skipUnless(os.environ.get('TMD_SINGBOX_BINARY'), 'optional actual sing-box runtime')
    def test_actual_runtime_all_protocols_start_local_socks_and_release_port(self):
        # No traffic through the outbound and no user's node credentials.
        nodes = [imp.normalize(node) for node in NODES] + [imp.parse_link(link) for link in LINKS]
        for node in nodes:
            with self.subTest(protocol=node['type'], transport=node.get('transport')):
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
                with net.running_ss(node, port) as process:
                    self.assertIsNone(process.poll())
                    with socket.create_connection(('127.0.0.1', port), timeout=1):
                        pass
                self.assertIsNotNone(process.poll())
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', port))

    def test_save_endpoint_keeps_only_selected_node_and_redacts_response(self):
        old = web._flask_app.config.get('LOGIN_DISABLED', False)
        content = yaml.safe_dump({'proxies': NODES})
        payload = {'network_mode': 'ss', 'node_content': content, 'media_types': ['video'], 'file_formats': {}}
        try:
            web._flask_app.config['LOGIN_DISABLED'] = True
            for index, original in enumerate(NODES):
                payload['node_index'] = index
                config = {'api_id': 1, 'api_hash': '0' * 32}
                with mock.patch.object(web, '_read_config', return_value=config), mock.patch.object(web, '_write_config') as write:
                    response = web._flask_app.test_client().post('/api/config', json=payload)
                self.assertEqual(response.status_code, 200)
                saved = write.call_args.args[0]
                self.assertEqual(saved['ss_proxy']['node']['type'], original['type'])
                self.assertNotIn('node_content', saved)
                self.assertNotIn('proxies', saved)
                for secret in (UID, PASSWORD, KEY):
                    self.assertNotIn(secret, response.get_data(as_text=True))
            payload['node_index'] = -1
            with mock.patch.object(web, '_read_config', return_value={}), mock.patch.object(web, '_write_config') as write:
                response = web._flask_app.test_client().post('/api/config', json=payload)
            self.assertEqual(response.status_code, 400)
            write.assert_not_called()
        finally:
            web._flask_app.config['LOGIN_DISABLED'] = old
