"""Node-link protocol mapping and credential privacy without a user's node."""
import base64
import copy
import os
import socket
import unittest
from unittest import mock

from module import network_setup as net, web

USER_ID = '00000000-0000-4000-8000-000000000001'
PUBLIC_KEY = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=')
TLS_LINK = 'vless://' + USER_ID + '@example.com:443?encryption=none&security=tls&sni=example.com&type=tcp&flow=xtls-rprx-vision#Demo'
REALITY_LINK = ('vless://' + USER_ID + '@[::1]:443?security=reality&type=tcp'
                '&flow=xtls-rprx-vision&sni=example.com&fp=chrome&pbk=' + PUBLIC_KEY + '&sid=0123456789abcdef#Demo')


class VLESSSetupTests(unittest.TestCase):
    def test_vision_tls_and_reality_map_security_fields_and_ipv6(self):
        tls = net.parse_node_link(TLS_LINK)
        self.assertEqual(tls['uuid'], USER_ID)
        self.assertEqual(tls['tls'], {'enabled': True, 'server_name': 'example.com'})
        node = net.parse_node_link(REALITY_LINK)
        self.assertEqual(node['server'], '::1')
        self.assertEqual(node['tls']['reality']['public_key'], PUBLIC_KEY)
        self.assertEqual(node['tls']['reality']['short_id'], '0123456789abcdef')
        self.assertEqual(node['tls']['utls']['fingerprint'], 'chrome')
        outbound = net.singbox_config(node, 12345)['outbounds'][0]
        self.assertEqual(outbound['type'], 'vless')
        self.assertEqual(outbound['flow'], 'xtls-rprx-vision')
        self.assertNotIn('password', outbound)

    def test_rejects_incomplete_links_unsupported_transport_and_weakened_tls(self):
        base = 'vless://' + USER_ID + '@example.com:443?'
        for suffix in ['security=none', 'security=tls&type=ws', 'security=tls&encryption=new',
                       'security=tls&allowInsecure=1', 'security=tls&insecure=true',
                       'security=tls&headerType=http', 'security=reality&pbk=bad',
                       'security=tls&security=reality', 'security=tls&pqv=extra',
                       'security=tls&flow=unsupported', 'security=tls&fp=unsupported']:
            with self.subTest(suffix=suffix), self.assertRaises(ValueError) as error:
                net.parse_node_link(base + suffix)
            self.assertNotIn(USER_ID, str(error.exception))
        for link in ['https://example.com/sub', 'vless://missing@example.com:443?security=tls',
                     base.replace(':443?', ':0?') + 'security=tls',
                       REALITY_LINK.replace('0123456789abcdef', 'odd'),
                       REALITY_LINK.replace('fp=chrome', 'fp='),
                     TLS_LINK.replace('&sni=example.com', '&sni=')]:
            with self.subTest(link=link), self.assertRaises(ValueError):
                net.parse_node_link(link)

    def test_retains_saved_vless_and_never_returns_uuid_or_reality_credentials(self):
        config = {}
        net.apply_settings(config, {'network_mode': 'link', 'node_link': REALITY_LINK})
        saved = copy.deepcopy(config)
        net.apply_settings(config, {'network_mode': 'ss', 'ss_link': ''})
        self.assertEqual(saved, config)
        public = web._public_config(config)
        self.assertEqual(public['network_status']['node_protocol'], 'vless')
        self.assertNotIn('ss_proxy', public)
        self.assertNotIn(USER_ID, repr(public))
        self.assertNotIn(PUBLIC_KEY, repr(public))
        net.apply_settings(config, {'network_mode': 'direct'})
        self.assertFalse(config['ss_proxy']['enabled'])
        self.assertNotIn('proxy', config)

    def test_candidate_network_test_isolated_from_saved_config(self):
        config = {'proxy': {'hostname': 'original', 'port': 1080}}
        before = copy.deepcopy(config)
        old = web._flask_app.config.get('LOGIN_DISABLED', False)
        try:
            web._flask_app.config['LOGIN_DISABLED'] = True
            with mock.patch.object(web, '_read_config', return_value=config), \
                    mock.patch.object(net, 'probe', return_value='ok') as probe, \
                    mock.patch.object(web, '_write_config') as write:
                response = web._flask_app.test_client().post('/api/network/test', json={'network_mode': 'ss', 'ss_link': TLS_LINK})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(probe.call_args.args[0]['ss_proxy']['node']['uuid'], USER_ID)
            self.assertEqual(config, before)
            write.assert_not_called()
        finally:
            web._flask_app.config['LOGIN_DISABLED'] = old

    @unittest.skipUnless(os.environ.get('TMD_SINGBOX_BINARY'), 'optional actual sing-box runtime')
    def test_actual_runtime_accepts_tls_and_reality_and_releases_ports(self):
        for link in [TLS_LINK, REALITY_LINK]:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            with net.running_ss(net.parse_node_link(link), port) as process:
                self.assertIsNone(process.poll())
                with socket.create_connection(('127.0.0.1', port), timeout=1):
                    pass
            self.assertIsNotNone(process.poll())
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(('127.0.0.1', port))
