"""Bounded, offline node imports. Never fetch subscriptions or return credentials."""
import base64
import re
import uuid
from urllib.parse import parse_qs, unquote, urlsplit

import yaml

LIMIT = 256 * 1024
MAX_NODES = 500
TYPES = {'ss', 'vless', 'vmess', 'trojan', 'hysteria2', 'tuic', 'socks5', 'http'}
LABELS = {'ss': 'SS', 'vless': 'VLESS', 'vmess': 'VMess', 'trojan': 'Trojan',
          'hysteria2': 'Hysteria2', 'tuic': 'TUIC', 'socks5': 'SOCKS5', 'http': 'HTTP'}


class ImportLoader(yaml.SafeLoader):
    def __init__(self, stream):
        super().__init__(stream)
        self.depth = self.nodes = 0

    def compose_node(self, parent, index):
        self.nodes += 1
        self.depth += 1
        try:
            if self.depth > 24 or self.nodes > 16000 or self.check_event(yaml.AliasEvent):
                raise ValueError('配置过于复杂或含 YAML 引用，请导出不含锚点的普通节点列表。')
            return super().compose_node(parent, index)
        finally:
            self.depth -= 1

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in result:
                raise ValueError('配置含重复字段或非文字字段名，请重新导出。')
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def _document(text):
    loader = ImportLoader(text)
    try:
        return loader.get_single_data()
    except yaml.YAMLError as error:
        # YAML exception messages can contain the entire credential-bearing line.
        raise ValueError('YAML / JSON 格式不正确，请检查缩进和引号，或重新导出文件。') from error
    finally:
        loader.dispose()


def _string(value, label, optional=False):
    if optional and value in (None, ''):
        return ''
    if not isinstance(value, str) or not value or len(value) > 8192 or '\x00' in value:
        raise ValueError(label + '不完整或格式不正确。')
    return value


def _integer(value, label, minimum=1, maximum=65535):
    if isinstance(value, bool) or not re.fullmatch(r'\d+', str(value)):
        raise ValueError(label + '需要填写有效整数。')
    result = int(value)
    if not minimum <= result <= maximum:
        raise ValueError(label + '超出允许范围。')
    return result


def _boolean(value, label):
    if isinstance(value, bool):
        return value
    if value in (0, 1, '0', '1', 'true', 'false'):
        return value in (1, '1', 'true')
    raise ValueError(label + '需要填写 true 或 false。')


def _uuid(value):
    try:
        return str(uuid.UUID(_string(value, 'UUID')))
    except (ValueError, AttributeError) as error:
        raise ValueError('UUID 不完整，请重新导出节点。') from error


def _known(data, allowed):
    if set(data) - set(allowed):
        raise ValueError('此节点含暂不支持的附加选项，请通过已有代理软件的 SOCKS5 使用。')


def _tls(data, host, required=False):
    enabled = _boolean(data.get('tls', required), 'TLS')
    if not enabled:
        if required or any(data.get(key) for key in ('servername', 'sni', 'reality-opts', 'alpn', 'client-fingerprint', 'skip-cert-verify')):
            raise ValueError('此节点需要完整的 TLS 设置，请重新导出。')
        return None
    if _boolean(data.get('skip-cert-verify', False), '证书校验'):
        raise ValueError('此节点要求跳过证书校验，请使用有效证书的节点，或通过已有客户端的 SOCKS5 使用。')
    server_name = _string(data.get('servername') or data.get('sni') or host, 'TLS 服务器名称')
    if any(c.isspace() for c in server_name):
        raise ValueError('TLS 服务器名称不能包含空格。')
    tls = {'enabled': True, 'server_name': server_name}
    if data.get('alpn'):
        alpn = data['alpn']
        if not isinstance(alpn, list) or not alpn or len(alpn) > 20:
            raise ValueError('ALPN 需要填写列表。')
        if any(not isinstance(v, str) or not v or len(v) > 255 or any(c.isspace() for c in v) for v in alpn):
            raise ValueError('ALPN 格式不正确。')
        tls['alpn'] = alpn
    reality = data.get('reality-opts')
    fingerprint = data.get('client-fingerprint') or ('chrome' if reality else '')
    if fingerprint:
        if fingerprint not in {'chrome', 'firefox', 'safari', 'ios', 'android', 'edge', '360', 'qq', 'random', 'randomized'}:
            raise ValueError('此 TLS 指纹暂不支持。')
        tls['utls'] = {'enabled': True, 'fingerprint': fingerprint}
    if reality:
        if not isinstance(reality, dict):
            raise ValueError('REALITY 配置格式不正确。')
        _known(reality, {'public-key', 'short-id'})
        key = _string(reality.get('public-key'), 'REALITY 公钥')
        short = reality.get('short-id', '')
        if not re.fullmatch(r'[A-Za-z0-9_-]{43}=?', key):
            raise ValueError('REALITY 公钥格式不正确。')
        if not isinstance(short, str) or not re.fullmatch(r'(?:[0-9a-fA-F]{2}){0,8}', short):
            raise ValueError('REALITY 短 ID 格式不正确，请使用引号包住它。')
        tls['reality'] = {'enabled': True, 'public_key': key.rstrip('='), 'short_id': short}
    return tls


def _transport(data):
    kind = data.get('network') or 'tcp'
    if kind in {'tcp', 'raw', 'none'}:
        if data.get('ws-opts') or data.get('grpc-opts'):
            raise ValueError('传输方式与附加参数不一致。')
        return None
    if kind == 'ws':
        if data.get('grpc-opts'):
            raise ValueError('传输方式与附加参数不一致。')
        options = data.get('ws-opts') or {}
        if not isinstance(options, dict):
            raise ValueError('WebSocket 参数格式不正确。')
        _known(options, {'path', 'headers', 'max-early-data', 'early-data-header-name'})
        transport = {'type': 'ws', 'path': _string(options.get('path') or '/', 'WebSocket 路径')}
        headers = options.get('headers') or {}
        if not isinstance(headers, dict) or len(headers) > 30 or any(not isinstance(k, str) or not isinstance(v, str) or '\r' in k + v or '\n' in k + v for k, v in headers.items()):
            raise ValueError('WebSocket 请求头格式不正确。')
        if headers:
            transport['headers'] = headers
        if options.get('max-early-data'):
            transport['max_early_data'] = _integer(options['max-early-data'], 'WebSocket Early Data', 0, 65535)
        if options.get('early-data-header-name'):
            transport['early_data_header_name'] = _string(options['early-data-header-name'], 'WebSocket Early Data 请求头')
        return transport
    if kind == 'grpc':
        if data.get('ws-opts'):
            raise ValueError('传输方式与附加参数不一致。')
        options = data.get('grpc-opts') or {}
        if not isinstance(options, dict):
            raise ValueError('gRPC 参数格式不正确。')
        _known(options, {'grpc-service-name'})
        return {'type': 'grpc', 'service_name': _string(options.get('grpc-service-name'), 'gRPC 服务名', optional=True)}
    raise ValueError('此传输方式暂不支持。目前可导入 TCP、WebSocket 和 gRPC；其他方式请用已有客户端的 SOCKS5。')


def normalize(data):
    """Convert a Clash-style node to a strict, credential-private sing-box node."""
    if not isinstance(data, dict):
        raise ValueError('节点必须是一组字段。')
    kind = data.get('type')
    if isinstance(kind, str):
        kind = kind.lower()
    kind = {'shadowsocks': 'ss', 'hy2': 'hysteria2'}.get(kind, kind) if isinstance(kind, str) else ''
    if kind not in TYPES:
        raise ValueError('节点协议暂不支持，请通过已有代理客户端的 SOCKS5 使用。')
    common = {'type', 'name', 'server', 'port', 'udp', 'tls', 'servername', 'sni',
              'skip-cert-verify', 'client-fingerprint', 'alpn', 'network', 'smux'}
    fields = {
        'ss': {'cipher', 'password'}, 'vless': {'uuid', 'flow', 'encryption', 'reality-opts', 'ws-opts', 'grpc-opts', 'packet-encoding'},
        'vmess': {'uuid', 'alterId', 'cipher', 'ws-opts', 'grpc-opts', 'packet-encoding'},
        'trojan': {'password', 'ws-opts', 'grpc-opts'},
        'hysteria2': {'password', 'obfs', 'obfs-password', 'up', 'down'},
        'tuic': {'uuid', 'password', 'congestion-controller', 'udp-relay-mode', 'reduce-rtt'},
        'socks5': {'username', 'password'}, 'http': {'username', 'password'},
    }
    _known(data, common | fields[kind])
    if data.get('smux'):
        mux = data['smux']
        if not isinstance(mux, dict) or mux.get('enabled') is not False or set(mux) != {'enabled'}:
            raise ValueError('此节点使用额外的多路复用设置，请通过已有客户端的 SOCKS5 使用。')
    host = _string(data.get('server'), '服务器地址')
    if any(c.isspace() for c in host) or any(c in host for c in '/?#@[]'):
        raise ValueError('服务器地址应只填写域名或 IP。')
    name = data.get('name') or LABELS[kind] + ' 节点'
    if not isinstance(name, str):
        raise ValueError('节点名称需要填写文字。')
    node = {'type': kind, 'name': ''.join(c for c in name if c.isprintable())[:100],
            'server': host, 'server_port': _integer(data.get('port'), '端口')}
    if kind == 'ss':
        from module.network_setup import METHODS
        if data.get('cipher') not in METHODS:
            raise ValueError('SS 加密方式暂不支持。')
        if any(data.get(key) for key in common - {'type', 'name', 'server', 'port', 'udp', 'smux'}):
            raise ValueError('SS 节点含附加传输选项，请通过已有客户端的 SOCKS5 使用。')
        node.update(method=data['cipher'], password=_string(data.get('password'), 'SS 密码'))
        return node
    if kind == 'vless' and 'tls' not in data:
        raise ValueError('VLESS 配置缺少 TLS 设置。请导出完整的 TLS / REALITY 节点。')
    tls = _tls(data, host, required=kind in {'vless', 'trojan', 'hysteria2', 'tuic'})
    if tls:
        if kind == 'socks5':
            raise ValueError('SOCKS5 的额外 TLS 包装暂不支持，请填写代理客户端提供的普通 SOCKS5 地址。')
        if kind in {'hysteria2', 'tuic'} and 'utls' in tls:
            raise ValueError('此 QUIC 节点的 TLS 指纹配置暂不支持。')
        node['tls'] = tls
    if kind in {'vless', 'vmess', 'tuic'}:
        node['uuid'] = _uuid(data.get('uuid'))
    if kind in {'trojan', 'hysteria2', 'tuic'}:
        node['password'] = _string(data.get('password'), '节点密码')
    if kind in {'vless', 'vmess', 'trojan'}:
        transport = _transport(data)
        if transport:
            node['transport'] = transport
    elif data.get('network') not in (None, '', 'tcp'):
        raise ValueError('此节点的网络设置暂不支持。')
    if kind == 'vless':
        if data.get('encryption') not in (None, '', 'none'):
            raise ValueError('新版 VLESS Encryption 暂不支持，请使用已有客户端的 SOCKS5。')
        flow = data.get('flow') or ''
        if flow not in {'', 'xtls-rprx-vision'} or (flow and node.get('transport')):
            raise ValueError('VLESS 流控与传输方式不兼容。')
        if flow:
            node['flow'] = flow
        if tls.get('reality') and node.get('transport'):
            raise ValueError('目前 REALITY 支持 TCP / Vision，请使用完整 TCP 节点。')
    if kind == 'vmess':
        cipher = data.get('cipher') or 'auto'
        if cipher not in {'auto', 'none', 'zero', 'aes-128-gcm', 'chacha20-poly1305', 'aes-128-ctr'}:
            raise ValueError('VMess 加密方式暂不支持。')
        node.update(security=cipher, alter_id=_integer(data.get('alterId', 0), 'VMess Alter ID', 0))
    if kind == 'hysteria2':
        if data.get('obfs'):
            if data['obfs'] != 'salamander':
                raise ValueError('Hysteria2 混淆方式暂不支持。')
            node['obfs'] = {'type': 'salamander', 'password': _string(data.get('obfs-password'), '混淆密码')}
        elif data.get('obfs-password'):
            raise ValueError('混淆密码需要同时指定混淆方式。')
        for key in ('up', 'down'):
            if data.get(key):
                match = re.fullmatch(r'(\d+)\s*(?:[Mm][Bb][Pp][Ss])?', str(data[key]))
                if not match:
                    raise ValueError('Hysteria2 带宽需要填写 Mbps 数值。')
                node[key + '_mbps'] = _integer(match[1], '带宽', 1, 1000000)
    if kind == 'tuic':
        congestion = data.get('congestion-controller', 'cubic')
        relay = data.get('udp-relay-mode', 'native')
        if congestion not in {'cubic', 'new_reno', 'bbr'} or relay not in {'native', 'quic'}:
            raise ValueError('TUIC 拥塞控制或 UDP 转发设置暂不支持。')
        node.update(congestion_control=congestion, udp_relay_mode=relay,
                    zero_rtt_handshake=_boolean(data.get('reduce-rtt', False), 'TUIC RTT'))
    if kind in {'socks5', 'http'}:
        node['username'] = _string(data.get('username'), '用户名', optional=True)
        node['password'] = _string(data.get('password'), '代理密码', optional=True)
    return node


def _base64(text):
    try:
        return base64.b64decode(text + '=' * (-len(text) % 4), altchars=b'-_', validate=True).decode('utf-8')
    except (ValueError, UnicodeError) as error:
        raise ValueError('Base64 节点内容不完整，请重新导出。') from error


def parse_link(link):
    scheme, separator, body = link.partition('://')
    link = scheme.lower() + separator + body
    if link.startswith('ss://'):
        from module.network_setup import parse_ss_link
        query = link.split('#', 1)[0].partition('?')[2]
        if query and set(parse_qs(query, keep_blank_values=True)) - {'plugin'}:
            raise ValueError('SS 链接含暂不支持的附加选项，请通过已有客户端的 SOCKS5 使用。')
        return {'type': 'ss', **parse_ss_link(link)}
    if link.startswith('vmess://'):
        try:
            original = _document(_base64(link[8:].split('#', 1)[0]))
            if not isinstance(original, dict):
                raise ValueError('VMess 分享格式不正确。')
            _known(original, {'v', 'ps', 'add', 'port', 'id', 'aid', 'scy', 'net', 'type', 'host', 'path', 'tls', 'sni', 'alpn', 'fp'})
            if original.get('type') not in (None, '', 'none'):
                raise ValueError('VMess 伪装设置暂不支持。')
            data = {'type': 'vmess', 'name': original.get('ps'), 'server': original.get('add'),
                    'port': original.get('port'), 'uuid': original.get('id'), 'alterId': original.get('aid', 0),
                    'cipher': original.get('scy') or 'auto', 'network': original.get('net') or 'tcp',
                    'tls': original.get('tls') in ('tls', True), 'sni': original.get('sni'), 'client-fingerprint': original.get('fp')}
            if original.get('tls') not in (None, '', 'none', 'tls', False, True):
                raise ValueError('VMess TLS 设置暂不支持。')
            if original.get('alpn'):
                data['alpn'] = _string(original['alpn'], 'ALPN').split(',')
            if data['network'] == 'ws':
                data['ws-opts'] = {'path': original.get('path') or '/', 'headers': {'Host': original['host']} if original.get('host') else {}}
            elif data['network'] == 'grpc':
                data['grpc-opts'] = {'grpc-service-name': original.get('path') or ''}
            elif original.get('path') or original.get('host'):
                raise ValueError('VMess 传输方式与附加设置不一致。')
            return normalize(data)
        except (TypeError, KeyError, AttributeError) as error:
            raise ValueError('VMess 分享链接格式不正确，请重新导出。') from error
    try:
        address = urlsplit(link)
        host, port = address.hostname, address.port
        if port is None and address.scheme in {'hy2', 'hysteria2'}:
            port = 443
        options = parse_qs(address.query, keep_blank_values=True, max_num_fields=40)
    except ValueError as error:
        raise ValueError('节点地址、端口或参数不正确。') from error
    kind = {'hy2': 'hysteria2', 'socks': 'socks5', 'https': 'http'}.get(address.scheme, address.scheme)
    if kind not in TYPES or address.path not in ('', '/') or any(len(v) != 1 for v in options.values()):
        raise ValueError('这不是支持的完整节点链接。订阅网址请先在客户端导出为 YAML 或节点列表。')
    params = {k: v[0] for k, v in options.items()}
    allowed = {'sni', 'servername', 'alpn', 'insecure', 'allowInsecure'}
    if kind in {'vless', 'trojan'}:
        allowed |= {'type', 'security', 'encryption', 'flow', 'fp', 'pbk', 'sid', 'path', 'host', 'serviceName', 'headerType', 'spx'}
    if kind == 'hysteria2':
        allowed |= {'obfs', 'obfs-password'}
    if kind == 'tuic':
        allowed |= {'congestion_control', 'udp_relay_mode', 'reduce_rtt'}
    _known(params, allowed if kind not in {'socks5', 'http'} else set())
    data = {'type': kind, 'name': unquote(address.fragment) or None, 'server': host, 'port': port,
            'sni': params.get('sni') or params.get('servername')}
    if address.scheme == 'https':
        data['tls'] = True
    if params.get('alpn'):
        data['alpn'] = params['alpn'].split(',')
    if any(_boolean(params[k], '证书校验') for k in ('insecure', 'allowInsecure') if k in params):
        data['skip-cert-verify'] = True
    if kind == 'vless':
        if address.password is not None:
            raise ValueError('VLESS 链接格式不正确。')
        data['uuid'] = unquote(address.username or '')
        data['flow'] = params.get('flow', '')
        data['encryption'] = params.get('encryption', 'none')
    elif kind in {'trojan', 'hysteria2'}:
        auth = (address.username or '') + (':' + address.password if address.password is not None else '')
        data['password'] = unquote(auth)
    elif kind == 'tuic':
        data.update(uuid=unquote(address.username or ''), password=unquote(address.password or ''),
                    **{'congestion-controller': params.get('congestion_control', 'cubic'), 'udp-relay-mode': params.get('udp_relay_mode', 'native'),
                       'reduce-rtt': _boolean(params.get('reduce_rtt', False), 'TUIC RTT')})
    else:
        data.update(username=unquote(address.username or ''), password=unquote(address.password or ''))
    if kind in {'vless', 'trojan'}:
        security = params.get('security', 'tls' if kind == 'trojan' else 'none')
        if security not in {'tls', 'reality'} or (security == 'reality' and kind != 'vless'):
            raise ValueError('此节点需要 TLS 或 VLESS REALITY，请导出完整设置。')
        data['tls'] = True
        data['network'] = params.get('type') or 'tcp'
        data['client-fingerprint'] = params.get('fp')
        if params.get('headerType', 'none') != 'none' or params.get('spx', '') not in ('', '/'):
            raise ValueError('节点附加伪装设置暂不支持。')
        if security == 'reality':
            data['reality-opts'] = {'public-key': params.get('pbk'), 'short-id': params.get('sid', '')}
        elif params.get('pbk') or params.get('sid'):
            raise ValueError('TLS 与 REALITY 设置不一致。')
        if data['network'] == 'ws':
            data['ws-opts'] = {'path': params.get('path') or '/', 'headers': {'Host': params['host']} if params.get('host') else {}}
        elif data['network'] == 'grpc':
            data['grpc-opts'] = {'grpc-service-name': params.get('serviceName', '')}
        elif params.get('path') or params.get('host') or params.get('serviceName'):
            raise ValueError('传输方式与附加参数不一致。')
    if kind == 'hysteria2':
        data.update(obfs=params.get('obfs'), **{'obfs-password': params.get('obfs-password')})
    return normalize(data)


def read_import(content):
    if not isinstance(content, str) or not content.strip():
        raise ValueError('请先粘贴节点链接或上传 YAML / JSON 文件。')
    if len(content.encode('utf-8')) > LIMIT:
        raise ValueError('配置超过 256 KB，请只导出需要的节点。')
    content = content.lstrip('\ufeff').strip()
    lines = [line.strip() for line in content.splitlines() if line.strip() and not line.lstrip().startswith('#')]
    if all(re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', line) for line in lines):
        entries, source = lines, 'links'
    elif re.fullmatch(r'[A-Za-z0-9_+/=-]+', ''.join(content.split())):
        decoded = _base64(''.join(content.split()))
        entries = [line.strip() for line in decoded.splitlines() if line.strip()]
        if not entries or not all(re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*://', line) for line in entries):
            raise ValueError('Base64 内容不是节点链接列表。')
        source = 'base64'
    else:
        document = _document(content)
        if isinstance(document, dict) and 'proxies' in document:
            entries = document['proxies']
        elif isinstance(document, dict) and 'type' in document:
            entries = [document]
        elif isinstance(document, list):
            entries = document
        else:
            raise ValueError('没有找到节点。请导出含 proxies 的 Clash / Mihomo YAML 或 JSON；不读取远程订阅和规则。')
        source = 'json' if content[0] in '[{' else 'yaml'
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_NODES:
        raise ValueError('配置需要包含 1–500 个节点。')
    results = []
    for index, entry in enumerate(entries):
        try:
            if isinstance(entry, str):
                if len(entry) > 16384 or any(c.isspace() for c in entry):
                    raise ValueError('节点链接不完整，请重新复制。')
                node = parse_link(entry)
            else:
                node = normalize(entry)
            results.append({'index': index, 'name': node['name'], 'protocol': LABELS[node['type']], 'supported': True, 'node': node})
        except ValueError as error:
            # Don't expose arbitrary node fields, malformed lines, passwords or UUIDs.
            results.append({'index': index, 'name': '第 ' + str(index + 1) + ' 个节点',
                            'protocol': '', 'supported': False, 'message': str(error)})
        except (TypeError, AttributeError, KeyError) as error:
            results.append({'index': index, 'name': '第 ' + str(index + 1) + ' 个节点',
                            'protocol': '', 'supported': False, 'message': '节点字段格式不正确，请重新导出。'})
    return {'format': source, 'nodes': results}


def preview(content):
    result = read_import(content)
    return {'format': result['format'], 'nodes': [{k: v for k, v in item.items() if k != 'node'} for item in result['nodes']]}


def select(content, index=None):
    nodes = read_import(content)['nodes']
    supported = [item for item in nodes if item['supported']]
    if index in (None, '', -1, '-1'):
        if len(supported) != 1:
            raise ValueError('请先读取配置，并选择一个可用节点。')
        selected = supported[0]
    else:
        number = _integer(index, '节点序号', 0, len(nodes) - 1)
        selected = nodes[number]
    if not selected['supported']:
        raise ValueError(selected['message'])
    return selected['node']
