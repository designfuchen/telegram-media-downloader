"""Private, managed node-link setup and connection checks for the Web wizard."""
import atexit
import base64
import contextlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from urllib.parse import parse_qs, unquote, urlsplit

import requests

METHODS = {'aes-128-gcm', 'aes-192-gcm', 'aes-256-gcm', 'chacha20-ietf-poly1305',
           'xchacha20-ietf-poly1305', '2022-blake3-aes-128-gcm',
           '2022-blake3-aes-256-gcm', '2022-blake3-chacha20-poly1305',
           'aes-128-ctr', 'aes-192-ctr', 'aes-256-ctr', 'aes-128-cfb',
           'aes-192-cfb', 'aes-256-cfb', 'rc4-md5', 'chacha20-ietf', 'xchacha20'}
LOCK = threading.RLock()
_PROCESS = None
_RUNTIME_DIR = None
LOCAL_PORT = 1088


def credentials_ready(config):
    try:
        valid_id = int(config.get('api_id') or 0) > 0
    except (TypeError, ValueError):
        valid_id = False
    return valid_id and bool(re.fullmatch(r'[0-9a-fA-F]{32}', str(config.get('api_hash') or '')))


def binary_path():
    configured = os.environ.get('TMD_SINGBOX_BINARY')
    return configured if configured and Path(configured).is_file() else shutil.which('sing-box')


def _decode(text):
    try:
        return base64.b64decode(text + '=' * (-len(text) % 4), altchars=b'-_', validate=True).decode()
    except (ValueError, UnicodeError) as error:
        raise ValueError('SS 链接的编码不完整，请重新复制完整分享链接。') from error


def parse_ss_link(value):
    value = str(value or '').strip()
    if not value.startswith('ss://') or len(value) > 8192:
        raise ValueError('请粘贴以 ss:// 开头的单个节点链接；不支持订阅网址。')
    body, _, fragment = value[5:].partition('#')
    body, _, query = body.partition('?')
    if parse_qs(query).get('plugin'):
        raise ValueError('此节点需要额外插件。请先在代理客户端中连接，再填写 SOCKS5 地址。')
    if '@' not in body:
        body = _decode(body)
    if '@' not in body:
        raise ValueError('SS 链接缺少服务器地址。')
    userinfo, hostpart = body.rsplit('@', 1)
    userinfo = unquote(userinfo)
    if ':' not in userinfo:
        userinfo = _decode(userinfo)
    method, sep, password = userinfo.partition(':')
    try:
        address = urlsplit('//' + hostpart.rstrip('/'))
        hostname, port = address.hostname, address.port
    except ValueError as error:
        raise ValueError('SS 链接的服务器或端口不正确。') from error
    if not hostname or not port or address.username or address.path or address.query:
        raise ValueError('SS 链接需要服务器地址和 1–65535 的端口。')
    if not sep or not password or method not in METHODS:
        raise ValueError('SS 链接的密码为空，或加密方式暂不支持。')
    if any(char.isspace() for char in hostname):
        raise ValueError('SS 服务器地址不能包含空格。')
    return {'name': unquote(fragment)[:100] or hostname, 'server': hostname,
            'server_port': port, 'method': method, 'password': password}


def parse_vless_link(value):
    """Parse TCP TLS / REALITY links without silently dropping connection options."""
    value = str(value or '').strip()
    if not value.startswith('vless://') or len(value) > 8192 or any(c.isspace() for c in value):
        raise ValueError('请重新复制完整的 vless:// 单节点分享链接。')
    try:
        address = urlsplit(value)
        hostname, port = address.hostname, address.port
        user_id = str(uuid.UUID(unquote(address.username or '')))
        options = parse_qs(address.query, keep_blank_values=True, max_num_fields=40)
    except (ValueError, AttributeError) as error:
        raise ValueError('VLESS 链接的 UUID、地址或端口不完整，请重新复制节点。') from error
    if not hostname or not port or address.password is not None or address.path not in ('', '/'):
        raise ValueError('VLESS 链接需要 UUID、服务器地址和 1–65535 的端口。')
    if any(len(values) != 1 for values in options.values()):
        raise ValueError('VLESS 链接包含重复参数，请重新导出节点链接。')
    params = {key: values[0] for key, values in options.items()}
    supported = {'type', 'security', 'encryption', 'flow', 'sni', 'fp', 'pbk', 'sid',
                 'alpn', 'headerType', 'spx', 'allowInsecure', 'insecure'}
    if any(key not in supported and value for key, value in params.items()):
        raise ValueError('此 VLESS 链接含暂不支持的附加选项。请用已有代理客户端连接，再填写 SOCKS5。')
    if params.get('type', 'tcp') not in {'tcp', 'raw', 'none'} or params.get('headerType', 'none') != 'none':
        raise ValueError('目前支持 VLESS 的 TCP / Vision 节点。其他传输方式请通过代理客户端的 SOCKS5 使用。')
    if params.get('encryption', 'none') != 'none':
        raise ValueError('此 VLESS 加密选项暂不支持，请通过已有客户端的 SOCKS5 使用。')
    if params.get('allowInsecure', '0') not in {'', '0', 'false'} or params.get('insecure', '0') not in {'', '0', 'false'}:
        raise ValueError('此节点要求跳过证书校验。请使用有有效证书的节点，或在已有客户端中配置后使用 SOCKS5。')
    if params.get('spx', '') not in {'', '/'}:
        raise ValueError('此 REALITY 节点有自定义附加设置，请通过已有客户端的 SOCKS5 使用。')
    security = params.get('security', 'none')
    if security not in {'tls', 'reality'}:
        raise ValueError('此入口支持启用 TLS 或 REALITY 的 VLESS 节点，请从客户端导出完整链接。')
    flow = params.get('flow', '')
    if flow not in {'', 'xtls-rprx-vision'}:
        raise ValueError('VLESS 流控暂不支持，请使用 Vision 节点或已有客户端的 SOCKS5。')
    server_name = params.get('sni', hostname)
    if not server_name or any(c.isspace() for c in server_name):
        raise ValueError('VLESS 的 TLS 服务器名称不完整，请重新复制节点链接。')
    tls = {'enabled': True, 'server_name': server_name}
    if params.get('alpn'):
        alpn = params['alpn'].split(',')
        if any(not item or len(item) > 255 or any(c.isspace() for c in item) for item in alpn):
            raise ValueError('VLESS 的 ALPN 参数不正确，请重新导出节点链接。')
        tls['alpn'] = alpn
    fingerprint = params.get('fp', 'chrome' if security == 'reality' else '')
    if security == 'reality' and not fingerprint:
        raise ValueError('REALITY 链接的 TLS 指纹为空，请重新导出完整节点。')
    if fingerprint:
        if fingerprint not in {'chrome', 'firefox', 'safari', 'ios', 'android', 'edge', '360', 'qq', 'random', 'randomized'}:
            raise ValueError('此节点的 TLS 指纹暂不支持，请通过已有客户端的 SOCKS5 使用。')
        tls['utls'] = {'enabled': True, 'fingerprint': fingerprint}
    if security == 'reality':
        public_key = params.get('pbk', '')
        short_id = params.get('sid', '')
        try:
            key_bytes = base64.b64decode(public_key + '=' * (-len(public_key) % 4), altchars=b'-_', validate=True)
        except ValueError as error:
            raise ValueError('REALITY 链接缺少有效的公钥，请重新导出完整节点。') from error
        if len(key_bytes) != 32 or not re.fullmatch(r'[A-Za-z0-9_-]{43}=?', public_key):
            raise ValueError('REALITY 链接缺少有效的公钥，请重新导出完整节点。')
        if not re.fullmatch(r'(?:[0-9a-fA-F]{2}){0,8}', short_id):
            raise ValueError('REALITY 的短 ID 不正确，请重新导出完整节点。')
        tls['reality'] = {'enabled': True, 'public_key': public_key.rstrip('='), 'short_id': short_id}
    elif params.get('pbk') or params.get('sid'):
        raise ValueError('链接的 TLS 与 REALITY 参数不一致，请重新导出节点。')
    return {'type': 'vless', 'name': unquote(address.fragment)[:100] or hostname,
            'server': hostname, 'server_port': port, 'uuid': user_id, 'flow': flow, 'tls': tls}


def parse_node_link(value):
    value = str(value or '').strip()
    if value.startswith('ss://'):
        return parse_ss_link(value)
    if value.startswith('vless://'):
        return parse_vless_link(value)
    raise ValueError('请粘贴 ss:// 或 vless:// 开头的单节点链接，不是订阅网址。')


def singbox_config(node, port):
    if node.get('type') in (None, 'ss'):
        outbound = {'type': 'shadowsocks', 'tag': 'node-out',
                    **{key: node[key] for key in ('server', 'server_port', 'method', 'password')}}
    else:
        kind = node['type']
        fields = {
            'vless': {'uuid', 'flow', 'tls', 'transport'},
            'vmess': {'uuid', 'security', 'alter_id', 'tls', 'transport'},
            'trojan': {'password', 'tls', 'transport'},
            'hysteria2': {'password', 'tls', 'obfs', 'up_mbps', 'down_mbps'},
            'tuic': {'uuid', 'password', 'tls', 'congestion_control', 'udp_relay_mode', 'zero_rtt_handshake'},
            'socks5': {'username', 'password', 'tls'}, 'http': {'username', 'password', 'tls'},
        }
        if kind not in fields:
            raise ValueError('节点协议暂不支持，请重新选择节点。')
        outbound = {'type': 'socks' if kind == 'socks5' else kind, 'tag': 'node-out',
                    **{key: node[key] for key in {'server', 'server_port'} | fields[kind] if key in node}}
        if kind == 'socks5':
            outbound['version'] = '5'
    return {'log': {'level': 'error'},
            'inbounds': [{'type': 'socks', 'tag': 'local', 'listen': '127.0.0.1', 'listen_port': port}],
            'outbounds': [outbound],
            'route': {'final': 'node-out'}}


@contextlib.contextmanager
def running_ss(node, port):
    """Own a proxy process; node credentials travel through a pipe only."""
    binary = binary_path()
    if not binary:
        raise ValueError('缺少节点运行组件。本项目 Docker 镜像已内置；Mac 可安装 sing-box，或改用 SOCKS5。')
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('127.0.0.1', port))
        except OSError as error:
            raise ValueError('本地代理端口已被占用，请关闭旧测试或改用 SOCKS5。') from error
    configuration = json.dumps(singbox_config(node, port)).encode()
    check = subprocess.run([binary, 'check', '-c', '/dev/stdin'], input=configuration,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10, check=False)
    if check.returncode:
        raise ValueError('节点配置未通过检查，请重新导出链接，或通过已有客户端的 SOCKS5 使用。')
    process = subprocess.Popen([binary, 'run', '-c', '/dev/stdin'], stdin=subprocess.PIPE,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        process.stdin.write(configuration)
        process.stdin.close()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise ValueError('节点组件启动失败，请检查本地端口或节点设置。')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    break
            except OSError:
                time.sleep(.05)
        else:
            raise ValueError('节点组件启动超时，请重试。')
        yield process
    finally:
        if process.stdin and not process.stdin.closed:
            process.stdin.close()
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)


def ensure_started(config):
    global _PROCESS, _RUNTIME_DIR
    ss = config.get('ss_proxy') or {}
    if not ss.get('enabled'):
        return
    with LOCK:
        if _PROCESS is not None and _PROCESS.poll() is None:
            return
        context = running_ss(ss['node'], LOCAL_PORT)
        process = context.__enter__()
        _RUNTIME_DIR = context
        _PROCESS = process


def stop():
    global _PROCESS, _RUNTIME_DIR
    with LOCK:
        if _RUNTIME_DIR is not None:
            _RUNTIME_DIR.__exit__(None, None, None)
        _PROCESS = _RUNTIME_DIR = None


def status(config):
    ss = config.get('ss_proxy') or {}
    node = ss.get('node') or {}
    return {'ss_installed': bool(binary_path()), 'ss_configured': bool(node),
            'ss_name': node.get('name', ''), 'telegram_configured': credentials_ready(config),
            'node_protocol': node.get('type', 'ss') if node else '',
            'mode': 'ss' if ss.get('enabled') else 'proxy' if config.get('proxy') else 'direct'}


def apply_settings(config, payload):
    mode = payload.get('network_mode')
    if mode is None:  # Preserve compatibility with existing config clients.
        return
    if mode not in {'direct', 'proxy', 'ss', 'link'}:
        raise ValueError('请选择直接连接、节点链接或 SOCKS5 / HTTP 代理。')
    previous = dict(config.get('ss_proxy') or {})
    if mode in {'ss', 'link'}:
        link = str(payload.get('node_link') or payload.get('ss_link') or '').strip()
        content = payload.get('node_content')
        if content:
            from module.node_import import select
            node = select(content, payload.get('node_index'))
        else:
            node = parse_node_link(link) if link else previous.get('node')
        if not node:
            raise ValueError('请粘贴完整节点链接，或导入 YAML / JSON 后选择节点。')
        config['ss_proxy'] = {'enabled': True, 'node': node}
        config['proxy'] = {'scheme': 'socks5', 'hostname': '127.0.0.1', 'port': LOCAL_PORT}
    else:
        previous['enabled'] = False
        config['ss_proxy'] = previous
        if mode == 'direct':
            config.pop('proxy', None)
    # An explicitly selected simple network must not retain hidden media exits.
    config.pop('media_proxy_pool', None)


def _check_http(proxy):
    proxies = {}
    if proxy:
        scheme = 'socks5h' if proxy.get('scheme') == 'socks5' else proxy.get('scheme', 'http')
        from urllib.parse import quote
        host = str(proxy['hostname'])
        if ':' in host:
            host = '[' + host + ']'
        auth = ''
        if proxy.get('username') or proxy.get('password'):
            auth = quote(str(proxy.get('username', '')), safe='') + ':' + quote(str(proxy.get('password', '')), safe='') + '@'
        url = f"{scheme}://{auth}{host}:{int(proxy['port'])}"
        proxies = {'http': url, 'https': url}
    with requests.Session() as session:
        session.trust_env = False
        response = session.get('https://api.telegram.org', proxies=proxies, timeout=(5, 8), allow_redirects=False)
    if response.status_code >= 500:
        raise ValueError('Telegram 服务暂时不可用，请稍后再试。')


def probe(config):
    try:
        ss = config.get('ss_proxy') or {}
        if ss.get('enabled'):
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            with running_ss(ss['node'], port):
                _check_http({'scheme': 'socks5', 'hostname': '127.0.0.1', 'port': port})
        else:
            _check_http(config.get('proxy'))
    except requests.RequestException as error:
        # Requests exceptions can contain proxy usernames/passwords; never echo them.
        raise ValueError('无法通过所选网络连接 Telegram。请检查节点是否可用、代理地址及端口。') from error
    return '网络可访问 Telegram。账号能否登录仍以 Telegram 登录结果为准。'


atexit.register(stop)
