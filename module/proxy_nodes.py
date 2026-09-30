"""Manage Shadowsocks outbound nodes for the telegram-proxy (sing-box) container.

The downloader's Telegram traffic rides through a separate sing-box container
on the NAS. When a proxy VPS runs out of monthly traffic, the operator adds a
backup node (ss:// share link) and switches over from the Web console:

  1. parse the ss:// link into server/port/method/password
  2. rewrite the shadowsocks outbound in the mounted sing-box config
  3. restart the telegram-proxy container over the mounted Docker socket

Both mounts are optional: on a dev Mac (neither present) every action fails
softly with a clear message instead of breaking the console.
"""

import base64
import http.client
import json
import os
import socket
import urllib.parse

from loguru import logger

SINGBOX_CONFIG = os.environ.get("TMD_SINGBOX_CONFIG", "/app/tgproxy/config.json")
DOCKER_SOCK = os.environ.get("TMD_DOCKER_SOCK", "/var/run/docker.sock")
PROXY_CONTAINER = os.environ.get("TMD_PROXY_CONTAINER", "telegram-proxy")


def _b64decode_loose(value: str) -> str:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)).decode()


def parse_ss_link(link: str) -> dict:
    """Parse an ss:// share link (SIP002 plain or base64 userinfo variants)."""
    link = str(link or "").strip()
    if not link.startswith("ss://"):
        raise ValueError("仅支持 ss:// 格式的分享链接")
    body = link[5:]
    fragment = ""
    if "#" in body:
        body, fragment = body.rsplit("#", 1)
    if "?" in body:
        body = body.split("?", 1)[0]
    if "@" not in body:
        body = _b64decode_loose(body)  # legacy: whole body base64
    if "@" not in body:
        raise ValueError("链接缺少服务器地址部分")
    userinfo, hostpart = body.rsplit("@", 1)
    userinfo = urllib.parse.unquote(userinfo)
    if ":" not in userinfo:
        try:
            userinfo = _b64decode_loose(userinfo)
        except Exception as error:
            raise ValueError("无法解析加密方式与密码") from error
    if ":" not in userinfo:
        raise ValueError("无法解析加密方式与密码")
    method, password = userinfo.split(":", 1)
    if ":" not in hostpart:
        raise ValueError("链接缺少端口")
    host, port_text = hostpart.rsplit(":", 1)
    try:
        port = int(port_text)
    except ValueError as error:
        raise ValueError("端口不是数字") from error
    if not host or not (0 < port < 65536):
        raise ValueError("服务器地址或端口不正确")
    name = urllib.parse.unquote(fragment).strip() if fragment else f"{host}:{port}"
    return {
        "name": name,
        "server": host,
        "server_port": port,
        "method": method,
        "password": password,
    }


def available() -> bool:
    """Whether the sing-box config is reachable from this process."""
    return os.path.exists(SINGBOX_CONFIG)


def active_node() -> dict:
    """Return the shadowsocks outbound currently live in sing-box."""
    if not available():
        return {}
    try:
        with open(SINGBOX_CONFIG, encoding="utf-8") as config_file:
            config = json.load(config_file)
        for outbound in config.get("outbounds") or []:
            if outbound.get("type") == "shadowsocks":
                return {
                    "server": outbound.get("server"),
                    "server_port": outbound.get("server_port"),
                    "method": outbound.get("method"),
                }
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(f"读取 sing-box 配置失败：{error}")
    return {}


def _restart_proxy_container() -> None:
    """POST /containers/<name>/restart on the mounted Docker socket."""

    class _UnixHTTPConnection(http.client.HTTPConnection):
        def __init__(self, sock_path):
            super().__init__("localhost")
            self._sock_path = sock_path

        def connect(self):
            unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            unix_socket.settimeout(30)
            unix_socket.connect(self._sock_path)
            self.sock = unix_socket

    connection = _UnixHTTPConnection(DOCKER_SOCK)
    try:
        connection.request(
            "POST", f"/containers/{PROXY_CONTAINER}/restart?t=5"
        )
        response = connection.getresponse()
        response.read()
        if response.status not in (204, 200):
            raise RuntimeError(f"Docker API 返回 {response.status}")
    finally:
        connection.close()


def switch_node(node: dict) -> None:
    """Point the sing-box shadowsocks outbound at `node` and restart telegram-proxy.

    The config write is atomic-ish (temp + replace in the same directory) so a
    crash mid-write cannot leave sing-box with half a JSON file.
    """
    if not available():
        raise RuntimeError(
            "sing-box 配置不可见：容器缺少 /app/tgproxy 挂载，无法切换节点"
        )
    with open(SINGBOX_CONFIG, encoding="utf-8") as config_file:
        config = json.load(config_file)
    replaced = False
    for outbound in config.get("outbounds") or []:
        if outbound.get("type") == "shadowsocks":
            outbound["server"] = node["server"]
            outbound["server_port"] = int(node["server_port"])
            outbound["method"] = node["method"]
            outbound["password"] = node["password"]
            replaced = True
            break
    if not replaced:
        raise RuntimeError("sing-box 配置里没有 shadowsocks 出站，无法切换")
    temp_path = SINGBOX_CONFIG + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as config_file:
        json.dump(config, config_file, ensure_ascii=False, indent=2)
    os.replace(temp_path, SINGBOX_CONFIG)
    if not os.path.exists(DOCKER_SOCK):
        raise RuntimeError(
            "配置已写入，但 Docker 套接字不可见，请手动重启 telegram-proxy 容器"
        )
    _restart_proxy_container()
    logger.success(
        "代理节点已切换为 {}:{}，telegram-proxy 已重启",
        node["server"],
        node["server_port"],
    )
