"""web ui for media download"""

import logging
import asyncio
import os
import hashlib
import csv
import copy
import tempfile
import re
import secrets
import signal
import io
import json
import shutil
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from typing import Any, Optional

from flask import (
    Flask,
    Response,
    jsonify,
    render_template,
    redirect,
    url_for,
    request,
    send_file,
    session,
)
from flask_login import (
    LoginManager,
    UserMixin,
    login_required,
    login_user,
    logout_user,
)
from flask.sessions import SecureCookieSessionInterface
from loguru import logger
from ruamel import yaml
import qrcode

import utils
from module.app import Application
from module import batch_queue
from module import remote_access
from module import network_setup
from module import local_storage
from module import proxy_nodes
from module import rate_governor
from module import speed_governor
from module.download_history import list_downloads
from module.download_tasks import (
    channel_counts,
    confirm_import_batch,
    create_import_batch,
    get_import_batch,
    get_channel_configs,
    list_channel_files,
    list_channel_state_events,
    list_channels,
    list_import_batches,
    list_import_items,
    migrate_config_channels,
    list_tasks,
    request_cancel,
    request_retry,
    refresh_completed_channels,
    set_channel_enabled,
    set_channels_paused,
    task_counts,
    undo_import_batch,
    upsert_channel_config,
)
from module.download_stat import (
    DownloadState,
    get_download_snapshot,
    get_download_state,
    get_total_download_speed,
    set_channel_download_paused,
    set_download_state,
)
from module import config_io
from utils.format import format_byte
from utils.platform import reveal_path

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

_flask_app = Flask(__name__)


class TransportSessionInterface(SecureCookieSessionInterface):
    def get_cookie_secure(self, app):
        return _secure_request()


_flask_app.session_interface = TransportSessionInterface()

_channel_api_cache = {}
_channel_api_cache_lock = threading.Lock()
_channel_api_refreshing = set()
_channel_api_cache_generation = 0
_CHANNEL_API_CACHE_TTL = 120.0
_CHANNEL_REFRESH_QUEUE_LIMIT = 8
_channel_refresh_executor = ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="channel-dashboard"
)
_persisted_stats_cache_lock = threading.Lock()
_persisted_stats_cache = {"updated_at": 0.0, "value": None}
_persisted_stats_refreshing = False
_PERSISTED_STATS_CACHE_TTL = 60.0
_persisted_stats_executor = ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="persisted-stats"
)
_live_event_cache_lock = threading.Lock()
_live_event_cache = {"updated_at": 0.0, "value": None}
_LIVE_EVENT_CACHE_TTL = 0.75
_system_status_cache_lock = threading.Lock()
_system_status_cache = {"updated_at": 0.0, "value": None}
_SYSTEM_STATUS_CACHE_TTL = 2.0

_flask_app.secret_key = secrets.token_hex(32)
_flask_app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)
_login_manager = LoginManager()
_login_manager.login_view = "login"
_login_manager.init_app(_flask_app)
web_login_users: dict = {}


def _refresh_persisted_stats() -> None:
    """Refresh expensive task/history aggregates outside request handling."""
    global _persisted_stats_refreshing  # pylint: disable=global-statement
    try:
        value = {
            "tasks": task_counts(),
            "channels": channel_counts(),
        }
        with _persisted_stats_cache_lock:
            _persisted_stats_cache.update(
                {"updated_at": time.monotonic(), "value": value}
            )
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(f"持久化统计后台刷新失败，继续使用旧数据: {error}")
    finally:
        with _persisted_stats_cache_lock:
            _persisted_stats_refreshing = False


def _cached_persisted_stats() -> dict:
    """Return cached aggregates and schedule at most one slow refresh.

    The dashboard polls every few seconds, but the underlying aggregates scan
    all task/history rows.  Returning the last snapshot keeps those reads out
    of the request path and prevents the UI from competing with media writes.
    """
    global _persisted_stats_refreshing  # pylint: disable=global-statement
    now = time.monotonic()
    with _persisted_stats_cache_lock:
        cached = _persisted_stats_cache["value"]
        stale = (
            cached is None
            or now - _persisted_stats_cache["updated_at"]
            >= _PERSISTED_STATS_CACHE_TTL
        )
        if stale and not _persisted_stats_refreshing:
            _persisted_stats_refreshing = True
            _persisted_stats_executor.submit(_refresh_persisted_stats)
        if cached is not None:
            return cached
    return {
        "tasks": {
            "queued": 0,
            "downloading": 0,
            "retrying": 0,
            "retry_requested": 0,
            "completed": 0,
            "skipped": 0,
            "failed": 0,
            "cancelled": 0,
            "paused": 0,
            "active": 0,
        },
        "channels": {
            "total": 0,
            "downloading": 0,
            "waiting": 0,
            "failed": 0,
            "paused": 0,
            "disabled": 0,
        },
    }

web_application: Optional[Application] = None
service_started_at = time.time()
login_attempts: dict = {}
_login_attempt_lock = threading.Lock()
_config_lock = threading.Lock()
_storage_probe_lock = threading.Lock()
storage_probe = {
    "path": "",
    "writable": None,
    "message": "尚未检测写入",
    "checked_at": "",
}
storage_host_cache = {"host": "", "checked_at": 0.0, "online": False}
_storage_host_lock = threading.Lock()
_yaml = yaml.YAML()
_yaml.indent(mapping=2, sequence=4, offset=2)


_flask_app.config.update(SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax", MAX_CONTENT_LENGTH=2 * 1024 * 1024)


@_flask_app.before_request
def require_secure_transport():
    """Plain HTTP is limited to a loopback URL on the same device."""
    peer = request.remote_addr or ""
    host = urlsplit("//" + request.host).hostname
    if peer in {"127.0.0.1", "::1"} and host in {"127.0.0.1", "localhost", "::1"}:
        return None
    if _secure_request():
        return None
    return jsonify({"ok": False, "message": "远程访问请使用 HTTPS；本机请打开 http://127.0.0.1。"}), 426


def _secure_request():
    if request.is_secure:
        return True
    trusted = (getattr(web_application, "config", {}) or {}).get("web_trusted_proxies") or []
    return isinstance(trusted, list) and request.remote_addr in trusted and request.headers.get("X-Forwarded-Proto", "").lower() == "https"


@_flask_app.before_request
def reject_cross_origin_changes():
    """Block browser cross-site writes, including loopback console attacks."""
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        origin = request.headers.get("Origin")
        expected_origin = ("https://" + request.host) if _secure_request() else request.host_url.rstrip("/")
        if (origin and origin.rstrip("/") != expected_origin.rstrip("/")) or request.headers.get("Sec-Fetch-Site") == "cross-site":
            return jsonify({"ok": False, "message": "已拒绝跨站请求，请从下载器页面重试。"}), 403
    return None


@_flask_app.after_request
def disable_dynamic_response_cache(response):
    """Prevent browsers and mobile webviews from reusing stale runtime state."""
    if not request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "no-referrer"
    if _secure_request():
        # Flask writes its session cookie after after_request handlers.
        # A custom session interface below supplies the Secure flag per request.
        response.headers["Strict-Transport-Security"] = "max-age=31536000"
    return response

SECRET_FIELDS = ("api_hash", "bot_token", "web_login_secret")
MEDIA_TYPES = {
    "audio",
    "document",
    "photo",
    "video",
    "voice",
    "video_note",
    "animation",
}
PATH_PREFIXES = {"chat_title", "media_datetime", "media_type"}
FILE_NAME_PREFIXES = {"message_id", "file_name", "caption"}
LANGUAGES = {"EN", "ZH", "RU", "UA"}
PROXY_SCHEMES = {"socks4", "socks5", "http"}
TASK_STATUSES = {
    "queued",
    "downloading",
    "retrying",
    "retry_requested",
    "completed",
    "skipped",
    "failed",
    "cancelled",
    "paused",
}
CHANNEL_STATUSES = {
    "validating",
    "ready",
    "scanning",
    "queued",
    "downloading",
    "backoff",
    "paused",
    "blocked",
    "completed",
    "disabled",
}
LOGIN_ATTEMPT_LIMIT = 5
LOGIN_ATTEMPT_WINDOW = 300
TAILSCALE_CACHE_SECONDS = 30
tailscale_cache = {"updated_at": 0.0, "value": {}}
_tailscale_lock = threading.Lock()


class User(UserMixin):
    """Web Login User"""

    def __init__(self):
        self.sid = "root"

    @property
    def id(self):
        """ID"""
        return self.sid


@_login_manager.user_loader
def load_user(_):
    """
    Load a user object from the user ID.

    Returns:
        User: The user object.
    """
    return User()


def get_flask_app() -> Flask:
    """get flask app instance"""
    return _flask_app


def run_web_server(app: Application):
    """
    Runs a web server using the Flask framework.
    """

    _serve_app(app.web_host, app.web_port, debug=app.debug_web)


def _serve_app(host: str, port: int, debug: bool = False) -> None:
    """Serve the console with waitress (production) when available.

    Falls back to Flask's threaded dev server. waitress is a pure-Python WSGI
    server -- installs cleanly on macOS/Linux/Alpine and handles multiple
    phones/desktops far better than the dev server.
    """
    flask_app = get_flask_app()
    if not debug:
        try:
            from waitress import serve as _waitress_serve  # pylint: disable=import-outside-toplevel

            _waitress_serve(flask_app, host=host, port=int(port), threads=16)
            return
        except ImportError:
            log.warning("waitress 未安装，回退到 Flask 内置服务器（建议 pip install waitress）")
    flask_app.run(host, int(port), debug=debug, use_reloader=False, threaded=True)


# pylint: disable = W0603
def init_web(app: Application):
    """
    Set the value of the users variable.

    Args:
        users: The list of users to set.

    Returns:
        None.
    """
    global web_application, web_login_users, service_started_at
    web_application = app
    service_started_at = time.time()
    _flask_app.secret_key = secrets.token_bytes(32)
    if app.config.get("start_paused", False):
        set_download_state(DownloadState.StopDownload)
    if app.web_login_secret:
        web_login_users = {"root": app.web_login_secret}
    else:
        raise ValueError("请先运行 scripts/init_config.py，设置独立的 Web 访问密码。")
    if len(app.web_login_secret) < 16 or app.web_login_secret == "CHANGE_ME":
        raise ValueError("Web 访问密码至少需要 16 位，请修改 config.yaml 后重启。")
    _flask_app.config["LOGIN_DISABLED"] = False
    migrate_config_channels(app.config.get("chat") or [])
    set_channel_download_paused(
        [row["chat_id"] for row in get_channel_configs(False) if row["paused"]],
        True,
    )
    if app.debug_web:
        threading.Thread(target=run_web_server, args=(app,)).start()
    else:
        threading.Thread(
            target=lambda: _serve_app(app.web_host, app.web_port),
            daemon=True,
        ).start()
    # Optionally bring up a Cloudflare Tunnel so the console is reachable from
    # any phone/computer over HTTPS -- no Tailscale, no port forwarding.
    try:
        remote_access.ensure_started(app.config, app.web_port)
    except Exception as error:  # pylint: disable=broad-except
        log.warning("cloudflared 启动失败: %s", error)


def _storage_status() -> dict:
    """Return the configured download path and its current disk health."""
    save_path = (
        Path(web_application.save_path).expanduser()
        if web_application
        else Path()
    )
    available = save_path.is_dir()
    nas_host = str(
        (web_application.config.get("nas_host") if web_application else "") or ""
    ).strip()
    host_online = _storage_host_online(nas_host) if nas_host and not available else available
    mount_state = "mounted" if available else ("unmounted" if host_online else "offline")
    storage = {
        "path": str(save_path),
        "available": available,
        "host": nas_host,
        "host_online": host_online,
        "mount_state": mount_state,
        "mount_url_configured": bool(
            (web_application.config.get("nas_mount_url") if web_application else "")
        ),
        "free": 0,
        "total": 0,
        "writable": None,
        "probe_message": "尚未检测写入",
        "probe_checked_at": "",
    }
    with _storage_probe_lock:
        if storage_probe["path"] == str(save_path):
            storage.update(
                {
                    "writable": storage_probe["writable"],
                    "probe_message": storage_probe["message"],
                    "probe_checked_at": storage_probe["checked_at"],
                }
            )
    if available:
        usage = shutil.disk_usage(save_path)
        storage.update({"free": usage.free, "total": usage.total})
    return storage


def _storage_host_online(host: str) -> bool:
    """Probe the NAS hostname without treating an unmounted share as offline."""
    now = time.time()
    with _storage_host_lock:
        if (
            storage_host_cache["host"] == host
            and now - storage_host_cache["checked_at"] < 10
        ):
            return bool(storage_host_cache["online"])
        try:
            socket.getaddrinfo(host, 445, type=socket.SOCK_STREAM)
            online = True
        except socket.gaierror:
            online = False
        storage_host_cache.update(
            {"host": host, "checked_at": now, "online": online}
        )
        return online


@_flask_app.route("/api/mount_storage", methods=["POST"])
@login_required
def mount_configured_storage():
    """Ask Finder to mount the configured SMB share using macOS credentials."""
    mount_url = str(
        (web_application.config.get("nas_mount_url") if web_application else "") or ""
    ).strip()
    if not mount_url.lower().startswith("smb://"):
        return jsonify({"ok": False, "message": "尚未配置 SMB 挂载地址"}), 400
    if sys.platform != "darwin":
        return (
            jsonify(
                {
                    "ok": False,
                    "message": "非 macOS 环境请在系统层挂载共享（Docker 用 volume 映射，Linux 用 mount.cifs/autofs）",
                }
            ),
            400,
        )
    try:
        subprocess.Popen(
            ["open", mount_url],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        return jsonify({"ok": False, "message": f"无法打开 Finder：{exc}"}), 500
    return jsonify(
        {
            "ok": True,
            "message": "已请求 Finder 挂载共享目录，请在系统窗口完成登录",
        }
    )


@_flask_app.route("/api/open_download_folder", methods=["POST"])
@login_required
def open_download_folder():
    """Open the containing Finder folder for a downloaded file."""
    payload = request.get_json(silent=True) or {}
    requested_path = str(payload.get("path") or "").strip()
    if not requested_path:
        return jsonify({"ok": False, "message": "缺少下载文件路径"}), 400
    if web_application is None or not web_application.save_path:
        return jsonify({"ok": False, "message": "尚未配置下载目录"}), 400

    root = Path(web_application.save_path).expanduser().resolve()
    target = Path(requested_path).expanduser().resolve()
    try:
        target.relative_to(root)
    except ValueError:
        return jsonify({"ok": False, "message": "只能打开当前下载目录内的文件"}), 403

    folder = target if target.is_dir() else target.parent
    if not folder.is_dir():
        return jsonify({"ok": False, "message": "下载目录不存在，文件可能已被移动"}), 404
    ok, message = reveal_path(str(folder))
    if not ok:
        return jsonify({"ok": False, "message": message}), 500
    return jsonify({"ok": True, "message": message, "folder": str(folder)})


def _set_storage_probe(path: Path, writable: bool, message: str) -> None:
    """Remember the latest explicit write probe for the configured storage."""
    with _storage_probe_lock:
        storage_probe.update(
            {
                "path": str(path),
                "writable": writable,
                "message": message,
                "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            }
        )


def _tailscale_cli() -> Optional[str]:
    """Locate the Tailscale CLI installed on this machine."""
    candidates = (
        shutil.which("tailscale"),
        "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
    )
    return next((path for path in candidates if path and Path(path).is_file()), None)


def _tailscale_status(force: bool = False) -> dict:
    """Return a short, cached Tailscale and Serve status payload."""
    now = time.time()
    with _tailscale_lock:
        if (
            not force
            and tailscale_cache["value"]
            and now - tailscale_cache["updated_at"] < TAILSCALE_CACHE_SECONDS
        ):
            return dict(tailscale_cache["value"])

        value = {
            "installed": False,
            "connected": False,
            "serve_enabled": False,
            "ipv4": "",
            "url": "",
        }
        cli = _tailscale_cli()
        if not cli:
            tailscale_cache.update({"updated_at": now, "value": value})
            return dict(value)

        value["installed"] = True
        try:
            status_result = subprocess.run(
                [cli, "status", "--json"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
            if status_result.returncode == 0:
                status = json.loads(status_result.stdout)
                value["connected"] = status.get("BackendState") == "Running"
                addresses = status.get("TailscaleIPs") or []
                value["ipv4"] = next(
                    (address for address in addresses if ":" not in address), ""
                )

            serve_result = subprocess.run(
                [cli, "serve", "status", "--json"],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
            if serve_result.returncode == 0 and serve_result.stdout.strip():
                serve = json.loads(serve_result.stdout)
                web_hosts = serve.get("Web") or {}
                if web_hosts:
                    host = next(iter(web_hosts)).split(":", 1)[0]
                    value["serve_enabled"] = True
                    value["url"] = f"https://{host}/"
        except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError):
            pass

        tailscale_cache.update({"updated_at": now, "value": value})
        return dict(value)


def _get_config_path() -> Path:
    """Return the configured YAML path for the running web application."""
    if web_application is None:
        raise RuntimeError("Web application is not initialized")

    config_path = Path(web_application.config_file)
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path
    return config_path.resolve()


def _read_config() -> Any:
    """Load the YAML configuration while preserving comments and key order."""
    config_path = _get_config_path()
    if not config_path.exists():
        return yaml.comments.CommentedMap()

    return config_io.load(config_path, _yaml) or yaml.comments.CommentedMap()


def _write_config(config: Any) -> None:
    """Persist atomically; never downgrade encrypted or mounted files to unsafe writes."""
    config_io.dump(_get_config_path(), config, _yaml)


def _persist_download_state(paused: bool) -> None:
    """Persist the pause state so restarts do not unexpectedly resume downloads."""
    if web_application is not None:
        web_application.config["start_paused"] = paused
    with _config_lock:
        config = _read_config()
        config["start_paused"] = paused
        _write_config(config)


def _persist_remote_access(enabled: bool) -> None:
    """Persist whether the Cloudflare Tunnel should auto-start on restart."""
    if web_application is not None:
        remote_cfg = dict(web_application.config.get("remote_access") or {})
        remote_cfg["enabled"] = enabled
        remote_cfg.setdefault("provider", "cloudflared")
        web_application.config["remote_access"] = remote_cfg
    with _config_lock:
        config = _read_config()
        remote_cfg = dict(config.get("remote_access") or {})
        remote_cfg["enabled"] = enabled
        remote_cfg.setdefault("provider", "cloudflared")
        config["remote_access"] = remote_cfg
        _write_config(config)


def _integer(value: Any, field: str, minimum: int, maximum: int) -> int:
    """Parse and validate an integer field from the web form."""
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} 必须是整数") from exc

    if not minimum <= result <= maximum:
        raise ValueError(f"{field} 必须在 {minimum} 到 {maximum} 之间")
    return result


def _string_list(value: Any, field: str, allowed: set) -> list:
    """Validate a list of supported string values."""
    if not isinstance(value, list):
        raise ValueError(f"{field} 格式不正确")
    result = [str(item).strip() for item in value if str(item).strip()]
    if any(item not in allowed for item in result):
        raise ValueError(f"{field} 包含不支持的选项")
    return result


def _public_config(config: Any) -> dict:
    """Return the editable configuration without exposing stored secrets."""
    result = copy.deepcopy(dict(config))
    proxy = dict(result.get("proxy") or {})
    secret_status = {field: bool(result.get(field)) for field in SECRET_FIELDS}
    secret_status["proxy_password"] = bool(proxy.get("password"))

    for field in SECRET_FIELDS:
        result[field] = ""
    proxy["password"] = ""
    proxy["enabled"] = bool(result.get("proxy"))
    result["proxy"] = proxy
    result.pop("chat", None)
    result["channel_storage"] = "sqlite"
    def redact(value):
        if isinstance(value, dict):
            for key in list(value):
                if re.search(r"(?i)password|token|secret|api_hash|session_string", key):
                    value[key] = ""
                else:
                    redact(value[key])
        elif isinstance(value, list):
            for item in value:
                redact(item)
    redact(result)
    result.pop("proxy_nodes", None)
    # Node UUIDs and REALITY credentials must never be returned to the browser.
    # A display name and protocol are supplied separately in network_status.
    result.pop("ss_proxy", None)
    result["network_status"] = network_setup.status(config)
    result["secret_status"] = secret_status
    secret_status["api_hash"] = network_setup.credentials_ready(config)
    return result


def _merge_config(config: Any, payload: dict) -> Any:
    """Apply validated editable fields while preserving unknown YAML keys."""
    if not isinstance(payload, dict):
        raise ValueError("配置数据格式不正确")

    api_id = str(payload.get("api_id", "")).strip()
    if api_id:
        config["api_id"] = _integer(api_id, "api_id", 1, 2_147_483_647)

    for field in SECRET_FIELDS:
        value = str(payload.get(field, "")).strip()
        if value:
            if field == "api_hash" and not re.fullmatch(r"[0-9a-fA-F]{32}", value):
                raise ValueError("API Hash 应为 Telegram 官方页面提供的 32 位字符串。")
            config[field] = value

    config.pop("chat", None)
    config["channel_storage"] = "sqlite"

    media_types = _string_list(payload.get("media_types"), "media_types", MEDIA_TYPES)
    if not media_types and not bool(payload.get("enable_download_txt", config.get("enable_download_txt", False))):
        raise ValueError("请选择至少一种下载内容，也可以只选择文字。")
    config["media_types"] = media_types

    formats = payload.get("file_formats")
    if not isinstance(formats, dict):
        raise ValueError("file_formats 格式不正确")
    config["file_formats"] = {
        media_type: [
            item.strip().lower()
            for item in str(formats.get(media_type, "all")).split(",")
            if item.strip()
        ]
        or ["all"]
        for media_type in ("audio", "document", "video")
    }

    save_path = str(payload.get("save_path", "")).strip()
    if save_path:
        config["save_path"] = os.path.expanduser(save_path)

    config["file_path_prefix"] = _string_list(
        payload.get("file_path_prefix", []), "file_path_prefix", PATH_PREFIXES
    )
    config["file_name_prefix"] = _string_list(
        payload.get("file_name_prefix", []), "file_name_prefix", FILE_NAME_PREFIXES
    )
    config["file_name_prefix_split"] = str(
        payload.get("file_name_prefix_split", " - ")
    )
    config["max_download_task"] = _integer(
        payload.get("max_download_task", 5), "max_download_task", 1, 64
    )
    config["max_concurrent_transmissions"] = _integer(
        payload.get("max_concurrent_transmissions", 25),
        "max_concurrent_transmissions",
        1,
        320,
    )

    language = str(payload.get("language", "EN")).upper()
    if language not in LANGUAGES:
        raise ValueError("language 选项不正确")
    config["language"] = language
    config["hide_file_name"] = bool(payload.get("hide_file_name"))
    config["enable_download_txt"] = bool(payload.get("enable_download_txt"))
    config["keep_service_alive"] = bool(payload.get("keep_service_alive"))

    date_format = str(payload.get("date_format", "%Y_%m")).strip() or "%Y_%m"
    try:
        datetime.now().strftime(date_format)
    except (TypeError, ValueError) as exc:
        raise ValueError("date_format 格式不正确") from exc
    config["date_format"] = date_format

    web_host = str(payload.get("web_host", "127.0.0.1")).strip()
    if not web_host:
        raise ValueError("web_host 不能为空")
    config["web_host"] = web_host
    config["web_port"] = _integer(payload.get("web_port", 5000), "web_port", 1, 65535)

    proxy = payload.get("proxy") or {}
    if bool(proxy.get("enabled")):
        scheme = str(proxy.get("scheme", "socks5")).lower()
        if scheme not in PROXY_SCHEMES:
            raise ValueError("代理类型不支持")
        hostname = str(proxy.get("hostname", "")).strip()
        if not hostname:
            raise ValueError("启用代理后必须填写代理地址")
        previous_proxy = dict(config.get("proxy") or {})
        next_proxy = {
            "scheme": scheme,
            "hostname": hostname,
            "port": _integer(proxy.get("port"), "代理端口", 1, 65535),
        }
        username = str(proxy.get("username", "")).strip()
        password = str(proxy.get("password", "")).strip()
        if username:
            next_proxy["username"] = username
        if password:
            next_proxy["password"] = password
        elif previous_proxy.get("password"):
            next_proxy["password"] = previous_proxy["password"]
        config["proxy"] = next_proxy
    else:
        config.pop("proxy", None)

    network_setup.apply_settings(config, payload)
    return config


def _login_client_key() -> str:
    """Throttle the actual TCP peer; untrusted forwarded headers are ignored."""
    return request.remote_addr or "unknown"


def _recent_login_attempts(client_key: str) -> list:
    """Discard expired failed-login timestamps and return the active window."""
    cutoff = time.time() - LOGIN_ATTEMPT_WINDOW
    attempts = [stamp for stamp in login_attempts.get(client_key, []) if stamp >= cutoff]
    login_attempts[client_key] = attempts
    return attempts


@_flask_app.route("/login", methods=["GET", "POST"])
def login():
    """
    Function to handle the login route.

    Parameters:
    - No parameters

    Returns:
    - If the request method is "POST" and the username and
      password match the ones in the web_login_users dictionary,
      it returns a JSON response with a code of "1".
    - Otherwise, it returns a JSON response with a code of "0".
    - If the request method is not "POST", it returns the rendered "login.html" template.
    """
    if request.method == "POST":
        client_key = _login_client_key()
        with _login_attempt_lock:
            attempts = _recent_login_attempts(client_key)
            if len(attempts) >= LOGIN_ATTEMPT_LIMIT:
                return jsonify({"code": "0", "message": "尝试次数过多，请五分钟后再试"}), 429
            attempts.append(time.time())  # Reserve before verification, including malformed requests.
        password = request.form.get("password", "")
        expected = web_login_users.get("root", "")
        if not password or len(password) > 4096:
            return jsonify({"code": "0", "message": "请输入安装时设置的访问密码。"}), 400
        if expected and secrets.compare_digest(expected.encode("utf-8"), password.encode("utf-8")):
            login_user(User())
            session.permanent = True
            with _login_attempt_lock:
                login_attempts.pop(client_key, None)
            return jsonify({"code": "1"})
        return jsonify({"code": "0", "message": "访问密码不正确。"}), 401

    return render_template("login.html")


@_flask_app.route("/logout", methods=["POST"])
@login_required
def logout():
    """End the current Web console session."""
    logout_user()
    return jsonify({"ok": True})


@_flask_app.route("/")
@login_required
def index():
    """Index html"""
    return render_template(
        "index.html",
        download_state=(
            "pause" if get_download_state() is DownloadState.Downloading else "continue"
        ),
    )


@_flask_app.route("/api/batch/summary")
@login_required
def web_batch_summary():
    """Return batch-download progress counters."""
    return jsonify(batch_queue.summary())


@_flask_app.route("/api/batch/plan")
@login_required
def web_batch_plan():
    """Return the persisted current batch and all of its 20 rows."""
    batch = batch_queue.current_batch()
    records = batch.pop("records", [])
    return jsonify({"batch": batch, "records": records})


@_flask_app.route("/api/batch/current")
@login_required
def web_batch_current():
    """Explicit alias used by the current-batch console."""
    return web_batch_plan()


@_flask_app.route("/api/batch/next", methods=["POST"])
@login_required
def web_batch_next():
    """Verify unresolved rows from the fixed current batch."""
    dispatched = batch_queue.dispatch_next()
    return jsonify(
        {
            "dispatched": len(dispatched),
            "records": dispatched,
            "batch": batch_queue.current_batch(),
        }
    )


@_flask_app.route("/api/batch/retry", methods=["POST"])
@login_required
def web_batch_retry():
    """Retry one current-batch row, or every transient failure."""
    data = request.get_json(silent=True) or {}
    order_no = data.get("order_no")
    try:
        order_no = None if order_no in (None, "") else int(order_no)
    except (TypeError, ValueError):
        return jsonify({"error": "频道序号不正确"}), 400
    records = batch_queue.retry_failed(order_no)
    return jsonify({"retried": len(records), "records": records})


@_flask_app.route("/api/batch/skip", methods=["POST"])
@login_required
def web_batch_skip():
    """Skip one unresolved row in the fixed current batch."""
    try:
        order_no = int((request.get_json(silent=True) or {}).get("order_no"))
    except (TypeError, ValueError):
        return jsonify({"error": "频道序号不正确"}), 400
    changed = batch_queue.skip_item(order_no)
    if not changed:
        return jsonify({"error": "该频道当前不能跳过，请刷新页面后重试"}), 409
    return jsonify({"skipped": 1, "order_no": order_no})


@_flask_app.route("/api/batch/advance", methods=["POST"])
@login_required
def web_batch_advance():
    """Advance only after the current batch is fully imported or skipped."""
    try:
        batch = batch_queue.advance_batch()
    except ValueError as error:
        return jsonify({"error": str(error)}), 409
    return jsonify({"batch": batch})


@_flask_app.route("/batch")
@login_required
def batch_page():
    """Keep existing bookmarks working inside the unified console."""
    return redirect(url_for("index", view="imports"), code=302)


def _runtime_blocker():
    if getattr(web_application, "account_logging_out", False):
        return "正在退出账号，请稍候。"
    if getattr(web_application, "preview_mode", False):
        return "当前是界面演示，未运行 Telegram 下载服务；保存设置不会启动下载。"
    if getattr(web_application, "restart_required", False):
        return "新设置已保存。请重启下载器，让 API、网络和保存位置生效后再开始。"
    if not getattr(web_application, "telegram_ready", False):
        return "Telegram 尚未登录。请在设置页输入手机号和验证码，完成 Telegram 登录。"
    return ""


@_flask_app.route("/api/setup/login", methods=["GET", "POST"])
@login_required
def telegram_browser_login():
    controller = getattr(web_application, "telegram_login", None)
    if request.method == "GET":
        if getattr(web_application, "telegram_ready", False):
            return jsonify({"stage": "ready", "message": "Telegram 已登录，可以添加频道了。"})
        if getattr(web_application, "preview_mode", False):
            return jsonify({"stage": "preview", "message": "此页面只演示界面。请打开实际下载器后登录。"})
        return jsonify(controller.status() if controller else {"stage": "waiting", "message": "请先保存 API 凭证并测试网络连接，再登录 Telegram。"})
    if not request.is_json:
        return jsonify({"ok": False, "message": "请求格式不正确。"}), 400
    if not controller or getattr(web_application, "preview_mode", False):
        return jsonify({"ok": False, "message": "登录服务尚未准备好。"}), 409
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "message": "请求格式不正确。"}), 400
    action, value = payload.get("action"), payload.get("value", "")
    if action not in {"phone", "code", "password", "resend"} or not isinstance(value, str) or (action != "resend" and not value) or len(value) > 256:
        return jsonify({"ok": False, "message": "请填写当前步骤需要的信息。"}), 400
    future = asyncio.run_coroutine_threadsafe(controller.submit(action, value), web_application.loop)
    try:
        result = future.result(timeout=30)
        return jsonify(result), 200 if result.get("ok") else 400
    except TimeoutError:
        future.cancel()
        return jsonify({"ok": False, "message": "连接超时，请检查网络后重试。"}), 504


_local_launch_lock = threading.Lock()
_local_launch_expires = time.monotonic() + 300


@_flask_app.route("/api/account")
@login_required
def telegram_account():
    identity = getattr(web_application, "telegram_account", None)
    return jsonify({"account": identity, "signed_in": bool(identity) and bool(getattr(web_application, "telegram_ready", False))})


@_flask_app.route("/api/account/profile", methods=["POST"])
@login_required
def new_local_account_profile():
    root = os.environ.get("TMD_DESKTOP_ROOT_DIR")
    if os.environ.get("TMD_DESKTOP") != "1" or not root or request.remote_addr not in {"127.0.0.1", "::1"}:
        return jsonify({"ok": False, "message": "请在 Mac 应用中添加账号。"}), 409
    from module.account_management import prepare_profile
    payload = request.get_json(silent=True)
    try:
        if not isinstance(payload, dict):
            raise ValueError("请求格式不正确。")
        with _config_lock:
            prepare_profile(root, payload.get("id"), _read_config())
    except (ValueError, OSError):
        return jsonify({"ok": False, "message": "无法创建账号空间，请重试。"}), 400
    return jsonify({"ok": True})


@_flask_app.route("/api/account/logout", methods=["POST"])
@login_required
def logout_telegram_account():
    controller = getattr(web_application, "account_logout", None)
    if os.environ.get("TMD_DESKTOP") != "1" or request.remote_addr not in {"127.0.0.1", "::1"} or not controller:
        return jsonify({"ok": False, "message": "当前没有可退出的 Telegram 登录。"}), 409
    set_download_state(DownloadState.StopDownload)
    _persist_download_state(True)
    future = asyncio.run_coroutine_threadsafe(controller.run(), web_application.loop)
    try:
        future.result(timeout=40)
    except Exception:
        future.cancel()
        return jsonify({"ok": False, "message": "退出未完成，下载已暂停。请检查网络后重试。"}), 503
    return jsonify({"ok": True, "message": "已退出此账号。文件和下载记录已保留。"})


@_flask_app.route("/api/local-launch", methods=["POST"])
def local_launch_login():
    """Redeem a short-lived, single-use secret from the local launcher's URL fragment."""
    if request.remote_addr not in {"127.0.0.1", "::1"} or urlsplit("//" + request.host).hostname not in {"127.0.0.1", "localhost", "::1"} or not request.is_json:
        return jsonify({"ok": False}), 403
    body = request.get_json(silent=True)
    provided = body.get("token", "") if isinstance(body, dict) else ""
    with _local_launch_lock:
        expected = os.environ.get("TMD_LOCAL_LAUNCH_TOKEN", "")
        if not isinstance(provided, str) or len(expected) < 32 or time.monotonic() > _local_launch_expires or not secrets.compare_digest(provided, expected):
            return jsonify({"ok": False}), 403
        os.environ.pop("TMD_LOCAL_LAUNCH_TOKEN", None)
        login_user(User())
        session.permanent = True
    return jsonify({"ok": True})


@_flask_app.route("/api/app/quit", methods=["POST"])
@_flask_app.route("/api/app/restart", methods=["POST"])
@login_required
def restart_desktop():
    if os.environ.get("TMD_DESKTOP") != "1" or request.remote_addr not in {"127.0.0.1", "::1"}:
        return jsonify({"ok": False, "message": "请在运行设备上重新启动下载器。"}), 409
    web_application.desktop_restart_requested = request.path.endswith("restart")
    threading.Timer(0.4, lambda: os.kill(os.getpid(), signal.SIGINT)).start()
    return jsonify({"ok": True, "message": "正在重新连接，页面稍后会自动恢复。"})


@_flask_app.route("/api/setup/status")
@login_required
def setup_status():
    config = _read_config()
    channels = get_channel_configs(False)
    return jsonify({
        "desktop": os.environ.get("TMD_DESKTOP") == "1",
        "restart_required": bool(getattr(web_application,"restart_required",False)),
        "credentials_saved": network_setup.credentials_ready(config),
        "preview": bool(getattr(web_application, "preview_mode", False)),
        "telegram_ready": bool(getattr(web_application, "telegram_ready", False)) and not bool(getattr(web_application, "preview_mode", False)),
        "channel_count": len(channels),
        "enabled_count": sum(bool(row["enabled"]) for row in channels),
        "message": _runtime_blocker(),
        "storage": _storage_status(),
    })


@_flask_app.route("/get_download_state")
@login_required
def web_get_download_state():
    """Return the current download state and the next available action."""
    is_downloading = not _runtime_blocker() and get_download_state() is DownloadState.Downloading
    return jsonify(
        {
            "state": "downloading" if is_downloading else "paused",
            "action": "pause" if is_downloading else "continue",
        }
    )



@_flask_app.route("/api/import/template.csv")
@login_required
def import_template():
    kind = request.args.get("kind", "channels")
    content = {
        "channels": "chat_id,start_message_id,group_name,priority,download_filter\n@replace_with_your_channel,0,学习,normal,\n",
        "invites": "order,invite_link,title,chat_id\n1,替换为你的私有频道邀请链接,频道名称（可选）,\n",
    }.get(kind)
    if content is None:
        return jsonify({"ok": False, "message": "请选择频道模板或邀请链接模板。"}), 400
    return send_file(io.BytesIO(content.encode("utf-8-sig")), mimetype="text/csv; charset=utf-8",
                     as_attachment=True, download_name=kind + "-template.csv")


@_flask_app.route("/api/batch/import", methods=["POST"])
@login_required
def import_invite_csv():
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "message": "导入数据格式不正确。"}), 400
    content = str(payload.get("content") or "")
    if len(content.encode("utf-8")) > 2_000_000:
        return jsonify({"ok": False, "message": "CSV 太大，请拆成每次不超过 2 MB 的文件。"}), 400
    rows = list(csv.DictReader(io.StringIO(content.lstrip("\ufeff"))))
    if not rows or len(rows) > 5000:
        return jsonify({"ok": False, "message": "请填写模板，单次导入 1–5000 条邀请链接。"}), 400
    for i, row in enumerate(rows, 2):
        link = str(row.get("invite_link") or "").strip()
        if not re.fullmatch(r"https://(?:t\.me|telegram\.me)/(?:\+|joinchat/)[A-Za-z0-9_-]+", link):
            return jsonify({"ok": False, "message": f"第 {i} 行需要完整的私有频道邀请链接。已加入的公开频道请用频道模板。"}), 400
    with tempfile.TemporaryDirectory(prefix="tmd-import-") as directory:
        path = Path(directory) / "invites.csv"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content.lstrip("\ufeff"))
        result = batch_queue.load_from_csv(str(path))
    return jsonify({"ok": True, "message": f"已添加 {result['loaded']} 个邀请链接，跳过 {result['skipped']} 个重复链接。请先手动加入，再验证。", "result": result})


@_flask_app.route("/api/network/status")
@login_required
def network_status():
    with _config_lock:
        config = _read_config()
    return jsonify({"ok": True, **network_setup.status(config)})


@_flask_app.route("/api/network/test", methods=["POST"])
@login_required
def network_test():
    try:
        with _config_lock:
            config = copy.deepcopy(_read_config())
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            raise ValueError("网络设置格式不正确。")
        # Build a temporary candidate; testing never saves or restarts live connections.
        if payload.get("network_mode") == "proxy":
            proxy = payload.get("proxy") or {}
            if not isinstance(proxy, dict):
                raise ValueError("代理设置格式不正确。")
            previous = config.get("proxy") or {}
            config["proxy"] = {"scheme": str(proxy.get("scheme", "socks5")),
                               "hostname": str(proxy.get("hostname") or "").strip(),
                               "port": _integer(proxy.get("port"), "代理端口", 1, 65535),
                               "username": str(proxy.get("username") or ""),
                               "password": str(proxy.get("password") or previous.get("password") or "")}
            if config["proxy"]["scheme"] not in PROXY_SCHEMES or not config["proxy"]["hostname"]:
                raise ValueError("请填写正确的代理类型与地址。")
        network_setup.apply_settings(config, payload)
        message = network_setup.probe(config)
        return jsonify({"ok": True, "message": message})
    except ValueError as error:
        return jsonify({"ok": False, "message": str(error)}), 400
    except (OSError, subprocess.TimeoutExpired):
        return jsonify({"ok": False, "message": "网络组件未能完成检查，请重试或使用 SOCKS5。"}), 400


@_flask_app.route("/api/network/import", methods=["POST"])
@login_required
def network_import():
    """Preview local content without saving, contacting nodes or fetching URLs."""
    from module import node_import
    try:
        if request.content_length and request.content_length > 2 * node_import.LIMIT:
            raise ValueError("文件过大，请只导出需要的节点（最多 256 KB）。")
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            raise ValueError("请上传 YAML / JSON 或粘贴节点内容。")
        result = node_import.preview(payload.get("content"))
        response = jsonify({"ok": True, **result})
        response.headers['Cache-Control'] = 'no-store'
        return response
    except ValueError as error:
        return jsonify({"ok": False, "message": str(error)}), 400


@_flask_app.route("/api/config", methods=["GET", "POST"])
@login_required
def web_config():
    """Read or update config.yaml from the local web console."""
    try:
        with _config_lock:
            config = _read_config()
            if request.method == "GET":
                return jsonify({"ok": True, "config": _public_config(config)})

            if not request.is_json:
                return jsonify({"ok": False, "message": "请求必须使用 JSON 格式"}), 400
            previous_config = copy.deepcopy(config)
            config = _merge_config(config, request.get_json(silent=True))
            _write_config(config)
            changed = {key for key in set(previous_config) | set(config) if previous_config.get(key) != config.get(key)}
            if changed and changed <= {"save_path"}:
                _apply_live_save_path(config["save_path"])
            elif changed and getattr(web_application, "telegram_ready", False):
                web_application.restart_required = True
            return jsonify(
                {
                    "ok": True,
                    "message": ("设置已保存，请按页面提示登录 Telegram"
                                if getattr(web_application, "awaiting_setup", False)
                                else "设置已保存，重启下载器后生效"),
                    "config": _public_config(config),
                }
            )
    except (OSError, RuntimeError, ValueError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400


@_flask_app.route("/get_download_status")
@login_required
def get_download_speed():
    """Get download speed"""
    return (
        '{ "download_speed" : "'
        + format_byte(get_total_download_speed())
        + '/s" , "upload_speed" : "0.00 B/s" } '
    )


def _format_duration(seconds: Optional[float]) -> str:
    """Format an ETA for the compact web console."""
    if seconds is None or seconds < 0:
        return "—"
    total = int(seconds)
    if total < 60:
        return f"{total}秒"
    minutes, remaining_seconds = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}分 {remaining_seconds}秒"
    hours, remaining_minutes = divmod(minutes, 60)
    return f"{hours}小时 {remaining_minutes}分"


def _live_transfer_summary(snapshot: Optional[dict] = None) -> dict:
    """Aggregate active progress once for system, channel, and file views."""
    snapshot = get_download_snapshot() if snapshot is None else snapshot
    paused = get_download_state() is DownloadState.StopDownload
    now = time.time()
    channels = {}
    latest_progress_at = 0.0

    for chat_id, messages in snapshot.items():
        channel = {
            "active_files": 0,
            "downloaded_bytes": 0,
            "total_bytes": 0,
            "speed_bytes": 0,
            "last_progress_at": 0.0,
        }
        for value in messages.values():
            downloaded = max(int(value.get("down_byte") or 0), 0)
            total = max(int(value.get("total_size") or 0), 0)
            if total and downloaded >= total:
                continue
            channel["active_files"] += 1
            channel["downloaded_bytes"] += min(downloaded, total) if total else downloaded
            channel["total_bytes"] += total
            channel["speed_bytes"] += max(int(value.get("download_speed") or 0), 0)
            channel["last_progress_at"] = max(
                channel["last_progress_at"], float(value.get("end_time") or 0)
            )
        if channel["active_files"]:
            channels[str(chat_id)] = channel
            latest_progress_at = max(latest_progress_at, channel["last_progress_at"])

    def finalize(values: dict) -> dict:
        total = values["total_bytes"]
        downloaded = values["downloaded_bytes"]
        last_at = values.get("last_progress_at") or 0
        stalled_seconds = max(int(now - last_at), 0) if last_at else 0
        stalled = bool(values["active_files"] and last_at and stalled_seconds >= 15)
        speed = 0 if paused or stalled else values["speed_bytes"]
        remaining = max(total - downloaded, 0)
        eta_seconds = remaining / speed if speed > 0 else None
        eta = "等待继续" if paused else ("等待数据" if stalled else _format_duration(eta_seconds))
        return {
            **values,
            "progress": round(downloaded / total * 100, 1) if total else 0,
            "downloaded": format_byte(downloaded),
            "total": format_byte(total),
            "speed_bytes": speed,
            "speed": format_byte(speed) + "/s",
            "eta_seconds": round(eta_seconds) if eta_seconds is not None else None,
            "eta": eta,
            "paused": paused,
            "stalled": stalled,
            "stalled_seconds": stalled_seconds,
        }

    finalized_channels = {chat_id: finalize(values) for chat_id, values in channels.items()}
    aggregate = {
        "active_files": sum(item["active_files"] for item in channels.values()),
        "downloaded_bytes": sum(item["downloaded_bytes"] for item in channels.values()),
        "total_bytes": sum(item["total_bytes"] for item in channels.values()),
        "speed_bytes": sum(item["speed_bytes"] for item in channels.values()),
        "last_progress_at": latest_progress_at,
    }
    result = finalize(aggregate)
    result["channels"] = finalized_channels
    if paused and result["active_files"]:
        result["effective_state"] = "suspended"
    elif paused:
        result["effective_state"] = "paused"
    elif result["active_files"]:
        result["effective_state"] = "downloading"
    else:
        result["effective_state"] = "idle"
    return result


@_flask_app.route("/set_download_state", methods=["POST"])
@login_required
def web_set_download_state():
    """Set download state"""
    state = request.args.get("state")
    if state == "continue" and _runtime_blocker():
        return jsonify({"ok": False, "message": _runtime_blocker()}), 409

    if state == "continue" and get_download_state() is DownloadState.StopDownload:
        storage = _storage_status()
        if not storage["available"] or storage["writable"] is False:
            message = (
                "保存目录写入检测未通过，无法开始下载"
                if storage["available"]
                else "保存目录不可用，无法开始下载"
            )
            return (
                jsonify(
                    {
                        "ok": False,
                        "message": message,
                        "storage": storage,
                    }
                ),
                409,
            )
        set_download_state(DownloadState.Downloading)
        _persist_download_state(False)
        return "pause"

    if state == "pause" and get_download_state() is DownloadState.Downloading:
        set_download_state(DownloadState.StopDownload)
        _persist_download_state(True)
        return "continue"

    return state


@_flask_app.route("/get_app_version")
def get_app_version():
    """Get telegram_media_downloader version"""
    return utils.__version__


def _telegram_message_url(chat_id, message_id) -> str:
    """Build a Telegram message URL when the configured chat ID permits it."""
    chat = str(chat_id or "").strip()
    message = str(message_id or "").strip()
    if not message:
        return ""
    if chat.startswith("-100") and chat[4:].isdigit():
        return f"https://t.me/c/{chat[4:]}/{message}"
    if chat.startswith("@") and len(chat) > 1:
        return f"https://t.me/{chat[1:]}/{message}"
    return ""


def _history_web_record(record: dict) -> dict:
    """Format a persistent history row for the web console."""
    return {
        "chat": record["chat_id"],
        "chat_title": record["chat_title"],
        "id": str(record["message_id"]),
        "filename": record["file_name"],
        "total_size": format_byte(record["total_size"]),
        "total_size_bytes": record["total_size"],
        "download_progress": "100.0",
        "download_speed": "—",
        "save_path": record["save_path"].replace("\\", "/"),
        "media_type": record["media_type"],
        "completed_at": record["completed_at"],
        "telegram_url": _telegram_message_url(
            record["chat_id"], record["message_id"]
        ),
    }


@_flask_app.route("/api/download_history")
@login_required
def get_download_history():
    """Return persistent, server-paginated download history."""
    try:
        page = max(int(request.args.get("page", 1)), 1)
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except (TypeError, ValueError):
        return jsonify({"code": 1, "msg": "分页参数不正确", "count": 0, "data": []}), 400

    history = list_downloads(limit=limit, offset=(page - 1) * limit)
    return jsonify(
        {
            "code": 0,
            "msg": "",
            "count": history["total"],
            "data": [_history_web_record(record) for record in history["records"]],
        }
    )


def _task_web_record(record: dict) -> dict:
    """Format a persistent task row for desktop and mobile views."""
    return {
        "chat": record["chat_id"],
        "chat_title": record["chat_title"],
        "id": str(record["message_id"]),
        "filename": record["file_name"] or f"消息 #{record['message_id']}",
        "media_type": record["media_type"],
        "total_size": format_byte(record["total_size"]),
        "total_size_bytes": record["total_size"],
        "status": record["status"],
        "attempts": record["attempts"],
        "max_attempts": record["max_attempts"],
        "error": record["error"],
        "updated_at": record["updated_at"],
        "next_retry_at": record["next_retry_at"],
        "save_path": record["save_path"].replace("\\", "/"),
        "telegram_url": _telegram_message_url(
            record["chat_id"], record["message_id"]
        ),
    }


def _channel_web_record(record: dict, live_transfer: Optional[dict] = None) -> dict:
    """Format a channel-level queue row for desktop and mobile views."""
    live_transfer = live_transfer or {}
    global_paused = get_download_state() is DownloadState.StopDownload
    active_files = int(live_transfer.get("active_files") or record["downloading"] or 0)
    completed_size = max(int(record.get("completed_size") or 0), 0)
    total_size = max(int(record.get("total_size") or 0), 0)
    active_downloaded = max(int(live_transfer.get("downloaded_bytes") or 0), 0)
    downloaded_size = min(completed_size + active_downloaded, total_size)
    count_total = max(int(record.get("total") or 0) - int(record.get("cancelled") or 0), 0)
    count_completed = int(record.get("completed") or 0) + int(record.get("skipped") or 0)
    count_progress = round(count_completed / count_total * 100, 1) if count_total else 0
    byte_progress = round(downloaded_size / total_size * 100, 1) if total_size else None
    # File count is the stable denominator: Telegram photos often enter the queue
    # without a byte size. Capacity progress remains available as secondary data.
    overall_progress = count_progress
    display_status = record["status"]
    state_reason = record.get("state_reason", "")
    completed_channel = record["status"] == "completed" and not int(record["pending"] or 0)
    if completed_channel:
        display_status = "completed"
        state_reason = "全部下载完成，已归档，不再访问 Telegram"
    elif bool(record["paused"]):
        display_status = "paused"
        state_reason = "频道已暂停，点击开始继续下载"
    elif global_paused and active_files:
        display_status = "suspended"
        state_reason = f"全局暂停，{active_files} 个文件已保留当前进度"
    elif global_paused:
        display_status = "paused"
        state_reason = "全局下载已暂停，点击开始可恢复"
    elif active_files and record["status"] == "downloading":
        state_reason = f"{active_files} 个文件正在传输"

    if not completed_channel and _runtime_blocker():
        display_status = "paused"
        state_reason = _runtime_blocker()
        global_paused = True

    return {
        "chat": record["chat_id"],
        "chat_title": record["chat_title"],
        "status": record["status"],
        "display_status": display_status,
        "global_paused": global_paused,
        "channel_running": (
            not completed_channel and not global_paused and not bool(record["paused"])
        ),
        "channel_action": (
            None
            if completed_channel
            else "pause" if not global_paused and not bool(record["paused"]) else "start"
        ),
        "paused": bool(record["paused"]),
        "enabled": bool(record.get("enabled", 1)),
        "priority": record.get("priority", "normal"),
        "group_name": record.get("group_name", ""),
        "last_read_message_id": record.get("last_read_message_id", 0),
        "download_filter": record.get("download_filter", ""),
        "validation_status": record.get("validation_status", "valid"),
        "validation_error": record.get("validation_error", ""),
        "scan_status": record.get("scan_status", "idle"),
        "start_blocked": bool(_runtime_blocker()) and not completed_channel,
        "state_reason": state_reason,
        "state_changed_at": record.get("state_changed_at", ""),
        "next_action_at": record.get("next_action_at"),
        "state_version": record.get("state_version", 1),
        "queued": record["queued"],
        "downloading": record["downloading"],
        "retrying": record["retrying"],
        "retry_requested": record["retry_requested"],
        "pending": record["pending"],
        "completed": record["completed"],
        "failed": record["failed"],
        "cancelled": record["cancelled"],
        "skipped": record["skipped"],
        "total": record["total"],
        "total_size": format_byte(record["total_size"]),
        "total_size_bytes": record["total_size"],
        "completed_size": format_byte(completed_size),
        "completed_size_bytes": completed_size,
        "channel_downloaded": format_byte(downloaded_size),
        "channel_downloaded_bytes": downloaded_size,
        "channel_progress": overall_progress,
        "channel_count_progress": count_progress,
        "channel_byte_progress": byte_progress,
        "channel_completed_count": count_completed,
        "channel_total_count": count_total,
        "channel_progress_label": f"{count_completed}/{count_total}",
        "last_activity": record["last_activity"],
        "active_files": active_files,
        "active_progress": live_transfer.get("progress"),
        "active_downloaded": live_transfer.get("downloaded", "0 B"),
        "active_total": live_transfer.get("total", "0 B"),
        "active_speed": live_transfer.get("speed", "0 B/s"),
        "active_eta": live_transfer.get("eta", "等待进度"),
        "active_paused": bool(global_paused and active_files),
    }


def _fast_channel_snapshot(search: str, status: str, limit: int, offset: int) -> dict:
    """Return a non-blocking channel list while aggregate statistics refresh."""
    search_value = str(search or "").strip().lower()
    records = []
    for item in get_channel_configs(enabled_only=False):
        lifecycle = str(item.get("lifecycle_state") or "ready")
        title = str(item.get("chat_title") or item.get("chat_id") or "")
        chat_id = str(item.get("chat_id") or "")
        if search_value and search_value not in title.lower() and search_value not in chat_id.lower():
            continue
        if status and lifecycle != status:
            continue
        record = dict(item)
        record.update(
            {
                "status": lifecycle,
                "scan_status": item.get("scan_status", "idle"),
                "queued": 0,
                "downloading": 0,
                "retrying": 0,
                "retry_requested": 0,
                "failed": 0,
                "cancelled": 0,
                "skipped": 0,
                "completed": 0,
                "pending": 0,
                "total": 0,
                "completed_size": 0,
                "total_size": 0,
                "last_activity": item.get("state_changed_at") or item.get("updated_at") or "",
            }
        )
        records.append(record)
    records.sort(key=lambda row: str(row.get("chat_title") or row.get("chat_id") or ""), reverse=True)
    return {"total": len(records), "records": records[offset : offset + limit]}


def _invalidate_channel_api_cache() -> None:
    """Drop channel snapshots after a control action changes their state."""
    global _channel_api_cache_generation
    with _channel_api_cache_lock:
        _channel_api_cache_generation += 1
        _channel_api_cache.clear()
        _channel_api_refreshing.clear()


def _refresh_channel_cache(
    cache_key,
    search: str,
    status: str,
    limit: int,
    offset: int,
    generation: int,
) -> None:
    try:
        channels = list_channels(search=search, status=status, limit=limit, offset=offset)
        with _channel_api_cache_lock:
            if generation == _channel_api_cache_generation:
                _channel_api_cache[cache_key] = (time.monotonic(), channels)
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(f"频道列表后台刷新失败，继续使用旧数据: {error}")
    finally:
        with _channel_api_cache_lock:
            if generation == _channel_api_cache_generation:
                _channel_api_refreshing.discard(cache_key)


@_flask_app.route("/api/channels")
@login_required
def get_download_channels():
    """Return the channel-first task queue with server-side pagination."""
    try:
        page = max(int(request.args.get("page", 1)), 1)
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except (TypeError, ValueError):
        return jsonify({"code": 1, "msg": "分页参数不正确", "count": 0, "data": []}), 400

    requested_status = str(request.args.get("status", "")).strip()
    if requested_status and requested_status not in CHANNEL_STATUSES:
        return jsonify({"code": 1, "msg": "频道状态不正确", "count": 0, "data": []}), 400

    search_value = str(request.args.get("search", ""))
    offset = (page - 1) * limit
    cache_key = (page, limit, requested_status, search_value.strip().lower())
    now = time.monotonic()
    with _channel_api_cache_lock:
        cache_generation = _channel_api_cache_generation
        cached = _channel_api_cache.get(cache_key)
        should_refresh = (
            not cached or now - cached[0] >= _CHANNEL_API_CACHE_TTL
        ) and cache_key not in _channel_api_refreshing
        should_refresh = should_refresh and (
            len(_channel_api_refreshing) < _CHANNEL_REFRESH_QUEUE_LIMIT
        )
        if should_refresh:
            _channel_api_refreshing.add(cache_key)
    channels = cached[1] if cached else _fast_channel_snapshot(
        search_value, requested_status, limit, offset
    )
    if should_refresh:
        _channel_refresh_executor.submit(
            _refresh_channel_cache,
            cache_key,
            search_value,
            requested_status,
            limit,
            offset,
            cache_generation,
        )
    transfer_channels = _live_transfer_summary()["channels"]
    return jsonify(
        {
            "code": 0,
            "msg": "",
            "count": channels["total"],
            "data": [
                _channel_web_record(
                    record, transfer_channels.get(str(record["chat_id"]))
                )
                for record in channels["records"]
            ],
        }
    )


@_flask_app.route("/api/channel_state_history")
@login_required
def get_channel_state_history():
    """Return the persisted lifecycle timeline for one channel."""
    chat_id = str(request.args.get("chat_id") or "").strip()
    if not chat_id:
        return jsonify({"ok": False, "message": "缺少 chat_id", "data": []}), 400
    try:
        limit = min(max(int(request.args.get("limit", 30)), 1), 200)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "limit 参数不正确", "data": []}), 400
    return jsonify({"ok": True, "data": list_channel_state_events(chat_id, limit)})


@_flask_app.route("/api/channel_library", methods=["GET", "POST"])
@login_required
def channel_library():
    """Read or update SQLite-backed channel master data."""
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        try:
            channel = upsert_channel_config(payload, source="web")
        except (TypeError, ValueError) as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400
        _invalidate_channel_api_cache()
        return jsonify(
            {
                "ok": True,
                "message": "频道已保存，准备好后可以开始下载。",
                "channel": channel,
                "channel_count": len(get_channel_configs(enabled_only=False)),
            }
        )

    try:
        page = max(int(request.args.get("page", 1)), 1)
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except (TypeError, ValueError):
        return jsonify({"code": 1, "msg": "分页参数不正确", "count": 0, "data": []}), 400
    channels = list_channels(
        search=request.args.get("search", ""),
        limit=limit,
        offset=(page - 1) * limit,
    )
    return jsonify(
        {
            "code": 0,
            "msg": "",
            "count": channels["total"],
            "data": [_channel_web_record(record) for record in channels["records"]],
        }
    )


@_flask_app.route("/api/channel_library/state", methods=["POST"])
@login_required
def channel_library_state():
    """Enable or disable one channel without deleting its history."""
    payload = request.get_json(silent=True) or {}
    chat_id = str(payload.get("chat_id") or "").strip()
    if not chat_id:
        return jsonify({"ok": False, "message": "缺少 chat_id"}), 400
    enabled = bool(payload.get("enabled"))
    changed = set_channel_enabled(chat_id, enabled)
    return jsonify(
        {
            "ok": bool(changed),
            "changed": changed,
            "message": "频道已启用" if enabled else "频道已停用，历史记录已保留",
        }
    ), (200 if changed else 404)


@_flask_app.route("/api/channel_library/refresh", methods=["POST"])
@login_required
def channel_library_refresh():
    """Ask finished channels to look for messages posted since they completed.

    A completed channel is retired (`enabled = 0`) and therefore invisible to
    the scanner, so "check for updates" has to re-open it. The scan itself
    resumes from the stored cursor, and `_get_channel_scan_limiter()` caps how
    many run at once, so selecting many channels queues them rather than
    flooding the metadata pipeline.
    """
    payload = request.get_json(silent=True) or {}
    raw = payload.get("chat_ids")
    if raw is None:
        single = payload.get("chat_id")
        raw = [single] if single else []
    if not isinstance(raw, (list, tuple)):
        raw = [raw]
    chat_ids = [str(item).strip() for item in raw if str(item or "").strip()]
    if not chat_ids:
        return jsonify({"ok": False, "message": "请先选择要检查更新的频道"}), 400

    result = refresh_completed_channels(chat_ids)
    refreshed = len(result["refreshed"])
    skipped = len(result["skipped"])
    missing = len(result["missing"])

    if refreshed:
        message = f"{refreshed} 个频道正在检查更新，有新内容会自动开始下载"
        if skipped or missing:
            message += f"（跳过 {skipped + missing} 个：未完成或正在下载的频道无需检查）"
    elif skipped or missing:
        message = "选中的频道都在下载中或尚未完成，无需检查更新"
    else:
        message = "没有可检查的频道"

    return jsonify(
        {
            "ok": bool(refreshed),
            "refreshed": result["refreshed"],
            "skipped": result["skipped"],
            "missing": result["missing"],
            "message": message,
        }
    )


@_flask_app.route("/api/channel_imports", methods=["GET", "POST"])
@login_required
def channel_imports():
    """Create an import batch or list recent batches."""
    if request.method == "GET":
        return jsonify({"ok": True, "batches": list_import_batches()})
    payload = request.get_json(silent=True) or {}
    try:
        batch = create_import_batch(
            payload.get("content", ""),
            source_name=payload.get("source_name", ""),
            defaults=payload.get("defaults") or {},
        )
    except (TypeError, ValueError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400
    return jsonify(
        {
            "ok": True,
            "message": "导入批次已创建，正在后台校验 Telegram 访问权限",
            "batch": batch,
        }
    ), 201


@_flask_app.route("/api/channel_imports/<int:batch_id>")
@login_required
def channel_import_detail(batch_id: int):
    """Return one import batch and a paginated review list."""
    batch = get_import_batch(batch_id)
    if batch is None:
        return jsonify({"ok": False, "message": "导入批次不存在"}), 404
    try:
        page = max(int(request.args.get("page", 1)), 1)
        limit = min(max(int(request.args.get("limit", 100)), 1), 500)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "分页参数不正确"}), 400
    items = list_import_items(batch_id, limit=limit, offset=(page - 1) * limit)
    return jsonify(
        {
            "ok": True,
            "batch": batch,
            "count": items["total"],
            "items": items["records"],
        }
    )


@_flask_app.route("/api/channel_imports/<int:batch_id>/confirm", methods=["POST"])
@login_required
def confirm_channel_import(batch_id: int):
    """Commit all validated rows from one batch to the live channel library."""
    try:
        batch = confirm_import_batch(batch_id)
    except ValueError as exc:
        return jsonify({"ok": False, "message": str(exc)}), 409
    return jsonify(
        {
            "ok": True,
            "message": f"已导入 {batch['changed']} 个频道，下载进程将自动接管",
            "batch": batch,
        }
    )


@_flask_app.route("/api/channel_imports/<int:batch_id>/undo", methods=["POST"])
@login_required
def undo_channel_import(batch_id: int):
    """Undo an imported batch without deleting download evidence."""
    try:
        batch = undo_import_batch(batch_id)
    except ValueError as exc:
        return jsonify({"ok": False, "message": str(exc)}), 404
    return jsonify(
        {
            "ok": True,
            "message": (
                f"批次已撤销：移除 {batch['removed']} 个空频道，"
                f"停用 {batch['disabled']} 个已有记录的频道"
            ),
            "batch": batch,
        }
    )


@_flask_app.route("/api/channel_files")
@login_required
def get_channel_files():
    """Return one channel's active, failed, and completed files."""
    chat_id = request.args.get("chat_id")
    if chat_id is None or not str(chat_id).strip():
        return jsonify({"code": 1, "msg": "缺少 chat_id", "count": 0, "data": []}), 400
    try:
        page = max(int(request.args.get("page", 1)), 1)
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except (TypeError, ValueError):
        return jsonify({"code": 1, "msg": "分页参数不正确", "count": 0, "data": []}), 400

    requested_statuses = {
        item.strip()
        for item in request.args.get("status", "").split(",")
        if item.strip()
    }
    if requested_statuses - TASK_STATUSES:
        return jsonify({"code": 1, "msg": "任务状态不正确", "count": 0, "data": []}), 400
    files = list_channel_files(
        chat_id,
        statuses=sorted(requested_statuses),
        limit=limit,
        offset=(page - 1) * limit,
    )
    return jsonify(
        {
            "code": 0,
            "msg": "",
            "count": files["total"],
            "data": [_task_web_record(record) for record in files["records"]],
        }
    )


@_flask_app.route("/api/channel_state", methods=["POST"])
@login_required
def set_download_channel_state():
    """Start or pause one or more channels and their active transfers."""
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "").strip().lower()
    chat_ids = payload.get("chat_ids")
    apply_all = bool(payload.get("all"))
    if apply_all:
        chat_ids = [
            row["chat_id"]
            for row in get_channel_configs(False)
            if row["enabled"]
        ]
    if chat_ids is None and payload.get("chat_id") is not None:
        chat_ids = [payload.get("chat_id")]
    if action not in {"pause", "resume", "start"}:
        return jsonify({"ok": False, "message": "频道操作不正确"}), 400
    if (
        not isinstance(chat_ids, list)
        or not chat_ids
        or (not apply_all and len(chat_ids) > 5000)
    ):
        return jsonify({"ok": False, "message": "请选择 1 到 5000 个频道"}), 400

    normalized = [str(chat_id).strip() for chat_id in chat_ids if str(chat_id).strip()]
    if not normalized:
        return jsonify({"ok": False, "message": "频道 ID 不能为空"}), 400
    is_pause = action == "pause"
    if not is_pause and _runtime_blocker():
        return jsonify({"ok": False, "message": _runtime_blocker()}), 409
    globally_paused = get_download_state() is DownloadState.StopDownload
    if not is_pause and get_download_state() is DownloadState.StopDownload:
        storage = _storage_status()
        if not storage["available"] or storage["writable"] is False:
            message = (
                "保存目录写入检测未通过，无法开始频道下载"
                if storage["available"]
                else "保存目录不可用，无法开始频道下载"
            )
            return jsonify({"ok": False, "message": message, "storage": storage}), 409

    if not is_pause and globally_paused:
        selected = set(normalized)
        other_channels = [
            row["chat_id"]
            for row in get_channel_configs(False)
            if row["enabled"] and row["chat_id"] not in selected
        ]
        if other_channels:
            set_channels_paused(other_channels, True)
            set_channel_download_paused(other_channels, True)

    changed = set_channels_paused(normalized, is_pause)
    set_channel_download_paused(normalized, is_pause)
    if apply_all and is_pause:
        set_download_state(DownloadState.StopDownload)
        _persist_download_state(True)
    elif not is_pause and (globally_paused or apply_all):
        set_download_state(DownloadState.Downloading)
        _persist_download_state(False)
    _invalidate_channel_api_cache()
    return jsonify(
        {
            "ok": True,
            "changed": changed,
            "all": apply_all,
            "download_state": (
                "paused" if is_pause and apply_all else "downloading"
            ),
            "message": (
                f"已暂停 {changed} 个频道，当前下载进度已保留"
                if is_pause
                else f"已开始下载 {changed} 个频道，正在进入调度队列"
            ),
        }
    )


@_flask_app.route("/api/download_tasks")
@login_required
def get_persistent_download_tasks():
    """Return server-paginated persistent download tasks."""
    try:
        page = max(int(request.args.get("page", 1)), 1)
        limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    except (TypeError, ValueError):
        return jsonify({"code": 1, "msg": "分页参数不正确", "count": 0, "data": []}), 400

    requested_statuses = {
        item.strip()
        for item in request.args.get("status", "").split(",")
        if item.strip()
    }
    if requested_statuses - TASK_STATUSES:
        return jsonify({"code": 1, "msg": "任务状态不正确", "count": 0, "data": []}), 400

    tasks = list_tasks(
        statuses=sorted(requested_statuses),
        limit=limit,
        offset=(page - 1) * limit,
        chat_id=request.args.get("chat_id"),
    )
    return jsonify(
        {
            "code": 0,
            "msg": "",
            "count": tasks["total"],
            "data": [_task_web_record(record) for record in tasks["records"]],
        }
    )


@_flask_app.route("/api/retry_tasks", methods=["POST"])
@login_required
def retry_persistent_download_tasks():
    """Request one failed task, or all failed tasks, for retry."""
    payload = request.get_json(silent=True) or {}
    try:
        if bool(payload.get("all")):
            changed = request_retry()
        else:
            if payload.get("chat_id") is None or payload.get("message_id") is None:
                return jsonify({"ok": False, "message": "缺少 chat_id 或 message_id"}), 400
            changed = request_retry(payload["chat_id"], int(payload["message_id"]))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "任务参数不正确"}), 400

    cooling = speed_governor.cooldown_remaining()
    if changed and cooling > 0:
        message = (
            f"已加入重试队列；当前处于限速熔断期，"
            f"约 {int(cooling // 60) + 1} 分钟后自动开始重试"
        )
    elif changed:
        message = "已加入重试队列"
    else:
        message = "没有可重试的失败任务"
    return jsonify({"ok": True, "changed": changed, "message": message})


@_flask_app.route("/api/cancel_task", methods=["POST"])
@login_required
def cancel_persistent_download_task():
    """Cancel one queued task without interrupting active file writes."""
    payload = request.get_json(silent=True) or {}
    try:
        if payload.get("chat_id") is None or payload.get("message_id") is None:
            raise ValueError("missing task key")
        changed = request_cancel(payload["chat_id"], int(payload["message_id"]))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "任务参数不正确"}), 400

    return jsonify(
        {
            "ok": bool(changed),
            "changed": changed,
            "message": (
                "任务已取消"
                if changed
                else "任务已开始、已完成或已被取消，无法再次取消"
            ),
        }
    ), (200 if changed else 409)


def _local_browser_request():
    """Do not open a Mac dialog from a remote browser or a rebound hostname."""
    return (request.remote_addr in ('127.0.0.1', '::1')
            and urlsplit(request.host_url).hostname in ('localhost', '127.0.0.1', '::1'))


@_flask_app.route('/api/download/preferences', methods=['POST'])
@login_required
def save_download_preferences():
    payload = request.get_json(silent=True)
    fields = {'media_types', 'file_formats', 'enable_download_txt', 'file_path_prefix', 'file_name_prefix', 'max_download_task'}
    if not isinstance(payload, dict) or set(payload) - fields:
        return jsonify({'ok': False, 'message': '下载偏好格式不正确。'}), 400
    try:
        with _config_lock:
            config = copy.deepcopy(_read_config())
            candidate = _public_config(config)
            if not config.get('api_id'):
                candidate.pop('api_id', None)
            candidate['file_formats'] = {key: ','.join(value) if isinstance(value, list) else str(value) for key, value in config.get('file_formats', {}).items()}
            candidate.update(payload)
            validated = _merge_config(copy.deepcopy(config), candidate)
            changed = {key for key in payload if config.get(key) != validated.get(key)}
            for key in payload:
                config[key] = validated[key]
            _write_config(config)
            for key in changed - {'max_download_task'}:
                setattr(web_application, key, copy.deepcopy(config[key]))
                web_application.config[key] = copy.deepcopy(config[key])
            if 'max_download_task' in changed:
                web_application.config['max_download_task'] = config['max_download_task']
                web_application.restart_required = True
        return jsonify({'ok': True, 'message': '下载偏好已保存，对后续扫描和新任务生效。' + ('并发数量已更改，请重新连接。' if 'max_download_task' in changed else '')})
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'message': '请选择下载内容（可只选文字），并检查格式和并发数量。'}), 400
    except OSError:
        return jsonify({'ok': False, 'message': '保存失败，请检查配置目录写入权限。'}), 400


def _apply_live_save_path(path):
    web_application.save_path = str(path)
    web_application.config["save_path"] = str(path)


@_flask_app.route('/api/storage/path', methods=['POST'])
@login_required
def save_storage_path():
    """Change the destination for new files without reconnecting Telegram."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or not isinstance(payload.get('path'), str):
        return jsonify({'ok': False, 'message': '请选择保存文件夹。'}), 400
    try:
        path = Path(payload['path']).expanduser()
        if not path.is_absolute() or not path.is_dir():
            raise ValueError('请选择一个已存在的文件夹。')
        with tempfile.TemporaryFile(dir=path) as probe:
            probe.write(b'1')
            probe.flush()
        with _config_lock:
            config = _read_config()
            config['save_path'] = str(path)
            _write_config(config)
            _apply_live_save_path(path)
        return jsonify({'ok': True, 'path': str(path), 'message': '保存位置已更新，无需重新登录。已排队或正在下载的文件保留原路径，新创建的任务使用新位置。'})
    except (OSError, ValueError):
        return jsonify({'ok': False, 'message': '无法写入这个文件夹，请检查权限或选择其他位置。'}), 400


@_flask_app.route('/api/storage/capabilities')
@login_required
def storage_capabilities():
    return jsonify({'ok': True, 'native_picker': _local_browser_request()
                    and local_storage.native_picker_available(),
                    'device': 'Mac' if sys.platform == 'darwin' else '运行设备'})


@_flask_app.route('/api/storage/choose', methods=['POST'])
@login_required
def choose_storage_folder():
    if (not _local_browser_request() or not request.is_json
            or request.headers.get('X-TMD-Local-Picker') != '1'):
        return jsonify({'ok': False, 'message': '请在运行下载器的 Mac 上打开本地页面选择文件夹。'}), 403
    origin = request.headers.get('Origin')
    if origin and origin != request.host_url.rstrip('/'):
        return jsonify({'ok': False, 'message': '请从本地下载器页面选择文件夹。'}), 403
    try:
        path = local_storage.choose_folder()
    except ValueError as error:
        return jsonify({'ok': False, 'message': str(error)}), 400
    return jsonify({'ok': True, 'cancelled': path is None, 'path': path})


@_flask_app.route("/api/test_storage", methods=["POST"])
@login_required
def test_storage_path():
    """Verify that the selected download directory is mounted and writable."""
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({'ok': False, 'message': '目录参数不正确'}), 400
    path_value = str(payload.get("path") or "").strip()
    if not path_value:
        return jsonify({"ok": False, "message": "请先填写保存目录"}), 400

    path = Path(path_value).expanduser()
    if not path.is_dir():
        _set_storage_probe(path, False, "目录不存在，请选择已有文件夹；共享目录请先挂载")
        return jsonify({"ok": False, "message": "目录不存在，请选择已有文件夹；共享目录请先挂载"}), 400

    probe_script = """
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

probe = Path(sys.argv[1]) / f".tmd-write-test-{os.getpid()}"
try:
    probe.write_text("ok", encoding="utf-8")
finally:
    probe.unlink(missing_ok=True)
"""
    try:
        probe = subprocess.run(
            [sys.executable, "-c", probe_script, str(path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
            cwd="/",
        )
    except (OSError, subprocess.SubprocessError) as exc:
        if isinstance(exc, subprocess.TimeoutExpired):
            _set_storage_probe(path, False, "存储响应超时，请检查目录后重试")
            return jsonify(
                {"ok": False, "message": "存储响应超时，请检查目录后重试"}
            ), 504
        _set_storage_probe(path, False, f"写入检测失败：{exc}")
        return jsonify({"ok": False, "message": f"写入检测失败：{exc}"}), 400

    if probe.returncode != 0:
        detail = (probe.stderr or "").strip().splitlines()
        message = detail[-1] if detail else "目录不可写"
        _set_storage_probe(path, False, f"写入检测失败：{message}")
        return jsonify({"ok": False, "message": f"写入检测失败：{message}"}), 400

    usage = shutil.disk_usage(path)
    _set_storage_probe(path, True, "保存目录可写")
    return jsonify(
        {
            "ok": True,
            "message": "保存目录可写",
            "free": usage.free,
            "total": usage.total,
        }
    )


@_flask_app.route("/api/remote/status")
@login_required
def get_remote_access_status():
    """Return Cloudflare Tunnel remote-access state."""
    return jsonify({"ok": True, "remote_access": remote_access.status()})


@_flask_app.route("/api/remote/enable", methods=["POST"])
@login_required
def enable_remote_access():
    """Start a Cloudflare Tunnel to the local Web console (no VPN needed)."""
    if not remote_access.is_installed():
        return (
            jsonify(
                {
                    "ok": False,
                    "message": "未检测到 cloudflared，请先安装：brew install cloudflared",
                }
            ),
            409,
        )
    port = getattr(web_application, "web_port", 5002)
    remote_cfg = (web_application.config.get("remote_access") or {}) if web_application else {}
    token = str(remote_cfg.get("cloudflared_token") or "")
    state = remote_access.start(port, token=token)
    # Persist the intent so it comes back up on restart.
    _persist_remote_access(True)
    return jsonify({"ok": True, "remote_access": state})


@_flask_app.route("/api/remote/disable", methods=["POST"])
@login_required
def disable_remote_access():
    """Stop the Cloudflare Tunnel."""
    state = remote_access.stop()
    _persist_remote_access(False)
    return jsonify({"ok": True, "remote_access": state})


@_flask_app.route("/api/remote_qr.png")
@login_required
def get_remote_access_qr():
    """QR code for the current remote URL (Cloudflare Tunnel preferred)."""
    access = remote_access.status()
    url = access.get("url") or _tailscale_status().get("url")
    if not url:
        return jsonify({"ok": False, "message": "远程访问地址尚未就绪"}), 404

    code = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=7,
        border=3,
    )
    code.add_data(url)
    code.make(fit=True)
    image = code.make_image(fill_color="#151916", back_color="#fffef9")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    return send_file(buffer, mimetype="image/png", max_age=60)


def _live_event_payload() -> dict:
    """Compact live snapshot pushed over SSE (cheap to compute every second)."""
    now = time.monotonic()
    with _live_event_cache_lock:
        cached = _live_event_cache["value"]
        if cached is not None and now - _live_event_cache["updated_at"] < _LIVE_EVENT_CACHE_TTL:
            return cached
    value = {
        "download_state": (
            "downloading"
            if get_download_state() is DownloadState.Downloading
            else "paused"
        ),
        "total_speed": get_total_download_speed(),
        "transfer": _live_transfer_summary(),
        "governor": rate_governor.snapshot(),
        "speed_governor": speed_governor.snapshot(),
        "server_time": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    with _live_event_cache_lock:
        _live_event_cache.update({"updated_at": now, "value": value})
    return value


@_flask_app.route("/api/events/stream")
@login_required
def stream_live_events():
    """Server-Sent Events: push live progress instead of the client polling.

    Lets phones and the desktop console share one cheap push stream rather than
    every device hammering /get_download_status on a timer -- important once the
    queue spans thousands of channels.
    """

    def _generate():
        # Prompt the browser to auto-reconnect after 3s if the stream drops.
        yield "retry: 3000\n\n"
        while True:
            try:
                payload = _live_event_payload()
            except Exception as error:  # pylint: disable=broad-except
                payload = {"error": str(error)}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            time.sleep(1)

    return Response(_generate(), mimetype="text/event-stream")


@_flask_app.route("/api/proxy_nodes", methods=["GET", "POST"])
@login_required
def manage_proxy_nodes():
    """List stored proxy nodes, or add one from an ss:// share link."""
    stored = web_application.config.setdefault("proxy_nodes", [])
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        try:
            node = proxy_nodes.parse_ss_link(payload.get("link"))
        except ValueError as error:
            return jsonify({"ok": False, "message": str(error)}), 400
        for existing in stored:
            if (
                existing.get("server") == node["server"]
                and int(existing.get("server_port") or 0) == node["server_port"]
            ):
                return jsonify({"ok": False, "message": "该节点已在列表里"}), 409
        stored.append(node)
        try:
            web_application.update_config()
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(f"proxy node persisted in memory only: {error}")
    active = proxy_nodes.active_node()
    return jsonify(
        {
            "ok": True,
            "manageable": proxy_nodes.available(),
            "active": active,
            "nodes": [
                {
                    "index": index,
                    "name": item.get("name"),
                    "server": item.get("server"),
                    "server_port": item.get("server_port"),
                    "method": item.get("method"),
                    "active": (
                        item.get("server") == active.get("server")
                        and int(item.get("server_port") or 0)
                        == int(active.get("server_port") or -1)
                    ),
                }
                for index, item in enumerate(stored)
            ],
        }
    )


@_flask_app.route("/api/proxy_nodes/switch", methods=["POST"])
@login_required
def switch_proxy_node():
    """Rewrite the sing-box outbound to the chosen node and restart telegram-proxy."""
    payload = request.get_json(silent=True) or {}
    stored = web_application.config.get("proxy_nodes") or []
    try:
        node = stored[int(payload.get("index"))]
    except (TypeError, ValueError, IndexError):
        return jsonify({"ok": False, "message": "节点序号不正确"}), 400
    if get_download_state() is DownloadState.Downloading:
        return (
            jsonify(
                {
                    "ok": False,
                    "message": "请先点击「暂停全部」再切换代理：切换会重启代理容器，"
                    "正在传输的文件会被打断",
                }
            ),
            409,
        )
    try:
        proxy_nodes.switch_node(node)
    except Exception as error:  # pylint: disable=broad-except
        return jsonify({"ok": False, "message": str(error)}), 500
    return jsonify(
        {
            "ok": True,
            "message": f"已切换到 {node.get('name')}（{node.get('server')}），"
            "telegram-proxy 已重启，约 10 秒后生效",
        }
    )


@_flask_app.route("/api/proxy_nodes/delete", methods=["POST"])
@login_required
def delete_proxy_node():
    """Remove one stored node (does not touch the live sing-box config)."""
    payload = request.get_json(silent=True) or {}
    stored = web_application.config.get("proxy_nodes") or []
    try:
        removed = stored.pop(int(payload.get("index")))
    except (TypeError, ValueError, IndexError):
        return jsonify({"ok": False, "message": "节点序号不正确"}), 400
    try:
        web_application.update_config()
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(f"proxy node removal persisted in memory only: {error}")
    return jsonify({"ok": True, "message": f"已删除节点 {removed.get('name')}"})


@_flask_app.route("/api/speed_governor", methods=["POST"])
@login_required
def set_speed_governor_base():
    """Adjust the adaptive controller's starting concurrency (base only).

    The 16 ceiling is a red line and is not adjustable from the Web UI;
    values are clamped to [floor, ceiling] by the governor itself.
    """
    payload = request.get_json(silent=True) or {}
    try:
        requested = int(payload.get("base_concurrency"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "message": "并发数值不正确"}), 400
    applied = speed_governor.set_base(requested)
    try:
        adaptive = web_application.config.setdefault("adaptive_speed", {})
        adaptive["base_concurrency"] = applied
        web_application.update_config()
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(f"speed_governor base persisted in memory only: {error}")
    snapshot_data = speed_governor.snapshot()
    message = f"起步并发已调整为 {applied}，调速器将从这里重新上探"
    if applied != requested:
        message += f"（{requested} 超出允许范围 {snapshot_data['floor']}–{snapshot_data['ceiling']}，已自动收紧）"
    if snapshot_data["cooldown_remaining"] > 0:
        message += f"；当前限速熔断中，约 {int(snapshot_data['cooldown_remaining'] // 60) + 1} 分钟后生效"
    return jsonify({"ok": True, "applied": applied, "speed_governor": snapshot_data, "message": message})


@_flask_app.route("/api/system_status")
@login_required
def get_system_status():
    """Return service, task, and configured storage health."""
    now = time.monotonic()
    with _system_status_cache_lock:
        cached = _system_status_cache["value"]
        if (
            cached is not None
            and now - _system_status_cache["updated_at"] < _SYSTEM_STATUS_CACHE_TTL
        ):
            return jsonify(cached)
    transfer = _live_transfer_summary()
    scheduler = getattr(web_application, "download_scheduler", None)
    scheduler_status = scheduler.snapshot() if scheduler is not None else {
        "policy": "smooth_weighted_round_robin",
        "max_workers": getattr(web_application, "max_download_task", 0),
        "active_channels": 0,
        "queued": 0,
        "in_flight": 0,
        "dispatched": 0,
        "channels": [],
    }
    persisted_stats = _cached_persisted_stats()
    counts = dict(persisted_stats["channels"])
    counts["total"] = len(get_channel_configs(False))
    payload = {
            "ok": True,
            "uptime_seconds": max(int(time.time() - service_started_at), 0),
            "tasks": persisted_stats["tasks"],
            "channels": counts,
            "scheduler": scheduler_status,
            "storage": _storage_status(),
            "remote": _tailscale_status(),
            "remote_access": remote_access.status(),
            "governor": rate_governor.snapshot(),
            "speed_governor": speed_governor.snapshot(),
            "download_state": (
                "downloading"
                if get_download_state() is DownloadState.Downloading
                else "paused"
            ),
            "transfer": transfer,
            "server_time": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
    with _system_status_cache_lock:
        _system_status_cache.update({"updated_at": now, "value": payload})
    return jsonify(payload)


@_flask_app.route("/get_download_list")
@login_required
def get_download_list():
    """get download list"""
    if request.args.get("already_down") is None:
        return jsonify([])

    already_down = request.args.get("already_down") == "true"

    if already_down:
        history = list_downloads(limit=200)
        return jsonify(
            [_history_web_record(record) for record in history["records"]]
        )

    download_result = get_download_snapshot()
    paused = get_download_state() is DownloadState.StopDownload
    # Canonical per-channel titles, used to fill in any task row whose own
    # chat_title was empty at queue time (the file was queued before the scan
    # resolved the channel name). Without this the UI falls back to the raw
    # numeric chat_id for those rows.
    channel_titles = {
        str(row["chat_id"]): row["chat_title"]
        for row in get_channel_configs(False)
        if row.get("chat_title") and row["chat_title"] != str(row["chat_id"])
    }
    result = []
    live_task_keys = set()
    for chat_id, messages in download_result.items():
        for idx, value in messages.items():
            is_already_down = value["down_byte"] == value["total_size"]
            if is_already_down:
                continue
            stalled_seconds = max(
                int(time.time() - float(value.get("end_time") or time.time())), 0
            )
            stalled = stalled_seconds >= 15
            speed_bytes = (
                0
                if paused or stalled
                else max(int(value.get("download_speed") or 0), 0)
            )
            download_speed = (
                "等待继续"
                if paused
                else ("等待数据" if stalled else format_byte(speed_bytes) + "/s")
            )
            total_size = value["total_size"]
            downloaded_size = max(int(value.get("down_byte") or 0), 0)
            remaining_size = max(total_size - downloaded_size, 0)
            eta_seconds = remaining_size / speed_bytes if speed_bytes else None
            progress = (
                round(downloaded_size / total_size * 100, 1) if total_size else 0
            )
            result.append(
                {
                    "chat": str(chat_id),
                    "id": str(idx),
                    "filename": os.path.basename(value["file_name"]),
                    "total_size": format_byte(total_size),
                    "total_size_bytes": total_size,
                    "downloaded_size": format_byte(downloaded_size),
                    "downloaded_size_bytes": downloaded_size,
                    "remaining_size": format_byte(remaining_size),
                    "download_progress": str(progress),
                    "download_speed": download_speed,
                    "eta_seconds": round(eta_seconds) if eta_seconds is not None else None,
                    "eta": (
                        "等待继续"
                        if paused
                        else ("等待数据" if stalled else _format_duration(eta_seconds))
                    ),
                    "stalled": stalled,
                    "stalled_seconds": stalled_seconds,
                    "save_path": value["file_name"].replace("\\", "/"),
                    "status": "suspended" if paused else "downloading",
                }
            )
            live_task_keys.add((str(chat_id), str(idx)))

    persistent = list_tasks(
        statuses=("queued", "downloading", "retrying", "retry_requested"),
        limit=200,
        oldest_first=True,
    )
    persistent_by_key = {
        (str(task["chat_id"]), str(task["message_id"])): task
        for task in persistent["records"]
    }
    for live_task in result:
        metadata = persistent_by_key.get((live_task["chat"], live_task["id"]), {})
        live_task.update(
            {
                "chat_title": metadata.get("chat_title")
                or channel_titles.get(live_task["chat"], ""),
                "media_type": metadata.get("media_type", ""),
                "telegram_url": _telegram_message_url(
                    live_task["chat"], live_task["id"]
                ),
            }
        )
    status_text = {
        "queued": "等待中",
        "downloading": "准备下载",
        "retrying": "等待重试",
        "retry_requested": "恢复中",
    }
    for task in persistent["records"]:
        task_key = (str(task["chat_id"]), str(task["message_id"]))
        if task_key in live_task_keys:
            continue
        effective_status = (
            "suspended"
            if paused and task["status"] == "downloading"
            else task["status"]
        )
        result.append(
            {
                "chat": str(task["chat_id"]),
                "chat_title": task["chat_title"]
                or channel_titles.get(str(task["chat_id"]), ""),
                "id": str(task["message_id"]),
                "filename": task["file_name"] or f"消息 #{task['message_id']}",
                "total_size": format_byte(task["total_size"]),
                "total_size_bytes": task["total_size"],
                "downloaded_size": "0 B",
                "downloaded_size_bytes": 0,
                "remaining_size": format_byte(task["total_size"]),
                "download_progress": "0",
                "download_speed": (
                    "等待继续"
                    if effective_status == "suspended"
                    else status_text.get(task["status"], "等待中")
                ),
                "eta_seconds": None,
                "eta": "等待继续" if effective_status == "suspended" else "—",
                "save_path": task["save_path"].replace("\\", "/"),
                "status": effective_status,
                "media_type": task["media_type"],
                "telegram_url": _telegram_message_url(
                    task["chat_id"], task["message_id"]
                ),
            }
        )
    for position, task in enumerate(result, start=1):
        task["queue_position"] = position
    return jsonify(result)
