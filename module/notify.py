"""Lightweight push notifications (WxPusher) for batch-completion alerts.

This is intentionally dependency-free (stdlib urllib only) and best-effort:
any failure is logged and swallowed so a notification problem can never
interrupt or crash the downloader itself. It sends to WxPusher, whose API
server is reachable directly from a China-based NAS without the proxy, so
completion alerts still arrive even if the Telegram tunnel is down.

Docs: https://wxpusher.zjiecode.com/docs/
"""

import json
import threading
import urllib.request
from typing import List

from loguru import logger

_API_URL = "https://wxpusher.zjiecode.com/api/send/message"

_lock = threading.Lock()
_state = {
    "enabled": False,
    "app_token": "",
    "uids": [],  # type: List[str]
}


def configure(app_token: str = "", uids=None, enabled: bool = False) -> None:
    """Set WxPusher credentials from config (idempotent)."""
    normalized_uids = []
    if uids:
        if isinstance(uids, str):
            normalized_uids = [uids.strip()]
        else:
            normalized_uids = [str(u).strip() for u in uids if str(u).strip()]
    with _lock:
        _state["app_token"] = str(app_token or "").strip()
        _state["uids"] = normalized_uids
        # Only treat as enabled when explicitly on AND fully configured.
        _state["enabled"] = bool(
            enabled and _state["app_token"] and _state["uids"]
        )
    if _state["enabled"]:
        logger.info(
            f"WxPusher 通知已启用，接收人 {len(_state['uids'])} 个"
        )


def is_enabled() -> bool:
    with _lock:
        return bool(_state["enabled"])


def send(title: str, content: str) -> bool:
    """Send one push message. Best-effort; never raises."""
    with _lock:
        if not _state["enabled"]:
            return False
        app_token = _state["app_token"]
        uids = list(_state["uids"])

    payload = {
        "appToken": app_token,
        "content": content,
        "summary": title[:99] if title else "",
        "contentType": 1,  # plain text
        "uids": uids,
    }
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        _API_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8", "ignore")
        result = json.loads(body)
        if result.get("code") == 1000:
            logger.info("WxPusher 通知已发送")
            return True
        logger.warning(f"WxPusher 通知返回异常：{body}")
        return False
    except Exception as error:  # pylint: disable=broad-except
        logger.warning(f"WxPusher 通知发送失败：{error}")
        return False
