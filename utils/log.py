"""Redact credentials in both application and dependency logs."""
import logging
import re
import sys
import threading
import traceback
from loguru import logger

_values = set()
_lock = threading.RLock()
_SECRET_NAMES = {"api_hash", "bot_token", "web_login_secret", "password", "token", "cloudflared_token", "app_token", "auth_key", "uuid", "phone", "phone_number"}
_PATTERNS = [
    (re.compile(r"(?i)\b(?:ss|ssr|vmess|vless|trojan|hysteria2|hy2|tuic)://[^\s'\"]+"), "[节点已隐藏]"),
    (re.compile(r"(?i)\b[0-9a-f]{32}\b"), "[密钥已隐藏]"),
    (re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{25,}\b"), "[token 已隐藏]"),
    (re.compile(r"(?<!\w)\+\d[\d -]{7,18}\d"), "[手机号已隐藏]"),
    (re.compile(r"(?i)(?:api_hash|api_id|bot_token|password|token|auth_key|phone_number)\s*[:=]\s*[^,\s}]+"), "[凭证已隐藏]"),
    (re.compile(r"(?i)(https?://)[^/\s:@]+:[^/@\s]+@"), r"\1[凭证已隐藏]@"),
]

def register_secrets(document):
    if not isinstance(document, dict):
        return
    with _lock:
        for name, value in document.items():
            if name in _SECRET_NAMES and isinstance(value, (str, int)) and len(str(value)) >= 4:
                _values.add(str(value))
            if isinstance(value, dict):
                register_secrets(value)
            elif isinstance(value, list):
                for item in value:
                    register_secrets(item)

def redact(text):
    text = str(text)
    with _lock:
        values = sorted(_values, key=len, reverse=True)
    for value in values:
        text = text.replace(value, "[凭证已隐藏]")
    for pattern, replacement in _PATTERNS:
        text = pattern.sub(replacement, text)
    return text

def _patch_record(record):
    record["message"] = redact(record["message"])
    if record["exception"]:
        kind, value, tb = record["exception"]
        record["exception"] = type(record["exception"])(kind, Exception(redact(value)), tb)

# Do not include diagnostic local variables in tracebacks.
logger.remove()
logger.configure(patcher=_patch_record)
logger.add(sys.stderr, diagnose=False, backtrace=False)
_original_factory = logging.getLogRecordFactory()

def _factory(*args, **kwargs):
    record = _original_factory(*args, **kwargs)
    record.msg, record.args = redact(record.getMessage()), ()
    if record.exc_info:
        record.exc_text = redact("".join(traceback.format_exception(*record.exc_info)))
        record.exc_info = None
    return record

logging.setLogRecordFactory(_factory)

class LogFilter(logging.Filter):
    def filter(self, record):
        return record.funcName != "invoke"
