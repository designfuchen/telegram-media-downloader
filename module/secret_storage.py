"""Keychain-backed keys; secrets cross pipes rather than command-line arguments."""
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from utils import crypto

_keys = {}
_lock = threading.RLock()

def enabled():
    return bool(os.environ.get("TMD_KEYCHAIN_HELPER") or os.environ.get("TMD_SECRET_KEY"))

def _identity(path):
    return "vault-" + hashlib.sha256(str(Path(path).resolve()).encode()).hexdigest()

def _key(path, create=False):
    identity = _identity(path)
    with _lock:
        if identity in _keys:
            return _keys[identity]
        external = os.environ.get("TMD_SECRET_KEY")
        if external:
            try:
                key = base64.b64decode(external, validate=True)
            except ValueError as error:
                raise ValueError("TMD_SECRET_KEY 必须是 32 字节密钥的 Base64。") from error
        else:
            helper = os.environ.get("TMD_KEYCHAIN_HELPER")
            if sys.platform != "darwin" or not helper or not Path(helper).is_file():
                raise OSError("系统钥匙串不可用，请解锁钥匙串或重新安装应用。")
            result = subprocess.run([helper], input=json.dumps({"account": identity, "create": create}),
                                    text=True, capture_output=True, timeout=30, check=False)
            if result.returncode:
                raise OSError("无法读取系统钥匙串。请解锁钥匙串，并允许本应用访问。")
            try:
                key = base64.b64decode(result.stdout.strip(), validate=True)
            except ValueError as error:
                raise OSError("钥匙串返回的数据无效。") from error
        if len(key) != 32:
            raise ValueError("受保护存储需要 32 字节密钥。")
        _keys[identity] = key
        return key

def seal(data, path):
    return crypto.encrypt(data, _key(path, create=True), _identity(path).encode())

def unseal(data, path):
    try:
        return crypto.decrypt(data, _key(path), _identity(path).encode())
    except ValueError as error:
        raise ValueError("受保护数据校验失败。请勿覆盖原文件，检查钥匙串是否可用。") from error
