"""Atomic configuration persistence with optional Keychain-backed encryption."""
import base64
from collections.abc import Mapping
import io
import json
import os
from pathlib import Path
import tempfile
import threading
from module import secret_storage

FORMAT = "telegram-downloader-vault-v1"
_serialization_lock = threading.RLock()

def atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise OSError("不允许写入符号链接配置。")
    descriptor, temporary = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)

def encode(data, path):
    if secret_storage.enabled():
        return json.dumps({"format": FORMAT, "ciphertext": base64.b64encode(
            secret_storage.seal(data, path)).decode()}, separators=(",", ":")).encode()
    return data

def decode(data, path):
    if data.lstrip().startswith(b"{"):
        try:
            document = json.loads(data)
        except ValueError:
            document = {}
        if document.get("format") == FORMAT:
            if not secret_storage.enabled():
                raise OSError("受保护配置需要系统钥匙串或原来的 TMD_SECRET_KEY。")
            return secret_storage.unseal(base64.b64decode(document["ciphertext"], validate=True), path)
    return data

def load(path, serializer):
    data = decode(Path(path).read_bytes(), path).decode("utf-8")
    loader = getattr(serializer, "safe_load", None) or serializer.load
    with _serialization_lock:
        return loader(data) or {}

def reader(path):
    return io.StringIO(decode(Path(path).read_bytes(), path).decode("utf-8"))

def dump(path, document, serializer):
    with _serialization_lock:
        _dump(path, document, serializer)

def _dump(path, document, serializer):
    if not secret_storage.enabled() and Path(path).exists():
        # A caller must not overwrite an existing vault after losing its key.
        decode(Path(path).read_bytes(), path)
    stream = io.StringIO()
    if hasattr(serializer, "safe_dump"):
        # Application.load_config uses ruamel's round-trip containers. PyYAML's
        # SafeDumper cannot serialize those subclasses, including nested ones.
        serializer.safe_dump(_plain_values(document), stream, allow_unicode=True)
    else:
        serializer.dump(document, stream)
    atomic_write(path, encode(stream.getvalue().encode("utf-8"), path))

def _plain_values(value):
    if isinstance(value, Mapping):
        return {_plain_values(key): _plain_values(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_values(item) for item in value]
    if isinstance(value, str):
        return str(value)
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return float(value)
    return value
