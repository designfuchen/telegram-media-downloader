"""Platform-agnostic storage readiness checks for the download target.

Kept dependency-free so it is unit-testable without the Telegram stack.
Replaces the old macOS-only ``/Volumes`` assumption.
"""

import os
import threading
import time


_CACHE_LOCK = threading.Lock()
_CACHE = {}
_READY_TTL_SECONDS = 5.0
_FAILED_TTL_SECONDS = 1.0


def path_is_writable(path: str) -> bool:
    """Return True only if ``path`` is a directory we can actually write into."""
    if not path or not os.path.isdir(path):
        return False
    probe = os.path.join(path, f".tmd_write_test_{os.getpid()}")
    try:
        with open(probe, "w", encoding="utf-8") as handle:
            handle.write("ok")
        os.remove(probe)
        return True
    except OSError:
        return False


def nearest_mount_point(path: str):
    """Return the nearest existing ancestor that is a real mount point.

    Returns the absolute mount path, or ``None`` if none is found.
    """
    probe = os.path.abspath(path)
    while True:
        if os.path.isdir(probe) and os.path.ismount(probe):
            return probe
        parent = os.path.dirname(probe)
        if parent == probe:
            return probe if os.path.ismount(probe) else None
        probe = parent


def is_on_dedicated_mount(path: str) -> bool:
    """True when ``path`` lives on a mount that is NOT the root filesystem.

    A mounted NAS share resolves to its own mount point (e.g. ``/mnt/nas``),
    so this returns True. When the share is unmounted, the leftover local
    directory resolves up to the root ``/`` mount, so this returns False --
    exactly the signal we want to block downloads on.
    """
    mount = nearest_mount_point(path)
    if mount is None:
        return False
    return os.path.abspath(mount) != os.path.abspath(os.sep)


def storage_is_ready(save_path: str, require_mount: bool = False) -> bool:
    """Return True when the download target is safe to write to.

    When ``require_mount`` is set, additionally require the path to live on a
    dedicated mount (not the root filesystem). This guards the classic NAS
    footgun: an unmounted share leaves an empty, locally-writable directory
    behind, silently redirecting downloads to the wrong disk.
    """
    cache_key = (os.path.abspath(str(save_path or "")), bool(require_mount))
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached and now - cached["checked_at"] < cached["ttl"]:
            return bool(cached["ready"])

        ready = path_is_writable(save_path)
        if ready and require_mount:
            ready = is_on_dedicated_mount(save_path)
        _CACHE[cache_key] = {
            "ready": bool(ready),
            "checked_at": now,
            "ttl": _READY_TTL_SECONDS if ready else _FAILED_TTL_SECONDS,
        }
        return bool(ready)


def clear_storage_cache() -> None:
    """Clear cached probe results (used after config changes and by tests)."""
    with _CACHE_LOCK:
        _CACHE.clear()
