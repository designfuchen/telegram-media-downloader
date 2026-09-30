"""Remote access without a VPN.

Manages a Cloudflare Tunnel (``cloudflared``) so the Web console is reachable
from any phone or computer over an HTTPS URL, with no Tailscale, no router
port-forwarding and no public IP required.

Two modes:
  * Quick tunnel  -- ``cloudflared tunnel --url http://127.0.0.1:<port>``.
    Zero config, gives an ephemeral ``https://<random>.trycloudflare.com`` URL.
    Perfect for personal use; the URL changes on each restart.
  * Named tunnel  -- if the user configured a ``cloudflared`` token, run
    ``cloudflared tunnel run --token <token>`` for a stable custom domain.

The module is dependency-free (only stdlib + subprocess) so its parsing logic
is unit-testable without a running tunnel.
"""

import re
import shutil
import subprocess
import threading
import time
from typing import Optional

# Public trycloudflare URL pattern printed by the quick tunnel on startup.
_QUICK_URL_RE = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")

_state_lock = threading.RLock()
_state = {
    "enabled": False,
    "mode": "",          # "quick" | "named" | ""
    "url": "",
    "running": False,
    "message": "未启动",
    "updated_at": 0.0,
}
_process: Optional[subprocess.Popen] = None
_reader_thread: Optional[threading.Thread] = None


def cloudflared_path() -> Optional[str]:
    """Locate the cloudflared binary, or None if it is not installed."""
    return shutil.which("cloudflared")


def is_installed() -> bool:
    """Whether the cloudflared binary is available on this machine."""
    return cloudflared_path() is not None


def parse_quick_url(line: str) -> Optional[str]:
    """Extract a trycloudflare URL from a line of cloudflared output."""
    match = _QUICK_URL_RE.search(line or "")
    return match.group(0) if match else None


def _set_state(**changes) -> None:
    with _state_lock:
        _state.update(changes)
        _state["updated_at"] = time.time()


def status() -> dict:
    """Return a copy of the current remote-access state for the Web console."""
    with _state_lock:
        snapshot = dict(_state)
    snapshot["installed"] = is_installed()
    return snapshot


def _drain_output(process: subprocess.Popen) -> None:
    """Read cloudflared stdout/stderr, capturing the public URL when it appears."""
    stream = process.stdout
    if stream is None:
        return
    for raw in iter(stream.readline, ""):
        url = parse_quick_url(raw)
        if url:
            _set_state(url=url, running=True, message="隧道已就绪")
        if process.poll() is not None:
            break
    _set_state(running=False, message="隧道已停止")


def start(port: int, token: str = "", extra_args: Optional[list] = None) -> dict:
    """Start a Cloudflare Tunnel to the local Web console.

    Parameters
    ----------
    port: int
        Local Web console port to expose.
    token: str
        Optional named-tunnel token for a stable custom domain.
    """
    global _process, _reader_thread

    binary = cloudflared_path()
    if not binary:
        _set_state(
            enabled=False,
            running=False,
            message="未检测到 cloudflared，请先安装：brew install cloudflared",
        )
        return status()

    with _state_lock:
        already = _process is not None and _process.poll() is None
    if already:
        return status()

    if token:
        mode = "named"
        command = [binary, "tunnel", "run"]
    else:
        mode = "quick"
        command = [
            binary,
            "tunnel",
            "--no-autoupdate",
            "--url",
            f"http://127.0.0.1:{int(port)}",
        ]
    if extra_args:
        command.extend(str(arg) for arg in extra_args)

    try:
        # pylint: disable = consider-using-with
        _process = subprocess.Popen(
            command,
            env=dict(os.environ, **({"TUNNEL_TOKEN": token} if token else {})),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except OSError as exc:
        _set_state(enabled=False, running=False, message=f"启动失败：{exc}")
        return status()

    _set_state(
        enabled=True,
        mode=mode,
        url="" if mode == "quick" else _state.get("url", ""),
        running=True,
        message="正在建立隧道…",
    )
    _reader_thread = threading.Thread(
        target=_drain_output, args=(_process,), daemon=True
    )
    _reader_thread.start()
    return status()


def stop() -> dict:
    """Terminate the running Cloudflare Tunnel, if any."""
    global _process
    with _state_lock:
        process = _process
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
    _process = None
    _set_state(enabled=False, running=False, url="", message="已停止")
    return status()


def ensure_started(config: dict, port: int) -> dict:
    """Start the tunnel at boot if remote access is enabled in config."""
    remote = (config or {}).get("remote_access") or {}
    if not remote.get("enabled"):
        return status()
    if str(remote.get("provider", "cloudflared")) != "cloudflared":
        return status()
    return start(port, token=str(remote.get("cloudflared_token") or ""))
