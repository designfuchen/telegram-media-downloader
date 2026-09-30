"""Local account profiles and a coordinated Telegram sign-out.

Profiles never share sessions, SQLite queues or console credentials. Only an
allowlist of device preferences is inherited when the user adds an account.
"""
import asyncio
import copy
import json
import os
from pathlib import Path
import re


INHERITED_SETTINGS = {
    "api_id", "api_hash", "proxy", "ss_proxy", "media_types", "file_formats",
    "file_path_prefix", "file_name_prefix", "file_name_prefix_split",
    "enable_download_txt", "date_format", "language", "max_download_task",
    "max_concurrent_transmissions", "media_session_pool_size",
}


def prepare_profile(root, profile_id, config):
    """Create a new private profile; never overwrite an existing one."""
    if not isinstance(profile_id, str) or not re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", profile_id
    ):
        raise ValueError("账号标识不正确。")
    root = Path(root).resolve(strict=True)
    accounts = root / "accounts"
    if accounts.is_symlink():
        raise ValueError("账号目录不可用。")
    accounts.mkdir(mode=0o700, exist_ok=True)
    target = accounts / profile_id
    target.mkdir(mode=0o700)  # Exclusive: an existing profile is never modified.
    preferences = {key: copy.deepcopy(config[key]) for key in INHERITED_SETTINGS if key in config}
    preferences["start_paused"] = True
    # A distinct output directory also avoids two accounts writing the same file.
    preferences["save_path"] = str(Path(config.get("save_path") or Path.home() / "Downloads/Telegram") / ("Account-" + profile_id[:8]))
    try:
        from module import config_io
        path = target / "bootstrap.json"
        config_io.atomic_write(path, config_io.encode(json.dumps(preferences, ensure_ascii=False).encode(), path))
    except BaseException:
        (target / "bootstrap.json").unlink(missing_ok=True)
        target.rmdir()
        raise


def account_identity(user):
    phone = str(getattr(user, "phone_number", "") or "")
    return {
        "id": str(user.id),
        "display_name": " ".join(filter(None, [getattr(user, "first_name", ""), getattr(user, "last_name", "")])) or "Telegram 账号",
        "username": str(getattr(user, "username", "") or ""),
        "phone_hint": ("•••• " + phone[-4:]) if phone else "",
    }


class AccountLogout:
    def __init__(self, application, client, workers):
        self.application, self.client, self.workers = application, client, workers
        self.lock = asyncio.Lock()
        self.finished = False

    async def run(self):
        async with self.lock:
            if self.finished:
                return
            self.application.account_logging_out = True
            self.application.telegram_ready = False
            try:
                # Stop download/watchdog roots before revoking authorization.
                for task in self.workers:
                    task.cancel()
                if self.workers:
                    _, pending = await asyncio.wait(self.workers, timeout=15)
                    if pending:
                        raise TimeoutError("下载任务仍在停止。")
                    await asyncio.gather(*self.workers, return_exceptions=True)
                await asyncio.wait_for(self.client.log_out(), timeout=20)
                self.application.telegram_account = None
                self.finished = True
            finally:
                self.application.account_logging_out = False
