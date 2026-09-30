"""Persistent task, channel-library, and import state backed by SQLite."""

import csv
import io
import os
import re
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import parse_qs, urlparse

from module.download_history import get_history_path


DEFAULT_MAX_ATTEMPTS = 3
RETRY_DELAYS = (60, 300, 900)
TRANSIENT_RETRY_DELAYS = (60, 300, 900, 1800, 3600)
# A resumable attempt that added bytes is converging; retry it almost at once so
# a 2 GB file finishes in minutes instead of one hourly slice per attempt.
PROGRESS_RETRY_DELAY = 30
# Ceiling for transport-class failures. Generous on purpose: partial files carry
# real downloaded bytes and must not be discarded over a proxy outage. It exists
# so "retrying" cannot become a permanent state without the operator noticing.
TRANSIENT_MAX_ATTEMPTS = 240
TRANSIENT_TRANSFER_ERROR_MARKERS = (
    "等待首字节超过",
    "传输无进度超过",
    "telegram 未返回下载文件",
    "telegram 限流",
    "文件引用",
    "broken pipe",
    "connection lost",
    "connection reset",
    "socket.send",
    "socket closed",
    "another coroutine",
    "transport endpoint",
    "timed out",
    "timeout",
    "下载超时",
    "database is locked",
    "database table is locked",
)
TERMINAL_STATUSES = {"completed", "skipped", "failed", "cancelled"}
ACTIVE_STATUSES = {"queued", "downloading", "retrying", "retry_requested", "paused"}
CHANNEL_PRIORITIES = {"low", "normal", "high"}
IMPORT_ITEM_STATUSES = {
    "pending",
    "validating",
    "valid",
    "duplicate",
    "invalid",
    "imported",
}
CHANNEL_LIFECYCLE_STATES = {
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

_SCHEMA_READY = set()
_SCHEMA_READY_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def task_queued_age_seconds(chat_id, message_id: int) -> float:
    """Seconds since this task was (re)queued, or 0.0 if unknown.

    Used to decide whether a file's media reference is old enough to have
    expired and must be proactively refreshed before download. A file scanned
    hours ago (deep-queue tail) returns a large age; a freshly scanned or
    just-recovered task returns a small one.
    """
    with _connect() as connection:
        row = connection.execute(
            "SELECT queued_at FROM download_tasks WHERE chat_id = ? AND message_id = ?",
            (str(chat_id), int(message_id)),
        ).fetchone()
    if not row or not row["queued_at"]:
        return 0.0
    try:
        queued = datetime.fromisoformat(row["queued_at"])
    except (TypeError, ValueError):
        return 0.0
    if queued.tzinfo is None:
        queued = queued.replace(tzinfo=timezone.utc)
    return max((datetime.now(timezone.utc) - queued).total_seconds(), 0.0)


def _ensure_columns(
    connection: sqlite3.Connection, table: str, columns: dict
) -> None:
    """Add new nullable/defaulted columns without rebuilding an existing table."""
    existing = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }
    for name, definition in columns.items():
        if name not in existing:
            try:
                connection.execute(
                    f"ALTER TABLE {table} ADD COLUMN {name} {definition}"
                )
            except sqlite3.OperationalError as error:
                # Flask and the downloader can initialize a brand-new database
                # at the same time. A concurrent successful ALTER is harmless.
                if "duplicate column" not in str(error).lower():
                    raise


def _connect() -> sqlite3.Connection:
    database_path = Path(
        os.environ.get("TMD_TASK_DB") or get_history_path()
    ).expanduser()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=10000")
    schema_key = str(database_path.resolve())
    with _SCHEMA_READY_LOCK:
        if schema_key in _SCHEMA_READY:
            return connection
    # A production database can contain millions of completed-file rows. Once
    # the durable schema is already present, startup must remain read-only:
    # replaying CREATE INDEX / legacy de-duplication here can hold SQLite's
    # writer lock for minutes and prevent both the Web console and downloads
    # from starting. Fresh databases still take the full initialization path
    # below because this sentinel index does not exist yet.
    schema_exists = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'index' AND name = 'idx_download_history_chat_message'
        """
    ).fetchone()
    if schema_exists:
        with _SCHEMA_READY_LOCK:
            _SCHEMA_READY.add(schema_key)
        return connection
    # Changing journal_mode takes a database-wide lock. Doing it on every
    # short-lived connection can collide with the web reader or the single DB
    # writer and incorrectly fail a completed file with "database is locked".
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS download_tasks (
            chat_id TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            chat_title TEXT NOT NULL DEFAULT '',
            file_name TEXT NOT NULL DEFAULT '',
            media_type TEXT NOT NULL DEFAULT '',
            total_size INTEGER NOT NULL DEFAULT 0,
            save_path TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            error TEXT NOT NULL DEFAULT '',
            queued_at TEXT NOT NULL,
            started_at TEXT,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            next_retry_at TEXT,
            PRIMARY KEY (chat_id, message_id)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_download_tasks_status_updated
        ON download_tasks(status, updated_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_download_tasks_chat_status
        ON download_tasks(chat_id, status, updated_at DESC)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS download_channel_state (
            chat_id TEXT PRIMARY KEY,
            chat_title TEXT NOT NULL DEFAULT '',
            paused INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    _ensure_columns(
        connection,
        "download_channel_state",
        {
            "enabled": "INTEGER NOT NULL DEFAULT 1",
            "priority": "TEXT NOT NULL DEFAULT 'normal'",
            "group_name": "TEXT NOT NULL DEFAULT ''",
            "last_read_message_id": "INTEGER NOT NULL DEFAULT 0",
            "download_filter": "TEXT NOT NULL DEFAULT ''",
            "upload_telegram_chat_id": "TEXT NOT NULL DEFAULT ''",
            "validation_status": "TEXT NOT NULL DEFAULT 'valid'",
            "validation_error": "TEXT NOT NULL DEFAULT ''",
            "last_validated_at": "TEXT",
            "scan_status": "TEXT NOT NULL DEFAULT 'idle'",
            "last_scanned_at": "TEXT",
            "last_success_at": "TEXT",
            "next_scan_at": "TEXT",
            "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
            "source": "TEXT NOT NULL DEFAULT 'discovered'",
            "import_batch_id": "INTEGER",
            "config_revision": "INTEGER NOT NULL DEFAULT 1",
            "lifecycle_state": "TEXT NOT NULL DEFAULT 'ready'",
            "state_reason": "TEXT NOT NULL DEFAULT ''",
            "state_changed_at": "TEXT",
            "next_action_at": "TEXT",
            "state_version": "INTEGER NOT NULL DEFAULT 1",
        },
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_channel_library_enabled_priority
        ON download_channel_state(enabled, priority, updated_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_channel_library_validation
        ON download_channel_state(validation_status, updated_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_channel_library_lifecycle
        ON download_channel_state(lifecycle_state, next_action_at, updated_at DESC)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_state_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            from_state TEXT NOT NULL DEFAULT '',
            to_state TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_channel_state_events_chat
        ON channel_state_events(chat_id, id DESC)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_import_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'validating',
            total_count INTEGER NOT NULL DEFAULT 0,
            pending_count INTEGER NOT NULL DEFAULT 0,
            valid_count INTEGER NOT NULL DEFAULT 0,
            duplicate_count INTEGER NOT NULL DEFAULT 0,
            invalid_count INTEGER NOT NULL DEFAULT 0,
            imported_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_import_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            line_number INTEGER NOT NULL,
            raw_value TEXT NOT NULL DEFAULT '',
            chat_id TEXT NOT NULL DEFAULT '',
            start_message_id INTEGER NOT NULL DEFAULT 0,
            group_name TEXT NOT NULL DEFAULT '',
            priority TEXT NOT NULL DEFAULT 'normal',
            download_filter TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            error TEXT NOT NULL DEFAULT '',
            chat_title TEXT NOT NULL DEFAULT '',
            validation_attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(batch_id, line_number),
            FOREIGN KEY(batch_id) REFERENCES channel_import_batches(id)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_channel_import_items_work
        ON channel_import_items(status, next_retry_at, updated_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_channel_import_items_batch
        ON channel_import_items(batch_id, line_number)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS download_history (
            chat_id TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            chat_title TEXT NOT NULL DEFAULT '',
            file_name TEXT NOT NULL,
            total_size INTEGER NOT NULL,
            save_path TEXT NOT NULL,
            media_type TEXT NOT NULL DEFAULT '',
            completed_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, message_id, save_path)
        )
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_download_history_chat_completed
        ON download_history(chat_id, completed_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_download_history_completed_message
        ON download_history(completed_at DESC, message_id DESC)
        """
    )
    # One Telegram message represents one downloaded media item. Older builds
    # keyed history by the destination path as well, so a naming/path change
    # could count the same message twice in the channel total.
    connection.execute(
        """
        DELETE FROM download_history
        WHERE rowid NOT IN (
            SELECT MAX(rowid) FROM download_history
            GROUP BY chat_id, message_id
        )
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_download_history_chat_message
        ON download_history(chat_id, message_id)
        """
    )
    connection.commit()
    with _SCHEMA_READY_LOCK:
        _SCHEMA_READY.add(schema_key)
    return connection


def _upsert_channel(
    connection: sqlite3.Connection, chat_id, chat_title: str = ""
) -> None:
    """Register a configured or discovered channel without losing its title."""
    chat_key = str(chat_id)
    title = str(chat_title or "")
    now = _now()
    connection.execute(
        """
        INSERT INTO download_channel_state (
            chat_id, chat_title, paused, created_at, updated_at
        ) VALUES (?, ?, 0, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            chat_title = CASE
                WHEN excluded.chat_title = '' OR excluded.chat_title = excluded.chat_id
                    THEN download_channel_state.chat_title
                ELSE excluded.chat_title
            END,
            updated_at = CASE
                WHEN excluded.chat_title = '' OR excluded.chat_title = excluded.chat_id
                    THEN download_channel_state.updated_at
                ELSE excluded.updated_at
            END
        """,
        (chat_key, title, now, now),
    )


def _refresh_channel_state(connection: sqlite3.Connection, chat_id) -> Optional[dict]:
    """Reconcile one persisted channel lifecycle from its authoritative evidence."""
    chat_key = str(chat_id)
    channel = connection.execute(
        """
        SELECT enabled, paused, validation_status, validation_error, scan_status,
               last_success_at, lifecycle_state, state_reason, state_changed_at,
               next_action_at
        FROM download_channel_state WHERE chat_id = ?
        """,
        (chat_key,),
    ).fetchone()
    if channel is None:
        return None
    tasks = connection.execute(
        """
        SELECT
            SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
            SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END) AS downloading,
            SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END) AS retrying,
            SUM(CASE WHEN status = 'retry_requested' THEN 1 ELSE 0 END) AS retry_requested,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status = 'paused' THEN 1 ELSE 0 END) AS paused,
            SUM(CASE WHEN status IN ('completed', 'skipped') THEN 1 ELSE 0 END) AS finished,
            MIN(CASE WHEN status = 'retrying' THEN next_retry_at END) AS next_retry_at
        FROM download_tasks WHERE chat_id = ?
        """,
        (chat_key,),
    ).fetchone()
    history_count = connection.execute(
        "SELECT COUNT(*) FROM download_history WHERE chat_id = ?", (chat_key,)
    ).fetchone()[0]
    counts = {key: int(tasks[key] or 0) for key in tasks.keys() if key != "next_retry_at"}
    next_action_at = tasks["next_retry_at"] or channel["next_action_at"]
    # Historical success only proves that an older scan completed.  It must
    # never make a currently interrupted/new scan look complete.
    completion_evidence = channel["scan_status"] == "completed"

    if not channel["enabled"]:
        if (
            completion_evidence
            and not counts["queued"]
            and not counts["downloading"]
            and not counts["retrying"]
            and not counts["retry_requested"]
            and not counts["failed"]
            and (counts["finished"] or history_count)
        ):
            state = "completed"
            reason = "全部下载完成，已归档，不再访问 Telegram"
            next_action_at = None
        else:
            state, reason, next_action_at = "disabled", "频道已停用，任务与历史记录保留", None
    elif channel["validation_status"] == "pending":
        state, reason = "validating", "等待 Telegram 访问权限校验"
    elif channel["validation_status"] == "invalid":
        state = "blocked"
        reason = channel["validation_error"] or "Telegram 权限校验失败"
        next_action_at = None
    elif channel["paused"]:
        state, reason, next_action_at = "paused", "频道已暂停，不会领取新任务", None
    elif counts["downloading"]:
        state = "downloading"
        reason = f"{counts['downloading']} 个文件正在下载"
    elif channel["scan_status"] == "scanning":
        state, reason = "scanning", "正在读取频道消息并生成任务"
    elif counts["retrying"]:
        state = "backoff"
        reason = f"{counts['retrying']} 个任务等待自动重试"
    elif counts["queued"] + counts["retry_requested"]:
        state = "queued"
        reason = f"{counts['queued'] + counts['retry_requested']} 个文件等待公平调度"
        next_action_at = None
    elif counts["failed"]:
        state = "blocked"
        reason = f"{counts['failed']} 个任务重试耗尽，需要处理"
        next_action_at = None
    elif (
        completion_evidence
        and not counts["paused"]
        and (counts["finished"] or history_count)
    ):
        state, reason, next_action_at = "completed", "当前已发现任务处理完成", None
    elif channel["scan_status"] == "backoff":
        state = "backoff"
        reason = channel["validation_error"] or "Telegram 限流，等待自动恢复"
    elif channel["scan_status"] == "failed":
        state = "blocked"
        reason = channel["validation_error"] or "频道扫描失败"
        next_action_at = None
    elif counts["finished"] or history_count:
        state, reason, next_action_at = "completed", "当前已发现任务处理完成", None
    else:
        state, reason, next_action_at = "ready", "等待下一次频道扫描", None

    previous = channel["lifecycle_state"] or ""
    first_observation = channel["state_changed_at"] is None
    state_changed = previous != state
    if (
        not first_observation
        and not state_changed
        and str(channel["state_reason"] or "") == str(reason or "")
        and (channel["next_action_at"] or None) == (next_action_at or None)
    ):
        return {
            "chat_id": chat_key,
            "state": state,
            "reason": reason,
            "next_action_at": next_action_at,
            "changed": False,
        }
    now = _now()
    connection.execute(
        """
        UPDATE download_channel_state
        SET lifecycle_state = ?, state_reason = ?, next_action_at = ?,
            state_changed_at = CASE WHEN ? OR state_changed_at IS NULL THEN ?
                                    ELSE state_changed_at END,
            state_version = state_version + CASE WHEN ? THEN 1 ELSE 0 END
        WHERE chat_id = ?
        """,
        (state, reason, next_action_at, state_changed, now, state_changed, chat_key),
    )
    if state_changed or first_observation:
        connection.execute(
            """
            INSERT INTO channel_state_events (
                chat_id, from_state, to_state, reason, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (chat_key, "" if first_observation else previous, state, reason, now),
        )
        connection.execute(
            """
            DELETE FROM channel_state_events
            WHERE chat_id = ? AND id NOT IN (
                SELECT id FROM channel_state_events
                WHERE chat_id = ? ORDER BY id DESC LIMIT 100
            )
            """,
            (chat_key, chat_key),
        )
    return {
        "chat_id": chat_key,
        "state": state,
        "reason": reason,
        "next_action_at": next_action_at,
        "changed": state_changed,
    }


def refresh_channel_state(chat_id) -> Optional[dict]:
    """Public reconciliation entrypoint for one channel."""
    with _connect() as connection:
        return _refresh_channel_state(connection, chat_id)


def list_channel_state_events(chat_id, limit: int = 30) -> list:
    """Return recent lifecycle transitions for one channel."""
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT id, chat_id, from_state, to_state, reason, created_at
            FROM channel_state_events
            WHERE chat_id = ? ORDER BY id DESC LIMIT ?
            """,
            (str(chat_id), min(max(int(limit), 1), 200)),
        ).fetchall()
    return [dict(row) for row in rows]


def register_channels(channels: Iterable) -> int:
    """Register configured channels so empty channels still appear in the UI."""
    changed = 0
    with _connect() as connection:
        for channel in channels:
            if isinstance(channel, dict):
                chat_id = channel.get("chat_id")
                chat_title = channel.get("chat_title", "")
            else:
                chat_id = channel
                chat_title = ""
            if chat_id is None or str(chat_id).strip() == "":
                continue
            _upsert_channel(connection, chat_id, chat_title)
            _refresh_channel_state(connection, chat_id)
            changed += 1
    return changed


def migrate_config_channels(channels: Iterable) -> int:
    """Move legacy YAML channel configuration into the SQLite channel library."""
    changed = 0
    with _connect() as connection:
        for item in channels or []:
            if not isinstance(item, dict):
                item = {"chat_id": item}
            chat_id = item.get("chat_id")
            if chat_id is None or not str(chat_id).strip():
                continue
            now = _now()
            _upsert_channel(connection, chat_id, item.get("chat_title", ""))
            connection.execute(
                """
                UPDATE download_channel_state
                SET enabled = CASE WHEN ? IS NULL THEN enabled ELSE ? END,
                    priority = CASE WHEN ? IS NULL THEN priority ELSE ? END,
                    group_name = CASE WHEN ? IS NULL THEN group_name ELSE ? END,
                    last_read_message_id = MAX(last_read_message_id, ?),
                    download_filter = CASE WHEN ? IS NULL THEN download_filter ELSE ? END,
                    upload_telegram_chat_id = CASE
                        WHEN ? IS NULL THEN upload_telegram_chat_id ELSE ? END,
                    validation_status = CASE
                        WHEN validation_status IN ('pending', 'invalid')
                            THEN validation_status
                        ELSE 'valid'
                    END,
                    source = CASE WHEN source = 'discovered' THEN 'config_migration'
                                  ELSE source END,
                    updated_at = ?
                WHERE chat_id = ?
                """,
                (
                    None if "enabled" not in item else 1 if item.get("enabled") else 0,
                    1 if item.get("enabled", True) else 0,
                    None if "priority" not in item else _priority(item.get("priority")),
                    _priority(item.get("priority")),
                    None if "group_name" not in item else str(item.get("group_name") or "").strip(),
                    str(item.get("group_name") or "").strip(),
                    _non_negative_int(item.get("last_read_message_id", 0)),
                    None if "download_filter" not in item else str(item.get("download_filter") or "").strip(),
                    str(item.get("download_filter") or "").strip(),
                    None if "upload_telegram_chat_id" not in item else str(item.get("upload_telegram_chat_id") or "").strip(),
                    str(item.get("upload_telegram_chat_id") or "").strip(),
                    now,
                    str(chat_id).strip(),
                ),
            )
            _refresh_channel_state(connection, chat_id)
            changed += 1
    return changed


def get_channel_configs(enabled_only: bool = True) -> list:
    """Return channel-library rows in downloader-ready form."""
    where = "WHERE enabled = 1" if enabled_only else ""
    with _connect() as connection:
        rows = connection.execute(
            f"""
            SELECT chat_id, chat_title, enabled, paused, priority, group_name,
                   last_read_message_id, download_filter,
                   upload_telegram_chat_id, validation_status,
                   validation_error, last_validated_at, source, import_batch_id,
                   config_revision, lifecycle_state, state_reason,
                   state_changed_at, next_action_at, state_version,
                   created_at, updated_at
            FROM download_channel_state
            {where}
            ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END,
                     created_at, chat_id
            """
        ).fetchall()
    return [dict(row) for row in rows]


def get_channel_config(chat_id) -> Optional[dict]:
    """Return one channel-library row."""
    with _connect() as connection:
        row = connection.execute(
            "SELECT * FROM download_channel_state WHERE chat_id = ?",
            (str(chat_id).strip(),),
        ).fetchone()
    return dict(row) if row else None


def upsert_channel_config(channel: dict, source: str = "web") -> dict:
    """Create or update one channel without touching its task/history rows."""
    if not isinstance(channel, dict):
        raise ValueError("频道配置格式不正确")
    chat_id, linked_message_id = normalize_channel_reference(channel.get("chat_id"))
    start_message_id = _non_negative_int(
        channel.get("last_read_message_id", linked_message_id or 0)
    )
    now = _now()
    with _connect() as connection:
        _upsert_channel(connection, chat_id, channel.get("chat_title", ""))
        connection.execute(
            """
            UPDATE download_channel_state
            SET enabled = ?, priority = ?, group_name = ?,
                last_read_message_id = ?, download_filter = ?,
                upload_telegram_chat_id = ?, source = ?, updated_at = ?
                , config_revision = config_revision + 1
            WHERE chat_id = ?
            """,
            (
                1 if channel.get("enabled", True) else 0,
                _priority(channel.get("priority")),
                str(channel.get("group_name") or "").strip(),
                start_message_id,
                str(channel.get("download_filter") or "").strip(),
                str(channel.get("upload_telegram_chat_id") or "").strip(),
                str(source or "web"),
                now,
                chat_id,
            ),
        )
        _refresh_channel_state(connection, chat_id)
    return get_channel_config(chat_id) or {}


def set_channel_enabled(chat_id, enabled: bool) -> int:
    """Enable or disable one channel in the runtime registry."""
    with _connect() as connection:
        cursor = connection.execute(
            """
            UPDATE download_channel_state
            SET enabled = ?, updated_at = ?, config_revision = config_revision + 1
            WHERE chat_id = ?
            """,
            (1 if enabled else 0, _now(), str(chat_id).strip()),
        )
        if enabled and cursor.rowcount > 0:
            # Disabled channels keep every task row.  Re-enabling must make
            # those rows visible to the persisted retry hydrator; otherwise a
            # task that was already queued before disable can remain stranded
            # until the entire service is restarted.
            connection.execute(
                """
                UPDATE download_tasks
                SET status = 'retry_requested', updated_at = ?,
                    next_retry_at = NULL
                WHERE chat_id = ?
                  AND status IN ('queued', 'paused', 'retrying')
                """,
                (_now(), str(chat_id).strip()),
            )
        _refresh_channel_state(connection, chat_id)
    return max(cursor.rowcount, 0)


def refresh_completed_channels(chat_ids) -> dict:
    """Re-open finished channels so the scanner looks for new messages.

    A channel that finishes is retired by `_retire_completed_channel`
    (`enabled = 0`), and `channel_registry_worker` only ever looks at enabled
    rows — so bumping `config_revision` alone would be silently ignored. Both
    have to move together.

    The scan that follows is incremental: it resumes from
    `last_read_message_id`, so this costs one pass over whatever was posted
    since the channel finished, not the whole history.

    Only retired-and-completed rows are touched. A paused or actively
    downloading channel is reported back as skipped rather than resumed,
    because "check for updates" must never restart a transfer the operator
    deliberately stopped.
    """
    keys = []
    for chat_id in chat_ids or []:
        key = str(chat_id).strip()
        if key and key not in keys:
            keys.append(key)
    result = {"refreshed": [], "skipped": [], "missing": []}
    if not keys:
        return result

    with _connect() as connection:
        for key in keys:
            row = connection.execute(
                "SELECT enabled, scan_status FROM download_channel_state"
                " WHERE chat_id = ?",
                (key,),
            ).fetchone()
            if row is None:
                result["missing"].append(key)
                continue
            if int(row["enabled"] or 0) or row["scan_status"] != "completed":
                result["skipped"].append(key)
                continue
            connection.execute(
                """
                UPDATE download_channel_state
                SET enabled = 1, paused = 0, updated_at = ?,
                    config_revision = config_revision + 1
                WHERE chat_id = ?
                """,
                (_now(), key),
            )
            _refresh_channel_state(connection, key)
            result["refreshed"].append(key)
    return result



def _retire_completed_channel(
    connection: sqlite3.Connection,
    chat_id,
    require_completed_scan: bool = True,
) -> bool:
    """Retire one channel using an existing transaction."""
    chat_key = str(chat_id).strip()
    channel = connection.execute(
        "SELECT enabled, scan_status, last_success_at FROM download_channel_state WHERE chat_id = ?",
        (chat_key,),
    ).fetchone()
    if channel is None or not channel["enabled"]:
        return False
    if require_completed_scan and channel["scan_status"] != "completed":
        return False

    counts = connection.execute(
        """
        SELECT
            SUM(CASE WHEN status IN (
                'queued', 'downloading', 'retrying', 'retry_requested', 'paused'
            ) THEN 1 ELSE 0 END) AS active,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status IN ('completed', 'skipped')
                THEN 1 ELSE 0 END) AS finished
        FROM download_tasks WHERE chat_id = ?
        """,
        (chat_key,),
    ).fetchone()
    history_count = connection.execute(
        "SELECT COUNT(*) FROM download_history WHERE chat_id = ?",
        (chat_key,),
    ).fetchone()[0]
    if int(counts["active"] or 0) or int(counts["failed"] or 0):
        return False
    if not int(counts["finished"] or 0) and not int(history_count or 0):
        return False

    cursor = connection.execute(
        """
        UPDATE download_channel_state
        SET enabled = 0, paused = 0, scan_status = 'completed', validation_error = '',
            consecutive_failures = 0, updated_at = ?,
            config_revision = config_revision + 1
        WHERE chat_id = ? AND enabled = 1
        """,
        (_now(), chat_key),
    )
    if cursor.rowcount <= 0:
        return False
    _refresh_channel_state(connection, chat_key)
    return True


def retire_completed_channel(chat_id) -> bool:
    """Disable a completed channel that is no longer accessible.

    This is intentionally conservative: a channel is retired only when it has
    completion evidence and no queued, downloading, retrying, retry-requested,
    or failed tasks.  Download history and task rows are preserved.
    """
    with _connect() as connection:
        return _retire_completed_channel(
            connection, chat_id, require_completed_scan=True
        )


def retire_completed_channels(limit: int = 5000) -> int:
    """Archive already completed channels before any Telegram access."""
    changed = 0
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT chat_id, enabled FROM download_channel_state
            WHERE scan_status = 'completed' AND enabled = 1
            ORDER BY updated_at ASC LIMIT ?
            """,
            (min(max(int(limit), 1), 50000),),
        ).fetchall()
        for row in rows:
            changed += int(
                _retire_completed_channel(
                    connection, row["chat_id"], require_completed_scan=True
                )
            )
    return changed


def update_channel_cursor(chat_id, last_read_message_id: int) -> None:
    """Persist a channel cursor independently from config.yaml."""
    now = _now()
    with _connect() as connection:
        _upsert_channel(connection, chat_id)
        connection.execute(
            """
            UPDATE download_channel_state
            SET last_read_message_id = MAX(last_read_message_id, ?),
                last_scanned_at = ?, updated_at = ?
            WHERE chat_id = ?
            """,
            (_non_negative_int(last_read_message_id), now, now, str(chat_id)),
        )


def update_channel_scan_state(
    chat_id, status: str, error: str = "", retry_after: int = 0
) -> None:
    """Expose downloader scan health to the channel library."""
    now = _now()
    success = status == "completed"
    failed = status == "failed"
    backoff = status == "backoff"
    next_action_at = (
        (datetime.now(timezone.utc) + timedelta(seconds=max(int(retry_after), 1)))
        .isoformat(timespec="seconds")
        if backoff
        else None
    )
    with _connect() as connection:
        _upsert_channel(connection, chat_id)
        connection.execute(
            """
            UPDATE download_channel_state
            SET scan_status = ?, last_scanned_at = ?,
                last_success_at = CASE WHEN ? THEN ? ELSE last_success_at END,
                consecutive_failures = CASE
                    WHEN ? THEN 0
                    WHEN ? THEN consecutive_failures + 1
                    ELSE consecutive_failures END,
                validation_error = CASE
                    WHEN ? THEN ''
                    WHEN ? OR ? THEN ?
                    ELSE validation_error END,
                next_action_at = ?,
                updated_at = ?
            WHERE chat_id = ?
            """,
            (
                str(status), now, success, now, success, failed, success, failed,
                backoff, str(error or "")[:2000], next_action_at, now, str(chat_id),
            ),
        )
        _refresh_channel_state(connection, chat_id)
        if success:
            _retire_completed_channel(
                connection, chat_id, require_completed_scan=True
            )


def _priority(value) -> str:
    result = str(value or "normal").strip().lower()
    return result if result in CHANNEL_PRIORITIES else "normal"


def _non_negative_int(value, default: int = 0) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return max(int(default), 0)


def normalize_channel_reference(value) -> tuple:
    """Normalize IDs, usernames, and common Telegram links."""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("频道地址不能为空")
    linked_message_id = 0
    candidate = raw
    if raw.lower().startswith("tg://"):
        parsed = urlparse(raw)
        domain = (parse_qs(parsed.query).get("domain") or [""])[0]
        candidate = f"@{domain}" if domain else ""
    elif re.match(r"^https?://", raw, re.IGNORECASE):
        parsed = urlparse(raw)
        if parsed.netloc.lower() not in {"t.me", "www.t.me", "telegram.me"}:
            raise ValueError("仅支持 Telegram 频道链接")
        parts = [part for part in parsed.path.split("/") if part]
        if parts and parts[0] == "c" and len(parts) >= 2 and parts[1].isdigit():
            candidate = f"-100{parts[1]}"
            if len(parts) >= 3:
                linked_message_id = _non_negative_int(parts[2])
        elif parts:
            username_index = 1 if parts[0] == "s" and len(parts) > 1 else 0
            username = parts[username_index]
            candidate = f"@{username.lstrip('@')}"
            if len(parts) > username_index + 1:
                linked_message_id = _non_negative_int(parts[username_index + 1])
        else:
            candidate = ""

    candidate = candidate.strip()
    if candidate.lstrip("-").isdigit():
        return str(int(candidate)), linked_message_id
    if not candidate.startswith("@"):
        candidate = f"@{candidate}"
    if not re.fullmatch(r"@[A-Za-z0-9_]{5,64}", candidate):
        raise ValueError("频道 ID、用户名或链接格式不正确")
    return candidate, linked_message_id


def _parse_import_rows(content: str, defaults: dict) -> list:
    """Parse newline input or a CSV document into normalized import rows."""
    text = str(content or "").lstrip("\ufeff")
    non_empty = [(index, line) for index, line in enumerate(text.splitlines(), 1) if line.strip()]
    if not non_empty:
        raise ValueError("请粘贴频道列表或上传 CSV")

    first_line = non_empty[0][1].lower()
    is_csv_header = "chat_id" in {part.strip() for part in first_line.split(",")}
    rows = []
    if is_csv_header:
        reader = csv.DictReader(io.StringIO(text))
        for line_number, item in enumerate(reader, 2):
            if not any(str(value or "").strip() for value in item.values()):
                continue
            rows.append((line_number, item, ",".join(str(value or "") for value in item.values())))
    else:
        for line_number, line in non_empty:
            values = next(csv.reader([line]))
            item = {
                "chat_id": values[0] if values else "",
                "start_message_id": values[1] if len(values) > 1 else "",
                "group_name": values[2] if len(values) > 2 else "",
                "priority": values[3] if len(values) > 3 else "",
            }
            rows.append((line_number, item, line))

    result = []
    for line_number, item, raw_value in rows:
        source = {**(defaults or {}), **{key: value for key, value in item.items() if str(value or "").strip()}}
        result.append(
            {
                "line_number": line_number,
                "raw_value": raw_value.strip(),
                "chat_id": source.get("chat_id", ""),
                "start_message_id": source.get("start_message_id", source.get("last_read_message_id", 0)),
                "group_name": str(source.get("group_name") or "").strip(),
                "priority": _priority(source.get("priority")),
                "download_filter": str(source.get("download_filter") or "").strip(),
            }
        )
    return result


def _refresh_import_batch(connection: sqlite3.Connection, batch_id: int) -> dict:
    """Recalculate counters and lifecycle state for one import batch."""
    counts = {
        row["status"]: int(row["count"])
        for row in connection.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM channel_import_items WHERE batch_id = ? GROUP BY status
            """,
            (int(batch_id),),
        ).fetchall()
    }
    pending_count = counts.get("pending", 0) + counts.get("validating", 0)
    valid_count = counts.get("valid", 0)
    imported_count = counts.get("imported", 0)
    current = connection.execute(
        "SELECT status FROM channel_import_batches WHERE id = ?", (int(batch_id),)
    ).fetchone()
    current_status = current["status"] if current else "validating"
    if current_status in {"imported", "undone"}:
        status = current_status
    elif pending_count:
        status = "validating"
    else:
        status = "ready"
    connection.execute(
        """
        UPDATE channel_import_batches
        SET status = ?, pending_count = ?, valid_count = ?,
            duplicate_count = ?, invalid_count = ?, imported_count = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            status,
            pending_count,
            valid_count,
            counts.get("duplicate", 0),
            counts.get("invalid", 0),
            imported_count,
            _now(),
            int(batch_id),
        ),
    )
    row = connection.execute(
        "SELECT * FROM channel_import_batches WHERE id = ?", (int(batch_id),)
    ).fetchone()
    return dict(row) if row else {}


def create_import_batch(content: str, source_name: str = "", defaults: Optional[dict] = None) -> dict:
    """Create a traceable batch with structural validation and deduplication."""
    rows = _parse_import_rows(content, defaults or {})
    now = _now()
    with _connect() as connection:
        existing = {
            row["chat_id"]
            for row in connection.execute("SELECT chat_id FROM download_channel_state").fetchall()
        }
        cursor = connection.execute(
            """
            INSERT INTO channel_import_batches (
                source_name, status, total_count, created_at, updated_at
            ) VALUES (?, 'validating', ?, ?, ?)
            """,
            (str(source_name or "手动粘贴")[:255], len(rows), now, now),
        )
        batch_id = int(cursor.lastrowid)
        seen = set()
        for row in rows:
            status = "pending"
            error = ""
            try:
                chat_id, linked_message_id = normalize_channel_reference(row["chat_id"])
                start_message_id = _non_negative_int(
                    row.get("start_message_id"), linked_message_id
                )
                if chat_id in seen:
                    status, error = "duplicate", "本批次内重复"
                elif chat_id in existing:
                    status, error = "duplicate", "频道库中已存在"
                seen.add(chat_id)
            except ValueError as exc:
                chat_id = str(row.get("chat_id") or "").strip()
                start_message_id = 0
                status, error = "invalid", str(exc)
            connection.execute(
                """
                INSERT INTO channel_import_items (
                    batch_id, line_number, raw_value, chat_id,
                    start_message_id, group_name, priority, download_filter,
                    status, error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    row["line_number"],
                    row["raw_value"],
                    chat_id,
                    start_message_id,
                    row["group_name"],
                    row["priority"],
                    row["download_filter"],
                    status,
                    error,
                    now,
                    now,
                ),
            )
        return _refresh_import_batch(connection, batch_id)


def get_import_batch(batch_id: int) -> Optional[dict]:
    """Return one import batch with current counters."""
    with _connect() as connection:
        row = connection.execute(
            "SELECT * FROM channel_import_batches WHERE id = ?", (int(batch_id),)
        ).fetchone()
    return dict(row) if row else None


def list_import_batches(limit: int = 20) -> list:
    """Return recent import batches for the import center."""
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT * FROM channel_import_batches
            ORDER BY id DESC LIMIT ?
            """,
            (min(max(int(limit), 1), 100),),
        ).fetchall()
    return [dict(row) for row in rows]


def list_import_items(batch_id: int, limit: int = 100, offset: int = 0) -> dict:
    """Return server-paginated rows for one import batch."""
    page_limit = min(max(int(limit), 1), 500)
    page_offset = max(int(offset), 0)
    with _connect() as connection:
        total = connection.execute(
            "SELECT COUNT(*) FROM channel_import_items WHERE batch_id = ?",
            (int(batch_id),),
        ).fetchone()[0]
        rows = connection.execute(
            """
            SELECT * FROM channel_import_items
            WHERE batch_id = ? ORDER BY line_number LIMIT ? OFFSET ?
            """,
            (int(batch_id), page_limit, page_offset),
        ).fetchall()
    return {"total": int(total), "records": [dict(row) for row in rows]}


def claim_import_items(limit: int = 3) -> list:
    """Atomically claim pending rows for Telegram access validation."""
    now = _now()
    stale = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(timespec="seconds")
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            """
            SELECT * FROM channel_import_items
            WHERE (status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= ?))
               OR (status = 'validating' AND updated_at < ?)
            ORDER BY id LIMIT ?
            """,
            (now, stale, min(max(int(limit), 1), 20)),
        ).fetchall()
        ids = [int(row["id"]) for row in rows]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            connection.execute(
                f"""
                UPDATE channel_import_items
                SET status = 'validating', validation_attempts = validation_attempts + 1,
                    updated_at = ? WHERE id IN ({placeholders})
                """,
                [now, *ids],
            )
    return [dict(row) for row in rows]


def mark_import_item_validation(
    item_id: int,
    valid: bool,
    chat_title: str = "",
    error: str = "",
    retry_after: int = 0,
) -> Optional[dict]:
    """Finish or defer Telegram validation for one import item."""
    now = datetime.now(timezone.utc)
    if retry_after > 0:
        status = "pending"
        next_retry_at = (now + timedelta(seconds=retry_after)).isoformat(timespec="seconds")
    else:
        status = "valid" if valid else "invalid"
        next_retry_at = None
    with _connect() as connection:
        row = connection.execute(
            "SELECT batch_id FROM channel_import_items WHERE id = ?", (int(item_id),)
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            """
            UPDATE channel_import_items
            SET status = ?, chat_title = ?, error = ?, next_retry_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                str(chat_title or "")[:255],
                str(error or "")[:2000],
                next_retry_at,
                now.isoformat(timespec="seconds"),
                int(item_id),
            ),
        )
        return _refresh_import_batch(connection, int(row["batch_id"]))


def confirm_import_batch(batch_id: int) -> dict:
    """Insert validated rows into the channel library as one idempotent action."""
    now = _now()
    imported = 0
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        batch = connection.execute(
            "SELECT * FROM channel_import_batches WHERE id = ?", (int(batch_id),)
        ).fetchone()
        if batch is None:
            raise ValueError("导入批次不存在")
        if batch["pending_count"]:
            raise ValueError("频道仍在校验中，请稍后再确认")
        rows = connection.execute(
            "SELECT * FROM channel_import_items WHERE batch_id = ? AND status = 'valid'",
            (int(batch_id),),
        ).fetchall()
        for row in rows:
            existing = connection.execute(
                "SELECT 1 FROM download_channel_state WHERE chat_id = ?", (row["chat_id"],)
            ).fetchone()
            if existing:
                connection.execute(
                    "UPDATE channel_import_items SET status = 'duplicate', error = '确认时频道已存在', updated_at = ? WHERE id = ?",
                    (now, row["id"]),
                )
                continue
            connection.execute(
                """
                INSERT INTO download_channel_state (
                    chat_id, chat_title, paused, enabled, priority, group_name,
                    last_read_message_id, download_filter, validation_status,
                    validation_error, last_validated_at, source, import_batch_id,
                    config_revision, created_at, updated_at
                ) VALUES (?, ?, 0, 1, ?, ?, ?, ?, 'valid', '', ?, 'import', ?, 1, ?, ?)
                """,
                (
                    row["chat_id"], row["chat_title"], row["priority"], row["group_name"],
                    row["start_message_id"], row["download_filter"], now,
                    int(batch_id), now, now,
                ),
            )
            connection.execute(
                "UPDATE channel_import_items SET status = 'imported', error = '', updated_at = ? WHERE id = ?",
                (now, row["id"]),
            )
            _refresh_channel_state(connection, row["chat_id"])
            imported += 1
        connection.execute(
            """
            UPDATE channel_import_batches
            SET status = 'imported', completed_at = ?, updated_at = ? WHERE id = ?
            """,
            (now, now, int(batch_id)),
        )
        result = _refresh_import_batch(connection, int(batch_id))
    result["changed"] = imported
    return result


def add_channel_to_library(
    chat_id,
    chat_title: str = "",
    start_message_id: int = 0,
    source: str = "batch",
) -> bool:
    """Add (or re-enable) one channel in the library so downloading starts.

    Idempotent: an existing channel is left in place (returns False), a new
    one is inserted enabled (returns True). Used by the batch-download
    workflow after an invite link resolves to a numeric chat_id.
    """
    chat_id = str(chat_id)
    now = _now()
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        existing = connection.execute(
            "SELECT 1 FROM download_channel_state WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        if existing:
            return False
        connection.execute(
            """
            INSERT INTO download_channel_state (
                chat_id, chat_title, paused, enabled, priority, group_name,
                last_read_message_id, download_filter, validation_status,
                validation_error, last_validated_at, source, import_batch_id,
                config_revision, created_at, updated_at
            ) VALUES (?, ?, 0, 1, 'normal', '', ?, '', 'valid', '', ?, ?, NULL, 1, ?, ?)
            """,
            (
                chat_id, str(chat_title or ""), _non_negative_int(start_message_id),
                now, str(source or "batch"), now, now,
            ),
        )
        _refresh_channel_state(connection, chat_id)
    return True


def undo_import_batch(batch_id: int) -> dict:
    """Undo a batch; channels with task/history evidence are retained but disabled."""
    now = _now()
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        batch = connection.execute(
            "SELECT * FROM channel_import_batches WHERE id = ?", (int(batch_id),)
        ).fetchone()
        if batch is None:
            raise ValueError("导入批次不存在")
        batch_chat_ids = [
            row["chat_id"]
            for row in connection.execute(
                "SELECT chat_id FROM download_channel_state WHERE import_batch_id = ?",
                (int(batch_id),),
            ).fetchall()
        ]
        removable = connection.execute(
            """
            DELETE FROM download_channel_state
            WHERE import_batch_id = ?
              AND NOT EXISTS (SELECT 1 FROM download_tasks task
                              WHERE task.chat_id = download_channel_state.chat_id)
              AND NOT EXISTS (SELECT 1 FROM download_history history
                              WHERE history.chat_id = download_channel_state.chat_id)
            """,
            (int(batch_id),),
        ).rowcount
        disabled = connection.execute(
            """
            UPDATE download_channel_state
            SET enabled = 0, updated_at = ? WHERE import_batch_id = ?
            """,
            (now, int(batch_id)),
        ).rowcount
        for chat_id in batch_chat_ids:
            if connection.execute(
                "SELECT 1 FROM download_channel_state WHERE chat_id = ?", (chat_id,)
            ).fetchone():
                _refresh_channel_state(connection, chat_id)
            else:
                connection.execute(
                    "DELETE FROM channel_state_events WHERE chat_id = ?", (chat_id,)
                )
        connection.execute(
            """
            UPDATE channel_import_batches
            SET status = 'undone', updated_at = ?, completed_at = ? WHERE id = ?
            """,
            (now, now, int(batch_id)),
        )
        result = _refresh_import_batch(connection, int(batch_id))
    result.update({"removed": max(removable, 0), "disabled": max(disabled, 0)})
    return result


def queue_task(
    chat_id,
    message_id: int,
    chat_title: str = "",
    file_name: str = "",
    media_type: str = "",
    total_size: int = 0,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    force: bool = False,
    refresh_state: bool = True,
) -> bool:
    """Claim a task for the in-memory queue, preventing duplicate work."""
    chat_key = str(chat_id)
    task_id = int(message_id)
    now = _now()
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        _upsert_channel(connection, chat_key, chat_title)
        current = connection.execute(
            """
            SELECT status, attempts, max_attempts
            FROM download_tasks
            WHERE chat_id = ? AND message_id = ?
            """,
            (chat_key, task_id),
        ).fetchone()
        if current is None:
            connection.execute(
                """
                INSERT INTO download_tasks (
                    chat_id, message_id, chat_title, file_name, media_type,
                    total_size, status, attempts, max_attempts, queued_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?)
                """,
                (
                    chat_key,
                    task_id,
                    str(chat_title or ""),
                    str(file_name or ""),
                    str(media_type or ""),
                    int(total_size or 0),
                    max(int(max_attempts), 1),
                    now,
                    now,
                ),
            )
            if refresh_state:
                _refresh_channel_state(connection, chat_key)
            return True

        status = current["status"]
        attempts = int(current["attempts"])
        allowed = status in {"retry_requested", "retrying"}
        allowed = allowed or (
            status == "failed" and attempts < int(current["max_attempts"])
        )
        if not force and not allowed:
            return False

        connection.execute(
            """
            UPDATE download_tasks
            SET status = 'queued',
                chat_title = CASE WHEN ? = '' THEN chat_title ELSE ? END,
                file_name = CASE WHEN ? = '' THEN file_name ELSE ? END,
                media_type = CASE WHEN ? = '' THEN media_type ELSE ? END,
                total_size = CASE WHEN ? = 0 THEN total_size ELSE ? END,
                error = CASE WHEN ? THEN '' ELSE error END,
                queued_at = ?, updated_at = ?, next_retry_at = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            (
                str(chat_title or ""),
                str(chat_title or ""),
                str(file_name or ""),
                str(file_name or ""),
                str(media_type or ""),
                str(media_type or ""),
                int(total_size or 0),
                int(total_size or 0),
                bool(force),
                now,
                now,
                chat_key,
                task_id,
            ),
        )
        if refresh_state:
            _refresh_channel_state(connection, chat_key)
        return True


def pause_task(chat_id, message_id: int) -> int:
    """Mark a task as paused (channel paused). No retry attempt is consumed.

    Resumed later by ``set_channels_paused(..., paused=False)`` which flips
    'paused' back to 'retry_requested' for re-download.
    """
    now = _now()
    with _connect() as connection:
        cursor = connection.execute(
            """
            UPDATE download_tasks
            SET status = 'paused', updated_at = ?, next_retry_at = NULL
            WHERE chat_id = ? AND message_id = ?
              AND status IN ('downloading', 'queued', 'retrying', 'retry_requested')
            """,
            (now, str(chat_id), int(message_id)),
        )
        _refresh_channel_state(connection, chat_id)
    return max(cursor.rowcount, 0)


def start_task(chat_id, message_id: int) -> int:
    """Claim a queued task for download and increment its attempt count."""
    now = _now()
    with _connect() as connection:
        channel_was_downloading = connection.execute(
            """
            SELECT 1 FROM download_tasks
            WHERE chat_id = ? AND status = 'downloading'
            LIMIT 1
            """,
            (str(chat_id),),
        ).fetchone()
        cursor = connection.execute(
            """
            UPDATE download_tasks
            SET status = 'downloading', attempts = attempts + 1,
                started_at = ?, updated_at = ?, next_retry_at = NULL
            WHERE chat_id = ? AND message_id = ?
              AND status IN ('queued', 'retrying', 'retry_requested')
              AND NOT EXISTS (
                  SELECT 1
                  FROM download_channel_state AS channel
                  WHERE channel.chat_id = download_tasks.chat_id
                    AND (channel.paused = 1 OR channel.enabled = 0)
              )
            """,
            (now, now, str(chat_id), int(message_id)),
        )
        if cursor.rowcount <= 0:
            return 0
        row = connection.execute(
            """
            SELECT attempts FROM download_tasks
            WHERE chat_id = ? AND message_id = ?
            """,
            (str(chat_id), int(message_id)),
        ).fetchone()
        if not channel_was_downloading:
            _refresh_channel_state(connection, chat_id)
    return int(row["attempts"]) if row else 0


def task_claim_state(chat_id, message_id: int) -> Optional[dict]:
    """Explain why a worker could not claim a persisted task."""
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT task.status, channel.enabled, channel.paused
            FROM download_tasks AS task
            LEFT JOIN download_channel_state AS channel
              ON channel.chat_id = task.chat_id
            WHERE task.chat_id = ? AND task.message_id = ?
            """,
            (str(chat_id), int(message_id)),
        ).fetchone()
    return dict(row) if row else None


def defer_task(chat_id, message_id: int, reason: str = "") -> int:
    """Persist an unstarted item for later hydration without consuming retry."""
    now = _now()
    with _connect() as connection:
        cursor = connection.execute(
            """
            UPDATE download_tasks
            SET status = 'retry_requested', error = ?, updated_at = ?,
                next_retry_at = NULL
            WHERE chat_id = ? AND message_id = ?
              AND status IN ('queued', 'retrying', 'retry_requested', 'paused')
            """,
            (str(reason or "")[:2000], now, str(chat_id), int(message_id)),
        )
        if cursor.rowcount > 0:
            _refresh_channel_state(connection, chat_id)
    return max(cursor.rowcount, 0)


def release_orphaned_claims(active_keys, min_age_seconds: int = 900) -> int:
    """Return tasks stuck in 'downloading' with no live worker to the retry queue.

    `start_task` marks a row 'downloading' before the transfer begins. If the
    worker coroutine then blocks forever — an unbounded Telegram await, a
    wedged socket — the row stays claimed until the process restarts, because
    `recover_interrupted_tasks()` only runs at startup. One such leak on
    2026-08-14 held a task for 15.5 hours and, once every other channel had
    drained, made the whole downloader look idle.

    ``active_keys`` is the set of (chat_id, message_id) this process currently
    owns, so a legitimately slow multi-GB transfer is never touched no matter
    how long it runs. ``min_age_seconds`` only guards the narrow window between
    claiming a row and registering ownership.
    """
    owned = {(str(a), int(b)) for a, b in (active_keys or ())}
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=max(int(min_age_seconds), 60))
    ).isoformat(timespec="seconds")
    released = 0
    with _connect() as connection:
        rows = connection.execute(
            """
            SELECT chat_id, message_id FROM download_tasks
            WHERE status = 'downloading' AND updated_at <= ?
            """,
            (cutoff,),
        ).fetchall()
        now = _now()
        touched = set()
        for row in rows:
            key = (str(row["chat_id"]), int(row["message_id"]))
            if key in owned:
                continue
            cursor = connection.execute(
                """
                UPDATE download_tasks
                SET status = 'retry_requested', updated_at = ?,
                    next_retry_at = NULL,
                    error = 'worker 未释放该任务，已自动回收重试'
                WHERE chat_id = ? AND message_id = ? AND status = 'downloading'
                """,
                (now, key[0], key[1]),
            )
            if cursor.rowcount > 0:
                released += cursor.rowcount
                touched.add(key[0])
        for chat_id in touched:
            _refresh_channel_state(connection, chat_id)
    return released


def finish_task(
    chat_id,
    message_id: int,
    status: str,
    save_path: str = "",
    total_size: int = 0,
    file_name: str = "",
    media_type: str = "",
) -> None:
    """Persist a successful or skipped terminal task state."""
    if status not in {"completed", "skipped"}:
        raise ValueError("finish_task only accepts completed or skipped")
    with _connect() as connection:
        _finish_task_on_connection(
            connection,
            chat_id,
            message_id,
            status,
            save_path=save_path,
            total_size=total_size,
            file_name=file_name,
            media_type=media_type,
        )
        if not _channel_has_downloading(connection, chat_id):
            _refresh_channel_state(connection, chat_id)
            _retire_completed_channel(
                connection, chat_id, require_completed_scan=True
            )


def _finish_task_on_connection(
    connection: sqlite3.Connection,
    chat_id,
    message_id: int,
    status: str,
    save_path: str = "",
    total_size: int = 0,
    file_name: str = "",
    media_type: str = "",
) -> None:
    """Update one terminal task using the caller's transaction."""
    now = _now()
    resolved_name = file_name or (Path(save_path).name if save_path else "")
    connection.execute(
        """
        UPDATE download_tasks
        SET status = ?, save_path = ?,
            file_name = CASE WHEN ? = '' THEN file_name ELSE ? END,
            media_type = CASE WHEN ? = '' THEN media_type ELSE ? END,
            total_size = CASE WHEN ? = 0 THEN total_size ELSE ? END,
            error = '', updated_at = ?, completed_at = ?,
            next_retry_at = NULL
        WHERE chat_id = ? AND message_id = ?
        """,
        (
            status,
            str(save_path or ""),
            resolved_name,
            resolved_name,
            str(media_type or ""),
            str(media_type or ""),
            int(total_size or 0),
            int(total_size or 0),
            now,
            now,
            str(chat_id),
            int(message_id),
        ),
    )


def _channel_has_downloading(
    connection: sqlite3.Connection, chat_id
) -> bool:
    """Return whether another file still keeps this channel active."""
    return (
        connection.execute(
            """
            SELECT 1 FROM download_tasks
            WHERE chat_id = ? AND status = 'downloading'
            LIMIT 1
            """,
            (str(chat_id),),
        ).fetchone()
        is not None
    )


def complete_task_with_history(
    chat_id,
    message_id: int,
    save_path: str,
    total_size: int,
    chat_title: str = "",
    media_type: str = "",
) -> None:
    """Atomically record the file and mark its task complete.

    Both tables intentionally live in the task database.  A process crash can
    therefore no longer leave a history row without a completed task, or a
    completed task without history.
    """
    completed = _now()
    file_path = Path(save_path)
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT INTO download_history (
                chat_id, message_id, chat_title, file_name, total_size,
                save_path, media_type, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO UPDATE SET
                chat_title = excluded.chat_title,
                file_name = excluded.file_name,
                total_size = excluded.total_size,
                save_path = excluded.save_path,
                media_type = excluded.media_type,
                completed_at = excluded.completed_at
            """,
            (
                str(chat_id),
                int(message_id),
                str(chat_title or ""),
                file_path.name,
                int(total_size),
                str(file_path),
                str(media_type or ""),
                completed,
            ),
        )
        _finish_task_on_connection(
            connection,
            chat_id,
            message_id,
            "completed",
            save_path=str(file_path),
            total_size=total_size,
            file_name=file_path.name,
            media_type=media_type,
        )
        if not _channel_has_downloading(connection, chat_id):
            _refresh_channel_state(connection, chat_id)
            _retire_completed_channel(
                connection, chat_id, require_completed_scan=True
            )


def fail_task(
    chat_id, message_id: int, error: str, made_progress: bool = False
) -> dict:
    """Persist a failure and return whether another automatic retry is due.

    ``made_progress`` means the attempt banked more verified chunks into the
    resumable partial file.  Such a task is converging and must come back in
    ``PROGRESS_RETRY_DELAY`` seconds; sending it to the back of the transport
    backoff instead is what left multi-GB files sitting at 60-98 % complete
    across dozens of hourly attempts.
    """
    now = datetime.now(timezone.utc)
    error_text = str(error or "下载失败，请查看日志")[:2000]
    normalized_error = error_text.lower()
    transient_transfer_error = (
        any(marker in normalized_error for marker in TRANSIENT_TRANSFER_ERROR_MARKERS)
        or (
            normalized_error.startswith("downloaded ")
            and " bytes, expected " in normalized_error
        )
    )
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT attempts, max_attempts
            FROM download_tasks
            WHERE chat_id = ? AND message_id = ?
            """,
            (str(chat_id), int(message_id)),
        ).fetchone()
        if row is None:
            return {"retry": False, "delay": 0, "attempts": 0}

        attempts = int(row["attempts"])
        max_attempts = int(row["max_attempts"])
        # A broken Telegram/media connection is not evidence that the file is
        # bad, so keep it retryable.  Attempts still advance, however, so a
        # prolonged outage backs off from one minute to one hour instead of
        # replaying the whole failed batch every minute forever.
        if transient_transfer_error:
            attempts_after_failure = attempts
            # Finite, but far more patient than a bad-file failure: a transport
            # outage must not permanently retire a recoverable partial file.
            # Unbounded retry, however, let single files reach 118 attempts and
            # cycle forever with no operator signal.
            should_retry = attempts < TRANSIENT_MAX_ATTEMPTS
            if made_progress:
                # Converging: come straight back instead of waiting an hour.
                delay = PROGRESS_RETRY_DELAY
            else:
                delay = TRANSIENT_RETRY_DELAYS[
                    min(max(attempts - 1, 0), len(TRANSIENT_RETRY_DELAYS) - 1)
                ]
        else:
            attempts_after_failure = attempts
            should_retry = attempts < max_attempts
            delay = RETRY_DELAYS[
                min(max(attempts - 1, 0), len(RETRY_DELAYS) - 1)
            ]
        status = "retrying" if should_retry else "failed"
        next_retry = (
            (now + timedelta(seconds=delay)).isoformat(timespec="seconds")
            if should_retry
            else None
        )
        connection.execute(
            """
            UPDATE download_tasks
            SET status = ?, attempts = ?, error = ?, updated_at = ?,
                completed_at = CASE WHEN ? = 'failed' THEN ? ELSE NULL END,
                next_retry_at = ?
            WHERE chat_id = ? AND message_id = ?
            """,
            (
                status,
                attempts_after_failure,
                error_text,
                now.isoformat(timespec="seconds"),
                status,
                now.isoformat(timespec="seconds"),
                next_retry,
                str(chat_id),
                int(message_id),
            ),
        )
        _refresh_channel_state(connection, chat_id)
    return {
        "retry": should_retry,
        "delay": delay,
        "attempts": attempts_after_failure,
        "transient": transient_transfer_error,
    }


def recover_interrupted_tasks() -> int:
    """Recover queued work while preserving retry backoff and partial priority."""
    now = _now()
    with _connect() as connection:
        chat_ids = [
            row["chat_id"]
            for row in connection.execute(
                "SELECT DISTINCT chat_id FROM download_tasks WHERE status IN ('queued', 'downloading')"
            ).fetchall()
        ]
        # Previously active transfers come first after a restart so partial
        # files are reused promptly. Tasks already in retrying keep their
        # next_retry_at timestamp; restarting must not bypass backoff.
        cursor = connection.execute(
            """
            UPDATE download_tasks
            SET status = 'retry_requested',
                attempts = CASE
                    WHEN status = 'downloading'
                    THEN MAX(MIN(attempts, max_attempts) - 1, 0)
                    ELSE attempts
                END,
                updated_at = CASE
                    WHEN status = 'downloading'
                    THEN '1970-01-01T00:00:00+00:00'
                    ELSE ?
                END,
                next_retry_at = NULL,
                error = CASE
                    WHEN status = 'downloading' THEN '服务重启后优先恢复未完成传输'
                    WHEN error = '' THEN '服务重启后自动恢复'
                    ELSE error
                END
            WHERE status IN ('queued', 'downloading')
            """,
            (now,),
        )
        for chat_id in chat_ids:
            _refresh_channel_state(connection, chat_id)
    return max(cursor.rowcount, 0)


def list_retry_requests(
    limit: int = 100,
    participating_chat_ids: Optional[Iterable] = None,
    max_new_channels: int = 0,
    participating_limits: Optional[dict] = None,
    new_channel_limit: int = 0,
) -> list:
    """Return due work without over-admitting inactive channels.

    When a serial/fixed-channel scheduler is already busy, persisted work from
    other channels must stay in SQLite. Otherwise those inactive messages fill
    the hot queue but cannot be dispatched, preventing a completed active slot
    from receiving its replacement.
    """
    task_limit = min(max(int(limit), 1), 500)
    now = _now()
    due_clause = """
        (task.status = 'retry_requested'
         OR (task.status = 'retrying'
             AND (task.next_retry_at IS NULL OR task.next_retry_at <= ?)))
    """
    with _connect() as connection:
        if participating_chat_ids is None:
            rows = connection.execute(
                f"""
                SELECT task.chat_id, task.message_id
                FROM download_tasks AS task
                JOIN download_channel_state AS channel
                  ON channel.chat_id = task.chat_id
                WHERE {due_clause}
                  AND channel.enabled = 1 AND channel.paused = 0
                ORDER BY task.updated_at ASC
                LIMIT ?
                """,
                (now, task_limit),
            ).fetchall()
            return [dict(row) for row in rows]

        # Query participating channels directly in SQLite.  The old code
        # fetched the globally oldest 500 rows first and filtered afterwards;
        # 500 older tasks from inactive channels could therefore hide every
        # refill for a currently downloading channel and drain live workers to
        # zero even though eligible work existed.
        participating = sorted(
            {str(chat_id) for chat_id in participating_chat_ids}
        )
        selected = []
        if participating:
            limits = {
                str(chat_id): max(int(value or 0), 0)
                for chat_id, value in dict(participating_limits or {}).items()
            }
            if participating_limits is None:
                limits = {chat_id: task_limit for chat_id in participating}
            for chat_id in participating:
                remaining = task_limit - len(selected)
                channel_limit = min(limits.get(chat_id, 0), remaining)
                if channel_limit <= 0:
                    continue
                rows = connection.execute(
                    f"""
                    SELECT task.chat_id, task.message_id
                    FROM download_tasks AS task
                    JOIN download_channel_state AS channel
                      ON channel.chat_id = task.chat_id
                    WHERE {due_clause} AND task.chat_id = ?
                      AND channel.enabled = 1 AND channel.paused = 0
                    ORDER BY task.updated_at ASC
                    LIMIT ?
                    """,
                    (now, chat_id, channel_limit),
                ).fetchall()
                selected.extend(dict(row) for row in rows)

        remaining = task_limit - len(selected)
        new_limit = max(int(max_new_channels or 0), 0)
        if remaining <= 0 or new_limit <= 0:
            return selected

        exclusion = ""
        channel_parameters = [now]
        if participating:
            placeholders = ",".join("?" for _ in participating)
            exclusion = f"AND task.chat_id NOT IN ({placeholders})"
            channel_parameters.extend(participating)
        channel_parameters.append(new_limit)
        channel_rows = connection.execute(
            f"""
            SELECT task.chat_id, MIN(task.updated_at) AS oldest
            FROM download_tasks AS task
            JOIN download_channel_state AS channel
              ON channel.chat_id = task.chat_id
            WHERE {due_clause}
              AND channel.enabled = 1 AND channel.paused = 0
              {exclusion}
            GROUP BY task.chat_id
            ORDER BY oldest ASC, task.chat_id ASC
            LIMIT ?
            """,
            channel_parameters,
        ).fetchall()
        new_chat_ids = [str(row["chat_id"]) for row in channel_rows]
        if not new_chat_ids:
            return selected

        per_new_channel = max(int(new_channel_limit or 0), 0)
        if per_new_channel <= 0:
            per_new_channel = remaining
        for chat_id in new_chat_ids:
            remaining = task_limit - len(selected)
            channel_limit = min(per_new_channel, remaining)
            if channel_limit <= 0:
                break
            rows = connection.execute(
                f"""
                SELECT task.chat_id, task.message_id
                FROM download_tasks AS task
                JOIN download_channel_state AS channel
                  ON channel.chat_id = task.chat_id
                WHERE {due_clause} AND task.chat_id = ?
                  AND channel.enabled = 1 AND channel.paused = 0
                ORDER BY task.updated_at ASC
                LIMIT ?
                """,
                (now, chat_id, channel_limit),
            ).fetchall()
            selected.extend(dict(row) for row in rows)
        return selected


def claim_transport_probe(exclude_chat_ids=None) -> Optional[dict]:
    """Release one transient retry early to test whether transport recovered.

    The task stays persisted and keeps its attempt/error history.  Only its
    next-run gate is opened, so a failed probe returns to normal backoff and a
    successful probe can wake more work without losing any file.  When the
    configured active-channel set is full but has idle global slots, callers
    can exclude those active channels so the probe opens one standby channel
    instead of merely buffering another file behind a full per-channel limit.
    """
    now = _now()
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        excluded = sorted({str(chat_id) for chat_id in (exclude_chat_ids or ())})
        exclusion = ""
        parameters = []
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            exclusion = f"AND task.chat_id NOT IN ({placeholders})"
            parameters.extend(excluded)
        row = connection.execute(
            f"""
            SELECT task.chat_id, task.message_id
            FROM download_tasks AS task
            JOIN download_channel_state AS channel
              ON channel.chat_id = task.chat_id
            WHERE task.status = 'retrying'
              AND channel.enabled = 1
              AND channel.paused = 0
              {exclusion}
            ORDER BY COALESCE(task.next_retry_at, task.updated_at) ASC,
                     task.updated_at ASC
            LIMIT 1
            """,
            parameters,
        ).fetchone()
        if row is None:
            return None
        cursor = connection.execute(
            """
            UPDATE download_tasks
            SET status = 'retry_requested', next_retry_at = NULL,
                updated_at = ?
            WHERE chat_id = ? AND message_id = ? AND status = 'retrying'
            """,
            (now, row["chat_id"], int(row["message_id"])),
        )
        if cursor.rowcount <= 0:
            return None
        _refresh_channel_state(connection, row["chat_id"])
        return dict(row)


def release_transport_retries(
    preferred_chat_id=None, max_channels: int = 3, limit: int = 100
) -> int:
    """Wake a bounded retry wave after a successful transport probe."""
    channel_limit = max(int(max_channels or 1), 1)
    task_limit = min(max(int(limit or 1), 1), 500)
    preferred = str(preferred_chat_id) if preferred_chat_id is not None else ""
    now = _now()
    with _connect() as connection:
        channel_rows = connection.execute(
            """
            SELECT task.chat_id, MIN(COALESCE(task.next_retry_at, task.updated_at)) AS due
            FROM download_tasks AS task
            JOIN download_channel_state AS channel
              ON channel.chat_id = task.chat_id
            WHERE task.status = 'retrying'
              AND channel.enabled = 1
              AND channel.paused = 0
            GROUP BY task.chat_id
            ORDER BY CASE WHEN task.chat_id = ? THEN 0 ELSE 1 END,
                     due ASC
            LIMIT ?
            """,
            (preferred, channel_limit),
        ).fetchall()
        chat_ids = [row["chat_id"] for row in channel_rows]
        if not chat_ids:
            return 0
        placeholders = ",".join("?" for _ in chat_ids)
        task_rows = connection.execute(
            f"""
            SELECT rowid, chat_id
            FROM download_tasks
            WHERE status = 'retrying' AND chat_id IN ({placeholders})
            ORDER BY COALESCE(next_retry_at, updated_at) ASC, updated_at ASC
            LIMIT ?
            """,
            (*chat_ids, task_limit),
        ).fetchall()
        if not task_rows:
            return 0
        rowids = [int(row["rowid"]) for row in task_rows]
        row_placeholders = ",".join("?" for _ in rowids)
        cursor = connection.execute(
            f"""
            UPDATE download_tasks
            SET status = 'retry_requested', next_retry_at = NULL,
                updated_at = ?
            WHERE rowid IN ({row_placeholders}) AND status = 'retrying'
            """,
            (now, *rowids),
        )
        for chat_id in {row["chat_id"] for row in task_rows}:
            _refresh_channel_state(connection, chat_id)
        return max(cursor.rowcount, 0)


def refill_retry_capacity(
    active_channel_capacity: dict,
    max_active_channels: int,
    per_channel_limit: int,
    limit: int,
) -> int:
    """Wake retrying files only to fill proven-live scheduler capacity.

    Attempts and partial files are preserved.  Active channels are filled
    first; only when fewer than ``max_active_channels`` are active may another
    channel be opened.  This prevents a successful transfer from leaving idle
    workers merely because the remaining persisted files have a future
    per-file retry timestamp.
    """
    global_limit = min(max(int(limit or 0), 0), 500)
    if global_limit <= 0:
        return 0
    channel_limit = max(int(max_active_channels or 1), 1)
    file_limit = max(int(per_channel_limit or 1), 1)
    active_capacity = {
        str(chat_id): min(max(int(capacity or 0), 0), file_limit)
        for chat_id, capacity in dict(active_channel_capacity or {}).items()
    }
    active_capacity = {
        chat_id: capacity
        for chat_id, capacity in active_capacity.items()
        if capacity > 0
    }
    active_ids = set(str(chat_id) for chat_id in dict(active_channel_capacity or {}))
    now = _now()
    released = 0

    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")

        def release_for_channel(chat_id: str, capacity: int) -> int:
            nonlocal released
            capacity = min(max(int(capacity), 0), global_limit - released)
            if capacity <= 0:
                return 0
            rows = connection.execute(
                """
                SELECT task.rowid
                FROM download_tasks AS task
                JOIN download_channel_state AS channel
                  ON channel.chat_id = task.chat_id
                WHERE task.chat_id = ? AND task.status = 'retrying'
                  AND channel.enabled = 1 AND channel.paused = 0
                ORDER BY COALESCE(task.next_retry_at, task.updated_at) ASC,
                         task.updated_at ASC
                LIMIT ?
                """,
                (chat_id, capacity),
            ).fetchall()
            if not rows:
                return 0
            rowids = [int(row["rowid"]) for row in rows]
            placeholders = ",".join("?" for _ in rowids)
            cursor = connection.execute(
                f"""
                UPDATE download_tasks
                SET status = 'retry_requested', next_retry_at = NULL,
                    updated_at = ?
                WHERE rowid IN ({placeholders}) AND status = 'retrying'
                """,
                (now, *rowids),
            )
            count = max(cursor.rowcount, 0)
            released += count
            if count:
                _refresh_channel_state(connection, chat_id)
            return count

        for chat_id, capacity in active_capacity.items():
            if released >= global_limit:
                break
            release_for_channel(chat_id, capacity)

        new_channel_slots = max(channel_limit - len(active_ids), 0)
        if released < global_limit and new_channel_slots > 0:
            parameters = []
            exclusion = ""
            if active_ids:
                placeholders = ",".join("?" for _ in active_ids)
                exclusion = f"AND task.chat_id NOT IN ({placeholders})"
                parameters.extend(sorted(active_ids))
            parameters.append(new_channel_slots)
            channel_rows = connection.execute(
                f"""
                SELECT task.chat_id,
                       MIN(COALESCE(task.next_retry_at, task.updated_at)) AS due
                FROM download_tasks AS task
                JOIN download_channel_state AS channel
                  ON channel.chat_id = task.chat_id
                WHERE task.status = 'retrying'
                  AND channel.enabled = 1 AND channel.paused = 0
                  {exclusion}
                GROUP BY task.chat_id
                ORDER BY due ASC, task.chat_id ASC
                LIMIT ?
                """,
                parameters,
            ).fetchall()
            for row in channel_rows:
                if released >= global_limit:
                    break
                release_for_channel(
                    str(row["chat_id"]),
                    min(file_limit, global_limit - released),
                )

    return released


def request_retry(chat_id=None, message_id: Optional[int] = None) -> int:
    """Request one failed task, or every failed task, for a fresh retry cycle."""
    now = _now()
    with _connect() as connection:
        if chat_id is None and message_id is None:
            chat_ids = [
                row["chat_id"]
                for row in connection.execute(
                    "SELECT DISTINCT chat_id FROM download_tasks WHERE status = 'failed'"
                ).fetchall()
            ]
            cursor = connection.execute(
                """
                UPDATE download_tasks
                SET status = 'retry_requested', attempts = 0, error = '',
                    updated_at = ?, completed_at = NULL, next_retry_at = NULL
                WHERE status = 'failed'
                """,
                (now,),
            )
        else:
            chat_ids = [str(chat_id)]
            cursor = connection.execute(
                """
                UPDATE download_tasks
                SET status = 'retry_requested', attempts = 0, error = '',
                    updated_at = ?, completed_at = NULL, next_retry_at = NULL
                WHERE status = 'failed' AND chat_id = ? AND message_id = ?
                """,
                (now, str(chat_id), int(message_id or 0)),
            )
        for channel_id in chat_ids:
            _refresh_channel_state(connection, channel_id)
    return max(cursor.rowcount, 0)


def request_cancel(chat_id, message_id: int) -> int:
    """Cancel a task that has not started downloading yet."""
    now = _now()
    with _connect() as connection:
        cursor = connection.execute(
            """
            UPDATE download_tasks
            SET status = 'cancelled', error = '', updated_at = ?,
                completed_at = ?, next_retry_at = NULL
            WHERE chat_id = ? AND message_id = ?
              AND status IN ('queued', 'retrying', 'retry_requested')
            """,
            (now, now, str(chat_id), int(message_id)),
        )
        _refresh_channel_state(connection, chat_id)
    return max(cursor.rowcount, 0)


def mark_retry_unavailable(chat_id, message_id: int, error: str) -> None:
    """Mark a retry request failed when its Telegram message cannot be fetched."""
    now = _now()
    with _connect() as connection:
        connection.execute(
            """
            UPDATE download_tasks
            SET status = 'failed', error = ?, updated_at = ?, completed_at = ?
            WHERE chat_id = ? AND message_id = ?
            """,
            (str(error)[:2000], now, now, str(chat_id), int(message_id)),
        )
        _refresh_channel_state(connection, chat_id)


def task_counts() -> dict:
    """Return task counts grouped by persisted status."""
    counts = {
        "queued": 0,
        "downloading": 0,
        "retrying": 0,
        "retry_requested": 0,
        "completed": 0,
        "skipped": 0,
        "failed": 0,
        "cancelled": 0,
        "paused": 0,
    }
    with _connect() as connection:
        rows = connection.execute(
            "SELECT status, COUNT(*) AS count FROM download_tasks GROUP BY status"
        ).fetchall()
    for row in rows:
        counts[row["status"]] = int(row["count"])
    counts["active"] = sum(counts[name] for name in ACTIVE_STATUSES)
    return counts


def set_channels_paused(chat_ids: Iterable, paused: bool) -> int:
    """Pause or resume channels without interrupting an active file write."""
    channel_ids = list(dict.fromkeys(str(chat_id) for chat_id in chat_ids if str(chat_id)))
    if not channel_ids:
        return 0

    now = _now()
    pause_value = 1 if paused else 0
    placeholders = ",".join("?" for _ in channel_ids)
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        for chat_id in channel_ids:
            _upsert_channel(connection, chat_id)
        cursor = connection.execute(
            f"""
            UPDATE download_channel_state
            SET config_revision = config_revision + CASE
                    WHEN paused = 1 AND ? = 0 THEN 1 ELSE 0 END,
                paused = ?, updated_at = ?
            WHERE chat_id IN ({placeholders})
            """,
            [pause_value, pause_value, now, *channel_ids],
        )
        if not paused:
            connection.execute(
                f"""
                UPDATE download_tasks
                SET status = 'retry_requested', updated_at = ?, next_retry_at = NULL
                WHERE chat_id IN ({placeholders})
                  AND status IN ('queued', 'retrying', 'paused')
                """,
                [now, *channel_ids],
            )
        for chat_id in channel_ids:
            _refresh_channel_state(connection, chat_id)
    return max(cursor.rowcount, 0)


def list_channels(
    search: str = "",
    status: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Return one channel page without aggregating every persisted file.

    ``download_channel_state`` is authoritative: every queue/import path
    upserts it before creating file evidence.  Paginate those few thousand
    rows first, then aggregate only the selected channel IDs.  The previous
    CTE grouped the complete task and history tables before applying LIMIT, so
    a 50-row dashboard page scanned millions of completed-file rows.
    """
    page_limit = min(max(int(limit), 1), 200)
    page_offset = max(int(offset), 0)
    requested_status = str(status or "").strip()
    search_value = f"%{str(search or '').strip().lower()}%"
    parameters: list = []
    filters = []
    if str(search or "").strip():
        filters.append(
            "(LOWER(state.chat_id) LIKE ? OR LOWER(state.chat_title) LIKE ?)"
        )
        parameters.extend([search_value, search_value])
    if requested_status:
        filters.append("state.lifecycle_state = ?")
        parameters.append(requested_status)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    with _connect() as connection:
        state_rows = connection.execute(
            f"""
            SELECT state.chat_id, state.chat_title, state.paused, state.enabled,
                   state.priority, state.group_name, state.last_read_message_id,
                   state.download_filter, state.validation_status,
                   state.validation_error, state.scan_status,
                   state.lifecycle_state AS status, state.state_reason,
                   COALESCE(state.state_changed_at, state.updated_at, '')
                       AS state_changed_at,
                   state.next_action_at, state.state_version, state.updated_at,
                   COUNT(*) OVER() AS _total_count
            FROM download_channel_state AS state
            {where}
            ORDER BY
                CASE state.lifecycle_state
                    WHEN 'downloading' THEN 0
                    WHEN 'blocked' THEN 1
                    WHEN 'backoff' THEN 2
                    WHEN 'queued' THEN 3
                    WHEN 'scanning' THEN 4
                    WHEN 'validating' THEN 5
                    WHEN 'paused' THEN 6
                    WHEN 'ready' THEN 7
                    WHEN 'completed' THEN 8
                    WHEN 'disabled' THEN 9
                    ELSE 10
                END,
                CASE WHEN CAST(state.chat_title AS INTEGER) = 0 THEN 1 ELSE 0 END,
                CAST(state.chat_title AS INTEGER) DESC,
                state.chat_title COLLATE NOCASE
            LIMIT ? OFFSET ?
            """,
            parameters + [page_limit, page_offset],
        ).fetchall()

        if not state_rows:
            return {"total": 0, "records": []}

        channel_ids = [str(row["chat_id"]) for row in state_rows]
        placeholders = ",".join("?" for _ in channel_ids)
        task_rows = connection.execute(
            f"""
            SELECT
                chat_id,
                MAX(NULLIF(chat_title, '')) AS chat_title,
                SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
                SUM(CASE WHEN status = 'downloading' THEN 1 ELSE 0 END) AS downloading,
                SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END) AS retrying,
                SUM(CASE WHEN status = 'retry_requested' THEN 1 ELSE 0 END)
                    AS retry_requested,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)
                    AS task_completed,
                SUM(CASE WHEN status = 'completed' THEN total_size ELSE 0 END)
                    AS task_completed_bytes,
                SUM(CASE WHEN status NOT IN ('completed', 'skipped')
                         THEN total_size ELSE 0 END) AS task_bytes,
                MAX(updated_at) AS task_updated_at
            FROM download_tasks
            WHERE chat_id IN ({placeholders})
            GROUP BY chat_id
            """,
            channel_ids,
        ).fetchall()
        history_rows = connection.execute(
            f"""
            SELECT chat_id, MAX(NULLIF(chat_title, '')) AS chat_title,
                   COUNT(*) AS completed, SUM(total_size) AS history_bytes,
                   MAX(completed_at) AS history_updated_at
            FROM download_history
            WHERE chat_id IN ({placeholders})
            GROUP BY chat_id
            """,
            channel_ids,
        ).fetchall()

    total = int(state_rows[0]["_total_count"])
    tasks_by_chat = {str(row["chat_id"]): dict(row) for row in task_rows}
    history_by_chat = {str(row["chat_id"]): dict(row) for row in history_rows}
    records = []
    count_keys = (
        "queued", "downloading", "retrying", "retry_requested", "failed",
        "cancelled", "skipped",
    )
    for state_row in state_rows:
        record = dict(state_row)
        record.pop("_total_count", None)
        state_updated_at = str(record.pop("updated_at", "") or "")
        chat_id = str(record["chat_id"])
        task = tasks_by_chat.get(chat_id, {})
        history = history_by_chat.get(chat_id, {})
        record["chat_title"] = (
            task.get("chat_title")
            or history.get("chat_title")
            or record.get("chat_title")
            or chat_id
        )
        for key in count_keys:
            record[key] = int(task.get(key) or 0)
        task_completed = int(task.get("task_completed") or 0)
        history_completed = int(history.get("completed") or 0)
        record["completed"] = history_completed or task_completed
        record["completed_size"] = (
            int(history.get("history_bytes") or 0)
            if history_completed
            else int(task.get("task_completed_bytes") or 0)
        )
        record["total_size"] = (
            int(task.get("task_bytes") or 0) + record["completed_size"]
        )
        record["pending"] = sum(
            record[key]
            for key in ("queued", "downloading", "retrying", "retry_requested")
        )
        record["total"] = record["pending"] + sum(
            record[key] for key in ("failed", "cancelled", "skipped", "completed")
        )
        record["last_activity"] = max(
            str(task.get("task_updated_at") or ""),
            str(history.get("history_updated_at") or ""),
            str(record.get("state_changed_at") or ""),
            state_updated_at,
        )
        records.append(record)
    return {"total": total, "records": records}


def scanning_channel_count() -> int:
    """Return how many channels are still actively scanning their history.

    Used to detect when a batch of channels has fully drained (no scan in
    progress and no active download task) so a completion notification can
    fire exactly once per batch.
    """
    with _connect() as connection:
        row = connection.execute(
            "SELECT COUNT(*) FROM download_channel_state WHERE scan_status = 'scanning'"
        ).fetchone()
    return int(row[0] or 0)


def channel_counts() -> dict:
    """Return channel-level counts used by the primary dashboard metrics."""
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN lifecycle_state = 'downloading' THEN 1 ELSE 0 END)
                    AS downloading,
                SUM(CASE WHEN lifecycle_state = 'queued' THEN 1 ELSE 0 END)
                    AS waiting,
                SUM(CASE WHEN lifecycle_state = 'blocked' THEN 1 ELSE 0 END)
                    AS failed,
                SUM(CASE WHEN lifecycle_state = 'paused' THEN 1 ELSE 0 END)
                    AS paused,
                SUM(CASE WHEN lifecycle_state = 'disabled' THEN 1 ELSE 0 END)
                    AS disabled
            FROM download_channel_state
            """
        ).fetchone()
    return {key: int(row[key] or 0) for key in row.keys()}


def list_channel_files(
    chat_id,
    statuses: Optional[Iterable[str]] = None,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """Return tasks and completed history for one channel without duplicates."""
    selected = [str(item) for item in (statuses or []) if str(item)]
    page_limit = min(max(int(limit), 1), 200)
    page_offset = max(int(offset), 0)
    parameters: list = [str(chat_id), str(chat_id)]
    status_filter = ""
    if selected:
        status_filter = "WHERE status IN ({})".format(
            ",".join("?" for _ in selected)
        )
        parameters.extend(selected)
    union_query = """
        SELECT chat_id, message_id, chat_title, file_name, media_type,
               total_size, save_path, status, attempts, max_attempts,
               error, updated_at, completed_at, next_retry_at
        FROM download_tasks
        WHERE chat_id = ?
        UNION ALL
        SELECT history.chat_id, history.message_id, history.chat_title,
               history.file_name, history.media_type, history.total_size,
               history.save_path, 'completed' AS status, 1 AS attempts,
               1 AS max_attempts, '' AS error,
               history.completed_at AS updated_at, history.completed_at,
               NULL AS next_retry_at
        FROM download_history AS history
        WHERE history.chat_id = ?
          AND NOT EXISTS (
              SELECT 1 FROM download_tasks AS task
              WHERE task.chat_id = history.chat_id
                AND task.message_id = history.message_id
          )
    """
    with _connect() as connection:
        total = connection.execute(
            f"SELECT COUNT(*) FROM ({union_query}) {status_filter}", parameters
        ).fetchone()[0]
        rows = connection.execute(
            f"""
            SELECT * FROM ({union_query})
            {status_filter}
            ORDER BY updated_at DESC, message_id DESC
            LIMIT ? OFFSET ?
            """,
            parameters + [page_limit, page_offset],
        ).fetchall()
    return {"total": int(total), "records": [dict(row) for row in rows]}


def list_tasks(
    statuses: Optional[Iterable[str]] = None,
    limit: int = 50,
    offset: int = 0,
    oldest_first: bool = False,
    chat_id=None,
) -> dict:
    """Return a filtered page of persistent tasks, newest first."""
    selected = [str(status) for status in (statuses or []) if str(status)]
    page_limit = min(max(int(limit), 1), 500)
    page_offset = max(int(offset), 0)
    filters = []
    parameters: list = []
    if selected:
        filters.append("status IN ({})".format(
            ",".join("?" for _ in selected)
        ))
        parameters.extend(selected)
    if chat_id is not None:
        filters.append("chat_id = ?")
        parameters.append(str(chat_id))
    where = f" WHERE {' AND '.join(filters)}" if filters else ""

    order_by = (
        "queued_at ASC, message_id ASC"
        if oldest_first
        else "updated_at DESC, message_id DESC"
    )
    with _connect() as connection:
        total = connection.execute(
            f"SELECT COUNT(*) FROM download_tasks{where}", parameters
        ).fetchone()[0]
        rows = connection.execute(
            f"""
            SELECT chat_id, message_id, chat_title, file_name, media_type,
                   total_size, save_path, status, attempts, max_attempts,
                   error, queued_at, started_at, updated_at, completed_at,
                   next_retry_at
            FROM download_tasks{where}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
            """,
            parameters + [page_limit, page_offset],
        ).fetchall()
    return {"total": int(total), "records": [dict(row) for row in rows]}
