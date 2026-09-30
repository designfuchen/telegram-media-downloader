"""Persistent download history backed by SQLite."""

import os
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


DEFAULT_DATABASE_NAME = "download_history.db"
MESSAGE_ID_PATTERN = re.compile(r"^(\d+)\s+-\s+")
_SCHEMA_READY = set()
_SCHEMA_READY_LOCK = threading.Lock()


def get_history_path() -> Path:
    """Return the configured history database path."""
    configured = os.environ.get("TMD_HISTORY_DB") or os.environ.get("TMD_TASK_DB")
    return Path(configured or (Path.cwd() / DEFAULT_DATABASE_NAME)).expanduser()


def _connect() -> sqlite3.Connection:
    history_path = get_history_path()
    history_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(history_path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=10000")
    schema_key = str(history_path.resolve())
    with _SCHEMA_READY_LOCK:
        if schema_key in _SCHEMA_READY:
            return connection
    # The task repository owns the shared database schema and normally creates
    # this index during process startup.  The history page may be the first
    # caller of this module hours later, while downloads are actively writing.
    # Re-running the legacy deduplication DELETE in that request path requires
    # a write lock and can stall every transfer.  If the final schema already
    # exists, mark this module ready using only a sqlite_master read.
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
    # journal_mode is database-wide setup, not per-query setup. Reissuing it
    # from Flask readers while the downloader commits can itself require the
    # lock and turn a successful download into a false database failure.
    connection.execute("PRAGMA journal_mode=WAL")
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
        CREATE INDEX IF NOT EXISTS idx_download_history_completed_at
        ON download_history(completed_at DESC)
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


def record_download(
    chat_id,
    message_id: int,
    save_path: str,
    total_size: int,
    chat_title: str = "",
    media_type: str = "",
    completed_at: Optional[str] = None,
) -> None:
    """Insert or update a successfully downloaded file."""
    completed = completed_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    file_path = Path(save_path)
    with _connect() as connection:
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


def list_downloads(limit: int = 50, offset: int = 0) -> dict:
    """Return a page of completed downloads ordered newest first."""
    page_limit = min(max(int(limit), 1), 500)
    page_offset = max(int(offset), 0)
    with _connect() as connection:
        total = connection.execute(
            "SELECT COUNT(*) FROM download_history"
        ).fetchone()[0]
        rows = connection.execute(
            """
            SELECT chat_id, message_id, chat_title, file_name, total_size,
                   save_path, media_type, completed_at
            FROM download_history
            ORDER BY completed_at DESC, message_id DESC
            LIMIT ? OFFSET ?
            """,
            (page_limit, page_offset),
        ).fetchall()
    return {"total": total, "records": [dict(row) for row in rows]}


def history_count() -> int:
    """Return the number of persistent history records."""
    with _connect() as connection:
        return int(connection.execute("SELECT COUNT(*) FROM download_history").fetchone()[0])


def import_existing_files(save_path: str, chat_id) -> int:
    """Backfill history from existing files for a single configured chat."""
    root = Path(save_path).expanduser()
    if not root.is_dir():
        return 0

    imported = 0
    with _connect() as connection:
        for file_path in root.rglob("*"):
            if not file_path.is_file() or file_path.name.startswith("."):
                continue
            match = MESSAGE_ID_PATTERN.match(file_path.name)
            if not match:
                continue
            file_stat = file_path.stat()
            relative_parts = file_path.relative_to(root).parts
            chat_title = relative_parts[0] if len(relative_parts) > 1 else ""
            media_type = file_path.suffix.lower().lstrip(".")
            completed_at = datetime.fromtimestamp(
                file_stat.st_mtime, timezone.utc
            ).isoformat(timespec="seconds")
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO download_history (
                    chat_id, message_id, chat_title, file_name, total_size,
                    save_path, media_type, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(chat_id),
                    int(match.group(1)),
                    chat_title,
                    file_path.name,
                    file_stat.st_size,
                    str(file_path),
                    media_type,
                    completed_at,
                ),
            )
            imported += max(cursor.rowcount, 0)
    return imported
