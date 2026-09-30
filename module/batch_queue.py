"""Persistent manual-join batches for the Telegram download workflow.

The downloader never joins a Telegram channel.  It only verifies invite links
after the user has joined a fixed batch by hand, then imports the channels the
account can already access.  A persisted current-batch pointer prevents page
refreshes, restarts, or partial failures from silently moving on to new links.
"""

import csv
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from module.download_history import get_history_path

BATCH_SIZE = 10
TERMINAL_STATUSES = {"imported", "skipped"}
VERIFYABLE_STATUSES = {"pending", "not_joined", "transient_error"}
RETRYABLE_STATUSES = {"pending", "not_joined", "transient_error", "invalid"}
TRANSIENT_ERROR_TOKENS = (
    "connection lost",
    "connection reset",
    "connection refused",
    "network",
    "proxy",
    "timeout",
    "timed out",
    "temporarily unavailable",
    "floodwait",
    "flood wait",
    "socket",
)
PERMANENT_ERROR_TOKENS = (
    "invitehashinvalid",
    "invite_hash_invalid",
    "invite hash invalid",
    "invitehashexpired",
    "invite_hash_expired",
    "invite hash expired",
)
_SCHEMA_READY = set()
_SCHEMA_READY_LOCK = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_column(
    connection: sqlite3.Connection, table: str, name: str, definition: str
) -> None:
    columns = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if name not in columns:
        try:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
        except sqlite3.OperationalError as error:
            # Two startup workers may initialize a brand-new database at the
            # same time. A concurrent successful ALTER makes this harmless.
            if "duplicate column" not in str(error).lower():
                raise


def _assign_missing_batches(connection: sqlite3.Connection) -> None:
    """Give legacy/unassigned rows a stable 20-row batch number."""
    rows = connection.execute(
        "SELECT order_no, batch_no FROM channel_batch_queue ORDER BY order_no"
    ).fetchall()
    for index, row in enumerate(rows):
        if int(row["batch_no"] or 0) > 0:
            continue
        connection.execute(
            "UPDATE channel_batch_queue SET batch_no = ? WHERE order_no = ?",
            (index // BATCH_SIZE + 1, row["order_no"]),
        )


def _initial_batch_number(connection: sqlite3.Connection) -> int:
    row = connection.execute("""
        SELECT MIN(batch_no) AS batch_no
        FROM channel_batch_queue
        WHERE status NOT IN ('imported', 'skipped')
        """).fetchone()
    if row and row["batch_no"]:
        return int(row["batch_no"])
    row = connection.execute(
        "SELECT COALESCE(MAX(batch_no), 1) AS batch_no FROM channel_batch_queue"
    ).fetchone()
    return max(int(row["batch_no"] or 1), 1)


def _connect() -> sqlite3.Connection:
    path = os.environ.get("TMD_TASK_DB") or get_history_path()
    path_key = os.path.abspath(os.path.expanduser(str(path)))
    os.makedirs(os.path.dirname(path_key) or ".", exist_ok=True)
    connection = sqlite3.connect(path_key, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=10000")
    with _SCHEMA_READY_LOCK:
        if path_key in _SCHEMA_READY:
            return connection
    connection.execute("""
        CREATE TABLE IF NOT EXISTS channel_batch_queue (
            order_no INTEGER PRIMARY KEY,
            invite_link TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            chat_id TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT ''
        )
        """)
    _ensure_column(
        connection, "channel_batch_queue", "batch_no", "INTEGER NOT NULL DEFAULT 0"
    )
    _ensure_column(
        connection, "channel_batch_queue", "attempts", "INTEGER NOT NULL DEFAULT 0"
    )
    _ensure_column(
        connection, "channel_batch_queue", "error_kind", "TEXT NOT NULL DEFAULT ''"
    )
    _ensure_column(
        connection, "channel_batch_queue", "next_retry_at", "TEXT NOT NULL DEFAULT ''"
    )
    _ensure_column(
        connection, "channel_batch_queue", "last_attempt_at", "TEXT NOT NULL DEFAULT ''"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_batch_status ON channel_batch_queue(status, order_no)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_batch_number ON channel_batch_queue(batch_no, order_no)"
    )
    connection.execute("""
        CREATE TABLE IF NOT EXISTS channel_batch_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_batch_no INTEGER NOT NULL DEFAULT 1,
            batch_size INTEGER NOT NULL DEFAULT 20,
            updated_at TEXT NOT NULL DEFAULT ''
        )
        """)
    _assign_missing_batches(connection)

    # The old worker treated every exception as a permanently invalid link.
    # Safely recover known transport failures into the new transient state.
    transient_sql = " OR ".join("LOWER(error) LIKE ?" for _ in TRANSIENT_ERROR_TOKENS)
    connection.execute(
        f"""
        UPDATE channel_batch_queue
        SET status = 'transient_error', error_kind = 'transient',
            error = REPLACE(error, '邀请链接无效：', '临时连接失败：')
        WHERE status = 'failed' AND ({transient_sql})
        """,
        tuple(f"%{token}%" for token in TRANSIENT_ERROR_TOKENS),
    )
    connection.execute("""
        UPDATE channel_batch_queue
        SET error = REPLACE(error, '邀请链接无效：', '临时连接失败：')
        WHERE status = 'transient_error' AND error LIKE '邀请链接无效：%'
        """)
    connection.execute("""
        UPDATE channel_batch_queue
        SET status = 'invalid', error_kind = 'permanent'
        WHERE status = 'failed'
        """)
    if not connection.execute(
        "SELECT 1 FROM channel_batch_state WHERE id = 1"
    ).fetchone():
        connection.execute(
            """
            INSERT INTO channel_batch_state
                (id, current_batch_no, batch_size, updated_at)
            VALUES (1, ?, ?, ?)
            """,
            (_initial_batch_number(connection), BATCH_SIZE, _now()),
        )
    # Schema upgrades and legacy-state normalization happen before callers
    # start their own IMMEDIATE transaction (for example while claiming work).
    connection.commit()
    with _SCHEMA_READY_LOCK:
        _SCHEMA_READY.add(path_key)
    return connection


def _current_number(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT current_batch_no FROM channel_batch_state WHERE id = 1"
    ).fetchone()
    return max(int(row["current_batch_no"] if row else 1), 1)


def _batch_payload(connection: sqlite3.Connection, batch_no: int) -> dict:
    rows = connection.execute(
        """
        SELECT order_no, batch_no, invite_link, title, chat_id, status,
               error, error_kind, attempts, next_retry_at, last_attempt_at,
               updated_at
        FROM channel_batch_queue
        WHERE batch_no = ?
        ORDER BY order_no
        """,
        (int(batch_no),),
    ).fetchall()
    records = [dict(row) for row in rows]
    counts = {}
    for record in records:
        counts[record["status"]] = counts.get(record["status"], 0) + 1
    imported = counts.get("imported", 0)
    skipped = counts.get("skipped", 0)
    total = len(records)
    complete = bool(total) and imported + skipped == total
    next_row = connection.execute(
        "SELECT MIN(batch_no) AS batch_no FROM channel_batch_queue WHERE batch_no > ?",
        (int(batch_no),),
    ).fetchone()
    return {
        "number": int(batch_no),
        "batch_size": BATCH_SIZE,
        "total": total,
        "imported": imported,
        "skipped": skipped,
        "pending": counts.get("pending", 0) + counts.get("not_joined", 0),
        "queued": counts.get("queued", 0) + counts.get("resolving", 0),
        "transient": counts.get("transient_error", 0),
        "invalid": counts.get("invalid", 0),
        "complete": complete,
        "can_verify": any(
            record["status"] in VERIFYABLE_STATUSES for record in records
        ),
        "can_retry": counts.get("transient_error", 0) > 0,
        "has_more": bool(next_row and next_row["batch_no"]),
        "can_advance": complete and bool(next_row and next_row["batch_no"]),
        "records": records,
    }


def load_from_csv(path: str) -> dict:
    """Load invite links once without resetting any existing progress."""
    if not path or not os.path.exists(path):
        return {"loaded": 0, "skipped": 0, "total_in_db": total_count()}

    inserted = 0
    skipped = 0
    with open(path, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    with _connect() as connection:
        for row in rows:
            invite = str(row.get("invite_link") or "").strip()
            if not invite:
                continue
            try:
                order_no = int(str(row.get("order") or "0").strip() or 0)
            except ValueError:
                order_no = 0
            title = str(row.get("title") or row.get("label") or "").strip()
            if connection.execute(
                "SELECT 1 FROM channel_batch_queue WHERE invite_link = ?", (invite,)
            ).fetchone():
                skipped += 1
                continue
            chat_id = str(row.get("chat_id") or "").strip()
            connection.execute(
                """
                INSERT OR IGNORE INTO channel_batch_queue
                    (order_no, invite_link, title, chat_id, status, updated_at)
                VALUES (?, ?, ?, ?, 'pending', ?)
                """,
                (order_no or None, invite, title, chat_id, _now()),
            )
            inserted += 1
        _assign_missing_batches(connection)
    return {"loaded": inserted, "skipped": skipped, "total_in_db": total_count()}


def total_count() -> int:
    with _connect() as connection:
        return int(
            connection.execute("SELECT COUNT(*) FROM channel_batch_queue").fetchone()[0]
        )


def summary() -> dict:
    """Return global counters plus the persisted current batch summary."""
    with _connect() as connection:
        rows = connection.execute(
            "SELECT status, COUNT(*) AS c FROM channel_batch_queue GROUP BY status"
        ).fetchall()
        current = _batch_payload(connection, _current_number(connection))
    counts = {row["status"]: int(row["c"]) for row in rows}
    current.pop("records", None)
    return {
        "total": sum(counts.values()),
        "pending": counts.get("pending", 0) + counts.get("not_joined", 0),
        "queued": counts.get("queued", 0) + counts.get("resolving", 0),
        "imported": counts.get("imported", 0),
        "not_joined": counts.get("not_joined", 0),
        "transient": counts.get("transient_error", 0),
        "invalid": counts.get("invalid", 0),
        "failed": counts.get("transient_error", 0) + counts.get("invalid", 0),
        "skipped": counts.get("skipped", 0),
        "current": current,
    }


def current_batch() -> dict:
    """Return all rows in the fixed current batch."""
    with _connect() as connection:
        return _batch_payload(connection, _current_number(connection))


def plan_next(n: int = BATCH_SIZE) -> List[dict]:
    """Compatibility wrapper: the plan is always the fixed current batch."""
    return current_batch()["records"][: max(min(int(n), BATCH_SIZE), 1)]


def dispatch_next(n: int = BATCH_SIZE) -> List[dict]:
    """Queue only unresolved rows from the fixed current batch."""
    del n  # Batch membership is fixed; callers cannot accidentally take 40.
    now = _now()
    with _connect() as connection:
        batch_no = _current_number(connection)
        placeholders = ",".join("?" for _ in VERIFYABLE_STATUSES)
        rows = connection.execute(
            f"""
            SELECT order_no, invite_link, title, status
            FROM channel_batch_queue
            WHERE batch_no = ? AND status IN ({placeholders})
            ORDER BY order_no
            """,
            (batch_no, *sorted(VERIFYABLE_STATUSES)),
        ).fetchall()
        for row in rows:
            reset_attempts = row["status"] == "transient_error"
            connection.execute(
                """
                UPDATE channel_batch_queue
                SET status = 'queued', error = '', error_kind = '',
                    attempts = CASE WHEN ? THEN 0 ELSE attempts END,
                    next_retry_at = '', updated_at = ?
                WHERE order_no = ?
                """,
                (reset_attempts, now, row["order_no"]),
            )
    return [dict(row) for row in rows]


def claim_queued(limit: int = 5) -> List[dict]:
    """Claim ready rows and increment their persisted attempt counter."""
    now = _now()
    claimed = []
    with _connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            """
            SELECT order_no, invite_link, title, attempts
            FROM channel_batch_queue
            WHERE status = 'queued'
              AND (next_retry_at = '' OR next_retry_at <= ?)
            ORDER BY batch_no, order_no
            LIMIT ?
            """,
            (now, int(limit)),
        ).fetchall()
        for row in rows:
            attempt = int(row["attempts"] or 0) + 1
            connection.execute(
                """
                UPDATE channel_batch_queue
                SET status = 'resolving', attempts = ?, last_attempt_at = ?,
                    updated_at = ?
                WHERE order_no = ? AND status = 'queued'
                """,
                (attempt, now, now, row["order_no"]),
            )
            item = dict(row)
            item["attempt"] = attempt
            claimed.append(item)
    return claimed


def classify_resolution_error(error: Exception) -> str:
    """Classify only proven invalid-link errors as permanent."""
    value = f"{type(error).__name__} {error}".lower()
    if any(token in value for token in PERMANENT_ERROR_TOKENS):
        return "permanent"
    return "transient"


def mark_transient_error(
    order_no: int,
    error: str,
    retry_after: Optional[int] = None,
    max_auto_attempts: int = 3,
) -> dict:
    """Back off transient failures, then leave them for explicit user retry."""
    now_dt = datetime.now(timezone.utc)
    with _connect() as connection:
        row = connection.execute(
            "SELECT attempts FROM channel_batch_queue WHERE order_no = ?",
            (int(order_no),),
        ).fetchone()
        attempts = int(row["attempts"] or 0) if row else max_auto_attempts
        auto_retry = attempts < max(int(max_auto_attempts), 1)
        delay = max(int(retry_after or (5 * (2 ** max(attempts - 1, 0)))), 1)
        status = "queued" if auto_retry else "transient_error"
        next_retry_at = (
            (now_dt + timedelta(seconds=delay)).isoformat() if auto_retry else ""
        )
        connection.execute(
            """
            UPDATE channel_batch_queue
            SET status = ?, error = ?, error_kind = 'transient',
                next_retry_at = ?, updated_at = ?
            WHERE order_no = ?
            """,
            (
                status,
                str(error or "")[:500],
                next_retry_at,
                now_dt.isoformat(),
                int(order_no),
            ),
        )
    return {
        "status": status,
        "attempts": attempts,
        "retry_after": delay if auto_retry else 0,
    }


def mark_result(
    order_no: int,
    status: str,
    chat_id: str = "",
    error: str = "",
) -> None:
    """Record a successful, user-actionable, or permanent outcome."""
    allowed = {
        "imported",
        "not_joined",
        "invalid",
        "pending",
        "queued",
        "transient_error",
        "skipped",
    }
    normalized = str(status)
    if normalized not in allowed:
        raise ValueError(f"Unsupported batch status: {normalized}")
    error_kind = "permanent" if normalized == "invalid" else ""
    with _connect() as connection:
        connection.execute(
            """
            UPDATE channel_batch_queue
            SET status = ?, chat_id = CASE WHEN ? = '' THEN chat_id ELSE ? END,
                error = ?, error_kind = ?, next_retry_at = '', updated_at = ?
            WHERE order_no = ?
            """,
            (
                normalized,
                str(chat_id or ""),
                str(chat_id or ""),
                str(error or "")[:500],
                error_kind,
                _now(),
                int(order_no),
            ),
        )


def retry_failed(order_no: Optional[int] = None) -> List[dict]:
    """Retry one actionable row or all transient rows in the current batch."""
    now = _now()
    with _connect() as connection:
        batch_no = _current_number(connection)
        if order_no is None:
            rows = connection.execute(
                """
                SELECT order_no, invite_link, title, status
                FROM channel_batch_queue
                WHERE batch_no = ? AND status = 'transient_error'
                ORDER BY order_no
                """,
                (batch_no,),
            ).fetchall()
        else:
            placeholders = ",".join("?" for _ in RETRYABLE_STATUSES)
            rows = connection.execute(
                f"""
                SELECT order_no, invite_link, title, status
                FROM channel_batch_queue
                WHERE batch_no = ? AND order_no = ?
                  AND status IN ({placeholders})
                """,
                (batch_no, int(order_no), *sorted(RETRYABLE_STATUSES)),
            ).fetchall()
        for row in rows:
            connection.execute(
                """
                UPDATE channel_batch_queue
                SET status = 'queued', error = '', error_kind = '', attempts = 0,
                    next_retry_at = '', updated_at = ?
                WHERE order_no = ?
                """,
                (now, row["order_no"]),
            )
    return [dict(row) for row in rows]


def skip_item(order_no: int) -> bool:
    """Skip one unresolved row in the current batch so the user can advance."""
    with _connect() as connection:
        batch_no = _current_number(connection)
        cursor = connection.execute(
            """
            UPDATE channel_batch_queue
            SET status = 'skipped', error = '', error_kind = '',
                next_retry_at = '', updated_at = ?
            WHERE batch_no = ? AND order_no = ?
              AND status IN ('pending', 'not_joined', 'transient_error', 'invalid')
            """,
            (_now(), batch_no, int(order_no)),
        )
        return cursor.rowcount > 0


def advance_batch() -> dict:
    """Move to the next fixed batch only after every row is terminal."""
    with _connect() as connection:
        current_no = _current_number(connection)
        current = _batch_payload(connection, current_no)
        if not current["complete"]:
            raise ValueError("当前批次还有未处理频道，不能进入下一批")
        row = connection.execute(
            "SELECT MIN(batch_no) AS batch_no FROM channel_batch_queue WHERE batch_no > ?",
            (current_no,),
        ).fetchone()
        if not row or not row["batch_no"]:
            return current
        next_no = int(row["batch_no"])
        connection.execute(
            """
            UPDATE channel_batch_state
            SET current_batch_no = ?, updated_at = ? WHERE id = 1
            """,
            (next_no, _now()),
        )
        return _batch_payload(connection, next_no)
