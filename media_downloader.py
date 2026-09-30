"""Downloads media from telegram."""
import asyncio
import errno
import logging
import math
import os
import shutil
import sqlite3
import subprocess
import time
from typing import List, Optional, Tuple, Union

import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.async_utils import (
    TelegramRequestTimeout,
    run_blocking,
    run_db,
    telegram_call,
)
from module.download_history import history_count
from module.download_stat import (
    DownloadState,
    get_download_state,
    get_total_download_speed,
    is_channel_download_paused,
    remove_download_result,
    set_pause_release,
    take_pause_aborted,
    update_download_status,
)
from module.download_tasks import (
    claim_transport_probe,
    claim_import_items,
    channel_counts,
    complete_task_with_history,
    defer_task,
    fail_task,
    finish_task,
    get_channel_config,
    get_channel_configs,
    list_retry_requests,
    mark_import_item_validation,
    mark_retry_unavailable,
    queue_task as persist_queue_task,
    recover_interrupted_tasks,
    refill_retry_capacity,
    release_orphaned_claims,
    release_transport_retries,
    retire_completed_channel,
    pause_task,
    scanning_channel_count,
    start_task as persist_start_task,
    task_counts,
    task_claim_state,
    task_queued_age_seconds,
    update_channel_cursor,
    update_channel_scan_state,
)
from module.fair_scheduler import FairChannelScheduler
from module import batch_queue
from module import network_setup
from module import notify
from module import rate_governor
from module import speed_governor
from module import storage_health
from module.download_tasks import add_channel_to_library
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_status,
    set_max_concurrent_transmissions,
    set_download_streams_per_file,
    set_media_proxy_pool,
    set_media_session_pool_size,
    set_meta_data,
    update_cloud_upload_stat,
    upload_telegram_chat,
)
from module.web import init_web
from utils.format import truncate_filename, validate_title
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData
from utils.updates import check_for_updates

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)

CONFIG_NAME = os.environ.get("TMD_CONFIG_FILE", "config.yaml")
DATA_FILE_NAME = os.environ.get("TMD_DATA_FILE", "data.yaml")
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

queue = FairChannelScheduler()
RETRY_TIME_OUT = 3
# A media reference queued longer than this is proactively refreshed right
# before download, so a deep-queue file never hits FILE_REFERENCE_EXPIRED at
# download time (root fix for the mass simultaneous expiry of a channel whose
# whole queue was hydrated at once). Well under Telegram's reference lifetime.
REFERENCE_REFRESH_AGE = 1500  # seconds (25 min)
NAS_RETRY_INTERVAL = 5
CONNECTION_CHECK_INTERVAL = 20
# A throttled account makes even lightweight probes slow, so keep the timeout
# and the consecutive-failure budget generous. A spurious recycle is expensive:
# it wipes the speed governor's learned sweet spot and restarts every in-flight
# file from zero. Bias hard toward tolerance.
CONNECTION_CHECK_TIMEOUT = 20
CONNECTION_FAILURE_LIMIT = 5
METADATA_REQUEST_TIMEOUT = 45
_download_errors: dict = {}
_download_progress: dict = {}
# (chat_id, message_id) currently owned by a live worker. The orphan reaper
# treats any 'downloading' row that is NOT in here as a leaked claim, so a
# genuinely slow multi-GB transfer is never reclaimed however long it runs.
_active_claims: set = set()
ORPHAN_CLAIM_SCAN_INTERVAL = 300
ORPHAN_CLAIM_MIN_AGE = 900
_channel_registry_revisions: dict = {}
_channel_scan_tasks: dict = {}
CHANNEL_SCAN_CONCURRENCY = 2
_channel_scan_limiter = None
_channel_scan_limiter_loop = None
NAS_FINALIZE_CONCURRENCY = 4
_nas_finalize_limiter = None
_nas_finalize_limiter_loop = None
_monotonic = time.monotonic
_retry_wakeup = asyncio.Event()
RECENT_TRANSPORT_HEALTH_WINDOW = 15.0
_last_successful_transfer_at = 0.0

# Forwarding, cloud uploads and bot notifications are completion side effects.
# They must never occupy one of the media-transfer workers after the file and
# its durable database state are complete.  Keep the queue bounded so a broken
# upload destination cannot consume unbounded memory.
POSTPROCESS_WORKERS = 4
POSTPROCESS_QUEUE_SIZE = 256
SCHEDULER_PREFETCH_RESERVE = 25
_postprocess_queue: asyncio.Queue = asyncio.Queue(
    maxsize=POSTPROCESS_QUEUE_SIZE
)


class IncompleteDownloadError(IOError):
    """Telegram returned a file whose size does not match its metadata."""

    def __init__(self, message: str, *, downloaded_bytes: int = -1):
        super().__init__(message)
        self.downloaded_bytes = int(downloaded_bytes)


class DownloadStalledError(TimeoutError):
    """A media transfer stopped progressing for longer than the configured limit."""

    def __init__(self, message: str, *, had_progress: bool = False):
        super().__init__(message)
        self.had_progress = bool(had_progress)


def _get_channel_scan_limiter() -> asyncio.Semaphore:
    """Return one loop-local limiter shared by startup and imported scans."""
    global _channel_scan_limiter  # pylint: disable=global-statement
    global _channel_scan_limiter_loop  # pylint: disable=global-statement
    loop = asyncio.get_running_loop()
    if _channel_scan_limiter is None or _channel_scan_limiter_loop is not loop:
        _channel_scan_limiter = asyncio.Semaphore(CHANNEL_SCAN_CONCURRENCY)
        _channel_scan_limiter_loop = loop
    return _channel_scan_limiter


def _get_nas_finalize_limiter() -> asyncio.Semaphore:
    """Bound cross-filesystem copies without blocking the asyncio loop."""
    global _nas_finalize_limiter  # pylint: disable=global-statement
    global _nas_finalize_limiter_loop  # pylint: disable=global-statement
    loop = asyncio.get_running_loop()
    if _nas_finalize_limiter is None or _nas_finalize_limiter_loop is not loop:
        _nas_finalize_limiter = asyncio.Semaphore(NAS_FINALIZE_CONCURRENCY)
        _nas_finalize_limiter_loop = loop
    return _nas_finalize_limiter


def _scheduler_prefetch_free_slots(snapshot: Optional[dict] = None) -> int:
    """Return bounded hot-queue capacity beyond the active worker pool.

    Thousands of scanned messages must remain durable in SQLite rather than
    sitting in memory until their Telegram file references expire.  A small
    reserve still hides grouped GetMessages latency when a transfer finishes.
    """
    state = snapshot if snapshot is not None else queue.snapshot()
    occupied = int(state.get("queued", 0)) + int(state.get("in_flight", 0))
    workers = max(int(queue.max_workers), 1)
    target = workers + min(workers, SCHEDULER_PREFETCH_RESERVE)
    return max(target - occupied, 0)


def _scheduler_can_prefetch(chat_id, snapshot: Optional[dict] = None) -> bool:
    """Admit hot work without bypassing the active-channel ceiling."""
    state = snapshot if snapshot is not None else queue.snapshot()
    if _scheduler_prefetch_free_slots(state) <= 0:
        return False
    channel_limit = max(int(queue.max_active_channels or 0), 0)
    if channel_limit <= 0:
        return True
    channel_states = {
        str(item["chat_id"]): item
        for item in state.get("channels", [])
        if int(item.get("queued", 0)) + int(item.get("in_flight", 0)) > 0
    }
    chat_key = str(chat_id)
    if chat_key not in channel_states:
        if len(channel_states) < channel_limit:
            return True
        # A full active-channel set can still leave global workers idle when
        # one or more active channels have no queued replacement.  Admit only
        # the single standby channel that retry_request_worker probes; the
        # scheduler itself already limits that standby to the real vacancy.
        return bool(_standby_channel_slots(state, channel_limit))
    reserve_per_channel = math.ceil(
        SCHEDULER_PREFETCH_RESERVE / max(channel_limit, 1)
    )
    channel_target = max(int(queue.max_per_channel), 1) + reserve_per_channel
    channel_state = channel_states[chat_key]
    channel_occupied = int(channel_state.get("queued", 0)) + int(
        channel_state.get("in_flight", 0)
    )
    return channel_occupied < channel_target


def _reference_is_stale(queued_age_seconds: float) -> bool:
    """Whether a queued task is old enough that its media reference should be
    proactively refreshed before download. Pure function for testability."""
    return float(queued_age_seconds or 0.0) >= REFERENCE_REFRESH_AGE


async def _refresh_reference_if_stale(client, message, node):
    """Proactively refresh a long-queued file's reference just before download.

    Returns a message with a fresh reference when the task has sat in queue
    longer than REFERENCE_REFRESH_AGE; otherwise returns it unchanged.
    A FloodWait is propagated after updating the account governor so this file
    releases its worker slot instead of sleeping inside the global pool.
    """
    try:
        age = await run_db(task_queued_age_seconds, node.chat_id, message.id)
    except Exception:  # pylint: disable=broad-except
        return message
    if not _reference_is_stale(age):
        return message
    try:
        # This runs AFTER the task has been claimed (status='downloading'), so
        # an unbounded wait here holds that claim forever: the worker never
        # reaches its failure path, `updated_at` never moves, and only a
        # process restart frees it. Measured 2026-08-14: one task sat in
        # 'downloading' for 15.5 hours this way, and once every other channel
        # had drained it was the only work left, so the downloader looked
        # paused. Every other metadata call in this file is already bounded by
        # METADATA_REQUEST_TIMEOUT; this was the one that was not.
        # TelegramRequestTimeout subclasses TimeoutError, so the broad handler
        # below falls back to the un-refreshed message, which is the same
        # behaviour as any other refresh failure.
        return await telegram_call(
            fetch_message(client, message),
            METADATA_REQUEST_TIMEOUT,
            f"reference refresh for message {message.id}",
        )
    except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
        rate_governor.penalize(wait_err.value + 3)
        raise
    except Exception:  # pylint: disable=broad-except
        return message


def _should_refresh_reference(error: Exception) -> bool:
    """Whether a failed transfer warrants re-fetching the message for a fresh
    file reference before retrying.

    Two symptoms of a stale/expired media reference, both of which the retry
    loop must recover from by refreshing the reference (otherwise it loops on
    the same dead reference until it gives up — the FILE_REFERENCE_EXPIRED
    stall that blocks deep-queue channels):

    * ``DownloadStalledError`` that never produced a first byte, and
    * ``IncompleteDownloadError`` that downloaded exactly 0 bytes — a
      FILE_REFERENCE_EXPIRED surfaces here as a 0-byte file rather than a
      raised FileReferenceExpired.

    A transfer that already received some bytes is NOT refreshed: keep the
    message so Pyrogram can resume the partial file instead of restarting.
    """
    if isinstance(error, DownloadStalledError):
        return not error.had_progress
    if isinstance(error, IncompleteDownloadError):
        return error.downloaded_bytes == 0
    return False

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)


def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """Check download task if finish

    Parameters
    ----------
    media_size: int
        The size of the downloaded resource
    download_path: str
        Resource download hold path
    ui_file_name: str
        Really show file name

    """
    download_size = os.path.getsize(download_path)
    if media_size == download_size:
        logger.success(f"{_t('Successfully downloaded')} - {ui_file_name}")
    else:
        logger.warning(
            f"{_t('Media downloaded with wrong size')}: "
            f"{download_size}, {_t('actual')}: "
            f"{media_size}, {_t('file name')}: {ui_file_name}"
        )
        os.remove(download_path)
        raise IncompleteDownloadError(
            f"downloaded {download_size} bytes, expected {media_size} bytes",
            downloaded_bytes=download_size,
        )


async def _download_media_with_watchdog(
    client: pyrogram.Client,
    message: pyrogram.types.Message,
    temp_file_name: str,
    progress_args: tuple,
    progress_state: Optional[dict] = None,
):
    """Download one file and cancel only that coroutine if it stops progressing.

    ``progress_state`` may be supplied by the caller so it can tell afterwards
    whether this attempt actually moved the file forward, which decides how
    soon the persisted retry should run again.
    """
    first_byte_timeout = max(
        float(app.config.get("download_first_byte_timeout", 30) or 30), 5.0
    )
    stall_timeout = max(
        float(app.config.get("download_stall_timeout", 60) or 60), 10.0
    )
    if progress_state is None:
        progress_state = {}
    progress_state.setdefault("bytes", 0)
    progress_state["changed_at"] = _monotonic()

    async def tracked_progress(current: int, total: int, *args):
        if int(current or 0) > progress_state["bytes"]:
            progress_state["bytes"] = int(current)
            progress_state["changed_at"] = _monotonic()
            speed_governor.record_transfer_progress(progress_args[3].chat_id)
        await update_download_status(current, total, *args)

    transfer = asyncio.create_task(
        client.download_media(
            message,
            file_name=temp_file_name,
            progress=tracked_progress,
            progress_args=progress_args,
        )
    )

    async def cancel_transfer():
        transfer.cancel()
        done, _ = await asyncio.wait((transfer,), timeout=5)
        if done:
            await asyncio.gather(transfer, return_exceptions=True)
        else:
            logger.warning(
                f"Message[{message.id}] cancellation did not settle in 5 seconds; "
                "slot released and late result will be consumed"
            )

            def consume_late_result(task):
                try:
                    task.result()
                except (asyncio.CancelledError, Exception):
                    pass

            transfer.add_done_callback(consume_late_result)

    try:
        while not transfer.done():
            done, _ = await asyncio.wait((transfer,), timeout=2)
            if done:
                break
            if (
                get_download_state() is DownloadState.StopDownload
                or is_channel_download_paused(progress_args[3].chat_id)
            ):
                progress_state["changed_at"] = _monotonic()
                continue
            timeout = stall_timeout if progress_state["bytes"] else first_byte_timeout
            if _monotonic() - progress_state["changed_at"] >= timeout:
                await cancel_transfer()
                had_progress = bool(progress_state["bytes"])
                phase = "传输无进度" if had_progress else "等待首字节"
                if not had_progress:
                    # A burst of these means the account is throttled
                    # server-side; the speed governor trips a global cooldown.
                    speed_governor.record_first_byte_stall()
                raise DownloadStalledError(
                    f"{phase}超过 {int(timeout)} 秒",
                    had_progress=had_progress,
                )
        return await transfer
    except asyncio.CancelledError:
        await cancel_transfer()
        raise


def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file to download path

    Parameters
    ----------
    temp_download_path: str
        Temporary download path

    download_path: str
        Download path

    """

    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    try:
        # Same-filesystem rename is atomic: the final name is never observable
        # with partial content and an older valid file remains intact until
        # the new download has passed its size check.
        os.replace(temp_download_path, download_path)
        return
    except OSError as error:
        if error.errno != errno.EXDEV:
            raise

    # A custom temp_path may be on another filesystem. Copy to a sibling
    # staging name, fsync it, and only then atomically publish the final name.
    staging_path = f"{download_path}.tmd-finalizing"
    try:
        with open(temp_download_path, "rb") as source, open(
            staging_path, "wb"
        ) as target:
            shutil.copyfileobj(source, target, length=4 * 1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        os.replace(staging_path, download_path)
        os.remove(temp_download_path)
    finally:
        if os.path.exists(staging_path):
            os.remove(staging_path)


async def _finalize_download_file(temp_download_path: str, download_path: str):
    """Publish a verified file without blocking every asyncio worker.

    ``temp_path`` and the NAS download path can live on different filesystems.
    In that case ``_move_to_download_path`` copies and fsyncs the complete file,
    which is intentionally synchronous for durability but must not run on the
    event-loop thread.  Bound those copies so they cannot saturate the NAS.
    """
    async with _get_nas_finalize_limiter():
        await run_blocking(
            _move_to_download_path,
            temp_download_path,
            download_path,
        )


def _check_timeout(retry: int, _: int):
    """Check if message download timeout, then add message id into failed_ids

    Parameters
    ----------
    retry: int
        Retry download message times

    message_id: int
        Try to download message 's id

    """
    if retry == 2:
        return True
    return False


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)


def _existing_file_is_complete(file_size: int, expected_size: int) -> bool:
    """Only skip a file when available Telegram metadata proves it complete."""
    actual = max(int(file_size or 0), 0)
    expected = max(int(expected_size or 0), 0)
    if expected:
        return actual == expected
    # Some old/text-like media lacks a size. Preserve an existing non-empty
    # file rather than destroying it when no authoritative comparison exists.
    return actual > 0


def _task_key(chat_id, message_id: int) -> tuple:
    """Return a stable in-memory key for a Telegram download attempt."""
    return str(chat_id), int(message_id)


def _remember_download_error(chat_id, message_id: int, error: str) -> None:
    """Keep the most useful failure reason until the task is persisted."""
    _download_errors[_task_key(chat_id, message_id)] = str(error)[:2000]


def _remember_download_progress(chat_id, message_id: int, advanced: bool) -> None:
    """Record whether the just-failed attempt added bytes to the partial file.

    Resumable downloads keep their verified chunks, so an attempt that moved a
    large file forward is making progress even though it ended in an error and
    must be retried within a minute rather than after the full transport
    backoff.  An attempt that produced nothing is the one that has to back off.
    """
    _download_progress[_task_key(chat_id, message_id)] = bool(advanced)


def _message_task_metadata(message: pyrogram.types.Message) -> dict:
    """Extract lightweight task metadata without downloading the message."""
    chat_title = ""
    if message.chat:
        chat_title = (
            getattr(message.chat, "title", None)
            or getattr(message.chat, "first_name", None)
            or ""
        )

    for media_type in (
        "audio",
        "document",
        "photo",
        "video",
        "voice",
        "video_note",
        "animation",
    ):
        media = getattr(message, media_type, None)
        if media is not None:
            return {
                "chat_title": chat_title,
                "file_name": getattr(media, "file_name", None) or "",
                "media_type": media_type,
                "total_size": int(getattr(media, "file_size", 0) or 0),
            }

    return {
        "chat_title": chat_title,
        "file_name": "",
        "media_type": "text" if message.text else "",
        "total_size": 0,
    }


def _storage_is_ready(save_path: str) -> bool:
    """Platform-agnostic readiness check for the configured download target."""
    return storage_health.storage_is_ready(
        save_path, require_mount=bool(app.config.get("nas_require_mount", False))
    )


async def _wait_for_save_path() -> bool:
    """Block downloads while the NAS/target storage is unavailable.

    Never writes to a local fallback; the queue is preserved and downloads
    resume automatically once storage returns. Works on macOS, Linux and
    inside Docker (no ``/Volumes`` assumption).
    """
    save_path = os.path.abspath(os.path.expanduser(app.save_path))
    # Best-effort create for local first-run, but never for a required mount
    # (creating it could mask an unmounted share).
    if not bool(app.config.get("nas_require_mount", False)):
        try:
            await run_blocking(os.makedirs, save_path, exist_ok=True)
        except OSError:
            pass

    if await run_blocking(_storage_is_ready, save_path):
        return True

    warned = False
    while app.is_running and not await run_blocking(_storage_is_ready, save_path):
        if not warned:
            logger.warning(f"存储不可用（NAS 掉线？），已暂停下载并保留队列: {save_path}")
            warned = True
        await asyncio.sleep(NAS_RETRY_INTERVAL)

    if warned and app.is_running:
        logger.success(f"存储已恢复，继续下载: {save_path}")
    return app.is_running


def _parse_chat_id(value):
    """Restore numeric Telegram chat IDs read from SQLite."""
    text = str(value)
    return int(text) if text.lstrip("-").isdigit() else text


# pylint: disable = R0912


async def _get_media_meta(
    chat_id: Union[int, str],
    message: pyrogram.types.Message,
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
) -> Tuple[str, str, Optional[str]]:
    """Extract file name and file id from media object.

    Parameters
    ----------
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice]
        Media object to be extracted.
    _type: str
        Type of media object.

    Returns
    -------
    Tuple[str, str, Optional[str]]
        file_name, file_format
    """
    if _type in ["audio", "document", "video"]:
        # pylint: disable = C0301
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]  # type: ignore
    else:
        file_format = None

    file_name = None
    temp_file_name = None
    dirname = validate_title(f"{chat_id}")
    if message.chat and message.chat.title:
        dirname = validate_title(f"{message.chat.title}")

    if message.date:
        datetime_dir_name = message.date.strftime(app.date_format)
    else:
        datetime_dir_name = "0"

    if _type in ["voice", "video_note"]:
        # pylint: disable = C0209
        file_format = media_obj.mime_type.split("/")[-1]  # type: ignore
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        file_name = "{} - {}_{}.{}".format(
            message.id,
            _type,
            media_obj.date.isoformat(),  # type: ignore
            file_format,
        )
        file_name = validate_title(file_name)
        temp_file_name = os.path.join(app.temp_save_path, dirname, file_name)

        file_name = os.path.join(file_save_path, file_name)
    else:
        file_name = getattr(media_obj, "file_name", None)
        caption = getattr(message, "caption", None)

        file_name_suffix = ".unknown"
        if not file_name:
            file_name_suffix = get_extension(
                media_obj.file_id, getattr(media_obj, "mime_type", "")
            )
        else:
            # file_name = file_name.split(".")[0]
            _, file_name_without_suffix = os.path.split(os.path.normpath(file_name))
            file_name, file_name_suffix = os.path.splitext(file_name_without_suffix)
            if not file_name_suffix:
                file_name_suffix = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                )

        if caption:
            caption = validate_title(caption)
            app.set_caption_name(chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(chat_id, message.media_group_id)

        if not file_name and message.photo:
            file_name = f"{message.photo.file_unique_id}"

        gen_file_name = (
            app.get_file_name(message.id, file_name, caption) + file_name_suffix
        )

        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)

        temp_file_name = os.path.join(app.temp_save_path, dirname, gen_file_name)

        file_name = os.path.join(file_save_path, gen_file_name)
    return truncate_filename(file_name), truncate_filename(temp_file_name), file_format


async def add_download_task(
    message: pyrogram.types.Message,
    node: TaskNode,
    count_task: bool = True,
    force: bool = False,
):
    """Add Download task"""
    if message.empty:
        return False
    metadata = _message_task_metadata(message)
    if not await run_db(
        persist_queue_task,
        node.chat_id,
        message.id,
        force=force,
        refresh_state=False,
        **metadata,
    ):
        return False
    node.download_status[message.id] = DownloadStatus.Downloading
    if count_task:
        node.total_task += 1
    if not _scheduler_can_prefetch(node.chat_id):
        # The task is already durable. Keep it eligible for grouped
        # just-in-time hydration instead of retaining a file reference in a
        # huge in-memory queue until it expires.
        deferred = await run_db(
            defer_task,
            node.chat_id,
            message.id,
            "等待下载槽位，任务已持久化",
        )
        if deferred:
            _retry_wakeup.set()
            return True
        # A concurrent state transition won the race. Retain the in-memory
        # item unless persistence explicitly accepted the deferral.
    await queue.put((message, node))
    return True


async def save_msg_to_file(
    app, chat_id: Union[int, str], message: pyrogram.types.Message
):
    """Write message text into file"""
    dirname = validate_title(
        message.chat.title if message.chat and message.chat.title else str(chat_id)
    )
    datetime_dir_name = message.date.strftime(app.date_format) if message.date else "0"

    file_save_path = app.get_file_save_path("msg", dirname, datetime_dir_name)
    file_name = os.path.join(
        app.temp_save_path,
        file_save_path,
        f"{app.get_file_name(message.id, None, None)}.txt",
    )

    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None

    with open(file_name, "w", encoding="utf-8") as f:
        f.write(message.text or "")

    return DownloadStatus.SuccessDownload, file_name


def _download_needs_postprocess(node: TaskNode, download_status: DownloadStatus) -> bool:
    """Return whether completion has asynchronous side effects to perform."""
    cloud_upload = bool(
        not node.upload_telegram_chat_id
        and download_status is DownloadStatus.SuccessDownload
        and app.cloud_drive_config.enable_upload_file
    )
    bot_update = bool(node.bot and node.reply_message_id)
    return bool(node.upload_telegram_chat_id or cloud_upload or bot_update)


def _clear_terminal_node_status(
    message: pyrogram.types.Message,
    node: TaskNode,
    expected_status: DownloadStatus,
) -> None:
    """Remove only the terminal state owned by this completed attempt."""
    if (
        not message.media_group_id
        and node.download_status.get(message.id) is expected_status
    ):
        node.download_status.pop(message.id, None)
        node.upload_status.pop(message.id, None)


async def _run_download_postprocess(
    client: pyrogram.Client,
    message: pyrogram.types.Message,
    node: TaskNode,
    download_status: DownloadStatus,
    file_name: Optional[str],
) -> None:
    """Run non-transfer completion work outside the download worker pool."""
    await upload_telegram_chat(
        client,
        node.upload_user if node.upload_user else client,
        app,
        node,
        message,
        download_status,
        file_name,
    )

    if (
        not node.upload_telegram_chat_id
        and download_status is DownloadStatus.SuccessDownload
        and file_name
    ):
        ui_file_name = file_name
        if app.hide_file_name:
            ui_file_name = f"****{os.path.splitext(file_name)[-1]}"
        if await app.upload_file(
            file_name, update_cloud_upload_stat, (node, message.id, ui_file_name)
        ):
            node.upload_success_count += 1

    await report_bot_status(node.bot, node)


async def postprocess_worker(worker_index: int = 0) -> None:
    """Consume completion side effects without holding download slots."""
    while app.is_running or not _postprocess_queue.empty():
        payload = await _postprocess_queue.get()
        client, message, node, download_status, file_name = payload
        try:
            await _run_download_postprocess(
                client, message, node, download_status, file_name
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # pylint: disable=broad-except
            logger.exception(
                f"Postprocess worker[{worker_index}] Message[{message.id}] error: {error}"
            )
        finally:
            _clear_terminal_node_status(message, node, download_status)
            _postprocess_queue.task_done()


async def download_task(
    client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode
):
    """Download and Forward media"""

    task_key = _task_key(node.chat_id, message.id)
    _download_errors.pop(task_key, None)
    _download_progress.pop(task_key, None)
    download_status, file_name = await download_media(
        client, message, app.media_types, app.file_formats, node
    )

    if app.enable_download_txt and message.text and not message.media:
        download_status, file_name = await save_msg_to_file(app, node.chat_id, message)

    file_size = os.path.getsize(file_name) if file_name else 0
    metadata = _message_task_metadata(message)
    media_type = metadata["media_type"]
    retry_result = {"retry": False, "delay": 0, "attempts": 0}
    if download_status is not DownloadStatus.FailedDownload:
        _download_errors.pop(task_key, None)

    if download_status is DownloadStatus.SuccessDownload and file_name:
        try:
            await run_db(
                complete_task_with_history,
                chat_id=node.chat_id,
                message_id=message.id,
                save_path=file_name,
                total_size=file_size,
                chat_title=metadata["chat_title"],
                media_type=media_type,
            )
        except Exception as history_error:
            # The file already passed size verification and is safely on disk.
            # Keep the task retryable so the database can reconcile it instead
            # of silently claiming success without history.
            _remember_download_error(
                node.chat_id,
                message.id,
                f"文件已落盘，但完成状态入库失败：{history_error}",
            )
            download_status = DownloadStatus.FailedDownload
            retry_result = await run_db(
                fail_task,
                node.chat_id,
                message.id,
                _download_errors.pop(task_key),
            )
    elif download_status is DownloadStatus.SkipDownload:
        if file_name and os.path.isfile(file_name):
            try:
                await run_db(
                    complete_task_with_history,
                    chat_id=node.chat_id,
                    message_id=message.id,
                    save_path=file_name,
                    total_size=os.path.getsize(file_name),
                    chat_title=metadata["chat_title"],
                    media_type=media_type,
                )
            except Exception as history_error:
                _remember_download_error(
                    node.chat_id,
                    message.id,
                    f"已有完整文件，但完成状态入库失败：{history_error}",
                )
                download_status = DownloadStatus.FailedDownload
                retry_result = await run_db(
                    fail_task,
                    node.chat_id,
                    message.id,
                    _download_errors.pop(task_key),
                )
        else:
            await run_db(
                finish_task,
                node.chat_id,
                message.id,
                "skipped",
                total_size=metadata["total_size"],
                file_name=metadata["file_name"],
                media_type=media_type,
            )
    elif download_status is DownloadStatus.PausedDownload:
        await run_db(pause_task, node.chat_id, message.id)
    elif download_status is DownloadStatus.FailedDownload:
        retry_result = await run_db(
            fail_task,
            node.chat_id,
            message.id,
            _download_errors.pop(task_key, "下载失败，请查看 log/tdl.log"),
            made_progress=_download_progress.pop(task_key, False),
        )

    if not node.bot:
        app.set_download_id(node, message.id, download_status)
    node.download_status[message.id] = download_status

    # Task accounting belongs to the durable transfer stage.  Forwarding,
    # cloud upload and notification may block for minutes and therefore run in
    # a separate bounded pool after this function returns the download slot.
    node.stat(download_status)
    node.total_download_byte += file_size
    if _download_needs_postprocess(node, download_status):
        await _postprocess_queue.put(
            (client, message, node, download_status, file_name)
        )
    else:
        _clear_terminal_node_status(message, node, download_status)
    return download_status, retry_result


# pylint: disable = R0915,R0914


@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    """
    Download media from Telegram.

    A failed transfer is returned to the durable retry queue.  This attempt
    never sleeps and retries while holding a global media-transfer slot.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Current message id.
    """

    # pylint: disable = R0912

    # Root fix for FILE_REFERENCE_EXPIRED on deep-queue channels: if this file
    # has been queued long enough for its reference to have expired, refresh it
    # now so the download below uses a reference that is fresh at the moment of
    # use. Fresh/just-queued files skip this entirely (no metadata cost).
    message = await _refresh_reference_if_stale(client, message, node)

    file_name: str = ""
    ui_file_name: str = ""
    task_start_time: float = time.time()
    media_size = 0
    _media = None
    try:
        for _type in media_types:
            _media = getattr(message, _type, None)
            if _media is None:
                continue
            file_name, temp_file_name, file_format = await _get_media_meta(
                node.chat_id, message, _media, _type
            )
            media_size = getattr(_media, "file_size", 0)

            ui_file_name = file_name
            if app.hide_file_name:
                ui_file_name = f"****{os.path.splitext(file_name)[-1]}"

            if _can_download(_type, file_formats, file_format):
                if _is_exist(file_name):
                    file_size = os.path.getsize(file_name)
                    if _existing_file_is_complete(file_size, media_size):
                        logger.info(
                            f"id={message.id} {ui_file_name} "
                            f"{_t('already download,download skipped')}.\n"
                        )

                        return DownloadStatus.SkipDownload, file_name
            else:
                return DownloadStatus.SkipDownload, None

            break
    except Exception as e:
        _remember_download_error(
            node.chat_id, message.id, f"读取媒体信息失败：{e}"
        )
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.FailedDownload, None
    if _media is None:
        return DownloadStatus.SkipDownload, None

    message_id = message.id
    # Owned here so the failure paths below can tell a productive attempt (the
    # resumable handler banked more chunks) from a wasted one (zero bytes).
    attempt_progress: dict = {"bytes": 0}

    try:
        temp_download_path = await _download_media_with_watchdog(
            client,
            message,
            temp_file_name,
            (
                message_id,
                ui_file_name,
                task_start_time,
                node,
                client,
            ),
            attempt_progress,
        )

        if temp_download_path and isinstance(temp_download_path, str):
            _check_download_finish(media_size, temp_download_path, ui_file_name)
            # The verified temporary file can be atomically published at once.
            # A fixed 500 ms sleep here used to hold every global slot after
            # the network transfer had already completed; small-image channels
            # spent far more time sleeping than downloading.
            await _finalize_download_file(temp_download_path, file_name)
            return DownloadStatus.SuccessDownload, file_name
    except pyrogram.errors.FileReferenceExpired as error:
        _remember_download_error(
            node.chat_id, message.id, f"Telegram 文件引用已过期：{error}"
        )
        logger.warning(
            f"Message[{message.id}] file reference expired; "
            "releasing the slot for grouped persisted refresh"
        )
    except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
        _remember_download_error(
            node.chat_id,
            message.id,
            f"Telegram 限流，需要等待 {wait_err.value} 秒",
        )
        rate_governor.penalize(wait_err.value + 3)
        speed_governor.penalize_flood()
        logger.warning("Message[{}]: FlowWait {}", message.id, wait_err.value)
    except (IncompleteDownloadError, DownloadStalledError) as error:
        _remember_download_error(node.chat_id, message.id, str(error))
        logger.warning(
            f"Message[{message.id}] transfer interrupted: {error}; "
            "releasing the slot for persisted retry"
        )
    except pyrogram.errors.BadRequest as error:
        _remember_download_error(
            node.chat_id, message.id, f"Telegram 请求无效：{error}"
        )
        logger.warning(f"Message[{message.id}] bad request: {error}")
    except TypeError as error:
        _remember_download_error(
            node.chat_id, message.id, f"下载超时：{error or '连接未响应'}"
        )
        logger.warning(
            f"{_t('Timeout Error occurred when downloading Message')}[{message.id}]; "
            "releasing the slot for persisted retry"
        )
    except Exception as e:
        # Aborted because its channel was paused -> not a failure; it will be
        # re-downloaded on resume without consuming a retry attempt.
        if take_pause_aborted(node.chat_id, message.id):
            return DownloadStatus.PausedDownload, None
        _remember_download_error(node.chat_id, message.id, str(e))
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )

    if _task_key(node.chat_id, message.id) not in _download_errors:
        _remember_download_error(
            node.chat_id, message.id, "Telegram 未返回下载文件"
        )
    _remember_download_progress(
        node.chat_id, message.id, int(attempt_progress.get("bytes", 0) or 0) > 0
    )
    return DownloadStatus.FailedDownload, None


def _load_config():
    """Load config"""
    app.load_config()


def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    from module.secure_session import install
    from module import config_io, secret_storage
    if not secret_storage.enabled():
        logger.error("需要受保护的凭证存储：请使用完整 Mac 应用，或设置 TMD_SECRET_KEY 后运行源码 / Docker。")
        return False
    install()
    try:
        _load_config()
        config_io.dump(app.config_file, app.config, __import__('yaml'))
        if os.path.exists(app.app_data_file):
            config_io.dump(app.app_data_file, app.app_data, __import__('yaml'))
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
            diagnose=False,
            backtrace=False,
        )
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False

    return True


def _schedule_retry(
    message: pyrogram.types.Message, node: TaskNode, retry_result: dict
) -> None:
    """Reserve accounting and wake the single persisted retry consumer."""
    if not app.keep_service_alive or not retry_result.get("retry"):
        return
    node.total_task += 1
    node.download_status[message.id] = DownloadStatus.Downloading
    # fail_task already persisted the retry state and next_retry_at.  Creating
    # a second sleeping coroutine here races retry_request_worker for the same
    # row and was the source of duplicate accounting and uneven refill.
    _retry_wakeup.set()


async def _wait_for_retry_wakeup(timeout: float = 2.0) -> None:
    """Sleep until capacity changes, while retaining a short polling fallback."""
    try:
        await asyncio.wait_for(_retry_wakeup.wait(), timeout=max(timeout, 0.01))
    except asyncio.TimeoutError:
        pass
    finally:
        _retry_wakeup.clear()


async def _hydrate_persisted_retry(
    message: pyrogram.types.Message, node: TaskNode
) -> bool:
    """Restore one persisted logical task without inflating channel totals."""
    return await add_download_task(message, node, count_task=False)


async def _hydrate_retry_group(client: pyrogram.Client, group: dict) -> int:
    """Refresh and enqueue one channel's retry batch in isolation.

    One unavailable channel must not abort the whole retry-refill pass.  Its
    files return to durable backoff while the remaining active channels keep
    receiving replacement work.
    """
    tasks_for_chat = group["tasks"]
    try:
        await rate_governor.acquire()
        messages = await telegram_call(
            client.get_messages(
                chat_id=group["chat_id"],
                message_ids=[
                    int(task["message_id"]) for task in tasks_for_chat
                ],
            ),
            METADATA_REQUEST_TIMEOUT,
            "retry GetMessages",
        )
    except pyrogram.errors.FloodWait as wait_error:
        rate_governor.penalize(wait_error.value)
        error_text = f"Telegram 限流，需要等待 {wait_error.value} 秒"
    except Exception as error:  # pylint: disable=broad-except
        error_text = f"retry GetMessages timed out: {error}"
    else:
        if not isinstance(messages, list):
            messages = [messages] if messages else []
        messages_by_id = {
            int(message.id): message
            for message in messages
            if message and not getattr(message, "empty", False)
        }
        hydrated = 0
        for task in tasks_for_chat:
            message = messages_by_id.get(int(task["message_id"]))
            if message is None:
                await run_db(
                    mark_retry_unavailable,
                    group["chat_id"],
                    task["message_id"],
                    "Telegram 消息不存在或当前账号无权访问",
                )
                continue
            if await _hydrate_persisted_retry(
                message, group["chat_config"].node
            ):
                hydrated += 1
        return hydrated

    for task in tasks_for_chat:
        await run_db(
            fail_task,
            group["chat_id"],
            task["message_id"],
            error_text,
        )
    logger.warning(
        f"Retry metadata refresh failed for channel {group['chat_id']}; "
        f"deferred {len(tasks_for_chat)} files without blocking other channels: "
        f"{error_text}"
    )
    return 0


async def _hydrate_retry_groups(client: pyrogram.Client, groups) -> int:
    """Hydrate independent channels concurrently.

    A metadata timeout is scoped to one channel.  Running channel groups one
    after another made a single 45-second timeout hold every free scheduler
    slot even though the other channels were healthy.
    """
    groups = list(groups)
    if not groups:
        return 0
    results = await asyncio.gather(
        *(_hydrate_retry_group(client, group) for group in groups)
    )
    return sum(int(result or 0) for result in results)


def _transport_recently_healthy() -> bool:
    """Treat a just-completed file as proof that refill is safe."""
    return bool(
        get_total_download_speed() > 0
        or (
            _last_successful_transfer_at > 0
            and _monotonic() - _last_successful_transfer_at
            <= RECENT_TRANSPORT_HEALTH_WINDOW
        )
    )


def _standby_channel_slots(scheduler_state: dict, channel_limit: int) -> int:
    """Return one fallback channel slot only when the hot set is idle.

    The configured active-channel count remains the normal working set.  A
    single standby is admitted only when all active channels have no queued
    work and the global scheduler has real vacancy; this prevents a cold
    channel from displacing ready work while avoiding the long zero-throughput
    gap seen at channel boundaries.
    """
    channel_limit = max(int(channel_limit or 0), 0)
    if channel_limit <= 0:
        return 0
    queued = int(scheduler_state.get("queued", 0) or 0)
    occupied = queued + int(scheduler_state.get("in_flight", 0) or 0)
    workers = max(int(scheduler_state.get("max_workers", 0) or 0), 0)
    active_channels = {
        str(item.get("chat_id"))
        for item in scheduler_state.get("channels", [])
        if int(item.get("queued", 0) or 0)
        + int(item.get("in_flight", 0) or 0)
        > 0
    }
    if (
        queued == 0
        and len(active_channels) >= channel_limit
        and workers > occupied
    ):
        return 1
    return 0


async def retry_request_worker(client: pyrogram.client.Client):
    """Consume retry requests created by recovery or the Web console."""
    await asyncio.sleep(2)
    last_probe_at = 0.0
    probe_interval = max(
        float(app.config.get("transport_probe_interval", 30) or 30), 10.0
    )
    while app.is_running:
        try:
            # Global pause must mean TOTAL silence toward Telegram — the
            # operator uses it to rest a throttled account. Retry hydration
            # calls GetMessages, so it has to stop here too; otherwise
            # "暂停全部" keeps poking Telegram every 2s and the rest never
            # actually happens.
            if get_download_state() is DownloadState.StopDownload:
                await asyncio.sleep(10)
                continue
            # Hydrate only the bounded hot-queue vacancy. The old fixed batch
            # of up to 100 ran every two seconds even when the scheduler was
            # already full, leaving thousands of in-memory messages to expire.
            scheduler_state = queue.snapshot()
            prefetch_free = _scheduler_prefetch_free_slots(scheduler_state)
            if prefetch_free <= 0:
                await _wait_for_retry_wakeup(2)
                continue
            # Grouping IDs by channel turns dozens of sequential GetMessages
            # round trips into one request per channel and reduces FloodWait.
            batch_limit = min(
                max(int(app.max_download_task) * 4, 1),
                500,
                prefetch_free,
            )
            recovered_chat_id = speed_governor.consume_recovery_signal()
            if recovered_chat_id is not None:
                released = await run_db(
                    release_transport_retries,
                    recovered_chat_id,
                    max(int(app.config.get("max_active_channels", 0) or 0), 1),
                    batch_limit,
                )
                if released:
                    logger.info(
                        f"Transport recovered; released {released} persisted retries"
                    )

            cooling = speed_governor.cooldown_remaining() > 0
            now = _monotonic()
            if cooling and now - last_probe_at < probe_interval:
                await asyncio.sleep(2)
                continue
            if cooling:
                batch_limit = 1

            # If Telegram is demonstrably returning bytes, keep the fixed pool
            # full instead of waiting up to an hour for every persisted
            # per-file backoff timestamp.  Only enough tasks to fill the real
            # free capacity are released, and only within the configured
            # channel/per-channel limits.
            occupied = int(scheduler_state.get("queued", 0)) + int(
                scheduler_state.get("in_flight", 0)
            )
            free_capacity = max(int(queue.max_workers) - occupied, 0)
            if not cooling and free_capacity > 0 and _transport_recently_healthy():
                active_capacity = {}
                for channel_state in scheduler_state.get("channels", []):
                    channel_occupied = int(channel_state.get("queued", 0)) + int(
                        channel_state.get("in_flight", 0)
                    )
                    active_capacity[str(channel_state["chat_id"])] = max(
                        int(queue.max_per_channel) - channel_occupied, 0
                    )
                released = await run_db(
                    refill_retry_capacity,
                    active_capacity,
                    max(int(queue.max_active_channels or 0), 1),
                    int(queue.max_per_channel),
                    min(free_capacity, batch_limit),
                )
                if released:
                    logger.info(
                        f"Live transport capacity refill released {released} persisted retries"
                    )

            participating = {
                str(item["chat_id"])
                for item in scheduler_state.get("channels", [])
                if int(item.get("queued", 0))
                + int(item.get("in_flight", 0))
                > 0
            }
            channel_limit = max(int(queue.max_active_channels or 0), 1)
            reserve_per_channel = math.ceil(
                SCHEDULER_PREFETCH_RESERVE / channel_limit
            )
            channel_hot_target = (
                max(int(queue.max_per_channel), 1) + reserve_per_channel
            )
            participating_capacity = {}
            for item in scheduler_state.get("channels", []):
                chat_key = str(item["chat_id"])
                if chat_key not in participating:
                    continue
                occupied_for_chat = int(item.get("queued", 0)) + int(
                    item.get("in_flight", 0)
                )
                participating_capacity[chat_key] = max(
                    channel_hot_target - occupied_for_chat, 0
                )
            standby_slots = 0
            if int(queue.max_active_channels or 0) > 0:
                standby_slots = _standby_channel_slots(
                    scheduler_state, int(queue.max_active_channels)
                )
                if standby_slots:
                    logger.info(
                        "Active channels have no queued work; admitting one standby channel "
                        "to use %s idle scheduler slots",
                        max(int(queue.max_workers) - occupied, 0),
                    )
                retry_tasks = await run_db(
                    list_retry_requests,
                    batch_limit,
                    participating,
                    max(
                        int(queue.max_active_channels) - len(participating),
                        standby_slots,
                    ),
                    participating_capacity,
                    channel_hot_target,
                )
            else:
                retry_tasks = await run_db(
                    list_retry_requests, limit=batch_limit
                )
            if not retry_tasks and standby_slots and free_capacity > 0:
                # Normal retry hydration respects next_retry_at.  When the
                # active set is full but has no queued work, release exactly
                # one oldest retry from a non-active channel as a transport
                # probe so an idle global slot can make progress. A failed
                # probe returns to its normal persistent backoff.
                probe = await run_db(
                    claim_transport_probe, sorted(participating)
                )
                if probe is not None:
                    retry_tasks = [probe]
                    logger.info(
                        "Released one persisted retry as a standby-channel "
                        "transport probe"
                    )
            transport_idle = (
                int(scheduler_state.get("queued", 0)) == 0
                and int(scheduler_state.get("in_flight", 0)) == 0
                and get_total_download_speed() == 0
            )
            if (
                not retry_tasks
                and transport_idle
                and now - last_probe_at >= probe_interval
            ):
                probe = await run_db(claim_transport_probe)
                if probe is not None:
                    speed_governor.arm_transport_probe(probe["chat_id"])
                    retry_tasks = [probe]
                    last_probe_at = now
                    logger.info(
                        "Released one persisted retry as a Telegram transport probe"
                    )
            elif cooling and retry_tasks:
                speed_governor.arm_transport_probe(retry_tasks[0]["chat_id"])
                last_probe_at = now

            grouped_tasks = {}
            for task in retry_tasks:
                chat_id = _parse_chat_id(task["chat_id"])
                chat_config = app.chat_download_config.get(chat_id)
                if chat_config is None:
                    chat_config = app.chat_download_config.get(str(chat_id))
                if chat_config is None:
                    channel = await run_db(get_channel_config, chat_id)
                    if channel and not bool(channel.get("enabled", 1)):
                        # A disabled/completed channel deliberately does not
                        # contact Telegram. Keep its task persisted so an
                        # explicit re-enable can hydrate it later.
                        continue
                    await run_db(
                        mark_retry_unavailable,
                        chat_id,
                        task["message_id"],
                        "频道已从频道库删除，无法重试",
                    )
                    continue

                group = grouped_tasks.setdefault(
                    str(chat_id),
                    {
                        "chat_id": chat_id,
                        "chat_config": chat_config,
                        "tasks": [],
                    },
                )
                group["tasks"].append(task)

            await _hydrate_retry_groups(client, grouped_tasks.values())
        except Exception as error:
            logger.exception(f"Retry request worker error: {error}")
        await _wait_for_retry_wakeup(2)


async def connection_watchdog(
    client: pyrogram.Client,
    interval: float = CONNECTION_CHECK_INTERVAL,
    timeout: float = CONNECTION_CHECK_TIMEOUT,
    failure_limit: int = CONNECTION_FAILURE_LIMIT,
):
    """Recycle the process after repeated Telegram connection failures.

    Pyrogram can retain a truthy ``is_connected`` flag after its underlying
    socket is gone.  A lightweight GetState probe catches that stale state.
    The process exits through the normal shutdown path; Docker's
    ``restart: unless-stopped`` policy then creates a completely fresh client,
    including all media sessions.
    """
    failures = 0
    await asyncio.sleep(max(float(interval), 0.0))
    while app.is_running and not app.restart_program:
        try:
            await telegram_call(
                client.invoke(pyrogram.raw.functions.updates.GetState()),
                max(float(timeout), 0.1),
                "connection GetState",
            )
            if failures:
                logger.info("Telegram connection probe recovered")
            failures = 0
        except pyrogram.errors.FloodWait as wait_error:
            rate_governor.penalize(wait_error.value)
            failures = 0
        except Exception as error:  # Network failures vary by transport.
            # During a known stall cooldown the account is being throttled, not
            # disconnected: GetState is slow for the same reason downloads are.
            # Recycling then is both harmful (wipes the governor sweet spot,
            # restarts in-flight files from zero) and pointless (a fresh socket
            # is throttled just the same). Defer any recycle until the account
            # is healthy again. This was the cause of 7 self-restarts in one
            # throttled day, each erasing the learned sweet spot.
            if speed_governor.cooldown_remaining() > 0:
                if failures:
                    failures = 0
                logger.info(
                    "连接探测超时，但处于限速熔断期，判为限速拖慢而非连接中断，不重启"
                )
                await asyncio.sleep(max(float(interval), 0.0))
                continue
            # If files are actively downloading, the account is demonstrably
            # reachable — the probe just lost a race. Recycling now would kill
            # every in-flight transfer for nothing. Only a probe failure while
            # downloads are ALSO dead counts toward a recycle. get_total_
            # download_speed() decays to 0 after 5s of no bytes, so a non-zero
            # reading means data arrived within the last few seconds.
            if get_total_download_speed() > 0:
                if failures:
                    failures = 0
                logger.info(
                    "连接探测超时，但仍有文件在实际下载，判为探测竞争而非连接中断，不重启"
                )
                await asyncio.sleep(max(float(interval), 0.0))
                continue
            failures += 1
            logger.warning(
                f"Telegram connection probe failed "
                f"({failures}/{max(int(failure_limit), 1)}): {error}"
            )
            if failures >= max(int(failure_limit), 1):
                logger.error(
                    "Telegram connection remained unavailable; "
                    "requesting a clean service recycle"
                )
                app.restart_program = True
                return
        await asyncio.sleep(max(float(interval), 0.0))


async def orphan_claim_reaper():
    """Return leaked 'downloading' claims to the retry queue while running.

    `recover_interrupted_tasks()` only runs at startup, so before this a worker
    that blocked forever held its task until the next restart. Ownership is
    tracked in `_active_claims`, so an in-flight transfer is never reclaimed no
    matter how long it legitimately takes.
    """
    await asyncio.sleep(ORPHAN_CLAIM_SCAN_INTERVAL)
    while app.is_running:
        try:
            released = await run_db(
                release_orphaned_claims,
                set(_active_claims),
                ORPHAN_CLAIM_MIN_AGE,
            )
            if released:
                logger.warning(
                    f"回收了 {released} 个无人认领的 downloading 任务"
                    f"（超过 {ORPHAN_CLAIM_MIN_AGE} 秒且无 worker 持有），已转入重试队列"
                )
                _retry_wakeup.set()
        except Exception as error:  # pylint: disable=broad-except
            logger.exception(f"Orphan claim reaper error: {error}")
        await asyncio.sleep(ORPHAN_CLAIM_SCAN_INTERVAL)


async def checkpoint_worker():
    """Persist last-read IDs regularly while the Web service stays alive."""
    while app.is_running:
        await asyncio.sleep(30)
        try:
            for chat_id, cursor in app.pending_channel_cursors():
                await run_db(update_channel_cursor, chat_id, cursor)
                app.mark_channel_cursor_checkpointed(chat_id, cursor)
            checkpoint = app.build_checkpoint_data()
            await run_blocking(app.write_checkpoint_data, checkpoint)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(f"Unable to checkpoint data.yaml: {error}")


async def channel_import_validation_worker(client: pyrogram.client.Client):
    """Validate pending import rows through the authenticated Telegram session."""
    await asyncio.sleep(2)
    while app.is_running:
        try:
            # Global pause = total silence; throttle cooldown = no imports.
            # Import validation calls get_chat, so both gates apply here.
            if (
                get_download_state() is DownloadState.StopDownload
                or speed_governor.cooldown_remaining() > 0
            ):
                await asyncio.sleep(10)
                continue
            items = await run_db(claim_import_items, limit=3)
            if not items:
                await asyncio.sleep(2)
                continue
            for item in items:
                try:
                    chat_id = app.runtime_chat_id(item["chat_id"])
                    chat = await telegram_call(
                        client.get_chat(chat_id),
                        METADATA_REQUEST_TIMEOUT,
                        "import get_chat",
                    )
                    title = (
                        getattr(chat, "title", None)
                        or getattr(chat, "first_name", None)
                        or getattr(chat, "username", None)
                        or str(item["chat_id"])
                    )
                    await run_db(
                        mark_import_item_validation, item["id"], True, str(title)
                    )
                except pyrogram.errors.FloodWait as wait_error:
                    rate_governor.penalize(wait_error.value)
                    await run_db(
                        mark_import_item_validation,
                        item["id"],
                        False,
                        error=f"Telegram 限流，{wait_error.value} 秒后重试",
                        retry_after=max(int(wait_error.value), 1),
                    )
                except (pyrogram.errors.BadRequest, pyrogram.errors.Forbidden) as error:
                    await run_db(
                        mark_import_item_validation,
                        item["id"], False, error=f"账号无法访问：{error}"
                    )
                except Exception as error:  # Network/session failures are retried briefly.
                    attempts = int(item.get("validation_attempts") or 0) + 1
                    await run_db(
                        mark_import_item_validation,
                        item["id"],
                        False,
                        error=f"校验失败：{error}",
                        retry_after=30 if attempts < 3 else 0,
                    )
        except Exception as error:
            logger.warning(f"Channel import validation worker error: {error}")
            await asyncio.sleep(3)


def _extract_flood_wait(error: BaseException):
    """Return a FloodWait hidden anywhere in an exception's cause chain.

    Metadata helpers occasionally wrap Telegram's 420 inside another
    exception; treating that as a generic failure mislabels a healthy
    channel as "needs attention" (observed on channels 21/22/27).
    """
    seen = set()
    stack = [error]
    while stack:
        current = stack.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, pyrogram.errors.FloodWait):
            return current
        stack.append(getattr(current, "__cause__", None))
        stack.append(getattr(current, "__context__", None))
    return None


def _is_transient_database_error(error: BaseException) -> bool:
    """Return whether a nested SQLite error only means the database is busy."""
    seen = set()
    stack = [error]
    markers = ("database is locked", "database table is locked", "database is busy")
    while stack:
        current = stack.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, sqlite3.OperationalError):
            message = str(current).lower()
            if any(marker in message for marker in markers):
                return True
        stack.append(getattr(current, "__cause__", None))
        stack.append(getattr(current, "__context__", None))
    return False


async def _persist_scan_backoff(chat_id, error: BaseException, retry_after: int) -> None:
    """Best-effort scan backoff persistence when SQLite itself is contended."""
    try:
        await run_db(
            update_channel_scan_state,
            chat_id,
            "backoff",
            f"数据库繁忙，{retry_after} 秒后自动重试：{error}",
            retry_after=retry_after,
        )
    except Exception as state_error:  # pylint: disable=broad-except
        if not _is_transient_database_error(state_error):
            raise
        logger.warning(
            f"Channel {chat_id} scan backoff state could not be persisted yet: "
            f"{state_error}"
        )


async def _run_channel_scan(
    client: pyrogram.Client,
    chat_id,
    chat_config: ChatDownloadConfig,
):
    """Run one channel scan with a cap applied only while it uses Telegram."""
    await _run_channel_scan_limited(client, chat_id, chat_config)


async def _run_channel_scan_limited(
    client: pyrogram.Client,
    chat_id,
    chat_config: ChatDownloadConfig,
):
    """Persist one bounded channel scan independently of YAML."""
    chat_config.node = TaskNode(chat_id=chat_id)
    chat_config.node.scheduler_priority = chat_config.priority
    try:
        while app.is_running:
            # A persisted global pause is also a metadata pause.  Keep this
            # coroutine alive so clicking "开始全部" resumes the same scan
            # without requiring a service restart or a config revision.
            while (
                app.is_running
                and get_download_state() is DownloadState.StopDownload
            ):
                await asyncio.sleep(1)
            if not app.is_running:
                return

            try:
                # Never retain a scarce scan permit during a 30-second retry
                # sleep or a FloodWait. Five backing-off channels previously
                # occupied all five permits and made newly imported channels
                # appear permanently stuck before scanning.
                async with _get_channel_scan_limiter():
                    await run_db(
                        update_channel_scan_state, chat_id, "scanning"
                    )
                    await download_chat_task(
                        client, chat_config, chat_config.node
                    )
                await run_db(update_channel_scan_state, chat_id, "completed")
                return
            except TelegramRequestTimeout as error:
                retry_after = 30
                await run_db(
                    update_channel_scan_state,
                    chat_id,
                    "backoff",
                    str(error),
                    retry_after=retry_after,
                )
                await asyncio.sleep(retry_after)
                continue
            except pyrogram.errors.FloodWait as wait_error:
                retry_after = max(int(wait_error.value), 1) + 3
                rate_governor.penalize(retry_after)
                await run_db(
                    update_channel_scan_state,
                    chat_id,
                    "backoff",
                    f"Telegram 限流，{retry_after} 秒后自动恢复",
                    retry_after=retry_after,
                )
                await rate_governor.wait_if_backing_off()
                continue
            except (pyrogram.errors.ChannelInvalid, pyrogram.errors.ChannelPrivate) as error:
                # Only a successful current scan may archive a channel.  A
                # permission/network error after an older successful scan does
                # not prove the newly imported work is complete.
                logger.warning(f"Download {chat_id} error: {error}")
                await run_db(
                    update_channel_scan_state, chat_id, "failed", str(error)
                )
                return
            except Exception as error:
                if _is_transient_database_error(error):
                    # SQLite contention is infrastructure backpressure, not a
                    # permanent Telegram/channel failure.  Returning here used
                    # to strand a newly imported channel forever because the
                    # registry only re-ran scans after a config revision.
                    retry_after = 5
                    logger.warning(
                        f"Download {chat_id} paused by database contention; "
                        f"retrying scan in {retry_after} seconds: {error}"
                    )
                    await _persist_scan_backoff(chat_id, error, retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                nested_flood = _extract_flood_wait(error)
                if nested_flood is not None:
                    # A wrapped 420 is still a rate limit, not a channel
                    # failure: back off and rescan instead of flagging it.
                    retry_after = max(int(nested_flood.value), 1) + 3
                    rate_governor.penalize(retry_after)
                    await run_db(
                        update_channel_scan_state,
                        chat_id,
                        "backoff",
                        f"Telegram 限流，{retry_after} 秒后自动恢复",
                        retry_after=retry_after,
                    )
                    await rate_governor.wait_if_backing_off()
                    continue
                logger.warning(f"Download {chat_id} error: {error}")
                await run_db(
                    update_channel_scan_state, chat_id, "failed", str(error)
                )
                return
    except asyncio.CancelledError:
        await run_db(update_channel_scan_state, chat_id, "idle")
        raise
    finally:
        chat_config.need_check = True


def _schedule_channel_scan(
    client: pyrogram.Client,
    chat_id,
    chat_config: ChatDownloadConfig,
) -> bool:
    """Start a scan unless the same channel already has one in flight."""
    current = _channel_scan_tasks.get(chat_id)
    if current is not None and not current.done():
        return False
    _channel_scan_tasks[chat_id] = app.loop.create_task(
        _run_channel_scan(client, chat_id, chat_config)
    )
    return True


async def channel_registry_worker(client: pyrogram.Client):
    """Discover channel-library changes without restarting the downloader."""
    await asyncio.sleep(2)
    try:
        while app.is_running:
            enabled_rows = await run_db(get_channel_configs, enabled_only=True)
            enabled_keys = set()
            for item in enabled_rows:
                chat_id = app.runtime_chat_id(item["chat_id"])
                enabled_keys.add(chat_id)
                revision = int(item.get("config_revision") or 1)
                previous_revision = _channel_registry_revisions.get(chat_id)
                if bool(item.get("paused")):
                    chat_config = app.chat_download_config.get(chat_id)
                    if chat_config is not None:
                        chat_config.node.is_stop_transmission = True
                    _channel_registry_revisions[chat_id] = revision
                    continue
                if chat_id not in app.chat_download_config:
                    chat_config = app.assign_channel_config(item)
                    if _schedule_channel_scan(client, chat_id, chat_config):
                        _channel_registry_revisions[chat_id] = revision
                elif previous_revision is not None and previous_revision != revision:
                    chat_config = app.assign_channel_config(item)
                    if _schedule_channel_scan(client, chat_id, chat_config):
                        _channel_registry_revisions[chat_id] = revision
                elif previous_revision is None:
                    _channel_registry_revisions[chat_id] = revision

            for chat_id, chat_config in list(app.chat_download_config.items()):
                if chat_id not in enabled_keys:
                    chat_config.node.is_stop_transmission = True
            await asyncio.sleep(3)
    finally:
        active = [task for task in _channel_scan_tasks.values() if not task.done()]
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)


async def worker(client: pyrogram.client.Client, worker_index: int = 0):
    """Work for download task"""
    global _last_successful_transfer_at  # pylint: disable=W0603
    while app.is_running:
        item = None
        message = None
        node = None
        claim_key = None
        try:
            # Adaptive concurrency: only the first N workers may take new
            # tasks; the gate also blocks pickup during a global stall
            # cooldown. In-flight transfers are never interrupted.
            await speed_governor.wait_for_turn(worker_index)
            item = await queue.get()
            message = item[0]
            node: TaskNode = item[1]

            if node.is_stop_transmission:
                await run_db(
                    defer_task,
                    node.chat_id,
                    message.id,
                    "频道已停用，保留任务等待重新启用",
                )
                continue

            # The stall breaker may have tripped while this worker was already
            # parked inside queue.get(); requeue instead of starting a
            # transfer the cooldown is meant to prevent.
            if speed_governor.cooldown_remaining() > 0 and worker_index != 0:
                await queue.put(item)
                await asyncio.sleep(2)
                continue

            if not await _wait_for_save_path():
                continue
            # Global pause: freeze here until resumed.
            while (
                app.is_running
                and get_download_state() is DownloadState.StopDownload
            ):
                await asyncio.sleep(1)
            if not app.is_running:
                continue
            # Channel pause: release the scheduler slot and requeue so other
            # channels keep flowing instead of this worker freezing in place.
            if is_channel_download_paused(node.chat_id):
                await queue.put(item)
                await asyncio.sleep(0.5)
                continue
            # Metadata scans and GetMessages calls share the account-level
            # governor, but an API FloodWait must not drain the file-transfer
            # pool.  download_media handles its own FloodWait below, so queued
            # files with a valid reference can continue using Telegram's CDN.
            # Register ownership BEFORE the claim lands in SQLite, so the
            # orphan reaper can never observe a 'downloading' row whose owner
            # has not been recorded yet.
            claim_key = _task_key(node.chat_id, message.id)
            _active_claims.add(claim_key)
            if await run_db(persist_start_task, node.chat_id, message.id) == 0:
                claim_state = await run_db(
                    task_claim_state, node.chat_id, message.id
                )
                if (
                    claim_state
                    and claim_state.get("status")
                    in {"queued", "retrying", "retry_requested"}
                    and bool(claim_state.get("enabled", 1))
                    and not bool(claim_state.get("paused", 0))
                ):
                    # A transient claim race/SQLite contention must not drop the
                    # only in-memory copy of an otherwise runnable task.
                    await queue.put(item)
                    await asyncio.sleep(0.1)
                continue
            if node.client:
                download_status, retry_result = await download_task(
                    node.client, message, node
                )
            else:
                download_status, retry_result = await download_task(
                    client, message, node
                )
            if download_status in {
                DownloadStatus.SuccessDownload,
                DownloadStatus.SkipDownload,
            }:
                _last_successful_transfer_at = _monotonic()
            _schedule_retry(message, node, retry_result)
        except Exception as error:
            logger.exception(f"{error}")
            if message is not None and node is not None:
                retry_result = await run_db(
                    fail_task, node.chat_id, message.id, str(error)
                )
                _schedule_retry(message, node, retry_result)
        finally:
            # Always drop ownership, including on CancelledError, so a shut
            # down or restarted worker cannot leave a phantom live claim that
            # makes the reaper skip a genuinely stranded row.
            if claim_key is not None:
                _active_claims.discard(claim_key)
            if item is not None:
                if message is not None and node is not None:
                    remove_download_result(node.chat_id, message.id)
                if node is not None:
                    await queue.task_done(node.chat_id)
                # Wake persisted retry hydration immediately when a real slot
                # becomes free instead of waiting for the next polling tick.
                _retry_wakeup.set()


async def supervised_worker(client, worker_index=0):
    """Restart failed workers; cancellation and shutdown never respawn work."""
    while app.is_running:
        try:
            await worker(client, worker_index)
            if app.is_running:
                logger.warning(f"下载任务 {worker_index} 提前退出，正在恢复。")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"下载任务 {worker_index} 异常退出，正在恢复。")
        if app.is_running:
            await asyncio.sleep(1)


async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    """Download all task"""
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=node.end_offset_id,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )

    chat_download_config.node = node

    if chat_download_config.ids_to_retry:
        logger.info(f"{_t('Downloading files failed during last run')}...")
        # channels.GetMessages is an account-level metadata call; pace it
        # through the governor so parallel scanners cannot burst into a 420.
        await rate_governor.acquire()
        skipped_messages: list = await telegram_call(  # type: ignore
            client.get_messages(
                chat_id=node.chat_id,
                message_ids=chat_download_config.ids_to_retry,
            ),
            METADATA_REQUEST_TIMEOUT,
            "scan retry GetMessages",
        )

        for message in skipped_messages:
            await add_download_task(message, node)

    async for message in messages_iter:  # type: ignore
        meta_data = MetaData()

        caption = message.caption
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(node.chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                node.chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(node.chat_id, message.media_group_id)
        set_meta_data(meta_data, message, caption)

        if app.need_skip_message(chat_download_config, message.id):
            continue

        if app.exec_filter(chat_download_config, meta_data):
            await add_download_task(message, node)
        else:
            node.download_status[message.id] = DownloadStatus.SkipDownload
            if message.media_group_id:
                await upload_telegram_chat(
                    client,
                    node.upload_user,
                    app,
                    node,
                    message,
                    DownloadStatus.SkipDownload,
                )

    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True


async def download_all_chat(client: pyrogram.Client):
    """Download All chat"""
    # Do not replay the legacy archive sweep at startup. It counts task and
    # history evidence for old channels and monopolizes the single ordered DB
    # executor on large libraries, delaying completed-file commits for minutes.
    # A channel is already retired atomically when its current scan completes.
    rows = await run_db(get_channel_configs, enabled_only=True)
    revisions = {
        app.runtime_chat_id(item["chat_id"]): int(item.get("config_revision") or 1)
        for item in rows
    }
    _channel_registry_revisions.update(revisions)
    runnable_keys = {
        app.runtime_chat_id(item["chat_id"])
        for item in rows
        if not bool(item.get("paused"))
    }
    # Paused or retired channels must not consume the bounded in-memory
    # active-channel set before the one explicitly resumed channel arrives.
    channels = [
        (chat_id, config)
        for chat_id, config in app.chat_download_config.items()
        if chat_id in runnable_keys
    ]
    if not channels:
        return

    scan_queue = asyncio.Queue()
    for channel in channels:
        scan_queue.put_nowait(channel)

    async def scan_worker():
        while not scan_queue.empty():
            try:
                key, value = scan_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await _run_channel_scan(client, key, value)
            finally:
                scan_queue.task_done()

    scan_worker_count = min(max(app.max_download_task, 1), CHANNEL_SCAN_CONCURRENCY, len(channels))
    await asyncio.gather(*(scan_worker() for _ in range(scan_worker_count)))


async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if app.restart_program:
            break
        if not app.keep_service_alive and not app.bot_token and finish:
            break

        await asyncio.sleep(1)


def _exec_loop():
    """Exec loop"""

    app.loop.run_until_complete(run_until_all_task_finish())


def _batch_numeric_chat_id(chat) -> str:
    """Convert a raw Telegram chat object to the app's numeric chat_id form."""
    from pyrogram import raw, utils  # pylint: disable=import-outside-toplevel

    if isinstance(chat, raw.types.Channel):
        return str(utils.get_channel_id(chat.id))
    if isinstance(chat, raw.types.Chat):
        return str(-chat.id)
    return ""


async def channel_batch_resolve_worker(client: pyrogram.Client):
    """Resolve dispatched invite links and import the ones already joined.

    Uses CheckChatInvite (preview only -- never joins). A link the user has
    joined resolves to a numeric chat_id and is added to the channel library
    so downloading starts; a link not yet joined rolls back to 'pending' so
    it reappears in the next batch plan. Best-effort and FloodWait-aware.
    """
    from pyrogram import raw  # pylint: disable=import-outside-toplevel

    await asyncio.sleep(8)
    while app.is_running:
        try:
            # Invite resolution is metadata traffic too (CheckChatInvite), so
            # it stops on global pause (total silence) and on stall cooldown.
            if (
                get_download_state() is DownloadState.StopDownload
                or speed_governor.cooldown_remaining() > 0
            ):
                await asyncio.sleep(10)
                continue
            items = await run_db(batch_queue.claim_queued, limit=3)
            if not items:
                await asyncio.sleep(3)
                continue
            for item in items:
                order_no = item["order_no"]
                invite = str(item["invite_link"])
                invite_hash = invite.rsplit("+", 1)[-1].rsplit("/", 1)[-1]
                await rate_governor.wait_if_backing_off()
                try:
                    result = await telegram_call(
                        client.invoke(
                            raw.functions.messages.CheckChatInvite(hash=invite_hash)
                        ),
                        METADATA_REQUEST_TIMEOUT,
                        "CheckChatInvite",
                    )
                except pyrogram.errors.FloodWait as wait_error:
                    rate_governor.penalize(wait_error.value)
                    await run_db(
                        batch_queue.mark_transient_error,
                        order_no,
                        error=f"Telegram 限流，{wait_error.value} 秒后重试",
                        retry_after=wait_error.value,
                    )
                    continue
                except Exception as error:  # pylint: disable=broad-except
                    if batch_queue.classify_resolution_error(error) == "permanent":
                        await run_db(
                            batch_queue.mark_result,
                            order_no,
                            "invalid",
                            error=f"邀请链接失效：{error}",
                        )
                    else:
                        retry = await run_db(
                            batch_queue.mark_transient_error,
                            order_no,
                            error=f"临时连接失败：{error}",
                        )
                        logger.warning(
                            f"批次频道 #{order_no} 验证失败；"
                            f"状态={retry['status']}，尝试={retry['attempts']}：{error}"
                        )
                    continue

                if isinstance(result, raw.types.ChatInvite):
                    # Preview only -> the account has NOT joined this one yet.
                    await run_db(
                        batch_queue.mark_result,
                        order_no, "not_joined", error="尚未加入，请先在 Telegram 手动加入"
                    )
                    continue

                chat = getattr(result, "chat", None)
                chat_id = _batch_numeric_chat_id(chat) if chat is not None else ""
                if not chat_id:
                    await run_db(
                        batch_queue.mark_transient_error,
                        order_no, error="暂时未能获取 Chat ID，请稍后重新验证"
                    )
                    continue
                title = str(getattr(chat, "title", "") or item.get("title") or "")
                try:
                    await run_db(
                        add_channel_to_library, chat_id, title, source="batch"
                    )
                    await run_db(
                        batch_queue.mark_result,
                        order_no,
                        "imported",
                        chat_id=chat_id,
                    )
                    logger.info(f"批量导入频道 {title} ({chat_id})")
                except Exception as error:  # pylint: disable=broad-except
                    await run_db(
                        batch_queue.mark_result,
                        order_no, "failed", chat_id=chat_id, error=f"导入失败：{error}"
                    )
                await asyncio.sleep(1)
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(f"channel_batch_resolve_worker error: {error}")
            await asyncio.sleep(3)


def _batch_is_drained(counts: dict, scanning: int) -> bool:
    """A batch is done only when no runnable or failed file remains."""
    return (
        int(counts.get("active", 0)) == 0
        and int(counts.get("failed", 0)) == 0
        and int(scanning) == 0
    )


async def batch_completion_notifier():
    """Push a WxPusher alert once a batch of channels finishes downloading.

    Detects the transition from "there is pending work" (active download
    tasks or a channel still scanning) to "nothing pending" and fires a
    single notification per batch. This matches the user's workflow of
    adding a batch of channels, letting it drain, then adding the next.
    Best-effort: any error here must never affect downloading.
    """
    await asyncio.sleep(20)  # let startup / recovery settle first
    had_work = False
    idle_confirmations = 0
    while app.is_running:
        try:
            if not notify.is_enabled():
                await asyncio.sleep(30)
                continue

            counts, scanning = await asyncio.gather(
                run_db(task_counts),
                run_db(scanning_channel_count),
            )
            pending = int(counts.get("active", 0)) + int(scanning)
            failed = int(counts.get("failed", 0))
            paused = get_download_state() is DownloadState.StopDownload

            if pending > 0 and not paused:
                had_work = True
                idle_confirmations = 0
            elif had_work and _batch_is_drained(counts, scanning) and not paused:
                # Require two consecutive idle reads (~1 min) so a brief gap
                # between two channels does not trigger a false "done".
                idle_confirmations += 1
                if idle_confirmations >= 2:
                    channels, total_files = await asyncio.gather(
                        run_db(channel_counts),
                        run_db(history_count),
                    )
                    completed = int(counts.get("completed", 0))
                    title = "📥 本批频道下载完成"
                    content = (
                        "本批频道已全部下载完成，可以添加下一批了。\n"
                        f"• 已完成文件：{completed}\n"
                        f"• 累计入库文件：{total_files}\n"
                        f"• 频道总数：{channels.get('total', 0)}\n"
                        f"• 失败文件：{failed}"
                    )
                    await run_blocking(notify.send, title, content)
                    had_work = False
                    idle_confirmations = 0
            elif failed > 0:
                # Failed files are unresolved work.  In particular, a retry
                # wave may briefly download a few files and then return the
                # rest to failure state.  That transition must never be
                # announced as "the whole batch completed".
                idle_confirmations = 0
        except Exception as error:  # pylint: disable=broad-except
            logger.warning(f"batch_completion_notifier error: {error}")
        await asyncio.sleep(30)


async def start_server(client: pyrogram.Client):
    """
    Start the server using the provided client.
    """
    await client.start()


async def stop_server(client: pyrogram.Client):
    """
    Stop the server using the provided client.
    """
    try:
        await asyncio.wait_for(client.stop(), timeout=15)
    except asyncio.TimeoutError:
        logger.warning("Timed out while stopping the stale Telegram session")
    except ConnectionError:
        # The watchdog may discover that Pyrogram already marked itself down.
        pass


def wait_for_initial_setup(application):
    """Wait in the console until credentials and optional SS runtime are usable."""
    reported = ""
    while application.is_running:
        if network_setup.credentials_ready(application.config):
            try:
                network_setup.ensure_started(application.config)
                return
            except (ValueError, OSError, subprocess.TimeoutExpired):
                message = "SS 组件未能启动。请检查节点设置或改用 SOCKS5；配置页仍可修改。"
        else:
            message = "首次运行：请打开 Web 控制台的配置页填写 Telegram API ID 和 API Hash，再按网页提示完成 Telegram 登录。"
        if message != reported:
            logger.warning(message)
            reported = message
        time.sleep(1)
        application.load_config()
    raise KeyboardInterrupt


def main():
    """Main function of the downloader."""
    tasks = []
    client = None
    try:
        app.pre_run()
        app.awaiting_setup = True
        init_web(app)
        wait_for_initial_setup(app)
        app.awaiting_setup = False
        rate_governor.configure(app.config.get("api_min_interval", 1.2))
        set_pause_release(app.config.get("pause_release_transfers", False))
        _wxpusher_cfg = app.config.get("wxpusher") or {}
        notify.configure(
            app_token=_wxpusher_cfg.get("app_token", ""),
            uids=_wxpusher_cfg.get("uids", []),
            enabled=bool(_wxpusher_cfg.get("enabled", False)),
        )
        # Load the batch-download CSV once (idempotent) if present.
        _batch_csv = app.config.get("batch_csv_path", "/app/dbdata/channels.csv")
        try:
            _batch_loaded = batch_queue.load_from_csv(_batch_csv)
            if _batch_loaded.get("loaded"):
                logger.info(
                    f"批量频道 CSV 已加载 {_batch_loaded['loaded']} 条，"
                    f"库内共 {_batch_loaded['total_in_db']} 条"
                )
        except Exception as _batch_error:  # pylint: disable=broad-except
            logger.warning(f"批量频道 CSV 加载失败：{_batch_error}")
        _adaptive_cfg = app.config.get("adaptive_speed") or {}
        _adaptive_enabled = bool(_adaptive_cfg.get("enabled", False))
        _worker_ceiling = max(int(app.max_download_task), 1)
        if _adaptive_enabled:
            _worker_ceiling = max(
                int(_adaptive_cfg.get("max_concurrency", 16) or 16),
                int(app.max_download_task),
            )
        speed_governor.configure(
            enabled=_adaptive_enabled,
            base=int(_adaptive_cfg.get("base_concurrency", app.max_download_task)),
            ceiling=_worker_ceiling,
            floor=int(_adaptive_cfg.get("min_concurrency", 6) or 6),
            adjust_interval=_adaptive_cfg.get("adjust_interval", 300),
            stall_threshold=_adaptive_cfg.get("stall_threshold", 8),
            stall_window=_adaptive_cfg.get("stall_window", 180),
            cooldown_seconds=_adaptive_cfg.get("cooldown_seconds", 900),
            hold_after_decrease=_adaptive_cfg.get("hold_after_decrease", 1800),
            lock_seconds=_adaptive_cfg.get("lock_seconds", 21600),
            improve_ratio=_adaptive_cfg.get("improve_ratio", 1.03),
        )
        queue.max_workers = _worker_ceiling
        queue.max_per_channel = max(
            min(
                int(app.config.get("max_downloads_per_channel", 4) or 4),
                queue.max_workers,
            ),
            1,
        )
        # 0 = 多频道并行(默认)；1 = 单频道串行，下完一个自动切下一个
        queue.max_active_channels = max(
            int(app.config.get("max_active_channels", 0) or 0), 0
        )
        app.download_scheduler = queue
        client = HookClient(
            "media_downloader",
            api_id=app.api_id,
            api_hash=app.api_hash,
            proxy=app.proxy,
            workdir=app.session_file_path,
            start_timeout=app.start_timeout,
            no_updates=True,
        )

        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)
        set_media_session_pool_size(
            client, app.config.get("media_session_pool_size", 4)
        )
        # Spread media sessions over every configured exit. With one exit this
        # is a no-op and behaviour is unchanged.
        _exits = set_media_proxy_pool(client, app.media_proxy_pool)
        if _exits > 1:
            logger.success(f"媒体连接将分散到 {_exits} 个代理出口")

        # One file is fetched as N concurrent chunk ranges. This shortens a
        # single file's wall time (Pyrogram is otherwise strictly sequential:
        # one 1 MiB chunk per round trip); it does not raise total throughput,
        # which saturates on the account side. Pyrogram's own get_file
        # semaphore must be able to hold workers * streams or the extra
        # streams just queue.
        _streams = set_download_streams_per_file(
            app.config.get("download_streams_per_file", 1)
        )
        if _streams > 1:
            need = int(app.max_download_task) * _streams
            if app.max_concurrent_transmissions < need:
                logger.warning(
                    f"download_streams_per_file={_streams} 需要 "
                    f"max_concurrent_transmissions >= {need}，当前 "
                    f"{app.max_concurrent_transmissions}，多余的流会被 Pyrogram 排队"
                )
            logger.success(f"单个文件将并行拉取 {_streams} 个数据块范围")

        from module.browser_login import BrowserLogin
        app.telegram_login = BrowserLogin(client)
        client.authorize = app.telegram_login.authorize
        app.telegram_ready = False
        app.loop.run_until_complete(start_server(client))
        from module.account_management import AccountLogout, account_identity
        app.telegram_account = account_identity(client.me)
        app.account_logout = AccountLogout(app, client, tasks)
        app.telegram_ready = True
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))

        tasks.append(app.loop.create_task(download_all_chat(client)))
        if app.keep_service_alive:
            recovered = recover_interrupted_tasks()
            if recovered:
                logger.info(f"Recovered {recovered} interrupted download tasks")
            tasks.append(app.loop.create_task(retry_request_worker(client)))
            tasks.append(app.loop.create_task(connection_watchdog(client)))
            tasks.append(app.loop.create_task(checkpoint_worker()))
            tasks.append(app.loop.create_task(orphan_claim_reaper()))
            tasks.append(app.loop.create_task(channel_import_validation_worker(client)))
            tasks.append(app.loop.create_task(channel_registry_worker(client)))
            tasks.append(app.loop.create_task(channel_batch_resolve_worker(client)))
            tasks.append(app.loop.create_task(batch_completion_notifier()))
        for _postprocess_index in range(POSTPROCESS_WORKERS):
            tasks.append(
                app.loop.create_task(postprocess_worker(_postprocess_index))
            )
        for _worker_index in range(_worker_ceiling):
            task = app.loop.create_task(supervised_worker(client, _worker_index))
            tasks.append(task)
        if _adaptive_enabled:
            tasks.append(
                app.loop.create_task(
                    speed_governor.controller(
                        get_total_download_speed,
                        # Download-path floods only: scan/metadata 420s must
                        # not step the transfer concurrency down.
                        speed_governor.download_flood_events,
                        lambda: queue.snapshot().get("in_flight", 0),
                        get_paused=lambda: get_download_state()
                        is DownloadState.StopDownload,
                    )
                )
            )

        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
        _exec_loop()
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except Exception as e:
        logger.exception("{}", e)
    finally:
        app.telegram_ready = False
        app.is_running = False
        if not _postprocess_queue.empty():
            try:
                app.loop.run_until_complete(
                    asyncio.wait_for(_postprocess_queue.join(), timeout=15)
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Shutdown left {_postprocess_queue.qsize()} completion "
                    "side effects pending; downloaded files and database "
                    "records are already durable"
                )
        if app.bot_token:
            app.loop.run_until_complete(stop_download_bot())
        if client is not None:
            app.loop.run_until_complete(stop_server(client))
        network_setup.stop()
        for task in tasks:
            task.cancel()
        if tasks:
            app.loop.run_until_complete(
                asyncio.gather(*tasks, return_exceptions=True)
            )
        logger.info(_t("Stopped!"))
        # check_for_updates(app.proxy)
        logger.info(f"{_t('update config')}......")
        app.update_config()
        logger.success(
            f"{_t('Updated last read message_id to config file')},"
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )


if __name__ == "__main__":
    if _check_config():
        main()
