"""Pyrogram ext"""

import asyncio
import html
import itertools
import math
import os
import secrets
import struct
import time
import types as python_types
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from functools import wraps
from io import BytesIO, StringIO
from mimetypes import MimeTypes
from typing import Callable, Iterable, List, Optional, Tuple, Union

import pyrogram
import pyrogram.client as pyrogram_client_module

from module import rate_governor
from module.async_utils import telegram_call
from loguru import logger
from pyrogram import enums, parser, types, utils
from pyrogram.client import Cache
from pyrogram.enums import MessageEntityType
from pyrogram.file_id import (
    FILE_REFERENCE_FLAG,
    PHOTO_TYPES,
    WEB_LOCATION_FLAG,
    FileType,
    b64_decode,
    rle_decode,
)
from pyrogram.mime_types import mime_types

from module.app import (
    Application,
    CloudDriveUploadStat,
    DownloadStatus,
    ForwardStatus,
    TaskNode,
    UploadProgressStat,
    UploadStatus,
)
from module.download_stat import get_download_result
from module.language import Language, _t
from module.send_media_group_v2 import cache_media, send_media_group_v2
from utils.format import (
    create_progress_bar,
    extract_info_from_link,
    format_byte,
    truncate_filename,
)
from utils.meta_data import MetaData

_mimetypes = MimeTypes()
_mimetypes.readfp(StringIO(mime_types))
_download_cache = Cache(1024 * 1024 * 1024)
_default_get_media_session = pyrogram_client_module.get_session
_default_handle_download = pyrogram.Client.handle_download
_MAX_MEDIA_BUILDERS_PER_DC = 4
_MEDIA_BUILD_TIMEOUT = 35
_MEDIA_POOL_WARM_TIMEOUT = 25.0
_MEDIA_RPC_TIMEOUT = 45.0
# How many times one GetFile chunk may be re-issued before the file gives up.
# Pyrogram's own per-chunk retry cannot be used here (see isolated_invoke), and
# without a replacement a single transient chunk timeout truncates a multi-GB
# file: get_file() swallows the error, returns a short file, and the whole
# transfer restarts from byte 0.  Worst-case wall time for one chunk is
# _MEDIA_RPC_TIMEOUT * attempts + sum(delays), which MUST stay below
# download_stall_timeout or the file watchdog cancels a healthy retry.
_MEDIA_INVOKE_ATTEMPTS = 3
_MEDIA_INVOKE_RETRY_DELAY = 0.5
# How many times one file may re-enter get_file() to escape a session that was
# quarantined mid-transfer. Each round resumes from the chunk boundary already
# on disk, so the cost is one round trip; the bound stops a genuinely
# unavailable file from spinning here instead of entering persisted backoff.
_RESUME_ROUNDS = 4
# The pool is the real parallelism limit: each session allows 2 concurrent RPCs,
# so pool N supports 2N in-flight chunk requests. A worker count above 2*pool
# just queues inside the same connections.
#
# 2026-08-13, owner decision: raised to 80 so a 150-worker / pool-75 config is
# expressible. This is against the measured evidence and is recorded as an
# explicit override, not a recommendation. The A/B that day found pool 12 beat
# pool 24 by 60 % on completed GB/hour (17.8 vs 11.1) with a quarter of the
# truncations, because connection-failure rate rises non-linearly with the
# number of TCP connections and one failure quarantines every transfer sharing
# that session (measured blast radius 10-18x). The 2026-07-21 incident — worker
# count 8 -> 16 -> 30 — cost a full day of account-level throttling.
# If throughput drops or FloodWaits appear, lower the pool first.
_MEDIA_POOL_HARD_CAP = 80
_DOWNLOAD_CHUNK_SIZE = 1024 * 1024
# Pyrogram fetches one file strictly sequentially: it awaits a 1 MiB chunk,
# yields it, then asks for the next. So a single file's speed is
# `chunk / round-trip`, independent of bandwidth. Measured 2026-08-15 with two
# concurrent files: 1.0-1.3 MB/s each, i.e. ~0.85 s per chunk.
#
# `get_file(file_id, size, limit, offset)` already takes a CHUNK RANGE
# (`limit` = chunk count, `offset` = first chunk), so N ranges can be fetched
# concurrently without touching the session pool, quarantine or retry paths.
#
# This does NOT raise total throughput — that saturates near 7 MB/s at about
# 15 concurrent chunk requests regardless of how they are grouped. It shortens
# the wall time of ONE file, which is what empties `.temp` of half-finished
# files (700 of them / 50 GB on 2026-08-15).
_STREAMS_PER_FILE = 1
# Chunks each stream fetches per window. A failed window is discarded back to
# the contiguous watermark, so this also bounds the re-download cost.
_STREAM_SEGMENT_CHUNKS = 4
_download_path_locks = {}
_download_path_locks_guard = None


def set_download_streams_per_file(streams: int) -> int:
    """Set how many chunk ranges of ONE file are fetched concurrently.

    1 keeps stock sequential behaviour. The caller must ensure
    ``max_concurrent_transmissions >= workers * streams`` or Pyrogram's own
    ``get_file_semaphore`` serialises the extra streams away.
    """
    global _STREAMS_PER_FILE  # pylint: disable=global-statement
    _STREAMS_PER_FILE = max(int(streams or 1), 1)
    return _STREAMS_PER_FILE


async def _fetch_one_segment(
    client,
    file_id,
    expected_size: int,
    fd: int,
    start_chunk: int,
    chunk_count: int,
    on_bytes,
):
    """Fetch a contiguous chunk range and write it at its absolute offset.

    Returns the number of bytes written. Writing with ``os.pwrite`` keeps the
    segments independent, so no segment has to wait for an earlier one.
    """
    written = 0
    offset = start_chunk * _DOWNLOAD_CHUNK_SIZE
    async for chunk in client.get_file(
        file_id, expected_size, chunk_count, start_chunk, None, ()
    ):
        if not chunk:
            break
        os.pwrite(fd, chunk, offset + written)
        written += len(chunk)
        await on_bytes(len(chunk))
        if written >= chunk_count * _DOWNLOAD_CHUNK_SIZE:
            break
    return written


def _path_lock_guard():
    """Create the loop-bound lock lazily after the downloader loop exists."""
    global _download_path_locks_guard  # pylint: disable=global-statement
    if _download_path_locks_guard is None:
        _download_path_locks_guard = asyncio.Lock()
    return _download_path_locks_guard


@asynccontextmanager
async def _exclusive_download_path(path: str):
    """Prevent a late cancelled transfer and its retry writing one temp file."""
    guard = _path_lock_guard()
    async with guard:
        state = _download_path_locks.get(path)
        if state is None:
            state = {"lock": asyncio.Lock(), "users": 0}
            _download_path_locks[path] = state
        state["users"] += 1
    try:
        async with state["lock"]:
            yield
    finally:
        async with guard:
            state["users"] -= 1
            if state["users"] == 0:
                _download_path_locks.pop(path, None)


async def _windowed_parallel_download(
    client,
    file_id,
    expected_size: int,
    file,
    resume_size: int,
    progress,
    progress_args,
):
    """Fetch one file as successive windows of concurrent chunk ranges.

    The invariant that makes this safe to resume is that ``watermark`` only
    advances when a whole window has landed, so the bytes on disk are always
    contiguous — exactly the guarantee the sequential path gives. A window that
    fails is truncated away, costing at most one window of re-download instead
    of the whole file.
    """
    fd = file.fileno()
    watermark = resume_size
    window_chunks = max(_STREAMS_PER_FILE * _STREAM_SEGMENT_CHUNKS, 1)
    stall_rounds = 0

    while watermark < expected_size:
        remaining = expected_size - watermark
        remaining_chunks = math.ceil(remaining / _DOWNLOAD_CHUNK_SIZE)
        this_window = min(window_chunks, remaining_chunks)
        first_chunk = watermark // _DOWNLOAD_CHUNK_SIZE

        # Spread the window over the streams; a short tail simply uses fewer.
        segments = []
        assigned = 0
        per_stream = math.ceil(this_window / _STREAMS_PER_FILE)
        while assigned < this_window:
            count = min(per_stream, this_window - assigned)
            segments.append((first_chunk + assigned, count))
            assigned += count

        window_bytes = 0
        window_lock = asyncio.Lock()

        async def on_bytes(count: int):
            nonlocal window_bytes
            async with window_lock:
                window_bytes += count
                current = min(watermark + window_bytes, expected_size)
            if progress:
                # Absolute progress keeps the stall watchdog and the UI honest;
                # the extra positional argument tells the statistics module how
                # much was already on disk before this attempt.
                await progress(
                    current, expected_size, *(tuple(progress_args or ()) + (resume_size,))
                )

        results = await asyncio.gather(
            *(
                _fetch_one_segment(
                    client, file_id, expected_size, fd, start, count, on_bytes
                )
                for start, count in segments
            ),
            return_exceptions=True,
        )

        failure = next((r for r in results if isinstance(r, BaseException)), None)
        fetched = sum(r for r in results if isinstance(r, int))
        expected_window = min(
            this_window * _DOWNLOAD_CHUNK_SIZE, expected_size - watermark
        )

        if failure is not None or fetched != expected_window:
            # Any hole makes everything past the watermark unusable on resume.
            os.ftruncate(fd, watermark)
            file.seek(watermark)
            if failure is not None:
                raise failure
            # A short window with no exception is get_file swallowing an error
            # again. Retry the same window a bounded number of times, then let
            # the persisted backoff take over.
            stall_rounds += 1
            if stall_rounds > _RESUME_ROUNDS:
                break
            continue

        stall_rounds = 0
        watermark += fetched

    os.fsync(fd)
    file.seek(watermark)
    return watermark


async def _resumable_handle_download(client, packet):
    """Pyrogram download handler that preserves verified full chunks.

    Stock Pyrogram opens ``.temp`` with ``wb`` and deletes it on any network
    error or cancellation.  With an unstable proxy this turns every retry into
    a complete re-download.  Resume only at Telegram's 1 MiB chunk boundary;
    an incomplete tail is truncated so bytes are never duplicated or skipped.
    """
    (
        file_id,
        directory,
        file_name,
        in_memory,
        file_size,
        progress,
        progress_args,
    ) = packet
    if in_memory:
        return await _default_handle_download(client, packet)

    directory = os.fspath(directory)
    os.makedirs(directory, exist_ok=True)
    target_path = os.path.abspath(
        os.path.join(directory, os.fspath(file_name)).replace("\\\\", "/")
    )
    temp_path = f"{target_path}.temp"
    expected_size = max(int(file_size or 0), 0)

    async with _exclusive_download_path(temp_path):
        existing_size = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
        if expected_size and existing_size > expected_size:
            existing_size = 0
        resume_size = existing_size - (existing_size % _DOWNLOAD_CHUNK_SIZE)

        mode = "r+b" if os.path.exists(temp_path) else "wb"
        file = open(temp_path, mode)  # pylint: disable=consider-using-with
        try:
            if existing_size != resume_size:
                file.truncate(resume_size)
            file.seek(resume_size)

            # Client.get_file() SWALLOWS its errors (`except Exception:
            # log.exception(e)`) and simply stops yielding, so a broken stream
            # arrives here as a short file rather than an exception. The most
            # common cause in production is a media session quarantined by a
            # DIFFERENT file's transport error: every transfer sharing that
            # connection then dies at its next chunk. Measured 2026-08-11:
            # 164 real connection failures killed 2350 in-flight files (14x).
            #
            # Re-entering get_file() acquires a fresh pooled session (via
            # _get_pooled_media_session, which skips quarantined ones) and
            # resumes from the chunk boundary already on disk, so the blast
            # radius costs one round trip instead of the whole file.
            # Parallel path: only when a stream count is configured AND the
            # size is known (ranges cannot be split without a total). Every
            # other case falls through to the stock sequential loop below.
            if _STREAMS_PER_FILE > 1 and expected_size:
                actual_size = await _windowed_parallel_download(
                    client,
                    file_id,
                    expected_size,
                    file,
                    file.tell(),
                    progress,
                    progress_args,
                )
                file.close()
                if actual_size != expected_size:
                    raise IOError(
                        f"downloaded {actual_size} bytes, "
                        f"expected {expected_size} bytes"
                    )
                os.replace(temp_path, target_path)
                return target_path

            retries = 0
            while not expected_size or file.tell() < expected_size:
                position = file.tell()
                # get_file's offset is in whole chunks, so an unaligned tail
                # would silently skip or duplicate bytes. Full chunks are
                # 1 MiB, so this only trims a short final write.
                aligned = position - (position % _DOWNLOAD_CHUNK_SIZE)
                if aligned != position:
                    file.truncate(aligned)
                    file.seek(aligned)
                    position = aligned
                # Keep UI progress absolute, but tell the statistics callback
                # how many bytes were already on disk before this attempt.
                # This prevents a large resumed file from appearing as a
                # multi-GB/s download on its first progress callback.
                resumed_progress_args = tuple(progress_args or ()) + (position,)
                async for chunk in client.get_file(
                    file_id,
                    expected_size,
                    0,
                    position // _DOWNLOAD_CHUNK_SIZE,
                    progress,
                    resumed_progress_args,
                ):
                    file.write(chunk)
                file.flush()
                if not expected_size or file.tell() >= expected_size:
                    break
                # A round that produced nothing is not a quarantine casualty —
                # the file itself is unavailable. Hand it back to the persisted
                # retry so its backoff applies instead of spinning here.
                if file.tell() <= position:
                    break
                # retries counts RE-ENTRIES only, so the budget is
                # 1 initial call + _RESUME_ROUNDS retries.
                if retries >= _RESUME_ROUNDS:
                    break
                retries += 1
            file.flush()
            actual_size = file.tell()
        except BaseException:
            # Cancellation, FloodWait and transport errors all preserve the
            # completed chunks.  The outer state machine decides the backoff.
            file.close()
            raise
        else:
            file.close()

        if expected_size and actual_size != expected_size:
            raise IOError(
                f"downloaded {actual_size} bytes, expected {expected_size} bytes"
            )

        os.replace(temp_path, target_path)
        return target_path


def reset_download_cache():
    """Reset download cache"""
    _download_cache.store.clear()


def _guess_mime_type(filename: str) -> Optional[str]:
    """Guess mime type"""
    return _mimetypes.guess_type(filename)[0]


def _guess_extension(mime_type: str) -> Optional[str]:
    """Guess extension"""
    return _mimetypes.guess_extension(mime_type)


def get_utf16_length(text: str) -> int:
    """
    Returns the length of UTF-16 units for the string text.

    Notes:
      - Using 'utf-16-le' encoding (without BOM), dividing the number of bytes by 2 gives the number of UTF-16 units in the string.
      - This correctly counts both regular characters (1 unit) and emoji characters outside the BMP (2 units).
    """
    # After encoding to utf-16-le, every 2 bytes represent 1 UTF-16 unit
    return len(text.encode("utf-16-le")) // 2


def get_media_obj(
    message: pyrogram.types.Message,
    media: str = None,
    caption: str = None,
    caption_entities: List[pyrogram.types.MessageEntity] = None,
    parse_mode: Optional[enums.ParseMode] = None,
) -> Union[
    types.InputMediaPhoto,
    types.InputMediaVideo,
    types.InputMediaAudio,
    types.InputMediaDocument,
    types.InputMediaAnimation,
]:
    """Get media object"""
    media_type = message.media
    if media_type == pyrogram.enums.MessageMediaType.PHOTO:
        return types.InputMediaPhoto(
            media,
            caption=caption,
            caption_entities=caption_entities,
            parse_mode=parse_mode,
        )

    if media_type == pyrogram.enums.MessageMediaType.VIDEO:
        return types.InputMediaVideo(
            media,
            caption=caption,
            caption_entities=caption_entities,
            width=message.video.width,
            height=message.video.height,
            duration=message.video.duration,
            parse_mode=parse_mode,
        )

    if media_type in [
        pyrogram.enums.MessageMediaType.AUDIO,
        pyrogram.enums.MessageMediaType.VOICE,
    ]:
        return types.InputMediaAudio(
            media,
            caption=caption,
            caption_entities=caption_entities,
            parse_mode=parse_mode,
        )

    if media_type == pyrogram.enums.MessageMediaType.DOCUMENT:
        return types.InputMediaDocument(
            media,
            caption=caption,
            caption_entities=caption_entities,
            parse_mode=parse_mode,
        )

    if media_type == pyrogram.enums.MessageMediaType.ANIMATION:
        return types.InputMediaAnimation(
            media,
            caption=caption,
            caption_entities=caption_entities,
            parse_mode=parse_mode,
        )

    return None


def _get_file_type(file_id: str):
    """Get file type"""
    decoded = rle_decode(b64_decode(file_id))

    # File id versioning. Major versions lower than 4 don't have a minor version
    major = decoded[-1]

    if major < 4:
        buffer = BytesIO(decoded[:-1])
    else:
        buffer = BytesIO(decoded[:-2])

    file_type, _ = struct.unpack("<ii", buffer.read(8))

    file_type &= ~WEB_LOCATION_FLAG
    file_type &= ~FILE_REFERENCE_FLAG

    try:
        file_type = FileType(file_type)
    except ValueError as exc:
        raise ValueError(f"Unknown file_type {file_type} of file_id {file_id}") from exc

    return file_type


def get_extension(file_id: str, mime_type: str, dot: bool = True) -> str:
    """Get extension"""

    if not file_id:
        if dot:
            return ".unknown"
        return "unknown"

    file_type = _get_file_type(file_id)

    guessed_extension = _guess_extension(mime_type)

    if file_type in PHOTO_TYPES:
        extension = "jpg"
    elif file_type == FileType.VOICE:
        extension = guessed_extension or "ogg"
    elif file_type in (FileType.VIDEO, FileType.ANIMATION, FileType.VIDEO_NOTE):
        extension = guessed_extension or "mp4"
    elif file_type == FileType.DOCUMENT:
        extension = guessed_extension or "zip"
    elif file_type == FileType.STICKER:
        extension = guessed_extension or "webp"
    elif file_type == FileType.AUDIO:
        extension = guessed_extension or "mp3"
    else:
        extension = "unknown"

    if dot:
        extension = "." + extension
    return extension


async def send_message_by_language(
    client: pyrogram.client.Client,
    language: Language,
    chat_id: Union[int, str],
    reply_to_message_id: int,
    language_str: List[str],
):
    """Record download status"""
    msg = language_str[language.value - 1]

    return await client.send_message(
        chat_id, msg, reply_to_message_id=reply_to_message_id
    )


async def download_thumbnail(
    client: pyrogram.Client,
    temp_path: str,
    message: pyrogram.types.Message,
):
    """Downloads the thumbnail of a video message to a temporary file.

    Args:
        client: A Pyrogram client instance.
        temp_path: The path to a temporary directory where the thumbnail file
                   will be stored.
        message: A Pyrogram Message object representing the video message.

    Returns:
        A string representing the path of the thumbnail file, or None if the
        download failed.

    Raises:
        ValueError: If the downloaded thumbnail file size doesn't match the
                    expected file size.
    """
    thumbnail_file = None
    if message.video.thumbs:
        message = await fetch_message(client, message)
        thumbnail = message.video.thumbs[0] if message.video.thumbs else None
        unique_name = os.path.join(
            temp_path,
            "thumbnail",
            f"thumb-{int(time.time())}-{secrets.token_hex(8)}.jpg",
        )

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                thumbnail_file = await client.download_media(
                    thumbnail, file_name=unique_name
                )

                if os.path.getsize(thumbnail_file) == thumbnail.file_size:
                    break

                raise ValueError(
                    f"Thumbnail file size is {os.path.getsize(thumbnail_file)}"
                    f" bytes, actual {thumbnail.file_size}: {thumbnail_file}"
                )

            except Exception as e:
                if attempt == max_attempts:
                    logger.exception(
                        f"Failed to download thumbnail after {max_attempts}"
                        f" attempts: {e}"
                    )
                else:
                    message = await fetch_message(client, message)
                    logger.warning(
                        f"Attempt {attempt} to download thumbnail failed: {e}"
                    )
                    # Wait 2 seconds before retrying
                    await asyncio.sleep(2)

                thumbnail = None
                thumbnail_file = None
    return thumbnail_file


async def upload_telegram_chat(
    client: pyrogram.Client,
    upload_user: pyrogram.Client,
    app: Application,
    node: TaskNode,
    message: pyrogram.types.Message,
    download_status: DownloadStatus,
    file_name: str = None,
):
    """Upload telegram chat"""
    # upload telegram
    if node.upload_telegram_chat_id:
        if download_status in {
            DownloadStatus.SkipDownload,
            DownloadStatus.FailedDownload,
        } and message.media:
            if message.media_group_id:
                await proc_cache_forward(client, node, message, True, app)
            return

        if download_status is DownloadStatus.SuccessDownload or (
            download_status is DownloadStatus.SkipDownload and not message.media
        ):
            try:
                await upload_telegram_chat_message(
                    client,
                    upload_user,
                    app,
                    node,
                    message,
                    file_name,
                )
            except Exception as e:
                logger.exception(f"Upload file {file_name} error: {e}")
            finally:
                if file_name and app.after_upload_telegram_delete:
                    os.remove(file_name)

            # forward text
            # FIXME: fix upload text
            # if (
            #     download_status is DownloadStatus.SkipDownload
            #     and message.text
            #     and bot
            # ):
            #     await upload_telegram_chat(
            #         client, app, node.upload_telegram_chat_id, message, file_name
            #     )


async def upload_telegram_chat_message(
    client: pyrogram.Client,
    upload_user: pyrogram.Client,
    app: Application,
    node: TaskNode,
    message: pyrogram.types.Message,
    file_name: str = None,
) -> ForwardStatus:
    """See upload telegram_chat"""
    forward_status = ForwardStatus.FailedForward
    max_attempts = 3
    for _ in range(1, max_attempts + 1):
        try:
            forward_status = await _upload_telegram_chat_message(
                client, upload_user, app, node, message, file_name
            )
            break
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            rate_governor.penalize(wait_err.value)
            await asyncio.sleep(wait_err.value * 2)
            logger.warning(
                "Upload Message[{}]: FlowWait {}", message.id, wait_err.value
            )
        except Exception as e:
            logger.exception(f"Upload file {file_name} error: {e}")
            return ForwardStatus.FailedForward

    if forward_status != ForwardStatus.CacheForward:
        node.stat_forward(forward_status)
    return forward_status


# pylint: disable=R0912
async def _upload_signal_message(
    client: pyrogram.Client,
    upload_user: pyrogram.Client,
    app: Application,
    node: TaskNode,
    upload_telegram_chat_id: Union[int, str, None],
    message: pyrogram.types.Message,
    file_name: Optional[str],
    caption: Optional[str] = None,
    text: Optional[str] = None,
):
    """
    Uploads a video or message to a Telegram chat.

    Parameters:
        client (pyrogram.Client): The pyrogram client.
        upload_telegram_chat_id (Union[int, str]): The ID of the chat to upload to.
        message (pyrogram.types.Message): The message to upload.
        file_name (str): The name of the file to upload.
    """
    ui_file_name = file_name
    if file_name:
        ui_file_name = (
            f"****{os.path.splitext(file_name)[-1]}"
            if app.hide_file_name
            else file_name
        )

    if message.video:
        # Download thumbnail
        thumbnail_file = await download_thumbnail(client, app.temp_save_path, message)
        try:
            # TODO(tangyoha): add more log when upload video more than 2000MB failed
            # Send video to the destination chat
            if node.reply_to_message:
                await node.reply_to_message.reply_video(
                    file_name,
                    caption=caption,
                    message_thread_id=node.topic_id,
                    thumb=thumbnail_file,
                    width=message.video.width,
                    height=message.video.height,
                    duration=message.video.duration,
                    parse_mode=pyrogram.enums.ParseMode.HTML,
                )
            else:
                await upload_user.send_video(
                    upload_telegram_chat_id,
                    file_name,
                    thumb=thumbnail_file,
                    width=message.video.width,
                    height=message.video.height,
                    duration=message.video.duration,
                    caption=caption,
                    parse_mode=pyrogram.enums.ParseMode.HTML,
                    progress=update_upload_stat,
                    progress_args=(
                        message.id,
                        ui_file_name,
                        time.time(),
                        node,
                        upload_user,
                    ),
                    message_thread_id=node.topic_id,
                )
        except Exception as e:
            raise e
        finally:
            if thumbnail_file:
                os.remove(str(thumbnail_file))

    elif message.photo:
        if node.reply_to_message:
            await node.reply_to_message.reply_photo(
                file_name,
                caption=caption,
                message_thread_id=node.topic_id,
            )
        else:
            await upload_user.send_photo(
                upload_telegram_chat_id,
                file_name,
                caption=caption,
                progress=update_upload_stat,
                progress_args=(
                    message.id,
                    ui_file_name,
                    time.time(),
                    node,
                    upload_user,
                ),
                message_thread_id=node.topic_id,
            )
    elif message.document:
        if node.reply_to_message:
            await node.reply_to_message.reply_document(
                file_name,
                caption=caption,
                message_thread_id=node.topic_id,
            )
        else:
            await upload_user.send_document(
                upload_telegram_chat_id,
                file_name,
                caption=caption,
                progress=update_upload_stat,
                progress_args=(
                    message.id,
                    ui_file_name,
                    time.time(),
                    node,
                    upload_user,
                ),
                message_thread_id=node.topic_id,
            )
    elif message.voice:
        if node.reply_to_message:
            await node.reply_to_message.reply_voice(
                file_name,
                caption=caption,
                message_thread_id=node.topic_id,
            )
        else:
            await upload_user.send_voice(
                upload_telegram_chat_id,
                file_name,
                caption=caption,
                progress=update_upload_stat,
                progress_args=(
                    message.id,
                    ui_file_name,
                    time.time(),
                    node,
                    upload_user,
                ),
                message_thread_id=node.topic_id,
            )
    elif message.video_note:
        if node.reply_to_message:
            await node.reply_to_message.reply_video_note(
                file_name,
                caption=caption,
                message_thread_id=node.topic_id,
            )
        else:
            await upload_user.send_video_note(
                upload_telegram_chat_id,
                file_name,
                caption=caption,
                progress=update_upload_stat,
                progress_args=(
                    message.id,
                    ui_file_name,
                    time.time(),
                    node,
                    upload_user,
                ),
                message_thread_id=node.topic_id,
            )
    elif message.text:
        if node.reply_to_message:
            await node.reply_to_message.reply(
                message.text if text is None else text, message_thread_id=node.topic_id
            )
        else:
            await upload_user.send_message(
                upload_telegram_chat_id,
                message.text if text is None else text,
                message_thread_id=node.topic_id,
            )


def truncate_caption(
    text: str,
    entities: Optional[List[pyrogram.raw.base.MessageEntity]] = None,
    limit: int = 1024,
) -> Tuple[str, Optional[List[pyrogram.types.MessageEntity]]]:
    """
    Truncate caption to ensure it doesn't exceed Telegram limits

    Args:
        text: Original text
        entities: List of text entities
        limit: UTF-16 encoding unit limit (default 1024)

    Returns:
        Tuple[str, Optional[List[pyrogram.raw.types.MessageEntity]]]: Truncated text and corresponding entity list
    """
    if not text:
        return text, entities

    # Calculate UTF-16 length
    utf16_length = get_utf16_length(text)

    if utf16_length <= limit:
        return text, entities

    # If exceeds limit, need to truncate
    # Use binary search to find suitable truncation position
    left, right = 0, len(text)
    while left < right:
        mid = (left + right + 1) // 2
        if get_utf16_length(text[:mid]) <= limit:
            left = mid
        else:
            right = mid - 1

    truncated_text = text[:left]

    # If there are entities, need to adjust entity list
    if entities:
        truncated_entities = []
        for entity in entities:
            if entity.offset >= left:
                continue
            if entity.offset + entity.length <= left:
                truncated_entities.append(entity)
            else:
                # For entities that cross the truncation point, adjust length
                new_entity = deepcopy(entity)
                new_entity.length = left - entity.offset
                truncated_entities.append(new_entity)
        return truncated_text, truncated_entities

    return truncated_text, None


async def process_caption(
    client,
    app,
    upload_telegram_chat_id,
    caption: str,
    caption_entities: Optional[List[pyrogram.types.MessageEntity]],
):
    """
    Process message caption: Use plain text without formatting for ad filtering and synchronously update caption_entities.
    After removing matched ad text, remove or adjust corresponding MessageEntity objects.

    Args:
        client: Pyrogram client instance
        app: Application object containing replace_advertisement_list property
            caption: Original caption text
            caption_entities: List of MessageEntity objects

        Returns:
        str: Cleaned caption
    """
    if not caption:
        return None

    update_caption = caption
    if caption and caption_entities:
        update_caption = pyrogram.parser.Parser.unparse(caption, caption_entities, True)

    for ad_text in app.replace_advertisement_list:
        update_caption = update_caption.replace(ad_text, "")

    advertisement = app.group_add_advertisement.get(upload_telegram_chat_id, "")

    ad_length = get_utf16_length(f"\n{advertisement}" if advertisement else "")

    max_caption_length = 4096 if client.me and client.me.is_premium else 1024
    available_length = max_caption_length - ad_length

    try:
        new_caption, new_entities = await convect_caption_entities(
            client, update_caption
        )
    except Exception as e:
        logger.exception(f"Error parsing caption: {e}")
        new_caption = update_caption
        new_entities = None

    truncated_caption, truncated_entities = truncate_caption(
        new_caption, new_entities, available_length
    )

    if advertisement:
        truncated_caption += f"\n{advertisement}"

    try:
        if truncated_entities:
            truncated_entities = convert_entities(truncated_entities)
            return pyrogram.parser.Parser.unparse(
                truncated_caption, truncated_entities, True
            )
    except Exception as e:
        logger.exception(f"Error unparsing caption: {e}")
        return truncated_caption

    return truncated_caption


def convert_message_entity(client, entity: "pyrogram.raw.base.MessageEntity") -> Optional["pyrogram.types.MessageEntity"]:
    # Special case for InputMessageEntityMentionName -> MessageEntityType.TEXT_MENTION
    # This happens in case of UpdateShortSentMessage inside send_message() where entities are parsed from the input
    if isinstance(entity, pyrogram.raw.types.InputMessageEntityMentionName):
        entity_type = enums.MessageEntityType.TEXT_MENTION
        user_id = entity.user_id.user_id
    else:
        entity_type = enums.MessageEntityType(entity.__class__)
        user_id = getattr(entity, "user_id", None)

    return pyrogram.types.MessageEntity(
        type=entity_type,
        offset=entity.offset,
        length=entity.length,
        url=getattr(entity, "url", None),
        user=types.User(id=user_id),
        language=getattr(entity, "language", None),
        custom_emoji_id=getattr(entity, "document_id", None),
        expandable=getattr(entity, "collapsed", None),
        client=client
    )

def convert_entities(
    entities: List[pyrogram.raw.base.MessageEntity],
) -> List[pyrogram.types.MessageEntity]:
    """Convert raw message entities to types message entities"""
    if not entities:
        return []

    try:
        return [
            convert_message_entity(None, entity) for entity in entities
        ]
    except Exception as e:
        logger.warning(f"Failed to convert entities: {e}")
        return []


async def convect_caption_entities(client, text):
    # Convert back to entities format
    try:
        return (await client.parser.parse(text, None)).values()
    except Exception as e:
        print(f"Error parsing markdown: {e}")
        # If parsing fails, return cleaned text without entities
        return text, None


async def _upload_telegram_chat_message(
    client: pyrogram.Client,
    upload_user: pyrogram.Client,
    app: Application,
    node: TaskNode,
    message: pyrogram.types.Message,
    file_name: str = None,
):
    """
    Uploads a Telegram chat message to the destination chat.

    Args:
        client (pyrogram.Client): The client used to interact with the Telegram API.
        upload_user (pyrogram.Client): The client used to upload the message.
        app (Application): The application instance.
        node (TaskNode): The task node associated with the message.
        message (pyrogram.types.Message): The Telegram chat message to be uploaded.
        file_name (str): The name of the file to be uploaded.

    Returns:
        None
    """
    await app.forward_limit_call.wait(node)

    caption = await process_caption(
        client,
        app,
        node.upload_telegram_chat_id,
        message.caption,
        message.caption_entities,
    )

    new_text = None
    # proc only text
    if not message.media and message.text:
        new_text = await process_caption(
            client, app, node.upload_telegram_chat_id, message.text, message.entities
        )

    if message.caption and message.media_group_id:
        app.set_caption_name(node.chat_id, message.media_group_id, message.caption)
        app.set_caption_entities(
            node.chat_id, message.media_group_id, message.caption_entities
        )

    if not message.media_group_id:
        if not node.has_protected_content:
            if node.reply_to_message:
                if message.text:
                    await node.reply_to_message.reply(
                        message.text,
                        message_thread_id=node.topic_id,
                    )
                elif message.photo:
                    await node.reply_to_message.reply_photo(
                        message.photo.file_id,
                        caption=caption,
                        message_thread_id=node.topic_id,
                    )
                elif message.video:
                    await node.reply_to_message.reply_video(
                        message.video.file_id,
                        caption=caption,
                        message_thread_id=node.topic_id,
                    )
                elif message.document:
                    await node.reply_to_message.reply_document(
                        message.document.file_id,
                        caption=caption,
                        message_thread_id=node.topic_id,
                    )
                elif message.audio:
                    await node.reply_to_message.reply_audio(
                        message.audio.file_id,
                        caption=caption,
                        message_thread_id=node.topic_id,
                    )
            else:
                if new_text:
                    await client.send_message(
                        node.upload_telegram_chat_id,
                        new_text,
                        parse_mode=enums.ParseMode.HTML,
                    )
                else:
                    await message.copy(
                        node.upload_telegram_chat_id,
                        caption=caption,
                        parse_mode=enums.ParseMode.HTML,
                    )
        else:
            await _upload_signal_message(
                client,
                upload_user,
                app,
                node,
                node.upload_telegram_chat_id,
                message,
                file_name,
                caption,
                new_text,
            )
        return ForwardStatus.SuccessForward

    return await forward_multi_media(
        client, upload_user, app, node, message, caption, file_name
    )


# pylint: disable=R0912
async def forward_multi_media(
    client: pyrogram.Client,
    _: pyrogram.Client,
    app: Application,
    node: TaskNode,
    message: pyrogram.types.Message,
    caption: Optional[str] = None,
    file_name: Optional[str] = None,
):
    """Forward multi media by cache"""
    media_obj = get_media_obj(
        message, file_name, caption
    )  # , parse_mode=enums.ParseMode.HTML)
    if not node.has_protected_content:
        media = getattr(message, message.media.value)
        if not media:
            return ForwardStatus.SkipForward
        media_obj.media = media.file_id if media else ""

    need_upload = False
    initialize_group = False
    init_event = None
    async with node.media_group_ids_lock:
        if message.media_group_id not in node.media_group_ids:
            node.media_group_ids[message.media_group_id] = None
            init_event = asyncio.Event()
            node.media_group_init_events[message.media_group_id] = init_event
            initialize_group = True
        elif node.media_group_ids[message.media_group_id] is None:
            init_event = node.media_group_init_events.get(message.media_group_id)

    if initialize_group:
        try:
            media_group = await get_media_group_with_retry(
                client, node.chat_id, message.id, 5
            )
        except BaseException:
            async with node.media_group_ids_lock:
                node.media_group_ids.pop(message.media_group_id, None)
                node.media_group_init_events.pop(message.media_group_id, None)
                init_event.set()
            raise
        async with node.media_group_ids_lock:
            if media_group:
                node.media_group_ids[message.media_group_id] = {
                    item.id: None for item in media_group
                }
                for item in media_group:
                    node.upload_status[item.id] = None
            else:
                node.media_group_ids.pop(message.media_group_id, None)
            node.media_group_init_events.pop(message.media_group_id, None)
            init_event.set()
        if not media_group:
            logger.error("Get Media Group Error! message id: {}", message.id)
            return ForwardStatus.FailedForward
    elif init_event is not None:
        try:
            await asyncio.wait_for(init_event.wait(), timeout=50)
        except asyncio.TimeoutError:
            logger.error("Get Media Group timed out! message id: {}", message.id)
            return ForwardStatus.FailedForward

    async with node.media_group_ids_lock:
        media_group = node.media_group_ids.get(message.media_group_id)
        if not media_group or message.id not in media_group:
            return ForwardStatus.FailedForward
        if not media_group[message.id]:
            node.upload_status[message.id] = UploadStatus.Uploading
            need_upload = True

    _media = None
    if need_upload:
        try:
            ui_file_name = file_name
            if file_name:
                ui_file_name = (
                    f"****{os.path.splitext(file_name)[-1]}"
                    if app.hide_file_name
                    else file_name
                )
                media_obj.thumb = (
                    await download_thumbnail(client, app.temp_save_path, message)
                    if message.video
                    else None
                )

            _media = await cache_media(
                client,
                node.upload_telegram_chat_id,  # type: ignore
                media_obj,
                progress=update_upload_stat,
                progress_args=(
                    message.id,
                    ui_file_name,
                    time.time(),
                    node,
                    client,
                ),
            )
        except Exception as e:
            logger.exception(f"{e}")
        finally:
            if file_name and message.video and media_obj.thumb:
                os.remove(str(media_obj.thumb))

        async with node.media_group_ids_lock:
            if not _media:
                node.upload_status[message.id] = UploadStatus.FailedUpload
                return ForwardStatus.FailedForward

            node.media_group_ids[message.media_group_id][message.id] = _media
            node.upload_status[message.id] = UploadStatus.SuccessUpload

    return await proc_cache_forward(client, node, message, bool(file_name), app)


async def proc_cache_forward(
    client: pyrogram.Client,
    node: TaskNode,
    message: pyrogram.types.Message,
    check_download_status: bool,
    app: Application,
):
    """Process other cache forward"""
    multi_media: List[pyrogram.raw.types.InputSingleMedia] = []

    async with node.media_group_ids_lock:
        # Check if the message's media group is valid
        media_group = node.media_group_ids.get(message.media_group_id)
        if not media_group:
            return

        # Check if all items are in a valid state for forwarding
        for key, media_item in media_group.items():
            download_status = node.download_status.get(key, DownloadStatus.Downloading)
            upload_status = node.upload_status.get(key, UploadStatus.Uploading)

            # Skip if download is not needed or failed
            if node.skip_msg_id(key) or download_status in {
                DownloadStatus.SkipDownload,
                DownloadStatus.FailedDownload,
            }:
                continue

            # Return if any media is still downloading or uploading
            if (
                (
                    check_download_status
                    and download_status == DownloadStatus.Downloading
                )
                or upload_status == UploadStatus.Uploading
                or upload_status == UploadStatus.SkipUpload
            ):
                return ForwardStatus.CacheForward

            # Collect the media items that are valid for forwarding
            if media_item:
                multi_media.append(media_item)

        if len(multi_media) > 1:
            caption_item = None
            for item in multi_media:
                if item.message:
                    caption_item = item
                    break
            if caption_item:
                for item in multi_media:
                    if item is not caption_item:
                        item.message = ""
                        item.entities = None

        completed_group_ids = tuple(media_group)
        node.media_group_ids.pop(message.media_group_id)

    forward_status = ForwardStatus.SuccessForward

    reply_to_message_id = None
    message_thread_id = node.topic_id
    business_connection_id = None
    upload_telegram_chat_id = node.upload_telegram_chat_id
    if node.reply_to_message:
        if node.reply_to_message.chat.type != pyrogram.enums.ChatType.PRIVATE:
            reply_to_message_id = node.reply_to_message.id
        message_thread_id = node.reply_to_message.message_thread_id
        business_connection_id = node.reply_to_message.business_connection_id
        upload_telegram_chat_id = node.reply_to_message.chat.id
    if multi_media:
        if not await send_media_group_v2(
            client,
            upload_telegram_chat_id,  # type: ignore
            multi_media,
            message_thread_id=message_thread_id,
            reply_to_message_id=reply_to_message_id,
        ):
            forward_status = ForwardStatus.FailedForward
    else:
        forward_status = ForwardStatus.SkipForward

    node.stat_forward(forward_status, len(multi_media))
    for message_id in completed_group_ids:
        node.download_status.pop(message_id, None)
        node.upload_status.pop(message_id, None)

    return ForwardStatus.CacheForward


def record_download_status(func):
    """Record download status"""

    @wraps(func)
    async def inner(
        client: pyrogram.client.Client,
        message: pyrogram.types.Message,
        media_types: List[str],
        file_formats: dict,
        node: TaskNode,
    ):
        if _download_cache[(node.chat_id, message.id)] is DownloadStatus.Downloading:
            return DownloadStatus.Downloading, None

        key = (node.chat_id, message.id)
        _download_cache[key] = DownloadStatus.Downloading
        try:
            status, file_name = await func(
                client, message, media_types, file_formats, node
            )
        except BaseException:
            # Cancellation/transport errors must not leave a permanent
            # Downloading marker that makes every future retry a no-op.
            _download_cache.store.pop(key, None)
            raise

        # Persistence owns de-duplication across retries/restarts. This cache is
        # only an in-flight guard and must not grow for the lifetime of every
        # downloaded message.
        _download_cache.store.pop(key, None)

        return status, file_name

    return inner


async def report_bot_download_status(
    client: pyrogram.Client,
    node: TaskNode,
    download_status: DownloadStatus,
    download_size: int = 0,
):
    """
    Sends a message with the current status of the download bot.

    Parameters:
        client (pyrogram.Client): The client instance.
        node (TaskNode): The download task node.
        download_status (DownloadStatus): The current download status.

    Returns:
        None
    """
    node.stat(download_status)
    node.total_download_byte += download_size
    await report_bot_status(client, node)


async def report_bot_forward_status(
    client: pyrogram.Client,
    node: TaskNode,
    status: ForwardStatus,
):
    """
    Sends a message with the current status of the download bot.

    Parameters:
        client (pyrogram.Client): The client instance.
        node (TaskNode): The download task node.
        status (ForwardStatus): The current forward status.

    Returns:
        None
    """
    node.stat_forward(status)
    await report_bot_status(client, node)


async def report_bot_status(
    client: pyrogram.Client,
    node: TaskNode,
    immediate_reply=False,
):
    """see _report_bot_status"""
    try:
        return await _report_bot_status(client, node, immediate_reply)
    except Exception as e:
        logger.debug(f"{e}")


async def _report_bot_status(
    client: pyrogram.Client,
    node: TaskNode,
    immediate_reply=False,
):
    """
    Sends a message with the current status of the download bot.

    Parameters:
        client (pyrogram.Client): The client instance.
        node (TaskNode): The download task node.
        immediate_reply(bool): Immediate reply

    Returns:
        None
    """
    if not node.reply_message_id or not node.bot:
        return

    if immediate_reply or node.can_reply():
        if node.upload_telegram_chat_id:
            node.forward_msg_detail_str = (
                f"\n🔄 {_t('Forward')}\n"
                f"├─ 📁 {_t('Total')}: {node.total_forward_task}\n"
                f"├─ ✅ {_t('Success')}: {node.success_forward_task}\n"
                f"├─ ❌ {_t('Failed')}: {node.failed_forward_task}\n"
                f"└─ ⏩ {_t('Skipped')}: {node.skip_forward_task}\n"
            )

        upload_msg_detail_str: str = ""

        if node.upload_success_count:
            upload_msg_detail_str = (
                f"\n☁️ {_t('Upload')}\n"
                f"└─ ✅ {_t('Success')}: {node.upload_success_count}\n"
            )

        for idx, value in node.cloud_drive_upload_stat_dict.items():
            if value.transferred == value.total:
                continue

            temp_file_name = truncate_filename(os.path.basename(value.file_name), 10)
            upload_msg_detail_str += (
                f" ├─ 🆔 {_t('Message ID')}: {idx}\n"
                f" │   ├─ 📁 : {temp_file_name}\n"
                f" │   ├─ 📏 : {value.total}\n"
                f" │   ├─ ⏫ : {value.speed}\n"
                f" │   └─ 📊 : ["
                f'{create_progress_bar(int(value.percentage.split("%")[0]))}]'
                f" ({value.percentage})%\n"
            )

        download_result_str = ""
        download_result = get_download_result()
        if node.chat_id in download_result:
            messages = download_result[node.chat_id]
            for idx, value in messages.items():
                task_id = value["task_id"]
                if task_id != node.task_id or value["down_byte"] == value["total_size"]:
                    continue

                temp_file_name = truncate_filename(
                    os.path.basename(value["file_name"]), 10
                )
                progress = int(value["down_byte"] / value["total_size"] * 100)
                download_result_str += (
                    f" ├─ 🆔 {_t('Message ID')}: {idx}\n"
                    f" │   ├─ 📁 : {temp_file_name}\n"
                    f" │   ├─ 📏 : {format_byte(value['total_size'])}\n"
                    f" │   ├─ ⏬ : {format_byte(value['download_speed'])}/s\n"
                    f" │   └─ 📊 : [{create_progress_bar(progress)}]"
                    f" ({progress}%)\n"
                )

            if download_result_str:
                download_result_str = (
                    f"\n📥 {_t('Download Progresses')}:\n" + download_result_str
                )

        upload_result_str = ""
        for idx, value in node.upload_stat_dict.items():
            if value.total_size == value.upload_size:
                continue

            temp_file_name = truncate_filename(os.path.basename(value.file_name), 10)
            progress = int(value.upload_size / value.total_size * 100)
            upload_result_str += (
                f" ├─ 🆔 {_t('Message ID')}: {idx}\n"
                f" │   ├─ 📁 : {temp_file_name}\n"
                f" │   ├─ 📏 : {format_byte(value.total_size)}\n"
                f" │   ├─ ⏫ : {format_byte(value.upload_speed)}/s\n"
                f" │   └─ 📊 : [{create_progress_bar(progress)}]"
                f" ({progress}%)\n"
            )

        if upload_result_str:
            upload_result_str = f"\n📤 {_t('Upload Progresses')}:\n" + upload_result_str

        new_msg_str = (
            f"`\n"
            f"🆔 task id: {node.task_id}\n"
            f"📥 {_t('Downloading')}: {format_byte(node.total_download_byte)}\n"
            f"├─ 📁 {_t('Total')}: {node.total_download_task}\n"
            f"├─ ✅ {_t('Success')}: {node.success_download_task}\n"
            f"├─ ❌ {_t('Failed')}: {node.failed_download_task}\n"
            f"└─ ⏩ {_t('Skipped')}: {node.skip_download_task}\n"
            f"{node.forward_msg_detail_str}"
            f"{upload_msg_detail_str}"
            f"{upload_result_str}"
            f"{download_result_str}\n`"
        )

        if new_msg_str != node.last_edit_msg:
            node.last_edit_msg = new_msg_str
            await client.edit_message_text(
                node.from_user_id,
                node.reply_message_id,
                new_msg_str,
                parse_mode=pyrogram.enums.ParseMode.MARKDOWN,
            )


def set_max_concurrent_transmissions(
    client: pyrogram.Client, max_concurrent_transmissions: int
):
    """Set maximum concurrent transmissions"""
    if getattr(client, "max_concurrent_transmissions", None):
        client.max_concurrent_transmissions = max_concurrent_transmissions
        client.save_file_semaphore = asyncio.Semaphore(
            client.max_concurrent_transmissions
        )
        client.get_file_semaphore = asyncio.Semaphore(
            client.max_concurrent_transmissions
        )


async def _build_media_session(
    client: pyrogram.Client, dc_id: int, auth_key, bootstrap: bool
):
    """Create one media Session. Runs OUTSIDE media_sessions_lock so the slow
    TCP + auth handshake never blocks workers that only want an existing
    connection (the 2026-07-24 pool-24 stall was exactly this: session.start()
    held the global lock, serializing every worker behind each new build)."""
    from pyrogram import raw  # pylint: disable=import-outside-toplevel
    from pyrogram.errors import AuthBytesInvalid  # pylint: disable=import-outside-toplevel
    from pyrogram.session import Session  # pylint: disable=import-outside-toplevel
    from pyrogram.session.auth import Auth  # pylint: disable=import-outside-toplevel

    home_dc = await client.storage.dc_id()
    if auth_key is None:  # bootstrap: no session exists yet for this DC
        if dc_id == home_dc:
            auth_key = await client.storage.auth_key()
        else:
            auth_key = await asyncio.wait_for(
                Auth(client, dc_id, await client.storage.test_mode()).create(),
                timeout=_MEDIA_BUILD_TIMEOUT,
            )

    session = Session(
        client, dc_id, auth_key, await client.storage.test_mode(), is_media=True
    )
    try:
        await asyncio.wait_for(session.start(), timeout=_MEDIA_BUILD_TIMEOUT)
    except BaseException:
        try:
            await asyncio.wait_for(session.stop(), timeout=5)
        except BaseException:
            pass
        raise

    if bootstrap and dc_id != home_dc:
        for _ in range(3):
            exported_auth = await telegram_call(
                client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id)),
                _MEDIA_BUILD_TIMEOUT,
                "ExportAuthorization",
            )
            try:
                await telegram_call(
                    session.invoke(
                        raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id, bytes=exported_auth.bytes
                        )
                    ),
                    _MEDIA_BUILD_TIMEOUT,
                    "ImportAuthorization",
                )
            except AuthBytesInvalid:
                continue
            break
        else:
            await session.stop()
            raise AuthBytesInvalid
    _instrument_media_session(client, dc_id, session)
    return session


def _media_session_is_healthy(session) -> bool:
    """Return whether a pooled media session can accept new work."""
    state = getattr(session, "_tmd_health_state", None)
    if state and state.get("unhealthy"):
        return False
    started = getattr(session, "is_started", None)
    if hasattr(started, "is_set") and not started.is_set():
        return False
    connection = getattr(session, "connection", None)
    if connection is None:
        return False
    connected = getattr(connection, "is_connected", None)
    if connected is not None and not bool(connected):
        return False
    return True


def _is_transport_failure(error: BaseException) -> bool:
    """Recognize failures that make one Session unsafe to reuse."""
    # A Telegram response timeout can be transient and does not prove the TCP
    # connection is broken. TimeoutError inherits OSError on Python 3, so this
    # check must precede the generic OSError branch.
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return False
    if isinstance(error, (ConnectionError, OSError)):
        return True
    text = str(error or "").lower()
    return any(
        token in text
        for token in (
            "connection lost",
            "connection reset",
            "socket.send",
            "socket is closed",
            "another coroutine",
            "transport endpoint",
            "broken pipe",
        )
    )


def _consume_media_task(task: asyncio.Task) -> None:
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


def _mark_media_session_unhealthy(client, dc_id: int, session, error) -> None:
    """Quarantine exactly one broken session and schedule its removal."""
    state = getattr(session, "_tmd_health_state", None)
    if state is None or state.get("unhealthy"):
        return
    state["unhealthy"] = True
    state["error"] = str(error or "")[:500]
    task = asyncio.create_task(_evict_media_session(client, dc_id, session))
    task.add_done_callback(_consume_media_task)


async def _evict_media_session(client, dc_id: int, session) -> None:
    """Remove a failed Session from every pool before stopping it."""
    auth_key = getattr(session, "auth_key", None)
    async with client.media_sessions_lock:
        pools = getattr(client, "_tmd_media_session_pools", {})
        pool = pools.get(dc_id) or []
        pools[dc_id] = [item for item in pool if item is not session]
        for key, value in list(client.media_sessions.items()):
            if value is session:
                client.media_sessions.pop(key, None)
    try:
        await asyncio.wait_for(session.stop(), timeout=5)
    except BaseException:
        pass
    # Refill the exact DC immediately.  Previously the pool only regrew when a
    # later file happened to request a session, so a burst of failures could
    # shrink effective throughput for minutes while nominal worker slots stayed
    # occupied by retries.
    if auth_key is not None:
        try:
            await _schedule_media_pool_growth(client, dc_id, auth_key)
        except BaseException as error:
            logger.warning(f"media 连接补位失败 dc={dc_id}: {error}")


def _instrument_media_session(client, dc_id: int, session) -> None:
    """Install cancellation cleanup and a per-session restart guard."""
    if getattr(session, "_tmd_instrumented", False):
        return

    session._tmd_instrumented = True
    session._tmd_health_state = {
        "unhealthy": False,
        "restart_generation": 0,
        "send_timeouts": 0,
        "error": "",
    }
    restart_lock = asyncio.Lock()
    # The production pool has 20 sessions per DC while the global file pool is
    # 40.  Most batches live in one Telegram DC, so allow two outstanding RPCs
    # per TCP stream to make all 40 global slots useful. Cancellation below
    # still releases the permit immediately and a timed-out chunk is not sent
    # twice on the same connection.
    invoke_semaphore = asyncio.Semaphore(2)
    original_restart = session.restart
    original_send = session.send
    original_invoke = session.invoke

    async def guarded_restart(_session):
        state = _session._tmd_health_state
        generation = int(state["restart_generation"])
        async with restart_lock:
            if state.get("unhealthy"):
                return
            # ping_worker and recv_worker can schedule restart together. The
            # second caller observes that the first already rebuilt it.
            if int(state["restart_generation"]) != generation:
                return
            try:
                await original_restart()
                state["restart_generation"] = generation + 1
            except BaseException as error:
                logger.warning(
                    f"media 连接重启失败 dc={dc_id}: {error}"
                )
                _mark_media_session_unhealthy(client, dc_id, _session, error)

    async def cancellation_safe_send(_session, *args, **kwargs):
        # Let Session.send execute its own response-waiter cleanup even when a
        # file watchdog cancels the outer download coroutine.
        inner = asyncio.create_task(original_send(*args, **kwargs))

        def detached_send_done(task):
            try:
                task.result()
                _session._tmd_health_state["send_timeouts"] = 0
            except asyncio.CancelledError:
                pass
            except BaseException as error:
                state = _session._tmd_health_state
                if isinstance(error, asyncio.TimeoutError):
                    state["send_timeouts"] = int(
                        state.get("send_timeouts", 0)
                    ) + 1
                # A response timeout does not mean the TCP connection is
                # broken.  Several files can time out together on a busy
                # session; quarantining on their combined count aborts every
                # other healthy transfer sharing that session.
                if _is_transport_failure(error):
                    _mark_media_session_unhealthy(
                        client, dc_id, _session, error
                    )

        try:
            result = await asyncio.shield(inner)
            _session._tmd_health_state["send_timeouts"] = 0
            return result
        except asyncio.CancelledError:
            # A watchdog cancellation must not leave Session.send waiting on
            # its response event after the file slot has been released.  The
            # old shield-only path let that request keep sharing the media
            # connection with new transfers, while its response entry stayed
            # live until Pyrogram's RPC timeout. Retire the session so the
            # cancelled request and all of its response state disappear
            # together, then give the inner task a short window to settle.
            inner.cancel()
            _mark_media_session_unhealthy(
                client,
                dc_id,
                _session,
                "cancelled media request; retiring session",
            )
            done, _ = await asyncio.wait((inner,), timeout=2)
            if done:
                await asyncio.gather(inner, return_exceptions=True)
            else:
                inner.add_done_callback(detached_send_done)
            raise
        except BaseException as error:
            state = _session._tmd_health_state
            if isinstance(error, asyncio.TimeoutError):
                state["send_timeouts"] = int(state.get("send_timeouts", 0)) + 1
            if _is_transport_failure(error):
                _mark_media_session_unhealthy(
                    client, dc_id, _session, error
                )
            raise

    async def isolated_invoke(_session, *args, **kwargs):
        # Session.invoke otherwise retries the same dead TCP connection ten
        # times.  A media request must fail once, quarantine that connection,
        # and let the persisted file retry obtain a different pooled session.
        async def invoke_once():
            # Bound per-connection multiplexing; parallelism beyond two comes
            # from the media-session pool.
            async with invoke_semaphore:
                if _session._tmd_health_state.get("unhealthy"):
                    raise RuntimeError("media session quarantined")
                call_kwargs = dict(kwargs)
                # Invoke Pyrogram with zero internal retries because its
                # recursive retry calls the monkey-patched self.invoke again.
                # A congested proxy can deliver a valid 1 MiB response after
                # Pyrogram's 15-second default.  Wait longer once instead of
                # issuing a duplicate GetFile for the same offset; if this
                # still times out, the persisted file retry resumes on a
                # different pooled connection.
                call_kwargs["retries"] = 0
                call_kwargs.setdefault("timeout", _MEDIA_RPC_TIMEOUT)
                return await original_invoke(*args, **call_kwargs)

        # Retry a timed-out chunk HERE rather than letting Pyrogram do it.
        # Session.invoke's retry path calls ``self.invoke`` recursively, which is
        # this very wrapper, so it would try to acquire invoke_semaphore while
        # already holding a permit — with two concurrent retrying requests that
        # deadlocks the connection.  Retrying at this level releases the permit
        # between attempts (invoke_once owns the ``async with``), so it cannot.
        #
        # Only a response timeout is retried.  _is_transport_failure() is False
        # for TimeoutError precisely because a slow response does not prove the
        # TCP connection is broken, and re-issuing that one chunk is far cheaper
        # than discarding hundreds of megabytes of a finished-but-short file.
        last_error: Optional[BaseException] = None
        for attempt in range(_MEDIA_INVOKE_ATTEMPTS):
            try:
                # Do not shield this wrapper.  A file watchdog must release the
                # connection permit immediately; cancellation_safe_send already
                # keeps Pyrogram's response-waiter cleanup alive in the
                # background.
                return await invoke_once()
            except BaseException as error:
                # cancellation_safe_send owns the consecutive-timeout counter.
                # Counting the same exception again here would quarantine a
                # connection after its first transient timeout instead of two.
                if _is_transport_failure(error):
                    _mark_media_session_unhealthy(client, dc_id, _session, error)
                    raise
                # Cancellation and real RPC errors (FloodWait, bad request,
                # quarantined session) must surface immediately and untouched.
                if not isinstance(error, (asyncio.TimeoutError, TimeoutError)):
                    raise
                last_error = error
                if attempt + 1 >= _MEDIA_INVOKE_ATTEMPTS:
                    raise
                logger.debug(
                    f"media chunk 超时，重试 {attempt + 1}/"
                    f"{_MEDIA_INVOKE_ATTEMPTS - 1} dc={dc_id}"
                )
                await asyncio.sleep(_MEDIA_INVOKE_RETRY_DELAY * (attempt + 1))
        raise last_error  # pragma: no cover - loop always returns or raises

    session.restart = python_types.MethodType(guarded_restart, session)
    session.send = python_types.MethodType(cancellation_safe_send, session)
    session.invoke = python_types.MethodType(isolated_invoke, session)


def _media_build_backoff(client, dc_id: int) -> dict:
    backoffs = getattr(client, "_tmd_media_build_backoff", None)
    if backoffs is None:
        backoffs = client._tmd_media_build_backoff = {}
    return backoffs.setdefault(
        dc_id, {"failures": 0, "next_attempt": 0.0}
    )


def _record_media_build_result(client, dc_id: int, success: bool) -> None:
    state = _media_build_backoff(client, dc_id)
    if success:
        state.update({"failures": 0, "next_attempt": 0.0})
        return
    failures = int(state.get("failures", 0)) + 1
    state["failures"] = failures
    state["next_attempt"] = time.monotonic() + min(2 ** min(failures, 5), 30)


async def _get_pooled_media_session(client: pyrogram.Client, dc_id: int):
    """Return one of several MTProto media connections for a data center.

    The patched Pyrogram build caches exactly one media Session per DC. Every
    GetFile then shares that single TCP connection, capping aggregate speed
    even with many workers. A pool keeps the same authorization but spreads
    files over independent sessions. Sessions are built outside the lock; the
    lock is only held for the fast pick/append bookkeeping.
    """
    pool_size = max(min(int(getattr(client, "_tmd_media_pool_size", 1)), _MEDIA_POOL_HARD_CAP), 1)
    if pool_size == 1:
        session = await _default_get_media_session(client, dc_id)
        _instrument_media_session(client, dc_id, session)
        return session

    # --- Phase 1: fast decision under the lock (no network I/O here) ---
    async with client.media_sessions_lock:
        pools = getattr(client, "_tmd_media_session_pools", None)
        if pools is None:
            pools = client._tmd_media_session_pools = {}
        pool = pools.setdefault(dc_id, [])
        building = getattr(client, "_tmd_media_session_building", None)
        if building is None:
            building = client._tmd_media_session_building = {}
        existing = client.media_sessions.get(dc_id)
        if existing is not None and existing not in pool:
            _instrument_media_session(client, dc_id, existing)
            pool.append(existing)
        for session in list(pool):
            _instrument_media_session(client, dc_id, session)
            if not _media_session_is_healthy(session):
                _mark_media_session_unhealthy(
                    client, dc_id, session, "media session health check failed"
                )
        pool[:] = [
            session for session in pool if _media_session_is_healthy(session)
        ]

        have = len(pool)
        reserved = int(building.get(dc_id, 0))
        backoff = _media_build_backoff(client, dc_id)
        build_allowed = time.monotonic() >= float(
            backoff.get("next_attempt", 0.0)
        )
        do_build = False
        bootstrap = False
        auth_key = None
        if have == 0:
            # No session yet. Exactly one caller bootstraps; others wait.
            if reserved == 0 and build_allowed:
                do_build = True
                bootstrap = True
                building[dc_id] = 1
        elif (
            have + reserved < pool_size
            and reserved < _MAX_MEDIA_BUILDERS_PER_DC
            and build_allowed
        ):
            do_build = True
            auth_key = pool[0].auth_key
            building[dc_id] = reserved + 1

        pick = None
        if have > 0:
            cursors = getattr(client, "_tmd_media_session_cursors", None)
            if cursors is None:
                cursors = client._tmd_media_session_cursors = {}
            cursor = int(cursors.get(dc_id, 0))
            cursors[dc_id] = cursor + 1
            pick = pool[cursor % have]

    # --- Phase 2: grow the pool OUTSIDE the lock ---
    # Growth (pool already has a session) happens in the background so the
    # request path never blocks on a build. Only the bootstrap (empty pool)
    # is awaited inline, because nothing can be returned until it exists.
    if do_build and not bootstrap:
        tasks = getattr(client, "_tmd_media_session_tasks", None)
        if tasks is None:
            tasks = client._tmd_media_session_tasks = set()
        task = asyncio.ensure_future(
            _grow_media_pool(client, dc_id, auth_key)
        )
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    if pick is not None:
        return pick

    if not do_build and reserved == 0:
        # All sessions were quarantined and this DC is in build backoff. Wait
        # only for that bounded cooldown, then let one caller bootstrap again;
        # do not make every file sit in a blind 30-second polling loop.
        wait_seconds = max(
            float(backoff.get("next_attempt", 0.0)) - time.monotonic(), 0.05
        )
        await asyncio.sleep(min(wait_seconds, 30.0))
        return await _get_pooled_media_session(client, dc_id)

    if do_build and bootstrap:
        new_session = None
        try:
            new_session = await _build_media_session(
                client, dc_id, None, True
            )
            _record_media_build_result(client, dc_id, True)
            async with client.media_sessions_lock:
                pool = pools.setdefault(dc_id, [])
                client.media_sessions[dc_id] = new_session
                if new_session not in pool:
                    pool.append(new_session)
        except BaseException:
            _record_media_build_result(client, dc_id, False)
            raise
        finally:
            async with client.media_sessions_lock:
                building[dc_id] = max(int(building.get(dc_id, 0)) - 1, 0)
        await _schedule_media_pool_growth(client, dc_id, new_session.auth_key)
        return new_session

    # Pool was empty and someone else is bootstrapping. Give the bounded
    # background warm-up a few seconds so the first imported batch is spread
    # over several sessions instead of binding every long file to session #1.
    wait_started = time.monotonic()
    warm_floor = pool_size
    for _ in range(600):  # hard ceiling: ~30s
        async with client.media_sessions_lock:
            pool = pools.get(dc_id) or []
            if pool:
                reserved = int(building.get(dc_id, 0))
                if (
                    len(pool) < warm_floor
                    and reserved > 0
                    and time.monotonic() - wait_started < _MEDIA_POOL_WARM_TIMEOUT
                ):
                    pool = None
            if pool:
                cursors = getattr(client, "_tmd_media_session_cursors", None)
                if cursors is None:
                    cursors = client._tmd_media_session_cursors = {}
                cursor = int(cursors.get(dc_id, 0))
                cursors[dc_id] = cursor + 1
                return pool[cursor % len(pool)]
        await asyncio.sleep(0.05)
    # Bootstrap never produced a session; fall back to Pyrogram's default.
    fallback = await _default_get_media_session(client, dc_id)
    _instrument_media_session(client, dc_id, fallback)
    return fallback


async def _grow_media_pool(client: pyrogram.Client, dc_id: int, auth_key):
    """Background pool growth: build one more session and append it. Never
    blocks a download — failures just leave the pool at its current size."""
    building = client._tmd_media_session_building
    try:
        while True:
            session = await _build_media_session(client, dc_id, auth_key, False)
            auth_key = session.auth_key
            async with client.media_sessions_lock:
                pool = client._tmd_media_session_pools.setdefault(dc_id, [])
                counters = getattr(
                    client, "_tmd_media_session_key_counters", None
                )
                if counters is None:
                    counters = client._tmd_media_session_key_counters = {}
                counter = int(counters.get(dc_id, 0)) + 1
                counters[dc_id] = counter
                key = dc_id if not pool else (dc_id, counter)
                client.media_sessions[key] = session
                pool.append(session)
                target = max(
                    min(
                        int(getattr(client, "_tmd_media_pool_size", 1)),
                        _MEDIA_POOL_HARD_CAP,
                    ),
                    1,
                )
                other_builders = max(int(building.get(dc_id, 1)) - 1, 0)
                pool_has_capacity = len(pool) + other_builders < target
            _record_media_build_result(client, dc_id, True)
            if not pool_has_capacity:
                break
    except Exception as error:  # pylint: disable=broad-except
        _record_media_build_result(client, dc_id, False)
        logger.warning(f"media 连接池扩容失败 dc={dc_id}: {error}")
    finally:
        async with client.media_sessions_lock:
            building[dc_id] = max(int(building.get(dc_id, 0)) - 1, 0)


async def _schedule_media_pool_growth(
    client: pyrogram.Client, dc_id: int, auth_key
) -> None:
    """Warm a DC pool in the background without blocking file workers."""
    async with client.media_sessions_lock:
        pools = getattr(client, "_tmd_media_session_pools", {})
        pool = pools.get(dc_id) or []
        building = getattr(client, "_tmd_media_session_building", None)
        if building is None:
            building = client._tmd_media_session_building = {}
        target = max(
            min(
                int(getattr(client, "_tmd_media_pool_size", 1)),
                _MEDIA_POOL_HARD_CAP,
            ),
            1,
        )
        reserved = int(building.get(dc_id, 0))
        count = min(
            _MAX_MEDIA_BUILDERS_PER_DC - reserved,
            max(target - len(pool) - reserved, 0),
        )
        if count <= 0:
            return
        building[dc_id] = reserved + count
        tasks = getattr(client, "_tmd_media_session_tasks", None)
        if tasks is None:
            tasks = client._tmd_media_session_tasks = set()
        for _ in range(count):
            task = asyncio.create_task(
                _grow_media_pool(client, dc_id, auth_key)
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)


def set_media_session_pool_size(client: pyrogram.Client, pool_size: int) -> None:
    """Enable 1-80 independent download connections per Telegram data center.

    The pool is the real parallelism limit — every worker round-robins onto one
    of these sessions, and each session permits 2 concurrent RPCs, so a pool of
    N supports 2N in-flight chunk requests. Sizing the pool below
    ``max_download_task / 2`` silently caps concurrency no matter how many
    workers are spawned.

    History worth keeping: at pool_size=45 production stalled 368 large files at
    51-98 % complete. The cause was NOT the pool itself but chunk retries being
    disabled — one timed-out chunk discarded the whole file. With chunk retry
    restored (see isolated_invoke) congestion costs one chunk, not one file.
    """
    client._tmd_media_pool_size = max(
        min(int(pool_size or 1), _MEDIA_POOL_HARD_CAP), 1
    )
    client.handle_download = python_types.MethodType(
        _resumable_handle_download, client
    )
    pyrogram_client_module.get_session = _get_pooled_media_session


def set_media_proxy_pool(client: pyrogram.Client, proxies) -> int:
    """Spread media connections over several SOCKS exits.

    Measured 2026-08-15: the line itself carries 33+ MB/s (16 parallel streams
    to Cloudflare reached 26 MB/s while the downloader was already using 7),
    yet every Telegram media connection settled at ~0.30 MB/s and the aggregate
    stayed at 7-8 MB/s no matter the concurrency — 150 workers moved no more
    bytes than 24. The limit is the route from ONE proxy exit to Telegram, so
    the only way past it is to use more than one exit.

    `Session.start()` builds its socket with
    `self.client.connection_factory(proxy=self.client.proxy, media=...)`, so
    wrapping that factory is enough to give each media session a different
    exit. Only media connections rotate: the main MTProto connection carries
    auth and metadata and stays on the primary proxy, where its session state
    and the rate governor's assumptions remain valid.

    A dead exit degrades rather than breaks: its sessions fail, get
    quarantined and rebuilt on the next exit in the rotation.
    """
    entries = [dict(p) for p in (proxies or []) if p]
    if len(entries) < 2:
        return len(entries)

    base_factory = getattr(client, "_tmd_base_connection_factory", None)
    if base_factory is None:
        base_factory = client.connection_factory
        client._tmd_base_connection_factory = base_factory

    counter = itertools.count()

    def rotating_factory(*args, **kwargs):
        if kwargs.get("media"):
            kwargs["proxy"] = entries[next(counter) % len(entries)]
        return base_factory(*args, **kwargs)

    client.connection_factory = rotating_factory
    client._tmd_media_proxies = entries
    logger.info(
        "媒体连接将轮换 {} 个代理出口：{}",
        len(entries),
        ", ".join(
            "%s:%s" % (e.get("hostname"), e.get("port")) for e in entries
        ),
    )
    return len(entries)


async def fetch_message(client: pyrogram.Client, message: pyrogram.types.Message):
    """
    This function retrieves a message from a specified chat using the Pyrogram library.
     Args:
        client (pyrogram.Client): A client instance created using Pyrogram.
        message (pyrogram.types.Message): A message instance returned from Pyrogram.
     Returns:
        pyrogram.types.Message: A message object retrieved from the specified chat.
    """
    await rate_governor.acquire()
    return await telegram_call(
        client.get_messages(
            chat_id=message.chat.id,
            message_ids=message.id,
        ),
        45,
        "GetMessages",
    )


async def retry(func: Callable, args: tuple = (), max_attempts=3, wait_second=15):
    """
    Asynchronously retries the provided function
    a specified number of times with a specified wait time between retries.

    :param func: The function to be retried.
    :param args: The arguments to be passed to the function.
    :param max_attempts: The maximum number of attempts to retry the function.
        Defaults to 3.
    :param wait_second: The wait time in seconds between each retry attempt.
        Defaults to 15.

    :return: The result of the function
    if it succeeds within the maximum number of attempts, otherwise None.
    """

    for _ in range(1, max_attempts + 1):
        try:
            return await func(*args)
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            rate_governor.penalize(wait_err.value)
            logger.warning("bad call retry: FlowWait {}", wait_err.value)
            await asyncio.sleep(wait_err.value)
        except Exception as e:
            logger.exception("Error: {}", e)
            await asyncio.sleep(wait_second)

    logger.error("Failed after {} attempts", max_attempts)
    return None


async def get_media_group_with_retry(
    client: pyrogram.Client,
    chat_id: Union[int, str],
    message_id: int,
    max_attempts: int = 3,
    wait_second: int = 15,
):
    """
    get_media_group_with_retry
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return await telegram_call(
                client.get_media_group(chat_id, message_id),
                45,
                "GetMediaGroup",
            )
        except Exception as e:
            if attempt == max_attempts:
                logger.error("Failed Get Media Group[{}]", message_id)
                return types.List()

            logger.exception("Get Message[{}]: Error {}", message_id, e)
            await asyncio.sleep(wait_second)
    return types.List()


async def check_user_permission(
    client: pyrogram.Client, user_id: Union[int, str], chat_id: Union[int, str]
) -> bool:
    """
    Check if the user has permission to send videos in the group.

    Args:
        client (pyrogram.Client): A client instance created using Pyrogram.
        user_id (Union[int, str]): User Id
        chat_id (Union[int, str]): Chat Id

     Returns:
        if can_send_media_messages return True
    """
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return member and (
            not member.permissions or member.permissions.can_send_media_messages
        )
    except Exception:
        # logger.exception(e)
        pass

    return False


def set_meta_data(
    meta_data: MetaData, message: pyrogram.types.Message, caption: str = None
):
    """Get all meta data"""
    # message
    meta_data.message_date = getattr(message, "date", None)
    if caption:
        meta_data.message_caption = caption
    else:
        meta_data.message_caption = getattr(message, "caption", None) or ""
    meta_data.message_id = getattr(message, "id", None)

    from_user = getattr(message, "from_user")
    meta_data.sender_id = from_user.id if from_user else 0
    meta_data.sender_name = (from_user.username if from_user else "") or ""
    meta_data.reply_to_message_id = getattr(
        message, "reply_to_message_id", 1
    )  # 1 for General

    meta_data.message_thread_id = getattr(message, "message_thread_id", 1)
    # media
    for kind in meta_data.AVAILABLE_MEDIA:
        media_obj = getattr(message, kind, None)
        if media_obj is not None:
            meta_data.media_type = kind
            break
    else:
        return
    meta_data.media_file_name = getattr(media_obj, "file_name", None) or ""
    meta_data.media_file_size = getattr(media_obj, "file_size", None)
    meta_data.media_width = getattr(media_obj, "width", None)
    meta_data.media_height = getattr(media_obj, "height", None)
    meta_data.media_duration = getattr(media_obj, "duration", None)
    meta_data.file_extension = get_extension(
        media_obj.file_id, getattr(media_obj, "mime_type", ""), False
    )


async def parse_link(client: pyrogram.Client, link_str: str):
    """Parse link"""
    link = extract_info_from_link(link_str)
    if link.comment_id:
        chat = await client.get_chat(link.group_id)
        if chat:
            return chat.linked_chat.id, link.comment_id, link.topic_id

    return link.group_id, link.post_id, link.topic_id


async def update_cloud_upload_stat(
    transferred: str,
    total: str,
    percentage: str,
    speed: str,
    eta: str,
    node: TaskNode,
    message_id: int,
    file_name: str,
):
    """
    Update the cloud upload statistics with the given information.

    Args:
        transferred (str): The amount of data transferred.
        total (str): The total size of the file.
        percentage (str): The percentage of the file uploaded.
        speed (str): The upload speed.
        eta (str): The estimated time of arrival for the upload to complete.
        node (TaskNode): The task node associated with the upload.
        message_id (int): The ID of the message.
        file_name (str): The name of the file being uploaded.

    Returns:
        None
    """
    node.cloud_drive_upload_stat_dict[message_id] = CloudDriveUploadStat(
        file_name=file_name,
        transferred=transferred,
        total=total,
        percentage=percentage,
        speed=speed,
        eta=eta,
    )


async def update_upload_stat(
    upload_size: int,
    total_size: int,
    message_id: int,
    file_name: str,
    start_time: float,
    node: TaskNode,
    client: pyrogram.Client,
):
    """update_upload_status"""
    cur_time = time.time()

    if node.is_stop_transmission:
        client.stop_transmission()

    # TODO(tyh): web control upload stop

    if node.upload_stat_dict.get(message_id):
        upload_stat = node.upload_stat_dict[message_id]

        if cur_time - upload_stat.last_stat_time >= 1.0:
            upload_stat.upload_speed = max(
                int(
                    (upload_size - upload_stat.upload_size)
                    / (cur_time - upload_stat.last_stat_time)
                ),
                0,
            )
            upload_stat.last_stat_time = cur_time
            upload_stat.upload_size = upload_size

        node.upload_stat_dict[message_id] = upload_stat
    else:
        duration = cur_time - start_time
        upload_stat = UploadProgressStat(
            file_name=file_name,
            total_size=total_size,
            upload_size=upload_size,
            start_time=start_time,
            last_stat_time=cur_time,
            upload_speed=upload_size / (duration if duration > 0 else 1),
        )
        node.upload_stat_dict[message_id] = upload_stat


# pylint: enable=W0201
class HookSession(pyrogram.session.Session):
    """Hook Session"""

    def start_timeout(self: pyrogram.session.Session, start_timeout: int):
        """
        Set the start timeout for the session.

        Args:
            start_timeout (int): The start timeout value in seconds.

        Returns:
            None
        """
        self.START_TIMEOUT = start_timeout


# pylint: disable=all
class HookClient(pyrogram.Client):
    """Hook Client"""

    # pylint: disable=R0901
    START_TIME_OUT = 60

    def __init__(self, name: str, **kwargs):
        if "start_timeout" in kwargs:
            value = kwargs.get("start_timeout")
            if value:
                self.START_TIME_OUT = value
            kwargs.pop("start_timeout")

        super().__init__(name, **kwargs)

    async def connect(
        self,
    ) -> bool:
        """
        Connects the client to the server.

        Returns:
            bool: True if the client successfully
                connects to the server, False otherwise.

        Raises:
            ConnectionError: If the client is already connected.

        """
        if self.is_connected:  # type: ignore
            raise ConnectionError("Client is already connected")

        await self.load_session()

        self.session = HookSession(
            self,
            await self.storage.dc_id(),
            await self.storage.auth_key(),
            await self.storage.test_mode(),
        )
        self.session.start_timeout(self.START_TIME_OUT)

        await self.session.start()

        self.is_connected = True

        return bool(await self.storage.user_id())

    async def start(self):
        """
        Starts the client by performing necessary initialization steps.

        Returns:
            The initialized client instance.
        """
        is_authorized = await self.connect()

        try:
            if not is_authorized:
                await self.authorize()

            if not await self.storage.is_bot() and self.takeout:
                self.takeout_id = (
                    await self.invoke(
                        pyrogram.raw.functions.account.InitTakeoutSession()
                    )
                ).id
                logger.warning(f"Takeout session {self.takeout_id} initiated")

            await self.invoke(pyrogram.raw.functions.updates.GetState())
        except (Exception, KeyboardInterrupt):
            await self.disconnect()
            raise
        else:
            self.me = await self.get_me()
            await self.initialize()

            return self


# pylint: disable=R0914,R0913
async def forward_messages(
    client: pyrogram.Client,
    chat_id: Union[int, str, None],
    from_chat_id: Union[int, str],
    message_ids: Union[int, Iterable[int]],
    disable_notification: bool = None,
    schedule_date: datetime = None,
    protect_content: bool = None,
    drop_author: bool = None,
    topic_id: int = None,
    caption: str = None,
    caption_entities: List[pyrogram.types.MessageEntity] = None,
) -> Union["types.Message", List["types.Message"]]:
    """Forward messages of any kind."""

    is_iterable = not isinstance(message_ids, int)
    message_ids = list(message_ids) if is_iterable else [message_ids]  # type: ignore

    r = await client.invoke(
        pyrogram.raw.functions.messages.ForwardMessages(
            to_peer=await client.resolve_peer(chat_id),
            from_peer=await client.resolve_peer(from_chat_id),
            id=message_ids,
            silent=disable_notification or None,
            random_id=[client.rnd_id() for _ in message_ids],
            schedule_date=pyrogram.utils.datetime_to_timestamp(schedule_date),
            noforwards=protect_content,
            drop_author=drop_author,
            top_msg_id=topic_id,
        )
    )

    forwarded_messages = []

    users = {i.id: i for i in r.users}
    chats = {i.id: i for i in r.chats}

    for i in r.updates:
        if isinstance(
            i,
            (
                pyrogram.raw.types.UpdateNewMessage,
                pyrogram.raw.types.UpdateNewChannelMessage,
                pyrogram.raw.types.UpdateNewScheduledMessage,
            ),
        ):
            forwarded_messages.append(
                # pylint: disable=W0212
                await types.Message._parse(client, i.message, users, chats)
            )

    if caption and not is_iterable and forwarded_messages:
        await client.edit_message_caption(
            chat_id,
            forwarded_messages[0].id,
            caption=caption,
            caption_entities=caption_entities,
        )

    return types.List(forwarded_messages) if is_iterable else forwarded_messages[0]
