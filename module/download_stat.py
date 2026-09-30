"""Download Stat"""
import asyncio
import threading
import time
from enum import Enum

from pyrogram import Client

from module.app import TaskNode


class DownloadState(Enum):
    """Download state"""

    Downloading = 1
    StopDownload = 2


_download_result: dict = {}
_total_download_speed: int = 0
_total_download_size: int = 0
_last_download_time: float = time.time()
_download_state: DownloadState = DownloadState.Downloading
_download_result_lock = threading.RLock()
_channel_download_paused: set[str] = set()

# v3.5-final: true pause = abort in-flight transfer & free the slot.
# Off by default (freeze in place). Enable via config ``pause_release_transfers``.
_pause_release_enabled: bool = False
# Per-(chat, message) markers so concurrent files of the same channel do not
# race on a shared flag when aborted for a pause.
_pause_aborted_keys: set = set()


def set_pause_release(enabled: bool) -> None:
    """Enable/disable aborting in-flight transfers on channel pause."""
    global _pause_release_enabled  # pylint: disable=W0603
    _pause_release_enabled = bool(enabled)


def pause_release_enabled() -> bool:
    """Whether channel pause aborts in-flight transfers to free the slot."""
    return _pause_release_enabled


def mark_pause_aborted(chat_id, message_id: int) -> None:
    """Record that a transfer was aborted because its channel was paused."""
    with _download_result_lock:
        _pause_aborted_keys.add((str(chat_id), int(message_id)))


def take_pause_aborted(chat_id, message_id: int) -> bool:
    """Consume and return whether this transfer was aborted for a pause."""
    key = (str(chat_id), int(message_id))
    with _download_result_lock:
        if key in _pause_aborted_keys:
            _pause_aborted_keys.discard(key)
            return True
        return False


def get_download_result() -> dict:
    """get global download result"""
    return _download_result


def get_download_snapshot() -> dict:
    """Return a stable copy for readers running outside the download loop."""
    with _download_result_lock:
        return {
            chat_id: {
                message_id: dict(value) for message_id, value in messages.items()
            }
            for chat_id, messages in _download_result.items()
        }


def remove_download_result(chat_id, message_id: int) -> None:
    """Drop one terminal/stopped transfer from the live-only progress cache."""
    with _download_result_lock:
        messages = _download_result.get(chat_id)
        if messages is None:
            messages = _download_result.get(str(chat_id))
        if not messages:
            return
        messages.pop(int(message_id), None)
        if not messages:
            _download_result.pop(chat_id, None)
            _download_result.pop(str(chat_id), None)


def get_total_download_speed() -> int:
    """get total download speed"""
    global _total_download_speed  # pylint: disable=W0603
    global _total_download_size  # pylint: disable=W0603
    global _last_download_time  # pylint: disable=W0603

    # A terminal transfer is removed from the live progress cache immediately.
    # Do not keep showing its last sampled rate (or carry its pending bytes into
    # the next file), which produced impossible post-completion spikes.
    with _download_result_lock:
        has_active_transfer = any(
            bool(messages) for messages in _download_result.values()
        )
        if not has_active_transfer:
            _total_download_speed = 0
            _total_download_size = 0
            _last_download_time = time.time()
            return 0

    # The counter is only refreshed by progress callbacks. Once every
    # transfer stops (idle, pause, stall) no callback ever fires again, so
    # without this decay the last non-zero reading would be reported forever
    # to both the Web console and the speed governor.
    if time.time() - _last_download_time > 5.0:
        return 0
    return _total_download_speed


def get_download_state() -> DownloadState:
    """get download state"""
    return _download_state


# pylint: disable = W0603
def set_download_state(state: DownloadState):
    """set download state"""
    global _download_state
    _download_state = state


def set_channel_download_paused(chat_ids, paused: bool) -> None:
    """Suspend or resume active transfers for selected channels."""
    normalized = {str(chat_id) for chat_id in chat_ids if str(chat_id)}
    with _download_result_lock:
        if paused:
            _channel_download_paused.update(normalized)
        else:
            _channel_download_paused.difference_update(normalized)


def is_channel_download_paused(chat_id) -> bool:
    """Return whether progress callbacks for a channel should wait."""
    with _download_result_lock:
        return str(chat_id) in _channel_download_paused


async def update_download_status(
    down_byte: int,
    total_size: int,
    message_id: int,
    file_name: str,
    start_time: float,
    node: TaskNode,
    client: Client,
    resume_offset: int = 0,
):
    """update_download_status"""
    cur_time = time.time()
    # pylint: disable = W0603
    global _total_download_speed
    global _total_download_size
    global _last_download_time

    if node.is_stop_transmission:
        client.stop_transmission()

    chat_id = node.chat_id

    # Channel pause with release enabled: abort this transfer to free its
    # connection/scheduler slot for other channels. It is re-queued (status
    # 'paused', no retry attempt consumed) and re-downloaded on resume.
    # Global pause still freezes in place to preserve partial progress.
    if (
        _pause_release_enabled
        and get_download_state() != DownloadState.StopDownload
        and is_channel_download_paused(chat_id)
    ):
        mark_pause_aborted(chat_id, message_id)
        client.stop_transmission()

    while (
        get_download_state() == DownloadState.StopDownload
        or is_channel_download_paused(chat_id)
    ):
        if node.is_stop_transmission:
            client.stop_transmission()
        await asyncio.sleep(1)

    with _download_result_lock:
        if not _download_result.get(chat_id):
            _download_result[chat_id] = {}

        if _download_result[chat_id].get(message_id):
            last_download_byte = _download_result[chat_id][message_id]["down_byte"]
            last_time = _download_result[chat_id][message_id]["end_time"]
            download_speed = _download_result[chat_id][message_id]["download_speed"]
            each_second_total_download = _download_result[chat_id][message_id][
                "each_second_total_download"
            ]
            end_time = _download_result[chat_id][message_id]["end_time"]

            _total_download_size += down_byte - last_download_byte
            each_second_total_download += down_byte - last_download_byte

            if cur_time - last_time >= 1.0:
                download_speed = int(each_second_total_download / (cur_time - last_time))
                end_time = cur_time
                each_second_total_download = 0

            download_speed = max(download_speed, 0)

            _download_result[chat_id][message_id]["down_byte"] = down_byte
            _download_result[chat_id][message_id]["end_time"] = end_time
            _download_result[chat_id][message_id]["download_speed"] = download_speed
            _download_result[chat_id][message_id][
                "each_second_total_download"
            ] = each_second_total_download
        else:
            # Pyrogram reports absolute file progress after a resumed transfer.
            # The bytes already present in ``.temp`` are not network traffic in
            # this attempt and must not be counted as an instantaneous burst.
            first_attempt_bytes = max(int(down_byte) - int(resume_offset or 0), 0)
            each_second_total_download = first_attempt_bytes
            elapsed = max(cur_time - start_time, 0.001)
            _download_result[chat_id][message_id] = {
                "down_byte": down_byte,
                "total_size": total_size,
                "file_name": file_name,
                "start_time": start_time,
                "end_time": cur_time,
                "download_speed": first_attempt_bytes / elapsed,
                "each_second_total_download": each_second_total_download,
                "task_id": node.task_id,
            }
            _total_download_size += first_attempt_bytes

    if cur_time - _last_download_time >= 1.0:
        # update speed
        _total_download_speed = int(
            _total_download_size / (cur_time - _last_download_time)
        )
        _total_download_speed = max(_total_download_speed, 0)
        _total_download_size = 0
        _last_download_time = cur_time
