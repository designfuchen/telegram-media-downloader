"""Adaptive download-concurrency governor (Premium speed autopilot).

Goal: find and hold the fastest sustainable concurrency Telegram allows for
this account, instead of pinning a hand-picked number.

Three responsibilities, all process-global and thread/async safe:

1. Resizable worker gate: workers are spawned up to a hard ceiling, but only
   ``current_limit`` of them may pick up new tasks. The limit moves at
   runtime; in-flight transfers are never interrupted by a limit change.

2. Step controller: periodically raise the limit by one while the account is
   healthy (no FloodWait, no first-byte stalls, queue saturated) and the last
   raise actually improved throughput. If a raise buys < ``improve_ratio``
   extra speed, step back and lock the sweet spot for ``lock_seconds``.

3. Global stall cooldown (the missing back-pressure from the 7/22 incident):
   a burst of "waiting for first byte" timeouts means the account is being
   throttled server-side. Retrying harder makes it worse. When
   ``stall_threshold`` stalls land inside ``stall_window`` seconds, stop
   picking up new tasks for ``cooldown_seconds`` and drop the limit to the
   floor. Existing transfers keep running.
"""

import asyncio
import threading
import time

from loguru import logger

_lock = threading.RLock()

# --- static configuration (filled by configure()) ---
_enabled: bool = False
_floor: int = 6
_base: int = 8
_ceiling: int = 16
_adjust_interval: float = 300.0
_stall_threshold: int = 8
_stall_window: float = 180.0
_cooldown_seconds: float = 900.0
_hold_after_decrease: float = 1800.0
_lock_seconds: float = 21600.0
_improve_ratio: float = 1.03

# --- runtime state ---
_current_limit: int = 8
_cooldown_until: float = 0.0
_hold_until: float = 0.0
_locked_until: float = 0.0
_locked_limit: int = 0
_stall_events: list = []
_cooldown_count: int = 0
_speed_by_limit: dict = {}
_last_direction: int = 0  # +1 raised last time, 0 steady/lowered
# Download-path FloodWaits only. Scan/metadata 420s (channels.GetMessages
# etc.) go through rate_governor and are irrelevant to transfer concurrency;
# they must never step the transfer limit down.
_download_flood_events: int = 0
_probe_chat_id = None
_recovered_chat_id = None


def configure(
    *,
    enabled: bool,
    base: int,
    ceiling: int,
    floor: int,
    adjust_interval: float = 300.0,
    stall_threshold: int = 8,
    stall_window: float = 180.0,
    cooldown_seconds: float = 900.0,
    hold_after_decrease: float = 1800.0,
    lock_seconds: float = 21600.0,
    improve_ratio: float = 1.03,
) -> None:
    """Install limits. base is the starting concurrency, ceiling a red line."""
    global _enabled, _base, _ceiling, _floor, _current_limit
    global _adjust_interval, _stall_threshold, _stall_window
    global _cooldown_seconds, _hold_after_decrease, _lock_seconds, _improve_ratio
    global _cooldown_until, _hold_until, _locked_until, _locked_limit
    global _cooldown_count, _last_direction, _download_flood_events
    global _probe_chat_id, _recovered_chat_id
    with _lock:
        _download_flood_events = 0
        _probe_chat_id = None
        _recovered_chat_id = None
        _cooldown_until = 0.0
        _hold_until = 0.0
        _locked_until = 0.0
        _locked_limit = 0
        _cooldown_count = 0
        _last_direction = 0
        _stall_events.clear()
        _speed_by_limit.clear()
        _enabled = bool(enabled)
        _floor = max(int(floor), 1)
        _ceiling = max(int(ceiling), _floor)
        _base = min(max(int(base), _floor), _ceiling)
        _current_limit = _base
        _adjust_interval = max(float(adjust_interval), 60.0)
        _stall_threshold = max(int(stall_threshold), 2)
        _stall_window = max(float(stall_window), 30.0)
        _cooldown_seconds = max(float(cooldown_seconds), 60.0)
        _hold_after_decrease = max(float(hold_after_decrease), 0.0)
        _lock_seconds = max(float(lock_seconds), 0.0)
        _improve_ratio = max(float(improve_ratio), 1.0)


def enabled() -> bool:
    """Whether adaptive control is on (off = legacy fixed concurrency)."""
    with _lock:
        return _enabled


def current_limit() -> int:
    """How many workers may currently pick up new tasks."""
    with _lock:
        return _current_limit


def cooldown_remaining() -> float:
    """Seconds the global stall cooldown still blocks new task pickup."""
    with _lock:
        return max(_cooldown_until - time.time(), 0.0)


def record_first_byte_stall() -> None:
    """Register one 'no first byte' timeout; trip the breaker on a burst."""
    global _cooldown_until, _current_limit, _hold_until, _cooldown_count
    global _last_direction
    now = time.time()
    with _lock:
        # Fixed-concurrency mode must stay fixed.  Previously this path still
        # armed the adaptive 15-minute global breaker even while the console
        # said "adaptive speed disabled".  A burst of file timeouts therefore
        # left 59 workers idle and made normal per-file backoff look like a
        # scheduler deadlock.
        if not _enabled:
            return
        _stall_events.append(now)
        cutoff = now - _stall_window
        while _stall_events and _stall_events[0] < cutoff:
            _stall_events.pop(0)
        if len(_stall_events) < _stall_threshold:
            return
        if _cooldown_until > now:
            return
        _cooldown_until = now + _cooldown_seconds
        _hold_until = max(_hold_until, _cooldown_until + _hold_after_decrease)
        previous = _current_limit
        _current_limit = _floor
        _cooldown_count += 1
        _last_direction = 0
        _stall_events.clear()
    logger.warning(
        "首字节超时密集出现（{} 次 / {} 秒内），判定账号被限速：全局暂停领取新任务 "
        "{} 秒，并发 {} → {}（在传文件不受影响）",
        _stall_threshold,
        int(_stall_window),
        int(_cooldown_seconds),
        previous,
        _floor,
    )


def arm_transport_probe(chat_id) -> None:
    """Mark the one retry allowed to test whether Telegram recovered."""
    global _probe_chat_id
    with _lock:
        _probe_chat_id = str(chat_id)


def record_transfer_progress(chat_id) -> bool:
    """Clear a transport cooldown as soon as a probe receives real bytes."""
    global _cooldown_until, _current_limit, _hold_until
    global _probe_chat_id, _recovered_chat_id
    now = time.time()
    chat_key = str(chat_id)
    with _lock:
        recovered = _cooldown_until > now or _probe_chat_id == chat_key
        if not recovered:
            return False
        _cooldown_until = 0.0
        _hold_until = 0.0
        _current_limit = _base if _enabled else _ceiling
        _probe_chat_id = None
        _recovered_chat_id = chat_key
        _stall_events.clear()
    logger.success("Telegram 传输探针已收到数据，立即恢复正常领取任务")
    return True


def consume_recovery_signal():
    """Return the recovered channel once so persisted backoff can be released."""
    global _recovered_chat_id
    with _lock:
        chat_id = _recovered_chat_id
        _recovered_chat_id = None
        return chat_id


def set_base(value: int) -> int:
    """Manual tuning entry point: move the probe's starting concurrency.

    Red line: clamped to [floor, ceiling] — the 16 ceiling cannot be raised
    from here. Clears any sweet-spot lock so probing restarts from the new
    base, but an active stall cooldown keeps the limit at the floor until it
    expires (safety beats operator impatience).
    """
    global _base, _current_limit, _locked_until, _locked_limit, _last_direction
    with _lock:
        _base = min(max(int(value), _floor), _ceiling)
        _locked_until = 0.0
        _locked_limit = 0
        _last_direction = 0
        if _cooldown_until <= time.time():
            _current_limit = _base
        return _base


def download_flood_events() -> int:
    """Count of download-path FloodWaits since configure()."""
    with _lock:
        return _download_flood_events


def penalize_flood() -> None:
    """A download-path FloodWait: step down and hold before probing again."""
    global _current_limit, _hold_until, _download_flood_events, _last_direction
    with _lock:
        _download_flood_events += 1
        if not _enabled:
            return
        previous = _current_limit
        _current_limit = max(_floor, _current_limit - 2)
        _last_direction = 0
        _hold_until = max(_hold_until, time.time() + _hold_after_decrease)
        if previous != _current_limit:
            logger.warning(
                "下载路径 FloodWait：并发 {} → {}，{} 分钟内不再上探",
                previous,
                _current_limit,
                int(_hold_after_decrease // 60),
            )


async def wait_for_turn(worker_index: int) -> None:
    """Gate one worker: honour the live limit and the global cooldown."""
    while True:
        with _lock:
            limit = _current_limit if _enabled else _ceiling
            cooling = _cooldown_until - time.time()
        # Keep exactly one worker alive as a health probe. A total stop for the
        # whole cooldown made every failed file enter backoff together and left
        # the downloader showing 0 B/s long after the route had recovered.
        if cooling > 0 and worker_index != 0:
            await asyncio.sleep(min(cooling, 5.0))
            continue
        if worker_index < limit:
            return
        await asyncio.sleep(2.0)


async def controller(
    get_total_speed,
    get_flood_events,
    get_in_flight,
    get_paused=None,
) -> None:
    """Periodic step controller. Runs for the lifetime of the service."""
    global _current_limit, _hold_until, _locked_until, _locked_limit, _last_direction
    if not enabled():
        return
    last_flood = int(get_flood_events() or 0)
    samples: list = []
    window_started = time.time()
    window_limit = current_limit()
    while True:
        await asyncio.sleep(10)
        try:
            samples.append(max(int(get_total_speed() or 0), 0))
        except Exception:  # pylint: disable=broad-except
            samples.append(0)
        if time.time() - window_started < _adjust_interval:
            continue

        avg_speed = sum(samples) / max(len(samples), 1)
        samples = []
        window_started = time.time()

        # Global pause: workers hold their items (in_flight stays high) and
        # the speed counter goes stale at its pre-pause value, so a paused
        # window looks "saturated and healthy". Probing on that would silently
        # climb to the ceiling during a long pause and resume at full blast.
        try:
            if get_paused is not None and get_paused():
                window_limit = current_limit()
                continue
        except Exception:  # pylint: disable=broad-except
            pass

        floods_now = int(get_flood_events() or 0)
        flood_delta = max(floods_now - last_flood, 0)
        last_flood = floods_now
        now = time.time()

        with _lock:
            limit = _current_limit
            cooling = _cooldown_until > now
            holding = _hold_until > now
            locked = _locked_until > now
        # A window is only a valid measurement of `limit` if the limit did
        # not move mid-window (stall breaker / flood penalty) and the account
        # was healthy: cooldown windows and flood windows measure Telegram's
        # throttling, not what this concurrency can deliver.
        stable_window = limit == window_limit
        window_limit = limit

        try:
            in_flight = int(get_in_flight() or 0)
        except Exception:  # pylint: disable=broad-except
            in_flight = 0
        saturated = in_flight >= max(limit - 1, 1)
        if (
            stable_window
            and not cooling
            and flood_delta == 0
            and saturated
            and avg_speed > 0
        ):
            with _lock:
                previous_avg = _speed_by_limit.get(limit)
                _speed_by_limit[limit] = (
                    avg_speed
                    if previous_avg is None
                    else previous_avg * 0.5 + avg_speed * 0.5
                )

        # penalize_flood() already stepped down and set the hold the moment
        # the FloodWait happened; the window pass only skips probing on top
        # of that instead of punishing the same event a second time.
        if cooling or flood_delta > 0 or holding or locked or not saturated:
            continue

        with _lock:
            # Did the last raise pay for itself? Compare EMA at L vs L-1.
            if _last_direction > 0:
                current_avg = _speed_by_limit.get(_current_limit)
                lower_avg = _speed_by_limit.get(_current_limit - 1)
                if (
                    current_avg is not None
                    and lower_avg is not None
                    and current_avg < lower_avg * _improve_ratio
                ):
                    _current_limit -= 1
                    _locked_limit = _current_limit
                    _locked_until = now + _lock_seconds
                    _last_direction = 0
                    window_limit = _current_limit
                    logger.success(
                        "并发 {} 相比 {} 无显著提速（{:.1f} vs {:.1f} MB/s），"
                        "回落并锁定甜蜜点 {}（{} 小时后再探）",
                        _locked_limit + 1,
                        _locked_limit,
                        (current_avg or 0) / 1024 / 1024,
                        (lower_avg or 0) / 1024 / 1024,
                        _locked_limit,
                        int(_lock_seconds // 3600),
                    )
                    continue
            if _current_limit < _ceiling:
                _current_limit += 1
                _last_direction = 1
                logger.info(
                    "账号健康且队列饱和：并发上探 {} → {}（天花板 {}）",
                    _current_limit - 1,
                    _current_limit,
                    _ceiling,
                )
        # A deliberate boundary adjustment (raise or sweet-spot step-back)
        # is not mid-window instability: sync the marker so the very next
        # window already counts as a valid measurement of the new limit.
        # Without this every probe step needs two windows instead of one.
        window_limit = current_limit()


def snapshot() -> dict:
    """Lightweight telemetry for the Web console and the Telegram bot."""
    now = time.time()
    with _lock:
        return {
            "enabled": _enabled,
            "current_limit": _current_limit,
            "base": _base,
            "ceiling": _ceiling,
            "floor": _floor,
            "cooldown_remaining": round(max(_cooldown_until - now, 0.0), 1),
            "cooldown_count": _cooldown_count,
            "download_floods": _download_flood_events,
            "hold_remaining": round(max(_hold_until - now, 0.0), 1),
            "locked_limit": _locked_limit if _locked_until > now else 0,
            "locked_remaining": round(max(_locked_until - now, 0.0), 1),
            "recent_stalls": len(_stall_events),
            "speed_by_limit": {
                str(k): round(v / 1024 / 1024, 2)
                for k, v in sorted(_speed_by_limit.items())
            },
        }
