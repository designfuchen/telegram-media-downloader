"""Account-level rate governor.

Two responsibilities, both process-global and thread/async safe:

1. Reactive backoff: whenever a Telegram metadata request hits FloodWait, API
   consumers back off until the wait window elapses. Existing file transfers
   continue; their own FloodWait handling remains local to the transfer.

2. Proactive spacing: startup defaults to 1.2 seconds between metadata
   requests. File transfer chunks are unaffected; api_min_interval overrides it.
"""

import asyncio
import threading
import time

_lock = threading.RLock()
_backoff_until: float = 0.0
_min_interval: float = 0.0
_next_slot: float = 0.0
_flood_events: int = 0


def configure(min_interval: float) -> None:
    """Set the proactive minimum spacing between throttled calls (seconds)."""
    global _min_interval
    with _lock:
        _min_interval = max(float(min_interval or 0.0), 0.0)


def penalize(seconds) -> None:
    """Register a FloodWait; back the whole account off for ``seconds``."""
    global _backoff_until, _flood_events
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        seconds = 0.0
    if seconds <= 0:
        return
    with _lock:
        _backoff_until = max(_backoff_until, time.time() + seconds)
        _flood_events += 1


def backoff_remaining() -> float:
    """Seconds the account should still stay backed off (0 if clear)."""
    with _lock:
        return max(_backoff_until - time.time(), 0.0)


async def wait_if_backing_off() -> None:
    """Block while a global FloodWait backoff window is active."""
    while True:
        remaining = backoff_remaining()
        if remaining <= 0:
            return
        await asyncio.sleep(min(remaining, 5.0))


async def acquire() -> None:
    """Await the next allowed call slot (backoff first, then spacing)."""
    await wait_if_backing_off()
    if _min_interval <= 0:
        return
    global _next_slot
    while True:
        with _lock:
            now = time.time()
            if now >= _next_slot:
                _next_slot = now + _min_interval
                return
            sleep_for = _next_slot - now
        await asyncio.sleep(sleep_for)


def snapshot() -> dict:
    """Lightweight telemetry for the Web console."""
    with _lock:
        return {
            "backoff_remaining": round(max(_backoff_until - time.time(), 0.0), 1),
            "min_interval": _min_interval,
            "flood_events": _flood_events,
        }
