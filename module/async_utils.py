"""Small async boundaries for blocking persistence and bounded Telegram calls.

The downloader runs its scheduler, progress callbacks and Telegram client on one
event loop.  SQLite, filesystem probes and stdlib HTTP calls are synchronous, so
they must not run on that loop.  Database work uses one dedicated executor to
preserve the existing write order and avoid creating a new SQLite writer race.
"""

import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor


class TelegramRequestTimeout(TimeoutError):
    """A metadata request did not finish inside the caller's deadline."""


_DB_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tmd-db")


async def run_db(function, *args, **kwargs):
    """Run one synchronous database operation without blocking the event loop."""
    loop = asyncio.get_running_loop()
    call = functools.partial(function, *args, **kwargs)
    return await loop.run_in_executor(_DB_EXECUTOR, call)


async def run_blocking(function, *args, **kwargs):
    """Run non-database blocking work in asyncio's general worker pool."""
    return await asyncio.to_thread(function, *args, **kwargs)


def _consume_background_result(task: asyncio.Task) -> None:
    """Retrieve a detached task result so late failures are never unhandled."""
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass


async def telegram_call(awaitable, timeout: float, label: str = "Telegram request"):
    """Bound a Telegram metadata request without corrupting Pyrogram state.

    Cancelling Pyrogram while ``Session.send`` owns a response waiter can leave
    that waiter inside the session and later produce "another coroutine" or
    connection-restart races.  Shielding lets Pyrogram finish its own timeout
    cleanup, while this downloader coroutine releases its scan/worker slot.
    """
    task = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.wait_for(
            asyncio.shield(task), timeout=max(float(timeout), 0.1)
        )
    except asyncio.TimeoutError as error:
        task.add_done_callback(_consume_background_result)
        raise TelegramRequestTimeout(
            f"{label} exceeded {float(timeout):g} seconds"
        ) from error
