"""Async weighted fair scheduler partitioned by Telegram channel."""

import asyncio
from collections import defaultdict, deque


PRIORITY_WEIGHTS = {"high": 4, "normal": 2, "low": 1}


class FairChannelScheduler:
    """Schedule channel queues with smooth weighted round-robin fairness."""

    def __init__(
        self,
        max_workers: int = 1,
        max_per_channel: int = 4,
        max_active_channels: int = 0,
    ):
        self.max_workers = max(int(max_workers), 1)
        self.max_per_channel = max(int(max_per_channel), 1)
        # 0 (or negative) = unlimited: any number of channels may download at
        # once (weighted round-robin spreads them). >0 = serial mode: only this
        # many channels download concurrently; a channel must fully drain
        # before the next one starts (auto-advance).
        self.max_active_channels = max(int(max_active_channels), 0)
        self._queues = defaultdict(deque)
        self._priorities = {}
        self._in_flight = defaultdict(int)
        self._current_weights = defaultdict(int)
        self._condition = asyncio.Condition()
        self._sequence = 0
        self._dispatched = defaultdict(int)

    @staticmethod
    def _channel_key(item) -> str:
        return str(item[1].chat_id)

    @staticmethod
    def _priority(item) -> str:
        priority = str(getattr(item[1], "scheduler_priority", "normal") or "normal")
        return priority if priority in PRIORITY_WEIGHTS else "normal"

    async def put(self, item) -> None:
        """Append an item to its channel queue and wake waiting workers."""
        chat_id = self._channel_key(item)
        async with self._condition:
            self._queues[chat_id].append(item)
            self._priorities[chat_id] = self._priority(item)
            self._condition.notify_all()

    def _active_channels(self) -> list:
        channel_keys = list(self._queues.keys())
        return [
            chat_id
            for chat_id in channel_keys
            if self._queues.get(chat_id) or self._in_flight.get(chat_id, 0)
        ]

    def _channel_capacity(self) -> int:
        # Weighted round-robin already provides fairness while channels have
        # queued work. Do not hard-partition workers by channel count: a nearly
        # empty or stalled channel would otherwise leave its unused share idle.
        return min(self.max_workers, self.max_per_channel)

    def _select_channel(self):
        capacity = self._channel_capacity()
        eligible = [
            chat_id
            for chat_id, items in self._queues.items()
            if items and self._in_flight[chat_id] < capacity
        ]
        if not eligible:
            return None

        # Serial mode: cap how many channels download at once. While the active
        # channels (those with files in flight) are at the limit, only feed
        # those already-active channels; do not open a new one. A channel that
        # fully drains (no queue, no in-flight) frees a slot so the next channel
        # auto-starts. If every active channel is momentarily at per-channel
        # capacity, return None so the free worker waits instead of jumping to
        # a new channel.
        if self.max_active_channels > 0:
            active = {cid for cid, n in self._in_flight.items() if n > 0}
            if len(active) >= self.max_active_channels:
                restricted = [cid for cid in eligible if cid in active]
                if restricted:
                    eligible = restricted
                else:
                    # The configured channel limit is a preferred working set,
                    # not a reason to leave global workers idle.  When every
                    # active channel has no dispatchable queue item, allow one
                    # already-prefetched standby channel to use the vacancy.
                    # The retry hydrator admits at most one standby channel at
                    # a time, so this does not turn the scheduler into an
                    # unbounded channel fan-out.
                    pass

        total_weight = 0
        for chat_id in eligible:
            weight = PRIORITY_WEIGHTS[self._priorities.get(chat_id, "normal")]
            self._current_weights[chat_id] += weight
            total_weight += weight
        selected = max(
            eligible,
            key=lambda chat_id: (
                self._current_weights[chat_id],
                -self._dispatched[chat_id],
                chat_id,
            ),
        )
        self._current_weights[selected] -= total_weight
        return selected

    async def get(self):
        """Return the next fair item, waiting when all channels are at capacity."""
        async with self._condition:
            while True:
                chat_id = self._select_channel()
                if chat_id is not None:
                    item = self._queues[chat_id].popleft()
                    self._in_flight[chat_id] += 1
                    self._dispatched[chat_id] += 1
                    self._sequence += 1
                    return item
                await self._condition.wait()

    async def task_done(self, chat_id) -> None:
        """Release one per-channel concurrency slot and wake blocked workers."""
        key = str(chat_id)
        async with self._condition:
            self._in_flight[key] = max(self._in_flight[key] - 1, 0)
            if not self._queues[key] and not self._in_flight[key]:
                self._current_weights.pop(key, None)
            self._condition.notify_all()

    def snapshot(self) -> dict:
        """Return lightweight scheduler telemetry for the Web console."""
        # Flask reads on other threads. Copy dictionaries before iteration;
        # deque length is safe to inspect, though telemetry may be approximate.
        queues = self._queues.copy()
        priorities = self._priorities.copy()
        in_flight = self._in_flight.copy()
        dispatched = self._dispatched.copy()
        channels = []
        for chat_id in queues.keys() | in_flight.keys():
            if not queues.get(chat_id) and not in_flight.get(chat_id, 0):
                continue
            channels.append(
                {
                    "chat_id": chat_id,
                    "priority": priorities.get(chat_id, "normal"),
                    "queued": len(queues.get(chat_id, ())),
                    "in_flight": in_flight.get(chat_id, 0),
                    "dispatched": dispatched.get(chat_id, 0),
                }
            )
        channels.sort(
            key=lambda item: (
                {"high": 0, "normal": 1, "low": 2}.get(item["priority"], 1),
                -item["queued"],
                item["chat_id"],
            )
        )
        return {
            "policy": "smooth_weighted_round_robin",
            "max_workers": self.max_workers,
            "max_per_channel": self.max_per_channel,
            "active_channels": len(channels),
            "queued": sum(item["queued"] for item in channels),
            "in_flight": sum(item["in_flight"] for item in channels),
            "dispatched": self._sequence,
            "channels": channels,
        }
