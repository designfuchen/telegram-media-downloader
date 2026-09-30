"""Focused regression coverage for queue, persistence and connection fixes."""

import asyncio
import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import media_downloader
from module import (
    download_history,
    download_stat,
    download_tasks,
    pyrogram_extension,
    speed_governor,
    storage_health,
    web,
)
from module.app import (
    Application,
    ChatDownloadConfig,
    DownloadStatus,
    TaskNode,
)
from module.async_utils import TelegramRequestTimeout, telegram_call
from module.fair_scheduler import FairChannelScheduler
from module.pyrogram_extension import (
    _evict_media_session,
    _get_pooled_media_session,
    _instrument_media_session,
    _resumable_handle_download,
)


class DatabaseRegressionTests(unittest.TestCase):
    """Exercise the SQLite state machine using an isolated database."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "tasks.db"
        self.env = mock.patch.dict(
            os.environ,
            {
                "TMD_TASK_DB": str(self.database),
                "TMD_HISTORY_DB": str(self.database),
            },
            clear=False,
        )
        self.env.start()
        download_tasks._SCHEMA_READY.clear()
        download_history._SCHEMA_READY.clear()

    def tearDown(self):
        self.env.stop()
        download_tasks._SCHEMA_READY.clear()
        download_history._SCHEMA_READY.clear()
        self.temp_dir.cleanup()

    def test_completion_and_history_are_consistent_and_deduplicated(self):
        self.assertTrue(
            download_tasks.queue_task(
                "chat-1", 7, chat_title="Channel", total_size=12
            )
        )
        first_path = Path(self.temp_dir.name) / "first.mp4"
        second_path = Path(self.temp_dir.name) / "renamed.mp4"
        download_tasks.complete_task_with_history(
            "chat-1", 7, str(first_path), 12, "Channel", "video"
        )
        download_tasks.complete_task_with_history(
            "chat-1", 7, str(second_path), 12, "Channel", "video"
        )

        with download_tasks._connect() as connection:
            task = connection.execute(
                "SELECT status, save_path FROM download_tasks "
                "WHERE chat_id = 'chat-1' AND message_id = 7"
            ).fetchone()
            history = connection.execute(
                "SELECT save_path FROM download_history "
                "WHERE chat_id = 'chat-1' AND message_id = 7"
            ).fetchall()
        self.assertEqual(task["status"], "completed")
        self.assertEqual(task["save_path"], str(second_path))
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["save_path"], str(second_path))

    def test_channel_is_not_retired_without_current_completed_scan(self):
        download_tasks.queue_task("chat-2", 1)
        download_tasks.finish_task("chat-2", 1, "completed")
        self.assertFalse(download_tasks.retire_completed_channel("chat-2"))
        channel = download_tasks.get_channel_config("chat-2")
        self.assertEqual(channel["enabled"], 1)

        download_tasks.update_channel_scan_state("chat-2", "completed")
        channel = download_tasks.get_channel_config("chat-2")
        self.assertEqual(channel["enabled"], 0)
        download_tasks.set_channel_enabled("chat-2", True)
        download_tasks.update_channel_scan_state(
            "chat-2", "failed", "temporary network error"
        )
        self.assertFalse(download_tasks.retire_completed_channel("chat-2"))
        channel = download_tasks.get_channel_config("chat-2")
        self.assertEqual(channel["enabled"], 1)

    def test_reenable_makes_preserved_tasks_retryable(self):
        download_tasks.queue_task("chat-3", 10)
        download_tasks.set_channel_enabled("chat-3", False)
        download_tasks.set_channel_enabled("chat-3", True)
        state = download_tasks.task_claim_state("chat-3", 10)
        self.assertEqual(state["status"], "retry_requested")

    def test_transport_failures_back_off_without_losing_the_file(self):
        download_tasks.queue_task("chat-network", 11)

        expected_delays = [60, 300, 900, 1800, 3600]
        for attempt, expected_delay in enumerate(expected_delays, start=1):
            self.assertEqual(
                download_tasks.start_task("chat-network", 11), attempt
            )
            retry = download_tasks.fail_task(
                "chat-network", 11, "传输无进度超过 60 秒"
            )
            self.assertTrue(retry["retry"])
            self.assertTrue(retry["transient"])
            self.assertEqual(retry["delay"], expected_delay)
            state = download_tasks.task_claim_state("chat-network", 11)
            self.assertEqual(state["status"], "retrying")
            self.assertTrue(download_tasks.queue_task("chat-network", 11))

        with download_tasks._connect() as connection:
            task = connection.execute(
                "SELECT attempts FROM download_tasks "
                "WHERE chat_id = 'chat-network' AND message_id = 11"
            ).fetchone()
        self.assertEqual(task["attempts"], 5)

    def test_resumable_progress_retries_immediately_instead_of_backing_off(self):
        """A partial file that grew must not wait out the transport backoff.

        Production had 368 multi-GB files parked at 51-98 % complete: each
        attempt banked more chunks yet was sent to the back of the 1-hour
        transient backoff, so they never converged.
        """
        download_tasks.queue_task("chat-progress", 14)

        for attempt in range(1, 6):
            self.assertEqual(
                download_tasks.start_task("chat-progress", 14), attempt
            )
            retry = download_tasks.fail_task(
                "chat-progress",
                14,
                "downloaded 1073741824 bytes, expected 2147483648 bytes",
                made_progress=True,
            )
            self.assertTrue(retry["retry"])
            self.assertTrue(retry["transient"])
            self.assertEqual(retry["delay"], download_tasks.PROGRESS_RETRY_DELAY)
            self.assertTrue(download_tasks.queue_task("chat-progress", 14))

    def test_stalled_transfer_without_progress_still_backs_off(self):
        """Zero-byte attempts keep the escalating backoff that protects the account."""
        download_tasks.queue_task("chat-noprogress", 15)

        for attempt, expected_delay in enumerate([60, 300, 900], start=1):
            self.assertEqual(
                download_tasks.start_task("chat-noprogress", 15), attempt
            )
            retry = download_tasks.fail_task(
                "chat-noprogress",
                15,
                "等待首字节超过 180 秒",
                made_progress=False,
            )
            self.assertEqual(retry["delay"], expected_delay)
            self.assertTrue(download_tasks.queue_task("chat-noprogress", 15))

    def test_transient_retry_is_finite(self):
        """Transport failures are patient but must not retry forever."""
        download_tasks.queue_task("chat-forever", 16)
        with download_tasks._connect() as connection:
            connection.execute(
                "UPDATE download_tasks SET attempts = ? "
                "WHERE chat_id = 'chat-forever' AND message_id = 16",
                (download_tasks.TRANSIENT_MAX_ATTEMPTS,),
            )

        retry = download_tasks.fail_task(
            "chat-forever", 16, "connection lost", made_progress=False
        )

        self.assertFalse(retry["retry"])
        self.assertTrue(retry["transient"])
        state = download_tasks.task_claim_state("chat-forever", 16)
        self.assertEqual(state["status"], "failed")

    def test_permanent_failure_keeps_original_retry_ceiling(self):
        download_tasks.queue_task("chat-permanent", 12)

        for attempt in range(1, 4):
            self.assertEqual(
                download_tasks.start_task("chat-permanent", 12), attempt
            )
            retry = download_tasks.fail_task(
                "chat-permanent", 12, "Telegram 消息不存在或当前账号无权访问"
            )
            self.assertEqual(retry["retry"], attempt < 3)
            if attempt < 3:
                self.assertTrue(download_tasks.queue_task("chat-permanent", 12))

        state = download_tasks.task_claim_state("chat-permanent", 12)
        self.assertEqual(state["status"], "failed")

    def test_database_busy_is_retried_as_transient(self):
        download_tasks.queue_task("chat-database", 13)
        self.assertEqual(download_tasks.start_task("chat-database", 13), 1)

        retry = download_tasks.fail_task(
            "chat-database", 13, "database is locked"
        )

        self.assertTrue(retry["retry"])
        self.assertTrue(retry["transient"])
        self.assertEqual(retry["delay"], 60)
        state = download_tasks.task_claim_state("chat-database", 13)
        self.assertEqual(state["status"], "retrying")

    def test_history_reader_does_not_repeat_schema_writes(self):
        """Opening history during an active writer must remain read-only."""
        download_tasks.queue_task("chat-history", 14)
        download_history._SCHEMA_READY.clear()
        writer = sqlite3.connect(self.database, timeout=0.1)
        try:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("BEGIN IMMEDIATE")
            writer.execute(
                "UPDATE download_tasks SET error = 'writer-active' "
                "WHERE chat_id = 'chat-history' AND message_id = 14"
            )
            self.assertEqual(download_history.history_count(), 0)
        finally:
            writer.rollback()
            writer.close()

    def test_history_first_page_uses_composite_sort_index(self):
        download_tasks.queue_task("chat-history-index", 15)
        download_tasks.complete_task_with_history(
            "chat-history-index", 15, str(self.database.parent / "15.mp4"), 10
        )
        with download_tasks._connect() as connection:
            plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT chat_id, message_id FROM download_history
                ORDER BY completed_at DESC, message_id DESC LIMIT 50
                """
            ).fetchall()
        details = " ".join(str(row[3]) for row in plan)
        self.assertIn("idx_download_history_completed_message", details)
        self.assertNotIn("TEMP B-TREE", details)

    def test_resuming_channel_bumps_revision_for_scan_restart(self):
        download_tasks.add_channel_to_library("chat-rescan", "Rescan")
        before = download_tasks.get_channel_config("chat-rescan")
        download_tasks.set_channels_paused(["chat-rescan"], True)
        paused = download_tasks.get_channel_config("chat-rescan")
        self.assertEqual(paused["config_revision"], before["config_revision"])

        download_tasks.set_channels_paused(["chat-rescan"], False)
        resumed = download_tasks.get_channel_config("chat-rescan")
        self.assertEqual(
            resumed["config_revision"], before["config_revision"] + 1
        )

    def test_retry_hydration_respects_active_channel_ceiling(self):
        for chat_id in ("active", "next-a", "next-b"):
            for message_id in (1, 2):
                download_tasks.queue_task(chat_id, message_id)
                download_tasks.defer_task(chat_id, message_id, "cold queue")

        active_only = download_tasks.list_retry_requests(
            10, ["active"], max_new_channels=0
        )
        self.assertEqual(
            {item["chat_id"] for item in active_only}, {"active"}
        )

        with_one_new = download_tasks.list_retry_requests(
            10, ["active"], max_new_channels=1
        )
        selected_channels = {item["chat_id"] for item in with_one_new}
        self.assertIn("active", selected_channels)
        self.assertLessEqual(len(selected_channels), 2)

    def test_retry_hydration_reserves_capacity_for_new_channels(self):
        for chat_id in ("active-a", "active-b", "new-a", "new-b", "new-c"):
            for message_id in range(1, 31):
                download_tasks.queue_task(chat_id, message_id)
                download_tasks.defer_task(chat_id, message_id, "cold queue")

        selected = download_tasks.list_retry_requests(
            85,
            ["active-a", "active-b"],
            max_new_channels=3,
            participating_limits={"active-a": 5, "active-b": 5},
            new_channel_limit=25,
        )
        counts = {}
        for item in selected:
            counts[item["chat_id"]] = counts.get(item["chat_id"], 0) + 1

        self.assertEqual(counts["active-a"], 5)
        self.assertEqual(counts["active-b"], 5)
        self.assertEqual(len(counts), 5)
        self.assertEqual(sorted(counts.values()), [5, 5, 25, 25, 25])

    def test_retry_hydration_never_selects_disabled_completed_channel(self):
        download_tasks.queue_task("disabled", 1)
        download_tasks.defer_task("disabled", 1, "preserved")
        download_tasks.set_channel_enabled("disabled", False)
        download_tasks.queue_task("enabled", 2)
        download_tasks.defer_task("enabled", 2, "ready")

        selected = download_tasks.list_retry_requests(
            10,
            [],
            max_new_channels=2,
            new_channel_limit=5,
        )

        self.assertEqual(
            {(item["chat_id"], item["message_id"]) for item in selected},
            {("enabled", 2)},
        )

    def test_transport_probe_wakes_work_without_losing_attempt_history(self):
        for message_id in (21, 22):
            download_tasks.queue_task("chat-probe", message_id)
            self.assertEqual(download_tasks.start_task("chat-probe", message_id), 1)
            download_tasks.fail_task(
                "chat-probe", message_id, "等待首字节超过 20 秒"
            )
        download_tasks.queue_task("chat-other", 31)
        self.assertEqual(download_tasks.start_task("chat-other", 31), 1)
        download_tasks.fail_task("chat-other", 31, "connection lost")

        probe = download_tasks.claim_transport_probe()
        self.assertEqual(probe["chat_id"], "chat-probe")
        probe_state = download_tasks.task_claim_state(
            probe["chat_id"], probe["message_id"]
        )
        self.assertEqual(probe_state["status"], "retry_requested")

        released = download_tasks.release_transport_retries(
            preferred_chat_id="chat-probe", max_channels=1, limit=10
        )
        self.assertEqual(released, 1)
        self.assertEqual(
            download_tasks.task_claim_state("chat-other", 31)["status"],
            "retrying",
        )
        with download_tasks._connect() as connection:
            attempts = connection.execute(
                "SELECT attempts FROM download_tasks "
                "WHERE chat_id = 'chat-probe' ORDER BY message_id"
            ).fetchall()
        self.assertEqual([row["attempts"] for row in attempts], [1, 1])

    def test_transport_probe_can_skip_full_active_channels_for_standby(self):
        for chat_id in ("active-a", "active-b", "standby"):
            download_tasks.queue_task(chat_id, 1)
            self.assertEqual(download_tasks.start_task(chat_id, 1), 1)
            download_tasks.fail_task(chat_id, 1, "connection lost")

        probe = download_tasks.claim_transport_probe(
            exclude_chat_ids={"active-a", "active-b"}
        )

        self.assertEqual(
            (probe["chat_id"], probe["message_id"]), ("standby", 1)
        )
        self.assertEqual(
            download_tasks.task_claim_state("standby", 1)["status"],
            "retry_requested",
        )

    def test_live_capacity_refill_preserves_attempts_and_channel_limits(self):
        for chat_id in ("chat-a", "chat-b", "chat-c"):
            for message_id in range(1, 6):
                download_tasks.queue_task(chat_id, message_id)
                self.assertEqual(
                    download_tasks.start_task(chat_id, message_id), 1
                )
                download_tasks.fail_task(
                    chat_id, message_id, "等待首字节超过 20 秒"
                )

        released = download_tasks.refill_retry_capacity(
            {"chat-a": 2}, max_active_channels=2,
            per_channel_limit=3, limit=5
        )
        self.assertEqual(released, 5)
        with download_tasks._connect() as connection:
            states = {
                row["chat_id"]: (row["ready"], row["attempts"])
                for row in connection.execute(
                    """
                    SELECT chat_id,
                           SUM(CASE WHEN status = 'retry_requested' THEN 1 ELSE 0 END) AS ready,
                           MAX(attempts) AS attempts
                    FROM download_tasks GROUP BY chat_id
                    """
                )
            }
        self.assertEqual(states["chat-a"], (2, 1))
        self.assertEqual(states["chat-b"], (3, 1))
        self.assertEqual(states["chat-c"], (0, 1))

    def test_active_channel_refill_is_not_hidden_by_older_inactive_work(self):
        download_tasks.queue_task("inactive", 1)
        with download_tasks._connect() as connection:
            connection.executemany(
                """
                INSERT INTO download_tasks (
                    chat_id, message_id, status, queued_at, updated_at
                ) VALUES ('inactive', ?, 'retry_requested', ?, ?)
                """,
                [
                    (message_id, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00")
                    for message_id in range(2, 502)
                ],
            )
            connection.execute(
                "UPDATE download_tasks SET status = 'retry_requested', "
                "updated_at = '2026-01-01T00:00:00+00:00' "
                "WHERE chat_id = 'inactive' AND message_id = 1"
            )
        download_tasks.queue_task("active", 9001)
        with download_tasks._connect() as connection:
            connection.execute(
                "UPDATE download_tasks SET status = 'retry_requested', "
                "updated_at = '2026-08-10T00:00:00+00:00' "
                "WHERE chat_id = 'active' AND message_id = 9001"
            )

        rows = download_tasks.list_retry_requests(
            limit=1,
            participating_chat_ids={"active"},
            max_new_channels=0,
        )

        self.assertEqual(
            [(row["chat_id"], row["message_id"]) for row in rows],
            [("active", 9001)],
        )

    def test_channel_list_query_returns_aggregates(self):
        download_tasks.queue_task(
            "chat-4", 3, chat_title="4.Channel", total_size=100
        )
        result = download_tasks.list_channels(limit=50)
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["records"][0]["chat_id"], "chat-4")
        self.assertEqual(result["records"][0]["pending"], 1)

    def test_channel_stays_active_until_its_last_downloading_file_finishes(self):
        download_tasks.queue_task("chat-active", 1)
        download_tasks.queue_task("chat-active", 2)
        self.assertEqual(download_tasks.start_task("chat-active", 1), 1)
        self.assertEqual(download_tasks.start_task("chat-active", 2), 1)

        download_tasks.finish_task("chat-active", 1, "completed")
        channel = download_tasks.get_channel_config("chat-active")
        remaining = download_tasks.task_claim_state("chat-active", 2)
        self.assertEqual(channel["lifecycle_state"], "downloading")
        self.assertEqual(remaining["status"], "downloading")

        download_tasks.finish_task("chat-active", 2, "completed")
        channel = download_tasks.get_channel_config("chat-active")
        self.assertEqual(channel["lifecycle_state"], "completed")
        self.assertEqual(channel["enabled"], 1)

    def test_channel_api_returns_fast_snapshot_then_cached_aggregates(self):
        download_tasks.queue_task(
            "chat-5", 8, chat_title="5.Channel", total_size=200
        )
        web._flask_app.config["LOGIN_DISABLED"] = True
        with web._channel_api_cache_lock:
            web._channel_api_cache.clear()
            web._channel_api_refreshing.clear()
        client = web._flask_app.test_client()

        first = client.get("/api/channels?page=1&limit=50")
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.get_json()["count"], 1)

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with web._channel_api_cache_lock:
                if web._channel_api_cache:
                    break
            time.sleep(0.02)
        second = client.get("/api/channels?page=1&limit=50")
        payload = second.get_json()
        self.assertEqual(payload["data"][0]["pending"], 1)

    def test_channel_state_change_invalidates_cached_channel_rows(self):
        download_tasks.queue_task(
            "chat-cache", 9, chat_title="Cached.Channel", total_size=100
        )
        download_tasks.set_channels_paused(["chat-cache"], True)
        web._flask_app.config["LOGIN_DISABLED"] = True
        cache_key = (1, 50, "", "")
        with web._channel_api_cache_lock:
            web._channel_api_cache[cache_key] = (
                time.monotonic(),
                {"total": 1, "records": [{"chat_id": "chat-cache", "paused": 1}]},
            )

        client = web._flask_app.test_client()
        with mock.patch.object(
            web, "get_download_state", return_value=download_stat.DownloadState.Downloading
        ), mock.patch.object(web, "set_channel_download_paused"), mock.patch.object(web, "_runtime_blocker", return_value=""):
            response = client.post(
                "/api/channel_state",
                json={"chat_ids": ["chat-cache"], "action": "start"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(download_tasks.get_channel_config("chat-cache")["paused"])
        with web._channel_api_cache_lock:
            self.assertFalse(web._channel_api_cache)

    def test_dashboard_persisted_stats_reuse_fresh_cache(self):
        old_cache = dict(web._persisted_stats_cache)
        old_refreshing = web._persisted_stats_refreshing
        cached = {
            "tasks": {"active": 7},
            "channels": {"total": 3},
        }
        try:
            with web._persisted_stats_cache_lock:
                web._persisted_stats_cache.update(
                    {"updated_at": time.monotonic(), "value": cached}
                )
                web._persisted_stats_refreshing = False
            with mock.patch.object(web, "task_counts") as task_counts, mock.patch.object(
                web, "channel_counts"
            ) as channel_counts:
                self.assertEqual(web._cached_persisted_stats(), cached)
                task_counts.assert_not_called()
                channel_counts.assert_not_called()
        finally:
            with web._persisted_stats_cache_lock:
                web._persisted_stats_cache.clear()
                web._persisted_stats_cache.update(old_cache)
                web._persisted_stats_refreshing = old_refreshing


class FixedConcurrencyRegressionTests(unittest.TestCase):
    def test_disabled_adaptive_mode_never_arms_global_cooldown(self):
        speed_governor.configure(
            enabled=False, base=60, ceiling=60, floor=1,
            stall_threshold=1, stall_window=60, cooldown_seconds=900,
        )
        speed_governor.record_first_byte_stall()
        self.assertEqual(speed_governor.cooldown_remaining(), 0)

    def test_recent_success_keeps_retry_refill_eligible_after_speed_hits_zero(self):
        old_completed_at = media_downloader._last_successful_transfer_at
        media_downloader._last_successful_transfer_at = 90.0
        try:
            with mock.patch.object(
                media_downloader, "_monotonic", return_value=100.0
            ), mock.patch.object(
                media_downloader, "get_total_download_speed", return_value=0
            ):
                self.assertTrue(media_downloader._transport_recently_healthy())
            with mock.patch.object(
                media_downloader, "_monotonic", return_value=110.0
            ), mock.patch.object(
                media_downloader, "get_total_download_speed", return_value=0
            ):
                self.assertFalse(media_downloader._transport_recently_healthy())
        finally:
            media_downloader._last_successful_transfer_at = old_completed_at

    def test_idle_active_channels_allow_one_standby_prefetch(self):
        snapshot = {
            "max_workers": 6,
            "queued": 0,
            "in_flight": 2,
            "channels": [
                {"chat_id": "active-a", "queued": 0, "in_flight": 1},
                {"chat_id": "active-b", "queued": 0, "in_flight": 1},
            ],
        }
        fake_queue = SimpleNamespace(
            max_workers=6,
            max_per_channel=3,
            max_active_channels=2,
        )
        with mock.patch.object(media_downloader, "queue", fake_queue):
            self.assertTrue(
                media_downloader._scheduler_can_prefetch(
                    "standby", snapshot
                )
            )


class SchedulerRefillTests(unittest.IsolatedAsyncioTestCase):
    """One completed file must immediately make room for its replacement."""

    async def test_channel_slot_refills_without_waiting_for_batch_completion(self):
        scheduler = FairChannelScheduler(
            max_workers=6, max_per_channel=3, max_active_channels=2
        )
        for chat_id in ("chat-a", "chat-b", "chat-c"):
            node = SimpleNamespace(
                chat_id=chat_id, scheduler_priority="normal"
            )
            for message_id in range(4):
                await scheduler.put((message_id, node))

        initial = await asyncio.gather(
            *(scheduler.get() for _ in range(6))
        )
        initial_channels = [item[1].chat_id for item in initial]
        self.assertEqual(len(set(initial_channels)), 2)
        released_channel = initial_channels[0]

        await scheduler.task_done(released_channel)
        replacement = await asyncio.wait_for(scheduler.get(), timeout=0.05)

        self.assertEqual(replacement[1].chat_id, released_channel)
        self.assertEqual(scheduler.snapshot()["in_flight"], 6)

    async def test_idle_active_set_allows_one_prefetched_standby(self):
        scheduler = FairChannelScheduler(
            max_workers=6, max_per_channel=3, max_active_channels=2
        )
        nodes = {
            chat_id: SimpleNamespace(chat_id=chat_id, scheduler_priority="normal")
            for chat_id in ("chat-a", "chat-b", "chat-c")
        }
        # Establish two active channels, then leave only in-flight work on
        # them. A prefetched standby must be able to use the idle slot instead
        # of waiting for both metadata requests to finish.
        for chat_id in ("chat-a", "chat-b"):
            for message_id in range(3):
                await scheduler.put((message_id, nodes[chat_id]))
        initial = await asyncio.gather(*(scheduler.get() for _ in range(6)))
        await scheduler.put((99, nodes["chat-c"]))
        replacement = await asyncio.wait_for(scheduler.get(), timeout=0.05)
        self.assertEqual(replacement[1].chat_id, "chat-c")

    async def test_imported_channel_scans_share_the_startup_concurrency_cap(self):
        active = 0
        peak = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked_scan(*_args):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if peak == media_downloader.CHANNEL_SCAN_CONCURRENCY:
                started.set()
            try:
                await release.wait()
            finally:
                active -= 1

        scans = media_downloader.CHANNEL_SCAN_CONCURRENCY + 4
        old_running = media_downloader.app.is_running
        media_downloader.app.is_running = True
        try:
            with mock.patch.object(
                media_downloader,
                "download_chat_task",
                side_effect=blocked_scan,
            ), mock.patch.object(
                media_downloader,
                "run_db",
                new=mock.AsyncMock(),
            ):
                tasks = [
                    asyncio.create_task(
                        media_downloader._run_channel_scan(
                            object(), f"chat-{index}", ChatDownloadConfig()
                        )
                    )
                    for index in range(scans)
                ]
                await asyncio.wait_for(started.wait(), timeout=0.2)
                await asyncio.sleep(0)
                self.assertEqual(peak, media_downloader.CHANNEL_SCAN_CONCURRENCY)
                release.set()
                await asyncio.gather(*tasks)
        finally:
            media_downloader.app.is_running = old_running

    async def test_imported_channel_scan_retries_database_contention(self):
        attempts = 0

        async def scan_with_one_busy_error(*_args):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise sqlite3.OperationalError("database is locked")

        old_running = media_downloader.app.is_running
        media_downloader.app.is_running = True
        try:
            with mock.patch.object(
                media_downloader,
                "download_chat_task",
                side_effect=scan_with_one_busy_error,
            ), mock.patch.object(
                media_downloader,
                "run_db",
                new=mock.AsyncMock(),
            ), mock.patch.object(
                media_downloader.asyncio,
                "sleep",
                new=mock.AsyncMock(),
            ):
                await media_downloader._run_channel_scan(
                    object(), "chat-database-scan", ChatDownloadConfig()
                )
        finally:
            media_downloader.app.is_running = old_running

        self.assertEqual(attempts, 2)

    async def test_hot_prefetch_does_not_admit_sixth_channel(self):
        old_workers = media_downloader.queue.max_workers
        old_channels = media_downloader.queue.max_active_channels
        old_per_channel = media_downloader.queue.max_per_channel
        media_downloader.queue.max_workers = 100
        media_downloader.queue.max_active_channels = 5
        media_downloader.queue.max_per_channel = 20
        try:
            state = {
                "queued": 20,
                "in_flight": 80,
                "channels": [
                    {
                        "chat_id": f"chat-{index}",
                        "queued": 4,
                        "in_flight": 16,
                    }
                    for index in range(5)
                ],
            }
            self.assertTrue(
                media_downloader._scheduler_can_prefetch("chat-0", state)
            )
            self.assertFalse(
                media_downloader._scheduler_can_prefetch("chat-5", state)
            )

            full_channel = dict(state)
            full_channel["queued"] = 5
            full_channel["in_flight"] = 20
            full_channel["channels"] = [
                {
                    "chat_id": "chat-0",
                    "queued": 5,
                    "in_flight": 20,
                }
            ]
            self.assertFalse(
                media_downloader._scheduler_can_prefetch(
                    "chat-0", full_channel
                )
            )
        finally:
            media_downloader.queue.max_workers = old_workers
            media_downloader.queue.max_active_channels = old_channels
            media_downloader.queue.max_per_channel = old_per_channel


class ApplicationCheckpointTests(unittest.TestCase):
    """Checkpoints must stay bounded when SQLite owns retries."""

    def test_sqlite_checkpoint_does_not_scan_or_serialize_terminal_ids(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            application = Application(
                str(Path(temp_dir) / "config.yaml"),
                str(Path(temp_dir) / "data.yaml"),
                "test",
            )
            application.config["channel_storage"] = "sqlite"
            channel = ChatDownloadConfig()
            channel.finish_task = 1
            channel.last_read_message_id = 99
            channel.ids_to_retry = list(range(1000))
            channel.node.download_status = {
                item: DownloadStatus.SuccessDownload for item in range(1000)
            }
            application.chat_download_config["chat"] = channel

            checkpoint = application.build_checkpoint_data()

            self.assertEqual(checkpoint["chat"][0]["ids_to_retry"], [])
            self.assertEqual(application.pending_channel_cursors(), [("chat", 100)])
            application.executor.shutdown(wait=True)
            application.loop.close()


class NotificationRegressionTests(unittest.TestCase):
    """A retry wave must not masquerade as a completed batch."""

    def test_failed_files_keep_batch_unresolved(self):
        self.assertFalse(
            media_downloader._batch_is_drained(
                {"active": 0, "failed": 12}, scanning=0
            )
        )
        self.assertTrue(
            media_downloader._batch_is_drained(
                {"active": 0, "failed": 0}, scanning=0
            )
        )


class AsyncBoundaryTests(unittest.IsolatedAsyncioTestCase):
    """Timeouts and pooled sessions must not corrupt shared async state."""

    async def test_nas_finalization_runs_off_the_event_loop(self):
        with mock.patch.object(
            media_downloader,
            "run_blocking",
            new=mock.AsyncMock(return_value=None),
        ) as run_blocking:
            await media_downloader._finalize_download_file(
                "/tmp/verified.partial",
                "/app/downloads/channel/final.mp4",
            )

        run_blocking.assert_awaited_once_with(
            media_downloader._move_to_download_path,
            "/tmp/verified.partial",
            "/app/downloads/channel/final.mp4",
        )

    async def test_stalled_transfer_releases_slot_without_inline_sleep_retry(self):
        node = TaskNode("chat-stalled")
        media = SimpleNamespace(file_size=1024, mime_type="video/mp4")
        message = SimpleNamespace(
            id=76,
            video=media,
            media_group_id=None,
        )
        with mock.patch.object(
            media_downloader,
            "_refresh_reference_if_stale",
            new=mock.AsyncMock(return_value=message),
        ), mock.patch.object(
            media_downloader,
            "_get_media_meta",
            new=mock.AsyncMock(
                return_value=("/tmp/final.mp4", "/tmp/partial.mp4", "mp4")
            ),
        ), mock.patch.object(
            media_downloader, "_can_download", return_value=True
        ), mock.patch.object(
            media_downloader, "_is_exist", return_value=False
        ), mock.patch.object(
            media_downloader,
            "_download_media_with_watchdog",
            new=mock.AsyncMock(
                side_effect=media_downloader.DownloadStalledError(
                    "等待首字节超过 20 秒", had_progress=False
                )
            ),
        ) as transfer, mock.patch.object(
            media_downloader.asyncio,
            "sleep",
            new=mock.AsyncMock(),
        ) as sleep:
            status, path = await media_downloader.download_media(
                object(), message, ["video"], {}, node
            )

        self.assertEqual(status, DownloadStatus.FailedDownload)
        self.assertIsNone(path)
        transfer.assert_awaited_once()
        sleep.assert_not_awaited()

    async def test_blocked_postprocess_does_not_hold_download_completion(self):
        started = asyncio.Event()
        release = asyncio.Event()
        local_postprocess_queue = asyncio.Queue(maxsize=4)
        node = TaskNode(
            "chat-postprocess", bot=object(), reply_message_id=1
        )
        message = SimpleNamespace(
            id=77,
            text=None,
            media=object(),
            media_group_id=None,
            chat=SimpleNamespace(title="Channel", first_name=None),
        )

        async def blocked_postprocess(*_args):
            started.set()
            await release.wait()

        old_running = media_downloader.app.is_running
        media_downloader.app.is_running = True
        with tempfile.TemporaryDirectory() as temp_dir:
            downloaded = Path(temp_dir) / "complete.mp4"
            downloaded.write_bytes(b"complete")
            with mock.patch.object(
                media_downloader,
                "_postprocess_queue",
                local_postprocess_queue,
            ), mock.patch.object(
                media_downloader,
                "_run_download_postprocess",
                side_effect=blocked_postprocess,
            ), mock.patch.object(
                media_downloader,
                "download_media",
                new=mock.AsyncMock(
                    return_value=(
                        DownloadStatus.SuccessDownload,
                        str(downloaded),
                    )
                ),
            ), mock.patch.object(
                media_downloader,
                "run_db",
                new=mock.AsyncMock(return_value=None),
            ), mock.patch.object(
                media_downloader.app, "set_download_id"
            ):
                postprocess = asyncio.create_task(
                    media_downloader.postprocess_worker(0)
                )
                result = await asyncio.wait_for(
                    media_downloader.download_task(object(), message, node),
                    timeout=0.2,
                )
                await asyncio.wait_for(started.wait(), timeout=0.2)
                self.assertEqual(result[0], DownloadStatus.SuccessDownload)
                self.assertFalse(postprocess.done())
                # A newer retry/status generation must survive completion of
                # the older asynchronous postprocess item.
                node.download_status[message.id] = DownloadStatus.Downloading
                release.set()
                await asyncio.wait_for(local_postprocess_queue.join(), timeout=0.2)
                self.assertEqual(
                    node.download_status[message.id], DownloadStatus.Downloading
                )
                media_downloader.app.is_running = False
                postprocess.cancel()
                await asyncio.gather(postprocess, return_exceptions=True)
        media_downloader.app.is_running = old_running

    async def test_persisted_retry_hydration_does_not_inflate_total(self):
        node = TaskNode("chat-retry")
        node.total_task = 9
        message = SimpleNamespace(id=88)
        with mock.patch.object(
            media_downloader,
            "add_download_task",
            new=mock.AsyncMock(return_value=True),
        ) as add_task:
            self.assertTrue(
                await media_downloader._hydrate_persisted_retry(message, node)
            )
        add_task.assert_awaited_once_with(message, node, count_task=False)
        self.assertEqual(node.total_task, 9)

    async def test_failed_retry_metadata_group_does_not_block_next_channel(self):
        first_node = TaskNode("chat-timeout")
        second_node = TaskNode("chat-healthy")
        healthy_message = SimpleNamespace(id=2, empty=False)
        client = SimpleNamespace(
            get_messages=mock.Mock(
                side_effect=[TimeoutError("metadata timeout"), object()]
            )
        )
        first_group = {
            "chat_id": "chat-timeout",
            "chat_config": SimpleNamespace(node=first_node),
            "tasks": [{"message_id": 1}],
        }
        second_group = {
            "chat_id": "chat-healthy",
            "chat_config": SimpleNamespace(node=second_node),
            "tasks": [{"message_id": 2}],
        }

        with mock.patch.object(
            media_downloader.rate_governor,
            "acquire",
            new=mock.AsyncMock(),
        ), mock.patch.object(
            media_downloader,
            "telegram_call",
            new=mock.AsyncMock(return_value=[healthy_message]),
        ), mock.patch.object(
            media_downloader,
            "run_db",
            new=mock.AsyncMock(return_value={"retry": True}),
        ) as run_db, mock.patch.object(
            media_downloader,
            "_hydrate_persisted_retry",
            new=mock.AsyncMock(return_value=True),
        ) as hydrate:
            first_count = await media_downloader._hydrate_retry_group(
                client, first_group
            )
            second_count = await media_downloader._hydrate_retry_group(
                client, second_group
            )

        self.assertEqual(first_count, 0)
        self.assertEqual(second_count, 1)
        run_db.assert_any_await(
            media_downloader.fail_task,
            "chat-timeout",
            1,
            "retry GetMessages timed out: metadata timeout",
        )
        hydrate.assert_awaited_once_with(healthy_message, second_node)

    async def test_slow_retry_metadata_channel_does_not_delay_healthy_channel(self):
        slow_started = asyncio.Event()
        release_slow = asyncio.Event()
        healthy_hydrated = asyncio.Event()

        async def hydrate_group(_client, group):
            if group["chat_id"] == "slow":
                slow_started.set()
                await release_slow.wait()
                return 0
            healthy_hydrated.set()
            return 1

        groups = [{"chat_id": "slow"}, {"chat_id": "healthy"}]
        with mock.patch.object(
            media_downloader,
            "_hydrate_retry_group",
            side_effect=hydrate_group,
        ):
            pending = asyncio.create_task(
                media_downloader._hydrate_retry_groups(object(), groups)
            )
            await asyncio.wait_for(slow_started.wait(), timeout=0.1)
            await asyncio.wait_for(healthy_hydrated.wait(), timeout=0.1)
            release_slow.set()
            self.assertEqual(await asyncio.wait_for(pending, timeout=0.1), 1)

    async def test_retry_scheduling_has_one_persisted_consumer(self):
        node = TaskNode("chat-retry-owner")
        node.total_task = 3
        message = SimpleNamespace(id=89)
        old_keep_alive = media_downloader.app.keep_service_alive
        media_downloader.app.keep_service_alive = True
        media_downloader._retry_wakeup.clear()
        try:
            with mock.patch.object(
                media_downloader.app.loop, "create_task"
            ) as create_task:
                media_downloader._schedule_retry(
                    message, node, {"retry": True, "delay": 60}
                )
            create_task.assert_not_called()
            self.assertTrue(media_downloader._retry_wakeup.is_set())
            self.assertEqual(node.total_task, 4)
        finally:
            media_downloader.app.keep_service_alive = old_keep_alive
            media_downloader._retry_wakeup.clear()

    async def test_telegram_timeout_does_not_cancel_inner_cleanup(self):
        completed = asyncio.Event()

        async def slow_request():
            await asyncio.sleep(0.15)
            completed.set()
            return "done"

        with self.assertRaises(TelegramRequestTimeout):
            await telegram_call(slow_request(), 0.01, "slow")
        await asyncio.wait_for(completed.wait(), timeout=0.3)

    async def test_session_restart_is_serialized(self):
        client = _FakeClient()
        session = _FakeSession()
        _instrument_media_session(client, 2, session)

        await asyncio.gather(session.restart(), session.restart())

        self.assertEqual(session.restart_calls, 1)

    async def test_transport_failure_quarantines_only_broken_session(self):
        client = _FakeClient()
        broken = _FakeSession(send_error=ConnectionError("connection lost"))
        healthy = _FakeSession()
        client._tmd_media_session_pools = {2: [broken, healthy]}
        client.media_sessions = {2: broken, (2, 1): healthy}
        _instrument_media_session(client, 2, broken)

        with self.assertRaises(ConnectionError):
            await broken.send("request")
        await asyncio.sleep(0.02)

        self.assertNotIn(broken, client._tmd_media_session_pools[2])
        self.assertIn(healthy, client._tmd_media_session_pools[2])
        self.assertEqual(broken.stop_calls, 1)

    async def test_timed_out_chunk_is_retried_instead_of_truncating_the_file(self):
        """One slow chunk must not end the transfer.

        Pyrogram's get_file() swallows a chunk error and simply stops yielding,
        so without a retry here the caller receives a short file. That is how
        multi-GB downloads ended as 'downloaded N bytes, expected M bytes'.
        """
        client = _FakeClient()
        session = _FlakySession(failures=2, error=TimeoutError("Request timed out"))
        client._tmd_media_session_pools = {2: [session]}
        client.media_sessions = {2: session}
        _instrument_media_session(client, 2, session)

        result = await session.invoke("request")

        self.assertEqual(result, "ok")
        self.assertEqual(session.send_calls, 3)
        # A retried timeout is not evidence of a broken connection.
        self.assertFalse(session._tmd_health_state["unhealthy"])
        self.assertIn(session, client._tmd_media_session_pools[2])

    async def test_chunk_retry_gives_up_after_the_attempt_budget(self):
        client = _FakeClient()
        session = _FlakySession(failures=99, error=TimeoutError("Request timed out"))
        client._tmd_media_session_pools = {2: [session]}
        client.media_sessions = {2: session}
        _instrument_media_session(client, 2, session)

        with self.assertRaises(TimeoutError):
            await session.invoke("request")

        self.assertEqual(
            session.send_calls, pyrogram_extension._MEDIA_INVOKE_ATTEMPTS
        )

    async def test_transport_failure_is_not_retried_on_the_same_connection(self):
        """A dead TCP connection must fail fast so another session serves the retry."""
        client = _FakeClient()
        session = _FlakySession(failures=99, error=ConnectionError("connection lost"))
        client._tmd_media_session_pools = {2: [session]}
        client.media_sessions = {2: session}
        _instrument_media_session(client, 2, session)

        with mock.patch(
            "module.pyrogram_extension._schedule_media_pool_growth",
            new=mock.AsyncMock(),
        ):
            with self.assertRaises(ConnectionError):
                await session.invoke("request")

        self.assertEqual(session.send_calls, 1)

    async def test_concurrent_chunk_retries_do_not_deadlock_the_connection(self):
        """Regression guard for why retries were disabled in the first place.

        Pyrogram's own retry path recurses into the patched invoke and would
        re-acquire the per-connection permit it already holds; two concurrent
        retrying requests then deadlock. Retrying outside the permit must not.
        """
        client = _FakeClient()
        session = _FlakySession(failures=4, error=TimeoutError("Request timed out"))
        client._tmd_media_session_pools = {2: [session]}
        client.media_sessions = {2: session}
        _instrument_media_session(client, 2, session)

        results = await asyncio.wait_for(
            asyncio.gather(
                session.invoke("a"), session.invoke("b"), return_exceptions=True
            ),
            timeout=10,
        )

        self.assertEqual(len(results), 2)
        for outcome in results:
            self.assertNotIsInstance(outcome, asyncio.TimeoutError)

    async def test_repeated_response_timeouts_do_not_destroy_media_session(self):
        client = _FakeClient()
        session = _FakeSession(send_error=TimeoutError("Request timed out"))
        client._tmd_media_session_pools = {2: [session]}
        client.media_sessions = {2: session}
        _instrument_media_session(client, 2, session)

        with mock.patch(
            "module.pyrogram_extension._schedule_media_pool_growth",
            new=mock.AsyncMock(),
        ):
            with self.assertRaises(TimeoutError):
                await session.send("request")
            await asyncio.sleep(0)
            self.assertIn(session, client._tmd_media_session_pools[2])

            with self.assertRaises(TimeoutError):
                await session.send("request")
            await asyncio.sleep(0.02)
        self.assertIn(session, client._tmd_media_session_pools[2])
        self.assertEqual(session.stop_calls, 0)

    async def test_pyrogram_internal_retry_stays_disabled(self):
        """Retries happen in the wrapper, never inside Pyrogram's invoke.

        Pyrogram retries by calling ``self.invoke`` recursively, which is the
        patched wrapper, so its retry would re-acquire the per-connection permit
        it already holds and deadlock. Each attempt must therefore still be
        issued with ``retries=0`` and the longer media RPC timeout, while the
        retry loop itself lives outside the permit (see isolated_invoke).
        """
        client = _FakeClient()
        session = _TransientTimeoutSession()
        client._tmd_media_session_pools = {2: [session]}
        client.media_sessions = {2: session}
        _instrument_media_session(client, 2, session)

        with self.assertRaises(TimeoutError):
            await session.invoke("request")

        attempts = pyrogram_extension._MEDIA_INVOKE_ATTEMPTS
        self.assertEqual(session.invoke_calls, attempts)
        # Every attempt disables Pyrogram's own recursive retry ...
        self.assertEqual(session.invoke_retries, [0] * attempts)
        # ... and keeps the longer media RPC timeout.
        self.assertEqual(session.invoke_timeouts, [45.0] * attempts)
        # A retried response timeout is not proof of a broken connection.
        self.assertIn(session, client._tmd_media_session_pools[2])

    async def test_evicted_media_session_schedules_immediate_replacement(self):
        client = _FakeClient()
        broken = _FakeSession()
        client._tmd_media_session_pools = {2: [broken]}
        client.media_sessions = {2: broken}

        with mock.patch(
            "module.pyrogram_extension._schedule_media_pool_growth",
            new=mock.AsyncMock(),
        ) as grow:
            await _evict_media_session(client, 2, broken)

        grow.assert_awaited_once_with(client, 2, broken.auth_key)
        self.assertEqual(client._tmd_media_session_pools[2], [])
        self.assertEqual(broken.stop_calls, 1)

    async def test_media_session_bounds_rpc_per_connection(self):
        client = _FakeClient()
        session = _ContendedSession()
        _instrument_media_session(client, 2, session)

        results = await asyncio.gather(
            session.invoke("one"),
            session.invoke("two"),
            session.invoke("three"),
        )

        self.assertEqual(results, ["ok", "ok", "ok"])
        self.assertEqual(session.max_active_invokes, 2)
        self.assertEqual(session.invoke_retries, [0, 0, 0])

    async def test_cancelled_rpc_releases_connection_permit(self):
        client = _FakeClient()
        session = _ContendedSession(block=True)
        _instrument_media_session(client, 2, session)

        first = asyncio.create_task(session.invoke("one"))
        await asyncio.wait_for(session.first_entered.wait(), timeout=0.1)
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        second = asyncio.create_task(session.invoke("two"))
        await asyncio.sleep(0.02)
        self.assertEqual(session.invoke_calls, 2)
        self.assertEqual(session.max_active_invokes, 1)
        session.release.set()
        self.assertEqual(await asyncio.wait_for(second, timeout=0.2), "ok")

    async def test_cancelled_media_send_retires_session_and_stops_inner_request(self):
        client = _FakeClient()
        session = _CancellableSendSession()
        client._tmd_media_session_pools = {2: [session]}
        client.media_sessions = {2: session}
        _instrument_media_session(client, 2, session)

        request = asyncio.create_task(session.invoke("request"))
        await asyncio.wait_for(session.started.wait(), timeout=0.1)
        request.cancel()
        await asyncio.gather(request, return_exceptions=True)
        await asyncio.sleep(0.02)

        self.assertTrue(session.cancelled.is_set())
        self.assertNotIn(session, client._tmd_media_session_pools[2])
        self.assertEqual(session.stop_calls, 1)

    async def test_bootstrap_warms_pool_without_blocking_first_session(self):
        client = _FakeClient()
        client._tmd_media_pool_size = 5
        built = []

        async def fake_build(_client, dc_id, auth_key, bootstrap):
            del _client, dc_id, auth_key, bootstrap
            session = _FakeSession()
            built.append(session)
            await asyncio.sleep(0)
            return session

        with mock.patch(
            "module.pyrogram_extension._build_media_session",
            side_effect=fake_build,
        ):
            first = await _get_pooled_media_session(client, 4)
            self.assertIs(first, built[0])
            tasks = list(getattr(client, "_tmd_media_session_tasks", ()))
            if tasks:
                await asyncio.gather(*tasks)

        self.assertEqual(len(client._tmd_media_session_pools[4]), 5)
        self.assertEqual(client._tmd_media_session_building[4], 0)

    async def test_partial_file_resumes_from_last_full_chunk(self):
        chunk = 1024 * 1024
        expected = (b"a" * chunk) + (b"b" * chunk) + b"tail"
        client = _FakeDownloadClient(expected)

        with tempfile.TemporaryDirectory() as temp_dir:
            packet = (
                object(), temp_dir, "video.mp4", False, len(expected), None, ()
            )
            client.fail_after_chunks = 1
            with self.assertRaises(ConnectionError):
                await _resumable_handle_download(client, packet)

            temp_path = Path(temp_dir) / "video.mp4.temp"
            self.assertEqual(temp_path.stat().st_size, chunk)

            client.fail_after_chunks = None
            result = await _resumable_handle_download(client, packet)

            self.assertEqual(client.offsets, [0, 1])
            self.assertEqual(client.progress_args, [(0,), (chunk,)])
            self.assertEqual(Path(result).read_bytes(), expected)
            self.assertFalse(temp_path.exists())

    async def test_quarantined_session_midstream_does_not_truncate_the_file(self):
        """A session quarantined by ANOTHER file must not kill this transfer.

        get_file swallows the error and stops yielding, so this arrives as a
        short file. Re-entering get_file picks a fresh pooled session and
        resumes from the chunk boundary already on disk. Measured 2026-08-11:
        164 real connection failures killed 2350 in-flight files without this.
        """
        chunk = 1024 * 1024
        expected = (b"a" * chunk) + (b"b" * chunk) + (b"c" * chunk) + b"tail"
        # First two calls stop after one chunk; the third completes.
        client = _SilentTruncateClient(expected, stop_after=1, flaky_rounds=2)

        with tempfile.TemporaryDirectory() as temp_dir:
            packet = (
                object(), temp_dir, "video.mp4", False, len(expected), None, ()
            )
            result = await _resumable_handle_download(client, packet)

            self.assertEqual(Path(result).read_bytes(), expected)
            # Each round resumed at the next chunk boundary rather than 0.
            self.assertEqual(client.offsets, [0, 1, 2])
            self.assertFalse((Path(temp_dir) / "video.mp4.temp").exists())

    async def test_zero_progress_round_stops_instead_of_spinning(self):
        """An unavailable file must fall back to persisted backoff, not loop."""
        client = _NoProgressClient()
        with tempfile.TemporaryDirectory() as temp_dir:
            packet = (
                object(), temp_dir, "video.mp4", False, 4 * 1024 * 1024, None, ()
            )
            with self.assertRaises(IOError) as caught:
                await _resumable_handle_download(client, packet)

            self.assertIn("expected", str(caught.exception))
            # One productive-less round only: no spinning up to the bound.
            self.assertEqual(client.calls, 1)

    async def test_resume_rounds_are_bounded(self):
        """A stream that always truncates gives up and reports a short file."""
        chunk = 1024 * 1024
        expected = b"".join(bytes([97 + i]) * chunk for i in range(12))
        # Never completes: every call stops after one chunk.
        client = _SilentTruncateClient(expected, stop_after=1, flaky_rounds=99)

        with tempfile.TemporaryDirectory() as temp_dir:
            packet = (
                object(), temp_dir, "video.mp4", False, len(expected), None, ()
            )
            with self.assertRaises(IOError):
                await _resumable_handle_download(client, packet)

            # 1 initial + _RESUME_ROUNDS retries, then hand back to the
            # persisted retry — and the partial keeps every banked chunk.
            self.assertEqual(
                client.calls, pyrogram_extension._RESUME_ROUNDS + 1
            )
            temp_path = Path(temp_dir) / "video.mp4.temp"
            self.assertEqual(
                temp_path.stat().st_size,
                (pyrogram_extension._RESUME_ROUNDS + 1) * chunk,
            )

    async def test_resumed_bytes_are_not_reported_as_new_download_speed(self):
        node = SimpleNamespace(
            chat_id="chat-speed", task_id="task", is_stop_transmission=False
        )
        client = mock.Mock()
        resume_offset = 5 * 1024 * 1024
        new_bytes = 1024 * 1024
        with download_stat._download_result_lock:
            download_stat._download_result.clear()
            download_stat._total_download_speed = 0
            download_stat._total_download_size = 0
            download_stat._last_download_time = 100.0
        with mock.patch("module.download_stat.time.time", return_value=101.0):
            await download_stat.update_download_status(
                resume_offset + new_bytes,
                20 * 1024 * 1024,
                99,
                "resume.mp4",
                100.0,
                node,
                client,
                resume_offset,
            )
        self.assertEqual(download_stat._total_download_speed, new_bytes)
        self.assertEqual(
            download_stat._download_result["chat-speed"][99]["download_speed"],
            new_bytes,
        )
        download_stat.remove_download_result("chat-speed", 99)

    async def test_empty_live_cache_resets_stale_speed_and_pending_bytes(self):
        with download_stat._download_result_lock:
            download_stat._download_result.clear()
            download_stat._total_download_speed = 2 * 1024 * 1024 * 1024
            download_stat._total_download_size = 512 * 1024 * 1024
            download_stat._last_download_time = time.time()

        self.assertEqual(download_stat.get_total_download_speed(), 0)
        self.assertEqual(download_stat._total_download_size, 0)

    async def test_transport_probe_clears_cooldown_and_signals_recovery(self):
        speed_governor.configure(
            enabled=True,
            base=45,
            ceiling=45,
            floor=6,
            stall_threshold=2,
            stall_window=60,
            cooldown_seconds=60,
        )
        speed_governor.record_first_byte_stall()
        speed_governor.record_first_byte_stall()
        self.assertGreater(speed_governor.cooldown_remaining(), 0)
        await asyncio.wait_for(speed_governor.wait_for_turn(0), timeout=0.1)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(speed_governor.wait_for_turn(1), timeout=0.01)
        speed_governor.arm_transport_probe("chat-recovery")
        self.assertTrue(speed_governor.record_transfer_progress("chat-recovery"))
        self.assertEqual(speed_governor.cooldown_remaining(), 0)
        self.assertEqual(
            speed_governor.consume_recovery_signal(), "chat-recovery"
        )
        self.assertIsNone(speed_governor.consume_recovery_signal())


class StorageAndFileTests(unittest.TestCase):
    """Fast local checks protect NAS and exact-file semantics."""

    def test_storage_probe_is_cached(self):
        storage_health.clear_storage_cache()
        with mock.patch(
            "module.storage_health.path_is_writable", return_value=True
        ) as writable:
            self.assertTrue(storage_health.storage_is_ready("/tmp/example"))
            self.assertTrue(storage_health.storage_is_ready("/tmp/example"))
        self.assertEqual(writable.call_count, 1)

    def test_existing_file_requires_exact_known_size(self):
        self.assertTrue(media_downloader._existing_file_is_complete(100, 100))
        self.assertFalse(media_downloader._existing_file_is_complete(99, 100))
        self.assertFalse(media_downloader._existing_file_is_complete(101, 100))
        self.assertTrue(media_downloader._existing_file_is_complete(1, 0))
        self.assertFalse(media_downloader._existing_file_is_complete(0, 0))

    def test_publish_uses_atomic_replace(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "partial"
            target = Path(temp_dir) / "final"
            source.write_bytes(b"complete")
            target.write_bytes(b"old")

            media_downloader._move_to_download_path(str(source), str(target))

            self.assertEqual(target.read_bytes(), b"complete")
            self.assertFalse(source.exists())


class _FakeConnection:
    is_connected = True


class _FakeSession:
    def __init__(self, send_error=None):
        self.connection = _FakeConnection()
        self.is_started = asyncio.Event()
        self.is_started.set()
        self.auth_key = object()
        self.restart_calls = 0
        self.stop_calls = 0
        self.send_error = send_error

    async def restart(self):
        self.restart_calls += 1
        await asyncio.sleep(0.01)

    async def send(self, *args, **kwargs):
        del args, kwargs
        if self.send_error:
            raise self.send_error
        return "ok"

    async def invoke(self, *args, **kwargs):
        return await self.send(*args, **kwargs)

    async def stop(self):
        self.stop_calls += 1


class _FlakySession(_FakeSession):
    """Fail the first ``failures`` sends, then succeed. Counts real send calls."""

    def __init__(self, failures: int, error: BaseException):
        super().__init__()
        self.failures = int(failures)
        self.error = error
        self.send_calls = 0

    async def send(self, *args, **kwargs):
        del args, kwargs
        self.send_calls += 1
        if self.send_calls <= self.failures:
            raise self.error
        return "ok"


class _CancellableSendSession(_FakeSession):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def send(self, *args, **kwargs):
        del args, kwargs
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class _FakeClient:
    def __init__(self):
        self.media_sessions_lock = asyncio.Lock()
        self.media_sessions = {}


class _ContendedSession(_FakeSession):
    def __init__(self, block=False):
        super().__init__()
        self.block = block
        self.release = asyncio.Event()
        self.first_entered = asyncio.Event()
        self.active_invokes = 0
        self.max_active_invokes = 0
        self.invoke_calls = 0
        self.invoke_retries = []

    async def invoke(self, *args, **kwargs):
        del args
        self.invoke_retries.append(kwargs.get("retries"))
        self.invoke_calls += 1
        self.active_invokes += 1
        self.max_active_invokes = max(
            self.max_active_invokes, self.active_invokes
        )
        self.first_entered.set()
        try:
            if self.block:
                await self.release.wait()
            else:
                await asyncio.sleep(0.01)
            return "ok"
        finally:
            self.active_invokes -= 1


class _TransientTimeoutSession(_FakeSession):
    def __init__(self):
        super().__init__()
        self.invoke_calls = 0
        self.invoke_retries = []
        self.invoke_timeouts = []

    async def invoke(self, *args, **kwargs):
        del args
        self.invoke_calls += 1
        self.invoke_retries.append(kwargs.get("retries"))
        self.invoke_timeouts.append(kwargs.get("timeout"))
        raise TimeoutError("Request timed out")


class _SilentTruncateClient:
    """Mimics the real Client.get_file: swallows errors and stops yielding.

    Production get_file ends with `except Exception: log.exception(e)` and then
    falls off the function, so a session quarantined mid-transfer surfaces as a
    SHORT FILE, never as an exception. `stop_after` chunks are emitted per call
    for the first `flaky_rounds` calls, then the stream completes normally.
    """

    def __init__(self, payload, stop_after, flaky_rounds):
        self.payload = payload
        self.stop_after = stop_after
        self.flaky_rounds = flaky_rounds
        self.offsets = []
        self.calls = 0

    async def get_file(self, _fid, _size, _limit, offset, _progress, _pargs):
        self.calls += 1
        self.offsets.append(offset)
        truncate_this_round = self.calls <= self.flaky_rounds
        start = offset * 1024 * 1024
        emitted = 0
        while start < len(self.payload):
            end = min(start + 1024 * 1024, len(self.payload))
            yield self.payload[start:end]
            emitted += 1
            start = end
            if truncate_this_round and emitted >= self.stop_after:
                return  # silent stop, exactly like the real get_file


class _NoProgressClient:
    """get_file that yields nothing at all — the file itself is unavailable."""

    def __init__(self):
        self.calls = 0

    async def get_file(self, _fid, _size, _limit, offset, _progress, _pargs):
        self.calls += 1
        return
        yield  # pragma: no cover - makes this an async generator


class _FakeDownloadClient:
    def __init__(self, payload):
        self.payload = payload
        self.fail_after_chunks = None
        self.offsets = []
        self.progress_args = []

    async def get_file(
        self, _file_id, _file_size, _limit, offset, _progress, _progress_args
    ):
        self.offsets.append(offset)
        self.progress_args.append(_progress_args)
        start = offset * 1024 * 1024
        emitted = 0
        while start < len(self.payload):
            end = min(start + 1024 * 1024, len(self.payload))
            yield self.payload[start:end]
            emitted += 1
            start = end
            if self.fail_after_chunks == emitted:
                raise ConnectionError("connection lost")


class TempPathConfigTests(unittest.TestCase):
    """os.replace only avoids a copy when temp and final share a MOUNT.

    On the production NAS these were two bind mounts of different btrfs
    subvolumes: st_dev matched after remounting, yet rename() still returned
    EXDEV, so every finished file was copied and fsynced instead of renamed.
    Making the temp directory configurable is what allows it to be placed
    inside save_path's mount.
    """

    def test_temp_save_path_is_configurable(self):
        app = Application("config.yaml", "data.yaml", "media_downloader")
        app.assign_config(
            {
                "api_id": "1",
                "api_hash": "h",
                "media_types": ["video"],
                "file_formats": {"video": ["all"]},
                "save_path": "/srv/media",
                "temp_save_path": "/srv/media/.tmd-temp",
            }
        )
        self.assertEqual(app.save_path, "/srv/media")
        self.assertEqual(app.temp_save_path, "/srv/media/.tmd-temp")

    def test_temp_save_path_defaults_to_local_temp(self):
        app = Application("config.yaml", "data.yaml", "media_downloader")
        default_temp = app.temp_save_path
        app.assign_config(
            {
                "api_id": "1",
                "api_hash": "h",
                "media_types": ["video"],
                "file_formats": {"video": ["all"]},
                "save_path": "/srv/media",
            }
        )
        self.assertEqual(app.temp_save_path, default_temp)


class OrphanedClaimTests(unittest.TestCase):
    """A worker that blocks forever must not strand its task until restart.

    2026-08-14: one task sat in 'downloading' for 15.5 hours because
    `_refresh_reference_if_stale` awaited `fetch_message` with no timeout —
    the claim landed in SQLite, then the coroutine never returned, and
    `recover_interrupted_tasks()` only runs at startup.
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "tasks.db"
        self.env = mock.patch.dict(
            os.environ,
            {"TMD_TASK_DB": str(self.database), "TMD_HISTORY_DB": str(self.database)},
            clear=False,
        )
        self.env.start()
        download_tasks._SCHEMA_READY.clear()
        download_history._SCHEMA_READY.clear()

    def tearDown(self):
        self.env.stop()
        download_tasks._SCHEMA_READY.clear()
        download_history._SCHEMA_READY.clear()
        self.temp_dir.cleanup()

    def _age_claim(self, chat_id, message_id, seconds):
        old = (
            datetime.now(timezone.utc) - timedelta(seconds=seconds)
        ).isoformat(timespec="seconds")
        with download_tasks._connect() as connection:
            connection.execute(
                "UPDATE download_tasks SET updated_at = ? "
                "WHERE chat_id = ? AND message_id = ?",
                (old, str(chat_id), int(message_id)),
            )

    def test_unowned_stale_claim_is_returned_to_the_retry_queue(self):
        download_tasks.queue_task("chat-orphan", 1)
        download_tasks.start_task("chat-orphan", 1)
        self._age_claim("chat-orphan", 1, 3600)

        released = download_tasks.release_orphaned_claims(set(), 900)

        self.assertEqual(released, 1)
        state = download_tasks.task_claim_state("chat-orphan", 1)
        self.assertEqual(state["status"], "retry_requested")

    def test_claim_held_by_a_live_worker_is_never_reclaimed(self):
        """A legitimate multi-GB transfer may run for hours — do not touch it."""
        download_tasks.queue_task("chat-live", 2)
        download_tasks.start_task("chat-live", 2)
        self._age_claim("chat-live", 2, 24 * 3600)

        released = download_tasks.release_orphaned_claims({("chat-live", 2)}, 900)

        self.assertEqual(released, 0)
        state = download_tasks.task_claim_state("chat-live", 2)
        self.assertEqual(state["status"], "downloading")

    def test_recent_claim_is_left_alone(self):
        """Covers the window between claiming a row and registering ownership."""
        download_tasks.queue_task("chat-fresh", 3)
        download_tasks.start_task("chat-fresh", 3)

        released = download_tasks.release_orphaned_claims(set(), 900)

        self.assertEqual(released, 0)
        state = download_tasks.task_claim_state("chat-fresh", 3)
        self.assertEqual(state["status"], "downloading")

    def test_attempts_are_preserved_so_backoff_is_not_reset(self):
        download_tasks.queue_task("chat-attempts", 4)
        for _ in range(3):
            download_tasks.start_task("chat-attempts", 4)
            download_tasks.fail_task("chat-attempts", 4, "connection lost")
            download_tasks.queue_task("chat-attempts", 4)
        download_tasks.start_task("chat-attempts", 4)
        self._age_claim("chat-attempts", 4, 3600)
        with download_tasks._connect() as connection:
            before = connection.execute(
                "SELECT attempts FROM download_tasks "
                "WHERE chat_id = 'chat-attempts' AND message_id = 4"
            ).fetchone()["attempts"]

        download_tasks.release_orphaned_claims(set(), 900)

        with download_tasks._connect() as connection:
            after = connection.execute(
                "SELECT attempts FROM download_tasks "
                "WHERE chat_id = 'chat-attempts' AND message_id = 4"
            ).fetchone()["attempts"]
        self.assertEqual(after, before)


class ReferenceRefreshTimeoutTests(unittest.IsolatedAsyncioTestCase):
    """The refresh runs AFTER the task is claimed, so it must be bounded.

    Unbounded, a hung Telegram call holds status='downloading' forever: the
    worker never reaches its failure path and only a restart frees the row.
    """

    async def test_hung_reference_refresh_falls_back_instead_of_hanging(self):
        message = SimpleNamespace(id=4242)
        node = SimpleNamespace(chat_id="chat-refresh")

        async def never_returns(*_args, **_kwargs):
            await asyncio.Event().wait()

        with mock.patch.object(
            media_downloader, "run_db", new=mock.AsyncMock(return_value=99999)
        ), mock.patch.object(
            media_downloader, "fetch_message", new=never_returns
        ), mock.patch.object(
            media_downloader, "METADATA_REQUEST_TIMEOUT", 0.2
        ):
            result = await asyncio.wait_for(
                media_downloader._refresh_reference_if_stale(None, message, node),
                timeout=5,
            )

        # Falls back to the un-refreshed message rather than blocking forever.
        self.assertIs(result, message)


class MediaProxyPoolTests(unittest.TestCase):
    """One proxy exit's route to Telegram caps the account at ~7-8 MB/s.

    Measured 2026-08-15: 150 workers moved no more bytes than 24, while the
    same line carried 33+ MB/s to other destinations. Spreading MEDIA sessions
    over several exits is the only way past that route, so the rotation must
    (a) actually alternate, (b) never move the main MTProto connection, and
    (c) be a no-op with a single exit.
    """

    def _client(self):
        calls = []

        def base_factory(**kwargs):
            calls.append(kwargs)
            return object()

        client = SimpleNamespace(
            connection_factory=base_factory,
            proxy={"scheme": "socks5", "hostname": "telegram-proxy", "port": 1080},
        )
        return client, calls

    def test_media_connections_alternate_between_exits(self):
        client, calls = self._client()
        exits = [
            {"scheme": "socks5", "hostname": "telegram-proxy", "port": 1080},
            {"scheme": "socks5", "hostname": "telegram-proxy2", "port": 1080},
        ]
        self.assertEqual(pyrogram_extension.set_media_proxy_pool(client, exits), 2)

        for _ in range(6):
            client.connection_factory(media=True, proxy=client.proxy)

        hosts = [c["proxy"]["hostname"] for c in calls]
        self.assertEqual(hosts, ["telegram-proxy", "telegram-proxy2"] * 3)

    def test_main_connection_never_rotates(self):
        """Auth and metadata must stay on the primary exit."""
        client, calls = self._client()
        exits = [
            {"scheme": "socks5", "hostname": "telegram-proxy", "port": 1080},
            {"scheme": "socks5", "hostname": "telegram-proxy2", "port": 1080},
        ]
        pyrogram_extension.set_media_proxy_pool(client, exits)

        for _ in range(4):
            client.connection_factory(media=False, proxy=client.proxy)

        for call in calls:
            self.assertEqual(call["proxy"]["hostname"], "telegram-proxy")

    def test_single_exit_is_a_no_op(self):
        client, _ = self._client()
        original = client.connection_factory
        self.assertEqual(
            pyrogram_extension.set_media_proxy_pool(
                client, [{"scheme": "socks5", "hostname": "telegram-proxy", "port": 1080}]
            ),
            1,
        )
        self.assertIs(client.connection_factory, original)

    def test_empty_pool_is_a_no_op(self):
        client, _ = self._client()
        original = client.connection_factory
        self.assertEqual(pyrogram_extension.set_media_proxy_pool(client, []), 0)
        self.assertIs(client.connection_factory, original)

    def test_reapplying_does_not_stack_wrappers(self):
        """Repeated configuration must not wrap the wrapper each time."""
        client, calls = self._client()
        exits = [
            {"scheme": "socks5", "hostname": "a", "port": 1},
            {"scheme": "socks5", "hostname": "b", "port": 2},
        ]
        pyrogram_extension.set_media_proxy_pool(client, exits)
        pyrogram_extension.set_media_proxy_pool(client, exits)

        for _ in range(4):
            client.connection_factory(media=True, proxy=client.proxy)

        # Each call reaches the ORIGINAL factory exactly once.
        self.assertEqual(len(calls), 4)
        self.assertEqual([c["proxy"]["hostname"] for c in calls], ["a", "b", "a", "b"])


class _RangeClient:
    """get_file that honours the (limit, offset) chunk range contract.

    Real Pyrogram computes `total = abs(limit) or (1<<31)-1` and starts at
    `offset * 1 MiB`, so a range request must yield exactly `limit` chunks (or
    fewer at EOF). The parallel downloader depends on that contract; a fake
    that ignored `limit` would let a broken implementation pass.
    """

    CHUNK = 1024 * 1024

    def __init__(self, payload, fail_on_offsets=(), short_on_offsets=()):
        self.payload = payload
        self.fail_on_offsets = set(fail_on_offsets)
        self.short_on_offsets = set(short_on_offsets)
        self.calls = []
        self.concurrent = 0
        self.max_concurrent = 0

    async def get_file(self, _fid, _size, limit, offset, _progress, _pargs):
        self.calls.append((offset, limit))
        self.concurrent += 1
        self.max_concurrent = max(self.max_concurrent, self.concurrent)
        try:
            if offset in self.fail_on_offsets:
                raise OSError(f"transport failure at chunk {offset}")
            start = offset * self.CHUNK
            emitted = 0
            while start < len(self.payload) and emitted < (limit or 1 << 31):
                end = min(start + self.CHUNK, len(self.payload))
                await asyncio.sleep(0)  # let sibling segments interleave
                yield self.payload[start:end]
                emitted += 1
                start = end
                if offset in self.short_on_offsets and emitted >= 1:
                    return  # silent short stream, like the real get_file
        finally:
            self.concurrent -= 1


class ParallelChunkDownloadTests(unittest.TestCase):
    """One file fetched as N concurrent chunk ranges.

    Pyrogram is strictly sequential per file (one 1 MiB chunk per round trip),
    so a single file is latency-bound. These tests pin the correctness
    properties that make splitting it safe: exact bytes, contiguous resume,
    and no partial window surviving a failure.
    """

    def setUp(self):
        self._original = pyrogram_extension._STREAMS_PER_FILE
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        pyrogram_extension.set_download_streams_per_file(self._original)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _download(self, client, payload, name="f.bin", progress=None):
        packet = (
            "fid",
            self.tmp,
            name,
            False,
            len(payload),
            progress,
            (),
        )
        return asyncio.run(
            pyrogram_extension._resumable_handle_download(client, packet)
        )

    def test_file_is_byte_exact_across_many_windows(self):
        """The whole point: same bytes as the sequential path, in any order."""
        pyrogram_extension.set_download_streams_per_file(4)
        payload = os.urandom(int(9.5 * 1024 * 1024))  # unaligned tail on purpose
        client = _RangeClient(payload)
        path = self._download(client, payload)
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), payload)

    def test_segments_actually_run_concurrently(self):
        """A serialised implementation would still pass the byte check."""
        pyrogram_extension.set_download_streams_per_file(4)
        payload = os.urandom(16 * 1024 * 1024)
        client = _RangeClient(payload)
        self._download(client, payload)
        self.assertGreater(client.max_concurrent, 1)

    def test_ranges_do_not_overlap_or_leave_gaps(self):
        pyrogram_extension.set_download_streams_per_file(3)
        payload = os.urandom(12 * 1024 * 1024)
        client = _RangeClient(payload)
        self._download(client, payload)
        covered = []
        for offset, limit in client.calls:
            covered.extend(range(offset, offset + limit))
        self.assertEqual(sorted(covered), list(range(12)))

    def test_failed_window_leaves_only_contiguous_bytes(self):
        """The resume invariant: no hole may survive on disk.

        Resume uses the file SIZE as the watermark, so a window that failed
        after some segments landed must be truncated away entirely — otherwise
        the next attempt resumes past a gap and silently corrupts the file.
        """
        pyrogram_extension.set_download_streams_per_file(4)
        payload = os.urandom(32 * 1024 * 1024)
        client = _RangeClient(payload, fail_on_offsets={8})
        with self.assertRaises(OSError):
            self._download(client, payload)
        temp = os.path.join(self.tmp, "f.bin.temp")
        size = os.path.getsize(temp)
        with open(temp, "rb") as handle:
            self.assertEqual(handle.read(), payload[:size])
        self.assertEqual(size % (1024 * 1024), 0)

    def test_resume_completes_the_file_after_a_failure(self):
        pyrogram_extension.set_download_streams_per_file(4)
        payload = os.urandom(32 * 1024 * 1024)
        with self.assertRaises(OSError):
            self._download(_RangeClient(payload, fail_on_offsets={8}), payload)
        path = self._download(_RangeClient(payload), payload)
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), payload)

    def test_progress_is_absolute_and_monotonic(self):
        """The stall watchdog cancels a transfer whose progress stops rising."""
        pyrogram_extension.set_download_streams_per_file(4)
        payload = os.urandom(16 * 1024 * 1024)
        seen = []

        async def progress(current, total, *_args):
            seen.append((current, total))

        self._download(_RangeClient(payload), payload, progress=progress)
        self.assertTrue(seen)
        self.assertEqual(seen[-1][0], len(payload))
        self.assertEqual(seen, sorted(seen))
        self.assertTrue(all(total == len(payload) for _c, total in seen))

    def test_single_stream_keeps_the_sequential_path(self):
        """streams=1 must not change behaviour at all."""
        pyrogram_extension.set_download_streams_per_file(1)
        payload = os.urandom(4 * 1024 * 1024)
        client = _RangeClient(payload)
        path = self._download(client, payload)
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), payload)
        # The sequential path asks for an unbounded range from one offset.
        self.assertEqual([limit for _o, limit in client.calls], [0])

    def test_unknown_size_falls_back_to_sequential(self):
        """Ranges cannot be split without a total size."""
        pyrogram_extension.set_download_streams_per_file(4)
        payload = os.urandom(3 * 1024 * 1024)
        client = _RangeClient(payload)
        packet = ("fid", self.tmp, "u.bin", False, 0, None, ())
        asyncio.run(pyrogram_extension._resumable_handle_download(client, packet))
        self.assertEqual([limit for _o, limit in client.calls], [0])

    def test_silently_short_window_does_not_spin_forever(self):
        """get_file swallows errors; a short window must hit a bounded retry."""
        pyrogram_extension.set_download_streams_per_file(2)
        payload = os.urandom(16 * 1024 * 1024)
        client = _RangeClient(payload, short_on_offsets={0})
        with self.assertRaises(IOError):
            self._download(client, payload)
        self.assertLessEqual(
            len(client.calls),
            2 * (pyrogram_extension._RESUME_ROUNDS + 2),
        )


class ChannelUpdateCheckTests(unittest.TestCase):
    """「检查更新」must re-open finished channels and nothing else.

    A channel that finishes is retired with `enabled = 0`, and
    `channel_registry_worker` only ever reads enabled rows — so bumping
    `config_revision` alone is silently ignored. Both fields have to move, and
    the operator's paused channels must not be dragged back into downloading
    as a side effect.
    """

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "tasks.db"
        self.env = mock.patch.dict(
            os.environ,
            {"TMD_TASK_DB": str(self.database), "TMD_HISTORY_DB": str(self.database)},
            clear=False,
        )
        self.env.start()
        download_tasks._SCHEMA_READY.clear()
        download_history._SCHEMA_READY.clear()

    def tearDown(self):
        self.env.stop()
        download_tasks._SCHEMA_READY.clear()
        download_history._SCHEMA_READY.clear()
        self.temp_dir.cleanup()

    def _make_channel(self, chat_id, enabled, scan_status, cursor=500):
        download_tasks.upsert_channel_config({"chat_id": chat_id})
        with download_tasks._connect() as connection:
            connection.execute(
                "UPDATE download_channel_state SET enabled = ?, scan_status = ?,"
                " last_read_message_id = ? WHERE chat_id = ?",
                (1 if enabled else 0, scan_status, cursor, str(chat_id)),
            )

    def _row(self, chat_id):
        with download_tasks._connect() as connection:
            return connection.execute(
                "SELECT enabled, paused, config_revision, last_read_message_id"
                " FROM download_channel_state WHERE chat_id = ?",
                (str(chat_id),),
            ).fetchone()

    def test_finished_channel_is_re_enabled_and_revision_bumped(self):
        """Both must move: the scanner ignores disabled rows entirely."""
        self._make_channel("-1001000000001", enabled=False, scan_status="completed")
        before = self._row("-1001000000001")["config_revision"]

        result = download_tasks.refresh_completed_channels(["-1001000000001"])

        self.assertEqual(result["refreshed"], ["-1001000000001"])
        after = self._row("-1001000000001")
        self.assertEqual(after["enabled"], 1)
        self.assertGreater(after["config_revision"], before)

    def test_scan_cursor_is_preserved_so_the_rescan_is_incremental(self):
        """Resetting the cursor would re-read the channel's whole history."""
        self._make_channel("-1001000000002", enabled=False, scan_status="completed", cursor=39658)
        download_tasks.refresh_completed_channels(["-1001000000002"])
        self.assertEqual(self._row("-1001000000002")["last_read_message_id"], 39658)

    def test_paused_channel_is_not_resumed(self):
        """Checking for updates must never restart a deliberately paused channel."""
        self._make_channel("-1001000000011", enabled=True, scan_status="completed")
        with download_tasks._connect() as connection:
            connection.execute(
                "UPDATE download_channel_state SET paused = 1 WHERE chat_id = ?",
                ("-1001000000011",),
            )

        result = download_tasks.refresh_completed_channels(["-1001000000011"])

        self.assertEqual(result["refreshed"], [])
        self.assertEqual(result["skipped"], ["-1001000000011"])
        self.assertEqual(self._row("-1001000000011")["paused"], 1)

    def test_unfinished_channel_is_skipped(self):
        self._make_channel("-1001000000021", enabled=False, scan_status="scanning")
        result = download_tasks.refresh_completed_channels(["-1001000000021"])
        self.assertEqual(result["skipped"], ["-1001000000021"])
        self.assertEqual(self._row("-1001000000021")["enabled"], 0)

    def test_unknown_channel_is_reported_not_created(self):
        result = download_tasks.refresh_completed_channels(["-1001000000099"])
        self.assertEqual(result["missing"], ["-1001000000099"])
        self.assertIsNone(self._row("-1001000000099"))

    def test_duplicate_ids_are_collapsed(self):
        self._make_channel("-1001000000003", enabled=False, scan_status="completed")
        before = self._row("-1001000000003")["config_revision"]
        result = download_tasks.refresh_completed_channels(["-1001000000003", "-1001000000003", " -1001000000003 "])
        self.assertEqual(result["refreshed"], ["-1001000000003"])
        self.assertEqual(self._row("-1001000000003")["config_revision"], before + 1)

    def test_mixed_selection_refreshes_only_the_finished_ones(self):
        """The realistic case: the operator selects a page of rows."""
        self._make_channel("-1001000000004", enabled=False, scan_status="completed")
        self._make_channel("-1001000000012", enabled=True, scan_status="scanning")
        self._make_channel("-1001000000022", enabled=False, scan_status="scanning")

        result = download_tasks.refresh_completed_channels(["-1001000000004", "-1001000000012", "-1001000000022"])

        self.assertEqual(result["refreshed"], ["-1001000000004"])
        self.assertEqual(sorted(result["skipped"]), ["-1001000000012", "-1001000000022"])
        self.assertEqual(self._row("-1001000000012")["enabled"], 1)
        self.assertEqual(self._row("-1001000000022")["enabled"], 0)

    def test_empty_selection_is_a_no_op(self):
        self.assertEqual(
            download_tasks.refresh_completed_channels([]),
            {"refreshed": [], "skipped": [], "missing": []},
        )
