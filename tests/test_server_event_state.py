from __future__ import annotations

import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import server


class RunLogPersistenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.previous = {
            "run_log": server._run_log,
            "dirty": server._run_log_dirty,
            "flush_task": server._run_log_flush_task,
            "flush_lock": server._run_log_flush_lock,
            "path": server.RUN_LOG_PATH,
            "scheduler_task": server._scheduler_task,
            "downloader": server._dl,
        }
        server._run_log = []
        server._run_log_dirty = 0
        server._run_log_flush_task = None
        server._run_log_flush_lock = asyncio.Lock()
        server.RUN_LOG_PATH = Path(self.temp_dir.name) / "run_log.json"
        server._scheduler_task = None
        server._dl = None

    async def asyncTearDown(self) -> None:
        task = server._run_log_flush_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        server._run_log = self.previous["run_log"]
        server._run_log_dirty = self.previous["dirty"]
        server._run_log_flush_task = self.previous["flush_task"]
        server._run_log_flush_lock = self.previous["flush_lock"]
        server.RUN_LOG_PATH = self.previous["path"]
        server._scheduler_task = self.previous["scheduler_task"]
        server._dl = self.previous["downloader"]
        self.temp_dir.cleanup()

    async def test_high_frequency_events_share_one_debounced_write(self) -> None:
        writes = []

        def record_write(path, data, **_kwargs) -> None:
            writes.append((path, copy.deepcopy(data)))

        with (
            mock.patch.object(server, "_RUN_LOG_FLUSH_DELAY", 0),
            mock.patch("automate._atomic_write_json", side_effect=record_write),
        ):
            server._log_event({"kind": "edit", "id": 1})
            server._log_event({"kind": "edit", "id": 2})
            server._log_event({"kind": "edit", "id": 3})
            await server._run_log_flush_task

        self.assertEqual(len(writes), 1)
        self.assertEqual([item["id"] for item in writes[0][1]], [1, 2, 3])
        self.assertEqual(server._run_log_dirty, 0)

    async def test_failed_write_stays_dirty_for_forced_retry(self) -> None:
        server._run_log.append({"kind": "pending"})
        server._run_log_dirty = 1

        with mock.patch("automate._atomic_write_json", side_effect=OSError("disk busy")):
            with self.assertRaisesRegex(OSError, "disk busy"):
                await server.flush_run_log()

        self.assertEqual(server._run_log_dirty, 1)

        await server.flush_run_log()

        self.assertEqual(server._run_log_dirty, 0)
        persisted = json.loads(server.RUN_LOG_PATH.read_text(encoding="utf-8"))
        self.assertEqual(persisted, [{"kind": "pending"}])

    async def test_event_added_during_write_is_flushed_in_next_snapshot(self) -> None:
        server._run_log.append({"kind": "first"})
        server._run_log_dirty = 1
        snapshots = []

        async def write_and_append(_writer, _path, data, **_kwargs) -> None:
            snapshots.append(copy.deepcopy(data))
            if len(snapshots) == 1:
                server._run_log.append({"kind": "during_write"})
                server._run_log_dirty += 1

        with mock.patch.object(server.asyncio, "to_thread", new=write_and_append):
            await server.flush_run_log()

        self.assertEqual(len(snapshots), 2)
        self.assertEqual(
            [item["kind"] for item in snapshots[-1]],
            ["first", "during_write"],
        )
        self.assertEqual(server._run_log_dirty, 0)

    async def test_shutdown_persists_log_emitted_while_scheduler_stops(self) -> None:
        scheduler_started = asyncio.Event()

        async def scheduler() -> None:
            scheduler_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                server._log_event({"kind": "scheduler_stopped"})
                raise

        server._scheduler_task = asyncio.create_task(scheduler())
        server._dl = SimpleNamespace(stop=mock.AsyncMock())
        await scheduler_started.wait()

        with (
            mock.patch.object(server, "_RUN_LOG_FLUSH_DELAY", 3600),
            mock.patch.object(server, "flush_message_map", new=mock.AsyncMock()),
        ):
            server._log_event({"kind": "before_shutdown"})
            await server._shutdown()

        persisted = json.loads(server.RUN_LOG_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            [item["kind"] for item in persisted],
            ["before_shutdown", "scheduler_stopped"],
        )
        self.assertIsNone(server._scheduler_task)
        self.assertIsNone(server._run_log_flush_task)
        server._dl.stop.assert_awaited_once()


class PairSourceCacheInvalidationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.previous_cache = server._pairs_for_source_cache
        self.previous_state_lock = server._state_lock
        server._pairs_for_source_cache = {}
        server._state_lock = asyncio.Lock()
        self.config = {
            "interval_seconds": 3600,
            "pairs": [
                {
                    "name": "existing",
                    "source": 100,
                    "dest": 200,
                    "type": "all",
                    "paused": False,
                }
            ],
        }

        def load_pairs():
            return copy.deepcopy(self.config)

        def save_pairs(config):
            self.config = copy.deepcopy(config)

        self.patchers = [
            mock.patch.object(server, "DASH_PASS", None),
            mock.patch.object(server, "_pairs_file_exists", return_value=True),
            mock.patch.object(server, "load_pairs", side_effect=load_pairs),
            mock.patch.object(server, "save_pairs", side_effect=save_pairs),
            mock.patch.object(server, "_log_event"),
        ]
        started = [patcher.start() for patcher in self.patchers]
        self.load_pairs_mock = started[2]

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        server._pairs_for_source_cache = self.previous_cache
        server._state_lock = self.previous_state_lock

    @staticmethod
    def _pair_payload(**overrides):
        payload = {
            "name": "new",
            "source": 300,
            "dest": 400,
            "type": "all",
            "delay_seconds": 1,
            "max_per_run": 200,
        }
        payload.update(overrides)
        return payload

    async def test_reuses_source_lookup_within_ttl(self) -> None:
        first = server._pairs_for_source(100)
        second = server._pairs_for_source(100)

        self.assertIs(first, second)
        self.load_pairs_mock.assert_called_once_with()

    async def test_pair_add_invalidates_cached_empty_source(self) -> None:
        self.assertEqual(server._pairs_for_source(300), [])

        response = await server.app.test_client().post(
            "/api/pairs", json=self._pair_payload()
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [pair["name"] for pair in server._pairs_for_source(300)], ["new"]
        )

    async def test_pair_update_invalidates_old_and_new_sources(self) -> None:
        cached = server._pairs_for_source(100)
        self.assertEqual(cached[0]["dest"], 200)

        response = await server.app.test_client().post(
            "/api/pairs",
            json=self._pair_payload(
                name="existing",
                source=101,
                dest=201,
                replacements=[{"find": "a", "replace": "b"}],
            ),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(server._pairs_for_source(100), [])
        updated = server._pairs_for_source(101)
        self.assertEqual(updated[0]["dest"], 201)
        self.assertEqual(
            updated[0]["replacements"],
            [{"find": "a", "replace": "b", "regex": False}],
        )

    async def test_pair_pause_invalidates_cached_pair_configuration(self) -> None:
        self.assertFalse(server._pairs_for_source(100)[0]["paused"])

        response = await server.app.test_client().post(
            "/api/pairs/existing/pause", json={"paused": True}
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(server._pairs_for_source(100)[0]["paused"])

    async def test_pair_delete_invalidates_cached_source(self) -> None:
        self.assertEqual(
            [pair["name"] for pair in server._pairs_for_source(100)], ["existing"]
        )

        response = await server.app.test_client().delete("/api/pairs/existing")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(server._pairs_for_source(100), [])


if __name__ == "__main__":
    unittest.main()
