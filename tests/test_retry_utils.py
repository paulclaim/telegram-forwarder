from __future__ import annotations

import unittest

from telethon.errors import AuthKeyDuplicatedError, FloodWaitError
from types import SimpleNamespace
from unittest import mock

import automate
from automate import _retry_is_due, _retry_ready_at, drain_retry_queue

from downloader import TelegramDownloader
from retry_utils import is_fatal_session_error


class FatalSessionErrorTests(unittest.TestCase):
    def test_detects_auth_key_duplicated(self) -> None:
        self.assertTrue(is_fatal_session_error(AuthKeyDuplicatedError(request=None)))

    def test_detects_wrapped_fatal_session_error(self) -> None:
        try:
            try:
                raise RuntimeError("AUTH_KEY_DUPLICATED")
            except RuntimeError as cause:
                raise ValueError("outer error") from cause
        except ValueError as error:
            self.assertTrue(is_fatal_session_error(error))

    def test_does_not_treat_network_error_as_fatal_session(self) -> None:
        self.assertFalse(is_fatal_session_error(ConnectionError("network unreachable")))


class TransferTimeoutTests(unittest.TestCase):
    def test_allows_slow_small_openwrt_download(self) -> None:
        self.assertEqual(TelegramDownloader._transfer_timeout(2_845_078), 180.0)
        self.assertEqual(TelegramDownloader._transfer_timeout(0), 180.0)
        self.assertEqual(
            TelegramDownloader._transfer_timeout(25 * 1024 * 1024),
            600.0,
        )


class RetryScheduleTests(unittest.TestCase):
    def test_fast_initial_retry_and_bounded_exponential_delay(self):
        for attempts, delay in [(1, 30), (2, 60), (3, 120), (20, 900)]:
            self.assertEqual(
                _retry_ready_at({"last_attempt_at": 100, "attempts": attempts}, {}),
                100 + delay,
            )

    def test_explicit_minimum_is_preserved(self):
        self.assertEqual(
            _retry_ready_at({"last_attempt_at": 100, "attempts": 20},
                            {"retry_min_interval_seconds": 1800}), 1900,
        )

    def test_force_cannot_bypass_server_cooldown(self):
        item = {"flood_wait_until": 200, "next_attempt_at": 500}
        self.assertFalse(_retry_is_due(item, {}, 199, force=True))
        self.assertTrue(_retry_is_due(item, {}, 200, force=True))
        self.assertFalse(_retry_is_due(item, {}, 200))

    def test_new_item_is_immediately_due(self):
        self.assertTrue(_retry_is_due({"attempts": 0}, {}, 100))


class RetryDrainTests(unittest.IsolatedAsyncioTestCase):
    async def test_floodwait_keeps_item_pending_and_persists_cooldown(self):
        item = {"id": "test:1", "pair": "test", "src_id": 1,
                "attempts": 0, "status": "pending"}
        data = {"items": [item]}
        pair = {"name": "test", "source": 100, "dest": 200,
                "retry_max_attempts": 1}
        client = SimpleNamespace(
            get_messages=mock.AsyncMock(side_effect=FloodWaitError(None, capture=120)))
        with mock.patch.object(automate, "load_pairs", return_value={"pairs": [pair]}),              mock.patch.object(automate, "load_retry_queue", return_value=data),              mock.patch.object(automate, "_flush_retry_queue_locked", new=mock.AsyncMock()),              mock.patch.object(automate, "ensure_connected", new=mock.AsyncMock(return_value=True)),              mock.patch.object(automate.time, "time", return_value=1000):
            await drain_retry_queue(SimpleNamespace(client=client))
            self.assertEqual(item["status"], "pending")
            self.assertEqual(item["attempts"], 0)
            self.assertEqual(item["flood_wait_until"], 1121)
            await drain_retry_queue(SimpleNamespace(client=client), force=True)
            self.assertEqual(client.get_messages.await_count, 1)


if __name__ == "__main__":
    unittest.main()
