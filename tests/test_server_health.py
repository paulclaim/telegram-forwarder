from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest import mock

from telethon.errors import AuthKeyDuplicatedError

import server


class _ConnectedClient:
    def is_connected(self) -> bool:
        return True


class HealthEndpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.previous_downloader = server._dl
        self.previous_session_error = server._telegram_session_error
        server._dl = SimpleNamespace(client=_ConnectedClient())

    def tearDown(self) -> None:
        server._dl = self.previous_downloader
        server._telegram_session_error = self.previous_session_error

    async def test_exposes_redacted_fatal_session_state(self) -> None:
        server._telegram_session_error = "AuthKeyDuplicatedError"

        response = await server.app.test_client().get("/healthz")
        payload = await response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["telegram_connected"])
        self.assertEqual(payload["telegram_session_error"], "AuthKeyDuplicatedError")

    async def test_healthy_session_has_null_error(self) -> None:
        server._telegram_session_error = None

        response = await server.app.test_client().get("/healthz")
        payload = await response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(payload["telegram_session_error"])


class StartupSessionFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_fatal_session_keeps_web_startup_available(self) -> None:
        fake_client = SimpleNamespace(add_event_handler=mock.Mock())
        fake_downloader = SimpleNamespace(
            client=fake_client,
            start=mock.AsyncMock(side_effect=AuthKeyDuplicatedError(request=None)),
        )
        fake_scheduler_task = object()
        previous_downloader = server._dl
        previous_scheduler_task = server._scheduler_task
        previous_session_error = server._telegram_session_error

        def close_scheduler_coroutine(coroutine: object) -> object:
            close = getattr(coroutine, "close", None)
            if callable(close):
                close()
            return fake_scheduler_task

        try:
            with (
                mock.patch("server.TelegramDownloader", return_value=fake_downloader),
                mock.patch("server.load_config", return_value={}),
                mock.patch("server.load_pairs", return_value={"pairs": []}),
                mock.patch("server.load_message_map"),
                mock.patch("server._load_run_log"),
                mock.patch("server._register_event_handlers"),
                mock.patch("server.asyncio.create_task", side_effect=close_scheduler_coroutine),
                mock.patch("builtins.print"),
            ):
                await server._startup()

            self.assertEqual(server._telegram_session_error, "AuthKeyDuplicatedError")
            self.assertIs(server._scheduler_task, fake_scheduler_task)
        finally:
            server._dl = previous_downloader
            server._scheduler_task = previous_scheduler_task
            server._telegram_session_error = previous_session_error


if __name__ == "__main__":
    unittest.main()
