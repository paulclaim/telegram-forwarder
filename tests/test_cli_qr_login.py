from __future__ import annotations

import argparse
import asyncio
import io
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import cli
from telethon.errors import AuthTokenExpiredError, SessionPasswordNeededError


class TelegramQrLoginTests(unittest.IsolatedAsyncioTestCase):
    def test_timeout_range_is_bounded(self) -> None:
        self.assertEqual(cli._qr_timeout("300"), 300)
        with self.assertRaises(argparse.ArgumentTypeError):
            cli._qr_timeout("59")
        with self.assertRaises(argparse.ArgumentTypeError):
            cli._qr_timeout("not-a-number")

    def _client(self):
        client = mock.Mock()
        client.connect = mock.AsyncMock()
        client.disconnect = mock.AsyncMock()
        client.is_user_authorized = mock.AsyncMock(side_effect=[False, True])
        client.qr_login = mock.AsyncMock()
        client.sign_in = mock.AsyncMock()
        return client

    def _qr_login(self):
        qr_login = mock.Mock()
        qr_login.url = "tg://login?token=not-a-real-token"
        qr_login.expires = datetime.now(timezone.utc) + timedelta(minutes=1)
        qr_login.wait = mock.AsyncMock(return_value=mock.Mock())
        qr_login.recreate = mock.AsyncMock()
        return qr_login

    async def _run(self, client, qr_login):
        client.qr_login.return_value = qr_login
        args = argparse.Namespace(timeout=300)
        with (
            mock.patch.object(cli.sys.stdin, "isatty", return_value=True),
            mock.patch.object(cli.sys.stdout, "isatty", return_value=True),
            mock.patch.dict(
                os.environ,
                {
                    "TELETHON_SESSION_FILE": "qr_test_session",
                    "TELETHON_SESSION_STRING": "",
                },
            ),
            mock.patch.object(cli, "load_config", return_value={"api_id": 1, "api_hash": "hash"}),
            mock.patch.object(cli, "get_telegram_proxy", return_value=None),
            mock.patch.object(cli, "TelegramClient", return_value=client) as constructor,
            mock.patch.object(cli, "_render_login_qr") as render,
            mock.patch("builtins.print"),
        ):
            await cli.cmd_login_qr(args)
        return constructor, render

    async def test_scanned_qr_authorizes_and_disconnects(self) -> None:
        client = self._client()
        qr_login = self._qr_login()

        constructor, render = await self._run(client, qr_login)

        constructor.assert_called_once()
        self.assertTrue(constructor.call_args.args[0].endswith("qr_test_session"))
        render.assert_called_once_with(qr_login.url)
        qr_login.wait.assert_awaited_once()
        client.disconnect.assert_awaited_once()

    async def test_two_step_verification_password_is_entered_once(self) -> None:
        client = self._client()
        qr_login = self._qr_login()
        qr_login.wait.side_effect = SessionPasswordNeededError(request=None)

        with mock.patch.object(cli.getpass, "getpass", return_value="test-password"):
            await self._run(client, qr_login)

        client.sign_in.assert_awaited_once_with(password="test-password")
        client.disconnect.assert_awaited_once()

    async def test_expired_qr_is_refreshed_within_same_attempt(self) -> None:
        client = self._client()
        qr_login = self._qr_login()
        qr_login.wait.side_effect = [asyncio.TimeoutError(), mock.Mock()]

        _constructor, render = await self._run(client, qr_login)

        qr_login.recreate.assert_awaited_once()
        self.assertEqual(render.call_count, 2)
        client.disconnect.assert_awaited_once()

    async def test_server_expired_token_is_refreshed_and_waited_again(self) -> None:
        client = self._client()
        qr_login = self._qr_login()
        old_url = qr_login.url
        new_url = "tg://login?token=refreshed-token"
        qr_login.wait.side_effect = [AuthTokenExpiredError(request=None), mock.Mock()]

        async def refresh_token() -> None:
            qr_login.url = new_url
            qr_login.expires = datetime.now(timezone.utc) + timedelta(minutes=1)

        qr_login.recreate.side_effect = refresh_token

        _constructor, render = await self._run(client, qr_login)

        qr_login.recreate.assert_awaited_once()
        self.assertEqual(qr_login.wait.await_count, 2)
        self.assertEqual(
            render.call_args_list,
            [mock.call(old_url), mock.call(new_url)],
        )
        client.sign_in.assert_not_awaited()
        client.disconnect.assert_awaited_once()

    def test_rendered_qr_does_not_print_login_url(self) -> None:
        output = io.StringIO()
        login_url = "tg://login?token=super-secret-token"

        with mock.patch.object(cli.sys, "stdout", output):
            cli._render_login_qr(login_url)

        rendered = output.getvalue()
        self.assertNotIn(login_url, rendered)
        self.assertNotIn("super-secret-token", rendered)
        self.assertIn("链接桌面设备", rendered)
