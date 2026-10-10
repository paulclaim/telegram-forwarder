from __future__ import annotations

import argparse
import asyncio
import io
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import cli
from telethon.errors import (
    AuthTokenExpiredError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)


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


class TelegramPhoneLoginTests(unittest.IsolatedAsyncioTestCase):
    async def _run(
        self,
        *,
        getpass_values=("123456",),
        sign_in_effects=(),
        send_code_effects=(),
    ):
        client = mock.Mock()
        client.connect = mock.AsyncMock()
        client.disconnect = mock.AsyncMock()
        client.is_user_authorized = mock.AsyncMock(side_effect=[False, True])
        client.send_code_request = mock.AsyncMock(
            return_value=mock.Mock(phone_code_hash="code-hash")
        )
        if send_code_effects:
            client.send_code_request.side_effect = list(send_code_effects)
        client.sign_in = mock.AsyncMock()
        if sign_in_effects:
            client.sign_in.side_effect = list(sign_in_effects)

        with (
            mock.patch.object(cli.sys.stdin, "isatty", return_value=True),
            mock.patch.object(cli.sys.stdout, "isatty", return_value=True),
            mock.patch.dict(
                os.environ,
                {"TELETHON_SESSION_FILE": "phone_test_session", "TELETHON_SESSION_STRING": ""},
            ),
            mock.patch.object(cli, "load_config", return_value={"api_id": 1, "api_hash": "hash"}),
            mock.patch.object(cli, "get_telegram_proxy", return_value=None),
            mock.patch.object(cli, "TelegramClient", return_value=client) as constructor,
            mock.patch("builtins.input", return_value="+8613800000000"),
            mock.patch.object(cli.getpass, "getpass", side_effect=list(getpass_values)),
            mock.patch("builtins.print"),
        ):
            await cli.cmd_login_phone(argparse.Namespace())
        return client, constructor

    async def test_phone_code_authorizes_and_disconnects(self) -> None:
        client, constructor = await self._run()

        self.assertTrue(constructor.call_args.args[0].endswith("phone_test_session"))
        client.send_code_request.assert_awaited_once_with("+8613800000000")
        client.sign_in.assert_awaited_once_with(
            phone="+8613800000000", code="123456", phone_code_hash="code-hash"
        )
        client.disconnect.assert_awaited_once()

    async def test_phone_login_prompts_for_two_step_password(self) -> None:
        client, _constructor = await self._run(
            getpass_values=("123456", "secret-password"),
            sign_in_effects=(SessionPasswordNeededError(request=None), None),
        )

        self.assertEqual(client.sign_in.await_count, 2)
        self.assertEqual(
            client.sign_in.await_args_list[-1], mock.call(password="secret-password")
        )
        client.disconnect.assert_awaited_once()

    async def test_wrong_code_prompts_again_with_same_hash(self) -> None:
        client, _constructor = await self._run(
            getpass_values=("11111", "123456"),
            sign_in_effects=(PhoneCodeInvalidError(request=None), None),
        )

        self.assertEqual(client.sign_in.await_count, 2)
        self.assertEqual(
            client.sign_in.await_args_list,
            [
                mock.call(phone="+8613800000000", code="11111", phone_code_hash="code-hash"),
                mock.call(phone="+8613800000000", code="123456", phone_code_hash="code-hash"),
            ],
        )
        client.disconnect.assert_awaited_once()

    async def test_expired_code_requests_a_new_code(self) -> None:
        client, _constructor = await self._run(
            getpass_values=("00000", "123456"),
            sign_in_effects=(PhoneCodeExpiredError(request=None), None),
            send_code_effects=(
                mock.Mock(phone_code_hash="code-hash"),
                mock.Mock(phone_code_hash="new-hash"),
            ),
        )

        self.assertEqual(client.send_code_request.await_count, 2)
        self.assertEqual(
            client.sign_in.await_args_list[-1],
            mock.call(phone="+8613800000000", code="123456", phone_code_hash="new-hash"),
        )
        client.disconnect.assert_awaited_once()

    async def test_repeated_wrong_code_fails_with_friendly_error(self) -> None:
        with self.assertRaises(RuntimeError) as ctx:
            await self._run(
                getpass_values=("1", "2", "3"),
                sign_in_effects=[PhoneCodeInvalidError(request=None)] * 3,
            )

        self.assertIn("多次输入错误", str(ctx.exception))

    async def test_flood_wait_on_code_request_is_reported_friendly(self) -> None:
        with self.assertRaises(RuntimeError) as ctx:
            await self._run(send_code_effects=(FloodWaitError(request=None, capture=37),))

        self.assertIn("37 秒", str(ctx.exception))
