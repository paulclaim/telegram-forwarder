from __future__ import annotations

import unittest

from telethon.errors import AuthKeyDuplicatedError

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


if __name__ == "__main__":
    unittest.main()
