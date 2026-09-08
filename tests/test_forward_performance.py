import sqlite3
import unittest
from types import SimpleNamespace
from unittest import mock

from automate import _copy_ahead_window, _copy_concurrency
from automate_multi import forward_plan
from downloader import TelegramDownloader


class _ForwardClient:
    def __init__(self):
        self.request = None

    async def get_input_entity(self, value):
        return value

    async def __call__(self, request):
        self.request = request
        return SimpleNamespace(updates=[])


class ForwardBatchPerformanceTests(unittest.IsolatedAsyncioTestCase):
    def test_copy_concurrency_stays_bounded_by_configuration(self):
        class _Downloader:
            max_concurrent = 3

        self.assertEqual(_copy_concurrency({}, _Downloader()), 3)
        self.assertEqual(_copy_concurrency({"copy_concurrency": 5}, _Downloader()), 3)
        self.assertEqual(_copy_ahead_window(3), 6)

    async def test_forward_batch_accepts_message_ids_without_refetch(self):
        dl = TelegramDownloader({"api_id": 1, "api_hash": "x"})
        client = _ForwardClient()
        dl.client = client

        forwarded = await dl.forward_batch(100, 200, [11, 12, 13])

        self.assertEqual(client.request.id, [11, 12, 13])
        self.assertEqual(forwarded, [None, None, None])

    async def test_multi_forward_uses_plan_ids_and_has_no_trailing_delay(self):
        class _Client:
            async def get_messages(self, *args, **kwargs):
                raise AssertionError("forward_plan must not refetch planned message ids")

        class _Downloader:
            def __init__(self):
                self.client = _Client()
                self.calls = []

            async def forward_batch(self, source, dest, msgs, **kwargs):
                self.calls.append((source, dest, list(msgs), kwargs))
                return [SimpleNamespace(id=1000 + msg_id) for msg_id in msgs]

        dl = _Downloader()
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE forward_log ("
            "src_msg_id INTEGER PRIMARY KEY, dst_msg_id INTEGER, "
            "topic_id INTEGER, forwarded_at INTEGER)"
        )
        job = {}

        with mock.patch("automate_multi.record_mappings", new=mock.AsyncMock()) as record, \
             mock.patch("automate_multi.asyncio.sleep", new=mock.AsyncMock()) as sleep:
            await forward_plan(
                dl, 100, 200, {7: [1, 2, 3]}, conn, job, "pair",
                inter_batch_delay=0.5,
            )

        self.assertEqual(dl.calls[0][2], [1, 2, 3])
        record.assert_awaited_once_with("pair", [(1, 1001), (2, 1002), (3, 1003)])
        sleep.assert_not_awaited()
        self.assertEqual(job["done"], 3)
        self.assertEqual(job["ok"], 3)
        self.assertEqual(job["fail"], 0)
        self.assertEqual(
            conn.execute("SELECT src_msg_id, dst_msg_id FROM forward_log ORDER BY src_msg_id").fetchall(),
            [(1, 1001), (2, 1002), (3, 1003)],
        )
        conn.close()

    async def test_multi_forward_keeps_delay_between_batches(self):
        class _Downloader:
            client = object()

            async def forward_batch(self, source, dest, msgs, **kwargs):
                return [SimpleNamespace(id=2000 + msg_id) for msg_id in msgs]

        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE forward_log ("
            "src_msg_id INTEGER PRIMARY KEY, dst_msg_id INTEGER, "
            "topic_id INTEGER, forwarded_at INTEGER)"
        )
        ids = list(range(1, 102))

        with mock.patch("automate_multi.record_mappings", new=mock.AsyncMock()), \
             mock.patch("automate_multi.asyncio.sleep", new=mock.AsyncMock()) as sleep:
            await forward_plan(
                _Downloader(), 100, 200, {7: ids}, conn, {}, "pair",
                inter_batch_delay=0.25,
            )

        sleep.assert_awaited_once_with(0.25)
        conn.close()


if __name__ == "__main__":
    unittest.main()
