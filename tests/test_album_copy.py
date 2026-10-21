"""Media-group (album) handling on the copy path.

A source channel posts an album as N consecutive messages sharing
``grouped_id``. Re-uploading them one by one produced N unrelated posts on the
destination (each with its own preview) — the mirrored album lost its grid
layout. These tests pin down the fix:

* ``_copy_send_spec`` keeps the video/sticker/document attributes per item.
* ``_send_copy_album`` emits ONE ``SendMultiMediaRequest`` for the group.
* the copy pipeline collects adjacent ``grouped_id`` slots into one album,
  without deadlocking on the (smaller) download-ahead window.
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from telethon.tl.functions.messages import SendMultiMediaRequest, UploadMediaRequest
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    DocumentEmpty,
    InputMediaUploadedDocument,
    InputMediaUploadedPhoto,
    PhotoEmpty,
    PeerChannel,
    UpdateMessageID,
    UpdateNewMessage,
)

import automate
from downloader import ALBUM_MAX_ITEMS, TelegramDownloader


def _video_msg(mid: int, grouped_id=None, caption: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        id=mid,
        peer_id=PeerChannel(111),
        grouped_id=grouped_id,
        message=caption,
        entities=None,
        photo=None,
        document=SimpleNamespace(
            mime_type="video/mp4",
            size=2048,
            attributes=[DocumentAttributeVideo(duration=15, w=720, h=1280)],
        ),
        media=object(),
        video=None,
    )


def _video_payload() -> dict:
    return {
        "kind": "media",
        "temp_path": "/tmp/not-used.mp4",
        "size": 2048,
        "original_name": "clip.mp4",
        "video_attr": DocumentAttributeVideo(duration=15, w=720, h=1280),
        "sticker_attr": None,
        "animated_attr": None,
        "is_video_document": True,
    }


class CopySendSpecTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dl = TelegramDownloader({"api_id": 1, "api_hash": "x"})

    def test_video_keeps_metadata_and_stays_playable(self) -> None:
        spec = self.dl._copy_send_spec(_video_msg(1), _video_payload())

        self.assertFalse(spec["force_document"])
        self.assertTrue(spec["supports_streaming"])
        self.assertFalse(spec["is_photo"])
        self.assertEqual(spec["mime_type"], "video/mp4")
        kinds = [type(a) for a in spec["attributes"]]
        self.assertIn(DocumentAttributeFilename, kinds)
        self.assertIn(DocumentAttributeVideo, kinds)

    def test_plain_document_keeps_filename(self) -> None:
        msg = SimpleNamespace(
            id=2, message="", entities=None, photo=None,
            document=SimpleNamespace(mime_type="application/zip", attributes=[]),
        )
        payload = {
            "kind": "media", "temp_path": "/tmp/x.zip", "original_name": "pack.zip",
            "video_attr": None, "sticker_attr": None, "animated_attr": None,
            "is_video_document": False,
        }

        spec = self.dl._copy_send_spec(msg, payload)

        self.assertTrue(spec["force_document"])
        self.assertIsNone(spec["supports_streaming"])
        self.assertEqual(
            [getattr(a, "file_name", None) for a in spec["attributes"]], ["pack.zip"]
        )

    def test_text_override_drops_entities(self) -> None:
        msg = _video_msg(3, caption="old")
        msg.entities = [object()]

        spec = self.dl._copy_send_spec(msg, _video_payload(), "new caption")

        self.assertEqual(spec["caption"], "new caption")
        self.assertIsNone(spec["entities"])


class AlignSentTests(unittest.TestCase):
    def test_maps_random_ids_and_single_leftover(self) -> None:
        dl = TelegramDownloader({"api_id": 1, "api_hash": "x"})
        known = SimpleNamespace(id=11, random_id=1001)
        leftover = SimpleNamespace(id=13, random_id=None)
        result = SimpleNamespace(updates=[
            UpdateNewMessage(message=known, pts=1, pts_count=1),
            UpdateMessageID(id=12, random_id=1002),
            UpdateNewMessage(message=leftover, pts=2, pts_count=1),
        ])

        aligned = dl._align_sent([1001, 1002, 1003], result)

        self.assertEqual([m.id for m in aligned], [11, 12, 13])


class _AlbumClient:
    """Records uploadMedia/sendMultiMedia traffic for _send_copy_album."""

    def __init__(self) -> None:
        self.uploads = 0
        self.sent_request = None
        self.multi_media = None

    async def get_input_entity(self, value):
        return value

    async def upload_file(self, path):
        self.uploads += 1
        return SimpleNamespace(name=path)

    async def __call__(self, request):
        if isinstance(request, UploadMediaRequest):
            if isinstance(request.media, InputMediaUploadedPhoto):
                return SimpleNamespace(photo=PhotoEmpty(id=1))
            return SimpleNamespace(document=DocumentEmpty(id=2))
        if isinstance(request, SendMultiMediaRequest):
            self.sent_request = request
            self.multi_media = list(request.multi_media)
            return SimpleNamespace(updates=[
                UpdateMessageID(random_id=item.random_id, id=900 + idx)
                for idx, item in enumerate(request.multi_media)
            ])
        raise AssertionError(f"unexpected request {type(request).__name__}")


class SendCopyAlbumTests(unittest.IsolatedAsyncioTestCase):
    async def test_sends_one_multi_media_request(self) -> None:
        dl = TelegramDownloader({"api_id": 1, "api_hash": "x"})
        client = _AlbumClient()
        dl.client = client

        entries = [
            {"msg": _video_msg(101, grouped_id=77, caption="c1"),
             "payload": _video_payload(), "text_override": None},
            {"msg": _video_msg(102, grouped_id=77, caption="c2"),
             "payload": _video_payload(), "text_override": None},
            {"msg": _video_msg(103, grouped_id=77, caption="c3"),
             "payload": _video_payload(), "text_override": "replaced"},
        ]

        sent = await dl._send_copy_album(entries, -100123, dest_topic=42)

        self.assertEqual(client.uploads, 3)
        self.assertIsNotNone(client.sent_request)
        self.assertEqual(len(client.multi_media), 3)
        self.assertEqual([m.message for m in client.multi_media],
                         ["c1", "c2", "replaced"])
        self.assertEqual(client.sent_request.reply_to.reply_to_msg_id, 42)
        self.assertEqual([m.id for m in sent], [900, 901, 902])

    async def test_retry_after_lost_response_reuses_random_ids(self) -> None:
        dl = TelegramDownloader({"api_id": 1, "api_hash": "x"})

        class LostResponseClient(_AlbumClient):
            def __init__(self):
                super().__init__()
                self.attempts = []
                self.lose_response = True

            async def __call__(self, request):
                if isinstance(request, SendMultiMediaRequest):
                    self.attempts.append([m.random_id for m in request.multi_media])
                    if self.lose_response:
                        raise TimeoutError("response lost after server accepted album")
                return await super().__call__(request)

        client = LostResponseClient()
        dl.client = client
        entries = [
            {"msg": _video_msg(mid, grouped_id=77),
             "payload": _video_payload(), "text_override": None}
            for mid in (101, 102)
        ]
        with mock.patch("downloader.sleep_backoff", new=mock.AsyncMock()):
            with self.assertRaises(TimeoutError):
                await dl._send_copy_album(entries, -100123, dest_topic=42)
        client.lose_response = False
        await dl._send_copy_album(entries, -100123, dest_topic=42)
        self.assertEqual(len(client.attempts), 4)
        self.assertTrue(all(ids == client.attempts[0] for ids in client.attempts))
        await dl._send_copy_album(entries, -100123, dest_topic=43)
        self.assertNotEqual(client.attempts[-1], client.attempts[0])


class _PipelineClient:
    def __init__(self, messages) -> None:
        self._messages = messages

    def is_connected(self) -> bool:
        return True

    async def iter_messages(self, source, **kwargs):
        for msg in self._messages:
            await asyncio.sleep(0)
            yield msg


class _PipelineDL:
    """Fake downloader: no network, records album vs single sends."""

    max_concurrent = 1

    def __init__(self, messages, album_sink, album_error=None) -> None:
        self.client = _PipelineClient(messages)
        self.album_sink = album_sink
        self.album_error = album_error
        self.single_sends = []

    async def is_source_protected(self, source) -> bool:
        return True

    def _is_media(self, msg) -> bool:
        return True

    async def _download_copy_media(self, msg, *, temp_dir=None):
        return {
            "kind": "media",
            "temp_path": f"/tmp/fake-{msg.id}",
            "size": 1024,
            "original_name": f"{msg.id}.mp4",
            "video_attr": None,
            "sticker_attr": None,
            "animated_attr": None,
            "is_video_document": True,
        }

    def _unlink_quiet(self, path) -> None:
        pass

    async def _send_copy_payload(self, msg, dest, payload, dest_topic=None,
                                 text_override=None):
        self.single_sends.append(msg.id)
        return SimpleNamespace(id=5000 + msg.id)

    async def _send_copy_album(self, entries, dest_id, dest_topic=None):
        if self.album_error is not None:
            raise self.album_error
        ids = [entry["msg"].id for entry in entries]
        self.album_sink.append(ids)
        return [SimpleNamespace(id=7000 + idx) for idx, _ in enumerate(entries)]


class CopyPipelineAlbumTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.patches = [
            mock.patch.object(automate, "save_pair_watermark",
                                       new=self._async_recorder("wm")),
            mock.patch.object(automate, "record_mappings",
                                       new=self._async_recorder("map")),
            mock.patch.object(automate, "flush_message_map",
                                       new=self._async_recorder("flush")),
            mock.patch.object(automate, "lookup_dest_id",
                                       new=lambda *a, **k: None),
        ]
        self.mocks = [p.start() for p in self.patches]
        self.addCleanup(lambda: [p.stop() for p in self.patches])

    @staticmethod
    def _async_recorder(label):
        calls = []

        async def _rec(*args, **kwargs):
            calls.append((args, kwargs))
            return None

        _rec.calls = calls
        return _rec

    def _pair(self, name="album-pair"):
        return {
            "name": name,
            "source": -100111,
            "dest": -100222,
            "type": "media",
            "delay_seconds": 0,
            "copy_concurrency": 1,  # ahead window = 2 → album > window
        }

    async def test_grouped_messages_go_out_as_one_album(self) -> None:
        msgs = [_video_msg(101 + i, grouped_id=555) for i in range(6)]
        album_sink: list[list[int]] = []
        dl = _PipelineDL(msgs, album_sink)
        state = {"album-pair": {"last_msg_id": 100}}

        result = await asyncio.wait_for(
            automate.run_pair(dl, self._pair(), state), timeout=15
        )

        self.assertEqual(album_sink, [[101, 102, 103, 104, 105, 106]])
        self.assertEqual(dl.single_sends, [])
        self.assertEqual(result["forwarded"], 6)
        self.assertEqual(result["last_id"], 106)

    async def test_groups_longer_than_ten_are_chunked(self) -> None:
        count = ALBUM_MAX_ITEMS + 2
        msgs = [_video_msg(200 + i, grouped_id=999) for i in range(count)]
        album_sink: list[list[int]] = []
        dl = _PipelineDL(msgs, album_sink)

        result = await asyncio.wait_for(
            automate.run_pair(dl, self._pair("long-album"),
                              {"long-album": {"last_msg_id": 199}}),
            timeout=15,
        )

        self.assertEqual([len(chunk) for chunk in album_sink],
                         [ALBUM_MAX_ITEMS, 2])
        self.assertEqual(result["forwarded"], count)
        self.assertEqual(result["last_id"], 200 + count - 1)

    async def test_many_albums_back_to_back_do_not_leak_pipeline_slots(self) -> None:
        # Five separate albums with a download-ahead window of 2: if the album
        # path failed to release/advance slots correctly, the producer would
        # starve after a couple of groups and the run would hang.
        msgs = []
        for group in range(5):
            gid = 1000 + group
            msgs.extend(_video_msg(600 + group * 3 + i, grouped_id=gid) for i in range(3))
        album_sink: list[list[int]] = []
        dl = _PipelineDL(msgs, album_sink)

        result = await asyncio.wait_for(
            automate.run_pair(dl, self._pair("multi-album"),
                              {"multi-album": {"last_msg_id": 599}}),
            timeout=20,
        )

        self.assertEqual(len(album_sink), 5)
        self.assertEqual([len(chunk) for chunk in album_sink], [3] * 5)
        self.assertEqual(result["forwarded"], 15)
        self.assertEqual(result["last_id"], 614)

    async def test_album_failure_falls_back_to_single_sends(self) -> None:
        msgs = [_video_msg(301 + i, grouped_id=1234) for i in range(3)]
        album_sink: list[list[int]] = []
        dl = _PipelineDL(msgs, album_sink, album_error=RuntimeError("boom"))

        result = await asyncio.wait_for(
            automate.run_pair(dl, self._pair("fallback"),
                              {"fallback": {"last_msg_id": 300}}),
            timeout=15,
        )

        self.assertEqual(album_sink, [])
        self.assertEqual(dl.single_sends, [301, 302, 303])
        self.assertEqual(result["forwarded"], 3)
        self.assertEqual(result["last_id"], 303)

    async def test_uncertain_album_result_does_not_fall_back_or_advance(self) -> None:
        msgs = [_video_msg(301 + i, grouped_id=1234) for i in range(3)]
        dl = _PipelineDL(msgs, [], album_error=TimeoutError("response lost"))
        result = await automate.run_pair(
            dl, self._pair("uncertain"), {"uncertain": {"last_msg_id": 300}}
        )
        self.assertEqual(dl.single_sends, [])
        self.assertEqual(result["last_id"], 300)
        self.assertEqual(result["last_scanned_id"], 300)
        self.assertTrue(result["aborted_transient"])
        self.assertEqual(automate.record_mappings.calls, [])

    async def test_native_consecutive_albums_keep_groups_and_batch_limit(self) -> None:
        msgs = [_video_msg(1 + i, grouped_id=i // 3 + 1) for i in range(240)]
        dl = _PipelineDL(msgs, [])
        dl.is_source_protected = mock.AsyncMock(return_value=False)
        batches = []

        async def forward(dest, messages, source, **kwargs):
            batches.append(list(messages))
            return [SimpleNamespace(id=1000 + m.id) for m in messages]

        dl.client.forward_messages = forward
        result = await automate.run_pair(dl, self._pair("native"), {})
        self.assertEqual(result["forwarded"], len(msgs))
        self.assertGreater(len(batches), 1)
        self.assertTrue(all(len(batch) <= 100 for batch in batches))
        self.assertEqual([m.id for batch in batches for m in batch],
                         [m.id for m in msgs])
        groups = {}
        for idx, batch in enumerate(batches):
            for m in batch:
                groups.setdefault(m.grouped_id, set()).add(idx)
        self.assertTrue(all(len(indices) == 1 for indices in groups.values()))

    async def test_ungrouped_messages_still_send_one_by_one(self) -> None:
        msgs = [_video_msg(401), _video_msg(402), _video_msg(403)]
        album_sink: list[list[int]] = []
        dl = _PipelineDL(msgs, album_sink)

        result = await asyncio.wait_for(
            automate.run_pair(dl, self._pair("singles"),
                              {"singles": {"last_msg_id": 400}}),
            timeout=15,
        )

        self.assertEqual(album_sink, [])
        self.assertEqual(dl.single_sends, [401, 402, 403])
        self.assertEqual(result["forwarded"], 3)


if __name__ == "__main__":
    unittest.main()
