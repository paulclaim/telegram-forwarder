"""
Long-running forwarder: poll configured (source, dest) pairs on an interval,
copy only new messages since each pair's persisted watermark.

Config is pairs.json (or PAIRS_JSON env var). State lives in watermarks.json
(or STATE_PATH env var) so it survives container restarts.

Run modes:
    python automate.py                  # loop forever
    RUN_ONCE_AND_EXIT=1 python automate.py   # one pass, then exit (for tests)
"""

import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

from telethon.errors import FloodWaitError, MessageNotModifiedError

from downloader import TelegramDownloader, load_config
from retry_utils import ensure_connected, is_transient_error

BASE_DIR = Path(__file__).parent
DEFAULT_PAIRS = BASE_DIR / "pairs.json"
DEFAULT_STATE = BASE_DIR / "watermarks.json"
DEFAULT_MSG_MAP = BASE_DIR / "message_map.json"
DEFAULT_RETRY_QUEUE = BASE_DIR / "retry_queue.json"


def _resolve_pairs_path() -> Path:
    # PAIRS_PATH wins (cloud volume), else local default.
    return Path(os.environ.get("PAIRS_PATH", str(DEFAULT_PAIRS)))


def _resolve_state_path() -> Path:
    return Path(os.environ.get("STATE_PATH", str(DEFAULT_STATE)))


def _resolve_msg_map_path() -> Path:
    return Path(os.environ.get("MSG_MAP_PATH", str(DEFAULT_MSG_MAP)))


def _resolve_retry_queue_path() -> Path:
    return Path(os.environ.get("RETRY_QUEUE_PATH", str(DEFAULT_RETRY_QUEUE)))


# Per-pair regex find/replace rules applied to message text + caption in
# copy-mode. Inspired by aahnik/tgcf's "format" plugin. Returns a new string —
# never mutates input. Bad regex patterns are skipped silently.
def apply_replacements(text: Optional[str], rules: Optional[list]) -> str:
    if not text:
        return text or ""
    if not rules:
        return text
    out = text
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        find = rule.get("find", "")
        replace = rule.get("replace", "")
        if not find:
            continue
        if rule.get("regex", False):
            try:
                out = re.sub(find, replace, out)
            except re.error:
                continue
        else:
            out = out.replace(find, replace)
    return out


# ── Source-message-id → destination-message-id map ────────────────────────
# Powers live edit/delete propagation (telemirror-style). When the source
# edits or deletes a message we forwarded, the event handler in server.py
# looks up the dest id here and applies the same change. Structure:
#   {pair_name: {str(src_id): dest_id}}
# JSON requires string keys; we cast on read.
_msg_map: dict[str, dict[str, int]] = {}
_msg_map_lock = asyncio.Lock()
_msg_map_loaded = False


def load_message_map() -> dict:
    """Read from disk into in-memory _msg_map. Idempotent."""
    global _msg_map, _msg_map_loaded
    path = _resolve_msg_map_path()
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                _msg_map = data
        except (json.JSONDecodeError, OSError) as e:
            print(f"message_map.json unreadable, starting fresh: {e}", file=sys.stderr)
            _msg_map = {}
    _msg_map_loaded = True
    return _msg_map


async def _flush_message_map_locked() -> None:
    """Caller must hold _msg_map_lock. Atomic write via .tmp+replace."""
    path = _resolve_msg_map_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_msg_map, f)
    tmp.replace(path)


async def record_mappings(pair_name: str, pairs_iter) -> None:
    """Record many (src_id, dest_id) pairs atomically + flush.

    pairs_iter: iterable of (src_id, dest_id) tuples.
    Skips Nones (failed sends). Safe to call concurrently from multiple runners.
    """
    async with _msg_map_lock:
        bucket = _msg_map.setdefault(pair_name, {})
        any_added = False
        for src_id, dest_id in pairs_iter:
            if src_id is None or dest_id is None:
                continue
            bucket[str(src_id)] = int(dest_id)
            any_added = True
        if any_added:
            await _flush_message_map_locked()


def mapped_src_ids(pair_name: str) -> set[int]:
    """Return the set of source msg ids already forwarded for this pair.
    Reads from in-memory _msg_map (loaded at server startup, updated atomically
    by record_mappings). Used by the /gaps endpoint to compute missing ids."""
    bucket = _msg_map.get(pair_name)
    if not bucket:
        return set()
    return {int(k) for k in bucket.keys()}


def lookup_dest_id(pair_name: str, src_id: int) -> Optional[int]:
    """Return mapped dest id or None if pair/src isn't recorded."""
    bucket = _msg_map.get(pair_name)
    if not bucket:
        return None
    val = bucket.get(str(src_id))
    return int(val) if val is not None else None


async def forget_mappings(pair_name: str, src_ids) -> None:
    """Remove recorded entries (called after delete-propagation runs).
    Stops the map from growing forever for ephemeral source messages."""
    async with _msg_map_lock:
        bucket = _msg_map.get(pair_name)
        if not bucket:
            return
        changed = False
        for sid in src_ids:
            if bucket.pop(str(sid), None) is not None:
                changed = True
        if changed:
            await _flush_message_map_locked()


def load_pairs() -> dict:
    path = _resolve_pairs_path()
    if not path.exists():
        # First boot: seed from PAIRS_JSON env var if provided (inline JSON).
        # Lets a fresh container start with a known pair config.
        seed = os.environ.get("PAIRS_JSON")
        if seed and seed.lstrip().startswith("{"):
            try:
                cfg = json.loads(seed)
                save_pairs(cfg)
                print(f"Seeded pairs from PAIRS_JSON ({len(cfg.get('pairs', []))} pairs)")
                return cfg
            except json.JSONDecodeError as e:
                print(f"PAIRS_JSON is not valid JSON: {e}", file=sys.stderr)
                raise
        raise FileNotFoundError(
            f"Pairs config not found: {path}. Copy pairs.example.json to pairs.json, "
            "or set the PAIRS_JSON env var (inline JSON, seeds the file)."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_pairs(cfg: dict) -> None:
    path = _resolve_pairs_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, sort_keys=True)
    tmp.replace(path)


def load_state() -> dict:
    path = _resolve_state_path()
    if not path.exists():
        # First boot: seed from INITIAL_WATERMARKS_JSON if provided.
        # Stops us from re-forwarding history when a deployment starts with
        # an empty volume but the user has already copied messages elsewhere.
        seed = os.environ.get("INITIAL_WATERMARKS_JSON")
        if seed:
            try:
                state = json.loads(seed)
                save_state(state)
                print(f"Seeded watermarks from INITIAL_WATERMARKS_JSON ({len(state)} pairs)")
                return state
            except json.JSONDecodeError as e:
                print(f"INITIAL_WATERMARKS_JSON is not valid JSON: {e}", file=sys.stderr)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    path = _resolve_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    tmp.replace(path)


# Serializes the per-pair atomic save. Without this, two concurrent runners
# each hold a stale snapshot of the full state dict and clobber each other's
# keys when they call save_state(state). Observed 2026-05-19: scheduled jobs
# successfully forwarded N messages and saved wm=X, but concurrent bulk runs
# on different pairs wrote stale dicts that reset that pair's wm back to its
# pre-run value (or 0). Next bulk iteration on the reset pair re-forwarded
# everything → duplicates in destination.
_save_lock = asyncio.Lock()


async def save_pair_watermark(name: str, last_msg_id: int, updated_at: int, *, allow_regression: bool = False) -> None:
    """Atomic read-modify-write of one pair's watermark. Reload latest from
    disk, update only this pair's key, write back. Preserves any updates other
    runners made to other keys between our last load and now.

    By default refuses to write a value LOWER than what's already on disk.
    This protects against a stale in-flight run (loaded state when wm was X,
    started copying at X+1) zapping a manual repair (`/api/pairs/.../watermark`
    set wm=Y where Y > X). Pass `allow_regression=True` to override (used by
    the repair endpoint itself when you want to roll a watermark backwards).

    Extra per-pair keys (e.g. `transient_fail`) are preserved. When the new
    watermark advances past a tracked failing msg id, that counter is cleared.
    """
    async with _save_lock:
        path = _resolve_state_path()
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                state = json.load(f)
        else:
            state = {}
        entry = dict(state.get(name) or {})
        if not allow_regression:
            current = int(entry.get("last_msg_id", 0))
            if last_msg_id < current:
                return
        entry["last_msg_id"] = last_msg_id
        entry["updated_at"] = updated_at
        # Advancing past a stuck msg clears its consecutive-fail counter.
        tf = entry.get("transient_fail")
        if isinstance(tf, dict) and int(tf.get("msg_id", 0) or 0) <= last_msg_id:
            entry.pop("transient_fail", None)
        state[name] = entry
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        tmp.replace(path)


async def record_transient_fail(name: str, msg_id: int) -> int:
    """Bump the consecutive transient-fail counter for msg_id. Returns new count.

    Same msg_id → count+1. Different msg_id → reset to 1. Persisted in
    watermarks.json under pair.transient_fail so it survives restarts/cycles.
    """
    async with _save_lock:
        path = _resolve_state_path()
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                state = json.load(f)
        else:
            state = {}
        entry = dict(state.get(name) or {})
        prev = entry.get("transient_fail") if isinstance(entry.get("transient_fail"), dict) else {}
        if int(prev.get("msg_id", 0) or 0) == int(msg_id):
            count = int(prev.get("count", 0) or 0) + 1
        else:
            count = 1
        entry["transient_fail"] = {"msg_id": int(msg_id), "count": count}
        state[name] = entry
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        tmp.replace(path)
        return count


def _transient_skip_after(pair: dict) -> int:
    """How many consecutive transient failures on the same msg before skip+advance.

    Default 3. Set pair.transient_skip_after=0 (or negative) to never auto-skip
    (old pure-retry behaviour). 1 = skip on first transient failure.
    """
    raw = pair.get("transient_skip_after", 3)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 3


def _pair_key(pair: dict) -> str:
    return pair.get("name") or f"{pair['source']}:{pair['dest']}"


# ── Retry queue for messages skipped after consecutive transient failures ──
# Main watermark keeps advancing so the pair is not blocked; skipped media is
# parked here and re-attempted later (scheduler drain + manual API).
_retry_lock = asyncio.Lock()


def _retry_item_id(pair_name: str, src_id: int) -> str:
    return f"{pair_name}:{int(src_id)}"


def load_retry_queue() -> dict:
    """Return {items: [...]} from disk. Missing/corrupt → empty list."""
    path = _resolve_retry_queue_path()
    if not path.exists():
        return {"items": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return data
        if isinstance(data, list):
            return {"items": data}
    except (json.JSONDecodeError, OSError) as e:
        print(f"retry_queue.json unreadable, starting fresh: {e}", file=sys.stderr)
    return {"items": []}


async def _flush_retry_queue_locked(data: dict) -> None:
    """Caller must hold _retry_lock. Atomic write via .tmp+replace."""
    path = _resolve_retry_queue_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    tmp.replace(path)


async def enqueue_retry(
    pair: dict,
    msg,
    *,
    reason: str,
    size_bytes: int = 0,
    kind: str = "",
) -> dict:
    """Park a skipped message for later retry. Idempotent per (pair, src_id).

    Returns the queue item (existing or newly created).
    """
    name = _pair_key(pair)
    src_id = int(getattr(msg, "id", 0) or 0)
    if not src_id:
        return {}
    item_id = _retry_item_id(name, src_id)
    now = int(time.time())
    size = int(size_bytes or _msg_media_size_bytes(msg) or 0)
    if not kind:
        if getattr(msg, "photo", None):
            kind = "photo"
        elif getattr(msg, "document", None):
            kind = "document"
        elif getattr(msg, "video", None):
            kind = "video"
        elif getattr(msg, "media", None):
            kind = "media"
        else:
            kind = "message"
    async with _retry_lock:
        data = load_retry_queue()
        items = data.setdefault("items", [])
        for it in items:
            if it.get("id") == item_id:
                # Refresh reason / size; keep attempts so history is preserved.
                it["reason"] = str(reason)[:500]
                it["size_bytes"] = size
                it["kind"] = kind
                it["updated_at"] = now
                if it.get("status") == "done":
                    it["status"] = "pending"
                await _flush_retry_queue_locked(data)
                return it
        item = {
            "id": item_id,
            "pair": name,
            "src_id": src_id,
            "source": pair.get("source"),
            "dest": pair.get("dest"),
            "dest_topic": pair.get("dest_topic"),
            "drop_author": bool(pair.get("drop_author", True)),
            "reason": str(reason)[:500],
            "size_bytes": size,
            "kind": kind,
            "enqueued_at": now,
            "updated_at": now,
            "attempts": 0,
            "last_attempt_at": None,
            "last_error": None,
            "status": "pending",  # pending | dead
        }
        items.append(item)
        await _flush_retry_queue_locked(data)
        print(
            f"[{name}] queued for retry: src#{src_id} {kind}"
            f"{f' {size // 1024}KB' if size else ''} — {reason}",
            flush=True,
        )
        return item


async def remove_retry_items(item_ids: list[str]) -> int:
    """Delete items by id. Returns number removed."""
    wanted = {str(x) for x in item_ids}
    if not wanted:
        return 0
    async with _retry_lock:
        data = load_retry_queue()
        before = len(data.get("items") or [])
        data["items"] = [it for it in data.get("items") or [] if it.get("id") not in wanted]
        removed = before - len(data["items"])
        if removed:
            await _flush_retry_queue_locked(data)
        return removed


def _retry_min_interval_seconds(pair_cfg: Optional[dict] = None) -> int:
    """Min seconds between retry attempts for the same item (default 900 = 15min)."""
    raw = (pair_cfg or {}).get("retry_min_interval_seconds", 900)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 900


def _retry_max_attempts(pair_cfg: Optional[dict] = None) -> int:
    """Max drain attempts before marking dead. 0 = unlimited. Default 20."""
    raw = (pair_cfg or {}).get("retry_max_attempts", 20)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 20


async def drain_retry_queue(
    dl: TelegramDownloader,
    *,
    pair_name: Optional[str] = None,
    max_items: int = 5,
    force: bool = False,
    job: Optional[dict] = None,
) -> dict:
    """Re-attempt pending retry-queue items without touching the main watermark.

    Uses the same transport choice as normal runs (native if allowed, else copy).
    On success: remove item + record message_map. On transient fail: bump attempts
    and leave pending (or mark dead after retry_max_attempts). On permanent fail:
    mark dead so it stops burning cycles but remains visible for manual action.
    """
    cfg = load_pairs()
    pairs_by_name = {_pair_key(p): p for p in cfg.get("pairs", [])}
    now = int(time.time())
    ok = fail = skipped = 0
    results: list[dict] = []

    async with _retry_lock:
        data = load_retry_queue()
        items = list(data.get("items") or [])

    # Snapshot candidates outside the lock; mutate disk per-item under lock.
    candidates = []
    for it in items:
        if it.get("status") not in (None, "pending"):
            continue
        if pair_name and it.get("pair") != pair_name:
            continue
        pair = pairs_by_name.get(it.get("pair") or "")
        if not pair:
            # Pair gone — keep item but don't try.
            skipped += 1
            continue
        min_iv = _retry_min_interval_seconds(pair)
        last_at = it.get("last_attempt_at") or 0
        if not force and last_at and (now - int(last_at)) < min_iv:
            skipped += 1
            continue
        candidates.append((it, pair))
        if max_items and len(candidates) >= max_items:
            break

    if job:
        job.update({"status": "running", "total": len(candidates), "done": 0, "ok": 0, "fail": 0})

    if not await ensure_connected(dl.client, label="retry-queue"):
        return {
            "forwarded": 0, "failed": 0, "skipped": skipped + len(candidates),
            "error": "not connected", "results": [],
        }

    for idx, (it, pair) in enumerate(candidates):
        if job and job.get("cancel"):
            break
        name = it["pair"]
        src_id = int(it["src_id"])
        source = pair["source"]
        dest = pair["dest"]
        dest_topic = pair.get("dest_topic")
        drop_author = bool(pair.get("drop_author", True))
        max_att = _retry_max_attempts(pair)

        async with _retry_lock:
            data = load_retry_queue()
            cur = next((x for x in data.get("items") or [] if x.get("id") == it["id"]), None)
            if not cur or cur.get("status") not in (None, "pending"):
                skipped += 1
                continue
            cur["attempts"] = int(cur.get("attempts") or 0) + 1
            cur["last_attempt_at"] = int(time.time())
            cur["updated_at"] = cur["last_attempt_at"]
            attempt_n = cur["attempts"]
            await _flush_retry_queue_locked(data)

        print(f"[retry-queue] {name} src#{src_id} attempt {attempt_n}", flush=True)
        try:
            msgs = await dl.client.get_messages(source, ids=[src_id])
            msg = msgs[0] if msgs else None
            if msg is None:
                raise RuntimeError("source message missing (deleted?)")

            # Apply per-pair text replacements if any.
            override = None
            replacements = pair.get("replacements") or []
            if replacements and msg.message:
                transformed = apply_replacements(msg.message, replacements)
                if transformed != msg.message:
                    override = transformed

            src_protected = await dl.is_source_protected(source)
            sent = None
            if not src_protected and override is None:
                forwarded = await dl.forward_batch(
                    source, dest, [msg],
                    drop_author=drop_author, top_msg_id=dest_topic,
                )
                sent = forwarded[0] if forwarded else None
            else:
                # Protected sources, or caption rewrite needed → copy path.
                # When only replacements force copy on unprotected sources, still
                # prefer native+edit for speed; match main-path behaviour.
                if not src_protected:
                    forwarded = await dl.forward_batch(
                        source, dest, [msg],
                        drop_author=drop_author, top_msg_id=dest_topic,
                    )
                    sent = forwarded[0] if forwarded else None
                    if sent is not None and override is not None:
                        try:
                            await dl.client.edit_message(
                                dest, sent.id, override, parse_mode=None,
                            )
                        except MessageNotModifiedError:
                            pass
                        except Exception as e:
                            print(f"[retry-queue] edit after forward failed: {e}")
                else:
                    sent = await dl._copy_message_to(
                        msg, dest, dest_topic=dest_topic, text_override=override,
                    )

            if sent is None:
                raise TimeoutError("forward/copy returned no result")

            dest_msg_id = getattr(sent, "id", None)
            if dest_msg_id is not None:
                await record_mappings(name, [(src_id, dest_msg_id)])

            async with _retry_lock:
                data = load_retry_queue()
                data["items"] = [
                    x for x in data.get("items") or [] if x.get("id") != it["id"]
                ]
                await _flush_retry_queue_locked(data)

            ok += 1
            results.append({"id": it["id"], "src_id": src_id, "status": "ok",
                            "dest_id": dest_msg_id})
            print(f"[retry-queue] ✓ {name} src#{src_id} → dest#{dest_msg_id}", flush=True)

        except Exception as e:
            fail += 1
            err_s = f"{type(e).__name__}: {e}"
            transient = is_transient_error(e) or isinstance(e, TimeoutError)
            async with _retry_lock:
                data = load_retry_queue()
                cur = next((x for x in data.get("items") or [] if x.get("id") == it["id"]), None)
                if cur:
                    cur["last_error"] = err_s[:500]
                    cur["updated_at"] = int(time.time())
                    if (not transient) or (max_att > 0 and int(cur.get("attempts") or 0) >= max_att):
                        cur["status"] = "dead"
                        results.append({"id": it["id"], "src_id": src_id, "status": "dead",
                                        "error": err_s})
                        print(
                            f"[retry-queue] ✗ {name} src#{src_id} marked dead: {err_s}",
                            file=sys.stderr, flush=True,
                        )
                    else:
                        results.append({"id": it["id"], "src_id": src_id, "status": "pending",
                                        "error": err_s})
                        print(
                            f"[retry-queue] ⏳ {name} src#{src_id} will retry later: {err_s}",
                            flush=True,
                        )
                    await _flush_retry_queue_locked(data)

        if job:
            job.update({"done": idx + 1, "ok": ok, "fail": fail})

    if job and job.get("status") == "running":
        job["status"] = "finished" if not job.get("cancel") else "cancelled"
        job["finished_at"] = int(time.time())

    summary = {
        "forwarded": ok,
        "failed": fail,
        "skipped": skipped,
        "results": results,
    }
    print(
        f"[retry-queue] drain done: ok={ok} fail={fail} deferred={skipped}",
        flush=True,
    )
    return summary


def _msg_media_size_bytes(msg) -> int:
    """Largest media-attachment size in bytes, or 0 if no media.
    Used by the per-pair `max_file_size_mb` filter to skip huge files in
    copy-mode WITHOUT attempting download (adaptive timeouts still apply for
    allowed files). Native forwards don't care because Telegram relays the
    bytes server-side."""
    if getattr(msg, "document", None):
        return getattr(msg.document, "size", 0) or 0
    if getattr(msg, "video", None):
        return getattr(msg.video, "size", 0) or 0
    if getattr(msg, "audio", None):
        return getattr(msg.audio, "size", 0) or 0
    if getattr(msg, "voice", None):
        return getattr(msg.voice, "size", 0) or 0
    if getattr(msg, "photo", None):
        sizes = getattr(msg.photo, "sizes", []) or []
        return max((getattr(s, "size", 0) or 0) for s in sizes) if sizes else 0
    return 0


# Files larger than this share a single download slot so multi-GB videos can't
# pile up on disk during the download-ahead pipeline (important on OpenWrt).
_COPY_LARGE_FILE_BYTES = 20 * 1024 * 1024


def _copy_concurrency(pair: dict, dl: TelegramDownloader) -> int:
    """Clamp copy-mode download-ahead concurrency to 1–3."""
    raw = pair.get("copy_concurrency")
    if raw is None:
        raw = getattr(dl, "max_concurrent", 3)
    try:
        return max(1, min(3, int(raw)))
    except (TypeError, ValueError):
        return 1


def _matches_type(msg, ftype: str, dl: TelegramDownloader) -> bool:
    # Telegram service messages ("user joined", "channel created", "pinned X",
    # etc.) cannot be forwarded — forward_messages errors out and copy-mode has
    # nothing to send. Skip them regardless of ftype.
    from telethon.tl.patched import MessageService
    if isinstance(msg, MessageService):
        return False
    if ftype == "all":
        return True
    if ftype == "media":
        return dl._is_media(msg)
    if ftype == "documents":
        return dl._is_document(msg)
    if ftype == "messages":
        return bool(msg.message and not msg.media)
    if ftype == "docs_and_text":
        # documents (with or without caption) + text-only messages.
        # Skips photos/videos/voice/etc.
        return dl._is_document(msg) or bool(msg.message and not msg.media)
    return False


# Per-pair locks so the bulk runner, scheduler, and manual UI clicks can't
# concurrently iterate the same source — concurrent runs double-post because
# each runner loads the same watermark and walks the same message range.
_pair_locks: dict[str, asyncio.Lock] = {}


def _get_pair_lock(name: str) -> asyncio.Lock:
    lock = _pair_locks.get(name)
    if lock is None:
        lock = asyncio.Lock()
        _pair_locks[name] = lock
    return lock


async def run_pair(dl: TelegramDownloader, pair: dict, state: dict, job: Optional[dict] = None) -> dict:
    name = _pair_key(pair)
    lock = _get_pair_lock(name)
    if lock.locked():
        # Another runner has this pair. Bail out instead of double-posting.
        print(f"[{name}] another runner holds the lock — skipping this attempt")
        if job:
            job.update({"status": "finished", "total": 0})
        return {"forwarded": 0, "failed": 0, "last_id": int(state.get(name, {}).get("last_msg_id", 0)),
                "skipped_locked": True}
    async with lock:
        return await _run_pair_locked(dl, pair, state, job)


class TransientNetworkAbort(Exception):
    """Stop the current pair run without advancing past a failed message.

    Raised on single-message failures that look like temporary network blips.
    The watermark stays at the last successful id so the next cycle retries.
    """

    def __init__(self, msg_id: int, err: BaseException):
        self.msg_id = msg_id
        self.err = err
        super().__init__(f"msg #{msg_id}: {type(err).__name__}: {err}")


async def _run_pair_locked(dl: TelegramDownloader, pair: dict, state: dict, job: Optional[dict] = None) -> dict:
    name = _pair_key(pair)
    # Transition job out of "queued" immediately so the dashboard shows the
    # correct state even for zero-work runs (no new msgs → fast path returns
    # before any batch flush, so we'd otherwise stay stuck on queued).
    if job and job.get("status") == "queued":
        job["status"] = "running"
    source = pair["source"]
    dest = pair["dest"]
    source_topic = pair.get("source_topic")
    dest_topic = pair.get("dest_topic")
    ftype = pair.get("type", "all")
    delay = float(pair.get("delay_seconds", 1.0))
    max_per_run = int(pair.get("max_per_run", 0)) or None
    # Strip the "Forwarded from X" header on native forwards.
    # Default True (most cloning use-cases want a clean mirror).
    # Telegram requires the SENDING account to have Premium for this flag
    # to work; non-premium accounts should set drop_author:false in their pair.
    drop_author = bool(pair.get("drop_author", True))
    # Per-pair text replacements (list of {find, replace, regex}). Stays on
    # the native path: forward server-side, then edit the dest caption with
    # apply_replacements(text). Two API calls per changed msg vs a full
    # download+upload — still ~100x faster than copy-mode.
    replacements = pair.get("replacements") or []

    watermark = int(state.get(name, {}).get("last_msg_id", 0))

    # Bail early if Telegram is unreachable — leave watermark untouched so the
    # next scheduler cycle (after reconnect backoff) retries the same range.
    if not await ensure_connected(dl.client, label=name):
        print(f"[{name}] telegram not connected — skipping this run", file=sys.stderr)
        if job:
            job.update({"status": "error", "error": "not connected"})
        return {
            "forwarded": 0,
            "failed": 0,
            "last_id": watermark,
            "aborted_transient": True,
            "error": "not connected",
        }

    # Pick transport: native server-side forward (fast, no bandwidth) when
    # allowed, otherwise copy-mode (download + re-upload). Native works for:
    #   - any non-protected source (noforwards=False)
    #   - forum-topic dests (via raw ForwardMessagesRequest with top_msg_id —
    #     see dl.forward_batch)
    #   - pairs WITH replacements: forward first, then edit_message the dest
    #     caption with apply_replacements(text). Two API calls per msg instead
    #     of a download+upload — still 100x faster than copy-mode.
    # Only blocker: source has forwarding disabled (noforwards=True) → must
    # fall back to copy-mode because the bytes can't leave the channel.
    src_protected = await dl.is_source_protected(source)
    use_native = not src_protected

    mode = "native" if use_native else "copy"
    topic_note = ""
    if source_topic:
        topic_note += f" src_topic={source_topic}"
    if dest_topic:
        topic_note += f" dst_topic={dest_topic}"
    print(f"[{name}] source={source} dest={dest} type={ftype}{topic_note} since=#{watermark} mode={mode}")

    # Topic-aware iteration. reply_to=topic_id calls messages.getReplies and
    # returns only that topic's messages. Topic id=1 ("General") doesn't carry
    # reply_to, so server-side filter doesn't work — fall back to whole-chat
    # iteration in that case (handled by caller setting source_topic to null).
    iter_kwargs = {"min_id": watermark}
    if source_topic and source_topic > 1:
        iter_kwargs["reply_to"] = source_topic
    # When ftype="all" every message matches, so cap the fetch at max_per_run
    # — for cloned pairs sitting at watermark=0 this stops us from walking the
    # entire topic just to throw most of it away. For selective types we'd
    # undershoot, so leave uncapped there.
    if max_per_run and ftype == "all":
        iter_kwargs["limit"] = max_per_run

    ok = fail = 0
    last_ok_id = watermark

    if use_native:
        # Native server-side forward, STREAMING in batches of up to 100.
        # Telegram allows up to 100 ids per messages.forwardMessages call.
        # Streaming (instead of build-full-list-then-forward) keeps memory
        # flat on huge channels — 80k messages * 5KB/msg-object would otherwise
        # eat ~400MB before any forwarding starts and crash the worker.
        # iter_messages(reverse=True, min_id=X) yields msg.id > X in ASCENDING
        # order, so we forward chronologically and each successful batch
        # advances the watermark.
        BATCH = 100
        iter_kwargs.pop("limit", None)  # streaming: paginate via Telethon, cap by max_per_run below
        batch = []
        i = 0

        async def _do_forward(msgs_batch):
            if dest_topic and dest_topic > 1:
                return await dl.forward_batch(
                    source, dest, msgs_batch, drop_author=drop_author, top_msg_id=dest_topic
                )
            return await dl.client.forward_messages(
                dest, msgs_batch, source, drop_author=drop_author
            )

        async def _apply_edits(edits):
            # Caption rewrites after durable mapping. Bounded concurrency so a
            # batch of 100 replacements doesn't serialize 25s of 0.25s sleeps.
            if not edits:
                return
            sem = asyncio.Semaphore(5)

            async def _one(src_id, dest_id, new_text):
                async with sem:
                    try:
                        await dl.client.edit_message(dest, dest_id, new_text, parse_mode=None)
                    except FloodWaitError as fw:
                        print(f"[{name}] edit-after flood wait {fw.seconds}s at src#{src_id}")
                        await asyncio.sleep(fw.seconds + 1)
                        try:
                            await dl.client.edit_message(dest, dest_id, new_text, parse_mode=None)
                        except Exception as e:
                            print(f"[{name}] edit src#{src_id}->dst#{dest_id} retry failed: {e}")
                    except MessageNotModifiedError:
                        pass
                    except Exception as e:
                        print(f"[{name}] edit src#{src_id}->dst#{dest_id} failed: {type(e).__name__}: {e}")
                    await asyncio.sleep(0.1)

            await asyncio.gather(*[_one(s, d, t) for s, d, t in edits])

        async def _forward_slice(msgs_batch) -> bool:
            """Forward one slice; binary-split on failure so one bad id doesn't
            drop up to 99 good ones. Returns True if any msg was accepted.

            Permanent single-msg failures skip + advance watermark. Transient
            network failures normally raise TransientNetworkAbort so the run
            stops with the watermark left at the last successful id. After
            `transient_skip_after` consecutive failures on the SAME msg id
            (default 3), skip + advance so one stuck file can't block the pair
            forever on flaky links (OpenWrt etc.).
            """
            nonlocal ok, fail, last_ok_id, i
            if not msgs_batch:
                return False

            async def _attempt():
                try:
                    return await _do_forward(msgs_batch), None
                except FloodWaitError as fw:
                    print(f"[{name}] flood wait {fw.seconds}s")
                    await asyncio.sleep(fw.seconds + 1)
                    try:
                        return await _do_forward(msgs_batch), None
                    except Exception as e:
                        return None, e
                except Exception as e:
                    return None, e

            forwarded, err = await _attempt()
            if err is not None:
                if len(msgs_batch) == 1:
                    m = msgs_batch[0]
                    if is_transient_error(err):
                        fail += 1
                        skip_after = _transient_skip_after(pair)
                        count = await record_transient_fail(name, m.id)
                        if skip_after > 0 and count >= skip_after:
                            print(
                                f"[{name}] msg #{m.id} transient failure "
                                f"{count}/{skip_after}: {type(err).__name__}: {err} "
                                f"— skip after consecutive failures "
                                f"(watermark advances past #{m.id})",
                                file=sys.stderr,
                            )
                            await enqueue_retry(
                                pair, m,
                                reason=f"native transient x{count}: {type(err).__name__}: {err}",
                            )
                            last_ok_id = max(last_ok_id, m.id)
                            now = int(time.time())
                            state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
                            await save_pair_watermark(name, last_ok_id, now)
                            i += 1
                            return False
                        print(
                            f"[{name}] msg #{m.id} transient failure "
                            f"{count}/{skip_after or '∞'}: "
                            f"{type(err).__name__}: {err} — aborting run "
                            f"(watermark stays at #{last_ok_id})",
                            file=sys.stderr,
                        )
                        raise TransientNetworkAbort(m.id, err)
                    print(f"[{name}] msg #{m.id} failed: {err} — skipping")
                    fail += 1
                    # Advance past the single bad message so we don't loop forever.
                    last_ok_id = max(last_ok_id, m.id)
                    now = int(time.time())
                    state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
                    await save_pair_watermark(name, last_ok_id, now)
                    i += 1
                    return False
                mid = len(msgs_batch) // 2
                print(
                    f"[{name}] batch of {len(msgs_batch)} failed: {err} — "
                    f"splitting into {mid}+{len(msgs_batch) - mid}"
                )
                left = await _forward_slice(msgs_batch[:mid])
                right = await _forward_slice(msgs_batch[mid:])
                return left or right

            # Single-message forward_messages returns a Message (not a list)
            # in some Telethon versions. Normalize so we can always zip.
            forwarded_list = forwarded if isinstance(forwarded, (list, tuple)) else [forwarded]
            mappings = []
            edits = []
            for m, f in zip(msgs_batch, forwarded_list):
                if f is not None:
                    ok += 1
                    last_ok_id = m.id
                    dest_id = getattr(f, "id", None)
                    mappings.append((m.id, dest_id))
                    if replacements and m.message and dest_id is not None:
                        new_text = apply_replacements(m.message, replacements)
                        if new_text != m.message:
                            edits.append((m.id, dest_id, new_text))
                else:
                    fail += 1
            if mappings:
                await record_mappings(name, mappings)
            await _apply_edits(edits)
            i += len(msgs_batch)
            now = int(time.time())
            state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
            await save_pair_watermark(name, last_ok_id, now)
            if job:
                job.update({"done": i, "ok": ok, "fail": fail, "last_id": last_ok_id, "status": "running"})
            print(f"[{name}] progress {i} (ok={ok} fail={fail} last_id=#{last_ok_id})")
            return True

        async def _flush_batch():
            nonlocal batch
            if not batch:
                return False
            current = list(batch)
            batch.clear()
            return await _forward_slice(current)

        aborted_transient = False
        try:
            async for m in dl.client.iter_messages(source, reverse=True, **iter_kwargs):
                if job and job.get("cancel"):
                    print(f"[{name}] cancelled during scan at i={i}")
                    job["status"] = "cancelled"
                    break
                if not _matches_type(m, ftype, dl):
                    continue
                batch.append(m)
                if len(batch) >= BATCH:
                    await _flush_batch()
                    if max_per_run and i >= max_per_run:
                        break
                    await asyncio.sleep(delay)
            # Final partial batch.
            if batch and not (job and job.get("cancel")) and not aborted_transient:
                await _flush_batch()
        except TransientNetworkAbort as abort:
            aborted_transient = True
            batch.clear()
            print(
                f"[{name}] aborted on transient network error at src#{abort.msg_id}; "
                f"will retry from watermark=#{last_ok_id}",
                file=sys.stderr,
            )
            if job and job.get("status") == "running":
                job["status"] = "error"
                job["error"] = str(abort)
        if job and job["status"] == "running":
            job["status"] = "finished"
        print(f"[{name}] done: forwarded={ok} failed={fail} new_watermark=#{last_ok_id}"
              f"{' (aborted_transient)' if aborted_transient else ''}")
        return {
            "forwarded": ok,
            "failed": fail,
            "last_id": last_ok_id,
            **({"aborted_transient": True} if aborted_transient else {}),
        }

    # ── Copy-mode (protected source) — stream + download-ahead + ordered upload.
    # Streaming keeps memory flat (mirrors native). Download-ahead overlaps CDN
    # fetch with the previous upload; uploads stay sequential so dest order and
    # per-message watermark semantics match the old serial path.
    # `max_file_size_mb` skips huge files BEFORE download. 0 / missing = unlimited.
    max_size_bytes = int(pair.get("max_file_size_mb", 0) or 0) * 1024 * 1024
    concurrency = _copy_concurrency(pair, dl)
    iter_kwargs.pop("limit", None)  # stream; cap via max_per_run below
    (BASE_DIR / "temp").mkdir(exist_ok=True)

    if job:
        job.update({"status": "running"})

    print(f"[{name}] copy pipeline concurrency={concurrency}")

    download_sem = asyncio.Semaphore(concurrency)
    large_sem = asyncio.Semaphore(1)  # at most one >20MB download at a time
    # Ordered handoff: producer fills slots by arrival order; uploader drains 0..n
    ready: dict[int, dict] = {}
    ready_event = asyncio.Event()
    producer_done = False
    cancelled = False
    # Transient network abort: stop producer + uploader without advancing past
    # the failed message (watermark stays at last successful id).
    aborted_transient = False
    next_upload_idx = 0
    next_slot = 0
    skipped_oversize = 0
    inflight: set[asyncio.Task] = set()

    def _media_kind(m) -> str:
        if getattr(m, "photo", None):
            return " photo"
        if getattr(m, "document", None):
            dsize = getattr(m.document, "size", 0) or 0
            return f" doc/{dsize // 1024}KB" if dsize else " doc"
        if getattr(m, "video", None):
            return " video"
        if getattr(m, "media", None):
            return " media"
        return ""

    async def _download_slot(slot: int, m) -> None:
        override = None
        if replacements and m.message:
            transformed = apply_replacements(m.message, replacements)
            if transformed != m.message:
                override = transformed
        size = _msg_media_size_bytes(m)
        use_large = size > _COPY_LARGE_FILE_BYTES
        try:
            async with download_sem:
                if use_large:
                    async with large_sem:
                        payload = await dl._download_copy_media(m)
                else:
                    payload = await dl._download_copy_media(m)
            ready[slot] = {
                "msg": m, "payload": payload, "override": override, "error": None,
            }
        except Exception as e:
            ready[slot] = {
                "msg": m, "payload": None, "override": override, "error": e,
            }
        ready_event.set()

    async def _producer():
        nonlocal next_slot, skipped_oversize, producer_done, cancelled
        try:
            async for m in dl.client.iter_messages(source, reverse=True, **iter_kwargs):
                if job and job.get("cancel"):
                    cancelled = True
                    print(f"[{name}] cancelled during stream at slot={next_slot}")
                    break
                if aborted_transient:
                    break
                if not _matches_type(m, ftype, dl):
                    continue
                if max_size_bytes > 0:
                    sz = _msg_media_size_bytes(m)
                    if sz > max_size_bytes:
                        skipped_oversize += 1
                        print(
                            f"[{name}] skip #{m.id}: media {sz // (1024*1024)} MB "
                            f"> cap {pair.get('max_file_size_mb')} MB"
                        )
                        continue
                if max_per_run and next_slot >= max_per_run:
                    break
                slot = next_slot
                next_slot += 1
                print(f"[{name}] → dl {slot + 1} src#{m.id}{_media_kind(m)}", flush=True)
                task = asyncio.create_task(_download_slot(slot, m))
                inflight.add(task)
                task.add_done_callback(inflight.discard)
                # Bound in-flight downloads roughly to concurrency so we don't
                # queue thousands of tasks on a huge backlog.
                while len(inflight) >= concurrency * 2:
                    if aborted_transient or cancelled:
                        break
                    await asyncio.sleep(0.05)
                    if job and job.get("cancel"):
                        cancelled = True
                        break
                if cancelled:
                    break
        finally:
            if inflight:
                await asyncio.gather(*list(inflight), return_exceptions=True)
            producer_done = True
            ready_event.set()
            if skipped_oversize:
                print(
                    f"[{name}] filter: skipped {skipped_oversize} oversize msg(s) — "
                    f"they'll be re-evaluated next run if cap is raised"
                )

    async def _uploader():
        nonlocal ok, fail, last_ok_id, next_upload_idx, cancelled, aborted_transient
        processed = 0
        while True:
            if job and job.get("cancel"):
                cancelled = True
            if aborted_transient:
                return
            while next_upload_idx not in ready:
                if aborted_transient:
                    return
                if producer_done and next_upload_idx >= next_slot:
                    return
                if cancelled and next_upload_idx not in ready:
                    # Drain any already-finished slots so temps get cleaned; stop
                    # waiting forever for slots the producer abandoned.
                    if producer_done:
                        return
                ready_event.clear()
                if next_upload_idx in ready:
                    break
                if producer_done and next_upload_idx >= next_slot:
                    return
                try:
                    await asyncio.wait_for(ready_event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass

            item = ready.pop(next_upload_idx)
            m = item["msg"]
            payload = item["payload"]
            override = item["override"]
            err = item["error"]
            processed += 1

            async def _handle_transient(reason: BaseException | str) -> bool:
                """Count consecutive transient fails on this msg.

                Returns True if we skipped + advanced (caller continues).
                Returns False if we aborted the run (caller must stop).
                """
                nonlocal aborted_transient, fail, last_ok_id
                fail += 1
                if isinstance(payload, dict) and payload.get("kind") == "media":
                    dl._unlink_quiet(payload.get("temp_path"))
                skip_after = _transient_skip_after(pair)
                count = await record_transient_fail(name, m.id)
                if skip_after > 0 and count >= skip_after:
                    print(
                        f"[{name}] transient failure at src#{m.id} "
                        f"{count}/{skip_after}: {reason} — skip after consecutive "
                        f"failures (watermark advances past #{m.id})",
                        file=sys.stderr,
                        flush=True,
                    )
                    await enqueue_retry(
                        pair, m,
                        reason=f"copy transient x{count}: {reason}",
                    )
                    last_ok_id = max(last_ok_id, m.id)
                    now = int(time.time())
                    state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
                    await save_pair_watermark(name, last_ok_id, now)
                    return True
                aborted_transient = True
                print(
                    f"[{name}] transient failure at src#{m.id} "
                    f"{count}/{skip_after or '∞'}: {reason} — "
                    f"aborting copy run (watermark stays at #{last_ok_id})",
                    file=sys.stderr,
                    flush=True,
                )
                return False

            if err is not None:
                print(f"[{name}] ✗ dl src#{m.id}: {err}", flush=True)
                if is_transient_error(err):
                    if not await _handle_transient(err):
                        if job:
                            job.update({
                                "done": processed, "ok": ok, "fail": fail,
                                "last_id": last_ok_id, "total": max(next_slot, processed),
                                "status": "error",
                                "error": "transient network abort",
                            })
                        return
                    # skipped — fall through to advance next_upload_idx
                else:
                    # Permanent download error: skip this message and advance watermark
                    # so a single bad media item cannot stall the pair forever.
                    fail += 1
                    last_ok_id = max(last_ok_id, m.id)
                    now = int(time.time())
                    state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
                    await save_pair_watermark(name, last_ok_id, now)
            elif payload is None:
                # download gave up after internal retries (timeouts etc.) — treat
                # as transient so the next cycle re-tries this message (until
                # transient_skip_after is hit).
                if not await _handle_transient("download returned no payload after retries"):
                    if job:
                        job.update({
                            "done": processed, "ok": ok, "fail": fail,
                            "last_id": last_ok_id, "total": max(next_slot, processed),
                            "status": "error",
                            "error": "transient network abort",
                        })
                    return
            else:
                try:
                    sent = await dl._send_copy_payload(
                        m, dest, payload,
                        dest_topic=dest_topic, text_override=override,
                    )
                except Exception as e:
                    print(f"[{name}] ✗ ul src#{m.id}: {e}", flush=True)
                    if is_transient_error(e):
                        if not await _handle_transient(e):
                            if job:
                                job.update({
                                    "done": processed, "ok": ok, "fail": fail,
                                    "last_id": last_ok_id, "total": max(next_slot, processed),
                                    "status": "error",
                                    "error": "transient network abort",
                                })
                            return
                        sent = "skipped_permanent"
                    else:
                        if isinstance(payload, dict) and payload.get("kind") == "media":
                            dl._unlink_quiet(payload.get("temp_path"))
                        # Permanent upload error: skip + advance watermark.
                        fail += 1
                        last_ok_id = max(last_ok_id, m.id)
                        now = int(time.time())
                        state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
                        await save_pair_watermark(name, last_ok_id, now)
                        sent = "skipped_permanent"
                if sent and sent != "skipped_permanent":
                    ok += 1
                    last_ok_id = m.id
                    now = int(time.time())
                    state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
                    await save_pair_watermark(name, last_ok_id, now)
                    dest_msg_id = getattr(sent, "id", None)
                    if dest_msg_id is not None:
                        await record_mappings(name, [(m.id, dest_msg_id)])
                elif sent is None and not aborted_transient:
                    # upload returned None after internal retries → treat as
                    # transient so the next cycle re-tries this message.
                    if not await _handle_transient("upload returned no result after retries"):
                        if job:
                            job.update({
                                "done": processed, "ok": ok, "fail": fail,
                                "last_id": last_ok_id, "total": max(next_slot, processed),
                                "status": "error",
                                "error": "transient network abort",
                            })
                        return

            if aborted_transient:
                if job:
                    job.update({
                        "done": processed, "ok": ok, "fail": fail,
                        "last_id": last_ok_id, "total": max(next_slot, processed),
                        "status": "error",
                        "error": "transient network abort",
                    })
                return

            if job:
                job.update({
                    "done": processed, "ok": ok, "fail": fail,
                    "last_id": last_ok_id, "total": max(next_slot, processed),
                })
            if processed % 25 == 0:
                print(f"[{name}] progress {processed} (ok={ok} fail={fail})", flush=True)
            next_upload_idx += 1
            await asyncio.sleep(delay)

    try:
        await asyncio.gather(_producer(), _uploader())
    finally:
        # Drop any leftover payloads (cancel / crash mid-pipeline).
        for item in ready.values():
            payload = item.get("payload")
            if isinstance(payload, dict) and payload.get("kind") == "media":
                dl._unlink_quiet(payload.get("temp_path"))
        ready.clear()
        try:
            import shutil
            shutil.rmtree(BASE_DIR / "temp", ignore_errors=True)
        except Exception:
            pass

    if job:
        if cancelled or (job.get("cancel") and job.get("status") not in ("finished", "error")):
            job["status"] = "cancelled"
        elif aborted_transient and job.get("status") == "running":
            job["status"] = "error"
            job["error"] = "transient network abort"
        elif job.get("status") == "running":
            job["status"] = "finished"

    if next_slot == 0 and not aborted_transient:
        print(f"[{name}] no new messages")
    print(
        f"[{name}] done: forwarded={ok} failed={fail} new_watermark=#{last_ok_id}"
        f"{' (aborted_transient)' if aborted_transient else ''}"
        f"{' (cancelled)' if cancelled and not aborted_transient else ''}"
    )
    return {
        "forwarded": ok, "failed": fail, "last_id": last_ok_id,
        **({"cancelled": True} if cancelled and not aborted_transient else {}),
        **({"aborted_transient": True} if aborted_transient else {}),
    }


async def run_once(dl: Optional[TelegramDownloader] = None) -> dict:
    cfg = load_pairs()
    state = load_state()
    pairs = cfg.get("pairs", [])
    if not pairs:
        print("No pairs configured — nothing to do.")
        return {}

    own = False
    if dl is None:
        dl = TelegramDownloader(load_config())
        await dl.start()
        own = True

    summary = {}
    try:
        for pair in pairs:
            try:
                summary[_pair_key(pair)] = await run_pair(dl, pair, state)
            except Exception as e:
                print(f"[{_pair_key(pair)}] FAILED: {e}", file=sys.stderr)
                summary[_pair_key(pair)] = {"error": str(e)}
    finally:
        if own:
            await dl.stop()
    return summary


async def main():
    cfg = load_pairs()
    interval = int(cfg.get("interval_seconds", 3600))
    run_once_only = os.environ.get("RUN_ONCE_AND_EXIT") == "1"

    dl = TelegramDownloader(load_config())
    await dl.start()

    try:
        while True:
            t0 = time.time()
            try:
                await run_once(dl=dl)
            except Exception as e:
                print(f"run_once crashed: {e}", file=sys.stderr)
            if run_once_only:
                return
            elapsed = time.time() - t0
            sleep_for = max(60, interval - int(elapsed))
            print(f"sleeping {sleep_for}s until next run...")
            await asyncio.sleep(sleep_for)
    finally:
        await dl.stop()


if __name__ == "__main__":
    asyncio.run(main())
