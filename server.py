"""
Web UI + REST API for telegram-forwarder.

One Quart app that:
  - serves the single-page UI on /
  - exposes JSON endpoints (auth required) for chat lists, pair CRUD,
    manual runs, one-shot forwards, and run history
  - runs the background scheduler in the same event loop, sharing one
    TelegramClient with the HTTP handlers
  - exposes /healthz unauthenticated for platform healthchecks
"""

import asyncio
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from quart import Quart, request, jsonify, render_template, Response

from telethon import events, utils as tg_utils
from telethon.errors import MessageNotModifiedError
from telethon.tl.functions.channels import CreateChannelRequest, ToggleForumRequest
from telethon.tl.functions.messages import CreateForumTopicRequest

from downloader import TelegramDownloader, load_config, retry_once_on_flood
from automate import (
    BASE_DIR,
    load_pairs,
    save_pairs,
    load_state,
    save_state,
    save_pair_watermark,
    run_pair,
    _pair_key,
    _get_pair_lock,
    _matches_type,
    apply_replacements,
    load_message_map,
    flush_message_map,
    lookup_dest_id,
    forget_mappings,
    mapped_src_ids,
    record_mappings,
    load_retry_queue,
    remove_retry_items,
    drain_retry_queue,
)
from retry_utils import ensure_connected, is_fatal_session_error, sleep_backoff

# ───── State ──────────────────────────────────────────────────────────────

app = Quart(__name__)
app.config["PROVIDE_AUTOMATIC_OPTIONS"] = True
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True

DASH_USER = os.environ.get("DASH_USER", "admin")
DASH_PASS = os.environ.get("DASH_PASS")  # if unset, auth disabled (local dev only)
# Set ALLOW_INSECURE_OPEN_DASH=1 to silence the open-dash warning (e.g. SSH-tunnel only).
ALLOW_INSECURE_OPEN_DASH = os.environ.get("ALLOW_INSECURE_OPEN_DASH", "").strip() in ("1", "true", "yes")

_state_lock = asyncio.Lock()        # protects pairs.json + watermarks.json + run_log
_dl: Optional[TelegramDownloader] = None  # shared Telegram client
_scheduler_task: Optional[asyncio.Task] = None
_telegram_session_error: Optional[str] = None
_run_log: list[dict] = []           # in-memory ring buffer
_RUN_LOG_MAX = 500
RUN_LOG_PATH = Path(os.environ.get("RUN_LOG_PATH", str(BASE_DIR / "run_log.json")))

# Job registry — live + recently-finished jobs. status: queued|running|finished|cancelled|error
_jobs: dict[str, dict] = {}
_JOBS_MAX = 200
_job_counter = 0

# Global pause flag. When True: scheduler skips new pair runs, bulks halt
# between pairs, all in-flight jobs receive cancel. Survives nothing on
# restart — re-pause after deploy if you still want the brake on.
_paused: bool = False


def _new_job(kind: str, label: str, total: int = 0) -> dict:
    global _job_counter
    _job_counter += 1
    job_id = f"job-{_job_counter}-{int(time.time())}"
    job = {
        "id": job_id,
        "kind": kind,
        "label": label,
        "status": "queued",
        "started_at": int(time.time()),
        "finished_at": None,
        "total": total,
        "done": 0,
        "ok": 0,
        "fail": 0,
        "last_id": None,
        "cancel": False,
    }
    _jobs[job_id] = job
    if len(_jobs) > _JOBS_MAX:
        # Evict oldest finished jobs.
        finished = [j for j in _jobs.values() if j["status"] in ("finished", "cancelled", "error")]
        finished.sort(key=lambda j: j["finished_at"] or 0)
        for j in finished[: len(_jobs) - _JOBS_MAX]:
            _jobs.pop(j["id"], None)
    return job


def _log_event(event: dict) -> None:
    event = {"ts": int(time.time()), **event}
    _run_log.append(event)
    if len(_run_log) > _RUN_LOG_MAX:
        del _run_log[: len(_run_log) - _RUN_LOG_MAX]
    try:
        from automate import _atomic_write_json
        # fsync=False: ring-buffer log, losing the tail on power-cut is fine —
        # not worth stalling the event loop on slow flash for every event.
        _atomic_write_json(RUN_LOG_PATH, _run_log[-_RUN_LOG_MAX:], fsync=False)
    except Exception:
        pass


def _load_run_log() -> None:
    if RUN_LOG_PATH.exists():
        try:
            with open(RUN_LOG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    _run_log.extend(data[-_RUN_LOG_MAX:])
        except Exception as e:
            print(f"failed to load run log: {e}", file=sys.stderr)


# ───── Auth ───────────────────────────────────────────────────────────────


def _check_auth(header: Optional[str]) -> bool:
    if not DASH_PASS:
        return True  # disabled in local dev
    if not header or not header.startswith("Basic "):
        return False
    import base64
    try:
        raw = base64.b64decode(header[6:]).decode("utf-8")
    except Exception:
        return False
    if ":" not in raw:
        return False
    user, _, pw = raw.partition(":")
    return secrets.compare_digest(user, DASH_USER) and secrets.compare_digest(pw, DASH_PASS)


def _auth_required():
    return Response(
        "Authentication required",
        401,
        {"WWW-Authenticate": 'Basic realm="telegram-forwarder"'},
    )


@app.before_request
async def _auth_gate():
    path = request.path or ""
    if path == "/healthz":
        return None
    if not _check_auth(request.headers.get("Authorization")):
        return _auth_required()


# ───── Routes: HTML ───────────────────────────────────────────────────────


@app.route("/")
async def index():
    return await render_template("index.html")


@app.route("/healthz")
async def healthz():
    """Liveness + Telegram readiness.

    - ok: process is serving
    - telegram_ready: client object exists
    - telegram_connected: client reports is_connected() (best-effort probe)
    """
    ready = _dl is not None and _dl.client is not None
    connected = False
    if ready:
        try:
            connected = bool(_dl.client.is_connected())
        except Exception:
            connected = False
    status = 200 if ready else 503
    return jsonify({
        "ok": True,
        "telegram_ready": ready,
        "telegram_connected": connected,
        "telegram_session_error": _telegram_session_error,
    }), status


# ───── Routes: chats ──────────────────────────────────────────────────────


@app.route("/api/chats")
async def api_chats():
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    limit = int(request.args.get("limit", 300))
    try:
        chats = await _dl.list_chats(limit=limit)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[api/chats] ERROR: {type(e).__name__}: {e}\n{tb}", file=sys.stderr)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500
    return jsonify({"chats": chats})


@app.route("/api/topics")
async def api_topics():
    """List forum topics for a chat. Returns {"topics": []} if the chat isn't
    a forum supergroup (so the UI can hide the topic picker)."""
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    chat_id_str = request.args.get("chat")
    if not chat_id_str:
        return jsonify({"error": "chat parameter required"}), 400
    try:
        chat_id = int(chat_id_str)
    except ValueError:
        return jsonify({"error": "chat must be an integer"}), 400
    try:
        topics = await _dl.list_topics(chat_id)
    except Exception:
        # Not a forum, or no access — caller treats as "no topics".
        return jsonify({"topics": []})
    return jsonify({"topics": topics})


# ───── Routes: pairs ──────────────────────────────────────────────────────


@app.route("/api/pairs")
async def api_pairs_get():
    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        state = load_state()
    enriched = []
    for p in cfg.get("pairs", []):
        key = _pair_key(p)
        wm = state.get(key, {})
        enriched.append({**p, "watermark": wm.get("last_msg_id", 0), "updated_at": wm.get("updated_at", 0)})
    return jsonify({"interval_seconds": cfg.get("interval_seconds", 3600), "pairs": enriched})


def _pairs_file_exists() -> bool:
    from automate import _resolve_pairs_path
    return _resolve_pairs_path().exists()


@app.route("/api/pairs", methods=["POST"])
async def api_pairs_post():
    body = await request.get_json(force=True)
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    def _opt_int(v):
        if v in (None, "", "null"):
            return None
        return int(v)
    try:
        new_pair = {
            "name": name,
            "source": int(body["source"]),
            "dest": int(body["dest"]),
            "source_topic": _opt_int(body.get("source_topic")),
            "dest_topic": _opt_int(body.get("dest_topic")),
            "type": body.get("type", "all"),
            "delay_seconds": float(body.get("delay_seconds", 1.0)),
            "max_per_run": int(body.get("max_per_run", 200)),
        }
        # Only persist drop_author when caller actually sent it — otherwise
        # leave it absent so run_pair's default (True) applies.
        if "drop_author" in body:
            new_pair["drop_author"] = bool(body["drop_author"])
        # When paused, scheduler skips this pair. Manual runs still work.
        if "paused" in body:
            new_pair["paused"] = bool(body["paused"])
        # Copy-mode media size cap (MB). 0/missing = unlimited. Only consulted
        # in copy-mode runs — native forwards always relay server-side.
        if "max_file_size_mb" in body:
            raw = body.get("max_file_size_mb")
            if raw not in (None, "", "null"):
                try:
                    v = int(raw)
                    if v > 0:
                        new_pair["max_file_size_mb"] = v
                except (TypeError, ValueError):
                    return jsonify({"error": "max_file_size_mb must be a non-negative integer"}), 400
        # Optional per-pair text replacements. Schema is a list of
        # {find, replace, regex?} dicts. Pairs with replacements stay on the
        # native path — run_pair forwards server-side and then edits the dest
        # caption (parse_mode=None) with apply_replacements(text).
        if "replacements" in body:
            raw = body.get("replacements") or []
            if not isinstance(raw, list):
                return jsonify({"error": "replacements must be a list"}), 400
            cleaned = []
            for r in raw:
                if not isinstance(r, dict) or not r.get("find"):
                    continue
                cleaned.append({
                    "find": str(r.get("find", "")),
                    "replace": str(r.get("replace", "")),
                    "regex": bool(r.get("regex", False)),
                })
            if cleaned:
                new_pair["replacements"] = cleaned
        # Drop nulls so JSON stays clean for users who don't use topics.
        new_pair = {k: v for k, v in new_pair.items() if v is not None}
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid pair: {e}"}), 400

    if new_pair["type"] not in ("all", "media", "documents", "messages", "docs_and_text"):
        return jsonify({"error": "type must be one of all|media|documents|messages|docs_and_text"}), 400

    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        existing = next((p for p in cfg["pairs"] if p.get("name") == name), None)
        if existing:
            existing.update(new_pair)
            action = "updated"
        else:
            cfg.setdefault("pairs", []).append(new_pair)
            action = "added"
        if body.get("interval_seconds"):
            cfg["interval_seconds"] = int(body["interval_seconds"])
        save_pairs(cfg)
    _log_event({"kind": "pair_saved", "pair": name, "action": action})
    return jsonify({"ok": True, "action": action, "pair": new_pair})


@app.route("/api/pairs/<name>", methods=["DELETE"])
async def api_pair_delete(name: str):
    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        before = len(cfg.get("pairs", []))
        cfg["pairs"] = [p for p in cfg.get("pairs", []) if p.get("name") != name]
        save_pairs(cfg)
        removed = before - len(cfg["pairs"])
        # Don't delete watermark — keeping it means if you re-add the pair you don't re-forward history.
    _log_event({"kind": "pair_deleted", "pair": name, "removed": removed})
    return jsonify({"ok": True, "removed": removed})


@app.route("/api/pairs/<name>/pause", methods=["POST"])
async def api_pair_pause(name: str):
    """Toggle a pair's paused flag. Body: {"paused": true/false}.
    Paused pairs are skipped by the scheduler; manual runs still work."""
    body = await request.get_json(force=True)
    paused = bool(body.get("paused", True))
    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
        pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
        if not pair:
            return jsonify({"error": f"pair '{name}' not found"}), 404
        pair["paused"] = paused
        save_pairs(cfg)
    _log_event({"kind": "pair_paused" if paused else "pair_unpaused", "pair": name})
    return jsonify({"ok": True, "pair": name, "paused": paused})


@app.route("/api/pairs/<name>/gaps")
async def api_pair_gaps(name: str):
    """Scan the source channel and return src msg ids that are NOT in our
    message_map for this pair — i.e., messages that exist in the source but
    were never successfully forwarded. Useful for finding what we missed.

    Query: ?limit=N restricts how far back we scan (default: full channel).
    Returns: {"source_count": N, "mapped_count": M, "missing": [id, ...]}
    """
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
    if not pair:
        return jsonify({"error": f"pair '{name}' not found"}), 404
    limit = request.args.get("limit")
    limit = int(limit) if limit else None
    source = pair["source"]
    ftype = pair.get("type", "all")
    mapped = mapped_src_ids(name)
    source_ids: list[int] = []
    iter_kwargs = {"limit": limit} if limit else {}
    async for m in _dl.client.iter_messages(source, **iter_kwargs):
        if not _matches_type(m, ftype, _dl):
            continue
        source_ids.append(m.id)
    source_set = set(source_ids)
    missing = sorted(source_set - mapped)
    return jsonify({
        "source_count": len(source_set),
        "mapped_count": len(mapped & source_set),
        "missing_count": len(missing),
        "missing": missing[:1000],  # cap response size; UI shows count
        "missing_truncated": len(missing) > 1000,
    })


@app.route("/api/pairs/<name>/repair", methods=["POST"])
async def api_pair_repair(name: str):
    """Forward the specific src ids passed in body — bypasses watermark logic.
    Body: {"src_ids": [123, 456, ...]} — typically the output of /gaps.
    Uses native batched forward when allowed; falls back to copy-mode for
    protected (noforwards) sources. Holds the per-pair lock so concurrent
    run_pair / drain cannot double-post.
    """
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    body = await request.get_json(force=True)
    src_ids = body.get("src_ids") or []
    if not isinstance(src_ids, list) or not src_ids:
        return jsonify({"error": "src_ids must be a non-empty list"}), 400
    src_ids = [int(x) for x in src_ids]

    cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
    if not pair:
        return jsonify({"error": f"pair '{name}' not found"}), 404
    source = pair["source"]
    dest = pair["dest"]
    dest_topic = pair.get("dest_topic")
    drop_author = bool(pair.get("drop_author", True))
    replacements = pair.get("replacements") or []

    pair_lock = _get_pair_lock(name)
    if pair_lock.locked():
        return jsonify({"error": f"pair '{name}' is already running"}), 409

    job = _new_job(kind="pair-repair", label=f"pair:{name} (repair {len(src_ids)} msgs)")
    job["total"] = len(src_ids)
    job["status"] = "running"
    _log_event({"kind": "repair_started", "pair": name, "count": len(src_ids), "job_id": job["id"]})

    async with pair_lock:
        # Fetch source messages by id (Telethon: get_messages with explicit ids).
        msgs = await _dl.client.get_messages(source, ids=src_ids)
        msgs = [m for m in msgs if m is not None]
        protected = await _dl.is_source_protected(source)
        ok, fail = 0, 0

        if protected:
            import shutil
            import uuid as _uuid
            run_temp = BASE_DIR / "temp" / f"repair-{_uuid.uuid4().hex[:10]}"
            run_temp.mkdir(parents=True, exist_ok=True)
            try:
                for i, m in enumerate(msgs, 1):
                    if job.get("cancel"):
                        job["status"] = "cancelled"
                        break
                    override = None
                    if replacements and m.message:
                        transformed = apply_replacements(m.message, replacements)
                        if transformed != m.message:
                            override = transformed
                    try:
                        sent = await _dl._copy_message_to(
                            m, dest, dest_topic=dest_topic,
                            text_override=override, temp_dir=run_temp,
                        )
                        if sent is not None:
                            ok += 1
                            dest_id = getattr(sent, "id", None)
                            if dest_id is not None:
                                await record_mappings(
                                    name, [(m.id, dest_id)], force_flush=True,
                                )
                        else:
                            fail += 1
                    except Exception as e:
                        print(f"[repair {name}] copy #{m.id} failed: {e}", file=sys.stderr)
                        fail += 1
                    job.update({"done": i, "ok": ok, "fail": fail})
            finally:
                shutil.rmtree(run_temp, ignore_errors=True)
        else:
            BATCH = 100
            for i in range(0, len(msgs), BATCH):
                if job.get("cancel"):
                    job["status"] = "cancelled"
                    break
                batch = msgs[i:i + BATCH]
                try:
                    forwarded = await _dl.forward_batch(
                        source, dest, batch, drop_author=drop_author, top_msg_id=dest_topic,
                    )
                    mappings = []
                    for src_msg, fwd in zip(batch, forwarded):
                        if fwd is not None:
                            ok += 1
                            dest_id = getattr(fwd, "id", None)
                            mappings.append((src_msg.id, dest_id))
                            if replacements and src_msg.message and dest_id is not None:
                                new_text = apply_replacements(src_msg.message, replacements)
                                if new_text != src_msg.message:
                                    try:
                                        await _dl.client.edit_message(
                                            dest, dest_id, new_text, parse_mode=None,
                                        )
                                    except MessageNotModifiedError:
                                        pass
                                    except Exception as e:
                                        print(f"[repair {name}] edit after forward failed: {e}")
                        else:
                            fail += 1
                    if mappings:
                        await record_mappings(name, mappings, force_flush=True)
                except Exception as e:
                    print(f"[repair {name}] batch failed: {e}")
                    fail += len(batch)
                job.update({"done": min(i + BATCH, len(msgs)), "ok": ok, "fail": fail})

    job["finished_at"] = int(time.time())
    if job["status"] == "running":
        job["status"] = "finished"
    _log_event({"kind": "repair_finished", "pair": name, "ok": ok, "fail": fail, "job_id": job["id"]})
    return jsonify({"ok": True, "forwarded": ok, "failed": fail, "job_id": job["id"], "mode": "copy" if protected else "native"})


@app.route("/api/multi-forward", methods=["POST"])
async def api_multi_forward():
    """One-shot: index a source channel, dedupe against a dest supergroup
    across all topics, and forward missing files into topics chosen by regex
    rules on the filename.

    Body schema:
        {
            "source": int,                  # required, e.g. -1001303766825
            "dest":   int,                  # required, e.g. -1003776591963
            "rules":  [                     # required, ordered (first match wins)
                {"pattern": "S\\d+E\\d+|Season \\d+", "topic": 10, "label": "TV show"},
                ...
            ],
            "default_topic": int,           # required — fallback when no rule matches
            "dry_run": bool,                # optional, default False
            "skip_dest_index": bool,        # optional, default False (re-use existing dst_msgs)
            "pair_name": str,               # optional — for message_map; default "multi-<abs(source)>"
            "drop_author": bool             # optional, default True
        }

    Returns immediately with {ok, job_id}. Progress visible via /api/jobs.
    """
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    body = await request.get_json(force=True)
    # Basic validation
    for k in ("source", "dest", "rules", "default_topic"):
        if k not in body:
            return jsonify({"error": f"missing field: {k}"}), 400
    if not isinstance(body["rules"], list):
        return jsonify({"error": "rules must be a list"}), 400
    # Verify regexes compile before we kick off the job — fail fast.
    from automate_multi import compile_rules, run_multi_forward
    try:
        compile_rules(body["rules"])
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    label = f"multi-fwd src={body['source']} → dest={body['dest']}"
    job = _new_job(kind="multi-forward", label=label)
    job["status"] = "running"

    async def _run():
        try:
            await run_multi_forward(_dl, body, job)
        except Exception as e:
            print(f"[multi-fwd] FAILED: {type(e).__name__}: {e}")
            import traceback; traceback.print_exc()
            job["status"] = "error"
            job["error"] = f"{type(e).__name__}: {e}"
            job["finished_at"] = int(time.time())

    asyncio.create_task(_run())
    _log_event({"kind": "multi_forward_started", "source": body["source"],
                "dest": body["dest"], "dry_run": bool(body.get("dry_run")),
                "job_id": job["id"]})
    return jsonify({"ok": True, "job_id": job["id"], "db_path": str(job.get("db_path", ""))})


@app.route("/api/pairs/<name>/watermark", methods=["POST"])
async def api_set_watermark(name: str):
    """Manually set a pair's watermark.

    Body: ``{"last_msg_id": int, "last_scanned_id"?: int}``.

    Used to repair clobbered watermarks or to skip ahead / re-forward a range.
    When rolling *last_msg_id* backwards without an explicit *last_scanned_id*,
    ``save_pair_watermark(..., allow_regression=True)`` also pulls the scanned
    floor down to the new watermark so ``scan_from = max(wm, scanned)`` actually
    re-iterates the repaired range.

    Refuses while the pair is mid-run so an in-flight runner cannot race the
    repaired value (or re-forward against a stale min_id snapshot).
    """
    body = await request.get_json(force=True)
    wm = int(body.get("last_msg_id", 0))
    scanned_raw = body.get("last_scanned_id", None)
    scanned = int(scanned_raw) if scanned_raw is not None else None
    pair_lock = _get_pair_lock(name)
    if pair_lock.locked():
        return jsonify({"error": f"pair '{name}' is running — pause/wait, then set watermark"}), 409
    async with pair_lock:
        # Manual repair allowed to roll backwards (e.g., to re-forward a range).
        # Omitting last_scanned_id → save_pair_watermark clamps scanned to wm.
        await save_pair_watermark(
            name, wm, int(time.time()),
            allow_regression=True,
            last_scanned_id=scanned,
        )
    _log_event({
        "kind": "watermark_set", "pair": name, "wm": wm,
        **({"last_scanned_id": scanned} if scanned is not None else {}),
    })
    return jsonify({
        "ok": True, "pair": name, "watermark": wm,
        **({"last_scanned_id": scanned if scanned is not None else wm}),
    })


@app.route("/api/retry-queue")
async def api_retry_queue_list():
    """List parked messages that were auto-skipped after consecutive transient fails.

    Query: ?pair=NAME&status=pending|dead  (both optional).
    """
    pair = request.args.get("pair")
    status = request.args.get("status")
    data = load_retry_queue()
    items = list(data.get("items") or [])
    if pair:
        items = [it for it in items if it.get("pair") == pair]
    if status:
        items = [it for it in items if (it.get("status") or "pending") == status]
    # Newest first for the UI.
    items.sort(key=lambda it: int(it.get("enqueued_at") or 0), reverse=True)
    pending = sum(1 for it in items if (it.get("status") or "pending") == "pending")
    dead = sum(1 for it in items if it.get("status") == "dead")
    return jsonify({
        "items": items,
        "counts": {"total": len(items), "pending": pending, "dead": dead},
    })


@app.route("/api/retry-queue/drain", methods=["POST"])
async def api_retry_queue_drain():
    """Re-attempt pending retry-queue items. Body (all optional):
      {"pair": "lsp-media", "max_items": 10, "force": true}
    force=true ignores retry_min_interval_seconds.
    """
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    body = {}
    try:
        body = await request.get_json(silent=True) or {}
    except Exception:
        body = {}
    pair = body.get("pair")
    max_items = int(body.get("max_items") or 10)
    force = bool(body.get("force", False))
    job = _new_job(
        kind="retry-drain",
        label=f"retry-queue{f' [{pair}]' if pair else ''} (max {max_items})",
    )
    _log_event({
        "kind": "retry_drain_started",
        "pair": pair, "max_items": max_items, "force": force, "job_id": job["id"],
    })
    try:
        result = await drain_retry_queue(
            _dl, pair_name=pair, max_items=max_items, force=force, job=job,
        )
        job["finished_at"] = int(time.time())
        if job.get("status") == "running":
            job["status"] = "finished"
        _log_event({
            "kind": "retry_drain_finished", "result": result, "job_id": job["id"],
        })
        return jsonify({"ok": True, "job_id": job["id"], **result})
    except Exception as e:
        job.update({"status": "error", "finished_at": int(time.time()), "error": str(e)})
        _log_event({"kind": "retry_drain_error", "error": str(e), "job_id": job["id"]})
        return jsonify({"error": str(e), "job_id": job["id"]}), 500


@app.route("/api/retry-queue", methods=["DELETE"])
async def api_retry_queue_delete():
    """Remove items from the retry queue. Body: {"ids": ["pair:123", ...]}."""
    body = await request.get_json(force=True)
    ids = body.get("ids") or []
    if not isinstance(ids, list) or not ids:
        return jsonify({"error": "ids must be a non-empty list"}), 400
    removed = await remove_retry_items([str(x) for x in ids])
    _log_event({"kind": "retry_queue_deleted", "ids": ids, "removed": removed})
    return jsonify({"ok": True, "removed": removed})


@app.route("/api/pairs/<name>/run", methods=["POST"])
async def api_pair_run(name: str):
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
    if not pair:
        return jsonify({"error": f"pair '{name}' not found"}), 404

    # Optional one-shot watermark override. POST body `{"from_msg_id": 0}` makes
    # this run start from the given id (typically 0 for a full re-forward) without
    # touching the persisted watermark — the next save inside run_pair still
    # writes the new high-water id, so subsequent runs resume normally.
    body = {}
    try:
        body = await request.get_json(silent=True) or {}
    except Exception:
        body = {}
    override = body.get("from_msg_id")

    async with _state_lock:
        state = load_state()
    if override is not None:
        try:
            override_int = int(override)
            state[name] = {**state.get(name, {}), "last_msg_id": override_int}
            _log_event({"kind": "watermark_override", "pair": name, "from_msg_id": override_int})
        except (TypeError, ValueError):
            return jsonify({"error": "from_msg_id must be an integer"}), 400
    job = _new_job(kind="pair-manual", label=f"pair:{name} (manual)")
    _log_event({"kind": "run_started", "pair": name, "trigger": "manual", "job_id": job["id"]})
    try:
        result = await run_pair(_dl, pair, state, job=job)
        # No save_state here — run_pair persists per-pair via save_pair_watermark.
        job["finished_at"] = int(time.time())
        _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "manual", "job_id": job["id"]})
        return jsonify({"ok": True, "result": result, "job_id": job["id"]})
    except Exception as e:
        job.update({"status": "error", "finished_at": int(time.time())})
        _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "manual", "job_id": job["id"]})
        return jsonify({"error": str(e)}), 500


# ───── Routes: bulk run ──────────────────────────────────────────────────


_bulk_runs: dict[str, dict] = {}  # bulk_id -> {names, status, current, started_at, cancel}


async def _bulk_runner(bulk_id: str, names: list[str]) -> None:
    state_holder = {}  # filled in from disk per-pair
    bulk = _bulk_runs[bulk_id]
    for idx, name in enumerate(names, 1):
        if bulk.get("cancel") or _paused:
            bulk["status"] = "cancelled"
            break
        bulk["current"] = name
        bulk["index"] = idx
        cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
        pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
        if not pair:
            _log_event({"kind": "bulk_skip", "pair": name, "reason": "not found", "bulk_id": bulk_id})
            continue
        async with _state_lock:
            state_holder["state"] = load_state()
        job = _new_job(kind="pair-bulk", label=f"pair:{name} (bulk {idx}/{len(names)})")
        _log_event({"kind": "run_started", "pair": name, "trigger": "bulk", "job_id": job["id"], "bulk_id": bulk_id})
        try:
            result = await run_pair(_dl, pair, state_holder["state"], job=job)
            # Per-pair watermark already persisted atomically inside run_pair
            # via save_pair_watermark. Don't save_state(full_dict) here — it
            # would write our stale snapshot over keys other runners updated.
            if job["status"] == "running":
                job["status"] = "finished"
            job["finished_at"] = int(time.time())
            _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "bulk", "job_id": job["id"], "bulk_id": bulk_id})
        except Exception as e:
            job.update({"status": "error", "finished_at": int(time.time())})
            _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "bulk", "job_id": job["id"], "bulk_id": bulk_id})
        # Honor cancel between pairs without aborting mid-run.
        if bulk.get("cancel"):
            bulk["status"] = "cancelled"
            break
    if bulk["status"] == "running":
        bulk["status"] = "finished"
    bulk["finished_at"] = int(time.time())


@app.route("/api/run-all", methods=["POST"])
async def api_run_all():
    """Fire-and-forget serial backfill. Body: {"names": [...]} or {"prefix": "..."}.

    Returns immediately with the bulk_id and the queued names. Each pair runs
    one-at-a-time in a background task and appears as a separate job in
    /api/jobs so the existing job panel shows live progress. Cancel a single
    pair via /api/jobs/<id>/cancel; cancel the whole bulk via
    /api/run-all/<bulk_id>/cancel.
    """
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    body = await request.get_json(silent=True) or {}
    cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    all_names = [p.get("name") for p in cfg.get("pairs", []) if p.get("name")]
    names: list[str]
    if body.get("names"):
        wanted = set(body["names"])
        names = [n for n in all_names if n in wanted]
    elif body.get("prefix"):
        prefix = body["prefix"]
        names = [n for n in all_names if n.startswith(prefix)]
    else:
        names = list(all_names)
    if not names:
        return jsonify({"error": "no pairs matched"}), 400

    bulk_id = f"bulk-{int(time.time())}-{secrets.token_hex(3)}"
    _bulk_runs[bulk_id] = {
        "id": bulk_id, "names": names, "status": "running",
        "index": 0, "total": len(names), "current": None,
        "started_at": int(time.time()), "finished_at": None, "cancel": False,
    }
    asyncio.create_task(_bulk_runner(bulk_id, names))
    _log_event({"kind": "bulk_started", "bulk_id": bulk_id, "count": len(names)})
    return jsonify({"ok": True, "bulk_id": bulk_id, "queued": names})


@app.route("/api/run-all/<bulk_id>/cancel", methods=["POST"])
async def api_bulk_cancel(bulk_id: str):
    bulk = _bulk_runs.get(bulk_id)
    if not bulk:
        return jsonify({"error": "bulk run not found"}), 404
    bulk["cancel"] = True
    _log_event({"kind": "bulk_cancel_requested", "bulk_id": bulk_id})
    return jsonify({"ok": True, "bulk": {k: v for k, v in bulk.items() if k != "cancel"}})


@app.route("/api/run-all")
async def api_run_all_status():
    """List recent bulk runs (live + finished)."""
    runs = sorted(_bulk_runs.values(), key=lambda b: b["started_at"], reverse=True)[:20]
    return jsonify({"bulks": [{k: v for k, v in b.items() if k != "cancel"} for b in runs]})


# ───── Routes: pause / resume ───────────────────────────────────────────


@app.route("/api/pause", methods=["GET"])
async def api_pause_status():
    return jsonify({"paused": _paused})


@app.route("/api/pause", methods=["POST"])
async def api_pause():
    """Halt everything: cancel in-flight jobs + bulks, block scheduler from
    starting new pair runs. Until /api/resume is hit.
    """
    global _paused
    _paused = True
    cancelled_jobs = []
    for jid, job in _jobs.items():
        if job.get("status") in ("running", "queued"):
            job["cancel"] = True
            cancelled_jobs.append(jid)
    cancelled_bulks = []
    for bid, bulk in _bulk_runs.items():
        if bulk.get("status") == "running":
            bulk["cancel"] = True
            cancelled_bulks.append(bid)
    _log_event({"kind": "paused", "cancelled_jobs": cancelled_jobs, "cancelled_bulks": cancelled_bulks})
    return jsonify({"ok": True, "paused": True, "cancelled_jobs": cancelled_jobs, "cancelled_bulks": cancelled_bulks})


@app.route("/api/resume", methods=["POST"])
async def api_resume():
    global _paused
    was = _paused
    _paused = False
    _log_event({"kind": "resumed", "was_paused": was})
    return jsonify({"ok": True, "paused": False})


# ───── Routes: one-shot forward ──────────────────────────────────────────


@app.route("/api/forward-once", methods=["POST"])
async def api_forward_once():
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    body = await request.get_json(force=True)
    def _opt_int(v):
        if v in (None, "", "null"):
            return None
        return int(v)
    try:
        source = int(body["source"])
        dest = int(body["dest"])
        source_topic = _opt_int(body.get("source_topic"))
        dest_topic = _opt_int(body.get("dest_topic"))
        ftype = body.get("type", "all")
        limit = int(body.get("limit", 50))
        delay = float(body.get("delay_seconds", 1.0))
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid params: {e}"}), 400

    job = _new_job(kind="oneshot", label=f"oneshot {source}->{dest}", total=limit)
    _log_event({"kind": "oneshot_started", "source": source, "dest": dest, "type": ftype, "limit": limit, "job_id": job["id"]})

    # Prefer native server-side forward when the source allows it — oneshot
    # used to always download+reupload even for unprotected channels.
    protected = await _dl.is_source_protected(source)
    mode = "copy" if protected else "native"
    print(f"[oneshot {source}->{dest}] mode={mode}")

    iter_kwargs = {"limit": limit or None}
    if source_topic and source_topic > 1:
        iter_kwargs["reply_to"] = source_topic

    # Collect matching msgs in reverse (newest first from iter_messages), then
    # sort ascending. Cap list growth via limit so large one-shots stay bounded.
    msgs = []
    scanned = 0
    async for m in _dl.client.iter_messages(source, **iter_kwargs):
        scanned += 1
        if job.get("cancel"):
            print(f"[oneshot {source}->{dest}] cancelled during scan ({len(msgs)} matched)")
            job["status"] = "cancelled"
            job["finished_at"] = int(time.time())
            return jsonify({"ok": True, "result": {
                "forwarded": 0, "failed": 0, "scanned": scanned, "cancelled": True, "mode": mode,
            }, "job_id": job["id"]})
        if _matches_type(m, ftype, _dl):
            msgs.append(m)
            if limit and len(msgs) >= limit:
                break
    msgs.sort(key=lambda m: m.id)

    job["total"] = len(msgs)
    job["status"] = "running"
    ok = fail = 0
    try:
        if not protected:
            # Native batch path — no local media I/O. Stream batches without
            # holding extra state beyond the matched list (already capped).
            BATCH = 100
            i = 0
            for start in range(0, len(msgs), BATCH):
                if job.get("cancel"):
                    job["status"] = "cancelled"
                    break
                batch = msgs[start:start + BATCH]

                async def _send(b=batch):
                    if dest_topic and dest_topic > 1:
                        await _dl.forward_batch(source, dest, b, drop_author=True, top_msg_id=dest_topic)
                    else:
                        await _dl.client.forward_messages(dest, b, source, drop_author=True)

                try:
                    await retry_once_on_flood(_send, label=f"oneshot {source}->{dest}")
                    ok += len(batch)
                except Exception as e:
                    print(f"[oneshot {source}->{dest}] batch failed: {e}")
                    fail += len(batch)
                i += len(batch)
                job.update({"done": i, "ok": ok, "fail": fail, "last_id": batch[-1].id})
                if start + BATCH < len(msgs):
                    await asyncio.sleep(delay)
        else:
            import shutil
            import uuid as _uuid
            run_temp = BASE_DIR / "temp" / f"oneshot-{_uuid.uuid4().hex[:10]}"
            run_temp.mkdir(parents=True, exist_ok=True)
            try:
                for i, m in enumerate(msgs, 1):
                    if job.get("cancel"):
                        job["status"] = "cancelled"
                        break
                    try:
                        success = await _dl._copy_message_to(
                            m, dest, dest_topic=dest_topic, temp_dir=run_temp,
                        )
                        if success:
                            ok += 1
                        else:
                            fail += 1
                    except Exception as e:
                        print(f"[oneshot {source}->{dest}] copy #{m.id} failed: {e}", file=sys.stderr)
                        fail += 1
                    job.update({"done": i, "ok": ok, "fail": fail, "last_id": m.id})
                    await asyncio.sleep(delay)
            finally:
                shutil.rmtree(run_temp, ignore_errors=True)
        if job["status"] != "cancelled":
            job["status"] = "finished"
    finally:
        job["finished_at"] = int(time.time())

    result = {
        "forwarded": ok, "failed": fail, "scanned": scanned,
        "cancelled": job["status"] == "cancelled", "mode": mode,
    }
    _log_event({"kind": "oneshot_finished", "source": source, "dest": dest, "result": result, "job_id": job["id"]})
    return jsonify({"ok": True, "result": result, "job_id": job["id"]})


# ───── Routes: clone forum end-to-end ───────────────────────────────────


def _slug(s: str, fallback: str = "x", maxlen: int = 40) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (s or "").lower()).strip("-")
    return (s or fallback)[:maxlen]


async def _clone_forum_impl(*, source_id: int, dest_title: str, skip_general: bool,
                            ftype: str, delay_seconds: float, max_per_run: int,
                            job: dict) -> dict:
    """Create dest forum, mirror topics, write pairs.json. Mutates `job` for progress."""
    # 1. Validate source — list_topics raises if not a forum / no access.
    src_topics = await _dl.list_topics(source_id)
    if not src_topics:
        raise ValueError(f"source chat {source_id} is not a forum supergroup, or has no topics")

    to_clone = [t for t in src_topics if not (skip_general and t["id"] == 1)]
    skipped = [t["title"] for t in src_topics if (skip_general and t["id"] == 1)]
    job["total"] = len(to_clone)

    # 2. Create destination supergroup with forum flag.
    create_result = await _dl.client(CreateChannelRequest(
        title=dest_title,
        about="",
        megagroup=True,
        forum=True,
    ))
    dest_channel = create_result.chats[0]
    dest_chat_id = tg_utils.get_peer_id(dest_channel)

    # If forum flag didn't take (older Telegram cores), toggle explicitly.
    if not getattr(dest_channel, "forum", False):
        try:
            await _dl.client(ToggleForumRequest(channel=dest_channel, enabled=True, tabs=False))
        except Exception as e:
            print(f"ToggleForum failed (may be benign): {e}", file=sys.stderr)

    dest_peer = await _dl.client.get_input_entity(dest_chat_id)

    # 3. Create matching topics, throttled to avoid FloodWait.
    for idx, st in enumerate(to_clone, 1):
        if job.get("cancel"):
            job["status"] = "cancelled"
            break
        try:
            await _dl.client(CreateForumTopicRequest(
                peer=dest_peer,
                title=st["title"],
                random_id=secrets.randbits(63),
            ))
            job["ok"] += 1
        except Exception as e:
            job["fail"] += 1
            print(f"failed to create topic {st['title']!r}: {e}", file=sys.stderr)
        job["done"] = idx
        await asyncio.sleep(0.5)  # gentle throttle

    # 4. List dest topics to resolve title -> id, then write pairs.
    new_topics = await _dl.list_topics(dest_chat_id)
    # First occurrence wins (Telegram permits duplicate titles; src shouldn't have any).
    title_to_id = {}
    for t in new_topics:
        title_to_id.setdefault(t["title"], t["id"])

    dest_slug = _slug(dest_title, fallback="clone")
    pairs_added = []
    pairs_missing = []

    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        existing_names = {p.get("name") for p in cfg.get("pairs", [])}

        for st in to_clone:
            dest_topic_id = title_to_id.get(st["title"])
            if not dest_topic_id:
                pairs_missing.append(st["title"])
                continue
            slug_fallback = f"topic-{st['id']}"
            base_name = f"{dest_slug}--{_slug(st['title'], fallback=slug_fallback)}"
            name = base_name
            n = 2
            while name in existing_names:
                name = f"{base_name}-{n}"
                n += 1
            existing_names.add(name)
            cfg.setdefault("pairs", []).append({
                "name": name,
                "source": source_id,
                "dest": dest_chat_id,
                "source_topic": st["id"],
                "dest_topic": dest_topic_id,
                "type": ftype,
                "delay_seconds": delay_seconds,
                "max_per_run": max_per_run,
            })
            pairs_added.append(name)

        save_pairs(cfg)

    return {
        "dest_chat_id": dest_chat_id,
        "dest_title": dest_title,
        "topics_attempted": len(to_clone),
        "topics_created": len(pairs_added),
        "pairs_added": pairs_added,
        "skipped_general": skipped,
        "pairs_missing": pairs_missing,  # source topics whose dest twin we couldn't resolve
    }


@app.route("/api/clone-forum", methods=["POST"])
async def api_clone_forum():
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    body = await request.get_json(force=True)
    try:
        source = int(body["source"])
        dest_title = (body.get("dest_title") or "").strip()
        if not dest_title:
            return jsonify({"error": "dest_title required"}), 400
        skip_general = bool(body.get("skip_general", True))
        ftype = body.get("type", "all")
        delay = float(body.get("delay_seconds", 1.0))
        max_per_run = int(body.get("max_per_run", 200))
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid params: {e}"}), 400

    if ftype not in ("all", "media", "documents", "messages", "docs_and_text"):
        return jsonify({"error": "type must be one of all|media|documents|messages|docs_and_text"}), 400

    job = _new_job(kind="clone-forum", label=f"clone {source} -> {dest_title!r}")
    job["status"] = "running"
    _log_event({"kind": "clone_started", "source": source, "dest_title": dest_title, "job_id": job["id"]})
    try:
        result = await _clone_forum_impl(
            source_id=source, dest_title=dest_title, skip_general=skip_general,
            ftype=ftype, delay_seconds=delay, max_per_run=max_per_run, job=job,
        )
        if job["status"] != "cancelled":
            job["status"] = "finished"
        job["finished_at"] = int(time.time())
        _log_event({"kind": "clone_finished", "source": source, "result": result, "job_id": job["id"]})
        return jsonify({"ok": True, "result": result, "job_id": job["id"]})
    except Exception as e:
        job.update({"status": "error", "finished_at": int(time.time())})
        _log_event({"kind": "clone_error", "source": source, "error": str(e), "job_id": job["id"]})
        return jsonify({"error": str(e)}), 500


# ───── Routes: run log ───────────────────────────────────────────────────


@app.route("/api/runs")
async def api_runs():
    n = int(request.args.get("n", 50))
    return jsonify({"events": _run_log[-n:][::-1]})


@app.route("/api/jobs")
async def api_jobs():
    """All recent jobs (live + finished). Filter with ?status=running for just live."""
    status_filter = request.args.get("status")
    jobs = list(_jobs.values())
    if status_filter:
        jobs = [j for j in jobs if j["status"] == status_filter]
    jobs.sort(key=lambda j: j["started_at"], reverse=True)
    # Strip the internal `cancel` flag from the response.
    return jsonify({"jobs": [{k: v for k, v in j.items() if k != "cancel"} for j in jobs[:100]]})


@app.route("/api/jobs/<job_id>/cancel", methods=["POST"])
async def api_job_cancel(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    if job["status"] not in ("running", "queued"):
        return jsonify({"error": f"job is {job['status']}, can't cancel"}), 400
    job["cancel"] = True
    _log_event({"kind": "job_cancel_requested", "job_id": job_id, "label": job["label"]})
    return jsonify({"ok": True, "job": {k: v for k, v in job.items() if k != "cancel"}})


# ───── Background scheduler ──────────────────────────────────────────────


async def _scheduler_loop():
    global _telegram_session_error
    # Consecutive reconnect failures drive exponential backoff (30s → 10min).
    # Resets to 0 on a successful connect so brief blips don't accumulate.
    reconnect_fail_streak = 0
    next_pair_run = 0.0
    while True:
        try:
            if _telegram_session_error:
                # A replaced SQLite session is not picked up by an existing
                # TelegramClient. Stay alive for Web diagnostics, but require a
                # service restart after a fresh session is installed.
                await asyncio.sleep(300)
                continue
            if _dl and not await ensure_connected(_dl.client, label="scheduler"):
                reconnect_fail_streak += 1
                print(
                    f"[scheduler] reconnect failed (streak={reconnect_fail_streak})",
                    file=sys.stderr,
                )
                await sleep_backoff(
                    reconnect_fail_streak - 1,
                    base=30.0,
                    cap=600.0,
                    label="scheduler",
                )
                continue
            reconnect_fail_streak = 0
            cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
            interval = max(60, int(cfg.get("interval_seconds", 3600)))
            pairs = cfg.get("pairs", [])
            active = [p for p in pairs if not p.get("paused")]
            fatal_session_detected = False
            if _paused:
                print(f"[scheduler] PAUSED — sleeping {interval}s, next check at "
                      f"{datetime.fromtimestamp(time.time() + interval, tz=timezone.utc).strftime('%H:%M:%S UTC')}")
            else:
                print(f"[scheduler] cycle start — {len(active)} active pair(s), interval={interval}s")
            run_main_cycle = time.monotonic() >= next_pair_run
            for pair in pairs if run_main_cycle else []:
                if _paused:
                    break
                # Per-pair pause: set `"paused": true` in pairs.json to keep a
                # pair out of the scheduler. Manual /api/pairs/<name>/run still works.
                if pair.get("paused"):
                    continue
                # Soft re-check between pairs: if the link died mid-cycle, stop
                # burning through remaining pairs until the next reconnect pass.
                if _dl and not await ensure_connected(_dl.client, label="scheduler"):
                    print(
                        "[scheduler] lost connection mid-cycle — remaining pairs deferred",
                        file=sys.stderr,
                    )
                    break
                name = _pair_key(pair)
                job = _new_job(kind="pair-scheduled", label=f"pair:{name} (scheduled)")
                try:
                    async with _state_lock:
                        state = load_state()
                    _log_event({"kind": "run_started", "pair": name, "trigger": "scheduler", "job_id": job["id"]})
                    result = await run_pair(_dl, pair, state, job=job)
                    # No save_state here — see _bulk_runner comment.
                    job["finished_at"] = int(time.time())
                    _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "scheduler", "job_id": job["id"]})
                    # One pair aborted on a network blip; keep going to other
                    # pairs (they may still succeed) rather than collapsing the
                    # whole cycle.
                    if isinstance(result, dict) and result.get("aborted_transient"):
                        print(
                            f"[scheduler] {name} aborted_transient — continuing other pairs",
                            file=sys.stderr,
                        )
                except Exception as e:
                    job.update({"status": "error", "finished_at": int(time.time())})
                    print(f"scheduler error for {name}: {e}", file=sys.stderr)
                    _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "scheduler", "job_id": job["id"]})
                    if is_fatal_session_error(e):
                        _telegram_session_error = type(e).__name__
                        fatal_session_detected = True
                        print(
                            "[scheduler] FATAL Telegram session invalid; stopping scheduled "
                            "Telegram work until a fresh session is installed and the service restarts",
                            file=sys.stderr,
                        )
                        break

            if run_main_cycle:
                next_pair_run = time.monotonic() + interval

            # During the main cycle waiting period, drain parked retries so skipped
            # media is eventually recovered without blocking watermarks.
            if not _paused and not fatal_session_detected and _dl:
                try:
                    rq = load_retry_queue()
                    pending_n = sum(
                        1 for it in (rq.get("items") or [])
                        if (it.get("status") or "pending") == "pending"
                    )
                    if pending_n:
                        rjob = _new_job(
                            kind="retry-drain",
                            label=f"retry-queue (scheduled, pending={pending_n})",
                        )
                        _log_event({
                            "kind": "retry_drain_started",
                            "trigger": "scheduler",
                            "pending": pending_n,
                            "job_id": rjob["id"],
                        })
                        rresult = await drain_retry_queue(
                            _dl, max_items=20, force=False, job=rjob,
                        )
                        rjob["finished_at"] = int(time.time())
                        if rjob.get("status") == "running":
                            rjob["status"] = "finished"
                        _log_event({
                            "kind": "retry_drain_finished",
                            "trigger": "scheduler",
                            "result": rresult,
                            "job_id": rjob["id"],
                        })
                except Exception as e:
                    print(f"[scheduler] retry-queue drain error: {e}", file=sys.stderr)
                    if is_fatal_session_error(e):
                        _telegram_session_error = type(e).__name__
                        print(
                            "[scheduler] FATAL Telegram session invalid during retry drain; "
                            "stopping scheduled Telegram work until service restart",
                            file=sys.stderr,
                        )

            sleep_seconds = min(30, max(0, next_pair_run - time.monotonic()))
            next_run = datetime.fromtimestamp(time.time() + sleep_seconds, tz=timezone.utc).strftime('%H:%M:%S UTC')
            print(f"[scheduler] cycle done — sleeping {sleep_seconds:.0f}s, next run at {next_run}")
            await asyncio.sleep(sleep_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"scheduler loop crashed: {e}", file=sys.stderr)
            await sleep_backoff(0, base=60.0, cap=600.0, label="scheduler")


# ───── Live edit/delete propagation ───────────────────────────────────────
# When a source message is edited or deleted, mirror the change to every
# pair's destination. Inspired by khoben/telemirror's edit/delete propagation.
# We register one global handler (no chats= filter) and dispatch by looking
# up event.chat_id against current pair sources — that way runtime pair adds
# and deletes are picked up without re-registering. Handler failures are
# logged and swallowed so a single broken pair can't kill the dispatcher.


def _pairs_for_source(source_chat_id: int) -> list[dict]:
    """Return all configured pairs whose source matches this chat id.
    Reads pairs.json each call so newly-added pairs pick up edits without
    a restart. Cheap (file is small)."""
    try:
        cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    except Exception:
        return []
    return [p for p in cfg.get("pairs", []) if int(p.get("source", 0)) == int(source_chat_id)]


async def _on_source_edited(event) -> None:
    if not _dl or not _dl.client:
        return
    chat_id = getattr(event, "chat_id", None)
    if chat_id is None:
        return
    pairs = _pairs_for_source(chat_id)
    if not pairs:
        return
    msg = event.message
    src_msg_id = getattr(msg, "id", None)
    if src_msg_id is None:
        return
    for pair in pairs:
        name = _pair_key(pair)
        dest_msg_id = lookup_dest_id(name, src_msg_id)
        if dest_msg_id is None:
            # Message was forwarded before this feature shipped (or before
            # we joined). Nothing to update on the dest side.
            continue
        try:
            new_text = apply_replacements(msg.message or "", pair.get("replacements") or [])
            text_was_transformed = bool(pair.get("replacements")) and new_text != (msg.message or "")
            await _dl.client.edit_message(
                int(pair["dest"]),
                dest_msg_id,
                new_text,
                parse_mode=None,
                formatting_entities=None if text_was_transformed else msg.entities,
            )
            _log_event({"kind": "live_edit", "pair": name, "src_id": src_msg_id, "dest_id": dest_msg_id})
        except MessageNotModifiedError:
            pass  # source emitted an edit event but content is unchanged (reactions, views, etc.)
        except Exception as e:
            print(f"[live-edit {name}] {src_msg_id} -> {dest_msg_id} failed: {e}", file=sys.stderr)
            _log_event({"kind": "live_edit_error", "pair": name, "src_id": src_msg_id, "error": str(e)})


async def _on_source_deleted(event) -> None:
    if not _dl or not _dl.client:
        return
    chat_id = getattr(event, "chat_id", None)
    # Channel deletes always include chat_id; private/group deletes may not,
    # and we don't mirror those anyway — skip if missing.
    if chat_id is None:
        return
    pairs = _pairs_for_source(chat_id)
    if not pairs:
        return
    deleted_ids = list(getattr(event, "deleted_ids", []) or [])
    if not deleted_ids:
        return
    for pair in pairs:
        name = _pair_key(pair)
        dest = int(pair["dest"])
        to_delete = []
        mapped_srcs = []
        for sid in deleted_ids:
            did = lookup_dest_id(name, sid)
            if did is not None:
                to_delete.append(did)
                mapped_srcs.append(sid)
        if not to_delete:
            continue
        try:
            await _dl.client.delete_messages(dest, to_delete)
            await forget_mappings(name, mapped_srcs)
            _log_event({"kind": "live_delete", "pair": name, "count": len(to_delete)})
        except Exception as e:
            print(f"[live-delete {name}] {to_delete} failed: {e}", file=sys.stderr)
            _log_event({"kind": "live_delete_error", "pair": name, "error": str(e)})


def _register_event_handlers() -> None:
    if not _dl or not _dl.client:
        return
    _dl.client.add_event_handler(_on_source_edited, events.MessageEdited())
    _dl.client.add_event_handler(_on_source_deleted, events.MessageDeleted())


# ───── Lifecycle ──────────────────────────────────────────────────────────


@app.before_serving
async def _startup():
    global _dl, _scheduler_task, _telegram_session_error
    _telegram_session_error = None
    _load_run_log()
    # Trigger one-time pairs.json seeding from PAIRS_JSON env var if applicable.
    # Without this, a fresh volume + env var seed never lands on disk because
    # downstream handlers short-circuit on _pairs_file_exists().
    try:
        load_pairs()
    except FileNotFoundError:
        print("no pairs configured yet — add some via the web UI")
    load_message_map()
    _dl = TelegramDownloader(load_config())
    try:
        await _dl.start()
    except Exception as exc:
        if not is_fatal_session_error(exc):
            raise
        _telegram_session_error = type(exc).__name__
        print(
            "[startup] FATAL Telegram session invalid; Web diagnostics will remain "
            "available, but Telegram work is disabled until a fresh session is "
            "installed and the service restarts",
            file=sys.stderr,
        )
    _register_event_handlers()
    # Warm up the entity cache so the scheduler's first run can resolve channel IDs
    # immediately (a fresh session string has an empty SQLite entity cache).
    # Step 1: fetch all dialogs (populates most channels).
    if not _telegram_session_error:
        try:
            await _dl.client.get_dialogs()
        except Exception as e:
            print(f"[startup] get_dialogs warmup failed (non-fatal): {e}", file=sys.stderr)
    # Step 2: explicitly resolve every source/dest ID in pairs.json so archived
    # or low-activity channels that fall outside get_dialogs results are cached too.
    if not _telegram_session_error:
        try:
            pairs_cfg = load_pairs() if _pairs_file_exists() else {}
            ids = set()
            for p in pairs_cfg.get("pairs", []):
                for k in ("source", "dest"):
                    if p.get(k):
                        ids.add(int(p[k]))
            for cid in ids:
                try:
                    await _dl.client.get_input_entity(cid)
                except Exception:
                    pass
            print(f"[startup] entity warmup done — {len(ids)} pair channels resolved")
        except Exception as e:
            print(f"[startup] pair entity warmup failed (non-fatal): {e}", file=sys.stderr)
    _scheduler_task = asyncio.create_task(_scheduler_loop())
    if not DASH_PASS and not ALLOW_INSECURE_OPEN_DASH:
        print(
            "[startup] WARNING: DASH_PASS unset — web UI has NO authentication. "
            "Set DASH_PASS, or ALLOW_INSECURE_OPEN_DASH=1 if intentional "
            "(e.g. SSH tunnel only).",
            file=sys.stderr,
        )
    print(f"server ready — dash_user={DASH_USER!r} auth={'on' if DASH_PASS else 'OFF (set DASH_PASS)'}")


@app.after_serving
async def _shutdown():
    try:
        await flush_message_map()
    except Exception as e:
        print(f"[shutdown] flush_message_map failed: {e}", file=sys.stderr)
    if _scheduler_task:
        _scheduler_task.cancel()
    if _dl:
        await _dl.stop()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
