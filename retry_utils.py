"""Transient network/error classification and reconnect helpers.

Used by the scheduler, native/copy forward paths, and media transfer so that
intermittent OpenWrt connectivity does not permanently skip messages.
FloodWait is intentionally NOT treated as "transient" here — callers already
sleep the server-required duration and retry separately.
"""

from __future__ import annotations

import asyncio
import random
import sys
from typing import Optional

from telethon.errors import AuthRestartError, FloodWaitError, ServerError, TimedOutError

# Substring markers that appear in Telethon/OS exception messages when the
# underlying TCP session or DNS path is flaky. Matched case-insensitively.
_TRANSIENT_MARKERS = (
    "connection",
    "disconnect",
    "reset by peer",
    "network is unreachable",
    "temporary failure",
    "server closed the connection",
    "not connected",
    "broken pipe",
    "timed out",
    "timeout",
    "network unreachable",
    "name or service not known",
    "temporary failure in name resolution",
    "cannot connect",
    "connection refused",
    "connection reset",
    "connection aborted",
    "eof occurred",
    "transport is closing",
)


def is_transient_error(exc: BaseException) -> bool:
    """Return True if *exc* looks like a temporary network / transport fault.

    Permanent RPC/business errors (ChatWriteForbidden, MessageIdInvalid, …)
    return False so the watermark can advance past a single bad message.
    Unknown exceptions default to permanent to avoid infinite retry loops.
    FloodWaitError is False — callers must honour the server-specified wait.
    """
    if isinstance(exc, FloodWaitError):
        return False
    if isinstance(exc, (OSError, ConnectionError, TimeoutError, asyncio.TimeoutError)):
        return True
    if isinstance(exc, (ServerError, TimedOutError, AuthRestartError)):
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in _TRANSIENT_MARKERS)


async def sleep_backoff(
    attempt: int,
    *,
    base: float = 2.0,
    cap: float = 300.0,
    label: Optional[str] = None,
) -> float:
    """Sleep ``min(cap, base * 2**attempt) + jitter`` and return the delay used.

    *attempt* is 0-based (first failure → attempt=0 → ~base seconds).
    """
    attempt = max(0, int(attempt))
    delay = min(float(cap), float(base) * (2 ** attempt))
    delay += random.uniform(0.0, 1.0)
    if label:
        print(f"[{label}] backoff {delay:.1f}s (attempt {attempt + 1})", file=sys.stderr)
    await asyncio.sleep(delay)
    return delay


async def ensure_connected(client, *, label: str = "client") -> bool:
    """Return True if *client* is connected (reconnecting if needed).

    Does not loop with backoff — callers (scheduler) own the retry cadence.
    """
    if client is None:
        return False
    try:
        if client.is_connected():
            return True
    except Exception:
        # is_connected should be safe, but never let a probe crash the loop.
        pass
    print(f"[{label}] not connected — connecting…", file=sys.stderr)
    try:
        await client.connect()
        return bool(client.is_connected())
    except Exception as e:
        print(f"[{label}] connect failed: {type(e).__name__}: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    # Tiny self-check so deploy/debug can smoke the classifier without pytest.
    cases = [
        (ConnectionError("reset by peer"), True),
        (asyncio.TimeoutError(), True),
        (TimeoutError("timed out"), True),
        (OSError("Network is unreachable"), True),
        (ServerError(request=None, message="server"), True),
        (TimedOutError(request=None, message="timeout"), True),
        (Exception("ChatWriteForbidden"), False),
        (Exception("weird permanent"), False),
        (Exception("Server closed the connection"), True),
        (FloodWaitError(request=None, capture=1), False),
    ]

    failed = 0
    for exc, expected in cases:
        got = is_transient_error(exc)
        status = "ok" if got == expected else "FAIL"
        if got != expected:
            failed += 1
        print(f"  {status}: {type(exc).__name__}: {exc!r} → {got} (want {expected})")
    raise SystemExit(failed)
