"""
Standalone-mode runtime for the Telegram bot.

When enabled (BOT_STANDALONE=1 or BACKEND_API_URL set to 'local' / empty),
`bot.py` routes every `call_api()` call through `dispatch(payload)` below,
which runs the exact same async handler that the FastAPI backend exposes
at /api/bot/action — just in-process.

How it works
------------
• A dedicated asyncio event loop runs in a background thread. All motor
  (MongoDB) operations, the TronGrid/Blockcypher watcher, and the
  Storm.gift refund watcher live on that loop.
• `dispatch(payload)` is a SYNC facade: it submits the coroutine to the
  background loop via `asyncio.run_coroutine_threadsafe` and blocks on
  the result. This means the bot's existing sync `call_api()` keeps
  working untouched — no refactor of 200+ call sites required.

Usage
-----
    from bot.standalone import start, dispatch, is_active

    start()                     # idempotent; launches watchers + loop
    reply = dispatch({"action": "get_balance", "telegram_user_id": "123"})

"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Optional

# Make sure `from app....` imports resolve regardless of CWD.
# /app/backend/bot/standalone.py  -> BACKEND_DIR = /app/backend
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

log = logging.getLogger("bot.standalone")


# --------------------------------------------------------------------
# Public "is this standalone mode?" helper
# --------------------------------------------------------------------
def is_standalone_mode() -> bool:
    """True when the bot should talk to Mongo directly, not over HTTP."""
    env = os.environ.get("BOT_STANDALONE", "").strip().lower()
    if env in ("1", "true", "yes", "on"):
        return True
    # Also treat an explicit BACKEND_API_URL=local as a hint.
    url = os.environ.get("BACKEND_API_URL", "").strip().lower()
    return url in ("local", "standalone", "")


# --------------------------------------------------------------------
# Background event loop
# --------------------------------------------------------------------
_loop: Optional[asyncio.AbstractEventLoop] = None
_thread: Optional[threading.Thread] = None
_ready = threading.Event()
_started = False
_lock = threading.Lock()


def _loop_worker() -> None:
    """Runs the asyncio loop forever in a background thread."""
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

    async def _bootstrap() -> None:
        """Runs once inside the background loop before handing off."""
        from app.db import ensure_indexes, admins_col, settings_col
        from app.auth import hash_password
        from app.config import (
            DEFAULT_ADMIN_USERNAME,
            DEFAULT_ADMIN_PASSWORD,
            DEFAULT_SETTINGS,
        )
        from app.watcher import watcher_loop
        from app.refund import refund_loop

        await ensure_indexes()
        # Seed default admin + settings (same as FastAPI lifespan)
        existing = await admins_col.find_one({"username": DEFAULT_ADMIN_USERNAME})
        if not existing:
            await admins_col.insert_one({
                "username": DEFAULT_ADMIN_USERNAME,
                "password_hash": hash_password(DEFAULT_ADMIN_PASSWORD),
            })
            log.info("Seeded admin user: %s", DEFAULT_ADMIN_USERNAME)
        existing_s = await settings_col.find_one({"_id": "global"})
        if not existing_s:
            await settings_col.insert_one({"_id": "global", **DEFAULT_SETTINGS})
            log.info("Seeded default settings")

        # Kick off background watchers (unless the host process already runs them —
        # e.g. the FastAPI backend is live and we only want the bot in standalone
        # mode for HTTP-bypass resilience, not to duplicate blockchain polling).
        skip_w = os.environ.get('STANDALONE_SKIP_WATCHERS', '').strip().lower()
        if skip_w in ('1', 'true', 'yes', 'on'):
            log.info(
                'Standalone: SKIPPING blockchain + refund watchers '
                '(STANDALONE_SKIP_WATCHERS=1 — assumed already running elsewhere).'
            )
        else:
            asyncio.create_task(watcher_loop(), name="watcher_loop")
            asyncio.create_task(refund_loop(), name="refund_loop")
            log.info("Blockchain watcher + refund watcher started (standalone)")

    try:
        _loop.run_until_complete(_bootstrap())
    except Exception as e:  # noqa: BLE001
        log.exception("standalone bootstrap failed: %s", e)
        _ready.set()
        return
    _ready.set()
    try:
        _loop.run_forever()
    finally:
        # Clean up outstanding tasks
        try:
            pending = asyncio.all_tasks(_loop)
            for t in pending:
                t.cancel()
            _loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:  # noqa: BLE001
            pass
        _loop.close()


def start(timeout: float = 15.0) -> None:
    """Idempotently start the background runtime. Blocks until ready."""
    global _thread, _started
    with _lock:
        if _started:
            return
        _thread = threading.Thread(
            target=_loop_worker,
            name="bot-standalone-loop",
            daemon=True,
        )
        _thread.start()
        _started = True
    if not _ready.wait(timeout=timeout):
        raise RuntimeError("standalone runtime failed to start within timeout")
    log.info("Standalone runtime ready")


def is_active() -> bool:
    return _started and _loop is not None and _loop.is_running()


def stop(timeout: float = 5.0) -> None:
    """Gracefully stop the background loop (Ctrl+C / bot shutdown)."""
    global _started
    if not is_active():
        return
    try:
        _loop.call_soon_threadsafe(_loop.stop)
    except Exception:  # noqa: BLE001
        pass
    if _thread is not None:
        _thread.join(timeout=timeout)
    _started = False


# --------------------------------------------------------------------
# Dispatch facade (sync → background loop)
# --------------------------------------------------------------------
def dispatch(payload: Dict[str, Any], timeout: float = 30.0) -> Any:
    """SYNC dispatch of a bot-action payload to the in-process handler.

    Returns whatever the HTTP endpoint would have returned:
      • dict/list on success
      • dict with {"error": ...} on handled errors
      • None on failure (to match the current `call_api` contract)
    """
    if not is_active():
        start()
    from app.routers.bot import handle_action  # imported lazily after start()

    fut = asyncio.run_coroutine_threadsafe(handle_action(payload), _loop)
    try:
        return fut.result(timeout=timeout)
    except asyncio.TimeoutError:
        log.warning("dispatch timeout for action=%s", payload.get("action"))
        fut.cancel()
        return None
    except Exception as e:  # noqa: BLE001
        log.warning("dispatch error for action=%s: %s", payload.get("action"), e)
        return None


# --------------------------------------------------------------------
# Convenience: run a coro on the worker loop (blocking)
# --------------------------------------------------------------------
def run_coro(coro, timeout: float = 30.0):
    if not is_active():
        start()
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    return fut.result(timeout=timeout)


__all__ = ["start", "stop", "dispatch", "is_active", "is_standalone_mode", "run_coro"]
