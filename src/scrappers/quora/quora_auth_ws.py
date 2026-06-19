"""
WebSocket handler for remote Quora authentication.

Uses sync_playwright in a thread executor (same pattern as quora_poster.py)
so it works on Windows where asyncio's SelectorEventLoop cannot spawn
subprocesses. Screenshots and actions are bridged between the thread and the
async WebSocket via a thread-safe queue and run_coroutine_threadsafe.

Protocol (JSON over WebSocket):
  server → client:
    {type: "status", msg: str}
    {type: "frame",  data: <base64 JPEG>, url: str}
    {type: "done",   cookies: int, formkey: bool, gql_query: str|null}
    {type: "error",  msg: str}
  client → server:
    {type: "click",       x: float, y: float}
    {type: "dblclick",    x: float, y: float}
    {type: "scroll",      dy: float}
    {type: "key",         text: str}
    {type: "special_key", key: str}
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import queue
import shutil
import time
from pathlib import Path

from fastapi import WebSocket, WebSocketDisconnect
from playwright.sync_api import sync_playwright

from src.utils.http_helpers import STEALTH_ARGS, STEALTH_UA, get_stealth

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_PROFILE_DIR  = _PROJECT_ROOT / "data" / "quora_profile"
_SESSION_FILE = _PROJECT_ROOT / "data" / "quora_session.json"
_BROWSER_W    = 1280
_BROWSER_H    = 800
_FRAME_SEC    = 0.8   # seconds between screenshots
_ACTION_POLL  = 0.05  # action queue poll interval within each frame window


async def run_auth_session(websocket: WebSocket) -> None:
    """
    Run one remote auth session over an already-accepted WebSocket.

    Starts a sync Playwright thread; feeds it WebSocket actions via a queue;
    the thread sends frames back via run_coroutine_threadsafe.
    """
    loop     = asyncio.get_running_loop()
    action_q: queue.Queue = queue.Queue()

    playwright_future = loop.run_in_executor(
        None, _playwright_thread, action_q, websocket, loop
    )

    async def _feed_queue() -> None:
        try:
            while True:
                raw = await websocket.receive_text()
                try:
                    action_q.put(json.loads(raw))
                except json.JSONDecodeError:
                    pass
        except (WebSocketDisconnect, Exception):
            action_q.put({"type": "_stop"})

    feed_task = asyncio.create_task(_feed_queue())
    try:
        await playwright_future
    finally:
        feed_task.cancel()
        try:
            await feed_task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# Synchronous Playwright thread
# ---------------------------------------------------------------------------

def _playwright_thread(
    action_q: queue.Queue,
    websocket: WebSocket,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Runs entirely in a thread. Communicates with the event loop via queues."""

    def send(msg: dict) -> None:
        fut = asyncio.run_coroutine_threadsafe(
            websocket.send_text(json.dumps(msg)), loop
        )
        try:
            fut.result(timeout=3.0)
        except Exception:
            pass

    (_PROJECT_ROOT / "data").mkdir(parents=True, exist_ok=True)

    # Wipe the existing profile — the previous session may belong to a banned
    # account. Every auth session must start from a clean browser state.
    send({"type": "status", "msg": "Clearing old session…"})
    if _PROFILE_DIR.exists():
        shutil.rmtree(_PROFILE_DIR)
    _PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    send({"type": "status", "msg": "Launching browser…"})

    try:
        with sync_playwright() as pw:
            context = pw.chromium.launch_persistent_context(
                user_data_dir=str(_PROFILE_DIR),
                headless=True,
                args=STEALTH_ARGS,
                user_agent=STEALTH_UA,
                locale="en-US",
                viewport={"width": _BROWSER_W, "height": _BROWSER_H},
            )
            page = context.new_page()

            # Inject stealth fingerprint patches so Quora doesn't detect headless
            try:
                page.add_init_script(get_stealth().script_payload)
            except Exception:
                pass

            send({"type": "status", "msg": "Navigating to Quora login…"})
            try:
                page.goto(
                    "https://www.quora.com/login",
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
                page.wait_for_timeout(2_000)
            except Exception as exc:
                send({"type": "error", "msg": f"Navigation failed: {exc}"})
                context.close()
                return

            send({
                "type": "status",
                "msg": "Log in below — session saves automatically when you land on your feed.",
            })

            _run_interactive_loop(send, action_q, context, page)

    except Exception as exc:
        logger.error("[auth-ws] thread error: %s", exc)
        send({"type": "error", "msg": "Browser error — check server logs."})


def _run_interactive_loop(send, action_q: queue.Queue, context, page) -> None:
    """
    Screenshot → send frame → drain action queue → repeat until login or stop.
    """
    while True:
        try:
            data = page.screenshot(type="jpeg", quality=65, full_page=False)
            b64  = base64.b64encode(data).decode()
            send({"type": "frame", "data": b64, "url": page.url})
        except Exception as exc:
            logger.debug("[auth-ws] screenshot: %s", exc)

        if _is_authenticated(page):
            _capture_and_finish(send, context, page)
            return

        deadline = time.monotonic() + _FRAME_SEC
        while time.monotonic() < deadline:
            try:
                action = action_q.get_nowait()
            except queue.Empty:
                time.sleep(_ACTION_POLL)
                continue

            if action.get("type") == "_stop":
                return

            _handle_action(page, action)

            # Re-check after each action — login redirect may have fired
            if _is_authenticated(page):
                _capture_and_finish(send, context, page)
                return


def _is_authenticated(page) -> bool:
    """
    Return True only when Quora confirms an authenticated session.

    URL-only checks are unreliable (Quora redirects /login → / for bots too).
    Checks in priority order; first definitive signal wins.
    """
    if "/login" in page.url or "quora.com" not in page.url:
        return False

    # 1. In-memory cookie set — most direct signal.
    #    Unauthenticated visits produce ~12 cookies (Cloudflare + tracking).
    #    A real Quora session produces 40-100+ cookies including m-s (session)
    #    and m-uid (user ID). Checking cookie names is more reliable than count.
    try:
        cookies   = page.context.cookies()
        names     = {c.get("name", "") for c in cookies}
        # m-uid is only set after successful authentication
        if "m-uid" in names and len(cookies) > 20:
            logger.info("[auth-ws] login detected — %d cookies, m-uid present", len(cookies))
            return True
    except Exception:
        pass

    # 2. Quora's React initialisation object
    try:
        result = page.evaluate("""() => {
            const s = window.__INITIAL_STATE__;
            if (s && typeof s.loggedIn === 'boolean') return s.loggedIn;
            return null;
        }""")
        if result is True:
            return True
        if result is False:
            return False
    except Exception:
        pass

    # 3. SSR JSON string in page source
    try:
        content = page.content()
        for marker in ('"loggedIn":true', '"isLoggedIn":true'):
            if marker in content:
                return True
        for marker in ('"loggedIn":false', '"isLoggedIn":false'):
            if marker in content:
                return False
    except Exception:
        pass

    return False


def _handle_action(page, action: dict) -> None:
    t = action.get("type")
    try:
        if t == "click":
            page.mouse.click(float(action["x"]), float(action["y"]))
        elif t == "dblclick":
            page.mouse.dblclick(float(action["x"]), float(action["y"]))
        elif t == "scroll":
            page.mouse.wheel(0, float(action.get("dy", 0)))
        elif t == "key":
            page.keyboard.type(str(action["text"]))
        elif t == "special_key":
            page.keyboard.press(str(action["key"]))
    except Exception as exc:
        logger.debug("[auth-ws] action %s: %s", t, exc)


def _capture_and_finish(send, context, page) -> None:
    """Persist the session to disk and notify the client."""
    try:
        cookies = context.cookies()
        formkey = _extract_formkey(page)
        session = {"cookies": cookies, "formkey": formkey, "gql_template": {}}
        _SESSION_FILE.write_text(json.dumps(session, indent=2))
        logger.info(
            "[auth-ws] session saved — %d cookies, formkey=%s",
            len(cookies), bool(formkey),
        )
    except Exception as exc:
        logger.warning("[auth-ws] save failed: %s", exc)
        send({"type": "error", "msg": f"Session save failed: {exc}"})
        return

    send({"type": "done", "cookies": len(cookies), "formkey": bool(formkey), "gql_query": None})


def _extract_formkey(page) -> str:
    for expr in [
        "() => window.formkey || ''",
        "() => document.querySelector('meta[name=\"formkey\"]')?.getAttribute('content') || ''",
        "() => (window.__INITIAL_STATE__?.formkey) || ''",
    ]:
        try:
            val = page.evaluate(expr)
            if val:
                return str(val)
        except Exception:
            continue
    try:
        val = page.evaluate("""() => {
            for (const s of document.querySelectorAll('script:not([src])')) {
                const m = s.textContent.match(/"formkey"\\s*:\\s*"([^"]+)"/);
                if (m) return m[1];
            }
            return '';
        }""")
        if val:
            return str(val)
    except Exception:
        pass
    return ""
