"""
Admin WebSocket endpoint — Quora remote browser session.

Streams a live Playwright browser (JPEG screenshots) to the admin panel.
Admin interacts via mouse/keyboard events forwarded through the WebSocket.
On login success, auto-navigates to the probe URL, captures GQL + cookies,
saves data/quora_session.json, then sends {"type":"done"} and closes.

WebSocket: GET /api/admin/quora/auth/ws?token=<admin_jwt>
"""

import asyncio
import base64
import json
import logging
from pathlib import Path

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from jose import JWTError, jwt

from config.settings import get_settings

router = APIRouter(prefix="/api/admin", tags=["admin"])
logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PROFILE_DIR  = _PROJECT_ROOT / "data" / "quora_profile"
_SESSION_FILE = _PROJECT_ROOT / "data" / "quora_session.json"
_LOGIN_URL    = "https://www.quora.com/login"
_PROBE_URL    = "https://www.quora.com/search?q=foreclosures&type=question"
_VIEWPORT     = {"width": 1280, "height": 800}

_SKIP_QUERY_NAMES = {
    "facebookAutoLogin_Query",
    "loginPageFacebookLogin_Query",
    "LoggedOutHomePage_Query",
}


def _verify_token(token: str) -> dict:
    s = get_settings()
    return jwt.decode(token, s.admin_jwt_secret, algorithms=["HS256"])


@router.websocket("/quora/auth/ws")
async def quora_auth_ws(ws: WebSocket, token: str = Query(...)):
    try:
        _verify_token(token)
    except (JWTError, Exception):
        await ws.close(code=1008, reason="Unauthorized")
        return

    await ws.accept()
    logger.info("[quora_auth_ws] client connected")

    stop = asyncio.Event()
    captured_gql: dict      = {}
    all_query_names: list   = []

    from playwright.async_api import async_playwright
    from src.utils.http_helpers import STEALTH_ARGS, STEALTH_UA, apply_stealth_to_page

    try:
        async with async_playwright() as pw:
            _PROFILE_DIR.mkdir(parents=True, exist_ok=True)
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=str(_PROFILE_DIR),
                headless=True,
                args=STEALTH_ARGS,
                user_agent=STEALTH_UA,
                locale="en-US",
                viewport=_VIEWPORT,
            )
            page = await context.new_page()
            await apply_stealth_to_page(page)

            # ── GQL response interceptor ──────────────────────────────────
            async def handle_response(response):
                if "gql_para_POST" not in response.url:
                    return
                try:
                    body     = await response.json()
                    req_body = json.loads(response.request.post_data or "{}")
                    name     = req_body.get("queryName", "")
                    if name:
                        all_query_names.append(name)
                    if not captured_gql and name and name not in _SKIP_QUERY_NAMES:
                        captured_gql.update({
                            "url":       response.url,
                            "queryName": name,
                            "variables": req_body.get("variables", {}),
                            "extensions": req_body.get("extensions", {}),
                            "sample_response_keys": list((body.get("data") or {}).keys()),
                        })
                except Exception:
                    pass

            page.on("response", handle_response)
            await page.goto(_LOGIN_URL, wait_until="domcontentloaded")
            await ws.send_json({"type": "status", "msg": "Browser opened — log in below"})

            # ── Screenshot loop ───────────────────────────────────────────
            async def screenshot_loop():
                logged_in = False
                while not stop.is_set():
                    try:
                        shot = await page.screenshot(
                            type="jpeg", quality=70, full_page=False
                        )
                        current_url = page.url
                        await ws.send_json({
                            "type": "frame",
                            "data": base64.b64encode(shot).decode(),
                            "url":  current_url,
                        })

                        # Detect successful login
                        if (
                            not logged_in
                            and "/login" not in current_url
                            and current_url not in (_LOGIN_URL, "about:blank", "")
                            and "quora.com" in current_url
                        ):
                            logged_in = True
                            logger.info("[quora_auth_ws] login detected → capturing session")
                            await ws.send_json({"type": "status", "msg": "Login detected — capturing session…"})

                            # Navigate to probe URL for GQL capture
                            await page.goto(_PROBE_URL, wait_until="domcontentloaded")
                            await asyncio.sleep(4)
                            await page.evaluate("window.scrollTo(0, 800)")
                            await asyncio.sleep(3)

                            formkey = await _extract_formkey(page)
                            cookies = await context.cookies()

                            session = {
                                "cookies":      cookies,
                                "formkey":      formkey,
                                "gql_template": captured_gql,
                            }
                            _SESSION_FILE.write_text(json.dumps(session, indent=2))
                            logger.info(
                                "[quora_auth_ws] session saved — cookies=%d formkey=%s gql=%s",
                                len(cookies), bool(formkey),
                                captured_gql.get("queryName", "none"),
                            )

                            stop.set()
                            await ws.send_json({
                                "type":      "done",
                                "cookies":   len(cookies),
                                "formkey":   bool(formkey),
                                "gql_query": captured_gql.get("queryName", ""),
                            })
                            return

                    except WebSocketDisconnect:
                        stop.set()
                        return
                    except Exception as exc:
                        logger.debug("[quora_auth_ws] screenshot_loop: %s", exc)

                    await asyncio.sleep(0.3)

            # ── Event loop ────────────────────────────────────────────────
            async def event_loop():
                while not stop.is_set():
                    try:
                        msg = await asyncio.wait_for(ws.receive_json(), timeout=1.0)
                        t   = msg.get("type")
                        if   t == "click":
                            await page.mouse.click(float(msg["x"]), float(msg["y"]))
                        elif t == "dblclick":
                            await page.mouse.dblclick(float(msg["x"]), float(msg["y"]))
                        elif t == "key":
                            await page.keyboard.type(str(msg["text"]))
                        elif t == "special_key":
                            await page.keyboard.press(str(msg["key"]))
                        elif t == "scroll":
                            await page.mouse.wheel(0, float(msg.get("dy", 100)))
                    except asyncio.TimeoutError:
                        continue
                    except WebSocketDisconnect:
                        stop.set()
                        return
                    except Exception as exc:
                        logger.debug("[quora_auth_ws] event_loop: %s", exc)

            await asyncio.gather(screenshot_loop(), event_loop())

            try:
                await context.close()
            except Exception:
                pass

    except WebSocketDisconnect:
        logger.info("[quora_auth_ws] client disconnected")
    except Exception as exc:
        logger.error("[quora_auth_ws] fatal: %s", exc)
        try:
            await ws.send_json({"type": "error", "msg": str(exc)})
        except Exception:
            pass


async def _extract_formkey(page) -> str:
    for expr in [
        "() => window.formkey || ''",
        "() => document.querySelector('meta[name=\"formkey\"]')?.getAttribute('content') || ''",
        "() => (window.__INITIAL_STATE__?.formkey) || ''",
    ]:
        try:
            val = await page.evaluate(expr)
            if val:
                return str(val)
        except Exception:
            continue
    try:
        val = await page.evaluate("""() => {
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
