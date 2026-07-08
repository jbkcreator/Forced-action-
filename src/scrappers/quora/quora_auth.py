"""
Quora session capture — persistent Playwright profile with manual login.

Run once to initialise the profile. Log in manually in the browser window,
then press Enter. The profile (data/quora_profile/) persists the session
and Cloudflare clearance across subsequent quora_engine.py runs.

Usage:
    python -m src.scrappers.quora.quora_auth
"""

import asyncio
import json
import logging
from pathlib import Path

from playwright.async_api import async_playwright

from src.utils.http_helpers import STEALTH_ARGS, STEALTH_UA, apply_stealth_to_page, get_playwright_proxy

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

_PROJECT_ROOT = Path(__file__).resolve().parents[3]   # Forced-action-/
SESSION_FILE  = _PROJECT_ROOT / "data" / "quora_session.json"
PROFILE_DIR   = _PROJECT_ROOT / "data" / "quora_profile"
_LOGIN_URL    = "https://www.quora.com/login"
_PROBE_URL    = "https://www.quora.com/search?q=foreclosures&type=question"

_SKIP_QUERY_NAMES = {
    "facebookAutoLogin_Query",
    "loginPageFacebookLogin_Query",
    "LoggedOutHomePage_Query",
}


async def capture_session() -> None:
    (_PROJECT_ROOT / "data").mkdir(parents=True, exist_ok=True)
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            args=STEALTH_ARGS,
            user_agent=STEALTH_UA,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            proxy=get_playwright_proxy(),
        )
        page = await context.new_page()
        await apply_stealth_to_page(page)

        logger.info(f"Navigating to {_LOGIN_URL}")
        await page.goto(_LOGIN_URL, wait_until="domcontentloaded")

        print("\n>>> Log in manually in the browser, then press Enter. <<<\n")
        await asyncio.get_event_loop().run_in_executor(None, input)

        if "/login" in page.url:
            logger.error("Still on login page — aborting.")
            await context.close()
            return

        logger.info(f"Logged in. URL: {page.url}")

        # Intercept GraphQL responses on the search page
        all_query_names: list[str] = []
        captured_gql: dict         = {}
        all_requests: list[dict]   = []

        async def handle_response(response):
            all_requests.append({"method": response.request.method, "url": response.url})
            if "gql_para_POST" not in response.url:
                return
            try:
                body = await response.json()
                # Also capture query name from the request side
                req_body = json.loads(response.request.post_data or "{}")
                name = req_body.get("queryName", "")
                if name:
                    all_query_names.append(name)
                    logger.info(f"  GQL: {name}")
                if not captured_gql and name and name not in _SKIP_QUERY_NAMES:
                    captured_gql.update({
                        "url":       response.url,
                        "queryName": name,
                        "variables": req_body.get("variables", {}),
                        "extensions": req_body.get("extensions", {}),
                        "sample_response_keys": list((body.get("data") or {}).keys()),
                    })
            except Exception as exc:
                logger.debug(f"GQL parse error: {exc}")

        page.on("response", handle_response)

        logger.info("Navigating to search page…")
        await page.goto(_PROBE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(4_000)
        await page.evaluate("window.scrollTo(0, 800)")
        await page.wait_for_timeout(3_000)

        formkey   = await _extract_formkey(page)
        final_url = page.url
        page_html = await page.content()
        await page.screenshot(
            path=str(_PROJECT_ROOT / "data" / "quora_debug_screenshot.png"),
            full_page=False,
        )
        cookies = await context.cookies()
        await context.close()

    (_PROJECT_ROOT / "data" / "quora_debug_requests.json").write_text(
        json.dumps(all_requests, indent=2)
    )
    (_PROJECT_ROOT / "data" / "quora_debug_page.html").write_text(
        page_html, encoding="utf-8"
    )

    login_wall = "/login" in final_url or (
        "log in" in page_html.lower()[:3000]
        and "foreclosur" not in page_html.lower()[:3000]
    )

    logger.info("--- Results ---")
    logger.info(f"  Final URL   : {final_url}")
    logger.info(f"  GQL queries : {all_query_names}")
    logger.info(f"  Login wall? : {'YES' if login_wall else 'no'}")
    logger.info(f"  cookies     : {len(cookies)}")

    session = {"cookies": cookies, "formkey": formkey, "gql_template": captured_gql}
    SESSION_FILE.write_text(json.dumps(session, indent=2))

    logger.info(f"Session saved → {SESSION_FILE}")
    logger.info(f"  formkey  : {formkey[:16]}…" if formkey else "  formkey  : NOT FOUND")
    logger.info(f"  gql shape: {captured_gql.get('queryName', 'NOT CAPTURED')}")


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


if __name__ == "__main__":
    asyncio.run(capture_session())
