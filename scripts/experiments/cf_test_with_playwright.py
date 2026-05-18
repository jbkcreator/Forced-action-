"""
Verify the captured Pinellas Clerk session lets Playwright bypass Cloudflare.

Strategy: launch Playwright against the **same Edge binary** and the **same
persistent user-data-dir** that the capture script used. This preserves the
TLS fingerprint Cloudflare bound the cf_clearance cookie to — cookie-transfer
to a different browser engine (e.g. Playwright's bundled Chromium) fails
re-challenge because the TLS handshake differs.

Run AFTER  scripts/experiments/cf_capture_pinellas_clerk.py  has completed
and left the profile at data/cf_session/edge_profile/.

The script:
  1. Closes any stale Edge processes still holding the profile lock.
  2. Launches Playwright with launch_persistent_context against the Edge profile.
  3. Navigates to the same URL.
  4. Looks for portal-content markers vs CF-challenge markers in the DOM.

Headful by default. Pass --headless to suppress.
"""

import asyncio
import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EDGE_PROFILE_DIR = PROJECT_ROOT / "data" / "cf_session" / "edge_profile"
TARGET_URL = "https://officialrecords.mypinellasclerk.gov/search/SearchTypeDocType"

_EDGE_CANDIDATES = [
    os.environ.get("CHROME_PATH"),
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
]

# Page heuristics
CF_CHALLENGE_SIGNALS = (
    "Just a moment...",
    "Checking your browser",
    "cf-challenge",
    "cf-mitigated",
    "needsReview",
)
PORTAL_CONTENT_SIGNALS = (
    "Document Type",
    "Recording Date",
    "Search",
    "Official Records",
)


def _find_edge() -> str | None:
    for p in _EDGE_CANDIDATES:
        if p and os.path.isfile(p):
            return p
    return None


async def main(headless: bool, hold_seconds: int):
    if not EDGE_PROFILE_DIR.exists():
        print(f"[!] No Edge profile found at {EDGE_PROFILE_DIR}")
        print("    Run  scripts/experiments/cf_capture_pinellas_clerk.py  first.")
        sys.exit(1)

    edge_path = _find_edge()
    if not edge_path:
        print("[!] Edge binary not found.")
        sys.exit(1)

    from playwright.async_api import async_playwright

    print(f"[*] Edge binary:        {edge_path}")
    print(f"[*] Persistent profile: {EDGE_PROFILE_DIR}")
    print(f"[*] Launching Playwright + Edge (headless={headless})")
    print("[*] If Edge is still running from the capture step, close it first")
    print("    — only one process can hold the profile lock at a time.")

    async with async_playwright() as pw:
        try:
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=str(EDGE_PROFILE_DIR),
                executable_path=edge_path,
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                ],
                ignore_default_args=["--enable-automation"],
            )
        except Exception as exc:
            print(f"[!] Failed to launch persistent context: {exc}")
            print("    If you see a 'profile in use' error, close any open Edge")
            print("    window using this profile and retry.")
            sys.exit(2)

        # The persistent context starts with one blank page already
        if context.pages:
            page = context.pages[0]
        else:
            page = await context.new_page()

        print(f"[*] Navigating to {TARGET_URL}")
        try:
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=45_000)
        except Exception as exc:
            print(f"[!] Navigation error: {exc}")
            await context.close()
            sys.exit(2)

        # Cloudflare often resolves the JS challenge by *navigating* to the real
        # portal page — that destroys the initial execution context. Wait for
        # things to settle, then retry the DOM reads if they hit a stale context.
        async def safe_read():
            """Returns (title, body). Retries on 'execution context destroyed'."""
            for attempt in range(5):
                try:
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    await asyncio.sleep(2)
                    t = await page.title()
                    b = await page.evaluate("document.body ? document.body.innerText : ''")
                    return t, b
                except Exception as e:
                    msg = str(e).lower()
                    if "execution context" in msg or "navigation" in msg:
                        print(f"    (attempt {attempt+1}: page navigating, retrying…)")
                        await asyncio.sleep(3)
                        continue
                    raise
            return "", ""

        title, body_text = await safe_read()
        body_lower = body_text.lower()
        print(f"[*] Final URL after settle: {page.url}")

        cf_hits = [s for s in CF_CHALLENGE_SIGNALS if s.lower() in body_lower or s.lower() in title.lower()]
        portal_hits = [s for s in PORTAL_CONTENT_SIGNALS if s.lower() in body_lower]

        print()
        print("─" * 60)
        print(f"  Page title:        {title!r}")
        print(f"  Body length:       {len(body_text)} chars")
        print(f"  CF challenge hits: {cf_hits if cf_hits else 'NONE — looks clear'}")
        print(f"  Portal hits:       {portal_hits if portal_hits else 'NONE — portal not visible'}")
        print("─" * 60)

        if cf_hits and not portal_hits:
            print("  VERDICT: Cloudflare challenge still present.")
            print("           Cookie+fingerprint pair failed — likely the profile was")
            print("           not reused cleanly. Confirm no other Edge instance is open.")
            verdict = "BLOCKED"
        elif portal_hits:
            print("  VERDICT: Portal accessible — Cloudflare bypass is working.")
            verdict = "OK"
        else:
            print("  VERDICT: Inconclusive — neither CF challenge nor expected")
            print("           portal markers found. Inspect manually.")
            verdict = "UNKNOWN"

        # Screenshot
        shot = PROJECT_ROOT / "data" / "cf_session" / f"verify_{verdict.lower()}.png"
        await page.screenshot(path=str(shot), full_page=True)
        print(f"  Screenshot saved → {shot}")
        print()

        if not headless and hold_seconds > 0:
            print(f"[*] Keeping browser open for {hold_seconds}s so you can inspect…")
            await asyncio.sleep(hold_seconds)

        await context.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--headless", action="store_true", help="Run without visible window")
    ap.add_argument("--hold", type=int, default=30,
                    help="Seconds to keep browser open in headful mode (default 30)")
    args = ap.parse_args()
    asyncio.run(main(headless=args.headless, hold_seconds=args.hold))
