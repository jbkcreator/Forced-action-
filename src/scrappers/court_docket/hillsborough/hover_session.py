"""HOVER access session — Playwright + REAL Edge persistent profile.

HOVER (hover.hillsclerk.com) is protected by PerimeterX. The ONLY launch that
passes is the real Edge binary with a warmed persistent profile, headed
(Xvfb auto-started on headless Linux), from a US IP. The production server
egresses US, so a proxy is OFF by default; pass use_proxy=True for local
testing (Oxylabs US-Florida via http_helpers).

This module owns ACCESS only:
  - launch real Edge persistent context
  - land on the home page and verify we're not PerimeterX-blocked
  - navigate to Case Search by CLICKING the on-page "Search" link
    (a direct goto to caseSearch.html is aborted by the PX script)
  - dismiss the harmless "shopping cart" error popup
  - if a real, visible "Press & Hold" challenge appears, hold it

Scraping logic lives in hover_scraper.py.

Notes:
  - PerimeterX flags the persistent visitor id (`_pxvid`) stored in the
    profile if hammered. Keep velocity low; rotate the profile on a hard
    block (delete the profile dir) — the caller/runner owns rotation policy.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import subprocess
import sys
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Page, BrowserContext

from src.utils.cf_persistent_browser import find_edge_binary, profile_dir_for

logger = logging.getLogger(__name__)

HOME_URL = "https://hover.hillsclerk.com/"
SEARCH_URL = "https://hover.hillsclerk.com/html/case/caseSearch.html"
SEARCH_LINK = "a[href*='caseSearch']"

# Substrings that mean PerimeterX denied us.
_BLOCK_MARKERS = (
    "access to this page has been denied",
    "px-cdn.net",
    "perimeterx",
    "/captcha/captcha.js",
)

_STEALTH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-setuid-sandbox",
]


class HoverBlockedError(RuntimeError):
    """PerimeterX denied access (403 / block page)."""


class HoverSessionError(RuntimeError):
    """Session could not be established (no Edge, launch failure, etc.)."""


class HoverSession:
    """A live, PerimeterX-cleared HOVER browser session.

    Usage:
        async with HoverSession(use_proxy=True) as s:
            await s.goto_search()
            ...  # drive s.page / s.context
    """

    def __init__(
        self,
        profile_name: str = "hover",
        use_proxy: bool = False,
        headless: bool = False,
        downloads_dir: Optional[Path] = None,
        pacing: tuple[float, float] = (0.4, 1.1),
        debug_dir: Optional[Path] = None,
    ):
        self.profile_name = profile_name
        self.use_proxy = use_proxy
        self.headless = headless
        self.downloads_dir = Path(downloads_dir) if downloads_dir else Path("data/court_docket/hover_downloads")
        self.pacing = pacing
        self.debug_dir = Path(debug_dir) if debug_dir else Path("data/court_docket/hover_debug")
        self._pw = None
        self._ctx: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._xvfb = None

    # ── context manager ─────────────────────────────────────────────────
    async def __aenter__(self) -> "HoverSession":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ── lifecycle ───────────────────────────────────────────────────────
    async def start(self) -> None:
        edge = find_edge_binary()
        if not edge:
            raise HoverSessionError(
                "No Edge binary found. Install Microsoft Edge or set CF_BYPASS_BROWSER_PATH."
            )
        profile_dir = profile_dir_for(self.profile_name)
        profile_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.debug_dir.mkdir(parents=True, exist_ok=True)

        # Headed Edge on a headless Linux host needs a virtual display.
        if not self.headless and not os.environ.get("DISPLAY") and sys.platform != "win32":
            try:
                self._xvfb = subprocess.Popen(
                    ["Xvfb", ":99", "-screen", "0", "1400x900x24"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                os.environ["DISPLAY"] = ":99"
                await asyncio.sleep(0.5)
            except FileNotFoundError:
                logger.warning("[hover] Xvfb not installed — falling back to headless (PX may block)")
                self.headless = True

        proxy = None
        if self.use_proxy:
            # STICKY session — fixed sessid so the same US exit IP persists,
            # anchoring the warmed PerimeterX trust cookie to one IP.
            from src.utils.http_helpers import get_sticky_playwright_proxy
            proxy = get_sticky_playwright_proxy()

        self._pw = await async_playwright().start()
        try:
            self._ctx = await self._pw.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                executable_path=edge,
                headless=self.headless,
                proxy=proxy,
                accept_downloads=True,
                downloads_path=str(self.downloads_dir),
                args=_STEALTH_ARGS,
                ignore_default_args=["--enable-automation"],
            )
        except Exception as exc:
            await self._stop_pw()
            raise HoverSessionError(f"Edge launch failed: {exc}") from exc

        # Inject playwright-stealth fingerprint patches into every page of the
        # persistent context BEFORE first navigation (best-effort).
        try:
            from src.utils.http_helpers import get_stealth
            await self._ctx.add_init_script(get_stealth().script_payload)
            logger.info("[hover] playwright-stealth init script applied")
        except Exception as exc:
            logger.warning("[hover] could not apply playwright-stealth: %s", exc)

        self._page = self._ctx.pages[0] if self._ctx.pages else await self._ctx.new_page()

        # Land on home and verify access.
        resp = await self._page.goto(HOME_URL, wait_until="domcontentloaded", timeout=45000)
        await self._pace()
        if await self.is_blocked():
            await self.debug_capture("home_blocked")
            status = resp.status if resp else "?"
            raise HoverBlockedError(f"PerimeterX blocked the HOVER home page (HTTP {status})")
        logger.info("[hover] session established (proxy=%s, headless=%s)", bool(proxy), self.headless)

    async def close(self) -> None:
        try:
            if self._ctx:
                await self._ctx.close()
        except Exception:
            pass
        await self._stop_pw()
        if self._xvfb is not None:
            try:
                self._xvfb.terminate(); self._xvfb.wait(timeout=3)
            except Exception:
                pass
            self._xvfb = None

    async def _stop_pw(self) -> None:
        try:
            if self._pw:
                await self._pw.stop()
        except Exception:
            pass
        self._pw = None

    # ── navigation ──────────────────────────────────────────────────────
    async def goto_search(self) -> None:
        """Open Case Search by clicking the on-page Search link (human-like).

        Direct goto(caseSearch.html) returns net::ERR_ABORTED under PerimeterX,
        so we click the link from whatever page we're on (falls back to the
        home page first)."""
        page = self.page
        try:
            link = page.locator(SEARCH_LINK).first
            if not await link.count():
                await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=45000)
                await self._pace()
                link = page.locator(SEARCH_LINK).first
            await link.click(timeout=10000)
        except Exception as exc:
            await self.debug_capture("goto_search_link_failed")
            raise HoverSessionError(f"could not click Search link: {exc}") from exc

        await self.dismiss_cart_popup()
        await self.maybe_press_and_hold()
        await self.dismiss_cart_popup()
        # The search form's year field is our readiness signal.
        try:
            await page.wait_for_selector("#txtYear", state="visible", timeout=20000)
        except Exception as exc:
            await self.debug_capture("search_form_not_ready")
            if await self.is_blocked():
                raise HoverBlockedError("PerimeterX blocked the Case Search page") from exc
            raise HoverSessionError(f"Case Search form did not render: {exc}") from exc
        await self._pace()

    # ── PerimeterX helpers ──────────────────────────────────────────────
    async def is_blocked(self) -> bool:
        try:
            body = (await self._page.content()).lower()
        except Exception:
            return False
        return any(m in body for m in _BLOCK_MARKERS)

    async def maybe_press_and_hold(self) -> str:
        """Hold the PerimeterX 'Press & Hold' control only if it is genuinely
        visible. Returns 'no_challenge' | 'held' | 'no_button'."""
        page = self.page
        frames = [page.main_frame] + [f for f in page.frames if f is not page.main_frame]
        target = None
        for fr in frames:
            for sel in ("div[role='button']:has-text('Press')",
                        "button:has-text('Press & Hold')",
                        "#px-captcha", "[id*='px-captcha']"):
                try:
                    loc = fr.locator(sel).first
                    if await loc.count() and await loc.is_visible():
                        box = await loc.bounding_box()
                        if box and box["width"] > 10:
                            target = box
                            break
                except Exception:
                    continue
            if target:
                break
        if not target:
            return "no_challenge"
        cx, cy = target["x"] + target["width"] / 2, target["y"] + target["height"] / 2
        logger.info("[hover] Press&Hold challenge visible — holding")
        await page.mouse.move(cx, cy)
        await asyncio.sleep(0.2)
        await page.mouse.down()
        for k in range(22):  # ~7s hold with light jitter (behavioral, not a sleep-loop)
            await page.mouse.move(cx + (k % 3 - 1) * 0.6, cy + ((k + 1) % 3 - 1) * 0.6)
            await asyncio.sleep(0.32)
        await page.mouse.up()
        await asyncio.sleep(3)
        return "held"

    async def dismiss_cart_popup(self) -> None:
        """The background cart XHR can 403 and pop an 'Error getting shopping
        cart!' modal — harmless; close it so it doesn't intercept clicks."""
        try:
            close = self._page.locator("button:has-text('Close'), .modal button:has-text('Close')")
            if await close.count() and await close.first.is_visible():
                await close.first.click(timeout=3000)
                await self._pace()
        except Exception:
            pass

    # ── utils ───────────────────────────────────────────────────────────
    async def _pace(self) -> None:
        await asyncio.sleep(random.uniform(*self.pacing))

    async def debug_capture(self, tag: str) -> Optional[Path]:
        """Screenshot + HTML dump for failure diagnosis."""
        try:
            png = self.debug_dir / f"{tag}.png"
            await self._page.screenshot(path=str(png))
            (self.debug_dir / f"{tag}.html").write_text(await self._page.content(), encoding="utf-8")
            logger.info("[hover] debug capture -> %s", png)
            return png
        except Exception:
            return None

    @property
    def page(self) -> Page:
        if not self._page:
            raise HoverSessionError("session not started")
        return self._page

    @property
    def context(self) -> BrowserContext:
        if not self._ctx:
            raise HoverSessionError("session not started")
        return self._ctx
