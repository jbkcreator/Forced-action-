"""Pinellas Court Records access session — Playwright (stealth Chromium) + 2captcha.

The Pinellas Clerk Court Records portal (courtrecords.mypinellasclerk.gov) is an
ASP.NET "Acclaim" site (Ken Burke, CPA / Pinellas Clerk). Its bot wall is a
Google **reCAPTCHA v2** that fires on the search Submit — NOT Cloudflare and NOT
PerimeterX. So unlike HOVER (Hillsborough), this needs no real-Edge profile:
plain stealth Chromium passes, and the captcha is solved via 2captcha. This
mirrors the access layer the evictions engine already uses for the bulk
date-range export (`_pinellas_direct_scrape` + `_solve_recaptcha_2captcha`).

This module owns ACCESS only:
  - launch one stealth Chromium context (reused across a batch of cases)
  - open the Case search page
  - solve the reCAPTCHA once, keep the warmed context so later lookups skip it
    (re-solve only if it reappears)

Per-case search + docket-detail extraction lives in `court_scraper.py`.

Cost note: every reCAPTCHA solve costs 2captcha credit, so the session is
designed to solve once and reuse the context for the whole batch.
"""
from __future__ import annotations

import logging
import random
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page

# Reuse the proven access helpers from the evictions engine — same portal,
# same reCAPTCHA. The solver is imported (not duplicated) so both paths stay
# in lock-step if the portal's captcha handling changes.
from src.utils.http_helpers import (
    STEALTH_ARGS, STEALTH_UA, apply_stealth_to_page, get_playwright_proxy,
)
from src.scrappers.evictions.evictions_engine import _solve_recaptcha_2captcha

logger = logging.getLogger(__name__)

BASE_URL = "https://courtrecords.mypinellasclerk.gov"
# Case search page (s=e => the search form with the Name/Case/Attorney/Calendar tabs).
SEARCH_URL = f"{BASE_URL}/MyCr/Cases/Search?s=e"

# Substrings that mean we hit a hard bot/error wall (not a normal no-results).
_BLOCK_MARKERS = (
    "access denied",
    "request blocked",
    "unusual traffic",
)


class PinellasCourtBlockedError(RuntimeError):
    """A hard bot wall / unsolvable captcha denied access."""


class PinellasCourtSessionError(RuntimeError):
    """Session could not be established (launch failure, form never rendered)."""


class PinellasCourtSession:
    """A live courtrecords.mypinellasclerk.gov browser session.

    Usage:
        async with PinellasCourtSession() as s:
            await s.goto_search()        # land on the Case search form
            ...                          # court_scraper drives s.page / s.context
    """

    def __init__(
        self,
        use_proxy: bool = False,
        headless: bool = True,
        downloads_dir: Optional[Path] = None,
        debug_dir: Optional[Path] = None,
        pacing: tuple[float, float] = (0.4, 1.1),
    ):
        # Headless by default — the reCAPTCHA path works headless (unlike HOVER,
        # which must be headed for PerimeterX). --headful is for debugging.
        self.use_proxy = use_proxy
        self.headless = headless
        self.downloads_dir = Path(downloads_dir) if downloads_dir else Path("data/court_docket/pinellas_downloads")
        self.debug_dir = Path(debug_dir) if debug_dir else Path("data/court_docket/pinellas_debug")
        self.pacing = pacing
        self._pw = None
        self._ctx: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._captcha_solved = False

    # ── context manager ─────────────────────────────────────────────────
    async def __aenter__(self) -> "PinellasCourtSession":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    # ── lifecycle ───────────────────────────────────────────────────────
    async def start(self) -> None:
        self.downloads_dir.mkdir(parents=True, exist_ok=True)
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        proxy = get_playwright_proxy() if self.use_proxy else None

        self._pw = await async_playwright().start()
        try:
            browser = await self._pw.chromium.launch(
                headless=self.headless,
                downloads_path=str(self.downloads_dir),
                args=STEALTH_ARGS,
                proxy=proxy,
            )
            self._ctx = await browser.new_context(
                user_agent=STEALTH_UA,
                accept_downloads=True,
            )
        except Exception as exc:
            await self._stop_pw()
            raise PinellasCourtSessionError(f"Chromium launch failed: {exc}") from exc

        self._page = await self._ctx.new_page()
        await apply_stealth_to_page(self._page)
        logger.info("[pinellas-court] session established (proxy=%s, headless=%s)",
                    bool(proxy), self.headless)

    async def close(self) -> None:
        try:
            if self._ctx:
                await self._ctx.close()
        except Exception:
            pass
        await self._stop_pw()

    async def _stop_pw(self) -> None:
        try:
            if self._pw:
                await self._pw.stop()
        except Exception:
            pass
        self._pw = None

    # ── navigation ──────────────────────────────────────────────────────
    async def goto_search(self) -> None:
        """Open the Case search form and activate the Case tab."""
        page = self.page
        try:
            await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=45000)
            await self._pace()
        except Exception as exc:
            await self.debug_capture("goto_search_failed")
            raise PinellasCourtSessionError(f"could not open search page: {exc}") from exc

        if await self.is_blocked():
            await self.debug_capture("search_blocked")
            raise PinellasCourtBlockedError("courtrecords blocked the search page")

        # Activate the "Case" tab (Name / Case / Attorney / Calendar).
        # TO CONFIRM exact markup on first live run — best-effort selectors.
        for sel in ("a:has-text('Case')", "li:has-text('Case') > a",
                    "[role='tab']:has-text('Case')", ".tab:has-text('Case')"):
            try:
                tab = page.locator(sel).first
                if await tab.count() and await tab.is_visible():
                    await tab.click(timeout=4000)
                    await self._pace()
                    break
            except Exception:
                continue

    async def solve_captcha_if_present(self) -> bool:
        """Solve the reCAPTCHA on the current page via 2captcha if one is shown.

        Returns True if a captcha was solved this call. Tracks whether the
        session has ever solved one so callers can avoid redundant solves.
        """
        page = self.page
        try:
            await page.wait_for_selector('iframe[src*="google.com/recaptcha"]', timeout=8000)
        except Exception:
            return False  # no captcha appeared
        logger.info("[pinellas-court] reCAPTCHA detected — solving via 2captcha")
        solved = await _solve_recaptcha_2captcha(page, page.url)
        if not solved:
            raise PinellasCourtBlockedError(
                "reCAPTCHA present but 2captcha solve failed "
                "(check TWOCAPTCHA_API_KEY / sitekey extraction)"
            )
        self._captcha_solved = True
        return True

    # ── helpers ─────────────────────────────────────────────────────────
    async def is_blocked(self) -> bool:
        try:
            body = (await self._page.content()).lower()
        except Exception:
            return False
        return any(m in body for m in _BLOCK_MARKERS)

    async def _pace(self) -> None:
        import asyncio
        await asyncio.sleep(random.uniform(*self.pacing))

    async def debug_capture(self, tag: str) -> Optional[Path]:
        """Screenshot + HTML dump for failure diagnosis / selector pinning."""
        try:
            png = self.debug_dir / f"{tag}.png"
            await self._page.screenshot(path=str(png), full_page=True)
            (self.debug_dir / f"{tag}.html").write_text(await self._page.content(), encoding="utf-8")
            logger.info("[pinellas-court] debug capture -> %s", png)
            return png
        except Exception:
            return None

    @property
    def page(self) -> Page:
        if not self._page:
            raise PinellasCourtSessionError("session not started")
        return self._page

    @property
    def context(self) -> BrowserContext:
        if not self._ctx:
            raise PinellasCourtSessionError("session not started")
        return self._ctx
