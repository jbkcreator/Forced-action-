"""Browser-use (Sonnet) fallback for Pinellas court-docket extraction.

Fires ONLY when the deterministic Playwright path hits extraction / navigation
drift on an **already-cleared** detail page (the primary session has already
passed the reCAPTCHA). This agent does NOT try to beat the bot wall — browser-use
cannot solve the reCAPTCHA (that is exactly why the evictions engine replaced its
agent path with the 2captcha direct path). It is a markup-drift safety net only.

It returns the SAME structured dict shape as court_scraper.scrape_case, tagged
`extraction_path="browser_use"`, after validating the shape. On any agent failure
it returns None so the caller falls back to recording a warning + screenshot.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from config.constants import BROWSER_MODEL  # e.g. claude-sonnet-4-5
except Exception:  # pragma: no cover
    BROWSER_MODEL = "claude-sonnet-4-5-20250929"


def _make_llm():
    from browser_use import ChatAnthropic
    from config.settings import get_settings
    return ChatAnthropic(
        model=BROWSER_MODEL, temperature=0,
        api_key=get_settings().anthropic_api_key.get_secret_value(),
    )


def _build_task(detail_url: str, case_number: str) -> str:
    return (
        f"You are on the Pinellas Clerk Court Records site "
        f"(courtrecords.mypinellasclerk.gov), case {case_number}.\n"
        f"FIRST: go to this case detail URL: {detail_url}\n"
        f"If that page does not show the case, open the 'Case' search tab, enter "
        f"Case Number {case_number}, Submit, and click the blue case number in the "
        f"results to open the detail page. Do NOT attempt to solve any CAPTCHA — "
        f"if one blocks you, stop and report what you have.\n"
        f"Read the four sections of the case detail page and return ALL data as a "
        f"single JSON object with EXACTLY these keys:\n"
        f"  header: {{case_type, date_filed, status, court, judicial_officer, "
        f"uniform_case_number, style_plaintiff, style_defendant}}\n"
        f"  parties: list of {{name, party_type, party_address, attorney, "
        f"lead_attorney_address}}\n"
        f"  events: list of {{date, event, comments, docket_num, pages, doc_status}}\n"
        f"  documents: list of {{title, doc_date, docket_num, pages, doc_status, "
        f"image_available}}\n"
        f"  financial: list of {{date, description, amount}}\n"
        f"  balance_due: string or null\n"
        f"Return ONLY the JSON object, no prose. Do not buy/checkout or add to cart."
    )


def _empty_result(case_number: str) -> dict:
    return {
        "case_number": case_number, "county": "pinellas", "status": "error",
        "ucn": None, "extraction_path": "browser_use", "header": {}, "parties": [],
        "events": [], "documents": [], "financial": [], "balance_due": None,
        "detail_url": None, "warnings": [], "error": None, "screenshots": [],
    }


def _coerce_shape(payload: dict, case_number: str, detail_url: str) -> Optional[dict]:
    """Validate the agent JSON has the expected shape; coerce into our dict."""
    if not isinstance(payload, dict):
        return None
    res = _empty_result(case_number)
    res["detail_url"] = detail_url
    res["header"] = payload.get("header") or {}
    for key in ("parties", "events", "documents", "financial"):
        val = payload.get(key)
        res[key] = val if isinstance(val, list) else []
    res["balance_due"] = payload.get("balance_due")
    # Accept only if the agent actually recovered something structural.
    if not (res["header"] or res["parties"] or res["events"]):
        return None
    res["status"] = "ok"
    return res


def _parse_agent_json(text: str) -> Optional[dict]:
    if not text:
        return None
    # Strip code fences / surrounding prose, grab the outermost JSON object.
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


async def extract_detail_with_agent(session, case_number: str) -> Optional[dict]:
    """Run a browser-use Sonnet agent over a fresh browser to extract the detail.

    `session` is the live PinellasCourtSession (already past the captcha); we read
    its current detail URL so the agent can jump straight to the case and avoid a
    fresh search/captcha. Returns the validated dict or None on any failure.
    """
    try:
        detail_url = session.page.url
    except Exception:
        detail_url = ""
    if "CaseDetails" not in detail_url:
        logger.warning("[pinellas-court][fallback] no detail URL to hand off — skipping")
        return None

    try:
        from browser_use import Agent, Browser
        from src.utils.http_helpers import STEALTH_ARGS, get_browser_use_proxy
    except Exception as exc:
        logger.warning("[pinellas-court][fallback] browser_use unavailable: %s", exc)
        return None

    proxy = get_browser_use_proxy() if session.use_proxy else None
    browser = Browser(
        headless=session.headless, disable_security=True, proxy=proxy,
        ignore_default_args=["--enable-automation"], enable_default_extensions=False,
        minimum_wait_page_load_time=1.5, wait_between_actions=1.0, args=STEALTH_ARGS,
    )
    try:
        await browser.start()
        agent = Agent(
            task=_build_task(detail_url, case_number),
            llm=_make_llm(), browser=browser, max_steps=30, use_judge=False,
        )
        history = await agent.run()
        try:
            text = history.final_result()
        except Exception:
            text = str(history)
        payload = _parse_agent_json(str(text))
        result = _coerce_shape(payload, case_number, detail_url) if payload else None
        if result is None:
            logger.warning("[pinellas-court][fallback] agent returned no usable structure")
        return result
    except Exception as exc:
        logger.warning("[pinellas-court][fallback] agent failed: %s", str(exc)[:200])
        return None
    finally:
        try:
            await browser.stop()
        except Exception:
            pass
