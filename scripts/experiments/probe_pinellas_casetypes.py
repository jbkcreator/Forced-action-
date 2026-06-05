"""
One-off probe: dump the Pinellas courtrecords Case-search form structure.

Purpose (Step 1 of the probate/divorce/judgment-on-courtrecords plan):
  Confirm whether probate / divorce / judgment are *filterable case types* in the
  same bulk Case search the eviction 2captcha scraper uses, and pin the exact
  CaseType <option> labels so per-signal keyword filters aren't guessed.

This probe stops at the *search form* — it does NOT submit, so it never triggers
the reCAPTCHA and spends zero 2captcha credit. Cheap and safe to re-run.

Dumps to scratch/:
  - pinellas_casetypes.json  (all <select> elements + their options, date inputs,
                              submit buttons, the resolved portal URL)
  - pinellas_casetypes_form.png  (full-page screenshot of the search form)
  - pinellas_casetypes_page.html (raw HTML for offline selector inspection)

Usage:
    python -m scripts.experiments.probe_pinellas_casetypes
    python -m scripts.experiments.probe_pinellas_casetypes --no-proxy --headless
"""

import argparse
import asyncio
import json
from pathlib import Path

from playwright.async_api import async_playwright

from src.utils.http_helpers import (
    STEALTH_UA, STEALTH_ARGS, apply_stealth_to_page, get_playwright_proxy,
)
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

# Fallback if the DB source URL can't be resolved. From the selector notes:
# courtrecords.mypinellasclerk.gov, Case search is /MyCr/Cases/Search?s=e
FALLBACK_URL = "https://courtrecords.mypinellasclerk.gov/MyCr/Cases/Search?s=e"

SCRATCH = Path("scratch")


def _resolve_url(county_id: str) -> str:
    """Prefer the live DB-configured evictions/court_records source URL."""
    try:
        from src.utils.county_config import get_county_config
        cfg = get_county_config(county_id)
        sources = cfg.get("sources", {})
        src = sources.get("evictions") or sources.get("court_records") or {}
        url = src.get("url")
        if url:
            logger.info("[probe] Using DB source URL for %s: %s", county_id, url)
            return url
        logger.warning("[probe] No evictions/court_records URL in DB — using fallback")
    except Exception as e:
        logger.warning("[probe] Could not read county_config (%s) — using fallback", e)
    return FALLBACK_URL


# Dump every <select>, its options, every date-ish input, and submit buttons.
# Runs in page context so it sees the rendered DOM (post-JS), not raw HTML.
_DUMP_JS = """
() => {
    const selects = [...document.querySelectorAll('select')].map(s => ({
        id: s.id || null,
        name: s.getAttribute('name') || null,
        class: s.className || null,
        multiple: s.multiple,
        option_count: s.options.length,
        options: [...s.options].map(o => ({
            value: o.value,
            text: (o.text || '').trim(),
        })),
    }));
    const inputs = [...document.querySelectorAll('input')]
        .filter(i => /date|from|to|case|search/i.test(
            (i.id||'') + ' ' + (i.name||'') + ' ' + (i.placeholder||'') + ' ' + (i.type||'')
        ))
        .map(i => ({
            id: i.id || null, name: i.getAttribute('name') || null,
            type: i.type || null, placeholder: i.placeholder || null,
            class: i.className || null,
        }));
    const buttons = [...document.querySelectorAll('button, input[type=submit]')].map(b => ({
        tag: b.tagName.toLowerCase(),
        type: b.getAttribute('type') || null,
        value: b.getAttribute('value') || null,
        text: (b.innerText || '').trim() || null,
        id: b.id || null,
    }));
    const tabs = [...document.querySelectorAll('a, [role=tab], li > a')]
        .map(a => (a.innerText || '').trim())
        .filter(t => /name|case|attorney|calendar/i.test(t));
    return { selects, inputs, buttons, tabs, title: document.title, url: location.href };
}
"""


async def probe(url: str, headless: bool, no_proxy: bool) -> dict:
    SCRATCH.mkdir(parents=True, exist_ok=True)
    proxy = None if no_proxy else get_playwright_proxy()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, args=STEALTH_ARGS)
        context = await browser.new_context(user_agent=STEALTH_UA, proxy=proxy)
        page = await context.new_page()
        await apply_stealth_to_page(page)
        try:
            logger.info("[probe] Navigating to %s", url)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(2000)

            # Activate the Case tab (where the CaseType dropdown + date range live).
            for sel in (
                'a:has-text("Case")', 'li:has-text("Case") > a',
                '[role="tab"]:has-text("Case")', '.tab:has-text("Case")',
            ):
                tab = page.locator(sel)
                if await tab.count() > 0:
                    await tab.first.click()
                    await page.wait_for_timeout(1200)
                    logger.info("[probe] Clicked Case tab via: %s", sel)
                    break
            else:
                logger.warning("[probe] Case tab not found — dumping whatever rendered")

            dump = await page.evaluate(_DUMP_JS)
            dump["resolved_url"] = url

            (SCRATCH / "pinellas_casetypes.json").write_text(
                json.dumps(dump, indent=2), encoding="utf-8"
            )
            (SCRATCH / "pinellas_casetypes_page.html").write_text(
                await page.content(), encoding="utf-8"
            )
            await page.screenshot(
                path=str(SCRATCH / "pinellas_casetypes_form.png"), full_page=True
            )
            return dump
        finally:
            await browser.close()


def _print_summary(dump: dict) -> None:
    print("\n" + "=" * 70)
    print(f"PAGE: {dump.get('title')}  |  {dump.get('url')}")
    print(f"Tabs seen: {dump.get('tabs')}")
    print("=" * 70)
    selects = dump.get("selects", [])
    if not selects:
        print("\n⚠️  NO <select> elements found — Case tab may not have activated, "
              "or the CaseType filter is a custom widget (not a native select).\n")
    for s in selects:
        label = s.get("id") or s.get("name") or s.get("class") or "<anon>"
        print(f"\n<select> {label!r}  (multiple={s['multiple']}, {s['option_count']} options)")
        # Heuristic: only print fully for the likely CaseType select to keep noise down.
        looks_casetype = "case" in (label or "").lower() or s["option_count"] > 5
        for o in s["options"]:
            if looks_casetype:
                print(f"    {o['value']!r:>10}  →  {o['text']}")
        if not looks_casetype:
            print(f"    (small/unrelated select — {s['option_count']} options, skipped)")
    print("\nDate/case inputs:")
    for i in dump.get("inputs", []):
        print(f"    id={i['id']!r} name={i['name']!r} type={i['type']!r} ph={i['placeholder']!r}")
    print("\nSubmit buttons:")
    for b in dump.get("buttons", []):
        if (b.get("type") == "submit") or (b.get("value")) or (b.get("text")):
            print(f"    {b['tag']} type={b['type']!r} value={b['value']!r} text={b['text']!r}")
    print("\nArtifacts: scratch/pinellas_casetypes.json | _form.png | _page.html\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Probe Pinellas courtrecords Case-search form")
    ap.add_argument("--county-id", default="pinellas")
    ap.add_argument("--url", default=None, help="Override the portal URL")
    ap.add_argument("--headless", action="store_true", help="Run headless (default: headful)")
    ap.add_argument("--no-proxy", action="store_true", help="Disable Oxylabs proxy")
    args = ap.parse_args()

    url = args.url or _resolve_url(args.county_id)
    dump = asyncio.run(probe(url, headless=args.headless, no_proxy=args.no_proxy))
    _print_summary(dump)


if __name__ == "__main__":
    main()
