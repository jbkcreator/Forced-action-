# Pinellas Clerk ORI — Cloudflare Bypass

**Status:** Proof of concept validated 2026-05-11. Production wiring pending.

**Scope:** Pinellas Clerk Official Records portal
(`officialrecords.mypinellasclerk.gov`). Affects the lien, judgment, probate,
divorce, and eviction signal scrapers — all of which read from this portal.
Hillsborough Clerk is **not** Cloudflare-protected and uses the existing
`browser_use` / `selector` path; this document applies only to Pinellas.

---

## Problem

The Pinellas Clerk Official Records portal is fronted by Cloudflare bot
management. Both vanilla Playwright and `browser_use` (the LLM-driven Agent)
get bounced to a "Just a moment…" challenge page and never reach the actual
search form. As a result, no Pinellas Clerk-sourced signals (liens,
judgments, probate, divorce, evictions) flow into the platform today, even
though Hillsborough equivalents flow fine.

This was flagged as a major blocker in
[`PINELLAS_EXPANSION_RESEARCH.md`](./PINELLAS_EXPANSION_RESEARCH.md) and
the multi-county build plan; the workaround documented there was an
operational PRR (Public Records Request) cadence plus a parallel
bulk-data-agreement procurement track with the Clerk's office.

This document describes a **technical bridge** that lets the platform
ingest Pinellas Clerk data automatically while the bulk-data agreement
is being negotiated.

---

## Solution (validated)

Use `nodriver` (CDP-driven, undetected-chromedriver successor) to drive a
**real Microsoft Edge install** against a **persistent user-data
directory**. Cloudflare's JS challenge resolves automatically — **no human
input is required** — and the cleared session state is preserved in the
profile directory. Subsequent runs through Playwright can reuse that same
profile (same Edge binary, same TLS fingerprint, same cookies) and never
see the challenge.

### Why this works

`cf_clearance` is bound server-side to:

- The exact TLS handshake fingerprint of the browser that earned it
- The User-Agent string
- The client IP
- Browser fingerprint signals (timezone, screen, fonts)

Transferring cookies via JSON to a different browser engine (Playwright's
bundled Chromium) **does not work** — we proved this. The TLS handshake
differs from Edge's and Cloudflare re-challenges.

Reusing the **same Edge binary + same user-data-dir** preserves all of
those bindings simultaneously, so Cloudflare sees a returning legitimate
client and waves it through.

### Why nodriver is required for the first hit

nodriver patches the navigator.webdriver flag, the Chrome automation
strings, and CDP-detection surfaces. A plain `playwright.chromium.launch`
or even `playwright.chromium.launch_persistent_context` against Edge gets
flagged on the *first* visit because Cloudflare's JS challenge inspects
those automation markers. nodriver passes the challenge cleanly the first
time; once the profile is warmed, Playwright can reuse it because the
challenge isn't re-served.

---

## Files

```
scripts/experiments/
├── cf_capture_pinellas_clerk.py    # Warm the persistent Edge profile.
│                                    # Launches Edge via nodriver, navigates
│                                    # to the portal, waits for CF challenge
│                                    # to clear (automatic — no human input).
│                                    # Saves cookies as JSON (diagnostic) and
│                                    # leaves the user-data-dir intact.
│
└── cf_test_with_playwright.py      # Verifier. Opens Playwright with
                                     # launch_persistent_context against
                                     # the same Edge binary + same profile.
                                     # Confirms the search form is visible
                                     # (not the CF challenge page).

data/cf_session/
├── edge_profile/                    # Persistent Edge user-data-dir.
│                                    # cf_clearance, ASP.NET_SessionId,
│                                    # AVI_COOKIE, fingerprint state.
│                                    # DO NOT DELETE — this IS the bypass.
│
├── pinellas_clerk_storage_state.json  # Diagnostic dump of cookies in
│                                       # Playwright's storage_state format.
│                                       # Not used by the verifier or
│                                       # production engine.
│
└── verify_ok.png                    # Latest verifier screenshot.
```

Also added to `src/utils/action_sequence.py`:
- Monkey-patch for `nodriver.cdp.network.Cookie.from_json` to tolerate
  missing `sameParty` / `sourceScheme` / `sourcePort` fields that newer
  Edge CDP versions don't emit. Without this patch nodriver crashes when
  reading cookies from any modern Edge.

---

## How to run today

One-time setup:

```powershell
.venv\Scripts\python.exe -m pip install nodriver
```

Warm the profile (no human input required — the script waits for CF to
auto-resolve, then captures):

```powershell
.venv\Scripts\python.exe scripts\experiments\cf_capture_pinellas_clerk.py
```

Verify Playwright + the warmed profile can reach the portal:

```powershell
.venv\Scripts\python.exe scripts\experiments\cf_test_with_playwright.py
```

Expected output of the verifier:

```
VERDICT: Portal accessible — Cloudflare bypass is working.
```

---

## Production wiring (NEXT)

### 1. Lien engine changes

The lien engine's selector-mode path needs to optionally launch Playwright
against the persistent Edge profile when the source is flagged as
Cloudflare-protected.

Current pattern (works for Hillsborough):

```python
async with async_playwright() as pw:
    browser = await pw.chromium.launch(headless=not headful, args=_browser_args)
    context = await browser.new_context(accept_downloads=True)
```

New pattern (works for Pinellas when `use_persistent_edge_profile=true` in
`special_flags`):

```python
async with async_playwright() as pw:
    if source.get("use_persistent_edge_profile"):
        edge_path = _find_edge_binary()
        profile_dir = PROJECT_ROOT / "data" / "cf_session" / "edge_profile"
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            executable_path=edge_path,
            headless=not headful,
            accept_downloads=True,
            args=["--disable-blink-features=AutomationControlled"],
            ignore_default_args=["--enable-automation"],
        )
    else:
        browser = await pw.chromium.launch(headless=not headful, args=_browser_args)
        context = await browser.new_context(accept_downloads=True)
```

Everything downstream (cached `playwright_code`, action_sequence pattern,
smoke test, ColumnMapper, loader) works unchanged — the LLM-generated
scrape function doesn't know or care which browser engine is hosting it.

### 2. Source config in admin UI

For Pinellas liens (`source_id=9`) and any other Cloudflare-protected
sources, set in `special_flags`:

```json
{
  "scrape_mode": "selector",
  "use_persistent_edge_profile": true,
  "selectors": { ... },
  "playwright_code": "<populated on first run>"
}
```

Hillsborough liens (`source_id=2`) keeps `use_persistent_edge_profile`
unset — uses the existing Chromium path.

### 3. Failure detection

The engine should distinguish "CF challenge detected" from other errors and
surface a clear operator alert rather than retrying noisily. Detection
heuristic in the generated code:

```python
title = await page.title()
if "just a moment" in title.lower() or "checking your browser" in title.lower():
    raise PlaywrightCodeError("CF_CHALLENGE_DETECTED — profile refresh needed")
```

When this fires:
1. Engine clears the cached `playwright_code` (existing self-heal path)
2. Engine logs a high-visibility alert ("Pinellas Clerk profile expired")
3. Operator runs the capture script to refresh, next run resumes

### 4. Concurrency lock

The persistent Edge profile can only be held by one process at a time.
Implications:
- Pinellas Clerk scrapes must run **sequentially**, not concurrently
- The capture script and the engine cannot run simultaneously
- Easy enforcement: a simple file-lock around runs that touch the profile

---

## Operational reality

The bypass works, but is not lights-out forever. Operational profile:

| Condition | Re-capture frequency |
|---|---|
| Same server, stable IP, daily scrape | A few days to a few weeks |
| IP changes (DHCP renewal, network change) | Instant — profile dies |
| Edge auto-updates to a new version | Instant — TLS fingerprint changes |
| Cloudflare tightens bot rules on Pinellas Clerk | Whenever they want |

Re-capture is ~30 seconds of script-runtime (no human input needed for the
JS challenge itself). The "human cost" is "did the alert fire today?"
rather than "did someone solve a CAPTCHA?"

### What this isn't

- **Not** a substitute for the formal bulk-data agreement with the Pinellas
  Clerk's office. That conversation must continue in parallel. This bridge
  is a stopgap, not the destination.
- **Not** scalable across multiple servers without per-server profile
  warming. If the platform later runs scrapers across a cluster, each box
  needs its own warmed profile or the lien path needs centralization.
- **Not** robust to Cloudflare policy changes on Pinellas Clerk's end.
  Site owners can change bot rules at any time and we'd need to re-test.

---

## Tomorrow's tasks

1. Wire `use_persistent_edge_profile` flag into the lien engine
   (`src/scrappers/liens/lien_engine.py`) — mirror the
   `permit_engine._scrape_selector` pattern but with the Edge-profile
   launch path described above.
2. Add CF-challenge detection to the action_sequence system prompt so
   generated code raises `PlaywrightCodeError` on bot-block pages.
3. Capture Pinellas Clerk selectors via DevTools → save to
   `special_flags.selectors` for `source_id=9`.
4. Set `scrape_mode='selector'` + `use_persistent_edge_profile=true` on
   `source_id=9`.
5. Test end-to-end: warm profile → run lien engine for Pinellas →
   verify CSV downloads + loads to DB.
6. Document the operator runbook ("if Pinellas lien scrape alerts
   `CF_CHALLENGE_DETECTED`, run `python scripts\experiments\cf_capture_pinellas_clerk.py`").
7. Continue Hillsborough lien engine migration to the action_sequence path
   in parallel — it's blocked on portal research, not on this bypass work.

---

## References

- [`PINELLAS_EXPANSION_RESEARCH.md`](./PINELLAS_EXPANSION_RESEARCH.md) — original
  Pinellas source survey, identified Cloudflare blocker
- [`MULTI_COUNTY_SCRAPING_ARCHITECTURE.md`](./MULTI_COUNTY_SCRAPING_ARCHITECTURE.md) — 4-mode
  routing layer, base context for engine changes
- [`DYNAMIC_ACTION_SEQUENCE_ARCHITECTURE.md`](./DYNAMIC_ACTION_SEQUENCE_ARCHITECTURE.md) — the
  LLM-generated Playwright code pattern that this work plugs into
- nodriver project: https://github.com/ultrafunkamsterdam/nodriver
