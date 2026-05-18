"""
Cloudflare-bypass session capture for the Pinellas Clerk ORI portal.

Uses `nodriver` (CDP-driven, undetected-chromedriver successor) to:
  1. Open the portal in a real Chrome window.
  2. Let YOU solve the Cloudflare challenge manually — the script pauses and
     waits for you to press Enter once you can see the search form.
  3. Extract cookies + localStorage and write them out as a Playwright-format
     storage_state.json file at the path printed at the end.

Run this only when the saved session is missing or stale (Cloudflare clearance
cookies typically last hours to a few days, then need refreshing).

  Install once:    pip install nodriver
  Then run:        python scripts/experiments/cf_capture_pinellas_clerk.py
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# Output path (Playwright storage_state JSON format) — kept for diagnostic only.
# The real production handoff is the PERSISTENT EDGE PROFILE directory below,
# because Cloudflare binds cf_clearance to TLS fingerprint and cookie-transfer
# alone won't pass the re-challenge.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = PROJECT_ROOT / "data" / "cf_session" / "pinellas_clerk_storage_state.json"
EDGE_PROFILE_DIR = PROJECT_ROOT / "data" / "cf_session" / "edge_profile"
TARGET_URL = "https://officialrecords.mypinellasclerk.gov/search/SearchTypeDocType"

# Browser auto-detection — env override wins; otherwise probe Chrome, then Edge.
_CHROMIUM_CANDIDATES = [
    os.environ.get("CHROME_PATH"),
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
]


def _find_browser() -> str | None:
    for p in _CHROMIUM_CANDIDATES:
        if p and os.path.isfile(p):
            return p
    return None


def _patch_nodriver_cookie_parser():
    """
    nodriver's `Cookie.from_json` requires fields (`sameParty`, `sourceScheme`,
    `sourcePort`) that newer Chrome/Edge CDP versions don't always emit, so
    calling `browser.cookies.get_all()` blows up with KeyError.

    Wrap it to default any missing field instead of crashing.
    """
    try:
        from nodriver.cdp import network as nw
    except Exception as e:
        print(f"[!] could not import nodriver.cdp.network for patch: {e}")
        return

    orig = nw.Cookie.from_json

    def lenient_from_json(json):
        j = dict(json)
        j.setdefault("sameParty",     False)
        j.setdefault("sourceScheme",  "Unset")
        j.setdefault("sourcePort",    -1)
        # `expires` / `sameSite` / `partitionKey` are already conditional in orig.
        try:
            return orig(j)
        except KeyError as ke:
            # Last-resort: fall back to a hand-rolled minimal Cookie if a new
            # required key shows up. We only use a subset for storage_state.
            class _MinimalCookie:
                pass
            mc = _MinimalCookie()
            mc.name        = str(j.get("name", ""))
            mc.value       = str(j.get("value", ""))
            mc.domain      = str(j.get("domain", ""))
            mc.path        = str(j.get("path", "/"))
            mc.http_only   = bool(j.get("httpOnly", False))
            mc.secure      = bool(j.get("secure", False))
            mc.session     = bool(j.get("session", False))
            mc.expires     = j.get("expires")
            mc.same_site   = j.get("sameSite")
            mc.to_json     = lambda: {
                "name": mc.name, "value": mc.value, "domain": mc.domain,
                "path": mc.path, "expires": mc.expires or -1,
                "httpOnly": mc.http_only, "secure": mc.secure,
                "sameSite": str(mc.same_site or "Lax"),
            }
            return mc

    # Replace the classmethod. nodriver calls it as Cookie.from_json(dict).
    nw.Cookie.from_json = lenient_from_json
    print("[*] nodriver Cookie.from_json patched for sameParty/source* tolerance")


async def main():
    try:
        import nodriver as uc
    except ImportError:
        print("[!] nodriver not installed. Run:  .venv\\Scripts\\python.exe -m pip install nodriver")
        sys.exit(1)

    browser_path = _find_browser()
    if not browser_path:
        print("[!] No Chrome or Edge install found on this machine.")
        print("    Install Chrome from https://www.google.com/chrome/ and rerun,")
        print("    or set CHROME_PATH to a Chromium-family browser executable.")
        sys.exit(1)

    print(f"[*] Browser binary: {browser_path}")
    _patch_nodriver_cookie_parser()
    EDGE_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[*] Persistent profile: {EDGE_PROFILE_DIR}")
    print(f"[*] Launching → {TARGET_URL}")
    print("[*] A browser window will open. Solve the Cloudflare challenge if")
    print("    one appears, then return here and press Enter.")
    print("[*] IMPORTANT: this profile is NOT deleted on exit — the verify")
    print("    script reads from it directly.")
    print()

    browser = await uc.start(
        headless=False,
        browser_executable_path=browser_path,
        user_data_dir=str(EDGE_PROFILE_DIR),
    )
    page = await browser.get(TARGET_URL)

    # Give the page a moment to render the CF challenge
    await asyncio.sleep(2)

    # Wait for human to clear the challenge
    input(">>> Once you can see the Pinellas Clerk search form (not the CF "
          "challenge), press Enter here to capture session: ")

    print("[*] Extracting cookies + localStorage...")

    # Re-grab the active tab in case CF redirected. Iterate all tabs and pick
    # the one currently on the target host.
    print("    - locating active tab...")
    target_tab = page
    try:
        tabs = browser.tabs if hasattr(browser, "tabs") else []
        for t in tabs:
            url = getattr(t, "url", "") or ""
            if "mypinellasclerk.gov" in url:
                target_tab = t
                break
    except Exception as exc:
        print(f"      (tab scan failed, using original page handle: {exc})")

    # --- cookies via nodriver high-level API (parser is patched above) ----
    print("    - reading cookies...")
    pw_cookies = []
    same_site_map = {
        "Strict": "Strict", "Lax": "Lax", "None": "None",
        "no_restriction": "None", "lax": "Lax", "strict": "Strict",
    }

    raw = []
    try:
        raw = await asyncio.wait_for(browser.cookies.get_all(), timeout=20.0)
        print(f"      OK: {len(raw or [])} cookies")
    except Exception as exc:
        print(f"      cookie fetch failed: {exc!r}")
        raw = []

    for c in (raw or []):
        d = c.to_json() if hasattr(c, "to_json") else (
            dict(c) if isinstance(c, dict) else dict(c.__dict__)
        )
        expires = d.get("expires", -1)
        if expires is None or expires == 0:
            expires = -1
        pw_cookies.append({
            "name":     d.get("name"),
            "value":    d.get("value"),
            "domain":   d.get("domain"),
            "path":     d.get("path", "/"),
            "expires":  float(expires) if expires != -1 else -1,
            "httpOnly": bool(d.get("httpOnly", d.get("http_only", False))),
            "secure":   bool(d.get("secure", False)),
            "sameSite": same_site_map.get(d.get("sameSite", d.get("same_site", "Lax")), "Lax"),
        })

    # --- localStorage via page.evaluate ------------------------------------
    print("    - reading localStorage...")
    ls_raw = []
    try:
        ls_raw = await asyncio.wait_for(
            target_tab.evaluate("""
                (() => {
                    const out = [];
                    for (let i = 0; i < localStorage.length; i++) {
                        const k = localStorage.key(i);
                        out.push({name: k, value: localStorage.getItem(k)});
                    }
                    return out;
                })()
            """),
            timeout=10.0,
        )
        print(f"      localStorage OK: {len(ls_raw or [])} entries")
    except Exception as exc:
        print(f"      localStorage failed (continuing without): {exc!r}")
        ls_raw = []

    storage_state = {
        "cookies": pw_cookies,
        "origins": [
            {
                "origin":       "https://officialrecords.mypinellasclerk.gov",
                "localStorage": ls_raw or [],
            }
        ],
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(storage_state, indent=2))

    print(f"[+] Captured {len(pw_cookies)} cookies, {len(ls_raw)} localStorage entries")
    print(f"[+] Saved JSON → {OUTPUT_PATH}  (diagnostic only)")
    print(f"[+] Edge profile retained → {EDGE_PROFILE_DIR}  (this is what the verify script uses)")
    print()
    print("Next step: run  python scripts/experiments/cf_test_with_playwright.py")
    print("to verify Playwright + Edge + that profile can reuse the session and skip CF.")

    # browser.stop() is sync in this nodriver version — don't await
    try:
        result = browser.stop()
        if asyncio.iscoroutine(result):
            await result
    except Exception as exc:
        print(f"[!] browser.stop warning (non-fatal): {exc}")


if __name__ == "__main__":
    asyncio.run(main())
