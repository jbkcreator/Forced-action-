"""
E2E Playwright tests for Concierge Chat (M5a + M5b).

Requires both servers running:
  - Backend:  uvicorn src.api.main:app --port 8000
  - Frontend: npm run dev (port 5173)

Run:
  python tests/e2e_concierge_chat.py
"""

import time
import sys
from playwright.sync_api import sync_playwright, expect

FRONTEND = "http://localhost:5173"
BACKEND  = "http://localhost:8000"

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"

results = []

def check(name, cond, detail=""):
    status = PASS if cond else FAIL
    print(f"  {status}  {name}" + (f" — {detail}" if detail else ""))
    results.append((name, cond))


def run_api_smoke(requests_session):
    """Quick API-level smoke tests before browser tests."""
    import urllib.request, json

    print("\n[1] API smoke tests")

    # Create session
    req = urllib.request.Request(
        f"{BACKEND}/api/chat/sessions",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        body = json.loads(r.read())
    session_id = body.get("session_id", "")
    check("POST /api/chat/sessions returns session_id", bool(session_id), session_id[:12])

    # Send a message
    payload = json.dumps({"session_id": session_id, "content": "what areas do you cover?"}).encode()
    req = urllib.request.Request(
        f"{BACKEND}/api/chat/messages",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        msg_body = json.loads(r.read())
    check("POST /api/chat/messages returns content", bool(msg_body.get("content")), str(msg_body.get("content", ""))[:40])
    check("Response has intent label", "intent" in msg_body, str(msg_body.keys()))

    # Escalate
    payload = json.dumps({"reason": "user_requested"}).encode()
    req = urllib.request.Request(
        f"{BACKEND}/api/chat/sessions/{session_id}/escalate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        esc_body = json.loads(r.read())
    check("POST /escalate returns route", "route" in esc_body, esc_body.get("route"))

    return session_id


def run_landing_chat(page):
    """Test pre-signup chat widget on landing page."""
    print("\n[2] Landing page — pre-signup chat")

    page.goto(FRONTEND, wait_until="networkidle", timeout=15_000)

    # Chat bubble — aria-label is "Ask AI"
    bubble = page.locator("button[aria-label='Ask AI']").first
    bubble.wait_for(state="visible", timeout=10_000)
    check("Chat bubble visible on landing page", bubble.is_visible())

    # Click to open
    bubble.click()
    time.sleep(0.5)

    # Drawer opens — identified by role=dialog aria-label="Forced Action Concierge"
    chat_container = page.locator("[role='dialog'][aria-label='Forced Action Concierge']").first
    chat_container.wait_for(state="visible", timeout=5_000)
    check("Chat panel opens on click", chat_container.is_visible())

    # Send a message via the textarea inside the drawer
    textarea = chat_container.locator("textarea").first
    textarea.fill("what ZIP codes do you cover?")
    textarea.press("Enter")

    # Wait for assistant reply
    time.sleep(8)  # allow Claude call + streaming to complete

    # At least 2 bubbles (user + assistant)
    msgs = chat_container.locator("p, [class*='message'], [class*='bubble']")
    count = msgs.count()
    check("Assistant reply appears in chat", count >= 2, f"{count} elements")

    # Close chat
    close_btn = page.locator("button[aria-label='Close chat']").first
    if close_btn.is_visible():
        close_btn.click()


def run_dashboard_chat(page, feed_uuid):
    """Test post-signup chat widget on dashboard."""
    print(f"\n[3] Dashboard page — post-signup chat (uuid={feed_uuid[:8]}…)")

    page.goto(f"{FRONTEND}/dashboard/{feed_uuid}", wait_until="domcontentloaded", timeout=20_000)
    time.sleep(2)  # allow React hydration + initial data fetches

    bubble = page.locator("button[aria-label='Ask AI']").first
    bubble.wait_for(state="visible", timeout=10_000)
    check("Chat bubble visible on dashboard", bubble.is_visible())

    bubble.click()
    time.sleep(0.5)

    chat_container = page.locator("[role='dialog'][aria-label='Forced Action Concierge']").first
    chat_container.wait_for(state="visible", timeout=5_000)
    check("Dashboard chat panel opens", chat_container.is_visible())

    textarea = chat_container.locator("textarea").first
    textarea.fill("how do I get more leads?")
    textarea.press("Enter")
    time.sleep(8)

    msgs = chat_container.locator("p, [class*='message'], [class*='bubble']")
    count = msgs.count()
    check("Dashboard assistant reply appears", count >= 2, f"{count} elements")


def get_any_feed_uuid():
    """Pull a subscriber feed UUID from the DB for dashboard test."""
    import urllib.request, json
    try:
        req = urllib.request.Request(f"{BACKEND}/api/admin/subscribers?limit=1",
                                     headers={"Authorization": "Bearer skip"})
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        if data and isinstance(data, list) and data[0].get("event_feed_uuid"):
            return data[0]["event_feed_uuid"]
    except Exception:
        pass
    # Fall back to a known test UUID from the feed endpoint
    return None


def main():
    # 1. API smoke (no browser)
    run_api_smoke(None)

    # 2 + 3. Browser tests
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=150)
        ctx = browser.new_context(viewport={"width": 1280, "height": 800})
        page = ctx.new_page()

        run_landing_chat(page)

        feed_uuid = get_any_feed_uuid() or "rec-d2ef54ca"
        run_dashboard_chat(page, feed_uuid)

        browser.close()

    # Summary
    passed = sum(1 for _, ok in results if ok)
    total  = len(results)
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} passed")
    if passed < total:
        print("Failed:")
        for name, ok in results:
            if not ok:
                print(f"  ✗ {name}")
        sys.exit(1)
    else:
        print("All checks passed.")


if __name__ == "__main__":
    main()
