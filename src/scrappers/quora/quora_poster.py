"""
Quora answer poster — submits a drafted answer to a Quora question via Playwright.

Uses sync_playwright so it can be called from a thread pool executor without
conflicting with FastAPI's asyncio event loop.

Auth: requires a populated persistent profile at data/quora_profile/ captured
by quora_auth.py. The profile must contain valid m-b/m-s session cookies.

Not called directly — invoked by POST /api/admin/quora/{id}/post via
asyncio.get_event_loop().run_in_executor().
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.sync_api import Page, sync_playwright

from src.utils.http_helpers import STEALTH_ARGS, STEALTH_UA, get_playwright_proxy, get_stealth

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_PROFILE_DIR  = _PROJECT_ROOT / "data" / "quora_profile"
_DEBUG_DIR    = _PROJECT_ROOT / "data" / "debug" / "quora"

_MAX_POST_ATTEMPTS = 3

# How long to wait for interactive elements after page load
_ELEMENT_TIMEOUT = 15_000   # ms
_SUBMIT_TIMEOUT  = 30_000   # ms — submit + GQL round trip


def post_answer_to_quora(
    qid: int,
    question_url: str,
    answer_markdown: str,
) -> str:
    """
    Navigate to question_url, click Answer, type the answer, submit.
    Returns quora_answer_id on success.
    Raises RuntimeError on any failure so the caller can record error_log.

    Runs synchronously — call via run_in_executor from async context.
    """
    if not _PROFILE_DIR.exists() or not any(_PROFILE_DIR.iterdir()):
        raise RuntimeError(
            "Quora profile not found — run quora_auth.py first to capture a logged-in session"
        )

    _DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(_PROFILE_DIR),
            headless=True,
            args=STEALTH_ARGS,
            user_agent=STEALTH_UA,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            proxy=get_playwright_proxy(),
        )
        try:
            page = context.new_page()
            page.add_init_script(get_stealth().script_payload)
            answer_id = _submit_answer(page, qid, question_url, answer_markdown)
            return answer_id
        except Exception as exc:
            _save_failure_screenshot(context, qid)
            raise RuntimeError(f"Post failed for qid={qid}: {exc}") from exc
        finally:
            context.close()


def _submit_answer(
    page: Page,
    qid: int,
    question_url: str,
    answer_markdown: str,
) -> str:
    logger.info("[poster] navigating  qid=%s  url=%s", qid, question_url)

    # domcontentloaded only — networkidle never fires on Quora (perpetual long-poll)
    page.goto(question_url, wait_until="domcontentloaded", timeout=30_000)

    # Give React/Relay a moment to hydrate after DOM is ready
    page.wait_for_timeout(3_000)

    _assert_authenticated(page)
    _click_answer_button(page, qid)

    # Answer click may navigate to a draft page or open an inline editor — wait for editor
    editor = _wait_for_editor(page, qid)
    _type_answer(page, editor, answer_markdown)

    answer_id = _submit_and_capture(page, qid)
    logger.info("[poster] answer posted  qid=%s  answer_id=%s", qid, answer_id)
    return answer_id


def _assert_authenticated(page: Page) -> None:
    """Raise if the page indicates a logged-out session."""
    content = page.content()
    if "loggedIn:false" in content or "/login" in page.url:
        raise RuntimeError(
            "Quora session expired — re-run quora_auth.py to refresh the profile"
        )
    # Presence of the question title confirms the page loaded correctly
    if not page.locator(".puppeteer_test_question_title").count():
        logger.warning("[poster] question title not found — page may not have loaded fully")


def _click_answer_button(page: Page, qid: int) -> None:
    """Click the Answer button. Uses role-based selector which is auth-stable."""
    logger.debug("[poster] looking for Answer button  qid=%s", qid)
    try:
        btn = page.get_by_role("button", name=re.compile(r"^Answer$", re.I))
        btn.wait_for(state="visible", timeout=_ELEMENT_TIMEOUT)
        btn.click()
        logger.debug("[poster] Answer button clicked  qid=%s", qid)
        return
    except Exception:
        pass

    # Fallback: any visible element with text "Answer" that behaves as a button
    for selector in [
        "button:has-text('Answer')",
        "[role='button']:has-text('Answer')",
        "a:has-text('Answer')",
    ]:
        try:
            el = page.locator(selector).first
            el.wait_for(state="visible", timeout=5_000)
            el.click()
            logger.debug("[poster] Answer button clicked via fallback  selector=%s  qid=%s",
                         selector, qid)
            return
        except Exception:
            continue

    raise RuntimeError(
        f"Answer button not found for qid={qid} — "
        "session may be logged out or Quora's DOM changed"
    )


def _wait_for_editor(page: Page, qid: int):
    """
    Wait for the rich-text editor to appear. Clicking Answer either opens an
    inline editor or navigates to a draft page — handle both by waiting on the
    contenteditable element regardless of URL.
    """
    logger.debug("[poster] waiting for editor  qid=%s", qid)
    try:
        editor = page.locator('[contenteditable="true"]').first
        editor.wait_for(state="visible", timeout=_ELEMENT_TIMEOUT)
        return editor
    except Exception as exc:
        raise RuntimeError(
            f"Answer editor did not appear for qid={qid} — "
            "Answer button may have opened a login prompt instead"
        ) from exc


def _type_answer(page: Page, editor, answer_markdown: str) -> None:
    """
    Insert the answer into Quora's rich-text editor.

    Two-step approach:
    1. insertHTML with <br><br> paragraph separators (avoids <p> reversal bug)
       and link text as plain text (Quora sanitizer strips raw <a> tags).
    2. For each markdown link, select the plain link text and apply
       execCommand('createLink') — the same command Quora's own toolbar uses,
       which the editor accepts and keeps.
    """
    editor.click()
    page.wait_for_timeout(500)

    links = _extract_md_links(answer_markdown)
    html  = _md_to_html(answer_markdown)
    page.evaluate("html => document.execCommand('insertHTML', false, html)", html)
    page.wait_for_timeout(500)

    for link_text, link_url in links:
        page.evaluate("""([text, url]) => {
            const editor = document.querySelector('[contenteditable="true"]');
            if (!editor) return false;
            const walker = document.createTreeWalker(editor, NodeFilter.SHOW_TEXT);
            let node;
            while ((node = walker.nextNode())) {
                const idx = node.textContent.indexOf(text);
                if (idx < 0) continue;
                const range = document.createRange();
                range.setStart(node, idx);
                range.setEnd(node, idx + text.length);
                const sel = window.getSelection();
                sel.removeAllRanges();
                sel.addRange(range);
                document.execCommand('createLink', false, url);
                return true;
            }
            return false;
        }""", [link_text, link_url])
        page.wait_for_timeout(200)


def _extract_md_links(md: str) -> list[tuple[str, str]]:
    """Return all (label, url) pairs from markdown [label](url) syntax."""
    return re.findall(r'\[([^\]]+)\]\(([^)]+)\)', md)


def _md_to_html(md: str) -> str:
    """
    Convert answer markdown to HTML for execCommand('insertHTML').

    Uses <br><br> between paragraphs (not <p> tags) because Quora's
    contenteditable reverses the order of block-level elements on insertion.
    Links are rendered as plain label text — caller applies them separately
    via execCommand('createLink') to avoid Quora's <a> tag sanitizer.
    """
    import re as _re

    def inline(text: str) -> str:
        # Links → label only (URL applied separately via createLink)
        text = _re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        # Bold: **text**
        text = _re.sub(r'\*\*([^*\n]+)\*\*', r'<strong>\1</strong>', text)
        # Italic: *text*
        text = _re.sub(r'(?<!\*)\*([^*\n]+)\*(?!\*)', r'<em>\1</em>', text)
        return text

    parts: list[str] = []
    paragraph_lines: list[str] = []
    in_ul = False

    def flush_paragraph() -> None:
        nonlocal paragraph_lines
        if paragraph_lines:
            if parts:
                parts.append('<br><br>')
            parts.append(inline(' '.join(paragraph_lines)))
            paragraph_lines = []

    def close_list() -> None:
        nonlocal in_ul
        if in_ul:
            in_ul = False

    for raw_line in md.splitlines():
        line = raw_line.rstrip()

        # H1–H3 → bold inline (Quora doesn't render article headers)
        h = _re.match(r'^#{1,3}\s+(.*)', line)
        if h:
            close_list()
            flush_paragraph()
            if parts:
                parts.append('<br><br>')
            parts.append(f'<strong>{inline(h.group(1))}</strong>')
            continue

        # Blockquote → italic inline
        bq = _re.match(r'^>\s*(.*)', line)
        if bq:
            close_list()
            flush_paragraph()
            if parts:
                parts.append('<br><br>')
            parts.append(f'<em>{inline(bq.group(1))}</em>')
            continue

        # Unordered list item → bullet with line break
        li = _re.match(r'^[-*]\s+(.*)', line)
        if li:
            flush_paragraph()
            if not in_ul:
                if parts:
                    parts.append('<br>')
                in_ul = True
            else:
                parts.append('<br>')
            parts.append(f'• {inline(li.group(1))}')
            continue

        # Empty line → paragraph boundary
        if not line:
            close_list()
            flush_paragraph()
            continue

        # Regular text — accumulate into current paragraph
        close_list()
        paragraph_lines.append(line)

    close_list()
    flush_paragraph()
    return ''.join(parts)


def _submit_and_capture(page: Page, qid: int) -> str:
    """Click Submit and capture the answer ID from the GQL response."""
    answer_id: Optional[str] = None

    def _on_response(response):
        nonlocal answer_id
        if "gql_para_POST" in response.url and answer_id is None:
            try:
                body = response.json()
                aid = _extract_answer_id(body)
                if aid:
                    answer_id = aid
            except Exception:
                pass

    page.on("response", _on_response)

    # Submit button — role-based with text fallback
    submitted = False
    try:
        btn = page.get_by_role("button", name=re.compile(r"Submit|Post", re.I))
        btn.wait_for(state="visible", timeout=_ELEMENT_TIMEOUT)
        btn.click()
        submitted = True
    except Exception:
        pass

    if not submitted:
        for selector in ["button:has-text('Submit')", "button:has-text('Post')"]:
            try:
                el = page.locator(selector).first
                el.wait_for(state="visible", timeout=5_000)
                el.click()
                submitted = True
                break
            except Exception:
                continue

    if not submitted:
        raise RuntimeError("Submit button not found — Quora UI may have changed")

    # Wait for the GQL mutation response that confirms the post
    page.wait_for_timeout(_SUBMIT_TIMEOUT // 3)  # give the request time to fire

    page.remove_listener("response", _on_response)

    if not answer_id:
        answer_id = _answer_id_from_url(page.url) or f"qid_{qid}_posted"

    return answer_id


def _extract_answer_id(body: dict, depth: int = 0) -> Optional[str]:
    if depth > 8 or not isinstance(body, dict):
        return None
    for key, val in body.items():
        if key in ("answerId", "aid") and isinstance(val, (str, int)) and val:
            return str(val)
        if isinstance(val, dict):
            found = _extract_answer_id(val, depth + 1)
            if found:
                return found
        if isinstance(val, list):
            for item in val:
                found = _extract_answer_id(item, depth + 1)
                if found:
                    return found
    return None


def _answer_id_from_url(url: str) -> Optional[str]:
    match = re.search(r"/answer/(\d+)", url)
    return match.group(1) if match else None


def _save_failure_screenshot(context, qid: int) -> None:
    try:
        pages = context.pages
        if pages:
            ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
            path = _DEBUG_DIR / f"post_failure_{qid}_{ts}.jpg"
            pages[-1].screenshot(path=str(path), type="jpeg", quality=80)
            logger.info("[poster] failure screenshot saved  path=%s", path)
    except Exception as exc:
        logger.warning("[poster] could not save failure screenshot: %s", exc)
