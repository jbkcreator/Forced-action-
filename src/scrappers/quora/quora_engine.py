"""
Quora authenticated search scraper.

Requires a session file created by quora_auth.py:
    python -m src.scrappers.quora.quora_auth

Usage:
    python -m src.scrappers.quora.quora_engine "foreclosures Tampa FL"
    python -m src.scrappers.quora.quora_engine "foreclosures Tampa FL" --dump-raw
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import quote

from playwright.async_api import Page, async_playwright

from src.utils.http_helpers import STEALTH_ARGS, STEALTH_UA, apply_stealth_to_page

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
SESSION_FILE  = _PROJECT_ROOT / "data" / "quora_session.json"
PROFILE_DIR   = _PROJECT_ROOT / "data" / "quora_profile"
_DEBUG_DIR    = _PROJECT_ROOT / "data" / "debug" / "quora"
_SEARCH_URL   = "https://www.quora.com/search?q={query}&type=question"

Status = Literal["ok", "partial", "auth_required", "error"]


@dataclass
class QuoraResult:
    # ── Core fields ───────────────────────────────────────────────────────────
    position: int
    title: str
    url: str
    answer_count: int
    follower_count: int
    top_answer_snippet: str
    top_answer_author: str
    top_answer_author_url: str
    # ── Identity fields ───────────────────────────────────────────────────────
    qid: Optional[int]                            = None
    quora_id: Optional[str]                       = None
    slug: Optional[str]                           = None
    canonical_url: Optional[str]                  = None
    # ── Count fields ─────────────────────────────────────────────────────────
    decanonicalized_answer_count: Optional[int]   = None
    comment_count: Optional[int]                  = None
    view_count: Optional[int]                     = None
    # ── Timestamp fields ─────────────────────────────────────────────────────
    created_time: Optional[datetime]              = None
    last_activity_time: Optional[datetime]        = None
    # ── Content fields ───────────────────────────────────────────────────────
    question_description: Optional[str]           = None
    topics: list[str]                             = field(default_factory=list)
    top_answer_upvotes: Optional[int]             = None
    top_answer_author_credentials: Optional[str]  = None
    # ── Question state flags ──────────────────────────────────────────────────
    is_locked: bool                               = False
    is_deleted: bool                              = False
    is_sensitive: bool                            = False
    viewer_should_show_write_answer: Optional[bool] = None
    viewer_cant_answer: Optional[bool]            = None
    is_user_limited_distro: Optional[bool]        = None
    # ── Scoring / Cora ────────────────────────────────────────────────────────
    deterministic_score: Optional[int]            = None
    deterministic_reasons: list[str]              = field(default_factory=list)
    cora_classification: Optional[dict]           = None
    cora_answer_draft: Optional[dict]             = None
    # ── Raw payload ───────────────────────────────────────────────────────────
    raw_metadata: Optional[dict]                  = None


@dataclass
class QuoraSearchResponse:
    query: str
    results: list[QuoraResult]         = field(default_factory=list)
    status: Status                     = "error"
    error: Optional[str]               = None
    raw_records: Optional[list[dict]]  = None


# ---------------------------------------------------------------------------
# Qtext flattening
# ---------------------------------------------------------------------------

def flatten_quora_qtext(value: Any) -> str:
    """
    Quora question titles arrive as either a plain string or a Qtext JSON
    structure like {"sections": [{"spans": [{"text": "...", "modifiers": {}}]}]}.
    This helper always returns a plain string regardless of input shape.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped.startswith("{"):
            return stripped
        try:
            value = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return stripped
    if isinstance(value, dict):
        try:
            sections = value.get("sections") or []
            parts = []
            for section in sections:
                for span in (section.get("spans") or []):
                    text = span.get("text", "")
                    if text:
                        parts.append(text)
            result = " ".join(parts).strip()
            return result if result else str(value)
        except Exception:
            return str(value)
    return str(value)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def scrape_quora(
    queries: list[str],
    max_results: int = 20,
    headless: bool = False,
    dump_raw: bool = False,
) -> list[QuoraSearchResponse]:
    responses: list[QuoraSearchResponse] = []
    async with async_playwright() as pw:
        context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            args=STEALTH_ARGS,
            user_agent=STEALTH_UA,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        try:
            for query in queries:
                result = await _scrape_query(context, query, max_results, dump_raw)
                responses.append(result)
        finally:
            await context.close()

    return responses


# ---------------------------------------------------------------------------
# Per-query scrape
# ---------------------------------------------------------------------------

async def _scrape_query(
    context, query: str, max_results: int, dump_raw: bool
) -> QuoraSearchResponse:
    page = await context.new_page()
    captured_records: list[dict] = []

    captured_urls: list[str] = []

    async def handle_response(response):
        url = response.url
        captured_urls.append(url)
        if "gql_para_POST" not in url and "graphql" not in url.lower() and "/api/" not in url.lower():
            return
        try:
            body    = await response.json()
            records = _parse_gql_response(body)
            if records:
                captured_records.extend(records)
                logger.debug(f"[quora] +{len(records)} records via {url} (total {len(captured_records)})")
            elif dump_raw:
                logger.debug(f"[quora] 0 records from {url}")
        except Exception as exc:
            logger.debug(f"[quora] GraphQL parse error ({url}): {exc}")

    page.on("response", handle_response)

    try:
        await apply_stealth_to_page(page)
        url = _SEARCH_URL.format(query=quote(query))
        logger.info(f"[quora] query={query!r}")
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

        if await _is_auth_wall(page):
            await page.close()
            return QuoraSearchResponse(
                query=query,
                status="auth_required",
                error="Session expired — re-run quora_auth.py",
            )

        # Wait for initial results to render, then scroll to load more.
        # GQL (SearchResultsListQuery) fires lazily and may be blocked by
        # Turnstile — DOM extraction below is the fallback for that case.
        await _wait_for_search_results(page)
        await _scroll_for_results(page, captured_records, max_results)

        # If GQL yielded nothing after scrolling, give the page one more
        # moment then hand off to DOM extraction.
        if not captured_records:
            await page.wait_for_timeout(2_000)

    except Exception as exc:
        logger.error(f"[quora] Error for {query!r}: {exc}")
        try:
            await page.close()
        except Exception:
            pass
        return QuoraSearchResponse(query=query, status="error", error=str(exc))

    # DOM fallback — if GQL yielded nothing but the page rendered results,
    # extract directly from the visible DOM. Gives title + URL (no qid/counts)
    # which is sufficient for Cora classification.
    if not captured_records:
        dom_records = await _extract_from_dom(page, max_results, dump_raw=dump_raw)
        if dom_records:
            logger.info(f"[quora] GQL empty — using DOM fallback ({len(dom_records)} records)")
            captured_records.extend(dom_records)

    await page.close()

    if dump_raw:
        _dump_to_disk(query, captured_records)
        _dump_urls(query, captured_urls)

    raw_records_out = captured_records if dump_raw else None
    results = []
    for i, record in enumerate(captured_records[:max_results], start=1):
        result = _record_to_result(i, record, include_raw=dump_raw)
        if result:
            result.deterministic_score, result.deterministic_reasons = score_quora_result(result, query)
            results.append(result)

    return QuoraSearchResponse(
        query=query,
        results=results,
        status="ok" if results else "partial",
        raw_records=raw_records_out,
    )


async def _extract_from_dom(
    page: Page, max_results: int, dump_raw: bool = False
) -> list[dict]:
    """
    Extract question titles, URLs, and stats directly from the rendered DOM.
    Used when GQL interception yields nothing (Turnstile blocked the API call
    but the page still rendered results visually).
    Returns records compatible with _record_to_result().
    """
    try:
        payload = await page.evaluate("""(maxResults) => {
            const seen = new Set();
            const results = [];
            const cardHtmlSamples = [];

            // Primary: Quora's own puppeteer test class on question titles
            const titleEls = document.querySelectorAll('.puppeteer_test_question_title');
            for (const el of titleEls) {
                if (results.length >= maxResults) break;
                const title = (el.innerText || el.textContent || '').trim();
                if (!title) continue;

                // Walk up to find the question anchor (URL)
                let node = el;
                let anchor = null;
                for (let i = 0; i < 6 && node; i++) {
                    if (node.tagName === 'A' && node.href && node.href.includes('quora.com/')) {
                        anchor = node; break;
                    }
                    const a = node.querySelector && node.querySelector('a[href*="quora.com/"]');
                    if (a) { anchor = a; break; }
                    node = node.parentElement;
                }
                const url = anchor ? anchor.href : '';
                if (!url || seen.has(url)) continue;
                seen.add(url);

                // Walk up to find the full result card (stats live in a sibling subtree).
                // Keep climbing until a node's text contains "answer", up to 16 levels.
                let cardRoot = el;
                let statsText = '';
                for (let i = 0; i < 16 && cardRoot.parentElement; i++) {
                    cardRoot = cardRoot.parentElement;
                    const t = cardRoot.innerText || cardRoot.textContent || '';
                    if (/[0-9][\d,]*\s+[Aa]nswers?/.test(t)) {
                        statsText = t;
                        break;
                    }
                }

                // Parse "N answer(s)" from the card stats row text
                const answerMatch = statsText.match(/([0-9][\d,]*)\s+[Aa]nswers?/);
                const answerCount = answerMatch
                    ? parseInt(answerMatch[1].replace(/,/g, ''), 10) : 0;

                // Follower count: the Follow button uses an animated counter where
                // a qu-visibility--hidden span holds the stale value and its next
                // sibling (absolutely positioned, visible) holds the live count.
                let followerCount = 0;
                const hiddenCountSpan = cardRoot.querySelector
                    && cardRoot.querySelector(
                        'button[aria-pressed] .qu-visibility--hidden');
                if (hiddenCountSpan && hiddenCountSpan.nextElementSibling) {
                    const n = parseInt(
                        (hiddenCountSpan.nextElementSibling.innerText
                         || hiddenCountSpan.nextElementSibling.textContent
                         || '').trim(), 10);
                    if (!isNaN(n)) { followerCount = n; }
                }

                if (cardHtmlSamples.length < 2) {
                    cardHtmlSamples.push(cardRoot.outerHTML);
                }

                results.push({ title, url, answerCount, followerCount });
            }

            // Fallback: any quora.com link whose text looks like a question
            if (!results.length) {
                const anchors = document.querySelectorAll('a[href*="quora.com/"]');
                for (const a of anchors) {
                    if (results.length >= maxResults) break;
                    const href = a.href;
                    if (/(profile|topic|search|login|settings|about|blog)/.test(href)) continue;
                    const text = (a.innerText || a.textContent || '').trim();
                    if (text.length < 15 || seen.has(href)) continue;
                    seen.add(href);
                    results.push({ title: text, url: href, answerCount: 0, followerCount: 0 });
                }
            }

            return { records: results, cardHtmlSamples };
        }""", max_results)

        if dump_raw and payload.get("cardHtmlSamples"):
            _dump_card_html(payload["cardHtmlSamples"])

        return [
            {
                "title": r["title"],
                "url": r["url"],
                "answerCount": r.get("answerCount", 0),
                "followerCount": r.get("followerCount", 0),
            }
            for r in (payload.get("records") or [])
            if r.get("title") and r.get("url")
        ]
    except Exception as exc:
        logger.warning(f"[quora] DOM extraction failed: {exc}")
        return []


async def _is_auth_wall(page: Page) -> bool:
    return "/login" in page.url


async def _wait_for_search_results(page: Page) -> None:
    """
    Wait until Quora search results are present in the DOM.
    The search page triggers a Cloudflare Turnstile challenge that must
    resolve before SearchResultsListQuery fires. We wait for actual result
    elements rather than a fixed timeout so the scrape doesn't start before
    the GQL response arrives.
    """
    # Quora question cards in search results all carry this test class
    selectors = [
        ".puppeteer_test_question_title",
        "[class*='q-box'][class*='qu-pb']",   # outer result card pattern
        "a[href*='/'][class*='question']",
    ]
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, state="visible", timeout=15_000)
            logger.debug(f"[quora] search results visible via selector: {sel}")
            return
        except Exception:
            continue

    # None of the selectors matched — page may still be loading or blocked.
    # Fall back to a generous fixed wait so the GQL listener has time to fire.
    logger.warning("[quora] result selectors not found — waiting 10s as fallback")
    await page.wait_for_timeout(10_000)


async def _scroll_for_results(page: Page, records: list, max_results: int) -> None:
    for _ in range(10):
        if len(records) >= max_results:
            break
        prev = len(records)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(2_000)
        if len(records) == prev:
            break


# ---------------------------------------------------------------------------
# GraphQL response parsing
# ---------------------------------------------------------------------------

def _parse_gql_response(payload: dict) -> list[dict]:
    data       = payload.get("data", {})
    connection = (
        data.get("searchConnection")
        or data.get("search")
        or data.get("searchResults")
        or data.get("questionSearch")
    )
    if not connection:
        return []

    records = []
    for edge in connection.get("edges", []):
        node     = edge.get("node", {})
        question = node.get("question") or (
            node if node.get("__typename") in ("Question", None) else None
        )
        if question:
            records.append(question)
    return records


# ---------------------------------------------------------------------------
# Defensive field helpers
# ---------------------------------------------------------------------------

def _get_first(obj: dict, keys: list[str], default=None):
    """Return the first non-None value found under any of the given keys."""
    for key in keys:
        val = obj.get(key)
        if val is not None:
            return val
    return default


def _parse_unix_ts(val) -> Optional[datetime]:
    """Parse a Unix timestamp in either seconds or microseconds to UTC datetime."""
    if val is None:
        return None
    try:
        v = float(val)
        if v > 1e12:
            v = v / 1_000_000
        return datetime.fromtimestamp(v, tz=timezone.utc)
    except Exception:
        return None


def _parse_topics(record: dict) -> list[str]:
    topics_obj = record.get("topics") or record.get("questionTopics") or {}
    names: list[str] = []

    if isinstance(topics_obj, dict):
        for edge in topics_obj.get("edges", []):
            node = edge.get("node", {})
            name = (
                node.get("name")
                or node.get("translatedName")
                or node.get("title")
            )
            if name:
                names.append(str(name))
    elif isinstance(topics_obj, list):
        for t in topics_obj:
            if isinstance(t, dict):
                name = t.get("name") or t.get("translatedName") or t.get("title")
                if name:
                    names.append(str(name))
            elif isinstance(t, str):
                names.append(t)

    return names


def _parse_author_credentials(author: dict) -> Optional[str]:
    cred = (
        author.get("credential")
        or author.get("bestCredential")
        or author.get("credentials")
        or {}
    )
    if isinstance(cred, dict):
        return (
            cred.get("translatedCredential")
            or cred.get("credentialText")
            or cred.get("name")
            or cred.get("text")
        )
    if isinstance(cred, str):
        return cred
    return None


# ---------------------------------------------------------------------------
# Record → QuoraResult
# ---------------------------------------------------------------------------

def _record_to_result(
    position: int, record: dict, include_raw: bool = False
) -> Optional["QuoraResult"]:
    raw_title = _get_first(record, ["title", "qtext", "questionText"], "")
    title = flatten_quora_qtext(raw_title)
    if not title:
        return None

    # ── Identity ─────────────────────────────────────────────────────────────
    qid_raw = _get_first(record, ["qid", "id"])
    qid = int(qid_raw) if qid_raw is not None else None
    quora_id = str(qid_raw) if qid_raw is not None else None
    slug = record.get("slug")

    raw_url = _get_first(record, ["url", "canonicalUrl"], "")
    url = _normalize_quora_url(raw_url)
    canonical_url = url or None

    # ── Counts ────────────────────────────────────────────────────────────────
    answer_count   = _get_first(record, ["answerCount", "numAnswers"], 0) or 0
    decanonicalized_answer_count = record.get("decanonicalizedAnswerCount")
    follower_count = _get_first(record, ["followerCount", "numFollowers"], 0) or 0
    comment_count  = _get_first(record, ["numDisplayComments", "commentCount"])
    view_count     = _get_first(record, ["viewCount", "numViews", "views"])
    view_count     = int(view_count) if view_count is not None else None

    # ── Timestamps (Quora uses microseconds) ──────────────────────────────────
    created_ts     = _get_first(record, ["creationTime", "createdTime", "createdAt"])
    last_active_ts = _get_first(record, ["lastFollowTime", "lastActivityTime", "updatedAt"])

    # ── Content ───────────────────────────────────────────────────────────────
    raw_desc = _get_first(record, ["questionDescription", "description", "body"])
    if isinstance(raw_desc, dict):
        raw_desc = raw_desc.get("text") or raw_desc.get("content")
    description = flatten_quora_qtext(raw_desc) or None

    topics = _parse_topics(record)

    # ── Top answer ────────────────────────────────────────────────────────────
    top: dict = (
        record.get("topAnswer")
        or record.get("firstAnswer")
        or _first_edge(record.get("answers"))
        or {}
    )
    snippet = top.get("content") or top.get("text") or top.get("preview") or ""
    if isinstance(snippet, dict):
        snippet = snippet.get("text") or snippet.get("content") or ""
    snippet = flatten_quora_qtext(snippet)[:300].strip()

    author: dict = top.get("author") or {}
    author_name = _get_first(author, ["name", "displayName"], "")
    author_url  = _get_first(author, ["profileUrl", "url"], "")
    if author_url and not author_url.startswith("http"):
        author_url = f"https://www.quora.com{author_url}"

    upvotes     = _get_first(top, ["numUpvotes", "upvoteCount", "voteCount"])
    upvotes     = int(upvotes) if upvotes is not None else None
    credentials = _parse_author_credentials(author)

    # ── Flags ─────────────────────────────────────────────────────────────────
    is_locked   = bool(record.get("isLocked", False))
    is_deleted  = bool(record.get("isDeleted", False))
    is_sensitive = bool(record.get("isSensitive", False))
    viewer_should_show_write_answer = _nullable_bool(record.get("viewerShouldShowWriteAnswer"))
    viewer_cant_answer              = _nullable_bool(record.get("viewerCantAnswer"))
    is_user_limited_distro          = _nullable_bool(record.get("isUserLimitedDistro"))

    return QuoraResult(
        position                      = position,
        title                         = title,
        url                           = url,
        answer_count                  = answer_count,
        follower_count                = follower_count,
        top_answer_snippet            = snippet,
        top_answer_author             = str(author_name),
        top_answer_author_url         = author_url,
        qid                           = qid,
        quora_id                      = quora_id,
        slug                          = slug,
        canonical_url                 = canonical_url,
        decanonicalized_answer_count  = int(decanonicalized_answer_count) if decanonicalized_answer_count is not None else None,
        comment_count                 = int(comment_count) if comment_count is not None else None,
        view_count                    = view_count,
        created_time                  = _parse_unix_ts(created_ts),
        last_activity_time            = _parse_unix_ts(last_active_ts),
        question_description          = description,
        topics                        = topics,
        top_answer_upvotes            = upvotes,
        top_answer_author_credentials = credentials,
        is_locked                     = is_locked,
        is_deleted                    = is_deleted,
        is_sensitive                  = is_sensitive,
        viewer_should_show_write_answer = viewer_should_show_write_answer,
        viewer_cant_answer            = viewer_cant_answer,
        is_user_limited_distro        = is_user_limited_distro,
        raw_metadata                  = record if include_raw else None,
    )


def _normalize_quora_url(raw: str) -> str:
    if not raw:
        return ""
    raw = str(raw).strip()
    if raw.startswith("http"):
        return raw
    return f"https://www.quora.com{raw}"


def _nullable_bool(val) -> Optional[bool]:
    if val is None:
        return None
    return bool(val)


def _first_edge(connection: Optional[dict]) -> Optional[dict]:
    if not connection:
        return None
    edges = connection.get("edges", [])
    return edges[0].get("node") if edges else None


# ---------------------------------------------------------------------------
# Deterministic scoring
# ---------------------------------------------------------------------------

_FL_LOCATIONS   = ["florida", "tampa", "hillsborough", "pinellas", "orlando", "miami"]
_DISTRESS_KW    = ["foreclosure", "lis pendens", "mortgage default", "auction", "surplus funds"]
_LENDING_KW     = ["lender", "hard money", "private money", "bridge loan", "refinance"]
_REPAIR_KW      = ["repair", "restoration", "roof", "water damage", "fire damage", "renovation"]
_INTENT_PHRASES = ["how do i", "how can i", "what are my options", "what is the process", "how long"]
_COVID_KW       = ["covid", "pandemic", "market prediction"]
_TENANT_KW      = ["as a tenant", "tenant rights", "tenant's rights", "my landlord", "for renters"]
_GENERIC_KW     = ["what is foreclosure", "define foreclosure", "what is a mortgage", "what is a lien"]


def score_quora_result(result: "QuoraResult", keyword: str) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    t = result.title.lower()

    if any(loc in t for loc in _FL_LOCATIONS):
        score += 30
        reasons.append("Florida location")

    if any(kw in t for kw in _DISTRESS_KW):
        score += 25
        reasons.append("foreclosure intent")

    if any(kw in t for kw in _LENDING_KW):
        score += 20
        reasons.append("lending topic")

    if any(kw in t for kw in _REPAIR_KW):
        score += 15
        reasons.append("repair/restoration topic")

    if any(p in t for p in _INTENT_PHRASES):
        score += 10
        reasons.append("how-do-I phrasing")

    if (result.answer_count or 0) > 0:
        score += 10
        reasons.append("has answers")

    if (result.follower_count or 0) > 0:
        score += 5
        reasons.append("has followers")

    if (result.comment_count or 0) > 0:
        score += 5
        reasons.append("has comments")

    if any(kw in t for kw in _COVID_KW):
        score -= 25
        reasons.append("covid/generic prediction")

    if any(kw in t for kw in _TENANT_KW):
        score -= 25
        reasons.append("tenant-only question")

    if any(kw in t for kw in _GENERIC_KW):
        score -= 20
        reasons.append("too generic")

    if result.is_locked or result.is_deleted or result.is_sensitive or result.viewer_cant_answer:
        score -= 50
        reasons.append("question blocked/locked/deleted/sensitive")

    return max(0, min(100, score)), reasons


# ---------------------------------------------------------------------------
# Raw dump
# ---------------------------------------------------------------------------

def _result_to_dump_dict(r: QuoraResult) -> dict:
    return {
        "position":                     r.position,
        "qid":                          r.qid,
        "title":                        r.title,
        "url":                          r.url,
        "slug":                         r.slug,
        "answer_count":                 r.answer_count,
        "decanonicalized_answer_count": r.decanonicalized_answer_count,
        "follower_count":               r.follower_count,
        "comment_count":                r.comment_count,
        "view_count":                   r.view_count,
        "created_time":                 r.created_time.isoformat() if r.created_time else None,
        "last_activity_time":           r.last_activity_time.isoformat() if r.last_activity_time else None,
        "is_locked":                    r.is_locked,
        "is_deleted":                   r.is_deleted,
        "is_sensitive":                 r.is_sensitive,
        "viewer_should_show_write_answer": r.viewer_should_show_write_answer,
        "viewer_cant_answer":           r.viewer_cant_answer,
        "is_user_limited_distro":       r.is_user_limited_distro,
        "question_description":         r.question_description,
        "topics":                       r.topics,
        "top_answer_snippet":           r.top_answer_snippet,
        "top_answer_upvotes":           r.top_answer_upvotes,
        "top_answer_author":            r.top_answer_author,
        "top_answer_author_credentials": r.top_answer_author_credentials,
        "deterministic_score":          r.deterministic_score,
        "deterministic_reasons":        r.deterministic_reasons,
        "cora_classification":          r.cora_classification,
        "cora_answer_draft":            r.cora_answer_draft,
    }


def _dump_urls(query: str, urls: list[str]) -> None:
    _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = query[:40].replace(" ", "_").replace("/", "-")
    path = _DEBUG_DIR / f"response_urls_{slug}_{ts}.txt"
    path.write_text("\n".join(urls))
    logger.info(f"[quora] url dump → {path} ({len(urls)} responses captured)")


def _dump_card_html(samples: list[str]) -> None:
    """Save raw card HTML from DOM extraction for selector inspection."""
    _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = _DEBUG_DIR / f"dom_card_sample_{ts}.html"
    separator = "\n\n<!-- ===== CARD BREAK ===== -->\n\n"
    path.write_text(separator.join(samples), encoding="utf-8")
    logger.info(f"[quora] card HTML dump → {path} ({len(samples)} cards)")


def _dump_to_disk(query: str, raw_records: list[dict]) -> None:
    _DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = query[:40].replace(" ", "_").replace("/", "-")

    raw_path = _DEBUG_DIR / f"search_results_{slug}_{ts}.json"
    raw_path.write_text(json.dumps(raw_records, indent=2, default=str))
    logger.info(f"[quora] raw dump → {raw_path} ({len(raw_records)} records)")

    parsed = []
    for i, rec in enumerate(raw_records, start=1):
        r = _record_to_result(i, rec, include_raw=False)
        if r:
            r.deterministic_score, r.deterministic_reasons = score_quora_result(r, query)
            parsed.append(_result_to_dump_dict(r))

    parsed_path = _DEBUG_DIR / f"parsed_results_{slug}_{ts}.json"
    parsed_path.write_text(json.dumps(parsed, indent=2, default=str))
    logger.info(f"[quora] parsed dump → {parsed_path}")


# ---------------------------------------------------------------------------
# Session loader
# ---------------------------------------------------------------------------

def _load_session() -> dict:
    if not SESSION_FILE.exists():
        raise FileNotFoundError(
            f"No session file at {SESSION_FILE}. "
            "Run: python -m src.scrappers.quora.quora_auth"
        )
    return json.loads(SESSION_FILE.read_text())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument("queries", nargs="*", default=["foreclosures in Tampa FL"])
    ap.add_argument("--max-results", type=int, default=20)
    ap.add_argument("--dump-raw", action="store_true",
                    help="Save raw GQL + parsed results to data/debug/quora/")
    args = ap.parse_args()

    responses = asyncio.run(
        scrape_quora(args.queries, max_results=args.max_results, dump_raw=args.dump_raw)
    )

    for resp in responses:
        print(f"\n{'='*60}")
        print(f"Query : {resp.query!r}  [{resp.status}]")
        if resp.error:
            print(f"Error : {resp.error}")
        for r in resp.results:
            print(f"\n  [{r.position}] {r.title}")
            print(f"       {r.url}")
            print(f"       answers={r.answer_count}  followers={r.follower_count}"
                  f"  views={r.view_count}  score={r.deterministic_score}")
            if r.topics:
                print(f"       topics: {', '.join(r.topics)}")
            if r.created_time:
                print(f"       created: {r.created_time.date()}")
            if r.top_answer_author:
                cred = f" ({r.top_answer_author_credentials})" if r.top_answer_author_credentials else ""
                print(f"       Top answer by: {r.top_answer_author}{cred}"
                      f"  upvotes={r.top_answer_upvotes}")
            if r.top_answer_snippet:
                print(f"       {r.top_answer_snippet[:160]}")
