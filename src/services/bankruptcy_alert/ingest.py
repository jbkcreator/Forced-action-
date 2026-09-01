"""
Stage 12 — Bankruptcy filing ingestion from CourtListener.

Fetches bankruptcy dockets, filters by configured jurisdiction + chapter, and
upserts into `bankruptcy_filings` with case_number dedup (ON CONFLICT DO NOTHING).

Rate-limit friendly:
  - Paginates via the API `next` cursor up to COURTLISTENER_MAX_PAGES.
  - Sleeps COURTLISTENER_PAGE_DELAY_SECONDS between pages.
  - Reuses requests_get_with_retry (backoff on 429/5xx).

Returns an IngestResult with counts so the dispatch task can decide whether to
alert ops on failure.

All DB writes use raw SQL via sa_text (repo convention).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from sqlalchemy import text as sa_text
from sqlalchemy.orm import Session

from config.bankruptcy_alert_config import (
    COURTLISTENER_MAX_PAGES,
    COURTLISTENER_PAGE_DELAY_SECONDS,
    COURTLISTENER_PAGE_SIZE,
    DEFAULT_LOOKBACK_DAYS,
    JURISDICTIONS,
    RELEVANT_CHAPTERS,
    jurisdiction_for_docket,
)
from config.constants import (
    API_USER_AGENT,
    COURTLISTENER_API_URL,
    REQUEST_TIMEOUT_DEFAULT,
)
from config.settings import get_settings
from src.utils.http_helpers import requests_get_with_retry

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    fetched: int = 0
    matched: int = 0          # passed jurisdiction + chapter filter
    inserted: int = 0         # new rows (deduped)
    duplicates: int = 0       # already in DB
    pages_read: int = 0
    success: bool = True
    error: Optional[str] = None
    outcome: Optional[str] = None  # config.scraper_outcomes.ScraperOutcome value, set on failure
    new_filing_ids: list = field(default_factory=list)


def _auth_headers() -> dict:
    settings = get_settings()
    key = settings.court_listener_api_key.get_secret_value()
    return {
        "Authorization": f"Token {key}",
        "User-Agent": API_USER_AGENT,
    }


def _normalize_chapter(raw_chapter) -> Optional[str]:
    """CourtListener returns chapter as int/str/None. Normalize to '7'/'11'/'13' or None."""
    if raw_chapter is None or raw_chapter == "":
        return None
    return str(raw_chapter).strip()


def _extract_filing(docket: dict) -> Optional[dict]:
    """Map a raw CourtListener docket to a bankruptcy_filings row, or None if it
    isn't a bankruptcy docket / doesn't map to a configured jurisdiction."""
    if docket.get("federal_dn_case_type") != "bk":
        return None

    court = docket.get("court", "") or ""
    docket_number = docket.get("docket_number", "") or ""
    jurisdiction = jurisdiction_for_docket(court, docket_number)
    if jurisdiction is None:
        return None

    raw_name = docket.get("case_name", "") or ""
    filer = raw_name.replace("In re: ", "").strip() or None
    chapter = _normalize_chapter(docket.get("chapter"))

    return {
        "case_number": docket_number,
        "chapter": chapter,
        "court": court,
        "jurisdiction": jurisdiction,
        "filer": filer,
        # Trustee isn't on the base docket object; populated when present.
        "trustee": (docket.get("trustee_str") or None),
        "date_filed": docket.get("date_filed") or None,
        "docket_id": str(docket.get("id")) if docket.get("id") is not None else None,
        "nature_of_suit": docket.get("nature_of_suit") or None,
        "raw": {
            "case_name": raw_name,
            "assigned_to_str": docket.get("assigned_to_str"),
            "date_terminated": docket.get("date_terminated"),
        },
    }


def _fetch_dockets(
    court_code: str,
    start_date: str,
    *,
    max_pages: int,
) -> tuple[list[dict], int]:
    """Page through CourtListener dockets for a court since start_date.
    Returns (dockets, pages_read). Raises on hard HTTP failure."""
    headers = _auth_headers()
    params = {
        "court": court_code,
        "date_filed__gte": start_date,
        "page_size": COURTLISTENER_PAGE_SIZE,
    }

    url: Optional[str] = COURTLISTENER_API_URL
    use_params: Optional[dict] = params
    dockets: list[dict] = []
    pages = 0

    while url and pages < max_pages:
        resp = requests_get_with_retry(
            url,
            headers=headers,
            params=use_params,
            timeout=REQUEST_TIMEOUT_DEFAULT,
        )
        data = resp.json()
        dockets.extend(data.get("results", []))
        pages += 1

        # CourtListener returns an absolute `next` URL (params already embedded).
        url = data.get("next")
        use_params = None
        if url:
            time.sleep(COURTLISTENER_PAGE_DELAY_SECONDS)

    return dockets, pages


def _upsert_filing(db: Session, filing: dict) -> Optional[int]:
    """Insert a filing row; return its id if newly inserted, None if duplicate."""
    import json
    row = db.execute(sa_text("""
        INSERT INTO bankruptcy_filings
            (case_number, chapter, court, jurisdiction, filer, trustee,
             date_filed, docket_id, nature_of_suit, raw, created_at)
        VALUES
            (:case_number, :chapter, :court, :jurisdiction, :filer, :trustee,
             :date_filed, :docket_id, :nature_of_suit, CAST(:raw AS jsonb), NOW())
        ON CONFLICT (case_number) DO NOTHING
        RETURNING id
    """), {
        **{k: filing[k] for k in (
            "case_number", "chapter", "court", "jurisdiction", "filer",
            "trustee", "date_filed", "docket_id", "nature_of_suit",
        )},
        "raw": json.dumps(filing.get("raw") or {}),
    }).first()
    return int(row.id) if row else None


def ingest_filings(
    db: Session,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    chapters: tuple = RELEVANT_CHAPTERS,
    max_pages: int = COURTLISTENER_MAX_PAGES,
) -> IngestResult:
    """Fetch + filter + upsert bankruptcy filings across all configured jurisdictions.

    Idempotent: re-running over the same window inserts nothing new (case_number
    dedup). Safe to retry.
    """
    result = IngestResult()
    start_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    allowed_chapters = set(chapters)

    # One fetch per distinct court code (jurisdictions share courts via division prefix).
    court_codes = {cfg["court_code"] for cfg in JURISDICTIONS.values()}

    try:
        for court_code in court_codes:
            dockets, pages = _fetch_dockets(court_code, start_date, max_pages=max_pages)
            result.fetched += len(dockets)
            result.pages_read += pages

            for docket in dockets:
                filing = _extract_filing(docket)
                if filing is None:
                    continue
                # Chapter filter — keep NULL-chapter filings (can't classify) so we
                # don't silently drop relevant cases that lack a chapter field.
                if filing["chapter"] is not None and filing["chapter"] not in allowed_chapters:
                    continue
                result.matched += 1

                new_id = _upsert_filing(db, filing)
                if new_id is not None:
                    result.inserted += 1
                    result.new_filing_ids.append(new_id)
                else:
                    result.duplicates += 1

        db.flush()
        logger.info(
            "[bk-ingest] fetched=%d matched=%d inserted=%d duplicates=%d pages=%d",
            result.fetched, result.matched, result.inserted,
            result.duplicates, result.pages_read,
        )
    except (requests.HTTPError, requests.Timeout, requests.ConnectionError, requests.RequestException) as exc:
        from src.utils.scraper_outcome_classifier import classify_exception
        result.success = False
        result.error = f"CourtListener API error: {exc}"
        result.outcome = classify_exception(exc)
        logger.error("[bk-ingest] %s", result.error, exc_info=True)
    except Exception as exc:  # noqa: BLE001 — ingest must surface, not crash the cron
        from src.utils.scraper_outcome_classifier import classify_exception
        result.success = False
        result.error = f"Unexpected ingest error: {exc}"
        result.outcome = classify_exception(exc)
        logger.error("[bk-ingest] %s", result.error, exc_info=True)

    return result
