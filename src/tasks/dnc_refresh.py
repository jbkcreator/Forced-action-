"""
Monthly DNC re-scrub task — Tracerfy /dnc/scrub/ batch endpoint.

Keeps stored DNC data fresh within the FTC's 31-day safe harbor window.
Supported targets:
  owners      — Owner.phone_1 (leads). Staleness tracked via phone_metadata.
  dbpr        — DBPRContact.phone (licensed contractors). All non-null phones monthly.
  subscribers — Subscriber.phone. All non-null phones monthly.

Cost: 1 Tracerfy credit per phone checked ($0.02).

Recommended schedule: 1st of each month, 02:00 UTC (before any campaign).
Crontab: 0 2 1 * * python -m src.tasks.dnc_refresh --targets all

Usage:
  python -m src.tasks.dnc_refresh --dry-run
  python -m src.tasks.dnc_refresh --targets owners --limit 500
  python -m src.tasks.dnc_refresh --targets dbpr,subscribers
  python -m src.tasks.dnc_refresh --targets all
  python -m src.tasks.dnc_refresh --days 45
"""

import csv
import io
import json
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from sqlalchemy import text as sa_text

from config.settings import get_settings
from src.core.database import get_db_context
from src.core.models import Owner, SmsOptOut
from src.services.enrichment_log import log_usage
from src.services.phone_utils import normalize as normalize_phone
from src.utils.logger import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

_TRACERFY_BASE  = "https://tracerfy.com/v1/api"
_SCRUB_ENDPOINT = f"{_TRACERFY_BASE}/dnc/scrub/"
_QUEUE_ENDPOINT = f"{_TRACERFY_BASE}/dnc/queue/"
_BATCH_SIZE     = 1000
_POLL_INTERVAL  = 5
DNC_OPT_OUT_SOURCE = "tracerfy_dnc_refresh"


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }


# ---------------------------------------------------------------------------
# API helpers (shared across all targets)
# ---------------------------------------------------------------------------

def _submit_scrub_batch(phones: list[str], api_key: str) -> str:
    resp = requests.post(
        _SCRUB_ENDPOINT,
        headers=_headers(api_key),
        json={"phones": phones},
        timeout=30,
    )
    if resp.status_code in (401, 403):
        raise RuntimeError(f"Tracerfy DNC: invalid API key ({resp.status_code})")
    if resp.status_code == 429:
        raise RuntimeError("Tracerfy DNC: rate limited (429)")
    if not resp.ok:
        raise RuntimeError(f"Tracerfy DNC HTTP {resp.status_code}: {resp.text[:400]}")
    data = resp.json()
    queue_id = data.get("dnc_queue_id") or data.get("queue_id") or data.get("id")
    if not queue_id:
        raise RuntimeError(f"Tracerfy DNC: no queue_id in response: {data}")
    return str(queue_id)


def _poll_queue(queue_id: str, api_key: str) -> list[dict]:
    """Poll /dnc/queue/:id until pending=False, return CSV rows."""
    url = f"{_QUEUE_ENDPOINT}{queue_id}"
    for attempt in range(120):
        resp = requests.get(url, headers=_headers(api_key), timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Tracerfy DNC queue poll HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if not data.get("pending", True):
            download_url = data.get("download_url") or ""
            if not download_url:
                logger.warning("[DNCRefresh] queue=%s completed but download_url empty", queue_id)
                return []
            csv_resp = requests.get(download_url, timeout=60)
            if not csv_resp.ok:
                raise RuntimeError(f"Tracerfy DNC CSV download HTTP {csv_resp.status_code}")
            return list(csv.DictReader(io.StringIO(csv_resp.text)))
        logger.info("[DNCRefresh] queue=%s pending attempt=%d", queue_id, attempt + 1)
        time.sleep(_POLL_INTERVAL)
    raise RuntimeError(f"Tracerfy DNC queue {queue_id} did not complete within 10 minutes")


# ---------------------------------------------------------------------------
# Phone collection — one function per target
# ---------------------------------------------------------------------------

def _collect_owner_phones(
    session,
    county_id: str,
    cutoff: datetime,
    source: Optional[str] = None,
) -> list[tuple]:
    """Owners with phone_1 stale or never DNC-checked.

    When source is provided, only owner phones represented by a phone-bearing
    enriched_contacts row with that source are eligible.
    """
    source_filter = ""
    params = {"county_id": county_id, "cutoff": cutoff}
    if source:
        source_filter = """
          AND EXISTS (
              SELECT 1
              FROM enriched_contacts ec
              WHERE ec.property_id = o.property_id
                AND ec.source = :source
                AND o.phone_1 IN (ec.mobile_phone, ec.landline)
          )
        """
        params["source"] = source

    sql = sa_text(f"""
        SELECT o.id, o.phone_1
        FROM owners o
        WHERE o.county_id = :county_id
          AND o.phone_1 IS NOT NULL
          AND length(trim(o.phone_1)) > 0
          AND NOT EXISTS (
              SELECT 1
              FROM sms_opt_outs soo
              WHERE soo.phone = o.phone_1
          )
          {source_filter}
          AND (
              o.phone_metadata IS NULL
              OR o.phone_metadata->'phone_1'->>'dnc_checked_at' IS NULL
              OR (o.phone_metadata->'phone_1'->>'dnc_checked_at')::timestamptz < :cutoff
          )
        ORDER BY o.id
    """)
    rows = session.execute(sql, params).fetchall()
    return [(row[0], row[1]) for row in rows]


def _collect_known_suppressed_phones(session) -> set[str]:
    rows = session.execute(sa_text("SELECT phone FROM sms_opt_outs")).fetchall()
    return {row[0] for row in rows if row[0]}


def _collect_fresh_clean_phones(session, cutoff: datetime) -> set[str]:
    rows = session.execute(
        sa_text("""
            SELECT phone
            FROM dnc_phone_checks
            WHERE checked_at >= :cutoff
              AND national_dnc = false
              AND litigator = false
        """),
        {"cutoff": cutoff},
    ).fetchall()
    return {row[0] for row in rows if row[0]}


def _collect_dbpr_phones(session, county_id: str) -> list[tuple]:
    """All DBPR contacts with a non-null phone (run monthly — no per-phone staleness tracking)."""
    from sqlalchemy import text as sa_text
    sql = sa_text("""
        SELECT id, phone
        FROM dbpr_contacts
        WHERE county_id = :county_id
          AND phone IS NOT NULL
          AND length(trim(phone)) > 0
        ORDER BY id
    """)
    rows = session.execute(sql, {"county_id": county_id}).fetchall()
    return [(row[0], row[1]) for row in rows]


def _collect_subscriber_phones(session, county_id: str) -> list[tuple]:
    """All subscribers with a non-null phone (run monthly — no per-phone staleness tracking)."""
    from sqlalchemy import text as sa_text
    # Subscribers are not county-scoped — county_id filter is skipped.
    sql = sa_text("""
        SELECT id, phone
        FROM subscribers
        WHERE phone IS NOT NULL
          AND length(trim(phone)) > 0
        ORDER BY id
    """)
    rows = session.execute(sql, {}).fetchall()
    return [(row[0], row[1]) for row in rows]


def _upsert_dnc_phone_check(session, phone: str, national_dnc: bool, litigator: bool, row: dict) -> None:
    session.execute(
        sa_text("""
            INSERT INTO dnc_phone_checks
                (phone, national_dnc, litigator, checked_at, source, raw_result)
            VALUES
                (:phone, :national_dnc, :litigator, :checked_at, :source, CAST(:raw_result AS jsonb))
            ON CONFLICT (phone) DO UPDATE SET
                national_dnc = EXCLUDED.national_dnc,
                litigator = EXCLUDED.litigator,
                checked_at = EXCLUDED.checked_at,
                source = EXCLUDED.source,
                raw_result = EXCLUDED.raw_result
        """),
        {
            "phone": phone,
            "national_dnc": national_dnc,
            "litigator": litigator,
            "checked_at": datetime.now(timezone.utc),
            "source": DNC_OPT_OUT_SOURCE,
            "raw_result": json.dumps(row),
        },
    )


# ---------------------------------------------------------------------------
# Persist helpers — per-target post-scrub updates
# ---------------------------------------------------------------------------

def _update_owner_after_scrub(session, owner_id: int, phone: str,
                               national_dnc: bool, litigator: bool) -> None:
    """Mirror explicit Tracerfy DNC result into Owner.phone_metadata."""
    owner = session.get(Owner, owner_id)
    if not owner:
        logger.warning("[DNCRefresh/owners] Owner id=%d not found during metadata update", owner_id)
        return
    checked_at = datetime.now(timezone.utc).isoformat()
    meta = dict(owner.phone_metadata or {})
    phone_1_meta = dict(meta.get("phone_1") or {})
    phone_1_meta["dnc"]            = national_dnc
    phone_1_meta["litigator"]      = litigator
    phone_1_meta["dnc_checked_at"] = checked_at
    phone_1_meta["dnc_source"]     = DNC_OPT_OUT_SOURCE
    meta["phone_1"] = phone_1_meta
    owner.phone_metadata = meta


# DBPR and Subscriber have no phone_metadata JSONB — no per-record update needed.
# DNC suppression via sms_opt_outs (handled in the shared loop) is sufficient.


def _reenqueue_dnc_blocked_subscriber_calls(session) -> int:
    """
    Re-dispatch new_lead_signup for subscribers whose signup voice call was
    previously blocked by compliance:dnc_check_required and who now have a
    fresh clean DNC row. Called once after the subscribers scrub completes.

    Guards:
    - phone must be clean in dnc_phone_checks (national_dnc=false, litigator=false)
    - phone must not be in sms_opt_outs
    - subscriber must have no completed new_lead_voice_call decision
    """
    from src.agents.events.ingestion import publish_lifecycle_event

    rows = session.execute(sa_text("""
        SELECT DISTINCT ad.subscriber_id, s.phone
        FROM agent_decisions ad
        JOIN subscribers s ON s.id = ad.subscriber_id
        JOIN dnc_phone_checks dpc ON dpc.phone = s.phone
        WHERE ad.graph_name = 'new_lead_voice_call'
          AND ad.terminal_status = 'aborted'
          AND ad.summary->>'failure_reason' = 'compliance:dnc_check_required'
          AND dpc.national_dnc = false
          AND dpc.litigator = false
          AND NOT EXISTS (
              SELECT 1 FROM agent_decisions ad2
              WHERE ad2.subscriber_id = ad.subscriber_id
                AND ad2.graph_name = 'new_lead_voice_call'
                AND ad2.terminal_status = 'completed'
          )
          AND NOT EXISTS (
              SELECT 1 FROM sms_opt_outs soo
              WHERE soo.phone = s.phone
          )
    """)).fetchall()

    count = 0
    for subscriber_id, phone in rows:
        try:
            publish_lifecycle_event({
                "event_type": "new_lead_signup",
                "subscriber_id": subscriber_id,
                "source": "dnc_refresh_retry",
            })
            count += 1
            logger.info(
                "[DNCRefresh/subscribers] Re-enqueued new_lead_signup subscriber_id=%d phone=%s",
                subscriber_id, phone,
            )
        except Exception as e:
            logger.warning(
                "[DNCRefresh/subscribers] Re-enqueue failed subscriber_id=%d: %s",
                subscriber_id, e,
            )
    return count


# ---------------------------------------------------------------------------
# Shared scrub pipeline
# ---------------------------------------------------------------------------

def _run_scrub(
    label: str,
    rows: list[tuple],       # (entity_id, raw_phone)
    api_key: str,
    stats: dict,
    update_fn=None,          # optional callback(session, entity_id, phone, dnc, litigator)
    suppressed_phones: Optional[set[str]] = None,
    fresh_clean_phones: Optional[set[str]] = None,
) -> None:
    """
    Shared DNC scrub pipeline for any target.

    All explicit Tracerfy results are written to dnc_phone_checks. Positive
    DNC/litigator results are also written to sms_opt_outs, which remains the
    universal enforcement table. Phones already suppressed are never rechecked;
    fresh clean phones wait for the configured cool-off window before recheck.
    """
    suppressed_phones = suppressed_phones or set()
    fresh_clean_phones = fresh_clean_phones or set()
    stats.setdefault("normalize_skipped", 0)
    stats.setdefault("already_suppressed", 0)
    stats.setdefault("fresh_clean_skipped", 0)
    stats.setdefault("unmatched_csv", 0)
    stats.setdefault("tracerfy_unknown", 0)

    phone_to_ids: dict[str, list[int]] = {}
    for entity_id, raw_phone in rows:
        normalized = normalize_phone(raw_phone)
        if not normalized:
            stats["normalize_skipped"] += 1
            logger.warning(
                "[DNCRefresh/%s] Skipped id=%d: phone %r failed normalization",
                label, entity_id, raw_phone,
            )
            continue
        if normalized in suppressed_phones:
            stats["already_suppressed"] += 1
            logger.info(
                "[DNCRefresh/%s] Skipped id=%d phone=%s: already suppressed",
                label, entity_id, normalized,
            )
            continue
        if normalized in fresh_clean_phones:
            stats["fresh_clean_skipped"] += 1
            logger.info(
                "[DNCRefresh/%s] Skipped id=%d phone=%s: DNC clean check still fresh",
                label, entity_id, normalized,
            )
            continue
        if normalized in phone_to_ids:
            logger.warning(
                "[DNCRefresh/%s] Duplicate phone %s: id=%d shares normalized phone with id(s)=%s",
                label, normalized, entity_id, phone_to_ids[normalized],
            )
        phone_to_ids.setdefault(normalized, []).append(entity_id)

    phones = list(phone_to_ids.keys())
    if not phones:
        logger.info("[DNCRefresh/%s] No phones need DNC submit after local gates", label)
        return

    for batch_start in range(0, len(phones), _BATCH_SIZE):
        batch = phones[batch_start: batch_start + _BATCH_SIZE]
        batch_set = set(batch)
        batch_num = batch_start // _BATCH_SIZE + 1
        logger.info("[DNCRefresh/%s] Batch %d: submitting %d phones...", label, batch_num, len(batch))

        try:
            queue_id = _submit_scrub_batch(batch, api_key)
            logger.info("[DNCRefresh/%s] Batch %d queued; queue_id=%s", label, batch_num, queue_id)
            results  = _poll_queue(queue_id, api_key)
        except Exception as e:
            logger.error(
                "[DNCRefresh/%s] Batch %d failed: %s; %d phones will retry next run",
                label, batch_num, e, len(batch),
            )
            logger.debug(traceback.format_exc())
            stats["failed"] += len(batch)
            continue

        if batch_num == 1 and results:
            logger.info("[DNCRefresh/%s] CSV sample: %s", label, results[0])

        returned_phones: set[str] = set()

        with get_db_context() as session:
            for row in results:
                raw_phone    = row.get("phone") or ""
                national_dnc = (row.get("national_dnc") or "").strip().upper() in ("Y", "YES", "TRUE", "1")
                litigator    = (row.get("litigator") or "").strip().upper() in ("Y", "YES", "TRUE", "1")

                phone = normalize_phone(str(raw_phone).strip())
                if not phone:
                    stats["unmatched_csv"] += 1
                    logger.warning("[DNCRefresh/%s] CSV row has unnormalizable phone: %r", label, raw_phone)
                    continue

                returned_phones.add(phone)
                entity_ids = phone_to_ids.get(phone)
                if not entity_ids:
                    stats["unmatched_csv"] += 1
                    logger.warning("[DNCRefresh/%s] CSV phone %s was not in submitted batch", label, phone)
                    continue

                _upsert_dnc_phone_check(session, phone, national_dnc, litigator, row)

                if national_dnc:
                    stats["dnc_hits"] += 1
                if litigator:
                    stats["litigator_hits"] += 1

                if update_fn:
                    for entity_id in entity_ids:
                        try:
                            update_fn(session, entity_id, phone, national_dnc, litigator)
                        except Exception as e:
                            logger.warning(
                                "[DNCRefresh/%s] metadata update failed id=%d phone=%s: %s",
                                label, entity_id, phone, e,
                            )

                if national_dnc or litigator:
                    already = session.execute(
                        sa_text("SELECT 1 FROM sms_opt_outs WHERE phone = :phone LIMIT 1"),
                        {"phone": phone},
                    ).fetchone()
                    if not already:
                        session.add(SmsOptOut(
                            phone=phone,
                            keyword_used="DNC",
                            source=DNC_OPT_OUT_SOURCE,
                            opted_out_at=datetime.now(timezone.utc),
                        ))
                        stats["suppressed"] += 1
                        logger.info(
                            "[DNCRefresh/%s] Suppressed phone=%s national_dnc=%s litigator=%s ids=%s",
                            label, phone, national_dnc, litigator, entity_ids,
                        )

                log_usage(
                    db=session,
                    vendor="tracerfy",
                    purpose="dnc_refresh",
                    success=True,
                    cost_cents=1,
                )

            session.commit()

        missing_from_tracerfy = batch_set - returned_phones
        if missing_from_tracerfy:
            stats["tracerfy_unknown"] += len(missing_from_tracerfy)
            for phone in sorted(missing_from_tracerfy):
                logger.warning(
                    "[DNCRefresh/%s] Tracerfy returned no data for phone=%s; "
                    "DNC status unknown, metadata unchanged, will retry next run",
                    label, phone,
                )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_dnc_refresh(
    county_id: str = "hillsborough",
    days: Optional[int] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
    targets: Optional[list[str]] = None,
    source: Optional[str] = None,
) -> dict:
    """
    Run DNC re-scrub for the specified targets.

    targets: list of "owners", "dbpr", "subscribers", or ["all"] for everything.
    Default is all three.

    source: optional enriched_contacts.source filter for owners only.

    Owners use per-phone staleness tracking (phone_metadata.dnc_checked_at).
    DBPR and Subscribers run all non-null phones monthly (cron schedule = 31-day window).
    """
    settings = get_settings()

    if not settings.tracerfy_api_key:
        logger.warning("[DNCRefresh] TRACERFY_API_KEY not set — skipped")
        return {"skipped": True, "reason": "TRACERFY_API_KEY not configured"}

    api_key  = settings.tracerfy_api_key.get_secret_value()
    recheck  = days if days is not None else settings.dnc_recheck_days
    cutoff   = datetime.now(timezone.utc) - timedelta(days=recheck)

    active = set(targets or ["all"])
    if "all" in active:
        active = {"owners", "dbpr", "subscribers"}

    stats = {
        "total":                 0,
        "dnc_hits":              0,
        "litigator_hits":        0,
        "suppressed":            0,
        "failed":                0,
        "normalize_skipped":     0,
        "already_suppressed":    0,
        "fresh_clean_skipped":   0,
        "unmatched_csv":         0,
        "tracerfy_unknown":      0,
        "requeued_signup_calls": 0,
        "skipped":               False,
    }

    logger.info(
        "[DNCRefresh] Starting — county=%s targets=%s recheck=%d days",
        county_id, sorted(active), recheck,
    )

    # ── Collect phones per target ─────────────────────────────────────────
    target_rows: dict[str, list[tuple]] = {}

    with get_db_context() as session:
        suppressed_phones = _collect_known_suppressed_phones(session)
        fresh_clean_phones = _collect_fresh_clean_phones(session, cutoff)
        if "owners" in active:
            target_rows["owners"] = _collect_owner_phones(session, county_id, cutoff, source=source)
        if "dbpr" in active:
            target_rows["dbpr"] = _collect_dbpr_phones(session, county_id)
        if "subscribers" in active:
            target_rows["subscribers"] = _collect_subscriber_phones(session, county_id)

    for target, rows in target_rows.items():
        if limit is not None:
            rows = rows[:limit]
        stats["total"] += len(rows)
        logger.info("[DNCRefresh/%s] %d phones queued", target, len(rows))

    if stats["total"] == 0:
        logger.info("[DNCRefresh] No phones to re-scrub across all targets.")
        return stats

    if dry_run:
        for target, rows in target_rows.items():
            count = min(len(rows), limit) if limit else len(rows)
            logger.info("[DNCRefresh DRY RUN] %s: would scrub %d phones", target, count)
        logger.info("[DNCRefresh DRY RUN] Total: %d phones. No API call made.", stats["total"])
        return stats

    # ── Run scrub per target ──────────────────────────────────────────────
    if "owners" in active and target_rows.get("owners"):
        _run_scrub(
            label="owners",
            rows=target_rows["owners"][:limit] if limit else target_rows["owners"],
            api_key=api_key,
            stats=stats,
            update_fn=_update_owner_after_scrub,
            suppressed_phones=suppressed_phones,
            fresh_clean_phones=fresh_clean_phones,
        )

    if "dbpr" in active and target_rows.get("dbpr"):
        _run_scrub(
            label="dbpr",
            rows=target_rows["dbpr"][:limit] if limit else target_rows["dbpr"],
            api_key=api_key,
            stats=stats,
            update_fn=None,
            suppressed_phones=suppressed_phones,
            fresh_clean_phones=fresh_clean_phones,
        )

    if "subscribers" in active and target_rows.get("subscribers"):
        _run_scrub(
            label="subscribers",
            rows=target_rows["subscribers"][:limit] if limit else target_rows["subscribers"],
            api_key=api_key,
            stats=stats,
            update_fn=None,
            suppressed_phones=suppressed_phones,
            fresh_clean_phones=fresh_clean_phones,
        )
        with get_db_context() as session:
            stats["requeued_signup_calls"] = _reenqueue_dnc_blocked_subscriber_calls(session)
        if stats["requeued_signup_calls"]:
            logger.info(
                "[DNCRefresh/subscribers] Re-enqueued %d DNC-unblocked signup calls",
                stats["requeued_signup_calls"],
            )

    logger.info("=" * 60)
    logger.info("DNC REFRESH COMPLETE")
    logger.info("  Targets         : %s", sorted(active))
    logger.info("  Total phones    : %d", stats["total"])
    logger.info("  National DNC    : %d", stats["dnc_hits"])
    logger.info("  Litigators      : %d", stats["litigator_hits"])
    logger.info("  Newly suppressed: %d", stats["suppressed"])
    logger.info("  Failed batches  : %d", stats["failed"])
    logger.info("  Normalize skipped : %d", stats["normalize_skipped"])
    logger.info("  Already suppressed: %d", stats["already_suppressed"])
    logger.info("  Fresh clean skip  : %d", stats["fresh_clean_skipped"])
    logger.info("  Tracerfy unknown  : %d", stats["tracerfy_unknown"])
    logger.info("  CSV unmatched     : %d", stats["unmatched_csv"])
    logger.info("  Requeued signups  : %d", stats["requeued_signup_calls"])
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Monthly DNC re-scrub via Tracerfy")
    parser.add_argument("--county-id", dest="county_id", default="hillsborough")
    parser.add_argument("--targets", default="all",
                        help="Comma-separated targets: owners,dbpr,subscribers or 'all' (default: all)")
    parser.add_argument("--source", default=None,
                        help="Optional enriched_contacts.source filter for owners only, e.g. tracerfy or voters")
    parser.add_argument("--days", type=int, default=None,
                        help="Override recheck window in days for owners (default: DNC_RECHECK_DAYS)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max phones per target (default: all eligible)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show scope without calling the API")
    args = parser.parse_args()

    targets = [t.strip() for t in args.targets.split(",")]

    try:
        stats = run_dnc_refresh(
            county_id=args.county_id,
            days=args.days,
            limit=args.limit,
            dry_run=args.dry_run,
            targets=targets,
            source=args.source,
        )
        sys.exit(0)
    except Exception as e:
        logger.error("DNC refresh failed: %s", e)
        logger.debug(traceback.format_exc())
        sys.exit(1)
