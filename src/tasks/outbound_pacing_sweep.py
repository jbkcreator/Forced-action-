"""
Outbound pacing sweep — routes and dispatches first-touch outbound contacts (fa5.3).

Phase A: Route newly enriched contacts to the correct outbound channel.
  - Mobile phones    → staged for paced SMS dispatch (sets outbound_queued_at)
  - Landlines / VoIP / unknown → direct mail flag via direct_mail.py
  - No mailing address found   → left unrouted (outbound_queued_at stays NULL)
  - Per-contact try/except: a single bad row never aborts the batch.

Phase B: Dispatch staged mobile contacts with adaptive pacing.
  - Reads live delivery failure rate from sms_send_logs (outbound_first_touch)
  - Applies exponential backoff delay between sends
  - At high backpressure (delay >= 60s), processes 1 contact per cron tick;
    the 5-minute interval itself provides the effective inter-batch wait.
  - Each contact is processed in its own short DB session — claim, send, commit.
  - Confirmed-permanent failures (DNC, opt-out, invalid phone) are marked
    outbound_terminal=TRUE and excluded from future sweeps.
  - dnc_check_required is treated as transient — retries until DNC data is refreshed.

Run:
    python -m src.tasks.outbound_pacing_sweep                          # both counties
    python -m src.tasks.outbound_pacing_sweep --county-id hillsborough
    python -m src.tasks.outbound_pacing_sweep --dry-run --limit 10

Cron (every 5 minutes):
    */5 * * * * cd /path/to/app && python -m src.tasks.outbound_pacing_sweep >> logs/outbound_pacing_sweep.log 2>&1
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services import opt_in_sentinel
from src.services.outbound_optimizer import (
    calculate_pacing_delay,
    check_outbound_delivery_backpressure,
    process_new_outbound_targets,
)
from src.services.phone_utils import normalize as normalize_phone
from src.services.sms_compliance import send_sms
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

_DEFAULT_COUNTIES = ["hillsborough", "pinellas"]
_DEFAULT_LIMIT = 50

# Kept small (25) to limit the duration that FOR UPDATE locks are held while
# flag_direct_mail_eligible issues sub-queries per contact.
_PHASE_A_BATCH = 25

# suppress_reason values from sms_compliance that are confirmed permanent.
# dnc_check_required is intentionally excluded — it means "DNC data missing or stale",
# not "confirmed on DNC list". Those contacts retry after the monthly dnc_refresh.
_TERMINAL_SUPPRESS_REASONS = frozenset({
    "dnc_or_opted_out",
    "invalid_phone",
    "unresolvable",
    "prospect_not_contactable",
    "prospect_sms_consent_withdrawn",
})

_COUNTY_CITY: dict[str, str] = {
    "hillsborough": "Tampa",
    "pinellas":     "Pinellas County",
}


def _first_touch_body(county_id: str) -> str:
    city = _COUNTY_CITY.get(county_id)
    if city is None:
        logger.warning(
            "_first_touch_body: unknown county_id=%r — using generic 'your area' fallback",
            county_id,
        )
        city = "your area"
    return (
        f"Hi — we're a local home-buying team in {city}. We purchase properties "
        "directly from owners: no agent fees, no repairs, fast close on your timeline. "
        "If selling has ever crossed your mind, we'd love to make you a fair offer. "
        "Reply INFO to learn more, or STOP to opt out."
    )


def _phase_a(county_id: str, db, dry_run: bool) -> dict:
    """Route unprocessed enriched contacts to mobile queue or direct mail."""
    stats = {"routed_mobile": 0, "routed_direct_mail": 0, "unroutable": 0, "skipped": 0, "errors": 0}

    rows = db.execute(
        text("""
            SELECT id, property_id, mobile_phone, landline
            FROM enriched_contacts
            WHERE county_id          = :county_id
              AND match_success       = true
              AND outbound_queued_at  IS NULL
              AND (outbound_terminal IS NULL OR outbound_terminal = FALSE)
              AND (mobile_phone IS NOT NULL OR landline IS NOT NULL)
            ORDER BY enriched_at ASC
            LIMIT :batch
            FOR UPDATE SKIP LOCKED
        """),
        {"county_id": county_id, "batch": _PHASE_A_BATCH},
    ).mappings().all()

    if not rows:
        return stats

    for row in rows:
        carrier_type = "mobile" if row["mobile_phone"] else "landline"
        event_payload = {
            "contact_id":   row["id"],
            "property_id":  row["property_id"],
            "carrier_info": {"type": carrier_type},
        }

        if dry_run:
            logger.info(
                "[OutboundPacing][A][DRY RUN] county=%s contact_id=%d -> %s",
                county_id, row["id"], carrier_type,
            )
            if carrier_type == "mobile":
                stats["routed_mobile"] += 1
            else:
                stats["routed_direct_mail"] += 1
            continue

        try:
            result = process_new_outbound_targets(event_payload, db)
        except Exception as exc:
            stats["errors"] += 1
            logger.error(
                "[OutboundPacing][A] error routing contact_id=%d: %s",
                row["id"], exc, exc_info=True,
            )
            continue

        if result == "staged":
            stats["routed_mobile"] += 1
        elif result == "direct_mail":
            stats["routed_direct_mail"] += 1
        elif result == "unroutable":
            stats["unroutable"] += 1
        else:
            stats["skipped"] += 1

    logger.info(
        "[OutboundPacing][A] county=%s mobile=%d direct_mail=%d unroutable=%d skipped=%d errors=%d",
        county_id, stats["routed_mobile"], stats["routed_direct_mail"],
        stats["unroutable"], stats["skipped"], stats["errors"],
    )
    return stats


def _dispatch_one(contact_id: int, property_id: int, raw_phone: str, county_id: str) -> str:
    """
    Claim, send, and commit a single contact in one short DB session.

    Each contact has exactly one get_db_context() covering claim + send + commit.
    No cross-session lock contention; crash safety is per-contact.

    Returns: "sent" | "suppressed" | "terminal" | "skipped" | "error"
    """
    # Normalize once here; send_sms() also normalizes internally, but the
    # sms_send_logs lookup must use the same value that was written to the log.
    phone = normalize_phone(raw_phone) or raw_phone

    with get_db_context() as db:
        # Re-claim with FOR UPDATE SKIP LOCKED — skip cleanly if grabbed concurrently.
        row = db.execute(
            text("""
                SELECT id FROM enriched_contacts
                WHERE id = :cid
                  AND first_touch_sent_at IS NULL
                  AND (outbound_terminal IS NULL OR outbound_terminal = FALSE)
                FOR UPDATE SKIP LOCKED
            """),
            {"cid": contact_id},
        ).mappings().first()

        if row is None:
            return "skipped"

        opt_in_sentinel.mark_pending(phone)

        ok = send_sms(
            to=phone,
            body=_first_touch_body(county_id),
            db=db,
            message_type="opt_in_prompt",
            task_type="outbound_first_touch",
        )

        if ok:
            db.execute(
                text("UPDATE enriched_contacts SET first_touch_sent_at = :now WHERE id = :cid"),
                {"cid": contact_id, "now": datetime.now(timezone.utc)},
            )
            logger.info(
                "[OutboundPacing][B] sent contact_id=%d property_id=%d",
                contact_id, property_id,
            )
            return "sent"

        # Scope lookup to rows written in the last 5 seconds to avoid reading a
        # stale or concurrent-contact row with the same phone number.
        since = datetime.now(timezone.utc) - timedelta(seconds=5)
        log_row = db.execute(
            text("""
                SELECT suppress_reason FROM sms_send_logs
                WHERE phone     = :phone
                  AND task_type = 'outbound_first_touch'
                  AND created_at >= :since
                ORDER BY created_at DESC
                LIMIT 1
            """),
            {"phone": phone, "since": since},
        ).mappings().first()

        suppress_reason = log_row["suppress_reason"] if log_row else None

        if suppress_reason in _TERMINAL_SUPPRESS_REASONS:
            db.execute(
                text("UPDATE enriched_contacts SET outbound_terminal = TRUE WHERE id = :cid"),
                {"cid": contact_id},
            )
            logger.info(
                "[OutboundPacing][B] terminal contact_id=%d reason=%s",
                contact_id, suppress_reason,
            )
            return "terminal"

        logger.warning(
            "[OutboundPacing][B] suppressed contact_id=%d phone=%s reason=%s",
            contact_id, phone, suppress_reason,
        )
        return "suppressed"


def _phase_b(county_id: str, limit: int, dry_run: bool) -> dict:
    """Dispatch staged mobile contacts with adaptive pacing."""
    stats = {"sent": 0, "suppressed": 0, "terminal": 0, "skipped": 0, "errors": 0}

    # Pre-initialize so they're defined if the with-block raises before assignment.
    rows: list = []
    pacing_delay: float = 5.0

    with get_db_context() as db:
        drop_rate = check_outbound_delivery_backpressure(db)
        pacing_delay = calculate_pacing_delay(drop_rate)

        max_per_run = 1 if pacing_delay >= 60.0 else limit

        logger.info(
            "[OutboundPacing][B] county=%s drop_rate=%.3f delay=%.1fs max_per_run=%d",
            county_id, drop_rate, pacing_delay, max_per_run,
        )

        rows = list(db.execute(
            text("""
                SELECT id, property_id, mobile_phone
                FROM enriched_contacts
                WHERE county_id           = :county_id
                  AND mobile_phone        IS NOT NULL
                  AND outbound_queued_at  IS NOT NULL
                  AND first_touch_sent_at IS NULL
                  AND (outbound_terminal IS NULL OR outbound_terminal = FALSE)
                ORDER BY outbound_queued_at ASC
                LIMIT :max_per_run
            """),
            {"county_id": county_id, "max_per_run": max_per_run},
        ).mappings().all())

    if not rows:
        return stats

    for i, row in enumerate(rows):
        contact_id = row["id"]
        phone = row["mobile_phone"]

        if dry_run:
            logger.info(
                "[OutboundPacing][B][DRY RUN] county=%s contact_id=%d phone=%s delay=%.1fs",
                county_id, contact_id, phone, pacing_delay,
            )
            stats["sent"] += 1
            if i < len(rows) - 1:
                time.sleep(pacing_delay)
            continue

        try:
            outcome = _dispatch_one(contact_id, row["property_id"], phone, county_id)
            if outcome == "sent":
                stats["sent"] += 1
            elif outcome == "terminal":
                stats["terminal"] += 1
            elif outcome == "suppressed":
                stats["suppressed"] += 1
            elif outcome == "skipped":
                stats["skipped"] += 1
        except Exception as exc:
            stats["errors"] += 1
            logger.error(
                "[OutboundPacing][B] error contact_id=%d: %s",
                contact_id, exc, exc_info=True,
            )

        if i < len(rows) - 1:
            time.sleep(pacing_delay)

    logger.info(
        "[OutboundPacing][B] county=%s sent=%d suppressed=%d terminal=%d skipped=%d errors=%d",
        county_id, stats["sent"], stats["suppressed"], stats["terminal"],
        stats["skipped"], stats["errors"],
    )
    return stats


def run(
    county_ids: Optional[list[str]] = None,
    limit: int = _DEFAULT_LIMIT,
    dry_run: bool = False,
) -> dict:
    """
    Run Phase A + Phase B for each county.

    Separate DB sessions for each phase so Phase A routing commits before
    Phase B dispatch begins. A Phase B failure cannot roll back Phase A work.
    """
    counties = county_ids or _DEFAULT_COUNTIES
    results: dict = {}

    for county_id in counties:
        with get_db_context() as db:
            a = _phase_a(county_id, db, dry_run)

        b = _phase_b(county_id, limit, dry_run)

        results[county_id] = {"phase_a": a, "phase_b": b}

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Outbound pacing sweep (fa5.3)")
    parser.add_argument(
        "--county-id",
        dest="county_id",
        default=None,
        help="Single county to process. Omit for all default counties.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=_DEFAULT_LIMIT,
        help=f"Max contacts to dispatch in Phase B per county (default: {_DEFAULT_LIMIT})",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    county_ids = [args.county_id] if args.county_id else None

    try:
        results = run(county_ids=county_ids, limit=args.limit, dry_run=args.dry_run)
        for cid, r in results.items():
            print(f"\n[{cid}]")
            print(f"  Phase A: mobile={r['phase_a']['routed_mobile']}  direct_mail={r['phase_a']['routed_direct_mail']}  unroutable={r['phase_a']['unroutable']}  errors={r['phase_a']['errors']}")
            print(f"  Phase B: sent={r['phase_b']['sent']}  suppressed={r['phase_b']['suppressed']}  terminal={r['phase_b']['terminal']}  skipped={r['phase_b']['skipped']}  errors={r['phase_b']['errors']}")
        sys.exit(0)
    except Exception as exc:
        logger.error("[OutboundPacing] crashed: %s", exc, exc_info=True)
        sys.exit(1)
