"""
Outbound pacing sweep — routes and dispatches first-touch outbound contacts (fa5.3).

Phase A: Route newly enriched contacts to the correct outbound channel.
  - Mobile phones    → staged for paced SMS dispatch (sets outbound_queued_at)
  - Landlines / VoIP / unknown → direct mail flag via direct_mail.py

Phase B: Dispatch staged mobile contacts with adaptive pacing.
  - Reads live engagement drop-off from message_outcomes
  - Applies exponential backoff delay between sends
  - At high backpressure (delay >= 60s), processes 1 contact per cron tick;
    the 5-minute interval itself provides the effective inter-batch wait.

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
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from src.core.database import get_db_context
from src.services import opt_in_sentinel
from src.services.outbound_optimizer import (
    calculate_pacing_delay,
    check_outbound_delivery_backpressure,
    process_new_outbound_targets,
)
from src.services.sms_compliance import send_sms
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

_DEFAULT_COUNTIES = ["hillsborough", "pinellas"]
_DEFAULT_LIMIT = 50
_PHASE_A_BATCH = 200

_COUNTY_CITY: dict[str, str] = {
    "hillsborough": "Tampa",
    "pinellas":     "Pinellas County",
}


def _first_touch_body(county_id: str) -> str:
    city = _COUNTY_CITY.get(county_id, "your area")
    return (
        f"Hi — we're a local home-buying team in {city}. We purchase properties "
        "directly from owners: no agent fees, no repairs, fast close on your timeline. "
        "If selling has ever crossed your mind, we'd love to make you a fair offer. "
        "Reply INFO to learn more, or STOP to opt out."
    )


def _phase_a(county_id: str, db, dry_run: bool) -> dict:
    """Route unprocessed enriched contacts to mobile queue or direct mail."""
    stats = {"routed_mobile": 0, "routed_direct_mail": 0, "skipped": 0}

    rows = db.execute(
        text("""
            SELECT id, property_id, mobile_phone, landline
            FROM enriched_contacts
            WHERE county_id          = :county_id
              AND match_success       = true
              AND outbound_queued_at  IS NULL
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
        # BatchData populates mobile_phone for mobiles and landline for fixed-line.
        # When both are set, mobile takes priority for SMS routing.
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

        result = process_new_outbound_targets(event_payload, db)
        if result == "staged":
            stats["routed_mobile"] += 1
        elif result == "direct_mail":
            stats["routed_direct_mail"] += 1
        else:
            stats["skipped"] += 1

    logger.info(
        "[OutboundPacing][A] county=%s mobile=%d direct_mail=%d skipped=%d",
        county_id, stats["routed_mobile"], stats["routed_direct_mail"], stats["skipped"],
    )
    return stats


def _phase_b(county_id: str, limit: int, db, dry_run: bool) -> dict:
    """Dispatch staged mobile contacts with adaptive pacing."""
    stats = {"sent": 0, "suppressed": 0, "errors": 0}

    drop_rate = check_outbound_delivery_backpressure(db)
    pacing_delay = calculate_pacing_delay(drop_rate)

    # Cap batch size dynamically:
    # pacing_delay >= 60s → 1 contact per run; 5-min cron interval provides the wait.
    # pacing_delay <  60s → up to `limit` contacts; time.sleep() paces between sends.
    max_per_run = 1 if pacing_delay >= 60.0 else limit

    logger.info(
        "[OutboundPacing][B] county=%s drop_rate=%.3f delay=%.1fs max_per_run=%d",
        county_id, drop_rate, pacing_delay, max_per_run,
    )

    rows = db.execute(
        text("""
            SELECT id, property_id, mobile_phone
            FROM enriched_contacts
            WHERE county_id           = :county_id
              AND mobile_phone        IS NOT NULL
              AND outbound_queued_at  IS NOT NULL
              AND first_touch_sent_at IS NULL
            ORDER BY outbound_queued_at ASC
            LIMIT :max_per_run
            FOR UPDATE SKIP LOCKED
        """),
        {"county_id": county_id, "max_per_run": max_per_run},
    ).mappings().all()

    if not rows:
        return stats

    for i, row in enumerate(rows):
        phone = row["mobile_phone"]

        if dry_run:
            logger.info(
                "[OutboundPacing][B][DRY RUN] county=%s contact_id=%d phone=%s delay=%.1fs",
                county_id, row["id"], phone, pacing_delay,
            )
            stats["sent"] += 1
            continue

        try:
            # Open the opt-in consent window so a YES reply registers as consent.
            # mark_pending is a no-op when Redis is unavailable — degrades gracefully.
            opt_in_sentinel.mark_pending(phone)

            # message_type="opt_in_prompt" bypasses the SmsOptIn marketing gate.
            # Cold contacts have no prior consent record; "marketing" would suppress all sends.
            # sms_compliance still enforces DNC, opt-out, and quiet hours.
            ok = send_sms(
                to=phone,
                body=_first_touch_body(county_id),
                db=db,
                message_type="opt_in_prompt",
                task_type="outbound_first_touch",
            )

            if ok:
                db.execute(
                    text("""
                        UPDATE enriched_contacts
                        SET first_touch_sent_at = :now
                        WHERE id = :cid
                    """),
                    {"cid": row["id"], "now": datetime.now(timezone.utc)},
                )
                stats["sent"] += 1
                logger.info(
                    "[OutboundPacing][B] sent contact_id=%d property_id=%d",
                    row["id"], row["property_id"],
                )
            else:
                # Suppressed by compliance gate (DNC / opt-out / quiet hours).
                # Do NOT set first_touch_sent_at — quiet-hours contacts retry next sweep.
                stats["suppressed"] += 1
                logger.warning(
                    "[OutboundPacing][B] suppressed contact_id=%d phone=%s",
                    row["id"], phone,
                )

        except Exception as exc:
            stats["errors"] += 1
            logger.error(
                "[OutboundPacing][B] error contact_id=%d: %s",
                row["id"], exc, exc_info=True,
            )

        # Sleep between sends — skip after the last item in the batch
        if i < len(rows) - 1:
            time.sleep(pacing_delay)

    logger.info(
        "[OutboundPacing][B] county=%s sent=%d suppressed=%d errors=%d",
        county_id, stats["sent"], stats["suppressed"], stats["errors"],
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

        with get_db_context() as db:
            b = _phase_b(county_id, limit, db, dry_run)

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
            print(f"  Phase A: mobile={r['phase_a']['routed_mobile']}  direct_mail={r['phase_a']['routed_direct_mail']}")
            print(f"  Phase B: sent={r['phase_b']['sent']}  suppressed={r['phase_b']['suppressed']}  errors={r['phase_b']['errors']}")
        sys.exit(0)
    except Exception as exc:
        logger.error("[OutboundPacing] crashed: %s", exc, exc_info=True)
        sys.exit(1)
