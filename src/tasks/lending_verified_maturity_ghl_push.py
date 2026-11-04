"""One-off: push Verified maturity numbers already loaded into BatchDialer to GHL as cold contacts.

The PropertyRadar "verified_maturity" list was loaded into the dialer (pool ``verified_maturity``,
``source_record_ref`` = ``pr_list:verified_maturity:<radar_id>``) with no GHL step. This upserts each
active loaded number into the Next Deal Lending sub-account with cold-lead do-not-disturb and tags
``ndl-verified-maturity`` (Josh's call list, config.lending_compliance.GHL_CALL_LIST_TAGS), ``ndl-cold``
and ``ndl-state-<xx>``. It reuses the wholesaler GHL sink; nothing is loaded, scrubbed or dialed.

Skipped: warm numbers (a website or LendingFlow lead; cold do-not-disturb would block their texts),
and numbers suppressed or marked do-not-contact since the load.

Dry run by default (reads only, prints counts). ``--live`` needs ``--confirm N`` where N is the
number of contacts the dry run reports, so nothing is pushed unconfirmed.

Usage:
    python -m src.tasks.lending_verified_maturity_ghl_push
    python -m src.tasks.lending_verified_maturity_ghl_push --live --confirm 187

Exit codes: 0 ok, 1 some pushes failed, 3 refused (no GHL account / halt on), 4 count not confirmed.
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from typing import Any, Mapping

from sqlalchemy import text

from config.lending_compliance import GHL_TAG_COLD_LEAD
from config.lending_wholesalers import GHL_TAG_STATE_PREFIX
from src.core.database import get_db_context
from src.lending.global_halt import is_globally_halted
from src.lending.wholesaler_lists import ghl_sink, push_to_ghl

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_PUSH_FAILED = 1
EXIT_REFUSED = 3
EXIT_NOT_CONFIRMED = 4

POOL = "verified_maturity"
GHL_TAG_VERIFIED_MATURITY = "ndl-verified-maturity"

# One row per active loaded phone; state from the PropertyRadar list record behind the ref.
_RECORDS = text("""
    SELECT DISTINCT ON (d.phone)
           d.source_record_ref, d.phone, d.borrower_name, d.entity_name, r.state,
           (EXISTS (SELECT 1 FROM lending.web_leads w WHERE w.phone = d.phone)
            OR EXISTS (SELECT 1 FROM lending.lendingflow_leads l WHERE l.phone = d.phone)) AS warm,
           (EXISTS (SELECT 1 FROM lending.suppression_list s WHERE s.phone = d.phone)
            OR EXISTS (SELECT 1 FROM lending.contacts c WHERE c.phone = d.phone AND c.do_not_contact)) AS suppressed
      FROM lending.dialer_load_records d
      LEFT JOIN lending.pr_list_records r
             ON r.list_key = :pool AND r.radar_id = split_part(d.source_record_ref, ':', 3)
     WHERE d.pool = :pool AND d.active AND d.phone IS NOT NULL
     ORDER BY d.phone, d.loaded_at DESC
""")


def maturity_tags(record: Mapping[str, Any]) -> list[str]:
    tags = [GHL_TAG_VERIFIED_MATURITY, GHL_TAG_COLD_LEAD]
    state = str(record.get("state") or "").strip().lower()
    if state:
        tags.append(f"{GHL_TAG_STATE_PREFIX}{state}")
    return tags


def select_records(rows: list[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], Counter]:
    """Pushable records and skip counts by reason."""
    keep: list[dict[str, Any]] = []
    skipped: Counter = Counter()
    for row in rows:
        if row["warm"]:
            skipped["WARM_LEAD"] += 1
        elif row["suppressed"]:
            skipped["SUPPRESSED"] += 1
        else:
            keep.append({"source_record_ref": row["source_record_ref"], "phone": row["phone"], "pool": POOL,
                         "borrower_name": row["borrower_name"], "entity_name": row["entity_name"],
                         "state": row["state"]})
    return keep, skipped


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push loaded Verified maturity numbers to GHL.")
    parser.add_argument("--live", action="store_true", help="push to GHL (default: dry run)")
    parser.add_argument("--confirm", type=int, help="contact count from the dry run you approve pushing")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv)
    with get_db_context() as session:
        rows = [dict(r) for r in session.execute(_RECORDS, {"pool": POOL}).mappings()]
    records, skipped = select_records(rows)
    states = Counter(str(r["state"] or "UNKNOWN") for r in records)
    summary = {"active_loaded": len(rows), "to_push": len(records), "skipped": dict(skipped),
               "by_state": dict(states), "tags_example": maturity_tags(records[0]) if records else []}
    logger.info("[maturity-ghl] %s", json.dumps(summary, indent=2))
    if not args.live:
        logger.info("[maturity-ghl] dry run: nothing pushed")
        return EXIT_OK
    if args.confirm != len(records):
        logger.error("[maturity-ghl] refused: this run pushes %d contact(s); re-run with --confirm %d",
                     len(records), len(records))
        return EXIT_NOT_CONFIRMED
    if is_globally_halted():
        logger.error("[maturity-ghl] refused: lending emergency halt is on")
        return EXIT_REFUSED
    sink = ghl_sink()
    if sink is None:
        logger.error("[maturity-ghl] refused: lending GHL account not configured")
        return EXIT_REFUSED
    result = push_to_ghl(records, sink, tags_for=maturity_tags)
    logger.info("[maturity-ghl] pushed %d, failed %d", result.pushed, len(result.failed))
    return EXIT_PUSH_FAILED if result.failed else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
