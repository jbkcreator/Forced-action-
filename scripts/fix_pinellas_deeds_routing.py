"""
Fix Pinellas deed routing — deeds silently dropped since ~2026-09-04.

The Pinellas Clerk ORI liens mapping (county_column_mappings, source
'Pinellas Clerk ORI') routes rows to buckets by DocType. Its deed rule was
`match_exact: ["DEED", "TAX DEED"]`, but the Pinellas portal emits granular
deed labels (WARRANTY DEED, QUIT CLAIM DEED, SPECIAL WARRANTY DEED, etc.).
Unlike Hillsborough — whose value_maps folds "(D) DEED", "(DPL) DEED PLAT",
"(TAXDEED) TAX DEED" → "DEED" before routing — Pinellas has NO deed
normalization in its value_maps (only JUDGMENT variants). So every granular
deed label missed the exact match and fell through to `default: skip`.

Symptom: pinellas deeds table stuck at record_date 2026-09-04 while judgments
from the same scraper stayed current — the dial-list staleness check then
flagged "Pinellas deeds" stale.

Fix: switch the deeds rule from `match_exact` to `match_contains: ["DEED"]`,
mirroring the existing LIS PENDENS rule's style. "DEED" as a substring catches
every deed variant (incl. TAX DEED) and is safe against the other Pinellas
rules — none of LIS PENDENS / JUDGMENT / TAX LIEN / LIEN / FINANCING STATEMENT
/ CORPORATE LIEN contain the substring "DEED".

Scope: Pinellas liens mappings only (Hillsborough already works via value_maps).
Only affects future scrape runs — the 2026-09-05..present gap needs a
date-range re-scrape to backfill.

Idempotent: re-running when the rule is already match_contains is a no-op.
"""
import json
import logging

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("fix_pinellas_deeds_routing")


def _is_exact_deed_rule(rule: dict) -> bool:
    return (
        rule.get("bucket") == "deeds"
        and "DEED" in (rule.get("match_exact") or [])
        and not rule.get("match_contains")
    )


def main() -> None:
    with get_db_context() as db:
        rows = db.execute(text(
            "SELECT ccm.id, ccm.row_routing "
            "FROM county_column_mappings ccm "
            "JOIN county_sources cs ON cs.id = ccm.source_id "
            "WHERE cs.county_id = 'pinellas' AND cs.signal_type = 'liens' "
            "AND ccm.row_routing IS NOT NULL"
        )).fetchall()

        updated = 0
        for mapping_id, routing in rows:
            routing = routing or {}
            rules = routing.get("rules") or []
            changed = False
            for rule in rules:
                if _is_exact_deed_rule(rule):
                    rule.pop("match_exact", None)
                    rule["match_contains"] = ["DEED"]
                    changed = True
            if not changed:
                logger.info("id=%s: no exact deed rule to migrate — skip", mapping_id)
                continue

            db.execute(
                text("UPDATE county_column_mappings "
                     "SET row_routing = CAST(:r AS jsonb), updated_at = NOW() "
                     "WHERE id = :id"),
                {"r": json.dumps(routing), "id": mapping_id},
            )
            updated += 1
            logger.info("id=%s: deed rule migrated match_exact -> match_contains ['DEED']", mapping_id)

        db.commit()
        logger.info("done: %d Pinellas liens mapping(s) updated", updated)


if __name__ == "__main__":
    main()
