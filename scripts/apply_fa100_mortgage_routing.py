"""
fa100 (Sprint 4.4) — route MORTGAGE documents into the deeds bucket so the
deed loader populates deeds.mortgage_amount (feeds the equity pipeline).

Before this, county_column_mappings.row_routing sent only DEED/TAX DEED to the
deeds bucket and dropped everything unmatched (default: skip), so MORTGAGE
records were never ingested and est_mortgage_bal was always NULL.

We add an EXACT-match rule for "MORTGAGE" only — exact match deliberately
excludes "SATISFACTION OF MORTGAGE", "ASSIGNMENT OF MORTGAGE", and
"MODIFICATION OF MORTGAGE", which are not new debt. If the county's actual
DocType string differs (e.g. "MTG"), this rule is a harmless no-op until the
string is corrected — it never mis-routes.

Only mappings that carry both the routing column AND a 'Filing Amt' source
column can yield a mortgage amount, so we target those.

Idempotent: re-running does not duplicate the rule.
"""
import logging

from sqlalchemy import text

from src.core.database import get_db_context

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("fa100")

MORTGAGE_RULE = {"bucket": "deeds", "match_exact": ["MORTGAGE"]}


def _has_mortgage_rule(routing: dict) -> bool:
    for rule in routing.get("rules", []):
        if rule.get("bucket") == "deeds" and "MORTGAGE" in (rule.get("match_exact") or []):
            return True
    return False


def main() -> None:
    with get_db_context() as db:
        rows = db.execute(text(
            "SELECT id, source_columns, row_routing FROM county_column_mappings "
            "WHERE row_routing IS NOT NULL"
        )).fetchall()

        updated = 0
        for row in rows:
            mapping_id, source_columns, routing = row
            cols = source_columns or []
            routing = routing or {}
            route_col = routing.get("column")

            if route_col not in cols:
                logger.info("id=%s skip: routing column %r not in source_columns", mapping_id, route_col)
                continue
            if "Filing Amt" not in cols:
                logger.info("id=%s skip: no 'Filing Amt' column — cannot yield a mortgage amount", mapping_id)
                continue
            if _has_mortgage_rule(routing):
                logger.info("id=%s already has the MORTGAGE rule — no change", mapping_id)
                continue

            routing.setdefault("rules", []).append(MORTGAGE_RULE)
            db.execute(
                text("UPDATE county_column_mappings SET row_routing = CAST(:r AS jsonb), "
                     "updated_at = NOW() WHERE id = :id"),
                {"r": __import__("json").dumps(routing), "id": mapping_id},
            )
            updated += 1
            logger.info("id=%s added MORTGAGE -> deeds routing rule", mapping_id)

        db.commit()
        logger.info("fa100 done: %d mapping(s) updated", updated)


if __name__ == "__main__":
    main()
