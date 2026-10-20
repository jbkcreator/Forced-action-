"""R-1: seed lending.lender_routing_rules from the T-05 lender matrix.

The spec: "The six rows in #341 become the seed of lender_routing_rules."  The six T-05 rows
(config/lender_matrix.py) become lender rows, derived from that data; this script names no lender
itself.  The two rows that share the primary key prefix become one lender.

Each seeded lender records its T-05 ``rule_keys`` and gets NO boxes of its own: the router reads the
eligibility numbers from the T-05 rule values (the same ones the lead card's fit score uses), so the
numbers live in one place.  Boxes in ``lender_product_boxes`` are only for lenders T-05 does not define.

Safe defaults, so nothing routes until Josh acts:
  - every seeded lender has agreement_status 'pending' (the router never routes to a lender
    that is not 'signed'); Josh flips each to 'signed' through the admin API.
  - the primary lender (the T-05 primary prefix) gets the spec defaults: seat cap 15 and the
    ground-up minimum taken from its ground-up row ($500,000).
  - priorities continue after any existing rows; an existing lender (matched by name) is skipped
    whole, so re-running adds nothing and never overwrites Josh's edits.

The fit evaluator for the enrichment card keeps reading the T-05 rule values; nothing is copied here.

Run:  PYTHONPATH=. python migrations/apply_lending_lender_routing_seed.py
Run after apply_lending_lender_routing.py.
"""
import logging
import sys
from collections import OrderedDict

from sqlalchemy import create_engine, text

from config.lender_matrix import LENDER_MATRIX, LenderRules
from config.lending_enrichment import BACKFLIP_KEY_PREFIX
from config.lending_routing import PRIMARY_DEFAULT_CONSTRUCTION_MIN, PRIMARY_DEFAULT_SEAT_CAP
from config.settings import get_settings
from src.lending.contracts import LoanType

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

PRIMARY_DISPLAY_NAME = "Backflip"


def _group(rules: LenderRules) -> str:
    return PRIMARY_DISPLAY_NAME if rules.key.startswith(BACKFLIP_KEY_PREFIX) else rules.name


def _lenders() -> "OrderedDict[str, list[LenderRules]]":
    grouped: "OrderedDict[str, list[LenderRules]]" = OrderedDict()
    for rules in LENDER_MATRIX:
        grouped.setdefault(_group(rules), []).append(rules)
    return grouped


def _int(value):
    return int(value) if value is not None else None


def apply_to(conn) -> int:
    """Insert missing lenders (linked to their T-05 rule keys) on an open connection. Returns lenders inserted.

    Does not commit; the caller is responsible.
    """
    existing = set(conn.execute(text("SELECT name FROM lending.lender_routing_rules")).scalars().all())
    claimed_keys = {
        key
        for keys in conn.execute(text("SELECT rule_keys FROM lending.lender_routing_rules WHERE rule_keys IS NOT NULL")).scalars()
        for key in keys
    }
    priority = conn.execute(text("SELECT COALESCE(MAX(priority), 0) FROM lending.lender_routing_rules")).scalar()
    has_primary = bool(conn.execute(text("SELECT 1 FROM lending.lender_routing_rules WHERE is_primary LIMIT 1")).scalar())

    inserted = 0
    for name, rows in _lenders().items():
        # Skip a lender that already exists by name, or whose T-05 keys another row already uses (it was renamed):
        # re-seeding must never add an unsigned duplicate of a lender Josh already manages.
        if name in existing or claimed_keys & {r.key for r in rows}:
            continue
        priority += 1
        is_primary = (name == PRIMARY_DISPLAY_NAME) and not has_primary
        ground_up = [r for r in rows if LoanType.GROUND_UP_CONSTRUCTION in r.loan_types]
        construction_min = None
        monthly_cap = None
        if is_primary:
            monthly_cap = PRIMARY_DEFAULT_SEAT_CAP
            construction_min = next(
                (_int(r.min_loan_amount) for r in ground_up if r.min_loan_amount is not None),
                PRIMARY_DEFAULT_CONSTRUCTION_MIN,
            )
            has_primary = True
        conn.execute(
            text(
                "INSERT INTO lending.lender_routing_rules"
                "  (name, active, is_primary, agreement_status, priority, monthly_cap,"
                "   construction_min_amount, takes_ground_up, rule_keys)"
                " VALUES (:name, TRUE, :primary, 'pending', :prio, :cap, :cmin, :gu, :keys)"
            ),
            {
                "name": name, "primary": is_primary, "prio": priority, "cap": monthly_cap,
                "cmin": construction_min, "gu": bool(ground_up), "keys": [r.key for r in rows],
            },
        )
        inserted += 1
    return inserted


def main() -> None:
    engine = create_engine(str(get_settings().database_url), pool_pre_ping=True)
    with engine.begin() as conn:
        count = apply_to(conn)
    log.info("apply_lending_lender_routing_seed: %d lender(s) inserted", count)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Seed failed: %s", exc)
        sys.exit(1)
