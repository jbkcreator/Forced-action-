"""R-1: seed lending.lender_routing_rules from the T-05 lender matrix.

The spec: "The six rows in #341 become the seed of lender_routing_rules."  The six T-05 rows
(config/lender_matrix.py) become lender rows and product boxes, derived from that data; this
script names no lender itself.  The two rows that share the primary key prefix become one lender
with two boxes (a flip box and a ground-up box).

Safe defaults, so nothing routes until Josh acts:
  - every seeded lender has agreement_status 'pending' (the router never routes to a lender
    that is not 'signed'); Josh flips each to 'signed' through the admin API.
  - the primary lender (the T-05 primary prefix) gets the spec defaults: seat cap 15 and the
    ground-up minimum taken from its ground-up row ($500,000).
  - priorities continue after any existing rows; an existing lender (matched by name) is skipped
    whole, so re-running adds nothing and never overwrites Josh's edits.

Not carried over (the table has no column for them): minimum purchase price, property types and
cost parameters.  The T-05 fit evaluator for the enrichment card still reads config/lender_matrix.py
until it is cut over to this table.

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
    """Insert missing lenders and their boxes on an open connection. Returns lenders inserted.

    Does not commit; the caller is responsible.
    """
    existing = set(conn.execute(text("SELECT name FROM lending.lender_routing_rules")).scalars().all())
    priority = conn.execute(text("SELECT COALESCE(MAX(priority), 0) FROM lending.lender_routing_rules")).scalar()
    has_primary = bool(conn.execute(text("SELECT 1 FROM lending.lender_routing_rules WHERE is_primary LIMIT 1")).scalar())

    inserted = 0
    for name, rows in _lenders().items():
        if name in existing:
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
        lender_id = conn.execute(
            text(
                "INSERT INTO lending.lender_routing_rules"
                "  (name, active, is_primary, agreement_status, priority, monthly_cap,"
                "   construction_min_amount, takes_ground_up)"
                " VALUES (:name, TRUE, :primary, 'pending', :prio, :cap, :cmin, :gu)"
                " RETURNING id"
            ),
            {
                "name": name, "primary": is_primary, "prio": priority, "cap": monthly_cap,
                "cmin": construction_min, "gu": bool(ground_up),
            },
        ).scalar_one()
        for r in rows:
            conn.execute(
                text(
                    "INSERT INTO lending.lender_product_boxes"
                    "  (lender_id, loan_types, min_loan_amount, max_loan_amount, approved_states,"
                    "   credit_floor, max_ltv, max_ltc, min_completed_projects, requires_ground_up_build)"
                    " VALUES (:lid, :lt, :min, :max, :states, :credit, :ltv, :ltc, :proj, :gu)"
                ),
                {
                    "lid": lender_id, "lt": sorted(t.value for t in r.loan_types),
                    "min": _int(r.min_loan_amount), "max": _int(r.max_loan_amount),
                    "states": sorted(r.approved_states) or None, "credit": r.credit_floor,
                    "ltv": r.max_ltv, "ltc": r.max_ltc, "proj": r.min_completed_projects,
                    "gu": r.requires_ground_up_build,
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
