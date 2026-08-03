"""Price-band assignment service (REVINT-v2.2 I3).

Feature flag PRICE_BAND_TESTING_ENABLED is hardcoded False until Josh
ratifies the real floor/ceiling values.  When False, assign_price always
uses the band floor regardless of requested_price_cents.

INVARIANT: every band floor must equal the live price for that offer so
that the flag-off path is a no-op (charges exactly what it charges today).
A pytest in tests/test_revint.py::TestPriceBandDrift enforces this for
offers that have a canonical constant — see that class for details.

Two offer floors are still [FILL] — awaiting client confirmation of the
live prices for single_ZIP_pack and insurance_distress_pack before they
can be set correctly:
  - single_ZIP_pack: is this the same product as the $99 LEAD_PACK_PRICE?
  - insurance_distress_pack: confirm live price before enabling the flag.

Excluded from all price bands (no customer-facing price; RESPA gate):
  hard_money_intro, lender_intro
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.models import PriceAssignment

logger = logging.getLogger(__name__)

# ── Feature flag ──────────────────────────────────────────────────────────────

PRICE_BAND_TESTING_ENABLED: bool = False  # flip only after Josh ratifies bands

# ── Price bands (all values in cents) ────────────────────────────────────────

PRICE_BANDS: dict[str, dict[str, int]] = {
    # floor = live price so flag-off is a no-op (charges exactly what it charges today)
    "founder_tier":            {"floor": 110000, "ceiling": 130000},  # live: $1,100/mo
    "core_subscription":       {"floor": 29900,  "ceiling": 39900},   # live: $299/mo (plans.price_cents)
    "bankruptcy_alert":        {"floor": 29700,  "ceiling": 39700},   # live: $297/mo (bankruptcy_alert_config.PRICE_MONTHLY_CENTS)
    # [FILL] floor unconfirmed — client must ratify live price before flag flip
    "insurance_distress_pack": {"floor": 14700,  "ceiling": 24700},   # [FILL] confirm live price
    # hard_money_intro: EXCLUDED — no customer-facing price (RESPA gate)
    # lender_intro:     EXCLUDED — no customer-facing price (RESPA gate)
}

_RESPA_EXCLUDED: frozenset[str] = frozenset({"hard_money_intro", "lender_intro"})


# ── Public API ────────────────────────────────────────────────────────────────

def is_respa_excluded(offer: str) -> bool:
    """True for offers with no customer-facing price to test (RESPA gate)."""
    return offer in _RESPA_EXCLUDED

def assign_price(
    opportunity_thread_id: str,
    offer: str,
    requested_price_cents: int,
    db: Session,
    experiment_assignment_id: Optional[int] = None,
) -> PriceAssignment:
    """Assign a validated price to an opportunity thread for a given offer.

    When PRICE_BAND_TESTING_ENABLED is False the band floor is always used;
    requested_price_cents is ignored.  When True the requested price must fall
    within the configured band or ValueError is raised (band_validated=False
    row is never persisted in that case).

    Supersedes any existing active PriceAssignment for the same
    thread + offer before committing the new one.

    experiment_assignment_id: the id of the AgentLaneExperimentAssignment
    row (src/services/agent_lane_experiment_engine.py) this price came
    from, if any — not a Lifecycle AbAssignment id.
    """
    if offer in _RESPA_EXCLUDED:
        raise ValueError(
            f"offer '{offer}' is RESPA-gated and has no customer-facing price"
        )

    band = PRICE_BANDS.get(offer)
    if band is None:
        raise ValueError(f"no price band configured for offer '{offer}'")

    floor_cents = band["floor"]
    ceiling_cents = band["ceiling"]

    if not PRICE_BAND_TESTING_ENABLED:
        effective_price = floor_cents
        band_validated = True
    else:
        effective_price = requested_price_cents
        band_validated = floor_cents <= effective_price <= ceiling_cents
        if not band_validated:
            raise ValueError(
                f"requested price {effective_price}¢ outside band "
                f"[{floor_cents}, {ceiling_cents}] for offer '{offer}'"
            )

    # Supersede existing active assignment(s) for this thread + offer
    db.execute(
        text(
            "UPDATE price_assignments "
            "SET status = 'superseded' "
            "WHERE opportunity_thread_id = :thread "
            "  AND offer = :offer "
            "  AND status = 'active'"
        ),
        {"thread": opportunity_thread_id, "offer": offer},
    )

    now = datetime.now(timezone.utc)
    assignment = PriceAssignment(
        opportunity_thread_id=opportunity_thread_id,
        offer=offer,
        assigned_price_cents=effective_price,
        currency="usd",
        experiment_assignment_id=experiment_assignment_id,
        price_band_floor_cents=floor_cents,
        price_band_ceiling_cents=ceiling_cents,
        band_validated=band_validated,
        assigned_at=now,
        created_at=now,
        status="active",
    )
    db.add(assignment)
    db.flush()

    logger.info(
        "price_assignment: thread=%s offer=%s price=%d band_validated=%s experiment_assignment_id=%s",
        opportunity_thread_id, offer, effective_price, band_validated, experiment_assignment_id,
    )
    return assignment


def get_active_price(
    opportunity_thread_id: str,
    offer: str,
    db: Session,
) -> Optional[PriceAssignment]:
    """Return the latest active PriceAssignment for a thread + offer, or None."""
    row = db.execute(
        text(
            "SELECT id FROM price_assignments "
            "WHERE opportunity_thread_id = :thread "
            "  AND offer = :offer "
            "  AND status = 'active' "
            "ORDER BY assigned_at DESC "
            "LIMIT 1"
        ),
        {"thread": opportunity_thread_id, "offer": offer},
    ).fetchone()
    if row is None:
        return None
    return db.get(PriceAssignment, row.id)
