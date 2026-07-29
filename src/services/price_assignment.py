"""Price-band assignment service (REVINT-v2.2 I3).

Feature flag PRICE_BAND_TESTING_ENABLED is hardcoded False until Josh
ratifies the real floor/ceiling values.  When False, assign_price always
uses the band floor regardless of requested_price_cents.

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
    "founder_tier":            {"floor": 90000,  "ceiling": 120000},
    "core_subscription":       {"floor": 19700,  "ceiling": 29700},
    "single_ZIP_pack":         {"floor": 14900,  "ceiling": 24900},
    "insurance_distress_pack": {"floor": 14700,  "ceiling": 24700},
    "bankruptcy_alert":        {"floor": 24700,  "ceiling": 34700},
    # hard_money_intro: EXCLUDED — no customer-facing price (RESPA gate)
    # lender_intro:     EXCLUDED — no customer-facing price (RESPA gate)
}

_RESPA_EXCLUDED: frozenset[str] = frozenset({"hard_money_intro", "lender_intro"})


# ── Public API ────────────────────────────────────────────────────────────────

def assign_price(
    opportunity_thread_id: str,
    offer: str,
    requested_price_cents: int,
    db: Session,
    ab_assignment_id: Optional[int] = None,
) -> PriceAssignment:
    """Assign a validated price to an opportunity thread for a given offer.

    When PRICE_BAND_TESTING_ENABLED is False the band floor is always used;
    requested_price_cents is ignored.  When True the requested price must fall
    within the configured band or ValueError is raised (band_validated=False
    row is never persisted in that case).

    Supersedes any existing active PriceAssignment for the same
    thread + offer before committing the new one.
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
        ab_assignment_id=ab_assignment_id,
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
        "price_assignment: thread=%s offer=%s price=%d band_validated=%s ab_id=%s",
        opportunity_thread_id, offer, effective_price, band_validated, ab_assignment_id,
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
