"""Populate lead entitlements for the pro/founder_monthly/founder_annual plans.

`plans.entitlements` was empty ({}) for these three plans — every active/future
account on them fails `bucket_for()` in src/services/lead_delivery.py and is
silently excluded from lead-delivery candidacy for every grade, regardless of
territory lock or headroom. Starter is the only plan with a populated
entitlement bucket; these three are scaled off it:

    starter (base, $299/mo):   gold:20 ultra:2 bronze:10 silver:50 platinum:5
    pro (~1.67x, $499/mo):     rounded to 2x starter
    founder (~3.68x, $1100/mo or $11000/yr): rounded to 4x starter;
        monthly and annual are the same tier, just billed differently, so
        they get identical entitlements.

Only backfills rows that are still empty ({}), so it's safe to re-run and
won't clobber a value set deliberately after this runs.

Usage:
    PYTHONPATH=. python migrations/apply_pro_founder_plan_entitlements.py
"""
from __future__ import annotations

import json
import logging

from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ENTITLEMENTS = {
    "pro": {"gold": 40, "ultra": 4, "bronze": 20, "silver": 100, "platinum": 10},
    "founder_monthly": {"gold": 80, "ultra": 8, "bronze": 40, "silver": 200, "platinum": 20},
    "founder_annual": {"gold": 80, "ultra": 8, "bronze": 40, "silver": 200, "platinum": 20},
}


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    with engine.begin() as conn:
        for plan_id, entitlements in ENTITLEMENTS.items():
            result = conn.execute(
                text(
                    "UPDATE plans SET entitlements = :entitlements "
                    "WHERE plan_id = :plan_id AND entitlements = '{}'::jsonb"
                ),
                {"entitlements": json.dumps(entitlements), "plan_id": plan_id},
            )
            logger.info("plan %s: %d row(s) updated", plan_id, result.rowcount)

    logger.info("pro_founder_plan_entitlements complete.")


if __name__ == "__main__":
    main()
