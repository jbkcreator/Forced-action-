"""Dev-only seed for the Block 8 Retention Cohort viewport (T-B8-04).

NOT a migration and NOT for production — it inserts a handful of clearly-tagged
demo subscribers/accounts/movements so the local /admin/operator/retention
page renders a populated, visually varied grid (multiple cohort months,
channels, tiers, ZIPs, and a mix of never-churned / churned / reactivated
subscribers).

    PYTHONPATH=. python scripts/seed_retention_cohort_demo.py          # insert
    PYTHONPATH=. python scripts/seed_retention_cohort_demo.py --clean  # remove

Every seeded subscriber is tagged via event_feed_uuid LIKE 'rcdemo-%' so
--clean removes exactly what this script created and nothing else.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text as sa_text

from src.core.database import get_db_context
from src.core.models import CustomerAccount, MrrMovement, Subscriber, ZipTerritory

DEMO_TAG = "rcdemo"


def _clean(db) -> None:
    sub_ids = [r[0] for r in db.execute(sa_text(
        "SELECT id FROM subscribers WHERE event_feed_uuid LIKE :t"
    ), {"t": f"{DEMO_TAG}-%"}).fetchall()]
    if not sub_ids:
        print("[seed] no", DEMO_TAG, "demo rows found")
        return
    db.execute(sa_text(
        "DELETE FROM mrr_movements WHERE account_id IN "
        "(SELECT account_id FROM customer_accounts WHERE subscriber_id = ANY(:ids))"
    ), {"ids": sub_ids})
    db.execute(sa_text("DELETE FROM zip_territories WHERE subscriber_id = ANY(:ids)"), {"ids": sub_ids})
    db.execute(sa_text("DELETE FROM customer_accounts WHERE subscriber_id = ANY(:ids)"), {"ids": sub_ids})
    db.execute(sa_text("DELETE FROM subscribers WHERE id = ANY(:ids)"), {"ids": sub_ids})
    db.commit()
    print(f"[seed] removed {len(sub_ids)} {DEMO_TAG} demo subscribers (+ their accounts/movements/zips)")


def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _make_subscriber(db, *, cohort_month, tier, signup_source, utm_source, zip_code):
    tag = uuid.uuid4().hex[:10]
    sub = Subscriber(
        stripe_customer_id=f"cus_{DEMO_TAG}_{tag}",
        tier=tier, vertical="roofing", county_id="hillsborough", status="active",
        event_feed_uuid=f"{DEMO_TAG}-{tag}", email=f"{DEMO_TAG}_{tag}@example.com",
        created_at=cohort_month, signup_source=signup_source, utm_source=utm_source,
    )
    db.add(sub)
    db.flush()

    acct = CustomerAccount(subscriber_id=sub.id, status="active", mrr_cents=10000)
    db.add(acct)
    db.flush()

    if zip_code:
        db.add(ZipTerritory(
            zip_code=zip_code, vertical="roofing", county_id="hillsborough",
            subscriber_id=sub.id, status="locked",
        ))
        db.flush()

    return sub.id, acct.account_id


def _unique_zip(db) -> str:
    # zip_territories enforces one subscriber per (zip, vertical, county) —
    # a "9xxxx" prefix keeps demo ZIPs out of Hillsborough's real 33xxx range
    # and out of the reach of any pre-existing shared-DB row, so this always
    # lands on our own demo subscriber.
    while True:
        candidate = f"9{uuid.uuid4().int % 10000:04d}"
        exists = db.execute(sa_text(
            "SELECT 1 FROM zip_territories WHERE zip_code = :zip AND vertical = 'roofing' AND county_id = 'hillsborough'"
        ), {"zip": candidate}).scalar()
        if not exists:
            return candidate


def _movement(db, account_id, *, movement_type, at, delta=10000):
    db.add(MrrMovement(
        account_id=account_id, movement_type=movement_type,
        delta_cents=delta, mrr_after_cents=max(delta, 0), effective_at=at,
    ))
    db.flush()


def _seed(db) -> None:
    now = datetime.now(timezone.utc)
    # Three cohort months, each 1-3-5 months back, so the grid shows several
    # rows with different amounts of aging (older rows more filled in).
    cohorts = [_month_start(now - timedelta(days=d)) for d in (150, 90, 30)]

    # zip_territories enforces one subscriber per (zip, vertical, county) at a
    # time, so these are freshly generated + collision-checked, reserved for
    # the newest cohort so the ZIP filter has something concrete to isolate.
    demo_zips = [_unique_zip(db) for _ in range(3)]
    channels = [("direct", None), ("landing_page", None), (None, "facebook"), (None, "google")]
    tiers = ["pro", "starter", "dominator", "pro", "pro"]

    counts = {"subscribers": 0, "churned": 0, "reactivated": 0}

    for ci, cohort_month in enumerate(cohorts):
        zip_pool = iter(demo_zips) if ci == len(cohorts) - 1 else iter([])

        # 6 never-churned, 2 churned outright, 2 reactivated per cohort —
        # comfortably above the min-cohort-size-5 suppression floor.
        for i in range(6):
            signup_source, utm_source = channels[i % len(channels)]
            tier = tiers[i % len(tiers)]
            zip_code = next(zip_pool, None)
            _, acct = _make_subscriber(
                db, cohort_month=cohort_month, tier=tier,
                signup_source=signup_source, utm_source=utm_source, zip_code=zip_code,
            )
            _movement(db, acct, movement_type="new", at=cohort_month)
            counts["subscribers"] += 1

        for i in range(2):
            signup_source, utm_source = channels[i % len(channels)]
            _, acct = _make_subscriber(
                db, cohort_month=cohort_month, tier="starter",
                signup_source=signup_source, utm_source=utm_source, zip_code=next(zip_pool, None),
            )
            _movement(db, acct, movement_type="new", at=cohort_month)
            _movement(db, acct, movement_type="churn", at=cohort_month + timedelta(days=15), delta=-10000)
            counts["subscribers"] += 1
            counts["churned"] += 1

        for i in range(2):
            signup_source, utm_source = channels[i % len(channels)]
            _, acct = _make_subscriber(
                db, cohort_month=cohort_month, tier="dominator",
                signup_source=signup_source, utm_source=utm_source, zip_code=next(zip_pool, None),
            )
            _movement(db, acct, movement_type="new", at=cohort_month)
            _movement(db, acct, movement_type="churn", at=cohort_month + timedelta(days=10), delta=-10000)
            _movement(db, acct, movement_type="new", at=cohort_month + timedelta(days=20), delta=10000)
            counts["subscribers"] += 1
            counts["reactivated"] += 1

    db.commit()
    print(
        f"[seed] inserted {counts['subscribers']} demo subscribers across "
        f"{len(cohorts)} cohort months "
        f"({counts['churned']} churned, {counts['reactivated']} reactivated). "
        "Filters to try: tier=pro / tier=starter / tier=dominator, "
        "channel=direct / channel=meta (facebook normalizes to 'meta'), "
        f"zip={' / '.join(demo_zips)} (newest cohort only, one subscriber each)."
    )


def main() -> None:
    clean = "--clean" in sys.argv
    with get_db_context() as db:
        _clean(db) if clean else None
        if not clean:
            _seed(db)


if __name__ == "__main__":
    main()
