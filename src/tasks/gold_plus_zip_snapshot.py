"""
gold_plus_zip_snapshot — Nightly Gold+ lead count aggregation per ZIP.

Runs at 07:30 UTC, after CDS scoring (07:00). Upserts one row per
(zip_code, county_id) into gold_plus_zip_snapshots for today's date.

Sold-out reactivation eligibility reads this table as a fast pre-filter:
if a ZIP shows zero new Gold+ leads today, no reactivation messages are sent.
The full exclusivity+sold-out check still runs per-subscriber in the eligibility
layer; this snapshot gates the expensive per-subscriber pass.

Run:
    python -m src.tasks.gold_plus_zip_snapshot
"""

import logging
import sys
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.database import get_db_context

logger = logging.getLogger(__name__)


def run_snapshot(db: Session) -> dict:
    today = date.today()
    today_start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    rows = db.execute(text("""
        SELECT
            p.zip       AS zip_code,
            p.county_id AS county_id,
            COUNT(DISTINCT p.id) AS gold_plus_lead_count
        FROM distress_scores ds
        JOIN properties p ON p.id = ds.property_id
        WHERE ds.lead_tier IN ('Gold', 'Platinum', 'Ultra Platinum')
          AND ds.score_date >= :today_start
          AND ds.score_date < :today_start + INTERVAL '1 day'
          AND p.zip IS NOT NULL
          AND p.county_id IS NOT NULL
        GROUP BY p.zip, p.county_id
    """), {"today_start": today_start}).mappings().all()

    if not rows:
        logger.info("gold_plus_zip_snapshot: no Gold+ leads scored for %s", today)
        return {"upserted": 0, "date": str(today)}

    for row in rows:
        db.execute(text("""
            INSERT INTO gold_plus_zip_snapshots
                (zip_code, county_id, snapshot_date, gold_plus_lead_count, computed_at)
            VALUES
                (:zip, :county, :today, :count, :computed_at)
            ON CONFLICT (zip_code, county_id, snapshot_date)
            DO UPDATE SET
                gold_plus_lead_count = EXCLUDED.gold_plus_lead_count,
                computed_at          = EXCLUDED.computed_at
        """), {
            "zip": row["zip_code"],
            "county": row["county_id"],
            "today": today,
            "count": row["gold_plus_lead_count"],
            "computed_at": now,
        })

    db.commit()
    logger.info(
        "gold_plus_zip_snapshot: upserted %d ZIP rows for %s", len(rows), today
    )
    return {"upserted": len(rows), "date": str(today)}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stdout,
    )
    with get_db_context() as db:
        result = run_snapshot(db)
    print(result)
    sys.exit(0)
