"""
Live-data tests for the new content-quality rules in anomaly_pager.

Each scenario runs in its own session that is ALWAYS rolled back, so nothing
persists to the DB. The two scenarios run in separate sessions so seeded data
from one doesn't contaminate MAX/COUNT queries in the other.

Run:
    .venv/Scripts/python.exe -m scripts.test_content_quality_live
"""

from datetime import date, timedelta

from src.core.database import db
from src.core.models import CodeViolation, Property
from src.tasks.anomaly_pager import (
    _rule_scraper_field_coverage_drop,
    _rule_scraper_filing_date_not_advancing,
)

TODAY = date.today()
PREFIX = "CQTEST"
STUCK_DATE = date(2025, 8, 12)


def _test_field_coverage() -> None:
    """7 days of baseline with description populated; today's batch with
    description=NULL → expect field-coverage rule to fire."""
    session = db.get_session()
    try:
        prop_ids = []
        for i in range(15):
            p = Property(parcel_id=f"{PREFIX}-FC-{i:03d}", county_id="hillsborough")
            session.add(p)
            session.flush()
            prop_ids.append(p.id)
            for offset in range(1, 8):
                session.add(CodeViolation(
                    property_id=p.id,
                    record_number=f"{PREFIX}-FC-{offset}-{i}",
                    description="real description text",
                    opened_date=TODAY - timedelta(days=offset + 1),
                    date_added=TODAY - timedelta(days=offset),
                ))
        for i, pid in enumerate(prop_ids):
            session.add(CodeViolation(
                property_id=pid,
                record_number=f"{PREFIX}-FC-TODAY-{i}",
                description=None,
                opened_date=TODAY - timedelta(days=1),
                date_added=TODAY,
            ))
        session.flush()

        trips = [
            t for t in _rule_scraper_field_coverage_drop(session, TODAY)
            if t.context.get("source") == "violations"
            and t.context.get("field") == "description"
        ]
        print(f"\n[field-coverage] Trips for violations.description: {len(trips)}")
        for t in trips:
            print(f"  RULE FIRED: {t.observed}  vs  {t.baseline}")
        if not trips:
            print("  ⚠️  field-coverage did NOT fire.")
    finally:
        session.rollback()
        session.close()


def _test_filing_date_stuck() -> None:
    """3 days of violations all with opened_date frozen on STUCK_DATE → expect
    filing-date-not-advancing rule to fire."""
    session = db.get_session()
    try:
        for i in range(15):
            p = Property(parcel_id=f"{PREFIX}-FD-{i:03d}", county_id="hillsborough")
            session.add(p)
            session.flush()
            for offset in (2, 1, 0):
                session.add(CodeViolation(
                    property_id=p.id,
                    record_number=f"{PREFIX}-FD-{offset}-{i}",
                    description="real description text",
                    opened_date=STUCK_DATE,
                    date_added=TODAY - timedelta(days=offset),
                ))
        session.flush()

        trips = [
            t for t in _rule_scraper_filing_date_not_advancing(session, TODAY)
            if t.context.get("source") == "violations"
        ]
        print(f"\n[filing-date] Trips for violations.opened_date: {len(trips)}")
        for t in trips:
            print(f"  RULE FIRED: {t.observed}")
            print(f"  Baseline: {t.baseline}")
        if not trips:
            print("  ⚠️  filing-date rule did NOT fire.")
    finally:
        session.rollback()
        session.close()


def main() -> None:
    _test_field_coverage()
    _test_filing_date_stuck()
    print("\nBoth scenarios rolled back. Nothing persisted to DB.")


if __name__ == "__main__":
    main()
