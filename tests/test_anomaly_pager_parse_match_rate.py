"""
Unit tests for the parse_match_rate_drop rule in src/tasks/anomaly_pager.py.

Detects a silently-broken public-record parser: total volume can look normal
while the matched/(matched+unmatched) ratio craters (e.g. a county changed its
HTML and the parser can no longer extract the owner/address). Mirrors the
fixture style of tests/test_anomaly_pager_content_quality.py.

Run:
    pytest tests/test_anomaly_pager_parse_match_rate.py -v
"""

from datetime import date, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


@compiles(JSONB, "sqlite")
def _jsonb_to_json(type_, compiler, **kw):  # noqa: D401
    return "JSON"


@compiles(ARRAY, "sqlite")
def _array_to_text(type_, compiler, **kw):  # noqa: D401
    return "TEXT"


from src.core.models import Base, ScraperRunStats  # noqa: E402
from src.tasks.anomaly_pager import _rule_parse_match_rate_drop  # noqa: E402


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    # Only the table this rule reads — full metadata.create_all() trips over
    # newer Postgres-only DDL (e.g. the `plans` table) under SQLite.
    ScraperRunStats.__table__.create(bind=engine)
    s = sessionmaker(bind=engine)()
    try:
        yield s
    finally:
        s.close()
        engine.dispose()


def _stats(session, source_type, run_date, matched, unmatched):
    session.add(ScraperRunStats(
        source_type=source_type, county_id="hillsborough", run_date=run_date,
        total_scraped=matched + unmatched, matched=matched, unmatched=unmatched,
        skipped=0, run_success=True,
    ))


TODAY = date(2026, 5, 13)


def _seed_healthy_history(session, source_type, days=7, matched=80, unmatched=20):
    for d in range(1, days + 1):
        _stats(session, source_type, TODAY - timedelta(days=d), matched, unmatched)


class TestParseMatchRateDrop:
    def test_fires_when_match_rate_craters(self, session):
        # 7 days at ~80% match, then today 20/200 = 10% → well below 60% of baseline.
        _seed_healthy_history(session, "deeds")
        _stats(session, "deeds", TODAY, matched=20, unmatched=180)
        session.flush()

        trips = list(_rule_parse_match_rate_drop(session, TODAY))
        assert len(trips) == 1
        assert trips[0].rule == "parse_match_rate_drop"
        assert "deeds" in str(trips[0].context.get("source_type"))

    def test_quiet_when_match_rate_healthy(self, session):
        _seed_healthy_history(session, "deeds")
        _stats(session, "deeds", TODAY, matched=78, unmatched=22)   # ~78%, normal
        session.flush()
        assert list(_rule_parse_match_rate_drop(session, TODAY)) == []

    def test_skips_below_min_sample(self, session):
        _seed_healthy_history(session, "probate")
        _stats(session, "probate", TODAY, matched=1, unmatched=4)   # only 5 today
        session.flush()
        assert list(_rule_parse_match_rate_drop(session, TODAY)) == []

    def test_skips_without_enough_history(self, session):
        _stats(session, "foreclosures", TODAY - timedelta(days=1), 80, 20)   # 1 day only
        _stats(session, "foreclosures", TODAY, matched=10, unmatched=190)
        session.flush()
        assert list(_rule_parse_match_rate_drop(session, TODAY)) == []
