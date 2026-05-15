"""
Unit tests for content-quality rules in src/tasks/anomaly_pager.py.

Covers:
  - _rule_scraper_duplicate_rate — fires when >=95% of today's batch existed
    yesterday; skips small batches; skips when yesterday is empty
  - _rule_scraper_field_coverage_drop — fires when a required field's non-null
    rate drops >30pp vs 7-day baseline; skips small batches; needs >=3 history
  - Sunday off-day skip for the four M-Sat sources
  - Soft-launch gate: SHIP_CONTENT_QUALITY_ALERTS=0 logs trips but suppresses
    email send and scraper_alert_log writes

Run:
    pytest tests/test_anomaly_pager_content_quality.py -v
"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker


# Register SQLite type adapters once at import — Postgres-specific column types
# can't compile against SQLite otherwise. Has to run before Base.metadata is
# touched so the engine compiler sees the patched types.
@compiles(JSONB, "sqlite")
def _jsonb_to_json(type_, compiler, **kw):  # noqa: D401
    return "JSON"


@compiles(ARRAY, "sqlite")
def _array_to_text(type_, compiler, **kw):  # noqa: D401
    return "TEXT"


from src.core.models import (  # noqa: E402
    Base,
    BuildingPermit,
    CodeViolation,
    Foreclosure,
    LegalAndLien,
    LegalProceeding,
    Property,
    ScraperAlertLog,
)
from src.tasks import anomaly_pager  # noqa: E402
from src.tasks.anomaly_pager import (  # noqa: E402
    SOFT_LAUNCH_RULES,
    _is_soft_launched,
    _rule_scraper_duplicate_rate,
    _rule_scraper_field_coverage_drop,
    _rule_scraper_filing_date_not_advancing,
    run_and_page,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def session():
    """Function-scoped SQLite in-memory DB with full schema."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()
        engine.dispose()


def _property(session, prop_id: int) -> Property:
    """Insert one Property row (required FK target for all scraper rows)."""
    p = Property(id=prop_id, parcel_id=f"PARCEL-{prop_id:04d}", county_id="hillsborough")
    session.add(p)
    session.flush()
    return p


def _foreclosure(session, case_no: str, dt: date, *, plaintiff="Bank", auction_date=None, case_status="open", property_id=1):
    f = Foreclosure(
        property_id=property_id,
        case_number=case_no,
        plaintiff=plaintiff,
        auction_date=auction_date or datetime(2026, 1, 1),
        case_status=case_status,
        date_added=dt,
    )
    session.add(f)
    return f


def _violation(session, rec_no: str, dt: date, *, violation_type="trash", description="x", status="open", property_id=1):
    v = CodeViolation(
        property_id=property_id,
        record_number=rec_no,
        violation_type=violation_type,
        description=description,
        status=status,
        date_added=dt,
    )
    session.add(v)
    return v


# ---------------------------------------------------------------------------
# Duplicate-rate rule
# ---------------------------------------------------------------------------

class TestDuplicateRateRule:
    """Validates _rule_scraper_duplicate_rate. Use a Wed date so we steer
    clear of RULE_OFF_DAYS for any of the five sources."""

    TODAY = date(2026, 5, 13)        # Wednesday
    YESTERDAY = date(2026, 5, 12)    # Tuesday

    def test_fires_when_all_keys_repeat(self, session):
        _property(session, 1)
        for i in range(10):
            _foreclosure(session, f"REPEAT-{i}", self.YESTERDAY)
            _foreclosure(session, f"REPEAT-{i}-today", self.TODAY)  # dummy to bump id
        # Now also add today's rows with the same case_numbers as yesterday
        # (different ids since case_number unique-by-source — actually
        # case_number IS unique so we can't repeat. Simulate via separate
        # property to keep case_number unique-ish.) Use a different prop.
        # Simpler: just put yesterday's case_numbers AND today's at different
        # ids without violating unique constraint by giving them different
        # case_numbers but identical sets — but then they wouldn't intersect.
        # Real test: rewrite — use case_numbers that share between today
        # and yesterday by using distinct rows on different dates is
        # impossible if case_number is unique.
        #
        # Correct semantics: a real "duplicate rate" failure means the SAME
        # underlying records are being re-inserted across dates. With a
        # unique constraint on case_number, the DB rejects this. So the
        # bug-class we're detecting is: re-scrape produces the same set of
        # case_numbers, and the scraper would UPSERT (not re-insert). The
        # upsert path keeps the original `date_added` (when row was first
        # seen) and bumps something else. But our rule queries by
        # date_added — so if no rows have date_added=today, today's set is
        # empty, and the dup_rate query short-circuits via
        # DUPLICATE_RATE_MIN_TODAY_ROWS.
        #
        # That's actually load_validator's zero-record check, not ours.
        # For OUR rule, the bug-class we catch is: scraper writes today's
        # batch with new ids but matching case_numbers from yesterday by
        # *changing* date_added. SQLite/Postgres can't have both rows
        # because of unique constraint.
        #
        # To unit-test the rule, we have to bypass the unique constraint
        # by using non-foreclosure tables OR by accepting that the test
        # uses the rule's pure set-intersection logic.
        #
        # Pragmatic test: insert today's rows and yesterday's rows with
        # DIFFERENT case_numbers that happen to share a prefix → confirm
        # NO trip. Then insert today's rows whose case_numbers exactly
        # match a separately-inserted yesterday cohort — only possible
        # if we delete and re-add with different ids. SQLite doesn't
        # enforce unique on insert-with-different-id-but-same-case_number?
        # It does. So we need to use case_numbers that genuinely overlap
        # via a separate dataset.
        #
        # Cleanest: skip this exact test, use the violations table since
        # its record_number is also unique. Both have the same issue.
        #
        # FINAL DECISION: the only way to genuinely simulate the
        # duplicate-rate failure mode is to have the SAME case_number
        # appear on two different date_added values. With a unique
        # constraint at the DB layer, this is structurally impossible
        # in this codebase. So the duplicate-rate rule effectively
        # detects: "today's case_numbers were ALREADY seen yesterday"
        # which means today's date_added is wrong (the scraper marked
        # an old row with today's date). Since unique constraint
        # prevents two rows with same case_number, the duplicate-rate
        # scenario requires UPDATE date_added=today on existing rows
        # — which a broken scraper using upsert WOULD do.
        #
        # Test this by INSERTING with date_added=yesterday, then
        # UPDATE date_added=today on the same rows. After update,
        # all of today's rows have case_numbers that were "yesterday's"
        # before — but yesterday's set is now empty. So dup_rate
        # against yesterday = 0 (yesterday has nothing). Not what we
        # want either.
        #
        # The most realistic simulation: scraper runs today + yesterday
        # both wrote SEPARATE batches with OVERLAPPING case_numbers
        # but the unique constraint forces upsert behavior — meaning
        # the row already exists from yesterday and only gets its
        # date_added bumped to today. After this, NO rows have
        # date_added=yesterday for those case_numbers. dup_rate=0.
        #
        # Conclusion: in this codebase, the rule as written CANNOT
        # fire from a real scraper behavior because of the unique
        # constraint. The rule fires only when:
        #   1. Scraper writes today's batch WITHOUT upserting (rare,
        #      would crash on unique violation), AND
        #   2. The same case_numbers are independently in yesterday's
        #      batch (only possible without unique constraint).
        #
        # The TEST for this rule must therefore use a synthetic
        # setup that violates the model's unique constraint OR
        # uses a different schema. For the unit test, we'll skip
        # the real models and mock the session query layer instead.
        pytest.skip(
            "Foreclosure.case_number unique constraint prevents writing the "
            "same key on two different date_added values in real data. The "
            "rule's set-intersection logic is verified via mocked-session "
            "tests below."
        )


class TestDuplicateRateRuleMocked:
    """Verify rule logic without hitting the unique-constraint wall."""

    TODAY = date(2026, 5, 13)
    YESTERDAY = date(2026, 5, 12)

    def _mock_session_with_keys(self, source_name, today_keys, yesterday_keys):
        """Build a mock session that returns the given key sets when queried
        for today's and yesterday's batches for `source_name`."""
        from src.tasks.anomaly_pager import CONTENT_QUALITY_SOURCES
        from unittest.mock import MagicMock

        model, key_attr, _fields = CONTENT_QUALITY_SOURCES[source_name]
        key_col = getattr(model, key_attr)
        date_col = model.date_added

        session = MagicMock()

        # Each .query(col).filter(date_col == X).all() call returns a list
        # of (val,) tuples. The rule uses .filter(date_col == today/yesterday).
        # We hook on the value passed to .filter() to return the right set.
        def query_side_effect(*args, **kwargs):
            q = MagicMock()
            def filter_side_effect(criterion):
                f = MagicMock()
                # Inspect the BinaryExpression's right-side literal to decide
                # which day's keys to return.
                right_val = getattr(getattr(criterion, "right", None), "value", None)
                if right_val == self.TODAY:
                    f.all.return_value = [(k,) for k in today_keys]
                elif right_val == self.YESTERDAY:
                    f.all.return_value = [(k,) for k in yesterday_keys]
                else:
                    f.all.return_value = []
                return f
            q.filter.side_effect = filter_side_effect
            return q
        session.query.side_effect = query_side_effect
        return session

    def test_fires_when_dup_rate_above_threshold(self):
        # 10 keys today, 10 of them existed yesterday → 100% dup rate.
        keys = [f"CASE-{i}" for i in range(10)]
        session = self._mock_session_with_keys("foreclosures", keys, keys)
        trips = list(_rule_scraper_duplicate_rate(session, self.TODAY))
        # Only foreclosures matters here; ignore trips from other sources
        # (the mock returns the same key set for any source, but volumes are
        # consistent so all five should fire — we filter to foreclosures).
        foreclosure_trips = [t for t in trips if t.context["source"] == "foreclosures"]
        assert len(foreclosure_trips) == 1
        t = foreclosure_trips[0]
        assert t.rule == "scraper_duplicate_rate_high"
        assert t.context["duplicate_count"] == 10
        assert t.context["today_count"] == 10
        assert t.context["duplicate_rate"] == 1.0

    def test_no_fire_when_keys_are_new(self):
        session = self._mock_session_with_keys(
            "foreclosures",
            today_keys=[f"NEW-{i}" for i in range(10)],
            yesterday_keys=[f"OLD-{i}" for i in range(10)],
        )
        trips = list(_rule_scraper_duplicate_rate(session, self.TODAY))
        assert [t for t in trips if t.context["source"] == "foreclosures"] == []

    def test_skips_when_today_below_min_rows(self):
        # 3 keys today is below DUPLICATE_RATE_MIN_TODAY_ROWS (5)
        session = self._mock_session_with_keys(
            "foreclosures",
            today_keys=[f"K-{i}" for i in range(3)],
            yesterday_keys=[f"K-{i}" for i in range(3)],
        )
        trips = list(_rule_scraper_duplicate_rate(session, self.TODAY))
        assert [t for t in trips if t.context["source"] == "foreclosures"] == []

    def test_skips_when_yesterday_empty(self):
        session = self._mock_session_with_keys(
            "foreclosures",
            today_keys=[f"K-{i}" for i in range(10)],
            yesterday_keys=[],
        )
        trips = list(_rule_scraper_duplicate_rate(session, self.TODAY))
        assert [t for t in trips if t.context["source"] == "foreclosures"] == []


# ---------------------------------------------------------------------------
# Field-coverage rule — uses real SQLite
# ---------------------------------------------------------------------------

class TestFieldCoverageRule:
    """Field-coverage uses per-day COUNT queries and works against real rows
    because no unique-constraint blocks the scenario."""

    TODAY = date(2026, 5, 13)        # Wednesday — no off-day for any source

    def _seed_violations(self, session, dt: date, n: int, blank_description: bool = False):
        """Seed n violation rows on date `dt`. Each on its own Property."""
        for i in range(n):
            prop_id = i * 100 + int(dt.toordinal())
            _property(session, prop_id)
            _violation(
                session,
                rec_no=f"REC-{dt}-{i}",
                dt=dt,
                description="" if blank_description else "real description text",
                property_id=prop_id,
            )
        session.flush()

    def test_fires_when_field_coverage_drops_sharply(self, session):
        # 7 days of baseline at 100% description coverage, today at 0%.
        for offset in range(1, 8):
            day = self.TODAY - timedelta(days=offset)
            self._seed_violations(session, day, n=15, blank_description=False)
        self._seed_violations(session, self.TODAY, n=15, blank_description=True)

        trips = list(_rule_scraper_field_coverage_drop(session, self.TODAY))
        v_trips = [t for t in trips if t.context["source"] == "violations" and t.context["field"] == "description"]
        assert len(v_trips) == 1
        t = v_trips[0]
        assert t.rule == "scraper_field_coverage_drop"
        assert t.context["today_pct"] == 0.0
        assert t.context["baseline_pct"] == 100.0

    def test_no_fire_when_coverage_stable(self, session):
        for offset in range(1, 8):
            self._seed_violations(session, self.TODAY - timedelta(days=offset), n=15, blank_description=False)
        self._seed_violations(session, self.TODAY, n=15, blank_description=False)

        trips = list(_rule_scraper_field_coverage_drop(session, self.TODAY))
        assert not any(t.context.get("source") == "violations" for t in trips)

    def test_skips_when_today_below_min_rows(self, session):
        for offset in range(1, 8):
            self._seed_violations(session, self.TODAY - timedelta(days=offset), n=15, blank_description=False)
        # Today: only 5 rows — below FIELD_COVERAGE_MIN_ROWS (10)
        self._seed_violations(session, self.TODAY, n=5, blank_description=True)

        trips = list(_rule_scraper_field_coverage_drop(session, self.TODAY))
        assert not any(t.context.get("source") == "violations" for t in trips)

    def test_skips_when_insufficient_history(self, session):
        # Only 2 days of history (rule needs >=3)
        self._seed_violations(session, self.TODAY - timedelta(days=1), n=15, blank_description=False)
        self._seed_violations(session, self.TODAY - timedelta(days=2), n=15, blank_description=False)
        self._seed_violations(session, self.TODAY, n=15, blank_description=True)

        trips = list(_rule_scraper_field_coverage_drop(session, self.TODAY))
        assert not any(t.context.get("source") == "violations" for t in trips)


# ---------------------------------------------------------------------------
# Filing-date freshness rule
# ---------------------------------------------------------------------------

class TestFilingDateNotAdvancing:
    """Validates _rule_scraper_filing_date_not_advancing using real SQLite."""

    TODAY = date(2026, 5, 13)        # Wednesday — no off-day

    def _seed_violations_with_opened(self, session, dt: date, n: int, opened_date: date):
        """Insert n violations on date_added=dt, all with the given opened_date."""
        for i in range(n):
            prop_id = i * 1000 + int(dt.toordinal())
            _property(session, prop_id)
            v = CodeViolation(
                property_id=prop_id,
                record_number=f"FILING-{dt}-{i}",
                violation_type="trash",
                description="text",
                status="open",
                opened_date=opened_date,
                date_added=dt,
            )
            session.add(v)
        session.flush()

    def test_fires_when_filing_date_stuck(self, session):
        # Day-2 + Day-1 + today all have opened_date = same old date.
        stuck = date(2025, 8, 12)
        self._seed_violations_with_opened(session, self.TODAY - timedelta(days=2), n=10, opened_date=stuck)
        self._seed_violations_with_opened(session, self.TODAY - timedelta(days=1), n=10, opened_date=stuck)
        self._seed_violations_with_opened(session, self.TODAY, n=10, opened_date=stuck)

        trips = list(_rule_scraper_filing_date_not_advancing(session, self.TODAY))
        v_trips = [t for t in trips if t.context.get("source") == "violations"]
        assert len(v_trips) == 1
        t = v_trips[0]
        assert t.rule == "scraper_filing_date_not_advancing"
        assert t.context["today_count"] == 10

    def test_no_fire_when_filing_date_advancing(self, session):
        # Healthy: each day's max advances by 1 day.
        for offset in range(2, 0, -1):
            day_added = self.TODAY - timedelta(days=offset)
            day_opened = self.TODAY - timedelta(days=offset + 1)
            self._seed_violations_with_opened(session, day_added, n=10, opened_date=day_opened)
        # Today: max(opened_date) = yesterday, which is newer than the prior days.
        self._seed_violations_with_opened(session, self.TODAY, n=10, opened_date=self.TODAY - timedelta(days=1))

        trips = list(_rule_scraper_filing_date_not_advancing(session, self.TODAY))
        assert not any(t.context.get("source") == "violations" for t in trips)

    def test_skips_when_today_below_min_rows(self, session):
        stuck = date(2025, 8, 12)
        self._seed_violations_with_opened(session, self.TODAY - timedelta(days=2), n=10, opened_date=stuck)
        self._seed_violations_with_opened(session, self.TODAY - timedelta(days=1), n=10, opened_date=stuck)
        # Only 3 rows today (< FILING_DATE_MIN_TODAY_ROWS=5)
        self._seed_violations_with_opened(session, self.TODAY, n=3, opened_date=stuck)

        trips = list(_rule_scraper_filing_date_not_advancing(session, self.TODAY))
        assert not any(t.context.get("source") == "violations" for t in trips)

    def test_skips_when_no_prior_baseline(self, session):
        # Today alone, no priors → can't compute, skip.
        self._seed_violations_with_opened(session, self.TODAY, n=10, opened_date=date(2025, 8, 12))

        trips = list(_rule_scraper_filing_date_not_advancing(session, self.TODAY))
        assert not any(t.context.get("source") == "violations" for t in trips)


# ---------------------------------------------------------------------------
# Sunday off-day handling
# ---------------------------------------------------------------------------

class TestSundayOffDay:
    """M-Sat sources should be skipped on Sundays; foreclosures (daily) runs."""

    SUNDAY = date(2026, 5, 17)
    SATURDAY = date(2026, 5, 16)

    def test_violations_skipped_on_sunday(self, session):
        # Seed a clear field-coverage drop on Sunday — but the rule should
        # not yield a Trip for violations because Sunday is in its off-days.
        for offset in range(1, 8):
            for i in range(15):
                prop_id = i + offset * 1000
                _property(session, prop_id)
                _violation(session, rec_no=f"R-{offset}-{i}",
                           dt=self.SUNDAY - timedelta(days=offset),
                           description="real text",
                           property_id=prop_id)
        for i in range(15):
            _property(session, 99000 + i)
            _violation(session, rec_no=f"SUN-{i}",
                       dt=self.SUNDAY,
                       description="",
                       property_id=99000 + i)
        session.flush()

        trips = list(_rule_scraper_field_coverage_drop(session, self.SUNDAY))
        assert not any(t.context.get("source") == "violations" for t in trips)

    def test_foreclosures_not_skipped_on_sunday(self, session):
        # Foreclosures runs every day → RULE_OFF_DAYS["foreclosures"] is empty.
        # Set up a clean coverage drop and confirm the rule yields a Trip.
        for offset in range(1, 8):
            for i in range(15):
                prop_id = i + offset * 1000
                _property(session, prop_id)
                _foreclosure(session, case_no=f"F-{offset}-{i}",
                             dt=self.SUNDAY - timedelta(days=offset),
                             plaintiff="Real Bank",
                             property_id=prop_id)
        for i in range(15):
            _property(session, 99000 + i)
            _foreclosure(session, case_no=f"FSUN-{i}",
                         dt=self.SUNDAY,
                         plaintiff="",
                         property_id=99000 + i)
        session.flush()

        trips = list(_rule_scraper_field_coverage_drop(session, self.SUNDAY))
        f_trips = [t for t in trips
                   if t.context.get("source") == "foreclosures"
                   and t.context.get("field") == "plaintiff"]
        assert len(f_trips) == 1


# ---------------------------------------------------------------------------
# Soft-launch gate
# ---------------------------------------------------------------------------

class TestSoftLaunchGate:
    """SHIP_CONTENT_QUALITY_ALERTS=0 must suppress email + scraper_alert_log."""

    def test_is_soft_launched_default_off(self, monkeypatch):
        monkeypatch.delenv("SHIP_CONTENT_QUALITY_ALERTS", raising=False)
        for rule in SOFT_LAUNCH_RULES:
            assert _is_soft_launched(rule) is True
        # Non-soft-launch rules always pass through
        assert _is_soft_launched("scraper_volume_drop_50pct") is False

    def test_is_soft_launched_off_when_flag_set(self, monkeypatch):
        monkeypatch.setenv("SHIP_CONTENT_QUALITY_ALERTS", "1")
        for rule in SOFT_LAUNCH_RULES:
            assert _is_soft_launched(rule) is False

    def test_run_and_page_suppresses_soft_launch_dispatch(self, monkeypatch):
        """When the rule fires but soft-launch is on, send_alert is not called
        and no ScraperAlertLog row is written for the soft-launch rule."""
        monkeypatch.delenv("SHIP_CONTENT_QUALITY_ALERTS", raising=False)

        from src.tasks.anomaly_pager import Trip

        synthetic_trip = Trip(
            rule="scraper_duplicate_rate_high",
            observed="x",
            baseline="y",
            threshold="z",
            context={"source": "test", "duplicate_rate": 1.0},
        )

        with patch.object(anomaly_pager, "evaluate", return_value=[synthetic_trip]):
            with patch.object(anomaly_pager, "send_alert") as mock_send:
                with patch.object(anomaly_pager, "_record_paged") as mock_record:
                    result = run_and_page(today=date(2026, 5, 13))
                    assert result == [synthetic_trip]
                    mock_send.assert_not_called()
                    mock_record.assert_not_called()

    def test_run_and_page_dispatches_when_flag_on(self, monkeypatch):
        """When SHIP_CONTENT_QUALITY_ALERTS=1, soft-launch rule dispatches."""
        monkeypatch.setenv("SHIP_CONTENT_QUALITY_ALERTS", "1")

        from src.tasks.anomaly_pager import Trip

        synthetic_trip = Trip(
            rule="scraper_duplicate_rate_high",
            observed="x",
            baseline="y",
            threshold="z",
            context={"source": "test", "duplicate_rate": 1.0},
        )

        with patch.object(anomaly_pager, "evaluate", return_value=[synthetic_trip]):
            with patch.object(anomaly_pager, "_recently_paged", return_value=False):
                with patch.object(anomaly_pager, "send_alert") as mock_send:
                    with patch.object(anomaly_pager, "_record_paged") as mock_record:
                        run_and_page(today=date(2026, 5, 13))
                        mock_send.assert_called_once()
                        mock_record.assert_called_once()
