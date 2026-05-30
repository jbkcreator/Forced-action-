"""
Scenario test: full T+0 → T+24 county launch sequence.

Markers: scenario_platform
Requires: real Postgres via fresh_db fixture (savepoint rollback per test).

Flow:
  county_launch_runner (status=approved → launched, launched_at set)
  → county_waitlist_notifier (T+0 email/SMS to waiting entries)
  → county_launch_pulse (T+24 revenue pulse, once)

All external calls (Slack, email, SMS, pulse SMS) are mocked.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.scenario_platform


@pytest.mark.skipif(
    True,
    reason="Requires Postgres with JSONB support — remove skipif to run against real DB",
)
class TestT0AndT24FullSequence:
    def test_full_launch_sequence(self, fresh_db, monkeypatch):
        """
        Seeds an approved candidate + one waitlist entry, runs runner → notifier
        → pulse (with clock advanced 25h), asserts all guards stamped and audit
        trail complete.
        """
        from src.core.models import (
            CountyLaunchAudit,
            ExpansionCandidate,
            WaitlistEntry,
        )
        from src.tasks.county_launch_runner import run_county_launch_runner
        from src.tasks.county_waitlist_notifier import run_waitlist_notifier
        from src.tasks.county_launch_pulse import run_county_launch_pulse

        db = fresh_db
        county_id = "pinellas_scenario_test"

        # ── Seed: approved expansion candidate ─────────────────────────────
        candidate = ExpansionCandidate(
            county_id=county_id,
            priority=10,
            status="approved",
            approved_at=datetime.now(timezone.utc),
            approved_by_slack_user="U_TEST",
            last_slack_message_ts="111.222",
        )
        db.add(candidate)
        db.flush()

        # ── Seed: one waitlist entry for this county ───────────────────────
        entry = WaitlistEntry(
            zip_code="33701",
            vertical="roofing",
            county_id=county_id,
            name="Scenario Tester",
            email="scenario@test.com",
            sms_opt_in=False,
            waitlist_type="coming_soon",
            status="waiting",
        )
        db.add(entry)
        db.commit()

        # ── Fake Redis: all gates green ────────────────────────────────────
        store = {}
        source = "hillsborough"
        for key, val in [
            ("first_payment_rate", "35.0"),
            ("saved_card_rate", "75.0"),
            ("wallet_adoption", "20.0"),
            ("lock_conversion", "7.0"),
            ("retention_30d", "75.0"),
            ("free_tier_cost_ratio", "35.0"),
            ("county_profitability", "1.0"),
        ]:
            store[f"fa:ks_metric:{source}:{key}"] = val

        monkeypatch.setattr("src.tasks.county_launch_runner.rget", lambda k: store.get(k))
        monkeypatch.setattr("src.tasks.county_launch_runner.redis_available", lambda: True)

        # Stub Slack
        mock_slack = MagicMock()
        mock_slack.chat_postMessage.return_value = {"ts": "111.222"}
        with patch("src.tasks.county_launch_runner.WebClient", return_value=mock_slack):
            pass  # runner doesn't re-post; just needs the token not to crash

        # ── Step 1: Run launch runner ──────────────────────────────────────
        # Patch get_db_context to use our fresh_db session
        from contextlib import contextmanager

        @contextmanager
        def _use_fresh_db():
            yield db

        monkeypatch.setattr("src.tasks.county_launch_runner.get_db_context", _use_fresh_db)

        result_runner = run_county_launch_runner(dry_run=False)
        assert result_runner.get("launched") is True

        db.refresh(candidate)
        assert candidate.status == "launched"
        assert candidate.launched_at is not None

        # ── Step 2: Run waitlist notifier (T+0) ───────────────────────────
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.get_db_context", _use_fresh_db)
        mock_email = MagicMock(return_value=True)
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_email", mock_email)
        monkeypatch.setattr("src.tasks.county_waitlist_notifier.send_sms", MagicMock())

        result_notifier = run_waitlist_notifier(dry_run=False)
        assert result_notifier["processed"][0]["sent_email"] == 1
        assert result_notifier["processed"][0]["failed"] == 0

        db.refresh(candidate)
        db.refresh(entry)
        assert candidate.waitlist_notified_at is not None
        assert entry.status == "notified"
        assert entry.notified_email_at is not None
        mock_email.assert_called_once()

        # ── Step 3: Advance clock 25h, run T+24 pulse ─────────────────────
        # Force launched_at to be 25h ago so the pulse query matches
        candidate.launched_at = datetime.now(timezone.utc) - timedelta(hours=25)
        db.commit()

        monkeypatch.setattr("src.tasks.county_launch_pulse.get_db_context", _use_fresh_db)
        mock_pulse = MagicMock(return_value={"sent": True, "message": "FA pulse..."})
        monkeypatch.setattr("src.tasks.county_launch_pulse.run_daily_pulse", mock_pulse)

        result_pulse = run_county_launch_pulse(dry_run=False)
        assert result_pulse["processed"][0]["sent"] is True
        assert result_pulse["processed"][0]["county_id"] == county_id

        db.refresh(candidate)
        assert candidate.revenue_pulse_sent_at is not None
        mock_pulse.assert_called_once_with(county_id=county_id, dry_run=False)

        # ── Step 4: Second pulse run is a no-op ───────────────────────────
        result_pulse2 = run_county_launch_pulse(dry_run=False)
        assert result_pulse2 == {"no_pending_counties": True}
        assert mock_pulse.call_count == 1  # not called again

        # ── Step 5: Verify audit trail ─────────────────────────────────────
        from sqlalchemy import select
        audit_rows = db.execute(
            select(CountyLaunchAudit)
            .where(CountyLaunchAudit.county_id == county_id)
            .order_by(CountyLaunchAudit.created_at)
        ).scalars().all()

        event_types = [r.event_type for r in audit_rows]
        assert "launch_started" in event_types
        assert "launched" in event_types
        assert "waitlist_notified" in event_types
        assert "revenue_pulse_sent" in event_types
