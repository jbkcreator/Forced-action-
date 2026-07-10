"""Unit tests for the tiered volume guarantee shortfall sweep."""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


def _make_sub(**kwargs):
    defaults = dict(
        id=1, tier="starter", plan_price=600, stripe_customer_id="cus_123",
        email="sub@test.com", name="Jane", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    defaults.update(kwargs)
    return MagicMock(**defaults)


class TestPeriodBounds:
    def test_no_prior_credit_starts_at_signup(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import _period_bounds, CYCLE_DAYS

        created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        mock_db.execute.return_value.scalar.return_value = None

        start, end = _period_bounds(mock_db, 1, created_at)

        assert start == created_at
        assert end == created_at + timedelta(days=CYCLE_DAYS)

    def test_naive_created_at_from_postgres_is_normalized(self, mock_db):
        """subscribers.created_at is a naive TIMESTAMP column in Postgres —
        must not raise when compared against an aware datetime downstream."""
        from src.tasks.guarantee_shortfall_sweep import _period_bounds, CYCLE_DAYS

        naive_created_at = datetime(2026, 1, 1)  # no tzinfo, as psycopg2 returns it
        mock_db.execute.return_value.scalar.return_value = None

        start, end = _period_bounds(mock_db, 1, naive_created_at)

        assert start.tzinfo is not None
        assert end.tzinfo is not None
        assert end - start == timedelta(days=CYCLE_DAYS)
        now_aware = datetime.now(timezone.utc)
        assert end < now_aware  # comparison must not raise

    def test_prior_credit_continues_from_last_period_end(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import _period_bounds, CYCLE_DAYS

        last_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        mock_db.execute.return_value.scalar.return_value = last_end

        start, end = _period_bounds(mock_db, 1, datetime(2026, 1, 1, tzinfo=timezone.utc))

        assert start == last_end
        assert end == last_end + timedelta(days=CYCLE_DAYS)


class TestDeliveredCount:
    def test_returns_int_from_scalar(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import _delivered_count

        mock_db.execute.return_value.scalar.return_value = 7
        result = _delivered_count(mock_db, 1, datetime(2026, 1, 1, tzinfo=timezone.utc),
                                   datetime(2026, 1, 31, tzinfo=timezone.utc))
        assert result == 7

    def test_none_scalar_returns_zero(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import _delivered_count

        mock_db.execute.return_value.scalar.return_value = None
        result = _delivered_count(mock_db, 1, datetime(2026, 1, 1, tzinfo=timezone.utc),
                                   datetime(2026, 1, 31, tzinfo=timezone.utc))
        assert result == 0


class TestGuaranteeSweep:
    def _patch_common(self, sub, delivered, *, cycle_elapsed=True):
        now = datetime.now(timezone.utc)
        if cycle_elapsed:
            start, end = now - timedelta(days=31), now - timedelta(days=1)
        else:
            start, end = now - timedelta(days=1), now + timedelta(days=29)
        return patch.multiple(
            "src.tasks.guarantee_shortfall_sweep",
            _period_bounds=MagicMock(return_value=(start, end)),
            _delivered_count=MagicMock(return_value=delivered),
        )

    def test_quota_met_no_stripe_call(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter")  # quota=10
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(sub, delivered=10),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        mock_issue.assert_not_called()
        assert stats["checked"] == 1
        assert stats["shortfall"] == 0
        assert stats["credited"] == 0

    def test_shortfall_issues_prorated_credit_and_email(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        # starter quota=10, delivered=5 -> shortfall=5, plan_price=$600 -> credit=$300
        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(sub, delivered=5),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit",
                  return_value="txn_abc") as mock_issue,
            patch("src.tasks.guarantee_shortfall_sweep.send_email") as mock_email,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        assert stats["shortfall"] == 1
        assert stats["credited"] == 1
        mock_issue.assert_called_once()
        _, kwargs = mock_issue.call_args
        args = mock_issue.call_args.args
        assert args[0] == "cus_123"
        assert args[1] == 30000  # $300.00 in cents
        mock_email.assert_called_once()

    def test_stripe_failure_marks_failed_no_email(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(sub, delivered=5),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit",
                  side_effect=RuntimeError("stripe down")),
            patch("src.tasks.guarantee_shortfall_sweep.send_email") as mock_email,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        assert stats["failed"] == 1
        assert stats["credited"] == 0
        mock_email.assert_not_called()

    def test_no_stripe_customer_skips_charge(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600, stripe_customer_id=None)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(sub, delivered=5),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        mock_issue.assert_not_called()
        assert stats["credited"] == 0

    def test_dry_run_never_calls_stripe_or_commits(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(sub, delivered=5),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db, dry_run=True)

        mock_issue.assert_not_called()
        mock_db.commit.assert_not_called()
        assert stats["dry_run"] is True
        assert stats["shortfall"] == 1

    def test_unfinished_cycle_skipped(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(sub, delivered=5, cycle_elapsed=False),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        mock_issue.assert_not_called()
        assert stats["checked"] == 0
