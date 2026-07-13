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


def _exec_results(mock_db, *results):
    """Make successive mock_db.execute(...) calls return these results in
    order (each already the object .first()/.scalar()/etc. is called on)."""
    mock_db.execute.side_effect = list(results)


class TestPeriodBounds:
    def test_no_prior_credit_starts_at_signup(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import _period_bounds, CYCLE_DAYS

        created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        _exec_results(
            mock_db,
            MagicMock(first=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
        )

        start, end = _period_bounds(mock_db, 1, created_at)

        assert start == created_at
        assert end == created_at + timedelta(days=CYCLE_DAYS)

    def test_naive_created_at_from_postgres_is_normalized(self, mock_db):
        """subscribers.created_at is a naive TIMESTAMP column in Postgres —
        must not raise when compared against an aware datetime downstream."""
        from src.tasks.guarantee_shortfall_sweep import _period_bounds, CYCLE_DAYS

        naive_created_at = datetime(2026, 1, 1)  # no tzinfo, as psycopg2 returns it
        _exec_results(
            mock_db,
            MagicMock(first=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=None)),
        )

        start, end = _period_bounds(mock_db, 1, naive_created_at)

        assert start.tzinfo is not None
        assert end.tzinfo is not None
        assert end - start == timedelta(days=CYCLE_DAYS)
        now_aware = datetime.now(timezone.utc)
        assert end < now_aware  # comparison must not raise

    def test_prior_credit_continues_from_last_resolved_period_end(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import _period_bounds, CYCLE_DAYS

        last_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
        _exec_results(
            mock_db,
            MagicMock(first=MagicMock(return_value=None)),
            MagicMock(scalar=MagicMock(return_value=last_end)),
        )

        start, end = _period_bounds(mock_db, 1, datetime(2026, 1, 1, tzinfo=timezone.utc))

        assert start == last_end
        assert end == last_end + timedelta(days=CYCLE_DAYS)

    def test_failed_period_is_retried_not_skipped(self, mock_db):
        """A 'failed' row must not advance the cursor past it — the same
        period should be handed back so the sweep retries it."""
        from src.tasks.guarantee_shortfall_sweep import _period_bounds

        failed_start = datetime(2026, 2, 1, tzinfo=timezone.utc)
        failed_end = datetime(2026, 3, 3, tzinfo=timezone.utc)
        _exec_results(
            mock_db,
            MagicMock(first=MagicMock(return_value=MagicMock(
                period_start=failed_start, period_end=failed_end,
            ))),
        )

        start, end = _period_bounds(mock_db, 1, datetime(2026, 1, 1, tzinfo=timezone.utc))

        assert start == failed_start
        assert end == failed_end
        # Only the unresolved-lookup query should run — the cursor must not
        # advance past the failed period to compute a later one.
        assert mock_db.execute.call_count == 1

    def test_pending_period_is_retried_not_skipped(self, mock_db):
        """A 'pending' row (crashed mid-Stripe-call) is retried the same way."""
        from src.tasks.guarantee_shortfall_sweep import _period_bounds

        pending_start = datetime(2026, 2, 1, tzinfo=timezone.utc)
        pending_end = datetime(2026, 3, 3, tzinfo=timezone.utc)
        _exec_results(
            mock_db,
            MagicMock(first=MagicMock(return_value=MagicMock(
                period_start=pending_start, period_end=pending_end,
            ))),
        )

        start, end = _period_bounds(mock_db, 1, datetime(2026, 1, 1, tzinfo=timezone.utc))

        assert start == pending_start
        assert end == pending_end


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
    def _patch_common(self, delivered, *, cycle_elapsed=True, reserve_row_id=101):
        now = datetime.now(timezone.utc)
        if cycle_elapsed:
            start, end = now - timedelta(days=31), now - timedelta(days=1)
        else:
            start, end = now - timedelta(days=1), now + timedelta(days=29)
        return patch.multiple(
            "src.tasks.guarantee_shortfall_sweep",
            _period_bounds=MagicMock(return_value=(start, end)),
            _delivered_count=MagicMock(return_value=delivered),
            _reserve_period=MagicMock(return_value=reserve_row_id),
            _resolve_period=MagicMock(),
        )

    def test_quota_met_no_stripe_call(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter")  # quota=10
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(delivered=10),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        mock_issue.assert_not_called()
        assert stats["checked"] == 1
        assert stats["shortfall"] == 0
        assert stats["credited"] == 0

    def test_shortfall_issues_prorated_credit_with_idempotency_key_and_email(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        # starter quota=10, delivered=5 -> shortfall=5, plan_price=$600 -> credit=$300
        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]
        period_end = datetime(2026, 3, 3, tzinfo=timezone.utc)  # closed, in the past

        with (
            patch.multiple(
                "src.tasks.guarantee_shortfall_sweep",
                _period_bounds=MagicMock(return_value=(datetime(2026, 2, 1, tzinfo=timezone.utc), period_end)),
                _delivered_count=MagicMock(return_value=5),
                _reserve_period=MagicMock(return_value=101),
                _resolve_period=MagicMock(),
            ),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit",
                  return_value="txn_abc") as mock_issue,
            patch("src.tasks.guarantee_shortfall_sweep.send_email") as mock_email,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        assert stats["shortfall"] == 1
        assert stats["credited"] == 1
        mock_issue.assert_called_once()
        args = mock_issue.call_args.args
        kwargs = mock_issue.call_args.kwargs
        assert args[0] == "cus_123"
        assert args[1] == 30000  # $300.00 in cents
        # Deterministic per (subscriber, period_end) — a retry of this same
        # cycle must reuse this exact key so Stripe dedupes instead of
        # crediting twice.
        assert kwargs["idempotency_key"] == "guarantee-credit-1-2026-03-03"
        mock_email.assert_called_once()

    def test_stripe_failure_marks_failed_no_email(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(delivered=5),
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
            self._patch_common(delivered=5),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        mock_issue.assert_not_called()
        assert stats["credited"] == 0

    def test_dry_run_never_calls_stripe_or_reserves(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(delivered=5),
            patch("src.tasks.guarantee_shortfall_sweep._reserve_period") as mock_reserve,
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db, dry_run=True)
            mock_reserve.assert_not_called()

        mock_issue.assert_not_called()
        assert stats["dry_run"] is True
        assert stats["shortfall"] == 1

    def test_unfinished_cycle_skipped(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(delivered=5, cycle_elapsed=False),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        mock_issue.assert_not_called()
        assert stats["checked"] == 0

    def test_lost_reservation_race_skips_without_crediting(self, mock_db):
        """If _reserve_period returns None, another (concurrent or prior) run
        already claimed or resolved this cycle — must not call Stripe again."""
        from src.tasks.guarantee_shortfall_sweep import run_guarantee_shortfall_sweep

        sub = _make_sub(tier="starter", plan_price=600)
        mock_db.execute.return_value.fetchall.return_value = [sub]

        with (
            self._patch_common(delivered=5, reserve_row_id=None),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit") as mock_issue,
        ):
            stats = run_guarantee_shortfall_sweep(mock_db)

        mock_issue.assert_not_called()
        assert stats["credited"] == 0
        assert stats["failed"] == 0


class TestEvaluateSubscriberGuarantee:
    """evaluate_subscriber_guarantee() is shared by the daily sweep and by
    /api/upgrade (settling a closed cycle on the outgoing tier before a plan
    switch) — exercise it directly for the tier-gating behavior."""

    def test_unguaranteed_tier_returns_none(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import evaluate_subscriber_guarantee

        sub = _make_sub(tier="autopilot_pro")  # not in TIER_LEAD_QUOTAS
        result = evaluate_subscriber_guarantee(mock_db, sub)

        assert result is None

    def test_settles_closed_cycle_on_guaranteed_tier(self, mock_db):
        from src.tasks.guarantee_shortfall_sweep import evaluate_subscriber_guarantee

        now = datetime.now(timezone.utc)
        sub = _make_sub(tier="pro", plan_price=1100)  # quota=20

        with (
            patch.multiple(
                "src.tasks.guarantee_shortfall_sweep",
                _period_bounds=MagicMock(return_value=(now - timedelta(days=31), now - timedelta(days=1))),
                _delivered_count=MagicMock(return_value=10),
                _reserve_period=MagicMock(return_value=55),
                _resolve_period=MagicMock(),
            ),
            patch("src.tasks.guarantee_shortfall_sweep.issue_guarantee_credit",
                  return_value="txn_xyz"),
            patch("src.tasks.guarantee_shortfall_sweep.send_email"),
        ):
            result = evaluate_subscriber_guarantee(mock_db, sub)

        assert result == {"shortfall": True, "status": "issued"}
