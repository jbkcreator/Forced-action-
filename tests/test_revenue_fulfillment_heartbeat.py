"""Unit tests for the MB2 daily revenue & fulfillment heartbeat.

Follows the pattern used for src/tasks/guarantee_shortfall_sweep.py's tests:
patch out the individual DB-query helper functions and test build_report's
own orchestration (mismatch computation, exception-list population) rather
than re-testing each helper's raw SQL, which is simple GROUP BY aggregation
already exercised indirectly by the real dry-run against the shared DB.
"""
from contextlib import contextmanager
from datetime import date
from unittest.mock import MagicMock, patch

import pytest


def _empty_ledger():
    return {s: {"revenue_cents": 0, "count": 0} for s in
            ("subscription", "lead_unlock", "hot_lead_unlock", "lead_pack", "premium_report", "premium_brief")}


def _empty_refunds_disputes():
    return {
        "refunded_cents": 0, "refunded_count": 0, "disputes": [], "dispute_count": 0,
        "disputed_cents": 0, "dispute_fetch_error": None,
    }


def _empty_lead_pack():
    return {"by_status": {}, "stuck": []}


def _empty_premium():
    return {"by_status": {}}


def _empty_bankruptcy():
    return {
        "revenue_cents": 0, "invoice_count": 0, "configured": True, "error": None,
        "active_subscriptions": 0, "alerts_by_status": {},
    }


def _empty_loan_lane():
    return {"lanes_entered": 0, "lanes_funded": 0, "commissions_by_status": {}}


def _stripe_net(cents=0, charge_count=0, refund_count=0, configured=True, error=None):
    return {
        "stripe_net_cents": cents, "charge_count": charge_count,
        "refund_count": refund_count, "configured": configured, "error": error,
    }


@pytest.fixture
def mock_get_db_context():
    session = MagicMock()

    @contextmanager
    def _ctx():
        yield session

    return _ctx


class TestBuildReportOrchestration:
    def _patch_all(self, mock_get_db_context, **overrides):
        defaults = dict(
            _ledger_totals_by_stream=MagicMock(return_value=_empty_ledger()),
            _refunds_disputes=MagicMock(return_value=_empty_refunds_disputes()),
            _lead_pack_fulfillment=MagicMock(return_value=_empty_lead_pack()),
            _premium_fulfillment=MagicMock(return_value=_empty_premium()),
            _unfulfilled_lead_unlock_charges=MagicMock(return_value=[]),
            _bankruptcy_section=MagicMock(return_value=_empty_bankruptcy()),
            _loan_lane_section=MagicMock(return_value=_empty_loan_lane()),
            compute_stripe_net_revenue=MagicMock(return_value=_stripe_net()),
            get_db_context=mock_get_db_context,
        )
        defaults.update(overrides)
        return patch.multiple("src.tasks.revenue_fulfillment_heartbeat", **defaults)

    def test_clean_day_zero_mismatch_no_exceptions(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(mock_get_db_context):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["mismatch_cents"] == 0
        assert report["errors"] == []

    def test_matched_ledger_and_stripe_totals_yield_zero_mismatch(self, mock_get_db_context):
        """Ledger + bankruptcy revenue exactly matching Stripe's net must not
        alert — this is the ordinary "everything reconciled" case."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        ledger = _empty_ledger()
        ledger["subscription"] = {"revenue_cents": 60000, "count": 1}
        bankruptcy = _empty_bankruptcy()
        bankruptcy["revenue_cents"] = 29700

        with self._patch_all(
            mock_get_db_context,
            _ledger_totals_by_stream=MagicMock(return_value=ledger),
            _bankruptcy_section=MagicMock(return_value=bankruptcy),
            compute_stripe_net_revenue=MagicMock(return_value=_stripe_net(cents=89700, charge_count=2)),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["mismatch_cents"] == 0
        assert report["errors"] == []

    def test_same_day_refund_scenario_nets_to_zero_mismatch(self, mock_get_db_context):
        """Regression for the design-validation fix: compute_stripe_net_revenue
        already nets refunds against charges (tested directly in
        test_stripe_reconcile.py) — this confirms build_report trusts that
        net figure as-is rather than re-deriving/double-subtracting refunds
        against a ledger total that separately excludes refunded rows."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        ledger = _empty_ledger()
        ledger["lead_unlock"] = {"revenue_cents": 500, "count": 1}
        # A same-day charge + same-day refund of a DIFFERENT unrelated
        # transaction net to the same known total — the refund is already
        # netted inside stripe_net_cents by compute_stripe_net_revenue.
        with self._patch_all(
            mock_get_db_context,
            _ledger_totals_by_stream=MagicMock(return_value=ledger),
            compute_stripe_net_revenue=MagicMock(
                return_value=_stripe_net(cents=500, charge_count=2, refund_count=1)
            ),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["mismatch_cents"] == 0
        assert report["errors"] == []

    def test_nonzero_mismatch_is_flagged_as_exception(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(
            mock_get_db_context,
            compute_stripe_net_revenue=MagicMock(return_value=_stripe_net(cents=5747_00, charge_count=10)),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["mismatch_cents"] == 574700
        assert any("mismatch" in e.lower() for e in report["errors"])

    def test_stripe_not_configured_skips_reconciliation_without_false_alert(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(
            mock_get_db_context,
            compute_stripe_net_revenue=MagicMock(return_value=_stripe_net(configured=False)),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["stripe_configured"] is False
        assert report["reconciliation"]["mismatch_cents"] == 0
        assert not any("mismatch" in e.lower() for e in report["errors"])

    def test_stripe_fetch_error_reported_not_silently_zeroed(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(
            mock_get_db_context,
            compute_stripe_net_revenue=MagicMock(
                return_value=_stripe_net(configured=True, error="Stripe API down")
            ),
        ):
            report = build_report(date(2026, 7, 13))

        assert any("Stripe net-revenue fetch failed" in e for e in report["errors"])

    def test_unfulfilled_charge_surfaces_as_exception(self, mock_get_db_context):
        """The concrete paid-but-not-fulfilled bug this task exists to catch."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        unfulfilled = [{
            "payment_intent_id": "pi_orphan", "amount_cents": 500,
            "product_type": "lead_unlock", "customer_id": "cus_x",
        }]
        with self._patch_all(
            mock_get_db_context,
            _unfulfilled_lead_unlock_charges=MagicMock(return_value=unfulfilled),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["unfulfilled_lead_unlock_charges"] == unfulfilled
        assert any("no matching delivery record" in e for e in report["errors"])

    def test_stuck_lead_pack_surfaces_as_delayed_exception(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        stuck_lead_pack = {
            "by_status": {"enriching": 1},
            "stuck": [{"id": 1, "subscriber_id": 42, "purchased_at": None}],
        }
        with self._patch_all(
            mock_get_db_context,
            _lead_pack_fulfillment=MagicMock(return_value=stuck_lead_pack),
        ):
            report = build_report(date(2026, 7, 13))

        assert any("stuck past the fulfillment sweep" in e for e in report["errors"])

    def test_failed_bankruptcy_alerts_surface_as_exception(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        bankruptcy = _empty_bankruptcy()
        bankruptcy["alerts_by_status"] = {"sent": 4, "failed": 2}
        with self._patch_all(mock_get_db_context, _bankruptcy_section=MagicMock(return_value=bankruptcy)):
            report = build_report(date(2026, 7, 13))

        assert any("bankruptcy filing alert" in e for e in report["errors"])


def _reconciliation(mismatch_cents=0, stripe_configured=True):
    return {
        "known_total_cents": 0, "stripe_net_cents": mismatch_cents,
        "stripe_configured": stripe_configured, "mismatch_cents": mismatch_cents,
        "charge_count": 0, "refund_count": 0,
    }


class TestSendHeartbeatAlert:
    def test_subject_flags_exceptions_when_present(self):
        from src.tasks.revenue_fulfillment_heartbeat import send_heartbeat_alert

        report = {
            "run_date": date(2026, 7, 13),
            "ledger_total_cents": 0,
            "bankruptcy": _empty_bankruptcy(),
            "reconciliation": _reconciliation(mismatch_cents=500),
            "errors": ["something went wrong"],
        }
        with patch("src.services.email.send_alert", return_value=True) as mock_send:
            send_heartbeat_alert(report, "dummy.csv")

        subject = mock_send.call_args.args[0]
        assert "EXCEPTIONS FOUND" in subject

    def test_subject_clean_when_no_exceptions_and_zero_mismatch(self):
        from src.tasks.revenue_fulfillment_heartbeat import send_heartbeat_alert

        report = {
            "run_date": date(2026, 7, 13),
            "ledger_total_cents": 0,
            "bankruptcy": _empty_bankruptcy(),
            "reconciliation": _reconciliation(mismatch_cents=0),
            "errors": [],
        }
        with patch("src.services.email.send_alert", return_value=True) as mock_send:
            send_heartbeat_alert(report, "dummy.csv")

        subject = mock_send.call_args.args[0]
        assert "clean" in subject
        assert "EXCEPTIONS" not in subject
