"""Unit tests for the net-revenue and bankruptcy-alert Stripe pulls added to
src/tasks/stripe_reconcile.py for the MB2 revenue-fulfillment heartbeat."""
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _page(items, has_more=False):
    return SimpleNamespace(data=items, has_more=has_more)


def _bt(id_, amount):
    return SimpleNamespace(id=id_, amount=amount)


def _invoice(id_, subscription, amount_paid):
    return SimpleNamespace(id=id_, subscription=subscription, amount_paid=amount_paid)


class TestUtcDayWindow:
    def test_returns_86400_second_window(self):
        from src.tasks.stripe_reconcile import _utc_day_window

        start, end = _utc_day_window(date(2026, 7, 13))
        assert end - start == 86400

    def test_pinned_date_is_deterministic(self):
        from src.tasks.stripe_reconcile import _utc_day_window

        assert _utc_day_window(date(2026, 7, 13)) == _utc_day_window(date(2026, 7, 13))


class TestComputeStripeNetRevenue:
    def test_not_configured_returns_zeroed_dict(self):
        from src.tasks.stripe_reconcile import compute_stripe_net_revenue

        with patch("src.tasks.stripe_reconcile._init_stripe", return_value=False):
            result = compute_stripe_net_revenue(date(2026, 7, 13))

        assert result == {
            "stripe_net_cents": 0, "charge_count": 0, "refund_count": 0,
            "configured": False, "error": None,
        }

    def test_charges_only_sums_gross(self):
        from src.tasks.stripe_reconcile import compute_stripe_net_revenue

        def fake_list(**kwargs):
            if kwargs["type"] == "charge":
                return _page([_bt("bt_1", 1000), _bt("bt_2", 2000)])
            return _page([])

        with (
            patch("src.tasks.stripe_reconcile._init_stripe", return_value=True),
            patch("stripe.BalanceTransaction.list", side_effect=fake_list),
        ):
            result = compute_stripe_net_revenue(date(2026, 7, 13))

        assert result["stripe_net_cents"] == 3000
        assert result["charge_count"] == 2
        assert result["refund_count"] == 0
        assert result["error"] is None

    def test_same_day_refund_nets_against_charge(self):
        """The core regression this function exists for: a same-day refund
        must reduce the net total, not be invisible to it — Stripe posts
        the refund as its own negative type=refund transaction, separate
        from the original (unchanged) type=charge transaction."""
        from src.tasks.stripe_reconcile import compute_stripe_net_revenue

        def fake_list(**kwargs):
            if kwargs["type"] == "charge":
                return _page([_bt("bt_1", 5000)])
            return _page([_bt("bt_refund_1", -5000)])

        with (
            patch("src.tasks.stripe_reconcile._init_stripe", return_value=True),
            patch("stripe.BalanceTransaction.list", side_effect=fake_list),
        ):
            result = compute_stripe_net_revenue(date(2026, 7, 13))

        assert result["stripe_net_cents"] == 0
        assert result["charge_count"] == 1
        assert result["refund_count"] == 1

    def test_pagination_follows_has_more(self):
        from src.tasks.stripe_reconcile import compute_stripe_net_revenue

        pages = {
            "charge": [_page([_bt("bt_1", 100)], has_more=True), _page([_bt("bt_2", 200)])],
            "refund": [_page([])],
        }
        calls = {"charge": 0, "refund": 0}

        def fake_list(**kwargs):
            t = kwargs["type"]
            page = pages[t][calls[t]]
            calls[t] += 1
            return page

        with (
            patch("src.tasks.stripe_reconcile._init_stripe", return_value=True),
            patch("stripe.BalanceTransaction.list", side_effect=fake_list),
        ):
            result = compute_stripe_net_revenue(date(2026, 7, 13))

        assert result["stripe_net_cents"] == 300
        assert result["charge_count"] == 2

    def test_stripe_error_reports_as_error_not_zero_activity(self):
        import stripe as stripe_module
        from src.tasks.stripe_reconcile import compute_stripe_net_revenue

        with (
            patch("src.tasks.stripe_reconcile._init_stripe", return_value=True),
            patch("stripe.BalanceTransaction.list", side_effect=stripe_module.error.StripeError("boom")),
        ):
            result = compute_stripe_net_revenue(date(2026, 7, 13))

        assert result["configured"] is True
        assert result["error"] == "boom"
        assert result["stripe_net_cents"] == 0


class TestFetchBankruptcyAlertRevenue:
    def test_no_known_subscriptions_short_circuits(self, mock_db):
        from src.tasks.stripe_reconcile import fetch_bankruptcy_alert_revenue

        mock_db.execute.return_value.all.return_value = []
        with (
            patch("src.tasks.stripe_reconcile._init_stripe", return_value=True),
            patch("stripe.Invoice.list") as mock_list,
        ):
            result = fetch_bankruptcy_alert_revenue(mock_db, date(2026, 7, 13))

        mock_list.assert_not_called()
        assert result == {"revenue_cents": 0, "invoice_count": 0, "configured": True, "error": None}

    def test_filters_invoices_to_known_subscription_ids(self, mock_db):
        from src.tasks.stripe_reconcile import fetch_bankruptcy_alert_revenue

        mock_db.execute.return_value.all.return_value = [("sub_bk_1",), ("sub_bk_2",)]

        def fake_list(**kwargs):
            return _page([
                _invoice("in_1", "sub_bk_1", 29700),
                _invoice("in_2", "sub_bk_2", 29700),
                _invoice("in_3", "sub_unrelated", 150000),  # main-product invoice, must be excluded
            ])

        with (
            patch("src.tasks.stripe_reconcile._init_stripe", return_value=True),
            patch("stripe.Invoice.list", side_effect=fake_list),
        ):
            result = fetch_bankruptcy_alert_revenue(mock_db, date(2026, 7, 13))

        assert result["revenue_cents"] == 59400
        assert result["invoice_count"] == 2

    def test_stripe_error_reports_as_error(self, mock_db):
        import stripe as stripe_module
        from src.tasks.stripe_reconcile import fetch_bankruptcy_alert_revenue

        mock_db.execute.return_value.all.return_value = [("sub_bk_1",)]
        with (
            patch("src.tasks.stripe_reconcile._init_stripe", return_value=True),
            patch("stripe.Invoice.list", side_effect=stripe_module.error.StripeError("down")),
        ):
            result = fetch_bankruptcy_alert_revenue(mock_db, date(2026, 7, 13))

        assert result["error"] == "down"
        assert result["revenue_cents"] == 0
