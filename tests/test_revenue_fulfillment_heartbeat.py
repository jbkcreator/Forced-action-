"""Unit tests for the MB2 daily revenue & fulfillment heartbeat.

Follows the pattern used for src/tasks/guarantee_shortfall_sweep.py's tests:
patch out the individual DB-query helper functions and test build_report's
own orchestration (mismatch computation, exception-list population) rather
than re-testing each helper's raw SQL, which is simple GROUP BY aggregation
already exercised indirectly by the real dry-run against the shared DB.
"""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import Property, Subscriber
from src.services.revenue_ledger import mark_ledger_refunded, record_revenue


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


def _empty_unfulfilled():
    return {"charges": [], "error": None}


def _ledger_net(cents=0):
    return {"net_cents": cents}


class TestBuildReportOrchestration:
    def _patch_all(self, mock_get_db_context, **overrides):
        defaults = dict(
            _ledger_totals_by_stream=MagicMock(return_value=_empty_ledger()),
            _ledger_net_effect_today=MagicMock(return_value=_ledger_net()),
            _refunds_disputes=MagicMock(return_value=_empty_refunds_disputes()),
            _lead_pack_fulfillment=MagicMock(return_value=_empty_lead_pack()),
            _premium_fulfillment=MagicMock(return_value=_empty_premium()),
            _unfulfilled_lead_unlock_charges=MagicMock(return_value=_empty_unfulfilled()),
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

    def test_matched_net_and_stripe_totals_yield_zero_mismatch(self, mock_get_db_context):
        """Ledger net effect + bankruptcy revenue exactly matching Stripe's
        net must not alert — this is the ordinary "everything reconciled"
        case. Mismatch is driven by _ledger_net_effect_today, not the
        by-stream display total (_ledger_totals_by_stream) — they answer
        different questions."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        bankruptcy = _empty_bankruptcy()
        bankruptcy["revenue_cents"] = 29700

        with self._patch_all(
            mock_get_db_context,
            _ledger_net_effect_today=MagicMock(return_value=_ledger_net(60000)),
            _bankruptcy_section=MagicMock(return_value=bankruptcy),
            compute_stripe_net_revenue=MagicMock(return_value=_stripe_net(cents=89700, charge_count=2)),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["mismatch_cents"] == 0
        assert report["errors"] == []

    def test_same_day_refund_scenario_nets_to_zero_mismatch(self, mock_get_db_context):
        """compute_stripe_net_revenue already nets refunds against charges
        (tested directly in test_stripe_reconcile.py) — this confirms
        build_report trusts that net figure as-is against the ledger's own
        net-effect figure, not a gross-by-stream total."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(
            mock_get_db_context,
            _ledger_net_effect_today=MagicMock(return_value=_ledger_net(500)),
            compute_stripe_net_revenue=MagicMock(
                return_value=_stripe_net(cents=500, charge_count=2, refund_count=1)
            ),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["mismatch_cents"] == 0
        assert report["errors"] == []

    def test_cross_day_refund_nets_to_zero_mismatch(self, mock_get_db_context):
        """Regression for the Critical review finding: a charge from days
        ago refunded TODAY must reduce today's ledger net exactly as it
        reduces today's Stripe net (both are "net effect processed today"),
        or every non-same-day refund shows up as a false mismatch. This
        exercises build_report's wiring; _ledger_net_effect_today's own SQL
        (gross-originated-today minus refunded-today regardless of
        origination day) is exercised directly against a real DB in
        test_ledger_net_effect_today_cross_day_refund below."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        # Today: no new charges at all, but a 5-day-old $50 charge got
        # refunded today. Ledger net today = 0 (originated) - 5000 (refunded
        # today) = -5000. Stripe's own net today is the same: the refund
        # transaction alone, no charge transaction today.
        with self._patch_all(
            mock_get_db_context,
            _ledger_net_effect_today=MagicMock(return_value=_ledger_net(-5000)),
            compute_stripe_net_revenue=MagicMock(
                return_value=_stripe_net(cents=-5000, charge_count=0, refund_count=1)
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
        assert any("mismatch" in e["message"].lower() for e in report["errors"])
        assert any(e["key"] == "stripe_ledger_mismatch" for e in report["errors"])

    def test_stripe_not_configured_skips_reconciliation_without_false_alert(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(
            mock_get_db_context,
            compute_stripe_net_revenue=MagicMock(return_value=_stripe_net(configured=False)),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["reconciliation"]["stripe_configured"] is False
        assert report["reconciliation"]["mismatch_cents"] == 0
        assert not any("mismatch" in e["message"].lower() for e in report["errors"])

    def test_stripe_fetch_error_reported_not_silently_zeroed(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(
            mock_get_db_context,
            compute_stripe_net_revenue=MagicMock(
                return_value=_stripe_net(configured=True, error="Stripe API down")
            ),
        ):
            report = build_report(date(2026, 7, 13))

        assert any("Stripe net-revenue fetch failed" in e["message"] for e in report["errors"])

    def test_unfulfilled_charge_surfaces_as_exception(self, mock_get_db_context):
        """The concrete paid-but-not-fulfilled bug this task exists to catch."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        unfulfilled = {"charges": [{
            "payment_intent_id": "pi_orphan", "amount_cents": 500,
            "product_type": "lead_unlock", "customer_id": "cus_x",
        }], "error": None}
        with self._patch_all(
            mock_get_db_context,
            _unfulfilled_lead_unlock_charges=MagicMock(return_value=unfulfilled),
        ):
            report = build_report(date(2026, 7, 13))

        assert report["unfulfilled_lead_unlock_charges"] == unfulfilled
        assert any("no matching delivery record" in e["message"] for e in report["errors"])

    def test_unfulfilled_scan_failure_reported_not_silently_empty(self, mock_get_db_context):
        """Regression for the High review finding: a scan failure must show
        as "could not verify," never silently render as zero unfulfilled
        charges found."""
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        with self._patch_all(
            mock_get_db_context,
            _unfulfilled_lead_unlock_charges=MagicMock(
                return_value={"charges": [], "error": "Stripe API down"}
            ),
        ):
            report = build_report(date(2026, 7, 13))

        assert any("Unfulfilled-charge scan failed" in e["message"] for e in report["errors"])

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

        assert any("stuck past the fulfillment sweep" in e["message"] for e in report["errors"])

    def test_failed_bankruptcy_alerts_surface_as_exception(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import build_report

        bankruptcy = _empty_bankruptcy()
        bankruptcy["alerts_by_status"] = {"sent": 4, "failed": 2}
        with self._patch_all(mock_get_db_context, _bankruptcy_section=MagicMock(return_value=bankruptcy)):
            report = build_report(date(2026, 7, 13))

        assert any("bankruptcy filing alert" in e["message"] for e in report["errors"])


def _reconciliation(mismatch_cents=0, stripe_configured=True):
    return {
        "known_net_cents": 0, "stripe_net_cents": mismatch_cents,
        "stripe_configured": stripe_configured, "mismatch_cents": mismatch_cents,
        "charge_count": 0, "refund_count": 0,
    }


class TestSendHeartbeatAlert:
    """_dedupe_errors_for_alert is patched to pass errors through unchanged
    (identity) — cooldown/suppression behavior has its own dedicated tests
    in TestDedupeErrorsForAlert below. These tests are purely about the
    subject-line/body logic given a set of (already-deduped) alert errors."""

    def test_subject_flags_exceptions_when_present(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import send_heartbeat_alert

        report = {
            "run_date": date(2026, 7, 13),
            "ledger_total_cents": 0,
            "bankruptcy": _empty_bankruptcy(),
            "reconciliation": _reconciliation(mismatch_cents=500),
            "errors": [{"key": "something", "message": "something went wrong"}],
        }
        with patch("src.services.email.send_alert", return_value=True) as mock_send, \
             patch("src.tasks.revenue_fulfillment_heartbeat.get_db_context", mock_get_db_context), \
             patch("src.tasks.revenue_fulfillment_heartbeat._dedupe_errors_for_alert",
                   side_effect=lambda db, errors, **kw: errors):
            send_heartbeat_alert(report, "dummy.csv")

        subject = mock_send.call_args.args[0]
        assert "EXCEPTIONS FOUND" in subject

    def test_subject_clean_when_no_exceptions_and_zero_mismatch(self, mock_get_db_context):
        from src.tasks.revenue_fulfillment_heartbeat import send_heartbeat_alert

        report = {
            "run_date": date(2026, 7, 13),
            "ledger_total_cents": 0,
            "bankruptcy": _empty_bankruptcy(),
            "reconciliation": _reconciliation(mismatch_cents=0),
            "errors": [],
        }
        with patch("src.services.email.send_alert", return_value=True) as mock_send, \
             patch("src.tasks.revenue_fulfillment_heartbeat.get_db_context", mock_get_db_context), \
             patch("src.tasks.revenue_fulfillment_heartbeat._dedupe_errors_for_alert",
                   side_effect=lambda db, errors, **kw: errors):
            send_heartbeat_alert(report, "dummy.csv")

        subject = mock_send.call_args.args[0]
        assert "clean" in subject
        assert "EXCEPTIONS" not in subject

    def test_subject_clean_when_all_errors_suppressed_by_cooldown(self, mock_get_db_context):
        """A day where every exception is still in cooldown reads as
        "clean" in the subject line — the full detail is always in the CSV
        attachment regardless, so this isn't hiding anything, just not
        re-nagging on an already-acknowledged, unresolved issue."""
        from src.tasks.revenue_fulfillment_heartbeat import send_heartbeat_alert

        report = {
            "run_date": date(2026, 7, 13),
            "ledger_total_cents": 0,
            "bankruptcy": _empty_bankruptcy(),
            "reconciliation": _reconciliation(mismatch_cents=500),
            "errors": [{"key": "stripe_ledger_mismatch", "message": "mismatch: $5.00"}],
        }
        with patch("src.services.email.send_alert", return_value=True) as mock_send, \
             patch("src.tasks.revenue_fulfillment_heartbeat.get_db_context", mock_get_db_context), \
             patch("src.tasks.revenue_fulfillment_heartbeat._dedupe_errors_for_alert",
                   return_value=[]):
            send_heartbeat_alert(report, "dummy.csv")

        subject = mock_send.call_args.args[0]
        assert "clean" in subject
        body = mock_send.call_args.args[1]
        assert "in cooldown" in body


class TestDedupeErrorsForAlert:
    """_dedupe_errors_for_alert against a real DB — the actual suppression
    logic (does an alert_key within cooldown get dropped, does a changed
    content-keyed id set still alert, does the dollar-figure-varying
    mismatch key still suppress) is exactly what would silently regress if
    tested only against mocks."""

    def test_same_key_within_cooldown_is_suppressed(self, fresh_db):
        from src.tasks.revenue_fulfillment_heartbeat import _dedupe_errors_for_alert

        errors = [{"key": "stripe_ledger_mismatch", "message": "mismatch: $5.00"}]
        first = _dedupe_errors_for_alert(fresh_db, errors)
        assert first == errors

        # Same key, different dollar figure in the message — still suppressed,
        # cooldown tracks "still investigating this category," not the exact number.
        second = _dedupe_errors_for_alert(
            fresh_db, [{"key": "stripe_ledger_mismatch", "message": "mismatch: $9.00"}]
        )
        assert second == []

    def test_different_key_still_alerts_during_unrelated_cooldown(self, fresh_db):
        from src.tasks.revenue_fulfillment_heartbeat import _dedupe_errors_for_alert

        _dedupe_errors_for_alert(fresh_db, [{"key": "stripe_ledger_mismatch", "message": "m"}])

        fresh_ids_error = [{"key": "unfulfilled_charges:['pi_new']", "message": "1 unfulfilled"}]
        result = _dedupe_errors_for_alert(fresh_db, fresh_ids_error)
        assert result == fresh_ids_error

    def test_changed_id_set_re_alerts_even_though_category_recurs(self, fresh_db):
        """A genuinely new stuck lead pack purchase must still alert even
        while a DIFFERENT, unresolved stuck purchase is mid-cooldown —
        content-keyed alert_keys naturally re-alert on a changed set."""
        from src.tasks.revenue_fulfillment_heartbeat import _dedupe_errors_for_alert

        _dedupe_errors_for_alert(fresh_db, [{"key": "lead_pack_stuck:[1]", "message": "1 stuck"}])

        new_set_error = [{"key": "lead_pack_stuck:[1, 2]", "message": "2 stuck"}]
        result = _dedupe_errors_for_alert(fresh_db, new_set_error)
        assert result == new_set_error

    def test_past_cooldown_window_re_alerts(self, fresh_db):
        from datetime import timedelta
        from sqlalchemy import text as sa_text
        from src.tasks.revenue_fulfillment_heartbeat import _dedupe_errors_for_alert

        errors = [{"key": "dispute_fetch_failed", "message": "m"}]
        _dedupe_errors_for_alert(fresh_db, errors)
        # No commit needed (or wanted — see _dedupe_errors_for_alert's own
        # docstring on why it doesn't commit): a raw UPDATE via execute() is
        # already visible to a later read in the same transaction.
        fresh_db.execute(sa_text(
            "UPDATE revenue_heartbeat_alert_log SET alerted_at = alerted_at - INTERVAL '25 hours' "
            "WHERE alert_key = :k"
        ), {"k": "dispute_fetch_failed"})

        result = _dedupe_errors_for_alert(fresh_db, errors, cooldown_hours=24)
        assert result == errors


class TestLedgerNetEffectTodayRealDb:
    """Direct-DB coverage for the actual Critical bug this PR fixes — every
    other helper in this file is mocked out in TestBuildReportOrchestration
    per this module's docstring, but this function's date-bucketing logic
    (not "simple GROUP BY aggregation") is exactly what was wrong, so it
    gets exercised against real Postgres rather than only via mocks."""

    def _seed(self, db, *, cust="cus_lnet"):
        sub = Subscriber(stripe_customer_id=cust, tier="pro", vertical="roofing", county_id="hillsborough")
        db.add(sub); db.flush()
        prop = Property(parcel_id=f"P-{cust}", county_id="hillsborough")
        db.add(prop); db.flush()
        return sub, prop

    def test_charge_and_refund_same_day_nets_correctly(self, fresh_db):
        from src.tasks.revenue_fulfillment_heartbeat import _ledger_net_effect_today

        sub, prop = self._seed(fresh_db)
        today = datetime(2099, 6, 15, tzinfo=timezone.utc)
        record_revenue(fresh_db, subscriber_id=sub.id, product_type="lead_unlock",
                       amount_cents=500, source_table="sent_leads", source_id=90001,
                       property_id=prop.id, occurred_at=today)
        mark_ledger_refunded(fresh_db, source_table="sent_leads", source_id=90001, refunded_at=today)
        fresh_db.flush()

        net = _ledger_net_effect_today(
            fresh_db, today, today + timedelta(days=1)
        )
        assert net["net_cents"] == 0

    def test_cross_day_refund_reduces_todays_net_not_the_origination_days(self, fresh_db):
        """The exact scenario that was broken: a charge from 5 days ago,
        refunded today, must show up as -amount in TODAY's net (mirroring
        Stripe's own per-day cash model) — not silently absorbed into a day
        whose report has already been generated and sent."""
        from src.tasks.revenue_fulfillment_heartbeat import _ledger_net_effect_today

        sub, prop = self._seed(fresh_db, cust="cus_lnet_xday")
        origin_day = datetime(2099, 6, 10, tzinfo=timezone.utc)
        today = datetime(2099, 6, 15, tzinfo=timezone.utc)

        record_revenue(fresh_db, subscriber_id=sub.id, product_type="lead_unlock",
                       amount_cents=5000, source_table="sent_leads", source_id=90002,
                       property_id=prop.id, occurred_at=origin_day)
        mark_ledger_refunded(fresh_db, source_table="sent_leads", source_id=90002, refunded_at=today)
        fresh_db.flush()

        origin_day_net = _ledger_net_effect_today(fresh_db, origin_day, origin_day + timedelta(days=1))
        today_net = _ledger_net_effect_today(fresh_db, today, today + timedelta(days=1))

        # Origination day: the charge originated there, gross — the refund
        # hadn't happened yet as of that day's own net-effect view.
        assert origin_day_net["net_cents"] == 5000
        # Today: no new charges originated, but today's refund reduces
        # today's net by the full amount — same as Stripe's own net would.
        assert today_net["net_cents"] == -5000

    def test_partial_refund_nets_only_the_refunded_portion(self, fresh_db):
        from src.tasks.revenue_fulfillment_heartbeat import _ledger_net_effect_today

        sub, prop = self._seed(fresh_db, cust="cus_lnet_partial")
        today = datetime(2099, 6, 20, tzinfo=timezone.utc)
        record_revenue(fresh_db, subscriber_id=sub.id, product_type="premium_report",
                       amount_cents=10000, source_table="premium_purchases", source_id=90003,
                       property_id=prop.id, occurred_at=today)
        mark_ledger_refunded(fresh_db, source_table="premium_purchases", source_id=90003,
                             refunded_at=today, refunded_amount_cents=3000)
        fresh_db.flush()

        net = _ledger_net_effect_today(fresh_db, today, today + timedelta(days=1))
        # $100 originated - $30 actually refunded = $70, not $0 (which the
        # pre-fix all-or-nothing refunded_at-only exclusion would have given).
        assert net["net_cents"] == 7000


def _bt_with_charge(pi, product, amount=500, customer="cus_x"):
    from types import SimpleNamespace
    charge = SimpleNamespace(metadata={"product": product}, payment_intent=pi, amount=amount, customer=customer)
    return SimpleNamespace(source=charge)


class TestUnfulfilledLeadUnlockCharges:
    """Direct coverage for the two High review findings on this specific
    function: silent failure on Stripe error, and false-flagging a
    lead_unlock -> hot_lead_unlock upgrade whose SentLead row was overwritten
    in place."""

    def test_stripe_error_reports_as_error_not_silently_empty(self):
        import stripe as stripe_module
        from src.tasks.revenue_fulfillment_heartbeat import _unfulfilled_lead_unlock_charges

        db = MagicMock()
        day = datetime(2026, 7, 13, tzinfo=timezone.utc)
        with patch("src.tasks.revenue_fulfillment_heartbeat._init_stripe", return_value=True), \
             patch("stripe.BalanceTransaction.list", side_effect=stripe_module.error.StripeError("down")):
            result = _unfulfilled_lead_unlock_charges(day, day + timedelta(days=1), db)

        assert result == {"charges": [], "error": "down"}

    def test_ledger_row_present_avoids_false_positive_on_upgrade(self):
        """The exact scenario this fixes: SentLead's payment_intent_id was
        overwritten by a later hot-unlock (unique per subscriber/property),
        but platform_revenue_ledger still has this charge's own row under
        its collision-proof hash key — checking the ledger, not sent_leads,
        avoids flagging a genuinely-delivered purchase."""
        from types import SimpleNamespace
        from src.tasks.revenue_fulfillment_heartbeat import _unfulfilled_lead_unlock_charges

        db = MagicMock()
        db.execute.return_value.first.return_value = (1,)  # ledger row exists
        day = datetime(2026, 7, 13, tzinfo=timezone.utc)

        with patch("src.tasks.revenue_fulfillment_heartbeat._init_stripe", return_value=True), \
             patch("stripe.BalanceTransaction.list",
                   return_value=SimpleNamespace(
                       data=[_bt_with_charge("pi_upgraded", "hot_lead_unlock")], has_more=False)):
            result = _unfulfilled_lead_unlock_charges(day, day + timedelta(days=1), db)

        assert result == {"charges": [], "error": None}

    def test_no_ledger_row_flags_genuine_gap(self):
        from types import SimpleNamespace
        from src.tasks.revenue_fulfillment_heartbeat import _unfulfilled_lead_unlock_charges

        db = MagicMock()
        db.execute.return_value.first.return_value = None  # no ledger row
        day = datetime(2026, 7, 13, tzinfo=timezone.utc)

        with patch("src.tasks.revenue_fulfillment_heartbeat._init_stripe", return_value=True), \
             patch("stripe.BalanceTransaction.list",
                   return_value=SimpleNamespace(
                       data=[_bt_with_charge("pi_orphan", "lead_unlock")], has_more=False)):
            result = _unfulfilled_lead_unlock_charges(day, day + timedelta(days=1), db)

        assert result["error"] is None
        assert len(result["charges"]) == 1
        assert result["charges"][0]["payment_intent_id"] == "pi_orphan"
