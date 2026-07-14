"""
Stage 12 — Unit tests: Bankruptcy Filing Alert product.

Covers (no DB / mocked DB):
  - ingest._extract_filing: jurisdiction mapping, chapter normalization, non-bk skip
  - ingest chapter filter logic
  - config.jurisdiction_for_docket
  - alerts message formatting (email + sms)
  - subscription webhook status mapping
  - subscription checkout metadata building
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ── config.jurisdiction_for_docket ───────────────────────────────────────────

class TestJurisdictionMapping:
    def test_tampa_division_maps(self):
        from config.bankruptcy_alert_config import jurisdiction_for_docket
        assert jurisdiction_for_docket("flmb", "8:23-bk-12345") == "flmb-tampa"

    def test_orlando_division_maps(self):
        from config.bankruptcy_alert_config import jurisdiction_for_docket
        assert jurisdiction_for_docket("flmb", "6:24-bk-00001") == "flmb-orlando"

    def test_unknown_court_returns_none(self):
        from config.bankruptcy_alert_config import jurisdiction_for_docket
        assert jurisdiction_for_docket("nysb", "1:24-bk-00001") is None

    def test_unknown_division_returns_none(self):
        from config.bankruptcy_alert_config import jurisdiction_for_docket
        # flmb court but division prefix '9:' isn't configured
        assert jurisdiction_for_docket("flmb", "9:24-bk-00001") is None


# ── ingest._extract_filing ────────────────────────────────────────────────────

class TestExtractFiling:
    def _docket(self, **kw):
        base = {
            "id": 555,
            "docket_number": "8:23-bk-12345",
            "case_name": "In re: John Smith",
            "federal_dn_case_type": "bk",
            "date_filed": "2026-05-30",
            "court": "flmb",
            "chapter": 7,
            "nature_of_suit": None,
        }
        base.update(kw)
        return base

    def test_extracts_tampa_bk_filing(self):
        from src.services.bankruptcy_alert.ingest import _extract_filing
        f = _extract_filing(self._docket())
        assert f is not None
        assert f["case_number"] == "8:23-bk-12345"
        assert f["jurisdiction"] == "flmb-tampa"
        assert f["chapter"] == "7"
        assert f["filer"] == "John Smith"
        assert f["court"] == "flmb"
        assert f["docket_id"] == "555"

    def test_skips_non_bankruptcy(self):
        from src.services.bankruptcy_alert.ingest import _extract_filing
        assert _extract_filing(self._docket(federal_dn_case_type="cv")) is None

    def test_skips_unmapped_jurisdiction(self):
        from src.services.bankruptcy_alert.ingest import _extract_filing
        assert _extract_filing(self._docket(court="nysb", docket_number="1:23-bk-1")) is None

    def test_chapter_normalized_to_string(self):
        from src.services.bankruptcy_alert.ingest import _extract_filing
        assert _extract_filing(self._docket(chapter=13))["chapter"] == "13"

    def test_null_chapter_stays_none(self):
        from src.services.bankruptcy_alert.ingest import _extract_filing
        assert _extract_filing(self._docket(chapter=None))["chapter"] is None

    def test_strips_in_re_prefix(self):
        from src.services.bankruptcy_alert.ingest import _extract_filing
        f = _extract_filing(self._docket(case_name="In re: ACME Holdings LLC"))
        assert f["filer"] == "ACME Holdings LLC"


# ── ingest chapter filter ─────────────────────────────────────────────────────

class TestIngestChapterFilter:
    def _mock_db_capturing_inserts(self):
        db = MagicMock()
        inserted = []

        def execute(stmt, params=None):
            res = MagicMock()
            text = str(stmt)
            if "INSERT INTO bankruptcy_filings" in text:
                inserted.append(params)
                row = MagicMock()
                row.id = len(inserted)
                res.first.return_value = row
            else:
                res.first.return_value = None
            return res

        db.execute.side_effect = execute
        return db, inserted

    def test_chapter_7_11_13_pass_chapter_99_filtered(self):
        from src.services.bankruptcy_alert.ingest import ingest_filings

        dockets = [
            {"id": 1, "docket_number": "8:1-bk-1", "case_name": "In re: A",
             "federal_dn_case_type": "bk", "court": "flmb", "chapter": 7, "date_filed": "2026-05-30"},
            {"id": 2, "docket_number": "8:1-bk-2", "case_name": "In re: B",
             "federal_dn_case_type": "bk", "court": "flmb", "chapter": 13, "date_filed": "2026-05-30"},
            {"id": 3, "docket_number": "8:1-bk-3", "case_name": "In re: C",
             "federal_dn_case_type": "bk", "court": "flmb", "chapter": 99, "date_filed": "2026-05-30"},
        ]
        db, inserted = self._mock_db_capturing_inserts()

        with patch("src.services.bankruptcy_alert.ingest._fetch_dockets", return_value=(dockets, 1)):
            result = ingest_filings(db, lookback_days=1)

        # Chapter 99 filtered out; 7 and 13 inserted.
        assert result.matched == 2
        assert result.inserted == 2
        chapters = {p["chapter"] for p in inserted}
        assert chapters == {"7", "13"}

    def test_ingest_failure_sets_success_false(self):
        import requests
        from src.services.bankruptcy_alert.ingest import ingest_filings
        db = MagicMock()
        with patch("src.services.bankruptcy_alert.ingest._fetch_dockets",
                   side_effect=requests.Timeout("api down")):
            result = ingest_filings(db, lookback_days=1)
        assert result.success is False
        assert "api down" in result.error or "API error" in result.error


# ── alerts formatting ──────────────────────────────────────────────────────────

class TestAlertFormatting:
    def _filing(self, **kw):
        base = dict(id=1, case_number="8:23-bk-1", chapter="7", court="flmb",
                    jurisdiction="flmb-tampa", filer="John Smith", trustee=None,
                    date_filed=None)
        base.update(kw)
        return SimpleNamespace(**base)

    def test_email_single_filing(self):
        from src.services.bankruptcy_alert.alerts import _format_email
        subject, text, html = _format_email([self._filing()])
        assert "1 new bankruptcy filing" in subject
        assert "John Smith" in text
        assert "8:23-bk-1" in html

    def test_email_plural(self):
        from src.services.bankruptcy_alert.alerts import _format_email
        subject, text, html = _format_email([self._filing(), self._filing(case_number="8:23-bk-2")])
        assert "2 new bankruptcy filings" in subject

    def test_sms_single(self):
        from src.services.bankruptcy_alert.alerts import _format_sms
        body = _format_sms([self._filing()])
        assert "John Smith" in body
        assert "Ch.7" in body

    def test_sms_digest_for_multiple(self):
        from src.services.bankruptcy_alert.alerts import _format_sms
        body = _format_sms([self._filing(), self._filing(case_number="x"), self._filing(case_number="y")])
        assert "3 new bankruptcy filings" in body
        assert len(body) < 160


# ── subscription status mapping ──────────────────────────────────────────────

class TestSubscriptionWebhook:
    def test_status_mapping_active(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        captured = {}

        def execute(stmt, params=None):
            captured["params"] = params
            res = MagicMock()
            res.rowcount = 1
            return res

        db.execute.side_effect = execute
        subscription._on_subscription_updated({"id": "sub_1", "status": "active"}, db)
        assert captured["params"]["status"] == "active"

    def test_status_mapping_past_due_from_unpaid(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        captured = {}

        def execute(stmt, params=None):
            captured["params"] = params
            res = MagicMock(); res.rowcount = 1
            return res
        db.execute.side_effect = execute
        subscription._on_subscription_updated({"id": "sub_1", "status": "unpaid"}, db)
        assert captured["params"]["status"] == "past_due"

    def test_unknown_status_no_update(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        subscription._on_subscription_updated({"id": "sub_1", "status": "weird"}, db)
        db.execute.assert_not_called()

    def test_deleted_sets_canceled(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        sql_seen = {}

        def execute(stmt, params=None):
            sql_seen["text"] = str(stmt)
            sql_seen["params"] = params
            res = MagicMock(); res.rowcount = 1
            return res
        db.execute.side_effect = execute
        subscription._on_subscription_deleted({"id": "sub_9"}, db)
        assert sql_seen["params"]["status"] == "canceled"
        assert "canceled_at = NOW()" in sql_seen["text"]

    def test_checkout_ignores_non_bankruptcy_product(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        subscription._on_checkout_completed(
            {"metadata": {"product": "wallet"}, "customer": "cus_1"}, db
        )
        db.execute.assert_not_called()


# ── resolve_handler routing (shared webhook endpoint) ─────────────────────────

class TestResolveHandler:
    def test_checkout_with_bankruptcy_product_routes(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        h = subscription.resolve_handler(
            "checkout.session.completed",
            {"metadata": {"product": "bankruptcy_alerts"}},
            db,
        )
        assert h is subscription._on_checkout_completed

    def test_checkout_without_product_returns_none(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        # A property-product checkout (no bankruptcy metadata) must NOT be claimed.
        h = subscription.resolve_handler(
            "checkout.session.completed",
            {"metadata": {"tier": "starter", "vertical": "roofing"}},
            db,
        )
        assert h is None

    def test_subscription_updated_routes_on_metadata(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        h = subscription.resolve_handler(
            "customer.subscription.updated",
            {"id": "sub_1", "status": "active", "metadata": {"product": "bankruptcy_alerts"}},
            db,
        )
        assert h is subscription._on_subscription_updated

    def test_invoice_routes_when_subscription_owned(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        # _owns_subscription finds a row → invoice event is claimed.
        db.execute.return_value.first.return_value = MagicMock()
        h = subscription.resolve_handler(
            "invoice.payment_failed", {"subscription": "sub_9"}, db
        )
        assert h is subscription._on_payment_failed

    def test_invoice_not_routed_when_subscription_foreign(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        # No bankruptcy row for this subscription → not claimed (property product's).
        db.execute.return_value.first.return_value = None
        h = subscription.resolve_handler(
            "invoice.payment_failed", {"subscription": "sub_other"}, db
        )
        assert h is None

    def test_unrelated_event_type_returns_none(self):
        from src.services.bankruptcy_alert import subscription
        db = MagicMock()
        assert subscription.resolve_handler("charge.refunded", {}, db) is None


# ── checkout creation ─────────────────────────────────────────────────────────

class TestCreateCheckout:
    def test_raises_when_price_not_configured(self):
        from src.services.bankruptcy_alert import subscription
        with patch.object(subscription, "_init_stripe", return_value=True), \
             patch("src.services.bankruptcy_alert.subscription.get_settings") as gs:
            gs.return_value.active_stripe_price.return_value = None
            with pytest.raises(ValueError, match="not configured"):
                subscription.create_checkout(success_url="s", cancel_url="c")

    def test_raises_when_stripe_not_configured(self):
        from src.services.bankruptcy_alert import subscription
        with patch.object(subscription, "_init_stripe", return_value=False):
            with pytest.raises(RuntimeError, match="not configured"):
                subscription.create_checkout(success_url="s", cancel_url="c")

    def test_passes_trial_when_enabled(self):
        from src.services.bankruptcy_alert import subscription
        fake_session = SimpleNamespace(id="cs_1", url="https://stripe/cs_1")
        with patch.object(subscription, "_init_stripe", return_value=True), \
             patch("src.services.bankruptcy_alert.subscription.get_settings") as gs, \
             patch("src.services.bankruptcy_alert.subscription.TRIAL_DAYS", 14), \
             patch("stripe.checkout.Session.create", return_value=fake_session) as create:
            gs.return_value.active_stripe_price.return_value = "price_123"
            subscription.create_checkout(success_url="s", cancel_url="c", with_trial=True)
            kwargs = create.call_args.kwargs
            assert kwargs["subscription_data"]["trial_period_days"] == 14
            assert kwargs["metadata"]["product"] == "bankruptcy_alerts"


# ── post-signup invite (schedule + sweep) ─────────────────────────────────────

class TestInviteScheduling:
    def test_schedule_invite_inserts_when_absent(self):
        from src.services.bankruptcy_alert import invite
        db = MagicMock()
        captured = {}

        def execute(stmt, params=None):
            captured["text"] = str(stmt)
            captured["params"] = params
            res = MagicMock()
            res.first.return_value = MagicMock(id=1)  # row created
            return res
        db.execute.side_effect = execute

        with patch("src.services.bankruptcy_alert.invite.get_settings") as gs:
            gs.return_value.bankruptcy_invite_delay_minutes = 30
            created = invite.schedule_invite(db, subscriber_id=42)

        assert created is True
        assert "INSERT INTO message_outcomes" in captured["text"]
        assert "WHERE NOT EXISTS" in captured["text"]   # idempotent guard
        assert captured["params"]["sid"] == 42
        assert captured["params"]["tpl"] == "bankruptcy_alert_invite"

    def test_schedule_invite_noop_when_exists(self):
        from src.services.bankruptcy_alert import invite
        db = MagicMock()
        db.execute.return_value.first.return_value = None  # NOT EXISTS guard hit → no row
        with patch("src.services.bankruptcy_alert.invite.get_settings") as gs:
            gs.return_value.bankruptcy_invite_delay_minutes = 30
            created = invite.schedule_invite(db, subscriber_id=42)
        assert created is False

    def test_schedule_invite_never_raises(self):
        from src.services.bankruptcy_alert import invite
        db = MagicMock()
        db.execute.side_effect = RuntimeError("db boom")
        with patch("src.services.bankruptcy_alert.invite.get_settings") as gs:
            gs.return_value.bankruptcy_invite_delay_minutes = 30
            # Must swallow — signup must not break.
            assert invite.schedule_invite(db, subscriber_id=42) is False


class TestInviteBody:
    def test_body_contains_link_and_price(self):
        from src.services.bankruptcy_alert.invite import _email_body
        text, html = _email_body("Dana", "https://stripe/cs_test")
        assert "Dana" in text
        assert "$297/mo" in text
        assert "https://stripe/cs_test" in text
        assert "https://stripe/cs_test" in html

    def test_body_handles_missing_name(self):
        from src.services.bankruptcy_alert.invite import _email_body
        text, _ = _email_body(None, "https://stripe/cs_test")
        assert text.startswith("Hi,")


class TestInviteSweep:
    def _row(self, **kw):
        from datetime import datetime, timezone
        base = dict(outcome_id=1, scheduled_send_at=datetime.now(timezone.utc),
                    sub_id=10, email="x@e.com", name="X", status="active")
        base.update(kw)
        return SimpleNamespace(**base)

    def test_skips_churned_subscriber(self):
        from src.services.bankruptcy_alert import invite
        db = MagicMock()
        marks = []

        def execute(stmt, params=None):
            t = str(stmt)
            res = MagicMock()
            if "FROM message_outcomes m" in t and "JOIN subscribers" in t:
                res.fetchall.return_value = [self._row(status="churned")]
            elif "UPDATE message_outcomes" in t:
                marks.append(params)
            return res
        db.execute.side_effect = execute

        res = invite.send_due_invites(db)
        assert res.skipped == 1
        assert res.sent == 0
        assert marks[0]["status"] == "cancelled"

    def test_skips_already_subscribed(self):
        from src.services.bankruptcy_alert import invite
        db = MagicMock()

        def execute(stmt, params=None):
            t = str(stmt)
            res = MagicMock()
            if "FROM message_outcomes m" in t and "JOIN subscribers" in t:
                res.fetchall.return_value = [self._row()]
            elif "FROM bankruptcy_alert_subscriptions" in t:
                res.first.return_value = MagicMock()  # already subscribed
            return res
        db.execute.side_effect = execute

        res = invite.send_due_invites(db)
        assert res.skipped == 1
        assert res.sent == 0

    def test_sends_and_marks_sent(self):
        from src.services.bankruptcy_alert import invite
        db = MagicMock()
        marks = []

        def execute(stmt, params=None):
            t = str(stmt)
            res = MagicMock()
            if "FROM message_outcomes m" in t and "JOIN subscribers" in t:
                res.fetchall.return_value = [self._row()]
            elif "FROM bankruptcy_alert_subscriptions" in t:
                res.first.return_value = None  # not yet subscribed
            elif "UPDATE message_outcomes" in t:
                marks.append(params)
            return res
        db.execute.side_effect = execute

        with patch("src.services.bankruptcy_alert.subscription.create_checkout",
                   return_value={"url": "https://stripe/cs_abc", "session_id": "cs_abc"}), \
             patch("src.services.email.send_email", return_value=True) as send:
            res = invite.send_due_invites(db)

        assert res.sent == 1
        send.assert_called_once()
        # the emailed link is the freshly-minted session url
        assert "https://stripe/cs_abc" in send.call_args.kwargs.get("body_html", "") \
            or "https://stripe/cs_abc" in send.call_args.args[2]
        assert marks[-1]["status"] == "sent"

    def test_recent_failure_left_scheduled_for_retry(self):
        from src.services.bankruptcy_alert import invite
        db = MagicMock()
        marks = []

        def execute(stmt, params=None):
            t = str(stmt)
            res = MagicMock()
            if "FROM message_outcomes m" in t and "JOIN subscribers" in t:
                res.fetchall.return_value = [self._row()]  # scheduled just now → not stale
            elif "FROM bankruptcy_alert_subscriptions" in t:
                res.first.return_value = None
            elif "UPDATE message_outcomes" in t:
                marks.append(params)
            return res
        db.execute.side_effect = execute

        with patch("src.services.bankruptcy_alert.subscription.create_checkout",
                   side_effect=RuntimeError("stripe down")):
            res = invite.send_due_invites(db)

        assert res.failed == 1
        assert res.gave_up == 0
        assert marks == []  # left 'scheduled' — no status change, retried next sweep


# ── alerts._projected_mrr_cents (pure, no DB) ────────────────────────────────

class TestProjectedMrr:
    def test_active_and_trialing_counted(self):
        from src.services.bankruptcy_alert.alerts import _projected_mrr_cents
        assert _projected_mrr_cents({"active": 10, "trialing": 3}) == 13 * 29700

    def test_past_due_and_canceled_excluded(self):
        from src.services.bankruptcy_alert.alerts import _projected_mrr_cents
        assert _projected_mrr_cents({"active": 2, "past_due": 5, "canceled": 9}) == 2 * 29700

    def test_empty_counts_zero(self):
        from src.services.bankruptcy_alert.alerts import _projected_mrr_cents
        assert _projected_mrr_cents({}) == 0

    def test_status_summary_includes_projected_mrr(self):
        from src.services.bankruptcy_alert.alerts import status_summary
        db = MagicMock()

        def execute(stmt, params=None):
            t = str(stmt)
            res = MagicMock()
            if "FROM bankruptcy_alert_subscriptions" in t and "GROUP BY status" in t:
                res.fetchall.return_value = [
                    SimpleNamespace(status="active", c=10),
                    SimpleNamespace(status="trialing", c=3),
                ]
            elif "FROM bankruptcy_filing_alerts" in t:
                res.first.return_value = SimpleNamespace(sent=0, failed=0, last_24h=0)
            elif "FROM bankruptcy_filings" in t:
                res.first.return_value = SimpleNamespace(c=0)
            elif "FROM message_outcomes" in t:
                res.first.return_value = SimpleNamespace(invites_sent=0, converted=0)
            return res

        db.execute.side_effect = execute
        summary = status_summary(db)
        assert summary["projected_mrr_cents"] == 13 * 29700


# ── alerts._paid_mrr_cents (pure, no DB) ─────────────────────────────────────

class TestPaidMrr:
    def test_active_only_counted(self):
        from src.services.bankruptcy_alert.alerts import _paid_mrr_cents
        assert _paid_mrr_cents({"active": 10, "trialing": 3}) == 10 * 29700

    def test_trialing_excluded_not_yet_paid(self):
        from src.services.bankruptcy_alert.alerts import _paid_mrr_cents
        assert _paid_mrr_cents({"trialing": 5}) == 0

    def test_empty_counts_zero(self):
        from src.services.bankruptcy_alert.alerts import _paid_mrr_cents
        assert _paid_mrr_cents({}) == 0


# ── alerts._invite_conversion_stats (mocked DB) ──────────────────────────────

class TestInviteConversionStats:
    def test_computes_rate_from_sent_and_converted(self):
        from src.services.bankruptcy_alert.alerts import _invite_conversion_stats
        db = MagicMock()
        db.execute.return_value.first.return_value = SimpleNamespace(invites_sent=20, converted=5)

        stats = _invite_conversion_stats(db)

        assert stats["invites_sent"] == 20
        assert stats["converted"] == 5
        assert stats["conversion_rate_pct"] == 25.0
        assert stats["window_days"] == 7

    def test_zero_invites_sent_no_divide_by_zero(self):
        from src.services.bankruptcy_alert.alerts import _invite_conversion_stats
        db = MagicMock()
        db.execute.return_value.first.return_value = SimpleNamespace(invites_sent=0, converted=0)

        stats = _invite_conversion_stats(db)

        assert stats["invites_sent"] == 0
        assert stats["conversion_rate_pct"] is None

    def test_status_summary_includes_paid_mrr_and_conversion(self):
        from src.services.bankruptcy_alert.alerts import status_summary
        db = MagicMock()

        def execute(stmt, params=None):
            t = str(stmt)
            res = MagicMock()
            if "FROM bankruptcy_alert_subscriptions" in t and "GROUP BY status" in t:
                res.fetchall.return_value = [
                    SimpleNamespace(status="active", c=10),
                    SimpleNamespace(status="trialing", c=3),
                ]
            elif "FROM bankruptcy_filing_alerts" in t:
                res.first.return_value = SimpleNamespace(sent=0, failed=0, last_24h=0)
            elif "FROM bankruptcy_filings" in t:
                res.first.return_value = SimpleNamespace(c=0)
            elif "FROM message_outcomes" in t:
                res.first.return_value = SimpleNamespace(invites_sent=20, converted=5)
            return res

        db.execute.side_effect = execute
        summary = status_summary(db)

        assert summary["paid_mrr_cents"] == 10 * 29700
        assert summary["invite_conversion"]["invites_sent"] == 20
        assert summary["invite_conversion"]["converted"] == 5
        assert summary["invite_conversion"]["conversion_rate_pct"] == 25.0
