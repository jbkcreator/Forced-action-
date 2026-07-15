"""
Tests for the proactive referral prompt (deal-win / lead-pack-delivery
triggered SMS+email nudge) — src/services/referral_prompt_service.py and its
two hook sites plus funnel-state advancement.

Covers the four sprint verify criteria:
  1. deal-win / lead-pack-delivery fires SMS+email with the correct cached
     vertical copy.
  2. funnel state advances shown -> shared -> confirmed.
  3. a subscriber with no qualifying event gets no prompt.
  4. existing reactive paths (process_signup, confirm_purchase) are unaffected.
"""
import uuid
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.core.models import (
    DealOutcome,
    LeadPackPurchase,
    Property,
    ReferralForwardCopy,
    Subscriber,
)


def _make_sub(sub_id, vertical="roofing", phone="8135550100", email="sub@example.com"):
    sub = MagicMock()
    sub.id = sub_id
    sub.vertical = vertical
    sub.phone = phone
    sub.email = email
    return sub


def _funnel_row(db, subscriber_id):
    return db.execute(
        text("SELECT * FROM referral_prompt_funnel WHERE subscriber_id = :sid"),
        {"sid": subscriber_id},
    ).mappings().first()


class TestMaybeSendReferralPromptUnit:
    """Unit tests against mock_db — no real DB required."""

    def test_no_vertical_is_noop(self, mock_db):
        from src.services.referral_prompt_service import maybe_send_referral_prompt
        sub = _make_sub(1, vertical=None)
        result = maybe_send_referral_prompt(
            sub, mock_db, trigger_type="deal_win",
            trigger_source_table="deal_outcomes", trigger_source_id=1,
        )
        assert result is False
        mock_db.execute.assert_not_called()

    def test_none_subscriber_is_noop(self, mock_db):
        from src.services.referral_prompt_service import maybe_send_referral_prompt
        result = maybe_send_referral_prompt(
            None, mock_db, trigger_type="deal_win",
            trigger_source_table="deal_outcomes", trigger_source_id=1,
        )
        assert result is False

    def test_within_cooldown_skips_send(self, mock_db):
        from src.services.referral_prompt_service import maybe_send_referral_prompt
        sub = _make_sub(2)
        mock_db.execute.return_value.first.return_value = (1,)  # cooldown hit
        with patch("src.services.sms_compliance.send_sms") as mock_sms:
            result = maybe_send_referral_prompt(
                sub, mock_db, trigger_type="deal_win",
                trigger_source_table="deal_outcomes", trigger_source_id=2,
            )
        assert result is False
        mock_sms.assert_not_called()

    def test_no_cached_copy_skips_send(self, mock_db):
        from src.services.referral_prompt_service import maybe_send_referral_prompt
        sub = _make_sub(3)
        mock_db.execute.return_value.first.return_value = None  # no cooldown hit
        with patch("src.services.referral_engine.ensure_referral_code", return_value="abc12345"), \
             patch("src.services.forward_pack_renderer.get_current_copy", return_value=None), \
             patch("src.services.sms_compliance.send_sms") as mock_sms, \
             patch("src.services.email.send_email") as mock_email:
            result = maybe_send_referral_prompt(
                sub, mock_db, trigger_type="deal_win",
                trigger_source_table="deal_outcomes", trigger_source_id=3,
            )
        assert result is False
        mock_sms.assert_not_called()
        mock_email.assert_not_called()

    def test_sends_sms_and_email_with_cached_copy(self, mock_db):
        from src.services.referral_prompt_service import maybe_send_referral_prompt
        sub = _make_sub(4)
        mock_db.execute.return_value.first.return_value = None
        with patch("src.services.referral_engine.ensure_referral_code", return_value="abc12345"), \
             patch("src.services.forward_pack_renderer.get_current_copy",
                   return_value="Roofers love a referral. Share the wealth!"), \
             patch("src.services.sms_compliance.send_sms", return_value=True) as mock_sms, \
             patch("src.services.email.send_email", return_value=True) as mock_email, \
             patch("config.settings.get_settings") as mock_settings:
            mock_settings.return_value.app_base_url = "https://app.example.com"
            result = maybe_send_referral_prompt(
                sub, mock_db, trigger_type="deal_win",
                trigger_source_table="deal_outcomes", trigger_source_id=4,
            )
        assert result is True
        sms_body = mock_sms.call_args[0][1]
        assert "Roofers love a referral" in sms_body
        assert "https://app.example.com/share/abc12345" in sms_body
        email_body = mock_email.call_args.kwargs["body_text"]
        assert "Roofers love a referral" in email_body


class TestReferralPromptIntegration:
    """Integration tests against real Postgres (fresh_db) — exercises the
    real hook wiring and the real funnel table."""

    def _mk_sub(self, fresh_db, vertical="roofing"):
        uid = uuid.uuid4().hex[:8]
        phone = "813" + str(uuid.uuid4().int)[:7]
        sub = Subscriber(
            stripe_customer_id=f"cus_rp_{uid}", tier="starter", vertical=vertical,
            county_id="hillsborough", event_feed_uuid=f"rp-{uid}",
            email=f"rp_{uid}@example.com", phone=phone, status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        return sub

    def _seed_copy(self, fresh_db, vertical, body="Nice work! Pass it on, link inside."):
        """Upsert so re-runs (or a leftover row from an earlier committed test)
        never collide with the UNIQUE(vertical, week_start) constraint —
        mirrors forward_pack_renderer.render_weekly()'s own on-conflict pattern."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        from src.services.forward_pack_renderer import _current_week_start

        week_start = _current_week_start()
        stmt = pg_insert(ReferralForwardCopy).values(
            vertical=vertical, week_start=week_start, body=body,
        ).on_conflict_do_update(
            constraint="uq_referral_forward_copy_vertical_week",
            set_={"body": body},
        )
        fresh_db.execute(stmt)
        fresh_db.flush()

    def test_deal_win_hook_fires_prompt_and_creates_funnel_row(self, fresh_db):
        """Criterion 1 + 2 (shown): a closed_won DealOutcome drives the real
        deal_outcome_effects hook and lands a 'shown' funnel row with the
        cached vertical copy."""
        from src.services.deal_outcome_effects import record_outcome_side_effects

        sub = self._mk_sub(fresh_db)
        self._seed_copy(fresh_db, sub.vertical)
        prop = Property(
            parcel_id=f"P-RP-{uuid.uuid4().hex[:8]}", address="1 Referral Prompt Way",
            city="Tampa", state="FL", zip="33601", county_id="hillsborough",
        )
        fresh_db.add(prop)
        fresh_db.flush()
        outcome = DealOutcome(
            subscriber_id=sub.id, property_id=prop.id,
            deal_size_bucket="5_10k", deal_amount=6000, deal_date=date.today(),
            pipeline_stage="closed_won", confidence_tier="subscriber_reported",
            outcome_source="subscriber_tap",
        )
        fresh_db.add(outcome)
        fresh_db.flush()

        with patch("src.services.cora_suppression.create_suppression"), \
             patch("src.services.win_graphic.generate", return_value=None), \
             patch("src.services.win_autopsy.record_win_autopsy"), \
             patch("src.services.sms_compliance.send_sms", return_value=True) as mock_sms, \
             patch("src.services.email.send_email", return_value=True) as mock_email:
            record_outcome_side_effects(outcome, sub, fresh_db)

        mock_sms.assert_called_once()
        mock_email.assert_called_once()
        row = _funnel_row(fresh_db, sub.id)
        assert row is not None
        assert row["trigger_type"] == "deal_win"
        assert row["state"] == "shown"
        assert row["sms_sent"] is True
        assert row["email_sent"] is True

    def test_skip_bucket_fires_no_prompt(self, fresh_db):
        """Criterion 3: a closed_lost ('skip') outcome creates no funnel row."""
        from src.services.deal_outcome_effects import record_outcome_side_effects

        sub = self._mk_sub(fresh_db)
        self._seed_copy(fresh_db, sub.vertical)
        prop = Property(
            parcel_id=f"P-RPS-{uuid.uuid4().hex[:8]}", address="2 Skip Way",
            city="Tampa", state="FL", zip="33601", county_id="hillsborough",
        )
        fresh_db.add(prop)
        fresh_db.flush()
        outcome = DealOutcome(
            subscriber_id=sub.id, property_id=prop.id,
            deal_size_bucket="skip", deal_date=date.today(),
            pipeline_stage="closed_lost", confidence_tier="subscriber_reported",
            outcome_source="subscriber_tap",
        )
        fresh_db.add(outcome)
        fresh_db.flush()

        with patch("src.services.cora_suppression.create_suppression"), \
             patch("src.services.sms_compliance.send_sms") as mock_sms, \
             patch("src.services.email.send_email") as mock_email:
            record_outcome_side_effects(outcome, sub, fresh_db)

        mock_sms.assert_not_called()
        mock_email.assert_not_called()
        assert _funnel_row(fresh_db, sub.id) is None

    def test_subscriber_with_no_event_has_no_funnel_row(self, fresh_db):
        """Criterion 3: a freshly seeded subscriber with zero triggering
        events has zero funnel rows."""
        sub = self._mk_sub(fresh_db)
        assert _funnel_row(fresh_db, sub.id) is None

    def test_share_page_advances_shown_to_shared(self, fresh_db):
        """Criterion 2 (shared): visiting /share/{code} advances the most
        recent 'shown' row for that referrer."""
        from src.api.main import app

        sub = self._mk_sub(fresh_db)
        sub.referral_code = f"shr{uuid.uuid4().hex[:5]}"
        self._seed_copy(fresh_db, sub.vertical)
        fresh_db.flush()
        src_id = uuid.uuid4().int % 1_000_000_000
        fresh_db.execute(
            text(
                "INSERT INTO referral_prompt_funnel "
                "(subscriber_id, trigger_type, trigger_source_table, trigger_source_id, referral_code) "
                "VALUES (:sid, 'deal_win', 'deal_outcomes', :src, :code)"
            ),
            {"sid": sub.id, "src": src_id, "code": sub.referral_code},
        )
        fresh_db.commit()

        client = TestClient(app)
        try:
            resp = client.get(f"/share/{sub.referral_code}")
            assert resp.status_code == 200

            row = _funnel_row(fresh_db, sub.id)
            assert row["state"] == "shared"
            assert row["shared_at"] is not None
        finally:
            # This test commits (TestClient's get_db opens its own session,
            # so the nested-savepoint rollback the other tests rely on
            # wouldn't be visible to it) — clean up explicitly, matching
            # the _cleanup() convention in test_deal_capture_cde11.py.
            fresh_db.execute(
                text("DELETE FROM referral_prompt_funnel WHERE subscriber_id = :sid"),
                {"sid": sub.id},
            )
            fresh_db.execute(
                text("DELETE FROM subscribers WHERE id = :sid"), {"sid": sub.id}
            )
            fresh_db.commit()

    def test_confirm_purchase_call_site_advances_to_confirmed(self, fresh_db):
        """Criterion 2 (confirmed) + 4 (referral_engine unaffected): drives the
        REAL process_signup -> confirm_purchase flow, then the same mark_confirmed
        call the stripe_webhooks.py call sites make, and asserts referral_engine's
        own behavior (pending -> confirmed) is untouched."""
        from src.services.referral_engine import confirm_purchase, ensure_referral_code, process_signup
        from src.services.referral_prompt_service import mark_confirmed

        referrer = self._mk_sub(fresh_db)
        referee = self._mk_sub(fresh_db)
        code = ensure_referral_code(referrer.id, fresh_db)
        src_id = uuid.uuid4().int % 1_000_000_000
        fresh_db.execute(
            text(
                "INSERT INTO referral_prompt_funnel "
                "(subscriber_id, trigger_type, trigger_source_table, trigger_source_id, referral_code) "
                "VALUES (:sid, 'deal_win', 'deal_outcomes', :src, :code)"
            ),
            {"sid": referrer.id, "src": src_id, "code": code},
        )
        fresh_db.flush()

        event = process_signup(referee.id, code, fresh_db)
        assert event is not None
        assert event.status == "pending"  # referral_engine behavior — unchanged

        with patch("src.services.referral_notifier.publish"):
            confirmed = confirm_purchase(referee.id, fresh_db)
        assert confirmed.status == "confirmed"  # referral_engine behavior — unchanged

        mark_confirmed(confirmed.referrer_subscriber_id, confirmed.id, fresh_db)

        row = _funnel_row(fresh_db, referrer.id)
        assert row["state"] == "confirmed"
        assert row["confirmed_referral_event_id"] == confirmed.id

    def test_cooldown_blocks_second_prompt_within_30_days(self, fresh_db):
        """A subscriber with an existing 'shown' row inside the cooldown
        window gets no second prompt or funnel row on a second trigger."""
        from src.services.referral_prompt_service import maybe_send_referral_prompt

        sub = self._mk_sub(fresh_db)
        self._seed_copy(fresh_db, sub.vertical)
        prior_src_id = uuid.uuid4().int % 1_000_000_000
        new_src_id = uuid.uuid4().int % 1_000_000_000
        fresh_db.execute(
            text(
                "INSERT INTO referral_prompt_funnel "
                "(subscriber_id, trigger_type, trigger_source_table, trigger_source_id, referral_code, prompt_shown_at) "
                "VALUES (:sid, 'deal_win', 'deal_outcomes', :src, 'coldcode', :shown_at)"
            ),
            {"sid": sub.id, "src": prior_src_id, "shown_at": datetime.now(timezone.utc) - timedelta(days=5)},
        )
        fresh_db.flush()

        with patch("src.services.sms_compliance.send_sms") as mock_sms, \
             patch("src.services.email.send_email") as mock_email:
            result = maybe_send_referral_prompt(
                sub, fresh_db, trigger_type="lead_pack_delivery",
                trigger_source_table="lead_pack_purchases", trigger_source_id=new_src_id,
            )
        assert result is False
        mock_sms.assert_not_called()
        mock_email.assert_not_called()

    def test_lead_pack_fulfillment_hook_fires_on_delivered(self, fresh_db):
        """Criterion 1: the lead-pack-delivery hook fires from the real
        fulfill_purchase() 'passed' branch, not from the purchase/reservation
        webhook."""
        from src.tasks.lead_pack_fulfillment_sweep import fulfill_purchase

        sub = self._mk_sub(fresh_db)
        self._seed_copy(fresh_db, sub.vertical)
        prop_ids = []
        for i in range(5):
            prop = Property(
                parcel_id=f"P-LP-{uuid.uuid4().hex[:8]}", address=f"{i} Lead Pack Ave",
                city="Tampa", state="FL", zip="33601", county_id="hillsborough",
            )
            fresh_db.add(prop)
            fresh_db.flush()
            prop_ids.append(prop.id)

        purchase = LeadPackPurchase(
            subscriber_id=sub.id, zip_code="33601", vertical=sub.vertical,
            county_id="hillsborough",
            stripe_payment_intent_id=f"pi_rp_{uuid.uuid4().hex[:8]}",
            status="enriching", lead_ids=prop_ids, amount_cents=9900,
        )
        fresh_db.add(purchase)
        fresh_db.flush()

        all_hit = {
            pid: {"match_success": True, "mobile_phone": "8135550100",
                  "landline": None, "email": None, "mailing_address": None}
            for pid in prop_ids
        }

        with patch("src.services.tracerfy_fallback.hot_enrich_properties", return_value=all_hit), \
             patch("src.tasks.lead_pack_fulfillment_sweep._send_delivery_email"), \
             patch("src.services.win_story_publisher.publish_win_story"), \
             patch("src.services.referral_prompt_service.maybe_send_referral_prompt") as mock_prompt:
            outcome = fulfill_purchase(fresh_db, purchase)

        assert outcome == "delivered"
        mock_prompt.assert_called_once()
        _, kwargs = mock_prompt.call_args
        assert kwargs["trigger_type"] == "lead_pack_delivery"
        assert kwargs["trigger_source_table"] == "lead_pack_purchases"
        assert kwargs["trigger_source_id"] == purchase.id
