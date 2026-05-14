"""
Scenario tests — segmentation hook callsites.

Verifies that reclassify_safe() is invoked (via spy) from every event-driven
callsite. Uses marker: scenario_platform.
"""

import pytest
from unittest.mock import patch, MagicMock


pytestmark = pytest.mark.scenario_platform


# ── helpers ──────────────────────────────────────────────────────────────────

_RECLASSIFY_PATH = "src.services.segmentation_engine.reclassify_safe"


def _make_stripe_session(subscriber_id=None, customer_id="cus_test_hook"):
    return {
        "id": "cs_test",
        "customer": customer_id,
        "subscription": "sub_test",
        "metadata": {
            "tier": "starter",
            "vertical": "roofing",
            "county_id": "hillsborough",
            "zip_codes": "33647",
        },
        "customer_details": {"email": "hook_test@example.com", "name": "Hook Tester", "phone": None},
    }


# ── stripe callsite tests ─────────────────────────────────────────────────────

class TestStripeHooks:
    def test_stripe_payment_intent_succeeded_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber
        sub = Subscriber(
            stripe_customer_id="cus_pi_hook",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="pi-hook-uuid",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        pi = MagicMock()
        pi.id = "pi_test"
        pi.customer = "cus_pi_hook"
        pi.metadata = {"subscriber_id": str(sub.id), "product": "wallet_topup"}
        pi.amount_received = 1000
        pi.amount = 1000

        with patch(_RECLASSIFY_PATH) as spy:
            from src.services.stripe_webhooks import _on_payment_intent_succeeded
            _on_payment_intent_succeeded(pi, fresh_db)
            spy.assert_called_once_with(sub.id, fresh_db)

    def test_stripe_subscription_deleted_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber
        sub = Subscriber(
            stripe_customer_id="cus_del_hook",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="del-hook-uuid",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        subscription = {"customer": "cus_del_hook", "id": "sub_del_hook"}

        with patch(_RECLASSIFY_PATH) as spy:
            from src.services.stripe_webhooks import _on_subscription_deleted
            _on_subscription_deleted(subscription, fresh_db)
            spy.assert_called_once_with(sub.id, fresh_db)

    def test_classify_failure_does_not_break_webhook(self, fresh_db):
        """A classify exception must not propagate out of reclassify_safe."""
        from src.services.segmentation_engine import reclassify_safe

        def _raise(sub_id, db):
            raise RuntimeError("classify boom")

        with patch(_RECLASSIFY_PATH, side_effect=_raise):
            # Should not raise — reclassify_safe swallows errors by contract
            # Test the helper directly since we're patching at module level
            pass

        # Verify reclassify_safe itself swallows errors
        with patch("src.services.segmentation_engine.classify", side_effect=RuntimeError("boom")):
            with patch("src.services.revenue_signal.recompute"):
                reclassify_safe(9999, fresh_db)  # must not raise


# ── wallet_engine callsite ────────────────────────────────────────────────────

class TestWalletDebitHook:
    def test_wallet_debit_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber
        from src.services.wallet_engine import enroll, debit

        sub = Subscriber(
            stripe_customer_id="cus_debit_hook",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="debit-hook-uuid",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()
        enroll(sub.id, "starter_wallet", db=fresh_db)

        with patch(_RECLASSIFY_PATH) as spy:
            debit(sub.id, "view_contact", fresh_db)
            spy.assert_called_once_with(sub.id, fresh_db)


# ── wallet_to_lock callsite ───────────────────────────────────────────────────

class TestMarkLockCandidateHook:
    def test_mark_lock_candidate_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber
        from src.services.wallet_to_lock import mark_lock_candidate

        sub = Subscriber(
            stripe_customer_id="cus_lc_hook",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid="lc-hook-uuid",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        with patch(_RECLASSIFY_PATH) as spy:
            mark_lock_candidate(fresh_db, sub.id, "33647")
            spy.assert_called_once_with(sub.id, fresh_db)


# ── sms_commands callsite ─────────────────────────────────────────────────────

class TestSmsReplyHook:
    def test_sms_reply_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber, SmsOptIn
        from datetime import datetime, timezone
        import uuid

        run_id = uuid.uuid4().hex[:8]
        # phone must be all-numeric — hex may contain a-f which normalize() strips
        phone = f"+1813{abs(hash(run_id)) % 10000000:07d}"

        sub = Subscriber(
            stripe_customer_id=f"cus_sms_hook_{run_id}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"sms-hook-uuid-{run_id}",
            status="active",
            phone=phone,
        )
        fresh_db.add(sub)
        fresh_db.flush()
        fresh_db.add(SmsOptIn(
            phone=phone,
            subscriber_id=sub.id,
            source="widget",
            opt_in_message="test",
            opted_in_at=datetime.now(timezone.utc),
        ))
        fresh_db.flush()

        with patch(_RECLASSIFY_PATH) as spy:
            from src.services.sms_commands import dispatch
            dispatch(phone, "BALANCE", fresh_db)
            spy.assert_called_once_with(sub.id, fresh_db)


# ── stripe checkout callsite ──────────────────────────────────────────────────

class TestCheckoutCompletedHook:
    def test_stripe_checkout_completed_triggers_reclassify(self, fresh_db):
        import uuid
        run_id = uuid.uuid4().hex[:8]
        session = {
            "id": f"cs_{run_id}",
            "customer": f"cus_co_{run_id}",
            "subscription": f"sub_co_{run_id}",
            "metadata": {
                "tier": "starter",
                "vertical": "roofing",
                "county_id": "hillsborough",
                "zip_codes": "33600",
            },
            "customer_details": {
                "email": f"co_{run_id}@example.com",
                "name": "Checkout Tester",
                "phone": None,
            },
        }
        with patch(_RECLASSIFY_PATH) as spy, \
             patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
             patch("src.services.stripe_webhooks.send_welcome_email", create=True), \
             patch("src.services.stripe_webhooks._send_first_leads_email", create=True):
            from src.services.stripe_webhooks import _on_checkout_completed
            _on_checkout_completed(session, fresh_db)
            assert spy.call_count == 1


# ── lead unlock callsite ──────────────────────────────────────────────────────

class TestLeadUnlockHook:
    def test_lead_unlock_payment_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber, Property
        import uuid
        run_id = uuid.uuid4().hex[:8]

        sub = Subscriber(
            stripe_customer_id=f"cus_lu_{run_id}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"lu-uuid-{run_id}",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        prop = Property(
            parcel_id=f"lu-parcel-{run_id}",
            address=f"123 Lu St {run_id}",
            county_id="hillsborough",
        )
        fresh_db.add(prop)
        fresh_db.flush()

        pi = MagicMock()
        pi.id = f"pi_lu_{run_id}"
        pi.customer = f"cus_lu_{run_id}"
        pi.metadata = {"property_id": str(prop.id), "product": "lead_unlock"}
        pi.amount_received = 400

        with patch(_RECLASSIFY_PATH) as spy, \
             patch("src.services.stripe_webhooks._send_lead_unlock_email"):
            from src.services.stripe_webhooks import _on_lead_unlock_payment
            _on_lead_unlock_payment(pi, fresh_db)
            spy.assert_called_once_with(sub.id, fresh_db)


# ── wallet subscription invoice callsite ─────────────────────────────────────

class TestWalletSubInvoiceHook:
    def test_wallet_subscription_invoice_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber
        import uuid
        run_id = uuid.uuid4().hex[:8]

        sub = Subscriber(
            stripe_customer_id=f"cus_wsi_{run_id}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"wsi-uuid-{run_id}",
            status="active",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        invoice = {
            "id": f"in_{run_id}",
            "payment_intent": f"pi_{run_id}",
            "subscription": f"sub_wsi_{run_id}",
            "metadata": {"subscriber_id": str(sub.id), "tier": "starter_wallet"},
        }

        with patch(_RECLASSIFY_PATH) as spy, \
             patch("src.services.stripe_webhooks._extract_wallet_sub_metadata",
                   return_value={"subscriber_id": str(sub.id), "tier": "starter_wallet",
                                 "subscription_id": f"sub_wsi_{run_id}"}), \
             patch("src.services.wallet_engine.enroll"), \
             patch("src.services.stripe_webhooks.send_sms", create=True):
            from src.services.stripe_webhooks import _on_wallet_subscription_invoice
            _on_wallet_subscription_invoice(invoice, fresh_db)
            spy.assert_called_once_with(sub.id, fresh_db)


# ── auto_mode_followup click callsite ─────────────────────────────────────────

class TestAutoModeFollowupHook:
    def test_click_tracking_triggers_reclassify(self, fresh_db):
        from src.core.models import Subscriber, MessageOutcome
        from datetime import datetime, timedelta, timezone
        from contextlib import contextmanager
        import uuid
        run_id = uuid.uuid4().hex[:8]

        sub = Subscriber(
            stripe_customer_id=f"cus_amf_{run_id}",
            tier="starter",
            vertical="roofing",
            county_id="hillsborough",
            event_feed_uuid=f"amf-uuid-{run_id}",
            status="active",
            ghl_contact_id=f"ghl_{run_id}",
        )
        fresh_db.add(sub)
        fresh_db.flush()

        now = datetime.now(timezone.utc)
        outcome = MessageOutcome(
            subscriber_id=sub.id,
            message_type="sms",
            template_id="auto_mode_first_text",
            sent_at=now - timedelta(hours=25),
            replied_at=None,
            clicked_at=None,
        )
        fresh_db.add(outcome)
        fresh_db.flush()

        # run() uses get_db_context() internally — redirect it to fresh_db so
        # test data is visible and reclassify_safe fires in the same session.
        @contextmanager
        def _fake_ctx():
            yield fresh_db

        with patch(_RECLASSIFY_PATH) as spy, \
             patch("src.tasks.auto_mode_followup._trigger_vm_for_subscriber"), \
             patch("src.tasks.auto_mode_followup.get_db_context", return_value=_fake_ctx()):
            from src.tasks.auto_mode_followup import run
            run(dry_run=False)
            assert spy.call_count >= 1
