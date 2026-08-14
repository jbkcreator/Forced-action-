"""
B0-06 — Voice-path affirmative opt-in check.

Voice consent (PEWC) is distinct from the generic marketing consent captured
today — see docs/adr/0030. These tests drive the real capture endpoints and
the real dispatch gate, not internals.
"""
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import patch, PropertyMock, MagicMock

import pytest
from sqlalchemy import text

pytestmark = pytest.mark.scenario


@pytest.fixture
def client(fresh_db):
    from fastapi.testclient import TestClient
    from src.api.main import app
    import src.api.main as main_mod
    import src.core.database as db_mod

    def _override_db():
        yield fresh_db

    app.dependency_overrides[main_mod.get_db] = _override_db
    app.dependency_overrides[db_mod.get_db] = _override_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def test_voice_consent_migration_is_idempotent(pg_engine):
    if pg_engine is None:
        import pytest
        pytest.skip("DATABASE_URL not configured — skipping migration test")

    from migrations.apply_b0_06_voice_consent import main as apply_migration

    apply_migration()
    apply_migration()  # re-run must not raise

    with pg_engine.connect() as conn:
        cols = {
            row.column_name
            for row in conn.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'consent_acceptances'
                AND column_name LIKE 'voice_consent%'
            """)).fetchall()
        }

    assert cols == {"voice_consent_at", "voice_consent_text", "voice_consent_version"}


# ── capture: free_signup ─────────────────────────────────────────────────────

def test_free_signup_captures_voice_consent_when_accepted(client, fresh_db):
    email = f"vc_a_{uuid.uuid4().hex[:6]}@e.com"
    r = client.post("/api/free-signup", json={
        "email": email, "vertical": "roofing", "county_id": "hillsborough",
        "consent_acceptance": {
            "terms_accepted": True,
            "accepted_text_hash": "hash123",
            "voice_consent_accepted": True,
            "voice_consent_text": "I agree to receive automated/AI voice calls...",
            "voice_consent_version": "2026.07",
        },
    })
    assert r.status_code == 201, r.text

    from src.api.deps import VOICE_CONSENT_DISCLOSURES
    row = fresh_db.execute(text(
        "SELECT voice_consent_at, voice_consent_text, voice_consent_version "
        "FROM consent_acceptances WHERE email = :email"
    ), {"email": email}).first()
    assert row.voice_consent_at is not None
    assert row.voice_consent_text == VOICE_CONSENT_DISCLOSURES["2026.07"]
    assert row.voice_consent_version == "2026.07"


def test_free_signup_leaves_voice_consent_null_when_not_accepted(client, fresh_db):
    email = f"vc_b_{uuid.uuid4().hex[:6]}@e.com"
    r = client.post("/api/free-signup", json={
        "email": email, "vertical": "roofing", "county_id": "hillsborough",
        "consent_acceptance": {
            "terms_accepted": True,
            "accepted_text_hash": "hash123",
        },
    })
    assert r.status_code == 201, r.text

    row = fresh_db.execute(text(
        "SELECT voice_consent_at FROM consent_acceptances WHERE email = :email"
    ), {"email": email}).first()
    assert row.voice_consent_at is None


# ── capture: checkout ────────────────────────────────────────────────────────

def _mock_stripe_session(monkeypatch):
    from types import SimpleNamespace
    import stripe

    fake_session = SimpleNamespace(
        id="cs_test_fake123",
        client_secret="cs_test_fake123_secret",
        amount_total=9900,
    )
    monkeypatch.setattr(stripe.checkout.Session, "create", lambda **kw: fake_session)


def test_checkout_captures_voice_consent_when_accepted(client, fresh_db, monkeypatch):
    _mock_stripe_session(monkeypatch)
    email = f"vc_c_{uuid.uuid4().hex[:6]}@e.com"
    r = client.post("/api/checkout", json={
        "tier": "starter", "vertical": "roofing", "county_id": "hillsborough",
        "zip_codes": ["99999"], "email": email,
        "consent_acceptance": {
            "terms_accepted": True,
            "accepted_text_hash": "hash123",
            "voice_consent_accepted": True,
            "voice_consent_text": "I agree to receive automated/AI voice calls...",
            "voice_consent_version": "2026.07",
        },
    })
    assert r.status_code == 200, r.text

    from src.api.deps import VOICE_CONSENT_DISCLOSURES
    row = fresh_db.execute(text(
        "SELECT voice_consent_at, voice_consent_text, voice_consent_version "
        "FROM consent_acceptances WHERE email = :email AND source_flow = 'checkout'"
    ), {"email": email}).first()
    assert row.voice_consent_at is not None
    assert row.voice_consent_text == VOICE_CONSENT_DISCLOSURES["2026.07"]
    assert row.voice_consent_version == "2026.07"


def test_checkout_leaves_voice_consent_null_when_not_accepted(client, fresh_db, monkeypatch):
    _mock_stripe_session(monkeypatch)
    email = f"vc_d_{uuid.uuid4().hex[:6]}@e.com"
    r = client.post("/api/checkout", json={
        "tier": "starter", "vertical": "roofing", "county_id": "hillsborough",
        "zip_codes": ["99998"], "email": email,
        "consent_acceptance": {"terms_accepted": True, "accepted_text_hash": "hash123"},
    })
    assert r.status_code == 200, r.text

    row = fresh_db.execute(text(
        "SELECT voice_consent_at FROM consent_acceptances WHERE email = :email AND source_flow = 'checkout'"
    ), {"email": email}).first()
    assert row.voice_consent_at is None


# ── link: Stripe webhook backfills subscriber_id onto checkout consent row ──

def _stub_init_stripe():
    return patch("src.services.stripe_webhooks._init_stripe", return_value=True)


def _stub_settings_secret():
    from config.settings import AppSettings
    fake_secret = MagicMock()
    fake_secret.get_secret_value.return_value = "whsec_voice_consent_test"
    return patch.object(
        AppSettings, "active_stripe_webhook_secret",
        new_callable=PropertyMock, return_value=fake_secret,
    )


def _stub_construct_event(event):
    return patch(
        "src.services.stripe_webhooks.stripe.Webhook.construct_event",
        return_value=event,
    )


def _post_checkout_completed(event, db):
    from src.services.stripe_webhooks import handle_webhook
    raw = json.dumps(event).encode("utf-8")
    with _stub_init_stripe(), _stub_settings_secret(), _stub_construct_event(event), \
         patch("src.services.stripe_webhooks.push_subscriber_to_ghl"), \
         patch("src.services.email.send_welcome_email"), \
         patch("src.services.stripe_webhooks._send_first_leads_email"):
        return handle_webhook(raw, sig_header="t=stub,v1=stub", db=db)


def _make_checkout_completed_event(*, customer_id, sub_id, email, zip_code):
    return {
        "id": f"evt_vc_{uuid.uuid4().hex[:8]}",
        "type": "checkout.session.completed",
        "created": int(datetime.now(timezone.utc).timestamp()),
        "data": {
            "object": {
                "id": f"cs_{uuid.uuid4().hex[:8]}",
                "customer": customer_id,
                "subscription": sub_id,
                "payment_status": "paid",
                "metadata": {
                    "tier": "pro", "vertical": "roofing", "county_id": "hillsborough",
                    "zip_codes": zip_code, "is_founding": "False",
                },
                "customer_details": {"email": email, "name": "Voice Consent Test"},
            }
        },
    }


def test_stripe_webhook_links_subscriber_id_onto_checkout_consent_row(fresh_db):
    uid = uuid.uuid4().hex[:8]
    email = f"vc_link_{uid}@example.com"

    # Simulate the checkout consent row already written (no subscriber yet).
    fresh_db.execute(text("""
        INSERT INTO consent_acceptances
            (email, terms_version, privacy_version, accepted_at, source_flow,
             accepted_text_hash, voice_consent_at, voice_consent_text, voice_consent_version, created_at)
        VALUES (:email, '2026.06', '2026.06', now(), 'checkout',
                'hash123', now(), 'I agree to voice calls...', '2026.07', now())
    """), {"email": email})
    fresh_db.commit()

    event = _make_checkout_completed_event(
        customer_id=f"cus_{uid}", sub_id=f"sub_{uid}", email=email, zip_code="99996",
    )
    _post_checkout_completed(event, fresh_db)
    fresh_db.commit()

    sub_id = fresh_db.execute(text(
        "SELECT id FROM subscribers WHERE email = :email"
    ), {"email": email}).scalar_one()

    row = fresh_db.execute(text(
        "SELECT subscriber_id FROM consent_acceptances "
        "WHERE email = :email AND source_flow = 'checkout'"
    ), {"email": email}).first()
    assert row.subscriber_id == sub_id


def test_stripe_webhook_links_despite_email_case_mismatch(fresh_db):
    """Checkout lowercases the email (payload validator); Stripe may echo a
    different case. The backfill must still link, case-insensitively."""
    uid = uuid.uuid4().hex[:8]
    stored_email = f"vc_case_{uid}@example.com"          # as stored at checkout
    stripe_email = f"VC_Case_{uid}@Example.com"          # as Stripe echoes it

    fresh_db.execute(text("""
        INSERT INTO consent_acceptances
            (email, terms_version, privacy_version, accepted_at, source_flow,
             accepted_text_hash, voice_consent_at, created_at)
        VALUES (:email, '2026.06', '2026.06', now(), 'checkout', 'h', now(), now())
    """), {"email": stored_email})
    fresh_db.commit()

    event = _make_checkout_completed_event(
        customer_id=f"cus_{uid}", sub_id=f"sub_{uid}", email=stripe_email, zip_code="99994",
    )
    _post_checkout_completed(event, fresh_db)
    fresh_db.commit()

    row = fresh_db.execute(text(
        "SELECT subscriber_id FROM consent_acceptances "
        "WHERE email = :email AND source_flow = 'checkout'"
    ), {"email": stored_email}).first()
    assert row.subscriber_id is not None


def test_stripe_webhook_link_is_idempotent_on_replay(fresh_db):
    uid = uuid.uuid4().hex[:8]
    email = f"vc_link_replay_{uid}@example.com"

    fresh_db.execute(text("""
        INSERT INTO consent_acceptances
            (email, terms_version, privacy_version, accepted_at, source_flow, accepted_text_hash, created_at)
        VALUES (:email, '2026.06', '2026.06', now(), 'checkout', 'hash123', now())
    """), {"email": email})
    fresh_db.commit()

    event = _make_checkout_completed_event(
        customer_id=f"cus_{uid}", sub_id=f"sub_{uid}", email=email, zip_code="99995",
    )
    _post_checkout_completed(event, fresh_db)
    fresh_db.commit()
    # Replay with the same event id — must not raise or duplicate the link.
    _post_checkout_completed(event, fresh_db)
    fresh_db.commit()

    rows = fresh_db.execute(text(
        "SELECT subscriber_id FROM consent_acceptances "
        "WHERE email = :email AND source_flow = 'checkout'"
    ), {"email": email}).fetchall()
    assert len(rows) == 1
    assert rows[0].subscriber_id is not None


# ── gate: no automated voice call fires without stored voice consent ────────
# This IS the Definition of Done for B0-06.

def _make_sub_profile(sub_id=1, phone="+13135550101", vertical="roofing"):
    return {"id": sub_id, "name": "Test User", "phone": phone, "vertical": vertical}


def _settings_mock(agent_id="agent_123"):
    from unittest.mock import MagicMock as _MM
    s = _MM()
    s.synthflow_api_key = _MM()
    s.synthflow_api_key.get_secret_value.return_value = "sf_key"
    s.synthflow_api_base = "https://api.synthflow.ai/v2"
    s.synthflow_outbound_agent_roofing = agent_id
    return s


def _invoke_voice_drop(*, has_voice_consent, call_id="c1"):
    import sys
    from unittest.mock import MagicMock
    from src.agents.graphs import synthflow_voice_drop as vd
    from src.services.compliance_gator import ComplianceResult

    profile = _make_sub_profile()
    db_ctx = MagicMock()
    db_ctx.__enter__ = MagicMock(return_value=db_ctx)
    db_ctx.__exit__ = MagicMock(return_value=False)
    db_ctx.execute.return_value.first.return_value = None  # no recent dedup drop
    db_ctx.add = MagicMock()
    db_ctx.commit = MagicMock()

    hierarchy_result = {"action_allowed": True, "kill_switch_color": "green"}
    sms_module = MagicMock()
    sms_module.send_sms = MagicMock(return_value=True)

    with patch("src.agents.tools.read_tools.get_subscriber_profile", return_value=profile), \
         patch.object(vd, "get_subscriber_profile", return_value=profile), \
         patch.object(vd, "get_db_context", return_value=db_ctx), \
         patch.object(vd, "run_decision_hierarchy", return_value=hierarchy_result), \
         patch.object(vd, "validate_outbound", return_value=ComplianceResult(allowed=True)), \
         patch.object(vd, "has_voice_consent", return_value=has_voice_consent), \
         patch.object(vd, "initiate_call", return_value=call_id) as mock_initiate, \
         patch("src.services.allotment_engine.consume", return_value=True), \
         patch.object(vd, "allotment_consume", return_value=True), \
         patch.dict(sys.modules, {"src.services.sms_compliance": sms_module}), \
         patch("config.settings.get_settings", return_value=_settings_mock()):
        graph = vd.build_synthflow_voice_drop_graph().compile()
        result = graph.invoke({
            "decision_id": "d-vc-test",
            "subscriber_id": 1,
            "event_type": "high_intent_no_convert",
            "event_payload": {"vertical": "roofing"},
        })
    result["_initiate_mock"] = mock_initiate
    return result


def test_voice_drop_aborts_without_voice_consent():
    result = _invoke_voice_drop(has_voice_consent=False)
    assert result["terminal_status"] == "aborted"
    assert result["failure_reason"] == "voice_consent_required"
    result["_initiate_mock"].assert_not_called()


def test_voice_drop_proceeds_with_voice_consent():
    result = _invoke_voice_drop(has_voice_consent=True)
    assert result["sent"] is True
    assert result["call_id"] == "c1"
    result["_initiate_mock"].assert_called_once()


# ── the real consent query, exercised against a real DB ─────────────────────
# The gate tests above patch has_voice_consent; these prove the actual SELECT.

def _insert_subscriber(fresh_db, email):
    from src.core.models import Subscriber
    uid = uuid.uuid4().hex[:10]
    sub = Subscriber(
        stripe_customer_id=f"cus_vc_{uid}",
        tier="starter", vertical="roofing", county_id="hillsborough",
        status="active", email=email, event_feed_uuid=f"feed-{uid}",
    )
    fresh_db.add(sub)
    fresh_db.flush()
    return sub.id


def test_has_voice_consent_true_when_consent_row_present(fresh_db):
    from src.services.compliance_gator import has_voice_consent
    email = f"hvc_a_{uuid.uuid4().hex[:6]}@e.com"
    sid = _insert_subscriber(fresh_db, email)
    fresh_db.execute(text("""
        INSERT INTO consent_acceptances
            (email, subscriber_id, terms_version, privacy_version, accepted_at,
             source_flow, accepted_text_hash, voice_consent_at, voice_consent_text,
             voice_consent_version, created_at)
        VALUES (:email, :sid, '2026.06', '2026.06', now(),
                'checkout', 'h', now(), 'I consent to AI voice calls.', '2026.06', now())
    """), {"email": email, "sid": sid})
    fresh_db.flush()
    assert has_voice_consent(sid, fresh_db) is True


def test_has_voice_consent_false_when_row_lacks_voice_consent_at(fresh_db):
    from src.services.compliance_gator import has_voice_consent
    email = f"hvc_b_{uuid.uuid4().hex[:6]}@e.com"
    sid = _insert_subscriber(fresh_db, email)
    # Consent row exists but voice_consent_at is NULL (marketing-only, no PEWC).
    fresh_db.execute(text("""
        INSERT INTO consent_acceptances
            (email, subscriber_id, terms_version, privacy_version, accepted_at,
             source_flow, accepted_text_hash, consent_scope, created_at)
        VALUES (:email, :sid, '2026.06', '2026.06', now(),
                'checkout', 'h', 'marketing', now())
    """), {"email": email, "sid": sid})
    fresh_db.flush()
    assert has_voice_consent(sid, fresh_db) is False


def test_has_voice_consent_false_when_no_row_at_all(fresh_db):
    from src.services.compliance_gator import has_voice_consent
    email = f"hvc_c_{uuid.uuid4().hex[:6]}@e.com"
    sid = _insert_subscriber(fresh_db, email)
    fresh_db.flush()
    assert has_voice_consent(sid, fresh_db) is False
