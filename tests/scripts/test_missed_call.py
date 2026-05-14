import requests
from src.core.database import get_db_context
from src.core.models import DBPRContact, Subscriber, WebhookEvent
from src.services.signed_links import encode_landing_token

BASE_URL = "http://localhost:8001"

# ── Step 1: verify subscriber + SMS_SENT event ────────────────────────────────
print("=" * 50)
print("STEP 1 — subscriber + events after SMS send")
print("=" * 50)
with get_db_context() as db:
    c = db.query(DBPRContact).filter_by(license_number='TEST-SMS-001').first()
    sub = db.get(Subscriber, c.subscriber_id)
    sub_id      = sub.id
    feed_uuid   = sub.event_feed_uuid
    print('subscriber_id :', c.subscriber_id)
    print('signup_source :', sub.signup_source)
    print('signed_up_at  :', c.signed_up_at)
    events = db.query(WebhookEvent).filter(
        WebhookEvent.subscriber_id == sub_id
    ).all()
    print('events so far :', [e.event_type for e in events])
    assert "SIGNUP_COMPLETED"       in [e.event_type for e in events], "MISSING: SIGNUP_COMPLETED"
    assert "SIGNUP_SOURCE_ATTRIBUTED" in [e.event_type for e in events], "MISSING: SIGNUP_SOURCE_ATTRIBUTED"
    assert "SMS_SENT"               in [e.event_type for e in events], "MISSING: SMS_SENT"
    print("PASS\n")

# ── Step 2: resolve token ─────────────────────────────────────────────────────
print("=" * 50)
print("STEP 2 — token resolution (TOKEN_RESOLVED)")
print("=" * 50)
token = encode_landing_token(sub_id, 'cora_sms', ttl_hours=72)
print('token (first 40 chars):', token[:40] if token else "NONE — check LANDING_TOKEN_SECRET")
assert token, "encode_landing_token returned None — LANDING_TOKEN_SECRET not set"

resp = requests.post(
    f"{BASE_URL}/api/landing/resolve-token",
    json={"token": token},
    timeout=10,
)
print('status:', resp.status_code)
assert resp.status_code == 200, f"resolve-token failed: {resp.text}"
data = resp.json()
print('feed_uuid     :', data["feed_uuid"])
print('subscriber_id :', data["subscriber_id"])
print('signup_source :', data["signup_source"])
assert data["feed_uuid"] == feed_uuid, "feed_uuid mismatch"
assert data["signup_source"] == "cora_sms", "signup_source mismatch"
print("PASS\n")

# ── Step 3: proof moment ──────────────────────────────────────────────────────
print("=" * 50)
print("STEP 3 — proof leads (PROOF_MOMENT_VIEWED)")
print("=" * 50)
resp = requests.get(
    f"{BASE_URL}/api/proof-leads",
    params={"vertical": "roofing", "county_id": "hillsborough", "feed_uuid": feed_uuid},
    timeout=10,
)
print('status:', resp.status_code)
assert resp.status_code == 200, f"proof-leads failed: {resp.text}"
proof = resp.json()
leads = proof if isinstance(proof, list) else proof.get("leads", [])
print('leads returned:', len(leads))
print("PASS\n")

# ── Step 4: verify all events logged ─────────────────────────────────────────
print("=" * 50)
print("STEP 4 — final event log check")
print("=" * 50)
with get_db_context() as db:
    events = db.query(WebhookEvent).filter(
        WebhookEvent.subscriber_id == sub_id
    ).all()
    event_types = [e.event_type for e in events]
    print('all events:', event_types)
    assert "TOKEN_RESOLVED"     in event_types, "MISSING: TOKEN_RESOLVED"
    assert "PROOF_MOMENT_VIEWED" in event_types, "MISSING: PROOF_MOMENT_VIEWED"
    print("PASS\n")

print("=" * 50)
print("ALL STEPS PASSED")
print("=" * 50)
