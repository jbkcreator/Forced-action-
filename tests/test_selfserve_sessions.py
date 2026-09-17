"""WP-7 — session lifecycle helpers: suppression check, consent gate, and the
abandonment-sweep query helpers. Uses `fresh_db` (real Postgres, rolled back
after each test)."""
import uuid

from sqlalchemy import text

from src.services.selfserve_sessions import (
    create_session,
    is_backflip_suppressed,
    list_consented_abandoned_contacts,
    list_stale_session_tokens,
    mark_abandoned,
    record_consent,
    resolve_or_create_person,
)


def test_is_backflip_suppressed_true_for_active_contact(fresh_db):
    fresh_db.execute(
        text(
            "INSERT INTO fa_max_backflip_campaign_contacts (identifier_kind, identifier_value, active) "
            "VALUES ('email', 'suppressed@example.com', true)"
        )
    )
    fresh_db.flush()
    assert is_backflip_suppressed(fresh_db, email="suppressed@example.com") is True
    assert is_backflip_suppressed(fresh_db, email="SUPPRESSED@example.com") is True  # case-insensitive


def test_is_backflip_suppressed_false_for_unknown_contact(fresh_db):
    assert is_backflip_suppressed(fresh_db, email="nobody@example.com", phone="+18135551234") is False


def test_is_backflip_suppressed_false_for_inactive_row(fresh_db):
    fresh_db.execute(
        text(
            "INSERT INTO fa_max_backflip_campaign_contacts (identifier_kind, identifier_value, active) "
            "VALUES ('email', 'stale@example.com', false)"
        )
    )
    fresh_db.flush()
    assert is_backflip_suppressed(fresh_db, email="stale@example.com") is False


def test_is_backflip_suppressed_false_with_no_identifiers(fresh_db):
    assert is_backflip_suppressed(fresh_db) is False


def test_list_stale_session_tokens_and_mark_abandoned(fresh_db):
    prefill = {"property_id": None, "fields": {}}
    stale = create_session(fresh_db, prefill_snapshot=prefill)
    stale.token = str(uuid.uuid4())
    fresh = create_session(fresh_db, prefill_snapshot=prefill)
    fresh.token = str(uuid.uuid4())
    fresh_db.flush()

    fresh_db.execute(
        text("UPDATE selfserve_sessions SET last_activity_at = now() - interval '48 hours' WHERE token = :t"),
        {"t": stale.token},
    )
    fresh_db.flush()

    stale_tokens = list_stale_session_tokens(fresh_db, older_than_hours=24)
    assert stale.token in stale_tokens
    assert fresh.token not in stale_tokens  # 0 hours old, not stale yet

    mark_abandoned(fresh_db, stale.token)
    fresh_db.flush()
    row = fresh_db.execute(
        text("SELECT status FROM selfserve_sessions WHERE token = :t"), {"t": stale.token}
    ).first()
    assert row.status == "abandoned"


def test_mark_abandoned_never_touches_confirmed_session(fresh_db):
    prefill = {"property_id": None, "fields": {}}
    session_row = create_session(fresh_db, prefill_snapshot=prefill)
    session_row.token = str(uuid.uuid4())
    fresh_db.flush()
    fresh_db.execute(
        text("UPDATE selfserve_sessions SET status = 'confirmed' WHERE token = :t"),
        {"t": session_row.token},
    )
    fresh_db.flush()

    mark_abandoned(fresh_db, session_row.token)
    fresh_db.flush()
    row = fresh_db.execute(
        text("SELECT status FROM selfserve_sessions WHERE token = :t"), {"t": session_row.token}
    ).first()
    assert row.status == "confirmed"  # unchanged — a completed form is not abandoned


def test_list_consented_abandoned_contacts_filters_by_channel(fresh_db):
    person_id = resolve_or_create_person(fresh_db, source_reference="test-consented")
    record_consent(fresh_db, person_id, ["email"])

    prefill = {"property_id": None, "fields": {}}
    session_row = create_session(fresh_db, prefill_snapshot=prefill)
    session_row.token = str(uuid.uuid4())
    fresh_db.flush()
    fresh_db.execute(
        text(
            "UPDATE selfserve_sessions SET status = 'abandoned', person_id = :pid, "
            "contact = '{\"email\": \"x@example.com\"}'::jsonb WHERE token = :t"
        ),
        {"pid": person_id, "t": session_row.token},
    )
    fresh_db.flush()

    email_results = list_consented_abandoned_contacts(fresh_db, "email")
    assert any(r["token"] == session_row.token for r in email_results)

    sms_results = list_consented_abandoned_contacts(fresh_db, "sms")
    assert not any(r["token"] == session_row.token for r in sms_results)
