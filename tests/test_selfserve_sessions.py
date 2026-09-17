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
    submit_session,
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


def test_find_possible_person_match_by_email(fresh_db):
    first = create_session(fresh_db, prefill_snapshot={"property_id": None, "fields": {}})
    first.token = str(uuid.uuid4())
    fresh_db.flush()
    first_person = resolve_or_create_person(fresh_db, source_reference=first.token)
    fresh_db.execute(
        text(
            "UPDATE selfserve_sessions SET person_id = :pid, contact = '{\"email\": \"dup@example.com\"}'::jsonb "
            "WHERE token = :t"
        ),
        {"pid": first_person, "t": first.token},
    )
    fresh_db.flush()

    second = create_session(fresh_db, prefill_snapshot={"property_id": None, "fields": {}})
    second.token = str(uuid.uuid4())
    fresh_db.flush()

    from src.services.selfserve_sessions import find_possible_person_match

    match = find_possible_person_match(fresh_db, email="DUP@example.com", phone=None, exclude_token=second.token)
    assert match == first_person

    no_match = find_possible_person_match(fresh_db, email="nobody@example.com", phone=None, exclude_token=second.token)
    assert no_match is None


def test_flag_possible_identity_match_writes_exceptions_row(fresh_db):
    from src.services.selfserve_sessions import flag_possible_identity_match

    new_person = resolve_or_create_person(fresh_db, source_reference="new")
    existing_person = resolve_or_create_person(fresh_db, source_reference="existing")
    token = str(uuid.uuid4())

    flag_possible_identity_match(fresh_db, new_person_id=new_person, existing_person_id=existing_person, session_token=token)
    fresh_db.flush()

    row = fresh_db.execute(
        text(
            "SELECT lane, channel, status, agent_name, autonomy_tier_at_send, person_id, payload "
            "FROM relay_approval_queue WHERE idempotency_key = :key"
        ),
        {"key": f"selfserve-possible-match-{token}"},
    ).mappings().first()
    assert row is not None
    assert row["lane"] == "EXCEPTIONS"
    assert row["status"] == "pending"
    assert row["agent_name"] == "selfserve_identity_check"
    assert row["autonomy_tier_at_send"] == "A"
    assert str(row["person_id"]) == new_person
    assert row["payload"]["existing_person_id"] == existing_person

    # Idempotent — calling again for the same token does not duplicate.
    flag_possible_identity_match(fresh_db, new_person_id=new_person, existing_person_id=existing_person, session_token=token)
    fresh_db.flush()
    count = fresh_db.execute(
        text("SELECT count(*) FROM relay_approval_queue WHERE idempotency_key = :key"),
        {"key": f"selfserve-possible-match-{token}"},
    ).scalar()
    assert count == 1


def test_submit_session_flags_possible_match(fresh_db):
    """End-to-end through submit_session: a second borrower using the same
    email as an earlier session gets flagged, not merged."""
    prefill = {"property_id": None, "fields": {}}
    first = create_session(fresh_db, prefill_snapshot=prefill)
    first.token = str(uuid.uuid4())
    fresh_db.flush()
    first_updated = submit_session(
        fresh_db, token=first.token, corrections=None, confirmations={},
        contact={"email": "shared@example.com", "phone": None}, consent_channels=[],
    )

    second = create_session(fresh_db, prefill_snapshot=prefill)
    second.token = str(uuid.uuid4())
    fresh_db.flush()
    second_updated = submit_session(
        fresh_db, token=second.token, corrections=None, confirmations={},
        contact={"email": "shared@example.com", "phone": None}, consent_channels=[],
    )

    # Records stay separate — never auto-merged, per spec L567.
    assert first_updated.person_id is not None
    assert second_updated.person_id is not None
    assert first_updated.person_id != second_updated.person_id

    flagged = fresh_db.execute(
        text("SELECT count(*) FROM relay_approval_queue WHERE idempotency_key = :key"),
        {"key": f"selfserve-possible-match-{second.token}"},
    ).scalar()
    assert flagged == 1
