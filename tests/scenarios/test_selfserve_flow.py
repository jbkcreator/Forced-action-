"""WP-7 WI-7 — full self-serve flow against the real DB with FakeBackflipPort:
click -> prefill -> correct -> confirm -> handoff. Session snapshot stays
immutable; consent rows land in fa_max_person_consent; the client's Done-When
(recognized property, completed handoff) is exercised end to end.

Run:
    pytest tests/scenarios/test_selfserve_flow.py -v -m scenario
"""
from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from src.api.main import app
from src.core.database import get_db_context
from src.services.tracked_links import mint_link

pytestmark = pytest.mark.scenario

_TAG = "ztest-selfserve"


def _make_property(db) -> int:
    row = db.execute(
        text(
            "INSERT INTO properties (parcel_id, address, city, state, zip, county_id, "
            "beds, baths, sq_ft, year_built, created_at, updated_at) "
            f"VALUES ('{_TAG}-' || :suffix, '789 Flow St', 'Tampa', 'FL', '33602', "
            "'hillsborough', 3, 2, 1400, 1985, now(), now()) RETURNING id"
        ),
        {"suffix": uuid.uuid4().hex[:8]},
    ).first()
    return row.id


@pytest.fixture
def property_id():
    with get_db_context() as db:
        pid = _make_property(db)
        db.commit()
    yield pid
    with get_db_context() as db:
        db.execute(text("DELETE FROM selfserve_sessions WHERE property_id = :pid"), {"pid": pid})
        db.execute(
            text(
                "DELETE FROM tracked_link_clicks WHERE tracked_link_id IN "
                "(SELECT id FROM tracked_links WHERE property_id = :pid)"
            ),
            {"pid": pid},
        )
        db.execute(text("DELETE FROM tracked_links WHERE property_id = :pid"), {"pid": pid})
        db.execute(text("DELETE FROM properties WHERE id = :pid"), {"pid": pid})
        db.commit()


@pytest.fixture
def tracked_link_slug(property_id):
    with get_db_context() as db:
        link = mint_link(
            db, kind="source", label="scenario test link",
            created_by="scenario_test", property_id=property_id,
        )
        db.commit()
        slug = link.slug
    return slug


def test_click_to_handoff_end_to_end(monkeypatch, tracked_link_slug, property_id):
    from config.settings import settings as global_settings
    monkeypatch.setattr(global_settings, "backflip_adapter", "fake")

    client = TestClient(app, follow_redirects=False)

    click_resp = client.get(f"/go/{tracked_link_slug}")
    assert click_resp.status_code == 302
    token = click_resp.headers["location"].split("/selfserve/")[1]

    screen_resp = client.get(f"/selfserve/{token}")
    assert screen_resp.status_code == 200
    assert "789 Flow St" in screen_resp.text  # property recognized on click, per Done-When

    submit_resp = client.post(
        f"/api/selfserve/{token}/submit",
        data={
            "correction__address": "789 Flow St Unit B",
            "q__purchase_price": "250000",
            "q__rehab_budget": "40000",
            "q__exit_strategy": "flip",
            "contact_name": "Test Borrower",
            "contact_email": "scenario-borrower@example.com",
            "contact_phone": "8135551234",
            "consent": "on",
        },
        follow_redirects=False,
    )
    assert submit_resp.status_code == 302
    assert submit_resp.headers["location"].startswith("https://fake-backflip.test/prequal")

    with get_db_context() as db:
        row = db.execute(
            text(
                "SELECT status, handoff_ref, person_id, corrections, confirmations, prefill_snapshot "
                "FROM selfserve_sessions WHERE token = :token"
            ),
            {"token": token},
        ).mappings().first()
        assert row is not None
        assert row["status"] == "handed_off"
        assert row["handoff_ref"] == f"fake-{token}"
        assert row["confirmations"]["exit_strategy"] == "flip"
        assert row["corrections"]["address"] == "789 Flow St Unit B"
        # Immutable — the original snapshot still shows what we actually
        # presented, unmutated by the borrower's correction.
        assert row["prefill_snapshot"]["fields"]["address"]["value"] == "789 Flow St"

        person_id = row["person_id"]
        assert person_id is not None
        consent_rows = db.execute(
            text("SELECT channel, consented FROM fa_max_person_consent WHERE person_id = :pid"),
            {"pid": person_id},
        ).fetchall()
        channels = {r.channel for r in consent_rows}
        assert channels == {"email", "sms"}
        assert all(r.consented for r in consent_rows)

        # Teardown of rows this test itself created beyond the fixtures.
        # selfserve_sessions FKs to fa_max_persons, so it must go first.
        db.execute(text("DELETE FROM selfserve_sessions WHERE token = :token"), {"token": token})
        db.execute(text("DELETE FROM fa_max_person_consent WHERE person_id = :pid"), {"pid": person_id})
        db.execute(text("DELETE FROM fa_max_persons WHERE person_id = :pid"), {"pid": person_id})
        db.commit()


def test_submit_rejects_malformed_email(tracked_link_slug):
    client = TestClient(app, follow_redirects=False)
    click_resp = client.get(f"/go/{tracked_link_slug}")
    token = click_resp.headers["location"].split("/selfserve/")[1]

    resp = client.post(
        f"/api/selfserve/{token}/submit",
        data={"contact_name": "Bad Email", "contact_email": "not-an-email", "consent": "on"},
    )
    assert resp.status_code == 400
    assert "email" in resp.json()["detail"].lower()


def test_submit_rejects_invalid_phone(tracked_link_slug):
    client = TestClient(app, follow_redirects=False)

    click_resp = client.get(f"/go/{tracked_link_slug}")
    token = click_resp.headers["location"].split("/selfserve/")[1]
    resp = client.post(
        f"/api/selfserve/{token}/submit",
        data={"contact_name": "Bad Phone", "contact_email": "ok@example.com", "contact_phone": "123", "consent": "on"},
    )
    assert resp.status_code == 400
    assert "phone" in resp.json()["detail"].lower()


def test_submit_rejects_missing_phone(tracked_link_slug):
    """Contact info is mandatory — phone is no longer optional."""
    client = TestClient(app, follow_redirects=False)
    click_resp = client.get(f"/go/{tracked_link_slug}")
    token = click_resp.headers["location"].split("/selfserve/")[1]
    resp = client.post(
        f"/api/selfserve/{token}/submit",
        data={"contact_name": "No Phone", "contact_email": "nophone@example.com", "consent": "on"},
    )
    assert resp.status_code == 400
    assert "phone" in resp.json()["detail"].lower()


def test_submit_rejects_missing_property_address(monkeypatch):
    """Property details are mandatory — a session with no recognized
    property must not be submittable without at least typing an address."""
    from config.settings import settings as global_settings
    monkeypatch.setattr(global_settings, "backflip_adapter", "fake")

    with get_db_context() as db:
        link = mint_link(db, kind="source", label="no-property test link", created_by="scenario_test")
        db.commit()
        slug = link.slug

    client = TestClient(app, follow_redirects=False)
    click_resp = client.get(f"/go/{slug}")
    token = click_resp.headers["location"].split("/selfserve/")[1]

    resp = client.post(
        f"/api/selfserve/{token}/submit",
        data={
            "contact_name": "No Address", "contact_email": "noaddress@example.com",
            "contact_phone": "8135551234", "consent": "on",
        },
    )
    assert resp.status_code == 400
    assert "address" in resp.json()["detail"].lower()

    with get_db_context() as db:
        db.execute(text("DELETE FROM tracked_link_clicks WHERE tracked_link_id = :lid"), {"lid": link.id})
        db.execute(text("DELETE FROM selfserve_sessions WHERE tracked_link_id = :lid"), {"lid": link.id})
        db.execute(text("DELETE FROM tracked_links WHERE id = :lid"), {"lid": link.id})
        db.commit()


def test_unknown_slug_degrades_to_generic_flow_not_404():
    client = TestClient(app, follow_redirects=False)
    resp = client.get("/go/definitely-not-a-real-slug")
    assert resp.status_code == 302
    assert "/selfserve/" in resp.headers["location"]


def test_suppressed_contact_holds_off_instead_of_handing_off(monkeypatch, tracked_link_slug, property_id):
    """Client's answered attribution rule (client-response doc Q5): if a
    prospect already has an active Backflip campaign touch in motion, Forced
    Action holds off rather than sending a competing outreach. The session's
    own answers must still be saved — only the handoff redirect is held."""
    from config.settings import settings as global_settings
    monkeypatch.setattr(global_settings, "backflip_adapter", "fake")

    suppressed_email = f"suppressed-{uuid.uuid4().hex[:8]}@example.com"
    with get_db_context() as db:
        db.execute(
            text(
                "INSERT INTO fa_max_backflip_campaign_contacts (identifier_kind, identifier_value, active) "
                "VALUES ('email', :email, true)"
            ),
            {"email": suppressed_email},
        )
        db.commit()

    client = TestClient(app, follow_redirects=False)
    click_resp = client.get(f"/go/{tracked_link_slug}")
    token = click_resp.headers["location"].split("/selfserve/")[1]

    submit_resp = client.post(
        f"/api/selfserve/{token}/submit",
        data={
            "q__exit_strategy": "flip",
            "contact_name": "Suppressed Borrower",
            "contact_email": suppressed_email,
            "contact_phone": "8135551234",
            "consent": "on",
        },
        follow_redirects=False,
    )
    assert submit_resp.status_code == 200  # held — not a 302 to Backflip
    assert "no further action needed" in submit_resp.text.lower()

    with get_db_context() as db:
        row = db.execute(
            text("SELECT status, handoff_ref, person_id FROM selfserve_sessions WHERE token = :token"),
            {"token": token},
        ).mappings().first()
        assert row is not None
        assert row["status"] == "confirmed"  # saved, but never handed_off
        assert row["handoff_ref"] is None

        person_id = row["person_id"]

        # Spec's failure-behavior section: a blocked send must be "surfaced
        # to me" (Josh), not just logged server-side.
        exceptions_row = db.execute(
            text(
                "SELECT lane, status, payload FROM relay_approval_queue "
                "WHERE idempotency_key = :key"
            ),
            {"key": f"selfserve-suppressed-handoff-{token}"},
        ).mappings().first()
        assert exceptions_row is not None
        assert exceptions_row["lane"] == "EXCEPTIONS"
        assert exceptions_row["payload"]["reason"] == "handoff_held_active_backflip_touch"

        db.execute(text("DELETE FROM relay_approval_queue WHERE idempotency_key = :key"),
                   {"key": f"selfserve-suppressed-handoff-{token}"})
        db.execute(text("DELETE FROM selfserve_sessions WHERE token = :token"), {"token": token})
        db.execute(text("DELETE FROM fa_max_person_consent WHERE person_id = :pid"), {"pid": person_id})
        db.execute(text("DELETE FROM fa_max_persons WHERE person_id = :pid"), {"pid": person_id})
        db.execute(
            text("DELETE FROM fa_max_backflip_campaign_contacts WHERE identifier_value = :email"),
            {"email": suppressed_email},
        )
        db.commit()


def test_borrower_binding_survives_click_page_and_submit(monkeypatch):
    """WI-1 follow-up (2026-09-18): a buyer_entity_id resolved at link-mint
    time (an explicit name match) must be recognized at click, pre-fill the
    known questions on the page, and survive submit_session's own
    property-derived resolution unchanged — it must never be silently
    overwritten by whoever currently owns the (unrelated, unknown) property."""
    from config.settings import settings as global_settings
    monkeypatch.setattr(global_settings, "backflip_adapter", "fake")

    suffix = uuid.uuid4().hex[:8]
    name = f"{_TAG}-borrower-{suffix}"
    with get_db_context() as db:
        row = db.execute(
            text(
                "INSERT INTO buyer_entities (canonical_name, entity_type, primary_mailing_address, "
                "confidence_score, verification_status, total_purchase_count, total_cash_volume) "
                "VALUES (:name, 'Individual', '1 Test Way, Tampa, FL, 33602', 90, 'verified', 5, 900000) "
                "RETURNING id"
            ),
            {"name": name},
        ).first()
        entity_id = row.id
        link = mint_link(
            db, kind="source", label="scenario borrower link",
            created_by="scenario_test", buyer_entity_id=entity_id,
        )
        db.commit()
        slug = link.slug

    try:
        client = TestClient(app, follow_redirects=False)

        click_resp = client.get(f"/go/{slug}")
        assert click_resp.status_code == 302
        token = click_resp.headers["location"].split("/selfserve/")[1]

        screen_resp = client.get(f"/selfserve/{token}")
        assert screen_resp.status_code == 200
        assert "Borrower recognized" in screen_resp.text
        assert name in screen_resp.text  # entity_name question pre-filled
        assert 'value="5"' in screen_resp.text  # prior_flip_count pre-filled

        submit_resp = client.post(
            f"/api/selfserve/{token}/submit",
            data={
                "q__exit_strategy": "flip",
                "q__entity_name": name,
                "q__prior_flip_count": "5",
                "manual_address": "999999 Nonexistent Rd",  # no property match — must not derive a different entity
                "contact_name": "Borrower Binding Test",
                "contact_email": f"borrower-binding-{suffix}@example.com",
                "contact_phone": "8135551234",
                "consent": "on",
            },
            follow_redirects=False,
        )
        assert submit_resp.status_code == 302

        with get_db_context() as db:
            session_row = db.execute(
                text("SELECT buyer_entity_id, person_id FROM selfserve_sessions WHERE token = :token"),
                {"token": token},
            ).mappings().first()
            assert session_row is not None
            assert session_row["buyer_entity_id"] == entity_id
            person_id = session_row["person_id"]

            # selfserve_sessions FKs to fa_max_persons, so it must go first.
            db.execute(text("DELETE FROM selfserve_sessions WHERE token = :token"), {"token": token})
            db.execute(text("DELETE FROM fa_max_person_consent WHERE person_id = :pid"), {"pid": person_id})
            db.execute(text("DELETE FROM fa_max_persons WHERE person_id = :pid"), {"pid": person_id})
            db.commit()
    finally:
        with get_db_context() as db:
            db.execute(text("DELETE FROM tracked_link_clicks WHERE tracked_link_id = :lid"), {"lid": link.id})
            db.execute(text("DELETE FROM tracked_links WHERE id = :lid"), {"lid": link.id})
            db.execute(text("DELETE FROM buyer_entities WHERE id = :eid"), {"eid": entity_id})
            db.commit()
