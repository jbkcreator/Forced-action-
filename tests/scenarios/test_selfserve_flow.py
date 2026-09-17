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
            db, kind="property_mailer", label="scenario test mailer",
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
        assert row["status"] == "confirmed"  # saved, but never handed_off
        assert row["handoff_ref"] is None

        person_id = row["person_id"]
        db.execute(text("DELETE FROM selfserve_sessions WHERE token = :token"), {"token": token})
        db.execute(text("DELETE FROM fa_max_person_consent WHERE person_id = :pid"), {"pid": person_id})
        db.execute(text("DELETE FROM fa_max_persons WHERE person_id = :pid"), {"pid": person_id})
        db.execute(
            text("DELETE FROM fa_max_backflip_campaign_contacts WHERE identifier_value = :email"),
            {"email": suppressed_email},
        )
        db.commit()
