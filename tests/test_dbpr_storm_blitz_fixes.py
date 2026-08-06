"""
Regression tests for PR #213 review fixes:

  Issue 1 (hard blocker): DBPR thread IDs (DBPR-{id}) were rejected by the
  Cora→Relay contract validator which only accepted OPP-YYYY-##### format.
  Fix: extended _THREAD_ID_RE to also match DBPR-[0-9]+.

  Issue 2 (duplicate outreach): dbpr_email_sender could double-send to contacts
  already in the blitz pipeline. Fix: (a) dbpr_email_sender excludes contacts
  with active outbound_drafts; (b) _sync_relay_sent_statuses marks contacts
  sent after Relay dispatches.
"""
import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# Issue 1 — Cora→Relay contract accepts DBPR-{id} thread IDs
# ---------------------------------------------------------------------------

class TestCoraRelayThreadIdContract:
    def test_opp_format_still_accepted(self):
        from src.agents.contracts.cora_to_relay import _THREAD_ID_RE
        assert _THREAD_ID_RE.match("OPP-2026-00001")
        assert _THREAD_ID_RE.match("OPP-2024-99999")

    def test_dbpr_format_accepted(self):
        from src.agents.contracts.cora_to_relay import _THREAD_ID_RE
        assert _THREAD_ID_RE.match("DBPR-1")
        assert _THREAD_ID_RE.match("DBPR-123")
        assert _THREAD_ID_RE.match("DBPR-999999")

    def test_lowercase_dbpr_rejected(self):
        from src.agents.contracts.cora_to_relay import _THREAD_ID_RE
        assert not _THREAD_ID_RE.match("dbpr-123")

    def test_garbage_rejected(self):
        from src.agents.contracts.cora_to_relay import _THREAD_ID_RE
        assert not _THREAD_ID_RE.match("")
        assert not _THREAD_ID_RE.match("OPP-2026")
        assert not _THREAD_ID_RE.match("DBPR-")

    def test_validate_handoff_accepts_dbpr_thread_id(self):
        """validate_handoff must not raise for DBPR-format thread IDs."""
        from unittest.mock import patch
        from src.agents.contracts.cora_to_relay import validate_handoff

        # Patch DISPATCHERS so 'email' is registered without the side-effect import
        with patch("src.agents.contracts.cora_to_relay.DISPATCHERS", {"email": object()}):
            handoff = validate_handoff(
                idempotency_key="test-idem-key-001",
                channel="email",
                recipient="contractor@example.com",
                payload={"subject": "Storm leads available", "body": "Hello contractor"},
                thread_id="DBPR-42",
            )
        assert handoff.thread_id == "DBPR-42"

    def test_validate_handoff_rejects_old_lowercase_dbpr(self):
        """The old lowercase dbpr-{id} format must still be rejected (no regression)."""
        from unittest.mock import patch
        from src.agents.contracts.cora_to_relay import validate_handoff

        with patch("src.agents.contracts.cora_to_relay.DISPATCHERS", {"email": object()}):
            with pytest.raises(ValidationError, match="OPP-YYYY-#####"):
                validate_handoff(
                    idempotency_key="test-idem-key-002",
                    channel="email",
                    recipient="contractor@example.com",
                    payload={"subject": "Storm leads", "body": "Hello"},
                    thread_id="dbpr-42",
                )


# ---------------------------------------------------------------------------
# Issue 2a — dbpr_email_sender skips contacts with active blitz drafts
# ---------------------------------------------------------------------------

def _pg_url():
    try:
        from config.settings import get_settings
        url = get_settings().database_url
        return str(url) if url else None
    except Exception:
        return None


pytestmark_pg = pytest.mark.skipif(
    _pg_url() is None,
    reason="DATABASE_URL not set — requires real Postgres",
)

LICENSE_PREFIX = "STGBLITZ-"


def _cleanup():
    from src.core.database import get_db_context
    from src.core.models import DBPRContact, OutboundDraft
    with get_db_context() as db:
        # Remove any drafts referencing our test contacts first (FK order)
        contacts = db.query(DBPRContact).filter(
            DBPRContact.license_number.like(f"{LICENSE_PREFIX}%")
        ).all()
        if contacts:
            thread_ids = [f"DBPR-{c.id}" for c in contacts]
            db.query(OutboundDraft).filter(
                OutboundDraft.opportunity_thread_id.in_(thread_ids)
            ).delete(synchronize_session=False)
        db.query(DBPRContact).filter(
            DBPRContact.license_number.like(f"{LICENSE_PREFIX}%")
        ).delete(synchronize_session=False)
        db.commit()


@pytest.fixture(autouse=False)
def cleanup_blitz():
    _cleanup()
    yield
    _cleanup()


@pytestmark_pg
def test_sender_skips_contact_with_active_blitz_draft(cleanup_blitz):
    """dbpr_email_sender must not send to a contact that has an active outbound draft."""
    from datetime import datetime, timezone
    from unittest.mock import patch
    from src.core.database import get_db_context
    from src.core.models import DBPRContact, OutboundDraft

    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        contact = DBPRContact(
            license_number=f"{LICENSE_PREFIX}DRAFT",
            license_type_code="RC",
            full_name="Blitz, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email="stgblitz-draft@example.com",
            email_status="not_sent",
            created_at=now,
        )
        db.add(contact)
        db.flush()

        # Simulate an active blitz draft for this contact
        db.add(OutboundDraft(
            draft_id="stgblitz-test-draft-001",
            opportunity_thread_id=f"DBPR-{contact.id}",
            buyer_entity_id=None,
            venture_key="hillsborough_distress",
            cell_id="dbpr_storm_blitz",
            offer="founder_tier",
            avenue="storm_restoration_contractors",
            angle="storm_damage_lead_pipeline",
            subject="Storm leads for your area",
            body="Hello contractor, we have leads.",
            recommended_channel="email",
            confidence_score=100,
            status="draft",
        ))
        db.commit()

    send_mock = patch("src.tasks.dbpr_email_sender.send_email", return_value=True)
    with send_mock as mock_send:
        from src.tasks.dbpr_email_sender import run_dbpr_email_sender
        stats = run_dbpr_email_sender(county_id="hillsborough", limit=1000, delay=0)

    mock_send.assert_not_called()
    assert stats["total"] == 0


@pytestmark_pg
def test_sender_sends_contact_without_blitz_draft(cleanup_blitz):
    """dbpr_email_sender must still send to contacts with no active blitz draft."""
    from datetime import datetime, timezone
    from unittest.mock import patch
    from src.core.database import get_db_context
    from src.core.models import DBPRContact

    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}NODRAFT",
            license_type_code="RC",
            full_name="NoDraft, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email="stgblitz-nodraft@example.com",
            email_status="not_sent",
            created_at=now,
        ))
        db.commit()

    with patch("src.tasks.dbpr_email_sender.send_email", return_value=True):
        from src.tasks.dbpr_email_sender import run_dbpr_email_sender
        stats = run_dbpr_email_sender(county_id="hillsborough", limit=1000, delay=0)

    assert stats["sent"] == 1


# ---------------------------------------------------------------------------
# Issue 2b — _sync_relay_sent_statuses marks contacts sent after Relay dispatch
# ---------------------------------------------------------------------------

@pytestmark_pg
def test_sync_relay_sent_statuses_marks_contact_sent(cleanup_blitz):
    """After Relay marks a queue item sent, _sync_relay_sent_statuses must flip
    the corresponding dbpr_contact to email_status='sent'."""
    from datetime import datetime, timezone
    from src.core.database import get_db_context
    from src.core.models import DBPRContact
    from src.agents.cora.ingestion.dbpr_storm_producer import _sync_relay_sent_statuses

    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        contact = DBPRContact(
            license_number=f"{LICENSE_PREFIX}SYNC",
            license_type_code="RC",
            full_name="Sync, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email="stgblitz-sync@example.com",
            email_status="not_sent",
            created_at=now,
        )
        db.add(contact)
        db.flush()
        contact_id = contact.id

        # Simulate a Relay queue row in 'sent' state for this contact
        from sqlalchemy import text
        db.execute(text("""
            INSERT INTO relay_approval_queue
              (channel, recipient, thread_id, subject, payload, status,
               dispatched_at, created_at, updated_at, venture_key)
            VALUES
              ('email', 'stgblitz-sync@example.com', :thread_id,
               'Storm leads', '{}', 'sent',
               now(), now(), now(), 'hillsborough_distress')
        """), {"thread_id": f"DBPR-{contact_id}"})
        db.commit()

    with get_db_context() as db:
        count = _sync_relay_sent_statuses(db)
        db.commit()

    assert count >= 1

    with get_db_context() as db:
        c = db.get(DBPRContact, contact_id)
        assert c is not None
        assert c.email_status == "sent"
        assert c.email_sent_at is not None


@pytestmark_pg
def test_sync_relay_sent_statuses_ignores_pending_rows(cleanup_blitz):
    """_sync_relay_sent_statuses must not mark contacts sent for non-sent Relay rows."""
    from datetime import datetime, timezone
    from src.core.database import get_db_context
    from src.core.models import DBPRContact
    from src.agents.cora.ingestion.dbpr_storm_producer import _sync_relay_sent_statuses

    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        contact = DBPRContact(
            license_number=f"{LICENSE_PREFIX}PENDING",
            license_type_code="RC",
            full_name="Pending, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email="stgblitz-pending@example.com",
            email_status="not_sent",
            created_at=now,
        )
        db.add(contact)
        db.flush()
        contact_id = contact.id

        from sqlalchemy import text
        db.execute(text("""
            INSERT INTO relay_approval_queue
              (channel, recipient, thread_id, subject, payload, status,
               created_at, updated_at, venture_key)
            VALUES
              ('email', 'stgblitz-pending@example.com', :thread_id,
               'Storm leads', '{}', 'pending',
               now(), now(), 'hillsborough_distress')
        """), {"thread_id": f"DBPR-{contact_id}"})
        db.commit()

    with get_db_context() as db:
        _sync_relay_sent_statuses(db)
        db.commit()

    with get_db_context() as db:
        c = db.get(DBPRContact, contact_id)
        assert c is not None
        assert c.email_status == "not_sent"
