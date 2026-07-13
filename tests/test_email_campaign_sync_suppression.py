"""
Real-Postgres integration test: email_campaign_sync's Instantly two-way
suppression sync.

  - Inbound: Instantly-reported "unsubscribed" cascades into email_opt_outs
    via suppress_contact(), not just the legacy DBPRContact.is_opted_out flag.
  - Outbound: rows in email_opt_outs created since the campaign's
    last_synced_at get pushed to Instantly's block list.
"""
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from src.core.database import get_db_context
from src.core.models import CampaignContact, DBPRContact, EmailCampaign, EmailOptOut

LICENSE_PREFIX = "STGSYNC-"
UNSUB_EMAIL = "stgsync-unsub@example.com"
PUSH_EMAIL = "stgsync-push@example.com"
CAMPAIGN_NAME = "[STAGING TEST] Suppression Sync"

WORK_EMAIL_LICENSE = f"{LICENSE_PREFIX}WORK-1"
WORK_EMAIL = "stgsync-work@example.com"


def _pg_url():
    try:
        from config.settings import get_settings
        url = get_settings().database_url
        return str(url) if url else None
    except Exception:
        return None


pytestmark = pytest.mark.skipif(
    _pg_url() is None,
    reason="DATABASE_URL not set — requires real Postgres",
)


def _cleanup():
    with get_db_context() as db:
        camp_ids = [c.id for c in db.query(EmailCampaign).filter(EmailCampaign.name == CAMPAIGN_NAME).all()]
        if camp_ids:
            db.query(CampaignContact).filter(CampaignContact.campaign_id.in_(camp_ids)).delete(synchronize_session=False)
            db.query(EmailCampaign).filter(EmailCampaign.id.in_(camp_ids)).delete(synchronize_session=False)
        db.query(DBPRContact).filter(DBPRContact.license_number.like(f"{LICENSE_PREFIX}%")).delete(synchronize_session=False)
        db.query(EmailOptOut).filter(EmailOptOut.email.in_([UNSUB_EMAIL, PUSH_EMAIL, WORK_EMAIL])).delete(synchronize_session=False)
        db.commit()


@pytest.fixture(autouse=True)
def cleanup_around():
    _cleanup()
    yield
    _cleanup()


def test_inbound_unsubscribe_cascades_to_email_opt_outs():
    from src.tasks.email_campaign_sync import _sync_lead_statuses

    with get_db_context() as db:
        contact = DBPRContact(
            license_number=f"{LICENSE_PREFIX}1",
            license_type_code="RC",
            full_name="Unsub, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email=UNSUB_EMAIL,
        )
        db.add(contact)
        db.flush()
        campaign = EmailCampaign(name=CAMPAIGN_NAME, instantly_campaign_id="inst_camp_stgsync", status="active")
        db.add(campaign)
        db.flush()
        db.add(CampaignContact(campaign_id=campaign.id, dbpr_contact_id=contact.id))
        db.commit()
        campaign_id = campaign.id

    fake_page = {
        "leads": [{"id": "lead-1", "email": UNSUB_EMAIL, "status": "unsubscribed"}],
        "next_starting_after": None,
    }
    with get_db_context() as db:
        campaign = db.get(EmailCampaign, campaign_id)
        with patch("src.services.instantly_service.list_leads", return_value=fake_page):
            _sync_lead_statuses(campaign, dry_run=False)

    with get_db_context() as db:
        opted_out = db.query(EmailOptOut).filter_by(email=UNSUB_EMAIL).first()
        contact = db.query(DBPRContact).filter_by(license_number=f"{LICENSE_PREFIX}1").first()

    assert opted_out is not None
    assert opted_out.source == "instantly_sync"
    assert contact.is_opted_out is True


def test_inbound_unsubscribe_matches_via_work_email_when_lead_id_absent():
    """When instantly_lead_id matching fails, the fallback lookup must also
    check DBPRContact.work_email (not just email) — DBPR sends can target
    either field — and must suppress the actual reported address."""
    from src.tasks.email_campaign_sync import _sync_lead_statuses

    with get_db_context() as db:
        contact = DBPRContact(
            license_number=WORK_EMAIL_LICENSE,
            license_type_code="RC",
            full_name="Work Email Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email=None,
            work_email=WORK_EMAIL,
        )
        db.add(contact)
        db.flush()
        campaign = EmailCampaign(name=CAMPAIGN_NAME, instantly_campaign_id="inst_camp_stgsync_work", status="active")
        db.add(campaign)
        db.flush()
        db.add(CampaignContact(campaign_id=campaign.id, dbpr_contact_id=contact.id))
        db.commit()
        campaign_id = campaign.id

    # No instantly_lead_id on the campaign_contact row, so this must go
    # through the email-fallback lookup — matched by work_email, not email.
    fake_page = {
        "leads": [{"id": None, "email": WORK_EMAIL, "status": "unsubscribed"}],
        "next_starting_after": None,
    }
    with get_db_context() as db:
        campaign = db.get(EmailCampaign, campaign_id)
        with patch("src.services.instantly_service.list_leads", return_value=fake_page):
            _sync_lead_statuses(campaign, dry_run=False)

    with get_db_context() as db:
        opted_out = db.query(EmailOptOut).filter_by(email=WORK_EMAIL).first()
        contact = db.query(DBPRContact).filter_by(license_number=WORK_EMAIL_LICENSE).first()

    assert opted_out is not None, "work_email must be suppressed, not silently missed"
    assert contact.is_opted_out is True


def test_outbound_push_sends_unpushed_opt_outs_and_marks_them():
    from src.tasks.email_campaign_sync import _sync_outbound_suppressions

    with get_db_context() as db:
        db.add(EmailOptOut(email=PUSH_EMAIL, source="unsubscribe_link"))
        db.commit()

    with patch("src.services.instantly_service.add_to_block_list", return_value=True) as mock_push:
        pushed = _sync_outbound_suppressions(dry_run=False)

    assert pushed == 1
    # Batched: one call carrying a list that includes our email.
    assert mock_push.call_count == 1
    sent_list = mock_push.call_args[0][0]
    assert PUSH_EMAIL in sent_list

    # Durable watermark: the row is now flagged pushed and won't be re-pushed.
    with get_db_context() as db:
        row = db.query(EmailOptOut).filter_by(email=PUSH_EMAIL).first()
        assert row.pushed_to_instantly is True

    with patch("src.services.instantly_service.add_to_block_list", return_value=True) as mock_push2:
        pushed_again = _sync_outbound_suppressions(dry_run=False)
    assert PUSH_EMAIL not in [e for c in mock_push2.call_args_list for e in (c[0][0] if c[0] else [])]


def test_sync_lead_statuses_survives_null_email_lead():
    """Instantly can return a lead whose email key is present but null;
    (lead.get('email') or '') must not raise AttributeError."""
    from src.tasks.email_campaign_sync import _sync_lead_statuses

    with get_db_context() as db:
        campaign = EmailCampaign(name=CAMPAIGN_NAME, instantly_campaign_id="inst_camp_stgsync", status="active")
        db.add(campaign)
        db.commit()
        campaign_id = campaign.id

    fake_page = {
        "leads": [{"id": "lead-null", "email": None, "status": "active"}],
        "next_starting_after": None,
    }
    with get_db_context() as db:
        campaign = db.get(EmailCampaign, campaign_id)
        with patch("src.services.instantly_service.list_leads", return_value=fake_page):
            result = _sync_lead_statuses(campaign, dry_run=False)  # must not raise

    # No campaign-contact matches the null-email lead, so synced is 0 — the
    # point is that the null email did not raise AttributeError.
    assert result["synced"] == 0


def test_outbound_push_leaves_row_unpushed_when_instantly_fails():
    from src.tasks.email_campaign_sync import _sync_outbound_suppressions

    with get_db_context() as db:
        db.add(EmailOptOut(email=PUSH_EMAIL, source="unsubscribe_link"))
        db.commit()

    with patch("src.services.instantly_service.add_to_block_list", return_value=False):
        pushed = _sync_outbound_suppressions(dry_run=False)

    assert pushed == 0
    # Failed push must NOT advance the watermark — it retries next run.
    with get_db_context() as db:
        row = db.query(EmailOptOut).filter_by(email=PUSH_EMAIL).first()
        assert row.pushed_to_instantly is False
