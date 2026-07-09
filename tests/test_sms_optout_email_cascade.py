"""
Real-Postgres integration test: an SMS opt-out (STOP / IVR) cascades to email
suppression for the same contact (ADR 0028 — block every channel).
record_opt_out is the shared entry point for both STOP and IVR opt-outs.
"""
import pytest

from src.core.database import get_db_context
from src.core.models import DBPRContact, EmailOptOut, SmsOptOut

LICENSE = "STGCASCADE-1"
PHONE = "+18135550142"
EMAIL = "stgcascade@example.com"


def _pg_url():
    try:
        from config.settings import get_settings
        return str(get_settings().database_url) if get_settings().database_url else None
    except Exception:
        return None


pytestmark = pytest.mark.skipif(_pg_url() is None, reason="requires real Postgres")


def _cleanup():
    with get_db_context() as db:
        db.query(DBPRContact).filter(DBPRContact.license_number == LICENSE).delete(synchronize_session=False)
        db.query(EmailOptOut).filter(EmailOptOut.email == EMAIL).delete(synchronize_session=False)
        db.query(SmsOptOut).filter(SmsOptOut.phone == PHONE).delete(synchronize_session=False)
        db.commit()


@pytest.fixture(autouse=True)
def cleanup_around():
    _cleanup()
    yield
    _cleanup()


def test_sms_stop_cascades_to_email_via_dbpr_contact():
    from src.services.sms_compliance import record_opt_out

    # A prospect (no Subscriber row) with both phone and email on the DBPR record.
    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=LICENSE,
            license_type_code="RC",
            full_name="Cascade, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email=EMAIL,
            phone=PHONE,
        ))
        db.commit()

    with get_db_context() as db:
        record_opt_out(PHONE, keyword="STOP", source="inbound_sms", db=db)
        db.commit()

    with get_db_context() as db:
        sms_row = db.query(SmsOptOut).filter_by(phone=PHONE).first()
        email_row = db.query(EmailOptOut).filter_by(email=EMAIL).first()

    assert sms_row is not None, "phone should be suppressed"
    assert email_row is not None, "email must be cascaded-suppressed from the SMS opt-out"
    assert email_row.source == "cascaded_from_sms"
