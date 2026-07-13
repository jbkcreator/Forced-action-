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

RAW_LICENSE = "STGCASCADE-RAW-1"
RAW_PHONE_STORED = "8135551000"      # unnormalized, as DBPR import data can be
RAW_PHONE_NORMALIZED = "+18135551000"
RAW_EMAIL = "stgcascade-raw@example.com"


def _pg_url():
    try:
        from config.settings import get_settings
        return str(get_settings().database_url) if get_settings().database_url else None
    except Exception:
        return None


pytestmark = pytest.mark.skipif(_pg_url() is None, reason="requires real Postgres")


def _cleanup():
    with get_db_context() as db:
        db.query(DBPRContact).filter(
            DBPRContact.license_number.in_([LICENSE, RAW_LICENSE])
        ).delete(synchronize_session=False)
        db.query(EmailOptOut).filter(EmailOptOut.email.in_([EMAIL, RAW_EMAIL])).delete(synchronize_session=False)
        db.query(SmsOptOut).filter(SmsOptOut.phone.in_([PHONE, RAW_PHONE_NORMALIZED])).delete(synchronize_session=False)
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


def test_repeat_stop_still_cascades_when_phone_already_suppressed():
    """A phone opted out before the cascade shipped (or a repeat STOP after
    deploy) must still reach email — record_opt_out's early return on an
    existing SmsOptOut row must not skip the cascade."""
    from src.services.sms_compliance import record_opt_out

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
        # Simulate a phone opt-out that predates the cross-channel cascade —
        # SmsOptOut row exists, no matching EmailOptOut yet.
        db.add(SmsOptOut(phone=PHONE, keyword_used="STOP", source="inbound_sms"))
        db.commit()

    with get_db_context() as db:
        assert db.query(EmailOptOut).filter_by(email=EMAIL).first() is None

    with get_db_context() as db:
        record_opt_out(PHONE, keyword="STOP", source="inbound_sms", db=db)
        db.commit()

    with get_db_context() as db:
        email_row = db.query(EmailOptOut).filter_by(email=EMAIL).first()
    assert email_row is not None, "cascade must run even when the SMS opt-out already existed"


def test_cascade_matches_dbpr_contact_with_unnormalized_stored_phone():
    """DBPR-imported phones aren't guaranteed to be normalized (e.g. raw
    '8135551000' instead of '+18135551000'). suppress_contact(phone=...) must
    still resolve the sibling email by comparing digit-only forms."""
    from src.services.email_suppression import suppress_contact

    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=RAW_LICENSE,
            license_type_code="RC",
            full_name="Raw Phone Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email=RAW_EMAIL,
            phone=RAW_PHONE_STORED,
        ))
        db.commit()

    with get_db_context() as db:
        suppress_contact(db, phone=RAW_PHONE_NORMALIZED, source="cascaded_from_sms")
        db.commit()

    with get_db_context() as db:
        email_row = db.query(EmailOptOut).filter_by(email=RAW_EMAIL).first()
    assert email_row is not None, "raw-format stored phone must still resolve to its sibling email"
