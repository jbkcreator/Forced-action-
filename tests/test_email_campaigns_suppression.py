"""
Real-Postgres integration test: campaign eligibility excludes contacts in
email_opt_outs (shared by count_eligible and the Instantly enqueue path,
both built on _eligibility_filters).
"""
import pytest
from datetime import datetime, timezone

from src.core.database import get_db_context
from src.core.models import DBPRContact, EmailOptOut

LICENSE_PREFIX = "STGSUPP2-"
SUPPRESSED_EMAIL = "stgsupp2-suppressed@example.com"
CLEAN_EMAIL = "stgsupp2-clean@example.com"
TEST_VERTICAL = "staging_test_vertical_suppression"


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
        db.query(DBPRContact).filter(
            DBPRContact.license_number.like(f"{LICENSE_PREFIX}%")
        ).delete(synchronize_session=False)
        db.query(EmailOptOut).filter(
            EmailOptOut.email.in_([SUPPRESSED_EMAIL, CLEAN_EMAIL])
        ).delete(synchronize_session=False)
        db.commit()


@pytest.fixture(autouse=True)
def cleanup_around():
    _cleanup()
    yield
    _cleanup()


def test_count_eligible_excludes_suppressed_email():
    from src.services.email_campaigns import count_eligible

    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}1",
            license_type_code="RC",
            full_name="Suppressed, Contractor",
            county_id="hillsborough",
            vertical=TEST_VERTICAL,
            enrichment_status="enriched",
            email=SUPPRESSED_EMAIL,
            email_verified=True,
        ))
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}2",
            license_type_code="RC",
            full_name="Clean, Contractor",
            county_id="hillsborough",
            vertical=TEST_VERTICAL,
            enrichment_status="enriched",
            email=CLEAN_EMAIL,
            email_verified=True,
        ))
        db.add(EmailOptOut(email=SUPPRESSED_EMAIL, source="manual"))
        db.commit()

    from unittest.mock import patch
    with patch("src.services.email_campaigns.list_counties",
               return_value=[{"county_id": "hillsborough", "status": "launched"}]):
        n = count_eligible(county_id="hillsborough", zips=[], vertical=TEST_VERTICAL)

    assert n == 1


def test_count_eligible_excludes_when_work_email_suppressed():
    # Instantly sends to `email or work_email`; a suppressed work_email must
    # exclude the contact even when its primary email is clean.
    from src.services.email_campaigns import count_eligible

    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}3",
            license_type_code="RC",
            full_name="WorkEmail, Contractor",
            county_id="hillsborough",
            vertical=TEST_VERTICAL,
            enrichment_status="enriched",
            email=CLEAN_EMAIL,
            work_email=SUPPRESSED_EMAIL,
            email_verified=True,
        ))
        db.add(EmailOptOut(email=SUPPRESSED_EMAIL, source="manual"))
        db.commit()

    from unittest.mock import patch
    with patch("src.services.email_campaigns.list_counties",
               return_value=[{"county_id": "hillsborough", "status": "launched"}]):
        n = count_eligible(county_id="hillsborough", zips=[], vertical=TEST_VERTICAL)

    assert n == 0
