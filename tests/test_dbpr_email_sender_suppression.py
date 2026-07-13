"""
Real-Postgres integration test: dbpr_email_sender candidate query excludes
contacts present in email_opt_outs.

WHY REAL POSTGRES: run_dbpr_email_sender() opens its own get_db_context()
and queries via the ORM directly against the DB — a mocked session can't
exercise the actual filter. Runs only when DATABASE_URL points at Postgres.
Every row this test creates carries the STGSUPP- marker and is deleted in
teardown regardless of pass/fail.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from src.core.database import get_db_context
from src.core.models import DBPRContact, EmailOptOut

LICENSE_PREFIX = "STGSUPP-"
SUPPRESSED_EMAIL = "stgsupp-suppressed@example.com"
CLEAN_EMAIL = "stgsupp-clean@example.com"


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


def test_candidate_query_excludes_suppressed_email():
    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}1",
            license_type_code="RC",
            full_name="Suppressed, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email=SUPPRESSED_EMAIL,
            email_status="not_sent",
            created_at=now,
        ))
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}2",
            license_type_code="RC",
            full_name="Clean, Contractor",
            county_id="hillsborough",
            vertical="roofing",
            enrichment_status="enriched",
            email=CLEAN_EMAIL,
            email_status="not_sent",
            created_at=now,
        ))
        db.add(EmailOptOut(email=SUPPRESSED_EMAIL, source="manual"))
        db.commit()

    with patch("src.tasks.dbpr_email_sender.send_email", return_value=True):
        from src.tasks.dbpr_email_sender import run_dbpr_email_sender
        stats = run_dbpr_email_sender(county_id="hillsborough", limit=1000, delay=0)

    with get_db_context() as db:
        sent_emails = {
            c.email for c in db.query(DBPRContact).filter(
                DBPRContact.license_number.like(f"{LICENSE_PREFIX}%"),
                DBPRContact.email_status == "sent",
            ).all()
        }

    assert CLEAN_EMAIL in sent_emails
    assert SUPPRESSED_EMAIL not in sent_emails
    assert stats["total"] == 1
