"""
Real-Postgres integration test: BatchData enrichment writes mobile vs.
landline numbers into their own columns instead of collapsing both into
`phone`.

WHY REAL POSTGRES: _run_batchdata_stage opens its own get_db_context() and
queries/writes via the ORM directly — a mocked session can't exercise the
actual persist path. Runs only when DATABASE_URL points at Postgres.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from src.core.database import get_db_context
from src.core.models import DBPRContact

LICENSE_PREFIX = "STGPHONE-"
MOBILE_NUMBER = "+18135551212"
LANDLINE_NUMBER = "+18135553434"


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
        db.commit()


@pytest.fixture(autouse=True)
def cleanup_around():
    _cleanup()
    yield
    _cleanup()


def _batchdata_result(number: str, line_type: str) -> dict:
    return {
        "phoneNumbers": [{"number": number, "type": line_type, "score": 100}],
        "emails": [],
    }


def test_mobile_and_landline_write_to_separate_columns():
    now = datetime.now(timezone.utc)
    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}mobile",
            license_type_code="CVC",
            full_name="Mobile, Contractor",
            county_id="stgphone_test",
            vertical="solar",
            enrichment_status="pending",
            created_at=now,
        ))
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}landline",
            license_type_code="CVC",
            full_name="Landline, Contractor",
            county_id="stgphone_test",
            vertical="solar",
            enrichment_status="pending",
            created_at=now,
        ))
        db.commit()

    fake_results = [
        _batchdata_result(MOBILE_NUMBER, "mobile"),
        _batchdata_result(LANDLINE_NUMBER, "landline"),
    ]

    with patch("src.services.skip_trace._call_batch_data", return_value=fake_results):
        from src.tasks.dbpr_enrichment import _run_batchdata_stage
        _run_batchdata_stage(
            county_id="stgphone_test",
            limit=100,
            dry_run=False,
            api_key="fake-key",
        )

    with get_db_context() as db:
        mobile_contact = db.query(DBPRContact).filter(
            DBPRContact.license_number == f"{LICENSE_PREFIX}mobile"
        ).one()
        landline_contact = db.query(DBPRContact).filter(
            DBPRContact.license_number == f"{LICENSE_PREFIX}landline"
        ).one()

    assert mobile_contact.mobile_phone == MOBILE_NUMBER
    assert mobile_contact.landline_phone is None
    assert mobile_contact.phone == MOBILE_NUMBER

    assert landline_contact.landline_phone == LANDLINE_NUMBER
    assert landline_contact.mobile_phone is None
    assert landline_contact.phone == LANDLINE_NUMBER
