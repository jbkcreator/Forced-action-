"""
Real-Postgres integration tests for src.tasks.dbpr_tracerfy_enrichment.

WHY REAL POSTGRES: the module opens its own get_db_context() sessions and
does raw text() reads/writes directly — a mocked session can't exercise the
actual persist path (mirrors tests/test_dbpr_enrichment_phone_split.py).

Mocks only the Tracerfy HTTP boundary (_submit_trace_batch/_poll_trace_queue),
never the DB.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from sqlalchemy import text

from src.core.database import get_db_context
from src.core.models import DBPRContact

LICENSE_PREFIX = "STGTRACERFY-"


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


def _make_contact(suffix: str, vertical: str, status: str = "pending",
                   queue_id=None, county_id: str = "stgtracerfy_test") -> None:
    with get_db_context() as db:
        db.add(DBPRContact(
            license_number=f"{LICENSE_PREFIX}{suffix}",
            license_type_code="CCC",
            full_name=f"{suffix.upper()}, CONTRACTOR",
            address="123 Test St",
            city="Tampa",
            state="FL",
            zip_code="33601",
            county_id=county_id,
            vertical=vertical,
            enrichment_status=status,
            tracerfy_queue_id=queue_id,
            created_at=datetime.now(timezone.utc),
        ))
        db.commit()


def _get_contact(suffix: str) -> dict:
    with get_db_context() as db:
        row = db.execute(text("""
            SELECT * FROM dbpr_contacts WHERE license_number = :ln
        """), {"ln": f"{LICENSE_PREFIX}{suffix}"}).mappings().one()
        return dict(row)


def _trace_row(address="123 Test St", zip_="33601", mobile=None, landline=None, email=None):
    row = {"address": address, "zip": zip_, "city": "Tampa", "state": "FL"}
    if mobile:
        row["primary_phone"] = mobile
        row["primary_phone_type"] = "Mobile"
    if landline and not mobile:
        row["primary_phone"] = landline
        row["primary_phone_type"] = "Landline"
    if email:
        row["email_1"] = email
    return row


MODULE = "src.tasks.dbpr_tracerfy_enrichment"


def test_vertical_filter_excludes_non_matching_rows():
    _make_contact("roofing1", "roofing")
    _make_contact("general1", "general")

    with patch(f"{MODULE}._submit_trace_batch") as mock_submit:
        mock_submit.return_value = ("Q1", 0)
        with patch(f"{MODULE}._poll_trace_queue", return_value=[]):
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(verticals=["roofing"], county_id="stgtracerfy_test")

    # Only the roofing contact's address should have been submitted.
    submitted_records = mock_submit.call_args[0][0]
    submitted_labels = {r["label"] for r in submitted_records}
    roofing_contact = _get_contact("roofing1")
    general_contact = _get_contact("general1")

    assert str(roofing_contact["id"]) in submitted_labels
    assert str(general_contact["id"]) not in submitted_labels
    assert general_contact["enrichment_status"] == "pending"  # never touched


def test_submission_commits_before_poll_and_stores_queue_id():
    _make_contact("resume1", "roofing")

    with patch(f"{MODULE}._submit_trace_batch", return_value=("Q42", 0)):
        # Poll raises — simulates a crash between submit and poll completing.
        with patch(f"{MODULE}._poll_trace_queue", side_effect=RuntimeError("network died")):
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(verticals=["roofing"], county_id="stgtracerfy_test")

    contact = _get_contact("resume1")
    assert contact["enrichment_status"] == "tracerfy_submitted"
    assert contact["tracerfy_queue_id"] == "Q42"


def test_resume_polls_stored_queue_and_never_resubmits():
    _make_contact("resume2", "roofing", status="tracerfy_submitted", queue_id="Q99")
    contact = _get_contact("resume2")

    hit_row = _trace_row(mobile="+18135551234")

    with patch(f"{MODULE}._submit_trace_batch") as mock_submit:
        with patch(f"{MODULE}._poll_trace_queue", return_value=[hit_row]) as mock_poll:
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(verticals=["roofing"], county_id="stgtracerfy_test")

    mock_submit.assert_not_called()  # the core idempotency guarantee
    assert mock_poll.call_args[0][0] == "Q99"

    result = _get_contact("resume2")
    assert result["enrichment_status"] == "enriched"
    assert result["mobile_phone"] == "+18135551234"
    assert result["tracerfy_queue_id"] is None


def test_hit_writes_mobile_and_flips_enriched():
    _make_contact("hit1", "solar")
    contact = _get_contact("hit1")
    hit_row = _trace_row(mobile="+18135559999", email="test@example.com")

    with patch(f"{MODULE}._submit_trace_batch", return_value=("Q1", 0)):
        with patch(f"{MODULE}._poll_trace_queue", return_value=[hit_row]):
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(verticals=["solar"], county_id="stgtracerfy_test")

    result = _get_contact("hit1")
    assert result["enrichment_status"] == "enriched"
    assert result["mobile_phone"] == "+18135559999"
    assert result["phone"] == "+18135559999"
    assert result["email"] == "test@example.com"


def test_normal_miss_flips_to_awaiting_address_only_not_terminal_failed():
    """A normal-mode (name+address) miss is eligible for retry — it must
    NOT land on terminal 'failed' directly, since the address-only fallback
    (if ever enabled) needs a distinct pending-retry state to select from."""
    _make_contact("miss1", "roofing")

    with patch(f"{MODULE}._submit_trace_batch", return_value=("Q1", 0)):
        with patch(f"{MODULE}._poll_trace_queue", return_value=[]):  # no result row at all -> unmatched -> miss
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(verticals=["roofing"], county_id="stgtracerfy_test")

    result = _get_contact("miss1")
    assert result["enrichment_status"] == "awaiting_address_only"
    assert result["mobile_phone"] is None
    assert result["tracerfy_queue_id"] is None
    assert result["tracerfy_mode"] is None


def test_address_only_fallback_disabled_by_default_leaves_row_awaiting():
    """With the flag off (default), a normal miss stops at
    awaiting_address_only in the same run — no advanced-mode submission
    happens automatically."""
    _make_contact("noflag1", "roofing")

    with patch(f"{MODULE}._submit_trace_batch", return_value=("Q1", 0)) as mock_submit:
        with patch(f"{MODULE}._poll_trace_queue", return_value=[]):
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(verticals=["roofing"], county_id="stgtracerfy_test")

    assert mock_submit.call_count == 1  # only the normal-mode submission — no address-only follow-up
    result = _get_contact("noflag1")
    assert result["enrichment_status"] == "awaiting_address_only"


def test_address_only_fallback_retries_awaiting_rows_advanced_mode():
    """With the flag on, a row already sitting in awaiting_address_only
    (from a prior normal-mode miss) gets submitted address_only=True, and a
    hit there still writes phone fields and flips to enriched."""
    _make_contact("fallback1", "roofing", status="awaiting_address_only")

    hit_row = _trace_row(mobile="+18135550001")

    with patch(f"{MODULE}._submit_trace_batch", return_value=("Q2", 0)) as mock_submit:
        with patch(f"{MODULE}._poll_trace_queue", return_value=[hit_row]):
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(
                verticals=["roofing"], county_id="stgtracerfy_test",
                enable_address_only_fallback=True,
            )

    # address_only=True on the second (advanced) call.
    assert mock_submit.call_args_list[-1].kwargs.get("address_only") is True
    result = _get_contact("fallback1")
    assert result["enrichment_status"] == "enriched"
    assert result["mobile_phone"] == "+18135550001"


def test_advanced_miss_is_terminal_failed():
    """A miss on the address-only (advanced) retry has exhausted both trace
    types — it must land on terminal 'failed', not loop back to
    awaiting_address_only again."""
    _make_contact("terminal1", "roofing", status="awaiting_address_only")

    with patch(f"{MODULE}._submit_trace_batch", return_value=("Q3", 0)):
        with patch(f"{MODULE}._poll_trace_queue", return_value=[]):
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(
                verticals=["roofing"], county_id="stgtracerfy_test",
                enable_address_only_fallback=True,
            )

    result = _get_contact("terminal1")
    assert result["enrichment_status"] == "failed"


def test_resume_advanced_mode_submission_resolves_as_advanced():
    """A crashed advanced-mode submission (stored tracerfy_mode='advanced')
    must resolve a miss as terminal 'failed' on resume, not
    'awaiting_address_only' again — proves the resume path reads the stored
    mode rather than assuming normal."""
    _make_contact("resumeadv1", "roofing", status="tracerfy_submitted", queue_id="Q4")
    with get_db_context() as db:
        db.execute(text("UPDATE dbpr_contacts SET tracerfy_mode = 'advanced' WHERE license_number = :ln"),
                   {"ln": f"{LICENSE_PREFIX}resumeadv1"})
        db.commit()

    with patch(f"{MODULE}._submit_trace_batch") as mock_submit:
        with patch(f"{MODULE}._poll_trace_queue", return_value=[]):
            from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
            run_dbpr_tracerfy_enrichment(verticals=["roofing"], county_id="stgtracerfy_test")

    mock_submit.assert_not_called()  # idempotency guarantee still holds
    result = _get_contact("resumeadv1")
    assert result["enrichment_status"] == "failed"


def test_dry_run_makes_no_api_call_and_no_writes():
    _make_contact("dry1", "roofing")

    with patch(f"{MODULE}._submit_trace_batch") as mock_submit:
        from src.tasks.dbpr_tracerfy_enrichment import run_dbpr_tracerfy_enrichment
        run_dbpr_tracerfy_enrichment(verticals=["roofing"], county_id="stgtracerfy_test", dry_run=True)

    mock_submit.assert_not_called()
    result = _get_contact("dry1")
    assert result["enrichment_status"] == "pending"
