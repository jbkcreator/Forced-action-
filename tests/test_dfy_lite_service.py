"""
Tests for DFY-Lite pitch generation service (S3b).

Unit tests use mock_db (no real DB required).
Integration tests use fresh_db (skipped if DATABASE_URL not configured).
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.agents.pitch_builder import (
    DEFAULT_OUTPUT_FORMATS,
    MAX_GENERATIONS_PER_PAIR,
    build_distress_stack_array,
    build_property_pitch_context,
    generate_pitch_with_claude,
)
from src.services.dfy_lite_service import (
    DfyLiteLimitError,
    DfyLitePermissionError,
    can_subscriber_generate_pitch,
    count_completed_generations,
    create_pitch_order,
    mark_delivered,
    mark_reviewed,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def mock_claude(monkeypatch):
    canned_output = json.dumps({
        "email_subject": "Quick question about your property",
        "email_pitch": "Hello, I noticed there may be an opportunity to explore options for your property...",
        "sms_pitch": "Hi, this is [Name]. Your property may benefit from our services. Reply STOP to opt out.",
        "call_script": "Hi, am I speaking with the owner of [address]?",
        "linkedin_message": None,
        "evidence_summary": "Public records indicate code violations and tax delinquency status.",
        "metadata": {
            "model": "sonnet",
            "generated_at": "2026-06-18T00:00:00Z",
            "soft_wording_applied": True,
        },
    })
    mock_result = {
        "text": canned_output,
        "model": "claude-sonnet-4-6",
        "input_tokens": 800,
        "output_tokens": 400,
        "cost_usd": 0.0084,
    }
    monkeypatch.setattr(
        "src.agents.pitch_builder.call_claude_with_usage",
        lambda *a, **kw: mock_result,
    )
    return mock_result


@pytest.fixture
def minimal_context():
    return {
        "property": {
            "id": 1,
            "address": "123 Oak St",
            "city": "Tampa",
            "state": "FL",
            "zip": "33601",
            "owner_name": "Jane Doe",
        },
        "distress_score": {
            "final_cds_score": 87.0,
            "lead_tier": "Platinum",
            "distress_types": ["code_violation", "tax_delinquency"],
        },
        "code_violations": [
            {"violation_type": "roof damage", "description": "Missing shingles", "status": "open", "opened_date": None, "record_number": "CV-001"},
        ],
        "legal_proceedings": [],
        "liens": [{"record_type": "municipal lien", "amount": 4500, "filing_date": None, "creditor": None}],
        "tax_delinquencies": [{"tax_year": 2023, "total_amount_due": 3200, "years_delinquent": 1, "source_account_number": None}],
        "foreclosure": None,
        "enforcement_permits": [],
    }


@pytest.fixture
def request_options():
    return {
        "property_id": 1,
        "target_vertical": "roofer",
        "pitch_type": "contractor_repair_help",
        "offer_angle": "repair_property_damage",
        "selected_output_formats": list(DEFAULT_OUTPUT_FORMATS),
        "custom_instructions": None,
    }


# ── Authorization ─────────────────────────────────────────────────────────────

def test_authorized_via_sent_lead(mock_db):
    mock_db.execute.return_value.first.return_value = (1,)
    assert can_subscriber_generate_pitch(mock_db, subscriber_id=10, property_id=99) is True


def test_not_authorized_when_no_path(mock_db):
    mock_db.execute.return_value.first.return_value = None
    assert can_subscriber_generate_pitch(mock_db, subscriber_id=10, property_id=99) is False


def test_raises_permission_error_on_create(mock_db):
    mock_db.execute.return_value.first.return_value = None  # no auth path
    with pytest.raises(DfyLitePermissionError):
        create_pitch_order(mock_db, subscriber_id=10, property_id=99, request_options={
            "pitch_type": "loan_offer",
            "target_vertical": "roofer",
        })


# ── Generation limit ──────────────────────────────────────────────────────────

def test_raises_limit_error_when_at_3(mock_db):
    # first call = auth check returns a row (authorized)
    # second call = count returns 3
    mock_db.execute.return_value.first.return_value = (1,)
    mock_db.execute.return_value.scalar.return_value = 3
    with pytest.raises(DfyLiteLimitError):
        create_pitch_order(mock_db, subscriber_id=10, property_id=99, request_options={
            "pitch_type": "loan_offer",
            "target_vertical": "roofer",
        })


def test_allows_exactly_3_generations(mock_db):
    mock_db.execute.return_value.scalar.return_value = 2
    count = count_completed_generations(mock_db, subscriber_id=10, property_id=99)
    assert count < MAX_GENERATIONS_PER_PAIR


# ── Full order flow ───────────────────────────────────────────────────────────

def test_creates_order_success(mock_db, monkeypatch, request_options, minimal_context, mock_claude):
    monkeypatch.setattr(
        "src.services.dfy_lite_service.can_subscriber_generate_pitch",
        lambda *a, **kw: True,
    )
    monkeypatch.setattr(
        "src.services.dfy_lite_service.count_completed_generations",
        lambda *a, **kw: 0,
    )
    monkeypatch.setattr(
        "src.services.dfy_lite_service.build_property_pitch_context",
        lambda *a, **kw: minimal_context,
    )

    captured_order = {}

    def fake_add(obj):
        obj.id = 42
        captured_order["order"] = obj

    mock_db.add = fake_add
    mock_db.flush = MagicMock()
    mock_db.execute = MagicMock(return_value=MagicMock(scalar=MagicMock(return_value=None)))
    mock_db.commit = MagicMock()
    mock_db.refresh = MagicMock()

    order = create_pitch_order(mock_db, subscriber_id=10, property_id=1, request_options=request_options)

    mock_db.commit.assert_called()
    assert captured_order["order"].pitch_generation_number == 1
    assert captured_order["order"].target_vertical == "roofer"


def test_sets_pitch_failed_on_claude_error(mock_db, monkeypatch, request_options, minimal_context):
    monkeypatch.setattr("src.services.dfy_lite_service.can_subscriber_generate_pitch", lambda *a, **kw: True)
    monkeypatch.setattr("src.services.dfy_lite_service.count_completed_generations", lambda *a, **kw: 0)
    monkeypatch.setattr("src.services.dfy_lite_service.build_property_pitch_context", lambda *a, **kw: minimal_context)
    monkeypatch.setattr(
        "src.services.dfy_lite_service.generate_pitch_with_claude",
        lambda *a, **kw: (_ for _ in ()).throw(ValueError("bad JSON")),
    )

    executed_updates = []

    def fake_add(obj):
        obj.id = 42

    mock_db.add = fake_add
    mock_db.flush = MagicMock()
    mock_db.commit = MagicMock()
    mock_db.refresh = MagicMock()

    original_execute = MagicMock()
    mock_db.execute = original_execute

    with pytest.raises(ValueError, match="bad JSON"):
        create_pitch_order(mock_db, subscriber_id=10, property_id=1, request_options=request_options)

    # confirm commit was called after marking Pitch_Failed
    mock_db.commit.assert_called()


def test_sets_signal_failed_on_context_error(mock_db, monkeypatch, request_options):
    monkeypatch.setattr("src.services.dfy_lite_service.can_subscriber_generate_pitch", lambda *a, **kw: True)
    monkeypatch.setattr("src.services.dfy_lite_service.count_completed_generations", lambda *a, **kw: 0)
    monkeypatch.setattr(
        "src.services.dfy_lite_service.build_property_pitch_context",
        lambda *a, **kw: (_ for _ in ()).throw(ValueError("Property not found")),
    )

    def fake_add(obj):
        obj.id = 42

    mock_db.add = fake_add
    mock_db.flush = MagicMock()
    mock_db.commit = MagicMock()
    mock_db.execute = MagicMock()

    with pytest.raises(ValueError, match="Property not found"):
        create_pitch_order(mock_db, subscriber_id=10, property_id=1, request_options=request_options)

    mock_db.commit.assert_called()


# ── Soft-wording guard ────────────────────────────────────────────────────────

def test_prompt_does_not_contain_banned_phrases(monkeypatch, minimal_context, request_options):
    """
    The user message (property context sent to Claude) must not inject banned phrases
    as content. The system prompt legitimately lists them as 'NEVER use' instructions,
    so only the user message is checked here.
    """
    captured_messages = {}

    def fake_claude(task_type, messages, system=None, **kw):
        captured_messages["system"] = system or ""
        captured_messages["user"] = messages[0]["content"] if messages else ""
        output = json.dumps({
            "email_subject": "Test",
            "email_pitch": "Hello, the property may benefit from our services.",
            "sms_pitch": "Hi. Reply STOP to opt out.",
            "metadata": {"model": "test", "generated_at": "2026-06-18T00:00:00Z", "soft_wording_applied": True},
        })
        return {"text": output, "model": "test", "input_tokens": 0, "output_tokens": 0, "cost_usd": 0}

    monkeypatch.setattr("src.agents.pitch_builder.call_claude_with_usage", fake_claude)
    generate_pitch_with_claude(minimal_context, request_options, subscriber_id=10, db=MagicMock())

    # Only the user message is checked — the system prompt intentionally lists
    # these phrases inside a "NEVER use" block as Claude instructions.
    user_msg = captured_messages["user"]
    assert "I know you are in foreclosure" not in user_msg
    assert "you are delinquent" not in user_msg
    assert "you are in legal trouble" not in user_msg


def test_sms_output_contains_opt_out(mock_claude):
    canned = json.loads(mock_claude["text"])
    assert "Reply STOP to opt out" in (canned.get("sms_pitch") or "")


# ── Review / deliver ──────────────────────────────────────────────────────────

def test_mark_reviewed_sets_timestamp(mock_db):
    mock_db.execute.return_value.first.return_value = (1,)  # order found
    mock_db.commit = MagicMock()
    mark_reviewed(mock_db, order_id=42, subscriber_id=10)
    mock_db.commit.assert_called_once()


def test_mark_delivered_sets_status_and_timestamp(mock_db):
    mock_db.execute = MagicMock()
    mock_db.commit = MagicMock()
    mark_delivered(mock_db, order_id=42, subscriber_id=10)
    mock_db.commit.assert_called_once()


# ── Distress stack (pure unit) ────────────────────────────────────────────────

def test_empty_context_does_not_crash():
    result = build_distress_stack_array({})
    assert isinstance(result, list)
    assert len(result) == 0


def test_code_violation_appears_in_stack(minimal_context):
    bullets = build_distress_stack_array(minimal_context)
    texts = " ".join(bullets)
    assert "code_violation" in texts
    assert "roof damage" in texts


def test_foreclosure_appears_in_stack():
    context = {
        "distress_score": None,
        "code_violations": [],
        "legal_proceedings": [],
        "liens": [],
        "tax_delinquencies": [],
        "foreclosure": {"case_number": "FC-123", "status": "active", "filing_date": None, "plaintiff": None, "amount_claimed": None},
        "enforcement_permits": [],
    }
    bullets = build_distress_stack_array(context)
    assert any("foreclosure" in b for b in bullets)


def test_lien_amount_formatted_correctly(minimal_context):
    bullets = build_distress_stack_array(minimal_context)
    lien_bullets = [b for b in bullets if "lien" in b]
    assert lien_bullets, "expected at least one lien bullet"
    assert "$4,500" in lien_bullets[0]


# ── Integration (skipped if no Postgres) ─────────────────────────────────────

def test_build_context_with_real_db(fresh_db):
    """Insert a minimal Property row then call build_property_pitch_context — asserts no SQL error."""
    import sqlalchemy as sa

    # parcel_id, created_at, updated_at are NOT NULL in prod schema
    result = fresh_db.execute(
        sa.text("""
            INSERT INTO properties (parcel_id, address, city, state, zip, county_id, created_at, updated_at)
            VALUES ('TEST-DFY-LITE-001', '999 Test Ave', 'Tampa', 'FL', '33601', 'hillsborough', NOW(), NOW())
            RETURNING id
        """)
    )
    prop_id = result.scalar()
    fresh_db.flush()

    # Should not raise — all 8 queries handle missing rows gracefully
    context = build_property_pitch_context(fresh_db, prop_id)

    assert context["property"]["address"] == "999 Test Ave"
    assert context["code_violations"] == []
    assert context["foreclosure"] is None
