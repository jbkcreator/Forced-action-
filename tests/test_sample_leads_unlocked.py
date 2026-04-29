"""
Tests that /api/sample-leads returns unlocked=true with real owner contact
when the caller passes a feed_uuid whose subscriber has a SentLead row for
the property — i.e. after a $4 unlock. Without feed_uuid (anonymous landing
visit) the phone stays masked.
"""

from unittest.mock import MagicMock, patch

import pytest


def _make_lead_row(prop_id=10, address="123 Main St", phone="+18135550100"):
    prop = MagicMock()
    prop.id = prop_id
    prop.address = address
    prop.city = "Tampa"
    prop.zip = "33510"
    prop.year_built = 1990
    prop.sq_ft = 1500

    score = MagicMock()
    score.final_cds_score = 80.0
    score.lead_tier = "gold"
    score.vertical_scores = {"roofing": 75}
    score.distress_types = []

    owner = MagicMock()
    owner.phone_1 = phone
    owner.phone_2 = None
    owner.phone_3 = None
    owner.email_1 = "owner@example.com"
    owner.email_2 = None
    owner.owner_name = "Test Owner"

    return (prop, score, owner)


@patch("src.api.main.get_settings")
def test_anonymous_visit_returns_masked_phone(mock_get_settings):
    """No feed_uuid → phone is masked, unlocked=false."""
    from src.api.main import sample_leads

    mock_get_settings.return_value = MagicMock(debug=True)

    db = MagicMock()
    db.execute.return_value.all.return_value = [_make_lead_row()]
    db.execute.return_value.scalar_one_or_none.return_value = None  # no Incident
    db.execute.return_value.scalars.return_value.all.return_value = []

    result = sample_leads(
        zip_code="33510",
        vertical="roofing",
        county_id="hillsborough",
        feed_uuid=None,
        db=db,
    )

    assert len(result["leads"]) == 1
    assert result["leads"][0]["unlocked"] is False
    assert result["leads"][0]["phone"] == "•••-•••-••••"
    assert result["leads"][0]["owner_name"] is None
    assert result["leads"][0]["email"] is None


@patch("src.api.main.get_settings")
def test_unlocked_lead_returns_real_phone_and_email(mock_get_settings):
    """feed_uuid resolves to subscriber with SentLead → unlocked=true, real phone+email."""
    from src.api.main import sample_leads

    mock_get_settings.return_value = MagicMock(debug=True)

    subscriber = MagicMock(id=42)
    db = MagicMock()

    rows = [_make_lead_row(prop_id=10, phone="+18135550100")]

    # Three calls: subscriber lookup, leads query, unlocks query, then per-lead Incident lookup
    call_count = {"i": 0}

    def execute_router(stmt, *a, **k):
        result = MagicMock()
        # First call resolves the subscriber via feed_uuid
        if call_count["i"] == 0:
            result.scalar_one_or_none.return_value = subscriber
        # Second call returns the lead rows
        elif call_count["i"] == 1:
            result.all.return_value = rows
        # Third call returns the unlocked property IDs
        elif call_count["i"] == 2:
            result.scalars.return_value.all.return_value = [10]
        # Fourth call: per-lead Incident lookup
        else:
            result.scalar_one_or_none.return_value = None
        call_count["i"] += 1
        return result

    db.execute.side_effect = execute_router

    result = sample_leads(
        zip_code="33510",
        vertical="roofing",
        county_id="hillsborough",
        feed_uuid="abc-uuid",
        db=db,
    )

    assert len(result["leads"]) == 1
    lead = result["leads"][0]
    assert lead["unlocked"] is True
    assert lead["phone"] == "+18135550100"
    assert lead["email"] == "owner@example.com"
    assert lead["owner_name"] == "Test Owner"


@patch("src.api.main.get_settings")
def test_feed_uuid_with_no_unlock_stays_masked(mock_get_settings):
    """Subscriber exists but hasn't unlocked this lead → still masked."""
    from src.api.main import sample_leads

    mock_get_settings.return_value = MagicMock(debug=True)

    subscriber = MagicMock(id=42)
    db = MagicMock()
    rows = [_make_lead_row(prop_id=10)]

    call_count = {"i": 0}

    def execute_router(stmt, *a, **k):
        result = MagicMock()
        if call_count["i"] == 0:
            result.scalar_one_or_none.return_value = subscriber
        elif call_count["i"] == 1:
            result.all.return_value = rows
        elif call_count["i"] == 2:
            # No SentLead rows for this subscriber/property
            result.scalars.return_value.all.return_value = []
        else:
            result.scalar_one_or_none.return_value = None
        call_count["i"] += 1
        return result

    db.execute.side_effect = execute_router

    result = sample_leads(
        zip_code="33510",
        vertical="roofing",
        county_id="hillsborough",
        feed_uuid="abc-uuid",
        db=db,
    )

    assert result["leads"][0]["unlocked"] is False
    assert result["leads"][0]["phone"] == "•••-•••-••••"
