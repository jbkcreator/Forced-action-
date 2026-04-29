"""
Proof Moment service tests — Item 24.

Unit tests: mock DB session.
Integration: fresh_db fixture (requires Postgres).

Run:
    pytest tests/test_proof_moment.py -v
    pytest tests/test_proof_moment.py -v -k "unit"
"""
from unittest.mock import MagicMock, patch

import pytest

from src.services.proof_moment import _blur_address, get_proof_leads


# ============================================================================
# Unit tests — _blur_address
# ============================================================================


class TestBlurAddressUnit:
    def test_keeps_house_number(self):
        result = _blur_address("1234 Oak Street Lane")
        assert result.startswith("1234")

    def test_masks_street_name(self):
        result = _blur_address("1234 Oak Street")
        parts = result.split()
        assert parts[0] == "1234"
        assert all(c == "*" for c in parts[1])
        assert all(c == "*" for c in parts[2])

    def test_min_three_asterisks(self):
        result = _blur_address("1 Ln")
        parts = result.split()
        assert len(parts[1]) >= 3

    def test_mask_length_matches_word(self):
        result = _blur_address("999 Mississippi")
        parts = result.split()
        assert len(parts[1]) == len("Mississippi")

    def test_none_returns_placeholder(self):
        assert _blur_address(None) == "*** ***"

    def test_empty_string_returns_placeholder(self):
        assert _blur_address("") == "*** ***"

    def test_single_word_no_crash(self):
        result = _blur_address("1234")
        assert result == "1234"

    def test_multi_suffix(self):
        result = _blur_address("5 Main St NW")
        parts = result.split()
        assert parts[0] == "5"
        assert len(parts) == 4
        assert all(c == "*" for c in parts[1])


# ============================================================================
# Unit tests — get_proof_leads
# ============================================================================


class TestGetProofLeadsUnit:
    def _make_db(self, rows=None):
        db = MagicMock()
        row_list = rows or []
        db.execute.return_value.all.return_value = row_list
        db.execute.return_value.scalar_one_or_none.return_value = None
        return db

    def test_returns_structure_when_no_leads(self):
        db = self._make_db(rows=[])
        result = get_proof_leads("roofing", "hillsborough", db)
        assert result["revealed"] is None
        assert result["blurred"] == []
        assert result["county_id"] == "hillsborough"
        assert result["vertical"] == "roofing"

    def test_single_lead_goes_to_revealed(self):
        prop = MagicMock()
        prop.id = 1
        prop.address = "100 Elm Street"
        prop.city = "Tampa"
        prop.state = "FL"
        prop.zip = "33601"

        score = MagicMock()
        score.final_cds_score = 85.0
        score.lead_tier = "Platinum"
        score.urgency_level = "high"
        score.vertical_scores = {"roofing": 90}
        score.distress_types = {"foreclosure": 1}

        db = self._make_db(rows=[(prop, score)])
        result = get_proof_leads("roofing", "hillsborough", db)

        assert result["revealed"] is not None
        assert result["blurred"] == []
        assert result["revealed"]["address"] == "100 Elm Street"
        assert result["revealed"]["score"] == 85.0

    def test_first_lead_revealed_rest_blurred(self):
        def _row(pid, address):
            prop = MagicMock()
            prop.id = pid
            prop.address = address
            prop.city = "Tampa"
            prop.state = "FL"
            prop.zip = "33601"
            score = MagicMock()
            score.final_cds_score = 75.0
            score.lead_tier = "Gold"
            score.urgency_level = "medium"
            score.vertical_scores = {}
            score.distress_types = {}
            return (prop, score)

        rows = [_row(1, "100 Elm St"), _row(2, "200 Oak Ave"), _row(3, "300 Pine Rd")]
        db = self._make_db(rows=rows)
        result = get_proof_leads("roofing", "hillsborough", db)

        assert result["revealed"]["address"] == "100 Elm St"
        assert len(result["blurred"]) == 2

    def test_blurred_address_is_masked(self):
        def _row(pid, address):
            prop = MagicMock()
            prop.id = pid
            prop.address = address
            prop.city = "Tampa"
            prop.state = "FL"
            prop.zip = "33601"
            score = MagicMock()
            score.final_cds_score = 70.0
            score.lead_tier = "Gold"
            score.urgency_level = "low"
            score.vertical_scores = {}
            score.distress_types = {}
            return (prop, score)

        rows = [_row(1, "100 Main St"), _row(2, "200 Maple Ave")]
        db = self._make_db(rows=rows)
        result = get_proof_leads("roofing", "hillsborough", db)

        blurred_addr = result["blurred"][0]["address"]
        assert "*" in blurred_addr

    def test_blurred_lead_contact_is_none(self):
        def _row(pid, address):
            prop = MagicMock()
            prop.id = pid
            prop.address = address
            prop.city = "Tampa"
            prop.state = "FL"
            prop.zip = "33601"
            score = MagicMock()
            score.final_cds_score = 70.0
            score.lead_tier = "Gold"
            score.urgency_level = "low"
            score.vertical_scores = {}
            score.distress_types = {}
            return (prop, score)

        rows = [_row(1, "100 Main St"), _row(2, "200 Oak Ave")]
        db = self._make_db(rows=rows)
        result = get_proof_leads("investor", "hillsborough", db)
        assert result["blurred"][0]["contact"] is None

    def test_revealed_contact_populated_when_enriched_exists(self):
        prop = MagicMock()
        prop.id = 1
        prop.address = "100 Elm St"
        prop.city = "Tampa"
        prop.state = "FL"
        prop.zip = "33601"
        score = MagicMock()
        score.final_cds_score = 90.0
        score.lead_tier = "Ultra Platinum"
        score.urgency_level = "high"
        score.vertical_scores = {}
        score.distress_types = {}

        enriched = MagicMock()
        enriched.mobile_phone = "+18135550100"
        enriched.email = "owner@example.com"
        enriched.mailing_address = "123 PO Box"

        db = MagicMock()
        db.execute.return_value.all.return_value = [(prop, score)]
        db.execute.return_value.scalar_one_or_none.return_value = enriched

        result = get_proof_leads("roofing", "hillsborough", db)
        assert result["revealed"]["contact"]["mobile_phone"] == "+18135550100"

    def test_unknown_vertical_falls_back_gracefully(self):
        db = self._make_db(rows=[])
        result = get_proof_leads("nonexistent_vertical", "hillsborough", db)
        assert result["revealed"] is None


class TestGetProofLeadsUnlockAware:
    """When feed_uuid is provided and matches a SentLead row, the previously
    blurred lead is upgraded to unlocked=true with contact populated."""

    def _row(self, pid, address="100 Elm St"):
        prop = MagicMock()
        prop.id = pid
        prop.address = address
        prop.city = "Tampa"
        prop.state = "FL"
        prop.zip = "33601"
        score = MagicMock()
        score.final_cds_score = 75.0
        score.lead_tier = "Gold"
        score.urgency_level = "medium"
        score.vertical_scores = {}
        score.distress_types = {}
        return (prop, score)

    def test_paid_unlock_blurred_lead_becomes_unlocked(self):
        rows = [self._row(1, "100 Elm St"), self._row(2, "200 Oak Ave"), self._row(3, "300 Pine Rd")]
        subscriber = MagicMock(id=42)
        owner = MagicMock(
            owner_name="Jane Doe", phone_1="+18135550199", phone_2=None, phone_3=None,
            email_1="jane@example.com", email_2=None, mailing_address="123 PO Box",
        )

        # Drive the multiple db.execute calls in order
        call_count = {"i": 0}

        def execute_router(stmt, *a, **k):
            result = MagicMock()
            i = call_count["i"]
            if i == 0:
                # Top-leads query
                result.all.return_value = rows
            elif i == 1:
                # Subscriber lookup by feed_uuid
                result.scalar_one_or_none.return_value = subscriber
            elif i == 2:
                # SentLead property_id list — say property #2 was paid-unlocked
                result.scalars.return_value.all.return_value = [2]
            else:
                # Per-row Owner / EnrichedContact lookups — return owner for each prop
                result.scalar_one_or_none.return_value = owner
            call_count["i"] += 1
            return result

        db = MagicMock()
        db.execute.side_effect = execute_router

        result = get_proof_leads("roofing", "hillsborough", db, feed_uuid="abc")

        # First lead is the FREE preview — unlocked stays False
        assert result["revealed"] is not None
        assert result["revealed"]["unlocked"] is False
        # Property #2 (blurred slot) should now be unlocked + contact populated
        unlocked_lead = next((l for l in result["blurred"] if l["property_id"] == 2), None)
        assert unlocked_lead is not None
        assert unlocked_lead["unlocked"] is True
        assert unlocked_lead["contact"] is not None
        # Property #3 stays blurred (no SentLead row)
        still_blurred = next((l for l in result["blurred"] if l["property_id"] == 3), None)
        assert still_blurred is not None
        assert still_blurred["unlocked"] is False
        assert still_blurred["contact"] is None

    def test_no_feed_uuid_keeps_legacy_behavior(self):
        rows = [self._row(1, "100 Elm St"), self._row(2, "200 Oak Ave")]
        db = MagicMock()
        db.execute.return_value.all.return_value = rows
        db.execute.return_value.scalar_one_or_none.return_value = None

        result = get_proof_leads("roofing", "hillsborough", db)
        assert result["revealed"]["unlocked"] is False
        for blurred in result["blurred"]:
            assert blurred["unlocked"] is False
            assert blurred["contact"] is None
