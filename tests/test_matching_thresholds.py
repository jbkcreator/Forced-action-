"""
Tests for tiered fuzzy match confidence scoring + per-county threshold overrides.

Covers:
  - config/matching.py: for_county(), COUNTY_OVERRIDES, default fallback
  - BaseLoader._thresholds property (county-aware)
  - BaseLoader._classify_match tier logic (matched / pending_review / unmatched)
  - LLM-verified override (always matched regardless of score)
  - Pinellas stopgap widens the pending_review band as designed
  - column_mapper SIGNAL_SCHEMAS["deeds"] includes "Legal"
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock

from config.matching import (
    THRESHOLDS,
    COUNTY_OVERRIDES,
    MatchingThresholds,
    for_county,
)
from src.loaders.base import BaseLoader
from src.loaders.column_mapper import SIGNAL_SCHEMAS


# ---------------------------------------------------------------------------
# Minimal concrete BaseLoader for testing — overrides the single abstractmethod
# ---------------------------------------------------------------------------

class _ConcreteLoader(BaseLoader):
    def load_from_dataframe(self, df, skip_duplicates: bool = True):
        return (0, 0, 0)


@pytest.fixture
def hills_loader():
    return _ConcreteLoader(session=MagicMock(), county_id="hillsborough")


@pytest.fixture
def pinellas_loader():
    return _ConcreteLoader(session=MagicMock(), county_id="pinellas")


@pytest.fixture
def unknown_loader():
    return _ConcreteLoader(session=MagicMock(), county_id="orange")


# ===========================================================================
# config/matching.py
# ===========================================================================

class TestForCounty:
    """for_county() returns the right MatchingThresholds for each county."""

    def test_default_thresholds_unchanged(self):
        """Sanity — default config must not regress."""
        assert THRESHOLDS.auto_match == 0.92
        assert THRESHOLDS.review_min == 0.75
        assert THRESHOLDS.address_floor == 75
        assert THRESHOLDS.owner_name_floor == 75
        assert THRESHOLDS.legal_desc_floor == 75

    def test_hillsborough_uses_defaults(self):
        """Hillsborough is not in COUNTY_OVERRIDES → falls back to THRESHOLDS."""
        assert for_county("hillsborough") is THRESHOLDS

    def test_pinellas_is_loosened(self):
        """Stopgap: Pinellas review_min and floors drop to 0.65 / 65."""
        t = for_county("pinellas")
        assert t.auto_match == 0.92      # auto_match stays strict
        assert t.review_min == 0.65
        assert t.address_floor == 60      # intentionally lower (per-method split 2026-05-22)
        assert t.owner_name_floor == 65
        assert t.legal_desc_floor == 65

    def test_unknown_county_falls_back_to_defaults(self):
        assert for_county("orange") is THRESHOLDS
        assert for_county("nonexistent_county") is THRESHOLDS

    def test_none_county_falls_back_to_defaults(self):
        """Loader constructed without county_id must not crash."""
        assert for_county(None) is THRESHOLDS

    def test_pinellas_in_overrides_dict(self):
        assert "pinellas" in COUNTY_OVERRIDES
        assert isinstance(COUNTY_OVERRIDES["pinellas"], MatchingThresholds)

    def test_thresholds_are_frozen(self):
        """Dataclass is frozen so accidental mutation raises."""
        with pytest.raises(Exception):
            THRESHOLDS.auto_match = 0.5  # type: ignore[misc]


# ===========================================================================
# BaseLoader._thresholds property
# ===========================================================================

class TestThresholdsProperty:

    def test_hills_loader_uses_default_thresholds(self, hills_loader):
        assert hills_loader._thresholds is THRESHOLDS

    def test_pinellas_loader_uses_loose_thresholds(self, pinellas_loader):
        t = pinellas_loader._thresholds
        assert t.review_min == 0.65
        assert t.address_floor == 60  # intentionally lower (per-method split 2026-05-22)

    def test_unknown_county_falls_back(self, unknown_loader):
        assert unknown_loader._thresholds is THRESHOLDS


# ===========================================================================
# BaseLoader._classify_match — tier classification logic
# ===========================================================================

class TestClassifyMatch:
    """Score → tier mapping for Hillsborough (strict) and Pinellas (loose)."""

    # ── Hillsborough (default) — review band is 75–92 ──────────────────────

    def test_hills_strong_score_is_matched(self, hills_loader):
        assert hills_loader._classify_match(95, "owner_name") == "matched"

    def test_hills_at_auto_match_boundary_is_matched(self, hills_loader):
        # 0.92 exactly should be matched (>=)
        assert hills_loader._classify_match(92, "owner_name") == "matched"

    def test_hills_just_below_auto_match_is_pending_review(self, hills_loader):
        assert hills_loader._classify_match(91, "owner_name") == "pending_review"

    def test_hills_at_review_min_boundary_is_pending_review(self, hills_loader):
        # 0.75 exactly should be pending_review (>=)
        assert hills_loader._classify_match(75, "owner_name") == "pending_review"

    def test_hills_below_review_min_is_unmatched(self, hills_loader):
        assert hills_loader._classify_match(74, "owner_name") == "unmatched"

    def test_hills_very_low_score_is_unmatched(self, hills_loader):
        assert hills_loader._classify_match(40, "address") == "unmatched"

    # ── Pinellas — review band widens to 65–92 ─────────────────────────────

    def test_pinellas_at_review_min_boundary_is_pending_review(self, pinellas_loader):
        assert pinellas_loader._classify_match(65, "owner_name") == "pending_review"

    def test_pinellas_just_below_loose_min_is_unmatched(self, pinellas_loader):
        assert pinellas_loader._classify_match(64, "owner_name") == "unmatched"

    def test_pinellas_score_70_is_pending_review_NOT_unmatched(self, pinellas_loader):
        """
        This is the core stopgap behavior: a Pinellas match scoring 70%
        used to be discarded; it should now land in pending_review.
        """
        assert pinellas_loader._classify_match(70, "owner_name") == "pending_review"

    def test_pinellas_same_70_score_unmatched_in_hills(self, hills_loader):
        """Cross-county sanity — 70% on Hillsborough stays unmatched."""
        assert hills_loader._classify_match(70, "owner_name") == "unmatched"

    def test_pinellas_owner_name_auto_match_lowered_to_75(self, pinellas_loader):
        """Stopgap (2026-06-11): owner-name auto-match lowered to 0.75 for Pinellas
        because LLM verification is disabled and owner-name is the only match path
        for probate/divorce. Address/legal-desc auto-match stays strict at 0.92."""
        # Owner-name methods auto-match at >= 75
        assert pinellas_loader._classify_match(75, "owner_name") == "matched"
        assert pinellas_loader._classify_match(91, "owner_name") == "matched"
        assert pinellas_loader._classify_match(74, "owner_name") == "pending_review"
        assert pinellas_loader._classify_match(75, "owner_name_zip") == "matched"
        assert pinellas_loader._classify_match(75, "owner_name_city") == "matched"
        # 65–75 owner-name band is still pending_review (Lifecycle triage)
        assert pinellas_loader._classify_match(70, "owner_name") == "pending_review"

    def test_pinellas_address_auto_match_unchanged(self, pinellas_loader):
        """Non-owner-name methods are NOT loosened — auto_match stays 0.92."""
        assert pinellas_loader._classify_match(91, "address") == "pending_review"
        assert pinellas_loader._classify_match(91, "legal_desc") == "pending_review"
        assert pinellas_loader._classify_match(92, "address") == "matched"

    # ── LLM-verified override ──────────────────────────────────────────────

    def test_llm_verified_always_matched_regardless_of_score(self, hills_loader):
        """LLM gate explicitly bypasses the score-based tier."""
        assert hills_loader._classify_match(10, "llm_verified") == "matched"
        assert hills_loader._classify_match(50, "llm_verified") == "matched"
        assert hills_loader._classify_match(0, "llm_verified") == "matched"

    def test_llm_verified_override_in_pinellas(self, pinellas_loader):
        assert pinellas_loader._classify_match(30, "llm_verified") == "matched"

    # ── Edge cases ─────────────────────────────────────────────────────────

    def test_none_match_method_uses_score_only(self, hills_loader):
        """No method passed — falls through to score-based logic."""
        assert hills_loader._classify_match(95, None) == "matched"
        assert hills_loader._classify_match(80, None) == "pending_review"
        assert hills_loader._classify_match(50, None) == "unmatched"

    def test_parcel_id_method_uses_score(self, hills_loader):
        """parcel_id method is not a verification override — uses score (will be 100)."""
        assert hills_loader._classify_match(100, "parcel_id") == "matched"


# ===========================================================================
# Boundary semantics: review_min vs auto_match are inclusive lower bounds
# ===========================================================================

class TestBoundarySemantics:

    def test_review_min_is_inclusive(self, hills_loader):
        """A score exactly at review_min*100 must be pending_review, not unmatched."""
        floor = int(THRESHOLDS.review_min * 100)
        assert hills_loader._classify_match(floor, "address") == "pending_review"
        assert hills_loader._classify_match(floor - 1, "address") == "unmatched"

    def test_auto_match_is_inclusive(self, hills_loader):
        floor = int(THRESHOLDS.auto_match * 100)
        assert hills_loader._classify_match(floor, "address") == "matched"
        assert hills_loader._classify_match(floor - 1, "address") == "pending_review"

    def test_pinellas_review_boundary_at_65(self, pinellas_loader):
        assert pinellas_loader._classify_match(65, "address") == "pending_review"
        assert pinellas_loader._classify_match(64, "address") == "unmatched"


# ===========================================================================
# column_mapper SIGNAL_SCHEMAS["deeds"] — Legal column added
# ===========================================================================

class TestDeedsSchemaLegal:
    """Audit fix: Pinellas deeds CSV has Legal in a non-canonical column."""

    def test_legal_is_in_deeds_schema(self):
        assert "Legal" in SIGNAL_SCHEMAS["deeds"]

    def test_existing_deeds_canonical_columns_preserved(self):
        """Adding Legal must not drop any pre-existing canonical column."""
        required = {"Grantor", "Grantee", "Instrument", "document_type",
                    "Book", "Page", "RecordDate", "sale_price"}
        assert required.issubset(set(SIGNAL_SCHEMAS["deeds"]))

    def test_no_duplicate_columns(self):
        cols = SIGNAL_SCHEMAS["deeds"]
        assert len(cols) == len(set(cols)), "duplicate columns in deeds schema"


# ===========================================================================
# Loader threshold-floor wiring — verify self._thresholds is plumbed through
# to the find_property_by_* callsites we edited
# ===========================================================================

class TestLoaderFloorWiring:
    """
    Spot-check that the loaders we patched pull their floor from self._thresholds.
    The actual matching call is mocked — we just assert the threshold arg's value.
    """

    def test_violations_address_floor_is_county_aware(self, monkeypatch):
        from src.loaders.violations import ViolationLoader

        hills = ViolationLoader(session=MagicMock(), county_id="hillsborough")
        pin = ViolationLoader(session=MagicMock(), county_id="pinellas")

        called_thresholds = []

        def fake_find_property_by_address(self, address, threshold=85, **kw):
            called_thresholds.append(threshold)
            return None

        monkeypatch.setattr(
            BaseLoader, "find_property_by_address", fake_find_property_by_address
        )
        # quarantine_unmatched + safe_add aren't relevant for this test
        monkeypatch.setattr(hills, "quarantine_unmatched", lambda **kw: None)
        monkeypatch.setattr(pin, "quarantine_unmatched", lambda **kw: None)

        df = pd.DataFrame([{
            "Record Number": "V1",
            "Address": "123 MAIN ST",
            "Record Type": "X",
            "Description": None,
            "Status": None,
            "Date": None,
            "Fine Amount": None,
            "Is Lien": None,
        }])

        hills.load_from_dataframe(df, skip_duplicates=False)
        pin.load_from_dataframe(df, skip_duplicates=False)

        # Hillsborough uses default address floor (75); Pinellas uses per-method split (60)
        assert called_thresholds[0] == 75
        assert called_thresholds[1] == 60  # address_floor intentionally 60 (2026-05-22 split)

    def test_lis_pendens_owner_name_floor_is_county_aware(self, monkeypatch):
        from src.loaders.lis_pendens import LisPendensLoader

        hills = LisPendensLoader(session=MagicMock(), county_id="hillsborough")
        pin = LisPendensLoader(session=MagicMock(), county_id="pinellas")

        called_thresholds = []

        def fake_find_property_by_owner_name(self, name, threshold=80):
            called_thresholds.append(threshold)
            return None

        # Other strategies must not match so we fall through to owner_name path
        monkeypatch.setattr(
            BaseLoader, "find_property_by_owner_name", fake_find_property_by_owner_name
        )
        monkeypatch.setattr(
            BaseLoader, "find_property_by_legal_description", lambda self, t: None
        )
        monkeypatch.setattr(
            BaseLoader, "extract_parcel_ids_from_text", lambda self, t: []
        )
        monkeypatch.setattr(hills, "quarantine_unmatched", lambda **kw: None)
        monkeypatch.setattr(pin, "quarantine_unmatched", lambda **kw: None)

        df = pd.DataFrame([{
            "document_type": "LIS PENDENS",
            "Instrument": "LP-1",
            "Grantor": "BANK X",
            "Grantee": "SMITH JOHN",
            "Legal": None,
            "RecordDate": "2026-01-01",
        }])

        hills.load_from_dataframe(df, skip_duplicates=False)
        pin.load_from_dataframe(df, skip_duplicates=False)

        assert called_thresholds[0] == 75
        assert called_thresholds[1] == 65


# ===========================================================================
# Liens code-lien business rule preserved
# ===========================================================================

class TestLiensCodeLienBusinessRule:
    """
    Code liens MUST keep their hardcoded name_threshold=90 even on Pinellas,
    because of the prior 113-record false-positive cascade incident. Only
    non-code-lien types should follow the county floor.
    """

    def test_code_lien_threshold_constant_in_module(self):
        # Threshold construction is inline in liens.py — read source to confirm
        # the 90 constant for code liens is still present.
        import inspect
        from src.loaders import liens
        src = inspect.getsource(liens)
        # The line we want preserved:
        assert "90 if is_code_lien" in src, (
            "Code-lien hardcoded 90 threshold removed — would re-introduce "
            "the 113-record cascade incident risk on Pinellas/Hillsborough."
        )

    def test_non_code_lien_uses_county_floor(self):
        import inspect
        from src.loaders import liens
        src = inspect.getsource(liens)
        # The non-code branch must read from county-aware thresholds, not a literal 75
        assert "self._thresholds.owner_name_floor" in src, (
            "Non-code-lien path is not using self._thresholds — Pinellas "
            "stopgap won't apply to mechanics/judgment/tax liens."
        )
