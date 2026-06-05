"""Tests for BaseLoader.normalize_owner_name expansion (Pinellas probate/lien fixes).

Covers the multi-word noise phrase stripping (must run BEFORE per-token strip
or "AS TRUSTEE OF THE" gets partially eaten) plus newly added single-token
suffixes (EST, INDIVIDUALLY, AKA, FKA, NKA).
"""
import pytest

from src.loaders.base import BaseLoader, _OWNER_NAME_NOISE_PHRASES, _OWNER_NAME_SUFFIXES


class TestEstSuffix:
    """Pinellas probate records show 'EST' (not 'ESTATE') after the decedent."""

    def test_trailing_est_stripped(self):
        assert BaseLoader.normalize_owner_name("HICKS DANIEL JOSEPH EST") == "HICKS DANIEL JOSEPH"

    def test_est_word_boundary(self):
        # "WEST" must not be stripped as "EST"
        out = BaseLoader.normalize_owner_name("WEST JOHN")
        assert "WEST" in out

    def test_estate_still_stripped(self):
        assert BaseLoader.normalize_owner_name("SMITH JOHN ESTATE") == "SMITH JOHN"


class TestIndividuallyAkaFka:
    def test_individually_stripped(self):
        assert BaseLoader.normalize_owner_name("SMITH JANE INDIVIDUALLY") == "SMITH JANE"

    def test_aka_stripped(self):
        assert BaseLoader.normalize_owner_name("DOE J AKA DOE JOHN") == "DOE J DOE JOHN"

    def test_fka_stripped(self):
        assert BaseLoader.normalize_owner_name("DOE JOHN FKA SMITH JOHN") == "DOE JOHN SMITH JOHN"

    def test_nka_stripped(self):
        assert BaseLoader.normalize_owner_name("JONES MARY NKA JONES MARIA") == "JONES MARY JONES MARIA"


class TestTrusteePhrases:
    def test_as_trustee_of_the_wholesale(self):
        # Critical: must strip the whole phrase, not leave residual "AS OF"
        out = BaseLoader.normalize_owner_name(
            "JOHN SMITH AS TRUSTEE OF THE SMITH FAMILY TRUST"
        )
        assert "AS" not in out.split()
        assert "OF" not in out.split()
        assert "THE" not in out.split()
        assert "TRUSTEE" not in out
        assert "TRUST" not in out
        # Real-name tokens preserved
        assert "JOHN" in out and "SMITH" in out and "FAMILY" in out

    def test_as_successor_trustee(self):
        out = BaseLoader.normalize_owner_name("JONES MARY AS SUCCESSOR TRUSTEE")
        assert "SUCCESSOR" not in out
        assert "TRUSTEE" not in out
        assert "JONES" in out and "MARY" in out

    def test_as_nominee_for(self):
        out = BaseLoader.normalize_owner_name("MERS AS NOMINEE FOR LENDER")
        assert "NOMINEE" not in out
        assert "LENDER" in out


class TestExistingBehaviorPreserved:
    def test_llc_still_stripped(self):
        out = BaseLoader.normalize_owner_name("ACME PROPERTIES LLC")
        assert "LLC" not in out
        assert "ACME" in out and "PROPERTIES" in out

    def test_trust_still_stripped(self):
        out = BaseLoader.normalize_owner_name("SMITH FAMILY TRUST")
        assert "TRUST" not in out
        assert "SMITH" in out and "FAMILY" in out

    def test_punctuation_removed(self):
        out = BaseLoader.normalize_owner_name("O'BRIEN, JOHN")
        assert "," not in out and "'" not in out

    def test_empty_input(self):
        assert BaseLoader.normalize_owner_name("") == ""
        assert BaseLoader.normalize_owner_name(None) == ""


class TestEstateOf:
    """Bug fix: 'ESTATE OF' must strip as a unit so 'OF' is not left as residue."""

    def test_estate_of_prefix_stripped(self):
        # "ESTATE OF JOHN SMITH" → "JOHN SMITH", not "OF JOHN SMITH"
        out = BaseLoader.normalize_owner_name("ESTATE OF JOHN SMITH")
        assert "OF" not in out.split()
        assert "ESTATE" not in out
        assert "JOHN" in out and "SMITH" in out

    def test_estate_of_phrase_in_noise_list(self):
        assert "ESTATE OF" in _OWNER_NAME_NOISE_PHRASES

    def test_plain_estate_suffix_still_stripped(self):
        # Existing behaviour: "SMITH JOHN ESTATE" → "SMITH JOHN"
        assert BaseLoader.normalize_owner_name("SMITH JOHN ESTATE") == "SMITH JOHN"

    def test_estate_of_strips_unit_leaving_name_only(self):
        # "ESTATE OF JOHN SMITH HEIRS" — only ESTATE OF is noise; HEIRS stays
        out = BaseLoader.normalize_owner_name("ESTATE OF JOHN SMITH HEIRS")
        assert "ESTATE" not in out
        assert "OF" not in out.split()
        assert "JOHN" in out and "SMITH" in out


class _StubLoader(BaseLoader):
    """Minimal concrete BaseLoader for TestOwnerNameMultiCommaOrder."""
    def load_from_dataframe(self, df, skip_duplicates=True):
        return (0, 0, 0)


class TestOwnerNameMultiCommaOrder:
    """Bug fix: full string must be tried first so 'SMITH, JOHN' → 100% not 67%."""

    def test_full_string_first_in_segment_list(self):
        from unittest.mock import MagicMock, patch

        loader = _StubLoader(session=MagicMock(), county_id="pinellas")

        tried: list = []

        def capture(self_inner, name, threshold=80):
            tried.append(name)
            return None  # no match, let it exhaust all segments

        with patch.object(BaseLoader, "find_property_by_owner_name", capture):
            loader.find_property_by_owner_name_multi("SMITH, JOHN", threshold=65)

        # Full un-split string must be the very first attempt
        assert tried[0] == "SMITH, JOHN", (
            f"Expected full string first, got {tried[0]!r}. "
            "Comma-split ordering is wrong — Pinellas LAST, FIRST names return at ~67% instead of 100%."
        )

    def test_single_name_no_comma_unchanged(self):
        from unittest.mock import MagicMock, patch

        loader = _StubLoader(session=MagicMock(), county_id="pinellas")

        tried: list = []

        def capture(self_inner, name, threshold=80):
            tried.append(name)
            return None

        with patch.object(BaseLoader, "find_property_by_owner_name", capture):
            loader.find_property_by_owner_name_multi("SMITH JOHN", threshold=65)

        # No comma → exactly one segment, behaviour unchanged
        assert tried == ["SMITH JOHN"]


class TestSharedListsExported:
    def test_phrase_list_includes_required_entries(self):
        assert "AS TRUSTEE OF THE" in _OWNER_NAME_NOISE_PHRASES
        assert "AS NOMINEE FOR" in _OWNER_NAME_NOISE_PHRASES
        assert "SUCCESSOR IN INTEREST" in _OWNER_NAME_NOISE_PHRASES
        assert "ESTATE OF" in _OWNER_NAME_NOISE_PHRASES

    def test_suffix_list_includes_new_tokens(self):
        for token in ("EST", "INDIVIDUALLY", "AKA", "FKA", "NKA", "REV", "IRREV"):
            assert token in _OWNER_NAME_SUFFIXES, f"{token} missing from suffix list"


class TestLegalDescCondoRegex:
    """Fix 4: PartyAddress values starting with CONDO must route to legal-desc match."""

    def test_condo_routed_to_legal_desc(self):
        from src.loaders.legal_proceedings import _LEGAL_DESC_RE
        assert _LEGAL_DESC_RE.match("CONDO 5B BUILDING C")
        assert _LEGAL_DESC_RE.match("condo unit b bldg 3")

    def test_existing_keywords_still_match(self):
        from src.loaders.legal_proceedings import _LEGAL_DESC_RE
        assert _LEGAL_DESC_RE.match("LOT 4 BLOCK 7")
        assert _LEGAL_DESC_RE.match("UNIT 2")
        assert _LEGAL_DESC_RE.match("TRACT 5")

    def test_street_address_not_matched(self):
        from src.loaders.legal_proceedings import _LEGAL_DESC_RE
        assert not _LEGAL_DESC_RE.match("123 MAIN ST")
        assert not _LEGAL_DESC_RE.match("11308 N BLACKBARK DR")


class TestCountyAwareThresholds:
    """Fix 3: Pinellas overrides reach the threshold lookup."""

    def test_pinellas_override_returns_lower_floor(self):
        from config.matching import for_county
        pin = for_county("pinellas")
        assert pin.review_min == 0.65
        assert pin.legal_desc_floor == 65
        assert pin.owner_name_floor == 65
        assert pin.address_floor == 60  # intentionally lower (per-method split 2026-05-22)

    def test_hillsborough_uses_default(self):
        from config.matching import for_county
        hil = for_county("hillsborough")
        assert hil.review_min == 0.75
        assert hil.legal_desc_floor == 75

    def test_unknown_county_falls_back_to_default(self):
        from config.matching import for_county
        thr = for_county("nonexistent")
        assert thr.review_min == 0.75
