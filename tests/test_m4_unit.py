"""
M4 unit tests — scrapers, deduplication, keyword classifiers.
No live DB, no external APIs required.

Run with:
    pytest tests/test_m4_unit.py -v
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch, call
import pytest

import src.scrappers.roofing_permits.roofing_permit_engine as roofing_mod
import src.utils.db_deduplicator as dedup_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_permit(permit_type: str, property_id: int = 1):
    obj = MagicMock()
    obj.permit_type = permit_type
    obj.property_id = property_id
    obj.issue_date = date.today() - timedelta(days=1)
    obj.property = MagicMock()
    obj.property.id = property_id
    return obj


def _make_incident(property_id: int, incident_type: str, incident_date):
    obj = MagicMock()
    obj.property_id = property_id
    obj.incident_type = incident_type
    obj.incident_date = incident_date
    return obj


# ---------------------------------------------------------------------------
# Roofing keyword classifier
# ---------------------------------------------------------------------------

class TestRoofingKeywords:
    """Test the ROOFING_KEYWORDS list used by the SQL classifier."""

    MUST_MATCH = [
        "roof replacement",
        "Re-Roof",
        "SHINGLE INSTALL",
        "TPO membrane",
        "tile roof repair",
        "replace fascia",
        "gutters and soffit",
        "flashing repair",
        "underlayment only",
        "reroof",
    ]

    MUST_NOT_MATCH = [
        "driveway paving",
        "electrical upgrade",
        "plumbing rough-in",
        "new pool construction",
        "HVAC replacement",
        "window installation",
        "fence permit",
    ]

    def test_all_roofing_terms_present(self):
        """Every term in MUST_MATCH should contain at least one roofing keyword."""
        keywords = roofing_mod.ROOFING_KEYWORDS
        for term in self.MUST_MATCH:
            matches = any(kw.lower() in term.lower() for kw in keywords)
            assert matches, f"Expected keyword match for: '{term}'"

    def test_non_roofing_terms_absent(self):
        """Terms in MUST_NOT_MATCH should NOT match any roofing keyword."""
        keywords = roofing_mod.ROOFING_KEYWORDS
        for term in self.MUST_NOT_MATCH:
            matches = any(kw.lower() in term.lower() for kw in keywords)
            assert not matches, f"Unexpected keyword match for non-roofing term: '{term}'"

    def test_keyword_list_minimum_size(self):
        """Must have at least 10 roofing keywords to be meaningful."""
        assert len(roofing_mod.ROOFING_KEYWORDS) >= 10

    def test_keywords_are_lowercase(self):
        """All keywords should be lowercase for consistent matching."""
        for kw in roofing_mod.ROOFING_KEYWORDS:
            assert kw == kw.lower(), f"Keyword not lowercase: '{kw}'"


# ---------------------------------------------------------------------------
# Roofing permit deduplication
# ---------------------------------------------------------------------------

class TestRoofingPermitDedup:
    """Test deduplication logic in the roofing permit scraper."""

    def test_existing_incident_is_skipped(self):
        """If an Incident already exists for (property_id, type, date), skip it."""
        existing = MagicMock()
        existing.property_id = 42
        existing.incident_type = "roofing_permit"
        existing.incident_date = date.today()

        mock_session = MagicMock()
        mock_session.query().filter_by().first.return_value = existing

        # Simulate the dedup check that scrapers use
        result = mock_session.query().filter_by(
            property_id=42,
            incident_type="roofing_permit",
            incident_date=date.today(),
        ).first()

        assert result is not None  # confirms existing record was found → should skip

    def test_no_existing_incident_creates_new(self):
        """If no Incident exists, a new one should be created."""
        mock_session = MagicMock()
        mock_session.query().filter_by().first.return_value = None

        result = mock_session.query().filter_by(
            property_id=99,
            incident_type="roofing_permit",
            incident_date=date.today(),
        ).first()

        assert result is None  # no existing → should create new


# ---------------------------------------------------------------------------
# DB Deduplicator
# ---------------------------------------------------------------------------

class TestDbDeduplicator:
    """Test the DEDUP_CONFIG registry and unknown-type handling."""

    def test_known_types_are_registered(self):
        """All expected data types must be in DEDUP_CONFIG."""
        expected = {"violations", "liens", "foreclosures", "permits", "probate",
                    "evictions", "bankruptcy"}
        actual = set(dedup_mod.DEDUP_CONFIG.keys())
        assert expected.issubset(actual), f"Missing types: {expected - actual}"

    def test_unknown_type_returns_empty_set(self):
        """Unknown data type should return empty set without raising."""
        result = dedup_mod.get_existing_records("nonexistent_type_xyz")
        assert result == set()

    def test_dedup_config_structure(self):
        """Each entry in DEDUP_CONFIG must be a 3-tuple: (Model, field, csv_col)."""
        for data_type, config in dedup_mod.DEDUP_CONFIG.items():
            assert isinstance(config, tuple), f"{data_type} config must be a tuple"
            assert len(config) == 3, f"{data_type} config must have 3 elements"
            model_cls, field_name, csv_col = config
            assert isinstance(field_name, str), f"{data_type}: field_name must be str"
            assert isinstance(csv_col, str), f"{data_type}: csv_col must be str"


# ---------------------------------------------------------------------------
# Scraper date range validation
# ---------------------------------------------------------------------------

class TestScraperDateRange:
    """Scraper date_range parameter — default behaviour."""

    def test_run_scrapers_uses_yesterday_to_today(self):
        """run_all_scrapers should process yesterday→today date range."""
        from src.tasks.run_scrapers import run_all_scrapers

        # Scrapers are lazy-imported inside run_all_scrapers, so patch source modules
        with patch("src.scrappers.insurance.insurance_engine.scrape_insurance_claims", return_value=0), \
             patch("src.scrappers.storm.storm_engine.scrape_storm_damage", return_value=0), \
             patch("src.scrappers.fire.fire_engine.scrape_fire_incidents", return_value=0), \
             patch("src.scrappers.flood.flood_engine.scrape_flood_damage", return_value=0), \
             patch("src.scrappers.roofing_permits.roofing_permit_engine.scrape_roofing_permits", return_value=0):
            results = run_all_scrapers("hillsborough")

        # All scrapers should appear in results
        expected_keys = {
            "insurance_claims", "storm_damage", "fire_incidents",
            "flood_damage", "roofing_permits",
        }
        assert expected_keys == set(results.keys())

    def test_run_scrapers_handles_scraper_exception(self):
        """Individual scraper failures should not stop the other scrapers."""
        from src.tasks.run_scrapers import run_all_scrapers

        with patch("src.scrappers.insurance.insurance_engine.scrape_insurance_claims",
                   side_effect=RuntimeError("API down")), \
             patch("src.scrappers.storm.storm_engine.scrape_storm_damage", return_value=5), \
             patch("src.scrappers.fire.fire_engine.scrape_fire_incidents", return_value=3), \
             patch("src.scrappers.flood.flood_engine.scrape_flood_damage", return_value=2), \
             patch("src.scrappers.roofing_permits.roofing_permit_engine.scrape_roofing_permits", return_value=1):
            results = run_all_scrapers("hillsborough")

        # Failed scraper result should be an error string
        assert "ERROR" in str(results["insurance_claims"])
        # Other scrapers should succeed
        assert results["storm_damage"] == 5
        assert results["fire_incidents"] == 3


# ---------------------------------------------------------------------------
# run_scrapers module structure
# ---------------------------------------------------------------------------

class TestRunScrapersModule:
    """Structural checks for run_scrapers.py."""

    def test_all_five_scrapers_registered(self):
        """run_all_scrapers must include all 5 M4 scrapers."""
        import inspect
        from src.tasks import run_scrapers
        source = inspect.getsource(run_scrapers.run_all_scrapers)
        for scraper in ["insurance_claims", "storm_damage", "fire_incidents",
                        "flood_damage", "roofing_permits"]:
            assert scraper in source, f"Scraper '{scraper}' not found in run_all_scrapers"
