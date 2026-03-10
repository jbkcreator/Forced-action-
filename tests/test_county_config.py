"""
Tests for:
  - COUNTY_CONFIG / get_county_config
  - BaseLoader.county_id param and property scoping
  - /api/zip-check invalid ZIP validation
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# 1. COUNTY_CONFIG — get_county_config
# ---------------------------------------------------------------------------

class TestCountyConfig:

    def test_hillsborough_returns_config(self):
        from config.constants import get_county_config
        cfg = get_county_config("hillsborough")
        assert cfg["display_name"] == "Hillsborough County"
        assert cfg["state"] == "FL"

    def test_hillsborough_has_required_urls(self):
        from config.constants import get_county_config
        cfg = get_county_config("hillsborough")
        for key in ("permit", "violation", "probate", "civil", "foreclosure",
                    "tax", "parcel", "master", "clerk_base", "clerk_access"):
            assert key in cfg["urls"], f"Missing URL key: {key}"
            assert cfg["urls"][key].startswith("http"), f"URL not valid for key: {key}"

    def test_hillsborough_has_court_config(self):
        from config.constants import get_county_config
        cfg = get_county_config("hillsborough")
        assert "bankruptcy_code" in cfg["court"]
        assert "division_prefix" in cfg["court"]

    def test_unknown_county_raises_value_error(self):
        from config.constants import get_county_config
        with pytest.raises(ValueError, match="Unknown county"):
            get_county_config("pinellas")

    def test_unknown_county_lists_supported(self):
        from config.constants import get_county_config
        with pytest.raises(ValueError, match="hillsborough"):
            get_county_config("does_not_exist")

    def test_county_config_contains_hillsborough(self):
        from config.constants import COUNTY_CONFIG
        assert "hillsborough" in COUNTY_CONFIG

    def test_permit_url_is_accela(self):
        from config.constants import get_county_config
        cfg = get_county_config("hillsborough")
        assert "accela.com" in cfg["urls"]["permit"].lower()

    def test_foreclosure_url_is_realforeclose(self):
        from config.constants import get_county_config
        cfg = get_county_config("hillsborough")
        assert "realforeclose.com" in cfg["urls"]["foreclosure"].lower()


# ---------------------------------------------------------------------------
# 2. BaseLoader — county_id param
# ---------------------------------------------------------------------------

class TestBaseLoaderCountyId:

    def _make_loader(self, county_id="hillsborough"):
        """Instantiate a concrete subclass of BaseLoader for testing."""
        from src.loaders.violations import ViolationLoader
        session = MagicMock()
        return ViolationLoader(session, county_id=county_id)

    def test_default_county_is_hillsborough(self):
        loader = self._make_loader()
        assert loader.county_id == "hillsborough"

    def test_custom_county_stored(self):
        loader = self._make_loader(county_id="pinellas")
        assert loader.county_id == "pinellas"

    def test_find_property_by_parcel_scopes_to_county(self):
        from src.loaders.violations import ViolationLoader
        session = MagicMock()
        loader = ViolationLoader(session, county_id="hillsborough")

        loader.find_property_by_parcel_id("12345")

        call_kwargs = session.query.return_value.filter_by.call_args[1]
        assert call_kwargs.get("county_id") == "hillsborough"

    def test_find_property_by_parcel_returns_none_for_empty_id(self):
        loader = self._make_loader()
        result = loader.find_property_by_parcel_id("")
        assert result is None

    def test_find_property_by_parcel_returns_none_for_nan(self):
        import pandas as pd
        loader = self._make_loader()
        result = loader.find_property_by_parcel_id(pd.NA)
        assert result is None


# ---------------------------------------------------------------------------
# 3. /api/zip-check — invalid ZIP validation
# ---------------------------------------------------------------------------

class TestZipCheckValidation:

    def _call(self, zip_code, vertical="roofing", county_id="hillsborough"):
        """Call zip_check directly, mocking the DB."""
        from src.api.main import zip_check
        db = MagicMock()
        # No properties exist for invalid ZIPs
        db.execute.return_value.scalar.return_value = 0
        return zip_check(zip_code=zip_code, vertical=vertical, county_id=county_id, db=db)

    def test_non_florida_zip_returns_invalid(self):
        result = self._call("10001")  # New York
        assert result["status"] == "invalid"

    def test_all_zeros_returns_invalid(self):
        result = self._call("00000")
        assert result["status"] == "invalid"

    def test_99999_returns_invalid(self):
        result = self._call("99999")
        assert result["status"] == "invalid"

    def test_zip_not_in_db_returns_invalid(self):
        from src.api.main import zip_check
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 0  # no properties
        result = zip_check(zip_code="33999", vertical="roofing", county_id="hillsborough", db=db)
        assert result["status"] == "invalid"

    def test_valid_zip_in_db_returns_available(self):
        from src.api.main import zip_check
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 100   # properties exist
        db.execute.return_value.scalar_one_or_none.return_value = None  # no territory row
        result = zip_check(zip_code="33510", vertical="roofing", county_id="hillsborough", db=db)
        assert result["status"] == "available"

    def test_34xxx_zip_passes_florida_check(self):
        from src.api.main import zip_check
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 50
        db.execute.return_value.scalar_one_or_none.return_value = None
        result = zip_check(zip_code="34201", vertical="roofing", county_id="hillsborough", db=db)
        assert result["status"] == "available"

    def test_non_digit_zip_returns_invalid(self):
        result = self._call("ABCDE")
        assert result["status"] == "invalid"

    def test_short_zip_returns_invalid(self):
        result = self._call("335")
        assert result["status"] == "invalid"

    def test_taken_zip_returns_taken(self):
        from src.api.main import zip_check
        from unittest.mock import MagicMock
        territory = MagicMock()
        territory.status = "locked"
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 100
        db.execute.return_value.scalar_one_or_none.return_value = territory
        result = zip_check(zip_code="33601", vertical="roofing", county_id="hillsborough", db=db)
        assert result["status"] == "taken"

    def test_grace_zip_returns_grace(self):
        from src.api.main import zip_check
        territory = MagicMock()
        territory.status = "grace"
        db = MagicMock()
        db.execute.return_value.scalar.return_value = 100
        db.execute.return_value.scalar_one_or_none.return_value = territory
        result = zip_check(zip_code="33601", vertical="roofing", county_id="hillsborough", db=db)
        assert result["status"] == "grace"


# ---------------------------------------------------------------------------
# 4. Scraper engine county_id defaults
# ---------------------------------------------------------------------------

class TestScraperEngineCountyDefaults:
    """Verify all 4 refactored engines default to hillsborough."""

    def test_permit_pipeline_default_county(self):
        import inspect
        from src.scrappers.permit.permit_engine import run_permit_pipeline
        sig = inspect.signature(run_permit_pipeline)
        assert sig.parameters["county_id"].default == "hillsborough"

    def test_foreclosure_default_county(self):
        import inspect
        from src.scrappers.foreclosures.foreclosure_engine import scrape_realforeclose_calendar
        sig = inspect.signature(scrape_realforeclose_calendar)
        assert sig.parameters["county_id"].default == "hillsborough"

    def test_probate_pipeline_default_county(self):
        import inspect
        from src.scrappers.probate.probate_engine import run_probate_pipeline
        sig = inspect.signature(run_probate_pipeline)
        assert sig.parameters["county_id"].default == "hillsborough"

    def test_lien_pipeline_default_county(self):
        import inspect
        from src.scrappers.liens.lien_engine import run_lien_pipeline
        sig = inspect.signature(run_lien_pipeline)
        assert sig.parameters["county_id"].default == "hillsborough"

    def test_permit_url_resolves_from_county_config(self):
        from config.constants import get_county_config
        cfg = get_county_config("hillsborough")
        from config.constants import PERMIT_SEARCH_URL
        assert cfg["urls"]["permit"] == PERMIT_SEARCH_URL

    def test_foreclosure_url_resolves_from_county_config(self):
        from config.constants import get_county_config, REALFORECLOSE_BASE_URL
        cfg = get_county_config("hillsborough")
        assert cfg["urls"]["foreclosure"] == REALFORECLOSE_BASE_URL
