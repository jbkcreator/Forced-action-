"""
src.utils.scraper_db_helper.load_scraped_data_to_db()'s failure-path
classification.

Regression for a real bug found while auditing whether any remaining code
path could still let a genuine no-data condition show up as a stale
scraper in Vera's CRON FRESHNESS section (check_cron_freshness() only looks
at run_success=true, not error_type). This is the shared load path for
every data_type in LOADER_MAP (violations, foreclosures, liens, deeds,
evictions, probate, permits, bankruptcy, tax, divorce_filings,
tax_deed_auction, vacant_land) — its except block already special-cased
ScraperNoDataError to compute error_type='no_data', but then hardcoded
run_success=False right next to it regardless, contradicting its own
classification. No loader raises ScraperNoDataError today (confirmed via
grep across src/loaders/), so this was latent rather than actively firing,
but it's the exact bug class this whole system exists to close and would
misfire the moment any loader is ever extended to raise it.

Mocks record_scraper_stats() entirely (same pattern as
tests/test_scraper_error_type_tagging.py) rather than touching the real DB:
load_scraped_data_to_db() has no run_date parameter, so a live-DB test here
would upsert into TODAY's real production row for whatever source_type is
under test — and outcome_category/run_success/error_type are "latest write
wins" columns, not additive, so that could clobber real monitoring data for
a source that genuinely ran today. Mocking is both safer and simpler here.
"""
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils import scraper_db_helper
from src.utils.scraper_db_helper import load_scraped_data_to_db
from src.utils.scraper_exceptions import ScraperNoDataError

_DATA_TYPE = "divorce_filings"


class _FakeLoader:
    def __init__(self, session, county_id):
        pass

    def load_from_csv(self, path, **kwargs):
        raise self._exc


class _FakeNoDataLoader(_FakeLoader):
    _exc = ScraperNoDataError("synthetic: no rows in CSV")


class _FakeTimeoutLoader(_FakeLoader):
    import requests
    _exc = requests.exceptions.Timeout("synthetic timeout")


def test_scraper_no_data_error_is_not_reported_as_a_failure(monkeypatch):
    """The core regression: a confirmed no-data condition must not make
    check_cron_freshness() (which only checks run_success=true) treat the
    source as stale."""
    monkeypatch.setitem(scraper_db_helper.LOADER_MAP, _DATA_TYPE, _FakeNoDataLoader)

    with patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_stats:
        with pytest.raises(ScraperNoDataError):
            load_scraped_data_to_db(
                _DATA_TYPE, Path("does-not-need-to-exist.csv"),
                county_id="hillsborough",
            )

    mock_stats.assert_called_once()
    kwargs = mock_stats.call_args.kwargs
    assert "run_success" not in kwargs  # outcome= drives it, not a hardcoded bool
    assert kwargs["outcome"] == "NO_DATA"


def test_a_real_failure_is_still_reported_as_a_failure(monkeypatch):
    """The fix must not accidentally make every load-failure look like a
    success — only a genuine ScraperNoDataError should derive True."""
    monkeypatch.setitem(scraper_db_helper.LOADER_MAP, _DATA_TYPE, _FakeTimeoutLoader)

    import requests
    with patch("src.utils.scraper_db_helper.record_scraper_stats") as mock_stats:
        with pytest.raises(requests.exceptions.Timeout):
            load_scraped_data_to_db(
                _DATA_TYPE, Path("does-not-need-to-exist.csv"),
                county_id="hillsborough",
            )

    mock_stats.assert_called_once()
    kwargs = mock_stats.call_args.kwargs
    assert kwargs["outcome"] == "TIMEOUT"
