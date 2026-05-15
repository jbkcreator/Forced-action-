"""Smoke-import test for all 17 scraper engines.

Establishes the import-health baseline: every engine module must import without
raising. Runs on `dev` (pre-refactor) and on `refactor/scraper-config-yaml`
(post-refactor) with identical result — any new ImportError or NameError
from the YAML refactor is caught here.
"""

import importlib

import pytest

SCRAPER_MODULES = [
    "src.scrappers.bankruptcy.bankruptcy_engine",
    "src.scrappers.dbpr.dbpr_engine",
    "src.scrappers.deliquencies.tax_delinquent_engine",
    "src.scrappers.divorce.divorce_engine",
    "src.scrappers.evictions.evictions_engine",
    "src.scrappers.fire.fire_engine",
    "src.scrappers.flood.flood_engine",
    "src.scrappers.foreclosures.foreclosure_engine",
    "src.scrappers.insurance.insurance_engine",
    "src.scrappers.liens.lien_engine",
    "src.scrappers.master.master_engine",
    "src.scrappers.permit.permit_engine",
    "src.scrappers.probate.probate_engine",
    "src.scrappers.roofing_permits.roofing_permit_engine",
    "src.scrappers.storm.storm_engine",
    "src.scrappers.sunbiz.sunbiz_engine",
    "src.scrappers.violation.violation_engine",
]


@pytest.mark.parametrize("module_name", SCRAPER_MODULES)
def test_scraper_module_imports(module_name):
    importlib.import_module(module_name)
