"""
Data loaders for inserting scraped data into database.

This module provides a clean API for scrapers to insert data directly
or load from CSV files. Each loader handles matching, validation, and insertion.

Usage:
    # From scraper (DataFrame):
    from src.loaders import ViolationLoader
    loader = ViolationLoader(session)
    matched, unmatched, skipped = loader.load_from_dataframe(df)
    
    # From CSV (testing):
    from src.loaders import ViolationLoader
    loader = ViolationLoader(session)
    matched, unmatched, skipped = loader.load_from_csv("data/raw/violations/file.csv")
"""

from src.loaders.base import BaseLoader
from src.loaders.master import MasterPropertyLoader
from src.loaders.violations import ViolationLoader
from src.loaders.liens import LienLoader
from src.loaders.deeds import DeedLoader
from src.loaders.legal_proceedings import ProbateLoader, EvictionLoader, BankruptcyLoader
from src.loaders.tax import TaxDelinquencyLoader
from src.loaders.foreclosures import ForeclosureLoader
from src.loaders.permits import BuildingPermitLoader

__all__ = [
    'BaseLoader',
    'MasterPropertyLoader',
    'ViolationLoader',
    'LienLoader',
    'DeedLoader',
    'ProbateLoader',
    'EvictionLoader',
    'BankruptcyLoader',
    'TaxDelinquencyLoader',
    'ForeclosureLoader',
    'BuildingPermitLoader',
]
