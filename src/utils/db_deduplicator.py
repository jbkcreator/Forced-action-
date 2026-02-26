"""
Database Deduplication Utilities

Provides functions to query database for existing records and filter DataFrames
to only new records BEFORE processing/scraping. Replaces CSV-to-CSV comparison.

This is the PRIMARY deduplication strategy - DB is the single source of truth.
"""

import traceback
from typing import Set, Dict, Any
import pandas as pd

from src.core.database import get_db_context
from src.core.models import (
    CodeViolation,
    LegalAndLien,
    Foreclosure,
    BuildingPermit,
    LegalProceeding,  # For probate, evictions, bankruptcy
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


# Mapping of data types to (Model, unique_field, csv_column)
DEDUP_CONFIG: Dict[str, tuple] = {
    'violations': (CodeViolation, 'record_number', 'Record Number'),
    'liens': (LegalAndLien, 'instrument_number', 'Instrument'),
    # Note: 'tax' uses custom logic in tax_delinquent_engine.py (queries by account_number + year)
    'foreclosures': (Foreclosure, 'case_number', 'Case Number'),
    'permits': (BuildingPermit, 'permit_number', 'Record Number'),
    # LegalProceeding uses polymorphic pattern with record_type discriminator
    'probate': (LegalProceeding, 'case_number', 'Case Number'),
    'evictions': (LegalProceeding, 'case_number', 'Case Number'),
    'bankruptcy': (LegalProceeding, 'case_number', 'Case Number'),
}


def get_existing_records(data_type: str, **filters) -> Set[str]:
    """
    Query database for existing records and return set of unique identifiers.
    
    Args:
        data_type: Type of data ('violations', 'liens', 'tax', etc.)
        **filters: Additional filters (e.g., tax_year=2026 for tax delinquencies)
        
    Returns:
        Set of unique identifiers (strings) for O(1) lookup
        
    Example:
        >>> existing = get_existing_records('violations')
        >>> existing = get_existing_records('tax', tax_year=2026)
        >>> existing = get_existing_records('probate', proceeding_type='Probate')
    """
    if data_type not in DEDUP_CONFIG:
        logger.error(f"Unknown data type: {data_type}")
        return set()
    
    model_class, field_name, _ = DEDUP_CONFIG[data_type]
    
    try:
        with get_db_context() as session:
            # Build base query
            query = session.query(getattr(model_class, field_name))
            
            # Apply filters if provided
            for filter_key, filter_value in filters.items():
                if hasattr(model_class, filter_key):
                    query = query.filter(getattr(model_class, filter_key) == filter_value)
            
            # Execute and convert to set
            results = query.distinct().all()
            existing = {row[0] for row in results if row[0]}
            
            filter_desc = f" (filters: {filters})" if filters else ""
            logger.info(f"Found {len(existing):,} existing {data_type} records in database{filter_desc}")
            return existing
            
    except Exception as e:
        logger.error(f"Failed to query existing {data_type} records: {e}")
        logger.debug(traceback.format_exc())
        return set()


def filter_new_records(df: pd.DataFrame, data_type: str, **filters) -> pd.DataFrame:
    """
    Filter DataFrame to only NEW records not in database.
    
    Args:
        df: DataFrame with scraped data
        data_type: Type of data ('violations', 'liens', 'tax', etc.)
        **filters: Additional filters for DB query (e.g., record_type='Probate')
        
    Returns:
        DataFrame containing only new records (or original df if error occurs)
        
    Example:
        >>> df_scraped = pd.read_csv('violations.csv')
        >>> df_new = filter_new_records(df_scraped, 'violations')
        >>> # Only process/load df_new (saves time and API costs!)
        
        >>> # For polymorphic models (LegalProceeding):
        >>> df_new = filter_new_records(df, 'probate', record_type='Probate')
    """
    try:
        if data_type not in DEDUP_CONFIG:
            logger.error(f"Unknown data type: {data_type}")
            return df
        
        _, _, csv_column = DEDUP_CONFIG[data_type]
        
        # Ensure column exists
        if csv_column not in df.columns:
            logger.warning(f"Column '{csv_column}' not found in DataFrame. Columns: {df.columns.tolist()}")
            logger.warning("Returning original DataFrame (no filtering)")
            return df
        
        # Get existing records from DB
        existing = get_existing_records(data_type, **filters)
        
        if not existing:
            logger.info(f"No existing records in DB, all {len(df):,} records are new")
            return df
        
        # Filter DataFrame
        initial_count = len(df)
        
        # Filter: keep rows where identifier NOT in existing set
        df_new = df[~df[csv_column].isin(existing)].copy()
        
        filtered_count = initial_count - len(df_new)
        logger.info(f"Filtered {filtered_count:,} existing records")
        logger.info(f"NEW records to process: {len(df_new):,}")
        
        return df_new
        
    except Exception as e:
        logger.error(f"Error during deduplication for {data_type}: {e}")
        logger.error(traceback.format_exc())
        logger.warning("Returning original DataFrame (no filtering)")
        return df


def check_all_exist(df: pd.DataFrame, data_type: str, **filters) -> bool:
    """
    Check if ALL records in DataFrame already exist in database.
    
    Useful for early exit before expensive processing (Firecrawl, etc.).
    
    Args:
        df: DataFrame with scraped data
        data_type: Type of data ('violations', 'liens', 'tax', etc.)
        **filters: Additional filters for DB query
        
    Returns:
        True if ALL records exist (nothing new), False otherwise
        
    Example:
        >>> if check_all_exist(df_scraped, 'tax', tax_year=2026):
        ...     logger.info("All records exist, skipping Firecrawl enrichment")
        ...     return True
    """
    df_new = filter_new_records(df, data_type, **filters)
    
    if df_new.empty:
        logger.info("✓ All records already exist in database - nothing new to process")
        return True
    
    return False


def get_dedup_summary(df_original: pd.DataFrame, df_new: pd.DataFrame, data_type: str) -> Dict[str, Any]:
    """
    Generate summary statistics for deduplication.
    
    Args:
        df_original: Original scraped DataFrame
        df_new: Filtered DataFrame with only new records
        data_type: Type of data
        
    Returns:
        Dictionary with summary statistics
    """
    total = len(df_original)
    new = len(df_new)
    existing = total - new
    
    return {
        'data_type': data_type,
        'total_scraped': total,
        'new_records': new,
        'existing_records': existing,
        'dedup_rate': (existing / total * 100) if total > 0 else 0,
        'new_rate': (new / total * 100) if total > 0 else 0,
    }
