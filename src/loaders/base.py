"""
Base loader class with shared matching and normalization utilities.

All data loaders inherit from BaseLoader to access:
- Address/name normalization
- Fuzzy matching functions
- Duplicate checking
- Date/amount parsing
"""

import re
import logging
from typing import Tuple, Optional, Dict, Any
from datetime import datetime
from abc import ABC, abstractmethod

import pandas as pd
from rapidfuzz import fuzz
from sqlalchemy.orm import Session

from src.core.models import Property, Owner

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """
    Abstract base class for all data loaders.
    
    Provides:
    - Normalization utilities
    - Fuzzy matching functions
    - Duplicate checking
    - CSV/DataFrame loading
    """
    
    def __init__(self, session: Session):
        """
        Initialize loader with database session.
        
        Args:
            session: SQLAlchemy database session
        """
        self.session = session
    
    # ========================================================================
    # ABSTRACT METHODS (must be implemented by subclasses)
    # ========================================================================
    
    @abstractmethod
    def load_from_dataframe(
        self,
        df: pd.DataFrame,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load data from pandas DataFrame.
        
        Args:
            df: DataFrame with data to load
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        pass
    
    def load_from_csv(
        self,
        csv_path: str,
        skip_duplicates: bool = True
    ) -> Tuple[int, int, int]:
        """
        Load data from CSV file.
        
        Args:
            csv_path: Path to CSV file
            skip_duplicates: Skip existing records
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading data from: {csv_path}")
        df = pd.read_csv(csv_path)
        return self.load_from_dataframe(df, skip_duplicates)
    
    # ========================================================================
    # NORMALIZATION UTILITIES
    # ========================================================================
    
    @staticmethod
    def normalize_address(addr: str) -> str:
        """
        Standardize address for fuzzy matching.
        
        Args:
            addr: Raw address string
            
        Returns:
            Normalized address string
        """
        if pd.isna(addr) or not addr:
            return ""
        
        addr = str(addr).upper().strip()
        
        # Remove directional prefixes/suffixes
        addr = re.sub(r'\b(NORTH|SOUTH|EAST|WEST|N|S|E|W|NE|NW|SE|SW)\b\.?', '', addr)
        
        # Standardize street types
        street_types = {
            r'\bST\b\.?': 'STREET',
            r'\bRD\b\.?': 'ROAD',
            r'\bAVE\b\.?': 'AVENUE',
            r'\bBLVD\b\.?': 'BOULEVARD',
            r'\bDR\b\.?': 'DRIVE',
            r'\bLN\b\.?': 'LANE',
            r'\bCT\b\.?': 'COURT',
            r'\bCIR\b\.?': 'CIRCLE',
            r'\bPL\b\.?': 'PLACE',
            r'\bTER\b\.?': 'TERRACE',
            r'\bWAY\b\.?': 'WAY',
            r'\bPKWY\b\.?': 'PARKWAY',
        }
        for abbr, full in street_types.items():
            addr = re.sub(abbr, full, addr)
        
        # Remove unit/apt/lot numbers
        addr = re.sub(r'(APT|UNIT|LOT|STE|SUITE|#)\s*\w+', '', addr)
        
        # Remove extra whitespace
        addr = re.sub(r'\s+', ' ', addr).strip()
        
        return addr
    
    @staticmethod
    def normalize_owner_name(name: str) -> str:
        """
        Standardize owner name for fuzzy matching.
        
        Args:
            name: Raw owner name string
            
        Returns:
            Normalized owner name
        """
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).upper().strip()
        
        # Remove legal suffixes
        suffixes = [
            'LLC', 'INC', 'CORP', 'CO', 'LTD', 'LP', 'LLP', 'PLLC',
            'TRUSTEE', 'TRUST', 'ESTATE', 'REVOCABLE', 'IRREVOCABLE',
            'THE', 'AND', '&'
        ]
        for suffix in suffixes:
            name = re.sub(rf'\b{suffix}\b\.?', '', name)
        
        # Remove punctuation
        name = re.sub(r'[^\w\s]', ' ', name)
        
        # Remove extra whitespace
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    @staticmethod
    def parse_amount(amount_str: str) -> Optional[float]:
        """Parse monetary amount from string."""
        if pd.isna(amount_str) or not amount_str:
            return None
        
        # Remove currency symbols and commas
        clean = str(amount_str).replace('$', '').replace(',', '').strip()
        
        try:
            return float(clean)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def parse_date(date_str: str) -> Optional[datetime]:
        """Parse date from various formats."""
        if pd.isna(date_str) or not date_str:
            return None
        
        date_formats = [
            '%m/%d/%Y',
            '%Y-%m-%d',
            '%m/%d/%Y %H:%M:%S %p',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %I:%M %p',
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(str(date_str).strip(), fmt)
            except ValueError:
                continue
        
        return None
    
    # ========================================================================
    # PROPERTY MATCHING UTILITIES
    # ========================================================================
    
    def find_property_by_parcel_id(self, parcel_id: str) -> Optional[Property]:
        """
        Find property by exact parcel ID match.
        
        Args:
            parcel_id: Parcel ID to search for
            
        Returns:
            Property object or None
        """
        if pd.isna(parcel_id) or not parcel_id:
            return None
        
        return self.session.query(Property).filter_by(
            parcel_id=str(parcel_id).strip()
        ).first()
    
    def find_property_by_address(
        self,
        address: str,
        threshold: int = 85
    ) -> Optional[Tuple[Property, int]]:
        """
        Find property by fuzzy address matching.
        
        Args:
            address: Address to search for
            threshold: Minimum similarity score (0-100)
            
        Returns:
            Tuple of (Property, score) or None
        """
        if pd.isna(address) or not address:
            return None
        
        normalized_search = self.normalize_address(address)
        if not normalized_search:
            return None
        
        properties = self.session.query(Property).all()
        
        best_match = None
        best_score = 0
        
        for prop in properties:
            if not prop.address:
                continue
            
            normalized_prop = self.normalize_address(prop.address)
            score = fuzz.ratio(normalized_search, normalized_prop)
            
            if score > best_score:
                best_score = score
                best_match = prop
        
        if best_score >= threshold:
            return best_match, best_score
        
        return None
    
    def find_property_by_owner_name(
        self,
        owner_name: str,
        threshold: int = 75
    ) -> Optional[Tuple[Property, int]]:
        """
        Find property by fuzzy owner name matching.
        
        Args:
            owner_name: Owner name to search for
            threshold: Minimum similarity score (0-100)
            
        Returns:
            Tuple of (Property, score) or None
        """
        if pd.isna(owner_name) or not owner_name:
            return None
        
        normalized_search = self.normalize_owner_name(owner_name)
        if not normalized_search:
            return None
        
        owners = self.session.query(Owner).all()
        
        best_match = None
        best_score = 0
        
        for owner in owners:
            if not owner.name:
                continue
            
            normalized_owner = self.normalize_owner_name(owner.name)
            # Use token_sort_ratio for word order independence
            score = fuzz.token_sort_ratio(normalized_search, normalized_owner)
            
            if score > best_score:
                best_score = score
                best_match = owner.property
        
        if best_score >= threshold:
            return best_match, best_score
        
        return None
    
    # ========================================================================
    # DUPLICATE CHECKING
    # ========================================================================
    
    def check_duplicate(
        self,
        model: Any,
        unique_fields: Dict[str, Any]
    ) -> bool:
        """
        Check if record already exists in database.
        
        Args:
            model: SQLAlchemy model class
            unique_fields: Dict of field names and values to check
            
        Returns:
            True if duplicate exists, False otherwise
        """
        existing = self.session.query(model).filter_by(**unique_fields).first()
        return existing is not None
