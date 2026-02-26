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
        skip_duplicates: bool = True,
        **kwargs
    ) -> Tuple[int, int, int]:
        """
        Load data from CSV file.
        
        Args:
            csv_path: Path to CSV file
            skip_duplicates: Skip existing records
            **kwargs: Additional arguments passed to load_from_dataframe (e.g., sample_mode)
            
        Returns:
            Tuple of (matched, unmatched, skipped)
        """
        logger.info(f"Loading data from: {csv_path}")
        
        # Try reading with different options to handle malformed CSVs
        try:
            # First attempt: standard read
            df = pd.read_csv(csv_path)
        except pd.errors.ParserError:
            logger.warning(f"CSV parsing error - trying with error handling...")
            try:
                # Second attempt: skip bad lines
                df = pd.read_csv(csv_path, on_bad_lines='warn')
            except Exception:
                # Third attempt: try engine='python' for more flexible parsing
                logger.warning(f"Still having issues - trying Python engine...")
                df = pd.read_csv(csv_path, engine='python', on_bad_lines='warn')
        
        return self.load_from_dataframe(df, skip_duplicates, **kwargs)
    
    # ========================================================================
    # NORMALIZATION UTILITIES
    # ========================================================================
    
    @staticmethod
    def normalize_address(addr: str) -> str:
        """
        Standardize address for matching - uses same logic as CSV matching.
        
        Extracts street address only, removing city/state/zip and normalizing
        to lowercase with standard abbreviations.
        
        Args:
            addr: Raw address string
            
        Returns:
            Normalized street address (lowercase, abbreviated)
        """
        if pd.isna(addr) or not addr:
            return ""
        
        addr = str(addr).lower().strip()
        
        # Filter out invalid addresses
        invalid_patterns = ['not provided', 'landlord/tenant', 'progress residential', 
                           'right of wy', 'processed', 'row at', 'intersection',
                           'final', 'piles at', 'accumulations', 'county facility']
        for pattern in invalid_patterns:
            if pattern in addr:
                return ""
        
        # Filter intersections (addresses with &)
        if ' & ' in addr or ' and ' in addr:
            return ""
        
        # Remove semicolon and everything after it
        addr = addr.split(';')[0].strip()
        
        # Remove periods
        addr = addr.replace('.', '')
        
        # Standardize common abbreviations to match database format
        replacements = {
            ' street': ' st',
            ' drive': ' dr',
            ' road': ' rd',
            ' avenue': ' ave',
            ' lane': ' ln',
            ' circle': ' cir',
            ' boulevard': ' blvd',
            ' court': ' ct',
            ' place': ' pl',
            ' way': ' wy',
            'florida': 'fl',
        }
        
        for old, new in replacements.items():
            addr = addr.replace(old, new)
        
        # Remove extra spaces
        addr = ' '.join(addr.split())
        
        # Check if address starts with a number (most real addresses do)
        parts = addr.split()
        if not parts or not any(char.isdigit() for char in parts[0]):
            return ""
        
        # Split by comma and take first part (street only)
        addr = addr.split(',')[0].strip()
        
        # Remove common city names that might be embedded at the end
        cities = ['tampa', 'riverview', 'valrico', 'gibsonton', 'lithia', 
                  'brandon', 'seffner', 'plant city', 'sun city center',
                  'thonotosassa', 'odessa', 'lutz', 'wesley chapel']
        
        for city in cities:
            # Remove city name if it appears at the end
            if addr.endswith(' ' + city):
                addr = addr[:-len(city)].strip()
        
        # Remove trailing state/zip patterns
        addr = addr.split(' fl ')[0].strip()
        
        # Remove numeric-only zip codes at the end
        parts = addr.split()
        if parts and parts[-1].replace('-', '').isdigit():
            addr = ' '.join(parts[:-1])
        
        # Remove unit/apt/lot indicators and numbers
        addr = re.sub(r'\s+(apt|unit|lot|ste|suite|#)\s*\d+', '', addr)
        
        return addr.strip()
    
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
    def parse_int(value_str: str) -> Optional[int]:
        """Parse integer from string, handling decimals and invalid values."""
        if pd.isna(value_str) or not value_str:
            return None
        
        # Convert to string and clean
        clean = str(value_str).strip()
        
        # Skip non-numeric values
        if not clean or clean in ['U', 'TA', 'N/A', '']:
            return None
        
        try:
            # Try to convert to float first (handles decimal strings like '0.00')
            # Then convert to int
            float_val = float(clean)
            # Only return valid positive integers
            if float_val >= 0:
                return int(float_val)
            return None
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
        Find property by indexed database lookup with exact match.
        
        Uses same normalization as CSV matching for consistency.
        
        Args:
            address: Address to search for
            threshold: Minimum similarity score (0-100) - kept for compatibility
            
        Returns:
            Tuple of (Property, score) or None
        """
        if pd.isna(address) or not address:
            return None
        
        normalized_search = self.normalize_address(address)
        if not normalized_search:
            return None
        
        # Strategy 1: Direct string matching using normalized address
        # Query all properties and normalize their addresses (could be optimized with a computed column)
        properties = self.session.query(Property).filter(
            Property.address.isnot(None)
        ).all()
        
        for prop in properties:
            if not prop.address:
                continue
            
            normalized_prop = self.normalize_address(prop.address)
            
            # Exact match after normalization
            if normalized_search == normalized_prop:
                return prop, 100
        
        # Strategy 2: Fuzzy match fallback if no exact match
        # Only check first 1000 properties to avoid performance issues
        best_match = None
        best_score = 0
        
        check_limit = min(1000, len(properties))
        for i, prop in enumerate(properties[:check_limit]):
            if not prop.address:
                continue
            
            normalized_prop = self.normalize_address(prop.address)
            if not normalized_prop:
                continue
            
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
        Find property by indexed owner name lookup with fuzzy fallback.
        
        Uses efficient database queries with indexes before falling back
        to fuzzy matching on a limited result set.
        
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
        
        # Strategy 1: Exact match (fastest - uses index)
        exact_owner = self.session.query(Owner).filter(
            Owner.owner_name.ilike(normalized_search)
        ).first()
        
        if exact_owner:
            return exact_owner.property, 100
        
        # Strategy 2: Partial match with LIKE (uses index)
        # Try matching on first and last name parts
        search_parts = normalized_search.split()
        if len(search_parts) >= 2:
            # Match on first and last name
            name_pattern = f"%{search_parts[0]}%{search_parts[-1]}%"
            partial_matches = self.session.query(Owner).filter(
                Owner.owner_name.ilike(name_pattern)
            ).limit(50).all()
            
            if partial_matches:
                best_match = None
                best_score = 0
                
                for owner in partial_matches:
                    if not owner.owner_name:
                        continue
                    
                    normalized_owner = self.normalize_owner_name(owner.owner_name)
                    # Use token_sort_ratio for word order independence
                    score = fuzz.token_sort_ratio(normalized_search, normalized_owner)
                    
                    if score > best_score:
                        best_score = score
                        best_match = owner.property
                
                if best_score >= threshold:
                    return best_match, best_score
        
        # Strategy 3: Last resort - check first 100 owners only
        # This avoids loading all 530k owners into memory
        owners = self.session.query(Owner).limit(100).all()
        
        best_match = None
        best_score = 0
        
        for owner in owners:
            if not owner.owner_name:
                continue
            
            normalized_owner = self.normalize_owner_name(owner.owner_name)
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
