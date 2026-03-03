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
        self._affected_property_ids: set = set()
    
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
        Find property by address using three escalating strategies.

        Strategy 1 — SQL ILIKE on house-number prefix (indexed, fast).
                     Narrows the table to properties sharing the same street
                     number, then exact-compares normalized forms in Python.
        Strategy 2 — pg_trgm similarity() on properties.address (DB-side,
                     uses GIN trigram index).  Wrapped in begin_nested()
                     savepoint so a ProgrammingError when pg_trgm is not yet
                     installed does not abort the outer transaction.
        Strategy 3 — rapidfuzz partial_ratio on the pg_trgm candidates (or
                     on the ILIKE candidates when Strategy 2 is unavailable)
                     to pick the best-scoring result above the threshold.

        Args:
            address:   Raw address string from the scraper output.
            threshold: Minimum rapidfuzz score to accept (0-100).

        Returns:
            Tuple of (Property, score) or None.
        """
        if pd.isna(address) or not address:
            return None

        normalized_search = self.normalize_address(address)
        if not normalized_search:
            return None

        # ── Strategy 1: SQL ILIKE on house number prefix ─────────────────
        # Extract the leading house number so we only pull a small slice of
        # the table rather than scanning all 522 k rows in Python.
        house_number = normalized_search.split()[0] if normalized_search.split() else ""
        candidates: list = []

        if house_number and house_number.isdigit():
            ilike_rows = (
                self.session.query(Property)
                .filter(Property.address.ilike(f"{house_number} %"))
                .all()
            )
            for prop in ilike_rows:
                if not prop.address:
                    continue
                normalized_prop = self.normalize_address(prop.address)
                if normalized_prop == normalized_search:
                    return prop, 100   # exact match — done
                if normalized_prop:
                    candidates.append((prop, normalized_prop))

        # ── Strategy 2: pg_trgm full-table similarity ────────────────────
        trgm_props: list = []
        try:
            from sqlalchemy import func as sqlfunc
            with self.session.begin_nested():   # savepoint — protects outer tx
                trgm_props = (
                    self.session.query(Property)
                    .filter(
                        Property.address.isnot(None),
                        sqlfunc.similarity(Property.address, address) >= 0.3,
                    )
                    .order_by(sqlfunc.similarity(Property.address, address).desc())
                    .limit(15)
                    .all()
                )
        except Exception:
            # pg_trgm not installed — savepoint rolled back, outer tx survives
            trgm_props = []

        for prop in trgm_props:
            if not prop.address:
                continue
            normalized_prop = self.normalize_address(prop.address)
            if normalized_prop and (prop.id, normalized_prop) not in {(p.id, n) for p, n in candidates}:
                candidates.append((prop, normalized_prop))

        # ── Strategy 3: rapidfuzz score on all candidates ────────────────
        best_match: Optional[Property] = None
        best_score = 0

        for prop, normalized_prop in candidates:
            score = fuzz.token_sort_ratio(normalized_search, normalized_prop)
            if score > best_score:
                best_score = score
                best_match = prop

        if best_score >= threshold:
            return best_match, best_score

        return None
    
    def find_property_by_legal_description(
        self,
        legal_text: str,
        threshold: int = 70,
    ) -> Optional[Tuple[Property, int]]:
        """
        Find property by parsing the legal description from a county recorder record.

        Strategy:
          1. Extract lot number, block number, and subdivision name from the
             incoming text using regex.
          2. Build a multi-ILIKE query against properties.legal_description
             using the extracted tokens (GIN trigram index makes this fast).
          3. Score each candidate with token_sort_ratio and return the best
             match above the threshold.

        This is the highest-confidence matching method for liens, deeds, and
        judgments — legal descriptions uniquely identify a parcel and do not
        vary in format the way owner names do.

        Args:
            legal_text: The 'Legal' field value from the county recorder CSV.
            threshold:  Minimum rapidfuzz score to accept (0-100).

        Returns:
            Tuple of (Property, score) or None.
        """
        if not legal_text or pd.isna(legal_text):
            return None

        legal = str(legal_text).upper().strip()
        if not legal:
            return None

        # ── Parse key tokens ────────────────────────────────────────────────
        lot_match   = re.search(r'\bLOT\s+(\d+\w*)\b', legal)
        block_match = re.search(r'\bB(?:LOCK|LK)\s+(\d+\w*)\b', legal)

        # Subdivision = text before the first structural keyword
        subd_raw = re.split(r'\b(?:LOT|BLK|BLOCK|SEC|SECTION|UNIT|TRACT)\b', legal)[0].strip()
        # Keep only words longer than 3 chars (skip filler like "OF", "THE")
        subd_words = [w for w in subd_raw.split() if len(w) > 3][:4]

        if not lot_match and not subd_words:
            return None  # Not enough info to narrow down

        # ── Build ILIKE filters ──────────────────────────────────────────────
        from sqlalchemy import and_

        filters = [Property.legal_description.isnot(None)]

        if lot_match:
            filters.append(Property.legal_description.ilike(f'%LOT {lot_match.group(1)}%'))
        if block_match:
            filters.append(Property.legal_description.ilike(f'%{block_match.group(0)}%'))
        for word in subd_words:
            filters.append(Property.legal_description.ilike(f'%{word}%'))

        candidates = (
            self.session.query(Property)
            .filter(and_(*filters))
            .limit(10)
            .all()
        )

        if not candidates:
            return None

        # ── Score candidates ────────────────────────────────────────────────
        best_match = None
        best_score = 0
        for prop in candidates:
            if not prop.legal_description:
                continue
            score = fuzz.token_sort_ratio(legal, prop.legal_description.upper())
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
        Find property by owner name with three escalating strategies.

        Strategy 1 — Exact ilike (uses index, instant).
        Strategy 2 — LIKE pattern match on first+last parts, tried in both
                     word orders (handles property-appraiser "LAST FIRST" vs
                     recorder "FIRST LAST" format difference), fuzzy-scored
                     on up to 50 candidates.
        Strategy 3 — pg_trgm full-table similarity search (DB-side, uses GIN
                     trigram index, covers all 500k+ owners efficiently).
                     Falls back gracefully if pg_trgm is not installed.

        Args:
            owner_name: Raw owner/grantor name from the source record.
            threshold:  Minimum rapidfuzz score to accept (0-100).

        Returns:
            Tuple of (Property, score) or None.
        """
        if pd.isna(owner_name) or not owner_name:
            return None

        normalized_search = self.normalize_owner_name(owner_name)
        if not normalized_search:
            return None

        # ── Strategy 1: Exact case-insensitive match ─────────────────────
        exact_owner = self.session.query(Owner).filter(
            Owner.owner_name.ilike(normalized_search)
        ).first()
        if exact_owner:
            return exact_owner.property, 100

        # ── Strategy 2: LIKE pattern + fuzzy (both word orders) ──────────
        search_parts = normalized_search.split()
        best_match: Optional[Property] = None
        best_score = 0

        if len(search_parts) >= 2:
            # Try both "FIRST ... LAST" and "LAST ... FIRST" patterns
            # because property appraiser stores LAST FIRST, recorder stores FIRST LAST
            patterns = [
                f"%{search_parts[0]}%{search_parts[-1]}%",   # original order
                f"%{search_parts[-1]}%{search_parts[0]}%",   # reversed order
            ]
            seen_ids: set = set()
            candidates = []
            for pattern in patterns:
                rows = self.session.query(Owner).filter(
                    Owner.owner_name.ilike(pattern)
                ).limit(50).all()
                for r in rows:
                    if r.id not in seen_ids:
                        seen_ids.add(r.id)
                        candidates.append(r)

            for owner in candidates:
                if not owner.owner_name:
                    continue
                normalized_owner = self.normalize_owner_name(owner.owner_name)
                score = fuzz.token_sort_ratio(normalized_search, normalized_owner)
                if score > best_score:
                    best_score = score
                    best_match = owner.property

            if best_score >= threshold:
                return best_match, best_score

        # ── Strategy 3: pg_trgm full-table similarity (replaces 100-row cap) ─
        # Uses GIN trigram index — DB-side scan, no Python loop over 500k rows.
        # IMPORTANT: wrapped in begin_nested() (savepoint) so that a ProgrammingError
        # from similarity() when pg_trgm is not installed rolls back only the savepoint
        # and leaves the outer transaction alive. A bare try/except is NOT enough —
        # psycopg2 aborts the entire transaction on any SQL error, so subsequent queries
        # (e.g. duplicate checks for the next record) would fail with InFailedSqlTransaction.
        trgm_owners = []
        try:
            from sqlalchemy import func as sqlfunc
            with self.session.begin_nested():   # savepoint
                trgm_owners = (
                    self.session.query(Owner)
                    .filter(sqlfunc.similarity(Owner.owner_name, normalized_search) >= 0.35)
                    .order_by(sqlfunc.similarity(Owner.owner_name, normalized_search).desc())
                    .limit(10)
                    .all()
                )
        except Exception:
            # pg_trgm not installed — savepoint rolled back, outer transaction intact
            trgm_owners = []

        for owner in trgm_owners:
            if not owner.owner_name:
                continue
            normalized_owner = self.normalize_owner_name(owner.owner_name)
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
    
    def safe_add(self, record: Any) -> bool:
        """
        Add a record to the session using a savepoint so that a DB constraint
        violation (UniqueViolation, etc.) on one row does NOT abort the whole
        transaction.  All other rows that succeeded remain staged for the final
        session.commit() called by the engine.

        Returns True if the record was staged successfully, False if it was
        rejected by the database (error is logged at WARNING level).
        """
        try:
            with self.session.begin_nested():   # creates a SAVEPOINT
                self.session.add(record)
                self.session.flush()            # send INSERT to DB now
            # Track affected property IDs for ingestion-time rescoring
            pid = getattr(record, "property_id", None)
            if pid is not None:
                self._affected_property_ids.add(pid)
            return True
        except Exception as e:
            logger.warning(f"Skipped record — DB rejected it: {e}")
            return False

    def get_affected_property_ids(self) -> list:
        """
        Return the list of property IDs that were added/updated during this
        loader run. Used by scraper_db_helper to trigger targeted rescoring.
        """
        return list(self._affected_property_ids)

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
