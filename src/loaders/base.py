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


# ─── match_method taxonomy ──────────────────────────────────────────────────
# Stored on every destination row (legal_and_liens.match_method, deeds.match_method,
# legal_proceedings.match_method, etc.). Use these constants instead of string
# literals so a typo doesn't silently corrupt the per-county debugging signal.
#
# Cascade order (stage 1 → 5), short-circuits on first hit ≥ floor:
#   1. parcel_id        — exact match on properties.parcel_id (1.0 confidence)
#   2. normalized_addr  — existing find_property_by_address waterfall (≥0.75)
#   3. owner_name_zip   — owner-name fuzzy scoped to properties.zip equality
#   4. owner_name_city  — owner-name fuzzy scoped to properties.city equality
#   5. owner_name       — owner-name fuzzy across the entire county
# Alternates:
#   legal_desc          — find_property_by_legal_description (used when
#                          PartyAddress is a lot/block string, not a street)
#   llm_verified        — promoted from any owner-* stage by the LLM tiebreaker
MATCH_METHOD_PARCEL_ID   = "parcel_id"
MATCH_METHOD_NORM_ADDR   = "normalized_address"
MATCH_METHOD_OWNER_ZIP   = "owner_name_zip"
MATCH_METHOD_OWNER_CITY  = "owner_name_city"
MATCH_METHOD_OWNER_NAME  = "owner_name"
MATCH_METHOD_LEGAL_DESC  = "legal_desc"
MATCH_METHOD_LLM         = "llm_verified"


# Multi-word role/relationship phrases stripped from owner names before
# fuzzy matching. Exported so other loaders (e.g. lis_pendens) can reuse
# the same list when filtering candidate party names.
_OWNER_NAME_NOISE_PHRASES: tuple[str, ...] = (
    "AS TRUSTEE OF THE",
    "AS SUCCESSOR TRUSTEE",
    "AS TRUSTEE OF",
    "AS NOMINEE FOR",
    "AS NOMINEE",
    "AS SUCCESSOR",
    "SUCCESSOR IN INTEREST",
    "THROUGH UNDER",
    "CLAIMING BY",
    "TRUSTEE OF THE",
    "TRUSTEE OF",
    # Probate/decedent prefix — "ESTATE OF JOHN SMITH" must strip as a unit
    # so Phase 2 ("ESTATE" single-token suffix) doesn't leave residual "OF".
    "ESTATE OF",
    # Trust compound phrases — surface in deeds (e.g. "MORGAN FAMILY LIVING
    # TRUST DATED MAY 7 2026"). Stripping the trust descriptor leaves the
    # family/surname token which is what the property table actually stores.
    "FAMILY LIVING TRUST",
    "REVOCABLE LIVING TRUST",
    "IRREVOCABLE LIVING TRUST",
    "FAMILY TRUST",
    "LIVING TRUST",
    "LAND TRUST",
)

# Tail phrase that often follows trust names: "DATED MAY 7 2026", "DTD 4/15/24",
# "DATED THE 15TH DAY OF JANUARY 2026". Stripped wholesale before token sort so
# the variable date string does not drive score differences.
_TRUST_DATE_TAIL_RE = re.compile(r'\b(?:DATED|DTD)\b.*$', re.IGNORECASE)


# Trust-type classification. Ordered longest-first so "FAMILY LIVING TRUST"
# wins over "FAMILY TRUST" when both phrases match. Returned as a snake_case
# tag so it can be persisted as metadata or shown as a separate audit column
# without losing the trust-type signal that name normalization strips.
_TRUST_TYPE_PATTERNS: tuple[tuple[str, str], ...] = (
    ("REVOCABLE LIVING TRUST",   "revocable_living_trust"),
    ("IRREVOCABLE LIVING TRUST", "irrevocable_living_trust"),
    ("FAMILY LIVING TRUST",      "family_living_trust"),
    ("FAMILY TRUST",             "family_trust"),
    ("REVOCABLE TRUST",          "revocable_trust"),
    ("IRREVOCABLE TRUST",        "irrevocable_trust"),
    ("LIVING TRUST",             "living_trust"),
    ("LAND TRUST",               "land_trust"),
    ("TRUST",                    "trust"),   # bare-word fallback
)


def extract_trust_type(raw_name: str | None) -> str | None:
    """Classify a raw owner/grantor string as a trust type, or return None.

    Examples:
      "MORGAN FAMILY LIVING TRUST DATED MAY 7 2026" -> "family_living_trust"
      "ROBERTS FAMILY TRUST"                        -> "family_trust"
      "PETER J. BILLIA REVOCABLE LIVING TRUST"      -> "revocable_living_trust"
      "BASSINGTHWAITE TRUSTEE"                      -> None  (trustee, not trust)
      "SMITH JOHN L"                                -> None

    Use this alongside normalize_owner_name when you want the trust-type
    signal preserved as separate metadata (the normalizer strips it).
    """
    if not raw_name:
        return None
    upper = str(raw_name).upper()
    for phrase, tag in _TRUST_TYPE_PATTERNS:
        if re.search(rf'\b{re.escape(phrase)}\b', upper):
            return tag
    return None

# Single-token suffixes / role labels.
_OWNER_NAME_SUFFIXES: tuple[str, ...] = (
    "LLC", "INC", "CORP", "CO", "LTD", "LP", "LLP", "PLLC",
    "TRUSTEE", "TRUST", "ESTATE", "EST",
    "REVOCABLE", "IRREVOCABLE", "REV", "IRREV",
    "INDIVIDUALLY", "AKA", "FKA", "NKA", "DBA",
    "THE", "AND", "&",
)


class BaseLoader(ABC):
    """
    Abstract base class for all data loaders.

    Provides:
    - Normalization utilities
    - Fuzzy matching functions
    - LLM-assisted borderline match verification
    - Duplicate checking
    - CSV/DataFrame loading
    """

    # Per-loader LLM call budget (calls per scraper run).
    # Subclasses override this to set their own cap.
    _LLM_MAX_CALLS: int = 20

    def __init__(self, session: Session, county_id: str = "hillsborough"):
        """
        Initialize loader with database session.

        Args:
            session: SQLAlchemy database session
            county_id: County slug from COUNTY_CONFIG (default: hillsborough)
        """
        self.session = session
        self.county_id = county_id
        self._affected_property_ids: set = set()
        from src.loaders.llm_matcher import LLMPropertyMatcher
        self._llm_matcher = LLMPropertyMatcher(max_calls=self._LLM_MAX_CALLS)
    
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
    def normalize_address(addr: str, county_id: Optional[str] = None) -> str:
        """
        Standardize address for matching.

        Delegates the heavy lifting (USPS suffixes, directionals at any
        position, unit stripping, zero-padding) to the canonical
        `src.utils.address_normalize.normalize_street_address` so the same
        rules apply at ingestion (MasterPropertyLoader) and match time.
        County-specific city/CDP token stripping stays here because it
        requires DB access.
        """
        from src.utils.address_normalize import normalize_street_address

        addr_norm = normalize_street_address(addr)
        if not addr_norm:
            return ""

        if county_id:
            try:
                from src.utils.county_config import get_county_config
                tokens = get_county_config(county_id).get("address_city_tokens", []) or []
            except Exception:
                tokens = []
            # Strip longest tokens first so "sun city center" wins over "center".
            for city in sorted(tokens, key=len, reverse=True):
                city = str(city).lower().strip()
                if city and addr_norm.endswith(' ' + city):
                    addr_norm = addr_norm[:-len(city)].strip()
                    break

        return addr_norm.strip()
    
    @staticmethod
    def normalize_owner_name(name: str) -> str:
        """
        Standardize owner name for fuzzy matching.

        Two-phase strip: multi-word noise phrases first so things like
        "AS TRUSTEE OF THE" are removed wholesale (otherwise the per-token
        loop would leave residual "AS OF"), then single-word suffixes,
        then punctuation and whitespace.

        Args:
            name: Raw owner name string

        Returns:
            Normalized owner name
        """
        if pd.isna(name) or not name:
            return ""

        name = str(name).upper().strip()

        # Phase 0: strip trust date-tail. "MORGAN FAMILY LIVING TRUST DATED MAY 7
        # 2026" → "MORGAN FAMILY LIVING TRUST". Done before phrase stripping so
        # the dangling date tokens don't survive as residual noise.
        name = _TRUST_DATE_TAIL_RE.sub('', name).strip()

        # Phase 1: multi-word noise phrases (longest first to avoid the
        # "AS TRUSTEE OF" substring eating into "AS TRUSTEE OF THE").
        for phrase in sorted(_OWNER_NAME_NOISE_PHRASES, key=len, reverse=True):
            name = re.sub(rf'\b{re.escape(phrase)}\b', ' ', name)

        # Phase 2: single-word suffixes / role labels
        for suffix in _OWNER_NAME_SUFFIXES:
            name = re.sub(rf'\b{suffix}\b\.?', '', name)

        # Remove punctuation
        name = re.sub(r'[^\w\s]', ' ', name)

        # Collapse whitespace
        name = re.sub(r'\s+', ' ', name).strip()

        # Phase 3: drop standalone middle initials (single-character tokens)
        # so "ANHEIER DIANE M" aligns with "ANHEIER DIANE MATILDE". Skip the
        # first token to avoid collapsing real first-initial owners like
        # "J SMITH" → "SMITH".
        tokens = name.split()
        if len(tokens) > 1:
            tokens = [tokens[0]] + [t for t in tokens[1:] if len(t) > 1]
            name = ' '.join(tokens)

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
            '%Y-%m-%d %I:%M %p',
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
    
    @staticmethod
    def extract_parcel_ids_from_text(text: str) -> list:
        """
        Extract Florida-style parcel IDs from free text (e.g. legal descriptions).

        Recognises two formats:
        - Hyphenated: 29-30-27-0000-00000-0001 (6 groups of digits separated by hyphens)
        - Compact:    293027000000000001       (18 contiguous digits, same structure)

        Also looks for IDs preceded by keywords like PARCEL, FOLIO, PCN, PIN.

        Returns a list of candidate parcel IDs in hyphenated canonical form
        (matching the typical properties.parcel_id storage format), with any
        duplicates removed but order preserved.
        """
        if not text or pd.isna(text):
            return []

        text = str(text).upper().strip()
        candidates: list = []
        seen: set = set()

        # Pattern 1: Hyphenated — SS-TT-RR-SSSS-SSSSS-SSSS (6 hyphen-separated groups)
        for m in re.finditer(r'\b(\d{1,2}-\d{1,2}-\d{1,2}-\d{4}-\d{4,5}-\d{4})\b', text):
            pid = m.group(1)
            if pid not in seen:
                seen.add(pid)
                candidates.append(pid)

        # Pattern 2: Compact 18-digit string (no hyphens)
        # Convert to hyphenated form: XX-XX-XX-XXXX-XXXXX-XXXX
        for m in re.finditer(r'\b(\d{18})\b', text):
            digits = m.group(1)
            pid = f"{digits[0:2]}-{digits[2:4]}-{digits[4:6]}-{digits[6:10]}-{digits[10:15]}-{digits[15:18]}"
            if pid not in seen:
                seen.add(pid)
                candidates.append(pid)

        # Pattern 3: Keyword-preceded ID (PARCEL, FOLIO, PCN, PIN followed by digits/hyphens)
        for m in re.finditer(r'\b(?:PARCEL|FOLIO|PCN|PIN)[:\s#]*(\d[\d-]{10,})', text):
            raw = m.group(1).strip('-')
            # If already hyphenated and looks valid, take as-is
            if '-' in raw and raw not in seen:
                seen.add(raw)
                candidates.append(raw)
            # If compact digits, try to format
            elif '-' not in raw and len(raw) == 18 and raw not in seen:
                pid = f"{raw[0:2]}-{raw[2:4]}-{raw[4:6]}-{raw[6:10]}-{raw[10:15]}-{raw[15:18]}"
                if pid not in seen:
                    seen.add(pid)
                    candidates.append(pid)

        return candidates

    @staticmethod
    def normalize_parcel_id(parcel_id: str) -> str:
        """Strip all separators from a parcel ID, leaving only alphanumerics.

        Allows separator-agnostic matching between sources that use slashes
        (Pinellas: 31/31/17/95096/888/0010) and those that use hyphens
        (Hillsborough: 17-29-16-17028-000-0010).
        """
        return re.sub(r'[^A-Za-z0-9]', '', str(parcel_id)).upper()

    def find_property_by_parcel_id(self, parcel_id: str) -> Optional[Property]:
        """Find property by parcel ID.

        Stage 1 — exact match (uses the parcel_id unique index, fast).
        Stage 2 — separator-normalized match via regexp_replace on the DB column
                  (handles slash vs hyphen format differences across counties).
                  Requires idx_property_parcel_id_normalized function index.
        """
        if pd.isna(parcel_id) or not parcel_id:
            return None

        clean = str(parcel_id).strip()

        # Stage 1: exact
        prop = self.session.query(Property).filter_by(
            parcel_id=clean,
            county_id=self.county_id,
        ).first()
        if prop:
            return prop

        # Stage 2: normalize both sides — strip all non-alphanumeric characters
        normalized = self.normalize_parcel_id(clean)
        if not normalized:
            return None

        from sqlalchemy import func as sqlfunc, text as sa_text
        try:
            with self.session.begin_nested():
                prop = (
                    self.session.query(Property)
                    .filter(
                        sqlfunc.regexp_replace(Property.parcel_id, '[^A-Za-z0-9]', '', 'g') == normalized,
                        Property.county_id == self.county_id,
                    )
                    .first()
                )
        except Exception:
            prop = None
        return prop
    
    def find_property_by_address(
        self,
        address: str,
        threshold: int = 85,
        zip_code: Optional[str] = None,
        strict_house_number: bool = True,
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
            zip_code:  Optional ZIP to pre-filter candidates in both strategies.
                       When provided, narrows the search to matching ZIP only —
                       safe to pass whenever the source record contains a ZIP.

        Returns:
            Tuple of (Property, score) or None.
        """
        if pd.isna(address) or not address:
            return None

        normalized_search = self.normalize_address(address, self.county_id)
        if not normalized_search:
            return None

        # ── Strategy 1: SQL ILIKE on house number prefix ─────────────────
        # Extract the leading house number so we only pull a small slice of
        # the table rather than scanning all 522 k rows in Python.
        house_number = normalized_search.split()[0] if normalized_search.split() else ""
        candidates: list = []

        if house_number and house_number.isdigit():
            ilike_filters = [
                Property.normalized_address.ilike(f"{house_number} %"),
                Property.county_id == self.county_id,
            ]
            if zip_code:
                ilike_filters.append(Property.zip == zip_code)
            ilike_rows = (
                self.session.query(Property)
                .filter(*ilike_filters)
                .all()
            )
            for prop in ilike_rows:
                normalized_prop = prop.normalized_address or ""
                if not normalized_prop:
                    continue
                if normalized_prop == normalized_search:
                    return prop, 100   # exact match — done
                candidates.append((prop, normalized_prop))

        # ── Strategy 2: pg_trgm full-table similarity ────────────────────
        trgm_props: list = []
        try:
            from sqlalchemy import func as sqlfunc
            with self.session.begin_nested():   # savepoint — protects outer tx
                trgm_filters = [
                    Property.normalized_address.isnot(None),
                    Property.county_id == self.county_id,
                    sqlfunc.similarity(Property.normalized_address, normalized_search) >= 0.3,
                ]
                if zip_code:
                    trgm_filters.append(Property.zip == zip_code)
                trgm_props = (
                    self.session.query(Property)
                    .filter(*trgm_filters)
                    .order_by(sqlfunc.similarity(Property.normalized_address, normalized_search).desc())
                    .limit(15)
                    .all()
                )
        except Exception:
            # pg_trgm not installed — savepoint rolled back, outer tx survives
            trgm_props = []

        for prop in trgm_props:
            normalized_prop = prop.normalized_address or ""
            if normalized_prop and (prop.id, normalized_prop) not in {(p.id, n) for p, n in candidates}:
                candidates.append((prop, normalized_prop))

        # ── Strategy 3: rapidfuzz score on all candidates ────────────────
        best_match: Optional[Property] = None
        best_score = 0

        search_number = normalized_search.split()[0] if normalized_search.split() else ""
        for prop, normalized_prop in candidates:
            # Reject if house numbers differ — avoids wrong-number fuzzy matches
            # (e.g. "6710 Hartford" matching "6716 Hartford" at score 92)
            # Disabled for permits: permit addresses often omit or abbreviate
            # the house number differently (e.g. unit numbers, lot references).
            prop_number = normalized_prop.split()[0] if normalized_prop.split() else ""
            if strict_house_number and search_number and prop_number and search_number != prop_number:
                continue
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

        # Normalize recorder long-forms to the abbreviations the property
        # appraiser stores in legal_description. Also strip possessive apostrophes
        # (recorder: "GIBBS ADD"; appraiser: "GIBB'S ADD") so ILIKE filters match.
        for long_form, short_form in [
            ('SUBDIVISION', 'SUB'),
            ('BUILDING',    'BLDG'),
            ('ADDITION',    'ADD'),
        ]:
            legal = re.sub(rf'\b{long_form}\b', short_form, legal)
        legal = legal.replace("'", "")

        # ── Parse key tokens ────────────────────────────────────────────────
        lot_match   = re.search(r'\bLOT\s+(\d+\w*)\b', legal)
        block_match = re.search(r'\bB(?:LOCK|LK)\s+(\d+\w*)\b', legal)

        # Subdivision = text before the first structural keyword
        parts = re.split(r'\b(?:LOT|BLK|BLOCK|SEC|SECTION|UNIT|TRACT)\b', legal)
        subd_raw = parts[0].strip()
        # ORI often puts subdivision AFTER lot/block (e.g. "LOT 16 BLOCK 4 OAKWOOD ESTATES")
        # Fallback: use the tail after the last structural keyword when prefix is empty.
        if not subd_raw and len(parts) > 1:
            subd_raw = parts[-1].strip()
        # Keep only words longer than 3 chars (skip filler like "OF", "THE", "SUB")
        subd_words = [w for w in subd_raw.split() if len(w) > 3][:4]

        if not lot_match and not subd_words:
            return None  # Not enough info to narrow down

        # ── Build ILIKE filters ──────────────────────────────────────────────
        from sqlalchemy import and_, func as sa_func

        # Strip apostrophes from the DB field at match time so GIBBS matches GIBB'S.
        legal_desc_stripped = sa_func.replace(Property.legal_description, "'", "")

        filters = [Property.legal_description.isnot(None)]

        if lot_match:
            # Use word-boundary regex (~*) instead of ILIKE to prevent LOT 1 matching LOT 10-19
            lot_num = lot_match.group(1)
            filters.append(Property.legal_description.op('~*')(rf'\mLOT {lot_num}\M'))
        if block_match:
            # Match either spelling: BLK 3 ↔ BLOCK 3 (ORI uses BLOCK, HCPA uses BLK)
            blk_num = block_match.group(1)
            filters.append(Property.legal_description.op('~*')(rf'\mB(LOCK|LK) {blk_num}\M'))
        for word in subd_words:
            filters.append(legal_desc_stripped.ilike(f'%{word}%'))

        filters.append(Property.county_id == self.county_id)
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
        threshold: int = 80
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
            Owner.owner_name.ilike(normalized_search),
            Owner.county_id == self.county_id,
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
                    Owner.owner_name.ilike(pattern),
                    Owner.county_id == self.county_id,
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
                    .filter(
                        sqlfunc.similarity(Owner.owner_name, normalized_search) >= 0.35,
                        Owner.county_id == self.county_id,
                    )
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

    def find_property_by_owner_name_multi(
        self,
        raw_name: str,
        threshold: int = 80,
    ) -> Optional[Tuple[Property, int]]:
        """
        Like find_property_by_owner_name but handles comma-separated multi-party
        fields (e.g. "KUMP LEOPOLD A, KUMP CARMEN M" or trust/multi-grantor strings).

        Splits on commas and tries each segment individually, returning the first
        match that meets the threshold. Falls back to the full string last so that
        single-name callers see identical behaviour.
        """
        if pd.isna(raw_name) or not raw_name:
            return None

        segments = [s.strip() for s in str(raw_name).split(',') if s.strip()]
        # Full string first: "SMITH, JOHN" normalises comma away → "SMITH JOHN"
        # (100% match). Individual segments are fallback for multi-grantor fields
        # like "KUMP LEOPOLD A, KUMP CARMEN M" where the combined string scores
        # too low and each party needs to be tried separately.
        if len(segments) > 1:
            segments = [str(raw_name)] + segments

        for segment in segments:
            result = self.find_property_by_owner_name(segment, threshold=threshold)
            if result:
                return result
        return None

    def find_property_by_owner_name_scoped(
        self,
        owner_name: str,
        scope_column: str,
        scope_value: str,
        threshold: int = 80,
    ) -> Optional[Tuple[Property, int]]:
        """
        Owner-name fuzzy match restricted to properties where a scoping column
        (zip or city) equals an exact value. Used by the cascade's stage 3
        (owner+zip) and stage 4 (owner+city).

        The strict scope makes false positives far less likely than a county-wide
        owner-name search, so we can keep the same fuzzy threshold without
        relaxing it.

        Args:
            owner_name:   Raw owner name to match.
            scope_column: Either 'zip' or 'city' — the Property column to filter on.
            scope_value:  The exact value to match in that column.
            threshold:    Minimum rapidfuzz score.

        Returns:
            (Property, score) or None.
        """
        if pd.isna(owner_name) or not owner_name or not scope_value:
            return None
        if scope_column not in ('zip', 'city'):
            raise ValueError(f"scope_column must be 'zip' or 'city', got {scope_column!r}")

        normalized_search = self.normalize_owner_name(owner_name)
        if not normalized_search:
            return None

        # Get the candidate Owner rows whose property matches the scope value.
        scope_col_attr = getattr(Property, scope_column)
        candidates = (
            self.session.query(Owner)
            .join(Property, Owner.property_id == Property.id)
            .filter(
                scope_col_attr == scope_value,
                Owner.county_id == self.county_id,
                Owner.owner_name.isnot(None),
            )
            .all()
        )

        best_match: Optional[Property] = None
        best_score = 0
        for owner in candidates:
            normalized_owner = self.normalize_owner_name(owner.owner_name or "")
            if not normalized_owner:
                continue
            score = fuzz.token_sort_ratio(normalized_search, normalized_owner)
            if score > best_score:
                best_score = score
                best_match = owner.property

        if best_score >= threshold:
            return best_match, best_score
        return None

    def find_property_cascade(
        self,
        *,
        parcel_id:    Optional[str] = None,
        address:      Optional[str] = None,
        owner_name:   Optional[str] = None,
        zip_code:     Optional[str] = None,
        city:         Optional[str] = None,
        legal_desc:   Optional[str] = None,
        addr_threshold:  int = 75,
        owner_threshold: int = 80,
        legal_threshold: int = 70,
    ) -> Tuple[Optional[Property], Optional[str], Optional[int]]:
        """
        Unified property-matching cascade. Walks five stages in order and
        short-circuits at the first stage that produces a match meeting its
        threshold. Returns the granular `match_method` so per-county debugging
        can tell which stage actually carried the match.

        Stage order:
            1. parcel_id           — exact match (confidence 100)
            2. normalized_address  — find_property_by_address (ilike → pg_trgm → rapidfuzz)
            3. owner_name + zip    — owner fuzzy scoped to properties.zip equality
            4. owner_name + city   — owner fuzzy scoped to properties.city equality
            5. owner_name          — owner fuzzy across the entire county
            (alt) legal_desc       — tried only after stage 5 if all else failed

        Any stage whose required input is missing is skipped silently — so a
        name-only caller (just `owner_name=...`) goes straight to stages 3→5
        (or just 5 if no zip/city). A parcel-id-only caller hits stage 1 and
        returns immediately.

        Returns:
            (property, match_method_const, confidence_int_0_to_100) or (None, None, None)
        """
        # Stage 1 — parcel_id (exact)
        if parcel_id and not pd.isna(parcel_id):
            prop = self.find_property_by_parcel_id(parcel_id)
            if prop:
                return prop, MATCH_METHOD_PARCEL_ID, 100

        # Stage 2 — normalized address
        if address and not pd.isna(address):
            result = self.find_property_by_address(
                address, threshold=addr_threshold, zip_code=zip_code,
            )
            if result:
                prop, score = result
                return prop, MATCH_METHOD_NORM_ADDR, score

        # Stages 3 & 4 — owner_name scoped by zip / city
        if owner_name and not pd.isna(owner_name):
            if zip_code:
                result = self.find_property_by_owner_name_scoped(
                    owner_name, scope_column='zip', scope_value=zip_code,
                    threshold=owner_threshold,
                )
                if result:
                    prop, score = result
                    return prop, MATCH_METHOD_OWNER_ZIP, score

            if city:
                result = self.find_property_by_owner_name_scoped(
                    owner_name, scope_column='city', scope_value=city,
                    threshold=owner_threshold,
                )
                if result:
                    prop, score = result
                    return prop, MATCH_METHOD_OWNER_CITY, score

            # Stage 5 — owner name across county
            result = self.find_property_by_owner_name(owner_name, threshold=owner_threshold)
            if result:
                prop, score = result
                return prop, MATCH_METHOD_OWNER_NAME, score

        # Alternate path — legal description (lot/block/subdivision strings).
        # Tried last because it's the noisiest match strategy.
        if legal_desc and not pd.isna(legal_desc):
            result = self.find_property_by_legal_description(legal_desc, threshold=legal_threshold)
            if result:
                prop, score = result
                return prop, MATCH_METHOD_LEGAL_DESC, score

        return None, None, None

    # ========================================================================
    # LLM VERIFICATION HELPERS
    # ========================================================================

    def _get_top_owner_candidates_base(self, owner_name: str, limit: int = 3) -> list:
        """
        Re-query pg_trgm for the top-N owner candidates by similarity score.
        Returns Property objects for use as LLM context candidates.
        Returns an empty list if pg_trgm is unavailable or the name is empty.
        """
        from sqlalchemy import func as sqlfunc

        normalized = self.normalize_owner_name(owner_name)
        if not normalized:
            return []

        try:
            with self.session.begin_nested():
                owners = (
                    self.session.query(Owner)
                    .filter(
                        sqlfunc.similarity(Owner.owner_name, normalized) >= 0.3,
                        Owner.county_id == self.county_id,
                    )
                    .order_by(sqlfunc.similarity(Owner.owner_name, normalized).desc())
                    .limit(limit)
                    .all()
                )
                return [o.property for o in owners if o.property]
        except Exception:
            return []

    def _apply_llm_verification(
        self,
        raw_row: dict,
        current_best,
        match_score: int,
        record_type: str,
        match_field: str,
        force: bool = False,
    ) -> tuple:
        """
        Optionally run LLM verification on a borderline name match.

        Decision logic:
        - score >= HIGH_CONFIDENCE and not force  → accept as-is (no LLM)
        - score < LLM_SCORE_FLOOR and not force   → return unchanged (caller quarantines)
        - budget exhausted                        → log warning, return unchanged
        - otherwise                               → call LLM; accept or quarantine based on result

        Args:
            raw_row:      Full source row dict for LLM context.
            current_best: Property object from name matching (may be None).
            match_score:  Rapidfuzz score (0-100).
            record_type:  Key into RECORD_TYPE_CONTEXT in llm_matcher.py.
            match_field:  CSV column whose value was matched (for LLM prompt context).
            force:        If True, bypass the HIGH_CONFIDENCE skip (for suspicious strategies).

        Returns:
            (property_record_or_None, match_method_or_None)

            match_method semantics:
              - 'llm_verified' when the LLM actually ran and accepted/overrode the match
              - None when no LLM-driven decision was made (high confidence skip,
                below floor, or budget exhausted) — the caller keeps whatever
                cascade stage the match came from (parcel_id / normalized_address
                / owner_name_zip / owner_name_city / owner_name) unchanged.
        """
        from src.loaders.llm_matcher import HIGH_CONFIDENCE, LLM_SCORE_FLOOR

        if current_best is None:
            return None, None

        # High confidence — no LLM needed; preserve cascade-stage method
        if match_score >= HIGH_CONFIDENCE and not force:
            return current_best, None

        # Below floor — cannot rescue with LLM; preserve cascade-stage method
        # so caller can decide whether to quarantine based on its own classify-match call.
        if match_score < LLM_SCORE_FLOOR and not force:
            return current_best, None

        # Budget exhausted — log and pass through; preserve cascade-stage method
        if self._llm_matcher.budget_exhausted:
            logger.warning(
                "[LLM] Budget exhausted — skipping verification for %s match "
                "(score=%d%%, record_type=%s). Accepting match as-is.",
                match_field, match_score, record_type,
            )
            return current_best, None

        # Run LLM verification
        owner_name_val = (
            raw_row.get(match_field) or raw_row.get('owner_name') or
            raw_row.get('Lead Name') or raw_row.get('LastName/CompanyName') or ''
        )
        top_candidates = self._get_top_owner_candidates_base(str(owner_name_val), limit=3)

        llm_result = self._llm_matcher.verify_match(
            raw_row=raw_row,
            candidates=top_candidates,
            current_best=current_best,
            current_score=match_score,
            record_type=record_type,
            match_field=match_field,
        )

        if llm_result.matched and llm_result.confidence in ('high', 'medium'):
            # LLM may have selected a different candidate — resolve it
            if llm_result.property_id and llm_result.property_id != current_best.id:
                override = self.session.get(Property, llm_result.property_id)
                if override:
                    logger.info(
                        "[LLM] Overrode name match to property_id=%d "
                        "(confidence=%s, record_type=%s): %s",
                        llm_result.property_id, llm_result.confidence,
                        record_type, llm_result.reason,
                    )
                    return override, 'llm_verified'
            logger.info(
                "[LLM] Verified match (confidence=%s, score=%d%%, record_type=%s): %s",
                llm_result.confidence, match_score, record_type, llm_result.reason,
            )
            return current_best, 'llm_verified'
        else:
            logger.info(
                "[LLM] Could not improve match (confidence=%s, score=%d%%, record_type=%s): %s — keeping original",
                llm_result.confidence, match_score, record_type, llm_result.reason,
            )
            return current_best, None  # LLM ran but didn't improve — preserve cascade-stage method

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

    @property
    def _thresholds(self):
        """County-aware matching thresholds for this loader."""
        from config.matching import for_county
        return for_county(self.county_id)

    def _classify_match(self, score: int, match_method: Optional[str]) -> str:
        """Return tier string based on score and method: 'matched', 'pending_review', or 'unmatched'."""
        if match_method == "llm_verified":
            return "matched"
        normalized = score / 100.0
        t = self._thresholds
        if normalized >= t.auto_match:
            return "matched"
        if normalized >= t.review_min:
            return "pending_review"
        return "unmatched"

    def quarantine_unmatched(
        self,
        source_type: str,
        raw_row: dict,
        county_id: str = None,
        instrument_number: str = None,
        grantor: str = None,
        address_string: str = None,
        match_status: str = "unmatched",
        match_confidence: Optional[float] = None,
        candidate_property_id: Optional[int] = None,
        match_method: Optional[str] = None,
    ) -> None:
        """
        Store an unmatched or pending-review record in the staging table.
        Records can be re-matched later when the master parcel is refreshed.
        """
        from src.core.models import UnmatchedRecord
        from datetime import datetime, timezone

        if county_id is None:
            county_id = self.county_id

        # Sanitize raw_row — convert non-serializable values to strings
        import math
        safe_raw = {}
        for k, v in raw_row.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                safe_raw[k] = None
            else:
                try:
                    import json
                    json.dumps(v)
                    safe_raw[k] = v
                except (TypeError, ValueError):
                    safe_raw[k] = str(v)

        values = dict(
            source_type=source_type,
            county_id=county_id,
            raw_data=safe_raw,
            instrument_number=str(instrument_number) if instrument_number else None,
            grantor=str(grantor)[:500] if grantor else None,
            address_string=str(address_string)[:500] if address_string else None,
            match_status=match_status,
            match_attempted_at=datetime.now(timezone.utc),
            match_confidence=round(match_confidence, 3) if match_confidence is not None else None,
            candidate_property_id=candidate_property_id,
            match_method=match_method,
        )

        try:
            with self.session.begin_nested():
                if values["instrument_number"]:
                    # Upsert so re-encountering the same source record refreshes
                    # the attempt timestamp + raw_data instead of raising on the
                    # partial unique index uq_unmatched_instrument_source_county.
                    from sqlalchemy.dialects.postgresql import insert as pg_insert
                    stmt = pg_insert(UnmatchedRecord.__table__).values(**values)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["instrument_number", "source_type", "county_id"],
                        index_where=UnmatchedRecord.instrument_number.isnot(None),
                        set_={
                            "raw_data":               stmt.excluded.raw_data,
                            "match_status":           stmt.excluded.match_status,
                            "match_attempted_at":     stmt.excluded.match_attempted_at,
                            "grantor":                stmt.excluded.grantor,
                            "address_string":         stmt.excluded.address_string,
                            "match_confidence":       stmt.excluded.match_confidence,
                            "candidate_property_id":  stmt.excluded.candidate_property_id,
                            "match_method":           stmt.excluded.match_method,
                        },
                    )
                    self.session.execute(stmt)
                else:
                    # No instrument_number → partial unique index doesn't apply;
                    # plain insert is fine.
                    self.session.add(UnmatchedRecord(**values))
        except Exception as e:
            logger.warning("Could not quarantine unmatched record (source=%s): %s", source_type, e)

    def get_affected_property_ids(self) -> list:
        """
        Return the list of property IDs that were added/updated during this
        loader run. Used by scraper_db_helper to trigger targeted rescoring.
        """
        return list(self._affected_property_ids)

    def check_duplicate(
        self,
        model: Any,
        unique_fields: Dict[str, Any],
        scope_county: bool = True,
    ) -> bool:
        """
        Check if record already exists in database.

        Args:
            model: SQLAlchemy model class
            unique_fields: Dict of field names and values to check
            scope_county: If True (default), auto-add county_id to the filter.
                Pass False when the model's DB-level unique constraint is
                single-column (e.g. legal_and_liens.instrument_number,
                deeds.instrument_number, legal_proceedings.case_number) —
                otherwise the dedup query is stricter than the DB constraint
                and rows that look unique-within-county will still crash
                INSERT with UniqueViolation.

        Returns:
            True if duplicate exists, False otherwise
        """
        if scope_county and hasattr(model, 'county_id') and 'county_id' not in unique_fields:
            unique_fields = {**unique_fields, 'county_id': self.county_id}
        existing = self.session.query(model).filter_by(**unique_fields).first()
        return existing is not None
