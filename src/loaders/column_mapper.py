"""
Column Mapping Middleware — sits between scrapers and loaders.

Flow per scraper run:
1. Scraper/loader calls ColumnMapper.get_or_create(signal_type, source_id, sample_df).
2. Check county_column_mappings for an is_approved=True mapping for this source.
3. Cache hit → return the mapping dict.
4. Cache miss → call LLM → save as is_approved=False (pending admin review) → return mapping.
5. LLM failure → raise NeedsMappingError (admin must create one manually before load can proceed).

Admin flow:
- Pending (is_approved=False) mappings surface in /api/admin/mappings/pending.
- Admin reviews, edits individual columns if needed, and approves.
- Or admin can create a mapping directly: POST /api/admin/mappings/manual with a file upload.
- Approved mappings are used on the next run without any LLM call.
"""

import json
import logging
from typing import Optional

import pandas as pd
import anthropic

from config.settings import get_settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical schemas — these are the column names the loaders expect.
# For master_data the canonical names are the HCPA (Hillsborough) column set;
# all other counties' names get mapped to these before the loader sees them.
# ---------------------------------------------------------------------------

SIGNAL_SCHEMAS: dict[str, list[str]] = {
    "master_data": [
        "FOLIO",        # unique parcel/property ID from the county appraiser
        "OWNER",        # primary owner name
        "SITE_ADDR",    # property street address (not mailing)
        "SITE_CITY",    # property city
        "SITE_ZIP",     # property ZIP code (5-digit)
        "TYPE",         # property use/type code
        "LEGAL1",       # legal description part 1
        "LEGAL2",       # legal description part 2
        "LEGAL3",       # legal description part 3
        "LEGAL4",       # legal description part 4
        "HEAT_AR",      # heated/living area square footage
        "ADDR_1",       # owner mailing street address
        "CITY",         # owner mailing city
        "STATE",        # owner mailing state (2-letter)
        "ZIP",          # owner mailing ZIP code
        "ASD_VAL",      # assessed value (market/county appraiser value)
        "TAX_VAL",      # taxable value (after exemptions)
        "ACREAGE",      # lot size in acres
        "YEAR_BUILT",   # year structure was built
        "tBEDS",        # number of bedrooms
        "tBATHS",       # number of bathrooms
        "SALE1_DATE",   # most recent sale date
        "SALE1_PRC",    # most recent sale price
    ],
    "foreclosures": [
        "Case Number",
        "Property Address",
        "Parcel ID",
        "Judgment Amount",
        "Auction Type",
        "Auction Status",
        "Plaintiff",
        "Defendant",
        "Auction Start Date/Time",
        "Case Detail URL",
    ],
    "liens": [
        "Grantor", "Grantee", "Instrument", "Legal", "document_type",
        "BookType", "Book", "Page", "RecordDate", "Filing Amt",
    ],
    "violations": [
        "record_number", "opened_date", "violation_type", "status", "address",
    ],
    "permits": [
        "Record Number",    # permit / application number
        "Date",             # issue / filed date
        "Record Type",      # permit type
        "Status",           # application status
        "Address",          # property address
        "Expiration Date",  # permit expiration date
        "Description",      # permit description
        "Action",           # current workflow action
        "Project Name",     # project name
    ],
    "court_records": [
        "Case Number", "FilingDate", "CaseTypeDescription", "Title",
        "PartyType", "LastName/CompanyName", "FirstName", "PartyAddress",
    ],
    "tax_delinquency": [
        "Account Number",    # parcel account id (A + parcel_id strip)
        "Tax Yr",            # tax year
        "Cert Status",       # tax certificate status
        "Deed Status",       # tax deed application status
        "Owner",             # owner name
        "Property Address",  # site address
        "Total Due",         # total amount owed (bulk download value)
        "Years Delinquent",  # years delinquent (bulk download value)
    ],
    "deeds": [
        "Grantor", "Grantee", "Instrument", "document_type",
        "Book", "Page", "RecordDate", "sale_price",
    ],
    "probate": [
        "CaseNumber", "PartyType", "LastName/CompanyName", "FirstName", "MiddleName",
        "FilingDate", "PartyAddress", "Title", "CaseTypeDescription",
    ],
    "divorce_filings": [
        "CaseNumber", "PartyType", "LastName/CompanyName", "FirstName", "MiddleName",
        "FilingDate", "PartyAddress", "Title", "CaseTypeDescription",
    ],
}

# Human-readable descriptions for LLM prompts, keyed by canonical column name.
# Only master_data columns need descriptions because their names are not self-evident.
_MASTER_DESCRIPTIONS: dict[str, str] = {
    "FOLIO":      "unique parcel/property ID assigned by the county property appraiser",
    "OWNER":      "primary owner name (person, LLC, trust, etc.)",
    "SITE_ADDR":  "property street address (where the property is located, not mailing)",
    "SITE_CITY":  "property city",
    "SITE_ZIP":   "property ZIP code (5-digit)",
    "TYPE":       "property use/type code (residential, commercial, etc.)",
    "LEGAL1":     "legal description part 1",
    "LEGAL2":     "legal description part 2",
    "LEGAL3":     "legal description part 3",
    "LEGAL4":     "legal description part 4",
    "HEAT_AR":    "heated/living area in square feet",
    "ADDR_1":     "owner mailing street address",
    "CITY":       "owner mailing city",
    "STATE":      "owner mailing state (2-letter abbreviation)",
    "ZIP":        "owner mailing ZIP code",
    "ASD_VAL":    "county assessed value (market value used for assessments)",
    "TAX_VAL":    "taxable value (assessed value minus exemptions)",
    "ACREAGE":    "lot size in acres",
    "YEAR_BUILT": "year the structure was built",
    "tBEDS":      "number of bedrooms",
    "tBATHS":     "number of bathrooms",
    "SALE1_DATE": "most recent sale date",
    "SALE1_PRC":  "most recent sale price",
}


class SkipMapping(Exception):
    """Raised when signal_type has no canonical schema defined."""


class NeedsMappingError(Exception):
    """
    Raised when LLM mapping fails and no approved mapping exists.
    The admin must create one manually via the admin UI before the load can proceed.
    """


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

class ColumnMapper:
    """
    Middleware between scrapers and loaders.

    Usage:
        mapper = ColumnMapper()
        mapping = mapper.get_or_create(signal_type, source_id, sample_df)
        canonical_df = ColumnMapper.apply(raw_df, mapping)
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._client = anthropic.Anthropic(
            api_key=settings.anthropic_api_key.get_secret_value()
        )

    # ------------------------------------------------------------------
    # Core public methods
    # ------------------------------------------------------------------

    def get_or_create(
        self,
        signal_type: str,
        source_id: int,
        sample_df: pd.DataFrame,
    ) -> dict[str, str]:
        """
        Return a column mapping dict {source_col: canonical_col} for the given source.

        Check order:
        1. Approved mapping for this source → use it directly.
        2. Pending (LLM-proposed, not yet approved) mapping → apply optimistically,
           no new LLM call (avoids redundant API calls between admin reviews).
        3. No mapping at all → call LLM → save as pending → return it.
        4. LLM fails → raise NeedsMappingError.

        Columns not covered by the mapping are passed through unchanged when apply() is called.
        """
        schema = SIGNAL_SCHEMAS.get(signal_type)
        if schema is None:
            raise SkipMapping(f"No canonical schema for signal_type '{signal_type}'")

        raw_cols = list(sample_df.columns)

        # 1 & 2: check DB (approved first, then pending)
        existing = self._fetch_best(source_id, raw_cols)
        if existing is not None:
            return existing

        # 3: no usable mapping — call LLM (pass prior rejection feedback if any)
        prior_feedback = self._fetch_reject_feedback(source_id)
        logger.info(
            "[ColumnMapper] No usable mapping — calling LLM for source_id=%s signal=%s%s",
            source_id, signal_type,
            " (with prior rejection feedback)" if prior_feedback else "",
        )
        sample_rows = sample_df.head(3).fillna("").astype(str).to_dict("records")
        mapping = self._call_llm(raw_cols, schema, signal_type, sample_rows, prior_feedback=prior_feedback)
        self._save_pending(source_id, raw_cols, mapping, sample_rows)
        logger.info("[ColumnMapper] LLM mapping saved as pending for source_id=%s", source_id)
        return mapping

    @staticmethod
    def apply(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        """Apply a mapping dict to rename df columns. Unmapped columns pass through.

        Legacy single-DataFrame path used by simple loaders. New multi-bucket
        flows (e.g. liens engine fanning out into deeds/judgments/probate) use
        `apply_transformations(df, mapping_row)` which honors post_processors,
        value_maps, and row_routing on a CountyColumnMapping row.
        """
        return df.rename(columns=mapping)

    @classmethod
    def fetch_mapping_row(cls, source_id: int, raw_cols: Optional[list[str]] = None):
        """
        Return the active CountyColumnMapping row for this source.

        Resolution order:
          1. Approved row whose source_columns overlap the current scrape ≥50%.
          2. Pending (non-rejected) row whose source_columns overlap ≥50%.
          3. If `raw_cols` is None, no overlap check — return the latest approved
             (or pending) regardless. Callers that already have a DataFrame
             should pass raw_cols so a stale-mapping situation can fall back.
          4. None if no row matches.
        """
        from src.core.database import get_db_context
        from src.core.models import CountyColumnMapping

        with get_db_context() as session:
            approved = (
                session.query(CountyColumnMapping)
                .filter_by(source_id=source_id, is_approved=True)
                .order_by(CountyColumnMapping.approved_at.desc())
                .first()
            )
            pending = (
                session.query(CountyColumnMapping)
                .filter(
                    CountyColumnMapping.source_id == source_id,
                    CountyColumnMapping.is_approved == False,
                    CountyColumnMapping.reject_feedback == None,
                )
                .order_by(CountyColumnMapping.created_at.desc())
                .first()
            )

            chosen = None
            if raw_cols is None:
                chosen = approved or pending
            else:
                raw_set = set(raw_cols)
                for candidate in (approved, pending):
                    if candidate is None:
                        continue
                    stored = set(candidate.source_columns) if isinstance(candidate.source_columns, list) else set()
                    if not stored:
                        continue
                    if len(stored & raw_set) / len(stored) >= 0.5:
                        chosen = candidate
                        break

            if chosen is None:
                return None
            # Detach so the session can close — attributes still readable.
            session.expunge(chosen)
            return chosen

    @classmethod
    def apply_transformations(
        cls,
        df: pd.DataFrame,
        mapping_row,
    ) -> dict[str, pd.DataFrame]:
        """
        Full transformation pipeline driven by a CountyColumnMapping row.

        Order (each step is optional, controlled by which JSONB fields are set):
          1. Column rename  (`mapping`)
          2. Post-processors (`post_processors` — currently only split_on_separator)
          3. Value maps     (`value_maps` — per-column value normalization)
          4. Row routing    (`row_routing` — split rows into per-bucket DataFrames)

        Returns:
          dict[bucket_name -> DataFrame]. When `row_routing` is unset, the
          result is `{"_default": <single_df>}`. Routed rows that match no
          rule and have `default == "skip"` are excluded entirely.
        """
        # 1. Rename
        df = df.rename(columns=mapping_row.mapping or {})

        # 2. Post-processors
        for pp in mapping_row.post_processors or []:
            df = cls._apply_post_processor(df, pp)

        # 3. Value maps (case-insensitive lookup, preserves unmapped values)
        for col, vmap in (mapping_row.value_maps or {}).items():
            if col in df.columns and vmap:
                normalized = {str(k).strip().upper(): v for k, v in vmap.items()}
                df = df.copy()
                df[col] = df[col].fillna("").astype(str).apply(
                    lambda raw: normalized.get(raw.strip().upper(), raw)
                )

        # 4. Routing
        if mapping_row.row_routing:
            return cls._apply_row_routing(df, mapping_row.row_routing)
        return {"_default": df}

    # ------------------------------------------------------------------
    # Transformation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_post_processor(df: pd.DataFrame, pp: dict) -> pd.DataFrame:
        """
        Apply a single post-processor op to df. New ops slot in here.

        Supported ops:
          split_on_separator
            Split `from` column on `sep` into two output columns in `into`.
            If the source column is missing, this is a no-op (allows the same
            mapping row to apply to subsets of the data).
        """
        op = pp.get("op")
        if op == "split_on_separator":
            src_col = pp["from"]
            sep = pp.get("sep", "/")
            into = pp["into"]
            if src_col not in df.columns:
                return df
            parts = df[src_col].astype(str).str.split(sep, n=len(into) - 1, expand=True)
            df = df.copy()
            for i, out_col in enumerate(into):
                df[out_col] = parts[i].str.strip() if i in parts.columns else ""
            return df.drop(columns=[src_col])

        logger.warning("[ColumnMapper] Unknown post_processor op=%r — skipping", op)
        return df

    @staticmethod
    def _apply_row_routing(df: pd.DataFrame, routing: dict) -> dict[str, pd.DataFrame]:
        """
        Split df into bucket-keyed DataFrames per the routing config.

        Rule shape (any one of `match_exact` / `match_contains` per rule):
          {"match_exact":    ["DEED", "TAX DEED"], "bucket": "deeds"}
          {"match_contains": ["LIS PENDENS"],       "bucket": "liens"}

        Rules are evaluated in order; the first match wins. Rows matching no
        rule go to the `default` bucket (or are dropped if default == "skip").
        """
        col = routing.get("column")
        if not col or col not in df.columns:
            logger.warning(
                "[ColumnMapper] row_routing column=%r not in DataFrame — returning single bucket",
                col,
            )
            return {"_default": df}

        default_bucket = routing.get("default", "skip")
        rules = routing.get("rules") or []
        values = df[col].fillna("").astype(str).str.strip().str.upper()

        def _bucket_for(value: str) -> str:
            for rule in rules:
                exacts = rule.get("match_exact") or []
                if isinstance(exacts, str):
                    exacts = [exacts]
                if exacts and value in {e.upper() for e in exacts}:
                    return rule["bucket"]
                contains = rule.get("match_contains") or []
                if isinstance(contains, str):
                    contains = [contains]
                for substr in contains:
                    if substr.upper() in value:
                        return rule["bucket"]
            return default_bucket

        df = df.copy()
        df["_bucket"] = values.apply(_bucket_for)

        out: dict[str, pd.DataFrame] = {}
        for bucket, group in df.groupby("_bucket"):
            if bucket == "skip":
                continue
            out[bucket] = group.drop(columns=["_bucket"]).reset_index(drop=True)
        return out

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _fetch_best(
        self, source_id: int, raw_cols: list[str]
    ) -> Optional[dict[str, str]]:
        """
        Return the best available mapping for this source:
        - Approved (is_approved=True) takes priority over everything.
        - Non-rejected pending (is_approved=False, reject_feedback IS NULL) is returned
          optimistically so we don't call the LLM again while admin review is outstanding.
        - Rejected mappings (reject_feedback IS NOT NULL) are skipped — the caller should
          fetch the prior feedback and pass it to a fresh LLM call.
        - Returns None when no usable mapping exists.

        Validates column overlap: if the stored source_columns covers less than 50% of
        raw_cols the mapping is too stale and we re-map.
        """
        from src.core.database import get_db_context
        from src.core.models import CountyColumnMapping

        with get_db_context() as session:
            approved = (
                session.query(CountyColumnMapping)
                .filter_by(source_id=source_id, is_approved=True)
                .order_by(CountyColumnMapping.approved_at.desc())
                .first()
            )
            # Only use pending if it has NOT been rejected
            pending = (
                session.query(CountyColumnMapping)
                .filter(
                    CountyColumnMapping.source_id == source_id,
                    CountyColumnMapping.is_approved == False,
                    CountyColumnMapping.reject_feedback == None,
                )
                .order_by(CountyColumnMapping.created_at.desc())
                .first()
            )

            # Try approved first; if its source_columns no longer overlap with the
            # current scrape we fall back to the pending row before giving up.
            # Previously this returned None as soon as approved overlap failed,
            # which triggered a fresh LLM call + new pending on every run and
            # let duplicate pending rows accumulate for the same source.
            raw_set = set(raw_cols)

            def _overlap(candidate) -> float:
                if candidate is None:
                    return -1.0
                stored = set(candidate.source_columns) if isinstance(candidate.source_columns, list) else set()
                if not stored:
                    return 0.0
                return len(stored & raw_set) / len(stored)

            for label, candidate in (
                ("approved", approved),
                ("pending (optimistic)", pending),
            ):
                if candidate is None:
                    continue
                overlap = _overlap(candidate)
                if overlap < 0.5:
                    logger.info(
                        "[ColumnMapper] %s mapping overlap too low (%.0f%%) for source_id=%s — trying next",
                        label, 100 * overlap, source_id,
                    )
                    continue
                logger.info("[ColumnMapper] Using %s mapping for source_id=%s", label, source_id)
                return dict(candidate.mapping)

            return None

    def _fetch_reject_feedback(self, source_id: int) -> Optional[str]:
        """Return the most recent rejection feedback for this source, if any."""
        from src.core.database import get_db_context
        from src.core.models import CountyColumnMapping

        with get_db_context() as session:
            row = (
                session.query(CountyColumnMapping)
                .filter(
                    CountyColumnMapping.source_id == source_id,
                    CountyColumnMapping.is_approved == False,
                    CountyColumnMapping.reject_feedback != None,
                )
                .order_by(CountyColumnMapping.created_at.desc())
                .first()
            )
            return row.reject_feedback if row else None

    def _save_pending(
        self,
        source_id: int,
        raw_cols: list[str],
        mapping: dict[str, str],
        sample_rows: list[dict],
    ) -> None:
        from src.core.database import get_db_context
        from src.core.models import CountyColumnMapping

        with get_db_context() as session:
            row = CountyColumnMapping(
                source_id=source_id,
                source_columns=sorted(raw_cols),
                mapping=mapping,
                is_approved=False,
                mapped_by="llm",
                sample_rows=sample_rows,
            )
            session.add(row)

    # ------------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------------

    def _call_llm(
        self,
        raw_cols: list[str],
        schema: list[str],
        signal_type: str,
        sample_rows: Optional[list] = None,
        prior_feedback: Optional[str] = None,
    ) -> dict[str, str]:
        prompt = self._build_prompt(raw_cols, schema, signal_type, sample_rows or [], prior_feedback=prior_feedback)

        try:
            response = self._client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2048,
                temperature=0,
                system=(
                    "You are a data engineer mapping CSV column headers from public records portals "
                    "to a canonical database schema. Respond ONLY with a valid JSON object. "
                    "No explanation, no markdown fences."
                ),
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            logger.error("[ColumnMapper] LLM call failed: %s", e)
            raise NeedsMappingError(
                f"LLM mapping failed for signal_type={signal_type}: {e}"
            ) from e

        raw_text = response.content[0].text.strip()
        # Strip markdown fences if present
        if raw_text.startswith("```"):
            parts = raw_text.split("```")
            raw_text = parts[1].lstrip("json").strip() if len(parts) > 1 else raw_text

        try:
            mapping: dict = json.loads(raw_text)
        except json.JSONDecodeError as e:
            logger.error("[ColumnMapper] LLM returned non-JSON: %s | %r", e, raw_text[:300])
            # Identity mapping — admin will need to fix it
            mapping = {}

        # Build final mapping: every raw col must have an entry.
        valid_targets = set(schema)
        result: dict[str, str] = {}
        for col in raw_cols:
            target = mapping.get(col, col)
            # Only accept target if it's a known canonical column; otherwise pass-through
            result[col] = target if target in valid_targets else col

        return result

    @staticmethod
    def _build_prompt(
        raw_cols: list[str],
        schema: list[str],
        signal_type: str,
        sample_rows: list[dict],
        prior_feedback: Optional[str] = None,
    ) -> str:
        # For master_data include descriptions to help LLM map non-obvious names
        if signal_type == "master_data":
            schema_block = "\n".join(
                f"  {col}: {_MASTER_DESCRIPTIONS.get(col, '')}"
                for col in schema
            )
        else:
            schema_block = "\n".join(f"  {col}" for col in schema)

        sample_block = json.dumps(sample_rows, indent=2) if sample_rows else "  (no sample)"

        feedback_block = (
            f"\nPRIOR ADMIN REJECTION FEEDBACK — your previous mapping was rejected with this note:\n"
            f"{prior_feedback}\n"
            "Apply this feedback to correct the mapping this time.\n"
            if prior_feedback else ""
        )

        return f"""You are mapping column headers from a {signal_type} CSV export to a canonical schema.

SOURCE COLUMNS (from the CSV file — these are the keys you must map FROM):
{json.dumps(raw_cols, indent=2)}

SAMPLE DATA (first few rows — use this to understand what each source column contains):
{sample_block}

CANONICAL TARGET COLUMNS (map TO these exact names):
{schema_block}
{feedback_block}
RULES:
- Every source column MUST appear as a key in your output JSON.
- Values must be taken verbatim from the canonical list above.
- If a source column has no reasonable match, map it to itself (pass-through).
- Do NOT invent canonical column names not listed above.
- Map based on both the column name AND the sample data values.

OUTPUT: a single JSON object only, no explanation.
Example: {{"RawCol": "CANONICAL_COL", "NoMatch": "NoMatch"}}
"""


# ---------------------------------------------------------------------------
# Backwards-compat alias used by existing code
# ---------------------------------------------------------------------------

class LLMColumnMapper(ColumnMapper):
    """Backwards-compatible alias. Prefer ColumnMapper for new code."""

    def map(self, df: pd.DataFrame, signal_type: str, source_id: int) -> pd.DataFrame:
        mapping = self.get_or_create(signal_type, source_id, df)
        return self.apply(df, mapping)
