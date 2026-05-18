"""
Tests for the lien scraper pipeline — no browser, no DB required.

After the ORI→ColumnMapper collapse, column-rename / BookPage-split / doc-type
value normalization / DocType→bucket routing are all owned by ColumnMapper
(see test_column_mapper_extensions.py). The lien engine retains only:

- _code_lien_label       (party-name based labelling, county-keyword driven)
- _sub_categorise_liens  (HOA/TAX/MECHANICS/CODE labels inside the liens bucket)
- _save_buckets          (writes bucketed DataFrames into processed/<bucket>/new/)
- LienLoader county-awareness (filer keywords, code-lien city map)
- ORI→legal_proceedings column bridge (fallback constant used when ColumnMapper
  has no approved mapping for the probate/divorce signal)
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.scrappers.liens.lien_engine import (
    _code_lien_label,
    _sub_categorise_liens,
    _save_buckets,
    _HOA_KEYWORDS,
    _IRS_KEYWORDS,
    _ORI_TO_LEGAL_COLS_FALLBACK,
)
from src.loaders.column_mapper import SIGNAL_SCHEMAS


HILLSBOROUGH_COUNTY_CFG = {
    "city_filer_keywords": ["CITY OF TAMPA", "HILLSBOROUGH COUNTY"],
    "code_lien_type_map":  {"TCL": "TAMPA", "CCL": None},
}

PINELLAS_COUNTY_CFG = {
    "city_filer_keywords": [
        "PINELLAS COUNTY", "CITY OF ST. PETERSBURG", "CITY OF CLEARWATER",
        "CITY OF LARGO", "CITY OF PINELLAS PARK",
    ],
    "code_lien_type_map": {},
}


# ---------------------------------------------------------------------------
# 1. _code_lien_label
# ---------------------------------------------------------------------------

class TestCodeLienLabel:

    def test_no_match_returns_none(self):
        result = _code_lien_label("JOHN DOE", "JANE DOE",
                                  HILLSBOROUGH_COUNTY_CFG["city_filer_keywords"],
                                  HILLSBOROUGH_COUNTY_CFG["code_lien_type_map"])
        assert result is None

    def test_hillsborough_tampa_match_uses_tcl_code(self):
        result = _code_lien_label("CITY OF TAMPA", "JOHN DOE",
                                  HILLSBOROUGH_COUNTY_CFG["city_filer_keywords"],
                                  HILLSBOROUGH_COUNTY_CFG["code_lien_type_map"])
        assert result == "CODE LIENS (TCL)"

    def test_hillsborough_county_match_uses_ccl_code(self):
        result = _code_lien_label("HILLSBOROUGH COUNTY", "JOHN DOE",
                                  HILLSBOROUGH_COUNTY_CFG["city_filer_keywords"],
                                  HILLSBOROUGH_COUNTY_CFG["code_lien_type_map"])
        assert result == "CODE LIENS (CCL)"

    def test_pinellas_generic_code_lien_label(self):
        # Pinellas has empty code_lien_type_map — generic "CODE LIEN" label
        result = _code_lien_label("CITY OF ST. PETERSBURG", "JOHN DOE",
                                  PINELLAS_COUNTY_CFG["city_filer_keywords"],
                                  PINELLAS_COUNTY_CFG["code_lien_type_map"])
        assert result == "CODE LIEN"


# ---------------------------------------------------------------------------
# 2. _sub_categorise_liens — labels rows inside the liens bucket
# ---------------------------------------------------------------------------

class TestSubCategoriseLiens:

    def _row(self, doc_type, grantor="JOHN DOE", grantee="JANE DOE"):
        return {"DocType": doc_type, "Grantor": grantor, "Grantee": grantee}

    def test_lis_pendens_labelled(self):
        df = pd.DataFrame([self._row("LIS PENDENS")])
        out = _sub_categorise_liens(df, HILLSBOROUGH_COUNTY_CFG)
        assert out["document_type"].iloc[0] == "LIS PENDENS"

    def test_tax_lien_labelled(self):
        df = pd.DataFrame([self._row("TAX LIEN")])
        out = _sub_categorise_liens(df, HILLSBOROUGH_COUNTY_CFG)
        assert out["document_type"].iloc[0] == "TAX LIEN"

    def test_hoa_keyword_in_grantor_labelled_hoa(self):
        df = pd.DataFrame([self._row("LIEN", grantor="PALM CREST HOMEOWNERS ASSOCIATION")])
        out = _sub_categorise_liens(df, HILLSBOROUGH_COUNTY_CFG)
        assert out["document_type"].iloc[0] == "HOA LIENS (HL)"

    def test_irs_keyword_in_grantor_labelled_tax(self):
        df = pd.DataFrame([self._row("LIEN", grantor="INTERNAL REVENUE SERVICE")])
        out = _sub_categorise_liens(df, HILLSBOROUGH_COUNTY_CFG)
        assert out["document_type"].iloc[0] == "TAX LIEN"

    def test_hillsborough_city_filer_typed_code_lien(self):
        df = pd.DataFrame([self._row("LIEN", grantor="CITY OF TAMPA")])
        out = _sub_categorise_liens(df, HILLSBOROUGH_COUNTY_CFG)
        assert out["document_type"].iloc[0] == "CODE LIENS (TCL)"

    def test_pinellas_city_filer_generic_label(self):
        df = pd.DataFrame([self._row("LIEN", grantor="CITY OF ST. PETERSBURG")])
        out = _sub_categorise_liens(df, PINELLAS_COUNTY_CFG)
        assert out["document_type"].iloc[0] == "CODE LIEN"

    def test_plain_lien_defaults_to_mechanics(self):
        df = pd.DataFrame([self._row("LIEN", grantor="ACME PLUMBING LLC")])
        out = _sub_categorise_liens(df, HILLSBOROUGH_COUNTY_CFG)
        assert out["document_type"].iloc[0] == "MECHANICS LIENS (ML)"

    def test_empty_df_no_op(self):
        result = _sub_categorise_liens(pd.DataFrame(), HILLSBOROUGH_COUNTY_CFG)
        assert result.empty


# ---------------------------------------------------------------------------
# 3. _save_buckets — writes bucketed DataFrames to per-bucket new/ dirs
# ---------------------------------------------------------------------------

class TestSaveBuckets:

    def _patch_dirs(self, tmp_path, monkeypatch):
        import src.scrappers.liens.lien_engine as eng
        bucket_dirs = {
            "liens":     tmp_path / "liens",
            "deeds":     tmp_path / "deeds",
            "judgments": tmp_path / "judgments",
            "probate":   tmp_path / "probate",
            "divorce":   tmp_path / "divorce",
        }
        monkeypatch.setattr(eng, "_BUCKET_DIRS", bucket_dirs)
        return bucket_dirs

    def test_deeds_bucket_written(self, tmp_path, monkeypatch):
        dirs = self._patch_dirs(tmp_path, monkeypatch)
        buckets = {
            "deeds": pd.DataFrame([{"Grantor": "A", "Grantee": "B", "DocType": "DEED"}]),
        }
        counts = _save_buckets(buckets, HILLSBOROUGH_COUNTY_CFG)
        deed_files = list((dirs["deeds"] / "new").glob("*.csv"))
        assert len(deed_files) == 1
        assert sum(counts.values()) == 1

    def test_liens_bucket_gets_sub_categorisation(self, tmp_path, monkeypatch):
        dirs = self._patch_dirs(tmp_path, monkeypatch)
        # One HOA lien (by grantor name) + one mechanics lien — should both
        # land in liens dir with the correct document_type labels.
        buckets = {
            "liens": pd.DataFrame([
                {"DocType": "LIEN", "Grantor": "PALM CREST HOMEOWNERS ASSOCIATION", "Grantee": "X"},
                {"DocType": "LIEN", "Grantor": "ACME PLUMBING LLC", "Grantee": "X"},
            ]),
        }
        _save_buckets(buckets, HILLSBOROUGH_COUNTY_CFG)
        lien_csv = next((dirs["liens"] / "new").glob("*.csv"))
        df = pd.read_csv(lien_csv)
        labels = set(df["document_type"])
        assert "HOA LIENS (HL)" in labels
        assert "MECHANICS LIENS (ML)" in labels

    def test_skip_bucket_omitted(self, tmp_path, monkeypatch):
        # row_routing default is "skip" — those rows never reach _save_buckets,
        # but if they somehow show up under "skip" the function ignores them.
        dirs = self._patch_dirs(tmp_path, monkeypatch)
        buckets = {
            "skip":  pd.DataFrame([{"DocType": "WEIRD"}]),
            "deeds": pd.DataFrame([{"DocType": "DEED", "Grantor": "A", "Grantee": "B"}]),
        }
        counts = _save_buckets(buckets, HILLSBOROUGH_COUNTY_CFG)
        # Only the deeds file was written
        assert (dirs["deeds"] / "new").exists()

    def test_empty_bucket_no_write(self, tmp_path, monkeypatch):
        dirs = self._patch_dirs(tmp_path, monkeypatch)
        counts = _save_buckets({"deeds": pd.DataFrame()}, HILLSBOROUGH_COUNTY_CFG)
        assert counts == {}

    def test_default_bucket_treated_as_liens(self, tmp_path, monkeypatch):
        # Mappings without row_routing return {"_default": df} — _save_buckets
        # routes that into the liens bucket (sub-categorised).
        dirs = self._patch_dirs(tmp_path, monkeypatch)
        buckets = {
            "_default": pd.DataFrame([{"DocType": "LIS PENDENS", "Grantor": "X", "Grantee": "Y"}]),
        }
        _save_buckets(buckets, HILLSBOROUGH_COUNTY_CFG)
        assert (dirs["liens"] / "new").exists()
        lien_csv = next((dirs["liens"] / "new").glob("*.csv"))
        df = pd.read_csv(lien_csv)
        assert df["document_type"].iloc[0] == "LIS PENDENS"


# ---------------------------------------------------------------------------
# 4. Module-level constants
# ---------------------------------------------------------------------------

class TestConstants:

    def test_hoa_keywords_complete(self):
        for kw in ("ASSOCIATION", "HOA", "CONDO"):
            assert kw in _HOA_KEYWORDS

    def test_irs_keywords_complete(self):
        for kw in ("INTERNAL REVENUE", "STATE OF FLORIDA"):
            assert kw in _IRS_KEYWORDS


# ---------------------------------------------------------------------------
# 5. LienLoader county awareness
# ---------------------------------------------------------------------------

def _make_lien_loader(county_id, county_cfg):
    """Build a LienLoader with mocked DB session and county config."""
    from src.loaders.liens import LienLoader
    session = MagicMock()
    with patch("src.utils.county_config.get_county_config", return_value=county_cfg):
        with patch("src.loaders.llm_matcher.LLMPropertyMatcher"):
            return LienLoader(session, county_id)


class TestLienLoaderCountyAwareness:

    def test_hillsborough_loads_correct_keywords(self):
        loader = _make_lien_loader("hillsborough", HILLSBOROUGH_COUNTY_CFG)
        assert "CITY OF TAMPA" in loader._city_filer_keywords
        assert "HILLSBOROUGH COUNTY" in loader._city_filer_keywords

    def test_pinellas_loads_correct_keywords(self):
        loader = _make_lien_loader("pinellas", PINELLAS_COUNTY_CFG)
        assert "PINELLAS COUNTY" in loader._city_filer_keywords
        assert "CITY OF ST. PETERSBURG" in loader._city_filer_keywords
        assert "CITY OF TAMPA" not in loader._city_filer_keywords

    def test_hillsborough_code_lien_city_map(self):
        loader = _make_lien_loader("hillsborough", HILLSBOROUGH_COUNTY_CFG)
        assert "TCL" in loader._code_lien_city_map
        assert loader._code_lien_city_map["TCL"] == "TAMPA"

    def test_pinellas_empty_code_lien_city_map(self):
        loader = _make_lien_loader("pinellas", PINELLAS_COUNTY_CFG)
        assert loader._code_lien_city_map == {}

    def test_county_id_stored_on_loader(self):
        loader = _make_lien_loader("pinellas", PINELLAS_COUNTY_CFG)
        assert loader.county_id == "pinellas"


# ---------------------------------------------------------------------------
# 6. ORI → legal_proceedings column bridge fallback
# ---------------------------------------------------------------------------

class TestOriToLegalColsFallback:

    def test_all_four_key_columns_mapped(self):
        assert _ORI_TO_LEGAL_COLS_FALLBACK["Instrument"]  == "CaseNumber"
        assert _ORI_TO_LEGAL_COLS_FALLBACK["Grantor"]     == "LastName/CompanyName"
        assert _ORI_TO_LEGAL_COLS_FALLBACK["RecordDate"]  == "FilingDate"
        assert _ORI_TO_LEGAL_COLS_FALLBACK["Legal"]       == "PartyAddress"

    def test_fallback_renames_ori_df(self):
        df = pd.DataFrame({
            "Instrument": ["2024-001"],
            "Grantor":    ["JOHN DOE"],
            "RecordDate": ["2024-01-15"],
            "Legal":      ["LOT 1 BLK 2"],
            "Grantee":    ["JANE DOE"],
        })
        result = df.rename(columns=_ORI_TO_LEGAL_COLS_FALLBACK)
        assert "CaseNumber"           in result.columns
        assert "LastName/CompanyName" in result.columns
        assert "FilingDate"           in result.columns
        assert "PartyAddress"         in result.columns
        assert "Instrument"           not in result.columns
        # Grantee (no mapping) passes through
        assert "Grantee"              in result.columns


# ---------------------------------------------------------------------------
# 7. SIGNAL_SCHEMAS — probate and divorce_filings present
# ---------------------------------------------------------------------------

class TestSignalSchemasExtended:

    def test_probate_schema_present(self):
        assert "probate" in SIGNAL_SCHEMAS

    def test_divorce_filings_schema_present(self):
        assert "divorce_filings" in SIGNAL_SCHEMAS

    def test_probate_schema_has_required_loader_columns(self):
        schema = SIGNAL_SCHEMAS["probate"]
        for col in ("CaseNumber", "LastName/CompanyName", "FilingDate", "PartyAddress"):
            assert col in schema, f"Missing '{col}' in probate schema"

    def test_divorce_schema_has_required_loader_columns(self):
        schema = SIGNAL_SCHEMAS["divorce_filings"]
        for col in ("CaseNumber", "LastName/CompanyName", "FilingDate", "PartyAddress"):
            assert col in schema, f"Missing '{col}' in divorce_filings schema"
