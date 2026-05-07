"""
Tests for the lien scraper pipeline — no browser, no DB required.

Covers:
- Column normalisation  (_normalize_ori_columns)
- Doc-type normalisation (_canonical_doc_type, _normalize_doc_types)
- Categorisation routing (categorize_and_split_data)
- LienLoader county-awareness (__init__, _party_is_filer, is_code_lien)
- ORI → legal_proceedings column bridge (fallback constant)
- SIGNAL_SCHEMAS completeness (probate + divorce_filings schemas added)
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.scrappers.liens.lien_engine import (
    _normalize_ori_columns,
    _canonical_doc_type,
    _normalize_doc_types,
    _HILLSBOROUGH_DOC_MAP,
    _HOA_KEYWORDS,
    _IRS_KEYWORDS,
    _ORI_TO_LEGAL_COLS_FALLBACK,
    categorize_and_split_data,
)
from src.loaders.column_mapper import SIGNAL_SCHEMAS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

HILLSBOROUGH_SOURCE = {
    "ori_column_map": {},
    "ori_book_page_col": None,
    "ori_doc_type_map": {},
}

PINELLAS_SOURCE = {
    "ori_column_map": {
        "DirectName":       "Grantor",
        "IndirectName":     "Grantee",
        "InstrumentNumber": "Instrument",
        "Comments":         "Legal",
        "DocTypeDescription": "DocType",
    },
    "ori_book_page_col": "BookPage",
    "ori_doc_type_map": {
        "JUDGEMENT LIEN":                              "JUDGMENT",
        "JUDGEMENT":                                   "JUDGMENT",
        "LIEN (IRS)":                                  "TAX LIEN",
        "CERTIFIED COPY OF A COURT JUDGMENT OR ORDER": "JUDGMENT",
    },
}

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
# 1. Column normalisation
# ---------------------------------------------------------------------------

class TestNormalizeOriColumns:

    def test_hillsborough_no_rename(self):
        df = pd.DataFrame({"Grantor": ["A"], "Grantee": ["B"], "Instrument": ["12345"]})
        result = _normalize_ori_columns(df, HILLSBOROUGH_SOURCE)
        assert "Grantor" in result.columns
        assert "Grantee" in result.columns
        assert "Instrument" in result.columns

    def test_pinellas_renames_all_ori_columns(self):
        df = pd.DataFrame({
            "DirectName":        ["JOHN DOE"],
            "IndirectName":      ["JANE DOE"],
            "InstrumentNumber":  ["2024-123456"],
            "Comments":          ["LOT 1 BLK 2"],
            "DocTypeDescription":["LIEN"],
        })
        result = _normalize_ori_columns(df, PINELLAS_SOURCE)
        assert "Grantor"    in result.columns
        assert "Grantee"    in result.columns
        assert "Instrument" in result.columns
        assert "Legal"      in result.columns
        assert "DocType"    in result.columns
        # Old names gone
        assert "DirectName"         not in result.columns
        assert "InstrumentNumber"   not in result.columns

    def test_pinellas_bookpage_split(self):
        df = pd.DataFrame({"BookPage": ["23544/1338"]})
        result = _normalize_ori_columns(df, PINELLAS_SOURCE)
        assert result["Book"].iloc[0] == "23544"
        assert result["Page"].iloc[0] == "1338"
        assert "BookPage" not in result.columns

    def test_bookpage_missing_slash_gives_empty_page(self):
        df = pd.DataFrame({"BookPage": ["99999"]})
        result = _normalize_ori_columns(df, PINELLAS_SOURCE)
        assert result["Book"].iloc[0] == "99999"
        assert result["Page"].iloc[0] == ""

    def test_filing_amt_added_when_absent(self):
        df = pd.DataFrame({"Grantor": ["X"]})
        result = _normalize_ori_columns(df, HILLSBOROUGH_SOURCE)
        assert "Filing Amt" in result.columns
        assert result["Filing Amt"].isna().all()

    def test_existing_filing_amt_not_overwritten(self):
        df = pd.DataFrame({"Grantor": ["X"], "Filing Amt": [500.0]})
        result = _normalize_ori_columns(df, HILLSBOROUGH_SOURCE)
        assert result["Filing Amt"].iloc[0] == 500.0


# ---------------------------------------------------------------------------
# 2. Doc-type normalisation
# ---------------------------------------------------------------------------

class TestCanonicalDocType:

    # Hillsborough coded format (built-in map)
    @pytest.mark.parametrize("raw,expected", [
        ("(D) DEED",                              "DEED"),
        ("(TAXDEED) TAX DEED",                    "DEED"),
        ("(JUD) JUDGMENT",                        "JUDGMENT"),
        ("(CCJ) CERTIFIED COPY OF A COURT JUDGMENT", "JUDGMENT"),
        ("(LN) LIEN",                             "LIEN"),
        ("(LNCORPTX) CORP TAX LIEN FOR STATE OF FLORIDA", "TAX LIEN"),
        ("(LP) LIS PENDENS",                      "LIS PENDENS"),
        ("LIS PENDENS",                           "LIS PENDENS"),
    ])
    def test_hillsborough_coded_types(self, raw, expected):
        assert _canonical_doc_type(raw, {}) == expected

    # Pinellas verbose format (config override map)
    @pytest.mark.parametrize("raw,expected", [
        ("JUDGEMENT LIEN",                                "JUDGMENT"),
        ("JUDGEMENT",                                     "JUDGMENT"),
        ("LIEN (IRS)",                                    "TAX LIEN"),
        ("CERTIFIED COPY OF A COURT JUDGMENT OR ORDER",   "JUDGMENT"),
    ])
    def test_pinellas_verbose_types(self, raw, expected):
        assert _canonical_doc_type(raw, PINELLAS_SOURCE["ori_doc_type_map"]) == expected

    def test_passthrough_for_unknown_type(self):
        assert _canonical_doc_type("SOME RANDOM TYPE", {}) == "SOME RANDOM TYPE"

    def test_case_insensitive_config_override(self):
        doc_map = {"judgement lien": "JUDGMENT"}
        # Should match case-insensitively
        assert _canonical_doc_type("JUDGEMENT LIEN", doc_map) == "JUDGMENT"

    def test_config_override_takes_priority_over_hillsborough_map(self):
        # If config remaps something the built-in map also covers, config wins
        doc_map = {"(D) DEED": "SPECIAL DEED"}
        assert _canonical_doc_type("(D) DEED", doc_map) == "SPECIAL DEED"


class TestNormalizeDocTypes:

    def test_applies_to_doctype_column(self):
        df = pd.DataFrame({"DocType": ["(LN) LIEN", "(D) DEED", "(JUD) JUDGMENT"]})
        result = _normalize_doc_types(df, HILLSBOROUGH_SOURCE)
        assert result["DocType"].tolist() == ["LIEN", "DEED", "JUDGMENT"]

    def test_no_doctype_column_is_noop(self):
        df = pd.DataFrame({"Other": ["x"]})
        result = _normalize_doc_types(df, HILLSBOROUGH_SOURCE)
        assert list(result.columns) == ["Other"]

    def test_nan_doctype_becomes_passthrough(self):
        df = pd.DataFrame({"DocType": [None]})
        result = _normalize_doc_types(df, HILLSBOROUGH_SOURCE)
        # None → "" → not in any map → stays ""
        assert result["DocType"].iloc[0] == ""


# ---------------------------------------------------------------------------
# 3. Categorisation routing
# ---------------------------------------------------------------------------

def _make_row(doc_type, grantor="JOHN DOE", grantee="JANE DOE"):
    return {"DocType": doc_type, "Grantor": grantor, "Grantee": grantee}


def _df_with_doctype(doc_type, grantor="JOHN DOE", grantee="JANE DOE"):
    return pd.DataFrame([_make_row(doc_type, grantor, grantee)])


class TestCategorizeAndSplitData:

    def _run(self, df, county_cfg, tmp_path, monkeypatch):
        """Patch module-level dir constants so _save() writes to tmp_path."""
        import src.scrappers.liens.lien_engine as eng
        monkeypatch.setattr(eng, "PROCESSED_LIENS_DIR",     tmp_path / "liens")
        monkeypatch.setattr(eng, "PROCESSED_DEEDS_DIR",     tmp_path / "deeds")
        monkeypatch.setattr(eng, "PROCESSED_JUDGMENTS_DIR", tmp_path / "judgments")
        monkeypatch.setattr(eng, "PROCESSED_DATA_DIR",      tmp_path)
        return categorize_and_split_data(df, county_cfg)

    def test_deed_routed_to_deed_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("DEED")
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("deed" in k for k in counts)
        assert not any("lien" in k for k in counts)

    def test_judgment_routed_to_judgment_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("JUDGMENT")
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("judgment" in k for k in counts)

    def test_tax_lien_routed_to_lien_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("TAX LIEN")
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("lien" in k for k in counts)

    def test_hoa_lien_routed_to_lien_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("LIEN", grantor="PALM CREST HOMEOWNERS ASSOCIATION")
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("lien" in k for k in counts)

    # Hillsborough code lien (typed label)
    def test_hillsborough_tcl_code_lien_routed_to_lien_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("JUDGMENT", grantor="CITY OF TAMPA", grantee="JOHN SMITH")
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("lien" in k for k in counts)

    # Pinellas code lien (generic "CODE LIEN" label)
    def test_pinellas_generic_code_lien_routed_to_lien_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("LIEN", grantor="CITY OF ST. PETERSBURG", grantee="JOHN SMITH")
        counts = self._run(df, PINELLAS_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("lien" in k for k in counts)

    # Pinellas probate (from ORI — no-op for Hillsborough)
    def test_probate_routed_to_probate_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("PROBATE")
        counts = self._run(df, PINELLAS_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("probate" in k for k in counts)
        assert not any("lien" in k or "deed" in k or "judgment" in k for k in counts)

    def test_probate_real_property_also_routed(self, tmp_path, monkeypatch):
        df = _df_with_doctype("PROBATE REAL PROPERTY")
        counts = self._run(df, PINELLAS_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("probate" in k for k in counts)

    # Pinellas divorce (from ORI)
    def test_domestic_relations_routed_to_divorce_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("DOMESTIC RELATIONS JUDGMENT")
        counts = self._run(df, PINELLAS_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("divorce" in k for k in counts)
        assert not any("lien" in k or "deed" in k or "judgment" in k for k in counts)

    def test_dissolution_of_marriage_routed_to_divorce_dir(self, tmp_path, monkeypatch):
        df = _df_with_doctype("DISSOLUTION OF MARRIAGE")
        counts = self._run(df, PINELLAS_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("divorce" in k for k in counts)

    def test_lis_pendens_routed_to_lien_dir(self, tmp_path, monkeypatch):
        # LIS PENDENS is in lien_types — goes to liens dir (handled by LisPendensLoader at load time)
        df = _df_with_doctype("LIS PENDENS")
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        assert any("lien" in k for k in counts)

    def test_unknown_type_skipped(self, tmp_path, monkeypatch):
        df = _df_with_doctype("RANDOM UNKNOWN TYPE")
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        assert len(counts) == 0

    def test_row_counts_correct(self, tmp_path, monkeypatch):
        df = pd.DataFrame([
            _make_row("DEED"),
            _make_row("DEED"),
            _make_row("JUDGMENT"),
        ])
        counts = self._run(df, HILLSBOROUGH_COUNTY_CFG, tmp_path, monkeypatch)
        deed_count = next(v for k, v in counts.items() if "deed" in k)
        judgment_count = next(v for k, v in counts.items() if "judgment" in k)
        assert deed_count == 2
        assert judgment_count == 1


# ---------------------------------------------------------------------------
# 4. LienLoader county awareness
# ---------------------------------------------------------------------------

def _make_lien_loader(county_id, county_cfg):
    """Build a LienLoader with mocked DB session and county config."""
    from src.loaders.liens import LienLoader
    session = MagicMock()
    # Both are local imports inside __init__ — patch at their source modules
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

    def test_party_is_filer_hillsborough(self):
        loader = _make_lien_loader("hillsborough", HILLSBOROUGH_COUNTY_CFG)
        assert loader._party_is_filer("CITY OF TAMPA CODE ENFORCEMENT")
        assert loader._party_is_filer("HILLSBOROUGH COUNTY")
        assert not loader._party_is_filer("CITY OF ST. PETERSBURG")
        assert not loader._party_is_filer("JOHN DOE")

    def test_party_is_filer_pinellas(self):
        loader = _make_lien_loader("pinellas", PINELLAS_COUNTY_CFG)
        assert loader._party_is_filer("CITY OF ST. PETERSBURG CODE ENFORCEMENT")
        assert loader._party_is_filer("PINELLAS COUNTY")
        assert not loader._party_is_filer("CITY OF TAMPA")
        assert not loader._party_is_filer("JOHN DOE")

    def test_is_code_lien_hillsborough_tcl(self):
        loader = _make_lien_loader("hillsborough", HILLSBOROUGH_COUNTY_CFG)
        # TCL type code in doc_type triggers code lien
        doc_type_upper = "CODE LIENS (TCL)"
        is_code_lien = (
            any(code in doc_type_upper for code in loader._code_lien_city_map)
            or "CODE LIEN" in doc_type_upper
        )
        assert is_code_lien

    def test_is_code_lien_pinellas_generic_label(self):
        loader = _make_lien_loader("pinellas", PINELLAS_COUNTY_CFG)
        # Pinellas has no type codes — generic "CODE LIEN" label detected
        doc_type_upper = "CODE LIEN"
        is_code_lien = (
            any(code in doc_type_upper for code in loader._code_lien_city_map)
            or "CODE LIEN" in doc_type_upper
        )
        assert is_code_lien

    def test_is_not_code_lien_for_regular_lien(self):
        loader = _make_lien_loader("hillsborough", HILLSBOROUGH_COUNTY_CFG)
        doc_type_upper = "HOA LIENS (HL)"
        is_code_lien = (
            any(code in doc_type_upper for code in loader._code_lien_city_map)
            or "CODE LIEN" in doc_type_upper
        )
        assert not is_code_lien

    def test_county_id_stored_on_loader(self):
        loader = _make_lien_loader("pinellas", PINELLAS_COUNTY_CFG)
        assert loader.county_id == "pinellas"


# ---------------------------------------------------------------------------
# 5. ORI → legal_proceedings column bridge
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
# 6. SIGNAL_SCHEMAS — probate and divorce_filings added
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
