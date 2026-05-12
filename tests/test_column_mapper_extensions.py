"""
Tests for ColumnMapper's transformation pipeline.

Covers the three JSONB fields added on `county_column_mappings`:
- post_processors  (currently only split_on_separator)
- value_maps       (per-column value normalization)
- row_routing      (split rows into bucket-keyed DataFrames)

And the end-to-end ordering through `ColumnMapper.apply_transformations`.

No DB required — the helpers operate on a SimpleNamespace masquerading as a
CountyColumnMapping row.
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from src.loaders.column_mapper import ColumnMapper


def _mapping_row(**fields):
    """Build a stand-in mapping row. Defaults to no transformations."""
    return SimpleNamespace(
        mapping=fields.get("mapping", {}),
        post_processors=fields.get("post_processors", None),
        value_maps=fields.get("value_maps", None),
        row_routing=fields.get("row_routing", None),
    )


# ---------------------------------------------------------------------------
# 1. Column rename only
# ---------------------------------------------------------------------------

class TestColumnRename:

    def test_simple_rename(self):
        df = pd.DataFrame({"old": [1, 2]})
        row = _mapping_row(mapping={"old": "new"})
        out = ColumnMapper.apply_transformations(df, row)
        assert "new" in out["_default"].columns
        assert "old" not in out["_default"].columns

    def test_unmapped_columns_pass_through(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        row = _mapping_row(mapping={"a": "renamed_a"})
        out = ColumnMapper.apply_transformations(df, row)
        cols = set(out["_default"].columns)
        assert cols == {"renamed_a", "b"}


# ---------------------------------------------------------------------------
# 2. Post-processors — split_on_separator
# ---------------------------------------------------------------------------

class TestPostProcessorSplit:

    def test_split_bookpage(self):
        df = pd.DataFrame({"BookPage": ["23544/1338", "99/100"]})
        row = _mapping_row(
            post_processors=[
                {"op": "split_on_separator", "from": "BookPage", "sep": "/", "into": ["Book", "Page"]},
            ],
        )
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert "Book" in out.columns and "Page" in out.columns
        assert "BookPage" not in out.columns
        assert out["Book"].tolist() == ["23544", "99"]
        assert out["Page"].tolist() == ["1338", "100"]

    def test_missing_separator_gives_empty_second_part(self):
        df = pd.DataFrame({"BookPage": ["99999"]})
        row = _mapping_row(
            post_processors=[
                {"op": "split_on_separator", "from": "BookPage", "sep": "/", "into": ["Book", "Page"]},
            ],
        )
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert out["Book"].iloc[0] == "99999"
        assert out["Page"].iloc[0] == ""

    def test_missing_source_column_is_no_op(self):
        df = pd.DataFrame({"OtherCol": ["x"]})
        row = _mapping_row(
            post_processors=[
                {"op": "split_on_separator", "from": "BookPage", "sep": "/", "into": ["Book", "Page"]},
            ],
        )
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert list(out.columns) == ["OtherCol"]

    def test_unknown_op_logs_and_passes_through(self):
        df = pd.DataFrame({"x": [1]})
        row = _mapping_row(post_processors=[{"op": "totally_made_up", "from": "x"}])
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert list(out.columns) == ["x"]


# ---------------------------------------------------------------------------
# 3. Value maps — per-column value normalization
# ---------------------------------------------------------------------------

class TestValueMaps:

    def test_doc_type_normalization(self):
        df = pd.DataFrame({"DocType": ["JUDGEMENT", "LIEN (IRS)", "DEED"]})
        row = _mapping_row(
            value_maps={"DocType": {"JUDGEMENT": "JUDGMENT", "LIEN (IRS)": "TAX LIEN"}},
        )
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert out["DocType"].tolist() == ["JUDGMENT", "TAX LIEN", "DEED"]

    def test_case_insensitive_lookup(self):
        df = pd.DataFrame({"DocType": ["judgement", "  Judgement  "]})
        row = _mapping_row(value_maps={"DocType": {"JUDGEMENT": "JUDGMENT"}})
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert out["DocType"].tolist() == ["JUDGMENT", "JUDGMENT"]

    def test_unmapped_values_pass_through(self):
        df = pd.DataFrame({"DocType": ["SOMETHING ELSE"]})
        row = _mapping_row(value_maps={"DocType": {"JUDGEMENT": "JUDGMENT"}})
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert out["DocType"].iloc[0] == "SOMETHING ELSE"

    def test_missing_column_is_no_op(self):
        df = pd.DataFrame({"Other": ["x"]})
        row = _mapping_row(value_maps={"DocType": {"JUDGEMENT": "JUDGMENT"}})
        out = ColumnMapper.apply_transformations(df, row)["_default"]
        assert list(out.columns) == ["Other"]


# ---------------------------------------------------------------------------
# 4. Row routing
# ---------------------------------------------------------------------------

class TestRowRouting:

    BASE_ROUTING = {
        "column":  "DocType",
        "default": "skip",
        "rules": [
            {"match_exact":    ["DEED", "TAX DEED"], "bucket": "deeds"},
            {"match_contains": ["LIS PENDENS"],       "bucket": "liens"},
            {"match_exact":    ["JUDGMENT"],          "bucket": "judgments"},
            {"match_contains": ["DOMESTIC RELATIONS"], "bucket": "divorce"},
        ],
    }

    def test_exact_match_routes_to_bucket(self):
        df = pd.DataFrame({"DocType": ["DEED", "TAX DEED"]})
        row = _mapping_row(row_routing=self.BASE_ROUTING)
        out = ColumnMapper.apply_transformations(df, row)
        assert "deeds" in out and len(out["deeds"]) == 2

    def test_contains_match(self):
        df = pd.DataFrame({"DocType": ["LIS PENDENS", "AMENDED LIS PENDENS"]})
        row = _mapping_row(row_routing=self.BASE_ROUTING)
        out = ColumnMapper.apply_transformations(df, row)
        assert "liens" in out and len(out["liens"]) == 2

    def test_skip_default_drops_unmatched(self):
        df = pd.DataFrame({"DocType": ["DEED", "WEIRD UNKNOWN"]})
        row = _mapping_row(row_routing=self.BASE_ROUTING)
        out = ColumnMapper.apply_transformations(df, row)
        assert set(out.keys()) == {"deeds"}
        assert len(out["deeds"]) == 1

    def test_non_skip_default_keeps_unmatched(self):
        routing = {**self.BASE_ROUTING, "default": "liens"}
        df = pd.DataFrame({"DocType": ["WEIRD UNKNOWN"]})
        row = _mapping_row(row_routing=routing)
        out = ColumnMapper.apply_transformations(df, row)
        assert "liens" in out and len(out["liens"]) == 1

    def test_routing_is_case_insensitive(self):
        df = pd.DataFrame({"DocType": ["  deed  ", "DEED", "Deed"]})
        row = _mapping_row(row_routing=self.BASE_ROUTING)
        out = ColumnMapper.apply_transformations(df, row)
        assert len(out["deeds"]) == 3

    def test_routing_column_missing_returns_single_bucket(self):
        df = pd.DataFrame({"NotTheDocCol": ["foo"]})
        row = _mapping_row(row_routing=self.BASE_ROUTING)
        out = ColumnMapper.apply_transformations(df, row)
        assert "_default" in out


# ---------------------------------------------------------------------------
# 5. End-to-end ordering: rename → post_processors → value_maps → routing
# ---------------------------------------------------------------------------

class TestEndToEnd:

    def test_pinellas_flow(self):
        """
        Mirrors the real Pinellas liens mapping that the migration synthesized:
        1. Rename raw ORI columns
        2. Split BookPage
        3. Normalize JUDGEMENT LIEN → JUDGMENT
        4. Route to bucket by DocType
        """
        df = pd.DataFrame({
            "DirectName":       ["JOHN DOE", "CITY OF CLEARWATER", "X"],
            "IndirectName":     ["JANE DOE", "JOHN SMITH",         "Y"],
            "InstrumentNumber": ["1", "2", "3"],
            "Comments":         ["L1", "L2", "L3"],
            "DocTypeDescription": ["DEED", "JUDGEMENT LIEN", "PROBATE REAL PROPERTY"],
            "BookPage":         ["10/20", "30/40", "50/60"],
        })
        row = _mapping_row(
            mapping={
                "DirectName": "Grantor", "IndirectName": "Grantee",
                "InstrumentNumber": "Instrument", "Comments": "Legal",
                "DocTypeDescription": "DocType",
            },
            post_processors=[
                {"op": "split_on_separator", "from": "BookPage", "sep": "/", "into": ["Book", "Page"]},
            ],
            value_maps={"DocType": {"JUDGEMENT LIEN": "JUDGMENT"}},
            row_routing={
                "column": "DocType",
                "default": "skip",
                "rules": [
                    {"match_exact": ["DEED"], "bucket": "deeds"},
                    {"match_exact": ["JUDGMENT"], "bucket": "judgments"},
                    {"match_exact": ["PROBATE REAL PROPERTY"], "bucket": "probate"},
                ],
            },
        )

        out = ColumnMapper.apply_transformations(df, row)

        assert set(out.keys()) == {"deeds", "judgments", "probate"}
        assert len(out["deeds"]) == 1
        assert len(out["judgments"]) == 1
        assert len(out["probate"]) == 1

        # Renames + split visible across all buckets
        deeds = out["deeds"]
        assert set(["Grantor", "Grantee", "Instrument", "Legal", "DocType",
                    "Book", "Page"]).issubset(set(deeds.columns))
        assert deeds["Grantor"].iloc[0] == "JOHN DOE"
        assert deeds["Book"].iloc[0] == "10"

        # Value normalization visible in routing decision
        assert out["judgments"]["DocType"].iloc[0] == "JUDGMENT"

    def test_no_routing_returns_default_bucket(self):
        df = pd.DataFrame({"a": [1]})
        row = _mapping_row(mapping={"a": "b"})
        out = ColumnMapper.apply_transformations(df, row)
        assert list(out.keys()) == ["_default"]
        assert "b" in out["_default"].columns
